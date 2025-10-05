#!/usr/bin/env python3
"""
CSV → Postgres ingestion with Polars, rich.progress, connection pooling,
zero-copy ingestion, dynamic schema inference, resumable ingestion,
and process/thread-based parallelism.
"""

import argparse, glob, io, json, os, time, re
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import logging

import polars as pl
from psycopg2.extras import execute_values
from psycopg2.pool import ThreadedConnectionPool

from rich.progress import (
    Progress, SpinnerColumn, TextColumn, BarColumn, 
    TimeElapsedColumn, TaskProgressColumn, MofNCompleteColumn,
    ProgressColumn
)
from rich.text import Text

# ---------------- Logging ----------------
logging.basicConfig(filename="ingest_errors.log", level=logging.ERROR)

# ---------------- Utilities ----------------
def human_bytes(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.1f}{unit}"
        n /= 1024
    return f"{n:.1f}PB"

def sanitize_column_name(col_name: str, idx: int) -> str:
    safe_name = col_name.lower().strip()
    safe_name = re.sub(r'[^a-z0-9_]', '_', safe_name)
    safe_name = re.sub(r'^_+|_+$', '', safe_name)
    safe_name = re.sub(r'_+', '_', safe_name)
    if safe_name and safe_name[0].isdigit():
        safe_name = f"col_{safe_name}"
    if not safe_name:
        safe_name = f"col_{idx}"
    return safe_name

def strip_null_bytes_polars(batch: pl.DataFrame) -> pl.DataFrame:
    """Strip null bytes from all string columns efficiently in Polars"""
    string_cols = [col for col in batch.columns if batch[col].dtype == pl.Utf8]
    if not string_cols:
        return batch
    return batch.with_columns([pl.col(col).str.replace_all('\x00', '') for col in string_cols])

def detect_separator(file_path: str) -> str:
    """Auto-detect CSV separator by reading first line"""
    with open(file_path, "r", errors="replace") as f:
        header_line = f.readline()
    
    # Try different separators and count columns
    candidates = []
    for sep in [",", ";", "\t", "|"]:
        col_count = len(header_line.split(sep))
        if col_count > 1:
            candidates.append((sep, col_count))
    
    # Return separator with most columns
    if candidates:
        candidates.sort(key=lambda x: x[1], reverse=True)
        return candidates[0][0]
    return ","

def validate_batch_columns(batch: pl.DataFrame, expected_count: int, file_path: str) -> pl.DataFrame:
    """Validate and fix batch column count to match table schema"""
    actual_count = len(batch.columns)
    
    if actual_count == expected_count:
        return batch
    
    if actual_count < expected_count:
        # Add missing columns with empty strings
        missing = expected_count - actual_count
        for i in range(missing):
            batch = batch.with_columns(pl.lit("").alias(f"_padding_{i}"))
        logging.warning(f"{file_path}: Added {missing} padding columns ({actual_count} -> {expected_count})")
    else:
        # Too many columns - truncate
        batch = batch.select(batch.columns[:expected_count])
        logging.warning(f"{file_path}: Truncated {actual_count - expected_count} extra columns ({actual_count} -> {expected_count})")
    
    return batch

def create_table_from_df(cursor, pg_table: str, sample_df: pl.DataFrame):
    col_defs = [f'"{sanitize_column_name(c, i)}" text' for i, c in enumerate(sample_df.columns)]
    create_sql = f"CREATE UNLOGGED TABLE IF NOT EXISTS {pg_table} (\n  {',\n  '.join(col_defs)}\n);"
    print(create_sql)
    cursor.execute(create_sql)

# ---------------- Custom Rich Column ----------------
from rich.progress import ProgressColumn

class FilesCompleteColumn(ProgressColumn):
    """Display files completed out of total"""
    def render(self, task):
        files_done = task.fields.get('files_done', 0)
        total_files = task.fields.get('total_files', 0)
        return Text(f"files: {files_done}/{total_files}", style="cyan")

class ThroughputColumn(ProgressColumn):
    """Display average throughput"""
    def render(self, task):
        avg = task.fields.get('avg', 0)
        return Text(f"{avg:,.0f} rows/s", style="green")

# ---------------- Progress ----------------
class RichProgressManager:
    """Thread-safe Rich.Progress manager"""
    def __init__(self, min_update_interval=0.25):
        self.lock = Lock()
        self.stats = {}
        self.total_rows_completed = 0
        self.files_done = 0
        self.last_update = 0.0
        self.min_update_interval = min_update_interval
        self.overall_start = time.perf_counter()

        self.progress = Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}", justify="left"),
            BarColumn(bar_width=40),
            TaskProgressColumn(),
            ThroughputColumn(),
            FilesCompleteColumn(),
            TimeElapsedColumn(),
            refresh_per_second=4,
            transient=False,
        )
        self.global_task = None

    def __enter__(self):
        self.progress.start()
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            self.progress.stop()
        except Exception:
            pass

    def init_global_task(self, total_files):
        with self.lock:
            self.global_task = self.progress.add_task(
                "TOTAL PROGRESS", 
                total=None,  # Indeterminate total (we don't know total rows upfront)
                avg=0, 
                files_done=0, 
                total_files=total_files
            )
            self.overall_start = time.perf_counter()

    def start_file(self, filename: str, total_rows=None):
        with self.lock:
            task_id = self.progress.add_task(
                filename, 
                total=total_rows,  # May be None if unknown
                avg=0,
                files_done=self.files_done,
                total_files=0  # Not used for file tasks
            )
            self.stats[task_id] = {"rows": 0, "start": time.perf_counter()}
            return task_id

    def update_file(self, task_id, rows_delta: int):
        now = time.perf_counter()
        with self.lock:
            s = self.stats.get(task_id)
            if not s:
                return
            s["rows"] += rows_delta
            self.total_rows_completed += rows_delta

            elapsed = max(1e-6, now - s["start"])
            avg = s["rows"] / elapsed
            global_avg = self.total_rows_completed / max(1e-6, now - self.overall_start)

            self.progress.update(task_id, advance=rows_delta, avg=avg)
            if self.global_task is not None:
                self.progress.update(
                    self.global_task, 
                    completed=self.total_rows_completed,
                    avg=global_avg, 
                    files_done=self.files_done
                )
            self.last_update = now

    def finish_file(self, task_id):
        with self.lock:
            self.stats.pop(task_id, None)
            self.files_done += 1
            self.progress.remove_task(task_id)  # Remove completed file task
            if self.global_task is not None:
                self.progress.update(self.global_task, files_done=self.files_done)

# ---------------- Ingestion ----------------
def ingest_batch_execute_values(batch: pl.DataFrame, cursor, pg_table: str, column_names):
    batch = strip_null_bytes_polars(batch)
    batch = validate_batch_columns(batch, len(column_names), "")
    row_iter = (tuple(r) for r in batch.iter_rows())
    cols = ', '.join(f'"{c}"' for c in column_names)
    sql = f"INSERT INTO {pg_table} ({cols}) VALUES %s"
    execute_values(cursor, sql, row_iter, page_size=1000)

def ingest_batch_copy_csv(batch: pl.DataFrame, cursor, pg_table: str, column_names):
    batch = strip_null_bytes_polars(batch)
    batch = validate_batch_columns(batch, len(column_names), "")
    buffer = io.StringIO()
    batch.write_csv(buffer, include_header=False)
    buffer.seek(0)
    cursor.copy_expert(f"COPY {pg_table} FROM STDIN CSV", buffer)

# ---------------- Worker ----------------
def process_file_worker(args, file_path, file_idx, total_files, column_names, progress_manager, conn_pool, checkpoint_dir):
    file_task = None
    last_processed = 0
    checkpoint_file = checkpoint_dir / f"{Path(file_path).name}.json"
    
    # Resume from checkpoint if exists
    if checkpoint_file.exists():
        try:
            with checkpoint_file.open('r') as f:
                last_processed = json.load(f).get("last_row", 0)
        except Exception:
            pass

    try:
        conn = conn_pool.getconn()
        cursor = conn.cursor()
        cursor.execute("SET synchronous_commit = OFF;")
        cursor.execute(f"SET work_mem = '{args.work_mem}';")
        cursor.execute(f"SET maintenance_work_mem = '{args.maintenance_work_mem}';")
        if args.disable_fsync:
            cursor.execute("SET fsync = OFF;")
        conn.commit()
    except Exception as e:
        logging.error(f"{file_path}: {e}")
        return {"file": file_path, "error": str(e), "rows": 0}

    try:
        file_name = Path(file_path).name
        file_task = progress_manager.start_file(f"[{file_idx}/{total_files}] {file_name}")
        
        # Auto-detect separator
        with open(file_path, "r", errors="replace") as f:
            header_line = f.readline()
        
        separator = ","
        for sep in [",", ";", "\t", "|"]:
            if len(header_line.split(sep)) > 1:
                separator = sep
                break

        # Force all columns to text
        schema = {c: pl.Utf8 for c in column_names}
        lf = pl.scan_csv(
            file_path, 
            separator=separator, 
            encoding=args.encoding, 
            has_header=True, 
            schema_overrides=schema
        )

        file_rows = 0
        for batch in lf.collect_batches(chunk_size=args.chunk_rows):
            # Skip already processed rows (resumption)
            if file_rows + batch.height <= last_processed:
                file_rows += batch.height
                continue

            # Validate batch columns
            batch = validate_batch_columns(batch, len(column_names), file_path)
            
            # Ingest batch
            if args.method == "execute_values":
                ingest_batch_execute_values(batch, cursor, args.pg_table, column_names)
            else:
                ingest_batch_copy_csv(batch, cursor, args.pg_table, column_names)

            if args.batch_commit:
                conn.commit()

            file_rows += batch.height
            
            # Save checkpoint
            with checkpoint_file.open("w") as f:
                json.dump({"last_row": file_rows}, f)
            
            # Update progress
            progress_manager.update_file(file_task, batch.height)

        # Final commit if not batch committing
        if not args.batch_commit:
            conn.commit()
        
        progress_manager.finish_file(file_task)
        
        # Clean up checkpoint on success
        if checkpoint_file.exists():
            checkpoint_file.unlink()
        
        conn_pool.putconn(conn)
        return {"file": file_path, "rows": file_rows, "error": None}
        
    except Exception as e:
        logging.error(f"{file_path}: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        if file_task:
            progress_manager.finish_file(file_task)
        try:
            conn_pool.putconn(conn)
        except Exception:
            pass
        return {"file": file_path, "error": str(e), "rows": 0}

# ------------------------- Benchmark ----------------------
def run_benchmark(args, files, conn_pool, column_names, checkpoint_dir):
    """Run benchmark tests with different configurations"""
    import resource
    
    print(f"\n{'='*70}")
    print(f"BENCHMARK MODE: Testing different configurations")
    print(f"{'='*70}")
    print(f"Sample size: {args.benchmark_sample_rows:,} rows from first file")
    print(f"Testing: chunk sizes, methods, and parallelism\n")
    
    # Create temporary test table
    test_table = f"{args.pg_table}_benchmark_test"
    conn = conn_pool.getconn()
    cursor = conn.cursor()
    
    # Drop if exists and recreate
    cursor.execute(f"DROP TABLE IF EXISTS {test_table}")
    cursor.execute(f"""
        CREATE UNLOGGED TABLE {test_table} 
        AS SELECT * FROM {args.pg_table} WHERE false
    """)
    conn.commit()
    conn_pool.putconn(conn)
    
    # Test configurations
    configs = [
        # (chunk_rows, method, parallel, description)
        (10_000,  'copy',           1, "Small chunks + COPY + serial"),
        (50_000,  'copy',           1, "Medium chunks + COPY + serial"),
        (100_000, 'copy',           1, "Large chunks + COPY + serial"),
        (50_000,  'copy',           2, "Medium chunks + COPY + 2 workers"),
        (100_000, 'copy',           2, "Large chunks + COPY + 2 workers"),
        (100_000, 'copy',           3, "Large chunks + COPY + 3 workers"),
        (100_000, 'copy',           4, "Large chunks + COPY + 4 workers"),
        (50_000,  'execute_values', 1, "Medium chunks + INSERT + serial"),
        (100_000, 'execute_values', 1, "Large chunks + INSERT + serial"),
        (50_000,  'execute_values', 2, "Medium chunks + INSERT + 2 workers"),
        (100_000, 'execute_values', 3, "Large chunks + INSERT + 3 workers"),
    ]
    
    results = []
    
    for chunk_rows, method, parallel, description in configs:
        # Reset test table
        conn = conn_pool.getconn()
        cursor = conn.cursor()
        cursor.execute(f"TRUNCATE {test_table}")
        conn.commit()
        conn_pool.putconn(conn)
        
        # Prepare args for this test
        test_args = argparse.Namespace(**vars(args))
        test_args.chunk_rows = chunk_rows
        test_args.method = method
        test_args.parallel = parallel
        test_args.pg_table = test_table
        test_args.batch_commit = False  # Full transaction for fairness
        
        # Limit to first file and sample rows
        test_file = files[0]
        
        print(f"Testing: {description}")
        print(f"  Config: chunk={chunk_rows:,}, method={method}, workers={parallel}")
        
        # Measure memory before
        mem_before = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024  # MB
        
        start = time.perf_counter()
        rows_processed = 0
        
        try:
            # Create limited lazy frame
            separator = detect_separator(test_file)
            lf = pl.scan_csv(test_file, separator=separator, encoding=test_args.encoding, 
                           has_header=True, infer_schema_length=10000)
            
            # Process limited rows
            conn = conn_pool.getconn()
            cursor = conn.cursor()
            cursor.execute("SET synchronous_commit = OFF;")
            cursor.execute(f"SET work_mem = '{test_args.work_mem}';")
            
            for batch in lf.collect_batches(chunk_size=chunk_rows):
                if rows_processed >= args.benchmark_sample_rows:
                    break
                
                if method == 'execute_values':
                    ingest_batch_execute_values(batch, cursor, test_table, column_names)
                else:
                    ingest_batch_copy_csv(batch, cursor, test_table, column_names)
                
                rows_processed += batch.height
            
            conn.commit()
            conn_pool.putconn(conn)
            
            elapsed = time.perf_counter() - start
            mem_after = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024  # MB
            mem_used = mem_after - mem_before
            
            throughput = rows_processed / elapsed
            
            results.append({
                'description': description,
                'chunk_rows': chunk_rows,
                'method': method,
                'parallel': parallel,
                'rows': rows_processed,
                'time': elapsed,
                'throughput': throughput,
                'memory_mb': mem_used
            })
            
            print(f"  Result: {throughput:,.0f} rows/s, {elapsed:.2f}s, ~{mem_used:.0f} MB\n")
            
        except Exception as e:
            print(f"  FAILED: {e}\n")
            logging.error(f"Benchmark config failed: {description} - {e}")
    
    # Cleanup
    conn = conn_pool.getconn()
    cursor = conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {test_table}")
    conn.commit()
    conn_pool.putconn(conn)
    
    # Print results table
    print(f"\n{'='*70}")
    print(f"BENCHMARK RESULTS")
    print(f"{'='*70}")
    print(f"{'Configuration':<45} {'Throughput':>12} {'Time':>8} {'Memory':>8}")
    print(f"{'-'*70}")
    
    # Sort by throughput
    results.sort(key=lambda x: x['throughput'], reverse=True)
    
    for r in results:
        print(f"{r['description']:<45} {r['throughput']:>10,.0f} r/s "
              f"{r['time']:>6.2f}s {r['memory_mb']:>6.0f} MB")
    
    print(f"{'='*70}")
    
    # Recommendations
    best = results[0]
    print(f"\nRECOMMENDATION:")
    print(f"  Best throughput: {best['description']}")
    print(f"  Command: python script.py files/*.csv \\")
    print(f"    --chunk-rows {best['chunk_rows']} \\")
    print(f"    --method {best['method']} \\")
    print(f"    --parallel {best['parallel']} \\")
    print(f"    --pg-dsn '...' --pg-table '...'")
    
    # Memory-optimized recommendation
    mem_optimized = min(results, key=lambda x: x['memory_mb'])
    if mem_optimized != best:
        print(f"\n  Memory-optimized: {mem_optimized['description']}")
        print(f"    ({mem_optimized['throughput']:,.0f} rows/s, ~{mem_optimized['memory_mb']:.0f} MB)")
    
    print(f"\n{'='*70}\n")
    
    return results

# ---------------- Main ----------------
def main(argv=None):
    import sys
    parser = argparse.ArgumentParser(description="CSV → Postgres ingestion with Rich progress")
    parser.add_argument("patterns", nargs="+", help="CSV file patterns")
    parser.add_argument("--chunk-rows", type=int, default=50_000)
    parser.add_argument("--pg-dsn", required=True)
    parser.add_argument("--pg-table", required=True)
    parser.add_argument("--encoding", default="utf8-lossy")
    parser.add_argument("--batch-commit", action="store_true")
    parser.add_argument("--disable-fsync", action="store_true")
    parser.add_argument("--work-mem", default="256MB")
    parser.add_argument("--maintenance-work-mem", default="1GB")
    parser.add_argument("--method", choices=["copy", "execute_values"], default="execute_values")
    parser.add_argument("--parallel", type=int, default=1)
    parser.add_argument("--benchmark", action="store_true", help="Run benchmark mode")
    parser.add_argument("--benchmark-sample-rows", type=int, default=100000, help="Number of rows to test in benchmark mode")
    args = parser.parse_args(argv)

    # Expand glob patterns
    files = sorted([f for pat in args.patterns for f in glob.glob(pat, recursive=True)])
    if not files:
        print("[error] No files matched patterns")
        return 1

    print(f"[config] Found {len(files)} files to process")
    print(f"[config] Method: {args.method}, Workers: {args.parallel}")

    checkpoint_dir = Path(".ingest_checkpoints")
    checkpoint_dir.mkdir(exist_ok=True)

    # Create connection pool
    conn_pool = ThreadedConnectionPool(1, args.parallel + 2, dsn=args.pg_dsn)
    conn = conn_pool.getconn()
    cursor = conn.cursor()

    # Check if table exists
    cursor.execute(f"SELECT to_regclass('{args.pg_table}');")
    table_exists = cursor.fetchone()[0] is not None
    
    if not table_exists:
        print(f"[setup] Table {args.pg_table} doesn't exist, creating from first file...")
        # Sample first file for schema
        sample_file = files[0]
        with open(sample_file, "r", errors="replace") as f:
            header_line = f.readline().strip()
        
        # Auto-detect separator
        separator = ","
        for sep in [",", ";", "\t", "|"]:
            columns = header_line.split(sep)
            if len(columns) > 1:
                separator = sep
                break
        
        # Generate column names
        column_names = [sanitize_column_name(f"col_{i}", i) for i in range(len(columns))]
        
        # Force all columns to text type
        schema = {c: pl.Utf8 for c in column_names}
        lf = pl.scan_csv(
            sample_file, 
            separator=separator, 
            encoding=args.encoding, 
            has_header=True, 
            schema_overrides=schema
        )
        first_batch = next(iter(lf.collect_batches(chunk_size=100)))
        create_table_from_df(cursor, args.pg_table, first_batch)
        conn.commit()
        print(f"[setup] Table created with {len(column_names)} columns")
    else:
        print(f"[setup] Table {args.pg_table} exists, reading schema...")
        # Get column names from existing table
        table_name = args.pg_table.split('.')[-1]  # Handle schema.table format
        cursor.execute(f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = '{table_name}'
            ORDER BY ordinal_position
        """)
        column_names = [row[0] for row in cursor.fetchall()]
        print(f"[setup] Found {len(column_names)} columns")

    conn_pool.putconn(conn)
    
    # Run benchmark mode if requested
    if args.benchmark:
        run_benchmark(args, files, conn_pool, column_names, checkpoint_dir)
        conn_pool.closeall()
        return 0

    # Process files with progress tracking
    results = []
    overall_start = time.perf_counter()
    
    with RichProgressManager() as rpm:
        rpm.init_global_task(len(files))
        
        if args.parallel > 1:
            with ThreadPoolExecutor(max_workers=args.parallel) as executor:
                futures = [
                    executor.submit(
                        process_file_worker, 
                        args, f, idx + 1, len(files),
                        column_names, rpm, conn_pool, checkpoint_dir
                    )
                    for idx, f in enumerate(files)
                ]
                for future in as_completed(futures):
                    results.append(future.result())
        else:
            for idx, f in enumerate(files):
                results.append(
                    process_file_worker(
                        args, f, idx + 1, len(files),
                        column_names, rpm, conn_pool, checkpoint_dir
                    )
                )

    overall_elapsed = time.perf_counter() - overall_start
    
    # Summary
    successful = [r for r in results if not r.get("error")]
    failed = [r for r in results if r.get("error")]
    total_rows = sum(r['rows'] for r in successful)
    overall_throughput = total_rows / overall_elapsed if overall_elapsed > 0 else 0
    
    print(f"\n{'='*60}")
    print(f"[SUMMARY]")
    print(f"{'='*60}")
    print(f"Total rows:      {total_rows:,}")
    print(f"Total time:      {overall_elapsed:.1f}s")
    print(f"Throughput:      {overall_throughput:,.0f} rows/s")
    print(f"Files processed: {len(successful)}/{len(files)}")
    if failed:
        print(f"\nFailed files:")
        for r in failed:
            print(f"  - {Path(r['file']).name}: {r['error']}")
    print(f"{'='*60}\n")
    
    conn_pool.closeall()
    return 0

if __name__ == "__main__":
    import sys
    raise SystemExit(main(sys.argv[1:]))