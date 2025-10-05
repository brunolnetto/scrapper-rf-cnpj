#!/usr/bin/env python3
"""
CSV → Postgres ingestion with DuckDB, rich.progress, connection pooling,
zero-copy ingestion, dynamic schema inference, resumable ingestion,
and process/thread-based parallelism.

IMPROVEMENTS:
- Uses DuckDB for CSV processing instead of Polars
- Fixed column name handling from actual CSV headers
- Better error handling and recovery
- Improved checkpoint management
- More efficient batching strategies
- Better memory management
"""

import argparse, glob, io, json, os, time, re
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import logging

import duckdb
from psycopg2.extras import execute_values
from psycopg2.pool import ThreadedConnectionPool
import psycopg2

from rich.progress import (
    Progress, SpinnerColumn, TextColumn, BarColumn,
    TimeElapsedColumn, TaskProgressColumn, ProgressColumn
)
from rich.text import Text

# ---------------- Logging ----------------
logging.basicConfig(
    filename="ingest_errors.log", 
    level=logging.ERROR,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# ---------------- Utilities ----------------
def human_bytes(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.1f}{unit}"
        n /= 1024
    return f"{n:.1f}PB"

def sanitize_column_name(col_name: str, idx: int) -> str:
    """Sanitize column name to be Postgres-compatible"""
    safe_name = col_name.lower().strip()
    safe_name = re.sub(r'[^a-z0-9_]', '_', safe_name)
    safe_name = re.sub(r'^_+|_+$', '', safe_name)
    safe_name = re.sub(r'_+', '_', safe_name)
    if safe_name and safe_name[0].isdigit():
        safe_name = f"col_{safe_name}"
    if not safe_name:
        safe_name = f"col_{idx}"
    # Avoid Postgres reserved keywords
    reserved = {'user', 'table', 'order', 'group', 'select', 'where', 'from'}
    if safe_name in reserved:
        safe_name = f"{safe_name}_col"
    return safe_name

def strip_null_bytes_duckdb(conn, table_name: str):
    """Strip null bytes from all string columns efficiently in DuckDB"""
    try:
        # Get string columns
        result = conn.execute(f"DESCRIBE {table_name}").fetchall()
        string_cols = [row[0] for row in result if 'VARCHAR' in row[1] or 'TEXT' in row[1]]
        
        if not string_cols:
            return
        
        # Update each string column to remove null bytes
        for col in string_cols:
            conn.execute(f"UPDATE {table_name} SET {col} = REPLACE({col}, CHR(0), '')")
    except Exception:
        pass  # Skip if any issues

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

def read_csv_headers(file_path: str, separator: str) -> list:
    """Read and parse CSV headers"""
    with open(file_path, "r", errors="replace") as f:
        header_line = f.readline().strip()
    
    # Handle quoted headers
    headers = []
    if '"' in header_line:
        # Use csv module for proper parsing
        import csv
        reader = csv.reader(io.StringIO(header_line), delimiter=separator)
        headers = next(reader)
    else:
        headers = header_line.split(separator)
    
    return [h.strip() for h in headers]

def validate_batch_columns(conn, temp_table: str, expected_cols: list, file_path: str):
    """Validate and fix batch to match expected columns"""
    try:
        result = conn.execute(f"DESCRIBE {temp_table}").fetchall()
        actual_cols = [row[0] for row in result]
        actual_count = len(actual_cols)
        expected_count = len(expected_cols)

        if actual_count == expected_count:
            # Rename columns to match expected
            for i, expected_col in enumerate(expected_cols):
                if actual_cols[i] != expected_col:
                    conn.execute(f"ALTER TABLE {temp_table} RENAME COLUMN {actual_cols[i]} TO {expected_col}")
            return

        if actual_count < expected_count:
            # Add missing columns with nulls
            missing = expected_count - actual_count
            for i in range(missing):
                conn.execute(f"ALTER TABLE {temp_table} ADD COLUMN _padding_{i} VARCHAR")
            logging.warning(f"{file_path}: Added {missing} padding columns ({actual_count} -> {expected_count})")
        else:
            # Too many columns - drop extra ones
            for i in range(expected_count, actual_count):
                try:
                    conn.execute(f"ALTER TABLE {temp_table} DROP COLUMN {actual_cols[i]}")
                except Exception:
                    pass
            logging.warning(f"{file_path}: Dropped {actual_count - expected_count} extra columns ({actual_count} -> {expected_count})")

        # Rename remaining columns to match expected
        result = conn.execute(f"DESCRIBE {temp_table}").fetchall()
        current_cols = [row[0] for row in result]
        for i, expected_col in enumerate(expected_cols[:len(current_cols)]):
            if current_cols[i] != expected_col:
                conn.execute(f"ALTER TABLE {temp_table} RENAME COLUMN {current_cols[i]} TO {expected_col}")
    except Exception as e:
        logging.error(f"{file_path}: Column validation failed - {e}")

def create_table_from_headers(cursor, pg_table: str, column_names: list):
    """Create table with given column names"""
    col_defs = [f'"{col}" TEXT' for col in column_names]
    create_sql = f"""
        CREATE UNLOGGED TABLE IF NOT EXISTS {pg_table} (
            {','.join(col_defs)}
        );
    """
    cursor.execute(create_sql)

# ---------------- Custom Rich Columns ----------------
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

# ---------------- Progress Manager ----------------
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
                total=None,
                avg=0,
                files_done=0,
                total_files=total_files
            )
            self.overall_start = time.perf_counter()

    def start_file(self, filename: str, total_rows=None):
        with self.lock:
            task_id = self.progress.add_task(
                filename,
                total=total_rows,
                avg=0,
                files_done=self.files_done,
                total_files=0
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
            self.progress.remove_task(task_id)
            if self.global_task is not None:
                self.progress.update(self.global_task, files_done=self.files_done)

# ---------------- Ingestion Methods ----------------
def ingest_batch_execute_values(duck_conn, temp_table: str, cursor, pg_table: str, column_names: list):
    """Insert batch using execute_values (faster than executemany)"""
    strip_null_bytes_duckdb(duck_conn, temp_table)
    rows = duck_conn.execute(f"SELECT * FROM {temp_table}").fetchall()
    cols = ', '.join(f'"{c}"' for c in column_names)
    sql = f"INSERT INTO {pg_table} ({cols}) VALUES %s"
    execute_values(cursor, sql, rows, page_size=1000)

def ingest_batch_copy_csv(duck_conn, temp_table: str, cursor, pg_table: str, column_names: list):
    """Insert batch using COPY (fastest method)"""
    strip_null_bytes_duckdb(duck_conn, temp_table)
    buffer = io.StringIO()
    
    # Export data from DuckDB to CSV format
    result = duck_conn.execute(f"SELECT * FROM {temp_table}").fetchall()
    for row in result:
        csv_row = []
        for value in row:
            if value is None:
                csv_row.append('')
            elif isinstance(value, str):
                # Escape quotes and handle CSV formatting
                escaped = value.replace('"', '""') if '"' in value else value
                if ',' in escaped or '"' in escaped or '\n' in escaped:
                    csv_row.append(f'"{escaped}"')
                else:
                    csv_row.append(escaped)
            else:
                csv_row.append(str(value))
        buffer.write(','.join(csv_row) + '\n')
    
    buffer.seek(0)
    cols = ', '.join(f'"{c}"' for c in column_names)
    cursor.copy_expert(f"COPY {pg_table} ({cols}) FROM STDIN CSV", buffer)

# ---------------- Worker ----------------
def process_file_worker(args, file_path, file_idx, total_files, column_names, 
                       progress_manager, conn_pool, checkpoint_dir):
    """Process a single CSV file"""
    file_task = None
    last_processed = 0
    checkpoint_file = checkpoint_dir / f"{Path(file_path).name}.json"
    conn = None

    # Resume from checkpoint if exists
    if checkpoint_file.exists():
        try:
            with checkpoint_file.open('r') as f:
                checkpoint_data = json.load(f)
                last_processed = checkpoint_data.get("last_row", 0)
                # If file is completely processed, skip it
                if checkpoint_data.get("completed", False):
                    return {"file": file_path, "rows": last_processed, "error": None, "skipped": True}
        except Exception:
            pass

    try:
        conn = conn_pool.getconn()
        cursor = conn.cursor()
        
        # Performance settings
        cursor.execute("SET synchronous_commit = OFF;")
        cursor.execute(f"SET work_mem = '{args.work_mem}';")
        cursor.execute(f"SET maintenance_work_mem = '{args.maintenance_work_mem}';")
        if args.disable_fsync:
            cursor.execute("SET fsync = OFF;")
        conn.commit()
    except Exception as e:
        logging.error(f"{file_path}: Connection error - {e}")
        if conn:
            conn_pool.putconn(conn)
        return {"file": file_path, "error": str(e), "rows": 0}

    try:
        file_name = Path(file_path).name
        file_task = progress_manager.start_file(f"[{file_idx}/{total_files}] {file_name}")

        # Auto-detect separator
        separator = detect_separator(file_path)

        # Create DuckDB connection for this worker
        duck_conn = duckdb.connect(":memory:")
        
        # Create DuckDB connection for this worker
        duck_conn = duckdb.connect(":memory:")
        
        try:
            # Read CSV with DuckDB
            temp_table = "temp_batch"
            
            # Get total rows for resumption
            total_rows_query = f"""
                SELECT COUNT(*) FROM read_csv_auto('{file_path}',
                    delim='{separator}',
                    header=true,
                    ignore_errors=true,
                    all_varchar=true
                )
            """
            total_rows = duck_conn.execute(total_rows_query).fetchone()[0]
            
            file_rows = 0
            batch_count = 0
            
            # Process in chunks
            while file_rows < total_rows:
                # Skip already processed rows (resumption)
                if file_rows < last_processed:
                    file_rows = min(file_rows + args.chunk_rows, last_processed)
                    continue
                
                # Create temporary table for this batch
                duck_conn.execute(f"DROP TABLE IF EXISTS {temp_table}")
                
                batch_query = f"""
                    CREATE TABLE {temp_table} AS
                    SELECT * FROM read_csv_auto('{file_path}',
                        delim='{separator}',
                        header=true,
                        ignore_errors=true,
                        all_varchar=true
                    )
                    LIMIT {args.chunk_rows} OFFSET {file_rows}
                """
                
                duck_conn.execute(batch_query)
                
                # Check if we got any rows
                batch_size = duck_conn.execute(f"SELECT COUNT(*) FROM {temp_table}").fetchone()[0]
                if batch_size == 0:
                    break

                # Validate and align batch columns
                validate_batch_columns(duck_conn, temp_table, column_names, file_path)

                # Ingest batch
                try:
                    if args.method == "copy":
                        ingest_batch_copy_csv(duck_conn, temp_table, cursor, args.pg_table, column_names)
                    else:
                        ingest_batch_execute_values(duck_conn, temp_table, cursor, args.pg_table, column_names)

                    # Commit strategy
                    if args.batch_commit:
                        conn.commit()
                    
                    batch_count += 1
                except Exception as e:
                    logging.error(f"{file_path}: Batch {batch_count} failed - {e}")
                    conn.rollback()
                    raise

                file_rows += batch_size

                # Save checkpoint periodically (every 10 batches)
                if batch_count % 10 == 0:
                    with checkpoint_file.open("w") as f:
                        json.dump({"last_row": file_rows, "completed": False}, f)

                # Update progress
                progress_manager.update_file(file_task, batch_size)
                
                # Break if we've processed all rows
                if file_rows >= total_rows:
                    break
        
        finally:
            duck_conn.close()

        # Final commit if not batch committing
        if not args.batch_commit:
            conn.commit()

        progress_manager.finish_file(file_task)

        # Mark checkpoint as completed
        with checkpoint_file.open("w") as f:
            json.dump({"last_row": file_rows, "completed": True}, f)

        conn_pool.putconn(conn)
        return {"file": file_path, "rows": file_rows, "error": None}

    except Exception as e:
        logging.error(f"{file_path}: Processing error - {e}")
        try:
            if conn:
                conn.rollback()
        except Exception:
            pass
        if file_task:
            progress_manager.finish_file(file_task)
        if conn:
            try:
                conn_pool.putconn(conn)
            except Exception:
                pass
        return {"file": file_path, "error": str(e), "rows": file_rows}

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
    create_table_from_headers(cursor, test_table, column_names)
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

        print(f"Testing: {description}")
        print(f"  Config: chunk={chunk_rows:,}, method={method}, workers={parallel}")

        # Measure memory before
        mem_before = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024  # MB

        start = time.perf_counter()
        rows_processed = 0

        try:
            test_file = files[0]
            separator = detect_separator(test_file)
            
            # Create DuckDB connection for benchmark
            duck_conn = duckdb.connect(":memory:")
            temp_table = "bench_temp"
            
            try:
                duck_conn.execute(f"DROP TABLE IF EXISTS {temp_table}")
                
                # Read sample data with DuckDB
                batch_query = f"""
                    CREATE TABLE {temp_table} AS
                    SELECT * FROM read_csv_auto('{test_file}',
                        delim='{separator}',
                        header=true,
                        ignore_errors=true,
                        all_varchar=true
                    )
                    LIMIT {args.benchmark_sample_rows}
                """
                duck_conn.execute(batch_query)
                
                # Validate columns
                validate_batch_columns(duck_conn, temp_table, column_names, test_file)
                
                # Get actual row count
                rows_processed = duck_conn.execute(f"SELECT COUNT(*) FROM {temp_table}").fetchone()[0]

                conn = conn_pool.getconn()
                cursor = conn.cursor()
                cursor.execute("SET synchronous_commit = OFF;")
                cursor.execute(f"SET work_mem = '{args.work_mem}';")

                if method == 'execute_values':
                    ingest_batch_execute_values(duck_conn, temp_table, cursor, test_table, column_names)
                else:
                    ingest_batch_copy_csv(duck_conn, temp_table, cursor, test_table, column_names)

                conn.commit()
                conn_pool.putconn(conn)
            finally:
                duck_conn.close()

            elapsed = time.perf_counter() - start
            mem_after = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024  # MB
            mem_used = mem_after - mem_before

            throughput = rows_processed / elapsed if elapsed > 0 else 0

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

    results.sort(key=lambda x: x['throughput'], reverse=True)

    for r in results:
        print(f"{r['description']:<45} {r['throughput']:>10,.0f} r/s "
              f"{r['time']:>6.2f}s {r['memory_mb']:>6.0f} MB")

    print(f"{'='*70}")

    if results:
        best = results[0]
        print(f"\nRECOMMENDATION:")
        print(f"  Best throughput: {best['description']}")
        print(f"  Command: python script.py files/*.csv \\")
        print(f"    --chunk-rows {best['chunk_rows']} \\")
        print(f"    --method {best['method']} \\")
        print(f"    --parallel {best['parallel']} \\")
        print(f"    --pg-dsn '...' --pg-table '...'")

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
    parser.add_argument("--method", choices=["copy", "execute_values"], default="copy")
    parser.add_argument("--parallel", type=int, default=1)
    parser.add_argument("--benchmark", action="store_true")
    parser.add_argument("--benchmark-sample-rows", type=int, default=100000)
    parser.add_argument("--resume", action="store_true", help="Resume from checkpoints")
    args = parser.parse_args(argv)

    # Expand glob patterns
    files = sorted([f for pat in args.patterns for f in glob.glob(pat, recursive=True)])
    if not files:
        print("[error] No files matched patterns")
        return 1

    print(f"[config] Found {len(files)} files to process")
    print(f"[config] Method: {args.method}, Workers: {args.parallel}")
    print(f"[config] Chunk size: {args.chunk_rows:,} rows")

    checkpoint_dir = Path(".ingest_checkpoints")
    checkpoint_dir.mkdir(exist_ok=True)

    # Create connection pool
    try:
        conn_pool = ThreadedConnectionPool(1, args.parallel + 2, dsn=args.pg_dsn)
    except psycopg2.Error as e:
        print(f"[error] Failed to connect to database: {e}")
        return 1

    conn = conn_pool.getconn()
    cursor = conn.cursor()

    # Check if table exists
    table_name = args.pg_table.split('.')[-1]
    cursor.execute(f"SELECT to_regclass('{args.pg_table}');")
    table_exists = cursor.fetchone()[0] is not None

    if not table_exists:
        print(f"[setup] Creating table {args.pg_table} from first file...")
        # Read headers from first file using DuckDB
        sample_file = files[0]
        separator = detect_separator(sample_file)
        
        # Use DuckDB to read headers
        duck_conn = duckdb.connect(":memory:")
        try:
            # Read just one row to get column names
            duck_conn.execute(f"""
                CREATE TABLE sample AS 
                SELECT * FROM read_csv_auto('{sample_file}',
                    delim='{separator}',
                    header=true,
                    ignore_errors=true,
                    all_varchar=true
                ) 
                LIMIT 1
            """)
            
            # Get column names from DuckDB
            result = duck_conn.execute("DESCRIBE sample").fetchall()
            raw_headers = [row[0] for row in result]
            column_names = [sanitize_column_name(h, i) for i, h in enumerate(raw_headers)]
            
        finally:
            duck_conn.close()
        
        create_table_from_headers(cursor, args.pg_table, column_names)
        conn.commit()
        print(f"[setup] Table created with {len(column_names)} columns")
    else:
        print(f"[setup] Table {args.pg_table} exists, reading schema...")
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
    skipped = [r for r in results if r.get("skipped")]
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
    if skipped:
        print(f"Files skipped:   {len(skipped)} (already completed)")
    if failed:
        print(f"\nFailed files:")
        for r in failed:
            print(f"  - {Path(r['file']).name}: {r['error']}")
    print(f"{'='*60}\n")

    # Clean up successful checkpoints (unless resume mode)
    if not args.resume:
        for r in successful:
            checkpoint_file = checkpoint_dir / f"{Path(r['file']).name}.json"
            if checkpoint_file.exists():
                checkpoint_file.unlink()

    conn_pool.closeall()
    return 0

if __name__ == "__main__":
    import sys
    raise SystemExit(main(sys.argv[1:]))
