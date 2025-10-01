from pathlib import Path
import argparse
import duckdb
import time
import glob
import signal
import sys
import re
import os

def parse_args(argv):
    parser = argparse.ArgumentParser(description="Ingest Receita Federal CNPJ CSV files into PostgreSQL using DuckDB")
    
    parser.add_argument("source", type=Path, help="CSV directory or file")
    parser.add_argument("--table", default="cnpjs", help="Target PostgreSQL table")
    parser.add_argument("--limit-rows", type=int, default=None)
    parser.add_argument("--sample-rows", type=int, default=5)
    parser.add_argument("--user", default="postgres")
    parser.add_argument("--password", default="postgres")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", default=5432)
    parser.add_argument("--dbname", default="postgres")
    parser.add_argument("--overwrite", action="store_true", 
                        help="Drop table if exists before creating")
    parser.add_argument("--encoding", default=None,
                        help="CSV file encoding (default: auto-detect)")
    parser.add_argument("--encoding-sample-size", type=int, default=100000,
                        help="Bytes to sample for encoding detection (default: 100000)")
    parser.add_argument("--threads", type=int, default=None,
                        help="Number of threads for DuckDB (default: auto)")
    return parser.parse_args(argv)


def detect_encoding(file_path, sample_size=100000):
    """Detect file encoding by trying common encodings.
    Returns DuckDB-compatible encoding names."""
    
    # Map Python encoding names to DuckDB-compatible names
    encodings_to_try = [
        ('utf-8', 'utf-8'),
        ('windows-1252', 'CP1252'),
        ('cp1252', 'CP1252'),
        ('latin-1', 'latin-1'),
        ('iso-8859-1', 'latin-1'),
        ('windows-1250', 'windows-1250-2000'),
        ('cp1250', 'CP1250'),
        ('windows-1251', 'CP1251'),
        ('cp1251', 'CP1251'),
    ]
    
    try:
        with open(file_path, 'rb') as f:
            sample = f.read(sample_size)
        
        # Try each encoding
        for python_enc, duckdb_enc in encodings_to_try:
            try:
                sample.decode(python_enc)
                return duckdb_enc
            except (UnicodeDecodeError, LookupError):
                continue
        
        # Fallback to latin-1 (always works)
        return 'latin-1'
    except Exception as e:
        print(f"⚠ Warning: Could not detect encoding: {e}")
        return 'latin-1'


def group_files_by_encoding(file_paths, sample_size=100000):
    """Group files by their detected encoding.
    Returns dict of {encoding: [file_paths]}.
    """
    encoding_groups = {}
    
    print(f"🔍 Detecting encoding for {len(file_paths)} files...")
    for file_path in file_paths:
        encoding = detect_encoding(file_path, sample_size)
        if encoding not in encoding_groups:
            encoding_groups[encoding] = []
        encoding_groups[encoding].append(file_path)
        print(f"   {Path(file_path).name}: {encoding}")
    
    print(f"\n📊 Encoding distribution:")
    for encoding, files in encoding_groups.items():
        total_size = sum(Path(f).stat().st_size for f in files) / (1024 * 1024)
        print(f"   {encoding}: {len(files)} files, {total_size:.1f} MB")
    
    return encoding_groups


def _quote_path(p: str) -> str:
    """Quote file path for embedding into SQL; use POSIX style for DuckDB compatibility."""
    # convert to posix style (forward slashes) to play nicely across platforms
    pposix = Path(p).as_posix()
    # escape single quotes by doubling them
    return pposix.replace("'", "''")


def ingest_with_duckdb_to_postgres(args):
    # DuckDB connection (in-memory) with optimizations
    con = duckdb.connect()

    # Performance optimizations for large CNPJ files
    if args.threads:
        con.execute(f"SET threads TO {args.threads}")
    else:
        con.execute("SET threads TO 4")  # Conservative default for stability
    con.execute("SET preserve_insertion_order = false")  # Faster inserts
    con.execute("SET memory_limit = '8GB'")  # Increased for large files
    con.execute("SET max_temp_directory_size = '20GB'")  # Allow more temp space
    # Note: If your DuckDB build doesn't support some of these SET options they will raise.

    # Default patterns for Receita Federal table files (without extensions)
    TABLE_PATTERNS = {
        "empresa": "*EMPRECSV*",
        "estabelecimento": "*ESTABELE*",
        "socios": "*SOCIO*",
        "simples": "*SIMPLES*",
        "cnae": "*CNAE*",
        "moti": "*MOTI*",
        "natju": "*NATJU*",
        "munic": "*MUNIC*",
        "pais": "*PAIS*",
        "cnaes": "*CNAE*"
    }

    # Determine glob pattern: use table-specific pattern or fallback to table name
    if args.source.is_dir():
        pattern = TABLE_PATTERNS.get(args.table, f"*{args.table.upper()}*")
        pattern = f"{args.source}/{pattern}"
    else:
        pattern = str(args.source)

    # Collect matching files
    matching_files = sorted(glob.glob(pattern))
    if not matching_files:
        print(f"✗ No files found matching pattern: {pattern}")
        print(f"   Available files in directory:")
        if args.source.is_dir():
            for f in sorted(args.source.iterdir())[:10]:  # Show first 10 files
                print(f"     {f.name}")
        con.close()
        return

    # Group by detected encoding
    encoding_groups = group_files_by_encoding(matching_files, args.encoding_sample_size)

    total_size_mb = sum(Path(f).stat().st_size for f in matching_files) / (1024 * 1024)
    print(f" Found {len(matching_files)} matching files, total size: {total_size_mb:.1f} MB")

    # If user explicitly passed --encoding, override grouping and treat all as that encoding:
    if args.encoding:
        print(f"⚑ --encoding passed: forcing all files to use {args.encoding}")
        encoding_groups = {args.encoding: matching_files}

    # --- Build master temp_ingest from groups, honoring each group's encoding ---
    master_created = False
    temp_count = 0

    group_index = 0
    for encoding, files in encoding_groups.items():
        group_index += 1
        print(f"\n➡ Processing encoding group {group_index}/{len(encoding_groups)}: {encoding}")
        # Build a SQL expression that reads the group's files.
        # We create a per-group temp table by UNION ALL-ing single-file read_csv selects.
        # This avoids trying to read all groups with one global encoding.
        select_parts = []
        for f in files:
            f_quoted = _quote_path(f)
            # Use read_csv (not read_csv_auto) with header=True and delim=';'. sample_size=-1 for full sampling.
            # You can tweak options (ignore_errors, parallel) if needed.
            part = (
                f"SELECT * FROM read_csv('{f_quoted}', "
                f"encoding='{encoding}', delim=';', header=TRUE, sample_size=-1, parallel=TRUE)"
            )
            select_parts.append(part)
        group_select_sql = "\nUNION ALL\n".join(select_parts)

        # Name for the per-group temp table
        group_table = f"temp_ingest_group_{group_index}"

        try:
            print(f"   Creating group table {group_table} from {len(files)} files (encoding={encoding}) ...")
            con.execute(f"CREATE OR REPLACE TABLE {group_table} AS {group_select_sql}")
            group_rows = con.execute(f"SELECT COUNT(*) FROM {group_table}").fetchone()[0]
            print(f"   ✓ Group rows: {group_rows:,}")
        except Exception as e:
            print(f"✗ Failed to create per-group table for encoding {encoding}: {e}")
            print("  Aborting ingestion so you can inspect problematic files.")
            con.close()
            return

        if not master_created:
            # First group's table becomes the master temp_ingest (schema inferred)
            print("   Creating master temp_ingest from first encoding group...")
            try:
                con.execute(f"CREATE OR REPLACE TABLE temp_ingest AS SELECT * FROM {group_table}")
                temp_count = con.execute("SELECT COUNT(*) FROM temp_ingest").fetchone()[0]
                print(f"   ✓ temp_ingest rows after creation: {temp_count:,}")
                master_created = True
            except Exception as e:
                print(f"✗ Failed to create master temp_ingest from {group_table}: {e}")
                con.close()
                return
            # drop per-group table now (we'll keep it if subsequent insert fails for debugging)
            try:
                con.execute(f"DROP TABLE IF EXISTS {group_table}")
            except Exception:
                pass
        else:
            # Insert group rows into existing master. This requires compatible schema.
            print(f"   Inserting {group_rows:,} rows from {group_table} into master temp_ingest...")
            try:
                con.execute(f"INSERT INTO temp_ingest SELECT * FROM {group_table}")
                temp_count = con.execute("SELECT COUNT(*) FROM temp_ingest").fetchone()[0]
                print(f"   ✓ temp_ingest rows after insert: {temp_count:,}")
                # drop the group table
                con.execute(f"DROP TABLE IF EXISTS {group_table}")
            except Exception as e:
                # Schema/typing mismatch likely — fail loud and preserve group table for inspection
                print("✗ Insert failed — likely a schema mismatch between groups.")
                print(f"  Error: {e}")
                print(f"  Leaving {group_table} in DuckDB for inspection (you can run `DESCRIBE {group_table}`).")
                print("  Aborting ingestion so you can fix / normalize schemas across files.")
                con.close()
                return

    # After processing groups, we have temp_ingest with all rows read respecting each file encoding
    temp_count = con.execute("SELECT COUNT(*) FROM temp_ingest").fetchone()[0]
    print(f"\n✓ Loaded {temp_count:,} rows into DuckDB in groups (respected per-file encodings)")

    # --- Attach PostgreSQL and continue as before ---
    print(f"\n Connecting to PostgreSQL at {args.host}:{args.port}/{args.dbname}")
    pg_connection = f"dbname={args.dbname} user={args.user} password={args.password} host={args.host} port={args.port}"

    try:
        con.execute(f"ATTACH '{pg_connection}' AS pg (TYPE POSTGRES)")
        print("✓ Connected to PostgreSQL")
    except Exception as e:
        print(f"✗ Failed to connect to PostgreSQL: {e}")
        con.close()
        return

    # Drop table if overwrite flag is set
    if args.overwrite:
        try:
            con.execute(f"DROP TABLE IF EXISTS pg.{args.table}")
            print(f"✓ Dropped existing table '{args.table}'")
        except Exception as e:
            print(f"⚠ Warning: Could not drop table: {e}")

    # Create table structure in PostgreSQL and bulk insert (your existing path)
    print(f"\n Inserting data into PostgreSQL table '{args.table}'...")
    print(f"   Using bulk insert (fastest method)")
    insert_start = time.perf_counter()
    try:
        con.execute(f"CREATE TABLE IF NOT EXISTS pg.{args.table} AS SELECT * FROM temp_ingest")
        insert_elapsed = time.perf_counter() - insert_start
        print(f"✓ Data successfully loaded into PostgreSQL in {insert_elapsed:.2f}s")
    except Exception as e:
        if "already exists" in str(e).lower():
            print(f"✗ Table '{args.table}' already exists. Use --overwrite to replace it.")
            con.close()
            return
        else:
            print(f"✗ Failed to create table: {e}")
            con.close()
            return

    # Count rows in PostgreSQL
    count = con.execute(f"SELECT COUNT(*) FROM pg.{args.table}").fetchone()[0]
    print(f"✓ Total rows in PostgreSQL table: {count:,}")

    # Sample & schema (unchanged)
    if args.sample_rows:
        print(f"\n Sample rows (first {args.sample_rows}):")
        rows = con.execute(f"SELECT * FROM pg.{args.table} LIMIT {args.sample_rows}").fetchall()
        cols = [desc[0] for desc in con.description]
        print("  " + " | ".join(cols))
        print("  " + "-" * (len(" | ".join(cols))))
        for row in rows:
            print("  " + " | ".join(str(val)[:50] for val in row))

    print(f"\n Table schema:")
    schema = con.execute(f"""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = '{args.table}'
    """).fetchall()
    for col_name, col_type in schema:
        print(f"  {col_name}: {col_type}")

    con.close()


def main(argv):
    args = parse_args(argv)
    start = time.perf_counter()
    
    print(f"Starting DuckDB → PostgreSQL CSV ingestion...")
    print(f"   Source: {args.source}")
    print(f"   Target: {args.host}:{args.port}/{args.dbname}.{args.table}")
    if args.limit_rows:
        print(f"   Limit: {args.limit_rows:,} rows")
    if args.threads:
        print(f"   Threads: {args.threads}")
    print()
    
    ingest_with_duckdb_to_postgres(args)
    
    elapsed = time.perf_counter() - start
    print(f"\n✓ Ingestion finished in {elapsed:.2f}s")


if __name__ == "__main__":
    import sys
    main(sys.argv[1:])