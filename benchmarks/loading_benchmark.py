from pathlib import Path
import argparse
import duckdb
import time
import glob
import signal
import sys
import re
import os

# --- keep your parse_args, detect_encoding, group_files_by_encoding as-is --- #
# (I assume you already have those functions from your posted snippet)


def _safe_ident(name: str) -> str:
    """Very small check to avoid injecting unsafe table names into SQL."""
    if not re.match(r'^[A-Za-z_][A-Za-z0-9_]*$', name):
        raise ValueError(f"Unsafe table name: {name!r}")
    return name


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
