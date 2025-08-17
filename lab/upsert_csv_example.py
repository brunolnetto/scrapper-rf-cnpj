import asyncio
import csv
import io
import os
import uuid
import time
from pathlib import Path
from typing import List

import asyncpg  # used for table creation earlier if you prefer; not required here
import psycopg2
from tqdm.asyncio import tqdm_asyncio

# --- helper to detect delimiter/header (simple and robust) ---
def sniff_delim_and_header(path: Path, expected_headers: List[str], n_lines: int = 16):
    # Read first n_lines raw
    with open(path, "r", encoding="utf-8", errors="replace", newline="") as f:
        head = [f.readline() for _ in range(n_lines)]
    head = [h for h in head if h]

    if not head:
        raise RuntimeError("Empty file")

    candidates = [';', ',', '\t', '|']
    best = ( -1, ';', 0, [] )  # (matches, delim, header_idx, tokens)
    expected = set(expected_headers)

    for delim in candidates:
        for idx, line in enumerate(head):
            tokens = [t.strip().strip('"').strip("'") for t in line.rstrip("\n").split(delim)]
            matches = sum(1 for t in tokens if t in expected)
            if matches > best[0]:
                best = (matches, delim, idx, tokens)

    matches, delim, header_idx, tokens = best
    header_line = head[header_idx].rstrip("\n")
    actual_cols = [t.strip().strip('"').strip("'") for t in header_line.split(delim)]
    return delim, header_idx, actual_cols

# --- main worker using COPY FROM STDIN (psycopg2) ---
def _copy_chunk_to_temp(conn, tmp_table, cols, csv_text: str, delim: str):
    """
    conn: psycopg2 connection (blocking)
    tmp_table: name (already created)
    cols: list of column names in order
    csv_text: CSV bytes/text containing rows (NO header)
    """
    cur = conn.cursor()
    quoted_cols = [f'"{c}"' for c in cols]
    cols_sql = ", ".join(quoted_cols)
    copy_sql = f"COPY {tmp_table} ({cols_sql}) FROM STDIN WITH CSV DELIMITER '{delim}'"

    # psycopg2's copy_expert expects a file-like object; provide a StringIO
    buf = io.StringIO(csv_text)
    cur.copy_expert(copy_sql, buf)
    conn.commit()
    cur.close()

async def process_csv_file_via_copy(pg_dsn: str, table_name: str, primary_keys: List[str],
                                    headers: List[str], csv_path: Path,
                                    chunk_size: int, semaphore: asyncio.Semaphore,
                                    lock: asyncio.Lock, total_rows):
    """
    Streams file into Postgres via COPY in chunks.
    Uses asyncio.to_thread to run blocking DB copy operations without blocking the loop.
    """
    # sniff delimiter + header
    delim, header_idx, actual_cols = sniff_delim_and_header(csv_path, headers)
    print(f"[INFO] {csv_path.name}: detected delimiter='{delim}', header at line {header_idx}, columns_found={len(actual_cols)}")

    # build mapping from input column index -> target index
    # If file has no header matching expected headers, we still handle: we'll assume input columns number >= len(headers)
    # We'll compute positions for known columns and fill missing with empty
    input_col_to_pos = {name: i for i, name in enumerate(actual_cols)}
    # For safety, if actual_cols seems not to match, we'll treat header as header-less and parse by position - risky.
    use_header = any(h in input_col_to_pos for h in headers)

    # Prepare tmp_table DDL and target columns order
    tmp_table = f"tmp_csv_{os.getpid()}_{uuid.uuid4().hex[:8]}"
    col_defs_tmp = ", ".join(f'"{h}" TEXT' for h in headers)

    # We'll open a psycopg2 connection per file-worker (blocking). Use the same DSN string as asyncpg but psycopg2 format
    # If your pg_dsn is like postgresql://user:pass@host/db, psycopg2 will accept it.
    conn = psycopg2.connect(pg_dsn)
    try:
        cur = conn.cursor()
        cur.execute(f'CREATE TEMP TABLE "{tmp_table}" ({col_defs_tmp})')
        conn.commit()
        cur.close()

        # Read file line-by-line, skipping header lines up to header_idx
        with open(csv_path, "r", encoding="utf-8", errors="replace", newline="") as f:
            # skip lines up to header_idx (we already detected which line is header)
            for _ in range(header_idx + 1):
                next(f, None)

            chunk_rows_for_writer = []
            chunk_count = 0
            total_for_file = 0

            reader = csv.reader(f, delimiter=delim)
            for row in reader:
                # Build row in target order 'headers'
                if use_header:
                    # map by column name where possible; else insert empty string
                    out_row = []
                    for h in headers:
                        pos = input_col_to_pos.get(h)
                        if pos is None or pos >= len(row):
                            out_row.append("")
                        else:
                            out_row.append(row[pos])
                else:
                    # no header mapping — attempt by position (dangerous). We'll pad/truncate
                    out_row = [ row[i] if i < len(row) else "" for i in range(len(headers)) ]

                chunk_rows_for_writer.append(out_row)
                if len(chunk_rows_for_writer) >= chunk_size:
                    # flush chunk to DB via COPY
                    # build CSV text (no header) for this chunk
                    buf = io.StringIO()
                    writer = csv.writer(buf, delimiter=delim, lineterminator="\n", quoting=csv.QUOTE_MINIMAL)
                    writer.writerows(chunk_rows_for_writer)
                    csv_text = buf.getvalue()

                    # actually run copy in a thread to avoid blocking event loop
                    await asyncio.to_thread(_copy_chunk_to_temp, conn, tmp_table, headers, csv_text, delim)

                    # upsert from tmp_table into target
                    await asyncio.to_thread(_upsert_from_tmp, conn, table_name, tmp_table, headers, primary_keys)

                    # truncate tmp_table and continue
                    cur = conn.cursor()
                    cur.execute(f'TRUNCATE TABLE "{tmp_table}"')
                    conn.commit()
                    cur.close()

                    total_for_file += len(chunk_rows_for_writer)
                    async with lock:
                        total_rows[0] += len(chunk_rows_for_writer)

                    chunk_rows_for_writer = []
                    chunk_count += 1

            # flush remaining rows
            if chunk_rows_for_writer:
                buf = io.StringIO()
                writer = csv.writer(buf, delimiter=delim, lineterminator="\n", quoting=csv.QUOTE_MINIMAL)
                writer.writerows(chunk_rows_for_writer)
                csv_text = buf.getvalue()
                await asyncio.to_thread(_copy_chunk_to_temp, conn, tmp_table, headers, csv_text, delim)
                await asyncio.to_thread(_upsert_from_tmp, conn, table_name, tmp_table, headers, primary_keys)
                total_for_file += len(chunk_rows_for_writer)
                async with lock:
                    total_rows[0] += len(chunk_rows_for_writer)

        # cleanup: temp table will be dropped at session end when connection closes
        print(f"[INFO] {csv_path.name} finished: rows streamed: {total_for_file}")
    finally:
        conn.close()

def _upsert_from_tmp(conn, table_name, tmp_table, headers, primary_keys):
    """
    Blocking helper to run upsert from tmp table to final table using the same psycopg2 connection.
    """
    cur = conn.cursor()
    pk_list = ", ".join(f'"{c}"' for c in primary_keys)
    collist = ", ".join(f'"{c}"' for c in headers)
    set_clause = ", ".join(f'"{c}"=EXCLUDED."{c}"' for c in headers if c not in primary_keys)

    upsert_sql = (
        f'INSERT INTO "{table_name}" ({collist}) '
        f'SELECT DISTINCT ON ({pk_list}) {collist} '
        f'FROM "{tmp_table}" '
        f'ORDER BY {pk_list} '
        f'ON CONFLICT ({pk_list}) DO UPDATE SET {set_clause}'
    )
    cur.execute(upsert_sql)
    conn.commit()
    cur.close()


# ----------------------------
# Async CSV upsert with concurrency
# ----------------------------
async def ensure_table_from_headers(pg_dsn, table_name: str, headers: list[str], primary_keys: list[str]):
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP TABLE IF EXISTS "{table_name}"')
        col_defs = [f'"{h}" TEXT PRIMARY KEY' if h in primary_keys else f'"{h}" TEXT' for h in headers]
        await conn.execute(f'CREATE TABLE "{table_name}" ({", ".join(col_defs)})')
    finally:
        await conn.close()

async def async_csv_upsert_multiple_files(
    pg_dsn: str,
    csv_paths: list[Path],
    table_name: str,
    primary_keys: list[str],
    headers: list[str],
    chunk_size: int = 100_000,
    file_concurrency: int = 2,
    chunk_concurrency: int = 4
):
    # Make sure target table exists
    await ensure_table_from_headers(pg_dsn, table_name, headers, primary_keys)

    total_rows = [0]
    lock = asyncio.Lock()
    # file concurrency controls how many files are processed in parallel
    file_semaphore = asyncio.Semaphore(file_concurrency)

    async def file_worker(csv_path: Path):
        async with file_semaphore:
            # CALL THE NEW FUNCTION (was process_csv_file before)
            await process_csv_file_via_copy(
                pg_dsn, table_name, primary_keys, headers, csv_path,
                chunk_size, None, lock, total_rows
            )

    tasks = [asyncio.create_task(file_worker(f)) for f in csv_paths]
    for f in tqdm_asyncio.as_completed(tasks, total=len(tasks), desc="Overall CSV progress"):
        await f

    print(f"🎉 Upsert completed: {total_rows[0]} rows")
    return total_rows[0]

# ----------------------------
# Main
# ----------------------------
if __name__ == "__main__":
    from os import getcwd, path
    pg_dsn = "postgresql://postgres:postgres@localhost/benchmark_db"
    csv_folder = Path(path.join(getcwd(), "data/EXTRACTED_FILES"))
    print(csv_folder)
    csv_files = sorted(csv_folder.glob("*.SOCIOCSV"))
    print(csv_files)

    table_name = "bench_table_csv"
    primary_keys = ['cnpj_basico']
    headers = [
        "cnpj_basico",
        "identificador_socio",
        "nome_socio_razao_social",
        "cpf_cnpj_socio",
        "qualificacao_socio",
        "data_entrada_sociedade",
        "pais",
        "representante_legal",
        "nome_do_representante",
        "qualificacao_representante_legal",
        "faixa_etaria",
    ]

    inserted_rows = asyncio.run(async_csv_upsert_multiple_files(
        pg_dsn, csv_files, table_name, primary_keys, headers,
        chunk_size=50_000, file_concurrency=3, chunk_concurrency=4
    ))
    print(f"Total rows upserted: {inserted_rows}")
