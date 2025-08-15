import asyncio
import os
import uuid
from pathlib import Path
import time
import asyncpg
import polars as pl
from tqdm.asyncio import tqdm_asyncio

# ----------------------------
# Helpers
# ----------------------------
async def ensure_table_from_headers(pg_dsn, table_name: str, headers: list[str], primary_keys: list[str]):
    conn = await asyncpg.connect(pg_dsn)
    await conn.execute(f'DROP TABLE IF EXISTS "{table_name}"')
    col_defs = [f'"{h}" TEXT PRIMARY KEY' if h in primary_keys else f'"{h}" TEXT' for h in headers]
    await conn.execute(f'CREATE TABLE "{table_name}" ({", ".join(col_defs)})')
    await conn.close()

async def process_chunk(pg_dsn, table_name, primary_keys, headers, df_chunk: pl.DataFrame):
    # Cast all columns to string
    df_chunk = df_chunk.select([pl.col(h).cast(str) for h in headers])

    tmp_table = f"tmp_csv_{os.getpid()}_{uuid.uuid4().hex[:8]}"
    col_defs_tmp = [f'"{h}" TEXT' for h in headers]

    conn = await asyncpg.connect(pg_dsn)
    await conn.execute(f'CREATE TEMP TABLE "{tmp_table}" ({", ".join(col_defs_tmp)})')

    # Convert to list of dicts for async copy
    records = list(df_chunk.to_dicts())
    await conn.copy_records_to_table(tmp_table, records=records, columns=headers)

    pk_list = ", ".join(f'"{c}"' for c in primary_keys)
    collist = ", ".join(f'"{c}"' for c in headers)
    set_clause = ", ".join(f'"{c}"=EXCLUDED."{c}"' for c in headers if c not in primary_keys)

    await conn.execute(
        f'INSERT INTO "{table_name}" ({collist}) '
        f'SELECT DISTINCT ON ({pk_list}) {collist} '
        f'FROM "{tmp_table}" '
        f'ORDER BY {pk_list} '
        f'ON CONFLICT ({pk_list}) DO UPDATE SET {set_clause}'
    )

    await conn.close()
    return len(records)

async def process_csv_file(pg_dsn, table_name, primary_keys, headers, csv_path, chunk_size, semaphore, lock, total_rows):
    """Process one CSV lazily in chunks."""
    async def sem_worker(df_chunk):
        async with semaphore:
            n = await process_chunk(pg_dsn, table_name, primary_keys, headers, df_chunk)
            async with lock:
                total_rows[0] += n
            return n

    # Lazy read
    lazy_df = pl.scan_csv(
        csv_path,
        separator=";",
        encoding="utf8-lossy",
        schema_overrides={h: pl.Utf8 for h in headers},
    ).sort(primary_keys, descending=True).unique(subset=primary_keys)

    # Collect in manageable chunks and push to DB
    for df_chunk in lazy_df.fetch(chunk_size):
        await sem_worker(df_chunk)

# ----------------------------
# Async CSV upsert with concurrency
# ----------------------------
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
    await ensure_table_from_headers(pg_dsn, table_name, headers, primary_keys)
    total_rows = [0]
    lock = asyncio.Lock()
    chunk_semaphore = asyncio.Semaphore(chunk_concurrency)
    file_semaphore = asyncio.Semaphore(file_concurrency)

    async def file_worker(csv_path):
        async with file_semaphore:
            await process_csv_file(pg_dsn, table_name, primary_keys, headers, csv_path, chunk_size, chunk_semaphore, lock, total_rows)

    tasks = [asyncio.create_task(file_worker(f)) for f in csv_paths]
    for f in tqdm_asyncio.as_completed(tasks, total=len(tasks), desc="Overall CSV progress"):
        await f

    print(f"🎉 Upsert completed: {total_rows[0]} rows")
    return total_rows[0]

# ----------------------------
# Main
# ----------------------------
if __name__ == "__main__":
    pg_dsn = "postgresql://postgres:postgres@localhost/benchmark_db"
    csv_folder = Path("/mnt/c/Users/SuasVendas/github/scrapper-rf-cnpj/data/EXTRACTED_FILES")
    csv_files = sorted(csv_folder.glob("*.SOCIOCSV"))

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
