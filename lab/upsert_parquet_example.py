import asyncio
import os
import uuid
from pathlib import Path
import time
import duckdb
import asyncpg
import pyarrow.parquet as pq
from tqdm.asyncio import tqdm_asyncio

def parquet_columns(parquet_path: Path):
    pf = pq.ParquetFile(str(parquet_path))
    return pf.schema.names

async def ensure_table_from_parquet(pg_dsn, table_name: str, parquet_path: Path, primary_keys: list[str]):
    cols = parquet_columns(parquet_path)
    if not cols:
        print("⚠️  No columns found in Parquet file.")
        return

    conn = await asyncpg.connect(pg_dsn)
    print(f"🗑 Dropping table if it exists: {table_name}")
    await conn.execute(f'DROP TABLE IF EXISTS "{table_name}"')

    col_defs = [f'"{c}" TEXT PRIMARY KEY' if c in primary_keys else f'"{c}" TEXT' for c in cols]
    col_defs_sql = ", ".join(col_defs)
    print(f"📦 Creating table {table_name} with columns: {cols}")
    await conn.execute(f'CREATE TABLE "{table_name}" ({col_defs_sql})')
    await conn.close()

async def process_chunk(pg_dsn, table_name, primary_keys, cols, df_chunk):
    tmp_table = f"tmp_parquet_{os.getpid()}_{uuid.uuid4().hex[:8]}"
    cols_defs_tmp = [f'"{c}" TEXT' for c in cols]
    col_defs_sql_tmp = ", ".join(cols_defs_tmp)

    conn = await asyncpg.connect(pg_dsn)
    await conn.execute(f'CREATE TEMP TABLE "{tmp_table}" ({col_defs_sql_tmp})')

    records = df_chunk.to_records(index=False)
    await conn.copy_records_to_table(tmp_table, records=records.tolist(), columns=cols)

    pk_list = ", ".join(f'"{c}"' for c in primary_keys)
    collist = ", ".join(f'"{c}"' for c in cols)
    set_clause = ", ".join(f'"{c}"=EXCLUDED."{c}"' for c in cols if c not in primary_keys)

    await conn.execute(
        f'INSERT INTO "{table_name}" ({collist}) '
        f'SELECT DISTINCT ON ({pk_list}) {collist} '
        f'FROM "{tmp_table}" '
        f'ORDER BY {pk_list} '
        f'ON CONFLICT ({pk_list}) DO UPDATE SET {set_clause}'
    )

    await conn.close()
    return len(df_chunk)

async def async_parquet_upsert(
    pg_dsn: str,
    parquet_path: Path,
    table_name: str,
    primary_keys: list[str],
    chunk_size: int = 200_000,
    concurrency: int = 4
):
    con_duck = duckdb.connect()
    parquet_str = str(parquet_path)

    await ensure_table_from_parquet(pg_dsn, table_name, parquet_path, primary_keys)

    cols = parquet_columns(parquet_path)
    total_count = int(con_duck.execute(f"SELECT COUNT(*) FROM read_parquet('{parquet_str}')").fetch_df().iloc[0,0])
    print(f"🧮 Total rows to process: {total_count}")

    offsets = list(range(0, total_count, chunk_size))
    total_rows = 0
    lock = asyncio.Lock()

    async def worker(offset):
        df = con_duck.execute(
            f"SELECT * FROM read_parquet('{parquet_str}') LIMIT {chunk_size} OFFSET {offset}"
        ).fetch_df()
        if df.empty:
            return 0
        n = await process_chunk(pg_dsn, table_name, primary_keys, cols, df)

        async with lock:
            nonlocal total_rows
            total_rows += n
        return n

    start_time = time.time()
    semaphore = asyncio.Semaphore(concurrency)

    async def sem_worker(off):
        async with semaphore:
            return await worker(off)

    tasks = [sem_worker(off) for off in offsets]
    for f in tqdm_asyncio.as_completed(tasks, total=len(tasks)):
        await f
        elapsed = time.time() - start_time

    con_duck.close()
    print(f"🎉 Upsert completed: {total_rows} rows in {time.time() - start_time:.1f}s")
    return total_rows

if __name__ == '__main__':
    pg_dsn = "postgresql://postgres:postgres@localhost/benchmark_db"
    file_path = Path("/mnt/c/Users/SuasVendas/github/scrapper-rf-cnpj/data/CONVERTED_FILES/simples.parquet")
    table_name = "bench_table_parquet"
    primary_keys = ["cnpj_basico"]
    inserted_rows = asyncio.run(async_parquet_upsert(pg_dsn, file_path, table_name, primary_keys, concurrency=4))
    print(f"Total rows upserted: {inserted_rows}")
