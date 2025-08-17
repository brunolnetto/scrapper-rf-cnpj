import asyncio
import os
import uuid
import time
from pathlib import Path
from typing import List

import asyncpg
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm.asyncio import tqdm_asyncio


def parquet_columns(parquet_path: Path) -> List[str]:
    pf = pq.ParquetFile(str(parquet_path))
    return pf.schema.names


async def ensure_table_from_parquet(pg_dsn: str, table_name: str, parquet_path: Path, primary_keys: List[str]):
    cols = parquet_columns(parquet_path)
    if not cols:
        raise RuntimeError("No columns found in Parquet file.")

    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP TABLE IF EXISTS "{table_name}"')
        col_defs = [f'"{c}" TEXT PRIMARY KEY' if c in primary_keys else f'"{c}" TEXT' for c in cols]
        await conn.execute(f'CREATE TABLE "{table_name}" ({", ".join(col_defs)})')
    finally:
        await conn.close()


async def _process_batches_for_worker(pg_dsn: str, table_name: str, primary_keys: list, cols: list,
                                      batches_iter, tmp_table_name: str):
    """
    Reuse a single DB connection and a single temp table per worker.
    batches_iter: synchronous iterable yielding (nrows, records).
    tmp_table_name: plain identifier (no quotes)
    """
    conn = await asyncpg.connect(pg_dsn)
    try:
        # Create a persistent temp table for the session (do NOT use ON COMMIT DROP)
        col_defs_tmp = ", ".join(f'"{c}" TEXT' for c in cols)
        # Use IF NOT EXISTS to be idempotent during retries
        await conn.execute(f'CREATE TEMP TABLE IF NOT EXISTS "{tmp_table_name}" ({col_defs_tmp})')

        total = 0
        pk_list = ", ".join(f'"{c}"' for c in primary_keys)
        collist = ", ".join(f'"{c}"' for c in cols)
        set_clause = ", ".join(f'"{c}"=EXCLUDED."{c}"' for c in cols if c not in primary_keys)

        for nrows, records in batches_iter:
            if not records:
                continue

            # Try copy; if the temp table somehow doesn't exist, recreate and retry once.
            try:
                await conn.copy_records_to_table(tmp_table_name, records=records, columns=cols)
            except asyncpg.exceptions.UndefinedTableError:
                # defensive: recreate temp table and retry once
                print(f"[worker] temp table {tmp_table_name} missing: recreating and retrying copy...")
                await conn.execute(f'CREATE TEMP TABLE IF NOT EXISTS "{tmp_table_name}" ({col_defs_tmp})')
                await conn.copy_records_to_table(tmp_table_name, records=records, columns=cols)

            # upsert from temp table
            await conn.execute(
                f'INSERT INTO "{table_name}" ({collist}) '
                f'SELECT DISTINCT ON ({pk_list}) {collist} '
                f'FROM "{tmp_table_name}" '
                f'ORDER BY {pk_list} '
                f'ON CONFLICT ({pk_list}) DO UPDATE SET {set_clause}'
            )

            # clear temp table for next batch
            await conn.execute(f'TRUNCATE TABLE "{tmp_table_name}"')
            total += nrows

        return total
    finally:
        await conn.close()


async def async_parquet_upsert(
    pg_dsn: str,
    parquet_path: Path,
    table_name: str,
    primary_keys: List[str],
    chunk_size: int = 200_000,
    concurrency: int = 4,
):
    """
    Stream Parquet file by row group, then by chunk_size rows, and upsert to Postgres.
    """

    pf = pq.ParquetFile(str(parquet_path))
    cols = pf.schema.names
    if not cols:
        raise RuntimeError("No columns found in Parquet file.")

    # create target table
    await ensure_table_from_parquet(pg_dsn, table_name, parquet_path, primary_keys)

    # total rows (fast)
    total_rows = pf.metadata.num_rows
    print(f"🧮 Total rows in parquet: {total_rows}")
    num_row_groups = pf.num_row_groups
    print(f"🔢 Row groups: {num_row_groups}")

    # A coroutine generator that yields batches (nrows, records_as_tuples) reading row groups and slicing to chunk_size
    def batches_generator(pf: pq.ParquetFile, cols, row_group_indices, chunk_size):
        """
        Synchronous generator that yields (nrows, records_as_list_of_tuples).
        Reads row groups from pf and slices into chunk_size sub-tables.
        """
        for rg in row_group_indices:
            table: pa.Table = pf.read_row_group(rg, columns=cols)
            n_rg = table.num_rows
            if n_rg == 0:
                continue

            start = 0
            while start < n_rg:
                take = min(chunk_size, n_rg - start)
                sub = table.slice(start, take)  # pa.Table
                # Convert columns to python lists
                col_lists = [sub.column(i).to_pylist() for i in range(sub.num_columns)]
                # Create tuples by zipping columns
                records = list(zip(*col_lists))
                yield (len(records), records)
                start += take


    # Partition row groups among workers (round-robin)
    workers_row_groups = [[] for _ in range(concurrency)]
    for i in range(num_row_groups):
        workers_row_groups[i % concurrency].append(i)

    # Only make as many workers as we actually need
    worker_partitions = [rgs for rgs in workers_row_groups if rgs]
    num_workers = len(worker_partitions)
    print(f"Starting {num_workers} parquet worker(s) (concurrency requested: {concurrency})")

    # Semaphore is optional here; since we spawn exactly num_workers tasks <= concurrency it's redundant,
    # but keep it if you later want more fine-grained control
    semaphore = asyncio.Semaphore(concurrency)

    async def worker_task(worker_id: int, rg_list: list[int]):
        tmp_table = f"tmp_parquet_{os.getpid()}_{uuid.uuid4().hex[:8]}_{worker_id}"
        # batches_generator(pf, cols, rg_list, chunk_size) is a synchronous generator yielding (nrows, records)
        batches_iter = batches_generator(pf, cols, rg_list, chunk_size)

        # Keep failures visible and avoid silent swallowing
        try:
            async with semaphore:
                return await _process_batches_for_worker(
                    pg_dsn, table_name, primary_keys, cols, batches_iter, tmp_table
                )
        except Exception:
            # log useful context and re-raise so the main loop knows the task failed
            import traceback
            print(f"[worker {worker_id}] failed while processing row groups: {rg_list}")
            traceback.print_exc()
            raise

    # Launch one task per partition
    tasks = [asyncio.create_task(worker_task(wid, rgs)) for wid, rgs in enumerate(worker_partitions)]

    start_time = time.time()
    total_inserted = 0
    # collect results as they finish, progress shown by tqdm_asyncio
    for ch in tqdm_asyncio.as_completed(tasks, total=len(tasks), desc="Parquet workers"):
        res = await ch  # will propagate exceptions from workers
        total_inserted += res

    elapsed = time.time() - start_time
    print(f"🎉 Upsert completed: {total_inserted} rows in {elapsed:.1f}s")
    return total_inserted


if __name__ == "__main__":
    import sys
    from os import getcwd, path
    

    pg_dsn = "postgresql://postgres:postgres@localhost/benchmark_db"
    file_path = Path(path.join(getcwd(), "data/CONVERTED_FILES/simples.parquet"))
    table_name = "bench_table_parquet"
    primary_keys = ["cnpj_basico"]

    # Adjust chunk_size down if you hit memory issues
    inserted_rows = asyncio.run(async_parquet_upsert(pg_dsn, file_path, table_name, primary_keys, chunk_size=50_000, concurrency=4))
    print(f"Total rows upserted: {inserted_rows}")
