# ingestion/cli.py
import argparse
import asyncio
import os
from .base import create_pool
from .ingestors import (
    batch_generator_csv as csv_batch_gen, 
    batch_generator_parquet as parquet_batch_gen,
)
from .uploader import async_upsert

def parse_args():
    p = argparse.ArgumentParser(description="Unified CSV/Parquet uploader")
    p.add_argument('--dsn', required=True)
    p.add_argument('--files', required=True, nargs='+', help='One or more file paths')
    p.add_argument('--file-type', choices=['csv', 'parquet'], required=True)
    p.add_argument('--table', required=True)
    p.add_argument('--pk', required=True, help='Comma-separated primary key columns (e.g. id)')
    p.add_argument('--headers', required=True, help='Comma-separated header list in desired order')
    p.add_argument('--chunk-size', type=int, default=50000)
    p.add_argument(
        "--sub-batch-size",
        type=int,
        default=5_000,
        help="Sub-batch size for internal streaming to avoid large memory spikes"
    )
    p.add_argument('--concurrency', type=int, default=1)
    p.add_argument('--max-retries', type=int, default=3)
    return p.parse_args()

async def run(args):
    headers = [h.strip() for h in args.headers.split(',')]
    pks = [p.strip() for p in args.pk.split(',')]
    batch_gen = csv_batch_gen if args.file_type == 'csv' else parquet_batch_gen
    pool = await create_pool(args.dsn, min_size=1, max_size=args.concurrency)
    try:
        # process files sequentially here; you can parallelize by creating tasks that call async_upsert with pool
        for file_path in args.files:
            await async_upsert(
                pool, file_path, 
                headers, 
                args.table, 
                pks, 
                batch_gen, 
                chunk_size=args.chunk_size, 
                max_retries=args.max_retries
            )
    finally:
        await pool.close()

def main():
    args = parse_args()
    asyncio.run(run(args))

if __name__ == '__main__':
    main()
