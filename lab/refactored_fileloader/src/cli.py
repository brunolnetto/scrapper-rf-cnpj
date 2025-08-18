# ingestion/cli.py
import argparse
import asyncio
import os
import time
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
    
    # Set pool size to accommodate concurrent file processing
    # Reserve 1-2 connections for other operations, rest for file processing
    max_pool_size = max(args.concurrency + 2, 5)
    pool = await create_pool(args.dsn, min_size=1, max_size=max_pool_size)
    
    # Semaphore to limit concurrent file processing to respect pool limits
    semaphore = asyncio.Semaphore(args.concurrency)
    
    async def process_file(file_path):
        """Process a single file with semaphore protection."""
        async with semaphore:
            start_time = time.time()
            try:
                print(f"[{time.strftime('%H:%M:%S')}] Starting: {os.path.basename(file_path)}")
                await async_upsert(
                    pool, file_path, 
                    headers, 
                    args.table, 
                    pks, 
                    batch_gen, 
                    chunk_size=args.chunk_size, 
                    max_retries=args.max_retries
                )
                elapsed = time.time() - start_time
                print(f"[{time.strftime('%H:%M:%S')}] ✓ Completed: {os.path.basename(file_path)} ({elapsed:.1f}s)")
                return True
            except Exception as e:
                elapsed = time.time() - start_time
                print(f"[{time.strftime('%H:%M:%S')}] ✗ Failed: {os.path.basename(file_path)} ({elapsed:.1f}s) - {e}")
                return False
    
    try:
        # Create tasks for concurrent file processing
        print(f"Processing {len(args.files)} files with concurrency={args.concurrency}")
        start_time = time.time()
        
        tasks = [process_file(file_path) for file_path in args.files]
        
        # Process all files concurrently, but respect semaphore limits
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Count successes and failures
        successes = sum(1 for r in results if r is True)
        failures = len(results) - successes
        total_time = time.time() - start_time
        
        print(f"\n=== Processing Complete ===")
        print(f"Total files: {len(args.files)}")
        print(f"Successful: {successes}")
        print(f"Failed: {failures}")
        print(f"Total time: {total_time:.1f}s")
        print(f"Avg time per file: {total_time/len(args.files):.1f}s")
        
        if failures > 0:
            print(f"Warning: {failures} files failed processing")
            
    finally:
        await pool.close()

def main():
    args = parse_args()
    asyncio.run(run(args))

if __name__ == '__main__':
    main()
