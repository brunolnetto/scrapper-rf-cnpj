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
from .file_loader import FileLoader
from .uploader import async_upsert

def parse_args():
    p = argparse.ArgumentParser(description="Unified CSV/Parquet uploader with auto-detection")
    p.add_argument('--dsn', required=True, help='PostgreSQL connection string')
    p.add_argument('files', nargs='+', help='One or more file paths (supports glob patterns)')
    p.add_argument('--file-type', choices=['csv', 'parquet'], 
                   help='File type (auto-detected if not specified)')
    p.add_argument('--table', required=True, help='Target database table')
    p.add_argument('--pk', help='Comma-separated primary key columns (auto-detected if not specified)')
    p.add_argument('--headers', help='Comma-separated header list in desired order (auto-detected if not specified)')
    p.add_argument('--batch-size', type=int, default=50000, help='Records per batch')
    p.add_argument(
        "--sub-batch-size",
        type=int,
        default=5_000,
        help="Sub-batch size for internal streaming to avoid large memory spikes"
    )
    p.add_argument('--concurrency', type=int, default=4, help='File-level concurrency')
    p.add_argument('--internal-concurrency', type=int, default=2, 
                   help='Sub-batch concurrency within each file')
    p.add_argument('--disable-internal-parallelism', action='store_true',
                   help='Disable parallel processing within files')
    p.add_argument('--max-retries', type=int, default=3, help='Retry attempts for failed operations')
    return p.parse_args()

async def run(args):
    # Handle optional headers and primary keys
    headers = None
    pks = None
    
    if args.headers:
        headers = [h.strip() for h in args.headers.split(',')]
    if args.pk:
        pks = [p.strip() for p in args.pk.split(',')]
    
    # Auto-detect file types if not specified
    if args.file_type:
        # Use specified file type for all files
        batch_gen = csv_batch_gen if args.file_type == 'csv' else parquet_batch_gen
        print(f"Using specified file type: {args.file_type}")
    else:
        # Auto-detect format - will be determined per file
        batch_gen = None
        print("Auto-detecting file formats...")
    
    # Determine parallelism settings
    enable_internal_parallelism = not args.disable_internal_parallelism
    
    if enable_internal_parallelism:
        print(f"Internal parallelism: ENABLED (concurrency={args.internal_concurrency})")
    else:
        print("Internal parallelism: DISABLED")
    
    # Set pool size to accommodate both file-level and internal concurrency
    if enable_internal_parallelism:
        max_pool_size = max(args.concurrency * args.internal_concurrency + 5, 10)
    else:
        max_pool_size = max(args.concurrency + 2, 5)
    
    print(f"Database pool size: {max_pool_size}")
    pool = await create_pool(args.dsn, min_size=1, max_size=max_pool_size)
    
    # Semaphore to limit concurrent file processing to respect pool limits
    semaphore = asyncio.Semaphore(args.concurrency)
    
    async def process_file(file_path):
        """Process a single file with semaphore protection and auto-detection."""
        async with semaphore:
            # Capture start time right when we acquire the semaphore
            start_time = time.time()
            start_timestamp = time.strftime('%H:%M:%S')
            filename = os.path.basename(file_path)
            
            # Initialize local variables for this file
            file_headers = headers  # Use global headers if provided
            file_pks = pks  # Use global pks if provided
            
            # Determine batch generator and get file info
            if batch_gen:
                # Use specified batch generator
                file_batch_gen = batch_gen
                file_format = args.file_type
                
                # For manual mode, we need headers and pks
                if not file_headers or not file_pks:
                    print(f"[{start_timestamp}] ✗ Skipped: {filename} - Manual mode requires --headers and --pk")
                    return None
                    
                # Create the batch generator with headers
                batch_generator = lambda path, h, chunk_size: file_batch_gen(path, h, chunk_size)
            else:
                # Auto-detect format and create appropriate batch generator
                try:
                    file_format = FileLoader.detect_file_format(file_path)
                    file_batch_gen = FileLoader.get_batch_generator_for_file(file_path)
                    print(f"[{start_timestamp}] Detected format: {file_format} for {filename}")
                    
                    # For auto-detection, use default headers and primary key if not provided
                    if not file_headers:
                        # Auto-detect headers from first file
                        if file_format == 'csv':
                            import csv
                            with open(file_path, 'r', encoding='utf-8') as f:
                                reader = csv.reader(f)
                                file_headers = next(reader)
                        elif file_format == 'parquet':
                            import pyarrow.parquet as pq
                            pf = pq.ParquetFile(file_path)
                            file_headers = pf.schema.names
                        print(f"[{start_timestamp}] Auto-detected headers: {file_headers}")
                    
                    if not file_pks:
                        # Use first column as default primary key
                        file_pks = [file_headers[0]] if file_headers else ['id']
                        print(f"[{start_timestamp}] Using default primary key: {file_pks}")
                    
                    # Create the batch generator with headers
                    batch_generator = lambda path, h, chunk_size: file_batch_gen(path, h, chunk_size)
                    
                except ValueError as e:
                    print(f"[{start_timestamp}] ✗ Format detection failed: {filename} - {e}")
                    return None
            
            try:
                print(f"[{start_timestamp}] Starting: {filename} ({file_format})")
                
                # Use enhanced uploader with configurable internal parallelism
                rows_processed = await async_upsert(
                    pool, 
                    file_path,
                    file_headers,
                    args.table,
                    file_pks,
                    batch_generator,
                    chunk_size=args.batch_size,
                    sub_batch_size=args.sub_batch_size,
                    max_retries=args.max_retries,
                    enable_internal_parallelism=enable_internal_parallelism,
                    internal_concurrency=args.internal_concurrency
                )
                elapsed = time.time() - start_time
                end_timestamp = time.strftime('%H:%M:%S')
                print(f"[{end_timestamp}] ✓ Completed: {filename} ({rows_processed} rows, {elapsed:.1f}s)")
                return elapsed  # Return elapsed time for aggregation
            except Exception as e:
                elapsed = time.time() - start_time
                end_timestamp = time.strftime('%H:%M:%S')
                print(f"[{end_timestamp}] ✗ Failed: {filename} ({elapsed:.1f}s) - {e}")
                return None  # Return None for failed processing
    
    try:
        # Create tasks for concurrent file processing
        print(f"Processing {len(args.files)} files with concurrency={args.concurrency}")
        overall_start_time = time.time()
        
        tasks = [process_file(file_path) for file_path in args.files]
        
        # Process all files concurrently, but respect semaphore limits
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Calculate statistics from individual file timings
        successful_timings = [r for r in results if isinstance(r, (int, float)) and r > 0]
        failed_count = sum(1 for r in results if r is None or isinstance(r, Exception))
        successful_count = len(successful_timings)
        
        overall_elapsed = time.time() - overall_start_time
        
        print(f"\n=== Processing Complete ===")
        print(f"Total files: {len(args.files)}")
        print(f"Successful: {successful_count}")
        print(f"Failed: {failed_count}")
        print(f"Total time: {overall_elapsed:.1f}s")
        
        if successful_timings:
            avg_time = sum(successful_timings) / len(successful_timings)
            print(f"Avg time per file: {avg_time:.1f}s")
            print(f"Min time: {min(successful_timings):.1f}s")
            print(f"Max time: {max(successful_timings):.1f}s")
        
        if failed_count > 0:
            print(f"Warning: {failed_count} files failed processing")
            
    finally:
        await pool.close()

def main():
    args = parse_args()
    asyncio.run(run(args))

if __name__ == '__main__':
    main()
