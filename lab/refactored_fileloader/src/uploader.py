# src/uploader.py
import asyncio
import asyncpg
import os
import uuid
import hashlib
import random
import logging
from typing import Callable, List, Optional, Iterable, Tuple

from . import base

logging.basicConfig(level=logging.INFO)

async def record_manifest(conn: asyncpg.Connection, filename: str, status: str, checksum: Optional[bytes], filesize: Optional[int], run_id: str, notes: Optional[str] = None, rows_processed: Optional[int] = None):
    await conn.execute(
        """
        INSERT INTO ingestion_manifest (filename, checksum, filesize, processed_at, rows, status, run_id, notes)
        VALUES ($1, $2, $3, NOW(), $4, $5, $6, $7)
        ON CONFLICT (filename) DO UPDATE
          SET status = EXCLUDED.status,
              notes = EXCLUDED.notes,
              processed_at = NOW(),
              rows = EXCLUDED.rows;
        """,
        filename, checksum, filesize, rows, status, run_id, notes
    )

def emit_log(event: str, **kwargs):
    payload = {"event": event}
    payload.update(kwargs)
    logging.info(payload)

async def async_upsert(
    pool: asyncpg.Pool,
    file_path: str,
    headers: List[str],
    table: str,
    primary_keys: List[str],
    batch_gen: Callable[[str, List[str], int], Iterable[List[Tuple]]],
    chunk_size: int = 50_000,
    sub_batch_size: int = 5_000,  # NEW: size for internal sub-batches
    max_retries: int = 3,
    run_id: Optional[str] = None,
    types: dict = None,
    enable_internal_parallelism: bool = False,  # NEW: Enable parallel sub-batch processing
    internal_concurrency: int = 3  # NEW: Sub-batch concurrency within file
):
    # Generate unique run_id for this file processing session
    run_id = run_id or f"{uuid.uuid4().hex[:8]}_{os.getpid()}"
    filename = os.path.basename(file_path)
    filesize = None
    checksum = None
    
    emit_log("file_processing_started", run_id=run_id, filename=filename, file_path=file_path)
    
    # MEMORY-EFFICIENT: Calculate file size and checksum without loading entire file into memory
    try:
        filesize = os.path.getsize(file_path)
        
        # Skip checksum for very large files to avoid performance issues
        checksum_threshold_mb = int(os.getenv("ETL_CHECKSUM_THRESHOLD_MB", "1000"))  # 1000MB (1GB) default
        checksum_threshold_bytes = checksum_threshold_mb * 1024 * 1024
        if filesize > checksum_threshold_bytes:
            logging.info(f"[PERFORMANCE] Skipping checksum for large file {filename}: {filesize:,} bytes (> {checksum_threshold_mb}MB)")
            checksum = None
        else:
            # Calculate checksum in chunks to avoid memory issues
            sha256 = hashlib.sha256()
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(8192), b""):
                    sha256.update(chunk)
            checksum = sha256.digest()
            logging.info(f"[MEMORY] File {filename}: {filesize:,} bytes, checksum calculated efficiently")
    except Exception as e:
        logging.warning(f"Could not calculate checksum for {filename}: {e}")
        checksum = None

    async with pool.acquire() as conn:
        await conn.execute(base.ensure_table_sql(table, headers, base.map_types(headers, types), primary_keys))
        # Ensure manifest table exists using centralized schema
        await base.ensure_manifest_table(conn)

    async with pool.acquire() as conn:
        rows_total = 0
        batch_idx = 0
        
        # Create semaphore for internal concurrency if enabled
        internal_semaphore = asyncio.Semaphore(internal_concurrency) if enable_internal_parallelism else None
        
        for batch in batch_gen(file_path, headers, chunk_size):
            emit_log("batch_started", run_id=run_id, batch_idx=batch_idx, 
                    batch_size=len(batch), parallel_mode=enable_internal_parallelism)
            
            if enable_internal_parallelism:
                # Process sub-batches in parallel
                rows_processed = await _process_batch_parallel(
                    pool, batch, sub_batch_size, table, headers, primary_keys, 
                    run_id, batch_idx, max_retries, internal_semaphore, types
                )
            else:
                # Process sub-batches sequentially (original logic)
                rows_processed = await _process_batch_sequential(
                    conn, batch, sub_batch_size, table, headers, primary_keys,
                    run_id, batch_idx, max_retries, types
                )
            
            rows_total += rows_processed
            batch_idx += 1

        emit_log("file_completed", run_id=run_id, filename=filename, rows=rows_total,
                parallel_mode=enable_internal_parallelism)
        # Note: Manifest recording is handled by the audit service at the loading strategy level
        # await record_manifest(conn, filename, "success", checksum, filesize, run_id, rows_processed=rows_total)
    
    return rows_total  # Return the total number of rows processed


async def _process_batch_sequential(
    conn: asyncpg.Connection,
    batch: List[Tuple],
    sub_batch_size: int,
    table: str,
    headers: List[str],
    primary_keys: List[str],
    run_id: str,
    batch_idx: int,
    max_retries: int,
    types: dict = None
) -> int:
    """Process batch sub-batches sequentially (original logic)."""
    rows_processed = 0
    
    for i in range(0, len(batch), sub_batch_size):
        sub_batch = batch[i:i+sub_batch_size]
        tmp_table = f"tmp_{os.getpid()}_{uuid.uuid4().hex[:8]}"
        types_map = base.map_types(headers, types)
        await conn.execute(base.create_temp_table_sql(tmp_table, headers, types_map))
        emit_log("temp_table_created", run_id=run_id, tmp_table=tmp_table)

        for attempt in range(max_retries):
            try:
                async with conn.transaction():
                    await conn.copy_records_to_table(tmp_table, records=sub_batch, columns=headers)
                    sql = base.upsert_from_temp_sql(table, tmp_table, headers, primary_keys)
                    await conn.execute(sql)
                    await conn.execute(f'TRUNCATE {base.quote_ident(tmp_table)};')
                rows_processed += len(sub_batch)
                emit_log("batch_committed", run_id=run_id, batch_idx=batch_idx, rows=len(sub_batch))
                break
            except (asyncpg.PostgresError, OSError) as e:
                emit_log("batch_error", run_id=run_id, batch_idx=batch_idx, attempt=attempt, error=str(e))
                if attempt + 1 >= max_retries:
                    raise
                backoff = 2 ** attempt + random.uniform(0, 0.5)
                await asyncio.sleep(backoff)
    
    return rows_processed


async def _process_batch_parallel(
    pool: asyncpg.Pool,
    batch: List[Tuple],
    sub_batch_size: int,
    table: str,
    headers: List[str],
    primary_keys: List[str],
    run_id: str,
    batch_idx: int,
    max_retries: int,
    semaphore: asyncio.Semaphore,
    types: dict = None
) -> int:
    """
    Process batch sub-batches in parallel with upsert safety.
    
    NOTE: While sub-batches are prepared in parallel, the final upsert operations
    are serialized to prevent race conditions with overlapping primary keys.
    """
    import time
    
    # Split batch into sub-batches
    sub_batches = []
    for i in range(0, len(batch), sub_batch_size):
        sub_batch = batch[i:i+sub_batch_size]
        sub_batches.append((sub_batch, i // sub_batch_size))
    
    # Create a serialization lock for upsert operations
    upsert_lock = asyncio.Lock()
    
    async def process_sub_batch(sub_batch_data):
        sub_batch, sub_batch_idx = sub_batch_data
        
        async with semaphore:  # Control concurrency
            async with pool.acquire() as conn:
                tmp_table = f"tmp_{os.getpid()}_{uuid.uuid4().hex[:8]}"
                types_map = base.map_types(headers, types)
                
                try:
                    await conn.execute(base.create_temp_table_sql(tmp_table, headers, types_map))
                    emit_log("temp_table_created", run_id=run_id, tmp_table=tmp_table, 
                            sub_batch_idx=sub_batch_idx, parallel=True)

                    for attempt in range(max_retries):
                        try:
                            # Prepare data in parallel (this part is safe)
                            async with conn.transaction():
                                await conn.copy_records_to_table(tmp_table, records=sub_batch, columns=headers)
                            
                            # Serialize the upsert operation to prevent race conditions
                            async with upsert_lock:
                                async with conn.transaction():
                                    sql = base.upsert_from_temp_sql(table, tmp_table, headers, primary_keys)
                                    await conn.execute(sql)
                                    await conn.execute(f'TRUNCATE {base.quote_ident(tmp_table)};')
                            
                            emit_log("sub_batch_committed", run_id=run_id, batch_idx=batch_idx, 
                                    sub_batch_idx=sub_batch_idx, rows=len(sub_batch), parallel=True, serialized=True)
                            return len(sub_batch)  # Return processed row count
                            
                        except (asyncpg.PostgresError, OSError) as e:
                            emit_log("sub_batch_error", run_id=run_id, batch_idx=batch_idx, 
                                    sub_batch_idx=sub_batch_idx, attempt=attempt, error=str(e), parallel=True)
                            if attempt + 1 >= max_retries:
                                raise
                            backoff = 2 ** attempt + random.uniform(0, 0.5)
                            await asyncio.sleep(backoff)
                
                finally:
                    # Cleanup temp table
                    try:
                        await conn.execute(f'DROP TABLE IF EXISTS {base.quote_ident(tmp_table)};')
                    except:
                        pass  # Ignore cleanup errors
    
    # Execute sub-batches concurrently
    batch_start_time = time.time()
    results = await asyncio.gather(*[process_sub_batch(sb) for sb in sub_batches])
    batch_elapsed = time.time() - batch_start_time
    
    # Aggregate results
    total_rows = sum(r for r in results if r is not None)
    
    emit_log("parallel_batch_completed", run_id=run_id, batch_idx=batch_idx, 
            rows=total_rows, sub_batches=len(sub_batches), 
            elapsed_time=batch_elapsed)
    
    return total_rows
