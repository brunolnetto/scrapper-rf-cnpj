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

async def record_manifest(conn: asyncpg.Connection, filename: str, status: str, checksum: Optional[bytes], filesize: Optional[int], run_id: str, notes: Optional[str] = None, rows: Optional[int] = None):
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
    types: dict = None
):
    # Generate unique run_id for this file processing session
    run_id = run_id or f"{uuid.uuid4().hex[:8]}_{os.getpid()}"
    filename = os.path.basename(file_path)
    filesize = None
    checksum = None
    
    emit_log("file_processing_started", run_id=run_id, filename=filename, file_path=file_path)
    try:
        with open(file_path, "rb") as f:
            data = f.read()
            filesize = len(data)
            checksum = hashlib.sha256(data).digest()
    except Exception:
        pass

    async with pool.acquire() as conn:
        await conn.execute(base.ensure_table_sql(table, headers, base.map_types(headers, types), primary_keys))
        # Ensure manifest table exists using centralized schema
        await base.ensure_manifest_table(conn)

    async with pool.acquire() as conn:
        rows_total = 0
        batch_idx = 0
        for batch in batch_gen(file_path, headers, chunk_size):
            # split batch into sub-batches to limit memory usage
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
                        rows_total += len(sub_batch)
                        emit_log("batch_committed", run_id=run_id, batch_idx=batch_idx, rows=len(sub_batch))
                        break
                    except (asyncpg.PostgresError, OSError) as e:
                        emit_log("batch_error", run_id=run_id, batch_idx=batch_idx, attempt=attempt, error=str(e))
                        if attempt + 1 >= max_retries:
                            await record_manifest(conn, filename, "failed", checksum, filesize, run_id, notes=str(e), rows=rows_total)
                            raise
                        backoff = 2 ** attempt + random.uniform(0, 0.5)
                        await asyncio.sleep(backoff)

                batch_idx += 1

        emit_log("file_completed", run_id=run_id, filename=filename, rows=rows_total)
        await record_manifest(conn, filename, "success", checksum, filesize, run_id, rows=rows_total)
