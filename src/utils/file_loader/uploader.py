"""
uploader.py
High-performance async uploader for PostgreSQL with internal parallelism.
Adapted from lab/refactored_fileloader for production use.
"""
import asyncio
import asyncpg
import os
import uuid
import hashlib
import logging
from typing import Callable, List, Optional, Iterable, Tuple, Dict, Any

from ...setup.logging import logger

async def record_manifest(conn: asyncpg.Connection, filename: str, status: str, 
                         checksum: Optional[bytes], filesize: Optional[int], 
                         run_id: str, notes: Optional[str] = None, rows: Optional[int] = None):
    """Record file processing manifest for audit trail."""
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

def ensure_table_sql(table: str, headers: List[str], types: Dict[str, str], primary_keys: List[str]) -> str:
    """Generate SQL to ensure table exists with correct schema."""
    # Create columns with types
    columns = []
    for header in headers:
        col_type = types.get(header, 'TEXT')
        columns.append(f'"{header}" {col_type}')
    
    # Add primary key constraint
    pk_constraint = f"PRIMARY KEY ({', '.join(f'\"{pk}\"' for pk in primary_keys)})"
    columns.append(pk_constraint)
    
    return f"CREATE TABLE IF NOT EXISTS {table} ({', '.join(columns)})"

def map_types(headers: List[str], types: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    """Map headers to PostgreSQL types."""
    if types:
        return types
    
    # Default all to TEXT
    return {header: 'TEXT' for header in headers}

async def ensure_manifest_table(conn: asyncpg.Connection):
    """Ensure manifest table exists."""
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS ingestion_manifest (
            filename TEXT PRIMARY KEY,
            checksum BYTEA,
            filesize INTEGER,
            processed_at TIMESTAMP DEFAULT NOW(),
            rows INTEGER,
            status TEXT,
            run_id TEXT,
            notes TEXT
        )
    """)

class AsyncFileUploader:
    """High-performance async file uploader with internal parallelism."""
    
    def __init__(self, pool: asyncpg.Pool, enable_parallel: bool = False, 
                 internal_concurrency: int = 3, sub_batch_size: int = 5000):
        self.pool = pool
        self.enable_parallel = enable_parallel
        self.internal_concurrency = internal_concurrency
        self.sub_batch_size = sub_batch_size
        
    async def upload_file(self, file_path: str, headers: List[str], table: str,
                         primary_keys: List[str], batch_generator: Callable,
                         chunk_size: int = 50_000, max_retries: int = 3,
                         types: Optional[Dict[str, str]] = None) -> int:
        """
        Upload file with high-performance async processing.
        
        Args:
            file_path: Path to the file to upload
            headers: Column headers
            table: Target table name  
            primary_keys: Primary key columns
            batch_generator: Function that yields batches of data
            chunk_size: Size of main batches
            max_retries: Maximum retry attempts
            types: Column type mappings
            
        Returns:
            Number of rows processed
        """
        run_id = f"{uuid.uuid4().hex[:8]}_{os.getpid()}"
        filename = os.path.basename(file_path)
        filesize = None
        checksum = None
        
        logger.info(f"[AsyncUploader] Starting upload: {filename} (parallel: {self.enable_parallel})")
        
        # Calculate file metadata
        try:
            with open(file_path, "rb") as f:
                data = f.read()
                filesize = len(data)
                checksum = hashlib.sha256(data).digest()
        except Exception as e:
            logger.warning(f"Could not calculate file metadata: {e}")

        # Ensure table and manifest exist
        async with self.pool.acquire() as conn:
            table_sql = ensure_table_sql(table, headers, map_types(headers, types), primary_keys)
            await conn.execute(table_sql)
            await ensure_manifest_table(conn)

        # Process file
        async with self.pool.acquire() as conn:
            rows_total = 0
            batch_idx = 0
            
            # Create semaphore for internal concurrency if enabled
            internal_semaphore = asyncio.Semaphore(self.internal_concurrency) if self.enable_parallel else None
            
            for batch in batch_generator(file_path, headers, chunk_size):
                logger.debug(f"[AsyncUploader] Processing batch {batch_idx}, size: {len(batch)}")
                
                if self.enable_parallel:
                    # Process sub-batches in parallel
                    rows_processed = await self._process_batch_parallel(
                        batch, table, headers, primary_keys, run_id, batch_idx, 
                        max_retries, internal_semaphore, types
                    )
                else:
                    # Process sub-batches sequentially
                    rows_processed = await self._process_batch_sequential(
                        conn, batch, table, headers, primary_keys,
                        run_id, batch_idx, max_retries, types
                    )
                
                rows_total += rows_processed
                batch_idx += 1

            logger.info(f"[AsyncUploader] Completed: {filename}, rows: {rows_total}")
            await record_manifest(conn, filename, "success", checksum, filesize, run_id, rows=rows_total)
        
        return rows_total

    async def _process_batch_parallel(self, batch: List[Tuple], table: str, headers: List[str],
                                    primary_keys: List[str], run_id: str, batch_idx: int,
                                    max_retries: int, semaphore: asyncio.Semaphore,
                                    types: Optional[Dict[str, str]]) -> int:
        """Process batch with internal parallelism."""
        
        # Split batch into sub-batches
        sub_batches = [
            batch[i:i + self.sub_batch_size] 
            for i in range(0, len(batch), self.sub_batch_size)
        ]
        
        async def process_sub_batch(sub_batch_idx: int, sub_batch: List[Tuple]) -> int:
            async with semaphore:
                async with self.pool.acquire() as conn:
                    return await self._upsert_sub_batch(
                        conn, sub_batch, table, headers, primary_keys,
                        f"{run_id}_b{batch_idx}_sb{sub_batch_idx}", max_retries, types
                    )
        
        # Process all sub-batches concurrently
        tasks = [
            process_sub_batch(idx, sub_batch) 
            for idx, sub_batch in enumerate(sub_batches)
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Handle results and exceptions
        total_rows = 0
        for idx, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Sub-batch {idx} failed: {result}")
                raise result
            total_rows += result
        
        return total_rows

    async def _process_batch_sequential(self, conn: asyncpg.Connection, batch: List[Tuple],
                                      table: str, headers: List[str], primary_keys: List[str],
                                      run_id: str, batch_idx: int, max_retries: int,
                                      types: Optional[Dict[str, str]]) -> int:
        """Process batch sequentially."""
        
        # Split batch into sub-batches
        sub_batches = [
            batch[i:i + self.sub_batch_size] 
            for i in range(0, len(batch), self.sub_batch_size)
        ]
        
        total_rows = 0
        for sub_batch_idx, sub_batch in enumerate(sub_batches):
            rows = await self._upsert_sub_batch(
                conn, sub_batch, table, headers, primary_keys,
                f"{run_id}_b{batch_idx}_sb{sub_batch_idx}", max_retries, types
            )
            total_rows += rows
        
        return total_rows

    async def _upsert_sub_batch(self, conn: asyncpg.Connection, sub_batch: List[Tuple],
                              table: str, headers: List[str], primary_keys: List[str],
                              sub_run_id: str, max_retries: int,
                              types: Optional[Dict[str, str]]) -> int:
        """Upsert a sub-batch of data."""
        
        if not sub_batch:
            return 0
        
        # Generate UPSERT SQL
        columns = ', '.join(f'"{h}"' for h in headers)
        placeholders = ', '.join(f'${i+1}' for i in range(len(headers)))
        
        # Create conflict resolution for UPSERT
        update_columns = [h for h in headers if h not in primary_keys]
        if update_columns:
            update_clause = ', '.join(f'"{col}" = EXCLUDED."{col}"' for col in update_columns)
            conflict_clause = f'ON CONFLICT ({", ".join(f\'"{pk}"\' for pk in primary_keys)}) DO UPDATE SET {update_clause}'
        else:
            conflict_clause = f'ON CONFLICT ({", ".join(f\'"{pk}"\' for pk in primary_keys)}) DO NOTHING'
        
        sql = f"""
            INSERT INTO {table} ({columns})
            VALUES ({placeholders})
            {conflict_clause}
        """
        
        # Execute with retries
        for attempt in range(max_retries):
            try:
                await conn.executemany(sql, sub_batch)
                return len(sub_batch)
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Sub-batch upsert attempt {attempt + 1} failed: {e}, retrying...")
                    await asyncio.sleep(0.1 * (attempt + 1))  # Exponential backoff
                else:
                    logger.error(f"Sub-batch upsert failed after {max_retries} attempts: {e}")
                    raise
        
        return 0
