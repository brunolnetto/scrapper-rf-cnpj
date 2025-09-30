"""
memory_optimized_uploader.py
Memory-efficient uploader with integrated monitoring and cleanup.
"""
import asyncio
import asyncpg
import os
import uuid
import random
import gc
from typing import Callable, List, Optional, Iterable, Tuple, Any
from contextlib import asynccontextmanager

from . import base
from .....setup.logging import logger


class FileUploader:
    """Memory-aware uploader with integrated monitoring and cleanup."""
    
    def __init__(self, memory_monitor: Optional[Any] = None):
        self.memory_monitor = memory_monitor
        self.temp_tables_created = set()
        self.active_connections = set()
    
    async def cleanup_temp_tables(self, conn: asyncpg.Connection):
        """Ensure all temporary tables are cleaned up."""
        for temp_table in list(self.temp_tables_created):
            try:
                await conn.execute(f'DROP TABLE IF EXISTS {base.quote_ident(temp_table)};')
                self.temp_tables_created.discard(temp_table)
            except Exception as e:
                logger.warning(f"Failed to cleanup temp table {temp_table}: {e}")
    
    @asynccontextmanager
    async def managed_connection(self, pool: asyncpg.Pool):
        """Context manager for connection lifecycle with cleanup."""
        conn = None
        try:
            conn = await pool.acquire()
            self.active_connections.add(id(conn))
            yield conn
        finally:
            if conn:
                try:
                    await self.cleanup_temp_tables(conn)
                except Exception:
                    pass
                finally:
                    self.active_connections.discard(id(conn))
                    await pool.release(conn)
    
    async def upsert(
        self,
        pool: asyncpg.Pool,
        file_path: str,
        headers: List[str],
        table: str,
        primary_keys: List[str],
        batch_gen: Callable[[str, List[str], int], Iterable[List[Tuple]]],
        chunk_size: int = 30_000,  # Reduced default chunk size
        sub_batch_size: int = 3_000,  # Reduced sub-batch size
        max_retries: int = 3,
        run_id: Optional[str] = None,
        types: dict = None,
        enable_internal_parallelism: bool = False,
        internal_concurrency: int = 2,  # Reduced concurrency
        checksum_threshold_mb: int = 1000
    ):
        """Memory-aware async upsert with integrated monitoring."""
        
        # Generate unique run_id
        run_id = run_id or f"{uuid.uuid4().hex[:8]}_{os.getpid()}"
        filename = os.path.basename(file_path)
        filesize = None
        
        # Memory check before starting
        if self.memory_monitor.should_prevent_processing():
            raise MemoryError("Insufficient memory to start processing")
        
        logger.info(f"[MemoryAware] Starting processing: {filename}")
        
        # Calculate file size efficiently
        try:
            filesize = os.path.getsize(file_path)
            
            # Adjust chunk sizes based on file size and memory constraints
            if self.memory_monitor:
                available_memory = self.memory_monitor.get_available_memory_budget()
                if available_memory < 500:  # Less than 500MB available
                    chunk_size = min(chunk_size, 15_000)
                    sub_batch_size = min(sub_batch_size, 1_500)
                    logger.warning(f"[MemoryAware] Reduced chunk sizes due to memory constraints: "
                                 f"chunk={chunk_size}, sub_batch={sub_batch_size}")
            
            # Skip checksum for large files
            checksum_threshold_bytes = checksum_threshold_mb * 1024 * 1024
            if filesize > checksum_threshold_bytes:
                logger.info(f"[MemoryAware] Skipping checksum for large file: {filesize:,} bytes")
            
            # Disable parallelism for large files or low memory
            if filesize > 2_000_000_000:  # > 2GB
                enable_internal_parallelism = False
                logger.info(f"[MemoryAware] Disabled parallelism for large file: {filename}")
            
            if self.memory_monitor and self.memory_monitor.get_available_memory_budget() < 1000:
                enable_internal_parallelism = False
                logger.info(f"[MemoryAware] Disabled parallelism due to memory constraints")
                
        except Exception as e:
            logger.warning(f"Could not analyze file {filename}: {e}")
        
        # Ensure table exists
        async with self.managed_connection(pool) as conn:
            await conn.execute(
                base.ensure_table_sql(
                    table, 
                    headers, 
                    base.map_types(headers, types), 
                    primary_keys
                )
            )
        
        # Process data with memory monitoring
        rows_total = 0
        batch_idx = 0
        
        try:
            for batch in batch_gen(file_path, headers, chunk_size):
                # Memory check before each batch
                if self.memory_monitor.should_prevent_processing():
                    logger.error(f"[MemoryAware] Memory limit exceeded at batch {batch_idx}")
                    raise MemoryError(f"Memory limit exceeded at batch {batch_idx}")
                
                # Log memory status every 10 batches
                if batch_idx % 10 == 0:
                    status = self.memory_monitor.get_status_report()
                    logger.info(f"[MemoryAware] Batch {batch_idx}: "
                                f"Memory usage {status['usage_above_baseline_mb']:.1f}MB, "
                                f"Budget remaining {status['budget_remaining_mb']:.1f}MB")
                
                logger.info(f"[MemoryAware] Processing batch {batch_idx}: {len(batch)} rows")
                
                try:
                    if enable_internal_parallelism:
                        rows_processed = await self._process_batch_parallel(
                            pool, batch, sub_batch_size, table, headers, primary_keys,
                            run_id, batch_idx, max_retries, internal_concurrency, types
                        )
                    else:
                        async with self.managed_connection(pool) as conn:
                            rows_processed = await self._process_batch_sequential(
                                conn, batch, sub_batch_size, table, headers, primary_keys,
                                run_id, batch_idx, max_retries, types
                            )
                    
                    rows_total += rows_processed
                    batch_idx += 1
                    
                    # Memory cleanup after each batch
                    del batch
                    gc.collect()
                    
                    # Aggressive cleanup every 5 batches
                    if batch_idx % 5 == 0:
                        if self.memory_monitor.is_memory_pressure_high():
                            cleanup_stats = self.memory_monitor.perform_aggressive_cleanup()
                            logger.info(f"[MemoryAware] Batch {batch_idx}: "
                                      f"Cleanup freed {cleanup_stats.get('freed_mb', 0):.1f}MB")
                    
                except Exception as e:
                    logger.error(f"[MemoryAware] Batch {batch_idx} failed: {e}")
                    # Clean up batch memory on error
                    del batch
                    gc.collect()
                    raise
            
            logger.info(f"[MemoryAware] Completed {filename}: {rows_total} rows processed")
            return rows_total
            
        except Exception as e:
            logger.error(f"[MemoryAware] Processing failed for {filename}: {e}")
            raise
        finally:
            # Final cleanup
            gc.collect()
            if self.memory_monitor:
                final_cleanup = self.memory_monitor.perform_aggressive_cleanup()
                logger.info(f"[MemoryAware] Final cleanup freed {final_cleanup.get('freed_mb', 0):.1f}MB")
    
    async def _process_batch_sequential(
        self,
        conn: asyncpg.Connection,
        batch: List[Tuple],
        sub_batch_size: int,
        table: str,
        headers: List[str],
        primary_keys: List[str],
        max_retries: int,
        types: dict = None
    ) -> int:
        """Memory-aware sequential batch processing."""
        rows_processed = 0
        
        for i in range(0, len(batch), sub_batch_size):
            sub_batch = batch[i:i+sub_batch_size]
            tmp_table = f"tmp_{os.getpid()}_{uuid.uuid4().hex[:8]}"
            
            # Track temp table
            self.temp_tables_created.add(tmp_table)
            
            try:
                types_map = base.map_types(headers, types)
                await conn.execute(base.create_temp_table_sql(tmp_table, headers, types_map))
                
                for attempt in range(max_retries):
                    try:
                        async with conn.transaction():
                            await conn.copy_records_to_table(tmp_table, records=sub_batch, columns=headers)
                            sql = base.upsert_from_temp_sql(table, tmp_table, headers, primary_keys)
                            await conn.execute(sql)
                            await conn.execute(f'TRUNCATE {base.quote_ident(tmp_table)};')
                        
                        rows_processed += len(sub_batch)
                        break
                        
                    except (asyncpg.PostgresError, OSError) as e:
                        logger.warning(f"[MemoryAware] Sub-batch retry {attempt + 1}: {e}")
                        if attempt + 1 >= max_retries:
                            raise
                        await asyncio.sleep(2 ** attempt + random.uniform(0, 0.5))
            
            finally:
                # Always cleanup temp table
                try:
                    await conn.execute(f'DROP TABLE IF EXISTS {base.quote_ident(tmp_table)};')
                    self.temp_tables_created.discard(tmp_table)
                except Exception:
                    pass
                
                # Cleanup sub_batch memory
                del sub_batch
        
        return rows_processed
    
    async def _process_batch_parallel(
        self,
        pool: asyncpg.Pool,
        batch: List[Tuple],
        sub_batch_size: int,
        table: str,
        headers: List[str],
        primary_keys: List[str],
        max_retries: int,
        internal_concurrency: int,
        types: dict = None
    ) -> int:
        """Memory-aware parallel batch processing with reduced concurrency."""
        
        # Create sub-batches
        sub_batches = []
        for i in range(0, len(batch), sub_batch_size):
            sub_batch = batch[i:i+sub_batch_size]
            sub_batches.append(sub_batch)
        
        # Reduce concurrency if memory pressure is high
        if self.memory_monitor and self.memory_monitor.is_memory_pressure_high():
            internal_concurrency = max(1, internal_concurrency // 2)
            logger.warning(f"[MemoryAware] Reduced concurrency to {internal_concurrency} due to memory pressure")
        
        semaphore = asyncio.Semaphore(internal_concurrency)
        upsert_lock = asyncio.Lock()
        
        async def process_sub_batch(sub_batch: List[Tuple]) -> int:
            async with semaphore:
                async with self.managed_connection(pool) as conn:
                    tmp_table = f"tmp_{os.getpid()}_{uuid.uuid4().hex[:8]}"
                    
                    try:
                        types_map = base.map_types(headers, types)
                        await conn.execute(base.create_temp_table_sql(tmp_table, headers, types_map))
                        
                        for attempt in range(max_retries):
                            try:
                                # Prepare data
                                async with conn.transaction():
                                    await conn.copy_records_to_table(tmp_table, records=sub_batch, columns=headers)
                                
                                # Serialize upsert operations
                                async with upsert_lock:
                                    async with conn.transaction():
                                        sql = base.upsert_from_temp_sql(table, tmp_table, headers, primary_keys)
                                        await conn.execute(sql)
                                        await conn.execute(f'TRUNCATE {base.quote_ident(tmp_table)};')
                                
                                return len(sub_batch)
                                
                            except (asyncpg.PostgresError, OSError) as e:
                                logger.warning(f"[MemoryAware] Parallel sub-batch retry {attempt + 1}: {e}")
                                if attempt + 1 >= max_retries:
                                    raise
                                await asyncio.sleep(2 ** attempt + random.uniform(0, 0.5))
                    
                    finally:
                        # Cleanup temp table
                        try:
                            await conn.execute(f'DROP TABLE IF EXISTS {base.quote_ident(tmp_table)};')
                        except Exception:
                            pass
                        
                        # Cleanup sub-batch memory
                        del sub_batch
        
        # Execute sub-batches with memory monitoring
        try:
            results = await asyncio.gather(*[process_sub_batch(sb) for sb in sub_batches])
            return sum(r for r in results if r is not None)
        finally:
            # Cleanup sub_batches memory
            del sub_batches
            gc.collect()


# Factory function for backward compatibility
async def async_upsert(
    pool: asyncpg.Pool,
    file_path: str,
    headers: List[str],
    table: str,
    primary_keys: List[str],
    batch_gen: Callable[[str, List[str], int], Iterable[List[Tuple]]],
    memory_monitor: Optional[Any] = None,
    **kwargs
):
    """Factory function for memory-aware uploader."""
    uploader = FileUploader(memory_monitor=memory_monitor)
    return await uploader.upsert(
        pool, file_path, headers, table, primary_keys, batch_gen, **kwargs
    )