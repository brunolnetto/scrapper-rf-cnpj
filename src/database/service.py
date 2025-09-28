"""
DatabaseService - Consolidated database operations.
Combines FileUploader functionality with direct record loading capabilities.
"""
import asyncio
import asyncpg
import os
import uuid
import random
import gc
from typing import List, Tuple, Optional, Any, Dict
from contextlib import asynccontextmanager
from concurrent.futures import ThreadPoolExecutor

from . import utils as base
from ..setup.logging import logger
from .models.audit import AuditStatus
from .engine import Database
from ..core.services.memory.service import MemoryMonitor


class DatabaseService:
    """
    Handles all database operations: connections, temp tables, and record loading.
    """
    
    def __init__(self, database: Database, memory_monitor: Optional[MemoryMonitor] = None):
        self.database = database
        self.memory_monitor = memory_monitor
        
        # Thread-safe tracking
        self._temp_table_lock = asyncio.Lock() if asyncio else None
        self.temp_tables_created = set()
        self._connection_lock = asyncio.Lock() if asyncio else None
        self.active_connections = set()
    
    def load_records_directly(self, table_info: Any, records: List[Tuple]) -> Tuple[bool, Optional[str], int]:
        """
        Synchronous wrapper for async database operations.
        FIX: Provide sync interface that BatchProcessor expects while handling table_info attributes safely.
        """
        if not records:
            return True, None, 0
        
        try:
            # Safely extract table info attributes with fallbacks
            table_name = getattr(table_info, 'table_name', 'unknown_table')
            columns = getattr(table_info, 'columns', [f'col_{i}' for i in range(len(records[0]) if records else 0)])
            primary_keys = getattr(table_info, 'primary_keys', [])
            types = getattr(table_info, 'types', {})
            
            # Create a standardized table info object for async method
            safe_table_info = type('SafeTableInfo', (), {
                'table_name': table_name,
                'columns': columns,
                'primary_keys': primary_keys,
                'types': types
            })()
            
            return self._sync_wrapper(self._async_load_records_directly(safe_table_info, records))
            
        except Exception as e:
            logger.error(f"Sync load_records_directly failed: {e}")
            return False, str(e), 0

    def _sync_wrapper(self, coro):
        """
        Robust sync/async boundary handling.
        FIX: Simplified to use asyncio.run for each async call.
        """
        try:
            import asyncio
            return asyncio.run(coro)
        except Exception as e:
            logger.error(f"Sync wrapper failed: {e}")
            return False, str(e), 0

    async def _async_load_records_directly(self, table_info, records: List[Tuple]) -> Tuple[bool, Optional[str], int]:
        """
        Internal async implementation with robust error handling.
        """
        # FIX: Handle database interface more flexibly
        try:
            if hasattr(self.database, 'get_async_pool'):
                pool = await self.database.get_async_pool()
            elif hasattr(self.database, 'pool'):
                pool = self.database.pool
            elif isinstance(self.database, asyncpg.Pool):
                pool = self.database
            else:
                raise NotImplementedError(f"Database type {type(self.database)} not supported for async operations")

            async with self.managed_connection(pool) as conn:
                tmp_table = f"tmp_direct_{os.getpid()}_{uuid.uuid4().hex[:8]}"
                headers = table_info.columns
                types_map = base.map_types(headers, table_info.types)
                
                try:
                    await conn.execute(base.create_temp_table_sql(tmp_table, headers, types_map))
                    
                    async with conn.transaction():
                        await conn.copy_records_to_table(tmp_table, records=records, columns=headers)
                        
                        if table_info.primary_keys:
                            sql = base.upsert_from_temp_sql(table_info.table_name, tmp_table, headers, table_info.primary_keys)
                            await conn.execute(sql)
                        else:
                            # Handle tables without primary keys with a simple INSERT
                            insert_sql = f'INSERT INTO {base.quote_ident(table_info.table_name)} SELECT * FROM {base.quote_ident(tmp_table)}'
                            await conn.execute(insert_sql)
                    
                    return True, None, len(records)
                    
                finally:
                    # Always ensure the temporary database table is dropped
                    try:
                        await conn.execute(f'DROP TABLE IF EXISTS {base.quote_ident(tmp_table)};')
                    except Exception:
                        pass
                        
        except Exception as e:
            logger.error(f"Async record loading failed for table '{table_info.table_name}': {e}")
            return False, str(e), 0

    async def cleanup_temp_tables(self, conn: asyncpg.Connection):
        """Thread-safe temp table cleanup."""
        if self._temp_table_lock:
            async with self._temp_table_lock:
                for temp_table in list(self.temp_tables_created):
                    try:
                        await conn.execute(f'DROP TABLE IF EXISTS {base.quote_ident(temp_table)};')
                        self.temp_tables_created.discard(temp_table)
                    except Exception as e:
                        logger.warning(f"Failed to cleanup temp table {temp_table}: {e}")
        else:
            # Fallback without locking
            for temp_table in list(self.temp_tables_created):
                try:
                    await conn.execute(f'DROP TABLE IF EXISTS {base.quote_ident(temp_table)};')
                    self.temp_tables_created.discard(temp_table)
                except Exception as e:
                    logger.warning(f"Failed to cleanup temp table {temp_table}: {e}")

    @asynccontextmanager
    async def managed_connection(self, pool: asyncpg.Pool):
        """Thread-safe connection lifecycle management."""
        conn = None
        try:
            conn = await pool.acquire()
            if self._connection_lock:
                async with self._connection_lock:
                    self.active_connections.add(id(conn))
            else:
                self.active_connections.add(id(conn))
            yield conn
        finally:
            if conn:
                try:
                    await self.cleanup_temp_tables(conn)
                except Exception:
                    pass
                finally:
                    if self._connection_lock:
                        async with self._connection_lock:
                            self.active_connections.discard(id(conn))
                    else:
                        self.active_connections.discard(id(conn))
                    await pool.release(conn)

    async def upsert_batches(
        self,
        pool: asyncpg.Pool,
        table_info: Any,
        batch_generator: Any,
        chunk_size: int = 30_000,
        sub_batch_size: int = 3_000,
        max_retries: int = 3,
        enable_internal_parallelism: bool = False,
        internal_concurrency: int = 2
    ) -> int:
        """
        Memory-aware batch upsert with integrated monitoring.
        """
        rows_total = 0
        batch_idx = 0
        
        try:
            for batch in batch_generator:
                # Memory check before each batch
                if self.memory_monitor and self.memory_monitor.should_prevent_processing():
                    logger.error(f"[DatabaseService] Memory limit exceeded at batch {batch_idx}")
                    raise MemoryError(f"Memory limit exceeded at batch {batch_idx}")
                
                logger.info(f"[DatabaseService] Processing batch {batch_idx}: {len(batch)} rows")
                
                try:
                    if enable_internal_parallelism:
                        rows_processed = await self._process_batch_bounded_concurrency(
                            pool, batch, sub_batch_size, table_info,
                            batch_idx, max_retries, internal_concurrency
                        )
                    else:
                        async with self.managed_connection(pool) as conn:
                            rows_processed = await self._process_batch_sequential_optimized(
                                conn, batch, sub_batch_size, table_info,
                                batch_idx, max_retries
                            )
                    
                    rows_total += rows_processed
                    batch_idx += 1
                    
                    # Cleanup batch memory
                    del batch
                    
                    # Adaptive cleanup based on memory pressure
                    if batch_idx % 10 == 0 and self.memory_monitor:
                        if self.memory_monitor.is_memory_pressure_high():
                            gc.collect()
                            cleanup_stats = self.memory_monitor.perform_aggressive_cleanup()
                            logger.info(f"[DatabaseService] Batch {batch_idx}: "
                                      f"Cleanup freed {cleanup_stats.get('freed_mb', 0):.1f}MB")
                    
                except Exception as e:
                    logger.error(f"[DatabaseService] Batch {batch_idx} failed: {e}")
                    del batch
                    raise
            
            logger.info(f"[DatabaseService] Completed processing: {rows_total} rows processed")
            return rows_total
            
        finally:
            # Final cleanup only if memory pressure exists
            if self.memory_monitor and self.memory_monitor.is_memory_pressure_high():
                gc.collect()
                final_cleanup = self.memory_monitor.perform_aggressive_cleanup()
                logger.info(f"[DatabaseService] Final cleanup freed {final_cleanup.get('freed_mb', 0):.1f}MB")

    async def _process_batch_sequential_optimized(
        self,
        conn: asyncpg.Connection,
        batch: List[Tuple],
        sub_batch_size: int,
        table_info: Any,
        batch_idx: int,
        max_retries: int
    ) -> int:
        """Optimized sequential processing with single temp table reuse."""
        rows_processed = 0
        
        # Use single temp table per connection and reuse via TRUNCATE
        tmp_table = f"tmp_{os.getpid()}_{batch_idx}"
        
        if self._temp_table_lock:
            async with self._temp_table_lock:
                self.temp_tables_created.add(tmp_table)
        else:
            self.temp_tables_created.add(tmp_table)
        
        try:
            headers = table_info.columns
            types_map = base.map_types(headers, getattr(table_info, 'types', {}))
            await conn.execute(base.create_temp_table_sql(tmp_table, headers, types_map))
            
            # Process sub-batches using same temp table
            for i in range(0, len(batch), sub_batch_size):
                sub_batch = batch[i:i+sub_batch_size]
                
                for attempt in range(max_retries):
                    try:
                        async with conn.transaction():
                            await conn.copy_records_to_table(tmp_table, records=sub_batch, columns=headers)
                            
                            primary_keys = getattr(table_info, 'primary_keys', [])
                            if primary_keys:
                                sql = base.upsert_from_temp_sql(table_info.table_name, tmp_table, headers, primary_keys)
                                await conn.execute(sql)
                            else:
                                # Simple insert for tables without primary keys
                                insert_sql = f'INSERT INTO {base.quote_ident(table_info.table_name)} SELECT * FROM {base.quote_ident(tmp_table)}'
                                await conn.execute(insert_sql)
                            
                            await conn.execute(f'TRUNCATE {base.quote_ident(tmp_table)};')
                        
                        rows_processed += len(sub_batch)
                        break
                        
                    except (asyncpg.PostgresError, OSError) as e:
                        logger.warning(f"[DatabaseService] Sub-batch retry {attempt + 1}: {e}")
                        if attempt + 1 >= max_retries:
                            raise
                        await asyncio.sleep(2 ** attempt + random.uniform(0, 0.5))
                
                # Cleanup sub-batch reference immediately
                del sub_batch
        
        finally:
            # Always cleanup temp table
            try:
                await conn.execute(f'DROP TABLE IF EXISTS {base.quote_ident(tmp_table)};')
                if self._temp_table_lock:
                    async with self._temp_table_lock:
                        self.temp_tables_created.discard(tmp_table)
                else:
                    self.temp_tables_created.discard(tmp_table)
            except Exception:
                pass
        
        return rows_processed

    async def _process_batch_bounded_concurrency(
        self,
        pool: asyncpg.Pool,
        batch: List[Tuple],
        sub_batch_size: int,
        table_info: Any,
        batch_idx: int,
        max_retries: int,
        internal_concurrency: int
    ) -> int:
        """Fixed bounded concurrency without asyncio.gather memory spike."""
        
        # Reduce concurrency if memory pressure is high
        if self.memory_monitor and self.memory_monitor.is_memory_pressure_high():
            internal_concurrency = max(1, internal_concurrency // 2)
        
        # Use bounded worker pattern instead of gather
        semaphore = asyncio.Semaphore(internal_concurrency)
        upsert_lock = asyncio.Lock()
        
        async def process_sub_batch_worker(start_idx: int, sub_batch: List[Tuple]) -> int:
            async with semaphore:
                async with self.managed_connection(pool) as conn:
                    tmp_table = f"tmp_{os.getpid()}_{batch_idx}_{start_idx}"
                    headers = table_info.columns
                    types_map = base.map_types(headers, getattr(table_info, 'types', {}))
                    
                    try:
                        await conn.execute(base.create_temp_table_sql(tmp_table, headers, types_map))
                        
                        for attempt in range(max_retries):
                            try:
                                # Prepare data
                                async with conn.transaction():
                                    await conn.copy_records_to_table(tmp_table, records=sub_batch, columns=headers)
                                
                                # Serialize upsert operations
                                async with upsert_lock:
                                    async with conn.transaction():
                                        primary_keys = getattr(table_info, 'primary_keys', [])
                                        if primary_keys:
                                            sql = base.upsert_from_temp_sql(table_info.table_name, tmp_table, headers, primary_keys)
                                            await conn.execute(sql)
                                        else:
                                            insert_sql = f'INSERT INTO {base.quote_ident(table_info.table_name)} SELECT * FROM {base.quote_ident(tmp_table)}'
                                            await conn.execute(insert_sql)
                                
                                return len(sub_batch)
                                
                            except (asyncpg.PostgresError, OSError) as e:
                                if attempt + 1 >= max_retries:
                                    raise
                                await asyncio.sleep(2 ** attempt + random.uniform(0, 0.5))
                    
                    finally:
                        # Cleanup temp table and sub-batch
                        try:
                            await conn.execute(f'DROP TABLE IF EXISTS {base.quote_ident(tmp_table)};')
                        except Exception:
                            pass
                        del sub_batch
        
        # Process with bounded concurrency - avoid building large sub_batches list
        total_rows = 0
        tasks = []
        
        for i in range(0, len(batch), sub_batch_size):
            sub_batch = batch[i:i+sub_batch_size]
            task = asyncio.create_task(process_sub_batch_worker(i, sub_batch))
            tasks.append(task)
        
        # Process completed tasks as they finish to free memory earlier
        for completed_task in asyncio.as_completed(tasks):
            try:
                rows = await completed_task
                total_rows += rows
            except Exception as e:
                logger.error(f"[DatabaseService] Sub-batch task failed: {e}")
                raise
        
        return total_rows

    async def ensure_table_exists(self, pool: asyncpg.Pool, table_info: Any):
        """Ensure target table exists with proper schema."""
        async with self.managed_connection(pool) as conn:
            headers = table_info.columns
            primary_keys = getattr(table_info, 'primary_keys', [])
            types_map = base.map_types(headers, getattr(table_info, 'types', {}))
            
            sql = base.ensure_table_sql(table_info.table_name, headers, types_map, primary_keys)
            await conn.execute(sql)
            
            logger.debug(f"[DatabaseService] Ensured table exists: {table_info.table_name}")

    def get_connection_pool(self):
        """Get async connection pool from database."""
        if not hasattr(self.database, 'get_async_pool'):
            raise NotImplementedError("Database must have get_async_pool method")
        import asyncio
        return asyncio.run(self.database.get_async_pool())

    def log_status(self, operation: str):
        """Log current database service status."""
        logger.info(f"[DatabaseService] {operation} - "
                   f"Active connections: {len(self.active_connections)}, "
                   f"Temp tables: {len(self.temp_tables_created)}")
        
        if self.memory_monitor:
            status = self.memory_monitor.get_status_report()
            if status:
                logger.info(f"[DatabaseService] Memory status - "
                           f"Usage: {status.get('usage_above_baseline_mb', 0):.1f}MB, "
                           f"Budget: {status.get('budget_remaining_mb', 0):.1f}MB")