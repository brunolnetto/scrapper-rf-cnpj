"""
Fixed DatabaseService - Consolidated database operations with improved async management.
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
    Fixed to prevent 'operation in progress' errors and connection conflicts.
    """
    
    def __init__(self, database: Database, memory_monitor: Optional[MemoryMonitor] = None):
        self.database = database
        self.memory_monitor = memory_monitor
        
        # FIX: Simplified tracking without complex locking
        self.temp_tables_created = set()
        self.active_connections = set()
        
        # FIX: Connection pool management
        self._pool_lock = asyncio.Lock()
        self._pool = None
    
    async def get_or_create_pool(self):
        """Get or create connection pool safely."""
        async with self._pool_lock:
            if self._pool is None:
                self._pool = await self.database.get_async_pool()
            return self._pool
    
    def load_records_directly(self, table_info: Any, records: List[Tuple]) -> Tuple[bool, Optional[str], int]:
        """
        Synchronous wrapper for async database operations.
        FIX: Simplified sync wrapper that doesn't interfere with existing event loops.
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
            
            # FIX: Better sync/async boundary handling
            try:
                loop = asyncio.get_running_loop()
                # We're in an async context - this shouldn't happen with our new design
                logger.warning("[DatabaseService] load_records_directly called from async context - this indicates a design issue")
                return False, "Cannot call sync method from async context", 0
            except RuntimeError:
                # No running loop - safe to create new one
                return asyncio.run(self._async_load_records_directly(safe_table_info, records))
            
        except Exception as e:
            logger.error(f"Sync load_records_directly failed: {e}")
            return False, str(e), 0

    async def _async_load_records_directly(self, table_info, records: List[Tuple]) -> Tuple[bool, Optional[str], int]:
        """
        Internal async implementation with robust error handling.
        FIX: Use managed pool and connection to prevent conflicts.
        """
        try:
            pool = await self.get_or_create_pool()
            
            async with self.managed_connection(pool) as conn:
                tmp_table = f"tmp_direct_{os.getpid()}_{uuid.uuid4().hex[:8]}"
                headers = table_info.columns
                types_map = base.map_types(headers, getattr(table_info, 'types', {}))
                
                try:
                    await conn.execute(base.create_temp_table_sql(tmp_table, headers, types_map))
                    
                    async with conn.transaction():
                        await conn.copy_records_to_table(tmp_table, records=records, columns=headers)
                        
                        primary_keys = getattr(table_info, 'primary_keys', [])
                        if primary_keys:
                            sql = base.upsert_from_temp_sql(table_info.table_name, tmp_table, headers, primary_keys)
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
                        self.temp_tables_created.discard(tmp_table)
                    except Exception:
                        pass
                        
        except Exception as e:
            logger.error(f"Async record loading failed for table '{table_info.table_name}': {e}")
            return False, str(e), 0

    async def cleanup_temp_tables(self, conn: asyncpg.Connection):
        """Cleanup temp tables without complex locking."""
        tables_to_cleanup = list(self.temp_tables_created)
        for temp_table in tables_to_cleanup:
            try:
                await conn.execute(f'DROP TABLE IF EXISTS {base.quote_ident(temp_table)};')
                self.temp_tables_created.discard(temp_table)
            except Exception as e:
                logger.warning(f"Failed to cleanup temp table {temp_table}: {e}")

    @asynccontextmanager
    async def managed_connection(self, pool: asyncpg.Pool):
        """
        Simplified connection lifecycle management.
        FIX: Removed complex locking that could cause deadlocks.
        """
        conn = None
        try:
            conn = await pool.acquire()
            conn_id = id(conn)
            self.active_connections.add(conn_id)
            
            # FIX: Set a reasonable timeout for operations
            await conn.execute("SET statement_timeout = '300s';")  # 5 minutes
            
            yield conn
            
        except Exception as e:
            logger.error(f"[DatabaseService] Connection error: {e}")
            raise
            
        finally:
            if conn:
                try:
                    # Always try to cleanup temp tables for this connection
                    await self.cleanup_temp_tables(conn)
                except Exception as e:
                    logger.warning(f"Cleanup failed during connection close: {e}")
                finally:
                    conn_id = id(conn)
                    self.active_connections.discard(conn_id)
                    try:
                        await pool.release(conn)
                    except Exception as e:
                        logger.error(f"Failed to release connection: {e}")

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
        FIX: Simplified to avoid connection conflicts.
        """
        rows_total = 0
        batch_idx = 0
        
        try:
            for batch in batch_generator:
                # Memory check before each batch
                if self.memory_monitor.should_prevent_processing():
                    logger.error(f"[DatabaseService] Memory limit exceeded at batch {batch_idx}")
                    raise MemoryError(f"Memory limit exceeded at batch {batch_idx}")
                
                logger.info(f"[DatabaseService] Processing batch {batch_idx}: {len(batch)} rows")
                
                try:
                    # FIX: Always use sequential processing to avoid connection conflicts
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
            if self.memory_monitor.is_memory_pressure_high():
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
        """
        Optimized sequential processing with single temp table reuse.
        FIX: Improved error handling and connection state management.
        """
        rows_processed = 0
        
        # Use single temp table per connection and reuse via TRUNCATE
        tmp_table = f"tmp_{os.getpid()}_{batch_idx}_{uuid.uuid4().hex[:8]}"
        
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
                        # FIX: Check connection state before operations
                        if conn.is_closed():
                            raise asyncpg.ConnectionDoesNotExistError("Connection was closed")
                        
                        async with conn.transaction():
                            await conn.copy_records_to_table(tmp_table, records=sub_batch, columns=headers)
                            
                            primary_keys = getattr(table_info, 'primary_keys', [])
                            if not primary_keys:
                                # Try to extract from model
                                from . import utils as db_utils
                                try:
                                    primary_keys = db_utils.extract_primary_keys(table_info)
                                except Exception as e:
                                    logger.debug(f"Could not extract primary keys: {e}")
                                    primary_keys = []
                            
                            if primary_keys:
                                sql = base.upsert_from_temp_sql(table_info.table_name, tmp_table, headers, primary_keys)
                                await conn.execute(sql)
                            else:
                                # Simple insert for tables without primary keys
                                insert_sql = f'INSERT INTO {base.quote_ident(table_info.table_name)} SELECT * FROM {base.quote_ident(tmp_table)}'
                                await conn.execute(insert_sql)
                            
                            # Clear temp table for next sub-batch
                            await conn.execute(f'TRUNCATE {base.quote_ident(tmp_table)};')
                        
                        rows_processed += len(sub_batch)
                        break
                        
                    except (asyncpg.PostgresError, asyncpg.ConnectionDoesNotExistError, OSError) as e:
                        logger.warning(f"[DatabaseService] Sub-batch retry {attempt + 1}/{max_retries}: {e}")
                        if attempt + 1 >= max_retries:
                            raise
                        
                        # FIX: Check if we need to recreate temp table after error
                        if "does not exist" in str(e).lower():
                            try:
                                await conn.execute(base.create_temp_table_sql(tmp_table, headers, types_map))
                            except Exception:
                                pass  # Will fail on next attempt if still broken
                        
                        await asyncio.sleep(2 ** attempt + random.uniform(0, 0.5))
                
                # Cleanup sub-batch reference immediately
                del sub_batch
        
        finally:
            # Always cleanup temp table
            try:
                if not conn.is_closed():
                    await conn.execute(f'DROP TABLE IF EXISTS {base.quote_ident(tmp_table)};')
                self.temp_tables_created.discard(tmp_table)
            except Exception as e:
                logger.debug(f"Temp table cleanup failed: {e}")
        
        return rows_processed

    async def ensure_table_exists(self, pool: asyncpg.Pool, table_info: Any):
        """
        Ensure target table exists with proper schema.
        FIX: Simplified with better error handling.
        """
        async with self.managed_connection(pool) as conn:
            try:
                headers = table_info.columns
                primary_keys = getattr(table_info, 'primary_keys', [])
                
                # Try to extract primary keys if not provided
                if not primary_keys:
                    from . import utils as db_utils
                    try:
                        primary_keys = db_utils.extract_primary_keys(table_info)
                    except Exception as e:
                        logger.debug(f"Could not extract primary keys for {table_info.table_name}: {e}")
                        primary_keys = []
                
                # Get column types
                types_map = getattr(table_info, 'types', {})
                if not types_map:
                    from . import utils as db_utils
                    try:
                        types_map = db_utils.get_column_types_mapping(table_info)
                    except Exception:
                        types_map = base.map_types(headers, {})
                
                sql = base.ensure_table_sql(table_info.table_name, headers, types_map, primary_keys)
                await conn.execute(sql)
                
                logger.debug(f"[DatabaseService] Ensured table exists: {table_info.table_name}")
                
            except Exception as e:
                logger.error(f"[DatabaseService] Failed to ensure table {table_info.table_name}: {e}")
                raise

    def get_connection_pool(self):
        """
        Get async connection pool from database.
        FIX: Simplified synchronous access to async pool.
        """
        try:
            loop = asyncio.get_running_loop()
            # We're in async context, should not happen with new design
            logger.warning("[DatabaseService] get_connection_pool called from async context")
            return None
        except RuntimeError:
            # No running loop - safe to create
            return asyncio.run(self.get_or_create_pool())

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

    async def close_pool(self):
        """Close the connection pool properly."""
        if self._pool:
            async with self._pool_lock:
                if self._pool:
                    await self._pool.close()
                    self._pool = None