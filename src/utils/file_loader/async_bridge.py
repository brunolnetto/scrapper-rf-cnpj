"""
async_bridge.py
Bridge between SQLAlchemy (sync) and AsyncPG (async) for high-performance loading.
Provides seamless integration of async uploader with existing database setup.
"""
import asyncio
import asyncpg
from typing import List, Dict, Optional, Tuple
from urllib.parse import urlparse

from ...setup.logging import logger
from ...database.schemas import Database
from ...core.schemas import TableInfo
from .uploader import AsyncFileUploader

class AsyncDatabaseBridge:
    """Bridge for integrating async processing with existing SQLAlchemy setup."""
    
    def __init__(self, database: Database, config):
        self.database = database
        self.config = config
        self._pool = None
        
    async def get_async_pool(self) -> asyncpg.Pool:
        """Get or create async connection pool."""
        if self._pool is None:
            # Extract connection details from SQLAlchemy engine
            url = self.database.engine.url
            
            # Build asyncpg DSN from SQLAlchemy URL
            if url.password:
                dsn = f"postgresql://{url.username}:{url.password}@{url.host}:{url.port or 5432}/{url.database}"
            else:
                dsn = f"postgresql://{url.username}@{url.host}:{url.port or 5432}/{url.database}"
            
            # Get pool configuration from ETL config
            min_size = getattr(self.config.etl, 'async_pool_min_size', 1)
            max_size = getattr(self.config.etl, 'async_pool_max_size', 10)
            
            logger.info(f"[AsyncBridge] Creating connection pool (min: {min_size}, max: {max_size})")
            
            self._pool = await asyncpg.create_pool(
                dsn,
                min_size=min_size,
                max_size=max_size
            )
            
        return self._pool
    
    async def load_file_async(self, table_info: TableInfo, file_path: str, 
                             batch_generator_func, encoding: str = 'utf-8') -> Tuple[bool, Optional[str], int]:
        """
        Load file using async uploader with SQLAlchemy compatibility.
        
        Args:
            table_info: Table information from existing system
            file_path: Path to file to load
            batch_generator_func: Function to generate batches
            encoding: File encoding for CSV files
            
        Returns:
            Tuple of (success, error_message, rows_processed)
        """
        try:
            pool = await self.get_async_pool()
            
            # Get async configuration
            enable_parallel = getattr(self.config.etl, 'enable_parallel_processing', False)
            internal_concurrency = getattr(self.config.etl, 'internal_concurrency', 3)
            sub_batch_size = getattr(self.config.etl, 'sub_batch_size', 5000)
            chunk_size = getattr(self.config.etl, 'chunk_size', 50000)
            
            logger.info(f"[AsyncBridge] Loading {file_path} with async uploader")
            logger.info(f"[AsyncBridge] Config - parallel: {enable_parallel}, concurrency: {internal_concurrency}")
            
            # Create async uploader
            uploader = AsyncFileUploader(
                pool=pool,
                enable_parallel=enable_parallel,
                internal_concurrency=internal_concurrency,
                sub_batch_size=sub_batch_size
            )
            
            # Get table metadata
            headers = self._get_table_columns(table_info)
            primary_keys = self._get_primary_keys(table_info)
            types = self._get_column_types(table_info)
            
            # Create encoding-aware batch generator
            def encoding_batch_generator(path, headers, chunk_size):
                if file_path.endswith('.csv'):
                    from .ingestors import batch_generator_csv
                    return batch_generator_csv(path, headers, chunk_size, encoding=encoding)
                else:
                    from .ingestors import batch_generator_parquet
                    return batch_generator_parquet(path, headers, chunk_size)
            
            # Upload file
            rows_processed = await uploader.upload_file(
                file_path=file_path,
                headers=headers,
                table=table_info.table_name,
                primary_keys=primary_keys,
                batch_generator=encoding_batch_generator,
                chunk_size=chunk_size,
                types=types
            )
            
            logger.info(f"[AsyncBridge] Successfully processed {rows_processed} rows")
            return True, None, rows_processed
            
        except Exception as e:
            logger.error(f"[AsyncBridge] Async loading failed: {e}")
            return False, str(e), 0
    
    def _get_table_columns(self, table_info: TableInfo) -> List[str]:
        """Get table columns from table_info."""
        return table_info.columns
    
    def _get_primary_keys(self, table_info: TableInfo) -> List[str]:
        """Get primary keys from SQLAlchemy models."""
        try:
            from ...utils.model_utils import get_table_index_columns
            index_columns = get_table_index_columns(table_info.table_name)
            if index_columns:
                return index_columns
        except Exception as e:
            logger.warning(f"Could not get index columns: {e}")
        
        # Fallback to default
        return ['cnpj_basico']
    
    def _get_column_types(self, table_info: TableInfo) -> Dict[str, str]:
        """Get column types from SQLAlchemy models."""
        try:
            from ...utils.model_utils import get_model_by_table_name
            model = get_model_by_table_name(table_info.table_name)
            
            types = {}
            for column in model.__table__.columns:
                # Map SQLAlchemy types to PostgreSQL types
                col_type = str(column.type)
                if 'VARCHAR' in col_type or 'TEXT' in col_type:
                    types[column.name] = 'TEXT'
                elif 'FLOAT' in col_type or 'DOUBLE' in col_type:
                    types[column.name] = 'DOUBLE PRECISION'
                elif 'INTEGER' in col_type or 'BIGINT' in col_type:
                    types[column.name] = 'BIGINT'
                else:
                    types[column.name] = 'TEXT'  # Default fallback
            
            return types
            
        except Exception as e:
            logger.warning(f"Could not get column types: {e}")
            # Fallback to all TEXT
            return {col: 'TEXT' for col in table_info.columns}
    
    async def close(self):
        """Close async pool."""
        if self._pool:
            await self._pool.close()
            self._pool = None
            logger.info("[AsyncBridge] Connection pool closed")
    
    def __del__(self):
        """Cleanup on object destruction."""
        if self._pool:
            logger.warning("[AsyncBridge] Pool not properly closed, attempting cleanup")
            # Note: This is not ideal but provides some cleanup
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    loop.create_task(self.close())
            except:
                pass
