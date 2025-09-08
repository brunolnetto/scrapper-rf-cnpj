"""
Unified data loading utilities for CNPJ ETL.

This module provides efficient data loading for both CSV and Parquet files.
Now uses the enhanced loader from lab/refactored_fileloader for high-performance async processing.

USAGE GUIDE:
- UnifiedLoader: Use for normal-sized files (< 2GB) with async processing
- LargeFileLoader: Use for large files (4GB+) to avoid OOM kills

Example:
    from src.database.dml import UnifiedLoader, LargeFileLoader
    from src.database.dml import table_name_to_table_info

    # For normal files
    loader = UnifiedLoader(database, config)
    table_info = table_name_to_table_info('empresa')
    success, error, rows = loader.load_file(table_info, 'data/file.csv')

    # For large files (4GB+)
    large_loader = LargeFileLoader(database, config)
    success, error, rows = large_loader.load_file(table_info, 'data/large_file.parquet')
"""
import asyncio
from pathlib import Path
from typing import Tuple, Optional, Union, List
from sqlalchemy import text, inspect

from ..setup.logging import logger
from ..core.constants import TABLES_INFO_DICT
from ..core.schemas import TableInfo
from ..core.services.loading.file_loader.file_loader import FileLoader
from ..core.services.loading.file_loader.uploader import async_upsert
from ..core.services.loading.file_loader.connection_factory import (
    create_asyncpg_pool_from_sqlalchemy,
    extract_primary_keys,
    get_column_types_mapping
)
from ..core.utils.models import get_table_columns
from .engine import Database


class BaseFileLoader:
    """Base class with common file processing functionality."""

    def __init__(self, logger_prefix: str = "BaseLoader"):
        self.logger_prefix = logger_prefix

    def _validate_file(self, file_path: Union[str, Path]) -> Optional[Path]:
        """Validate file exists and return Path object."""
        file_path = Path(file_path)
        if not file_path.exists():
            return None
        return file_path

    def _get_encoding(self, table_info: TableInfo) -> str:
        """Extract encoding from table_info with fallback to utf-8."""
        encoding = getattr(table_info, 'encoding', None)
        if encoding and hasattr(encoding, 'value'):
            return encoding.value
        return 'utf-8'

    def _create_file_loader(self, file_path: Path, encoding: str) -> FileLoader:
        """Create and return configured FileLoader."""
        return FileLoader(str(file_path), encoding=encoding)

    def _detect_format(self, file_loader: FileLoader, file_path: Path) -> str:
        """Detect and log file format."""
        detected_format = file_loader.get_format()
        logger.info(f"[{self.logger_prefix}] Detected format: {detected_format} for {file_path.name}")
        return detected_format


def apply_transforms_to_batch(table_info: TableInfo, batch: List[Tuple], headers: List[str]) -> List[Tuple]:
    """Apply row-level transforms to a batch of data."""
    transform_func = getattr(table_info, 'transform_map', None)

    # Import default transform for proper identity comparison
    from ..core.transforms import default_transform_map

    if not transform_func or transform_func is default_transform_map:
        # No transforms needed, return batch as-is
        return batch

    # Apply transforms to each row
    logger.debug(f"[TransformUtil] Applying {transform_func.__name__} transforms to batch")
    transformed_batch = []

    for row_tuple in batch:
        try:
            # Convert tuple to dictionary
            row_dict = dict(zip(headers, row_tuple))

            # Apply transform function
            transformed_dict = transform_func(row_dict)

            # Convert back to tuple in correct order
            transformed_tuple = tuple(
                transformed_dict.get(header, row_tuple[i])
                for i, header in enumerate(headers)
            )

            transformed_batch.append(transformed_tuple)

        except Exception as e:
            # On transform error, use original row and log warning
            logger.warning(f"[TransformUtil] Transform failed for row in {table_info.table_name}: {e}")
            transformed_batch.append(row_tuple)

    return transformed_batch


def get_table_model(table_name: str, base=None):
    """Get the SQLAlchemy model class for a given table name.
    
    Args:
        table_name: Name of the table
        
    Returns:
        SQLAlchemy model class or None if not found
    """
    # Search through all mappers in the registry
    for mapper in base.registry.mappers:
        if mapper.class_.__tablename__ == table_name:
            return mapper.class_

    return None


def table_name_to_table_info(table_name: str) -> TableInfo:
    """Convert table name to TableInfo object."""
    table_info_dict = TABLES_INFO_DICT[table_name]

    # Get table info
    label = table_info_dict["label"]
    zip_group = table_info_dict["group"]
    columns = get_table_columns(table_name)  # Derive from SQLAlchemy model
    encoding = table_info_dict["encoding"]
    transform_map = table_info_dict.get("transform_map", lambda x: x)
    expression = table_info_dict["expression"]
    
    # Get the SQLAlchemy model class for this table
    # Search through all mappers in the registry
    from .models import MainBase    
    table_model = get_table_model(table_name, MainBase)

    # Create table info object
    return TableInfo(
        label, 
        zip_group, 
        table_name, 
        columns, 
        encoding, 
        transform_map, 
        expression, 
        table_model
    )

def inspect_primary_keys(table_class) -> list:
    inspector = inspect(table_class)
    pk_columns = []
    for column in inspector.columns:
        if column.primary_key:
            pk_columns.append(column.name)
    return pk_columns

def get_primary_key_columns(table_name: str, base=None, table_model=None) -> List[str]:
    """Extract primary key column names from SQLAlchemy model metadata.

    Args:
        table_name: Name of the table to get primary keys for
        base: SQLAlchemy declarative base to search in (defaults to MainBase)
        table_model: Direct SQLAlchemy model class (takes precedence if provided)

    Returns:
        List of primary key column names
    """
    # If table_model is provided directly, use it
    if table_model is not None:
        return inspect_primary_keys(table_model)

    # Get the table class from the registry
    table_class = get_table_model(table_name, base)

    if not table_class:
        raise ValueError(f"Table '{table_name}' not found in SQLAlchemy models")

    # Extract primary key columns from the table
    return inspect_primary_keys(table_class)


class UnifiedLoader(BaseFileLoader):
    """
    Enhanced unified data loader replacing pandas/polars-based UnifiedLoader.

    Features:
    - Robust 4-layer file format detection
    - High-performance async processing with internal parallelism
    - Memory-efficient streaming (no full file loading)
    - Support for both CSV and Parquet files
    - Configurable encoding for CSV files
    - Complete audit trail with checksums
    """

    def __init__(self, database: Database, config=None):
        BaseFileLoader.__init__(self, logger_prefix="UnifiedLoader")
        self.database = database
        self.config = config
        logger.info("[UnifiedLoader] Initialized with async support")
    
    def load_file(
        self,
        table_info: TableInfo,
        file_path: Union[str, Path],
        chunk_size: Optional[int] = None,
        max_retries: int = 3,
    ) -> Tuple[bool, Optional[str], int]:
        """
        Load any file (CSV or Parquet) with automatic format detection.
        
        This is the main entry point that replaces the old UnifiedLoader methods:
        - load_csv_file()
        - load_parquet_file()  
        - load_file()
        
        Args:
            table_info: Table information with columns, encoding, etc.
            file_path: Path to file (CSV or Parquet)
            chunk_size: Optional chunk size override
            max_retries: Number of retry attempts
            
        Returns:
            Tuple of (success, error_message, rows_processed)
        """
        try:
            # Validate file exists
            file_path = self._validate_file(file_path)
            if not file_path:
                return False, "File not found", 0
            
            # Get encoding and create file loader
            encoding = self._get_encoding(table_info)
            file_loader = self._create_file_loader(file_path, encoding)
            
            # Detect format (not used in this flow)
            self._detect_format(file_loader, file_path)
            
            # Execute load
            return self._execute_load(table_info, file_path, file_loader, chunk_size, max_retries)
            
        except Exception as e:
            error_msg = f"Failed to load {Path(file_path).name}: {e}"
            logger.error(f"[UnifiedLoader] {error_msg}")
            return False, error_msg, 0

    def _execute_load(
        self,
        table_info: TableInfo,
        file_path: Path,
        file_loader: FileLoader,
        chunk_size: Optional[int],
        max_retries: int
    ) -> Tuple[bool, Optional[str], int]:
        """Execute the actual file loading."""
        logger.info(f"[UnifiedLoader] Loading file: {file_path.name}")
        return asyncio.run(self._async_load_file(table_info, file_path, file_loader, chunk_size, max_retries))
    
    async def _async_load_file(
        self,
        table_info: TableInfo,
        file_path: Path,
        file_loader: FileLoader,
        chunk_size: Optional[int],
        max_retries: int
    ) -> Tuple[bool, Optional[str], int]:
        """Async file loading using lab components."""
        
        # Create asyncpg pool
        pool = await self._create_async_pool()
        
        try:
            # Get configuration and metadata
            config_params = self._get_config_params(chunk_size)
            primary_keys = extract_primary_keys(table_info)
            column_types = get_column_types_mapping(table_info)
            
            # Log configuration
            self._log_config_params(config_params)
            
            # Create batch generator with transforms
            batch_gen = self._create_batch_generator(file_path, file_loader, table_info, config_params['chunk_size'])
            
            # Execute async upsert
            rows_processed = await self._execute_async_upsert(
                pool, file_path, table_info, primary_keys, batch_gen, 
                config_params, column_types, max_retries
            )
            
            logger.info(f"[UnifiedLoader] Successfully processed {rows_processed:,} rows from {file_path.name}")
            return True, None, rows_processed
            
        except Exception as e:
            logger.error(f"[UnifiedLoader] Async loading failed for {file_path.name}: {e}")
            return False, str(e), 0
        
        finally:
            # Always close the pool
            await pool.close()
    
    async def _create_async_pool(self):
        """Create and return asyncpg pool."""
        return await create_asyncpg_pool_from_sqlalchemy(self.database, self.config)
    
    def _get_config_params(self, chunk_size: Optional[int]) -> dict:
        """Get configuration parameters with defaults and memory optimization."""
        base_chunk_size = chunk_size or getattr(self.config.etl, 'chunk_size', 50000)
        
        # Memory optimization for large files
        if hasattr(self, 'database') and self.config:
            # Check if we're dealing with a large file by looking at recent file operations
            # This is a heuristic - in production you might want more sophisticated detection
            if base_chunk_size > 50000:
                base_chunk_size = min(base_chunk_size, 50000)
                logger.info(f"[MEMORY] Reduced chunk_size to {base_chunk_size} for memory efficiency")
        
        return {
            'chunk_size': base_chunk_size,
            'sub_batch_size': getattr(self.config.etl, 'sub_batch_size', 5000),
            'enable_parallelism': getattr(self.config.etl, 'enable_internal_parallelism', True),
            'internal_concurrency': min(getattr(self.config.etl, 'internal_concurrency', 3), 2)  # Cap at 2 for safety
        }
    
    def _log_config_params(self, config_params: dict):
        """Log configuration parameters."""
        logger.info(f"[UnifiedLoader] Processing with chunk_size={config_params['chunk_size']}, "
                   f"sub_batch_size={config_params['sub_batch_size']}, "
                   f"parallelism={config_params['enable_parallelism']}")
    
    def _create_batch_generator(self, _file_path: Path, file_loader: FileLoader, 
                               table_info: TableInfo, chunk_size: int):
        """Create batch generator function with transforms."""
        def create_batch_gen(_file_path: str, headers: List[str], _chunk_size: int):
            return self._apply_transforms_to_batches(
                file_loader.batch_generator(headers, chunk_size), 
                table_info, 
                headers
            )
        return create_batch_gen
    
    async def _execute_async_upsert(self, pool, file_path: Path, table_info: TableInfo,
                                   primary_keys: List[str], batch_gen, config_params: dict,
                                   column_types: dict, max_retries: int) -> int:
        """Execute async upsert operation."""
        return await async_upsert(
            pool=pool,
            file_path=str(file_path),
            headers=table_info.columns,
            table=table_info.table_name,
            primary_keys=primary_keys,
            batch_gen=batch_gen,
            chunk_size=config_params['chunk_size'],
            sub_batch_size=config_params['sub_batch_size'],
            max_retries=max_retries,
            types=column_types,
            enable_internal_parallelism=config_params['enable_parallelism'],
            internal_concurrency=config_params['internal_concurrency']
        )
    
    def _apply_transforms_to_batches(self, batch_generator, table_info: TableInfo, headers: List[str]):
        """Apply row-level transforms from table_info.transform_map to each batch."""
        transform_func = getattr(table_info, 'transform_map', None)

        # Import default transform for proper identity comparison
        from ..core.transforms import default_transform_map

        if not transform_func or transform_func is default_transform_map:
            # No transforms needed, yield batches as-is
            logger.debug(f"[UnifiedLoader] No transforms for table {table_info.table_name}")
            for batch in batch_generator:
                yield batch
        else:
            # Apply transforms to each row using shared utility
            logger.info(f"[UnifiedLoader] ✅ APPLYING TRANSFORMS: {transform_func.__name__} to {table_info.table_name}")

            for batch in batch_generator:
                yield apply_transforms_to_batch(table_info, batch, headers)
    
    # Compatibility methods for existing code that expects specific method names
    def load_csv_file(
        self,
        table_info: TableInfo,
        csv_file: Union[str, Path],
        chunk_size: int = 50000,
        max_retries: int = 3,
    ) -> Tuple[bool, Optional[str], int]:
        """
        Compatibility method for existing CSV loading code.
        Delegates to load_file() with enhanced detection.
        """
        logger.debug(f"[UnifiedLoader] CSV compatibility mode for {Path(csv_file).name}")
        return self.load_file(table_info, csv_file, chunk_size, max_retries)
    
    def load_parquet_file(
        self,
        table_info: TableInfo,
        parquet_file: Union[str, Path],
        chunk_size: int = 50000,
        max_retries: int = 3,
    ) -> Tuple[bool, Optional[str], int]:
        """
        Compatibility method for existing Parquet loading code.
        Delegates to load_file() with enhanced detection.
        """
        logger.debug(f"[UnifiedLoader] Parquet compatibility mode for {Path(parquet_file).name}")
        return self.load_file(table_info, parquet_file, chunk_size, max_retries)


class LargeFileLoader(BaseFileLoader):
    """
    Memory-efficient synchronous loader for large files (4GB+).

    This loader avoids async processing and connection pooling issues that
    can cause OOM kills with very large files. Use this instead of UnifiedLoader
    for files larger than ~2GB.

    Key optimizations:
    - Synchronous processing (no async memory accumulation)
    - Small chunk sizes (100-500 rows)
    - Proper SQL parameter binding with dictionaries
    - Transform application with error handling
    - Minimal memory footprint
    """

    def __init__(self, database: Database, config=None):
        BaseFileLoader.__init__(self, logger_prefix="LargeFileLoader")
        self.database = database
        self.config = config
        logger.info("[LargeFileLoader] Initialized with memory-safe synchronous processing")

    def load_file(
        self,
        table_info: TableInfo,
        file_path: Union[str, Path],
        chunk_size: Optional[int] = 200,
        max_batches: Optional[int] = None,
        apply_transforms: bool = True,
    ) -> Tuple[bool, Optional[str], int]:
        """
        Load large file with memory-safe synchronous processing.

        Args:
            table_info: Table information with columns, encoding, etc.
            file_path: Path to file (CSV or Parquet)
            chunk_size: Number of rows per batch (default: 200 for memory safety)
            max_batches: Maximum number of batches to process (None for all)
            apply_transforms: Whether to apply row-level transforms

        Returns:
            Tuple of (success, error_message, rows_processed)
        """
        try:
            # Validate file and get configuration
            file_path = self._validate_file(file_path)
            if not file_path:
                return False, "File not found", 0
            
            encoding = self._get_encoding(table_info)
            
            # Log configuration
            self._log_large_file_config(file_path, chunk_size, apply_transforms)
            
            # Create file loader and detect format
            file_loader = self._create_file_loader(file_path, encoding)
            self._detect_format(file_loader, file_path)
            
            # Execute synchronous load
            return self._execute_sync_load(table_info, file_path, file_loader, 
                                         chunk_size, max_batches, apply_transforms)
            
        except Exception as e:
            error_msg = f"Failed to load {Path(file_path).name}: {e}"
            logger.error(f"[LargeFileLoader] {error_msg}")
            return False, error_msg, 0

    def _log_large_file_config(self, file_path: Path, chunk_size: int, apply_transforms: bool):
        """Log large file loading configuration."""
        logger.info(f"[LargeFileLoader] Loading file: {file_path.name}")
        logger.info(f"[LargeFileLoader] Using chunk_size={chunk_size}, apply_transforms={apply_transforms}")
    
    def _execute_sync_load(self, table_info: TableInfo, file_path: Path, file_loader: FileLoader,
                          chunk_size: int, max_batches: Optional[int], apply_transforms: bool) -> Tuple[bool, Optional[str], int]:
        """Execute synchronous file loading."""
        return self._sync_load_file(table_info, file_path, file_loader, chunk_size, max_batches, apply_transforms)

    def _sync_load_file(
        self,
        table_info: TableInfo,
        file_path: Path,
        file_loader: FileLoader,
        chunk_size: int,
        max_batches: Optional[int],
        apply_transforms: bool
    ) -> Tuple[bool, Optional[str], int]:
        """Synchronous file loading with memory safety."""

        import time
        start_time = time.time()
        batch_count = 0
        total_rows = 0

        try:
            for batch in file_loader.batch_generator(table_info.columns, chunk_size):
                batch_count += 1
                batch_rows = len(batch)
                total_rows += batch_rows

                # Progress reporting
                if batch_count % 10 == 0:
                    elapsed = time.time() - start_time
                    logger.info(f"[LargeFileLoader] Batch {batch_count}: {total_rows:,} rows, {elapsed:.1f}s, {total_rows/elapsed:.0f} rows/sec")

                # Apply transforms if requested
                if apply_transforms and table_info.transform_map:
                    batch = self._apply_transforms_to_batch(table_info, batch)

                # Insert batch synchronously
                self._insert_batch_synchronously(table_info, batch)

                # Stop if max_batches reached
                if max_batches and batch_count >= max_batches:
                    break

            elapsed = time.time() - start_time
            logger.info(f"[LargeFileLoader] Successfully processed {total_rows:,} rows from {file_path.name} in {elapsed:.1f} seconds")
            return True, None, total_rows

        except Exception as e:
            logger.error(f"[LargeFileLoader] Synchronous loading failed for {file_path.name}: {e}")
            return False, str(e), total_rows

    def _apply_transforms_to_batch(self, table_info: TableInfo, batch: List[Tuple]) -> List[Tuple]:
        """Apply row-level transforms to a batch using shared utility."""
        return apply_transforms_to_batch(table_info, batch, table_info.columns)

    def _insert_batch_synchronously(self, table_info: TableInfo, batch: List[Tuple]):
        """Insert a batch of rows synchronously with proper SQL formatting and UPSERT logic."""
        if not batch:
            return

        with self.database.engine.connect() as conn:
            # Prepare INSERT statement with named parameters and UPSERT logic
            columns = table_info.columns
            placeholders = ', '.join(f':{col}' for col in columns)
            columns_str = ', '.join(columns)

            # Get primary key columns using metadata introspection
            conflict_columns = get_primary_key_columns(
                table_info.table_name, 
                table_model=table_info.table_model
            )

            if conflict_columns:
                conflict_clause = f"ON CONFLICT ({', '.join(conflict_columns)}) DO UPDATE SET "
                update_parts = []
                for col in columns:
                    if col not in conflict_columns:
                        update_parts.append(f"{col} = EXCLUDED.{col}")
                if update_parts:
                    conflict_clause += ', '.join(update_parts)
                else:
                    # If all columns are in the primary key, just do nothing
                    conflict_clause = f"ON CONFLICT ({', '.join(conflict_columns)}) DO NOTHING"
            else:
                conflict_clause = ""

            sql = f'INSERT INTO {table_info.table_name} ({columns_str}) VALUES ({placeholders}) {conflict_clause}'

            # Insert each row as a dictionary (SQLAlchemy requirement)
            for row_tuple in batch:
                try:
                    # Convert tuple to dict for SQLAlchemy parameter binding
                    row_dict = dict(zip(columns, row_tuple))
                    conn.execute(text(sql), row_dict)
                except Exception as e:
                    # Log warning but continue with other rows
                    logger.warning(f"[LargeFileLoader] Failed to insert row in {table_info.table_name}: {e}")
                    continue

            conn.commit()


