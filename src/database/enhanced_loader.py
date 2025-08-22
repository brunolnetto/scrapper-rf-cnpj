"""
enhanced_loader.py
Complete replacement for UnifiedLoader using lab/refactored_fileloader components.
Provides high-performance async loading with robust file detection and internal parallelism.
"""
import asyncio
from pathlib import Path
from typing import Tuple, Optional, Union, List
from ..setup.logging import logger
from ..core.schemas import TableInfo
from ..database.schemas import Database
from ..utils.file_loader.file_loader import FileLoader
from ..utils.file_loader.uploader import async_upsert
from ..utils.file_loader.connection_factory import (
    create_asyncpg_pool_from_sqlalchemy, 
    extract_primary_keys,
    get_column_types_mapping
)


class EnhancedUnifiedLoader:
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
        self.database = database
        self.config = config
        logger.info("[EnhancedLoader] Initialized with async support")
    
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
        file_path = Path(file_path)
        if not file_path.exists():
            return False, f"File not found: {file_path}", 0
        
        try:
            # Get encoding from table_info if available
            encoding = getattr(table_info, 'encoding', None)
            if encoding and hasattr(encoding, 'value'):
                encoding = encoding.value
            else:
                encoding = 'utf-8'  # Default encoding
            
            logger.info(f"[EnhancedLoader] Loading file: {file_path.name} (encoding: {encoding})")
            
            # Use lab's robust file detection
            file_loader = FileLoader(str(file_path), encoding=encoding)
            detected_format = file_loader.get_format()
            
            logger.info(f"[EnhancedLoader] Detected format: {detected_format}")
            
            # Load using async processing
            return asyncio.run(self._async_load_file(table_info, file_path, file_loader, chunk_size, max_retries))
            
        except Exception as e:
            error_msg = f"Failed to load {file_path.name}: {e}"
            logger.error(f"[EnhancedLoader] {error_msg}")
            return False, error_msg, 0
    
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
        pool = await create_asyncpg_pool_from_sqlalchemy(self.database, self.config)
        
        try:
            # Get configuration parameters
            chunk_size = chunk_size or getattr(self.config.etl, 'chunk_size', 50000)
            sub_batch_size = getattr(self.config.etl, 'sub_batch_size', 5000)
            enable_parallelism = getattr(self.config.etl, 'enable_internal_parallelism', True)
            internal_concurrency = getattr(self.config.etl, 'internal_concurrency', 3)
            
            # Get table metadata
            primary_keys = extract_primary_keys(table_info)
            column_types = get_column_types_mapping(table_info)
            
            logger.info(f"[EnhancedLoader] Processing with chunk_size={chunk_size}, "
                       f"sub_batch_size={sub_batch_size}, parallelism={enable_parallelism}")
            
            # Create a compatible batch generator function for async_upsert with transforms
            def create_batch_gen(headers: List[str], chunk_size: int):
                return self._apply_transforms_to_batches(
                    file_loader.batch_generator(headers, chunk_size), 
                    table_info, 
                    headers
                )
            
            # Use lab's high-performance async uploader
            rows_processed = await async_upsert(
                pool=pool,
                file_path=str(file_path),
                headers=table_info.columns,
                table=table_info.table_name,
                primary_keys=primary_keys,
                batch_gen=create_batch_gen,
                chunk_size=chunk_size,
                sub_batch_size=sub_batch_size,
                max_retries=max_retries,
                types=column_types,
                enable_internal_parallelism=enable_parallelism,
                internal_concurrency=internal_concurrency
            )
            
            logger.info(f"[EnhancedLoader] Successfully processed {rows_processed:,} rows from {file_path.name}")
            return True, None, rows_processed
            
        except Exception as e:
            logger.error(f"[EnhancedLoader] Async loading failed for {file_path.name}: {e}")
            return False, str(e), 0
        
        finally:
            # Always close the pool
            await pool.close()
    
    def _apply_transforms_to_batches(self, batch_generator, table_info: TableInfo, headers: List[str]):
        """Apply row-level transforms from table_info.transform_map to each batch."""
        transform_func = getattr(table_info, 'transform_map', None)
        
        if not transform_func or transform_func.__name__ == 'default_transform_map':
            # No transforms needed, yield batches as-is
            logger.debug(f"[EnhancedLoader] No transforms for table {table_info.table_name}")
            for batch in batch_generator:
                yield batch
        else:
            # Apply transforms to each row
            logger.info(f"[EnhancedLoader] Applying {transform_func.__name__} transforms to {table_info.table_name}")
            
            for batch in batch_generator:
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
                        logger.warning(f"[EnhancedLoader] Transform failed for row in {table_info.table_name}: {e}")
                        transformed_batch.append(row_tuple)
                
                yield transformed_batch
    
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
        logger.debug(f"[EnhancedLoader] CSV compatibility mode for {Path(csv_file).name}")
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
        logger.debug(f"[EnhancedLoader] Parquet compatibility mode for {Path(parquet_file).name}")
        return self.load_file(table_info, parquet_file, chunk_size, max_retries)