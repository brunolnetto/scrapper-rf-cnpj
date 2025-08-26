"""
Unified data loading utilities for CNPJ ETL.

This module provides efficient data loading for both CSV and Parquet files.
Now uses the enhanced loader from lab/refactored_fileloader for high-performance async processing.
"""
import asyncio
from pathlib import Path
from typing import Tuple, Optional, Union, List
from sqlalchemy import text

from ..setup.logging import logger
from ..core.constants import TABLES_INFO_DICT
from ..core.schemas import TableInfo
from ..utils.model_utils import get_table_columns
from .schemas import Database
from ..utils.file_loader.file_loader import FileLoader
from ..utils.file_loader.uploader import async_upsert
from ..utils.file_loader.connection_factory import (
    create_asyncpg_pool_from_sqlalchemy, 
    extract_primary_keys,
    get_column_types_mapping
)


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

    # Create table info object
    return TableInfo(
        label, zip_group, table_name, columns, encoding, transform_map, expression
    )


def generate_tables_indices(engine, tables_to_indices):
    """
    Generates indices for the database tables.

    Args:
        engine: The database engine.
        tables_to_indices: Dict mapping table names to lists of column names for indexing.

    Returns:
        None
    """

    # Criar índices na base de dados:
    logger.info(
        f"Generating indices on database tables {list(tables_to_indices.keys())}"
    )

    # Index metadata
    fields_list = [
        (table_name, column_name, f"{table_name}_{column_name}")
        for table_name, columns in tables_to_indices.items()
        for column_name in columns
    ]
    mask = 'create index if not exists {index_name} on {table_name} using btree("{column_name}");'

    # Execute index queries
    try:
        with engine.connect() as conn:
            for table_name_, column_name_, index_name_ in fields_list:
                # Compile a SQL string
                query_str = mask.format(
                    table_name=table_name_,
                    column_name=column_name_,
                    index_name=index_name_,
                )
                query = text(query_str)
                print(query_str)
                # Execute the compiled SQL string
                try:
                    conn.execute(query)
                    print(
                        f"Index {index_name_} created on column {column_name_} of table {table_name_}."
                    )

                except Exception as e:
                    msg = f"Error generating index {index_name_} on column `{column_name_}` for table {table_name_}"
                    logger.error(f"{msg}: {e}")

                message = f"Index {index_name_} generated on column `{column_name_}` for table {table_name_}"
                logger.info(message)

        # Commit all index creations at once
        conn.commit()
        message = f"Index created on tables {list(tables_to_indices.keys())}"
        logger.info(message)

    except Exception as e:
        logger.error(f"Failed to generate indices: {e}")


class UnifiedLoader:
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
            
            logger.info(f"[UnifiedLoader] Loading file: {file_path.name} (encoding: {encoding})")
            
            # Use lab's robust file detection
            file_loader = FileLoader(str(file_path), encoding=encoding)
            detected_format = file_loader.get_format()
            
            logger.info(f"[UnifiedLoader] Detected format: {detected_format}")
            
            # Load using async processing
            return asyncio.run(self._async_load_file(table_info, file_path, file_loader, chunk_size, max_retries))
            
        except Exception as e:
            error_msg = f"Failed to load {file_path.name}: {e}"
            logger.error(f"[UnifiedLoader] {error_msg}")
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

            logger.info(f"[UnifiedLoader] Processing with chunk_size={chunk_size}, "
                       f"sub_batch_size={sub_batch_size}, parallelism={enable_parallelism}")
            
            # Create a compatible batch generator function for async_upsert with transforms
            def create_batch_gen(file_path: str, headers: List[str], chunk_size: int):
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
            
            logger.info(f"[UnifiedLoader] Successfully processed {rows_processed:,} rows from {file_path.name}")
            return True, None, rows_processed
            
        except Exception as e:
            logger.error(f"[UnifiedLoader] Async loading failed for {file_path.name}: {e}")
            return False, str(e), 0
        
        finally:
            # Always close the pool
            await pool.close()
    
    def _apply_transforms_to_batches(self, batch_generator, table_info: TableInfo, headers: List[str]):
        """Apply row-level transforms from table_info.transform_map to each batch."""
        transform_func = getattr(table_info, 'transform_map', None)
        
        if not transform_func or transform_func.__name__ == 'default_transform_map':
            # No transforms needed, yield batches as-is
            logger.debug(f"[UnifiedLoader] No transforms for table {table_info.table_name}")
            for batch in batch_generator:
                yield batch
        else:
            # Apply transforms to each row
            logger.info(f"[UnifiedLoader] Applying {transform_func.__name__} transforms to {table_info.table_name}")
            
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
                        logger.warning(f"[UnifiedLoader] Transform failed for row in {table_info.table_name}: {e}")
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


