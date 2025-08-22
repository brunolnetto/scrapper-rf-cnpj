"""
Unified data loading utilities for CNPJ ETL.

This module provides efficient data loading for both CSV and Parquet files.
Now uses the enhanced loader from lab/refactored_fileloader for high-performance async processing.
"""

from sqlalchemy import text

from ..setup.logging import logger
from ..core.constants import TABLES_INFO_DICT
from ..core.schemas import TableInfo
from ..utils.model_utils import get_table_columns
from .schemas import Database
from .enhanced_loader import EnhancedUnifiedLoader

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


# Legacy UnifiedLoader - now a simple factory for EnhancedUnifiedLoader
def UnifiedLoader(database: Database, config=None):
    """
    Factory function for backward compatibility.
    Creates and returns EnhancedUnifiedLoader instance.
    
    Args:
        database: Database instance
        config: Optional configuration (recommended for async features)
        
    Returns:
        EnhancedUnifiedLoader instance
    """
    logger.debug("[UnifiedLoader] Creating EnhancedUnifiedLoader instance")
    return EnhancedUnifiedLoader(database, config)
