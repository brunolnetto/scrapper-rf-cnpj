from sqlalchemy import inspect
from typing import List, Tuple

from ..core.constants import TABLES_INFO_DICT
from ..core.schemas import TableInfo
from ..core.utils import get_table_columns
from ..setup.logging import logger

from typing import List, Dict, Set
from sqlalchemy.ext.declarative import DeclarativeMeta
from .models.business import MainBase


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
    from ..utils.models import MainBase    
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


def get_model_by_table_name(table_name: str) -> DeclarativeMeta:
    """
    Get SQLAlchemy model by table name.
    
    Args:
        table_name: Name of the table
        
    Returns:
        SQLAlchemy model class
        
    Raises:
        ValueError: If table not found
    """
    for mapper in MainBase.registry.mappers:
        if mapper.class_.__tablename__ == table_name:
            return mapper.class_
    
    raise ValueError(f"Table '{table_name}' not found in SQLAlchemy models")


def get_table_columns(table_name: str) -> List[str]:
    """
    Get column names for a table from its SQLAlchemy model.
    
    Args:
        table_name: Name of the table
        
    Returns:
        List of column names
    """
    model = get_model_by_table_name(table_name)
    return [col.name for col in model.__table__.columns]


def get_table_index_columns(table_name: str) -> List[str]:
    """
    Get index column names for a table from its SQLAlchemy model.
    
    Args:
        table_name: Name of the table
        
    Returns:
        List of column names that have indexes
    """
    model = get_model_by_table_name(table_name)
    index_columns = set()
    
    # Get columns from defined indexes
    for index in model.__table__.indexes:
        for column in index.columns:
            index_columns.add(column.name)
    
    # Get primary key columns (they have implicit indexes)
    for column in model.__table__.primary_key:
        index_columns.add(column.name)
    
    return list(index_columns)


def get_tables_to_indices() -> Dict[str, Set[str]]:
    """
    Get mapping of table names to their index columns derived from SQLAlchemy models.
    
    Returns:
        Dict mapping table names to sets of index column names
    """
    tables_to_indices = {}
    
    for mapper in MainBase.registry.mappers:
        table_name = mapper.class_.__tablename__
        index_columns = get_table_index_columns(table_name)
        if index_columns:
            tables_to_indices[table_name] = set(index_columns)
    
    return tables_to_indices

def extract_primary_keys(table_info: TableInfo) -> list[str]:
    """
    Extract primary key columns from table info.
    
    Args:
        table_info: TableInfo object with table metadata
        
    Returns:
        List of primary key column names
    """
    try:
        # Try to get primary keys from SQLAlchemy model
        model = get_model_by_table_name(table_info.table_name)
        pk_columns = [col.name for col in model.__table__.primary_key.columns]
        if pk_columns:
            logger.debug(f"[ConnectionFactory] Extracted primary keys for {table_info.table_name}: {pk_columns}")
            return pk_columns
    except Exception as e:
        logger.warning(f"[ConnectionFactory] Failed to extract primary keys for {table_info.table_name}: {e}")
    
    # Fallback: try to get from table_info directly
    try:
        pk_columns = [col.name for col in table_info.table_model.__table__.primary_key.columns]
        if pk_columns:
            logger.debug(f"[ConnectionFactory] Fallback primary keys for {table_info.table_name}: {pk_columns}")
            return pk_columns
    except Exception as e:
        logger.warning(f"[ConnectionFactory] Fallback primary key extraction failed for {table_info.table_name}: {e}")
    
    logger.error(f"[ConnectionFactory] Could not determine primary keys for {table_info.table_name}")
    return []


def get_column_types_mapping(table_info: TableInfo) -> dict[str, str]:
    """
    Get PostgreSQL column type mapping from table info.
    
    Args:
        table_info: TableInfo object with table metadata
        
    Returns:
        Dictionary mapping column names to PostgreSQL types
    """
    try:
        model = get_model_by_table_name(table_info.table_name)
        
        types = {}
        for column in model.__table__.columns:
            # Map SQLAlchemy types to PostgreSQL types
            col_type = str(column.type).upper()
            if any(t in col_type for t in ['VARCHAR', 'TEXT', 'STRING']):
                types[column.name] = 'TEXT'
            elif any(t in col_type for t in ['FLOAT', 'DOUBLE', 'NUMERIC', 'DECIMAL']):
                types[column.name] = 'DOUBLE PRECISION'
            elif any(t in col_type for t in ['INTEGER', 'BIGINT', 'INT']):
                types[column.name] = 'BIGINT'
            elif 'TIMESTAMP' in col_type:
                types[column.name] = 'TIMESTAMP WITH TIME ZONE'
            elif 'DATE' in col_type:
                types[column.name] = 'DATE'
            else:
                types[column.name] = 'TEXT'  # Safe default
        
        return types
        
    except Exception as e:
        logger.warning(f"Could not get column types for {table_info.table_name}: {e}")
        # Fallback to all TEXT
        return {col: 'TEXT' for col in table_info.columns}
