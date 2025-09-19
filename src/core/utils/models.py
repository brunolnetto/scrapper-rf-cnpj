"""
Utility functions to derive table information from SQLAlchemy models.
"""

from typing import List, Dict, Set
from sqlalchemy.ext.declarative import DeclarativeMeta
from ...database.models import MainBase


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
