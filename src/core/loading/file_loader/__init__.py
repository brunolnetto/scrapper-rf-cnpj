"""
File loader package with sync and async capabilities.
Provides unified file loading with robust format detection and encoding support.
"""

from .file_loader import FileLoader
from .ingestors import batch_generator_csv, batch_generator_parquet
from .uploader import async_upsert
from .base import create_pool, map_types, ensure_table_sql, upsert_from_temp_sql

__all__ = [
    'FileLoader',
    'batch_generator_csv', 
    'batch_generator_parquet',
    'async_upsert',
    'create_pool',
    'map_types',
    'ensure_table_sql',
    'upsert_from_temp_sql'
]
