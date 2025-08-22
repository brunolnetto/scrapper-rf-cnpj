"""
File loader package with sync and async capabilities.
Provides unified file loading with robust format detection and encoding support.
"""

from .file_loader import FileLoader
from .ingestors import batch_generator_csv, batch_generator_parquet
from .uploader import AsyncFileUploader  
from .async_bridge import AsyncDatabaseBridge

__all__ = [
    'FileLoader',
    'batch_generator_csv', 
    'batch_generator_parquet',
    'AsyncFileUploader',
    'AsyncDatabaseBridge'
]
