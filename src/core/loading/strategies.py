"""
Data loading strategies for the CNPJ ETL project.

Implements the strategy pattern for loading data into the database using different backends (CSV, Parquet/DuckDB).
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Tuple, Optional
from pathlib import Path
from database.schemas import Database
from core.schemas import AuditMetadata
from setup.logging import logger

# Import existing table population functions
from database.dml import populate_table
from database.dml_duckdb import populate_table_duckdb

class DataLoadingStrategy(ABC):
    """Abstract base class for data loading strategies."""
    
    @abstractmethod
    def load_table(
        self, 
        database: Database, 
        table_name: str, 
        source_path: Path,
        table_files: Optional[List[str]] = None,
        **kwargs
    ) -> Tuple[bool, Optional[str], int]:
        """Load a single table and return (success, error, rows_loaded)."""
        pass
    
    @abstractmethod
    def load_multiple_tables(
        self,
        database: Database,
        table_to_files: Dict[str, List[str]],
        source_path: Path,
        **kwargs
    ) -> Dict[str, Tuple[bool, Optional[str], int]]:
        """Load multiple tables and return results dict."""
        pass

class CSVLoadingStrategy(DataLoadingStrategy):
    """CSV-based loading strategy using pandas."""
    
    def load_table(self, database, table_name, source_path, table_files=None, **kwargs):
        try:
            logger.info(f"[CSV] Loading table '{table_name}' from CSV files: {table_files}")
            populate_table(database, table_name, str(source_path), table_files or [])
            return True, None, -1  # Row count not tracked in current populate_table
        except Exception as e:
            logger.error(f"[CSV] Failed to load table '{table_name}': {e}")
            return False, str(e), 0
    
    def load_multiple_tables(self, database, table_to_files, source_path, **kwargs):
        results = {}
        for table_name, zipfile_to_files in table_to_files.items():
            # Flatten the nested structure: {zip_filename: [csv_files]} -> [csv_files]
            all_files = []
            for zip_filename, csv_files in zipfile_to_files.items():
                all_files.extend(csv_files)
            
            if not all_files:
                logger.warning(f"[CSV] No files found for table '{table_name}'")
                results[table_name] = (False, "No files found", 0)
                continue
                
            results[table_name] = self.load_table(database, table_name, source_path, all_files, **kwargs)
        return results

class ParquetLoadingStrategy(DataLoadingStrategy):
    """Parquet-based loading strategy using DuckDB."""
    
    def load_table(self, database, table_name, source_path, table_files=None, **kwargs):
        try:
            logger.info(f"[Parquet] Loading table '{table_name}' from Parquet directory: {source_path}")
            # table_files is ignored for Parquet; expects {table_name}.parquet in source_path
            success, error, rows = populate_table_duckdb(database, table_name, source_path)
            return success, error, rows
        except Exception as e:
            logger.error(f"[Parquet] Failed to load table '{table_name}': {e}")
            return False, str(e), 0
    
    def load_multiple_tables(self, database, table_to_files, source_path, **kwargs):
        results = {}
        for table_name in table_to_files.keys():
            results[table_name] = self.load_table(database, table_name, source_path, None, **kwargs)
        return results

class AutoLoadingStrategy(DataLoadingStrategy):
    """Auto-detecting loading strategy (Parquet preferred if available)."""
    
    def __init__(self, prefer_parquet: bool = True):
        self.prefer_parquet = prefer_parquet
        self.csv_strategy = CSVLoadingStrategy()
        self.parquet_strategy = ParquetLoadingStrategy()
    
    def load_table(self, database, table_name, source_path, table_files=None, **kwargs):
        parquet_file = source_path / f"{table_name}.parquet"
        if self.prefer_parquet and parquet_file.exists():
            logger.info(f"[Auto] Using Parquet strategy for table '{table_name}'")
            return self.parquet_strategy.load_table(database, table_name, source_path, None, **kwargs)
        else:
            logger.info(f"[Auto] Using CSV strategy for table '{table_name}'")
            return self.csv_strategy.load_table(database, table_name, source_path, table_files, **kwargs)
    
    def load_multiple_tables(self, database, table_to_files, source_path, **kwargs):
        results = {}
        for table_name, zipfile_to_files in table_to_files.items():
            # Flatten the nested structure: {zip_filename: [csv_files]} -> [csv_files]
            all_files = []
            for zip_filename, csv_files in zipfile_to_files.items():
                all_files.extend(csv_files)
            
            if not all_files:
                logger.warning(f"[Auto] No files found for table '{table_name}'")
                results[table_name] = (False, "No files found", 0)
                continue
                
            results[table_name] = self.load_table(database, table_name, source_path, all_files, **kwargs)
        return results