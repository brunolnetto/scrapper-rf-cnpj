"""
Data loading strategies for the CNPJ ETL project.

Implements the strategy pattern for loading data into the database using a unified loader
that supports both CSV and Parquet formats with auto-detection.
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Tuple, Optional

from ...database.schemas import Database
from ...database.dml import table_name_to_table_info, UnifiedLoader
from ...setup.logging import logger
from ...setup.config import PathConfig

class BaseDataLoadingStrategy(ABC):
    """Abstract base class for data loading strategies."""
    
    @abstractmethod
    def load_table(
        self, 
        database: Database, 
        table_name: str, 
        path_config: PathConfig,
        table_files: Optional[List[str]] = None
    ) -> Tuple[bool, Optional[str], int]:
        """Load a single table and return (success, error, rows_loaded)."""
        pass
    
    @abstractmethod
    def load_multiple_tables(
        self,
        database: Database,
        table_to_files: Dict[str, List[str]],
        path_config: PathConfig
    ) -> Dict[str, Tuple[bool, Optional[str], int]]:
        """Load multiple tables and return results dict."""
        pass


class DataLoadingStrategy(BaseDataLoadingStrategy):
    """Auto-detecting loading strategy using unified loader (Parquet preferred if available)."""

    def load_table(self, database: Database, table_name: str, path_config: PathConfig, table_files=None):
        logger.info(f"[Auto] Started Loading table '{table_name}':")

        try:
            # Use unified loader with auto-detection
            table_info = table_name_to_table_info(table_name)
            loader = UnifiedLoader(database)

            # Auto-detect file format
            parquet_file = path_config.conversion_path / f"{table_name}.parquet"

            if parquet_file.exists():
                logger.info(f"Using Parquet file for table '{table_name}': {parquet_file}")
                success, error, total_rows = loader.load_parquet_file(table_info, parquet_file)
                logger.info(f"[Auto] Completed loading table '{table_name}' using Parquet")
                return success, error, total_rows

            elif table_files:
                # Load multiple CSV files
                logger.info(f"Using CSV files for table '{table_name}': {table_files}")
                total_rows = 0
                for filename in table_files:
                    csv_file = path_config.extract_path / filename
                    success, error, rows = loader.load_csv_file(table_info, csv_file)
                    if not success:
                        return success, error, total_rows
                    total_rows += rows

                    logger.info(f"[Auto] Completed loading table '{table_name}' using CSV")
                return True, None, total_rows
            else:
                # Panicking
                logger.error(f"No valid files found for table '{table_name}'")
                return False, f"No valid files found for table {table_name}", 0

            return success, error, rows
            
        except Exception as e:
            logger.error(f"[Auto] Failed to load table '{table_name}': {e}")
            return False, str(e), 0

    def load_multiple_tables(self, database, table_to_files, path_config: PathConfig):
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
                
            results[table_name] = self.load_table(database, table_name, path_config, all_files)
        return results

