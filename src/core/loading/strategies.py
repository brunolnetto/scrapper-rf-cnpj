"""
Simplified data loading strategies using only the enhanced loader.
All complexity removed - now just a clean interface to EnhancedUnifiedLoader.
"""
from abc import ABC, abstractmethod
from typing import List, Tuple, Optional

from ...database.schemas import Database
from ...setup.logging import logger
from ...setup.config import PathConfig
from ...database.dml import table_name_to_table_info

class BaseDataLoadingStrategy(ABC):
    """Abstract base class for data loading strategies."""
    
    @abstractmethod
    def load_table(self, database: Database, table_name: str, path_config: PathConfig, 
                   table_files: Optional[List[str]] = None) -> Tuple[bool, Optional[str], int]:
        """Load a single table."""
        pass

class DataLoadingStrategy(BaseDataLoadingStrategy):
    """Simplified loading strategy using only EnhancedUnifiedLoader."""
    
    def __init__(self, config, audit_service=None):
        self.config = config
        self.audit_service = audit_service
        # Keep for compatibility but simplified
        self.parallel_enabled = config.etl.is_parallel
        self.internal_concurrency = config.etl.internal_concurrency
        logger.info("Loading strategy initialized (simplified architecture)")
        
    def load_table(self, database: Database, table_name: str, path_config: PathConfig, 
                   table_files: Optional[List[str]] = None) -> Tuple[bool, Optional[str], int]:
        """Load table using EnhancedUnifiedLoader only."""
        
        logger.info(f"[LoadingStrategy] Loading table '{table_name}' with UnifiedLoader")
        
        try:
            table_info = table_name_to_table_info(table_name)
            from ...database.dml import UnifiedLoader
            loader = UnifiedLoader(database, self.config)
            
            # Check Parquet first (maintain existing priority)
            parquet_file = path_config.conversion_path / f"{table_name}.parquet"
            if parquet_file.exists():
                logger.info(f"[LoadingStrategy] Loading Parquet file: {parquet_file.name}")
                return self._create_manifest_and_load(loader, table_info, parquet_file, table_name)
            
            elif table_files:
                logger.info(f"[LoadingStrategy] Loading {len(table_files)} CSV files for table: {table_name}")
                return self._load_csv_files(loader, table_info, path_config, table_files, table_name)
            
            else:
                return False, f"No files found for table {table_name}", 0
                
        except Exception as e:
            logger.error(f"[LoadingStrategy] Failed to load table '{table_name}': {e}")
            return False, str(e), 0
    
    def _create_manifest_and_load(self, loader, table_info, file_path, table_name):
        """Create manifest entry and load file."""
        # Create manifest entry (start processing)
        self._create_manifest_entry(str(file_path), table_name, "PROCESSING", 0)
        
        try:
            success, error, rows = loader.load_file(table_info, file_path)
            
            # Update manifest entry (completed)
            status = "COMPLETED" if success else "FAILED" 
            error_msg = str(error) if error else None
            self._create_manifest_entry(str(file_path), table_name, status, rows, error_msg)
            
            return success, error, rows
            
        except Exception as e:
            self._create_manifest_entry(str(file_path), table_name, "FAILED", 0, str(e))
            raise
    
    def _load_csv_files(self, loader, table_info, path_config, table_files, table_name):
        """Load multiple CSV files."""
        total_rows = 0
        
        for filename in table_files:
            csv_file = path_config.extract_path / filename
            logger.info(f"[LoadingStrategy] Processing CSV file: {filename}")
            
            success, error, rows = self._create_manifest_and_load(loader, table_info, csv_file, table_name)
            
            if not success:
                logger.error(f"[LoadingStrategy] Failed to load {filename}: {error}")
                return success, error, total_rows
            
            total_rows += rows
            logger.info(f"[LoadingStrategy] Successfully loaded {rows:,} rows from {filename}")
        
        return True, None, total_rows
    
    def _create_manifest_entry(self, file_path: str, table_name: str, status: str, 
                              rows_processed: Optional[int] = None, error_msg: Optional[str] = None) -> None:
        """Create manifest entry for loaded file."""
        # Log error messages if provided
        if error_msg and status in ["FAILED", "ERROR"]:
            logger.error(f"[LoadingStrategy] Error processing {file_path}: {error_msg}")
        
        if self.audit_service and hasattr(self.audit_service, 'create_file_manifest'):
            try:
                from pathlib import Path
                from datetime import datetime
                file_path_obj = Path(file_path)
                
                # Calculate file info if file exists
                checksum = None
                filesize = None
                if file_path_obj.exists():
                    filesize = file_path_obj.stat().st_size
                
                # Generate meaningful notes based on processing context
                notes_parts = []
                if rows_processed is not None:
                    notes_parts.append(f"Processed {rows_processed:,} rows")
                if filesize is not None:
                    notes_parts.append(f"File size: {filesize:,} bytes")
                if error_msg:
                    notes_parts.append(f"Error: {error_msg}")
                if status == "COMPLETED":
                    notes_parts.append("Successfully loaded")
                elif status == "FAILED":
                    notes_parts.append("Loading failed")
                elif status == "PARTIAL":
                    notes_parts.append("Partially loaded")
                
                notes = "; ".join(notes_parts) if notes_parts else None
                
                # Create manifest entry with table name and notes
                self.audit_service.create_file_manifest(
                    str(file_path_obj),
                    status=status,
                    checksum=checksum,
                    filesize=filesize,
                    rows=rows_processed,
                    table_name=table_name,
                    notes=notes
                )
                        
            except Exception as e:
                logger.warning(f"Failed to create manifest entry for {file_path}: {e}")
        else:
            logger.debug("No audit service available for manifest tracking")

    def load_multiple_tables(self, database, table_to_files, path_config: PathConfig):
        """Load multiple tables with simplified processing."""
        results = {}
        for table_name, zipfile_to_files in table_to_files.items():
            # Flatten the nested structure
            all_files = []
            for zip_filename, csv_files in zipfile_to_files.items():
                all_files.extend(csv_files)
            
            if not all_files:
                logger.warning(f"[LoadingStrategy] No files found for table '{table_name}'")
                results[table_name] = (False, "No files found", 0)
                continue
                
            results[table_name] = self.load_table(database, table_name, path_config, all_files)
        return results