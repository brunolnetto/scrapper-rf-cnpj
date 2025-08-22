"""
Enhanced data loading strategies using refactored file loader components.
"""
from abc import ABC, abstractmethod
from typing import Dict, List, Tuple, Optional
import asyncio
from pathlib import Path

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
    """Enhanced loading strategy with robust detection and optional async processing."""
    
    def __init__(self, config, audit_service=None):
        self.config = config
        self.audit_service = audit_service
        # Use ETL_IS_PARALLEL to control internal parallelism as well
        self.parallel_enabled = config.etl.is_parallel
        self.internal_concurrency = config.etl.internal_concurrency
        logger.info(f"Loading strategy initialized (parallel: {self.parallel_enabled}, internal_concurrency: {self.internal_concurrency})")
        
    def load_table(self, database: Database, table_name: str, path_config: PathConfig, 
                   table_files: Optional[List[str]] = None) -> Tuple[bool, Optional[str], int]:
        """Load table with enhanced detection and optional async processing."""
        
        logger.info(f"[Enhanced] Loading table '{table_name}' with enhanced strategy")
        
        try:
            table_info = table_name_to_table_info(table_name)
            
            # Check Parquet first (maintain existing priority)
            parquet_file = path_config.conversion_path / f"{table_name}.parquet"
            if parquet_file.exists():
                return self._load_parquet_enhanced(database, table_info, parquet_file)
            elif table_files:
                return self._load_csv_files_enhanced(database, table_info, path_config, table_files)
            else:
                return False, f"No files found for table {table_name}", 0
                
        except Exception as e:
            logger.error(f"[Enhanced] Failed to load table '{table_name}': {e}")
            # Fallback to standard loading
            return self._fallback_to_standard(database, table_name, path_config, table_files)
    
    def _load_parquet_enhanced(self, database, table_info, parquet_file):
        """Load Parquet file with enhanced validation and manifest tracking."""
        try:
            from ...utils.file_loader import FileLoader
            
            # Robust detection and validation
            loader = FileLoader(str(parquet_file))
            if loader.get_format() != 'parquet':
                raise ValueError(f"File validation failed: expected parquet, got {loader.get_format()}")
            
            logger.info(f"[Enhanced] Validated Parquet file: {parquet_file.name}")
            
            # Create manifest entry (start processing)
            self._create_manifest_entry(str(parquet_file), table_info.table_name, "PROCESSING", 0)
            
            # Use enhanced loading if parallel processing is enabled
            if self.parallel_enabled and self._should_use_async(table_info.table_name):
                success, error, rows = self._load_parquet_async(database, table_info, parquet_file, loader)
            else:
                # Use existing UnifiedLoader with enhanced confidence
                logger.info(f"[Enhanced] Using standard UnifiedLoader for Parquet: {parquet_file.name}")
                from ...database.dml import UnifiedLoader
                success, error, rows = UnifiedLoader(database).load_parquet_file(table_info, parquet_file)
            
            # Update manifest entry (completed)
            status = "COMPLETED" if success else "FAILED"
            error_msg = str(error) if error else None
            self._create_manifest_entry(str(parquet_file), table_info.table_name, status, rows, error_msg)
            
            return success, error, rows
        except Exception as e:
            logger.warning(f"[Enhanced] Parquet enhanced loading failed: {e}, falling back")
            self._create_manifest_entry(str(parquet_file), table_info.table_name, "FAILED", 0, str(e))
            return self._fallback_parquet_loading(database, table_info, parquet_file)
    
    def _create_manifest_entry(self, file_path: str, table_name: str, status: str, 
                              rows_processed: Optional[int] = None, error_msg: Optional[str] = None) -> None:
        """Create manifest entry for loaded file."""
        if self.audit_service and hasattr(self.audit_service, 'create_file_manifest'):
            try:
                from pathlib import Path
                file_path_obj = Path(file_path)
                
                # Calculate file info if file exists
                checksum = None
                filesize = None
                if file_path_obj.exists():
                    filesize = file_path_obj.stat().st_size
                    # Let the audit service calculate checksum internally
                
                # Create manifest entry with table name included
                self.audit_service.create_file_manifest(
                    str(file_path_obj),
                    status=status,
                    checksum=checksum,
                    filesize=filesize,
                    rows=rows_processed
                )
                
                # Update the manifest entry with table name
                self.audit_service.update_manifest_table_name(str(file_path_obj), table_name)
                        
            except Exception as e:
                logger.warning(f"Failed to create manifest entry for {file_path}: {e}")
        else:
            logger.debug("No audit service available for manifest tracking")

    def _fallback_parquet_loading(self, database, table_info, parquet_file):
        """Fallback Parquet loading using current UnifiedLoader."""
        from ...database.dml import UnifiedLoader
        return UnifiedLoader(database).load_parquet_file(table_info, parquet_file)
    
    def _load_csv_files_enhanced(self, database, table_info, path_config, table_files):
        """Load CSV files with enhanced detection, processing, and manifest tracking."""
        total_rows = 0
        
        for filename in table_files:
            csv_file = path_config.extract_path / filename
            
            try:
                from ...utils.file_loader import FileLoader
                
                # Robust detection and validation with encoding support
                loader = FileLoader(str(csv_file), encoding=table_info.encoding.value)
                if loader.get_format() != 'csv':
                    logger.warning(f"File {filename} detected as {loader.get_format()}, treating as CSV")
                
                # Create manifest entry (start processing)
                self._create_manifest_entry(str(csv_file), table_info.table_name, "PROCESSING", 0)
                
                # Use enhanced loading if parallel processing is enabled
                if self.parallel_enabled and self._should_use_async(table_info.table_name):
                    success, error, rows = self._load_csv_async(database, table_info, csv_file, loader)
                else:
                    # Use existing UnifiedLoader with enhanced confidence
                    logger.info(f"[Enhanced] Using standard UnifiedLoader for CSV: {filename}")
                    from ...database.dml import UnifiedLoader
                    success, error, rows = UnifiedLoader(database).load_csv_file(table_info, csv_file)
                
                if not success:
                    self._create_manifest_entry(str(csv_file), table_info.table_name, "FAILED", 0, str(error))
                    return success, error, total_rows
                
                # Update manifest entry (completed)
                self._create_manifest_entry(str(csv_file), table_info.table_name, "COMPLETED", rows)
                total_rows += rows
                
            except Exception as e:
                logger.warning(f"[Enhanced] CSV enhanced loading failed for {filename}: {e}")
                self._create_manifest_entry(str(csv_file), table_info.table_name, "FAILED", 0, str(e))
                # Fallback for this file
                success, error, rows = self._fallback_csv_loading(database, table_info, csv_file)
                if not success:
                    return success, error, total_rows
                total_rows += rows
        
        return True, None, total_rows
    
    def _should_use_async(self, table_name: str) -> bool:
        """Determine if async processing should be used for this table."""
        # Use async for large tables
        large_tables = {'empresa', 'estabelecimento', 'socios', 'simples'}
        should_use = table_name in large_tables
        logger.debug(f"[Enhanced] Async processing for {table_name}: {should_use}")
        return should_use
    
    def _should_use_high_performance(self, file_path, table_name: str) -> bool:
        """Determine if high-performance CLI loader should be used."""
        # Use high-performance loader for large files
        try:
            file_size = file_path.stat().st_size
            # Use for files larger than 100MB
            large_file_threshold = 100 * 1024 * 1024  # 100MB
            return file_size > large_file_threshold
        except:
            return False
    
    def _load_parquet_async(self, database, table_info, parquet_file, loader):
        """Load Parquet file using async processing."""
        import asyncio
        
        try:
            from ...utils.file_loader import AsyncDatabaseBridge
            
            logger.info(f"[Enhanced] Using async processing for Parquet: {parquet_file.name}")
            
            # Create async bridge
            bridge = AsyncDatabaseBridge(database, self.config)
            
            # Run async loading
            async def async_load():
                try:
                    return await bridge.load_file_async(
                        table_info=table_info,
                        file_path=str(parquet_file),
                        batch_generator_func=None,  # Will use parquet ingestor internally
                        encoding='utf-8'  # Not used for parquet but required
                    )
                finally:
                    await bridge.close()
            
            # Execute async code
            return asyncio.run(async_load())
            
        except Exception as e:
            logger.warning(f"[Enhanced] Async Parquet loading failed: {e}")
            logger.info(f"[Enhanced] Falling back to standard Parquet loading")
            return self._fallback_parquet_loading(database, table_info, parquet_file)
    
    def _load_csv_async(self, database, table_info, csv_file, loader):
        """Load CSV file using async processing."""
        import asyncio
        
        try:
            from ...utils.file_loader import AsyncDatabaseBridge
            
            logger.info(f"[Enhanced] Using async processing for CSV: {csv_file.name}")
            
            # Create async bridge
            bridge = AsyncDatabaseBridge(database, self.config)
            
            # Run async loading with table-specific encoding
            async def async_load():
                try:
                    return await bridge.load_file_async(
                        table_info=table_info,
                        file_path=str(csv_file),
                        batch_generator_func=None,  # Will use csv ingestor internally
                        encoding=table_info.encoding.value
                    )
                finally:
                    await bridge.close()
            
            # Execute async code
            return asyncio.run(async_load())
            
        except Exception as e:
            logger.warning(f"[Enhanced] Async CSV loading failed: {e}")
            logger.info(f"[Enhanced] Falling back to standard CSV loading")
            return self._fallback_csv_loading(database, table_info, csv_file)
    
    def _fallback_to_standard(self, database, table_name, path_config, table_files):
        """Fallback to standard loading strategy."""
        logger.info(f"[Enhanced] Falling back to standard loading for table '{table_name}'")
        from .strategies import DataLoadingStrategy
        standard_strategy = DataLoadingStrategy()
        return standard_strategy.load_table(database, table_name, path_config, table_files)
    
    def _fallback_parquet_loading(self, database, table_info, parquet_file):
        """Fallback Parquet loading using current UnifiedLoader."""
        from ...database.dml import UnifiedLoader
        return UnifiedLoader(database).load_parquet_file(table_info, parquet_file)
    
    def _fallback_csv_loading(self, database, table_info, csv_file):
        """Fallback CSV loading using current UnifiedLoader."""
        from ...database.dml import UnifiedLoader
        return UnifiedLoader(database).load_csv_file(table_info, csv_file)

    def load_multiple_tables(self, database, table_to_files, path_config: PathConfig):
        """Load multiple tables with enhanced processing."""
        results = {}
        for table_name, zipfile_to_files in table_to_files.items():
            # Flatten the nested structure
            all_files = []
            for zip_filename, csv_files in zipfile_to_files.items():
                all_files.extend(csv_files)
            
            if not all_files:
                logger.warning(f"[Enhanced] No files found for table '{table_name}'")
                results[table_name] = (False, "No files found", 0)
                continue
                
            results[table_name] = self.load_table(database, table_name, path_config, all_files)
        return results
