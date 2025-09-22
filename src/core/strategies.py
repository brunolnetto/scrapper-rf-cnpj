from typing import Optional, Any
from datetime import datetime

from ..setup.logging import logger
from ..setup.config import ConfigurationService, AppConfig
from ..utils.db_admin import (
    create_database_if_not_exists,
    truncate_tables,
    get_table_row_counts,
)
from .interfaces import Pipeline, OrchestrationStrategy


class DownloadOnlyStrategy:
    """Strategy for download-only mode."""
    
    def get_name(self) -> str:
        return "Download Only"
    
    def validate_parameters(self, **kwargs) -> bool:
        return True  # No specific parameters required
    
    def execute(self, pipeline: Pipeline, config_service: ConfigurationService, **kwargs) -> Optional[Any]:
        logger.info("[DOWNLOAD-ONLY] Running in download-only mode...")
        
        if not hasattr(pipeline, 'retrieve_data'):
            logger.error("[DOWNLOAD-ONLY] Pipeline does not support download-only mode")
            return None
        
        try:
            audits = pipeline.retrieve_data()
            if audits:
                logger.info(f"[DOWNLOAD-ONLY] Successfully downloaded {len(audits)} files")
                logger.info(f"[DOWNLOAD-ONLY] Files saved to: {str(config_service.pipeline.get_temporal_download_path(config_service.year, config_service.month))}")
            else:
                logger.warning("[DOWNLOAD-ONLY] No files were downloaded")
            return audits
        except (OSError, IOError, ConnectionError, TimeoutError) as e:
            logger.error(f"[DOWNLOAD-ONLY] Download failed: {e}")
            raise


class DownloadAndLoadStrategy:
    """Strategy for downloading and loading data directly, skipping conversion."""
    
    def get_name(self) -> str:
        return "Download+Load"
    
    def validate_parameters(self, **kwargs) -> bool:
        return True  # No specific parameters required
    
    def execute(self, pipeline: Pipeline, config_service, **kwargs) -> Optional[Any]:
        logger.info("[DOWNLOAD+LOAD] Running download+load strategy (skip conversion)...")
        
        # Check if pipeline supports direct CSV loading
        if not hasattr(pipeline, 'load_csv_files_directly'):
            logger.warning("[DOWNLOAD+LOAD] Pipeline doesn't have direct CSV loading, using standard flow")
            return self._execute_standard_flow(pipeline, config_service, **kwargs)
        
        try:
            # Download data first
            downloaded_files = pipeline.retrieve_data()
            if not downloaded_files:
                logger.error("[DOWNLOAD+LOAD] Download failed, no files retrieved")
                return None
            
            logger.info(f"[DOWNLOAD+LOAD] Downloaded {len(downloaded_files)} file(s), proceeding with direct CSV loading")
            
            # Load CSV files directly without conversion
            audit_metadata = pipeline.load_csv_files_directly()
            if not audit_metadata:
                logger.warning("[DOWNLOAD+LOAD] Direct CSV loading returned no results")
                return None
                
            logger.info("[DOWNLOAD+LOAD] Successfully completed download+load without conversion")
            return audit_metadata
            
        except (OSError, IOError, ValueError, RuntimeError) as e:
            logger.error(f"[DOWNLOAD+LOAD] Strategy failed: {e}")
            logger.info("[DOWNLOAD+LOAD] Falling back to standard ETL flow...")
            return self._execute_standard_flow(pipeline, config_service, **kwargs)
    
    def _execute_standard_flow(self, pipeline: Pipeline, config_service, **kwargs) -> Optional[Any]:
        """Fallback to standard download -> convert -> load flow."""
        try:
            # Download
            downloaded_files = pipeline.retrieve_data()
            if not downloaded_files:
                return None
            
            # Create audit metadata for loading
            from pathlib import Path
            year = kwargs.get('year', config_service.year)
            month = kwargs.get('month', config_service.month)
            download_path = str(config_service.pipeline.get_temporal_download_path(year, month))
            audit_metadata = pipeline.audit_service.create_audit_metadata(downloaded_files, download_path)
            
            # Insert table audits before loading
            pipeline.audit_service.insert_audits(audit_metadata)
            
            # Convert
            converted_files = pipeline.convert_to_parquet(audit_metadata)
            if not converted_files:
                logger.warning("[DOWNLOAD+LOAD] Conversion failed in fallback")
                return None
            
            # Load data using the proper method
            loading_result = pipeline.data_loader.load_data(audit_metadata)
            return loading_result
            
        except (OSError, IOError, ConnectionError, TimeoutError, ValueError, RuntimeError) as e:
            logger.error(f"[DOWNLOAD+LOAD] Fallback strategy failed: {e}")
            raise


class DownloadAndConvertStrategy:
    """Strategy for download and convert to Parquet mode."""
    
    def get_name(self) -> str:
        return "Download and Convert"
    
    def validate_parameters(self, **kwargs) -> bool:
        return True  # No specific parameters required
    
    def execute(self, pipeline: Pipeline, config_service: ConfigurationService, **kwargs) -> Optional[Any]:
        logger.info("[DOWNLOAD-CONVERT] Running in download-and-convert mode...")
        
        required_methods = ['retrieve_data', 'convert_to_parquet', 'audit_service']
        for method in required_methods:
            if not hasattr(pipeline, method):
                logger.error(f"[DOWNLOAD-CONVERT] Pipeline missing required method: {method}")
                return None
        
        try:
            # Download files
            audits = pipeline.retrieve_data()
            if not audits:
                logger.warning("[DOWNLOAD-CONVERT] No files were downloaded")
                return None
            
            # Create audit metadata and convert to Parquet
            download_path = str(config_service.pipeline.get_temporal_download_path(config_service.year, config_service.month))
            audit_metadata = pipeline.audit_service.create_audit_metadata(audits, download_path)
            conversion_path = pipeline.convert_to_parquet(audit_metadata)
            
            logger.info(f"[DOWNLOAD-CONVERT] Successfully downloaded {len(audits)} files")
            logger.info(f"[DOWNLOAD-CONVERT] Files converted to Parquet and saved to: {conversion_path}")
            
            # Cleanup downloaded files if configured
            if config_service.pipeline.delete_files:
                from ..utils.misc import remove_folder
                remove_folder(download_path)
                logger.info("[DOWNLOAD-CONVERT] Cleaned up downloaded files")
            
            return conversion_path
        except (OSError, IOError, ConnectionError, TimeoutError, ValueError, RuntimeError) as e:
            logger.error(f"[DOWNLOAD-CONVERT] Process failed: {e}")
            raise


class ConvertOnlyStrategy:
    """Strategy for convert existing CSV files to Parquet mode."""
    
    def get_name(self) -> str:
        return "Convert Only"
    
    def validate_parameters(self, **kwargs) -> bool:
        return True  # No specific parameters required
    
    def execute(self, pipeline: Pipeline, _config_service: ConfigurationService, **kwargs) -> Optional[Any]:
        logger.info("[CONVERT-ONLY] Running in convert-only mode...")
        
        if not hasattr(pipeline, 'convert_existing_csvs_to_parquet'):
            logger.error("[CONVERT-ONLY] Pipeline does not support convert-only mode")
            return None
        
        try:
            # Convert existing CSV files to Parquet
            conversion_path = pipeline.convert_existing_csvs_to_parquet()
            if conversion_path:
                logger.info("[CONVERT-ONLY] Successfully converted existing CSV files to Parquet")
                logger.info(f"[CONVERT-ONLY] Parquet files saved to: {conversion_path}")
            else:
                logger.warning("[CONVERT-ONLY] No CSV files were converted")
            return conversion_path
        except (OSError, IOError, ValueError, RuntimeError) as e:
            logger.error(f"[CONVERT-ONLY] Conversion failed: {e}")
            raise


class FullETLStrategy:
    """Strategy for full ETL processing with database operations."""
    
    def get_name(self) -> str:
        return "Full ETL"
    
    def validate_parameters(self, **_kwargs) -> bool:
        # Validate that year and month are provided or can be defaulted
        return True
    
    def execute(self, pipeline: Pipeline, config_service: ConfigurationService, **kwargs) -> Optional[Any]:
        # Get parameters with defaults
        year = kwargs.get('year')
        month = kwargs.get('month')
        full_refresh = kwargs.get('full_refresh', False)
        clear_tables = kwargs.get('clear_tables', "")
        
        # Set defaults if not provided
        if year is None or month is None:
            today = datetime.today()
            year = year or today.year
            month = month or today.month
        
        month = int(month)
        year = int(year)
        
        # Database configuration
        main_db_config = config_service.pipeline.database
        user = main_db_config.user
        password = main_db_config.password
        host = main_db_config.host
        port = main_db_config.port
        maintenance_db = main_db_config.maintenance_db
        prod_db = main_db_config.database_name
        
        logger.info(f"[FULL-ETL] ETL start: {datetime.now():%Y-%m-%d %H:%M:%S}")
        logger.info(f"[FULL-ETL] Running ETL for year={year}, month={str(month).zfill(2)}")
        logger.info(f"[FULL-ETL] Target database: {prod_db}")
        
        # Ensure the production database exists
        create_database_if_not_exists(
            user, password, host, port, maintenance_db, prod_db
        )
        
        # Handle full refresh option
        if full_refresh:
            logger.info("[FULL-ETL] Full refresh requested - clearing tables before upsert...")
            if not self._clear_production_tables(config_service, clear_tables):
                logger.error("[FULL-ETL] Failed to clear tables. Aborting ETL process.")
                return None
        
        # Get current row counts for validation
        logger.info("[FULL-ETL] Getting current table row counts...")
        initial_row_counts = get_table_row_counts(user, password, host, port, prod_db)

        logger.info(f"[FULL-ETL] Current row counts: {initial_row_counts}")
        
        # Run the ETL job  
        try:
            logger.info("[FULL-ETL] Starting data processing and upsert to database...")
            
            # Step 1: Download and create audit metadata
            logger.info("[FULL-ETL] Step 1: Downloading and creating audit metadata...")
            audits = pipeline.retrieve_data()
            if not audits:
                logger.warning("[FULL-ETL] No data downloaded")
                return None
                
            # Create audit metadata
            from pathlib import Path
            download_path = str(config_service.pipeline.get_temporal_download_path(year, month))
            audit_metadata = pipeline.audit_service.create_audit_metadata(audits, download_path)
            
            # Step 2: Insert table audits BEFORE loading (critical for file manifest linking)
            logger.info("[FULL-ETL] Step 2: Inserting table audits before loading...")
            
            pipeline.audit_service.insert_audits(audit_metadata)
            logger.info("[FULL-ETL] Table audits inserted successfully - file manifests can now link properly")
        
            # Step 3: Convert to Parquet
            logger.info("[FULL-ETL] Step 3: Converting to Parquet...")
            pipeline.convert_to_parquet(audit_metadata)
            
            # Step 4: Load data (file manifests can now link to existing table audits)
            logger.info("[FULL-ETL] Step 4: Loading data to database...")
            pipeline.data_loader.load_data(audit_metadata)
            
            # Step 5: Cleanup downloaded files if configured
            if config_service.pipeline.delete_files:
                from ..utils.misc import remove_folder
                remove_folder(download_path)
                extract_path = str(config_service.pipeline.get_temporal_extraction_path(year, month))
                remove_folder(extract_path)
                logger.info("[FULL-ETL] Cleaned up temporary files")
            
            if audit_metadata:
                # Get final row counts for validation
                logger.info("[FULL-ETL] Getting final table row counts...")
                final_row_counts = get_table_row_counts(user, password, host, port, prod_db)
                logger.info(f"[FULL-ETL] Final row counts: {final_row_counts}")
                
                logger.info(f"[FULL-ETL] ETL job for {year}-{str(month).zfill(2)} completed successfully.")
                logger.info(f"[FULL-ETL] Data successfully upserted to production database '{prod_db}'.")
                
                # Log row count changes and update audit metadata
                self._update_audit_metadata(audit_metadata, initial_row_counts, final_row_counts)
                
                return audit_metadata
            else:
                logger.warning("[FULL-ETL] No data processed")
                return None
                
        except (OSError, IOError, ConnectionError, TimeoutError, ValueError, RuntimeError) as e:
            logger.error(f"[FULL-ETL] ETL job failed: {e}")
            logger.error("[FULL-ETL] Production database may be in inconsistent state.")
            raise

    def _clear_production_tables(self, config_service: ConfigurationService, table_names=None):
        """Clear (truncate) production tables before upsert."""
        main_db_config = config_service.pipeline.database
        prod_db = main_db_config.database_name
        
        logger.info(f"[CLEAR] Clearing tables in production database '{prod_db}'...")
        
        success = truncate_tables(
            main_db_config.user,
            main_db_config.password,
            main_db_config.host,
            main_db_config.port,
            prod_db,
            table_names,
        )
        
        if success:
            logger.info("[CLEAR] Tables cleared successfully.")
        else:
            logger.error("[CLEAR] Failed to clear tables.")
        
        return success
    
    def _update_audit_metadata(self, audit_metadata, initial_row_counts, final_row_counts):
        """Update audit metadata with row count changes."""
        for table_name in final_row_counts:
            initial_count = initial_row_counts.get(table_name, 0)
            final_count = final_row_counts[table_name]
            change = final_count - initial_count
            
            row_count_metadata = {
                "before": initial_count,
                "after": final_count,
                "diff": change,
            }
            
            # Find corresponding audit entry
            table_audit = [
                audit for audit in audit_metadata.audit_list
                if audit.entity_name == table_name
            ]
            
            if len(table_audit) == 1:
                table_audit[0].notes = {"row_count": row_count_metadata}
                logger.info(f"[METRICS] Table '{table_name}': {initial_count} -> {final_count} (diff: {change:+d})")
            else:
                logger.warning(f"[METRICS] Expected 1 audit entry for table '{table_name}', found {len(table_audit)}")


class LoadOnlyStrategy:
    """Strategy for loading existing Parquet/CSV files to database."""
    
    def get_name(self) -> str:
        return "Load Only"
    
    def validate_parameters(self, **kwargs) -> bool:
        return True  # No specific parameters required

    def execute(self, pipeline: Pipeline, _config_service: ConfigurationService, **kwargs) -> Optional[Any]:
        logger.info("[LOAD-ONLY] Running in load-only mode...")
        
        # For load-only, we need to create synthetic audit metadata from existing files
        if not hasattr(pipeline, 'create_audit_metadata_from_existing_csvs'):
            logger.error("[LOAD-ONLY] Pipeline does not support load-only mode")
            return None
        
        try:
            # Create metadata from existing files and load
            audit_metadata = pipeline.create_audit_metadata_from_existing_csvs()
            if not audit_metadata:
                logger.warning("[LOAD-ONLY] No existing files found to load")
                return None
            
            # Insert table audits into the database first
            if hasattr(pipeline, 'audit_service'):
                pipeline.audit_service.insert_audits(audit_metadata)
                logger.info("[LOAD-ONLY] Table audits inserted successfully")
            else:
                logger.warning("[LOAD-ONLY] Pipeline does not have audit service - table audits not created")
            
            # Use data loader to load into database
            if hasattr(pipeline, 'data_loader'):
                pipeline.data_loader.load_data(audit_metadata)
                logger.info("[LOAD-ONLY] Successfully loaded existing files to database")
                return audit_metadata
            else:
                logger.error("[LOAD-ONLY] Pipeline does not have data loader")
                return None
                
        except (OSError, IOError, ConnectionError, TimeoutError, ValueError, RuntimeError) as e:
            logger.error(f"[LOAD-ONLY] Load failed: {e}")
            raise


class ConvertAndLoadStrategy:
    """Strategy for converting existing CSV files and loading to database."""
    
    def get_name(self) -> str:
        return "Convert and Load"
    
    def validate_parameters(self, **kwargs) -> bool:
        return True  # No specific parameters required
    
    def execute(self, pipeline: Pipeline, _config_service: ConfigurationService, **kwargs) -> Optional[Any]:
        logger.info("[CONVERT-LOAD] Running in convert-and-load mode...")
        
        required_methods = ['create_audit_metadata_from_existing_csvs', 'convert_to_parquet', 'data_loader']
        for method in required_methods:
            if not hasattr(pipeline, method):
                logger.error(f"[CONVERT-LOAD] Pipeline missing required method: {method}")
                return None
        
        try:
            # Create metadata from existing CSV files
            audit_metadata = pipeline.create_audit_metadata_from_existing_csvs()
            if not audit_metadata:
                logger.warning("[CONVERT-LOAD] No existing CSV files found")
                return None
            
            # Convert to Parquet
            conversion_path = pipeline.convert_to_parquet(audit_metadata)
            logger.info(f"[CONVERT-LOAD] Files converted to Parquet: {conversion_path}")
            
            # Insert table audits into the database first
            if hasattr(pipeline, 'audit_service'):
                pipeline.audit_service.insert_audits(audit_metadata)
                logger.info("[CONVERT-LOAD] Table audits inserted successfully")
            else:
                logger.warning("[CONVERT-LOAD] Pipeline does not have audit service - table audits not created")
            
            # Load to database
            pipeline.data_loader.load_data(audit_metadata)
            logger.info("[CONVERT-LOAD] Successfully converted and loaded files to database")
            
            return audit_metadata
            
        except (OSError, IOError, ConnectionError, TimeoutError, ValueError, RuntimeError) as e:
            logger.error(f"[CONVERT-LOAD] Process failed: {e}")
            raise


class StrategyFactory:
    """Factory for creating orchestration strategies based on boolean flags."""
    
    @staticmethod
    def create_strategy(download: bool = False, convert: bool = False, load: bool = False) -> OrchestrationStrategy:
        """
        Create appropriate strategy based on boolean flags.
        
        Valid combinations:
        - (True, False, False) = 100 = Download Only
        - (True, True, False)  = 110 = Download and Convert
        - (False, True, False) = 010 = Convert Only
        - (False, False, True) = 001 = Load Only
        - (False, True, True)  = 011 = Convert and Load
        - (True, True, True)   = 111 = Full ETL
        
        Args:
            download: Download files from source
            convert: Convert files to Parquet
            load: Load files to database
        """
        # Create strategy key from boolean flags
        strategy_key = (download, convert, load)
        
        strategy_map = {
            (True, False, False): DownloadOnlyStrategy(),        # 100
            (True, True, False):  DownloadAndConvertStrategy(),  # 110
            (True, False, True):  DownloadAndLoadStrategy(),        # 101
            (False, True, False): ConvertOnlyStrategy(),         # 010
            (False, False, True): LoadOnlyStrategy(),            # 001
            (False, True, True):  ConvertAndLoadStrategy(),      # 011
            (True, True, True):   FullETLStrategy(),             # 111
        }
        
        if strategy_key not in strategy_map:
            valid_combinations = [
                "100 (download only)",
                "110 (download and convert)",
                "010 (convert only)",
                "001 (load only)",
                "011 (convert and load)",
                "111 (full ETL)"
            ]
            raise ValueError(
                f"Invalid strategy combination: download={download}, convert={convert}, load={load}\n"
                f"Valid combinations: {', '.join(valid_combinations)}"
            )
        
        return strategy_map[strategy_key]
    
    @staticmethod
    def get_valid_combinations():
        """Get list of valid strategy combinations."""
        return [
            {"download": True, "convert": False, "load": False, "name": "Download Only"},
            {"download": True, "convert": True, "load": False, "name": "Download and Convert"},
            {"download": False, "convert": True, "load": False, "name": "Convert Only"},
            {"download": False, "convert": False, "load": True, "name": "Load Only"},
            {"download": False, "convert": True, "load": True, "name": "Convert and Load"},
            {"download": True, "convert": True, "load": True, "name": "Full ETL"},
        ]