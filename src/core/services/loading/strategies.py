"""
Simplified data loading strategies using only the enhanced loader.
All complexity removed - now just a clean interface to EnhancedUnifiedLoader.
"""
from abc import ABC, abstractmethod
from typing import List, Tuple, Optional

from ....database.engine import Database
from ....setup.logging import logger
from ....setup.config import PathConfig
from ....database.dml import table_name_to_table_info

class BaseDataLoadingStrategy(ABC):
    """Abstract base class for data loading strategies."""
    
    @abstractmethod
    def load_table(self, database: Database, table_name: str, path_config: PathConfig, 
                   table_files: Optional[List[str]] = None, 
                   batch_id=None, subbatch_id=None) -> Tuple[bool, Optional[str], int]:
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
        logger.info("Loading strategy initialized")

    def load_table(self, database: Database, table_name: str, path_config: PathConfig, 
                   table_files: Optional[List[str]] = None, 
                   batch_id=None, subbatch_id=None) -> Tuple[bool, Optional[str], int]:
        """Load table using EnhancedUnifiedLoader with batch tracking."""
        
        logger.info(f"[LoadingStrategy] Loading table '{table_name}' with UnifiedLoader")
        
        try:
            table_info = table_name_to_table_info(table_name)
            from ....database.dml import UnifiedLoader
            loader = UnifiedLoader(database, self.config)
            
            # Check Parquet first (maintain existing priority)
            parquet_file = path_config.conversion_path / f"{table_name}.parquet"
            if parquet_file.exists():
                # Centralized Parquet filtering
                from ....core.utils.development_filter import DevelopmentFilter
                dev_filter = DevelopmentFilter(self.config)

                if not dev_filter.filter_parquet_file_by_size(parquet_file):
                    return True, "Skipped large file in development mode", 0

                logger.info(f"[LoadingStrategy] Loading Parquet file: {parquet_file.name}")
                return self._create_manifest_and_load(
                    loader, table_info, parquet_file, table_name, 
                    batch_id=batch_id, subbatch_id=subbatch_id
                )

            elif table_files:
                # Centralized CSV filtering
                from ....core.utils.development_filter import DevelopmentFilter
                dev_filter = DevelopmentFilter(self.config)

                # Apply table limit filtering first
                table_files = dev_filter.filter_csv_files_by_table_limit(table_files, table_name)

                if not table_files:
                    logger.info(f"[LoadingStrategy] No files to load for table '{table_name}' after filtering")
                    return True, "No files to load after development filtering", 0

                logger.info(f"[LoadingStrategy] Loading {len(table_files)} CSV files for table: {table_name}")
                return self._load_csv_files(
                    loader, table_info, path_config, table_files, table_name,
                    batch_id=batch_id, subbatch_id=subbatch_id
                )
            
            else:
                return False, f"No files found for table {table_name}", 0
                
        except Exception as e:
            logger.error(f"[LoadingStrategy] Failed to load table '{table_name}': {e}")
            return False, str(e), 0
    
    def _create_manifest_and_load(self, loader, table_info, file_path, table_name, batch_id=None, subbatch_id=None):
        """Create manifest entry and load file with batch tracking."""
        try:
            # Create manifest entry
            manifest_id = self._create_manifest_entry(
                str(file_path), table_name, "PROCESSING", 
                batch_id=batch_id, subbatch_id=subbatch_id
            )
            
            # Load the file - removed incorrect table_name parameter
            success, error, rows = loader.load_file(table_info, file_path)
            
            # Update manifest entry (completed)
            status = "COMPLETED" if success else "FAILED" 
            error_msg = str(error) if error else None
            self._update_manifest_entry(manifest_id, status, rows, error_msg)
            
            return success, error, rows
            
        except Exception as e:
            # Create failed manifest entry if we didn't create one yet
            if 'manifest_id' not in locals():
                self._create_manifest_entry(
                    str(file_path), table_name, "FAILED", 0, str(e),
                    batch_id=batch_id, subbatch_id=subbatch_id
                )
            else:
                self._update_manifest_entry(manifest_id, "FAILED", 0, str(e))
            raise
    
    def _load_csv_files(self, loader, table_info, path_config, table_files, table_name, batch_id=None, subbatch_id=None):
        """Load multiple CSV files with batch tracking."""
        total_rows = 0
        
        for filename in table_files:
            csv_file = path_config.extract_path / filename
            logger.info(f"[LoadingStrategy] Processing CSV file: {filename}")
            
            success, error, rows = self._create_manifest_and_load(
                loader, table_info, csv_file, table_name,
                batch_id=batch_id, subbatch_id=subbatch_id
            )
            
            if not success:
                logger.error(f"[LoadingStrategy] Failed to load {filename}: {error}")
                return success, error, total_rows
            
            total_rows += rows
            logger.info(f"[LoadingStrategy] Successfully loaded {rows:,} rows from {filename}")
        
        return True, None, total_rows
    
    def _create_manifest_entry(self, file_path: str, table_name: str, status: str,
                              rows_processed: Optional[int] = None, error_msg: Optional[str] = None,
                              batch_id=None, subbatch_id=None, audit_id: Optional[str] = None) -> Optional[str]:
        """Create manifest entry for loaded file with batch tracking."""
        # Log error messages if provided
        if error_msg and status in ["FAILED", "ERROR"]:
            logger.error(f"[LoadingStrategy] Error processing {file_path}: {error_msg}")
        
        if self.audit_service and hasattr(self.audit_service, 'create_file_manifest'):
            try:
                from pathlib import Path
                import json
                file_path_obj = Path(file_path)
                
                # Find audit_id if not provided
                if not audit_id:
                    audit_id = self._find_audit_id_for_file(table_name, file_path_obj.name)
                    if not audit_id:
                        logger.warning(f"No audit entry found for table {table_name}, file {file_path_obj.name}")
                        # Create a placeholder audit entry (this is a fallback)
                        audit_id = self._create_placeholder_audit_entry(table_name, file_path_obj.name)
                
                # Calculate file info if file exists
                checksum = None
                filesize = None
                if file_path_obj.exists():
                    filesize = file_path_obj.stat().st_size
                
                # Create structured JSON notes
                notes_data = {
                    "file_info": {
                        "size_bytes": filesize,
                        "format": file_path_obj.suffix.lstrip('.') if file_path_obj.suffix else "unknown"
                    },
                    "processing": {
                        "status": status,
                        "table_name": table_name
                    }
                }
                
                if rows_processed is not None:
                    notes_data["processing"]["rows_processed"] = rows_processed
                    
                if error_msg:
                    notes_data["processing"]["error_message"] = error_msg
                    
                if batch_id:
                    notes_data["tracking"] = {
                        "batch_id": str(batch_id),
                        "subbatch_id": str(subbatch_id) if subbatch_id else None
                    }
                
                # Create manifest entry with batch tracking and audit reference
                manifest_id = self.audit_service.create_file_manifest(
                    str(file_path_obj),
                    status=status,
                    audit_id=audit_id,  # FIXED: Required audit_id reference
                    checksum=checksum,
                    filesize=filesize,
                    rows=rows_processed,
                    table_name=table_name,
                    notes=json.dumps(notes_data),
                    batch_id=batch_id,
                    subbatch_id=subbatch_id
                )
                return manifest_id
                        
            except Exception as e:
                logger.warning(f"Failed to create manifest entry for {file_path}: {e}")
        else:
            logger.debug("No audit service available for manifest tracking")
        
        return None

    def _update_manifest_entry(self, manifest_id: str, status: str, 
                              rows_processed: Optional[int] = None, error_msg: Optional[str] = None) -> None:
        """Update existing manifest entry with processing results."""
        if self.audit_service and manifest_id and hasattr(self.audit_service, 'update_file_manifest'):
            try:
                import json
                from datetime import datetime
                
                # Create updated notes
                notes_data = {
                    "processing_update": {
                        "final_status": status,
                        "completion_timestamp": str(datetime.now())
                    }
                }
                
                if rows_processed is not None:
                    notes_data["processing_update"]["rows_processed"] = rows_processed
                    
                if error_msg:
                    notes_data["processing_update"]["error_message"] = error_msg
                
                # Update the manifest entry
                self.audit_service.update_file_manifest(
                    manifest_id=manifest_id,
                    status=status,
                    rows_processed=rows_processed,
                    error_msg=error_msg,
                    notes=json.dumps(notes_data)
                )
                logger.debug(f"Updated manifest entry {manifest_id} with status {status}")
                
            except Exception as e:
                logger.warning(f"Failed to update manifest entry {manifest_id}: {e}")
        else:
            logger.debug("No audit service available for manifest update")

    def load_multiple_tables(self, database, table_to_files, path_config: PathConfig):
        """Load multiple tables with per-table batch tracking instead of umbrella batch."""
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
                
            # Create individual batch for each table instead of using umbrella batch
            if self.audit_service:
                # Generate table-specific batch name
                from datetime import datetime
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                table_batch_name = f"Table_Load_{table_name}_{timestamp}"
                
                # Start table-specific batch
                table_batch_id = self.audit_service._start_batch(
                    target_table=table_name,
                    batch_name=table_batch_name
                )
                
                try:
                    # Create subbatch within the table-specific batch
                    with self.audit_service.subbatch_context(
                        batch_id=table_batch_id,
                        table_name=table_name
                    ) as subbatch_id:
                        results[table_name] = self.load_table(
                            database, table_name, path_config, all_files, 
                            batch_id=table_batch_id, subbatch_id=subbatch_id
                        )
                    
                    # Complete the table batch
                    from ....database.models import BatchStatus
                    success = results[table_name][0] if results[table_name] else False
                    status = BatchStatus.COMPLETED if success else BatchStatus.FAILED
                    self.audit_service._complete_batch_with_accumulated_metrics(table_batch_id, status)
                    
                except Exception as e:
                    # Complete the table batch with failure status
                    from ....database.models import BatchStatus
                    self.audit_service._complete_batch_with_accumulated_metrics(table_batch_id, BatchStatus.FAILED)
                    logger.error(f"[LoadingStrategy] Failed to load table {table_name}: {e}")
                    results[table_name] = (False, str(e), 0)
            else:
                # Fallback without batch tracking
                results[table_name] = self.load_table(database, table_name, path_config, all_files)
                
        return results

    def _find_audit_id_for_file(self, table_name: str, filename: str) -> Optional[str]:
        """Find existing audit entry ID for a given table and filename."""
        try:
            from sqlalchemy import text
            
            # Query audit table to find matching entry
            query = '''
            SELECT audi_id FROM table_ingestion_manifest 
            WHERE audi_table_name = :table_name 
            AND :filename = ANY(audi_filenames)
            ORDER BY audi_created_at DESC 
            LIMIT 1
            '''
            
            with self.audit_service.database.engine.connect() as conn:
                result = conn.execute(text(query), {
                    'table_name': table_name,
                    'filename': filename
                })
                row = result.fetchone()
                return str(row[0]) if row else None
                
        except Exception as e:
            logger.error(f"Failed to find audit ID for {table_name}/{filename}: {e}")
            return None

    def _create_placeholder_audit_entry(self, table_name: str, filename: str) -> str:
        """Create a placeholder audit entry for files without existing audit records."""
        try:
            from ....database.models import AuditDB
            from datetime import datetime
            import uuid
            
            # Create minimal audit entry
            audit_id = str(uuid.uuid4())
            placeholder_audit = AuditDB(
                audi_id=audit_id,
                audi_table_name=table_name,
                audi_filenames=[filename],
                audi_file_size_bytes=0,
                audi_source_updated_at=datetime.now(),
                audi_created_at=datetime.now(),
                audi_downloaded_at=None,
                audi_processed_at=None,
                audi_inserted_at=datetime.now(),
                audi_ingestion_year=datetime.now().year,
                audi_ingestion_month=datetime.now().month,
                audi_metadata={"placeholder": True, "created_for": "file_manifest_requirement"}
            )
            
            # Insert placeholder audit entry
            with self.audit_service.database.session_maker() as session:
                session.add(placeholder_audit)
                session.commit()
                
            logger.info(f"Created placeholder audit entry {audit_id} for {table_name}/{filename}")
            return audit_id
            
        except Exception as e:
            logger.error(f"Failed to create placeholder audit entry for {table_name}/{filename}: {e}")
            raise RuntimeError(f"Cannot create file manifest without audit_id: {e}")