"""
Simplified data loading strategies using only the enhanced loader.
All complexity removed - now just a clean interface to EnhancedUnifiedLoader.

Batch Hierarchy:
1. Pipeline Batch: Top-level batch for entire ETL run (e.g., "CNPJ_Pipeline_2024-08_20250908_143022")
2. File Batch: One batch per file being processed (e.g., "File_Empresas0.zip_empresa_20250908_143025_(Pipeline_abc123)")
3. Subbatch: Parallel processing units within a file batch (e.g., "Process_empresas0.csv_part1")

This structure allows:
- Pipeline-level tracking of entire ETL runs
- File-level batching for large files that may be split
- Subbatch-level parallelization within file processing
- Proper audit trail with hierarchical relationships
"""
from abc import ABC, abstractmethod
from typing import List, Tuple, Optional
from pathlib import Path

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
        self._pipeline_batch_id = None  # Set by pipeline for proper hierarchy
        logger.info("Loading strategy initialized")
    
    def set_pipeline_batch_id(self, batch_id):
        """Set the pipeline-level batch ID for proper hierarchy."""
        self._pipeline_batch_id = batch_id
        logger.debug(f"Pipeline batch ID set: {batch_id}")

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
                dev_filter = DevelopmentFilter(self.config.etl)

                if not dev_filter.check_blob_size_limit(parquet_file):
                    return True, "Skipped large file in development mode", 0

                logger.info(f"[LoadingStrategy] Loading Parquet file: {parquet_file.name}")
                return self._create_manifest_and_load(
                    loader, table_info, parquet_file, table_name, 
                    batch_id=batch_id, subbatch_id=subbatch_id
                )

            elif table_files:
                # Centralized CSV filtering
                from ....core.utils.development_filter import DevelopmentFilter
                dev_filter = DevelopmentFilter(self.config.etl)

                # Apply table limit filtering first - keep as Path objects
                file_paths = [Path(f) for f in table_files]
                filtered_paths = dev_filter.filter_files_by_blob_limit(file_paths, table_name)

                if not filtered_paths:
                    logger.info(f"[LoadingStrategy] No files to load for table '{table_name}' after filtering")
                    return True, "No files to load after development filtering", 0

                logger.info(f"[LoadingStrategy] Loading {len(filtered_paths)} CSV files for table: {table_name}")
                return self._load_csv_files(
                    loader, table_info, path_config, filtered_paths, table_name,
                    batch_id=batch_id, subbatch_id=subbatch_id
                )
            
            else:
                return False, f"No files found for table {table_name}", 0
                
        except Exception as e:
            logger.error(f"[LoadingStrategy] Failed to load table '{table_name}': {e}")
            return False, str(e), 0
    
    def _create_manifest_and_load(self, loader, table_info, file_path, table_name, batch_id=None, subbatch_id=None):
        """
        Unified manifest creation and loading with consistent batch/subbatch hierarchy.
        Always creates proper 4-tier hierarchy regardless of file size.
        """
        try:
            # Get batch configuration from config
            batch_size = getattr(self.config.etl, 'row_batch_size', 10000)
            subbatch_size = getattr(self.config.etl, 'row_subbatch_size', 1000)
            
            # Check file size and row count
            import polars as pl
            from pathlib import Path
            
            file_path_obj = Path(file_path)
            total_rows = 0
            
            # Get actual row count for proper batching decisions
            if file_path_obj.suffix.lower() == '.parquet':
                try:
                    df_lazy = pl.scan_parquet(file_path)
                    total_rows = df_lazy.select(pl.count()).collect().item()
                except Exception as e:
                    logger.warning(f"Could not get row count from {file_path}: {e}")
                    total_rows = 1  # Default to small but ensure batch creation
            else:
                # For CSV, default to ensure batch creation
                total_rows = 1
            
<<<<<<< HEAD
            # Always use unified batching approach (no special case for small files)
            return self._load_with_batching(
                loader, table_info, file_path, table_name, 
                total_rows, batch_size, subbatch_size, batch_id, subbatch_id
            )
=======
            if total_rows > batch_size:
                # Row-driven subbatching: split file into multiple subbatches within existing batch
                return self._load_with_row_driven_batches(
                    loader, table_info, file_path, table_name, 
                    total_rows, batch_size, subbatch_size, batch_id, subbatch_id
                )
            else:
                # File is small enough, use single batch approach
                return self._load_single_batch(
                    loader, table_info, file_path, table_name, batch_id, subbatch_id
                )
>>>>>>> dc4d89e (refactor() fix MB variables and (sub)batch persistence)
            
        except Exception as e:
            logger.error(f"[LoadingStrategy] Failed to load {file_path}: {e}")
            # Create failed manifest entry
            self._create_manifest_entry(
                str(file_path), table_name, "FAILED", 0, str(e),
                batch_id=batch_id, subbatch_id=subbatch_id
            )
            raise

    def _load_with_batching(self, loader, table_info, file_path, table_name, 
                           total_rows: int, batch_size: int, subbatch_size: int,
                           parent_batch_id=None, parent_subbatch_id=None) -> Tuple[bool, Optional[str], int]:
        """
<<<<<<< HEAD
        Unified loading approach that always creates proper batch/subbatch hierarchy.
        Works consistently for files of any size - no special cases.
        
        Creates:
        - One batch per file (even if file has 1 row)  
        - One or more subbatches within each batch based on subbatch_size
        - Consistent 4-tier hierarchy: Table → File → Batch → Subbatch
=======
        Load file with row-driven subbatch creation within existing file batch.
        Creates subbatches instead of separate top-level batches.
        
        Example: 10,000 rows, batch_size=1,000 within existing file batch
        → 10 subbatches within the file batch
>>>>>>> dc4d89e (refactor() fix MB variables and (sub)batch persistence)
        """
        import math
        from datetime import datetime
        
        logger.info(f"[LoadingStrategy] Unified batching: {total_rows:,} rows")
        
        if not self.audit_service:
            # Fallback without batch tracking
            return self._load_entire_file(loader, table_info, file_path, table_name, None, None)
        
        # Calculate batch structure (always at least 1 batch)
        total_batches = max(1, math.ceil(total_rows / batch_size)) if total_rows > 0 else 1
        total_processed_rows = 0
        
<<<<<<< HEAD
=======
        logger.info(f"[LoadingStrategy] Row-driven subbatching: {total_rows:,} rows → {total_batches} subbatches within file batch")
        
        # Process file in row-driven subbatches within existing file batch
>>>>>>> dc4d89e (refactor() fix MB variables and (sub)batch persistence)
        for batch_num in range(total_batches):
            batch_start = batch_num * batch_size if total_rows > 0 else 0
            batch_end = min(batch_start + batch_size, total_rows) if total_rows > 0 else max(1, total_rows)
            actual_batch_rows = max(1, batch_end - batch_start) if total_rows > 0 else 1
            
<<<<<<< HEAD
            # Create row-driven batch name
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]
            batch_name = f"Batch_{table_name}_{batch_num+1}of{total_batches}_rows{batch_start}-{batch_end}_{timestamp}"
            
            # Create batch with proper context management
            with self.audit_service.batch_context(
                target_table=table_name, 
                batch_name=batch_name
            ) as row_batch_id:
                
                logger.info(f"[LoadingStrategy] Processing batch {batch_num+1}/{total_batches}: rows {batch_start}-{batch_end}")
                batch_processed_rows = 0
                
                # Calculate subbatches within this batch (always at least 1)
                subbatches_in_batch = max(1, math.ceil(actual_batch_rows / subbatch_size))
                
                for subbatch_num in range(subbatches_in_batch):
                    subbatch_start = batch_start + (subbatch_num * subbatch_size)
                    subbatch_end = min(subbatch_start + subbatch_size, batch_end)
                    
                    # Create subbatch name
                    subbatch_name = f"Subbatch_{subbatch_num+1}of{subbatches_in_batch}_rows{subbatch_start}-{subbatch_end}"
                    
                    with self.audit_service.subbatch_context(
                        batch_id=row_batch_id,
                        table_name=table_name,
                        description=subbatch_name
                    ) as subbatch_id:
                        
                        # For all files, load the entire file within the subbatch context
                        # The batch/subbatch structure provides consistent audit trail
                        success, error, rows = self._load_entire_file(
                            loader, table_info, file_path, table_name, row_batch_id, subbatch_id
                        )
                        
                        if not success:
                            return False, error, total_processed_rows
                        
                        batch_processed_rows += rows
                        
                        # For small files, break after first subbatch since file is fully processed
                        if total_rows <= subbatch_size:
                            break
                
                total_processed_rows += batch_processed_rows
                logger.info(f"[LoadingStrategy] Completed batch {batch_num+1}/{total_batches}: {batch_processed_rows:,} rows")
                
                # For small files, break after first batch since file is fully processed  
                if total_rows <= batch_size:
                    break
        
        return True, None, total_processed_rows

=======
            # Create subbatch name for this row chunk
            subbatch_name = f"RowChunk_{batch_num+1}of{total_batches}_rows{start_row}-{end_row}"
            
            if self.audit_service and parent_batch_id:
                # Use existing file batch, create subbatch for this row range
                with self.audit_service.subbatch_context(
                    batch_id=parent_batch_id,
                    table_name=table_name,
                    description=subbatch_name
                ) as chunk_subbatch_id:
                    # Load this specific row range
                    success, error, rows = self._load_row_range(
                        loader, table_info, file_path, table_name,
                        start_row, end_row, parent_batch_id, chunk_subbatch_id
                    )
                    
                    if not success:
                        return False, error, total_processed_rows
                    
                    total_processed_rows += rows
                    logger.info(f"[LoadingStrategy] Completed row chunk {batch_num+1}/{total_batches}: {rows:,} rows")
            else:
                # Fallback without audit tracking
                success, error, rows = self._load_row_range(
                    loader, table_info, file_path, table_name, start_row, end_row
                )
                if not success:
                    return False, error, total_processed_rows
                total_processed_rows += rows
        
        return True, None, total_processed_rows

    def _load_row_range(self, loader, table_info, file_path, table_name, 
                       start_row: int, end_row: int, batch_id=None, subbatch_id=None):
        """Load a specific row range from a file."""
        try:
            # Create manifest entry for this row range
            range_info = f"rows_{start_row}-{end_row}"
            manifest_id = self._create_manifest_entry(
                f"{file_path}#{range_info}", table_name, "PROCESSING", 
                batch_id=batch_id, subbatch_id=subbatch_id
            )
            
            # Load the specific row range with batch context
            # Note: This would need to be implemented in UnifiedLoader to support row ranges
            # For now, we'll load the entire file (this is a limitation)
            success, error, total_rows = loader.load_file(
                table_info, file_path,
                batch_id=batch_id, subbatch_id=subbatch_id
            )
            
            if success:
                # Calculate actual rows in this range
                actual_rows = min(end_row - start_row, total_rows - start_row) if total_rows > start_row else 0
                
                # Update manifest entry
                self._update_manifest_entry(manifest_id, "COMPLETED", actual_rows)
                return True, None, actual_rows
            else:
                self._update_manifest_entry(manifest_id, "FAILED", 0, error)
                return False, error, 0
                
        except Exception as e:
            logger.error(f"[LoadingStrategy] Failed to load row range {start_row}-{end_row}: {e}")
            if 'manifest_id' in locals():
                self._update_manifest_entry(manifest_id, "FAILED", 0, str(e))
            return False, str(e), 0
>>>>>>> dc4d89e (refactor() fix MB variables and (sub)batch persistence)



    def _load_entire_file(self, loader, table_info, file_path, table_name, batch_id=None, subbatch_id=None):
        """Load entire file in a single operation with manifest tracking."""
        # Create manifest entry
        manifest_id = self._create_manifest_entry(
            str(file_path), table_name, "PROCESSING", 
            batch_id=batch_id, subbatch_id=subbatch_id
        )
        
<<<<<<< HEAD
        # Load the entire file
        success, error, rows = loader.load_file(table_info, file_path)
=======
        # Load the file with batch context
        success, error, rows = loader.load_file(
            table_info, file_path, 
            batch_id=batch_id, subbatch_id=subbatch_id
        )
>>>>>>> dc4d89e (refactor() fix MB variables and (sub)batch persistence)
        
        # Update manifest entry
        status = "COMPLETED" if success else "FAILED" 
        error_msg = str(error) if error else None
        self._update_manifest_entry(manifest_id, status, rows, error_msg)
        
        return success, error, rows
    
    def _load_csv_files(self, loader, table_info, path_config, file_paths, table_name, batch_id=None, subbatch_id=None):
        """Load multiple CSV files with batch tracking."""
        total_rows = 0
        
        for file_path in file_paths:
            # file_path is already a Path object from the filtering
            if isinstance(file_path, str):
                csv_file = path_config.extract_path / file_path
            else:
                csv_file = file_path
            
            logger.info(f"[LoadingStrategy] Processing CSV file: {csv_file.name}")
            
            success, error, rows = self._create_manifest_and_load(
                loader, table_info, csv_file, table_name,
                batch_id=batch_id, subbatch_id=subbatch_id
            )
            
            if not success:
                logger.error(f"[LoadingStrategy] Failed to load {csv_file.name}: {error}")
                return success, error, total_rows
            
            total_rows += rows
            logger.info(f"[LoadingStrategy] Successfully loaded {rows:,} rows from {csv_file.name}")
        
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
        """
        Load multiple tables with proper hierarchical batch tracking:
        - One pipeline batch contains all table processing
        - Each file gets its own batch (can have multiple files per table)
        - Each file batch can have parallel subbatches
        """
        results = {}
        
        for table_name, zipfile_to_files in table_to_files.items():
            logger.info(f"[LoadingStrategy] Processing table '{table_name}'")
            
            # Process each file within the table as separate batches
            table_results = []
            table_total_rows = 0
            table_success = True
            table_errors = []
            
            for zip_filename, csv_files in zipfile_to_files.items():
                if not csv_files:
                    logger.warning(f"[LoadingStrategy] No CSV files in {zip_filename} for table '{table_name}'")
                    continue
                
                # Create file-level batch within the pipeline batch
                file_batch_result = self._process_file_batch(
                    database, table_name, zip_filename, csv_files, path_config
                )
                
                success, error, rows = file_batch_result
                table_results.append(file_batch_result)
                table_total_rows += rows
                
                if not success:
                    table_success = False
                    table_errors.append(f"{zip_filename}: {error}")
                    
            # Consolidate table results
            if table_success:
                results[table_name] = (True, None, table_total_rows)
                logger.info(f"[LoadingStrategy] Table '{table_name}' completed: {table_total_rows:,} rows")
            else:
                error_msg = "; ".join(table_errors)
                results[table_name] = (False, error_msg, table_total_rows)
                logger.error(f"[LoadingStrategy] Table '{table_name}' failed: {error_msg}")
                
        return results

    def _process_file_batch(self, database, table_name: str, zip_filename: str, 
                           csv_files: List[str], path_config: PathConfig) -> Tuple[bool, Optional[str], int]:
        """
        Process files with direct row-driven batching (no file batch wrapper).
        This creates proper 4-tier hierarchy: Table → File → Batch → Subbatch
        where batches are row-driven segments, not file-level wrappers.
        """
        logger.info(f"[LoadingStrategy] Processing {len(csv_files)} files for {table_name} (row-driven batching)")
        
        if not self.audit_service:
            # Fallback without batch tracking
            return self._load_files_without_batch_tracking(
                database, table_name, csv_files, path_config
            )
        
        total_success = True
        total_processed_rows = 0
        total_errors = []
        
        # Process each CSV file with direct row-driven batching
        for csv_file in csv_files:
            try:
                logger.info(f"[LoadingStrategy] Processing file: {csv_file}")
                
                # Direct file processing - let load_table create appropriate row-driven batches
                # No file batch wrapper - this creates proper hierarchy per user specification
                success, error, rows = self.load_table(
                    database, table_name, path_config, [csv_file],
                    batch_id=None,  # No parent batch - creates proper row-driven batches
                    subbatch_id=None
                )
                
                if success:
                    total_processed_rows += rows
                    logger.info(f"[LoadingStrategy] Successfully processed {csv_file}: {rows} rows")
                else:
                    total_success = False
                    error_msg = f"Failed to process {csv_file}: {error}"
                    total_errors.append(error_msg)
                    logger.error(f"[LoadingStrategy] {error_msg}")
                    
            except Exception as e:
                total_success = False
                error_msg = f"Exception processing {csv_file}: {e}"
                total_errors.append(error_msg)
                logger.error(f"[LoadingStrategy] {error_msg}")
        
        # Return results
        if total_success:
            logger.info(f"[LoadingStrategy] All files processed successfully. Total rows: {total_processed_rows}")
            return True, None, total_processed_rows
        else:
            error_summary = f"Errors in {len(total_errors)} files: {'; '.join(total_errors[:3])}"
            if len(total_errors) > 3:
                error_summary += f" (and {len(total_errors) - 3} more)"
            logger.error(f"[LoadingStrategy] Processing completed with errors: {error_summary}")
            return False, error_summary, total_processed_rows

    def _load_files_without_batch_tracking(self, database, table_name: str, 
                                          csv_files: List[str], path_config: PathConfig) -> Tuple[bool, Optional[str], int]:
        """Fallback file loading without batch tracking."""
        return self.load_table(database, table_name, path_config, csv_files)

    def _find_audit_id_for_file(self, table_name: str, filename: str) -> Optional[str]:
        """Find existing audit entry ID for a given table and filename."""
        try:
            from sqlalchemy import text
            
            # Query audit table to find matching entry
            query = '''
            SELECT audi_id FROM table_ingestion_manifest 
            WHERE audi_table_name = :table_name 
            AND audi_filenames::jsonb ? :filename
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