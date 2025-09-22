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
from ....database.dml import table_name_to_table_info
from ....database.models.audit import AuditStatus

class BaseDataLoadingStrategy(ABC):
    """Abstract base class for data loading strategies."""
    
    @abstractmethod
    def load_table(self, database: Database, table_name: str, pipeline_config,
                   table_files: Optional[List[str]] = None) -> Tuple[bool, Optional[str], int]:
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

    def load_table(self, database: Database, table_name: str, pipeline_config,
                   table_files: Optional[List[str]] = None, 
                   batch_id=None, subbatch_id=None, force_csv: bool = False) -> Tuple[bool, Optional[str], int]:
        """
        Load table using EnhancedUnifiedLoader with batch tracking.
        
        Args:
            force_csv: If True, skip Parquet file detection and force CSV loading
        """
        
        logger.info(f"[LoadingStrategy] Loading table '{table_name}' with UnifiedLoader")
        
        try:
            table_info = table_name_to_table_info(table_name)
            from ....database.dml import UnifiedLoader
            loader = UnifiedLoader(database, self.config)
            
            # Check Parquet first (maintain existing priority) - UNLESS force_csv=True
            if not force_csv:
                parquet_file = pipeline_config.get_temporal_conversion_path(self.config.year, self.config.month) / f"{table_name}.parquet"
                if parquet_file.exists():
                    # Centralized Parquet filtering
                    from ....core.utils.development_filter import DevelopmentFilter
                    dev_filter = DevelopmentFilter(self.config.pipeline.development)

                    if not dev_filter.check_blob_size_limit(parquet_file):
                        return True, "Skipped large file in development mode", 0

                    logger.info(f"[LoadingStrategy] Loading Parquet file: {parquet_file.name}")
                    return self._create_manifest_and_load(
                        loader, table_info, parquet_file, table_name, 
                        batch_id=batch_id, subbatch_id=subbatch_id
                    )
            else:
                logger.info(f"[LoadingStrategy] Forcing CSV loading for table '{table_name}' (skipping Parquet)")

            if table_files:
                # Centralized CSV filtering
                from ....core.utils.development_filter import DevelopmentFilter
                dev_filter = DevelopmentFilter(self.config.pipeline.development)

                # Apply table limit filtering first - keep as Path objects
                file_paths = [Path(f) for f in table_files]
                filtered_paths = dev_filter.filter_files_by_blob_limit(file_paths, table_name)

                if not filtered_paths:
                    logger.info(f"[LoadingStrategy] No files to load for table '{table_name}' after filtering")
                    return True, "No files to load after development filtering", 0

                logger.info(f"[LoadingStrategy] Loading {len(filtered_paths)} CSV files for table: {table_name}")
                return self._load_csv_files(
                    loader, table_info, pipeline_config, filtered_paths, table_name,
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
            if hasattr(self.config, 'pipeline'):
                batch_size = getattr(self.config.pipeline.loading, 'batch_size', 1000)
                subbatch_size = getattr(self.config.pipeline.loading, 'sub_batch_size', 500)
            
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
            
            # Use FileLoader for proper batch processing with hierarchy context management
            return self._load_with_file_loader_batching(
                loader, table_info, file_path, table_name, 
                batch_size, subbatch_size, batch_id, subbatch_id
            )
            
        except Exception as e:
            logger.error(f"[LoadingStrategy] Failed to load {file_path}: {e}")
            
            # Create failed manifest entry (use AuditStatus enum)
            self._create_manifest_entry(
                str(file_path), table_name, AuditStatus.FAILED, 0, str(e),
                batch_id=batch_id, subbatch_id=subbatch_id
            )
            raise

    def _load_with_file_loader_batching(self, loader, table_info, file_path, table_name,
                                      batch_size: int, subbatch_size: int, 
                                      parent_batch_id=None, parent_subbatch_id=None) -> Tuple[bool, Optional[str], int]:
        """
        Use FileLoader for proper batch processing with hierarchy context management.
        
        This method leverages the existing FileLoader service to:
        1. Auto-detect file format (CSV/Parquet) 
        2. Process files in actual row chunks (not full file loads)
        3. Maintain proper hierarchy: Table → File → Batch → Subbatch
        4. Support encoding detection for CNPJ files
        """
        from datetime import datetime
        from pathlib import Path
        
        try:
            # Initialize variables early to avoid scope issues
            file_manifest_id = None
            total_processed_rows = 0
            batch_num = 0
            
            # Initialize FileLoader with encoding detection for CNPJ files
            from .file_loader import FileLoader
            
            # Use UTF-8 for most CNPJ files, but FileLoader will auto-detect issues
            encoding = getattr(self.config.pipeline.data_source, 'encoding', 'utf-8')
            file_loader = FileLoader(str(file_path), encoding=encoding)
            
            logger.info(f"[LoadingStrategy] FileLoader detected format: {file_loader.get_format()}")
            
            # Get table headers from table_info (columns is already a list of strings)
            headers = table_info.columns
            
            # Variables already initialized above
            
            if not self.audit_service:
                # Fallback: load via FileLoader without context management
                return self._load_file_without_batching(file_loader, loader, table_info, headers, batch_size)
            
            # Find the table manifest ID for proper audit linking
            logger.debug(f"FileLoader path: Looking up table audit for {table_name}")
            table_manifest_id = self._find_table_audit_by_table_name(table_name)
            logger.info(f"FileLoader table audit lookup result for {table_name}: {table_manifest_id}")
            if not table_manifest_id:
                logger.warning(f"No table audit found for {table_name}, proceeding without audit linking")
            
            # Create a file manifest entry for this processing session
            file_manifest_id = None
            if table_manifest_id:
                # Create initial manifest entry with PROCESSING status (use AuditStatus enum)
                file_manifest_id = self._create_manifest_entry(
                    str(file_path), table_name, AuditStatus.RUNNING, 
                    table_manifest_id=table_manifest_id
                )
                logger.debug(f"[LoadingStrategy] Created file manifest {file_manifest_id} with PROCESSING status")
            
            # Use FileLoader's batch_generator for actual row processing
            for batch_chunk in file_loader.batch_generator(headers, chunk_size=batch_size):
                batch_num += 1
                
                # Create batch context for this chunk
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]
                batch_name = f"FileLoader_Batch_{table_name}_chunk{batch_num}_{len(batch_chunk)}rows_{timestamp}"
                
                with self.audit_service.batch_context(
                    target_table=table_name, 
                    batch_name=batch_name,
                    file_manifest_id=file_manifest_id,
                    table_manifest_id=table_manifest_id
                ) as batch_id:
                    
                    logger.info(f"[LoadingStrategy] Processing FileLoader batch {batch_num}: {len(batch_chunk)} rows")
                    
                    # Process subbatches within this batch chunk
                    subbatch_chunks = self._split_batch_into_subbatches(batch_chunk, subbatch_size)
                    
                    for subbatch_num, subbatch_chunk in enumerate(subbatch_chunks):
                        subbatch_name = f"Subbatch_{subbatch_num+1}of{len(subbatch_chunks)}_rows{len(subbatch_chunk)}"
                        
                        with self.audit_service.subbatch_context(
                            batch_id=batch_id,
                            table_name=table_name,
                            description=subbatch_name
                        ) as subbatch_id:
                            
                            # Load this specific subbatch chunk
                            success, error, rows = self._load_batch_chunk(
                                loader, table_info, subbatch_chunk, table_name, batch_id, subbatch_id
                            )
                            
                            # Collect file processing event for subbatch metrics
                            if self.audit_service and subbatch_id:
                                status_enum = AuditStatus.COMPLETED if success else AuditStatus.FAILED
                                self.audit_service.collect_file_processing_event(
                                    subbatch_id=subbatch_id,
                                    status=status_enum,
                                    rows=rows or 0,
                                    bytes_=0  # We don't have byte count for in-memory chunks
                                )
                            
                            if not success:
                                logger.error(f"[LoadingStrategy] Subbatch failed: {error}")
                                
                                # Update file manifest to FAILED status
                                if file_manifest_id and self.audit_service:
                                    self._update_manifest_entry(
                                        manifest_id=file_manifest_id,
                                        status=AuditStatus.FAILED, 
                                        rows_processed=total_processed_rows,
                                        error_msg=error
                                    )
                                    logger.debug(f"[LoadingStrategy] Updated file manifest {file_manifest_id} to FAILED")
                                
                                return False, error, total_processed_rows
                                
                            total_processed_rows += rows
                            logger.debug(f"[LoadingStrategy] Subbatch completed: {rows} rows")
            
            logger.info(f"[LoadingStrategy] FileLoader processing complete: {total_processed_rows} total rows")
            
            # Update file manifest to COMPLETED status with final metrics
            if file_manifest_id and self.audit_service:
                logger.info(f"[LoadingStrategy] Updating file manifest {file_manifest_id} to COMPLETED with {total_processed_rows} rows")
                self._update_manifest_entry(
                    manifest_id=file_manifest_id,
                    status=AuditStatus.COMPLETED,
                    rows_processed=total_processed_rows,
                    error_msg=None
                )
                logger.debug(f"[LoadingStrategy] Updated file manifest {file_manifest_id} to COMPLETED")
            else:
                logger.warning(f"[LoadingStrategy] Cannot update file manifest: file_manifest_id={file_manifest_id}, audit_service={bool(self.audit_service)}")
                
            return True, None, total_processed_rows
            
        except Exception as e:
            logger.error(f"[LoadingStrategy] FileLoader batching failed for {file_path}: {e}")
            
            # Update file manifest to FAILED status on exception (only if it was created)
            if file_manifest_id and self.audit_service:
                self._update_manifest_entry(
                    manifest_id=file_manifest_id,
                    status=AuditStatus.FAILED,
                    rows_processed=total_processed_rows,
                    error_msg=str(e)
                )
                logger.debug(f"[LoadingStrategy] Updated file manifest {file_manifest_id} to FAILED due to exception")
                
            return False, str(e), 0

    def _split_batch_into_subbatches(self, batch_chunk, subbatch_size):
        """Split a batch chunk into smaller subbatch chunks."""
        subbatch_chunks = []
        for i in range(0, len(batch_chunk), subbatch_size):
            subbatch_chunk = batch_chunk[i:i + subbatch_size]
            subbatch_chunks.append(subbatch_chunk)
        return subbatch_chunks

    def _load_batch_chunk(
        self, 
        loader, table_info, batch_chunk, table_name, 
        batch_id, subbatch_id
    ):
        """
        Load a specific batch chunk (list of tuples) using existing loader infrastructure.
        """
        try:
            # Chunks should NOT create file manifest entries - they are tracked in batch_audit/subbatch_audit
            chunk_description = f"chunk_{len(batch_chunk)}rows"
            manifest_id = None  # No file manifest for chunks
            
            # Load batch chunk using existing UnifiedLoader
            # Note: This requires UnifiedLoader to support batch data loading
            if hasattr(loader, 'load_batch_data'):
                success, error, rows = loader.load_batch_data(
                    table_info, batch_chunk, batch_id=batch_id, subbatch_id=subbatch_id
                )
            else:
                # Fallback: create temporary data and use existing load_file method
                success, error, rows = self._load_chunk_via_temporary_method(
                    loader, table_info, batch_chunk, batch_id, subbatch_id
                )
            
            # Update manifest only if one was created (not for chunks)
            if manifest_id:
                from ....database.models.audit import AuditStatus
                status = AuditStatus.COMPLETED if success else AuditStatus.FAILED
                error_msg = str(error) if error else None
                self._update_manifest_entry(manifest_id, status, rows, error_msg)
            
            return success, error, rows
            
        except Exception as e:
            logger.error(f"[LoadingStrategy] Failed to load batch chunk: {e}")
            return False, str(e), 0

    def _load_chunk_via_temporary_method(self, loader, table_info, batch_chunk, batch_id, subbatch_id):
        """
        Load chunk data by creating a temporary in-memory file and using UnifiedLoader's load_file method.
        This bridges the gap between chunk data and file-based loading.
        """
        try:
            import tempfile
            import pandas as pd
            from pathlib import Path
            
            # Convert chunk data to DataFrame for consistent handling
            if not isinstance(batch_chunk, pd.DataFrame):
                # Assume batch_chunk is a list of rows or dict-like objects
                df = pd.DataFrame(batch_chunk)
            else:
                df = batch_chunk
            
            if df.empty:
                logger.warning("[LoadingStrategy] Empty batch chunk provided")
                return True, None, 0
            
            # Create temporary Parquet file (more efficient than CSV for memory chunks)
            with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as temp_file:
                temp_path = Path(temp_file.name)
                
                # Write chunk to temporary Parquet file
                df.to_parquet(temp_path, index=False)
                logger.debug(f"[LoadingStrategy] Created temporary file {temp_path} with {len(df)} rows")
                
                # Use UnifiedLoader to load the temporary file
                success, error, rows = loader.load_file(
                    table_info=table_info,
                    file_path=temp_path,
                    batch_id=batch_id,
                    subbatch_id=subbatch_id
                )
                
                # Clean up temporary file
                temp_path.unlink(missing_ok=True)
                logger.debug(f"[LoadingStrategy] Cleaned up temporary file {temp_path}")
                
                return success, error, rows
                
        except Exception as e:
            logger.error(f"[LoadingStrategy] Failed to load chunk via temporary method: {e}")
            return False, str(e), 0

    def _load_file_without_batching(self, file_loader, loader, table_info, headers, chunk_size):
        """Fallback for loading entire file when no audit service available."""
        total_rows = 0
        
        for batch_chunk in file_loader.batch_generator(headers, chunk_size=chunk_size):
            # Load without context management
            success, error, rows = self._load_chunk_via_temporary_method(
                loader, table_info, batch_chunk, None, None
            )
            if not success:
                return False, error, total_rows
            total_rows += rows
            
        return True, None, total_rows

    def _load_with_batching(self, loader, table_info, file_path, table_name, 
                           total_rows: int, batch_size: int, subbatch_size: int,
                           parent_batch_id=None, parent_subbatch_id=None) -> Tuple[bool, Optional[str], int]:
        """
        Unified loading approach that always creates proper batch/subbatch hierarchy.
        Works consistently for files of any size - no special cases.
        
        Creates:
        - One batch per file (even if file has 1 row)  
        - One or more subbatches within each batch based on subbatch_size
        - Consistent 4-tier hierarchy: Table → File → Batch → Subbatch
        """
        import math
        from datetime import datetime
        
        logger.info(f"[LoadingStrategy] Unified batching: {total_rows:,} rows")
        
        # Debug audit service state
        logger.debug(f"Audit service available: {self.audit_service is not None}")
        if self.audit_service:
            logger.debug(f"Audit service database available: {self.audit_service.database is not None}")
        
        # Find the table manifest ID for proper audit linking
        table_manifest_id = self._find_table_audit_by_table_name(table_name)
        logger.info(f"Table audit lookup result for {table_name}: {table_manifest_id}")
        if not table_manifest_id:
            logger.warning(f"No table audit found for {table_name}, proceeding without audit linking")
        
        # Create a file manifest entry for this processing session
        file_manifest_id = None
        if table_manifest_id:
            file_manifest_id = self._create_manifest_entry(
                str(file_path), table_name, AuditStatus.RUNNING, 
                table_manifest_id=table_manifest_id
            )
        
        if not self.audit_service:
            # Fallback without batch tracking
            return self._load_entire_file(loader, table_info, file_path, table_name, None, None)
        
        # Calculate batch structure (always at least 1 batch)
        total_batches = max(1, math.ceil(total_rows / batch_size)) if total_rows > 0 else 1
        total_processed_rows = 0
        
        for batch_num in range(total_batches):
            batch_start = batch_num * batch_size if total_rows > 0 else 0
            batch_end = min(batch_start + batch_size, total_rows) if total_rows > 0 else max(1, total_rows)
            actual_batch_rows = max(1, batch_end - batch_start) if total_rows > 0 else 1
            
            # Create row-driven batch name
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]
            batch_name = f"Batch_{table_name}_{batch_num+1}of{total_batches}_rows{batch_start}-{batch_end}_{timestamp}"
            
            # Create batch with proper context management
            with self.audit_service.batch_context(
                target_table=table_name, 
                batch_name=batch_name,
                file_manifest_id=file_manifest_id,
                table_manifest_id=table_manifest_id
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




    def _load_entire_file(self, loader, table_info, file_path, table_name, batch_id=None, subbatch_id=None):
        """Load entire file in a single operation with manifest tracking."""
        # Create manifest entry
        manifest_id = self._create_manifest_entry(
            str(file_path), table_name, AuditStatus.RUNNING, 
            batch_id=batch_id, subbatch_id=subbatch_id
        )
        
        # Load the entire file
        success, error, rows = loader.load_file(table_info, file_path)
        
        # Update manifest entry
        from ....database.models.audit import AuditStatus
        status = AuditStatus.COMPLETED if success else AuditStatus.FAILED
        error_msg = str(error) if error else None
        self._update_manifest_entry(manifest_id, status, rows, error_msg)
        
        return success, error, rows
    
    def _load_csv_files(self, loader, table_info, path_config, file_paths, table_name, batch_id=None, subbatch_id=None):
        """Load multiple CSV files with batch tracking."""
        total_rows = 0
        
        for file_path in file_paths:
            # file_path is already a Path object from the filtering
            if isinstance(file_path, str):
                csv_file = path_config.get_temporal_extraction_path(self.config.year, self.config.month) / file_path
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
                              table_manifest_id: Optional[str] = None, batch_id: Optional[str] = None, 
                              subbatch_id: Optional[str] = None) -> Optional[str]:
        """Create manifest entry for loaded file."""
        # Early return for chunks - they should NOT create file manifest entries
        # Chunks are tracked in batch_audit and subbatch_audit only
        if "chunk_" in file_path or file_path.startswith("chunk"):
            logger.debug(f"Skipping file manifest creation for chunk: {file_path}")
            return None
        
        # Log error messages if provided
        if error_msg and status in [AuditStatus.FAILED, AuditStatus.ERROR]:
            logger.error(f"[LoadingStrategy] Error processing {file_path}: {error_msg}")
        
        if self.audit_service and hasattr(self.audit_service, 'create_file_manifest'):
            try:
                from pathlib import Path
                import json
                file_path_obj = Path(file_path)
                
                # Find table_manifest_id if not provided
                if not table_manifest_id:
                    table_manifest_id = self._find_table_audit_for_file(table_name, file_path_obj.name)
                    if not table_manifest_id:
                        logger.warning(f"No table audit entry found for table {table_name}, file {file_path_obj.name}")
                        # Create a placeholder table audit entry (this is a fallback for real files only)
                        table_manifest_id = self._create_placeholder_table_audit_entry(table_name, file_path_obj.name)
                
                # Calculate file info if file exists
                checksum = None
                filesize = None
                if file_path_obj.exists():
                    filesize = file_path_obj.stat().st_size
                    # Calculate SHA256 checksum
                    import hashlib
                    with open(file_path_obj, 'rb') as f:
                        file_hash = hashlib.sha256()
                        # Read file in chunks to handle large files efficiently
                        for chunk in iter(lambda: f.read(4096), b""):
                            file_hash.update(chunk)
                        checksum = file_hash.hexdigest()
                
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
                
                # Create manifest entry with table audit reference
                manifest_id = self.audit_service.create_file_manifest(
                    str(file_path_obj),
                    status=status,
                    table_manifest_id=table_manifest_id,  # Table audit reference
                    checksum=checksum,
                    filesize=filesize,
                    rows=rows_processed,
                    table_name=table_name,
                    notes=json.dumps(notes_data)
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

    def load_multiple_tables(self, database, table_to_files, pipeline_config, force_csv: bool = False):
        """
        Load multiple tables with proper hierarchical batch tracking:
        - One table-level context contains all file processing for all tables
        - Each file gets its own batch via FileLoader chunking
        - Each file batch can have parallel subbatches
        
        Args:
            force_csv: If True, force CSV loading and skip Parquet file detection
        """
        # Create table-level audit context for all tables
        table_names = list(table_to_files.keys())
        
        if self.audit_service and hasattr(self.audit_service, 'table_context'):
            table_context_manager = self.audit_service.table_context(table_names)
        else:
            # Fallback if no table context available
            from contextlib import nullcontext
            table_context_manager = nullcontext()
        
        with table_context_manager as table_manifest_id:
            logger.info(f"[LoadingStrategy] Starting table-level processing for {len(table_names)} tables")
            
            results = {}
            
            for table_name, zipfile_to_files in table_to_files.items():
                logger.info(f"[LoadingStrategy] Processing table '{table_name}' with FileLoader batching")
                
                # Process each file within the table as separate file-level operations
                table_results = []
                table_total_rows = 0
                table_success = True
                table_errors = []
                
                for zip_filename, csv_files in zipfile_to_files.items():
                    if not csv_files:
                        logger.warning(f"[LoadingStrategy] No CSV files in {zip_filename} for table '{table_name}'")
                        continue
                    
                    # Process files using FileLoader batch processing
                    file_batch_result = self._process_file_batch_with_fileloader(
                        database, table_name, zip_filename, csv_files, pipeline_config, force_csv
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
                
                # IMPROVEMENT: Update table audit completion immediately after each table finishes
                # This ensures individual completion timestamps rather than bulk timestamps
                if self.audit_service:
                    try:
                        from datetime import datetime
                        from sqlalchemy import text
                        import json
                        
                        # Prepare completion metadata for this table
                        table_result = results[table_name]
                        completion_metadata = {
                            "loading_completed": True,
                            "completion_timestamp": datetime.now().isoformat(),
                            "loading_success": table_result[0],
                            "rows_loaded": table_result[2],
                            "error_message": table_result[1] if not table_result[0] else None
                        }
                        
                        # Update table audit completion immediately for this table
                        with self.audit_service.database.engine.begin() as conn:
                            # Get year and month from config
                            year = self.config.year if hasattr(self.config, 'year') else self.config.pipeline.year
                            month = self.config.month if hasattr(self.config, 'month') else self.config.pipeline.month
                            
                            # First, get current audit_metadata for this specific table
                            current_result = conn.execute(text('''
                                SELECT notes FROM table_audit_manifest 
                                WHERE entity_name = :table_name 
                                AND ingestion_year = :year 
                                AND ingestion_month = :month
                            '''), {
                                'table_name': table_name,
                                'year': year,
                                'month': month
                            })
                            
                            row = current_result.fetchone()
                            current_metadata = row[0] if row and row[0] else {}
                            
                            # Merge with completion metadata
                            if isinstance(current_metadata, str):
                                current_metadata = json.loads(current_metadata)
                            elif current_metadata is None:
                                current_metadata = {}
                            
                            # Merge the completion metadata
                            merged_metadata = {**current_metadata, **completion_metadata}
                            
                            # Update with individual timestamp and merged metadata
                            conn.execute(text('''
                                UPDATE table_audit_manifest 
                                SET completed_at = :completed_at,
                                    notes = :metadata_json
                                WHERE entity_name = :table_name 
                                AND ingestion_year = :year 
                                AND ingestion_month = :month
                            '''), {
                                'completed_at': datetime.now(),  # Individual timestamp per table
                                'metadata_json': json.dumps(merged_metadata),
                                'table_name': table_name,
                                'year': year,
                                'month': month
                            })
                            
                            logger.info(f"[LoadingStrategy] Updated completion timestamp for table '{table_name}' individually")
                        
                    except Exception as e:
                        logger.warning(f"[LoadingStrategy] Failed to update completion timestamp for '{table_name}': {e}")
                    
            logger.info(f"[LoadingStrategy] Completed table-level processing for {len(table_names)} tables")
            return results

    def _process_file_batch_with_fileloader(self, database, table_name: str, zip_filename: str, 
                                          csv_files: List[str], pipeline_config, force_csv: bool = False) -> Tuple[bool, Optional[str], int]:
        """
        Process files using FileLoader batch processing with proper hierarchy.
        Each file creates its own batch/subbatch structure via FileLoader.
        
        Args:
            force_csv: If True, force CSV loading and skip Parquet file detection
        """
        logger.info(f"[LoadingStrategy] Processing {len(csv_files)} files for {table_name} using FileLoader")
        
        total_success = True
        total_processed_rows = 0
        total_errors = []
        
        # Process each CSV file with FileLoader batching
        for csv_file in csv_files:
            try:
                logger.info(f"[LoadingStrategy] Processing file: {csv_file}")
                
                # Use FileLoader-based processing via load_table
                # This will create proper File → Batch → Subbatch hierarchy
                success, error, rows = self.load_table(
                    database, table_name, pipeline_config, [csv_file],
                    batch_id=None,  # FileLoader will create its own batch hierarchy
                    subbatch_id=None,
                    force_csv=force_csv
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
                                          csv_files: List[str], pipeline_config, force_csv: bool = False) -> Tuple[bool, Optional[str], int]:
        """Fallback file loading without batch tracking."""
        return self.load_table(database, table_name, pipeline_config, csv_files, force_csv=force_csv)

    def _find_table_audit_for_file(self, table_name: str, filename: str) -> Optional[str]:
        """Find existing table audit entry ID for a given table, preferring real files over placeholders."""
        try:
            from sqlalchemy import text
            
            # First try to find a real audit entry (non-placeholder) for this table
            query_real = '''
            SELECT table_audit_id FROM table_audit_manifest 
            WHERE entity_name = :table_name 
            AND (audit_metadata IS NULL OR audit_metadata::jsonb ->> 'placeholder' IS NULL OR audit_metadata::jsonb ->> 'placeholder' != 'true')
            ORDER BY created_at DESC 
            LIMIT 1
            '''
            
            with self.audit_service.database.engine.connect() as conn:
                result = conn.execute(text(query_real), {'table_name': table_name})
                row = result.fetchone()
                if row:
                    return str(row[0])
            
            # Fallback: look for any audit entry for this table (including placeholders)
            query_any = '''
            SELECT table_audit_id FROM table_audit_manifest 
            WHERE entity_name = :table_name 
            ORDER BY created_at DESC 
            LIMIT 1
            '''
            
            with self.audit_service.database.engine.connect() as conn:
                result = conn.execute(text(query_any), {'table_name': table_name})
                row = result.fetchone()
                return str(row[0]) if row else None
                
        except Exception as e:
            logger.error(f"Failed to find audit ID for {table_name}/{filename}: {e}")
            return None

    def _find_table_audit_by_table_name(self, table_name: str) -> Optional[str]:
        """Find existing table audit entry ID for a given table name."""
        try:
            from sqlalchemy import text
            
            query = '''
            SELECT table_audit_id FROM table_audit_manifest 
            WHERE entity_name = :entity_name 
            ORDER BY created_at DESC 
            LIMIT 1
            '''
            
            with self.audit_service.database.engine.connect() as conn:
                result = conn.execute(text(query), {'entity_name': table_name})
                row = result.fetchone()
                manifest_id = str(row[0]) if row else None
                logger.debug(f"Table audit lookup for {table_name}: found={manifest_id is not None}, id={manifest_id}")
                return manifest_id
                
        except Exception as e:
            logger.error(f"Failed to find table audit ID for {table_name}: {e}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            return None

    def _create_placeholder_table_audit_entry(self, table_name: str, filename: str) -> str:
        """Create a placeholder table audit entry for files without existing audit records."""
        try:
            # Use new uniform audit model
            from ....database.models.audit import TableAuditManifest, AuditStatus
            from datetime import datetime
            import uuid
            
            # Create minimal table audit entry
            table_audit_id = str(uuid.uuid4())
            placeholder_audit = TableAuditManifest(
                table_audit_id=table_audit_id,
                entity_name=table_name,
                status=AuditStatus.COMPLETED,
                source_files=[filename],
                file_size_bytes=0,
                source_updated_at=datetime.now(),
                created_at=datetime.now(),
                started_at=datetime.now(),
                completed_at=datetime.now(),
                updated_at=datetime.now(),
                ingestion_year=self.config.year,
                ingestion_month=self.config.month,
                notes={"placeholder": True, "created_for": "file_manifest_requirement"}
            )
            
            # Insert placeholder audit entry
            
            with self.audit_service.database.session_maker() as session:
                session.add(placeholder_audit)
                session.commit()
                
            logger.info(f"Created placeholder table audit entry {table_audit_id} for {table_name}/{filename}")
            return table_audit_id
            
        except Exception as e:
            logger.error(f"Failed to create placeholder table audit entry for {table_name}/{filename}: {e}")
            raise RuntimeError(f"Cannot create file manifest without table_audit_id: {e}")