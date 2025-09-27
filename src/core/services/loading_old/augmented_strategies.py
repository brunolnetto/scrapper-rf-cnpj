"""
Enhanced data loading strategies with memory awareness and improved code organization.
Eliminates code duplication and integrates with memory monitoring.
"""
from abc import ABC, abstractmethod
from typing import List, Tuple, Optional, Dict, Any
from pathlib import Path
from datetime import datetime
from contextlib import nullcontext
import tempfile
import json
import uuid
import hashlib
import os
import asyncio

from ....database.engine import Database
from ....database.models.audit import AuditStatus
from ....setup.logging import logger
from ....database.dml import table_name_to_table_info
from ....database.engine import Database
from .file_loader import base


class BaseDataLoadingStrategy(ABC):
    """Abstract base class for data loading strategies."""
    
    @abstractmethod
    def load_table(
        self, database: Database, table_name: str,
        table_files: Optional[List[str]] = None
    ) -> Tuple[bool, Optional[str], int]:
        """Load a single table."""
        pass


class DataLoadingStrategy(BaseDataLoadingStrategy):
    """
    Enhanced loading strategy with memory awareness and reduced code duplication.
    """
    
    def __init__(self, config, audit_service=None, memory_monitor=None):
        self.config = config
        self.audit_service = audit_service
        self.memory_monitor = memory_monitor
        self._pipeline_batch_id = None
        logger.info("Memory-aware loading strategy initialized")
    
    def set_pipeline_batch_id(self, batch_id: str):
        """Set the pipeline-level batch ID for proper hierarchy."""
        self._pipeline_batch_id = batch_id
        logger.debug(f"Pipeline batch ID set: {batch_id}")

    def load_table(
        self, database: Database, table_name: str, 
        table_files: Optional[List[str]] = None, 
        batch_id=None, subbatch_id=None, force_csv: bool = False
    ) -> Tuple[bool, Optional[str], int]:
        """
        Load table with memory awareness and unified file handling.
        """
        logger.info(f"[MemoryAwareStrategy] Loading table '{table_name}'")
        
        # Memory check before processing
        if self.memory_monitor and self.memory_monitor.should_prevent_processing():
            error_msg = "Insufficient memory to process table"
            logger.error(f"[MemoryAwareStrategy] {error_msg}")
            return False, error_msg, 0
        
        try:
            table_info = table_name_to_table_info(table_name)
            loader = self._create_loader(database)
            
            # Apply development filtering early
            filtered_files = self._apply_development_filtering(table_files, table_name)
            
            # Choose loading path: Parquet first, then CSV
            if not force_csv:
                parquet_result = self._load_parquet_file(
                    loader, table_info, table_name, batch_id, subbatch_id
                )
                if parquet_result:
                    return parquet_result
            
            if filtered_files:
                return self._load_csv_files(
                    loader, table_info, filtered_files, table_name, batch_id, subbatch_id
                )
            
            return False, f"No files found for table {table_name}", 0
                
        except Exception as e:
            logger.error(f"[MemoryAwareStrategy] Failed to load table '{table_name}': {e}")
            return False, str(e), 0

    def _create_loader(self, database: Database):
        """Create appropriate loader instance."""
        from ....database.dml import UnifiedLoader
        return UnifiedLoader(database, self.config)

    def _apply_development_filtering(self, table_files: Optional[List[str]], table_name: str) -> Optional[List[Path]]:
        """Apply development filtering to files."""
        if not table_files:
            return None
            
        from ....core.utils.development_filter import DevelopmentFilter
        dev_filter = DevelopmentFilter(self.config.pipeline.development)
        
        file_paths = [Path(f) for f in table_files]
        filtered_paths = dev_filter.filter_files_by_blob_limit(file_paths, table_name)
        
        if not filtered_paths:
            logger.info(f"[MemoryAwareStrategy] No files to load for table '{table_name}' after filtering")
        
        return filtered_paths

    def _load_parquet_file(self, loader, table_info, table_name: str, batch_id=None, subbatch_id=None) -> Optional[Tuple[bool, Optional[str], int]]:
        """Try loading from Parquet file if available."""
        parquet_file = self.config.data_sink.paths.get_temporal_conversion_path(self.config.year, self.config.month) / f"{table_name}.parquet"
        
        if not parquet_file.exists():
            return None
            
        from ....core.utils.development_filter import DevelopmentFilter
        dev_filter = DevelopmentFilter(self.config.pipeline.development)
        
        if not dev_filter.check_blob_size_limit(parquet_file):
            return True, "Skipped large file in development mode", 0
        
        logger.info(f"[MemoryAwareStrategy] Loading Parquet file: {parquet_file.name}")
        return self._load_csv_file(loader, table_info, parquet_file, table_name, batch_id, subbatch_id)

    def _load_csv_files(self, loader, table_info, file_paths: List[Path], table_name: str, batch_id=None, subbatch_id=None) -> Tuple[bool, Optional[str], int]:
        """Load CSV files with memory optimization."""
        logger.info(f"[MemoryAwareStrategy] Loading {len(file_paths)} CSV files for table: {table_name}")
        
        total_rows = 0
        
        for file_path in file_paths:
            # Resolve file path
            if isinstance(file_path, str):
                csv_file = self.config.data_sink.paths.get_temporal_extraction_path(self.config.year, self.config.month) / file_path
            else:
                csv_file = file_path
            
            logger.info(f"[MemoryAwareStrategy] Processing CSV file: {csv_file.name}")
            
            success, error, rows = self._load_csv_file(loader, table_info, csv_file, table_name, batch_id, subbatch_id)
            
            if not success:
                logger.error(f"[MemoryAwareStrategy] Failed to load {csv_file.name}: {error}")
                return success, error, total_rows
            
            total_rows += rows
            logger.info(f"[MemoryAwareStrategy] Successfully loaded {rows:,} rows from {csv_file.name}")
            
            # Memory cleanup between files
            if self.memory_monitor and self.memory_monitor.is_memory_pressure_high():
                cleanup_stats = self.memory_monitor.perform_aggressive_cleanup()
                logger.info(f"Inter-file cleanup freed {cleanup_stats.get('freed_mb', 0):.1f}MB")
        
        return True, None, total_rows

    def _load_csv_file(self, loader, table_info, file_path: Path, table_name: str, batch_id=None, subbatch_id=None) -> Tuple[bool, Optional[str], int]:
        """
        Load a single file with integrated memory monitoring and FileLoader batch processing.
        """
        try:
            # Create memory-aware file loader
            from .file_loader.augmented_file_loader import create_file_loader
            
            encoding = getattr(self.config.pipeline.data_source, 'encoding', 'utf-8')
            file_loader = create_file_loader(str(file_path), encoding=encoding, memory_monitor=self.memory_monitor)
            
            logger.info(f"[MemoryAwareStrategy] FileLoader detected format: {file_loader.get_format()}")
            
            # Get processing recommendations
            recommendations = file_loader.get_recommended_processing_params()
            logger.debug(f"[MemoryAwareStrategy] Processing recommendations: {recommendations}")
            
            # Use FileLoader with batch processing
            return self._process_file_with_batching(file_loader, loader, table_info, table_name, recommendations)

        except Exception as e:
            logger.error(f"[MemoryAwareStrategy] Failed to load file {file_path}: {e}")
            return False, str(e), 0

    def _process_file_with_batching(self, file_loader, loader, table_info, table_name: str, recommendations: Dict[str, Any]) -> Tuple[bool, Optional[str], int]:
        """
        Process file using FileLoader batching with memory awareness.
        """
        total_processed_rows = 0
        batch_num = 0
        headers = table_info.columns
        
        # Get recommended chunk size
        chunk_size = recommendations.get('final_chunk_size', recommendations.get('chunk_size', 20_000))
        
        # Find table audit for proper linking
        table_manifest_id = self._find_table_audit_by_table_name(table_name)
        
        # Create file manifest if audit service available
        file_manifest_id = None
        if table_manifest_id and self.audit_service:
            file_manifest_id = self._create_file_manifest(str(file_loader.file_path), table_name, table_manifest_id)
        
        try:
            # Process file in batches using FileLoader
            for batch_chunk in file_loader.batch_generator(headers, chunk_size=chunk_size):
                batch_num += 1
                
                # Memory check before each batch
                if self.memory_monitor and self.memory_monitor.should_prevent_processing():
                    error_msg = f"Memory limit exceeded at batch {batch_num}"
                    logger.error(f"[MemoryAwareStrategy] {error_msg}")
                    self._update_file_manifest(file_manifest_id, AuditStatus.FAILED, total_processed_rows, error_msg)
                    return False, error_msg, total_processed_rows
                
                # Process batch with context management
                success, error, rows = self._process_batch_with_context(
                    batch_chunk, loader, table_info, table_name, batch_num, 
                    file_manifest_id, table_manifest_id, recommendations
                )
                
                if not success:
                    self._update_file_manifest(file_manifest_id, AuditStatus.FAILED, total_processed_rows, error)
                    return False, error, total_processed_rows
                
                total_processed_rows += rows
                logger.debug(f"[MemoryAwareStrategy] Batch {batch_num} completed: {rows} rows")
            
            # Mark file as completed
            self._update_file_manifest(file_manifest_id, AuditStatus.COMPLETED, total_processed_rows)
            
            logger.info(f"[MemoryAwareStrategy] File processing complete: {total_processed_rows} total rows")
            return True, None, total_processed_rows
            
        except Exception as e:
            logger.error(f"[MemoryAwareStrategy] File processing failed: {e}")
            self._update_file_manifest(file_manifest_id, AuditStatus.FAILED, total_processed_rows, str(e))
            return False, str(e), total_processed_rows

    def _process_batch_with_context(
        self, 
        batch_chunk: List[Tuple], 
        loader, table_info, 
        table_name: str, 
        batch_num: int, 
        file_manifest_id: Optional[str], table_manifest_id: Optional[str], recommendations: Dict[str, Any]
    ) -> Tuple[bool, Optional[str], int]:
        """
        Process a batch chunk with proper audit context management.
        """
        if not self.audit_service:
            # Fallback without context management
            return self._load_batch_chunk_direct(batch_chunk, loader, table_info)
        
        # Create batch context
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]
        batch_name = f"MemoryAware_Batch_{table_name}_chunk{batch_num}_{len(batch_chunk)}rows_{timestamp}"
        
        with self.audit_service.batch_context(
            target_table=table_name,
            batch_name=batch_name,
            file_manifest_id=file_manifest_id,
            table_manifest_id=table_manifest_id
        ) as batch_id:
            
            logger.info(f"[MemoryAwareStrategy] Processing batch {batch_num}: {len(batch_chunk)} rows")
            
            # Process subbatches
            subbatch_size = recommendations.get('sub_batch_size', 3_000)
            subbatch_chunks = self._split_into_subbatches(batch_chunk, subbatch_size)
            
            total_batch_rows = 0
            
            for subbatch_num, subbatch_chunk in enumerate(subbatch_chunks):
                subbatch_name = f"Subbatch_{subbatch_num+1}of{len(subbatch_chunks)}_rows{len(subbatch_chunk)}"
                
                with self.audit_service.subbatch_context(
                    batch_id=batch_id,
                    table_name=table_name,
                    description=subbatch_name
                ) as subbatch_id:
                    
                    success, error, rows = self._load_batch_chunk_direct(subbatch_chunk, loader, table_info)
                    
                    # Record processing event
                    if subbatch_id:
                        status = AuditStatus.COMPLETED if success else AuditStatus.FAILED
                        self.audit_service.collect_file_processing_event(
                            subbatch_id=subbatch_id,
                            status=status,
                            rows=rows or 0,
                            bytes_=0
                        )
                    
                    if not success:
                        return False, error, total_batch_rows
                    
                    total_batch_rows += rows
            
            return True, None, total_batch_rows

    def _load_batch_chunk_direct(
        self, batch_chunk: List[Tuple], loader, table_info
    ) -> Tuple[bool, Optional[str], int]:
        """
        Load batch chunk directly without temporary files - addresses critical performance issue.
        """
        try:
            if not batch_chunk:
                return True, None, 0
            
            # Check if loader supports direct record loading
            if hasattr(loader, 'load_records_directly'):
                # Direct path - no temporary files needed
                return loader.load_records_directly(
                    table_info=table_info,
                    records=batch_chunk
                )
            
            # Fallback: use the existing uploader's copy_records_to_table mechanism
            # This avoids the DataFrame -> Parquet -> file I/O roundtrip
            return self._load_records_via_uploader(batch_chunk, loader, table_info)
                    
        except Exception as e:
            logger.error(f"[MemoryAwareStrategy] Failed to load batch chunk: {e}")
            return False, str(e), 0

    def _load_records_via_uploader(self, batch_chunk: List[Tuple], loader, table_info) -> Tuple[bool, Optional[str], int]:
        """
        Load records directly via uploader mechanism, bypassing temporary files.
        """
        async_pool = None
        try:
            # Ensure the audit service and its database object are available
            if not hasattr(self, 'audit_service') or not self.audit_service.database:
                raise ValueError("Audit service or database not configured for direct upload.")

            database = self.audit_service.database

            # Correctly structure the logic to get an async pool.
            # First, try to get a cached or existing pool from the database object.
            if hasattr(database, '_async_pool_cache') and database._async_pool_cache:
                async_pool = database._async_pool_cache
            # As a fallback, if the database object has a SQLAlchemy engine, create a pool from it.
            elif hasattr(database, 'engine') and database.engine:
                try:
                    # The _get_or_create_async_pool helper is the intended way to do this.
                    async_pool = self._get_or_create_async_pool(database)
                except Exception as e:
                    logger.warning(f"Could not create async pool from engine: {e}")
                    # If pool creation fails, use the temp file fallback.
                    return self._minimal_temp_file_fallback(batch_chunk, loader, table_info)
            
            # If no pool could be obtained through any method, use the fallback.
            if not async_pool:
                logger.debug("No async pool was available, using temp file fallback.")
                return self._minimal_temp_file_fallback(batch_chunk, loader, table_info)

            # If a pool was successfully obtained, proceed with the direct upload.
            return self._process_with_async_pool(batch_chunk, table_info, async_pool)

        except Exception as e:
            logger.error(f"[MemoryAwareStrategy] Records via uploader failed unexpectedly: {e}", exc_info=True)
            return self._minimal_temp_file_fallback(batch_chunk, loader, table_info)

    def _get_or_create_async_pool(self, database):
        """Get or create async pool for database operations."""
        try:
            # Check if we already have a pool stored
            if hasattr(database, '_async_pool_cache') and database._async_pool_cache:
                return database._async_pool_cache
            
            # Create new pool
            from ....database.utils import create_asyncpg_pool_from_sqlalchemy
            
            # This needs to be async, so we'll handle it in the caller
            return None  # Signal that we need async creation
            
        except Exception as e:
            logger.warning(f"Failed to get async pool: {e}")
            return None

    def _process_with_async_pool(self, batch_chunk: List[Tuple], table_info, async_pool) -> Tuple[bool, Optional[str], int]:
        """Process records using async pool."""
        try:
            # Create a simple async function to handle the processing
            async def async_process():
                from .file_loader.augmented_uploader import FileUploader
                uploader = FileUploader(memory_monitor=self.memory_monitor)
                
                async with uploader.managed_connection(async_pool) as conn:
                    tmp_table = f"tmp_direct_{os.getpid()}_{uuid.uuid4().hex[:8]}"
                    headers = table_info.columns
                    types_map = base.map_types(headers, {})
                    
                    try:
                        await conn.execute(base.create_temp_table_sql(tmp_table, headers, types_map))
                        
                        async with conn.transaction():
                            await conn.copy_records_to_table(tmp_table, records=batch_chunk, columns=headers)
                            
                            primary_keys = self._get_primary_keys_from_table_info(table_info)
                            if primary_keys:
                                sql = base.upsert_from_temp_sql(table_info.table_name, tmp_table, headers, primary_keys)
                                await conn.execute(sql)
                            else:
                                await conn.execute(f'INSERT INTO {base.quote_ident(table_info.table_name)} SELECT * FROM {base.quote_ident(tmp_table)}')
                        
                        return True, None, len(batch_chunk)
                        
                    finally:
                        try:
                            await conn.execute(f'DROP TABLE IF EXISTS {base.quote_ident(tmp_table)}')
                        except Exception:
                            pass
            
            # Handle async execution based on context
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    # We're in an async context, but we need to return sync
                    # Create a new thread for async processing
                    import concurrent.futures
                    import threading
                    
                    def run_in_new_loop():
                        new_loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(new_loop)
                        try:
                            return new_loop.run_until_complete(async_process())
                        finally:
                            new_loop.close()
                    
                    with concurrent.futures.ThreadPoolExecutor() as executor:
                        future = executor.submit(run_in_new_loop)
                        return future.result(timeout=300)  # 5 minute timeout
                else:
                    return loop.run_until_complete(async_process())
                    
            except Exception as e:
                logger.warning(f"Async processing failed: {e}, falling back to temp file")
                return self._minimal_temp_file_fallback(batch_chunk, loader, table_info)
                
        except Exception as e:
            logger.error(f"[MemoryAwareStrategy] Async pool processing failed: {e}")
            return self._minimal_temp_file_fallback(batch_chunk, loader, table_info)

    def _minimal_temp_file_fallback(self, batch_chunk: List[Tuple], loader, table_info) -> Tuple[bool, Optional[str], int]:
        """
        Minimal temporary file creation avoiding pandas DataFrame.
        Uses PyArrow directly for better memory efficiency.
        """
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
            from pathlib import Path
            
            # Create PyArrow table directly from tuples (more memory efficient than pandas)
            columns = {}
            for i, col_name in enumerate(table_info.columns):
                columns[col_name] = [row[i] if i < len(row) else None for row in batch_chunk]
            
            # Create Arrow table (more efficient than pandas DataFrame)
            arrow_table = pa.table(columns)
            
            # Write to temporary file using PyArrow (more efficient than pandas.to_parquet)
            with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as temp_file:
                temp_path = Path(temp_file.name)
                
                try:
                    # Use PyArrow writer with compression for better performance
                    pq.write_table(arrow_table, temp_path, compression='snappy')
                    
                    # Load using existing loader
                    success, error, rows = loader.load_file(
                        table_info=table_info,
                        file_path=temp_path,
                        batch_id=None,
                        subbatch_id=None
                    )
                    
                    return success, error, rows
                    
                finally:
                    # Cleanup
                    temp_path.unlink(missing_ok=True)
                    del arrow_table
                    del columns
                    
        except Exception as e:
            logger.error(f"[MemoryAwareStrategy] Minimal temp file fallback failed: {e}")
            return False, str(e), 0

    def _get_primary_keys_from_table_info(self, table_info) -> List[str]:
        """Extract primary keys from table info."""
        try:
            if hasattr(table_info, 'primary_keys') and table_info.primary_keys:
                return table_info.primary_keys
            
            # Fallback: try to get from table model
            if hasattr(table_info, 'table_model'):
                pk_columns = [col.name for col in table_info.table_model.__table__.primary_key.columns]
                if pk_columns:
                    return pk_columns
            
            # Default fallback
            logger.warning(f"No primary keys found for {table_info.table_name}")
            return []
            
        except Exception as e:
            logger.warning(f"Failed to extract primary keys: {e}")
            return []

    def load_multiple_tables(self, database, table_to_files, force_csv: bool = False):
        """
        Load multiple tables with memory awareness and optimized processing order.
        """
        table_names = list(table_to_files.keys())
        
        # Memory pre-check
        if self.memory_monitor and not self._perform_memory_precheck():
            raise MemoryError("Insufficient memory to process all tables")
        
        # Optimize processing order
        optimized_order = self._optimize_processing_order(table_to_files)
        
        # Create table context if available
        context_manager = self._create_table_context(table_names)
        
        with context_manager as table_manifest_id:
            logger.info(f"[MemoryAwareStrategy] Starting processing for {len(table_names)} tables")
            
            results = {}
            
            for table_name in optimized_order:
                if table_name not in table_to_files:
                    continue
                
                logger.info(f"[MemoryAwareStrategy] Processing table '{table_name}'")
                
                # Memory check before each table
                if self.memory_monitor and self.memory_monitor.should_prevent_processing():
                    error_msg = f"Memory limit exceeded before processing {table_name}"
                    logger.error(f"[MemoryAwareStrategy] {error_msg}")
                    results[table_name] = (False, error_msg, 0)
                    continue
                
                # Process table
                table_result = self._process_single_table(database, table_name, table_to_files[table_name], force_csv)
                results[table_name] = table_result
                
                # Update individual table audit completion
                self._update_table_audit_completion(table_name, table_result)
                
                # Inter-table cleanup
                if self.memory_monitor and self.memory_monitor.is_memory_pressure_high():
                    cleanup_stats = self.memory_monitor.perform_aggressive_cleanup()
                    logger.info(f"Inter-table cleanup freed {cleanup_stats.get('freed_mb', 0):.1f}MB")
            
            return results

    def _perform_memory_precheck(self) -> bool:
        """Perform memory pre-check before starting processing."""
        if not self.memory_monitor:
            return True
        
        try:
            status = self.memory_monitor.get_status_report()
            logger.info(f"[MemoryAwareStrategy] Pre-processing memory status: "
                       f"Usage: {status['usage_above_baseline_mb']:.1f}MB, "
                       f"Budget: {status['budget_remaining_mb']:.1f}MB")
            
            return not self.memory_monitor.should_prevent_processing()
            
        except Exception as e:
            logger.error(f"Memory pre-check failed: {e}")
            return False

    def _optimize_processing_order(self, table_to_files: Dict[str, Dict]) -> List[str]:
        """Optimize processing order by file size (smallest first)."""
        try:
            table_sizes = []
            
            for table_name, zipfile_to_files in table_to_files.items():
                total_size = 0
                for csv_files in zipfile_to_files.values():
                    for csv_file in csv_files:
                        try:
                            file_path = Path(csv_file)
                            if file_path.exists():
                                total_size += file_path.stat().st_size
                        except Exception:
                            total_size += 1000  # Default small size
                
                table_sizes.append((table_name, total_size))
            
            # Sort by size (smallest first)
            table_sizes.sort(key=lambda x: x[1])
            optimized_order = [table_name for table_name, _ in table_sizes]
            
            logger.info(f"[MemoryAwareStrategy] Optimized processing order: {optimized_order}")
            return optimized_order
            
        except Exception as e:
            logger.error(f"Failed to optimize processing order: {e}")
            return list(table_to_files.keys())

    def _create_table_context(self, table_names: List[str]):
        """Create appropriate table context manager."""
        if self.audit_service and hasattr(self.audit_service, 'table_context'):
            return self.audit_service.table_context(table_names)
        else:
            return nullcontext()

    def _process_single_table(
        self, 
        database, 
        table_name: str, 
        zipfile_to_files: Dict,  
        force_csv: bool
    ) -> Tuple[bool, Optional[str], int]:
        """Process a single table with all its files."""
        table_success = True
        table_total_rows = 0
        table_errors = []
        
        for zip_filename, csv_files in zipfile_to_files.items():
            if not csv_files:
                logger.warning(f"[MemoryAwareStrategy] No CSV files in {zip_filename} for table '{table_name}'")
                continue
            
            success, error, rows = self.load_table(database, table_name, csv_files, force_csv=force_csv)
            
            table_total_rows += rows
            
            if not success:
                table_success = False
                table_errors.append(f"{zip_filename}: {error}")
        
        if table_success:
            return True, None, table_total_rows
        else:
            error_msg = "; ".join(table_errors)
            return False, error_msg, table_total_rows

    def _update_table_audit_completion(
        self, table_name: str, table_result: Tuple[bool, Optional[str], int]
    ):
        """Update individual table audit completion."""
        if not self.audit_service:
            return
        
        try:
            from sqlalchemy import text
            
            success, error, rows = table_result
            completion_metadata = {
                "loading_completed": True,
                "completion_timestamp": datetime.now().isoformat(),
                "loading_success": success,
                "rows_loaded": rows,
                "error_message": error if not success else None
            }
            
            if self.memory_monitor:
                status = self.memory_monitor.get_status_report()
                completion_metadata["memory_info"] = {
                    "peak_usage_mb": status['usage_above_baseline_mb'],
                    "pressure_level": status['pressure_level']
                }
            
            with self.audit_service.database.engine.begin() as conn:
                # Update table audit completion
                conn.execute(text('''
                    UPDATE table_audit_manifest 
                    SET completed_at = :completed_at,
                        notes = :metadata_json
                    WHERE entity_name = :table_name 
                    AND ingestion_year = :year 
                    AND ingestion_month = :month
                '''), {
                    'completed_at': datetime.now(),
                    'metadata_json': json.dumps(completion_metadata),
                    'table_name': table_name,
                    'year': self.config.year,
                    'month': self.config.month
                })
                
                logger.info(f"[MemoryAwareStrategy] Updated completion for table '{table_name}'")
                
        except Exception as e:
            logger.warning(f"[MemoryAwareStrategy] Failed to update completion for '{table_name}': {e}")

    # Utility methods with reduced duplication
    def _split_into_subbatches(self, batch_chunk: List[Tuple], subbatch_size: int) -> List[List[Tuple]]:
        """Split batch into subbatches."""
        return [batch_chunk[i:i + subbatch_size] for i in range(0, len(batch_chunk), subbatch_size)]

    def _find_table_audit_by_table_name(self, table_name: str) -> Optional[str]:
        """Find existing table audit entry ID."""
        if not self.audit_service:
            return None
        
        try:
            from sqlalchemy import text
            
            with self.audit_service.database.engine.connect() as conn:
                result = conn.execute(text('''
                    SELECT table_audit_id FROM table_audit_manifest 
                    WHERE entity_name = :entity_name 
                    ORDER BY created_at DESC 
                    LIMIT 1
                '''), {'entity_name': table_name})
                
                row = result.fetchone()
                return str(row[0]) if row else None
                
        except Exception as e:
            logger.error(f"Failed to find table audit ID for {table_name}: {e}")
            return None

    def _create_file_manifest(self, file_path: str, table_name: str, table_manifest_id: str) -> Optional[str]:
        """Create file manifest entry."""
        if not self.audit_service:
            return None
        
        try:
            file_path_obj = Path(file_path)
            
            # Calculate file info
            checksum = None
            filesize = None
            if file_path_obj.exists():
                filesize = file_path_obj.stat().st_size
                checksum = self._calculate_file_checksum(file_path_obj)
            
            # Create manifest entry
            notes_data = {
                "file_info": {
                    "size_bytes": filesize,
                    "format": file_path_obj.suffix.lstrip('.') if file_path_obj.suffix else "unknown"
                },
                "processing": {
                    "status": AuditStatus.RUNNING.value,
                    "table_name": table_name
                }
            }
            
            manifest_id = self.audit_service.create_file_manifest(
                str(file_path_obj),
                status=AuditStatus.RUNNING,
                table_manifest_id=table_manifest_id,
                checksum=checksum,
                filesize=filesize,
                table_name=table_name,
                notes=json.dumps(notes_data)
            )
            
            return manifest_id
            
        except Exception as e:
            logger.warning(f"Failed to create file manifest for {file_path}: {e}")
            return None

    def _update_file_manifest(self, manifest_id: Optional[str], status: AuditStatus, rows_processed: int, error_msg: Optional[str] = None):
        """Update file manifest entry."""
        if not manifest_id or not self.audit_service:
            return
        
        try:
            notes_data = {
                "processing_update": {
                    "final_status": status.value,
                    "completion_timestamp": datetime.now().isoformat(),
                    "rows_processed": rows_processed
                }
            }
            
            if error_msg:
                notes_data["processing_update"]["error_message"] = error_msg
            
            self.audit_service.update_file_manifest(
                manifest_id=manifest_id,
                status=status,
                rows_processed=rows_processed,
                error_msg=error_msg,
                notes=json.dumps(notes_data)
            )
            
        except Exception as e:
            logger.warning(f"Failed to update file manifest {manifest_id}: {e}")

    def _calculate_file_checksum(self, file_path: Path) -> Optional[str]:
        """Calculate file checksum efficiently."""
        try:
            file_hash = hashlib.sha256()
            with open(file_path, 'rb') as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    file_hash.update(chunk)
            return file_hash.hexdigest()
        except Exception as e:
            logger.warning(f"Failed to calculate checksum for {file_path}: {e}")
            return None


def create_strategy(config, audit_service=None, memory_monitor=None) -> DataLoadingStrategy:
    """Factory function to create memory-aware strategy."""
    return DataLoadingStrategy(config, audit_service, memory_monitor)