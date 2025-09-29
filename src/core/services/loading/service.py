"""
Fixed LoadingService with proper async/sync boundary handling.
This version detects the calling context and handles both scenarios correctly.
"""
from typing import List, Tuple, Optional, Dict, Any
from pathlib import Path
from datetime import datetime
from contextlib import nullcontext
import json
import hashlib
import uuid
import asyncio
import threading

from ....setup.logging import logger
from ....setup.config.loader import ConfigLoader
from ....database.engine import Database
from ....database.models.audit import AuditStatus
from ....database.utils import table_name_to_table_info
from ....database.schemas import TableInfo
from ....database.service import DatabaseService
from ...schemas import AuditMetadata

from ..memory.service import MemoryMonitor
from ..audit.service import AuditService
from .batch_processor import BatchProcessor
from .file_handler import FileHandler


class FileLoadingService:
    """
    Main orchestrator with proper async/sync context detection and handling.
    """
    
    def __init__(
        self,
        database: Database,
        config: ConfigLoader,
        audit_service: AuditService
    ):
        self.database = database
        self.config = config
        self.audit_service = audit_service
        self.memory_monitor = MemoryMonitor(config.pipeline.memory)
        
        # Initialize service components
        self.file_handler = FileHandler(config)
        self.database_service = DatabaseService(database, self.memory_monitor)
        self.batch_processor = BatchProcessor(config, self.audit_service)

        self._connection_pool = None
        self._pool_lock = None
        self._init_lock = threading.Lock()

        
        logger.info("LoadingService initialized with integrated memory monitoring")

    def _detect_async_context(self) -> bool:
        """
        Detect if we're currently running in an async context.
        """
        try:
            loop = asyncio.get_running_loop()
            return True
        except RuntimeError:
            return False

    async def _ensure_connection_pool(self):
        # create asyncio lock lazily, but guard creation with a thread-safe lock
        if self._pool_lock is None:
            with self._init_lock:
                if self._pool_lock is None:
                    self._pool_lock = asyncio.Lock()

        loop = asyncio.get_running_loop()
        logger.debug(f"[LoadingService] Ensuring pool on loop id={id(loop)}")

        async with self._pool_lock:
            if self._connection_pool is None:
                logger.info(f"[LoadingService] Creating connection pool on loop id={id(loop)}")
                self._connection_pool = await self.database.get_async_pool()
        return self._connection_pool



    async def load_table(
        self, 
        table_name: str,
        table_files: Optional[List[str]] = None,
        batch_id: Optional[str] = None,
        subbatch_id: Optional[str] = None,
        force_csv: bool = False
    ) -> Tuple[bool, Optional[str], int]:
        """
        Load a single table with memory awareness and integrated processing.
        FIX: Handles both sync and async calling contexts properly.
        """
        logger.info(f"[LoadingService] Loading table '{table_name}'")
        
        # Memory check before processing
        if self.memory_monitor and self.memory_monitor.should_prevent_processing():
            error_msg = "Insufficient memory to process table"
            logger.error(f"[LoadingService] {error_msg}")
            return False, error_msg, 0
        
        try:
            table_info: TableInfo = table_name_to_table_info(table_name)
            
            # Apply development filtering early
            filtered_files = self._apply_development_filtering(table_files, table_name)
            
            # Choose loading path: Parquet first, then CSV
            if not force_csv:
                parquet_result = await self._try_parquet_loading(table_info, table_name)
                if parquet_result:
                    return parquet_result
            
            if filtered_files:
                return await self._load_csv_files(table_info, filtered_files, table_name)
            
            return False, f"No files found for table {table_name}", 0
                
        except Exception as e:
            logger.error(f"[LoadingService] Failed to load table '{table_name}': {e}")
            return False, str(e), 0

    def _apply_development_filtering(self, table_files: Optional[List[str]], table_name: str) -> Optional[List[Path]]:
        """Apply development filtering to files."""
        if not table_files:
            return None
            
        from ....core.utils.development_filter import DevelopmentFilter
        dev_filter = DevelopmentFilter(self.config.pipeline.development)
        
        file_paths = [Path(f) for f in table_files]
        filtered_paths = dev_filter.filter_files_by_blob_limit(file_paths, table_name)
        
        if not filtered_paths:
            logger.info(f"[LoadingService] No files to load for table '{table_name}' after filtering")
        
        return filtered_paths

    async def _try_parquet_loading(self, table_info: TableInfo, table_name: str) -> Optional[Tuple[bool, Optional[str], int]]:
        """Try loading from Parquet file if available with robust config handling."""
        try:
            parquet_file = None
                    
            try:
                parquet_file = self.config.pipeline.data_sink.paths.get_temporal_conversion_path(
                    self.config.year, self.config.month
                ) / f"{table_name}.parquet"
            except AttributeError:
                pass
            
            if not parquet_file:
                # Final fallback - construct from year/month if available
                if hasattr(self.config, 'year') and hasattr(self.config, 'month'):
                    base_path = Path(f"/tmp/conversion/{self.config.year}/{self.config.month}")
                    parquet_file = base_path / f"{table_name}.parquet"
                else:
                    logger.debug(f"[LoadingService] No Parquet file path found for {table_name}")
                    return None
        except Exception as e:
            logger.debug(f"[LoadingService] Config path resolution failed: {e}")
            return None
        
        if not parquet_file or not parquet_file.exists():
            return None
            
        # Apply development filtering if available
        try:
            from ....core.utils.development_filter import DevelopmentFilter
            dev_filter = DevelopmentFilter(self.config.pipeline.development)
            
            if not dev_filter.check_blob_size_limit(parquet_file):
                return True, "Skipped large file in development mode", 0
        except (ImportError, AttributeError):
            # Development filter not available or configured
            pass
        
        logger.info(f"[LoadingService] Loading Parquet file: {parquet_file.name}")
        return await self._load_single_file(table_info, parquet_file, table_name)

    async def _load_csv_files(self, table_info: TableInfo, file_paths: List[Path], table_name: str) -> Tuple[bool, Optional[str], int]:
        """Load CSV files with memory optimization."""
        logger.info(f"[LoadingService] Loading {len(file_paths)} CSV files for table: {table_name}")
        
        # Optimize processing order (smallest first)
        optimized_files = self.file_handler.optimize_processing_order(file_paths)
        
        total_rows = 0
        
        for file_path in optimized_files:
            # Resolve file path
            if isinstance(file_path, str):
                try:
                    csv_file = self.config.data_sink.paths.get_temporal_extraction_path(
                        self.config.year, self.config.month
                    ) / file_path
                except AttributeError:
                    csv_file = Path(file_path)
            else:
                csv_file = file_path
            
            logger.info(f"[LoadingService] Processing CSV file: {csv_file.name}")
            
            success, error, rows = await self._load_single_file(table_info, csv_file, table_name)
            
            if not success:
                logger.error(f"[LoadingService] Failed to load {csv_file.name}: {error}")
                return success, error, total_rows
            
            total_rows += rows
            logger.info(f"[LoadingService] Successfully loaded {rows:,} rows from {csv_file.name}")
            
            # Memory cleanup between files
            if self.memory_monitor and self.memory_monitor.is_memory_pressure_high():
                cleanup_stats = self.memory_monitor.perform_aggressive_cleanup()
                logger.info(f"Inter-file cleanup freed {cleanup_stats.get('freed_mb', 0):.1f}MB")
        
        return True, None, total_rows

    async def _load_single_file(self, table_info: TableInfo, file_path: Path, table_name: str) -> Tuple[bool, Optional[str], int]:
        """
        Load a single file using coordinated FileHandler + DatabaseService + BatchProcessor.
        FIX: Properly handles async context detection and coroutine execution.
        """
        try:
            # Get processing recommendations from FileHandler
            recommendations = self.file_handler.get_recommended_processing_params(str(file_path), table_info.columns)
            logger.debug(f"[LoadingService] Processing recommendations: {recommendations}")
            
            # Memory check before processing
            if recommendations.get('memory_info', {}).get('should_prevent', False):
                error_msg = "Memory constraints prevent processing this file"
                logger.error(f"[LoadingService] {error_msg}")
                return False, error_msg, 0
            
            return await self._async_load_single_file(
                table_info, file_path, table_name, recommendations
            )
                
        except Exception as e:
            logger.error(f"[LoadingService] Failed to load file {file_path}: {e}")
            return False, str(e), 0

    async def _async_load_single_file(
        self, 
        table_info: TableInfo, 
        file_path: Path, 
        table_name: str,
        recommendations: dict
    ) -> Tuple[bool, Optional[str], int]:
        """
        Async implementation of single file loading.
        """

        # Generate batches using FileHandler
        stream  = await self.file_handler.generate_batches(
            str(file_path), 
            table_info.columns,
            chunk_size=recommendations.get('chunk_size', 20_000)
        )

        try:
            # Get or create connection pool once
            pool = await self._ensure_connection_pool()
            
            # Ensure table exists in database
            await self.database_service.ensure_table_exists(pool, table_info)
            
            # Find table audit for proper linking
            table_manifest_id = await asyncio.to_thread(self._find_table_audit_by_table_name, table_name)
            
            # Create file manifest if audit service available
            file_manifest_id = None
            if table_manifest_id and self.audit_service:
                file_manifest_id = await asyncio.to_thread(
                    self._create_file_manifest, 
                    str(file_path), table_name, table_manifest_id
            )

            # Process file with batch context management
            total_processed_rows = 0
            batch_num = 0
            
            try:
                async for batch_chunk in stream:
                    batch_num += 1
                    
                    # Memory check before each batch
                    if self.memory_monitor and self.memory_monitor.should_prevent_processing():
                        error_msg = f"Memory limit exceeded at batch {batch_num}"
                        logger.error(f"[LoadingService] {error_msg}")
                        
                        await asyncio.to_thread(
                            self._update_file_manifest,
                            file_manifest_id, AuditStatus.FAILED, total_processed_rows, error_msg
                        )
                        
                        return False, error_msg, total_processed_rows
                    
                    # Process batch using async DatabaseService directly
                    success, error, rows = await self._async_process_batch_with_context(
                        batch_chunk=batch_chunk,
                        pool=pool,
                        table_info=table_info,
                        table_name=table_name,
                        batch_num=batch_num,
                        file_manifest_id=file_manifest_id,
                        table_manifest_id=table_manifest_id,
                        recommendations=recommendations
                    )
                    
                    if not success:
                        await asyncio.to_thread(
                            self._update_file_manifest,
                            file_manifest_id, AuditStatus.FAILED, total_processed_rows, error
                        )
                        return False, error, total_processed_rows
                    
                    total_processed_rows += rows
                    logger.debug(f"[LoadingService] Batch {batch_num} completed: {rows} rows")
                
                # Mark file as completed
                await asyncio.to_thread(
                    self._update_file_manifest,
                    file_manifest_id, AuditStatus.COMPLETED, total_processed_rows
                )
                
                logger.info(f"[LoadingService] File processing complete: {total_processed_rows} total rows")
                return True, None, total_processed_rows
                
            except Exception as e:
                logger.error(f"[LoadingService] File processing failed: {e}")
                await asyncio.to_thread(
                    self._update_file_manifest,
                    file_manifest_id, AuditStatus.FAILED, total_processed_rows, str(e)
                )
                await stream.stop()
                return False, str(e), total_processed_rows
            
            finally:
                await stream.stop()
            
                
        except Exception as e:
            logger.error(f"[LoadingService] Async file loading failed: {e}")
            await stream.stop()
            return False, str(e), 0

    async def _async_process_batch_with_context(
        self,
        batch_chunk,
        pool,
        table_info: TableInfo,
        table_name: str,
        batch_num: int,
        file_manifest_id: Optional[str],
        table_manifest_id: Optional[str],
        recommendations: dict
    ) -> Tuple[bool, Optional[str], int]:
        """
        Process a single batch within async context.
        """
        try:
            # Use DatabaseService async connection manager
            async with self.database_service.managed_connection(pool) as conn:
                # Apply transforms if needed
                from ....database.utils import apply_transforms_to_batch
                transformed_batch = apply_transforms_to_batch(table_info, batch_chunk, table_info.columns)
                
                # Process directly with async connection
                tmp_table = f"tmp_batch_{batch_num}_{uuid.uuid4().hex[:8]}"
                headers = table_info.columns
                
                from ....database import utils as base
                types_map = base.map_types(headers, getattr(table_info, 'types', {}))
                
                await conn.execute(base.create_temp_table_sql(tmp_table, headers, types_map))
                
                try:
                    async with conn.transaction():
                        await conn.copy_records_to_table(tmp_table, records=transformed_batch, columns=headers)
                        
                        # Get primary keys safely
                        primary_keys = getattr(table_info, 'primary_keys', None)
                        if not primary_keys:
                            # Extract from model
                            from ....database.utils import extract_primary_keys
                            primary_keys = extract_primary_keys(table_info)
                        
                        if primary_keys:
                            sql = base.upsert_from_temp_sql(table_info.table_name, tmp_table, headers, primary_keys)
                            await conn.execute(sql)
                        else:
                            # Simple insert for tables without primary keys
                            insert_sql = f'INSERT INTO {base.quote_ident(table_info.table_name)} SELECT * FROM {base.quote_ident(tmp_table)}'
                            await conn.execute(insert_sql)
                    
                    return True, None, len(transformed_batch)
                    
                finally:
                    # Always cleanup temp table
                    try:
                        await conn.execute(f'DROP TABLE IF EXISTS {base.quote_ident(tmp_table)};')
                    except Exception:
                        pass
                        
        except Exception as e:
            logger.error(f"[LoadingService] Batch processing failed: {e}")
            return False, str(e), 0

    async def load_multiple_tables(self, table_to_files: Dict[str, Dict], force_csv: bool = False) -> Dict[str, Tuple[bool, Optional[str], int]]:
        """
        Load multiple tables with memory awareness and optimized processing order.
        FIX: Uses safe coroutine execution for async operations.
        """
        table_names = list(table_to_files.keys())
        
        # Memory pre-check
        if self.memory_monitor and not self._perform_memory_precheck(table_to_files):
            raise MemoryError("Insufficient memory to process all tables")
        
        # Optimize processing order
        optimized_order = self._optimize_table_processing_order(table_to_files)
        
        return await self._async_load_multiple_tables(optimized_order, table_to_files, force_csv)

    async def _async_load_multiple_tables(
        self, 
        optimized_order: List[str], 
        table_to_files: Dict[str, Dict], 
        force_csv: bool
    ) -> Dict[str, Tuple[bool, Optional[str], int]]:
        """
        Async implementation of multiple table loading.
        """
        # Create table context if available
        context_manager = self._create_table_context(optimized_order)
        
        # Ensure we have connection pool
        await self._ensure_connection_pool()
        
        # FIX: Handle both async and sync context managers
        if hasattr(context_manager, '__aenter__'):
            # Async context manager
            async with context_manager as table_manifest_id:
                return await self._process_tables_async(optimized_order, table_to_files, force_csv)
        else:
            # Sync context manager
            with context_manager as table_manifest_id:
                return await self._process_tables_async(optimized_order, table_to_files, force_csv)

    async def _process_tables_async(self, optimized_order: List[str], table_to_files: Dict[str, Dict], force_csv: bool) -> Dict[str, Tuple[bool, Optional[str], int]]:
        """Process tables within context."""
        logger.info(f"[LoadingService] Starting processing for {len(optimized_order)} tables")
        
        results = {}
        
        for table_name in optimized_order:
            if table_name not in table_to_files:
                continue
            
            logger.info(f"[LoadingService] Processing table '{table_name}'")
            
            # Memory check before each table
            if self.memory_monitor and self.memory_monitor.should_prevent_processing():
                error_msg = f"Memory limit exceeded before processing {table_name}"
                logger.error(f"[LoadingService] {error_msg}")
                results[table_name] = (False, error_msg, 0)
                continue
            
            # Process table
            table_result = await self._process_single_table(table_name, table_to_files[table_name], force_csv)
            results[table_name] = table_result
            
            # Update individual table audit completion
            await asyncio.to_thread(
                self._update_table_audit_completion, table_name, table_result
            )
            
            # Inter-table cleanup
            if self.memory_monitor and self.memory_monitor.is_memory_pressure_high():
                cleanup_stats = self.memory_monitor.perform_aggressive_cleanup()
                logger.info(f"Inter-table cleanup freed {cleanup_stats.get('freed_mb', 0):.1f}MB")
        
        return results

    async def load_data(self, audit_metadata: AuditMetadata, force_csv: bool = False) -> AuditMetadata:
        """
        Load data for all tables using the configured strategy.
        Updates audit_metadata with insertion timestamps.
        """
        table_to_files = audit_metadata.tablename_to_zipfile_to_files
        
        # Load multiple tables
        results = await self.load_multiple_tables(table_to_files, force_csv=force_csv)
        
        # Update audit_metadata with results
        for audit in audit_metadata.audit_list:
            result = results.get(audit.entity_name)
            if result and result[0]:  # success
                audit.completed_at = datetime.now()
                logger.debug(f"Set completed_at for {audit.entity_name}: success with {result[2]} rows")
            else:
                audit.completed_at = datetime.now()
                if result:
                    logger.warning(f"Completed {audit.entity_name} with issues: {result[1]}")
                else:
                    logger.warning(f"No result recorded for {audit.entity_name}")
        
        return audit_metadata

    def _perform_memory_precheck(self, table_to_files: Dict[str, Dict]) -> bool:
        """Perform memory pre-check before starting processing."""
        if not self.memory_monitor:
            return True
        
        try:
            status = self.memory_monitor.get_status_report()
            logger.info(f"[LoadingService] Pre-processing memory status: "
                       f"Usage: {status['usage_above_baseline_mb']:.1f}MB, "
                       f"Budget: {status['budget_remaining_mb']:.1f}MB")
            
            return not self.memory_monitor.should_prevent_processing()
            
        except Exception as e:
            logger.error(f"Memory pre-check failed: {e}")
            return False

    def _optimize_table_processing_order(self, table_to_files: Dict[str, Dict]) -> List[str]:
        """Optimize processing order by total file size (smallest first)."""
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
            
            logger.info(f"[LoadingService] Optimized table processing order: {optimized_order}")
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

    async def _process_single_table(self, table_name: str, zipfile_to_files: Dict, force_csv: bool) -> Tuple[bool, Optional[str], int]:
        """Process a single table with all its files."""
        table_success = True
        table_total_rows = 0
        table_errors = []
        
        for zip_filename, csv_files in zipfile_to_files.items():
            if not csv_files:
                logger.warning(f"[LoadingService] No CSV files in {zip_filename} for table '{table_name}'")
                continue
            
            success, error, rows = await self.load_table(table_name, csv_files, force_csv=force_csv)
            
            table_total_rows += rows
            
            if not success:
                table_success = False
                table_errors.append(f"{zip_filename}: {error}")
        
        if table_success:
            return True, None, table_total_rows
        else:
            error_msg = "; ".join(table_errors)
            return False, error_msg, table_total_rows

    # ... (helper methods remain the same as in previous version)
    
    def _find_table_audit_by_table_name(self, table_name: str) -> Optional[str]:
        """Find existing table audit entry ID."""
        if not self.audit_service:
            return None
        
        try:
            from sqlalchemy import text
            
            with self.audit_service.database.engine.connect() as conn:
                result = conn.execute(
                    text('''
                        SELECT table_audit_id FROM table_audit_manifest 
                        WHERE entity_name = :entity_name 
                        ORDER BY created_at DESC 
                        LIMIT 1
                    '''), {'entity_name': table_name}
                )
                
                row = result.fetchone()
                return str(row[0]) if row else None
                
        except Exception as e:
            logger.error(f"Failed to find table audit ID for {table_name}: {e}")
            return None

    def _create_file_manifest(self, file_path: str, table_name: str, table_manifest_id: str) -> Optional[str]:
        """Create file manifest entry with robust audit service handling."""
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
            
            # Create manifest entry with fallback methods
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
            
            # Try multiple audit service interfaces
            manifest_id = None
            
            if hasattr(self.audit_service, 'create_file_manifest'):
                manifest_id = self.audit_service.create_file_manifest(
                    str(file_path_obj),
                    status=AuditStatus.RUNNING,
                    table_manifest_id=table_manifest_id,
                    checksum=checksum,
                    filesize=filesize,
                    table_name=table_name,
                    notes=json.dumps(notes_data)
                )
            elif hasattr(self.audit_service, 'create_manifest'):
                manifest_id = self.audit_service.create_manifest(
                    file_path=str(file_path_obj),
                    status=AuditStatus.RUNNING,
                    checksum=checksum,
                    filesize=filesize,
                    notes=json.dumps(notes_data)
                )
            else:
                logger.warning(f"[LoadingService] Audit service has no recognized manifest creation method")
                return None
            
            return manifest_id
            
        except Exception as e:
            logger.warning(f"Failed to create file manifest for {file_path}: {e}")
            return None

    def _update_file_manifest(self, manifest_id: Optional[str], status: AuditStatus, rows_processed: int, error_msg: Optional[str] = None):
        """Update file manifest entry with robust method handling."""
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
            
            # Try multiple audit service update interfaces
            updated = False
            
            if hasattr(self.audit_service, 'update_file_manifest'):
                try:
                    self.audit_service.update_file_manifest(
                        manifest_id=manifest_id,
                        status=status,
                        rows_processed=rows_processed,
                        error_msg=error_msg,
                        notes=json.dumps(notes_data)
                    )
                    updated = True
                except Exception as e:
                    logger.debug(f"update_file_manifest failed: {e}")
            
            if not updated and hasattr(self.audit_service, 'update_manifest'):
                try:
                    self.audit_service.update_manifest(
                        manifest_id=manifest_id,
                        status=status,
                        notes=json.dumps(notes_data)
                    )
                    updated = True
                except Exception as e:
                    logger.debug(f"update_manifest failed: {e}")
            
            if not updated:
                logger.warning(f"[LoadingService] No working audit service update method found for manifest {manifest_id}")
            
        except Exception as e:
            logger.warning(f"Failed to update file manifest {manifest_id}: {e}")

    def _update_table_audit_completion(self, table_name: str, table_result: Tuple[bool, Optional[str], int]):
        """Update individual table audit completion with memory info."""
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
                conn.execute(
                    text('''
                        UPDATE table_audit_manifest 
                        SET completed_at = :completed_at,
                            notes = :metadata_json
                        WHERE entity_name = :table_name 
                        AND ingestion_year = :year 
                        AND ingestion_month = :month
                    '''
                ), {
                    'completed_at': datetime.now(),
                    'metadata_json': json.dumps(completion_metadata),
                    'table_name': table_name,
                    'year': self.config.year,
                    'month': self.config.month
                })
                
                logger.info(f"[LoadingService] Updated completion for table '{table_name}'")
                
        except Exception as e:
            logger.warning(f"[LoadingService] Failed to update completion for '{table_name}': {e}")

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

    async def close_resources(self):
        if self._connection_pool:
            try:
                loop = asyncio.get_running_loop()
                logger.info(f"[LoadingService] Closing pool on loop id={id(loop)}")
                await self._connection_pool.close()
            except Exception as e:
                logger.exception("Error closing pool")
            finally:
                self._connection_pool = None
