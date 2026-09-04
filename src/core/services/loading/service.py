"""
service.py - DuckDB-only loading service (sqldim-refactored).

All tables are loaded from Parquet via DuckDBLoader (zero-copy streaming into
PostgreSQL).  The asyncpg batch-CSV path has been removed.
"""
from typing import List, Tuple, Optional, Dict, Any
from pathlib import Path
from datetime import datetime
from contextlib import nullcontext, suppress
from dataclasses import dataclass
import json
import hashlib
import asyncio
import time

from ....setup.logging import logger
from ....setup.config.loader import ConfigLoader
from ....database.engine import Database
from ....database.models.audit import AuditStatus
from ....database.service import DatabaseService
from ...schemas import AuditMetadata

from ..memory.service import MemoryMonitor
from ..audit.service import AuditService
from .file_handler import FileHandler
from .duckdb_loader import DuckDBLoader, make_duckdb_loader
from ..observability.observatory import PipelineObservatory
from ...medallion import CNPJ_LOAD_ORDER

@dataclass
class AuditHandles:
    table_manifest_id: Optional[str] = None
    file_manifest_id: Optional[str] = None
    batch_id: Optional[str] = None


class FileLoadingService:
    """Orchestrates DuckDB-only Parquet→PostgreSQL loading for all CNPJ tables."""
    
    def __init__(
        self,
        database: Database,
        config: ConfigLoader,
        audit_service: AuditService,
    ):
        self.database = database
        self.config = config
        self.audit_service = audit_service
        self.memory_monitor = MemoryMonitor(config.pipeline.memory)

        self.file_handler = FileHandler(config)
        self.database_service = DatabaseService(database, self.memory_monitor)

        # sqldim DriftObservatory — persisted to <logs_dir>/observatory.duckdb.
        # Fail loudly rather than silently falling back to an in-memory store.
        logs_path = getattr(config.pipeline, "logs_path", None) or "logs"
        obs_path = Path(str(logs_path)) / "observatory.duckdb"
        self._observatory = PipelineObservatory.from_path(obs_path)

        # DuckDB streaming loader — the only supported Parquet→PostgreSQL path.
        # Observatory is injected so per-table SCD metrics are recorded there.
        self._duckdb_loader: Optional[DuckDBLoader] = make_duckdb_loader(
            config, observatory=self._observatory
        )

        logger.info("[LoadingService] Initialised (DuckDB-only mode)")

    def _log_cleanup_stats(self, prefix: str, cleanup_stats: Dict[str, Any], *, level: str = "info") -> None:
        """Normalize cleanup logging so rate-limit skips do not look like frees."""
        log_func = getattr(logger, level, logger.info)
        if cleanup_stats.get("skipped"):
            log_func(f"{prefix} cleanup skipped (rate limited)")
            return
        freed = cleanup_stats.get("freed_mb", 0.0)
        log_func(f"{prefix} cleanup freed {freed:.1f}MB")

    def _serialize_notes(self, notes: dict) -> str:
        """Serialize notes dict to JSON."""
        try:
            return json.dumps(notes)
        except Exception:
            try:
                return json.dumps({"error": "notes_serialization_failed"})
            except Exception:
                return "{}"

    def _find_existing_file_manifest(self, file_path_obj: Path, table_manifest_id: str) -> Optional[str]:
        """Find existing file manifest."""
        try:
            from sqlalchemy import text
            with self.audit_service.database.engine.connect() as conn:
                result = conn.execute(
                    text('''
                        SELECT file_audit_id FROM file_audit_manifest
                        WHERE file_path = :file_path AND parent_table_audit_id = :table_audit_id
                        ORDER BY created_at DESC LIMIT 1
                    '''), {'file_path': str(file_path_obj), 'table_audit_id': table_manifest_id}
                )
                row = result.fetchone()
                return str(row[0]) if row and row[0] else None
        except Exception:
            return None

    async def load_table(
        self,
        table_name: str,
        table_files: Optional[List[str]] = None,
        batch_id: Optional[str] = None,
        subbatch_id: Optional[str] = None,
    ) -> Tuple[bool, Optional[str], int]:
        """Load a table from its Parquet file via DuckDB (streaming, O(1) memory).

        Raises FileNotFoundError when the Parquet source does not exist so the
        orchestrator can surface a clear error instead of silently producing
        zero rows.
        """
        logger.info(f"[LoadingService] Loading table '{table_name}'")

        if self.memory_monitor.should_prevent_processing():
            error_msg = "Insufficient memory to process table"
            logger.error(f"[LoadingService] {error_msg}")
            return False, error_msg, 0

        try:
            parquet_file = self.config.pipeline.get_temporal_conversion_path(
                self.config.year, self.config.month
            ) / f"{table_name}.parquet"
        except Exception as exc:
            raise FileNotFoundError(
                f"Could not resolve Parquet path for '{table_name}': {exc}"
            ) from exc

        if not parquet_file.exists():
            raise FileNotFoundError(
                f"Parquet file not found for '{table_name}': {parquet_file}"
            )

        if self._duckdb_loader is None:
            raise RuntimeError(
                f"DuckDB loader not initialised — cannot load '{table_name}'. "
                "Set loading.use_duckdb = true in config."
            )

        batch_date = (
            f"{self.config.year}-{self.config.month:02d}-01"
            if hasattr(self.config, "year") and hasattr(self.config, "month")
            else None
        )
        return self._duckdb_loader.load(
            parquet_path=parquet_file,
            table_name=table_name,
            batch_date=batch_date,
        )

    def _apply_development_filtering(self, table_files: Optional[List[str]], table_name: str) -> Optional[List[Path]]:
        """Apply development filtering to files."""
        if not table_files:
            return None
            
        from ....core.utils.development_filter import DevelopmentFilter
        dev_filter = DevelopmentFilter(self.config.pipeline.development)
        
        # Resolve bare filenames against the extraction path
        try:
            base_path = self.config.pipeline.get_temporal_extraction_path(
                self.config.year, self.config.month
            )
        except Exception:
            base_path = Path(".")
        
        def _resolve(f: str) -> Path:
            p = Path(f)
            if p.is_absolute() or p.exists():
                return p
            candidate = base_path / p.name
            return candidate if candidate.exists() else p

        file_paths = [_resolve(f) for f in table_files]
        try:
            filtered_paths = dev_filter.filter_files_by_blob_limit(file_paths, table_name)
        except (OSError, FileNotFoundError):
            # File may not exist at the unresolved path (e.g. parquet loaded from conversion dir)
            filtered_paths = file_paths
        
        if not filtered_paths:
            logger.info(f"[LoadingService] No files after filtering for '{table_name}'")
        
        return filtered_paths


    async def _resolve_table_manifest_id(self, table_name: str) -> Optional[str]:
        """Find the current table manifest id without blocking the event loop."""
        if not self.audit_service:
            return None
        try:
            return await asyncio.wait_for(
                asyncio.to_thread(self._find_table_audit_by_table_name, table_name),
                timeout=2.0
            )
        except asyncio.TimeoutError:
            logger.warning(f"[LoadingService] Table audit lookup timed out for {table_name}")
        except Exception as e:
            logger.warning(f"[LoadingService] Failed to resolve table audit for {table_name}: {e}")
        return None

    async def _initialize_file_audit(
        self,
        table_name: str,
        file_path: Path,
        table_manifest_id: Optional[str]
    ) -> AuditHandles:
        """Create file manifest and start batch tracking for a file."""
        handles = AuditHandles(table_manifest_id=table_manifest_id)
        if not self.audit_service:
            return handles

        if not handles.table_manifest_id:
            handles.table_manifest_id = await self._resolve_table_manifest_id(table_name)
            if not handles.table_manifest_id:
                return handles

        pipeline_config = getattr(self.config, "pipeline", None)
        audit_config = getattr(pipeline_config, "audit", None)
        initial_timeout = getattr(audit_config, "file_manifest_initial_timeout", 20.0)
        extended_timeout = getattr(audit_config, "file_manifest_extended_timeout", 0.0)
        defer_manifest_checksum = getattr(audit_config, "defer_manifest_checksum", True)

        manifest_task = asyncio.create_task(
            asyncio.to_thread(
                self._create_file_manifest,
                str(file_path),
                table_name,
                handles.table_manifest_id,
                defer_manifest_checksum
            )
        )

        file_manifest_id: Optional[str] = None

        try:
            try:
                file_manifest_id = await asyncio.wait_for(
                    asyncio.shield(manifest_task),
                    timeout=initial_timeout
                )
            except asyncio.TimeoutError:
                logger.info(
                    f"[LoadingService] File manifest for {file_path.name} taking longer than {initial_timeout:.1f}s"
                )

                if extended_timeout and extended_timeout > 0:
                    try:
                        file_manifest_id = await asyncio.wait_for(
                            asyncio.shield(manifest_task),
                            timeout=extended_timeout
                        )
                    except asyncio.TimeoutError:
                        logger.error(
                            f"[LoadingService] File manifest still pending after {initial_timeout + extended_timeout:.1f}s for {file_path.name}"
                        )
                        return handles
                else:
                    file_manifest_id = await asyncio.shield(manifest_task)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(f"[LoadingService] Failed to create file manifest for {file_path.name}: {e}")
                return handles
        finally:
            if not manifest_task.done():
                manifest_task.cancel()
                with suppress(Exception):
                    await manifest_task

        if not file_manifest_id:
            logger.warning(f"[LoadingService] File manifest unavailable for {file_path.name}")
            return handles

        handles.file_manifest_id = file_manifest_id

        if defer_manifest_checksum and file_manifest_id:
            asyncio.create_task(
                self._finalize_manifest_metadata(
                    file_manifest_id=file_manifest_id,
                    file_path=str(file_path)
                )
            )

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]
        batch_name = f"FileLoad_{file_path.name}_{timestamp}"
        batch_task = asyncio.create_task(
            asyncio.to_thread(
                self.audit_service._start_batch,
                table_name,
                batch_name,
                file_manifest_id
            )
        )

        try:
            handles.batch_id = await asyncio.wait_for(
                asyncio.shield(batch_task),
                timeout=15.0
            )
        except asyncio.TimeoutError:
            logger.warning(
                f"[LoadingService] Batch start timed out for {file_path.name}; waiting for completion"
            )
            try:
                handles.batch_id = await asyncio.wait_for(
                    asyncio.shield(batch_task),
                    timeout=30.0
                )
            except asyncio.TimeoutError:
                logger.warning(f"[LoadingService] Batch start still pending for {file_path.name}")
                batch_task.cancel()
                with suppress(Exception):
                    await batch_task
            except Exception as e:
                logger.warning(f"[LoadingService] Failed to complete batch start for {file_path.name}: {e}")
        except Exception as e:
            logger.warning(f"[LoadingService] Failed to start batch for {file_path.name}: {e}")

        logger.info(
            f"[LoadingService] Audit initialized for {file_path.name} "
            f"(file_manifest={handles.file_manifest_id}, batch={handles.batch_id})"
        )
        return handles

    async def _finalize_manifest_metadata(self, file_manifest_id: str, file_path: str) -> None:
        """Populate deferred manifest metadata (checksum, size) asynchronously."""
        try:
            checksum, filesize = await asyncio.to_thread(
                self._compute_manifest_metadata,
                Path(file_path)
            )

            if checksum is None and filesize is None:
                return

            notes_payload = {
                "file_info": {
                    "size_bytes": filesize
                },
                "processing": {
                    "checksum_deferred": False,
                    "checksum_algorithm": "md5" if checksum else None,
                    "checksum_enriched_at": datetime.now().isoformat()
                }
            }

            await asyncio.to_thread(
                self.audit_service.update_file_manifest_metadata,
                file_manifest_id,
                checksum,
                filesize,
                self._serialize_notes(notes_payload)
            )
            logger.debug(
                f"[LoadingService] Manifest metadata enriched for {Path(file_path).name}"
            )
        except FileNotFoundError:
            logger.debug(
                f"[LoadingService] Manifest metadata enrichment skipped; file missing: {file_path}"
            )
        except Exception as exc:
            logger.warning(
                f"[LoadingService] Manifest metadata enrichment failed for {Path(file_path).name}: {exc}"
            )

    def _compute_manifest_metadata(self, file_path: Path) -> tuple[Optional[str], Optional[int]]:
        if not file_path.exists():
            return None, None

        filesize = None
        checksum = None

        try:
            stat_result = file_path.stat()
            filesize = stat_result.st_size
        except OSError:
            filesize = None

        try:
            checksum = self._calculate_file_checksum(file_path)
        except Exception:
            checksum = None

        return checksum, filesize

    async def _record_subbatch_completion(
        self,
        batch_id,
        table_name: str,
        batch_num: int,
        rows_processed: int
    ) -> None:
        """Record successful subbatch metrics without blocking the event loop."""
        if not self.audit_service:
            return
        subbatch_name = f"Subbatch_{batch_num:05d}_rows{rows_processed}"

        def _sync_record_success():
            try:
                subbatch_id = self.audit_service._start_subbatch(
                    batch_id,
                    table_name,
                    subbatch_name
                )
            except Exception as start_err:
                logger.debug(f"[LoadingService] Subbatch start failed for batch {batch_num}: {start_err}")
                return

            try:
                self.audit_service.collect_file_processing_event(
                    subbatch_id,
                    AuditStatus.COMPLETED,
                    int(rows_processed or 0),
                    0
                )
            except Exception as metrics_err:
                logger.debug(f"[LoadingService] Subbatch metrics failed for batch {batch_num}: {metrics_err}")

            try:
                self.audit_service._complete_subbatch_with_accumulated_metrics(
                    subbatch_id,
                    AuditStatus.COMPLETED
                )
            except Exception as complete_err:
                logger.debug(f"[LoadingService] Subbatch completion failed for batch {batch_num}: {complete_err}")

        try:
            await asyncio.wait_for(
                asyncio.to_thread(_sync_record_success),
                timeout=4.0
            )
        except asyncio.TimeoutError:
            logger.debug(f"[LoadingService] Subbatch record timed out for batch {batch_num}")
        except Exception as e:
            logger.debug(f"[LoadingService] Subbatch record scheduling failed for batch {batch_num}: {e}")

    async def _record_failed_subbatch(
        self,
        batch_id,
        table_name: str,
        batch_num: int,
        error_message: str
    ) -> None:
        """Record failed subbatch metrics when a batch errors."""
        if not self.audit_service:
            return
        subbatch_name = f"failed_batch_{batch_num}"

        def _sync_record_failure():
            try:
                subbatch_id = self.audit_service._start_subbatch(
                    batch_id,
                    table_name,
                    subbatch_name
                )
            except Exception as start_err:
                logger.debug(
                    f"[LoadingService] Failed subbatch start skipped for batch {batch_num}: {start_err}"
                )
                return

            try:
                self.audit_service.collect_file_processing_event(
                    subbatch_id,
                    AuditStatus.FAILED,
                    0,
                    0
                )
            except Exception as metrics_err:
                logger.debug(
                    f"[LoadingService] Failed subbatch metrics skipped for batch {batch_num}: {metrics_err}"
                )

            try:
                self.audit_service._complete_subbatch_with_accumulated_metrics(
                    subbatch_id,
                    AuditStatus.FAILED,
                    error_message
                )
            except Exception as complete_err:
                logger.debug(
                    f"[LoadingService] Failed subbatch completion skipped for batch {batch_num}: {complete_err}"
                )

        try:
            await asyncio.wait_for(
                asyncio.to_thread(_sync_record_failure),
                timeout=4.0
            )
        except asyncio.TimeoutError:
            logger.debug(f"[LoadingService] Failed subbatch record timed out for batch {batch_num}")
        except Exception as e:
            logger.debug(f"[LoadingService] Failed subbatch scheduling failed for batch {batch_num}: {e}")

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

    def _create_file_manifest(
        self,
        file_path: str,
        table_name: str,
        table_manifest_id: str,
        defer_checksum: bool = False
    ) -> Optional[str]:
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
                if not defer_checksum:
                    checksum = self._calculate_file_checksum(file_path_obj)

            notes_data = {
                "file_info": {
                    "size_bytes": filesize,
                    "format": file_path_obj.suffix.lstrip('.') if file_path_obj.suffix else "unknown"
                },
                "processing": {
                    "status": AuditStatus.RUNNING.value,
                    "table_name": table_name,
                    "checksum_deferred": defer_checksum
                }
            }

            # Check for existing manifest
            existing = self._find_existing_file_manifest(file_path_obj, table_manifest_id)
            if existing:
                return existing

            # Create new manifest
            if hasattr(self.audit_service, 'create_file_manifest'):
                return self.audit_service.create_file_manifest(
                    str(file_path_obj),
                    status=AuditStatus.RUNNING,
                    table_manifest_id=table_manifest_id,
                    checksum=checksum,
                    filesize=filesize,
                    table_name=table_name,
                    notes=self._serialize_notes(notes_data)
                )

            if hasattr(self.audit_service, 'create_manifest'):
                return self.audit_service.create_manifest(
                    file_path=str(file_path_obj),
                    status=AuditStatus.RUNNING,
                    checksum=checksum,
                    filesize=filesize,
                    notes=self._serialize_notes(notes_data)
                )

            logger.warning("[LoadingService] No manifest creation method available")
            return None
            
        except Exception as e:
            logger.warning(f"Failed to create file manifest for {file_path}: {e}")
            return None

    def _update_file_manifest(self, file_manifest_id: Optional[str], status: AuditStatus, rows_processed: int, error_msg: Optional[str] = None):
        """Update file manifest entry."""
        if not file_manifest_id or not self.audit_service:
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

            try:
                self.audit_service.update_file_manifest(
                    file_manifest_id=file_manifest_id,
                    status=status,
                    rows_processed=rows_processed,
                    error_msg=error_msg,
                    notes=self._serialize_notes(notes_data)
                )
            except Exception as e:
                logger.debug(f"update_file_manifest failed for {file_manifest_id}: {e}")
            
        except Exception as e:
            logger.warning(f"Failed to update file manifest {file_manifest_id}: {e}")

    def _calculate_file_checksum(self, file_path: Path) -> Optional[str]:
        """Calculate MD5 checksum of file."""
        try:
            hash_md5 = hashlib.md5()
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_md5.update(chunk)
            return hash_md5.hexdigest()
        except Exception as e:
            logger.warning(f"Failed to calculate checksum for {file_path}: {e}")
            return None

    async def shutdown(self):
        """Shutdown service and cleanup resources."""
        logger.info("[LoadingService] Shutting down")
        try:
            if self.file_handler:
                await self.file_handler.shutdown_all(timeout=10.0)
        except Exception as e:
            logger.error(f"[LoadingService] File handler shutdown failed: {e}")
        logger.info("[LoadingService] Shutdown complete")

    async def load_data(self, audit_metadata: AuditMetadata) -> AuditMetadata:
        """
        Load data for all tables using DuckDB streaming.
        Updates audit_metadata with insertion timestamps.
        """
        table_to_files = audit_metadata.tablename_to_zipfile_to_files

        results = await self.load_multiple_tables(table_to_files)

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

    async def load_multiple_tables(self, table_to_files: Dict[str, Dict]) -> Dict[str, Tuple[bool, Optional[str], int]]:
        """
        Load multiple tables with memory awareness and optimized processing order.
        """
        if not self._perform_memory_precheck(table_to_files):
            raise MemoryError("Insufficient memory to process all tables")

        optimized_order = self._determine_load_order(table_to_files)
        return await self._async_load_multiple_tables(optimized_order, table_to_files)

    async def _async_load_multiple_tables(
        self,
        optimized_order: List[str],
        table_to_files: Dict[str, Dict],
    ) -> Dict[str, Tuple[bool, Optional[str], int]]:
        """
        Async implementation of multiple table loading.
        """
        context_manager = self._create_table_context(optimized_order)

        # Enter and exit audit table context to set started_at/status
        try:
            if hasattr(context_manager, '__aenter__'):
                try:
                    mapping = await context_manager.__aenter__()
                finally:
                    try:
                        await context_manager.__aexit__(None, None, None)
                    except Exception:
                        logger.debug("Failed to exit async table_context cleanly")
            else:
                try:
                    mapping = context_manager.__enter__()
                finally:
                    try:
                        context_manager.__exit__(None, None, None)
                    except Exception:
                        logger.debug("Failed to exit sync table_context cleanly")
        except Exception as e:
            logger.debug(f"Failed to initialize table context: {e}")

        return await self._process_tables_async(optimized_order, table_to_files)

    async def _process_tables_async(self, optimized_order: List[str], table_to_files: Dict[str, Dict]) -> Dict[str, Tuple[bool, Optional[str], int]]:
        """Process tables within context."""
        logger.info(f"[LoadingService] Starting processing for {len(optimized_order)} tables")
        
        results = {}
        
        for table_name in optimized_order:
            if table_name not in table_to_files:
                continue
            
            logger.info(f"[LoadingService] Processing table '{table_name}'")
            
            # Memory check before each table
            if self.memory_monitor.should_prevent_processing():
                error_msg = f"Memory limit exceeded before processing {table_name}"
                logger.error(f"[LoadingService] {error_msg}")
                results[table_name] = (False, error_msg, 0)
                continue
            
            # Process table
            table_result = await self._process_single_table(table_name, table_to_files[table_name])
            results[table_name] = table_result
            
            logger.info(f"[LoadingService] Table '{table_name}' completed: {table_result[2]:,} rows")
            
            # Update individual table audit completion (with timeout)
            try:
                await asyncio.wait_for(
                    asyncio.to_thread(
                        self._update_table_audit_completion, table_name, table_result
                    ),
                    timeout=5.0
                )
            except asyncio.TimeoutError:
                logger.warning(f"[LoadingService] Table audit update timed out for '{table_name}'")
            except Exception as e:
                logger.warning(f"[LoadingService] Table audit update failed for '{table_name}': {e}")
            
            # Inter-table cleanup
            if self.memory_monitor.is_memory_pressure_high():
                cleanup_stats = self.memory_monitor.perform_aggressive_cleanup()
                self._log_cleanup_stats("Inter-table", cleanup_stats)
        
        return results

    async def _process_single_table(self, table_name: str, zipfile_to_files: Dict) -> Tuple[bool, Optional[str], int]:
        """Process a single table."""
        # Flatten files from all zipfiles
        all_files = []
        for csv_files in zipfile_to_files.values():
            all_files.extend(csv_files)

        return await self.load_table(
            table_name=table_name,
            table_files=all_files,
        )

    def _perform_memory_precheck(self, table_to_files: Dict[str, Dict]) -> bool:
        """Perform memory pre-check before starting processing."""
        if not self.memory_monitor:
            return True
        
        try:
            status = self.memory_monitor.get_status_report()
            logger.info(f"[LoadingService] Pre-processing memory status: "
                       f"Usage: {status['usage_above_baseline_mb']:.1f}MB, "
                       f"Budget: {status['budget_remaining_mb']:.1f}MB")
            
            # In development mode with low memory systems, allow processing to start
            if hasattr(self.config, 'is_development_mode') and self.config.is_development_mode():
                logger.info("[LoadingService] Development mode: Skipping aggressive pre-check")
                return True
            
            return not self.memory_monitor.should_prevent_processing()
            
        except Exception as e:
            logger.error(f"Memory pre-check failed: {e}")
            return False

    def _determine_load_order(self, table_to_files: Dict[str, Dict]) -> List[str]:
        """Return tables in the canonical CNPJ dependency order.

        Tables present in CNPJ_LOAD_ORDER are emitted in that fixed order.
        Any remaining tables not in the canonical list are appended at the end
        sorted by file size (smallest first) so unfamiliar tables still load
        in a reasonable order.
        """
        known = [t for t in CNPJ_LOAD_ORDER if t in table_to_files]
        unknown = [
            t for t in table_to_files
            if t not in set(CNPJ_LOAD_ORDER)
        ]
        if unknown:
            unknown = self._optimize_table_processing_order({t: table_to_files[t] for t in unknown})
            logger.info(
                f"[LoadingService] Unknown tables appended at end: {unknown}"
            )
        order = known + unknown
        logger.info(f"[LoadingService] Load order: {order}")
        return order

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
        """Create table context manager if audit service supports it."""
        if not self.audit_service or not hasattr(self.audit_service, 'table_context'):
            return nullcontext()
        
        try:
            return self.audit_service.table_context(table_names)
        except Exception as e:
            logger.debug(f"Failed to create table context: {e}")
            return nullcontext()

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
                        WHERE entity_name = :entity_name
                          AND completed_at IS NULL
                    '''),
                    {
                        'completed_at': datetime.now(),
                        'metadata_json': json.dumps(completion_metadata),
                        'entity_name': table_name
                    }
                )
                
        except Exception as e:
            logger.warning(f"Failed to update table audit completion for {table_name}: {e}")

    async def close_resources(self):
        """Release any stateful resources held by the service."""
        logger.debug("[LoadingService] close_resources called (no-op in DuckDB-only mode)")