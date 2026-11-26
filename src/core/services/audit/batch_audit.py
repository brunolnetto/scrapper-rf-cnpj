"""Batch and subbatch audits with in-memory metric accumulators and watchdogs.

Mixin of :class:`AuditService`; relies on attributes set in its ``__init__``.
"""

import time
import uuid
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Dict, Generator, Optional

from sqlalchemy import text

from ....database.models.audit import AuditStatus
from ....setup.logging import logger
from .accumulator import BatchAccumulator


class BatchAuditMixin:
    """Batch and subbatch audits with in-memory metric accumulators and watchdogs."""

    def _handle_batch_timeout(self, accumulator: BatchAccumulator):
        """Handle batch timeout/watchdog trigger."""
        metrics = accumulator.get_metrics_snapshot()
        logger.error(
            f"Batch operation timeout - elapsed: {metrics['elapsed_seconds']:.1f}s, "
            f"idle: {metrics['idle_seconds']:.1f}s, completed: {metrics['files_completed']}, "
            f"failed: {metrics['files_failed']}"
        )
        accumulator.cancel()

    @contextmanager
    def batch_context(
        self, target_table: str, batch_name: str, 
        year: Optional[int] = None, month: Optional[int] = None,
        file_manifest_id: Optional[str] = None
    ) -> Generator[uuid.UUID, None, None]:
        """
        Enhanced batch context with temporal tracking and structured logging.
        
        Args:
            target_table: Single table name to process
            batch_name: Human-readable batch description
            year: Optional year (uses config if not provided)
            month: Optional month (uses config if not provided)
            
        Yields:
            batch_id: UUID for the created batch
        """
        # Use config's temporal context if enabled and not provided
        year = year or self.config.year
        month = month or self.config.month
        batch_name_with_context = f"{batch_name} ({year}-{month:02d})"
        
        batch_id = self._start_batch(
            target_table, batch_name_with_context, 
            file_manifest_id=file_manifest_id
        )
        
        try:
            logger.info(f"Starting batch processing for {target_table}", 
                       extra={"batch_id": str(batch_id), "table_name": target_table, 
                             "year": year, "month": month, "batch_name": batch_name})
            yield batch_id
            
            self._complete_batch_with_accumulated_metrics(batch_id, AuditStatus.COMPLETED)
            logger.info("Batch processing completed successfully", 
                       extra={"batch_id": str(batch_id), "table_name": target_table})
            
        except (OSError, IOError, ConnectionError, ValueError, RuntimeError) as e:
            logger.error(f"Batch processing failed for {target_table}: {e}", 
                        extra={"batch_id": str(batch_id), "table_name": target_table, 
                              "error": str(e)})
            self._complete_batch_with_accumulated_metrics(batch_id, AuditStatus.FAILED, str(e))
            raise
        finally:
            self._cleanup_batch_context(batch_id)

    @contextmanager  
    def subbatch_context(
        self, batch_id: uuid.UUID, table_name: str, 
        description: Optional[str] = None
    ) -> Generator[uuid.UUID, None, None]:
        """
        Enhanced subbatch context with structured logging.
        
        Args:
            batch_id: Parent batch ID
            table_name: Name of the table being processed
            description: Optional description for the subbatch
            
        Yields:
            subbatch_id: UUID for the created subbatch
        """
        subbatch_id = self._start_subbatch(batch_id, table_name, description)
        
        try:
            logger.info("Starting subbatch processing", 
                       extra={"subbatch_id": str(subbatch_id), "batch_id": str(batch_id), 
                             "table_name": table_name, "description": description})
            yield subbatch_id
            
            self._complete_subbatch_with_accumulated_metrics(subbatch_id, AuditStatus.COMPLETED)
            logger.info("Subbatch processing completed", 
                       extra={"subbatch_id": str(subbatch_id), "table_name": table_name})
            
        except (OSError, IOError, ConnectionError, ValueError, RuntimeError) as e:
            logger.error(f"Subbatch processing failed: {e}", 
                        extra={"subbatch_id": str(subbatch_id), "batch_id": str(batch_id), 
                              "table_name": table_name, "error": str(e)})
            self._complete_subbatch_with_accumulated_metrics(subbatch_id, AuditStatus.FAILED, str(e))
            raise
        finally:
            self._cleanup_subbatch_context(subbatch_id)

    def _cleanup_batch_context(self, batch_id: uuid.UUID) -> None:
        """Clean up batch context after completion (and stop its watchdog)."""
        with self._metrics_lock:
            accumulator = self._active_batches.pop(str(batch_id), None)
        if accumulator is not None:
            accumulator.cancel()

    def _cleanup_subbatch_context(self, subbatch_id: uuid.UUID) -> None:
        """Clean up subbatch context after completion (and stop its watchdog)."""
        with self._metrics_lock:
            accumulator = self._active_subbatches.pop(str(subbatch_id), None)
            self._subbatch_to_batch.pop(str(subbatch_id), None)
        if accumulator is not None:
            accumulator.cancel()

    def collect_file_processing_event(self, subbatch_id: str, status: AuditStatus, rows: int = 0, bytes_: int = 0) -> None:
        """
        Collect file processing events for batch metrics (in-memory accumulation).
        No database operations - just accumulates metrics for later batch updates.
        
        Args:
            subbatch_id: The subbatch ID to update
            status: File processing status
            rows: Number of rows processed
            bytes_: Number of bytes processed
        """
        try:
            with self._metrics_lock:
                # Update subbatch accumulator
                subbatch_id_str = str(subbatch_id)
                if subbatch_id_str not in self._active_subbatches:
                    return  # Subbatch not being tracked
                
                accumulator = self._active_subbatches[subbatch_id_str]
                accumulator.add_file_event(status, rows, bytes_)
                
                # Update parent batch accumulator
                batch_id = self._subbatch_to_batch.get(subbatch_id_str)
                if batch_id and batch_id in self._active_batches:
                    batch_accumulator = self._active_batches[batch_id]
                    batch_accumulator.add_file_event(status, rows, bytes_)
                        
        except Exception as e:
            logger.debug(f"Failed to collect file event: {e}")  # Non-critical

    def _start_batch(
        self, target_table: str, batch_name: str, 
        file_manifest_id: Optional[str] = None
    ) -> uuid.UUID:
        """Start a new batch for a single target table and return its ID with timeout support."""
        try:
            batch_id = uuid.uuid4()
            batch_data = {
                'batch_id': str(batch_id),
                'parent_file_audit_id': file_manifest_id if file_manifest_id else None,
                'batch_name': batch_name,
                'target_table': target_table,
                'status': AuditStatus.RUNNING.value,
                'created_at': datetime.now(),
                'started_at': datetime.now(),
                'description': f"Processing table: {target_table}"
            }

            with self.database.engine.begin() as conn:
                conn.execute(text('''
                    INSERT INTO batch_audit_manifest
                    (batch_audit_id, parent_file_audit_id, target_table, status, created_at, started_at, description)
                    VALUES (:batch_id, :parent_file_audit_id, :target_table, :status, :created_at, :started_at, :description)
                '''), batch_data)

            # Start in-memory tracking with timeout configuration
            with self._metrics_lock:
                accumulator = BatchAccumulator(timeout_config=self.timeout_config)
                self._active_batches[str(batch_id)] = accumulator
                
                # Start watchdog for this batch
                accumulator.start_watchdog(self._handle_batch_timeout)

            logger.info(f"Started batch {batch_id} for table: {target_table} with timeout config: "
                       f"operation={self.timeout_config.operation_timeout_seconds}s, "
                       f"idle={self.timeout_config.max_idle_seconds}s")
            return batch_id

        except Exception as e:
            logger.error(f"Failed to start batch: {e}")
            raise

    def _start_subbatch(self, batch_id: uuid.UUID, table_name: str, description: str = None) -> uuid.UUID:
        """Start a new subbatch and return its ID with timeout support."""
        try:
            subbatch_id = uuid.uuid4()
            subbatch_data = {
                'subbatch_audit_id': str(subbatch_id),
                'batch_audit_id': str(batch_id),
                'processing_step': f"upsert_{table_name}",  # Descriptive processing step
                'table_name': table_name,
                'status': AuditStatus.RUNNING.value,
                'created_at': datetime.now(),
                'started_at': datetime.now(),
                'description': description or f"Processing table: {table_name}"
            }

            with self.database.engine.begin() as conn:
                conn.execute(text('''
                    INSERT INTO subbatch_audit_manifest
                    (
                        subbatch_audit_id, 
                        parent_batch_audit_id, 
                        processing_step, 
                        table_name, 
                        status, 
                        created_at, 
                        started_at, 
                        description
                    )
                    VALUES (
                        :subbatch_audit_id, 
                        :batch_audit_id, 
                        :processing_step, 
                        :table_name, 
                        :status, 
                        :created_at, 
                        :started_at, 
                        :description
                    )
                '''), subbatch_data)

            # Start in-memory tracking with timeout configuration
            with self._metrics_lock:
                accumulator = BatchAccumulator(timeout_config=self.timeout_config)
                self._active_subbatches[str(subbatch_id)] = accumulator
                self._subbatch_to_batch[str(subbatch_id)] = str(batch_id)
                
                # Start watchdog for this subbatch
                accumulator.start_watchdog(self._handle_batch_timeout)

            logger.info(f"Started subbatch {subbatch_id} for table: {table_name}")
            return subbatch_id

        except Exception as e:
            logger.error(f"Failed to start subbatch: {e}")
            raise

    def _complete_batch_with_accumulated_metrics(self, batch_id: uuid.UUID, status: AuditStatus, 
                                               error_message: str = None) -> None:
        """Complete batch with accumulated metrics - single DB update."""
        completed_at = datetime.now()
        completion_perf = time.perf_counter()
        
        # Get accumulated metrics
        with self._metrics_lock:
            accumulator = self._active_batches.get(str(batch_id), BatchAccumulator())
            duration = max(completion_perf - accumulator.start_time, 0.0)
            
            # Build comprehensive batch statistics
            batch_stats = {
                "start_time": accumulator.start_time,
                "end_time": accumulator.start_time + duration,
                "files_processed": accumulator.files_completed,
                "files_failed": accumulator.files_failed,
                "total_rows": accumulator.total_rows,
                "total_bytes": accumulator.total_bytes,
                "memory_peak_mb": getattr(accumulator, 'memory_peak_mb', 0),
                "cpu_peak_percent": getattr(accumulator, 'cpu_peak_percent', 0),
                "data_quality_issues": getattr(accumulator, 'data_quality_issues', 0),
                "avg_completeness": getattr(accumulator, 'avg_completeness', 100),
                "duplicates_found": getattr(accumulator, 'duplicates_found', 0),
                "io_operations": getattr(accumulator, 'io_operations', 0),
                "network_bytes": getattr(accumulator, 'network_bytes', 0),
                "temp_storage_mb": getattr(accumulator, 'temp_storage_mb', 0)
            }
            
            # Cleanup: stop the watchdog, otherwise its timer reschedules forever
            accumulator.cancel()
            self._active_batches.pop(str(batch_id), None)
        
        # Collect comprehensive batch metrics
        try:
            comprehensive_batch_metrics = self.collect_batch_audit_metrics(batch_stats)
            
            # Update database with comprehensive metrics
            self.update_batch_audit_with_metrics(str(batch_id), comprehensive_batch_metrics)
            
        except Exception as metrics_error:
            logger.warning(f"Failed to collect comprehensive batch metrics: {metrics_error}")
        
        # Single database update with basic completion status
        try:
            update_data = {
                'batch_id': str(batch_id),
                'status': status.value,
                'completed_at': completed_at,
                'error_message': error_message,
                'metrics_summary': f"Files: {accumulator.files_completed + accumulator.files_failed}, Rows: {accumulator.total_rows:,}, Duration: {duration:.3f}s"
            }

            with self.database.engine.begin() as conn:
                conn.execute(text('''
                    UPDATE batch_audit_manifest
                    SET 
                        status = :status, 
                        completed_at = :completed_at, 
                        error_message = :error_message,
                        description = description || ' | ' || :metrics_summary
                    WHERE batch_audit_id = :batch_id
                '''), update_data)

            logger.info(f"Completed batch {batch_id} with status: {status.value} in {duration:.3f}s")

        except Exception as e:
            logger.error(f"Failed to complete batch {batch_id}: {e}")

    def _complete_subbatch_with_accumulated_metrics(self, subbatch_id: uuid.UUID, status: AuditStatus, 
                                                  error_message: str = None) -> None:
        """Complete subbatch with accumulated metrics - single DB update."""
        completed_at = datetime.now()
        completion_perf = time.perf_counter()
        
        # Get accumulated metrics
        with self._metrics_lock:
            accumulator = self._active_subbatches.get(str(subbatch_id), BatchAccumulator())
            duration = max(completion_perf - accumulator.start_time, 0.0)
            
            # Build comprehensive subbatch statistics
            subbatch_stats = {
                "start_time": accumulator.start_time,
                "end_time": accumulator.start_time + duration,
                "rows_processed": accumulator.total_rows,
                "table_name": getattr(accumulator, 'table_name', 'unknown'),
                "processing_stage": getattr(accumulator, 'processing_stage', 'data_loading'),
                "operation_type": getattr(accumulator, 'operation_type', 'upsert'),
                "rows_inserted": getattr(accumulator, 'rows_inserted', 0),
                "rows_updated": getattr(accumulator, 'rows_updated', 0),
                "rows_skipped": getattr(accumulator, 'rows_skipped', 0),
                "rows_failed": getattr(accumulator, 'rows_failed', 0),
                "duplicates_handled": getattr(accumulator, 'duplicates_handled', 0),
                "constraint_violations": getattr(accumulator, 'constraint_violations', 0),
                "null_values": getattr(accumulator, 'null_values', 0),
                "type_conversions": getattr(accumulator, 'type_conversions', 0),
                "validation_errors": getattr(accumulator, 'validation_errors', 0),
                "completeness_percent": getattr(accumulator, 'completeness_percent', 100),
                "db_connection_time_ms": getattr(accumulator, 'db_connection_time_ms', 0),
                "query_execution_time_ms": getattr(accumulator, 'query_execution_time_ms', 0),
                "transformation_time_ms": getattr(accumulator, 'transformation_time_ms', 0),
                "warning_count": getattr(accumulator, 'warning_count', 0),
                "retry_attempts": getattr(accumulator, 'retry_attempts', 0),
                "recovery_actions": getattr(accumulator, 'recovery_actions', [])
            }
            
            # Cleanup: stop the watchdog, otherwise its timer reschedules forever
            accumulator.cancel()
            self._active_subbatches.pop(str(subbatch_id), None)
            self._subbatch_to_batch.pop(str(subbatch_id), None)
        
        # Collect comprehensive subbatch metrics
        try:
            comprehensive_subbatch_metrics = self.collect_subbatch_audit_metrics(subbatch_stats)
            
            # Update database with comprehensive metrics
            self.update_subbatch_audit_with_metrics(str(subbatch_id), comprehensive_subbatch_metrics)
            
        except Exception as metrics_error:
            logger.warning(f"Failed to collect comprehensive subbatch metrics: {metrics_error}")
        
        # Single database update with basic completion status
        try:
            update_data = {
                'subbatch_id': str(subbatch_id),
                'status': status.value,
                'completed_at': completed_at,
                'error_message': error_message,
                'rows_processed': accumulator.total_rows
            }

            with self.database.engine.begin() as conn:
                conn.execute(text('''
                    UPDATE subbatch_audit_manifest
                    SET status = :status, 
                        completed_at = :completed_at, 
                        error_message = :error_message,
                        rows_processed = :rows_processed
                    WHERE subbatch_audit_id = :subbatch_id
                '''), update_data)

            logger.info(f"Completed subbatch {subbatch_id} with status: {status.value} in {duration:.3f}s")

        except Exception as e:
            logger.error(f"Failed to complete subbatch {subbatch_id}: {e}")

    def update_subbatch_metrics(self, subbatch_id: uuid.UUID, rows_processed: int = None, 
                               notes: str = None) -> None:
        """Update metrics for a subbatch (files_processed removed since subbatches always handle 1 file)."""
        try:
            update_data = {
                'subbatch_id': str(subbatch_id),
                'rows_processed': rows_processed,
                'notes': notes
            }

            # Filter out None values
            update_data = {k: v for k, v in update_data.items() if v is not None}

            if len(update_data) > 1:  # More than just subbatch_id
                set_clause = ', '.join([f"{k} = :{k}" for k in update_data.keys() if k != 'subbatch_id'])
                
                with self.database.engine.begin() as conn:
                    conn.execute(text(f'''
                        UPDATE subbatch_audit_manifest
                        SET {set_clause}
                        WHERE subbatch_audit_id = :subbatch_id
                    '''), update_data)

                logger.debug(f"Updated metrics for subbatch {subbatch_id}")

        except Exception as e:
            logger.error(f"Failed to update subbatch metrics {subbatch_id}: {e}")

    def get_batch_status(self, batch_id: uuid.UUID) -> Optional[Dict[str, Any]]:
        """Get the current status of a batch."""
        return self.repository.get_batch_status(batch_id)

    def get_batch_metrics_snapshot(self, batch_id: uuid.UUID) -> Optional[Dict[str, Any]]:
        """Get thread-safe snapshot of batch metrics."""
        with self._metrics_lock:
            accumulator = self._active_batches.get(str(batch_id))
            if accumulator:
                return accumulator.get_metrics_snapshot()
        return None

    def get_all_active_batches_status(self) -> Dict[str, Dict[str, Any]]:
        """Get status of all active batches and subbatches."""
        with self._metrics_lock:
            status = {}
            
            # Active batches
            for batch_id, accumulator in self._active_batches.items():
                status[f"batch_{batch_id}"] = accumulator.get_metrics_snapshot()
            
            # Active subbatches
            for subbatch_id, accumulator in self._active_subbatches.items():
                parent_batch = self._subbatch_to_batch.get(subbatch_id, "unknown")
                status[f"subbatch_{subbatch_id}"] = {
                    **accumulator.get_metrics_snapshot(),
                    "parent_batch_id": parent_batch
                }
            
            return status
