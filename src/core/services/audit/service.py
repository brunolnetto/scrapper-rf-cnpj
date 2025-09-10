"""
Unified audit management service for the CNPJ ETL project.

This service centralizes audit creation, validation, and insertion logic for use by ETL and data loading components.
Includes comprehensive manifest tracking, file integrity verification, and batch tracking capabilities.
"""

import hashlib
import os
import uuid
import time
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Dict, Any, Generator
from contextlib import contextmanager
from dataclasses import dataclass, field
from threading import Lock

from sqlalchemy import text

from ....setup.logging import logger
from ....setup.config import get_config, ConfigurationService, AppConfig
from ....database.engine import Database
from ....database.models import AuditDB, BatchIngestionManifest, SubbatchIngestionManifest, BatchStatus, SubbatchStatus
from ....database.utils.models import create_audits, create_audit_metadata, insert_audit
from ...schemas import AuditMetadata, FileInfo, FileGroupInfo
from ...utils.schemas import create_file_groups


@dataclass
class BatchAccumulator:
    """Metrics accumulator for efficient batch tracking."""
    files_completed: int = 0
    files_failed: int = 0
    total_rows: int = 0
    total_bytes: int = 0
    start_time: float = field(default_factory=time.time)
    last_activity: float = field(default_factory=time.time)

    def add_file_event(self, status: str, rows: int = 0, bytes_: int = 0):
        """Add a file processing event to the accumulator."""
        if status in ['COMPLETED', 'SUCCESS']:
            self.files_completed += 1
            self.total_rows += rows
            self.total_bytes += bytes_
        elif status in ['FAILED', 'ERROR']:
            self.files_failed += 1
        self.last_activity = time.time()


class AuditService:
    """Unified audit and batch tracking service with comprehensive manifest capabilities."""

    def __init__(self, database: Database, config: Optional[ConfigurationService] = None):
        self.database = database
        self.config = config or get_config()
        
        # Batch tracking state
        self._active_batches: Dict[str, BatchAccumulator] = {}
        self._active_subbatches: Dict[str, BatchAccumulator] = {}
        self._subbatch_to_batch: Dict[str, str] = {}
        self._metrics_lock = Lock()

    @contextmanager
    def batch_context(self, target_table: str, batch_name: str, 
                     year: Optional[int] = None, month: Optional[int] = None) -> Generator[uuid.UUID, None, None]:
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
        if self.config.batch_config.enable_temporal_context:
            year = year or self.config.year
            month = month or self.config.month
            batch_name_with_context = f"{batch_name} ({year}-{month:02d})"
        else:
            batch_name_with_context = batch_name
        
        batch_id = self._start_batch(target_table, batch_name_with_context)
        
        try:
            logger.info(f"Starting batch processing for {target_table}", 
                       extra={"batch_id": str(batch_id), "table_name": target_table, 
                             "year": year, "month": month, "batch_name": batch_name})
            yield batch_id
            
            self._complete_batch_with_accumulated_metrics(batch_id, BatchStatus.COMPLETED)
            logger.info(f"Batch processing completed successfully", 
                       extra={"batch_id": str(batch_id), "table_name": target_table})
            
        except (OSError, IOError, ConnectionError, ValueError, RuntimeError) as e:
            logger.error(f"Batch processing failed for {target_table}: {e}", 
                        extra={"batch_id": str(batch_id), "table_name": target_table, 
                              "error": str(e)})
            self._complete_batch_with_accumulated_metrics(batch_id, BatchStatus.FAILED, str(e))
            raise
        finally:
            self._cleanup_batch_context(batch_id)

    @contextmanager  
    def subbatch_context(self, batch_id: uuid.UUID, table_name: str, 
                        description: Optional[str] = None) -> Generator[uuid.UUID, None, None]:
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
            logger.info(f"Starting subbatch processing", 
                       extra={"subbatch_id": str(subbatch_id), "batch_id": str(batch_id), 
                             "table_name": table_name, "description": description})
            yield subbatch_id
            
            self._complete_subbatch_with_accumulated_metrics(subbatch_id, SubbatchStatus.COMPLETED)
            logger.info(f"Subbatch processing completed", 
                       extra={"subbatch_id": str(subbatch_id), "table_name": table_name})
            
        except (OSError, IOError, ConnectionError, ValueError, RuntimeError) as e:
            logger.error(f"Subbatch processing failed: {e}", 
                        extra={"subbatch_id": str(subbatch_id), "batch_id": str(batch_id), 
                              "table_name": table_name, "error": str(e)})
            self._complete_subbatch_with_accumulated_metrics(subbatch_id, SubbatchStatus.FAILED, str(e))
            raise
        finally:
            self._cleanup_subbatch_context(subbatch_id)

    def create_audits_from_files(self, files_info: List[FileInfo]) -> List[AuditDB]:
        """
        Create audit records from a list of FileInfo objects.
        Filters for relevant files and groups them appropriately.
        """
        filtered_files = self._filter_relevant_files(files_info)
        file_groups = create_file_groups(filtered_files)
        return self._create_audits_from_groups(file_groups)

    def create_audit_metadata(
        self, audits: List[AuditDB], download_folder: str
    ) -> AuditMetadata:
        """
        Create audit metadata from audit records and download folder.
        """
        return create_audit_metadata(audits, download_folder)

    def insert_audits(self, audit_metadata: AuditMetadata) -> None:
        """
        Insert all audit records in audit_metadata into the database.
        Creates manifest entries for both successful and failed insertions.
        """
        for audit in audit_metadata.audit_list:
            try:
                # Attempt to insert the audit record
                insert_audit(self.database, audit.to_audit_db())

                # ✅ SUCCESS: Create manifest entry for successful insertion
                self._create_audit_manifest_entry(audit, "success")

                logger.info(f"Successfully inserted audit for {audit.audi_table_name}")

            except Exception as e:
                # ❌ FAILURE: Create manifest entry for failed insertion
                self._create_audit_manifest_entry(audit, "failed", error_message=str(e))

                logger.error(f"Failed to insert audit for {audit.audi_table_name}: {e}")

    def _create_audit_manifest_entry(self, audit, status: str, error_message: str = None) -> None:
        """Create a manifest entry for an audit insertion attempt.
        
        Note: This should NOT create file_ingestion_manifest entries for ZIP files.
        ZIP files are download artifacts, not processed data files.
        Only CSV/Parquet files that are actually processed should go in file_ingestion_manifests.
        """
        try:
            # Extract file information from audit
            file_path = "unknown"
            if hasattr(audit, 'audi_filenames') and audit.audi_filenames:
                file_path = audit.audi_filenames[0]  # Use first filename

            # Skip ZIP files - they shouldn't be in file_ingestion_manifests
            # Only processed CSV/Parquet files should be tracked there
            if file_path.endswith('.zip'):
                logger.debug(f"Skipping manifest entry for ZIP file: {file_path}")
                return

            # Extract table name
            table_name = getattr(audit, 'audi_table_name', 'unknown')

            # Extract file size if available
            filesize = getattr(audit, 'audi_file_size_bytes', 0)
            if filesize is None:
                filesize = 0

            # Extract audit ID (required for manifest entry)
            audit_id = getattr(audit, 'audi_id', None)
            if not audit_id:
                logger.error(f"Audit entry missing audi_id: {audit}")
                return

            # Generate manifest ID for audit entry
            manifest_id = str(uuid.uuid4())

            # Create manifest entry with table name and audit reference
            self._insert_manifest_entry(
                manifest_id=manifest_id,
                table_name=table_name,
                file_path=file_path,
                status=status,
                checksum=None,  # Audits don't have checksums
                filesize=int(filesize),
                rows_processed=0,  # Audit insertions don't have row counts
                processed_at=datetime.now(),
                audit_id=str(audit_id),  # FIXED: Required audit_id reference
                batch_id=None,  # Audit entries don't have batch_id
                subbatch_id=None  # Audit entries don't have subbatch_id
            )

            logger.debug(f"Created {status} manifest entry for audit: {table_name}")

        except (OSError, IOError, ValueError, RuntimeError) as e:
            logger.error(f"Failed to create manifest entry for audit {audit}: {e}")

    def _filter_relevant_files(self, files_info: List[FileInfo]) -> List[FileInfo]:
        """
        Filter files to only include relevant ones (e.g., .zip or layout .pdf).
        """
        return [
            info
            for info in files_info
            if info.filename.endswith(".zip")
            or (info.filename.endswith(".pdf") and "layout" in info.filename.lower())
        ]

    def _create_audits_from_groups(
        self, file_groups: List[FileGroupInfo]
    ) -> List[AuditDB]:
        """
        Create audit records from file groups using the database with ETL temporal context.
        """
        return create_audits(self.database, file_groups, 
                            etl_year=self.config.etl.year, 
                            etl_month=self.config.etl.month)

    # Manifest tracking capabilities
    def create_file_manifest(self, file_path: str, status: str, audit_id: str, 
                           checksum: Optional[str] = None, filesize: Optional[int] = None, 
                           rows: Optional[int] = None, table_name: str = 'unknown', 
                           notes: Optional[str] = None, batch_id: Optional[str] = None, 
                           subbatch_id: Optional[str] = None) -> str:
        """
        Create manifest entry for processed file with required audit reference.
        
        Args:
            file_path: Path to the file
            status: Processing status
            audit_id: Required audit entry ID for referential integrity
            checksum: Optional file checksum
            filesize: Optional file size in bytes
            rows: Optional number of rows processed
            table_name: Table name associated with the file
            notes: Optional processing notes
            subbatch_id: Optional subbatch ID for batch tracking
            
        Returns:
            manifest_id: The created manifest ID
        """

        try:
            file_path_obj = Path(file_path)

            # Calculate file metadata if not provided
            if file_path_obj.exists():
                if filesize is None:
                    filesize = file_path_obj.stat().st_size
                if checksum is None:
                    checksum = self._calculate_file_checksum(file_path_obj)

            # Generate default notes if none provided
            if notes is None:
                notes_parts = []
                if rows is not None:
                    notes_parts.append(f"Processed {rows:,} rows")
                if filesize is not None:
                    notes_parts.append(f"File size: {filesize:,} bytes")
                if status == "COMPLETED":
                    notes_parts.append("Successfully processed")
                elif status == "FAILED":
                    notes_parts.append("Processing failed")
                elif status == "PARTIAL":
                    notes_parts.append("Partially processed")
                elif status == "PENDING":
                    notes_parts.append("Processing pending")
                
                notes = "; ".join(notes_parts) if notes_parts else f"Status: {status}"

            # Generate manifest ID
            manifest_id = str(uuid.uuid4())

            # Insert manifest entry
            self._insert_manifest_entry(
                manifest_id=manifest_id,
                table_name=table_name,
                file_path=str(file_path_obj),
                status=status,
                checksum=checksum,
                filesize=filesize,
                rows_processed=rows,
                processed_at=datetime.now(),
                audit_id=audit_id,  # FIXED: Required audit_id reference
                notes=notes,
                batch_id=batch_id,
                subbatch_id=subbatch_id
            )

            # Collect metrics for batch tracking (no additional DB calls)
            if subbatch_id:
                self._collect_file_event(subbatch_id, status, rows or 0, filesize or 0)

            logger.info(f"Manifest entry created for {file_path_obj.name} (status: {status})")
            return manifest_id

        except Exception as e:
            logger.error(f"Failed to create manifest entry for {file_path}: {e}")
            return ""

    def _calculate_file_checksum(self, file_path: Path, algorithm: str = 'sha256') -> str:
        """Calculate file checksum for integrity verification."""
        try:
            # Skip checksum for very large files to avoid performance issues
            filesize = file_path.stat().st_size
            checksum_threshold_mb = int(os.getenv("ETL_CHECKSUM_THRESHOLD_MB", "1000"))  # 1000MB (1GB) default
            checksum_threshold_bytes = checksum_threshold_mb * 1024 * 1024
            
            if filesize > checksum_threshold_bytes:
                logger.info(f"[PERFORMANCE] Skipping checksum calculation for large file {file_path.name}: {filesize:,} bytes (> {checksum_threshold_mb}MB)")
                return None
            
            hash_func = hashlib.new(algorithm)
            with open(file_path, 'rb') as f:
                # Read in chunks to handle large files
                for chunk in iter(lambda: f.read(8192), b""):
                    hash_func.update(chunk)
            return hash_func.hexdigest()
        except Exception as e:
            logger.error(f"Failed to calculate checksum for {file_path}: {e}")
            return None

    def _insert_manifest_entry(self, manifest_id: str, table_name: str, file_path: str, status: str,
                              checksum: Optional[str], filesize: Optional[int],
                              rows_processed: Optional[int], processed_at: datetime, 
                              audit_id: str, notes: Optional[str] = None, batch_id: Optional[str] = None, 
                              subbatch_id: Optional[str] = None) -> None:
        """Insert manifest entry into database with required audit_id and optional batch/subbatch references."""
        try:
            insert_query = '''
            INSERT INTO file_ingestion_manifest
            (file_manifest_id, table_name, file_path, status, checksum, filesize, rows_processed, processed_at, audit_id, notes, batch_id, subbatch_id)
            VALUES (:file_manifest_id, :table_name, :file_path, :status, :checksum, :filesize, :rows_processed, :processed_at, :audit_id, :notes, :batch_id, :subbatch_id)
            '''

            with self.database.engine.begin() as conn:
                conn.execute(text(insert_query), {
                    'file_manifest_id': manifest_id,
                    'table_name': table_name,
                    'file_path': file_path,
                    'status': status,
                    'checksum': checksum,
                    'filesize': filesize,
                    'rows_processed': rows_processed,
                    'processed_at': processed_at,
                    'audit_id': audit_id,
                    'notes': notes,
                    'batch_id': batch_id,
                    'subbatch_id': subbatch_id
                })

        except Exception as e:
            logger.error(f"Failed to insert manifest entry: {e}")

    def get_file_manifest_history(self, filename: str) -> List[dict]:
        """Get manifest history for a specific file."""
        try:
            from sqlalchemy import text

            query = '''
            SELECT file_path, filename, status, checksum, filesize, rows_processed, processed_at
            FROM file_ingestion_manifest
            WHERE filename = :filename
            ORDER BY processed_at DESC
            '''

            with self.database.engine.connect() as conn:
                result = conn.execute(text(query), {'filename': filename})
                return [dict(row._mapping) for row in result]

        except Exception as e:
            logger.error(f"Failed to get manifest history for {filename}: {e}")
            return []

    def verify_file_integrity(self, file_path: str) -> bool:
        """Verify file integrity against stored manifest."""
        try:
            from sqlalchemy import text
            file_path_obj = Path(file_path)

            # Get stored checksum
            with self.database.engine.connect() as conn:
                result = conn.execute(text('''
                    SELECT checksum FROM file_ingestion_manifest
                    WHERE file_path = :file_path
                    ORDER BY processed_at DESC LIMIT 1
                '''), {'file_path': str(file_path_obj)})

                row = result.fetchone()
                if not row or not row[0]:
                    logger.warning(f"No checksum found for {file_path}")
                    return True

                stored_checksum = row[0]

            # Calculate current checksum
            current_checksum = self._calculate_file_checksum(file_path_obj)

            if current_checksum == stored_checksum:
                logger.debug(f"File integrity verified: {file_path}")
                return True
            else:
                logger.error(f"File integrity check failed: {file_path}")
                return False

        except Exception as e:
            logger.error(f"Error verifying file integrity for {file_path}: {e}")
            return False

    def update_manifest_notes(self, file_path: str, notes: str, append: bool = False) -> None:
        """Update notes for an existing manifest entry."""
        try:
            from sqlalchemy import text
            from pathlib import Path
            file_path_obj = Path(file_path)

            # Get current notes if appending
            current_notes = None
            if append:
                with self.database.engine.connect() as conn:
                    result = conn.execute(text('''
                        SELECT notes FROM file_ingestion_manifest
                        WHERE file_path = :file_path
                        ORDER BY processed_at DESC LIMIT 1
                    '''), {'file_path': str(file_path_obj)})

                    row = result.fetchone()
                    if row and row[0]:
                        current_notes = row[0]

            # Prepare new notes
            if append and current_notes:
                updated_notes = f"{current_notes}; {notes}"
            else:
                updated_notes = notes

            # Update the notes
            with self.database.engine.begin() as conn:
                conn.execute(text('''
                    UPDATE file_ingestion_manifest
                    SET notes = :notes, processed_at = :processed_at
                    WHERE file_path = :file_path
                    AND processed_at = (
                        SELECT MAX(processed_at) FROM file_ingestion_manifest
                        WHERE file_path = :file_path
                    )
                '''), {
                    'file_path': str(file_path_obj),
                    'notes': updated_notes,
                    'processed_at': datetime.now()
                })

            logger.info(f"Updated notes for {file_path_obj.name}")

        except Exception as e:
            logger.error(f"Failed to update manifest notes for {file_path}: {e}")

    def update_file_manifest(self, manifest_id: str, status: str, 
                            rows_processed: Optional[int] = None, 
                            error_msg: Optional[str] = None,
                            notes: Optional[dict] = None) -> None:
        """Update an existing file manifest entry with completion details."""
        try:
            from sqlalchemy import text
            import json
            
            # Build update fields dynamically
            update_fields = ['status = :status', 'processed_at = :processed_at']
            params = {
                'manifest_id': manifest_id,
                'status': status,
                'processed_at': datetime.now()
            }
            
            if rows_processed is not None:
                update_fields.append('rows_processed = :rows_processed')
                params['rows_processed'] = rows_processed
                
            if error_msg is not None:
                update_fields.append('error_message = :error_msg')
                params['error_msg'] = error_msg
                
            if notes is not None:
                update_fields.append('notes = :notes')
                params['notes'] = json.dumps(notes) if isinstance(notes, dict) else str(notes)
            
            # Execute update
            with self.database.engine.begin() as conn:
                conn.execute(text(f'''
                    UPDATE file_ingestion_manifest 
                    SET {', '.join(update_fields)}
                    WHERE file_manifest_id = :manifest_id
                '''), params)
                
            # Collect metrics for batch accumulation if this is a completion
            if status in ['COMPLETED', 'FAILED'] and rows_processed is not None:
                # Get subbatch_id for metrics collection
                try:
                    with self.database.engine.connect() as conn:
                        result = conn.execute(text('''
                            SELECT subbatch_id FROM file_ingestion_manifest
                            WHERE file_manifest_id = :manifest_id
                        '''), {'manifest_id': manifest_id})
                        row = result.fetchone()
                        if row and row[0]:
                            subbatch_id = str(row[0])
                            self._collect_file_event(subbatch_id, status, rows_processed or 0, 0)
                            logger.debug(f"Collected file metrics: {status}, {rows_processed} rows")
                except Exception as metrics_error:
                    logger.warning(f"Failed to collect metrics for manifest {manifest_id}: {metrics_error}")
                
            logger.debug(f"Updated manifest {manifest_id} with status {status}")
            
        except Exception as e:
            logger.error(f"Failed to update file manifest {manifest_id}: {e}")

    def _collect_file_event(self, subbatch_id: str, status: str, rows: int = 0, bytes_: int = 0) -> None:
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

    # Batch lifecycle management methods
    def _start_batch(self, target_table: str, batch_name: str) -> uuid.UUID:
        """Start a new batch for a single target table and return its ID."""
        try:
            batch_id = uuid.uuid4()
            batch_data = {
                'batch_id': str(batch_id),
                'batch_name': batch_name,
                'target_table': target_table,
                'status': BatchStatus.RUNNING.value,
                'started_at': datetime.now(),
                'description': f"Processing table: {target_table}"
            }

            with self.database.engine.begin() as conn:
                conn.execute(text('''
                    INSERT INTO batch_ingestion_manifest
                    (batch_id, batch_name, target_table, status, started_at, description)
                    VALUES (:batch_id, :batch_name, :target_table, :status, :started_at, :description)
                '''), batch_data)

            # Start in-memory tracking
            with self._metrics_lock:
                self._active_batches[str(batch_id)] = BatchAccumulator()

            logger.info(f"Started batch {batch_id} for table: {target_table}")
            return batch_id

        except Exception as e:
            logger.error(f"Failed to start batch: {e}")
            raise

    def _start_subbatch(self, batch_id: uuid.UUID, table_name: str, description: str = None) -> uuid.UUID:
        """Start a new subbatch and return its ID."""
        try:
            subbatch_id = uuid.uuid4()
            subbatch_data = {
                'subbatch_manifest_id': str(subbatch_id),
                'batch_manifest_id': str(batch_id),
                'table_name': table_name,
                'status': SubbatchStatus.RUNNING.value,
                'started_at': datetime.now(),
                'description': description or f"Processing table: {table_name}"
            }

            with self.database.engine.begin() as conn:
                conn.execute(text('''
                    INSERT INTO subbatch_ingestion_manifest
                    (subbatch_manifest_id, batch_manifest_id, table_name, status, started_at, description)
                    VALUES (:subbatch_manifest_id, :batch_manifest_id, :table_name, :status, :started_at, :description)
                '''), subbatch_data)

            # Start in-memory tracking
            with self._metrics_lock:
                self._active_subbatches[str(subbatch_id)] = BatchAccumulator()
                self._subbatch_to_batch[str(subbatch_id)] = str(batch_id)

            logger.info(f"Started subbatch {subbatch_id} for table: {table_name}")
            return subbatch_id

        except Exception as e:
            logger.error(f"Failed to start subbatch: {e}")
            raise

    def _complete_batch_with_accumulated_metrics(self, batch_id: uuid.UUID, status: BatchStatus, 
                                               error_message: str = None) -> None:
        """Complete batch with accumulated metrics - single DB update."""
        completed_at = datetime.now()
        
        # Get accumulated metrics
        with self._metrics_lock:
            accumulator = self._active_batches.get(str(batch_id), BatchAccumulator())
            duration = completed_at.timestamp() - accumulator.start_time
            
            # Cleanup
            self._active_batches.pop(str(batch_id), None)
        
        # Single database update with all metrics
        try:
            update_data = {
                'batch_id': str(batch_id),
                'status': status.value,
                'completed_at': completed_at,
                'error_message': error_message,
                'metrics_summary': f"Files: {accumulator.files_completed + accumulator.files_failed}, Rows: {accumulator.total_rows:,}, Duration: {duration:.1f}s"
            }

            with self.database.engine.begin() as conn:
                conn.execute(text('''
                    UPDATE batch_ingestion_manifest
                    SET status = :status, completed_at = :completed_at, error_message = :error_message,
                        description = description || ' | ' || :metrics_summary
                    WHERE batch_id = :batch_id
                '''), update_data)

            logger.info(f"Completed batch {batch_id} with status: {status.value} in {duration:.1f}s")

        except Exception as e:
            logger.error(f"Failed to complete batch {batch_id}: {e}")

    def _complete_subbatch_with_accumulated_metrics(self, subbatch_id: uuid.UUID, status: SubbatchStatus, 
                                                  error_message: str = None) -> None:
        """Complete subbatch with accumulated metrics - single DB update."""
        completed_at = datetime.now()
        
        # Get accumulated metrics
        with self._metrics_lock:
            accumulator = self._active_subbatches.get(str(subbatch_id), BatchAccumulator())
            duration = completed_at.timestamp() - accumulator.start_time
            
            # Cleanup
            self._active_subbatches.pop(str(subbatch_id), None)
            self._subbatch_to_batch.pop(str(subbatch_id), None)
        
        # Single database update with all metrics
        try:
            update_data = {
                'subbatch_id': str(subbatch_id),
                'status': status.value,
                'completed_at': completed_at,
                'error_message': error_message,
                'files_processed': accumulator.files_completed + accumulator.files_failed,
                'rows_processed': accumulator.total_rows
            }

            with self.database.engine.begin() as conn:
                conn.execute(text('''
                    UPDATE subbatch_ingestion_manifest
                    SET status = :status, completed_at = :completed_at, error_message = :error_message,
                        files_processed = :files_processed, rows_processed = :rows_processed
                    WHERE subbatch_manifest_id = :subbatch_id
                '''), update_data)

            logger.info(f"Completed subbatch {subbatch_id} with status: {status.value} in {duration:.1f}s")

        except Exception as e:
            logger.error(f"Failed to complete subbatch {subbatch_id}: {e}")

    def _cleanup_batch_context(self, batch_id: uuid.UUID) -> None:
        """Clean up batch context after completion."""
        with self._metrics_lock:
            self._active_batches.pop(str(batch_id), None)

    def _cleanup_subbatch_context(self, subbatch_id: uuid.UUID) -> None:
        """Clean up subbatch context after completion."""
        with self._metrics_lock:
            self._active_subbatches.pop(str(subbatch_id), None)
            self._subbatch_to_batch.pop(str(subbatch_id), None)

    def update_subbatch_metrics(self, subbatch_id: uuid.UUID, files_processed: int = None, 
                               rows_processed: int = None, notes: str = None) -> None:
        """Update metrics for a subbatch."""
        try:
            update_data = {
                'subbatch_id': str(subbatch_id),
                'files_processed': files_processed,
                'rows_processed': rows_processed,
                'notes': notes
            }

            # Filter out None values
            update_data = {k: v for k, v in update_data.items() if v is not None}

            if len(update_data) > 1:  # More than just subbatch_id
                set_clause = ', '.join([f"{k} = :{k}" for k in update_data.keys() if k != 'subbatch_id'])
                
                with self.database.engine.begin() as conn:
                    conn.execute(text(f'''
                        UPDATE subbatch_ingestion_manifest
                        SET {set_clause}
                        WHERE subbatch_manifest_id = :subbatch_id
                    '''), update_data)

                logger.debug(f"Updated metrics for subbatch {subbatch_id}")

        except Exception as e:
            logger.error(f"Failed to update subbatch metrics {subbatch_id}: {e}")

    def get_batch_status(self, batch_id: uuid.UUID) -> Optional[Dict[str, Any]]:
        """Get the current status of a batch."""
        try:
            with self.database.engine.connect() as conn:
                result = conn.execute(text('''
                    SELECT batch_id, batch_name, target_table, status, started_at, completed_at, 
                           error_message, description
                    FROM batch_ingestion_manifest
                    WHERE batch_id = :batch_id
                '''), {'batch_id': str(batch_id)})
                
                row = result.fetchone()
                return dict(row._mapping) if row else None

        except Exception as e:
            logger.error(f"Failed to get batch status {batch_id}: {e}")
            return None




