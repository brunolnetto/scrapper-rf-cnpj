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
import json
from pathlib import Path
from typing import List, Optional, Dict, Any, Generator
from typing import Union
from contextlib import contextmanager
from dataclasses import dataclass, field
from threading import Lock

from sqlalchemy import text

from ....setup.logging import logger
from ....setup.config import get_config, ConfigurationService
from ....database.engine import Database
from ....database.models.audit import (
    TableAuditManifest, 
    AuditStatus
)
from ....utils.models import create_audits, create_audit_metadata, insert_audit
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

    def add_file_event(self, status: AuditStatus, rows: int = 0, bytes_: int = 0):
        """Add a file processing event to the accumulator.

        Accepts legacy status strings and normalizes them to the
        AuditStatus values before updating metrics.
        """
        # Accept either AuditStatus instances or legacy status strings and normalize
        norm_status = None
        try:
            if isinstance(status, AuditStatus):
                norm_status = status
            elif isinstance(status, str):
                # try mapping by name (case-insensitive) then by value
                try:
                    norm_status = AuditStatus[status.strip().upper()]
                except Exception:
                    try:
                        norm_status = AuditStatus(status)
                    except Exception:
                        norm_status = AuditStatus.PENDING
            else:
                norm_status = AuditStatus.PENDING
        except Exception:
            norm_status = AuditStatus.PENDING

        if norm_status == AuditStatus.COMPLETED:
            self.files_completed += 1
            self.total_rows += int(rows or 0)
        elif norm_status == AuditStatus.FAILED:
            self.files_failed += 1
        # update last activity timestamp
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

    def _coerce_status(self, status: Union[AuditStatus, str]) -> AuditStatus:
        """Coerce a status value (enum or string) into an AuditStatus enum.

        - If already an AuditStatus, return as-is.
        - If string, try mapping by name (case-insensitive) then by value.
        - On failure, return AuditStatus.PENDING as safe default.
        """
        try:
            if isinstance(status, AuditStatus):
                return status
            if isinstance(status, str):
                try:
                    return AuditStatus[status.strip().upper()]
                except Exception:
                    try:
                        return AuditStatus(status)
                    except Exception:
                        return AuditStatus.PENDING
        except Exception:
            return AuditStatus.PENDING
        return AuditStatus.PENDING

    @contextmanager
    def batch_context(self, target_table: str, batch_name: str, 
                     year: Optional[int] = None, month: Optional[int] = None,
                     file_manifest_id: Optional[str] = None, 
                     table_manifest_id: Optional[str] = None) -> Generator[uuid.UUID, None, None]:
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
        if hasattr(self.config, 'year') and hasattr(self.config, 'month'):
            year = year or self.config.year
            month = month or self.config.month
            batch_name_with_context = f"{batch_name} ({year}-{month:02d})"
        else:
            batch_name_with_context = batch_name
        
        batch_id = self._start_batch(target_table, batch_name_with_context, 
                                     file_manifest_id=file_manifest_id, 
                                     table_manifest_id=table_manifest_id)
        
        try:
            logger.info(f"Starting batch processing for {target_table}", 
                       extra={"batch_id": str(batch_id), "table_name": target_table, 
                             "year": year, "month": month, "batch_name": batch_name})
            yield batch_id
            
            self._complete_batch_with_accumulated_metrics(batch_id, AuditStatus.COMPLETED)
            logger.info(f"Batch processing completed successfully", 
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
            
            self._complete_subbatch_with_accumulated_metrics(subbatch_id, AuditStatus.COMPLETED)
            logger.info(f"Subbatch processing completed", 
                       extra={"subbatch_id": str(subbatch_id), "table_name": table_name})
            
        except (OSError, IOError, ConnectionError, ValueError, RuntimeError) as e:
            logger.error(f"Subbatch processing failed: {e}", 
                        extra={"subbatch_id": str(subbatch_id), "batch_id": str(batch_id), 
                              "table_name": table_name, "error": str(e)})
            self._complete_subbatch_with_accumulated_metrics(subbatch_id, AuditStatus.FAILED, str(e))
            raise
        finally:
            self._cleanup_subbatch_context(subbatch_id)

    def create_audits_from_files(self, files_info: List[FileInfo]) -> List[TableAuditManifest]:
        """
        Create audit records from a list of FileInfo objects.
        Filters for relevant files and groups them appropriately.
        """
        filtered_files = self._filter_relevant_files(files_info)
        file_groups = create_file_groups(filtered_files, self.config.year, self.config.month)
        return self._create_audits_from_groups(file_groups)

    def create_audit_metadata(
        self, audits: List[TableAuditManifest], download_folder: str
    ) -> AuditMetadata:
        """
        Create audit metadata from audit records and download folder.
        """
        return create_audit_metadata(audits, download_folder)

    def insert_audits(self, audit_metadata: AuditMetadata, create_file_manifests: bool = True) -> None:
        """
        Insert all audit records in audit_metadata into the database.
        Creates manifest entries for both successful and failed insertions.
        
        Args:
            audit_metadata: Audit metadata with table information
            create_file_manifests: If False, skip creating file manifest entries to avoid duplicates
                                  when file processing will create its own manifests later
        """
        for audit in audit_metadata.audit_list:
            try:
                # Convert schema to SQLAlchemy model and insert
                audit_model = audit.to_db_model()
                insert_audit(self.database, audit_model)

                # ✅ SUCCESS: Create manifest entry for successful insertion (if enabled)
                if create_file_manifests:
                    # Pass the enum itself; manifest entry expects AuditStatus
                    self._create_audit_manifest_entry(audit, AuditStatus.COMPLETED)
                else:
                    logger.debug(f"Skipping file manifest creation for {audit.entity_name} (will be created during processing)")

                logger.info(f"Successfully inserted audit for {audit.entity_name}")

            except Exception as e:
                # ❌ FAILURE: Create manifest entry for failed insertion (always create for failures)
                self._create_audit_manifest_entry(audit, AuditStatus.FAILED, error_message=str(e))

                logger.error(f"Failed to insert audit for {audit.entity_name}: {e}")

    def _create_audit_manifest_entry(self, audit, status: AuditStatus, error_message: str = None) -> None:
        """Create a manifest entry for an audit insertion attempt.
        
        Note: This should NOT create file_ingestion_manifest entries for ZIP files.
        ZIP files are download artifacts, not processed data files.
        Only CSV/Parquet files that are actually processed should go in file_ingestion_manifests.
        """
        try:
            # Extract file information from audit
            file_path = "unknown"
            if hasattr(audit, 'source_files') and audit.source_files:
                file_path = audit.source_files[0]  # Use first filename

            # Skip ZIP files - they shouldn't be in file_ingestion_manifests
            # Only processed CSV/Parquet files should be tracked there
            if file_path.endswith('.zip'):
                logger.debug(f"Skipping manifest entry for ZIP file: {file_path}")
                return

            # Extract table name
            table_name = getattr(audit, 'entity_name', 'unknown')

            # Extract file size if available
            filesize = getattr(audit, 'file_size_bytes', 0)
            if filesize is None:
                filesize = 0

            # Extract audit ID (table manifest ID)
            table_audit_id = getattr(audit, 'table_audit_id', None)
            if not table_audit_id:
                logger.error(f"Audit entry missing table_manifest_id: {audit}")
                return

            # Generate manifest ID for audit entry
            manifest_id = str(uuid.uuid4())

            # Create manifest entry with table name and audit reference
            self._insert_manifest_entry(
                manifest_id=manifest_id,
                table_name=Path(file_path).name if file_path != "unknown" else table_name,  # entity_name should be filename
                file_path=file_path,
                status=status,
                checksum=None,  # Audits don't have checksums
                filesize=int(filesize),
                rows_processed=0,  # Audit insertions don't have row counts
                table_manifest_id=str(table_audit_id)
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
                or (info.filename.endswith(".pdf") 
                and "layout" in info.filename.lower())
        ]

    def _create_audits_from_groups(
        self, file_groups: List[FileGroupInfo]
    ) -> List[TableAuditManifest]:
        """
        Create audit records from file groups using the database with ETL temporal context.
        """
        return create_audits(
            self.database, file_groups, 
            etl_year=self.config.year, 
            etl_month=self.config.month
        )

    # Manifest tracking capabilities
    def _find_table_audit(self, table_name: str) -> Optional[str]:
        """Find the most recent table audit ID for a given table name."""
        try:
            from sqlalchemy import text
            
            query = '''
            SELECT table_audit_id FROM table_audit_manifest 
            WHERE entity_name = :table_name 
            ORDER BY created_at DESC 
            LIMIT 1
            '''
            
            with self.database.engine.connect() as conn:
                result = conn.execute(text(query), {'table_name': table_name})
                row = result.fetchone()
                return row[0] if row else None
                
        except Exception as e:
            logger.error(f"Failed to find table audit for {table_name}: {e}")
            return None

    def create_file_manifest(
        self, file_path: str, status: AuditStatus, table_manifest_id: str, 
            checksum: Optional[str] = None, filesize: Optional[int] = None, 
            rows: Optional[int] = None, table_name: str = 'unknown', 
            notes: Optional[str] = None) -> str:
        """
        Create manifest entry for processed file with required table audit reference.
        
        Args:
            file_path: Path to the file
            status: Processing status
            table_manifest_id: Required table audit entry ID for referential integrity
            checksum: Optional file checksum
            filesize: Optional file size in bytes
            rows: Optional number of rows processed
            table_name: Table name associated with the file
            notes: Optional processing notes
            
        Returns:
            manifest_id: The created manifest ID
        """

        try:
            file_path_obj = Path(file_path)

            # Use provided table_manifest_id if available, otherwise find one
            if not table_manifest_id:
                table_manifest_id = self._find_table_audit(table_name)
                if not table_manifest_id:
                    logger.warning(f"No table audit found for table {table_name}, file processing may be incomplete")

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

                # Prefer AuditStatus enum comparisons. Accept AuditStatus or string.
                if isinstance(status, AuditStatus):
                    status_name = status.value
                elif isinstance(status, str):
                    status_name = status.strip().upper()
                else:
                    status_name = AuditStatus.PENDING.value

                if status_name == AuditStatus.COMPLETED.value:
                    notes_parts.append("Successfully processed")
                elif status_name == AuditStatus.FAILED.value:
                    notes_parts.append("Processing failed")
                elif status_name == "PARTIAL":
                    notes_parts.append("Partially processed")
                elif status_name == AuditStatus.PENDING.value:
                    notes_parts.append("Processing pending")

                notes = "; ".join(notes_parts) if notes_parts else f"Status: {status_name}"

            # Generate manifest ID
            manifest_id = str(uuid.uuid4())

            # Insert manifest entry
            self._insert_manifest_entry(
                manifest_id=manifest_id,
                table_name=file_path_obj.name,  # entity_name should be filename
                file_path=str(file_path_obj),
                status=status,
                checksum=checksum,
                filesize=filesize,
                rows_processed=rows,
                table_manifest_id=table_manifest_id
            )

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

    def _insert_manifest_entry(
        self, manifest_id: str, table_name: str, file_path: str, status: AuditStatus,
        checksum: Optional[str], filesize: Optional[int],
        rows_processed: Optional[int], 
        table_manifest_id: str, 
        notes: Optional[str] = None
    ) -> None:
        """Insert manifest entry into database with required table_manifest_id."""
        try:
            insert_query = '''
            INSERT INTO file_audit_manifest
            (
                file_audit_id, 
                parent_table_audit_id, 
                entity_name, 
                status, 
                created_at,
                file_path,
                checksum,
                filesize, 
                rows_processed,
                notes
            )
            VALUES (
                :file_audit_id, 
                :table_audit_id, 
                :entity_name, 
                :status,  
                :created_at,
                :file_path,
                :checksum,
                :filesize, 
                :rows_processed,
                :notes
            )
            '''

            # Coerce status to AuditStatus enum for consistent DB values
            norm_status = self._coerce_status(status)

            # Ensure notes are serialized consistently
            try:
                if notes is None:
                    safe_notes = '{}'  # store empty object
                elif isinstance(notes, dict):
                    safe_notes = json.dumps(notes)
                else:
                    # wrap string notes into a dict for consistency
                    safe_notes = json.dumps({'notes': str(notes)})
            except Exception:
                safe_notes = json.dumps({'notes': str(notes)})

            with self.database.engine.begin() as conn:
                conn.execute(text(insert_query), {
                    'file_audit_id': manifest_id,
                    'table_audit_id': table_manifest_id,
                    'entity_name': table_name,
                    'status': norm_status.value,
                    'created_at': datetime.now(),
                    'file_path': file_path,
                    'checksum': checksum,
                    'filesize': filesize,
                    'rows_processed': rows_processed,
                    'notes': safe_notes
                })

        except Exception as e:
            logger.error(f"Failed to insert manifest entry: {e}")

    def get_file_manifest_history(self, filename: str) -> List[dict]:
        """Get manifest history for a specific file."""
        try:
            from sqlalchemy import text

            query = '''
            SELECT 
                file_path, 
                entity_name as filename, 
                status, 
                checksum, 
                filesize, 
                rows_processed, 
                completed_at
            FROM file_audit_manifest
            WHERE entity_name = :filename
            ORDER BY completed_at DESC
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
                query=text('''
                    SELECT checksum 
                    FROM file_audit_manifest
                    WHERE file_path = :file_path
                    ORDER BY completed_at DESC LIMIT 1
                ''')
                result = conn.execute(
                    query, 
                    {'file_path': str(file_path_obj)}
                )

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
            # Get current audit_metadata if appending
            current_meta = None
            if append:
                with self.database.engine.connect() as conn:
                    query=text('''
                        SELECT notes 
                        FROM file_audit_manifest
                        WHERE file_path = :file_path
                        ORDER BY updated_at DESC LIMIT 1
                    ''')
                    result = conn.execute(
                        query, 
                        {'file_path': str(file_path_obj)}
                    )

                    row = result.fetchone()
                    if row and row[0]:
                        try:
                            current_meta = row[0] if isinstance(row[0], dict) else json.loads(row[0])
                        except Exception:
                            current_meta = {'notes': row[0]}

            # Prepare new audit_metadata
            if append and current_meta and isinstance(current_meta, dict):
                # merge notes into 'processing_update' or 'notes'
                existing_notes = current_meta.get('notes')
                if existing_notes:
                    merged = f"{existing_notes}; {notes}"
                else:
                    merged = notes
                current_meta['notes'] = merged
                updated_meta = current_meta
            else:
                updated_meta = notes

            # Update the audit_metadata
            with self.database.engine.begin() as conn:
                conn.execute(text('''
                    UPDATE file_audit_manifest
                    SET notes = :notes, updated_at = :updated_at
                    WHERE file_path = :file_path
                    AND updated_at = (
                        SELECT MAX(updated_at) FROM file_audit_manifest
                        WHERE file_path = :file_path
                    )
                '''), {
                    'file_path': str(file_path_obj),
                    'notes': json.dumps(updated_meta),
                    'updated_at': datetime.now()
                })

            logger.info(f"Updated notes for {file_path_obj.name}")

        except Exception as e:
            logger.error(f"Failed to update manifest notes for {file_path}: {e}")

    def update_table_audit_after_conversion(
        self, table_name: str, processed_file_path: str, 
        processing_metadata: dict = None
    ) -> None:
        """
        Update table audit after file conversion to reference processed files instead of source files.
        
        Args:
            table_name: Name of the table that was processed
            processed_file_path: Path to the processed file (e.g., Parquet file)
            processing_metadata: Optional metadata about the processing
        """
        try:
            import json
            from pathlib import Path
            
            processed_filename = Path(processed_file_path).name
            
            # Update table audit to reference processed file
            with self.database.engine.begin() as conn:
                # First, get the table audit for this table
                result = conn.execute(text('''
                    SELECT table_audit_id 
                    FROM table_audit_manifest 
                    WHERE entity_name = :table_name
                    ORDER BY created_at DESC 
                    LIMIT 1
                '''), {'table_name': table_name})
                
                row = result.fetchone()
                if not row:
                    logger.warning(f"No table audit found for {table_name}")
                    return
                
                table_manifest_id = row[0]
                
                # Prepare metadata
                metadata = processing_metadata or {}
                metadata.update({
                    "conversion_completed": True,
                    "converted_file": processed_filename,
                    "conversion_timestamp": datetime.now().isoformat()
                })
                
                # Update the table audit
                source_files_json = json.dumps([processed_filename])
                metadata_json = json.dumps(metadata)
                
                conn.execute(text('''
                    UPDATE table_audit_manifest 
                    SET source_files = :source_files,
                        notes = :notes,
                        completed_at = :completed_at,
                        status = :status
                    WHERE table_audit_id = :table_audit_id
                '''), {
                    'source_files': source_files_json,
                    'notes': metadata_json,
                    'completed_at': datetime.now(),
                    'status': 'COMPLETED',
                    'table_audit_id': str(table_manifest_id)
                })
                
                logger.info(f"Updated table audit for {table_name} to reference {processed_filename}")
                
        except Exception as e:
            logger.error(f"Failed to update table audit after conversion for {table_name}: {e}")

    def update_file_manifest(
        self, manifest_id: str, status: AuditStatus, 
        rows_processed: Optional[int] = None, 
        error_msg: Optional[str] = None,
        notes: Optional[dict] = None
    ) -> None:
        """Update an existing file manifest entry with completion details."""
        try:
            from sqlalchemy import text
            import json
            
            # Coerce status to enum and build update fields dynamically
            norm_status = self._coerce_status(status)

            update_fields = ['status = :status', 'completed_at = :completed_at']
            params = {
                'manifest_id': manifest_id,
                'status': norm_status.value,
                'completed_at': datetime.now()
            }
            
            if rows_processed is not None:
                update_fields.append('rows_processed = :rows_processed')
                params['rows_processed'] = rows_processed
                
            if error_msg is not None:
                update_fields.append('error_message = :error_msg')
                params['error_msg'] = error_msg
                
            if notes is not None:
                # Write notes into audit_metadata JSON field
                update_fields.append('notes = :notes')
                try:
                    params['notes'] = json.dumps(notes) if isinstance(notes, dict) else json.dumps({'notes': str(notes)})
                except Exception:
                    params['notes'] = json.dumps({'notes': str(notes)})
            
            # Execute update
            with self.database.engine.begin() as conn:
                conn.execute(text(f'''
                    UPDATE file_audit_manifest 
                    SET {', '.join(update_fields)}
                    WHERE file_audit_id = :manifest_id
                '''), params)
                
                # Collect metrics for batch accumulation if this is a completion
                try:
                    if norm_status in (AuditStatus.COMPLETED, AuditStatus.FAILED) and rows_processed is not None:
                        with self.database.engine.connect() as conn:
                            # Query chain: file_audit → batch_audit → subbatch_audit (latest subbatch)
                            result = conn.execute(text('''
                                SELECT s.subbatch_audit_id 
                                FROM file_audit_manifest f
                                JOIN batch_audit_manifest b ON f.file_audit_id = b.parent_file_audit_id
                                JOIN subbatch_audit_manifest s ON b.batch_audit_id = s.parent_batch_audit_id
                                WHERE f.file_audit_id = :manifest_id
                                ORDER BY s.started_at DESC
                                LIMIT 1
                            '''), {'manifest_id': manifest_id})
                            row = result.fetchone()
                            if row and row[0]:
                                subbatch_id = str(row[0])
                                # Use coerced enum when collecting
                                self.collect_file_processing_event(subbatch_id, norm_status, int(rows_processed or 0), 0)
                                logger.debug(f"Collected file metrics: {norm_status}, {rows_processed} rows")
                except Exception as metrics_error:
                    logger.debug(f"Could not collect metrics for manifest {manifest_id}: {metrics_error}")
                        
            logger.debug(f"Updated manifest {manifest_id} with status {status}")
            
        except Exception as e:
            logger.error(f"Failed to update file manifest {manifest_id}: {e}")

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

    # Batch lifecycle management methods
    def _start_batch(self, target_table: str, batch_name: str, 
                     file_manifest_id: Optional[str] = None, 
                     table_manifest_id: Optional[str] = None) -> uuid.UUID:
        """Start a new batch for a single target table and return its ID."""
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
                    (batch_audit_id, parent_file_audit_id, entity_name, target_table, status, created_at, started_at, description)
                    VALUES (:batch_id, :parent_file_audit_id, :batch_name, :target_table, :status, :created_at, :started_at, :description)
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
                'subbatch_audit_id': str(subbatch_id),
                'batch_audit_id': str(batch_id),
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
                        entity_name, 
                        table_name, 
                        status, 
                        created_at, 
                        started_at, 
                        description
                    )
                    VALUES (
                        :subbatch_audit_id, 
                        :batch_audit_id, 
                        :table_name, 
                        :table_name, 
                        :status, 
                        :created_at, 
                        :started_at, 
                        :description
                    )
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

    def _complete_batch_with_accumulated_metrics(self, batch_id: uuid.UUID, status: AuditStatus, 
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
                    UPDATE batch_audit_manifest
                    SET 
                        status = :status, 
                        completed_at = :completed_at, 
                        error_message = :error_message,
                        description = description || ' | ' || :metrics_summary
                    WHERE batch_audit_id = :batch_id
                '''), update_data)

            logger.info(f"Completed batch {batch_id} with status: {status.value} in {duration:.1f}s")

        except Exception as e:
            logger.error(f"Failed to complete batch {batch_id}: {e}")

    def _complete_subbatch_with_accumulated_metrics(self, subbatch_id: uuid.UUID, status: AuditStatus, 
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
        try:
            with self.database.engine.connect() as conn:
                result = conn.execute(text('''
                    SELECT 
                        batch_audit_id, 
                        entity_name, 
                        target_table, 
                        status, 
                        started_at, 
                        completed_at, 
                        error_message, 
                        description
                    FROM batch_audit_manifest
                    WHERE batch_audit_id = :batch_id
                '''), {'batch_id': str(batch_id)})
                
                row = result.fetchone()
                return dict(row._mapping) if row else None

        except Exception as e:
            logger.error(f"Failed to get batch status {batch_id}: {e}")
            return None




