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

    def add_file_event(self, status: AuditStatus, rows: int = 0):
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
                self._create_audit_manifest_entry(audit, AuditStatus.FAILED)

                logger.error(f"Failed to insert audit for {audit.entity_name}: {e}")

    def _create_audit_manifest_entry(self, audit, status: AuditStatus) -> None:
        """Create a manifest entry for an audit insertion attempt.
        
        Note: This should NOT create file_ingestion_manifest entries for ZIP files.
        ZIP files are download artifacts, not processed data files.
        Only CSV/Parquet files that are actually processed should go in file_ingestion_manifests.
        """
        try:
            # Extract file information from audit
            file_path = audit.source_files[0]  # Use first filename

            # Skip ZIP files - they shouldn't be in file_ingestion_manifests
            # Only processed CSV/Parquet files should be tracked there
            if file_path.endswith('.zip'):
                logger.debug(f"Skipping manifest entry for ZIP file: {file_path}")
                return

            # Extract table name
            table_name = getattr(audit, 'entity_name', 'unknown')

            # File size tracking moved to file level - default to 0 for table audit entries
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

            # Collect comprehensive file metrics
            try:
                processing_stats = {
                    "rows_processed": rows or 0,
                    "duration_seconds": 0,  # Will be updated during processing
                    "status": status.value if isinstance(status, AuditStatus) else str(status)
                }
                
                comprehensive_file_metrics = self.collect_file_audit_metrics(str(file_path_obj), processing_stats)
                
                # Update file audit with comprehensive metrics
                self.update_file_audit_with_metrics(manifest_id, comprehensive_file_metrics)
                
            except Exception as file_metrics_error:
                logger.warning(f"Failed to collect comprehensive file metrics for {file_path_obj.name}: {file_metrics_error}")

            logger.info(f"Manifest entry created for {file_path_obj.name} (status: {status.value})")
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

                updated_meta = merged
            else:
                updated_meta = notes

            # Update the audit_metadata
            with self.database.engine.begin() as conn:
                conn.execute(text('''
                    UPDATE file_audit_manifest
                    SET notes = :notes, updated_at = :updated_at
                    WHERE file_path = :file_path
                '''), {
                    'file_path': str(file_path_obj),
                    'notes': json.dumps(updated_meta)
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
    def _start_batch(
        self, target_table: str, batch_name: str, 
        file_manifest_id: Optional[str] = None
    ) -> uuid.UUID:
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
                    (batch_audit_id, parent_file_audit_id, target_table, status, created_at, started_at, description)
                    VALUES (:batch_id, :parent_file_audit_id, :target_table, :status, :created_at, :started_at, :description)
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
            
            # Build comprehensive batch statistics
            batch_stats = {
                "start_time": accumulator.start_time,
                "end_time": completed_at.timestamp(),
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
            
            # Cleanup
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
            
            # Build comprehensive subbatch statistics
            subbatch_stats = {
                "start_time": accumulator.start_time,
                "end_time": completed_at.timestamp(),
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
            
            # Cleanup
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

    def collect_comprehensive_column_metrics(self, table_name: str, parquet_file_path: str) -> Dict[str, Any]:
        """
        Collect comprehensive column-level metrics from a Parquet file.
        
        Args:
            table_name: Name of the table
            parquet_file_path: Path to the Parquet file
            
        Returns:
            Dictionary containing comprehensive column metrics
        """
        try:
            from pathlib import Path
            import polars as pl
            
            file_path = Path(parquet_file_path)
            if not file_path.exists():
                logger.warning(f"Parquet file not found: {parquet_file_path}")
                return {}
                
            logger.info(f"Collecting comprehensive metrics for {table_name} from {file_path.name}")
            
            # Read parquet file with Polars for efficient analysis
            df = pl.read_parquet(str(file_path))
            
            total_rows = len(df)
            if total_rows == 0:
                return {
                    "table_name": table_name,
                    "total_rows": 0,
                    "total_columns": len(df.columns),
                    "file_size_bytes": file_path.stat().st_size,
                    "column_metrics": {},
                    "data_quality": {"empty_file": True}
                }
            
            # Calculate comprehensive metrics
            metrics = {
                "table_name": table_name,
                "total_rows": total_rows,
                "total_columns": len(df.columns),
                "file_size_bytes": file_path.stat().st_size,
                "collection_timestamp": datetime.now().isoformat(),
                "column_metrics": {},
                "data_quality": {
                    "empty_file": False,
                    "total_null_values": 0,
                    "completeness_percentage": 0.0
                }
            }
            
            total_null_count = 0
            
            # Analyze each column
            for column in df.columns:
                try:
                    col_series = df[column]
                    
                    # Basic statistics
                    null_count = col_series.null_count()
                    non_null_count = total_rows - null_count
                    completeness = (non_null_count / total_rows * 100) if total_rows > 0 else 0
                    total_null_count += null_count
                    
                    # Column data type
                    col_dtype = str(col_series.dtype)
                    
                    # Unique value analysis
                    unique_count = col_series.n_unique()
                    cardinality_ratio = (unique_count / total_rows * 100) if total_rows > 0 else 0
                    
                    column_metrics = {
                        "data_type": col_dtype,
                        "total_values": total_rows,
                        "non_null_count": non_null_count,
                        "null_count": null_count,
                        "null_percentage": (null_count / total_rows * 100) if total_rows > 0 else 0,
                        "completeness_percentage": completeness,
                        "unique_count": unique_count,
                        "cardinality_ratio": cardinality_ratio,
                        "is_unique": unique_count == non_null_count
                    }
                    
                    # Type-specific analysis
                    if col_dtype in ['Utf8', 'String']:
                        # String column analysis
                        non_null_series = col_series.drop_nulls()
                        if len(non_null_series) > 0:
                            lengths = non_null_series.str.len_chars()
                            column_metrics.update({
                                "min_length": lengths.min(),
                                "max_length": lengths.max(),
                                "avg_length": float(lengths.mean()) if lengths.mean() is not None else 0,
                                "empty_strings": (non_null_series.str.len_chars() == 0).sum(),
                                "pattern_analysis": {
                                    "contains_digits": (non_null_series.str.contains(r'\d')).sum(),
                                    "contains_letters": (non_null_series.str.contains(r'[a-zA-Z]')).sum(),
                                    "contains_special": (non_null_series.str.contains(r'[^a-zA-Z0-9\s]')).sum()
                                }
                            })
                        
                    elif col_dtype in ['Int64', 'Int32', 'Float64', 'Float32']:
                        # Numeric column analysis
                        non_null_series = col_series.drop_nulls()
                        if len(non_null_series) > 0:
                            column_metrics.update({
                                "min_value": float(non_null_series.min()),
                                "max_value": float(non_null_series.max()),
                                "mean_value": float(non_null_series.mean()),
                                "median_value": float(non_null_series.median()),
                                "zero_count": (non_null_series == 0).sum(),
                                "negative_count": (non_null_series < 0).sum() if col_dtype.startswith('Int') or col_dtype.startswith('Float') else 0
                            })
                    
                    # Sample values for reference (first 5 non-null unique values)
                    sample_values = col_series.drop_nulls().unique().head(5).to_list()
                    column_metrics["sample_values"] = sample_values
                    
                    metrics["column_metrics"][column] = column_metrics
                    
                except Exception as col_error:
                    logger.warning(f"Failed to analyze column {column}: {col_error}")
                    metrics["column_metrics"][column] = {
                        "error": str(col_error),
                        "data_type": "unknown"
                    }
            
            # Overall data quality metrics
            total_cells = total_rows * len(df.columns)
            overall_completeness = ((total_cells - total_null_count) / total_cells * 100) if total_cells > 0 else 0
            
            metrics["data_quality"].update({
                "total_null_values": total_null_count,
                "total_cells": total_cells,
                "completeness_percentage": overall_completeness,
                "columns_with_nulls": sum(1 for col_metrics in metrics["column_metrics"].values() 
                                        if col_metrics.get("null_count", 0) > 0),
                "fully_populated_columns": sum(1 for col_metrics in metrics["column_metrics"].values() 
                                             if col_metrics.get("null_count", 0) == 0)
            })
            
            logger.info(f"Collected metrics for {table_name}: {total_rows:,} rows, {len(df.columns)} columns, {overall_completeness:.1f}% complete")
            return metrics
            
        except Exception as e:
            logger.error(f"Failed to collect comprehensive metrics for {table_name}: {e}")
            return {
                "table_name": table_name,
                "error": str(e),
                "collection_timestamp": datetime.now().isoformat()
            }

    def update_table_audit_with_comprehensive_metrics(
        self, table_name: str, comprehensive_metrics: Dict[str, Any]
    ) -> None:
        """
        Update table audit record with comprehensive column metrics.
        
        Args:
            table_name: Name of the table
            comprehensive_metrics: Detailed metrics collected from the data
        """
        try:
            import json
            
            # Find the most recent table audit for this table
            table_audit_id = self._find_table_audit(table_name)
            if not table_audit_id:
                logger.warning(f"No table audit found for {table_name} - cannot update with metrics")
                return
            
            # Update the table audit with comprehensive metrics
            with self.database.engine.begin() as conn:
                conn.execute(text('''
                    UPDATE table_audit_manifest 
                    SET metrics = :metrics,
                        notes = COALESCE(notes, '{}')::jsonb || (:additional_notes)::jsonb
                    WHERE table_audit_id = :table_audit_id
                '''), {
                    'metrics': json.dumps(comprehensive_metrics),
                    'additional_notes': json.dumps({
                        "comprehensive_metrics_updated": True,
                        "metrics_collection_timestamp": comprehensive_metrics.get("collection_timestamp"),
                        "data_quality_score": comprehensive_metrics.get("data_quality", {}).get("completeness_percentage", 0)
                    }),
                    'table_audit_id': str(table_audit_id)
                })
                
            logger.info(f"Updated table audit for {table_name} with comprehensive metrics")
            
        except Exception as e:
            logger.error(f"Failed to update table audit with comprehensive metrics for {table_name}: {e}")

    def get_table_metrics_summary(self) -> Dict[str, Any]:
        """
        Get a summary of metrics across all tables in the audit system.
        
        Returns:
            Dictionary containing summary statistics
        """
        try:
            with self.database.engine.connect() as conn:
                # Get basic table counts
                result = conn.execute(text('''
                    SELECT 
                        COUNT(*) as total_tables,
                        COUNT(CASE WHEN status = 'COMPLETED' THEN 1 END) as completed_tables,
                        COUNT(CASE WHEN metrics IS NOT NULL AND metrics != '{}' THEN 1 END) as tables_with_metrics,
                        SUM(CASE 
                            WHEN metrics IS NOT NULL 
                            AND JSON_EXTRACT_PATH_TEXT(metrics, 'total_rows') IS NOT NULL
                            THEN CAST(JSON_EXTRACT_PATH_TEXT(metrics, 'total_rows') AS INTEGER)
                            ELSE 0 
                        END) as total_rows_across_tables
                    FROM table_audit_manifest
                    WHERE ingestion_year = :year AND ingestion_month = :month
                '''), {'year': self.config.year, 'month': self.config.month})
                
                summary_row = result.fetchone()
                
                summary = {
                    "audit_summary": {
                        "total_tables": summary_row[0] if summary_row else 0,
                        "completed_tables": summary_row[1] if summary_row else 0,
                        "tables_with_metrics": summary_row[2] if summary_row else 0,
                        "total_rows_across_tables": summary_row[3] if summary_row else 0,
                        "reporting_period": f"{self.config.year}-{self.config.month:02d}"
                    },
                    "collection_timestamp": datetime.now().isoformat()
                }
                
                return summary
                
        except Exception as e:
            logger.error(f"Failed to generate metrics summary: {e}")
            return {
                "error": str(e),
                "collection_timestamp": datetime.now().isoformat()
            }

    def collect_file_audit_metrics(self, file_path: str, processing_stats: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Collect comprehensive metrics for file-level audit tracking.
        
        Args:
            file_path: Path to the processed file
            processing_stats: Optional processing statistics (rows, duration, etc.)
            
        Returns:
            Dictionary containing file-level metrics
        """
        try:
            from pathlib import Path
            
            file_obj = Path(file_path)
            
            # Basic file information
            file_metrics = {
                "file_name": file_obj.name,
                "file_path": str(file_obj),
                "file_extension": file_obj.suffix.lower(),
                "collection_timestamp": datetime.now().isoformat()
            }
            
            # File size and existence check
            if file_obj.exists():
                stat_info = file_obj.stat()
                file_metrics.update({
                    "file_size_bytes": stat_info.st_size,
                    "file_size_mb": round(stat_info.st_size / (1024 * 1024), 2),
                    "last_modified": datetime.fromtimestamp(stat_info.st_mtime).isoformat(),
                    "file_exists": True
                })
                
                # File format specific analysis
                if file_obj.suffix.lower() == '.parquet':
                    try:
                        import polars as pl
                        df = pl.read_parquet(str(file_obj))
                        file_metrics.update({
                            "format_type": "parquet",
                            "row_count": len(df),
                            "column_count": len(df.columns),
                            "column_names": df.columns,
                            "estimated_memory_mb": round(df.estimated_size() / (1024 * 1024), 2) if hasattr(df, 'estimated_size') else None,
                            "data_completeness": {
                                "total_cells": len(df) * len(df.columns),
                                "null_cells": sum(df[col].null_count() for col in df.columns) if len(df) > 0 else 0
                            }
                        })
                    except Exception as parquet_error:
                        file_metrics["parquet_analysis_error"] = str(parquet_error)
                        
                elif file_obj.suffix.lower() == '.csv':
                    try:
                        # Quick CSV analysis without loading full file
                        with open(file_obj, 'r', encoding='utf-8') as f:
                            first_line = f.readline().strip()
                            f.seek(0, 2)  # Go to end
                            estimated_lines = f.tell() / len(first_line) if len(first_line) > 0 else 0
                            
                        file_metrics.update({
                            "format_type": "csv",
                            "estimated_rows": int(estimated_lines),
                            "sample_first_line": first_line[:100],  # First 100 chars
                            "delimiter_detected": ";" if ";" in first_line else "," if "," in first_line else "unknown"
                        })
                    except Exception as csv_error:
                        file_metrics["csv_analysis_error"] = str(csv_error)
            else:
                file_metrics.update({
                    "file_size_bytes": 0,
                    "file_exists": False
                })
            
            # Processing statistics if provided
            if processing_stats:
                file_metrics["processing_stats"] = {
                    "rows_processed": processing_stats.get("rows_processed", 0),
                    "processing_duration_seconds": processing_stats.get("duration_seconds", 0),
                    "processing_throughput_rows_per_sec": (
                        processing_stats.get("rows_processed", 0) / max(processing_stats.get("duration_seconds", 1), 1)
                    ),
                    "errors_encountered": processing_stats.get("error_count", 0),
                    "warnings_encountered": processing_stats.get("warning_count", 0),
                    "processing_status": processing_stats.get("status", "unknown")
                }
            
            # Performance classifications
            if file_metrics.get("file_size_bytes", 0) > 0:
                size_bytes = file_metrics["file_size_bytes"]
                if size_bytes < 1024 * 1024:  # < 1MB
                    size_category = "small"
                elif size_bytes < 100 * 1024 * 1024:  # < 100MB
                    size_category = "medium"
                elif size_bytes < 1024 * 1024 * 1024:  # < 1GB
                    size_category = "large"
                else:
                    size_category = "very_large"
                    
                file_metrics["performance_classification"] = {
                    "size_category": size_category,
                    "complexity_estimate": "low" if file_metrics.get("column_count", 0) < 10 else "medium" if file_metrics.get("column_count", 0) < 50 else "high"
                }
            
            return file_metrics
            
        except Exception as e:
            logger.error(f"Failed to collect file audit metrics for {file_path}: {e}")
            return {
                "file_path": file_path,
                "error": str(e),
                "collection_timestamp": datetime.now().isoformat()
            }

    def collect_batch_audit_metrics(self, batch_stats: Dict[str, Any]) -> Dict[str, Any]:
        """
        Collect comprehensive metrics for batch-level audit tracking.
        
        Args:
            batch_stats: Batch processing statistics
            
        Returns:
            Dictionary containing batch-level metrics
        """
        try:
            import time
            start_time = batch_stats.get("start_time", time.time())
            end_time = batch_stats.get("end_time", time.time())
            duration = max(end_time - start_time, 0)
            
            batch_metrics = {
                "batch_summary": {
                    "total_files_processed": batch_stats.get("files_processed", 0),
                    "total_files_failed": batch_stats.get("files_failed", 0),
                    "total_rows_processed": batch_stats.get("total_rows", 0),
                    "total_bytes_processed": batch_stats.get("total_bytes", 0),
                    "processing_duration_seconds": duration,
                    "success_rate_percentage": (
                        (batch_stats.get("files_processed", 0) / max(batch_stats.get("files_processed", 0) + batch_stats.get("files_failed", 0), 1)) * 100
                    )
                },
                "performance_metrics": {
                    "avg_rows_per_second": batch_stats.get("total_rows", 0) / max(duration, 1),
                    "avg_bytes_per_second": batch_stats.get("total_bytes", 0) / max(duration, 1),
                    "avg_files_per_minute": (batch_stats.get("files_processed", 0) + batch_stats.get("files_failed", 0)) / max(duration / 60, 1),
                    "memory_peak_mb": batch_stats.get("memory_peak_mb", 0),
                    "memory_efficiency": batch_stats.get("memory_efficiency_score", "unknown")
                },
                "quality_metrics": {
                    "data_quality_issues": batch_stats.get("data_quality_issues", 0),
                    "schema_validation_failures": batch_stats.get("schema_failures", 0),
                    "data_completeness_average": batch_stats.get("avg_completeness", 0),
                    "duplicate_records_found": batch_stats.get("duplicates_found", 0)
                },
                "resource_utilization": {
                    "cpu_usage_peak_percent": batch_stats.get("cpu_peak_percent", 0),
                    "io_operations": batch_stats.get("io_operations", 0),
                    "network_bytes_transferred": batch_stats.get("network_bytes", 0),
                    "temp_storage_used_mb": batch_stats.get("temp_storage_mb", 0)
                },
                "collection_timestamp": datetime.now().isoformat()
            }
            
            # Add efficiency ratings
            throughput_rating = "high" if batch_metrics["performance_metrics"]["avg_rows_per_second"] > 1000 else \
                               "medium" if batch_metrics["performance_metrics"]["avg_rows_per_second"] > 100 else "low"
                               
            batch_metrics["efficiency_rating"] = {
                "throughput_rating": throughput_rating,
                "success_rating": "excellent" if batch_metrics["batch_summary"]["success_rate_percentage"] > 95 else \
                                 "good" if batch_metrics["batch_summary"]["success_rate_percentage"] > 85 else "needs_improvement",
                "overall_score": min(100, max(0, 
                    (batch_metrics["batch_summary"]["success_rate_percentage"] * 0.6) + 
                    (min(batch_metrics["performance_metrics"]["avg_rows_per_second"] / 1000 * 40, 40))
                ))
            }
            
            return batch_metrics
            
        except Exception as e:
            logger.error(f"Failed to collect batch audit metrics: {e}")
            return {
                "error": str(e),
                "collection_timestamp": datetime.now().isoformat()
            }

    def collect_subbatch_audit_metrics(self, subbatch_stats: Dict[str, Any]) -> Dict[str, Any]:
        """
        Collect comprehensive metrics for subbatch-level audit tracking.
        
        Args:
            subbatch_stats: Subbatch processing statistics
            
        Returns:
            Dictionary containing subbatch-level metrics
        """
        try:
            import time
            start_time = subbatch_stats.get("start_time", time.time())
            end_time = subbatch_stats.get("end_time", time.time())
            duration = max(end_time - start_time, 0)
            
            subbatch_metrics = {
                "processing_summary": {
                    "table_name": subbatch_stats.get("table_name", "unknown"),
                    "rows_processed": subbatch_stats.get("rows_processed", 0),
                    "processing_duration_seconds": duration,
                    "processing_stage": subbatch_stats.get("processing_stage", "unknown"),
                    "operation_type": subbatch_stats.get("operation_type", "upsert")  # insert, update, upsert, delete
                },
                "data_operation_metrics": {
                    "rows_inserted": subbatch_stats.get("rows_inserted", 0),
                    "rows_updated": subbatch_stats.get("rows_updated", 0),
                    "rows_skipped": subbatch_stats.get("rows_skipped", 0),
                    "rows_failed": subbatch_stats.get("rows_failed", 0),
                    "duplicate_rows_handled": subbatch_stats.get("duplicates_handled", 0),
                    "constraint_violations": subbatch_stats.get("constraint_violations", 0)
                },
                "performance_details": {
                    "rows_per_second": subbatch_stats.get("rows_processed", 0) / max(duration, 1),
                    "avg_row_size_bytes": subbatch_stats.get("avg_row_size", 0),
                    "database_connection_time_ms": subbatch_stats.get("db_connection_time_ms", 0),
                    "query_execution_time_ms": subbatch_stats.get("query_execution_time_ms", 0),
                    "data_transformation_time_ms": subbatch_stats.get("transformation_time_ms", 0)
                },
                "data_quality": {
                    "null_value_count": subbatch_stats.get("null_values", 0),
                    "data_type_conversions": subbatch_stats.get("type_conversions", 0),
                    "validation_errors": subbatch_stats.get("validation_errors", 0),
                    "data_cleansing_applied": subbatch_stats.get("cleansing_operations", []),
                    "completeness_percentage": subbatch_stats.get("completeness_percent", 0)
                },
                "error_analysis": {
                    "error_types": subbatch_stats.get("error_types", {}),
                    "warning_count": subbatch_stats.get("warning_count", 0),
                    "retry_attempts": subbatch_stats.get("retry_attempts", 0),
                    "recovery_actions": subbatch_stats.get("recovery_actions", [])
                },
                "collection_timestamp": datetime.now().isoformat()
            }
            
            # Calculate efficiency scores
            if subbatch_metrics["processing_summary"]["rows_processed"] > 0:
                success_rate = (
                    (subbatch_metrics["data_operation_metrics"]["rows_inserted"] + 
                     subbatch_metrics["data_operation_metrics"]["rows_updated"]) / 
                    subbatch_metrics["processing_summary"]["rows_processed"] * 100
                )
                
                subbatch_metrics["quality_score"] = {
                    "processing_success_rate": success_rate,
                    "data_quality_score": max(0, 100 - subbatch_metrics["data_quality"]["validation_errors"] * 5),
                    "performance_score": min(100, subbatch_metrics["performance_details"]["rows_per_second"] / 10),
                    "overall_health": "excellent" if success_rate > 98 and subbatch_metrics["data_quality"]["validation_errors"] == 0 else \
                                   "good" if success_rate > 90 else "needs_attention"
                }
            
            return subbatch_metrics
            
        except Exception as e:
            logger.error(f"Failed to collect subbatch audit metrics: {e}")
            return {
                "error": str(e),
                "collection_timestamp": datetime.now().isoformat()
            }

    def update_file_audit_with_metrics(self, file_audit_id: str, file_metrics: Dict[str, Any]) -> None:
        """Update file audit record with comprehensive metrics."""
        try:
            import json
            
            with self.database.engine.begin() as conn:
                conn.execute(text('''
                    UPDATE file_audit_manifest 
                    SET metrics = :metrics,
                        notes = COALESCE(notes, '{}')::jsonb || (:additional_notes)::jsonb
                    WHERE file_audit_id = :file_audit_id
                '''), {
                    'metrics': json.dumps(file_metrics),
                    'additional_notes': json.dumps({
                        "file_metrics_updated": True,
                        "metrics_collection_timestamp": file_metrics.get("collection_timestamp"),
                        "file_size_category": file_metrics.get("performance_classification", {}).get("size_category", "unknown")
                    }),
                    'file_audit_id': file_audit_id
                })
                
            logger.info(f"Updated file audit {file_audit_id} with comprehensive metrics")
            
        except Exception as e:
            logger.error(f"Failed to update file audit {file_audit_id} with metrics: {e}")

    def update_batch_audit_with_metrics(self, batch_audit_id: str, batch_metrics: Dict[str, Any]) -> None:
        """Update batch audit record with comprehensive metrics."""
        try:
            import json
            
            with self.database.engine.begin() as conn:
                conn.execute(text('''
                    UPDATE batch_audit_manifest 
                    SET metrics = :metrics,
                        notes = COALESCE(notes, '{}')::jsonb || (:additional_notes)::jsonb
                    WHERE batch_audit_id = :batch_audit_id
                '''), {
                    'metrics': json.dumps(batch_metrics),
                    'additional_notes': json.dumps({
                        "batch_metrics_updated": True,
                        "metrics_collection_timestamp": batch_metrics.get("collection_timestamp"),
                        "success_rate": batch_metrics.get("batch_summary", {}).get("success_rate_percentage", 0),
                        "efficiency_rating": batch_metrics.get("efficiency_rating", {}).get("overall_score", 0)
                    }),
                    'updated_at': datetime.now(),
                    'batch_audit_id': batch_audit_id
                })
                
            logger.info(f"Updated batch audit {batch_audit_id} with comprehensive metrics")
            
        except Exception as e:
            logger.error(f"Failed to update batch audit {batch_audit_id} with metrics: {e}")

    def update_subbatch_audit_with_metrics(self, subbatch_audit_id: str, subbatch_metrics: Dict[str, Any]) -> None:
        """Update subbatch audit record with comprehensive metrics."""
        try:
            import json
            
            with self.database.engine.begin() as conn:
                conn.execute(text('''
                    UPDATE subbatch_audit_manifest 
                    SET metrics = :metrics,
                        notes = COALESCE(notes, '{}')::jsonb || (:additional_notes)::jsonb
                    WHERE subbatch_audit_id = :subbatch_audit_id
                '''), {
                    'metrics': json.dumps(subbatch_metrics),
                    'additional_notes': json.dumps({
                        "subbatch_metrics_updated": True,
                        "metrics_collection_timestamp": subbatch_metrics.get("collection_timestamp"),
                        "processing_health": subbatch_metrics.get("quality_score", {}).get("overall_health", "unknown"),
                        "rows_per_second": subbatch_metrics.get("performance_details", {}).get("rows_per_second", 0)
                    }),
                    'updated_at': datetime.now(),
                    'subbatch_audit_id': subbatch_audit_id
                })
                
            logger.info(f"Updated subbatch audit {subbatch_audit_id} with comprehensive metrics")
            
        except Exception as e:
            logger.error(f"Failed to update subbatch audit {subbatch_audit_id} with metrics: {e}")

    def get_comprehensive_audit_summary(self) -> Dict[str, Any]:
        """
        Get a comprehensive summary of all audit metrics across all manifest levels.
        
        Returns:
            Dictionary containing cross-level audit summary
        """
        try:
            with self.database.engine.connect() as conn:
                # Table-level metrics
                table_result = conn.execute(text('''
                    SELECT 
                        COUNT(*) as total_tables,
                        COUNT(CASE WHEN status = 'COMPLETED' THEN 1 END) as completed_tables,
                        COUNT(CASE WHEN metrics IS NOT NULL AND metrics != '{}' THEN 1 END) as tables_with_metrics,
                        AVG(CASE 
                            WHEN metrics IS NOT NULL 
                            AND JSON_EXTRACT_PATH_TEXT(metrics, 'data_quality', 'completeness_percentage') IS NOT NULL
                            THEN CAST(JSON_EXTRACT_PATH_TEXT(metrics, 'data_quality', 'completeness_percentage') AS FLOAT)
                            ELSE NULL 
                        END) as avg_data_completeness
                    FROM table_audit_manifest
                    WHERE ingestion_year = :year AND ingestion_month = :month
                '''), {'year': self.config.year, 'month': self.config.month})
                
                table_summary = table_result.fetchone()
                
                # File-level metrics
                file_result = conn.execute(text('''
                    SELECT 
                        COUNT(*) as total_files,
                        COUNT(CASE WHEN status = 'COMPLETED' THEN 1 END) as completed_files,
                        COUNT(CASE WHEN metrics IS NOT NULL AND metrics != '{}' THEN 1 END) as files_with_metrics,
                        SUM(CASE 
                            WHEN metrics IS NOT NULL 
                            AND JSON_EXTRACT_PATH_TEXT(metrics, 'file_size_bytes') IS NOT NULL
                            THEN CAST(JSON_EXTRACT_PATH_TEXT(metrics, 'file_size_bytes') AS BIGINT)
                            ELSE 0 
                        END) as total_file_size_bytes
                    FROM file_audit_manifest f
                    JOIN table_audit_manifest t ON f.parent_table_audit_id = t.table_audit_id
                    WHERE t.ingestion_year = :year AND t.ingestion_month = :month
                '''), {'year': self.config.year, 'month': self.config.month})
                
                file_summary = file_result.fetchone()
                
                # Batch-level metrics
                batch_result = conn.execute(text('''
                    SELECT 
                        COUNT(*) as total_batches,
                        COUNT(CASE WHEN status = 'COMPLETED' THEN 1 END) as completed_batches,
                        COUNT(CASE WHEN metrics IS NOT NULL AND metrics != '{}' THEN 1 END) as batches_with_metrics,
                        AVG(CASE 
                            WHEN metrics IS NOT NULL 
                            AND JSON_EXTRACT_PATH_TEXT(metrics, 'batch_summary', 'success_rate_percentage') IS NOT NULL
                            THEN CAST(JSON_EXTRACT_PATH_TEXT(metrics, 'batch_summary', 'success_rate_percentage') AS FLOAT)
                            ELSE NULL 
                        END) as avg_success_rate
                    FROM batch_audit_manifest b
                    JOIN file_audit_manifest f ON b.parent_file_audit_id = f.file_audit_id
                    JOIN table_audit_manifest t ON f.parent_table_audit_id = t.table_audit_id
                    WHERE t.ingestion_year = :year AND t.ingestion_month = :month
                '''), {'year': self.config.year, 'month': self.config.month})
                
                batch_summary = batch_result.fetchone()
                
                # Subbatch-level metrics
                subbatch_result = conn.execute(text('''
                    SELECT 
                        COUNT(*) as total_subbatches,
                        COUNT(CASE WHEN status = 'COMPLETED' THEN 1 END) as completed_subbatches,
                        COUNT(CASE WHEN metrics IS NOT NULL AND metrics != '{}' THEN 1 END) as subbatches_with_metrics,
                        SUM(CASE WHEN rows_processed IS NOT NULL THEN rows_processed ELSE 0 END) as total_rows_processed
                    FROM subbatch_audit_manifest s
                    JOIN batch_audit_manifest b ON s.parent_batch_audit_id = b.batch_audit_id
                    JOIN file_audit_manifest f ON b.parent_file_audit_id = f.file_audit_id
                    JOIN table_audit_manifest t ON f.parent_table_audit_id = t.table_audit_id
                    WHERE t.ingestion_year = :year AND t.ingestion_month = :month
                '''), {'year': self.config.year, 'month': self.config.month})
                
                subbatch_summary = subbatch_result.fetchone()
                
                comprehensive_summary = {
                    "reporting_period": f"{self.config.year}-{self.config.month:02d}",
                    "collection_timestamp": datetime.now().isoformat(),
                    "table_level_summary": {
                        "total_tables": table_summary[0] if table_summary else 0,
                        "completed_tables": table_summary[1] if table_summary else 0,
                        "tables_with_metrics": table_summary[2] if table_summary else 0,
                        "avg_data_completeness": round(table_summary[3], 2) if table_summary and table_summary[3] else 0
                    },
                    "file_level_summary": {
                        "total_files": file_summary[0] if file_summary else 0,
                        "completed_files": file_summary[1] if file_summary else 0,
                        "files_with_metrics": file_summary[2] if file_summary else 0,
                        "total_file_size_gb": round((file_summary[3] if file_summary else 0) / (1024**3), 2)
                    },
                    "batch_level_summary": {
                        "total_batches": batch_summary[0] if batch_summary else 0,
                        "completed_batches": batch_summary[1] if batch_summary else 0,
                        "batches_with_metrics": batch_summary[2] if batch_summary else 0,
                        "avg_success_rate": round(batch_summary[3], 2) if batch_summary and batch_summary[3] else 0
                    },
                    "subbatch_level_summary": {
                        "total_subbatches": subbatch_summary[0] if subbatch_summary else 0,
                        "completed_subbatches": subbatch_summary[1] if subbatch_summary else 0,
                        "subbatches_with_metrics": subbatch_summary[2] if subbatch_summary else 0,
                        "total_rows_processed": subbatch_summary[3] if subbatch_summary else 0
                    }
                }
                
                # Calculate overall health metrics
                total_operations = (comprehensive_summary["table_level_summary"]["total_tables"] + 
                                  comprehensive_summary["file_level_summary"]["total_files"] +
                                  comprehensive_summary["batch_level_summary"]["total_batches"] +
                                  comprehensive_summary["subbatch_level_summary"]["total_subbatches"])
                
                completed_operations = (comprehensive_summary["table_level_summary"]["completed_tables"] + 
                                      comprehensive_summary["file_level_summary"]["completed_files"] +
                                      comprehensive_summary["batch_level_summary"]["completed_batches"] +
                                      comprehensive_summary["subbatch_level_summary"]["completed_subbatches"])
                
                comprehensive_summary["overall_health"] = {
                    "total_operations": total_operations,
                    "completed_operations": completed_operations,
                    "overall_success_rate": round((completed_operations / max(total_operations, 1)) * 100, 2),
                    "metrics_coverage": {
                        "tables": round((comprehensive_summary["table_level_summary"]["tables_with_metrics"] / 
                                       max(comprehensive_summary["table_level_summary"]["total_tables"], 1)) * 100, 2),
                        "files": round((comprehensive_summary["file_level_summary"]["files_with_metrics"] / 
                                      max(comprehensive_summary["file_level_summary"]["total_files"], 1)) * 100, 2),
                        "batches": round((comprehensive_summary["batch_level_summary"]["batches_with_metrics"] / 
                                        max(comprehensive_summary["batch_level_summary"]["total_batches"], 1)) * 100, 2),
                        "subbatches": round((comprehensive_summary["subbatch_level_summary"]["subbatches_with_metrics"] / 
                                           max(comprehensive_summary["subbatch_level_summary"]["total_subbatches"], 1)) * 100, 2)
                    }
                }
                
                return comprehensive_summary
                
        except Exception as e:
            logger.error(f"Failed to generate comprehensive audit summary: {e}")
            return {
                "error": str(e),
                "collection_timestamp": datetime.now().isoformat()
            }




