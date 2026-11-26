"""File-level audits (file_audit_manifest): manifests, checksums and integrity checks.

Mixin of :class:`AuditService`; relies on attributes set in its ``__init__``.
"""

import hashlib
import os
import uuid
from concurrent.futures import as_completed
from pathlib import Path
from typing import Any, Dict, List, Optional

from ....database.models.audit import AuditStatus
from ....setup.logging import logger


class FileAuditMixin:
    """File-level audits (file_audit_manifest): manifests, checksums and integrity checks."""

    def create_file_manifest(
        self, file_path: str, status: AuditStatus, table_manifest_id: str, 
        checksum: Optional[str] = None, filesize: Optional[int] = None, 
        rows: Optional[int] = None, table_name: str = 'unknown', 
        notes: Optional[str] = None
    ) -> str:
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
            file_manifest_id = str(uuid.uuid4())

            # Insert manifest entry
            self.repository.insert_file_manifest(
                file_manifest_id=file_manifest_id,
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
                self.update_file_audit_with_metrics(file_manifest_id, comprehensive_file_metrics)
                
            except Exception as file_metrics_error:
                logger.warning(f"Failed to collect comprehensive file metrics for {file_path_obj.name}: {file_metrics_error}")

            logger.info(f"Manifest entry created for {file_path_obj.name} (status: {status.value})")
            return file_manifest_id

        except Exception as e:
            logger.error(f"Failed to create manifest entry for {file_path}: {e}")
            return ""

    def update_file_manifest(
        self,
        file_manifest_id: Optional[str] = None,
        manifest_id: Optional[str] = None,
        status: AuditStatus = AuditStatus.PENDING,
        rows_processed: Optional[int] = None,
        error_msg: Optional[str] = None,
        notes: Optional[dict] = None,
    ) -> None:
        """Update an existing file manifest entry with completion details."""
        try:
            # Normalize manifest id (accept either keyword)
            manifest = manifest_id or file_manifest_id
            
            # Update via repository
            self.repository.update_file_manifest(
                file_manifest_id=manifest,
                status=status,
                rows_processed=rows_processed,
                error_msg=error_msg,
                notes=notes
            )
            
            # Collect metrics for batch accumulation if this is a completion
            if status in (AuditStatus.COMPLETED, AuditStatus.FAILED) and rows_processed is not None:
                subbatch_id = self.repository.find_subbatch_for_file_manifest(manifest)
                if subbatch_id:
                    self.collect_file_processing_event(subbatch_id, status, int(rows_processed or 0), 0)
                    logger.debug(f"Collected file metrics: {status}, {rows_processed} rows")
                        
            logger.debug(f"Updated manifest {manifest} with status {status}")
            
        except Exception as e:
            logger.error(f"Error updating file manifest {file_manifest_id}: {e}")
            raise

    def update_file_manifest_metadata(
        self,
        file_manifest_id: str,
        checksum: Optional[str] = None,
        filesize: Optional[int] = None,
        notes: Optional[str] = None
    ) -> None:
        """Update checksum/filesize details without affecting status."""
        try:
            self.repository.update_file_manifest_metadata(
                file_manifest_id=file_manifest_id,
                checksum=checksum,
                filesize=filesize,
                notes=notes
            )
            logger.debug(f"Updated manifest metadata for {file_manifest_id}")
        except Exception as e:
            logger.warning(f"Failed to update manifest metadata for {file_manifest_id}: {e}")

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
        except (OSError, IOError, PermissionError) as e:
            logger.error(f"File access error calculating checksum for {file_path}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error calculating checksum for {file_path}: {e}")
            return None

    def get_file_manifest_history(self, filename: str) -> List[dict]:
        """Get manifest history for a specific file."""
        return self.repository.get_file_manifest_history(filename)

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

    def batch_create_file_manifests_async(
        self, 
        manifest_data_list: List[Dict[str, Any]], 
        max_workers: Optional[int] = None
    ) -> List[uuid.UUID]:
        """Create multiple file manifests concurrently using thread pool."""
        if not self._async_enabled or not self._executor:
            # Fallback to synchronous processing
            return [self.create_file_manifest(**data) for data in manifest_data_list]
        
        max_workers = max_workers or self._executor._max_workers
        manifest_ids = []
        
        # Submit all tasks
        futures = []
        for data in manifest_data_list:
            future = self._executor.submit(self.create_file_manifest, **data)
            futures.append(future)
        
        # Collect results as they complete
        for future in as_completed(futures):
            try:
                manifest_id = future.result(timeout=self.timeout_config.operation_timeout_seconds)
                manifest_ids.append(manifest_id)
            except Exception as e:
                logger.error(f"Failed to create file manifest in async batch: {e}")
                # Continue processing other manifests
        
        return manifest_ids

    def batch_update_file_manifests_async(
        self, 
        update_data_list: List[Dict[str, Any]], 
        max_workers: Optional[int] = None
    ) -> int:
        """Update multiple file manifests concurrently using thread pool."""
        if not self._async_enabled or not self._executor:
            # Fallback to synchronous processing
            success_count = 0
            for data in update_data_list:
                try:
                    self.update_file_manifest(**data)
                    success_count += 1
                except Exception as e:
                    logger.error(f"Failed to update file manifest: {e}")
            return success_count
        
        max_workers = max_workers or self._executor._max_workers
        success_count = 0
        
        # Submit all tasks
        futures = []
        for data in update_data_list:
            future = self._executor.submit(self._safe_update_file_manifest, **data)
            futures.append(future)
        
        # Collect results as they complete
        for future in as_completed(futures):
            try:
                if future.result(timeout=self.timeout_config.operation_timeout_seconds):
                    success_count += 1
            except Exception as e:
                logger.error(f"Failed to update file manifest in async batch: {e}")
        
        return success_count

    def _safe_update_file_manifest(self, **kwargs) -> bool:
        """Thread-safe wrapper for update_file_manifest that returns success status."""
        try:
            self.update_file_manifest(**kwargs)
            return True
        except Exception as e:
            logger.error(f"Safe update file manifest failed: {e}")
            return False
