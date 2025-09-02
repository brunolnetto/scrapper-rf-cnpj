"""
Unified audit management service for the CNPJ ETL project.

This service centralizes audit creation, validation, and insertion logic for use by ETL and data loading components.
Includes comprehensive manifest tracking and file integrity verification capabilities.
"""

import hashlib
import os
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from ....setup.logging import logger
from ....database.engine import Database
from ....database.models import AuditDB
from ....database.utils.models import create_audits, create_audit_metadata, insert_audit
from ...schemas import AuditMetadata, FileInfo, FileGroupInfo
from ...utils.schemas import create_file_groups


class AuditService:
    """Unified audit management service with comprehensive manifest tracking."""

    def __init__(self, database: Database, config=None):
        self.database = database
        self.config = config
        self.manifest_enabled = config.etl.manifest_tracking if config else False
        logger.info(f"Audit service initialized (manifest: {self.manifest_enabled})")

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
                if self.manifest_enabled:
                    self._create_audit_manifest_entry(audit, "success")

                logger.info(f"Successfully inserted audit for {audit.audi_table_name}")

            except Exception as e:
                # ❌ FAILURE: Create manifest entry for failed insertion
                if self.manifest_enabled:
                    self._create_audit_manifest_entry(audit, "failed", error_message=str(e))

                logger.error(f"Failed to insert audit for {audit.audi_table_name}: {e}")

    def _create_audit_manifest_entry(self, audit, status: str, error_message: str = None) -> None:
        """Create a manifest entry for an audit insertion attempt."""
        try:
            # Extract file information from audit
            file_path = "unknown"
            if hasattr(audit, 'audi_filenames') and audit.audi_filenames:
                file_path = audit.audi_filenames[0]  # Use first filename

            # Extract table name
            table_name = getattr(audit, 'audi_table_name', 'unknown')

            # Extract file size if available
            filesize = getattr(audit, 'audi_file_size_bytes', 0)
            if filesize is None:
                filesize = 0

            # Create manifest entry with table name
            self._insert_manifest_entry(
                table_name=table_name,
                file_path=file_path,
                filename=Path(file_path).name if file_path != "unknown" else f"audit_{table_name}",
                status=status,
                checksum=None,  # Audits don't have checksums
                filesize=int(filesize),
                rows_processed=0,  # Audit insertions don't have row counts
                processed_at=datetime.now()
            )

            logger.debug(f"Created {status} manifest entry for audit: {table_name}")

        except Exception as e:
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
        Create audit records from file groups using the database.
        """
        return create_audits(self.database, file_groups)

    # Manifest tracking capabilities
    def create_file_manifest(self, file_path: str, status: str, checksum: Optional[str] = None,
                           filesize: Optional[int] = None, rows: Optional[int] = None,
                           table_name: str = 'unknown', notes: Optional[str] = None) -> None:
        """Create manifest entry for processed file."""
        if not self.manifest_enabled:
            logger.debug(f"Manifest tracking disabled, skipping {Path(file_path).name}")
            return

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

            # Insert manifest entry
            self._insert_manifest_entry(
                table_name=table_name,
                file_path=str(file_path_obj),
                filename=file_path_obj.name,
                status=status,
                checksum=checksum,
                filesize=filesize,
                rows_processed=rows,
                processed_at=datetime.now(),
                notes=notes
            )

            logger.info(f"Manifest entry created for {file_path_obj.name} (status: {status})")

        except Exception as e:
            logger.error(f"Failed to create manifest entry for {file_path}: {e}")

    def _calculate_file_checksum(self, file_path: Path, algorithm: str = 'sha256') -> str:
        """Calculate file checksum for integrity verification."""
        try:
            # Skip checksum for very large files (> 1GB) to avoid performance issues
            filesize = file_path.stat().st_size
            checksum_threshold = int(os.getenv("ETL_CHECKSUM_THRESHOLD_BYTES", "1000000000"))  # 1GB default
            
            if filesize > checksum_threshold:
                logger.info(f"[PERFORMANCE] Skipping checksum calculation for large file {file_path.name}: {filesize:,} bytes (> {checksum_threshold:,})")
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

    def _insert_manifest_entry(self, table_name: str, file_path: str, filename: str, status: str,
                              checksum: Optional[str], filesize: Optional[int],
                              rows_processed: Optional[int], processed_at: datetime, 
                              notes: Optional[str] = None) -> None:
        """Insert manifest entry into database with upsert logic."""
        try:
            from sqlalchemy import text
            import uuid

            insert_query = '''
            INSERT INTO file_ingestion_manifest
            (manifest_id, table_name, file_path, status, checksum, filesize, rows_processed, processed_at, notes)
            VALUES (:manifest_id, :table_name, :file_path, :status, :checksum, :filesize, :rows_processed, :processed_at, :notes)
            '''

            with self.database.engine.begin() as conn:
                conn.execute(text(insert_query), {
                    'manifest_id': str(uuid.uuid4()),
                    'table_name': table_name,
                    'file_path': file_path,
                    'status': status,
                    'checksum': checksum,
                    'filesize': filesize,
                    'rows_processed': rows_processed,
                    'processed_at': processed_at,
                    'notes': notes
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
        if not self.manifest_enabled:
            return True

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
        if not self.manifest_enabled:
            logger.debug("Manifest tracking disabled, skipping notes update")
            return

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




