"""
Unified audit management service for the CNPJ ETL project.

This service centralizes audit creation, validation, and insertion logic for use by ETL and data loading components.
Includes manifest tracking and file integrity verification capabilities.
"""

import hashlib
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from ...setup.logging import logger
from ...database.schemas import Database
from ...database.models import AuditDB
from ...database.utils.models import create_audits, create_audit_metadata, insert_audit
from ..schemas import AuditMetadata, FileInfo, FileGroupInfo
from ..utils.schemas import create_file_groups

class AuditService:
    """Centralized audit management service with manifest tracking."""

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
        """
        for audit in audit_metadata.audit_list:
            try:
                insert_audit(self.database, audit.to_audit_db())
            except Exception as e:
                logger.error(f"Error inserting audit {audit}: {e}")

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
                           filesize: Optional[int] = None, rows: Optional[int] = None) -> None:
        """Create manifest entry for processed file."""
        if not self.manifest_enabled:
            logger.debug(f"Manifest tracking disabled, skipping {Path(file_path).name}")
            return
        
        try:
            file_path_obj = Path(file_path)
            
            # Calculate missing file info if not provided
            if checksum is None and file_path_obj.exists():
                checksum = self._calculate_file_checksum(file_path_obj)
            
            if filesize is None and file_path_obj.exists():
                filesize = file_path_obj.stat().st_size
            
            # Ensure manifest table exists
            self._ensure_manifest_table()
            
            # Insert manifest entry
            self._insert_manifest_entry(
                file_path=str(file_path_obj),
                filename=file_path_obj.name,
                status=status,
                checksum=checksum,
                filesize=filesize,
                rows_processed=rows,
                processed_at=datetime.now()
            )
            
            logger.info(f"Manifest entry created for {file_path_obj.name} (status: {status})")
            
        except Exception as e:
            logger.error(f"Failed to create manifest entry for {file_path}: {e}")
    
    def _calculate_file_checksum(self, file_path: Path, algorithm: str = 'sha256') -> str:
        """Calculate file checksum for integrity verification."""
        try:
            hash_func = hashlib.new(algorithm)
            with open(file_path, 'rb') as f:
                # Read in chunks to handle large files
                for chunk in iter(lambda: f.read(8192), b""):
                    hash_func.update(chunk)
            return hash_func.hexdigest()
        except Exception as e:
            logger.warning(f"Could not calculate checksum for {file_path}: {e}")
            return None
    
    def _ensure_manifest_table(self) -> None:
        """Ensure manifest table exists in the audit database."""
        try:
            from sqlalchemy import text
            
            manifest_schema = '''
            CREATE TABLE IF NOT EXISTS ingestion_manifest (
                id SERIAL PRIMARY KEY,
                table_name VARCHAR(100),
                file_path TEXT NOT NULL,
                filename VARCHAR(255) NOT NULL,
                status VARCHAR(50) NOT NULL,
                checksum VARCHAR(128),
                file_size BIGINT,
                rows_processed INTEGER,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            CREATE INDEX IF NOT EXISTS idx_manifest_filename ON ingestion_manifest(filename);
            CREATE INDEX IF NOT EXISTS idx_manifest_status ON ingestion_manifest(status);
            CREATE INDEX IF NOT EXISTS idx_manifest_processed_at ON ingestion_manifest(processed_at);
            '''
            
            with self.database.engine.begin() as conn:
                conn.execute(text(manifest_schema))
                
            logger.debug("Manifest table schema ensured")
            
        except Exception as e:
            logger.warning(f"Could not create manifest table: {e}")
    
    def _insert_manifest_entry(self, file_path: str, filename: str, status: str, 
                              checksum: Optional[str], filesize: Optional[int], 
                              rows_processed: Optional[int], processed_at: datetime) -> None:
        """Insert manifest entry into database."""
        try:
            from sqlalchemy import text
            
            insert_sql = '''
            INSERT INTO ingestion_manifest 
            (file_path, filename, status, checksum, file_size, rows_processed, processed_at)
            VALUES (:file_path, :filename, :status, :checksum, :file_size, :rows_processed, :processed_at)
            '''
            
            params = {
                'file_path': file_path,
                'filename': filename,
                'status': status,
                'checksum': checksum,
                'file_size': filesize,
                'rows_processed': rows_processed,
                'processed_at': processed_at
            }
            
            with self.database.engine.begin() as conn:
                conn.execute(text(insert_sql), params)
                
        except Exception as e:
            logger.error(f"Failed to insert manifest entry: {e}")
    
    def verify_file_integrity(self, file_path: str) -> bool:
        """Verify file integrity against stored checksum."""
        if not self.manifest_enabled:
            return True
            
        try:
            from sqlalchemy import text
            file_path_obj = Path(file_path)
            
            # Get stored checksum
            with self.database.engine.connect() as conn:
                result = conn.execute(text('''
                    SELECT checksum FROM ingestion_manifest 
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
    
    def update_manifest_table_name(self, file_path: str, table_name: str) -> None:
        """Update manifest entry with table name."""
        if not self.manifest_enabled:
            return
            
        try:
            from sqlalchemy import text
            
            update_sql = '''
            UPDATE ingestion_manifest 
            SET table_name = :table_name
            WHERE file_path = :file_path 
            AND table_name IS NULL
            '''
            
            with self.database.engine.begin() as conn:
                conn.execute(text(update_sql), {
                    'table_name': table_name,
                    'file_path': file_path
                })
                
        except Exception as e:
            logger.warning(f"Could not update manifest entry with table name: {e}")
