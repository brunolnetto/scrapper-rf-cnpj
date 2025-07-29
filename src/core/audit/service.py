"""
Unified audit management service for the CNPJ ETL project.

This service centralizes audit creation, validation, and insertion logic for use by ETL and data loading components.
"""

from typing import List, Optional
from datetime import datetime
from database.schemas import Database
from database.models import AuditDB
from core.schemas import AuditMetadata, FileInfo, FileGroupInfo
from core.utils.schemas import create_file_groups
from setup.logging import logger

# Import existing audit utilities
from database.utils.models import create_audits, create_audit_metadata, insert_audit

class AuditService:
    """Centralized audit management service."""
    
    def __init__(self, database: Database):
        self.database = database
    
    def create_audits_from_files(self, files_info: List[FileInfo]) -> List[AuditDB]:
        """
        Create audit records from a list of FileInfo objects.
        Filters for relevant files and groups them appropriately.
        """
        filtered_files = self._filter_relevant_files(files_info)
        file_groups = create_file_groups(filtered_files)
        return self._create_audits_from_groups(file_groups)
    
    def create_audit_metadata(
        self, 
        audits: List[AuditDB], 
        download_folder: str
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
            info for info in files_info 
            if info.filename.endswith('.zip') or
            (info.filename.endswith('.pdf') and 'layout' in info.filename.lower())
        ]
    
    def _create_audits_from_groups(self, file_groups: List[FileGroupInfo]) -> List[AuditDB]:
        """
        Create audit records from file groups using the database.
        """
        return create_audits(self.database, file_groups)