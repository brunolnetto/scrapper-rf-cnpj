"""Table-level audits: creation from discovered files, lookup and post-conversion updates.

Mixin of :class:`AuditService`; relies on attributes set in its ``__init__``.
"""

import uuid
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from sqlalchemy import text

from ....database.models.audit import AuditStatus, TableAuditManifest
from ....setup.logging import logger
from ....utils.models import create_audit_metadata, create_audits, insert_audit
from ...schemas import AuditMetadata, FileGroupInfo, FileInfo
from ...utils.schemas import create_file_groups


class TableAuditMixin:
    """Table-level audits: creation from discovered files, lookup and post-conversion updates."""

    def create_audits_from_files(
        self, files_info: List[FileInfo]
    ) -> List[TableAuditManifest]:
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

    def insert_audits(
        self, audit_metadata: AuditMetadata, create_file_manifests: bool = True
    ) -> None:
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

            # Only create file manifests for real processed files (csv/parquet) or explicit paths.
            # This avoids creating entries for zip identifiers, synthetic markers, or table identifiers.
            if not isinstance(file_path, str):
                logger.debug(f"Skipping non-string source_files entry for audit: {file_path}")
                return

            lower_fp = file_path.lower()
            if lower_fp == "synthetic_conversion":
                logger.debug(f"Skipping synthetic conversion manifest for: {file_path}")
                return

            # Skip archive-level artifacts and abstract identifiers
            if lower_fp.endswith('.zip') or '$' in file_path or '/' not in file_path and not (lower_fp.endswith('.csv') or lower_fp.endswith('.parquet')):
                logger.debug(f"Skipping manifest entry for non-data source: {file_path}")
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
            file_manifest_id = str(uuid.uuid4())

            # Create manifest entry with table name and audit reference
            self.repository.insert_file_manifest(
                file_manifest_id=file_manifest_id,
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
            self.database, 
            file_groups, 
            etl_year=self.config.year, 
            etl_month=self.config.month
        )

    def _find_table_audit(self, table_name: str) -> Optional[str]:
        """Find the most recent table audit ID for a given table name."""
        return self.repository.find_table_audit(table_name)

    @contextmanager
    def table_context(self, table_names: List[str]):
        """
        Context manager that marks table-level audits as started and sets
        their status to RUNNING. If a table audit does not exist for the
        given table (in the current ingestion period), a new entry is
        inserted with started_at populated.

        Yields:
            dict: mapping of table_name -> table_audit_id
        """
        mapping: Dict[str, Optional[str]] = {}
        try:
            with self.database.engine.begin() as conn:
                for table_name in table_names:
                    try:
                        # Try to find an existing table audit
                        result = conn.execute(text('''
                            SELECT table_audit_id, started_at
                            FROM table_audit_manifest
                            WHERE entity_name = :table_name
                            ORDER BY created_at DESC
                            LIMIT 1
                        '''), {'table_name': table_name})
                        row = result.fetchone()

                        if row and row[0]:
                            table_audit_id = str(row[0])
                            started_at = row[1]
                            # Only set started_at if it's missing
                            if not started_at:
                                conn.execute(text('''
                                    UPDATE table_audit_manifest
                                    SET started_at = :started_at,
                                        status = :status
                                    WHERE table_audit_id = :table_audit_id
                                '''), {
                                    'started_at': datetime.now(),
                                    'status': AuditStatus.RUNNING.value,
                                    'table_audit_id': table_audit_id
                                })
                        else:
                            # Insert a new table audit manifest for this table
                            table_audit_id = str(uuid.uuid4())
                            conn.execute(text('''
                                INSERT INTO table_audit_manifest
                                (table_audit_id, entity_name, status, created_at, started_at, ingestion_year, ingestion_month)
                                VALUES (:table_audit_id, :entity_name, :status, :created_at, :started_at, :year, :month)
                            '''), {
                                'table_audit_id': table_audit_id,
                                'entity_name': table_name,
                                'status': AuditStatus.RUNNING.value,
                                'created_at': datetime.now(),
                                'started_at': datetime.now(),
                                'year': self.config.year,
                                'month': self.config.month
                            })

                        mapping[table_name] = table_audit_id

                    except Exception as e:
                        logger.debug(f"Failed to mark table audit started for {table_name}: {e}")
                        mapping[table_name] = None

            yield mapping

        finally:
            # No automatic completion behavior here; completion is handled elsewhere
            return

    def update_table_audit_after_conversion(
        self, table_name: str, processed_file_path: str, 
        processing_metadata: dict = None,
        original_sources: list | None = None,
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
                    "conversion_timestamp": datetime.now().isoformat(),
                    "provenance": "converted_parquet",
                })

                # Preserve or set source_files: prefer provided original_sources
                if original_sources:
                    source_files_json = json.dumps(list(original_sources))
                    # also keep original_sources in notes for clarity
                    metadata.setdefault('original_sources', list(original_sources))
                else:
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
