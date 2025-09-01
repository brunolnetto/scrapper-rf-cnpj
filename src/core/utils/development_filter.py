"""
Centralized development mode filtering utilities.
Extracts all filtering logic from strategies.py and etl.py for better maintainability.
"""
from pathlib import Path
from typing import List, Dict, Any, Tuple, Optional

from ...setup.config import ConfigurationService
from ...setup.logging import logger
from ...database.models import AuditDB

class DevelopmentFilter:
    """Centralized development mode filtering utilities."""

    def __init__(self, config: ConfigurationService):
        self.config = config
        self.is_enabled = config.is_development_mode()

    def filter_audits_by_size(self, audits: List[AuditDB]) -> List[AuditDB]:
        """Filter audits by file size limit."""
        if not self.is_enabled:
            return audits

        file_size_limit = self.config.get_file_size_limit()
        filtered_audits = [
            audit for audit in audits
            if audit.audi_file_size_bytes < file_size_limit
        ]

        logger.debug(f"[DEV-MODE] Size filtering: {len(audits)} → {len(filtered_audits)} audits")
        return filtered_audits

    def filter_audits_by_table_limit(self, audits: List[AuditDB]) -> List[AuditDB]:
        """Filter audits by max files per table."""
        if not self.is_enabled:
            return audits

        max_files_per_table = self.config.get_max_files_per_table()
        table_counts = {}
        filtered_audits = []

        for audit in audits:
            table_name = audit.audi_table_name
            if table_name not in table_counts:
                table_counts[table_name] = 0

            if table_counts[table_name] < max_files_per_table:
                filtered_audits.append(audit)
                table_counts[table_name] += 1
            else:
                logger.debug(f"[DEV-MODE] Skipping additional file for table '{table_name}'")

        logger.debug(f"[DEV-MODE] Table limit filtering: {len(audits)} → {len(filtered_audits)} audits")
        return filtered_audits

    def filter_csv_files_by_size(self, csv_files: List[Path], extract_path: Path) -> List[Path]:
        """Filter CSV files by size limit."""
        if not self.is_enabled:
            return csv_files

        file_size_limit = self.config.get_file_size_limit()
        filtered_files = []

        for csv_file in csv_files:
            file_size = csv_file.stat().st_size
            if file_size < file_size_limit:
                filtered_files.append(csv_file)
                logger.debug(f"[DEV-MODE] Including CSV: {csv_file.name} ({file_size} bytes)")
            else:
                logger.debug(f"[DEV-MODE] Skipping large CSV: {csv_file.name} ({file_size} bytes)")

        logger.info(f"[DEV-MODE] CSV filtering: {len(csv_files)} → {len(filtered_files)} files")
        return filtered_files

    def filter_parquet_file_by_size(self, parquet_file: Path) -> bool:
        """Check if Parquet file should be processed based on size."""
        if not self.is_enabled:
            return True

        file_size_limit = self.config.get_file_size_limit()
        file_size = parquet_file.stat().st_size

        if file_size >= file_size_limit:
            logger.info(f"[DEV-MODE] Skipping large Parquet file: {parquet_file.name} ({file_size} bytes > {file_size_limit})")
            return False

        logger.debug(f"[DEV-MODE] Including Parquet file: {parquet_file.name} ({file_size} bytes)")
        return True

    def filter_csv_files_by_table_limit(self, csv_files: List[str], table_name: str) -> List[str]:
        """Filter CSV files by max files per table limit."""
        if not self.is_enabled:
            return csv_files

        max_files_per_table = self.config.get_max_files_per_table()

        if len(csv_files) > max_files_per_table:
            logger.debug(f"[DEV-MODE] Limiting table '{table_name}' to {max_files_per_table} files (was {len(csv_files)})")
            return csv_files[:max_files_per_table]

        return csv_files

    def log_filtering_summary(self, original_count: int, filtered_count: int, filter_type: str):
        """Log filtering summary with consistent formatting."""
        if self.is_enabled:
            logger.info(f"[DEV-MODE] {filter_type}: {original_count} → {filtered_count} items")

    def log_conversion_summary(self, audit_map: Dict[str, Dict[str, List[str]]]):
        """Log conversion summary with file count and size limit information."""
        if self.is_enabled:
            total_files = sum(len(files) for table_files in audit_map.values() for files in table_files.values())
            file_size_limit = self.config.get_file_size_limit()
            logger.info(f"[DEV-MODE] Converting {total_files} files (filtered by size limit: {file_size_limit} bytes)")
