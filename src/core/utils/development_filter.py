"""
Development mode filtering utilities with blob limits and row sampling.
Provides comprehensive development mode controls for safer testing and faster iteration.
"""
from pathlib import Path
from typing import List, Dict, Any, Union
import random
import math

import polars as pl
from ...setup.config.models import (
    DevelopmentConfig, 
    DownloadConfig, ConversionConfig, LoadingConfig
)
            
from ...setup.logging import logger
from ...database.models import TableAuditManifest

class DevelopmentFilter:
    """Development mode filtering with comprehensive controls."""

    def __init__(self, config: Any):
        # Handle both new SOLID config and legacy config formats
        if hasattr(config, 'pipeline') and hasattr(config.pipeline, 'development'):
            # SOLID ConfigurationService format via config service
            self.development = config.pipeline.development
            self.is_enabled = config.pipeline.development.enabled
        elif hasattr(config, 'development'):
            # Already new format via config service  
            self.development = config.development
            self.is_enabled = config.development.enabled
        else:
            # Legacy format - create wrapper
            dev_config = DevelopmentConfig(
                enabled=getattr(config, 'is_development_mode', lambda: False)(),
                max_files_per_table=getattr(config, 'get_max_files_per_table', lambda: 5)(),
                file_size_limit_mb=50,  # Default - fixed property name
                row_limit_percent=0.1
            )
            self.development = dev_config
            self.is_enabled = dev_config.enabled

    def filter_audits_by_size(self, audits: List[TableAuditManifest]) -> List[TableAuditManifest]:
        """Filter audits by file size limit."""
        if not self.is_enabled:
            return audits

        file_size_limit_bytes = self.development.file_size_limit_mb * 1024 * 1024
        filtered_audits = [
            audit for audit in audits
            if audit.file_size_bytes < file_size_limit_bytes
        ]

        if len(filtered_audits) != len(audits):
            logger.info(
                f"[DEV-MODE] File size filtering: {len(audits)} → {len(filtered_audits)} audits "
                f"(limit: {self.development.file_size_limit_mb}MB)"
            )
        
        return filtered_audits

    def filter_audits_by_table_limit(self, audits: List[TableAuditManifest]) -> List[TableAuditManifest]:
        """Filter audits by max files per table with strategic selection."""
        if not self.is_enabled:
            return audits

        max_files = self.development.max_files_per_table
        table_groups = {}
        
        # Group audits by table
        for audit in audits:
            table_name = audit.table_name
            if table_name not in table_groups:
                table_groups[table_name] = []
            table_groups[table_name].append(audit)

        filtered_audits = []
        
        for table_name, table_audits in table_groups.items():
            if len(table_audits) <= max_files:
                filtered_audits.extend(table_audits)
            else:
                # Strategic selection: first, middle, last, then random
                selected = self._select_representative_files(table_audits, max_files)
                filtered_audits.extend(selected)
                
                logger.info(
                    f"[DEV-MODE] {table_name}: Limited to {len(selected)} files "
                    f"(from {len(table_audits)} available)"
                )

        return filtered_audits

    def filter_files_by_blob_limit(self, file_paths: List[Path], table_name: str) -> List[Path]:
        """Limit number of files per blob with strategic selection."""
        if not self.is_enabled:
            return file_paths

        max_files = self.development.max_files_per_blob
        if len(file_paths) <= max_files:
            return file_paths

        selected = self._select_representative_files(file_paths, max_files)
        
        logger.info(
            f"[DEV-MODE] {table_name}: Limited to {len(selected)} files per blob "
            f"(from {len(file_paths)} available)"
        )
        
        return selected

    def check_blob_size_limit(self, file_path: Path) -> bool:
        """Check if file exceeds file size limit for development mode."""
        if not self.is_enabled:
            return True

        file_size_mb = file_path.stat().st_size / (1024 * 1024)
        max_size_mb = self.development.file_size_limit_mb

        if file_size_mb > max_size_mb:
            logger.debug(
                f"[DEV-MODE] Skipping {file_path.name} "
                f"({file_size_mb:.1f}MB > {max_size_mb}MB limit)"
            )
            return False

        return True

    def filter_dataframe_by_percentage(self, df: Any, table_name: str) -> Any:
        """Sample dataframe by configured percentage in development mode."""
        if not self.is_enabled:
            return df

        original_rows = len(df)
        row_limit_percent = self.development.row_limit_percent
        target_rows = max(1, int(original_rows * row_limit_percent))

        if target_rows >= original_rows:
            return df

        # Use polars-native sampling for better performance
        if hasattr(df, 'sample'):  # polars DataFrame
            sampled_df = df.sample(n=target_rows, seed=42)
        elif hasattr(df, 'head'):  # polars LazyFrame or other polars types
            # For polars LazyFrame, take systematic sample for better distribution
            step = max(1, original_rows // target_rows)
            if hasattr(df, 'filter') and hasattr(df, 'with_row_count'):
                # Use polars row_count for systematic sampling
                sampled_df = (df.with_row_count()
                             .filter(pl.col("row_nr") % step == 0)
                             .head(target_rows)
                             .drop("row_nr"))
            else:
                sampled_df = df.head(target_rows)
        else:
            # Unsupported DataFrame type - fail fast instead of silent fallback
            raise TypeError(
                f"Unsupported DataFrame type: {type(df)}. "
                f"Only polars DataFrames and LazyFrames are supported. "
                f"Convert your data to polars format first."
            )

        logger.info(
            f"[DEV-MODE] {table_name}: Sampled {target_rows:,} rows "
            f"({row_limit_percent:.1%}) from {original_rows:,}"
        )

        return sampled_df

    def get_sampling_configuration(self, table_name: str) -> Dict[str, Any]:
        """Get sampling configuration for a specific table."""
        if not self.is_enabled:
            return {
                "enabled": False,
                "row_sampling": False,
                "file_limiting": False
            }

        return {
            "enabled": True,
            "row_sampling": True,
            "row_limit_percent": self.development.row_limit_percent,
            "file_limiting": True,
            "max_files_per_table": self.development.max_files_per_table,
            "file_size_limit_mb": self.development.file_size_limit_mb,
            "table_name": table_name
        }

    def get_development_summary(
        self, 
        table_name: str, 
        files_processed: int, 
        rows_processed: int,
        original_files: int = None,
        original_rows: int = None
    ) -> Dict[str, Any]:
        """Generate summary of development mode filtering."""
        if not self.is_enabled:
            return {"development_mode": False}

        summary = {
            "development_mode": True,
            "table_name": table_name,
            "files_processed": files_processed,
            "rows_processed": rows_processed,
            "configuration": {
                "row_limit_percent": self.development.row_limit_percent,
                "max_files_per_table": self.development.max_files_per_table,
                "file_size_limit_mb": self.development.file_size_limit_mb
            }
        }

        if original_files is not None:
            summary["files_reduction"] = {
                "original": original_files,
                "processed": files_processed,
                "reduction_percent": (1 - files_processed / max(1, original_files)) * 100
            }

        if original_rows is not None:
            summary["rows_reduction"] = {
                "original": original_rows,
                "processed": rows_processed,
                "reduction_percent": (1 - rows_processed / max(1, original_rows)) * 100
            }

        return summary

    def _select_representative_files(self, items: List[Any], max_count: int) -> List[Any]:
        """Select representative files using strategic sampling."""
        if len(items) <= max_count:
            return items

        if max_count == 1:
            return [items[0]]

        selected = []

        if max_count >= 3:
            # Always include first, middle, and last for coverage
            selected.extend([
                items[0],                    # First
                items[len(items) // 2],      # Middle
                items[-1]                    # Last
            ])
            
            # Fill remaining slots with random selection
            remaining_items = [item for item in items if item not in selected]
            additional_count = max_count - 3
            
            if additional_count > 0 and remaining_items:
                additional = random.sample(
                    remaining_items, 
                    min(additional_count, len(remaining_items))
                )
                selected.extend(additional)
        else:
            # For max_count == 2, take first and last
            selected = [items[0], items[-1]]

        return selected

    def log_simple_filtering(self, original_count: int, filtered_count: int, item_type: str) -> None:
        """Log simple filtering results for development mode."""
        if not self.is_enabled:
            return
            
        if original_count != filtered_count:
            reduction_pct = (1 - filtered_count / original_count) * 100 if original_count > 0 else 0
            logger.info(
                f"[DEV-MODE] {item_type.capitalize()} filtering: {original_count} → {filtered_count} "
                f"({reduction_pct:.1f}% reduction)"
            )
        else:
            logger.debug(f"[DEV-MODE] No {item_type} filtering applied: {original_count} items")

    def log_conversion_summary(self, audit_map: Dict[str, Dict[str, List[str]]]) -> None:
        """Log conversion summary for development mode."""
        if not self.is_enabled:
            return
            
        total_files = 0
        tables_count = len(audit_map)
        
        for table_name, zip_files in audit_map.items():
            table_file_count = sum(len(csv_files) for csv_files in zip_files.values())
            total_files += table_file_count
            logger.debug(f"[DEV-MODE] {table_name}: {table_file_count} files to convert")
        
        logger.info(f"[DEV-MODE] Conversion summary: {tables_count} tables, {total_files} files total")

    def log_filtering_summary(self, summaries: List[Dict[str, Any]]) -> None:
        """Log comprehensive filtering summary."""
        if not self.is_enabled:
            return

        logger.info("Development mode filtering summary:")
        
        total_files_original = 0
        total_files_processed = 0
        total_rows_original = 0
        total_rows_processed = 0

        for summary in summaries:
            if not summary.get("development_mode"):
                continue

            table_name = summary.get("table_name", "unknown")
            files_processed = summary.get("files_processed", 0)
            rows_processed = summary.get("rows_processed", 0)

            logger.info(f"  {table_name}: {files_processed} files, {rows_processed:,} rows")

            # Accumulate totals if reduction info is available
            if "files_reduction" in summary:
                total_files_original += summary["files_reduction"]["original"]
                total_files_processed += summary["files_reduction"]["processed"]

            if "rows_reduction" in summary:
                total_rows_original += summary["rows_reduction"]["original"]
                total_rows_processed += summary["rows_reduction"]["processed"]

        if total_files_original > 0:
            files_reduction_pct = (1 - total_files_processed / total_files_original) * 100
            logger.info(
                f"Total file reduction: {total_files_original} → {total_files_processed} "
                f"({files_reduction_pct:.1f}% reduction)"
            )

        if total_rows_original > 0:
            rows_reduction_pct = (1 - total_rows_processed / total_rows_original) * 100
            logger.info(
                f"Total row reduction: {total_rows_original:,} → {total_rows_processed:,} "
                f"({rows_reduction_pct:.1f}% reduction)"
            )
