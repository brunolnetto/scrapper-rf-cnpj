"""
Unified data loading service for the CNPJ ETL project.

This service uses the strategy pattern to load data into the database using the selected backend (CSV, Parquet, or Auto).
"""

from datetime import datetime

from ...setup.config import PathConfig
from ...setup.logging import logger
from ...database.engine import Database
from ..schemas import AuditMetadata
from .strategies import BaseDataLoadingStrategy


class DataLoadingService:
    """Unified service for data loading operations."""

    def __init__(
        self,
        database: Database,
        path_config: PathConfig,
        strategy: BaseDataLoadingStrategy,
        config=None,
        audit_service=None
    ):
        self.database = database
        self.path_config = path_config
        self.config = config
        self.audit_service = audit_service  # Store audit service
        
        # Always use enhanced strategy (replace legacy implementation)
        if config:
            try:
                from .strategies import DataLoadingStrategy
                self.strategy = DataLoadingStrategy(config, audit_service=audit_service)
                logger.info("Using enhanced loading strategy (default)")
            except ImportError as e:
                logger.warning(f"Enhanced strategy not available, using fallback: {e}")
                self.strategy = strategy
        else:
            # Fallback if no config provided
            self.strategy = strategy

    def load_data(self, audit_metadata: AuditMetadata) -> AuditMetadata:
        """
        Load data for all tables using the configured strategy.
        Updates audit_metadata with insertion timestamps.

        Args:
            audit_metadata: Audit metadata with table-to-files mapping
            **kwargs: Additional arguments for the strategy

        Returns:
            Updated AuditMetadata
        """
        table_to_files = audit_metadata.tablename_to_zipfile_to_files

        # Prepare mapping: table_name -> list of files (for CSV) or just table_name (for Parquet)
        results = self.strategy.load_multiple_tables(
            database=self.database,
            table_to_files=table_to_files,
            path_config=self.path_config,
        )

        # Update audit_metadata with insertion timestamps
        now = datetime.now()
        for audit in audit_metadata.audit_list:
            result = results.get(audit.audi_table_name)
            if result and result[0]:  # success
                audit.audi_inserted_at = now
                logger.debug(f"Set audi_inserted_at for {audit.audi_table_name}: success with {result[2]} rows")
            else:
                # Even if loading failed or had no changes, we processed it - set timestamp
                audit.audi_inserted_at = now
                logger.warning(
                    f"Setting audi_inserted_at despite issue with table {audit.audi_table_name}: {result[1] if result else 'No result'}"
                )
        return audit_metadata
