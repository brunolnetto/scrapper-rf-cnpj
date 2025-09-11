"""
Unified data loading service for the CNPJ ETL project.

This service uses the strategy pattern to load data into the database using the selected backend (CSV, Parquet, or Auto).
"""

from datetime import datetime
from pathlib import Path
from typing import Dict, Any

from ....setup.logging import logger
from ....database.engine import Database
from ...schemas import AuditMetadata
from ...utils.batch_optimizer import BatchSizeOptimizer
from .strategies import BaseDataLoadingStrategy


class DataLoadingService:
    """Unified service for data loading operations with dynamic batch optimization."""

    def __init__(
        self,
        database: Database,
        pipeline_config,
        strategy: BaseDataLoadingStrategy,
        config,
        audit_service=None
    ):
        self.database = database
        self.pipeline_config = pipeline_config
        self.config = config
        self.audit_service = audit_service  # Store audit service
        
        # Initialize batch optimizer if we have pipeline configuration
        if hasattr(config, 'pipeline'):
            self.batch_optimizer = BatchSizeOptimizer(config)
            logger.debug("Initialized BatchSizeOptimizer with pipeline configuration")
        else:
            self.batch_optimizer = None
            logger.debug("No pipeline configuration provided, batch optimization disabled")
        
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

    def get_batch_configurations(self, file_paths: Dict[str, Path]) -> Dict[str, Any]:
        """Get optimized batch configurations for multiple files."""
        if not self.batch_optimizer:
            logger.debug("Batch optimizer not available, using default configurations")
            return {}
        
        try:
            configurations = self.batch_optimizer.get_batch_configurations_for_files(file_paths)
            self.batch_optimizer.log_optimization_summary(configurations)
            return configurations
        except Exception as e:
            logger.warning(f"Batch optimization failed, using defaults: {e}")
            return {}

    def get_batch_configuration_for_table(self, file_path: Path, table_name: str) -> Dict[str, Any]:
        """Get optimized batch configuration for a single table."""
        if not self.batch_optimizer:
            return {"batch_size": 50000, "parallel_workers": 1, "use_streaming": False}
        
        try:
            config = self.batch_optimizer.get_batch_configuration_for_file(file_path, table_name)
            return {
                "batch_size": config.batch_size,
                "parallel_workers": config.parallel_workers,
                "use_streaming": config.use_streaming,
                "estimated_batches": config.estimated_batches,
                "optimization_reason": config.optimization_reason
            }
        except Exception as e:
            logger.warning(f"Batch optimization failed for {table_name}, using defaults: {e}")
            return {"batch_size": 50000, "parallel_workers": 1, "use_streaming": False}

    def load_data(self, audit_metadata: AuditMetadata) -> AuditMetadata:
        """
        Load data for all tables using the configured strategy.
        Updates audit_metadata with insertion timestamps.
        Each table will create its own batch for better tracking.

        Args:
            audit_metadata: Audit metadata with table-to-files mapping

        Returns:
            Updated AuditMetadata
        """
        table_to_files = audit_metadata.tablename_to_zipfile_to_files

        # Each table will create its own batch in the strategy
        results = self.strategy.load_multiple_tables(
            database=self.database,
            table_to_files=table_to_files,
            pipeline_config=self.pipeline_config,
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
