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
        self.batch_optimizer = BatchSizeOptimizer(config)
        logger.debug("Initialized BatchSizeOptimizer with pipeline configuration")
        
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

    def load_data(self, audit_metadata: AuditMetadata, force_csv: bool = False) -> AuditMetadata:
        """
        Load data for all tables using the configured strategy.
        Updates audit_metadata with insertion timestamps.
        Each table will create its own batch for better tracking.

        Args:
            audit_metadata: Audit metadata with table-to-files mapping
            force_csv: If True, force CSV loading and skip Parquet file detection

        Returns:
            Updated AuditMetadata
        """
        table_to_files = audit_metadata.tablename_to_zipfile_to_files

        # Each table will create its own batch in the strategy
        results = self.strategy.load_multiple_tables(
            database=self.database,
            table_to_files=table_to_files,
            pipeline_config=self.pipeline_config,
            force_csv=force_csv,
        )

        # Update audit_metadata with insertion timestamps and persist to database
        for audit in audit_metadata.audit_list:
            result = results.get(audit.entity_name)
            if result and result[0]:  # success
                audit.completed_at = datetime.now()  # Individual timestamp per table
                logger.debug(f"Set inserted_at for {audit.entity_name}: success with {result[2]} rows")
            else:
                # Even if loading failed or had no changes, we processed it - set timestamp
                audit.completed_at = datetime.now()  # Individual timestamp per table
                logger.warning(
                    f"Setting inserted_at despite issue with table {audit.entity_name}: {result[1] if result else 'No result'}"
                )
                
            # Persist inserted_at to database
            self._persist_table_audit_completion(audit, result)
            
        return audit_metadata

    def _persist_table_audit_completion(self, audit, result) -> None:
        """Persist table audit completion timestamp and metadata to database."""
        try:
            import json
            from sqlalchemy import text
            
            # Prepare completion metadata
            completion_metadata = {
                "loading_completed": True,
                "completion_timestamp": audit.completed_at.isoformat() if audit.completed_at else None,
                "loading_success": result[0] if result else False,
                "rows_loaded": result[2] if result and len(result) > 2 else 0,
                "error_message": result[1] if result and not result[0] else None
            }
            
            # Get year and month from config (handle different config structures)
            year = self.config.pipeline.year
            month = self.config.pipeline.month            
            
            # Update table audit in database - Use audit service's database connection
            if self.audit_service is None:
                logger.warning(f"No audit service available, cannot persist table audit completion for {audit.entity_name}")
                return
                
            with self.audit_service.database.engine.begin() as conn:
                # First, get current audit_metadata
                current_result = conn.execute(text('''
                    SELECT notes FROM table_audit_manifest 
                    WHERE entity_name = :entity_name 
                    AND ingestion_year = :year 
                    AND ingestion_month = :month
                '''), {
                    'entity_name': audit.entity_name,
                    'year': year,
                    'month': month
                })
                
                row = current_result.fetchone()
                current_metadata = row[0] if row and row[0] else {}
                
                # Merge with completion metadata
                if isinstance(current_metadata, str):
                    import json as json_module
                    current_metadata = json_module.loads(current_metadata)
                elif current_metadata is None:
                    current_metadata = {}
                
                # Merge the completion metadata
                merged_metadata = {**current_metadata, **completion_metadata}
                
                # Update with merged metadata
                conn.execute(text('''
                    UPDATE table_audit_manifest 
                    SET completed_at = :completed_at,
                        notes = :metadata_json
                    WHERE entity_name = :entity_name 
                    AND ingestion_year = :year 
                    AND ingestion_month = :month
                '''), {
                    'completed_at': audit.completed_at,
                    'metadata_json': json.dumps(merged_metadata),
                    'entity_name': audit.entity_name,
                    'year': year,
                    'month': month
                })
                
                logger.debug(f"Persisted completion data for table audit: {audit.entity_name}")
                
        except Exception as e:
            logger.error(f"Failed to persist table audit completion for {audit.entity_name}: {e}")
