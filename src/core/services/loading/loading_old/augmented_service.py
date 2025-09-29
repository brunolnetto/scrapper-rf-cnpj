"""
memory_integrated_service.py
Enhanced data loading service with full memory monitoring integration.
"""

from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional

from ....setup.logging import logger
from ....database.engine import Database
from ...schemas import AuditMetadata
from ...utils.batch_optimizer import BatchSizeOptimizer
from .strategies import BaseDataLoadingStrategy
from ..memory.service import MemoryMonitor

from .file_loader.augmented_file_loader import create_file_loader


class DataLoadingService:
    """
    Enhanced data loading service with comprehensive memory monitoring and optimization.
    """

    def __init__(
        self,
        database: Database,
        strategy: BaseDataLoadingStrategy,
        config,
        audit_service=None,
        memory_monitor: Optional[MemoryMonitor] = None
    ):
        self.database = database
        self.config = config
        self.audit_service = audit_service
        
        # Initialize memory monitor if not provided
        self.memory_monitor = memory_monitor or MemoryMonitor(config.memory_monitor)    
        
        # Initialize batch optimizer with memory awareness
        self.batch_optimizer = BatchSizeOptimizer(config) if config else None
        
        # Strategy with memory integration
        try:
            from .augmented_strategies import DataLoadingStrategy
            self.strategy = DataLoadingStrategy(
                config, 
                memory_monitor=self.memory_monitor,
                audit_service=audit_service
            )
            logger.info("Using memory-aware loading strategy")
        except ImportError:
            logger.warning("Memory-aware strategy not available, using enhanced fallback")
            self.strategy = strategy

    def pre_loading_memory_check(self, file_paths: Dict[str, Path]) -> bool:
        """
        Perform comprehensive memory check before starting data loading.
        
        Returns:
            True if safe to proceed, False otherwise
        """
        if not self.memory_monitor:
            logger.warning("[MemoryIntegratedService] No memory monitor available for pre-check")
            return True
        
        try:
            # Get current memory status
            status = self.memory_monitor.get_status_report()
            logger.info(f"[MemoryIntegratedService] Pre-loading memory status: "
                       f"Usage: {status['usage_above_baseline_mb']:.1f}MB, "
                       f"Budget: {status['budget_remaining_mb']:.1f}MB, "
                       f"Pressure: {status['pressure_level']:.2f}")
            
            # Check if we should prevent processing
            if self.memory_monitor.should_prevent_processing():
                logger.error("[MemoryIntegratedService] Insufficient memory to start processing")
                return False
            
            # Estimate memory requirements for all files
            total_estimated_memory = 0
            for table_name, file_path in file_paths.items():
                try:
                    loader = create_file_loader(str(file_path), memory_monitor=self.memory_monitor)
                    estimates = loader.estimate_memory_requirements(['dummy'] * 10)  # Rough estimate
                    total_estimated_memory += estimates.get('estimated_memory_per_chunk_mb', 100)
                except Exception as e:
                    logger.warning(f"Could not estimate memory for {table_name}: {e}")
                    total_estimated_memory += 100  # Conservative default
            
            available_budget = status['budget_remaining_mb']
            if total_estimated_memory > available_budget * 0.8:  # Use only 80% of budget
                logger.error(f"[MemoryIntegratedService] Estimated memory requirement ({total_estimated_memory:.1f}MB) "
                           f"exceeds 80% of available budget ({available_budget:.1f}MB)")
                return False
            
            logger.info(f"[MemoryIntegratedService] Memory pre-check passed: "
                       f"Estimated {total_estimated_memory:.1f}MB vs {available_budget:.1f}MB available")
            return True
            
        except Exception as e:
            logger.error(f"[MemoryIntegratedService] Pre-loading memory check failed: {e}")
            return False

    def optimize_processing_order(self, file_paths: Dict[str, Path]) -> Dict[str, Path]:
        """
        Optimize the processing order of files based on size and memory requirements.
        Process smaller files first to reduce memory fragmentation.
        """
        try:
            # Sort files by size (smallest first)
            file_sizes = []
            for table_name, file_path in file_paths.items():
                try:
                    size = file_path.stat().st_size
                    file_sizes.append((table_name, file_path, size))
                except Exception as e:
                    logger.warning(f"Could not get size for {table_name}: {e}")
                    file_sizes.append((table_name, file_path, float('inf')))
            
            # Sort by size, smallest first
            file_sizes.sort(key=lambda x: x[2])
            
            # Create ordered dictionary
            optimized_order = {}
            for table_name, file_path, size in file_sizes:
                optimized_order[table_name] = file_path
            
            logger.info(f"[MemoryIntegratedService] Optimized processing order: "
                       f"{list(optimized_order.keys())}")
            
            return optimized_order
            
        except Exception as e:
            logger.error(f"Failed to optimize processing order: {e}")
            return file_paths

    def get_batch_configurations(self, file_paths: Dict[str, Path]) -> Dict[str, Dict[str, Any]]:
        """
        Get batch configurations that are aware of both file size and memory constraints.
        """
        configurations = {}
        
        for table_name, file_path in file_paths.items():
            try:
                # Create loader to get recommendations
                loader = create_file_loader(str(file_path), memory_monitor=self.memory_monitor)
                recommendations = loader.get_recommended_processing_params()
                
                # Integrate with batch optimizer if available
                try:
                    optimizer_config = self.batch_optimizer.get_batch_configuration_for_file(file_path, table_name)
                    # Merge recommendations with optimizer suggestions
                    recommendations.update({
                        'optimizer_batch_size': optimizer_config.batch_size,
                        'optimizer_workers': optimizer_config.parallel_workers,
                        'optimizer_streaming': optimizer_config.use_streaming,
                        'optimization_reason': optimizer_config.optimization_reason
                    })
                    
                    # Use the more conservative settings
                    recommendations['final_chunk_size'] = min(
                        recommendations.get('chunk_size', 20_000),
                        optimizer_config.batch_size
                    )
                    recommendations['final_enable_parallelism'] = (
                        recommendations.get('enable_parallelism', False) and
                        optimizer_config.parallel_workers > 1
                    )
                    
                except Exception as e:
                    logger.warning(f"Batch optimizer failed for {table_name}: {e}")
                
                configurations[table_name] = recommendations
                
            except Exception as e:
                logger.error(f"Failed to get configuration for {table_name}: {e}")
                configurations[table_name] = {
                    'chunk_size': 10_000,
                    'sub_batch_size': 1_500,
                    'enable_parallelism': False,
                    'error': str(e)
                }
        
        return configurations

    def load_data(
        self, 
        audit_metadata: AuditMetadata, 
        force_csv: bool = False
    ) -> AuditMetadata:
        """
        Load data with comprehensive memory monitoring and optimization.
        """
        table_to_files = audit_metadata.tablename_to_zipfile_to_files
        
        # Pre-loading checks
        file_paths = {}
        for table_name, zipfile_to_files in table_to_files.items():
            for zipfile, files in zipfile_to_files.items():
                if files:
                    file_paths[table_name] = Path(files[0])  # Use first file for estimation
                    break
        
        # Memory pre-check
        if not self.pre_loading_memory_check(file_paths):
            raise MemoryError("Insufficient memory to proceed with data loading")
        
        # Optimize processing order
        optimized_order = self.optimize_processing_order(file_paths)
        
        # Get memory-aware configurations
        configurations = self.get_batch_configurations(file_paths)
        
        # Log optimization summary
        self._log_optimization_summary(configurations)
        
        # Process files with memory monitoring
        results = {}
        processed_count = 0
        
        try:
            for table_name in optimized_order.keys():
                if table_name not in table_to_files:
                    continue
                
                logger.info(f"[MemoryIntegratedService] Processing table {processed_count + 1}/{len(optimized_order)}: {table_name}")
                
                # Memory check before each table
                if self.memory_monitor.should_prevent_processing():
                    logger.error(f"Memory limit exceeded before processing {table_name}")
                    raise MemoryError(f"Memory limit exceeded before processing {table_name}")
                
                # Get configuration for this table
                config = configurations.get(table_name, {})
                
                # Process single table with memory-aware strategy
                table_files = {table_name: table_to_files[table_name]}
                
                try:
                    # Use memory-aware loading
                    table_results = self._load_table_with_memory_monitoring(
                        table_files, config, force_csv
                    )
                    results.update(table_results)
                    
                    processed_count += 1
                    logger.info(f"[MemoryIntegratedService] Completed {table_name} "
                               f"({processed_count}/{len(optimized_order)})")
                    
                    # Inter-table cleanup
                    if self.memory_monitor:
                        cleanup_stats = self.memory_monitor.perform_aggressive_cleanup()
                        logger.info(f"Inter-table cleanup freed {cleanup_stats.get('freed_mb', 0):.1f}MB")
                
                except Exception as e:
                    logger.error(f"Failed to process table {table_name}: {e}")
                    results[table_name] = (False, str(e), 0)
                    # Continue with other tables
                    continue
            
            # Update audit metadata
            self._update_audit_metadata(audit_metadata, results)
            
            return audit_metadata
            
        except Exception as e:
            logger.error(f"Data loading failed: {e}")
            # Update audit metadata with partial results
            self._update_audit_metadata(audit_metadata, results)
            raise
        
        finally:
            # Final cleanup
            final_cleanup = self.memory_monitor.perform_aggressive_cleanup()
            logger.info(f"Final cleanup freed {final_cleanup.get('freed_mb', 0):.1f}MB")

    def load_single_table_memory_aware(
        self,
        database: Database,
        table_to_files: Dict[str, Dict],
        pipeline_config: Any,
        memory_config: Dict[str, Any],
        force_csv: bool,
        memory_monitor: Optional[Any]
    ) -> Dict[str, tuple]:
        """
        Load a single table using pre-determined memory-aware configurations.

        This method is called directly by the DataLoadingService's optimized loop
        and uses the `memory_config` passed from it, which contains parameters like
        the optimal chunk size based on current system memory.
        """
        # The input dictionary is guaranteed to contain exactly one table.
        try:
            table_name = list(table_to_files.keys())[0]
            zipfile_to_files = table_to_files[table_name]
        except IndexError:
            logger.error("load_single_table_memory_aware was called with an empty table_to_files dictionary.")
            return {}

        logger.info(f"[MemoryAwareStrategy] Loading single table '{table_name}' with optimized config: {memory_config}")

        total_rows = 0
        all_errors = []

        try:
            table_info = table_name_to_table_info(table_name)
            loader = self._create_loader(database)

            # First, check for an optimized Parquet file, unless CSV is forced.
            if not force_csv:
                parquet_result = self._try_parquet_loading(loader, table_info, table_name)
                if parquet_result:
                    logger.info(f"Loaded '{table_name}' from pre-existing Parquet file.")
                    return {table_name: parquet_result}
            
            # If no Parquet, process the provided source files (likely CSVs).
            for zip_filename, files in zipfile_to_files.items():
                filtered_files = self._apply_development_filtering(files, table_name)
                if not filtered_files:
                    continue

                for file_path in filtered_files:
                    if self.memory_monitor and self.memory_monitor.should_prevent_processing():
                        raise MemoryError(f"Memory limit exceeded before processing file {file_path.name}")
                    
                    # Create the file loader to handle reading in batches.
                    from .file_loader.augmented_file_loader import create_file_loader
                    encoding = getattr(self.config.pipeline.data_source, 'encoding', 'utf-8')
                    file_loader = create_file_loader(str(file_path), encoding=encoding, memory_monitor=self.memory_monitor)
                    
                    # Process the file using the provided memory configuration.
                    success, error, rows = self._process_file_with_batching(
                        file_loader, loader, table_info, table_name, recommendations=memory_config
                    )
                    
                    if success:
                        total_rows += rows
                    else:
                        all_errors.append(f"File {file_path.name}: {error}")
            
            if all_errors:
                error_summary = "; ".join(all_errors)
                logger.error(f"Errors occurred while processing table '{table_name}': {error_summary}")
                return {table_name: (False, error_summary, total_rows)}
            else:
                return {table_name: (True, None, total_rows)}

        except Exception as e:
            logger.error(f"[MemoryAwareStrategy] Critical failure processing table '{table_name}': {e}", exc_info=True)
            return {table_name: (False, str(e), total_rows)}

    def _load_table_with_memory_monitoring(
        self, 
        table_files: Dict[str, Dict], 
        config: Dict[str, Any], 
        force_csv: bool
    ) -> Dict[str, tuple]:
        """Load a single table with memory monitoring."""
        
        # Use enhanced strategy if available
        if hasattr(self.strategy, 'load_single_table_memory_aware'):
            return self.strategy.load_single_table_memory_aware(
                database=self.database,
                table_to_files=table_files,
                pipeline_config=self.pipeline_config,
                memory_config=config,
                force_csv=force_csv,
                memory_monitor=self.memory_monitor
            )
        else:
            # Fallback to regular strategy
            return self.strategy.load_multiple_tables(
                database=self.database,
                table_to_files=table_files,
                pipeline_config=self.pipeline_config,
                force_csv=force_csv
            )

    def _update_audit_metadata(self, audit_metadata: AuditMetadata, results: Dict[str, tuple]):
        """Update audit metadata with processing results."""
        for audit in audit_metadata.audit_list:
            result = results.get(audit.entity_name)
            if result and result[0]:  # success
                audit.completed_at = datetime.now()
                logger.debug(f"Set completed_at for {audit.entity_name}: success with {result[2]} rows")
            else:
                audit.completed_at = datetime.now()
                if result:
                    logger.warning(f"Completed {audit.entity_name} with issues: {result[1]}")
                else:
                    logger.warning(f"No result recorded for {audit.entity_name}")
            
            # Persist to database
            self._persist_table_audit_completion(audit, result)

    def _persist_table_audit_completion(self, audit, result):
        """Persist table audit completion with memory info."""
        try:
            import json
            from sqlalchemy import text
            
            completion_metadata = {
                "loading_completed": True,
                "completion_timestamp": audit.completed_at.isoformat() if audit.completed_at else None,
                "loading_success": result[0] if result else False,
                "rows_loaded": result[2] if result and len(result) > 2 else 0,
                "error_message": result[1] if result and not result[0] else None
            }
            
            # Add memory information if available
            status = self.memory_monitor.get_status_report()
            completion_metadata["memory_info"] = {
                "peak_usage_mb": status['usage_above_baseline_mb'],
                "final_pressure_level": status['pressure_level'],
                "budget_remaining_mb": status['budget_remaining_mb']
            }
            
            year = self.config.pipeline.year
            month = self.config.pipeline.month
            
            with self.audit_service.database.engine.begin() as conn:
                conn.execute(text('''
                    UPDATE table_audit_manifest 
                    SET completed_at = :completed_at,
                        notes = :metadata_json
                    WHERE entity_name = :entity_name 
                    AND ingestion_year = :year 
                    AND ingestion_month = :month
                '''), {
                    'completed_at': audit.completed_at,
                    'metadata_json': json.dumps(completion_metadata),
                    'entity_name': audit.entity_name,
                    'year': year,
                    'month': month
                })
                
                logger.debug(f"Persisted completion data for {audit.entity_name}")
                    
        except Exception as e:
            logger.error(f"Failed to persist audit completion for {audit.entity_name}: {e}")

    def _log_optimization_summary(self, configurations: Dict[str, Dict[str, Any]]):
        """Log summary of memory optimizations."""
        logger.info("[MemoryIntegratedService] Memory optimization summary:")
        for table_name, config in configurations.items():
            logger.info(f"  {table_name}: "
                       f"chunk={config.get('chunk_size', 'unknown')}, "
                       f"parallel={config.get('enable_parallelism', False)}, "
                       f"streaming={config.get('use_streaming', False)}")
            
            if 'memory_info' in config:
                mem_info = config['memory_info']
                logger.info(f"    Memory: {mem_info.get('available_mb', 0):.1f}MB available, "
                           f"pressure={mem_info.get('pressure_level', 0):.2f}")


