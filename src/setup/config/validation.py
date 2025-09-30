"""
Configuration validation layer.

This module provides comprehensive validation of configuration consistency
and relationships between different configuration sections.
"""

from typing import List, Tuple, Optional
import logging
import os
import re

from .models import AppConfig, Environment

logger = logging.getLogger(__name__)


class ConfigurationValidator:
    """Comprehensive configuration validation."""
    
    def __init__(self, config: AppConfig):
        self.config = config
        self.errors: List[str] = []
        self.warnings: List[str] = []
    
    def validate_all(self) -> Tuple[List[str], List[str]]:
        """Run all validations and return errors and warnings."""
        self.errors.clear()
        self.warnings.clear()
        
        self._validate_memory_constraints()
        self._validate_worker_relationships()
        self._validate_batch_hierarchies()
        self._validate_environment_specific()
        self._validate_database_connectivity()
        self._validate_path_accessibility()
        self._validate_url_connectivity()
        
        return self.errors, self.warnings
    
    def _validate_memory_constraints(self):
        """Validate memory usage will not exceed limits."""
        try:
            memory_config = self.config.pipeline.memory
            conversion_config = self.config.pipeline.conversion
            
            # Estimate memory usage for conversion (approximate)
            estimated_memory_mb = (
                conversion_config.chunk_size * 
                conversion_config.workers * 
                conversion_config.row_estimation_factor
            ) / (1024 * 1024)
            
            if estimated_memory_mb > memory_config.memory_limit_mb:
                self.errors.append(
                    f"Estimated conversion memory {estimated_memory_mb:.1f}MB exceeds memory_limit_mb {memory_config.memory_limit_mb}MB"
                )
            
            # Warn if memory usage is very high (>80% of limit)
            if estimated_memory_mb > memory_config.memory_limit_mb * 0.8:
                self.warnings.append(
                    f"Estimated conversion memory {estimated_memory_mb:.1f}MB is >80% of memory limit ({memory_config.memory_limit_mb}MB)"
                )
        except Exception as e:
            self.errors.append(f"Memory validation failed: {str(e)}")
    
    def _validate_worker_relationships(self):
        """Validate worker counts make sense for system."""
        try:
            pipeline = self.config.pipeline
            total_workers = (
                pipeline.conversion.workers + 
                pipeline.loading.workers + 
                pipeline.download.workers
            )
            
            available_cores = os.cpu_count() or 4
            
            # Error if total workers significantly exceed available cores
            if total_workers > available_cores * 2:
                self.errors.append(
                    f"Total workers ({total_workers}) is > 2x available cores ({available_cores}); risk of oversubscription"
                )
            elif total_workers > int(available_cores * 1.5):
                self.warnings.append(
                    f"Total workers ({total_workers}) is >1.5x available cores ({available_cores}); consider reducing parallelism"
                )
            
            # Validate individual worker counts
            if pipeline.conversion.workers > max(1, available_cores):
                self.warnings.append(
                    f"Conversion workers ({pipeline.conversion.workers}) exceed available cores ({available_cores})"
                )
            
            if pipeline.loading.workers > max(1, available_cores // 2):
                self.warnings.append(
                    f"Loading workers ({pipeline.loading.workers}) is large relative to available cores ({available_cores})"
                )
        except Exception as e:
            self.errors.append(f"Worker validation failed: {str(e)}")
    
    def _validate_batch_hierarchies(self):
        """Validate batch size relationships are logical."""
        try:
            loading = self.config.pipeline.loading
            conversion = self.config.pipeline.conversion
            
            # Sub-batch size vs batch size
            if loading.sub_batch_size > loading.batch_size:
                self.errors.append(
                    f"Loading.sub_batch_size ({loading.sub_batch_size}) cannot be greater than loading.batch_size ({loading.batch_size})"
                )
            
            # Min vs max batch sizes
            if loading.min_batch_size > loading.max_batch_size:
                self.errors.append(
                    f"Loading.min_batch_size ({loading.min_batch_size}) is greater than loading.max_batch_size ({loading.max_batch_size})"
                )
            
            # Batch size vs min/max ranges
            if loading.batch_size < loading.min_batch_size:
                self.errors.append(
                    f"Loading.batch_size ({loading.batch_size}) is smaller than min_batch_size ({loading.min_batch_size})"
                )
            
            if loading.batch_size > loading.max_batch_size:
                self.errors.append(
                    f"Loading.batch_size ({loading.batch_size}) is greater than max_batch_size ({loading.max_batch_size})"
                )
        except Exception as e:
            self.errors.append(f"Batch hierarchy validation failed: {str(e)}")
    
    def _validate_environment_specific(self):
        """Apply environment-specific validations."""
        try:
            pipeline = self.config.pipeline
            available_cores = os.cpu_count() or 4
            
            if pipeline.environment == Environment.DEVELOPMENT:
                # Development can be relaxed but should warn on production-like settings
                if pipeline.loading.batch_size > 100000:
                    self.warnings.append("Development environment with very large loading.batch_size; consider lowering for dev runs")
            
            elif pipeline.environment == Environment.PRODUCTION:
                # Stricter requirements for production
                if pipeline.conversion.workers > available_cores * 2:
                    self.errors.append("Production conversion.workers too large relative to available cores")
                if pipeline.development and getattr(pipeline.development, "enabled", False):
                    self.warnings.append("Development mode enabled in production environment")
            
            elif pipeline.environment == Environment.TESTING:
                # Testing should use small batches to speed up tests
                if pipeline.loading.batch_size > 5000:
                    self.warnings.append("Testing environment with large loading.batch_size; consider reducing to speed tests")
        except Exception as e:
            self.errors.append(f"Environment-specific validation failed: {str(e)}")
    
    def _validate_database_connectivity(self):
        """Validate database configuration completeness."""
        try:
            # Validate pipeline main DB (attached to data_sink) and audit DB (attached to audit)
            candidates = {
                "pipeline_main": getattr(self.config.pipeline.data_sink, "database", None),
                "audit_db": getattr(self.config.audit, "database", None)
            }
            for db_name, db_config in candidates.items():
                if db_config is None:
                    self.errors.append(f"Database '{db_name}' is not configured")
                    continue

                if not getattr(db_config, "host", None):
                    self.errors.append(f"Database '{db_name}' missing host configuration")

                if not getattr(db_config, "user", None):
                    self.errors.append(f"Database '{db_name}' missing user configuration")

                if not getattr(db_config, "password", None):
                    self.warnings.append(f"Database '{db_name}' missing password configuration")

                if not getattr(db_config, "database_name", None):
                    self.errors.append(f"Database '{db_name}' missing database name")

                port = getattr(db_config, "port", None)
                try:
                    port_int = int(port)
                    if not (1 <= port_int <= 65535):
                        self.errors.append(f"Database '{db_name}' port ({port}) out of valid range")
                except Exception:
                    self.errors.append(f"Database '{db_name}' has invalid port: {port}")
                    
        except Exception as e:
            self.errors.append(f"Database validation failed: {str(e)}")
    
    def _validate_path_accessibility(self):
        """Validate path configuration accessibility."""
        try:
            paths = self.config.pipeline.data_sink.paths
            
            # Check if paths are reasonable
            for path_name, path_value in [
                ("download", paths.download),
                ("extraction", paths.extraction), 
                ("conversion", paths.conversion)
            ]:
                if not path_value:
                    self.errors.append(f"Path '{path_name}' is not configured")
                    continue

                # If path exists ensure it's a directory and writable
                if os.path.exists(path_value):
                    if not os.path.isdir(path_value):
                        self.errors.append(f"Path '{path_name}' ({path_value}) exists but is not a directory")
                    else:
                        # Check writability
                        if not os.access(path_value, os.W_OK):
                            self.warnings.append(f"Path '{path_name}' ({path_value}) is not writable by the current user")
                else:
                    # Not existing is a warning (we often create dirs at runtime)
                    self.warnings.append(f"Path '{path_name}' ({path_value}) does not exist; it will be created at runtime if needed")
        except Exception as e:
            self.errors.append(f"Path validation failed: {str(e)}")
    
    def _validate_url_connectivity(self):
        """Validate URL configuration format."""
        try:
            # URLs are stored on pipeline.data_source in current loader
            ds = getattr(self.config.pipeline, "data_source", None)
            if ds is None:
                self.errors.append("Data source configuration (URLs) is missing")
                return

            url_map = {
                "base_url": getattr(ds, "base_url", None),
                "layout_url": getattr(ds, "layout_url", None)
            }

            for url_name, url_value in url_map.items():
                if not url_value:
                    self.errors.append(f"URL '{url_name}' is empty")
                    continue

                if not isinstance(url_value, str) or not url_value.startswith(("http://", "https://")):
                    self.errors.append(f"URL '{url_name}' ({url_value}) must start with http:// or https://")

                if (self.config.pipeline.environment == Environment.PRODUCTION and "localhost" in url_value):
                    self.warnings.append(f"URL '{url_name}' contains localhost in production environment")
        except Exception as e:
            self.errors.append(f"URL validation failed: {str(e)}")
    
    def has_errors(self) -> bool:
        """Check if there are any validation errors."""
        return len(self.errors) > 0
    
    def has_warnings(self) -> bool:
        """Check if there are any validation warnings."""
        return len(self.warnings) > 0
    
    def get_summary(self) -> str:
        """Get a summary of validation results."""
        error_count = len(self.errors)
        warning_count = len(self.warnings)
        
        if error_count == 0 and warning_count == 0:
            return "Configuration validation: OK (no errors or warnings)"
        
        summary = []
        if error_count > 0:
            summary.append(f"{error_count} error(s)")
        if warning_count > 0:
            summary.append(f"{warning_count} warning(s)")
        
        return f"Configuration validation completed: {', '.join(summary)}"
    
    def log_results(self, logger: Optional[logging.Logger] = None):
        """Log validation results."""
        log = logger or logging.getLogger(__name__)
        
        if self.has_errors():
            for e in self.errors:
                log.error(e)
        
        if self.has_warnings():
            for w in self.warnings:
                log.warning(w)
        
        if not self.has_errors() and not self.has_warnings():
            log.info("Configuration validation passed with no issues")


def validate_configuration(config: AppConfig) -> Tuple[List[str], List[str]]:
    """Convenience function to validate configuration."""
    validator = ConfigurationValidator(config)
    return validator.validate_all()