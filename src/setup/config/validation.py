"""
Configuration validation layer.

This module provides comprehensive validation of configuration consistency
and relationships between different configuration sections.
"""

from typing import List, Tuple, Optional
import os
import logging
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
            conversion = self.config.etl.conversion
            
            # Estimate memory usage for conversion
            estimated_memory_mb = (
                conversion.chunk_size * 
                conversion.workers * 
                conversion.row_estimation_factor
            ) / (1024 * 1024)
            
            if estimated_memory_mb > conversion.memory_limit_mb:
                self.errors.append(
                    f"Estimated memory usage ({estimated_memory_mb:.1f}MB) exceeds "
                    f"conversion memory limit ({conversion.memory_limit_mb}MB). "
                    f"Consider reducing chunk_size ({conversion.chunk_size}) or "
                    f"workers ({conversion.workers})"
                )
            
            # Warn if memory usage is very high
            if estimated_memory_mb > conversion.memory_limit_mb * 0.8:
                self.warnings.append(
                    f"High memory usage ({estimated_memory_mb:.1f}MB) approaching "
                    f"limit ({conversion.memory_limit_mb}MB)"
                )
                
        except Exception as e:
            self.errors.append(f"Memory validation failed: {str(e)}")
    
    def _validate_worker_relationships(self):
        """Validate worker counts make sense for system."""
        try:
            etl = self.config.etl
            total_workers = (
                etl.conversion.workers + 
                etl.loading.workers + 
                etl.download.workers
            )
            
            available_cores = os.cpu_count() or 4
            
            # Error if total workers significantly exceed available cores
            if total_workers > available_cores * 2:
                self.errors.append(
                    f"Total workers ({total_workers}) significantly exceeds "
                    f"available CPU cores ({available_cores}). This may cause "
                    f"performance degradation."
                )
            
            # Warning if workers exceed cores by reasonable margin
            elif total_workers > available_cores * 1.5:
                self.warnings.append(
                    f"Total workers ({total_workers}) exceeds available "
                    f"CPU cores ({available_cores}) by significant margin"
                )
            
            # Validate individual worker counts
            if etl.conversion.workers > available_cores:
                self.warnings.append(
                    f"Conversion workers ({etl.conversion.workers}) exceed "
                    f"available cores ({available_cores})"
                )
            
            if etl.loading.workers > available_cores // 2:
                self.warnings.append(
                    f"Loading workers ({etl.loading.workers}) may be too high "
                    f"for database operations"
                )
                
        except Exception as e:
            self.errors.append(f"Worker validation failed: {str(e)}")
    
    def _validate_batch_hierarchies(self):
        """Validate batch size relationships are logical."""
        try:
            loading = self.config.etl.loading
            conversion = self.config.etl.conversion
            
            # Loading batch size vs conversion chunk size
            if loading.batch_size > conversion.chunk_size:
                self.errors.append(
                    f"Loading batch size ({loading.batch_size}) cannot exceed "
                    f"conversion chunk size ({conversion.chunk_size})"
                )
            
            # Sub-batch size vs batch size
            if loading.sub_batch_size > loading.batch_size:
                self.errors.append(
                    f"Loading sub-batch size ({loading.sub_batch_size}) cannot exceed "
                    f"batch size ({loading.batch_size})"
                )
            
            # Min vs max batch sizes
            if loading.min_batch_size > loading.max_batch_size:
                self.errors.append(
                    f"Min batch size ({loading.min_batch_size}) cannot exceed "
                    f"max batch size ({loading.max_batch_size})"
                )
            
            # Batch size vs min/max ranges
            if loading.batch_size < loading.min_batch_size:
                self.warnings.append(
                    f"Batch size ({loading.batch_size}) is below minimum "
                    f"({loading.min_batch_size})"
                )
            
            if loading.batch_size > loading.max_batch_size:
                self.warnings.append(
                    f"Batch size ({loading.batch_size}) exceeds maximum "
                    f"({loading.max_batch_size})"
                )
                
        except Exception as e:
            self.errors.append(f"Batch hierarchy validation failed: {str(e)}")
    
    def _validate_environment_specific(self):
        """Apply environment-specific validations."""
        try:
            etl = self.config.etl
            
            if etl.environment == Environment.DEVELOPMENT:
                # Development-specific warnings
                if etl.conversion.chunk_size > 10000:
                    self.warnings.append(
                        f"Large conversion chunk size ({etl.conversion.chunk_size}) "
                        f"in development mode may cause memory issues"
                    )
                
                if etl.loading.batch_size > 5000:
                    self.warnings.append(
                        f"Large loading batch size ({etl.loading.batch_size}) "
                        f"in development mode may be inefficient"
                    )
                
                if not etl.development.enabled:
                    self.warnings.append(
                        "Development optimizations are disabled in development environment"
                    )
            
            elif etl.environment == Environment.PRODUCTION:
                # Production-specific validations
                if etl.development.enabled:
                    self.errors.append(
                        "Development mode cannot be enabled in production environment"
                    )
                
                if etl.conversion.chunk_size < 10000:
                    self.warnings.append(
                        f"Small conversion chunk size ({etl.conversion.chunk_size}) "
                        f"in production may be inefficient"
                    )
                
                if etl.loading.batch_size < 1000:
                    self.warnings.append(
                        f"Small loading batch size ({etl.loading.batch_size}) "
                        f"in production may be inefficient"
                    )
            
            elif etl.environment == Environment.TESTING:
                # Testing-specific validations
                if etl.conversion.chunk_size > 5000:
                    self.warnings.append(
                        f"Large conversion chunk size ({etl.conversion.chunk_size}) "
                        f"in testing environment may slow down tests"
                    )
                    
        except Exception as e:
            self.errors.append(f"Environment-specific validation failed: {str(e)}")
    
    def _validate_database_connectivity(self):
        """Validate database configuration completeness."""
        try:
            for db_name, db_config in self.config.databases.items():
                if not db_config.host:
                    self.errors.append(f"Database '{db_name}' missing host configuration")
                
                if not db_config.user:
                    self.errors.append(f"Database '{db_name}' missing user configuration")
                
                if not db_config.password:
                    self.warnings.append(f"Database '{db_name}' missing password configuration")
                
                if not db_config.database_name:
                    self.errors.append(f"Database '{db_name}' missing database name")
                
                # Validate port ranges
                if not (1 <= db_config.port <= 65535):
                    self.errors.append(
                        f"Database '{db_name}' port ({db_config.port}) out of valid range"
                    )
                    
        except Exception as e:
            self.errors.append(f"Database validation failed: {str(e)}")
    
    def _validate_path_accessibility(self):
        """Validate path configuration accessibility."""
        try:
            paths = self.config.paths
            
            # Check if paths are reasonable
            for path_name, path_value in [
                ("download_path", paths.download_path),
                ("extract_path", paths.extract_path), 
                ("conversion_path", paths.conversion_path)
            ]:
                if not path_value:
                    self.errors.append(f"Path '{path_name}' is empty")
                    continue
                
                # Check for absolute paths that might be problematic
                if os.path.isabs(path_value) and not os.path.exists(os.path.dirname(path_value)):
                    self.warnings.append(
                        f"Path '{path_name}' ({path_value}) parent directory does not exist"
                    )
                    
        except Exception as e:
            self.errors.append(f"Path validation failed: {str(e)}")
    
    def _validate_url_connectivity(self):
        """Validate URL configuration format."""
        try:
            urls = self.config.urls
            
            # Basic URL format validation
            for url_name, url_value in [
                ("base_url", urls.base_url),
                ("layout_url", urls.layout_url)
            ]:
                if not url_value:
                    self.errors.append(f"URL '{url_name}' is empty")
                    continue
                
                if not url_value.startswith(('http://', 'https://')):
                    self.errors.append(
                        f"URL '{url_name}' ({url_value}) must start with http:// or https://"
                    )
                
                # Check for localhost in production
                if (self.config.etl.environment == Environment.PRODUCTION and 
                    'localhost' in url_value):
                    self.warnings.append(
                        f"URL '{url_name}' contains localhost in production environment"
                    )
                    
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
            return "✅ Configuration validation passed with no issues"
        
        summary = []
        if error_count > 0:
            summary.append(f"❌ {error_count} error(s)")
        if warning_count > 0:
            summary.append(f"⚠️ {warning_count} warning(s)")
        
        return f"Configuration validation completed: {', '.join(summary)}"
    
    def log_results(self, logger: Optional[logging.Logger] = None):
        """Log validation results."""
        log = logger or logging.getLogger(__name__)
        
        if self.has_errors():
            log.error("Configuration validation errors:")
            for error in self.errors:
                log.error(f"  - {error}")
        
        if self.has_warnings():
            log.warning("Configuration validation warnings:")
            for warning in self.warnings:
                log.warning(f"  - {warning}")
        
        if not self.has_errors() and not self.has_warnings():
            log.info("Configuration validation passed successfully")


def validate_configuration(config: AppConfig) -> Tuple[List[str], List[str]]:
    """Convenience function to validate configuration."""
    validator = ConfigurationValidator(config)
    return validator.validate_all()
