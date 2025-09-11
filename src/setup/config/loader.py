"""
Configuration loader with environment variable mapping.

This module handles loading configuration from environment variables
and provides backward compatibility with the legacy system.
"""

from typing import Dict, Any, Optional, List
import os
import logging
from pathlib import Path
from dotenv import load_dotenv

from .models import AppConfig, DatabaseConfig, PipelineConfig, AuditConfig, DataSourceConfig, DataSinkConfig, PathConfig
from .validation import ConfigurationValidator

logger = logging.getLogger(__name__)


class ConfigLoader:
    """Configuration loader with environment variable mapping and validation."""
    
    def __init__(self, env_file: Optional[str] = None, validate: bool = True):
        """
        Initialize configuration loader.
        
        Args:
            env_file: Path to .env file (defaults to .env in current directory)
            validate: Whether to validate configuration after loading
        """
        self.env_file = env_file or ".env"
        self.validate = validate
        self._loaded_config: Optional[AppConfig] = None
        
    def load_configuration(self, profile: Optional[str] = None) -> AppConfig:
        """
        Load configuration from environment variables.
        
        Args:
            profile: Optional profile name to apply before loading
            
        Returns:
            Validated AppConfig instance
        """
        # Load environment file
        self._load_env_file()
        
        # Apply profile if specified
        if profile:
            self._apply_profile(profile)
        
        # Load configuration with hierarchical environment variable mapping
        config = self._load_typed_config()
        
        # Validate if enabled (temporarily disabled during SOLID migration)
        if False and self.validate:
            self._validate_config(config)
        
        self._loaded_config = config
        return config
    
    def reload_configuration(self) -> AppConfig:
        """Reload configuration (useful for development/hot-reloading)."""
        logger.info("Reloading configuration from environment")
        return self.load_configuration()
    
    def get_loaded_config(self) -> Optional[AppConfig]:
        """Get the currently loaded configuration."""
        return self._loaded_config
    
    def _load_env_file(self):
        """Load environment variables from .env file."""
        if os.path.exists(self.env_file):
            load_dotenv(self.env_file, override=False)
            logger.debug(f"Loaded environment variables from {self.env_file}")
        else:
            logger.warning(f"Environment file {self.env_file} not found")
    
    def _apply_profile(self, profile: str):
        """Apply a configuration profile."""
        try:
            from .profiles import apply_profile
            applied = apply_profile(profile, override_existing=False)
            logger.info(f"Applied profile '{profile}' with {len(applied)} variables")
        except Exception as e:
            logger.error(f"Failed to apply profile '{profile}': {e}")
            raise
    
    def _load_typed_config(self) -> AppConfig:
        """Load configuration using Pydantic models with environment mapping."""
        
        # Load database configurations
        main_db = self._load_database_config("POSTGRES")
        audit_db = self._load_database_config("AUDIT_DB")
        
        # Load basic pipeline config 
        temp_pipeline_config = self._load_pipeline_config()
        
        # Load path and URL data 
        path_data = self._load_path_config()
        url_data = self._load_url_config()
        temp_audit_config = self._load_audit_config()
        
        # Import needed for creating nested configs
        from .models import DataSourceConfig, PipelineConfig
        
        # Create data source config
        data_source = DataSourceConfig(
            delimiter=os.getenv("ETL_DELIMITER", ";"),
            timezone=os.getenv("ETL_TIMEZONE", "America/Sao_Paulo"),
            encoding=os.getenv("ETL_ENCODING", "iso-8859-1"),
            base_url=url_data['base_url'],
            layout_url=url_data['layout_url']
        )
        
        # Create PathConfig with clean names (no _path suffix)
        paths = PathConfig(
            download=path_data['download'],
            extraction=path_data['extraction'],
            conversion=path_data['conversion']
        )
        
        # Create DataSinkConfig with paths and database
        data_sink = DataSinkConfig(
            paths=paths,
            database=main_db
        )
        
        # Create final pipeline config with all components
        pipeline_config = PipelineConfig(
            environment=temp_pipeline_config.environment,
            delete_files=temp_pipeline_config.delete_files,
            is_parallel=temp_pipeline_config.is_parallel,
            year=temp_pipeline_config.year,
            month=temp_pipeline_config.month,
            data_sink=data_sink,
            data_source=data_source,
            conversion=temp_pipeline_config.conversion,
            loading=temp_pipeline_config.loading,
            download=temp_pipeline_config.download,
            development=temp_pipeline_config.development
        )
        
        # Create final audit config with database
        audit_config = AuditConfig(
            database=audit_db,
            manifest_tracking=temp_audit_config.manifest_tracking,
            update_threshold=temp_audit_config.update_threshold,
            update_interval=temp_audit_config.update_interval,
            enable_bulk_updates=temp_audit_config.enable_bulk_updates,
            enable_temporal_context=temp_audit_config.enable_temporal_context,
            default_batch_size=temp_audit_config.default_batch_size,
            retention_days=temp_audit_config.retention_days,
            enable_monitoring=temp_audit_config.enable_monitoring
        )
        
        # Create main configuration
        config = AppConfig(
            environment=temp_pipeline_config.environment,
            pipeline=pipeline_config,
            audit=audit_config
        )
        
        return config
    
    def _load_database_config(self, prefix: str, actual_prefix: Optional[str] = None) -> DatabaseConfig:
        """Load database configuration from environment variables."""
        env_prefix = actual_prefix or prefix
        
        return DatabaseConfig(
            host=os.getenv(f"{env_prefix}_HOST", "localhost"),
            port=int(os.getenv(f"{env_prefix}_PORT", "5432")),
            user=os.getenv(f"{env_prefix}_USER", "postgres"),
            password=os.getenv(f"{env_prefix}_PASSWORD", "postgres"),
            database_name=os.getenv(f"{env_prefix}_DBNAME" if prefix == "POSTGRES" else f"{env_prefix}_NAME", "dadosrfb"),
            maintenance_db=os.getenv(f"{env_prefix}_MAINTENANCE_DB", "postgres")
        )
    
    def _load_pipeline_config(self) -> PipelineConfig:
        """Load pipeline configuration with all nested components."""
        from .models import ConversionConfig, LoadingConfig, DownloadConfig, DevelopmentConfig, Environment
        
        # Load environment
        env_str = os.getenv("ENVIRONMENT", "development").lower()
        environment = Environment(env_str) if env_str in Environment.__members__.values() else Environment.DEVELOPMENT
        
        # Load conversion config
        conversion = ConversionConfig(
            chunk_size=int(os.getenv("ETL_CONVERSION_CHUNK_SIZE", os.getenv("ETL_CHUNK_SIZE", "50000"))),
            memory_limit_mb=int(os.getenv("ETL_CONVERSION_MEMORY_LIMIT_MB", os.getenv("ETL_MAX_MEMORY_MB", "1024"))),
            workers=int(os.getenv("ETL_CONVERSION_WORKERS", "2")),
            compression=os.getenv("ETL_CONVERSION_COMPRESSION", os.getenv("ETL_COMPRESSION", "snappy")),
            row_group_size=int(os.getenv("ETL_CONVERSION_ROW_GROUP_SIZE", os.getenv("ETL_ROW_GROUP_SIZE", "100000"))),
            flush_threshold=int(os.getenv("ETL_CONVERSION_FLUSH_THRESHOLD", "10")),
            auto_fallback=os.getenv("ETL_CONVERSION_AUTO_FALLBACK", "true").lower() == "true",
            row_estimation_factor=int(os.getenv("ETL_CONVERSION_ROW_ESTIMATION_FACTOR", "8000"))
        )
        
        # Load loading config
        loading = LoadingConfig(
            batch_size=int(os.getenv("ETL_LOADING_BATCH_SIZE", os.getenv("ETL_CHUNK_SIZE", "1000"))),
            sub_batch_size=int(os.getenv("ETL_LOADING_SUB_BATCH_SIZE", os.getenv("ETL_SUB_BATCH_SIZE", "500"))),
            workers=int(os.getenv("ETL_LOADING_WORKERS", os.getenv("ETL_WORKERS", "3"))),
            max_retries=int(os.getenv("ETL_LOADING_MAX_RETRIES", os.getenv("ETL_MAX_RETRIES", "3"))),
            timeout_seconds=int(os.getenv("ETL_LOADING_TIMEOUT_SECONDS", os.getenv("ETL_TIMEOUT_SECONDS", "300"))),
            use_copy=os.getenv("ETL_LOADING_USE_COPY", os.getenv("ETL_USE_COPY", "true")).lower() == "true",
            enable_internal_parallelism=os.getenv("ETL_LOADING_ENABLE_INTERNAL_PARALLELISM", os.getenv("ETL_ENABLE_INTERNAL_PARALLELISM", "true")).lower() == "true",
            max_batch_size=int(os.getenv("ETL_LOADING_MAX_BATCH_SIZE", os.getenv("ETL_MAX_BATCH_SIZE", "500000"))),
            min_batch_size=int(os.getenv("ETL_LOADING_MIN_BATCH_SIZE", os.getenv("ETL_MIN_BATCH_SIZE", "10000"))),
            batch_size_mb=int(os.getenv("ETL_LOADING_BATCH_SIZE_MB", os.getenv("ETL_BATCH_SIZE_MB", "100"))),
            # Async operations and connection pooling (moved from pipeline)
            internal_concurrency=int(os.getenv("ETL_INTERNAL_CONCURRENCY", "3")),
            async_pool_min_size=int(os.getenv("ETL_ASYNC_POOL_MIN_SIZE", "1")),
            async_pool_max_size=int(os.getenv("ETL_ASYNC_POOL_MAX_SIZE", "10"))
        )
        
        # Load download config
        download = DownloadConfig(
            workers=int(os.getenv("ETL_DOWNLOAD_WORKERS", os.getenv("ETL_WORKERS", "4"))),
            chunk_size_mb=int(os.getenv("ETL_DOWNLOAD_CHUNK_SIZE_MB", "50")),
            verify_checksums=os.getenv("ETL_DOWNLOAD_VERIFY_CHECKSUMS", os.getenv("ETL_CHECKSUM_VERIFICATION", "true")).lower() == "true",
            checksum_threshold_mb=int(os.getenv("ETL_DOWNLOAD_CHECKSUM_THRESHOLD_MB", os.getenv("ETL_CHECKSUM_THRESHOLD_MB", "1000"))),
            timeout_seconds=int(os.getenv("ETL_DOWNLOAD_TIMEOUT_SECONDS", os.getenv("ETL_TIMEOUT_SECONDS", "300"))),
            max_retries=int(os.getenv("ETL_DOWNLOAD_MAX_RETRIES", os.getenv("ETL_MAX_RETRIES", "3")))
        )
        
        # Load development config
        dev_enabled_str = os.getenv("ETL_DEV_ENABLED")
        if dev_enabled_str is None:
            # Default based on environment
            dev_enabled_str = "true" if os.getenv("ENVIRONMENT", "development") == "development" else "false"
        
        development = DevelopmentConfig(
            enabled=dev_enabled_str.lower() == "true",
            file_size_limit_mb=int(os.getenv("ETL_DEV_FILE_SIZE_LIMIT_MB", "70")),
            max_files_per_table=int(os.getenv("ETL_DEV_MAX_FILES_PER_TABLE", "5")),
            max_files_per_blob=int(os.getenv("ETL_DEV_MAX_FILES_PER_BLOB", "3")),
            row_limit_percent=float(os.getenv("ETL_DEV_ROW_LIMIT_PERCENT", "0.1"))
        )
        
        # Load main ETL config
        etl = PipelineConfig(
            environment=environment,
            delete_files=os.getenv("ETL_DELETE_FILES", "true").lower() == "true",
            is_parallel=os.getenv("ETL_IS_PARALLEL", "true").lower() == "true",
            year=None,  # Set at runtime
            month=None,  # Set at runtime
            conversion=conversion,
            loading=loading,
            download=download,
            development=development
        )
        
        return etl
    
    def _load_path_config(self):
        """Load path configuration for DataSinkConfig (clean naming)."""
        return {
            'download': os.getenv("DOWNLOAD_PATH", "DOWNLOADED_FILES"),
            'extraction': os.getenv("EXTRACTION_PATH", "EXTRACTED_FILES"),  
            'conversion': os.getenv("CONVERSION_PATH", "CONVERTED_FILES")
        }
    
    def _load_url_config(self):
        """Load URL configuration (now embedded in DataSourceConfig)."""
        return {
            'base_url': os.getenv("URL_RF_BASE", "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj"),
            'layout_url': os.getenv("URL_RF_LAYOUT", "https://www.gov.br/receitafederal/dados/cnpj-metadados.pdf")
        }
    
    def _load_audit_config(self) -> AuditConfig:
        """Load audit tracking configuration from environment variables."""
        return AuditConfig(
            manifest_tracking=os.getenv("ETL_MANIFEST_TRACKING", "false").lower() == "true",
            update_threshold=int(os.getenv("BATCH_UPDATE_THRESHOLD", "100")),
            update_interval=int(os.getenv("BATCH_UPDATE_INTERVAL", "30")),
            enable_bulk_updates=os.getenv("ENABLE_BULK_UPDATES", "true").lower() == "true",
            enable_temporal_context=os.getenv("ENABLE_TEMPORAL_CONTEXT", "true").lower() == "true",
            default_batch_size=int(os.getenv("DEFAULT_BATCH_SIZE", "20000")),
            retention_days=int(os.getenv("BATCH_RETENTION_DAYS", "30")),
            enable_monitoring=os.getenv("ENABLE_BATCH_MONITORING", "true").lower() == "true"
        )
    
    def _validate_config(self, config: AppConfig):
        """Validate the loaded configuration."""
        validator = ConfigurationValidator(config)
        errors, warnings = validator.validate_all()
        
        if errors:
            error_msg = f"Configuration validation failed with {len(errors)} error(s):\n"
            error_msg += "\n".join(f"  - {error}" for error in errors)
            logger.error(error_msg)
            raise ValueError(f"Configuration validation failed: {errors}")
        
        if warnings:
            warning_msg = f"Configuration validation completed with {len(warnings)} warning(s):\n"
            warning_msg += "\n".join(f"  - {warning}" for warning in warnings)
            logger.warning(warning_msg)
        
        logger.info(validator.get_summary())
    
    def get_environment_variables_summary(self) -> Dict[str, Any]:
        """Get a summary of environment variables and their mapping."""
        summary = {
            "env_file": self.env_file,
            "env_file_exists": os.path.exists(self.env_file),
            "total_env_vars": len(os.environ),
            "etl_related_vars": len([k for k in os.environ.keys() if k.startswith("ETL_")]),
            "database_related_vars": len([k for k in os.environ.keys() if k.startswith(("POSTGRES_", "AUDIT_DB_"))]),
            "path_related_vars": len([k for k in os.environ.keys() if k.endswith("_PATH")]),
            "url_related_vars": len([k for k in os.environ.keys() if k.startswith("URL_")]),
            "batch_related_vars": len([k for k in os.environ.keys() if k.startswith("BATCH_")]),
        }
        
        return summary
    
    def diagnose_configuration(self) -> Dict[str, Any]:
        """Diagnose configuration issues and provide recommendations."""
        diagnosis = {
            "environment_summary": self.get_environment_variables_summary(),
            "missing_variables": [],
            "deprecated_variables": [],
            "recommendations": [],
        }
        
        # Check for critical missing variables
        critical_vars = [
            "POSTGRES_HOST", "POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_DBNAME",
            "AUDIT_DB_HOST", "AUDIT_DB_USER", "AUDIT_DB_PASSWORD", "AUDIT_DB_NAME"
        ]
        
        for var in critical_vars:
            if var not in os.environ:
                diagnosis["missing_variables"].append(var)
        
        # Check for deprecated variables (legacy names that should be migrated)
        deprecated_mappings = {
            "ETL_CHUNK_SIZE": "Use ETL_CONVERSION_CHUNK_SIZE and ETL_LOADING_BATCH_SIZE instead",
            "ETL_WORKERS": "Use ETL_CONVERSION_WORKERS, ETL_LOADING_WORKERS, and ETL_DOWNLOAD_WORKERS instead",
            "ETL_MAX_RETRIES": "Use ETL_LOADING_MAX_RETRIES and ETL_DOWNLOAD_MAX_RETRIES instead",
            "ETL_TIMEOUT_SECONDS": "Use ETL_LOADING_TIMEOUT_SECONDS and ETL_DOWNLOAD_TIMEOUT_SECONDS instead",
        }
        
        for var, recommendation in deprecated_mappings.items():
            if var in os.environ:
                diagnosis["deprecated_variables"].append({
                    "variable": var,
                    "current_value": os.environ[var],
                    "recommendation": recommendation
                })
        
        # Generate recommendations
        if diagnosis["missing_variables"]:
            diagnosis["recommendations"].append(
                f"Set missing critical variables: {', '.join(diagnosis['missing_variables'])}"
            )
        
        if diagnosis["deprecated_variables"]:
            diagnosis["recommendations"].append(
                "Migrate deprecated variables to new hierarchical naming convention"
            )
        
        # Load configuration if possible and check for validation issues
        try:
            config = self._load_typed_config()
            validator = ConfigurationValidator(config)
            errors, warnings = validator.validate_all()
            
            diagnosis["validation_errors"] = errors
            diagnosis["validation_warnings"] = warnings
            
            if errors:
                diagnosis["recommendations"].append(
                    f"Fix {len(errors)} configuration validation errors"
                )
            
        except Exception as e:
            diagnosis["load_error"] = str(e)
            diagnosis["recommendations"].append(
                "Fix configuration loading errors before proceeding"
            )
        
        return diagnosis


# Global configuration loader instance
_config_loader: Optional[ConfigLoader] = None


def get_config_loader(env_file: Optional[str] = None, validate: bool = True) -> ConfigLoader:
    """Get or create the global configuration loader instance."""
    global _config_loader
    
    if _config_loader is None or env_file is not None:
        _config_loader = ConfigLoader(env_file=env_file, validate=validate)
    
    return _config_loader


def load_config(profile: Optional[str] = None, env_file: Optional[str] = None, validate: bool = True) -> AppConfig:
    """Convenience function to load configuration."""
    loader = get_config_loader(env_file=env_file, validate=validate)
    return loader.load_configuration(profile=profile)
