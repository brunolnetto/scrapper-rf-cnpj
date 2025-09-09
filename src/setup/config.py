"""
Centralized configuration management for the CNPJ ETL project.

This module provides a unified interface for accessing configuration settings
from environment variables, eliminating hard-coded values throughout the codebase.
"""

from typing import Dict, Any, Optional, Tuple
from pathlib import Path
import os
from dataclasses import dataclass
from dotenv import load_dotenv

from .logging import logger


@dataclass
class DatabaseConfig:
    """Database connection configuration."""

    host: str
    port: int
    user: str
    password: str
    database_name: str
    maintenance_db: str = "postgres"

    def get_connection_string(self, db_name: Optional[str] = None) -> str:
        """Get PostgreSQL connection string."""
        target_db = db_name or self.database_name
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{target_db}"

    def get_maintenance_connection_string(self) -> str:
        """Get connection string for maintenance database."""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.maintenance_db}"

    def __repr__(self):
        return (
            f"DatabaseConfig(host={self.host}, port={self.port}, user={self.user}, "
            f"password={self.password}, database={self.database_name}, "
            f"maintenance_db={self.maintenance_db})"
        )

@dataclass
class ETLStageConfig:
    """Base configuration for ETL stages."""
    enabled: bool = True

@dataclass
class BatchTrackingConfig(ETLStageConfig):
    """Configuration for batch tracking system."""
    accumulation_enabled: bool = True  # Enable in-memory accumulation
    log_level: str = "INFO"  # Logging level for batch tracking
    metric_buffer_size: int = 1000  # Size of metric accumulation buffer
    update_threshold: int = 100  # Update metrics every N files
    update_interval: int = 30  # Update metrics every N seconds
    enable_bulk_updates: bool = True  # Enable bulk database updates
    enable_temporal_context: bool = True  # Include year/month in batch names
    default_batch_size: int = 20_000  # Default processing batch size
    retention_days: int = 30  # Keep batch records for N days
    enable_monitoring: bool = True  # Enable batch monitoring features


@dataclass
class DownloadConfig(ETLStageConfig):
    """Download stage configuration."""
    base_url: str = ""
    workers: int = 4
    chunk_size_mb: int = 50
    verify_checksums: bool = True
    checksum_threshold_bytes: int = 1_000_000_000  # Skip checksum for files > 1GB


@dataclass
class ConversionConfig(ETLStageConfig):
    """Conversion stage configuration."""
    chunk_size: int = 50000
    max_memory_mb: int = 1024
    compression: str = "snappy"
    row_group_size: int = 100000
    flush_threshold: int = 10
    auto_fallback: bool = True
    row_estimation_factor: int = 8000
    workers: int = 2
    cleanup_threshold_ratio: float = 0.7
    baseline_buffer_mb: int = 256
    max_file_size_mb: int = 1000
    enable_file_splitting: bool = False
    chunk_processing_delay: float = 0.0
    default_chunksize: int = 500_000


@dataclass
class LoadingConfig(ETLStageConfig):
    """Loading stage configuration."""
    batch_size: int = 50000
    max_batch_size: int = 500000
    min_batch_size: int = 10000
    batch_size_mb: int = 100
    sub_batch_size: int = 5000
    parallel_workers: int = 3
    internal_concurrency: int = 3
    use_copy: bool = True
    enable_internal_parallelism: bool = True
    manifest_tracking: bool = True
    async_pool_min_size: int = 2
    async_pool_max_size: int = 10


@dataclass
class DevelopmentConfig:
    """Development mode configuration."""
    enabled: bool = False
    file_size_limit_mb: int = 1000
    max_files_per_table: int = 3
    max_files_per_blob: int = 3
    row_limit_percent: float = 0.1
    max_blob_size_mb: int = 500
    sample_percentage: float = 0.1


@dataclass
class ETLConfig:
    """Unified ETL pipeline configuration with stage-specific settings."""
    download: DownloadConfig
    conversion: ConversionConfig
    loading: LoadingConfig
    development: DevelopmentConfig
    
    # Global ETL settings (accessed directly by the codebase)
    year: int
    month: int
    delimiter: str = ";"
    timezone: str = "America/Sao_Paulo"
    delete_files: bool = True
    is_parallel: bool = True
    
    # Legacy attributes needed by existing code
    chunk_size: int = 50000
    max_retries: int = 3
    timeout_seconds: int = 300
    parallel_workers: int = 4
    sub_batch_size: int = 5000
    enable_internal_parallelism: bool = True
    internal_concurrency: int = 3
    
    def get_stage_config(self, stage_name: str) -> ETLStageConfig:
        """Get configuration for a specific stage."""
        stage_map = {
            "download": self.download,
            "conversion": self.conversion,
            "loading": self.loading
        }
        
        if stage_name not in stage_map:
            raise ValueError(f"Unknown stage: {stage_name}. Available: {list(stage_map.keys())}")
        
        return stage_map[stage_name]
    
    def is_development_mode(self) -> bool:
        """Check if development mode is enabled."""
        return self.development.enabled
    
    @property
    def development_mode(self) -> bool:
        """Backward compatibility property for development mode."""
        return self.development.enabled


@dataclass
class PathConfig:
    """File path configuration."""

    download_path: Path
    extract_path: Path
    conversion_path: Path
    log_path: Path

    def ensure_directories_exist(self) -> None:
        """Ensure all configured directories exist."""
        for path in [self.download_path, self.extract_path, self.conversion_path, self.log_path]:
            path.mkdir(parents=True, exist_ok=True)


@dataclass
class URLConfig:
    """URL configuration for data sources."""

    base_url: str
    layout_url: str

    def get_files_url(self, year: int, month: int) -> str:
        """Get the URL for files of a specific year and month."""
        return f"{self.base_url}/{year}-{month:02d}"


class ConfigurationService:
    """Centralized configuration management."""

    def __init__(self, month: int, year: int, env_file: str = ".env"):
        """
        Initialize configuration service.

        Args:
            env_file: Path to the environment file
        """
        self.env_file = env_file
        self.month = month
        self.year = year
        self._load_configs()

    def _load_configs(self) -> None:
        """Load all configuration sections."""
        load_dotenv(self.env_file)

        self.databases = self._load_databases_config()
        self.etl = self._load_etl_config()
        self.paths = self._load_path_config()
        self.urls = self._load_url_config()
        self._batch_config = None  # Lazy-loaded

        # Skip directory creation during init - create them lazily when needed
        self.paths.ensure_directories_exist()

        logger.info("Configuration loaded successfully")

    @property
    def batch_config(self) -> BatchTrackingConfig:
        """Lazy-loaded batch tracking configuration."""
        if self._batch_config is None:
            self._batch_config = BatchTrackingConfig(
                update_threshold=int(os.getenv("BATCH_UPDATE_THRESHOLD", "100")),
                update_interval=int(os.getenv("BATCH_UPDATE_INTERVAL", "30")),
                enable_bulk_updates=os.getenv("ENABLE_BULK_UPDATES", "true").lower() == "true",
                enable_temporal_context=os.getenv("ENABLE_TEMPORAL_CONTEXT", "true").lower() == "true",
                default_batch_size=int(os.getenv("DEFAULT_BATCH_SIZE", "20000")),
                retention_days=int(os.getenv("BATCH_RETENTION_DAYS", "30")),
                enable_monitoring=os.getenv("ENABLE_BATCH_MONITORING", "true").lower() == "true"
            )
        return self._batch_config

    def _load_main_database_config(self) -> DatabaseConfig:
        """Load database configuration from environment variables."""
        return DatabaseConfig(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            user=os.getenv("POSTGRES_USER", "postgres"),
            password=os.getenv("POSTGRES_PASSWORD", "postgres"),
            database_name=os.getenv("POSTGRES_DBNAME", "dadosrfb"),
            maintenance_db=os.getenv("POSTGRES_MAINTENANCE_DB", "postgres"),
        )

    def _load_audit_database_config(self) -> DatabaseConfig:
        return DatabaseConfig(
            host=os.getenv("AUDIT_DB_HOST", "localhost"),
            port=int(os.getenv("AUDIT_DB_PORT", "5432")),
            user=os.getenv("AUDIT_DB_USER", "postgres"),
            password=os.getenv("AUDIT_DB_PASSWORD", "postgres"),
            database_name=os.getenv("AUDIT_DB_NAME", "dadosrfb_analysis"),
            maintenance_db="",  # Not used for audit DB
        )

    def _load_databases_config(self) -> Dict[str, DatabaseConfig]:
        """Load all database configurations."""
        return {
            "main": self._load_main_database_config(),
            "audit": self._load_audit_database_config(),
        }

    def _load_etl_config(self) -> ETLConfig:
        """Load ETL configuration from environment variables."""
        
        # Create stage-specific configurations
        download_config = DownloadConfig(
            base_url=os.getenv("URL_RF_BASE", "https://dadosabertos.rfb.gov.br/CNPJ"),
            workers=int(os.getenv("ETL_WORKERS", "4")),
            chunk_size_mb=int(os.getenv("ETL_DOWNLOAD_CHUNK_SIZE_MB", "50")),
            verify_checksums=os.getenv("ETL_CHECKSUM_VERIFICATION", "true").lower() == "true",
            checksum_threshold_bytes=int(os.getenv("ETL_CHECKSUM_THRESHOLD_BYTES", "1000000000"))
        )

        conversion_config = ConversionConfig(
            chunk_size=int(os.getenv("ETL_CHUNK_SIZE", "50000")),
            max_memory_mb=int(os.getenv("ETL_MAX_MEMORY_MB", "1024")),
            compression=os.getenv("ETL_COMPRESSION", "snappy"),
            row_group_size=int(os.getenv("ETL_ROW_GROUP_SIZE", "100000")),
            flush_threshold=int(os.getenv("ETL_CONVERSION_FLUSH_THRESHOLD", "10")),
            auto_fallback=os.getenv("ETL_CONVERSION_AUTO_FALLBACK", "true").lower() == "true",
            row_estimation_factor=int(os.getenv("ETL_CONVERSION_ROW_ESTIMATION_FACTOR", "8000"))
        )
        
        loading_config = LoadingConfig(
            batch_size=int(os.getenv("ETL_CHUNK_SIZE", "50000")),
            sub_batch_size=int(os.getenv("ETL_SUB_BATCH_SIZE", "5000")),
            min_batch_size=int(os.getenv("ETL_MIN_BATCH_SIZE", "10000")),
            max_batch_size=int(os.getenv("ETL_MAX_BATCH_SIZE", "500000")),
            batch_size_mb=int(os.getenv("ETL_BATCH_SIZE_MB", "100")),
            parallel_workers=int(os.getenv("ETL_WORKERS", "4")),
            internal_concurrency=int(os.getenv("ETL_INTERNAL_CONCURRENCY", "3")),
            use_copy=os.getenv("ETL_USE_COPY", "true").lower() == "true",
            enable_internal_parallelism=os.getenv("ETL_ENABLE_INTERNAL_PARALLELISM", "true").lower() == "true",
            manifest_tracking=os.getenv("ETL_MANIFEST_TRACKING", "true").lower() == "true",
            async_pool_min_size=int(os.getenv("ETL_ASYNC_POOL_MIN_SIZE", "2")),
            async_pool_max_size=int(os.getenv("ETL_ASYNC_POOL_MAX_SIZE", "10"))
        )
        
        development_config = DevelopmentConfig(
            enabled=os.getenv("ENVIRONMENT", "development").lower() == "development",
            file_size_limit_mb=int(os.getenv("ETL_DEV_FILE_SIZE_LIMIT", "70000000")) // (1024 * 1024),  # Convert bytes to MB
            max_files_per_table=int(os.getenv("ETL_DEV_MAX_FILES_PER_TABLE", "3")),
            max_files_per_blob=int(os.getenv("ETL_DEV_MAX_FILES_PER_BLOB", "3")),
            row_limit_percent=float(os.getenv("ETL_DEV_ROW_LIMIT_PERCENT", "0.1")),
            max_blob_size_mb=int(os.getenv("ETL_DEV_MAX_BLOB_SIZE_MB", "500")),
            sample_percentage=float(os.getenv("ETL_DEV_SAMPLE_PERCENTAGE", "0.1"))
        )

        return ETLConfig(
            download=download_config,
            conversion=conversion_config,
            loading=loading_config,
            development=development_config,
            year=int(self.year),
            month=int(self.month),
            delimiter=os.getenv("ETL_FILE_DELIMITER", ";"),
            timezone=os.getenv("ETL_TIMEZONE", "America/Sao_Paulo"),
            delete_files=os.getenv("ETL_DELETE_FILES", "true").lower() == "true",
            is_parallel=os.getenv("ETL_IS_PARALLEL", "true").lower() == "true",
            # Legacy attributes for backward compatibility
            chunk_size=conversion_config.chunk_size,
            max_retries=int(os.getenv("ETL_MAX_RETRIES", "3")),
            timeout_seconds=int(os.getenv("ETL_TIMEOUT_SECONDS", "300")),
            parallel_workers=loading_config.parallel_workers,
            sub_batch_size=loading_config.sub_batch_size,
            enable_internal_parallelism=loading_config.enable_internal_parallelism,
            internal_concurrency=loading_config.internal_concurrency
        )

    def _load_path_config(self) -> PathConfig:
        """Load path configuration from environment variables with temporal versioning."""
        root_path = Path.cwd()
        data_path = Path.cwd() / "data"
        
        # Create temporal subdirectory based on ETL year-month
        temporal_suffix = f"{self.etl.year}-{self.etl.month:02d}"
        
        return PathConfig(
            download_path=data_path / os.getenv("DOWNLOAD_PATH", "DOWNLOADED_FILES") / temporal_suffix,
            extract_path=data_path / os.getenv("EXTRACT_PATH", "EXTRACTED_FILES") / temporal_suffix,
            conversion_path=data_path / os.getenv("CONVERT_PATH", "CONVERTED_FILES") / temporal_suffix,
            log_path=root_path / "logs",
        )

    def _load_url_config(self) -> URLConfig:
        """Load URL configuration from environment variables."""
        return URLConfig(
            base_url=os.getenv("URL_RF_BASE"), 
            layout_url=os.getenv("URL_RF_LAYOUT")
        )

    def is_development_mode(self) -> bool:
        """Check if development mode is enabled."""
        return self.etl.development.enabled

    def get_file_size_limit(self) -> int:
        """Get file size limit for development mode (in bytes)."""
        return self.etl.development.file_size_limit_mb * 1024 * 1024

    def get_max_files_per_table(self) -> int:
        """Get maximum files per table for development mode."""
        return self.etl.development.max_files_per_table

    def get_max_files_per_blob(self) -> int:
        """Get maximum files per ZIP blob for development mode."""
        return self.etl.development.max_files_per_blob

    def get_sample_percentage(self) -> float:
        """Get sample percentage for development mode."""
        return self.etl.development.sample_percentage

    def get_year(self) -> int:
        """Get the configured year for data processing."""
        return self.etl.year

    def get_month(self) -> int:
        """Get the configured month for data processing."""
        return self.etl.month

    def set_temporal_config(self, year: int = None, month: int = None) -> None:
        """Update temporal configuration for data processing."""
        if year is not None:
            self.etl.year = year
        if month is not None:
            self.etl.month = month
        logger.info(f"Updated temporal config: year={self.etl.year}, month={self.etl.month}")

    def reload(self) -> None:
        """Reload configuration from environment file."""
        logger.info("Reloading configuration...")
        self._load_configs()

    def validate(self) -> bool:
        """Validate configuration settings."""
        errors = []

        # Validate database config
        if not self.main_database_config.host:
            errors.append("Database host is not configured")
        if not self.main_database_config.user:
            errors.append("Database user is not configured")
        if not self.main_database_config.password:
            errors.append("Database password is not configured")

        # Validate ETL config
        if self.etl.chunk_size <= 0:
            errors.append("ETL chunk size must be positive")
        if self.etl.max_retries <= 0:
            errors.append("ETL max retries must be positive")
        if self.etl.parallel_workers <= 0:
            errors.append("ETL parallel workers must be positive")

        # Validate paths
        if not self.paths.download_path:
            errors.append("Download path is not configured")
        if not self.paths.extract_path:
            errors.append("Extract path is not configured")

        if errors:
            for error in errors:
                logger.error(f"Configuration validation error: {error}")
            return False

        logger.info("Configuration validation passed")
        return True

    def get_summary(self) -> Dict[str, Any]:
        """Get a summary of current configuration."""
        return {
            "databases": {
                "main": {
                    "host": self.databases["main"].host,
                    "port": self.databases["main"].port,
                    "database": self.databases["main"].database_name,
                    "maintenance_db": self.databases["main"].maintenance_db,
                },
                "audit": {
                    "host": self.databases["audit"].host,
                    "port": self.databases["audit"].port,
                    "database": self.databases["audit"].database_name,
                },
            },
            "etl": {
                "delimiter": self.etl.delimiter, 
                "chunk_size": self.etl.chunk_size,
                "max_retries": self.etl.max_retries,
                "parallel_workers": self.etl.parallel_workers,
                "delete_files": self.etl.delete_files,
                "is_parallel": self.etl.is_parallel,
                "development_mode": self.etl.development_mode,
                "year": self.etl.year,
                "month": self.etl.month,
                "timezone": self.etl.timezone,
            },
            "paths": {
                "download_path": str(self.paths.download_path),
                "extract_path": str(self.paths.extract_path),
                "conversion_path": str(self.paths.conversion_path),
                "log_path": str(self.paths.log_path),
            },
            "urls": {
                "base_url": self.urls.base_url,
                "layout_url": self.urls.layout_url,
            },
        }


# Global configuration cache
_config_cache: Dict[Tuple[int, int], ConfigurationService] = {}


def get_config(year: int = None, month: int = None) -> ConfigurationService:
    """Get the configuration service instance with caching based on year/month."""
    global _config_cache
    
    # Use current date as defaults if not specified
    if year is None or month is None:
        from datetime import datetime
        current = datetime.now()
        year = year or current.year
        month = month or current.month
    
    # Use cached instance if available
    cache_key = (year, month)
    if cache_key not in _config_cache:
        _config_cache[cache_key] = ConfigurationService(month=month, year=year)
    
    return _config_cache[cache_key]


def reload_config() -> None:
    """Reload the global configuration by clearing the cache."""
    global _config_cache
    _config_cache.clear()


# Backward compatibility functions
def get_databases_config() -> Dict[str, DatabaseConfig]:
    """Get database configuration (backward compatibility)."""
    return get_config().databases


def get_etl_config() -> ETLConfig:
    """Get ETL configuration (backward compatibility)."""
    return get_config().etl


def get_path_config() -> PathConfig:
    """Get path configuration (backward compatibility)."""
    return get_config().paths


def get_url_config() -> URLConfig:
    """Get URL configuration (backward compatibility)."""
    return get_config().urls


def get_batch_config() -> BatchTrackingConfig:
    """Get batch tracking configuration (backward compatibility)."""
    return get_config().batch_config