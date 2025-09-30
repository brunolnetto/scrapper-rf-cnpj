"""
Pydantic configuration models with validation.

This module defines typed configuration models following SOLID principles
with semantic clarity and comprehensive validation.

Configuration Architecture:
==========================

DataSourceConfig: Brazilian Federal Revenue specific settings (delimiter, timezone, URLs, encoding)
PathConfig: Clean file system paths (download, extraction, conversion) - no suffixes, clear names
DataSinkConfig: Output paths and database configuration for ETL pipeline results
ConversionConfig: CSV to Parquet conversion settings
LoadingConfig: Database loading and batching settings  
DownloadConfig: File download and verification settings
DevelopmentConfig: Development mode optimizations
AuditConfig: Audit database and tracking configuration
PipelineConfig: Main pipeline orchestration (combines data source, data sink, and operation configs)
AppConfig: Top-level application configuration (combines pipeline and audit)

Clean Architecture Principles:
=============================
- PathConfig: Pure path management with clean naming (download, extraction, conversion)
- DataSinkConfig: Groups output concerns (paths + database) for SOLID compliance
- Direct access properties: config.pipeline.database, config.pipeline.paths
- Semantic clarity: extraction (not extract), database (not main_database)
- No legacy compatibility - clean, modern architecture only
"""

from pydantic import (
    BaseModel, 
    Field, 
    field_validator, 
    model_validator, 
    PrivateAttr
)
from enum import Enum
from typing import Literal, Optional, Dict, Any
from pathlib import Path
import os


class Environment(str, Enum):
    """Application environment enumeration."""
    DEVELOPMENT = "development"
    PRODUCTION = "production"
    TESTING = "testing"


class DatabaseConfig(BaseModel):
    """Database connection configuration with validation."""
    
    host: str = Field(default="localhost", description="Database host")
    port: int = Field(default=5432, ge=1, le=65535, description="Database port")
    user: str = Field(default="postgres", description="Database username")
    password: str = Field(default="postgres", description="Database password")
    database_name: str = Field(default="dadosrfb", description="Database name")
    maintenance_db: str = Field(default="postgres", description="Maintenance database")
    
    @field_validator('port')
    @classmethod
    def validate_port(cls, v):
        if not (1 <= v <= 65535):
            raise ValueError('Port must be between 1 and 65535')
        return v
    
    @field_validator('host')
    @classmethod
    def validate_host(cls, v):
        if not v.strip():
            raise ValueError('Host cannot be empty')
        return v.strip()
    
    def get_connection_string(self, db_name: Optional[str] = None) -> str:
        """Get PostgreSQL connection string."""
        target_db = db_name or self.database_name
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{target_db}"

    def get_maintenance_connection_string(self) -> str:
        """Get connection string for maintenance database."""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.maintenance_db}"


class MemoryMonitorConfig(BaseModel):
    """
    Memory monitoring and management configuration.
    - memory_limit_mb: legacy absolute budget above baseline (MB).
    - memory_limit_mode: "absolute" uses memory_limit_mb; "fraction" uses memory_limit_fraction * system_total.
    - memory_limit_fraction: fraction of system or cgroup memory (0.0 < f <= 1.0) used when memory_limit_mode == "fraction".
    """
    memory_limit_mb: int = Field(
        default=3072,
        ge=256,
        le=16384,
        description="Legacy absolute budget (MB) above baseline if memory_limit_mode == 'absolute'"
    )

    memory_limit_mode: Literal["absolute", "fraction"] = Field(
        default="fraction",
        description="Use an absolute MB budget or a fraction of system/cgroup memory"
    )

    memory_limit_fraction: float = Field(
        default=0.5,
        ge=0.05,
        le=1.0,
        description="If mode == 'fraction', use this fraction of total memory as effective budget"
    )

    cleanup_threshold_ratio: float = Field(
        default=0.6,
        ge=0.1,
        le=1.0,
        description="EWMA pressure level at which cleanup is recommended (0.0-1.0)"
    )

    baseline_buffer_mb: int = Field(
        default=512,
        ge=64,
        le=4096,
        description="Minimum system buffer to keep available (MB). Monitor may scale this down on small hosts."
    )

    # Operational knobs
    baseline_samples: int = Field(default=7, ge=3, le=31,
                                 description="Number of samples used to compute baseline")
    warmup_seconds: float = Field(default=1.0, ge=0.0, le=30.0,
                                  description="Seconds to wait before baseline sampling")
    cleanup_rate_limit_seconds: float = Field(default=1.0, ge=0.1, le=60.0,
                                              description="Minimum seconds between aggressive cleanup calls")
    smoothing_alpha: float = Field(default=0.25, ge=0.01, le=1.0,
                                   description="EWMA alpha for pressure smoothing (lower = slower reaction)")
    record_history_len: int = Field(default=128, ge=0, le=10000,
                                    description="Keep a short ring buffer of recent status reports for diagnostics")

    # Optional toggle to let monitor try to call malloc_trim on glibc based systems
    enable_malloc_trim: bool = Field(default=True)

    @field_validator('memory_limit_mb')
    @classmethod
    def validate_memory_limit(cls, v):
        if not (256 <= v <= 16384):
            raise ValueError('Memory limit must be between 256MB and 16384MB')
        return v

    @field_validator('cleanup_threshold_ratio')
    @classmethod
    def validate_cleanup_threshold(cls, v):
        if not (0.1 <= v <= 1.0):
            raise ValueError('Cleanup threshold ratio must be between 0.1 and 1.0')
        return v

    @field_validator('baseline_buffer_mb')
    @classmethod
    def validate_baseline_buffer(cls, v):
        if not (64 <= v <= 4096):
            raise ValueError('Baseline buffer must be between 64MB and 4096MB')
        return v

    @field_validator('memory_limit_fraction')
    @classmethod
    def validate_fraction(cls, v):
        if not (0.05 <= v <= 1.0):
            raise ValueError('memory_limit_fraction must be between 0.05 and 1.0')
        return v

class ConversionConfig(MemoryMonitorConfig):
    """CSV to Parquet conversion configuration."""
    
    chunk_size: int = Field(
        default=50000, 
        ge=1000, 
        le=1000000, 
        description="Rows per conversion batch"
    )
    workers: int = Field(
        default=2, 
        ge=1, 
        le=16, 
        description="Number of conversion workers"
    )
    compression: str = Field(
        default="snappy", 
        pattern="^(snappy|gzip|lz4)$", 
        description="Parquet compression algorithm"
    )
    row_group_size: int = Field(
        default=100000,
        ge=10000,
        le=1000000,
        description="Number of rows per Parquet row group"
    )
    flush_threshold: int = Field(
        default=10,
        ge=1,
        le=100,
        description="Number of files to process before flushing to disk"
    )
    auto_fallback: bool = Field(
        default=True,
        description="Automatically fallback to smaller batches on memory issues"
    )
    row_estimation_factor: int = Field(
        default=8000,
        ge=1000,
        le=50000,
        description="Estimation factor for memory usage per row (bytes)"
    )
    max_file_size_mb: int = Field(
        default=1000,
        ge=100,
        le=10000,
        description="Maximum file size in MB for processing before using file splitting strategy"
    )
    
    @field_validator('chunk_size')
    @classmethod
    def validate_chunk_size(cls, v):
        if not (1000 <= v <= 1000000):
            raise ValueError('Chunk size must be between 1K and 1M rows')
        return v


class LoadingConfig(BaseModel):
    """Database loading configuration."""
    
    batch_size: int = Field(
        default=1000, 
        ge=100, 
        le=100000, 
        description="Database insertion batch size"
    )
    sub_batch_size: int = Field(
        default=500,
        ge=50,
        le=50000,
        description="Sub-batch size for parallel processing"
    )
    workers: int = Field(
        default=3, 
        ge=1, 
        le=8, 
        description="Number of loading workers"
    )
    max_retries: int = Field(
        default=3, 
        ge=0, 
        le=10, 
        description="Maximum retry attempts for failed operations"
    )
    timeout_seconds: int = Field(
        default=300,
        ge=30,
        le=3600,
        description="Timeout for loading operations in seconds"
    )
    use_copy: bool = Field(
        default=True, 
        description="Use COPY for faster inserts"
    )
    enable_internal_parallelism: bool = Field(
        default=True,
        description="Enable parallel processing within batches"
    )
    max_batch_size: int = Field(
        default=500000,
        ge=1000,
        le=10000000,
        description="Maximum batch size for loading operations"
    )
    min_batch_size: int = Field(
        default=10000,
        ge=100,
        le=1000000,
        description="Minimum batch size for loading operations"
    )
    batch_size_mb: int = Field(
        default=100,
        ge=10,
        le=1000,
        description="Batch size limit in MB"
    )
    
    # Async operations and connection pooling
    internal_concurrency: int = Field(
        default=3, 
        ge=1, 
        le=16, 
        description="Internal concurrency level for async operations"
    )
    async_pool_min_size: int = Field(
        default=1, 
        ge=1, 
        le=50, 
        description="Minimum size of async connection pool"
    )
    async_pool_max_size: int = Field(
        default=10, 
        ge=1, 
        le=100, 
        description="Maximum size of async connection pool"
    )
    
    @field_validator('sub_batch_size')
    @classmethod
    def validate_sub_batch_size(cls, v, info):
        if 'batch_size' in info.data:
            batch_size = info.data['batch_size']
            if v > batch_size:
                raise ValueError('Sub-batch size cannot exceed batch size')
        return v
    
    @field_validator('min_batch_size')
    @classmethod
    def validate_min_batch_size(cls, v, info):
        if 'max_batch_size' in info.data:
            max_batch_size = info.data['max_batch_size']
            if v > max_batch_size:
                raise ValueError('Min batch size cannot exceed max batch size')
        return v


class DataSourceConfig(BaseModel):
    """Brazilian Federal Revenue data source configuration."""
    
    # Data format specifics
    delimiter: str = Field(
        default=";", 
        description="CSV delimiter used by Brazilian Federal Revenue files"
    )
    timezone: str = Field(
        default="America/Sao_Paulo", 
        description="Timezone for parsing timestamps from data source"
    )
    encoding: str = Field(
        default="iso-8859-1", 
        description="Default file encoding used by RF (some files may differ)"
    )
    
    # Data source URLs
    base_url: str = Field(
        default="https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj",
        description="Base URL for Brazilian Federal Revenue CNPJ data files"
    )
    layout_url: str = Field(
        default="https://www.gov.br/receitafederal/dados/cnpj-metadados.pdf",
        description="URL for CNPJ data layout documentation"
    )
    
    @field_validator('base_url', 'layout_url')
    @classmethod
    def validate_url(cls, v):
        if not v.startswith(('http://', 'https://')):
            raise ValueError('URL must start with http:// or https://')
        return v
    
    @field_validator('delimiter')
    @classmethod
    def validate_delimiter(cls, v):
        if len(v) != 1:
            raise ValueError('Delimiter must be a single character')
        return v


class DownloadConfig(BaseModel):
    """Download configuration."""
    
    workers: int = Field(
        default=4,
        ge=1,
        le=16,
        description="Number of download workers"
    )
    chunk_size_mb: int = Field(
        default=50,
        ge=1,
        le=1024,
        description="Chunk size in MB for downloading large files"
    )
    verify_checksums: bool = Field(
        default=True,
        description="Verify file integrity using checksums after download"
    )
    checksum_threshold_mb: int = Field(
        default=1000,
        ge=1,
        le=10000,
        description="File size threshold in MB for checksum verification"
    )
    timeout_seconds: int = Field(
        default=300,
        ge=30,
        le=3600,
        description="Timeout for download operations in seconds"
    )
    max_retries: int = Field(
        default=3,
        ge=0,
        le=10,
        description="Maximum retry attempts for failed downloads"
    )


class DevelopmentConfig(BaseModel):
    """Development mode configuration."""
    
    enabled: bool = Field(
        default=True,
        description="Enable development mode optimizations"
    )
    file_size_limit_mb: int = Field(
        default=70,
        ge=1,
        le=1000,
        description="File size limit in MB for development mode"
    )
    max_files_per_table: int = Field(
        default=5,
        ge=1,
        le=100,
        description="Maximum number of files to process per table"
    )
    max_files_per_blob: int = Field(
        default=10,
        ge=1,
        le=50,
        description="Maximum number of files to process per blob/batch"
    )
    row_limit_percent: float = Field(
        default=0.1,
        ge=0.001,
        le=1.0,
        description="Percentage of rows to process in development mode"
    )


class PathConfig(BaseModel):
    """File system paths configuration for ETL stages."""
    
    download: str = Field(
        default="DOWNLOADED_FILES", 
        description="Directory for storing downloaded ZIP files"
    )
    extraction: str = Field(
        default="EXTRACTED_FILES", 
        description="Directory for storing extracted CSV files" 
    )
    conversion: str = Field(
        default="CONVERTED_FILES", 
        description="Directory for storing converted Parquet files"
    )
    
    # Path utility methods
    def get_download_path(self) -> Path:
        """Get download path as Path object."""
        return Path(self.download)
    
    def get_extraction_path(self) -> Path:
        """Get extraction path as Path object."""
        return Path(self.extraction)
    
    def get_conversion_path(self) -> Path:
        """Get conversion path as Path object."""
        return Path(self.conversion)
    
    # Temporal path methods
    def get_temporal_download_path(self, year: int, month: int) -> Path:
        """Get download path with temporal period subdirectory."""
        period_suffix = f"{year:04d}-{month:02d}"
        return Path(self.download) / period_suffix
    
    def get_temporal_extraction_path(self, year: int, month: int) -> Path:
        """Get extraction path with temporal period subdirectory."""
        period_suffix = f"{year:04d}-{month:02d}"
        return Path(self.extraction) / period_suffix
    
    def get_temporal_conversion_path(self, year: int, month: int) -> Path:
        """Get conversion path with temporal period subdirectory."""
        period_suffix = f"{year:04d}-{month:02d}"
        return Path(self.conversion) / period_suffix
    
    def ensure_directories_exist(self):
        """Create directories if they don't exist."""
        for path in [self.get_download_path(), self.get_extraction_path(), self.get_conversion_path()]:
            path.mkdir(parents=True, exist_ok=True)
    
    def ensure_temporal_directories_exist(self, year: int, month: int):
        """Create temporal directories for a specific period if they don't exist."""
        temporal_paths = [
            self.get_temporal_download_path(year, month),
            self.get_temporal_extraction_path(year, month),
            self.get_temporal_conversion_path(year, month)
        ]
        for path in temporal_paths:
            path.mkdir(parents=True, exist_ok=True)


class DataSinkConfig(BaseModel):
    """Data sink configuration for output paths and database."""
    
    # File system paths for ETL stages
    paths: PathConfig = Field(
        default_factory=PathConfig,
        description="File system paths for different ETL stages"
    )
    
    # Main database for processed data
    database: DatabaseConfig = Field(
        default_factory=DatabaseConfig, 
        description="Main PostgreSQL database for CNPJ data"
    )
    
    # Delegated path utility methods for convenience
    def get_download_path(self) -> Path:
        """Get download path as Path object."""
        return self.paths.get_download_path()
    
    def get_extraction_path(self) -> Path:
        """Get extraction path as Path object."""
        return self.paths.get_extraction_path()
    
    def get_conversion_path(self) -> Path:
        """Get conversion path as Path object."""
        return self.paths.get_conversion_path()
    
    # Delegated temporal path methods
    def get_temporal_download_path(self, year: int, month: int) -> Path:
        """Get download path with temporal period subdirectory."""
        return self.paths.get_temporal_download_path(year, month)
    
    def get_temporal_extraction_path(self, year: int, month: int) -> Path:
        """Get extraction path with temporal period subdirectory."""
        return self.paths.get_temporal_extraction_path(year, month)
    
    def get_temporal_conversion_path(self, year: int, month: int) -> Path:
        """Get conversion path with temporal period subdirectory."""
        return self.paths.get_temporal_conversion_path(year, month)
    
    def ensure_directories_exist(self):
        """Create directories if they don't exist."""
        self.paths.ensure_directories_exist()
    
    def ensure_temporal_directories_exist(self, year: int, month: int):
        """Create temporal directories for a specific period if they don't exist."""
        self.paths.ensure_temporal_directories_exist(year, month)


class PipelineConfig(BaseModel):
    """Main pipeline configuration with nested components."""
    
    environment: Environment = Field(default=Environment.DEVELOPMENT, description="Pipeline execution environment (DEVELOPMENT/PRODUCTION)")
    development: DevelopmentConfig = Field(default_factory=DevelopmentConfig, description="Development mode and testing settings")
    
    delete_files: bool = Field(default=True, description="Delete temporary files after successful processing")
    is_parallel: bool = Field(default=True, description="Enable parallel processing across multiple cores")
    
    # Temporal configuration (set at runtime)
    year: Optional[int] = Field(default=None, gt=0, description="Year for data processing")
    month: Optional[int] = Field(default=None, ge=1, le=12, description="Month for data processing")

    # Memory monitoring and management
    memory: MemoryMonitorConfig = Field(default_factory=MemoryMonitorConfig, description="Memory monitoring and management settings")
    
    # Data source configuration (urls, formats, encoding)
    data_source: DataSourceConfig = Field(default_factory=DataSourceConfig, description="Brazilian Federal Revenue data source settings")

    # Data sink configuration (output paths and database)
    data_sink: DataSinkConfig = Field(default_factory=DataSinkConfig, description="Data sink configuration for outputs and database")
    
    # Pipeline steps configurations
    download: DownloadConfig = Field(default_factory=DownloadConfig, description="File download and verification settings")
    conversion: ConversionConfig = Field(default_factory=ConversionConfig, description="CSV to Parquet conversion settings")
    loading: LoadingConfig = Field(default_factory=LoadingConfig, description="Database loading and batching settings")
    
    # Direct access properties for clean architecture
    @property
    def database(self) -> DatabaseConfig:
        """Direct access to database configuration."""
        return self.data_sink.database
    
    @property
    def paths(self) -> PathConfig:
        """Direct access to path configuration."""
        return self.data_sink.paths
    
    # Utility methods (delegated to DataSinkConfig)
    def get_download_path(self) -> Path:
        """Get download path as Path object."""
        return self.data_sink.get_download_path()
    
    def get_extraction_path(self) -> Path:
        """Get extraction path as Path object."""
        return self.data_sink.get_extraction_path()
    
    def get_conversion_path(self) -> Path:
        """Get conversion path as Path object."""
        return self.data_sink.get_conversion_path()
    
    # Temporal utility methods (delegated to DataSinkConfig)
    def get_temporal_download_path(self, year: int, month: int) -> Path:
        """Get download path with temporal period subdirectory."""
        return self.data_sink.get_temporal_download_path(year, month)
    
    def get_temporal_extraction_path(self, year: int, month: int) -> Path:
        """Get extraction path with temporal period subdirectory."""
        return self.data_sink.get_temporal_extraction_path(year, month)
    
    def get_temporal_conversion_path(self, year: int, month: int) -> Path:
        """Get conversion path with temporal period subdirectory."""
        return self.data_sink.get_temporal_conversion_path(year, month)
    
    def ensure_directories_exist(self):
        """Create directories if they don't exist."""
        self.data_sink.ensure_directories_exist()
    
    def ensure_temporal_directories_exist(self, year: int, month: int):
        """Create temporal directories for a specific period if they don't exist."""
        self.data_sink.ensure_temporal_directories_exist(year, month)
    
    @model_validator(mode='after')
    def validate_config_relationships(self):
        """Validate relationships between different configuration sections."""
        conversion = self.conversion
        loading = self.loading
        
        if conversion and loading:
           
            # Ensure sub-batch size is reasonable relative to batch size
            if loading.sub_batch_size > loading.batch_size:
                raise ValueError(
                    f"Loading sub-batch size ({loading.sub_batch_size}) cannot exceed "
                    f"batch size ({loading.batch_size})"
                )
        
        return self


class AuditConfig(BaseModel):
    """Audit and tracking configuration."""
    
    # Audit database connection
    database: DatabaseConfig = Field(default_factory=DatabaseConfig, description="Audit database for ETL tracking and manifests")

    # Batch tracking settings
    update_threshold: int = Field(
        default=100,
        ge=1,
        le=10000,
        description="Number of operations before triggering batch status update"
    )
    update_interval: int = Field(
        default=30,
        ge=1,
        le=3600,
        description="Interval in seconds between batch status updates"
    )
    enable_bulk_updates: bool = Field(
        default=True,
        description="Enable bulk database operations for better performance"
    )
    enable_temporal_context: bool = Field(
        default=True,
        description="Enable temporal context tracking for time-based analysis"
    )
    default_batch_size: int = Field(
        default=20000,
        ge=1000,
        le=1000000,
        description="Default batch size for batch tracking operations"
    )
    retention_days: int = Field(
        default=30,
        ge=1,
        le=365,
        description="Number of days to retain batch tracking data"
    )


class AppConfig(BaseModel):
    """Main application configuration combining all sections."""
    
    environment: Environment = Field(default=Environment.DEVELOPMENT, description="Application environment mode")
    
    # Core configurations
    pipeline: PipelineConfig = Field(default_factory=PipelineConfig, description="Pipeline processing configuration")
    audit: AuditConfig = Field(default_factory=AuditConfig, description="Audit and tracking configuration")
    
    # Temporal properties (set by get_config)
    _year: Optional[int] = PrivateAttr(default=None)
    _month: Optional[int] = PrivateAttr(default=None)
    
    @property
    def year(self) -> Optional[int]:
        """Temporal year for processing."""
        return self._year
    
    @property
    def month(self) -> Optional[int]:
        """Temporal month for processing."""
        return self._month
    
    def is_development_mode(self) -> bool:
        """Check if in development mode."""
        return (self.environment == Environment.DEVELOPMENT or 
                self.pipeline.development.enabled)
    
    def get_max_files_per_blob(self) -> int:
        """Get maximum files per blob from development configuration."""
        return self.pipeline.development.max_files_per_blob
    
    @model_validator(mode='after')
    def validate_app_config(self):
        """Validate application-level configuration consistency."""
        pipeline = self.pipeline
        
        if pipeline and pipeline.environment == Environment.PRODUCTION:
            # Production-specific validations
            if pipeline.development.enabled:
                raise ValueError("Development mode cannot be enabled in production environment")
        
        return self
