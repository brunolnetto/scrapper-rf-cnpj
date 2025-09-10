"""
Pydantic configuration models with validation.

This module defines typed configuration models that replace the legacy
string-based environment variable system with semantic clarity and
comprehensive validation.
"""

from pydantic import BaseModel, Field, field_validator, model_validator, PrivateAttr
from enum import Enum
from typing import Optional, Dict, Any
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


class ConversionConfig(BaseModel):
    """CSV to Parquet conversion configuration."""
    
    chunk_size: int = Field(
        default=50000, 
        ge=1000, 
        le=1000000, 
        description="Rows per conversion batch"
    )
    memory_limit_mb: int = Field(
        default=1024, 
        ge=256, 
        le=16384, 
        description="Memory limit in MB"
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
        default=3,
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


class ETLConfig(BaseModel):
    """Main ETL configuration with nested components."""
    
    environment: Environment = Field(default=Environment.DEVELOPMENT, description="ETL execution environment (DEVELOPMENT/PRODUCTION)")
    timezone: str = Field(default="America/Sao_Paulo", description="Timezone for processing timestamps")
    delimiter: str = Field(default=";", description="Delimiter for Brazilian Federal Revenue CSV files")
    delete_files: bool = Field(default=True, description="Delete temporary files after successful processing")
    is_parallel: bool = Field(default=True, description="Enable parallel processing across multiple cores")
    internal_concurrency: int = Field(default=3, ge=1, le=16, description="Internal concurrency level for async operations")
    manifest_tracking: bool = Field(default=False, description="Enable comprehensive audit trail and batch tracking")
    
    # Pool configuration
    async_pool_min_size: int = Field(default=1, ge=1, le=50, description="Minimum size of async connection pool")
    async_pool_max_size: int = Field(default=10, ge=1, le=100, description="Maximum size of async connection pool")
    
    # Temporal configuration (set at runtime)
    year: Optional[int] = Field(default=None, ge=2020, le=2030, description="Year for data processing")
    month: Optional[int] = Field(default=None, ge=1, le=12, description="Month for data processing")
    
    # Nested configurations
    conversion: ConversionConfig = Field(default_factory=ConversionConfig, description="CSV to Parquet conversion settings")
    loading: LoadingConfig = Field(default_factory=LoadingConfig, description="Database loading and batching settings")
    download: DownloadConfig = Field(default_factory=DownloadConfig, description="File download and verification settings")
    development: DevelopmentConfig = Field(default_factory=DevelopmentConfig, description="Development mode and testing settings")
    
    @model_validator(mode='after')
    def validate_config_relationships(self):
        """Validate relationships between different configuration sections."""
        conversion = self.conversion
        loading = self.loading
        
        if conversion and loading:
            # Ensure loading batch size doesn't exceed conversion chunk size
            if loading.batch_size > conversion.chunk_size:
                raise ValueError(
                    f"Loading batch size ({loading.batch_size}) cannot exceed "
                    f"conversion chunk size ({conversion.chunk_size})"
                )
            
            # Ensure sub-batch size is reasonable relative to batch size
            if loading.sub_batch_size > loading.batch_size:
                raise ValueError(
                    f"Loading sub-batch size ({loading.sub_batch_size}) cannot exceed "
                    f"batch size ({loading.batch_size})"
                )
        
        return self
    
    # Legacy compatibility methods
    @property
    def chunk_size(self) -> int:
        """Legacy compatibility: delegate to loading config."""
        return self.loading.batch_size


class PathConfig(BaseModel):
    """Path configuration for data directories."""
    
    download_path: str = Field(default="DOWNLOADED_FILES", description="Directory for storing downloaded ZIP files")
    extract_path: str = Field(default="EXTRACTED_FILES", description="Directory for storing extracted CSV files")
    conversion_path: str = Field(default="CONVERTED_FILES", description="Directory for storing converted Parquet files")
    
    def get_download_path(self) -> Path:
        """Get download path as Path object."""
        return Path(self.download_path)
    
    def get_extract_path(self) -> Path:
        """Get extract path as Path object."""
        return Path(self.extract_path)
    
    def get_conversion_path(self) -> Path:
        """Get conversion path as Path object."""
        return Path(self.conversion_path)
    
    def ensure_directories_exist(self):
        """Create directories if they don't exist."""
        for path in [self.get_download_path(), self.get_extract_path(), self.get_conversion_path()]:
            path.mkdir(parents=True, exist_ok=True)


class URLConfig(BaseModel):
    """URL configuration for external services."""
    
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


class BatchConfig(BaseModel):
    """Batch tracking configuration."""
    
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
    enable_monitoring: bool = Field(
        default=True,
        description="Enable real-time batch monitoring and alerts"
    )


class AppConfig(BaseModel):
    """Main application configuration combining all sections."""
    
    environment: Environment = Field(default=Environment.DEVELOPMENT, description="Application environment mode")
    
    # Database configurations
    main_database: DatabaseConfig = Field(default_factory=DatabaseConfig, description="Main PostgreSQL database for CNPJ data")
    audit_database: DatabaseConfig = Field(default_factory=DatabaseConfig, description="Audit database for ETL tracking and manifests")
    
    # Core configurations
    etl: ETLConfig = Field(default_factory=ETLConfig, description="ETL processing configuration")
    paths: PathConfig = Field(default_factory=PathConfig, description="File system paths for ETL processing")
    urls: URLConfig = Field(default_factory=URLConfig, description="URLs for data sources and services")
    batch: BatchConfig = Field(default_factory=BatchConfig, description="Batch processing and audit configuration")
    
    # Temporal properties for legacy compatibility (set by get_config)
    _year: Optional[int] = PrivateAttr(default=None)
    _month: Optional[int] = PrivateAttr(default=None)
    
    @property
    def year(self) -> Optional[int]:
        """Legacy compatibility: temporal year."""
        return self._year
    
    @property
    def month(self) -> Optional[int]:
        """Legacy compatibility: temporal month."""
        return self._month
    
    @property
    def databases(self) -> Dict[str, DatabaseConfig]:
        """Legacy compatibility: provide databases as dict."""
        return {
            'main': self.main_database,
            'audit': self.audit_database
        }
    
    @property
    def batch_config(self) -> BatchConfig:
        """Legacy compatibility: batch_config property."""
        return self.batch
    
    def is_development_mode(self) -> bool:
        """Legacy compatibility: check if in development mode."""
        return (self.environment == Environment.DEVELOPMENT or 
                self.etl.development.enabled)
    
    @model_validator(mode='after')
    def validate_app_config(self):
        """Validate application-level configuration consistency."""
        etl = self.etl
        
        if etl and etl.environment == Environment.PRODUCTION:
            # Production-specific validations
            if etl.development.enabled:
                raise ValueError("Development mode cannot be enabled in production environment")
        
        return self
