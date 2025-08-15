"""
Centralized configuration management for the CNPJ ETL project.

This module provides a unified interface for accessing configuration settings
from environment variables, eliminating hard-coded values throughout the codebase.
"""

from typing import Dict, Any, Optional
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
class ETLConfig:
    """ETL process configuration."""

    delimiter: str = ";"
    chunk_size: int = 50000
    max_retries: int = 3
    parallel_workers: int = 4
    delete_files: bool = True
    is_parallel: bool = True
    timezone: str = "America/Sao_Paulo"

    # Development mode settings
    development_mode: bool = False
    development_file_size_limit: int = 50000  # bytes


@dataclass
class PathConfig:
    """File path configuration."""

    download_path: Path
    extract_path: Path
    conversion_path: Path
    log_path: Path

    def ensure_directories_exist(self) -> None:
        """Ensure all configured directories exist."""
        for path in [self.download_path, self.extract_path, self.log_path]:
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

    def __init__(self, env_file: str = ".env"):
        """
        Initialize configuration service.

        Args:
            env_file: Path to the environment file
        """
        self.env_file = env_file
        self._load_configs()

    def _load_configs(self) -> None:
        """Load all configuration sections."""
        load_dotenv(self.env_file)

        self.databases = self._load_databases_config()
        self.etl = self._load_etl_config()
        self.paths = self._load_path_config()
        self.urls = self._load_url_config()

        # Skip directory creation during init - create them lazily when needed
        self.paths.ensure_directories_exist()

        logger.info("Configuration loaded successfully")

    def _load_database_config(self) -> DatabaseConfig:
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
            "main": self._load_database_config(),
            "audit": self._load_audit_database_config(),
        }

    def _load_etl_config(self) -> ETLConfig:
        """Load ETL configuration from environment variables."""
        return ETLConfig(
            timezone=os.getenv("ETL_TIMEZONE", "America/Sao_Paulo"),
            delimiter=os.getenv("ETL_FILE_DELIMITER", ";"),
            chunk_size=int(os.getenv("ETL_CHUNK_SIZE", "50000")),
            max_retries=int(os.getenv("ETL_MAX_RETRIES", "3")),
            parallel_workers=int(os.getenv("ETL_WORKERS", "4")),
            delete_files=os.getenv("ETL_DELETE_FILES", "true").lower() == "true",
            is_parallel=os.getenv("ETL_IS_PARALLEL", "true").lower() == "true",
            development_mode=os.getenv("ENVIRONMENT", "development").lower()
            == "development",
            development_file_size_limit=int(
                os.getenv("ETL_DEV_FILE_SIZE_LIMIT", "50000")
            ),
        )

    def _load_path_config(self) -> PathConfig:
        """Load path configuration from environment variables."""
        root_path = Path.cwd()
        data_path = Path.cwd() / "data"
        return PathConfig(
            download_path=data_path / os.getenv("DOWNLOAD_PATH", "DOWNLOADED_FILES"),
            extract_path=data_path / os.getenv("EXTRACT_PATH", "EXTRACTED_FILES"),
            conversion_path=data_path / os.getenv("CONVERT_PATH", "CONVERTED_FILES"),
            log_path=root_path / "logs",
        )

    def _load_url_config(self) -> URLConfig:
        """Load URL configuration from environment variables."""
        return URLConfig(
            base_url=os.getenv("RF_BASE_URL"), 
            layout_url=os.getenv("RF_LAYOUT_URL")
        )

    def is_development_mode(self) -> bool:
        """Check if running in development mode."""
        return self.etl.development_mode

    def get_file_size_limit(self) -> int:
        """Get file size limit for development mode filtering."""
        return self.etl.development_file_size_limit

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
                    "database": self.databases["main"].database,
                    "maintenance_db": self.databases["main"].maintenance_db,
                },
                "audit": {
                    "host": self.databases["audit"].host,
                    "port": self.databases["audit"].port,
                    "database": self.databases["audit"].database,
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


# Global configuration instance
_config_service: Optional[ConfigurationService] = None


def get_config() -> ConfigurationService:
    """Get the global configuration service instance."""
    global _config_service
    if _config_service is None:
        _config_service = ConfigurationService()
    return _config_service


def reload_config() -> None:
    """Reload the global configuration."""
    global _config_service
    if _config_service is not None:
        _config_service.reload()


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
