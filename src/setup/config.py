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

from setup.logging import logger


@dataclass
class DatabaseConfig:
    """Database connection configuration."""
    host: str
    port: int
    user: str
    password: str
    database: str
    maintenance_db: str = 'postgres'
    
    def get_connection_string(self, db_name: Optional[str] = None) -> str:
        """Get PostgreSQL connection string."""
        target_db = db_name or self.database
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{target_db}"
    
    def get_maintenance_connection_string(self) -> str:
        """Get connection string for maintenance database."""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.maintenance_db}"


@dataclass
class ETLConfig:
    """ETL process configuration."""
    chunk_size: int = 50000
    max_retries: int = 3
    parallel_workers: int = 4
    prefer_parquet: bool = True
    delete_files: bool = True
    use_enhanced_loading: bool = True
    is_parallel: bool = True
    timezone: str = 'America/Sao_Paulo'
    
    # Development mode settings
    development_mode: bool = False
    development_file_size_limit: int = 50000  # bytes


@dataclass
class PathConfig:
    """File path configuration."""
    download_path: Path
    extract_path: Path
    log_path: Path
    
    def ensure_directories_exist(self) -> None:
        """Ensure all configured directories exist."""
        for path in [self.download_path, self.extract_path, self.log_path]:
            path.mkdir(parents=True, exist_ok=True)


@dataclass
class URLConfig:
    """URL configuration for data sources."""
    base_url: str = "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj"
    layout_url: str = "https://www.gov.br/receitafederal/dados/cnpj-metadados.pdf"
    
    def get_files_url(self, year: int, month: int) -> str:
        """Get the URL for files of a specific year and month."""
        return f"{self.base_url}/{year}-{month:02d}"


class ConfigurationService:
    """Centralized configuration management."""
    
    def __init__(self, env_file: str = '.env'):
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
        
        self.database = self._load_database_config()
        self.audit_db_config = self._load_audit_database_config()
        self.etl = self._load_etl_config()
        self.paths = self._load_path_config()
        self.urls = self._load_url_config()
        
        # Ensure directories exist
        self.paths.ensure_directories_exist()
        
        logger.info("Configuration loaded successfully")
    
    def _load_database_config(self) -> DatabaseConfig:
        """Load database configuration from environment variables."""
        return DatabaseConfig(
            host=os.getenv('POSTGRES_HOST', 'localhost'),
            port=int(os.getenv('POSTGRES_PORT', '5432')),
            user=os.getenv('POSTGRES_USER', 'postgres'),
            password=os.getenv('POSTGRES_PASSWORD', 'postgres'),
            database=os.getenv('POSTGRES_DBNAME', 'dadosrfb'),
            maintenance_db=os.getenv('POSTGRES_MAINTENANCE_DB', 'postgres')
        )
    
    def _load_etl_config(self) -> ETLConfig:
        """Load ETL configuration from environment variables."""
        return ETLConfig(
            timezone=os.getenv('ETL_TIMEZONE', 'America/Sao_Paulo'),
            chunk_size=int(os.getenv('ETL_CHUNK_SIZE', '50000')),
            max_retries=int(os.getenv('ETL_MAX_RETRIES', '3')),
            parallel_workers=int(os.getenv('ETL_WORKERS', '4')),
            prefer_parquet=os.getenv('ETL_PREFER_PARQUET', 'true').lower() == 'true',
            delete_files=os.getenv('ETL_DELETE_FILES', 'true').lower() == 'true',
            use_enhanced_loading=os.getenv('ETL_USE_ENHANCED_LOADING', 'true').lower() == 'true',
            is_parallel=os.getenv('ETL_IS_PARALLEL', 'true').lower() == 'true',
            development_mode=os.getenv('ENVIRONMENT', 'development').lower() == 'development',
            development_file_size_limit=int(os.getenv('ETL_DEV_FILE_SIZE_LIMIT', '50000'))
        )
    
    def _load_path_config(self) -> PathConfig:
        """Load path configuration from environment variables."""
        root_path = Path.cwd() / 'data'
        return PathConfig(
            download_path=root_path / os.getenv('DOWNLOAD_PATH', 'DOWNLOAD_FILES'),
            extract_path=root_path / os.getenv('EXTRACT_PATH', 'EXTRACTED_FILES'),
            log_path=Path.cwd() / 'logs'
        )
    
    def _load_url_config(self) -> URLConfig:
        """Load URL configuration from environment variables."""
        return URLConfig(
            base_url=os.getenv('RF_BASE_URL', URLConfig.base_url),
            layout_url=os.getenv('RF_LAYOUT_URL', URLConfig.layout_url)
        )
    
    def _load_audit_database_config(self) -> DatabaseConfig:
        return DatabaseConfig(
            host=os.getenv('AUDIT_DB_HOST', 'localhost'),
            port=int(os.getenv('AUDIT_DB_PORT', '5432')),
            user=os.getenv('AUDIT_DB_USER', 'postgres'),
            password=os.getenv('AUDIT_DB_PASSWORD', 'postgres'),
            database=os.getenv('AUDIT_DB_NAME', 'dadosrfb_analysis'),
            maintenance_db=''  # Not used for audit DB
        )
    
    def get_database_names(self) -> Dict[str, str]:
        """Get database names for blue-green deployment."""
        return {
            'prod_db': self.database.database,
            'old_db': f"{self.database.database}_old",
            'new_db': f"{self.database.database}_new"
        }
    
    def is_development_mode(self) -> bool:
        """Check if running in development mode."""
        return self.etl.development_mode
    
    def should_filter_small_files(self) -> bool:
        """Check if small files should be filtered in development mode."""
        return self.is_development_mode()
    
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
        if not self.database.host:
            errors.append("Database host is not configured")
        if not self.database.user:
            errors.append("Database user is not configured")
        if not self.database.password:
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
            'database': {
                'host': self.database.host,
                'port': self.database.port,
                'database': self.database.database,
                'maintenance_db': self.database.maintenance_db
            },
            'audit_database': {
                'host': self.audit_db_config.host,
                'port': self.audit_db_config.port,
                'database': self.audit_db_config.database
            },
            'etl': {
                'chunk_size': self.etl.chunk_size,
                'max_retries': self.etl.max_retries,
                'parallel_workers': self.etl.parallel_workers,
                'prefer_parquet': self.etl.prefer_parquet,
                'delete_files': self.etl.delete_files,
                'use_enhanced_loading': self.etl.use_enhanced_loading,
                'is_parallel': self.etl.is_parallel,
                'development_mode': self.etl.development_mode
            },
            'paths': {
                'download_path': str(self.paths.download_path),
                'extract_path': str(self.paths.extract_path),
                'log_path': str(self.paths.log_path)
            },
            'urls': {
                'base_url': self.urls.base_url,
                'layout_url': self.urls.layout_url
            }
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
def get_database_config() -> DatabaseConfig:
    """Get database configuration (backward compatibility)."""
    return get_config().database


def get_etl_config() -> ETLConfig:
    """Get ETL configuration (backward compatibility)."""
    return get_config().etl


def get_path_config() -> PathConfig:
    """Get path configuration (backward compatibility)."""
    return get_config().paths


def get_url_config() -> URLConfig:
    """Get URL configuration (backward compatibility)."""
    return get_config().urls