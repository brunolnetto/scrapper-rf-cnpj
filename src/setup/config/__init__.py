"""
SOLID-compliant Pydantic Configuration System

Clean configuration architecture following Single Responsibility Principle.
"""

import os
from .models import (
    Environment,
    DatabaseConfig,
    ConversionConfig,
    LoadingConfig,
    DownloadConfig,
    DevelopmentConfig,
    DataSourceConfig,
    PipelineConfig,
    AuditConfig,
    AppConfig,
)
from .validation import ConfigurationValidator
from .profiles import ConfigurationProfile, load_profile, get_profile
from .loader import ConfigLoader, load_config


def get_config(year=None, month=None):
    """
    Load SOLID-compliant Pydantic configuration.
    
    Args:
        year: Year for temporal processing context
        month: Month for temporal processing context
        
    Returns:
        AppConfig: Fully configured application settings
    """
    # Determine profile based on environment
    profile = os.getenv("CONFIG_PROFILE", "development")
    
    # Load the new configuration
    config = load_config(profile=profile)
    
    # Add temporal metadata if provided (using private attributes)
    if year is not None:
        config._year = year
    if month is not None:
        config._month = month
        
    return config


# Export the new AppConfig as ConfigurationService for type compatibility
ConfigurationService = AppConfig


__all__ = [
    "Environment",
    "DatabaseConfig", 
    "ConversionConfig",
    "LoadingConfig",
    "DownloadConfig",
    "DevelopmentConfig",
    "ETLConfig",
    "PathConfig",
    "URLConfig",
    "BatchConfig",
    "AppConfig",
    "ConfigurationService",  # Type alias for compatibility
    "ConfigurationValidator",
    "ConfigurationProfile",
    "load_profile",
    "get_profile",
    "ConfigLoader",
    "load_config",
    "get_config",  # Direct new system
]
