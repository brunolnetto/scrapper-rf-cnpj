"""
Nuclear Migration: Pure Pydantic Configuration System

Complete replacement of legacy configuration with typed Pydantic models.
No legacy compatibility layer needed - direct migration approach.
"""

import os
from .models import (
    Environment,
    DatabaseConfig,
    ConversionConfig,
    LoadingConfig,
    DownloadConfig,
    DevelopmentConfig,
    ETLConfig,
    PathConfig,
    URLConfig,
    BatchConfig,
    AppConfig,
)
from .validation import ConfigurationValidator
from .profiles import ConfigurationProfile, load_profile, get_profile
from .loader import ConfigLoader, load_config


def get_config(year=None, month=None):
    """
    NEW: Direct Pydantic configuration - no legacy system!
    
    Returns the new typed configuration system directly.
    Temporal parameters (year/month) are handled as metadata.
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
