"""
Configuration profiles for different environments.

This module provides predefined configuration profiles optimized for
different scenarios (development, production, testing).
"""

from typing import Dict, Any, Optional
import os
from enum import Enum


class ProfileType(str, Enum):
    """Available configuration profiles."""
    DEVELOPMENT = "development"
    PRODUCTION = "production"
    TESTING = "testing"
    PERFORMANCE = "performance"
    MEMORY_OPTIMIZED = "memory_optimized"


class ConfigurationProfile:
    """Predefined configuration profiles for different scenarios."""
    
    # Development profile - optimized for fast iteration and debugging
    DEVELOPMENT = {
        # ETL Configuration
        "ETL_ENVIRONMENT": "development",
        "ETL_TIMEZONE": "America/Sao_Paulo",
        "ETL_DELIMITER": ";",
        "ETL_DELETE_FILES": "false",  # Keep files for debugging
        "ETL_IS_PARALLEL": "true",
        "ETL_INTERNAL_CONCURRENCY": "2",  # Reduced for stability
        "ETL_MANIFEST_TRACKING": "true",  # Enable for debugging
        
        # Conversion Configuration (Memory-optimized for development)
        "ETL_CONVERSION_CHUNK_SIZE": "5000",  # Small chunks
        "ETL_CONVERSION_MEMORY_LIMIT_MB": "512",  # Modest memory
        "ETL_CONVERSION_WORKERS": "1",  # Single worker for stability
        "ETL_CONVERSION_COMPRESSION": "snappy",
        "ETL_CONVERSION_ROW_GROUP_SIZE": "50000",
        "ETL_CONVERSION_FLUSH_THRESHOLD": "5",
        "ETL_CONVERSION_AUTO_FALLBACK": "true",
        "ETL_CONVERSION_ROW_ESTIMATION_FACTOR": "8000",
        
        # Loading Configuration (Small batches)
        "ETL_LOADING_BATCH_SIZE": "500",  # Small batches
        "ETL_LOADING_SUB_BATCH_SIZE": "250",
        "ETL_LOADING_WORKERS": "2",
        "ETL_LOADING_MAX_RETRIES": "3",
        "ETL_LOADING_TIMEOUT_SECONDS": "180",
        "ETL_LOADING_USE_COPY": "true",
        "ETL_LOADING_ENABLE_INTERNAL_PARALLELISM": "false",  # Simpler debugging
        "ETL_LOADING_MAX_BATCH_SIZE": "50000",
        "ETL_LOADING_MIN_BATCH_SIZE": "100",
        "ETL_LOADING_BATCH_SIZE_MB": "50",
        
        # Download Configuration
        "ETL_DOWNLOAD_WORKERS": "2",
        "ETL_DOWNLOAD_CHUNK_SIZE_MB": "25",
        "ETL_DOWNLOAD_VERIFY_CHECKSUMS": "false",  # Skip for speed
        "ETL_DOWNLOAD_CHECKSUM_THRESHOLD_MB": "100",
        "ETL_DOWNLOAD_TIMEOUT_SECONDS": "180",
        "ETL_DOWNLOAD_MAX_RETRIES": "2",
        
        # Development Configuration
        "ETL_DEV_ENABLED": "true",
        "ETL_DEV_FILE_SIZE_LIMIT_MB": "50",
        "ETL_DEV_MAX_FILES_PER_TABLE": "3",
        "ETL_DEV_MAX_FILES_PER_BLOB": "2",
        "ETL_DEV_ROW_LIMIT_PERCENT": "0.05",  # 5% of data
        
        # Connection Pools
        "ETL_ASYNC_POOL_MIN_SIZE": "1",
        "ETL_ASYNC_POOL_MAX_SIZE": "5",
        
        # Batch Configuration
        "BATCH_UPDATE_THRESHOLD": "50",
        "BATCH_UPDATE_INTERVAL": "15",
        "BATCH_ENABLE_BULK_UPDATES": "true",
        "BATCH_ENABLE_TEMPORAL_CONTEXT": "true",
        "BATCH_DEFAULT_BATCH_SIZE": "5000",
        "BATCH_RETENTION_DAYS": "7",  # Shorter retention
        "BATCH_ENABLE_MONITORING": "true",
    }
    
    # Production profile - optimized for performance and throughput
    PRODUCTION = {
        # ETL Configuration
        "ETL_ENVIRONMENT": "production",
        "ETL_TIMEZONE": "America/Sao_Paulo",
        "ETL_DELIMITER": ";",
        "ETL_DELETE_FILES": "true",  # Clean up after processing
        "ETL_IS_PARALLEL": "true",
        "ETL_INTERNAL_CONCURRENCY": "6",  # Higher concurrency
        "ETL_MANIFEST_TRACKING": "true",
        
        # Conversion Configuration (Performance-optimized)
        "ETL_CONVERSION_CHUNK_SIZE": "50000",  # Large chunks
        "ETL_CONVERSION_MEMORY_LIMIT_MB": "4096",  # High memory
        "ETL_CONVERSION_WORKERS": "4",  # Multiple workers
        "ETL_CONVERSION_COMPRESSION": "snappy",
        "ETL_CONVERSION_ROW_GROUP_SIZE": "250000",
        "ETL_CONVERSION_FLUSH_THRESHOLD": "20",
        "ETL_CONVERSION_AUTO_FALLBACK": "true",
        "ETL_CONVERSION_ROW_ESTIMATION_FACTOR": "8000",
        
        # Loading Configuration (Large batches)
        "ETL_LOADING_BATCH_SIZE": "10000",  # Large batches
        "ETL_LOADING_SUB_BATCH_SIZE": "2500",
        "ETL_LOADING_WORKERS": "6",
        "ETL_LOADING_MAX_RETRIES": "5",
        "ETL_LOADING_TIMEOUT_SECONDS": "600",
        "ETL_LOADING_USE_COPY": "true",
        "ETL_LOADING_ENABLE_INTERNAL_PARALLELISM": "true",
        "ETL_LOADING_MAX_BATCH_SIZE": "1000000",
        "ETL_LOADING_MIN_BATCH_SIZE": "5000",
        "ETL_LOADING_BATCH_SIZE_MB": "200",
        
        # Download Configuration
        "ETL_DOWNLOAD_WORKERS": "8",
        "ETL_DOWNLOAD_CHUNK_SIZE_MB": "100",
        "ETL_DOWNLOAD_VERIFY_CHECKSUMS": "true",
        "ETL_DOWNLOAD_CHECKSUM_THRESHOLD_MB": "1000",
        "ETL_DOWNLOAD_TIMEOUT_SECONDS": "600",
        "ETL_DOWNLOAD_MAX_RETRIES": "5",
        
        # Development Configuration (Disabled)
        "ETL_DEV_ENABLED": "false",
        "ETL_DEV_FILE_SIZE_LIMIT_MB": "1000",
        "ETL_DEV_MAX_FILES_PER_TABLE": "1000",
        "ETL_DEV_MAX_FILES_PER_BLOB": "100",
        "ETL_DEV_ROW_LIMIT_PERCENT": "1.0",
        
        # Connection Pools
        "ETL_ASYNC_POOL_MIN_SIZE": "5",
        "ETL_ASYNC_POOL_MAX_SIZE": "20",
        
        # Batch Configuration
        "BATCH_UPDATE_THRESHOLD": "1000",
        "BATCH_UPDATE_INTERVAL": "60",
        "BATCH_ENABLE_BULK_UPDATES": "true",
        "BATCH_ENABLE_TEMPORAL_CONTEXT": "true",
        "BATCH_DEFAULT_BATCH_SIZE": "50000",
        "BATCH_RETENTION_DAYS": "90",
        "BATCH_ENABLE_MONITORING": "true",
    }
    
    # Testing profile - optimized for fast test execution
    TESTING = {
        # ETL Configuration
        "ETL_ENVIRONMENT": "testing",
        "ETL_TIMEZONE": "America/Sao_Paulo",
        "ETL_DELIMITER": ";",
        "ETL_DELETE_FILES": "true",  # Clean up test artifacts
        "ETL_IS_PARALLEL": "false",  # Sequential for deterministic tests
        "ETL_INTERNAL_CONCURRENCY": "1",
        "ETL_MANIFEST_TRACKING": "false",  # Disable for speed
        
        # Conversion Configuration (Minimal for testing)
        "ETL_CONVERSION_CHUNK_SIZE": "1000",  # Very small chunks
        "ETL_CONVERSION_MEMORY_LIMIT_MB": "256",
        "ETL_CONVERSION_WORKERS": "1",
        "ETL_CONVERSION_COMPRESSION": "snappy",
        "ETL_CONVERSION_ROW_GROUP_SIZE": "10000",
        "ETL_CONVERSION_FLUSH_THRESHOLD": "1",
        "ETL_CONVERSION_AUTO_FALLBACK": "true",
        "ETL_CONVERSION_ROW_ESTIMATION_FACTOR": "8000",
        
        # Loading Configuration (Minimal batches)
        "ETL_LOADING_BATCH_SIZE": "100",  # Very small batches
        "ETL_LOADING_SUB_BATCH_SIZE": "50",
        "ETL_LOADING_WORKERS": "1",
        "ETL_LOADING_MAX_RETRIES": "1",
        "ETL_LOADING_TIMEOUT_SECONDS": "30",
        "ETL_LOADING_USE_COPY": "true",
        "ETL_LOADING_ENABLE_INTERNAL_PARALLELISM": "false",
        "ETL_LOADING_MAX_BATCH_SIZE": "10000",
        "ETL_LOADING_MIN_BATCH_SIZE": "10",
        "ETL_LOADING_BATCH_SIZE_MB": "10",
        
        # Download Configuration
        "ETL_DOWNLOAD_WORKERS": "1",
        "ETL_DOWNLOAD_CHUNK_SIZE_MB": "10",
        "ETL_DOWNLOAD_VERIFY_CHECKSUMS": "false",
        "ETL_DOWNLOAD_CHECKSUM_THRESHOLD_MB": "50",
        "ETL_DOWNLOAD_TIMEOUT_SECONDS": "30",
        "ETL_DOWNLOAD_MAX_RETRIES": "1",
        
        # Development Configuration
        "ETL_DEV_ENABLED": "true",
        "ETL_DEV_FILE_SIZE_LIMIT_MB": "10",
        "ETL_DEV_MAX_FILES_PER_TABLE": "1",
        "ETL_DEV_MAX_FILES_PER_BLOB": "1",
        "ETL_DEV_ROW_LIMIT_PERCENT": "0.01",  # 1% of data
        
        # Connection Pools
        "ETL_ASYNC_POOL_MIN_SIZE": "1",
        "ETL_ASYNC_POOL_MAX_SIZE": "2",
        
        # Batch Configuration
        "BATCH_UPDATE_THRESHOLD": "10",
        "BATCH_UPDATE_INTERVAL": "5",
        "BATCH_ENABLE_BULK_UPDATES": "false",  # Simpler for testing
        "BATCH_ENABLE_TEMPORAL_CONTEXT": "false",
        "BATCH_DEFAULT_BATCH_SIZE": "1000",
        "BATCH_RETENTION_DAYS": "1",
        "BATCH_ENABLE_MONITORING": "false",
    }
    
    # Performance profile - maximum throughput optimization
    PERFORMANCE = {
        # Inherit from production and override specific settings
        **PRODUCTION,
        
        # More aggressive settings
        "ETL_CONVERSION_CHUNK_SIZE": "100000",  # Very large chunks
        "ETL_CONVERSION_MEMORY_LIMIT_MB": "8192",  # High memory
        "ETL_CONVERSION_WORKERS": "8",
        "ETL_LOADING_BATCH_SIZE": "20000",  # Very large batches
        "ETL_LOADING_WORKERS": "8",
        "ETL_DOWNLOAD_WORKERS": "12",
        "ETL_INTERNAL_CONCURRENCY": "8",
        "ETL_ASYNC_POOL_MAX_SIZE": "30",
    }
    
    # Memory-optimized profile - for resource-constrained environments
    MEMORY_OPTIMIZED = {
        # Inherit from development and optimize for low memory
        **DEVELOPMENT,
        
        # Very conservative memory settings
        "ETL_CONVERSION_CHUNK_SIZE": "2000",
        "ETL_CONVERSION_MEMORY_LIMIT_MB": "256",
        "ETL_CONVERSION_WORKERS": "1",
        "ETL_LOADING_BATCH_SIZE": "200",
        "ETL_LOADING_SUB_BATCH_SIZE": "100",
        "ETL_LOADING_WORKERS": "1",
        "ETL_DOWNLOAD_WORKERS": "1",
        "ETL_INTERNAL_CONCURRENCY": "1",
        "ETL_ASYNC_POOL_MAX_SIZE": "3",
        "ETL_CONVERSION_FLUSH_THRESHOLD": "1",
    }


def load_profile(profile_name: str) -> Dict[str, Any]:
    """Load a predefined configuration profile."""
    profiles = {
        ProfileType.DEVELOPMENT: ConfigurationProfile.DEVELOPMENT,
        ProfileType.PRODUCTION: ConfigurationProfile.PRODUCTION,
        ProfileType.TESTING: ConfigurationProfile.TESTING,
        ProfileType.PERFORMANCE: ConfigurationProfile.PERFORMANCE,
        ProfileType.MEMORY_OPTIMIZED: ConfigurationProfile.MEMORY_OPTIMIZED,
    }
    
    profile = profiles.get(profile_name)
    if profile is None:
        available = ", ".join(profiles.keys())
        raise ValueError(f"Unknown profile '{profile_name}'. Available: {available}")
    
    return profile.copy()


def apply_profile(profile_name: str, override_existing: bool = False) -> Dict[str, str]:
    """
    Apply a configuration profile to environment variables.
    
    Args:
        profile_name: Name of the profile to apply
        override_existing: Whether to override existing environment variables
    
    Returns:
        Dictionary of variables that were set
    """
    profile = load_profile(profile_name)
    applied = {}
    
    for key, value in profile.items():
        if key not in os.environ or override_existing:
            os.environ[key] = str(value)
            applied[key] = str(value)
    
    return applied


def get_current_profile() -> Optional[str]:
    """Detect the current profile based on environment variables."""
    environment = os.getenv("ETL_ENVIRONMENT", "development").lower()
    
    # Map environment to profile
    env_to_profile = {
        "development": ProfileType.DEVELOPMENT,
        "production": ProfileType.PRODUCTION,
        "testing": ProfileType.TESTING,
    }
    
    return env_to_profile.get(environment)


def get_profile_summary(profile_name: str) -> Dict[str, Any]:
    """Get a summary of a configuration profile."""
    profile = load_profile(profile_name)
    
    summary = {
        "profile_name": profile_name,
        "total_variables": len(profile),
        "key_settings": {
            "environment": profile.get("ETL_ENVIRONMENT"),
            "conversion_chunk_size": profile.get("ETL_CONVERSION_CHUNK_SIZE"),
            "loading_batch_size": profile.get("ETL_LOADING_BATCH_SIZE"),
            "conversion_workers": profile.get("ETL_CONVERSION_WORKERS"),
            "loading_workers": profile.get("ETL_LOADING_WORKERS"),
            "download_workers": profile.get("ETL_DOWNLOAD_WORKERS"),
            "memory_limit_mb": profile.get("ETL_CONVERSION_MEMORY_LIMIT_MB"),
            "development_enabled": profile.get("ETL_DEV_ENABLED"),
        },
        "optimization_focus": _get_optimization_focus(profile_name),
    }
    
    return summary


def _get_optimization_focus(profile_name: str) -> str:
    """Get the optimization focus description for a profile."""
    focus_map = {
        ProfileType.DEVELOPMENT: "Fast iteration, debugging, stability",
        ProfileType.PRODUCTION: "High throughput, reliability, resource efficiency",
        ProfileType.TESTING: "Fast execution, deterministic behavior, minimal resources",
        ProfileType.PERFORMANCE: "Maximum throughput, aggressive resource usage",
        ProfileType.MEMORY_OPTIMIZED: "Minimal memory usage, resource conservation",
    }
    
    return focus_map.get(profile_name, "Unknown optimization focus")


def compare_profiles(profile1: str, profile2: str) -> Dict[str, Any]:
    """Compare two configuration profiles."""
    p1 = load_profile(profile1)
    p2 = load_profile(profile2)
    
    all_keys = set(p1.keys()) | set(p2.keys())
    differences = {}
    
    for key in sorted(all_keys):
        val1 = p1.get(key, "NOT_SET")
        val2 = p2.get(key, "NOT_SET")
        
        if val1 != val2:
            differences[key] = {
                profile1: val1,
                profile2: val2
            }
    
    return {
        "profile1": profile1,
        "profile2": profile2,
        "total_differences": len(differences),
        "differences": differences,
        "common_variables": len(set(p1.keys()) & set(p2.keys())),
        "profile1_unique": len(set(p1.keys()) - set(p2.keys())),
        "profile2_unique": len(set(p2.keys()) - set(p1.keys())),
    }


def get_profile(environment) -> Dict[str, str]:
    """Get configuration profile for a specific environment.
    
    Args:
        environment: Environment enum value or string
        
    Returns:
        Dict[str, str]: Environment variables for the profile
    """
    from .models import Environment as EnvironmentEnum
    
    # Convert enum to string if needed
    if isinstance(environment, EnvironmentEnum):
        env_name = environment.value
    else:
        env_name = str(environment).lower()
    
    # Map environment to profile type
    env_mapping = {
        "development": ProfileType.DEVELOPMENT,
        "production": ProfileType.PRODUCTION, 
        "testing": ProfileType.TESTING,
        "performance": ProfileType.PERFORMANCE,
        "memory_optimized": ProfileType.MEMORY_OPTIMIZED,
    }
    
    profile_type = env_mapping.get(env_name, ProfileType.DEVELOPMENT)
    return load_profile(profile_type)
