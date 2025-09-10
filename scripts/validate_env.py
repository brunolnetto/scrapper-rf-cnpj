#!/usr/bin/env python3
"""
Environment Configuration Validator for CNPJ ETL Pipeline

This script validates environment variables and provides recommendations
for optimal configuration based on system resources.
"""

import os
import sys
import psutil
from pathlib import Path
from typing import Dict, List, Tuple, Optional

class EnvironmentValidator:
    """Validates environment configuration for CNPJ ETL pipeline."""
    
    def __init__(self):
        self.warnings = []
        self.errors = []
        self.info = []
        
    def validate_all(self) -> bool:
        """Run all validation checks."""
        print("🔍 CNPJ ETL Environment Configuration Validator")
        print("=" * 50)
        
        # Check if .env file exists
        env_file = Path('.env')
        if not env_file.exists():
            self.errors.append("❌ .env file not found. Copy .env.template to .env first.")
            return False
            
        # Load environment variables
        self._load_env_file(env_file)
        
        # Run validation checks
        self._validate_database_config()
        self._validate_paths()
        self._validate_performance_settings()
        self._validate_memory_settings()
        self._validate_development_settings()
        self._check_system_resources()
        
        # Print results
        self._print_results()
        
        return len(self.errors) == 0
    
    def _load_env_file(self, env_file: Path):
        """Load environment variables from .env file."""
        try:
            with open(env_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        # Remove quotes
                        value = value.strip('\'"')
                        os.environ[key] = value
        except Exception as e:
            self.errors.append(f"❌ Failed to load .env file: {e}")
    
    def _validate_database_config(self):
        """Validate database configuration."""
        print("\n📊 Database Configuration")
        
        # Main database
        required_db_vars = [
            'POSTGRES_HOST', 'POSTGRES_PORT', 'POSTGRES_USER', 
            'POSTGRES_PASSWORD', 'POSTGRES_DBNAME'
        ]
        
        for var in required_db_vars:
            if not os.getenv(var):
                self.errors.append(f"❌ Missing required database variable: {var}")
            else:
                print(f"✅ {var}: {os.getenv(var)}")
        
        # Audit database
        audit_db_vars = [
            'AUDIT_DB_HOST', 'AUDIT_DB_PORT', 'AUDIT_DB_USER',
            'AUDIT_DB_PASSWORD', 'AUDIT_DB_NAME'
        ]
        
        for var in audit_db_vars:
            if not os.getenv(var):
                self.errors.append(f"❌ Missing required audit database variable: {var}")
            else:
                print(f"✅ {var}: {os.getenv(var)}")
        
        # Check if main and audit databases are different
        main_db = os.getenv('POSTGRES_DBNAME')
        audit_db = os.getenv('AUDIT_DB_NAME')
        if main_db == audit_db:
            self.warnings.append("⚠️ Main and audit databases are the same. Consider using separate databases.")
    
    def _validate_paths(self):
        """Validate file system paths."""
        print("\n📁 File System Paths")
        
        path_vars = ['DOWNLOAD_PATH', 'EXTRACT_PATH', 'CONVERT_PATH']
        
        for var in path_vars:
            path_str = os.getenv(var)
            if not path_str:
                self.errors.append(f"❌ Missing path variable: {var}")
                continue
                
            path = Path(path_str)
            try:
                # Try to create the directory
                path.mkdir(parents=True, exist_ok=True)
                print(f"✅ {var}: {path} (writable)")
            except PermissionError:
                self.errors.append(f"❌ No write permission for {var}: {path}")
            except Exception as e:
                self.errors.append(f"❌ Invalid path {var}: {path} - {e}")
    
    def _validate_performance_settings(self):
        """Validate performance-related settings."""
        print("\n🚀 Performance Settings")
        
        # ETL Workers
        workers = self._get_int_env('ETL_WORKERS', 4)
        cpu_count = psutil.cpu_count()
        
        if workers > cpu_count:
            self.warnings.append(f"⚠️ ETL_WORKERS ({workers}) > CPU cores ({cpu_count}). Consider reducing.")
        elif workers < cpu_count // 2:
            self.info.append(f"💡 ETL_WORKERS ({workers}) is conservative. You could increase up to {cpu_count}.")
        else:
            print(f"✅ ETL_WORKERS: {workers} (CPU cores: {cpu_count})")
        
        # Chunk size
        chunk_size = self._get_int_env('ETL_CHUNK_SIZE', 50000)
        if chunk_size < 10000:
            self.warnings.append("⚠️ ETL_CHUNK_SIZE is very small. May impact performance.")
        elif chunk_size > 200000:
            self.warnings.append("⚠️ ETL_CHUNK_SIZE is very large. May cause memory issues.")
        else:
            print(f"✅ ETL_CHUNK_SIZE: {chunk_size:,}")
        
        # Parallelism settings
        is_parallel = os.getenv('ETL_IS_PARALLEL', 'true').lower() == 'true'
        internal_parallel = os.getenv('ETL_ENABLE_INTERNAL_PARALLELISM', 'true').lower() == 'true'
        
        if not is_parallel and workers > 1:
            self.warnings.append("⚠️ ETL_IS_PARALLEL=false but ETL_WORKERS > 1. Enable parallelism for better performance.")
        
        print(f"✅ Parallelism: External={is_parallel}, Internal={internal_parallel}")
    
    def _validate_memory_settings(self):
        """Validate memory-related settings."""
        print("\n🧠 Memory Settings")
        
        max_memory_mb = self._get_int_env('ETL_MAX_MEMORY_MB', 1024)
        system_memory_gb = psutil.virtual_memory().total / (1024**3)
        
        if max_memory_mb > system_memory_gb * 1024 * 0.8:
            self.warnings.append(f"⚠️ ETL_MAX_MEMORY_MB ({max_memory_mb}MB) is > 80% of system memory ({system_memory_gb:.1f}GB)")
        elif max_memory_mb < 512:
            self.warnings.append("⚠️ ETL_MAX_MEMORY_MB is very low. May impact performance.")
        else:
            print(f"✅ ETL_MAX_MEMORY_MB: {max_memory_mb}MB (System: {system_memory_gb:.1f}GB)")
        
        # Pool sizes
        pool_max = self._get_int_env('ETL_ASYNC_POOL_MAX_SIZE', 10)
        workers = self._get_int_env('ETL_WORKERS', 4)
        
        if pool_max < workers:
            self.warnings.append(f"⚠️ ETL_ASYNC_POOL_MAX_SIZE ({pool_max}) < ETL_WORKERS ({workers}). May cause connection bottlenecks.")
        else:
            print(f"✅ Connection pool: max={pool_max}, workers={workers}")
    
    def _validate_development_settings(self):
        """Validate development mode settings."""
        print("\n🔧 Development Settings")
        
        environment = os.getenv('ENVIRONMENT', 'development')
        print(f"Environment: {environment}")
        
        if environment == 'development':
            dev_vars = {
                'ETL_DEV_FILE_SIZE_LIMIT_MB': 70,
                'ETL_DEV_MAX_FILES_PER_TABLE': 5,
                'ETL_DEV_ROW_LIMIT_PERCENT': 0.1
            }
            
            for var, default in dev_vars.items():
                value = os.getenv(var)
                if value:
                    if var == 'ETL_DEV_ROW_LIMIT_PERCENT':
                        percent = float(value) * 100
                        print(f"✅ {var}: {percent}% of data")
                    else:
                        print(f"✅ {var}: {value}")
                else:
                    print(f"⚠️ {var}: using default ({default})")
        else:
            print("Production mode - development filters disabled")
    
    def _check_system_resources(self):
        """Check system resources and provide recommendations."""
        print("\n💻 System Resources")
        
        # Memory
        memory = psutil.virtual_memory()
        print(f"RAM: {memory.total / (1024**3):.1f}GB total, {memory.available / (1024**3):.1f}GB available")
        
        # CPU
        cpu_count = psutil.cpu_count()
        print(f"CPU: {cpu_count} cores")
        
        # Disk space for paths
        path_vars = ['DOWNLOAD_PATH', 'EXTRACT_PATH', 'CONVERT_PATH']
        for var in path_vars:
            path_str = os.getenv(var)
            if path_str:
                try:
                    path = Path(path_str)
                    if path.exists():
                        disk_usage = psutil.disk_usage(str(path))
                        free_gb = disk_usage.free / (1024**3)
                        total_gb = disk_usage.total / (1024**3)
                        print(f"{var}: {free_gb:.1f}GB free / {total_gb:.1f}GB total")
                        
                        if free_gb < 100:
                            self.warnings.append(f"⚠️ Low disk space for {var}: {free_gb:.1f}GB free")
                except:
                    pass
    
    def _get_int_env(self, var: str, default: int) -> int:
        """Get integer environment variable with default."""
        try:
            return int(os.getenv(var, str(default)))
        except ValueError:
            self.warnings.append(f"⚠️ Invalid integer value for {var}, using default: {default}")
            return default
    
    def _print_results(self):
        """Print validation results."""
        print("\n" + "=" * 50)
        print("📋 VALIDATION RESULTS")
        print("=" * 50)
        
        if self.errors:
            print("\n❌ ERRORS (must be fixed):")
            for error in self.errors:
                print(f"  {error}")
        
        if self.warnings:
            print("\n⚠️ WARNINGS (recommendations):")
            for warning in self.warnings:
                print(f"  {warning}")
        
        if self.info:
            print("\n💡 INFORMATION:")
            for info in self.info:
                print(f"  {info}")
        
        if not self.errors and not self.warnings:
            print("\n✅ All checks passed! Configuration looks good.")
        elif not self.errors:
            print(f"\n✅ Configuration is valid with {len(self.warnings)} recommendations.")
        else:
            print(f"\n❌ Configuration has {len(self.errors)} errors that must be fixed.")

def main():
    """Main function."""
    validator = EnvironmentValidator()
    
    if len(sys.argv) > 1 and sys.argv[1] == '--help':
        print("""
CNPJ ETL Environment Configuration Validator

Usage:
  python scripts/validate_env.py

This script will:
  1. Check if .env file exists
  2. Validate all required environment variables
  3. Check database connectivity settings
  4. Validate file system paths
  5. Analyze performance settings vs system resources
  6. Provide optimization recommendations

Requirements:
  - .env file (copy from .env.template)
  - psutil package (pip install psutil)
        """)
        return
    
    success = validator.validate_all()
    sys.exit(0 if success else 1)

if __name__ == '__main__':
    main()
