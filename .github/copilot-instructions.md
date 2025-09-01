# AI Agent Instructions - CNPJ ETL Project

## Project Overview
Production ETL pipeline that processes Brazilian Federal Revenue public CNPJ data (~17GB uncompressed) into PostgreSQL. Features high-performance async processing, comprehensive auditing, incremental loading, and robust file detection.

## Core Architecture
- **Main Flow**: `src/main.py` → `PipelineOrchestrator` → `ReceitaCNPJPipeline` → Strategy Execution
- **Configuration**: `ConfigurationService` in `src/setup/config.py` - single source of truth with temporal tracking
- **Dual Databases**: Production (`MainBase`) + Audit (`AuditBase`) with separate SQLAlchemy bases
- **Strategy Pattern**: Multiple execution strategies (DownloadOnly, DownloadAndConvert, FullETL, etc.)
- **Interface-based Design**: Protocol-based architecture for pipeline and strategy decoupling
- **Enhanced Loading**: `EnhancedUnifiedLoader` with async processing and 4-layer file detection
- **Lazy Loading**: Resources initialize on-demand with `@property` pattern

## Critical Development Patterns

### Configuration Access
```python
# ALWAYS use ConfigurationService - never direct env vars
config_service = ConfigurationService(year=2024, month=12)  # With temporal parameters
db_config = config_service.databases['main']  # or 'audit'
etl_config = config_service.etl  # ETL_CHUNK_SIZE, etc.

# Or use global function for convenience
from src.setup.config import get_config
config = get_config(year=2024, month=12)

# Temporal configuration methods
config_service.set_temporal_config(year=2024, month=12)
current_year = config_service.get_year()
current_month = config_service.get_month()
```

### Resource Initialization (Lazy Loading)
```python
# Mandatory pattern in ReceitaCNPJPipeline - avoids unnecessary connections
@property
def database(self):
    if self._database is None:
        self._init_databases()
    return self._database

@property
def audit_service(self):
    if self._audit_service is None:
        self._audit_service = AuditService(self.database)
    return self._audit_service

@property
def data_loader(self):
    if not hasattr(self, '_data_loader') or self._data_loader is None:
        self._data_loader = DataLoadingService(
            self.database, self.config.paths, self.loading_strategy
        )
    return self._data_loader
```

### Robust File Detection (4-Layer Validation)
```python
# FileLoader auto-detects format with comprehensive validation
from src.utils.file_loader import FileLoader

loader = FileLoader(file_path, encoding='utf-8')

# Detection layers:
# 1. File existence and type validation
# 2. Extension-based format detection (.csv, .parquet, .txt, .dat)
# 3. Content-based validation (magic bytes for parquet, delimiter detection for CSV)
# 4. Fallback mechanisms with encoding support

detected_format = loader.get_format()  # Returns 'csv' or 'parquet'
batch_generator = loader.get_batch_generator(headers, chunk_size)
```

### Structured JSON Logging
```python
# All logs use JSON format with comprehensive metadata
# NEVER use print() statements - always use logger
from src.setup.logging import logger

logger.info("Processing started", extra={
    "table_name": table_name,
    "file_path": file_path,
    "rows_processed": count
})

# Logs saved to: logs/YYYY-MM-DD/HH_MM/ with rotation
# Fields include: timestamp, level, module, function, process info, custom metadata
```

### Memory-Efficient Streaming Processing
```python
# Use streaming for large files - never load entire file into memory
from src.utils.conversion import ProcessingConfig

config = ProcessingConfig(
    chunk_size=50000,        # Process in batches
    max_memory_mb=1024,      # Hard memory limits
    row_group_size=50000     # Parquet optimization
)

# Process large CSV files without memory issues
for batch_df in csv_stream_processor(file_path, config):
    process_batch(batch_df)  # Each batch is small and manageable
```

## Essential Commands

### ETL Execution
```bash
# Via just (recommended) - task runner command
just run-current                    # current month/year
just run-etl 2024 12               # specific period

# Direct execution with CLI args (updated interface)
python -m src.main --download --year 2024 --month 12  # download only
python -m src.main --download --convert --year 2024 --month 12  # download + convert
python -m src.main --convert --load --year 2024 --month 12  # convert + load
python -m src.main --download --convert --load --year 2024 --month 12  # full ETL
python -m src.main --load --full-refresh --year 2024 --month 12  # full refresh
```

### Development Workflow
```bash
# Setup and dependencies
just install         # uv pip install -r requirements.txt
just env            # Create virtual environment
just lint           # ruff check --fix src (auto-fix F + ARG rules)

# Maintenance and cleanup
just clean          # Remove logs and cache files
just clean-logs     # Remove build artifacts and log files
just clean-cache    # Remove cache directories

# Code search and replace
just search "token" # Search for token across codebase (excludes venv/.git)
just replace "old" "new"  # Replace token in codebase

# ETL operations
just run            # Run ETL for current date
just run-etl 2024 12 # Run ETL for specific year/month
```

### Environment Configuration
```bash
# .env file structure (copy from .env.template)
ENVIRONMENT=development  # or production
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_password
POSTGRES_DBNAME=dadosrfb

# Audit database (separate from main)
AUDIT_DB_HOST=localhost
AUDIT_DB_PORT=5432
AUDIT_DB_USER=postgres
AUDIT_DB_PASSWORD=your_password
AUDIT_DB_NAME=dadosrfb_analysis

# Performance tuning
ETL_CHUNK_SIZE=50000
ETL_SUB_BATCH_SIZE=5000
ETL_INTERNAL_CONCURRENCY=3
ETL_MANIFEST_TRACKING=true
```

## Enhanced Data Loading
```python
# Unified loading via factory pattern (backward compatible)
from src.database.dml import UnifiedLoader

# Creates EnhancedUnifiedLoader internally
loader = UnifiedLoader(database, config)

# Auto-detection with robust validation + async processing
success, error, rows = loader.load_file(table_info, file_path)

# Supports both CSV and Parquet with transforms
success, error, rows = loader.load_csv_file(table_info, csv_file)
success, error, rows = loader.load_parquet_file(table_info, parquet_file)
```

## Interface-Based Architecture

### Pipeline Protocol
```python
# All pipelines implement this interface
class ReceitaCNPJPipeline(Pipeline):
    def run(self, **kwargs) -> Optional[Any]:
        # Execute pipeline logic
        pass
    
    def validate_config(self) -> bool:
        # Validate pipeline configuration
        return True
    
    def get_name(self) -> str:
        return "ReceitaCNPJPipeline"
```

### Strategy Pattern
```python
# Multiple execution strategies available
strategies = {
    (True, False, False): DownloadOnlyStrategy(),
    (True, True, False): DownloadAndConvertStrategy(), 
    (False, True, True): ConvertAndLoadStrategy(),
    (True, True, True): FullETLStrategy(),
}

# Strategy execution
result = strategy.execute(pipeline, config_service, **kwargs)
```

### File Detection & Processing
```python
# Integrated file detection with content validation
from src.utils.file_loader import FileLoader

# 4-layer detection: existence, extension, content, fallback
loader = FileLoader(file_path, encoding='utf-8')
detected_format = loader.get_format()  # 'csv' or 'parquet'

# Streaming batch generation with encoding support
for batch in loader.batch_generator(headers, chunk_size):
    process_batch(batch)  # List[Tuple] format
```

## Dependencies and Build
- **Package Manager**: `uv` (modern, fast) over traditional pip
- **Core**: asyncpg (async database), pyarrow (file processing), sqlalchemy (ORM), pydantic (validation)
- **Performance**: polars (CSV→Parquet conversion), psutil (memory monitoring)
- **Web/Data**: requests (HTTP), beautifulsoup4 (HTML parsing), lxml (XML processing)
- **Development**: ruff (linting), python-dotenv (config), rich (CLI output)
- **Database**: psycopg2-binary + psycopg (PostgreSQL drivers)
- **Python**: >=3.9, configured via `pyproject.toml`
- **Build System**: Standard PEP 621 with uv for dependency management

### Performance Configuration
```bash
# Enhanced loading configuration in .env
ETL_CHUNK_SIZE=50000                    # Main batch size
ETL_SUB_BATCH_SIZE=5000                # Internal sub-batch size
ETL_INTERNAL_CONCURRENCY=3             # Parallel sub-batches per file
ETL_ASYNC_POOL_MIN_SIZE=2              # Connection pool minimum
ETL_ASYNC_POOL_MAX_SIZE=10             # Connection pool maximum
```

## Data Structure Specifics

### Main Tables (indexed by `cnpj_basico`)
- `empresa` (~50M records) - company headquarters, capital_social transformed
- `estabelecimento` (~60M) - branches with `cnpj_ordem` + `cnpj_dv`
- `socios` (~30M) - partner data
- `simples` (~40M) - MEI/Simples Nacional

### Data Transformations
```python
# Row-level transforms (pure Python, no pandas dependency)
def empresa_transform_map(row_dict: Dict[str, str]) -> Dict[str, str]:
    """Converts Brazilian number format to standard format"""
    if "capital_social" in row_dict and row_dict["capital_social"]:
        # Handle: "1.234.567,89" → "1234567.89"
        value = row_dict["capital_social"]
        if "," in value:
            parts = value.split(",")
            if len(parts) == 2:
                integer_part = parts[0].replace(".", "")  # Remove thousands separators
                decimal_part = parts[1]
                row_dict["capital_social"] = f"{integer_part}.{decimal_part}"
    return row_dict

# Applied automatically by EnhancedUnifiedLoader during processing
# All other tables use default_transform_map (no-op)
```

### Audit System
- **Tracking**: Every processed file generates record in `AuditDB`
- **Metadata**: `AuditService` centralizes audit creation/insertion
- **Validation**: Initial vs final row counts per table

## Processing Flow

### Complete Pipeline
1. **CLI Validation**: `validate_cli_arguments()` ensures valid strategy combinations
2. **Orchestrator Setup**: `PipelineOrchestrator` with `ReceitaCNPJPipeline` and selected strategy
3. **Temporal Configuration**: Year/month parameters applied to configuration service
4. **Strategy Execution**: Selected strategy (DownloadOnly, DownloadAndConvert, FullETL, etc.)
5. **Scraping**: `scrap_data()` - extracts metadata from gov.br source
6. **Download**: ZIP files to `data/DOWNLOAD_FILES/`
7. **Extraction**: CSV to `data/EXTRACTED_FILES/`
8. **Conversion**: Parquet to `data/CONVERTED_FILES/` (optimization)
9. **Loading**: PostgreSQL upsert with auditing

### Enhanced Loading Architecture
- **Robust Detection**: 4-layer file validation (existence, extension, content, fallback)
- **Batch Processing**: Streaming ingestors for CSV/Parquet with encoding support
- **Internal Parallelism**: Async concurrent sub-batches within same file
- **Advanced Auditing**: Complete manifest tracking with checksums and metadata
- **Memory Efficient**: Streaming processing with configurable batch sizes
- **Transform Support**: Row-level transforms applied during processing

### Development Mode
- Size filtering by `development_file_size_limit`
- Structured JSON logs in `logs/YYYY-MM-DD/HH_MM/`
- Configuration via `ENVIRONMENT=development`
- Auto-detection: `config.is_development_mode()`

## CLI Validation & Parameter Handling

### Strategy Validation
```python
# Valid strategy combinations enforced at CLI level
valid_combinations = [
    (True, False, False),  # Download Only: --download
    (True, True, False),   # Download+Convert: --download --convert  
    (False, True, False),  # Convert Only: --convert
    (False, False, True),  # Load Only: --load
    (False, True, True),   # Convert+Load: --convert --load
    (True, True, True),    # Full ETL: --download --convert --load
]

# Database operations require --load flag
if not args.load and (args.full_refresh or args.clear_tables):
    raise ValueError("Database operations require --load flag")
```

### Temporal Parameter Validation
```python
# Year and month validation in CLI
if args.year <= 2000 or (args.month < 1 or args.month > 12):
    raise ValueError("Invalid year/month parameters")
```

## Project Structure & Key Directories

### Core Architecture (`src/core/`)
- **`interfaces.py`**: Protocol definitions for Pipeline and OrchestrationStrategy
- **`orchestrator.py`**: `PipelineOrchestrator` - main execution coordinator
- **`etl.py`**: `ReceitaCNPJPipeline` - main pipeline implementation
- **`strategies.py`**: Strategy implementations (DownloadOnly, FullETL, etc.)
- **`constants.py`**: Table definitions and configuration constants
- **`schemas.py`**: Pydantic models for data validation
- **`audit/`**: Audit service and metadata tracking
- **`download/`**: File download service implementation
- **`loading/`**: Data loading strategies and unified loader

### Configuration & Setup (`src/setup/`)
- **`config.py`**: `ConfigurationService` - centralized configuration management
- **`logging.py`**: Structured logging configuration
- **`base.py`**: Database initialization utilities

### Database Layer (`src/database/`)
- **`models.py`**: SQLAlchemy models for MainBase and AuditBase
- **`dml.py`**: `UnifiedLoader` and data manipulation operations
- **`engine.py`**: Database connection management
- **`schemas.py`**: Database schema definitions
- **`utils/`**: Database utilities and admin functions

### Utilities (`src/utils/`)
- **`conversion.py`**: CSV to Parquet conversion utilities
- **`file_loader.py`**: Robust file detection and streaming
- **`models.py`**: Table schema utilities

### Development & Analysis (`lab/`)
- **`main.ipynb`**: Jupyter notebook for data exploration
- **`parquet_to_postgres.py`**: Direct Parquet loading utilities
- **`sink_into_duckdb.py`**: DuckDB integration for fast analytics
- **`refactored_fileloader/tools/`**: Performance analysis tools

### Examples (`examples/`)
- **`loading_example.py`**: Demonstrates data loading patterns

## Development Workflow & Testing

### Justfile Commands (Essential for Productivity)
```bash
# Core development cycle
just install          # Install dependencies with uv (fast package manager)
just lint            # Auto-fix linting issues (ruff F + ARG rules)
just run             # Run ETL for current date
just run-etl 2024 12 # Run ETL for specific year/month

# Code maintenance
just search "token"  # Search codebase (excludes venv/.git)
just replace "old" "new"  # Replace across codebase
just clean          # Remove logs and cache files

# Quality assurance
just check           # Lint check without fixes
```

### Testing Patterns
```python
# Use lab/ directory for data exploration and testing
# Examples in lab/refactored_fileloader/tools/:
# - benchmark_socios_ingestion.py: Performance benchmarking
# - analyze_socios_uniqueness.py: Data quality analysis
# - deep_socios_analysis.py: Source vs DB comparison

# Run analysis tools for data validation
cd lab/
python tools/analyze_socios_uniqueness.py
python tools/benchmark_socios_ingestion.py
```

### Environment Management
```bash
# Development mode automatically enables:
# - File size filtering (development_file_size_limit)
# - Reduced parallelism for debugging
# - Structured JSON logging to logs/YYYY-MM-DD/HH_MM/

# Switch environments by editing .env:
ENVIRONMENT=development  # For development/testing
ENVIRONMENT=production   # For production runs
```

## Advanced Architectural Patterns

### Dual Database Architecture
```python
# Separate bases for production and audit data
from sqlalchemy.ext.declarative import declarative_base

AuditBase = declarative_base()  # Audit tables
MainBase = declarative_base()   # Production tables

class AuditDB(AuditBase):
    __tablename__ = "table_ingestion_manifest"
    # Audit-specific fields...

class Empresa(MainBase):
    __tablename__ = "empresa"
    # Production data fields...
```

### Robust File Detection System
```python
# 4-layer validation in FileLoader._detect_format():
# 1. File existence and accessibility
# 2. Extension-based detection (.csv, .parquet, .txt, .dat)
# 3. Content-based validation (magic bytes, delimiter detection)
# 4. Encoding fallback mechanisms

# Usage pattern:
loader = FileLoader(file_path, encoding='utf-8')
format = loader.get_format()  # 'csv' or 'parquet'
batch_gen = loader.get_batch_generator(headers, chunk_size)
```

### Memory-Efficient Processing
```python
# Never load entire files into memory
from src.utils.conversion import ProcessingConfig

config = ProcessingConfig(
    chunk_size=50000,        # Process in batches
    max_memory_mb=1024,      # Hard memory limits
    row_group_size=50000     # Parquet optimization
)

# Streaming processing for 17GB+ datasets
for batch_df in csv_stream_processor(file_path, config):
    process_batch(batch_df)  # Each batch is manageable
```

### Brazilian Number Format Transformation
```python
# Critical for empresa.capital_social field
def empresa_transform_map(row_dict: Dict[str, str]) -> Dict[str, str]:
    """Convert Brazilian format '1.234.567,89' to '1234567.89'"""
    if "capital_social" in row_dict and row_dict["capital_social"]:
        value = row_dict["capital_social"]
        if "," in value:
            parts = value.split(",")
            if len(parts) == 2:
                # Remove thousand separators and swap decimal separator
                integer_part = parts[0].replace(".", "")
                decimal_part = parts[1]
                row_dict["capital_social"] = f"{integer_part}.{decimal_part}"
    return row_dict
```

### Structured Logging with Context
```python
# JSON logging with comprehensive metadata
logger.info("Processing started", extra={
    "table_name": table_name,
    "file_path": file_path,
    "rows_processed": count,
    "processing_time_ms": duration,
    "memory_usage_mb": memory_mb
})

# Logs automatically saved to: logs/YYYY-MM-DD/HH_MM/
# Include process, thread, module, function context
```

## Data Integrity & Validation

### Primary Key Patterns
```python
# CNPJ relationships (8-digit base + order + check digit)
# empresa: PRIMARY KEY (cnpj_basico)
# estabelecimento: PRIMARY KEY (cnpj_basico, cnpj_ordem, cnpj_dv)
# socios: PRIMARY KEY (cnpj_basico, identificador_socio)

# Note: socios uses composite key due to ~47% duplicates in source
# UPSERT operations handle deduplication automatically
```

### Manifest Tracking System
```python
# Every file operation creates audit trail
from src.core.audit.service import AuditService

audit_service = AuditService(database, config)
audit_service.create_file_manifest(
    file_path=file_path,
    status="PROCESSING",  # PROCESSING, SUCCESS, FAILED
    checksum=checksum,
    filesize=filesize,
    rows=rows_processed,
    table_name=table_name
)
```

## Performance Optimization Patterns

### Async Processing Configuration
```bash
# .env performance tuning
ETL_CHUNK_SIZE=50000                    # Main batch size
ETL_SUB_BATCH_SIZE=5000                # Internal sub-batch size  
ETL_INTERNAL_CONCURRENCY=3             # Parallel sub-batches per file
ETL_ASYNC_POOL_MIN_SIZE=2              # Connection pool minimum
ETL_ASYNC_POOL_MAX_SIZE=10             # Connection pool maximum
```

### Connection Pool Management
```python
# Async connection pooling for high-throughput loading
engine = create_engine(
    uri,
    poolclass=pool.QueuePool,
    pool_size=20,              # Connection pool size
    max_overflow=10,           # Max overflow connections
    pool_recycle=3600,         # Recycle connections hourly
)
```

### Error Recovery Patterns
```python
# Tenacity for automatic retry with exponential backoff
from tenacity import retry, stop_after_attempt, wait_exponential

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10)
)
def download_file(url, destination):
    # Network operations with automatic retry
    pass
```

## Troubleshooting Quick Reference

### Common Issues & Solutions
- **Connection errors**: Verify `.env` database credentials and dual database setup
- **Download timeouts**: gov.br scraping can fail - uses tenacity for auto-retry
- **Memory issues**: Reduce `ETL_CHUNK_SIZE` (default 50000) or increase system RAM
- **Invalid format**: `FileLoader` auto-detects CSV/Parquet with fallback mechanisms
- **Strategy validation**: Use valid CLI combinations (see CLI Validation section)
- **Temporal errors**: Ensure year > 2000 and month 1-12
- **Disk space**: Monitor `data/` directory (~50GB required)
- **Manifest tracking**: Ensure `ETL_MANIFEST_TRACKING=true` in `.env`

### Debug Commands
```bash
# Check database connections
python -c "from src.setup.config import get_config; print(get_config().databases)"

# Verify file detection
python -c "from src.utils.file_loader import FileLoader; print(FileLoader('file.csv').get_format())"

# Check ETL configuration
python -c "from src.setup.config import get_config; print(get_config().etl)"
```

## Code Quality Standards

### Linting & Formatting
```bash
# Ruff configuration in pyproject.toml
[tool.ruff]
lint.select = ["F", "ARG"]  # Pyflakes + unused arguments
lint.ignore = []           # No ignores for maximum code quality

# Auto-fix common issues
just lint  # Applies fixes automatically
```

### Import Organization
```python
# Standard library imports first
import os
from pathlib import Path
from typing import Dict, List, Optional

# Third-party imports
import sqlalchemy as sa
from pydantic import BaseModel

# Local imports (grouped by package)
from ..setup.config import ConfigurationService
from ..database.engine import Database
from .interfaces import Pipeline
```

### Type Hints & Documentation
```python
# Use comprehensive type hints
def process_batch(
    batch: List[Tuple[str, ...]], 
    table_name: str,
    config: ConfigurationService
) -> Tuple[bool, str, int]:
    """
    Process a batch of records for loading.
    
    Args:
        batch: List of tuples containing row data
        table_name: Target table name
        config: Configuration service instance
        
    Returns:
        Tuple of (success: bool, error_message: str, rows_processed: int)
    """
```

This comprehensive guide covers the essential patterns and conventions that will help AI agents be immediately productive in this CNPJ ETL codebase. The patterns emphasize robust error handling, performance optimization, and maintainable architecture specific to large-scale data processing pipelines.

### Enhanced Data Loading
```python
# Unified loading via factory pattern (backward compatible)
from src.database.dml import UnifiedLoader

# Creates EnhancedUnifiedLoader internally
loader = UnifiedLoader(database, config)

# Auto-detection with robust validation + async processing
success, error, rows = loader.load_file(table_info, file_path)

# Supports both CSV and Parquet with transforms
success, error, rows = loader.load_csv_file(table_info, csv_file)
success, error, rows = loader.load_parquet_file(table_info, parquet_file)
```

## Interface-Based Architecture

### Pipeline Protocol
```python
# All pipelines implement this interface
class ReceitaCNPJPipeline(Pipeline):
    def run(self, **kwargs) -> Optional[Any]:
        # Execute pipeline logic
        pass
    
    def validate_config(self) -> bool:
        # Validate configuration
        return True
    
    def get_name(self) -> str:
        return "ReceitaCNPJPipeline"
```

### Strategy Pattern
```python
# Multiple execution strategies available
strategies = {
    (True, False, False): DownloadOnlyStrategy(),
    (True, True, False): DownloadAndConvertStrategy(), 
    (False, True, True): ConvertAndLoadStrategy(),
    (True, True, True): FullETLStrategy(),
}

# Strategy execution
result = strategy.execute(pipeline, config_service, **kwargs)
```

### File Detection & Processing
```python
# Integrated file detection with content validation
from src.utils.file_loader import FileLoader

# 4-layer detection: existence, extension, content, fallback
loader = FileLoader(file_path, encoding='utf-8')
detected_format = loader.get_format()  # 'csv' or 'parquet'

# Streaming batch generation with encoding support
for batch in loader.batch_generator(headers, chunk_size):
    process_batch(batch)  # List[Tuple] format
```

## Essential Commands

### ETL Execution
```bash
# Via just (recommended) - task runner command
just run-current                    # current month/year
just run-etl 2024 12               # specific period

# Direct execution with CLI args (updated interface)
python -m src.main --download --year 2024 --month 12  # download only
python -m src.main --download --convert --year 2024 --month 12  # download + convert
python -m src.main --convert --load --year 2024 --month 12  # convert + load
python -m src.main --download --convert --load --year 2024 --month 12  # full ETL
python -m src.main --load --full-refresh --year 2024 --month 12  # full refresh
```

### Development
```bash
just install         # uv for dependencies (modern package manager)
just env            # Create virtual environment
just lint           # ruff with auto-fix (F + ARG rules)
just clean          # Remove logs and cache files
just clean-logs     # Remove build artifacts and log files
just clean-cache    # Remove cache directories
just search "token" # Search for token in codebase
just replace "old" "new"  # Replace token in codebase
```

### Dependencies and Build
- **Package Manager**: `uv` (modern, fast) over traditional pip
- **Core**: asyncpg (async database), pyarrow (file processing), sqlalchemy (ORM), pydantic (validation)
- **Performance**: polars (CSV→Parquet conversion), psutil (memory monitoring)
- **Web/Data**: requests (HTTP), beautifulsoup4 (HTML parsing), lxml (XML processing)
- **Development**: ruff (linting), python-dotenv (config), rich (CLI output)
- **Database**: psycopg2-binary + psycopg (PostgreSQL drivers)
- **Python**: >=3.9, configured via `pyproject.toml`
- **Build System**: Standard PEP 621 with uv for dependency management

### Performance Configuration
```bash
# Enhanced loading configuration in .env
ETL_CHUNK_SIZE=50000                    # Main batch size
ETL_SUB_BATCH_SIZE=5000                # Internal sub-batch size
ETL_INTERNAL_CONCURRENCY=3             # Parallel sub-batches per file
ETL_ASYNC_POOL_MIN_SIZE=2              # Connection pool minimum
ETL_ASYNC_POOL_MAX_SIZE=10             # Connection pool maximum
```

## Data Structure Specifics

### Main Tables (indexed by `cnpj_basico`)
- `empresa` (~50M records) - company headquarters, capital_social transformed
- `estabelecimento` (~60M) - branches with `cnpj_ordem` + `cnpj_dv`
- `socios` (~30M) - partner data
- `simples` (~40M) - MEI/Simples Nacional

### Data Transformations
```python
# Row-level transforms (pure Python, no pandas dependency)
def empresa_transform_map(row_dict: Dict[str, str]) -> Dict[str, str]:
    """Converts Brazilian number format to standard format"""
    if "capital_social" in row_dict and row_dict["capital_social"]:
        # Handle: "1.234.567,89" → "1234567.89"
        value = row_dict["capital_social"]
        if "," in value:
            parts = value.split(",")
            if len(parts) == 2:
                integer_part = parts[0].replace(".", "")  # Remove thousands separators
                decimal_part = parts[1]
                row_dict["capital_social"] = f"{integer_part}.{decimal_part}"
    return row_dict

# Applied automatically by EnhancedUnifiedLoader during processing
# All other tables use default_transform_map (no-op)
```

### Audit System
- **Tracking**: Every processed file generates record in `AuditDB`
- **Metadata**: `AuditService` centralizes audit creation/insertion
- **Validation**: Initial vs final row counts per table

## Processing Flow

### Complete Pipeline
1. **CLI Validation**: `validate_cli_arguments()` ensures valid strategy combinations
2. **Orchestrator Setup**: `PipelineOrchestrator` with `ReceitaCNPJPipeline` and selected strategy
3. **Temporal Configuration**: Year/month parameters applied to configuration service
4. **Strategy Execution**: Selected strategy (DownloadOnly, DownloadAndConvert, FullETL, etc.)
5. **Scraping**: `scrap_data()` - extracts metadata from gov.br source
6. **Download**: ZIP files to `data/DOWNLOAD_FILES/`
7. **Extraction**: CSV to `data/EXTRACTED_FILES/`
8. **Conversion**: Parquet to `data/CONVERTED_FILES/` (optimization)
9. **Loading**: PostgreSQL upsert with auditing

### Enhanced Loading Architecture
- **Robust Detection**: 4-layer file validation (existence, extension, content, fallback)
- **Batch Processing**: Streaming ingestors for CSV/Parquet with encoding support
- **Internal Parallelism**: Async concurrent sub-batches within same file
- **Advanced Auditing**: Complete manifest tracking with checksums and metadata
- **Memory Efficient**: Streaming processing with configurable batch sizes
- **Transform Support**: Row-level transforms applied during processing

### Development Mode
- Size filtering by `development_file_size_limit`
- Structured JSON logs in `logs/YYYY-MM-DD/HH_MM/`
- Configuration via `ENVIRONMENT=development`
- Auto-detection: `config.is_development_mode()`

## CLI Validation & Parameter Handling

### Strategy Validation
```python
# Valid strategy combinations enforced at CLI level
valid_combinations = [
    (True, False, False),  # Download Only: --download
    (True, True, False),   # Download+Convert: --download --convert  
    (False, True, False),  # Convert Only: --convert
    (False, False, True),  # Load Only: --load
    (False, True, True),   # Convert+Load: --convert --load
    (True, True, True),    # Full ETL: --download --convert --load
]

# Database operations require --load flag
if not args.load and (args.full_refresh or args.clear_tables):
    raise ValueError("Database operations require --load flag")
```

### Temporal Parameter Validation
```python
# Year and month validation in CLI
if args.year <= 2000 or (args.month < 1 or args.month > 12):
    raise ValueError("Invalid year/month parameters")
```

## Project Structure & Key Directories

### Core Architecture (`src/core/`)
- **`interfaces.py`**: Protocol definitions for Pipeline and OrchestrationStrategy
- **`orchestrator.py`**: `PipelineOrchestrator` - main execution coordinator
- **`etl.py`**: `ReceitaCNPJPipeline` - main pipeline implementation
- **`strategies.py`**: Strategy implementations (DownloadOnly, FullETL, etc.)
- **`constants.py`**: Table definitions and configuration constants
- **`schemas.py`**: Pydantic models for data validation
- **`audit/`**: Audit service and metadata tracking
- **`download/`**: File download service implementation
- **`loading/`**: Data loading strategies and unified loader

### Configuration & Setup (`src/setup/`)
- **`config.py`**: `ConfigurationService` - centralized configuration management
- **`logging.py`**: Structured logging configuration
- **`base.py`**: Database initialization utilities

### Database Layer (`src/database/`)
- **`models.py`**: SQLAlchemy models for MainBase and AuditBase
- **`dml.py`**: `UnifiedLoader` and data manipulation operations
- **`engine.py`**: Database connection management
- **`schemas.py`**: Database schema definitions
- **`utils/`**: Database utilities and admin functions

### Development & Analysis (`lab/`)
- **`main.ipynb`**: Jupyter notebook for data exploration
- **`parquet_to_postgres.py`**: Direct Parquet loading utilities
- **`sink_into_duckdb.py`**: DuckDB integration for fast analytics
- **`refactored_fileloader/tools/`**: Performance analysis tools

### Examples (`examples/`)
- **`loading_example.py`**: Demonstrates data loading patterns

## Performance Testing
```bash
# Comprehensive performance analysis for real CNPJ data
cd lab/
python tools/benchmark_socios_ingestion.py  # Multi-config benchmark
python tools/analyze_socios_uniqueness.py   # PK analysis
python tools/deep_socios_analysis.py        # Source vs DB comparison
```

## Key Architectural Patterns

### Protocol-Based Design
- **Pipeline Protocol**: Ensures all pipelines implement `run()`, `validate_config()`, `get_name()`
- **Strategy Protocol**: Enables pluggable execution strategies
- **Benefits**: Loose coupling, testability, extensibility

### Factory Pattern for Loading
```python
# UnifiedLoader factory creates appropriate loader based on context
loader = UnifiedLoader(database, config)  # Returns EnhancedUnifiedLoader
```

### Lazy Initialization
- **Database connections**: Only established when first accessed
- **Services**: AuditService, DataLoadingService initialized on-demand
- **Benefits**: Faster startup, reduced resource usage

### Configuration Hierarchy
1. **Environment variables** (.env file)
2. **CLI parameters** (year, month, strategy flags)
3. **Runtime configuration** (ConfigurationService methods)
4. **Validation** (Pydantic models and CLI validation)

## Troubleshooting
- **Connection errors**: Check `.env` and dual database setup
- **Download timeouts**: gov.br web scraping can fail - auto retry with tenacity
- **Memory issues**: Adjust `ETL_CHUNK_SIZE` (default 50000) and `ETL_MAX_MEMORY_MB`
- **Invalid format**: `UnifiedLoader` robustly detects CSV/Parquet with fallback
- **Strategy validation errors**: Use valid CLI flag combinations (see CLI Validation section)
- **Temporal parameter errors**: Ensure year > 2000 and month 1-12
- **Configuration errors**: Use `ConfigurationService(year=X, month=Y)` constructor pattern
- **Disk space**: Monitor `data/` (~50GB needed) and use `just clean` for maintenance
