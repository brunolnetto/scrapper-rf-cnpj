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

### Utilities (`src/utils/`)
- **`conversion.py`**: CSV to Parquet conversion utilities
- **`file_loader.py`**: Robust file detection and streaming
- **`model_utils.py`**: Table schema utilities
Based on CNPJ metadata documentation, partners should be uniquely identified by:
```python
# Theoretical PK (from documentation)
PRIMARY KEY (cnpj_basico, identificador_socio, cpf_cnpj_socio, qualificacao_socio)

# Current implementation (works in practice due to UPSERT deduplication)
PRIMARY KEY (cnpj_basico, identificador_socio)
```
- Use analysis tools in `lab/refactored_fileloader/tools/analyze_socios_*.py` to validate uniqueness
- Current PK achieves 100% uniqueness due to UPSERT handling of ~47% source duplicates

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
