# AI Agent Instructions - CNPJ ETL Project

## Project Overview
Production ETL pipeline that processes Brazilian Federal Revenue public CNPJ data (~17GB uncompressed) into PostgreSQL. Features high-performance async processing, comprehensive auditing, incremental loading, and robust file detection.

## Core Architecture
- **Main Flow**: `src/main.py` → `ETLOrchestrator` → `CNPJ_ETL` → Download/Extract/Convert/Load
- **Configuration**: `ConfigurationService` in `src/setup/config.py` - single source of truth
- **Dual Databases**: Production (`MainBase`) + Audit (`AuditBase`) with separate SQLAlchemy bases
- **Enhanced Loading**: `EnhancedUnifiedLoader` with async processing and 4-layer file detection
- **Lazy Loading**: Resources initialize on-demand with `@property` pattern
- **Simplified Strategy**: Clean `DataLoadingStrategy` (147 lines) for unified loading

## Critical Development Patterns

### Configuration Access
```python
# ALWAYS use ConfigurationService - never direct env vars
config_service = ConfigurationService()
db_config = config_service.databases['main']  # or 'audit'
etl_config = config_service.etl  # ETL_CHUNK_SIZE, etc.
```

### Resource Initialization (Lazy Loading)
```python
# Mandatory pattern in CNPJ_ETL - avoids unnecessary connections
@property
def database(self):
    if self._database is None:
        self._init_databases()
    return self._database

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
just run                    # current month/year
just run-etl 2024 12       # specific period

# Direct execution with CLI args  
python -m src.main --year 2024 --month 12 --full-refresh true
python -m src.main --download-only --year 2024 --month 12  # download only
python -m src.main --convert-only  # CSV→Parquet conversion only
```

### Development
```bash
just install    # uv for dependencies (modern package manager)
just lint      # ruff with unused import detection
just clean     # clear logs and cache
just search "token"  # search codebase
```

### Dependencies and Build
- **Package Manager**: `uv` (modern, fast) over traditional pip
- **Core**: asyncpg (async database), pyarrow (file processing), sqlalchemy (ORM)
- **Performance**: polars (CSV→Parquet conversion only)
- **Linting**: `ruff` with F (Pyflakes) + ARG (unused arguments) rules
- **Python**: >=3.9, configured via `pyproject.toml`

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
1. **Scraping**: `scrap_data()` - extracts metadata from gov.br source
2. **Download**: ZIP files to `data/DOWNLOAD_FILES/`
3. **Extraction**: CSV to `data/EXTRACTED_FILES/`
4. **Conversion**: Parquet to `data/CONVERTED_FILES/` (optimization)
5. **Loading**: PostgreSQL upsert with auditing

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

## Partner/Stakeholder Uniqueness
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
cd lab/refactored_fileloader
python tools/benchmark_socios_ingestion.py  # Multi-config benchmark
python tools/analyze_socios_uniqueness.py   # PK analysis
python tools/deep_socios_analysis.py        # Source vs DB comparison
```

## Troubleshooting
- **Connection errors**: Check `.env` and dual database setup
- **Download timeouts**: gov.br web scraping can fail - auto retry
- **Memory issues**: Adjust `ETL_CHUNK_SIZE` (default 50000)
- **Disk space**: Monitor `data/` (~50GB needed)
- **Invalid format**: `UnifiedLoader` robustly detects CSV/Parquet
