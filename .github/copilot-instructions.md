# AI Agent Instructions - CNPJ ETL Project

## Project Overview
Production ETL pipeline that processes Brazilian Federal Revenue public CNPJ data (~17GB uncompressed) into PostgreSQL. Features comprehensive auditing, incremental loading, and performance optimizations based on dynamic web scraping.

## Core Architecture
- **Main Flow**: `src/main.py` → `ETLOrchestrator` → `CNPJ_ETL` → Download/Extract/Convert/Load
- **Configuration**: `ConfigurationService` in `src/setup/config.py` - single source of truth
- **Dual Databases**: Production (`MainBase`) + Audit (`AuditBase`) with separate SQLAlchemy bases
- **Auto-detection**: Parquet preferred over CSV via `UnifiedLoader` with robust detection
- **Lazy Loading**: Resources initialize on-demand with `@property` pattern
- **Strategy Pattern**: `DataLoadingStrategy` for unified loading

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

### Data Loading (Parquet Priority)
```python
# Strategy Pattern - always check Parquet first
parquet_file = path_config.conversion_path / f"{table_name}.parquet"
if parquet_file.exists():
    success, error, rows = loader.load_parquet_file(table_info, parquet_file)
else:
    # fallback to multiple CSV files
    success, error, rows = loader.load_csv_file(table_info, csv_files)
```

### Robust File Detection (lab/refactored_fileloader)
```python
# Robust 4-layer format detection pattern
from lab.refactored_fileloader.src.file_loader import FileLoader

# Auto-detection with content validation
loader = FileLoader(file_path)
detected_format = loader.get_format()  # 'csv' or 'parquet'

# Unified batch generation
for batch in loader.batch_generator(headers, chunk_size):
    process_batch(batch)
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

### High-Performance CLI (lab/refactored_fileloader)
```bash
# Auto-detect mixed CSV/Parquet files with dual parallelism
python -m lab.refactored_fileloader.src.cli 
    --dsn postgresql://user:pass@localhost:5433/testdb 
    --table socios 
    --concurrency 4 --internal-concurrency 2 
    data/*.{csv,parquet}

# Performance testing tools
python lab/refactored_fileloader/tools/benchmark_socios_ingestion.py
python lab/refactored_fileloader/tools/analyze_socios_performance.py
```

### Dependencies and Build
- **Package Manager**: `uv` (modern, fast) over traditional pip
- **Linting**: `ruff` with F (Pyflakes) + ARG (unused arguments) rules
- **Python**: >=3.9, configured via `pyproject.toml`

## Data Structure Specifics

### Main Tables (indexed by `cnpj_basico`)
- `empresa` (~50M records) - company headquarters, capital_social transformed
- `estabelecimento` (~60M) - branches with `cnpj_ordem` + `cnpj_dv`
- `socios` (~30M) - partner data
- `simples` (~40M) - MEI/Simples Nacional

### Data Transformations
```python
# Example transform_map specific to empresa
def empresa_transform_map(artifact):
    artifact["capital_social"] = artifact["capital_social"].str.replace(",", ".")
    artifact["capital_social"] = artifact["capital_social"].astype(float)
    return artifact
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

### Refactored Loading Architecture
- **Robust Detection**: `lab/refactored_fileloader` with multi-layer validation
- **Batch Processing**: Specialized ingestors for CSV/Parquet
- **Internal Parallelism**: Concurrent sub-batches within same file
- **Advanced Auditing**: Manifest with checksums and complete metadata

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
