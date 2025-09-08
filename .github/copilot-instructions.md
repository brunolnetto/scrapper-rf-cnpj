# AI Agent Instructions - CNPJ ETL Project

## Project Overview
A production ETL pipeline processing Brazilian Federal Revenue CNPJ data (~17GB uncompressed, 60M+ records) into PostgreSQL with full audit trails, incremental loading, and async processing.

## Core Architecture

### Strategy Pattern Flow
`src/main.py` → `GenericOrchestrator` → Strategy (`FullETLStrategy`, `DownloadOnlyStrategy`, etc.) → `ReceitaCNPJPipeline`

### Dual Database Setup
- **Production**: `MainBase` models (empresa, estabelecimento, socios, etc.)
- **Audit**: `AuditBase` models (table_ingestion_manifest, batch_ingestion_manifest, file_ingestion_manifest)

### Key Services
- **Audit**: `src/core/services/audit/service.py` - Hierarchical batch tracking with manifest entries
- **Loading**: `src/core/services/loading/service.py` - Async data loading with enhanced file detection  
- **Conversion**: Polars-based CSV→Parquet with memory optimization
- **Download**: Retry-enabled ZIP fetching from Receita Federal

## Critical Patterns

### Configuration with Temporal Context
```python
from src.setup.config import get_config
config = get_config(year=2024, month=12)  # Year/month for data versioning
# Access nested: config.databases['main'], config.paths.download_path
```

### Lazy Initialization (Core Pattern)
```python
@property
def audit_service(self):
    if self._audit_service is None:
        self._init_databases()  # Creates both main + audit DB connections
    return self._audit_service
```

### File Format Detection (4-Layer System)
```python
from src.utils.file_loader import FileLoader
loader = FileLoader(file_path, encoding='utf-8')
format = loader.get_format()  # Returns 'csv' or 'parquet'
# Layers: existence → extension → magic bytes → content validation
```

### Strategy Execution
```python
# CLI creates strategy from boolean flags: --download --convert --load
strategy = StrategyFactory.create_strategy(download=True, convert=True, load=True)
orchestrator = GenericOrchestrator(pipeline, strategy, config_service)
orchestrator.run(year=2024, month=12, full_refresh=False)
```

### Audit Context Management
```python
# Hierarchical tracking: batch → subbatch → file manifest
with audit_service.batch_context("estabelecimento", "monthly_load") as batch_id:
    with audit_service.subbatch_context(batch_id, "estabelecimento") as subbatch_id:
        # File processing happens here
```

## Essential Commands
- **Full ETL**: `just run-etl 2024 12` or `python -m src.main --year 2024 --month 12`
- **Partial Strategies**: `python -m src.main --download --convert` (skip loading)
- **Setup**: `just install && just env` 
- **Development**: `just lint` (ruff auto-fix), `just clean` (logs/cache)

## Environment Configuration
Copy `.env.template` to `.env` and configure:
- **Dual DB**: `POSTGRES_*` (main) + `AUDIT_DB_*` (audit)
- **Performance**: `ETL_CHUNK_SIZE=50000`, `ETL_INTERNAL_CONCURRENCY=3`
- **Paths**: `DOWNLOAD_PATH`, `EXTRACT_PATH`, `CONVERT_PATH`

## Development Workflows

### Data Exploration
- `lab/main.ipynb` - Primary analysis notebook
- `lab/pk_candidate_evaluator.py` - Primary key analysis for CNPJ data
- `lab/test_files_row_integrity.py` - Data validation across formats

### Testing File Loading
- `lab/refactored_fileloader/tools/test_ingest.py` - File loader validation
- `lab/upsert_parquet_example.py` - Database loading examples

### Performance Analysis  
- `lab/memory_monitor.py` - Memory usage tracking
- Environment limits: `ETL_DEV_FILE_SIZE_LIMIT`, `ETL_DEV_MAX_FILES_PER_TABLE`

## Database Patterns

### Model Relationships
```python
# Main tables use CNPJ-based relationships
empresa.cnpj_basico → estabelecimento.cnpj_basico → socios.cnpj_basico
# Audit tables track processing metadata with foreign keys
AuditDB.manifests → AuditManifest.audit_id
```

### Lazy DB Properties
```python
# ReceitaCNPJPipeline pattern - all DB access is lazy
@property
def database(self):
    if self._database is None:
        self._init_databases()  # Creates main + audit connections
    return self._database
```

## File Processing Specifics

### Brazilian RF Data Peculiarities
- **Encoding**: Mixed (ISO-8859-1 for some tables, UTF-8 for others)
- **Delimiters**: Semicolon (`;`) separated, not comma
- **File Naming**: Pattern-based (K3241.K03200Y*.D50809.EMPRECSV)
- **Extensions**: No extensions (`.ESTABELE`, `.EMPRECSV`, `.SOCIOCSV`)

### Format Detection Strategy
```python
# FileLoader handles RF's inconsistent file extensions
loader = FileLoader(file_path)  # Auto-detects format despite missing .csv
# Fallback order: extension → magic bytes → content parsing → CSV assumption
```

## Integration Points
- **External API**: Receita Federal download URLs (configurable base)
- **PostgreSQL**: Dual connection pools (main + audit databases)  
- **File System**: Three-stage processing (download → extract → convert directories)
- **Polars**: CSV→Parquet conversion for memory efficiency

---
*For questions about audit tracking, file detection edge cases, or performance tuning, refer to the lab/ notebooks for working examples.*
