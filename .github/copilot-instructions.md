# AI Agent Instructions - CNPJ ETL Project

> **Branch Context**: `refactor-new-file-loader` - Core services reorganized under `src/core/services/` for modularity and error handling.

## Project Overview
This ETL pipeline processes Brazilian Federal Revenue CNPJ data (~17GB uncompressed) into PostgreSQL. It features async processing, auditing, incremental loading, and robust file detection.

## Core Architecture
- **Main Flow**: `src/main.py` → `PipelineOrchestrator` → `ReceitaCNPJPipeline` → Strategy Execution
- **Configuration**: `ConfigurationService` (`src/setup/config.py`) centralizes settings with temporal tracking.
- **Databases**: Dual setup - Production (`MainBase`) and Audit (`AuditBase`).
- **Strategies**: Execution strategies (e.g., DownloadOnly, FullETL) via the Strategy Pattern.
- **Services**: Modularized under `src/core/services/` (audit, conversion, download, loading).
- **File Detection**: `FileLoader` validates files in 4 layers (existence, extension, content, fallback).

## Key Patterns

### Configuration Access
```python
from src.setup.config import get_config
config = get_config(year=2024, month=12)
```

### Lazy Loading
```python
@property
def database(self):
    if self._database is None:
        self._init_databases()
    return self._database
```

### File Detection
```python
from src.utils.file_loader import FileLoader
loader = FileLoader(file_path, encoding='utf-8')
format = loader.get_format()  # 'csv' or 'parquet'
```

### Logging
```python
from src.setup.logging import logger
logger.info("Processing started", extra={"table_name": table_name})
```

## Essential Commands
- **Run ETL**: `just run-etl 2024 12` or `python -m src.main --download --convert --load --year 2024 --month 12`
- **Setup**: `just install` (dependencies), `just env` (virtual environment).
- **Lint**: `just lint` (auto-fix issues).

## Development Tips
- Use `lab/` for data exploration (e.g., `tools/analyze_socios_uniqueness.py`).
- Validate CLI arguments in `src/main.py`.
- Test ingestion workflows with `lab/refactored_fileloader/tools/test_ingest.py`.

## External Dependencies
- **Core**: asyncpg, pyarrow, sqlalchemy, pydantic.
- **Performance**: polars (CSV→Parquet), psutil (memory).
- **Development**: ruff (linting), rich (CLI).

## Data Flow
1. **Download**: ZIP files to `data/DOWNLOADED_FILES/`.
2. **Extraction**: CSVs to `data/EXTRACTED_FILES/`.
3. **Conversion**: Parquet files in `data/CONVERTED_FILES/`.
4. **Loading**: PostgreSQL upserts with auditing.

## File Structure Highlights
- `src/core/`: Core services (audit, download, loading).
- `src/utils/`: Utilities (file detection, conversion).
- `lab/`: Jupyter notebooks and analysis tools.

## Testing
- Use `just check` for lint checks.
- Run `lab/tools/benchmark_ingestion_performance.py` for performance tests.

---
Feedback is welcome to refine these instructions further.
