# Enhanced ETL Pipeline with Internal Parallelism

High-performance ETL pipeline for CSV/Parquet files with automatic format detection and dual-level concurrency.

## ✨ Key Features

- 🚀 **Auto-Detection**: Zero-config CSV/Parquet processing with robust validation
- ⚡ **Dual Parallelism**: File-level + internal sub-batch concurrency  
- 🛡️ **Production Ready**: Comprehensive logging, audit trails, retry logic
- 🔧 **Developer Friendly**: Backward compatible, intuitive CLI, extensive docs

## 🚀 Quick Start

```bash
# Auto-detect and process mixed formats
python -m src.cli --dsn postgresql://user:pass@localhost/db --table my_table data/*

# High-performance mode for large files
python -m src.cli --dsn postgresql://... --table my_table --internal-concurrency 4 files/*.parquet

# Legacy mode (manual format specification)
python -m src.cli --dsn postgresql://... --table my_table --file-type csv files/*.csv
```

<details>
<summary>📖 <strong>Command Line Options</strong></summary>

### Required
- `--dsn`: PostgreSQL connection string
- `--table`: Target database table
- `files`: File paths (supports glob patterns)

### Performance Tuning
- `--concurrency N`: File-level parallelism (default: 4)
- `--internal-concurrency N`: Sub-batch parallelism (default: 2)
- `--disable-internal-parallelism`: Force sequential processing
- `--batch-size N`: Records per batch (default: 50000)

### Advanced
- `--file-type {csv,parquet}`: Override auto-detection
- `--max-retries N`: Retry attempts (default: 3)

</details>

<details>
<summary>🏗️ <strong>Architecture Overview</strong></summary>

### Core Components

**FileLoader** (`src/file_loader.py`): Multi-layer format detection and batch generation
```python
loader = FileLoader(file_path)
batch_gen = loader.get_batch_generator()
```

**Enhanced Uploader** (`src/uploader.py`): Configurable parallel/sequential processing
```python
await async_upsert(pool, batch_gen, table_name, 
                   enable_internal_parallelism=True, internal_concurrency=4)
```

**CLI Interface** (`src/cli.py`): Auto-detection, dynamic pool sizing, comprehensive logging

### Processing Pipeline
1. **Format Detection** → 2. **Batch Generation** → 3. **Parallel Processing** → 4. **Database Upsert** → 5. **Audit Logging**

</details>

<details>
<summary>🔧 <strong>Configuration Examples</strong></summary>

### Development
```bash
python -m src.cli --dsn postgresql://localhost/dev_db --table test_table \
    --internal-concurrency 2 --concurrency 2 --batch-size 5000 test_files/*
```

### Production (High-Throughput)
```bash
python -m src.cli --dsn postgresql://prod-db:5432/warehouse --table fact_table \
    --internal-concurrency 4 --concurrency 6 --batch-size 20000 data/daily_export/*
```

### Memory-Constrained
```bash
python -m src.cli --dsn postgresql://localhost/db --table my_table \
    --internal-concurrency 2 --concurrency 3 --batch-size 10000 files/*
```

</details>

<details>
<summary>📊 <strong>Performance Optimization</strong></summary>

### Connection Pool Sizing
```
Pool Size = file_concurrency × internal_concurrency + 5
```

### Recommendations by Use Case

| Scenario | Configuration | Benefit |
|----------|---------------|---------|
| **Large Files (>1GB)** | `--internal-concurrency 4 --concurrency 2` | Parallel sub-batch processing |
| **Many Small Files** | `--internal-concurrency 2 --concurrency 6` | Higher file-level parallelism |
| **Mixed Workloads** | `--internal-concurrency 3 --concurrency 4` | Balanced performance |

</details>

<details>
<summary>🧪 <strong>Testing & Validation</strong></summary>

```bash
# Quick validation (no database required)
python3 validate_implementation.py

# Performance testing
python3 test_internal_parallelism.py

# Package testing (from workspace root)
python -m pytest lab/refactored_fileloader/tests/ --cov=lab/refactored_fileloader
```

</details>

<details>
<summary>🔍 <strong>Troubleshooting</strong></summary>

### Common Issues & Solutions

**"Too many connections" Error**
```bash
--concurrency 2 --internal-concurrency 2
# OR
--disable-internal-parallelism
```

**High Memory Usage**
```bash
--batch-size 5000 --internal-concurrency 2
```

**No Performance Improvement**
```bash
--internal-concurrency 4  # for CPU-bound workloads
--concurrency 8          # for I/O-bound workloads
```

### Monitoring
```bash
# Example output with timing metrics
[10:15:30] ✓ Completed: file.parquet (15432 rows, 4.2s)
```

</details>

<details>
<summary>📚 <strong>Documentation</strong></summary>

### Implementation Guides
- **[INTERNAL_PARALLELISM.md](INTERNAL_PARALLELISM.md)** - Technical implementation details
- **[MIGRATION_GUIDE.md](MIGRATION_GUIDE.md)** - Performance optimization guide  
- **[ENHANCEMENT_SUMMARY.md](ENHANCEMENT_SUMMARY.md)** - Complete feature overview
- **[CLEANUP_SUMMARY.md](CLEANUP_SUMMARY.md)** - Code consolidation details

### API Reference
- **FileLoader**: Format detection and batch generation
- **async_upsert**: Configurable parallel/sequential processing
- **CLI**: Command-line interface with auto-detection

</details>

<details>
<summary>🔄 <strong>Migration Guide</strong></summary>

### Backward Compatibility ✅
All existing commands work unchanged:
```bash
python -m src.cli --dsn ... --table ... --file-type csv files/*.csv
```

### Enable New Features Incrementally
```bash
# Step 1: Remove manual file type specification  
python -m src.cli --dsn ... --table ... files/*

# Step 2: Enable internal parallelism
python -m src.cli --dsn ... --table ... --internal-concurrency 4 files/*
```

</details>

<details>
<summary>🛠️ <strong>Development Setup</strong></summary>

### Module Import Context
Always run from workspace root (uses relative imports):
```bash
# From workspace root
python -m lab.refactored_fileloader.src.cli --help

# From within package  
cd lab/refactored_fileloader && python -m src.cli --help
```

### Dependencies
- **Python 3.7+** (async/await support)
- **asyncpg** (PostgreSQL async driver)
- **pyarrow** (Parquet processing)
- Standard library: asyncio, logging, argparse

</details>

## 🎯 Summary

**Zero-config ETL pipeline** with automatic format detection, dual-level parallelism, and production-grade reliability. Perfect for data engineering teams requiring high-performance processing with minimal configuration overhead.

**Key Benefits**: 🔄 Auto-detection • ⚡ High performance • 🛡️ Production ready • 🔧 Flexible config • 📈 Proven scalability
