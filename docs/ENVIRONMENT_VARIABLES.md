# Environment Variables Documentation

## Overview

This document provides comprehensive documentation for all environment variables used in the Brazilian Federal Revenue CNPJ ETL pipeline. These variables control database connections, ETL processing behavior, performance tuning, and development settings.

## Configuration Categories

### 1. Environment & Database Configuration

#### General Environment
```bash
ENVIRONMENT=development
```
- **Purpose**: Sets the application environment mode
- **Values**: `development` | `production`
- **Default**: `development`
- **Impact**: Affects logging levels, error handling verbosity, and development features

#### Main Database (Production Data)
```bash
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_DBNAME=dadosrfb
POSTGRES_MAINTENANCE_DB=postgres
```
- **Purpose**: Configuration for the main PostgreSQL database storing CNPJ data
- **Tables**: Contains `empresa`, `estabelecimento`, `socios`, `cnae`, etc.
- **Requirements**: Database must exist or be creatable by the specified user
- **Performance Impact**: Connection pooling (20 connections, 10 overflow)

#### Audit Database (Metadata & Tracking)
```bash
AUDIT_DB_HOST=localhost
AUDIT_DB_PORT=5432
AUDIT_DB_USER=postgres
AUDIT_DB_PASSWORD=postgres
AUDIT_DB_NAME=dadosrfb_analysis
```
- **Purpose**: Separate database for ETL audit trails and batch tracking
- **Tables**: Contains `table_ingestion_manifest`, `batch_ingestion_manifest`, etc.
- **Benefits**: Isolates audit data from production data for better performance
- **Backup Strategy**: Should be backed up separately from main database

### 2. File System & Path Configuration

#### ETL Processing Paths
```bash
DOWNLOAD_PATH='DOWNLOADED_FILES'
EXTRACT_PATH='EXTRACTED_FILES'
CONVERT_PATH='CONVERTED_FILES'
```
- **Purpose**: Directory structure for the three-stage ETL process
- **Workflow**: Download ZIP → Extract CSV → Convert to Parquet
- **Storage Requirements**: 
  - Download: ~17GB for full dataset
  - Extract: ~60GB uncompressed
  - Convert: ~15GB optimized Parquet files
- **Cleanup**: Files can be deleted after successful processing (controlled by `ETL_DELETE_FILES`)

#### External Data Sources
```bash
URL_RF_BASE="https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj"
URL_RF_LAYOUT="https://www.gov.br/receitafederal/dados/cnpj-metadados.pdf"
```
- **Purpose**: Brazilian Federal Revenue official data endpoints
- **Update Frequency**: Monthly releases (new year-month datasets)
- **Reliability**: Government source with retry mechanisms implemented
- **Backup Strategy**: URLs can change; monitor official announcements

### 3. Core ETL Processing Configuration

#### Main ETL Settings
```bash
ETL_TIMEZONE=America/Sao_Paulo
ETL_CHUNK_SIZE=50000
ETL_SUB_BATCH_SIZE=25000
ETL_MAX_RETRIES=3
ETL_WORKERS=4
ETL_IS_PARALLEL=true
ETL_DELETE_FILES=true
ETL_FILE_DELIMITER=;
ETL_INTERNAL_CONCURRENCY=3
ETL_MANIFEST_TRACKING=false
ETL_TIMEOUT_SECONDS=300
```

**Detailed Breakdown:**
- **`ETL_TIMEZONE`**: Timezone for timestamp operations (Brazilian timezone)
- **`ETL_CHUNK_SIZE`**: Rows per processing chunk (memory vs. performance trade-off)
- **`ETL_SUB_BATCH_SIZE`**: Rows per sub-batch within chunks (fine-grained control)
- **`ETL_MAX_RETRIES`**: Retry attempts for failed operations
- **`ETL_WORKERS`**: Parallel worker processes (should match CPU cores)
- **`ETL_IS_PARALLEL`**: Enable/disable parallel processing
- **`ETL_DELETE_FILES`**: Auto-cleanup processed files (saves disk space)
- **`ETL_FILE_DELIMITER`**: CSV delimiter (Brazilian RF uses semicolon)
- **`ETL_INTERNAL_CONCURRENCY`**: Internal async concurrency level
- **`ETL_MANIFEST_TRACKING`**: Enable detailed file manifest tracking
- **`ETL_TIMEOUT_SECONDS`**: Operation timeout (prevents hanging)

#### Connection Pool Settings
```bash
ETL_ASYNC_POOL_MIN_SIZE=1
ETL_ASYNC_POOL_MAX_SIZE=10
```
- **Purpose**: Async database connection pool configuration
- **Tuning**: Increase for higher concurrency, decrease for memory conservation
- **Monitoring**: Watch for connection exhaustion in logs

#### Data Integrity
```bash
ETL_CHECKSUM_THRESHOLD_MB=1000
```
- **Purpose**: Files larger than 1000MB (1GB) skip checksum verification (performance optimization)
- **Security**: Smaller files still verified for data integrity
- **Trade-off**: Speed vs. verification completeness

### 4. Download Configuration

```bash
ETL_DOWNLOAD_CHUNK_SIZE_MB=50
ETL_CHECKSUM_VERIFICATION=true
```
- **`ETL_DOWNLOAD_CHUNK_SIZE_MB`**: Download chunk size for large files
- **`ETL_CHECKSUM_VERIFICATION`**: Enable file integrity verification
- **Network Optimization**: Larger chunks = fewer requests but more memory usage

### 5. Conversion Configuration (CSV → Parquet)

```bash
ETL_MAX_MEMORY_MB=1024
ETL_COMPRESSION=snappy
ETL_ROW_GROUP_SIZE=100000
ETL_CONVERSION_FLUSH_THRESHOLD=10
ETL_CONVERSION_AUTO_FALLBACK=true
ETL_CONVERSION_ROW_ESTIMATION_FACTOR=8000
```

**Performance Tuning:**
- **`ETL_MAX_MEMORY_MB`**: Memory limit for conversion process
- **`ETL_COMPRESSION`**: Parquet compression algorithm (`snappy` | `gzip` | `lz4`)
- **`ETL_ROW_GROUP_SIZE`**: Parquet row group size (affects query performance)
- **`ETL_CONVERSION_FLUSH_THRESHOLD`**: Memory flush trigger
- **`ETL_CONVERSION_AUTO_FALLBACK`**: Automatic fallback for large files
- **`ETL_CONVERSION_ROW_ESTIMATION_FACTOR`**: Row size estimation for memory planning

### 6. Loading Configuration (Database Insertion)

```bash
ETL_MAX_BATCH_SIZE=500000
ETL_MIN_BATCH_SIZE=10000
ETL_BATCH_SIZE_MB=100
ETL_USE_COPY=true
ETL_ENABLE_INTERNAL_PARALLELISM=true
```

**Database Loading Optimization:**
- **`ETL_MAX_BATCH_SIZE`**: Maximum rows per database batch
- **`ETL_MIN_BATCH_SIZE`**: Minimum rows per database batch
- **`ETL_BATCH_SIZE_MB`**: Memory-based batch sizing
- **`ETL_USE_COPY`**: Use PostgreSQL COPY for bulk loading (faster than INSERT)
- **`ETL_ENABLE_INTERNAL_PARALLELISM`**: Enable parallel database loading

### 7. Development Mode Settings

```bash
ETL_DEV_FILE_SIZE_LIMIT_MB=70
ETL_DEV_MAX_FILES_PER_TABLE=5
ETL_DEV_MAX_FILES_PER_BLOB=3
ETL_DEV_MAX_BLOB_SIZE_MB=500
ETL_DEV_ROW_LIMIT_PERCENT=0.1
ETL_DEV_SAMPLE_PERCENTAGE=0.1
```

**Development Filtering (when `ENVIRONMENT=development`):**
- **`ETL_DEV_FILE_SIZE_LIMIT_MB`**: Skip files larger than 70MB
- **`ETL_DEV_MAX_FILES_PER_TABLE`**: Limit files per table type
- **`ETL_DEV_MAX_FILES_PER_BLOB`**: Limit files per ZIP archive
- **`ETL_DEV_MAX_BLOB_SIZE_MB`**: Skip large ZIP files
- **`ETL_DEV_ROW_LIMIT_PERCENT`**: Process only 10% of rows
- **`ETL_DEV_SAMPLE_PERCENTAGE`**: Sample percentage for testing

**Benefits**: Faster development cycles, reduced resource usage, representative data samples

### 8. Batch Tracking & Monitoring

```bash
BATCH_UPDATE_THRESHOLD=100
BATCH_UPDATE_INTERVAL=30
ENABLE_BULK_UPDATES=true
ENABLE_TEMPORAL_CONTEXT=true
DEFAULT_BATCH_SIZE=20000
BATCH_RETENTION_DAYS=30
ENABLE_BATCH_MONITORING=true
```

**Audit System Configuration:**
- **`BATCH_UPDATE_THRESHOLD`**: Minimum operations before batch update
- **`BATCH_UPDATE_INTERVAL`**: Update interval in seconds
- **`ENABLE_BULK_UPDATES`**: Use bulk update operations for performance
- **`ENABLE_TEMPORAL_CONTEXT`**: Enable time-based audit tracking
- **`DEFAULT_BATCH_SIZE`**: Default batch size for audit operations
- **`BATCH_RETENTION_DAYS`**: How long to keep audit records
- **`ENABLE_BATCH_MONITORING`**: Enable comprehensive batch monitoring

## Environment-Specific Recommendations

### Development Environment
```bash
ENVIRONMENT=development
ETL_WORKERS=2
ETL_CHUNK_SIZE=10000
ETL_MAX_MEMORY_MB=512
ETL_DEV_FILE_SIZE_LIMIT_MB=50
```

### Production Environment
```bash
ENVIRONMENT=production
ETL_WORKERS=8
ETL_CHUNK_SIZE=100000
ETL_MAX_MEMORY_MB=4096
ETL_IS_PARALLEL=true
ETL_ENABLE_INTERNAL_PARALLELISM=true
```

## Performance Tuning Guidelines

### Memory Optimization
- **Low Memory (< 8GB)**: Reduce `ETL_CHUNK_SIZE`, `ETL_MAX_MEMORY_MB`
- **High Memory (> 16GB)**: Increase chunk sizes and worker counts
- **Monitor**: Watch for OOM kills in system logs

### CPU Optimization
- **`ETL_WORKERS`**: Should not exceed CPU core count
- **`ETL_INTERNAL_CONCURRENCY`**: 2-4 for most systems
- **Balance**: More workers = more memory usage

### Network Optimization
- **`ETL_DOWNLOAD_CHUNK_SIZE_MB`**: Increase for fast connections
- **`ETL_MAX_RETRIES`**: Increase for unreliable networks
- **Timeout**: Adjust `ETL_TIMEOUT_SECONDS` based on network speed

### Database Optimization
- **Connection Pools**: Monitor connection usage
- **Batch Sizes**: Larger batches = better performance but more memory
- **`ETL_USE_COPY`**: Always `true` for bulk loading

## Security Considerations

### Database Credentials
- **Principle**: Use least-privilege database users
- **Separation**: Different users for main and audit databases
- **Networks**: Restrict database access to ETL servers only

### File System
- **Permissions**: ETL process should own its working directories
- **Cleanup**: Enable `ETL_DELETE_FILES` to prevent data accumulation
- **Monitoring**: Watch disk space usage

### External URLs
- **Validation**: Verify RF URLs before production deployment
- **Monitoring**: Monitor for URL changes or availability issues

## Troubleshooting Common Issues

### Out of Memory
```bash
# Reduce these values
ETL_CHUNK_SIZE=25000
ETL_MAX_MEMORY_MB=512
ETL_WORKERS=2
```

### Slow Performance
```bash
# Increase these values (if memory allows)
ETL_CHUNK_SIZE=100000
ETL_WORKERS=6
ETL_ENABLE_INTERNAL_PARALLELISM=true
```

### Database Connection Issues
```bash
# Reduce connection pressure
ETL_ASYNC_POOL_MAX_SIZE=5
ETL_WORKERS=2
ETL_INTERNAL_CONCURRENCY=2
```

### Development Testing
```bash
# Fast development cycles
ENVIRONMENT=development
ETL_DEV_FILE_SIZE_LIMIT_MB=10
ETL_DEV_MAX_FILES_PER_TABLE=2
ETL_DEV_ROW_LIMIT_PERCENT=0.05
```

## Monitoring & Metrics

### Key Metrics to Monitor
- **Memory Usage**: Process memory vs. `ETL_MAX_MEMORY_MB`
- **Processing Speed**: Rows/second vs. `ETL_CHUNK_SIZE`
- **Error Rates**: Failed operations vs. `ETL_MAX_RETRIES`
- **Database Connections**: Active connections vs. pool sizes

### Log Analysis
- Look for `[DEV-MODE]` logs when development filtering is active
- Monitor `[MEMORY]` logs for memory optimization opportunities
- Watch for retry patterns indicating network or database issues

## Migration & Deployment

### Environment File Management
1. **Template**: Use `.env.template` as reference
2. **Production**: Create `.env` from template with production values
3. **Security**: Never commit `.env` files to version control
4. **Validation**: Test configuration changes in development first


This documentation should be updated whenever new environment variables are added or existing ones are modified.
