# Environment Variables Quick Reference

## 🚀 Quick Setup

### Development Environment
```bash
cp .env.template .env
# Edit .env with your settings
```

### Essential Variables
```bash
# Database
POSTGRES_HOST=localhost
POSTGRES_DBNAME=dadosrfb
AUDIT_DB_NAME=dadosrfb_analysis

# Performance
ETL_WORKERS=4
ETL_CHUNK_SIZE=50000
ETL_MAX_MEMORY_MB=1024
```

## 📊 Performance Tuning Cheat Sheet

| Scenario | ETL_WORKERS | ETL_CHUNK_SIZE | ETL_MAX_MEMORY_MB |
|----------|-------------|----------------|-------------------|
| **Low Memory (4GB)** | 2 | 10000 | 512 |
| **Medium (8GB)** | 4 | 50000 | 1024 |
| **High Memory (16GB+)** | 8 | 100000 | 4096 |

## 🔧 Common Configurations

### Development (Fast Testing)
```bash
ENVIRONMENT=development
ETL_DEV_FILE_SIZE_LIMIT_MB=10     # 10MB limit
ETL_DEV_MAX_FILES_PER_TABLE=2     # Only 2 files per table
ETL_DEV_ROW_LIMIT_PERCENT=0.05    # 5% of data
```

### Production (Full Processing)
```bash
ENVIRONMENT=production
ETL_IS_PARALLEL=true
ETL_ENABLE_INTERNAL_PARALLELISM=true
ETL_USE_COPY=true
ETL_DELETE_FILES=true
```

## 🐛 Troubleshooting

### Out of Memory?
```bash
ETL_CHUNK_SIZE=25000          # ⬇️ Reduce
ETL_MAX_MEMORY_MB=512         # ⬇️ Reduce
ETL_WORKERS=2                 # ⬇️ Reduce
```

### Too Slow?
```bash
ETL_WORKERS=6                 # ⬆️ Increase (≤ CPU cores)
ETL_CHUNK_SIZE=100000         # ⬆️ Increase
ETL_ENABLE_INTERNAL_PARALLELISM=true  # ✅ Enable
```

### Database Issues?
```bash
ETL_ASYNC_POOL_MAX_SIZE=5     # ⬇️ Reduce connections
ETL_MAX_RETRIES=5             # ⬆️ Increase retries
ETL_TIMEOUT_SECONDS=600       # ⬆️ Increase timeout
```

## 📁 File Paths

| Variable | Purpose | Typical Size |
|----------|---------|--------------|
| `DOWNLOAD_PATH` | ZIP files from RF | ~17GB |
| `EXTRACT_PATH` | Extracted CSV files | ~60GB |
| `CONVERT_PATH` | Parquet files | ~15GB |

## 🔒 Security Checklist

- [ ] Use separate database users for main and audit DBs
- [ ] Set restrictive file permissions on `.env`
- [ ] Never commit `.env` to version control
- [ ] Use least-privilege database permissions
- [ ] Monitor disk space usage

## 📈 Monitoring

### Key Metrics
- **Memory**: Process usage vs `ETL_MAX_MEMORY_MB`
- **Speed**: Rows/sec vs `ETL_CHUNK_SIZE`
- **Connections**: Active vs `ETL_ASYNC_POOL_MAX_SIZE`

### Log Patterns
```bash
# Development mode active
grep "\[DEV-MODE\]" logs/

# Memory optimization
grep "\[MEMORY\]" logs/

# Error patterns
grep "ERROR" logs/ | grep -E "(retry|timeout|connection)"
```

---
📖 **Full Documentation**: See [ENVIRONMENT_VARIABLES.md](./ENVIRONMENT_VARIABLES.md) for complete details.
