# Batch/Subbatch Manifest Persistence Analysis
## CNPJ ETL Pipeline - Comprehensive Documentation

*Analysis Date: September 9, 2025*  
*Generated from codebase investigation of audit service and loading strategies*

---

## 📋 Executive Summary

The CNPJ ETL pipeline implements a **4-tier hierarchical manifest persistence system** providing comprehensive observability for processing 60M+ records across ~17GB of Brazilian Federal Revenue data. This system enables precise tracking, error isolation, recovery capabilities, and performance monitoring from pipeline-level down to individual file processing.

---

## 🏗️ Architecture Overview

### **Hierarchical Structure**

```
┌─────────────────────────────────────────────────────────────────┐
│ 1. TABLE INGESTION MANIFEST (table_ingestion_manifest)         │
│    ├─ Table-Level Metadata: AuditDB model                      │
│    ├─ Temporal Context: Year/Month (2025-02)                   │
│    ├─ Target: Single table (empresa, estabelecimento, etc.)    │
│    └─ Scope: Table processing metadata and lifecycle           │
│                                                                 │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │ 2. FILE INGESTION MANIFEST (file_ingestion_manifest)     │ │
│  │    ├─ File-Level Metadata: AuditManifest model           │ │
│  │    ├─ Links to: audit_id (table manifest)                │ │
│  │    ├─ Target: Individual file processing tracking        │ │
│  │    └─ Purpose: File integrity and processing status      │ │
│  │                                                           │ │
│  │  ┌─────────────────────────────────────────────────────┐ │ │
│  │  │ 3. BATCH INGESTION MANIFEST (batch_ingestion_manifest) │ │
│  │  │    ├─ Batch-Level Execution: BatchIngestionManifest│ │ │
│  │  │    ├─ Links to: file_manifest_id (primary file)    │ │ │
│  │  │    ├─ Target: Logical processing unit               │ │ │
│  │  │    └─ Purpose: Batch execution coordination        │ │ │
│  │  │                                                     │ │ │
│  │  │  ┌───────────────────────────────────────────────┐ │ │ │
│  │  │  │ 4. SUBBATCH INGESTION MANIFEST               │ │ │ │
│  │  │  │    ├─ Granular Units: SubbatchIngestionManifest│ │ │ │
│  │  │  │    ├─ Links to: batch_manifest_id (parent)    │ │ │ │
│  │  │  │    ├─ Parallel Processing Capability          │ │ │ │
│  │  │  │    └─ Atomic Operation Tracking               │ │ │ │
│  │  │  └───────────────────────────────────────────────┘ │ │ │
│  │  └─────────────────────────────────────────────────────┘ │ │
│  └───────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

---

## 🗄️ Database Models & Schema

### **1. TableIngestionManifest** (Table-Level Metadata)

```sql
-- Table: table_ingestion_manifest (AuditDB model)
CREATE TABLE table_ingestion_manifest (
    audi_id UUID PRIMARY KEY,
    audi_table_name VARCHAR(255) NOT NULL,     -- "empresa", "estabelecimento", "simples"
    audi_filenames JSON NOT NULL,              -- Array of files for this table
    audi_file_size_bytes BIGINT,               -- Total size of all files
    audi_source_updated_at TIMESTAMP,          -- When source was last updated
    audi_created_at TIMESTAMP,                 -- Audit record creation
    audi_downloaded_at TIMESTAMP,              -- Download completion
    audi_processed_at TIMESTAMP,               -- Processing completion  
    audi_inserted_at TIMESTAMP,                -- Database insertion completion
    audi_metadata JSON,                        -- Additional metadata
    audi_ingestion_year INTEGER NOT NULL,      -- ETL year context
    audi_ingestion_month INTEGER NOT NULL      -- ETL month context
);
```

**Key Features:**
- **Table-Level Scope**: One record per table per ETL run
- **Temporal Context**: Year/month for ETL batching
- **Lifecycle Tracking**: Complete audit trail from source to database
- **File Aggregation**: JSON array of all files for the table
- **Precedence Validation**: Ensures timestamps follow logical order

### **2. FileIngestionManifest** (File-Level Processing)

```sql
-- Table: file_ingestion_manifest (AuditManifest model)
CREATE TABLE file_ingestion_manifest (
    file_manifest_id UUID PRIMARY KEY,
    audit_id UUID REFERENCES table_ingestion_manifest(audi_id) NOT NULL,
    batch_id UUID REFERENCES batch_ingestion_manifest(batch_id),
    subbatch_id UUID REFERENCES subbatch_ingestion_manifest(subbatch_manifest_id),
    table_name VARCHAR(100),                    -- Associated table
    file_path TEXT NOT NULL,                    -- Full file path
    status VARCHAR(64) NOT NULL,                -- COMPLETED, FAILED, PARTIAL
    checksum TEXT,                              -- SHA256 for integrity
    filesize BIGINT,                            -- Bytes processed
    rows_processed BIGINT,                      -- Actual rows loaded
    processed_at TIMESTAMP,                     -- Processing timestamp
    error_message TEXT,                         -- Error details
    notes TEXT                                  -- Additional metadata
);
```

**Key Features:**
- **File-Level Granularity**: One record per file processed
- **Multi-Level Links**: References table audit, batch, and subbatch
- **Integrity Verification**: SHA256 checksums with configurable threshold (1GB default)
- **Processing Metrics**: Precise row counts and file sizes
- **Error Tracking**: Detailed error messages and status codes

### **3. BatchIngestionManifest** (Batch Execution Tracking)

```sql
-- Table: batch_ingestion_manifest
CREATE TABLE batch_ingestion_manifest (
    batch_id UUID PRIMARY KEY,
    file_manifest_id UUID REFERENCES file_ingestion_manifest(file_manifest_id),
    batch_name VARCHAR(200) NOT NULL,          -- "File_Simples.zip_simples_20250909_211245"
    target_table VARCHAR(100) NOT NULL,        -- "simples" (single table)
    status VARCHAR(64),                         -- RUNNING, COMPLETED, FAILED
    started_at TIMESTAMP,                       -- Batch start time
    completed_at TIMESTAMP,                     -- Batch completion time
    description TEXT,                           -- Context + metrics summary
    error_message TEXT                          -- Failure details
);
```

**Key Features:**
- **Batch-Level Execution**: Tracks logical processing units
- **File Reference**: Optional link to primary file being processed
- **Single Table Focus**: Each batch targets one specific table
- **Status Lifecycle**: PENDING → RUNNING → COMPLETED/FAILED
- **Execution Metrics**: Duration and completion tracking

### **4. SubbatchIngestionManifest** (Granular Processing Units)

```sql
-- Table: subbatch_ingestion_manifest
CREATE TABLE subbatch_ingestion_manifest (
    subbatch_manifest_id UUID PRIMARY KEY,
    batch_manifest_id UUID REFERENCES batch_ingestion_manifest(batch_id),
    table_name VARCHAR(100) NOT NULL,           -- "empresa", "estabelecimento"
    status VARCHAR(64),                         -- RUNNING, COMPLETED, FAILED
    started_at TIMESTAMP,                       -- Subbatch start time
    completed_at TIMESTAMP,                     -- Subbatch completion time
    description TEXT,                           -- "Process_EMPRE1234.csv_part1"
    error_message TEXT,                         -- Failure details
    files_processed INTEGER DEFAULT 0,          -- Files in this subbatch
    rows_processed BIGINT DEFAULT 0             -- Rows processed in this subbatch
);
```

**Key Features:**
- **Hierarchical Relationship**: `batch_manifest_id` → `batch_ingestion_manifest`
- **Granular Scope**: Individual file or row range processing
- **Parallel Processing**: Multiple subbatches can run concurrently
- **Processing Metrics**: Direct tracking of files/rows processed
- **Execution Details**: Comprehensive status and error tracking

### **3. FileIngestionManifest** (File-Level Metadata)

*Note: This section was moved to position 2 to reflect the correct hierarchy order. The file ingestion manifest serves as the bridge between table audits and batch execution.*

---

## ⚙️ Implementation Patterns

### **Context Manager Pattern**

```python
# File-level batch processing with nested subbatches
with self.audit_service.batch_context(
    target_table=table_name,
    batch_name=f"File_{zip_filename}_{table_name}_{timestamp}"
) as batch_id:
    
    for csv_file in csv_files:
        with self.audit_service.subbatch_context(
            batch_id=batch_id,
            table_name=table_name,
            description=f"Process_{csv_file}"
        ) as subbatch_id:
            # Actual data loading
            result = self.load_table(
                database, table_name, path_config, [csv_file],
                batch_id=batch_id, subbatch_id=subbatch_id
            )
```

### **In-Memory Metrics Accumulation**

```python
@dataclass
class BatchAccumulator:
    """Efficient metrics collection without DB hits per operation"""
    files_completed: int = 0
    files_failed: int = 0
    total_rows: int = 0
    total_bytes: int = 0
    start_time: float = field(default_factory=time.time)
    last_activity: float = field(default_factory=time.time)

    def add_file_event(self, status: str, rows: int = 0, bytes_: int = 0):
        """Add file processing event to accumulator"""
        if status in ['COMPLETED', 'SUCCESS']:
            self.files_completed += 1
            self.total_rows += rows
            self.total_bytes += bytes_
        elif status in ['FAILED', 'ERROR']:
            self.files_failed += 1
        self.last_activity = time.time()
```

### **Thread-Safe Metrics Collection**

```python
class AuditService:
    def __init__(self, database: Database, config: ConfigurationService):
        # Thread-safe in-memory tracking
        self._active_batches: Dict[str, BatchAccumulator] = {}
        self._active_subbatches: Dict[str, BatchAccumulator] = {}
        self._subbatch_to_batch: Dict[str, str] = {}
        self._metrics_lock = Lock()

    def _collect_file_event(self, subbatch_id: str, status: str, 
                           rows: int = 0, bytes_: int = 0) -> None:
        """Thread-safe metrics accumulation"""
        with self._metrics_lock:
            # Update subbatch accumulator
            if subbatch_id_str in self._active_subbatches:
                accumulator = self._active_subbatches[subbatch_id_str]
                accumulator.add_file_event(status, rows, bytes_)
                
                # Update parent batch accumulator
                batch_id = self._subbatch_to_batch.get(subbatch_id_str)
                if batch_id in self._active_batches:
                    self._active_batches[batch_id].add_file_event(status, rows, bytes_)
```

### **Row-Driven Batching for Large Files**

```python
def _load_with_row_driven_batches(self, loader, table_info, file_path, table_name, 
                                 total_rows: int, batch_size: int = 50000, 
                                 subbatch_size: int = 10000):
    """
    Creates multiple BatchIngestionManifest entries for very large files.
    Example: 1M rows → 20 batches of 50K rows → 100 subbatches of 10K rows
    """
    total_batches = math.ceil(total_rows / batch_size)
    
    for batch_num in range(total_batches):
        start_row = batch_num * batch_size
        end_row = min(start_row + batch_size, total_rows)
        
        # Create dedicated batch for this row range
        batch_name = f"RowBatch_{table_name}_{batch_num+1}of{total_batches}_{timestamp}"
        batch_id = self.audit_service._start_batch(target_table=table_name, batch_name=batch_name)
        
        # Process in smaller subbatches within this batch
        subbatches_in_batch = math.ceil((end_row - start_row) / subbatch_size)
        for subbatch_num in range(subbatches_in_batch):
            subbatch_start = start_row + (subbatch_num * subbatch_size)
            subbatch_end = min(subbatch_start + subbatch_size, end_row)
            
            with self.audit_service.subbatch_context(
                batch_id=batch_id,
                table_name=table_name,
                description=f"RowSubbatch_{subbatch_num+1}of{subbatches_in_batch}_rows{subbatch_start}-{subbatch_end}"
            ) as subbatch_id:
                # Load specific row range
                self._load_row_range(loader, table_info, file_path, table_name,
                                   subbatch_start, subbatch_end, batch_id, subbatch_id)
```

---

## 🔄 Lifecycle Management

### **Batch Lifecycle States**

```python
# State transitions for BatchIngestionManifest
BATCH_STATES = {
    'CREATED': 'Batch created, not yet started',
    'RUNNING': 'Active processing of subbatches',
    'COMPLETED': 'All subbatches completed successfully',
    'FAILED': 'One or more subbatches failed',
    'PARTIAL': 'Some subbatches completed, others failed'
}

# State transitions for SubbatchIngestionManifest  
SUBBATCH_STATES = {
    'RUNNING': 'Processing individual files/row ranges',
    'COMPLETED': 'All files in subbatch processed successfully',
    'FAILED': 'Processing failed with error details'
}
```

### **Persistence Timeline**

```
Timeline: Table Audit → File Processing → Batch Execution → Subbatch Processing
─────────────────────────────────────────────────────────────────────────────

1. TABLE AUDIT CREATION (t=0s)
   ├─ INSERT INTO table_ingestion_manifest (AuditDB)
   ├─ Status: Created with table metadata
   ├─ Links: audi_table_name, audi_filenames array
   └─ Return audi_id

2. FILE MANIFEST CREATION (t=1s)
   ├─ INSERT INTO file_ingestion_manifest (AuditManifest)
   ├─ Links: audit_id → table audit
   ├─ Status: File processing initiated
   └─ Return file_manifest_id

3. BATCH START (t=2s)
   ├─ INSERT INTO batch_ingestion_manifest
   ├─ Links: file_manifest_id → primary file (optional)
   ├─ Status: RUNNING
   ├─ Create BatchAccumulator in memory
   └─ Return batch_id

4. SUBBATCH START (t=3s)
   ├─ INSERT INTO subbatch_ingestion_manifest  
   ├─ Links: batch_manifest_id → parent batch
   ├─ Status: RUNNING
   ├─ Create SubbatchAccumulator in memory
   └─ Return subbatch_id

5. FILE PROCESSING (t=4s-30s)
   ├─ Process CSV/Parquet files
   ├─ Accumulate metrics in memory (thread-safe)
   ├─ UPDATE file_ingestion_manifest with progress
   └─ Link processing to batch_id and subbatch_id

6. SUBBATCH COMPLETION (t=30s)
   ├─ UPDATE subbatch_ingestion_manifest
   ├─ Status: COMPLETED/FAILED
   ├─ Set final metrics (files_processed, rows_processed)
   └─ Clean up SubbatchAccumulator

7. BATCH COMPLETION (t=45s)
   ├─ UPDATE batch_ingestion_manifest
   ├─ Status: COMPLETED/FAILED  
   ├─ Aggregate metrics from all subbatches
   └─ Clean up BatchAccumulator

8. TABLE AUDIT COMPLETION (t=50s)
   ├─ UPDATE table_ingestion_manifest
   ├─ Set audi_inserted_at timestamp
   └─ Complete table processing lifecycle
```

---

## 📊 Observability & Monitoring

### **Available Metrics**

**Batch-Level Metrics:**
- **Duration**: Total processing time from start to completion
- **Throughput**: Rows processed per second across all subbatches
- **Success Rate**: Percentage of subbatches that completed successfully  
- **File Count**: Total files processed across all subbatches
- **Data Volume**: Total bytes processed

**Subbatch-Level Metrics:**
- **Individual Performance**: Processing time per file/row range
- **Error Isolation**: Specific failure points with detailed error messages
- **Parallel Efficiency**: Concurrent subbatch processing analysis
- **Resource Usage**: Memory and CPU utilization per subbatch

**File-Level Metrics:**
- **Integrity Verification**: SHA256 checksums for data validation
- **Processing Status**: COMPLETED, FAILED, PARTIAL per file
- **Row Accuracy**: Expected vs actual row counts
- **Size Analysis**: File size distribution and processing patterns

### **Query Examples**

```sql
-- Get batch processing summary for specific month
SELECT 
    b.batch_name,
    b.target_table,
    b.status,
    b.started_at,
    b.completed_at,
    EXTRACT(EPOCH FROM (b.completed_at - b.started_at)) as duration_seconds,
    b.files_processed,
    b.rows_processed,
    COUNT(s.subbatch_manifest_id) as subbatch_count
FROM batch_ingestion_manifest b
LEFT JOIN subbatch_ingestion_manifest s ON b.batch_id = s.batch_manifest_id
WHERE b.batch_name LIKE '%2025_02%'
GROUP BY b.batch_id, b.batch_name, b.target_table, b.status, b.started_at, b.completed_at, b.files_processed, b.rows_processed
ORDER BY b.started_at DESC;

-- Find problematic files with processing issues
SELECT 
    f.file_path,
    f.table_name,
    f.status,
    f.error_message,
    f.filesize,
    f.rows_processed,
    s.description as subbatch_context,
    b.batch_name
FROM file_ingestion_manifest f
JOIN subbatch_ingestion_manifest s ON f.subbatch_id = s.subbatch_manifest_id
JOIN batch_ingestion_manifest b ON s.batch_manifest_id = b.batch_id
WHERE f.status IN ('FAILED', 'PARTIAL')
ORDER BY f.processed_at DESC;

-- Performance analysis: throughput by table
SELECT 
    s.table_name,
    COUNT(*) as subbatch_count,
    SUM(s.files_processed) as total_files,
    SUM(s.rows_processed) as total_rows,
    AVG(EXTRACT(EPOCH FROM (s.completed_at - s.started_at))) as avg_duration_seconds,
    SUM(s.rows_processed) / SUM(EXTRACT(EPOCH FROM (s.completed_at - s.started_at))) as avg_rows_per_second
FROM subbatch_ingestion_manifest s
WHERE s.status = 'COMPLETED'
  AND s.completed_at >= NOW() - INTERVAL '7 days'
GROUP BY s.table_name
ORDER BY avg_rows_per_second DESC;

-- Checksum verification and integrity analysis
SELECT 
    f.table_name,
    COUNT(*) as total_files,
    COUNT(f.checksum) as files_with_checksum,
    AVG(f.filesize) as avg_file_size,
    SUM(CASE WHEN f.status = 'COMPLETED' THEN 1 ELSE 0 END) as successful_files,
    SUM(CASE WHEN f.status = 'FAILED' THEN 1 ELSE 0 END) as failed_files
FROM file_ingestion_manifest f
WHERE f.processed_at >= NOW() - INTERVAL '24 hours'
GROUP BY f.table_name
ORDER BY total_files DESC;
```

---

## 🔧 Configuration & Optimization

### **Environment Variables**

```bash
# Batch tracking configuration
ETL_BATCH_ENABLED=true                          # Enable/disable batch tracking
ETL_BATCH_TEMPORAL_CONTEXT=true                 # Include year/month in batch names
ETL_CHECKSUM_THRESHOLD_MB=1000                  # Skip checksums for files >1GB
ETL_CHUNK_SIZE=50000                            # Rows per processing chunk
ETL_INTERNAL_CONCURRENCY=3                      # Parallel subbatches

# Development mode filtering (affects batch size)
ETL_DEV_MODE=true                               # Enable development filtering
ETL_DEV_FILE_SIZE_LIMIT_MB=500                  # Only sample files >500MB
ETL_DEV_ROW_LIMIT_PERCENT=10                    # Sample 10% of large files
ETL_DEV_MAX_FILES_PER_TABLE=5                   # Limit files per table in dev
```

### **Performance Optimizations**

**In-Memory Accumulation:**
- Metrics collected in `BatchAccumulator` to minimize DB calls
- Thread-safe operations with `Lock()` for concurrent processing
- Single DB update per batch/subbatch completion

**Selective Checksum Calculation:**
- Skip SHA256 for files >1GB (configurable threshold)
- Focus integrity verification on smaller, critical files
- Performance vs integrity trade-off

**Hierarchical Cleanup:**
- Automatic cleanup of in-memory accumulators
- Foreign key constraints ensure referential integrity
- Batch failure doesn't orphan subbatch records

---

## 🚨 Error Handling & Recovery

### **Failure Modes**

**Batch-Level Failures:**
- **Pipeline Crashes**: Entire ETL run fails, batch marked as FAILED
- **Resource Exhaustion**: Memory/disk issues during processing
- **Configuration Errors**: Invalid parameters or missing dependencies

**Subbatch-Level Failures:**
- **File Corruption**: Invalid CSV/Parquet format detected
- **Schema Mismatches**: Column count/type inconsistencies
- **Database Connectivity**: Connection timeouts or transaction failures

**File-Level Failures:**
- **Missing Files**: Expected files not found in extract directory
- **Permission Issues**: File access denied during processing
- **Integrity Violations**: Checksum mismatches indicating corruption

### **Recovery Strategies**

**Granular Restart:**
```python
# Query failed subbatches for targeted recovery
failed_subbatches = audit_service.get_failed_subbatches(batch_id)
for subbatch in failed_subbatches:
    # Restart specific subbatch without affecting completed ones
    audit_service.restart_subbatch(subbatch.subbatch_manifest_id)
```

**Batch Resumption:**
```python
# Resume from last successful batch
last_successful = audit_service.get_last_successful_batch(table_name)
remaining_files = audit_service.get_unprocessed_files(since=last_successful.completed_at)
# Process only remaining files
```

**File Integrity Recovery:**
```python
# Re-download corrupted files based on checksum mismatches
corrupted_files = audit_service.get_files_with_checksum_issues()
for file_manifest in corrupted_files:
    # Re-download and re-process specific files
    pipeline.redownload_file(file_manifest.file_path)
```

---

## 🎯 Usage Patterns & Best Practices

### **Development vs Production**

**Development Mode:**
- File size filtering: Only process files >500MB for sampling
- Row limiting: Process 10% of large files for faster iteration
- Reduced concurrency: Limit parallel subbatches to avoid resource contention

**Production Mode:**
- Full processing: All files processed without sampling
- Maximum throughput: Optimal concurrency based on system resources
- Complete integrity: All checksums calculated and verified

### **Monitoring Best Practices**

**Dashboard Queries:**
- Batch completion rates by table and time period
- Average processing duration trends
- Error frequency and failure patterns
- Data throughput metrics (rows/second, MB/minute)

**Alert Conditions:**
- Batch failures requiring immediate attention
- Subbatch error rates exceeding thresholds
- Processing duration significantly above baseline
- Checksum failures indicating data corruption

### **Maintenance Operations**

**Regular Cleanup:**
```sql
-- Archive old manifest records (>90 days)
INSERT INTO manifest_archive 
SELECT * FROM file_ingestion_manifest 
WHERE processed_at < NOW() - INTERVAL '90 days';

DELETE FROM file_ingestion_manifest 
WHERE processed_at < NOW() - INTERVAL '90 days';
```

**Performance Analysis:**
```sql
-- Identify tables requiring optimization
SELECT table_name, AVG(duration), AVG(rows_per_second)
FROM batch_performance_view
WHERE processed_date >= NOW() - INTERVAL '30 days'
GROUP BY table_name
HAVING AVG(duration) > 300  -- Tables taking >5 minutes
ORDER BY AVG(duration) DESC;
```

---

## 📈 Future Enhancements

### **Planned Improvements**

**Enhanced Parallel Processing:**
- Dynamic subbatch sizing based on file characteristics
- Adaptive concurrency based on system resource availability
- Cross-table parallel processing with dependency management

**Advanced Monitoring:**
- Real-time progress tracking with WebSocket updates
- Predictive failure detection based on processing patterns
- Automated recovery workflows for common failure scenarios

**Data Quality Integration:**
- Schema validation within batch processing
- Data quality metrics collection per batch/subbatch
- Automated data profiling and anomaly detection

### **Integration Opportunities**

**External Monitoring:**
- Prometheus metrics export for batch/subbatch status
- Grafana dashboards for real-time ETL monitoring
- PagerDuty integration for critical failure alerts

**Cloud Optimization:**
- AWS Batch integration for scalable processing
- S3-based checkpoint storage for large batch recovery
- Lambda-triggered batch restart for automated recovery

---

## 📚 References & Related Documentation

**Core Implementation Files:**
- `src/core/services/audit/service.py` - Audit service with batch management
- `src/core/services/loading/strategies.py` - Loading strategies with batch context
- `src/database/models.py` - Database models for manifest persistence
- `src/core/etl.py` - Main pipeline with batch integration

**Configuration Files:**
- `.env.template` - Environment variable documentation
- `src/setup/config.py` - Configuration management system
- `src/core/constants.py` - Table definitions and metadata

**Related Lab Files:**
- `lab/main.ipynb` - Interactive analysis and testing
- `lab/pk_candidate_evaluator.py` - Primary key analysis
- `lab/test_files_row_integrity.py` - Data validation utilities

---

*This document represents a comprehensive analysis of the batch/subbatch manifest persistence system as implemented in the CNPJ ETL pipeline. The system provides production-grade observability, error handling, and recovery capabilities for processing large-scale Brazilian government datasets.*

**Last Updated:** September 9, 2025  
**Version:** 1.0  
**Authors:** AI Analysis based on codebase investigation
