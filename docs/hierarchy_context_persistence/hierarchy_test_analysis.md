# Hierarchy Context Persistence Analysis

## Current Implementation Analysis

### Your Test Case
- **Table**: 1 table with 2 CSV files  
- **Data**: 10,000 rows per file (20,000 total)
- **Configuration**: batch_size=1000, subbatch_size=500
- **Expected Results**:
  - 1 `table_ingestion_manifest` entry
  - 2 `file_ingestion_manifest` entries  
  - 10 `batch_ingestion_manifest` entries (5 per file)
  - 20 `subbatch_ingestion_manifest` entries (10 per file)

### Current Implementation Trace

#### What Actually Happens:

1. **`load_multiple_tables()`** calls `_process_file_batch()` for each zip/file group
2. **`_process_file_batch()`** calls `load_table()` for each CSV file individually
3. **`load_table()`** → `_create_manifest_and_load()` → `_load_with_batching()`
4. **`_load_with_batching()`** creates:
   - **Per file**: `math.ceil(10000/1000) = 10 batches`
   - **Per batch**: `math.ceil(1000/500) = 2 subbatches`
   - **Per file total**: 10 batches × 2 subbatches = 20 subbatch contexts

#### Actual Database Entries:

**❌ MISSING**: Table-level manifest (0 entries instead of 1)
**✅ CORRECT**: File-level manifests (2 entries) 
**✅ CORRECT**: Batch-level manifests (20 entries total: 10 per file)
**✅ CORRECT**: Subbatch-level manifests (40 entries total: 20 per file)

### Issues Identified

1. **No Table Context**: `load_multiple_tables()` doesn't create table-level audit context
2. **Missing Table Manifest**: No entry in `table_ingestion_manifest`
3. **Doubled Subbatches**: Getting 40 instead of 20 subbatch entries (each file processed completely in each subbatch)

### The Root Problem

The current implementation creates:
```
File1 (10k rows):
├── Batch1 (rows 0-1000)
│   ├── Subbatch1 (rows 0-500) → LOADS ENTIRE FILE
│   └── Subbatch2 (rows 500-1000) → LOADS ENTIRE FILE  
├── Batch2 (rows 1000-2000)
│   ├── Subbatch1 → LOADS ENTIRE FILE
│   └── Subbatch2 → LOADS ENTIRE FILE
...
```

**Problem**: `_load_entire_file()` is called in every subbatch, but it loads the **entire file each time**, not just the subbatch portion.

## Required Fixes

### 1. Add Table-Level Context
```python
def load_multiple_tables(self, database, table_to_files, path_config: PathConfig):
    # Create table-level audit context
    with self.audit_service.table_context(
        table_names=list(table_to_files.keys())
    ) as table_manifest_id:
        # Process files within table context
        ...
```

### 2. Fix File Loading Logic
Currently: `_load_entire_file()` loads complete file in every subbatch
Should be: Load specific row ranges per subbatch OR load once per file

### 3. Coordinate File → Batch → Subbatch Hierarchy
Need proper cascade where:
- Table context encompasses all files
- File context encompasses all its batches  
- Batch context encompasses all its subbatches
- Subbatch processes specific row ranges

## Conclusion

**Current Status**: ❌ **NOT CASCADING CORRECTLY**

Your expected hierarchy is sound, but the implementation has these issues:
1. Missing table-level context creation
2. File loading logic loads entire file multiple times
3. No proper row-range splitting in subbatches

The cascaded context managers are working (creating the database entries), but the file processing logic needs to be fixed to match the hierarchical structure.
