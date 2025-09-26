# Revised Hierarchy Context Management Using Existing FileLoader

## Key Insight

The user has a sophisticated `FileLoader` service that already handles:
- **Format detection** (CSV/Parquet auto-detection)
- **Batch processing** via `batch_generator()` method  
- **Encoding support** for CSV files
- **Chunk-based processing** with configurable `chunk_size`

Instead of `_load_entire_file()`, we should use the FileLoader's batch processing capabilities and wrap them with proper hierarchy context management.

## Revised Strategy Architecture

### Current Problem
```python
# Current broken approach
with subbatch_context() as subbatch_id:
    success, error, rows = self._load_entire_file(...)  # ❌ Loads entire file every time
```

### Proposed Solution
```python
# Revised approach using FileLoader
with batch_context() as batch_id:
    file_loader = FileLoader(file_path, encoding='utf-8')
    
    for batch_chunk in file_loader.batch_generator(headers, chunk_size=subbatch_size):
        with subbatch_context(batch_id) as subbatch_id:
            # Load only this chunk/subbatch
            success, error, rows = loader.load_batch_chunk(batch_chunk, batch_id, subbatch_id)
```

## Integration Points

### 1. File Loader Integration
```python
from ....core.services.loading.file_loader import FileLoader

def _load_with_file_loader_batching(self, loader, table_info, file_path, table_name, 
                                   batch_size: int, subbatch_size: int, 
                                   parent_batch_id=None, parent_subbatch_id=None):
    """
    Use FileLoader for proper batch processing with hierarchy context management.
    """
    # Initialize FileLoader with proper encoding for CNPJ files
    encoding = 'utf-8'  # or get from config
    file_loader = FileLoader(str(file_path), encoding=encoding)
    
    # Get table headers from table_info
    headers = [col.name for col in table_info.columns]
    
    batch_num = 0
    total_processed_rows = 0
    
    # Use FileLoader's batch_generator for actual row processing
    for batch_chunk in file_loader.batch_generator(headers, chunk_size=batch_size):
        batch_num += 1
        
        # Create batch context for this chunk
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]
        batch_name = f"Batch_{table_name}_chunk{batch_num}_{len(batch_chunk)}rows_{timestamp}"
        
        with self.audit_service.batch_context(
            target_table=table_name, 
            batch_name=batch_name
        ) as batch_id:
            
            # Process subbatches within this batch chunk
            subbatch_chunks = self._split_batch_into_subbatches(batch_chunk, subbatch_size)
            
            for subbatch_num, subbatch_chunk in enumerate(subbatch_chunks):
                subbatch_name = f"Subbatch_{subbatch_num+1}_rows{len(subbatch_chunk)}"
                
                with self.audit_service.subbatch_context(
                    batch_id=batch_id,
                    table_name=table_name,
                    description=subbatch_name
                ) as subbatch_id:
                    
                    # Load this specific subbatch chunk
                    success, error, rows = self._load_batch_chunk(
                        loader, table_info, subbatch_chunk, batch_id, subbatch_id
                    )
                    
                    if not success:
                        return False, error, total_processed_rows
                        
                    total_processed_rows += rows
    
    return True, None, total_processed_rows
```

### 2. Chunk Loading Method
```python
def _load_batch_chunk(self, loader, table_info, batch_chunk, batch_id, subbatch_id):
    """
    Load a specific batch chunk (list of tuples) using existing loader infrastructure.
    """
    try:
        # Create manifest entry for this chunk
        manifest_id = self._create_manifest_entry(
            f"chunk_{len(batch_chunk)}rows", table_info.name, "PROCESSING",
            batch_id=batch_id, subbatch_id=subbatch_id
        )
        
        # Use existing loader to insert batch_chunk
        # This needs to interface with your existing DatabaseLoader
        success, error, rows = loader.load_batch_data(
            table_info, batch_chunk, batch_id=batch_id, subbatch_id=subbatch_id
        )
        
        # Update manifest
        status = AuditStatus.COMPLETED if success else AuditStatus.FAILED
        self._update_manifest_entry(manifest_id, status, rows, str(error) if error else None)
        
        return success, error, rows
        
    except Exception as e:
        return False, str(e), 0
```

### 3. Table-Level Context Addition
```python
def load_multiple_tables(self, database, table_to_files, path_config: PathConfig):
    """
    Add proper table-level context management.
    """
    # Create table-level audit context
    table_names = list(table_to_files.keys())
    
    if self.audit_service and hasattr(self.audit_service, 'table_context'):
        context_manager = self.audit_service.table_context(table_names)
    else:
        # Fallback if no table context available
        from contextlib import nullcontext
        context_manager = nullcontext()
    
    with context_manager as table_manifest_id:
        # Process each table within table context
        results = {}
        
        for table_name, zipfile_to_files in table_to_files.items():
            # Process files using FileLoader batch processing
            # ... rest of existing logic
```

## Benefits of This Approach

1. **✅ Preserves FileLoader**: Uses existing, tested file processing logic
2. **✅ Proper Row Splitting**: Actually processes chunks/ranges instead of loading entire files
3. **✅ Maintains Hierarchy**: Table → File → Batch → Subbatch with proper contexts
4. **✅ Encoding Support**: Leverages FileLoader's encoding detection/configuration  
5. **✅ Format Agnostic**: Works with both CSV and Parquet via FileLoader auto-detection

## Expected Results for Test Case

With this approach, your test case will produce:
- **1** `table_ingestion_manifest` entry (table-level context)
- **2** `file_ingestion_manifest` entries (one per CSV file)
- **20** `batch_ingestion_manifest` entries (FileLoader chunks: ~10 per file)
- **40** `subbatch_ingestion_manifest` entries (subbatch splits within chunks)

The key difference: **actual row-level processing** instead of repeated full file loads.
