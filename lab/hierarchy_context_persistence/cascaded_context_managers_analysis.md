# Cascaded Context Managers Analysis

## Executive Summary

After analyzing the existing CNPJ ETL architecture, **cascaded context managers are the perfect solution** for implementing proper table → file → batch → subbatch hierarchy while preserving all existing excellent patterns.

**Key Finding**: We don't need to replace anything - we can enhance the existing `AuditService` and `DataLoadingService` with minimal additions that leverage the proven patterns already in place.

## Current Architecture Strengths

### 🎯 **Existing Excellence to Preserve**

#### 1. AuditService Context Managers
```python
# Already implemented and working perfectly
@contextmanager
def batch_context(self, target_table: str, batch_name: str, year=None, month=None):
    # Excellent temporal tracking
    # Proper resource management
    # Accumulated metrics with BatchAccumulator
    # Exception handling and cleanup

@contextmanager  
def subbatch_context(self, batch_id: uuid.UUID, table_name: str, description=None):
    # Perfect parent-child relationship handling
    # Structured logging with proper context
    # Metric accumulation and rollup
```

#### 2. DataLoadingService Strategy Pattern
```python
class DataLoadingService:
    # Excellent strategy pattern implementation
    # BatchSizeOptimizer integration
    # Proper ETL configuration delegation
    # Audit service integration
```

#### 3. BatchAccumulator Pattern
```python
@dataclass
class BatchAccumulator:
    files_completed: int = 0
    files_failed: int = 0
    total_rows: int = 0
    total_bytes: int = 0
    # Perfect metrics aggregation pattern
```

#### 4. FileLoader Architecture
```python
class FileLoader:
    # Robust 4-layer format detection
    # Configurable encoding support
    # Unified CSV/Parquet interface
    # Excellent error handling
```

## Cascaded Context Manager Design

### 🏗️ **Natural Hierarchy Extension**

The existing architecture already demonstrates the pattern - we just need to extend it upward:

```python
# Current (excellent, keep as-is):
with audit_service.batch_context("empresa", "RowBatch_1of5") as batch_id:
    with audit_service.subbatch_context(batch_id, "empresa", "rows_0-1000") as subbatch_id:
        # Processing happens here

# Enhanced (cascaded):
with audit_service.table_context("empresa") as table_ctx:
    with audit_service.file_context(table_ctx, file_path, audit_id) as file_ctx:
        with audit_service.batch_context(file_ctx, "RowBatch_1of5") as batch_ctx:
            with audit_service.subbatch_context(batch_ctx, "rows_0-1000") as subbatch_ctx:
                # Same processing, better hierarchy
```

### 🔧 **Context Objects Design**

```python
@dataclass
class TableContext:
    """Table-level processing context."""
    table_id: str
    table_name: str
    audit_service: 'AuditService'
    start_time: float
    files_processed: int = 0
    files_failed: int = 0
    total_rows: int = 0
    total_bytes: int = 0
    
    def add_file_result(self, success: bool, rows: int = 0, bytes_: int = 0):
        """Accumulate file processing results."""
        if success:
            self.files_processed += 1
        else:
            self.files_failed += 1
        self.total_rows += rows
        self.total_bytes += bytes_

@dataclass  
class FileContext:
    """File-level processing context."""
    file_id: str
    file_path: Path
    audit_id: str
    table_context: TableContext
    audit_service: 'AuditService'
    start_time: float
    file_size_bytes: int = 0
    format_detected: str = ""
    
    def get_batch_name(self, batch_num: int, total_batches: int) -> str:
        """Generate consistent batch names."""
        return f"File_{self.file_path.name}_Batch_{batch_num}of{total_batches}"

# Keep existing BatchContext and SubbatchContext as-is!
```

### 🎯 **Implementation Strategy**

#### Phase 1: Add Context Classes (Zero Disruption)
- Add `TableContext` and `FileContext` dataclasses
- No changes to existing code
- Pure additive enhancement

#### Phase 2: Add Context Manager Methods (Minimal Addition)
```python
class AuditService:  # Keep ALL existing methods unchanged!
    
    @contextmanager
    def table_context(self, table_name: str) -> Generator[TableContext, None, None]:
        """NEW: Table-level coordination context."""
        table_id = str(uuid.uuid4())
        start_time = time.time()
        
        table_ctx = TableContext(
            table_id=table_id,
            table_name=table_name,
            audit_service=self,
            start_time=start_time
        )
        
        try:
            logger.info(f"[TableContext] Starting table processing: {table_name}")
            yield table_ctx
            
            # Aggregate metrics and log summary
            duration = time.time() - start_time
            logger.info(f"[TableContext] Table {table_name} completed: "
                       f"{table_ctx.files_processed} files, {table_ctx.total_rows:,} rows, "
                       f"{duration:.1f}s")
            
        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"[TableContext] Table {table_name} failed after {duration:.1f}s: {e}")
            raise
    
    @contextmanager
    def file_context(self, table_ctx: TableContext, file_path: Path, audit_id: str) -> Generator[FileContext, None, None]:
        """NEW: File-level coordination context."""
        file_id = str(uuid.uuid4())
        start_time = time.time()
        
        # Get file size for metrics
        file_size = file_path.stat().st_size if file_path.exists() else 0
        
        file_ctx = FileContext(
            file_id=file_id,
            file_path=file_path,
            audit_id=audit_id,
            table_context=table_ctx,
            audit_service=self,
            start_time=start_time,
            file_size_bytes=file_size
        )
        
        try:
            logger.info(f"[FileContext] Starting file processing: {file_path.name} "
                       f"({file_size / (1024*1024):.1f}MB)")
            yield file_ctx
            
            # File completed successfully
            duration = time.time() - start_time
            logger.info(f"[FileContext] File {file_path.name} completed in {duration:.1f}s")
            
        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"[FileContext] File {file_path.name} failed after {duration:.1f}s: {e}")
            raise
    
    # ENHANCED: Update existing batch_context to work with FileContext
    @contextmanager
    def batch_context(self, context_or_table: Union[FileContext, str], batch_name: str, 
                     year: Optional[int] = None, month: Optional[int] = None) -> Generator[uuid.UUID, None, None]:
        """
        Enhanced batch context - works with FileContext OR legacy table name.
        Maintains backward compatibility while enabling cascaded usage.
        """
        # Handle both new FileContext and legacy string table name
        if isinstance(context_or_table, FileContext):
            target_table = context_or_table.table_context.table_name
            file_context = context_or_table
        else:
            target_table = context_or_table
            file_context = None
        
        # Use existing implementation with context awareness
        batch_name_with_context = self._build_batch_name(batch_name, year, month)
        batch_id = self._start_batch(target_table, batch_name_with_context)
        
        try:
            logger.info(f"[BatchContext] Starting batch: {batch_name_with_context}", 
                       extra={
                           "batch_id": str(batch_id), 
                           "table_name": target_table,
                           "file_path": str(file_context.file_path) if file_context else None
                       })
            yield batch_id
            
            self._complete_batch_with_accumulated_metrics(batch_id, BatchStatus.COMPLETED)
            logger.info(f"[BatchContext] Batch completed: {batch_name_with_context}")
            
        except Exception as e:
            logger.error(f"[BatchContext] Batch failed: {batch_name_with_context}: {e}")
            self._complete_batch_with_accumulated_metrics(batch_id, BatchStatus.FAILED, str(e))
            raise
        finally:
            self._cleanup_batch_context(batch_id)
    
    # Keep existing subbatch_context UNCHANGED!
```

#### Phase 3: Update LoadingStrategy (Optional Enhancement)
```python
class DataLoadingStrategy:  # Keep existing methods!
    
    def load_table_with_cascade(self, database: Database, table_name: str, 
                               files_with_audits: List[Tuple[Path, str]]) -> Tuple[bool, Optional[str], int]:
        """NEW: Enhanced table loading with cascaded contexts."""
        if not self.audit_service:
            # Fallback to existing method
            return self.load_table(database, table_name, ...)
        
        total_rows = 0
        failed_files = []
        
        with self.audit_service.table_context(table_name) as table_ctx:
            for file_path, audit_id in files_with_audits:
                try:
                    with self.audit_service.file_context(table_ctx, file_path, audit_id) as file_ctx:
                        # Determine batching strategy based on file size
                        if self._needs_row_batching(file_path):
                            rows = self._load_file_with_row_batching(database, file_ctx)
                        else:
                            rows = self._load_file_single_batch(database, file_ctx)
                        
                        total_rows += rows
                        table_ctx.add_file_result(True, rows, file_ctx.file_size_bytes)
                        
                except Exception as e:
                    logger.error(f"Failed to process file {file_path.name}: {e}")
                    failed_files.append(str(e))
                    table_ctx.add_file_result(False, 0, 0)
        
        if failed_files:
            return False, f"Failed {len(failed_files)} files: {'; '.join(failed_files[:3])}", total_rows
        else:
            return True, None, total_rows
    
    def _load_file_with_row_batching(self, database: Database, file_ctx: FileContext) -> int:
        """Load large file with row-driven batching."""
        total_rows = 0
        file_size_mb = file_ctx.file_size_bytes / (1024 * 1024)
        estimated_rows = int(file_size_mb * 8000)  # Heuristic for CNPJ data
        batch_size = getattr(self.config.etl, 'row_batch_size', 10000)
        
        total_batches = max(1, math.ceil(estimated_rows / batch_size))
        
        for batch_num in range(total_batches):
            batch_name = file_ctx.get_batch_name(batch_num + 1, total_batches)
            
            with self.audit_service.batch_context(file_ctx, batch_name) as batch_id:
                # Process subbatches within this batch
                subbatch_rows = self._process_batch_subbatches(database, file_ctx, batch_id, batch_num, batch_size)
                total_rows += subbatch_rows
        
        return total_rows
    
    def _load_file_single_batch(self, database: Database, file_ctx: FileContext) -> int:
        """Load small file as single batch."""
        batch_name = f"SingleBatch_{file_ctx.file_path.name}"
        
        with self.audit_service.batch_context(file_ctx, batch_name) as batch_id:
            with self.audit_service.subbatch_context(batch_id, file_ctx.table_context.table_name, "CompleteFile") as subbatch_id:
                # Use existing UnifiedLoader
                from ....database.dml import UnifiedLoader, table_name_to_table_info
                
                table_info = table_name_to_table_info(file_ctx.table_context.table_name)
                loader = UnifiedLoader(database, self.config)
                
                success, error, rows = loader.load_file(
                    table_info, file_ctx.file_path,
                    batch_id=batch_id, subbatch_id=subbatch_id
                )
                
                if not success:
                    raise Exception(f"File loading failed: {error}")
                
                return rows
```

## Key Benefits Analysis

### 🎯 **1. Preserves All Existing Excellence**
- ✅ Keep `BatchAccumulator` pattern (proven and efficient)
- ✅ Keep existing context manager error handling
- ✅ Keep `BatchSizeOptimizer` integration
- ✅ Keep development filtering logic
- ✅ Keep strategy pattern architecture

### 🎯 **2. Natural Architecture Extension**
- ✅ Cascaded contexts feel natural for hierarchical data
- ✅ Each level has clear, single responsibility
- ✅ Parent-child relationships automatically maintained
- ✅ Resource cleanup guaranteed at each level

### 🎯 **3. Backward Compatibility**
- ✅ Existing `batch_context()` calls continue working
- ✅ Can migrate incrementally (table by table)
- ✅ No disruption to proven production code
- ✅ Legacy and enhanced patterns coexist

### 🎯 **4. Enhanced Observability**
- ✅ Complete processing trail from table → subbatch
- ✅ Proper metric aggregation up the hierarchy
- ✅ Structured logging with full context
- ✅ Error attribution at appropriate level

### 🎯 **5. Development Mode Integration**
- ✅ Table-level file filtering
- ✅ File-level size limits
- ✅ Batch-level row optimization
- ✅ Subbatch-level parallelization

## Implementation Roadmap

### **Phase 1: Foundation (1-2 days)**
1. Add context dataclasses (`TableContext`, `FileContext`)
2. Add context manager methods to `AuditService`
3. Enhance existing `batch_context` for dual usage
4. Unit tests for context managers

### **Phase 2: Integration (2-3 days)**
1. Add cascaded methods to `DataLoadingStrategy`
2. Update `DataLoadingService` to use cascaded contexts
3. Integration tests with real data files
4. Performance validation

### **Phase 3: Migration (1-2 weeks)**
1. Migrate high-volume tables to cascaded contexts
2. Monitor performance and error rates
3. Gradually retire legacy direct calls
4. Documentation and training

## Risk Mitigation

### **Low Risk Profile**
- ✅ **Additive Changes Only**: No existing code modification required
- ✅ **Proven Patterns**: Extending existing successful context managers
- ✅ **Gradual Migration**: Can test with single tables before full rollout
- ✅ **Easy Rollback**: Legacy patterns remain available

### **Contingency Plans**
- **Performance Issues**: Can disable cascaded contexts per table
- **Memory Problems**: Context objects are lightweight dataclasses
- **Complexity Issues**: Can revert to legacy patterns instantly
- **Integration Problems**: Each level can be tested independently

## Conclusion

**Cascaded context managers are the perfect architectural enhancement** for the CNPJ ETL project because:

1. **Natural Fit**: Extends existing excellent patterns rather than replacing them
2. **Risk-Free**: Additive changes with full backward compatibility
3. **Complete Solution**: Provides proper hierarchy without losing proven functionality
4. **Performance Friendly**: Leverages existing efficient metric accumulation
5. **Maintainable**: Clear separation of concerns at each hierarchy level

**Recommendation**: Proceed with cascaded context manager implementation as the optimal solution for achieving proper table → file → batch → subbatch hierarchy while preserving all existing architectural excellence.
