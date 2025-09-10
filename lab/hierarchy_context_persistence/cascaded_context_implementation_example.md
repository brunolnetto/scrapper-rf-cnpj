# Cascaded Context Managers - Implementation Example

## Complete Working Example

This document provides a concrete implementation example of how cascaded context managers would work with the existing CNPJ ETL architecture.

## Context Class Definitions

```python
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, List, Dict, Any
import time
import uuid

@dataclass
class TableContext:
    """Table-level processing context with metric aggregation."""
    table_id: str
    table_name: str
    audit_service: 'AuditService'
    start_time: float = field(default_factory=time.time)
    
    # Aggregated metrics from all files
    files_processed: int = 0
    files_failed: int = 0
    total_rows: int = 0
    total_bytes: int = 0
    processing_errors: List[str] = field(default_factory=list)
    
    # Development filtering context
    development_limits_applied: bool = False
    files_filtered_count: int = 0
    
    def add_file_result(self, success: bool, rows: int = 0, bytes_: int = 0, error: str = None):
        """Accumulate file processing results."""
        if success:
            self.files_processed += 1
        else:
            self.files_failed += 1
            if error:
                self.processing_errors.append(error)
        
        self.total_rows += rows
        self.total_bytes += bytes_
    
    def get_success_rate(self) -> float:
        """Calculate processing success rate."""
        total_files = self.files_processed + self.files_failed
        return (self.files_processed / total_files) if total_files > 0 else 0.0
    
    def get_processing_summary(self) -> Dict[str, Any]:
        """Get comprehensive processing summary."""
        duration = time.time() - self.start_time
        return {
            "table_name": self.table_name,
            "duration_seconds": round(duration, 2),
            "files_processed": self.files_processed,
            "files_failed": self.files_failed,
            "success_rate": round(self.get_success_rate() * 100, 2),
            "total_rows": self.total_rows,
            "total_bytes": self.total_bytes,
            "rows_per_second": round(self.total_rows / duration, 2) if duration > 0 else 0,
            "development_filtering": {
                "applied": self.development_limits_applied,
                "files_filtered": self.files_filtered_count
            },
            "errors": self.processing_errors
        }

@dataclass  
class FileContext:
    """File-level processing context with audit integration."""
    file_id: str
    file_path: Path
    audit_id: str
    table_context: TableContext
    audit_service: 'AuditService'
    start_time: float = field(default_factory=time.time)
    
    # File characteristics
    file_size_bytes: int = 0
    format_detected: str = ""
    encoding_used: str = "utf-8"
    
    # Processing metrics
    batches_created: int = 0
    subbatches_created: int = 0
    rows_processed: int = 0
    
    def __post_init__(self):
        """Initialize file characteristics."""
        if self.file_path.exists():
            self.file_size_bytes = self.file_path.stat().st_size
            
            # Detect file format using existing FileLoader pattern
            try:
                from ...services.loading.file_loader import FileLoader
                loader = FileLoader(str(self.file_path))
                self.format_detected = loader.format
                self.encoding_used = loader.encoding
            except Exception:
                # Fallback format detection
                if self.file_path.suffix.lower() == '.parquet':
                    self.format_detected = 'parquet'
                else:
                    self.format_detected = 'csv'
    
    def get_batch_name(self, batch_num: int, total_batches: int, 
                      row_start: int = None, row_end: int = None) -> str:
        """Generate consistent batch names for the file."""
        base_name = f"File_{self.file_path.name}_Batch_{batch_num}of{total_batches}"
        
        if row_start is not None and row_end is not None:
            base_name += f"_rows{row_start}-{row_end}"
        
        return base_name
    
    def needs_row_batching(self, row_batch_size: int = 10000) -> bool:
        """Determine if file needs row-driven batching based on size."""
        # For Parquet, we can get exact row count
        if self.format_detected == 'parquet':
            try:
                import polars as pl
                df = pl.scan_parquet(self.file_path)
                row_count = df.select(pl.count()).collect().item()
                return row_count > row_batch_size
            except Exception:
                pass
        
        # For CSV, use size heuristic (typical CNPJ data: ~8000 rows per MB)
        estimated_rows = (self.file_size_bytes / (1024 * 1024)) * 8000
        return estimated_rows > row_batch_size
    
    def get_estimated_batches(self, row_batch_size: int = 10000) -> int:
        """Estimate number of batches needed for the file."""
        if self.format_detected == 'parquet':
            try:
                import polars as pl
                df = pl.scan_parquet(self.file_path)
                row_count = df.select(pl.count()).collect().item()
                return max(1, math.ceil(row_count / row_batch_size))
            except Exception:
                pass
        
        # CSV size heuristic
        estimated_rows = (self.file_size_bytes / (1024 * 1024)) * 8000
        return max(1, math.ceil(estimated_rows / row_batch_size))
    
    def add_batch_created(self, subbatch_count: int = 1):
        """Track batch creation."""
        self.batches_created += 1
        self.subbatches_created += subbatch_count
    
    def get_processing_summary(self) -> Dict[str, Any]:
        """Get file processing summary."""
        duration = time.time() - self.start_time
        return {
            "file_name": self.file_path.name,
            "file_size_mb": round(self.file_size_bytes / (1024 * 1024), 2),
            "format": self.format_detected,
            "encoding": self.encoding_used,
            "duration_seconds": round(duration, 2),
            "batches_created": self.batches_created,
            "subbatches_created": self.subbatches_created,
            "rows_processed": self.rows_processed,
            "rows_per_second": round(self.rows_processed / duration, 2) if duration > 0 else 0,
            "audit_id": self.audit_id
        }
```

## Enhanced AuditService Implementation

```python
import math
from contextlib import contextmanager
from typing import Generator, Union

class AuditService:  # Keep ALL existing methods unchanged!
    
    @contextmanager
    def table_context(self, table_name: str, 
                     apply_development_filtering: bool = True) -> Generator[TableContext, None, None]:
        """
        Table-level processing context with development filtering integration.
        
        Args:
            table_name: Name of the table to process
            apply_development_filtering: Whether to apply development mode limits
            
        Yields:
            TableContext: Table processing context with metric aggregation
        """
        table_id = str(uuid.uuid4())
        
        table_ctx = TableContext(
            table_id=table_id,
            table_name=table_name,
            audit_service=self
        )
        
        try:
            logger.info(f"[TableContext] Starting table processing: {table_name}", 
                       extra={"table_id": table_id, "table_name": table_name})
            
            # Apply development filtering if enabled
            if apply_development_filtering and self.config.is_development_mode():
                table_ctx.development_limits_applied = True
                logger.info(f"[TableContext] Development mode active for table: {table_name}")
            
            yield table_ctx
            
            # Table processing completed successfully
            summary = table_ctx.get_processing_summary()
            logger.info(f"[TableContext] Table {table_name} completed successfully", 
                       extra={"table_summary": summary})
            
        except Exception as e:
            # Table processing failed
            summary = table_ctx.get_processing_summary()
            summary["error"] = str(e)
            logger.error(f"[TableContext] Table {table_name} failed: {e}", 
                        extra={"table_summary": summary})
            raise
    
    @contextmanager
    def file_context(self, table_ctx: TableContext, file_path: Path, 
                    audit_id: str) -> Generator[FileContext, None, None]:
        """
        File-level processing context with audit integration.
        
        Args:
            table_ctx: Parent table context
            file_path: Path to the file being processed
            audit_id: Audit record ID for referential integrity
            
        Yields:
            FileContext: File processing context
        """
        file_id = str(uuid.uuid4())
        
        file_ctx = FileContext(
            file_id=file_id,
            file_path=file_path,
            audit_id=audit_id,
            table_context=table_ctx,
            audit_service=self
        )
        
        try:
            logger.info(f"[FileContext] Starting file processing: {file_path.name}", 
                       extra={
                           "file_id": file_id,
                           "table_id": table_ctx.table_id,
                           "file_size_mb": round(file_ctx.file_size_bytes / (1024*1024), 2),
                           "format": file_ctx.format_detected,
                           "audit_id": audit_id
                       })
            
            yield file_ctx
            
            # File processing completed successfully
            summary = file_ctx.get_processing_summary()
            table_ctx.add_file_result(True, file_ctx.rows_processed, file_ctx.file_size_bytes)
            
            logger.info(f"[FileContext] File {file_path.name} completed successfully", 
                       extra={"file_summary": summary})
            
        except Exception as e:
            # File processing failed
            summary = file_ctx.get_processing_summary()
            summary["error"] = str(e)
            table_ctx.add_file_result(False, 0, 0, str(e))
            
            logger.error(f"[FileContext] File {file_path.name} failed: {e}", 
                        extra={"file_summary": summary})
            raise
    
    @contextmanager
    def batch_context(self, context_or_table: Union[FileContext, str], batch_name: str, 
                     year: Optional[int] = None, month: Optional[int] = None) -> Generator[uuid.UUID, None, None]:
        """
        Enhanced batch context - works with FileContext OR legacy table name.
        Maintains 100% backward compatibility while enabling cascaded usage.
        
        Args:
            context_or_table: FileContext (new cascaded) or table name string (legacy)
            batch_name: Human-readable batch description
            year: Optional year for temporal context
            month: Optional month for temporal context
            
        Yields:
            batch_id: UUID for the created batch
        """
        # Handle both FileContext (cascaded) and string table name (legacy)
        if isinstance(context_or_table, FileContext):
            target_table = context_or_table.table_context.table_name
            file_context = context_or_table
            
            # Track batch creation in file context
            file_context.add_batch_created()
            
        else:
            # Legacy usage with table name string
            target_table = context_or_table
            file_context = None
        
        # Build batch name with temporal context (existing logic)
        if self.config.batch_config.enable_temporal_context:
            year = year or self.config.year
            month = month or self.config.month
            batch_name_with_context = f"{batch_name} ({year}-{month:02d})"
        else:
            batch_name_with_context = batch_name
        
        # Use existing batch creation logic
        batch_id = self._start_batch(target_table, batch_name_with_context)
        
        try:
            extra_context = {
                "batch_id": str(batch_id), 
                "table_name": target_table,
                "year": year, 
                "month": month, 
                "batch_name": batch_name
            }
            
            if file_context:
                extra_context.update({
                    "file_path": str(file_context.file_path),
                    "file_id": file_context.file_id,
                    "audit_id": file_context.audit_id
                })
            
            logger.info(f"[BatchContext] Starting batch: {batch_name_with_context}", 
                       extra=extra_context)
            
            yield batch_id
            
            # Batch completed successfully
            self._complete_batch_with_accumulated_metrics(batch_id, BatchStatus.COMPLETED)
            logger.info(f"[BatchContext] Batch completed: {batch_name_with_context}", 
                       extra={"batch_id": str(batch_id)})
            
        except Exception as e:
            # Batch failed
            logger.error(f"[BatchContext] Batch failed: {batch_name_with_context}: {e}", 
                        extra={"batch_id": str(batch_id), "error": str(e)})
            self._complete_batch_with_accumulated_metrics(batch_id, BatchStatus.FAILED, str(e))
            raise
        finally:
            self._cleanup_batch_context(batch_id)
    
    # Keep existing subbatch_context method COMPLETELY UNCHANGED!
    # It already works perfectly with the batch_id from enhanced batch_context
```

## Enhanced DataLoadingStrategy Implementation

```python
import math
from typing import Tuple, List, Optional

class DataLoadingStrategy:  # Keep ALL existing methods unchanged!
    
    def load_table_with_cascade(self, database: Database, table_name: str, 
                               files_with_audits: List[Tuple[Path, str]], 
                               path_config: PathConfig) -> Tuple[bool, Optional[str], int]:
        """
        Enhanced table loading with cascaded context hierarchy.
        Falls back to existing methods if audit_service unavailable.
        
        Args:
            database: Database connection
            table_name: Name of the table to load
            files_with_audits: List of (file_path, audit_id) tuples
            path_config: Path configuration for file locations
            
        Returns:
            Tuple of (success, error_message, total_rows)
        """
        if not self.audit_service:
            # Fallback to existing load_table method
            logger.info(f"[LoadingStrategy] No audit service, using legacy loading for {table_name}")
            file_paths = [str(file_path) for file_path, _ in files_with_audits]
            return self.load_table(database, table_name, path_config, file_paths)
        
        logger.info(f"[LoadingStrategy] Starting cascaded loading for table {table_name} with {len(files_with_audits)} files")
        
        total_rows = 0
        failed_files = []
        
        # Table-level context with development filtering
        with self.audit_service.table_context(table_name) as table_ctx:
            
            # Apply development filtering at table level
            if table_ctx.development_limits_applied:
                files_with_audits = self._apply_development_filtering(table_name, files_with_audits, table_ctx)
            
            # Process each file with proper hierarchy
            for file_path, audit_id in files_with_audits:
                try:
                    with self.audit_service.file_context(table_ctx, file_path, audit_id) as file_ctx:
                        
                        # Determine processing strategy based on file characteristics
                        if file_ctx.needs_row_batching(self._get_row_batch_size()):
                            logger.info(f"[LoadingStrategy] Using row-driven batching for {file_path.name}")
                            rows = self._load_file_with_row_batching(database, file_ctx)
                        else:
                            logger.info(f"[LoadingStrategy] Using single batch for {file_path.name}")
                            rows = self._load_file_single_batch(database, file_ctx)
                        
                        file_ctx.rows_processed = rows
                        total_rows += rows
                        
                        logger.info(f"[LoadingStrategy] File {file_path.name} completed: {rows:,} rows")
                        
                except Exception as e:
                    error_msg = f"File {file_path.name}: {str(e)}"
                    failed_files.append(error_msg)
                    logger.error(f"[LoadingStrategy] {error_msg}")
        
        # Return results
        if failed_files:
            error_summary = f"Failed {len(failed_files)}/{len(files_with_audits)} files: {'; '.join(failed_files[:3])}"
            if len(failed_files) > 3:
                error_summary += f" (and {len(failed_files) - 3} more)"
            return False, error_summary, total_rows
        else:
            logger.info(f"[LoadingStrategy] Table {table_name} completed successfully: "
                       f"{len(files_with_audits)} files, {total_rows:,} rows")
            return True, None, total_rows
    
    def _load_file_with_row_batching(self, database: Database, file_ctx: FileContext) -> int:
        """
        Load large file with row-driven batching using cascaded contexts.
        Creates proper batch → subbatch hierarchy for memory management.
        """
        total_rows = 0
        row_batch_size = self._get_row_batch_size()
        subbatch_size = self._get_subbatch_size()
        
        estimated_batches = file_ctx.get_estimated_batches(row_batch_size)
        
        logger.info(f"[LoadingStrategy] Row-driven batching: {estimated_batches} estimated batches "
                   f"(batch_size={row_batch_size:,})")
        
        # Process file in row-driven batches
        for batch_num in range(estimated_batches):
            row_start = batch_num * row_batch_size
            row_end = row_start + row_batch_size
            
            batch_name = file_ctx.get_batch_name(batch_num + 1, estimated_batches, row_start, row_end)
            
            with self.audit_service.batch_context(file_ctx, batch_name) as batch_id:
                
                # Process subbatches within this batch for parallelization
                batch_rows = self._process_batch_subbatches(
                    database, file_ctx, batch_id, row_start, row_end, subbatch_size
                )
                
                total_rows += batch_rows
                logger.debug(f"[LoadingStrategy] Batch {batch_num + 1}/{estimated_batches} completed: {batch_rows:,} rows")
        
        return total_rows
    
    def _process_batch_subbatches(self, database: Database, file_ctx: FileContext, 
                                 batch_id: uuid.UUID, row_start: int, row_end: int, 
                                 subbatch_size: int) -> int:
        """Process a batch with subbatch parallelization."""
        batch_row_count = row_end - row_start
        subbatch_count = max(1, math.ceil(batch_row_count / subbatch_size))
        
        total_batch_rows = 0
        
        for subbatch_num in range(subbatch_count):
            subbatch_start = row_start + (subbatch_num * subbatch_size)
            subbatch_end = min(subbatch_start + subbatch_size, row_end)
            
            subbatch_description = f"SubBatch_{subbatch_num+1}of{subbatch_count}_rows{subbatch_start}-{subbatch_end}"
            
            with self.audit_service.subbatch_context(batch_id, file_ctx.table_context.table_name, subbatch_description) as subbatch_id:
                
                # Use existing UnifiedLoader with batch context
                subbatch_rows = self._load_subbatch_data(database, file_ctx, batch_id, subbatch_id)
                total_batch_rows += subbatch_rows
        
        # Track subbatch creation in file context
        file_ctx.subbatches_created += subbatch_count
        
        return total_batch_rows
    
    def _load_file_single_batch(self, database: Database, file_ctx: FileContext) -> int:
        """
        Load small file as single batch using cascaded contexts.
        Uses existing proven UnifiedLoader for actual data processing.
        """
        batch_name = f"SingleBatch_{file_ctx.file_path.name}"
        
        with self.audit_service.batch_context(file_ctx, batch_name) as batch_id:
            with self.audit_service.subbatch_context(batch_id, file_ctx.table_context.table_name, "CompleteFile") as subbatch_id:
                
                # Use existing UnifiedLoader (no changes needed)
                rows = self._load_subbatch_data(database, file_ctx, batch_id, subbatch_id)
                
                # Track subbatch creation
                file_ctx.subbatches_created += 1
                
                return rows
    
    def _load_subbatch_data(self, database: Database, file_ctx: FileContext, 
                           batch_id: uuid.UUID, subbatch_id: uuid.UUID) -> int:
        """
        Load data using existing UnifiedLoader with batch context.
        This method uses existing proven loading logic.
        """
        from ....database.dml import UnifiedLoader, table_name_to_table_info
        
        table_info = table_name_to_table_info(file_ctx.table_context.table_name)
        loader = UnifiedLoader(database, self.config)
        
        # Use existing loader with batch context (existing method)
        success, error, rows = loader.load_file(
            table_info, file_ctx.file_path,
            batch_id=batch_id, subbatch_id=subbatch_id
        )
        
        if not success:
            raise Exception(f"UnifiedLoader failed: {error}")
        
        return rows
    
    def _apply_development_filtering(self, table_name: str, files_with_audits: List[Tuple[Path, str]], 
                                   table_ctx: TableContext) -> List[Tuple[Path, str]]:
        """Apply development mode filtering using existing DevelopmentFilter."""
        from ....core.utils.development_filter import DevelopmentFilter
        
        dev_filter = DevelopmentFilter(self.config.etl)
        if not dev_filter.is_enabled:
            return files_with_audits
        
        # Extract file paths for filtering
        file_paths = [file_path for file_path, _ in files_with_audits]
        
        # Apply existing development filtering logic
        filtered_paths = dev_filter.filter_files_by_blob_limit(file_paths, table_name)
        filtered_paths = [path for path in filtered_paths if dev_filter.check_blob_size_limit(path)]
        
        # Reconstruct tuples with audit IDs
        path_to_audit = {file_path: audit_id for file_path, audit_id in files_with_audits}
        filtered_files_with_audits = [
            (path, path_to_audit[path]) for path in filtered_paths
            if path in path_to_audit
        ]
        
        # Track filtering in table context
        filtered_count = len(files_with_audits) - len(filtered_files_with_audits)
        table_ctx.files_filtered_count = filtered_count
        
        if filtered_count > 0:
            logger.info(f"[LoadingStrategy] Development filtering: {len(files_with_audits)} → {len(filtered_files_with_audits)} files for {table_name}")
        
        return filtered_files_with_audits
    
    def _get_row_batch_size(self) -> int:
        """Get row batch size from configuration."""
        return getattr(self.config.etl, 'row_batch_size', 10000)
    
    def _get_subbatch_size(self) -> int:
        """Get subbatch size from configuration."""
        return getattr(self.config.etl, 'row_subbatch_size', 1000)
    
    # Keep ALL existing methods unchanged!
    # load_table(), _load_csv_files(), load_multiple_tables(), etc.
```

## Usage Example

```python
# Example: Load empresa table with cascaded contexts
def load_empresa_table_example():
    # Initialize services (existing patterns)
    audit_service = AuditService(database, config)
    loading_strategy = DataLoadingStrategy(config, audit_service)
    
    # Prepare files with audit IDs (existing pattern)  
    files_with_audits = [
        (Path("data/empresa_01.csv"), "audit_123"),
        (Path("data/empresa_02.csv"), "audit_124"),
        (Path("data/empresa.parquet"), "audit_125")
    ]
    
    # Load with cascaded contexts (NEW enhanced method)
    success, error, total_rows = loading_strategy.load_table_with_cascade(
        database=database,
        table_name="empresa", 
        files_with_audits=files_with_audits,
        path_config=path_config
    )
    
    # Results provide complete hierarchy tracking
    if success:
        print(f"Successfully loaded {total_rows:,} rows from {len(files_with_audits)} files")
    else:
        print(f"Loading failed: {error}")

# Backward compatibility: Existing calls continue working unchanged
def existing_usage_still_works():
    loading_strategy = DataLoadingStrategy(config, audit_service)
    
    # This continues to work exactly as before
    success, error, rows = loading_strategy.load_table(
        database, "empresa", path_config, ["empresa_01.csv", "empresa_02.csv"]
    )
```

## Key Benefits Demonstrated

1. **Preserves All Existing Excellence**: Uses existing `UnifiedLoader`, `BatchAccumulator`, development filtering
2. **Natural Hierarchy**: Table → File → Batch → Subbatch flows naturally through cascaded contexts
3. **Complete Backward Compatibility**: Existing `load_table()` calls work unchanged
4. **Enhanced Observability**: Complete processing trail with metrics at each level
5. **Development Mode Integration**: Filtering applied appropriately at table and file levels
6. **Proper Resource Management**: Automatic cleanup and error handling at each hierarchy level

This implementation shows how cascaded context managers provide the perfect solution for achieving proper hierarchy while preserving all existing architectural excellence.
