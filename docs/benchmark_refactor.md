# Comprehensive Benchmark Refactoring Plan

## Executive Summary

This plan addresses critical performance issues, architectural flaws, and configuration mismatches in the CNPJ benchmarking suite. The refactor focuses on reliability, accuracy, and maintainability while preserving the core comparison between DuckDB and Polars.

---

## Phase 1: Critical Bug Fixes (Week 1)

### 1.1 Memory Management Overhaul

**Priority**: 🔴 CRITICAL

#### Issues:
- Polars memory calculations ignore decompression overhead
- DuckDB memory limits not enforced system-wide
- Chunked processing loads all chunks into memory
- No real-time memory monitoring

#### Solutions:

**File**: `benchmarks/utils.py`

```python
# New utility functions
def estimate_memory_requirements(
    input_size_gb: float,
    operation: str = "csv_to_parquet"
) -> Dict[str, float]:
    """
    Estimate memory requirements for different operations.
    
    CSV operations typically need:
    - Input data: 1x (compressed on disk)
    - Decompressed in memory: 2-3x
    - Working memory: 1-2x
    - Output buffer: 0.5-1x
    Total: 4.5-7x input size
    """
    multipliers = {
        "csv_to_parquet": 6.0,  # Conservative estimate
        "parquet_aggregation": 2.0,
        "parquet_to_parquet": 1.5
    }
    
    multiplier = multipliers.get(operation, 6.0)
    estimated_peak = input_size_gb * multiplier
    
    return {
        "estimated_peak_gb": estimated_peak,
        "multiplier": multiplier,
        "operation": operation
    }

def check_memory_safety(
    required_memory_gb: float,
    safety_margin: float = 0.3
) -> Dict[str, Any]:
    """
    Check if operation is safe to execute.
    
    Args:
        required_memory_gb: Estimated memory requirement
        safety_margin: Reserve this fraction of system memory (0.3 = 30%)
    
    Returns:
        Dict with safety status and recommendations
    """
    mem = psutil.virtual_memory()
    available_gb = mem.available / (1024**3)
    total_gb = mem.total / (1024**3)
    
    # Keep safety_margin of total memory free
    usable_gb = total_gb * (1 - safety_margin)
    current_used_gb = (total_gb - available_gb)
    can_allocate_gb = usable_gb - current_used_gb
    
    is_safe = required_memory_gb <= can_allocate_gb
    
    return {
        "is_safe": is_safe,
        "required_gb": required_memory_gb,
        "available_gb": available_gb,
        "can_allocate_gb": can_allocate_gb,
        "current_usage_percent": (current_used_gb / total_gb) * 100,
        "recommendation": _get_memory_recommendation(
            required_memory_gb, can_allocate_gb, available_gb
        )
    }

def _get_memory_recommendation(
    required: float, 
    can_allocate: float, 
    available: float
) -> str:
    """Generate memory safety recommendation."""
    if required <= can_allocate:
        return "SAFE: Proceed with operation"
    elif required <= available:
        return "WARNING: Close other applications first"
    elif required <= available * 1.5:
        return "RISKY: Use chunked processing"
    else:
        return "UNSAFE: File too large for available memory"
```

**File**: `benchmarks/polars_benchmark.py`

```python
def benchmark_csv_ingestion(self, ...):
    # ... existing code ...
    
    input_size_gb = sum(get_file_size_gb(f) for f in csv_files)
    
    # NEW: Proper memory safety check
    memory_req = estimate_memory_requirements(
        input_size_gb, 
        operation="csv_to_parquet"
    )
    
    safety_check = check_memory_safety(
        memory_req["estimated_peak_gb"],
        safety_margin=0.3
    )
    
    logger.info(f"Memory requirement: {memory_req['estimated_peak_gb']:.2f}GB")
    logger.info(f"Memory safety: {safety_check['recommendation']}")
    
    if not safety_check["is_safe"]:
        logger.warning(
            f"Insufficient memory. Required: {memory_req['estimated_peak_gb']:.2f}GB, "
            f"Available: {safety_check['can_allocate_gb']:.2f}GB"
        )
        return self._fallback_to_chunked_processing(...)
    
    # Proceed with normal processing...
```

**File**: `benchmarks/polars_benchmark.py` - Fix chunked processing

```python
def benchmark_chunked_processing(self, ...):
    """Fixed: Stream chunks instead of loading all into memory."""
    
    with benchmark_context(...) as monitor:
        try:
            # Process first file to initialize output
            first_file = csv_files[0]
            logger.info(f"Initializing with first file: {first_file.name}")
            
            batch_reader = pl.read_csv_batched(
                first_file,
                separator=delimiter,
                encoding=encoding,
                batch_size=self.chunk_size,
                ignore_errors=True,
                has_header=False
            )
            
            # Write first batch to initialize schema
            first_batch = batch_reader.next_batches(1)[0]
            output_path.parent.mkdir(parents=True, exist_ok=True)
            first_batch.write_parquet(
                output_path, 
                compression="zstd",
                statistics=True
            )
            total_rows = len(first_batch)
            del first_batch
            
            # Stream remaining batches - APPEND mode
            while True:
                batches = batch_reader.next_batches(1)
                if not batches:
                    break
                    
                for batch in batches:
                    monitor.update_peak_memory()
                    
                    # FIXED: Append instead of accumulating
                    # Note: Polars doesn't support append mode directly
                    # Workaround: Use PyArrow
                    import pyarrow.parquet as pq
                    import pyarrow as pa
                    
                    # Convert to Arrow and append
                    arrow_table = batch.to_arrow()
                    
                    # Append to existing file
                    with pq.ParquetWriter(
                        output_path, 
                        arrow_table.schema,
                        compression='zstd'
                    ) as writer:
                        writer.write_table(arrow_table)
                    
                    total_rows += len(batch)
                    del batch, arrow_table
                    cleanup_memory()
            
            # Process remaining files
            for csv_file in csv_files[1:]:
                # Same streaming append logic...
                pass
            
            # Final stats
            output_size_gb = get_file_size_gb(output_path)
            compression_ratio = (input_size_gb / output_size_gb 
                               if output_size_gb > 0.01 else None)
            
            stats = monitor.finish()
            
            return BenchmarkResult(...)
```

### 1.2 Configuration Standardization

**Priority**: 🔴 CRITICAL

**File**: `benchmarks/config.py` (NEW)

```python
"""Centralized configuration for benchmark suite."""

from dataclasses import dataclass, field
from typing import Optional, Dict, Any
import os
import psutil

@dataclass
class BenchmarkConfig:
    """Unified configuration for all benchmarks."""
    
    # Memory settings
    memory_limit_gb: float = 8.0
    memory_safety_margin: float = 0.3  # Keep 30% system memory free
    polars_memory_multiplier: float = 6.0  # Conservative estimate
    
    # CPU/Threading
    max_threads: int = 8
    duckdb_threads: Optional[int] = None  # None = use max_threads
    polars_threads: Optional[int] = None  # None = use max_threads
    
    # CSV Format (Brazilian CNPJ standard)
    csv_delimiter: str = ";"
    csv_encoding: str = "ISO-8859-1"  # Brazilian standard
    csv_has_header: bool = False
    csv_ignore_errors: bool = True
    
    # Processing strategy
    chunk_size: int = 50_000
    schema_inference_rows: int = 10_000
    streaming_enabled: bool = True
    
    # Compression
    compression: str = "zstd"
    compression_level: int = 3
    
    # Parquet settings
    parquet_row_group_size: int = 100_000
    parquet_statistics: bool = True
    
    # Output settings
    output_dir: str = "benchmark_output"
    save_intermediate_files: bool = True
    verbose: bool = True
    
    # Metadata
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        """Validate and adjust configuration."""
        # Auto-detect optimal thread count if not specified
        if self.max_threads <= 0:
            self.max_threads = max(1, psutil.cpu_count(logical=False))
        
        if self.duckdb_threads is None:
            self.duckdb_threads = self.max_threads
        
        if self.polars_threads is None:
            self.polars_threads = self.max_threads
        
        # Validate memory settings
        total_memory_gb = psutil.virtual_memory().total / (1024**3)
        if self.memory_limit_gb > total_memory_gb * 0.9:
            logger.warning(
                f"Memory limit {self.memory_limit_gb}GB exceeds "
                f"90% of system memory ({total_memory_gb:.1f}GB). "
                f"Adjusting to {total_memory_gb * 0.7:.1f}GB"
            )
            self.memory_limit_gb = total_memory_gb * 0.7
        
        # Set environment variables for Polars
        os.environ['POLARS_MAX_THREADS'] = str(self.polars_threads)
        
    @classmethod
    def from_args(cls, args) -> 'BenchmarkConfig':
        """Create config from command-line arguments."""
        memory_limit = float(args.memory_limit.replace('GB', ''))
        
        return cls(
            memory_limit_gb=memory_limit,
            max_threads=args.max_threads if hasattr(args, 'max_threads') else 8,
            output_dir=str(args.output_dir),
            verbose=not args.quiet if hasattr(args, 'quiet') else True,
            streaming_enabled=not args.disable_streaming if hasattr(args, 'disable_streaming') else True,
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            'memory_limit_gb': self.memory_limit_gb,
            'max_threads': self.max_threads,
            'csv_delimiter': self.csv_delimiter,
            'csv_encoding': self.csv_encoding,
            'compression': self.compression,
            'streaming_enabled': self.streaming_enabled,
            'chunk_size': self.chunk_size,
        }
```

### 1.3 Fix Schema Inference Bug

**Priority**: 🟡 HIGH

**File**: `benchmarks/polars_benchmark.py`

```python
# BEFORE (WRONG):
infer_schema_length=min(1000, 100)  # Always returns 100!

# AFTER (FIXED):
infer_schema_length=self.config.schema_inference_rows
```

---

## Phase 2: Architectural Improvements (Week 2)

### 2.1 Separate Benchmark Concerns

**Priority**: 🟡 HIGH

Create clear separation between:
1. **Ingestion benchmarks** (CSV → Parquet)
2. **Query benchmarks** (Aggregations, filters)
3. **Integration benchmarks** (Tool → PostgreSQL)

**New Structure**:

```
benchmarks/
├── __init__.py
├── config.py                    # NEW: Central configuration
├── utils.py                     # Enhanced utilities
├── base.py                      # NEW: Base benchmark class
├── ingestion/                   # NEW: Ingestion-specific
│   ├── __init__.py
│   ├── duckdb_ingestion.py
│   ├── polars_ingestion.py
│   └── comparison.py
├── query/                       # NEW: Query-specific
│   ├── __init__.py
│   ├── duckdb_query.py
│   ├── polars_query.py
│   └── postgres_query.py        # Separated from DuckDB
├── integration/                 # NEW: Integration tests
│   ├── __init__.py
│   ├── duckdb_to_postgres.py
│   └── parquet_to_postgres.py
├── analysis/                    # Renamed from analyze_results.py
│   ├── __init__.py
│   ├── analyzer.py
│   ├── visualizer.py
│   └── reporter.py
└── runners/                     # NEW: Orchestration
    ├── __init__.py
    ├── quick_runner.py          # Quick tests
    ├── full_runner.py           # Comprehensive tests
    └── custom_runner.py         # User-defined tests
```

**File**: `benchmarks/base.py` (NEW)

```python
"""Base benchmark class with common functionality."""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import List, Optional, Dict, Any
from .config import BenchmarkConfig
from .utils import BenchmarkResult, benchmark_context, cleanup_memory

class BaseBenchmark(ABC):
    """Abstract base class for all benchmarks."""
    
    def __init__(self, config: BenchmarkConfig):
        self.config = config
        self.name = self.__class__.__name__
    
    @abstractmethod
    def setup(self) -> None:
        """Setup benchmark resources."""
        pass
    
    @abstractmethod
    def teardown(self) -> None:
        """Cleanup benchmark resources."""
        pass
    
    @abstractmethod
    def run(self, **kwargs) -> BenchmarkResult:
        """Execute the benchmark."""
        pass
    
    def validate_inputs(self, csv_files: List[Path]) -> None:
        """Validate input files exist and are readable."""
        for f in csv_files:
            if not f.exists():
                raise FileNotFoundError(f"File not found: {f}")
            if not f.is_file():
                raise ValueError(f"Not a file: {f}")
            if f.stat().st_size == 0:
                raise ValueError(f"Empty file: {f}")
    
    def estimate_duration(self, input_size_gb: float) -> float:
        """Estimate benchmark duration (to be overridden)."""
        # Rule of thumb: 100MB/s throughput
        return input_size_gb * 10  # seconds
    
    def __enter__(self):
        """Context manager support."""
        self.setup()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager cleanup."""
        self.teardown()
        cleanup_memory()


class IngestionBenchmark(BaseBenchmark):
    """Base class for CSV ingestion benchmarks."""
    
    @abstractmethod
    def ingest_csv(
        self, 
        csv_files: List[Path], 
        output_path: Path
    ) -> BenchmarkResult:
        """Ingest CSV files to Parquet."""
        pass


class QueryBenchmark(BaseBenchmark):
    """Base class for query benchmarks."""
    
    @abstractmethod
    def execute_query(
        self, 
        query: str, 
        data_source: Path
    ) -> BenchmarkResult:
        """Execute a query against data source."""
        pass
```

### 2.2 Error Handling & Recovery

**Priority**: 🟡 HIGH

**File**: `benchmarks/utils.py`

```python
from enum import Enum
from typing import Optional, Callable

class BenchmarkStatus(Enum):
    """Benchmark execution status."""
    SUCCESS = "success"
    PARTIAL_SUCCESS = "partial_success"
    FAILED = "failed"
    SKIPPED = "skipped"
    TIMEOUT = "timeout"
    OUT_OF_MEMORY = "out_of_memory"

@dataclass
class BenchmarkResult:
    """Enhanced with status and recovery info."""
    name: str
    status: BenchmarkStatus
    duration_seconds: float
    # ... existing fields ...
    
    # NEW fields
    partial_results: Optional[Dict[str, Any]] = None
    recovery_attempted: bool = False
    recovery_strategy: Optional[str] = None
    warnings: List[str] = field(default_factory=list)
    
    def is_success(self) -> bool:
        """Check if benchmark succeeded."""
        return self.status == BenchmarkStatus.SUCCESS
    
    def is_usable(self) -> bool:
        """Check if results are usable (success or partial)."""
        return self.status in [
            BenchmarkStatus.SUCCESS, 
            BenchmarkStatus.PARTIAL_SUCCESS
        ]


class BenchmarkRecovery:
    """Handle benchmark failures with recovery strategies."""
    
    @staticmethod
    def with_retry(
        func: Callable,
        max_retries: int = 3,
        backoff_seconds: int = 5
    ) -> BenchmarkResult:
        """Retry failed benchmarks with exponential backoff."""
        for attempt in range(max_retries):
            try:
                return func()
            except MemoryError:
                logger.error(f"Memory error on attempt {attempt + 1}")
                cleanup_memory()
                if attempt < max_retries - 1:
                    time.sleep(backoff_seconds * (2 ** attempt))
                else:
                    raise
            except Exception as e:
                logger.error(f"Error on attempt {attempt + 1}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(backoff_seconds)
                else:
                    raise
    
    @staticmethod
    def with_fallback(
        primary_func: Callable,
        fallback_func: Callable,
        condition: Optional[Callable] = None
    ) -> BenchmarkResult:
        """Try primary strategy, fallback on failure."""
        try:
            result = primary_func()
            if condition and not condition(result):
                logger.warning("Primary succeeded but condition failed, using fallback")
                return fallback_func()
            return result
        except Exception as e:
            logger.warning(f"Primary failed: {e}. Using fallback strategy.")
            return fallback_func()
```

**File**: `benchmarks/ingestion/duckdb_ingestion.py`

```python
def benchmark_csv_ingestion(self, csv_files: List[Path], ...):
    """Enhanced with per-file error handling."""
    
    with benchmark_context(...) as monitor:
        successful_files = []
        failed_files = []
        
        try:
            # Process files one by one with error recovery
            for i, csv_file in enumerate(csv_files):
                try:
                    logger.info(f"Processing file {i+1}/{len(csv_files)}: {csv_file.name}")
                    
                    if i == 0:
                        # Create table
                        self.connection.execute(f"CREATE TABLE {table_name} AS ...")
                    else:
                        # Insert
                        self.connection.execute(f"INSERT INTO {table_name} ...")
                    
                    successful_files.append(csv_file)
                    monitor.update_peak_memory()
                    
                except Exception as e:
                    logger.error(f"Failed to process {csv_file.name}: {e}")
                    failed_files.append((csv_file, str(e)))
                    
                    # Decide: continue or abort?
                    if len(failed_files) > len(csv_files) * 0.3:  # >30% failure
                        logger.error("Too many failures, aborting benchmark")
                        raise
            
            # Determine status
            if not failed_files:
                status = BenchmarkStatus.SUCCESS
            elif successful_files:
                status = BenchmarkStatus.PARTIAL_SUCCESS
            else:
                status = BenchmarkStatus.FAILED
            
            stats = monitor.finish()
            
            return BenchmarkResult(
                name=f"DuckDB_CSV_Ingestion_{table_name}",
                status=status,
                # ... other fields ...
                warnings=[f"Failed files: {[f.name for f, _ in failed_files]}"] if failed_files else [],
                metadata={
                    'successful_files': len(successful_files),
                    'failed_files': len(failed_files),
                    'failure_details': failed_files
                }
            )
            
        except Exception as e:
            # Critical failure
            stats = monitor.finish()
            return BenchmarkResult(
                name=f"DuckDB_CSV_Ingestion_{table_name}_CRITICAL_FAILURE",
                status=BenchmarkStatus.FAILED,
                errors=str(e),
                # ... other fields ...
            )
```

---

## Phase 3: Feature Enhancements (Week 3)

### 3.1 Real-Time Monitoring

**Priority**: 🟢 MEDIUM

**File**: `benchmarks/monitoring.py` (NEW)

```python
"""Real-time monitoring with alerts and dashboards."""

import threading
import time
from typing import Callable, Optional, List
from dataclasses import dataclass
import logging

@dataclass
class MemoryAlert:
    """Memory usage alert."""
    timestamp: float
    current_gb: float
    threshold_gb: float
    message: str
    severity: str  # "warning" | "critical"

class RealTimeMonitor:
    """Monitor benchmarks with real-time alerts."""
    
    def __init__(
        self,
        warning_threshold: float = 0.7,  # 70% of limit
        critical_threshold: float = 0.9,  # 90% of limit
        check_interval: float = 1.0  # Check every second
    ):
        self.warning_threshold = warning_threshold
        self.critical_threshold = critical_threshold
        self.check_interval = check_interval
        self.alerts: List[MemoryAlert] = []
        self.is_monitoring = False
        self.monitor_thread: Optional[threading.Thread] = None
        self.on_warning: Optional[Callable] = None
        self.on_critical: Optional[Callable] = None
    
    def start(self, memory_limit_gb: float):
        """Start monitoring."""
        self.memory_limit_gb = memory_limit_gb
        self.is_monitoring = True
        self.monitor_thread = threading.Thread(
            target=self._monitor_loop,
            daemon=True
        )
        self.monitor_thread.start()
    
    def stop(self):
        """Stop monitoring."""
        self.is_monitoring = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=5.0)
    
    def _monitor_loop(self):
        """Background monitoring loop."""
        process = psutil.Process()
        
        while self.is_monitoring:
            current_gb = process.memory_info().rss / (1024**3)
            usage_ratio = current_gb / self.memory_limit_gb
            
            if usage_ratio >= self.critical_threshold:
                alert = MemoryAlert(
                    timestamp=time.time(),
                    current_gb=current_gb,
                    threshold_gb=self.memory_limit_gb * self.critical_threshold,
                    message=f"CRITICAL: Memory at {usage_ratio*100:.1f}% of limit!",
                    severity="critical"
                )
                self.alerts.append(alert)
                logging.critical(alert.message)
                
                if self.on_critical:
                    self.on_critical(alert)
                    
            elif usage_ratio >= self.warning_threshold:
                alert = MemoryAlert(
                    timestamp=time.time(),
                    current_gb=current_gb,
                    threshold_gb=self.memory_limit_gb * self.warning_threshold,
                    message=f"WARNING: Memory at {usage_ratio*100:.1f}% of limit",
                    severity="warning"
                )
                self.alerts.append(alert)
                logging.warning(alert.message)
                
                if self.on_warning:
                    self.on_warning(alert)
            
            time.sleep(self.check_interval)
    
    def get_alert_summary(self) -> Dict[str, int]:
        """Get summary of alerts."""
        return {
            'total': len(self.alerts),
            'warnings': len([a for a in self.alerts if a.severity == "warning"]),
            'critical': len([a for a in self.alerts if a.severity == "critical"])
        }

# Usage in benchmarks:
def benchmark_with_monitoring(self, ...):
    monitor = RealTimeMonitor(warning_threshold=0.7, critical_threshold=0.9)
    
    def handle_critical_memory(alert: MemoryAlert):
        logger.critical("Stopping benchmark due to memory pressure!")
        # Could trigger graceful shutdown or fallback strategy
        self.trigger_emergency_cleanup()
    
    monitor.on_critical = handle_critical_memory
    monitor.start(memory_limit_gb=self.config.memory_limit_gb)
    
    try:
        # Run benchmark...
        result = self._execute_benchmark(...)
        result.metadata['memory_alerts'] = monitor.get_alert_summary()
        return result
    finally:
        monitor.stop()
```

### 3.2 Parallel Benchmark Execution

**Priority**: 🟢 MEDIUM

**File**: `benchmarks/runners/parallel_runner.py` (NEW)

```python
"""Execute benchmarks in parallel with resource management."""

import concurrent.futures
from typing import List, Dict, Any
import logging
from pathlib import Path

class ParallelBenchmarkRunner:
    """Run multiple benchmarks in parallel with resource limits."""
    
    def __init__(self, config: BenchmarkConfig, max_parallel: int = 2):
        self.config = config
        self.max_parallel = max_parallel
        self.logger = logging.getLogger(__name__)
    
    def run_benchmarks(
        self,
        benchmarks: List[BaseBenchmark],
        csv_files: List[Path]
    ) -> Dict[str, BenchmarkResult]:
        """
        Run benchmarks in parallel with resource management.
        
        Note: Only runs benchmarks that don't compete for the same resources.
        E.g., DuckDB and Polars can run in parallel if total memory allows.
        """
        results = {}
        
        # Group benchmarks by resource requirements
        groups = self._group_by_resources(benchmarks)
        
        for group_name, group_benchmarks in groups.items():
            self.logger.info(f"Running benchmark group: {group_name}")
            
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=self.max_parallel
            ) as executor:
                futures = {
                    executor.submit(
                        self._run_single_benchmark,
                        benchmark,
                        csv_files
                    ): benchmark.name
                    for benchmark in group_benchmarks
                }
                
                for future in concurrent.futures.as_completed(futures):
                    benchmark_name = futures[future]
                    try:
                        result = future.result()
                        results[benchmark_name] = result
                        self.logger.info(f"Completed: {benchmark_name}")
                    except Exception as e:
                        self.logger.error(f"Failed: {benchmark_name} - {e}")
                        results[benchmark_name] = self._create_error_result(
                            benchmark_name, 
                            str(e)
                        )
        
        return results
    
    def _group_by_resources(
        self, 
        benchmarks: List[BaseBenchmark]
    ) -> Dict[str, List[BaseBenchmark]]:
        """Group benchmarks that can run in parallel."""
        # Simple strategy: memory-intensive vs compute-intensive
        memory_intensive = []
        compute_intensive = []
        
        for benchmark in benchmarks:
            if "Ingestion" in benchmark.name:
                memory_intensive.append(benchmark)
            else:
                compute_intensive.append(benchmark)
        
        return {
            "memory_intensive": memory_intensive,
            "compute_intensive": compute_intensive
        }
    
    def _run_single_benchmark(
        self,
        benchmark: BaseBenchmark,
        csv_files: List[Path]
    ) -> BenchmarkResult:
        """Run a single benchmark with error handling."""
        with benchmark:
            return benchmark.run(csv_files=csv_files)
```

### 3.3 Enhanced Reporting

**Priority**: 🟢 MEDIUM

**File**: `benchmarks/analysis/reporter.py`

```python
"""Enhanced reporting with multiple output formats."""

from typing import List, Dict, Any
from pathlib import Path
import json
from datetime import datetime
from .utils import BenchmarkResult

class BenchmarkReporter:
    """Generate comprehensive benchmark reports."""
    
    def __init__(self, results: List[BenchmarkResult]):
        self.results = results
        self.timestamp = datetime.now()
    
    def generate_all(self, output_dir: Path):
        """Generate all report formats."""
        output_dir.mkdir(parents=True, exist_ok=True)
        
        self.generate_markdown(output_dir / "report.md")
        self.generate_json(output_dir / "report.json")
        self.generate_html(output_dir / "report.html")
        self.generate_csv(output_dir / "results.csv")
        self.generate_executive_summary(output_dir / "summary.txt")
    
    def generate_executive_summary(self, output_path: Path):
        """Generate executive summary for quick review."""
        lines = []
        lines.append("=" * 80)
        lines.append("BENCHMARK EXECUTIVE SUMMARY")
        lines.append("=" * 80)
        lines.append(f"Generated: {self.timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append(f"Total Benchmarks: {len(self.results)}")
        lines.append("")
        
        # Success rate
        successful = [r for r in self.results if r.is_success()]
        success_rate = (len(successful) / len(self.results)) * 100
        lines.append(f"Success Rate: {success_rate:.1f}%")
        lines.append("")
        
        # Key findings
        lines.append("KEY FINDINGS:")
        lines.append("")
        
        # Fastest tool
        if successful:
            fastest = min(successful, key=lambda x: x.duration_seconds)
            lines.append(f"🏆 FASTEST: {fastest.name}")
            lines.append(f"   Duration: {fastest.duration_seconds:.2f}s")
            lines.append("")
            
            # Most memory efficient
            most_efficient = min(successful, key=lambda x: x.peak_memory_gb)
            lines.append(f"💾 MOST MEMORY EFFICIENT: {most_efficient.name}")
            lines.append(f"   Peak Memory: {most_efficient.peak_memory_gb:.2f}GB")
            lines.append("")
            
            # Best compression
            compression_results = [r for r in successful if r.compression_ratio]
            if compression_results:
                best_compression = max(compression_results, key=lambda x: x.compression_ratio)
                lines.append(f"📦 BEST COMPRESSION: {best_compression.name}")
                lines.append(f"   Ratio: {best_compression.compression_ratio:.2f}x")
                lines.append("")
        
        # Recommendations
        lines.append("RECOMMENDATIONS:")
        lines.append("")
        lines.append(self._generate_recommendations())
        
        output_path.write_text('\n'.join(lines))
    
    def _generate_recommendations(self) -> str:
        """Generate recommendations based on results."""
        recommendations = []
        
        duckdb_results = [r for r in self.results if 'DuckDB' in r.name and r.is_usable()]
        polars_results = [r for r in self.results if 'Polars' in r.name and r.is_usable()]
        
        if duckdb_results and polars_results:
            duckdb_avg_time = sum(r.duration_seconds for r in duckdb_results) / len(duckdb_results)
            polars_avg_time = sum(r.duration_seconds for r in polars_results) / len(polars_results)
            
            duckdb_avg_mem = sum(r.peak_memory_gb for r in duckdb_results) / len(duckdb_results)
            polars_avg_mem = sum(r.peak_memory_gb for r in polars_results) / len(polars_results)
            
            if duckdb_avg_time < polars_avg_time * 0.8:
                recommendations.append("✓ Use DuckDB for: Simple CSV → Parquet conversion")
                recommendations.append("  Reason: 20%+ faster on average")
            
            if polars_avg_mem < duckdb_avg_mem * 0.8:
                recommendations.append("✓ Use Polars for: Memory-constrained environments")
                recommendations.append("  Reason: 20%+ lower memory usage")
            
            if not recommendations:
                recommendations.append("✓ Both tools perform similarly")
                recommendations.append("  Choose based on your existing stack:")
                recommendations.append("  - DuckDB: Better SQL integration")
                recommendations.append("  - Polars: Better Python DataFrame API")
        
        return '\n'.join(recommendations) if recommendations else "Insufficient data for recommendations"
    
    def generate_comparison_chart_data(self) -> Dict[str, Any]:
        """Generate data for visualization tools."""
        return {
            'timestamp': self.timestamp.isoformat(),
            'benchmarks': [
                {
                    'name': r.name,
                    'tool': 'DuckDB' if 'DuckDB' in r.name else 'Polars',
                    'operation': self._extract_operation(r.name),
                    'duration_seconds': r.duration_seconds,
                    'peak_memory_gb': r.peak_memory_gb,
                    'compression_ratio': r.compression_ratio,
                    'status': r.status.value if hasattr(r, 'status') else 'unknown',
                    'throughput_mb_per_sec': self._calculate_throughput(r)
                }
                for r in self.results
            ],
            'summary': self._generate_summary_stats()
        }
    
    def _extract_operation(self, name: str) -> str:
        """Extract operation type from benchmark name."""
        if 'Ingestion' in name:
            return 'ingestion'
        elif 'Aggregation' in name:
            return 'aggregation'
        elif 'Conversion' in name:
            return 'conversion'
        else:
            return 'other'
    
    def _calculate_throughput(self, result: BenchmarkResult) -> Optional[float]:
        """Calculate throughput in MB/s."""
        if hasattr(result, 'metadata') and result.metadata:
            input_size_gb = result.metadata.get('input_size_gb', 0)
            if input_size_gb > 0 and result.duration_seconds > 0:
                return (input_size_gb * 1024) / result.duration_seconds
        return None
    
    def _generate_summary_stats(self) -> Dict[str, Any]:
        """Generate summary statistics."""
        successful = [r for r in self.results if r.is_usable()]
        
        if not successful:
            return {'error': 'No successful benchmarks'}
        
        return {
            'total_benchmarks': len(self.results),
            'successful': len(successful),
            'avg_duration': sum(r.duration_seconds for r in successful) / len(successful),
            'avg_memory': sum(r.peak_memory_gb for r in successful) / len(successful),
            'total_data_processed_gb': sum(
                r.metadata.get('input_size_gb', 0) 
                for r in successful 
                if hasattr(r, 'metadata') and r.metadata
            )
        }
```

---

## Phase 4: Testing & Validation (Week 4)

### 4.1 Unit Tests

**Priority**: 🟡 HIGH

**File**: `tests/test_memory_utils.py` (NEW)

```python
"""Unit tests for memory utilities."""

import pytest
import psutil
from benchmarks.utils import (
    estimate_memory_requirements,
    check_memory_safety,
    cleanup_memory
)

class TestMemoryEstimation:
    """Test memory estimation functions."""
    
    def test_csv_to_parquet_estimation(self):
        """Test memory estimation for CSV → Parquet."""
        result = estimate_memory_requirements(
            input_size_gb=1.0,
            operation="csv_to_parquet"
        )
        
        assert result['estimated_peak_gb'] > 1.0
        assert result['estimated_peak_gb'] < 10.0
        assert result['multiplier'] == 6.0
    
    def test_safety_check_with_sufficient_memory(self):
        """Test safety check with sufficient memory."""
        # Request very small amount
        result = check_memory_safety(
            required_memory_gb=0.1,
            safety_margin=0.3
        )
        
        assert result['is_safe'] is True
        assert result['required_gb'] == 0.1
        assert 'SAFE' in result['recommendation']
    
    def test_safety_check_with_insufficient_memory(self):
        """Test safety check with insufficient memory."""
        total_mem_gb = psutil.virtual_memory().total / (1024**3)
        
        # Request more than system has
        result = check_memory_safety(
            required_memory_gb=total_mem_gb * 2,
            safety_margin=0.3
        )
        
        assert result['is_safe'] is False
        assert 'UNSAFE' in result['recommendation'] or 'RISKY' in result['recommendation']

class TestMemoryCleanup:
    """Test memory cleanup functions."""
    
    def test_cleanup_reduces_memory(self):
        """Test that cleanup actually reduces memory usage."""
        import gc
        
        # Create some garbage
        large_list = [i for i in range(1_000_000)]
        initial_memory = psutil.Process().memory_info().rss
        
        # Delete and cleanup
        del large_list
        cleanup_memory()
        
        final_memory = psutil.Process().memory_info().rss
        
        # Memory should not increase (may not decrease due to OS behavior)
        assert final_memory <= initial_memory * 1.1  # Allow 10% variance
```

**File**: `tests/test_config.py` (NEW)

```python
"""Unit tests for configuration."""

import pytest
from benchmarks.config import BenchmarkConfig

class TestBenchmarkConfig:
    """Test benchmark configuration."""
    
    def test_default_config(self):
        """Test default configuration values."""
        config = BenchmarkConfig()
        
        assert config.memory_limit_gb > 0
        assert config.max_threads > 0
        assert config.csv_delimiter == ";"
        assert config.csv_encoding == "ISO-8859-1"
    
    def test_config_validation(self):
        """Test configuration validation."""
        import psutil
        total_mem_gb = psutil.virtual_memory().total / (1024**3)
        
        # Try to set memory limit too high
        config = BenchmarkConfig(memory_limit_gb=total_mem_gb * 2)
        
        # Should be automatically adjusted
        assert config.memory_limit_gb < total_mem_gb
    
    def test_thread_count_auto_detection(self):
        """Test automatic thread count detection."""
        config = BenchmarkConfig(max_threads=0)
        
        # Should auto-detect
        assert config.max_threads > 0
        assert config.max_threads <= psutil.cpu_count(logical=True)
```

### 4.2 Integration Tests

**Priority**: 🟡 HIGH

**File**: `tests/test_integration.py` (NEW)

```python
"""Integration tests for complete benchmark flows."""

import pytest
from pathlib import Path
import tempfile
from benchmarks.config import BenchmarkConfig
from benchmarks.ingestion.duckdb_ingestion import DuckDBIngestionBenchmark
from benchmarks.ingestion.polars_ingestion import PolarsIngestionBenchmark

@pytest.fixture
def sample_csv_file():
    """Create a sample CSV file for testing."""
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
        # Write Brazilian CNPJ-like data
        for i in range(1000):
            f.write(f"{i};Company {i};12345678;Active\n")
        return Path(f.name)

@pytest.fixture
def test_config():
    """Create test configuration."""
    return BenchmarkConfig(
        memory_limit_gb=2.0,
        max_threads=2,
        chunk_size=100,
        verbose=False
    )

class TestDuckDBIntegration:
    """Integration tests for DuckDB benchmarks."""
    
    def test_complete_ingestion_flow(self, sample_csv_file, test_config):
        """Test complete CSV → Parquet flow."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "output.parquet"
            
            benchmark = DuckDBIngestionBenchmark(test_config)
            
            with benchmark:
                result = benchmark.ingest_csv(
                    csv_files=[sample_csv_file],
                    output_path=output_path
                )
            
            assert result.is_success()
            assert output_path.exists()
            assert result.rows_processed == 1000
            assert result.duration_seconds > 0
            assert result.peak_memory_gb > 0

class TestPolarsIntegration:
    """Integration tests for Polars benchmarks."""
    
    def test_streaming_ingestion(self, sample_csv_file, test_config):
        """Test streaming ingestion."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "output.parquet"
            
            benchmark = PolarsIngestionBenchmark(test_config)
            
            with benchmark:
                result = benchmark.ingest_csv(
                    csv_files=[sample_csv_file],
                    output_path=output_path
                )
            
            assert result.is_success()
            assert output_path.exists()
    
    def test_chunked_fallback(self, sample_csv_file, test_config):
        """Test chunked processing fallback."""
        # Force chunked processing with very low memory limit
        low_mem_config = BenchmarkConfig(
            memory_limit_gb=0.1,  # Very low
            chunk_size=50
        )
        
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "output.parquet"
            
            benchmark = PolarsIngestionBenchmark(low_mem_config)
            
            with benchmark:
                result = benchmark.ingest_csv(
                    csv_files=[sample_csv_file],
                    output_path=output_path
                )
            
            # Should succeed with chunked processing
            assert result.is_usable()
            assert 'chunked' in result.name.lower() or result.recovery_attempted

class TestComparison:
    """Test comparing DuckDB vs Polars."""
    
    def test_side_by_side_comparison(self, sample_csv_file, test_config):
        """Test side-by-side comparison of both tools."""
        results = {}
        
        with tempfile.TemporaryDirectory() as tmpdir:
            # DuckDB
            duckdb_output = Path(tmpdir) / "duckdb.parquet"
            duckdb_bench = DuckDBIngestionBenchmark(test_config)
            with duckdb_bench:
                results['duckdb'] = duckdb_bench.ingest_csv(
                    [sample_csv_file], 
                    duckdb_output
                )
            
            # Polars
            polars_output = Path(tmpdir) / "polars.parquet"
            polars_bench = PolarsIngestionBenchmark(test_config)
            with polars_bench:
                results['polars'] = polars_bench.ingest_csv(
                    [sample_csv_file], 
                    polars_output
                )
        
        # Both should succeed
        assert results['duckdb'].is_success()
        assert results['polars'].is_success()
        
        # Both should process same number of rows
        assert results['duckdb'].rows_processed == results['polars'].rows_processed
        
        # Generate comparison
        print(f"\nDuckDB: {results['duckdb'].duration_seconds:.2f}s, "
              f"{results['duckdb'].peak_memory_gb:.2f}GB")
        print(f"Polars: {results['polars'].duration_seconds:.2f}s, "
              f"{results['polars'].peak_memory_gb:.2f}GB")
```

### 4.3 Performance Regression Tests

**Priority**: 🟢 MEDIUM

**File**: `tests/test_performance_regression.py` (NEW)

```python
"""Performance regression tests."""

import pytest
import json
from pathlib import Path
from typing import Dict, Any

class PerformanceBaseline:
    """Store and compare against performance baselines."""
    
    def __init__(self, baseline_file: Path):
        self.baseline_file = baseline_file
        self.baselines = self._load_baselines()
    
    def _load_baselines(self) -> Dict[str, Any]:
        """Load baseline performance metrics."""
        if self.baseline_file.exists():
            with open(self.baseline_file) as f:
                return json.load(f)
        return {}
    
    def update_baseline(self, benchmark_name: str, metrics: Dict[str, float]):
        """Update baseline for a benchmark."""
        self.baselines[benchmark_name] = metrics
        
        with open(self.baseline_file, 'w') as f:
            json.dump(self.baselines, f, indent=2)
    
    def check_regression(
        self, 
        benchmark_name: str, 
        current_metrics: Dict[str, float],
        tolerance: float = 0.2  # 20% tolerance
    ) -> Dict[str, Any]:
        """Check if performance has regressed."""
        if benchmark_name not in self.baselines:
            return {'status': 'no_baseline', 'new_benchmark': True}
        
        baseline = self.baselines[benchmark_name]
        regressions = []
        
        for metric, current_value in current_metrics.items():
            if metric in baseline:
                baseline_value = baseline[metric]
                change_ratio = (current_value - baseline_value) / baseline_value
                
                if change_ratio > tolerance:
                    regressions.append({
                        'metric': metric,
                        'baseline': baseline_value,
                        'current': current_value,
                        'change_percent': change_ratio * 100
                    })
        
        return {
            'status': 'regression' if regressions else 'ok',
            'regressions': regressions
        }

@pytest.fixture
def baseline():
    """Performance baseline fixture."""
    baseline_file = Path('tests/performance_baselines.json')
    return PerformanceBaseline(baseline_file)

def test_duckdb_ingestion_no_regression(sample_csv_file, test_config, baseline):
    """Test that DuckDB ingestion hasn't regressed."""
    benchmark = DuckDBIngestionBenchmark(test_config)
    
    with tempfile.TemporaryDirectory() as tmpdir:
        output = Path(tmpdir) / "output.parquet"
        
        with benchmark:
            result = benchmark.ingest_csv([sample_csv_file], output)
        
        metrics = {
            'duration_seconds': result.duration_seconds,
            'peak_memory_gb': result.peak_memory_gb
        }
        
        regression_check = baseline.check_regression(
            'duckdb_ingestion_1000_rows',
            metrics,
            tolerance=0.3  # 30% tolerance for tests
        )
        
        if regression_check['status'] == 'regression':
            pytest.fail(
                f"Performance regression detected:\n"
                f"{json.dumps(regression_check['regressions'], indent=2)}"
            )
        
        # Update baseline if this is the first run
        if regression_check['status'] == 'no_baseline':
            baseline.update_baseline('duckdb_ingestion_1000_rows', metrics)
```

---

## Phase 5: Documentation & Deployment (Week 5)

### 5.1 Enhanced Documentation

**Priority**: 🟢 MEDIUM

**File**: `benchmarks/README.md` (UPDATED)

```markdown
# CNPJ Benchmarking Suite v2.0

## What's New in v2.0

### 🎯 Major Improvements
- **Memory Safety**: Automatic memory requirement estimation and fallback strategies
- **Error Recovery**: Per-file error handling with partial success tracking
- **Real-Time Monitoring**: Live memory alerts and resource tracking
- **Standardized Configuration**: Unified config for all benchmarks
- **Enhanced Reporting**: Multiple output formats with executive summaries

### 🐛 Critical Fixes
- Fixed memory calculation bugs in Polars benchmarks
- Fixed chunked processing memory accumulation
- Standardized CSV encoding across all tools
- Separated PostgreSQL benchmarks from DuckDB

## Quick Start

### Installation

```bash
# Install dependencies
pip install -r requirements.txt

# Run tests to verify installation
pytest tests/

# Run quick benchmark (recommended first time)
python -m benchmarks.runners.quick_runner
```

### Basic Usage

```bash
# Full benchmark with default settings
python -m benchmarks.runners.full_runner \
    --data-dir data/EXTRACTED_FILES \
    --output-dir results

# Memory-constrained environment
python -m benchmarks.runners.full_runner \
    --memory-limit 4GB \
    --enable-monitoring

# Custom benchmark
python -m benchmarks.runners.custom_runner \
    --config my_config.yaml
```

### Configuration

Create a configuration file `benchmark_config.yaml`:

```yaml
memory_limit_gb: 8.0
max_threads: 8
csv_encoding: ISO-8859-1
streaming_enabled: true
monitoring:
  enabled: true
  warning_threshold: 0.7
  critical_threshold: 0.9
```

## Architecture

```
benchmarks/
├── config.py              # Centralized configuration
├── utils.py               # Core utilities
├── base.py                # Base benchmark classes
├── monitoring.py          # Real-time monitoring
├── ingestion/             # Ingestion benchmarks
│   ├── duckdb_ingestion.py
│   └── polars_ingestion.py
├── query/                 # Query benchmarks
│   ├── duckdb_query.py
│   ├── polars_query.py
│   └── postgres_query.py
├── runners/               # Benchmark orchestration
│   ├── quick_runner.py
│   ├── full_runner.py
│   └── custom_runner.py
└── analysis/              # Result analysis
    ├── analyzer.py
    ├── visualizer.py
    └── reporter.py
```

## Memory Safety

The suite now includes automatic memory safety checks:

1. **Estimation**: Calculates expected memory usage before execution
2. **Safety Check**: Verifies sufficient memory is available
3. **Automatic Fallback**: Switches to chunked processing if needed
4. **Real-Time Monitoring**: Alerts when approaching memory limits

Example:

```python
from benchmarks.utils import estimate_memory_requirements, check_memory_safety

# Estimate memory for 5GB CSV file
estimate = estimate_memory_requirements(5.0, "csv_to_parquet")
# Returns: {'estimated_peak_gb': 30.0, 'multiplier': 6.0}

# Check if safe to proceed
safety = check_memory_safety(30.0, safety_margin=0.3)
if not safety['is_safe']:
    print(f"Recommendation: {safety['recommendation']}")
    # Use chunked processing instead
```

## Error Handling

Benchmarks now support:

- **Per-file error handling**: One failed file doesn't fail entire benchmark
- **Partial success tracking**: Records which files succeeded/failed
- **Automatic recovery**: Retries with fallback strategies
- **Detailed error reporting**: Captures errors per file

## Monitoring

Enable real-time monitoring:

```python
from benchmarks.monitoring import RealTimeMonitor

monitor = RealTimeMonitor(
    warning_threshold=0.7,  # Alert at 70% memory
    critical_threshold=0.9   # Critical at 90%
)

def on_critical_memory(alert):
    print(f"CRITICAL: {alert.message}")
    # Trigger graceful shutdown

monitor.on_critical = on_critical_memory
monitor.start(memory_limit_gb=8.0)

# Run benchmarks...

monitor.stop()
```

## Best Practices

### 1. Always Run Quick Test First
```bash
python -m benchmarks.runners.quick_runner
```

### 2. Enable Monitoring for Large Files
```bash
python -m benchmarks.runners.full_runner --enable-monitoring
```

### 3. Use Configuration Files
Store your settings in YAML for reproducibility:
```bash
python -m benchmarks.runners.full_runner --config production.yaml
```

### 4. Check Memory Before Running
```python
from benchmarks.utils import check_memory_safety
safety = check_memory_safety(required_memory_gb=20.0)
if not safety['is_safe']:
    # Adjust configuration or use chunked processing
```

### 5. Review Executive Summary
After benchmarks complete, check `summary.txt` for quick insights.

## Troubleshooting

### Out of Memory Errors

**Problem**: Benchmark fails with MemoryError

**Solutions**:
1. Enable automatic fallback:
   ```bash
   python -m benchmarks.runners.full_runner --enable-fallback
   ```

2. Reduce chunk size:
   ```yaml
   chunk_size: 25000  # Default is 50000
   ```

3. Lower memory limit to force chunked processing:
   ```bash
   --memory-limit 2GB
   ```

### Inconsistent Results

**Problem**: DuckDB and Polars show different results

**Check**:
1. Encoding configuration (should be `ISO-8859-1`)
2. Error handling settings (`ignore_errors` should match)
3. CSV delimiter (should be `;` for CNPJ files)

### Slow Performance

**Problem**: Benchmarks taking too long

**Optimizations**:
1. Increase thread count:
   ```yaml
   max_threads: 16
   ```

2. Use faster compression:
   ```yaml
   compression: snappy  # Instead of zstd
   ```

3. Reduce schema inference:
   ```yaml
   schema_inference_rows: 1000  # Instead of 10000
   ```

## API Reference

### Configuration

```python
from benchmarks.config import BenchmarkConfig

config = BenchmarkConfig(
    memory_limit_gb=8.0,
    max_threads=8,
    csv_encoding="ISO-8859-1",
    streaming_enabled=True
)
```

### Running Benchmarks

```python
from benchmarks.ingestion.duckdb_ingestion import DuckDBIngestionBenchmark

benchmark = DuckDBIngestionBenchmark(config)

with benchmark:
    result = benchmark.ingest_csv(
        csv_files=[Path("data.csv")],
        output_path=Path("output.parquet")
    )

print(f"Status: {result.status}")
print(f"Duration: {result.duration_seconds}s")
print(f"Memory: {result.peak_memory_gb}GB")
```

### Analyzing Results

```python
from benchmarks.analysis.reporter import BenchmarkReporter

reporter = BenchmarkReporter(results)
reporter.generate_all(output_dir=Path("reports"))
```

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development guidelines.

## License

See [LICENSE](LICENSE) for details.
```

### 5.2 Migration Guide

**File**: `MIGRATION_V1_TO_V2.md` (NEW)

```markdown
# Migration Guide: v1.0 → v2.0

## Overview

Version 2.0 introduces breaking changes for improved reliability and accuracy. This guide helps you migrate from v1.0 to v2.0.

## Breaking Changes

### 1. Configuration System

**v1.0**:
```python
benchmark = DuckDBBenchmark(memory_limit="8GB", threads=8)
```

**v2.0**:
```python
from benchmarks.config import BenchmarkConfig

config = BenchmarkConfig(
    memory_limit_gb=8.0,
    max_threads=8
)
benchmark = DuckDBIngestionBenchmark(config)
```

### 2. Result Status

**v1.0**:
```python
if not result.errors:
    print("Success")
```

**v2.0**:
```python
from benchmarks.utils import BenchmarkStatus

if result.status == BenchmarkStatus.SUCCESS:
    print("Success")
elif result.status == BenchmarkStatus.PARTIAL_SUCCESS:
    print("Partial success")
```

### 3. Benchmark Execution

**v1.0**:
```python
result = benchmark.benchmark_csv_ingestion(files, output)
```

**v2.0**:
```python
with benchmark:
    result = benchmark.ingest_csv(files, output)
```

## Step-by-Step Migration

### Step 1: Update Imports

```python
# OLD
from benchmarks.duckdb_benchmark import run_duckdb_benchmarks
from benchmarks.polars_benchmark import run_polars_benchmarks

# NEW
from benchmarks.config import BenchmarkConfig
from benchmarks.ingestion.duckdb_ingestion import DuckDBIngestionBenchmark
from benchmarks.ingestion.polars_ingestion import PolarsIngestionBenchmark
```

### Step 2: Update Configuration

```python
# OLD
memory_limit = "8GB"
threads = 8

# NEW
config = BenchmarkConfig(
    memory_limit_gb=8.0,
    max_threads=8,
    csv_encoding="ISO-8859-1"  # Now standardized
)
```

### Step 3: Update Benchmark Execution

```python
# OLD
results = run_duckdb_benchmarks(
    csv_files=files,
    output_dir=output,
    memory_limit="8GB"
)

# NEW
benchmark = DuckDBIngestionBenchmark(config)
with benchmark:
    result = benchmark.ingest_csv(
        csv_files=files,
        output_path=output / "output.parquet"
    )
```

### Step 4: Update Result Handling

```python
# OLD
if not result.errors:
    print(f"Success: {result.duration_seconds}s")
else:
    print(f"Failed: {result.errors}")

# NEW
if result.is_success():
    print(f"Success: {result.duration_seconds}s")
elif result.is_usable():
    print(f"Partial success with warnings:")
    for warning in result.warnings:
        print(f"  - {warning}")
else:
    print(f"Failed: {result.errors}")
```

## New Features to Adopt

### 1. Memory Safety Checks

```python
from benchmarks.utils import estimate_memory_requirements, check_memory_safety

# Before running benchmark
estimate = estimate_memory_requirements(5.0, "csv_to_parquet")
safety = check_memory_safety(estimate['estimated_peak_gb'])

if not safety['is_safe']:
    print(f"Warning: {safety['recommendation']}")
```

### 2. Real-Time Monitoring

```python
from benchmarks.monitoring import RealTimeMonitor

monitor = RealTimeMonitor()
monitor.start(memory_limit_gb=8.0)

# Run benchmarks...

monitor.stop()
print(f"Alerts: {monitor.get_alert_summary()}")
```

### 3. Enhanced Reporting

```python
from benchmarks.analysis.reporter import BenchmarkReporter

reporter = BenchmarkReporter(results)
reporter.generate_all(output_dir=Path("reports"))
# Generates: markdown, HTML, CSV, JSON, and executive summary
```

## Deprecated Features

The following features from v1.0 are deprecated:

1. `max_threads` parameter in Polars (now handled by config)
2. Direct memory limit strings (use `memory_limit_gb` float)
3. `benchmark_aggregation` with internal connection (use explicit connection)

