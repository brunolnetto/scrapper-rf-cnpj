#!/usr/bin/env python3
"""
Benchmark utilities for measuring memory, time, and performance.
"""

import time
import psutil
import os
import gc
from pathlib import Path
from dataclasses import dataclass, field
from typing import Dict, Any, Optional, Callable, List
import json
from contextlib import contextmanager
import logging
from enum import Enum

logger = logging.getLogger(__name__)

class BenchmarkStatus(Enum):
    """Status enumeration for benchmarks."""
    SUCCESS = "success"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"
    RUNNING = "running"
    ERROR = "error"

@dataclass  
class BenchmarkResult:
    """Enhanced results from a benchmark run."""
    name: str
    status: BenchmarkStatus
    duration_seconds: float = 0.0
    peak_memory_gb: float = 0.0
    start_memory_gb: float = 0.0
    end_memory_gb: float = 0.0
    memory_delta_gb: float = 0.0
    cpu_percent: float = 0.0
    io_read_gb: float = 0.0
    io_write_gb: float = 0.0
    output_size_gb: Optional[float] = None
    compression_ratio: Optional[float] = None
    rows_processed: Optional[int] = None
    errors: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    
    # Enhanced fields for compatibility
    execution_time: Optional[float] = None
    error_message: Optional[str] = None
    partial_results: Optional[Dict[str, Any]] = None
    recovery_attempts: int = 0
    recovery_info: Optional[str] = None
    warnings: List[str] = field(default_factory=list)
    
    def __post_init__(self):
        """Handle field compatibility."""
        # Sync execution_time with duration_seconds
        if self.execution_time is not None and self.duration_seconds == 0.0:
            self.duration_seconds = self.execution_time
        elif self.duration_seconds > 0.0 and self.execution_time is None:
            self.execution_time = self.duration_seconds
            
        # Sync error_message with errors
        if self.error_message is not None and self.errors is None:
            self.errors = self.error_message
        elif self.errors is not None and self.error_message is None:
            self.error_message = self.errors
    
    def is_success(self) -> bool:
        """Check if benchmark succeeded."""
        return self.status in [BenchmarkStatus.SUCCESS, BenchmarkStatus.COMPLETED]
    
    def is_usable(self) -> bool:
        """Check if results are usable (success or partial)."""
        return self.status in [BenchmarkStatus.SUCCESS, BenchmarkStatus.COMPLETED] or self.partial_results is not None
    
    def is_failure(self) -> bool:
        """Check if benchmark failed."""
        return self.status in [
            BenchmarkStatus.SUCCESS,
            BenchmarkStatus.PARTIAL_SUCCESS
        ]

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            'name': self.name,
            'duration_seconds': self.duration_seconds,
            'peak_memory_gb': self.peak_memory_gb,
            'start_memory_gb': self.start_memory_gb,
            'end_memory_gb': self.end_memory_gb,
            'memory_delta_gb': self.memory_delta_gb,
            'cpu_percent': self.cpu_percent,
            'io_read_gb': self.io_read_gb,
            'io_write_gb': self.io_write_gb,
            'output_size_gb': self.output_size_gb,
            'compression_ratio': self.compression_ratio,
            'rows_processed': self.rows_processed,
            'errors': self.errors,
            'metadata': self.metadata or {}
        }

class PerformanceMonitor:
    """Monitor system performance during benchmark execution."""
    
    def __init__(self):
        self.process = psutil.Process()
        self.start_time = None
        self.peak_memory = 0
        self.start_memory = 0
        self.start_io = None
        self.cpu_times = []
        
    def start(self):
        """Start monitoring."""
        gc.collect()  # Clean up before starting
        self.start_time = time.time()
        self.start_memory = self.process.memory_info().rss / (1024**3)
        self.peak_memory = self.start_memory
        self.start_io = self.process.io_counters()
        
    def update_peak_memory(self):
        """Update peak memory usage."""
        current_memory = self.process.memory_info().rss / (1024**3)
        if current_memory > self.peak_memory:
            self.peak_memory = current_memory
            
    def get_current_stats(self) -> Dict[str, float]:
        """Get current performance statistics."""
        current_memory = self.process.memory_info().rss / (1024**3)
        cpu_percent = self.process.cpu_percent()
        
        if current_memory > self.peak_memory:
            self.peak_memory = current_memory
            
        return {
            'current_memory_gb': current_memory,
            'peak_memory_gb': self.peak_memory,
            'cpu_percent': cpu_percent,
            'elapsed_seconds': time.time() - self.start_time if self.start_time else 0
        }
    
    def finish(self) -> Dict[str, float]:
        """Finish monitoring and return final stats."""
        end_time = time.time()
        end_memory = self.process.memory_info().rss / (1024**3)
        end_io = self.process.io_counters()
        
        duration = end_time - self.start_time
        memory_delta = end_memory - self.start_memory
        
        # Calculate I/O in GB
        io_read_gb = (end_io.read_bytes - self.start_io.read_bytes) / (1024**3)
        io_write_gb = (end_io.write_bytes - self.start_io.write_bytes) / (1024**3)
        
        # Average CPU during the run
        avg_cpu = self.process.cpu_percent()
        
        return {
            'duration_seconds': duration,
            'peak_memory_gb': self.peak_memory,
            'start_memory_gb': self.start_memory,
            'end_memory_gb': end_memory,
            'memory_delta_gb': memory_delta,
            'cpu_percent': avg_cpu,
            'io_read_gb': io_read_gb,
            'io_write_gb': io_write_gb
        }

@contextmanager
def benchmark_context(name: str):
    """Context manager for benchmarking operations."""
    monitor = PerformanceMonitor()
    logger.info(f"Starting benchmark: {name}")
    
    try:
        monitor.start()
        yield monitor
        
    except Exception as e:
        logger.error(f"Benchmark {name} failed: {e}")
        raise
    finally:
        stats = monitor.finish()
        logger.info(f"Finished benchmark: {name} - Duration: {stats['duration_seconds']:.2f}s, "
                   f"Peak Memory: {stats['peak_memory_gb']:.2f}GB")

def get_file_size_gb(file_path: Path) -> float:
    """Get file size in GB."""
    if not file_path.exists():
        return 0.0
    return file_path.stat().st_size / (1024**3)

def cleanup_memory():
    """Force garbage collection to free memory."""
    import gc
    gc.collect()

def detect_cnpj_file_format(file_path: Path) -> str:
    """Detect if a file is a Brazilian CNPJ CSV file based on naming patterns.
    
    Brazilian Federal Revenue files don't use standard extensions but follow naming patterns.
    Examples:
    - F.K03200$W.SIMPLES.CSV.D50913 -> CSV format
    - K3241.K03200Y*.D50809.EMPRECSV -> CSV format
    
    Returns:
        'csv', 'parquet', or 'unknown'
    """
    filename = file_path.name.upper()
    
    # Brazilian CNPJ file patterns (order matters - more specific first)
    cnpj_patterns = [
        'SIMPLES', 'ESTABELE', 'EMPRESA', 'SOCIO', 'CNAE', 
        'MOTI', 'MUNIC', 'NATJU', 'PAIS', 'QUALS'
    ]
    
    # Check if it's a CNPJ file (these are always CSV format)
    for pattern in cnpj_patterns:
        if pattern in filename:
            return 'csv'
    
    # Check standard extensions
    if file_path.suffix.lower() in ['.csv']:
        return 'csv'
    elif file_path.suffix.lower() in ['.parquet']:
        return 'parquet'
    
    # Additional heuristics for CNPJ files
    # Many CNPJ files contain 'CSV' in the name but don't end with .csv
    if 'CSV' in filename or filename.endswith('.D50913') or filename.endswith('.D50809'):
        return 'csv'
    
    return 'unknown'

def is_cnpj_file(file_path: Path) -> bool:
    """Check if a file is a Brazilian CNPJ data file."""
    return detect_cnpj_file_format(file_path) in ['csv', 'parquet']

def estimate_file_memory_requirements(file_path: Path, operation_type: str = "csv") -> float:
    """Estimate memory requirements for a file operation.
    
    Args:
        file_path: Path to the file
        operation_type: Type of operation ('csv', 'polars_csv', 'duckdb_csv', 'parquet', 'query')
    
    Returns:
        Estimated memory requirement in GB
    """
    if not file_path.exists():
        return 0.0
        
    file_size_gb = file_path.stat().st_size / (1024**3)
    
    # Memory multipliers for different operations
    multipliers = {
        "csv": 6.0,           # Conservative for generic CSV operations
        "polars_csv": 5.0,    # Polars is more memory efficient
        "duckdb_csv": 4.0,    # DuckDB has good memory management
        "parquet": 2.0,       # Parquet is compressed
        "query": 1.5          # Query operations on loaded data
    }
    
    multiplier = multipliers.get(operation_type, 3.0)
    return file_size_gb * multiplier

# Legacy benchmark result functions removed - use BenchmarkAnalyzer instead


# NEW: Memory Safety Utilities
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
        "parquet_to_csv": 3.0,
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
        "can_allocate_gb": can_allocate_gb,
        "available_gb": available_gb,
        "total_gb": total_gb,
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


class BenchmarkRecovery:
    """Handle benchmark failures with recovery strategies."""
    
    @staticmethod
    def with_retry(
        func: Callable,
        max_retries: int = 3,
        backoff_seconds: int = 5
    ) -> BenchmarkResult:
        """Retry failed benchmarks with exponential backoff."""
        last_exception = None
        
        for attempt in range(max_retries):
            try:
                result = func()
                if result.is_success():
                    return result
                elif attempt < max_retries - 1:
                    sleep_time = backoff_seconds * (2 ** attempt)
                    logger.warning(f"Benchmark failed, retrying in {sleep_time}s...")
                    time.sleep(sleep_time)
                    cleanup_memory()
            except Exception as e:
                last_exception = e
                if attempt < max_retries - 1:
                    sleep_time = backoff_seconds * (2 ** attempt)
                    logger.warning(f"Benchmark exception: {e}, retrying in {sleep_time}s...")
                    time.sleep(sleep_time)
                    cleanup_memory()
        
        # All retries failed
        return BenchmarkResult(
            name="Failed Benchmark",
            status=BenchmarkStatus.FAILED,
            duration_seconds=0.0,
            peak_memory_gb=0.0,
            start_memory_gb=0.0,
            end_memory_gb=0.0,
            memory_delta_gb=0.0,
            cpu_percent=0.0,
            io_read_gb=0.0,
            io_write_gb=0.0,
            errors=str(last_exception) if last_exception else "Max retries exceeded",
            recovery_attempted=True
        )
    
    @staticmethod
    def with_fallback(
        primary_func: Callable,
        fallback_func: Callable,
        condition: Optional[Callable] = None
    ) -> BenchmarkResult:
        """Try primary strategy, fallback on failure."""
        try:
            result = primary_func()
            if result.is_success() or (condition and condition(result)):
                return result
        except Exception as e:
            logger.warning(f"Primary strategy failed: {e}, trying fallback...")
            
        try:
            result = fallback_func()
            result.recovery_attempted = True
            result.recovery_strategy = "fallback"
            return result
        except Exception as e:
            return BenchmarkResult(
                name="Failed Benchmark with Fallback",
                status=BenchmarkStatus.FAILED,
                duration_seconds=0.0,
                peak_memory_gb=0.0,
                start_memory_gb=0.0,
                end_memory_gb=0.0,
                memory_delta_gb=0.0,
                cpu_percent=0.0,
                io_read_gb=0.0,
                io_write_gb=0.0,
                errors=f"Both primary and fallback failed: {e}",
                recovery_attempted=True
            )