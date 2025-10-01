#!/usr/bin/env python3
"""
Benchmark utilities for measuring memory, time, and performance.
"""

import time
import psutil
import os
import gc
from pathlib import Path
from dataclasses import dataclass
from typing import Dict, Any, Optional, Callable
import json
from contextlib import contextmanager
import logging

logger = logging.getLogger(__name__)

@dataclass
class BenchmarkResult:
    """Results from a benchmark run."""
    name: str
    duration_seconds: float
    peak_memory_gb: float
    start_memory_gb: float
    end_memory_gb: float
    memory_delta_gb: float
    cpu_percent: float
    io_read_gb: float
    io_write_gb: float
    output_size_gb: Optional[float] = None
    compression_ratio: Optional[float] = None
    rows_processed: Optional[int] = None
    errors: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None

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
    """Force garbage collection and memory cleanup."""
    import gc
    gc.collect()
    
    # Try to release memory back to OS (platform dependent)
    try:
        import ctypes
        if os.name == 'nt':  # Windows
            ctypes.windll.kernel32.SetProcessWorkingSetSize(-1, -1, -1)
    except:
        pass

def save_benchmark_results(results: list[BenchmarkResult], output_file: Path):
    """Save benchmark results to JSON file."""
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    data = {
        'timestamp': time.time(),
        'results': [r.to_dict() for r in results]
    }
    
    with open(output_file, 'w') as f:
        json.dump(data, f, indent=2)
    
    logger.info(f"Benchmark results saved to {output_file}")

def load_benchmark_results(input_file: Path) -> list[BenchmarkResult]:
    """Load benchmark results from JSON file."""
    with open(input_file, 'r') as f:
        data = json.load(f)
    
    results = []
    for r in data['results']:
        results.append(BenchmarkResult(**r))
    
    return results