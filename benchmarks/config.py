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
    
    # Enhanced execution settings
    max_concurrent_benchmarks: int = 3
    benchmark_timeout: float = 1800.0  # 30 minutes
    
    # CPU threads alias for compatibility
    @property
    def cpu_threads(self) -> int:
        return self.max_threads
    
    # CSV chunk size alias for compatibility  
    @property
    def csv_chunk_size(self) -> int:
        return self.chunk_size
    
    # CSV Format (Brazilian CNPJ standard)
    csv_delimiter: str = ";"
    csv_encoding: str = "ISO-8859-1"  # Brazilian standard
    csv_has_header: bool = False
    csv_ignore_errors: bool = True
    csv_infer_schema_length: int = 10_000
    
    # Processing strategy
    chunk_size: int = 50_000
    schema_inference_rows: int = 10_000
    streaming_enabled: bool = True
    
    # Compression settings
    compression: str = "zstd"
    compression_level: int = 3
    enable_compression: bool = True
    
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
        # Auto-detect CPU cores if not set
        cpu_count = psutil.cpu_count(logical=True) or 4
        if self.max_threads <= 0:
            self.max_threads = min(cpu_count, 8)
        
        # Set thread counts for specific tools
        if self.duckdb_threads is None:
            self.duckdb_threads = self.max_threads
        if self.polars_threads is None:
            self.polars_threads = self.max_threads
        
        # Adjust memory limit based on available system memory
        available_memory_gb = psutil.virtual_memory().total / (1024**3)
        if self.memory_limit_gb > available_memory_gb * 0.8:
            self.memory_limit_gb = available_memory_gb * 0.8
            
        # Set environment variables for thread control
        os.environ['DUCKDB_THREADS'] = str(self.duckdb_threads)
        os.environ['POLARS_MAX_THREADS'] = str(self.polars_threads)
        
    @classmethod
    def from_args(cls, args) -> 'BenchmarkConfig':
        """Create config from command-line arguments."""
        return cls(
            memory_limit_gb=getattr(args, 'memory_limit_gb', 8.0),
            max_threads=getattr(args, 'max_threads', 8),
            verbose=getattr(args, 'verbose', True),
            output_dir=getattr(args, 'output_dir', 'benchmark_output')
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            'memory_limit_gb': self.memory_limit_gb,
            'max_threads': self.max_threads,
            'csv_encoding': self.csv_encoding,
            'csv_delimiter': self.csv_delimiter,
            'chunk_size': self.chunk_size,
            'compression': self.compression,
            'metadata': self.metadata
        }