from typing import Optional, List, Dict
from dataclasses import dataclass
from pathlib import Path
from dataclasses import dataclass

import polars as pl

from ....setup.config.models import ConversionConfig, MemoryMonitorConfig


@dataclass
class ProcessingStrategy:
    """Configuration for a specific processing approach."""
    name: str
    row_group_size: int
    batch_size: Optional[int] = None  # For chunked approaches
    compression: str = "snappy"
    low_memory: bool = True
    streaming: bool = True

class LargeDatasetConfig(ConversionConfig):
    """
    Optimized configuration for very large datasets.
    """
    def __init__(self):
        super().__init__()
        
        # More conservative memory settings for reliability
        self.memory_limit_mb = 1200  # Reduced from 1500 for more safety
        self.cleanup_threshold_ratio = 0.7  # Earlier cleanup trigger
        self.baseline_buffer_mb = 512  # More system memory buffer
        
        # Optimize for large files
        self.row_group_size = 50000  # Smaller row groups for memory efficiency
        self.compression = "snappy"  # Fast compression
        
        # Single-threaded for memory control
        self.workers = 1

# Configuration helper for very constrained environments
class UltraConservativeConfig(ConversionConfig):
    """
    For extremely memory-constrained environments (e.g., 2GB container).
    """
    def __init__(self):
        super().__init__()
        
        # Fraction-based memory limiting (safer for containers)
        self.memory_limit_mode = "fraction"
        self.memory_limit_fraction = 0.4  # Use only 40% of available memory
        
        # Very conservative thresholds
        self.cleanup_threshold_ratio = 0.5  # Cleanup at 50% pressure
        self.baseline_buffer_mb = 256  # Smaller buffer for constrained envs
        
        # Small chunks for maximum safety
        self.row_group_size = 25000
        self.chunk_size = 25000
        self.compression = "snappy"
        
        # Single-threaded to avoid memory multiplication
        self.workers = 1


@dataclass
class ChunkIterator:
    """Iterator that reads CSV in fixed-size chunks."""
    csv_path: Path
    delimiter: str
    chunk_size: int
    schema_override: Dict
    expected_columns: List[str]
    _offset: int = 0
    _exhausted: bool = False
    
    def __iter__(self):
        return self
    
    def __next__(self) -> pl.DataFrame:
        """Read next chunk from CSV."""
        if self._exhausted:
            raise StopIteration
            
        try:
            # Read chunk using scan with offset
            chunk = pl.read_csv(
                str(self.csv_path),
                separator=self.delimiter,
                dtypes=self.schema_override,
                encoding="utf8-lossy",
                ignore_errors=True,
                skip_rows=self._offset,
                n_rows=self.chunk_size,
                has_header=False,
                low_memory=True,
                rechunk=False,
                quote_char='"'
            )
            
            # Check if we got any data
            if len(chunk) == 0:
                self._exhausted = True
                raise StopIteration
            
            # Rename columns if needed
            if self.expected_columns and len(chunk.columns) == len(self.expected_columns):
                chunk = chunk.rename(
                    {chunk.columns[i]: self.expected_columns[i] 
                     for i in range(len(self.expected_columns))}
                )
            
            self._offset += self.chunk_size
            
            # If we got less than chunk_size, we're done
            if len(chunk) < self.chunk_size:
                self._exhausted = True
            
            return chunk
            
        except Exception as e:
            self._exhausted = True
            raise StopIteration

@dataclass
class ProcessingLimits:
    """Centralized processing limits and thresholds."""
    
    # Memory safety factors
    MEMORY_SAFETY_FACTOR: float = 0.7
    MEMORY_BUFFER_FACTOR: float = 0.6
    
    # Partition limits
    MAX_PARTITIONS: int = 500
    MAX_CHUNKS_PER_PARTITION: int = 1000
    
    # File size thresholds (MB)
    LARGE_FILE_THRESHOLD_MB: float = 500.0
    SMALL_DATASET_THRESHOLD_MB: float = 50.0
    
    # Processing parameters
    MIN_CHUNK_SIZE: int = 10_000
    MAX_CHUNK_SIZE: int = 500_000
    DEFAULT_ROW_GROUP_SIZE: int = 50_000
    
    # Batch processing
    MAX_CONCURRENT_BATCHES: int = 10
    SCHEMA_SAMPLE_SIZE: int = 5_000
    
    # Memory estimation (bytes per column)
    BYTES_PER_COLUMN_ESTIMATE: int = 100


@dataclass
class MergeStrategy:
    """Configuration for a merge operation."""
    name: str
    use_lazy_frames: bool = True
    compression: str = "snappy"
    row_group_size: int = ProcessingLimits.DEFAULT_ROW_GROUP_SIZE
    maintain_order: bool = False
    statistics: bool = False