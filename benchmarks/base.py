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
        for file_path in csv_files:
            if not file_path.exists():
                raise FileNotFoundError(f"Input file not found: {file_path}")
            if not file_path.is_file():
                raise ValueError(f"Path is not a file: {file_path}")
            if file_path.stat().st_size == 0:
                raise ValueError(f"File is empty: {file_path}")
    
    def estimate_duration(self, input_size_gb: float) -> float:
        """Estimate benchmark duration (to be overridden)."""
        # Default: rough estimate of 10 seconds per GB
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