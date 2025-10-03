"""Enhanced Polars benchmark with new architecture."""

import time
import polars as pl
from pathlib import Path
from typing import Dict, Any, List, Optional
import logging
import gc

from .base import BaseBenchmark, IngestionBenchmark, QueryBenchmark
from .config import BenchmarkConfig
from .utils import BenchmarkResult, BenchmarkStatus, estimate_file_memory_requirements, check_memory_safety, detect_cnpj_file_format
from .monitoring import RealTimeMonitor

logger = logging.getLogger(__name__)

class PolarsIngestionBenchmark(IngestionBenchmark):
    """Polars data ingestion benchmark with enhanced error handling."""
    
    def __init__(self, config: BenchmarkConfig, file_path: Path, table_name: str):
        super().__init__(config)
        self.file_path = file_path
        self.table_name = table_name
        self.name = f"polars_ingestion_{table_name}"
        self.monitor = RealTimeMonitor()
    
    def setup(self) -> None:
        """Setup benchmark environment."""
        self.prepare_environment()
    
    def teardown(self) -> None:
        """Cleanup benchmark environment."""
        self.cleanup_environment()
    
    def run(self, **kwargs) -> BenchmarkResult:
        """Execute the benchmark."""
        result_data = self.run_ingestion()
        return BenchmarkResult(
            name=self.name,
            status=result_data.get('status', BenchmarkStatus.COMPLETED),
            execution_time=result_data.get('execution_time'),
            error_message=result_data.get('error_message'),
            metadata=result_data
        )
    
    def ingest_csv(self, csv_files: List[Path], output_path: Path) -> BenchmarkResult:
        """Ingest CSV files to Parquet (compatibility method)."""
        # Use the main run_ingestion method
        result_data = self.run_ingestion()
        return BenchmarkResult(
            name=self.name,
            status=result_data.get('status', BenchmarkStatus.COMPLETED),
            execution_time=result_data.get('execution_time'),
            error_message=result_data.get('error_message'),
            metadata=result_data
        )
    
    def estimate_memory_gb(self) -> float:
        """Estimate memory requirements for this benchmark."""
        return estimate_file_memory_requirements(self.file_path, operation_type="polars_csv")
    
    def prepare_environment(self) -> bool:
        """Prepare environment for benchmark execution."""
        # Check memory availability
        required_memory = self.estimate_memory_gb()
        if not check_memory_safety(required_memory, safety_margin=0.3):
            logger.warning(f"Insufficient memory for {self.name}: requires {required_memory:.1f}GB")
            return False
        
        # Start monitoring
        self.monitor.start(self.config.memory_limit_gb)
        
        # Set Polars configuration
        pl.Config.set_streaming_chunk_size(self.config.csv_chunk_size)
        pl.Config.set_fmt_str_lengths(100)
        
        return True
    
    def run_ingestion(self) -> Dict[str, Any]:
        """Run the Polars ingestion benchmark."""
        start_time = time.time()
        
        try:
            # Enhanced CSV reading with proper error handling
            scan_options = {
                "separator": self.config.csv_delimiter,
                "encoding": "utf8-lossy",  # Polars only supports utf8 variants
                "null_values": ["", "NULL", "null"],
                "infer_schema_length": self.config.csv_infer_schema_length,
                "ignore_errors": False,  # Fail fast on errors
                "truncate_ragged_lines": True
            }
            
            # Use lazy loading for memory efficiency
            logger.info(f"Starting Polars CSV scan: {self.file_path}")
            lazy_df = pl.scan_csv(str(self.file_path), **scan_options)
            
            # Get basic info without materializing
            row_count = lazy_df.select(pl.len()).collect().item()
            column_count = len(lazy_df.columns)
            
            # Memory-aware processing based on file size
            file_size_mb = self.file_path.stat().st_size / (1024 * 1024)
            
            if file_size_mb < 100:  # Small files - collect directly
                df = lazy_df.collect()
                memory_usage = df.estimated_size("mb")
                logger.info(f"Small file processed: {len(df)} rows, {memory_usage:.1f}MB")
            else:  # Large files - streaming processing
                logger.info("Using streaming processing for large file")
                
                # Use streaming collection (synchronous)
                df = lazy_df.collect(streaming=True)
                memory_usage = df.estimated_size("mb")
                row_count = len(df)
                
                # Check memory pressure
                if self.monitor.alerts and self.monitor.alerts[-1].severity == "critical":
                    raise RuntimeError("Memory pressure detected during processing")
                
                logger.info(f"Large file processed: {row_count} rows, {memory_usage:.1f}MB")
                
                # Clean up immediately for large files
                del df
                gc.collect()
            
            execution_time = time.time() - start_time
            
            return {
                "status": BenchmarkStatus.COMPLETED,
                "execution_time": execution_time,
                "rows_processed": row_count,
                "columns_processed": column_count,
                "peak_memory_mb": memory_usage,
                "file_size_mb": file_size_mb,
                "processing_mode": "streaming" if file_size_mb >= 100 else "direct"
            }
            
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"Polars ingestion failed: {e}")
            
            return {
                "status": BenchmarkStatus.FAILED,
                "execution_time": execution_time,
                "error_message": str(e),
                "rows_processed": 0,
                "columns_processed": 0
            }
    
    def cleanup_environment(self):
        """Clean up after benchmark execution."""
        self.monitor.stop()
        gc.collect()

class PolarsQueryBenchmark(QueryBenchmark):
    """Polars query benchmark with optimized execution."""
    
    def __init__(self, config: BenchmarkConfig, data_path: Path, query_name: str):
        super().__init__(config)
        self.data_path = data_path
        self.query_name = query_name
        self.name = f"polars_query_{query_name}"
        self.df: Optional[pl.DataFrame] = None
    
    def setup(self) -> None:
        """Setup benchmark environment."""
        self.prepare_environment()
    
    def teardown(self) -> None:
        """Cleanup benchmark environment."""
        self.cleanup_environment()
    
    def run(self, **kwargs) -> BenchmarkResult:
        """Execute the benchmark."""
        result_data = self.run_query()
        return BenchmarkResult(
            name=self.name,
            status=result_data.get('status', BenchmarkStatus.COMPLETED),
            execution_time=result_data.get('execution_time'),
            error_message=result_data.get('error_message'),
            metadata=result_data
        )
    
    def execute_query(self, query: str, data_source: Path) -> BenchmarkResult:
        """Execute a query against data source (compatibility method)."""
        # Use the main run_query method
        result_data = self.run_query()
        return BenchmarkResult(
            name=self.name,
            status=result_data.get('status', BenchmarkStatus.COMPLETED),
            execution_time=result_data.get('execution_time'),
            error_message=result_data.get('error_message'),
            metadata=result_data
        )
    
    def prepare_environment(self) -> bool:
        """Load data for query benchmarks."""
        try:
            # Check if file exists
            if not self.data_path.exists():
                logger.error(f"Data file not found: {self.data_path}")
                return False
                
            # Detect file format using CNPJ-aware detection
            file_format = detect_cnpj_file_format(self.data_path)
            
            if file_format == 'parquet':
                self.df = pl.read_parquet(self.data_path)
                logger.info(f"Loaded Parquet data: {len(self.df)} rows, {len(self.df.columns)} columns")
            elif file_format == 'csv':
                # For large CSV files, use lazy loading and sample for queries
                file_size_mb = self.data_path.stat().st_size / (1024 * 1024)
                
                if file_size_mb > 500:  # For files larger than 500MB, sample for queries
                    logger.info(f"Large CSV file ({file_size_mb:.1f}MB), using sample for queries")
                    # Read a sample for query testing
                    lazy_df = pl.scan_csv(
                        str(self.data_path),
                        separator=self.config.csv_delimiter,
                        encoding="utf8-lossy",
                        null_values=["", "NULL", "null"],
                        ignore_errors=True
                    )
                    # Take first 10000 rows for query testing
                    self.df = lazy_df.head(10000).collect()
                else:
                    self.df = pl.read_csv(
                        self.data_path,
                        separator=self.config.csv_delimiter,
                        encoding="utf8-lossy",  # Polars encoding compatibility
                        null_values=["", "NULL", "null"],
                        ignore_errors=True
                    )
                    
                logger.info(f"Loaded CSV data: {len(self.df)} rows, {len(self.df.columns)} columns")
            else:
                raise ValueError(f"Unsupported file format: {file_format} for {self.data_path}")
            
            logger.info(f"Loaded data for queries: {len(self.df)} rows, {len(self.df.columns)} columns")
            return True
            
        except Exception as e:
            logger.error(f"Failed to prepare query environment: {e}")
            return False
    
    def run_query(self) -> Dict[str, Any]:
        """Run the specific query benchmark."""
        if self.df is None:
            raise RuntimeError("Data not prepared for query benchmark")
        
        start_time = time.time()
        
        try:
            if self.query_name == "aggregation_basic":
                result = self._run_basic_aggregation()
            elif self.query_name == "aggregation_grouped":
                result = self._run_grouped_aggregation()
            elif self.query_name == "filter_complex":
                result = self._run_complex_filter()
            elif self.query_name == "join_performance":
                result = self._run_join_performance()
            else:
                raise ValueError(f"Unknown query: {self.query_name}")
            
            execution_time = time.time() - start_time
            
            return {
                "status": BenchmarkStatus.COMPLETED,
                "execution_time": execution_time,
                "result_rows": len(result) if hasattr(result, '__len__') else 1,
                "peak_memory_mb": result.estimated_size("mb") if hasattr(result, 'estimated_size') else 0
            }
            
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"Query {self.query_name} failed: {e}")
            
            return {
                "status": BenchmarkStatus.FAILED,
                "execution_time": execution_time,
                "error_message": str(e)
            }
    
    def _run_basic_aggregation(self) -> pl.DataFrame:
        """Basic aggregation benchmark."""
        return self.df.select([
            pl.len().alias("count"),
            pl.col("*").sum().suffix("_sum"),
            pl.col("*").mean().suffix("_avg")
        ])
    
    def _run_grouped_aggregation(self) -> pl.DataFrame:
        """Grouped aggregation benchmark."""
        # Assume first column is grouping column
        group_col = self.df.columns[0]
        
        return self.df.group_by(group_col).agg([
            pl.len().alias("count"),
            pl.col("*").sum().suffix("_sum"),
            pl.col("*").mean().suffix("_avg")
        ])
    
    def _run_complex_filter(self) -> pl.DataFrame:
        """Complex filtering benchmark."""
        # Create complex filter conditions
        return self.df.filter(
            (pl.col(self.df.columns[0]).is_not_null()) &
            (pl.col(self.df.columns[0]).str.len() > 5) if self.df.dtypes[0] == pl.Utf8
            else (pl.col(self.df.columns[0]) > 0)
        )
    
    def _run_join_performance(self) -> pl.DataFrame:
        """Join performance benchmark."""
        # Self-join on first column
        join_col = self.df.columns[0]
        
        # Create a smaller subset for join
        subset = self.df.sample(n=min(1000, len(self.df)))
        
        return self.df.join(
            subset.select([join_col]).unique(),
            on=join_col,
            how="inner"
        )
    
    def cleanup_environment(self):
        """Clean up query environment."""
        if self.df is not None:
            del self.df
            self.df = None
        gc.collect()

class PolarsBenchmarkSuite:
    """Complete Polars benchmark suite."""
    
    def __init__(self, config: BenchmarkConfig):
        self.config = config
        self.benchmarks: List[BaseBenchmark] = []
    
    def add_ingestion_benchmarks(self, data_files: List[Path]):
        """Add ingestion benchmarks for given files."""
        for file_path in data_files:
            table_name = file_path.stem
            benchmark = PolarsIngestionBenchmark(self.config, file_path, table_name)
            self.benchmarks.append(benchmark)
            logger.info(f"Added ingestion benchmark: {benchmark.name}")
    
    def add_query_benchmarks(self, data_path: Path):
        """Add query benchmarks for given data."""
        query_types = [
            "aggregation_basic",
            "aggregation_grouped", 
            "filter_complex",
            "join_performance"
        ]
        
        for query_type in query_types:
            benchmark = PolarsQueryBenchmark(self.config, data_path, query_type)
            self.benchmarks.append(benchmark)
            logger.info(f"Added query benchmark: {benchmark.name}")
    
    def get_benchmarks(self) -> List[BaseBenchmark]:
        """Get all configured benchmarks."""
        return self.benchmarks.copy()
    
    def estimate_total_memory(self) -> float:
        """Estimate total memory requirements."""
        return sum(b.estimate_memory_gb() for b in self.benchmarks if hasattr(b, 'estimate_memory_gb'))
    
    def run_all(self) -> List[BenchmarkResult]:
        """Run all benchmarks in the suite."""
        results = []
        
        for benchmark in self.benchmarks:
            logger.info(f"Running benchmark: {benchmark.name}")
            
            try:
                result = benchmark.run()
                results.append(result)
            except Exception as e:
                logger.error(f"Benchmark {benchmark.name} failed: {e}")
                failed_result = BenchmarkResult(
                    name=benchmark.name,
                    status=BenchmarkStatus.FAILED,
                    execution_time=None,
                    error_message=str(e),
                    metadata={}
                )
                results.append(failed_result)
        
        return results