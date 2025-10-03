"""Enhanced DuckDB benchmark with new architecture."""

import time
from pathlib import Path
from typing import Dict, Any, List, Optional
import logging
import gc

try:
    import duckdb
    DUCKDB_AVAILABLE = True
except ImportError:
    DUCKDB_AVAILABLE = False
    logging.warning("DuckDB not available - DuckDB benchmarks will be skipped")
import sys 

from .base import BaseBenchmark, IngestionBenchmark, QueryBenchmark
from .config import BenchmarkConfig
from .utils import BenchmarkResult, BenchmarkStatus, estimate_file_memory_requirements, check_memory_safety, detect_cnpj_file_format
from .monitoring import RealTimeMonitor

logger = logging.getLogger(__name__)

project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

class DuckDBIngestionBenchmark(IngestionBenchmark):
    """DuckDB data ingestion benchmark with enhanced configuration."""
    
    def __init__(self, config: BenchmarkConfig, file_path: Path, table_name: str):
        super().__init__(config)
        self.file_path = file_path
        self.table_name = table_name
        self.name = f"duckdb_ingestion_{table_name}"
        self.connection: Optional[duckdb.DuckDBPyConnection] = None
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
        return estimate_file_memory_requirements(self.file_path, operation_type="duckdb_csv")
    
    def prepare_environment(self) -> bool:
        """Prepare DuckDB environment for benchmark execution."""
        # Check memory availability
        required_memory = self.estimate_memory_gb()
        if not check_memory_safety(required_memory, safety_margin=0.3):
            logger.warning(f"Insufficient memory for {self.name}: requires {required_memory:.1f}GB")
            return False
        
        # Start monitoring
        self.monitor.start(self.config.memory_limit_gb)
        
        try:
            # Create DuckDB connection with optimized settings
            if not DUCKDB_AVAILABLE:
                raise RuntimeError("DuckDB not available")
                
            self.connection = duckdb.connect(":memory:")
            
            # Configure DuckDB for optimal performance
            self.connection.execute(f"SET threads = {self.config.max_threads}")
            self.connection.execute(f"SET memory_limit = '{int(self.config.memory_limit_gb * 1024)}MB'")
            self.connection.execute("SET enable_progress_bar = false")
            self.connection.execute("SET preserve_insertion_order = false")
            
            # Set CSV-specific configurations
            self.connection.execute("SET autodetect_csv_parameters = true")
            self.connection.execute("SET csv_buffer_size = '32MB'")
            
            logger.info(f"DuckDB connection configured for {self.name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to prepare DuckDB environment: {e}")
            if self.connection:
                try:
                    self.connection.close()
                except:
                    pass
                self.connection = None
            return False
    
    def run_ingestion(self) -> Dict[str, Any]:
        """Run the DuckDB ingestion benchmark."""
        if self.connection is None:
            raise RuntimeError("DuckDB connection not prepared")
        
        start_time = time.time()
        
        try:
            # Enhanced CSV reading with proper configuration
            csv_options = {
                "delimiter": self.config.csv_delimiter,
                "header": True,
                "auto_detect": True,
                "ignore_errors": False,
                "max_line_size": 1024 * 1024,  # 1MB max line size
                "normalize_names": True,
                "nullstr": ["", "NULL", "null"]
            }
            
            # Build CSV read query with options
            options_str = ", ".join([f"{k} = {repr(v)}" for k, v in csv_options.items()])
            
            # Create table from CSV
            create_query = f"""
                CREATE TABLE {self.table_name} AS 
                SELECT * FROM read_csv_auto('{self.file_path}', {options_str})
            """
            
            logger.info(f"Creating DuckDB table: {self.table_name}")
            self.connection.execute(create_query)
            
            # Get table statistics
            stats_query = f"""
                SELECT 
                    COUNT(*) as row_count,
                    COUNT(DISTINCT column_name) as column_count
                FROM information_schema.columns 
                WHERE table_name = '{self.table_name}'
            """
            
            stats_result = self.connection.execute(stats_query).fetchone()
            row_count_query = f"SELECT COUNT(*) FROM {self.table_name}"
            row_count = self.connection.execute(row_count_query).fetchone()[0]
            
            # Get memory usage
            memory_query = "SELECT current_setting('memory_limit')"
            memory_limit = self.connection.execute(memory_query).fetchone()[0]
            
            # Estimate actual memory usage (DuckDB doesn't expose this directly)
            file_size_mb = self.file_path.stat().st_size / (1024 * 1024)
            estimated_memory = file_size_mb * 0.8  # Conservative estimate
            
            execution_time = time.time() - start_time
            
            return {
                "status": BenchmarkStatus.COMPLETED,
                "execution_time": execution_time,
                "rows_processed": row_count,
                "columns_processed": stats_result[1] if stats_result else 0,
                "peak_memory_mb": estimated_memory,
                "file_size_mb": file_size_mb,
                "table_name": self.table_name
            }
            
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"DuckDB ingestion failed: {e}")
            
            return {
                "status": BenchmarkStatus.FAILED,
                "execution_time": execution_time,
                "error_message": str(e),
                "rows_processed": 0,
                "columns_processed": 0
            }
    
    def cleanup_environment(self):
        """Clean up DuckDB environment."""
        if self.connection:
            try:
                # Drop table if it exists
                self.connection.execute(f"DROP TABLE IF EXISTS {self.table_name}")
                self.connection.close()
            except Exception as e:
                logger.warning(f"Error cleaning up DuckDB: {e}")
            finally:
                self.connection = None
        
        self.monitor.stop()
        gc.collect()

class DuckDBQueryBenchmark(QueryBenchmark):
    """DuckDB query benchmark with optimized SQL execution."""
    
    def __init__(self, config: BenchmarkConfig, data_path: Path, query_name: str, table_name: str = "benchmark_data"):
        super().__init__(config)
        self.data_path = data_path
        self.query_name = query_name
        self.table_name = table_name
        self.name = f"duckdb_query_{query_name}"
        self.connection: Optional[duckdb.DuckDBPyConnection] = None
    
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
            # Check if DuckDB is available
            if not DUCKDB_AVAILABLE:
                logger.error("DuckDB not available for query benchmarks")
                return False
                
            # Create DuckDB connection
            self.connection = duckdb.connect(":memory:")
            
            # Configure DuckDB
            self.connection.execute(f"SET threads = {self.config.max_threads}")
            self.connection.execute(f"SET memory_limit = '{int(self.config.memory_limit_gb * 1024)}MB'")
            
            # Detect file format using CNPJ-aware detection
            file_format = detect_cnpj_file_format(self.data_path)
            
            if file_format == 'parquet':
                load_query = f"CREATE TABLE {self.table_name} AS SELECT * FROM '{self.data_path}'"
            elif file_format == 'csv':
                load_query = f"""
                    CREATE TABLE {self.table_name} AS 
                    SELECT * FROM read_csv_auto('{self.data_path}', 
                        delimiter = '{self.config.csv_delimiter}',
                        header = false,
                        auto_detect = true,
                        ignore_errors = true)
                """
            else:
                raise ValueError(f"Unsupported file format: {file_format} for {self.data_path}")
            
            self.connection.execute(load_query)
            
            # Get table info
            count_result = self.connection.execute(f"SELECT COUNT(*) FROM {self.table_name}").fetchone()
            logger.info(f"Loaded data for queries: {count_result[0]} rows")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to prepare DuckDB query environment: {e}")
            return False
    
    def run_query(self) -> Dict[str, Any]:
        """Run the specific query benchmark."""
        if self.connection is None:
            raise RuntimeError("DuckDB connection not prepared")
        
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
            elif self.query_name == "window_functions":
                result = self._run_window_functions()
            else:
                raise ValueError(f"Unknown query: {self.query_name}")
            
            execution_time = time.time() - start_time
            result_rows = len(result) if result else 0
            
            return {
                "status": BenchmarkStatus.COMPLETED,
                "execution_time": execution_time,
                "result_rows": result_rows,
                "query_type": self.query_name
            }
            
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"Query {self.query_name} failed: {e}")
            
            return {
                "status": BenchmarkStatus.FAILED,
                "execution_time": execution_time,
                "error_message": str(e)
            }
    
    def _run_basic_aggregation(self) -> List:
        """Basic aggregation benchmark."""
        query = f"""
            SELECT 
                COUNT(*) as total_rows,
                COUNT(DISTINCT *) as distinct_rows
            FROM {self.table_name}
        """
        return self.connection.execute(query).fetchall()
    
    def _run_grouped_aggregation(self) -> List:
        """Grouped aggregation benchmark."""
        # Get first column for grouping
        columns_query = f"PRAGMA table_info('{self.table_name}')"
        columns = self.connection.execute(columns_query).fetchall()
        first_col = columns[0][1] if columns else "column1"
        
        query = f"""
            SELECT 
                {first_col},
                COUNT(*) as count,
                COUNT(DISTINCT *) as distinct_count
            FROM {self.table_name}
            GROUP BY {first_col}
            ORDER BY count DESC
            LIMIT 100
        """
        return self.connection.execute(query).fetchall()
    
    def _run_complex_filter(self) -> List:
        """Complex filtering benchmark."""
        # Get column information
        columns_query = f"PRAGMA table_info('{self.table_name}')"
        columns = self.connection.execute(columns_query).fetchall()
        
        if len(columns) >= 2:
            col1, col2 = columns[0][1], columns[1][1]
            query = f"""
                SELECT * FROM {self.table_name}
                WHERE {col1} IS NOT NULL 
                AND {col2} IS NOT NULL
                AND LENGTH(CAST({col1} AS VARCHAR)) > 3
                LIMIT 1000
            """
        else:
            col1 = columns[0][1] if columns else "column1"
            query = f"""
                SELECT * FROM {self.table_name}
                WHERE {col1} IS NOT NULL
                LIMIT 1000
            """
        
        return self.connection.execute(query).fetchall()
    
    def _run_join_performance(self) -> List:
        """Join performance benchmark."""
        # Create a temporary table for self-join
        temp_table = f"{self.table_name}_temp"
        
        # Get first column for join
        columns_query = f"PRAGMA table_info('{self.table_name}')"
        columns = self.connection.execute(columns_query).fetchall()
        join_col = columns[0][1] if columns else "column1"
        
        # Create subset for join
        self.connection.execute(f"""
            CREATE TEMP TABLE {temp_table} AS 
            SELECT DISTINCT {join_col} 
            FROM {self.table_name} 
            LIMIT 1000
        """)
        
        # Perform join
        query = f"""
            SELECT COUNT(*) as join_count
            FROM {self.table_name} t1
            INNER JOIN {temp_table} t2 ON t1.{join_col} = t2.{join_col}
        """
        
        result = self.connection.execute(query).fetchall()
        
        # Clean up temp table
        self.connection.execute(f"DROP TABLE {temp_table}")
        
        return result
    
    def _run_window_functions(self) -> List:
        """Window functions benchmark."""
        # Get first column
        columns_query = f"PRAGMA table_info('{self.table_name}')"
        columns = self.connection.execute(columns_query).fetchall()
        col1 = columns[0][1] if columns else "column1"
        
        query = f"""
            SELECT 
                {col1},
                ROW_NUMBER() OVER (ORDER BY {col1}) as row_num,
                COUNT(*) OVER () as total_count,
                LAG({col1}) OVER (ORDER BY {col1}) as prev_value
            FROM {self.table_name}
            LIMIT 1000
        """
        return self.connection.execute(query).fetchall()
    
    def cleanup_environment(self):
        """Clean up query environment."""
        if self.connection:
            try:
                self.connection.execute(f"DROP TABLE IF EXISTS {self.table_name}")
                self.connection.close()
            except Exception as e:
                logger.warning(f"Error cleaning up DuckDB query environment: {e}")
            finally:
                self.connection = None
        gc.collect()

class DuckDBBenchmarkSuite:
    """Complete DuckDB benchmark suite."""
    
    def __init__(self, config: BenchmarkConfig):
        self.config = config
        self.benchmarks: List[BaseBenchmark] = []
    
    def add_ingestion_benchmarks(self, data_files: List[Path]):
        """Add ingestion benchmarks for given files."""
        for file_path in data_files:
            table_name = f"tbl_{file_path.stem.replace('-', '_')}"
            benchmark = DuckDBIngestionBenchmark(self.config, file_path, table_name)
            self.benchmarks.append(benchmark)
            logger.info(f"Added DuckDB ingestion benchmark: {benchmark.name}")
    
    def add_query_benchmarks(self, data_path: Path):
        """Add query benchmarks for given data."""
        query_types = [
            "aggregation_basic",
            "aggregation_grouped", 
            "filter_complex",
            "join_performance",
            "window_functions"
        ]
        
        table_name = f"tbl_{data_path.stem.replace('-', '_')}"
        
        for query_type in query_types:
            benchmark = DuckDBQueryBenchmark(self.config, data_path, query_type, table_name)
            self.benchmarks.append(benchmark)
            logger.info(f"Added DuckDB query benchmark: {benchmark.name}")
    
    def get_benchmarks(self) -> List[BaseBenchmark]:
        """Get all configured benchmarks."""
        return self.benchmarks.copy()
    
    def run_all(self) -> List[BenchmarkResult]:
        """Run all benchmarks in the suite."""
        results = []
        
        for benchmark in self.benchmarks:
            logger.info(f"Running DuckDB benchmark: {benchmark.name}")
            
            try:
                result = benchmark.run()
                results.append(result)
            except Exception as e:
                logger.error(f"DuckDB benchmark {benchmark.name} failed: {e}")
                failed_result = BenchmarkResult(
                    name=benchmark.name,
                    status=BenchmarkStatus.FAILED,
                    execution_time=None,
                    error_message=str(e),
                    metadata={}
                )
                results.append(failed_result)
        
        return results