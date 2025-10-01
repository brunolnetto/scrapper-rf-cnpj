#!/usr/bin/env python3
"""
DuckDB benchmark implementation for CNPJ processing.
"""

import sys
import os
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

# Load .env variables first
from dotenv import load_dotenv
load_dotenv()

import duckdb
import time
from typing import Optional, Dict, Any
import logging

from .utils import benchmark_context, BenchmarkResult, get_file_size_gb, cleanup_memory

logger = logging.getLogger(__name__)

class DuckDBBenchmark:
    """Benchmark DuckDB for CNPJ processing tasks."""
    
    def __init__(self, memory_limit: str = "8GB", threads: int = 8):
        self.memory_limit = memory_limit
        self.threads = threads
        self.connection = None
        
    def setup_connection(self):
        """Setup DuckDB connection with memory limits."""
        self.connection = duckdb.connect()
        self.connection.execute(f"PRAGMA memory_limit='{self.memory_limit}';")
        self.connection.execute(f"PRAGMA threads={self.threads};")
        logger.info(f"DuckDB setup: memory_limit={self.memory_limit}, threads={self.threads}")
        
    def cleanup_connection(self):
        """Clean up connection."""
        if self.connection:
            self.connection.close()
            self.connection = None
        cleanup_memory()
    
    def benchmark_csv_ingestion(self, 
                               csv_files: list[Path], 
                               output_path: Path,
                               table_name: str = "cnpj_data") -> BenchmarkResult:
        """
        Benchmark CSV ingestion to DuckDB table.
        
        Args:
            csv_files: List of CSV files to ingest
            output_path: Output parquet file path
            table_name: Name of the table to create
            
        Returns:
            BenchmarkResult with performance metrics
        """
        if not csv_files:
            raise ValueError("No CSV files provided")
            
        # Calculate input size
        input_size_gb = sum(get_file_size_gb(f) for f in csv_files)
        
        with benchmark_context(f"DuckDB CSV Ingestion ({input_size_gb:.2f}GB)") as monitor:
            try:
                self.setup_connection()
                
                # Create file list for DuckDB (CNPJ files don't have .csv extension)
                file_list = [str(f) for f in csv_files]
                
                # Monitor memory during ingestion
                monitor.update_peak_memory()
                
                # Create table from CSV with automatic schema detection
                logger.info(f"Ingesting {len(file_list)} CSV files to table '{table_name}'")
                
                # Read CSV parameters
                csv_params = """
                    sample_size=10000,
                    delim=';',
                    header=false,
                    encoding='ISO8859_1',
                    ignore_errors=true
                """
                
                if len(file_list) == 1:
                    # Single file
                    self.connection.execute(f"""
                    CREATE TABLE {table_name} AS 
                    SELECT * FROM read_csv('{file_list[0]}', {csv_params})
                    """)
                else:
                    # Multiple files - process one by one to avoid memory issues
                    # First file creates the table
                    logger.info(f"Creating table from first file: {Path(file_list[0]).name}")
                    self.connection.execute(f"""
                    CREATE TABLE {table_name} AS 
                    SELECT * FROM read_csv('{file_list[0]}', {csv_params})
                    """)
                    
                    monitor.update_peak_memory()
                    
                    # Remaining files are inserted
                    for i, file_path in enumerate(file_list[1:], 2):
                        logger.info(f"Inserting file {i}/{len(file_list)}: {Path(file_path).name}")
                        self.connection.execute(f"""
                        INSERT INTO {table_name} 
                        SELECT * FROM read_csv('{file_path}', {csv_params})
                        """)
                        monitor.update_peak_memory()
                
                monitor.update_peak_memory()
                
                # Get row count
                row_count_result = self.connection.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
                rows_processed = row_count_result[0] if row_count_result else 0
                
                # Export to parquet if requested
                output_size_gb = None
                compression_ratio = None
                
                if output_path:
                    logger.info(f"Exporting to parquet: {output_path}")
                    output_path.parent.mkdir(parents=True, exist_ok=True)
                    
                    self.connection.execute(f"""
                    COPY {table_name} TO '{output_path}' 
                    (FORMAT PARQUET, COMPRESSION 'ZSTD')
                    """)
                    
                    monitor.update_peak_memory()
                    output_size_gb = get_file_size_gb(output_path)
                    compression_ratio = input_size_gb / output_size_gb if output_size_gb > 0 else None
                
                # Get final stats
                stats = monitor.finish()
                
                return BenchmarkResult(
                    name=f"DuckDB_CSV_Ingestion_{table_name}",
                    duration_seconds=stats['duration_seconds'],
                    peak_memory_gb=stats['peak_memory_gb'],
                    start_memory_gb=stats['start_memory_gb'],
                    end_memory_gb=stats['end_memory_gb'],
                    memory_delta_gb=stats['memory_delta_gb'],
                    cpu_percent=stats['cpu_percent'],
                    io_read_gb=stats['io_read_gb'],
                    io_write_gb=stats['io_write_gb'],
                    output_size_gb=output_size_gb,
                    compression_ratio=compression_ratio,
                    rows_processed=rows_processed,
                    metadata={
                        'input_size_gb': input_size_gb,
                        'input_files': len(csv_files),
                        'memory_limit': self.memory_limit,
                        'threads': self.threads,
                        'table_name': table_name
                    }
                )
                
            except Exception as e:
                stats = monitor.finish()
                logger.error(f"DuckDB benchmark failed: {e}")
                
                return BenchmarkResult(
                    name=f"DuckDB_CSV_Ingestion_{table_name}_FAILED",
                    duration_seconds=stats['duration_seconds'],
                    peak_memory_gb=stats['peak_memory_gb'],
                    start_memory_gb=stats['start_memory_gb'],
                    end_memory_gb=stats['end_memory_gb'],
                    memory_delta_gb=stats['memory_delta_gb'],
                    cpu_percent=stats['cpu_percent'],
                    io_read_gb=stats['io_read_gb'],
                    io_write_gb=stats['io_write_gb'],
                    errors=str(e),
                    metadata={
                        'input_size_gb': input_size_gb,
                        'memory_limit': self.memory_limit,
                        'threads': self.threads
                    }
                )
            
            finally:
                self.cleanup_connection()
    
    def benchmark_aggregation(self, 
                             table_name: str,
                             query: str,
                             result_name: str) -> BenchmarkResult:
        """
        Benchmark SQL aggregation query.
        
        Args:
            table_name: Name of the table to query
            query: SQL query to execute
            result_name: Name for the benchmark result
            
        Returns:
            BenchmarkResult with performance metrics
        """
        with benchmark_context(f"DuckDB Aggregation ({result_name})") as monitor:
            try:
                self.setup_connection()
                
                # Execute query
                logger.info(f"Executing aggregation query: {result_name}")
                result = self.connection.execute(query).fetchall()
                
                monitor.update_peak_memory()
                
                # Get final stats
                stats = monitor.finish()
                
                return BenchmarkResult(
                    name=f"DuckDB_Aggregation_{result_name}",
                    duration_seconds=stats['duration_seconds'],
                    peak_memory_gb=stats['peak_memory_gb'],
                    start_memory_gb=stats['start_memory_gb'],
                    end_memory_gb=stats['end_memory_gb'],
                    memory_delta_gb=stats['memory_delta_gb'],
                    cpu_percent=stats['cpu_percent'],
                    io_read_gb=stats['io_read_gb'],
                    io_write_gb=stats['io_write_gb'],
                    rows_processed=len(result),
                    metadata={
                        'query': query,
                        'table_name': table_name,
                        'memory_limit': self.memory_limit,
                        'threads': self.threads
                    }
                )
                
            except Exception as e:
                stats = monitor.finish()
                logger.error(f"DuckDB aggregation benchmark failed: {e}")
                
                return BenchmarkResult(
                    name=f"DuckDB_Aggregation_{result_name}_FAILED",
                    duration_seconds=stats['duration_seconds'],
                    peak_memory_gb=stats['peak_memory_gb'],
                    start_memory_gb=stats['start_memory_gb'],
                    end_memory_gb=stats['end_memory_gb'],
                    memory_delta_gb=stats['memory_delta_gb'],
                    cpu_percent=stats['cpu_percent'],
                    io_read_gb=stats['io_read_gb'],
                    io_write_gb=stats['io_write_gb'],
                    errors=str(e),
                    metadata={
                        'query': query,
                        'memory_limit': self.memory_limit,
                        'threads': self.threads
                    }
                )
            
            finally:
                self.cleanup_connection()
                
    def benchmark_aggregation_with_connection(self,
                                            connection,
                                            query: str,
                                            result_name: str) -> BenchmarkResult:
        """
        Benchmark aggregation query using an external database connection.
        
        Args:
            connection: Database connection (e.g., SQLAlchemy connection)
            query: SQL query to execute
            result_name: Name for the benchmark result
            
        Returns:
            BenchmarkResult with performance metrics
        """
        from .utils import benchmark_context
        from sqlalchemy import text
        
        with benchmark_context() as monitor:
            try:
                # Execute query
                logger.info(f"Executing aggregation query on PostgreSQL: {result_name}")
                result = connection.execute(text(query)).fetchall()
                
                monitor.update_peak_memory()
                
                # Get final stats
                stats = monitor.finish()
                
                return BenchmarkResult(
                    name=f"PostgreSQL_Aggregation_{result_name}",
                    duration_seconds=stats['duration_seconds'],
                    peak_memory_gb=stats['peak_memory_gb'],
                    start_memory_gb=stats['start_memory_gb'],
                    end_memory_gb=stats['end_memory_gb'],
                    memory_delta_gb=stats['memory_delta_gb'],
                    cpu_percent=stats['cpu_percent'],
                    io_read_gb=stats['io_read_gb'],
                    io_write_gb=stats['io_write_gb'],
                    rows_processed=len(result),
                    metadata={
                        'query': query,
                        'connection_type': 'PostgreSQL'
                    }
                )
                
            except Exception as e:
                stats = monitor.finish()
                logger.error(f"PostgreSQL aggregation benchmark failed: {e}")
                
                return BenchmarkResult(
                    name=f"PostgreSQL_Aggregation_{result_name}_FAILED",
                    duration_seconds=stats['duration_seconds'],
                    peak_memory_gb=stats['peak_memory_gb'],
                    start_memory_gb=stats['start_memory_gb'],
                    end_memory_gb=stats['end_memory_gb'],
                    memory_delta_gb=stats['memory_delta_gb'],
                    cpu_percent=stats['cpu_percent'],
                    io_read_gb=stats['io_read_gb'],
                    io_write_gb=stats['io_write_gb'],
                    errors=str(e),
                    metadata={
                        'query': query,
                        'connection_type': 'PostgreSQL'
                    }
                )

def run_duckdb_benchmarks(csv_files: list[Path], 
                         output_dir: Path,
                         memory_limit: str = "8GB") -> list[BenchmarkResult]:
    """
    Run comprehensive DuckDB benchmarks.
    
    Args:
        csv_files: List of CSV files to process
        output_dir: Directory for output files
        memory_limit: Memory limit for DuckDB
        
    Returns:
        List of benchmark results
    """
    results = []
    benchmark = DuckDBBenchmark(memory_limit=memory_limit)
    
    # 1. CSV Ingestion benchmark
    parquet_output = output_dir / "duckdb_output.parquet"
    ingestion_result = benchmark.benchmark_csv_ingestion(
        csv_files=csv_files,
        output_path=parquet_output,
        table_name="cnpj_staging"
    )
    results.append(ingestion_result)
    
    # 2. Aggregation benchmarks using PostgreSQL database connection
    # Note: These benchmarks now connect to the actual PostgreSQL database
    # to test aggregation performance on already loaded data
    try:
        from pathlib import Path
        import sys
        sys.path.append(str(Path(__file__).parent.parent))
        from src.setup.config import get_config
        from src.database.engine import get_database_engine
        from sqlalchemy import text
        
        config = get_config()
        engine = get_database_engine(config.databases['main'])
        
        # Test with estabelecimento table (most common table with UF data)
        with engine.connect() as conn:
            # Check if estabelecimento table exists and has data
            result = conn.execute(text("SELECT COUNT(*) FROM estabelecimento LIMIT 1"))
            row = result.fetchone()
            if row and row[0] > 0:
                # Simple count by UF using actual table
                agg_result = benchmark.benchmark_aggregation_with_connection(
                    connection=conn,
                    query="SELECT uf, COUNT(*) as count FROM estabelecimento GROUP BY uf ORDER BY count DESC LIMIT 10",
                    result_name="count_by_uf_estabelecimento"
                )
                results.append(agg_result)
                
                # Complex aggregation with joins
                complex_agg_result = benchmark.benchmark_aggregation_with_connection(
                    connection=conn,
                    query="""
                    SELECT 
                        e.situacao_cadastral,
                        COUNT(*) as total_estabelecimentos,
                        COUNT(DISTINCT e.cnpj_basico) as unique_empresas
                    FROM estabelecimento e
                    WHERE e.uf IN ('SP', 'RJ', 'MG') 
                    GROUP BY e.situacao_cadastral
                    ORDER BY total_estabelecimentos DESC
                    LIMIT 10
                    """,
                    result_name="complex_aggregation_estabelecimento"
                )
                results.append(complex_agg_result)
            else:
                logger.warning("No data found in estabelecimento table, skipping aggregation benchmarks")
                
    except Exception as e:
        logger.warning(f"Could not connect to PostgreSQL for aggregation benchmarks: {e}")
        logger.info("Aggregation benchmarks will be skipped")
    
    return results