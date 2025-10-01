#!/usr/bin/env python3
"""
DuckDB benchmark implementation for CNPJ processing.
"""

import sys
import os
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

import duckdb
import time
from typing import Optional, Dict, Any
import logging

from benchmarks.utils import benchmark_context, BenchmarkResult, get_file_size_gb, cleanup_memory

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
                
                # Create file pattern for DuckDB
                if len(csv_files) == 1:
                    file_pattern = str(csv_files[0])
                else:
                    # Use glob pattern if files are in same directory
                    base_dir = csv_files[0].parent
                    if all(f.parent == base_dir for f in csv_files):
                        file_pattern = str(base_dir / "*.csv")
                    else:
                        # Multiple directories - use list
                        file_pattern = [str(f) for f in csv_files]
                
                # Monitor memory during ingestion
                monitor.update_peak_memory()
                
                # Create table from CSV with automatic schema detection
                logger.info(f"Ingesting CSV files to table '{table_name}'")
                
                if isinstance(file_pattern, list):
                    # Handle multiple files from different directories
                    self.connection.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto(?)", [file_pattern])
                else:
                    # Single pattern or glob
                    self.connection.execute(f"""
                    CREATE TABLE {table_name} AS 
                    SELECT * FROM read_csv_auto('{file_pattern}', 
                                               SAMPLE_SIZE=-1,
                                               DELIMITER=';',
                                               HEADER=false)
                    """)
                
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
    
    # 2. Aggregation benchmarks (if ingestion succeeded)
    if not ingestion_result.errors:
        # Simple count by UF
        agg_result = benchmark.benchmark_aggregation(
            table_name="cnpj_staging",
            query="SELECT uf, COUNT(*) as count FROM cnpj_staging GROUP BY uf ORDER BY count DESC",
            result_name="count_by_uf"
        )
        results.append(agg_result)
        
        # Complex aggregation (if you have these columns in your data)
        try:
            complex_agg_result = benchmark.benchmark_aggregation(
                table_name="cnpj_staging",
                query="""
                SELECT 
                    situacao_cadastral,
                    COUNT(*) as total_empresas,
                    COUNT(DISTINCT cnpj_basico) as unique_cnpj_basico
                FROM cnpj_staging 
                WHERE uf IN ('SP', 'RJ', 'MG')
                GROUP BY situacao_cadastral
                ORDER BY total_empresas DESC
                """,
                result_name="complex_aggregation"
            )
            results.append(complex_agg_result)
        except Exception as e:
            logger.warning(f"Complex aggregation skipped - columns may not exist: {e}")
    
    return results