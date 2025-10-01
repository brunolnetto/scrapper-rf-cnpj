#!/usr/bin/env python3
"""
Polars benchmark implementation for CNPJ processing.
"""

import sys
import os
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

import polars as pl
import time
import psutil
from typing import Optional, Dict, Any, Union
import logging

from .utils import benchmark_context, BenchmarkResult, get_file_size_gb, cleanup_memory

logger = logging.getLogger(__name__)

class PolarsBenchmark:
    """Benchmark Polars for CNPJ processing tasks."""
    
    def __init__(self, 
                 max_threads: Optional[int] = None,
                 streaming: bool = True,
                 chunk_size: int = 50000):
        """
        Initialize Polars benchmark.
        
        Args:
            max_threads: Maximum number of threads for Polars
            streaming: Whether to use streaming execution
            chunk_size: Chunk size for processing
        """
        self.streaming = streaming
        self.chunk_size = chunk_size
        self.max_threads = max_threads
        
        # Note: Polars automatically manages thread pool size
        # Manual thread pool configuration is deprecated in modern versions
        if max_threads:
            logger.info(f"Note: max_threads parameter set to {max_threads} but Polars manages threads automatically")
            
        logger.info(f"Polars setup: streaming={streaming}, chunk_size={chunk_size}")
    
    def benchmark_csv_ingestion(self, 
                               csv_files: list[Path], 
                               output_path: Path,
                               delimiter: str = ";",
                               encoding: str = "utf8-lossy",
                               memory_limit_gb: float = 8.0) -> BenchmarkResult:
        """
        Benchmark CSV ingestion with Polars.
        
        Args:
            csv_files: List of CSV files to ingest
            output_path: Output parquet file path
            delimiter: CSV delimiter
            encoding: File encoding
            
        Returns:
            BenchmarkResult with performance metrics
        """
        if not csv_files:
            raise ValueError("No CSV files provided")
            
        # Calculate input size
        input_size_gb = sum(get_file_size_gb(f) for f in csv_files)
        
        with benchmark_context(f"Polars CSV Ingestion ({input_size_gb:.2f}GB)") as monitor:
            try:
                # Memory safety: Check if file is too large for available memory
                available_memory_gb = psutil.virtual_memory().available / (1024**3)
                if input_size_gb > (memory_limit_gb * 0.8):  # 80% safety margin
                    logger.warning(f"Input size ({input_size_gb:.1f}GB) exceeds memory limit ({memory_limit_gb:.1f}GB), falling back to chunked processing")
                    return self._fallback_to_chunked_processing(csv_files, output_path, delimiter, encoding, monitor)
                
                # Strategy 1: Try lazy scan with streaming
                # CNPJ files don't have .csv extension, so we need to pass actual file paths
                if len(csv_files) == 1:
                    file_pattern = str(csv_files[0])
                else:
                    # For multiple files, Polars can accept a list of paths
                    file_pattern = [str(f) for f in csv_files]
                
                logger.info(f"Starting Polars lazy scan: {len(csv_files)} file(s)")
                
                # Create lazy frame with memory-friendly settings
                lazy_df = pl.scan_csv(
                    file_pattern,
                    separator=delimiter,
                    encoding=encoding,
                    infer_schema_length=min(1000, 100),  # Limit schema inference
                    ignore_errors=True,  # Handle malformed rows
                    has_header=False,  # CNPJ files typically don't have headers
                    low_memory=True  # Enable low memory mode
                )
                
                monitor.update_peak_memory()
                
                # Monitor memory during collection
                memory_before_collect = monitor.get_current_stats()['current_memory_gb']
                
                # Process and collect with streaming
                logger.info("Collecting data with streaming execution")
                
                if self.streaming:
                    # Use streaming with memory monitoring
                    df = lazy_df.collect(streaming=True)
                else:
                    # Check memory before eager collection
                    if memory_before_collect > (memory_limit_gb * 0.6):
                        logger.warning("Memory usage high, switching to streaming mode")
                        df = lazy_df.collect(streaming=True)
                    else:
                        df = lazy_df.collect()
                
                monitor.update_peak_memory()
                
                rows_processed = len(df)
                logger.info(f"Processed {rows_processed:,} rows")
                
                # Write to parquet with memory monitoring
                output_size_gb = None
                compression_ratio = None
                
                if output_path:
                    logger.info(f"Writing to parquet: {output_path}")
                    output_path.parent.mkdir(parents=True, exist_ok=True)
                    
                    # Monitor memory during write
                    memory_before_write = monitor.get_current_stats()['current_memory_gb']
                    
                    df.write_parquet(
                        output_path, 
                        compression="zstd",
                        compression_level=3,  # Balance compression vs speed
                        statistics=True,
                        use_pyarrow=False  # Use Polars native writer
                    )
                    
                    monitor.update_peak_memory()
                    output_size_gb = get_file_size_gb(output_path)
                    compression_ratio = input_size_gb / output_size_gb if output_size_gb > 0 else None
                
                # Clean up DataFrame and force garbage collection
                del df
                cleanup_memory()
                
                # Get final stats
                stats = monitor.finish()
                
                return BenchmarkResult(
                    name=f"Polars_CSV_Ingestion_{'streaming' if self.streaming else 'eager'}",
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
                        'streaming': self.streaming,
                        'chunk_size': self.chunk_size,
                        'threads': pl.threadpool_size(),
                        'delimiter': delimiter,
                        'encoding': encoding,
                        'memory_limit_gb': memory_limit_gb,
                        'memory_safety_used': False
                    }
                )
                
            except MemoryError as e:
                logger.error(f"Polars memory error: {e}")
                # Fallback to chunked processing
                return self._fallback_to_chunked_processing(csv_files, output_path, delimiter, encoding, monitor)
                
            except Exception as e:
                stats = monitor.finish()
                logger.error(f"Polars benchmark failed: {e}")
                
                return BenchmarkResult(
                    name=f"Polars_CSV_Ingestion_{'streaming' if self.streaming else 'eager'}_FAILED",
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
                        'streaming': self.streaming,
                        'chunk_size': self.chunk_size,
                        'threads': pl.threadpool_size(),
                        'memory_limit_gb': memory_limit_gb
                    }
                )
    
    def _fallback_to_chunked_processing(self, 
                                       csv_files: list[Path], 
                                       output_path: Path, 
                                       delimiter: str, 
                                       encoding: str, 
                                       monitor) -> BenchmarkResult:
        """Fallback to chunked processing when memory is constrained."""
        logger.info("Using chunked processing fallback due to memory constraints")
        
        try:
            # Use the existing chunked processing method
            return self.benchmark_chunked_processing(
                csv_files=csv_files,
                output_path=output_path,
                delimiter=delimiter,
                encoding=encoding
            )
        except Exception as e:
            stats = monitor.finish()
            return BenchmarkResult(
                name="Polars_CSV_Ingestion_CHUNKED_FALLBACK_FAILED",
                duration_seconds=stats['duration_seconds'],
                peak_memory_gb=stats['peak_memory_gb'],
                start_memory_gb=stats['start_memory_gb'],
                end_memory_gb=stats['end_memory_gb'],
                memory_delta_gb=stats['memory_delta_gb'],
                cpu_percent=stats['cpu_percent'],
                io_read_gb=stats['io_read_gb'],
                io_write_gb=stats['io_write_gb'],
                errors=f"Chunked fallback failed: {str(e)}",
                metadata={'fallback_used': True}
            )
    
    def benchmark_chunked_processing(self, 
                                   csv_files: list[Path], 
                                   output_path: Path,
                                   delimiter: str = ";",
                                   encoding: str = "utf8-lossy") -> BenchmarkResult:
        """
        Benchmark chunked CSV processing (fallback strategy).
        
        Args:
            csv_files: List of CSV files to process
            output_path: Output parquet file path
            delimiter: CSV delimiter
            encoding: File encoding
            
        Returns:
            BenchmarkResult with performance metrics
        """
        input_size_gb = sum(get_file_size_gb(f) for f in csv_files)
        
        with benchmark_context(f"Polars Chunked Processing ({input_size_gb:.2f}GB)") as monitor:
            try:
                total_rows = 0
                chunk_files = []
                
                # Process each file in chunks
                for i, csv_file in enumerate(csv_files):
                    logger.info(f"Processing file {i+1}/{len(csv_files)}: {csv_file.name}")
                    
                    # Read file in batches
                    batch_reader = pl.read_csv_batched(
                        csv_file,
                        separator=delimiter,
                        encoding=encoding,
                        batch_size=self.chunk_size,
                        ignore_errors=True,
                        has_header=False
                    )
                    
                    file_chunks = []
                    batch_idx = 0
                    
                    # Use next_batches() method to iterate over batches
                    while True:
                        batches = batch_reader.next_batches(1)
                        if not batches:
                            break
                        
                        for batch_df in batches:
                            monitor.update_peak_memory()
                            
                            # Simple processing (you can add transformations here)
                            processed_df = batch_df
                            
                            # Write chunk to temporary parquet
                            chunk_path = output_path.parent / f"chunk_{i}_{batch_idx}.parquet"
                            processed_df.write_parquet(chunk_path, compression="zstd")
                            file_chunks.append(chunk_path)
                            
                            total_rows += len(processed_df)
                            batch_idx += 1
                            
                            # Clean up batch
                            del processed_df
                        
                    chunk_files.extend(file_chunks)
                
                monitor.update_peak_memory()
                
                # Combine chunks into final parquet
                logger.info(f"Combining {len(chunk_files)} chunks into final parquet")
                
                # Read all chunks and combine
                combined_df = pl.concat([
                    pl.read_parquet(chunk_path) 
                    for chunk_path in chunk_files
                ])
                
                monitor.update_peak_memory()
                
                # Write final output
                if output_path:
                    output_path.parent.mkdir(parents=True, exist_ok=True)
                    combined_df.write_parquet(output_path, compression="zstd")
                    
                    # Clean up chunk files
                    for chunk_path in chunk_files:
                        chunk_path.unlink()
                
                output_size_gb = get_file_size_gb(output_path) if output_path else None
                compression_ratio = input_size_gb / output_size_gb if output_size_gb and output_size_gb > 0 else None
                
                # Clean up
                del combined_df
                cleanup_memory()
                
                # Get final stats
                stats = monitor.finish()
                
                return BenchmarkResult(
                    name="Polars_Chunked_Processing",
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
                    rows_processed=total_rows,
                    metadata={
                        'input_size_gb': input_size_gb,
                        'input_files': len(csv_files),
                        'chunk_size': self.chunk_size,
                        'total_chunks': len(chunk_files),
                        'threads': pl.threadpool_size()
                    }
                )
                
            except Exception as e:
                stats = monitor.finish()
                logger.error(f"Polars chunked processing failed: {e}")
                
                # Clean up chunk files on error
                for chunk_path in chunk_files:
                    try:
                        chunk_path.unlink()
                    except:
                        pass
                
                return BenchmarkResult(
                    name="Polars_Chunked_Processing_FAILED",
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
                        'chunk_size': self.chunk_size
                    }
                )
    
    def benchmark_parquet_aggregation(self, 
                                    parquet_path: Path,
                                    result_name: str) -> BenchmarkResult:
        """
        Benchmark aggregation operations on parquet data.
        
        Args:
            parquet_path: Path to parquet file
            result_name: Name for the benchmark
            
        Returns:
            BenchmarkResult with performance metrics
        """
        with benchmark_context(f"Polars Parquet Aggregation ({result_name})") as monitor:
            try:
                # Lazy scan parquet
                lazy_df = pl.scan_parquet(parquet_path)
                
                # Example aggregation - adapt based on your actual columns
                agg_query = (lazy_df
                    .group_by("column_2")  # Assuming this is UF or similar
                    .agg([
                        pl.count().alias("count"),
                        pl.col("column_1").n_unique().alias("unique_values")
                    ])
                    .sort("count", descending=True)
                )
                
                monitor.update_peak_memory()
                
                # Execute with streaming
                result_df = agg_query.collect(streaming=self.streaming)
                
                monitor.update_peak_memory()
                
                rows_processed = len(result_df)
                
                # Clean up
                del result_df
                cleanup_memory()
                
                # Get final stats
                stats = monitor.finish()
                
                return BenchmarkResult(
                    name=f"Polars_Parquet_Aggregation_{result_name}",
                    duration_seconds=stats['duration_seconds'],
                    peak_memory_gb=stats['peak_memory_gb'],
                    start_memory_gb=stats['start_memory_gb'],
                    end_memory_gb=stats['end_memory_gb'],
                    memory_delta_gb=stats['memory_delta_gb'],
                    cpu_percent=stats['cpu_percent'],
                    io_read_gb=stats['io_read_gb'],
                    io_write_gb=stats['io_write_gb'],
                    rows_processed=rows_processed,
                    metadata={
                        'parquet_size_gb': get_file_size_gb(parquet_path),
                        'streaming': self.streaming,
                        'threads': pl.threadpool_size()
                    }
                )
                
            except Exception as e:
                stats = monitor.finish()
                logger.error(f"Polars parquet aggregation failed: {e}")
                
                return BenchmarkResult(
                    name=f"Polars_Parquet_Aggregation_{result_name}_FAILED",
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
                        'streaming': self.streaming
                    }
                )

def run_polars_benchmarks(csv_files: list[Path], 
                         output_dir: Path,
                         max_threads: Optional[int] = None,
                         memory_limit_gb: float = 8.0) -> list[BenchmarkResult]:
    """
    Run comprehensive Polars benchmarks with memory safety.
    
    Args:
        csv_files: List of CSV files to process
        output_dir: Directory for output files
        max_threads: Maximum number of threads
        memory_limit_gb: Memory limit in GB for safety checks
        
    Returns:
        List of benchmark results
    """
    results = []
    
    # Calculate total input size for memory planning
    total_input_size_gb = sum(get_file_size_gb(f) for f in csv_files)
    logger.info(f"Total input size: {total_input_size_gb:.2f}GB, Memory limit: {memory_limit_gb}GB")
    
    # Test both streaming and eager execution (streaming first for safety)
    for streaming in [True, False]:
        benchmark = PolarsBenchmark(
            max_threads=max_threads, 
            streaming=streaming,
            chunk_size=50000
        )
        
        # 1. CSV Ingestion benchmark
        parquet_output = output_dir / f"polars_output_{'streaming' if streaming else 'eager'}.parquet"
        ingestion_result = benchmark.benchmark_csv_ingestion(
            csv_files=csv_files,
            output_path=parquet_output,
            delimiter=";",  # CNPJ files use semicolon
            encoding="utf8-lossy",  # Or "iso-8859-1" if needed
            memory_limit_gb=memory_limit_gb
        )
        results.append(ingestion_result)
        
        # Skip eager mode if streaming already failed due to memory
        if not streaming and ingestion_result.errors and "memory" in ingestion_result.errors.lower():
            logger.warning("Skipping eager mode due to memory constraints in streaming mode")
            break
        
        # 2. Aggregation benchmark (if ingestion succeeded)
        if not ingestion_result.errors and parquet_output.exists():
            agg_result = benchmark.benchmark_parquet_aggregation(
                parquet_path=parquet_output,
                result_name=f"{'streaming' if streaming else 'eager'}"
            )
            results.append(agg_result)
    
    # 3. Chunked processing benchmark (always run as safety fallback)
    chunked_benchmark = PolarsBenchmark(max_threads=max_threads, streaming=True, chunk_size=25000)
    chunked_output = output_dir / "polars_chunked_output.parquet"
    chunked_result = chunked_benchmark.benchmark_chunked_processing(
        csv_files=csv_files,
        output_path=chunked_output,
        delimiter=";",
        encoding="utf8-lossy"
    )
    results.append(chunked_result)
    
    return results