#!/usr/bin/env python3
"""
Conversion-focused benchmark comparing DuckDB vs existing Polars conversion service.
Tests CSV → Parquet conversion specifically using your actual conversion patterns.
"""

import sys
import os
from pathlib import Path
import time
import logging
from typing import List, Optional

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from .utils import benchmark_context, BenchmarkResult, get_file_size_gb, cleanup_memory
from .duckdb_benchmark import DuckDBBenchmark

logger = logging.getLogger(__name__)

class ConversionBenchmark:
    """Focused benchmark for CSV → Parquet conversion strategies."""
    
    def __init__(self, memory_limit_gb: float = 8.0):
        self.memory_limit_gb = memory_limit_gb
    
    def benchmark_existing_conversion(self, csv_file: Path, output_dir: Path) -> BenchmarkResult:
        """
        Benchmark your existing conversion service from src/core/services/conversion/
        """
        try:
            # Import your actual conversion service
            from src.core.services.conversion.service import ConversionService
            from src.core.services.conversion.service import MemoryMonitor  
            from src.setup.config import get_config
            
            config = get_config(year=2024, month=12)
            memory_monitor = MemoryMonitor(config)
            conversion_service = ConversionService(config, memory_monitor)
            
        except ImportError as e:
            logger.error(f"Could not import existing conversion service: {e}")
            return BenchmarkResult(
                name="Existing_Conversion_IMPORT_FAILED",
                duration_seconds=0,
                peak_memory_gb=0,
                start_memory_gb=0,
                end_memory_gb=0,
                memory_delta_gb=0,
                cpu_percent=0,
                io_read_gb=0,
                io_write_gb=0,
                errors=f"Import failed: {str(e)}"
            )
        
        input_size_gb = get_file_size_gb(csv_file)
        output_file = output_dir / "existing_conversion" / f"{csv_file.stem}.parquet"
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        with benchmark_context(f"Existing Conversion ({input_size_gb:.2f}GB)") as monitor:
            try:
                # Use your actual conversion logic
                success = conversion_service.convert_csv_to_parquet(
                    csv_path=csv_file,
                    output_path=output_file,
                    table_name=csv_file.stem.lower()
                )
                
                if not success:
                    raise Exception("Conversion service returned False")
                
                # Get final stats
                stats = monitor.finish()
                
                output_size_gb = get_file_size_gb(output_file) if output_file.exists() else None
                compression_ratio = input_size_gb / output_size_gb if output_size_gb and output_size_gb > 0 else None
                
                return BenchmarkResult(
                    name="Existing_Polars_Conversion",
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
                    metadata={
                        'input_size_gb': input_size_gb,
                        'conversion_service': 'existing_polars',
                        'memory_limit_gb': self.memory_limit_gb,
                        'output_file': str(output_file)
                    }
                )
                
            except Exception as e:
                stats = monitor.finish()
                logger.error(f"Existing conversion benchmark failed: {e}")
                
                return BenchmarkResult(
                    name="Existing_Polars_Conversion_FAILED",
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
                        'conversion_service': 'existing_polars',
                        'memory_limit_gb': self.memory_limit_gb
                    }
                )
    
    def benchmark_duckdb_conversion(self, csv_file: Path, output_dir: Path) -> BenchmarkResult:
        """Benchmark DuckDB CSV → Parquet conversion."""
        
        input_size_gb = get_file_size_gb(csv_file)
        output_file = output_dir / "duckdb_conversion" / f"{csv_file.stem}.parquet"
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        with benchmark_context(f"DuckDB Conversion ({input_size_gb:.2f}GB)") as monitor:
            try:
                import duckdb
                
                con = duckdb.connect()
                con.execute(f"PRAGMA memory_limit='{self.memory_limit_gb}GB'")
                con.execute(f"PRAGMA threads=8")
                
                # Handle Brazilian RF encoding and format (adapt to your actual file format)
                con.execute(f"""
                COPY (
                    SELECT * FROM read_csv_auto('{csv_file}', 
                                              delimiter=';',
                                              header=false,
                                              SAMPLE_SIZE=-1)
                ) TO '{output_file}' (FORMAT PARQUET, COMPRESSION ZSTD)
                """)
                
                # Get final stats
                stats = monitor.finish()
                
                output_size_gb = get_file_size_gb(output_file) if output_file.exists() else None
                compression_ratio = input_size_gb / output_size_gb if output_size_gb and output_size_gb > 0 else None
                
                return BenchmarkResult(
                    name="DuckDB_Direct_Conversion",
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
                    metadata={
                        'input_size_gb': input_size_gb,
                        'conversion_service': 'duckdb_direct',
                        'memory_limit_gb': self.memory_limit_gb,
                        'output_file': str(output_file)
                    }
                )
                
            except Exception as e:
                stats = monitor.finish()
                logger.error(f"DuckDB conversion benchmark failed: {e}")
                
                return BenchmarkResult(
                    name="DuckDB_Direct_Conversion_FAILED",
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
                        'conversion_service': 'duckdb_direct',
                        'memory_limit_gb': self.memory_limit_gb
                    }
                )

def run_conversion_benchmarks(csv_files: List[Path], 
                            output_dir: Path,
                            memory_limit_gb: float = 8.0) -> List[BenchmarkResult]:
    """
    Run focused conversion benchmarks.
    
    Args:
        csv_files: List of CSV files to convert
        output_dir: Directory for output files
        memory_limit_gb: Memory limit for benchmarks
        
    Returns:
        List of benchmark results
    """
    results = []
    benchmark = ConversionBenchmark(memory_limit_gb=memory_limit_gb)
    
    for csv_file in csv_files:
        logger.info(f"Benchmarking conversion for: {csv_file.name}")
        
        # 1. Test existing Polars conversion service
        existing_result = benchmark.benchmark_existing_conversion(csv_file, output_dir)
        results.append(existing_result)
        
        # 2. Test DuckDB conversion
        duckdb_result = benchmark.benchmark_duckdb_conversion(csv_file, output_dir)
        results.append(duckdb_result)
        
        # Clean up memory between tests
        cleanup_memory()
    
    return results

def main():
    """Main function for running conversion benchmarks."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Benchmark CSV → Parquet conversion")
    parser.add_argument("--data-dir", type=Path, default=Path("data"),
                       help="Directory containing CNPJ data files")
    parser.add_argument("--output-dir", type=Path, default=Path("conversion_benchmark_output"),
                       help="Directory for benchmark outputs")
    parser.add_argument("--pattern", default="*ESTABELE*",
                       help="File pattern to match")
    parser.add_argument("--memory-limit", default="8GB",
                       help="Memory limit")
    parser.add_argument("--max-files", type=int, default=2,
                       help="Maximum number of files to test")
    
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    
    logger.info("Starting conversion benchmark")
    logger.info(f"Data directory: {args.data_dir}")
    logger.info(f"Pattern: {args.pattern}")
    logger.info(f"Memory limit: {args.memory_limit}")
    
    # Find files
    from benchmarks.run_benchmarks import find_cnpj_files
    csv_files = find_cnpj_files(args.data_dir, args.pattern)
    
    if not csv_files:
        logger.error(f"No files found matching pattern '{args.pattern}' in {args.data_dir}")
        return 1
    
    # Limit files for focused testing
    csv_files = csv_files[:args.max_files]
    logger.info(f"Testing {len(csv_files)} files")
    
    # Create output directory
    args.output_dir.mkdir(parents=True, exist_ok=True)
    
    # Parse memory limit
    memory_limit_gb = float(args.memory_limit.replace('GB', ''))
    
    # Run benchmarks
    results = run_conversion_benchmarks(
        csv_files=csv_files,
        output_dir=args.output_dir,
        memory_limit_gb=memory_limit_gb
    )
    
    # Save results
    from benchmarks.utils import save_benchmark_results
    from datetime import datetime
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results_file = args.output_dir / f"conversion_benchmark_results_{timestamp}.json"
    save_benchmark_results(results, results_file)
    
    # Print summary
    logger.info("🎉 Conversion benchmarks completed!")
    logger.info(f"Results saved to: {results_file}")
    
    successful = [r for r in results if not r.errors]
    failed = [r for r in results if r.errors]
    
    logger.info(f"Summary: {len(successful)} successful, {len(failed)} failed")
    
    if successful:
        # Find fastest conversion method
        fastest = min(successful, key=lambda x: x.duration_seconds)
        most_efficient = min(successful, key=lambda x: x.peak_memory_gb)
        
        logger.info(f"[FASTEST] Fastest: {fastest.name} ({fastest.duration_seconds:.2f}s)")
        logger.info(f"[MEMORY_EFFICIENT] Most memory efficient: {most_efficient.name} ({most_efficient.peak_memory_gb:.2f}GB)")

        # Compare compression ratios
        compression_results = [r for r in successful if r.compression_ratio]
        if compression_results:
            best_compression = max(compression_results, key=lambda x: x.compression_ratio)
            logger.info(f"[COMPRESSION] Best compression: {best_compression.name} ({best_compression.compression_ratio:.2f}x)")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())