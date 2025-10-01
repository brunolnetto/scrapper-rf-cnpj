#!/usr/bin/env python3
"""
Main benchmark runner comparing DuckDB vs Polars for CNPJ processing.
"""

import sys
import os
from pathlib import Path
import argparse
import logging
from datetime import datetime
import json

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from .utils import BenchmarkResult, save_benchmark_results, cleanup_memory
from .duckdb_benchmark import run_duckdb_benchmarks
from .polars_benchmark import run_polars_benchmarks

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('benchmark.log')
    ]
)
logger = logging.getLogger(__name__)

def find_cnpj_files(data_dir: Path, pattern: str = "*ESTABELE*") -> list[Path]:
    """
    Find CNPJ CSV files in the data directory.
    
    Args:
        data_dir: Directory containing CNPJ data files
        pattern: File pattern to match
        
    Returns:
        List of CSV file paths
    """
    csv_files = []
    
    # Look in EXTRACTED_FILES directory (recursively)
    extracted_dir = data_dir / "EXTRACTED_FILES"
    if extracted_dir.exists():
        csv_files.extend(list(extracted_dir.rglob(pattern)))
    
    # Also look directly in data_dir (recursively)
    csv_files.extend(list(data_dir.rglob(pattern)))
    
    # Filter to only CSV-like files (exclude .zip, .parquet files)
    csv_files = [f for f in csv_files if not f.suffix.lower() in ['.zip', '.parquet']]
    
    # Remove duplicates and sort
    csv_files = sorted(list(set(csv_files)))
    
    logger.info(f"Found {len(csv_files)} CNPJ files matching pattern '{pattern}'")
    for f in csv_files[:5]:  # Show first 5 files
        logger.info(f"  - {f.name} ({f.stat().st_size / (1024**3):.2f}GB)")
    
    if len(csv_files) > 5:
        logger.info(f"  ... and {len(csv_files) - 5} more files")
    
    return csv_files

def generate_benchmark_report(results: list[BenchmarkResult], output_path: Path):
    """Generate a summary report of benchmark results."""
    
    # Group results by type
    duckdb_results = [r for r in results if r.name.startswith("DuckDB")]
    polars_results = [r for r in results if r.name.startswith("Polars")]
    
    report = []
    report.append("# CNPJ Processing Benchmark Report")
    report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report.append("")
    
    # Summary table
    report.append("## Summary")
    report.append("")
    report.append("| Tool | Operation | Duration (s) | Peak Memory (GB) | Compression Ratio | Rows Processed | Status |")
    report.append("|------|-----------|--------------|------------------|-------------------|----------------|--------|")
    
    for result in results:
        status = "✅ Success" if not result.errors else "❌ Failed"
        compression = f"{result.compression_ratio:.2f}x" if result.compression_ratio else "N/A"
        rows = f"{result.rows_processed:,}" if result.rows_processed else "N/A"
        
        report.append(f"| {result.name} | {result.name.split('_')[-1]} | "
                     f"{result.duration_seconds:.2f} | {result.peak_memory_gb:.2f} | "
                     f"{compression} | {rows} | {status} |")
    
    report.append("")
    
    # Detailed results
    report.append("## Detailed Results")
    report.append("")
    
    for result in results:
        report.append(f"### {result.name}")
        report.append("")
        
        if result.errors:
            report.append(f"**Status**: FAILED - {result.errors}")
        else:
            report.append("**Status**: SUCCESS")
        
        report.append("")
        report.append(f"- **Duration**: {result.duration_seconds:.2f} seconds")
        report.append(f"- **Peak Memory**: {result.peak_memory_gb:.2f} GB")
        report.append(f"- **Memory Delta**: {result.memory_delta_gb:.2f} GB")
        report.append(f"- **CPU Usage**: {result.cpu_percent:.1f}%")
        report.append(f"- **I/O Read**: {result.io_read_gb:.2f} GB")
        report.append(f"- **I/O Write**: {result.io_write_gb:.2f} GB")
        
        if result.rows_processed:
            report.append(f"- **Rows Processed**: {result.rows_processed:,}")
        
        if result.compression_ratio:
            report.append(f"- **Compression Ratio**: {result.compression_ratio:.2f}x")
        
        if result.output_size_gb:
            report.append(f"- **Output Size**: {result.output_size_gb:.2f} GB")
        
        # Metadata
        if result.metadata:
            report.append("")
            report.append("**Configuration**:")
            for key, value in result.metadata.items():
                report.append(f"- {key}: {value}")
        
        report.append("")
        report.append("---")
        report.append("")
    
    # Performance comparison
    if duckdb_results and polars_results:
        report.append("## Performance Comparison")
        report.append("")
        
        # Find comparable operations
        duckdb_ingestion = [r for r in duckdb_results if "Ingestion" in r.name and not r.errors]
        polars_ingestion = [r for r in polars_results if "Ingestion" in r.name and not r.errors]
        
        if duckdb_ingestion and polars_ingestion:
            duckdb_best = min(duckdb_ingestion, key=lambda x: x.duration_seconds)
            polars_best = min(polars_ingestion, key=lambda x: x.duration_seconds)
            
            speed_ratio = polars_best.duration_seconds / duckdb_best.duration_seconds
            memory_ratio = polars_best.peak_memory_gb / duckdb_best.peak_memory_gb
            
            report.append("### Ingestion Performance")
            report.append("")
            report.append(f"- **DuckDB Best**: {duckdb_best.duration_seconds:.2f}s, {duckdb_best.peak_memory_gb:.2f}GB")
            report.append(f"- **Polars Best**: {polars_best.duration_seconds:.2f}s, {polars_best.peak_memory_gb:.2f}GB")
            report.append("")
            
            speed_ratio_winner = "Polars" if speed_ratio < 1 else "DuckDB"
            speed_ratio_loser = "DuckDB" if speed_ratio < 1 else "Polars"
            
            report.append(f"[SPEED_RATIO] **{speed_ratio_winner} is {(1/speed_ratio):.2f}x faster** than {speed_ratio_loser}")
            
            memory_ratio_winner = "Polars" if memory_ratio < 1 else "DuckDB"
            memory_ratio_loser = "DuckDB" if memory_ratio < 1 else "Polars"

            report.append(f"[MEMORY_RATIO] **{memory_ratio_winner} uses {(1/memory_ratio):.2f}x less memory** than {memory_ratio_loser}")
    
    # Write report
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(report))
    
    logger.info(f"Benchmark report generated: {output_path}")

def main():
    parser = argparse.ArgumentParser(description="Benchmark DuckDB vs Polars for CNPJ processing")
    parser.add_argument("--data-dir", type=Path, default=Path("data"),
                       help="Directory containing CNPJ data files")
    parser.add_argument("--output-dir", type=Path, default=Path("benchmark_output"),
                       help="Directory for benchmark outputs")
    parser.add_argument("--pattern", default="*ESTABELE*",
                       help="File pattern to match (default: *ESTABELE*)")
    parser.add_argument("--memory-limit", default="8GB",
                       help="Memory limit for DuckDB (default: 8GB)")
    parser.add_argument("--max-files", type=int, default=None,
                       help="Maximum number of files to process (for testing)")
    parser.add_argument("--skip-duckdb", action="store_true",
                       help="Skip DuckDB benchmarks")
    parser.add_argument("--skip-polars", action="store_true",
                       help="Skip Polars benchmarks")
    
    args = parser.parse_args()
    
    logger.info("Starting CNPJ processing benchmarks")
    logger.info(f"Data directory: {args.data_dir}")
    logger.info(f"Output directory: {args.output_dir}")
    logger.info(f"Memory limit: {args.memory_limit}")
    
    # Find CNPJ files
    csv_files = find_cnpj_files(args.data_dir, args.pattern)
    
    if not csv_files:
        logger.error(f"No CNPJ files found in {args.data_dir} with pattern '{args.pattern}'")
        return 1
    
    # Limit files for testing
    if args.max_files:
        csv_files = csv_files[:args.max_files]
        logger.info(f"Limited to {len(csv_files)} files for testing")
    
    # Calculate total input size
    total_size_gb = sum(f.stat().st_size for f in csv_files) / (1024**3)
    logger.info(f"Total input size: {total_size_gb:.2f} GB")
    
    # Create output directory
    args.output_dir.mkdir(parents=True, exist_ok=True)
    
    # Run benchmarks
    all_results = []
    
    try:
        # DuckDB benchmarks
        if not args.skip_duckdb:
            logger.info("[DuckDB] Running DuckDB benchmarks...")
            cleanup_memory()
            
            duckdb_output_dir = args.output_dir / "duckdb"
            duckdb_output_dir.mkdir(exist_ok=True)
            
            duckdb_results = run_duckdb_benchmarks(
                csv_files=csv_files,
                output_dir=duckdb_output_dir,
                memory_limit=args.memory_limit
            )
            all_results.extend(duckdb_results)
            
            logger.info(f"DuckDB benchmarks completed: {len(duckdb_results)} results")
        
        # Polars benchmarks
        if not args.skip_polars:
            logger.info("[Polars] Running Polars benchmarks...")
            cleanup_memory()
            
            polars_output_dir = args.output_dir / "polars"
            polars_output_dir.mkdir(exist_ok=True)
            
            # Calculate memory limit for Polars (more conservative)
            memory_limit_for_polars = float(args.memory_limit.replace('GB', ''))
            
            polars_results = run_polars_benchmarks(
                csv_files=csv_files,
                output_dir=polars_output_dir,
                max_threads=8,  # Adjust based on your system
                memory_limit_gb=memory_limit_for_polars
            )
            all_results.extend(polars_results)
            
            logger.info(f"Polars benchmarks completed: {len(polars_results)} results")
        
        # Save results
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        results_file = args.output_dir / f"benchmark_results_{timestamp}.json"
        save_benchmark_results(all_results, results_file)
        
        # Generate report
        report_file = args.output_dir / f"benchmark_report_{timestamp}.md"
        generate_benchmark_report(all_results, report_file)
        
        # Print summary
        logger.info("[SUCCESS] Benchmarks completed successfully!")
        logger.info(f"Results saved to: {results_file}")
        logger.info(f"Report generated: {report_file}")
        
        # Quick summary
        successful_results = [r for r in all_results if not r.errors]
        failed_results = [r for r in all_results if r.errors]
        
        logger.info(f"Summary: {len(successful_results)} successful, {len(failed_results)} failed")
        
        if successful_results:
            fastest = min(successful_results, key=lambda x: x.duration_seconds)
            most_memory_efficient = min(successful_results, key=lambda x: x.peak_memory_gb)
            
            logger.info(f"🏃 Fastest: {fastest.name} ({fastest.duration_seconds:.2f}s)")
            logger.info(f"🧠 Most memory efficient: {most_memory_efficient.name} ({most_memory_efficient.peak_memory_gb:.2f}GB)")
        
        return 0
        
    except Exception as e:
        logger.error(f"Benchmark failed: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())