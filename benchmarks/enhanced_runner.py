"""Enhanced benchmark runner with new architecture."""

import argparse
import logging
import sys
from pathlib import Path
from typing import List

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

# Use relative imports within benchmarks module
try:
    from config import BenchmarkConfig
    from orchestrator import EnhancedBenchmarkOrchestrator, run_simple_benchmark
except ImportError:
    # Try absolute imports as fallback
    from benchmarks.config import BenchmarkConfig
    from benchmarks.orchestrator import EnhancedBenchmarkOrchestrator, run_simple_benchmark

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('benchmark_run.log')
    ]
)
logger = logging.getLogger(__name__)

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Enhanced CNPJ Data Benchmark Suite",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run simple benchmark on CSV files
  python enhanced_runner.py --files data/*.csv --engines polars duckdb
  
  # Run with custom configuration
  python enhanced_runner.py --files data/*.csv --memory-limit 16 --threads 8
  
  # Run ingestion only
  python enhanced_runner.py --files data/*.csv --ingestion-only
  
  # Run with parallel execution
  python enhanced_runner.py --files data/*.csv --parallel --max-concurrent 4
  
  # Compare two runs
  python enhanced_runner.py --compare baseline_run comparison_run
  
  # Analyze trends
  python enhanced_runner.py --trends --metric execution_time --days 30
        """
    )
    
    # File inputs
    parser.add_argument(
        '--files', 
        nargs='+', 
        type=Path,
        help='Data files to benchmark (CSV or Parquet)'
    )
    
    parser.add_argument(
        '--engines',
        nargs='+',
        choices=['polars', 'duckdb'],
        default=['polars', 'duckdb'],
        help='Benchmark engines to use'
    )
    
    # Configuration options
    parser.add_argument(
        '--memory-limit',
        type=float,
        default=8.0,
        help='Memory limit in GB (default: 8.0)'
    )
    
    parser.add_argument(
        '--threads',
        type=int,
        help='Number of CPU threads (default: auto-detect)'
    )
    
    parser.add_argument(
        '--chunk-size',
        type=int,
        default=50000,
        help='CSV chunk size (default: 50000)'
    )
    
    # Execution options
    parser.add_argument(
        '--parallel',
        action='store_true',
        help='Enable parallel execution'
    )
    
    parser.add_argument(
        '--max-concurrent',
        type=int,
        default=3,
        help='Maximum concurrent benchmarks (default: 3)'
    )
    
    parser.add_argument(
        '--ingestion-only',
        action='store_true',
        help='Run only ingestion benchmarks'
    )
    
    parser.add_argument(
        '--queries-only',
        action='store_true',
        help='Run only query benchmarks'
    )
    
    # Analysis options
    parser.add_argument(
        '--compare',
        nargs=2,
        metavar=('BASELINE', 'COMPARISON'),
        help='Compare two benchmark runs'
    )
    
    parser.add_argument(
        '--trends',
        action='store_true',
        help='Analyze performance trends'
    )
    
    parser.add_argument(
        '--metric',
        default='execution_time',
        choices=['execution_time', 'memory_usage'],
        help='Metric for trend analysis (default: execution_time)'
    )
    
    parser.add_argument(
        '--days',
        type=int,
        default=30,
        help='Days to analyze for trends (default: 30)'
    )
    
    # Output options
    parser.add_argument(
        '--output-dir',
        type=Path,
        default=Path('benchmark_results'),
        help='Output directory for results (default: benchmark_results)'
    )
    
    parser.add_argument(
        '--run-id',
        help='Custom run ID for this benchmark'
    )
    
    parser.add_argument(
        '--simple',
        action='store_true',
        help='Use simple benchmark mode (minimal configuration)'
    )
    
    return parser.parse_args()

def create_config(args) -> BenchmarkConfig:
    """Create benchmark configuration from arguments."""
    return BenchmarkConfig(
        memory_limit_gb=args.memory_limit,
        max_threads=args.threads if args.threads else 8,
        max_concurrent_benchmarks=args.max_concurrent,
        chunk_size=args.chunk_size,
        verbose=True,
        output_dir=str(args.output_dir)
    )

def detect_cnpj_file_format(file_path: Path) -> str:
    """Detect if a file is a Brazilian CNPJ CSV file based on naming patterns."""
    filename = file_path.name.upper()
    
    # Brazilian CNPJ file patterns
    cnpj_patterns = [
        'SIMPLES', 'ESTABELE', 'EMPRESA', 'SOCIO', 'CNAE', 
        'MOTI', 'MUNIC', 'NATJU', 'PAIS', 'QUALS'
    ]
    
    # Check if it's a CNPJ file
    for pattern in cnpj_patterns:
        if pattern in filename:
            return 'csv'  # Brazilian CNPJ files are CSV format
    
    # Check standard extensions
    if file_path.suffix.lower() in ['.csv', '.parquet']:
        return file_path.suffix.lower()[1:]  # Remove the dot
    
    return 'unknown'

def validate_files(files: List[Path]) -> List[Path]:
    """Validate and filter input files."""
    valid_files = []
    
    for file_path in files:
        if not file_path.exists():
            logger.warning(f"File not found: {file_path}")
            continue
        
        # Use Brazilian CNPJ file detection
        file_format = detect_cnpj_file_format(file_path)
        if file_format not in ['csv', 'parquet']:
            logger.warning(f"Unsupported file type: {file_path} (detected: {file_format})")
            continue
        
        if file_path.stat().st_size == 0:
            logger.warning(f"Empty file: {file_path}")
            continue
        
        valid_files.append(file_path)
        logger.info(f"Valid CNPJ file: {file_path.name} (format: {file_format}, size: {file_path.stat().st_size / (1024**2):.1f}MB)")
    
    return valid_files

def main():
    """Main entry point."""
    args = parse_arguments()
    
    try:
        # Handle analysis operations first
        if args.compare:
            config = create_config(args)
            orchestrator = EnhancedBenchmarkOrchestrator(config, args.output_dir)
            result = orchestrator.compare_runs(args.compare[0], args.compare[1])
            
            if 'error' in result:
                logger.error(f"Comparison failed: {result['error']}")
                return 1
            
            print(f"Comparison completed: {result['comparison_report'].summary}")
            print(f"Improvements: {len(result['comparison_report'].improvements)}")
            print(f"Regressions: {len(result['comparison_report'].regressions)}")
            return 0
        
        if args.trends:
            config = create_config(args)
            orchestrator = EnhancedBenchmarkOrchestrator(config, args.output_dir)
            result = orchestrator.analyze_trends(args.metric, args.days)
            
            if 'error' in result:
                logger.error(f"Trend analysis failed: {result['error']}")
                return 1
            
            if 'trend_analysis' in result:
                trend = result['trend_analysis']
                print(f"Trend Analysis for {args.metric}:")
                print(f"Direction: {trend.trend_direction}")
                print(f"Strength: {trend.trend_strength:.2f}")
                print(f"Recommendation: {trend.recommendation}")
            else:
                print(result['message'])
            return 0
        
        # Validate input files
        if not args.files:
            logger.error("No input files specified")
            return 1
        
        valid_files = validate_files(args.files)
        if not valid_files:
            logger.error("No valid input files found")
            return 1
        
        logger.info(f"Processing {len(valid_files)} files with engines: {args.engines}")
        
        # Run benchmarks
        if args.simple:
            # Simple benchmark mode
            result = run_simple_benchmark(
                data_files=valid_files,
                engines=args.engines,
                config=create_config(args),
                output_dir=args.output_dir
            )
        else:
            # Full orchestrator mode
            config = create_config(args)
            orchestrator = EnhancedBenchmarkOrchestrator(
                config=config,
                results_dir=args.output_dir,
                enable_monitoring=True,
                enable_parallel=args.parallel
            )
            
            # Configure benchmarks
            if not args.queries_only:
                orchestrator.configure_ingestion_benchmarks(valid_files, args.engines)
            
            if not args.ingestion_only and valid_files:
                orchestrator.configure_query_benchmarks(valid_files[0], args.engines)
            
            # Progress callback
            def progress_callback(message: str, progress: float):
                print(f"\r{message} ({progress:.1%})", end='', flush=True)
            
            # Run benchmarks
            result = orchestrator.run_comprehensive_benchmark(
                run_id=args.run_id,
                progress_callback=progress_callback,
                engines=args.engines
            )
            
            print()  # New line after progress
        
        # Handle results
        if 'error' in result:
            logger.error(f"Benchmark failed: {result['error']}")
            return 1
        
        # Print summary
        summary = result.get('summary', {})
        print(f"\n=== Benchmark Results ===")
        print(f"Run ID: {result.get('run_id', 'N/A')}")
        print(f"Total benchmarks: {summary.get('total_benchmarks', 0)}")
        print(f"Successful: {summary.get('successful', 0)}")
        print(f"Failed: {summary.get('failed', 0)}")
        print(f"Success rate: {summary.get('success_rate', 0):.1f}%")
        print(f"Total execution time: {summary.get('total_execution_time', 0):.2f}s")
        
        if summary.get('average_benchmark_time', 0) > 0:
            print(f"Average benchmark time: {summary.get('average_benchmark_time', 0):.2f}s")
            print(f"Fastest benchmark: {summary.get('fastest_benchmark', 0):.2f}s")
            print(f"Slowest benchmark: {summary.get('slowest_benchmark', 0):.2f}s")
        
        print(f"\nResults saved to: {result.get('results_file', 'N/A')}")
        print(f"Report saved to: {result.get('report_file', 'N/A')}")
        
        return 0
        
    except KeyboardInterrupt:
        logger.info("Benchmark interrupted by user")
        return 1
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        return 1

if __name__ == "__main__":
    sys.exit(main())