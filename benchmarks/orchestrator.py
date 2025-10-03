"""Enhanced benchmark orchestrator with comprehensive features."""

import time
import asyncio
from pathlib import Path
from typing import List, Dict, Any, Optional, Callable
import logging


from .config import BenchmarkConfig
from .utils import BenchmarkResult, BenchmarkStatus
from .analysis import BenchmarkAnalyzer
from .monitoring import RealTimeMonitor
from .parallel import ParallelExecutor, AsyncBenchmarkRunner

# Import enhanced benchmark implementations
try:
    from polars_enhanced import PolarsBenchmarkSuite
    POLARS_AVAILABLE = True
except ImportError:
    try:
        from benchmarks.polars_enhanced import PolarsBenchmarkSuite  
        POLARS_AVAILABLE = True
    except ImportError:
        POLARS_AVAILABLE = False
        logging.warning("Polars not available - skipping Polars benchmarks")

try:
    from duckdb_enhanced import DuckDBBenchmarkSuite
    DUCKDB_AVAILABLE = True
except ImportError:
    try:
        from benchmarks.duckdb_enhanced import DuckDBBenchmarkSuite
        DUCKDB_AVAILABLE = True
    except ImportError:
        DUCKDB_AVAILABLE = False
        logging.warning("DuckDB not available - skipping DuckDB benchmarks")

logger = logging.getLogger(__name__)

class EnhancedBenchmarkOrchestrator:
    """Orchestrate comprehensive benchmark execution with all enhancements."""
    
    def __init__(
        self,
        config: BenchmarkConfig,
        results_dir: Path = Path("benchmark_results"),
        enable_monitoring: bool = True,
        enable_parallel: bool = True
    ):
        self.config = config
        self.results_dir = Path(results_dir)
        self.results_dir.mkdir(exist_ok=True)
        
        # Initialize components
        self.analyzer = BenchmarkAnalyzer(self.results_dir)
        self.monitor = RealTimeMonitor() if enable_monitoring else None
        self.executor = ParallelExecutor(config, self.monitor) if enable_parallel else None
        self.async_runner = AsyncBenchmarkRunner(self.executor) if self.executor else None
        
        # Benchmark suites
        self.benchmark_suites: Dict[str, Any] = {}
        self._initialize_suites()
    
    def _initialize_suites(self):
        """Initialize available benchmark suites."""
        if POLARS_AVAILABLE:
            self.benchmark_suites['polars'] = PolarsBenchmarkSuite(self.config)
            logger.info("Initialized Polars benchmark suite")
        
        if DUCKDB_AVAILABLE:
            self.benchmark_suites['duckdb'] = DuckDBBenchmarkSuite(self.config)
            logger.info("Initialized DuckDB benchmark suite")
        
        if not self.benchmark_suites:
            logger.warning("No benchmark suites available - install polars and/or duckdb")
    
    def configure_ingestion_benchmarks(self, data_files: List[Path], engines: List[str] = None):
        """Configure ingestion benchmarks for specified engines."""
        if engines is None:
            engines = list(self.benchmark_suites.keys())
        
        for engine in engines:
            if engine in self.benchmark_suites:
                self.benchmark_suites[engine].add_ingestion_benchmarks(data_files)
                logger.info(f"Configured ingestion benchmarks for {engine}: {len(data_files)} files")
    
    def configure_query_benchmarks(self, data_path: Path, engines: List[str] = None):
        """Configure query benchmarks for specified engines."""
        if engines is None:
            engines = list(self.benchmark_suites.keys())
        
        for engine in engines:
            if engine in self.benchmark_suites:
                self.benchmark_suites[engine].add_query_benchmarks(data_path)
                logger.info(f"Configured query benchmarks for {engine}")
    
    def run_comprehensive_benchmark(
        self,
        run_id: str = None,
        progress_callback: Optional[Callable[[str, float], None]] = None,
        engines: List[str] = None
    ) -> Dict[str, Any]:
        """Run comprehensive benchmark suite with all enhancements."""
        
        if run_id is None:
            run_id = f"benchmark_{int(time.time())}"
        
        if engines is None:
            engines = list(self.benchmark_suites.keys())
        
        logger.info(f"Starting comprehensive benchmark run: {run_id}")
        logger.info(f"Engines: {engines}")
        
        # Collect all benchmarks
        all_benchmarks = []
        for engine in engines:
            if engine in self.benchmark_suites:
                suite_benchmarks = self.benchmark_suites[engine].get_benchmarks()
                all_benchmarks.extend(suite_benchmarks)
                logger.info(f"Added {len(suite_benchmarks)} benchmarks from {engine}")
        
        if not all_benchmarks:
            logger.error("No benchmarks to run")
            return {"error": "No benchmarks configured"}
        
        start_time = time.time()
        
        try:
            # Run benchmarks
            if self.executor and len(all_benchmarks) > 1:
                logger.info("Running benchmarks in parallel")
                results = self.executor.execute_parallel(all_benchmarks, progress_callback)
            else:
                logger.info("Running benchmarks sequentially")
                results = self._run_sequential(all_benchmarks, progress_callback)
            
            execution_time = time.time() - start_time
            
            # Save results
            results_file = self.analyzer.save_results(results, run_id)
            
            # Generate report
            report_file = self.results_dir / f"report_{run_id}.md"
            report_text = self.analyzer.generate_report(results, report_file)
            
            # Calculate summary statistics
            summary = self._generate_summary(results, execution_time)
            
            logger.info(f"Benchmark run completed: {len(results)} benchmarks in {execution_time:.2f}s")
            
            return {
                "run_id": run_id,
                "results": results,
                "results_file": str(results_file),
                "report_file": str(report_file),
                "summary": summary,
                "execution_time": execution_time
            }
            
        except Exception as e:
            logger.error(f"Benchmark run failed: {e}")
            return {"error": str(e), "run_id": run_id}
    
    async def run_async_benchmark(
        self,
        run_id: str = None,
        progress_callback: Optional[Callable[[str, float], None]] = None,
        engines: List[str] = None
    ) -> Dict[str, Any]:
        """Run benchmarks asynchronously."""
        if not self.async_runner:
            logger.error("Async runner not available")
            return {"error": "Async execution not enabled"}
        
        # Collect all benchmarks
        all_benchmarks = []
        for engine in engines or self.benchmark_suites.keys():
            if engine in self.benchmark_suites:
                suite_benchmarks = self.benchmark_suites[engine].get_benchmarks()
                all_benchmarks.extend(suite_benchmarks)
        
        if not all_benchmarks:
            return {"error": "No benchmarks configured"}
        
        logger.info(f"Starting async benchmark run: {run_id or 'auto'}")
        
        # Run asynchronously
        results = await self.async_runner.run_benchmarks_async(all_benchmarks, progress_callback)
        
        # Process results same as sync version
        if run_id is None:
            run_id = f"async_benchmark_{int(time.time())}"
        
        results_file = self.analyzer.save_results(results, run_id)
        report_file = self.results_dir / f"report_{run_id}.md"
        report_text = self.analyzer.generate_report(results, report_file)
        
        return {
            "run_id": run_id,
            "results": results,
            "results_file": str(results_file),
            "report_file": str(report_file)
        }
    
    def _run_sequential(
        self,
        benchmarks: List[Any],
        progress_callback: Optional[Callable[[str, float], None]] = None
    ) -> List[BenchmarkResult]:
        """Run benchmarks sequentially."""
        results = []
        
        for i, benchmark in enumerate(benchmarks):
            logger.info(f"Running benchmark {i+1}/{len(benchmarks)}: {benchmark.name}")
            
            try:
                result = benchmark.run()
                results.append(result)
                
                if progress_callback:
                    progress = (i + 1) / len(benchmarks)
                    progress_callback(f"Completed {benchmark.name}", progress)
                    
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
    
    def _generate_summary(self, results: List[BenchmarkResult], execution_time: float) -> Dict[str, Any]:
        """Generate summary statistics."""
        total = len(results)
        successful = sum(1 for r in results if r.status == BenchmarkStatus.COMPLETED)
        failed = sum(1 for r in results if r.status == BenchmarkStatus.FAILED)
        skipped = sum(1 for r in results if r.status == BenchmarkStatus.SKIPPED)
        
        # Calculate performance metrics
        successful_results = [r for r in results if r.status == BenchmarkStatus.COMPLETED and r.execution_time]
        
        if successful_results:
            avg_time = sum(r.execution_time for r in successful_results) / len(successful_results)
            min_time = min(r.execution_time for r in successful_results)
            max_time = max(r.execution_time for r in successful_results)
        else:
            avg_time = min_time = max_time = 0.0
        
        return {
            "total_benchmarks": total,
            "successful": successful,
            "failed": failed,
            "skipped": skipped,
            "success_rate": (successful / total * 100) if total > 0 else 0,
            "total_execution_time": execution_time,
            "average_benchmark_time": avg_time,
            "fastest_benchmark": min_time,
            "slowest_benchmark": max_time
        }
    
    def compare_runs(self, baseline_run_id: str, comparison_run_id: str) -> Dict[str, Any]:
        """Compare two benchmark runs."""
        # Find result files
        baseline_file = None
        comparison_file = None
        
        for file_path in self.results_dir.glob("benchmark_results_*.json"):
            if baseline_run_id in file_path.name:
                baseline_file = file_path
            elif comparison_run_id in file_path.name:
                comparison_file = file_path
        
        if not baseline_file or not comparison_file:
            return {"error": "Could not find result files for comparison"}
        
        try:
            comparison_report = self.analyzer.compare_runs(baseline_file, comparison_file)
            return {
                "comparison_report": comparison_report,
                "baseline_file": str(baseline_file),
                "comparison_file": str(comparison_file)
            }
        except Exception as e:
            return {"error": f"Comparison failed: {e}"}
    
    def analyze_trends(self, metric: str = "execution_time", days: int = 30) -> Dict[str, Any]:
        """Analyze performance trends."""
        try:
            trend_analysis = self.analyzer.analyze_trends(metric, days)
            if trend_analysis:
                return {"trend_analysis": trend_analysis}
            else:
                return {"message": "Insufficient data for trend analysis"}
        except Exception as e:
            return {"error": f"Trend analysis failed: {e}"}
    
    def get_monitoring_summary(self) -> Dict[str, Any]:
        """Get monitoring summary if available."""
        if self.monitor:
            return self.monitor.get_alert_summary()
        else:
            return {"message": "Monitoring not enabled"}
    
    def cleanup(self):
        """Clean up resources."""
        if self.monitor:
            self.monitor.stop()
        
        for suite in self.benchmark_suites.values():
            if hasattr(suite, 'cleanup'):
                suite.cleanup()
        
        logger.info("Orchestrator cleanup completed")

# Convenience function for simple benchmark runs
def run_simple_benchmark(
    data_files: List[Path],
    engines: List[str] = None,
    config: BenchmarkConfig = None,
    output_dir: Path = None
) -> Dict[str, Any]:
    """Run a simple benchmark with minimal configuration."""
    
    if config is None:
        config = BenchmarkConfig()
    
    if output_dir is None:
        output_dir = Path("benchmark_results")
    
    if engines is None:
        engines = []
        if POLARS_AVAILABLE:
            engines.append('polars')
        if DUCKDB_AVAILABLE:
            engines.append('duckdb')
    
    orchestrator = EnhancedBenchmarkOrchestrator(config, output_dir)
    
    # Configure ingestion benchmarks
    orchestrator.configure_ingestion_benchmarks(data_files, engines)
    
    # Add query benchmarks for first file if available
    if data_files:
        orchestrator.configure_query_benchmarks(data_files[0], engines)
    
    # Run benchmarks
    return orchestrator.run_comprehensive_benchmark(engines=engines)