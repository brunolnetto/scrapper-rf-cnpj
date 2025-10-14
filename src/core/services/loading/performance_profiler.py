"""
Performance Profiler - Benchmark-driven monitoring and validation.

Tracks actual throughput against benchmark baselines and alerts when performance degrades.
Based on Polars vs DuckDB benchmark results (August 2025).
"""
import time
from typing import Dict, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime

from ....setup.logging import logger


@dataclass
class BenchmarkBaseline:
    """Benchmark baseline for a specific table configuration."""
    table_name: str
    rows_per_second: float
    chunk_size: int
    concurrency: int
    method: str  # 'copy' or 'insert'
    description: str
    

class PerformanceProfiler:
    """
    Monitor and validate performance against benchmark baselines.
    
    Benchmark Results (Polars, 10k sample):
    - SOCIO (narrow, 11 cols): 243,603 r/s (large chunks + COPY + serial)
    - ESTABELE (wide, 30 cols): 17,833 r/s (medium chunks + COPY + 2 workers)
    - COPY vs INSERT speedup: 3-8x
    """
    
    # Benchmark baselines from actual test results
    BASELINES: Dict[str, BenchmarkBaseline] = {
        'socio_optimal': BenchmarkBaseline(
            table_name='socio',
            rows_per_second=243_603,
            chunk_size=100_000,
            concurrency=1,
            method='copy',
            description='Narrow table (11 cols), large chunks, serial COPY'
        ),
        'socio_parallel': BenchmarkBaseline(
            table_name='socio',
            rows_per_second=207_770,
            chunk_size=100_000,
            concurrency=2,
            method='copy',
            description='Narrow table (11 cols), large chunks, 2 workers'
        ),
        'socio_insert': BenchmarkBaseline(
            table_name='socio',
            rows_per_second=40_092,
            chunk_size=100_000,
            concurrency=1,
            method='insert',
            description='Narrow table (11 cols), INSERT operations'
        ),
        'estabele_optimal': BenchmarkBaseline(
            table_name='estabelecimento',
            rows_per_second=17_833,
            chunk_size=50_000,
            concurrency=2,
            method='copy',
            description='Wide table (30 cols), medium chunks, 2 workers'
        ),
        'estabele_serial': BenchmarkBaseline(
            table_name='estabelecimento',
            rows_per_second=15_304,
            chunk_size=100_000,
            concurrency=1,
            method='copy',
            description='Wide table (30 cols), large chunks, serial'
        ),
        'estabele_insert': BenchmarkBaseline(
            table_name='estabelecimento',
            rows_per_second=6_215,
            chunk_size=50_000,
            concurrency=1,
            method='insert',
            description='Wide table (30 cols), INSERT operations'
        ),
    }
    
    def __init__(self):
        self.metrics: Dict[str, Dict] = {}
        self.start_times: Dict[str, float] = {}
        
    def start_tracking(self, file_path: str, table_name: str, chunk_size: int, concurrency: int):
        """Start tracking performance for a file load operation."""
        tracking_id = f"{table_name}_{int(time.time())}"
        self.start_times[tracking_id] = time.perf_counter()
        self.metrics[tracking_id] = {
            'file_path': file_path,
            'table_name': table_name,
            'chunk_size': chunk_size,
            'concurrency': concurrency,
            'start_time': datetime.now(),
            'rows_processed': 0
        }
        return tracking_id
    
    def update_progress(self, tracking_id: str, rows_processed: int):
        """Update progress for ongoing operation."""
        if tracking_id in self.metrics:
            self.metrics[tracking_id]['rows_processed'] = rows_processed
    
    def finish_tracking(self, tracking_id: str, total_rows: int, success: bool = True) -> Optional[Dict]:
        """
        Finish tracking and compare against benchmarks.
        
        Returns:
            Dictionary with performance analysis or None if tracking_id not found.
        """
        if tracking_id not in self.start_times:
            return None
        
        elapsed = time.perf_counter() - self.start_times[tracking_id]
        metrics = self.metrics[tracking_id]
        
        rows_per_second = total_rows / elapsed if elapsed > 0 else 0
        
        # Get appropriate baseline
        table_name = metrics['table_name'].lower()
        baseline_key = self._get_baseline_key(table_name, metrics['chunk_size'], metrics['concurrency'])
        baseline = self.BASELINES.get(baseline_key)
        
        result = {
            'tracking_id': tracking_id,
            'table_name': metrics['table_name'],
            'file_path': metrics['file_path'],
            'total_rows': total_rows,
            'elapsed_seconds': elapsed,
            'rows_per_second': rows_per_second,
            'chunk_size': metrics['chunk_size'],
            'concurrency': metrics['concurrency'],
            'success': success
        }
        
        if baseline:
            efficiency = (rows_per_second / baseline.rows_per_second) * 100
            result.update({
                'baseline_rps': baseline.rows_per_second,
                'efficiency_pct': efficiency,
                'baseline_description': baseline.description
            })
            
            # Log performance analysis
            self._log_performance_analysis(result, baseline)
        else:
            logger.info(f"[Profiler] No baseline found for {table_name}, "
                       f"achieved {rows_per_second:,.0f} r/s")
        
        # Cleanup
        del self.start_times[tracking_id]
        
        return result
    
    def _get_baseline_key(self, table_name: str, chunk_size: int, concurrency: int) -> str:
        """Determine appropriate baseline key based on table and configuration."""
        # Normalize table names
        if 'socio' in table_name.lower():
            base = 'socio'
        elif 'estabele' in table_name.lower():
            base = 'estabele'
        else:
            # Generic baseline based on chunk size and concurrency
            return 'socio_optimal' if chunk_size >= 75_000 else 'estabele_optimal'
        
        # Choose specific baseline
        if concurrency == 1:
            return f'{base}_optimal' if base == 'socio' else f'{base}_serial'
        else:
            return f'{base}_parallel' if base == 'socio' else f'{base}_optimal'
    
    def _log_performance_analysis(self, result: Dict, baseline: BenchmarkBaseline):
        """Log detailed performance analysis with color-coded alerts."""
        efficiency = result['efficiency_pct']
        actual_rps = result['rows_per_second']
        baseline_rps = baseline.rows_per_second
        
        # Color-coded efficiency levels
        if efficiency >= 80:
            level = "✅ EXCELLENT"
            log_func = logger.info
        elif efficiency >= 60:
            level = "✓ GOOD"
            log_func = logger.info
        elif efficiency >= 40:
            level = "⚠️  SUBOPTIMAL"
            log_func = logger.warning
        else:
            level = "❌ POOR"
            log_func = logger.error
        
        log_func(
            f"[Profiler] {level} Performance "
            f"| Table: {result['table_name']} "
            f"| Actual: {actual_rps:,.0f} r/s "
            f"| Baseline: {baseline_rps:,.0f} r/s "
            f"| Efficiency: {efficiency:.1f}% "
            f"| Rows: {result['total_rows']:,} "
            f"| Time: {result['elapsed_seconds']:.1f}s"
        )
        
        # Provide optimization suggestions for poor performance
        if efficiency < 60:
            self._suggest_optimizations(result, baseline)
    
    def _suggest_optimizations(self, result: Dict, baseline: BenchmarkBaseline):
        """Suggest optimizations based on performance gap."""
        suggestions = []
        
        actual_chunk = result['chunk_size']
        baseline_chunk = baseline.chunk_size
        actual_concurrency = result['concurrency']
        baseline_concurrency = baseline.concurrency
        
        if actual_chunk < baseline_chunk:
            suggestions.append(
                f"Increase chunk_size from {actual_chunk:,} to {baseline_chunk:,}"
            )
        
        if actual_concurrency != baseline_concurrency:
            if baseline_concurrency == 1:
                suggestions.append(
                    f"Use serial processing (concurrency=1) instead of {actual_concurrency} workers"
                )
            else:
                suggestions.append(
                    f"Adjust concurrency to {baseline_concurrency} workers"
                )
        
        if suggestions:
            logger.warning(f"[Profiler] Optimization suggestions:")
            for i, suggestion in enumerate(suggestions, 1):
                logger.warning(f"[Profiler]   {i}. {suggestion}")
    
    def get_summary_stats(self) -> Dict:
        """Get summary statistics for all tracked operations."""
        if not self.metrics:
            return {}
        
        completed = [m for tid, m in self.metrics.items() if tid not in self.start_times]
        
        if not completed:
            return {'active_operations': len(self.start_times)}
        
        return {
            'total_operations': len(completed),
            'active_operations': len(self.start_times),
            'tables_processed': len(set(m['table_name'] for m in completed))
        }
    
    def validate_copy_usage(self, method: str, table_name: str) -> bool:
        """
        Validate that COPY protocol is being used (benchmark shows 3-8x faster than INSERT).
        
        Returns:
            True if using COPY, False if using INSERT (triggers warning).
        """
        if method.lower() == 'insert':
            logger.error(
                f"[Profiler] ❌ PERFORMANCE ISSUE: Using INSERT for {table_name}. "
                f"COPY is 3-8x faster! Check database loading configuration."
            )
            return False
        
        logger.debug(f"[Profiler] ✅ Confirmed COPY protocol for {table_name}")
        return True
