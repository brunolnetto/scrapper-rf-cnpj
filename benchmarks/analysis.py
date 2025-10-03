"""Enhanced analysis and reporting for benchmarks."""

import json
import time
from pathlib import Path
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, asdict
import logging

from .utils import BenchmarkResult, BenchmarkStatus

logger = logging.getLogger(__name__)

@dataclass
class ComparisonReport:
    """Benchmark comparison report."""
    baseline_name: str
    comparison_name: str
    improvements: Dict[str, float]  # metric -> percentage improvement
    regressions: Dict[str, float]   # metric -> percentage regression
    unchanged: List[str]
    summary: str

@dataclass
class TrendAnalysis:
    """Trend analysis over time."""
    metric: str
    trend_direction: str  # "improving" | "degrading" | "stable"
    trend_strength: float  # 0.0 to 1.0
    data_points: List[Dict[str, Any]]
    recommendation: str

class BenchmarkAnalyzer:
    """Analyze benchmark results with statistical insights."""
    
    def __init__(self, results_dir: Path):
        self.results_dir = Path(results_dir)
        self.results_dir.mkdir(exist_ok=True)
    
    def save_results(self, results: List[BenchmarkResult], run_id: str) -> Path:
        """Save benchmark results with metadata."""
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        filename = f"benchmark_results_{run_id}_{timestamp}.json"
        filepath = self.results_dir / filename
        
        # Convert results to JSON-serializable format
        data = {
            "run_id": run_id,
            "timestamp": timestamp,
            "results": [asdict(result) for result in results],
            "metadata": {
                "total_benchmarks": len(results),
                "successful": sum(1 for r in results if r.status == BenchmarkStatus.COMPLETED),
                "failed": sum(1 for r in results if r.status == BenchmarkStatus.FAILED),
                "skipped": sum(1 for r in results if r.status == BenchmarkStatus.SKIPPED)
            }
        }
        
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2, default=str)
        
        logger.info(f"Saved benchmark results to {filepath}")
        return filepath
    
    def load_results(self, filepath: Path) -> List[BenchmarkResult]:
        """Load benchmark results from file."""
        with open(filepath, 'r') as f:
            data = json.load(f)
        
        results = []
        for item in data["results"]:
            # Handle status enum conversion
            if isinstance(item["status"], str):
                item["status"] = BenchmarkStatus(item["status"])
            results.append(BenchmarkResult(**item))
        
        return results
    
    def compare_runs(self, baseline_file: Path, comparison_file: Path) -> ComparisonReport:
        """Compare two benchmark runs."""
        baseline_results = self.load_results(baseline_file)
        comparison_results = self.load_results(comparison_file)
        
        # Create lookup maps by benchmark name
        baseline_map = {r.name: r for r in baseline_results}
        comparison_map = {r.name: r for r in comparison_results}
        
        improvements = {}
        regressions = {}
        unchanged = []
        
        for name in baseline_map:
            if name not in comparison_map:
                continue
                
            baseline = baseline_map[name]
            comparison = comparison_map[name]
            
            # Skip failed benchmarks
            if baseline.status != BenchmarkStatus.COMPLETED or comparison.status != BenchmarkStatus.COMPLETED:
                continue
            
            # Compare execution time
            if baseline.execution_time and comparison.execution_time:
                time_change = ((baseline.execution_time - comparison.execution_time) / baseline.execution_time) * 100
                
                if abs(time_change) < 5:  # Less than 5% change considered unchanged
                    unchanged.append(name)
                elif time_change > 0:  # Positive means improvement (less time)
                    improvements[f"{name}_time"] = time_change
                else:  # Negative means regression (more time)
                    regressions[f"{name}_time"] = abs(time_change)
            
            # Compare memory usage if available
            baseline_memory = baseline.metadata.get("peak_memory_mb")
            comparison_memory = comparison.metadata.get("peak_memory_mb")
            
            if baseline_memory and comparison_memory:
                memory_change = ((baseline_memory - comparison_memory) / baseline_memory) * 100
                
                if abs(memory_change) < 5:
                    pass  # Already in unchanged list
                elif memory_change > 0:  # Positive means improvement (less memory)
                    improvements[f"{name}_memory"] = memory_change
                else:  # Negative means regression (more memory)
                    regressions[f"{name}_memory"] = abs(memory_change)
        
        # Generate summary
        total_improvements = len(improvements)
        total_regressions = len(regressions)
        
        if total_improvements > total_regressions:
            summary = f"Overall improvement: {total_improvements} improvements vs {total_regressions} regressions"
        elif total_regressions > total_improvements:
            summary = f"Overall regression: {total_regressions} regressions vs {total_improvements} improvements"
        else:
            summary = f"Mixed results: {total_improvements} improvements and {total_regressions} regressions"
        
        return ComparisonReport(
            baseline_name=baseline_file.stem,
            comparison_name=comparison_file.stem,
            improvements=improvements,
            regressions=regressions,
            unchanged=unchanged,
            summary=summary
        )
    
    def analyze_trends(self, metric: str = "execution_time", days: int = 30) -> Optional[TrendAnalysis]:
        """Analyze trends over time for a specific metric."""
        # Find all result files from the last N days
        cutoff_time = time.time() - (days * 24 * 60 * 60)
        recent_files = []
        
        for filepath in self.results_dir.glob("benchmark_results_*.json"):
            if filepath.stat().st_mtime >= cutoff_time:
                recent_files.append(filepath)
        
        if len(recent_files) < 2:
            logger.warning(f"Not enough data points for trend analysis (found {len(recent_files)}, need at least 2)")
            return None
        
        # Extract data points
        data_points = []
        for filepath in sorted(recent_files, key=lambda f: f.stat().st_mtime):
            try:
                results = self.load_results(filepath)
                timestamp = filepath.stat().st_mtime
                
                # Calculate average metric value for this run
                values = []
                for result in results:
                    if result.status == BenchmarkStatus.COMPLETED:
                        if metric == "execution_time" and result.execution_time:
                            values.append(result.execution_time)
                        elif metric == "memory_usage":
                            memory = result.metadata.get("peak_memory_mb")
                            if memory:
                                values.append(memory)
                
                if values:
                    avg_value = sum(values) / len(values)
                    data_points.append({
                        "timestamp": timestamp,
                        "value": avg_value,
                        "count": len(values)
                    })
            except Exception as e:
                logger.warning(f"Error processing {filepath}: {e}")
        
        if len(data_points) < 2:
            return None
        
        # Simple trend analysis
        values = [dp["value"] for dp in data_points]
        
        # Calculate trend direction and strength
        if len(values) >= 3:
            # Linear regression slope
            n = len(values)
            x_values = list(range(n))
            sum_x = sum(x_values)
            sum_y = sum(values)
            sum_xy = sum(x * y for x, y in zip(x_values, values))
            sum_x2 = sum(x * x for x in x_values)
            
            slope = (n * sum_xy - sum_x * sum_y) / (n * sum_x2 - sum_x * sum_x)
            
            # Determine trend direction
            if abs(slope) < 0.1:  # Very small slope
                trend_direction = "stable"
                trend_strength = 0.1
            elif slope < 0:
                trend_direction = "improving" if metric in ["execution_time", "memory_usage"] else "degrading"
                trend_strength = min(abs(slope) / (sum_y / n), 1.0)
            else:
                trend_direction = "degrading" if metric in ["execution_time", "memory_usage"] else "improving"
                trend_strength = min(slope / (sum_y / n), 1.0)
        else:
            # Simple comparison for 2 data points
            if values[-1] < values[0]:
                trend_direction = "improving" if metric in ["execution_time", "memory_usage"] else "degrading"
            elif values[-1] > values[0]:
                trend_direction = "degrading" if metric in ["execution_time", "memory_usage"] else "improving"
            else:
                trend_direction = "stable"
            
            trend_strength = abs(values[-1] - values[0]) / values[0]
        
        # Generate recommendation
        if trend_direction == "improving":
            recommendation = f"Performance is improving for {metric}. Continue current optimization efforts."
        elif trend_direction == "degrading":
            recommendation = f"Performance is degrading for {metric}. Investigation recommended."
        else:
            recommendation = f"Performance is stable for {metric}. Monitor for changes."
        
        return TrendAnalysis(
            metric=metric,
            trend_direction=trend_direction,
            trend_strength=trend_strength,
            data_points=data_points,
            recommendation=recommendation
        )
    
    def generate_report(self, results: List[BenchmarkResult], output_file: Optional[Path] = None) -> str:
        """Generate comprehensive benchmark report."""
        # Calculate statistics
        total_benchmarks = len(results)
        successful = [r for r in results if r.status == BenchmarkStatus.COMPLETED]
        failed = [r for r in results if r.status == BenchmarkStatus.FAILED]
        skipped = [r for r in results if r.status == BenchmarkStatus.SKIPPED]
        
        report_lines = [
            "# Benchmark Report",
            f"Generated: {time.strftime('%Y-%m-%d %H:%M:%S')}",
            "",
            "## Summary",
            f"- Total benchmarks: {total_benchmarks}",
            f"- Successful: {len(successful)} ({len(successful)/total_benchmarks*100:.1f}%)",
            f"- Failed: {len(failed)} ({len(failed)/total_benchmarks*100:.1f}%)",
            f"- Skipped: {len(skipped)} ({len(skipped)/total_benchmarks*100:.1f}%)",
            ""
        ]
        
        # Performance metrics
        if successful:
            execution_times = [r.execution_time for r in successful if r.execution_time]
            if execution_times:
                avg_time = sum(execution_times) / len(execution_times)
                min_time = min(execution_times)
                max_time = max(execution_times)
                
                report_lines.extend([
                    "## Performance Metrics",
                    f"- Average execution time: {avg_time:.2f}s",
                    f"- Fastest: {min_time:.2f}s",
                    f"- Slowest: {max_time:.2f}s",
                    ""
                ])
            
            # Memory usage
            memory_values = []
            for r in successful:
                memory = r.metadata.get("peak_memory_mb")
                if memory:
                    memory_values.append(memory)
            
            if memory_values:
                avg_memory = sum(memory_values) / len(memory_values)
                max_memory = max(memory_values)
                
                report_lines.extend([
                    "## Memory Usage",
                    f"- Average peak memory: {avg_memory:.1f} MB",
                    f"- Maximum peak memory: {max_memory:.1f} MB",
                    ""
                ])
        
        # Detailed results
        report_lines.extend([
            "## Detailed Results",
            ""
        ])
        
        for result in results:
            status_emoji = {
                BenchmarkStatus.COMPLETED: "✅",
                BenchmarkStatus.FAILED: "❌",
                BenchmarkStatus.SKIPPED: "⏭️",
                BenchmarkStatus.RUNNING: "🔄"
            }.get(result.status, "❓")
            
            report_lines.append(f"### {status_emoji} {result.name}")
            report_lines.append(f"- Status: {result.status.value}")
            
            if result.execution_time:
                report_lines.append(f"- Execution time: {result.execution_time:.2f}s")
            
            if result.error_message:
                report_lines.append(f"- Error: {result.error_message}")
            
            # Memory info
            memory = result.metadata.get("peak_memory_mb")
            if memory:
                report_lines.append(f"- Peak memory: {memory:.1f} MB")
            
            report_lines.append("")
        
        # Generate final report
        report_text = "\n".join(report_lines)
        
        # Save to file if specified
        if output_file:
            with open(output_file, 'w') as f:
                f.write(report_text)
            logger.info(f"Report saved to {output_file}")
        
        return report_text