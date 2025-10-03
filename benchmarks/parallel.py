"""Simple parallel execution for benchmarks."""

import time
import concurrent.futures
from typing import List, Callable
import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class ExecutionGroup:
    """Group of benchmarks to execute together."""
    name: str
    benchmarks: List[Callable]
    estimated_duration: float
    priority: int = 1

class ParallelExecutor:
    """Execute benchmarks in parallel groups."""
    
    def __init__(self, config, monitor=None):
        self.config = config
        self.monitor = monitor
        self.max_workers = min(getattr(config, 'max_threads', 4), 4)
        
    def create_execution_plan(self, benchmarks: List[Callable], max_groups: int = 5):
        """Create execution plan with groups."""
        groups = []
        benchmarks_per_group = max(1, len(benchmarks) // max_groups)
        
        for i in range(0, len(benchmarks), benchmarks_per_group):
            group_benchmarks = benchmarks[i:i + benchmarks_per_group]
            group_name = f"group_{i//benchmarks_per_group + 1}"
            estimated_duration = 30.0 * len(group_benchmarks)
            
            groups.append(ExecutionGroup(
                name=group_name,
                benchmarks=group_benchmarks,
                estimated_duration=estimated_duration
            ))
            
        logger.info(f"Created execution plan: {len(groups)} groups")
        return groups
    
    def execute_group(self, group: ExecutionGroup):
        """Execute a group of benchmarks."""
        logger.info(f"Executing {group.name} with {len(group.benchmarks)} benchmarks")
        results = []
        
        start_time = time.time()
        for benchmark in group.benchmarks:
            try:
                # Benchmark objects have a run() method, not callable
                if hasattr(benchmark, 'run'):
                    result = benchmark.run()
                else:
                    # Fallback for callable functions
                    result = benchmark()
                results.append(result)
            except Exception as e:
                logger.error(f"Benchmark failed: {e}")
                from .utils import BenchmarkResult, BenchmarkStatus
                results.append(BenchmarkResult(
                    name=getattr(benchmark, 'name', getattr(benchmark, '__name__', 'unknown')),
                    status=BenchmarkStatus.FAILED,
                    execution_time=0.0,
                    error_message=str(e)
                ))
        
        duration = time.time() - start_time
        logger.info(f"Group {group.name} completed in {duration:.1f}s")
        return results
    
    def execute_parallel(self, benchmarks: List[Callable], progress_callback=None):
        """Execute benchmarks in parallel groups."""
        groups = self.create_execution_plan(benchmarks, max_groups=5)
        all_results = []
        
        for i, group in enumerate(groups, 1):
            logger.info(f"Starting group {i}/{len(groups)}: {group.name}")
            
            group_results = self.execute_group(group)
            all_results.extend(group_results)
            
            if progress_callback:
                progress = (i / len(groups)) * 100
                progress_callback(f"Completed group {i}/{len(groups)}", progress)
            
            logger.info(f"Progress: {(i / len(groups)) * 100:.1f}% ({i}/{len(groups)} groups)")
        
        return all_results

class AsyncBenchmarkRunner:
    """Run benchmarks with async coordination."""
    
    def __init__(self, executor=None):
        self.executor = executor
        
    async def run_benchmarks_async(self, benchmarks: List[Callable], max_groups: int = 5):
        """Run benchmarks with async coordination."""
        if not self.executor:
            raise RuntimeError("No executor configured")
            
        groups = self.executor.create_execution_plan(benchmarks, max_groups)
        all_results = []
        
        for i, group in enumerate(groups, 1):
            logger.info(f"Starting group {i}/{len(groups)}: {group.name}")
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(self.executor.execute_group, group)
                
                try:
                    group_results = future.result(timeout=group.estimated_duration + 300)
                    all_results.extend(group_results)
                    
                    progress = (i / len(groups)) * 100
                    logger.info(f"Progress: {progress:.1f}% ({i}/{len(groups)} groups)")
                    
                except concurrent.futures.TimeoutError:
                    logger.error(f"Group {group.name} timed out")
        
        return all_results
