#!/usr/bin/env python3
"""
Enhanced Parallel Processor with Conflict Detection
Example implementation of deterministic processing strategies
"""

import asyncio
import asyncpg
from pathlib import Path
from typing import List, Dict, Set, Tuple, Optional
import polars as pl
import pandas as pd
from collections import defaultdict
import time
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ConflictAwareProcessor:
    """
    Enhanced processor that detects primary key conflicts and optimizes
    processing strategy accordingly.
    """
    
    def __init__(self, dsn: str, concurrency: int = 3):
        self.dsn = dsn
        self.concurrency = concurrency
        self.conflict_threshold = 0.1  # 10% overlap triggers sequential processing
        
    async def analyze_file_conflicts(self, files: List[Path], pk_columns: List[str]) -> Dict:
        """
        Pre-analyze files to detect primary key overlaps and determine
        optimal processing strategy.
        """
        logger.info(f"Analyzing {len(files)} files for primary key conflicts...")
        
        file_pk_sets = {}
        total_records = 0
        
        # Extract primary key sets from each file
        for file_path in files:
            pk_set = await self._extract_primary_keys(file_path, pk_columns)
            file_pk_sets[file_path] = pk_set
            total_records += len(pk_set)
            logger.info(f"{file_path.name}: {len(pk_set)} unique primary keys")
        
        # Detect conflicts between file pairs
        conflicts = self._detect_conflicts(file_pk_sets)
        
        # Determine processing strategy
        strategy = self._determine_strategy(conflicts, total_records)
        
        return {
            'strategy': strategy,
            'conflicts': conflicts,
            'file_pk_sets': file_pk_sets,
            'total_records': total_records,
            'conflict_ratio': len(conflicts) / len(files) if files else 0
        }
    
    async def _extract_primary_keys(self, file_path: Path, pk_columns: List[str]) -> Set:
        """Extract primary key values from a file efficiently."""
        try:
            if file_path.suffix.lower() == '.parquet':
                # Use polars for efficient parquet reading
                df = pl.scan_parquet(file_path).select(pk_columns).collect()
                pk_tuples = [tuple(row) for row in df.iter_rows()]
            else:
                # Use pandas for CSV with sampling for large files
                chunk_size = 10000
                pk_set = set()
                for chunk in pd.read_csv(file_path, chunksize=chunk_size, usecols=pk_columns):
                    chunk_pks = [tuple(row) for row in chunk[pk_columns].values]
                    pk_set.update(chunk_pks)
                pk_tuples = list(pk_set)
            
            return set(pk_tuples)
        
        except Exception as e:
            logger.warning(f"Error extracting PKs from {file_path}: {e}")
            return set()
    
    def _detect_conflicts(self, file_pk_sets: Dict[Path, Set]) -> List[Tuple[Path, Path]]:
        """Detect which file pairs have overlapping primary keys."""
        conflicts = []
        files = list(file_pk_sets.keys())
        
        for i, file1 in enumerate(files):
            for file2 in files[i+1:]:
                overlap = file_pk_sets[file1] & file_pk_sets[file2]
                if overlap:
                    conflicts.append((file1, file2, len(overlap)))
                    logger.info(f"Conflict detected: {file1.name} ↔ {file2.name} ({len(overlap)} overlapping keys)")
        
        return conflicts
    
    def _determine_strategy(self, conflicts: List, total_records: int) -> str:
        """Determine the optimal processing strategy based on conflict analysis."""
        if not conflicts:
            return "full_parallel"
        
        total_conflicts = sum(conflict[2] for conflict in conflicts)
        conflict_ratio = total_conflicts / total_records if total_records > 0 else 0
        
        if conflict_ratio < self.conflict_threshold:
            return "hybrid_parallel"  # Process most files in parallel, conflicts sequentially
        else:
            return "deterministic_sequential"  # High conflict rate, use deterministic order
    
    async def process_with_strategy(self, files: List[Path], table: str, 
                                   pk_columns: List[str], headers: List[str]) -> bool:
        """
        Process files using the optimal strategy based on conflict analysis.
        """
        start_time = time.time()
        
        # Analyze conflicts
        analysis = await self.analyze_file_conflicts(files, pk_columns)
        strategy = analysis['strategy']
        
        logger.info(f"Processing strategy: {strategy}")
        logger.info(f"Conflict ratio: {analysis['conflict_ratio']:.2%}")
        
        # Execute strategy
        if strategy == "full_parallel":
            success = await self._process_full_parallel(files, table, pk_columns, headers)
        elif strategy == "hybrid_parallel":
            success = await self._process_hybrid_parallel(files, table, pk_columns, headers, analysis)
        else:  # deterministic_sequential
            success = await self._process_deterministic_sequential(files, table, pk_columns, headers)
        
        elapsed_time = time.time() - start_time
        logger.info(f"Processing completed in {elapsed_time:.2f}s using {strategy} strategy")
        
        return success
    
    async def _process_full_parallel(self, files: List[Path], table: str, 
                                    pk_columns: List[str], headers: List[str]) -> bool:
        """Process all files concurrently - no conflicts detected."""
        logger.info("🚀 Full parallel processing (no conflicts)")
        
        # Use existing concurrent processing logic
        from src.cli import run_concurrent_processing
        return await run_concurrent_processing(files, table, pk_columns, headers, self.concurrency)
    
    async def _process_hybrid_parallel(self, files: List[Path], table: str, 
                                      pk_columns: List[str], headers: List[str], 
                                      analysis: Dict) -> bool:
        """Process non-conflicting files in parallel, conflicting files sequentially."""
        logger.info("🔀 Hybrid processing (minimal conflicts)")
        
        conflicts = analysis['conflicts']
        
        # Identify conflicting files
        conflicting_files = set()
        for conflict in conflicts:
            conflicting_files.update([conflict[0], conflict[1]])
        
        # Separate files into conflict-free and conflicting groups
        safe_files = [f for f in files if f not in conflicting_files]
        conflict_files = list(conflicting_files)
        
        logger.info(f"Safe files (parallel): {len(safe_files)}")
        logger.info(f"Conflict files (sequential): {len(conflict_files)}")
        
        # Process safe files in parallel
        if safe_files:
            safe_success = await self._process_full_parallel(safe_files, table, pk_columns, headers)
        else:
            safe_success = True
        
        # Process conflicting files sequentially by modification time (deterministic)
        conflict_files.sort(key=lambda f: f.stat().st_mtime)
        conflict_success = True
        
        for file_path in conflict_files:
            logger.info(f"Processing conflict file: {file_path.name}")
            file_success = await self._process_single_file(file_path, table, pk_columns, headers)
            conflict_success &= file_success
        
        return safe_success and conflict_success
    
    async def _process_deterministic_sequential(self, files: List[Path], table: str, 
                                               pk_columns: List[str], headers: List[str]) -> bool:
        """Process all files sequentially in deterministic order (high conflict rate)."""
        logger.info("🔄 Deterministic sequential processing (high conflict rate)")
        
        # Sort files by modification time for deterministic processing
        sorted_files = sorted(files, key=lambda f: f.stat().st_mtime)
        
        success = True
        for i, file_path in enumerate(sorted_files):
            logger.info(f"Processing file {i+1}/{len(sorted_files)}: {file_path.name}")
            file_success = await self._process_single_file(file_path, table, pk_columns, headers)
            success &= file_success
        
        return success
    
    async def _process_single_file(self, file_path: Path, table: str, 
                                  pk_columns: List[str], headers: List[str]) -> bool:
        """Process a single file using the existing CLI logic."""
        try:
            # Import and use existing processing logic
            import subprocess
            import sys
            
            cmd = [
                sys.executable, "-m", "src.cli",
                "--dsn", self.dsn,
                "--files", str(file_path),
                "--file-type", "parquet" if file_path.suffix.lower() == ".parquet" else "csv",
                "--table", table,
                "--pk", ",".join(pk_columns),
                "--headers", ",".join(headers),
                "--concurrency", "1"  # Single file processing
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            return result.returncode == 0
            
        except Exception as e:
            logger.error(f"Error processing {file_path}: {e}")
            return False

# Performance monitoring decorator
def monitor_performance(func):
    """Decorator to monitor function performance."""
    async def wrapper(*args, **kwargs):
        start_time = time.time()
        start_memory = psutil.Process().memory_info().rss if 'psutil' in globals() else 0
        
        try:
            result = await func(*args, **kwargs)
            success = True
        except Exception as e:
            logger.error(f"Function {func.__name__} failed: {e}")
            result = False
            success = False
        
        end_time = time.time()
        end_memory = psutil.Process().memory_info().rss if 'psutil' in globals() else 0
        
        logger.info(f"Performance Report - {func.__name__}:")
        logger.info(f"  Duration: {end_time - start_time:.2f}s")
        logger.info(f"  Memory Delta: {(end_memory - start_memory) / 1024 / 1024:.2f}MB")
        logger.info(f"  Success: {success}")
        
        return result
    
    return wrapper

# Example usage
async def main():
    """Example usage of the conflict-aware processor."""
    processor = ConflictAwareProcessor(
        dsn="postgresql://testuser:testpass@localhost:5433/testdb",
        concurrency=3
    )
    
    # Example file list
    files = [
        Path("data/sample_1.csv"),
        Path("data/sample_2.csv"),
        Path("data/sample_1.parquet"),
        Path("data/sample_2.parquet"),
    ]
    
    # Process with automatic strategy detection
    success = await processor.process_with_strategy(
        files=files,
        table="ingest_test_table",
        pk_columns=["id"],
        headers=["id", "name", "value"]
    )
    
    if success:
        print("🎉 All files processed successfully with optimal strategy!")
    else:
        print("❌ Some files failed processing")

if __name__ == "__main__":
    asyncio.run(main())
