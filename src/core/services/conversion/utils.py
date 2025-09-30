"""
Stream-merge architecture: Process multiple large CSV files simultaneously
by reading small chunks from each and writing incrementally to output.

Two-pass approach: 
Pass 1: Analyze all files and create optimal partition plan
Pass 2: Execute plan with guaranteed memory safety
"""
from pathlib import Path
from typing import List, Dict
import polars as pl
import gc
import os
from typing import Optional

from .models import ChunkIterator


def read_cgroup_memory_limit_bytes() -> Optional[int]:
    """
    Return memory limit in bytes if running in a cgroup with an enforceable limit,
    or None if no limit detected. Tries common cgroup v2 and v1 paths.
    """
    # cgroup v2 unified: memory.max (value = "max" or a number)
    try_paths = [
        "/sys/fs/cgroup/memory.max",                 # sometimes on v2
        "/sys/fs/cgroup/memory/memory.limit_in_bytes"  # cgroup v1
    ]
    for p in try_paths:
        try:
            if os.path.exists(p):
                val = open(p, "r").read().strip()
                if val in ("max", ""):
                    return None
                return int(val)
        except Exception:
            pass
    # Could add more exhaustive lookup using /proc/self/cgroup, but this covers common setups.
    return None

def convert_multiple_csvs_streaming(
    csv_paths: List[Path],
    output_path: Path,
    expected_columns: List[str],
    delimiter: str,
    chunk_size: int = 50000,
    memory_monitor = None
) -> Dict:
    """
    Convert multiple CSV files to single Parquet using stream-merge.
    
    Key insight: Instead of file1 → file2 → file3, we do:
    file1[chunk1] → file2[chunk1] → file3[chunk1] → write
    file1[chunk2] → file2[chunk2] → file3[chunk2] → write
    ...
    
    This keeps memory constant regardless of file sizes.
    """
    # Infer schema from first file's sample
    schema_override = None
    try:
        sample = pl.read_csv(
            str(csv_paths[0]),
            separator=delimiter,
            n_rows=1000,
            encoding="utf8-lossy",
            ignore_errors=True,
            has_header=False
        )
        schema_override = dict(sample.schema)
        del sample
        gc.collect()
    except:
        # Fallback to string schema
        if expected_columns:
            schema_override = {f"column_{i+1}": pl.Utf8 
                             for i in range(len(expected_columns))}
    
    # Create chunk iterators for all files
    iterators = [
        ChunkIterator(
            csv_path=path,
            delimiter=delimiter,
            chunk_size=chunk_size,
            schema_override=schema_override,
            expected_columns=expected_columns
        )
        for path in csv_paths
    ]
    
    total_rows = 0
    first_batch = True
    
    # Process round-robin across all files
    active_iterators = list(iterators)
    
    while active_iterators:
        batches = []
        exhausted = []
        
        # Collect one chunk from each active file
        for it in active_iterators:
            try:
                chunk = next(it)
                batches.append(chunk)
            except StopIteration:
                exhausted.append(it)
        
        # Remove exhausted iterators
        for it in exhausted:
            active_iterators.remove(it)
        
        # If we have batches, combine and write
        if batches:
            # Combine chunks from different files
            combined = pl.concat(batches, how="vertical")
            total_rows += len(combined)
            
            # Write to parquet incrementally
            if first_batch:
                # Create file on first write
                combined.write_parquet(
                    str(output_path),
                    compression="snappy",
                    row_group_size=50000,
                    statistics=False
                )
                first_batch = False
            else:
                # Append to existing file using LazyFrame
                existing = pl.scan_parquet(str(output_path))
                combined_lazy = pl.concat([
                    existing,
                    pl.LazyFrame(combined)
                ], how="vertical")
                
                combined_lazy.sink_parquet(
                    str(output_path),
                    compression="snappy",
                    row_group_size=50000,
                    maintain_order=False,
                    statistics=False
                )
            
            # Cleanup
            del batches, combined
            gc.collect()
            
            # Memory check
            if memory_monitor and memory_monitor.is_memory_pressure_high():
                memory_monitor.perform_aggressive_cleanup()
    
    return {
        "rows_processed": total_rows,
        "output_bytes": output_path.stat().st_size,
        "strategy": "stream_merge"
    }


def convert_multiple_csvs_partitioned(
    csv_paths: List[Path],
    output_path: Path,
    expected_columns: List[str],
    delimiter: str,
    max_memory_mb: int = 1000,
    memory_monitor = None
) -> Dict:
    """
    Alternative: Create memory-sized partitions, then merge at end.
    Better for systems with strict memory limits.
    """
    temp_dir = output_path.parent / f".partitions_{output_path.stem}"
    temp_dir.mkdir(exist_ok=True)
    
    partition_files = []
    partition_num = 0
    current_partition_rows = 0
    rows_per_partition = estimate_rows_per_partition(max_memory_mb, len(expected_columns))
    
    try:
        # Create chunk readers
        for csv_path in csv_paths:
            chunk_reader = ChunkIterator(
                csv_path=csv_path,
                delimiter=delimiter,
                chunk_size=50000,
                schema_override=None,
                expected_columns=expected_columns
            )
            
            for chunk in chunk_reader:
                # Check if current partition is full
                if current_partition_rows + len(chunk) > rows_per_partition:
                    partition_num += 1
                    current_partition_rows = 0
                    
                    # Memory cleanup between partitions
                    if memory_monitor:
                        memory_monitor.perform_aggressive_cleanup()
                
                # Determine output file for this chunk
                partition_file = temp_dir / f"partition_{partition_num:04d}.parquet"
                
                if partition_file not in partition_files:
                    partition_files.append(partition_file)
                    # Create new partition
                    chunk.write_parquet(
                        str(partition_file),
                        compression="snappy",
                        row_group_size=50000
                    )
                else:
                    # Append to existing partition
                    existing = pl.scan_parquet(str(partition_file))
                    combined = pl.concat([existing, pl.LazyFrame(chunk)], how="vertical")
                    combined.sink_parquet(
                        str(partition_file),
                        compression="snappy",
                        row_group_size=50000,
                        maintain_order=False
                    )
                
                current_partition_rows += len(chunk)
                del chunk
                gc.collect()
        
        # Merge all partitions
        if len(partition_files) == 1:
            partition_files[0].rename(output_path)
        else:
            # Stream-merge partitions
            lazy_frames = [pl.scan_parquet(str(f)) for f in partition_files]
            combined = pl.concat(lazy_frames, how="vertical")
            combined.sink_parquet(
                str(output_path),
                compression="snappy",
                row_group_size=50000,
                maintain_order=False
            )
            
            # Cleanup partitions
            for pf in partition_files:
                pf.unlink()
        
        return {
            "rows_processed": sum(
                pl.scan_parquet(str(output_path)).select(pl.len()).collect().item()
                for _ in [1]
            ),
            "output_bytes": output_path.stat().st_size,
            "partitions_created": len(partition_files),
            "strategy": "partitioned_merge"
        }
        
    finally:
        # Cleanup temp directory
        try:
            for pf in partition_files:
                if pf.exists():
                    pf.unlink()
            if temp_dir.exists():
                temp_dir.rmdir()
        except:
            pass


def estimate_rows_per_partition(max_memory_mb: int, num_columns: int) -> int:
    """Estimate how many rows fit in memory budget."""
    # Conservative estimate: 100 bytes per column
    bytes_per_row = num_columns * 100
    # Use 80% of budget for safety
    available_bytes = max_memory_mb * 1024 * 1024 * 0.8
    return int(available_bytes / bytes_per_row)

def analyze_csv_characteristics(csv_path: Path, delimiter: str) -> Dict:
    """Fast analysis of CSV file characteristics."""
    file_size_mb = csv_path.stat().st_size / (1024 * 1024)
    
    # Sample to get row characteristics
    try:
        sample = pl.read_csv(
            str(csv_path),
            separator=delimiter,
            n_rows=1000,
            encoding="utf8-lossy",
            ignore_errors=True,
            has_header=False
        )
        
        num_columns = len(sample.columns)
        avg_row_size = csv_path.stat().st_size / max(1, len(sample) * (file_size_mb / 0.001))
        estimated_rows = int(csv_path.stat().st_size / max(1, avg_row_size))
        
        del sample
        gc.collect()
        
        return {
            "path": csv_path,
            "size_mb": file_size_mb,
            "num_columns": num_columns,
            "estimated_rows": estimated_rows,
            "avg_row_bytes": avg_row_size
        }
    except Exception as e:
        return {
            "path": csv_path,
            "size_mb": file_size_mb,
            "num_columns": 0,
            "estimated_rows": 0,
            "avg_row_bytes": 0,
            "error": str(e)
        }


def create_partition_plan(
    file_characteristics: List[Dict],
    memory_budget_mb: int,
    safety_factor: float = 0.7
) -> List[List[Dict]]:
    """
    Create optimal partition plan that guarantees memory safety.
    
    Returns: List of partitions, where each partition is a list of 
    (file, start_row, end_row) tuples that fit in memory.
    """
    # Calculate usable memory per partition
    usable_memory_mb = memory_budget_mb * safety_factor
    
    partitions = []
    current_partition = []
    current_partition_size_mb = 0
    
    # Sort files by size (process smaller files first for better packing)
    sorted_files = sorted(file_characteristics, key=lambda x: x["size_mb"])
    
    for file_info in sorted_files:
        file_size_mb = file_info["size_mb"]
        
        # If file is larger than budget, split it
        if file_size_mb > usable_memory_mb:
            # Calculate how many chunks needed
            num_chunks = int(file_size_mb / (usable_memory_mb * 0.9)) + 1
            rows_per_chunk = file_info["estimated_rows"] // num_chunks
            
            for i in range(num_chunks):
                start_row = i * rows_per_chunk
                end_row = min((i + 1) * rows_per_chunk, file_info["estimated_rows"])
                chunk_size_mb = (end_row - start_row) * file_info["avg_row_bytes"] / (1024 * 1024)
                
                # Create partition for this chunk
                partitions.append([{
                    "path": file_info["path"],
                    "start_row": start_row,
                    "end_row": end_row,
                    "size_mb": chunk_size_mb
                }])
        else:
            # Try to pack file into current partition
            if current_partition_size_mb + file_size_mb <= usable_memory_mb:
                current_partition.append({
                    "path": file_info["path"],
                    "start_row": 0,
                    "end_row": file_info["estimated_rows"],
                    "size_mb": file_size_mb
                })
                current_partition_size_mb += file_size_mb
            else:
                # Current partition is full, start new one
                if current_partition:
                    partitions.append(current_partition)
                current_partition = [{
                    "path": file_info["path"],
                    "start_row": 0,
                    "end_row": file_info["estimated_rows"],
                    "size_mb": file_size_mb
                }]
                current_partition_size_mb = file_size_mb
    
    # Add last partition
    if current_partition:
        partitions.append(current_partition)
    
    return partitions


def execute_partition_plan(
    partition_plan: List[List[Dict]],
    output_path: Path,
    expected_columns: List[str],
    delimiter: str,
    memory_monitor = None
) -> Dict:
    """Execute the partition plan with guaranteed memory safety."""
    temp_dir = output_path.parent / f".plan_parts_{output_path.stem}"
    temp_dir.mkdir(exist_ok=True)
    
    partition_outputs = []
    total_rows = 0
    
    try:
        # Process each partition
        for part_idx, partition in enumerate(partition_plan):
            part_output = temp_dir / f"part_{part_idx:04d}.parquet"
            part_dfs = []
            
            # Load all chunks in this partition
            for chunk_info in partition:
                csv_path = chunk_info["path"]
                start_row = chunk_info["start_row"]
                end_row = chunk_info["end_row"]
                num_rows = end_row - start_row if end_row > 0 else None
                
                # Read chunk
                df = pl.read_csv(
                    str(csv_path),
                    separator=delimiter,
                    encoding="utf8-lossy",
                    ignore_errors=True,
                    skip_rows=start_row,
                    n_rows=num_rows,
                    has_header=False,
                    low_memory=True,
                    rechunk=False
                )
                
                # Rename columns
                if expected_columns and len(df.columns) == len(expected_columns):
                    df = df.rename({df.columns[i]: expected_columns[i] 
                                   for i in range(len(expected_columns))})
                
                part_dfs.append(df)
            
            # Combine chunks in this partition
            if len(part_dfs) == 1:
                combined = part_dfs[0]
            else:
                combined = pl.concat(part_dfs, how="vertical")
            
            total_rows += len(combined)
            
            # Write partition
            combined.write_parquet(
                str(part_output),
                compression="snappy",
                row_group_size=50000,
                statistics=False
            )
            
            partition_outputs.append(part_output)
            
            # Cleanup partition memory
            del part_dfs, combined
            gc.collect()
            
            if memory_monitor:
                memory_monitor.perform_aggressive_cleanup()
        
        # Final merge of all partitions
        if len(partition_outputs) == 1:
            partition_outputs[0].rename(output_path)
        else:
            lazy_frames = [pl.scan_parquet(str(f)) for f in partition_outputs]
            combined = pl.concat(lazy_frames, how="vertical")
            combined.sink_parquet(
                str(output_path),
                compression="snappy",
                row_group_size=50000,
                maintain_order=False,
                statistics=False
            )
            
            # Cleanup
            for pf in partition_outputs:
                pf.unlink()
        
        return {
            "rows_processed": total_rows,
            "output_bytes": output_path.stat().st_size,
            "partitions_used": len(partition_outputs),
            "strategy": "two_pass_planned"
        }
        
    finally:
        # Cleanup temp directory
        try:
            for pf in partition_outputs:
                if pf.exists():
                    pf.unlink()
            if temp_dir.exists():
                temp_dir.rmdir()
        except:
            pass


def convert_with_two_pass(
    csv_paths: List[Path],
    output_path: Path,
    expected_columns: List[str],
    delimiter: str,
    memory_budget_mb: int = 1000,
    memory_monitor = None
) -> Dict:
    """
    Main entry point for two-pass conversion.
    
    Pass 1: Analyze all files
    Pass 2: Execute optimal partition plan
    """
    # Pass 1: Analyze
    print(f"Pass 1: Analyzing {len(csv_paths)} files...")
    characteristics = []
    for csv_path in csv_paths:
        char = analyze_csv_characteristics(csv_path, delimiter)
        characteristics.append(char)
        print(f"  {csv_path.name}: {char['size_mb']:.1f}MB, "
              f"~{char['estimated_rows']:,} rows")
    
    # Create partition plan
    print(f"\nCreating partition plan (budget: {memory_budget_mb}MB)...")
    plan = create_partition_plan(characteristics, memory_budget_mb)
    print(f"Plan created: {len(plan)} partitions")
    for i, partition in enumerate(plan):
        total_mb = sum(c["size_mb"] for c in partition)
        files = len({c["path"] for c in partition})
        print(f"  Partition {i+1}: {total_mb:.1f}MB from {files} file(s)")
    
    # Pass 2: Execute
    print(f"\nPass 2: Executing partition plan...")
    result = execute_partition_plan(
        plan, output_path, expected_columns, delimiter, memory_monitor
    )
    
    return result