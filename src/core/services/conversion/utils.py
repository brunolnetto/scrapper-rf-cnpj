"""
Stream-merge architecture: Process multiple large CSV files simultaneously
by reading small chunks from each and writing incrementally to output.

Two-pass approach: 
Pass 1: Analyze all files and create optimal partition plan
Pass 2: Execute plan with guaranteed memory safety
"""
from pathlib import Path
import polars as pl
import gc
import os
from typing import Optional, Dict

from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
import polars as pl
import os


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

# helper: estimate avg row bytes (from small sample)
def estimate_avg_row_bytes(csv_path: Path, delimiter: str, sample_rows: int = 1000) -> int:
    sample = pl.read_csv(
        str(csv_path),
        separator=delimiter,
        n_rows=sample_rows,
        has_header=False,
        encoding="utf8-lossy",
        ignore_errors=True,
    )
    if sample.height == 0:
        return 200  # fallback

    # Option A: use Polars built-in estimate (preferred if your version has it)
    try:
        mem_bytes = sample.estimated_size()
    except AttributeError:
        # Option B: approximate via file size vs row count
        file_size = csv_path.stat().st_size
        mem_bytes = int(file_size * (len(sample) / max(1, sample_rows)))

    avg_row_bytes = max(50, int(mem_bytes / max(1, sample.height)))
    del sample
    gc.collect()
    return avg_row_bytes

def compute_chunk_size_for_file(path: Path, available_mb: int, default_min_rows=10000, default_max_rows=500000):
    avg_row_bytes = estimate_avg_row_bytes(path, delimiter=',')
    usable_bytes = max(available_mb * 1024 * 1024 * 0.6, 10 * 1024 * 1024)  # leave 40% buffer
    rows = max(default_min_rows, int(usable_bytes / avg_row_bytes))
    return min(rows, default_max_rows)

def infer_schema(csv_path: Path, delimiter: str, sample_size: int = 10000) -> Dict:
    """
    Infer optimal schema with reduced sample size for safety.
    """
    
    try:
        # Smaller sample for safety - reduced from 50000 to 10000
        sample_df = pl.read_csv(
            str(csv_path),
            separator=delimiter,
            n_rows=sample_size,
            encoding="utf8-lossy",
            ignore_errors=True,
            truncate_ragged_lines=True,
            null_values=["", "NULL", "null", "N/A", "n/a"],
            infer_schema_length=min(sample_size, 5000),  # Further reduced
            try_parse_dates=False,  # Disable for safety and speed
            has_header=False
        )
        
        schema = sample_df.schema
        
        # Clean up sample immediately
        del sample_df
        gc.collect()
        
        return dict(schema)
        
    except Exception as e:
        return None
