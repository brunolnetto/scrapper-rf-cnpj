"""
Deterministic CSV to Parquet conversion utilities for 1-10GB files.
Precise control over memory usage and processing parameters.
"""
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import gc
import time
from typing import List, Dict, Optional, Callable
from dataclasses import dataclass

try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
    psutil = None

from rich.progress import (
    Progress,
    SpinnerColumn,
    BarColumn,
    TextColumn,
    TimeElapsedColumn,
    MofNCompleteColumn,
    TimeRemainingColumn,
)

from .model_utils import get_table_columns
import logging

logger = logging.getLogger(__name__)
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

from ..setup.config import ConfigurationService
import polars as pl

@dataclass
class ProcessingConfig:
    """Fixed processing configuration - no estimation or adaptation."""
    chunk_size: int = 50_000  # Fixed chunk size
    max_memory_mb: int = 1024  # Hard memory limit
    row_group_size: int = 50_000  # Fixed parquet row group size
    workers: int = 1  # Fixed worker count
    compression: str = "snappy"  # Fixed compression

@dataclass
class ConversionStats:
    """Deterministic statistics tracking."""
    files_processed: int = 0
    files_failed: int = 0
    total_rows_processed: int = 0
    total_bytes_processed: int = 0
    start_time: float = 0.0

    def __post_init__(self):
        self.start_time = time.time()

    @property
    def elapsed_time(self) -> float:
        return time.time() - self.start_time

    @property
    def processing_rate_mb_per_sec(self) -> float:
        elapsed = self.elapsed_time
        if elapsed > 0:
            return (self.total_bytes_processed / (1024 * 1024)) / elapsed
        return 0.0

    def record_file(self, bytes_processed: int, rows_processed: int, success: bool):
        if success:
            self.files_processed += 1
            self.total_rows_processed += rows_processed
            self.total_bytes_processed += bytes_processed
        else:
            self.files_failed += 1

class PreciseMemoryMonitor:
    """Deterministic memory monitoring with hard limits."""

    def __init__(self, max_memory_mb: int):
        self.max_memory_mb = max_memory_mb
        self.hard_limit_mb = max_memory_mb
        self.process = None

        if PSUTIL_AVAILABLE:
            try:
                self.process = psutil.Process()
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                logger.warning("Cannot monitor process memory")

    def get_current_memory_mb(self) -> float:
        """Get exact current memory usage in MB."""
        if self.process and PSUTIL_AVAILABLE:
            try:
                return self.process.memory_info().rss / (1024 * 1024)
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
        return 0.0

    def get_available_system_memory_mb(self) -> float:
        """Get exact available system memory in MB."""
        if PSUTIL_AVAILABLE:
            try:
                return psutil.virtual_memory().available / (1024 * 1024)
            except Exception:
                pass
        return float('inf')  # Assume unlimited if can't measure

    def is_memory_limit_exceeded(self) -> bool:
        """Hard check against memory limit."""
        current_mb = self.get_current_memory_mb()
        return current_mb > self.hard_limit_mb

    def force_memory_cleanup(self) -> float:
        """Force immediate memory cleanup and return freed amount."""
        before_mb = self.get_current_memory_mb()
        gc.collect()
        gc.collect()  # Double collection for thoroughness
        after_mb = self.get_current_memory_mb()
        return max(0, before_mb - after_mb)

def get_file_size_bytes(file_path: Path) -> int:
    """Get exact file size in bytes."""
    if not file_path.exists():
        return 0
    try:
        return file_path.stat().st_size
    except (OSError, PermissionError):
        return 0

def process_csv_direct_streaming(
    csv_path: Path,
    output_path: Path,
    schema: Dict[str, pl.DataType],
    delimiter: str,
    config: ProcessingConfig,
    memory_monitor: PreciseMemoryMonitor,
    progress_callback: Optional[Callable[[int], None]] = None
) -> Dict[str, int]:
    """
    Process CSV using direct streaming - no chunking, no estimation.
    Returns exact metrics.
    """
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    input_bytes = get_file_size_bytes(csv_path)
    logger.info(f"Processing {csv_path.name} ({input_bytes:,} bytes)")

    try:
        # Ensure output directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Direct streaming conversion with fixed parameters
        lazy_frame = pl.scan_csv(
            str(csv_path),
            separator=delimiter,
            schema=schema,
            encoding="utf8-lossy",
            ignore_errors=True,
            truncate_ragged_lines=True,
            null_values=["", "NULL", "null", "N/A", "n/a"],
            try_parse_dates=False,
            low_memory=True,
            rechunk=False
        )

        # Stream directly to parquet with fixed settings
        lazy_frame.sink_parquet(
            str(output_path),
            compression=config.compression,
            row_group_size=config.row_group_size,
            maintain_order=False
        )

        # Get exact output metrics
        output_bytes = get_file_size_bytes(output_path)

        # Count actual rows processed
        rows_processed = pl.scan_parquet(str(output_path)).select(pl.len()).collect().item()

        if progress_callback:
            progress_callback(rows_processed)

        logger.info(f"✅ Processed {csv_path.name}: {rows_processed:,} rows, "
                   f"{input_bytes:,} → {output_bytes:,} bytes")

        return {
            "rows_processed": rows_processed,
            "input_bytes": input_bytes,
            "output_bytes": output_bytes
        }

    except Exception as e:
        # Clean up on failure
        if output_path.exists():
            try:
                output_path.unlink()
            except:
                pass
        raise e

def combine_parquet_files_deterministic(
    parquet_files: List[Path],
    output_path: Path,
    config: ProcessingConfig,
    memory_monitor: PreciseMemoryMonitor
) -> Dict[str, int]:
    """
    Combine parquet files with deterministic processing.
    """
    if not parquet_files:
        raise ValueError("No parquet files to combine")

    valid_files = [f for f in parquet_files if f.exists()]
    if not valid_files:
        raise ValueError("No valid parquet files found")

    if len(valid_files) == 1:
        # Simple case: rename single file
        valid_files[0].rename(output_path)
        output_bytes = get_file_size_bytes(output_path)
        rows = pl.scan_parquet(str(output_path)).select(pl.len()).collect().item()
        return {"total_rows": rows, "output_bytes": output_bytes}

    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Use lazy concatenation for memory efficiency
        lazy_frames = [pl.scan_parquet(str(f)) for f in valid_files]
        combined = pl.concat(lazy_frames)

        # Stream combined result to output
        combined.sink_parquet(
            str(output_path),
            compression=config.compression,
            row_group_size=config.row_group_size,
            maintain_order=False
        )

        # Get exact metrics
        output_bytes = get_file_size_bytes(output_path)
        total_rows = pl.scan_parquet(str(output_path)).select(pl.len()).collect().item()

        logger.info(f"Combined {len(valid_files)} files: {total_rows:,} rows, {output_bytes:,} bytes")

        return {"total_rows": total_rows, "output_bytes": output_bytes}

    except Exception as e:
        # Clean up failed output
        if output_path.exists():
            try:
                output_path.unlink()
            except:
                pass
        raise e

def convert_table_csvs_deterministic(
    table_name: str,
    csv_paths: List[Path],
    output_dir: Path,
    delimiter: str,
    expected_columns: List[str],
    config: ProcessingConfig
) -> str:
    """
    Deterministic CSV to Parquet conversion with precise control.
    """
    stats = ConversionStats()
    memory_monitor = PreciseMemoryMonitor(config.max_memory_mb)

    try:
        if not expected_columns:
            return f"[ERROR] No column mapping for '{table_name}'"

        # Filter to existing files only
        valid_files = [p for p in csv_paths if p.exists()]
        if not valid_files:
            return f"[ERROR] No valid CSV files for '{table_name}'"

        # Setup
        output_dir.mkdir(parents=True, exist_ok=True)
        final_output = output_dir / f"{table_name}.parquet"

        if final_output.exists():
            final_output.unlink()

        schema = {col: pl.Utf8 for col in expected_columns}

        # Calculate exact input size
        total_input_bytes = sum(get_file_size_bytes(p) for p in valid_files)

        logger.info(f"🚀 Converting '{table_name}': {len(valid_files)} files, "
                   f"{total_input_bytes:,} bytes")

        processed_parquet_files = []

        # Process each CSV file individually
        for i, csv_path in enumerate(valid_files, 1):
            file_bytes = get_file_size_bytes(csv_path)
            temp_output = output_dir / f"{table_name}_part_{i:03d}.parquet"

            try:
                logger.info(f"Processing file {i}/{len(valid_files)}: {csv_path.name}")

                # Check memory before processing
                if memory_monitor.is_memory_limit_exceeded():
                    freed_mb = memory_monitor.force_memory_cleanup()
                    logger.info(f"Memory cleanup freed {freed_mb:.1f}MB")

                    if memory_monitor.is_memory_limit_exceeded():
                        raise RuntimeError(f"Memory limit exceeded: "
                                         f"{memory_monitor.get_current_memory_mb():.1f}MB > "
                                         f"{config.max_memory_mb}MB")

                # Process file with progress tracking
                file_progress = {"rows": 0}

                def progress_callback(rows):
                    file_progress["rows"] = rows

                result = process_csv_direct_streaming(
                    csv_path, temp_output, schema, delimiter,
                    config, memory_monitor, progress_callback
                )

                processed_parquet_files.append(temp_output)
                stats.record_file(file_bytes, result["rows_processed"], True)

                logger.info(f"✅ File {i}/{len(valid_files)} completed: "
                           f"{result['rows_processed']:,} rows")

            except Exception as e:
                logger.error(f"❌ Failed file {i}/{len(valid_files)} ({csv_path.name}): {e}")
                stats.record_file(file_bytes, 0, False)
                continue

        if not processed_parquet_files:
            return f"[ERROR] No files successfully processed for '{table_name}'"

        # Combine all processed parquet files
        logger.info(f"🔄 Combining {len(processed_parquet_files)} parquet files...")

        try:
            result = combine_parquet_files_deterministic(
                processed_parquet_files, final_output, config, memory_monitor
            )

            # Clean up individual part files
            for part_file in processed_parquet_files:
                if part_file.exists() and part_file != final_output:
                    try:
                        part_file.unlink()
                    except:
                        pass

            # Final metrics
            final_bytes = get_file_size_bytes(final_output)
            compression_ratio = total_input_bytes / final_bytes if final_bytes > 0 else 0

            result_msg = (f"[OK] '{table_name}': {result['total_rows']:,} rows, "
                         f"{stats.files_processed}/{len(valid_files)} files, "
                         f"{final_bytes:,} bytes ({compression_ratio:.1f}x compression), "
                         f"{stats.processing_rate_mb_per_sec:.1f} MB/sec")

            logger.info(f"🎉 Completed '{table_name}' in {stats.elapsed_time:.1f}s")
            return result_msg

        except Exception as e:
            logger.error(f"Failed to combine files for '{table_name}': {e}")
            return f"[ERROR] Failed to combine '{table_name}': {e}"

    except Exception as e:
        logger.error(f"Conversion failed for '{table_name}': {e}")
        return f"[ERROR] Failed '{table_name}': {e}"

def convert_csvs_to_parquet_deterministic(
    audit_map: dict,
    unzip_dir: Path,
    output_dir: Path,
    config: Optional[ProcessingConfig] = None,
    delimiter: str = ";"
):
    """
    Main conversion function with deterministic processing.
    """
    if config is None:
        # Get configuration from service or use defaults
        try:
            from ..setup.config import get_config
            config_service = get_config()
            etl_config = config_service.etl
            config = ProcessingConfig(
                max_memory_mb=getattr(etl_config, 'conversion_max_memory_mb', 1024),
                workers=getattr(etl_config, 'conversion_workers', 1),
                chunk_size=getattr(etl_config, 'conversion_chunk_size', 50_000),
            )
        except:
            config = ProcessingConfig()  # Use defaults

    output_dir.mkdir(exist_ok=True)

    if not audit_map:
        logger.warning("No tables to convert")
        return

    logger.info(f"🚀 Starting deterministic conversion of {len(audit_map)} tables")
    logger.info(f"Configuration: {config.workers} workers, {config.max_memory_mb}MB memory limit, "
               f"{config.chunk_size:,} chunk size")

    # Log precise system info
    if PSUTIL_AVAILABLE:
        try:
            memory = psutil.virtual_memory()
            logger.info(f"System Memory: {memory.available/(1024**3):.2f}GB available, "
                       f"{memory.percent:.1f}% used")
            cpu_count = os.cpu_count() or 1
            logger.info(f"System CPUs: {cpu_count} cores available")
        except:
            logger.warning("Could not get system info")

    # Sort tables by file size (deterministic order)
    table_work = []
    for table_name, zip_map in audit_map.items():
        csv_paths = [unzip_dir / fname for files in zip_map.values() for fname in files]
        valid_paths = [p for p in csv_paths if p.exists()]

        if not valid_paths:
            logger.warning(f"No valid files for '{table_name}'")
            continue

        total_bytes = sum(get_file_size_bytes(p) for p in valid_paths)
        table_work.append((table_name, zip_map, valid_paths, total_bytes))

    # Sort by size (largest first) for consistent processing order
    table_work.sort(key=lambda x: x[3], reverse=True)

    logger.info(f"Processing {len(table_work)} tables with valid data")

    # Process tables
    with ThreadPoolExecutor(max_workers=config.workers) as executor:
        with Progress(
            SpinnerColumn(),
            TextColumn("[bold green]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
        ) as progress:

            main_task = progress.add_task("Converting tables", total=len(table_work))

            # Submit tasks
            tasks = {}
            for table_name, zip_map, csv_paths, total_bytes in table_work:
                expected_columns = get_table_columns(table_name)

                logger.info(f"📁 Queuing '{table_name}': {len(csv_paths)} files, {total_bytes:,} bytes")

                task = executor.submit(
                    convert_table_csvs_deterministic,
                    table_name,
                    csv_paths,
                    output_dir,
                    delimiter,
                    expected_columns,
                    config
                )

                tasks[task] = (table_name, total_bytes, len(csv_paths))

            # Process results
            completed = 0
            success_count = 0
            error_count = 0
            total_bytes_processed = 0

            for future in as_completed(tasks):
                table_name, table_bytes, file_count = tasks[future]

                try:
                    result = future.result()

                    if "[OK]" in result:
                        logger.info(f"✅ {result}")
                        success_count += 1
                        total_bytes_processed += table_bytes
                    else:
                        logger.error(f"❌ {result}")
                        error_count += 1

                except Exception as e:
                    logger.error(f"❌ Exception in '{table_name}': {e}")
                    error_count += 1

                completed += 1
                progress.update(main_task, completed=completed)

                # Force cleanup between tasks
                gc.collect()

    # Final deterministic summary
    total_mb = total_bytes_processed / (1024 * 1024)
    logger.info(f"🎉 Conversion Summary:")
    logger.info(f"   ✅ Successful: {success_count} tables")
    logger.info(f"   ❌ Failed: {error_count} tables")
    logger.info(f"   📊 Total processed: {total_bytes_processed:,} bytes ({total_mb:.1f}MB)")
    logger.info(f"   📁 Output directory: {output_dir}")

# Legacy utility functions for backward compatibility
def count_file_rows(file_path: Path, delimiter: str = ";", encoding: str = None, skip_header: bool = None) -> dict:
    """
    Count rows in a file and return detailed information.

    Args:
        file_path: Path to the file to analyze
        delimiter: CSV delimiter for format detection
        encoding: File encoding (auto-detect if None)
        skip_header: Whether to skip header row (auto-detect if None)

    Returns:
        Dictionary with row count information:
        {
            'total_lines': int,      # Total lines in file
            'data_rows': int,        # Data rows (excluding header if applicable)
            'has_header': bool,      # Whether file appears to have header
            'is_csv_like': bool,     # Whether file appears to be CSV format
            'encoding_used': str,    # Encoding that was successfully used
            'file_size_mb': float    # File size in MB
        }
    """
    result = {
        'total_lines': 0,
        'data_rows': 0,
        'has_header': False,
        'is_csv_like': False,
        'encoding_used': None,
        'file_size_mb': 0.0
    }

    if not file_path.exists():
        return result

    try:
        file_size = file_path.stat().st_size
        result['file_size_mb'] = file_size / (1024 * 1024)
    except (OSError, PermissionError):
        pass

    # Try different encodings
    encodings_to_try = [encoding] if encoding else ["utf-8", "latin-1", "cp1252", "iso-8859-1"]

    for enc in encodings_to_try:
        try:
            with open(file_path, 'r', encoding=enc, errors='replace') as f:
                # Read first few lines to analyze structure
                first_lines = []
                total_lines = 0

                for line in f:
                    total_lines += 1
                    if len(first_lines) < 5:
                        first_lines.append(line)

                result['total_lines'] = total_lines
                result['encoding_used'] = enc

                # Analyze structure
                if first_lines:
                    result['is_csv_like'] = any(delimiter in line for line in first_lines)

                    # Simple heuristic for header detection
                    if len(first_lines) >= 2:
                        first_line_words = len(first_lines[0].split())
                        second_line_words = len(first_lines[1].split())
                        result['has_header'] = first_line_words > second_line_words

                    # Calculate data rows
                    if result['has_header'] and skip_header is not False:
                        result['data_rows'] = max(0, total_lines - 1)
                    else:
                        result['data_rows'] = total_lines

                break  # Success with this encoding

        except Exception as e:
            logger.debug(f"Failed to analyze {file_path.name} with encoding {enc}: {e}")
            continue

    # If all encodings failed, try binary mode
    if result['total_lines'] == 0:
        try:
            with open(file_path, 'rb') as f:
                content = f.read()
                line_count = content.count(b'\n')
                if content and not content.endswith(b'\n'):
                    line_count += 1
                result['total_lines'] = line_count
                result['data_rows'] = line_count
                result['encoding_used'] = 'binary'
        except Exception as e:
            logger.warning(f"Could not analyze file {file_path.name}: {e}")

    return result

def get_exact_row_count(file_path: Path, delimiter: str = ";", encoding: str = None, skip_header: bool = None) -> int:
    """
    Get exact row count from any file, handling encoding issues and large files efficiently.

    Args:
        file_path: Path to the file to count rows in
        delimiter: CSV delimiter (used to detect if it's a CSV file)
        encoding: File encoding. If None, will try multiple encodings
        skip_header: Whether to skip header row for CSV files. If None, auto-detect.

    Returns:
        Exact number of rows in the file (excluding header if CSV and skip_header=True)
    """
    if not file_path.exists():
        return 0

    # Try different encodings if not specified
    encodings_to_try = [encoding] if encoding else ["utf-8", "latin-1", "cp1252", "iso-8859-1"]

    for enc in encodings_to_try:
        try:
            with open(file_path, 'r', encoding=enc, errors='replace') as f:
                lines = f.readlines()
                total_lines = len(lines)

                # Auto-detect header if not specified
                if skip_header is None and total_lines >= 2:
                    # Simple heuristic: header likely has fewer numeric values
                    first_line = lines[0].strip()
                    second_line = lines[1].strip()
                    first_numeric = sum(c.isdigit() for c in first_line)
                    second_numeric = sum(c.isdigit() for c in second_line)
                    has_header = first_numeric < second_numeric
                else:
                    has_header = bool(skip_header)

                if has_header:
                    return max(0, total_lines - 1)
                else:
                    return total_lines

        except Exception as e:
            logger.debug(f"Failed to count rows with encoding {enc}: {e}")
            continue

    # If all encodings failed, try binary mode as last resort
    try:
        with open(file_path, 'rb') as f:
            content = f.read()
            # Count newline characters (works for most text files)
            line_count = content.count(b'\n')
            if content and not content.endswith(b'\n'):
                line_count += 1
            return line_count
    except Exception as e:
        logger.warning(f"Could not count rows in {file_path.name}: {e}")
        return 0

# Backwards compatibility functions
def convert_table_csvs_to_parquet(table_name, csv_paths, output_dir, delimiter):
    """Original function signature for backwards compatibility."""
    expected_columns = get_table_columns(table_name)
    config = ProcessingConfig()  # Use default config
    return convert_table_csvs_deterministic(
        table_name, csv_paths, output_dir, delimiter, expected_columns, config
    )

def convert_csvs_to_parquet(
    audit_map: dict,
    unzip_dir: Path,
    output_dir: Path,
    max_workers: int = 4,
    delimiter: str = ";"
):
    """Enhanced function signature for backwards compatibility."""
    config = ProcessingConfig()
    config.workers = max_workers
    return convert_csvs_to_parquet_deterministic(audit_map, unzip_dir, output_dir, config, delimiter)
