"""
Improved memory management system for CSV to Parquet conversion.
Key improvements: proper schema inference, true streaming, memory cleanup between files.
"""
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import gc
import time
from typing import List, Dict, Optional, Callable, Any
from dataclasses import dataclass
import threading

import polars as pl

from ...setup.config import ConversionConfig
from ...setup.logging import logger

try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
    psutil = None


@dataclass
class MemorySnapshot:
    """Snapshot of memory usage at a point in time."""
    timestamp: float
    process_rss_mb: float
    process_vms_mb: float
    system_available_mb: float
    system_used_percent: float

    def __post_init__(self):
        if self.timestamp == 0:
            self.timestamp = time.time()

class MemoryMonitor:
    """
    Enhanced memory monitoring with better cleanup between operations.
    """

    def __init__(self, config: ConversionConfig):
        self.config = config
        self.baseline_snapshot: Optional[MemorySnapshot] = None
        self.process = None
        self.lock = threading.Lock()
        self.last_cleanup_time = 0
        
        if PSUTIL_AVAILABLE:
            try:
                self.process = psutil.Process()
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                logger.warning("Cannot monitor process memory - using fallback mode")
        
        # Establish baseline immediately
        self._establish_baseline()

    def _get_current_memory_info(self) -> MemorySnapshot:
        """Get current comprehensive memory information."""
        if self.process and PSUTIL_AVAILABLE:
            try:
                proc_info = self.process.memory_info()
                sys_info = psutil.virtual_memory()
                
                return MemorySnapshot(
                    timestamp=time.time(),
                    process_rss_mb=proc_info.rss / (1024 * 1024),
                    process_vms_mb=proc_info.vms / (1024 * 1024),
                    system_available_mb=sys_info.available / (1024 * 1024),
                    system_used_percent=sys_info.percent
                )
            except (psutil.NoSuchProcess, psutil.AccessDenied, AttributeError):
                pass
        
        # Fallback when psutil is unavailable
        return MemorySnapshot(
            timestamp=time.time(),
            process_rss_mb=0.0,
            process_vms_mb=0.0,
            system_available_mb=float('inf'),
            system_used_percent=0.0
        )

    def _establish_baseline(self) -> None:
        """Establish baseline memory usage before processing starts."""
        with self.lock:
            # Take multiple samples to get stable baseline
            samples = []
            for _ in range(3):
                samples.append(self._get_current_memory_info())
                time.sleep(0.1)
            
            # Use median values for stability
            rss_values = [s.process_rss_mb for s in samples]
            vms_values = [s.process_vms_mb for s in samples]
            
            rss_values.sort()
            vms_values.sort()
            median_idx = len(rss_values) // 2
            
            self.baseline_snapshot = MemorySnapshot(
                timestamp=time.time(),
                process_rss_mb=rss_values[median_idx],
                process_vms_mb=vms_values[median_idx],
                system_available_mb=samples[-1].system_available_mb,
                system_used_percent=samples[-1].system_used_percent
            )
            
        logger.info(f"Memory baseline established: "
                   f"Process={self.baseline_snapshot.process_rss_mb:.1f}MB, "
                   f"System available={self.baseline_snapshot.system_available_mb:.1f}MB")

    def get_memory_usage_above_baseline(self) -> float:
        """Get current memory usage above the established baseline in MB."""
        if not self.baseline_snapshot:
            return 0.0
            
        current = self._get_current_memory_info()
        return max(0.0, current.process_rss_mb - self.baseline_snapshot.process_rss_mb)

    def get_available_memory_budget(self) -> float:
        """Get remaining memory budget in MB considering baseline."""
        usage_above_baseline = self.get_memory_usage_above_baseline()
        return max(0.0, self.config.max_memory_mb - usage_above_baseline)

    def get_memory_pressure_level(self) -> float:
        """Get memory pressure as a ratio (0.0 = no pressure, 1.0 = at limit)."""
        usage_above_baseline = self.get_memory_usage_above_baseline()
        if self.config.max_memory_mb <= 0:
            return 0.0
        return min(1.0, usage_above_baseline / self.config.max_memory_mb)

    def is_memory_pressure_high(self) -> bool:
        """Check if memory pressure is above cleanup threshold."""
        return self.get_memory_pressure_level() >= self.config.cleanup_threshold_ratio

    def is_memory_limit_exceeded(self) -> bool:
        """Check if memory usage exceeds the configured limit above baseline."""
        usage_above_baseline = self.get_memory_usage_above_baseline()
        return usage_above_baseline > self.config.max_memory_mb

    def should_prevent_processing(self) -> bool:
        """
        Determine if processing should be prevented due to memory constraints.
        This considers both process memory and system-wide memory availability.
        """
        if self.is_memory_limit_exceeded():
            return True
            
        # Also check system memory availability
        current = self._get_current_memory_info()
        if current.system_available_mb < self.config.baseline_buffer_mb:
            logger.warning(f"Low system memory: {current.system_available_mb:.1f}MB available")
            return True
            
        if current.system_used_percent > 90:
            logger.warning(f"High system memory usage: {current.system_used_percent:.1f}%")
            return True
            
        return False

    def perform_aggressive_cleanup(self) -> Dict[str, float]:
        """
        Perform more aggressive cleanup between large files.
        """
        with self.lock:
            now = time.time()
            if now - self.last_cleanup_time < 0.5:  # Reduced rate limit
                return {"skipped": True}
                
            self.last_cleanup_time = now
            
        before_snapshot = self._get_current_memory_info()
        
        # Multi-stage aggressive cleanup
        cleanup_stats = {
            "before_mb": before_snapshot.process_rss_mb,
            "cleanup_type": "aggressive_inter_file"
        }
        
        # Stage 1: Multiple garbage collections
        for i in range(5):
            gc.collect()
            
        # Stage 2: Clear generation-specific collections
        for gen in range(3):
            try:
                gc.collect(gen)
            except:
                pass
                
        # Stage 3: Force Polars cleanup if available
        try:
            # Clear any cached state in Polars
            import polars as pl
            pl.clear_schema_cache()
        except:
            pass
            
        # Stage 4: OS-level hints
        if PSUTIL_AVAILABLE and hasattr(os, 'sync'):
            try:
                os.sync()  # Flush OS buffers
            except:
                pass
        
        time.sleep(0.1)  # Brief pause for cleanup to take effect
        
        after_snapshot = self._get_current_memory_info()
        cleanup_stats["after_mb"] = after_snapshot.process_rss_mb
        cleanup_stats["freed_mb"] = max(0, before_snapshot.process_rss_mb - after_snapshot.process_rss_mb)
        
        if cleanup_stats["freed_mb"] > 0:
            logger.info(f"Aggressive cleanup freed {cleanup_stats['freed_mb']:.1f}MB")
        
        return cleanup_stats

    def get_status_report(self) -> Dict[str, any]:
        """Get comprehensive memory status report."""
        current = self._get_current_memory_info()
        usage_above_baseline = self.get_memory_usage_above_baseline()
        budget_remaining = self.get_available_memory_budget()
        pressure = self.get_memory_pressure_level()
        
        return {
            "timestamp": current.timestamp,
            "baseline_mb": self.baseline_snapshot.process_rss_mb if self.baseline_snapshot else 0,
            "current_process_mb": current.process_rss_mb,
            "usage_above_baseline_mb": usage_above_baseline,
            "configured_limit_mb": self.config.max_memory_mb,
            "budget_remaining_mb": budget_remaining,
            "pressure_level": pressure,
            "system_available_mb": current.system_available_mb,
            "system_used_percent": current.system_used_percent,
            "should_cleanup": self.is_memory_pressure_high(),
            "should_block": self.should_prevent_processing()
        }

def infer_optimal_schema(csv_path: Path, delimiter: str, sample_size: int = 100000) -> Dict:
    """
    Infer optimal schema instead of using all strings.
    This is crucial for memory efficiency.
    """
    logger.info(f"Inferring schema for {csv_path.name}...")
    
    try:
        # Sample a portion of the file for schema inference
        sample_df = pl.read_csv(
            str(csv_path),
            separator=delimiter,
            n_rows=sample_size,
            encoding="utf8-lossy",
            ignore_errors=True,
            truncate_ragged_lines=True,
            null_values=["", "NULL", "null", "N/A", "n/a"],
            infer_schema_length=sample_size,  # Key: let Polars infer proper types
            try_parse_dates=True
        )
        
        schema = sample_df.schema
        logger.info(f"Inferred schema for {csv_path.name}: {len(schema)} columns, "
                   f"types: {dict(schema)}")
        
        # Clean up sample
        del sample_df
        gc.collect()
        
        return dict(schema)
        
    except Exception as e:
        logger.warning(f"Schema inference failed for {csv_path.name}: {e}. Using string fallback.")
        # Fallback to string schema - but this should be avoided
        return None

def process_csv_with_memory_v2(
    csv_path: Path,
    output_path: Path,
    expected_columns: List[str],
    delimiter: str,
    config: ConversionConfig,
    memory_monitor: MemoryMonitor,
    progress_callback: Optional[Callable[[int], None]] = None
) -> Dict[str, any]:
    """
    Improved CSV processing with proper streaming and schema inference.
    """
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    input_bytes = csv_path.stat().st_size
    logger.info(f"Processing {csv_path.name} ({input_bytes:,} bytes)")

    # Pre-processing memory check
    if memory_monitor.should_prevent_processing():
        status = memory_monitor.get_status_report()
        raise RuntimeError(f"Insufficient memory to process file. "
                         f"Usage: {status['usage_above_baseline_mb']:.1f}MB above baseline, "
                         f"Limit: {status['configured_limit_mb']}MB")

    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # IMPROVEMENT 1: Infer proper schema instead of all strings
        inferred_schema = infer_optimal_schema(csv_path, delimiter)
        
        # If schema inference failed, use a more memory-efficient fallback
        if inferred_schema is None:
            # Only use string for columns we expect, let Polars infer others
            schema_override = {col: pl.Utf8 for col in expected_columns}
        else:
            schema_override = inferred_schema

        # IMPROVEMENT 2: Use streaming-optimized scan configuration
        lazy_frame = pl.scan_csv(
            str(csv_path),
            separator=delimiter,
            schema_overrides=schema_override,  # Use schema_overrides instead of schema
            encoding="utf8-lossy",
            ignore_errors=True,
            truncate_ragged_lines=True,
            null_values=["", "NULL", "null", "N/A", "n/a"],
            try_parse_dates=True,  # Let Polars handle date parsing
            low_memory=True,
            rechunk=False,
            infer_schema_length=50000  # Reasonable inference length
        )

        # IMPROVEMENT 3: Add memory-efficient transformations
        # Filter to only expected columns if we have them
        if expected_columns:
            available_cols = []
            try:
                # Check which expected columns actually exist
                schema_cols = set(lazy_frame.schema.keys())
                available_cols = [col for col in expected_columns if col in schema_cols]
                
                if available_cols:
                    lazy_frame = lazy_frame.select(available_cols)
                    logger.info(f"Selected {len(available_cols)} of {len(expected_columns)} expected columns")
            except Exception as e:
                logger.warning(f"Column selection failed: {e}")

        # IMPROVEMENT 4: Use streaming sink with optimal parameters
        logger.info(f"Streaming {csv_path.name} directly to Parquet...")
        
        # Configure sink for maximum memory efficiency
        lazy_frame.sink_parquet(
            str(output_path),
            compression=config.compression,
            row_group_size=min(config.row_group_size, 50000),  # Smaller row groups for big files
            maintain_order=False,
            statistics=False,  # Disable statistics for memory savings
            compression_level=1 if config.compression in ['zstd', 'gzip'] else None  # Fast compression
        )

        # IMPROVEMENT 5: Immediate cleanup after sink
        del lazy_frame
        gc.collect()
        gc.collect()

        # Verify output and collect minimal metrics
        if not output_path.exists():
            raise RuntimeError("Output file was not created")
            
        output_bytes = output_path.stat().st_size
        
        # Get row count efficiently without loading into memory
        try:
            row_count = pl.scan_parquet(str(output_path)).select(pl.len()).collect().item()
        except Exception as e:
            logger.warning(f"Could not determine row count: {e}")
            row_count = 0

        if progress_callback:
            progress_callback(row_count)

        result = {
            "rows_processed": row_count,
            "input_bytes": input_bytes,
            "output_bytes": output_bytes,
            "memory_stats": memory_monitor.get_status_report()
        }

        logger.info(f"✅ Processed {csv_path.name}: {row_count:,} rows, "
                   f"{input_bytes:,} → {output_bytes:,} bytes, "
                   f"Memory: {result['memory_stats']['usage_above_baseline_mb']:.1f}MB above baseline")

        return result

    except Exception as e:
        # Cleanup on failure
        if output_path.exists():
            try:
                output_path.unlink()
            except:
                pass
        
        # Emergency cleanup
        memory_monitor.perform_aggressive_cleanup()
        raise e

def convert_table_csvs_v2(
    table_name: str,
    csv_paths: List[Path],
    output_dir: Path,
    delimiter: str,
    expected_columns: List[str],
    config: ConversionConfig
) -> str:
    """
    Convert table CSVs with improved memory management.
    Process files individually and combine only if necessary.
    """
    memory_monitor = MemoryMonitor(config)
    
    try:
        if not expected_columns:
            return f"[ERROR] No column mapping for '{table_name}'"

        valid_files = [p for p in csv_paths if p.exists()]
        if not valid_files:
            return f"[ERROR] No valid CSV files for '{table_name}'"

        output_dir.mkdir(parents=True, exist_ok=True)
        final_output = output_dir / f"{table_name}.parquet"

        if final_output.exists():
            final_output.unlink()

        total_input_bytes = sum(p.stat().st_size for p in valid_files)
        logger.info(f"🚀 Converting '{table_name}': {len(valid_files)} files, "
                   f"{total_input_bytes:,} bytes")
        
        # Log initial memory status
        status = memory_monitor.get_status_report()
        logger.info(f"Memory status - Baseline: {status['baseline_mb']:.1f}MB, "
                   f"Budget: {status['budget_remaining_mb']:.1f}MB")

        processed_files = []
        stats = {"files_processed": 0, "files_failed": 0, "total_rows": 0}
        start_time = time.time()

        # IMPROVEMENT 6: Process files individually with aggressive cleanup between files
        for i, csv_path in enumerate(valid_files, 1):
            try:
                logger.info(f"Processing file {i}/{len(valid_files)}: {csv_path.name}")
                
                # Aggressive cleanup before each file (especially after the first)
                if i > 1:
                    logger.info("Performing inter-file memory cleanup...")
                    cleanup_stats = memory_monitor.perform_aggressive_cleanup()
                    
                    # Wait a moment for cleanup to take effect
                    time.sleep(0.5)
                
                # Check memory before each file
                if memory_monitor.should_prevent_processing():
                    status = memory_monitor.get_status_report()
                    logger.error(f"Memory limit reached before file {i}. "
                               f"Usage: {status['usage_above_baseline_mb']:.1f}MB above baseline")
                    
                    # Try one more aggressive cleanup
                    logger.info("Attempting emergency cleanup...")
                    memory_monitor.perform_aggressive_cleanup()
                    time.sleep(1.0)
                    
                    if memory_monitor.should_prevent_processing():
                        break

                # IMPROVEMENT 7: Use individual file naming for large datasets
                if len(valid_files) == 1:
                    temp_output = final_output
                else:
                    temp_output = output_dir / f"{table_name}_part_{i:03d}.parquet"

                result = process_csv_with_memory_v2(
                    csv_path, temp_output, expected_columns, delimiter,
                    config, memory_monitor
                )

                processed_files.append(temp_output)
                stats["files_processed"] += 1
                stats["total_rows"] += result["rows_processed"]

                logger.info(f"✅ File {i}/{len(valid_files)} completed: "
                           f"{result['rows_processed']:,} rows, "
                           f"Memory: {result['memory_stats']['usage_above_baseline_mb']:.1f}MB above baseline")

                # IMPROVEMENT 8: Aggressive cleanup after each large file
                cleanup_stats = memory_monitor.perform_aggressive_cleanup()
                if cleanup_stats.get("freed_mb", 0) > 100:
                    logger.info(f"Post-file cleanup freed {cleanup_stats['freed_mb']:.1f}MB")

            except Exception as e:
                logger.error(f"❌ Failed file {i}/{len(valid_files)} ({csv_path.name}): {e}")
                stats["files_failed"] += 1
                
                # Cleanup after failure
                memory_monitor.perform_aggressive_cleanup()
                continue

        if not processed_files:
            return f"[ERROR] No files successfully processed for '{table_name}'"

        # IMPROVEMENT 9: Only combine if we have multiple part files
        if len(processed_files) == 1 and processed_files[0] != final_output:
            # Just rename the single file
            processed_files[0].rename(final_output)
            logger.info(f"✅ Single file renamed to {final_output.name}")
        elif len(processed_files) > 1:
            # Combine files with extreme memory care
            logger.info(f"🔄 Combining {len(processed_files)} parquet files...")
            
            # Pre-combine cleanup
            memory_monitor.perform_aggressive_cleanup()
            time.sleep(1.0)
            
            if memory_monitor.should_prevent_processing():
                logger.error("Insufficient memory to combine files - keeping separate parts")
                return f"[PARTIAL] '{table_name}': {stats['total_rows']:,} rows in {len(processed_files)} separate files"

            try:
                # Use minimal memory for combining
                lazy_frames = [pl.scan_parquet(str(f)) for f in processed_files]
                combined = pl.concat(lazy_frames, how="vertical")

                # Stream directly to final output
                combined.sink_parquet(
                    str(final_output),
                    compression=config.compression,
                    row_group_size=config.row_group_size,
                    maintain_order=False,
                    statistics=False
                )

                # Cleanup part files
                for part_file in processed_files:
                    if part_file.exists() and part_file != final_output:
                        try:
                            part_file.unlink()
                        except:
                            pass

                # Final cleanup
                del lazy_frames, combined
                memory_monitor.perform_aggressive_cleanup()

            except Exception as e:
                logger.error(f"Failed to combine files for '{table_name}': {e}")
                return f"[PARTIAL] '{table_name}': {stats['total_rows']:,} rows in {len(processed_files)} separate files"

        # Final metrics
        final_bytes = final_output.stat().st_size if final_output.exists() else 0
        compression_ratio = total_input_bytes / final_bytes if final_bytes > 0 else 0
        elapsed_time = time.time() - start_time
        processing_rate = (total_input_bytes / (1024 * 1024)) / elapsed_time if elapsed_time > 0 else 0

        result_msg = (f"[OK] '{table_name}': {stats['total_rows']:,} rows, "
                     f"{stats['files_processed']}/{len(valid_files)} files, "
                     f"{final_bytes:,} bytes ({compression_ratio:.1f}x compression), "
                     f"{processing_rate:.1f} MB/sec")

        # Final memory report
        final_status = memory_monitor.get_status_report()
        logger.info(f"🎉 Completed '{table_name}' in {elapsed_time:.1f}s. "
                   f"Final memory usage: {final_status['usage_above_baseline_mb']:.1f}MB above baseline")

        return result_msg

    except Exception as e:
        logger.error(f"Conversion failed for '{table_name}': {e}")
        return f"[ERROR] Failed '{table_name}': {e}"

def convert_csvs_to_parquet_v2(
    audit_map: dict,
    unzip_dir: Path,
    output_dir: Path,
    config: Optional[ConversionConfig] = None,
    delimiter: str = ";"
):
    """
    Main conversion function with improved memory management.
    IMPROVEMENT 10: Process tables sequentially for very large datasets.
    """
    if config is None:
        config = ConversionConfig()

    output_dir.mkdir(exist_ok=True)

    if not audit_map:
        logger.warning("No tables to convert")
        return

    logger.info(f"🚀 Starting conversion of {len(audit_map)} tables")
    logger.info(f"Configuration: {config.max_memory_mb}MB memory limit above baseline")

    # Enhanced system info logging
    if PSUTIL_AVAILABLE:
        try:
            memory = psutil.virtual_memory()
            logger.info(f"System Memory: {memory.total/(1024**3):.2f}GB total, "
                       f"{memory.available/(1024**3):.2f}GB available")
        except:
            pass

    # Create global memory monitor
    global_monitor = MemoryMonitor(config)

    # Prepare work queue sorted by size
    table_work = []
    for table_name, zip_map in audit_map.items():
        csv_paths = [unzip_dir / fname for files in zip_map.values() for fname in files]
        valid_paths = [p for p in csv_paths if p.exists()]

        if not valid_paths:
            logger.warning(f"No valid files for '{table_name}'")
            continue

        total_bytes = sum(p.stat().st_size for p in valid_paths)
        table_work.append((table_name, zip_map, valid_paths, total_bytes))

    # Sort by size (largest first) for better memory management
    table_work.sort(key=lambda x: x[3], reverse=True)

    logger.info(f"Processing {len(table_work)} tables with valid data")

    # IMPROVEMENT 11: For very large datasets, process sequentially instead of in parallel
    large_dataset_threshold = 1024 * 1024 * 1024  # 1GB
    has_large_datasets = any(total_bytes > large_dataset_threshold for _, _, _, total_bytes in table_work)
    
    if has_large_datasets:
        logger.info("🔧 Large datasets detected - using sequential processing for memory efficiency")
        use_parallel = False
    else:
        use_parallel = True

    from rich.progress import (
        Progress, SpinnerColumn, BarColumn, TextColumn,
        TimeElapsedColumn, MofNCompleteColumn, TimeRemainingColumn
    )

    success_count = 0
    error_count = 0
    total_bytes_processed = 0

    if use_parallel:
        # Parallel processing for smaller datasets
        with ThreadPoolExecutor(max_workers=min(config.workers, 2)) as executor:  # Limit workers for memory
            with Progress(
                SpinnerColumn(),
                TextColumn("[bold green]{task.description}"),
                BarColumn(),
                MofNCompleteColumn(),
                TimeElapsedColumn(),
                TimeRemainingColumn(),
            ) as progress:

                main_task = progress.add_task("Converting tables", total=len(table_work))
                tasks = {}
                
                for table_name, zip_map, csv_paths, total_bytes in table_work:
                    from ...utils.model_utils import get_table_columns
                    expected_columns = get_table_columns(table_name)

                    task = executor.submit(
                        convert_table_csvs_v2,
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

                    # Inter-task cleanup
                    global_monitor.perform_aggressive_cleanup()
    else:
        # Sequential processing for large datasets
        with Progress(
            SpinnerColumn(),
            TextColumn("[bold green]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TimeElapsedColumn(),
        ) as progress:

            main_task = progress.add_task("Converting tables", total=len(table_work))
            
            for i, (table_name, zip_map, csv_paths, total_bytes) in enumerate(table_work):
                logger.info(f"Processing table {i+1}/{len(table_work)}: {table_name} ({total_bytes:,} bytes)")
                
                # Aggressive cleanup before each table
                if i > 0:
                    logger.info("Performing inter-table cleanup...")
                    global_monitor.perform_aggressive_cleanup()
                    time.sleep(1.0)
                
                try:
                    from ...utils.model_utils import get_table_columns
                    expected_columns = get_table_columns(table_name)
                    
                    result = convert_table_csvs_v2(
                        table_name,
                        csv_paths,
                        output_dir,
                        delimiter,
                        expected_columns,
                        config
                    )
                    
                    if "[OK]" in result:
                        logger.info(f"✅ {result}")
                        success_count += 1
                        total_bytes_processed += total_bytes
                    else:
                        logger.error(f"❌ {result}")
                        error_count += 1
                        
                except Exception as e:
                    logger.error(f"❌ Exception in '{table_name}': {e}")
                    error_count += 1

                progress.update(main_task, completed=i+1)

    # Final comprehensive summary
    final_status = global_monitor.get_status_report()
    total_mb = total_bytes_processed / (1024 * 1024)
    
    logger.info(f"🎉 Conversion Summary:")
    logger.info(f"   ✅ Successful: {success_count} tables")
    logger.info(f"   ❌ Failed: {error_count} tables")
    logger.info(f"   📊 Total processed: {total_bytes_processed:,} bytes ({total_mb:.1f}MB)")
    logger.info(f"   🧠 Final memory usage: {final_status['usage_above_baseline_mb']:.1f}MB above baseline")
    logger.info(f"   📁 Output directory: {output_dir}")


# IMPROVEMENT 12: Additional utility functions for handling very large files

def estimate_memory_requirements(csv_path: Path, delimiter: str = ";") -> Dict[str, float]:
    """
    Estimate memory requirements for processing a CSV file.
    Helps decide processing strategy upfront.
    """
    file_size_mb = csv_path.stat().st_size / (1024 * 1024)
    
    # Sample first few lines to estimate row size
    try:
        sample = pl.read_csv(
            str(csv_path),
            separator=delimiter,
            n_rows=1000,
            encoding="utf8-lossy",
            ignore_errors=True
        )
        
        avg_row_size = file_size_mb / len(sample) if len(sample) > 0 else file_size_mb
        estimated_rows = file_size_mb / avg_row_size if avg_row_size > 0 else 0
        
        # Conservative memory estimate (3-5x file size for processing)
        estimated_memory_mb = file_size_mb * 4
        
        del sample
        gc.collect()
        
        return {
            "file_size_mb": file_size_mb,
            "estimated_rows": estimated_rows,
            "estimated_memory_mb": estimated_memory_mb,
            "avg_row_size_bytes": avg_row_size * 1024 * 1024
        }
        
    except Exception as e:
        logger.warning(f"Could not estimate memory for {csv_path.name}: {e}")
        return {
            "file_size_mb": file_size_mb,
            "estimated_rows": 0,
            "estimated_memory_mb": file_size_mb * 5,  # Conservative fallback
            "avg_row_size_bytes": 0
        }

def split_large_csv_file(
    csv_path: Path,
    output_dir: Path,
    max_chunk_size_mb: float = 500,
    delimiter: str = ";"
) -> List[Path]:
    """
    Split a very large CSV file into smaller chunks for processing.
    Use this when a single file is too large for available memory.
    """
    logger.info(f"Splitting large file {csv_path.name} into chunks of max {max_chunk_size_mb}MB")
    
    output_dir.mkdir(parents=True, exist_ok=True)
    chunk_files = []
    
    try:
        # Read header first
        header_df = pl.read_csv(
            str(csv_path),
            separator=delimiter,
            n_rows=1,
            encoding="utf8-lossy"
        )
        header_row = header_df.columns
        
        # Calculate approximate rows per chunk
        file_size_mb = csv_path.stat().st_size / (1024 * 1024)
        rows_per_chunk = max(10000, int((max_chunk_size_mb / file_size_mb) * 1000000))
        
        logger.info(f"Splitting into chunks of ~{rows_per_chunk:,} rows each")
        
        chunk_num = 1
        offset = 0
        
        while True:
            try:
                chunk = pl.read_csv(
                    str(csv_path),
                    separator=delimiter,
                    skip_rows=offset,
                    n_rows=rows_per_chunk,
                    encoding="utf8-lossy",
                    ignore_errors=True,
                    truncate_ragged_lines=True,
                    has_header=False,  # We'll add header manually
                    new_columns=header_row
                )
                
                if len(chunk) == 0:
                    break
                    
                chunk_path = output_dir / f"{csv_path.stem}_chunk_{chunk_num:03d}.csv"
                chunk.write_csv(str(chunk_path), separator=delimiter)
                chunk_files.append(chunk_path)
                
                logger.info(f"Created chunk {chunk_num}: {len(chunk):,} rows -> {chunk_path.name}")
                
                offset += rows_per_chunk
                chunk_num += 1
                
                # Cleanup
                del chunk
                gc.collect()
                
                # Safety check - don't create too many chunks
                if chunk_num > 100:
                    logger.warning("Created 100+ chunks - stopping to prevent excessive fragmentation")
                    break
                    
            except Exception as e:
                logger.error(f"Error creating chunk {chunk_num}: {e}")
                break
        
        logger.info(f"Split {csv_path.name} into {len(chunk_files)} chunks")
        return chunk_files
        
    except Exception as e:
        logger.error(f"Failed to split {csv_path.name}: {e}")
        # Cleanup any partial chunks
        for chunk_file in chunk_files:
            if chunk_file.exists():
                try:
                    chunk_file.unlink()
                except:
                    pass
        return []

def process_extremely_large_table(
    table_name: str,
    csv_paths: List[Path],
    output_dir: Path,
    delimiter: str,
    expected_columns: List[str],
    config: ConversionConfig,
    max_file_size_mb: float = 1000
) -> str:
    """
    Special handling for extremely large tables that exceed memory capacity.
    Streams CSV(s) directly into a single Parquet file with chunked reads,
    avoiding temporary CSV chunk materialization.

    Signature kept identical.
    """
    logger.info(f"🔧 Processing extremely large table '{table_name}' with streaming writer (max_file_size_mb={max_file_size_mb})")

    memory_monitor = MemoryMonitor(config)
    output_dir.mkdir(parents=True, exist_ok=True)
    final_output = output_dir / f"{table_name}.parquet"
    tmp_output = final_output.with_suffix(final_output.suffix + ".tmp")

    inputs = [p for p in csv_paths if p.exists() and p.stat().st_size > 0]
    if not inputs:
        return f"[ERROR] No processable files for '{table_name}'"

    # Try imports
    try:
        import pandas as pd
        import pyarrow as pa
        import pyarrow.parquet as pq
    except Exception as imp_err:
        logger.warning(f"pyarrow/pandas not available ({imp_err}). Falling back to CSV-splitting flow.")
        # fallback: keep original splitting behavior (omitted here for brevity)
        # ... call previous split+convert_table_csvs_v2 flow ...
        # (you already have fallback code in your file; keep it)
        return "[ERROR] pandas/pyarrow not available - fallback path not shown here"

    # derive schema (same as before)
    try:
        sample_rows = 10000
        sample_df = pl.read_csv(str(inputs[0]), separator=delimiter, n_rows=sample_rows, encoding="utf8-lossy", ignore_errors=True)
        if len(sample_df) > 0:
            pa_schema = sample_df.to_arrow().schema
        else:
            raise RuntimeError("Polars sample empty")
        del sample_df
        gc.collect()
    except Exception:
        try:
            sample_pd = pd.read_csv(str(inputs[0]), sep=delimiter, nrows=1000, dtype=str, on_bad_lines="skip")
            pa_schema = pa.Table.from_pandas(sample_pd, preserve_index=False).schema
            del sample_pd
            gc.collect()
        except Exception:
            if expected_columns:
                fields = [pa.field(c, pa.string()) for c in expected_columns]
                pa_schema = pa.schema(fields)
            else:
                pa_schema = pa.schema([pa.field("col_0", pa.string())])

    if expected_columns:
        existing_names = set(pa_schema.names)
        ordered_fields = []
        for col in expected_columns:
            if col in existing_names:
                ordered_fields.append(pa_schema.field(col))
            else:
                ordered_fields.append(pa.field(col, pa.string()))
        pa_schema = pa.schema(ordered_fields)

    writer = None
    total_rows_written = 0
    default_chunksize = getattr(config, "default_chunksize", None) or getattr(config, "chunk_rows", None) or 500_000
    chunksize = int(default_chunksize)

    try:
        if tmp_output.exists():
            try:
                tmp_output.unlink()
            except Exception:
                pass

        writer = pq.ParquetWriter(str(tmp_output), pa_schema, compression=getattr(config, "compression", "snappy"))

        for csv_path in inputs:
            fsize_mb = csv_path.stat().st_size / (1024 * 1024)
            if fsize_mb > max_file_size_mb:
                local_chunksize = max(50_000, int(chunksize / max(1, int(fsize_mb // max_file_size_mb))))
            else:
                local_chunksize = chunksize

            # ------------------- Robust CSV reader creation -------------------
            # Strategy:
            # 1) Try utf-8 with encoding_errors='replace' (if pandas supports it)
            # 2) If TypeError (older pandas), try utf-8 with engine='python' (tolerant but slower)
            # 3) If UnicodeDecodeError still occurs, fall back to latin1
            reader = None
            try:
                # pandas >= 1.5 supports encoding_errors
                reader = pd.read_csv(
                    str(csv_path),
                    sep=delimiter,
                    dtype=str,
                    chunksize=local_chunksize,
                    encoding="utf-8",
                    encoding_errors="replace",
                    on_bad_lines="skip",
                    low_memory=True
                )
                logger.debug(f"Using utf-8 (with replace) reader for {csv_path.name}")
            except TypeError:
                # older pandas doesn't support encoding_errors; try utf-8 with python engine
                try:
                    reader = pd.read_csv(
                        str(csv_path),
                        sep=delimiter,
                        dtype=str,
                        chunksize=local_chunksize,
                        encoding="utf-8",
                        on_bad_lines="skip",
                        low_memory=True,
                        engine="python"
                    )
                    logger.debug(f"Using utf-8 (python engine) reader for {csv_path.name}")
                except UnicodeDecodeError:
                    logger.warning(f"utf-8 decode failed for {csv_path.name} with python engine; falling back to latin1")
                    reader = pd.read_csv(
                        str(csv_path),
                        sep=delimiter,
                        dtype=str,
                        chunksize=local_chunksize,
                        encoding="latin1",
                        on_bad_lines="skip",
                        low_memory=True
                    )
            except UnicodeDecodeError:
                # utf-8 with replace should not usually raise, but be defensive
                logger.warning(f"utf-8 decode failed for {csv_path.name}; falling back to latin1")
                reader = pd.read_csv(
                    str(csv_path),
                    sep=delimiter,
                    dtype=str,
                    chunksize=local_chunksize,
                    encoding="latin1",
                    on_bad_lines="skip",
                    low_memory=True
                )
            # -----------------------------------------------------------------

            # iterate chunks (reader is an iterator)
            for df_chunk in reader:
                # align columns to expected_columns or pa_schema (same logic you had)
                if expected_columns:
                    missing = [c for c in expected_columns if c not in df_chunk.columns]
                    for c in missing:
                        df_chunk[c] = None
                    df_chunk = df_chunk.reindex(columns=expected_columns)
                else:
                    missing = [n for n in pa_schema.names if n not in df_chunk.columns]
                    for c in missing:
                        df_chunk[c] = None
                    df_chunk = df_chunk.reindex(columns=pa_schema.names)

                # convert to Arrow table, cast to schema if needed (same as before)
                try:
                    table = pa.Table.from_pandas(df_chunk, preserve_index=False)
                    if table.schema != pa_schema:
                        table = table.select(pa_schema.names)
                        table = table.cast(pa_schema)
                except Exception:
                    logger.debug("Chunk -> pyarrow conversion failed; coercing to strings")
                    for col in df_chunk.columns:
                        df_chunk[col] = df_chunk[col].astype("string")
                    table = pa.Table.from_pandas(df_chunk, preserve_index=False)
                    if table.schema != pa_schema:
                        table = table.select(pa_schema.names)

                writer.write_table(table)
                total_rows_written += table.num_rows

                if memory_monitor and memory_monitor.should_prevent_processing():
                    try:
                        gc.collect()
                        time.sleep(0.05)
                    except Exception:
                        pass

        # close writer and atomic replace tmp -> final (unchanged)
        writer.close()
        writer = None
        with open(tmp_output, "rb") as ftmp:
            try:
                os.fsync(ftmp.fileno())
            except Exception:
                pass
        os.replace(str(tmp_output), str(final_output))
        try:
            dir_fd = os.open(final_output.parent, os.O_DIRECTORY)
            os.fsync(dir_fd)
            os.close(dir_fd)
        except Exception:
            pass

        # read metadata row count if possible, else use total_rows_written
        try:
            meta = pq.ParquetFile(str(final_output)).metadata
            row_count = sum(meta.row_group(i).num_rows for i in range(meta.num_row_groups))
        except Exception:
            row_count = total_rows_written

        logger.info(f"✅ Streamed {table_name}: wrote ~{row_count:,} rows to {final_output.name}")
        return (f"[OK] '{table_name}': {row_count:,} rows streamed, "
                f"{final_output.stat().st_size:,} bytes, 1 output file")

    except Exception as e:
        logger.exception(f"Failed streaming extremely large table '{table_name}': {e}")
        try:
            if writer is not None:
                writer.close()
        except Exception:
            pass
        try:
            if tmp_output.exists():
                tmp_output.unlink()
        except Exception:
            pass
        return f"[ERROR] Failed extremely large table '{table_name}': {e}"


# IMPROVEMENT 13: Configuration adjustments for your specific use case

class LargeDatasetConfig(ConversionConfig):
    """
    Specialized configuration for very large datasets like yours.
    """
    def __init__(self):
        super().__init__()
        
        # More conservative memory settings
        self.max_memory_mb = 800  # Reduced from 1024 to leave more headroom
        self.cleanup_threshold_ratio = 0.6  # Cleanup earlier
        self.baseline_buffer_mb = 512  # More conservative system memory buffer
        
        # Optimize for large files
        self.row_group_size = 25000  # Smaller row groups for better memory control
        self.compression = "snappy"  # Faster compression for large datasets
        
        # Conservative worker count for large files
        self.workers = 1  # Single-threaded for very large datasets
        
        # Additional settings
        self.enable_file_splitting = True
        self.max_file_size_mb = 800  # Split files larger than this
        self.chunk_processing_delay = 0.5  # Pause between chunks


# IMPROVEMENT 14: Smart processing strategy selector

def select_processing_strategy(audit_map: dict, unzip_dir: Path) -> str:
    """
    Analyze the dataset and recommend the best processing strategy.
    """
    total_files = 0
    total_bytes = 0
    largest_file_mb = 0
    
    for table_name, zip_map in audit_map.items():
        csv_paths = [unzip_dir / fname for files in zip_map.values() for fname in files]
        valid_paths = [p for p in csv_paths if p.exists()]
        
        for path in valid_paths:
            size_mb = path.stat().st_size / (1024 * 1024)
            total_files += 1
            total_bytes += path.stat().st_size
            largest_file_mb = max(largest_file_mb, size_mb)
    
    total_gb = total_bytes / (1024 * 1024 * 1024)
    
    strategy = "standard"
    reasons = []
    
    if largest_file_mb > 2000:  # 2GB+ files
        strategy = "file_splitting"
        reasons.append(f"Largest file: {largest_file_mb:.1f}MB")
        
    if total_gb > 50:  # 50GB+ total
        strategy = "sequential_only"
        reasons.append(f"Total dataset: {total_gb:.1f}GB")
        
    if total_files > 100:
        strategy = "batch_processing"
        reasons.append(f"Many files: {total_files}")
    
    logger.info(f"Recommended strategy: {strategy}")
    if reasons:
        logger.info(f"Reasons: {', '.join(reasons)}")
    
    return strategy

# IMPROVEMENT 15: Main entry point with strategy selection

def convert_csvs_to_parquet(
    audit_map: dict,
    unzip_dir: Path,
    output_dir: Path,
    delimiter: str = ";"
):
    """
    Smart conversion that selects the best strategy based on dataset characteristics.
    """
    # Analyze dataset and select strategy
    strategy = select_processing_strategy(audit_map, unzip_dir)
    
    # Use appropriate configuration
    if strategy in ["file_splitting", "sequential_only"]:
        config = LargeDatasetConfig()
        logger.info("Using large dataset configuration")
    else:
        config = ConversionConfig()
        logger.info("Using standard configuration")
    
    # Execute with selected strategy
    if strategy == "file_splitting":
        # Process with file splitting for very large files
        for table_name, zip_map in audit_map.items():
            csv_paths = [unzip_dir / fname for files in zip_map.values() for fname in files]
            valid_paths = [p for p in csv_paths if p.exists()]
            
            if not valid_paths:
                continue
                
            from ...utils.model_utils import get_table_columns
            expected_columns = get_table_columns(table_name)
            
            result = process_extremely_large_table(
                table_name,
                valid_paths,
                output_dir,
                delimiter,
                expected_columns,
                config,
                max_file_size_mb=config.max_file_size_mb
            )
            logger.info(result)
    else:
        # Use improved standard processing
        convert_csvs_to_parquet_v2(
            audit_map,
            unzip_dir,
            output_dir,
            config,
            delimiter
        )


    