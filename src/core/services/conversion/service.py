"""
Improved memory management system for CSV to Parquet conversion.
Key improvements: proper schema inference, true streaming, memory cleanup between files.
"""
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import gc
import time
from typing import List, Dict, Optional, Callable
from dataclasses import dataclass

import polars as pl

from ....setup.config import ConversionConfig
from ..memory.service import MemoryMonitor
from ....setup.logging import logger

import psutil


def infer_optimal_schema(csv_path: Path, delimiter: str, sample_size: int = 10000) -> Dict:
    """
    Infer optimal schema with reduced sample size for safety.
    """
    logger.debug(f"Inferring schema for {csv_path.name}...")
    
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
        logger.debug(f"Inferred schema for {csv_path.name}: {len(schema)} columns")
        
        # Clean up sample immediately
        del sample_df
        gc.collect()
        
        return dict(schema)
        
    except Exception as e:
        logger.debug(f"Schema inference failed for {csv_path.name}: {e}. Using string fallback.")
        return None

@dataclass
class ProcessingStrategy:
    """Configuration for a specific processing approach."""
    name: str
    row_group_size: int
    batch_size: Optional[int] = None  # For chunked approaches
    compression: str = "snappy"
    low_memory: bool = True
    streaming: bool = True

def get_processing_strategies(file_size_mb: float, memory_pressure: float) -> List[ProcessingStrategy]:
    """
    Select appropriate processing strategies based on file size and memory pressure.
    Returns strategies ordered from most to least aggressive.
    """
    strategies = []
    
    # Strategy selection based on memory pressure
    if memory_pressure < 0.3:  # Low pressure - can be aggressive
        strategies.extend([
            ProcessingStrategy("full_streaming", row_group_size=100000),
            ProcessingStrategy("standard_streaming", row_group_size=50000),
            ProcessingStrategy("conservative_streaming", row_group_size=25000),
        ])
    elif memory_pressure < 0.6:  # Medium pressure - more conservative
        strategies.extend([
            ProcessingStrategy("standard_streaming", row_group_size=50000),
            ProcessingStrategy("conservative_streaming", row_group_size=25000),
            ProcessingStrategy("chunked_processing", row_group_size=25000, batch_size=500000),
        ])
    else:  # High pressure - very conservative
        strategies.extend([
            ProcessingStrategy("conservative_streaming", row_group_size=25000),
            ProcessingStrategy("chunked_processing", row_group_size=25000, batch_size=250000),
            ProcessingStrategy("minimal_processing", row_group_size=10000, batch_size=100000),
        ])
    
    # Add file size considerations
    if file_size_mb > 1000:  # Large files get additional conservative strategies
        strategies.append(
            ProcessingStrategy("micro_chunked", row_group_size=5000, batch_size=50000)
        )
    
    return strategies

def process_csv_with_polars_strategies(
    csv_path: Path,
    output_path: Path,
    expected_columns: List[str],
    delimiter: str,
    config: ConversionConfig,
    memory_monitor: MemoryMonitor,
    progress_callback: Optional[Callable[[int], None]] = None
) -> Dict[str, any]:
    """
    Process CSV using progressive Polars-only strategies.
    Replaces the old PyArrow fallback approach.
    """
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    input_bytes = csv_path.stat().st_size
    file_size_mb = input_bytes / (1024 * 1024)
    logger.info(f"Processing {csv_path.name} ({input_bytes:,} bytes)")

    # Pre-processing memory check
    if memory_monitor.should_prevent_processing():
        status = memory_monitor.get_status_report()
        raise RuntimeError(f"Insufficient memory to process file. "
                         f"Usage: {status['usage_above_baseline_mb']:.1f}MB above baseline, "
                         f"Limit: {status['configured_limit_mb']}MB")

    # Get processing strategies based on current conditions
    memory_pressure = memory_monitor.get_memory_pressure_level()
    strategies = get_processing_strategies(file_size_mb, memory_pressure)
    
    logger.info(f"Selected {len(strategies)} processing strategies for {csv_path.name}")

    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Try each strategy until one succeeds
        for i, strategy in enumerate(strategies):
            logger.info(f"Attempting strategy {i+1}/{len(strategies)}: {strategy.name}")
            
            try:
                # Cleanup before each strategy attempt
                if i > 0:
                    memory_monitor.perform_aggressive_cleanup()
                    time.sleep(0.5)
                
                # Check memory again before attempting
                if memory_monitor.should_prevent_processing():
                    logger.warning(f"Memory pressure too high for strategy {strategy.name}")
                    continue
                
                result = _execute_polars_strategy(
                    csv_path, output_path, expected_columns, delimiter,
                    strategy, memory_monitor, progress_callback
                )
                
                # Strategy succeeded
                logger.info(f"✅ Strategy {strategy.name} succeeded for {csv_path.name}")
                return result
                
            except (MemoryError, pl.ComputeError) as e:
                logger.warning(f"Strategy {strategy.name} failed: {e}")
                # Clean up any partial output
                if output_path.exists():
                    try:
                        output_path.unlink()
                    except:
                        pass
                # Try next strategy
                continue
            except Exception as e:
                logger.error(f"Unexpected error in strategy {strategy.name}: {e}")
                # Clean up and try next strategy
                if output_path.exists():
                    try:
                        output_path.unlink()
                    except:
                        pass
                continue
        
        # All strategies failed
        raise RuntimeError(f"All processing strategies failed for {csv_path.name}")
        
    except Exception as e:
        # Final cleanup on total failure
        if output_path.exists():
            try:
                output_path.unlink()
            except:
                pass
        memory_monitor.perform_aggressive_cleanup()
        raise e

def _execute_polars_strategy(
    csv_path: Path,
    output_path: Path,
    expected_columns: List[str],
    delimiter: str,
    strategy: ProcessingStrategy,
    memory_monitor: MemoryMonitor,
    progress_callback: Optional[Callable[[int], None]] = None
) -> Dict[str, any]:
    """
    Execute a specific Polars processing strategy.
    """
    logger.debug(f"Executing {strategy.name} on {csv_path.name}")
    
    # Schema inference with error handling
    try:
        inferred_schema = infer_optimal_schema(csv_path, delimiter, sample_size=5000)
    except Exception as e:
        logger.warning(f"Schema inference failed: {e}. Using string schema.")
        inferred_schema = None
    
    if strategy.batch_size is None:
        # Direct streaming approach
        return _execute_streaming_strategy(
            csv_path, output_path, expected_columns, delimiter,
            strategy, inferred_schema, memory_monitor, progress_callback
        )
    else:
        # Chunked processing approach
        return _execute_chunked_strategy(
            csv_path, output_path, expected_columns, delimiter,
            strategy, inferred_schema, memory_monitor, progress_callback
        )

def _execute_streaming_strategy(
    csv_path: Path,
    output_path: Path,
    expected_columns: List[str],
    delimiter: str,
    strategy: ProcessingStrategy,
    inferred_schema: Optional[Dict],
    memory_monitor: MemoryMonitor,
    progress_callback: Optional[Callable[[int], None]] = None
) -> Dict[str, any]:
    """
    Execute streaming strategy using Polars native streaming.
    """
    # Set up schema overrides
    if inferred_schema is None:
        if expected_columns:
            schema_override = {f"column_{i+1}": pl.Utf8 for i in range(len(expected_columns))}
        else:
            schema_override = None
    else:
        schema_override = inferred_schema

    # Create lazy frame with strategy-specific settings
    lazy_frame = pl.scan_csv(
        str(csv_path),
        separator=delimiter,
        schema_overrides=schema_override,
        encoding="utf8-lossy",
        ignore_errors=True,
        truncate_ragged_lines=True,
        null_values=["", "NULL", "null", "N/A", "n/a"],
        try_parse_dates=True,
        low_memory=strategy.low_memory,
        rechunk=False,
        infer_schema_length=5000,  # Reduced for safety
        has_header=False,
        quote_char='"'
    )

    # Handle column renaming
    if expected_columns:
        try:
            current_columns = lazy_frame.collect_schema().names()
            if len(current_columns) == len(expected_columns):
                column_mapping = {current_columns[i]: expected_columns[i] for i in range(len(expected_columns))}
                lazy_frame = lazy_frame.rename(column_mapping)
                lazy_frame = lazy_frame.select(expected_columns)
                logger.info(f"Renamed columns to expected names: {expected_columns}")
        except Exception as e:
            logger.warning(f"Column processing failed: {e}")

    # Execute streaming with strategy parameters
    logger.info(f"Streaming {csv_path.name} with {strategy.name} (row_group_size={strategy.row_group_size})")
    
    lazy_frame.sink_parquet(
        str(output_path),
        compression=strategy.compression,
        row_group_size=strategy.row_group_size,
        maintain_order=False,
        statistics=False,  # Disable for memory savings
        compression_level=1 if strategy.compression in ['zstd', 'gzip'] else None
    )

    # Cleanup and verification
    del lazy_frame
    gc.collect()

    if not output_path.exists():
        raise RuntimeError("Output file was not created")
        
    output_bytes = output_path.stat().st_size
    
    # Get row count efficiently
    try:
        row_count = pl.scan_parquet(str(output_path)).select(pl.len()).collect().item()
    except Exception as e:
        logger.warning(f"Could not determine row count: {e}")
        row_count = 0

    if progress_callback:
        progress_callback(row_count)

    return {
        "rows_processed": row_count,
        "input_bytes": csv_path.stat().st_size,
        "output_bytes": output_bytes,
        "strategy_used": strategy.name,
        "memory_stats": memory_monitor.get_status_report()
    }

def _execute_chunked_strategy(
    csv_path: Path,
    output_path: Path,
    expected_columns: List[str],
    delimiter: str,
    strategy: ProcessingStrategy,
    inferred_schema: Optional[Dict],
    memory_monitor: MemoryMonitor,
    progress_callback: Optional[Callable[[int], None]] = None
) -> Dict[str, any]:
    """
    Execute chunked processing strategy using Polars with file-level chunking.
    This processes the file in chunks and combines them.
    """
    logger.info(f"Using chunked strategy: {strategy.name} with batch_size={strategy.batch_size}")
    
    # Create temporary directory for chunks
    temp_dir = output_path.parent / f".temp_{output_path.stem}"
    temp_dir.mkdir(exist_ok=True)
    
    try:
        chunk_files = []
        total_rows = 0
        chunk_num = 1
        offset = 0
        
        while True:
            chunk_output = temp_dir / f"chunk_{chunk_num:03d}.parquet"
            
            try:
                # Process chunk using streaming
                if inferred_schema:
                    schema_override = inferred_schema
                else:
                    schema_override = {f"column_{i+1}": pl.Utf8 for i in range(len(expected_columns))} if expected_columns else None
                
                lazy_chunk = pl.scan_csv(
                    str(csv_path),
                    separator=delimiter,
                    schema_overrides=schema_override,
                    encoding="utf8-lossy",
                    ignore_errors=True,
                    skip_rows=offset,
                    n_rows=strategy.batch_size,
                    has_header=False,
                    low_memory=True,
                    quote_char='"'
                )
                
                # Apply column renaming if needed
                if expected_columns:
                    try:
                        current_columns = lazy_chunk.collect_schema().names()
                        if len(current_columns) == len(expected_columns):
                            column_mapping = {current_columns[i]: expected_columns[i] for i in range(len(expected_columns))}
                            lazy_chunk = lazy_chunk.rename(column_mapping).select(expected_columns)
                    except Exception:
                        pass
                
                # Check if chunk has data
                chunk_df = lazy_chunk.collect()
                if len(chunk_df) == 0:
                    break
                
                # Write chunk
                chunk_df.write_parquet(
                    str(chunk_output),
                    compression=strategy.compression,
                    row_group_size=strategy.row_group_size,
                    statistics=False
                )
                
                chunk_files.append(chunk_output)
                total_rows += len(chunk_df)
                
                logger.debug(f"Chunk {chunk_num}: {len(chunk_df):,} rows")
                
                # Cleanup chunk data
                del chunk_df, lazy_chunk
                gc.collect()
                
                # Memory pressure check
                if memory_monitor.is_memory_pressure_high():
                    logger.info(f"Memory pressure detected after chunk {chunk_num}, performing cleanup")
                    memory_monitor.perform_aggressive_cleanup()
                    time.sleep(0.5)
                
                offset += strategy.batch_size
                chunk_num += 1
                
                # Safety check
                if chunk_num > 1000:
                    logger.warning("Created 1000+ chunks, stopping to prevent excessive fragmentation")
                    break
                    
            except Exception as e:
                logger.error(f"Failed to process chunk {chunk_num}: {e}")
                break
        
        if not chunk_files:
            raise RuntimeError("No chunks were successfully processed")
        
        # Combine chunks
        logger.info(f"Combining {len(chunk_files)} chunks into final output")
        
        if len(chunk_files) == 1:
            # Just move the single chunk
            chunk_files[0].rename(output_path)
        else:
            # Combine multiple chunks using Polars
            lazy_frames = [pl.scan_parquet(str(f)) for f in chunk_files]
            combined = pl.concat(lazy_frames, how="vertical")
            combined.sink_parquet(
                str(output_path),
                compression=strategy.compression,
                row_group_size=strategy.row_group_size,
                maintain_order=False,
                statistics=False
            )
            
            # Cleanup intermediate files
            for chunk_file in chunk_files:
                try:
                    chunk_file.unlink()
                except:
                    pass
        
        # Final verification
        if not output_path.exists():
            raise RuntimeError("Final output file was not created")
        
        output_bytes = output_path.stat().st_size
        
        if progress_callback:
            progress_callback(total_rows)
        
        return {
            "rows_processed": total_rows,
            "input_bytes": csv_path.stat().st_size,
            "output_bytes": output_bytes,
            "strategy_used": strategy.name,
            "chunks_processed": len(chunk_files),
            "memory_stats": memory_monitor.get_status_report()
        }
        
    finally:
        # Cleanup temp directory
        try:
            for chunk_file in chunk_files:
                if chunk_file.exists():
                    chunk_file.unlink()
            if temp_dir.exists():
                temp_dir.rmdir()
        except Exception as e:
            logger.warning(f"Cleanup of temp files failed: {e}")

def convert_table_csvs(
    table_name: str,
    csv_paths: List[Path],
    output_dir: Path,
    delimiter: str,
    expected_columns: List[str],
    config: ConversionConfig
) -> str:
    """
    Convert table CSVs with improved memory management using Polars-only strategies.
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
        logger.info(f"Converting '{table_name}': {len(valid_files)} files, "
                   f"{total_input_bytes:,} bytes")
        
        # Log initial memory status
        status = memory_monitor.get_status_report()
        logger.info(f"Memory status - Baseline: {status['baseline_mb']:.1f}MB, "
                   f"Budget: {status['budget_remaining_mb']:.1f}MB")

        processed_files = []
        stats = {"files_processed": 0, "files_failed": 0, "total_rows": 0}
        start_time = time.time()

        # Process files individually with cleanup between files
        for i, csv_path in enumerate(valid_files, 1):
            try:
                logger.info(f"Processing file {i}/{len(valid_files)}: {csv_path.name}")
                
                # Aggressive cleanup before each file (especially after the first)
                if i > 1:
                    logger.info("Performing inter-file memory cleanup...")
                    cleanup_stats = memory_monitor.perform_aggressive_cleanup()
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

                # Use individual file naming for large datasets
                if len(valid_files) == 1:
                    temp_output = final_output
                else:
                    temp_output = output_dir / f"{table_name}_part_{i:03d}.parquet"

                # Use new Polars-only processing
                result = process_csv_with_polars_strategies(
                    csv_path, temp_output, expected_columns, delimiter,
                    config, memory_monitor
                )

                processed_files.append(temp_output)
                stats["files_processed"] += 1
                stats["total_rows"] += result["rows_processed"]

                logger.info(f"✅ File {i}/{len(valid_files)} completed: "
                           f"{result['rows_processed']:,} rows using {result['strategy_used']}, "
                           f"Memory: {result['memory_stats']['usage_above_baseline_mb']:.1f}MB above baseline")

                # Post-file cleanup
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

        # Combine files if necessary (same logic as before)
        if len(processed_files) == 1 and processed_files[0] != final_output:
            # Just rename the single file
            processed_files[0].rename(final_output)
            logger.info(f"✅ Single file renamed to {final_output.name}")
        elif len(processed_files) > 1:
            logger.info(f"Combining {len(processed_files)} parquet files...")

            # Pre-combine cleanup
            memory_monitor.perform_aggressive_cleanup()
            time.sleep(1.0)
            
            if memory_monitor.should_prevent_processing():
                logger.error("Insufficient memory to combine files - keeping separate parts")
                return f"[PARTIAL] '{table_name}': {stats['total_rows']:,} rows in {len(processed_files)} separate files"

            try:
                # Use Polars for combining (no PyArrow needed)
                lazy_frames = [pl.scan_parquet(str(f)) for f in processed_files]
                combined = pl.concat(lazy_frames, how="vertical")
                combined.sink_parquet(
                    str(final_output),
                    compression=config.compression,
                    row_group_size=config.row_group_size,
                    maintain_order=False,
                    statistics=False
                )
                
                # Cleanup parts after successful combination
                for part_file in processed_files:
                    try:
                        if part_file.exists() and part_file != final_output:
                            part_file.unlink()
                    except Exception as e:
                        logger.warning(f"Could not remove part {part_file}: {e}")

                # Final cleanup of combination objects
                del lazy_frames, combined
                memory_monitor.perform_aggressive_cleanup()
                
            except Exception as e:
                logger.exception(f"Failed to combine files for '{table_name}': {e}")
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
        logger.info(f"Completed '{table_name}' in {elapsed_time:.1f}s. "
                   f"Final memory usage: {final_status['usage_above_baseline_mb']:.1f}MB above baseline")

        return result_msg

    except Exception as e:
        logger.error(f"Conversion failed for '{table_name}': {e}")
        return f"[ERROR] Failed '{table_name}': {e}"

def convert_csvs_to_parquet(
    audit_map: dict,
    unzip_dir: Path,
    output_dir: Path,
    config: Optional[ConversionConfig] = None,
    delimiter: str = ";"
):
    """
    Main conversion function with improved memory management using Polars-only approach.
    """
    if config is None:
        config = ConversionConfig()

    output_dir.mkdir(exist_ok=True)

    if not audit_map:
        logger.warning("No tables to convert")
        return

    logger.info(f"Starting conversion of {len(audit_map)} tables")
    logger.info(f"Configuration: {config.memory_limit_mb}MB memory limit above baseline")

    # Enhanced system info logging
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

    # Determine processing approach based on dataset size
    large_dataset_threshold = 1024 * 1024 * 1024  # 1GB
    has_large_datasets = any(total_bytes > large_dataset_threshold for _, _, _, total_bytes in table_work)
    
    if has_large_datasets:
        logger.info("Large datasets detected - using sequential processing for memory efficiency")
        use_parallel = False
    else:
        use_parallel = True

    from rich.progress import (
        Progress, SpinnerColumn, BarColumn, TextColumn,
        TimeElapsedColumn, MofNCompleteColumn, TimeRemainingColumn
    )
    from ...utils.models import get_table_columns

    success_count = 0
    error_count = 0
    total_bytes_processed = 0

    if use_parallel:
        # Parallel processing for smaller datasets
        with ThreadPoolExecutor(max_workers=min(config.workers, 2)) as executor:
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
                    expected_columns = get_table_columns(table_name)

                    task = executor.submit(
                        convert_table_csvs,
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
                    from ...utils.models import get_table_columns
                    expected_columns = get_table_columns(table_name)
                    
                    result = convert_table_csvs(
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
    
    logger.info("Conversion Summary:")
    logger.info(f"   ✅ Successful: {success_count} tables")
    logger.info(f"   ❌ Failed: {error_count} tables")
    logger.info(f"   📊 Total processed: {total_bytes_processed:,} bytes ({total_mb:.1f}MB)")
    logger.info(f"   🧠 Final memory usage: {final_status['usage_above_baseline_mb']:.1f}MB above baseline")
    logger.info(f"   📁 Output directory: {output_dir}")

class LargeDatasetConfig(ConversionConfig):
    """
    Optimized configuration for very large datasets.
    """
    def __init__(self):
        super().__init__()
        
        # More conservative memory settings for reliability
        self.memory_limit_mb = 1200  # Reduced from 1500 for more safety
        self.cleanup_threshold_ratio = 0.7  # Earlier cleanup trigger
        self.baseline_buffer_mb = 512  # More system memory buffer
        
        # Optimize for large files
        self.row_group_size = 50000  # Smaller row groups for memory efficiency
        self.compression = "snappy"  # Fast compression
        
        # Single-threaded for memory control
        self.workers = 1

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
    
    if largest_file_mb > 1000:  # 1GB+ files (more conservative threshold)
        strategy = "large_file_streaming"
        reasons.append(f"Largest file: {largest_file_mb:.1f}MB")
        
    if total_gb > 20:  # 20GB+ total (more conservative)
        strategy = "sequential_only"
        reasons.append(f"Total dataset: {total_gb:.1f}GB")
        
    if total_files > 50:  # More conservative file count
        strategy = "batch_processing"
        reasons.append(f"Many files: {total_files}")
    
    logger.info(f"Recommended strategy: {strategy}")
    if reasons:
        logger.info(f"Reasons: {', '.join(reasons)}")
    
    return strategy

def convert_csvs_to_parquet_smart(
    audit_map: dict,
    unzip_dir: Path,
    output_dir: Path,
    delimiter: str = ";"
):
    """
    Smart conversion that selects the best strategy based on dataset characteristics.
    Now uses pure Polars approach throughout.
    """
    # Analyze dataset and select strategy
    strategy = select_processing_strategy(audit_map, unzip_dir)
    
    # Use appropriate configuration
    if strategy in ["large_file_streaming", "sequential_only"]:
        config = LargeDatasetConfig()
        logger.info("Using large dataset configuration")
    else:
        config = ConversionConfig()
        logger.info("Using standard configuration")
    
    # All strategies now use the same Polars-only implementation
    # The strategy selection mainly affects configuration parameters
    convert_csvs_to_parquet(
        audit_map,
        unzip_dir,
        output_dir,
        config,
        delimiter
    )

# Utility functions for pre-flight validation
def validate_csv_compatibility(csv_path: Path, delimiter: str = ";") -> Dict[str, any]:
    """
    Pre-flight validation to ensure CSV can be processed by Polars.
    """
    try:
        # Quick compatibility test
        test_scan = pl.scan_csv(
            str(csv_path),
            separator=delimiter,
            encoding="utf8-lossy",
            ignore_errors=True,
            has_header=False,
            n_rows=100  # Very small sample
        )
        
        # Test basic operations
        row_count = test_scan.select(pl.len()).collect().item()
        schema = test_scan.collect_schema()
        
        file_size_mb = csv_path.stat().st_size / (1024 * 1024)
        
        return {
            "compatible": True,
            "file_size_mb": file_size_mb,
            "sample_rows": row_count,
            "columns": len(schema),
            "estimated_total_rows": int((file_size_mb * 1024 * 1024) / (len(str(schema)) * 50)) if len(schema) > 0 else 0
        }
        
    except Exception as e:
        return {
            "compatible": False,
            "error": str(e),
            "file_size_mb": csv_path.stat().st_size / (1024 * 1024) if csv_path.exists() else 0
        }

def pre_flight_check(audit_map: dict, unzip_dir: Path) -> Dict[str, any]:
    """
    Run pre-flight checks on all CSV files to identify potential issues.
    """
    results = {
        "total_files": 0,
        "compatible_files": 0,
        "incompatible_files": 0,
        "total_size_mb": 0,
        "largest_file_mb": 0,
        "issues": []
    }
    
    for table_name, zip_map in audit_map.items():
        csv_paths = [unzip_dir / fname for files in zip_map.values() for fname in files]
        valid_paths = [p for p in csv_paths if p.exists()]
        
        for csv_path in valid_paths:
            results["total_files"] += 1
            validation = validate_csv_compatibility(csv_path)
            
            if validation["compatible"]:
                results["compatible_files"] += 1
                results["total_size_mb"] += validation["file_size_mb"]
                results["largest_file_mb"] = max(results["largest_file_mb"], validation["file_size_mb"])
            else:
                results["incompatible_files"] += 1
                results["issues"].append({
                    "file": str(csv_path),
                    "table": table_name,
                    "error": validation["error"],
                    "size_mb": validation["file_size_mb"]
                })
    
    return results