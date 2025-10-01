from pathlib import Path
from typing import List, Dict, Optional
import polars as pl
import gc
import time

from ....setup.logging import logger
from ..memory.service import MemoryMonitor
from .models import ProcessingStrategy
from .utils import infer_unified_schema, infer_schema_single


def get_processing_strategies(file_size_mb: float, memory_pressure: float) -> List[ProcessingStrategy]:
    """
    Select appropriate processing strategies based on file size and memory pressure.
    Returns strategies ordered from most to least aggressive.
    """
    strategies = []
    
    # Strategy selection based on memory pressure
    if memory_pressure < 0.3:  # Low pressure - can be aggressive
        strategies.extend([
            ProcessingStrategy(name="full_streaming", row_group_size=100000),
            ProcessingStrategy(name="standard_streaming", row_group_size=50000),
            ProcessingStrategy(name="conservative_streaming", row_group_size=25000),
        ])
    elif memory_pressure < 0.6:  # Medium pressure - more conservative
        strategies.extend([
            ProcessingStrategy(name="standard_streaming", row_group_size=50000),
            ProcessingStrategy(name="conservative_streaming", row_group_size=25000),
            ProcessingStrategy(name="chunked_processing", row_group_size=25000, batch_size=500000),
        ])
    else:  # High pressure - very conservative
        strategies.extend([
            ProcessingStrategy(name="conservative_streaming", row_group_size=25000),
            ProcessingStrategy(name="chunked_processing", row_group_size=25000, batch_size=250000),
            ProcessingStrategy(name="minimal_processing", row_group_size=10000, batch_size=100000),
        ])
    
    # Add file size considerations
    if file_size_mb > 1000:  # Large files get additional conservative strategies
        strategies.append(
            ProcessingStrategy(name="micro_chunked", row_group_size=50000, batch_size=50000)
        )
    
    return strategies

def _execute_chunked_strategy(
    csv_path: Path,
    output_path: Path,
    expected_columns: List[str],
    delimiter: str,
    strategy: ProcessingStrategy,
    inferred_schema: Optional[Dict],
    memory_monitor: MemoryMonitor
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
                    dtypes=schema_override,
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
            pl.scan_parquet(str(chunk_files[0])).sink_parquet(str(output_path), ...)
        else:
            lazy_frames = [pl.scan_parquet(str(f)) for f in chunk_files]
            pl.concat(lazy_frames, how="vertical").sink_parquet(  # Strict concat; handle schema mismatches upstream
                str(output_path),
                compression=strategy.compression,
                row_group_size=strategy.row_group_size,
                maintain_order=False,
                statistics=False
            )
        
        # Final verification
        if not output_path.exists():
            raise RuntimeError("Final output file was not created")
        
        output_bytes = output_path.stat().st_size
        
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

def _execute_streaming_strategy(
    csv_path: Path,
    output_path: Path,
    expected_columns: List[str],
    delimiter: str,
    strategy: ProcessingStrategy,
    inferred_schema: Optional[Dict],
    memory_monitor: MemoryMonitor
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
        dtypes=schema_override,
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

    return {
        "rows_processed": row_count,
        "input_bytes": csv_path.stat().st_size,
        "output_bytes": output_bytes,
        "strategy_used": strategy.name,
        "memory_stats": memory_monitor.get_status_report()
    }

def _execute_strategy(
    csv_path: Path,
    output_path: Path,
    expected_columns: List[str],
    delimiter: str,
    strategy: ProcessingStrategy,
    memory_monitor: MemoryMonitor
) -> Dict[str, any]:
    """
    Execute a specific Polars processing strategy.
    """
    logger.debug(f"Executing {strategy.name} on {csv_path.name}")
    
    # Schema inference with error handling
    try:
        inferred_schema = infer_schema_single(csv_path, delimiter, sample_size=5000)
    except Exception as e:
        logger.warning(f"Schema inference failed: {e}. Using string schema.")
        inferred_schema = None
    
    if strategy.batch_size is None:
        # Direct streaming approach
        return _execute_streaming_strategy(
            csv_path, output_path, expected_columns, delimiter,
            strategy, inferred_schema, memory_monitor
        )
    else:
        # Chunked processing approach
        return _execute_chunked_strategy(
            csv_path, output_path, expected_columns, delimiter,
            strategy, inferred_schema, memory_monitor
        )

def process_csv_with_strategies(
    csv_path: Path,
    output_path: Path,
    expected_columns: List[str],
    delimiter: str,
    memory_monitor: MemoryMonitor
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
        raise RuntimeError(
            f"Insufficient memory to process file. "
            f"Usage: {status['usage_above_baseline_mb']:.1f}MB above baseline, "
            f"Limit: {status['configured_limit_mb']}MB"
        )

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
                
                result = _execute_strategy(
                    csv_path, output_path, expected_columns, delimiter,
                    strategy, memory_monitor
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