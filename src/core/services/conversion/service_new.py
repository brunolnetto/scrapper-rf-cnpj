"""
FileConversionService - A comprehensive CSV to Parquet conversion service.

This service encapsulates all conversion logic with improved memory management,
multiple processing strategies, and better error handling.
"""
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import gc
import time
from typing import List, Dict, Optional, Callable, Union, Tuple
from dataclasses import dataclass
from enum import Enum
import logging

import polars as pl
import psutil

from ....setup.logging import logger
from ....setup.config import ConfigLoader
from ....setup.config.models import ConversionConfig
from ..memory.service import MemoryMonitor


class ProcessingStrategy(Enum):
    """Available processing strategies."""
    FULL_STREAMING = "full_streaming"
    STANDARD_STREAMING = "standard_streaming"
    CONSERVATIVE_STREAMING = "conservative_streaming"
    CHUNKED_PROCESSING = "chunked_processing"
    MINIMAL_PROCESSING = "minimal_processing"
    MICRO_CHUNKED = "micro_chunked"


@dataclass
class StrategyConfig:
    """Configuration for a specific processing strategy."""
    strategy: ProcessingStrategy
    row_group_size: int
    batch_size: Optional[int] = None
    compression: str = "snappy"
    low_memory: bool = True
    streaming: bool = True
    compression_level: Optional[int] = None


@dataclass
class ConversionResult:
    """Result of a conversion operation."""
    success: bool
    table_name: str
    rows_processed: int = 0
    input_bytes: int = 0
    output_bytes: int = 0
    strategy_used: str = ""
    chunks_processed: int = 0
    files_processed: int = 0
    files_failed: int = 0
    processing_time: float = 0.0
    compression_ratio: float = 0.0
    error_message: Optional[str] = None
    memory_stats: Optional[Dict] = None


@dataclass
class ValidationResult:
    """Result of CSV validation."""
    compatible: bool
    file_path: Path
    file_size_mb: float
    sample_rows: int = 0
    columns: int = 0
    estimated_total_rows: int = 0
    error_message: Optional[str] = None


class FileConversionService:
    """
    Service for converting CSV files to Parquet format with advanced memory management.
    
    Features:
    - Multiple processing strategies based on file size and memory pressure
    - Comprehensive memory monitoring and cleanup
    - Schema inference and column mapping
    - Chunked processing for large files
    - Parallel and sequential processing modes
    - Pre-flight validation
    - Detailed progress reporting
    """
    
    def __init__(self, config: ConfigLoader = None):
        """Initialize the conversion service."""
        self.config = config
        self.memory_monitor = MemoryMonitor(self.config.pipeline.memory)
        self.logger = logger
        
        # Strategy configurations
        self._strategy_configs = {
            ProcessingStrategy.FULL_STREAMING: StrategyConfig(
                strategy=ProcessingStrategy.FULL_STREAMING,
                row_group_size=100000
            ),
            ProcessingStrategy.STANDARD_STREAMING: StrategyConfig(
                strategy=ProcessingStrategy.STANDARD_STREAMING,
                row_group_size=50000
            ),
            ProcessingStrategy.CONSERVATIVE_STREAMING: StrategyConfig(
                strategy=ProcessingStrategy.CONSERVATIVE_STREAMING,
                row_group_size=25000
            ),
            ProcessingStrategy.CHUNKED_PROCESSING: StrategyConfig(
                strategy=ProcessingStrategy.CHUNKED_PROCESSING,
                row_group_size=25000,
                batch_size=500000
            ),
            ProcessingStrategy.MINIMAL_PROCESSING: StrategyConfig(
                strategy=ProcessingStrategy.MINIMAL_PROCESSING,
                row_group_size=10000,
                batch_size=100000
            ),
            ProcessingStrategy.MICRO_CHUNKED: StrategyConfig(
                strategy=ProcessingStrategy.MICRO_CHUNKED,
                row_group_size=5000,
                batch_size=50000
            )
        }
    
    def validate_csv(self, csv_path: Path, delimiter: str = ";") -> ValidationResult:
        """Pre-flight validation to ensure CSV can be processed."""
        try:
            # Quick compatibility test
            test_scan = pl.scan_csv(
                str(csv_path),
                separator=delimiter,
                encoding="utf8-lossy",
                ignore_errors=True,
                has_header=False,
                n_rows=100
            )
            
            # Test basic operations
            row_count = test_scan.select(pl.len()).collect().item()
            schema = test_scan.collect_schema()
            file_size_mb = csv_path.stat().st_size / (1024 * 1024)
            
            # Estimate total rows
            estimated_rows = int((file_size_mb * 1024 * 1024) / (len(str(schema)) * 50)) if len(schema) > 0 else 0
            
            return ValidationResult(
                compatible=True,
                file_path=csv_path,
                file_size_mb=file_size_mb,
                sample_rows=row_count,
                columns=len(schema),
                estimated_total_rows=estimated_rows
            )
            
        except Exception as e:
            file_size_mb = csv_path.stat().st_size / (1024 * 1024) if csv_path.exists() else 0
            return ValidationResult(
                compatible=False,
                file_path=csv_path,
                file_size_mb=file_size_mb,
                error_message=str(e)
            )
    
    def run_pre_flight_check(self, audit_map: Dict, unzip_dir: Path) -> Dict[str, any]:
        """Run pre-flight checks on all CSV files."""
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
                validation = self.validate_csv(csv_path)
                
                if validation.compatible:
                    results["compatible_files"] += 1
                    results["total_size_mb"] += validation.file_size_mb
                    results["largest_file_mb"] = max(results["largest_file_mb"], validation.file_size_mb)
                else:
                    results["incompatible_files"] += 1
                    results["issues"].append({
                        "file": str(csv_path),
                        "table": table_name,
                        "error": validation.error_message,
                        "size_mb": validation.file_size_mb
                    })
        
        return results
    
    def _infer_schema(self, csv_path: Path, delimiter: str, sample_size: int = 10000) -> Optional[Dict]:
        """Infer optimal schema with error handling."""
        self.logger.debug(f"Inferring schema for {csv_path.name}...")
        
        try:
            sample_df = pl.read_csv(
                str(csv_path),
                separator=delimiter,
                n_rows=sample_size,
                encoding="utf8-lossy",
                ignore_errors=True,
                truncate_ragged_lines=True,
                null_values=["", "NULL", "null", "N/A", "n/a"],
                infer_schema_length=min(sample_size, 5000),
                try_parse_dates=False,
                has_header=False
            )
            
            schema = dict(sample_df.schema)
            self.logger.debug(f"Inferred schema for {csv_path.name}: {len(schema)} columns")
            
            # Clean up sample immediately
            del sample_df
            gc.collect()
            
            return schema
            
        except Exception as e:
            self.logger.debug(f"Schema inference failed for {csv_path.name}: {e}")
            return None
    
    def _select_strategies(self, file_size_mb: float, memory_pressure: float) -> List[StrategyConfig]:
        """Select appropriate processing strategies based on conditions."""
        strategies = []
        
        if memory_pressure < 0.3:  # Low pressure
            strategies.extend([
                self._strategy_configs[ProcessingStrategy.FULL_STREAMING],
                self._strategy_configs[ProcessingStrategy.STANDARD_STREAMING],
                self._strategy_configs[ProcessingStrategy.CONSERVATIVE_STREAMING],
            ])
        elif memory_pressure < 0.6:  # Medium pressure
            strategies.extend([
                self._strategy_configs[ProcessingStrategy.STANDARD_STREAMING],
                self._strategy_configs[ProcessingStrategy.CONSERVATIVE_STREAMING],
                self._strategy_configs[ProcessingStrategy.CHUNKED_PROCESSING],
            ])
        else:  # High pressure
            strategies.extend([
                self._strategy_configs[ProcessingStrategy.CONSERVATIVE_STREAMING],
                self._strategy_configs[ProcessingStrategy.CHUNKED_PROCESSING],
                self._strategy_configs[ProcessingStrategy.MINIMAL_PROCESSING],
            ])
        
        # Add micro-chunked for very large files
        if file_size_mb > 1000:
            strategies.append(self._strategy_configs[ProcessingStrategy.MICRO_CHUNKED])
        
        return strategies
    
    def _execute_streaming_strategy(
        self,
        csv_path: Path,
        output_path: Path,
        expected_columns: List[str],
        delimiter: str,
        strategy_config: StrategyConfig,
        inferred_schema: Optional[Dict],
        progress_callback: Optional[Callable[[int], None]] = None
    ) -> Dict[str, any]:
        """Execute streaming strategy using Polars native streaming."""
        # Set up schema overrides
        if inferred_schema is None:
            if expected_columns:
                schema_override = {f"column_{i+1}": pl.Utf8 for i in range(len(expected_columns))}
            else:
                schema_override = None
        else:
            schema_override = inferred_schema

        # Create lazy frame
        lazy_frame = pl.scan_csv(
            str(csv_path),
            separator=delimiter,
            schema_overrides=schema_override,
            encoding="utf8-lossy",
            ignore_errors=True,
            truncate_ragged_lines=True,
            null_values=["", "NULL", "null", "N/A", "n/a"],
            try_parse_dates=True,
            low_memory=strategy_config.low_memory,
            rechunk=False,
            infer_schema_length=5000,
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
                    self.logger.info(f"Renamed columns to expected names: {expected_columns}")
            except Exception as e:
                self.logger.warning(f"Column processing failed: {e}")

        # Execute streaming
        self.logger.info(f"Streaming {csv_path.name} with {strategy_config.strategy.value}")
        
        lazy_frame.sink_parquet(
            str(output_path),
            compression=strategy_config.compression,
            row_group_size=strategy_config.row_group_size,
            maintain_order=False,
            statistics=False,
            compression_level=strategy_config.compression_level
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
            self.logger.warning(f"Could not determine row count: {e}")
            row_count = 0

        if progress_callback:
            progress_callback(row_count)

        return {
            "rows_processed": row_count,
            "input_bytes": csv_path.stat().st_size,
            "output_bytes": output_bytes,
            "strategy_used": strategy_config.strategy.value,
            "memory_stats": self.memory_monitor.get_status_report()
        }
    
    def _execute_chunked_strategy(
        self,
        csv_path: Path,
        output_path: Path,
        expected_columns: List[str],
        delimiter: str,
        strategy_config: StrategyConfig,
        inferred_schema: Optional[Dict],
        progress_callback: Optional[Callable[[int], None]] = None
    ) -> Dict[str, any]:
        """Execute chunked processing strategy."""
        self.logger.info(f"Using chunked strategy: {strategy_config.strategy.value}")
        
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
                    # Set up schema
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
                        n_rows=strategy_config.batch_size,
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
                        compression=strategy_config.compression,
                        row_group_size=strategy_config.row_group_size,
                        statistics=False
                    )
                    
                    chunk_files.append(chunk_output)
                    total_rows += len(chunk_df)
                    
                    self.logger.debug(f"Chunk {chunk_num}: {len(chunk_df):,} rows")
                    
                    # Cleanup chunk data
                    del chunk_df, lazy_chunk
                    gc.collect()
                    
                    # Memory pressure check
                    if self.memory_monitor.is_memory_pressure_high():
                        self.logger.info(f"Memory pressure detected after chunk {chunk_num}")
                        self.memory_monitor.perform_aggressive_cleanup()
                        time.sleep(0.5)
                    
                    offset += strategy_config.batch_size
                    chunk_num += 1
                    
                    # Safety check
                    if chunk_num > 1000:
                        self.logger.warning("Created 1000+ chunks, stopping")
                        break
                        
                except Exception as e:
                    self.logger.error(f"Failed to process chunk {chunk_num}: {e}")
                    break
            
            if not chunk_files:
                raise RuntimeError("No chunks were successfully processed")
            
            # Combine chunks
            self.logger.info(f"Combining {len(chunk_files)} chunks")
            
            if len(chunk_files) == 1:
                chunk_files[0].rename(output_path)
            else:
                lazy_frames = [pl.scan_parquet(str(f)) for f in chunk_files]
                combined = pl.concat(lazy_frames, how="vertical")
                combined.sink_parquet(
                    str(output_path),
                    compression=strategy_config.compression,
                    row_group_size=strategy_config.row_group_size,
                    maintain_order=False,
                    statistics=False
                )
                
                # Cleanup intermediate files
                for chunk_file in chunk_files:
                    try:
                        chunk_file.unlink()
                    except:
                        pass
            
            if not output_path.exists():
                raise RuntimeError("Final output file was not created")
            
            output_bytes = output_path.stat().st_size
            
            if progress_callback:
                progress_callback(total_rows)
            
            return {
                "rows_processed": total_rows,
                "input_bytes": csv_path.stat().st_size,
                "output_bytes": output_bytes,
                "strategy_used": strategy_config.strategy.value,
                "chunks_processed": len(chunk_files),
                "memory_stats": self.memory_monitor.get_status_report()
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
                self.logger.warning(f"Cleanup of temp files failed: {e}")
    
    def convert_single_csv(
        self,
        csv_path: Path,
        output_path: Path,
        expected_columns: List[str],
        delimiter: str = ";",
        progress_callback: Optional[Callable[[int], None]] = None
    ) -> ConversionResult:
        """Convert a single CSV file to Parquet format."""
        if not csv_path.exists():
            return ConversionResult(
                success=False,
                table_name=csv_path.stem,
                error_message=f"CSV file not found: {csv_path}"
            )

        input_bytes = csv_path.stat().st_size
        file_size_mb = input_bytes / (1024 * 1024)
        start_time = time.time()
        
        self.logger.info(f"Processing {csv_path.name} ({input_bytes:,} bytes)")

        # Pre-processing memory check
        if self.memory_monitor.should_prevent_processing():
            status = self.memory_monitor.get_status_report()
            return ConversionResult(
                success=False,
                table_name=csv_path.stem,
                error_message=f"Insufficient memory. Usage: {status['usage_above_baseline_mb']:.1f}MB above baseline"
            )

        # Get processing strategies
        memory_pressure = self.memory_monitor.get_memory_pressure_level()
        strategies = self._select_strategies(file_size_mb, memory_pressure)
        
        self.logger.info(f"Selected {len(strategies)} processing strategies")

        try:
            output_path.parent.mkdir(parents=True, exist_ok=True)

            # Try each strategy until one succeeds
            for i, strategy_config in enumerate(strategies):
                self.logger.info(f"Attempting strategy {i+1}/{len(strategies)}: {strategy_config.strategy.value}")
                
                try:
                    # Cleanup before each strategy attempt
                    if i > 0:
                        self.memory_monitor.perform_aggressive_cleanup()
                        time.sleep(0.5)
                    
                    # Check memory again
                    if self.memory_monitor.should_prevent_processing():
                        self.logger.warning(f"Memory pressure too high for strategy {strategy_config.strategy.value}")
                        continue
                    
                    # Infer schema
                    inferred_schema = self._infer_schema(csv_path, delimiter, sample_size=5000)
                    
                    # Execute strategy
                    if strategy_config.batch_size is None:
                        result = self._execute_streaming_strategy(
                            csv_path, output_path, expected_columns, delimiter,
                            strategy_config, inferred_schema, progress_callback
                        )
                    else:
                        result = self._execute_chunked_strategy(
                            csv_path, output_path, expected_columns, delimiter,
                            strategy_config, inferred_schema, progress_callback
                        )
                    
                    # Strategy succeeded
                    processing_time = time.time() - start_time
                    compression_ratio = input_bytes / result["output_bytes"] if result["output_bytes"] > 0 else 0
                    
                    self.logger.info(f"✅ Strategy {strategy_config.strategy.value} succeeded")
                    
                    return ConversionResult(
                        success=True,
                        table_name=csv_path.stem,
                        rows_processed=result["rows_processed"],
                        input_bytes=input_bytes,
                        output_bytes=result["output_bytes"],
                        strategy_used=result["strategy_used"],
                        chunks_processed=result.get("chunks_processed", 0),
                        files_processed=1,
                        processing_time=processing_time,
                        compression_ratio=compression_ratio,
                        memory_stats=result["memory_stats"]
                    )
                    
                except (MemoryError, pl.ComputeError) as e:
                    self.logger.warning(f"Strategy {strategy_config.strategy.value} failed: {e}")
                    # Clean up any partial output
                    if output_path.exists():
                        try:
                            output_path.unlink()
                        except:
                            pass
                    continue
                    
                except Exception as e:
                    self.logger.error(f"Unexpected error in strategy {strategy_config.strategy.value}: {e}")
                    if output_path.exists():
                        try:
                            output_path.unlink()
                        except:
                            pass
                    continue
            
            # All strategies failed
            return ConversionResult(
                success=False,
                table_name=csv_path.stem,
                error_message="All processing strategies failed",
                processing_time=time.time() - start_time
            )
            
        except Exception as e:
            # Final cleanup on total failure
            if output_path.exists():
                try:
                    output_path.unlink()
                except:
                    pass
            self.memory_monitor.perform_aggressive_cleanup()
            
            return ConversionResult(
                success=False,
                table_name=csv_path.stem,
                error_message=str(e),
                processing_time=time.time() - start_time
            )
    
    def convert_table_csvs(
        self,
        table_name: str,
        csv_paths: List[Path],
        output_dir: Path,
        delimiter: str,
        expected_columns: List[str],
        progress_callback: Optional[Callable[[int], None]] = None
    ) -> ConversionResult:
        """Convert multiple CSV files for a single table."""
        start_time = time.time()
        
        try:
            if not expected_columns:
                return ConversionResult(
                    success=False,
                    table_name=table_name,
                    error_message="No column mapping provided"
                )

            valid_files = [p for p in csv_paths if p.exists()]
            if not valid_files:
                return ConversionResult(
                    success=False,
                    table_name=table_name,
                    error_message="No valid CSV files found"
                )

            output_dir.mkdir(parents=True, exist_ok=True)
            final_output = output_dir / f"{table_name}.parquet"

            if final_output.exists():
                final_output.unlink()

            total_input_bytes = sum(p.stat().st_size for p in valid_files)
            self.logger.info(f"Converting '{table_name}': {len(valid_files)} files, {total_input_bytes:,} bytes")

            processed_files = []
            stats = {"files_processed": 0, "files_failed": 0, "total_rows": 0}

            # Process files individually
            for i, csv_path in enumerate(valid_files, 1):
                try:
                    self.logger.info(f"Processing file {i}/{len(valid_files)}: {csv_path.name}")
                    
                    # Cleanup between files
                    if i > 1:
                        self.memory_monitor.perform_aggressive_cleanup()
                        time.sleep(0.5)
                    
                    # Check memory before each file
                    if self.memory_monitor.should_prevent_processing():
                        self.logger.error(f"Memory limit reached before file {i}")
                        break

                    # Use individual file naming for large datasets
                    if len(valid_files) == 1:
                        temp_output = final_output
                    else:
                        temp_output = output_dir / f"{table_name}_part_{i:03d}.parquet"

                    result = self.convert_single_csv(
                        csv_path, temp_output, expected_columns, delimiter,
                        lambda rows: progress_callback(f"{table_name}_file_{i}", rows) if progress_callback else None
                    )

                    if result.success:
                        processed_files.append(temp_output)
                        stats["files_processed"] += 1
                        stats["total_rows"] += result.rows_processed
                        self.logger.info(f"✅ File {i}: {result.rows_processed:,} rows using {result.strategy_used}")
                    else:
                        stats["files_failed"] += 1
                        self.logger.error(f"❌ File {i} failed: {result.error_message}")

                except Exception as e:
                    self.logger.error(f"❌ Failed file {i} ({csv_path.name}): {e}")
                    stats["files_failed"] += 1
                    continue

            if not processed_files:
                return ConversionResult(
                    success=False,
                    table_name=table_name,
                    error_message="No files successfully processed",
                    files_failed=len(valid_files),
                    processing_time=time.time() - start_time
                )

            # Combine files if necessary
            final_bytes = 0
            if len(processed_files) == 1 and processed_files[0] != final_output:
                processed_files[0].rename(final_output)
                final_bytes = final_output.stat().st_size
            elif len(processed_files) > 1:
                self.logger.info(f"Combining {len(processed_files)} parquet files...")
                
                try:
                    lazy_frames = [pl.scan_parquet(str(f)) for f in processed_files]
                    combined = pl.concat(lazy_frames, how="vertical")
                    combined.sink_parquet(
                        str(final_output),
                        compression=self.config.conversion.compression,
                        row_group_size=self.config.conversion.row_group_size,
                        maintain_order=False,
                        statistics=False
                    )
                    
                    final_bytes = final_output.stat().st_size
                    
                    # Cleanup parts
                    for part_file in processed_files:
                        try:
                            if part_file.exists() and part_file != final_output:
                                part_file.unlink()
                        except:
                            pass
                    
                except Exception as e:
                    self.logger.error(f"Failed to combine files: {e}")
                    return ConversionResult(
                        success=False,
                        table_name=table_name,
                        error_message=f"File combination failed: {e}",
                        files_processed=stats["files_processed"],
                        files_failed=stats["files_failed"],
                        processing_time=time.time() - start_time
                    )
            else:
                final_bytes = final_output.stat().st_size if final_output.exists() else 0

            # Calculate metrics
            processing_time = time.time() - start_time
            compression_ratio = total_input_bytes / final_bytes if final_bytes > 0 else 0

            return ConversionResult(
                success=True,
                table_name=table_name,
                rows_processed=stats["total_rows"],
                input_bytes=total_input_bytes,
                output_bytes=final_bytes,
                files_processed=stats["files_processed"],
                files_failed=stats["files_failed"],
                processing_time=processing_time,
                compression_ratio=compression_ratio,
                memory_stats=self.memory_monitor.get_status_report()
            )

        except Exception as e:
            return ConversionResult(
                success=False,
                table_name=table_name,
                error_message=str(e),
                processing_time=time.time() - start_time
            )
    
    def convert_audit_map(
        self,
        audit_map: Dict,
        unzip_dir: Path,
        output_dir: Path,
        delimiter: str = ";",
        progress_callback: Optional[Callable[[str, int], None]] = None
    ) -> List[ConversionResult]:
        """Convert all tables from an audit map."""
        output_dir.mkdir(exist_ok=True)

        if not audit_map:
            self.logger.warning("No tables to convert")
            return []

        self.logger.info(f"Starting conversion of {len(audit_map)} tables")
        
        # Log system info
        try:
            memory = psutil.virtual_memory()
            self.logger.info(f"System Memory: {memory.total/(1024**3):.2f}GB total, "
                           f"{memory.available/(1024**3):.2f}GB available")
        except:
            pass

        # Prepare work queue
        from ....database.utils import get_table_columns
        
        table_work = []
        for table_name, zip_map in audit_map.items():
            csv_paths = [unzip_dir / fname for files in zip_map.values() for fname in files]
            valid_paths = [p for p in csv_paths if p.exists()]

            if not valid_paths:
                self.logger.warning(f"No valid files for '{table_name}'")
                continue

            expected_columns = get_table_columns(table_name)
            total_bytes = sum(p.stat().st_size for p in valid_paths)
            table_work.append((table_name, valid_paths, expected_columns, total_bytes))

        # Sort by size (largest first) for better memory management
        table_work.sort(key=lambda x: x[3], reverse=True)
        self.logger.info(f"Processing {len(table_work)} tables with valid data")

        # Determine processing approach
        large_dataset_threshold = 1024 * 1024 * 1024  # 1GB
        has_large_datasets = any(total_bytes > large_dataset_threshold for _, _, _, total_bytes in table_work)
        
        if has_large_datasets:
            self.logger.info("Large datasets detected - using sequential processing")
        else:
            self.logger.info("Using parallel processing for smaller datasets")

        results = []

        results = self._convert_sequential(table_work, output_dir, delimiter, progress_callback)

        # Summary
        successful = [r for r in results if r.success]
        failed = [r for r in results if not r.success]
        total_rows = sum(r.rows_processed for r in successful)
        total_bytes = sum(r.output_bytes for r in successful)

        self.logger.info("Conversion Summary:")
        self.logger.info(f"   ✅ Successful: {len(successful)} tables")
        self.logger.info(f"   ❌ Failed: {len(failed)} tables")
        self.logger.info(f"   📊 Total processed: {total_rows:,} rows, {total_bytes:,} bytes")
        self.logger.info(f"   📁 Output directory: {output_dir}")

        return results
    
    def _convert_parallel(
        self,
        table_work: List[Tuple],
        output_dir: Path,
        delimiter: str,
        progress_callback: Optional[Callable[[str, int], None]]
    ) -> List[ConversionResult]:
        """Convert tables using parallel processing."""
        results = []
        
        with ThreadPoolExecutor(max_workers=min(self.config.pipeline.conversion.workers, 2)) as executor:
            try:
                from rich.progress import (
                    Progress, SpinnerColumn, BarColumn, TextColumn,
                    TimeElapsedColumn, MofNCompleteColumn, TimeRemainingColumn
                )
                
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
                    
                    for table_name, csv_paths, expected_columns, total_bytes in table_work:
                        task = executor.submit(
                            self.convert_table_csvs,
                            table_name,
                            csv_paths,
                            output_dir,
                            delimiter,
                            expected_columns,
                            progress_callback
                        )
                        tasks[task] = (table_name, total_bytes, len(csv_paths))

                    # Process results
                    completed = 0
                    for future in as_completed(tasks):
                        table_name, table_bytes, file_count = tasks[future]

                        try:
                            result = future.result()
                            results.append(result)
                            
                            if result.success:
                                self.logger.info(f"✅ {table_name}: {result.rows_processed:,} rows")
                            else:
                                self.logger.error(f"❌ {table_name}: {result.error_message}")

                        except Exception as e:
                            error_result = ConversionResult(
                                success=False,
                                table_name=table_name,
                                error_message=str(e)
                            )
                            results.append(error_result)
                            self.logger.error(f"❌ Exception in '{table_name}': {e}")

                        completed += 1
                        progress.update(main_task, completed=completed)

                        # Inter-task cleanup
                        self.memory_monitor.perform_aggressive_cleanup()
                        
            except ImportError:
                # Fallback without rich progress
                self.logger.info("Rich not available, using simple progress logging")
                tasks = {}
                
                for table_name, csv_paths, expected_columns, total_bytes in table_work:
                    task = executor.submit(
                        self.convert_table_csvs,
                        table_name,
                        csv_paths,
                        output_dir,
                        delimiter,
                        expected_columns,
                        progress_callback
                    )
                    tasks[task] = table_name

                for i, future in enumerate(as_completed(tasks), 1):
                    table_name = tasks[future]
                    self.logger.info(f"Processing table {i}/{len(table_work)}: {table_name}")
                    
                    try:
                        result = future.result()
                        results.append(result)
                    except Exception as e:
                        error_result = ConversionResult(
                            success=False,
                            table_name=table_name,
                            error_message=str(e)
                        )
                        results.append(error_result)
                    
                    self.memory_monitor.perform_aggressive_cleanup()

        return results
    
    def _convert_sequential(
        self,
        table_work: List[Tuple],
        output_dir: Path,
        delimiter: str,
        progress_callback: Optional[Callable[[str, int], None]]
    ) -> List[ConversionResult]:
        """Convert tables using sequential processing."""
        results = []
        
        try:
            from rich.progress import (
                Progress, SpinnerColumn, BarColumn, TextColumn,
                TimeElapsedColumn, MofNCompleteColumn
            )
            
            with Progress(
                SpinnerColumn(),
                TextColumn("[bold green]{task.description}"),
                BarColumn(),
                MofNCompleteColumn(),
                TimeElapsedColumn(),
            ) as progress:

                main_task = progress.add_task("Converting tables", total=len(table_work))
                
                for i, (table_name, csv_paths, expected_columns, total_bytes) in enumerate(table_work):
                    self.logger.info(f"Processing table {i+1}/{len(table_work)}: {table_name} ({total_bytes:,} bytes)")
                    
                    # Aggressive cleanup before each table
                    if i > 0:
                        self.logger.info("Performing inter-table cleanup...")
                        self.memory_monitor.perform_aggressive_cleanup()
                        time.sleep(1.0)
                    
                    try:
                        result = self.convert_table_csvs(
                            table_name,
                            csv_paths,
                            output_dir,
                            delimiter,
                            expected_columns,
                            progress_callback
                        )
                        
                        results.append(result)
                        
                        if result.success:
                            self.logger.info(f"✅ {table_name}: {result.rows_processed:,} rows")
                        else:
                            self.logger.error(f"❌ {table_name}: {result.error_message}")
                            
                    except Exception as e:
                        error_result = ConversionResult(
                            success=False,
                            table_name=table_name,
                            error_message=str(e)
                        )
                        results.append(error_result)
                        self.logger.error(f"❌ Exception in '{table_name}': {e}")

                    progress.update(main_task, completed=i+1)
                    
        except ImportError:
            # Fallback without rich progress
            for i, (table_name, csv_paths, expected_columns, total_bytes) in enumerate(table_work):
                self.logger.info(f"Processing table {i+1}/{len(table_work)}: {table_name}")
                
                if i > 0:
                    self.memory_monitor.perform_aggressive_cleanup()
                    time.sleep(1.0)
                
                try:
                    result = self.convert_table_csvs(
                        table_name,
                        csv_paths,
                        output_dir,
                        delimiter,
                        expected_columns,
                        progress_callback
                    )
                    results.append(result)
                except Exception as e:
                    error_result = ConversionResult(
                        success=False,
                        table_name=table_name,
                        error_message=str(e)
                    )
                    results.append(error_result)

        return results
    
    def analyze_dataset(self, audit_map: Dict, unzip_dir: Path) -> Dict[str, any]:
        """Analyze the dataset and recommend processing strategy."""
        total_files = 0
        total_bytes = 0
        largest_file_mb = 0
        table_info = {}
        
        for table_name, zip_map in audit_map.items():
            csv_paths = [unzip_dir / fname for files in zip_map.values() for fname in files]
            valid_paths = [p for p in csv_paths if p.exists()]
            
            table_bytes = 0
            table_files = len(valid_paths)
            largest_table_file = 0
            
            for path in valid_paths:
                size_mb = path.stat().st_size / (1024 * 1024)
                total_files += 1
                total_bytes += path.stat().st_size
                table_bytes += path.stat().st_size
                largest_file_mb = max(largest_file_mb, size_mb)
                largest_table_file = max(largest_table_file, size_mb)
            
            table_info[table_name] = {
                "files": table_files,
                "total_mb": table_bytes / (1024 * 1024),
                "largest_file_mb": largest_table_file
            }
        
        total_gb = total_bytes / (1024 * 1024 * 1024)
        
        # Determine recommended strategy
        strategy = "standard"
        reasons = []
        
        if largest_file_mb > 1000:  # 1GB+ files
            strategy = "large_file_streaming"
            reasons.append(f"Largest file: {largest_file_mb:.1f}MB")
            
        if total_gb > 20:  # 20GB+ total
            strategy = "sequential_only"
            reasons.append(f"Total dataset: {total_gb:.1f}GB")
            
        if total_files > 50:
            strategy = "batch_processing"
            reasons.append(f"Many files: {total_files}")
        
        return {
            "total_files": total_files,
            "total_gb": total_gb,
            "largest_file_mb": largest_file_mb,
            "recommended_strategy": strategy,
            "reasons": reasons,
            "table_breakdown": table_info,
            "memory_recommendation": self._get_memory_recommendation(total_gb, largest_file_mb)
        }
    
    def _get_memory_recommendation(self, total_gb: float, largest_file_mb: float) -> Dict[str, any]:
        """Get memory configuration recommendations."""
        # Base memory requirements
        base_memory_mb = 512  # System buffer
        
        # Per-file processing memory (conservative estimate)
        file_processing_mb = min(largest_file_mb * 0.3, 1024)  # 30% of file size, max 1GB
        
        # Total recommended memory
        recommended_mb = base_memory_mb + file_processing_mb
        
        # Configuration recommendations
        if recommended_mb > 2048:
            config_type = "large_dataset"
            workers = 1
        elif recommended_mb > 1024:
            config_type = "medium_dataset"
            workers = 2
        else:
            config_type = "standard"
            workers = min(4, self.config.pipeline.conversion.workers)
        
        return {
            "recommended_memory_mb": recommended_mb,
            "config_type": config_type,
            "recommended_workers": workers,
            "estimated_file_memory_mb": file_processing_mb
        }
    
    def get_optimal_config(self, audit_map: Dict, unzip_dir: Path) -> ConversionConfig:
        """Get optimal configuration based on dataset analysis."""
        analysis = self.analyze_dataset(audit_map, unzip_dir)
        memory_rec = analysis["memory_recommendation"]
        
        config = ConversionConfig()
        
        # Adjust based on recommendations
        if memory_rec["config_type"] == "large_dataset":
            config.row_group_size = 50000
            config.compression = "snappy"
            config.workers = 1
            
        elif memory_rec["config_type"] == "medium_dataset":
            config.row_group_size = 100000
            config.compression = "snappy"
            config.workers = 2
            
        else:  # standard
            config.row_group_size = 150000
            config.compression = "snappy"
            config.workers = min(4, memory_rec["recommended_workers"])
        
        self.logger.info(f"Optimal config selected: {memory_rec['config_type']} "
                        f"({config.workers} workers)")
        
        return config
    
    def convert_with_auto_config(
        self,
        audit_map: Dict,
        unzip_dir: Path,
        output_dir: Path,
        delimiter: str = ";",
        progress_callback: Optional[Callable[[str, int], None]] = None
    ) -> List[ConversionResult]:
        """Convert with automatically optimized configuration."""
        # Analyze dataset and get optimal config
        optimal_config = self.get_optimal_config(audit_map, unzip_dir)
        
        # Create new service instance with optimal config
        optimal_service = FileConversionService(optimal_config)
        
        # Run conversion
        return optimal_service.convert_audit_map(
            audit_map, unzip_dir, output_dir, delimiter, 
            use_parallel=None, progress_callback=progress_callback
        )


# Utility function to maintain backward compatibility
def convert_csvs_to_parquet_smart(
    audit_map: dict,
    unzip_dir: Path,
    output_dir: Path,
    delimiter: str = ";"
) -> List[ConversionResult]:
    """
    Smart conversion function that maintains backward compatibility.
    Uses FileConversionService with automatic configuration.
    """
    service = FileConversionService()
    return service.convert_with_auto_config(
        audit_map, unzip_dir, output_dir, delimiter
    )