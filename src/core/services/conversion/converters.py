"""
Drop-in replacement (safer) for convert_table_csvs with memory-ballooning mitigations.
Key changes:
- Stream-merge writes batches to part files (avoids repeated reads of growing output).
- Two-pass writes chunk-level parquet files per partition and only assembles them as lazy frames.
- Memory checks before expensive concatenations; fallback to single-file writes.
- Stronger cleanup and capped partition count to avoid explosion of temp files.
- Additional logging of memory usage points for profiling.

Trade-offs: This approach prefers more intermediate parquet files (disk I/O) to reduce peak memory.
"""

from pathlib import Path
from typing import List, Dict, Optional
import polars as pl
import gc
import time
import math

from ....setup.logging import logger
from .strategies import process_csv_with_polars_strategies

def convert_with_two_pass_integration(
    csv_paths: List[Path],
    output_path: Path,
    expected_columns: List[str],
    delimiter: str,
    memory_budget_mb: float,
    memory_monitor,
    config
) -> Dict:
    unified_schema = infer_unified_schema(csv_paths, delimiter)
    if not unified_schema and expected_columns:
        unified_schema = {f"column_{i+1}": pl.Utf8 for i in range(len(expected_columns))}
    
    # Pass 1: analyze files (same as before but robust)
    file_info = []
    for path in csv_paths:
        size_mb = path.stat().st_size / (1024 * 1024)
        try:
            sample = pl.read_csv(
                str(path),
                separator=delimiter,
                n_rows=100,
                encoding="utf8-lossy",
                ignore_errors=True,
                has_header=False
            )
            sample_len = max(1, len(sample))
            sample_bytes = max(1, path.stat().st_size * 0.0001)
            bytes_per_row = max(100, sample_bytes / sample_len)
            estimated_rows = int(path.stat().st_size / bytes_per_row)
            del sample
            gc.collect()
        except Exception as e:
            logger.warning(f"Could not analyze {path.name}: {e}")
            bytes_per_row = 500
            estimated_rows = int(size_mb * 1024 * 1024 / bytes_per_row)
        
        file_info.append({
            "path": path,
            "size_mb": size_mb,
            "estimated_rows": max(1, estimated_rows),
            "bytes_per_row": bytes_per_row
        })
    
    # Partition planning (cap number of partitions to avoid explosion)
    usable_budget = max(100, memory_budget_mb * 0.7)
    partitions: List[List[Dict]] = []
    current_partition = []
    current_size = 0.0
    for info in sorted(file_info, key=lambda x: x["size_mb"]):
        if info["size_mb"] > usable_budget * 0.9:
            chunks_needed = max(1, int(math.ceil(info["size_mb"] / (usable_budget * 0.8))))
            rows_per_chunk = max(10000, info["estimated_rows"] // chunks_needed)
            for i in range(chunks_needed):
                start_row = i * rows_per_chunk
                remaining_rows = info["estimated_rows"] - start_row
                num_rows = min(rows_per_chunk, remaining_rows) if i < chunks_needed - 1 else None
                if start_row < info["estimated_rows"]:
                    partitions.append([{"path": info["path"], "start_row": start_row, "num_rows": num_rows}])
        else:
            if current_size + info["size_mb"] > usable_budget and current_partition:
                partitions.append(current_partition)
                current_partition = []
                current_size = 0.0
            current_partition.append({"path": info["path"], "start_row": 0, "num_rows": None})
            current_size += info["size_mb"]
    if current_partition:
        partitions.append(current_partition)
    
    # cap partitions
    MAX_PARTITIONS = 500
    if len(partitions) > MAX_PARTITIONS:
        logger.warning(f"Partition count {len(partitions)} exceeds MAX_PARTITIONS={MAX_PARTITIONS}. Merging adjacent partitions to reduce count.")
        new_parts = []
        group_size = math.ceil(len(partitions) / MAX_PARTITIONS)
        for i in range(0, len(partitions), group_size):
            merged = []
            for p in partitions[i:i+group_size]:
                merged.extend(p)
            new_parts.append(merged)
        partitions = new_parts
    
    if not partitions:
        raise RuntimeError("Could not create any valid partitions")    
    logger.info(f"Created {len(partitions)} partitions")    
    
    temp_dir = output_path.parent / f".parts_{output_path.stem}"
    temp_dir.mkdir(exist_ok=True)
    partition_files = []
    total_rows = 0
    
    try:
        for part_idx, partition in enumerate(partitions):
            part_output = temp_dir / f"part_{part_idx:04d}.parquet"
            # use a subdir for per-chunk intermediate files to avoid holding dfs
            part_chunk_dir = temp_dir / f"part_{part_idx:04d}_chunks"
            part_chunk_dir.mkdir(exist_ok=True)
            chunk_idx = 0
            chunk_files = []
            
            for chunk_info in partition:
                try:
                    df = pl.read_csv(
                        str(chunk_info["path"]),
                        separator=delimiter,
                        schema_overrides=unified_schema,
                        encoding="utf8-lossy",
                        ignore_errors=True,
                        skip_rows=chunk_info["start_row"],
                        n_rows=chunk_info["num_rows"],
                        has_header=False,
                        low_memory=True,
                        rechunk=False
                    )
                    if len(df) == 0:
                        continue
                    if expected_columns and len(df.columns) == len(expected_columns):
                        df = df.rename({df.columns[i]: expected_columns[i] for i in range(len(expected_columns))})
                    # write chunk to disk immediately
                    chunk_file = part_chunk_dir / f"chunk_{chunk_idx:06d}.parquet"
                    df.write_parquet(str(chunk_file), compression=config.compression if hasattr(config, 'compression') else "snappy", row_group_size=50000, statistics=False)
                    chunk_files.append(chunk_file)
                    total_rows += len(df)
                    chunk_idx += 1
                    del df
                    gc.collect()
                    if memory_monitor.is_memory_pressure_high():
                        memory_monitor.perform_aggressive_cleanup()
                        time.sleep(0.2)
                except Exception as e:
                    logger.warning(f"Failed to read/write chunk from {chunk_info['path'].name}: {e}")
                    continue
            
            if not chunk_files:
                logger.warning(f"Partition {part_idx} has no data, skipping")
                # cleanup empty chunk dir
                try:
                    for p in part_chunk_dir.glob('*'):
                        p.unlink()
                    part_chunk_dir.rmdir()
                except:
                    pass
                continue
            
            # combine chunk files lazily into single partition file
            if len(chunk_files) == 1:
                try:
                    chunk_files[0].rename(part_output)
                except Exception:
                    lazy = pl.scan_parquet(str(chunk_files[0]))
                    lazy.sink_parquet(str(part_output), compression=config.compression if hasattr(config, 'compression') else "snappy")
            else:
                lazy_frames = [pl.scan_parquet(str(f)) for f in chunk_files]
                combined = pl.concat(lazy_frames, how="vertical_relaxed")
                combined.sink_parquet(str(part_output), compression=config.compression if hasattr(config, 'compression') else "snappy", row_group_size=50000, maintain_order=False, statistics=False)
                # remove chunk files
                for f in chunk_files:
                    try:
                        f.unlink()
                    except:
                        pass
            partition_files.append(part_output)
            # remove the chunk dir
            try:
                part_chunk_dir.rmdir()
            except:
                pass
            if memory_monitor.is_memory_pressure_high():
                memory_monitor.perform_aggressive_cleanup()
        
        if not partition_files:
            raise RuntimeError("No partitions were successfully created")
        
        # Final merge into output_path
        if len(partition_files) == 1:
            try:
                partition_files[0].rename(output_path)
            except Exception:
                lazy = pl.scan_parquet(str(partition_files[0]))
                lazy.sink_parquet(str(output_path), compression=config.compression if hasattr(config, 'compression') else "snappy")
        else:
            lazy_frames = [pl.scan_parquet(str(f)) for f in partition_files]
            combined = pl.concat(lazy_frames, how="vertical_relaxed")
            combined.sink_parquet(str(output_path), compression=config.compression if hasattr(config, 'compression') else "snappy", row_group_size=50000, maintain_order=False, statistics=False)
            # cleanup partitions
            for pf in partition_files:
                try:
                    pf.unlink()
                except:
                    pass
        
        return {"rows_processed": total_rows, "output_bytes": output_path.stat().st_size if output_path.exists() else 0, "partitions_used": len(partitions), "strategy": "two_pass_planned"}
    finally:
        # Final best-effort cleanup of any leftover chunk files and directories
        try:
            if temp_dir.exists():
                for p in temp_dir.glob('*'):
                    try:
                        # remove nested directories' files then dirs
                        if p.is_dir():
                            for c in p.glob('*'):
                                try:
                                    c.unlink()
                                except:
                                    pass
                            try:
                                p.rmdir()
                            except:
                                pass
                        else:
                            try:
                                p.unlink()
                            except:
                                pass
                    except Exception:
                        pass
                try:
                    temp_dir.rmdir()
                except:
                    pass
        except Exception:
            pass


def convert_table_csvs_multifile(
    table_name: str,
    csv_paths: List[Path],
    output_dir: Path,
    delimiter: str,
    expected_columns: List[str],
    config,
    memory_monitor
) -> str:
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
        total_mb = total_input_bytes / (1024 * 1024)
        largest_file_mb = max(p.stat().st_size for p in valid_files) / (1024 * 1024)
        
        logger.info(f"Converting '{table_name}': {len(valid_files)} files, "
                   f"{total_mb:.1f}MB total, largest: {largest_file_mb:.1f}MB")
        
        status = memory_monitor.get_status_report()
        available_budget_mb = max(100.0, status.get('budget_remaining_mb', 100.0))
        
        logger.info(f"Memory budget: {available_budget_mb:.1f}MB available")
        
        start_time = time.time()
        
        # Strategy selection with safer thresholds
        if len(valid_files) == 1:
            logger.info("Using single-file strategies")
            result = process_csv_with_polars_strategies(
                valid_files[0], final_output, expected_columns, delimiter,
                config, memory_monitor
            )
        elif largest_file_mb > 500:  # Files over ~500MB use two-pass (conservative)
            logger.info(f"Using two-pass strategy (large file: {largest_file_mb:.1f}MB)")
            result = convert_with_two_pass_integration(
                valid_files, final_output, expected_columns, delimiter,
                available_budget_mb, memory_monitor, config
            )
        elif len(valid_files) <= 5 and total_mb < available_budget_mb:
            logger.info(f"Using stream-merge strategy (small dataset: {total_mb:.1f}MB)")
            result = convert_with_stream_merge_integration(
                valid_files, final_output, expected_columns, delimiter,
                memory_monitor, config
            )
        else:
            logger.info(f"Using two-pass strategy ({len(valid_files)} files, {total_mb:.1f}MB)")
            result = convert_with_two_pass_integration(
                valid_files, final_output, expected_columns, delimiter,
                available_budget_mb, memory_monitor, config
            )
        
        elapsed_time = time.time() - start_time
        processing_rate = total_mb / elapsed_time if elapsed_time > 0 else 0
        compression_ratio = total_input_bytes / result['output_bytes'] if result['output_bytes'] > 0 else 0
        
        result_msg = (f"[OK] '{table_name}': {result['rows_processed']:,} rows, "
                     f"{len(valid_files)} files, "
                     f"{result['output_bytes']:,} bytes ({compression_ratio:.1f}x), "
                     f"{processing_rate:.1f} MB/sec, "
                     f"strategy: {result.get('strategy', 'unknown')}")
        
        return result_msg
        
    except Exception as e:
        logger.exception(f"Conversion failed for '{table_name}': {e}")
        if final_output.exists():
            try:
                final_output.unlink()
            except:
                pass
        return f"[ERROR] Failed '{table_name}': {e}"


def convert_with_stream_merge_integration(
    csv_paths: List[Path],
    output_path: Path,
    expected_columns: List[str],
    delimiter: str,
    memory_monitor,
    config
) -> Dict:
    """Stream-merge with unified schema and memory-safe batching.
    Instead of repeatedly loading and appending to the final parquet file, write batch parts
    to a temp directory and do one final merge at the end. This avoids re-reading the growing
    output and keeps peak memory low.
    """
    
    schema_override = infer_unified_schema(csv_paths, delimiter)
    if not schema_override and expected_columns:
        schema_override = {f"column_{i+1}": pl.Utf8 for i in range(len(expected_columns))}
    
    status = memory_monitor.get_status_report()
    available_mb = max(100, status.get('budget_remaining_mb', 100))
    # dynamic chunk size: favor smaller chunks when memory is tight
    chunk_size = max(10000, min(50000, int(available_mb * 20)))
    
    logger.debug(f"Stream-merge chunk_size={chunk_size:,}")
    
    class ChunkIter:
        def __init__(self, path):
            self.path = path
            self.offset = 0
            self.done = False
        
        def read_next(self):
            if self.done:
                return None
            try:
                chunk = pl.read_csv(
                    str(self.path),
                    separator=delimiter,
                    schema_overrides=schema_override,
                    encoding="utf8-lossy",
                    ignore_errors=True,
                    skip_rows=self.offset,
                    n_rows=chunk_size,
                    has_header=False,
                    low_memory=True,
                    rechunk=False
                )
                
                if len(chunk) == 0:
                    self.done = True
                    return None
                
                if expected_columns and len(chunk.columns) == len(expected_columns):
                    chunk = chunk.rename({chunk.columns[i]: expected_columns[i] 
                                         for i in range(len(expected_columns))})
                
                self.offset += chunk_size
                if len(chunk) < chunk_size:
                    self.done = True
                
                return chunk
            except Exception as e:
                logger.warning(f"Error reading chunk from {self.path.name}: {e}")
                self.done = True
                return None
    
    iterators = [ChunkIter(p) for p in csv_paths]
    total_rows = 0
    
    # temp parts dir for stream batches
    temp_dir = output_path.parent / f".stream_parts_{output_path.stem}"
    temp_dir.mkdir(exist_ok=True)
    part_idx = 0
    batch_write_count = 0
    
    try:
        while any(not it.done for it in iterators):
            batches = []
            
            for it in iterators:
                if not it.done:
                    chunk = it.read_next()
                    if chunk is not None:
                        batches.append(chunk)
            
            if not batches:
                continue
            
            # If memory pressure is high, avoid concat and write each batch as a part immediately
            if memory_monitor.is_memory_pressure_high():
                logger.warning("High memory pressure detected before concat: writing batches as separate parts")
                for batch in batches:
                    part_file = temp_dir / f"stream_part_{part_idx:06d}.parquet"
                    batch.write_parquet(
                        str(part_file),
                        compression=config.compression if hasattr(config, 'compression') else "snappy",
                        row_group_size=50000,
                        statistics=False
                    )
                    logger.info(f"Wrote stream part {part_file.name}, rows={len(batch)}") 
                    total_rows += len(batch)
                    part_idx += 1
                    batch_write_count += 1
                    del batch
                    gc.collect()
                    if memory_monitor.is_memory_pressure_high():
                        memory_monitor.perform_aggressive_cleanup()
                continue  # go to next loop iteration
            
            # Normal path: attempt to concat batches but guard with pre-checks
            try:
                # Ensure schemas match (rename where necessary)
                base_cols = batches[0].columns
                for i, batch in enumerate(batches[1:], 1):
                    if batch.columns != base_cols:
                        if len(batch.columns) == len(base_cols):
                            batches[i] = batch.rename({batch.columns[j]: base_cols[j] for j in range(len(base_cols))})
                        else:
                            logger.warning("Batch schema differs in number of columns; will write separately")
                            # write offending batch separately to avoid concat explosion
                            part_file = temp_dir / f"stream_part_{part_idx:06d}.parquet"
                            batch.write_parquet(
                                str(part_file),
                                compression=config.compression if hasattr(config, 'compression') else "snappy",
                                row_group_size=50000,
                                statistics=False
                            )
                            logger.info(f"Wrote stream part {part_file.name} (schema-diff), rows={len(batch)}")
                            total_rows += len(batch)
                            part_idx += 1
                            batch_write_count += 1
                            batches[i] = None
                
                # filter out any None placeholders
                batches = [b for b in batches if b is not None]
                if not batches:
                    continue
                
                combined = pl.concat(batches, how="vertical_relaxed")
                rows = len(combined)
                total_rows += rows
                
                # write combined batch as a part file
                part_file = temp_dir / f"stream_part_{part_idx:06d}.parquet"
                combined.write_parquet(
                    str(part_file),
                    compression=config.compression if hasattr(config, 'compression') else "snappy",
                    row_group_size=50000,
                    statistics=False
                )
                logger.info(f"Wrote stream combined part {part_file.name}, rows={rows}")
                part_idx += 1
                batch_write_count += 1
                
                # Profiling memory
                try:
                    mem_used = memory_monitor.get_memory_usage_above_baseline()
                    logger.info(f"Memory after writing stream part: {mem_used}MB above baseline")
                except Exception:
                    pass
                
                del batches, combined
                gc.collect()
                if memory_monitor.is_memory_pressure_high():
                    memory_monitor.perform_aggressive_cleanup()
                
            except Exception as e:
                logger.exception(f"Failed to concat/write stream batches: {e}")
                # fallback: write each batch separately
                for batch in batches:
                    try:
                        part_file = temp_dir / f"stream_part_{part_idx:06d}.parquet"
                        batch.write_parquet(str(part_file),
                                           compression=config.compression if hasattr(config, 'compression') else "snappy",
                                           row_group_size=50000, statistics=False)
                        total_rows += len(batch)
                        part_idx += 1
                        batch_write_count += 1
                        del batch
                        gc.collect()
                    except Exception as e2:
                        logger.warning(f"Failed to write fallback stream part: {e2}")
                        continue
        
        # After loop: merge parts
        parts = sorted(temp_dir.glob("stream_part_*.parquet"))
        if not parts:
            # nothing written - create empty parquet?
            logger.warning("No stream parts were written; creating empty output file")
            empty_df = pl.DataFrame({c: [] for c in (expected_columns or [])})
            empty_df.write_parquet(str(output_path))
            return {"rows_processed": 0, "output_bytes": output_path.stat().st_size if output_path.exists() else 0, "strategy": "stream_merge"}
        
        if len(parts) == 1:
            # single part: move to output
            try:
                parts[0].rename(output_path)
            except Exception:
                # fallback to sink_parquet copy
                lazy = pl.scan_parquet(str(parts[0]))
                lazy.sink_parquet(str(output_path), compression=config.compression if hasattr(config, 'compression') else "snappy")
        else:
            lazy_frames = [pl.scan_parquet(str(p)) for p in parts]
            combined = pl.concat(lazy_frames, how="vertical_relaxed")
            combined.sink_parquet(
                str(output_path),
                compression=config.compression if hasattr(config, 'compression') else "snappy",
                row_group_size=50000,
                maintain_order=False,
                statistics=False
            )
            # cleanup parts
            for p in parts:
                try:
                    p.unlink()
                except:
                    pass
        return {"rows_processed": total_rows, "output_bytes": output_path.stat().st_size if output_path.exists() else 0, "strategy": "stream_merge"}
    finally:
        # Best-effort cleanup if temp dir still exists
        try:
            if temp_dir.exists():
                # remove any leftover files
                for p in temp_dir.glob("*"):
                    try:
                        p.unlink()
                    except:
                        pass
                try:
                    temp_dir.rmdir()
                except:
                    pass
        except Exception:
            pass


def infer_unified_schema(csv_paths: List[Path], delimiter: str, sample_size: int = 1000) -> Optional[Dict]:
    try:
        sample = pl.read_csv(
            str(csv_paths[0]),
            separator=delimiter,
            n_rows=sample_size,
            encoding="utf8-lossy",
            ignore_errors=True,
            has_header=False,
            infer_schema_length=0
        )
        
        num_cols = len(sample.columns)
        schema = {f"column_{i+1}": pl.Utf8 for i in range(num_cols)}
        
        del sample
        gc.collect()
        
        logger.debug(f"Created unified string schema with {num_cols} columns")
        return schema
    except Exception as e:
        logger.warning(f"Schema inference failed: {e}")
        try:
            del sample
        except:
            pass
        gc.collect()
        return None


