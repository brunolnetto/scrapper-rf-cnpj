"""
FileHandler - Consolidated file operations.
Combines FileLoader + FileProcessor functionality with bug fixes.
"""
import os
from typing import Iterable, List, Tuple, Optional, Any, Dict, Set
from pathlib import Path
import asyncio
import concurrent.futures
import threading
import uuid
import time

from ....setup.logging import logger
from .ingestors import create_batch_generator
from ..memory.service import MemoryMonitor
from ....setup.config.loader import ConfigLoader

class AsyncBatchStream:
    """
    Returned by FileHandler.generate_batches().
    Is an async iterable (supports `async for`) and exposes .stop(timeout) to cancel producer.
    """
    def __init__(self, file_handler, run_id):
        self._fh = file_handler
        self._run_id = run_id

    def __aiter__(self):
        # return the async generator that yields items from the queue
        return self._iter_impl()

    async def _iter_impl(self):
        control = self._fh._producer_controls.get(self._run_id)
        if not control:
            raise RuntimeError("Stream control not found (already stopped or invalid run_id)")

        q = control["queue"]
        sentinel = control["sentinel"]

        try:
            while True:
                item = await q.get()
                # Propagate producer exception if any
                if isinstance(item, Exception):
                    raise item
                if item is sentinel:
                    break
                yield item
        finally:
            # best-effort: ensure producer is stopped and joined
            try:
                await self.stop(timeout=5.0)
            except Exception:
                # swallow — caller may already be in shutdown path
                pass

    async def stop(self, timeout: float = 5.0) -> bool:
        """Ask the background producer to stop; wait up to `timeout` seconds for join."""
        return await self._fh.stop(self._run_id, timeout=timeout)

class FileHandler:
    """
    Handles all file-related operations: detection, validation, and batch processing.
    """
    
    def __init__(self, config: ConfigLoader):
        self.config = config
        self.memory_monitor = MemoryMonitor(config.pipeline.memory)

        self._producer_controls = {}
        self._producer_controls_lock = threading.Lock()

        self._active_threads: Set[threading.Thread] = set()
        self._shutdown_requested = False

        # Register cleanup on process exit
        import atexit
        atexit.register(self._emergency_cleanup)
    
    def detect_format(self, file_path: str) -> str:
        """Memory-efficient format detection."""
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
        
        if not os.path.isfile(file_path):
            raise ValueError(f"Path is not a file: {file_path}")
        
        base_name = os.path.basename(file_path)
        
        # Handle compressed files
        if base_name.endswith('.gz'):
            inner_name = base_name[:-3]
            ext = os.path.splitext(inner_name)[1].lower()
            if ext in ['.csv', '.parquet']:
                raise ValueError(f"Compressed files not yet supported: {base_name}")
        else:
            ext = os.path.splitext(base_name)[1].lower()
        
        # Format detection with minimal memory usage
        detected_format = None
        
        if ext == '.parquet':
            if self._validate_parquet_content(file_path):
                detected_format = 'parquet'
            else:
                raise ValueError(f"File has .parquet extension but invalid content: {file_path}")
                
        elif ext == '.csv':
            if self._validate_csv_content(file_path):
                detected_format = 'csv'
            else:
                raise ValueError(f"File has .csv extension but invalid content: {file_path}")
                
        elif ext in ['.txt', '.tsv', '.dat']:
            if self._validate_csv_content(file_path):
                detected_format = 'csv'
                logger.warning(f"Treating {ext} file as CSV format: {base_name}")
            else:
                raise ValueError(f"File with extension {ext} does not appear to be valid CSV: {file_path}")
                
        else:
            # Content-based fallback detection
            detected_format = self._detect_format_by_content(file_path)
            if not detected_format:
                raise ValueError(f"Unsupported file format. Extension: {ext}, File: {file_path}")
        
        logger.info(f"[FileHandler] Detected format: {detected_format} for {base_name}")
        return detected_format
    
    def _validate_parquet_content(self, file_path: str) -> bool:
        """Memory-efficient Parquet validation."""
        try:
            # Check magic bytes first (most efficient)
            with open(file_path, 'rb') as f:
                header = f.read(4)
                if header == b'PAR1':
                    return True
            
            # Fallback to pyarrow validation
            import pyarrow.parquet as pq
            try:
                pf = pq.ParquetFile(file_path)
                schema = pf.schema
                del pf, schema  # Explicit cleanup
                return True
            except Exception:
                return False
                
        except Exception:
            return False
    
    def _validate_csv_content(self, file_path: str) -> bool:
        """Memory-efficient CSV validation."""
        try:
            encoding = getattr(self.config.pipeline.data_source, 'encoding', 'utf-8')
            with open(file_path, 'r', encoding=encoding, errors='ignore') as f:
                sample_lines = []
                for i, line in enumerate(f):
                    if i >= 2:  # Only check first 2 lines
                        break
                    sample_lines.append(line.strip())
                
                # Validate sample lines
                for line in sample_lines:
                    if line and not any(delim in line for delim in [',', ';', '\t', '|']):
                        return False
                
                del sample_lines  # Explicit cleanup
                return True
                
        except Exception:
            return False
    
    def _detect_format_by_content(self, file_path: str) -> Optional[str]:
        """Content-based format detection with memory efficiency."""
        try:
            # Try Parquet first (binary format)
            if self._validate_parquet_content(file_path):
                return 'parquet'
            
            # Try CSV
            if self._validate_csv_content(file_path):
                return 'csv'
            
            return None
        except Exception:
            return None
    
    def _emergency_cleanup(self):
        """Emergency cleanup for process shutdown."""
        if not self._active_threads:
            return
            
        logger.warning(f"[FileHandler] Emergency cleanup: {len(self._active_threads)} active threads")
        
        # Signal all threads to stop
        self._shutdown_requested = True
        for run_id, control in list(self._producer_controls.items()):
            control["stop_event"].set()
        
        # Wait briefly for graceful shutdown
        deadline = time.perf_counter() + 3.0
        remaining_threads = list(self._active_threads)
        
        for thread in remaining_threads:
            if time.perf_counter() >= deadline:
                break
            if thread.is_alive():
                thread.join(timeout=0.5)
            if not thread.is_alive():
                self._active_threads.discard(thread)
        
        # Force kill remaining threads (Python limitation: we can't actually kill threads)
        if self._active_threads:
            logger.error(f"[FileHandler] {len(self._active_threads)} threads still alive, forcing process exit")
            import os
            os._exit(1)  # Nuclear option
    
    
    async def generate_batches(self, file_path: str, headers: List[str], chunk_size: int = 20_000):
        """
        Async wrapper that runs the synchronous create_batch_generator inside a background non-daemon thread,
        streaming batches into an asyncio.Queue. Returns an AsyncBatchStream instance which is both an
        async iterable and has a .stop(timeout) coroutine method.
        """
        # decide file_format & adjusted_chunk_size using thread-safe calls
        file_format = await asyncio.to_thread(self.detect_format, file_path)

        adjusted_chunk_size = chunk_size
        if self.memory_monitor:
            available_memory = self.memory_monitor.get_available_memory_budget()
            if available_memory < 200:
                adjusted_chunk_size = min(chunk_size, 8_000)
                logger.warning(f"[FileHandler] Severely reduced chunk size to {adjusted_chunk_size}")
            elif available_memory < 500:
                adjusted_chunk_size = min(chunk_size, 15_000)
                logger.warning(f"[FileHandler] Reduced chunk size to {adjusted_chunk_size} due to memory constraints")

        logger.info(f"[FileHandler] Using chunk size: {adjusted_chunk_size} for {file_format} file")

        # Control structures per run
        run_id = uuid.uuid4().hex
        q: asyncio.Queue = asyncio.Queue(maxsize=8)
        sentinel = object()
        stop_event = threading.Event()
        control = {
            "queue": q,
            "sentinel": sentinel,
            "stop_event": stop_event,
            "thread": None,
        }
        # register control
        self._producer_controls[run_id] = control

        loop = asyncio.get_running_loop()

        def producer():
            """Enhanced producer with shutdown detection."""
            gen = None
            try:
                gen = create_batch_generator(
                    path=file_path,
                    headers=headers,
                    chunk_size=adjusted_chunk_size,
                    encoding=getattr(self.config.pipeline.data_source, 'encoding', 'utf-8'),
                    memory_monitor=self.memory_monitor,
                    file_format=file_format
                )
                import time

                batch_count = 0
                for batch in gen:
                    # Check for shutdown more frequently
                    if stop_event.is_set() or self._shutdown_requested:
                        logger.debug(f"[FileHandler] Producer stopping after {batch_count} batches")
                        break
                    
                    # Non-blocking queue put with timeout
                    try:
                        fut = asyncio.run_coroutine_threadsafe(
                            asyncio.wait_for(q.put(batch), timeout=2.0), 
                            loop
                        )
                        fut.result(timeout=3.0)  # Total timeout: 5s
                        batch_count += 1
                        
                        # Yield CPU every 10 batches to check stop_event
                        if batch_count % 10 == 0:
                            time.sleep(0.001)
                            
                    except (asyncio.TimeoutError, concurrent.futures.TimeoutError):
                        logger.warning(f"[FileHandler] Producer queue timeout, stopping")
                        break
                    except Exception as e:
                        logger.error(f"[FileHandler] Producer queue error: {e}")
                        asyncio.run_coroutine_threadsafe(q.put(e), loop)
                        break

                # Signal completion only if we finished normally
                if not (stop_event.is_set() or self._shutdown_requested):
                    try:
                        asyncio.run_coroutine_threadsafe(q.put(sentinel), loop).result(timeout=1.0)
                    except Exception:
                        pass  # Consumer may have already stopped

            except Exception as exc:
                logger.exception(f"[FileHandler] Producer thread failed")
                try:
                    asyncio.run_coroutine_threadsafe(q.put(exc), loop).result(timeout=1.0)
                except Exception:
                    pass
            finally:
                # Aggressive cleanup
                try:
                    if gen is not None and hasattr(gen, "close"):
                        gen.close()
                except Exception:
                    pass
                
                # Remove from active threads
                self._active_threads.discard(threading.current_thread())

        # Create and track thread
        t = threading.Thread(target=producer, daemon=True, name=f"FileHandler-{run_id[:8]}")
        self._active_threads.add(t)
        control["thread"] = t
        t.start()

        # start daemon thread so it won't block process exit if something goes wrong
        # with the join during shutdown. Producer threads are short-lived for each
        # stream and we rely on the stop() path to request a graceful stop.
        t = threading.Thread(target=producer, daemon=True)
        control["thread"] = t
        t.start()

        # Return the async stream object (caller will `async for` on it)
        return AsyncBatchStream(self, run_id)

    async def stop(self, run_id: str, timeout: float = 5.0) -> bool:
        """Enhanced stop with aggressive cleanup."""
        control = self._producer_controls.get(run_id)
        if not control:
            return True

        stop_event: threading.Event = control["stop_event"]
        t: threading.Thread = control["thread"]
        q: asyncio.Queue = control["queue"]

        # Signal stop immediately
        stop_event.set()
        
        # Drain queue aggressively to unblock producer
        drained_items = 0
        try:
            while not q.empty() and drained_items < 20:  # Limit to prevent infinite loop
                try:
                    await asyncio.wait_for(q.get(), timeout=0.01)
                    drained_items += 1
                except asyncio.TimeoutError:
                    break
        except Exception:
            pass

        # Fast polling join with shorter timeout
        deadline = time.perf_counter() + timeout
        poll_interval = 0.02  # Poll every 20ms
        
        while t.is_alive() and time.perf_counter() < deadline:
            await asyncio.sleep(poll_interval)
            # Increase polling frequency as we approach timeout
            if time.perf_counter() > deadline - 1.0:
                poll_interval = 0.01

        joined = not t.is_alive()
        
        if not joined:
            logger.warning(f"[FileHandler] Thread {t.name} did not join within {timeout}s")
            # Remove from tracking anyway to prevent accumulation
            self._active_threads.discard(t)
        
        # Final cleanup
        self._producer_controls.pop(run_id, None)
        
        # Drain remaining queue items
        try:
            while not q.empty():
                q.get_nowait()
        except Exception:
            pass
            
        return joined
    
    def estimate_memory_requirements(self, file_path: str, headers: List[str], chunk_size: int = 20_000) -> dict:
        """
        Estimate memory requirements with optional calibration sampling.
        """
        try:
            file_size = os.path.getsize(file_path)
            file_format = self.detect_format(file_path)
            
            # Try calibration-based estimation first
            calibrated_estimate = self._calibrate_memory_requirements(file_path, headers, min(1000, chunk_size // 20))
            
            if calibrated_estimate:
                return {
                    'file_size_mb': file_size // 1024 // 1024,
                    'estimated_memory_per_chunk_mb': calibrated_estimate['memory_per_chunk_mb'],
                    'total_estimated_memory_mb': calibrated_estimate['total_estimated_mb'],
                    'format': file_format,
                    'recommended_chunk_size': calibrated_estimate['recommended_chunk_size'],
                    'calibrated': True,
                    'avg_bytes_per_row': calibrated_estimate['avg_bytes_per_row']
                }
            
            # Fallback to heuristic estimation
            if file_format == 'parquet':
                estimated_memory_per_chunk = (chunk_size * len(headers) * 50) // 1024 // 1024
                total_estimated_memory = min(file_size * 3 // 1024 // 1024, estimated_memory_per_chunk * 2)
            else:  # CSV
                estimated_memory_per_chunk = (chunk_size * len(headers) * 30) // 1024 // 1024
                total_estimated_memory = min(file_size * 2 // 1024 // 1024, estimated_memory_per_chunk * 2)
            
            return {
                'file_size_mb': file_size // 1024 // 1024,
                'estimated_memory_per_chunk_mb': estimated_memory_per_chunk,
                'total_estimated_memory_mb': total_estimated_memory,
                'format': file_format,
                'recommended_chunk_size': chunk_size,
                'calibrated': False
            }
            
        except Exception as e:
            logger.warning(f"Could not estimate memory requirements: {e}")
            return {
                'file_size_mb': 0,
                'estimated_memory_per_chunk_mb': 100,
                'total_estimated_memory_mb': 200,
                'format': 'unknown',
                'recommended_chunk_size': 10_000,
                'calibrated': False,
                'error': str(e)
            }
    
    def _get_available_memory_mb(self) -> float:
        """Get currently available system memory in MB"""
        try:
            import psutil
            return psutil.virtual_memory().available / 1024 / 1024
        except Exception:
            return 4000.0  # Default to assuming 4GB available if check fails

    def _calibrate_memory_requirements(self, file_path: str, headers: List[str], sample_size: int = 1000) -> Optional[dict]:
        """
        Calibrate memory requirements by sampling actual data.
        FIX: Avoid circular dependency by using ingestors directly instead of self.generate_batches.
        OPTIMIZED: Benchmark-driven memory utilization (95% for COPY operations).
        """
        if not self.memory_monitor:
            return None
        
        try:
            import psutil
            
            # Get baseline memory
            process = psutil.Process()
            baseline_rss = process.memory_info().rss
            
            # Use ingestors directly to avoid circular dependency
            file_format = self.detect_format(file_path)
            encoding = getattr(self.config.pipeline.data_source, 'encoding', 'utf-8')
            
            # Sample directly from ingestors
            batch_generator = create_batch_generator(
                path=file_path,
                headers=headers,
                chunk_size=sample_size,
                encoding=encoding,
                memory_monitor=self.memory_monitor,
                file_format=file_format
            )
            
            sample_batch = None
            for batch in batch_generator:
                sample_batch = batch
                break
            
            if not sample_batch:
                return None
            
            # Measure memory after sampling
            after_rss = process.memory_info().rss
            memory_delta = after_rss - baseline_rss
            
            # Calculate bytes per row
            actual_rows = len(sample_batch)
            if actual_rows > 0:
                avg_bytes_per_row = max(100, memory_delta // actual_rows)
                
                # Get available memory budget
                available_budget_mb = self.memory_monitor.get_available_memory_budget()
                
                # Benchmark-optimized: Use configured memory utilization for COPY operations
                # COPY operations can safely handle higher memory utilization
                # Default 70% for 8GB systems, can be increased to 95% for 16GB+ systems
                memory_utilization = getattr(self.config.pipeline.loading, 'memory_utilization_pct', 70) / 100.0
                safe_budget_bytes = available_budget_mb * 1024 * 1024 * memory_utilization
                
                # Adaptive chunking based on column count (wide tables need smaller chunks)
                num_columns = len(headers)
                base_chunk_size = int(safe_budget_bytes // avg_bytes_per_row)
                
                # Benchmark shows: ESTABLECIMENTO (30 cols) = 17,833 r/s vs SOCIO (11 cols) = 243,603 r/s
                # Wide tables (20+ columns) need smaller chunks for optimal performance
                if num_columns > 20:  # Wide table (like ESTABELE)
                    chunk_size_factor = 0.5  # Reduce chunk size by 50%
                elif num_columns > 15:  # Medium-wide table
                    chunk_size_factor = 0.7  # Reduce chunk size by 30%
                else:  # Narrow table (like SOCIO)
                    chunk_size_factor = 1.0  # Full chunk size
                
                recommended_chunk_size = max(1000, int(base_chunk_size * chunk_size_factor))
                
                # Memory-constrained systems: Use smaller caps for safety
                # <1GB available: 10k narrow, 5k wide (benchmark "small chunks")
                # 8GB systems: 50k narrow, 30k wide
                # 16GB+ systems: 150k narrow, 50k wide (benchmark optimal)
                available_memory_mb = self._get_available_memory_mb()
                if available_memory_mb < 1000:  # Less than 1GB available
                    if num_columns <= 15:
                        recommended_chunk_size = min(recommended_chunk_size, 10_000)
                    else:
                        recommended_chunk_size = min(recommended_chunk_size, 5_000)
                elif available_memory_mb < 4000:  # Less than 4GB available (8GB system)
                    if num_columns <= 15:
                        recommended_chunk_size = min(recommended_chunk_size, 50_000)
                    else:
                        recommended_chunk_size = min(recommended_chunk_size, 30_000)
                else:  # 16GB+ systems
                    if num_columns <= 15:
                        recommended_chunk_size = min(recommended_chunk_size, 150_000)
                    else:
                        recommended_chunk_size = min(recommended_chunk_size, 50_000)
                
                # Estimate memory for different chunk sizes
                memory_per_chunk_mb = (recommended_chunk_size * avg_bytes_per_row) // 1024 // 1024
                
                # Estimate total file memory requirement
                file_size = os.path.getsize(file_path)
                estimated_total_rows = file_size // (avg_bytes_per_row // 2)
                total_estimated_mb = (estimated_total_rows * avg_bytes_per_row * 0.5) // 1024 // 1024
                
                logger.info(f"[FileHandler] Calibration for {os.path.basename(file_path)} - "
                           f"Columns: {num_columns}, "
                           f"Avg bytes/row: {avg_bytes_per_row}, "
                           f"Recommended chunk: {recommended_chunk_size:,}, "
                           f"Memory per chunk: {memory_per_chunk_mb}MB, "
                           f"Memory util: {int(memory_utilization*100)}%")
                
                return {
                    'avg_bytes_per_row': avg_bytes_per_row,
                    'recommended_chunk_size': recommended_chunk_size,
                    'memory_per_chunk_mb': memory_per_chunk_mb,
                    'total_estimated_mb': total_estimated_mb,
                    'sample_rows': actual_rows,
                    'memory_delta_mb': memory_delta // 1024 // 1024,
                    'num_columns': num_columns,
                    'chunk_size_factor': chunk_size_factor
                }
            
            return None
            
        except Exception as e:
            logger.warning(f"Memory calibration failed: {e}")
            return None

    def get_recommended_processing_params(self, file_path: str, headers: List[str]) -> dict:
        """
        Get recommended processing parameters based on file size and available memory.
        OPTIMIZED: Benchmark-driven defaults (100k chunks, serial COPY wins).
        """
        try:
            file_size = os.path.getsize(file_path)
            file_size_gb = file_size / (1024**3)
            num_columns = len(headers)
            
            # Get system memory status for adaptive chunking
            available_memory_mb = self._get_available_memory_mb()
            
            # BENCHMARK-DRIVEN ADAPTIVE CHUNKING based on available RAM:
            # Small chunks (10k):  <1GB available - matches benchmark "Small chunks + COPY + serial"
            # Medium chunks (50k): 1-4GB available - good for 8GB systems
            # Large chunks (100k+): 16GB+ systems - benchmark optimal (243,603 r/s)
            
            if available_memory_mb < 1000:  # Less than 1GB available - CRITICAL LOW MEMORY
                # Use benchmark "Small chunks" configuration
                if num_columns > 20:  # Wide table
                    base_chunk = 5_000
                elif num_columns > 15:  # Medium-wide
                    base_chunk = 7_500
                else:  # Narrow table
                    base_chunk = 10_000
            elif available_memory_mb < 4000:  # 1-4GB available - Medium memory (8GB systems)
                # Medium chunks for 8GB systems
                if num_columns > 20:
                    base_chunk = 25_000
                elif num_columns > 15:
                    base_chunk = 37_500
                else:
                    base_chunk = 50_000
            else:  # 16GB+ systems - BENCHMARK OPTIMAL
                # Large chunks (100k) + serial COPY achieved 243,603 r/s vs 31,430 r/s with small chunks
                if num_columns > 20:  # Wide table (like ESTABELE - 30 columns)
                    base_chunk = 50_000
                elif num_columns > 15:  # Medium-wide table
                    base_chunk = 75_000
                else:  # Narrow table (like SOCIO - 11 columns)
                    base_chunk = 150_000
            
            # Benchmark-optimized base recommendations
            # Serial COPY outperforms 2-4 workers (243,603 r/s vs 207,770 r/s with 2 workers)
            recommendations = {
                'chunk_size': base_chunk,
                'sub_batch_size': max(1_000, base_chunk // 20),  # Scale sub-batch with chunk size
                'enable_parallelism': False,  # Serial wins for single files
                'concurrency': 1,  # Benchmark shows serial is faster
                'use_streaming': True if file_size_gb > 1.0 else False
            }
            
            # Adjust based on file size (very large files need more conservative chunks)
            if file_size_gb > 5.0:  # Very large files (4GB+)
                # Reduce chunk size by 50% for memory safety
                recommendations['chunk_size'] = recommendations['chunk_size'] // 2
                recommendations['use_streaming'] = True
            elif file_size_gb < 0.1:  # Small files can use parallelism
                recommendations.update({
                    'enable_parallelism': True,
                    'concurrency': 2  # Limit to 2 workers max (benchmark shows diminishing returns)
                })
            
            # Further adjust based on current memory pressure (runtime check)
            if self.memory_monitor:
                available_memory = self.memory_monitor.get_available_memory_budget()
                pressure_level = self.memory_monitor.get_memory_pressure_level()
                
                if available_memory < 300 or pressure_level > 0.7:
                    # Severe memory constraints
                    recommendations.update({
                        'chunk_size': min(recommendations['chunk_size'], 8_000),
                        'sub_batch_size': min(recommendations['sub_batch_size'], 1_000),
                        'enable_parallelism': False,
                        'concurrency': 1
                    })
                elif available_memory < 800 or pressure_level > 0.4:
                    # Moderate memory constraints
                    recommendations.update({
                        'chunk_size': min(recommendations['chunk_size'], 15_000),
                        'sub_batch_size': min(recommendations['sub_batch_size'], 2_000),
                        'enable_parallelism': False
                    })
                
                recommendations['memory_info'] = {
                    'available_mb': available_memory,
                    'pressure_level': pressure_level,
                    'should_prevent': self.memory_monitor.should_prevent_processing()
                }
            
            recommendations['file_size_gb'] = file_size_gb
            logger.info(f"[FileHandler] Recommended params for {os.path.basename(file_path)}: "
                       f"chunk={recommendations['chunk_size']}, "
                       f"parallel={recommendations['enable_parallelism']}")
            
            return recommendations
            
        except Exception as e:
            logger.error(f"Failed to get processing recommendations: {e}")
            return {
                'chunk_size': 10_000,
                'sub_batch_size': 1_500,
                'enable_parallelism': False,
                'concurrency': 1,
                'use_streaming': True,
                'error': str(e)
            }

    def optimize_processing_order(self, file_paths: List[Path]) -> List[Path]:
        """Optimize processing order by file size (smallest first)."""
        try:
            file_sizes = []
            for file_path in file_paths:
                try:
                    size = file_path.stat().st_size
                    file_sizes.append((file_path, size))
                except Exception as e:
                    logger.warning(f"Could not get size for {file_path}: {e}")
                    file_sizes.append((file_path, 0))

            # Sort by size (smallest first)
            file_sizes.sort(key=lambda x: x[1])
            optimized_order = [file_path for file_path, _ in file_sizes]

            logger.info(f"[FileHandler] Optimized processing order: {[str(p.name) for p in optimized_order]}")
            return optimized_order

        except Exception as e:
            logger.error(f"Failed to optimize processing order: {e}")
            return file_paths

    async def shutdown_all(self, timeout: float = 10.0) -> bool:
        """Shutdown all active streams."""
        if not self._producer_controls:
            return True
            
        logger.info(f"[FileHandler] Shutting down {len(self._producer_controls)} active streams")
        
        # Stop all streams concurrently
        stop_tasks = []
        for run_id in list(self._producer_controls.keys()):
            stop_tasks.append(self.stop(run_id, timeout=timeout/len(self._producer_controls)))
        
        if stop_tasks:
            results = await asyncio.gather(*stop_tasks, return_exceptions=True)
            all_stopped = all(r is True for r in results if not isinstance(r, Exception))
        else:
            all_stopped = True
        
        # Emergency cleanup for any remaining threads
        if self._active_threads:
            logger.warning(f"[FileHandler] {len(self._active_threads)} threads still active after shutdown")
            self._emergency_cleanup()
        
        return all_stopped