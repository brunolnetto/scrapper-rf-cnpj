"""
enhanced_file_loader.py
Enhanced file loader with integrated memory monitoring and optimized generators.
"""
import os
from typing import Iterable, List, Tuple, Optional, Any
import asyncio
from concurrent.futures import ThreadPoolExecutor

from .ingestors import create_batch_generator
from ..memory.service import MemoryMonitor
from ....setup.logging import logger
from ....database.engine import Database

class FileLoader:
    """
    Memory-aware file loader with integrated monitoring and optimized processing.
    """
    
    def __init__(self, file_path: str, encoding: str = 'utf-8', memory_monitor: Optional[Any] = None):
        self.file_path = file_path
        self.encoding = encoding
        self.memory_monitor: MemoryMonitor = memory_monitor
        self.format = self._detect_format()
        
        # Log memory status if monitor available
        if self.memory_monitor:
            status = self.memory_monitor.get_status_report()
            logger.info(f"[MemoryAwareLoader] Initialized: "
                       f"Available memory budget: {status['budget_remaining_mb']:.1f}MB, "
                       f"Pressure level: {status['pressure_level']:.2f}")

    def _detect_format(self) -> str:
        """
        Memory-efficient format detection.
        """
        if not os.path.exists(self.file_path):
            raise FileNotFoundError(f"File not found: {self.file_path}")
        
        if not os.path.isfile(self.file_path):
            raise ValueError(f"Path is not a file: {self.file_path}")
        
        base_name = os.path.basename(self.file_path)
        
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
            if self._validate_parquet_content():
                detected_format = 'parquet'
            else:
                raise ValueError(f"File has .parquet extension but invalid content: {self.file_path}")
                
        elif ext == '.csv':
            if self._validate_csv_content():
                detected_format = 'csv'
            else:
                raise ValueError(f"File has .csv extension but invalid content: {self.file_path}")
                
        elif ext in ['.txt', '.tsv', '.dat']:
            if self._validate_csv_content():
                detected_format = 'csv'
                logger.warning(f"Treating {ext} file as CSV format: {base_name}")
            else:
                raise ValueError(f"File with extension {ext} does not appear to be valid CSV: {self.file_path}")
                
        else:
            # Content-based fallback detection
            detected_format = self._detect_format_by_content()
            if not detected_format:
                raise ValueError(f"Unsupported file format. Extension: {ext}, File: {self.file_path}")
        
        logger.info(f"[MemoryAwareLoader] Detected format: {detected_format} for {base_name}")
        return detected_format
    
    def _validate_parquet_content(self) -> bool:
        """Memory-efficient Parquet validation."""
        try:
            # Check magic bytes first (most efficient)
            with open(self.file_path, 'rb') as f:
                header = f.read(4)
                if header == b'PAR1':
                    return True
            
            # Fallback to pyarrow validation
            import pyarrow.parquet as pq
            try:
                pf = pq.ParquetFile(self.file_path)
                # Just check schema existence, don't read data
                schema = pf.schema
                del pf, schema  # Explicit cleanup
                return True
            except Exception:
                return False
                
        except Exception:
            return False
    
    def _validate_csv_content(self) -> bool:
        """Memory-efficient CSV validation."""
        try:
            # Read only a small sample for validation
            with open(self.file_path, 'r', encoding=self.encoding, errors='ignore') as f:
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
    
    def _detect_format_by_content(self) -> Optional[str]:
        """Content-based format detection with memory efficiency."""
        try:
            # Try Parquet first (binary format)
            if self._validate_parquet_content():
                return 'parquet'
            
            # Try CSV
            if self._validate_csv_content():
                return 'csv'
            
            return None
        except Exception:
            return None
    
    def get_format(self) -> str:
        """Return the detected file format."""
        return self.format
    
    def batch_generator(self, headers: List[str], chunk_size: int = 20_000) -> Iterable[List[Tuple]]:
        """
        Generate batches of data using memory-aware generators.
        
        Args:
            headers: List of expected column headers
            chunk_size: Number of rows per batch (will be adjusted based on memory)
            
        Yields:
            List of tuples representing rows in the batch
        """
        # Adjust chunk size based on memory constraints
        adjusted_chunk_size = chunk_size
        if self.memory_monitor:
            available_memory = self.memory_monitor.get_available_memory_budget()
            if available_memory < 500:  # Less than 500MB
                adjusted_chunk_size = min(chunk_size, 15_000)
                logger.warning(f"[MemoryAwareLoader] Reduced chunk size to {adjusted_chunk_size} due to memory constraints")
            elif available_memory < 200:  # Less than 200MB
                adjusted_chunk_size = min(chunk_size, 8_000)
                logger.warning(f"[MemoryAwareLoader] Severely reduced chunk size to {adjusted_chunk_size}")
        
        logger.info(f"[MemoryAwareLoader] Using chunk size: {adjusted_chunk_size} for {self.format} file")
        
        # Use memory-aware generator
        return create_batch_generator(
            path=self.file_path,
            headers=headers,
            chunk_size=adjusted_chunk_size,
            encoding=self.encoding,
            memory_monitor=self.memory_monitor,
            file_format=self.format
        )
    
    def estimate_memory_requirements(self, headers: List[str], chunk_size: int = 20_000) -> dict:
        """
        Estimate memory requirements for processing this file.
        
        Returns:
            Dictionary with memory estimates
        """
        try:
            file_size = os.path.getsize(self.file_path)
            
            # Rough estimates based on file format
            if self.format == 'parquet':
                # Parquet is typically compressed, estimate ~3x expansion in memory
                estimated_memory_per_chunk = (chunk_size * len(headers) * 50) // 1024 // 1024  # MB
                total_estimated_memory = min(file_size * 3 // 1024 // 1024, estimated_memory_per_chunk * 2)
            else:  # CSV
                # CSV in memory typically uses ~2x the file size
                estimated_memory_per_chunk = (chunk_size * len(headers) * 30) // 1024 // 1024  # MB
                total_estimated_memory = min(file_size * 2 // 1024 // 1024, estimated_memory_per_chunk * 2)
            
            return {
                'file_size_mb': file_size // 1024 // 1024,
                'estimated_memory_per_chunk_mb': estimated_memory_per_chunk,
                'total_estimated_memory_mb': total_estimated_memory,
                'format': self.format,
                'recommended_chunk_size': chunk_size
            }
            
        except Exception as e:
            logger.warning(f"Could not estimate memory requirements: {e}")
            return {
                'file_size_mb': 0,
                'estimated_memory_per_chunk_mb': 100,  # Safe default
                'total_estimated_memory_mb': 200,
                'format': self.format,
                'recommended_chunk_size': 10_000  # Conservative default
            }
    
    def get_recommended_processing_params(self) -> dict:
        """
        Get recommended processing parameters based on file size and available memory.
        
        Returns:
            Dictionary with recommended parameters
        """
        try:
            file_size = os.path.getsize(self.file_path)
            file_size_gb = file_size / (1024**3)
            
            # Base recommendations
            recommendations = {
                'chunk_size': 20_000,
                'sub_batch_size': 3_000,
                'enable_parallelism': False,
                'concurrency': 1,
                'use_streaming': True if file_size_gb > 1.0 else False
            }
            
            # Adjust based on file size
            if file_size_gb > 5.0:  # Very large files
                recommendations.update({
                    'chunk_size': 15_000,
                    'sub_batch_size': 2_000,
                    'enable_parallelism': False,
                    'use_streaming': True
                })
            elif file_size_gb > 1.0:  # Large files
                recommendations.update({
                    'chunk_size': 25_000,
                    'sub_batch_size': 3_500,
                    'enable_parallelism': False
                })
            elif file_size_gb < 0.1:  # Small files
                recommendations.update({
                    'chunk_size': 50_000,
                    'sub_batch_size': 8_000,
                    'enable_parallelism': True,
                    'concurrency': 2
                })
            
            # Adjust based on available memory
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
            logger.info(f"[MemoryAwareLoader] Recommended params for {os.path.basename(self.file_path)}: "
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
    
    def __repr__(self) -> str:
        memory_status = ""
        if self.memory_monitor:
            status = self.memory_monitor.get_status_report()
            memory_status = f", memory_budget={status['budget_remaining_mb']:.1f}MB"
        
        return (f"MemoryAwareFileLoader(file_path='{self.file_path}', "
                f"format='{self.format}', encoding='{self.encoding}'{memory_status})")

    def load_records_directly(
        self, table_info: Any, records: List[Tuple], database: Database
    ) -> Tuple[bool, Optional[str], int]:
        """
        Loads a batch of records directly into the target table using an async uploader.
        
        This method bypasses temporary disk files by streaming records into a temporary
        database table and then performing a bulk upsert.
        """
        if not records:
            return True, None, 0

        # Define an inner async function to perform the actual database operations.
        async def _async_execute():
            # This logic is adapted from the strategy's original helper methods.
            # It assumes the `database` object can provide an async connection pool.
            if not hasattr(database, 'get_async_pool'):
                raise NotImplementedError("The database object must have a 'get_async_pool' method.")
            
            # Assuming these are available relative to the UnifiedLoader's file location
            from .uploader import FileUploader
            from . import base
            import uuid
             
            async_pool = await database.get_async_pool()
            uploader = FileUploader(memory_monitor=None) # A monitor could be passed if available

            async with uploader.managed_connection(async_pool) as conn:
                tmp_table = f"tmp_direct_{os.getpid()}_{uuid.uuid4().hex[:8]}"
                headers = table_info.columns
                types_map = base.map_types(headers, getattr(table_info, 'types', {}))
                
                try:
                    await conn.execute(base.create_temp_table_sql(tmp_table, headers, types_map))
                    
                    async with conn.transaction():
                        await conn.copy_records_to_table(tmp_table, records=records, columns=headers)
                        primary_keys = getattr(table_info, 'primary_keys', [])
                        
                        if primary_keys:
                            sql = base.upsert_from_temp_sql(table_info.table_name, tmp_table, headers, primary_keys)
                            await conn.execute(sql)
                        else: # Handle tables without primary keys with a simple INSERT
                            insert_sql = f'INSERT INTO {base.quote_ident(table_info.table_name)} SELECT * FROM {base.quote_ident(tmp_table)}'
                            await conn.execute(insert_sql)
                    
                    return True, None, len(records)
                finally:
                    # Always ensure the temporary database table is dropped.
                    await conn.execute(f'DROP TABLE IF EXISTS {base.quote_ident(tmp_table)};')

        # This wrapper handles running the async code from a synchronous context.
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # If we're already in an async context, run in a separate thread
                # to avoid blocking the existing event loop.
                with ThreadPoolExecutor(max_workers=1) as executor:
                    future = executor.submit(lambda: asyncio.run(_async_execute()))
                    return future.result(timeout=600)  # 10-minute timeout
            else:
                return asyncio.run(_async_execute())
        except Exception as e:
            logger.error(f"Direct record loading failed for table '{table_info.table_name}': {e}", exc_info=True)
            return False, str(e), 0

# Factory function for easy integration
def create_file_loader(file_path: str, encoding: str = 'utf-8', memory_monitor=None):
    """
    Factory function to create appropriate file loader.
    
    Args:
        file_path: Path to the file
        encoding: File encoding for CSV files
        memory_monitor: Optional memory monitor instance
        
    Returns:
        MemoryAwareFileLoader instance
    """
    return FileLoader(file_path, encoding, memory_monitor)