"""
file_loader.py
Enhanced unified loader for CSV and Parquet files for ETL pipeline.
Integrates with existing batch generator system and provides auto-detection.
Supports configurable encoding for CSV files.
"""
import os
from typing import Iterable, List, Tuple, Optional
from .ingestors import batch_generator_csv, batch_generator_parquet
from .....setup.logging import logger

class FileLoader:
    """
    Enhanced file loader that auto-detects format and provides unified interface
    to the existing batch generator system.
    """
    
    def __init__(self, file_path: str, encoding: str = 'utf-8'):
        self.file_path = file_path
        self.encoding = encoding  # Support configurable encoding
        self.format = self._detect_format()
        self._batch_generator = self._get_batch_generator()

    def _detect_format(self) -> str:
        """
        Robust auto-detection of file format with multiple validation layers:
        1. File existence check
        2. Extension-based detection
        3. Content-based validation (magic bytes)
        4. Fallback mechanisms
        """
        # Layer 1: File existence validation
        if not os.path.exists(self.file_path):
            raise FileNotFoundError(f"File not found: {self.file_path}")
        
        if not os.path.isfile(self.file_path):
            raise ValueError(f"Path is not a file: {self.file_path}")
        
        # Layer 2: Extension-based detection
        base_name = os.path.basename(self.file_path)
        
        # Handle compressed files
        if base_name.endswith('.gz'):
            # Remove .gz and check the underlying extension
            inner_name = base_name[:-3]
            ext = os.path.splitext(inner_name)[1].lower()
            if ext in ['.csv', '.parquet']:
                raise ValueError(f"Compressed files not yet supported: {base_name}")
        else:
            ext = os.path.splitext(base_name)[1].lower()
        
        # Layer 3: Content-based validation for common formats
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
            # Try to detect as CSV with different delimiters
            if self._validate_csv_content():
                detected_format = 'csv'
                logger.warning(f"Treating {ext} file as CSV format: {base_name}")
            else:
                raise ValueError(f"File with extension {ext} does not appear to be valid CSV: {self.file_path}")
                
        else:
            # Layer 4: Content-based fallback detection
            detected_format = self._detect_format_by_content()
            if not detected_format:
                raise ValueError(f"Unsupported file format. Extension: {ext}, File: {self.file_path}")
        
        return detected_format
    
    def _validate_parquet_content(self) -> bool:
        """Validate that file contains valid Parquet content using magic bytes."""
        try:
            # Parquet files start with 'PAR1' magic bytes
            with open(self.file_path, 'rb') as f:
                header = f.read(4)
                if header == b'PAR1':
                    return True
                
            # Alternative: Try to read with pyarrow (more thorough but slower)
            import pyarrow.parquet as pq
            try:
                pf = pq.ParquetFile(self.file_path)
                # Try to read schema to validate
                _ = pf.schema
                return True
            except Exception:
                return False
                
        except Exception:
            return False
    
    def _validate_csv_content(self) -> bool:
        """Validate that file contains valid CSV content."""
        try:
            # Use configurable encoding for CSV validation
            with open(self.file_path, 'r', encoding=self.encoding, errors='ignore') as f:
                # Try to read first few lines
                for i, line in enumerate(f):
                    if i >= 3:  # Check first 3 lines
                        break
                    if not line.strip():  # Empty line is OK
                        continue
                    # Very basic validation - should contain some delimiter
                    if not any(delim in line for delim in [',', ';', '\t', '|']):
                        return False
                return True
        except Exception:
            return False
    
    def _detect_format_by_content(self) -> Optional[str]:
        """Detect format by examining file content."""
        try:
            # Try Parquet first (binary format)
            if self._validate_parquet_content():
                return 'parquet'
            
            # Try CSV with current encoding
            if self._validate_csv_content():
                return 'csv'
            
            return None
        except Exception:
            return None
    
    def _get_batch_generator(self):
        """Get the appropriate batch generator based on detected format."""
        if self.format == 'parquet':
            return batch_generator_parquet
        elif self.format == 'csv':
            return batch_generator_csv
        else:
            raise ValueError(f"No batch generator available for format: {self.format}")
    
    def get_format(self) -> str:
        """Return the detected file format."""
        return self.format
    
    def batch_generator(self, headers: List[str], chunk_size: int = 20_000) -> Iterable[List[Tuple]]:
        """
        Generate batches of data from the file using the appropriate generator.
        
        Args:
            headers: List of expected column headers
            chunk_size: Number of rows per batch
            
        Yields:
            List of tuples representing rows in the batch
        """
        if self.format == 'csv':
            # Pass encoding to CSV batch generator
            yield from batch_generator_csv(self.file_path, headers, chunk_size, encoding=self.encoding)
        elif self.format == 'parquet':
            # Parquet doesn't need encoding
            yield from batch_generator_parquet(self.file_path, headers, chunk_size)
        else:
            raise ValueError(f"No batch generator available for format: {self.format}")
    
    def __repr__(self) -> str:
        return f"FileLoader(file_path='{self.file_path}', format='{self.format}', encoding='{self.encoding}')"
