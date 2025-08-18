"""
file_loader.py
Enhanced unified loader for CSV and Parquet files for ETL pipeline.
Integrates with existing batch generator system and provides auto-detection.
"""
import os
from typing import Iterable, List, Tuple, Callable
from .ingestors import batch_generator_csv, batch_generator_parquet

class FileLoader:
    """
    Enhanced file loader that auto-detects format and provides unified interface
    to the existing batch generator system.
    """
    
    def __init__(self, file_path: str):
        self.file_path = file_path
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
                print(f"Warning: Treating {ext} file as CSV format: {base_name}")
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
        """Validate that file contains valid CSV-like content."""
        try:
            # Check if file is readable and contains valid text
            with open(self.file_path, 'r', encoding='utf-8', errors='ignore') as f:
                # Read first few lines to validate structure
                sample_lines = []
                for i, line in enumerate(f):
                    if i >= 5:  # Check first 5 lines
                        break
                    sample_lines.append(line.strip())
            
            if not sample_lines:
                return False  # Empty file
            
            # Basic CSV validation: check for consistent delimiter patterns
            import csv
            sample_text = '\n'.join(sample_lines)
            
            # Try different common delimiters
            for delimiter in [',', ';', '\t', '|']:
                try:
                    sample_io = iter(sample_lines)
                    reader = csv.reader(sample_io, delimiter=delimiter)
                    rows = list(reader)
                    
                    if len(rows) >= 1:
                        # Check if rows have consistent column counts
                        if len(rows) == 1:
                            return True  # Single row is valid
                        
                        first_row_cols = len(rows[0])
                        if first_row_cols > 0:  # Has at least one column
                            # Allow some variance in column count (for missing trailing values)
                            consistent_rows = sum(1 for row in rows if abs(len(row) - first_row_cols) <= 1)
                            if consistent_rows >= len(rows) * 0.8:  # 80% of rows consistent
                                return True
                except:
                    continue
            
            return False
            
        except Exception:
            return False
    
    def _detect_format_by_content(self) -> str:
        """
        Fallback content-based format detection for files without clear extensions.
        """
        try:
            # Try Parquet first (more specific magic bytes)
            if self._validate_parquet_content():
                print(f"Warning: Detected Parquet format by content analysis: {os.path.basename(self.file_path)}")
                return 'parquet'
            
            # Try CSV (more permissive)
            if self._validate_csv_content():
                print(f"Warning: Detected CSV format by content analysis: {os.path.basename(self.file_path)}")
                return 'csv'
            
            return None
            
        except Exception:
            return None

    def _get_batch_generator(self) -> Callable:
        """Get the appropriate batch generator function."""
        if self.format == 'parquet':
            return batch_generator_parquet
        elif self.format == 'csv':
            return batch_generator_csv
        else:
            raise ValueError(f"Unknown format: {self.format}")

    def batch_generator(self, headers: List[str], chunk_size: int = 20_000) -> Iterable[List[Tuple]]:
        """
        Generate batches using the appropriate ingestor.
        This integrates with the existing ETL pipeline.
        """
        return self._batch_generator(self.file_path, headers, chunk_size)

    def get_format(self) -> str:
        """Get the detected file format."""
        return self.format

    @staticmethod
    def detect_file_format(file_path: str) -> str:
        """
        Static method to detect file format with robust validation.
        Creates a temporary FileLoader instance for detection.
        """
        try:
            # Use the robust detection by creating a temporary instance
            temp_loader = FileLoader.__new__(FileLoader)  # Create without calling __init__
            temp_loader.file_path = file_path
            return temp_loader._detect_format()
        except Exception as e:
            # Provide more informative error messages
            raise ValueError(f"Format detection failed for {file_path}: {str(e)}")

    @staticmethod
    def get_batch_generator_for_file(file_path: str) -> Callable:
        """
        Static method to get batch generator function for a file with robust detection.
        """
        format_type = FileLoader.detect_file_format(file_path)
        if format_type == 'parquet':
            return batch_generator_parquet
        elif format_type == 'csv':
            return batch_generator_csv
        else:
            raise ValueError(f"No batch generator available for format: {format_type}")
    
    @staticmethod
    def quick_detect_file_format(file_path: str) -> str:
        """
        Quick detection based only on file extension (legacy method).
        Use detect_file_format() for robust detection with content validation.
        """
        ext = os.path.splitext(file_path)[1].lower()
        if ext == '.parquet':
            return 'parquet'
        elif ext == '.csv':
            return 'csv'
        else:
            raise ValueError(f"Unsupported file extension: {ext}")

# Enhanced usage examples:
# 
# 1. Full FileLoader usage (recommended for new code):
# loader = FileLoader('/path/to/file.parquet')
# for batch in loader.batch_generator(headers=['id', 'name'], chunk_size=1000):
#     process_batch(batch)
#
# 2. Static method usage (for integration with existing CLI):
# batch_gen = FileLoader.get_batch_generator_for_file('/path/to/file.csv')
# for batch in batch_gen('/path/to/file.csv', headers, chunk_size):
#     process_batch(batch)
#
# 3. Format detection only:
# format_type = FileLoader.detect_file_format('/path/to/file.parquet')  # Returns 'parquet'
