"""
file_loader.py
Unified loader for CSV and Parquet files for ETL pipeline.
"""
import os
import pandas as pd

class FileLoader:
    """
    Loads data from CSV or Parquet files, auto-detecting format.
    """
    def __init__(self, file_path):
        self.file_path = file_path
        self.format = self._detect_format()

    def _detect_format(self):
        ext = os.path.splitext(self.file_path)[1].lower()
        if ext == '.parquet':
            return 'parquet'
        elif ext == '.csv':
            return 'csv'
        else:
            raise ValueError(f"Unsupported file extension: {ext}")

    def load(self):
        if self.format == 'parquet':
            return pd.read_parquet(self.file_path)
        elif self.format == 'csv':
            return pd.read_csv(self.file_path)
        else:
            raise ValueError(f"Unknown format: {self.format}")

# Example usage:
# loader = FileLoader('/path/to/file.parquet')
# df = loader.load()
