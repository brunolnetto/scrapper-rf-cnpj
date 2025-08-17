#!/usr/bin/env python3
"""
Test file_loader utility
"""
import pytest
import tempfile
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import csv
from src.file_loader import FileLoader

def test_file_loader_csv():
    """Test FileLoader with CSV files."""
    # Create test CSV
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        writer = csv.writer(f)
        writer.writerow(['id', 'name', 'value'])
        writer.writerow(['1', 'Alice', '100'])
        writer.writerow(['2', 'Bob', '200'])
        csv_path = f.name
    
    try:
        loader = FileLoader(csv_path)
        assert loader.format == 'csv'
        
        df = loader.load()
        assert len(df) == 2
        assert list(df.columns) == ['id', 'name', 'value']
        assert df.iloc[0]['name'] == 'Alice'
    finally:
        import os
        os.unlink(csv_path)

def test_file_loader_parquet():
    """Test FileLoader with Parquet files."""
    # Create test Parquet
    df = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'value': [100, 200, 300]
    })
    
    with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as f:
        df.to_parquet(f.name, index=False)
        parquet_path = f.name
    
    try:
        loader = FileLoader(parquet_path)
        assert loader.format == 'parquet'
        
        loaded_df = loader.load()
        assert len(loaded_df) == 3
        assert list(loaded_df.columns) == ['id', 'name', 'value']
        assert loaded_df.iloc[1]['name'] == 'Bob'
    finally:
        import os
        os.unlink(parquet_path)

def test_file_loader_unsupported_format():
    """Test FileLoader with unsupported file format."""
    with pytest.raises(ValueError, match="Unsupported file extension"):
        FileLoader('test.txt')
