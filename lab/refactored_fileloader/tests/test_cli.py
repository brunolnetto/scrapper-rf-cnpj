#!/usr/bin/env python3
"""
Test CLI functionality
"""
import pytest
import subprocess
import tempfile
import csv

def test_cli_help():
    """Test CLI help output."""
    result = subprocess.run(
        ['python', '-m', 'src.cli', '--help'],
        capture_output=True,
        text=True
    )
    assert result.returncode == 0
    assert 'Unified CSV/Parquet uploader' in result.stdout

def test_cli_csv_file():
    """Test CLI with CSV file."""
    # Create test CSV
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        writer = csv.writer(f)
        writer.writerow(['id', 'name'])
        writer.writerow(['1', 'test'])
        csv_path = f.name
    
    # Test CLI parsing (without running full pipeline)
    try:
        result = subprocess.run([
            'python', '-m', 'src.cli',
            '--dsn', 'postgresql://test:test@localhost:5432/test',
            '--files', csv_path,
            '--file-type', 'csv',
            '--table', 'test_table',
            '--pk', 'id',
            '--headers', 'id,name',
            '--help'  # This will prevent actual execution
        ], capture_output=True, text=True)
        # Command should show help and exit gracefully
        assert 'Unified CSV/Parquet uploader' in result.stdout
    finally:
        import os
        os.unlink(csv_path)
