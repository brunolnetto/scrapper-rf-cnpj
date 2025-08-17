"""
Simplified test fixtures for the refactored file loader test suite.
Only includes working, non-complex fixtures to avoid hanging issues.
"""
import pytest
import tempfile
import os
import csv
from typing import List, Dict, Any


@pytest.fixture
def test_dsn():
    """Standard test database DSN - corrected to use test_db."""
    return 'postgresql://testuser:testpass@localhost:5433/test_db'


def create_csv_file(headers: List[str], rows: List[Dict[str, Any]]) -> str:
    """Helper function to create CSV file from headers and rows."""
    fd, filepath = tempfile.mkstemp(suffix='.csv', prefix='test_')
    os.close(fd)
    
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)
    
    return filepath


@pytest.fixture
def csv_file_factory():
    """Simple CSV file factory with automatic cleanup."""
    created_files = []
    
    def _create_csv(headers: List[str], rows: List[Dict[str, Any]]) -> str:
        file_path = create_csv_file(headers, rows)
        created_files.append(file_path)
        return file_path
    
    yield _create_csv
    
    # Cleanup
    for file_path in created_files:
        try:
            os.remove(file_path)
        except FileNotFoundError:
            pass


@pytest.fixture
def simple_csv_data():
    """Simple CSV test data."""
    return {
        'headers': ['id', 'val'],
        'rows': [
            {'id': '1', 'val': 'foo'},
            {'id': '2', 'val': 'bar'}
        ]
    }


@pytest.fixture
def temp_csv_file(simple_csv_data, csv_file_factory):
    """Pre-created CSV file with simple test data."""
    return csv_file_factory(simple_csv_data['headers'], simple_csv_data['rows'])

# NOTE: Complex async fixtures removed to prevent hanging issues
# Use manual database setup in tests that need database access
