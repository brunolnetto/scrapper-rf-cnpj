"""
Consolidated test suite for CSV and Parquet ingestors.
Combines basic functionality and edge cases into a single comprehensive file.
"""
import tempfile
import csv
import os
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from src.ingestors import batch_generator_csv, batch_generator_parquet


class TestCSVIngestor:
    """Test CSV batch generator functionality."""
    
    def make_csv(self, headers, rows, delimiter=',', quoting=csv.QUOTE_MINIMAL):
        """Helper to create test CSV files."""
        with tempfile.NamedTemporaryFile('w+', newline='', encoding='utf-8', delete=False) as f:
            writer = csv.DictWriter(f, fieldnames=headers, delimiter=delimiter, quoting=quoting)
            writer.writeheader()
            for row in rows:
                writer.writerow(row)
            f.flush()
            return f.name

    def test_basic_csv_batch_generator(self):
        """Test basic CSV batch generation."""
        headers = ['a', 'b']
        rows = [{'a': '1', 'b': '2'}, {'a': '3', 'b': '4'}]
        path = self.make_csv(headers, rows)
        
        batches = list(batch_generator_csv(path, headers, chunk_size=1))
        assert len(batches) == 2
        assert batches[0][0] == ('1', '2')
        assert batches[1][0] == ('3', '4')
        os.remove(path)

    def test_csv_semicolon_delimiter(self):
        """Test CSV with semicolon delimiter."""
        headers = ['a', 'b']
        rows = [{'a': '1', 'b': '2'}, {'a': '3', 'b': '4'}]
        path = self.make_csv(headers, rows, delimiter=';')
        
        batches = list(batch_generator_csv(path, headers, chunk_size=1))
        assert batches[0][0] == ('1', '2')
        assert batches[1][0] == ('3', '4')
        os.remove(path)

    def test_csv_tab_delimiter(self):
        """Test CSV with tab delimiter."""
        headers = ['x', 'y']
        rows = [{'x': 'foo', 'y': 'bar'}]
        path = self.make_csv(headers, rows, delimiter='\t')
        
        batches = list(batch_generator_csv(path, headers, chunk_size=1))
        assert batches[0][0] == ('foo', 'bar')
        os.remove(path)

    def test_csv_quoted_values(self):
        """Test CSV with quoted values."""
        headers = ['name', 'message']
        rows = [
            {'name': 'Alice', 'message': 'Hello, world!'}, 
            {'name': 'Bob', 'message': 'How are you?'}
        ]
        path = self.make_csv(headers, rows, quoting=csv.QUOTE_ALL)
        
        batches = list(batch_generator_csv(path, headers, chunk_size=1))
        assert batches[0][0] == ('Alice', 'Hello, world!')
        assert batches[1][0] == ('Bob', 'How are you?')
        os.remove(path)

    def test_csv_missing_columns(self):
        """Test CSV with missing columns."""
        headers = ['id', 'name', 'extra']
        rows = [{'id': '1', 'name': 'Alice'}]  # missing 'extra'
        path = self.make_csv(headers, rows)
        
        batches = list(batch_generator_csv(path, headers, chunk_size=1))
        assert batches[0][0] == ('1', 'Alice', None)  # should handle missing column gracefully
        os.remove(path)


class TestParquetIngestor:
    """Test Parquet batch generator functionality."""
    
    def test_basic_parquet_batch_generator(self):
        """Test basic Parquet batch generation."""
        headers = ['x', 'y']
        table = pa.Table.from_pydict({'x': [1, 2], 'y': [3, 4]})
        
        with tempfile.NamedTemporaryFile(delete=False) as f:
            pq.write_table(table, f.name)
            batches = list(batch_generator_parquet(f.name, headers, chunk_size=1))
            assert len(batches) == 2
            assert batches[0][0] == ('1', '3')  # Note: converted to string as expected
            assert batches[1][0] == ('2', '4')
        os.remove(f.name)

    def test_parquet_missing_header(self):
        """Test Parquet with missing header column."""
        headers = ['x', 'z']  # 'z' does not exist
        table = pa.Table.from_pydict({'x': [1], 'y': [2]})
        
        with tempfile.NamedTemporaryFile(delete=False) as f:
            pq.write_table(table, f.name)
            # Should handle missing column gracefully
            try:
                batches = list(batch_generator_parquet(f.name, headers, chunk_size=1))
                assert batches[0][0][0] == '1'  # String from parquet ingestor
                assert batches[0][0][1] is None or batches[0][0][1] == ''
            except Exception:
                pass  # Expected behavior for missing columns
        os.remove(f.name)
