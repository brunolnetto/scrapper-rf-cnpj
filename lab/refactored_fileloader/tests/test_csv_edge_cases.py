import tempfile
import csv
from src.ingestors import batch_generator_csv
import pytest

def make_csv(headers, rows, delimiter=',', quoting=csv.QUOTE_MINIMAL):
    with tempfile.NamedTemporaryFile('w+', newline='', encoding='utf-8', delete=False) as f:
        writer = csv.DictWriter(f, fieldnames=headers, delimiter=delimiter, quoting=quoting)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
        f.flush()
        return f.name

def test_csv_semicolon_delimiter():
    headers = ['a', 'b']
    rows = [{'a': '1', 'b': '2'}, {'a': '3', 'b': '4'}]
    path = make_csv(headers, rows, delimiter=';')
    batches = list(batch_generator_csv(path, headers, chunk_size=1))
    assert batches[0][0] == ('1', '2')
    assert batches[1][0] == ('3', '4')

def test_csv_tab_delimiter():
    headers = ['x', 'y']
    rows = [{'x': 'foo', 'y': 'bar'}]
    path = make_csv(headers, rows, delimiter='\t')
    batches = list(batch_generator_csv(path, headers, chunk_size=1))
    assert batches[0][0] == ('foo', 'bar')

def test_csv_quoted_values():
    headers = ['name', 'message']
    rows = [{'name': 'Alice', 'message': 'Hello, world!'}, {'name': 'Bob', 'message': 'How are you?'}]
    path = make_csv(headers, rows, quoting=csv.QUOTE_ALL)
    batches = list(batch_generator_csv(path, headers, chunk_size=1))
    assert batches[0][0] == ('Alice', 'Hello, world!')
    assert batches[1][0] == ('Bob', 'How are you?')

def test_csv_missing_columns():
    headers = ['id', 'name', 'extra']
    rows = [{'id': '1', 'name': 'Alice'}]
    path = make_csv(headers, rows)
    batches = list(batch_generator_csv(path, headers, chunk_size=1))
    assert batches[0][0] == ('1', 'Alice', None)
