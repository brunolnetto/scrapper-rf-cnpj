import tempfile
import csv
from src.csv_ingestor import batch_generator
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
    # DictReader auto-detects delimiter only if sniffed, so we test fallback
    batches = list(batch_generator(path, headers, chunk_size=1))
    assert batches[0][0] == ('1', '2')
    assert batches[1][0] == ('3', '4')


def test_csv_tab_delimiter():
    headers = ['x', 'y']
    rows = [{'x': 'foo', 'y': 'bar'}]
    path = make_csv(headers, rows, delimiter='\t')
    batches = list(batch_generator(path, headers, chunk_size=1))
    assert batches[0][0] == ('foo', 'bar')


def test_csv_partial_header():
    headers = ['id', 'val']
    rows = [{'id': '1'}, {'id': '2', 'val': 'bar'}]
    path = make_csv(headers, rows)
    batches = list(batch_generator(path, headers, chunk_size=1))
    assert batches[0][0][0] == '1'
    assert batches[1][0][1] == 'bar'
    # Missing values should be None or empty string
    assert batches[0][0][1] in (None, '')


def test_csv_quote_all():
    headers = ['a', 'b']
    rows = [{'a': '"quoted"', 'b': 'plain'}]
    path = make_csv(headers, rows, quoting=csv.QUOTE_ALL)
    batches = list(batch_generator(path, headers, chunk_size=1))
    assert batches[0][0] == ('"quoted"', 'plain')
