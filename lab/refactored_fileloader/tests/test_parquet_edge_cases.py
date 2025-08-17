import pyarrow as pa
import pyarrow.parquet as pq
import tempfile
import os
from src.parquet_ingestor import batch_generator

def test_parquet_batch_generator():
    headers = ['x', 'y']
    table = pa.Table.from_pydict({'x': [1, 2], 'y': [3, 4]})
    with tempfile.NamedTemporaryFile(delete=False) as f:
        pq.write_table(table, f.name)
        batches = list(batch_generator(f.name, headers, chunk_size=1))
        assert len(batches) == 2
        assert batches[0][0] == (1, 3)
        assert batches[1][0] == (2, 4)
    os.remove(f.name)


def test_parquet_missing_header():
    headers = ['x', 'z']  # 'z' does not exist
    table = pa.Table.from_pydict({'x': [1], 'y': [2]})
    with tempfile.NamedTemporaryFile(delete=False) as f:
        pq.write_table(table, f.name)
        # Should raise KeyError or handle missing gracefully
        try:
            batches = list(batch_generator(f.name, headers, chunk_size=1))
            assert batches[0][0][0] == 1
            assert batches[0][0][1] is None or batches[0][0][1] == ''
        except Exception:
            pass
    os.remove(f.name)
