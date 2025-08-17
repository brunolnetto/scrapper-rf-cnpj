import pyarrow as pa
import pyarrow.parquet as pq
import tempfile
import os
from src.ingestors import batch_generator_parquet

def test_parquet_batch_generator():
    headers = ['x', 'y']
    table = pa.Table.from_pydict({'x': [1, 2], 'y': [3, 4]})
    with tempfile.NamedTemporaryFile(delete=False) as f:
        pq.write_table(table, f.name)
        batches = list(batch_generator_parquet(f.name, headers, chunk_size=1))
        assert len(batches) == 2
        assert batches[0][0] == ('1', '3')  # Note: converted to string as expected
        assert batches[1][0] == ('2', '4')
    os.remove(f.name)
