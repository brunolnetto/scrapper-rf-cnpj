import tempfile
import csv
from src.csv_ingestor import batch_generator

def test_csv_batch_generator():
    headers = ['a', 'b']
    rows = [{'a': '1', 'b': '2'}, {'a': '3', 'b': '4'}]
    with tempfile.NamedTemporaryFile('w+', newline='', encoding='utf-8', delete=False) as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
        f.flush()
        batches = list(batch_generator(f.name, headers, chunk_size=1))
        assert len(batches) == 2
        assert batches[0][0] == ('1', '2')
        assert batches[1][0] == ('3', '4')
