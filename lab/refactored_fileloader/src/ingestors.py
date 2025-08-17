# ingestion/parquet_ingestor.py
import pyarrow.parquet as pq
import csv

from typing import Iterable, List, Tuple

def batch_generator_parquet(path: str, headers: List[str], chunk_size: int = 20_000) -> Iterable[List[Tuple]]:
    pf = pq.ParquetFile(path)
    for record_batch in pf.iter_batches(batch_size=chunk_size):
        name_to_idx = {record_batch.schema.field(i).name: i for i in range(record_batch.num_columns)}

        rows = []
        for i in range(record_batch.num_rows):
            row = []
            for h in headers:
                if h in name_to_idx:
                    val = record_batch.column(name_to_idx[h])[i].as_py()
                    # normalize: str or None
                    row.append(str(val) if val is not None else None)
                else:
                    row.append(None)
            rows.append(tuple(row))
        yield rows

def batch_generator_csv(path: str, headers: List[str], chunk_size: int = 20_000) -> Iterable[List[Tuple]]:
    with open(path, 'r', newline='', encoding='utf-8') as f:
        # Dialect detection
        sample = f.read(4096)
        f.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample)
        except Exception:
            dialect = csv.get_dialect('excel')
        has_header = csv.Sniffer().has_header(sample) if sample else False

        reader = csv.DictReader(f, dialect=dialect) if has_header else csv.reader(f, dialect=dialect)
        batch = []
        for row in reader:
            if has_header:
                batch.append(tuple(row.get(h, None) for h in headers))
            else:
                batch.append(tuple(row[i] if i < len(headers) else None for i in range(len(headers))))
            if len(batch) >= chunk_size:
                yield batch
                batch = []
        if batch:
            yield batch
