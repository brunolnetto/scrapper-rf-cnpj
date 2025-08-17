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
        
        # More robust header detection
        reader = csv.reader(f, dialect=dialect)
        first_row = next(reader, None)
        
        # Check if first row contains header-like strings (non-numeric, matches expected headers)
        skip_header = False
        if first_row:
            # If any expected header is found in first row, treat it as header
            if any(header in first_row for header in headers):
                skip_header = True
            # Or if first row looks like text headers (no numbers)
            elif all(not cell.isdigit() for cell in first_row if cell):
                skip_header = True
        
        # If we decided not to skip header, reset and include first row as data
        if not skip_header and first_row:
            f.seek(0)
            reader = csv.reader(f, dialect=dialect)
        
        batch = []
        for row in reader:
            # Convert to tuple, handle missing columns and empty values
            processed_row = []
            for i, header in enumerate(headers):
                if i < len(row):
                    value = row[i].strip() if row[i] else None
                    processed_row.append(value if value else None)  # Empty string -> None
                else:
                    processed_row.append(None)
            batch.append(tuple(processed_row))
            
            if len(batch) >= chunk_size:
                yield batch
                batch = []
        if batch:
            yield batch
