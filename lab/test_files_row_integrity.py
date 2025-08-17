from pathlib import Path
import pyarrow.parquet as pq
import polars as pl
from os import getcwd, path

# Paths
parquet_file = Path(path.join(getcwd(), "data/CONVERTED_FILES/socios.parquet"))
csv_folder = Path(path.join(getcwd(), "data/EXTRACTED_FILES"))
csv_files = sorted(csv_folder.glob("*.SOCIOCSV"))

print(csv_files)

# Parquet row count
pf = pq.ParquetFile(parquet_file)
parquet_rows = pf.metadata.num_rows
print(f"Parquet rows: {parquet_rows}")

# CSV row counts (streaming with Polars)
total_row_count=0
for csv_path in csv_files:
    row_count = 0
    for batch in pl.read_csv(
        csv_path,
        separator=";",
        encoding="latin1",
        batch_size=50_000
    ).iter_slices():
        row_count += batch.height
    
    total_row_count+=row_count

print(f"CSV rows: {total_row_count}")
