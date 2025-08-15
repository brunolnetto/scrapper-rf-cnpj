#!/usr/bin/env python3
import sys
import pyarrow.parquet as pq
import pyarrow.dataset as ds

def head_parquet(path: str, n: int = 5):
    # --- Show first n rows without loading all ---
    print(f"=== First {n} rows from {path} ===")
    table = ds.dataset(path, format="parquet").head(n)
    print(table.to_pandas())

    # --- Metadata ---
    print("\n=== Parquet Metadata ===")
    parquet_file = pq.ParquetFile(path)
    meta = parquet_file.metadata
    schema = meta.schema

    print(f"Number of row groups: {meta.num_row_groups}")
    print(f"Total rows: {meta.num_rows}")
    print(f"Created by: {meta.created_by}")

    print("\n--- Column Metadata (from schema) ---")
    for i in range(len(schema)):
        field = schema.column(i)
        print(f"Column {i}: {field.name}, logical_type={field.logical_type}")

    print("\n--- Encoding per Row Group ---")
    for rg in range(meta.num_row_groups):
        rg_meta = meta.row_group(rg)
        print(f"Row Group {rg}: {rg_meta.num_rows} rows")
        for i in range(rg_meta.num_columns):
            col_meta = rg_meta.column(i)
            print(f"  - {col_meta.path_in_schema}: "
                  f"physical_type={col_meta.physical_type}, "
                  f"encodings={col_meta.encodings}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <parquet_path> [rows]")
        sys.exit(1)

    path = sys.argv[1]
    n = int(sys.argv[2]) if len(sys.argv) > 2 else 5
    head_parquet(path, n)
