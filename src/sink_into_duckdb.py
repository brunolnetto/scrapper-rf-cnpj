import duckdb
from pathlib import Path
import polars as pl

# Path where your individual .parquet files are
parquet_folder = Path("../data/PARQUET_FILES")

# Output DuckDB file
db_path = Path("../data/dadosrfb_202505.duckdb")
if db_path.exists():
    db_path.unlink()  # optional: delete old DB if rebuilding

# Connect to DuckDB
con = duckdb.connect(str(db_path))

# Iterate through parquet files and insert them as tables
for parquet_file in parquet_folder.glob("*.parquet"):
    table_name = parquet_file.stem  # e.g. "empresa"

    print(f"Inserting table: {table_name}")
    df = pl.read_parquet(parquet_file)  # lazy reading isn't needed here

    con.register("tmp", df.to_arrow())  # register Polars DF as Arrow
    con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM tmp")
    con.unregister("tmp")

con.close()
print(f"✅ All tables written to {db_path}")
