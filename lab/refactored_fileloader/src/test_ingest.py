import asyncio
from pathlib import Path
import subprocess

DATA_DIR = Path("data")
DSN = "postgresql://testuser:testpass@localhost:5433/testdb"
TABLE = "ingest_test_table"
PK = ["id"]
HEADERS = ["id","name","value"]

async def run_ingest(file_path: Path, file_type: str):
    cmd = [
        "python3", "-m", "src.cli",
        "--dsn", DSN,
        "--files", str(file_path),
        "--file-type", file_type,
        "--table", TABLE,
        "--pk", ",".join(PK),
        "--headers", ",".join(HEADERS)
    ]
    print(f"Running ingestion for {file_path}")
    proc = await asyncio.create_subprocess_exec(*cmd)
    await proc.wait()

async def main():
    for file_path in DATA_DIR.iterdir():
        if file_path.suffix.lower() == ".csv":
            await run_ingest(file_path, "csv")
        elif file_path.suffix.lower() == ".parquet":
            await run_ingest(file_path, "parquet")

if __name__ == "__main__":
    asyncio.run(main())
