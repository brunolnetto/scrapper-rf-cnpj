#!/usr/bin/env python3
import asyncio
from pathlib import Path
import time
import itertools
import subprocess

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)
DSN = "postgresql://testuser:testpass@localhost:5433/testdb"
TABLE = "ingest_test_table"
PK = ["id"]
HEADERS = ["id", "name", "value"]

# Define the parameters to sweep
CHUNK_SIZES = [1_000, 2_000, 3_000, 4_000]
SUB_BATCH_SIZES = [100, 200, 400, 800]

FILE_TO_TEST = DATA_DIR / "sample_1.csv"
GENERATOR_SCRIPT = Path("tools/generate_test_files.py")


def generate_mega_file():
    """Generate a 1_000_000-row CSV using your existing CLI."""
    if FILE_TO_TEST.exists():
        print(f"Mega file already exists: {FILE_TO_TEST}")
        return

    print(f"Generating mega file with 1_000_000 rows at {FILE_TO_TEST}")
    cmd = [
        "python3", str(GENERATOR_SCRIPT),
        "--output-dir", str(DATA_DIR),
        "--num-rows", "1000000",
        "--num-files", "1",
        "--intersection-ratio", "0.0",
        "--seed", "42"
    ]
    subprocess.run(cmd, check=True)
    print("Mega file generation done.")


async def run_ingest(file_path: Path, chunk_size: int, sub_batch_size: int):
    cmd = [
        "python3", "-m", "src.cli",
        "--dsn", DSN,
        "--file-type", "csv",
        "--table", TABLE,
        "--pk", ",".join(PK),
        "--headers", ",".join(HEADERS),
        "--batch-size", str(chunk_size),
        "--sub-batch-size", str(sub_batch_size),
        str(file_path)  # file as positional argument
    ]
    print(f"\nRunning ingestion: chunk={chunk_size}, sub_batch={sub_batch_size}")
    start = time.time()
    proc = await asyncio.create_subprocess_exec(*cmd)
    await proc.wait()
    elapsed = time.time() - start
    print(f"  Done in {elapsed:.2f}s")
    return (chunk_size, sub_batch_size, elapsed)


async def main():
    generate_mega_file()  # Ensure the mega file exists first

    results = []
    for chunk_size, sub_batch_size in itertools.product(CHUNK_SIZES, SUB_BATCH_SIZES):
        result = await run_ingest(FILE_TO_TEST, chunk_size, sub_batch_size)
        results.append(result)

    # Print summary
    print("\n=== Ingestion Benchmark Results ===")
    for chunk, batch, t in results:
        print(f"chunk={chunk:>6}, sub_batch={batch:>6} => {t:.2f}s")


if __name__ == "__main__":
    asyncio.run(main())
