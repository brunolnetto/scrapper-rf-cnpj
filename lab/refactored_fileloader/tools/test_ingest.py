import asyncio
from pathlib import Path
import subprocess
import sys
from typing import List

DATA_DIR = Path("data")
DSN = "postgresql://testuser:testpass@localhost:5433/testdb"
TABLE = "ingest_test_table"
PK = ["id"]
HEADERS = ["id","name","value"]
CONCURRENCY = 3  # Process up to 3 files concurrently
CHUNK_SIZE = 50000
SUB_BATCH_SIZE = 5000

async def run_ingest_batch(file_paths: List[Path], file_type: str):
    """Run ingestion for multiple files concurrently using the enhanced CLI."""
    if not file_paths:
        return True
        
    cmd = [
        "python3", "-m", "src.cli",
        "--dsn", DSN,
        "--files", *[str(path) for path in file_paths],
        "--file-type", file_type,
        "--table", TABLE,
        "--pk", ",".join(PK),
        "--headers", ",".join(HEADERS),
        "--chunk-size", str(CHUNK_SIZE),
        "--sub-batch-size", str(SUB_BATCH_SIZE),
        "--concurrency", str(CONCURRENCY),
        "--max-retries", "3"
    ]
    
    print(f"Running concurrent ingestion for {len(file_paths)} {file_type} files:")
    for path in file_paths:
        print(f"  - {path.name}")
    
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    
    stdout, stderr = await proc.communicate()
    
    if proc.returncode == 0:
        print(f"✓ Successfully processed {len(file_paths)} {file_type} files")
        if stdout:
            print("Output:", stdout.decode().strip())
        return True
    else:
        print(f"✗ Failed to process {file_type} files (exit code: {proc.returncode})")
        if stderr:
            print("Error:", stderr.decode().strip())
        if stdout:
            print("Output:", stdout.decode().strip())
        return False

async def main():
    """Process all files in DATA_DIR using concurrent batching by file type."""
    if not DATA_DIR.exists():
        print(f"Error: Data directory {DATA_DIR} does not exist")
        return False
    
    # Group files by type
    csv_files = []
    parquet_files = []
    
    for file_path in DATA_DIR.iterdir():
        if file_path.is_file():
            if file_path.suffix.lower() == ".csv":
                csv_files.append(file_path)
            elif file_path.suffix.lower() == ".parquet":
                parquet_files.append(file_path)
    
    if not csv_files and not parquet_files:
        print(f"No CSV or Parquet files found in {DATA_DIR}")
        return False
    
    print(f"Found {len(csv_files)} CSV files and {len(parquet_files)} Parquet files")
    
    success = True
    
    # Process CSV files concurrently
    if csv_files:
        print(f"\n=== Processing CSV Files ===")
        success &= await run_ingest_batch(csv_files, "csv")
    
    # Process Parquet files concurrently
    if parquet_files:
        print(f"\n=== Processing Parquet Files ===")
        success &= await run_ingest_batch(parquet_files, "parquet")
    
    if success:
        print(f"\n🎉 All files processed successfully!")
    else:
        print(f"\n❌ Some files failed to process")
        
    return success

if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
