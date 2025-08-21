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
CHUNK_SIZE = 10
SUB_BATCH_SIZE = 5

async def run_ingest_batch_auto_detect(file_paths: List[Path]):
    """Run ingestion for multiple files with auto-format detection."""
    if not file_paths:
        return True
        
    cmd = [
        "python3", "-m", "src.cli",
        "--dsn", DSN,
        # Note: No --file-type argument = auto-detection enabled
        "--table", TABLE,
        "--pk", ",".join(PK),
        "--headers", ",".join(HEADERS),
        "--batch-size", str(CHUNK_SIZE),
        "--sub-batch-size", str(SUB_BATCH_SIZE),
        "--concurrency", str(CONCURRENCY),
        "--max-retries", "3",
        *[str(path) for path in file_paths]  # files as positional arguments
    ]
    
    print(f"Running auto-detection ingestion for {len(file_paths)} mixed files:")
    for path in file_paths:
        print(f"  - {path.name}")
    
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    
    stdout, stderr = await proc.communicate()
    
    if proc.returncode == 0:
        print(f"✓ Successfully processed {len(file_paths)} files with auto-detection")
        if stdout:
            print("Output:", stdout.decode().strip())
        return True
    else:
        print(f"✗ Failed to process files (exit code: {proc.returncode})")
        if stderr:
            print("Error:", stderr.decode().strip())
        if stdout:
            print("Output:", stdout.decode().strip())
        return False

async def run_ingest_batch(file_paths: List[Path], file_type: str):
    """Run ingestion for multiple files concurrently using the enhanced CLI."""
    if not file_paths:
        return True
        
    cmd = [
        "python3", "-m", "src.cli",
        "--dsn", DSN,
        "--file-type", file_type,
        "--table", TABLE,
        "--pk", ",".join(PK),
        "--headers", ",".join(HEADERS),
        "--batch-size", str(CHUNK_SIZE),
        "--sub-batch-size", str(SUB_BATCH_SIZE),
        "--concurrency", str(CONCURRENCY),
        "--max-retries", "3",
        *[str(path) for path in file_paths]  # files as positional arguments
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
    """Process all files in DATA_DIR using concurrent batching with auto-detection."""
    if not DATA_DIR.exists():
        print(f"Error: Data directory {DATA_DIR} does not exist")
        return False
    
    # Collect all supported files
    all_files = []
    for file_path in DATA_DIR.iterdir():
        if file_path.is_file():
            ext = file_path.suffix.lower()
            if ext in ['.csv', '.parquet']:
                all_files.append(file_path)
    
    if not all_files:
        print(f"No CSV or Parquet files found in {DATA_DIR}")
        return False
    
    print(f"Found {len(all_files)} files for processing:")
    for f in all_files:
        print(f"  - {f.name}")
    
    # Option 1: Process all files together with auto-detection (NEW!)
    print(f"\n=== Processing All Files with Auto-Detection ===")
    auto_success = await run_ingest_batch_auto_detect(all_files)
    
    if auto_success:
        print(f"\n🎉 All files processed successfully with auto-detection!")
        return True
    else:
        print(f"\n❌ Auto-detection processing failed, falling back to type-specific processing...")
        
        # Option 2: Fallback to separate processing by type
        csv_files = [f for f in all_files if f.suffix.lower() == '.csv']
        parquet_files = [f for f in all_files if f.suffix.lower() == '.parquet']
        
        success = True
        
        # Process CSV files
        if csv_files:
            print(f"\n=== Processing CSV Files ===")
            success &= await run_ingest_batch(csv_files, "csv")
        
        # Process Parquet files
        if parquet_files:
            print(f"\n=== Processing Parquet Files ===")
            success &= await run_ingest_batch(parquet_files, "parquet")
        
        if success:
            print(f"\n🎉 All files processed successfully with fallback method!")
        else:
            print(f"\n❌ Some files failed to process")
            
        return success

if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
