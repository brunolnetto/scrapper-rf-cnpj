#!/usr/bin/env python3
"""
Simple test script to ingest socios.parquet file using the CLI.
This is a basic functional test before running the full performance benchmark.
"""

import asyncio
import time
import subprocess
import sys
from pathlib import Path

# Configuration
DATA_DIR = Path("data")
SOCIOS_FILE = DATA_DIR / "socios.parquet"
DSN = "postgresql://testuser:testpass@localhost:5433/testdb"
TABLE = "socios"

# Socios table headers and primary key
HEADERS = "cnpj_basico,identificador_socio,nome_socio_razao_social,cpf_cnpj_socio,qualificacao_socio,data_entrada_sociedade,pais,representante_legal,nome_do_representante,qualificacao_representante_legal,faixa_etaria"
PRIMARY_KEY = "cnpj_basico,identificador_socio"

# Test with moderate settings first
BATCH_SIZE = 50_000
SUB_BATCH_SIZE = 5_000
INTERNAL_CONCURRENCY = 1

async def test_socios_ingestion():
    """Test basic socios.parquet ingestion."""
    
    # Check if file exists
    if not SOCIOS_FILE.exists():
        print(f"❌ Error: File not found: {SOCIOS_FILE}")
        return False
    
    # Get file info
    file_size_mb = SOCIOS_FILE.stat().st_size / (1024 * 1024)
    print(f"📁 File: {SOCIOS_FILE}")
    print(f"📏 Size: {file_size_mb:.1f} MB")
    
    # Try to get row count
    try:
        import pyarrow.parquet as pq
        pf = pq.ParquetFile(SOCIOS_FILE)
        row_count = pf.metadata.num_rows
        print(f"📊 Rows: {row_count:,}")
    except ImportError:
        print("📊 Rows: Unknown (pyarrow not available)")
        row_count = None
    
    # Prepare command
    cmd = [
        "python3", "-m", "src.cli",
        "--dsn", DSN,
        "--file-type", "parquet",
        "--table", TABLE,
        "--pk", PRIMARY_KEY,
        "--headers", HEADERS,
        "--batch-size", str(BATCH_SIZE),
        "--sub-batch-size", str(SUB_BATCH_SIZE),
        "--internal-concurrency", str(INTERNAL_CONCURRENCY),
        "--max-retries", "3",
        str(SOCIOS_FILE)
    ]
    
    print(f"\n🔄 Starting ingestion test...")
    print(f"   Batch size: {BATCH_SIZE:,}")
    print(f"   Sub-batch size: {SUB_BATCH_SIZE:,}")
    print(f"   Internal concurrency: {INTERNAL_CONCURRENCY}")
    print(f"   Primary key: {PRIMARY_KEY}")
    print(f"   Headers: {len(HEADERS.split(','))} columns")
    
    start_time = time.time()
    
    try:
        # Run the CLI command
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        stdout, stderr = await proc.communicate()
        elapsed_time = time.time() - start_time
        
        if proc.returncode == 0:
            print(f"✅ SUCCESS! Completed in {elapsed_time:.1f} seconds")
            
            # Calculate performance metrics
            if row_count:
                rows_per_sec = row_count / elapsed_time
                print(f"📈 Performance: {rows_per_sec:.0f} rows/second")
            
            mb_per_sec = file_size_mb / elapsed_time
            print(f"📈 Data rate: {mb_per_sec:.1f} MB/second")
            
            # Show CLI output
            output_lines = stdout.decode().split('\n')
            print(f"\n📋 CLI Output:")
            for line in output_lines:
                if line.strip() and ('Completed' in line or 'Processing' in line or 'rows' in line):
                    print(f"   {line.strip()}")
            
            return True
            
        else:
            print(f"❌ FAILED! Exit code: {proc.returncode}")
            print(f"⏱️  Time elapsed: {elapsed_time:.1f} seconds")
            
            if stderr:
                error_msg = stderr.decode().strip()
                print(f"🚨 Error output:")
                print(f"   {error_msg}")
            
            if stdout:
                output_msg = stdout.decode().strip()
                print(f"📋 Standard output:")
                print(f"   {output_msg}")
            
            return False
            
    except Exception as e:
        elapsed_time = time.time() - start_time
        print(f"❌ EXCEPTION after {elapsed_time:.1f} seconds: {e}")
        return False

async def main():
    """Main execution."""
    print("🚀 SOCIOS PARQUET INGESTION TEST")
    print("=" * 50)
    
    success = await test_socios_ingestion()
    
    if success:
        print(f"\n🎉 Test completed successfully!")
        print(f"💡 You can now run the full performance benchmark:")
        print(f"   python tools/benchmark_socios_ingestion.py")
    else:
        print(f"\n💥 Test failed. Check the error messages above.")
        print(f"💡 Common issues:")
        print(f"   - Database connection (check DSN)")
        print(f"   - Table permissions")
        print(f"   - File accessibility")

if __name__ == "__main__":
    asyncio.run(main())
