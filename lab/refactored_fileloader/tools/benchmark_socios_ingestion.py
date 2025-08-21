#!/usr/bin/env python3
"""
Performance benchmark script for ingesting socios.parquet file.

This script tests different configurations to find optimal performance
for ingesting a large real-world CNPJ dataset (~25M rows, ~520MB).
"""

import asyncio
import time
import subprocess
import sys
from pathlib import Path
import psutil
import os

# Configuration
DATA_DIR = Path("data")
SOCIOS_FILE = DATA_DIR / "socios.parquet"
DSN = "postgresql://testuser:testpass@localhost:5433/testdb"
TABLE = "socios"

# Socios table schema
HEADERS = "cnpj_basico,identificador_socio,nome_socio_razao_social,cpf_cnpj_socio,qualificacao_socio,data_entrada_sociedade,pais,representante_legal,nome_do_representante,qualificacao_representante_legal,faixa_etaria"
PRIMARY_KEY = "cnpj_basico,identificador_socio"

# Performance test configurations
PERFORMANCE_CONFIGS = [
    # (batch_size, sub_batch_size, concurrency, description)
    (10_000, 1_000, 1, "Conservative: Small batches, single file"),
    (50_000, 5_000, 1, "Balanced: Medium batches, single file"),
    (100_000, 10_000, 1, "Aggressive: Large batches, single file"),
    (50_000, 5_000, 2, "Parallel: Medium batches, internal parallelism"),
    (100_000, 10_000, 2, "High throughput: Large batches, internal parallelism"),
]

def get_file_info():
    """Get file information for reporting."""
    if not SOCIOS_FILE.exists():
        raise FileNotFoundError(f"File not found: {SOCIOS_FILE}")
    
    file_size_mb = SOCIOS_FILE.stat().st_size / (1024 * 1024)
    
    # Get row count from parquet metadata
    try:
        import pyarrow.parquet as pq
        pf = pq.ParquetFile(SOCIOS_FILE)
        row_count = pf.metadata.num_rows
        return file_size_mb, row_count
    except ImportError:
        return file_size_mb, "Unknown (pyarrow not available)"

def get_system_info():
    """Get current system resource usage."""
    memory = psutil.virtual_memory()
    cpu_count = psutil.cpu_count()
    load_avg = os.getloadavg() if hasattr(os, 'getloadavg') else "N/A"
    
    return {
        'cpu_count': cpu_count,
        'memory_total_gb': round(memory.total / (1024**3), 2),
        'memory_available_gb': round(memory.available / (1024**3), 2),
        'memory_percent': memory.percent,
        'load_avg': load_avg
    }

async def run_ingestion_test(batch_size: int, sub_batch_size: int, internal_concurrency: int, description: str):
    """Run a single ingestion test with specified parameters."""
    
    # Prepare the command
    cmd = [
        "python3", "-m", "src.cli",
        "--dsn", DSN,
        "--file-type", "parquet",
        "--table", TABLE,
        "--pk", PRIMARY_KEY,
        "--headers", HEADERS,
        "--batch-size", str(batch_size),
        "--sub-batch-size", str(sub_batch_size),
        "--internal-concurrency", str(internal_concurrency),
        "--max-retries", "3",
        str(SOCIOS_FILE)
    ]
    
    print(f"\n{'='*60}")
    print(f"🔄 Testing: {description}")
    print(f"   Parameters: batch={batch_size:,}, sub_batch={sub_batch_size:,}, concurrency={internal_concurrency}")
    print(f"   Command: {' '.join(cmd[-6:])}")  # Show key parameters
    
    # Capture system state before
    start_system = get_system_info()
    start_time = time.time()
    
    try:
        # Run the ingestion
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        stdout, stderr = await proc.communicate()
        
        # Calculate results
        elapsed_time = time.time() - start_time
        end_system = get_system_info()
        
        if proc.returncode == 0:
            print(f"✅ SUCCESS in {elapsed_time:.1f}s")
            
            # Parse output for additional metrics if available
            output_lines = stdout.decode().split('\n')
            for line in output_lines:
                if 'rows' in line and 'Completed' in line:
                    print(f"   Output: {line.strip()}")
            
            return {
                'success': True,
                'elapsed_time': elapsed_time,
                'batch_size': batch_size,
                'sub_batch_size': sub_batch_size,
                'internal_concurrency': internal_concurrency,
                'description': description,
                'start_memory_gb': start_system['memory_available_gb'],
                'end_memory_gb': end_system['memory_available_gb'],
                'stdout': stdout.decode(),
                'stderr': stderr.decode()
            }
        else:
            print(f"❌ FAILED (exit code: {proc.returncode}) in {elapsed_time:.1f}s")
            error_msg = stderr.decode().strip()
            if error_msg:
                print(f"   Error: {error_msg[:200]}...")
            
            return {
                'success': False,
                'elapsed_time': elapsed_time,
                'batch_size': batch_size,
                'sub_batch_size': sub_batch_size,
                'internal_concurrency': internal_concurrency,
                'description': description,
                'error': error_msg,
                'stdout': stdout.decode(),
                'stderr': stderr.decode()
            }
            
    except Exception as e:
        elapsed_time = time.time() - start_time
        print(f"❌ EXCEPTION in {elapsed_time:.1f}s: {e}")
        return {
            'success': False,
            'elapsed_time': elapsed_time,
            'batch_size': batch_size,
            'sub_batch_size': sub_batch_size,
            'internal_concurrency': internal_concurrency,
            'description': description,
            'error': str(e)
        }

def print_summary(results, file_size_mb, row_count):
    """Print a comprehensive summary of all test results."""
    
    print(f"\n{'='*80}")
    print(f"📊 SOCIOS INGESTION PERFORMANCE SUMMARY")
    print(f"{'='*80}")
    print(f"File: {SOCIOS_FILE}")
    print(f"Size: {file_size_mb:.1f} MB")
    print(f"Rows: {row_count:,}")
    
    successful_results = [r for r in results if r['success']]
    failed_results = [r for r in results if not r['success']]
    
    print(f"\nResults: {len(successful_results)} successful, {len(failed_results)} failed")
    
    if successful_results:
        print(f"\n🏆 SUCCESSFUL CONFIGURATIONS:")
        print(f"{'Rank':<4} {'Time':<8} {'Rate':<12} {'Batch':<8} {'Sub':<8} {'Conc':<4} {'Description':<30}")
        print("-" * 80)
        
        # Sort by elapsed time (fastest first)
        sorted_results = sorted(successful_results, key=lambda x: x['elapsed_time'])
        
        for i, result in enumerate(sorted_results, 1):
            if isinstance(row_count, int):
                rate = f"{row_count / result['elapsed_time']:.0f} rows/s"
                mb_rate = f"{file_size_mb / result['elapsed_time']:.1f} MB/s"
            else:
                rate = f"{file_size_mb / result['elapsed_time']:.1f} MB/s"
                mb_rate = ""
            
            print(f"{i:<4} {result['elapsed_time']:<8.1f} {rate:<12} "
                  f"{result['batch_size']:<8,} {result['sub_batch_size']:<8,} "
                  f"{result['internal_concurrency']:<4} {result['description']:<30}")
        
        # Best configuration details
        best = sorted_results[0]
        print(f"\n🥇 BEST CONFIGURATION:")
        print(f"   Time: {best['elapsed_time']:.1f}s")
        print(f"   Config: batch={best['batch_size']:,}, sub_batch={best['sub_batch_size']:,}, concurrency={best['internal_concurrency']}")
        print(f"   Description: {best['description']}")
        if isinstance(row_count, int):
            print(f"   Throughput: {row_count / best['elapsed_time']:.0f} rows/s")
        print(f"   Data rate: {file_size_mb / best['elapsed_time']:.1f} MB/s")
    
    if failed_results:
        print(f"\n❌ FAILED CONFIGURATIONS:")
        for result in failed_results:
            print(f"   {result['description']}: {result.get('error', 'Unknown error')[:100]}")
    
    # System info
    system_info = get_system_info()
    print(f"\n💻 SYSTEM INFO:")
    print(f"   CPU cores: {system_info['cpu_count']}")
    print(f"   Memory: {system_info['memory_available_gb']:.1f}GB available / {system_info['memory_total_gb']:.1f}GB total ({system_info['memory_percent']:.1f}% used)")
    if system_info['load_avg'] != "N/A":
        print(f"   Load average: {system_info['load_avg']}")

async def main():
    """Main execution function."""
    print("🚀 SOCIOS PARQUET INGESTION PERFORMANCE BENCHMARK")
    print("=" * 60)
    
    # Verify file exists and get info
    try:
        file_size_mb, row_count = get_file_info()
        print(f"📁 File: {SOCIOS_FILE}")
        print(f"📏 Size: {file_size_mb:.1f} MB")
        print(f"📊 Rows: {row_count:,}" if isinstance(row_count, int) else f"📊 Rows: {row_count}")
    except FileNotFoundError as e:
        print(f"❌ Error: {e}")
        print("💡 Make sure the socios.parquet file exists in the data/ directory")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error reading file info: {e}")
        sys.exit(1)
    
    # Show system info
    system_info = get_system_info()
    print(f"\n💻 System: {system_info['cpu_count']} cores, {system_info['memory_available_gb']:.1f}GB RAM available")
    
    # Run all performance tests
    results = []
    total_configs = len(PERFORMANCE_CONFIGS)
    
    for i, (batch_size, sub_batch_size, internal_concurrency, description) in enumerate(PERFORMANCE_CONFIGS, 1):
        print(f"\n📋 Progress: {i}/{total_configs}")
        result = await run_ingestion_test(batch_size, sub_batch_size, internal_concurrency, description)
        results.append(result)
        
        # Short pause between tests to let system stabilize
        await asyncio.sleep(2)
    
    # Print comprehensive summary
    print_summary(results, file_size_mb, row_count)

if __name__ == "__main__":
    asyncio.run(main())
