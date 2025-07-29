#!/usr/bin/env python3
"""
Example script demonstrating Parquet to PostgreSQL loading utilities.

This script shows how to use the ParquetLoader class and convenience functions
to efficiently load Parquet files into PostgreSQL using different methods.
"""

import os
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from database.utils.parquet_loader import ParquetLoader, load_parquet_to_postgres, load_multiple_parquet_files
from database.engine import create_database_instance
from setup.logging import logger

def example_single_file_loading():
    """Example: Load a single Parquet file"""
    print("\n" + "="*60)
    print("EXAMPLE: Single Parquet File Loading")
    print("="*60)
    
    # Setup database connection
    connection_string = "postgresql://postgres:postgres@localhost:5432/postgres"
    database = create_database_instance(connection_string)
    
    # Example Parquet file path (replace with actual path)
    parquet_file = Path("data/EXTRACTED_FILES/parquet_data/empresa.parquet")
    
    if not parquet_file.exists():
        print(f"⚠️  Parquet file not found: {parquet_file}")
        print("   Please run the ETL pipeline first to generate Parquet files.")
        return
    
    # Method 1: Using convenience function
    print("\n1. Using convenience function (auto-select method):")
    metrics = load_parquet_to_postgres(
        database=database,
        parquet_file=parquet_file,
        table_name="empresa_test",
        method="auto"
    )
    
    print(f"   Success: {metrics.success}")
    print(f"   Rows loaded: {metrics.rows_loaded:,}")
    print(f"   Time: {metrics.load_time_seconds:.2f}s")
    print(f"   Throughput: {metrics.throughput_mb_per_second:.1f}MB/s")
    
    # Method 2: Using ParquetLoader class
    print("\n2. Using ParquetLoader class (specific method):")
    loader = ParquetLoader(database)
    
    # Try different methods
    methods = ["sqlalchemy", "duckdb", "streaming"]
    for method in methods:
        print(f"\n   Testing method: {method}")
        try:
            metrics = loader.load_table_from_parquet(
                table_name=f"empresa_test_{method}",
                parquet_file=parquet_file,
                method=method
            )
            print(f"   Success: {metrics.success}")
            print(f"   Time: {metrics.load_time_seconds:.2f}s")
            print(f"   Memory: {metrics.memory_peak_mb:.1f}MB")
        except Exception as e:
            print(f"   Error: {e}")

def example_multiple_files_loading():
    """Example: Load multiple Parquet files in parallel"""
    print("\n" + "="*60)
    print("EXAMPLE: Multiple Parquet Files Loading")
    print("="*60)
    
    # Setup database connection
    connection_string = "postgresql://postgres:postgres@localhost:5432/postgres"
    database = create_database_instance(connection_string)
    
    # Example Parquet files (replace with actual paths)
    parquet_dir = Path("data/EXTRACTED_FILES/parquet_data")
    
    if not parquet_dir.exists():
        print(f"⚠️  Parquet directory not found: {parquet_dir}")
        print("   Please run the ETL pipeline first to generate Parquet files.")
        return
    
    # Find all Parquet files
    parquet_files = list(parquet_dir.glob("*.parquet"))
    
    if not parquet_files:
        print("   No Parquet files found in directory.")
        return
    
    print(f"Found {len(parquet_files)} Parquet files:")
    for file in parquet_files:
        print(f"   - {file.name}")
    
    # Create table mapping (table name -> file path)
    table_files = {}
    for parquet_file in parquet_files:
        # Extract table name from filename (remove .parquet extension)
        table_name = parquet_file.stem
        table_files[table_name] = parquet_file
    
    # Load multiple files in parallel
    print(f"\nLoading {len(table_files)} tables in parallel...")
    metrics_list = load_multiple_parquet_files(
        database=database,
        table_files=table_files,
        method="auto",
        max_workers=4
    )
    
    # Print results
    print("\nResults:")
    successful = 0
    total_time = 0
    total_rows = 0
    
    for metrics in metrics_list:
        if metrics.success:
            successful += 1
            total_time += metrics.load_time_seconds
            total_rows += metrics.rows_loaded
            print(f"   ✅ {metrics.table_name}: {metrics.rows_loaded:,} rows in {metrics.load_time_seconds:.2f}s")
        else:
            print(f"   ❌ {metrics.table_name}: {metrics.error_message}")
    
    print(f"\nSummary: {successful}/{len(metrics_list)} tables loaded successfully")
    print(f"Total rows: {total_rows:,}")
    print(f"Total time: {total_time:.2f}s")

def example_performance_comparison():
    """Example: Compare performance of different loading methods"""
    print("\n" + "="*60)
    print("EXAMPLE: Performance Comparison")
    print("="*60)
    
    # Setup database connection
    connection_string = "postgresql://postgres:postgres@localhost:5432/postgres"
    database = create_database_instance(connection_string)
    
    # Example Parquet file
    parquet_file = Path("data/EXTRACTED_FILES/parquet_data/estabelecimento.parquet")
    
    if not parquet_file.exists():
        print(f"⚠️  Parquet file not found: {parquet_file}")
        print("   Please run the ETL pipeline first to generate Parquet files.")
        return
    
    loader = ParquetLoader(database)
    
    # Test all available methods
    methods = []
    if loader.duckdb_available:
        methods.extend(["sqlalchemy", "duckdb", "streaming"])
    else:
        methods.extend(["sqlalchemy", "streaming"])
    
    print(f"Testing methods: {', '.join(methods)}")
    print(f"File: {parquet_file.name}")
    print(f"Size: {parquet_file.stat().st_size / (1024*1024):.1f}MB")
    
    results = []
    
    for method in methods:
        print(f"\nTesting {method.upper()} method...")
        try:
            metrics = loader.load_table_from_parquet(
                table_name=f"perf_test_{method}",
                parquet_file=parquet_file,
                method=method
            )
            results.append(metrics)
            
            if metrics.success:
                print(f"   ✅ Success: {metrics.rows_loaded:,} rows in {metrics.load_time_seconds:.2f}s")
                print(f"   📊 Throughput: {metrics.throughput_mb_per_second:.1f}MB/s, {metrics.throughput_rows_per_second:.0f} rows/s")
                print(f"   💾 Memory: {metrics.memory_peak_mb:.1f}MB")
            else:
                print(f"   ❌ Failed: {metrics.error_message}")
                
        except Exception as e:
            print(f"   ❌ Error: {e}")
    
    # Show comparison
    successful_results = [r for r in results if r.success]
    if successful_results:
        print(f"\n📈 PERFORMANCE COMPARISON:")
        print("-" * 60)
        
        # Sort by load time (fastest first)
        successful_results.sort(key=lambda x: x.load_time_seconds)
        
        for i, metrics in enumerate(successful_results, 1):
            print(f"{i}. {metrics.method.upper():12s} | "
                  f"Time: {metrics.load_time_seconds:6.2f}s | "
                  f"Speed: {metrics.throughput_mb_per_second:6.1f}MB/s | "
                  f"Memory: {metrics.memory_peak_mb:6.1f}MB")
        
        fastest = successful_results[0]
        print(f"\n🏆 Fastest method: {fastest.method.upper()} ({fastest.load_time_seconds:.2f}s)")

def main():
    """Main function to run all examples"""
    print("🚀 PARQUET TO POSTGRESQL LOADING EXAMPLES")
    print("=" * 60)
    
    # Check if we're in the right directory
    if not Path("src").exists():
        print("❌ Please run this script from the project root directory")
        print("   cd /path/to/scrapper-rf-cnpj")
        print("   python examples/parquet_loading_example.py")
        return
    
    # Check database connection
    try:
        connection_string = "postgresql://postgres:postgres@localhost:5432/postgres"
        database = create_database_instance(connection_string)
        with database.engine.connect() as conn:
            conn.execute("SELECT 1")
        print("✅ Database connection successful")
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        print("   Please ensure PostgreSQL is running and accessible")
        return
    
    # Run examples
    try:
        example_single_file_loading()
        example_multiple_files_loading()
        example_performance_comparison()
    except Exception as e:
        print(f"❌ Error running examples: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()