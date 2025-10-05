#!/usr/bin/env python3
"""
Benchmark comparison script for CSV → Postgres ingestion
Compares Polars vs DuckDB implementations using their built-in --benchmark modes
"""

import argparse
import subprocess
import sys
import time
import json
import re
from pathlib import Path
from typing import Dict, List

def parse_benchmark_output(output: str) -> List[Dict]:
    """Parse benchmark results from script output"""
    results = []
    
    lines = output.split('\n')
    
    # Look for "Result:" lines in the output
    for i, line in enumerate(lines):
        if "Result:" in line:
            # Find the preceding "Testing:" line
            description = "Unknown"
            for j in range(i-1, max(i-10, -1), -1):
                if "Testing:" in lines[j]:
                    description = lines[j].replace("Testing:", "").strip()
                    break
            
            # Parse the result line: "Result: 33,920 rows/s, 0.16s, ~3 MB"
            match = re.search(r'Result:\s*(\d[\d,]*)\s*rows/s,\s*([\d.]+)s,\s*~?([\d.]+)\s*MB', line)
            if match:
                results.append({
                    'description': description,
                    'throughput': float(match.group(1).replace(',', '')),
                    'time': float(match.group(2)),
                    'memory_mb': float(match.group(3))
                })
    
    return results

def run_script_benchmark(script_path: str, csv_files: List[str], 
                        pg_dsn: str, pg_table: str, sample_rows: int) -> Dict:
    """Run a single script's benchmark mode"""
    
    cmd = [
        sys.executable, script_path,
        *csv_files,
        "--pg-dsn", pg_dsn,
        "--pg-table", pg_table,
        "--benchmark",
        "--benchmark-sample-rows", str(sample_rows)
    ]
    
    print(f"Running: {' '.join(cmd)}\n")
    
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=600
        )
        
        if result.returncode != 0:
            print(f"Error running benchmark:")
            print("STDOUT:", result.stdout)
            print("STDERR:", result.stderr)
            return {"error": result.stderr, "results": []}
        
        # Parse the output
        parsed_results = parse_benchmark_output(result.stdout)
        
        print(f"Parsed {len(parsed_results)} benchmark results")
        if not parsed_results:
            print("DEBUG: No results found. Sample output:")
            print(result.stdout[:1000] + "..." if len(result.stdout) > 1000 else result.stdout)
        
        return {
            "success": True,
            "results": parsed_results,
            "stdout": result.stdout
        }
        
    except subprocess.TimeoutExpired:
        return {"error": "Timeout after 600s", "results": []}
    except Exception as e:
        return {"error": str(e), "results": []}

def print_side_by_side_comparison(polars_data: Dict, duckdb_data: Dict):
    """Print side-by-side comparison of benchmark results"""
    
    print(f"\n{'='*120}")
    print(f"POLARS vs DUCKDB BENCHMARK COMPARISON")
    print(f"{'='*120}")
    
    polars_results = polars_data.get('results', [])
    duckdb_results = duckdb_data.get('results', [])
    
    if not polars_results and not duckdb_results:
        print("No results to compare")
        return
    
    # Match configurations by description
    config_map = {}
    
    for pr in polars_results:
        desc = pr['description']
        if desc not in config_map:
            config_map[desc] = {}
        config_map[desc]['polars'] = pr
    
    for dr in duckdb_results:
        desc = dr['description']
        if desc not in config_map:
            config_map[desc] = {}
        config_map[desc]['duckdb'] = dr
    
    # Print header
    print(f"{'Configuration':<45} {'Polars':>18} {'DuckDB':>18} {'Winner':>12} {'Speedup':>10}")
    print(f"{'-'*120}")
    
    polars_wins = 0
    duckdb_wins = 0
    
    for config, data in sorted(config_map.items()):
        polars = data.get('polars')
        duckdb = data.get('duckdb')
        
        p_throughput = f"{polars['throughput']:>10,.0f} r/s" if polars else "N/A".rjust(15)
        d_throughput = f"{duckdb['throughput']:>10,.0f} r/s" if duckdb else "N/A".rjust(15)
        
        if polars and duckdb:
            if polars['throughput'] > duckdb['throughput']:
                winner = "Polars"
                speedup = f"{polars['throughput']/duckdb['throughput']:.2f}x"
                polars_wins += 1
            else:
                winner = "DuckDB"
                speedup = f"{duckdb['throughput']/polars['throughput']:.2f}x"
                duckdb_wins += 1
        else:
            winner = "N/A"
            speedup = "N/A"
        
        print(f"{config:<45} {p_throughput:>18} {d_throughput:>18} {winner:>12} {speedup:>10}")
    
    print(f"{'='*120}")
    
    # Summary statistics
    print(f"\nSUMMARY:")
    print(f"{'-'*120}")
    
    if polars_results:
        polars_avg = sum(r['throughput'] for r in polars_results) / len(polars_results)
        polars_max = max(r['throughput'] for r in polars_results)
        polars_best = max(polars_results, key=lambda x: x['throughput'])
        print(f"Polars:  Avg: {polars_avg:>12,.0f} r/s  |  Max: {polars_max:>12,.0f} r/s  |  Wins: {polars_wins}/{len(config_map)}")
        print(f"         Best: {polars_best['description']}")
    
    if duckdb_results:
        duckdb_avg = sum(r['throughput'] for r in duckdb_results) / len(duckdb_results)
        duckdb_max = max(r['throughput'] for r in duckdb_results)
        duckdb_best = max(duckdb_results, key=lambda x: x['throughput'])
        print(f"DuckDB:  Avg: {duckdb_avg:>12,.0f} r/s  |  Max: {duckdb_max:>12,.0f} r/s  |  Wins: {duckdb_wins}/{len(config_map)}")
        print(f"         Best: {duckdb_best['description']}")
    
    if polars_results and duckdb_results:
        overall_speedup = duckdb_avg / polars_avg
        print(f"\nOverall: DuckDB is {overall_speedup:.2f}x {'faster' if overall_speedup > 1 else 'slower'} on average")
    
    print(f"{'='*120}\n")

def main():
    parser = argparse.ArgumentParser(
        description="Compare Polars vs DuckDB CSV ingestion benchmarks"
    )
    parser.add_argument("csv_files", nargs="+", help="CSV files to benchmark with")
    parser.add_argument("--polars-script", default="polars_benchmark.py",
                       help="Path to Polars script")
    parser.add_argument("--duckdb-script", default="duckdb_benchmark.py",
                       help="Path to DuckDB script")
    parser.add_argument("--pg-dsn", required=True, help="PostgreSQL connection string")
    parser.add_argument("--pg-table", default="benchmark_test",
                       help="Test table name")
    parser.add_argument("--sample-rows", type=int, default=100000,
                       help="Number of rows to benchmark (default: 100000)")
    parser.add_argument("--output-json", help="Save results to JSON file")
    
    args = parser.parse_args()
    
    # Verify scripts exist
    if not Path(args.polars_script).exists():
        print(f"Error: Polars script not found: {args.polars_script}")
        return 1
    
    if not Path(args.duckdb_script).exists():
        print(f"Error: DuckDB script not found: {args.duckdb_script}")
        return 1
    
        # Run Polars benchmark
    print(f"\n{'#'*120}")
    print(f"RUNNING POLARS BENCHMARK (--benchmark mode)")
    print(f"{'#'*120}\n")
    
    polars_table = f"{args.pg_table}_polars_{int(time.time())}"
    polars_data = run_script_benchmark(
        args.polars_script, args.csv_files, args.pg_dsn, 
        polars_table, args.sample_rows
    )
    
    if not polars_data.get('success'):
        print(f"Polars benchmark failed: {polars_data.get('error', 'Unknown error')}")
    
    # Clean up Polars table to avoid conflicts
    try:
        import psycopg2
        with psycopg2.connect(args.pg_dsn) as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"DROP TABLE IF EXISTS {polars_table}")
                cursor.execute(f"DROP TABLE IF EXISTS {polars_table}_benchmark_test")
                conn.commit()
    except Exception as e:
        print(f"Warning: Could not clean up Polars tables: {e}")
    
    # Run DuckDB benchmark
    print(f"\n{'#'*120}")
    print(f"RUNNING DUCKDB BENCHMARK (--benchmark mode)")
    print(f"{'#'*120}\n")
    
    duckdb_table = f"{args.pg_table}_duckdb_{int(time.time())}"
    duckdb_data = run_script_benchmark(
        args.duckdb_script, args.csv_files, args.pg_dsn,
        duckdb_table, args.sample_rows
    )
    
    if not duckdb_data.get('success'):
        print(f"DuckDB benchmark failed: {duckdb_data.get('error', 'Unknown error')}")
    
    # Clean up DuckDB table
    try:
        import psycopg2
        with psycopg2.connect(args.pg_dsn) as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"DROP TABLE IF EXISTS {duckdb_table}")
                cursor.execute(f"DROP TABLE IF EXISTS {duckdb_table}_benchmark_test")
                conn.commit()
    except Exception as e:
        print(f"Warning: Could not clean up DuckDB tables: {e}")
    
    # Print comparison
    print_side_by_side_comparison(polars_data, duckdb_data)
    
    # Save to JSON if requested
    if args.output_json:
        output = {
            "polars": polars_data,
            "duckdb": duckdb_data,
            "sample_rows": args.sample_rows
        }
        with open(args.output_json, 'w') as f:
            json.dump(output, f, indent=2)
        print(f"Full results saved to: {args.output_json}")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())