#!/usr/bin/env python3
"""
Analysis and visualization of benchmark results.
"""

import sys
import os
from pathlib import Path
import json
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from typing import List, Dict, Any
import argparse

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from benchmarks.utils import BenchmarkResult, load_benchmark_results

def analyze_results(results: List[BenchmarkResult]) -> Dict[str, Any]:
    """Analyze benchmark results and return summary statistics."""
    
    analysis = {
        'total_benchmarks': len(results),
        'successful': len([r for r in results if not r.errors]),
        'failed': len([r for r in results if r.errors]),
        'tools': {},
        'operations': {},
        'performance_winners': {}
    }
    
    # Group by tool (DuckDB vs Polars)
    duckdb_results = [r for r in results if r.name.startswith('DuckDB') and not r.errors]
    polars_results = [r for r in results if r.name.startswith('Polars') and not r.errors]
    
    analysis['tools'] = {
        'DuckDB': {
            'count': len(duckdb_results),
            'avg_duration': sum(r.duration_seconds for r in duckdb_results) / len(duckdb_results) if duckdb_results else 0,
            'avg_memory': sum(r.peak_memory_gb for r in duckdb_results) / len(duckdb_results) if duckdb_results else 0,
            'total_rows': sum(r.rows_processed for r in duckdb_results if r.rows_processed) 
        },
        'Polars': {
            'count': len(polars_results),
            'avg_duration': sum(r.duration_seconds for r in polars_results) / len(polars_results) if polars_results else 0,
            'avg_memory': sum(r.peak_memory_gb for r in polars_results) / len(polars_results) if polars_results else 0,
            'total_rows': sum(r.rows_processed for r in polars_results if r.rows_processed)
        }
    }
    
    # Find winners for different categories
    if results:
        successful_results = [r for r in results if not r.errors]
        
        if successful_results:
            analysis['performance_winners'] = {
                'fastest_overall': min(successful_results, key=lambda x: x.duration_seconds).name,
                'most_memory_efficient': min(successful_results, key=lambda x: x.peak_memory_gb).name,
                'best_compression': max([r for r in successful_results if r.compression_ratio], 
                                      key=lambda x: x.compression_ratio).name if any(r.compression_ratio for r in successful_results) else None
            }
    
    return analysis

def create_visualizations(results: List[BenchmarkResult], output_dir: Path):
    """Create visualization charts for benchmark results."""
    
    # Prepare data for plotting
    data = []
    for result in results:
        if not result.errors:  # Only include successful results
            tool = 'DuckDB' if result.name.startswith('DuckDB') else 'Polars'
            operation = 'Ingestion' if 'Ingestion' in result.name else 'Aggregation'
            
            data.append({
                'Tool': tool,
                'Operation': operation,
                'Name': result.name,
                'Duration (s)': result.duration_seconds,
                'Peak Memory (GB)': result.peak_memory_gb,
                'Compression Ratio': result.compression_ratio or 0,
                'Rows Processed': result.rows_processed or 0,
                'Throughput (rows/s)': (result.rows_processed / result.duration_seconds) if result.rows_processed and result.duration_seconds > 0 else 0
            })
    
    if not data:
        print("No successful results to visualize")
        return
    
    df = pd.DataFrame(data)
    
    # Set up the plotting style
    plt.style.use('seaborn-v0_8')
    sns.set_palette("husl")
    
    # Create figure with subplots
    fig, axes = plt.subplots(2, 2, figsize=(15, 12))
    fig.suptitle('CNPJ Processing Benchmark Results', fontsize=16, fontweight='bold')
    
    # 1. Duration comparison
    sns.barplot(data=df, x='Tool', y='Duration (s)', hue='Operation', ax=axes[0,0])
    axes[0,0].set_title('Processing Duration by Tool')
    axes[0,0].set_ylabel('Duration (seconds)')
    
    # 2. Memory usage comparison
    sns.barplot(data=df, x='Tool', y='Peak Memory (GB)', hue='Operation', ax=axes[0,1])
    axes[0,1].set_title('Peak Memory Usage by Tool')
    axes[0,1].set_ylabel('Peak Memory (GB)')
    
    # 3. Throughput comparison (if we have row data)
    if df['Throughput (rows/s)'].sum() > 0:
        throughput_df = df[df['Throughput (rows/s)'] > 0]
        sns.barplot(data=throughput_df, x='Tool', y='Throughput (rows/s)', hue='Operation', ax=axes[1,0])
        axes[1,0].set_title('Processing Throughput')
        axes[1,0].set_ylabel('Rows per Second')
    else:
        axes[1,0].text(0.5, 0.5, 'No throughput data available', ha='center', va='center', transform=axes[1,0].transAxes)
        axes[1,0].set_title('Processing Throughput')
    
    # 4. Compression efficiency (if we have compression data)
    if df['Compression Ratio'].sum() > 0:
        compression_df = df[df['Compression Ratio'] > 0]
        sns.barplot(data=compression_df, x='Tool', y='Compression Ratio', hue='Operation', ax=axes[1,1])
        axes[1,1].set_title('Compression Efficiency')
        axes[1,1].set_ylabel('Compression Ratio')
    else:
        axes[1,1].text(0.5, 0.5, 'No compression data available', ha='center', va='center', transform=axes[1,1].transAxes)
        axes[1,1].set_title('Compression Efficiency')
    
    plt.tight_layout()
    
    # Save plot
    plot_path = output_dir / 'benchmark_visualization.png'
    plt.savefig(plot_path, dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"Visualization saved to: {plot_path}")
    
    # Create detailed comparison chart
    if len(df) > 1:
        fig, ax = plt.subplots(1, 1, figsize=(12, 8))
        
        # Scatter plot: Duration vs Memory
        for tool in df['Tool'].unique():
            tool_data = df[df['Tool'] == tool]
            ax.scatter(tool_data['Duration (s)'], tool_data['Peak Memory (GB)'], 
                      label=tool, s=100, alpha=0.7)
            
            # Add labels for each point
            for _, row in tool_data.iterrows():
                ax.annotate(row['Name'].split('_')[-1], 
                           (row['Duration (s)'], row['Peak Memory (GB)']),
                           xytext=(5, 5), textcoords='offset points', fontsize=8)
        
        ax.set_xlabel('Duration (seconds)')
        ax.set_ylabel('Peak Memory (GB)')
        ax.set_title('Performance Scatter Plot: Duration vs Memory Usage')
        ax.legend()
        ax.grid(True, alpha=0.3)
        
        scatter_path = output_dir / 'performance_scatter.png'
        plt.savefig(scatter_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"Scatter plot saved to: {scatter_path}")

def generate_comparison_table(results: List[BenchmarkResult], output_path: Path):
    """Generate a detailed comparison table."""
    
    # Create DataFrame from results
    data = []
    for result in results:
        data.append({
            'Benchmark': result.name,
            'Status': 'Success' if not result.errors else 'Failed',
            'Duration (s)': f"{result.duration_seconds:.2f}",
            'Peak Memory (GB)': f"{result.peak_memory_gb:.2f}",
            'Memory Delta (GB)': f"{result.memory_delta_gb:.2f}",
            'CPU (%)': f"{result.cpu_percent:.1f}",
            'I/O Read (GB)': f"{result.io_read_gb:.2f}",
            'I/O Write (GB)': f"{result.io_write_gb:.2f}",
            'Rows Processed': f"{result.rows_processed:,}" if result.rows_processed else "N/A",
            'Compression Ratio': f"{result.compression_ratio:.2f}x" if result.compression_ratio else "N/A",
            'Errors': result.errors or "None"
        })
    
    df = pd.DataFrame(data)
    
    # Save as CSV
    csv_path = output_path.with_suffix('.csv')
    df.to_csv(csv_path, index=False)
    
    # Save as formatted HTML table
    html_path = output_path.with_suffix('.html')
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>CNPJ Benchmark Results</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; }}
            table {{ border-collapse: collapse; width: 100%; }}
            th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
            th {{ background-color: #f2f2f2; }}
            .success {{ background-color: #d4edda; }}
            .failed {{ background-color: #f8d7da; }}
        </style>
    </head>
    <body>
        <h1>CNPJ Processing Benchmark Results</h1>
        {df.to_html(index=False, escape=False, classes='table')}
    </body>
    </html>
    """
    
    with open(html_path, 'w') as f:
        f.write(html_content)
    
    print(f"Comparison table saved to: {csv_path}")
    print(f"HTML report saved to: {html_path}")

def main():
    parser = argparse.ArgumentParser(description="Analyze benchmark results")
    parser.add_argument("results_file", type=Path, help="Path to benchmark results JSON file")
    parser.add_argument("--output-dir", type=Path, default=Path("analysis_output"),
                       help="Directory for analysis outputs")
    
    args = parser.parse_args()
    
    if not args.results_file.exists():
        print(f"Results file not found: {args.results_file}")
        return 1
    
    # Load results
    print(f"Loading results from: {args.results_file}")
    results = load_benchmark_results(args.results_file)
    
    if not results:
        print("No results found in file")
        return 1
    
    print(f"Loaded {len(results)} benchmark results")
    
    # Create output directory
    args.output_dir.mkdir(parents=True, exist_ok=True)
    
    # Analyze results
    analysis = analyze_results(results)
    
    # Print summary
    print("\n" + "="*50)
    print("BENCHMARK ANALYSIS SUMMARY")
    print("="*50)
    print(f"Total benchmarks: {analysis['total_benchmarks']}")
    print(f"Successful: {analysis['successful']}")
    print(f"Failed: {analysis['failed']}")
    print()
    
    for tool, stats in analysis['tools'].items():
        if stats['count'] > 0:
            print(f"{tool}:")
            print(f"  - Benchmarks: {stats['count']}")
            print(f"  - Avg Duration: {stats['avg_duration']:.2f}s")
            print(f"  - Avg Memory: {stats['avg_memory']:.2f}GB")
            print(f"  - Total Rows: {stats['total_rows']:,}" if stats['total_rows'] else "  - Total Rows: N/A")
            print()
    
    if analysis['performance_winners']:
        print("Performance Winners:")
        for category, winner in analysis['performance_winners'].items():
            if winner:
                print(f"  - {category.replace('_', ' ').title()}: {winner}")
    
    # Create visualizations
    try:
        print("\nCreating visualizations...")
        create_visualizations(results, args.output_dir)
    except Exception as e:
        print(f"Failed to create visualizations: {e}")
    
    # Generate comparison table
    print("Generating comparison table...")
    table_path = args.output_dir / "comparison_table"
    generate_comparison_table(results, table_path)
    
    # Save analysis summary
    analysis_path = args.output_dir / "analysis_summary.json"
    with open(analysis_path, 'w') as f:
        json.dump(analysis, f, indent=2)
    
    print(f"\nAnalysis complete! Output saved to: {args.output_dir}")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())