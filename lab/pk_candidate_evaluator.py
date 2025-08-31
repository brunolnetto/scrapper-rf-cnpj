# Script to evaluate primary key candidates in a Parquet file
# Analyzes uniqueness, null counts, and data types for each column
# Usage: python pk_candidate_evaluator.py <path_to_parquet_file>

import sys
import polars as pl
from itertools import combinations

def evaluate_pk_candidates(parquet_path):
    """
    Evaluates columns in a Parquet file for primary key candidacy.
    Prints a report with uniqueness ratio, null count, and data type.
    Also evaluates composite keys from not-null columns.
    """
    try:
        # Read Parquet file with Polars in lazy mode for memory efficiency
        df = pl.scan_parquet(parquet_path)
        
        # Get schema and columns efficiently
        schema = df.collect_schema()
        columns = schema.names()
        total_rows = df.select(pl.len()).collect().item()
        
        print(f"Analyzing Parquet file: {parquet_path}")
        print(f"Total rows: {total_rows}")
        print("-" * 60)
        print(f"{'Column':<20} {'Unique (count/total)':<20} {'Nulls':<10} {'Data Type':<15} {'PK Candidate?'}")
        print("-" * 70)
        
        candidates = []
        col_stats = []
        for col in columns:
            unique_count = df.select(pl.col(col).n_unique()).collect().item()
            null_count = df.select(pl.col(col).null_count()).collect().item()
            uniqueness_ratio = (unique_count / total_rows) * 100 if total_rows > 0 else 0
            data_type = str(schema[col])
            
            # Criteria for PK candidate: high uniqueness, suitable type (allowing nulls)
            is_candidate = (
                uniqueness_ratio >= 95.0 and
                data_type in ['Int64', 'Utf8', 'String']
            )
            
            status = "Yes" if is_candidate else "No"
            print(f"{col:<20} {unique_count}/{total_rows} ({uniqueness_ratio:<10.2f}) {null_count:<10} {data_type:<15} {status}")
            
            if is_candidate:
                candidates.append(col)
            
            # Collect stats for composite
            col_stats.append((col, unique_count, null_count, uniqueness_ratio))
        
        print("-" * 60)
        if candidates:
            print(f"Single-column PK candidates: {', '.join(candidates)}")
        else:
            print("No single-column PK candidates found.")
        
        # Evaluate composite keys
        print("\nEvaluating composite key candidates...")
        # Use all columns, sorted by uniqueness descending
        all_stats = sorted(col_stats, key=lambda x: x[3], reverse=True)
        
        # Try combinations of 2 to 4 columns (limit for memory)
        composite_candidates = []
        for r in range(2, min(5, len(all_stats) + 1)):
            print(f"\n--- Evaluating combinations of {r} columns ---")
            combos = list(combinations([s[0] for s in all_stats], r))
            # Limit to top 20 combos per cardinality to save memory/time
            combos = combos[:20]
            for combo in combos:
                print(f"Evaluating: {', '.join(combo)}")
                unique_combo = df.select(list(combo)).unique().select(pl.len()).collect().item()
                ratio = (unique_combo / total_rows) * 100
                is_candidate = ratio >= 95.0
                print(f"  Unique: {unique_combo}/{total_rows} ({ratio:.2f}%), Candidate: {'Yes' if is_candidate else 'No'}")
                composite_candidates.append((combo, ratio, is_candidate))
            
            # Sort by ratio descending
            composite_candidates.sort(key=lambda x: x[1], reverse=True)
            
            print("\n" + "="*70)
            print("Top composite key candidates (sorted by uniqueness):")
            print(f"{'Composite Key':<50} {'Unique (count/total)':<20} {'Candidate?'}")
            print("-" * 70)
            for combo, ratio, is_candidate in composite_candidates[:10]:  # Show top 10
                status = "Yes" if is_candidate else "No"
                combo_str = ', '.join(combo)
                unique_count = int(ratio / 100 * total_rows)  # Approximate
                print(f"{combo_str:<50} {unique_count}/{total_rows} ({ratio:<10.2f}) {status}")
            
            strong_candidates = [c[0] for c in composite_candidates if c[2]]
            if strong_candidates:
                print(f"\nStrong composite PK candidates (>=95% unique): {strong_candidates[:5]}")  # Show top 5
            else:
                print("\nNo strong composite PK candidates found. Consider more columns or data cleaning.")
    
    except Exception as e:
        print(f"Error processing file: {e}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python pk_candidate_evaluator.py <path_to_parquet_file>")
        sys.exit(1)
    
    parquet_path = sys.argv[1]
    evaluate_pk_candidates(parquet_path)
