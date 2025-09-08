# Memory-efficient script to evaluate primary key candidates in a Parquet file
# Analyzes uniqueness, null counts, and data types for each column
# Usage: python pk_candidate_evaluator_efficient.py <path_to_parquet_file>

import sys
import polars as pl
from itertools import combinations

def evaluate_pk_candidates(parquet_path):
    """
    Evaluates columns in a Parquet file for primary key candidacy.
    Uses memory-efficient operations and focuses on most promising combinations.
    """
    try:
        # Read Parquet file with Polars in lazy mode
        df = pl.scan_parquet(parquet_path)
        
        # Get schema and columns efficiently
        schema = df.collect_schema()
        columns = schema.names()
        total_rows = df.select(pl.len()).collect().item()
        
        print(f"Analyzing Parquet file: {parquet_path}")
        print(f"Total rows: {total_rows}")
        print("-" * 60)
        print(f"{'Column':<25} {'Unique':<10} {'Nulls':<10} {'Ratio %':<10} {'Type':<15} {'PK?'}")
        print("-" * 80)
        
        candidates = []
        col_stats = []
        
        # Analyze each column
        for col in columns:
            # Use streaming operations to reduce memory usage
            unique_count = df.select(pl.col(col).n_unique()).collect().item()
            null_count = df.select(pl.col(col).null_count()).collect().item()
            uniqueness_ratio = (unique_count / total_rows) * 100 if total_rows > 0 else 0
            data_type = str(schema[col])
            
            # Criteria for PK candidate: high uniqueness, suitable type
            is_candidate = (
                uniqueness_ratio >= 95.0 and
                data_type in ['Int64', 'Utf8', 'String']
            )
            
            status = "Yes" if is_candidate else "No"
            print(f"{col:<25} {unique_count:<10} {null_count:<10} {uniqueness_ratio:<10.2f} {data_type:<15} {status}")
            
            if is_candidate:
                candidates.append(col)
            
            col_stats.append((col, unique_count, null_count, uniqueness_ratio))
        
        print("-" * 80)
        if candidates:
            print(f"Single-column PK candidates: {', '.join(candidates)}")
        else:
            print("No single-column PK candidates found.")
        
        # Filter non-null columns for composite key evaluation
        non_null_columns = [col for col, _, null_count, _ in col_stats if null_count == 0]
        print(f"\nNon-null columns ({len(non_null_columns)}): {', '.join(non_null_columns)}")
        
        if len(non_null_columns) < 2:
            print("Not enough non-null columns for composite key evaluation.")
            return
        
        # Sort by uniqueness descending and take top candidates
        non_null_stats = [(col, unique_count, null_count, uniqueness_ratio) 
                         for col, unique_count, null_count, uniqueness_ratio in col_stats 
                         if null_count == 0]
        non_null_stats.sort(key=lambda x: x[3], reverse=True)
        
        # Focus on most promising columns (top 5-6 by uniqueness)
        top_columns = [s[0] for s in non_null_stats[:6]]
        print(f"Top columns by uniqueness: {', '.join(top_columns)}")
        
        print(f"\n{'='*60}")
        print("EVALUATING COMPOSITE KEYS (Memory-efficient approach)")
        print(f"{'='*60}")
        
        composite_candidates = []
        
        # Evaluate 2-column combinations from top columns
        print(f"\n--- Combinations of 2 columns (from top {len(top_columns)}) ---")
        for combo in combinations(top_columns[:5], 2):  # Limit to top 5
            try:
                print(f"Testing: {', '.join(combo)}")
                
                # Use memory-efficient unique counting
                unique_combo = df.select(list(combo)).unique().select(pl.len()).collect().item()
                ratio = (unique_combo / total_rows) * 100
                is_candidate = ratio >= 95.0
                
                print(f"  → Unique: {unique_combo:,}/{total_rows:,} ({ratio:.2f}%) - {'✓ CANDIDATE' if is_candidate else '✗ Not sufficient'}")
                composite_candidates.append((combo, ratio, is_candidate, unique_combo))
                
            except Exception as e:
                print(f"  → Error: {e}")
        
        # Evaluate 3-column combinations (only most promising ones)
        print(f"\n--- Combinations of 3 columns (selected promising ones) ---")
        promising_3_combos = list(combinations(top_columns[:4], 3))[:5]  # Top 5 combos only
        
        for combo in promising_3_combos:
            try:
                print(f"Testing: {', '.join(combo)}")
                
                unique_combo = df.select(list(combo)).unique().select(pl.len()).collect().item()
                ratio = (unique_combo / total_rows) * 100
                is_candidate = ratio >= 95.0
                
                print(f"  → Unique: {unique_combo:,}/{total_rows:,} ({ratio:.2f}%) - {'✓ CANDIDATE' if is_candidate else '✗ Not sufficient'}")
                composite_candidates.append((combo, ratio, is_candidate, unique_combo))
                
            except Exception as e:
                print(f"  → Error: {e}")
        
        # Sort and display results
        composite_candidates.sort(key=lambda x: x[1], reverse=True)
        
        print(f"\n{'='*80}")
        print("SUMMARY: Top Composite Key Candidates")
        print(f"{'='*80}")
        print(f"{'Columns':<45} {'Unique Count':<15} {'Ratio %':<10} {'Status'}")
        print("-" * 80)
        
        for combo, ratio, is_candidate, unique_count in composite_candidates[:10]:
            status = "✓ CANDIDATE" if is_candidate else "✗ Insufficient"
            combo_str = ', '.join(combo)
            if len(combo_str) > 42:
                combo_str = combo_str[:39] + "..."
            print(f"{combo_str:<45} {unique_count:<15,} {ratio:<10.2f} {status}")
        
        # Final recommendations
        strong_candidates = [c[0] for c in composite_candidates if c[2]]
        
        print(f"\n{'='*60}")
        print("RECOMMENDATIONS")
        print(f"{'='*60}")
        
        if strong_candidates:
            print("✓ Strong composite PK candidates found (≥95% unique):")
            for i, combo in enumerate(strong_candidates[:3], 1):
                print(f"  {i}. {', '.join(combo)}")
                
            print(f"\n💡 For the 'socios' table constraint issue:")
            print(f"   Consider using: {', '.join(strong_candidates[0])}")
            print(f"   This combination has no nulls and high uniqueness.")
        else:
            print("✗ No strong composite PK candidates found (≥95% unique).")
            if composite_candidates:
                best = composite_candidates[0]
                print(f"   Best option: {', '.join(best[0])} ({best[1]:.2f}% unique)")
                print(f"   Consider data cleaning or additional columns.")
            else:
                print("   No viable composite keys found with current columns.")
    
    except Exception as e:
        print(f"❌ Error processing file: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python pk_candidate_evaluator_efficient.py <path_to_parquet_file>")
        sys.exit(1)
    
    parquet_path = sys.argv[1]
    evaluate_pk_candidates(parquet_path)
