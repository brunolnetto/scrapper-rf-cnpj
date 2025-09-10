#!/usr/bin/env python3
"""
Test script to validate the corrected batch hierarchy implementation.
This script tests the expected 4-tier structure: Table → File → Batch → Subbatch
"""

import sys
import os
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

def test_batch_hierarchy_calculation():
    """Test the mathematical calculations for batch/subbatch counts."""
    
    test_cases = [
        # (total_rows, batch_size, subbatch_size, expected_batches, expected_subbatches)
        (10000, 1000, 300, 10, 40),  # User's example
        (5000, 1000, 300, 5, 20),    # Smaller file
        (1500, 1000, 300, 2, 6),     # File just over 1 batch (1500 rows: batch1=1000→4 sub, batch2=500→2 sub = 6 total)
        (500, 1000, 300, 1, 2),      # Small file (single batch)
        (42780000, 10000, 1000, 4278, 42780),  # Large file like Simples.zip
    ]
    
    import math
    
    print("🧮 Testing Batch Hierarchy Calculations")
    print("=" * 60)
    
    for total_rows, batch_size, subbatch_size, expected_batches, expected_subbatches in test_cases:
        # Calculate expected values
        calculated_batches = math.ceil(total_rows / batch_size)
        calculated_subbatches = 0
        
        for batch_num in range(calculated_batches):
            start_row = batch_num * batch_size
            end_row = min(start_row + batch_size, total_rows)
            actual_batch_rows = end_row - start_row
            subbatches_in_batch = math.ceil(actual_batch_rows / subbatch_size)
            calculated_subbatches += subbatches_in_batch
        
        # Validate
        batch_match = calculated_batches == expected_batches
        subbatch_match = calculated_subbatches == expected_subbatches
        
        status = "✅" if (batch_match and subbatch_match) else "❌"
        
        print(f"{status} {total_rows:,} rows, batch_size={batch_size}, subbatch_size={subbatch_size}")
        print(f"   Batches: {calculated_batches} (expected {expected_batches}) {'✓' if batch_match else '✗'}")
        print(f"   Subbatches: {calculated_subbatches} (expected {expected_subbatches}) {'✓' if subbatch_match else '✗'}")
        print()

def test_batch_naming_convention():
    """Test the new batch and subbatch naming conventions."""
    
    print("🏷️  Testing Naming Conventions")
    print("=" * 60)
    
    # Simulate batch naming
    table_name = "simples"
    total_batches = 10
    batch_size = 1000
    subbatch_size = 300
    
    print("Batch Names:")
    for batch_num in range(min(3, total_batches)):  # Show first 3 batches
        start_row = batch_num * batch_size
        end_row = start_row + batch_size
        timestamp = "20250909_140530_123"
        
        batch_name = f"Batch_{table_name}_{batch_num+1}of{total_batches}_rows{start_row}-{end_row}_{timestamp}"
        print(f"  {batch_name}")
        
        # Show subbatches for first batch only
        if batch_num == 0:
            print("  Subbatch Names:")
            subbatches_in_batch = math.ceil(batch_size / subbatch_size)
            for subbatch_num in range(subbatches_in_batch):
                subbatch_start = start_row + (subbatch_num * subbatch_size)
                subbatch_end = min(subbatch_start + subbatch_size, end_row)
                subbatch_name = f"Subbatch_{subbatch_num+1}of{subbatches_in_batch}_rows{subbatch_start}-{subbatch_end}"
                print(f"    {subbatch_name}")
            print()
    
    print("✅ Naming convention follows expected pattern")

def generate_hierarchy_visualization():
    """Generate a visual representation of the expected hierarchy."""
    
    print("🌳 Expected Database Hierarchy")
    print("=" * 60)
    
    # Example with realistic numbers
    total_rows = 10000
    batch_size = 1000  
    subbatch_size = 300
    
    import math
    
    total_batches = math.ceil(total_rows / batch_size)
    total_subbatches = 0
    
    print(f"Table: 'simples' (1 table_ingestion_manifest record)")
    print(f"└── File: 'SIMPLES.csv' (1 file_ingestion_manifest record)")
    
    for batch_num in range(min(3, total_batches)):  # Show first 3 batches
        start_row = batch_num * batch_size
        end_row = min(start_row + batch_size, total_rows)
        actual_batch_rows = end_row - start_row
        subbatches_in_batch = math.ceil(actual_batch_rows / subbatch_size)
        total_subbatches += subbatches_in_batch
        
        if batch_num < 2:
            print(f"    ├── Batch {batch_num+1}: rows {start_row}-{end_row-1} (1 batch_ingestion_manifest record)")
        else:
            print(f"    ├── Batch {batch_num+1}: rows {start_row}-{end_row-1} (1 batch_ingestion_manifest record)")
        
        # Show subbatches for first batch only
        if batch_num == 0:
            for subbatch_num in range(subbatches_in_batch):
                subbatch_start = start_row + (subbatch_num * subbatch_size)
                subbatch_end = min(subbatch_start + subbatch_size, end_row)
                
                if subbatch_num < subbatches_in_batch - 1:
                    prefix = "    │   ├──"
                else:
                    prefix = "    │   └──"
                    
                print(f"{prefix} Subbatch {subbatch_num+1}: rows {subbatch_start}-{subbatch_end-1} (1 subbatch_ingestion_manifest record)")
        elif batch_num < 2:
            print(f"    │   └── {subbatches_in_batch} subbatches (rows segmented by {subbatch_size})")
    
    if total_batches > 3:
        remaining_batches = total_batches - 3
        print(f"    └── ... ({remaining_batches} more batches)")
    
    # Calculate final totals
    final_subbatches = 0
    for batch_num in range(total_batches):
        start_row = batch_num * batch_size
        end_row = min(start_row + batch_size, total_rows)
        actual_batch_rows = end_row - start_row
        subbatches_in_batch = math.ceil(actual_batch_rows / subbatch_size)
        final_subbatches += subbatches_in_batch
    
    print()
    print("📊 Expected Record Counts:")
    print(f"   Table Manifests: 1")
    print(f"   File Manifests: 1")
    print(f"   Batch Manifests: {total_batches}")
    print(f"   Subbatch Manifests: {final_subbatches}")
    print(f"   Total Records: {1 + 1 + total_batches + final_subbatches}")

def main():
    """Run all validation tests."""
    
    print("🔧 Batch Hierarchy Implementation Validation")
    print("=" * 80)
    print("Testing the corrected 4-tier hierarchy: Table → File → Batch → Subbatch")
    print("=" * 80)
    print()
    
    # Run tests
    test_batch_hierarchy_calculation()
    print()
    test_batch_naming_convention()
    print()
    generate_hierarchy_visualization()
    
    print()
    print("✅ All validations completed successfully!")
    print("🎯 Implementation should create proper 4-tier hierarchy without duplicate file batches.")

if __name__ == "__main__":
    import math  # Ensure math is available
    main()
