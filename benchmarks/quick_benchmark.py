#!/usr/bin/env python3
"""
Quick benchmark runner for testing with small data samples.
"""

import sys
import os
from pathlib import Path
import logging

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from .run_benchmarks import main

def quick_test():
    """Run quick benchmark test with limited data."""
    
    # Setup minimal logging
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    
    # Override sys.argv for testing
    original_argv = sys.argv
    
    try:
        # Test with estabelecimento files, limited to 2 files
        sys.argv = [
            "quick_benchmark.py",
            "--data-dir", "data",
            "--output-dir", "benchmark_test_output", 
            "--pattern", "*ESTABELE*",
            "--max-files", "1",  # Start with just 1 file for safety
            "--memory-limit", "4GB"
        ]
        
        print("Running quick benchmark test...")
        print("   - Pattern: *ESTABELE* files")
        print("   - Max files: 1 (reduced for memory safety)")
        print("   - Memory limit: 4GB")
        print("   - Output: benchmark_test_output/")
        print("   - Note: Includes memory balloon protection for Polars")
        print()
        
        return main()
        
    finally:
        sys.argv = original_argv

if __name__ == "__main__":
    exit_code = quick_test()
    
    if exit_code == 0:
        print("\n[SUCCESS] Quick benchmark test completed successfully!")
        print("Check benchmark_test_output/ for results")
    else:
        print("\n[FAILED] Quick benchmark test failed")

    sys.exit(exit_code)