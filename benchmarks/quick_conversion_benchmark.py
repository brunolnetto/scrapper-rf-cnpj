#!/usr/bin/env python3
"""
Quick conversion-focused benchmark test.
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from .conversion_benchmark import main

def quick_conversion_test():
    """Run quick conversion benchmark test."""
    
    # Override sys.argv for testing
    original_argv = sys.argv
    
    try:
        sys.argv = [
            "quick_conversion_benchmark.py",
            "--data-dir", "data",
            "--output-dir", "conversion_test_output", 
            "--pattern", "*ESTABELE*",
            "--max-files", "1",  # Test with just 1 file
            "--memory-limit", "4GB"
        ]
        
        print("🔄 Running quick conversion benchmark...")
        print("   - Tests: Existing Polars vs DuckDB conversion")
        print("   - Pattern: *ESTABELE* files")
        print("   - Max files: 1")
        print("   - Memory limit: 4GB")
        print("   - Output: conversion_test_output/")
        print()
        
        return main()
        
    finally:
        sys.argv = original_argv

if __name__ == "__main__":
    exit_code = quick_conversion_test()
    
    if exit_code == 0:
        print("\n[SUCCESS] Quick conversion benchmark completed!")
        print("Check conversion_test_output/ for results")
    else:
        print("\n[FAILED] Quick conversion benchmark failed")
    
    sys.exit(exit_code)