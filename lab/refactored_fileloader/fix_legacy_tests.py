#!/usr/bin/env python3
"""
Script to fix legacy test imports after ETL refactoring.
Updates all test files to use the new consolidated ingestors module.
"""

import os
import re
from pathlib import Path

def fix_test_file(file_path):
    """Fix imports and function calls in a test file."""
    print(f"Fixing {file_path}...")
    
    with open(file_path, 'r') as f:
        content = f.read()
    
    original_content = content
    
    # Fix imports
    content = re.sub(
        r'from src\.csv_ingestor import batch_generator',
        'from src.ingestors import batch_generator_csv',
        content
    )
    
    content = re.sub(
        r'from src\.parquet_ingestor import batch_generator',
        'from src.ingestors import batch_generator_parquet', 
        content
    )
    
    # Fix function calls - be careful to only replace the right ones
    # Replace batch_generator( with batch_generator_csv( when it's a CSV test
    if 'csv' in file_path.name.lower():
        content = re.sub(
            r'\bbatch_generator\(',
            'batch_generator_csv(',
            content
        )
    elif 'parquet' in file_path.name.lower():
        content = re.sub(
            r'\bbatch_generator\(',
            'batch_generator_parquet(',
            content
        )
    else:
        # For uploader tests and others, use CSV by default
        content = re.sub(
            r'\bbatch_generator,',
            'batch_generator_csv,',
            content
        )
        content = re.sub(
            r'\bbatch_generator\)',
            'batch_generator_csv)',
            content
        )
    
    # Fix primary key parameters (should be lists)
    content = re.sub(
        r"'id',\s*batch_generator",
        "['id'], batch_generator",
        content
    )
    
    # Write back if changed
    if content != original_content:
        with open(file_path, 'w') as f:
            f.write(content)
        print(f"  ✅ Updated {file_path}")
    else:
        print(f"  ⚪ No changes needed for {file_path}")

def main():
    """Fix all test files in the tests directory."""
    tests_dir = Path('tests')
    
    test_files = [
        f for f in tests_dir.rglob('*.py') 
        if f.name.startswith('test_') and f.name != '__init__.py'
    ]
    
    print(f"Found {len(test_files)} test files to check...")
    
    for test_file in test_files:
        try:
            fix_test_file(test_file)
        except Exception as e:
            print(f"❌ Error fixing {test_file}: {e}")
    
    print("\n🎉 Legacy test import fixing complete!")

if __name__ == '__main__':
    main()
