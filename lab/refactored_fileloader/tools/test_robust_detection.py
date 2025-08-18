#!/usr/bin/env python3
"""
Test script for robust FileLoader format detection capabilities.
"""

import os
import tempfile
from src.file_loader import FileLoader

def test_robust_detection():
    """Test the enhanced robust detection features."""
    
    print("🧪 Testing Robust FileLoader Detection")
    print("=" * 50)
    
    # Test 1: Valid files
    print("\n1. Testing valid file detection:")
    test_files = [
        ('data/sample_1.csv', 'csv'),
        ('data/sample_1.parquet', 'parquet'),
    ]
    
    for file_path, expected in test_files:
        try:
            detected = FileLoader.detect_file_format(file_path)
            status = "✅" if detected == expected else "❌"
            print(f"   {status} {file_path}: {detected} (expected: {expected})")
        except Exception as e:
            print(f"   ❌ {file_path}: ERROR - {e}")
    
    # Test 2: Error handling
    print("\n2. Testing error handling:")
    error_cases = [
        ('nonexistent.csv', 'File not found'),
        ('src/', 'Directory instead of file'),
    ]
    
    for file_path, expected_error in error_cases:
        try:
            detected = FileLoader.detect_file_format(file_path)
            print(f"   ❌ {file_path}: Unexpected success - {detected}")
        except Exception as e:
            print(f"   ✅ {file_path}: Expected error - {str(e)[:60]}...")
    
    # Test 3: Content validation
    print("\n3. Testing content validation with mock files:")
    
    # Create temporary files for testing
    with tempfile.TemporaryDirectory() as temp_dir:
        # Test CSV content validation
        csv_file = os.path.join(temp_dir, "test.csv")
        with open(csv_file, 'w') as f:
            f.write("id,name,value\n1,Alice,100\n2,Bob,200\n")
        
        try:
            detected = FileLoader.detect_file_format(csv_file)
            print(f"   ✅ Valid CSV content: {detected}")
        except Exception as e:
            print(f"   ❌ Valid CSV content: ERROR - {e}")
        
        # Test invalid "CSV" file
        fake_csv_file = os.path.join(temp_dir, "fake.csv")
        with open(fake_csv_file, 'wb') as f:
            f.write(b'\x89PNG\r\n\x1a\n')  # PNG header
        
        try:
            detected = FileLoader.detect_file_format(fake_csv_file)
            print(f"   ❌ Fake CSV file: Unexpected success - {detected}")
        except Exception as e:
            print(f"   ✅ Fake CSV file: Expected error - {str(e)[:60]}...")
    
    # Test 4: Batch generator retrieval
    print("\n4. Testing batch generator retrieval:")
    try:
        csv_gen = FileLoader.get_batch_generator_for_file('data/sample_1.csv')
        parquet_gen = FileLoader.get_batch_generator_for_file('data/sample_1.parquet')
        print(f"   ✅ CSV batch generator: {csv_gen.__name__}")
        print(f"   ✅ Parquet batch generator: {parquet_gen.__name__}")
    except Exception as e:
        print(f"   ❌ Batch generator retrieval: ERROR - {e}")
    
    # Test 5: FileLoader instance creation
    print("\n5. Testing FileLoader instance creation:")
    try:
        csv_loader = FileLoader('data/sample_1.csv')
        parquet_loader = FileLoader('data/sample_1.parquet')
        print(f"   ✅ CSV loader format: {csv_loader.get_format()}")
        print(f"   ✅ Parquet loader format: {parquet_loader.get_format()}")
    except Exception as e:
        print(f"   ❌ FileLoader creation: ERROR - {e}")
    
    print("\n🎉 Robust detection testing complete!")

if __name__ == "__main__":
    test_robust_detection()
