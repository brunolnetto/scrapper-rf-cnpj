# File Detection Examples

This directory contains examples demonstrating the comprehensive file type detection system.

## Available Examples

### 1. `comprehensive_file_detection_demo.py`
**Complete demonstration of all file detection capabilities**

This is the main demo that shows:
- ✅ Basic file type detection with confidence scoring
- ✅ 100% certainty verification for production use
- ✅ Production utility functions (`is_csv_file_certain`, `batch_verify_files`, etc.)
- ✅ Synthetic file testing with edge cases
- ✅ Detailed analysis and reporting

**Usage:**
```bash
python examples/comprehensive_file_detection_demo.py
```

**Key Functions:**
- `verify_file_type_100_percent()` - Production-ready 100% certainty verification
- `is_csv_file_certain()` - Quick CSV certainty check
- `is_parquet_file_certain()` - Quick Parquet certainty check
- `batch_verify_files()` - Verify multiple files at once
- `get_certain_file_type()` - Get type only if certain

### 2. `loading_example.py`
**Data loading integration example**

Shows how to integrate the file detection system with data loading pipelines.

### 3. `parquet_loading_example.py`
**Parquet-specific loading example**

Demonstrates working with Parquet files using the detection system.

## Quick Start

```python
# Import production utilities
from examples.comprehensive_file_detection_demo import (
    verify_file_type_100_percent,
    is_csv_file_certain,
    batch_verify_files
)

# Simple certainty check
if is_csv_file_certain('data.csv'):
    print("✅ Definitely a CSV file")

# Full verification with details
is_certain, file_type, details = verify_file_type_100_percent('data.csv')
if is_certain:
    print(f"100% certain: {file_type} with {details['confidence']:.1%} confidence")

# Batch processing
results = batch_verify_files(['file1.csv', 'file2.parquet'])
print(f"Verified {len(results['certain_files'])} files with certainty")
```

## Features

- **95-100% confidence scoring** with evidence collection
- **Magic number detection** for Parquet files (PAR1 signature)
- **Content structure validation** for CSV files
- **CNPJ-specific pattern recognition** for Brazilian government files
- **Multiple encoding support** (UTF-8, Latin1, CP1252)
- **4-layer verification system** for maximum certainty
- **Production-ready utilities** with error handling

## Confidence Levels

| Confidence | Certainty | Recommended Use |
|------------|-----------|-----------------|
| 95-100% | High | ✅ Production - 100% reliable |
| 85-94% | Medium | ⚠️ Development - Very reliable |
| 70-84% | Medium | ⚠️ With manual verification |
| < 70% | Low | ❌ Not recommended |

## For Production Use

Use the production utilities for maximum reliability:

```python
# Only process files we're 100% certain about
if is_csv_file_certain('data.csv'):
    df = pd.read_csv('data.csv', sep=';')  # Use detected separator
else:
    handle_uncertain_file('data.csv')
```
