#!/usr/bin/env python3
"""
Test script for blob-level development filtering.

This script tests the new blob-level filtering functionality 
to ensure it works as expected with the zip files.
"""

import sys
import os
from pathlib import Path

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.setup.config.models import DevelopmentConfig
from src.core.utils.development_filter import DevelopmentFilter


def test_blob_filtering():
    """Test blob-level filtering with actual zip files."""
    
    # Setup development config with 1GB limit
    dev_config = DevelopmentConfig(
        enabled=True,
        file_size_limit_mb=1000,  # 1GB limit
        max_files_per_table=50,
        max_files_per_blob=50,
        row_limit_percent=0.1
    )
    
    dev_filter = DevelopmentFilter(dev_config)
    
    # Get actual zip files from the downloaded data
    zip_dir = project_root / "data" / "DOWNLOADED_FILES" / "2025-09"
    if not zip_dir.exists():
        # Try alternative path structure
        zip_dir = Path("c:/Users/SuasVendas/github/scrapper-rf-cnpj/data/DOWNLOADED_FILES/2025-09")
        if not zip_dir.exists():
            print(f"❌ Zip directory not found: {zip_dir}")
            print(f"📁 Project root: {project_root}")
            print(f"📁 Looking for: data/DOWNLOADED_FILES/2025-09")
            return False
    
    zip_files = list(zip_dir.glob("*.zip"))
    if not zip_files:
        print(f"❌ No zip files found in: {zip_dir}")
        return False
    
    print(f"🔍 Testing blob filtering with {len(zip_files)} zip files")
    print(f"📊 File size limit: {dev_config.file_size_limit_mb}MB")
    print()
    
    # Show all file sizes grouped by table
    print("📁 File sizes grouped by table:")
    table_groups = {}
    for zip_file in sorted(zip_files):
        table_name = dev_filter._extract_table_name(zip_file.name)
        if table_name not in table_groups:
            table_groups[table_name] = []
        table_groups[table_name].append(zip_file)
    
    for table_name, files in sorted(table_groups.items()):
        total_size = sum(f.stat().st_size / (1024 * 1024) for f in files)
        status = "❌ EXCEEDS" if total_size > dev_config.file_size_limit_mb else "✅ OK"
        print(f"  {table_name:15} ({len(files):2} files): {total_size:8.2f}MB total {status}")
        for f in files:
            size_mb = f.stat().st_size / (1024 * 1024)
            print(f"    └─ {f.name:25} {size_mb:8.2f}MB")
    print()
    
    # Test blob-level filtering
    print("🧪 Testing blob-level filtering...")
    filtered_files = dev_filter.filter_files_by_blob_size_limit(zip_files, group_by_table=True)
    
    print(f"\n📊 Results:")
    print(f"  Original files: {len(zip_files)}")
    print(f"  Filtered files: {len(filtered_files)}")
    print(f"  Reduction: {len(zip_files) - len(filtered_files)} files excluded")
    
    print(f"\n✅ Included files:")
    for zip_file in sorted(filtered_files):
        size_mb = zip_file.stat().st_size / (1024 * 1024)
        table_name = dev_filter._extract_table_name(zip_file.name)
        print(f"  {zip_file.name:25} {size_mb:8.2f}MB (table: {table_name})")
    
    excluded_files = [f for f in zip_files if f not in filtered_files]
    if excluded_files:
        print(f"\n❌ Excluded files:")
        for zip_file in sorted(excluded_files):
            size_mb = zip_file.stat().st_size / (1024 * 1024)
            table_name = dev_filter._extract_table_name(zip_file.name)
            print(f"  {zip_file.name:25} {size_mb:8.2f}MB (table: {table_name})")
    
    # Verify expectations based on sum of file sizes
    print(f"\n🎯 Verification (sum-based filtering):")
    
    # Calculate total size for estabelecimentos table
    estabelecimentos_files = [f for f in zip_files if 'estabelecimentos' in f.name.lower()]
    estabelecimentos_included = [f for f in filtered_files if 'estabelecimentos' in f.name.lower()]
    estabelecimentos_total_mb = sum(f.stat().st_size / (1024 * 1024) for f in estabelecimentos_files)
    
    print(f"  Estabelecimentos files found: {len(estabelecimentos_files)}")
    print(f"  Estabelecimentos total size: {estabelecimentos_total_mb:.1f}MB")
    print(f"  Estabelecimentos files included: {len(estabelecimentos_included)}")
    
    # Check if estabelecimentos should be included or excluded based on total size
    should_be_excluded = estabelecimentos_total_mb > dev_config.file_size_limit_mb
    actually_excluded = len(estabelecimentos_included) == 0 and len(estabelecimentos_files) > 0
    
    if should_be_excluded and actually_excluded:
        print(f"  ✅ CORRECT: Estabelecimentos table excluded (total {estabelecimentos_total_mb:.1f}MB > {dev_config.file_size_limit_mb}MB)")
    elif not should_be_excluded and not actually_excluded:
        print(f"  ✅ CORRECT: Estabelecimentos table included (total {estabelecimentos_total_mb:.1f}MB ≤ {dev_config.file_size_limit_mb}MB)")
    else:
        print(f"  ❌ ERROR: Estabelecimentos filtering incorrect!")
        print(f"    Expected: {'exclude' if should_be_excluded else 'include'} (total {estabelecimentos_total_mb:.1f}MB)")
        print(f"    Actual: {'excluded' if actually_excluded else 'included'}")
        return False
    
    # Check empresas table
    empresas_files = [f for f in zip_files if 'empresas' in f.name.lower()]
    empresas_included = [f for f in filtered_files if 'empresas' in f.name.lower()]
    empresas_total_mb = sum(f.stat().st_size / (1024 * 1024) for f in empresas_files)
    
    print(f"  Empresas files found: {len(empresas_files)}")
    print(f"  Empresas total size: {empresas_total_mb:.1f}MB")
    print(f"  Empresas files included: {len(empresas_included)}")
    
    should_be_excluded = empresas_total_mb > dev_config.file_size_limit_mb
    actually_excluded = len(empresas_included) == 0 and len(empresas_files) > 0
    
    if should_be_excluded and actually_excluded:
        print(f"  ✅ CORRECT: Empresas table excluded (total {empresas_total_mb:.1f}MB > {dev_config.file_size_limit_mb}MB)")
    elif not should_be_excluded and not actually_excluded:
        print(f"  ✅ CORRECT: Empresas table included (total {empresas_total_mb:.1f}MB ≤ {dev_config.file_size_limit_mb}MB)")
    else:
        print(f"  ❌ ERROR: Empresas filtering incorrect!")
        print(f"    Expected: {'exclude' if should_be_excluded else 'include'} (total {empresas_total_mb:.1f}MB)")
        print(f"    Actual: {'excluded' if actually_excluded else 'included'}")
        return False
    
    # Check socios table
    socios_files = [f for f in zip_files if 'socios' in f.name.lower()]
    socios_included = [f for f in filtered_files if 'socios' in f.name.lower()]
    socios_total_mb = sum(f.stat().st_size / (1024 * 1024) for f in socios_files)
    
    print(f"  Socios files found: {len(socios_files)}")
    print(f"  Socios total size: {socios_total_mb:.1f}MB")  
    print(f"  Socios files included: {len(socios_included)}")
    
    should_be_excluded = socios_total_mb > dev_config.file_size_limit_mb
    actually_excluded = len(socios_included) == 0 and len(socios_files) > 0
    
    if should_be_excluded and actually_excluded:
        print(f"  ✅ CORRECT: Socios table excluded (total {socios_total_mb:.1f}MB > {dev_config.file_size_limit_mb}MB)")
    elif not should_be_excluded and not actually_excluded:
        print(f"  ✅ CORRECT: Socios table included (total {socios_total_mb:.1f}MB ≤ {dev_config.file_size_limit_mb}MB)")
    else:
        print(f"  ❌ ERROR: Socios filtering incorrect!")
        print(f"    Expected: {'exclude' if should_be_excluded else 'include'} (total {socios_total_mb:.1f}MB)")
        print(f"    Actual: {'excluded' if actually_excluded else 'included'}")
        return False
    
    print(f"\n🎉 Blob filtering test PASSED!")
    return True


def test_table_name_extraction():
    """Test the table name extraction logic."""
    print("🧪 Testing table name extraction...")
    
    dev_config = DevelopmentConfig(enabled=True)
    dev_filter = DevelopmentFilter(dev_config)
    
    test_cases = [
        ("Estabelecimentos0.zip", "estabelecimentos"),
        ("Estabelecimentos9.zip", "estabelecimentos"),
        ("Empresas5.zip", "empresas"),
        ("Socios3.zip", "socios"),
        ("Simples.zip", "simples"),
        ("Cnaes.zip", "cnaes"),
        ("Motivos.zip", "motivos"),
        ("empresa.csv", "empresas"),
        ("estabelecimento1.parquet", "estabelecimentos"),
    ]
    
    all_passed = True
    for filename, expected in test_cases:
        result = dev_filter._extract_table_name(filename)
        status = "✅" if result == expected else "❌"
        print(f"  {filename:25} → {result:15} (expected: {expected:15}) {status}")
        if result != expected:
            all_passed = False
    
    if all_passed:
        print("🎉 Table name extraction test PASSED!")
    else:
        print("❌ Table name extraction test FAILED!")
    
    return all_passed


if __name__ == "__main__":
    print("🚀 Testing Blob-Level Development Filtering")
    print("=" * 50)
    
    success = True
    
    # Test table name extraction first
    success &= test_table_name_extraction()
    print()
    
    # Test blob filtering
    success &= test_blob_filtering()
    
    if success:
        print(f"\n🎉 ALL TESTS PASSED!")
        print(f"✅ Blob-level filtering is working correctly")
        print(f"✅ ETL_DEV_FILE_SIZE_LIMIT_MB=1000 filters by SUM of file sizes per table group")
        print(f"   - Tables with total size ≤ 1000MB: INCLUDED")
        print(f"   - Tables with total size > 1000MB: EXCLUDED")
        sys.exit(0)
    else:
        print(f"\n❌ SOME TESTS FAILED!")
        sys.exit(1)