#!/usr/bin/env python3
"""
Memory leak diagnostic tool for CNPJ ETL pipeline.

Usage:
    python scripts/diagnose_memory_leak.py --test transform --table estabelecimento --rows 10000
    python scripts/diagnose_memory_leak.py --test copy --table socios --rows 5000
    python scripts/diagnose_memory_leak.py --test full --table empresa --rows 10000
    python scripts/diagnose_memory_leak.py --test all --table estabelecimento --rows 10000

Examples:
    # Test if transform is leaking memory
    python scripts/diagnose_memory_leak.py --test transform --table estabelecimento --rows 10000
    
    # Test if asyncpg COPY is leaking
    python scripts/diagnose_memory_leak.py --test copy --table socios --rows 5000
    
    # Full memory profile (recommended)
    python scripts/diagnose_memory_leak.py --test full --table estabelecimento --rows 10000
    
    # Run all tests
    python scripts/diagnose_memory_leak.py --test all --table estabelecimento --rows 5000
"""
import argparse
import asyncio
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.setup.logging import logger
from src.setup.config import get_config
from src.database.engine import create_database_instance
from src.database.models.business import MainBase
from src.database.utils import table_name_to_table_info
from lab.memory_leak_diagnostics import (
    test_transform_memory_leak,
    test_copy_memory_leak,
    test_full_batch_memory_profile
)


def generate_sample_batch(table_info, num_rows: int):
    """Generate a sample batch for testing."""
    import random
    import string
    
    def random_string(length=10):
        return ''.join(random.choices(string.ascii_letters + string.digits, k=length))
    
    def random_date():
        return f"{random.randint(2000, 2024)}{random.randint(1, 12):02d}{random.randint(1, 28):02d}"
    
    def random_cnpj():
        return ''.join(random.choices(string.digits, k=8))
    
    # Generate sample rows based on column count
    num_columns = len(table_info.columns)
    batch = []
    
    for _ in range(num_rows):
        row = []
        for col_idx, col_name in enumerate(table_info.columns):
            # Generate appropriate data based on column name
            if 'cnpj' in col_name.lower():
                row.append(random_cnpj())
            elif 'data' in col_name.lower() or 'date' in col_name.lower():
                row.append(random_date())
            elif 'numero' in col_name.lower() or 'codigo' in col_name.lower():
                row.append(str(random.randint(1, 9999)))
            else:
                row.append(random_string(random.randint(5, 20)))
        
        batch.append(tuple(row))
    
    logger.info(f"Generated sample batch: {num_rows} rows × {num_columns} columns")
    return batch


async def run_transform_test(table_name: str, num_rows: int):
    """Run transform memory leak test."""
    logger.info("=" * 60)
    logger.info("TRANSFORM MEMORY LEAK TEST")
    logger.info("=" * 60)
    
    # Get table info
    table_info = table_name_to_table_info(table_name)
    
    # Generate sample batch
    batch = generate_sample_batch(table_info, num_rows)
    
    # Run test
    mem_increase, mem_leaked, is_ok = await test_transform_memory_leak(
        table_info, batch, batch_num=1
    )
    
    # Report results
    logger.info("")
    logger.info("TRANSFORM TEST RESULTS:")
    logger.info(f"  Memory increase during transform: {mem_increase:.1f}MB")
    logger.info(f"  Memory leaked after GC: {mem_leaked:.1f}MB")
    logger.info(f"  Expected threshold: <10MB")
    logger.info(f"  Status: {'✅ PASS' if is_ok else '❌ FAIL - LEAK DETECTED'}")
    
    if not is_ok:
        logger.warning("")
        logger.warning("⚠️  Transform is leaking memory!")
        logger.warning("    Recommendation: Check apply_transforms_to_batch() in src/database/utils.py")
        logger.warning("    Should use in-place modification, not create new list")
    
    return is_ok


async def run_copy_test(table_name: str, num_rows: int):
    """Run asyncpg COPY memory leak test."""
    logger.info("=" * 60)
    logger.info("ASYNCPG COPY MEMORY LEAK TEST")
    logger.info("=" * 60)
    
    # Get config and database
    config = get_config()
    db_uri = config.pipeline.database.get_connection_string()
    database = create_database_instance(db_uri, MainBase)
    
    # Get table info
    table_info = table_name_to_table_info(table_name)
    
    # Generate sample batch
    batch = generate_sample_batch(table_info, num_rows)
    
    # Get connection pool
    pool = await database.get_async_pool()
    
    try:
        # Run test
        mem_increase, mem_leaked, is_ok = await test_copy_memory_leak(
            pool, table_info, batch, batch_num=1
        )
        
        # Report results
        logger.info("")
        logger.info("COPY TEST RESULTS:")
        logger.info(f"  Memory increase during COPY: {mem_increase:.1f}MB")
        logger.info(f"  Memory leaked after GC: {mem_leaked:.1f}MB")
        logger.info(f"  Expected threshold: <15MB")
        logger.info(f"  Status: {'✅ PASS' if is_ok else '❌ FAIL - LEAK DETECTED'}")
        
        if not is_ok:
            logger.warning("")
            logger.warning("⚠️  asyncpg COPY is leaking memory!")
            logger.warning("    Recommendation: Implement chunked COPY in service.py")
            logger.warning("    Split large batches into 5k-row chunks")
        
        return is_ok
        
    finally:
        await pool.close()


async def run_full_profile(table_name: str, num_rows: int):
    """Run full memory profile test."""
    logger.info("=" * 60)
    logger.info("FULL MEMORY PROFILE TEST")
    logger.info("=" * 60)
    
    # Get config and database
    config = get_config()
    db_uri = config.pipeline.database.get_connection_string()
    database = create_database_instance(db_uri, MainBase)
    
    # Get table info
    table_info = table_name_to_table_info(table_name)
    
    # Generate sample batch
    batch = generate_sample_batch(table_info, num_rows)
    
    # Get connection pool
    pool = await database.get_async_pool()
    
    try:
        # Run test
        profile = await test_full_batch_memory_profile(
            pool, table_info, batch, batch_num=1
        )
        
        # Report detailed results
        logger.info("")
        logger.info("FULL PROFILE RESULTS:")
        logger.info(f"  Batch size: {profile['row_count']} rows × {profile['column_count']} columns")
        logger.info("")
        logger.info("  Memory Breakdown:")
        logger.info(f"    Transform increase: +{profile['transform_delta']:.1f}MB")
        logger.info(f"    Delete original freed: {profile['delete_original_freed']:.1f}MB")
        logger.info(f"    COPY increase: +{profile['copy_delta']:.1f}MB")
        logger.info(f"    Delete transformed freed: {profile['delete_transformed_freed']:.1f}MB")
        logger.info(f"    Total leaked: {profile['total_leaked']:.1f}MB")
        logger.info("")
        logger.info("  Leak Analysis:")
        
        issues_found = False
        
        if profile['leak_source_analysis']['transform_creates_duplicate']:
            logger.warning("    ❌ Transform creates duplicate data (not in-place)")
            issues_found = True
        else:
            logger.info("    ✅ Transform modifies in-place")
        
        if profile['leak_source_analysis']['copy_buffers_excessively']:
            logger.warning("    ❌ COPY buffers excessively in memory")
            issues_found = True
        else:
            logger.info("    ✅ COPY memory usage reasonable")
        
        if profile['leak_source_analysis']['cleanup_incomplete']:
            logger.warning("    ❌ Cleanup incomplete - memory leaked after GC")
            issues_found = True
        else:
            logger.info("    ✅ Cleanup successful")
        
        if issues_found:
            logger.warning("")
            logger.warning("⚠️  Memory leaks detected! See recommendations above.")
        else:
            logger.info("")
            logger.info("✅ No significant memory leaks detected!")
        
        return not issues_found
        
    finally:
        await pool.close()


async def run_all_tests(table_name: str, num_rows: int):
    """Run all diagnostic tests."""
    logger.info("=" * 60)
    logger.info("RUNNING ALL MEMORY LEAK DIAGNOSTIC TESTS")
    logger.info("=" * 60)
    logger.info("")
    
    results = {}
    
    # Test 1: Transform
    logger.info("Test 1/3: Transform Memory Leak Test")
    results['transform'] = await run_transform_test(table_name, num_rows)
    logger.info("")
    
    # Test 2: COPY
    logger.info("Test 2/3: asyncpg COPY Memory Leak Test")
    results['copy'] = await run_copy_test(table_name, num_rows)
    logger.info("")
    
    # Test 3: Full Profile
    logger.info("Test 3/3: Full Memory Profile")
    results['full'] = await run_full_profile(table_name, num_rows)
    logger.info("")
    
    # Summary
    logger.info("=" * 60)
    logger.info("TEST SUMMARY")
    logger.info("=" * 60)
    logger.info(f"  Transform test: {'✅ PASS' if results['transform'] else '❌ FAIL'}")
    logger.info(f"  COPY test: {'✅ PASS' if results['copy'] else '❌ FAIL'}")
    logger.info(f"  Full profile: {'✅ PASS' if results['full'] else '❌ FAIL'}")
    logger.info("")
    
    all_passed = all(results.values())
    
    if all_passed:
        logger.info("🎉 All tests passed! No significant memory leaks detected.")
    else:
        logger.warning("⚠️  Some tests failed. Memory leaks detected.")
        logger.warning("    Review recommendations above and apply fixes.")
    
    return all_passed


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Diagnose memory leaks in CNPJ ETL pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    parser.add_argument(
        '--test',
        choices=['transform', 'copy', 'full', 'all'],
        required=True,
        help='Which test to run'
    )
    
    parser.add_argument(
        '--table',
        required=True,
        help='Table name to test (e.g., estabelecimento, socios, empresa)'
    )
    
    parser.add_argument(
        '--rows',
        type=int,
        default=10000,
        help='Number of rows to generate for testing (default: 10000)'
    )
    
    parser.add_argument(
        '--year',
        type=int,
        default=2025,
        help='Year for config (default: 2025)'
    )
    
    parser.add_argument(
        '--month',
        type=int,
        default=9,
        help='Month for config (default: 9)'
    )
    
    args = parser.parse_args()
    
    # Validate row count
    if args.rows < 100:
        logger.warning(f"Row count too small ({args.rows}), using minimum of 100")
        args.rows = 100
    elif args.rows > 100000:
        logger.warning(f"Row count too large ({args.rows}), using maximum of 100000")
        args.rows = 100000
    
    # Run selected test
    try:
        if args.test == 'transform':
            success = asyncio.run(run_transform_test(args.table, args.rows))
        elif args.test == 'copy':
            success = asyncio.run(run_copy_test(args.table, args.rows))
        elif args.test == 'full':
            success = asyncio.run(run_full_profile(args.table, args.rows))
        elif args.test == 'all':
            success = asyncio.run(run_all_tests(args.table, args.rows))
        
        sys.exit(0 if success else 1)
        
    except KeyboardInterrupt:
        logger.info("\nTest interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.exception(f"Test failed with error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
