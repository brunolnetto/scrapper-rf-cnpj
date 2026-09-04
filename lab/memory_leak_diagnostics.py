"""
Diagnostic tests for catastrophic memory leak investigation.
Run these to identify the exact source of the 4.5GB memory explosion.
"""
import asyncio
import uuid
from typing import Tuple, Optional
from pathlib import Path

from src.setup.logging import logger
from src.setup.config.loader import ConfigLoader
from src.database.engine import Database
from src.database.schemas import TableInfo
from src.database.models.audit import AuditStatus


async def test_transform_memory_leak(
    table_info: TableInfo, 
    batch_chunk, 
    batch_num: int
) -> Tuple[float, float, float]:
    """
    Test #1: Check if apply_transforms_to_batch is leaking memory.
    
    Returns: (mem_increase_during, mem_leaked_after_gc, expected_ok)
    """
    import psutil
    import gc
    
    process = psutil.Process()
    mem_before = process.memory_info().rss / 1024 / 1024
    
    # Apply transform
    from src.database.utils import apply_transforms_to_batch
    transformed = apply_transforms_to_batch(table_info, batch_chunk, table_info.columns)
    
    mem_after_transform = process.memory_info().rss / 1024 / 1024
    mem_increase = mem_after_transform - mem_before
    
    # Cleanup
    del batch_chunk
    del transformed
    gc.collect()
    
    mem_after_gc = process.memory_info().rss / 1024 / 1024
    mem_leaked = mem_after_gc - mem_before
    
    # Expected: Should leak <10MB (minor Python overhead)
    leak_is_ok = mem_leaked < 10.0
    
    logger.info(
        f"[MEM-TEST-TRANSFORM] Batch {batch_num}: "
        f"before={mem_before:.1f}MB, "
        f"increase={mem_increase:.1f}MB, "
        f"leaked_after_gc={mem_leaked:.1f}MB "
        f"{'✓ OK' if leak_is_ok else '✗ LEAK DETECTED'}"
    )
    
    return mem_increase, mem_leaked, leak_is_ok


async def test_copy_memory_leak(
    pool,
    table_info: TableInfo,
    batch_chunk,
    batch_num: int
) -> Tuple[float, float, float]:
    """
    Test #2: Check if asyncpg copy_records_to_table is leaking memory.
    
    Returns: (mem_increase_during, mem_leaked_after_gc, expected_ok)
    """
    import psutil
    import gc
    
    process = psutil.Process()
    mem_before = process.memory_info().rss / 1024 / 1024
    
    conn = await pool.acquire()
    try:
        # Create temp table
        tmp_table = f"tmp_memtest_{batch_num}_{uuid.uuid4().hex[:8]}"
        headers = table_info.columns
        
        from src.database import utils as base
        types_map = base.map_types(headers, getattr(table_info, 'types', {}))
        await conn.execute(base.create_temp_table_sql(tmp_table, headers, types_map))
        
        # Perform COPY
        await conn.copy_records_to_table(tmp_table, records=batch_chunk, columns=headers)
        
        mem_after_copy = process.memory_info().rss / 1024 / 1024
        mem_increase = mem_after_copy - mem_before
        
        # Cleanup
        await conn.execute(f'DROP TABLE IF EXISTS {base.quote_ident(tmp_table)};')
        del batch_chunk
        gc.collect()
        
        mem_after_gc = process.memory_info().rss / 1024 / 1024
        mem_leaked = mem_after_gc - mem_before
        
        # Expected: Should leak <15MB (asyncpg connection overhead)
        leak_is_ok = mem_leaked < 15.0
        
        logger.info(
            f"[MEM-TEST-COPY] Batch {batch_num}: "
            f"before={mem_before:.1f}MB, "
            f"increase={mem_increase:.1f}MB, "
            f"leaked_after_gc={mem_leaked:.1f}MB "
            f"{'✓ OK' if leak_is_ok else '✗ LEAK DETECTED'}"
        )
        
        return mem_increase, mem_leaked, leak_is_ok
        
    finally:
        await pool.release(conn)


async def test_full_batch_memory_profile(
    pool,
    table_info: TableInfo,
    batch_chunk,
    batch_num: int
) -> dict:
    """
    Test #3: Full memory profile of entire batch processing.
    
    Returns: dict with detailed memory breakdown
    """
    import psutil
    import gc
    
    process = psutil.Process()
    
    profile = {
        'batch_num': batch_num,
        'row_count': len(batch_chunk),
        'column_count': len(table_info.columns)
    }
    
    # Baseline
    gc.collect()
    profile['mem_baseline'] = process.memory_info().rss / 1024 / 1024
    
    # After transform
    from src.database.utils import apply_transforms_to_batch
    transformed = apply_transforms_to_batch(table_info, batch_chunk, table_info.columns)
    profile['mem_after_transform'] = process.memory_info().rss / 1024 / 1024
    profile['transform_delta'] = profile['mem_after_transform'] - profile['mem_baseline']
    
    # Delete original batch
    del batch_chunk
    gc.collect()
    profile['mem_after_delete_original'] = process.memory_info().rss / 1024 / 1024
    profile['delete_original_freed'] = profile['mem_after_transform'] - profile['mem_after_delete_original']
    
    # Prepare for COPY
    conn = await pool.acquire()
    try:
        tmp_table = f"tmp_profile_{batch_num}_{uuid.uuid4().hex[:8]}"
        headers = table_info.columns
        
        from src.database import utils as base
        types_map = base.map_types(headers, getattr(table_info, 'types', {}))
        await conn.execute(base.create_temp_table_sql(tmp_table, headers, types_map))
        
        # Before COPY
        profile['mem_before_copy'] = process.memory_info().rss / 1024 / 1024
        
        # During COPY (asyncpg buffers in memory)
        await conn.copy_records_to_table(tmp_table, records=transformed, columns=headers)
        
        profile['mem_after_copy'] = process.memory_info().rss / 1024 / 1024
        profile['copy_delta'] = profile['mem_after_copy'] - profile['mem_before_copy']
        
        # Delete transformed batch
        del transformed
        gc.collect()
        profile['mem_after_delete_transformed'] = process.memory_info().rss / 1024 / 1024
        profile['delete_transformed_freed'] = profile['mem_after_copy'] - profile['mem_after_delete_transformed']
        
        # Cleanup temp table
        await conn.execute(f'DROP TABLE IF EXISTS {base.quote_ident(tmp_table)};')
        gc.collect()
        
        profile['mem_final'] = process.memory_info().rss / 1024 / 1024
        profile['total_leaked'] = profile['mem_final'] - profile['mem_baseline']
        
        # Analysis
        profile['leak_source_analysis'] = {
            'transform_creates_duplicate': profile['delete_original_freed'] < profile['transform_delta'] * 0.5,
            'copy_buffers_excessively': profile['copy_delta'] > profile['transform_delta'],
            'cleanup_incomplete': profile['total_leaked'] > 20.0,
        }
        
    finally:
        await pool.release(conn)
    
    logger.info(
        f"[MEM-PROFILE] Batch {batch_num} ({profile['row_count']} rows):\n"
        f"  Transform: +{profile['transform_delta']:.1f}MB\n"
        f"  Delete original freed: {profile['delete_original_freed']:.1f}MB\n"
        f"  COPY: +{profile['copy_delta']:.1f}MB\n"
        f"  Delete transformed freed: {profile['delete_transformed_freed']:.1f}MB\n"
        f"  Total leaked: {profile['total_leaked']:.1f}MB\n"
        f"  Analysis: {profile['leak_source_analysis']}"
    )
    
    return profile


async def diagnostic_batch_processor(
    batch_chunk,
    pool,
    table_info: TableInfo,
    table_name: str,
    batch_num: int,
    **kwargs
) -> Tuple[bool, Optional[str], int]:
    """
    Diagnostic replacement for _async_process_batch_with_context.
    
    Use this to run memory leak tests during actual ETL processing:
    
    1. Comment out call to _async_process_batch_with_context in service.py
    2. Replace with call to this function
    3. Run ETL and watch for "[MEM-TEST]" log entries
    """
    rows = len(batch_chunk)
    
    # Choose which test to run:
    # await test_transform_memory_leak(table_info, batch_chunk, batch_num)
    # await test_copy_memory_leak(pool, table_info, batch_chunk, batch_num)
    profile = await test_full_batch_memory_profile(pool, table_info, batch_chunk, batch_num)
    
    # Return success (no actual processing, just diagnostics)
    return True, None, rows


# =============================================================================
# USAGE INSTRUCTIONS
# =============================================================================
"""
To diagnose the catastrophic memory leak:

1. In src/core/services/loading/service.py, find _async_load_single_file()
   
2. Replace the call to _async_process_batch_with_context with:
   
   from lab.memory_leak_diagnostics import diagnostic_batch_processor
   
   success, error, rows = await diagnostic_batch_processor(
       batch_chunk=batch_chunk,
       pool=pool,
       table_info=table_info,
       table_name=table_name,
       batch_num=batch_num,
       file_manifest_id=file_manifest_id,
       table_manifest_id=table_manifest_id,
       batch_id=batch_id,
       recommendations=recommendations
   )

3. Run ETL on a small file (1-2 batches)

4. Check logs for:
   - [MEM-TEST-TRANSFORM] - If leaked >10MB, transform is the problem
   - [MEM-TEST-COPY] - If leaked >15MB, asyncpg COPY is the problem  
   - [MEM-PROFILE] - Detailed breakdown showing exact leak source

5. Based on results:
   - If transform leaks: apply_transforms_to_batch needs in-place modification
   - If COPY leaks: need chunked COPY implementation
   - If both leak: need both fixes

6. After fixes applied, re-run diagnostics to confirm leak is fixed
"""
