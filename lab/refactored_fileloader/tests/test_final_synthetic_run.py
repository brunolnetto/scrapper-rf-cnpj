import pytest
import tempfile
import os
from src.base import create_pool
from src.csv_ingestor import batch_generator
from src.uploader import async_upsert

@pytest.mark.asyncio
async def test_final_synthetic_run():
    """
    Test comprehensive upsert behavior:
    1. INSERT: First upsert on empty table (all records are new)
    2. UPDATE: Second upsert with modified data (updates existing records)
    3. INSERT: Second upsert also adds new records
    This verifies the complete UPSERT = INSERT + UPDATE functionality.
    """
    dsn = 'postgresql://testuser:testpass@localhost:5433/testdb'
    pool = await create_pool(dsn)
    conn = await pool.acquire()
    await conn.execute('DROP TABLE IF EXISTS synthetic_table')
    await conn.execute('CREATE TABLE synthetic_table (id TEXT PRIMARY KEY, val TEXT)')
    await conn.execute('DROP TABLE IF EXISTS ingestion_manifest')
    await conn.execute('''
        CREATE TABLE ingestion_manifest (
            filename TEXT PRIMARY KEY,
            checksum BYTEA,
            filesize BIGINT,
            processed_at TIMESTAMP WITH TIME ZONE,
            rows BIGINT,
            status TEXT,
            run_id TEXT,
            notes TEXT
        )
    ''')
    await pool.release(conn)

    # Generate synthetic CSV data
    headers = ['id', 'val']
    rows = [{'id': str(i), 'val': f'value_{i}'} for i in range(1, 101)]
    with tempfile.NamedTemporaryFile('w+', newline='', encoding='utf-8', delete=False) as f:
        import csv
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
        f.flush()
        file_path = f.name

    # PHASE 1: First upsert (INSERT scenario - empty table)
    await async_upsert(
        pool,
        file_path,
        headers,
        'synthetic_table',
        'id',
        batch_generator,
        max_retries=2,
        run_id='synthetic_run_insert'
    )

    # Verify initial insert results
    conn = await pool.acquire()
    result = await conn.fetch('SELECT * FROM synthetic_table ORDER BY CAST(id AS INTEGER)')
    assert len(result) == 100
    for i, row in enumerate(result, 1):
        assert row['id'] == str(i)
        assert row['val'] == f'value_{i}'
    
    # Check manifest for first run
    manifest = await conn.fetchrow('SELECT * FROM ingestion_manifest WHERE filename = $1', os.path.basename(file_path))
    assert manifest['status'] == 'success'
    assert manifest['run_id'] == 'synthetic_run_insert'
    await pool.release(conn)

    # PHASE 2: Create updated data (UPDATE + INSERT scenario)
    # Update existing records (ids 1-50) with new values
    # Add new records (ids 101-110)
    updated_rows = []
    # Update existing records
    for i in range(1, 51):
        updated_rows.append({'id': str(i), 'val': f'updated_value_{i}'})
    # Keep some unchanged (ids 51-100)
    for i in range(51, 101):
        updated_rows.append({'id': str(i), 'val': f'value_{i}'})
    # Add new records
    for i in range(101, 111):
        updated_rows.append({'id': str(i), 'val': f'new_value_{i}'})

    # Create second file with updated/new data
    with tempfile.NamedTemporaryFile('w+', newline='', encoding='utf-8', delete=False) as f2:
        import csv
        writer = csv.DictWriter(f2, fieldnames=headers)
        writer.writeheader()
        for row in updated_rows:
            writer.writerow(row)
        f2.flush()
        file_path_2 = f2.name

    # PHASE 3: Second upsert (UPDATE + INSERT scenario)
    await async_upsert(
        pool,
        file_path_2,
        headers,
        'synthetic_table',
        'id',
        batch_generator,
        max_retries=2,
        run_id='synthetic_run_upsert'
    )

    # Verify final results after upsert
    conn = await pool.acquire()
    final_result = await conn.fetch('SELECT * FROM synthetic_table ORDER BY CAST(id AS INTEGER)')
    assert len(final_result) == 110  # 100 original + 10 new records
    
    # Verify updates (ids 1-50 should have updated values)
    for i in range(1, 51):
        row = next(r for r in final_result if r['id'] == str(i))
        assert row['val'] == f'updated_value_{i}', f"Row {i} should be updated"
    
    # Verify unchanged (ids 51-100 should have original values)
    for i in range(51, 101):
        row = next(r for r in final_result if r['id'] == str(i))
        assert row['val'] == f'value_{i}', f"Row {i} should be unchanged"
    
    # Verify new inserts (ids 101-110)
    for i in range(101, 111):
        row = next(r for r in final_result if r['id'] == str(i))
        assert row['val'] == f'new_value_{i}', f"Row {i} should be newly inserted"

    # Check manifest for second run
    manifest2 = await conn.fetchrow('SELECT * FROM ingestion_manifest WHERE filename = $1', os.path.basename(file_path_2))
    assert manifest2['status'] == 'success'
    assert manifest2['run_id'] == 'synthetic_run_upsert'
    
    await pool.release(conn)
    await pool.close()
    os.remove(file_path)
    os.remove(file_path_2)
