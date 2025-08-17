import pytest
import tempfile
import os
from src.base import create_pool
from src.csv_ingestor import batch_generator
from src.uploader import async_upsert

@pytest.mark.asyncio
async def test_upsert_insert_then_update():
    """Test that upsert correctly handles INSERT (new records) and UPDATE (existing records)."""
    dsn = 'postgresql://testuser:testpass@localhost:5433/testdb'
    pool = await create_pool(dsn)
    conn = await pool.acquire()
    
    # Setup test table
    await conn.execute('DROP TABLE IF EXISTS upsert_test')
    await conn.execute('CREATE TABLE upsert_test (id TEXT PRIMARY KEY, name TEXT, status TEXT)')
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

    # PHASE 1: Initial insert - 3 records
    headers = ['id', 'name', 'status']
    initial_data = [
        {'id': 'A', 'name': 'Alice', 'status': 'active'},
        {'id': 'B', 'name': 'Bob', 'status': 'inactive'},
        {'id': 'C', 'name': 'Charlie', 'status': 'active'}
    ]
    
    with tempfile.NamedTemporaryFile('w+', newline='', encoding='utf-8', delete=False) as f:
        import csv
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for row in initial_data:
            writer.writerow(row)
        f.flush()
        file1_path = f.name

    await async_upsert(
        pool, file1_path, headers, 'upsert_test', 'id',
        batch_generator, max_retries=2, run_id='insert_phase'
    )

    # Verify initial insert
    conn = await pool.acquire()
    result = await conn.fetch('SELECT * FROM upsert_test ORDER BY id')
    assert len(result) == 3
    assert result[0]['id'] == 'A' and result[0]['name'] == 'Alice' and result[0]['status'] == 'active'
    assert result[1]['id'] == 'B' and result[1]['name'] == 'Bob' and result[1]['status'] == 'inactive'
    assert result[2]['id'] == 'C' and result[2]['name'] == 'Charlie' and result[2]['status'] == 'active'
    await pool.release(conn)

    # PHASE 2: Upsert with updates and new records
    updated_data = [
        {'id': 'A', 'name': 'Alice', 'status': 'inactive'},  # UPDATE: status changed
        {'id': 'B', 'name': 'Robert', 'status': 'active'},   # UPDATE: name and status changed
        {'id': 'C', 'name': 'Charlie', 'status': 'active'},  # NO CHANGE: same values
        {'id': 'D', 'name': 'Diana', 'status': 'active'},    # INSERT: new record
        {'id': 'E', 'name': 'Edward', 'status': 'pending'}   # INSERT: new record
    ]
    
    with tempfile.NamedTemporaryFile('w+', newline='', encoding='utf-8', delete=False) as f:
        import csv
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for row in updated_data:
            writer.writerow(row)
        f.flush()
        file2_path = f.name

    await async_upsert(
        pool, file2_path, headers, 'upsert_test', 'id',
        batch_generator, max_retries=2, run_id='upsert_phase'
    )

    # Verify final state after upsert
    conn = await pool.acquire()
    final_result = await conn.fetch('SELECT * FROM upsert_test ORDER BY id')
    assert len(final_result) == 5  # 3 original + 2 new

    # Verify updates
    alice = next(r for r in final_result if r['id'] == 'A')
    assert alice['name'] == 'Alice' and alice['status'] == 'inactive'  # status updated
    
    bob = next(r for r in final_result if r['id'] == 'B')
    assert bob['name'] == 'Robert' and bob['status'] == 'active'  # name and status updated
    
    charlie = next(r for r in final_result if r['id'] == 'C')
    assert charlie['name'] == 'Charlie' and charlie['status'] == 'active'  # unchanged
    
    # Verify new inserts
    diana = next(r for r in final_result if r['id'] == 'D')
    assert diana['name'] == 'Diana' and diana['status'] == 'active'
    
    edward = next(r for r in final_result if r['id'] == 'E')
    assert edward['name'] == 'Edward' and edward['status'] == 'pending'

    await pool.release(conn)
    await pool.close()
    os.remove(file1_path)
    os.remove(file2_path)


@pytest.mark.asyncio
async def test_upsert_duplicate_keys_within_batch():
    """Test upsert behavior when the same key appears multiple times in a single batch."""
    dsn = 'postgresql://testuser:testpass@localhost:5433/testdb'
    pool = await create_pool(dsn)
    conn = await pool.acquire()
    
    # Setup test table
    await conn.execute('DROP TABLE IF EXISTS duplicate_test')
    await conn.execute('CREATE TABLE duplicate_test (id TEXT PRIMARY KEY, value TEXT)')
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

    # Create data with duplicate keys in the same file (last one should win)
    headers = ['id', 'value']
    duplicate_data = [
        {'id': 'X', 'value': 'first'},
        {'id': 'Y', 'value': 'unique'},
        {'id': 'X', 'value': 'second'},  # Same id as first row
        {'id': 'X', 'value': 'final'}    # Same id again - this should be the final value
    ]
    
    with tempfile.NamedTemporaryFile('w+', newline='', encoding='utf-8', delete=False) as f:
        import csv
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for row in duplicate_data:
            writer.writerow(row)
        f.flush()
        file_path = f.name

    await async_upsert(
        pool, file_path, headers, 'duplicate_test', 'id',
        batch_generator, max_retries=2, run_id='duplicate_test'
    )

    # Verify results - last value should win for duplicate keys
    conn = await pool.acquire()
    result = await conn.fetch('SELECT * FROM duplicate_test ORDER BY id')
    assert len(result) == 2  # Should only have 2 unique records
    
    x_row = next(r for r in result if r['id'] == 'X')
    assert x_row['value'] == 'final'  # Last occurrence should win
    
    y_row = next(r for r in result if r['id'] == 'Y')
    assert y_row['value'] == 'unique'

    await pool.release(conn)
    await pool.close()
    os.remove(file_path)
