import pytest
import os
import csv
import tempfile
from src.base import create_pool
from src.ingestors import batch_generator_csv
from src.uploader import async_upsert

def create_test_csv(headers, data):
    """Create a temporary CSV file with test data."""
    fd, filepath = tempfile.mkstemp(suffix='.csv', prefix='test_')
    os.close(fd)
    
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(data)
    
    return filepath

@pytest.mark.asyncio
async def test_upsert_insert_then_update():
    """Test that upsert correctly handles INSERT (new records) and UPDATE (existing records)."""
    # Use standalone setup without any fixtures
    test_dsn = 'postgresql://testuser:testpass@localhost:5433/test_db'
    pool = await create_pool(test_dsn)
    conn = await pool.acquire()
    
    try:
        # Setup tables manually
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
        
        headers = ['id', 'name', 'status']
        initial_data = [
            {'id': 'A', 'name': 'Alice', 'status': 'active'},
            {'id': 'B', 'name': 'Bob', 'status': 'inactive'},
            {'id': 'C', 'name': 'Charlie', 'status': 'active'}
        ]
        
        # PHASE 1: Initial insert - 3 records
        file1_path = create_test_csv(headers, initial_data)

        await async_upsert(
            pool,
            file1_path,
            headers,
            'upsert_test',
            ['id'], batch_generator_csv,
            max_retries=2,
            run_id='insert_test'
        )

        # Verify initial INSERT worked
        conn = await pool.acquire()
        result = await conn.fetch('SELECT * FROM upsert_test ORDER BY id')
        assert len(result) == 3
        assert result[0]['id'] == 'A' and result[0]['name'] == 'Alice' and result[0]['status'] == 'active'
        assert result[1]['id'] == 'B' and result[1]['name'] == 'Bob' and result[1]['status'] == 'inactive'
        assert result[2]['id'] == 'C' and result[2]['name'] == 'Charlie' and result[2]['status'] == 'active'
        await pool.release(conn)

        # PHASE 2: Upsert with updates and new records
        update_data = [
            {'id': 'A', 'name': 'Alice Updated', 'status': 'inactive'},  # Update existing
            {'id': 'B', 'name': 'Bob Updated', 'status': 'active'},      # Update existing
            {'id': 'D', 'name': 'Diana', 'status': 'active'},           # New record
            {'id': 'E', 'name': 'Eve', 'status': 'inactive'}            # New record
        ]
        file2_path = create_test_csv(headers, update_data)

        await async_upsert(
            pool,
            file2_path,
            headers,
            'upsert_test',
            ['id'], batch_generator_csv,
            max_retries=2,
            run_id='update_test'
        )

        # Verify UPDATE and INSERT worked
        conn = await pool.acquire()
        result = await conn.fetch('SELECT * FROM upsert_test ORDER BY id')
        assert len(result) == 4  # A, B updated + D, E new (C missing from update data)

        # Check updates (A and B should be updated)
        result_dict = {row['id']: row for row in result}
        assert result_dict['A']['name'] == 'Alice Updated' and result_dict['A']['status'] == 'inactive'
        assert result_dict['B']['name'] == 'Bob Updated' and result_dict['B']['status'] == 'active'
        
        # Check new records (D and E)
        assert result_dict['D']['name'] == 'Diana' and result_dict['D']['status'] == 'active'
        assert result_dict['E']['name'] == 'Eve' and result_dict['E']['status'] == 'inactive'
        
        await pool.release(conn)
        
    finally:
        await pool.close()
        # Cleanup files
        if 'file1_path' in locals():
            os.unlink(file1_path)
        if 'file2_path' in locals():
            os.unlink(file2_path)


@pytest.mark.asyncio
async def test_upsert_mixed_operations():
    """Test upsert with various operations in a single batch."""
    test_dsn = 'postgresql://testuser:testpass@localhost:5433/test_db'
    pool = await create_pool(test_dsn)
    conn = await pool.acquire()
    
    try:
        # Setup tables
        await conn.execute('DROP TABLE IF EXISTS upsert_test')
        await conn.execute('CREATE TABLE upsert_test (id TEXT PRIMARY KEY, name TEXT, status TEXT)')
        await conn.execute('DROP TABLE IF EXISTS ingestion_manifest')
        await conn.execute('''
            CREATE TABLE ingestion_manifest (
                filename TEXT PRIMARY KEY, checksum BYTEA, filesize BIGINT,
                processed_at TIMESTAMP WITH TIME ZONE, rows BIGINT,
                status TEXT, run_id TEXT, notes TEXT
            )
        ''')
        await pool.release(conn)
        
        headers = ['id', 'name', 'status']
        
        # Initial data: Create baseline
        initial_data = [
            {'id': '1', 'name': 'John', 'status': 'active'},
            {'id': '2', 'name': 'Jane', 'status': 'inactive'}
        ]
        file1_path = create_test_csv(headers, initial_data)
        
        await async_upsert(pool, file1_path, headers, 'upsert_test', ['id'], batch_generator_csv, run_id='baseline')
        
        # Mixed operations: update existing + insert new
        mixed_data = [
            {'id': '1', 'name': 'John Updated', 'status': 'inactive'},  # UPDATE
            {'id': '3', 'name': 'Bob', 'status': 'active'},            # INSERT
            {'id': '4', 'name': 'Alice', 'status': 'pending'}          # INSERT
        ]
        file2_path = create_test_csv(headers, mixed_data)
        
        await async_upsert(pool, file2_path, headers, 'upsert_test', ['id'], batch_generator_csv, run_id='mixed')
        
        # Verify results
        conn = await pool.acquire()
        result = await conn.fetch('SELECT * FROM upsert_test ORDER BY id')
        result_dict = {row['id']: row for row in result}
        
        # Should have 4 records total (2 original, 1 updated, 2 inserted)
        assert len(result) == 4
        assert result_dict['1']['name'] == 'John Updated' and result_dict['1']['status'] == 'inactive'
        assert result_dict['2']['name'] == 'Jane' and result_dict['2']['status'] == 'inactive'  # unchanged
        assert result_dict['3']['name'] == 'Bob' and result_dict['3']['status'] == 'active'
        assert result_dict['4']['name'] == 'Alice' and result_dict['4']['status'] == 'pending'
        
        await pool.release(conn)
        
    finally:
        await pool.close()
        # Cleanup files
        if 'file1_path' in locals():
            os.unlink(file1_path)
        if 'file2_path' in locals():
            os.unlink(file2_path)


@pytest.mark.asyncio 
async def test_upsert_duplicate_keys_in_batch():
    """Test upsert behavior when same key appears multiple times in single batch."""
    test_dsn = 'postgresql://testuser:testpass@localhost:5433/test_db'
    pool = await create_pool(test_dsn)
    conn = await pool.acquire()
    
    try:
        # Setup tables manually
        await conn.execute('DROP TABLE IF EXISTS upsert_test')
        await conn.execute('CREATE TABLE upsert_test (id TEXT PRIMARY KEY, name TEXT, status TEXT)')
        await conn.execute('DROP TABLE IF EXISTS ingestion_manifest')
        await conn.execute('''
            CREATE TABLE ingestion_manifest (
                filename TEXT PRIMARY KEY, checksum BYTEA, filesize BIGINT,
                processed_at TIMESTAMP WITH TIME ZONE, rows BIGINT,
                status TEXT, run_id TEXT, notes TEXT
            )
        ''')
        await pool.release(conn)
        
        headers = ['id', 'name', 'status']
        
        # Data with duplicate keys - last one should win
        duplicate_data = [
            {'id': 'X', 'name': 'First', 'status': 'active'},
            {'id': 'Y', 'name': 'Other', 'status': 'pending'},
            {'id': 'X', 'name': 'Last', 'status': 'inactive'}  # This should win
        ]
        file_path = create_test_csv(headers, duplicate_data)
        
        await async_upsert(pool, file_path, headers, 'upsert_test', ['id'], batch_generator_csv, run_id='duplicates')
        
        # Verify last value wins
        conn = await pool.acquire()
        result = await conn.fetch('SELECT * FROM upsert_test ORDER BY id')
        result_dict = {row['id']: row for row in result}
        
        assert len(result) == 2
        assert result_dict['X']['name'] == 'Last' and result_dict['X']['status'] == 'inactive'
        assert result_dict['Y']['name'] == 'Other' and result_dict['Y']['status'] == 'pending'
        
        await pool.release(conn)
        
    finally:
        await pool.close()
        # Cleanup files
        if 'file_path' in locals():
            os.unlink(file_path)
