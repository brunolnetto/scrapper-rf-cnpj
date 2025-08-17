import pytest
import asyncio
import os
import csv
import tempfile
from pathlib import Path
from src.base import create_pool
from src.ingestors import batch_generator_csv
from src.uploader import async_upsert

# Standalone test without any fixtures to avoid hanging issues

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
async def test_upsert_insert_then_update_standalone():
    """Test that upsert correctly handles INSERT and UPDATE operations."""
    
    # Database setup
    test_dsn = os.getenv('TEST_DATABASE_URL', 
                        'postgresql://testuser:testpass@localhost:5433/test_db')
    
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
        
        # PHASE 1: Initial insert - should insert 3 new records
        initial_data = [
            {'id': 'A', 'name': 'Alice', 'status': 'active'},
            {'id': 'B', 'name': 'Bob', 'status': 'inactive'},
            {'id': 'C', 'name': 'Charlie', 'status': 'active'}
        ]
        
        file1_path = create_test_csv(headers, initial_data)
        
        await async_upsert(
            pool, file1_path, headers, 'upsert_test', ['id'], batch_generator_csv, run_id='insert_phase'
        )
        
        # Verify insert worked
        conn = await pool.acquire()
        count = await conn.fetchval('SELECT COUNT(*) FROM upsert_test')
        assert count == 3, f"Expected 3 records after insert, got {count}"
        await pool.release(conn)
        
        # PHASE 2: Update existing + insert new records
        update_data = [
            {'id': 'A', 'name': 'Alice Updated', 'status': 'inactive'},  # UPDATE
            {'id': 'B', 'name': 'Bob Updated', 'status': 'active'},      # UPDATE
            {'id': 'D', 'name': 'David', 'status': 'pending'},           # INSERT new
            {'id': 'E', 'name': 'Eve', 'status': 'active'}               # INSERT new
        ]
        
        file2_path = create_test_csv(headers, update_data)
        
        await async_upsert(
            pool, file2_path, headers, 'upsert_test', ['id'], batch_generator_csv, run_id='update_phase'
        )
        
        # Verify final state
        conn = await pool.acquire()
        result = await conn.fetch('SELECT * FROM upsert_test ORDER BY id')
        result_dict = {row['id']: row for row in result}
        
        # Should have 5 total records (3 original + 2 new)
        assert len(result) == 5, f"Expected 5 records after upsert, got {len(result)}"
        
        # Verify updates
        assert result_dict['A']['name'] == 'Alice Updated'
        assert result_dict['A']['status'] == 'inactive'
        assert result_dict['B']['name'] == 'Bob Updated'
        assert result_dict['B']['status'] == 'active'
        
        # Verify unchanged record
        assert result_dict['C']['name'] == 'Charlie'
        assert result_dict['C']['status'] == 'active'
        
        # Verify new inserts
        assert result_dict['D']['name'] == 'David'
        assert result_dict['D']['status'] == 'pending'
        assert result_dict['E']['name'] == 'Eve'
        assert result_dict['E']['status'] == 'active'
        
        await pool.release(conn)
        
    finally:
        # Cleanup
        await pool.close()
        if 'file1_path' in locals():
            os.unlink(file1_path)
        if 'file2_path' in locals():
            os.unlink(file2_path)

@pytest.mark.asyncio
async def test_upsert_mixed_operations_standalone():
    """Test upsert with various operations in a single batch."""
    
    test_dsn = os.getenv('TEST_DATABASE_URL', 
                        'postgresql://testuser:testpass@localhost:5433/test_db')
    
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
        
        # Initial baseline
        initial_data = [
            {'id': '1', 'name': 'John', 'status': 'active'},
            {'id': '2', 'name': 'Jane', 'status': 'inactive'}
        ]
        file1_path = create_test_csv(headers, initial_data)
        
        await async_upsert(
            pool, file1_path, headers, 'upsert_test', ['id'], batch_generator_csv, run_id='baseline'
        )
        
        # Mixed operations: update existing + insert new
        mixed_data = [
            {'id': '1', 'name': 'John Updated', 'status': 'inactive'},  # UPDATE
            {'id': '3', 'name': 'Bob', 'status': 'active'},            # INSERT
            {'id': '4', 'name': 'Alice', 'status': 'pending'}          # INSERT
        ]
        file2_path = create_test_csv(headers, mixed_data)
        
        await async_upsert(
            pool, file2_path, headers, 'upsert_test', ['id'], batch_generator_csv, run_id='mixed'
        )
        
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
        if 'file1_path' in locals():
            os.unlink(file1_path)
        if 'file2_path' in locals():
            os.unlink(file2_path)
