#!/usr/bin/env python3
"""
Simple database test to isolate hanging issues.
"""
import pytest
import asyncio
import tempfile
import csv
from src.base import create_pool
from src.uploader import async_upsert
from src.ingestors import batch_generator_csv

def create_test_csv():
    """Create a simple test CSV file."""
    fd, filepath = tempfile.mkstemp(suffix='.csv', prefix='simple_test_')
    import os
    os.close(fd)
    
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['id', 'val'])  # header
        writer.writerow(['1', 'foo'])
        writer.writerow(['2', 'bar'])
    
    return filepath

@pytest.mark.asyncio
async def test_simple_upsert():
    """Simplified uploader test to isolate hanging."""
    dsn = 'postgresql://testuser:testpass@localhost:5433/testdb'
    test_file = create_test_csv()
    
    try:
        # Create connection pool
        pool = await create_pool(dsn, min_size=1, max_size=2)
        
        # Setup test table
        async with pool.acquire() as conn:
            await conn.execute('DROP TABLE IF EXISTS simple_test_table')
            await conn.execute('CREATE TABLE simple_test_table (id TEXT PRIMARY KEY, val TEXT)')
            await conn.execute('DROP TABLE IF EXISTS ingestion_manifest')
            await conn.execute('''
                CREATE TABLE ingestion_manifest (
                    filename TEXT PRIMARY KEY, checksum BYTEA, filesize BIGINT,
                    processed_at TIMESTAMP WITH TIME ZONE, rows BIGINT,
                    status TEXT, run_id TEXT, notes TEXT
                )
            ''')
        
        # Run the upsert
        await async_upsert(
            pool,
            test_file,
            ['id', 'val'],
            'simple_test_table',
            ['id'],
            batch_generator_csv,
            chunk_size=10,
            max_retries=1,
            run_id='simple_test'
        )
        
        # Verify results
        async with pool.acquire() as conn:
            result = await conn.fetch('SELECT * FROM simple_test_table ORDER BY id')
            assert len(result) == 2
            assert result[0]['id'] == '1' and result[0]['val'] == 'foo'
            assert result[1]['id'] == '2' and result[1]['val'] == 'bar'
        
        await pool.close()
        
    finally:
        import os
        try:
            os.remove(test_file)
        except:
            pass

if __name__ == '__main__':
    # Run directly for debugging
    asyncio.run(test_simple_upsert())
    print("✅ Direct async test passed!")
