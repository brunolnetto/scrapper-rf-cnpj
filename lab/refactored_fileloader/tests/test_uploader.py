import pytest
import os
from src.base import create_pool
from src.uploader import async_upsert
from src.ingestors import batch_generator_csv

@pytest.mark.asyncio
async def test_async_upsert(test_dsn, temp_csv_file):
    """Test basic async upsert functionality."""
    
    # Manual database setup
    pool = await create_pool(test_dsn)
    conn = await pool.acquire()
    
    try:
        # Setup tables
        await conn.execute('DROP TABLE IF EXISTS test_table')
        await conn.execute('CREATE TABLE test_table (id TEXT PRIMARY KEY, val TEXT)')
        await conn.execute('DROP TABLE IF EXISTS ingestion_manifest')
        await conn.execute('''
            CREATE TABLE ingestion_manifest (
                filename TEXT PRIMARY KEY, checksum BYTEA, filesize BIGINT,
                processed_at TIMESTAMP WITH TIME ZONE, rows BIGINT,
                status TEXT, run_id TEXT, notes TEXT
            )
        ''')
        await pool.release(conn)
        
        # Run upsert
        await async_upsert(
            pool,
            temp_csv_file,
            ['id', 'val'],
            'test_table',
            ['id'],  # Fixed: primary_keys should be a list
            batch_generator_csv,
            max_retries=2,
            run_id='test_run'
        )

        # Check results
        conn = await pool.acquire()
        result = await conn.fetch('SELECT * FROM test_table ORDER BY id')
        assert len(result) == 2
        assert result[0]['id'] == '1' and result[0]['val'] == 'foo'
        assert result[1]['id'] == '2' and result[1]['val'] == 'bar'
        
        # Check manifest
        manifest = await conn.fetchrow('SELECT * FROM ingestion_manifest WHERE filename = $1', os.path.basename(temp_csv_file))
        assert manifest['status'] == 'success'
        await pool.release(conn)
        
    finally:
        await pool.close()
