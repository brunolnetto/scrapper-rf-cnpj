import pytest
import tempfile
import os
from src.uploader import async_upsert
from src.base import create_pool
from src.ingestors import batch_generator_csv

@pytest.mark.asyncio
async def test_upsert_unique_violation():
    dsn = 'postgresql://testuser:testpass@localhost:5433/testdb'
    pool = await create_pool(dsn)
    conn = await pool.acquire()
    await conn.execute('DROP TABLE IF EXISTS test_table')
    await conn.execute('CREATE TABLE test_table (id TEXT PRIMARY KEY, val TEXT)')
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

    headers = ['id', 'val']
    # Duplicate primary key to trigger unique violation
    rows = [{'id': '1', 'val': 'foo'}, {'id': '1', 'val': 'bar'}]
    with tempfile.NamedTemporaryFile('w+', newline='', encoding='utf-8', delete=False) as f:
        import csv
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
        f.flush()
        file_path = f.name

    # Run upsert - this should succeed as upsert handles duplicates by updating
    await async_upsert(
        pool,
        file_path,
        headers,
        'test_table',
        ['id'], batch_generator_csv,
        max_retries=1,
        run_id='fault_test'
    )

    # Check results - should have one row with the last value
    conn = await pool.acquire()
    result = await conn.fetch('SELECT * FROM test_table ORDER BY id')
    assert len(result) == 1
    assert result[0]['id'] == '1'
    assert result[0]['val'] == 'bar'  # Last value wins in upsert
    
    # Check manifest for success
    manifest = await conn.fetchrow('SELECT * FROM ingestion_manifest WHERE filename = $1', os.path.basename(file_path))
    assert manifest['status'] == 'success'
    await pool.release(conn)
    await pool.close()
    os.remove(file_path)
