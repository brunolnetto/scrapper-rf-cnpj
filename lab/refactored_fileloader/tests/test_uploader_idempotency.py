import pytest
import tempfile
import os
from src.uploader import async_upsert
from src.base import create_pool
from src.csv_ingestor import batch_generator

@pytest.mark.asyncio
async def test_upsert_idempotency():
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
    rows = [{'id': '1', 'val': 'foo'}, {'id': '2', 'val': 'bar'}]
    with tempfile.NamedTemporaryFile('w+', newline='', encoding='utf-8', delete=False) as f:
        import csv
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
        f.flush()
        file_path = f.name

    # First upsert
    await async_upsert(
        pool,
        file_path,
        headers,
        'test_table',
        'id',
        batch_generator,
        max_retries=2,
        run_id='test_run_1'
    )
    # Second upsert (should be idempotent)
    await async_upsert(
        pool,
        file_path,
        headers,
        'test_table',
        'id',
        batch_generator,
        max_retries=2,
        run_id='test_run_2'
    )

    conn = await pool.acquire()
    result = await conn.fetch('SELECT * FROM test_table ORDER BY id')
    assert len(result) == 2
    assert result[0]['id'] == '1' and result[0]['val'] == 'foo'
    assert result[1]['id'] == '2' and result[1]['val'] == 'bar'
    await pool.release(conn)
    await pool.close()
    os.remove(file_path)
