import pytest
import tempfile
import os
import logging
from unittest.mock import patch
from src.uploader import async_upsert
from src.base import create_pool
from src.csv_ingestor import batch_generator

@pytest.mark.asyncio
async def test_upsert_emits_metrics_and_logs():
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

    logs = []
    def log_capture(msg):
        logs.append(msg)

    with patch.object(logging, 'info', side_effect=log_capture):
        await async_upsert(
            pool,
            file_path,
            headers,
            'test_table',
            'id',
            batch_generator,
            max_retries=1,
            run_id='obs_test'
        )

    # Check that metrics and logs were emitted
    metric_found = any('"metric":' in l for l in logs)
    log_found = any('"event":' in l for l in logs)
    assert metric_found
    assert log_found
    await pool.close()
    os.remove(file_path)
