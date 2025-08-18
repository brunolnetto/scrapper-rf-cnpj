import pytest
import tempfile
import os
import logging
import asyncio
from unittest.mock import patch
from src.uploader import async_upsert
from src.base import create_pool
from src.ingestors import batch_generator_csv

@pytest.mark.asyncio
async def test_upsert_emits_metrics_and_logs():
    """Test that upsert operations emit proper logging events with timeout protection."""
    dsn = 'postgresql://testuser:testpass@localhost:5433/testdb'
    pool = await create_pool(dsn)
    conn = await pool.acquire()
    
    # Setup test tables
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

    # Create test data
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

    # Capture logs
    captured_logs = []
    
    def mock_info(msg):
        captured_logs.append(str(msg))
    
    try:
        # Apply the mock with timeout protection
        with patch.object(logging, 'info', side_effect=mock_info):
            # Run the upsert with timeout protection
            await asyncio.wait_for(
                async_upsert(
                    pool,
                    file_path,
                    headers,
                    'test_table',
                    ['id'], 
                    batch_generator_csv,
                    max_retries=1,
                    run_id='obs_test'
                ),
                timeout=30.0  # 30 second timeout
            )

        # Verify that event logs were captured
        event_logs = [log for log in captured_logs if 'event' in log]
        
        # We should have at least some event logs
        assert len(event_logs) > 0, f"No event logs found. All captured logs: {captured_logs}"
        
        # Verify specific events we expect
        log_content = ' '.join(captured_logs)
        expected_events = ['temp_table_created', 'batch_committed', 'file_completed']
        found_events = [event for event in expected_events if event in log_content]
        
        assert len(found_events) > 0, f"Expected events {expected_events} not found in logs: {captured_logs}"
        
    except asyncio.TimeoutError:
        pytest.fail("Upsert operation timed out after 30 seconds")
    
    finally:
        # Cleanup
        try:
            await pool.close()
        except:
            pass
        try:
            os.remove(file_path)
        except:
            pass
