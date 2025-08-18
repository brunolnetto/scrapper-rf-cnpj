"""
Consolidated comprehensive test suite for uploader functionality.
Combines all upsert scenarios, fault injection, and integration tests.
"""
import pytest
import tempfile
import os
import csv
from src.uploader import async_upsert
from src.base import create_pool
from src.ingestors import batch_generator_csv


class TestUploaderCore:
    """Core uploader functionality tests."""
    
    def create_test_csv(self, headers, data):
        """Helper to create test CSV files."""
        fd, filepath = tempfile.mkstemp(suffix='.csv', prefix='test_')
        os.close(fd)
        
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(data)
        
        return filepath

    async def setup_test_db(self, dsn, table_name='test_table'):
        """Helper to setup test database and tables."""
        pool = await create_pool(dsn)
        conn = await pool.acquire()
        
        await conn.execute(f'DROP TABLE IF EXISTS {table_name}')
        await conn.execute(f'CREATE TABLE {table_name} (id TEXT PRIMARY KEY, val TEXT)')
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
        return pool

    @pytest.mark.asyncio
    async def test_basic_upsert(self):
        """Test basic upsert functionality."""
        dsn = 'postgresql://testuser:testpass@localhost:5433/testdb'
        pool = await self.setup_test_db(dsn)
        
        headers = ['id', 'val']
        data = [{'id': '1', 'val': 'foo'}, {'id': '2', 'val': 'bar'}]
        file_path = self.create_test_csv(headers, data)
        
        try:
            await async_upsert(
                pool, file_path, headers, 'test_table', 
                ['id'], batch_generator_csv, max_retries=1
            )
            
            # Verify data was inserted
            conn = await pool.acquire()
            result = await conn.fetch('SELECT id, val FROM test_table ORDER BY id')
            assert len(result) == 2
            assert result[0]['id'] == '1' and result[0]['val'] == 'foo'
            assert result[1]['id'] == '2' and result[1]['val'] == 'bar'
            await pool.release(conn)
            
        finally:
            await pool.close()
            os.remove(file_path)

    @pytest.mark.asyncio
    async def test_upsert_insert_then_update(self):
        """Test upsert with both INSERT and UPDATE operations."""
        dsn = 'postgresql://testuser:testpass@localhost:5433/testdb'
        pool = await self.setup_test_db(dsn, 'upsert_test')
        
        headers = ['id', 'val']
        
        try:
            # Phase 1: Initial insert
            initial_data = [{'id': 'A', 'val': 'value1'}, {'id': 'B', 'val': 'value2'}]
            file1_path = self.create_test_csv(headers, initial_data)
            
            await async_upsert(
                pool, file1_path, headers, 'upsert_test', 
                ['id'], batch_generator_csv, max_retries=1
            )
            
            # Phase 2: Update existing + insert new
            updated_data = [
                {'id': 'A', 'val': 'updated1'},  # Update existing
                {'id': 'C', 'val': 'value3'}     # Insert new
            ]
            file2_path = self.create_test_csv(headers, updated_data)
            
            await async_upsert(
                pool, file2_path, headers, 'upsert_test', 
                ['id'], batch_generator_csv, max_retries=1
            )
            
            # Verify final state
            conn = await pool.acquire()
            result = await conn.fetch('SELECT id, val FROM upsert_test ORDER BY id')
            assert len(result) == 3
            assert result[0]['id'] == 'A' and result[0]['val'] == 'updated1'  # Updated
            assert result[1]['id'] == 'B' and result[1]['val'] == 'value2'    # Unchanged
            assert result[2]['id'] == 'C' and result[2]['val'] == 'value3'    # New
            await pool.release(conn)
            
            os.remove(file2_path)
            
        finally:
            await pool.close()
            os.remove(file1_path)

    @pytest.mark.asyncio
    async def test_upsert_duplicate_keys_within_batch(self):
        """Test upsert behavior with duplicate keys in same batch (should use last value)."""
        dsn = 'postgresql://testuser:testpass@localhost:5433/testdb'
        pool = await self.setup_test_db(dsn)
        
        headers = ['id', 'val']
        # Same ID appears twice - should keep the last value
        data = [
            {'id': '1', 'val': 'first'},
            {'id': '1', 'val': 'last'},  # This should win
            {'id': '2', 'val': 'unique'}
        ]
        file_path = self.create_test_csv(headers, data)
        
        try:
            await async_upsert(
                pool, file_path, headers, 'test_table', 
                ['id'], batch_generator_csv, max_retries=1
            )
            
            # Verify "last value wins" behavior
            conn = await pool.acquire()
            result = await conn.fetch('SELECT id, val FROM test_table WHERE id = $1', '1')
            assert len(result) == 1
            assert result[0]['val'] == 'last'  # Should be the last occurrence
            await pool.release(conn)
            
        finally:
            await pool.close()
            os.remove(file_path)

    @pytest.mark.asyncio 
    async def test_upsert_idempotency(self):
        """Test that running the same upsert twice produces identical results."""
        dsn = 'postgresql://testuser:testpass@localhost:5433/testdb'
        pool = await self.setup_test_db(dsn)
        
        headers = ['id', 'val']
        data = [{'id': '1', 'val': 'test'}, {'id': '2', 'val': 'data'}]
        file_path = self.create_test_csv(headers, data)
        
        try:
            # Run upsert twice
            await async_upsert(
                pool, file_path, headers, 'test_table', 
                ['id'], batch_generator_csv, max_retries=1, run_id='run1'
            )
            
            await async_upsert(
                pool, file_path, headers, 'test_table', 
                ['id'], batch_generator_csv, max_retries=1, run_id='run2'
            )
            
            # Verify data is same after both runs
            conn = await pool.acquire()
            result = await conn.fetch('SELECT id, val FROM test_table ORDER BY id')
            assert len(result) == 2
            assert result[0]['id'] == '1' and result[0]['val'] == 'test'
            assert result[1]['id'] == '2' and result[1]['val'] == 'data'
            
            # Check manifest has both runs recorded
            manifest = await conn.fetch('SELECT run_id FROM ingestion_manifest ORDER BY run_id')
            assert len(manifest) == 2
            await pool.release(conn)
            
        finally:
            await pool.close()
            os.remove(file_path)
