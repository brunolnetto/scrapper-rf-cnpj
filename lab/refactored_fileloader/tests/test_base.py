import pytest
from src.base import (
    upsert_from_temp_sql, 
    quote_ident, 
    map_types, 
    create_temp_table_sql,
    ensure_manifest_table,
    create_pool,
    INGESTION_MANIFEST_SCHEMA
)

def test_quote_ident_valid():
    assert quote_ident('valid_name') == '"valid_name"'
    assert quote_ident('a1_b2') == '"a1_b2"'

def test_quote_ident_invalid():
    with pytest.raises(ValueError):
        quote_ident('1bad')
    with pytest.raises(ValueError):
        quote_ident('bad-name')

def test_upsert_from_temp_sql():
    sql = upsert_from_temp_sql('mytable', 'tmp_table', ['col1', 'col2'], ['col1'])
    assert 'ON CONFLICT' in sql
    assert 'INSERT INTO' in sql
    assert '"col1"' in sql
    assert '"col2"' in sql
    assert 'DISTINCT ON' in sql

@pytest.mark.asyncio
async def test_ensure_manifest_table():
    """Test that the manifest table schema is created correctly."""
    dsn = 'postgresql://testuser:testpass@localhost:5433/testdb'
    pool = await create_pool(dsn)
    conn = await pool.acquire()
    
    try:
        # Drop table if it exists
        await conn.execute('DROP TABLE IF EXISTS ingestion_manifest')
        
        # Create table using our function
        await ensure_manifest_table(conn)
        
        # Verify table was created with correct schema
        result = await conn.fetch("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'ingestion_manifest'
            ORDER BY column_name
        """)
        
        # Check that all expected columns exist
        columns = {row['column_name']: row['data_type'] for row in result}
        expected_columns = {
            'filename': 'text',
            'checksum': 'bytea', 
            'filesize': 'bigint',
            'processed_at': 'timestamp with time zone',
            'rows': 'bigint',
            'status': 'text',
            'run_id': 'text',
            'notes': 'text'
        }
        
        for col_name, col_type in expected_columns.items():
            assert col_name in columns, f"Column {col_name} not found"
            assert columns[col_name] == col_type, f"Column {col_name} has wrong type: {columns[col_name]}"
            
    finally:
        await pool.release(conn)
        await pool.close()

def test_manifest_schema_content():
    """Test that the manifest schema constant contains expected elements."""
    assert 'CREATE TABLE IF NOT EXISTS ingestion_manifest' in INGESTION_MANIFEST_SCHEMA
    assert 'filename TEXT PRIMARY KEY' in INGESTION_MANIFEST_SCHEMA
    assert 'checksum BYTEA' in INGESTION_MANIFEST_SCHEMA
    assert 'TIMESTAMP WITH TIME ZONE' in INGESTION_MANIFEST_SCHEMA
