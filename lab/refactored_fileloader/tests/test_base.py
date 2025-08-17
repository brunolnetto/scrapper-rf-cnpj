import pytest
from src.base import (
    upsert_sql, 
    quote_ident, 
    map_types, 
    create_temp_table_sql
)

def test_quote_ident_valid():
    assert quote_ident('valid_name') == '"valid_name"'
    assert quote_ident('a1_b2') == '"a1_b2"'

def test_quote_ident_invalid():
    with pytest.raises(ValueError):
        quote_ident('1bad')
    with pytest.raises(ValueError):
        quote_ident('bad-name')

def test_upsert_sql():
    sql = upsert_sql('mytable', ['col1', 'col2'], 'col1')
    assert 'ON CONFLICT' in sql
    assert 'INSERT INTO' in sql
    assert '"col1"' in sql
    assert '"col2"' in sql
