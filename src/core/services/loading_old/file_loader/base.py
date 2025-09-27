# ingestion/base.py
import re
import asyncpg
from typing import List, Dict

def quote_ident(ident: str) -> str:
    IDENTIFIER_REGEX = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')
    
    if not IDENTIFIER_REGEX.match(ident):
        raise ValueError(f"Invalid identifier: {ident!r}")
    return f'"{ident}"'

async def create_pool(dsn: str, min_size: int = 1, max_size: int = 10):
    return await asyncpg.create_pool(dsn, min_size=min_size, max_size=max_size)

def map_types(headers: List[str], types: Dict[str, str] = None) -> Dict[str, str]:
    if not types:
        return {h: 'TEXT' for h in headers}
    return {h: types.get(h, 'TEXT') for h in headers}

def create_temp_table_sql(tmp_table: str, headers: List[str], types: Dict[str, str]) -> str:
    """
    Create a TEMP table that lives for the session. Do NOT use ON COMMIT DROP here because
    many drivers (including asyncpg) may run the CREATE inside a transaction that gets committed
    immediately — which would drop the table before you can use it.
    """
    cols = ', '.join(f'{quote_ident(h)} {types[h]}' for h in headers)
    # create temp table for the session; keep it until the connection is closed
    return f'CREATE TEMP TABLE IF NOT EXISTS {quote_ident(tmp_table)} ({cols});'


def ensure_table_sql(target_table: str, headers: List[str], types: Dict[str, str], primary_keys: List[str]) -> str:
    cols = []
    for h in headers:
        typ = types.get(h, 'TEXT')
        cols.append(f'{quote_ident(h)} {typ}')
    pk_clause = f', PRIMARY KEY ({", ".join(quote_ident(pk) for pk in primary_keys)})' if primary_keys else ''
    return f'CREATE TABLE IF NOT EXISTS {quote_ident(target_table)} ({", ".join(cols)}{pk_clause});'

def upsert_from_temp_sql(
    target_table: str,
    tmp_table: str,
    headers: List[str],
    primary_keys: List[str],
) -> str:
    # Validate primary keys
    if not primary_keys:
        raise ValueError(f"No primary keys provided for table {target_table}")
    
    pk_list = ", ".join(quote_ident(c) for c in primary_keys)
    collist = ", ".join(quote_ident(c) for c in headers)
    select_list = ", ".join(quote_ident(c) for c in headers)

    # Using ROW_NUMBER() for deduplication instead of DISTINCT ON
    sql = (
        f"WITH ranked AS ("
        f"  SELECT {select_list}, ROW_NUMBER() OVER (PARTITION BY {pk_list} ORDER BY (SELECT NULL)) as rn "
        f"  FROM {quote_ident(tmp_table)} "
        f") "
        f"INSERT INTO {quote_ident(target_table)} ({collist}) "
        f"SELECT {select_list} FROM ranked WHERE rn = 1 "
        f"ON CONFLICT ({pk_list}) DO UPDATE SET "
        + ", ".join(f'{quote_ident(c)} = EXCLUDED.{quote_ident(c)}' for c in headers if c not in primary_keys)
        + ";"
    )
    
    return sql
