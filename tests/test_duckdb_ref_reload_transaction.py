"""Reference-table reload must be atomic (issue #32).

Integration test against a real PostgreSQL (testcontainers) through DuckDB's
postgres extension, the same path used in production.
"""

from pathlib import Path

import psycopg2
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src.core.services.loading.duckdb_loader import DuckDBLoader

TABLE = "tmp_ref_reload"


@pytest.fixture
def pg(postgres_container):
    params = {
        "host": postgres_container.get_container_host_ip(),
        "port": int(postgres_container.get_exposed_port(5432)),
        "user": postgres_container.username,
        "password": postgres_container.password,
        "dbname": postgres_container.dbname,
    }
    conn = psycopg2.connect(**params)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {TABLE}")
        cur.execute(
            f"CREATE TABLE {TABLE} (codigo TEXT NOT NULL, descricao TEXT NOT NULL, row_hash TEXT NOT NULL)"
        )
        cur.execute(f"INSERT INTO {TABLE} VALUES ('1', 'antigo-1', 'h1'), ('2', 'antigo-2', 'h2')")
    dsn = " ".join(f"{k}={v}" for k, v in params.items())
    yield conn, dsn
    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {TABLE}")
    conn.close()


def _rows(conn):
    with conn.cursor() as cur:
        cur.execute(f"SELECT codigo, descricao FROM {TABLE} ORDER BY codigo")
        return cur.fetchall()


def _parquet(tmp_path: Path, codes, descriptions) -> Path:
    path = tmp_path / f"{TABLE}.parquet"
    table = pa.table(
        {"column_1": codes, "column_2": descriptions},
        schema=pa.schema([pa.field("column_1", pa.string()), pa.field("column_2", pa.string())]),
    )
    pq.write_table(table, str(path))
    return path


def _upsert(dsn: str, parquet: Path) -> int:
    loader = DuckDBLoader(pg_dsn=dsn)
    duck = loader._make_connection()
    try:
        loader._attach_postgres(duck)
        return loader._standard_upsert(duck, parquet, TABLE)
    finally:
        duck.close()


def test_failed_insert_keeps_previous_rows(pg, tmp_path):
    conn, dsn = pg
    # NULL in a NOT NULL column makes the INSERT fail after the DELETE ran.
    parquet = _parquet(tmp_path, ["3", None], ["novo-3", "novo-4"])

    with pytest.raises(Exception):
        _upsert(dsn, parquet)

    assert _rows(conn) == [("1", "antigo-1"), ("2", "antigo-2")]


def test_successful_reload_replaces_rows(pg, tmp_path):
    conn, dsn = pg
    parquet = _parquet(tmp_path, ["3"], ["novo-3"])

    assert _upsert(dsn, parquet) == 1
    assert _rows(conn) == [("3", "novo-3")]
