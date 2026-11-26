"""--force-reload deletes a period's audits through the repository (issue #40).

Integration test against a real PostgreSQL (testcontainers).
"""

from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text

from src.core.services.audit.repository import AuditRepository
from src.database.models.audit import AuditBase

CHILD_DDL = [
    "CREATE TABLE file_audit_manifest (file_audit_id UUID PRIMARY KEY, parent_table_audit_id UUID)",
    "CREATE TABLE batch_audit_manifest (batch_audit_id UUID PRIMARY KEY, parent_file_audit_id UUID)",
    "CREATE TABLE subbatch_audit_manifest (subbatch_audit_id UUID PRIMARY KEY, parent_batch_audit_id UUID)",
]
CHILDREN = ["subbatch_audit_manifest", "batch_audit_manifest", "file_audit_manifest"]


@pytest.fixture
def engine(postgres_container):
    engine = create_engine(postgres_container.get_connection_url())
    AuditBase.metadata.create_all(engine)
    with engine.begin() as conn:
        for table in CHILDREN:
            conn.execute(text(f"DROP TABLE IF EXISTS {table}"))
        conn.execute(text("TRUNCATE table_audit_manifest CASCADE"))
    yield engine
    with engine.begin() as conn:
        for table in CHILDREN:
            conn.execute(text(f"DROP TABLE IF EXISTS {table}"))
    engine.dispose()


def _table_audit(conn, year, month):
    audit_id = uuid4()
    conn.execute(
        text(
            "INSERT INTO table_audit_manifest "
            "(table_audit_id, entity_name, status, created_at, ingestion_year, ingestion_month) "
            "VALUES (:id, 'empresa', 'COMPLETED', now(), :y, :m)"
        ),
        {"id": audit_id, "y": year, "m": month},
    )
    return audit_id


def _hierarchy(conn, year, month):
    table_id = _table_audit(conn, year, month)
    file_id, batch_id = uuid4(), uuid4()
    conn.execute(text("INSERT INTO file_audit_manifest VALUES (:f, :t)"), {"f": file_id, "t": table_id})
    conn.execute(text("INSERT INTO batch_audit_manifest VALUES (:b, :f)"), {"b": batch_id, "f": file_id})
    conn.execute(text("INSERT INTO subbatch_audit_manifest VALUES (:s, :b)"), {"s": uuid4(), "b": batch_id})


def _count(conn, table):
    return conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()


def test_deletes_only_the_target_period_leaf_to_root(engine):
    with engine.begin() as conn:
        for ddl in CHILD_DDL:
            conn.execute(text(ddl))
        _hierarchy(conn, 2026, 11)
        _hierarchy(conn, 2026, 10)

    deleted = AuditRepository(SimpleNamespace(engine=engine)).delete_period_audits(2026, 11)

    assert deleted == {
        "subbatch_audit_manifest": 1,
        "batch_audit_manifest": 1,
        "file_audit_manifest": 1,
        "table_audit_manifest": 1,
    }
    with engine.connect() as conn:
        for table in CHILDREN + ["table_audit_manifest"]:
            assert _count(conn, table) == 1, table


def test_missing_child_tables_are_skipped(engine):
    with engine.begin() as conn:
        _table_audit(conn, 2026, 11)
        _table_audit(conn, 2026, 10)

    deleted = AuditRepository(SimpleNamespace(engine=engine)).delete_period_audits(2026, 11)

    assert deleted == {"table_audit_manifest": 1}
    with engine.connect() as conn:
        assert _count(conn, "table_audit_manifest") == 1
