"""Audit models must cover every column the code writes (issue #57).

The file/batch/subbatch audit models were removed in a786779 while raw SQL
kept using their tables, so a fresh audit database lacked them. The static
contract test fails as soon as a model or column used by SQL disappears; the
integration test exercises the full hierarchy on a fresh PostgreSQL.
"""

import re
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, inspect, text

from src.core.services.audit.repository import AuditRepository
from src.database.models.audit import AuditBase, AuditStatus

AUDIT_TABLES = ["table_audit_manifest", "file_audit_manifest", "batch_audit_manifest", "subbatch_audit_manifest"]
SRC = Path(__file__).resolve().parents[1] / "src"


def _columns_used_in_sql(table: str) -> set[str]:
    code = "\n".join(p.read_text(encoding="utf-8") for p in SRC.rglob("*.py"))
    used: set[str] = set()
    for match in re.finditer(rf"INSERT INTO {table}\s*\(([^)]*)\)", code):
        used |= {c.strip() for c in match.group(1).split(",") if c.strip()}
    for match in re.finditer(rf"UPDATE {table}\s+SET(.*?)WHERE", code, re.S):
        used |= set(re.findall(r"(\w+)\s*=", match.group(1)))
    return used


@pytest.mark.parametrize("table", AUDIT_TABLES)
def test_models_define_every_column_used_by_sql(table):
    assert table in AuditBase.metadata.tables, f"no model for {table}"
    modelled = set(AuditBase.metadata.tables[table].columns.keys())
    used = _columns_used_in_sql(table)
    assert used, f"expected SQL writing to {table}"
    assert used <= modelled, f"{table} is missing columns: {sorted(used - modelled)}"


@pytest.fixture
def fresh_engine(postgres_container):
    engine = create_engine(postgres_container.get_connection_url())
    with engine.begin() as conn:
        conn.execute(text("DROP SCHEMA public CASCADE"))
        conn.execute(text("CREATE SCHEMA public"))
    AuditBase.metadata.create_all(engine)
    yield engine
    engine.dispose()


def test_fresh_database_supports_full_audit_hierarchy(fresh_engine):
    assert set(AUDIT_TABLES) <= set(inspect(fresh_engine).get_table_names())

    table_id, batch_id = uuid4(), uuid4()
    with fresh_engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO table_audit_manifest (table_audit_id, entity_name, status, created_at, "
                "ingestion_year, ingestion_month) VALUES (:id, 'empresa', 'PENDING', now(), 2026, 11)"
            ),
            {"id": table_id},
        )

    repo = AuditRepository(SimpleNamespace(engine=fresh_engine))
    file_id = str(uuid4())
    repo.insert_file_manifest(
        file_id, "empresa", "/data/empresa.parquet", AuditStatus.RUNNING,
        checksum="abc", filesize=10, rows_processed=None, table_manifest_id=str(table_id),
    )

    with fresh_engine.begin() as conn:
        assert conn.execute(text("SELECT COUNT(*) FROM file_audit_manifest")).scalar() == 1
        conn.execute(
            text(
                "INSERT INTO batch_audit_manifest (batch_audit_id, parent_file_audit_id, target_table, "
                "status, created_at) VALUES (:b, :f, 'empresa', 'RUNNING', now())"
            ),
            {"b": batch_id, "f": file_id},
        )
        conn.execute(
            text(
                "INSERT INTO subbatch_audit_manifest (subbatch_audit_id, parent_batch_audit_id, "
                "processing_step, table_name, status, created_at) "
                "VALUES (:s, :b, 'load', 'empresa', 'COMPLETED', now())"
            ),
            {"s": uuid4(), "b": batch_id},
        )
        conn.execute(
            text("UPDATE table_audit_manifest SET metrics = :m WHERE table_audit_id = :id"),
            {"m": '{"rows": 1}', "id": table_id},
        )

