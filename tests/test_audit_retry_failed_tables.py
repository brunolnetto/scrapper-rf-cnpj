"""Only COMPLETED audits mark a table as processed (issue #48).

Integration test against a real PostgreSQL (testcontainers).
"""

from types import SimpleNamespace
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from src.core.schemas import FileGroupInfo, FileInfo
from src.database.models.audit import AuditBase, AuditStatus, TableAuditManifest
from src.utils import models as models_module
from src.utils.models import create_audit, create_audits

YEAR, MONTH = 2026, 11


@pytest.fixture
def database(postgres_container):
    engine = create_engine(postgres_container.get_connection_url())
    AuditBase.metadata.create_all(engine)
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE table_audit_manifest CASCADE"))
    yield SimpleNamespace(engine=engine)
    engine.dispose()


def _record(database, table, status, year=YEAR, month=MONTH):
    with Session(database.engine) as session:
        session.add(
            TableAuditManifest(entity_name=table, status=status, ingestion_year=year, ingestion_month=month)
        )
        session.commit()


def _group(table):
    return FileGroupInfo(
        table_name=table,
        files=[FileInfo(filename=f"{table}.zip", tablename=table, path=f"https://example.test/{table}.zip")],
        year=YEAR,
        month=MONTH,
    )


@pytest.mark.parametrize("status", [AuditStatus.FAILED, AuditStatus.PENDING, AuditStatus.RUNNING])
def test_unfinished_audit_does_not_block_retry(database, status):
    _record(database, "empresa", status)

    audit = create_audit(database, _group("empresa"), YEAR, MONTH)

    assert audit is not None
    assert audit.entity_name == "empresa"
    assert audit.status == AuditStatus.PENDING
    assert (audit.ingestion_year, audit.ingestion_month) == (YEAR, MONTH)


def test_completed_audit_skips_table(database):
    _record(database, "empresa", AuditStatus.COMPLETED)
    assert create_audit(database, _group("empresa"), YEAR, MONTH) is None


def test_completed_audit_in_other_period_does_not_skip(database):
    _record(database, "empresa", AuditStatus.COMPLETED, month=MONTH - 1)
    assert create_audit(database, _group("empresa"), YEAR, MONTH) is not None


def test_create_audits_checks_each_group_once(database):
    _record(database, "empresa", AuditStatus.COMPLETED)
    groups = [_group("empresa"), _group("socios")]

    with patch.object(models_module, "create_audit", wraps=models_module.create_audit) as spy:
        audits = create_audits(database, groups, YEAR, MONTH)

    assert [a.entity_name for a in audits] == ["socios"]
    assert spy.call_count == len(groups)


def test_no_engine_returns_none():
    assert create_audit(SimpleNamespace(engine=None), _group("empresa"), YEAR, MONTH) is None
