"""Loading a table records the full audit hierarchy (issue #57).

Before this fix the DuckDB load path never called the file/batch/subbatch
audit helpers, so file_audit_manifest, batch_audit_manifest and
subbatch_audit_manifest stayed empty. Integration test with the real
AuditService on a fresh PostgreSQL; only the DuckDB→PostgreSQL merge is faked.
"""

from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text

from src.core.services.audit.service import AuditService
from src.core.services.loading.service import FileLoadingService
from src.database.models.audit import AuditBase


class _Monitor:
    def should_prevent_processing(self):
        return False

    def is_memory_pressure_high(self):
        return False

    def get_status_report(self):
        return {"usage_above_baseline_mb": 0.0, "pressure_level": 0.0}


class _FakeDuckDB:
    def __init__(self, result=None, error=None):
        self.result, self.error = result, error

    def load(self, **_kwargs):
        if self.error:
            raise self.error
        return self.result


@pytest.fixture
def audit_engine(postgres_container):
    engine = create_engine(postgres_container.get_connection_url())
    with engine.begin() as conn:
        conn.execute(text("DROP SCHEMA public CASCADE"))
        conn.execute(text("CREATE SCHEMA public"))
    AuditBase.metadata.create_all(engine)
    yield engine
    engine.dispose()


@pytest.fixture
def loader(audit_engine, tmp_path):
    config = SimpleNamespace(
        year=2026,
        month=11,
        pipeline=SimpleNamespace(
            loading=SimpleNamespace(enable_internal_parallelism=False),
            get_temporal_conversion_path=lambda _y, _m: tmp_path,
        ),
    )
    (tmp_path / "empresa.parquet").write_bytes(b"PAR1-fake-parquet")
    with audit_engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO table_audit_manifest (table_audit_id, entity_name, status, created_at, "
                "ingestion_year, ingestion_month) VALUES (:id, 'empresa', 'PENDING', now(), 2026, 11)"
            ),
            {"id": uuid4()},
        )

    audit_service = AuditService(SimpleNamespace(engine=audit_engine), config)
    service = FileLoadingService.__new__(FileLoadingService)
    service.config = config
    service.audit_service = audit_service
    service.memory_monitor = _Monitor()
    yield service
    audit_service.shutdown()


def _rows(engine, sql):
    with engine.connect() as conn:
        return conn.execute(text(sql)).mappings().all()


async def test_successful_load_records_file_batch_and_subbatch(loader, audit_engine):
    loader._duckdb_loader = _FakeDuckDB(result=(True, None, 42))

    assert await loader.load_table("empresa") == (True, None, 42)

    files = _rows(audit_engine, "SELECT * FROM file_audit_manifest")
    assert len(files) == 1
    assert files[0]["status"] == "COMPLETED"
    assert files[0]["file_path"].endswith("empresa.parquet")
    assert files[0]["filesize"] == len(b"PAR1-fake-parquet")
    assert files[0]["checksum"]  # deferred checksum was awaited and stored

    batches = _rows(audit_engine, "SELECT * FROM batch_audit_manifest")
    assert len(batches) == 1
    assert batches[0]["parent_file_audit_id"] == files[0]["file_audit_id"]
    assert batches[0]["status"] == "COMPLETED"

    subbatches = _rows(audit_engine, "SELECT * FROM subbatch_audit_manifest")
    assert len(subbatches) == 1
    assert subbatches[0]["parent_batch_audit_id"] == batches[0]["batch_audit_id"]
    assert subbatches[0]["status"] == "COMPLETED"


async def test_failed_load_marks_file_and_batch_failed(loader, audit_engine):
    loader._duckdb_loader = _FakeDuckDB(result=(False, "contract violation", 0))

    assert await loader.load_table("empresa") == (False, "contract violation", 0)

    assert [r["status"] for r in _rows(audit_engine, "SELECT status FROM file_audit_manifest")] == ["FAILED"]
    assert [r["status"] for r in _rows(audit_engine, "SELECT status FROM batch_audit_manifest")] == ["FAILED"]
    assert [r["status"] for r in _rows(audit_engine, "SELECT status FROM subbatch_audit_manifest")] == ["FAILED"]


async def test_exception_in_load_is_audited_and_reraised(loader, audit_engine):
    loader._duckdb_loader = _FakeDuckDB(error=RuntimeError("pg connection lost"))

    with pytest.raises(RuntimeError, match="pg connection lost"):
        await loader.load_table("empresa")

    assert [r["status"] for r in _rows(audit_engine, "SELECT status FROM file_audit_manifest")] == ["FAILED"]


async def test_audit_failure_does_not_stop_the_load(loader):
    loader._duckdb_loader = _FakeDuckDB(result=(True, None, 7))
    loader.audit_service.create_file_manifest = lambda *_a, **_k: (_ for _ in ()).throw(RuntimeError("audit down"))

    assert await loader.load_table("empresa") == (True, None, 7)
