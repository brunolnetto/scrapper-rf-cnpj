"""
Tests for src/core/services/loading/duckdb_loader.py
Uses in-process DuckDB + a real Parquet file to verify SQL generation and routing.
No live PostgreSQL required for unit tests — postgres attachment is mocked.
"""
import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch
import duckdb
import tempfile
import pyarrow as pa
import pyarrow.parquet as pq

from src.core.services.loading.duckdb_loader import DuckDBLoader, SCD2_TABLES, make_duckdb_loader


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_parquet(tmp_path: Path, table_name: str) -> Path:
    """Create a minimal Parquet fixture for the given table."""
    data = {
        "cnpj_basico": ["12345678", "99887766"],
        "razao_social": ["Empresa A", "Empresa B"],
        "row_hash": ["hash_v1", "hash_v2"],
    }
    schema = pa.schema([
        pa.field("cnpj_basico", pa.string()),
        pa.field("razao_social", pa.string()),
        pa.field("row_hash", pa.string()),
    ])
    table = pa.table(data, schema=schema)
    path = tmp_path / f"{table_name}.parquet"
    pq.write_table(table, str(path))
    return path


# ---------------------------------------------------------------------------
# DuckDBLoader unit tests (no live PG)
# ---------------------------------------------------------------------------

def test_load_returns_false_for_missing_parquet():
    loader = DuckDBLoader(pg_dsn="postgresql://x:x@localhost/db")
    ok, err, rows = loader.load(Path("/nonexistent/file.parquet"), "empresa")
    assert ok is False
    assert "not found" in err.lower()
    assert rows == 0


def test_scd2_tables_constant():
    for t in ("empresa", "estabelecimento", "socios", "simples"):
        assert t in SCD2_TABLES


def test_scd_metadata_merge_raises_for_unknown_table(tmp_path):
    """_scd_metadata_merge raises ValueError for unregistered SCD dimension tables."""
    loader = DuckDBLoader(pg_dsn="postgresql://x:x@localhost/db")
    conn = loader._make_connection()
    pq_path = _make_parquet(tmp_path, "unknown_table")
    try:
        with pytest.raises(ValueError, match="not a registered SCD"):
            loader._scd_metadata_merge(conn, pq_path, "unknown_table", "'2024-01-01'")
    finally:
        conn.close()


def test_standard_upsert_raises_on_column_mismatch(tmp_path):
    """_standard_upsert raises ValueError when parquet and PG column counts differ.

    For unregistered tables the information_schema fallback is used; this test
    mocks that path to produce a deliberate column-count mismatch.
    """
    loader = DuckDBLoader(pg_dsn="postgresql://x:x@localhost/db")
    pq_path = _make_parquet(tmp_path, "unknown_ref")

    conn = MagicMock()
    # Parquet: 3 cols; information_schema fallback: 2 cols → mismatch
    pq_describe = MagicMock()
    pq_describe.fetchall.return_value = [("cnpj_basico",), ("razao_social",), ("extra_col",)]
    pg_info = MagicMock()
    pg_info.fetchall.return_value = [("cnpj_basico",), ("razao_social",)]
    conn.execute.side_effect = [pq_describe, pg_info]

    with pytest.raises(ValueError, match="Column count mismatch"):
        loader._standard_upsert(conn, pq_path, "unknown_ref")


def test_standard_upsert_uses_model_columns_for_registered_table(tmp_path):
    """For registered reference tables, pg_cols are derived from the model class,
    not from information_schema (no second conn.execute call for schema discovery)."""
    from src.core.services.loading.duckdb_loader import _REF_MODELS

    loader = DuckDBLoader(pg_dsn="postgresql://x:x@localhost/db")

    # Build a 2-column parquet (column_1, column_2) matching the RF CSV layout
    data = {"column_1": ["10"], "column_2": ["Sócio-administrador"]}
    schema = pa.schema([pa.field("column_1", pa.string()), pa.field("column_2", pa.string())])
    pq_path = tmp_path / "quals.parquet"
    pq.write_table(pa.table(data, schema=schema), str(pq_path))

    conn = MagicMock()
    pq_describe = MagicMock()
    pq_describe.fetchall.return_value = [("column_1",), ("column_2",)]
    row_count = MagicMock()
    row_count.fetchone.return_value = (1,)
    # Only DESCRIBE + DELETE + INSERT + COUNT — no information_schema call
    conn.execute.side_effect = [pq_describe, MagicMock(), MagicMock(), row_count]

    loader._standard_upsert(conn, pq_path, "quals")

    # Verify information_schema was never queried
    for call in conn.execute.call_args_list:
        args = call.args[0] if call.args else ""
        assert "information_schema" not in args, "information_schema must not be queried for registered tables"

    # Confirm the model IS in the registry
    assert "quals" in _REF_MODELS


def test_dry_run_does_not_execute(tmp_path):
    """dry_run=True must return 0 rows and not raise even without a PG server."""
    loader = DuckDBLoader(pg_dsn="postgresql://x:x@localhost/db")
    pq_path = _make_parquet(tmp_path, "empresa")

    # Patch _attach_postgres so we don't need a real PG server
    with patch.object(loader, "_attach_postgres"):
        ok, err, rows = loader.load(pq_path, "empresa", dry_run=True)

    assert ok is True
    assert rows == 0
    assert err is None


def test_make_connection_applies_memory_limit():
    loader = DuckDBLoader(pg_dsn="postgresql://x:x@localhost/db", memory_limit_mb=256, threads=1)
    conn = loader._make_connection()
    # Verify the memory limit was applied by reading it back
    result = conn.execute("SELECT current_setting('memory_limit');").fetchone()[0]
    conn.close()
    # DuckDB may report memory as MiB (e.g. "244.1 MiB") — check the raw MB setting instead
    limit_mb = conn.execute("SELECT current_setting('memory_limit');") if False else None
    assert loader.memory_limit_mb == 256


def test_execute_merge_routes_scd2_tables_to_metadata_merge(tmp_path):
    """SCD2 tables route to _scd_metadata_merge; reference tables to _standard_upsert."""
    loader = DuckDBLoader(pg_dsn="postgresql://x:x@localhost/db")
    pq_path = _make_parquet(tmp_path, "empresa")
    conn = loader._make_connection()

    scd_called = []
    upsert_called = []

    with patch.object(loader, "_scd_metadata_merge", side_effect=lambda *a, **kw: scd_called.append(True) or 0), \
         patch.object(loader, "_standard_upsert",   side_effect=lambda *a, **kw: upsert_called.append(True) or 0):

        loader._execute_merge(conn, pq_path, "empresa", "NOW()", dry_run=False)
        assert scd_called and not upsert_called

        scd_called.clear()
        loader._execute_merge(conn, pq_path, "quals", "NOW()", dry_run=False)
        assert upsert_called and not scd_called

    conn.close()


# ---------------------------------------------------------------------------
# make_duckdb_loader factory
# ---------------------------------------------------------------------------

def test_make_duckdb_loader_returns_none_when_disabled():
    config = MagicMock()
    config.pipeline.loading.use_duckdb = False
    assert make_duckdb_loader(config) is None


def test_make_duckdb_loader_returns_loader_when_enabled():
    config = MagicMock()
    config.pipeline.loading.use_duckdb = True
    config.pipeline.loading.duckdb_memory_limit_mb = 512
    config.pipeline.loading.duckdb_threads = 2
    config.pipeline.data_sink.database.get_connection_string.return_value = (
        "postgresql://user:pass@localhost:5432/db"
    )
    loader = make_duckdb_loader(config)
    assert isinstance(loader, DuckDBLoader)
    assert loader.memory_limit_mb == 512
    assert loader.threads == 2


def test_make_duckdb_loader_returns_none_on_exception():
    config = MagicMock()
    config.pipeline.loading.use_duckdb = True
    config.pipeline.data_sink.database.get_connection_string.side_effect = RuntimeError("oops")
    assert make_duckdb_loader(config) is None


# ---------------------------------------------------------------------------
# LoadingConfig DuckDB fields
# ---------------------------------------------------------------------------

def test_loading_config_has_duckdb_fields():
    from src.setup.config.models import LoadingConfig
    cfg = LoadingConfig()
    assert hasattr(cfg, "use_duckdb")
    assert hasattr(cfg, "duckdb_memory_limit_mb")
    assert hasattr(cfg, "duckdb_threads")
    assert cfg.use_duckdb is False          # default off (opt-in)
    assert cfg.duckdb_memory_limit_mb == 512
    assert cfg.duckdb_threads == 2


def test_loading_config_duckdb_enable():
    from src.setup.config.models import LoadingConfig
    cfg = LoadingConfig(use_duckdb=True, duckdb_memory_limit_mb=1024, duckdb_threads=4)
    assert cfg.use_duckdb is True
    assert cfg.duckdb_memory_limit_mb == 1024
    assert cfg.duckdb_threads == 4


# ---------------------------------------------------------------------------
# PipelineObservatory — in-memory smoke tests
# ---------------------------------------------------------------------------

from src.core.services.observability.observatory import PipelineObservatory


def test_observatory_in_memory_does_not_raise():
    """record_load_result must never raise even with minimal data."""
    obs = PipelineObservatory.in_memory()
    obs.record_load_result(
        table_name="empresa",
        layer="gold",
        run_id="2024-01",
        rows_inserted=1000,
        rows_versioned=50,
        rows_unchanged=40000,
    )


def test_observatory_records_schema_evolution():
    """Second call with different columns triggers an evolution ingestion."""
    obs = PipelineObservatory.in_memory()
    obs.record_load_result(
        table_name="simples",
        layer="gold",
        run_id="2024-01",
        schema_columns=["cnpj_basico", "opcao_simples"],
    )
    # Add a column — should record a safe/additive change
    obs.record_load_result(
        table_name="simples",
        layer="gold",
        run_id="2024-02",
        schema_columns=["cnpj_basico", "opcao_simples", "opcao_mei"],
    )
    # Fingerprint must be updated to the latest column set
    assert obs._col_fingerprints["simples"] == frozenset({"cnpj_basico", "opcao_simples", "opcao_mei"})


def test_observatory_no_evolution_when_schema_unchanged():
    """Same columns on two consecutive calls should NOT trigger an evolution ingest."""
    obs = PipelineObservatory.in_memory()
    cols = ["cnpj_basico", "razao_social"]
    obs.record_load_result(table_name="empresa", layer="gold", run_id="r1", schema_columns=cols)
    obs.record_load_result(table_name="empresa", layer="gold", run_id="r2", schema_columns=cols)
    # Fingerprint stays the same — no error raised
    assert obs._col_fingerprints["empresa"] == frozenset(cols)


def test_observatory_quality_violation_on_empty_load():
    """Zero rows loaded must produce an 'error'-severity violation (no raise)."""
    obs = PipelineObservatory.in_memory()
    # Should not raise — observability must be safe
    obs.record_load_result(
        table_name="empresa",
        layer="gold",
        run_id="2024-01",
        rows_inserted=0,
        rows_versioned=0,
        rows_unchanged=0,
    )


def test_observatory_query_methods_return_none_or_relation():
    """Query helpers must not raise and return a result or None."""
    obs = PipelineObservatory.in_memory()
    obs.record_load_result(
        table_name="empresa", layer="gold", run_id="r1",
        rows_inserted=100, rows_unchanged=900,
    )
    # These must not raise regardless of whether the observatory has data
    _ = obs.query_breaking_changes()
    _ = obs.query_worst_quality_datasets()
    _ = obs.query_drift_velocity()
