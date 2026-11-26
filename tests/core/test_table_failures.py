"""Per-table conversion/load failures must fail the run (issue #30)."""

from pathlib import Path
from types import SimpleNamespace

import pytest

from src.core.pipeline import ReceitaCNPJPipeline
from src.core.schemas import AuditMetadata
from src.core.services.conversion import service as conversion_service
from src.core.services.conversion.service import ConversionError, convert_csvs_to_parquet
from src.core.services.loading.service import FileLoadingService, LoadError


# --------------------------------------------------------------------------
# Conversion
# --------------------------------------------------------------------------


@pytest.fixture
def no_db_columns(monkeypatch):
    monkeypatch.setattr(conversion_service, "_get_source_columns", lambda _t: [])


def _write_csv(directory: Path, name: str) -> str:
    (directory / name).write_text("a;b\n1;2\n")
    return name


@pytest.mark.usefixtures("no_db_columns")
def test_conversion_raises_when_a_table_fails(monkeypatch, tmp_path):
    def fake_convert(table_name, *_args):
        if table_name == "socios":
            return "[ERROR] socios: bad row"
        return f"[OK] {table_name}"

    monkeypatch.setattr(conversion_service, "convert_table_csvs_multifile", fake_convert)
    audit_map = {
        "empresa": {"E.zip": [_write_csv(tmp_path, "E.csv")]},
        "socios": {"S.zip": [_write_csv(tmp_path, "S.csv")]},
    }

    with pytest.raises(ConversionError) as exc_info:
        convert_csvs_to_parquet(audit_map, tmp_path, tmp_path / "out")

    assert list(exc_info.value.failures) == ["socios"]


@pytest.mark.usefixtures("no_db_columns")
def test_conversion_raises_when_source_files_are_missing(monkeypatch, tmp_path):
    monkeypatch.setattr(conversion_service, "convert_table_csvs_multifile", lambda t, *_a: f"[OK] {t}")
    audit_map = {
        "empresa": {"E.zip": [_write_csv(tmp_path, "E.csv")]},
        "simples": {"Simples.zip": ["missing.csv"]},
    }

    with pytest.raises(ConversionError) as exc_info:
        convert_csvs_to_parquet(audit_map, tmp_path, tmp_path / "out")

    assert exc_info.value.failures == {"simples": "no source files found"}


@pytest.mark.usefixtures("no_db_columns")
def test_conversion_succeeds_when_every_table_converts(monkeypatch, tmp_path):
    monkeypatch.setattr(conversion_service, "convert_table_csvs_multifile", lambda t, *_a: f"[OK] {t}")
    audit_map = {"empresa": {"E.zip": [_write_csv(tmp_path, "E.csv")]}}

    convert_csvs_to_parquet(audit_map, tmp_path, tmp_path / "out")


def test_pipeline_audits_successes_and_drops_partial_output(monkeypatch, tmp_path):
    out_dir = tmp_path / "parquet"
    out_dir.mkdir()

    def fake_smart(_audit_map, _extract, output_dir, _delimiter):
        (output_dir / "empresa.parquet").write_bytes(b"ok")
        (output_dir / "socios.parquet").write_bytes(b"partial")
        raise ConversionError({"socios": "bad row"})

    monkeypatch.setattr(conversion_service, "convert_csvs_to_parquet_smart", fake_smart)

    audited = []
    pipeline = ReceitaCNPJPipeline.__new__(ReceitaCNPJPipeline)
    pipeline.config = SimpleNamespace(
        year=2026,
        month=11,
        pipeline=SimpleNamespace(
            conversion=SimpleNamespace(workers=1),
            data_source=SimpleNamespace(delimiter=";"),
            get_temporal_conversion_path=lambda _y, _m: out_dir,
            get_temporal_extraction_path=lambda _y, _m: tmp_path,
        ),
    )
    pipeline.dev_filter = SimpleNamespace(log_conversion_summary=lambda _m: None)
    pipeline.storage_backend = None
    pipeline._audit_service = SimpleNamespace(
        update_table_audit_after_conversion=lambda table, *_a, **_k: audited.append(table)
    )
    metadata = SimpleNamespace(
        tablename_to_zipfile_to_files={"empresa": {"E.zip": ["E.csv"]}, "socios": {"S.zip": ["S.csv"]}}
    )

    with pytest.raises(ConversionError):
        pipeline.convert_to_parquet(metadata)

    assert audited == ["empresa"]
    assert (out_dir / "empresa.parquet").exists()
    assert not (out_dir / "socios.parquet").exists()


# --------------------------------------------------------------------------
# Loading
# --------------------------------------------------------------------------


class _Monitor:
    def should_prevent_processing(self):
        return False

    def is_memory_pressure_high(self):
        return False


def _loader() -> FileLoadingService:
    loader = FileLoadingService.__new__(FileLoadingService)
    loader.memory_monitor = _Monitor()
    loader.audit_service = None
    return loader


def _metadata(*tables):
    audits = [SimpleNamespace(entity_name=t, completed_at=None) for t in tables]
    return AuditMetadata.model_construct(
        audit_list=audits,
        tablename_to_zipfile_to_files={t: {"x": [f"{t}.parquet"]} for t in tables},
    )


async def test_load_data_raises_when_a_table_fails(monkeypatch):
    loader = _loader()

    async def fake_load_table(table_name, **_kwargs):
        if table_name == "cnae":
            return False, "contract violation", 0
        if table_name == "pais":
            raise FileNotFoundError("Parquet file not found for 'pais'")
        return True, None, 10

    monkeypatch.setattr(loader, "load_table", fake_load_table)
    monkeypatch.setattr(loader, "_perform_memory_precheck", lambda _t: True)
    monkeypatch.setattr(loader, "_create_table_context", lambda _t: None)
    metadata = _metadata("cnae", "pais", "moti")

    with pytest.raises(LoadError) as exc_info:
        await loader.load_data(metadata)

    assert set(exc_info.value.failures) == {"cnae", "pais"}
    # Every table was attempted, including the one after the exception.
    assert all(a.completed_at is not None for a in metadata.audit_list)


async def test_load_data_returns_metadata_when_all_tables_load(monkeypatch):
    loader = _loader()

    async def ok(**_kwargs):
        return True, None, 1

    monkeypatch.setattr(loader, "load_table", ok)
    monkeypatch.setattr(loader, "_perform_memory_precheck", lambda _t: True)
    monkeypatch.setattr(loader, "_create_table_context", lambda _t: None)
    metadata = _metadata("cnae", "moti")

    assert await loader.load_data(metadata) is metadata
