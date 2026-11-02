from pathlib import Path
from types import SimpleNamespace

from src.core.pipeline import ReceitaCNPJPipeline
from src.core.storage import LocalStorageBackend


def make_pipeline(tmp_path: Path):
    pipeline = object.__new__(ReceitaCNPJPipeline)
    pipeline.config = SimpleNamespace(
        year=2026,
        month=11,
        pipeline=SimpleNamespace(year=None, month=None),
    )
    pipeline.storage_backend = LocalStorageBackend(tmp_path / "objects")
    return pipeline


def test_parquet_outputs_are_persisted_with_period_scoped_keys(tmp_path: Path):
    pipeline = make_pipeline(tmp_path)
    output = tmp_path / "conversion"
    output.mkdir()
    parquet = output / "empresa.parquet"
    parquet.write_bytes(b"PAR1-test-data")

    stored = pipeline._persist_parquet_artifacts(output)

    expected_key = "processed/parquet/2026/11/empresa.parquet"
    assert stored["empresa.parquet"]["storage_key"] == expected_key
    assert stored["empresa.parquet"]["manifest_key"] == f"metadata/{expected_key}.json"
    assert pipeline.storage_backend.exists(expected_key)
    assert pipeline.storage_backend.exists(f"metadata/{expected_key}.json")


def test_existing_parquet_are_restored_into_local_workspace(tmp_path: Path):
    pipeline = make_pipeline(tmp_path)
    source = tmp_path / "source.parquet"
    source.write_bytes(b"PAR1-test-data")
    key = "processed/parquet/2026/11/empresa.parquet"
    pipeline.storage_backend.upload_file(source, key)

    output = tmp_path / "conversion"
    restored = pipeline._restore_parquet_artifacts(output)

    assert restored == [output / "empresa.parquet"]
    assert restored[0].read_bytes() == b"PAR1-test-data"
