"""`--download --load` (101) strategy (issue #31)."""

from pathlib import Path
from types import SimpleNamespace

import pytest

from src.core.strategies import DownloadAndLoadStrategy, StrategyFactory


class _Loader:
    def __init__(self, calls, error=None):
        self.calls = calls
        self.error = error

    async def load_data(self, metadata):
        self.calls.append(("load", id(self), metadata))
        if self.error:
            raise self.error
        return metadata

    async def close_resources(self):
        self.calls.append(("close", id(self)))


class _Pipeline:
    def __init__(self, audits, load_error=None):
        self.calls = []
        self._audits = audits
        self._load_error = load_error
        self.audit_service = SimpleNamespace(
            create_audit_metadata=lambda audits, path: self._record("metadata", path) or {"audits": audits},
            insert_audits=lambda _md, create_file_manifests: self._record("insert", create_file_manifests),
        )

    def _record(self, *event):
        self.calls.append(event)

    def retrieve_data(self):
        self.calls.append(("retrieve",))
        return self._audits

    def convert_to_parquet(self, metadata):
        self.calls.append(("convert", metadata))
        return Path("/tmp/parquet")

    @property
    def data_loader(self):
        # Mirrors the real property: a fresh instance on every access.
        return _Loader(self.calls, self._load_error)


def _config(tmp_path, delete_files=False):
    return SimpleNamespace(
        year=2026,
        month=11,
        pipeline=SimpleNamespace(
            delete_files=delete_files,
            get_temporal_download_path=lambda y, m: tmp_path / "dl" / f"{y}-{m:02d}",
            get_temporal_extraction_path=lambda y, m: tmp_path / "ex" / f"{y}-{m:02d}",
        ),
    )


def test_factory_maps_101_to_download_and_load():
    strategy = StrategyFactory.create_strategy(download=True, convert=False, load=True)
    assert isinstance(strategy, DownloadAndLoadStrategy)


def test_download_load_converts_then_loads_with_one_loader(tmp_path):
    pipeline = _Pipeline(audits=["a1"])

    result = DownloadAndLoadStrategy().execute(pipeline, _config(tmp_path))

    steps = [c[0] for c in pipeline.calls]
    assert steps == ["retrieve", "metadata", "insert", "convert", "load", "close"]
    load_call = next(c for c in pipeline.calls if c[0] == "load")
    close_call = next(c for c in pipeline.calls if c[0] == "close")
    assert load_call[1] == close_call[1], "resources must be closed on the loader that loaded"
    assert result == {"audits": ["a1"]}


def test_download_load_returns_none_when_nothing_downloaded(tmp_path):
    pipeline = _Pipeline(audits=[])
    assert DownloadAndLoadStrategy().execute(pipeline, _config(tmp_path)) is None
    assert [c[0] for c in pipeline.calls] == ["retrieve"]


def test_download_load_propagates_load_failure_and_closes(tmp_path):
    pipeline = _Pipeline(audits=["a1"], load_error=RuntimeError("load failed"))

    with pytest.raises(RuntimeError, match="load failed"):
        DownloadAndLoadStrategy().execute(pipeline, _config(tmp_path))

    steps = [c[0] for c in pipeline.calls]
    assert steps.count("retrieve") == 1, "must not re-download on failure"
    assert steps[-1] == "close"
