"""Shared strategy helpers and step ordering (issue #40)."""

import asyncio
from types import SimpleNamespace

import pytest

from src.core.strategies import ConvertAndLoadStrategy, LoadOnlyStrategy, _cleanup_local_files, _load_into_database


class _Loader:
    def __init__(self, calls, error=None):
        self.calls, self.error = calls, error

    async def load_data(self, metadata):
        self.calls.append(("load", id(asyncio.get_running_loop())))
        if self.error:
            raise self.error
        return metadata

    async def close_resources(self):
        self.calls.append(("close", id(asyncio.get_running_loop())))


def test_load_and_close_share_one_event_loop():
    calls = []
    pipeline = SimpleNamespace(data_loader=_Loader(calls))

    assert _load_into_database(pipeline, "md") == "md"
    assert [c[0] for c in calls] == ["load", "close"]
    assert calls[0][1] == calls[1][1]


def test_close_runs_when_load_fails():
    calls = []
    pipeline = SimpleNamespace(data_loader=_Loader(calls, RuntimeError("boom")))

    with pytest.raises(RuntimeError, match="boom"):
        _load_into_database(pipeline, "md")
    assert [c[0] for c in calls] == ["load", "close"]


def _config(tmp_path, delete_files):
    return SimpleNamespace(
        pipeline=SimpleNamespace(
            delete_files=delete_files,
            get_temporal_download_path=lambda y, m: tmp_path / "dl" / f"{y}-{m}",
            get_temporal_extraction_path=lambda y, m: tmp_path / "ex" / f"{y}-{m}",
        )
    )


@pytest.mark.parametrize("extracted", [True, False])
def test_cleanup_respects_flags(tmp_path, extracted):
    for d in ("dl", "ex"):
        (tmp_path / d / "2026-11").mkdir(parents=True)

    _cleanup_local_files(_config(tmp_path, True), 2026, 11, extracted=extracted)

    assert not (tmp_path / "dl" / "2026-11").exists()
    assert (tmp_path / "ex" / "2026-11").exists() is (not extracted)


def test_cleanup_disabled_keeps_files(tmp_path):
    (tmp_path / "dl" / "2026-11").mkdir(parents=True)
    _cleanup_local_files(_config(tmp_path, False), 2026, 11)
    assert (tmp_path / "dl" / "2026-11").exists()


class _Pipeline:
    def __init__(self):
        self.calls = []
        self.data_loader = _Loader(self.calls)
        self.audit_service = SimpleNamespace(
            insert_audits=lambda _md, **_kw: self.calls.append(("insert",))
        )

    def create_audit_metadata_from_existing_csvs(self):
        return "md"

    def create_audit_metadata_from_existing_parquet(self):
        return "md"

    def convert_to_parquet(self, _metadata):
        self.calls.append(("convert",))


def test_convert_and_load_inserts_audits_before_converting():
    pipeline = _Pipeline()
    ConvertAndLoadStrategy().execute(pipeline, None)
    assert [c[0] for c in pipeline.calls] == ["insert", "convert", "load", "close"]


def test_load_only_inserts_then_loads():
    pipeline = _Pipeline()
    LoadOnlyStrategy().execute(pipeline, None)
    assert [c[0] for c in pipeline.calls] == ["insert", "load", "close"]
