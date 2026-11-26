"""FullETL builds audit metadata from the downloaded audits (issue #34)."""

from types import SimpleNamespace

import pytest

from src.core import strategies as strategies_module
from src.core.strategies import FullETLStrategy


class _Loader:
    async def load_data(self, metadata):
        return metadata

    async def close_resources(self):
        return None


class _Pipeline:
    def __init__(self):
        self.calls = []
        self.data_loader = _Loader()
        self.audit_service = SimpleNamespace(
            create_audit_metadata=self._create_audit_metadata,
            insert_audits=lambda _md, **_kwargs: None,
        )

    def _create_audit_metadata(self, audits, download_path):
        self.calls.append(("create_audit_metadata", audits, download_path))
        return {"from": "downloaded", "audits": audits}

    def retrieve_data(self):
        return ["audit-empresa"]

    def create_audit_metadata_from_existing_csvs(self):
        # Recorded rather than raised: the old code swallowed any exception here.
        self.calls.append(("from_existing_csvs",))
        return {"from": "extracted-csvs"}

    def convert_to_parquet(self, metadata):
        self.calls.append(("convert", metadata))


@pytest.fixture
def no_db_admin(monkeypatch):
    monkeypatch.setattr(strategies_module, "create_database_if_not_exists", lambda *_a: None)
    monkeypatch.setattr(strategies_module, "get_table_row_counts", lambda *_a: {})


@pytest.mark.usefixtures("no_db_admin")
def test_full_etl_uses_downloaded_audits_even_with_existing_parquet(tmp_path):
    conversion = tmp_path / "parquet"
    conversion.mkdir()
    (conversion / "simples.parquet").write_bytes(b"stale")

    config = SimpleNamespace(
        pipeline=SimpleNamespace(
            database=SimpleNamespace(
                user="u", password="p", host="h", port=5432, maintenance_db="postgres", database_name="db"
            ),
            delete_files=False,
            get_temporal_download_path=lambda _y, _m: tmp_path / "dl",
            get_temporal_conversion_path=lambda _y, _m: conversion,
        ),
    )
    pipeline = _Pipeline()

    result = FullETLStrategy().execute(pipeline, config, year=2026, month=11)

    assert ("from_existing_csvs",) not in pipeline.calls
    assert result == {"from": "downloaded", "audits": ["audit-empresa"]}
    assert pipeline.calls[0] == ("create_audit_metadata", ["audit-empresa"], str(tmp_path / "dl"))
    assert ("convert", result) in pipeline.calls
