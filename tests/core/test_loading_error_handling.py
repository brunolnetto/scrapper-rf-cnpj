"""Loading helpers report errors instead of hiding them (issue #61)."""

import json
from datetime import datetime
from types import SimpleNamespace
from uuid import UUID

from src.core.services.loading import service as loading_service
from src.core.services.loading.file_handler import FileHandler
from src.core.services.loading.service import FileLoadingService


def _loader():
    return FileLoadingService.__new__(FileLoadingService)


def test_notes_with_datetime_and_uuid_are_kept():
    notes = {"at": datetime(2026, 11, 26, 12, 0), "id": UUID(int=1), "rows": 3}

    decoded = json.loads(_loader()._serialize_notes(notes))

    assert decoded == {"at": "2026-11-26 12:00:00", "id": str(UUID(int=1)), "rows": 3}


def test_unserializable_notes_fall_back_to_marker(monkeypatch):
    warnings = []
    monkeypatch.setattr(loading_service.logger, "warning", warnings.append)
    circular = {}
    circular["self"] = circular

    assert json.loads(_loader()._serialize_notes(circular)) == {"error": "notes_serialization_failed"}
    assert warnings


def test_manifest_lookup_failure_is_logged(monkeypatch):
    warnings = []
    monkeypatch.setattr(loading_service.logger, "warning", warnings.append)
    loader = _loader()

    def boom():
        raise RuntimeError("db down")

    loader.audit_service = SimpleNamespace(database=SimpleNamespace(engine=SimpleNamespace(connect=boom)))

    assert loader._find_existing_file_manifest("x.parquet", "id") is None
    assert any("db down" in w for w in warnings)


def test_format_detection(tmp_path):
    handler = FileHandler.__new__(FileHandler)
    handler.config = SimpleNamespace(pipeline=SimpleNamespace(data_source=SimpleNamespace(encoding="utf-8")))
    (tmp_path / "a.parquet").write_bytes(b"PAR1....")
    (tmp_path / "b.csv").write_text("x;y\n1;2\n")

    assert handler._detect_format_by_content(str(tmp_path / "a.parquet")) == "parquet"
    assert handler._detect_format_by_content(str(tmp_path / "b.csv")) == "csv"
    assert handler._detect_format_by_content(str(tmp_path / "missing")) is None
