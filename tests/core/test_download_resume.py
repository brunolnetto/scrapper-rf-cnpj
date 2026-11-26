"""Reuse previously downloaded archives instead of downloading again (issue #38)."""

from pathlib import Path
from types import SimpleNamespace

import pytest
import requests

from src.core.services.download.service import FileDownloadService
from src.core.storage import LocalStorageBackend, persist_artifact_manifest

_download_once = FileDownloadService._download_zipfile.__wrapped__

PAYLOAD = b"PK-fresh-archive-bytes"
KEY = "raw/zip/2026/11/Empresas0.zip"


class FakeResponse:
    def __init__(self, payload: bytes, content_length: bool = True):
        self.payload = payload
        self.headers = {"Content-Length": str(len(payload))} if content_length else {}
        self.body_read = False

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def raise_for_status(self):
        return None

    def iter_content(self, chunk_size):
        self.body_read = True
        for offset in range(0, len(self.payload), chunk_size):
            yield self.payload[offset : offset + chunk_size]


@pytest.fixture
def response(monkeypatch):
    holder = {}

    def fake_get(*_args, **_kwargs):
        holder["response"] = FakeResponse(PAYLOAD, holder.get("content_length", True))
        return holder["response"]

    monkeypatch.setattr(requests, "get", fake_get)
    return holder


def _service(tmp_path, with_storage=True):
    config = SimpleNamespace(
        year=2026,
        month=11,
        pipeline=SimpleNamespace(year=2026, month=11),
        is_development_mode=lambda: False,
    )
    storage = LocalStorageBackend(tmp_path / "objects")
    service = FileDownloadService(max_workers=1, config=config, storage_backend=storage)
    if not with_storage:
        service.storage_backend = storage = None
    return service, storage


def _audit():
    return SimpleNamespace(downloaded_at=None, notes=None)


def _run(service, downloads: Path):
    audit = _audit()
    _download_once(service, audit, "https://example.test", "Empresas0.zip", str(downloads), False)
    return audit


def test_local_archive_with_matching_size_is_reused(tmp_path, response):
    service, _ = _service(tmp_path, with_storage=False)
    downloads = tmp_path / "dl"
    downloads.mkdir()
    existing = b"X" * len(PAYLOAD)  # same size, different bytes: proves no rewrite
    (downloads / "Empresas0.zip").write_bytes(existing)

    audit = _run(service, downloads)

    assert response["response"].body_read is False
    assert (downloads / "Empresas0.zip").read_bytes() == existing
    assert audit.downloaded_at is not None


def test_local_archive_with_different_size_is_downloaded_again(tmp_path, response):
    service, _ = _service(tmp_path, with_storage=False)
    downloads = tmp_path / "dl"
    downloads.mkdir()
    (downloads / "Empresas0.zip").write_bytes(b"truncated")

    _run(service, downloads)

    assert response["response"].body_read is True
    assert (downloads / "Empresas0.zip").read_bytes() == PAYLOAD


def test_archive_is_restored_from_storage_when_manifest_matches(tmp_path, response):
    service, storage = _service(tmp_path)
    source = tmp_path / "previous.zip"
    source.write_bytes(PAYLOAD)
    storage.upload_file(source, KEY)
    persist_artifact_manifest(storage, source, KEY, stage="raw/zip", year=2026, month=11)

    downloads = tmp_path / "dl"
    _run(service, downloads)

    assert response["response"].body_read is False
    assert (downloads / "Empresas0.zip").read_bytes() == PAYLOAD


def test_storage_object_with_mismatched_manifest_is_not_reused(tmp_path, response):
    service, storage = _service(tmp_path)
    stale = tmp_path / "stale.zip"
    stale.write_bytes(b"older publication")
    storage.upload_file(stale, KEY)
    persist_artifact_manifest(storage, stale, KEY, stage="raw/zip", year=2026, month=11)

    downloads = tmp_path / "dl"
    _run(service, downloads)

    assert response["response"].body_read is True
    assert (downloads / "Empresas0.zip").read_bytes() == PAYLOAD
    assert storage.exists(KEY)


def test_without_content_length_nothing_is_reused(tmp_path, response):
    response["content_length"] = False
    service, _ = _service(tmp_path, with_storage=False)
    downloads = tmp_path / "dl"
    downloads.mkdir()
    (downloads / "Empresas0.zip").write_bytes(b"X" * len(PAYLOAD))

    _run(service, downloads)

    assert response["response"].body_read is True
    assert (downloads / "Empresas0.zip").read_bytes() == PAYLOAD


def test_reused_local_archive_is_persisted_when_storage_lacks_it(tmp_path, response):
    service, storage = _service(tmp_path)
    downloads = tmp_path / "dl"
    downloads.mkdir()
    (downloads / "Empresas0.zip").write_bytes(PAYLOAD)

    audit = _run(service, downloads)

    assert response["response"].body_read is False
    assert storage.exists(KEY)
    assert storage.exists(f"metadata/{KEY}.json")
    assert audit.notes["storage_manifests"] == [f"metadata/{KEY}.json"]
