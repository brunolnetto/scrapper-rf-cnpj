"""HTTP timeout and size verification for ZIP downloads (issue #29)."""

from types import SimpleNamespace

import pytest
import requests
from tenacity import RetryError

from src.core.services.download.service import (
    CONNECT_TIMEOUT_SECONDS,
    DEFAULT_READ_TIMEOUT_SECONDS,
    FileDownloadService,
    IncompleteDownloadError,
)

# Call the undecorated method so tests do not wait on tenacity's backoff.
_download_once = FileDownloadService._download_zipfile.__wrapped__


class FakeResponse:
    def __init__(self, payload: bytes, content_length: int | None = None):
        self.payload = payload
        self.headers = {} if content_length is None else {"Content-Length": str(content_length)}

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def raise_for_status(self):
        return None

    def iter_content(self, chunk_size):
        for offset in range(0, len(self.payload), chunk_size):
            yield self.payload[offset : offset + chunk_size]


def _service(timeout_seconds=None) -> FileDownloadService:
    service = FileDownloadService(max_workers=1, config=None, storage_backend=None)
    if timeout_seconds is not None:
        service.config = SimpleNamespace(
            pipeline=SimpleNamespace(download=SimpleNamespace(timeout_seconds=timeout_seconds)),
        )
    return service


def _audit():
    return SimpleNamespace(downloaded_at=None, notes=None)


def test_request_uses_configured_timeout(monkeypatch, tmp_path):
    seen = {}

    def fake_get(*_args, **kwargs):
        seen.update(kwargs)
        return FakeResponse(b"data", content_length=4)

    monkeypatch.setattr(requests, "get", fake_get)
    service = _service(timeout_seconds=120)

    _download_once(service, _audit(), "https://example.test", "A.zip", str(tmp_path), False)

    assert seen["timeout"] == (CONNECT_TIMEOUT_SECONDS, 120)


def test_request_timeout_defaults_without_config():
    assert _service()._request_timeout() == (CONNECT_TIMEOUT_SECONDS, DEFAULT_READ_TIMEOUT_SECONDS)


def test_truncated_download_raises_and_removes_partial_file(monkeypatch, tmp_path):
    monkeypatch.setattr(requests, "get", lambda *_a, **_k: FakeResponse(b"abc", content_length=10))

    with pytest.raises(IncompleteDownloadError, match="expected 10 bytes, got 3"):
        _download_once(_service(), _audit(), "https://example.test", "A.zip", str(tmp_path), False)

    assert not (tmp_path / "A.zip").exists()


def test_download_without_content_length_is_accepted(monkeypatch, tmp_path):
    monkeypatch.setattr(requests, "get", lambda *_a, **_k: FakeResponse(b"abc"))
    audit = _audit()

    _download_once(_service(), audit, "https://example.test", "A.zip", str(tmp_path), False)

    assert (tmp_path / "A.zip").read_bytes() == b"abc"
    assert audit.downloaded_at is not None


def test_read_timeout_is_retried_then_reraised(monkeypatch, tmp_path):
    calls = {"n": 0}

    def timing_out(*_a, **_k):
        calls["n"] += 1
        raise requests.exceptions.ReadTimeout("stalled")

    monkeypatch.setattr(requests, "get", timing_out)
    service = _service()
    retrying = FileDownloadService._download_zipfile.retry
    monkeypatch.setattr(retrying, "sleep", lambda _seconds: None)

    with pytest.raises(Exception) as exc_info:
        service._download_zipfile(_audit(), "https://example.test", "A.zip", str(tmp_path), False)

    assert calls["n"] == 3
    error = exc_info.value
    if isinstance(error, RetryError):
        error = error.last_attempt.exception()
    assert isinstance(error, requests.exceptions.ReadTimeout)
