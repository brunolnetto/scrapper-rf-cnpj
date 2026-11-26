"""Download/extraction failures must surface instead of being dropped (issue #28)."""

from types import SimpleNamespace

import pytest

from src.core.services.download.service import DownloadError, FileDownloadService


def _service(max_workers: int) -> FileDownloadService:
    return FileDownloadService(max_workers=max_workers, config=None, storage_backend=None)


def _audits():
    return [
        SimpleNamespace(entity_name="empresa", source_files=["Empresas0.zip"]),
        SimpleNamespace(entity_name="estabelecimento", source_files=["Estabelecimentos0.zip"]),
        SimpleNamespace(entity_name="socios", source_files=["Socios0.zip"]),
    ]


def _fail_for(entity_name: str):
    def fake(audit, *_args):
        if audit.entity_name == entity_name:
            raise OSError("connection reset")
        audit.processed = True
        return audit

    return fake


@pytest.mark.parametrize("parallel,max_workers", [(False, 1), (True, 3)], ids=["serial", "parallel"])
def test_failed_audit_raises_download_error(monkeypatch, tmp_path, parallel, max_workers):
    service = _service(max_workers)
    monkeypatch.setattr(service, "_download_and_extract_files", _fail_for("estabelecimento"))
    audits = _audits()

    with pytest.raises(DownloadError) as exc_info:
        service.download_and_extract(
            audits, "https://example.test", str(tmp_path / "d"), str(tmp_path / "e"), parallel=parallel
        )

    assert list(exc_info.value.failures) == ["estabelecimento"]
    assert "connection reset" in str(exc_info.value)
    # The remaining audits are still attempted before the error is raised.
    assert getattr(audits[0], "processed", False)
    assert getattr(audits[2], "processed", False)


@pytest.mark.parametrize("parallel,max_workers", [(False, 1), (True, 3)], ids=["serial", "parallel"])
def test_all_successful_returns_every_audit(monkeypatch, tmp_path, parallel, max_workers):
    service = _service(max_workers)
    monkeypatch.setattr(service, "_download_and_extract_files", _fail_for("nothing"))
    audits = _audits()

    result = service.download_and_extract(
        audits, "https://example.test", str(tmp_path / "d"), str(tmp_path / "e"), parallel=parallel
    )

    assert sorted(a.entity_name for a in result) == ["empresa", "estabelecimento", "socios"]
    assert None not in result


def test_single_file_error_propagates_with_filename(monkeypatch, tmp_path):
    service = _service(1)

    def broken_download(*_args, **_kwargs):
        raise OSError("truncated")

    monkeypatch.setattr(service, "_download_zipfile", broken_download)
    audit = SimpleNamespace(entity_name="empresa", source_files=["Empresas0.zip"])

    with pytest.raises(RuntimeError, match="Empresas0.zip: truncated"):
        service._download_and_extract_file(
            audit, "https://example.test", "Empresas0.zip", str(tmp_path), str(tmp_path), False
        )
