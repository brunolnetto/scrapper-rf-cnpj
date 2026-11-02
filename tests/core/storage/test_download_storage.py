from io import BytesIO
from pathlib import Path
from types import SimpleNamespace
from zipfile import ZipFile

from src.core.services.download.service import FileDownloadService
from src.core.storage import LocalStorageBackend


class FakeResponse:
    def __init__(self, payload: bytes):
        self.payload = payload
        self.headers = {"Content-Length": str(len(payload))}

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def raise_for_status(self):
        return None

    def iter_content(self, chunk_size):
        for offset in range(0, len(self.payload), chunk_size):
            yield self.payload[offset : offset + chunk_size]


def make_zip() -> bytes:
    buffer = BytesIO()
    with ZipFile(buffer, "w") as archive:
        archive.writestr("Empresas.csv", "cnpj_basico;razao_social\n123;Example\n")
    return buffer.getvalue()


def test_download_and_extract_persist_artifacts(monkeypatch, tmp_path: Path):
    import requests

    payload = make_zip()
    monkeypatch.setattr(requests, "get", lambda *args, **kwargs: FakeResponse(payload))

    config = SimpleNamespace(
        year=2026,
        month=11,
        pipeline=SimpleNamespace(year=2026, month=11),
        is_development_mode=lambda: False,
        get_max_files_per_blob=lambda: None,
    )
    storage = LocalStorageBackend(tmp_path / "objects")
    service = FileDownloadService(max_workers=1, config=config, storage_backend=storage)
    audit = SimpleNamespace(downloaded_at=None, processed_at=None)

    download_dir = tmp_path / "downloads"
    extract_dir = tmp_path / "extracted"

    service._download_zipfile(audit, "https://example.test", "Empresas.zip", str(download_dir), False)
    service._extract_zipfile(audit, "Empresas.zip", str(download_dir), str(extract_dir))

    assert storage.exists("raw/zip/2026/11/Empresas.zip")
    assert storage.exists("raw/extracted/2026/11/Empresas/Empresas.csv")
    assert (extract_dir / "Empresas.csv").is_file()
    assert audit.downloaded_at is not None
    assert audit.processed_at is not None
