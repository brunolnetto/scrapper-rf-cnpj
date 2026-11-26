"""Extraction moves files into place instead of copying them (issue #39)."""

import errno
import os
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace
from zipfile import ZipFile

import pytest

from src.core.services.download import service as download_module
from src.core.services.download.service import FileDownloadService, _move_into_place


def _zip(downloads: Path) -> None:
    buffer = BytesIO()
    with ZipFile(buffer, "w") as archive:
        archive.writestr("K3241.EMPRECSV", "1;A\n")
        archive.writestr("nested/K3241.SOCIOCSV", "2;B\n")
    downloads.mkdir(parents=True, exist_ok=True)
    (downloads / "Empresas0.zip").write_bytes(buffer.getvalue())


def _service() -> FileDownloadService:
    return FileDownloadService(max_workers=1, config=None, storage_backend=None)


def test_extracted_files_are_renamed_from_a_sibling_workspace(monkeypatch, tmp_path):
    downloads, extract_dir = tmp_path / "dl", tmp_path / "EXTRACTED" / "2026-11"
    _zip(downloads)
    moves = []
    real_replace = os.replace

    def spy_replace(src, dst):
        moves.append((Path(src), Path(dst)))
        return real_replace(src, dst)

    monkeypatch.setattr(download_module.os, "replace", spy_replace)
    copies = []
    real_copy = download_module.shutil.copyfileobj

    def spy_copy(*args, **kwargs):
        copies.append(args)
        return real_copy(*args, **kwargs)

    monkeypatch.setattr(download_module.shutil, "copyfileobj", spy_copy)

    audit = _service()._extract_zipfile(SimpleNamespace(), "Empresas0.zip", str(downloads), str(extract_dir))

    assert sorted(dst.relative_to(extract_dir).as_posix() for _, dst in moves) == [
        "K3241.EMPRECSV",
        "nested/K3241.SOCIOCSV",
    ]
    # Workspace is on the same filesystem, next to the extraction directory.
    assert all(src.is_relative_to(extract_dir.parent) for src, _ in moves)
    assert all(not src.is_relative_to(extract_dir) for src, _ in moves)
    # Files were renamed, never copied by the service, and the workspace is gone.
    assert (extract_dir / "K3241.EMPRECSV").read_text() == "1;A\n"
    assert (extract_dir / "nested" / "K3241.SOCIOCSV").read_text() == "2;B\n"
    assert not [p for p in extract_dir.parent.iterdir() if p.name.startswith(".cnpj-extract-")]
    assert audit.processed_at is not None
    # Only extract_zip_file (ZIP -> workspace) streams bytes; no second copy.
    assert len(copies) == 2


def test_move_falls_back_to_copy_across_filesystems(monkeypatch, tmp_path):
    source, target = tmp_path / "a.csv", tmp_path / "out" / "a.csv"
    source.write_text("data")
    target.parent.mkdir()
    real_replace = os.replace
    calls = {"n": 0}

    def cross_device_once(src, dst):
        calls["n"] += 1
        if calls["n"] == 1:
            raise OSError(errno.EXDEV, "Invalid cross-device link")
        return real_replace(src, dst)

    monkeypatch.setattr(download_module.os, "replace", cross_device_once)

    _move_into_place(source, target)

    assert target.read_text() == "data"
    assert not source.exists()
    assert not (target.parent / ".a.csv.partial").exists()


def test_move_propagates_other_errors(monkeypatch, tmp_path):
    source = tmp_path / "a.csv"
    source.write_text("data")

    def denied(*_args):
        raise PermissionError(errno.EACCES, "denied")

    monkeypatch.setattr(download_module.os, "replace", denied)

    with pytest.raises(PermissionError):
        _move_into_place(source, tmp_path / "b.csv")
    assert source.exists()
