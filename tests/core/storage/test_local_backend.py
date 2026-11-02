from pathlib import Path

import pytest

from src.core.storage import LocalStorageBackend, StorageBackend


def test_local_backend_implements_storage_contract(tmp_path: Path) -> None:
    backend = LocalStorageBackend(tmp_path / "objects")
    assert isinstance(backend, StorageBackend)


def test_upload_download_and_exists_use_bounded_file_operations(tmp_path: Path) -> None:
    root = tmp_path / "objects"
    source = tmp_path / "source.bin"
    payload = b"cnpj-data-" * 250_000
    source.write_bytes(payload)

    backend = LocalStorageBackend(root)
    key = backend.upload_file(source, "raw/zip/2026/11/source.zip")

    assert key == "raw/zip/2026/11/source.zip"
    assert backend.exists(key)
    assert (root / key).stat().st_size == len(payload)

    destination = tmp_path / "workspace" / "source.zip"
    assert backend.download_file(key, destination) == str(destination)
    assert destination.read_bytes() == payload


def test_list_keys_matches_prefix_not_substring(tmp_path: Path) -> None:
    backend = LocalStorageBackend(tmp_path / "objects")
    for key in ("raw/zip/a.zip", "raw/extracted/a.csv", "processed/raw-file.parquet"):
        source = tmp_path / key.replace("/", "_")
        source.write_text(key)
        backend.upload_file(source, key)

    assert backend.list_keys("raw/") == [
        "raw/extracted/a.csv",
        "raw/zip/a.zip",
    ]


def test_delete_is_idempotent(tmp_path: Path) -> None:
    backend = LocalStorageBackend(tmp_path / "objects")
    source = tmp_path / "source.txt"
    source.write_text("data")
    backend.upload_file(source, "raw/source.txt")

    backend.delete("raw/source.txt")
    backend.delete("raw/source.txt")

    assert not backend.exists("raw/source.txt")


@pytest.mark.parametrize(
    "key",
    ["", "../outside.txt", "raw/../../outside.txt", "/absolute/path", r"raw\..\outside.txt"],
)
def test_rejects_unsafe_keys(tmp_path: Path, key: str) -> None:
    backend = LocalStorageBackend(tmp_path / "objects")

    with pytest.raises(ValueError):
        backend.exists(key)


def test_rejects_symlink_escape(tmp_path: Path) -> None:
    root = tmp_path / "objects"
    outside = tmp_path / "outside"
    outside.mkdir()
    root.mkdir()
    (root / "escape").symlink_to(outside, target_is_directory=True)

    backend = LocalStorageBackend(root)
    with pytest.raises(ValueError):
        backend.exists("escape/secret.txt")
