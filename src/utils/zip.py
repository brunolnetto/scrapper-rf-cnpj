from __future__ import annotations

import shutil
import stat
from pathlib import Path, PurePosixPath
from zipfile import ZipFile


def _safe_member_path(root: Path, filename: str) -> Path:
    """Resolve a ZIP member while rejecting traversal and platform paths."""
    normalized = filename.replace("\\", "/")
    if not normalized or normalized.startswith("/"):
        raise ValueError(f"Unsafe ZIP member path: {filename!r}")

    raw_parts = normalized.rstrip("/").split("/")
    if not raw_parts or any(part in {"", ".", ".."} for part in raw_parts):
        raise ValueError(f"Unsafe ZIP member path: {filename!r}")
    if ":" in raw_parts[0]:
        raise ValueError(f"Drive-qualified ZIP member path is not allowed: {filename!r}")

    relative = PurePosixPath(*raw_parts)
    if relative.is_absolute():
        raise ValueError(f"Unsafe ZIP member path: {filename!r}")

    target = (root / Path(*relative.parts)).resolve()
    if not target.is_relative_to(root):
        raise ValueError(f"ZIP member escapes extraction directory: {filename!r}")
    return target


def extract_zip_file(file_path, extracted_files_path, max_files=None) -> list[Path]:
    """Extract regular ZIP members safely and with bounded memory.

    The destination is expected to be an isolated workspace. All archive member
    paths are validated before extraction, including entries later excluded by
    max_files. Unix symlink entries are rejected.
    """
    root = Path(extracted_files_path).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)

    with ZipFile(file_path, "r") as zip_ref:
        infos = zip_ref.infolist()
        validated: list[tuple[object, Path]] = []

        for info in infos:
            target = _safe_member_path(root, info.filename)
            mode = (info.external_attr >> 16) & 0xFFFF
            if stat.S_ISLNK(mode):
                raise ValueError(f"ZIP symlink entries are not allowed: {info.filename!r}")
            validated.append((info, target))

        files = [(info, target) for info, target in validated if not info.is_dir()]
        files.sort(key=lambda item: item[0].filename)
        if max_files is not None:
            files = files[:max_files]

        extracted: list[Path] = []
        for info, target in files:
            target.parent.mkdir(parents=True, exist_ok=True)
            with zip_ref.open(info, "r") as source, target.open("wb") as destination:
                shutil.copyfileobj(source, destination, length=1024 * 1024)
            extracted.append(target)

    return extracted


def list_zip_contents(zip_file_path):
    """Return metadata for all entries in a ZIP archive."""
    with ZipFile(zip_file_path, "r") as zip_ref:
        return zip_ref.infolist()
