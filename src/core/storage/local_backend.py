from __future__ import annotations

import os
import shutil
import tempfile
from pathlib import Path, PurePosixPath

from .base import PathLike, validate_object_key, validate_object_prefix


class LocalStorageBackend:
    """Filesystem implementation of the storage contract.

    Keys are relative to root and use POSIX separators on every platform.
    File transfers use bounded-memory copies and atomic replacement.
    """

    def __init__(self, root: PathLike):
        self.root = Path(root).expanduser().resolve()
        self.root.mkdir(parents=True, exist_ok=True)

    def _resolve(self, key: str) -> Path:
        normalized = validate_object_key(key)
        parsed = PurePosixPath(normalized)

        target = self.root.joinpath(*parsed.parts).resolve()
        if not target.is_relative_to(self.root):
            raise ValueError(f"Storage key escapes configured root: {key!r}")
        return target

    @staticmethod
    def _copy_atomic(source: Path, target: Path) -> None:
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary_path: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="wb", dir=target.parent, prefix=f".{target.name}.", delete=False
            ) as temporary:
                temporary_path = Path(temporary.name)
                with source.open("rb") as source_file:
                    shutil.copyfileobj(source_file, temporary, length=1024 * 1024)
                temporary.flush()
                os.fsync(temporary.fileno())
            os.replace(temporary_path, target)
        finally:
            if temporary_path is not None:
                temporary_path.unlink(missing_ok=True)

    def upload_file(self, source_path: PathLike, destination_key: str) -> str:
        source = Path(source_path)
        if not source.is_file():
            raise FileNotFoundError(source)
        target = self._resolve(destination_key)
        self._copy_atomic(source, target)
        return destination_key

    def download_file(self, source_key: str, destination_path: PathLike) -> str:
        source = self._resolve(source_key)
        if not source.is_file():
            raise FileNotFoundError(source)
        target = Path(destination_path).expanduser()
        self._copy_atomic(source, target)
        return str(target)

    def exists(self, key: str) -> bool:
        return self._resolve(key).is_file()

    def list_keys(self, prefix: str = "") -> list[str]:
        normalized_prefix = validate_object_prefix(prefix)
        return sorted(
            path.relative_to(self.root).as_posix()
            for path in self.root.rglob("*")
            if path.is_file()
            and path.relative_to(self.root).as_posix().startswith(normalized_prefix)
        )

    def delete(self, key: str) -> None:
        self._resolve(key).unlink(missing_ok=True)
