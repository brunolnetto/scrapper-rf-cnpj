from __future__ import annotations

import os
import re
import tempfile
from pathlib import Path
from typing import Iterable, Protocol, runtime_checkable

PathLike = str | Path

_WINDOWS_DRIVE_PREFIX = re.compile(r"^[A-Za-z]:")


def validate_object_key(key: str) -> str:
    """Validate a backend-neutral, relative POSIX object key."""
    if not isinstance(key, str) or not key or key.strip() != key:
        raise ValueError("Storage key must be a non-empty, trimmed string")
    if key.startswith("/") or "\\" in key or "\x00" in key:
        raise ValueError(f"Invalid storage key: {key!r}")
    if _WINDOWS_DRIVE_PREFIX.match(key):
        raise ValueError(f"Drive-qualified storage key is not allowed: {key!r}")

    parts = key.split("/")
    if any(part in {"", ".", ".."} for part in parts):
        raise ValueError(f"Invalid storage key: {key!r}")
    return key


def validate_object_prefix(prefix: str) -> str:
    """Validate a POSIX prefix; an empty prefix means all objects."""
    if not isinstance(prefix, str) or prefix.strip() != prefix:
        raise ValueError("Storage prefix must be a trimmed string")
    if not prefix:
        return prefix
    if prefix.startswith("/") or "\\" in prefix or "\x00" in prefix:
        raise ValueError(f"Invalid storage prefix: {prefix!r}")

    normalized = prefix[:-1] if prefix.endswith("/") else prefix
    parts = normalized.split("/")
    if not parts or any(part in {"", ".", ".."} for part in parts):
        raise ValueError(f"Invalid storage prefix: {prefix!r}")
    if _WINDOWS_DRIVE_PREFIX.match(prefix):
        raise ValueError(f"Drive-qualified storage prefix is not allowed: {prefix!r}")
    return prefix


def atomic_write_chunks(destination_path: PathLike, chunks: Iterable[bytes]) -> str:
    """Write streamed bytes to a local path and atomically replace the target."""
    target = Path(destination_path).expanduser()
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb", dir=target.parent, prefix=f".{target.name}.", delete=False
        ) as temporary:
            temporary_path = Path(temporary.name)
            for chunk in chunks:
                if chunk:
                    temporary.write(chunk)
            temporary.flush()
            os.fsync(temporary.fileno())
        os.replace(temporary_path, target)
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)
    return str(target)


@runtime_checkable
class StorageBackend(Protocol):
    """Contract for durable file storage used by ETL stages.

    Object keys are backend-relative identifiers, never local filesystem paths.
    Implementations must stream file contents rather than loading whole files
    into memory.
    """

    def upload_file(self, source_path: PathLike, destination_key: str) -> str:
        """Persist a local file and return its backend-relative key."""
        ...

    def download_file(self, source_key: str, destination_path: PathLike) -> str:
        """Materialize a stored object at a local path and return that path."""
        ...

    def exists(self, key: str) -> bool:
        """Return whether an object exists."""
        ...

    def list_keys(self, prefix: str = "") -> list[str]:
        """List object keys beginning with a prefix."""
        ...

    def delete(self, key: str) -> None:
        """Delete an object; deleting a missing object is a no-op."""
        ...
