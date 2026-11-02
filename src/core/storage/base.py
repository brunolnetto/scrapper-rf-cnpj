from __future__ import annotations

from pathlib import Path
from typing import Protocol, runtime_checkable

PathLike = str | Path


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
