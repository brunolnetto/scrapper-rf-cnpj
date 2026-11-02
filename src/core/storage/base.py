from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import List, Union

PathLike = Union[str, Path]


class StorageBackend(ABC):
    """Abstract storage backend for ETL files."""

    @abstractmethod
    def upload_file(self, source_path: PathLike, destination_key: str) -> str:
        """Upload a local file to the backend and return its logical key/path."""

    @abstractmethod
    def upload_bytes(self, data: bytes, destination_key: str) -> str:
        """Upload bytes to the backend and return its logical key/path."""

    @abstractmethod
    def download_file(self, source_key: str, destination_path: PathLike) -> str:
        """Download a remote object to a local file and return the local path."""

    @abstractmethod
    def exists(self, key: str) -> bool:
        """Return whether the object exists in the backend."""

    @abstractmethod
    def list_keys(self, prefix: str = "") -> List[str]:
        """List keys under a prefix."""

    @abstractmethod
    def make_dir(self, key: str) -> None:
        """Ensure a logical directory node exists."""
