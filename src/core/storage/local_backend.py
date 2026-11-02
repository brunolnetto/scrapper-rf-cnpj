from __future__ import annotations

from pathlib import Path
from typing import List

from .base import StorageBackend


class LocalStorageBackend(StorageBackend):
    """Local filesystem backend used by default for compatibility."""

    def __init__(self, root: str | Path):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def _resolve(self, key: str) -> Path:
        return self.root / key.lstrip("/")

    def upload_file(self, source_path, destination_key):
        target = self._resolve(destination_key)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(Path(source_path).read_bytes())
        return str(target)

    def upload_bytes(self, data: bytes, destination_key: str):
        target = self._resolve(destination_key)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
        return str(target)

    def download_file(self, source_key: str, destination_path):
        source = self._resolve(source_key)
        target = Path(destination_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(source.read_bytes())
        return str(target)

    def exists(self, key: str) -> bool:
        return self._resolve(key).exists()

    def list_keys(self, prefix: str = "") -> List[str]:
        if not self.root.exists():
            return []
        root_rel = self.root
        items: List[str] = []
        for path in self.root.rglob("*"):
            rel = path.relative_to(root_rel).as_posix()
            if prefix and prefix not in rel:
                continue
            items.append(rel)
        return items

    def make_dir(self, key: str) -> None:
        self._resolve(key).mkdir(parents=True, exist_ok=True)
