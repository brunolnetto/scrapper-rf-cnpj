from __future__ import annotations

from .base import StorageBackend
from .local_backend import LocalStorageBackend
from ...setup.config.models import StorageConfig


def create_storage_backend(config: StorageConfig | None = None) -> StorageBackend:
    """Construct the configured storage backend.

    Only the local implementation is enabled in this change. Unsupported cloud
    providers are deliberately rejected rather than silently falling back.
    """
    storage_config = config or StorageConfig()

    if storage_config.provider == "local":
        return LocalStorageBackend(storage_config.root_path)

    raise ValueError(
        f"Unsupported storage provider: {storage_config.provider!r}"
    )
