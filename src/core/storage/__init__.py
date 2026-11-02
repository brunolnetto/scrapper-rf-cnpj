from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional

from .blob_backend import BlobStorageBackend
from .local_backend import LocalStorageBackend
from .base import StorageBackend


@dataclass
class StorageConfig:
    provider: str = "local"
    root_path: str = "data"
    container: str = "cnpj"
    connection_string: Optional[str] = None
    account_name: Optional[str] = None
    account_key: Optional[str] = None
    region: Optional[str] = None

    @classmethod
    def from_env(cls) -> "StorageConfig":
        return cls(
            provider=os.getenv("STORAGE_PROVIDER", "local"),
            root_path=os.getenv("STORAGE_ROOT_PATH", "data"),
            container=os.getenv("STORAGE_CONTAINER", "cnpj"),
            connection_string=os.getenv("STORAGE_CONNECTION_STRING"),
            account_name=os.getenv("STORAGE_ACCOUNT_NAME"),
            account_key=os.getenv("STORAGE_ACCOUNT_KEY"),
            region=os.getenv("STORAGE_REGION"),
        )


def create_storage_backend(config: Optional[StorageConfig] = None) -> StorageBackend:
    runtime_config = config or StorageConfig.from_env()
    provider = (runtime_config.provider or "local").lower()

    if provider == "local":
        return LocalStorageBackend(runtime_config.root_path)

    if provider in {"azure", "blob"}:
        return BlobStorageBackend(
            provider="azure",
            container=runtime_config.container,
            connection_string=runtime_config.connection_string,
            account_name=runtime_config.account_name,
            account_key=runtime_config.account_key,
            region=runtime_config.region,
        )

    if provider in {"s3", "aws"}:
        return BlobStorageBackend(
            provider="s3",
            container=runtime_config.container,
            connection_string=runtime_config.connection_string,
            account_name=runtime_config.account_name,
            account_key=runtime_config.account_key,
            region=runtime_config.region,
        )

    raise ValueError(f"Unsupported storage provider: {provider}")


__all__ = ["StorageConfig", "create_storage_backend"]
