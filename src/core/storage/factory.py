from __future__ import annotations

from .base import StorageBackend
from .local_backend import LocalStorageBackend
from ...setup.config.models import StorageConfig


def create_storage_backend(config: StorageConfig | None = None) -> StorageBackend:
    """Construct the configured backend without importing unused cloud SDKs."""
    storage_config = config or StorageConfig()

    if storage_config.provider == "local":
        return LocalStorageBackend(storage_config.root_path)

    if storage_config.provider == "azure":
        from .azure_blob import AzureBlobStorageBackend

        return AzureBlobStorageBackend(
            account_url=storage_config.azure_account_url or "",
            container_name=storage_config.azure_container_name or "",
            max_concurrency=storage_config.azure_max_concurrency,
        )

    if storage_config.provider == "s3":
        from .s3 import S3StorageBackend

        return S3StorageBackend(
            bucket_name=storage_config.s3_bucket_name or "",
            region_name=storage_config.s3_region_name,
            endpoint_url=storage_config.s3_endpoint_url,
            max_concurrency=storage_config.s3_max_concurrency,
            multipart_threshold_mb=storage_config.s3_multipart_threshold_mb,
        )

    raise ValueError(f"Unsupported storage provider: {storage_config.provider!r}")
