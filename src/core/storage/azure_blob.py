from __future__ import annotations

from pathlib import Path
from typing import Any

from .base import PathLike, atomic_write_chunks, validate_object_key, validate_object_prefix


class AzureBlobStorageBackend:
    """Azure Blob Storage adapter using Microsoft Entra credential discovery.

    The container must already exist. Authentication is delegated to
    DefaultAzureCredential; no account keys or SAS tokens are accepted here.
    """

    def __init__(
        self,
        account_url: str,
        container_name: str,
        *,
        max_concurrency: int = 4,
        container_client: Any | None = None,
        credential: Any | None = None,
    ) -> None:
        if not account_url or not account_url.startswith("https://"):
            raise ValueError("Azure account_url must be an HTTPS URL")
        if not container_name or "/" in container_name:
            raise ValueError("Azure container_name must be a non-empty container name")
        if max_concurrency < 1:
            raise ValueError("max_concurrency must be >= 1")

        self.max_concurrency = max_concurrency
        self._service_client = None
        self._credential = credential
        self._owns_credential = False

        if container_client is not None:
            self.container_client = container_client
            return

        try:
            from azure.identity import DefaultAzureCredential
            from azure.storage.blob import BlobServiceClient
        except ImportError as exc:
            raise RuntimeError(
                "Azure storage requires the optional dependencies: "
                "uv sync --extra azure"
            ) from exc

        if self._credential is None:
            self._credential = DefaultAzureCredential()
            self._owns_credential = True

        self._service_client = BlobServiceClient(
            account_url=account_url,
            credential=self._credential,
            max_block_size=4 * 1024 * 1024,
            max_single_put_size=8 * 1024 * 1024,
        )
        self.container_client = self._service_client.get_container_client(container_name)

    def upload_file(self, source_path: PathLike, destination_key: str) -> str:
        source = Path(source_path)
        if not source.is_file():
            raise FileNotFoundError(source)
        key = validate_object_key(destination_key)
        blob_client = self.container_client.get_blob_client(blob=key)
        with source.open("rb") as stream:
            blob_client.upload_blob(
                data=stream,
                overwrite=True,
                max_concurrency=self.max_concurrency,
            )
        return key

    def download_file(self, source_key: str, destination_path: PathLike) -> str:
        key = validate_object_key(source_key)
        blob_client = self.container_client.get_blob_client(blob=key)
        downloader = blob_client.download_blob(max_concurrency=self.max_concurrency)
        return atomic_write_chunks(destination_path, downloader.chunks())

    def exists(self, key: str) -> bool:
        blob_key = validate_object_key(key)
        return bool(self.container_client.get_blob_client(blob=blob_key).exists())

    def list_keys(self, prefix: str = "") -> list[str]:
        validated_prefix = validate_object_prefix(prefix)
        blobs = self.container_client.list_blobs(name_starts_with=validated_prefix)
        return sorted(blob.name for blob in blobs)

    def delete(self, key: str) -> None:
        blob_key = validate_object_key(key)
        blob_client = self.container_client.get_blob_client(blob=blob_key)
        try:
            blob_client.delete_blob(delete_snapshots="include")
        except Exception as exc:
            # Only a missing blob is idempotent; auth/container/network failures
            # must remain visible to the caller.
            error_code = getattr(exc, "error_code", None)
            if error_code == "BlobNotFound":
                return
            raise

    def close(self) -> None:
        """Close the SDK client and the credential created by this adapter."""
        if self._service_client is not None:
            self._service_client.close()
        if self._owns_credential and self._credential is not None:
            self._credential.close()
