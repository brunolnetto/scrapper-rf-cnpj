from __future__ import annotations

import os
from pathlib import Path
from typing import List

from .base import StorageBackend


class BlobStorageBackend(StorageBackend):
    """Cloud blob backend that works with Azure Blob Storage or S3 when available."""

    def __init__(self, provider: str = "azure", container: str = "cnpj", connection_string: str | None = None,
                 account_name: str | None = None, account_key: str | None = None, region: str | None = None):
        self.provider = provider.lower()
        self.container = container
        self.connection_string = connection_string
        self.account_name = account_name
        self.account_key = account_key
        self.region = region
        self._client = None

    def _build_client(self):
        if self.provider == "azure":
            try:
                from azure.storage.blob import BlobServiceClient
            except ImportError as exc:  # pragma: no cover
                raise RuntimeError("azure-storage-blob is required for Azure Blob Storage support") from exc

            if self.connection_string:
                return BlobServiceClient.from_connection_string(self.connection_string)
            if self.account_name and self.account_key:
                return BlobServiceClient(account_url=f"https://{self.account_name}.blob.core.windows.net",
                                         credential=self.account_key)
            raise RuntimeError("Azure Blob Storage requires connection_string or account credentials")

        if self.provider in {"s3", "aws"}:
            try:
                import boto3
            except ImportError as exc:  # pragma: no cover
                raise RuntimeError("boto3 is required for S3 support") from exc

            session = boto3.session.Session(region_name=self.region)
            return session.client("s3")

        raise ValueError(f"Unsupported storage provider: {self.provider}")

    @property
    def client(self):
        if self._client is None:
            self._client = self._build_client()
        return self._client

    def upload_file(self, source_path, destination_key: str) -> str:
        source = Path(source_path)
        with source.open("rb") as fp:
            if self.provider == "azure":
                blob_client = self.client.get_blob_client(container=self.container, blob=destination_key)
                blob_client.upload_blob(fp, overwrite=True)
                return destination_key
            if self.provider in {"s3", "aws"}:
                self.client.upload_fileobj(fp, self.container, destination_key)
                return destination_key
        raise ValueError(f"Unsupported storage provider: {self.provider}")

    def upload_bytes(self, data: bytes, destination_key: str) -> str:
        if self.provider == "azure":
            blob_client = self.client.get_blob_client(container=self.container, blob=destination_key)
            blob_client.upload_blob(data, overwrite=True)
            return destination_key
        if self.provider in {"s3", "aws"}:
            self.client.put_object(Bucket=self.container, Key=destination_key, Body=data)
            return destination_key
        raise ValueError(f"Unsupported storage provider: {self.provider}")

    def download_file(self, source_key: str, destination_path) -> str:
        target = Path(destination_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        if self.provider == "azure":
            blob_client = self.client.get_blob_client(container=self.container, blob=source_key)
            with target.open("wb") as fp:
                fp.write(blob_client.download_blob().readall())
            return str(target)
        if self.provider in {"s3", "aws"}:
            self.client.download_fileobj(self.container, source_key, target.open("wb"))
            return str(target)
        raise ValueError(f"Unsupported storage provider: {self.provider}")

    def exists(self, key: str) -> bool:
        if self.provider == "azure":
            blob_client = self.client.get_blob_client(container=self.container, blob=key)
            return blob_client.exists()
        if self.provider in {"s3", "aws"}:
            try:
                self.client.head_object(Bucket=self.container, Key=key)
                return True
            except Exception:
                return False
        return False

    def list_keys(self, prefix: str = "") -> List[str]:
        if self.provider == "azure":
            container_client = self.client.get_container_client(self.container)
            return [blob.name for blob in container_client.list_blobs(name_starts_with=prefix)]
        if self.provider in {"s3", "aws"}:
            paginator = self.client.get_paginator("list_objects_v2")
            keys: List[str] = []
            for page in paginator.paginate(Bucket=self.container, Prefix=prefix):
                for item in page.get("Contents", []):
                    keys.append(item["Key"])
            return keys
        return []

    def make_dir(self, key: str) -> None:
        # Blob storage does not implement true directories; this is a no-op for compatibility.
        return None
