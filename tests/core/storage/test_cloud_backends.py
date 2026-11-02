from __future__ import annotations

from io import BytesIO
from pathlib import Path

import pytest

from src.core.storage import StorageBackend
from src.core.storage.azure_blob import AzureBlobStorageBackend
from src.core.storage.s3 import S3StorageBackend


class FakeAzureDownloader:
    def __init__(self, payload: bytes):
        self.payload = payload

    def chunks(self):
        for offset in range(0, len(self.payload), 3):
            yield self.payload[offset : offset + 3]


class FakeAzureBlob:
    def __init__(self, container, name: str):
        self.container = container
        self.name = name

    def upload_blob(self, data, overwrite, max_concurrency):
        self.container.objects[self.name] = data.read()

    def download_blob(self, max_concurrency):
        return FakeAzureDownloader(self.container.objects[self.name])

    def exists(self):
        return self.name in self.container.objects

    def delete_blob(self, delete_snapshots):
        if self.name not in self.container.objects:
            raise FakeAzureNotFound()
        del self.container.objects[self.name]


class FakeAzureNotFound(Exception):
    error_code = "BlobNotFound"


class FakeAzureContainer:
    def __init__(self):
        self.objects: dict[str, bytes] = {}

    def get_blob_client(self, blob):
        return FakeAzureBlob(self, blob)

    def list_blobs(self, name_starts_with):
        return [
            type("BlobItem", (), {"name": name})()
            for name in self.objects
            if name.startswith(name_starts_with)
        ]


class FakeS3Error(Exception):
    def __init__(self, code="404", status=404):
        super().__init__(code)
        self.response = {
            "Error": {"Code": code},
            "ResponseMetadata": {"HTTPStatusCode": status},
        }


class FakePaginator:
    def __init__(self, client):
        self.client = client

    def paginate(self, Bucket, Prefix):
        keys = sorted(
            key for key in self.client.objects
            if key.startswith(Prefix)
        )
        yield {"Contents": [{"Key": key} for key in keys]}


class FakeS3Client:
    def __init__(self):
        self.objects: dict[str, bytes] = {}

    def upload_fileobj(self, fileobj, Bucket, Key, Config=None):
        self.objects[Key] = fileobj.read()

    def get_object(self, Bucket, Key):
        if Key not in self.objects:
            raise FakeS3Error()
        return {"Body": BytesIO(self.objects[Key])}

    def head_object(self, Bucket, Key):
        if Key not in self.objects:
            raise FakeS3Error()
        return {}

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        return FakePaginator(self)

    def delete_object(self, Bucket, Key):
        self.objects.pop(Key, None)

    def close(self):
        pass


@pytest.mark.parametrize("provider", ["azure", "s3"])
def test_cloud_adapter_round_trip_list_exists_and_delete(tmp_path: Path, provider: str):
    source = tmp_path / "source.bin"
    payload = b"cnpj-cloud-storage-" * 10_000
    source.write_bytes(payload)
    target = tmp_path / "restored" / "artifact.bin"

    if provider == "azure":
        container = FakeAzureContainer()
        backend = AzureBlobStorageBackend(
            "https://account.blob.core.windows.net",
            "artifacts",
            container_client=container,
        )
    else:
        backend = S3StorageBackend("artifact-bucket", client=FakeS3Client())

    assert isinstance(backend, StorageBackend)
    key = "raw/zip/2026/11/source.zip"
    assert backend.upload_file(source, key) == key
    assert backend.exists(key)
    assert backend.list_keys("raw/zip/2026/11/") == [key]
    assert backend.download_file(key, target) == str(target)
    assert target.read_bytes() == payload

    backend.delete(key)
    backend.delete(key)
    assert not backend.exists(key)


@pytest.mark.parametrize(
    "key",
    ["", "../outside", "/absolute", "raw//empty", r"raw\..\outside", "C:/outside"],
)
def test_cloud_adapters_reject_unsafe_object_keys(key: str):
    azure = AzureBlobStorageBackend(
        "https://account.blob.core.windows.net",
        "artifacts",
        container_client=FakeAzureContainer(),
    )
    s3 = S3StorageBackend("artifact-bucket", client=FakeS3Client())

    with pytest.raises(ValueError):
        azure.exists(key)
    with pytest.raises(ValueError):
        s3.exists(key)


def test_azure_rejects_non_https_account_url():
    with pytest.raises(ValueError):
        AzureBlobStorageBackend(
            "http://account.blob.core.windows.net",
            "artifacts",
            container_client=FakeAzureContainer(),
        )


@pytest.mark.parametrize("prefix", ["/raw/", "../raw/", "raw//", "raw\\\\..\\\\"])
def test_cloud_adapters_reject_unsafe_prefixes(prefix: str):
    azure = AzureBlobStorageBackend(
        "https://account.blob.core.windows.net",
        "artifacts",
        container_client=FakeAzureContainer(),
    )
    s3 = S3StorageBackend("artifact-bucket", client=FakeS3Client())

    with pytest.raises(ValueError):
        azure.list_keys(prefix)
    with pytest.raises(ValueError):
        s3.list_keys(prefix)


def test_azure_rejects_raw_credential_material():
    for credential in ("account-key", {"account_key": "account-key"}):
        with pytest.raises(ValueError):
            AzureBlobStorageBackend(
                "https://account.blob.core.windows.net",
                "artifacts",
                credential=credential,
                container_client=FakeAzureContainer(),
            )


def test_s3_exists_does_not_hide_access_denied():
    client = FakeS3Client()

    def denied(**kwargs):
        raise FakeS3Error(code="AccessDenied", status=403)

    client.head_object = denied
    backend = S3StorageBackend("artifact-bucket", client=client)

    with pytest.raises(FakeS3Error):
        backend.exists("raw/file.bin")
