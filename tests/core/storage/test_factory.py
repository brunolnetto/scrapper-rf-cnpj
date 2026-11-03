import pytest
from pydantic import ValidationError

from src.core.storage import LocalStorageBackend, create_storage_backend
from src.setup.config.models import StorageConfig


def test_factory_creates_local_backend_by_default(tmp_path):
    backend = create_storage_backend(StorageConfig(root_path=str(tmp_path / "objects")))

    assert isinstance(backend, LocalStorageBackend)


def test_factory_selects_azure_backend(monkeypatch):
    from src.core.storage import azure_blob

    sentinel = object()
    monkeypatch.setattr(
        azure_blob,
        "AzureBlobStorageBackend",
        lambda **_kwargs: sentinel,
    )
    config = StorageConfig(
        provider="azure",
        azure_account_url="https://account.blob.core.windows.net",
        azure_container_name="cnpj-artifacts",
        azure_max_concurrency=3,
    )

    assert create_storage_backend(config) is sentinel


def test_factory_selects_s3_backend(monkeypatch):
    from src.core.storage import s3

    sentinel = object()
    monkeypatch.setattr(s3, "S3StorageBackend", lambda **_kwargs: sentinel)
    config = StorageConfig(
        provider="s3",
        s3_bucket_name="cnpj-artifacts",
        s3_region_name="sa-east-1",
    )

    assert create_storage_backend(config) is sentinel


@pytest.mark.parametrize("provider", ["azure", "s3"])
def test_storage_config_requires_provider_target(provider):
    with pytest.raises(ValidationError):
        StorageConfig(provider=provider)


def test_storage_config_rejects_unknown_provider():
    with pytest.raises(ValidationError):
        StorageConfig(provider="gcs")
