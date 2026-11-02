import pytest
from pydantic import ValidationError

from src.core.storage import LocalStorageBackend, create_storage_backend
from src.setup.config.models import StorageConfig


def test_factory_creates_local_backend_by_default(tmp_path):
    backend = create_storage_backend(StorageConfig(root_path=str(tmp_path / "objects")))

    assert isinstance(backend, LocalStorageBackend)


@pytest.mark.parametrize("provider", ["azure", "s3"])
def test_storage_config_requires_provider_target(provider):
    with pytest.raises(ValidationError):
        StorageConfig(provider=provider)


def test_storage_config_rejects_unknown_provider():
    with pytest.raises(ValidationError):
        StorageConfig(provider="gcs")
