import pytest
from pydantic import ValidationError

from src.core.storage import LocalStorageBackend, create_storage_backend
from src.setup.config.models import StorageConfig


def test_factory_creates_local_backend_by_default(tmp_path):
    backend = create_storage_backend(StorageConfig(root_path=str(tmp_path / "objects")))

    assert isinstance(backend, LocalStorageBackend)


def test_storage_config_rejects_unimplemented_provider():
    with pytest.raises(ValidationError):
        StorageConfig(provider="azure")  # type: ignore[arg-type]
