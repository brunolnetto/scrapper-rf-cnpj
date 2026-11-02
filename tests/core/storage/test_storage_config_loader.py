from src.setup.config.loader import ConfigLoader


def test_storage_configuration_is_loaded_from_environment(monkeypatch, tmp_path):
    monkeypatch.setenv("STORAGE_PROVIDER", "local")
    monkeypatch.setenv("STORAGE_ROOT_PATH", str(tmp_path / "objects"))

    config = ConfigLoader(env_file=str(tmp_path / "missing.env"), validate=False)._load_pipeline_config()

    assert config.storage.provider == "local"
    assert config.storage.root_path == str(tmp_path / "objects")


def test_azure_storage_configuration_is_loaded_from_environment(monkeypatch, tmp_path):
    monkeypatch.setenv("STORAGE_PROVIDER", "azure")
    monkeypatch.setenv("AZURE_STORAGE_ACCOUNT_URL", "https://account.blob.core.windows.net")
    monkeypatch.setenv("AZURE_STORAGE_CONTAINER", "cnpj-artifacts")
    monkeypatch.setenv("AZURE_STORAGE_MAX_CONCURRENCY", "3")

    config = ConfigLoader(env_file=str(tmp_path / "missing.env"), validate=False)._load_pipeline_config()

    assert config.storage.provider == "azure"
    assert config.storage.azure_account_url == "https://account.blob.core.windows.net"
    assert config.storage.azure_container_name == "cnpj-artifacts"
    assert config.storage.azure_max_concurrency == 3


def test_s3_storage_configuration_is_loaded_from_environment(monkeypatch, tmp_path):
    monkeypatch.setenv("STORAGE_PROVIDER", "s3")
    monkeypatch.setenv("AWS_S3_BUCKET", "cnpj-artifacts")
    monkeypatch.setenv("AWS_REGION", "sa-east-1")
    monkeypatch.setenv("AWS_S3_ENDPOINT_URL", "http://localhost:4566")
    monkeypatch.setenv("AWS_S3_MAX_CONCURRENCY", "2")
    monkeypatch.setenv("AWS_S3_MULTIPART_THRESHOLD_MB", "32")

    config = ConfigLoader(env_file=str(tmp_path / "missing.env"), validate=False)._load_pipeline_config()

    assert config.storage.provider == "s3"
    assert config.storage.s3_bucket_name == "cnpj-artifacts"
    assert config.storage.s3_region_name == "sa-east-1"
    assert config.storage.s3_endpoint_url == "http://localhost:4566"
    assert config.storage.s3_max_concurrency == 2
    assert config.storage.s3_multipart_threshold_mb == 32
