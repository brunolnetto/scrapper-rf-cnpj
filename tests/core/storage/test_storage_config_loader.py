from src.setup.config.loader import ConfigLoader


def test_storage_configuration_is_loaded_from_environment(monkeypatch, tmp_path):
    monkeypatch.setenv("STORAGE_PROVIDER", "local")
    monkeypatch.setenv("STORAGE_ROOT_PATH", str(tmp_path / "objects"))

    config = ConfigLoader(env_file=str(tmp_path / "missing.env"), validate=False)._load_pipeline_config()

    assert config.storage.provider == "local"
    assert config.storage.root_path == str(tmp_path / "objects")
