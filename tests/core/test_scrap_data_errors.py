"""scrap_data must not turn discovery failures into "no data" (issue #35)."""

from datetime import datetime
from types import SimpleNamespace

import pytest
import requests

from src.core.pipeline import ReceitaCNPJPipeline
from src.core.services.discovery import service as discovery_module

_current_scrape = None


class _FakeDiscovery:
    """Replaces the real service so no test ever reaches the RFB server."""

    def __init__(self, _config):
        pass

    def scrape_period_files(self, **kwargs):
        return _current_scrape(**kwargs)


@pytest.fixture(autouse=True)
def fake_discovery(monkeypatch):
    monkeypatch.setattr(discovery_module, "FederalRevenueDiscoveryService", _FakeDiscovery)


def _pipeline(scrape) -> ReceitaCNPJPipeline:
    global _current_scrape
    _current_scrape = scrape
    pipeline = ReceitaCNPJPipeline.__new__(ReceitaCNPJPipeline)
    pipeline.config = SimpleNamespace(pipeline=SimpleNamespace(year=2026, month=11))
    pipeline._discovery_service = None
    return pipeline


def _raise(error):
    def scrape(**_kwargs):
        raise error

    return scrape


def test_network_error_propagates():
    pipeline = _pipeline(_raise(requests.exceptions.ConnectionError("RFB unreachable")))
    with pytest.raises(requests.exceptions.ConnectionError):
        pipeline.scrap_data()


def test_unavailable_period_propagates():
    pipeline = _pipeline(_raise(ValueError("Period 2026-11 is not available")))
    with pytest.raises(ValueError, match="not available"):
        pipeline.scrap_data()


def test_empty_period_returns_empty_list():
    assert _pipeline(lambda **_k: []).scrap_data() == []


def test_files_are_mapped_to_tables():
    period_file = SimpleNamespace(
        filename="Empresas0.zip",
        download_url="https://example.test/Empresas0.zip",
        updated_at=datetime(2026, 11, 1),
        file_size=123,
    )
    files = _pipeline(lambda **_k: [period_file]).scrap_data()

    assert len(files) == 1
    assert files[0].filename == "Empresas0.zip"
    assert files[0].tablename == "empresa"
