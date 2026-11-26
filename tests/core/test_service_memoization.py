"""Pipeline services are created once per run (issue #33)."""

from types import SimpleNamespace

from src.core import pipeline as pipeline_module
from src.core.pipeline import ReceitaCNPJPipeline
from src.core.services.discovery.service import DataPeriod, FederalRevenueDiscoveryService
from src.core.services.loading import service as loading_module


def _bare_pipeline() -> ReceitaCNPJPipeline:
    pipeline = ReceitaCNPJPipeline.__new__(ReceitaCNPJPipeline)
    pipeline.config = SimpleNamespace()
    pipeline._database = object()
    pipeline._audit_service = object()
    pipeline._initialized = True
    pipeline._data_loader = None
    pipeline._discovery_service = None
    return pipeline


def test_data_loader_is_created_once(monkeypatch):
    created = []

    class FakeLoader:
        def __init__(self, *args):
            created.append(args)

    monkeypatch.setattr(loading_module, "FileLoadingService", FakeLoader)
    pipeline = _bare_pipeline()

    assert pipeline.data_loader is pipeline.data_loader
    assert len(created) == 1


def test_discovery_service_is_created_once():
    pipeline = _bare_pipeline()
    assert pipeline.discovery_service is pipeline.discovery_service


def test_init_starts_without_cached_services(monkeypatch):
    monkeypatch.setattr(pipeline_module, "create_storage_backend", lambda _cfg: None)
    monkeypatch.setattr(pipeline_module, "DevelopmentFilter", lambda _cfg: None)
    config = SimpleNamespace(
        pipeline=SimpleNamespace(storage=None, development=None),
        is_development_mode=lambda: False,
    )

    pipeline = ReceitaCNPJPipeline(config)

    assert pipeline._data_loader is None
    assert pipeline._discovery_service is None


def _period(year, month):
    return DataPeriod(
        year=year, month=month, directory_name=f"{year}-{month:02d}", last_modified="", url="u"
    )


def test_period_listing_is_fetched_once_per_instance(monkeypatch):
    calls = []

    def fake_parse(_self, _content):
        calls.append(1)
        return [_period(2026, 11), _period(2026, 10)]

    monkeypatch.setattr(FederalRevenueDiscoveryService, "parse_directory_listing", fake_parse)
    service = FederalRevenueDiscoveryService(config=None)

    assert service.find_period(2026, 10).month == 10
    assert service.get_latest_period().month == 11
    assert len(service.discover_available_periods()) == 2
    assert len(calls) == 1

    service.discover_available_periods(refresh=True)
    assert len(calls) == 2


def test_cached_listing_is_not_mutated_by_callers(monkeypatch):
    monkeypatch.setattr(
        FederalRevenueDiscoveryService, "parse_directory_listing", lambda _self, _c: [_period(2026, 11)]
    )
    service = FederalRevenueDiscoveryService(config=None)

    service.discover_available_periods().clear()

    assert len(service.discover_available_periods()) == 1

