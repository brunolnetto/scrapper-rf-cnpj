"""Memory cleanup stays best-effort but reports failures (issue #61)."""

from types import SimpleNamespace

from src.core.services.memory import service as memory_service
from src.core.services.memory.service import MemoryMonitor


def _monitor(on_cleanup):
    config = SimpleNamespace(baseline_samples=1, warmup_seconds=0.0, cleanup_rate_limit_seconds=0.0)
    return MemoryMonitor(config, on_cleanup=on_cleanup)


def test_failing_cleanup_hook_is_logged_and_cleanup_continues(monkeypatch):
    warnings = []
    monkeypatch.setattr(memory_service.logger, "warning", warnings.append)

    def hook():
        raise RuntimeError("cache already closed")

    result = _monitor(hook).perform_aggressive_cleanup()

    assert not result.get("skipped")
    assert any("cache already closed" in w for w in warnings)
