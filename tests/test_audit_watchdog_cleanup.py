"""Completing a batch/subbatch stops its watchdog timer (issue #57).

Completed accumulators were dropped without cancelling their watchdog, whose
non-daemon Timer rescheduled itself forever and kept the process alive
(the reason the CLI used to force os._exit).
"""

import threading
from types import SimpleNamespace
from unittest.mock import MagicMock

from src.core.services.audit.service import AuditService, BatchTimeout
from src.database.models.audit import AuditStatus


def _service():
    config = SimpleNamespace(pipeline=SimpleNamespace(loading=SimpleNamespace(enable_internal_parallelism=False)))
    timeout = BatchTimeout(enable_watchdog=True, watchdog_interval_seconds=60)
    return AuditService(SimpleNamespace(engine=MagicMock()), config, timeout_config=timeout)


def _live_timers():
    return [t for t in threading.enumerate() if isinstance(t, threading.Timer) and t.is_alive()]


def test_completing_batch_and_subbatch_stops_watchdogs():
    before = set(_live_timers())
    service = _service()

    batch_id = service._start_batch("empresa", "b1")
    subbatch_id = service._start_subbatch(batch_id, "empresa", "s1")
    assert len(set(_live_timers()) - before) >= 1

    service._complete_subbatch_with_accumulated_metrics(subbatch_id, AuditStatus.COMPLETED)
    service._complete_batch_with_accumulated_metrics(batch_id, AuditStatus.COMPLETED)

    leaked = [t for t in set(_live_timers()) - before if t.is_alive()]
    for t in leaked:
        t.join(timeout=1)
    assert not [t for t in leaked if t.is_alive()]


def test_watchdog_timers_are_daemon():
    service = _service()
    batch_id = service._start_batch("empresa", "b1")
    try:
        assert all(t.daemon for t in _live_timers())
    finally:
        service._complete_batch_with_accumulated_metrics(batch_id, AuditStatus.COMPLETED)
