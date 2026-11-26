"""Exit-code behaviour of the orchestrator and CLI entrypoint (issue #27)."""

import pytest

import src.main as cli
from src.core import orchestrator as orchestrator_module
from src.core.orchestrator import PipelineOrchestrator


class _Pipeline:
    def __init__(self, valid=True):
        self._valid = valid

    def get_name(self):
        return "FakePipeline"

    def validate_config(self):
        return self._valid


class _Strategy:
    def __init__(self, error=None, result="ok"):
        self._error = error
        self._result = result

    def get_name(self):
        return "FakeStrategy"

    def validate_parameters(self, **_kwargs):
        return True

    def execute(self, _pipeline, _config, **_kwargs):
        if self._error is not None:
            raise self._error
        return self._result


class _Router:
    def __init__(self):
        self.events = []

    def route(self, event):
        self.events.append(event)


@pytest.fixture
def router(monkeypatch):
    router = _Router()
    monkeypatch.setattr(orchestrator_module, "make_notification_router", lambda _cfg: router)
    return router


def _orchestrator(pipeline, strategy):
    return PipelineOrchestrator(pipeline, strategy, config_service=object())


def test_orchestrator_returns_result_on_success(router):
    orch = _orchestrator(_Pipeline(), _Strategy(result="done"))
    assert orch.run() == "done"
    assert router.events == []


def test_orchestrator_propagates_strategy_failure(router):
    orch = _orchestrator(_Pipeline(), _Strategy(error=RuntimeError("boom")))
    with pytest.raises(RuntimeError, match="boom"):
        orch.run()
    assert [e.event_type for e in router.events] == ["pipeline_crash"]


@pytest.mark.usefixtures("router")
def test_orchestrator_raises_on_invalid_config():
    orch = _orchestrator(_Pipeline(valid=False), _Strategy())
    with pytest.raises(ValueError, match="Invalid configuration"):
        orch.run()


class _Exited(Exception):
    def __init__(self, code):
        super().__init__(code)
        self.code = code


@pytest.fixture
def captured_exit(monkeypatch):
    def fake_exit(code):
        raise _Exited(code)

    monkeypatch.setattr(cli.os, "_exit", fake_exit)
    monkeypatch.setattr(cli.logging, "shutdown", lambda: None)


@pytest.mark.parametrize(
    ("behaviour", "expected"),
    [
        (lambda: 0, 0),
        (lambda: (_ for _ in ()).throw(RuntimeError("boom")), 1),
        (lambda: (_ for _ in ()).throw(KeyboardInterrupt()), 130),
        (lambda: (_ for _ in ()).throw(SystemExit(2)), 2),
        (lambda: (_ for _ in ()).throw(SystemExit(None)), 0),
    ],
    ids=["success", "exception", "interrupt", "argparse-error", "help"],
)
@pytest.mark.usefixtures("captured_exit")
def test_run_cli_maps_outcome_to_exit_code(monkeypatch, behaviour, expected):
    monkeypatch.setattr(cli, "main", lambda _argv=None: behaviour())
    with pytest.raises(_Exited) as exc_info:
        cli.run_cli([])
    assert exc_info.value.code == expected
