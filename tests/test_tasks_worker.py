from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import FastAPI

import wybra.tasks.worker as worker
from wybra.cache import CacheFeatureError
from wybra.core.composition import APP_CONFIG_ENV, APP_ROOT_ENV
from wybra.tools.project import ProjectToolConfigurationError


class _Receiver:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.shutdown_requested: asyncio.Event | None = None
        self.stopped = False

    async def listen(self, finish_event: asyncio.Event) -> None:
        self.shutdown_requested = finish_event
        self.started.set()
        await finish_event.wait()
        self.stopped = True


class _Capability:
    def __init__(self, receiver: _Receiver) -> None:
        self._receiver = receiver

    def receiver(self) -> _Receiver:
        return self._receiver


@pytest.mark.anyio
async def test_worker_runs_taskiq_receiver_inside_application_lifespan(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entered = False
    exited = False
    receiver = _Receiver()

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        nonlocal entered, exited
        entered = True
        try:
            yield
        finally:
            exited = True

    app = FastAPI(lifespan=lifespan)
    site = SimpleNamespace(
        optional_capability=lambda _capability: _Capability(receiver),
    )
    monkeypatch.setattr(worker, "get_site", lambda _app: site)
    shutdown_requested = asyncio.Event()

    running = asyncio.create_task(worker.run_worker(app, shutdown_requested))
    await receiver.started.wait()
    shutdown_requested.set()
    await running

    assert entered is True
    assert exited is True
    assert receiver.shutdown_requested is shutdown_requested
    assert receiver.stopped is True


def test_worker_requires_taskiq_capability() -> None:
    site = SimpleNamespace(optional_capability=lambda _capability: None)

    with pytest.raises(ProjectToolConfigurationError, match="backend = 'taskiq'"):
        worker._receiver_for_site(site)


@pytest.mark.anyio
async def test_worker_command_applies_selected_configuration_to_lifespan(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    selected_config = "selected.toml"
    project_root = tmp_path / "project"
    observed: dict[str, str | None] = {}
    monkeypatch.setenv(APP_CONFIG_ENV, "ambient.toml")
    monkeypatch.delenv(APP_ROOT_ENV, raising=False)
    monkeypatch.setattr(worker, "runtime_project_root", lambda: project_root)
    monkeypatch.setattr(worker, "configure_cli_logging", lambda *_args: None)
    monkeypatch.setattr(
        worker,
        "resolve_configured_asgi_app",
        lambda **_kwargs: SimpleNamespace(
            app_config=object(),
            app_target="example:app",
        ),
    )
    monkeypatch.setattr(worker, "import_from_string", lambda _target: object())

    async def inspect_lifespan(_app: object, _shutdown: asyncio.Event) -> None:
        observed[APP_CONFIG_ENV] = worker.os.environ.get(APP_CONFIG_ENV)
        observed[APP_ROOT_ENV] = worker.os.environ.get(APP_ROOT_ENV)

    monkeypatch.setattr(worker, "run_worker", inspect_lifespan)

    assert await worker._run_worker_command(config_source=selected_config) == 0
    assert observed == {
        APP_CONFIG_ENV: selected_config,
        APP_ROOT_ENV: project_root.resolve().as_posix(),
    }
    assert worker.os.environ[APP_CONFIG_ENV] == "ambient.toml"
    assert APP_ROOT_ENV not in worker.os.environ


@pytest.mark.anyio
async def test_worker_command_reports_operational_cache_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setattr(worker, "runtime_project_root", lambda: tmp_path)
    monkeypatch.setattr(worker, "configure_cli_logging", lambda *_args: None)
    monkeypatch.setattr(
        worker,
        "resolve_configured_asgi_app",
        lambda **_kwargs: SimpleNamespace(
            app_config=object(),
            app_target="example:app",
        ),
    )
    monkeypatch.setattr(worker, "import_from_string", lambda _target: object())

    async def fail_worker(_app: object, _shutdown: asyncio.Event) -> None:
        raise CacheFeatureError("Redis credentials are secret")

    monkeypatch.setattr(worker, "run_worker", fail_worker)

    with caplog.at_level("ERROR"):
        assert await worker._run_worker_command(config_source=None) == 1

    assert "CacheFeatureError" in caplog.text
    assert "Redis credentials" not in caplog.text
