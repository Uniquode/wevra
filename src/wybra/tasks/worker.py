"""Command-line worker for cache-backed Taskiq tasks."""

from __future__ import annotations

import asyncio
import logging
import os
import signal
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

import click

from wybra.cache import CacheFeatureError
from wybra.core.composition import CompositionError
from wybra.core.exceptions import ConfigurationError
from wybra.core.logging import LoggingConfigurationError
from wybra.site import Site, SiteCapabilityError, get_site
from wybra.tasks.capabilities import TasksCapability
from wybra.tasks.lifecycle import TaskLifecycleError
from wybra.tools.app_startup import (
    CONFIG_SOURCE_CONTEXT_KEY,
    CONFIG_SOURCE_HELP,
    CONFIG_SOURCE_OPTION,
    resolve_configured_asgi_app,
)
from wybra.tools.cli_logging import configure_cli_logging
from wybra.tools.lifespan import configured_asgi_lifespan
from wybra.tools.project import (
    ProjectToolConfigurationError,
    import_from_string,
    runtime_project_root,
)
from wybra.tools.runserver import runserver_environment_overrides

logger = logging.getLogger(__name__)


class _TaskiqReceiver(Protocol):
    async def listen(self, finish_event: asyncio.Event) -> None: ...


@runtime_checkable
class _TaskWorkerCapability(Protocol):
    def receiver(self) -> _TaskiqReceiver: ...


@click.command(
    name="wybra-task-worker",
    context_settings={"help_option_names": ["-h", "--help"], "max_content_width": 120},
    help="Run the configured cache-backed Taskiq worker.",
)
@click.option(
    CONFIG_SOURCE_OPTION,
    CONFIG_SOURCE_CONTEXT_KEY,
    help=CONFIG_SOURCE_HELP,
)
def worker_command(config_source: str | None) -> int:
    """Run the configured Taskiq worker until a termination signal arrives."""

    return asyncio.run(_run_worker_command(config_source=config_source))


async def _run_worker_command(*, config_source: str | None) -> int:
    try:
        project_root = runtime_project_root()
        configure_cli_logging()
        configured = resolve_configured_asgi_app(
            project_root=project_root,
            config_source=config_source,
        )
        configure_cli_logging(configured.app_config)
        with _worker_environment_overrides(
            project_root=project_root,
            config_source=config_source,
        ):
            app = import_from_string(configured.app_target)
            shutdown_requested = asyncio.Event()
            with _shutdown_signals(shutdown_requested):
                await run_worker(app, shutdown_requested)
    except (
        CacheFeatureError,
        CompositionError,
        ConfigurationError,
        LoggingConfigurationError,
        ProjectToolConfigurationError,
        SiteCapabilityError,
        TaskLifecycleError,
    ) as exc:
        logger.error("worker: failed: %s", type(exc).__name__)
        return 1
    return 0


async def run_worker(app: Any, shutdown_requested: asyncio.Event) -> None:
    """Run one configured Taskiq receiver inside the application's lifespan."""

    async with configured_asgi_lifespan(app):
        receiver = _receiver_for_site(get_site(app))
        await receiver.listen(shutdown_requested)


def _receiver_for_site(site: Site) -> _TaskiqReceiver:
    capability = site.optional_capability(TasksCapability)
    if not isinstance(capability, _TaskWorkerCapability):
        raise ProjectToolConfigurationError(
            "wybra-task-worker requires enabled tasks with backend = 'taskiq'."
        )
    return capability.receiver()


@contextmanager
def _worker_environment_overrides(
    *,
    project_root: Path,
    config_source: str | None,
) -> Any:
    overrides = runserver_environment_overrides(
        project_root=project_root,
        config_source=config_source,
        database_url=None,
        deployment_environment=None,
    )
    previous = {name: os.environ.get(name) for name in overrides}
    os.environ.update(overrides)
    try:
        yield
    finally:
        for name, value in previous.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value


@contextmanager
def _shutdown_signals(shutdown_requested: asyncio.Event) -> Iterator[None]:
    loop = asyncio.get_running_loop()
    loop_handlers: list[int] = []
    fallback_handlers: list[tuple[int, Any]] = []

    try:
        for signum in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(signum, shutdown_requested.set)
                loop_handlers.append(signum)
            except NotImplementedError, RuntimeError:
                previous = signal.signal(
                    signum,
                    lambda _signum, _frame: loop.call_soon_threadsafe(
                        shutdown_requested.set
                    ),
                )
                fallback_handlers.append((signum, previous))
        yield
    finally:
        for signum in reversed(loop_handlers):
            loop.remove_signal_handler(signum)
        for signum, previous in reversed(fallback_handlers):
            signal.signal(signum, previous)


def main(argv: Sequence[str] | None = None) -> int:
    try:
        result = worker_command.main(
            args=None if argv is None else list(argv),
            prog_name="wybra-task-worker",
            standalone_mode=False,
        )
    except click.exceptions.Exit as exc:
        return int(exc.exit_code or 0)
    except click.ClickException as exc:
        exc.show()
        return int(exc.exit_code or 1)
    return int(result or 0)


__all__ = ("main", "run_worker", "worker_command")
