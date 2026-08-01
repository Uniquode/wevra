from __future__ import annotations

import asyncio
import logging
import subprocess
import sys
import textwrap
import traceback
from collections.abc import Awaitable, Callable
from dataclasses import replace
from unittest.mock import patch
from uuid import uuid7

import pytest
from fastapi import FastAPI

import wybra.tasks.capabilities as task_capabilities_module
from wybra.config import ConfigSourceError, MappingConfigSource
from wybra.core.exceptions import ConfigurationError
from wybra.diagnostics.context import (
    reset_current_diagnostics,
    set_current_diagnostics,
)
from wybra.diagnostics.event_projection import DiagnosticsEventProjection
from wybra.diagnostics.records import RequestDiagnostics
from wybra.events import EventsCapability
from wybra.site import Site, SiteCapabilityError, start
from wybra.tasks import (
    TASK_EVENT_SCOPE,
    RetryPolicy,
    TaskDispatchPolicy,
    TaskFeature,
    TaskFeatureUnavailableError,
    TaskLifecycleError,
    TaskLifecycleKind,
    TaskLifecycleObservationEvent,
    TaskPayload,
    TaskPayloadError,
    TaskProgressError,
    TaskRegistry,
    TasksCapability,
    TasksSettings,
    TaskState,
    TaskSubmissionOptions,
    current_task_context,
    dispatch,
    task,
)
from wybra.tasks.capabilities import ImmediateTasksCapability
from wybra.tasks.lifecycle import TaskLifecycleEvent, TaskStatusProjection
from wybra.tasks.taskiq_runtime import load_taskiq
from wybra.utils.safety import MAX_SAFE_METADATA_ITEMS


def _exception_nodes(error: BaseException) -> tuple[BaseException, ...]:
    nodes: list[BaseException] = []
    pending = [error]
    seen: set[int] = set()
    while pending:
        node = pending.pop()
        if id(node) in seen:
            continue
        seen.add(id(node))
        nodes.append(node)
        for linked in (node.__cause__, node.__context__):
            if linked is not None:
                pending.append(linked)
    return tuple(nodes)


def test_task_settings_default_to_immediate_backend() -> None:
    settings = TasksSettings.load_settings({})

    assert settings.enabled is True
    assert settings.backend == "immediate"
    assert settings.default_queue == "default"
    assert settings.max_attempts == 1
    assert settings.backoff_multiplier == 2.0
    assert settings.maximum_delay_seconds is None
    assert settings.jitter_seconds == 0.0
    assert settings.status_retention_seconds == 3600.0
    assert settings.worker_concurrency == 1


def test_task_settings_load_complete_operational_policy() -> None:
    settings = TasksSettings.load_settings(
        {
            "tasks": {
                "default_queue": "priority",
                "max_attempts": 4,
                "initial_delay_seconds": 0.5,
                "backoff_multiplier": 3.0,
                "maximum_delay_seconds": 20.0,
                "jitter_seconds": 0.25,
                "status_retention_seconds": 600.0,
                "worker_id": "worker-a",
                "worker_concurrency": 8,
                "scheduler_owner": "scheduler-a",
            }
        }
    )

    assert settings.default_queue == "priority"
    assert settings.retry_policy == RetryPolicy(
        max_attempts=4,
        initial_delay_seconds=0.5,
        backoff_multiplier=3.0,
        maximum_delay_seconds=20.0,
        jitter_seconds=0.25,
    )
    assert settings.status_retention_seconds == 600.0
    assert settings.worker_id == "worker-a"
    assert settings.worker_concurrency == 8
    assert settings.scheduler_owner == "scheduler-a"


def test_task_settings_can_disable_module() -> None:
    settings = TasksSettings.load_settings({"tasks": {"enabled": "false"}})

    assert settings.enabled is False


def test_task_settings_reject_unimplemented_durable_backend() -> None:
    with pytest.raises(ConfigSourceError, match="tasks.backend"):
        TasksSettings.load_settings({"tasks": {"backend": "taskiq-redis"}})


def test_task_settings_accept_taskiq_backend_and_named_cache() -> None:
    settings = TasksSettings.load_settings(
        {"tasks": {"backend": "taskiq", "cache_name": "task_work"}}
    )

    assert settings.backend == "taskiq"
    assert settings.cache_name == "task_work"


@pytest.mark.parametrize(
    "cache_name",
    ("", " ", " task_work ", "invalid-name", "Default"),
)
def test_task_settings_immediate_backend_ignores_cache_name(cache_name: str) -> None:
    loaded = TasksSettings.load_settings(
        {"tasks": {"backend": "immediate", "cache_name": cache_name}}
    )
    constructed = TasksSettings(backend="immediate", cache_name=cache_name)

    assert loaded.cache_name == cache_name
    assert constructed.cache_name == cache_name


def test_task_settings_immediate_cache_name_must_remain_a_string() -> None:
    with pytest.raises(ConfigSourceError, match="tasks.cache_name"):
        TasksSettings.load_settings(
            {"tasks": {"backend": "immediate", "cache_name": 42}}
        )
    with pytest.raises(ConfigurationError, match="cache_name must be a string"):
        TasksSettings(backend="immediate", cache_name=42)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "cache_name",
    ("", " ", " task_work ", "invalid-name", "Default"),
)
def test_task_settings_reject_invalid_taskiq_cache_name(cache_name: str) -> None:
    with pytest.raises(
        (ConfigSourceError, ConfigurationError),
        match="cache_name|invalid cache name",
    ):
        TasksSettings.load_settings(
            {"tasks": {"backend": "taskiq", "cache_name": cache_name}}
        )


def test_taskiq_cache_name_diagnostic_is_bounded_and_secret_safe() -> None:
    marker = "TOP_SECRET"
    cache_name = f"{marker}-" + ("x" * 10_000)

    with pytest.raises(ConfigurationError) as raised:
        TasksSettings.load_settings(
            {"tasks": {"backend": "taskiq", "cache_name": cache_name}}
        )

    for node in _exception_nodes(raised.value):
        diagnostic = str(node)
        assert marker not in diagnostic
        assert cache_name not in diagnostic
        assert len(diagnostic) <= 400
    rendered = "".join(traceback.format_exception(raised.value))
    assert marker not in rendered
    assert cache_name not in rendered
    assert len(rendered) <= 1_200


@pytest.mark.anyio
async def test_taskiq_invalid_cache_name_startup_chain_is_secret_safe() -> None:
    marker = "STARTUP_SECRET"
    cache_name = f"{marker}-" + ("x" * 10_000)

    with pytest.raises(SiteCapabilityError) as raised:
        await start(
            FastAPI(),
            config_source=MappingConfigSource(
                {
                    "app": {"modules": ("wybra.tasks",)},
                    "tasks": {"backend": "taskiq", "cache_name": cache_name},
                }
            ),
        )

    for node in _exception_nodes(raised.value):
        diagnostic = str(node)
        assert marker not in diagnostic
        assert cache_name not in diagnostic
        assert len(diagnostic) <= 600


def test_taskiq_loader_reports_missing_optional_dependency(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def missing_import(name: str) -> object:
        raise ModuleNotFoundError(name=name)

    monkeypatch.setattr(
        "wybra.tasks.taskiq_runtime.importlib.import_module",
        missing_import,
    )

    with pytest.raises(ConfigurationError, match=r"Install wybra\[tasks\]"):
        load_taskiq()


def test_taskiq_loader_reports_broken_installed_dependency(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def broken_import(name: str) -> object:
        raise ImportError("missing transitive dependency")

    monkeypatch.setattr(
        "wybra.tasks.taskiq_runtime.importlib.import_module",
        broken_import,
    )

    with pytest.raises(ConfigurationError, match="could not be loaded"):
        load_taskiq()


def test_taskiq_loader_reports_missing_transitive_module(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def broken_import(name: str) -> object:
        raise ModuleNotFoundError(
            "No module named 'taskiq_dependency'",
            name="taskiq_dependency",
        )

    monkeypatch.setattr(
        "wybra.tasks.taskiq_runtime.importlib.import_module",
        broken_import,
    )

    with pytest.raises(ConfigurationError, match="could not be loaded") as raised:
        load_taskiq()

    assert "taskiq_dependency" not in str(raised.value)


def test_taskiq_loader_bounds_unexpected_initialisation_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider_secret = "redis://user:secret@example.invalid/0"

    def broken_import(name: str) -> object:
        raise RuntimeError(provider_secret)

    monkeypatch.setattr(
        "wybra.tasks.taskiq_runtime.importlib.import_module",
        broken_import,
    )

    with pytest.raises(ConfigurationError, match="could not be loaded") as raised:
        load_taskiq()

    diagnostic = str(raised.value)
    assert provider_secret not in diagnostic
    assert "secret" not in diagnostic
    assert raised.value.__cause__ is None
    assert raised.value.__context__ is None


def test_taskiq_loader_returns_installed_package() -> None:
    assert load_taskiq().__name__ == "taskiq"


def test_baseline_task_paths_do_not_import_taskiq() -> None:
    script = textwrap.dedent(
        """
        import asyncio
        import sys

        from fastapi import FastAPI


        class RejectTaskiqImports:
            def find_spec(self, fullname, path=None, target=None):
                if fullname == "taskiq" or fullname.startswith("taskiq."):
                    raise AssertionError(f"unexpected Taskiq import: {fullname}")
                return None


        sys.meta_path.insert(0, RejectTaskiqImports())

        from wybra.config import MappingConfigSource
        from wybra.site import start
        from wybra.tasks import TaskRegistry, TasksCapability, task

        registry = TaskRegistry()


        @task(name="tests.import_boundary", registry=registry)
        async def operation():
            return "complete"


        async def main():
            assert await operation.run() == "complete"
            for task_config in (
                {"enabled": False, "backend": "taskiq"},
                {"backend": "immediate"},
            ):
                site = await start(
                    FastAPI(),
                    config_source=MappingConfigSource(
                        {
                            "app": {"modules": ("wybra.tasks",)},
                            "tasks": task_config,
                        }
                    ),
                )
                try:
                    if task_config.get("enabled", True):
                        assert site.optional_capability(TasksCapability) is not None
                    else:
                        assert site.optional_capability(TasksCapability) is None
                finally:
                    await site.close()


        asyncio.run(main())
        assert not any(
            name == "taskiq" or name.startswith("taskiq.") for name in sys.modules
        )
        """
    )

    completed = subprocess.run(
        [sys.executable, "-c", script],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr


@pytest.mark.anyio
async def test_site_without_tasks_module_has_no_task_capability() -> None:
    site = await start(
        FastAPI(),
        config_source=MappingConfigSource({"app": {"modules": ()}}),
    )

    try:
        assert site.optional_capability(TasksCapability) is None
    finally:
        await site.close()


@pytest.mark.anyio
async def test_disabled_tasks_module_does_not_resolve_capability() -> None:
    registry = TaskRegistry()
    calls = 0

    @task(name="tests.disabled_direct", registry=registry)
    async def operation() -> None:
        nonlocal calls
        calls += 1

    site = await start(
        FastAPI(),
        config_source=MappingConfigSource(
            {
                "app": {"modules": ("wybra.tasks",)},
                "tasks": {"enabled": False},
            }
        ),
    )

    try:
        proxy = site.capability_proxy(TasksCapability)

        assert site.optional_capability(TasksCapability) is None
        assert proxy.available() is False
        assert await proxy.optional() is None
        await operation.run()
        assert calls == 1
    finally:
        await site.close()


@pytest.mark.anyio
async def test_configured_tasks_module_provides_immediate_capability() -> None:
    registry = TaskRegistry()

    @task(name="tests.configured_immediate", registry=registry)
    async def operation() -> None:
        return None

    site = await start(
        FastAPI(),
        config_source=MappingConfigSource(
            {
                "app": {"modules": ("wybra.tasks",)},
                "tasks": {"backend": "immediate"},
            }
        ),
    )

    try:
        capability = site.require_capability(TasksCapability)
        assert isinstance(capability, ImmediateTasksCapability)
        handle = await capability.submit(operation, operation.payload())
        assert (await handle.status()).state is TaskState.SUCCEEDED  # type: ignore[union-attr]
    finally:
        await site.close()


@pytest.mark.anyio
async def test_immediate_tasks_do_not_load_taskiq(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def unexpected_load() -> object:
        pytest.fail("Immediate tasks must not load Taskiq.")

    monkeypatch.setattr("wybra.tasks.setup.load_taskiq", unexpected_load)
    site = await start(
        FastAPI(),
        config_source=MappingConfigSource(
            {
                "app": {"modules": ("wybra.tasks",)},
                "tasks": {"backend": "immediate"},
            }
        ),
    )

    try:
        assert isinstance(
            site.require_capability(TasksCapability),
            ImmediateTasksCapability,
        )
    finally:
        await site.close()


@pytest.mark.anyio
async def test_disabled_taskiq_backend_does_not_load_taskiq(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def unexpected_load() -> object:
        pytest.fail("Disabled tasks must not load Taskiq.")

    monkeypatch.setattr("wybra.tasks.setup.load_taskiq", unexpected_load)
    site = await start(
        FastAPI(),
        config_source=MappingConfigSource(
            {
                "app": {"modules": ("wybra.tasks",)},
                "tasks": {"enabled": False, "backend": "taskiq"},
            }
        ),
    )

    try:
        assert site.optional_capability(TasksCapability) is None
    finally:
        await site.close()


@pytest.mark.anyio
async def test_taskiq_backend_requires_configured_caches_capability(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def unexpected_load() -> object:
        pytest.fail("Taskiq loader must run after cache selection.")

    monkeypatch.setattr("wybra.tasks.setup.load_taskiq", unexpected_load)
    with pytest.raises(SiteCapabilityError, match="require wybra.cache"):
        await start(
            FastAPI(),
            config_source=MappingConfigSource(
                {
                    "app": {"modules": ("wybra.tasks",)},
                    "tasks": {"backend": "taskiq"},
                }
            ),
        )


@pytest.mark.anyio
async def test_taskiq_backend_preflight_runs_after_cache_finalisation() -> None:
    with pytest.raises(SiteCapabilityError, match="no cache-backed Taskiq broker"):
        await start(
            FastAPI(),
            config_source=MappingConfigSource(
                {
                    "app": {"modules": ("wybra.tasks", "wybra.cache")},
                    "tasks": {"backend": "taskiq"},
                }
            ),
        )


@pytest.mark.anyio
async def test_taskiq_backend_requires_configured_named_cache() -> None:
    with pytest.raises(SiteCapabilityError, match="configured cache 'task_work'"):
        await start(
            FastAPI(),
            config_source=MappingConfigSource(
                {
                    "app": {"modules": ("wybra.cache", "wybra.tasks")},
                    "tasks": {"backend": "taskiq", "cache_name": "task_work"},
                }
            ),
        )


@pytest.mark.anyio
async def test_taskiq_missing_cache_diagnostic_bounds_long_name() -> None:
    marker = "missing_secret_cache"
    cache_name = ("a" * 10_000) + marker

    with pytest.raises(SiteCapabilityError) as raised:
        await start(
            FastAPI(),
            config_source=MappingConfigSource(
                {
                    "app": {"modules": ("wybra.cache", "wybra.tasks")},
                    "tasks": {"backend": "taskiq", "cache_name": cache_name},
                }
            ),
        )

    nodes = _exception_nodes(raised.value)
    assert nodes
    for node in nodes:
        diagnostic = str(node)
        assert marker not in diagnostic
        assert cache_name not in diagnostic
        assert len(diagnostic) <= 600
    assert "..." in str(raised.value)


@pytest.mark.anyio
async def test_taskiq_backend_reports_missing_optional_dependency_at_startup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def missing_taskiq() -> object:
        raise ConfigurationError(
            "Taskiq tasks require the wybra[tasks] optional dependency."
        )

    monkeypatch.setattr("wybra.tasks.setup.load_taskiq", missing_taskiq)
    with pytest.raises(SiteCapabilityError, match=r"wybra\[tasks\]"):
        await start(
            FastAPI(),
            config_source=MappingConfigSource(
                {
                    "app": {"modules": ("wybra.cache", "wybra.tasks")},
                    "tasks": {"backend": "taskiq"},
                }
            ),
        )


@pytest.mark.anyio
async def test_taskiq_preflight_never_provides_tasks_capability(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provided_types: list[type[object]] = []
    provide_capability = Site.provide_capability

    def recording_provide(
        site: Site,
        capability_type: type[object],
        capability: object,
    ) -> None:
        provided_types.append(capability_type)
        provide_capability(site, capability_type, capability)

    monkeypatch.setattr(Site, "provide_capability", recording_provide)
    configurations = (
        {
            "app": {"modules": ("wybra.tasks",)},
            "tasks": {"backend": "taskiq"},
        },
        {
            "app": {"modules": ("wybra.cache", "wybra.tasks")},
            "tasks": {"backend": "taskiq", "cache_name": "task_work"},
        },
        {
            "app": {"modules": ("wybra.cache", "wybra.tasks")},
            "tasks": {"backend": "taskiq"},
        },
    )

    for configuration in configurations:
        with pytest.raises(SiteCapabilityError):
            await start(
                FastAPI(),
                config_source=MappingConfigSource(configuration),
            )

    def missing_taskiq() -> object:
        raise ConfigurationError("Taskiq optional dependency is unavailable.")

    monkeypatch.setattr("wybra.tasks.setup.load_taskiq", missing_taskiq)
    with pytest.raises(SiteCapabilityError):
        await start(
            FastAPI(),
            config_source=MappingConfigSource(
                {
                    "app": {"modules": ("wybra.cache", "wybra.tasks")},
                    "tasks": {"backend": "taskiq"},
                }
            ),
        )

    assert TasksCapability not in provided_types


def test_immediate_provider_rejects_scheduling_features() -> None:
    capability = ImmediateTasksCapability(TasksSettings())

    assert capability.features.supports(TaskFeature.DEFERRED) is False
    assert capability.features.supports(TaskFeature.RECURRING) is False
    assert capability.features.supports("deferred") is False
    with pytest.raises(TaskFeatureUnavailableError, match="deferred"):
        capability.features.require(TaskFeature.DEFERRED)
    with pytest.raises(TaskFeatureUnavailableError, match="recurring"):
        capability.features.require(TaskFeature.RECURRING)


def test_status_projection_rejects_invalid_transition() -> None:
    projection = TaskStatusProjection()
    submitted = TaskLifecycleEvent.new(
        kind=TaskLifecycleKind.SUBMITTED,
        task_name="tests.example",
        schema_version=1,
        queue="default",
    )
    projection.apply(submitted)

    with pytest.raises(TaskLifecycleError, match="Invalid task lifecycle"):
        projection.apply(
            replace(
                submitted,
                kind=TaskLifecycleKind.SUCCEEDED,
                occurred_at=submitted.occurred_at + 1,
            )
        )


@pytest.mark.parametrize(
    ("field_name", "value"),
    (
        ("task_name", "tests.other"),
        ("schema_version", 2),
        ("queue", "other"),
        ("correlation_id", uuid7()),
        ("causation_id", uuid7()),
    ),
)
def test_status_projection_rejects_changed_task_identity(
    field_name: str,
    value: object,
) -> None:
    projection = TaskStatusProjection()
    submitted = TaskLifecycleEvent.new(
        kind=TaskLifecycleKind.SUBMITTED,
        task_name="tests.example",
        schema_version=1,
        queue="default",
    )
    projection.apply(submitted)

    with pytest.raises(TaskLifecycleError, match="identity"):
        projection.apply(
            replace(
                submitted,
                kind=TaskLifecycleKind.STARTED,
                occurred_at=submitted.occurred_at + 1,
                **{field_name: value},
            )
        )


def test_status_projection_rejects_stale_events() -> None:
    projection = TaskStatusProjection()
    submitted = TaskLifecycleEvent.new(
        kind=TaskLifecycleKind.SUBMITTED,
        task_name="tests.example",
        schema_version=1,
        queue="default",
    )
    projection.apply(submitted)

    with pytest.raises(TaskLifecycleError, match="timestamp"):
        projection.apply(
            replace(
                submitted,
                kind=TaskLifecycleKind.STARTED,
                occurred_at=submitted.occurred_at - 1,
            )
        )


def test_status_projection_rejects_inconsistent_attempt_progression() -> None:
    projection = TaskStatusProjection()
    submitted = TaskLifecycleEvent.new(
        kind=TaskLifecycleKind.SUBMITTED,
        task_name="tests.example",
        schema_version=1,
        queue="default",
    )
    started = replace(
        submitted,
        kind=TaskLifecycleKind.STARTED,
        occurred_at=submitted.occurred_at + 1,
    )
    retry = replace(
        submitted,
        kind=TaskLifecycleKind.RETRY_SCHEDULED,
        occurred_at=submitted.occurred_at + 2,
    )
    projection.apply(submitted)
    projection.apply(started)
    projection.apply(retry)

    with pytest.raises(TaskLifecycleError, match="attempt"):
        projection.apply(
            replace(
                submitted,
                kind=TaskLifecycleKind.STARTED,
                attempt=3,
                occurred_at=submitted.occurred_at + 3,
            )
        )


def test_status_projection_ignores_exact_event_replay() -> None:
    projection = TaskStatusProjection()
    submitted = TaskLifecycleEvent.new(
        kind=TaskLifecycleKind.SUBMITTED,
        task_name="tests.example",
        schema_version=1,
        queue="default",
    )

    first = projection.apply(submitted)
    replayed = projection.apply(submitted)

    assert replayed is first
    assert projection.lifecycle(submitted.task_id) == (submitted,)


def test_status_projection_ignores_exact_replay_after_history_eviction() -> None:
    projection = TaskStatusProjection(history_limit=3)
    submitted = TaskLifecycleEvent.new(
        kind=TaskLifecycleKind.SUBMITTED,
        task_name="tests.example",
        schema_version=1,
        queue="default",
    )
    started = replace(
        submitted,
        kind=TaskLifecycleKind.STARTED,
        occurred_at=submitted.occurred_at + 1,
    )
    first_progress = replace(
        submitted,
        kind=TaskLifecycleKind.PROGRESS,
        occurred_at=submitted.occurred_at + 2,
        progress={"completed": 1},
    )
    projection.apply(submitted)
    projection.apply(started)
    projection.apply(first_progress)
    current = None
    for index in range(3, 7):
        current = projection.apply(
            replace(
                submitted,
                kind=TaskLifecycleKind.PROGRESS,
                occurred_at=submitted.occurred_at + index,
                progress={"completed": index},
            )
        )

    replayed = projection.apply(first_progress)

    assert replayed is current
    assert [
        event.progress["completed"]  # type: ignore[index]
        for event in projection.lifecycle(submitted.task_id)
    ] == [4, 5, 6]


def test_status_projection_bounds_replay_tracking() -> None:
    projection = TaskStatusProjection(history_limit=2, replay_limit=3)
    submitted = TaskLifecycleEvent.new(
        kind=TaskLifecycleKind.SUBMITTED,
        task_name="tests.example",
        schema_version=1,
        queue="default",
    )
    projection.apply(submitted)
    projection.apply(
        replace(
            submitted,
            kind=TaskLifecycleKind.STARTED,
            occurred_at=submitted.occurred_at + 1,
        )
    )
    for index in range(2, 6):
        projection.apply(
            replace(
                submitted,
                kind=TaskLifecycleKind.PROGRESS,
                occurred_at=submitted.occurred_at + index,
                progress={"index": index},
            )
        )

    assert len(projection._applied_event_keys[submitted.task_id]) == 3
    assert len(projection._applied_event_order[submitted.task_id]) == 3


def test_lifecycle_progress_is_json_safe_immutable_and_secret_safe() -> None:
    projection = TaskStatusProjection()
    submitted = TaskLifecycleEvent.new(
        kind=TaskLifecycleKind.SUBMITTED,
        task_name="tests.example",
        schema_version=1,
        queue="default",
    )
    started = replace(
        submitted,
        kind=TaskLifecycleKind.STARTED,
        occurred_at=submitted.occurred_at + 1,
    )
    supplied = {
        "completed": 1,
        "details": {"token": "secret", "items": [1, 2]},
    }
    event = replace(
        submitted,
        kind=TaskLifecycleKind.PROGRESS,
        occurred_at=submitted.occurred_at + 2,
        progress=supplied,
    )
    projection.apply(submitted)
    projection.apply(started)
    projection.apply(event)
    supplied["completed"] = 2
    supplied["details"]["token"] = "changed"  # type: ignore[index]
    exposed_details = event.progress["details"]  # type: ignore[index]
    exposed_details["token"] = "changed again"  # type: ignore[index]

    assert event.progress == {
        "completed": 1,
        "details": {"items": [1, 2], "token": "[redacted]"},
    }
    assert projection.status(event.task_id).progress == event.progress  # type: ignore[union-attr]
    assert projection.lifecycle(event.task_id)[-1].progress == event.progress
    assert "secret" not in repr(event)
    assert "secret" not in repr(projection.status(event.task_id))
    with pytest.raises(TypeError):
        event.progress["completed"] = 3  # type: ignore[index, union-attr]


def test_lifecycle_progress_rejects_non_json_values() -> None:
    with pytest.raises(TaskLifecycleError, match="JSON-compatible"):
        TaskLifecycleEvent.new(
            kind=TaskLifecycleKind.PROGRESS,
            task_name="tests.example",
            schema_version=1,
            queue="default",
            progress={"invalid": object()},
        )


def test_lifecycle_progress_rejects_unbounded_metadata() -> None:
    with pytest.raises(TaskLifecycleError, match="safe limits"):
        TaskLifecycleEvent.new(
            kind=TaskLifecycleKind.PROGRESS,
            task_name="tests.example",
            schema_version=1,
            queue="default",
            progress={"items": list(range(MAX_SAFE_METADATA_ITEMS))},
        )


def test_status_projection_clears_progress_when_retry_attempt_starts() -> None:
    projection = TaskStatusProjection()
    submitted = TaskLifecycleEvent.new(
        kind=TaskLifecycleKind.SUBMITTED,
        task_name="tests.retry_progress",
        schema_version=1,
        queue="default",
    )
    projection.apply(submitted)
    projection.apply(
        replace(
            submitted,
            kind=TaskLifecycleKind.STARTED,
            occurred_at=submitted.occurred_at + 1,
        )
    )
    projection.apply(
        replace(
            submitted,
            kind=TaskLifecycleKind.PROGRESS,
            progress={"completed": 1},
            occurred_at=submitted.occurred_at + 2,
        )
    )
    projection.apply(
        replace(
            submitted,
            kind=TaskLifecycleKind.RETRY_SCHEDULED,
            error_type="RuntimeError",
            occurred_at=submitted.occurred_at + 3,
        )
    )

    status = projection.apply(
        replace(
            submitted,
            kind=TaskLifecycleKind.STARTED,
            attempt=2,
            occurred_at=submitted.occurred_at + 4,
        )
    )

    assert status.progress is None


def test_status_projection_bounds_history() -> None:
    projection = TaskStatusProjection(history_limit=3)
    submitted = TaskLifecycleEvent.new(
        kind=TaskLifecycleKind.SUBMITTED,
        task_name="tests.example",
        schema_version=1,
        queue="default",
    )
    projection.apply(submitted)
    projection.apply(
        replace(
            submitted,
            kind=TaskLifecycleKind.STARTED,
            occurred_at=submitted.occurred_at + 1,
        )
    )
    for index in range(2, 6):
        projection.apply(
            replace(
                submitted,
                kind=TaskLifecycleKind.PROGRESS,
                occurred_at=submitted.occurred_at + index,
                progress={"index": index},
            )
        )

    history = projection.lifecycle(submitted.task_id)

    assert len(history) == 3
    assert [event.progress for event in history] == [
        {"index": 3},
        {"index": 4},
        {"index": 5},
    ]


def test_status_projection_expires_terminal_status_and_history() -> None:
    now = [0.0]
    projection = TaskStatusProjection(
        retention_seconds=5.0,
        _clock=lambda: now[0],
    )
    submitted = replace(
        TaskLifecycleEvent.new(
            kind=TaskLifecycleKind.SUBMITTED,
            task_name="tests.example",
            schema_version=1,
            queue="default",
        ),
        occurred_at=1.0,
    )
    projection.apply(submitted)
    projection.apply(
        replace(submitted, kind=TaskLifecycleKind.STARTED, occurred_at=2.0)
    )
    projection.apply(
        replace(submitted, kind=TaskLifecycleKind.SUCCEEDED, occurred_at=3.0)
    )

    now[0] = 9.0

    assert projection.status(submitted.task_id) is None
    assert projection.lifecycle(submitted.task_id) == ()


@pytest.mark.anyio
async def test_immediate_submission_exposes_successful_lifecycle() -> None:
    registry = TaskRegistry()

    @task(name="tests.add", registry=registry)
    async def add(left: int, right: int) -> int:
        return left + right

    capability = ImmediateTasksCapability(TasksSettings())

    handle = await capability.submit(add, add.payload("2", 3))
    status = await handle.status()

    assert handle.identity == add.identity
    assert status is not None
    assert status.state is TaskState.SUCCEEDED
    assert [event.kind for event in await capability.lifecycle(handle.task_id)] == [
        TaskLifecycleKind.SUBMITTED,
        TaskLifecycleKind.STARTED,
        TaskLifecycleKind.SUCCEEDED,
    ]


@pytest.mark.anyio
async def test_immediate_submission_mirrors_safe_lifecycle_events() -> None:
    registry = TaskRegistry()

    class RecordingEvents:
        def __init__(self) -> None:
            self.events: list[TaskLifecycleObservationEvent] = []

        async def subscribe(
            self,
            selector,
            handler: Callable[[object], Awaitable[None]],
            *,
            history: bool = False,
        ) -> None:
            del selector, handler, history

        async def publish(self, event: object) -> None:
            assert isinstance(event, TaskLifecycleObservationEvent)
            self.events.append(event)

    @task(name="tests.observed", registry=registry)
    async def operation() -> str:
        return "not mirrored"

    events = RecordingEvents()
    capability = ImmediateTasksCapability(TasksSettings(), events=events)

    handle = await capability.submit(operation, operation.payload())

    assert [event.kind for event in events.events] == [
        TaskLifecycleKind.SUBMITTED,
        TaskLifecycleKind.STARTED,
        TaskLifecycleKind.SUCCEEDED,
    ]
    assert [str(event.scope) for event in events.events] == [
        "task.submitted",
        "task.started",
        "task.succeeded",
    ]
    assert all(event.task_id == handle.task_id for event in events.events)
    assert "not mirrored" not in repr(events.events)


@pytest.mark.anyio
async def test_immediate_submission_mirrors_retry_sequence() -> None:
    registry = TaskRegistry()
    attempts = 0

    class RecordingEvents:
        def __init__(self) -> None:
            self.events: list[TaskLifecycleObservationEvent] = []

        async def subscribe(
            self,
            selector,
            handler: Callable[[object], Awaitable[None]],
            *,
            history: bool = False,
        ) -> None:
            del selector, handler, history

        async def publish(self, event: object) -> None:
            assert isinstance(event, TaskLifecycleObservationEvent)
            self.events.append(event)

    @task(name="tests.observed_retry", registry=registry)
    async def operation() -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("retry")

    events = RecordingEvents()
    capability = ImmediateTasksCapability(
        TasksSettings(max_attempts=2),
        events=events,
    )

    await capability.submit(operation, operation.payload())

    assert [event.kind for event in events.events] == [
        TaskLifecycleKind.SUBMITTED,
        TaskLifecycleKind.STARTED,
        TaskLifecycleKind.RETRY_SCHEDULED,
        TaskLifecycleKind.STARTED,
        TaskLifecycleKind.SUCCEEDED,
    ]


@pytest.mark.anyio
async def test_composed_site_mirrors_task_lifecycle_through_core_events() -> None:
    registry = TaskRegistry()
    observed: list[TaskLifecycleObservationEvent] = []

    @task(name="tests.composed_events", registry=registry)
    async def operation() -> None:
        return None

    async def observe_task(event: object) -> None:
        assert isinstance(event, TaskLifecycleObservationEvent)
        observed.append(event)

    site = await start(
        FastAPI(),
        config_source=MappingConfigSource(
            {
                "app": {"modules": ("wybra.tasks",)},
                "wybra.events": {"enabled": True},
            }
        ),
    )
    try:
        events = site.require_capability(EventsCapability)
        await events.subscribe(TASK_EVENT_SCOPE, observe_task)
        capability = site.require_capability(TasksCapability)
        await capability.submit(operation, operation.payload())
    finally:
        await site.close()

    assert [event.kind for event in observed] == [
        TaskLifecycleKind.SUBMITTED,
        TaskLifecycleKind.STARTED,
        TaskLifecycleKind.SUCCEEDED,
    ]


@pytest.mark.anyio
async def test_disabled_core_events_skip_task_observation_construction() -> None:
    registry = TaskRegistry()

    @task(name="tests.disabled_event_construction", registry=registry)
    async def operation() -> None:
        return None

    site = await start(
        FastAPI(),
        config_source=MappingConfigSource(
            {
                "app": {"modules": ("wybra.tasks",)},
                "wybra.events": {"enabled": False},
            }
        ),
    )
    try:
        capability = site.require_capability(TasksCapability)
        with patch.object(
            TaskLifecycleObservationEvent,
            "from_lifecycle",
            side_effect=AssertionError("disabled events must not be constructed"),
        ) as factory:
            handle = await capability.submit(operation, operation.payload())
        factory.assert_not_called()
        assert (await handle.status()).state is TaskState.SUCCEEDED  # type: ignore[union-attr]
    finally:
        await site.close()


def test_task_observation_progress_is_immutable_and_secret_safe() -> None:
    supplied = {
        "api_key": "secret",
        "completed": 1,
    }
    event = TaskLifecycleObservationEvent(
        topic=TASK_EVENT_SCOPE("progress"),
        kind=TaskLifecycleKind.PROGRESS,
        task_id=uuid7(),
        task_name="tests.observation_progress",
        schema_version=1,
        queue="default",
        correlation_id=uuid7(),
        causation_id=None,
        attempt=1,
        worker_id="worker",
        progress=supplied,
    )

    supplied["api_key"] = "changed"
    supplied["completed"] = 2

    assert event.progress == {
        "api_key": "[redacted]",
        "completed": 1,
    }
    with pytest.raises(TypeError):
        event.progress["completed"] = 3  # type: ignore[index, union-attr]


@pytest.mark.anyio
async def test_task_observation_diagnostics_include_execution_identity() -> None:
    diagnostics = RequestDiagnostics(method="TASK", path="/tasks", level="trace")
    token = set_current_diagnostics(diagnostics)
    event = TaskLifecycleObservationEvent(
        topic=TASK_EVENT_SCOPE("failed"),
        kind=TaskLifecycleKind.FAILED,
        task_id=uuid7(),
        task_name="tests.diagnostic_task",
        schema_version=2,
        queue="priority",
        correlation_id=uuid7(),
        causation_id=uuid7(),
        attempt=3,
        worker_id="worker-a",
        error_type="RuntimeError",
    )
    try:
        await DiagnosticsEventProjection((TASK_EVENT_SCOPE,))(event)
    finally:
        reset_current_diagnostics(token)

    assert len(diagnostics.events) == 1
    attributes = diagnostics.events[0].attributes
    assert attributes["kind"] == "failed"
    assert attributes["task_id"] == str(event.task_id)
    assert attributes["task_name"] == "tests.diagnostic_task"
    assert attributes["schema_version"] == 2
    assert attributes["queue"] == "priority"
    assert attributes["correlation_id"] == str(event.correlation_id)
    assert attributes["causation_id"] == str(event.causation_id)
    assert attributes["attempt"] == 3
    assert attributes["worker_id"] == "worker-a"
    assert attributes["error_type"] == "RuntimeError"


@pytest.mark.anyio
async def test_immediate_task_reports_secret_safe_progress() -> None:
    registry = TaskRegistry()
    observed: list[TaskLifecycleObservationEvent] = []

    class RecordingEvents:
        async def subscribe(
            self,
            selector,
            handler: Callable[[object], Awaitable[None]],
            *,
            history: bool = False,
        ) -> None:
            del selector, handler, history

        async def publish(self, event: object) -> None:
            assert isinstance(event, TaskLifecycleObservationEvent)
            observed.append(event)

    @task(name="tests.progress", registry=registry)
    async def operation() -> None:
        context = current_task_context()
        assert context is not None
        await context.report_progress(
            {
                "completed": 2,
                "total": 5,
                "details": {"access_token": "sensitive"},
            }
        )

    capability = ImmediateTasksCapability(
        TasksSettings(),
        events=RecordingEvents(),
    )

    handle = await capability.submit(operation, operation.payload())
    status = await handle.status()

    assert status is not None
    assert status.state is TaskState.SUCCEEDED
    assert status.progress == {
        "completed": 2,
        "details": {"access_token": "[redacted]"},
        "total": 5,
    }
    assert [event.kind for event in await capability.lifecycle(handle.task_id)] == [
        TaskLifecycleKind.SUBMITTED,
        TaskLifecycleKind.STARTED,
        TaskLifecycleKind.PROGRESS,
        TaskLifecycleKind.SUCCEEDED,
    ]
    assert observed[2].progress == status.progress
    assert "sensitive" not in repr(observed)


@pytest.mark.anyio
async def test_direct_task_progress_reporting_is_a_no_op() -> None:
    registry = TaskRegistry()

    @task(name="tests.direct_progress", registry=registry)
    async def operation() -> str:
        context = current_task_context()
        assert context is not None
        await context.report_progress({"completed": 1})
        return "completed"

    assert await operation.run() == "completed"


@pytest.mark.anyio
async def test_direct_task_progress_still_validates_metadata() -> None:
    registry = TaskRegistry()

    @task(name="tests.direct_invalid_progress", registry=registry)
    async def operation() -> None:
        context = current_task_context()
        assert context is not None
        await context.report_progress({"invalid": object()})

    with pytest.raises(TaskProgressError, match="JSON-compatible"):
        await operation.run()


@pytest.mark.anyio
async def test_invalid_submitted_progress_becomes_a_safe_task_failure() -> None:
    registry = TaskRegistry()
    calls = 0

    @task(name="tests.invalid_progress", registry=registry)
    async def operation() -> None:
        nonlocal calls
        calls += 1
        context = current_task_context()
        assert context is not None
        await context.report_progress({"invalid": object()})

    capability = ImmediateTasksCapability(TasksSettings(max_attempts=3))

    handle = await capability.submit(operation, operation.payload())
    status = await handle.status()

    assert calls == 1
    assert status is not None
    assert status.state is TaskState.DEAD_LETTERED
    assert status.error_type == "TaskProgressError"
    assert status.progress is None
    assert [event.kind for event in await capability.lifecycle(handle.task_id)] == [
        TaskLifecycleKind.SUBMITTED,
        TaskLifecycleKind.STARTED,
        TaskLifecycleKind.FAILED,
        TaskLifecycleKind.DEAD_LETTERED,
    ]


@pytest.mark.anyio
async def test_late_progress_report_is_rejected_without_changing_status() -> None:
    registry = TaskRegistry()
    retained_context = None

    @task(name="tests.late_progress", registry=registry)
    async def operation() -> None:
        nonlocal retained_context
        retained_context = current_task_context()

    capability = ImmediateTasksCapability(TasksSettings())
    handle = await capability.submit(operation, operation.payload())
    original_lifecycle = await capability.lifecycle(handle.task_id)

    assert retained_context is not None
    with pytest.raises(TaskLifecycleError, match="Invalid task lifecycle"):
        await retained_context.report_progress({"completed": 1})

    assert (await handle.status()).state is TaskState.SUCCEEDED  # type: ignore[union-attr]
    assert await capability.lifecycle(handle.task_id) == original_lifecycle


@pytest.mark.anyio
async def test_lifecycle_event_publication_failure_does_not_change_execution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = TaskRegistry()
    calls = 0

    class FailingEvents:
        async def subscribe(
            self,
            selector,
            handler: Callable[[object], Awaitable[None]],
            *,
            history: bool = False,
        ) -> None:
            del selector, handler, history

        async def publish(self, event: object) -> None:
            del event
            raise RuntimeError("publisher unavailable")

    @task(name="tests.publication_failure", registry=registry)
    async def operation() -> None:
        nonlocal calls
        calls += 1

    capability = ImmediateTasksCapability(TasksSettings(), events=FailingEvents())
    monkeypatch.setattr(
        task_capabilities_module.logger,
        "warning",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            RuntimeError("warning handler unavailable")
        ),
    )

    handle = await capability.submit(operation, operation.payload())

    assert calls == 1
    assert (await handle.status()).state is TaskState.SUCCEEDED  # type: ignore[union-attr]
    assert [event.kind for event in await capability.lifecycle(handle.task_id)] == [
        TaskLifecycleKind.SUBMITTED,
        TaskLifecycleKind.STARTED,
        TaskLifecycleKind.SUCCEEDED,
    ]


@pytest.mark.anyio
async def test_lifecycle_logging_failure_does_not_change_execution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = TaskRegistry()
    calls = 0

    @task(name="tests.logging_failure", registry=registry)
    async def operation() -> None:
        nonlocal calls
        calls += 1
        context = current_task_context()
        assert context is not None
        await context.report_progress({"completed": 1})

    monkeypatch.setattr(
        task_capabilities_module.logger,
        "info",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            RuntimeError("logging handler unavailable")
        ),
    )
    capability = ImmediateTasksCapability(TasksSettings(max_attempts=2))

    handle = await capability.submit(operation, operation.payload())

    assert calls == 1
    assert (await handle.status()).state is TaskState.SUCCEEDED  # type: ignore[union-attr]
    assert [event.kind for event in await capability.lifecycle(handle.task_id)] == [
        TaskLifecycleKind.SUBMITTED,
        TaskLifecycleKind.STARTED,
        TaskLifecycleKind.PROGRESS,
        TaskLifecycleKind.SUCCEEDED,
    ]


@pytest.mark.anyio
async def test_sink_cancelled_error_does_not_cancel_task_execution() -> None:
    registry = TaskRegistry()
    calls = 0

    class CancellingEvents:
        async def subscribe(
            self,
            selector,
            handler: Callable[[object], Awaitable[None]],
            *,
            history: bool = False,
        ) -> None:
            del selector, handler, history

        async def publish(self, event: object) -> None:
            del event
            raise asyncio.CancelledError

    @task(name="tests.sink_cancellation", registry=registry)
    async def operation() -> None:
        nonlocal calls
        calls += 1

    capability = ImmediateTasksCapability(
        TasksSettings(),
        events=CancellingEvents(),
    )

    handle = await capability.submit(operation, operation.payload())

    assert calls == 1
    assert (await handle.status()).state is TaskState.SUCCEEDED  # type: ignore[union-attr]


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("configured_worker_id", "expected_worker_id"),
    ((None, "immediate"), ("inline-a", "inline-a")),
)
async def test_immediate_submission_uses_effective_worker_identity(
    configured_worker_id: str | None,
    expected_worker_id: str,
) -> None:
    registry = TaskRegistry()
    observed_worker_id: str | None = None

    @task(name="tests.worker_identity", registry=registry)
    async def inspect_worker() -> None:
        nonlocal observed_worker_id
        context = current_task_context()
        assert context is not None
        observed_worker_id = context.worker_id

    capability = ImmediateTasksCapability(TasksSettings(worker_id=configured_worker_id))

    handle = await capability.submit(inspect_worker, inspect_worker.payload())
    lifecycle = await capability.lifecycle(handle.task_id)

    assert observed_worker_id == expected_worker_id
    assert all(
        event.worker_id == expected_worker_id
        for event in lifecycle
        if event.kind is not TaskLifecycleKind.SUBMITTED
    )


@pytest.mark.anyio
async def test_immediate_submission_retries_and_records_terminal_failure() -> None:
    registry = TaskRegistry()
    attempts = 0

    @task(name="tests.fail", registry=registry)
    async def fail() -> None:
        nonlocal attempts
        attempts += 1
        raise RuntimeError("sensitive failure")

    capability = ImmediateTasksCapability(TasksSettings(max_attempts=2))

    handle = await capability.submit(fail, fail.payload())
    status = await handle.status()

    assert attempts == 2
    assert status is not None
    assert status.state is TaskState.DEAD_LETTERED
    assert status.error_type == "RuntimeError"
    assert "sensitive" not in repr(status)


@pytest.mark.anyio
async def test_handler_lifecycle_error_uses_configured_retries() -> None:
    registry = TaskRegistry()
    attempts = 0

    @task(name="tests.lifecycle_error", registry=registry)
    async def fail() -> None:
        nonlocal attempts
        attempts += 1
        raise TaskLifecycleError("handler lifecycle failure")

    capability = ImmediateTasksCapability(TasksSettings(max_attempts=3))

    handle = await capability.submit(fail, fail.payload())
    status = await handle.status()

    assert attempts == 3
    assert status is not None
    assert status.state is TaskState.DEAD_LETTERED
    assert status.error_type == "TaskLifecycleError"


@pytest.mark.anyio
async def test_task_lifecycle_logs_use_secret_safe_structured_fields(
    caplog: pytest.LogCaptureFixture,
) -> None:
    registry = TaskRegistry()

    @task(name="tests.safe_logs", registry=registry)
    async def fail(password: str) -> None:
        raise RuntimeError(f"credential leaked: {password}")

    capability = ImmediateTasksCapability(TasksSettings())

    with caplog.at_level(logging.INFO, logger="wybra.tasks.capabilities"):
        await capability.submit(fail, fail.payload(password="sensitive-value"))

    records = [
        record.wybra_task for record in caplog.records if hasattr(record, "wybra_task")
    ]
    assert [record["kind"] for record in records] == [
        "submitted",
        "started",
        "failed",
        "dead_lettered",
    ]
    assert records[-1]["task_name"] == "tests.safe_logs"
    assert records[-1]["error_type"] == "RuntimeError"
    assert "sensitive-value" not in caplog.text
    assert "sensitive-value" not in repr(records)


@pytest.mark.anyio
async def test_immediate_submission_honours_explicit_single_attempt_policy() -> None:
    registry = TaskRegistry()
    attempts = 0

    @task(
        name="tests.single_attempt",
        retry=RetryPolicy(max_attempts=1),
        registry=registry,
    )
    async def fail() -> None:
        nonlocal attempts
        attempts += 1
        raise RuntimeError

    capability = ImmediateTasksCapability(TasksSettings(max_attempts=3))

    await capability.submit(fail, fail.payload())

    assert attempts == 1


@pytest.mark.anyio
async def test_immediate_submission_applies_configured_retry_jitter() -> None:
    registry = TaskRegistry()
    delays: list[float] = []

    async def record_delay(delay: float) -> None:
        delays.append(delay)

    @task(name="tests.jitter", registry=registry)
    async def fail() -> None:
        raise RuntimeError

    capability = ImmediateTasksCapability(
        TasksSettings(
            max_attempts=2,
            initial_delay_seconds=2.0,
            jitter_seconds=4.0,
        ),
        _random=lambda: 0.5,
        _sleep=record_delay,
    )

    await capability.submit(fail, fail.payload())

    assert delays == [4.0]


@pytest.mark.anyio
async def test_immediate_submission_caps_retry_before_backoff_overflows() -> None:
    registry = TaskRegistry()
    delays: list[float] = []

    async def record_delay(delay: float) -> None:
        delays.append(delay)

    @task(
        name="tests.capped_backoff",
        retry=RetryPolicy(
            max_attempts=4,
            initial_delay_seconds=1.0,
            backoff_multiplier=1e308,
            maximum_delay_seconds=10.0,
        ),
        registry=registry,
    )
    async def fail() -> None:
        raise RuntimeError

    capability = ImmediateTasksCapability(
        TasksSettings(),
        _sleep=record_delay,
    )

    handle = await capability.submit(fail, fail.payload())

    assert delays == [1.0, 10.0, 10.0]
    assert (await handle.status()).state is TaskState.DEAD_LETTERED  # type: ignore[union-attr]


@pytest.mark.anyio
async def test_immediate_submission_validates_hand_constructed_payload() -> None:
    registry = TaskRegistry()
    received: list[int] = []

    @task(name="tests.validated_submit", registry=registry)
    async def operation(value: int) -> None:
        received.append(value)

    capability = ImmediateTasksCapability(TasksSettings())

    handle = await capability.submit(operation, TaskPayload({"value": "2"}))

    assert received == [2]
    assert (await handle.status()).state is TaskState.SUCCEEDED  # type: ignore[union-attr]


@pytest.mark.anyio
async def test_immediate_submission_rejects_invalid_payload_before_lifecycle() -> None:
    registry = TaskRegistry()
    calls = 0

    @task(name="tests.invalid_submit", registry=registry)
    async def operation(value: int) -> None:
        nonlocal calls
        calls += 1

    capability = ImmediateTasksCapability(TasksSettings())

    with pytest.raises(TaskPayloadError):
        await capability.submit(operation, TaskPayload({"value": "invalid"}))

    assert calls == 0


@pytest.mark.anyio
async def test_immediate_submission_propagates_idempotency_context() -> None:
    registry = TaskRegistry()
    observed_key: str | None = None

    @task(name="tests.idempotency", registry=registry)
    async def operation() -> None:
        nonlocal observed_key
        context = current_task_context()
        assert context is not None
        observed_key = context.idempotency_key

    capability = ImmediateTasksCapability(TasksSettings())

    await capability.submit(
        operation,
        operation.payload(),
        options=TaskSubmissionOptions(idempotency_key="message-123"),
    )

    assert observed_key == "message-123"


@pytest.mark.parametrize(
    ("field_name", "value"),
    (
        ("queue", ""),
        ("queue", "   "),
        ("queue", 1),
        ("idempotency_key", ""),
        ("idempotency_key", "   "),
        ("idempotency_key", 1),
    ),
)
def test_submission_options_reject_invalid_metadata(
    field_name: str,
    value: object,
) -> None:
    with pytest.raises(ValueError, match=field_name.replace("_", " ")):
        TaskSubmissionOptions(**{field_name: value})  # type: ignore[arg-type]


def test_submission_options_normalise_metadata() -> None:
    options = TaskSubmissionOptions(
        queue=" priority ",
        idempotency_key=" message-123 ",
    )

    assert options.queue == "priority"
    assert options.idempotency_key == "message-123"


@pytest.mark.anyio
async def test_dispatch_forwards_submission_options() -> None:
    registry = TaskRegistry()
    observed_key: str | None = None

    @task(name="tests.dispatch_options", registry=registry)
    async def operation() -> None:
        nonlocal observed_key
        context = current_task_context()
        assert context is not None
        observed_key = context.idempotency_key

    capability = ImmediateTasksCapability(TasksSettings())
    site = Site(
        app=FastAPI(),
        config=MappingConfigSource({}),  # type: ignore[arg-type]
    )
    site.provide_capability(TasksCapability, capability)

    result = await dispatch(
        site,
        operation,
        operation.payload(),
        policy=TaskDispatchPolicy.BACKGROUND,
        options=TaskSubmissionOptions(
            queue="priority",
            idempotency_key="message-123",
        ),
    )

    assert result.identity == operation.identity
    assert observed_key == "message-123"
    assert (await result.status()).queue == "priority"


@pytest.mark.anyio
@pytest.mark.parametrize(
    "policy",
    (TaskDispatchPolicy.DIRECT, TaskDispatchPolicy.PREFER_BACKGROUND),
)
async def test_direct_dispatch_rejects_submission_options(
    policy: TaskDispatchPolicy,
) -> None:
    registry = TaskRegistry()

    @task(name="tests.direct_options", registry=registry)
    async def operation() -> None:
        raise AssertionError("Task must not execute.")

    site = Site(
        app=FastAPI(),
        config=MappingConfigSource({}),  # type: ignore[arg-type]
    )

    with pytest.raises(ValueError, match="submission options"):
        await dispatch(
            site,
            operation,
            operation.payload(),
            policy=policy,
            options=TaskSubmissionOptions(idempotency_key="message-123"),
        )


@pytest.mark.anyio
async def test_prefer_background_runs_directly_when_capability_is_absent() -> None:
    registry = TaskRegistry()
    calls = 0

    @task(name="tests.direct_fallback", registry=registry)
    async def operation() -> None:
        nonlocal calls
        calls += 1

    site = Site(
        app=FastAPI(),
        config=MappingConfigSource({}),  # type: ignore[arg-type]
    )

    result = await dispatch(
        site,
        operation,
        operation.payload(),
        policy=TaskDispatchPolicy.PREFER_BACKGROUND,
    )

    assert result is None
    assert calls == 1


@pytest.mark.anyio
async def test_background_requires_capability_without_invoking_task() -> None:
    registry = TaskRegistry()
    calls = 0

    @task(name="tests.background_required", registry=registry)
    async def operation() -> None:
        nonlocal calls
        calls += 1

    site = Site(
        app=FastAPI(),
        config=MappingConfigSource({}),  # type: ignore[arg-type]
    )

    with pytest.raises(RuntimeError, match="requires TasksCapability"):
        await dispatch(
            site,
            operation,
            operation.payload(),
            policy=TaskDispatchPolicy.BACKGROUND,
        )

    assert calls == 0


@pytest.mark.anyio
async def test_prefer_background_does_not_fallback_after_submission_failure() -> None:
    registry = TaskRegistry()
    calls = 0

    @task(name="tests.no_fallback", registry=registry)
    async def operation() -> None:
        nonlocal calls
        calls += 1

    class FailingCapability:
        features = ImmediateTasksCapability(TasksSettings()).features

        async def submit(self, definition, payload, *, options=None):
            del definition, payload, options
            raise RuntimeError("broker unavailable")

        async def status(self, task_id):
            del task_id
            return None

        async def lifecycle(self, task_id):
            del task_id
            return ()

    site = Site(
        app=FastAPI(),
        config=MappingConfigSource({}),  # type: ignore[arg-type]
    )
    site.provide_capability(TasksCapability, FailingCapability())

    with pytest.raises(RuntimeError, match="broker unavailable"):
        await dispatch(
            site,
            operation,
            operation.payload(),
            policy=TaskDispatchPolicy.PREFER_BACKGROUND,
        )

    assert calls == 0
