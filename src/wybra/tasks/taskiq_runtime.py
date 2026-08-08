"""Private construction of the cache-backed Taskiq runtime."""

from __future__ import annotations

import importlib
from types import ModuleType
from typing import TYPE_CHECKING

from wybra.core.exceptions import ConfigurationError

if TYPE_CHECKING:
    from wybra.cache import CacheInstance
    from wybra.tasks.registry import TaskRegistry
    from wybra.tasks.settings import TasksSettings
    from wybra.tasks.taskiq_capabilities import CacheTaskiqTasksCapability


def load_taskiq() -> ModuleType:
    """Load the optional Taskiq package when a Taskiq backend is selected."""
    try:
        return importlib.import_module("taskiq")
    except ModuleNotFoundError as exc:
        if exc.name == "taskiq":
            message = (
                "Taskiq tasks require the wybra[tasks] optional dependency. "
                "Install wybra[tasks] to enable the Taskiq backend."
            )
        else:
            message = (
                "Taskiq could not be loaded. Check its installed version and "
                "dependencies."
            )
    except Exception:
        message = (
            "Taskiq could not be loaded. Check its installed version and dependencies."
        )
    raise ConfigurationError(message)


def build_taskiq_capability(
    cache: CacheInstance,
    settings: TasksSettings,
    task_registry: TaskRegistry | None,
) -> CacheTaskiqTasksCapability:
    """Construct the private Taskiq runtime from one selected cache instance."""
    load_taskiq()
    from wybra.cache import (
        AtomicCacheCapability,
        CacheTimeCapability,
        LeaseCacheCapability,
        StreamCacheCapability,
        WorkQueueCacheCapability,
    )
    from wybra.tasks.taskiq_broker import CacheTaskiqBroker, TaskiqBrokerPolicy
    from wybra.tasks.taskiq_capabilities import CacheTaskiqTasksCapability
    from wybra.tasks.taskiq_lifecycle import (
        CacheTaskiqLifecycleMiddleware,
        CacheTaskiqSmartRetryMiddleware,
        CacheTaskLifecycle,
        TaskiqLifecyclePolicy,
    )
    from wybra.tasks.taskiq_results import (
        CacheTaskiqResultBackend,
        TaskiqResultPolicy,
        safe_taskiq_result_encoder,
    )

    consumer = settings.worker_id or "taskiq"
    work_queue = cache.require(WorkQueueCacheCapability, consumer="tasks")
    streams = cache.require(StreamCacheCapability, consumer="tasks")
    atomic = cache.require(AtomicCacheCapability, consumer="tasks")
    leases = cache.require(LeaseCacheCapability, consumer="tasks")
    cache_time = cache.require(CacheTimeCapability, consumer="tasks")

    lifecycle = CacheTaskLifecycle(
        streams,
        atomic,
        leases,
        TaskiqLifecyclePolicy(
            owner="task-lifecycle",
            stream="events",
            queue=settings.default_queue,
            worker_id=settings.worker_id,
            status_retention_seconds=settings.status_retention_seconds,
            active_status_timeout_seconds=settings.active_status_timeout_seconds,
        ),
        cache_time,
    )
    lifecycle_middleware = CacheTaskiqLifecycleMiddleware(lifecycle)
    broker = CacheTaskiqBroker(
        work_queue,
        queue=settings.default_queue,
        consumer=consumer,
        policy=TaskiqBrokerPolicy(
            visibility_timeout_seconds=settings.visibility_timeout_seconds,
            wait_timeout_seconds=settings.wait_timeout_seconds,
            maximum_delivery_attempts=settings.max_delivery_attempts,
        ),
        on_publication_rejected=lifecycle_middleware.submission_rejected,
        on_delivery_exhausted=lifecycle_middleware.delivery_exhausted,
    )
    broker.add_middlewares(
        CacheTaskiqSmartRetryMiddleware(
            lifecycle_middleware,
            default_retry_count=settings.max_attempts,
            default_retry_label=True,
            default_delay=settings.initial_delay_seconds,
        ),
        lifecycle_middleware,
    )
    broker.with_result_backend(
        CacheTaskiqResultBackend(
            cache.values,
            TaskiqResultPolicy(
                retention_seconds=settings.result_retention_seconds,
                maximum_serialised_bytes=settings.max_result_bytes,
                encode=safe_taskiq_result_encoder,
            ),
        )
    )
    capability = CacheTaskiqTasksCapability(
        broker=broker,
        lifecycle_store=lifecycle,
        lifecycle_middleware=lifecycle_middleware,
        retry_policy=settings.retry_policy,
        worker_concurrency=settings.worker_concurrency,
        worker_shutdown_grace_seconds=settings.worker_shutdown_grace_seconds,
    )
    if task_registry is not None:
        for definition in task_registry.definitions():
            capability.register(definition)
    return capability


__all__ = ("build_taskiq_capability", "load_taskiq")
