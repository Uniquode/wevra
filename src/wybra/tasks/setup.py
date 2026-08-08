from __future__ import annotations

from wybra.cache import (
    CacheFeatureUnavailableError,
    CacheNotFoundError,
    CachesCapability,
)
from wybra.core.exceptions import ConfigurationError
from wybra.events import EventsCapability
from wybra.site import Site, SiteCapabilityError
from wybra.tasks.capabilities import ImmediateTasksCapability, TasksCapability
from wybra.tasks.models import TaskRegistrationError
from wybra.tasks.registry import discover_task_registry
from wybra.tasks.settings import TasksSettings
from wybra.tasks.taskiq_runtime import build_taskiq_capability, load_taskiq
from wybra.utils.safety import truncate_safe_string


async def setup_site(site: Site) -> None:
    settings = TasksSettings.load_settings(site.config)
    if not settings.enabled:
        return
    if settings.backend == "taskiq":
        return
    site.provide_capability(
        TasksCapability,
        ImmediateTasksCapability(
            settings,
            events=site.optional_capability(EventsCapability),
        ),
    )


async def post_setup_site(site: Site) -> None:
    settings = TasksSettings.load_settings(site.config)
    if not settings.enabled or settings.backend != "taskiq":
        return

    async def finalise_taskiq_setup() -> None:
        await _finalise_taskiq_setup(site, settings)

    site.defer_post_setup_finalisation(finalise_taskiq_setup)


async def _finalise_taskiq_setup(site: Site, settings: TasksSettings) -> None:
    caches = site.optional_capability(CachesCapability)
    if caches is None:
        raise SiteCapabilityError("Taskiq tasks require wybra.cache to be configured.")
    cache = None
    try:
        cache = caches.require(settings.cache_name, consumer="tasks")
    except CacheNotFoundError:
        pass
    if cache is None:
        cache_name = truncate_safe_string(settings.cache_name)
        raise SiteCapabilityError(
            f"Taskiq tasks configured cache {cache_name!r} was not found."
        )
    try:
        load_taskiq()
    except ConfigurationError as exc:
        raise SiteCapabilityError(str(exc)) from None
    try:
        capability = build_taskiq_capability(
            cache,
            settings,
            discover_task_registry(site.modules),
        )
    except (
        CacheFeatureUnavailableError,
        ConfigurationError,
        TaskRegistrationError,
        ValueError,
    ) as exc:
        raise SiteCapabilityError(str(exc)) from None
    site.provide_capability(TasksCapability, capability)


__all__ = ("post_setup_site", "setup_site")
