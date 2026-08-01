from __future__ import annotations

from wybra.cache import CacheNotFoundError, CachesCapability
from wybra.core.exceptions import ConfigurationError
from wybra.events import EventsCapability
from wybra.site import Site, SiteCapabilityError
from wybra.tasks.capabilities import ImmediateTasksCapability, TasksCapability
from wybra.tasks.settings import TasksSettings
from wybra.tasks.taskiq_runtime import load_taskiq
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
    caches = site.optional_capability(CachesCapability)
    if caches is None:
        raise SiteCapabilityError("Taskiq tasks require wybra.cache to be configured.")
    cache_missing = False
    try:
        caches.require(settings.cache_name, consumer="tasks")
    except CacheNotFoundError:
        cache_missing = True
    if cache_missing:
        cache_name = truncate_safe_string(settings.cache_name)
        raise SiteCapabilityError(
            f"Taskiq tasks configured cache {cache_name!r} was not found."
        )
    try:
        load_taskiq()
    except ConfigurationError as exc:
        raise SiteCapabilityError(str(exc)) from None
    raise SiteCapabilityError(
        "Taskiq tasks are configured but no cache-backed Taskiq broker is "
        "available yet."
    )


__all__ = ("post_setup_site", "setup_site")
