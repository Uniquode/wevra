from __future__ import annotations

from wybra.events import EventsCapability
from wybra.site import Site
from wybra.tasks.capabilities import ImmediateTasksCapability, TasksCapability
from wybra.tasks.settings import TasksSettings


async def setup_site(site: Site) -> None:
    settings = TasksSettings.load_settings(site.config)
    if not settings.enabled:
        return
    site.provide_capability(
        TasksCapability,
        ImmediateTasksCapability(
            settings,
            events=site.optional_capability(EventsCapability),
        ),
    )


__all__ = ("setup_site",)
