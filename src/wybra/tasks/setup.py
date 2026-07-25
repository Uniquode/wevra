from __future__ import annotations

from wybra.site import Site
from wybra.tasks.capabilities import ImmediateTasksCapability, TasksCapability
from wybra.tasks.settings import TasksSettings


async def setup_site(site: Site) -> None:
    settings = TasksSettings.load_settings(site.config)
    site.provide_capability(TasksCapability, ImmediateTasksCapability(settings))


__all__ = ("setup_site",)
