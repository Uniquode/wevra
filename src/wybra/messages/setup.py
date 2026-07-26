from __future__ import annotations

import time
from collections.abc import Awaitable, Callable

from fastapi import Request
from fastapi.responses import Response

from wybra.messages.capabilities import DefaultMessagesCapability, MessagesCapability
from wybra.messages.settings import MessagesSettings
from wybra.messages.storage import (
    REQUEST_ALERTS_RENDERED_ATTRIBUTE,
    storage_from_settings,
)
from wybra.sessions.cleanup import session_cleanup_registry_from_site
from wybra.site import Site

MESSAGES_MIDDLEWARE_STATE_ATTRIBUTE = "wybra_messages_middleware_registered"
MESSAGES_CLEANUP_INTERVAL_SECONDS = 60 * 60


async def setup_site(site: Site) -> None:
    settings = MessagesSettings.load_settings(site.config)
    site.app.state.messages_settings = settings
    register_messages_middleware(site)


async def post_setup_site(site: Site) -> None:
    settings = site.app.state.messages_settings
    capability = DefaultMessagesCapability(
        settings=settings,
        storage=storage_from_settings(site, settings),
    )
    site.provide_capability(MessagesCapability, capability)
    session_cleanup_registry_from_site(site).register(capability.cleanup_session_data)
    await capability.validate()


def register_messages_middleware(site: Site) -> None:
    if getattr(site.app.state, MESSAGES_MIDDLEWARE_STATE_ATTRIBUTE, False):
        return
    last_cleanup_at: float | None = None

    @site.app.middleware("http")
    async def wybra_messages_middleware(
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        nonlocal last_cleanup_at
        now = time.time()
        if (
            last_cleanup_at is None
            or now - last_cleanup_at >= MESSAGES_CLEANUP_INTERVAL_SECONDS
        ):
            capability = site.optional_capability(MessagesCapability)
            if capability is not None:
                last_cleanup_at = now
                await capability.cleanup_expired(now=now)
        response = await call_next(request)
        if getattr(request.state, REQUEST_ALERTS_RENDERED_ATTRIBUTE, False):
            capability = site.optional_capability(MessagesCapability)
            if capability is not None:
                await capability.acknowledge_alerts(request)
        return response

    setattr(site.app.state, MESSAGES_MIDDLEWARE_STATE_ATTRIBUTE, True)


__all__ = (
    "MESSAGES_CLEANUP_INTERVAL_SECONDS",
    "MESSAGES_MIDDLEWARE_STATE_ATTRIBUTE",
    "post_setup_site",
    "register_messages_middleware",
    "setup_site",
)
