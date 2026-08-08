"""ASGI lifespan helpers for operational Wybra commands."""

from __future__ import annotations

from contextlib import AbstractAsyncContextManager
from typing import Any

from wybra.tools.project import ProjectToolConfigurationError


def configured_asgi_lifespan(app: Any) -> AbstractAsyncContextManager[Any]:
    """Return the configured application's required async lifespan context."""

    router = getattr(app, "router", None)
    factory = getattr(router, "lifespan_context", None)
    if not callable(factory):
        raise ProjectToolConfigurationError(
            "Configured ASGI application does not expose a lifespan context."
        )
    context = factory(app)
    if not isinstance(context, AbstractAsyncContextManager):
        raise ProjectToolConfigurationError(
            "Configured ASGI application lifespan is not an async context manager."
        )
    return context


__all__ = ("configured_asgi_lifespan",)
