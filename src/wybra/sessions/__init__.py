"""Core Wybra request sessions."""

from __future__ import annotations

from importlib import import_module
from typing import Any

_EXPORT_MODULES = {
    "SessionCleanupRegistry": "wybra.sessions.cleanup",
    "CookieSessionStorage": "wybra.sessions.storage",
    "DatabaseSessionStorage": "wybra.sessions.storage",
    "FileSessionStorage": "wybra.sessions.storage",
    "MemorySessionStorage": "wybra.sessions.storage",
    "NamedCacheSessionStorage": "wybra.sessions.storage",
    "RequestSession": "wybra.sessions.state",
    "SessionIdentifierError": "wybra.sessions.exceptions",
    "SessionMiddlewareContext": "wybra.sessions.middleware",
    "SessionRecord": "wybra.sessions.storage",
    "SessionRecordModel": "wybra.sessions.models",
    "SessionStorage": "wybra.sessions.storage",
    "SessionStorageBackend": "wybra.sessions.config",
    "SessionStorageError": "wybra.sessions.exceptions",
    "SessionUnavailableError": "wybra.sessions.exceptions",
    "SessionsConfigurationError": "wybra.sessions.exceptions",
    "SessionsError": "wybra.sessions.exceptions",
    "SessionsSettings": "wybra.sessions.settings",
    "create_session_id": "wybra.sessions.ids",
    "module_config": "wybra.sessions.config",
    "request_session_from_scope": "wybra.sessions.state",
    "setup_core_sessions": "wybra.sessions.setup",
    "session_id_is_valid": "wybra.sessions.ids",
    "session_cleanup_registry_from_site": "wybra.sessions.cleanup",
    "validate_session_id": "wybra.sessions.ids",
    "validate_sessions": "wybra.sessions.validation",
    "validation_targets": "wybra.sessions.validation",
}


def __getattr__(name: str) -> Any:
    module_name = _EXPORT_MODULES.get(name)
    if module_name is None:
        raise AttributeError(f"module 'wybra.sessions' has no attribute {name!r}")
    value = getattr(import_module(module_name), name)
    globals()[name] = value
    return value


__all__ = sorted(_EXPORT_MODULES)
