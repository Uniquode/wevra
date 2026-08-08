from __future__ import annotations

import os
from collections.abc import Mapping, MutableMapping
from pathlib import Path

from envex import Env

from wybra.core.exceptions import ConfigurationError


def load_environment(
    *,
    environ: Mapping[str, str] | None = None,
    project_root: Path | None = None,
    read_dotenv: bool = True,
) -> Env:
    """Load process and local dotenv configuration through envex."""
    if environ is None:
        configured_environment: MutableMapping[str, str] = os.environ
    elif isinstance(environ, MutableMapping):
        configured_environment = environ
    else:
        configured_environment = dict(environ)

    base_environment = (
        dict(configured_environment) if read_dotenv else configured_environment
    )
    try:
        return Env(
            environ=base_environment,
            readenv=read_dotenv,
            search_path=project_root or Path.cwd(),
            update=False,
        )
    except Exception as exc:
        raise ConfigurationError(
            "Environment loader failed while initialising envex "
            f"({type(exc).__name__})."
        ) from exc


def runtime_environment() -> Env:
    return load_environment()


def environment_get(environ: object | None, name: str) -> str | None:
    if environ is None:
        return None
    getter = getattr(environ, "get", None)
    if not callable(getter):
        return None
    value = getter(name)
    return value if isinstance(value, str) else None


def environment_is_set(environ: object | None, name: str) -> bool:
    if environ is None:
        return False
    is_set = getattr(environ, "is_set", None)
    if callable(is_set):
        return bool(is_set(name))
    return isinstance(environ, Mapping) and name in environ


__all__ = (
    "environment_get",
    "environment_is_set",
    "load_environment",
    "runtime_environment",
)
