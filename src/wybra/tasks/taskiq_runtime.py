from __future__ import annotations

import importlib
from types import ModuleType

from wybra.core.exceptions import ConfigurationError


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


__all__ = ("load_taskiq",)
