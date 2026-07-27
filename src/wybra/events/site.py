"""Site lifecycle and capability event contracts."""

from __future__ import annotations

from dataclasses import dataclass
from inspect import BoundArguments
from typing import ClassVar

from wybra.events._core import (
    CAPABILITY,
    COMPLETED,
    EVT_SITE,
    MODULE,
    POST_SETUP,
    RESOLVED,
    SETUP,
    SETUP_FINALISATION,
    SHUTDOWN,
    STARTUP,
    UNAVAILABLE,
    Event,
    EventOutcome,
    EventSegment,
)


@dataclass(frozen=True, slots=True, kw_only=True)
class ModuleSetupEvent(Event):
    """An observation of a configured module's ``setup_site`` hook."""

    kind: ClassVar[EventSegment] = SETUP
    module: str
    outcome: str
    error_type: str | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class ModulePostSetupEvent(Event):
    """An observation of a configured module's ``post_setup_site`` hook."""

    kind: ClassVar[EventSegment] = POST_SETUP
    module: str
    outcome: str
    error_type: str | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class ModuleSetupFinalisationEvent(Event):
    """An observation of a module's deferred setup finalisation."""

    kind: ClassVar[EventSegment] = SETUP_FINALISATION
    module: str
    outcome: str
    error_type: str | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class CapabilityResolvedEvent(Event):
    """An observation of a proxy resolving a site capability."""

    kind: ClassVar[EventSegment] = RESOLVED
    capability_type: str


@dataclass(frozen=True, slots=True, kw_only=True)
class CapabilityUnavailableEvent(Event):
    """An observation of a proxy not finding a site capability."""

    kind: ClassVar[EventSegment] = UNAVAILABLE
    capability_type: str


@dataclass(frozen=True, slots=True, kw_only=True)
class CapabilityProvidedEvent(Event):
    """An observation that a capability was registered with a site."""

    kind: ClassVar[EventSegment] = COMPLETED
    capability_type: str


@dataclass(frozen=True, slots=True, kw_only=True)
class SiteLifecycleEvent(Event):
    """A site startup or shutdown outcome."""

    kind: ClassVar[EventSegment] = COMPLETED
    phase: str
    error_count: int = 0


def capability_provided_event(
    call: BoundArguments,
    outcome: EventOutcome | None,
) -> Event | None:
    """Build a capability-registration observation."""

    if outcome is None:
        return None
    capability_type = call.arguments["capability_type"]
    if not isinstance(capability_type, str):
        raise TypeError("Capability events require a capability type name.")
    return CapabilityProvidedEvent(
        topic=EVT_SITE(CAPABILITY, COMPLETED),
        capability_type=capability_type,
    )


def capability_resolution_event(
    call: BoundArguments,
    outcome: EventOutcome | None,
) -> Event | None:
    """Build an available or unavailable capability-resolution observation."""

    if outcome is None:
        return None
    capability_type = call.arguments["capability_type"]
    available = call.arguments["available"]
    if not isinstance(capability_type, str) or not isinstance(available, bool):
        raise TypeError("Capability resolution events require safe metadata.")
    if available:
        return CapabilityResolvedEvent(
            topic=EVT_SITE(CAPABILITY, RESOLVED),
            capability_type=capability_type,
        )
    return CapabilityUnavailableEvent(
        topic=EVT_SITE(CAPABILITY, UNAVAILABLE),
        capability_type=capability_type,
    )


def site_lifecycle_event(
    call: BoundArguments,
    outcome: EventOutcome | None,
) -> Event | None:
    """Build a site startup or shutdown observation."""

    if outcome is None:
        return None
    phase = call.arguments["phase"]
    error_count = call.arguments["error_count"]
    if not isinstance(phase, str) or not isinstance(error_count, int):
        raise TypeError("Site lifecycle events require safe lifecycle metadata.")
    topic = EVT_SITE(STARTUP if phase == "startup" else SHUTDOWN)
    return SiteLifecycleEvent(topic=topic, phase=phase, error_count=error_count)


def module_hook_event(
    call: BoundArguments,
    outcome: EventOutcome | None,
) -> Event:
    """Build a module hook start or terminal observation."""

    module_name = call.arguments["module_name"]
    attribute = call.arguments["attribute"]
    if not isinstance(module_name, str) or not isinstance(attribute, str):
        raise TypeError("Module hook events require a module and hook name.")
    event_type, topic_segment = {
        "setup_site": (ModuleSetupEvent, SETUP),
        "setup_finalisation": (ModuleSetupFinalisationEvent, SETUP_FINALISATION),
        "post_setup_site": (ModulePostSetupEvent, POST_SETUP),
    }.get(attribute, (ModulePostSetupEvent, POST_SETUP))
    if outcome is None:
        return event_type(
            topic=EVT_SITE(MODULE, topic_segment),
            module=module_name,
            outcome="started",
        )
    return event_type(
        topic=EVT_SITE(MODULE, topic_segment),
        module=module_name,
        outcome="succeeded" if outcome.succeeded else "failed",
        error_type=outcome.error_type,
    )


__all__ = (
    "ModulePostSetupEvent",
    "ModuleSetupEvent",
    "ModuleSetupFinalisationEvent",
    "capability_provided_event",
    "capability_resolution_event",
    "module_hook_event",
    "site_lifecycle_event",
)
