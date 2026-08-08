"""Operational validation for declared scope catalogue entries."""

from __future__ import annotations

import asyncio
from typing import Protocol

from wybra.core.composition import AppConfig
from wybra.core.exceptions import ConfigurationError
from wybra.scopes import ScopeDeclarationError, validate_site_scope_catalogue
from wybra.site import SiteCapabilityError, get_site
from wybra.tools.lifespan import configured_asgi_lifespan
from wybra.tools.project import ProjectToolConfigurationError, import_from_string
from wybra.tools.validation.core import ValidationCheck, ValidationResult


class ScopeValidationSettings(Protocol):
    app_config: AppConfig


def validate_scope_catalogue(
    settings: ScopeValidationSettings,
) -> ValidationResult:
    """Validate finalised declarations against the persisted scope catalogue."""

    try:
        missing = asyncio.run(_validate_configured_scope_catalogue(settings))
    except (
        ConfigurationError,
        ProjectToolConfigurationError,
        ScopeDeclarationError,
        SiteCapabilityError,
    ) as exc:
        return ValidationResult(
            name="scopes",
            errors=(f"Scope catalogue validation failed: {exc}",),
            checks=(
                ValidationCheck(
                    description="configured site scope catalogue can be inspected",
                    passed=False,
                ),
            ),
        )

    complete = not missing
    return ValidationResult(
        name="scopes",
        errors=tuple(
            f"Declared scope is missing from the persisted catalogue: {identifier}."
            for identifier in missing
        ),
        checks=(
            ValidationCheck(
                description="configured site scope catalogue can be inspected",
                passed=True,
            ),
            ValidationCheck(
                description="declared scopes exist in the persisted catalogue",
                passed=complete,
            ),
        ),
    )


async def _validate_configured_scope_catalogue(
    settings: ScopeValidationSettings,
) -> tuple[str, ...]:
    app_target = settings.app_config.runserver.asgi_app
    if app_target is None or not app_target.strip():
        raise ProjectToolConfigurationError(
            "[app.runserver].asgi_app must be configured for scope validation."
        )

    app = import_from_string(app_target.strip())
    lifespan_context = configured_asgi_lifespan(app)
    async with lifespan_context:
        return await validate_site_scope_catalogue(get_site(app))


__all__ = (
    "ScopeValidationSettings",
    "validate_scope_catalogue",
)
