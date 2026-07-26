from __future__ import annotations

from typing import Protocol

from wybra.cache import CachesSettings, cache_provider_configured
from wybra.config import ConfigService
from wybra.core.exceptions import ConfigurationError
from wybra.core.resources import PackageResourceSource, first_existing_resource
from wybra.messages.config import MessageStorageBackend
from wybra.messages.settings import MessagesSettings
from wybra.template.discovery import context_providers_from_modules
from wybra.tools.validation.core import ValidationCheck, ValidationResult, record_check

ALERT_COMPONENT_TEMPLATE = "components/alerts.html"
ALERT_STYLESHEET = "styles/messages.css"


class MessagesValidationSettings(Protocol):
    config: ConfigService

    @property
    def modules(self) -> tuple[str, ...]: ...


def validate_alerts(settings: MessagesValidationSettings) -> ValidationResult:
    errors: list[str] = []
    checks: list[ValidationCheck] = []

    if "wybra.messages" not in settings.modules:
        record_check(
            checks,
            errors,
            passed=True,
            description="wybra.messages is not configured",
        )
        return ValidationResult(
            name="alerts",
            errors=tuple(errors),
            checks=tuple(checks),
        )

    _record_settings_check(settings, checks, errors)
    _record_context_provider_check(settings, checks, errors)
    _record_resource_checks(checks, errors)
    return ValidationResult(name="alerts", errors=tuple(errors), checks=tuple(checks))


def _record_settings_check(
    settings: MessagesValidationSettings,
    checks: list[ValidationCheck],
    errors: list[str],
) -> None:
    config = getattr(settings, "config", None)
    if config is None:
        record_check(
            checks,
            errors,
            passed=False,
            description="messages settings load",
            error="Messages validation requires project settings with config.",
        )
        return
    try:
        messages_settings = MessagesSettings.load_settings(config)
    except Exception as exc:
        record_check(
            checks,
            errors,
            passed=False,
            description="messages settings load",
            error=f"Messages settings validation failed: {exc}",
        )
        return

    record_check(
        checks,
        errors,
        passed=True,
        description=(
            "messages settings load: "
            f"storage_backend={messages_settings.resolved_storage_backend.value}"
        ),
    )
    if messages_settings.resolved_storage_backend is MessageStorageBackend.DATABASE:
        record_check(
            checks,
            errors,
            passed="wybra.db" in settings.modules,
            description="database messages storage has database module",
            error="Database-backed messages require wybra.db in configured modules.",
        )
    if messages_settings.resolved_storage_backend is MessageStorageBackend.CACHE:
        if messages_settings.cache_url is not None:
            record_check(
                checks,
                errors,
                passed=True,
                description="cache messages storage uses deprecated legacy cache URL",
            )
        else:
            _record_named_cache_check(
                config,
                settings.modules,
                messages_settings.resolved_cache_name,
                checks,
                errors,
            )


def _record_named_cache_check(
    config: ConfigService,
    modules: tuple[str, ...],
    cache_name: str,
    checks: list[ValidationCheck],
    errors: list[str],
) -> None:
    description = (
        "cache messages storage selects named cache: "
        f"cache={cache_name}, required_feature=AtomicCacheCapability "
        "(verified at startup)"
    )
    if not cache_provider_configured(modules):
        record_check(
            checks,
            errors,
            passed=False,
            description=description,
            error=(
                "Cache-backed messages require a configured cache capability "
                "such as wybra.cache."
            ),
        )
        return
    try:
        CachesSettings.load_settings(config).require(cache_name)
    except ConfigurationError as exc:
        record_check(
            checks,
            errors,
            passed=False,
            description=description,
            error=(
                f"Cache {cache_name!r} for consumer 'queued request messages' "
                f"is invalid: {exc}"
            ),
        )
        return
    record_check(
        checks,
        errors,
        passed=True,
        description=description,
    )


def _record_context_provider_check(
    settings: MessagesValidationSettings,
    checks: list[ValidationCheck],
    errors: list[str],
) -> None:
    try:
        providers = context_providers_from_modules(settings.modules)
    except Exception as exc:
        record_check(
            checks,
            errors,
            passed=False,
            description="messages context provider discovery",
            error=f"Messages context provider validation failed: {exc}",
        )
        return

    record_check(
        checks,
        errors,
        passed=any(
            provider.__module__ == "wybra.messages.context" for provider in providers
        ),
        description="messages context provider is registered",
        error="Messages context provider is not registered.",
    )


def _record_resource_checks(
    checks: list[ValidationCheck],
    errors: list[str],
) -> None:
    template_source = PackageResourceSource(
        package="wybra.messages",
        directory="templates",
    )
    static_source = PackageResourceSource(package="wybra.messages", directory="static")
    record_check(
        checks,
        errors,
        passed=first_existing_resource((template_source,), ALERT_COMPONENT_TEMPLATE)
        is not None,
        description=f"alert template exists: {ALERT_COMPONENT_TEMPLATE}",
        error=f"Missing alert template: {ALERT_COMPONENT_TEMPLATE}",
    )
    record_check(
        checks,
        errors,
        passed=first_existing_resource((static_source,), ALERT_STYLESHEET) is not None,
        description=f"alert stylesheet exists: {ALERT_STYLESHEET}",
        error=f"Missing alert stylesheet: {ALERT_STYLESHEET}",
    )


validation_targets = {"alerts": validate_alerts}

__all__ = (
    "ALERT_COMPONENT_TEMPLATE",
    "ALERT_STYLESHEET",
    "MessagesValidationSettings",
    "validate_alerts",
    "validation_targets",
)
