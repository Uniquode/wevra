from __future__ import annotations

from typing import Protocol

from wybra.cache import CachesSettings, cache_provider_configured
from wybra.config import ConfigService
from wybra.core.exceptions import ConfigurationError
from wybra.core.runtime import DEFAULT_DEPLOYMENT_ENVIRONMENT
from wybra.sessions.config import SessionStorageBackend
from wybra.sessions.ids import create_session_id, validate_session_id
from wybra.sessions.settings import SessionsSettings
from wybra.tools.validation.core import ValidationCheck, ValidationResult, record_check


class SessionsValidationSettings(Protocol):
    config: ConfigService
    deployment_environment: str | None


def validate_sessions(settings: SessionsValidationSettings) -> ValidationResult:
    errors: list[str] = []
    checks: list[ValidationCheck] = []

    try:
        session_settings = SessionsSettings.load_settings(
            settings.config,
            deployment_environment=getattr(
                settings,
                "deployment_environment",
                DEFAULT_DEPLOYMENT_ENVIRONMENT,
            ),
        )
    except Exception as exc:
        record_check(
            checks,
            errors,
            passed=False,
            description="sessions settings load",
            error=f"Sessions settings failed to load: {exc}",
        )
        return ValidationResult(
            name="sessions",
            errors=tuple(errors),
            checks=tuple(checks),
        )

    record_check(
        checks,
        errors,
        passed=True,
        description=(
            "sessions settings load: "
            f"storage_backend={session_settings.resolved_storage_backend.value}"
        ),
    )
    record_check(
        checks,
        errors,
        passed=_session_id_policy_valid(),
        description="session identifier policy validates generated IDs",
        error="Generated session identifier did not validate.",
    )
    if session_settings.resolved_storage_backend is SessionStorageBackend.MEMORY:
        record_check(
            checks,
            errors,
            passed=session_settings.deployment_environment == "local",
            description="memory session storage is local-only",
            error="Memory session storage is only valid locally.",
        )
    if session_settings.resolved_storage_backend is SessionStorageBackend.CACHE:
        if session_settings.cache_url is not None:
            record_check(
                checks,
                errors,
                passed=True,
                description="cache session storage uses deprecated legacy cache URL",
            )
        else:
            _record_named_cache_check(
                settings.config,
                session_settings.resolved_cache_name,
                checks,
                errors,
            )
    if session_settings.resolved_storage_backend is SessionStorageBackend.FILE:
        record_check(
            checks,
            errors,
            passed=session_settings.resolved_file_directory.parent.exists(),
            description="file session storage parent directory exists",
            error=(
                "File-backed sessions require the parent directory to exist: "
                f"{session_settings.resolved_file_directory.parent}"
            ),
        )
    return ValidationResult(name="sessions", errors=tuple(errors), checks=tuple(checks))


def _session_id_policy_valid() -> bool:
    try:
        validate_session_id(create_session_id())
    except Exception:
        return False
    return True


def _record_named_cache_check(
    config: ConfigService,
    cache_name: str,
    checks: list[ValidationCheck],
    errors: list[str],
) -> None:
    description = f"cache session storage uses named cache: cache={cache_name}"
    if not cache_provider_configured(_configured_modules(config)):
        record_check(
            checks,
            errors,
            passed=False,
            description=description,
            error=(
                "Cache-backed sessions require a configured cache capability "
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
                f"Cache {cache_name!r} for consumer 'request sessions' "
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


def _configured_modules(config: ConfigService) -> tuple[str, ...]:
    app_config = config.get_config("app") or {}
    modules = app_config.get("modules", ())
    if isinstance(modules, list | tuple) and all(
        isinstance(module_name, str) for module_name in modules
    ):
        return tuple(modules)
    return ()


validation_targets = {"sessions": validate_sessions}

__all__ = ("SessionsValidationSettings", "validate_sessions", "validation_targets")
