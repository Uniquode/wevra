from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType
from urllib.parse import quote, urlsplit, urlunsplit

from wybra.cache.settings import CacheSettings, CachesSettings
from wybra.core.exceptions import ConfigurationError
from wybra.services.secrets import SecretsCapability, SecretsError


def resolve_redis_urls(
    settings: CachesSettings,
    secrets: SecretsCapability | None,
) -> Mapping[str, str]:
    resolved_urls: dict[str, str] = {}
    for instance in settings.instances:
        if instance.backend != "redis" or not instance.requires_secret_resolution:
            continue
        resolved_urls[instance.name] = _resolve_redis_url(instance, secrets)
    return MappingProxyType(resolved_urls)


def _resolve_redis_url(
    settings: CacheSettings,
    secrets: SecretsCapability | None,
) -> str:
    url_reference = settings.url_reference
    credentials_reference = settings.credentials_reference
    if url_reference is not None:
        return _resolve_reference(
            settings,
            secrets,
            reference=url_reference,
            label="URL",
        )
    if settings.url is None:
        raise ConfigurationError(
            f"Cache {settings.name!r} Redis URL is not configured."
        )
    if credentials_reference is None:
        return settings.url
    credentials = _resolve_reference(
        settings,
        secrets,
        reference=credentials_reference,
        label="credentials",
    )
    return _with_credentials(settings, settings.url, credentials)


def _resolve_reference(
    settings: CacheSettings,
    secrets: SecretsCapability | None,
    *,
    reference: tuple[str, str],
    label: str,
) -> str:
    source, key = reference
    if secrets is None:
        raise ConfigurationError(
            f"Cache {settings.name!r} Redis {label} reference requires the "
            "wybra.secrets module."
        )
    try:
        value = secrets.resolve(source, key).reveal()
    except SecretsError:
        raise ConfigurationError(
            f"Cache {settings.name!r} Redis {label} secret resolution failed "
            f"for source {source!r}."
        ) from None
    if not value.strip():
        raise ConfigurationError(
            f"Cache {settings.name!r} Redis {label} secret is blank."
        )
    return value


def _with_credentials(
    settings: CacheSettings,
    endpoint: str,
    credentials: str,
) -> str:
    try:
        parsed = urlsplit(endpoint)
    except ValueError:
        raise ConfigurationError(
            f"Cache {settings.name!r} Redis endpoint is invalid."
        ) from None
    if parsed.scheme.lower() not in {"redis", "rediss"} or not parsed.netloc:
        raise ConfigurationError(
            f"Cache {settings.name!r} Redis endpoint cannot accept credentials."
        )
    if parsed.username is not None or parsed.password is not None:
        raise ConfigurationError(
            f"Cache {settings.name!r} Redis endpoint already contains credentials."
        )
    username, separator, password = credentials.partition(":")
    if not separator or (not username and not password):
        raise ConfigurationError(
            f"Cache {settings.name!r} Redis credentials must use "
            "username:password form."
        )
    userinfo = f"{quote(username, safe='')}:{quote(password, safe='')}"
    return urlunsplit(
        (
            parsed.scheme,
            f"{userinfo}@{parsed.netloc}",
            parsed.path,
            parsed.query,
            parsed.fragment,
        )
    )


__all__ = ("resolve_redis_urls",)
