from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from hashlib import sha256
from typing import Any, ClassVar, Self
from urllib.parse import parse_qs, unquote, urlsplit

from wybra.cache.config import (
    CACHE_CONFIG_SECTION,
    CACHE_INSTANCE_CONFIG,
    DEFAULT_CACHE_BACKEND,
    DEFAULT_CACHE_NAME,
    module_config,
    to_cache_backend,
    to_cache_features,
    to_cache_name,
    to_cache_namespace,
    to_cache_servers,
    to_optional_secret_source,
)
from wybra.cache.memory_features import MEMORY_CACHE_FEATURES
from wybra.cache.redis_features import REDIS_CACHE_FEATURES
from wybra.config import BaseSettings, ConfigDef, ConfigService, CredentialReference
from wybra.config.transforms import to_optional_non_blank_string
from wybra.core.exceptions import ConfigurationError
from wybra.services.secrets import (
    ENVIRONMENT_SOURCE,
    KEYCHAIN_SOURCE,
    SecretsError,
    SecretSource,
    secret_key_value,
)

CACHE_BACKEND_FEATURES = {
    "memory": MEMORY_CACHE_FEATURES,
    "nats-jetstream": frozenset(),
    "redis": REDIS_CACHE_FEATURES,
}


@dataclass(frozen=True, slots=True)
class CacheSettings(BaseSettings):
    module_config: ClassVar[ConfigDef] = module_config
    config_section: ClassVar[str | None] = CACHE_CONFIG_SECTION

    backend: str = DEFAULT_CACHE_BACKEND
    servers: tuple[str, ...] | None = field(default=None, repr=False)
    url: str | None = field(default=None, repr=False)
    url_source: SecretSource | None = None
    url_key: str | None = None
    credentials_source: SecretSource | None = None
    credentials_key: str | None = None
    name: str = DEFAULT_CACHE_NAME
    namespace: str | None = None
    features: tuple[str, ...] | None = None

    @classmethod
    def load_settings(cls, config: ConfigService | Mapping[str, Any]) -> Self:
        values = cls.settings_kwargs(config)
        return cls(**values)

    def __post_init__(self) -> None:
        name = _cache_name(self.name)
        try:
            backend = to_cache_backend(self.backend)
        except ValueError as exc:
            raise ConfigurationError(f"{_section_name(name)}.backend: {exc}") from exc
        try:
            url = to_optional_non_blank_string(self.url)
        except (TypeError, ValueError) as exc:
            raise ConfigurationError(f"{_section_name(name)}.url: {exc}") from exc
        try:
            servers = to_cache_servers(self.servers)
        except (TypeError, ValueError) as exc:
            raise ConfigurationError(f"{_section_name(name)}.servers: {exc}") from exc
        url_source = _secret_source(self.url_source, name=name, field_name="url_source")
        url_key = _secret_key(self.url_key, name=name, field_name="url_key")
        credentials_source = _secret_source(
            self.credentials_source,
            name=name,
            field_name="credentials_source",
        )
        credentials_key = _secret_key(
            self.credentials_key,
            name=name,
            field_name="credentials_key",
        )
        _validate_connection_settings(
            name=name,
            backend=backend,
            servers=servers,
            url=url,
            url_source=url_source,
            url_key=url_key,
            credentials_source=credentials_source,
            credentials_key=credentials_key,
        )
        try:
            namespace = to_cache_namespace(self.namespace)
        except (TypeError, ValueError) as exc:
            raise ConfigurationError(f"{_section_name(name)}.namespace: {exc}") from exc
        if backend == "memory" and namespace is not None:
            raise ConfigurationError(
                f"{_section_name(name)}.namespace is not valid when backend is "
                "'memory'."
            )
        try:
            features = to_cache_features(self.features)
        except (TypeError, ValueError) as exc:
            raise ConfigurationError(f"{_section_name(name)}.features: {exc}") from exc
        if features is not None:
            unavailable = sorted(set(features) - CACHE_BACKEND_FEATURES[backend])
            if unavailable:
                raise ConfigurationError(
                    f"{_section_name(name)}.features contains feature(s) not "
                    f"implemented by backend {backend!r}: "
                    + ", ".join(unavailable)
                    + "."
                )
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "backend", backend)
        object.__setattr__(self, "servers", servers)
        object.__setattr__(self, "url", url)
        object.__setattr__(self, "url_source", url_source)
        object.__setattr__(self, "url_key", url_key)
        object.__setattr__(self, "credentials_source", credentials_source)
        object.__setattr__(self, "credentials_key", credentials_key)
        object.__setattr__(self, "namespace", namespace)
        object.__setattr__(self, "features", features)

    @property
    def partition(self) -> str:
        if self.backend == "memory":
            return f"process:{self.name}"
        if self.backend == "nats-jetstream":
            if not self.servers:
                raise ConfigurationError(
                    f"{_section_name(self.name)}.servers is required when backend is "
                    "'nats-jetstream'."
                )
            return _nats_jetstream_partition(self.servers, self.resolved_namespace)
        url_reference = self.url_reference
        if self.url is None and url_reference is None:
            raise ConfigurationError(
                f"{_section_name(self.name)}.url is required when backend is 'redis'."
            )
        if self.url is not None:
            return _redis_partition(self.url, self.resolved_namespace)
        assert url_reference is not None
        source, key = url_reference
        return _secret_redis_partition(source, key, self.resolved_namespace)

    @property
    def resolved_namespace(self) -> str:
        return self.namespace or self.name

    @property
    def url_reference(self) -> tuple[SecretSource, str] | None:
        if self.url_source is None:
            return None
        return (
            self.url_source,
            self.url_key or _default_url_key(self.name, self.url_source),
        )

    @property
    def credentials_reference(self) -> tuple[SecretSource, str] | None:
        if self.credentials_source is None:
            return None
        return (
            self.credentials_source,
            self.credentials_key or _default_credentials_key(self.credentials_source),
        )

    @property
    def requires_secret_resolution(self) -> bool:
        return self.url_reference is not None or self.credentials_reference is not None

    def credential_references(self) -> tuple[CredentialReference, ...]:
        references: list[CredentialReference] = []
        url_reference = self.url_reference
        if url_reference is not None and url_reference[0] == KEYCHAIN_SOURCE:
            references.append(
                CredentialReference(
                    name=_credential_reference_name(self.name, "url"),
                    key=url_reference[1],
                    owner="cache",
                    description=(f"Configured Redis URL for cache {self.name!r}."),
                    source=KEYCHAIN_SOURCE,
                    required=True,
                )
            )
        credentials_reference = self.credentials_reference
        if (
            credentials_reference is not None
            and credentials_reference[0] == KEYCHAIN_SOURCE
        ):
            references.append(
                CredentialReference(
                    name=_credential_reference_name(self.name, "credentials"),
                    key=credentials_reference[1],
                    owner="cache",
                    description=(
                        f"Configured Redis credentials for cache {self.name!r}."
                    ),
                    source=KEYCHAIN_SOURCE,
                    required=True,
                )
            )
        return tuple(references)


@dataclass(frozen=True, slots=True)
class CacheConfigurationDiagnostic:
    name: str
    backend: str
    partition: str
    features: tuple[str, ...] = ()
    health: str = "configured"


@dataclass(frozen=True, slots=True)
class CachesSettings(BaseSettings):
    module_config: ClassVar[ConfigDef] = module_config
    config_section: ClassVar[str | None] = CACHE_CONFIG_SECTION

    instances: tuple[CacheSettings, ...]

    def __post_init__(self) -> None:
        names = tuple(settings.name for settings in self.instances)
        duplicates = sorted({name for name in names if names.count(name) > 1})
        if duplicates:
            raise ConfigurationError(
                "Configured cache names contain duplicates: "
                + ", ".join(duplicates)
                + "."
            )
        if DEFAULT_CACHE_NAME not in names:
            raise ConfigurationError("Configured caches must include 'default'.")

    @classmethod
    def load_settings(
        cls,
        config: ConfigService | Mapping[str, Any],
    ) -> Self:
        sections = _cache_sections(config)
        instances = [
            _load_cache_settings(
                DEFAULT_CACHE_NAME,
                sections.get(CACHE_CONFIG_SECTION, {}),
            )
        ]
        for section_name in sorted(sections):
            if not section_name.startswith(f"{CACHE_CONFIG_SECTION}."):
                continue
            name = section_name.removeprefix(f"{CACHE_CONFIG_SECTION}.")
            instances.append(
                _load_cache_settings(
                    name,
                    sections[section_name],
                    nested=True,
                )
            )
        return cls(instances=tuple(instances))

    def require(self, name: str) -> CacheSettings:
        requested = _cache_name(name)
        for settings in self.instances:
            if settings.name == requested:
                return settings
        raise ConfigurationError(f"Configured cache {requested!r} was not found.")

    def diagnostics(self) -> tuple[CacheConfigurationDiagnostic, ...]:
        return tuple(
            CacheConfigurationDiagnostic(
                name=settings.name,
                backend=settings.backend,
                partition=settings.partition,
                features=(
                    tuple(sorted(CACHE_BACKEND_FEATURES[settings.backend]))
                    if settings.features is None
                    else tuple(sorted(settings.features))
                ),
            )
            for settings in self.instances
        )

    def credential_references(self) -> tuple[CredentialReference, ...]:
        references: dict[tuple[SecretSource, str], CredentialReference] = {}
        for settings in self.instances:
            for reference in settings.credential_references():
                references.setdefault((reference.source, reference.key), reference)
        return tuple(references.values())


def _cache_sections(
    config: ConfigService | Mapping[str, Any],
) -> Mapping[str, Mapping[str, Any]]:
    source = config.config.values if isinstance(config, ConfigService) else config
    sections: dict[str, Mapping[str, Any]] = {}
    for section_name, values in source.items():
        if section_name != CACHE_CONFIG_SECTION and not section_name.startswith(
            f"{CACHE_CONFIG_SECTION}."
        ):
            continue
        if not isinstance(values, Mapping):
            raise ConfigurationError(
                f"Config section {section_name!r} must be a mapping."
            )
        sections[section_name] = values
    return sections


def _load_cache_settings(
    name: str,
    configured: Mapping[str, Any],
    *,
    nested: bool = False,
) -> CacheSettings:
    name = _cache_name(name, nested=nested)
    section_name = _section_name(name)
    unknown = set(configured) - CACHE_INSTANCE_CONFIG.field_names
    if unknown:
        raise ConfigurationError(
            f"{section_name} contains unsupported field(s): "
            + ", ".join(sorted(unknown))
        )
    values = dict(configured)
    backend_value = values.get("backend", DEFAULT_CACHE_BACKEND)
    try:
        backend = to_cache_backend(backend_value)
    except (TypeError, ValueError) as exc:
        raise ConfigurationError(f"{section_name}.backend: {exc}") from exc
    try:
        url = to_optional_non_blank_string(values.get("url"))
    except (TypeError, ValueError) as exc:
        raise ConfigurationError(f"{section_name}.url: {exc}") from exc
    try:
        servers = to_cache_servers(values.get("servers"))
    except (TypeError, ValueError) as exc:
        raise ConfigurationError(f"{section_name}.servers: {exc}") from exc
    try:
        url_source = to_optional_secret_source(values.get("url_source"))
    except (TypeError, ValueError) as exc:
        raise ConfigurationError(f"{section_name}.url_source: {exc}") from exc
    try:
        url_key = to_optional_non_blank_string(values.get("url_key"))
    except (TypeError, ValueError) as exc:
        raise ConfigurationError(f"{section_name}.url_key: {exc}") from exc
    try:
        credentials_source = to_optional_secret_source(values.get("credentials_source"))
    except (TypeError, ValueError) as exc:
        raise ConfigurationError(f"{section_name}.credentials_source: {exc}") from exc
    try:
        credentials_key = to_optional_non_blank_string(values.get("credentials_key"))
    except (TypeError, ValueError) as exc:
        raise ConfigurationError(f"{section_name}.credentials_key: {exc}") from exc
    try:
        namespace = to_cache_namespace(values.get("namespace"))
    except (TypeError, ValueError) as exc:
        raise ConfigurationError(f"{section_name}.namespace: {exc}") from exc
    try:
        features = to_cache_features(values.get("features"))
    except (TypeError, ValueError) as exc:
        raise ConfigurationError(f"{section_name}.features: {exc}") from exc
    return CacheSettings(
        name=name,
        backend=backend,
        servers=servers,
        url=url,
        url_source=url_source,
        url_key=url_key,
        credentials_source=credentials_source,
        credentials_key=credentials_key,
        namespace=namespace,
        features=features,
    )


def _cache_name(value: object, *, nested: bool = False) -> str:
    try:
        return to_cache_name(value, nested=nested)
    except (TypeError, ValueError) as exc:
        raise ConfigurationError(str(exc)) from exc


def _section_name(name: str) -> str:
    if name == DEFAULT_CACHE_NAME:
        return CACHE_CONFIG_SECTION
    return f"{CACHE_CONFIG_SECTION}.{name}"


def _redis_partition(url: str, namespace: str) -> str:
    try:
        parsed = urlsplit(url)
        query = parse_qs(parsed.query)
        query_database = query.get("db", (None,))[0]
        database = _redis_database(query_database, parsed.path, parsed.scheme)
        if parsed.scheme.lower() == "unix":
            endpoint = f"unix:{unquote(parsed.path)}"
        else:
            port = parsed.port or 6379
            endpoint = (
                f"{parsed.scheme.lower()}:{parsed.hostname or 'localhost'}:{port}"
            )
        target = f"{endpoint}:{database}:{namespace}"
    except ValueError:
        target = f"redis:configured:{namespace}"
    fingerprint = sha256(target.encode("utf-8")).hexdigest()[:12]
    return f"redis:{fingerprint}"


def _secret_redis_partition(source: SecretSource, key: str, namespace: str) -> str:
    target = f"secret:{source}:{key}:{namespace}"
    fingerprint = sha256(target.encode("utf-8")).hexdigest()[:12]
    return f"redis:{fingerprint}"


def _nats_jetstream_partition(servers: tuple[str, ...], namespace: str) -> str:
    targets = tuple(sorted(_nats_server_target(server) for server in servers))
    fingerprint = sha256(f"{targets}:{namespace}".encode()).hexdigest()[:12]
    return f"nats-jetstream:{fingerprint}"


def _nats_server_target(server: str) -> str:
    try:
        parsed = urlsplit(server)
        port = parsed.port or 4222
        return f"{parsed.scheme.lower()}:{parsed.hostname or 'localhost'}:{port}"
    except ValueError:
        return "configured"


def _secret_source(
    value: SecretSource | str | None,
    *,
    name: str,
    field_name: str,
) -> SecretSource | None:
    if value is None:
        return None
    try:
        return to_optional_secret_source(value)
    except (SecretsError, TypeError, ValueError) as exc:
        raise ConfigurationError(f"{_section_name(name)}.{field_name}: {exc}") from exc


def _secret_key(
    value: str | None,
    *,
    name: str,
    field_name: str,
) -> str | None:
    if value is None:
        return None
    try:
        return secret_key_value(value, name=f"cache {field_name}")
    except (SecretsError, ValueError) as exc:
        raise ConfigurationError(f"{_section_name(name)}.{field_name}: {exc}") from exc


def _validate_connection_settings(
    *,
    name: str,
    backend: str,
    servers: tuple[str, ...] | None,
    url: str | None,
    url_source: SecretSource | None,
    url_key: str | None,
    credentials_source: SecretSource | None,
    credentials_key: str | None,
) -> None:
    section_name = _section_name(name)
    secret_values = (url_source, url_key, credentials_source, credentials_key)
    if backend == "memory":
        if servers is not None:
            raise ConfigurationError(
                f"{section_name}.servers is not valid when backend is 'memory'."
            )
        if url is not None:
            raise ConfigurationError(
                f"{section_name}.url is not valid when backend is 'memory'."
            )
        for field_name, value in zip(
            ("url_source", "url_key", "credentials_source", "credentials_key"),
            secret_values,
            strict=True,
        ):
            if value is not None:
                raise ConfigurationError(
                    f"{section_name}.{field_name} is not valid when backend is "
                    "'memory'."
                )
        return
    if backend == "nats-jetstream":
        if servers is None:
            raise ConfigurationError(
                f"{section_name}.servers is required when backend is 'nats-jetstream'."
            )
        if not servers:
            raise ConfigurationError(
                f"{section_name}.servers must contain at least one NATS server."
            )
        for field_name, value in zip(
            ("url", "url_source", "url_key", "credentials_source", "credentials_key"),
            (url, *secret_values),
            strict=True,
        ):
            if value is not None:
                raise ConfigurationError(
                    f"{section_name}.{field_name} is not valid when backend is "
                    "'nats-jetstream'."
                )
        return
    if servers is not None:
        raise ConfigurationError(
            f"{section_name}.servers is not valid when backend is 'redis'."
        )
    for source, field_name in (
        (url_source, "url_source"),
        (credentials_source, "credentials_source"),
    ):
        if source is not None and source not in {ENVIRONMENT_SOURCE, KEYCHAIN_SOURCE}:
            raise ConfigurationError(
                f"{section_name}.{field_name} must be 'environment' or 'keychain'."
            )
    if url_key is not None and url_source is None:
        raise ConfigurationError(
            f"{section_name}.url_key requires {section_name}.url_source."
        )
    if credentials_key is not None and credentials_source is None:
        raise ConfigurationError(
            f"{section_name}.credentials_key requires "
            f"{section_name}.credentials_source."
        )
    if url_source is None and url is None:
        raise ConfigurationError(
            f"{section_name}.url or {section_name}.url_source is required when "
            "backend is 'redis'."
        )
    if url_source is not None and url is not None:
        raise ConfigurationError(
            f"{section_name}.url cannot be configured with {section_name}.url_source."
        )
    if url_source == ENVIRONMENT_SOURCE and url_key is None:
        raise ConfigurationError(
            f"{section_name}.url_source='environment' requires {section_name}.url_key. "
            "Use the native WYBRA_CACHE URL override for the configured cache, "
            "or select a distinct environment variable key."
        )
    if url_source is not None and credentials_source is not None:
        raise ConfigurationError(
            f"{section_name}.url_source cannot be configured with "
            f"{section_name}.credentials_source."
        )
    if credentials_source is not None:
        if url is None:
            raise ConfigurationError(
                f"{section_name}.url is required with "
                f"{section_name}.credentials_source."
            )
        if _url_has_userinfo(url):
            raise ConfigurationError(
                f"{section_name}.url must not contain credentials when "
                f"{section_name}.credentials_source is configured."
            )


def _url_has_userinfo(url: str) -> bool:
    try:
        parsed = urlsplit(url)
        return parsed.username is not None or parsed.password is not None
    except ValueError:
        return False


def _default_url_key(name: str, source: SecretSource) -> str:
    if source == KEYCHAIN_SOURCE:
        if name == DEFAULT_CACHE_NAME:
            return "cache/redis/url"
        return f"cache/{name}/redis/url"
    raise ConfigurationError(
        "Redis URL secret references without a key support only the keychain source."
    )


def _default_credentials_key(source: SecretSource) -> str:
    if source == KEYCHAIN_SOURCE:
        return "cache/redis/credentials"
    if source == ENVIRONMENT_SOURCE:
        return "WYBRA_REDIS_CREDENTIALS"
    raise ConfigurationError(
        "Redis credential secret references support only the environment and "
        "keychain sources."
    )


def _credential_reference_name(name: str, kind: str) -> str:
    if name == DEFAULT_CACHE_NAME:
        return f"cache-{kind}"
    return f"cache-{name}-{kind}"


def _redis_database(
    query_database: str | None,
    path: str,
    scheme: str,
) -> int | str:
    if query_database is not None:
        try:
            return int(query_database)
        except ValueError:
            return "configured"
    if scheme.lower() not in {"redis", "rediss"}:
        return 0
    try:
        return int(unquote(path).replace("/", "")) if path else 0
    except ValueError:
        return 0


__all__ = (
    "CacheConfigurationDiagnostic",
    "CacheSettings",
    "CachesSettings",
    "CACHE_BACKEND_FEATURES",
    "DEFAULT_CACHE_NAME",
    "MEMORY_CACHE_FEATURES",
    "REDIS_CACHE_FEATURES",
)
