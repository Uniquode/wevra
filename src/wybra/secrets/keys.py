from __future__ import annotations

from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any, Final

from wybra.cache.config import module_config as cache_module_config
from wybra.cache.settings import CachesSettings
from wybra.config import ConfigService, CredentialReference, MappingConfigSource
from wybra.core.exceptions import ConfigurationError
from wybra.db.config import module_config as database_module_config
from wybra.db.settings import effective_database_config_from_config
from wybra.forms.config import (
    CSRF_TOKEN_SECRET_KEY_CURRENT,
    CSRF_TOKEN_SECRET_KEY_PREVIOUS,
)
from wybra.forms.settings import FormsSettings
from wybra.providers.settings import (
    PROVIDERS_CONFIG_SECTION,
    provider_client_secret_key,
    provider_private_key_secret_key,
    provider_settings_from_config,
)
from wybra.secrets.config import SecretsSettings
from wybra.services.crypto import SECRET_KEY_CURRENT, SECRET_KEY_PREVIOUS
from wybra.services.secrets import KEYCHAIN_SOURCE

SECRET_KEY_TYPE_SECRET: Final = "secret"
SECRET_KEY_TYPE_SECRET_PREVIOUS: Final = "secret-prev"
SECRET_KEY_TYPE_CSRF: Final = "csrf"
SECRET_KEY_TYPE_CSRF_PREVIOUS: Final = "csrf-prev"
SECRET_KEY_TYPE_GOOGLE: Final = "google"
SECRET_KEY_TYPE_GITHUB: Final = "github"
SECRET_KEY_TYPE_APPLE: Final = "apple"

SECRET_KEY_OWNER_CRYPTO: Final = "crypto"
SECRET_KEY_OWNER_FORMS: Final = "forms"
SECRET_KEY_OWNER_PROVIDERS: Final = "providers"


BUILTIN_CRYPTO_SECRET_KEYS: Final[tuple[CredentialReference, ...]] = (
    CredentialReference(
        name=SECRET_KEY_TYPE_SECRET,
        key=SECRET_KEY_CURRENT,
        owner=SECRET_KEY_OWNER_CRYPTO,
        description="Current system secret key.",
        source=KEYCHAIN_SOURCE,
        required=True,
        rotation_role="current",
    ),
    CredentialReference(
        name=SECRET_KEY_TYPE_SECRET_PREVIOUS,
        key=SECRET_KEY_PREVIOUS,
        owner=SECRET_KEY_OWNER_CRYPTO,
        description="Previous system secret keys used during key rotation.",
        source=KEYCHAIN_SOURCE,
        rotation_role="previous",
    ),
)
BUILTIN_FORMS_SECRET_KEYS: Final[tuple[CredentialReference, ...]] = (
    CredentialReference(
        name=SECRET_KEY_TYPE_CSRF,
        key=CSRF_TOKEN_SECRET_KEY_CURRENT,
        owner=SECRET_KEY_OWNER_FORMS,
        description="Current forms CSRF token secret.",
        source=KEYCHAIN_SOURCE,
        required=True,
        rotation_role="current",
    ),
    CredentialReference(
        name=SECRET_KEY_TYPE_CSRF_PREVIOUS,
        key=CSRF_TOKEN_SECRET_KEY_PREVIOUS,
        owner=SECRET_KEY_OWNER_FORMS,
        description="Previous forms CSRF token secrets.",
        source=KEYCHAIN_SOURCE,
        rotation_role="previous",
    ),
)
BUILTIN_PROVIDER_SECRET_KEYS: Final[tuple[CredentialReference, ...]] = (
    CredentialReference(
        name=SECRET_KEY_TYPE_GOOGLE,
        key=provider_client_secret_key(SECRET_KEY_TYPE_GOOGLE),
        owner=SECRET_KEY_OWNER_PROVIDERS,
        description="Provider google client secret.",
        source=KEYCHAIN_SOURCE,
        required=True,
    ),
    CredentialReference(
        name=SECRET_KEY_TYPE_GITHUB,
        key=provider_client_secret_key(SECRET_KEY_TYPE_GITHUB),
        owner=SECRET_KEY_OWNER_PROVIDERS,
        description="Provider github client secret.",
        source=KEYCHAIN_SOURCE,
        required=True,
    ),
    CredentialReference(
        name=SECRET_KEY_TYPE_APPLE,
        key=provider_private_key_secret_key(SECRET_KEY_TYPE_APPLE),
        owner=SECRET_KEY_OWNER_PROVIDERS,
        description="Provider apple private key.",
        source=KEYCHAIN_SOURCE,
        required=True,
    ),
)
BUILTIN_SECRET_KEYS: Final[tuple[CredentialReference, ...]] = (
    BUILTIN_CRYPTO_SECRET_KEYS
    + BUILTIN_FORMS_SECRET_KEYS
    + BUILTIN_PROVIDER_SECRET_KEYS
)


def configured_keychain_credential_references(
    *,
    raw_config: Mapping[str, Mapping[str, Any]] | None = None,
    project_root: Path | None = None,
    development: bool = False,
) -> tuple[CredentialReference, ...]:
    """Return configured keychain-backed credential references."""

    if development:
        return _deduplicate_keys(development_keychain_credential_references())
    return _deduplicate_keys(
        reference
        for reference in configured_credential_references(
            raw_config=raw_config,
            project_root=project_root,
        )
        if reference.source == KEYCHAIN_SOURCE
    )


def configured_credential_references(
    *,
    raw_config: Mapping[str, Mapping[str, Any]] | None = None,
    project_root: Path | None = None,
) -> tuple[CredentialReference, ...]:
    """Return configured credential references without resolving values."""
    if raw_config is None:
        return _deduplicate_keys(BUILTIN_SECRET_KEYS)

    keys: list[CredentialReference] = []
    keys.extend(_configured_secrets_settings(raw_config).credential_references())
    keys.extend(_configured_forms_keys(raw_config))
    keys.extend(_configured_provider_keys(raw_config))
    keys.extend(_configured_database_keys(raw_config, project_root=project_root))
    keys.extend(_configured_cache_keys(raw_config))
    return _deduplicate_keys(keys)


def development_keychain_credential_references() -> tuple[CredentialReference, ...]:
    """Return built-in development keychain credential references."""
    return tuple(
        CredentialReference(
            name=reference.name,
            key=builtin_keychain_secret_key(reference.name, development=True),
            owner=reference.owner,
            description=reference.description,
            source=reference.source,
            required=reference.required,
            rotation_role=reference.rotation_role,
        )
        for reference in BUILTIN_SECRET_KEYS
    )


def _configured_secrets_settings(
    raw_config: Mapping[str, Mapping[str, Any]],
) -> SecretsSettings:
    config = ConfigService(
        [MappingConfigSource(raw_config)],
        config_defs=(SecretsSettings.module_config,),
        discover_module_config=False,
    )
    return SecretsSettings.load_settings(config)


def _configured_provider_keys(
    raw_config: Mapping[str, Mapping[str, Any]],
) -> Iterable[CredentialReference]:
    providers_config = raw_config.get(PROVIDERS_CONFIG_SECTION)
    if providers_config is None:
        return ()
    if not isinstance(providers_config, Mapping):
        raise ConfigurationError(
            f"Providers config must be a [{PROVIDERS_CONFIG_SECTION}] table."
        )

    providers = provider_settings_from_config(providers_config)
    return tuple(
        reference
        for provider in providers
        for reference in provider.credential_references()
    )


def _configured_forms_keys(
    raw_config: Mapping[str, Mapping[str, Any]],
) -> Iterable[CredentialReference]:
    config = ConfigService(
        [MappingConfigSource(raw_config)],
        config_defs=(FormsSettings.module_config,),
        discover_module_config=False,
    )
    settings = FormsSettings.load_settings(config)
    return settings.credential_references()


def _configured_database_keys(
    raw_config: Mapping[str, Mapping[str, Any]],
    *,
    project_root: Path | None,
) -> Iterable[CredentialReference]:
    config = ConfigService(
        [MappingConfigSource(raw_config)],
        config_defs=(database_module_config,),
        discover_module_config=False,
    )
    effective = effective_database_config_from_config(
        config,
        project_root=(project_root or Path.cwd()).resolve(),
    )
    if effective is None:
        return ()
    return effective.credential_references()


def _configured_cache_keys(
    raw_config: Mapping[str, Mapping[str, Any]],
) -> Iterable[CredentialReference]:
    config = ConfigService(
        [MappingConfigSource(raw_config)],
        config_defs=(cache_module_config,),
        discover_module_config=False,
    )
    settings = CachesSettings.load_settings(config)
    return settings.credential_references()


def builtin_keychain_secret_key(name: str, *, development: bool = False) -> str:
    """Return the built-in keychain key for a named Wybra secret type."""
    key_type = normalise_secret_key_type(name)
    if key_type == SECRET_KEY_TYPE_SECRET:
        return _development_key(SECRET_KEY_CURRENT, development=development)
    if key_type == SECRET_KEY_TYPE_SECRET_PREVIOUS:
        return _development_key(SECRET_KEY_PREVIOUS, development=development)
    if key_type == SECRET_KEY_TYPE_CSRF:
        return _development_key(
            CSRF_TOKEN_SECRET_KEY_CURRENT,
            development=development,
        )
    if key_type == SECRET_KEY_TYPE_CSRF_PREVIOUS:
        return _development_key(
            CSRF_TOKEN_SECRET_KEY_PREVIOUS,
            development=development,
        )
    if key_type == SECRET_KEY_TYPE_GOOGLE:
        return provider_client_secret_key(
            SECRET_KEY_TYPE_GOOGLE, development=development
        )
    if key_type == SECRET_KEY_TYPE_GITHUB:
        return provider_client_secret_key(
            SECRET_KEY_TYPE_GITHUB, development=development
        )
    if key_type == SECRET_KEY_TYPE_APPLE:
        return provider_private_key_secret_key(
            SECRET_KEY_TYPE_APPLE,
            development=development,
        )
    raise ConfigurationError(f"Unknown secret type: {name}.")


def normalise_secret_key_type(name: str) -> str:
    key_type = name.strip().lower()
    if key_type:
        return key_type
    raise ConfigurationError("Secret type must not be blank.")


def _development_key(key: str, *, development: bool) -> str:
    if not development:
        return key
    *parent, leaf = key.split("/")
    return "/".join((*parent, "dev", leaf))


def _deduplicate_keys(
    keys: Iterable[CredentialReference],
) -> tuple[CredentialReference, ...]:
    deduplicated: dict[str, CredentialReference] = {}
    for key in keys:
        existing = deduplicated.get(key.name)
        if existing is None:
            deduplicated[key.name] = key
            continue
        # Credential names are a stable operator interface; duplicate names
        # are a programming/configuration error and must fail fast.
        raise ConfigurationError(
            f"Duplicate credential reference name '{key.name}' "
            f"(owners: {existing.owner!r} and {key.owner!r})."
        )
    return tuple(deduplicated.values())


__all__ = (
    "BUILTIN_SECRET_KEYS",
    "BUILTIN_CRYPTO_SECRET_KEYS",
    "BUILTIN_FORMS_SECRET_KEYS",
    "BUILTIN_PROVIDER_SECRET_KEYS",
    "SECRET_KEY_TYPE_APPLE",
    "SECRET_KEY_TYPE_CSRF",
    "SECRET_KEY_TYPE_CSRF_PREVIOUS",
    "SECRET_KEY_TYPE_GITHUB",
    "SECRET_KEY_TYPE_GOOGLE",
    "SECRET_KEY_TYPE_SECRET",
    "SECRET_KEY_TYPE_SECRET_PREVIOUS",
    "SECRET_KEY_OWNER_CRYPTO",
    "SECRET_KEY_OWNER_FORMS",
    "SECRET_KEY_OWNER_PROVIDERS",
    "builtin_keychain_secret_key",
    "development_keychain_credential_references",
    "configured_credential_references",
    "configured_keychain_credential_references",
    "normalise_secret_key_type",
)
