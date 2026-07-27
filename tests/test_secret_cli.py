import importlib
import json
from collections.abc import Mapping
from pathlib import Path

import click
import pytest
from click.testing import CliRunner

import wybra.secrets.cli as secret_cli
import wybra.secrets.keys as secret_keys
import wybra.secrets.sources as secret_sources
from support_database import sqlite_file_url
from wybra.auth.mfa.storage import TOTP_ACTIVE_STATUS
from wybra.auth.models import (
    IdentityProvider,
    IdentityTotpCredential,
    IdentityTotpRecoveryCode,
    User,
)
from wybra.config import ConfigService, CredentialReference
from wybra.db.persistence import close_database, create_database
from wybra.forms import CSRF_TOKEN_SECRET_KEY_CURRENT, CSRF_TOKEN_SECRET_KEY_PREVIOUS
from wybra.secrets.keys import (
    SECRET_KEY_TYPE_CSRF,
    SECRET_KEY_TYPE_GITHUB,
    SECRET_KEY_TYPE_GOOGLE,
    SECRET_KEY_TYPE_SECRET,
)
from wybra.services.crypto import (
    SECRET_KEY_CURRENT,
    SECRET_KEY_PREVIOUS,
    SecretEnvelopeService,
    generate_secret_key_entry,
    parse_secret_key_bundle,
)
from wybra.testing import create_test_database


class FakeKeyring:
    def __init__(self, values: Mapping[tuple[str, str], str] | None = None) -> None:
        self.values = dict(values or {})
        self.writes: list[tuple[str, str, str]] = []

    def get_password(self, service: str, username: str) -> str | None:
        return self.values.get((service, username))

    def set_password(self, service: str, username: str, value: str) -> None:
        self.writes.append((service, username, value))
        self.values[(service, username)] = value


def _install_fake_keyring(monkeypatch, keyring: FakeKeyring) -> None:
    original_import_module = importlib.import_module

    def import_module(name: str):
        if name == "keyring":
            return keyring
        return original_import_module(name)

    monkeypatch.setattr(secret_sources.sys, "platform", "linux")
    monkeypatch.setattr(secret_sources.importlib, "import_module", import_module)


def _app_config(path: Path) -> Path:
    path.write_text(
        """
[app]
modules = ["wybra.secrets", "wybra.forms", "wybra.auth", "wybra.providers"]

[wybra.forms]
csrf_token_secret_source = "keychain"

[secrets.keychain]
appname = "uniquode.io"
username = "deployment"

[secrets.crypto]
source = "keychain"
current_key = "SYSTEM_SECRET_KEY"
previous_keys = "SYSTEM_SECRET_KEYS_PREVIOUS"

[auth.providers.google]
enabled = true
secrets = "keychain"

[auth.providers.github]
enabled = false
secrets = "keychain"

[auth.providers.apple]
enabled = true
client_id = "com.example.app.web"
team_id = "TEAMID1234"
key_id = "KEYID1234"
secrets = "environment"
private_key_secret_key = "APPLE_PRIVATE_KEY"
""".strip(),
        encoding="utf-8",
    )
    return path


def _non_keychain_crypto_config(path: Path) -> Path:
    path.write_text(
        """
[app]
modules = ["wybra.secrets"]

[secrets.crypto]
source = "environment"
current_key = "SYSTEM_SECRET_KEY"
previous_keys = "SYSTEM_SECRET_KEYS_PREVIOUS"
""".strip(),
        encoding="utf-8",
    )
    return path


def _keychain_crypto_without_previous_config(path: Path) -> Path:
    path.write_text(
        """
[app]
modules = ["wybra.secrets"]

[secrets.keychain]
appname = "uniquode.io"

[secrets.crypto]
source = "keychain"
current_key = "SYSTEM_SECRET_KEY"
""".strip(),
        encoding="utf-8",
    )
    return path


def _non_keychain_csrf_config(path: Path) -> Path:
    path.write_text(
        """
[app]
modules = ["wybra.secrets", "wybra.forms"]

[wybra.forms]
csrf_token_secret = "inline-csrf-secret"
""".strip(),
        encoding="utf-8",
    )
    return path


def _database_keychain_config(path: Path) -> Path:
    path.write_text(
        """
[app]
modules = ["wybra.secrets"]

[app.database]
backend = "postgresql"
database = "uniquode"
credential_source = "keychain"

[secrets.keychain]
appname = "uniquode.io"
username = "deployment"
""".strip(),
        encoding="utf-8",
    )
    return path


def _cache_keychain_config(path: Path) -> Path:
    path.write_text(
        """
[app]
modules = ["wybra.secrets", "wybra.cache"]

[secrets.keychain]
appname = "uniquode.io"
username = "deployment"

[cache]
backend = "redis"
url_source = "keychain"

[cache.session]
backend = "redis"
url = "rediss://cache.internal/1"
credentials_source = "keychain"

[cache.tasks]
backend = "redis"
url_source = "keychain"
url_key = "cache/tasks/redis/connection-url"
""".strip(),
        encoding="utf-8",
    )
    return path


def _cache_environment_url_config(path: Path) -> Path:
    path.write_text(
        """
[app]
modules = ["wybra.secrets", "wybra.cache"]

[secrets.keychain]
appname = "uniquode.io"
username = "deployment"

[cache]
backend = "redis"
credentials_source = "keychain"
""".strip(),
        encoding="utf-8",
    )
    return path


def _reencrypt_app_config(path: Path, database_url: str) -> Path:
    path.write_text(
        f"""
[app]
modules = ["wybra.secrets", "wybra.auth"]
database_url = "{database_url}"

[app.templates]
auto_reload = true
cache_size = 0

[app.assets]
url_path = "/static/"
root = "static"

[secrets.keychain]
appname = "uniquode.io"
username = "deployment"

[secrets.crypto]
source = "keychain"
current_key = "SYSTEM_SECRET_KEY"
previous_keys = "SYSTEM_SECRET_KEYS_PREVIOUS"
""".strip(),
        encoding="utf-8",
    )
    return path


async def _reencrypt_configured_secrets(
    config_path: Path,
    *,
    dry_run: bool,
) -> secret_cli.ReencryptSecretsResult:
    context = click.Context(secret_cli.secret_command)
    context.obj = {
        secret_cli.CONFIG_SOURCE_CONTEXT_KEY: config_path.as_posix(),
    }
    return await secret_cli._reencrypt_secrets(context, dry_run=dry_run)


async def _create_reencrypt_database(
    database_url: str,
    *,
    provider_access_token: str,
    provider_refresh_token: str | None,
    totp_secret: str,
    recovery_code_verifier: str,
) -> None:
    database = await create_test_database(
        database_url=database_url,
        modules=("wybra.auth",),
    )
    try:
        with database.context:
            connection = database.connection()
            user = await User.create(
                email="user@example.com",
                hashed_password="hashed-password",
                is_active=True,
                is_superuser=False,
                is_verified=True,
                using_db=connection,
            )
            await IdentityProvider.create(
                provider_name="google",
                provider_subject="provider-subject",
                crypt_access_token=provider_access_token,
                crypt_refresh_token=provider_refresh_token,
                account_email="user@example.com",
                using_db=connection,
            )
            credential = await IdentityTotpCredential.create(
                user_id=user.id,
                crypt_secret=totp_secret,
                status=TOTP_ACTIVE_STATUS,
                created_at=1000.0,
                activated_at=1001.0,
                using_db=connection,
            )
            await IdentityTotpRecoveryCode.create(
                credential_id=credential.id,
                code_verifier=recovery_code_verifier,
                created_at=1002.0,
                using_db=connection,
            )
    finally:
        await close_database(database)


async def _reencrypt_database_values(database_url: str) -> dict[str, str | None]:
    database = await create_database(database_url, modules=("wybra.auth",))
    try:
        with database.context:
            connection = database.connection()
            provider = await IdentityProvider.all().using_db(connection).first()
            credential = await IdentityTotpCredential.all().using_db(connection).first()
            recovery_code = (
                await IdentityTotpRecoveryCode.all().using_db(connection).first()
            )
            if provider is None or credential is None or recovery_code is None:
                raise AssertionError("Expected re-encryption test rows to exist.")
            return {
                "access": provider.crypt_access_token,
                "refresh": provider.crypt_refresh_token,
                "totp": credential.crypt_secret,
                "recovery": recovery_code.code_verifier,
            }
    finally:
        await close_database(database)


def test_set_get_and_list_use_default_keychain_mapping(monkeypatch) -> None:
    keyring = FakeKeyring()
    _install_fake_keyring(monkeypatch, keyring)
    runner = CliRunner()

    result = runner.invoke(
        secret_cli.secret_command,
        ["set", SECRET_KEY_TYPE_SECRET],
        input="secret-value\n",
    )

    assert result.exit_code == 0, result.output
    assert keyring.values[("wybra", SECRET_KEY_CURRENT)] == "secret-value"

    result = runner.invoke(
        secret_cli.secret_command,
        ["get", "--json", SECRET_KEY_TYPE_SECRET],
    )

    assert result.exit_code == 0, result.output
    assert json.loads(result.output) == {
        "key": SECRET_KEY_CURRENT,
        "name": SECRET_KEY_TYPE_SECRET,
        "service": "wybra",
        "account": SECRET_KEY_CURRENT,
        "value": "secret-value",
    }

    result = runner.invoke(secret_cli.secret_command, ["list", "--json"])

    assert result.exit_code == 0, result.output
    records = json.loads(result.output)["keys"]
    assert records[SECRET_KEY_TYPE_SECRET]["key"] == SECRET_KEY_CURRENT
    assert records[SECRET_KEY_TYPE_SECRET]["exists"] is True
    assert records["secret-prev"]["key"] == SECRET_KEY_PREVIOUS
    assert records["secret-prev"]["exists"] is False
    assert records[SECRET_KEY_TYPE_CSRF]["key"] == CSRF_TOKEN_SECRET_KEY_CURRENT
    assert records[SECRET_KEY_TYPE_GOOGLE]["key"] == (
        "auth/providers/google/client-secret"
    )


def test_set_supports_json_bulk_input(monkeypatch) -> None:
    keyring = FakeKeyring()
    _install_fake_keyring(monkeypatch, keyring)

    result = CliRunner().invoke(
        secret_cli.secret_command,
        ["set", "--json"],
        input=json.dumps(
            {
                SECRET_KEY_CURRENT: "current-secret",
                "auth/providers/google/client-secret": "google-secret",
            }
        ),
    )

    assert result.exit_code == 0, result.output
    assert keyring.values == {
        ("wybra", SECRET_KEY_CURRENT): "current-secret",
        ("wybra", "auth/providers/google/client-secret"): "google-secret",
    }
    output = json.loads(result.output)
    assert output == {
        "stored": [
            {
                "key": SECRET_KEY_CURRENT,
                "service": "wybra",
                "account": SECRET_KEY_CURRENT,
            },
            {
                "key": "auth/providers/google/client-secret",
                "service": "wybra",
                "account": "auth/providers/google/client-secret",
            },
        ]
    }
    assert "google-secret" not in result.output


def test_set_and_get_name_support_development_default_keys(monkeypatch) -> None:
    keyring = FakeKeyring()
    _install_fake_keyring(monkeypatch, keyring)
    runner = CliRunner()

    result = runner.invoke(
        secret_cli.secret_command,
        ["set", "--dev", SECRET_KEY_TYPE_GOOGLE, "google-secret"],
    )

    assert result.exit_code == 0, result.output
    key = "auth/providers/google/dev/client-secret"
    assert keyring.values[("wybra", key)] == "google-secret"

    result = runner.invoke(
        secret_cli.secret_command,
        ["get", "--dev", SECRET_KEY_TYPE_GOOGLE],
    )

    assert result.exit_code == 0, result.output
    assert result.output == "google-secret\n"


def test_list_json_dev_reports_development_default_keys(monkeypatch) -> None:
    keyring = FakeKeyring(
        {
            ("wybra", "secrets/key/dev/current"): "secret",
            ("wybra", "auth/providers/github/dev/client-secret"): "github",
        }
    )
    _install_fake_keyring(monkeypatch, keyring)

    result = CliRunner().invoke(
        secret_cli.secret_command,
        ["list", "--json", "--dev"],
    )

    assert result.exit_code == 0, result.output
    keys = json.loads(result.output)["keys"]
    assert keys[SECRET_KEY_TYPE_SECRET]["key"] == "secrets/key/dev/current"
    assert keys[SECRET_KEY_TYPE_SECRET]["exists"] is True
    assert keys[SECRET_KEY_TYPE_GITHUB]["key"] == (
        "auth/providers/github/dev/client-secret"
    )
    assert keys[SECRET_KEY_TYPE_GITHUB]["exists"] is True
    assert keys[SECRET_KEY_TYPE_GOOGLE]["key"] == (
        "auth/providers/google/dev/client-secret"
    )
    assert keys[SECRET_KEY_TYPE_GOOGLE]["exists"] is False


def test_name_uses_default_key_even_when_config_uses_custom_key(
    monkeypatch,
    tmp_path: Path,
) -> None:
    keyring = FakeKeyring()
    _install_fake_keyring(monkeypatch, keyring)
    config_path = _app_config(tmp_path / "app.toml")

    result = CliRunner().invoke(
        secret_cli.secret_command,
        [
            "--config",
            config_path.as_posix(),
            "set",
            SECRET_KEY_TYPE_SECRET,
            "default-secret",
        ],
    )

    assert result.exit_code == 0, result.output
    assert keyring.values == {
        ("uniquode.io", SECRET_KEY_CURRENT): "default-secret",
    }


def test_name_uses_configured_database_credential_reference(
    monkeypatch,
    tmp_path: Path,
) -> None:
    keyring = FakeKeyring()
    _install_fake_keyring(monkeypatch, keyring)
    config_path = _database_keychain_config(tmp_path / "app.toml")
    runner = CliRunner()

    result = runner.invoke(
        secret_cli.secret_command,
        [
            "--config",
            config_path.as_posix(),
            "set",
            "database-user",
            "uniquode_user",
        ],
    )

    assert result.exit_code == 0, result.output
    assert keyring.values == {
        ("uniquode.io", "database/uniquode/app/user"): "uniquode_user"
    }

    result = runner.invoke(
        secret_cli.secret_command,
        [
            "--config",
            config_path.as_posix(),
            "get",
            "database-user",
        ],
    )

    assert result.exit_code == 0, result.output
    assert result.output == "uniquode_user\n"


def test_key_cannot_be_combined_with_name(monkeypatch) -> None:
    keyring = FakeKeyring()
    _install_fake_keyring(monkeypatch, keyring)

    result = CliRunner().invoke(
        secret_cli.secret_command,
        ["get", "--key", SECRET_KEY_CURRENT, SECRET_KEY_TYPE_SECRET],
    )

    assert result.exit_code != 0
    assert "NAME cannot be combined with --key" in result.output


def test_unknown_name_reports_usage_error_without_traceback(monkeypatch) -> None:
    keyring = FakeKeyring()
    _install_fake_keyring(monkeypatch, keyring)

    result = CliRunner().invoke(
        secret_cli.secret_command,
        ["set", "missing", "value"],
    )

    assert result.exit_code == 2
    assert "Unknown secret name: missing." in result.output
    assert "Traceback" not in result.output


def test_development_mode_rejects_configured_non_builtin_names(
    monkeypatch,
    tmp_path: Path,
) -> None:
    keyring = FakeKeyring()
    _install_fake_keyring(monkeypatch, keyring)
    config_path = _database_keychain_config(tmp_path / "app.toml")

    result = CliRunner().invoke(
        secret_cli.secret_command,
        [
            "--config",
            config_path.as_posix(),
            "set",
            "--dev",
            "database-user",
            "uniquode_user",
        ],
    )

    assert result.exit_code == 2
    assert "No development key is available for database-user." in result.output
    assert "Traceback" not in result.output
    assert keyring.values == {}


def test_duplicate_credential_reference_names_fail_fast_without_traceback(
    monkeypatch,
    tmp_path: Path,
) -> None:
    keyring = FakeKeyring()
    _install_fake_keyring(monkeypatch, keyring)
    config_path = _app_config(tmp_path / "app.toml")

    monkeypatch.setattr(
        secret_keys,
        "_configured_forms_keys",
        lambda _raw_config: (
            CredentialReference(
                name=SECRET_KEY_TYPE_SECRET,
                key="duplicate/forms/key",
                owner="forms",
                description="Duplicate key",
                source="keychain",
            ),
        ),
    )

    result = CliRunner().invoke(
        secret_cli.secret_command,
        ["--config", config_path.as_posix(), "list"],
    )

    assert result.exit_code != 0
    assert "Duplicate credential reference name 'secret'" in result.output
    assert "owners: 'crypto' and 'forms'" in result.output
    assert "Traceback" not in result.output


def test_list_uses_configured_keychain_metadata_and_app_key_refs(
    monkeypatch,
    tmp_path: Path,
) -> None:
    keyring = FakeKeyring(
        {
            ("uniquode.io", "SYSTEM_SECRET_KEY"): "current",
            (
                "uniquode.io",
                "auth/providers/google/client-secret",
            ): "google",
            ("uniquode.io", CSRF_TOKEN_SECRET_KEY_CURRENT): "csrf",
            ("uniquode.io", CSRF_TOKEN_SECRET_KEY_PREVIOUS): "previous-csrf",
        }
    )
    _install_fake_keyring(monkeypatch, keyring)
    config_path = _app_config(tmp_path / "app.toml")

    result = CliRunner().invoke(
        secret_cli.secret_command,
        ["--config", config_path.as_posix(), "list", "--json"],
    )

    assert result.exit_code == 0, result.output
    records = json.loads(result.output)["keys"]
    assert records[SECRET_KEY_TYPE_SECRET]["key"] == "SYSTEM_SECRET_KEY"
    assert records[SECRET_KEY_TYPE_SECRET]["service"] == "uniquode.io"
    assert records[SECRET_KEY_TYPE_SECRET]["account"] == "SYSTEM_SECRET_KEY"
    assert records[SECRET_KEY_TYPE_SECRET]["exists"] is True
    assert records["secret-prev"]["key"] == "SYSTEM_SECRET_KEYS_PREVIOUS"
    assert records["secret-prev"]["exists"] is False
    assert records[SECRET_KEY_TYPE_CSRF]["owner"] == "forms"
    assert records[SECRET_KEY_TYPE_CSRF]["description"] == (
        "Configured current forms CSRF token secret."
    )
    assert records[SECRET_KEY_TYPE_CSRF]["key"] == CSRF_TOKEN_SECRET_KEY_CURRENT
    assert records[SECRET_KEY_TYPE_CSRF]["exists"] is True
    assert records["csrf-prev"]["owner"] == "forms"
    assert records["csrf-prev"]["description"] == (
        "Configured previous forms CSRF token secrets."
    )
    assert records["csrf-prev"]["key"] == CSRF_TOKEN_SECRET_KEY_PREVIOUS
    assert records["csrf-prev"]["exists"] is True
    assert records[SECRET_KEY_TYPE_GOOGLE]["key"] == (
        "auth/providers/google/client-secret"
    )
    assert records[SECRET_KEY_TYPE_GOOGLE]["exists"] is True
    assert SECRET_KEY_TYPE_GITHUB not in records
    assert "apple" not in records


def test_list_includes_configured_database_keychain_references(
    monkeypatch,
    tmp_path: Path,
) -> None:
    keyring = FakeKeyring(
        {
            ("uniquode.io", "database/uniquode/app/user"): "app_user",
            (
                "uniquode.io",
                "database/uniquode/service-account/user",
            ): "service_user",
        }
    )
    _install_fake_keyring(monkeypatch, keyring)
    config_path = _database_keychain_config(tmp_path / "app.toml")

    result = CliRunner().invoke(
        secret_cli.secret_command,
        ["--config", config_path.as_posix(), "list", "--json"],
    )

    assert result.exit_code == 0, result.output
    records = json.loads(result.output)["keys"]
    assert records["database-user"] == {
        "description": "Configured runtime database username.",
        "exists": True,
        "key": "database/uniquode/app/user",
        "name": "database-user",
        "owner": "database",
        "required": True,
        "rotation_role": None,
        "service": "uniquode.io",
        "source": "keychain",
        "account": "database/uniquode/app/user",
    }
    assert records["database-password"] == {
        "description": "Configured runtime database password.",
        "exists": False,
        "key": "database/uniquode/app/password",
        "name": "database-password",
        "owner": "database",
        "required": True,
        "rotation_role": None,
        "service": "uniquode.io",
        "source": "keychain",
        "account": "database/uniquode/app/password",
    }
    assert records["database-sa-user"]["exists"] is True
    assert records["database-sa-user"]["key"] == (
        "database/uniquode/service-account/user"
    )
    assert records["database-sa-password"]["exists"] is False
    assert records["database-sa-password"]["key"] == (
        "database/uniquode/service-account/password"
    )
    assert "app_user" not in result.output
    assert "service_user" not in result.output


def test_list_includes_configured_cache_keychain_references(
    monkeypatch,
    tmp_path: Path,
) -> None:
    keyring = FakeKeyring(
        {
            ("uniquode.io", "cache/redis/url"): "redis://default",
            ("uniquode.io", "cache/redis/credentials"): "user:password",
            (
                "uniquode.io",
                "cache/tasks/redis/connection-url",
            ): "redis://tasks",
        }
    )
    _install_fake_keyring(monkeypatch, keyring)
    config_path = _cache_keychain_config(tmp_path / "app.toml")

    result = CliRunner().invoke(
        secret_cli.secret_command,
        ["--config", config_path.as_posix(), "list", "--json"],
    )

    assert result.exit_code == 0, result.output
    records = json.loads(result.output)["keys"]
    assert records["cache-url"]["key"] == "cache/redis/url"
    assert records["cache-url"]["exists"] is True
    assert records["cache-session-credentials"]["key"] == "cache/redis/credentials"
    assert records["cache-session-credentials"]["exists"] is True
    assert records["cache-tasks-url"]["key"] == "cache/tasks/redis/connection-url"
    assert records["cache-tasks-url"]["exists"] is True


def test_list_applies_environment_redis_url_for_cache_credentials(
    monkeypatch,
    tmp_path: Path,
) -> None:
    keyring = FakeKeyring({("uniquode.io", "cache/redis/credentials"): "user:password"})
    _install_fake_keyring(monkeypatch, keyring)
    ConfigService.set_runtime_environment(
        {"WYBRA_CACHE_URL": "rediss://cache.internal/0"}
    )
    config_path = _cache_environment_url_config(tmp_path / "app.toml")

    result = CliRunner().invoke(
        secret_cli.secret_command,
        ["--config", config_path.as_posix(), "list", "--json"],
    )

    assert result.exit_code == 0, result.output
    records = json.loads(result.output)["keys"]
    assert records["cache-credentials"]["key"] == "cache/redis/credentials"
    assert records["cache-credentials"]["exists"] is True


def test_list_excludes_crypto_keys_for_non_keychain_config(
    monkeypatch,
    tmp_path: Path,
) -> None:
    keyring = FakeKeyring()
    _install_fake_keyring(monkeypatch, keyring)
    config_path = tmp_path / "app.toml"
    config_path.write_text(
        """
[app]
modules = ["wybra.secrets"]

[secrets.crypto]
source = "environment"
current_key = "SYSTEM_SECRET_KEY"
""".strip(),
        encoding="utf-8",
    )

    result = CliRunner().invoke(
        secret_cli.secret_command,
        ["--config", config_path.as_posix(), "list", "--json"],
    )

    assert result.exit_code == 0, result.output
    assert json.loads(result.output) == {"keys": {}}


def test_list_excludes_csrf_fallback_without_keychain_config(
    monkeypatch,
    tmp_path: Path,
) -> None:
    keyring = FakeKeyring()
    _install_fake_keyring(monkeypatch, keyring)
    config_path = tmp_path / "app.toml"
    config_path.write_text(
        """
[app]
modules = ["wybra.secrets", "wybra.forms"]

[wybra.forms]
csrf_token_secret = "inline-csrf-secret"
""".strip(),
        encoding="utf-8",
    )

    result = CliRunner().invoke(
        secret_cli.secret_command,
        ["--config", config_path.as_posix(), "list", "--json"],
    )

    assert result.exit_code == 0
    assert json.loads(result.output) == {"keys": {}}


def test_get_honours_app_config_environment(monkeypatch, tmp_path: Path) -> None:
    keyring = FakeKeyring({("uniquode.io", "SYSTEM_SECRET_KEY"): "configured-secret"})
    _install_fake_keyring(monkeypatch, keyring)
    monkeypatch.setenv("APP_CONFIG", _app_config(tmp_path / "app.toml").as_posix())

    result = CliRunner().invoke(
        secret_cli.secret_command,
        ["get", "--key", "SYSTEM_SECRET_KEY"],
    )

    assert result.exit_code == 0
    assert result.output == "configured-secret\n"


def test_rotate_secret_key_updates_previous_before_current(
    monkeypatch,
    tmp_path: Path,
) -> None:
    current = generate_secret_key_entry(version="current")
    previous = generate_secret_key_entry(version="previous")
    keyring = FakeKeyring(
        {
            ("uniquode.io", "SYSTEM_SECRET_KEY"): current,
            ("uniquode.io", "SYSTEM_SECRET_KEYS_PREVIOUS"): previous,
        }
    )
    _install_fake_keyring(monkeypatch, keyring)
    config_path = _app_config(tmp_path / "app.toml")

    result = CliRunner().invoke(
        secret_cli.secret_command,
        ["--config", config_path.as_posix(), "rotate", "system", "--json"],
    )

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["target"] == "system"
    assert payload["old_current_version"] == "current"
    assert payload["new_current_version"] not in {"current", "previous"}
    assert payload["previous_key_count"] == 2
    new_current = keyring.values[("uniquode.io", "SYSTEM_SECRET_KEY")]
    new_previous = keyring.values[("uniquode.io", "SYSTEM_SECRET_KEYS_PREVIOUS")]
    assert new_previous == f"{current},{previous}"
    parse_secret_key_bundle(current=new_current, previous=new_previous)
    assert [write[1] for write in keyring.writes[:2]] == [
        "SYSTEM_SECRET_KEYS_PREVIOUS",
        "SYSTEM_SECRET_KEY",
    ]
    assert current not in result.output
    assert previous not in result.output
    assert new_current not in result.output


def test_rotate_secret_key_dry_run_does_not_write(
    monkeypatch,
    tmp_path: Path,
) -> None:
    current = generate_secret_key_entry(version="current")
    keyring = FakeKeyring({("uniquode.io", "SYSTEM_SECRET_KEY"): current})
    _install_fake_keyring(monkeypatch, keyring)
    config_path = _app_config(tmp_path / "app.toml")

    result = CliRunner().invoke(
        secret_cli.secret_command,
        [
            "--config",
            config_path.as_posix(),
            "rotate",
            "system",
            "--dry-run",
            "--json",
        ],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["dry_run"] is True
    assert payload["previous_key_count"] == 1
    assert keyring.values == {("uniquode.io", "SYSTEM_SECRET_KEY"): current}
    assert keyring.writes == []
    assert current not in result.output


def test_rotate_secret_key_refuses_non_keychain_crypto_source(
    monkeypatch,
    tmp_path: Path,
) -> None:
    keyring = FakeKeyring({("uniquode.io", "SYSTEM_SECRET_KEY"): "unchanged"})
    _install_fake_keyring(monkeypatch, keyring)
    config_path = _non_keychain_crypto_config(tmp_path / "app.toml")

    result = CliRunner().invoke(
        secret_cli.secret_command,
        ["--config", config_path.as_posix(), "rotate", "system"],
    )

    assert result.exit_code != 0
    assert "keychain-backed system secret keys" in result.output
    assert keyring.values == {("uniquode.io", "SYSTEM_SECRET_KEY"): "unchanged"}
    assert keyring.writes == []


def test_rotate_secret_key_uses_default_previous_keys_reference(
    monkeypatch,
    tmp_path: Path,
) -> None:
    current = generate_secret_key_entry(version="current")
    keyring = FakeKeyring({("uniquode.io", "SYSTEM_SECRET_KEY"): current})
    _install_fake_keyring(monkeypatch, keyring)
    config_path = _keychain_crypto_without_previous_config(tmp_path / "app.toml")

    result = CliRunner().invoke(
        secret_cli.secret_command,
        ["--config", config_path.as_posix(), "rotate", "system"],
    )

    assert result.exit_code == 0, result.output
    assert keyring.values[("uniquode.io", SECRET_KEY_PREVIOUS)] == current
    assert keyring.values[("uniquode.io", "SYSTEM_SECRET_KEY")] != current
    assert keyring.writes[0][:2] == ("uniquode.io", SECRET_KEY_PREVIOUS)
    assert keyring.writes[1][:2] == ("uniquode.io", "SYSTEM_SECRET_KEY")


def test_rotate_csrf_token_secret_updates_previous_before_current(
    monkeypatch,
    tmp_path: Path,
) -> None:
    keyring = FakeKeyring(
        {
            ("uniquode.io", CSRF_TOKEN_SECRET_KEY_CURRENT): "current-csrf-secret",
            (
                "uniquode.io",
                CSRF_TOKEN_SECRET_KEY_PREVIOUS,
            ): "previous-csrf-secret",
        }
    )
    _install_fake_keyring(monkeypatch, keyring)
    config_path = _app_config(tmp_path / "app.toml")

    result = CliRunner().invoke(
        secret_cli.secret_command,
        [
            "--config",
            config_path.as_posix(),
            "rotate",
            "csrf",
            "--json",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["target"] == "csrf"
    assert payload["previous_secret_count"] == 2
    new_current = keyring.values[("uniquode.io", CSRF_TOKEN_SECRET_KEY_CURRENT)]
    new_previous = keyring.values[("uniquode.io", CSRF_TOKEN_SECRET_KEY_PREVIOUS)]
    assert new_current != "current-csrf-secret"
    assert new_previous == "current-csrf-secret,previous-csrf-secret"
    assert [write[1] for write in keyring.writes[:2]] == [
        CSRF_TOKEN_SECRET_KEY_PREVIOUS,
        CSRF_TOKEN_SECRET_KEY_CURRENT,
    ]
    assert "current-csrf-secret" not in result.output
    assert "previous-csrf-secret" not in result.output
    assert new_current not in result.output


def test_rotate_csrf_token_secret_refuses_non_keychain_forms_config(
    monkeypatch,
    tmp_path: Path,
) -> None:
    keyring = FakeKeyring()
    _install_fake_keyring(monkeypatch, keyring)
    config_path = _non_keychain_csrf_config(tmp_path / "app.toml")

    result = CliRunner().invoke(
        secret_cli.secret_command,
        ["--config", config_path.as_posix(), "rotate", "csrf"],
    )

    assert result.exit_code != 0
    assert "keychain-backed CSRF token secrets" in result.output
    assert keyring.values == {}
    assert keyring.writes == []


@pytest.mark.anyio
async def test_reencrypt_secrets_dry_run_reports_without_writing_database_rows(
    monkeypatch,
    tmp_path: Path,
) -> None:
    current_key = generate_secret_key_entry(version="current")
    previous_key = generate_secret_key_entry(version="previous")
    current_service = SecretEnvelopeService.from_key_bundle(current_key, previous_key)
    previous_service = SecretEnvelopeService.from_key_bundle(previous_key)
    old_access = previous_service.encrypt_required("access-token")
    old_refresh = previous_service.encrypt_required("refresh-token")
    old_totp = previous_service.encrypt_required("totp-secret")
    recovery_verifier = previous_service.create_verifier_required(
        "recovery-code",
        context="totp-recovery-code",
    )
    database_url = sqlite_file_url(tmp_path / "reencrypt.sqlite3")
    await _create_reencrypt_database(
        database_url,
        provider_access_token=old_access,
        provider_refresh_token=old_refresh,
        totp_secret=old_totp,
        recovery_code_verifier=recovery_verifier,
    )
    keyring = FakeKeyring(
        {
            ("uniquode.io", "SYSTEM_SECRET_KEY"): current_key,
            ("uniquode.io", "SYSTEM_SECRET_KEYS_PREVIOUS"): previous_key,
        }
    )
    _install_fake_keyring(monkeypatch, keyring)
    config_path = _reencrypt_app_config(tmp_path / "app.toml", database_url)

    result = await _reencrypt_configured_secrets(config_path, dry_run=True)
    payload = secret_cli._reencrypt_result_payload(result, target="system")
    assert payload["target"] == "system"
    assert payload["dry_run"] is True
    assert payload["scanned"] == 3
    assert payload["rewritten"] == 3
    assert payload["unsupported_recovery_code_verifiers"] == 1
    values = await _reencrypt_database_values(database_url)
    assert values == {
        "access": old_access,
        "refresh": old_refresh,
        "totp": old_totp,
        "recovery": recovery_verifier,
    }
    output = json.dumps(payload)
    assert current_key not in output
    assert previous_key not in output
    assert old_access not in output
    assert old_refresh not in output
    assert old_totp not in output
    assert "access-token" not in output
    assert "refresh-token" not in output
    assert "totp-secret" not in output
    assert "recovery-code" not in output
    assert current_service.current_version_required() == "current"


@pytest.mark.anyio
async def test_reencrypt_secrets_rewrites_previous_version_provider_and_totp_secrets(
    monkeypatch,
    tmp_path: Path,
) -> None:
    current_key = generate_secret_key_entry(version="current")
    previous_key = generate_secret_key_entry(version="previous")
    current_service = SecretEnvelopeService.from_key_bundle(current_key, previous_key)
    previous_service = SecretEnvelopeService.from_key_bundle(previous_key)
    old_access = previous_service.encrypt_required("access-token")
    old_refresh = previous_service.encrypt_required("refresh-token")
    old_totp = previous_service.encrypt_required("totp-secret")
    recovery_verifier = previous_service.create_verifier_required(
        "recovery-code",
        context="totp-recovery-code",
    )
    database_url = sqlite_file_url(tmp_path / "reencrypt.sqlite3")
    await _create_reencrypt_database(
        database_url,
        provider_access_token=old_access,
        provider_refresh_token=old_refresh,
        totp_secret=old_totp,
        recovery_code_verifier=recovery_verifier,
    )
    keyring = FakeKeyring(
        {
            ("uniquode.io", "SYSTEM_SECRET_KEY"): current_key,
            ("uniquode.io", "SYSTEM_SECRET_KEYS_PREVIOUS"): previous_key,
        }
    )
    _install_fake_keyring(monkeypatch, keyring)
    config_path = _reencrypt_app_config(tmp_path / "app.toml", database_url)

    result = await _reencrypt_configured_secrets(config_path, dry_run=False)
    payload = secret_cli._reencrypt_result_payload(result, target="system")
    assert payload["dry_run"] is False
    assert payload["rewritten"] == 3
    assert payload["unsupported_recovery_code_verifiers"] == 1
    values = await _reencrypt_database_values(database_url)
    assert values["access"] != old_access
    assert values["refresh"] != old_refresh
    assert values["totp"] != old_totp
    assert values["recovery"] == recovery_verifier
    assert current_service.decrypt_required(values["access"] or "") == (
        "access-token",
        "current",
    )
    assert current_service.decrypt_required(values["refresh"] or "") == (
        "refresh-token",
        "current",
    )
    assert current_service.decrypt_required(values["totp"] or "") == (
        "totp-secret",
        "current",
    )
    output = json.dumps(payload)
    assert current_key not in output
    assert previous_key not in output
    assert old_access not in output
    assert old_refresh not in output
    assert old_totp not in output
    assert "access-token" not in output
    assert "refresh-token" not in output
    assert "totp-secret" not in output


@pytest.mark.anyio
async def test_reencrypt_secrets_skips_current_and_plaintext_values(
    monkeypatch,
    tmp_path: Path,
) -> None:
    current_key = generate_secret_key_entry(version="current")
    previous_key = generate_secret_key_entry(version="previous")
    current_service = SecretEnvelopeService.from_key_bundle(current_key, previous_key)
    current_access = current_service.encrypt_required("access-token")
    current_totp = current_service.encrypt_required("totp-secret")
    recovery_verifier = current_service.create_verifier_required(
        "recovery-code",
        context="totp-recovery-code",
    )
    database_url = sqlite_file_url(tmp_path / "reencrypt.sqlite3")
    await _create_reencrypt_database(
        database_url,
        provider_access_token=current_access,
        provider_refresh_token="plaintext-refresh-token",
        totp_secret=current_totp,
        recovery_code_verifier=recovery_verifier,
    )
    keyring = FakeKeyring(
        {
            ("uniquode.io", "SYSTEM_SECRET_KEY"): current_key,
            ("uniquode.io", "SYSTEM_SECRET_KEYS_PREVIOUS"): previous_key,
        }
    )
    _install_fake_keyring(monkeypatch, keyring)
    config_path = _reencrypt_app_config(tmp_path / "app.toml", database_url)

    result = await _reencrypt_configured_secrets(config_path, dry_run=False)
    payload = secret_cli._reencrypt_result_payload(result, target="system")
    assert payload["scanned"] == 3
    assert payload["rewritten"] == 0
    assert payload["skipped_current"] == 2
    assert payload["skipped_plaintext"] == 1
    values = await _reencrypt_database_values(database_url)
    assert values == {
        "access": current_access,
        "refresh": "plaintext-refresh-token",
        "totp": current_totp,
        "recovery": recovery_verifier,
    }
    output = json.dumps(payload)
    assert current_key not in output
    assert previous_key not in output
    assert current_access not in output
    assert current_totp not in output
    assert "access-token" not in output
    assert "plaintext-refresh-token" not in output
    assert "totp-secret" not in output


def test_blank_config_option_is_rejected() -> None:
    result = CliRunner().invoke(
        secret_cli.secret_command,
        ["--config", "   ", "list"],
    )

    assert result.exit_code == 2
    assert "--config must not be blank" in result.output
