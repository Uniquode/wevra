import base64
import csv
import io
import json
import logging
import sqlite3
import sys
import tomllib
import zlib
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager, closing
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from time import time
from types import SimpleNamespace

import click
import pytest
from click.testing import CliRunner
from cryptography.fernet import Fernet
from fastapi import FastAPI, Request
from tortoise.backends.base.client import BaseDBAsyncClient
from tortoise.exceptions import BaseORMException

import wybra.auth.admin.management as identity_management
import wybra.auth.cli.authmgr as authmgr
import wybra.auth.cli.authmgr.groups as authmgr_groups
import wybra.auth.cli.authmgr.output as authmgr_output
import wybra.auth.cli.authmgr.passwords as authmgr_passwords
import wybra.auth.cli.authmgr.runtime as authmgr_runtime
import wybra.auth.cli.authmgr.schema as authmgr_schema
import wybra.auth.cli.authmgr.scopes as authmgr_scopes
import wybra.auth.cli.authmgr.timestamps as authmgr_timestamps
import wybra.auth.cli.authmgr.users as authmgr_users
import wybra.auth.sessions as identity_sessions
import wybra.db.migrate as migrate_module
from support_database import sqlite_file_url
from wybra.auth import ERROR_EMAIL_VERIFICATION_REQUIRED, ERROR_INACTIVE_USER
from wybra.auth.accounts.manager import (
    InvalidPasswordException,
    UserAlreadyExists,
    UserManager,
    UserNotExists,
    create_user_manager,
)
from wybra.auth.accounts.schemas import UserCreate, UserUpdate
from wybra.auth.cli.authmgr.args import AuthmgrArgs
from wybra.auth.models import (
    AccessToken,
    Group,
    GroupScope,
    GroupUser,
    IdentityTotpCredential,
    IdentityTotpRecoveryCode,
    IdentityUserEmail,
    IdentityWebAuthnCredential,
    Scope,
    User,
)
from wybra.auth.options import PROVIDER, IdentityOptions
from wybra.auth.persistence import (
    TortoiseAuthPersistenceCapability,
    create_session_token_strategy,
    create_user_store,
)
from wybra.auth.persistence.contracts import AuthPersistenceCapability
from wybra.auth.settings import (
    APP_CONFIG_SECTION,
    AUTH_CONFIG_SECTION,
    AUTH_SETTINGS_OWNER,
    ENV_SESSION_LIFETIME,
    ENV_TOTP_ALLOWED_DRIFT,
    ENV_TOTP_CHALLENGE_EXPIRY_SECONDS,
    ENV_TOTP_PERIOD_SECONDS,
    ENV_TOTP_RECOVERY_WINDOW_SECONDS,
    PASSKEY_CONFIG_SECTION,
    PASSKEY_SECTION_FIELD,
    PASSWORD_POLICY_CONFIG_SECTION,
    PASSWORD_POLICY_SECTION_FIELD,
    PASSWORD_SECTION_FIELD,
    AuthSettings,
    load_auth_settings,
    load_runtime_auth_settings,
    validate_auth_settings,
)
from wybra.config import (
    AppConfigSource,
    ConfigService,
    ConfigSourceError,
    MappingConfigSource,
)
from wybra.core.composition import (
    AppConfig,
    AssetOptions,
    RouteOptions,
    TemplateOptions,
    load_app_config,
)
from wybra.core.config import RUNTIME_CONFIG_DEF
from wybra.core.exceptions import ConfigurationError
from wybra.db import DatabaseCapability
from wybra.db.capabilities import WybraDatabaseCapability
from wybra.db.persistence import Database, close_database, create_database
from wybra.db.urls import (
    SQLITE_MEMORY_DATABASE_URL,
    parse_sqlite_database_url,
    resolve_database_url,
)
from wybra.services.crypto import ENV_WYBRA_SECRET_KEY
from wybra.site import Site
from wybra.testing import create_test_database
from wybra.tools.app_startup import CONFIG_SOURCE_CONTEXT_KEY

STRONG_TEST_PASSWORD = "Correct horse 42!"
UPDATED_STRONG_TEST_PASSWORD = "New correct horse 42!"


async def run_authmgr_command(args: list[str]) -> int:
    invocation: tuple[AuthmgrArgs, str | None] | None = None

    def capture_invocation(ctx: click.Context, command_args: AuthmgrArgs) -> None:
        nonlocal invocation
        invocation = (
            command_args,
            authmgr_runtime._config_source_from_context(ctx),
        )
        ctx.exit()

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(authmgr_users, "_run_authmgr", capture_invocation)
        monkeypatch.setattr(authmgr_scopes, "_run_authmgr", capture_invocation)
        monkeypatch.setattr(authmgr_groups, "_run_authmgr", capture_invocation)
        exit_code = authmgr.main(args)

    if invocation is None:
        return exit_code
    command_args, config_source = invocation
    try:
        return await authmgr_runtime.run_authmgr(
            command_args,
            config_source=config_source,
        )
    except click.Abort:
        click.echo("Aborted!", err=True)
        return 1
    except click.ClickException as exc:
        exc.show()
        return int(exc.exit_code or 1)


def secret_key_entry_for_tests(version: str = "test") -> str:
    encoded_key = Fernet.generate_key().decode("ascii")
    raw_key = base64.urlsafe_b64decode(encoded_key)
    checksum = f"{(zlib.crc32(raw_key) & 0xFFFFFFFF):08x}"
    return f"{version}:{encoded_key}:{checksum}"


@dataclass(frozen=True, slots=True)
class AuthTestSettings:
    database_url: str
    identity_options: IdentityOptions = field(default_factory=IdentityOptions)


@dataclass(frozen=True, slots=True)
class MigrationTestSettings:
    database_url: str
    project_root: Path = Path.cwd()
    migrations_root: Path | None = None
    app_config: None = None

    @property
    def modules(self) -> tuple[str, ...]:
        return ("wybra.auth",)


@dataclass(slots=True)
class CaptureDelivery:
    verification_tokens: list[tuple[str, str]]

    async def send_verification_token(
        self,
        user: User,
        token: str,
        request: Request | None = None,
    ) -> None:
        self.verification_tokens.append((user.email, token))


class FailingVerificationDelivery:
    async def send_verification_token(
        self,
        user: User,
        token: str,
        request: Request | None = None,
    ) -> None:
        del user, token, request
        raise RuntimeError("verification delivery failed")


@dataclass(slots=True)
class ResetPasswordDelivery:
    reset_tokens: list[tuple[str, str]]

    async def send_reset_password_token(
        self,
        user: User,
        token: str,
        request: Request | None = None,
    ) -> None:
        del request
        self.reset_tokens.append((user.email, token))


def create_auth_test_app(
    *,
    database_url: str = SQLITE_MEMORY_DATABASE_URL,
    identity_options: IdentityOptions | None = None,
) -> FastAPI:
    options = identity_options or IdentityOptions()
    settings = AuthTestSettings(database_url=database_url, identity_options=options)
    app = FastAPI()
    app.state.settings = settings
    app.state.auth_settings = AuthSettings(
        database_url=database_url,
        identity_options=options,
    )
    site = Site(
        app=app,
        config=ConfigService(
            [
                MappingConfigSource(
                    {
                        "app": {
                            "modules": ("wybra.db", "wybra.auth"),
                            "database_url": database_url,
                        }
                    }
                )
            ]
        ),
    )
    app.state.test_database = None
    app.state.site = site
    return app


def _database_from_app(app: FastAPI) -> Database:
    database = app.state.test_database
    if not isinstance(database, Database):
        raise RuntimeError("Test database has not been initialised.")
    return database


async def initialise_app_identity_database(app: FastAPI) -> None:
    if app.state.test_database is None:
        database = await create_test_database(
            database_url=app.state.auth_settings.database_url,
            modules=("wybra.auth",),
        )
        app.state.test_database = database
        site = app.state.site
        site.provide_capability(
            DatabaseCapability,
            WybraDatabaseCapability(database),
        )
        site.provide_capability(
            AuthPersistenceCapability,
            TortoiseAuthPersistenceCapability(
                site.capability_proxy(DatabaseCapability)
            ),
        )


async def run_auth_app_test(
    app: FastAPI,
    callback: Callable[[], Awaitable[None]],
) -> None:
    try:
        await callback()
    finally:
        if app.state.test_database is not None:
            await close_database(_database_from_app(app))


def _connection_from_app(app: FastAPI) -> BaseDBAsyncClient:
    return _database_from_app(app).connection()


@asynccontextmanager
async def app_connection_scope(app: FastAPI) -> AsyncIterator[BaseDBAsyncClient]:
    with _database_from_app(app).context:
        yield _connection_from_app(app)


def run_auth_migration(argv: list[str]) -> int:
    def load_settings(database_url: str | None) -> MigrationTestSettings:
        if database_url is None:
            raise migrate_module.MigrationConfigurationError(
                "Test database URL is required."
            )

        return MigrationTestSettings(database_url=database_url)

    command = migrate_module.create_migrate_command(load_settings)
    return migrate_module.run_migrate_command(command, argv)


def write_auth_app_toml(
    config_path: Path,
    *auth_lines: str,
    database_url: str = "sqlite:///auth.sqlite3",
) -> Path:
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(
        "\n".join(
            [
                "[app]",
                f'database_url = "{database_url}"',
                'modules = ["wybra.auth"]',
                "",
                "[app.templates]",
                "auto_reload = true",
                "cache_size = 0",
                "",
                "[app.assets]",
                'url_path = "/static/"',
                'root = "static"',
                "",
                "[auth]",
                *auth_lines,
            ]
        ),
        encoding="utf-8",
    )
    return config_path


def load_auth_test_app_config(
    config_path: Path,
    *auth_lines: str,
    database_url: str = "sqlite:///auth.sqlite3",
) -> AppConfig:
    return load_app_config(
        project_root=config_path.parent,
        config_path=write_auth_app_toml(
            config_path,
            *auth_lines,
            database_url=database_url,
        ),
    )


def auth_settings_config(app_config: AppConfig) -> ConfigService:
    return ConfigService(
        [AppConfigSource(app_config)],
        config_defs=(RUNTIME_CONFIG_DEF, AuthSettings.module_config),
        discover_module_config=False,
    )


@pytest.fixture(autouse=True)
def _authmgr_app_config(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path = write_auth_app_toml(tmp_path / "app.toml")
    monkeypatch.setenv("APP_CONFIG", str(config_path))
    monkeypatch.setenv(ENV_WYBRA_SECRET_KEY, secret_key_entry_for_tests())
    monkeypatch.delenv("AUTH_CONFIG", raising=False)


def set_authmgr_database_url(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    database_url: str,
) -> None:
    config_path = write_auth_app_toml(tmp_path / "app.toml", database_url=database_url)
    monkeypatch.setenv("APP_CONFIG", str(config_path))


async def initialise_identity_database(database_url: str) -> None:
    async def initialise() -> None:
        database = await create_test_database(
            database_url=database_url,
            modules=("wybra.auth",),
        )
        await close_database(database)

    await initialise()


async def _with_identity_connection[T](
    database_url: str,
    callback: Callable[[BaseDBAsyncClient], Awaitable[T]],
) -> T:
    database = await create_database(database_url, modules=("wybra.auth",))
    try:
        return await callback(database.connection())
    finally:
        await close_database(database)


async def _with_generated_identity_connection[T](
    database_url: str,
    callback: Callable[[BaseDBAsyncClient], Awaitable[T]],
) -> T:
    database = await create_test_database(
        database_url=database_url,
        modules=("wybra.auth",),
    )
    try:
        return await callback(database.connection())
    finally:
        await close_database(database)


def initialise_legacy_identity_database(database_path: Path) -> None:
    with closing(sqlite3.connect(database_path)) as connection, connection:
        connection.execute(
            """
            CREATE TABLE identity_user (
                id CHAR(32) NOT NULL PRIMARY KEY,
                email VARCHAR(320) NOT NULL,
                hashed_password VARCHAR(1024) NOT NULL,
                is_active BOOLEAN NOT NULL,
                is_superuser BOOLEAN NOT NULL,
                is_verified BOOLEAN NOT NULL
            )
            """
        )


def sqlite_table_names(database_path: Path) -> set[str]:
    with closing(sqlite3.connect(database_path)) as connection:
        return {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }


def sqlite_table_columns(database_path: Path, table_name: str) -> set[str]:
    quoted_table_name = _quote_sqlite_identifier(table_name)
    with closing(sqlite3.connect(database_path)) as connection:
        return {
            row[1]
            for row in connection.execute(f"PRAGMA table_info({quoted_table_name})")
        }


def sqlite_table_indexes(database_path: Path, table_name: str) -> set[str]:
    quoted_table_name = _quote_sqlite_identifier(table_name)
    with closing(sqlite3.connect(database_path)) as connection:
        return {
            row[1]
            for row in connection.execute(f"PRAGMA index_list({quoted_table_name})")
        }


def _quote_sqlite_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


async def identity_users_from_database(database_url: str) -> list[User]:
    return await _with_identity_connection(
        database_url,
        lambda connection: User.all().using_db(connection).order_by("email"),
    )


async def identity_user_from_database(database_url: str, email: str) -> User | None:
    return await _with_identity_connection(
        database_url,
        lambda connection: User.get_or_none(email=email, using_db=connection),
    )


async def identity_user_emails_from_database(
    database_url: str,
) -> list[IdentityUserEmail]:
    return await _with_identity_connection(
        database_url,
        lambda connection: IdentityUserEmail.all().using_db(connection),
    )


async def totp_credentials_from_database(
    database_url: str,
    email: str,
) -> list[IdentityTotpCredential]:
    async def load_credentials() -> list[IdentityTotpCredential]:
        return await _with_identity_connection(database_url, _load)

    async def _load(connection: BaseDBAsyncClient) -> list[IdentityTotpCredential]:
        user = await User.get(email=email, using_db=connection)
        return list(
            await IdentityTotpCredential.filter(user_id=user.id)
            .using_db(connection)
            .order_by("created_at")
        )

    return await load_credentials()


async def totp_recovery_codes_from_database(
    database_url: str,
    credential: IdentityTotpCredential,
) -> list[IdentityTotpRecoveryCode]:
    return await _with_identity_connection(
        database_url,
        lambda connection: (
            IdentityTotpRecoveryCode.filter(
                credential_id=credential.id,
            )
            .using_db(connection)
            .order_by("created_at")
        ),
    )


async def add_webauthn_credential_to_database(
    database_url: str,
    email: str,
    *,
    credential_id: str,
    label: str | None = None,
    status: str = "active",
) -> str:
    async def add_credential() -> str:
        return await _with_identity_connection(database_url, _add)

    async def _add(connection: BaseDBAsyncClient) -> str:
        user = await User.get(email=email, using_db=connection)
        now = time()
        credential = await IdentityWebAuthnCredential.create(
            user_id=user.id,
            credential_id=credential_id,
            public_key=b"public-key",
            sign_count=1,
            status=status,
            label=label,
            created_at=now,
            revoked_at=now if status == "revoked" else None,
            user_verified=True,
            credential_device_type="multi_device",
            credential_backed_up=True,
            transports=["internal"],
            using_db=connection,
        )
        return str(credential.id)

    return await add_credential()


async def webauthn_credentials_from_database(
    database_url: str,
    email: str,
) -> list[IdentityWebAuthnCredential]:
    async def load_credentials() -> list[IdentityWebAuthnCredential]:
        return await _with_identity_connection(database_url, _load)

    async def _load(connection: BaseDBAsyncClient) -> list[IdentityWebAuthnCredential]:
        user = await User.get(email=email, using_db=connection)
        return list(
            await IdentityWebAuthnCredential.filter(user_id=user.id)
            .using_db(connection)
            .order_by("created_at")
        )

    return await load_credentials()


async def access_tokens_from_database(database_url: str) -> list[str]:
    async def load_tokens(connection: BaseDBAsyncClient) -> list[str]:
        return [
            token.token
            for token in await AccessToken.all().using_db(connection).order_by("token")
        ]

    return await _with_identity_connection(database_url, load_tokens)


async def scopes_from_database(database_url: str) -> list[Scope]:
    return await _with_identity_connection(
        database_url,
        lambda connection: Scope.all().using_db(connection).order_by("scope"),
    )


async def group_from_database(database_url: str, abbrev: str) -> Group:
    return await _with_identity_connection(
        database_url,
        lambda connection: Group.get(abbrev=abbrev, using_db=connection),
    )


async def group_scopes_from_database(database_url: str, abbrev: str) -> list[str]:
    async def load_group_scopes() -> list[str]:
        return await _with_identity_connection(database_url, _load)

    async def _load(connection: BaseDBAsyncClient) -> list[str]:
        group = await Group.get(abbrev=abbrev, using_db=connection)
        return list(
            await GroupScope.filter(group_id=group.id)
            .using_db(connection)
            .order_by("scope")
            .values_list("scope", flat=True)
        )

    return await load_group_scopes()


async def user_group_abbrevs_from_database(database_url: str, email: str) -> list[str]:
    async def load_user_groups() -> list[str]:
        return await _with_identity_connection(database_url, _load)

    async def _load(connection: BaseDBAsyncClient) -> list[str]:
        user = await User.get(email=email, using_db=connection)
        group_ids = (
            await GroupUser.filter(user_id=user.id)
            .using_db(connection)
            .values_list("group_id", flat=True)
        )
        return sorted(
            await Group.filter(id__in=tuple(group_ids))
            .using_db(connection)
            .values_list("abbrev", flat=True)
        )

    return await load_user_groups()


async def create_session_token_for_user(database_url: str, email: str) -> str:
    settings = AuthTestSettings(database_url=database_url)

    async def create_token() -> str:
        return await _with_identity_connection(database_url, _create)

    async def _create(connection: BaseDBAsyncClient) -> str:
        user = await User.get(email=email, using_db=connection)
        strategy = create_session_token_strategy(connection, settings.identity_options)
        return await strategy.write_token(user)

    return await create_token()


async def update_user_fields(database_url: str, email: str, **values: object) -> None:
    async def update_user() -> None:
        await _with_identity_connection(database_url, _update)

    async def _update(connection: BaseDBAsyncClient) -> None:
        user = await User.get(email=email, using_db=connection)
        for field_name, value in values.items():
            setattr(user, field_name, value)
        await user.save(using_db=connection)

    await update_user()


class TestAuthmgrBehaviour:
    def test_authmgr_project_script_is_defined(self) -> None:
        data = tomllib.loads(Path("pyproject.toml").read_text(encoding="utf-8"))

        assert (
            data["project"]["scripts"]["wybra-authmgr"] == "wybra.auth.cli.authmgr:main"
        )

    def test_authmgr_create_positional_is_email(self) -> None:
        result = CliRunner().invoke(
            authmgr.authmgr_command,
            ["user", "create", "--help"],
        )

        assert result.exit_code == 0
        assert "EMAIL" in result.output
        assert "TARGET" not in result.output

    def test_authmgr_update_positional_is_target(self) -> None:
        result = CliRunner().invoke(
            authmgr.authmgr_command,
            ["user", "update", "--help"],
        )

        assert result.exit_code == 0
        assert "TARGET" in result.output
        assert "EMAIL" not in result.output

    @pytest.mark.anyio
    @pytest.mark.parametrize(
        ("config_mode", "config_relative_path", "email", "auth_lines"),
        [
            (
                "env",
                Path("configured/app.toml"),
                "configured@example.com",
                ('session_cookie_name = "auth_session"',),
            ),
            (
                "option",
                Path("app.toml"),
                "option-config@example.com",
                (),
            ),
        ],
        ids=("app-config-env", "config-option"),
    )
    async def test_authmgr_loads_app_auth_configuration(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        config_mode: str,
        config_relative_path: Path,
        email: str,
        auth_lines: tuple[str, ...],
    ) -> None:
        database_path = tmp_path / f"{config_mode}-auth.sqlite3"
        database_url = sqlite_file_url(database_path)
        await initialise_identity_database(database_url)
        config_path = write_auth_app_toml(
            tmp_path / config_relative_path,
            *auth_lines,
            database_url=database_url,
        )
        if config_mode == "env":
            monkeypatch.setenv("APP_CONFIG", str(config_path))
            args_prefix: list[str] = []
        else:
            monkeypatch.chdir(tmp_path)
            monkeypatch.delenv("APP_CONFIG", raising=False)
            args_prefix = ["--config", str(config_path)]
        monkeypatch.setattr(sys, "stdin", io.StringIO(f"{STRONG_TEST_PASSWORD}\n"))

        assert (
            await run_authmgr_command(
                [*args_prefix, "user", "create", email, "--password", "-"]
            )
            == 0
        )

        [user] = await identity_users_from_database(database_url)
        assert user.email == email

    def test_authmgr_command_secret_service_uses_configured_crypto_source(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        configured_key_name = "AUTHMGR_CONFIGURED_SECRET_KEY"
        monkeypatch.setenv(ENV_WYBRA_SECRET_KEY, secret_key_entry_for_tests())
        monkeypatch.setenv(
            configured_key_name, secret_key_entry_for_tests("configured")
        )
        config_path = write_auth_app_toml(tmp_path / "app.toml")
        with config_path.open("a", encoding="utf-8") as handle:
            handle.write(
                f"""

    [secrets.crypto]
    source = "environment"
    current_key = "{configured_key_name}"
    """
            )
        app_config = load_app_config(project_root=tmp_path, config_path=config_path)

        service = authmgr_runtime._secret_envelope_service_for_command(app_config)

        assert service.current_version_required() == "configured"

    @pytest.mark.anyio
    async def test_authmgr_config_option_overrides_app_config_env(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        selected_database_url = sqlite_file_url(tmp_path / "selected-auth.sqlite3")
        await initialise_identity_database(selected_database_url)
        ambient_config = write_auth_app_toml(
            tmp_path / "ambient" / "app.toml",
            database_url=sqlite_file_url(tmp_path / "ambient-auth.sqlite3"),
        )
        selected_config = write_auth_app_toml(
            tmp_path / "selected" / "app.toml",
            database_url=selected_database_url,
        )
        monkeypatch.setenv("APP_CONFIG", str(ambient_config))
        monkeypatch.chdir(tmp_path)

        exit_code = await run_authmgr_command(
            ["--config", str(selected_config), "user", "list"]
        )

        captured = capsys.readouterr()
        assert exit_code == 0
        assert captured.err == ""

    def test_authmgr_rejects_blank_config_option(
        self,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        exit_code = authmgr.main(["--config", "   ", "user", "list"])

        captured = capsys.readouterr()
        assert exit_code == 1
        assert "--config must not be blank" in captured.err

    def test_authmgr_config_source_context_ignores_non_dict_context(self) -> None:
        ctx = click.Context(authmgr.authmgr_command, obj=object())

        assert authmgr_runtime._config_source_from_context(ctx) is None

    def test_authmgr_config_source_context_requires_string_value(self) -> None:
        ctx = click.Context(
            authmgr.authmgr_command,
            obj={CONFIG_SOURCE_CONTEXT_KEY: object()},
        )

        with pytest.raises(ConfigurationError, match="config_source must be a string"):
            authmgr_runtime._config_source_from_context(ctx)

    def test_authmgr_rejects_missing_app_config_even_when_auth_toml_exists(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        case_root = tmp_path / "auth-only"
        case_root.mkdir()
        (case_root / "auth.toml").write_text(
            "\n".join(
                [
                    "[auth]",
                    'database_url = "sqlite:///auth.sqlite3"',
                ]
            ),
            encoding="utf-8",
        )
        monkeypatch.chdir(case_root)
        monkeypatch.delenv("APP_CONFIG", raising=False)
        monkeypatch.setenv("AUTH_CONFIG", str(case_root / "auth.toml"))
        stdin = io.StringIO(f"{STRONG_TEST_PASSWORD}\n")
        monkeypatch.setattr(sys, "stdin", stdin)

        exit_code = authmgr.main(
            ["user", "create", "missing-config@example.com", "--password", "-"]
        )

        assert exit_code == 1
        assert stdin.tell() == 0
        captured = capsys.readouterr()
        assert "pass --config or set APP_CONFIG" in captured.err

    @pytest.mark.parametrize(
        ("environ_template", "expected_url"),
        [
            pytest.param(
                {"DATABASE_URL": "database_env"},
                "database_env",
                id="database-env-used",
            ),
            pytest.param(
                {"AUTH_DATABASE_URL": "auth_env"},
                "config",
                id="auth-env-ignored",
            ),
        ],
    )
    def test_app_database_url_precedence(
        self,
        tmp_path: Path,
        environ_template: dict[str, str],
        expected_url: str,
    ) -> None:
        urls = {
            "auth_env": sqlite_file_url(tmp_path / "auth-env.sqlite3"),
            "config": sqlite_file_url(tmp_path / "auth.sqlite3"),
            "database_env": sqlite_file_url(tmp_path / "database-env.sqlite3"),
        }
        app_config = load_auth_test_app_config(
            tmp_path / "app.toml",
            database_url=urls["config"],
        )
        environ = {
            key: urls[value] if value in urls else value
            for key, value in environ_template.items()
        }
        ConfigService.set_runtime_environment(environ)

        settings = load_auth_settings(
            auth_settings_config(app_config),
            app_config=app_config,
        )

        assert settings.database_url == urls[expected_url]

    def test_app_database_url_rejects_blank_database_env(self, tmp_path: Path) -> None:
        app_config = load_auth_test_app_config(
            tmp_path / "app.toml",
            database_url=sqlite_file_url(tmp_path / "auth.sqlite3"),
        )
        ConfigService.set_runtime_environment({"DATABASE_URL": "   "})

        with pytest.raises(ConfigurationError, match="DATABASE_URL must not be blank"):
            load_auth_settings(
                auth_settings_config(app_config),
                app_config=app_config,
            )

    def test_app_database_url_resolves_relative_sqlite_path_from_project_root(
        self,
        tmp_path: Path,
    ) -> None:
        config_path = tmp_path / "config" / "app.toml"
        write_auth_app_toml(
            config_path,
            database_url="sqlite:///relative-auth.sqlite3",
        )
        app_config = load_app_config(project_root=tmp_path, config_path=config_path)

        settings = load_auth_settings(
            auth_settings_config(app_config),
            app_config=app_config,
        )

        assert settings.database_url == sqlite_file_url(
            tmp_path / "relative-auth.sqlite3"
        )

    def test_auth_settings_use_structured_database_config(self, tmp_path: Path) -> None:
        config_path = tmp_path / "app.toml"
        config_path.write_text(
            """
    [app]
    modules = ["wybra.auth"]

    [app.templates]
    auto_reload = true
    cache_size = 0

    [app.assets]
    url_path = "/static/"

    [app.database]
    backend = "sqlite"
    database = "structured-auth.sqlite3"
    """.strip(),
            encoding="utf-8",
        )
        app_config = load_app_config(project_root=tmp_path, config_path=config_path)

        settings = load_auth_settings(
            auth_settings_config(app_config),
            app_config=app_config,
        )

        assert settings.database_url is None
        assert settings.database_connection is not None
        assert (
            settings.database_connection.credentials["file_path"]
            == (tmp_path / "structured-auth.sqlite3").resolve().as_posix()
        )

    def test_runtime_auth_settings_use_structured_environment_database_credentials(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        config_path = tmp_path / "app.toml"
        config_path.write_text(
            """
    [app]
    modules = ["wybra.auth"]

    [app.templates]
    auto_reload = true
    cache_size = 0

    [app.assets]
    url_path = "/static/"

    [app.database]
    backend = "postgresql"
    database = "uniquode"
    credential_source = "environment"
    user_key = "UNIQUODE_DB_USER"
    password_key = "UNIQUODE_DB_PASSWORD"
    """.strip(),
            encoding="utf-8",
        )
        app_config = load_app_config(project_root=tmp_path, config_path=config_path)
        monkeypatch.delenv("UNIQUODE_DB_USER", raising=False)
        monkeypatch.delenv("UNIQUODE_DB_PASSWORD", raising=False)
        ConfigService.set_runtime_environment(
            {
                "UNIQUODE_DB_USER": "app_user",
                "UNIQUODE_DB_PASSWORD": "app_password",
            }
        )

        settings = load_runtime_auth_settings(
            app_config=app_config,
            deployment_environment="local",
        )

        assert settings.database_connection is not None
        connection_config = settings.database_connection.tortoise_connection_config
        assert isinstance(connection_config, dict)
        assert connection_config == {
            "engine": "tortoise.backends.asyncpg",
            "credentials": {
                "database": "uniquode",
                "user": "app_user",
                "password": "app_password",
            },
        }

    def test_app_database_url_error_names_app_config_section(
        self, tmp_path: Path
    ) -> None:
        app_config = AppConfig(
            config_path=tmp_path / "app.toml",
            project_root=tmp_path,
            modules=("wybra.auth",),
            routes=RouteOptions(),
            templates=TemplateOptions(auto_reload=True, cache_size=0),
            assets=AssetOptions(url_path="/static/"),
        )

        with pytest.raises(ConfigurationError, match=r"\[app\]\.database_url"):
            load_auth_settings(
                auth_settings_config(app_config),
                app_config=app_config,
            )

    def test_app_auth_config_rejects_unknown_auth_options(self, tmp_path: Path) -> None:
        app_config = load_auth_test_app_config(
            tmp_path / "app.toml",
            "session_lifetme_seconds = 3600",
        )

        with pytest.raises(ConfigurationError, match="session_lifetme_seconds"):
            load_auth_settings(
                auth_settings_config(app_config),
                app_config=app_config,
            )

    def test_app_auth_config_rejects_stale_auth_database_url(
        self, tmp_path: Path
    ) -> None:
        app_config = load_auth_test_app_config(
            tmp_path / "app.toml",
            'database_url = "sqlite:///stale-auth.sqlite3"',
        )

        with pytest.raises(ConfigurationError, match="database_url"):
            load_auth_settings(
                auth_settings_config(app_config),
                app_config=app_config,
            )

    def test_app_auth_config_applies_identity_env_overrides(
        self, tmp_path: Path
    ) -> None:
        app_config = load_auth_test_app_config(
            tmp_path / "app.toml",
            "provider_enabled = true",
            'totp_mode = "disabled"',
            "passkey_enabled = true",
        )
        ConfigService.set_runtime_environment(
            {
                "PROVIDER_ENABLED": "false",
                "TOTP_MODE": "opt_in",
                "PASSKEY_ENABLED": "false",
                ENV_TOTP_ALLOWED_DRIFT: "2",
                ENV_TOTP_PERIOD_SECONDS: "60",
                ENV_TOTP_CHALLENGE_EXPIRY_SECONDS: "450",
                ENV_TOTP_RECOVERY_WINDOW_SECONDS: "900",
            }
        )

        settings = load_auth_settings(
            auth_settings_config(app_config),
            app_config=app_config,
        )

        assert settings.identity_options.provider_enabled is False
        assert settings.identity_options.totp_mode == "opt_in"
        assert settings.identity_options.passkey_enabled is False
        assert settings.identity_options.totp_allowed_drift == 2
        assert settings.identity_options.totp_period_seconds == 60
        assert settings.identity_options.totp_challenge_expiry_seconds == 450
        assert settings.identity_options.totp_recovery_window_seconds == 900

    def test_app_auth_configures_passkey_options(self, tmp_path: Path) -> None:
        app_config = load_auth_test_app_config(
            tmp_path / "app.toml",
            "passkey_enabled = true",
            "",
            "[auth.passkeys]",
            'rp_id = "app.example.com"',
            'rp_name = "Example App"',
            'allowed_origins = ["https://app.example.com/"]',
            "timeout_seconds = 180",
            'user_verification = "required"',
            "user_verification_satisfies_totp = false",
            'attestation = "none"',
            'discoverable_credentials = "required"',
            'counter_policy = "reject-regression"',
        )

        settings = load_auth_settings(
            auth_settings_config(app_config),
            app_config=app_config,
        )

        assert settings.identity_options.passkey_enabled is True
        assert settings.identity_options.passkey_rp_id == "app.example.com"
        assert settings.identity_options.passkey_rp_name == "Example App"
        assert settings.identity_options.passkey_allowed_origins == (
            "https://app.example.com",
        )
        assert settings.identity_options.passkey_timeout_seconds == 180
        assert settings.identity_options.passkey_user_verification == "required"
        assert (
            settings.identity_options.passkey_user_verification_satisfies_totp is False
        )
        assert settings.identity_options.passkey_discoverable_credentials == "required"
        assert settings.identity_options.passkey_counter_policy == "reject-regression"

    def test_auth_settings_merges_nested_passkey_config(
        self,
        tmp_path: Path,
    ) -> None:
        config_path = tmp_path / "app.toml"
        app_config = load_auth_test_app_config(
            config_path,
            "passkey_enabled = true",
            "",
            "[auth.passkeys]",
            'rp_id = "app.example.com"',
            'rp_name = "Example App"',
        )
        config = ConfigService(
            [
                MappingConfigSource(
                    {
                        APP_CONFIG_SECTION: {
                            "database_url": "sqlite:///from-config.sqlite3",
                        },
                        AUTH_CONFIG_SECTION: {
                            PASSKEY_SECTION_FIELD: {
                                "allowed_origins": ["https://login.example.com"],
                                "user_verification": "required",
                                "user_verification_satisfies_totp": False,
                            }
                        },
                    }
                )
            ],
            discover_module_config=False,
        )

        settings = load_auth_settings(
            config,
            app_config=app_config,
        )

        assert settings.identity_options.passkey_rp_id == "app.example.com"
        assert settings.identity_options.passkey_rp_name == "Example App"
        assert settings.identity_options.passkey_allowed_origins == (
            "https://login.example.com",
        )
        assert settings.identity_options.passkey_user_verification == "required"
        assert (
            settings.identity_options.passkey_user_verification_satisfies_totp is False
        )

    def test_auth_settings_uses_section_passkey_config_when_inline_config_missing(
        self,
        tmp_path: Path,
    ) -> None:
        config_path = tmp_path / "app.toml"
        app_config = load_auth_test_app_config(
            config_path,
            "passkey_enabled = true",
        )
        config = ConfigService(
            [
                MappingConfigSource(
                    {
                        APP_CONFIG_SECTION: {
                            "database_url": "sqlite:///from-config.sqlite3",
                        },
                        PASSKEY_CONFIG_SECTION: {
                            "rp_id": "app.example.com",
                            "rp_name": "Example App",
                            "allowed_origins": ["https://app.example.com"],
                        },
                    }
                )
            ],
            discover_module_config=False,
        )

        settings = load_auth_settings(
            config,
            app_config=app_config,
        )

        assert settings.identity_options.passkey_rp_id == "app.example.com"
        assert settings.identity_options.passkey_rp_name == "Example App"
        assert settings.identity_options.passkey_allowed_origins == (
            "https://app.example.com",
        )

    def test_app_auth_rejects_unknown_passkey_options(self, tmp_path: Path) -> None:
        app_config = load_auth_test_app_config(
            tmp_path / "app.toml",
            "passkey_enabled = true",
            "",
            "[auth.passkeys]",
            'rp_id = "app.example.com"',
            'rp_name = "Example App"',
            'allowed_origins = ["https://app.example.com"]',
            'credential_policy = "strict"',
        )

        with pytest.raises(ConfigurationError, match="credential_policy"):
            load_auth_settings(
                auth_settings_config(app_config),
                app_config=app_config,
            )

    @pytest.mark.parametrize(
        ("setting_name", "config_line"),
        [
            ("session_lifetime_seconds", "session_lifetime_seconds = 0"),
            ("totp_period_seconds", "totp_period_seconds = 0"),
            ("totp_challenge_expiry_seconds", "totp_challenge_expiry_seconds = -1"),
            ("totp_recovery_window_seconds", "totp_recovery_window_seconds = 0"),
        ],
    )
    def test_app_auth_config_rejects_non_positive_duration_settings(
        self,
        tmp_path: Path,
        setting_name: str,
        config_line: str,
    ) -> None:
        app_config = load_auth_test_app_config(tmp_path / "app.toml", config_line)

        with pytest.raises(
            ConfigSourceError,
            match=rf"auth\.{setting_name} is invalid: .*positive integer",
        ):
            load_auth_settings(
                auth_settings_config(app_config),
                app_config=app_config,
            )

    @pytest.mark.parametrize(
        ("env_name", "env_value", "setting_name"),
        [
            (ENV_SESSION_LIFETIME, "0", "session_lifetime_seconds"),
            (ENV_TOTP_PERIOD_SECONDS, "0", "totp_period_seconds"),
            (
                ENV_TOTP_CHALLENGE_EXPIRY_SECONDS,
                "-1",
                "totp_challenge_expiry_seconds",
            ),
            (ENV_TOTP_RECOVERY_WINDOW_SECONDS, "0", "totp_recovery_window_seconds"),
        ],
    )
    def test_app_auth_env_rejects_non_positive_duration_settings(
        self,
        tmp_path: Path,
        env_name: str,
        env_value: str,
        setting_name: str,
    ) -> None:
        app_config = load_auth_test_app_config(tmp_path / "app.toml")
        ConfigService.set_runtime_environment({env_name: env_value})

        with pytest.raises(
            ConfigSourceError,
            match=rf"auth\.{setting_name} is invalid: .*positive integer",
        ):
            load_auth_settings(
                auth_settings_config(app_config),
                app_config=app_config,
            )

    def test_auth_settings_load_from_central_config_provider(
        self, tmp_path: Path
    ) -> None:
        config_path = tmp_path / "app.toml"
        app_config = load_auth_test_app_config(config_path)
        config = ConfigService(
            [
                MappingConfigSource(
                    {
                        APP_CONFIG_SECTION: {
                            "database_url": "sqlite:///from-config.sqlite3",
                        },
                        AUTH_CONFIG_SECTION: {
                            "provider_enabled": True,
                            "totp_mode": "required",
                        },
                    }
                )
            ],
            discover_module_config=False,
        )

        settings = load_auth_settings(
            config,
            app_config=app_config,
        )

        assert settings.database_url == sqlite_file_url(
            app_config.project_root / "from-config.sqlite3"
        )
        assert settings.owner == AUTH_SETTINGS_OWNER
        assert settings.integration_enabled(PROVIDER) is True
        assert settings.is_totp_enabled() is True

    def test_auth_settings_rejects_unknown_loaded_auth_options_when_app_auth_exists(
        self,
        tmp_path: Path,
    ) -> None:
        config_path = tmp_path / "app.toml"
        app_config = load_auth_test_app_config(
            config_path,
            "provider_enabled = true",
        )
        config = ConfigService(
            [
                MappingConfigSource(
                    {
                        APP_CONFIG_SECTION: {
                            "database_url": "sqlite:///from-config.sqlite3",
                        },
                        AUTH_CONFIG_SECTION: {
                            "session_lifetme_seconds": 3600,
                        },
                    }
                )
            ],
            discover_module_config=False,
        )

        with pytest.raises(ConfigurationError, match="session_lifetme_seconds"):
            load_auth_settings(
                config,
                app_config=app_config,
            )

    def test_auth_settings_rejects_non_table_app_config_auth(
        self, tmp_path: Path
    ) -> None:
        config_path = tmp_path / "app.toml"
        app_config = load_auth_test_app_config(config_path)
        object.__setattr__(app_config, "auth", ["not-a-table"])
        config = ConfigService(
            [
                MappingConfigSource(
                    {
                        APP_CONFIG_SECTION: {
                            "database_url": "sqlite:///from-config.sqlite3",
                        },
                    }
                )
            ],
            discover_module_config=False,
        )

        with pytest.raises(ConfigurationError, match=r"\[auth\] must be a table"):
            load_auth_settings(
                config,
                app_config=app_config,
            )

    def test_auth_settings_public_values_are_immutable(self) -> None:
        settings = AuthSettings(
            database_url=SQLITE_MEMORY_DATABASE_URL,
            identity_options=IdentityOptions(password_common_fragments=["example"]),
        )

        with pytest.raises(AttributeError):
            settings.database_url = "sqlite:///mutated.sqlite3"  # type: ignore[misc]

        with pytest.raises(AttributeError):
            settings.identity_options.password_common_fragments = ("mutated",)  # type: ignore[misc]

        assert settings.identity_options.password_common_fragments == ("example",)

    def test_auth_settings_validation_allows_local_secret_sentinels(self) -> None:
        settings = AuthSettings(database_url=SQLITE_MEMORY_DATABASE_URL)

        validate_auth_settings(settings)

    def test_direct_auth_settings_reject_relative_sqlite_database_url(self) -> None:
        with pytest.raises(
            ConfigurationError,
            match="Relative SQLite database URLs require an application project root",
        ):
            AuthSettings(database_url="sqlite:///relative-auth.sqlite3")

    def test_auth_settings_rejects_unknown_deployment_environment(self) -> None:
        with pytest.raises(
            ConfigurationError,
            match="Deployment environment must be one of: local, staging, production.",
        ):
            AuthSettings(
                database_url=SQLITE_MEMORY_DATABASE_URL,
                deployment_environment="prod",
            )

    def test_auth_settings_validation_requires_non_local_token_secrets(self) -> None:
        settings = AuthSettings(
            database_url=SQLITE_MEMORY_DATABASE_URL,
            deployment_environment="production",
        )

        with pytest.raises(
            ConfigurationError,
            match="Non-local deployments must configure identity reset",
        ):
            validate_auth_settings(settings)

    def test_auth_settings_validation_requires_non_local_secure_session_cookie(
        self,
    ) -> None:
        settings = AuthSettings(
            database_url=SQLITE_MEMORY_DATABASE_URL,
            deployment_environment="production",
            identity_options=IdentityOptions(
                reset_password_token_secret="configured-reset-secret",
                verification_token_secret="configured-verification-secret",
                session_cookie_force_secure=False,
            ),
        )

        with pytest.raises(
            ConfigurationError,
            match="Non-local deployments must force secure session cookies",
        ):
            validate_auth_settings(settings)

    def test_auth_settings_validation_accepts_non_local_auth_policy(self) -> None:
        settings = AuthSettings(
            database_url=SQLITE_MEMORY_DATABASE_URL,
            deployment_environment="production",
            identity_options=IdentityOptions(
                reset_password_token_secret="configured-reset-secret",
                verification_token_secret="configured-verification-secret",
                session_cookie_force_secure=True,
            ),
        )

        validate_auth_settings(settings)

    def test_app_auth_configures_default_password_policy(self, tmp_path: Path) -> None:
        app_config = load_auth_test_app_config(
            tmp_path / "app.toml",
            "session_cookie_force_secure = true",
            "",
            "[auth.password.policy]",
            "minimum_length = 8",
            "minimum_strength = 0.25",
            "minimum_character_categories = 1",
            'common_fragments = ["example"]',
        )

        settings = load_auth_settings(
            auth_settings_config(app_config),
            app_config=app_config,
        )

        assert settings.identity_options.session_cookie_force_secure is True
        policy = settings.identity_options.resolved_password_policy()
        assert policy.minimum_length == 8
        assert policy.minimum_score == 0.25
        assert policy.minimum_character_categories == 1
        assert policy.common_fragments == ("example",)

    def test_auth_settings_merges_nested_password_policy_config(
        self,
        tmp_path: Path,
    ) -> None:
        config_path = tmp_path / "app.toml"
        app_config = load_auth_test_app_config(
            config_path,
            "",
            "[auth.password.policy]",
            "minimum_length = 8",
            "minimum_strength = 0.25",
        )
        config = ConfigService(
            [
                MappingConfigSource(
                    {
                        APP_CONFIG_SECTION: {
                            "database_url": "sqlite:///from-config.sqlite3",
                        },
                        AUTH_CONFIG_SECTION: {
                            PASSWORD_SECTION_FIELD: {
                                PASSWORD_POLICY_SECTION_FIELD: {
                                    "minimum_strength": 0.5,
                                    "common_fragments": ["example"],
                                }
                            }
                        },
                    }
                )
            ],
            discover_module_config=False,
        )

        settings = load_auth_settings(
            config,
            app_config=app_config,
        )

        policy = settings.identity_options.resolved_password_policy()
        assert policy.minimum_length == 8
        assert policy.minimum_score == 0.5
        assert policy.common_fragments == ("example",)

    def test_auth_settings_uses_section_password_policy_when_inline_policy_missing(
        self,
        tmp_path: Path,
    ) -> None:
        config_path = tmp_path / "app.toml"
        app_config = load_auth_test_app_config(config_path)
        config = ConfigService(
            [
                MappingConfigSource(
                    {
                        APP_CONFIG_SECTION: {
                            "database_url": "sqlite:///from-config.sqlite3",
                        },
                        AUTH_CONFIG_SECTION: {
                            PASSWORD_SECTION_FIELD: {},
                        },
                        PASSWORD_POLICY_CONFIG_SECTION: {
                            "minimum_length": 10,
                        },
                    }
                )
            ],
            discover_module_config=False,
        )

        settings = load_auth_settings(
            config,
            app_config=app_config,
        )

        assert settings.identity_options.resolved_password_policy().minimum_length == 10

    def test_auth_settings_rejects_conflicting_password_config_shapes(
        self,
        tmp_path: Path,
    ) -> None:
        config_path = tmp_path / "app.toml"
        app_config = load_auth_test_app_config(
            config_path,
            "",
            "[auth.password.policy]",
            "minimum_length = 8",
        )
        config = ConfigService(
            [
                MappingConfigSource(
                    {
                        APP_CONFIG_SECTION: {
                            "database_url": "sqlite:///from-config.sqlite3",
                        },
                        AUTH_CONFIG_SECTION: {
                            PASSWORD_SECTION_FIELD: "strict",
                        },
                    }
                )
            ],
            discover_module_config=False,
        )

        with pytest.raises(ConfigurationError, match="Conflicting auth.password"):
            load_auth_settings(
                config,
                app_config=app_config,
            )

    def test_auth_settings_rejects_non_table_password_config(
        self, tmp_path: Path
    ) -> None:
        config_path = tmp_path / "app.toml"
        app_config = load_auth_test_app_config(config_path)
        config = ConfigService(
            [
                MappingConfigSource(
                    {
                        APP_CONFIG_SECTION: {
                            "database_url": "sqlite:///from-config.sqlite3",
                        },
                        AUTH_CONFIG_SECTION: {
                            PASSWORD_SECTION_FIELD: "strict",
                        },
                    }
                )
            ],
            discover_module_config=False,
        )

        with pytest.raises(ConfigurationError, match=r"\[auth\.password\] table"):
            load_auth_settings(
                config,
                app_config=app_config,
            )

    def test_auth_settings_rejects_non_table_inline_password_policy(
        self,
        tmp_path: Path,
    ) -> None:
        config_path = tmp_path / "app.toml"
        app_config = load_auth_test_app_config(config_path)
        config = ConfigService(
            [
                MappingConfigSource(
                    {
                        APP_CONFIG_SECTION: {
                            "database_url": "sqlite:///from-config.sqlite3",
                        },
                        AUTH_CONFIG_SECTION: {
                            PASSWORD_SECTION_FIELD: {
                                PASSWORD_POLICY_SECTION_FIELD: "strict",
                            },
                        },
                        PASSWORD_POLICY_CONFIG_SECTION: {
                            "minimum_length": 10,
                        },
                    }
                )
            ],
            discover_module_config=False,
        )

        with pytest.raises(
            ConfigurationError, match=r"\[auth\.password\.policy\] table"
        ):
            load_auth_settings(
                config,
                app_config=app_config,
            )

    @pytest.mark.parametrize(
        "common_fragments_config",
        [
            "common_fragments = 123",
            'common_fragments = ["example", 123]',
        ],
    )
    def test_app_auth_rejects_invalid_password_common_fragments(
        self,
        tmp_path: Path,
        common_fragments_config: str,
    ) -> None:
        app_config = load_auth_test_app_config(
            tmp_path / "app.toml",
            "",
            "[auth.password.policy]",
            common_fragments_config,
        )

        with pytest.raises(ConfigurationError, match="common fragments"):
            load_auth_settings(
                auth_settings_config(app_config),
                app_config=app_config,
            )

    def test_app_auth_rejects_unknown_password_policy_options(
        self, tmp_path: Path
    ) -> None:
        app_config = load_auth_test_app_config(
            tmp_path / "app.toml",
            "",
            "[auth.password.policy]",
            "minimum_strenth = 0.25",
        )

        with pytest.raises(ConfigurationError, match="minimum_strenth"):
            load_auth_settings(
                auth_settings_config(app_config),
                app_config=app_config,
            )

    @pytest.mark.parametrize("command", ["group", "scope", "user"])
    def test_authmgr_root_help_exposes_resource_command_groups(
        self, command: str
    ) -> None:
        result = CliRunner().invoke(authmgr.authmgr_command, ["--help"])

        assert result.exit_code == 0
        assert command in result.output

    @pytest.mark.parametrize(
        "command",
        ["create", "update", "delete", "deactivate", "list", "password"],
    )
    def test_authmgr_user_help_exposes_user_commands(self, command: str) -> None:
        result = CliRunner().invoke(authmgr.authmgr_command, ["user", "--help"])

        assert result.exit_code == 0
        assert command in result.output

    @pytest.mark.parametrize(
        ("argv", "expected"),
        [
            (
                ["user", "update", "--help"],
                "--revoke-passkey [CREDENTIAL]",
            ),
            (
                ["user", "update", "--help"],
                "Omit CREDENTIAL to revoke all active",
            ),
            (
                ["user", "list", "--help"],
                "--passkeys",
            ),
            (
                ["user", "list", "--help"],
                "Include active passkey records for each",
            ),
        ],
    )
    def test_authmgr_user_help_exposes_passkey_management_options(
        self,
        argv: list[str],
        expected: str,
    ) -> None:
        result = CliRunner().invoke(authmgr.authmgr_command, argv)

        assert result.exit_code == 0
        assert expected in result.output

    @pytest.mark.parametrize(
        "usage",
        [
            "wybra-authmgr group create <abbrev>",
            "wybra-authmgr group <group> add-group <group>",
            "wybra-authmgr group effective-scopes <user-target>",
        ],
    )
    def test_authmgr_group_help_exposes_group_operations(self, usage: str) -> None:
        result = CliRunner().invoke(authmgr.authmgr_command, ["group", "--help"])

        assert result.exit_code == 0
        assert usage in result.output

    def test_authmgr_group_help_uses_operation_metavar(self) -> None:
        result = CliRunner().invoke(authmgr.authmgr_command, ["group", "--help"])

        assert result.exit_code == 0
        assert "Usage: wybra-authmgr group [OPTIONS] [OPERATION]..." in result.output
        assert "TOKENS" not in result.output

    @pytest.mark.parametrize(
        ("help_suffix_args", "help_option_args"),
        [
            pytest.param(["help"], ["--help"], id="root"),
            pytest.param(["help", "user"], ["user", "--help"], id="root-user-group"),
            pytest.param(["help", "scope"], ["scope", "--help"], id="root-scope-group"),
            pytest.param(
                ["help", "group"], ["group", "--help"], id="root-group-command"
            ),
            pytest.param(
                ["help", "user", "create"],
                ["user", "create", "--help"],
                id="root-user-create",
            ),
            pytest.param(
                ["help", "scope", "create"],
                ["scope", "create", "--help"],
                id="root-scope-create",
            ),
            pytest.param(["user", "help"], ["user", "--help"], id="user-group"),
            pytest.param(
                ["user", "help", "create"],
                ["user", "create", "--help"],
                id="user-create",
            ),
            pytest.param(["scope", "help"], ["scope", "--help"], id="scope-group"),
            pytest.param(
                ["scope", "help", "create"],
                ["scope", "create", "--help"],
                id="scope-create",
            ),
            pytest.param(["group", "help"], ["group", "--help"], id="group-command"),
        ],
    )
    def test_authmgr_help_suffix_matches_help_option(
        self,
        help_suffix_args: list[str],
        help_option_args: list[str],
    ) -> None:
        runner = CliRunner()

        suffix_result = runner.invoke(authmgr.authmgr_command, help_suffix_args)
        option_result = runner.invoke(authmgr.authmgr_command, help_option_args)

        assert suffix_result.exit_code == option_result.exit_code == 0
        assert suffix_result.output == option_result.output

    @pytest.mark.parametrize(
        ("argv", "usage"),
        [
            pytest.param(
                ["help", "group", "create"],
                "Usage: wybra-authmgr group create <abbrev>",
                id="root-group-create",
            ),
            pytest.param(
                ["group", "help", "create"],
                "Usage: wybra-authmgr group create <abbrev>",
                id="group-create",
            ),
            pytest.param(
                ["group", "help", "project", "update"],
                "Usage: wybra-authmgr group <group> update",
                id="group-target-update",
            ),
            pytest.param(
                ["group", "create", "--help"],
                "Usage: wybra-authmgr group create <abbrev>",
                id="group-create-help-option",
            ),
            pytest.param(
                ["group", "project", "add-group", "--help"],
                "Usage: wybra-authmgr group <group> add-group <group>",
                id="group-target-add-group-help-option",
            ),
        ],
    )
    def test_authmgr_help_path_shows_raw_group_operation_usage(
        self,
        argv: list[str],
        usage: str,
    ) -> None:
        result = CliRunner().invoke(authmgr.authmgr_command, argv)

        assert result.exit_code == 0
        assert usage in result.output

    def test_authmgr_preserves_help_as_option_value(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        captured_args: list[authmgr.AuthmgrArgs] = []

        def capture_args(_ctx: click.Context, args: authmgr.AuthmgrArgs) -> None:
            captured_args.append(args)

        monkeypatch.setattr(authmgr_users, "_run_authmgr", capture_args)

        result = CliRunner().invoke(
            authmgr.authmgr_command,
            ["user", "update", "alice@example.com", "--timezone", "help"],
        )

        assert result.exit_code == 0
        assert captured_args[0].preferred_timezone == "help"

    @pytest.mark.parametrize(
        ("argv", "expected_field", "expected_value"),
        [
            pytest.param(
                ["scope", "create", "help"],
                "scope",
                "help",
                id="scope-name",
            ),
            pytest.param(
                ["group", "create", "admins", "--description", "help"],
                "description",
                "help",
                id="group-description",
            ),
        ],
    )
    def test_authmgr_preserves_help_as_command_value(
        self,
        monkeypatch: pytest.MonkeyPatch,
        argv: list[str],
        expected_field: str,
        expected_value: str,
    ) -> None:
        captured_args: list[authmgr.AuthmgrArgs] = []

        def capture_args(_ctx: click.Context, args: authmgr.AuthmgrArgs) -> None:
            captured_args.append(args)

        target_module = authmgr_scopes if argv[0] == "scope" else authmgr_groups
        monkeypatch.setattr(target_module, "_run_authmgr", capture_args)

        result = CliRunner().invoke(authmgr.authmgr_command, argv)

        assert result.exit_code == 0
        assert getattr(captured_args[0], expected_field) == expected_value

    def test_authmgr_group_create_accepts_dash_prefixed_abbrev_after_terminator(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        captured_args: list[authmgr.AuthmgrArgs] = []

        def capture_args(_ctx: click.Context, args: authmgr.AuthmgrArgs) -> None:
            captured_args.append(args)

        monkeypatch.setattr(authmgr_groups, "_run_authmgr", capture_args)

        result = CliRunner().invoke(
            authmgr.authmgr_command,
            ["group", "create", "--", "-admins"],
        )

        assert result.exit_code == 0
        assert captured_args[0].command == "group-create"
        assert captured_args[0].group_target == "-admins"

    @pytest.mark.parametrize(
        "command",
        ["create", "update", "delete", "deactivate", "list", "password"],
    )
    def test_authmgr_rejects_top_level_user_action_commands(self, command: str) -> None:
        result = CliRunner().invoke(authmgr.authmgr_command, [command])

        assert result.exit_code == 2
        assert f"No such command '{command}'" in result.output

    def test_authmgr_rejects_unknown_command(self) -> None:
        result = CliRunner().invoke(authmgr.authmgr_command, ["unknown"])

        assert result.exit_code == 2
        assert "No such command 'unknown'" in result.output

    def test_authmgr_main_treats_falsy_click_exception_as_failure(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        class FalsyExitClickException(click.ClickException):
            exit_code = 0

        def raise_click_exception(*_args, **_kwargs) -> None:
            raise FalsyExitClickException("invalid usage")

        monkeypatch.setattr(authmgr.authmgr_command, "main", raise_click_exception)

        assert authmgr.main([]) == 1

        captured = capsys.readouterr()
        assert "invalid usage" in captured.err

    @pytest.mark.parametrize(
        "argv",
        [
            ["user", "create", "person@example.com", "--password", "secret"],
            ["user", "password", "person@example.com", "--password", "secret"],
            ["user", "update", "person@example.com", "--password", "secret"],
        ],
    )
    def test_authmgr_rejects_plain_command_line_password(self, argv: list[str]) -> None:
        result = CliRunner().invoke(
            authmgr.authmgr_command,
            argv,
        )

        assert result.exit_code == 2
        assert "must be '-' or omitted" in result.output
        assert "--password" in result.output

    @pytest.mark.parametrize("expires_at", ["4102444800", "0"])
    def test_authmgr_rejects_conflicting_expiry_update_options(
        self, expires_at: str
    ) -> None:
        result = CliRunner().invoke(
            authmgr.authmgr_command,
            [
                "user",
                "update",
                "person@example.com",
                "--expires-at",
                expires_at,
                "--no-expires-at",
            ],
        )

        assert result.exit_code == 2
        assert "not allowed with option '--expires-at'" in result.output

    @pytest.mark.parametrize(
        "argv",
        (
            ["user", "create", "person@example.com", "--display-name", "Name"],
            ["user", "create", "person@example.com", "--preferred-name", "Name"],
            ["user", "update", "person@example.com", "--display-name", "Name"],
            ["user", "update", "person@example.com", "--no-display-name"],
            ["user", "update", "person@example.com", "--preferred-name", "Name"],
            ["user", "update", "person@example.com", "--no-preferred-name"],
        ),
    )
    def test_authmgr_rejects_removed_profile_name_options(
        self, argv: list[str]
    ) -> None:
        result = CliRunner().invoke(
            authmgr.authmgr_command,
            argv,
        )

        assert result.exit_code == 2
        assert "No such option" in result.output

    def test_authmgr_accepts_flexible_expiry_timestamp_values(self) -> None:
        assert (
            authmgr_timestamps.parse_timestamp_filter("2100-01-01T00:00:00Z")
            == 4102444800.0
        )
        assert authmgr_timestamps.parse_timestamp_filter("4102444800") == 4102444800.0
        assert authmgr_timestamps.parse_timestamp_filter("20250101") == 20250101.0

    def test_authmgr_timestamp_parse_error_identifies_option(self) -> None:
        result = CliRunner().invoke(
            authmgr.authmgr_command,
            ["user", "list", "--since-created-at", "not-a-date"],
        )

        assert result.exit_code == 2
        assert "Invalid value for '--since-created-at'" in result.output
        assert "Invalid timestamp value: not-a-date" in result.output

    def test_authmgr_help_documents_numeric_timestamp_precedence(self) -> None:
        result = CliRunner().invoke(authmgr.authmgr_command, ["--help"])

        assert result.exit_code == 0
        assert "numeric input as Unix seconds before date parsing" in result.output

    def test_user_model_exposes_management_metadata_columns(self) -> None:
        user_fields = set(User._meta.fields_map)
        user_indexes = {
            tuple(index.describe()["fields"]) for index in User._meta.indexes
        }

        assert {
            "is_admin",
            "created_at",
            "modified_at",
            "last_login_at",
            "expires_at",
            "email_verification_sent_at",
            "preferred_timezone",
        }.issubset(user_fields)
        assert "display_name" not in user_fields
        assert "preferred_name" not in user_fields
        assert {
            ("is_active", "expires_at"),
            ("last_login_at",),
            ("created_at",),
            ("modified_at",),
            ("is_admin",),
            ("is_superuser",),
        }.issubset(user_indexes)

    def test_user_model_defines_modified_at_timestamp_default(self) -> None:
        assert callable(User._meta.fields_map["modified_at"].default)

    @pytest.mark.anyio
    async def test_user_management_metadata_defaults(self) -> None:
        settings = AuthTestSettings(database_url=SQLITE_MEMORY_DATABASE_URL)

        async def assert_defaults(connection: BaseDBAsyncClient) -> None:
            manager = create_user_manager(connection, settings.identity_options)
            await manager.create(
                UserCreate(
                    email="metadata@example.com",
                    password=STRONG_TEST_PASSWORD,
                ),
                safe=True,
            )

            user = await User.get(email="metadata@example.com", using_db=connection)

            assert user.is_admin is False
            assert isinstance(user.created_at, float)
            assert isinstance(user.modified_at, float)
            assert user.created_at > 0
            assert user.modified_at >= user.created_at
            assert user.last_login_at is None
            assert user.expires_at is None
            assert user.email_verification_sent_at is None
            assert user.preferred_timezone is None

        await _with_generated_identity_connection(
            settings.database_url,
            assert_defaults,
        )

    @pytest.mark.anyio
    async def test_user_manager_password_policy_uses_profile_fragments_when_available(
        self,
    ) -> None:
        settings = AuthTestSettings(database_url=SQLITE_MEMORY_DATABASE_URL)

        async def assert_profile_fragment_is_rejected(
            connection: BaseDBAsyncClient,
        ) -> None:
            manager = create_user_manager(connection, settings.identity_options)
            user = await manager.create(
                UserCreate(
                    email="profile-fragment@example.com",
                    password=STRONG_TEST_PASSWORD,
                ),
                safe=True,
            )

            async def profile_lookup(_user: User) -> object:
                return SimpleNamespace(
                    display_name="Operator Example",
                    preferred_name="Operator",
                )

            manager = create_user_manager(
                connection,
                settings.identity_options,
                profile_lookup=profile_lookup,
            )

            with pytest.raises(InvalidPasswordException) as exc_info:
                await manager.validate_password("operator account 123!", user)
            assert "strength requirement" in str(exc_info.value.reason)

        await _with_generated_identity_connection(
            settings.database_url,
            assert_profile_fragment_is_rejected,
        )

    @pytest.mark.anyio
    async def test_user_manager_get_by_email_resolves_secondary_emails(
        self,
        tmp_path: Path,
    ) -> None:
        database_url = sqlite_file_url(tmp_path / "secondary-email.sqlite3")
        await initialise_identity_database(database_url)
        web_app = create_auth_test_app(database_url=database_url)

        async def assert_secondary_email_lookup() -> None:
            await initialise_app_identity_database(web_app)

            async with app_connection_scope(web_app) as session:
                manager = create_user_manager(
                    session,
                    web_app.state.auth_settings.identity_options,
                )
                user = await manager.create(
                    UserCreate(
                        email="primary@example.com",
                        password=STRONG_TEST_PASSWORD,
                    ),
                    safe=True,
                )
                await IdentityUserEmail.create(
                    user_id=user.id,
                    email="alias@example.com",
                    is_primary=False,
                    is_verified=True,
                    using_db=session,
                )

                primary_user = await manager.get_by_email("Primary@Example.com")
                alias_user = await manager.get_by_email("Alias@Example.com")

                assert primary_user is not None
                assert alias_user is not None
                assert primary_user.id == user.id
                assert alias_user.id == user.id

        await run_auth_app_test(web_app, assert_secondary_email_lookup)

    @pytest.mark.anyio
    async def test_resolve_user_target_uses_secondary_email_addresses(
        self,
        tmp_path: Path,
    ) -> None:
        database_url = sqlite_file_url(tmp_path / "secondary-target.sqlite3")
        await initialise_identity_database(database_url)
        web_app = create_auth_test_app(database_url=database_url)

        async def assert_secondary_target_resolution() -> None:
            await initialise_app_identity_database(web_app)

            async with app_connection_scope(web_app) as session:
                manager = create_user_manager(
                    session,
                    web_app.state.auth_settings.identity_options,
                )
                user = await manager.create(
                    UserCreate(
                        email="target@example.com",
                        password=STRONG_TEST_PASSWORD,
                    ),
                    safe=True,
                )
                await IdentityUserEmail.create(
                    user_id=user.id,
                    email="linked@example.com",
                    is_primary=False,
                    is_verified=True,
                    using_db=session,
                )

                (
                    resolved_user,
                    target_error,
                ) = await identity_management.resolve_user_target(
                    session,
                    "linked@example.com",
                )
                (
                    resolved_user_mixed_case,
                    target_error_mixed_case,
                ) = await identity_management.resolve_user_target(
                    session,
                    "Linked@Example.com",
                )
                assert target_error is None
                assert target_error_mixed_case is None
                assert resolved_user is not None
                assert resolved_user_mixed_case is not None
                assert resolved_user.id == user.id
                assert resolved_user_mixed_case.id == user.id

        await run_auth_app_test(web_app, assert_secondary_target_resolution)

    @pytest.mark.anyio
    async def test_identity_session_authenticate_user_accepts_secondary_email_alias(
        self,
        tmp_path: Path,
    ) -> None:
        database_url = sqlite_file_url(tmp_path / "authenticate-secondary.sqlite3")
        await initialise_identity_database(database_url)
        web_app = create_auth_test_app(database_url=database_url)

        async def assert_secondary_alias_authentication() -> None:
            await initialise_app_identity_database(web_app)

            async with app_connection_scope(web_app) as session:
                manager = create_user_manager(
                    session,
                    web_app.state.auth_settings.identity_options,
                )
                user = await manager.create(
                    UserCreate(
                        email="person@example.com",
                        password=STRONG_TEST_PASSWORD,
                    ),
                    safe=True,
                )
                await IdentityUserEmail.create(
                    user_id=user.id,
                    email="Alias@Example.com",
                    is_primary=False,
                    is_verified=True,
                    using_db=session,
                )

            user = await identity_sessions.authenticate_user(
                Request({"type": "http", "app": web_app}),
                "alias@example.com",
                STRONG_TEST_PASSWORD,
            )

            assert user is not None
            assert user.email == "person@example.com"

        await run_auth_app_test(web_app, assert_secondary_alias_authentication)

    @pytest.mark.anyio
    async def test_identity_session_authenticate_user_secondary_email_stays_with_owner(
        self,
        tmp_path: Path,
    ) -> None:
        database_url = sqlite_file_url(
            tmp_path / "authenticate-secondary-owner.sqlite3"
        )
        await initialise_identity_database(database_url)
        web_app = create_auth_test_app(database_url=database_url)

        async def assert_secondary_alias_stays_with_owner() -> None:
            await initialise_app_identity_database(web_app)

            async with app_connection_scope(web_app) as session:
                manager = create_user_manager(
                    session,
                    web_app.state.auth_settings.identity_options,
                )
                primary_user = await manager.create(
                    UserCreate(
                        email="person-a@example.com",
                        password=STRONG_TEST_PASSWORD,
                    ),
                    safe=True,
                )
                await manager.create(
                    UserCreate(
                        email="person-b@example.com",
                        password=STRONG_TEST_PASSWORD,
                    ),
                    safe=True,
                )
                await IdentityUserEmail.create(
                    user_id=primary_user.id,
                    email="sharedalias@example.com",
                    is_primary=False,
                    is_verified=True,
                    using_db=session,
                )

            user = await identity_sessions.authenticate_user(
                Request({"type": "http", "app": web_app}),
                "sharedalias@example.com",
                STRONG_TEST_PASSWORD,
            )

            assert user is not None
            assert user.id == primary_user.id

        await run_auth_app_test(web_app, assert_secondary_alias_stays_with_owner)

    @pytest.mark.anyio
    async def test_identity_session_request_password_reset_uses_secondary_email_alias(
        self,
        tmp_path: Path,
    ) -> None:
        database_url = sqlite_file_url(
            tmp_path / "request-password-reset-secondary.sqlite3"
        )
        await initialise_identity_database(database_url)
        web_app = create_auth_test_app(database_url=database_url)
        web_app.state.identity_delivery = ResetPasswordDelivery(reset_tokens=[])

        async def assert_password_reset_alias_resolution() -> None:
            await initialise_app_identity_database(web_app)

            async with app_connection_scope(web_app) as session:
                manager = create_user_manager(
                    session,
                    web_app.state.auth_settings.identity_options,
                )
                user = await manager.create(
                    UserCreate(
                        email="owner@example.com",
                        password=STRONG_TEST_PASSWORD,
                    ),
                    safe=True,
                )
                await IdentityUserEmail.create(
                    user_id=user.id,
                    email="Alias@Example.com",
                    is_primary=False,
                    is_verified=True,
                    using_db=session,
                )

            await identity_sessions.request_password_reset(
                Request({"type": "http", "app": web_app}),
                "alias@example.com",
            )

            assert web_app.state.identity_delivery.reset_tokens == [
                (
                    "owner@example.com",
                    web_app.state.identity_delivery.reset_tokens[0][1],
                ),
            ]

        await run_auth_app_test(web_app, assert_password_reset_alias_resolution)

    @pytest.mark.anyio
    async def test_identity_session_request_password_reset_ignores_unknown_email_alias(
        self,
        tmp_path: Path,
    ) -> None:
        database_url = sqlite_file_url(
            tmp_path / "request-password-reset-unknown.sqlite3"
        )
        await initialise_identity_database(database_url)
        web_app = create_auth_test_app(database_url=database_url)
        web_app.state.identity_delivery = ResetPasswordDelivery(reset_tokens=[])

        async def assert_missing_alias_is_ignored() -> None:
            await initialise_app_identity_database(web_app)

            async with app_connection_scope(web_app) as session:
                manager = create_user_manager(
                    session,
                    web_app.state.auth_settings.identity_options,
                )
                await manager.create(
                    UserCreate(
                        email="owner@example.com",
                        password=STRONG_TEST_PASSWORD,
                    ),
                    safe=True,
                )

            await identity_sessions.request_password_reset(
                Request({"type": "http", "app": web_app}),
                "missing-alias@example.com",
            )

            assert web_app.state.identity_delivery.reset_tokens == []

        await run_auth_app_test(web_app, assert_missing_alias_is_ignored)

    @pytest.mark.anyio
    async def test_identity_session_reset_password_revokes_existing_sessions(
        self,
        tmp_path: Path,
    ) -> None:
        database_url = sqlite_file_url(tmp_path / "reset-password-revokes.sqlite3")
        await initialise_identity_database(database_url)
        web_app = create_auth_test_app(database_url=database_url)
        web_app.state.identity_delivery = ResetPasswordDelivery(reset_tokens=[])

        async def assert_reset_revokes_sessions() -> None:
            await initialise_app_identity_database(web_app)

            async with app_connection_scope(web_app) as session:
                manager = create_user_manager(
                    session,
                    web_app.state.auth_settings.identity_options,
                )
                user = await manager.create(
                    UserCreate(
                        email="reset-revoke@example.com",
                        password=STRONG_TEST_PASSWORD,
                    ),
                    safe=True,
                )
                await AccessToken.create(
                    token="existing-session",
                    user_id=user.id,
                    using_db=session,
                )

            await identity_sessions.request_password_reset(
                Request({"type": "http", "app": web_app}),
                "reset-revoke@example.com",
            )
            [(_email, reset_token)] = web_app.state.identity_delivery.reset_tokens

            assert await identity_sessions.reset_password(
                Request({"type": "http", "app": web_app}),
                reset_token,
                UPDATED_STRONG_TEST_PASSWORD,
            )

            async with app_connection_scope(web_app) as session:
                tokens = list(await AccessToken.all().using_db(session))
                assert tokens == []

        await run_auth_app_test(web_app, assert_reset_revokes_sessions)

    @pytest.mark.anyio
    async def test_user_manager_update_email_updates_primary_owned_email(
        self,
        tmp_path: Path,
    ) -> None:
        database_url = sqlite_file_url(tmp_path / "update-primary-email.sqlite3")
        await initialise_identity_database(database_url)
        web_app = create_auth_test_app(database_url=database_url)

        async def assert_email_update_is_synchronised() -> None:
            await initialise_app_identity_database(web_app)

            async with app_connection_scope(web_app) as session:
                manager = create_user_manager(
                    session,
                    web_app.state.auth_settings.identity_options,
                )
                user = await manager.create(
                    UserCreate(
                        email="old@example.com",
                        password=STRONG_TEST_PASSWORD,
                        is_verified=True,
                    ),
                    safe=True,
                )

                updated_user = await manager.update(
                    UserUpdate(email="New@Example.com"),
                    user,
                    safe=True,
                )

                assert updated_user.email == "new@example.com"
                assert updated_user.is_verified is False
                with pytest.raises(UserNotExists):
                    await manager.get_by_email("old@example.com")
                assert (await manager.get_by_email("new@example.com")).id == user.id

        await run_auth_app_test(web_app, assert_email_update_is_synchronised)
        [primary_email] = await identity_user_emails_from_database(database_url)
        assert primary_email.email == "new@example.com"
        assert primary_email.is_primary is True
        assert primary_email.is_verified is False

    @pytest.mark.anyio
    async def test_request_verification_uses_secondary_email_alias_for_lookup(
        self,
        tmp_path: Path,
    ) -> None:
        database_url = sqlite_file_url(
            tmp_path / "request-verification-secondary.sqlite3"
        )
        await initialise_identity_database(database_url)
        web_app = create_auth_test_app(database_url=database_url)
        web_app.state.identity_delivery = CaptureDelivery(verification_tokens=[])

        async def assert_verification_alias_resolution() -> None:
            await initialise_app_identity_database(web_app)

            async with app_connection_scope(web_app) as session:
                manager = create_user_manager(
                    session,
                    web_app.state.auth_settings.identity_options,
                )
                user = await manager.create(
                    UserCreate(
                        email="owner@example.com",
                        password=STRONG_TEST_PASSWORD,
                    ),
                    safe=True,
                )
                await IdentityUserEmail.create(
                    user_id=user.id,
                    email="Alias@Example.com",
                    is_primary=False,
                    is_verified=True,
                    using_db=session,
                )

            await identity_sessions.request_verification(
                Request({"type": "http", "app": web_app}),
                "alias@example.com",
            )

            assert web_app.state.identity_delivery.verification_tokens == [
                (
                    "owner@example.com",
                    web_app.state.identity_delivery.verification_tokens[0][1],
                ),
            ]

        await run_auth_app_test(web_app, assert_verification_alias_resolution)

    @pytest.mark.anyio
    async def test_user_manager_create_rollback_when_after_register_fails(
        self,
        tmp_path: Path,
    ) -> None:
        database_url = sqlite_file_url(tmp_path / "create-after-register-fail.sqlite3")
        await initialise_identity_database(database_url)
        web_app = create_auth_test_app(database_url=database_url)

        class _FailingPostRegisterManager(UserManager):
            async def on_after_register(
                self,
                user: User,
                request: Request | None = None,
            ) -> None:
                raise RuntimeError("post-register hook failed")

        async def assert_rollback_when_hook_fails() -> None:
            await initialise_app_identity_database(web_app)

            async with app_connection_scope(web_app) as session:
                manager = _FailingPostRegisterManager(
                    create_user_store(session),
                    web_app.state.auth_settings.identity_options,
                )
                with pytest.raises(RuntimeError, match="post-register hook failed"):
                    await manager.create(
                        UserCreate(
                            email="rollback-hook@example.com",
                            password=STRONG_TEST_PASSWORD,
                        ),
                        safe=True,
                    )

        await run_auth_app_test(web_app, assert_rollback_when_hook_fails)
        assert await identity_users_from_database(database_url) == []
        assert await identity_user_emails_from_database(database_url) == []

    @pytest.mark.anyio
    async def test_user_manager_duplicate_secondary_email_maps_to_user_already_exists(
        self,
        tmp_path: Path,
    ) -> None:
        database_url = sqlite_file_url(
            tmp_path / "create-duplicate-secondary-email.sqlite3"
        )
        await initialise_identity_database(database_url)
        web_app = create_auth_test_app(database_url=database_url)

        class _NoLookupManager(UserManager):
            async def get_by_email(self, user_email: str) -> User:
                del user_email
                raise UserNotExists()

        async def assert_duplicate_secondary_email_returns_user_already_exists() -> (
            None
        ):
            await initialise_app_identity_database(web_app)

            async with app_connection_scope(web_app) as session:
                manager = create_user_manager(
                    session,
                    web_app.state.auth_settings.identity_options,
                )
                primary_user = await manager.create(
                    UserCreate(
                        email="primary@example.com",
                        password=STRONG_TEST_PASSWORD,
                    ),
                    safe=True,
                )
                await IdentityUserEmail.create(
                    user_id=primary_user.id,
                    email="Alias@example.com",
                    is_primary=False,
                    is_verified=True,
                    using_db=session,
                )

                racing_manager = _NoLookupManager(
                    create_user_store(session),
                    web_app.state.auth_settings.identity_options,
                )
                with pytest.raises(UserAlreadyExists):
                    await racing_manager.create(
                        UserCreate(
                            email="ALIAS@example.com",
                            password=STRONG_TEST_PASSWORD,
                        ),
                        safe=True,
                    )

                users = list(await User.all().using_db(session))
                emails = list(await IdentityUserEmail.all().using_db(session))
                assert len(users) == 1
                assert users[0].email == "primary@example.com"
                assert len(emails) == 2

        await run_auth_app_test(
            web_app,
            assert_duplicate_secondary_email_returns_user_already_exists,
        )

    @pytest.mark.anyio
    async def test_generated_identity_schema_creates_user_management_metadata_columns(
        self,
        tmp_path: Path,
    ) -> None:
        database_path = tmp_path / "metadata.sqlite3"
        database_url = sqlite_file_url(database_path)

        await initialise_identity_database(database_url)

        columns = sqlite_table_columns(database_path, "identity_user")
        assert {
            "is_admin",
            "created_at",
            "modified_at",
            "last_login_at",
            "expires_at",
            "email_verification_sent_at",
            "preferred_timezone",
        }.issubset(columns)
        assert "display_name" not in columns
        assert "preferred_name" not in columns

    @pytest.mark.anyio
    async def test_generated_identity_schema_creates_authorisation_group_tables(
        self,
        tmp_path: Path,
    ) -> None:
        database_path = tmp_path / "groups.sqlite3"
        database_url = sqlite_file_url(database_path)

        await initialise_identity_database(database_url)

        assert {
            "identity_group",
            "identity_scope",
            "identity_group_scope",
            "identity_group_user",
            "identity_group_group",
        }.issubset(sqlite_table_names(database_path))
        assert sqlite_table_columns(database_path, "identity_group") == {
            "id",
            "abbrev",
            "description",
        }
        assert sqlite_table_columns(database_path, "identity_scope") == {
            "scope",
            "description",
        }
        assert {"group_id", "scope"}.issubset(
            sqlite_table_columns(database_path, "identity_group_scope")
        )
        assert {"group_id", "user_id"}.issubset(
            sqlite_table_columns(database_path, "identity_group_user")
        )
        assert {"parent_group_id", "child_group_id"}.issubset(
            sqlite_table_columns(database_path, "identity_group_group")
        )

    @pytest.mark.anyio
    async def test_generated_identity_schema_creates_identity_user_email_table(
        self,
        tmp_path: Path,
    ) -> None:
        database_path = tmp_path / "user-email.sqlite3"
        database_url = sqlite_file_url(database_path)

        await initialise_identity_database(database_url)

        assert "identity_user_email" in sqlite_table_names(database_path)
        assert {"id", "user_id", "email", "is_primary", "is_verified"}.issubset(
            sqlite_table_columns(database_path, "identity_user_email")
        )

    @pytest.mark.anyio
    async def test_generated_identity_schema_creates_webauthn_credential_table(
        self,
        tmp_path: Path,
    ) -> None:
        database_path = tmp_path / "webauthn.sqlite3"
        database_url = sqlite_file_url(database_path)

        await initialise_identity_database(database_url)

        assert "identity_webauthn_credential" in sqlite_table_names(database_path)
        assert {
            "id",
            "user_id",
            "credential_id",
            "public_key",
            "sign_count",
            "status",
            "label",
            "created_at",
            "last_used_at",
            "revoked_at",
            "user_verified",
            "credential_device_type",
            "credential_backed_up",
            "transports",
            "aaguid",
            "attestation_format",
        }.issubset(sqlite_table_columns(database_path, "identity_webauthn_credential"))

    def test_authmgr_reports_outdated_identity_schema_before_reading_password(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        database_path = tmp_path / "outdated-identity.sqlite3"
        initialise_legacy_identity_database(database_path)
        set_authmgr_database_url(monkeypatch, tmp_path, sqlite_file_url(database_path))
        stdin = io.StringIO(f"{STRONG_TEST_PASSWORD}\n")
        monkeypatch.setattr(sys, "stdin", stdin)

        exit_code = authmgr.main(
            ["user", "create", "outdated@example.com", "--password", "-"]
        )

        assert exit_code == 1
        assert stdin.tell() == 0
        captured = capsys.readouterr()
        assert "Auth database schema is not up to date" in captured.err
        assert "uv run wybra-migrate init" in captured.err
        assert "uv run wybra-migrate migrate" in captured.err
        assert "selected app config" in captured.err
        assert "explicit auth database" not in captured.err
        assert "is_admin" in captured.err

    @pytest.mark.anyio
    async def test_authmgr_reports_missing_group_tables_before_reading_password(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        database_path = tmp_path / "users-only.sqlite3"
        database_url = sqlite_file_url(database_path)
        await initialise_identity_database(database_url)
        with closing(sqlite3.connect(database_path)) as connection, connection:
            for table_name in (
                "identity_group",
                "identity_scope",
                "identity_group_scope",
                "identity_group_user",
                "identity_group_group",
            ):
                connection.execute(f"DROP TABLE {table_name}")
        set_authmgr_database_url(monkeypatch, tmp_path, database_url)
        stdin = io.StringIO(f"{STRONG_TEST_PASSWORD}\n")
        monkeypatch.setattr(sys, "stdin", stdin)

        exit_code = await run_authmgr_command(
            ["user", "create", "missing-groups@example.com", "--password", "-"]
        )

        assert exit_code == 1
        assert stdin.tell() == 0
        captured = capsys.readouterr()
        assert "Auth database schema is not up to date" in captured.err
        assert "Missing identity_group table" in captured.err

    def test_authmgr_reports_missing_identity_table_before_reading_password(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        database_path = tmp_path / "missing-identity.sqlite3"
        set_authmgr_database_url(monkeypatch, tmp_path, sqlite_file_url(database_path))
        stdin = io.StringIO(f"{STRONG_TEST_PASSWORD}\n")
        monkeypatch.setattr(sys, "stdin", stdin)

        exit_code = authmgr.main(
            ["user", "create", "missing@example.com", "--password", "-"]
        )

        assert exit_code == 1
        assert stdin.tell() == 0
        captured = capsys.readouterr()
        assert "Auth database schema is not up to date" in captured.err
        assert "Missing identity_user table" in captured.err
        assert "Missing identity_user columns" not in captured.err

    @pytest.mark.anyio
    async def test_authmgr_identity_schema_error_names_missing_user_table(self) -> None:
        with pytest.raises(ConfigurationError) as exc_info:
            await _with_identity_connection(
                SQLITE_MEMORY_DATABASE_URL,
                authmgr_schema._verify_identity_schema,
            )

        assert "Missing identity_user table" in str(exc_info.value)

    @pytest.mark.anyio
    async def test_authmgr_identity_schema_missing_columns_are_table_aware(
        self,
    ) -> None:
        async def assert_missing_group_column(connection: BaseDBAsyncClient) -> None:
            await connection.execute_script(
                """
                CREATE TABLE identity_user (
                    id TEXT PRIMARY KEY,
                    email TEXT,
                    hashed_password TEXT,
                    is_active BOOLEAN,
                    is_superuser BOOLEAN,
                    is_verified BOOLEAN,
                    password_login_enabled BOOLEAN,
                    is_admin BOOLEAN,
                    created_at REAL,
                    modified_at REAL,
                    last_login_at REAL,
                    expires_at REAL,
                    email_verification_sent_at REAL,
                    preferred_timezone TEXT
                );
                CREATE TABLE identity_group (
                    id TEXT PRIMARY KEY,
                    abbrev TEXT
                );
                CREATE TABLE identity_scope (
                    scope TEXT PRIMARY KEY,
                    description TEXT
                );
                CREATE TABLE identity_group_scope (
                    id INTEGER PRIMARY KEY,
                    group_id TEXT,
                    scope TEXT
                );
                CREATE TABLE identity_group_user (
                    id INTEGER PRIMARY KEY,
                    group_id TEXT,
                    user_id TEXT
                );
                CREATE TABLE identity_group_group (
                    id INTEGER PRIMARY KEY,
                    parent_group_id TEXT,
                    child_group_id TEXT
                );
                CREATE TABLE identity_user_email (
                    id TEXT PRIMARY KEY,
                    user_id TEXT,
                    email TEXT,
                    is_primary BOOLEAN,
                    is_verified BOOLEAN
                );
                """
            )
            await authmgr_schema._verify_identity_schema(connection)

        with pytest.raises(ConfigurationError) as exc_info:
            await _with_identity_connection(
                SQLITE_MEMORY_DATABASE_URL,
                assert_missing_group_column,
            )

        message = str(exc_info.value)
        assert "Missing identity schema columns: identity_group.description" in message
        assert "Missing identity_user columns" not in message

    @pytest.mark.anyio
    async def test_authmgr_identity_schema_status_normalises_column_name_case(
        self,
    ) -> None:
        async def assert_column_case_normalised(connection: BaseDBAsyncClient) -> None:
            for model in authmgr_schema._identity_schema_models():
                columns = ", ".join(
                    f'"{column.upper()}" TEXT'
                    for column in authmgr_schema._model_column_names(model)
                )
                await connection.execute_script(
                    f"CREATE TABLE {model._meta.db_table} ({columns});"
                )
            status = await authmgr_schema._identity_schema_status(connection)

            assert status.table_exists is True
            assert status.missing_columns == ()

        await _with_identity_connection(
            SQLITE_MEMORY_DATABASE_URL,
            assert_column_case_normalised,
        )

    @pytest.mark.anyio
    async def test_authmgr_reports_schema_inspection_error_without_leaking_context(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        class FailingConnection:
            capabilities = SimpleNamespace(dialect="sqlite")

            async def execute_query_dict(self, _query: str):
                raise BaseORMException("database is locked")

        with caplog.at_level(logging.DEBUG, logger="wybra.auth.cli.authmgr"):
            with pytest.raises(ConfigurationError) as exc_info:
                await authmgr_schema._verify_identity_schema(FailingConnection())  # type: ignore[arg-type]

        message = str(exc_info.value)
        assert "Auth database schema could not be inspected" in message
        assert "BaseORMException" not in message
        assert "database is locked" not in message
        assert "BaseORMException" in caplog.text
        assert "database is locked" in caplog.text

    @pytest.mark.anyio
    async def test_authentication_finalisation_updates_last_login_timestamp(
        self,
        tmp_path: Path,
    ) -> None:
        web_app = create_auth_test_app(
            database_url=sqlite_file_url(tmp_path / "last-login-finalisation.sqlite3")
        )

        async def assert_last_login_update() -> None:
            await initialise_app_identity_database(web_app)

            async with app_connection_scope(web_app) as session:
                manager = create_user_manager(
                    session,
                    web_app.state.auth_settings.identity_options,
                )
                user = await manager.create(
                    UserCreate(
                        email="login-time@example.com",
                        password=STRONG_TEST_PASSWORD,
                    ),
                    safe=True,
                )
                user.is_verified = True
                await user.save(using_db=session)
                assert user.last_login_at is None

            result = await identity_sessions.complete_authentication_ceremony(
                Request({"type": "http", "app": web_app}),
                user,
            )

            assert result.is_ok() is True

            async with app_connection_scope(web_app) as session:
                refreshed_user = await User.get_or_none(id=user.id, using_db=session)
                assert refreshed_user is not None
                assert isinstance(refreshed_user.last_login_at, float)
                assert refreshed_user.last_login_at > 0

        await run_auth_app_test(web_app, assert_last_login_update)

    @pytest.mark.anyio
    async def test_expired_user_is_rejected_during_authentication_finalisation(
        self,
        tmp_path: Path,
    ) -> None:
        web_app = create_auth_test_app(
            database_url=sqlite_file_url(tmp_path / "expired-finalisation.sqlite3")
        )

        async def assert_expired_user_rejected() -> None:
            await initialise_app_identity_database(web_app)

            async with app_connection_scope(web_app) as session:
                manager = create_user_manager(
                    session,
                    web_app.state.auth_settings.identity_options,
                )
                user = await manager.create(
                    UserCreate(
                        email="expired-login@example.com",
                        password=STRONG_TEST_PASSWORD,
                    ),
                    safe=True,
                )
                user.is_verified = True
                user.expires_at = time() - 60
                await user.save(using_db=session)

            result = await identity_sessions.complete_authentication_ceremony(
                Request({"type": "http", "app": web_app}),
                user,
            )

            assert result.is_failure() is True
            assert result.error_type == ERROR_INACTIVE_USER

            async with app_connection_scope(web_app) as session:
                refreshed_user = await User.get_or_none(id=user.id, using_db=session)
                assert refreshed_user is not None
                assert refreshed_user.last_login_at is None

        await run_auth_app_test(web_app, assert_expired_user_rejected)

    @pytest.mark.anyio
    async def test_inactive_user_is_rejected_during_authentication_finalisation(
        self,
        tmp_path: Path,
    ) -> None:
        web_app = create_auth_test_app(
            database_url=sqlite_file_url(tmp_path / "inactive-finalisation.sqlite3")
        )

        async def assert_inactive_user_rejected() -> None:
            await initialise_app_identity_database(web_app)

            async with app_connection_scope(web_app) as session:
                manager = create_user_manager(
                    session,
                    web_app.state.auth_settings.identity_options,
                )
                user = await manager.create(
                    UserCreate(
                        email="inactive-login@example.com",
                        password=STRONG_TEST_PASSWORD,
                    ),
                    safe=True,
                )
                user.is_verified = True
                user.is_active = False
                await user.save(using_db=session)

            result = await identity_sessions.complete_authentication_ceremony(
                Request({"type": "http", "app": web_app}),
                user,
            )

            assert result.is_failure() is True
            assert result.error_type == ERROR_INACTIVE_USER

            async with app_connection_scope(web_app) as session:
                refreshed_user = await User.get_or_none(id=user.id, using_db=session)
                assert refreshed_user is not None
                assert refreshed_user.last_login_at is None

        await run_auth_app_test(web_app, assert_inactive_user_rejected)

    @pytest.mark.anyio
    async def test_unverified_user_is_rejected_during_authentication_finalisation(
        self,
        tmp_path: Path,
    ) -> None:
        web_app = create_auth_test_app(
            database_url=sqlite_file_url(tmp_path / "unverified-finalisation.sqlite3")
        )

        async def assert_unverified_user_rejected() -> None:
            await initialise_app_identity_database(web_app)

            async with app_connection_scope(web_app) as session:
                manager = create_user_manager(
                    session,
                    web_app.state.auth_settings.identity_options,
                )
                user = await manager.create(
                    UserCreate(
                        email="unverified-login@example.com",
                        password=STRONG_TEST_PASSWORD,
                    ),
                    safe=True,
                )
                assert user.is_verified is False

            result = await identity_sessions.complete_authentication_ceremony(
                Request({"type": "http", "app": web_app}),
                user,
            )

            assert result.is_failure() is True
            assert result.error_type == ERROR_EMAIL_VERIFICATION_REQUIRED

            async with app_connection_scope(web_app) as session:
                refreshed_user = await User.get_or_none(id=user.id, using_db=session)
                assert refreshed_user is not None
                assert refreshed_user.last_login_at is None

        await run_auth_app_test(web_app, assert_unverified_user_rejected)

    def test_is_user_effectively_active_uses_exclusive_expiry_boundary(self) -> None:
        now = 200.0
        user = User(email="boundary@example.com", hashed_password="hash")
        user.is_active = True

        user.expires_at = now
        assert identity_management.is_user_effectively_active(user, now=now) is False

        user.expires_at = now + 0.001
        assert identity_management.is_user_effectively_active(user, now=now) is True

    @pytest.mark.anyio
    async def test_request_verification_records_email_verification_sent_timestamp(
        self,
        tmp_path: Path,
    ) -> None:
        web_app = create_auth_test_app(
            database_url=sqlite_file_url(tmp_path / "verification-timestamp.sqlite3")
        )
        web_app.state.identity_delivery = CaptureDelivery(verification_tokens=[])

        async def assert_verification_timestamp() -> None:
            await initialise_app_identity_database(web_app)

            async with app_connection_scope(web_app) as session:
                manager = create_user_manager(
                    session,
                    web_app.state.auth_settings.identity_options,
                )
                await manager.create(
                    UserCreate(
                        email="verify-time@example.com",
                        password=STRONG_TEST_PASSWORD,
                    ),
                    safe=True,
                )

            await identity_sessions.request_verification(
                Request({"type": "http", "app": web_app}),
                "verify-time@example.com",
            )

            async with app_connection_scope(web_app) as session:
                user = await User.get(email="verify-time@example.com", using_db=session)
                assert isinstance(user.email_verification_sent_at, float)
                assert user.email_verification_sent_at > 0

        await run_auth_app_test(web_app, assert_verification_timestamp)

    @pytest.mark.anyio
    async def test_request_verification_does_not_record_timestamp_when_delivery_fails(
        self,
        tmp_path: Path,
    ) -> None:
        web_app = create_auth_test_app(
            database_url=sqlite_file_url(
                tmp_path / "verification-delivery-fails.sqlite3"
            )
        )
        web_app.state.identity_delivery = FailingVerificationDelivery()

        async def assert_failed_delivery_does_not_throttle_user() -> None:
            await initialise_app_identity_database(web_app)

            async with app_connection_scope(web_app) as session:
                manager = create_user_manager(
                    session,
                    web_app.state.auth_settings.identity_options,
                )
                await manager.create(
                    UserCreate(
                        email="verify-atomic@example.com",
                        password=STRONG_TEST_PASSWORD,
                    ),
                    safe=True,
                )

            with pytest.raises(RuntimeError, match="verification delivery failed"):
                await identity_sessions.request_verification(
                    Request({"type": "http", "app": web_app}),
                    "verify-atomic@example.com",
                )

            async with app_connection_scope(web_app) as session:
                user = await User.get(
                    email="verify-atomic@example.com", using_db=session
                )
                assert user.email_verification_sent_at is None

        await run_auth_app_test(web_app, assert_failed_delivery_does_not_throttle_user)

    @pytest.mark.anyio
    async def test_request_verification_ignores_missing_users_without_modifying_rows(
        self,
        tmp_path: Path,
    ) -> None:
        web_app = create_auth_test_app(
            database_url=sqlite_file_url(tmp_path / "verification-missing-user.sqlite3")
        )
        web_app.state.identity_delivery = CaptureDelivery(verification_tokens=[])

        async def assert_missing_user_is_ignored() -> None:
            await initialise_app_identity_database(web_app)

            await identity_sessions.request_verification(
                Request({"type": "http", "app": web_app}),
                "missing@example.com",
            )

            async with app_connection_scope(web_app) as session:
                users = list(await User.all().using_db(session))
                assert users == []
                assert web_app.state.identity_delivery.verification_tokens == []

        await run_auth_app_test(web_app, assert_missing_user_is_ignored)

    @pytest.mark.anyio
    async def test_request_verification_rate_limits_recent_delivery(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        web_app = create_auth_test_app(
            database_url=sqlite_file_url(tmp_path / "verification-rate-limit.sqlite3")
        )
        web_app.state.identity_delivery = CaptureDelivery(verification_tokens=[])

        async def assert_recent_delivery_is_rate_limited() -> None:
            await initialise_app_identity_database(web_app)

            async with app_connection_scope(web_app) as session:
                manager = create_user_manager(
                    session,
                    web_app.state.auth_settings.identity_options,
                )
                user = await manager.create(
                    UserCreate(
                        email="verify-limited@example.com",
                        password=STRONG_TEST_PASSWORD,
                    ),
                    safe=True,
                )
                user.email_verification_sent_at = 1_000.0
                await user.save(using_db=session)

            monkeypatch.setattr(identity_sessions, "current_timestamp", lambda: 1_120.0)
            await identity_sessions.request_verification(
                Request({"type": "http", "app": web_app}),
                "verify-limited@example.com",
            )

            async with app_connection_scope(web_app) as session:
                user = await User.get(
                    email="verify-limited@example.com",
                    using_db=session,
                )
                assert user.email_verification_sent_at == 1_000.0
                assert web_app.state.identity_delivery.verification_tokens == []

        await run_auth_app_test(web_app, assert_recent_delivery_is_rate_limited)

    @pytest.mark.anyio
    @pytest.mark.parametrize(
        ("field_name", "field_value"),
        [
            ("is_active", False),
            ("expires_at", time() - 60),
        ],
    )
    async def test_request_verification_does_not_overwrite_ineligible_user_timestamp(
        self,
        field_name: str,
        field_value: object,
        tmp_path: Path,
    ) -> None:
        web_app = create_auth_test_app(
            database_url=sqlite_file_url(
                tmp_path / f"verification-ineligible-{field_name}.sqlite3"
            )
        )
        web_app.state.identity_delivery = CaptureDelivery(verification_tokens=[])

        async def assert_ineligible_user_timestamp_is_preserved() -> None:
            await initialise_app_identity_database(web_app)

            async with app_connection_scope(web_app) as session:
                manager = create_user_manager(
                    session,
                    web_app.state.auth_settings.identity_options,
                )
                user = await manager.create(
                    UserCreate(
                        email=f"{field_name.replace('_', '-')}@example.com",
                        password=STRONG_TEST_PASSWORD,
                    ),
                    safe=True,
                )
                setattr(user, field_name, field_value)
                user.email_verification_sent_at = 123.0
                await user.save(using_db=session)
                email = user.email

            await identity_sessions.request_verification(
                Request({"type": "http", "app": web_app}),
                email,
            )

            async with app_connection_scope(web_app) as session:
                refreshed_user = await User.get(email=email, using_db=session)
                assert refreshed_user.email_verification_sent_at == 123.0
                assert web_app.state.identity_delivery.verification_tokens == []

        await run_auth_app_test(web_app, assert_ineligible_user_timestamp_is_preserved)

    @pytest.mark.anyio
    async def test_authmgr_create_user_with_metadata_from_stdin_password(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        database_url = sqlite_file_url(tmp_path / "users.sqlite3")
        await initialise_identity_database(database_url)
        set_authmgr_database_url(monkeypatch, tmp_path, database_url)
        monkeypatch.setattr(sys, "stdin", io.StringIO(f"{STRONG_TEST_PASSWORD}\n"))

        exit_code = await run_authmgr_command(
            [
                "user",
                "create",
                "operator@example.com",
                "--password",
                "-",
                "--admin",
                "--superuser",
                "--unverified",
                "--timezone",
                "Australia/Melbourne",
                "--expires-at",
                "4102444800",
            ]
        )

        assert exit_code == 0

        [user] = await identity_users_from_database(database_url)
        assert user.email == "operator@example.com"
        assert user.hashed_password != STRONG_TEST_PASSWORD
        assert user.is_admin is True
        assert user.is_superuser is True
        assert user.is_verified is False
        assert user.preferred_timezone == "Australia/Melbourne"
        assert user.expires_at == 4102444800.0

    @pytest.mark.anyio
    async def test_authmgr_create_user_with_totp_outputs_one_time_material(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        database_url = sqlite_file_url(tmp_path / "create-totp.sqlite3")
        await initialise_identity_database(database_url)
        set_authmgr_database_url(monkeypatch, tmp_path, database_url)
        monkeypatch.setattr(sys, "stdin", io.StringIO(f"{STRONG_TEST_PASSWORD}\n"))

        exit_code = await run_authmgr_command(
            [
                "user",
                "create",
                "totp-create@example.com",
                "--password",
                "-",
                "--totp",
            ]
        )

        assert exit_code == 0
        captured = capsys.readouterr()
        assert "created user: totp-create@example.com" in captured.out
        assert "TOTP secret:" not in captured.out
        assert "Recovery codes:" not in captured.out
        assert "Operator credential material" in captured.err
        assert "TOTP secret:" in captured.err
        assert "otpauth://totp/Wybra:totp-create%40example.com?" in captured.err
        assert "Recovery codes:" in captured.err
        credentials = await totp_credentials_from_database(
            database_url, "totp-create@example.com"
        )
        assert [credential.status for credential in credentials] == ["active"]
        assert credentials[0].crypt_secret not in captured.err
        recovery_codes = await totp_recovery_codes_from_database(
            database_url, credentials[0]
        )
        assert len(recovery_codes) == 10
        assert all(code.code_verifier not in captured.err for code in recovery_codes)

    @pytest.mark.anyio
    async def test_authmgr_create_user_with_totp_supports_json_output(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        database_url = sqlite_file_url(tmp_path / "create-totp-json.sqlite3")
        await initialise_identity_database(database_url)
        set_authmgr_database_url(monkeypatch, tmp_path, database_url)
        monkeypatch.setattr(sys, "stdin", io.StringIO(f"{STRONG_TEST_PASSWORD}\n"))

        exit_code = await run_authmgr_command(
            [
                "user",
                "create",
                "totp-json@example.com",
                "--password",
                "-",
                "--totp",
                "--json",
            ]
        )

        assert exit_code == 0
        payload = json.loads(capsys.readouterr().out)
        assert payload["user"]["email"] == "totp-json@example.com"
        assert payload["totp"] == {
            "provisioned": True,
            "recovery_codes_generated": True,
        }
        assert "secret" not in payload["totp"]
        assert "provisioning_uri" not in payload["totp"]
        assert "recovery_codes" not in payload["totp"]

    @pytest.mark.anyio
    async def test_authmgr_create_user_with_totp_can_include_sensitive_json_output(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        database_url = sqlite_file_url(tmp_path / "create-totp-json-sensitive.sqlite3")
        await initialise_identity_database(database_url)
        set_authmgr_database_url(monkeypatch, tmp_path, database_url)
        monkeypatch.setattr(sys, "stdin", io.StringIO(f"{STRONG_TEST_PASSWORD}\n"))

        exit_code = await run_authmgr_command(
            [
                "user",
                "create",
                "totp-json-sensitive@example.com",
                "--password",
                "-",
                "--totp",
                "--json",
                "--include-secrets",
            ]
        )

        assert exit_code == 0
        payload = json.loads(capsys.readouterr().out)
        assert payload["user"]["email"] == "totp-json-sensitive@example.com"
        assert payload["totp"]["secret"]
        assert payload["totp"]["provisioning_uri"].startswith("otpauth://totp/")
        assert len(payload["totp"]["recovery_codes"]) == 10

    @pytest.mark.anyio
    async def test_authmgr_create_user_with_totp_does_not_provision_after_create_failure(  # noqa: E501
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        database_url = sqlite_file_url(tmp_path / "create-totp-failure.sqlite3")
        await initialise_identity_database(database_url)
        set_authmgr_database_url(monkeypatch, tmp_path, database_url)
        monkeypatch.setattr(sys, "stdin", io.StringIO(f"{STRONG_TEST_PASSWORD}\n"))
        assert (
            await run_authmgr_command(
                ["user", "create", "duplicate-totp@example.com", "--password", "-"]
            )
            == 0
        )
        capsys.readouterr()

        monkeypatch.setattr(sys, "stdin", io.StringIO(f"{STRONG_TEST_PASSWORD}\n"))
        exit_code = await run_authmgr_command(
            [
                "user",
                "create",
                "duplicate-totp@example.com",
                "--password",
                "-",
                "--totp",
            ]
        )

        captured = capsys.readouterr()
        assert exit_code == 1
        assert "already exists" in captured.err
        assert "TOTP secret:" not in captured.out
        assert "otpauth://" not in captured.out
        assert "Recovery codes:" not in captured.out
        assert "TOTP secret:" not in captured.err
        assert "otpauth://" not in captured.err
        assert "Recovery codes:" not in captured.err
        assert (
            await totp_credentials_from_database(
                database_url, "duplicate-totp@example.com"
            )
            == []
        )

    @pytest.mark.anyio
    async def test_authmgr_update_totp_replaces_existing_credential_and_recovery_codes(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        database_url = sqlite_file_url(tmp_path / "update-totp.sqlite3")
        await initialise_identity_database(database_url)
        set_authmgr_database_url(monkeypatch, tmp_path, database_url)
        monkeypatch.setattr(sys, "stdin", io.StringIO(f"{STRONG_TEST_PASSWORD}\n"))
        assert (
            await run_authmgr_command(
                [
                    "user",
                    "create",
                    "totp-update@example.com",
                    "--password",
                    "-",
                    "--totp",
                ]
            )
            == 0
        )
        old_credentials = await totp_credentials_from_database(
            database_url,
            "totp-update@example.com",
        )
        capsys.readouterr()

        exit_code = await run_authmgr_command(
            [
                "user",
                "update",
                "totp-update@example.com",
                "--totp",
                "--json",
                "--include-secrets",
            ]
        )

        assert exit_code == 0
        payload = json.loads(capsys.readouterr().out)
        assert payload["user"]["email"] == "totp-update@example.com"
        assert payload["totp"]["secret"]
        assert len(payload["totp"]["recovery_codes"]) == 10
        credentials = await totp_credentials_from_database(
            database_url, "totp-update@example.com"
        )
        assert [credential.status for credential in credentials] == [
            "disabled",
            "active",
        ]
        assert credentials[0].id == old_credentials[0].id
        assert credentials[1].id != old_credentials[0].id
        assert (
            len(await totp_recovery_codes_from_database(database_url, credentials[1]))
            == 10
        )

    @pytest.mark.anyio
    async def test_authmgr_update_rcodes_rotates_codes_without_replacing_totp_key(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        database_url = sqlite_file_url(tmp_path / "update-rcodes.sqlite3")
        await initialise_identity_database(database_url)
        set_authmgr_database_url(monkeypatch, tmp_path, database_url)
        monkeypatch.setattr(sys, "stdin", io.StringIO(f"{STRONG_TEST_PASSWORD}\n"))
        assert (
            await run_authmgr_command(
                ["user", "create", "rcodes@example.com", "--password", "-", "--totp"]
            )
            == 0
        )
        [credential] = await totp_credentials_from_database(
            database_url, "rcodes@example.com"
        )
        original_verifiers = {
            code.code_verifier
            for code in await totp_recovery_codes_from_database(
                database_url, credential
            )
        }
        capsys.readouterr()

        exit_code = await run_authmgr_command(
            ["user", "update", "rcodes@example.com", "--rcodes", "--json"]
        )

        assert exit_code == 0
        payload = json.loads(capsys.readouterr().out)
        assert "secret" not in payload["totp"]
        assert "provisioning_uri" not in payload["totp"]
        assert "recovery_codes" not in payload["totp"]
        assert payload["totp"] == {"recovery_codes_generated": True}
        [refreshed_credential] = await totp_credentials_from_database(
            database_url,
            "rcodes@example.com",
        )
        assert refreshed_credential.id == credential.id
        refreshed_verifiers = {
            code.code_verifier
            for code in await totp_recovery_codes_from_database(
                database_url,
                refreshed_credential,
            )
        }
        assert refreshed_verifiers
        assert refreshed_verifiers.isdisjoint(original_verifiers)

    @pytest.mark.anyio
    async def test_authmgr_update_no_totp_disables_active_totp_without_secret_output(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        database_url = sqlite_file_url(tmp_path / "disable-totp.sqlite3")
        await initialise_identity_database(database_url)
        set_authmgr_database_url(monkeypatch, tmp_path, database_url)
        monkeypatch.setattr(sys, "stdin", io.StringIO(f"{STRONG_TEST_PASSWORD}\n"))
        assert (
            await run_authmgr_command(
                [
                    "user",
                    "create",
                    "disable-totp@example.com",
                    "--password",
                    "-",
                    "--totp",
                ]
            )
            == 0
        )
        capsys.readouterr()

        exit_code = await run_authmgr_command(
            ["user", "update", "disable-totp@example.com", "--no-totp"]
        )

        assert exit_code == 0
        captured = capsys.readouterr()
        assert "updated user: disable-totp@example.com" in captured.out
        assert "TOTP secret:" not in captured.out
        assert "otpauth://" not in captured.out
        assert "Recovery codes:" not in captured.out
        assert "TOTP secret:" not in captured.err
        assert "otpauth://" not in captured.err
        assert "Recovery codes:" not in captured.err
        credentials = await totp_credentials_from_database(
            database_url,
            "disable-totp@example.com",
        )
        assert [credential.status for credential in credentials] == ["disabled"]

    @pytest.mark.parametrize(
        ("argv", "expected_message"),
        [
            pytest.param(
                ["user", "create", "person@example.com", "--rcodes"],
                "No such option '--rcodes'",
                id="create-rcodes",
            ),
            pytest.param(
                ["user", "update", "person@example.com", "--totp", "--no-totp"],
                "not allowed with option '--totp'",
                id="totp-no-totp",
            ),
            pytest.param(
                ["user", "update", "person@example.com", "--totp", "--rcodes"],
                "not allowed with option '--totp'",
                id="totp-rcodes",
            ),
            pytest.param(
                ["user", "update", "person@example.com", "--no-totp", "--rcodes"],
                "not allowed with option '--no-totp'",
                id="no-totp-rcodes",
            ),
            pytest.param(
                ["user", "update", "person@example.com", "--totp", "--revoke-passkey"],
                "not allowed with option '--totp'",
                id="totp-revoke-passkey",
            ),
            pytest.param(
                [
                    "user",
                    "update",
                    "person@example.com",
                    "--no-totp",
                    "--revoke-passkey",
                ],
                "not allowed with option '--no-totp'",
                id="no-totp-revoke-passkey",
            ),
            pytest.param(
                [
                    "user",
                    "update",
                    "person@example.com",
                    "--rcodes",
                    "--revoke-passkey",
                ],
                "not allowed with option '--rcodes'",
                id="rcodes-revoke-passkey",
            ),
        ],
    )
    def test_authmgr_rejects_invalid_totp_option_combinations(
        self,
        argv: list[str],
        expected_message: str,
    ) -> None:
        result = CliRunner().invoke(authmgr.authmgr_command, argv)

        assert result.exit_code == 2
        assert expected_message in result.output

    def test_authmgr_rejects_passkey_csv_output_combination(self) -> None:
        result = CliRunner().invoke(
            authmgr.authmgr_command, ["user", "list", "--csv", "--passkeys"]
        )

        assert result.exit_code == 2
        assert "not allowed with option '--passkeys'" in result.output

    @pytest.mark.anyio
    async def test_authmgr_update_rcodes_requires_active_totp(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        database_url = sqlite_file_url(tmp_path / "rcodes-without-totp.sqlite3")
        await initialise_identity_database(database_url)
        set_authmgr_database_url(monkeypatch, tmp_path, database_url)
        monkeypatch.setattr(sys, "stdin", io.StringIO(f"{STRONG_TEST_PASSWORD}\n"))
        assert (
            await run_authmgr_command(
                ["user", "create", "no-totp@example.com", "--password", "-"]
            )
            == 0
        )
        capsys.readouterr()

        exit_code = await run_authmgr_command(
            ["user", "update", "no-totp@example.com", "--rcodes"]
        )

        assert exit_code == 1
        assert "User does not have active TOTP." in capsys.readouterr().err

    @pytest.mark.anyio
    async def test_authmgr_list_with_passkeys_reports_active_credentials(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        database_url = sqlite_file_url(tmp_path / "list-passkeys.sqlite3")
        await initialise_identity_database(database_url)
        set_authmgr_database_url(monkeypatch, tmp_path, database_url)
        monkeypatch.setattr(sys, "stdin", io.StringIO(f"{STRONG_TEST_PASSWORD}\n"))
        assert (
            await run_authmgr_command(
                ["user", "create", "passkeys@example.com", "--password", "-"]
            )
            == 0
        )
        row_id = await add_webauthn_credential_to_database(
            database_url,
            "passkeys@example.com",
            credential_id="active-credential",
            label="Work laptop",
        )
        await add_webauthn_credential_to_database(
            database_url,
            "passkeys@example.com",
            credential_id="revoked-credential",
            status="revoked",
        )
        capsys.readouterr()

        exit_code = await run_authmgr_command(
            ["user", "list", "--json", "--passkeys", "--email", "passkeys@example.com"]
        )

        assert exit_code == 0
        [record] = json.loads(capsys.readouterr().out)
        assert record["email"] == "passkeys@example.com"
        assert record["passkeys"] == [
            {
                "id": row_id,
                "credential_id": "active-credential",
                "status": "active",
                "label": "Work laptop",
                "created_at": record["passkeys"][0]["created_at"],
                "user_verified": True,
                "credential_device_type": "multi_device",
                "credential_backed_up": True,
                "transports": ["internal"],
            }
        ]

    @pytest.mark.anyio
    async def test_authmgr_update_revoke_passkey_revokes_all_active_credentials(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        database_url = sqlite_file_url(tmp_path / "revoke-all-passkeys.sqlite3")
        await initialise_identity_database(database_url)
        set_authmgr_database_url(monkeypatch, tmp_path, database_url)
        monkeypatch.setattr(sys, "stdin", io.StringIO(f"{STRONG_TEST_PASSWORD}\n"))
        assert (
            await run_authmgr_command(
                ["user", "create", "revoke-all@example.com", "--password", "-"]
            )
            == 0
        )
        await add_webauthn_credential_to_database(
            database_url,
            "revoke-all@example.com",
            credential_id="first-credential",
        )
        await add_webauthn_credential_to_database(
            database_url,
            "revoke-all@example.com",
            credential_id="second-credential",
        )
        capsys.readouterr()

        exit_code = await run_authmgr_command(
            ["user", "update", "revoke-all@example.com", "--revoke-passkey", "--json"]
        )

        assert exit_code == 0
        payload = json.loads(capsys.readouterr().out)
        assert payload["user"]["email"] == "revoke-all@example.com"
        assert {
            passkey["credential_id"] for passkey in payload["passkeys"]["revoked"]
        } == {
            "first-credential",
            "second-credential",
        }
        credentials = await webauthn_credentials_from_database(
            database_url,
            "revoke-all@example.com",
        )
        assert [credential.status for credential in credentials] == [
            "revoked",
            "revoked",
        ]
        assert all(credential.revoked_at is not None for credential in credentials)

    @pytest.mark.anyio
    async def test_authmgr_update_revoke_passkey_can_revoke_single_credential(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        database_url = sqlite_file_url(tmp_path / "revoke-one-passkey.sqlite3")
        await initialise_identity_database(database_url)
        set_authmgr_database_url(monkeypatch, tmp_path, database_url)
        monkeypatch.setattr(sys, "stdin", io.StringIO(f"{STRONG_TEST_PASSWORD}\n"))
        assert (
            await run_authmgr_command(
                ["user", "create", "revoke-one@example.com", "--password", "-"]
            )
            == 0
        )
        row_id = await add_webauthn_credential_to_database(
            database_url,
            "revoke-one@example.com",
            credential_id="row-target",
        )
        await add_webauthn_credential_to_database(
            database_url,
            "revoke-one@example.com",
            credential_id="active-survivor",
        )
        capsys.readouterr()

        exit_code = await run_authmgr_command(
            ["user", "update", "revoke-one@example.com", "--revoke-passkey", row_id]
        )

        assert exit_code == 0
        captured = capsys.readouterr()
        assert "updated user: revoke-one@example.com" in captured.out
        assert "revoked passkeys: 1" in captured.out
        credentials = await webauthn_credentials_from_database(
            database_url,
            "revoke-one@example.com",
        )
        assert [credential.status for credential in credentials] == [
            "revoked",
            "active",
        ]

    @pytest.mark.anyio
    async def test_authmgr_update_revoke_passkey_accepts_public_credential_id(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        database_url = sqlite_file_url(tmp_path / "revoke-public-passkey.sqlite3")
        await initialise_identity_database(database_url)
        set_authmgr_database_url(monkeypatch, tmp_path, database_url)
        monkeypatch.setattr(sys, "stdin", io.StringIO(f"{STRONG_TEST_PASSWORD}\n"))
        assert (
            await run_authmgr_command(
                ["user", "create", "revoke-public@example.com", "--password", "-"]
            )
            == 0
        )
        await add_webauthn_credential_to_database(
            database_url,
            "revoke-public@example.com",
            credential_id="public-target",
        )
        capsys.readouterr()

        exit_code = await run_authmgr_command(
            [
                "user",
                "update",
                "revoke-public@example.com",
                "--revoke-passkey",
                "public-target",
            ]
        )

        assert exit_code == 0
        [credential] = await webauthn_credentials_from_database(
            database_url,
            "revoke-public@example.com",
        )
        assert credential.status == "revoked"

    @pytest.mark.anyio
    async def test_authmgr_update_revoke_passkey_requires_active_passkey(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        database_url = sqlite_file_url(tmp_path / "revoke-no-passkeys.sqlite3")
        await initialise_identity_database(database_url)
        set_authmgr_database_url(monkeypatch, tmp_path, database_url)
        monkeypatch.setattr(sys, "stdin", io.StringIO(f"{STRONG_TEST_PASSWORD}\n"))
        assert (
            await run_authmgr_command(
                ["user", "create", "no-passkeys@example.com", "--password", "-"]
            )
            == 0
        )
        capsys.readouterr()

        exit_code = await run_authmgr_command(
            ["user", "update", "no-passkeys@example.com", "--revoke-passkey"]
        )

        assert exit_code == 1
        assert "User does not have active passkeys." in capsys.readouterr().err

    @pytest.mark.anyio
    async def test_authmgr_scope_commands_manage_scope_records(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        database_url = sqlite_file_url(tmp_path / "scope-cli.sqlite3")
        await initialise_identity_database(database_url)
        set_authmgr_database_url(monkeypatch, tmp_path, database_url)

        assert (
            await run_authmgr_command(
                [
                    "scope",
                    "create",
                    "document:read",
                    "--description",
                    "Read documents.",
                ]
            )
            == 0
        )
        assert (
            await run_authmgr_command(
                [
                    "scope",
                    "update",
                    "document:read",
                    "--description",
                    "Read published documents.",
                ]
            )
            == 0
        )
        assert await run_authmgr_command(["scope", "list", "--json"]) == 0
        listed = json.loads(capsys.readouterr().out.splitlines()[-1])

        assert listed == [
            {
                "scope": "document:read",
                "description": "Read published documents.",
            }
        ]

        assert await run_authmgr_command(["scope", "delete", "document:read"]) == 0

        assert await scopes_from_database(database_url) == []

    @pytest.mark.anyio
    async def test_authmgr_scope_delete_rejects_used_scope(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        database_url = sqlite_file_url(tmp_path / "used-scope-cli.sqlite3")
        await initialise_identity_database(database_url)
        set_authmgr_database_url(monkeypatch, tmp_path, database_url)

        assert await run_authmgr_command(["scope", "create", "admin:read"]) == 0
        assert (
            await run_authmgr_command(
                ["group", "create", "admins", "--scope", "admin:read"]
            )
            == 0
        )

        assert await run_authmgr_command(["scope", "delete", "admin:read"]) == 1

        assert "Scope is assigned to one or more groups." in capsys.readouterr().err
        assert [scope.scope for scope in await scopes_from_database(database_url)] == [
            "admin:read"
        ]

    @pytest.mark.anyio
    async def test_authmgr_group_target_first_commands_manage_group(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        database_url = sqlite_file_url(tmp_path / "group-cli.sqlite3")
        await initialise_identity_database(database_url)
        set_authmgr_database_url(monkeypatch, tmp_path, database_url)

        assert await run_authmgr_command(["scope", "create", "project:read"]) == 0
        assert await run_authmgr_command(["scope", "create", "project:write"]) == 0
        assert (
            await run_authmgr_command(
                [
                    "group",
                    "create",
                    "project",
                    "--description",
                    "Project access",
                    "--scope",
                    "project:read",
                ]
            )
            == 0
        )
        assert (
            await run_authmgr_command(
                [
                    "group",
                    "project",
                    "update",
                    "--description",
                    "Project operators",
                    "--scope",
                    "project:write",
                    "--rm-scope",
                    "project:read",
                ]
            )
            == 0
        )
        assert await run_authmgr_command(["group", "project", "show", "--json"]) == 0
        shown = json.loads(capsys.readouterr().out.splitlines()[-1])

        assert shown["abbrev"] == "project"
        assert shown["description"] == "Project operators"
        assert shown["scopes"] == ["project:write"]
        assert await group_scopes_from_database(database_url, "project") == [
            "project:write"
        ]

        assert await run_authmgr_command(["group", "project", "delete", "--force"]) == 0

    @pytest.mark.anyio
    async def test_authmgr_group_membership_commands_manage_users_and_child_groups(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        database_url = sqlite_file_url(tmp_path / "group-membership-cli.sqlite3")
        await initialise_identity_database(database_url)
        set_authmgr_database_url(monkeypatch, tmp_path, database_url)
        monkeypatch.setattr(sys, "stdin", io.StringIO(f"{STRONG_TEST_PASSWORD}\n"))

        assert (
            await run_authmgr_command(
                ["user", "create", "member@example.com", "--password", "-"]
            )
            == 0
        )
        assert await run_authmgr_command(["group", "create", "parent"]) == 0
        assert await run_authmgr_command(["group", "create", "child"]) == 0
        assert (
            await run_authmgr_command(
                ["group", "parent", "add-user", "member@example.com"]
            )
            == 0
        )
        assert await run_authmgr_command(["group", "parent", "add-group", "child"]) == 0
        assert await run_authmgr_command(["group", "parent", "show", "--json"]) == 0
        shown = json.loads(capsys.readouterr().out.splitlines()[-1])

        assert shown["users"] == ["member@example.com"]
        assert shown["child_groups"] == ["child"]

        assert (
            await run_authmgr_command(
                ["group", "parent", "remove-user", "member@example.com"]
            )
            == 0
        )
        assert (
            await run_authmgr_command(["group", "parent", "remove-group", "child"]) == 0
        )
        assert await run_authmgr_command(["group", "parent", "delete", "--force"]) == 0

    def test_authmgr_group_parser_disambiguates_user_and_group_targets(self) -> None:
        ctx = click.Context(authmgr.authmgr_command, obj={})

        user_args = authmgr_groups._target_group_args(
            ctx,
            ("parent", "add-user", "member@example.com"),
        )
        group_args = authmgr_groups._target_group_args(
            ctx, ("parent", "add-group", "child")
        )

        assert user_args.user_target == "member@example.com"
        assert user_args.child_group_target == ""
        assert group_args.user_target == ""
        assert group_args.child_group_target == "child"

    @pytest.mark.anyio
    async def test_authmgr_create_and_update_user_group_memberships(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        database_url = sqlite_file_url(tmp_path / "user-groups-cli.sqlite3")
        await initialise_identity_database(database_url)
        set_authmgr_database_url(monkeypatch, tmp_path, database_url)
        monkeypatch.setattr(
            sys,
            "stdin",
            io.StringIO(f"{STRONG_TEST_PASSWORD}\n"),
        )

        for abbrev in ("alpha", "beta", "gamma"):
            assert await run_authmgr_command(["group", "create", abbrev]) == 0
        assert (
            await run_authmgr_command(
                [
                    "user",
                    "create",
                    "grouped@example.com",
                    "--password",
                    "-",
                    "--group",
                    "alpha",
                    "--group",
                    "beta",
                ]
            )
            == 0
        )
        assert await user_group_abbrevs_from_database(
            database_url, "grouped@example.com"
        ) == [
            "alpha",
            "beta",
        ]

        assert (
            await run_authmgr_command(
                [
                    "user",
                    "update",
                    "grouped@example.com",
                    "--rm-group",
                    "alpha",
                    "--add-group",
                    "gamma",
                ]
            )
            == 0
        )
        assert "updated user: grouped@example.com" in capsys.readouterr().out
        assert await user_group_abbrevs_from_database(
            database_url, "grouped@example.com"
        ) == [
            "beta",
            "gamma",
        ]

        assert (
            await run_authmgr_command(
                [
                    "user",
                    "update",
                    "grouped@example.com",
                    "--set-group",
                    "alpha",
                ]
            )
            == 0
        )
        assert await user_group_abbrevs_from_database(
            database_url, "grouped@example.com"
        ) == ["alpha"]

    @pytest.mark.anyio
    async def test_authmgr_create_with_missing_group_does_not_create_user(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        database_url = sqlite_file_url(tmp_path / "missing-create-group-cli.sqlite3")
        await initialise_identity_database(database_url)
        set_authmgr_database_url(monkeypatch, tmp_path, database_url)
        stdin = io.StringIO(f"{STRONG_TEST_PASSWORD}\n")
        monkeypatch.setattr(sys, "stdin", stdin)

        assert (
            await run_authmgr_command(
                [
                    "user",
                    "create",
                    "missing-create-group@example.com",
                    "--password",
                    "-",
                    "--group",
                    "missing",
                ]
            )
            == 1
        )

        assert stdin.tell() == 0
        assert (
            await identity_user_from_database(
                database_url, "missing-create-group@example.com"
            )
            is None
        )

    @pytest.mark.anyio
    async def test_authmgr_set_group_validates_targets_before_replacing_memberships(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        database_url = sqlite_file_url(tmp_path / "user-groups-invalid-cli.sqlite3")
        await initialise_identity_database(database_url)
        set_authmgr_database_url(monkeypatch, tmp_path, database_url)
        monkeypatch.setattr(sys, "stdin", io.StringIO(f"{STRONG_TEST_PASSWORD}\n"))

        assert await run_authmgr_command(["group", "create", "alpha"]) == 0
        assert await run_authmgr_command(["group", "create", "beta"]) == 0
        assert (
            await run_authmgr_command(
                [
                    "user",
                    "create",
                    "invalid-set@example.com",
                    "--password",
                    "-",
                    "--group",
                    "alpha",
                ]
            )
            == 0
        )

        assert (
            await run_authmgr_command(
                [
                    "user",
                    "update",
                    "invalid-set@example.com",
                    "--set-group",
                    "beta",
                    "--set-group",
                    "missing",
                ]
            )
            == 1
        )

        assert await user_group_abbrevs_from_database(
            database_url, "invalid-set@example.com"
        ) == ["alpha"]

    def test_authmgr_update_rejects_group_replacement_shortcut(self) -> None:
        result = CliRunner().invoke(
            authmgr.authmgr_command,
            ["user", "update", "user@example.com", "--group", "admins"],
        )

        assert result.exit_code == 2
        assert "use --set-group for replacement" in result.output

    @pytest.mark.parametrize("incremental_option", ["--add-group", "--rm-group"])
    def test_authmgr_update_rejects_group_replacement_with_incremental_edits(
        self,
        incremental_option: str,
    ) -> None:
        result = CliRunner().invoke(
            authmgr.authmgr_command,
            [
                "user",
                "update",
                "user@example.com",
                "--set-group",
                "admins",
                incremental_option,
                "editors",
            ],
        )

        assert result.exit_code == 2
        assert (
            "--set-group cannot be used with --add-group or --rm-group."
            in result.output
        )

    def test_authmgr_record_formatting_json_encodes_nested_values(
        self,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        record = {
            "email": "nested@example.com",
            "groups": [{"abbrev": "admins", "scopes": ["read", "write"]}],
        }

        assert authmgr_output._format_record_value(record["groups"]) == (
            '[{"abbrev": "admins", "scopes": ["read", "write"]}]'
        )

        authmgr_output._print_records(
            [record],
            field_names=("email", "groups"),
            json_output=False,
            csv_output=True,
        )
        rows = list(csv.DictReader(io.StringIO(capsys.readouterr().out)))

        assert rows == [
            {
                "email": "nested@example.com",
                "groups": '[{"abbrev": "admins", "scopes": ["read", "write"]}]',
            }
        ]

    def test_authmgr_totp_material_output_ignores_empty_payload(
        self,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        authmgr_runtime._print_totp_material({"user": {}})

        captured = capsys.readouterr()
        assert captured.out == ""
        assert captured.err == ""

    @pytest.mark.anyio
    async def test_authmgr_group_effective_scopes_reports_folded_scopes(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        database_url = sqlite_file_url(tmp_path / "effective-scopes-cli.sqlite3")
        await initialise_identity_database(database_url)
        set_authmgr_database_url(monkeypatch, tmp_path, database_url)
        monkeypatch.setattr(sys, "stdin", io.StringIO(f"{STRONG_TEST_PASSWORD}\n"))

        assert await run_authmgr_command(["scope", "create", "project:read"]) == 0
        assert (
            await run_authmgr_command(
                ["group", "create", "readers", "--scope", "project:read"]
            )
            == 0
        )
        assert (
            await run_authmgr_command(
                ["user", "create", "reader@example.com", "--password", "-"]
            )
            == 0
        )
        assert (
            await run_authmgr_command(
                ["group", "readers", "add-user", "reader@example.com"]
            )
            == 0
        )

        assert (
            await run_authmgr_command(
                ["group", "effective-scopes", "reader@example.com", "--json"]
            )
            == 0
        )
        effective_scopes = json.loads(capsys.readouterr().out.splitlines()[-1])

        assert effective_scopes["scopes"] == ["project:read"]
        assert effective_scopes["groups"] == ["readers"]
        assert effective_scopes["user"]["email"] == "reader@example.com"

    @pytest.mark.anyio
    async def test_authmgr_create_rejects_invalid_timezone_without_creating_user(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        database_url = sqlite_file_url(tmp_path / "invalid-create-timezone.sqlite3")
        await initialise_identity_database(database_url)
        set_authmgr_database_url(monkeypatch, tmp_path, database_url)
        monkeypatch.setattr(sys, "stdin", io.StringIO(f"{STRONG_TEST_PASSWORD}\n"))

        exit_code = await run_authmgr_command(
            [
                "user",
                "create",
                "invalid-timezone@example.com",
                "--password",
                "-",
                "--timezone",
                "Not/AZone",
            ]
        )

        assert exit_code == 1
        assert "Preferred timezone is invalid." in capsys.readouterr().err
        assert await identity_users_from_database(database_url) == []

    @pytest.mark.anyio
    @pytest.mark.parametrize(
        "invalid_timezone",
        ["Not/AZone", "../UTC"],
    )
    async def test_authmgr_update_rejects_invalid_timezone_without_updating_user(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
        invalid_timezone: str,
    ) -> None:
        database_url = sqlite_file_url(tmp_path / "invalid-update-timezone.sqlite3")
        await initialise_identity_database(database_url)
        set_authmgr_database_url(monkeypatch, tmp_path, database_url)
        monkeypatch.setattr(sys, "stdin", io.StringIO(f"{STRONG_TEST_PASSWORD}\n"))
        assert (
            await run_authmgr_command(
                [
                    "user",
                    "create",
                    "invalid-update-timezone@example.com",
                    "--password",
                    "-",
                    "--timezone",
                    "UTC",
                ]
            )
            == 0
        )
        capsys.readouterr()

        exit_code = await run_authmgr_command(
            [
                "user",
                "update",
                "invalid-update-timezone@example.com",
                "--timezone",
                invalid_timezone,
            ]
        )

        assert exit_code == 1
        assert "Preferred timezone is invalid." in capsys.readouterr().err
        [user] = await identity_users_from_database(database_url)
        assert user.preferred_timezone == "UTC"

    def test_authmgr_password_from_stdin_trims_crlf(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(sys, "stdin", io.StringIO("correct horse\r\n"))

        assert authmgr_passwords._read_password("-") == "correct horse"

    def test_authmgr_password_from_stdin_rejects_extra_data(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(sys, "stdin", io.StringIO("correct horse\nextra\n"))

        with pytest.raises(
            authmgr_passwords.PasswordSourceError, match="exactly one line"
        ):
            authmgr_passwords._read_password("-")

    def test_authmgr_password_from_stdin_preserves_whitespace_and_strips_newline(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(sys, "stdin", io.StringIO("  spacey  \n"))

        assert authmgr_passwords._read_password("-") == "  spacey  "

    def test_authmgr_password_from_stdin_rejects_empty_input(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(sys, "stdin", io.StringIO(""))

        with pytest.raises(
            authmgr_passwords.PasswordSourceError, match="No password received"
        ):
            authmgr_passwords._read_password("-")

    def test_authmgr_password_from_stdin_rejects_tty(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        stdin = io.StringIO("correct horse\n")
        stdin.isatty = lambda: True  # type: ignore[method-assign]
        monkeypatch.setattr(sys, "stdin", stdin)

        with pytest.raises(
            authmgr_passwords.PasswordSourceError, match="interactive stdin"
        ):
            authmgr_passwords._read_password("-")

    def test_authmgr_read_password_rejects_invalid_source(self) -> None:
        with pytest.raises(
            authmgr_passwords.PasswordSourceError, match="Unsupported password source"
        ):
            authmgr_passwords._read_password("invalid")

    @pytest.mark.anyio
    async def test_authmgr_create_rejects_duplicate_email(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        database_url = sqlite_file_url(tmp_path / "duplicate.sqlite3")
        await initialise_identity_database(database_url)
        set_authmgr_database_url(monkeypatch, tmp_path, database_url)

        monkeypatch.setattr(sys, "stdin", io.StringIO(f"{STRONG_TEST_PASSWORD}\n"))
        assert (
            await run_authmgr_command(
                ["user", "create", "duplicate@example.com", "--password", "-"]
            )
            == 0
        )

        monkeypatch.setattr(sys, "stdin", io.StringIO(f"{STRONG_TEST_PASSWORD}\n"))
        exit_code = await run_authmgr_command(
            ["user", "create", "duplicate@example.com", "--password", "-"]
        )

        assert exit_code == 1
        assert "already exists" in capsys.readouterr().err
        assert len(await identity_users_from_database(database_url)) == 1

    @pytest.mark.anyio
    async def test_authmgr_create_rejects_duplicate_secondary_email(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        database_url = sqlite_file_url(tmp_path / "duplicate-secondary.sqlite3")
        await initialise_identity_database(database_url)
        web_app = create_auth_test_app(database_url=database_url)

        async def seed_secondary_email_user() -> None:
            await initialise_app_identity_database(web_app)

            async with app_connection_scope(web_app) as session:
                manager = create_user_manager(
                    session,
                    web_app.state.auth_settings.identity_options,
                )
                user = await manager.create(
                    UserCreate(
                        email="primary@example.com",
                        password=STRONG_TEST_PASSWORD,
                    ),
                    safe=True,
                )
                await IdentityUserEmail.create(
                    user_id=user.id,
                    email="linked@example.com",
                    is_primary=False,
                    is_verified=True,
                    using_db=session,
                )

        await run_auth_app_test(web_app, seed_secondary_email_user)
        set_authmgr_database_url(monkeypatch, tmp_path, database_url)
        monkeypatch.setattr(sys, "stdin", io.StringIO(f"{STRONG_TEST_PASSWORD}\n"))

        exit_code = await run_authmgr_command(
            ["user", "create", "linked@example.com", "--password", "-"]
        )

        assert exit_code == 1
        assert "already exists" in capsys.readouterr().err

    @pytest.mark.anyio
    async def test_authmgr_list_json_omits_null_fields(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        database_url = sqlite_file_url(tmp_path / "list-json.sqlite3")
        await initialise_identity_database(database_url)
        set_authmgr_database_url(monkeypatch, tmp_path, database_url)
        monkeypatch.setattr(sys, "stdin", io.StringIO(f"{STRONG_TEST_PASSWORD}\n"))
        assert (
            await run_authmgr_command(
                [
                    "user",
                    "create",
                    "listed@example.com",
                    "--password",
                    "-",
                ]
            )
            == 0
        )
        capsys.readouterr()

        exit_code = await run_authmgr_command(["user", "list", "--json"])

        assert exit_code == 0
        [record] = json.loads(capsys.readouterr().out)
        assert record["email"] == "listed@example.com"
        assert "display_name" not in record
        assert "preferred_name" not in record
        assert "preferred_timezone" not in record
        assert "hashed_password" not in record

    @pytest.mark.anyio
    async def test_authmgr_update_resolves_id_and_updates_user_fields(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        database_url = sqlite_file_url(tmp_path / "update.sqlite3")
        await initialise_identity_database(database_url)
        set_authmgr_database_url(monkeypatch, tmp_path, database_url)
        monkeypatch.setattr(sys, "stdin", io.StringIO(f"{STRONG_TEST_PASSWORD}\n"))
        assert (
            await run_authmgr_command(
                ["user", "create", "update@example.com", "--password", "-"]
            )
            == 0
        )
        [created_user] = await identity_users_from_database(database_url)

        exit_code = await run_authmgr_command(
            [
                "user",
                "update",
                str(created_user.id),
                "--admin",
                "--superuser",
                "--no-verify",
                "--timezone",
                "UTC",
                "--expires-at",
                "4102444800",
            ]
        )

        assert exit_code == 0
        [user] = await identity_users_from_database(database_url)
        assert user.is_admin is True
        assert user.is_superuser is True
        assert user.is_verified is False
        assert user.preferred_timezone == "UTC"
        assert user.expires_at == 4102444800.0

    @pytest.mark.anyio
    async def test_authmgr_update_no_expires_at_without_existing_expiry_is_noop(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        database_url = sqlite_file_url(tmp_path / "no-expiry-noop.sqlite3")
        await initialise_identity_database(database_url)
        set_authmgr_database_url(monkeypatch, tmp_path, database_url)
        monkeypatch.setattr(sys, "stdin", io.StringIO(f"{STRONG_TEST_PASSWORD}\n"))
        assert (
            await run_authmgr_command(
                ["user", "create", "no-expiry@example.com", "--password", "-"]
            )
            == 0
        )
        [created_user] = await identity_users_from_database(database_url)
        capsys.readouterr()

        exit_code = await run_authmgr_command(
            ["user", "update", "no-expiry@example.com", "--no-expires-at"]
        )

        assert exit_code == 1
        assert "No user changes" in capsys.readouterr().err
        [user] = await identity_users_from_database(database_url)
        assert user.expires_at is None
        assert user.modified_at == created_user.modified_at

    @pytest.mark.anyio
    async def test_authmgr_update_can_clear_optional_account_metadata(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        database_url = sqlite_file_url(tmp_path / "clear-optional-fields.sqlite3")
        await initialise_identity_database(database_url)
        set_authmgr_database_url(monkeypatch, tmp_path, database_url)
        monkeypatch.setattr(sys, "stdin", io.StringIO(f"{STRONG_TEST_PASSWORD}\n"))
        assert (
            await run_authmgr_command(
                ["user", "create", "clear@example.com", "--password", "-"]
            )
            == 0
        )
        assert (
            await run_authmgr_command(
                [
                    "user",
                    "update",
                    "clear@example.com",
                    "--timezone",
                    "UTC",
                ]
            )
            == 0
        )

        exit_code = await run_authmgr_command(
            [
                "user",
                "update",
                "clear@example.com",
                "--no-timezone",
            ]
        )

        assert exit_code == 0
        [user] = await identity_users_from_database(database_url)
        assert user.preferred_timezone is None

    @pytest.mark.anyio
    @pytest.mark.parametrize(
        ("target", "expected_message"),
        [
            ("not-a-user-id", "valid user ID"),
            ("not-an-email@", "email address is invalid"),
        ],
    )
    async def test_authmgr_update_reports_malformed_targets(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
        target: str,
        expected_message: str,
    ) -> None:
        database_url = sqlite_file_url(tmp_path / "malformed-target.sqlite3")
        await initialise_identity_database(database_url)
        set_authmgr_database_url(monkeypatch, tmp_path, database_url)

        exit_code = await run_authmgr_command(["user", "update", target, "--admin"])

        assert exit_code == 1
        assert expected_message in capsys.readouterr().err

    @pytest.mark.anyio
    async def test_authmgr_update_rejects_final_superuser_demotion(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        database_url = sqlite_file_url(tmp_path / "final-superuser.sqlite3")
        await initialise_identity_database(database_url)
        set_authmgr_database_url(monkeypatch, tmp_path, database_url)
        monkeypatch.setattr(sys, "stdin", io.StringIO(f"{STRONG_TEST_PASSWORD}\n"))
        assert (
            await run_authmgr_command(
                ["user", "create", "root@example.com", "--password", "-", "--superuser"]
            )
            == 0
        )

        exit_code = await run_authmgr_command(
            ["user", "update", "root@example.com", "--no-superuser"]
        )

        assert exit_code == 1
        assert "final superuser" in capsys.readouterr().err
        [user] = await identity_users_from_database(database_url)
        assert user.is_superuser is True

    @pytest.mark.anyio
    async def test_authmgr_delete_and_deactivate_protect_superusers(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        database_url = sqlite_file_url(tmp_path / "protect-superuser.sqlite3")
        await initialise_identity_database(database_url)
        set_authmgr_database_url(monkeypatch, tmp_path, database_url)
        monkeypatch.setattr(sys, "stdin", io.StringIO(f"{STRONG_TEST_PASSWORD}\n"))
        assert (
            await run_authmgr_command(
                [
                    "user",
                    "create",
                    "protected@example.com",
                    "--password",
                    "-",
                    "--superuser",
                ]
            )
            == 0
        )

        assert (
            await run_authmgr_command(
                ["user", "delete", "protected@example.com", "--force"]
            )
            == 1
        )
        assert (
            await run_authmgr_command(
                ["user", "deactivate", "protected@example.com", "--force"]
            )
            == 1
        )

        captured = capsys.readouterr()
        assert "superuser" in captured.err
        [user] = await identity_users_from_database(database_url)
        assert user.is_active is True

    @pytest.mark.anyio
    async def test_authmgr_delete_protects_non_final_superuser(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        database_url = sqlite_file_url(tmp_path / "protect-non-final-superuser.sqlite3")
        await initialise_identity_database(database_url)
        set_authmgr_database_url(monkeypatch, tmp_path, database_url)
        for email in ("first-root@example.com", "second-root@example.com"):
            monkeypatch.setattr(sys, "stdin", io.StringIO(f"{STRONG_TEST_PASSWORD}\n"))
            assert (
                await run_authmgr_command(
                    ["user", "create", email, "--password", "-", "--superuser"]
                )
                == 0
            )

        exit_code = await run_authmgr_command(
            ["user", "delete", "first-root@example.com", "--force"]
        )

        assert exit_code == 1
        assert "cannot be deleted" in capsys.readouterr().err
        assert {
            user.email for user in await identity_users_from_database(database_url)
        } == {
            "first-root@example.com",
            "second-root@example.com",
        }

    @pytest.mark.anyio
    async def test_authmgr_delete_and_deactivate_normal_users_with_force(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        database_url = sqlite_file_url(tmp_path / "delete-deactivate.sqlite3")
        await initialise_identity_database(database_url)
        set_authmgr_database_url(monkeypatch, tmp_path, database_url)
        monkeypatch.setattr(sys, "stdin", io.StringIO(f"{STRONG_TEST_PASSWORD}\n"))
        assert (
            await run_authmgr_command(
                ["user", "create", "delete@example.com", "--password", "-"]
            )
            == 0
        )
        monkeypatch.setattr(sys, "stdin", io.StringIO(f"{STRONG_TEST_PASSWORD}\n"))
        assert (
            await run_authmgr_command(
                ["user", "create", "deactivate@example.com", "--password", "-"]
            )
            == 0
        )
        delete_token = await create_session_token_for_user(
            database_url, "delete@example.com"
        )
        deactivate_token = await create_session_token_for_user(
            database_url,
            "deactivate@example.com",
        )
        assert set(await access_tokens_from_database(database_url)) == {
            delete_token,
            deactivate_token,
        }

        assert (
            await run_authmgr_command(
                ["user", "delete", "delete@example.com", "--force"]
            )
            == 0
        )
        assert await access_tokens_from_database(database_url) == [deactivate_token]

        assert (
            await run_authmgr_command(
                ["user", "deactivate", "deactivate@example.com", "--force"]
            )
            == 0
        )

        [remaining_user] = await identity_users_from_database(database_url)
        assert remaining_user.email == "deactivate@example.com"
        assert remaining_user.is_active is False
        assert await access_tokens_from_database(database_url) == []

    @pytest.mark.anyio
    async def test_authmgr_deactivate_only_revokes_target_user_sessions(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        database_url = sqlite_file_url(tmp_path / "deactivate-target-sessions.sqlite3")
        await initialise_identity_database(database_url)
        set_authmgr_database_url(monkeypatch, tmp_path, database_url)
        for email in ("alice@example.com", "bob@example.com"):
            monkeypatch.setattr(sys, "stdin", io.StringIO(f"{STRONG_TEST_PASSWORD}\n"))
            assert (
                await run_authmgr_command(["user", "create", email, "--password", "-"])
                == 0
            )

        alice_token = await create_session_token_for_user(
            database_url, "alice@example.com"
        )
        bob_token = await create_session_token_for_user(database_url, "bob@example.com")
        assert set(await access_tokens_from_database(database_url)) == {
            alice_token,
            bob_token,
        }

        assert (
            await run_authmgr_command(
                ["user", "deactivate", "alice@example.com", "--force"]
            )
            == 0
        )

        assert await access_tokens_from_database(database_url) == [bob_token]

    @pytest.mark.anyio
    async def test_authmgr_delete_confirmation_identifies_resolved_user(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        database_url = sqlite_file_url(tmp_path / "delete-confirm.sqlite3")
        await initialise_identity_database(database_url)
        set_authmgr_database_url(monkeypatch, tmp_path, database_url)
        monkeypatch.setattr(sys, "stdin", io.StringIO(f"{STRONG_TEST_PASSWORD}\n"))
        assert (
            await run_authmgr_command(
                ["user", "create", "confirm@example.com", "--password", "-"]
            )
            == 0
        )
        [user] = await identity_users_from_database(database_url)
        monkeypatch.setattr("builtins.input", lambda prompt: print(prompt) or "no")
        capsys.readouterr()

        exit_code = await run_authmgr_command(["user", "delete", str(user.id)])

        assert exit_code == 1
        assert "confirm@example.com" in capsys.readouterr().out
        assert len(await identity_users_from_database(database_url)) == 1

    @pytest.mark.anyio
    async def test_authmgr_password_revokes_sessions_by_default(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        database_url = sqlite_file_url(tmp_path / "password-revoke.sqlite3")
        await initialise_identity_database(database_url)
        set_authmgr_database_url(monkeypatch, tmp_path, database_url)
        monkeypatch.setattr(sys, "stdin", io.StringIO(f"{STRONG_TEST_PASSWORD}\n"))
        assert (
            await run_authmgr_command(
                ["user", "create", "password@example.com", "--password", "-"]
            )
            == 0
        )
        token = await create_session_token_for_user(
            database_url, "password@example.com"
        )
        assert await access_tokens_from_database(database_url) == [token]

        monkeypatch.setattr(
            sys, "stdin", io.StringIO(f"{UPDATED_STRONG_TEST_PASSWORD}\n")
        )
        exit_code = await run_authmgr_command(
            ["user", "password", "password@example.com", "--password", "-"]
        )

        assert exit_code == 0
        assert await access_tokens_from_database(database_url) == []

    @pytest.mark.anyio
    async def test_authmgr_update_password_revokes_sessions_by_default(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        database_url = sqlite_file_url(tmp_path / "update-password-revoke.sqlite3")
        await initialise_identity_database(database_url)
        set_authmgr_database_url(monkeypatch, tmp_path, database_url)
        monkeypatch.setattr(sys, "stdin", io.StringIO(f"{STRONG_TEST_PASSWORD}\n"))
        assert (
            await run_authmgr_command(
                ["user", "create", "update-password@example.com", "--password", "-"]
            )
            == 0
        )
        token = await create_session_token_for_user(
            database_url, "update-password@example.com"
        )
        assert await access_tokens_from_database(database_url) == [token]

        monkeypatch.setattr(
            sys, "stdin", io.StringIO(f"{UPDATED_STRONG_TEST_PASSWORD}\n")
        )
        exit_code = await run_authmgr_command(
            ["user", "update", "update-password@example.com", "--password", "-"]
        )

        assert exit_code == 0
        assert await access_tokens_from_database(database_url) == []

    @pytest.mark.anyio
    async def test_authmgr_password_can_preserve_sessions(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        database_url = sqlite_file_url(tmp_path / "password-preserve.sqlite3")
        await initialise_identity_database(database_url)
        set_authmgr_database_url(monkeypatch, tmp_path, database_url)
        monkeypatch.setattr(sys, "stdin", io.StringIO(f"{STRONG_TEST_PASSWORD}\n"))
        assert (
            await run_authmgr_command(
                ["user", "create", "preserve@example.com", "--password", "-"]
            )
            == 0
        )
        token = await create_session_token_for_user(
            database_url, "preserve@example.com"
        )

        monkeypatch.setattr(
            sys, "stdin", io.StringIO(f"{UPDATED_STRONG_TEST_PASSWORD}\n")
        )
        exit_code = await run_authmgr_command(
            [
                "user",
                "password",
                "preserve@example.com",
                "--password",
                "-",
                "--no-revoke",
            ]
        )

        assert exit_code == 0
        assert await access_tokens_from_database(database_url) == [token]

    @pytest.mark.anyio
    async def test_authmgr_interactive_password_mismatch_aborts_when_input_ends(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        database_url = sqlite_file_url(tmp_path / "password-mismatch.sqlite3")
        await initialise_identity_database(database_url)
        set_authmgr_database_url(monkeypatch, tmp_path, database_url)

        with CliRunner().isolation(input="first password\nsecond password\n") as (
            _stdout,
            stderr,
            output,
        ):
            exit_code = await run_authmgr_command(
                ["user", "create", "mismatch@example.com"]
            )

        assert exit_code == 1
        rendered = output.getvalue().decode()
        assert "created user:" not in rendered
        assert "The two entered values do not match" in rendered
        assert "Aborted" in rendered
        assert "The two entered values do not match" in stderr.getvalue().decode()
        assert await identity_users_from_database(database_url) == []

    @pytest.mark.anyio
    async def test_authmgr_interactive_password_prompt_retries_after_mismatch(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        database_url = sqlite_file_url(tmp_path / "password-retry.sqlite3")
        await initialise_identity_database(database_url)
        set_authmgr_database_url(monkeypatch, tmp_path, database_url)

        with CliRunner().isolation(
            input=(
                "first password\n"
                "second password\n"
                f"{STRONG_TEST_PASSWORD}\n"
                f"{STRONG_TEST_PASSWORD}\n"
            ),
        ) as (_stdout, stderr, output):
            exit_code = await run_authmgr_command(
                ["user", "create", "retry@example.com"]
            )

        assert exit_code == 0
        rendered = output.getvalue().decode()
        assert "Password:" in rendered
        assert "The two entered values do not match" in rendered
        assert "Password:" in stderr.getvalue().decode()
        [user] = await identity_users_from_database(database_url)
        assert user.email == "retry@example.com"

    @pytest.mark.anyio
    @pytest.mark.parametrize("password_source", ["-", "stdin"])
    async def test_authmgr_create_with_stdin_password_does_not_prompt(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
        password_source: str,
    ) -> None:
        source_name = "dash" if password_source == "-" else password_source
        email = f"{source_name}@example.com"
        database_url = sqlite_file_url(
            tmp_path / f"stdin-password-{source_name}.sqlite3"
        )
        await initialise_identity_database(database_url)
        set_authmgr_database_url(monkeypatch, tmp_path, database_url)

        monkeypatch.setattr(sys, "stdin", io.StringIO(f"{STRONG_TEST_PASSWORD}\n"))
        exit_code = await run_authmgr_command(
            ["user", "create", email, "--password", password_source],
        )
        captured = capsys.readouterr()

        assert exit_code == 0
        assert captured.out == f"created user: {email}\n"
        assert "Password:" not in captured.err
        [user] = await identity_users_from_database(database_url)
        assert user.email == email

    @pytest.mark.anyio
    async def test_authmgr_create_with_empty_stdin_password_reports_password_option(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        database_url = sqlite_file_url(tmp_path / "stdin-password-empty.sqlite3")
        await initialise_identity_database(database_url)
        set_authmgr_database_url(monkeypatch, tmp_path, database_url)

        monkeypatch.setattr(sys, "stdin", io.StringIO())
        exit_code = await run_authmgr_command(
            ["user", "create", "empty-stdin@example.com", "--password", "-"],
        )
        captured = capsys.readouterr()

        assert exit_code == 2
        assert "Invalid value for '--password'" in captured.err
        assert "No password received on stdin." in captured.err
        assert await identity_users_from_database(database_url) == []

    @pytest.mark.anyio
    async def test_authmgr_password_command_prompts_by_default(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        database_url = sqlite_file_url(tmp_path / "password-default-prompt.sqlite3")
        await initialise_identity_database(database_url)
        set_authmgr_database_url(monkeypatch, tmp_path, database_url)
        monkeypatch.setattr(sys, "stdin", io.StringIO(f"{STRONG_TEST_PASSWORD}\n"))
        assert (
            await run_authmgr_command(
                ["user", "create", "default-prompt@example.com", "--password", "-"]
            )
            == 0
        )

        with CliRunner().isolation(
            input=(f"{UPDATED_STRONG_TEST_PASSWORD}\n{UPDATED_STRONG_TEST_PASSWORD}\n"),
        ) as (_stdout, stderr, output):
            exit_code = await run_authmgr_command(
                ["user", "password", "default-prompt@example.com"],
            )

        assert exit_code == 0
        rendered = output.getvalue().decode()
        assert "Password:" in rendered
        assert "Password:" in stderr.getvalue().decode()

    @pytest.mark.anyio
    async def test_authmgr_list_filters_by_email_domain_flags_and_effective_activity(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        database_url = sqlite_file_url(tmp_path / "list-filters.sqlite3")
        await initialise_identity_database(database_url)
        set_authmgr_database_url(monkeypatch, tmp_path, database_url)
        for email in ("alpha@example.com", "beta@example.org", "gamma@example.com"):
            monkeypatch.setattr(sys, "stdin", io.StringIO(f"{STRONG_TEST_PASSWORD}\n"))
            assert (
                await run_authmgr_command(["user", "create", email, "--password", "-"])
                == 0
            )
        assert (
            await run_authmgr_command(
                ["user", "update", "alpha@example.com", "--admin"]
            )
            == 0
        )
        assert (
            await run_authmgr_command(
                ["user", "deactivate", "beta@example.org", "--force"]
            )
            == 0
        )
        await update_user_fields(
            database_url, "gamma@example.com", expires_at=time() - 60
        )
        capsys.readouterr()

        assert (
            await run_authmgr_command(
                ["user", "list", "--json", "--domain", "example.com", "--admin"]
            )
            == 0
        )
        [admin_record] = json.loads(capsys.readouterr().out)
        assert admin_record["email"] == "alpha@example.com"

        assert await run_authmgr_command(["user", "list", "--json", "--inactive"]) == 0
        inactive_emails = {
            record["email"] for record in json.loads(capsys.readouterr().out)
        }
        assert inactive_emails == {"beta@example.org", "gamma@example.com"}

    @pytest.mark.anyio
    async def test_authmgr_list_uses_shared_effective_active_timestamp(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        database_url = sqlite_file_url(tmp_path / "list-now.sqlite3")
        await initialise_identity_database(database_url)
        set_authmgr_database_url(monkeypatch, tmp_path, database_url)
        monkeypatch.setattr(sys, "stdin", io.StringIO(f"{STRONG_TEST_PASSWORD}\n"))
        assert (
            await run_authmgr_command(
                ["user", "create", "boundary@example.com", "--password", "-"]
            )
            == 0
        )
        await update_user_fields(database_url, "boundary@example.com", expires_at=200.0)
        capsys.readouterr()

        clock_values = iter([100.0, 300.0])
        monkeypatch.setattr(
            "wybra.auth.admin.management.current_timestamp",
            lambda: next(clock_values),
        )

        assert await run_authmgr_command(["user", "list", "--json", "--active"]) == 0

        [record] = json.loads(capsys.readouterr().out)
        assert record["email"] == "boundary@example.com"
        assert record["effective_active"] is True

    @pytest.mark.anyio
    async def test_authmgr_active_filter_uses_exclusive_expiry_boundary(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        database_url = sqlite_file_url(tmp_path / "active-expiry-boundary.sqlite3")
        await initialise_identity_database(database_url)
        set_authmgr_database_url(monkeypatch, tmp_path, database_url)
        monkeypatch.setattr(sys, "stdin", io.StringIO(f"{STRONG_TEST_PASSWORD}\n"))
        assert (
            await run_authmgr_command(
                ["user", "create", "boundary@example.com", "--password", "-"]
            )
            == 0
        )
        await update_user_fields(database_url, "boundary@example.com", expires_at=200.0)
        monkeypatch.setattr(
            "wybra.auth.admin.management.current_timestamp", lambda: 200.0
        )
        capsys.readouterr()

        assert await run_authmgr_command(["user", "list", "--json"]) == 0
        [boundary_record] = json.loads(capsys.readouterr().out)
        assert boundary_record["email"] == "boundary@example.com"
        assert boundary_record["effective_active"] is False

        assert await run_authmgr_command(["user", "list", "--json", "--active"]) == 0
        assert json.loads(capsys.readouterr().out) == []

        assert await run_authmgr_command(["user", "list", "--json", "--inactive"]) == 0
        [record] = json.loads(capsys.readouterr().out)
        assert record["email"] == "boundary@example.com"
        assert record["effective_active"] is False

    @pytest.mark.anyio
    async def test_authmgr_list_timestamp_filters_and_ordering(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        database_url = sqlite_file_url(tmp_path / "list-timestamps.sqlite3")
        await initialise_identity_database(database_url)
        set_authmgr_database_url(monkeypatch, tmp_path, database_url)
        for email in ("first@z.example", "second@y.example", "third@y.example"):
            monkeypatch.setattr(sys, "stdin", io.StringIO(f"{STRONG_TEST_PASSWORD}\n"))
            assert (
                await run_authmgr_command(["user", "create", email, "--password", "-"])
                == 0
            )

        await update_user_fields(
            database_url,
            "first@z.example",
            created_at=100.0,
            modified_at=150.0,
            last_login_at=200.0,
        )
        await update_user_fields(
            database_url,
            "second@y.example",
            created_at=300.0,
            modified_at=350.0,
            last_login_at=400.0,
        )
        await update_user_fields(
            database_url,
            "third@y.example",
            created_at=500.0,
            modified_at=550.0,
            last_login_at=600.0,
        )
        capsys.readouterr()

        assert (
            await run_authmgr_command(
                [
                    "user",
                    "list",
                    "--json",
                    "--since-created-at",
                    "250",
                    "--order",
                    "email-domain",
                ]
            )
            == 0
        )
        records = json.loads(capsys.readouterr().out)
        assert [record["email"] for record in records] == [
            "second@y.example",
            "third@y.example",
        ]

        assert await run_authmgr_command(["user", "list", "--json", "-l", "450"]) == 0
        records = json.loads(capsys.readouterr().out)
        assert {record["email"] for record in records} == {
            "first@z.example",
            "second@y.example",
        }

    @pytest.mark.anyio
    async def test_authmgr_last_login_order_keeps_nulls_last(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        database_url = sqlite_file_url(tmp_path / "last-login-order.sqlite3")
        await initialise_identity_database(database_url)
        set_authmgr_database_url(monkeypatch, tmp_path, database_url)
        for email in ("never@example.com", "recent@example.com"):
            monkeypatch.setattr(sys, "stdin", io.StringIO(f"{STRONG_TEST_PASSWORD}\n"))
            assert (
                await run_authmgr_command(["user", "create", email, "--password", "-"])
                == 0
            )

        await update_user_fields(
            database_url, "recent@example.com", last_login_at=100.0
        )
        capsys.readouterr()

        assert (
            await run_authmgr_command(
                ["user", "list", "--json", "--order", "last-login-at"]
            )
            == 0
        )

        records = json.loads(capsys.readouterr().out)
        assert [record["email"] for record in records] == [
            "recent@example.com",
            "never@example.com",
        ]

    @pytest.mark.anyio
    async def test_authmgr_email_domain_order_sorts_in_python(
        self,
        tmp_path: Path,
    ) -> None:
        database_url = sqlite_file_url(tmp_path / "email-domain-order.sqlite3")

        async def assert_email_domain_order(connection: BaseDBAsyncClient) -> None:
            for email in (
                "primary@z.example",
                "secondary@a.example",
                "tertiary@m.example",
            ):
                await User.create(
                    email=email,
                    hashed_password="hash",
                    using_db=connection,
                )

            result = await identity_management.list_local_users_for_management(
                connection,
                order="email-domain",
            )

            assert result.is_ok() is True
            records = result.value["users"]
            assert [record["email"] for record in records] == [
                "secondary@a.example",
                "tertiary@m.example",
                "primary@z.example",
            ]

        await _with_generated_identity_connection(
            database_url, assert_email_domain_order
        )

    @pytest.mark.anyio
    async def test_authmgr_list_filters_by_login_presence(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        database_url = sqlite_file_url(tmp_path / "login-presence.sqlite3")
        await initialise_identity_database(database_url)
        set_authmgr_database_url(monkeypatch, tmp_path, database_url)
        for email in ("never@example.com", "recent@example.com"):
            monkeypatch.setattr(sys, "stdin", io.StringIO(f"{STRONG_TEST_PASSWORD}\n"))
            assert (
                await run_authmgr_command(["user", "create", email, "--password", "-"])
                == 0
            )

        await update_user_fields(
            database_url, "recent@example.com", last_login_at=100.0
        )
        capsys.readouterr()

        assert (
            await run_authmgr_command(["user", "list", "--json", "--never-logged-in"])
            == 0
        )
        [never_record] = json.loads(capsys.readouterr().out)
        assert never_record["email"] == "never@example.com"

        assert await run_authmgr_command(["user", "list", "--json", "--logged-in"]) == 0
        [logged_in_record] = json.loads(capsys.readouterr().out)
        assert logged_in_record["email"] == "recent@example.com"

    def test_authmgr_timestamp_parser_handles_numeric_iso_and_natural_values(
        self,
    ) -> None:
        assert authmgr_timestamps.parse_timestamp_filter("4102444800") == 4102444800.0
        assert authmgr_timestamps.parse_timestamp_filter("20250101") == 20250101.0
        assert (
            authmgr_timestamps.parse_timestamp_filter("2100-01-01T00:00:00Z")
            == 4102444800.0
        )
        assert isinstance(
            authmgr_timestamps.parse_timestamp_filter("1 June 2030"), float
        )

    def test_authmgr_timestamp_parser_rejects_invalid_values(self) -> None:
        with pytest.raises(ValueError, match="Invalid timestamp value"):
            authmgr_timestamps.parse_timestamp_filter("not-a-date")

    def test_authmgr_timestamp_parser_uses_day_month_year_order(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(authmgr_timestamps, "_local_timezone_name", lambda: "UTC")

        assert (
            authmgr_timestamps.parse_timestamp_filter("01/02/2030")
            == datetime(
                2030,
                2,
                1,
                tzinfo=UTC,
            ).timestamp()
        )

    @pytest.mark.parametrize(
        ("tzinfo", "expected"),
        [
            (SimpleNamespace(key="Australia/Melbourne"), "Australia/Melbourne"),
            (SimpleNamespace(zone="Europe/London"), "Europe/London"),
            (SimpleNamespace(tzname=lambda _: "CET"), "UTC"),
            (SimpleNamespace(key="", zone="", tzname=lambda _: ""), "UTC"),
            (None, "UTC"),
        ],
    )
    def test_authmgr_timezone_name_uses_available_tzinfo_name(
        self,
        tzinfo: object,
        expected: str,
    ) -> None:
        assert authmgr_timestamps._timezone_name_from_tzinfo(tzinfo) == expected

    def test_auth_database_url_parser_handles_relative_sqlite_paths(
        self,
        tmp_path: Path,
    ) -> None:
        relative_url = parse_sqlite_database_url("sqlite:///relative.db")

        assert relative_url is not None
        assert relative_url.path == Path("relative.db")
        assert relative_url.is_absolute is False
        assert resolve_database_url(
            "sqlite:///relative.db", tmp_path
        ) == sqlite_file_url(tmp_path / "relative.db")

    def test_auth_database_url_parser_handles_posix_absolute_sqlite_paths(
        self,
        tmp_path: Path,
    ) -> None:
        absolute_url = parse_sqlite_database_url("sqlite:////tmp/auth.db")

        assert absolute_url is not None
        assert absolute_url.path.as_posix() == "/tmp/auth.db"
        assert absolute_url.is_absolute is True
        assert (
            resolve_database_url("sqlite:////tmp/auth.db", tmp_path)
            == "sqlite:////tmp/auth.db"
        )

    def test_auth_database_url_parser_handles_windows_absolute_sqlite_path(
        self,
    ) -> None:
        sqlite_url = parse_sqlite_database_url("sqlite:///C:/data/auth.db")

        assert sqlite_url is not None
        assert sqlite_url.path.as_posix() == "C:/data/auth.db"
        assert sqlite_url.is_absolute is True

    def test_auth_database_url_resolves_windows_absolute_sqlite_path(
        self,
        tmp_path: Path,
    ) -> None:
        database_url = "sqlite:///C:/data/auth.db"

        assert resolve_database_url(database_url, tmp_path) == database_url

    def test_authmgr_human_output_formats_only_known_timestamp_fields(self) -> None:
        assert (
            authmgr_output._format_human_value("created_at", 4102444800.0)
            == "2100-01-01T00:00:00+00:00"
        )
        assert (
            authmgr_output._format_human_value("created_at", 4102444800)
            == "2100-01-01T00:00:00+00:00"
        )
        assert authmgr_output._format_human_value("quota", 1.5) == 1.5

    @pytest.mark.parametrize(
        ("value", "pattern", "expected"),
        [
            ("", "", True),
            ("anything", "*", True),
            ("foo", "foo", True),
            ("foo", "bar", False),
            ("foo-bar", "foo*bar", True),
            ("foo*bar", r"foo\*bar", True),
            ("foo-bar", r"foo\*bar", False),
            ("100%", r"100\%", True),
            ("name_value", r"name\_value", True),
            ("foo\\bar", r"foo\\bar", True),
            ("foo\\xbar", r"foo\\*bar", True),
            ("foo\\*bar", r"foo\\\*bar", True),
        ],
    )
    def test_authmgr_wildcard_pattern_examples(
        self,
        value: str,
        pattern: str,
        expected: bool,
    ) -> None:
        assert identity_management._wildcard_matches(value, pattern) is expected

    def test_authmgr_human_output_handles_missing_record_fields(
        self,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        authmgr_output._print_user_records(
            [{"email": "partial@example.com", "id": "user-1"}],
            json_output=False,
            csv_output=False,
        )

        assert capsys.readouterr().out == (
            "partial@example.com id=user-1 admin=False "
            "superuser=False active=False verified=False\n"
        )

    @pytest.mark.anyio
    async def test_authmgr_csv_output_uses_iso_timestamp_strings(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        database_url = sqlite_file_url(tmp_path / "list-csv.sqlite3")
        await initialise_identity_database(database_url)
        set_authmgr_database_url(monkeypatch, tmp_path, database_url)
        monkeypatch.setattr(sys, "stdin", io.StringIO(f"{STRONG_TEST_PASSWORD}\n"))
        assert (
            await run_authmgr_command(
                ["user", "create", "csv@example.com", "--password", "-"]
            )
            == 0
        )
        await update_user_fields(
            database_url, "csv@example.com", created_at=4102444800.0
        )
        capsys.readouterr()

        assert await run_authmgr_command(["user", "list", "--csv"]) == 0

        reader = csv.DictReader(io.StringIO(capsys.readouterr().out))
        assert reader.fieldnames == list(authmgr_output.USER_RECORD_FIELDS)
        [record] = reader.__iter__()
        assert record["email"] == "csv@example.com"
        assert record["created_at"] == "2100-01-01T00:00:00+00:00"
