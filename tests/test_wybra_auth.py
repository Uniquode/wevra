import ast
import logging
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest
from tortoise.backends.base.client import BaseDBAsyncClient
from tortoise.exceptions import BaseORMException
from webauthn.helpers.exceptions import InvalidAuthenticationResponse

import wybra.auth.accounts.bootstrap as auth_bootstrap
import wybra.auth.admin.management as auth_management
import wybra.auth.context as auth_context
import wybra.auth.mfa.webauthn as webauthn_mfa
from support_database import sqlite_file_url
from wybra.auth import (
    ERROR_ALREADY_EXISTS,
    ERROR_INVALID_PASSWORD,
    ERROR_PASSWORD_TOO_SHORT,
    ERROR_PASSWORD_TOO_WEAK,
    TOTP_ASSERTION_METHOD,
    AuthenticationAssertion,
    ChallengeDecision,
    ChallengeKind,
    ChallengeRecord,
    DefaultPasswordPolicy,
    IdentityIntegration,
    IdentityOptions,
    NoChallengePolicy,
    PasswordStrength,
    PrimaryAuthenticationContext,
    Result,
    RouteReplacement,
    RouterExtensionPlan,
    complete_challenge,
)
from wybra.auth.accounts.manager import public_password_failure_message
from wybra.auth.admin.management import (
    ERROR_CYCLIC_GROUP_MEMBERSHIP,
    ERROR_GROUP_HAS_MEMBERSHIPS,
    ERROR_INVALID_GROUP_ID,
    ERROR_INVALID_USER_ID,
    ERROR_NOT_FOUND,
    ERROR_SCOPE_IN_USE,
    add_child_group_to_group_for_management,
    add_scope_to_group_for_management,
    add_user_to_group_for_management,
    create_group_for_management,
    create_local_user_for_management,
    create_scope_for_management,
    delete_group_for_management,
    delete_scope_for_management,
    effective_scopes_for_user_for_management,
    get_group_for_management,
    list_candidate_child_groups_for_management,
    list_groups_for_management,
    list_scopes_for_management,
    remove_child_group_from_group_for_management,
    remove_scope_from_group_for_management,
    remove_user_from_group_for_management,
    resolve_group_target,
    update_group_for_management,
    update_scope_for_management,
)
from wybra.auth.mfa.challenges import assertions_satisfy_required_methods
from wybra.auth.mfa.storage import (
    TOTP_CODE_REPLAY_MESSAGE,
    TortoiseRecoveryCodeStore,
    TortoiseTOTPCredentialStore,
    TortoiseWebAuthnCredentialStore,
    WebAuthnCredentialRecord,
)
from wybra.auth.mfa.totp import (
    MAX_TOTP_ALLOWED_DRIFT,
    MAX_TOTP_PERIOD_SECONDS,
    MAX_TOTP_RECOVERY_WINDOW_SECONDS,
    generate_totp,
    totp_auth_uri,
    verify_totp,
)
from wybra.auth.models import (
    AccessToken,
    ExternalIdentityLink,
    Group,
    GroupGroup,
    GroupScope,
    GroupUser,
    IdentityProvider,
    IdentityTotpCredential,
    IdentityTotpRecoveryCode,
    IdentityUserEmail,
    IdentityWebAuthnCredential,
    InitialAdminBootstrap,
    Scope,
    User,
)
from wybra.auth.options import VALID_IDENTITY_INTEGRATIONS
from wybra.auth.persistence import auth_persistence_scope
from wybra.auth.persistence.contracts import EffectiveScopeSet, ScopeRecord
from wybra.auth.provider_credentials import (
    ProviderCredentialStorageError,
    TortoiseProviderCredentialStore,
)
from wybra.auth.routes import normalise_return_to
from wybra.auth.routes.totp import verify_totp_code_for_credential
from wybra.auth.session_tokens import (
    SESSION_TOKEN_MAX_LENGTH,
    generate_session_token,
)
from wybra.core.exceptions import ConfigurationError
from wybra.db.persistence import Database, close_database
from wybra.services.crypto import (
    ENVELOPE_PREFIX,
    PLAIN_TEXT_VERSION,
    VERIFIER_PREFIX,
    SecretDataError,
    SecretEnvelope,
    SecretEnvelopeService,
)
from wybra.template.context import TemplateContext
from wybra.testing import create_test_database


async def initialise_auth_database(database_url: str) -> Database:
    return await create_test_database(
        database_url=database_url,
        modules=("wybra.auth",),
    )


@asynccontextmanager
async def connection_scope(
    database: Database,
) -> AsyncIterator[BaseDBAsyncClient]:
    yield database.connection()


async def create_test_user(
    connection: BaseDBAsyncClient,
    *,
    email: str,
    hashed_password: str | None = "hash",
    is_active: bool = True,
    is_superuser: bool = False,
    is_verified: bool = True,
    **values: object,
) -> User:
    user = await User.create(
        email=email,
        hashed_password=hashed_password,
        is_active=is_active,
        is_superuser=is_superuser,
        is_verified=is_verified,
        using_db=connection,
        **values,
    )
    await IdentityUserEmail.create(
        user_id=user.id,
        email=email,
        is_primary=True,
        is_verified=is_verified,
        using_db=connection,
    )
    return user


def make_test_only_secret_service() -> SecretEnvelopeService:
    return SecretEnvelopeService.for_testing()


class MemoryChallengeStore:
    def __init__(self) -> None:
        self.records: dict[str, ChallengeRecord] = {}
        self.consumed: list[str] = []

    async def create_challenge(
        self,
        user_id: str,
        kind: ChallengeKind,
        expires_at: datetime,
        metadata: dict[str, object] | None = None,
    ) -> ChallengeRecord:
        record = ChallengeRecord(
            id=f"challenge-{len(self.records) + 1}",
            user_id=user_id,
            kind=kind,
            expires_at=expires_at,
            metadata=dict(metadata or {}),
        )
        self.records[record.id] = record
        return record

    async def get_challenge(self, challenge_id: str) -> ChallengeRecord | None:
        return self.records.get(challenge_id)

    async def consume_challenge(self, challenge_id: str) -> None:
        self.consumed.append(challenge_id)
        self.records.pop(challenge_id, None)


class TestAuthentication:
    def test_wybra_auth_package_is_independent_from_application_modules(self) -> None:
        source_root = Path(__file__).resolve().parents[1] / "src"

        for package_name in ("wybra.auth",):
            package_root = source_root.joinpath(*package_name.split("."))
            for path in package_root.rglob("*.py"):
                tree = ast.parse(path.read_text(), filename=str(path))
                imported_modules = {
                    alias.name
                    for node in ast.walk(tree)
                    if isinstance(node, ast.Import)
                    for alias in node.names
                } | {
                    node.module
                    for node in ast.walk(tree)
                    if isinstance(node, ast.ImportFrom) and node.module is not None
                }
                assert not any(
                    module == "host_app" or module.startswith("host_app.")
                    for module in imported_modules
                )

    @pytest.mark.anyio
    async def test_identity_template_context_treats_session_lookup_failure_as_anonymous(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        async def resolve_current_user(_request: object) -> None:
            raise BaseORMException("stale session token")

        async def assert_context() -> None:
            request = SimpleNamespace(state=SimpleNamespace())

            context = await auth_context.identity_template_context(
                request,
                TemplateContext(),
            )

            assert context.as_dict() == {
                "user": None,
                "identity": {"authenticated": False},
            }
            assert request.state.identity_clear_session_cookie is True

        monkeypatch.setattr(auth_context, "resolve_current_user", resolve_current_user)

        await assert_context()

    def test_wybra_auth_models_expose_authorisation_group_tables(self) -> None:
        assert Group._meta.db_table == "identity_group"
        assert Scope._meta.db_table == "identity_scope"
        assert GroupScope._meta.db_table == "identity_group_scope"
        assert GroupUser._meta.db_table == "identity_group_user"
        assert GroupGroup._meta.db_table == "identity_group_group"
        assert IdentityUserEmail._meta.db_table == "identity_user_email"

        assert {"group", "scope"}.issubset(GroupScope._meta.fields_map)
        assert {"group", "user"}.issubset(GroupUser._meta.fields_map)
        assert {"parent_group", "child_group"}.issubset(GroupGroup._meta.fields_map)
        assert GroupScope._meta.unique_together == (("group_id", "scope"),)
        assert GroupUser._meta.unique_together == (("group_id", "user_id"),)
        assert GroupGroup._meta.unique_together == (
            ("parent_group_id", "child_group_id"),
        )

    def test_wybra_auth_totp_seed_model_uses_encrypted_field(self) -> None:
        field = IdentityTotpCredential._meta.fields_map["crypt_secret"]

        assert field.null is False
        assert field.max_length == 1024

    def test_wybra_auth_webauthn_credential_model_uses_public_key_field(self) -> None:
        fields = IdentityWebAuthnCredential._meta.fields_map

        assert fields["public_key"].null is False
        assert fields["credential_id"].null is False
        assert fields["credential_id"].unique is True

    @pytest.mark.anyio
    async def test_auth_persistence_scope_rolls_back_on_error(
        self, tmp_path: Path
    ) -> None:
        async def assert_scope_rolls_back() -> None:
            database = await initialise_auth_database(
                sqlite_file_url(tmp_path / "auth-scope-rollback.sqlite3")
            )
            try:
                with pytest.raises(RuntimeError, match="abort create"):
                    async with auth_persistence_scope(database) as scope:
                        await scope.users.create_local_user(
                            {
                                "email": "rollback@example.com",
                                "hashed_password": "hash",
                                "is_verified": True,
                            },
                            primary_email="rollback@example.com",
                        )
                        raise RuntimeError("abort create")

                async with connection_scope(database) as session:
                    assert (
                        await User.filter(email="rollback@example.com")
                        .using_db(session)
                        .count()
                    ) == 0
                    assert (
                        await IdentityUserEmail.filter(email="rollback@example.com")
                        .using_db(session)
                        .count()
                    ) == 0
            finally:
                await close_database(database)

        await assert_scope_rolls_back()

    @pytest.mark.anyio
    async def test_user_store_reduces_primary_email_rows_to_one(
        self, tmp_path: Path
    ) -> None:
        async def assert_primary_email_repaired() -> None:
            database = await initialise_auth_database(
                sqlite_file_url(tmp_path / "primary-email-repair.sqlite3")
            )
            try:
                async with auth_persistence_scope(database) as scope:
                    user = await scope.users.create_local_user(
                        {
                            "email": "primary-original@example.com",
                            "hashed_password": "hash",
                            "is_verified": True,
                        },
                        primary_email="primary-original@example.com",
                    )
                    user_id = str(user.id)

                async with connection_scope(database) as session:
                    await IdentityUserEmail.create(
                        user_id=uuid.UUID(user_id),
                        email="stale-primary@example.com",
                        is_primary=True,
                        is_verified=True,
                        using_db=session,
                    )

                async with auth_persistence_scope(database) as scope:
                    db_user = await scope.get_user(user_id)
                    assert db_user is not None
                    await scope.users.save_user(
                        db_user,
                        primary_email="primary-updated@example.com",
                        primary_email_verified=True,
                    )

                async with connection_scope(database) as session:
                    emails = (
                        await IdentityUserEmail.filter(user_id=uuid.UUID(user_id))
                        .using_db(session)
                        .order_by("email")
                    )
                    primary_emails = [email for email in emails if email.is_primary]

                assert [email.email for email in primary_emails] == [
                    "primary-updated@example.com"
                ]
                assert [email.email for email in emails if not email.is_primary] == [
                    "stale-primary@example.com"
                ]
            finally:
                await close_database(database)

        await assert_primary_email_repaired()

    @pytest.mark.anyio
    async def test_authorisation_scope_management_lifecycle(
        self, tmp_path: Path
    ) -> None:
        async def assert_scope_lifecycle() -> None:
            database = await initialise_auth_database(
                sqlite_file_url(tmp_path / "scope.sqlite3")
            )
            try:
                async with connection_scope(database) as session:
                    created = await create_scope_for_management(
                        session,
                        scope="document:read",
                        description="Read documents.",
                    )
                    duplicate = await create_scope_for_management(
                        session,
                        scope="document:read",
                        description="Duplicate.",
                    )
                    updated = await update_scope_for_management(
                        session,
                        scope="document:read",
                        description="Read published documents.",
                    )
                    listed = await list_scopes_for_management(session)
                    deleted = await delete_scope_for_management(
                        session,
                        scope="document:read",
                    )
                    missing = await update_scope_for_management(
                        session,
                        scope="document:read",
                        description="Missing.",
                    )

                assert created.is_ok() is True
                assert created.value == {
                    "scope": "document:read",
                    "description": "Read documents.",
                }
                assert duplicate.is_failure() is True
                assert duplicate.error_type == ERROR_ALREADY_EXISTS
                assert updated.value == {
                    "scope": "document:read",
                    "description": "Read published documents.",
                }
                assert listed.value == {
                    "scopes": [
                        {
                            "scope": "document:read",
                            "description": "Read published documents.",
                        }
                    ]
                }
                assert deleted.is_ok() is True
                assert missing.is_failure() is True
                assert missing.error_type == ERROR_NOT_FOUND
            finally:
                await close_database(database)

        await assert_scope_lifecycle()

    @pytest.mark.anyio
    async def test_authorisation_scope_integrity_race_keeps_transaction_usable(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        async def assert_scope_race() -> None:
            database = await initialise_auth_database(
                sqlite_file_url(tmp_path / "scope-race.sqlite3")
            )
            original_get_or_none = Scope.get_or_none
            injected = False

            async def racing_get_or_none(
                *args: object, **kwargs: object
            ) -> Scope | None:
                nonlocal injected
                if kwargs.get("scope") == "document:race" and not injected:
                    injected = True
                    connection = cast(BaseDBAsyncClient, kwargs["using_db"])
                    await Scope.create(
                        scope="document:race",
                        description="Race winner.",
                        using_db=connection,
                    )
                    return None
                return await original_get_or_none(*args, **kwargs)

            monkeypatch.setattr(Scope, "get_or_none", staticmethod(racing_get_or_none))
            try:
                async with auth_persistence_scope(database) as scope:
                    created = await scope.management.create_scope(
                        scope="document:race",
                        description="Race loser.",
                    )
                    listed = await scope.management.list_scopes()

                assert injected is True
                assert created.is_failure() is True
                assert created.error_type == ERROR_ALREADY_EXISTS
                assert listed.value == {
                    "scopes": [
                        {
                            "scope": "document:race",
                            "description": "Race winner.",
                        }
                    ]
                }
            finally:
                await close_database(database)

        await assert_scope_race()

    @pytest.mark.anyio
    async def test_initial_admin_bootstrap_duplicate_claim_keeps_transaction_usable(
        self,
        tmp_path: Path,
    ) -> None:
        async def assert_duplicate_claim() -> None:
            database = await initialise_auth_database(
                sqlite_file_url(tmp_path / "bootstrap-claim-race.sqlite3")
            )
            try:
                async with database.transaction_for(
                    database.routes.connection().for_write()
                ) as connection:
                    await InitialAdminBootstrap.create(id=1, using_db=connection)

                    claimed = await auth_bootstrap._claim_initial_admin_bootstrap(
                        connection
                    )
                    admin = await auth_bootstrap.find_administrative_user(connection)

                assert claimed is False
                assert admin is None
            finally:
                await close_database(database)

        await assert_duplicate_claim()

    @pytest.mark.anyio
    async def test_deactivate_local_user_rechecks_locked_superuser(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        async def assert_deactivate_protects_promoted_user() -> None:
            database = await initialise_auth_database(
                sqlite_file_url(tmp_path / "deactivate-promoted-superuser.sqlite3")
            )
            try:
                async with connection_scope(database) as session:
                    stale_user = await create_test_user(
                        session,
                        email="promoted@example.com",
                        is_superuser=False,
                    )
                    await (
                        User.filter(id=stale_user.id)
                        .using_db(session)
                        .update(is_superuser=True)
                    )

                    async def stale_resolve_user_target(
                        connection: BaseDBAsyncClient,
                        target: str,
                    ):
                        del connection, target
                        return stale_user, None

                    monkeypatch.setattr(
                        auth_management,
                        "resolve_user_target",
                        stale_resolve_user_target,
                    )

                    result = await auth_management.deactivate_local_user_for_management(
                        session,
                        target="promoted@example.com",
                    )
                    current_user = await User.get(id=stale_user.id, using_db=session)

                assert result.is_failure() is True
                assert result.error_type == auth_management.ERROR_SUPERUSER_PROTECTED
                assert current_user.is_superuser is True
                assert current_user.is_active is True
            finally:
                await close_database(database)

        await assert_deactivate_protects_promoted_user()

    @pytest.mark.anyio
    async def test_update_local_user_uses_locked_current_user(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        async def assert_update_preserves_current_user_fields() -> None:
            database = await initialise_auth_database(
                sqlite_file_url(tmp_path / "update-current-user.sqlite3")
            )
            try:
                async with connection_scope(database) as session:
                    stale_user = await create_test_user(
                        session,
                        email="updated-current@example.com",
                    )
                    await (
                        User.filter(id=stale_user.id)
                        .using_db(session)
                        .update(preferred_timezone="Australia/Melbourne")
                    )

                    async def stale_resolve_user_target(
                        connection: BaseDBAsyncClient,
                        target: str,
                    ):
                        del connection, target
                        return stale_user, None

                    monkeypatch.setattr(
                        auth_management,
                        "resolve_user_target",
                        stale_resolve_user_target,
                    )

                    result = await auth_management.update_local_user_for_management(
                        session,
                        IdentityOptions(),
                        target="updated-current@example.com",
                        is_admin=True,
                    )
                    current_user = await User.get(id=stale_user.id, using_db=session)

                assert result.is_ok() is True
                assert result.value["is_admin"] is True
                assert result.value["preferred_timezone"] == "Australia/Melbourne"
                assert current_user.is_admin is True
                assert current_user.preferred_timezone == "Australia/Melbourne"
            finally:
                await close_database(database)

        await assert_update_preserves_current_user_fields()

    @pytest.mark.anyio
    async def test_authorisation_scope_delete_rejects_used_scope(
        self, tmp_path: Path
    ) -> None:
        async def assert_used_scope_delete() -> None:
            database = await initialise_auth_database(
                sqlite_file_url(tmp_path / "used-scope.sqlite3")
            )
            try:
                async with connection_scope(database) as session:
                    await create_scope_for_management(session, scope="admin:write")
                    await create_group_for_management(
                        session,
                        abbrev="admins",
                        description="Administrators",
                    )
                    await add_scope_to_group_for_management(
                        session,
                        group_target="admins",
                        scope="admin:write",
                    )

                    deleted = await delete_scope_for_management(
                        session,
                        scope="admin:write",
                    )
                    listed = await list_scopes_for_management(session)

                assert deleted.is_failure() is True
                assert deleted.error_type == ERROR_SCOPE_IN_USE
                assert listed.value == {
                    "scopes": [{"scope": "admin:write", "description": None}]
                }
            finally:
                await close_database(database)

        await assert_used_scope_delete()

    @pytest.mark.anyio
    async def test_authorisation_group_management_lifecycle(
        self, tmp_path: Path
    ) -> None:
        async def assert_group_lifecycle() -> None:
            database = await initialise_auth_database(
                sqlite_file_url(tmp_path / "group.sqlite3")
            )
            try:
                async with connection_scope(database) as session:
                    created = await create_group_for_management(
                        session,
                        abbrev="ops",
                        description="Operations team",
                    )
                    duplicate = await create_group_for_management(
                        session,
                        abbrev="ops",
                        description="Duplicate",
                    )
                    resolved_by_abbrev, abbrev_error = await resolve_group_target(
                        session,
                        "ops",
                    )
                    resolved_by_id, id_error = await resolve_group_target(
                        session,
                        str(created.value["id"]),
                    )
                    updated = await update_group_for_management(
                        session,
                        target="ops",
                        description="Operations and support",
                    )
                    shown = await get_group_for_management(session, target="ops")
                    listed = await list_groups_for_management(session)
                    deleted = await delete_group_for_management(session, target="ops")
                    missing = await get_group_for_management(session, target="ops")

                assert created.is_ok() is True
                assert created.value["abbrev"] == "ops"
                assert created.value["description"] == "Operations team"
                assert duplicate.is_failure() is True
                assert duplicate.error_type == ERROR_ALREADY_EXISTS
                assert resolved_by_abbrev is not None
                assert abbrev_error is None
                assert resolved_by_id is not None
                assert id_error is None
                assert updated.value["abbrev"] == "ops"
                assert updated.value["description"] == "Operations and support"
                assert shown.value == updated.value
                assert listed.value == {"groups": [updated.value]}
                assert deleted.value == updated.value
                assert missing.is_failure() is True
                assert missing.error_type == ERROR_INVALID_GROUP_ID
            finally:
                await close_database(database)

        await assert_group_lifecycle()

    @pytest.mark.anyio
    async def test_authorisation_group_target_distinguishes_invalid_from_missing_uuid(
        self,
        tmp_path: Path,
    ) -> None:
        async def assert_group_target_errors() -> None:
            database = await initialise_auth_database(
                sqlite_file_url(tmp_path / "group-target.sqlite3")
            )
            try:
                async with connection_scope(database) as session:
                    invalid_group, invalid_error = await resolve_group_target(
                        session,
                        "not a uuid",
                    )
                    missing_group, missing_error = await resolve_group_target(
                        session,
                        "00000000-0000-0000-0000-000000000001",
                    )

                assert invalid_group is None
                assert invalid_error == ERROR_INVALID_GROUP_ID
                assert missing_group is None
                assert missing_error == ERROR_NOT_FOUND
            finally:
                await close_database(database)

        await assert_group_target_errors()

    @pytest.mark.anyio
    async def test_authorisation_group_scope_assignment_rejects_duplicates(
        self,
        tmp_path: Path,
    ) -> None:
        async def assert_group_scope_assignment() -> None:
            database = await initialise_auth_database(
                sqlite_file_url(tmp_path / "group-scope.sqlite3")
            )
            try:
                async with connection_scope(database) as session:
                    await create_scope_for_management(session, scope="admin:read")
                    created_group = await create_group_for_management(
                        session,
                        abbrev="admins",
                        description="Administrators",
                    )
                    assigned = await add_scope_to_group_for_management(
                        session,
                        group_target="admins",
                        scope="admin:read",
                    )
                    duplicate = await add_scope_to_group_for_management(
                        session,
                        group_target="admins",
                        scope="admin:read",
                    )
                    shown = await get_group_for_management(session, target="admins")
                    deleted = await delete_group_for_management(
                        session,
                        target=str(created_group.value["id"]),
                    )

                assert assigned.is_ok() is True
                assert duplicate.is_failure() is True
                assert duplicate.error_type == ERROR_ALREADY_EXISTS
                assert shown.value["scopes"] == ["admin:read"]
                assert deleted.is_ok() is True
            finally:
                await close_database(database)

        await assert_group_scope_assignment()

    @pytest.mark.anyio
    async def test_authorisation_group_user_membership_rejects_duplicates_and_blocks_delete(  # noqa: E501
        self,
        tmp_path: Path,
    ) -> None:
        async def assert_user_membership() -> None:
            database = await initialise_auth_database(
                sqlite_file_url(tmp_path / "group-user.sqlite3")
            )
            try:
                async with connection_scope(database) as session:
                    await create_group_for_management(
                        session,
                        abbrev="staff",
                        description="Staff",
                    )
                    await create_local_user_for_management(
                        session,
                        IdentityOptions(),
                        email="staff@example.com",
                        password="Correct horse 42!",
                    )
                    assigned = await add_user_to_group_for_management(
                        session,
                        group_target="staff",
                        user_target="staff@example.com",
                    )
                    duplicate = await add_user_to_group_for_management(
                        session,
                        group_target="staff",
                        user_target="staff@example.com",
                    )
                    delete_result = await delete_group_for_management(
                        session,
                        target="staff",
                    )
                    shown = await get_group_for_management(session, target="staff")

                assert assigned.is_ok() is True
                assert duplicate.is_failure() is True
                assert duplicate.error_type == ERROR_ALREADY_EXISTS
                assert delete_result.is_failure() is True
                assert delete_result.error_type == ERROR_GROUP_HAS_MEMBERSHIPS
                assert shown.value["users"] == ["staff@example.com"]
            finally:
                await close_database(database)

        await assert_user_membership()

    @pytest.mark.anyio
    async def test_authorisation_nested_group_membership_rejects_duplicates_and_cycles(
        self,
        tmp_path: Path,
    ) -> None:
        async def assert_nested_group_membership() -> None:
            database = await initialise_auth_database(
                sqlite_file_url(tmp_path / "group-group.sqlite3")
            )
            try:
                async with connection_scope(database) as session:
                    for abbrev in ("parent", "child", "grandchild"):
                        await create_group_for_management(
                            session,
                            abbrev=abbrev,
                            description=f"{abbrev} group",
                        )

                    assigned = await add_child_group_to_group_for_management(
                        session,
                        parent_target="parent",
                        child_target="child",
                    )
                    duplicate = await add_child_group_to_group_for_management(
                        session,
                        parent_target="parent",
                        child_target="child",
                    )
                    self_membership = await add_child_group_to_group_for_management(
                        session,
                        parent_target="parent",
                        child_target="parent",
                    )
                    await add_child_group_to_group_for_management(
                        session,
                        parent_target="child",
                        child_target="grandchild",
                    )
                    cycle = await add_child_group_to_group_for_management(
                        session,
                        parent_target="grandchild",
                        child_target="parent",
                    )
                    delete_result = await delete_group_for_management(
                        session,
                        target="parent",
                    )
                    shown = await get_group_for_management(session, target="parent")

                assert assigned.is_ok() is True
                assert duplicate.is_failure() is True
                assert duplicate.error_type == ERROR_ALREADY_EXISTS
                assert self_membership.is_failure() is True
                assert self_membership.error_type == ERROR_CYCLIC_GROUP_MEMBERSHIP
                assert cycle.is_failure() is True
                assert cycle.error_type == ERROR_CYCLIC_GROUP_MEMBERSHIP
                assert delete_result.is_failure() is True
                assert delete_result.error_type == ERROR_GROUP_HAS_MEMBERSHIPS
                assert shown.value["child_groups"] == ["child"]
            finally:
                await close_database(database)

        await assert_nested_group_membership()

    @pytest.mark.anyio
    async def test_authorisation_membership_removal_and_candidate_child_groups(
        self,
        tmp_path: Path,
    ) -> None:
        async def assert_removal_and_candidates() -> None:
            database = await initialise_auth_database(
                sqlite_file_url(tmp_path / "group-removal.sqlite3")
            )
            try:
                async with connection_scope(database) as session:
                    await create_scope_for_management(session, scope="staff:read")
                    await create_local_user_for_management(
                        session,
                        IdentityOptions(),
                        email="member@example.com",
                        password="Correct horse 42!",
                    )
                    for abbrev in ("staff", "child", "candidate"):
                        await create_group_for_management(
                            session,
                            abbrev=abbrev,
                            description=f"{abbrev} group",
                        )
                    await add_scope_to_group_for_management(
                        session,
                        group_target="staff",
                        scope="staff:read",
                    )
                    await add_user_to_group_for_management(
                        session,
                        group_target="staff",
                        user_target="member@example.com",
                    )
                    await add_child_group_to_group_for_management(
                        session,
                        parent_target="staff",
                        child_target="child",
                    )

                    candidates = await list_candidate_child_groups_for_management(
                        session,
                        parent_target="staff",
                    )
                    scope_removed = await remove_scope_from_group_for_management(
                        session,
                        group_target="staff",
                        scope="staff:read",
                    )
                    user_removed = await remove_user_from_group_for_management(
                        session,
                        group_target="staff",
                        user_target="member@example.com",
                    )
                    child_removed = await remove_child_group_from_group_for_management(
                        session,
                        parent_target="staff",
                        child_target="child",
                    )
                    deleted = await delete_group_for_management(
                        session,
                        target="staff",
                    )

                assert [group["abbrev"] for group in candidates.value["groups"]] == [
                    "candidate"
                ]
                assert scope_removed.is_ok() is True
                assert user_removed.is_ok() is True
                assert child_removed.is_ok() is True
                assert deleted.is_ok() is True
            finally:
                await close_database(database)

        await assert_removal_and_candidates()

    @pytest.mark.anyio
    async def test_effective_scopes_invalid_user_target_returns_invalid_user_id(
        self,
        tmp_path: Path,
    ) -> None:
        async def assert_invalid_user_id() -> None:
            database = await initialise_auth_database(
                sqlite_file_url(tmp_path / "effective-invalid-user-id.sqlite3")
            )
            try:
                async with connection_scope(database) as session:
                    result = await effective_scopes_for_user_for_management(
                        session,
                        user_target="not-a-valid-user-id",
                    )

                assert result.is_failure() is True
                assert result.error_type == ERROR_INVALID_USER_ID
                assert (
                    result.message
                    == "User target must be an email address or valid user ID."
                )
            finally:
                await close_database(database)

        await assert_invalid_user_id()

    @pytest.mark.anyio
    async def test_effective_scopes_missing_user_returns_not_found(
        self, tmp_path: Path
    ) -> None:
        async def assert_missing_user() -> None:
            database = await initialise_auth_database(
                sqlite_file_url(tmp_path / "effective-missing-user.sqlite3")
            )
            try:
                async with connection_scope(database) as session:
                    result = await effective_scopes_for_user_for_management(
                        session,
                        user_target="missing.user@example.com",
                    )

                assert result.is_failure() is True
                assert result.error_type == ERROR_NOT_FOUND
                assert result.message == "No matching user was found."
            finally:
                await close_database(database)

        await assert_missing_user()

    @pytest.mark.anyio
    async def test_effective_scopes_resolve_direct_nested_and_duplicate_group_scopes(
        self,
        tmp_path: Path,
    ) -> None:
        async def assert_effective_scopes() -> None:
            database = await initialise_auth_database(
                sqlite_file_url(tmp_path / "effective.sqlite3")
            )
            try:
                async with connection_scope(database) as session:
                    await create_local_user_for_management(
                        session,
                        IdentityOptions(),
                        email="scope-user@example.com",
                        password="Correct horse 42!",
                    )
                    no_groups = await effective_scopes_for_user_for_management(
                        session,
                        user_target="scope-user@example.com",
                    )

                    for scope in ("document:read", "document:write"):
                        await create_scope_for_management(session, scope=scope)
                    for abbrev in ("direct", "nested", "duplicate"):
                        await create_group_for_management(
                            session,
                            abbrev=abbrev,
                            description=f"{abbrev} group",
                        )
                    await add_scope_to_group_for_management(
                        session,
                        group_target="direct",
                        scope="document:read",
                    )
                    await add_scope_to_group_for_management(
                        session,
                        group_target="nested",
                        scope="document:write",
                    )
                    await add_scope_to_group_for_management(
                        session,
                        group_target="duplicate",
                        scope="document:read",
                    )
                    await add_user_to_group_for_management(
                        session,
                        group_target="direct",
                        user_target="scope-user@example.com",
                    )
                    await add_child_group_to_group_for_management(
                        session,
                        parent_target="direct",
                        child_target="nested",
                    )
                    await add_child_group_to_group_for_management(
                        session,
                        parent_target="nested",
                        child_target="duplicate",
                    )

                    resolved = await effective_scopes_for_user_for_management(
                        session,
                        user_target="scope-user@example.com",
                    )

                assert no_groups.value["scopes"] == []
                assert resolved.value["scopes"] == ["document:read", "document:write"]
                assert resolved.value["groups"] == ["direct", "duplicate", "nested"]
            finally:
                await close_database(database)

        await assert_effective_scopes()

    @pytest.mark.anyio
    async def test_auth_persistence_exposes_authorisation_store(
        self,
        tmp_path: Path,
    ) -> None:
        database = await initialise_auth_database(
            sqlite_file_url(tmp_path / "authorisation-store.sqlite3")
        )
        try:
            async with auth_persistence_scope(database) as persistence:
                await persistence.management.create_local_user(
                    IdentityOptions(),
                    email="policy-user@example.com",
                    password="Correct horse 42!",
                )
                await persistence.management.create_scope(
                    scope="articles.article.update",
                    description="Update articles.",
                )
                await persistence.management.create_group(
                    abbrev="editors",
                    description="Article editors.",
                )
                await persistence.management.add_scope_to_group(
                    group_target="editors",
                    scope="articles.article.update",
                )
                await persistence.management.add_user_to_group(
                    group_target="editors",
                    user_target="policy-user@example.com",
                )
                user = await persistence.get_user_by_email("policy-user@example.com")
                assert user is not None

                scopes = await persistence.authorisation.list_scopes()
                effective = (
                    await persistence.authorisation.effective_scope_sets_for_user(
                        user.id
                    )
                )

            assert scopes == (
                ScopeRecord(
                    scope="articles.article.update",
                    title="Update articles.",
                ),
            )
            assert effective == EffectiveScopeSet(
                scopes=("articles.article.update",),
                groups=("editors",),
            )
        finally:
            await close_database(database)

    @pytest.mark.anyio
    async def test_effective_scope_resolution_is_cycle_safe_and_reads_current_data(
        self,
        tmp_path: Path,
    ) -> None:
        async def assert_cycle_safety_and_current_data() -> None:
            database = await initialise_auth_database(
                sqlite_file_url(tmp_path / "effective-current.sqlite3")
            )
            try:
                async with connection_scope(database) as session:
                    await create_local_user_for_management(
                        session,
                        IdentityOptions(),
                        email="current@example.com",
                        password="Correct horse 42!",
                    )
                    for scope in ("first:read", "second:read"):
                        await create_scope_for_management(session, scope=scope)
                    for abbrev in ("first", "second"):
                        await create_group_for_management(
                            session,
                            abbrev=abbrev,
                            description=f"{abbrev} group",
                        )
                    await add_scope_to_group_for_management(
                        session,
                        group_target="first",
                        scope="first:read",
                    )
                    await add_user_to_group_for_management(
                        session,
                        group_target="first",
                        user_target="current@example.com",
                    )
                    first = await effective_scopes_for_user_for_management(
                        session,
                        user_target="current@example.com",
                    )
                    first_group, _ = await resolve_group_target(session, "first")
                    second_group, _ = await resolve_group_target(session, "second")
                    await GroupGroup.create(
                        parent_group_id=first_group.id,
                        child_group_id=second_group.id,
                        using_db=session,
                    )
                    await GroupGroup.create(
                        parent_group_id=second_group.id,
                        child_group_id=first_group.id,
                        using_db=session,
                    )
                    before_second_scope = (
                        await effective_scopes_for_user_for_management(
                            session,
                            user_target="current@example.com",
                        )
                    )
                    await add_scope_to_group_for_management(
                        session,
                        group_target="second",
                        scope="second:read",
                    )
                    after_second_scope = await effective_scopes_for_user_for_management(
                        session,
                        user_target="current@example.com",
                    )

                assert first.value["scopes"] == ["first:read"]
                assert before_second_scope.value["scopes"] == ["first:read"]
                assert after_second_scope.value["scopes"] == [
                    "first:read",
                    "second:read",
                ]
                assert after_second_scope.value["groups"] == ["first", "second"]
            finally:
                await close_database(database)

        await assert_cycle_safety_and_current_data()

    def test_wybra_auth_result_carries_success_values_and_failure_reason(self) -> None:
        result = Result.ok({"id": "user-1"})

        assert result.is_ok() is True
        assert result.is_failure() is False
        assert result.value == {"id": "user-1"}
        assert result.error_type is None

        failure = Result.failure(ERROR_ALREADY_EXISTS, "Already exists.")

        assert failure.is_failure() is True
        assert failure.is_ok() is False
        assert failure.error_type == ERROR_ALREADY_EXISTS
        assert failure.message == "Already exists."
        assert failure.value is None

        empty_success = Result.ok()

        assert empty_success.is_ok() is True
        assert empty_success.is_failure() is False
        assert empty_success.value is None
        assert empty_success.error_type is None

    def test_wybra_auth_default_password_policy_scores_and_accepts_passphrases(
        self,
    ) -> None:
        policy = DefaultPasswordPolicy()

        strength = policy.strength("correct horse")
        validation = policy.validate("correct horse")

        assert strength.score >= policy.minimum_score
        assert strength.label in {"fair", "good", "strong"}
        assert validation.is_ok() is True

    @pytest.mark.parametrize(
        ("password", "error_type"),
        [
            ("   ", ERROR_INVALID_PASSWORD),
            ("short 1", ERROR_PASSWORD_TOO_SHORT),
            ("admin password 123!", ERROR_PASSWORD_TOO_WEAK),
            ("changeme 123!", ERROR_PASSWORD_TOO_WEAK),
            ("changeit 123!", ERROR_PASSWORD_TOO_WEAK),
            ("p4ssw0rd 123!", ERROR_PASSWORD_TOO_WEAK),
            ("pass phrase 123!", ERROR_PASSWORD_TOO_WEAK),
            ("tester account 123!", ERROR_PASSWORD_TOO_WEAK),
            ("test account 123!", ERROR_PASSWORD_TOO_WEAK),
            ("drowssap account 123!", ERROR_PASSWORD_TOO_WEAK),
            ("abcdefghijkl", ERROR_PASSWORD_TOO_WEAK),
        ],
    )
    def test_wybra_auth_default_password_policy_rejects_invalid_values(
        self,
        password: str,
        error_type: str,
    ) -> None:
        result = DefaultPasswordPolicy().validate(password)

        assert result.is_failure() is True
        assert result.error_type == error_type
        assert result.message

    @pytest.mark.parametrize(
        ("password", "user"),
        [
            (
                "signup 12345!",
                SimpleNamespace(email="signup@example.com"),
            ),
            (
                "operator 123!",
                SimpleNamespace(display_name="Operator Example"),
            ),
            (
                "david 123456!",
                SimpleNamespace(preferred_name="David"),
            ),
            (
                "identity 123!",
                SimpleNamespace(username="identity-admin"),
            ),
        ],
    )
    def test_wybra_auth_default_password_policy_rejects_account_detail_fragments(
        self,
        password: str,
        user: object,
    ) -> None:
        result = DefaultPasswordPolicy().validate(password, user)

        assert result.is_failure() is True
        assert result.error_type == ERROR_PASSWORD_TOO_WEAK
        assert result.message

    def test_wybra_auth_identity_options_accept_custom_password_policy(self) -> None:
        class RejectingPasswordPolicy:
            def strength(
                self,
                password: str,
                user: object | None = None,
            ) -> PasswordStrength:
                del password, user
                return PasswordStrength(
                    score=0.0,
                    label="weak",
                    feedback=("Rejected by custom policy.",),
                )

            def validate(
                self,
                password: str,
                user: object | None = None,
            ) -> Result[str]:
                del password, user
                return Result.failure(
                    ERROR_PASSWORD_TOO_WEAK,
                    "Rejected by custom policy.",
                )

        options = IdentityOptions(password_policy=RejectingPasswordPolicy())
        validation = options.resolved_password_policy().validate("correct horse")

        assert validation.is_failure() is True
        assert validation.error_type == ERROR_PASSWORD_TOO_WEAK
        assert validation.message == "Rejected by custom policy."

    def test_wybra_auth_integration_options_enabled(self) -> None:
        options = IdentityOptions(
            provider_enabled=True,
            passkey_enabled=True,
            passkey_rp_id="app.example.com",
            passkey_rp_name="Example App",
            passkey_allowed_origins=("https://app.example.com",),
            totp_mode="required",
        )

        for integration in VALID_IDENTITY_INTEGRATIONS:
            assert options.integration_enabled(integration) is True

    def test_wybra_auth_totp_auth_uri_rejects_unknown_algorithm(self) -> None:
        with pytest.raises(ValueError, match="Unsupported TOTP algorithm"):
            totp_auth_uri(
                account_name="person@example.com",
                secret="JBSWY3DPEHPK3PXP",
                issuer="wybra",
                algorithm="md5",
            )

    def test_wybra_auth_totp_verification_limits_allowed_drift(self) -> None:
        secret = "JBSWY3DPEHPK3PXP"
        timestamp = 1_800_000_000.0
        submitted_code = generate_totp(secret, timestamp=timestamp)

        accepted, counter = verify_totp(
            secret,
            submitted_code,
            timestamp=timestamp,
            allowed_drift=MAX_TOTP_ALLOWED_DRIFT,
        )
        assert accepted is True
        assert counter is not None

        with pytest.raises(ValueError, match="non-negative"):
            verify_totp(secret, submitted_code, allowed_drift=-1)

        with pytest.raises(ValueError, match="maximum"):
            verify_totp(
                secret,
                submitted_code,
                allowed_drift=MAX_TOTP_ALLOWED_DRIFT + 1,
            )

    def test_wybra_auth_identity_options_reject_invalid_totp_settings(self) -> None:
        with pytest.raises(ConfigurationError, match="TOTP mode"):
            IdentityOptions(totp_mode="turbo")  # type: ignore[arg-type]

        with pytest.raises(ConfigurationError, match="non-negative"):
            IdentityOptions(totp_allowed_drift=-1)

        with pytest.raises(ConfigurationError, match="must not exceed"):
            IdentityOptions(totp_allowed_drift=MAX_TOTP_ALLOWED_DRIFT + 1)

        with pytest.raises(ConfigurationError, match="positive"):
            IdentityOptions(totp_period_seconds=0)

        with pytest.raises(ConfigurationError, match="must not exceed"):
            IdentityOptions(totp_period_seconds=MAX_TOTP_PERIOD_SECONDS + 1)

        with pytest.raises(ConfigurationError, match="positive"):
            IdentityOptions(totp_challenge_expiry_seconds=0)

        with pytest.raises(ConfigurationError, match="positive"):
            IdentityOptions(totp_recovery_window_seconds=0)

        with pytest.raises(ConfigurationError, match="must not exceed"):
            IdentityOptions(
                totp_recovery_window_seconds=MAX_TOTP_RECOVERY_WINDOW_SECONDS + 1,
            )

    def test_wybra_auth_identity_options_validate_passkey_settings(self) -> None:
        options = IdentityOptions(
            passkey_enabled=True,
            passkey_rp_id="app.example.com",
            passkey_rp_name="Example App",
            passkey_allowed_origins=("https://app.example.com/",),
        )

        assert options.passkey_allowed_origins == ("https://app.example.com",)

        with pytest.raises(ConfigurationError, match="relying-party ID"):
            IdentityOptions(passkey_enabled=True)

        with pytest.raises(ConfigurationError, match="domain, not a URL"):
            IdentityOptions(
                passkey_enabled=True,
                passkey_rp_id="https://app.example.com",
                passkey_rp_name="Example App",
                passkey_allowed_origins=("https://app.example.com",),
            )

        with pytest.raises(ConfigurationError, match="domain, not a URL"):
            IdentityOptions(
                passkey_enabled=True,
                passkey_rp_id="app.example.com/path",
                passkey_rp_name="Example App",
                passkey_allowed_origins=("https://app.example.com",),
            )

        with pytest.raises(ConfigurationError, match="allowed origins"):
            IdentityOptions(
                passkey_enabled=True,
                passkey_rp_id="app.example.com",
                passkey_rp_name="Example App",
            )

        with pytest.raises(ConfigurationError, match="TOTP policy"):
            IdentityOptions(
                passkey_enabled=True,
                passkey_rp_id="app.example.com",
                passkey_rp_name="Example App",
                passkey_allowed_origins=("https://app.example.com",),
                passkey_user_verification_satisfies_totp="yes",  # type: ignore[arg-type]
            )

        with pytest.raises(ConfigurationError, match="scheme and host"):
            IdentityOptions(
                passkey_enabled=True,
                passkey_rp_id="app.example.com",
                passkey_rp_name="Example App",
                passkey_allowed_origins=("app.example.com",),
            )

        with pytest.raises(ConfigurationError, match="path"):
            IdentityOptions(
                passkey_enabled=True,
                passkey_rp_id="app.example.com",
                passkey_rp_name="Example App",
                passkey_allowed_origins=("https://app.example.com/login",),
            )

    def test_wybra_auth_identity_options_integration_enabled_rejects_unknown(
        self,
    ) -> None:
        options = IdentityOptions()
        unknown_integration: str = "sso"

        with pytest.raises(ConfigurationError):
            options.integration_enabled(
                cast(IdentityIntegration, unknown_integration),
            )

    def test_wybra_auth_password_failure_message_filters_unrecognised_reasons(
        self,
    ) -> None:
        assert (
            public_password_failure_message(
                "Internal breach provider matched tenant-specific denylist."
            )
            == "Password is invalid."
        )
        assert (
            public_password_failure_message(
                "Password does not meet the strength requirement."
            )
            == "Password does not meet the strength requirement."
        )

    @pytest.mark.anyio
    async def test_wybra_auth_no_challenge_policy_allows_direct_login(self) -> None:
        async def assert_policy() -> None:
            decision = await NoChallengePolicy().after_primary_authentication(
                PrimaryAuthenticationContext(user_id="user-1"),
                MemoryChallengeStore(),
            )

            assert isinstance(decision, ChallengeDecision)
            assert decision.requires_challenge is False
            assert decision.challenge is None

        await assert_policy()

    def test_wybra_auth_identity_provider_and_link_models_are_well_formed(self) -> None:
        provider_fields = set(IdentityProvider._meta.fields_map)
        external_identity_link_fields = set(ExternalIdentityLink._meta.fields_map)
        access_token_fields = set(AccessToken._meta.fields_map)
        user_email_fields = set(IdentityUserEmail._meta.fields_map)

        assert {
            "provider_name",
            "provider_subject",
            "crypt_access_token",
            "crypt_refresh_token",
            "account_email",
        }.issubset(provider_fields)
        assert {"user", "provider"}.issubset(external_identity_link_fields)
        assert {"token", "created_at", "user"}.issubset(access_token_fields)
        assert {
            "user",
            "email",
            "is_primary",
            "is_verified",
        }.issubset(user_email_fields)

        assert IdentityProvider._meta.db_table == "identity_provider"
        assert AccessToken._meta.db_table == "identity_access_token"
        assert (
            AccessToken._meta.fields_map["token"].max_length == SESSION_TOKEN_MAX_LENGTH
        )
        assert ExternalIdentityLink._meta.db_table == "identity_external_identity_link"
        assert IdentityProvider._meta.unique_together == (
            ("provider_name", "provider_subject"),
        )
        assert ExternalIdentityLink._meta.unique_together == (
            ("user_id", "provider_id"),
        )

    def test_wybra_auth_generated_session_tokens_fit_configured_column(self) -> None:
        token = generate_session_token()

        assert len(token) <= SESSION_TOKEN_MAX_LENGTH

    @pytest.mark.anyio
    async def test_wybra_auth_provider_credential_store_encrypts_tokens(
        self,
        tmp_path: Path,
    ) -> None:
        async def assert_encrypted_storage() -> None:
            database = await initialise_auth_database(
                sqlite_file_url(tmp_path / "provider-encrypted.sqlite3")
            )
            try:
                async with connection_scope(database) as session:
                    store = TortoiseProviderCredentialStore(
                        session,
                        make_test_only_secret_service(),
                    )
                    provider_id = await store.create_provider_credential(
                        provider_name="github",
                        provider_subject="subject-1",
                        access_token="access-token",
                        refresh_token="refresh-token",
                        account_email="person@example.com",
                    )
                    provider = await store.get_provider_credential(provider_id)

                    assert provider is not None
                    assert provider.crypt_access_token.startswith(
                        f"{ENVELOPE_PREFIX}|test|"
                    )
                    assert provider.crypt_refresh_token is not None
                    assert provider.crypt_refresh_token.startswith(
                        f"{ENVELOPE_PREFIX}|test|"
                    )
                    assert provider.crypt_access_token != "access-token"
                    assert provider.crypt_refresh_token != "refresh-token"
                    assert isinstance(
                        store.secret_envelopes(provider).access_token,
                        SecretEnvelope,
                    )
                    assert store.decrypt_access_token(provider) == "access-token"
                    assert store.decrypt_refresh_token(provider) == "refresh-token"
            finally:
                await close_database(database)

        await assert_encrypted_storage()

    @pytest.mark.anyio
    @pytest.mark.anyio
    async def test_wybra_auth_provider_credential_store_rejects_malformed_envelope(
        self,
        tmp_path: Path,
    ) -> None:
        async def assert_malformed_envelope_rejected() -> None:
            database = await initialise_auth_database(
                sqlite_file_url(tmp_path / "provider-malformed.sqlite3")
            )
            try:
                async with connection_scope(database) as session:
                    provider = await IdentityProvider.create(
                        provider_name="github",
                        provider_subject="subject-1",
                        crypt_access_token=f"{ENVELOPE_PREFIX}|test",
                        crypt_refresh_token=None,
                        account_email="person@example.com",
                        provider_enabled=True,
                        using_db=session,
                    )

                    store = TortoiseProviderCredentialStore(
                        session,
                        make_test_only_secret_service(),
                    )

                    with pytest.raises(SecretDataError, match="invalid or malformed"):
                        store.decrypt_access_token(provider)
            finally:
                await close_database(database)

        await assert_malformed_envelope_rejected()

    @pytest.mark.anyio
    async def test_wybra_auth_provider_credential_store_requires_keys_for_secret_operations(  # noqa: E501
        self,
        tmp_path: Path,
    ) -> None:
        async def assert_required_keys() -> None:
            database = await initialise_auth_database(
                sqlite_file_url(tmp_path / "provider-required-keys.sqlite3")
            )
            try:
                async with connection_scope(database) as session:
                    store = TortoiseProviderCredentialStore(
                        session,
                        SecretEnvelopeService.from_env({}),
                    )

                    with pytest.raises(
                        ProviderCredentialStorageError,
                        match="requires configured crypto secret material",
                    ):
                        await store.create_provider_credential(
                            provider_name="github",
                            provider_subject="subject-1",
                            access_token="access-token",
                            account_email="person@example.com",
                        )
            finally:
                await close_database(database)

        await assert_required_keys()

    def test_provider_disabled_identity_options_do_not_require_secret_keys(
        self,
    ) -> None:
        options = IdentityOptions(provider_enabled=False)
        secret_service = SecretEnvelopeService.from_env({})

        assert options.integration_enabled("provider") is False
        assert secret_service.decrypt("plain-token") == (
            "plain-token",
            PLAIN_TEXT_VERSION,
        )

    @pytest.mark.anyio
    async def test_wybra_auth_recovery_code_replacement_rejects_user_mismatch(
        self,
        tmp_path: Path,
    ) -> None:
        async def assert_mismatch_rejected() -> None:
            database = await initialise_auth_database(
                sqlite_file_url(tmp_path / "recovery-mismatch.sqlite3")
            )
            try:
                async with connection_scope(database) as session:
                    owner = await create_test_user(
                        session,
                        email="owner@example.com",
                    )
                    other_user = await create_test_user(
                        session,
                        email="other@example.com",
                    )

                    credential_store = TortoiseTOTPCredentialStore(
                        session,
                        make_test_only_secret_service(),
                    )
                    credential_id = (
                        await credential_store.create_pending_totp_credential(
                            str(owner.id),
                            "JBSWY3DPEHPK3PXP",
                        )
                    )
                    recovery_store = TortoiseRecoveryCodeStore(
                        session,
                        make_test_only_secret_service(),
                    )

                    with pytest.raises(ValueError, match="does not belong"):
                        await recovery_store.replace_recovery_codes(
                            str(other_user.id),
                            credential_id,
                            ("R3C0V3RY",),
                        )
            finally:
                await close_database(database)

        await assert_mismatch_rejected()

    @pytest.mark.anyio
    async def test_wybra_auth_totp_seed_storage_uses_encrypted_envelope(
        self,
        tmp_path: Path,
    ) -> None:
        async def assert_encrypted_storage() -> None:
            database = await initialise_auth_database(
                sqlite_file_url(tmp_path / "totp-encrypted.sqlite3")
            )
            try:
                async with connection_scope(database) as session:
                    user = await create_test_user(
                        session,
                        email="totp@example.com",
                    )

                    store = TortoiseTOTPCredentialStore(
                        session,
                        make_test_only_secret_service(),
                    )
                    credential_id = await store.create_pending_totp_credential(
                        str(user.id),
                        "JBSWY3DPEHPK3PXP",
                    )
                    credential = await store.get_totp_credential(credential_id)
                    assert credential is not None
                    assert credential.crypt_secret.startswith(
                        f"{ENVELOPE_PREFIX}|test|"
                    )
                    assert store.decrypt_totp_secret(credential) == "JBSWY3DPEHPK3PXP"
            finally:
                await close_database(database)

        await assert_encrypted_storage()

    @pytest.mark.anyio
    async def test_wybra_auth_totp_rejects_replayed_code_and_persists_counter(
        self,
        tmp_path: Path,
    ) -> None:
        async def assert_replay_rejected() -> None:
            database = await initialise_auth_database(
                sqlite_file_url(tmp_path / "totp-replay.sqlite3")
            )
            try:
                secret_service = make_test_only_secret_service()
                secret = "JBSWY3DPEHPK3PXP"
                timestamp = 1_700_000_000.0
                code = generate_totp(secret, timestamp=timestamp)
                options = IdentityOptions(totp_mode="opt_in")

                async with connection_scope(database) as session:
                    user = await create_test_user(
                        session,
                        email="totp-replay@example.com",
                    )
                    user_id = str(user.id)

                    store = TortoiseTOTPCredentialStore(session, secret_service)
                    credential_id = await store.create_pending_totp_credential(
                        user_id,
                        secret,
                    )
                    await store.activate_totp_credential(credential_id)

                    accepted, counter, error = await verify_totp_code_for_credential(
                        store=store,
                        credential_id=credential_id,
                        user_id=user_id,
                        code=code,
                        options=options,
                        timestamp=timestamp,
                    )

                    assert accepted is True
                    assert counter is not None
                    assert error is None

                async with connection_scope(database) as session:
                    credential = await IdentityTotpCredential.get_or_none(
                        id=uuid.UUID(credential_id),
                        using_db=session,
                    )
                    assert credential is not None
                    assert credential.last_used_counter == counter

                    store = TortoiseTOTPCredentialStore(session, secret_service)
                    (
                        replay_accepted,
                        replay_counter,
                        replay_error,
                    ) = await verify_totp_code_for_credential(
                        store=store,
                        credential_id=credential_id,
                        user_id=user_id,
                        code=code,
                        options=options,
                        timestamp=timestamp,
                    )

                    assert replay_accepted is False
                    assert replay_counter is None
                    assert replay_error == TOTP_CODE_REPLAY_MESSAGE
            finally:
                await close_database(database)

        await assert_replay_rejected()

    @pytest.mark.anyio
    async def test_wybra_auth_management_scope_uses_configured_secret_service_for_totp(
        self,
        tmp_path: Path,
    ) -> None:
        async def assert_management_secret_service_used() -> None:
            database = await initialise_auth_database(
                sqlite_file_url(tmp_path / "management-totp-crypto.sqlite3")
            )
            try:
                secret_service = SecretEnvelopeService.for_testing(version="management")
                async with connection_scope(database) as session:
                    user = await create_test_user(
                        session,
                        email="management-totp@example.com",
                    )
                    target = str(user.id)

                async with auth_persistence_scope(
                    database,
                    secret_service=secret_service,
                ) as scope:
                    result = await scope.management.provision_totp(
                        IdentityOptions(totp_mode="opt_in"),
                        target=target,
                    )

                assert result.is_ok() is True
                assert result.value is not None
                secret = result.value["totp"]["secret"]

                async with connection_scope(database) as session:
                    credential = (
                        await IdentityTotpCredential.all().using_db(session).first()
                    )
                    recovery_codes = list(
                        await IdentityTotpRecoveryCode.all().using_db(session)
                    )
                    store = TortoiseTOTPCredentialStore(session, secret_service)

                    assert credential is not None
                    assert credential.crypt_secret.startswith(
                        f"{ENVELOPE_PREFIX}|management|"
                    )
                    assert store.decrypt_totp_secret(credential) == secret
                    assert recovery_codes
                    assert all(
                        recovery_code.code_verifier.startswith(
                            f"{VERIFIER_PREFIX}|management|"
                        )
                        for recovery_code in recovery_codes
                    )
            finally:
                await close_database(database)

        await assert_management_secret_service_used()

    @pytest.mark.anyio
    async def test_wybra_auth_totp_verification_fails_closed_without_secret_keys(
        self,
        tmp_path: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        async def assert_missing_keys_handled() -> None:
            database = await initialise_auth_database(
                sqlite_file_url(tmp_path / "totp-missing-keys.sqlite3")
            )
            try:
                async with connection_scope(database) as session:
                    user = await create_test_user(
                        session,
                        email="totp-missing-key@example.com",
                    )

                    secret_service = make_test_only_secret_service()
                    store = TortoiseTOTPCredentialStore(session, secret_service)
                    secret = "JBSWY3DPEHPK3PXP"
                    credential_id = await store.create_pending_totp_credential(
                        str(user.id),
                        secret,
                    )
                    await store.activate_totp_credential(credential_id)

                    missing_key_store = TortoiseTOTPCredentialStore(
                        session,
                        SecretEnvelopeService.from_env({}),
                    )
                    accepted, counter, error = await verify_totp_code_for_credential(
                        store=missing_key_store,
                        credential_id=credential_id,
                        user_id=str(user.id),
                        code=generate_totp(secret),
                        options=IdentityOptions(totp_mode="opt_in"),
                    )

                    assert accepted is False
                    assert counter is None
                    assert error is None
            finally:
                await close_database(database)

        caplog.set_level(logging.ERROR, logger="wybra.auth.mfa.storage")
        await assert_missing_keys_handled()

        assert "Unable to verify TOTP credential" in caplog.text

    @pytest.mark.anyio
    async def test_wybra_auth_recovery_codes_use_keyed_verifiers(
        self,
        tmp_path: Path,
    ) -> None:
        async def assert_recovery_verifier_storage() -> None:
            database = await initialise_auth_database(
                sqlite_file_url(tmp_path / "recovery-verifier.sqlite3")
            )
            try:
                async with connection_scope(database) as session:
                    user = await create_test_user(
                        session,
                        email="recovery@example.com",
                    )

                    secret_service = make_test_only_secret_service()
                    credential_store = TortoiseTOTPCredentialStore(
                        session,
                        secret_service,
                    )
                    credential_id = (
                        await credential_store.create_pending_totp_credential(
                            str(user.id),
                            "JBSWY3DPEHPK3PXP",
                        )
                    )
                    await credential_store.activate_totp_credential(credential_id)
                    recovery_store = TortoiseRecoveryCodeStore(session, secret_service)
                    await recovery_store.replace_recovery_codes(
                        str(user.id),
                        credential_id,
                        ("R3C0V3RY",),
                    )
                    recovery_record = (
                        await IdentityTotpRecoveryCode.all().using_db(session).first()
                    )

                    assert recovery_record is not None
                    assert recovery_record.code_verifier.startswith(
                        f"{VERIFIER_PREFIX}|test|"
                    )
                    assert await recovery_store.consume_recovery_code(
                        str(user.id),
                        "R3C0V3RY",
                    )
                    assert not await recovery_store.consume_recovery_code(
                        str(user.id),
                        "R3C0V3RY",
                    )
            finally:
                await close_database(database)

        await assert_recovery_verifier_storage()

    @pytest.mark.anyio
    async def test_wybra_auth_webauthn_credential_store_lifecycle(
        self,
        tmp_path: Path,
    ) -> None:
        async def assert_webauthn_storage_lifecycle() -> None:
            database = await initialise_auth_database(
                sqlite_file_url(tmp_path / "webauthn-storage.sqlite3")
            )
            try:
                async with connection_scope(database) as session:
                    user = await create_test_user(
                        session,
                        email="passkey@example.com",
                    )

                    store = TortoiseWebAuthnCredentialStore(session)
                    row_id = await store.store_webauthn_credential(
                        str(user.id),
                        "credential-id",
                        b"public-key",
                        1,
                        label="  Work laptop  ",
                        user_verified=True,
                        credential_device_type="multi_device",
                        credential_backed_up=True,
                        transports=("internal",),
                        aaguid="aaguid",
                        attestation_format="none",
                    )

                    credential = await store.get_webauthn_credential("credential-id")
                    assert credential is not None
                    assert credential.id == row_id
                    assert credential.label == "Work laptop"
                    assert credential.public_key == b"public-key"
                    assert credential.transports == ("internal",)
                    assert (
                        await store.count_active_webauthn_credentials(str(user.id)) == 1
                    )

                    await store.update_webauthn_authentication(
                        "credential-id",
                        sign_count=2,
                        user_verified=False,
                        credential_device_type="single_device",
                        credential_backed_up=False,
                    )
                    updated = await store.get_user_webauthn_credential(
                        str(user.id),
                        row_id,
                    )
                    assert updated is not None
                    assert updated.sign_count == 2
                    assert updated.last_used_at is not None
                    assert updated.user_verified is False

                    assert await store.revoke_webauthn_credential(str(user.id), row_id)
                    assert (
                        await store.count_active_webauthn_credentials(str(user.id)) == 0
                    )
            finally:
                await close_database(database)

        await assert_webauthn_storage_lifecycle()

    def test_wybra_auth_webauthn_counter_regression_has_branch_reason(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        credential_id = webauthn_mfa.credential_id_to_text(b"credential")

        def reject_assertion(**_kwargs):
            raise InvalidAuthenticationResponse("Sign count regression detected.")

        monkeypatch.setattr(
            webauthn_mfa,
            "verify_authentication_response",
            reject_assertion,
        )

        with pytest.raises(webauthn_mfa.WebAuthnCeremonyError) as exc:
            webauthn_mfa.verify_passkey_authentication(
                IdentityOptions(
                    passkey_enabled=True,
                    passkey_rp_id="app.example.com",
                    passkey_rp_name="Example App",
                    passkey_allowed_origins=("https://app.example.com",),
                ),
                credential={"id": credential_id},
                expected_challenge=b"challenge",
                stored_credential=WebAuthnCredentialRecord(
                    id=str(uuid.uuid4()),
                    user_id=str(uuid.uuid4()),
                    credential_id=credential_id,
                    public_key=b"public-key",
                    sign_count=2,
                    status="active",
                    label=None,
                    created_at=1_000.0,
                    last_used_at=None,
                    revoked_at=None,
                    user_verified=True,
                    credential_device_type="multi_device",
                    credential_backed_up=True,
                    transports=("internal",),
                    aaguid=None,
                    attestation_format="none",
                ),
            )

        assert exc.value.reason == webauthn_mfa.WEBAUTHN_COUNTER_REGRESSION_REASON

    def test_user_verified_webauthn_assertion_satisfies_totp_requirement(self) -> None:
        now = 1_000.0
        assertion = AuthenticationAssertion(
            user_id="user-1",
            method="webauthn",
            asserted_at=now,
            ceremony_id="ceremony-1",
            user_verified=True,
        )

        assert not assertions_satisfy_required_methods(
            user_id="user-1",
            ceremony_id="ceremony-1",
            required_methods=(TOTP_ASSERTION_METHOD,),
            assertions=(assertion,),
            now=now,
        )

        assert assertions_satisfy_required_methods(
            user_id="user-1",
            ceremony_id="ceremony-1",
            required_methods=(TOTP_ASSERTION_METHOD,),
            assertions=(assertion,),
            now=now,
            webauthn_user_verification_satisfies_totp=True,
        )
        assert not assertions_satisfy_required_methods(
            user_id="user-1",
            ceremony_id="ceremony-1",
            required_methods=(TOTP_ASSERTION_METHOD,),
            assertions=(
                AuthenticationAssertion(
                    user_id="user-1",
                    method="webauthn",
                    asserted_at=now,
                    ceremony_id="ceremony-1",
                    user_verified=False,
                ),
            ),
            now=now,
            webauthn_user_verification_satisfies_totp=True,
        )

    @pytest.mark.anyio
    async def test_identity_user_email_stores_normalised_email(
        self, tmp_path: Path
    ) -> None:
        async def assert_normalised_email() -> None:
            database = await initialise_auth_database(
                sqlite_file_url(tmp_path / "normalised-email.sqlite3")
            )
            try:
                async with connection_scope(database) as session:
                    user = await User.create(
                        email="owner@example.com",
                        using_db=session,
                    )
                    identity_email = await IdentityUserEmail.create(
                        user_id=user.id,
                        email="Alias@Example.COM",
                        is_primary=True,
                        is_verified=True,
                        using_db=session,
                    )

                    assert identity_email.email == "alias@example.com"
            finally:
                await close_database(database)

        await assert_normalised_email()

    @pytest.mark.anyio
    async def test_wybra_auth_challenge_completion_consumes_existing_challenge(
        self,
    ) -> None:
        async def assert_challenge_completion() -> None:
            store = MemoryChallengeStore()
            challenge = await store.create_challenge(
                user_id="user-1",
                kind="totp",
                expires_at=datetime.now(UTC) + timedelta(minutes=5),
            )

            assert await complete_challenge(store, challenge.id) is True
            assert challenge.id in store.consumed
            assert await complete_challenge(store, challenge.id) is False

        await assert_challenge_completion()

    def test_wybra_auth_router_extension_plan_tracks_explicit_replacements(
        self,
    ) -> None:
        plan = RouterExtensionPlan(
            additive_route_names=("wybra.auth:challenge",),
            replacements=(
                RouteReplacement(
                    method="POST",
                    path="/login",
                    reason="Pause primary login for MFA challenge.",
                ),
            ),
        )

        assert plan.replaces("post", "/login") is True
        assert plan.replaces("GET", "/login") is False

    @pytest.mark.parametrize(
        ("value", "expected"),
        (
            (None, "/account"),
            ("", "/account"),
            ("/dashboard", "/dashboard"),
            ("/dashboard?tab=billing", "/dashboard?tab=billing"),
            ("/dashboard#ignored", "/dashboard"),
            ("https://evil.example/account", "/account"),
            ("//evil.example/account", "/account"),
            ("/%2f%2fevil.example/account", "/account"),
            ("/%5cevil.example/account", "/account"),
            ("/account%0d%0aLocation:%20https://evil.example", "/account"),
        ),
    )
    def test_normalise_return_to_accepts_only_local_redirect_paths(
        self,
        value: str | None,
        expected: str,
    ) -> None:
        assert normalise_return_to(value) == expected
