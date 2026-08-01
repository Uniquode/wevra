from __future__ import annotations

import logging
import secrets
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import jwt
from fastapi import Request
from pwdlib import PasswordHash
from pwdlib.hashers.argon2 import Argon2Hasher
from pwdlib.hashers.bcrypt import BcryptHasher

from wybra.auth.accounts.schemas import UserCreate, UserUpdate
from wybra.auth.delivery import IdentityDelivery, NullIdentityDelivery
from wybra.auth.email_normalisation import normalise_email_target
from wybra.auth.options import IdentityOptions
from wybra.auth.persistence.contracts import (
    DuplicateIdentityError,
    LocalUserRecord,
    UserStore,
)
from wybra.auth.result import (
    ERROR_INVALID_PASSWORD,
    ERROR_PASSWORD_TOO_SHORT,
    ERROR_PASSWORD_TOO_WEAK,
    ResultErrorType,
)

logger = logging.getLogger(__name__)
type PasswordProfileLookup = Callable[[LocalUserRecord], Awaitable[object | None]]
RESET_PASSWORD_TOKEN_AUDIENCE = "wybra:reset-password"
VERIFY_USER_TOKEN_AUDIENCE = "wybra:verify-user"
PASSWORD_POLICY_MESSAGES: dict[ResultErrorType, str] = {
    ERROR_PASSWORD_TOO_SHORT: "Password does not meet the minimum length requirement.",
    ERROR_PASSWORD_TOO_WEAK: "Password does not meet the strength requirement.",
}
DEFAULT_PASSWORD_POLICY_MESSAGE = "Password is invalid."
PUBLIC_PASSWORD_POLICY_MESSAGES = frozenset(
    {
        DEFAULT_PASSWORD_POLICY_MESSAGE,
        *PASSWORD_POLICY_MESSAGES.values(),
    }
)


class UserAlreadyExists(Exception):
    """Raised when creating or updating a user would duplicate an identity."""


class UserNotExists(Exception):
    """Raised when a user lookup does not resolve a local account."""


class InvalidID(Exception):
    """Raised when an identity ID cannot be parsed."""


class UserInactive(Exception):
    """Raised when an inactive user attempts an operation requiring activity."""


class UserAlreadyVerified(Exception):
    """Raised when verification is requested for an already verified user."""


class InvalidVerifyToken(Exception):
    """Raised when an email-verification token cannot be accepted."""


class InvalidResetPasswordToken(Exception):
    """Raised when a password-reset token cannot be accepted."""


class InvalidPasswordException(Exception):
    """Raised when password policy rejects a submitted password."""

    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(reason)


class PasswordHelper:
    """Password hashing boundary."""

    def __init__(self, password_hash: PasswordHash | None = None) -> None:
        self.password_hash = password_hash or PasswordHash(
            (
                Argon2Hasher(),
                BcryptHasher(),
            )
        )

    def verify_and_update(
        self,
        plain_password: str,
        hashed_password: str,
    ) -> tuple[bool, str | None]:
        return self.password_hash.verify_and_update(plain_password, hashed_password)

    def hash(self, password: str) -> str:
        return self.password_hash.hash(password)

    def generate(self) -> str:
        return secrets.token_urlsafe()


@dataclass(frozen=True, slots=True)
class PasswordPolicySubject:
    """Password-policy view combining account and optional profile fragments."""

    email: str
    id: uuid.UUID
    display_name: str | None = None
    preferred_name: str | None = None


class PasswordPolicyException(InvalidPasswordException):
    """Invalid-password boundary carrying a branchable policy error type."""

    def __init__(self, error_type: ResultErrorType | None) -> None:
        self.error_type = error_type
        super().__init__(_password_policy_message(error_type))


class UserManager:
    """Wybra-owned manager for local account lifecycle operations."""

    reset_password_token_lifetime_seconds: int = 3600
    reset_password_token_audience: str = RESET_PASSWORD_TOKEN_AUDIENCE
    verification_token_lifetime_seconds: int = 3600
    verification_token_audience: str = VERIFY_USER_TOKEN_AUDIENCE

    def __init__(
        self,
        user_store: UserStore,
        options: IdentityOptions,
        delivery: IdentityDelivery | None = None,
        password_helper: PasswordHelper | None = None,
        profile_lookup: PasswordProfileLookup | None = None,
    ) -> None:
        self.user_store = user_store
        self.delivery = delivery or NullIdentityDelivery()
        self.password_policy = options.resolved_password_policy()
        self.reset_password_token_secret = options.reset_password_token_secret
        self.verification_token_secret = options.verification_token_secret
        self.password_helper = password_helper or create_password_helper()
        self.profile_lookup = profile_lookup

    def parse_id(self, value: object) -> uuid.UUID:
        try:
            return value if isinstance(value, uuid.UUID) else uuid.UUID(str(value))
        except (TypeError, ValueError) as exc:
            raise InvalidID() from exc

    async def get(self, user_id: uuid.UUID) -> LocalUserRecord:
        user = await self.user_store.get(user_id)
        if user is None:
            raise UserNotExists()
        return user

    async def get_by_email(self, user_email: str) -> LocalUserRecord:
        user = await self.user_store.get_by_email(user_email)
        if user is None:
            raise UserNotExists()
        return user

    async def authenticate(self, credentials: object) -> LocalUserRecord | None:
        username = getattr(credentials, "username", None)
        password = getattr(credentials, "password", None)
        if not isinstance(username, str) or not isinstance(password, str):
            return None

        try:
            user = await self.get_by_email(username)
        except UserNotExists:
            self.password_helper.hash(password)
            return None

        if not user.password_login_enabled or not user.hashed_password:
            self.password_helper.hash(password)
            return None

        verified, updated_password_hash = self.password_helper.verify_and_update(
            password,
            user.hashed_password,
        )
        if not verified:
            return None
        if updated_password_hash is not None:
            user.hashed_password = updated_password_hash
            user = await self.user_store.save_user(user)

        return user

    async def validate_password(
        self,
        password: str,
        user: UserCreate | LocalUserRecord,
    ) -> None:
        validation = self.password_policy.validate(
            password,
            await self._password_policy_subject(user),
        )
        if validation.is_failure():
            logger.warning(
                "Password policy validation failed",
                extra={
                    "error_type": validation.error_type,
                    "policy_message": validation.message,
                    "user_id": getattr(user, "id", None),
                },
            )
            raise PasswordPolicyException(validation.error_type)

    async def create(
        self,
        user_create: UserCreate,
        safe: bool = False,
        request: Request | None = None,
    ) -> LocalUserRecord:
        await self.validate_password(user_create.password, user_create)

        normalised_email = normalise_email_target(user_create.email)
        if normalised_email is None:
            raise UserAlreadyExists()

        if await self.user_store.get_by_email(normalised_email):
            raise UserAlreadyExists()

        user_create_update = user_create.model_copy(update={"email": normalised_email})
        user_dict = (
            user_create_update.create_update_dict()
            if safe
            else user_create_update.create_update_dict_superuser()
        )
        password = user_dict.pop("password")
        user_dict["hashed_password"] = self.password_helper.hash(str(password))

        try:
            created_user = await self.user_store.create_local_user(
                user_dict,
                primary_email=normalised_email,
                after_create=lambda user: self.on_after_register(user, request),
            )
        except DuplicateIdentityError as exc:
            raise UserAlreadyExists() from exc

        return created_user

    async def update(
        self,
        user_update: UserUpdate,
        user: LocalUserRecord,
        safe: bool = False,
        request: Request | None = None,
    ) -> LocalUserRecord:
        update_dict = (
            user_update.create_update_dict()
            if safe
            else user_update.create_update_dict_superuser()
        )
        updated_user = await self._update(user, update_dict)
        await self.on_after_update(updated_user, update_dict, request)
        return updated_user

    async def request_verify(
        self,
        user: LocalUserRecord,
        request: Request | None = None,
    ) -> None:
        if not user.is_active:
            raise UserInactive()
        if user.is_verified:
            raise UserAlreadyVerified()

        token = _generate_jwt(
            {
                "sub": str(user.id),
                "email": user.email,
                "aud": self.verification_token_audience,
            },
            self.verification_token_secret,
            self.verification_token_lifetime_seconds,
        )
        await self.on_after_request_verify(user, token, request)

    async def verify(
        self,
        token: str,
        request: Request | None = None,
    ) -> LocalUserRecord:
        try:
            data = _decode_jwt(
                token,
                self.verification_token_secret,
                [self.verification_token_audience],
            )
            user_id = data["sub"]
            email = data["email"]
            parsed_id = self.parse_id(user_id)
            user = await self.get_by_email(email)
        except (KeyError, InvalidID, UserNotExists, jwt.PyJWTError) as exc:
            raise InvalidVerifyToken() from exc

        if parsed_id != user.id:
            raise InvalidVerifyToken()
        if user.is_verified:
            raise UserAlreadyVerified()

        verified_user = await self._update(user, {"is_verified": True})
        await self.on_after_verify(verified_user, request)
        return verified_user

    async def forgot_password(
        self,
        user: LocalUserRecord,
        request: Request | None = None,
    ) -> None:
        if not user.is_active:
            raise UserInactive()
        if not user.hashed_password:
            raise InvalidResetPasswordToken()

        token = _generate_jwt(
            {
                "sub": str(user.id),
                "password_fgpt": self.password_helper.hash(user.hashed_password),
                "aud": self.reset_password_token_audience,
            },
            self.reset_password_token_secret,
            self.reset_password_token_lifetime_seconds,
        )
        await self.on_after_forgot_password(user, token, request)

    async def reset_password(
        self,
        token: str,
        password: str,
        request: Request | None = None,
    ) -> LocalUserRecord:
        try:
            data = _decode_jwt(
                token,
                self.reset_password_token_secret,
                [self.reset_password_token_audience],
            )
            user_id = data["sub"]
            password_fingerprint = data["password_fgpt"]
            parsed_id = self.parse_id(user_id)
            user = await self.get(parsed_id)
        except (KeyError, InvalidID, UserNotExists, jwt.PyJWTError) as exc:
            raise InvalidResetPasswordToken() from exc

        if not user.hashed_password:
            raise InvalidResetPasswordToken()
        valid_password_fingerprint, _ = self.password_helper.verify_and_update(
            user.hashed_password,
            password_fingerprint,
        )
        if not valid_password_fingerprint:
            raise InvalidResetPasswordToken()
        if not user.is_active:
            raise UserInactive()

        updated_user = await self._update(user, {"password": password})
        await self.on_after_reset_password(updated_user, request)
        return updated_user

    async def _update(
        self,
        user: LocalUserRecord,
        update_dict: dict[str, object],
    ) -> LocalUserRecord:
        validated_update_dict: dict[str, object] = {}
        primary_email: str | None = None
        for field, value in update_dict.items():
            if field == "email":
                if not isinstance(value, str):
                    continue
                normalised_email = normalise_email_target(value)
                if normalised_email is None:
                    raise UserAlreadyExists()
                if normalised_email == user.email:
                    continue
                try:
                    await self.get_by_email(normalised_email)
                    raise UserAlreadyExists()
                except UserNotExists:
                    primary_email = normalised_email
                    validated_update_dict["email"] = normalised_email
                    validated_update_dict["is_verified"] = False
            elif field == "password" and value is not None:
                await self.validate_password(str(value), user)
                validated_update_dict["hashed_password"] = self.password_helper.hash(
                    str(value)
                )
            else:
                validated_update_dict[field] = value

        for key, value in validated_update_dict.items():
            setattr(user, key, value)
        try:
            return await self.user_store.save_user(
                user,
                primary_email=primary_email,
                primary_email_verified=False if primary_email is not None else None,
            )
        except DuplicateIdentityError as exc:
            raise UserAlreadyExists() from exc

    async def _password_policy_subject(
        self,
        user: UserCreate | LocalUserRecord,
    ) -> UserCreate | LocalUserRecord | PasswordPolicySubject:
        if isinstance(user, UserCreate) or self.profile_lookup is None:
            return user

        profile = await self.profile_lookup(user)
        if profile is None:
            return user

        return PasswordPolicySubject(
            email=user.email,
            id=user.id,
            display_name=_optional_profile_text(profile, "display_name"),
            preferred_name=_optional_profile_text(profile, "preferred_name"),
        )

    async def on_after_register(
        self,
        user: LocalUserRecord,
        request: Request | None = None,
    ) -> None:
        del user, request

    async def on_after_update(
        self,
        user: LocalUserRecord,
        update_dict: dict[str, object],
        request: Request | None = None,
    ) -> None:
        del user, update_dict, request

    async def on_after_request_verify(
        self,
        user: LocalUserRecord,
        token: str,
        request: Request | None = None,
    ) -> None:
        await self.delivery.send_verification_token(user, token, request)

    async def on_after_verify(
        self,
        user: LocalUserRecord,
        request: Request | None = None,
    ) -> None:
        del user, request

    async def on_after_forgot_password(
        self,
        user: LocalUserRecord,
        token: str,
        request: Request | None = None,
    ) -> None:
        await self.delivery.send_reset_password_token(user, token, request)

    async def on_after_reset_password(
        self,
        user: LocalUserRecord,
        request: Request | None = None,
    ) -> None:
        del user, request


def create_password_helper() -> PasswordHelper:
    return PasswordHelper()


def public_password_failure_message(error: object) -> str:
    if isinstance(error, PasswordPolicyException):
        return str(error.reason)

    reason = getattr(error, "reason", error)
    if isinstance(reason, str) and reason in PUBLIC_PASSWORD_POLICY_MESSAGES:
        return reason

    return DEFAULT_PASSWORD_POLICY_MESSAGE


def public_password_error_type(error: object) -> ResultErrorType:
    error_type = getattr(error, "error_type", None)
    if error_type in PASSWORD_POLICY_MESSAGES:
        return error_type

    return ERROR_INVALID_PASSWORD


def create_user_manager(
    connection,
    options: IdentityOptions,
    delivery: IdentityDelivery | None = None,
    profile_lookup: PasswordProfileLookup | None = None,
) -> UserManager:
    from wybra.auth.persistence import create_user_store

    return create_user_manager_from_store(
        create_user_store(connection),
        options,
        delivery,
        profile_lookup,
    )


def create_user_manager_from_store(
    store: UserStore,
    options: IdentityOptions,
    delivery: IdentityDelivery | None = None,
    profile_lookup: PasswordProfileLookup | None = None,
) -> UserManager:
    return UserManager(
        store,
        options,
        delivery,
        create_password_helper(),
        profile_lookup,
    )


def _generate_jwt(
    data: dict[str, object],
    secret: str,
    lifetime_seconds: int | None = None,
) -> str:
    payload = dict(data)
    if lifetime_seconds:
        payload["exp"] = datetime.now(UTC) + timedelta(seconds=lifetime_seconds)
    return jwt.encode(payload, secret, algorithm="HS256")


def _decode_jwt(
    encoded_jwt: str,
    secret: str,
    audience: list[str],
) -> dict[str, Any]:
    return jwt.decode(
        encoded_jwt,
        secret,
        audience=audience,
        algorithms=["HS256"],
    )


def decode_identity_jwt(
    encoded_jwt: str,
    secret: str,
    audience: list[str],
) -> dict[str, Any]:
    return _decode_jwt(encoded_jwt, secret, audience)


def _password_policy_message(error_type: ResultErrorType | None) -> str:
    if error_type is None:
        return DEFAULT_PASSWORD_POLICY_MESSAGE

    return PASSWORD_POLICY_MESSAGES.get(error_type, DEFAULT_PASSWORD_POLICY_MESSAGE)


def _optional_profile_text(profile: object, attribute: str) -> str | None:
    value = getattr(profile, attribute, None)
    return value if isinstance(value, str) else None
