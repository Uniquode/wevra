from __future__ import annotations

import base64
import hashlib
import hmac
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from secrets import token_urlsafe
from typing import Any

from fastapi import HTTPException, Request
from fastapi.responses import Response
from starlette.datastructures import FormData

from wybra.events import observe
from wybra.events.security import csrf_denial_event
from wybra.forms.security import (
    FORM_BODY_MAX_BYTES,
    is_form_content_type,
    is_safe_method,
)

CSRF_COOKIE_NAME = "wybra_forms_csrf"
CSRF_FIELD_NAME = "csrf_token"
CSRF_HEADER_NAME = "x-csrf-token"
CSRF_FORM_DATA_STATE_ATTR = "csrf_form_data"
CSRF_MAX_FORM_BODY_BYTES = FORM_BODY_MAX_BYTES
CSRF_NONCE_MAX_LENGTH = 256
CSRF_NONCE_MIN_LENGTH = 32
CSRF_RESPONSE_FINALISATION_STATE_ATTR = "wybra_forms_csrf_finalise_response"
CSRF_TOKEN_BYTES = 32
CSRF_TOKEN_MAX_AGE_SECONDS = 3_600
CSRF_TOKEN_SEPARATOR = "."
CSRF_EXEMPT_ENDPOINT_ATTR = "__wybra_csrf_exempt__"

logger = logging.getLogger(__name__)


async def request_form_data(request: Request) -> FormData:
    form_data = getattr(request.state, CSRF_FORM_DATA_STATE_ATTR, None)
    if isinstance(form_data, FormData):
        return form_data

    form_data = await request.form()
    setattr(request.state, CSRF_FORM_DATA_STATE_ATTR, form_data)
    return form_data


def csrf_exempt(func: Callable[..., Any]) -> Callable[..., Any]:
    setattr(func, CSRF_EXEMPT_ENDPOINT_ATTR, True)
    return func


async def validate_csrf(request: Request) -> None:
    endpoint = request.scope.get("endpoint")
    if getattr(endpoint, CSRF_EXEMPT_ENDPOINT_ATTR, False):
        return

    protector = getattr(request.app.state, "csrf", None)
    if protector is None:
        return
    if not isinstance(protector, CsrfProtector):  # pragma: no cover - defensive
        raise RuntimeError("CSRF protector is not configured correctly.")

    if not await protector.validate_request(request):
        await _publish_csrf_denial(request)
        raise HTTPException(status_code=403, detail="Invalid CSRF token.")


@observe(csrf_denial_event, context="csrf")
async def _publish_csrf_denial(request: Request) -> None:
    """Record a rejected CSRF request without its token or rejection detail."""

    del request


def request_csrf_response_finalisation(request: Request) -> None:
    setattr(request.state, CSRF_RESPONSE_FINALISATION_STATE_ATTR, True)


def csrf_response_finalisation_requested(request: Request) -> bool:
    return bool(getattr(request.state, CSRF_RESPONSE_FINALISATION_STATE_ATTR, False))


@dataclass(frozen=True, slots=True)
class CsrfField:
    """Opaque CSRF input prepared for template rendering.

    This is intentionally not a declarative ``Form`` field: request CSRF
    validation occurs before ordinary form parsing. Future signed action data
    or one-time nonce metadata belongs here rather than in application forms.
    """

    name: str
    token: str = field(repr=False)

    def rendering_context(self) -> dict[str, str]:
        """Return the compatibility context consumed by the CSRF widget."""
        return {"csrf_field_name": self.name, "csrf_token": self.token}


@dataclass(frozen=True, slots=True)
class CsrfProtector:
    secret: str = field(repr=False)
    previous_secrets: tuple[str, ...] = field(default=(), repr=False)
    cookie_name: str = CSRF_COOKIE_NAME
    field_name: str = CSRF_FIELD_NAME
    cookie_secure: bool = False
    max_form_body_bytes: int = CSRF_MAX_FORM_BODY_BYTES
    token_max_age_seconds: float = CSRF_TOKEN_MAX_AGE_SECONDS
    clock: Callable[[], float] = field(
        default=time.time,
        repr=False,
        compare=False,
    )

    def __post_init__(self) -> None:
        if not isinstance(self.secret, str) or not self.secret.strip():
            raise ValueError("CSRF token secret must be a non-blank string.")
        previous_secrets: list[str] = []
        for index, raw_secret in enumerate(self.previous_secrets):
            if not isinstance(raw_secret, str):
                raise ValueError(
                    f"Previous CSRF token secret at index {index} is not a string."
                )
            secret = raw_secret.strip()
            if not secret:
                raise ValueError(
                    f"Previous CSRF token secret at index {index} is blank."
                )
            previous_secrets.append(secret)
        if self.token_max_age_seconds <= 0:
            raise ValueError("CSRF token max age must be positive.")
        object.__setattr__(self, "secret", self.secret.strip())
        object.__setattr__(self, "previous_secrets", tuple(previous_secrets))

    def token_context(self, request: Request) -> dict[str, Any]:
        csrf_field = self.create_field(request)
        return {
            **csrf_field.rendering_context(),
            "csrf_field": csrf_field,
            "csrf_header_name": CSRF_HEADER_NAME,
        }

    def create_field(self, request: Request) -> CsrfField:
        """Create the opaque CSRF field rendered by protected POST forms."""
        return CsrfField(
            name=self.field_name,
            token=self.create_token(self._request_nonce(request)),
        )

    def set_cookie(self, request: Request, response: Response) -> None:
        response.set_cookie(
            self.cookie_name,
            self._request_nonce(request),
            path="/",
            secure=self.cookie_secure,
            httponly=True,
            samesite="strict",
        )

    async def validate_request(self, request: Request) -> bool:
        if is_safe_method(request.method):
            return True

        cookie_nonce = request.cookies.get(self.cookie_name)
        submitted_token = request.headers.get(CSRF_HEADER_NAME)
        if submitted_token is not None:
            if not cookie_nonce:
                return self._reject(request, "missing_cookie_nonce_for_header")
            if self.validate_token(submitted_token, cookie_nonce):
                return True

            return self._reject(request, "invalid_header_token")

        if not self._can_parse_form_body(request):
            return False

        try:
            form_data = await request_form_data(request)
        except Exception:
            return self._reject(request, "form_parse_failed")
        setattr(request.state, CSRF_FORM_DATA_STATE_ATTR, form_data)
        submitted_token = form_data.get(self.field_name)
        if not isinstance(submitted_token, str) or not cookie_nonce:
            return self._reject(request, "missing_form_token_or_cookie_nonce")

        if self.validate_token(submitted_token, cookie_nonce):
            return True

        return self._reject(request, "invalid_form_token")

    def create_token(self, nonce: str) -> str:
        issued_at = _format_timestamp(self.clock())
        payload = f"{nonce}{CSRF_TOKEN_SEPARATOR}{issued_at}"
        return f"{payload}{CSRF_TOKEN_SEPARATOR}{self._signature(payload)}"

    def validate_token(self, token: str, cookie_nonce: str) -> bool:
        parts = token.split(CSRF_TOKEN_SEPARATOR)
        if len(parts) != 3:
            return False

        nonce, issued_at, signature = parts
        if nonce != cookie_nonce:
            return False

        if not self._is_fresh_timestamp(issued_at):
            return False

        payload = f"{nonce}{CSRF_TOKEN_SEPARATOR}{issued_at}"
        return any(
            hmac.compare_digest(signature, self._signature(payload, secret=secret))
            for secret in (self.secret, *self.previous_secrets)
        )

    def _request_nonce(self, request: Request) -> str:
        existing_nonce = getattr(request.state, "csrf_nonce", None)
        if isinstance(existing_nonce, str):
            return existing_nonce

        cookie_nonce = request.cookies.get(self.cookie_name)
        nonce = (
            cookie_nonce
            if isinstance(cookie_nonce, str) and self._is_valid_nonce(cookie_nonce)
            else token_urlsafe(CSRF_TOKEN_BYTES)
        )
        request.state.csrf_nonce = nonce
        return nonce

    def _signature(self, payload: str, *, secret: str | None = None) -> str:
        digest = hmac.new(
            (secret or self.secret).encode("utf-8"),
            payload.encode("utf-8"),
            hashlib.sha256,
        ).digest()
        return base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")

    def _is_fresh_timestamp(self, issued_at: str) -> bool:
        try:
            issued_timestamp = float(issued_at)
        except ValueError:
            return False
        age = self.clock() - issued_timestamp
        return 0 <= age <= self.token_max_age_seconds

    @staticmethod
    def _is_valid_nonce(nonce: str) -> bool:
        return (
            CSRF_NONCE_MIN_LENGTH <= len(nonce) <= CSRF_NONCE_MAX_LENGTH
            and CSRF_TOKEN_SEPARATOR not in nonce
        )

    def _can_parse_form_body(self, request: Request) -> bool:
        if not is_form_content_type(request.headers.get("content-type", "")):
            return self._reject(request, "unsupported_content_type")

        content_length = request.headers.get("content-length")
        if content_length is None:
            return self._reject(request, "missing_content_length")

        try:
            body_size = int(content_length)
        except ValueError:
            return self._reject(request, "invalid_content_length")

        if body_size < 0:
            return self._reject(request, "invalid_content_length")

        if body_size > self.max_form_body_bytes:
            return self._reject(request, "form_body_too_large")

        return True

    def _reject(self, request: Request, reason: str) -> bool:
        logger.debug(
            "CSRF request rejected.",
            extra={
                "csrf_reason": reason,
                "method": request.method,
                "path": request.scope.get("path", ""),
            },
        )
        return False


def _format_timestamp(timestamp: float) -> str:
    return str(int(timestamp))


__all__ = (
    "CSRF_COOKIE_NAME",
    "CSRF_EXEMPT_ENDPOINT_ATTR",
    "CSRF_FIELD_NAME",
    "CSRF_FORM_DATA_STATE_ATTR",
    "CSRF_HEADER_NAME",
    "CSRF_MAX_FORM_BODY_BYTES",
    "CSRF_NONCE_MAX_LENGTH",
    "CSRF_NONCE_MIN_LENGTH",
    "CSRF_RESPONSE_FINALISATION_STATE_ATTR",
    "CSRF_TOKEN_BYTES",
    "CSRF_TOKEN_MAX_AGE_SECONDS",
    "CSRF_TOKEN_SEPARATOR",
    "CsrfField",
    "CsrfProtector",
    "csrf_exempt",
    "csrf_response_finalisation_requested",
    "request_csrf_response_finalisation",
    "request_form_data",
    "validate_csrf",
)
