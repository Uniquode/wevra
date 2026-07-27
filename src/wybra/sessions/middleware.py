from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field

from fastapi import FastAPI, Request
from fastapi.responses import Response

from wybra.auth.timestamps import current_timestamp
from wybra.diagnostics import backend_operation_diagnostics
from wybra.events import observe
from wybra.events.sessions import session_event
from wybra.sessions.cleanup import SessionCleanupRegistry
from wybra.sessions.ids import create_session_id, validate_session_id
from wybra.sessions.settings import SessionsSettings
from wybra.sessions.state import RequestSession
from wybra.sessions.storage import CookieSessionStorage, SessionRecord, SessionStorage

SESSION_MIDDLEWARE_STATE_ATTRIBUTE = "wybra_session_middleware_registered"
SESSION_CLEANUP_INTERVAL_SECONDS = 60 * 60


@dataclass(frozen=True, slots=True)
class SessionMiddlewareContext:
    settings: SessionsSettings
    storage: SessionStorage
    cleanup_registry: SessionCleanupRegistry | None = None
    _last_cleanup_at: float | None = field(default=None, init=False, repr=False)

    async def load_session(self, request: Request, *, now: float) -> RequestSession:
        try:
            async with backend_operation_diagnostics(
                "session",
                "load",
                attributes={"backend": type(self.storage).__name__},
            ):
                await self.cleanup_expired(now=now)
                cookie_value = request.cookies.get(self.settings.cookie_name)
                if isinstance(self.storage, CookieSessionStorage):
                    session = await self._load_cookie_session(cookie_value, now=now)
                else:
                    session = await self._load_server_side_session(
                        cookie_value, now=now
                    )
        except Exception as exc:
            await self._publish("load", "failed", error=exc)
            raise
        await self._publish(
            "load",
            "invalid" if session.invalid_cookie else "succeeded",
        )
        if not session.invalid_cookie:
            session.prospective_expires_at = (
                now + self.settings.resolved_lifetime_seconds
            )
            if session.session_id is None:
                session.expires_at = session.prospective_expires_at
        return session

    async def cleanup_expired(self, *, now: float) -> None:
        last_cleanup_at = self._last_cleanup_at
        if (
            last_cleanup_at is not None
            and now - last_cleanup_at < SESSION_CLEANUP_INTERVAL_SECONDS
        ):
            return
        object.__setattr__(self, "_last_cleanup_at", now)
        await self.storage.cleanup(now=now)

    async def finalise_response(
        self,
        response: Response,
        session: RequestSession,
        *,
        now: float,
    ) -> None:
        operation = "unchanged"
        succeeded = False
        try:
            async with backend_operation_diagnostics(
                "session",
                "finalise",
                attributes=lambda: {
                    "backend": type(self.storage).__name__,
                    "modified": session.modified,
                },
            ):
                if session.cleared or (session.modified and not session):
                    operation = "deleted"
                    await self._cleanup_session_data(session.cleanup_data())
                    if session.session_id is not None:
                        await self.storage.delete(session.session_id)
                    self._delete_cookie(response)
                    succeeded = True
                    return
                if not session.modified:
                    if session.invalid_cookie:
                        operation = "invalidated"
                        self._delete_cookie(response)
                    succeeded = True
                    return
                operation = "created" if session.session_id is None else "saved"
                session_id = session.session_id or create_session_id(now=now)
                created_at = (
                    session.created_at if session.created_at is not None else now
                )
                record = SessionRecord(
                    data=dict(session.data),
                    created_at=created_at,
                    updated_at=now,
                    expires_at=now + self.settings.resolved_lifetime_seconds,
                )
                if isinstance(self.storage, CookieSessionStorage):
                    cookie_value = self.storage.dump_cookie(session_id, record)
                else:
                    await self.storage.save(session_id, record)
                    cookie_value = session_id
                session.session_id = session_id
                session.created_at = created_at
                session.expires_at = record.expires_at
                self._set_cookie(response, cookie_value)
            succeeded = True
        except Exception as exc:
            await self._publish(operation, "failed", error=exc)
            raise
        finally:
            if succeeded and operation != "unchanged":
                await self._publish(operation, "succeeded")

    @observe(session_event)
    async def _publish(
        self,
        operation: str,
        outcome: str,
        *,
        error: Exception | None = None,
    ) -> None:
        del operation, outcome, error

    async def _load_cookie_session(
        self,
        cookie_value: str | None,
        *,
        now: float,
    ) -> RequestSession:
        if cookie_value is None:
            return RequestSession()
        assert isinstance(self.storage, CookieSessionStorage)
        loaded = self.storage.decode_cookie(cookie_value)
        if loaded is None:
            return RequestSession(invalid_cookie=True)
        session_id, record = loaded
        if record.expired(now):
            await self._cleanup_session_data(record.data)
            return RequestSession(invalid_cookie=True)
        return RequestSession(
            data=dict(record.data),
            session_id=session_id,
            created_at=record.created_at,
            expires_at=record.expires_at,
        )

    async def _cleanup_session_data(self, data: Mapping[str, object]) -> None:
        if self.cleanup_registry is not None:
            await self.cleanup_registry.cleanup_session_data(data)

    async def _load_server_side_session(
        self,
        cookie_value: str | None,
        *,
        now: float,
    ) -> RequestSession:
        if cookie_value is None:
            return RequestSession()
        try:
            session_id = validate_session_id(cookie_value)
        except Exception:
            return RequestSession(invalid_cookie=True)
        record = await self.storage.load(session_id, now=now)
        if record is None:
            return RequestSession(session_id=session_id, invalid_cookie=True)
        return RequestSession(
            data=dict(record.data),
            session_id=session_id,
            created_at=record.created_at,
            expires_at=record.expires_at,
        )

    def _set_cookie(self, response: Response, value: str) -> None:
        response.set_cookie(
            self.settings.cookie_name,
            value,
            max_age=max(1, int(self.settings.resolved_lifetime_seconds)),
            path=self.settings.cookie_path,
            domain=self.settings.cookie_domain,
            secure=self.settings.resolved_cookie_secure,
            httponly=True,
            samesite=self.settings.resolved_cookie_same_site,
        )

    def _delete_cookie(self, response: Response) -> None:
        response.delete_cookie(
            self.settings.cookie_name,
            path=self.settings.cookie_path,
            domain=self.settings.cookie_domain,
        )


def register_session_middleware(
    app: FastAPI,
    context: SessionMiddlewareContext,
) -> None:
    if getattr(app.state, SESSION_MIDDLEWARE_STATE_ATTRIBUTE, False):
        return

    @app.middleware("http")
    async def wybra_session_middleware(
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        session = await context.load_session(request, now=current_timestamp())
        request.scope["session"] = session
        response = await call_next(request)
        await context.finalise_response(response, session, now=current_timestamp())
        return response

    setattr(app.state, SESSION_MIDDLEWARE_STATE_ATTRIBUTE, True)


__all__ = (
    "SESSION_MIDDLEWARE_STATE_ATTRIBUTE",
    "SESSION_CLEANUP_INTERVAL_SECONDS",
    "SessionMiddlewareContext",
    "register_session_middleware",
)
