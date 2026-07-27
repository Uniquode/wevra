from __future__ import annotations

import asyncio
import sys
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from importlib import import_module
from inspect import BoundArguments
from pathlib import Path
from types import ModuleType
from typing import Annotated, ClassVar, Protocol, cast
from uuid import uuid7

import pytest
from fastapi import FastAPI, File, Request, UploadFile
from fastapi.responses import Response, StreamingResponse
from jinja2 import TemplateNotFound

from wybra.auth.options import IdentityOptions
from wybra.auth.routes.totp import verify_totp_code_for_credential
from wybra.config import MappingConfigSource
from wybra.db.persistence import close_database, create_database
from wybra.diagnostics import (
    DiagnosticsCapability,
    diagnostic_context,
)
from wybra.diagnostics.capabilities import activate_process_diagnostics
from wybra.diagnostics.event_projection import register_event_projection
from wybra.events import available_event_scopes, event_scope
from wybra.events._core import (
    _CURRENT_CONTEXT,
    BEGIN,
    CAPABILITY,
    EVENT_HISTORY_LIMIT,
    EVT_ACCOUNT,
    EVT_CREDENTIAL,
    EVT_EVENTS,
    EVT_EVENTS_ERRORS,
    EVT_FORM,
    EVT_REQUEST,
    EVT_ROUTE,
    EVT_SITE,
    EVT_SQL,
    EVT_TEMPLATE,
    EVT_VIEW,
    TRANSACTION,
    Event,
    EventContext,
    EventDispatcher,
    EventHandler,
    EventOutcome,
    EventRuntimeError,
    EventsCapability,
    current_context,
    event_segment,
    events_enabled,
    observe,
)
from wybra.events.auth import (
    AccountLifecycleEvent,
    CredentialAccessEvent,
    publish_account_lifecycle,
)
from wybra.events.db import DatabaseStatementEvent
from wybra.events.forms import FormValidationCompletedEvent
from wybra.events.http import RequestCompletedEvent
from wybra.events.site import (
    CapabilityProvidedEvent,
    CapabilityResolvedEvent,
    CapabilityUnavailableEvent,
    ModulePostSetupEvent,
    ModuleSetupEvent,
    ModuleSetupFinalisationEvent,
    SiteLifecycleEvent,
)
from wybra.events.template import TemplateRenderCompletedEvent
from wybra.events.views import RouteDispatchedEvent, ViewCompletedEvent
from wybra.forms import Form, TextField
from wybra.site import Site, SiteCapabilityError, start
from wybra.template import DefaultTemplateCapability
from wybra.testing import WybraTestClient
from wybra.views import View, ViewRouter


@dataclass(frozen=True, slots=True)
class TransactionStarted(Event):
    kind: ClassVar = BEGIN
    transaction_id: str


class ProxyTestCapability:
    pass


class UnavailableProxyCapability:
    pass


@dataclass(frozen=True, slots=True)
class TransactionOnly(Event):
    kind: ClassVar = event_segment("transactionsonly")
    transaction_id: str


@dataclass(frozen=True, slots=True)
class ObservedCacheSet(Event):
    event_scope: ClassVar = EVT_SQL(BEGIN)
    owner: str
    duration_seconds: float


class ObservedForm(Form):
    title = TextField(required=True)


class _DrainableEvents(Protocol):
    async def _drain(self) -> None: ...


async def _drain_events(events: EventsCapability) -> None:
    """Wait for internally queued delivery in deterministic event tests."""
    await cast(_DrainableEvents, events)._drain()


class RejectedTotpStore:
    async def verify_totp_credential(
        self,
        *,
        credential_id: str,
        user_id: str,
        code: str,
        period_seconds: int,
        allowed_drift: int,
        expected_status: str,
        timestamp: float | None,
    ) -> tuple[bool, int | None, str | None]:
        del (
            credential_id,
            user_id,
            code,
            period_seconds,
            allowed_drift,
            expected_status,
            timestamp,
        )
        return False, None, "invalid"


def _request_context_id() -> str:
    context = current_context()
    assert context is not None
    assert context.request_id is not None
    return str(context.request_id)


class TestEventDispatcher:
    def test_public_selector_surface_supports_subscriptions(self) -> None:
        selector = event_scope("sql")

        assert str(selector) == "sql"
        assert (selector, "Database statement and transaction diagnostics.") in (
            available_event_scopes()
        )

    @pytest.mark.anyio
    async def test_dispatches_matching_handlers_in_registration_order(self) -> None:
        dispatcher = EventDispatcher()
        observed: list[str] = []

        async def root_handler(event: Event) -> None:
            observed.append(f"root:{event.scope}")

        async def transaction_handler(event: Event) -> None:
            observed.append(f"transaction:{event.scope}")

        await dispatcher.subscribe(EVT_SQL, root_handler)
        await dispatcher.subscribe(EVT_SQL(TRANSACTION), transaction_handler)

        await dispatcher.publish(
            TransactionStarted(
                topic=EVT_SQL(TRANSACTION, BEGIN), transaction_id="transaction-1"
            )
        )
        await _drain_events(dispatcher)

        assert observed == [
            "root:sql.transaction.begin",
            "transaction:sql.transaction.begin",
        ]

    @pytest.mark.anyio
    async def test_prefix_subscription_does_not_match_partial_segments(self) -> None:
        dispatcher = EventDispatcher()
        observed: list[str] = []

        async def transaction_handler(event: Event) -> None:
            observed.append(str(event.scope))

        await dispatcher.subscribe(EVT_SQL(TRANSACTION), transaction_handler)

        await dispatcher.publish(
            TransactionOnly(
                topic=EVT_SQL("transactionsonly"), transaction_id="transaction-1"
            )
        )

        assert observed == []

    @pytest.mark.anyio
    async def test_handler_failure_is_logged_and_later_handlers_still_run(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        dispatcher = EventDispatcher()
        observed: list[str] = []

        async def failing_handler(_event: Event) -> None:
            raise RuntimeError("expected handler failure")

        async def later_handler(event: Event) -> None:
            observed.append(str(event.scope))

        await dispatcher.subscribe(EVT_SQL, failing_handler)
        await dispatcher.subscribe(EVT_SQL, later_handler)

        await dispatcher.publish(
            TransactionStarted(topic=EVT_SQL(BEGIN), transaction_id="transaction-1")
        )
        await _drain_events(dispatcher)

        assert observed == ["sql.begin"]
        assert "Event handler failed" in caplog.messages

    @pytest.mark.anyio
    async def test_event_publication_returns_before_a_slow_handler_completes(
        self,
    ) -> None:
        dispatcher = EventDispatcher()
        handler_started = asyncio.Event()
        release_handler = asyncio.Event()

        async def waiting_handler(event: Event) -> None:
            handler_started.set()
            await release_handler.wait()

        await dispatcher.subscribe(EVT_SQL, waiting_handler)

        publication = asyncio.create_task(
            dispatcher.publish(
                TransactionStarted(topic=EVT_SQL(BEGIN), transaction_id="transaction-1")
            )
        )
        await handler_started.wait()
        assert await publication is None
        release_handler.set()
        await _drain_events(dispatcher)

    @pytest.mark.anyio
    async def test_event_publication_does_not_run_synchronous_handler_work(
        self,
    ) -> None:
        dispatcher = EventDispatcher()

        async def blocking_handler(_event: Event) -> None:
            import time

            time.sleep(0.08)

        await dispatcher.subscribe(EVT_SQL, blocking_handler)

        started = asyncio.get_running_loop().time()
        await dispatcher.publish(
            TransactionStarted(topic=EVT_SQL(BEGIN), transaction_id="transaction-1")
        )

        assert asyncio.get_running_loop().time() - started < 0.04
        await _drain_events(dispatcher)

    @pytest.mark.anyio
    async def test_close_drains_started_delivery_and_rejects_later_events(
        self,
    ) -> None:
        dispatcher = EventDispatcher()
        handler_started = asyncio.Event()
        release_handler = asyncio.Event()
        delivered: list[str] = []

        async def waiting_handler(event: Event) -> None:
            transaction = cast(TransactionStarted, event)
            delivered.append(transaction.transaction_id)
            handler_started.set()
            await release_handler.wait()

        await dispatcher.subscribe(EVT_SQL, waiting_handler)
        await dispatcher.publish(
            TransactionStarted(topic=EVT_SQL(BEGIN), transaction_id="first")
        )
        await handler_started.wait()

        close = asyncio.create_task(dispatcher.close())
        await asyncio.sleep(0)
        assert not close.done()

        release_handler.set()
        await close
        await dispatcher.publish(
            TransactionStarted(topic=EVT_SQL(BEGIN), transaction_id="second")
        )

        assert delivered == ["first"]

    @pytest.mark.anyio
    async def test_pending_delivery_evicts_oldest_undelivered_events(
        self,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        monkeypatch.setattr(
            "wybra.events._core.EVENT_DELIVERY_PENDING_LIMIT",
            2,
        )
        dispatcher = EventDispatcher()
        first_handler_started = asyncio.Event()
        release_first_handler = asyncio.Event()
        delivered: list[str] = []

        async def slow_handler(event: Event) -> None:
            transaction = cast(TransactionStarted, event)
            delivered.append(transaction.transaction_id)
            if transaction.transaction_id == "first":
                first_handler_started.set()
                await release_first_handler.wait()

        await dispatcher.subscribe(EVT_SQL, slow_handler)
        await dispatcher.publish(
            TransactionStarted(topic=EVT_SQL(BEGIN), transaction_id="first")
        )
        await first_handler_started.wait()

        for transaction_id in ("second", "third", "fourth"):
            await dispatcher.publish(
                TransactionStarted(topic=EVT_SQL(BEGIN), transaction_id=transaction_id)
            )

        assert any(
            "Event delivery backlog is full" in message for message in caplog.messages
        )
        release_first_handler.set()
        await _drain_events(dispatcher)

        assert delivered == ["first", "third", "fourth"]

    @pytest.mark.anyio
    async def test_handler_failure_records_secret_safe_diagnostics(self) -> None:
        dispatcher = EventDispatcher()
        diagnostics = DiagnosticsCapability()

        async def failing_handler(_event: Event) -> None:
            raise RuntimeError("password=should-not-appear")

        await dispatcher.subscribe(EVT_SQL, failing_handler)

        async with diagnostic_context(
            diagnostics,
            kind="event_test",
            description="Event handler failure",
            level="trace",
        ):
            await dispatcher.publish(
                TransactionStarted(topic=EVT_SQL(BEGIN), transaction_id="secret-value")
            )
            await _drain_events(dispatcher)

        events = diagnostics.snapshots(EVT_EVENTS_ERRORS)[0].events

        handler_failure = next(
            event for event in events if event["name"] == "errors.handler.failed"
        )
        attributes = cast(dict[str, object], handler_failure["attributes"])
        assert attributes["error_type"] == "RuntimeError"
        assert "secret-value" not in str(handler_failure)
        assert "should-not-appear" not in str(handler_failure)

    @pytest.mark.anyio
    async def test_handler_failure_is_retained_at_default_diagnostics_settings(
        self,
    ) -> None:
        site = await start(
            FastAPI(),
            config_source=MappingConfigSource(
                {
                    "app": {
                        "modules": (),
                        "deployment_environment": "local",
                    },
                    "wybra.events": {"enabled": True},
                    "wybra.diagnostics": {"events_enabled": True},
                }
            ),
        )
        try:
            diagnostics = site.require_capability(DiagnosticsCapability)
            dispatcher = site.require_capability(EventsCapability)

            async def failing_handler(_event: Event) -> None:
                raise RuntimeError("expected handler failure")

            await dispatcher.subscribe(EVT_EVENTS, failing_handler)

            async with diagnostic_context(
                diagnostics,
                kind="event_test",
                description="Default diagnostics settings",
            ):
                await dispatcher.publish(
                    TransactionStarted(
                        topic=EVT_EVENTS(BEGIN), transaction_id="transaction-1"
                    )
                )
                await _drain_events(dispatcher)

            events = diagnostics.snapshots(EVT_EVENTS_ERRORS)[0].events
        finally:
            await site.close()

        assert [event["name"] for event in events] == ["errors.handler.failed"]
        assert events[0]["level"] == "info"

    @pytest.mark.anyio
    async def test_dispatch_diagnostic_reports_failed_handlers(self) -> None:
        dispatcher = EventDispatcher()
        diagnostics = DiagnosticsCapability()

        async def failing_handler(_event: Event) -> None:
            raise RuntimeError("expected handler failure")

        await dispatcher.subscribe(EVT_EVENTS, failing_handler)

        async with diagnostic_context(
            diagnostics,
            kind="event_test",
            description="Dispatch result",
            level="trace",
        ):
            await dispatcher.publish(
                TransactionStarted(
                    topic=EVT_EVENTS(BEGIN), transaction_id="transaction-1"
                )
            )
            await _drain_events(dispatcher)

        events = diagnostics.snapshots(EVT_EVENTS)[0].events
        dispatch = next(event for event in events if event["name"] == "dispatch")

        assert dispatch["result"] == "error"

    @pytest.mark.anyio
    async def test_delivery_does_not_require_diagnostics(self) -> None:
        dispatcher = EventDispatcher()
        observed: list[str] = []

        async def handler(event: Event) -> None:
            observed.append(str(event.scope))

        await dispatcher.subscribe(EVT_EVENTS, handler)

        await dispatcher.publish(
            TransactionStarted(topic=EVT_EVENTS(BEGIN), transaction_id="transaction-1")
        )
        await _drain_events(dispatcher)

        assert observed == ["events.begin"]

    @pytest.mark.anyio
    async def test_rejects_synchronous_handlers(self) -> None:
        def handler(_event: Event) -> None:
            pass

        with pytest.raises(TypeError, match="async"):
            await EventDispatcher().subscribe(EVT_SQL, cast(EventHandler, handler))

    @pytest.mark.anyio
    async def test_accepts_an_object_with_an_async_call_method(self) -> None:
        class Handler:
            async def __call__(self, _event: Event) -> None:
                pass

        await EventDispatcher().subscribe(EVT_SQL, Handler())

    @pytest.mark.anyio
    async def test_history_replays_only_the_latest_bounded_events(self) -> None:
        dispatcher = EventDispatcher()
        replayed: list[str] = []

        async def handler(event: Event) -> None:
            assert isinstance(event, TransactionStarted)
            replayed.append(event.transaction_id)

        for index in range(EVENT_HISTORY_LIMIT + 1):
            await dispatcher.publish(
                TransactionStarted(
                    topic=EVT_SQL(BEGIN), transaction_id=f"transaction-{index}"
                )
            )

        await dispatcher.subscribe(EVT_SQL, handler, history=True)

        assert replayed == [
            f"transaction-{index}" for index in range(1, EVENT_HISTORY_LIMIT + 1)
        ]

    @pytest.mark.anyio
    async def test_ordinary_subscription_remains_live_only(self) -> None:
        dispatcher = EventDispatcher()
        observed: list[str] = []

        await dispatcher.publish(
            TransactionStarted(topic=EVT_SQL(BEGIN), transaction_id="before")
        )

        async def handler(event: Event) -> None:
            assert isinstance(event, TransactionStarted)
            observed.append(event.transaction_id)

        await dispatcher.subscribe(EVT_SQL, handler)
        await dispatcher.publish(
            TransactionStarted(topic=EVT_SQL(BEGIN), transaction_id="after")
        )
        await _drain_events(dispatcher)

        assert observed == ["after"]

    @pytest.mark.anyio
    async def test_history_subscription_does_not_duplicate_a_concurrent_event(
        self,
    ) -> None:
        dispatcher = EventDispatcher()
        replay_started = asyncio.Event()
        release_replay = asyncio.Event()
        observed: list[str] = []

        await dispatcher.publish(
            TransactionStarted(topic=EVT_SQL(BEGIN), transaction_id="retained")
        )

        async def handler(event: Event) -> None:
            assert isinstance(event, TransactionStarted)
            if event.transaction_id == "retained":
                replay_started.set()
                await release_replay.wait()
            observed.append(event.transaction_id)

        registration = asyncio.create_task(
            dispatcher.subscribe(EVT_SQL, handler, history=True)
        )
        await replay_started.wait()
        await dispatcher.publish(
            TransactionStarted(topic=EVT_SQL(BEGIN), transaction_id="live")
        )
        release_replay.set()
        await registration
        await _drain_events(dispatcher)

        assert sorted(observed) == ["live", "retained"]

    @pytest.mark.anyio
    async def test_diagnostics_projection_replays_retained_events(self) -> None:
        dispatcher = EventDispatcher()
        await dispatcher.publish(
            TransactionStarted(topic=EVT_SQL(BEGIN), transaction_id="startup")
        )
        diagnostics = DiagnosticsCapability(allowed_scopes=(EVT_SQL,), level="trace")
        activate_process_diagnostics(diagnostics)
        try:
            await register_event_projection(dispatcher, (EVT_SQL,))
            snapshots = diagnostics.snapshots(EVT_SQL)
        finally:
            await diagnostics.close()

        assert len(snapshots) == 1
        assert [event["name"] for event in snapshots[0].events] == ["begin"]


class TestEventsSiteIntegration:
    @pytest.mark.anyio
    async def test_rejects_a_second_live_site_in_the_same_process(self) -> None:
        first_site = await start(
            FastAPI(),
            config_source=MappingConfigSource(
                {"app": {"modules": (), "deployment_environment": "local"}}
            ),
        )
        try:
            with pytest.raises(EventRuntimeError, match="Only one Wybra Site"):
                await start(
                    FastAPI(),
                    config_source=MappingConfigSource(
                        {
                            "app": {
                                "modules": (),
                                "deployment_environment": "local",
                            }
                        }
                    ),
                )
        finally:
            await first_site.close()

    @pytest.mark.anyio
    async def test_events_enabled_lazily_resolves_the_single_site_runtime(
        self,
    ) -> None:
        site = await start(
            FastAPI(),
            config_source=MappingConfigSource(
                {
                    "app": {"modules": (), "deployment_environment": "local"},
                    "wybra.events": {"enabled": False},
                }
            ),
        )
        try:
            assert not events_enabled()
        finally:
            await site.close()

    @pytest.mark.anyio
    async def test_observe_uses_bound_arguments_and_snapshots_context(self) -> None:
        site = await start(
            FastAPI(),
            config_source=MappingConfigSource(
                {
                    "app": {"modules": (), "deployment_environment": "local"},
                    "wybra.events": {"enabled": True},
                }
            ),
        )
        observed: list[ObservedCacheSet] = []

        async def handler(event: Event) -> None:
            if isinstance(event, ObservedCacheSet):
                observed.append(event)

        await site.require_capability(EventsCapability).subscribe(EVT_SQL, handler)

        def cache_event(
            call: BoundArguments,
            outcome: EventOutcome,
            operation: str,
        ) -> ObservedCacheSet:
            assert call.arguments["owner"] == "template"
            assert outcome.succeeded
            assert operation == "set"
            return ObservedCacheSet(
                owner="template",
                duration_seconds=outcome.duration_seconds,
            )

        @observe(cache_event, "set", context="set")
        async def set_cache(owner: str) -> str:
            return owner

        try:
            assert await set_cache("template") == "template"
        finally:
            await site.close()

        assert observed[0].context is not None
        assert observed[0].context.segments == ("set",)

    @pytest.mark.anyio
    async def test_observe_skips_its_descriptor_when_delivery_is_disabled(
        self,
    ) -> None:
        site = await start(
            FastAPI(),
            config_source=MappingConfigSource(
                {
                    "app": {"modules": (), "deployment_environment": "local"},
                    "wybra.events": {"enabled": False},
                }
            ),
        )
        descriptor_called = False

        def descriptor(*_args: object) -> None:
            nonlocal descriptor_called
            descriptor_called = True
            return None

        @observe(descriptor)
        async def operation() -> str:
            return "completed"

        try:
            assert await operation() == "completed"
        finally:
            await site.close()

        assert not descriptor_called

    @pytest.mark.anyio
    async def test_observe_preserves_a_business_failure_when_its_descriptor_fails(
        self,
    ) -> None:
        site = await start(
            FastAPI(),
            config_source=MappingConfigSource(
                {
                    "app": {"modules": (), "deployment_environment": "local"},
                    "wybra.events": {"enabled": True},
                }
            ),
        )

        def failing_descriptor(*_args: object) -> None:
            raise RuntimeError("event construction failed")

        @observe(failing_descriptor)
        async def operation() -> None:
            raise LookupError("business failure")

        try:
            with pytest.raises(LookupError, match="business failure"):
                await operation()
        finally:
            await site.close()

    @pytest.mark.anyio
    async def test_core_events_capability_discards_events_when_disabled(self) -> None:
        site = await start(
            FastAPI(),
            config_source=MappingConfigSource(
                {
                    "app": {
                        "modules": (),
                        "deployment_environment": "local",
                    }
                }
            ),
        )
        try:
            dispatcher = site.require_capability(EventsCapability)
            observed: list[Event] = []

            async def handler(event: Event) -> None:
                observed.append(event)

            await dispatcher.subscribe(EVT_SQL, handler)

            await dispatcher.publish(
                TransactionStarted(topic=EVT_SQL(BEGIN), transaction_id="transaction-1")
            )
        finally:
            await site.close()

        assert observed == []

    @pytest.mark.anyio
    async def test_core_events_capability_dispatches_when_enabled(self) -> None:
        site = await start(
            FastAPI(),
            config_source=MappingConfigSource(
                {
                    "app": {
                        "modules": (),
                        "deployment_environment": "local",
                    },
                    "wybra.events": {"enabled": True},
                    "wybra.diagnostics": {"events_enabled": True},
                }
            ),
        )
        try:
            dispatcher = site.require_capability(EventsCapability)
            observed: list[Event] = []

            async def handler(event: Event) -> None:
                observed.append(event)

            await dispatcher.subscribe(EVT_SQL, handler)

            await dispatcher.publish(
                TransactionStarted(topic=EVT_SQL(BEGIN), transaction_id="transaction-1")
            )
        finally:
            await site.close()

        assert [str(event.scope) for event in observed] == ["sql.begin"]

    @pytest.mark.anyio
    async def test_diagnostics_projects_selected_events_passively(self) -> None:
        site = await start(
            FastAPI(),
            config_source=MappingConfigSource(
                {
                    "app": {
                        "modules": (),
                        "deployment_environment": "local",
                    },
                    "wybra.events": {"enabled": True},
                    "wybra.diagnostics": {
                        "events_enabled": True,
                        "event_scopes": "sql",
                        "level": "trace",
                    },
                }
            ),
        )
        try:
            dispatcher = site.require_capability(EventsCapability)
            diagnostics = site.require_capability(DiagnosticsCapability)

            async with diagnostic_context(
                diagnostics,
                kind="event_test",
                description="Passive event projection",
                level="trace",
            ):
                request_id = uuid7()
                token = _CURRENT_CONTEXT.set(EventContext(request_id=request_id))
                try:
                    await dispatcher.publish(
                        TransactionStarted(
                            topic=EVT_SQL(BEGIN), transaction_id="transaction-1"
                        )
                    )
                finally:
                    _CURRENT_CONTEXT.reset(token)
                await _drain_events(dispatcher)

            events = diagnostics.snapshots(EVT_SQL)[0].events
        finally:
            await site.close()

        assert [event["name"] for event in events] == ["begin"]
        attributes = cast(dict[str, object], events[0]["attributes"])
        assert attributes == {
            "event_context_request_id": str(request_id),
            "event_type": "test_event_dispatch.TransactionStarted",
        }

    @pytest.mark.anyio
    async def test_outer_middleware_publishes_request_lifecycle_events(self) -> None:
        app = FastAPI()

        @app.get("/events", name="events")
        async def events_endpoint() -> dict[str, bool]:
            return {"ok": True}

        site = await start(
            app,
            config_source=MappingConfigSource(
                {
                    "app": {"modules": (), "deployment_environment": "local"},
                    "wybra.events": {"enabled": True},
                }
            ),
        )
        observed: list[Event] = []

        async def handler(event: Event) -> None:
            observed.append(event)

        await site.require_capability(EventsCapability).subscribe(EVT_REQUEST, handler)
        try:
            with WybraTestClient(app) as client:
                response = client.get("/events")
        finally:
            await site.close()

        assert response.json() == {"ok": True}
        assert [str(event.scope) for event in observed] == [
            "request.started",
            "request.completed",
        ]
        completed = observed[-1]
        assert isinstance(completed, RequestCompletedEvent)
        assert completed.status_code == 200
        assert completed.route_name == "events"
        assert completed.error_type is None

    @pytest.mark.anyio
    async def test_request_failure_publishes_one_completion_without_swallowing_error(
        self,
    ) -> None:
        app = FastAPI()

        @app.get("/failure", name="failure")
        async def failure_endpoint() -> None:
            raise RuntimeError("expected failure")

        site = await start(
            app,
            config_source=MappingConfigSource(
                {
                    "app": {"modules": (), "deployment_environment": "local"},
                    "wybra.events": {"enabled": True},
                }
            ),
        )
        observed: list[Event] = []

        async def handler(event: Event) -> None:
            observed.append(event)

        await site.require_capability(EventsCapability).subscribe(EVT_REQUEST, handler)
        try:
            with WybraTestClient(app, raise_server_exceptions=False) as client:
                response = client.get("/failure")
        finally:
            await site.close()

        assert response.status_code == 500
        assert [str(event.scope) for event in observed] == [
            "request.started",
            "request.completed",
        ]
        completed = observed[-1]
        assert isinstance(completed, RequestCompletedEvent)
        assert completed.status_code is None
        assert completed.error_type == "RuntimeError"

    @pytest.mark.anyio
    async def test_cancelling_request_observer_cannot_cancel_response(self) -> None:
        app = FastAPI()

        @app.get("/events")
        async def events_endpoint() -> dict[str, bool]:
            return {"ok": True}

        site = await start(
            app,
            config_source=MappingConfigSource(
                {
                    "app": {"modules": (), "deployment_environment": "local"},
                    "wybra.events": {"enabled": True},
                }
            ),
        )

        async def cancelling_handler(event: Event) -> None:
            raise asyncio.CancelledError()

        await site.require_capability(EventsCapability).subscribe(
            EVT_REQUEST, cancelling_handler
        )
        try:
            with WybraTestClient(app) as client:
                response = client.get("/events")
        finally:
            await site.close()

        assert response.json() == {"ok": True}

    @pytest.mark.anyio
    async def test_request_events_do_not_consume_multipart_uploads(self) -> None:
        app = FastAPI()

        @app.post("/upload", name="upload")
        async def upload_endpoint(
            upload: Annotated[UploadFile, File()],
        ) -> dict[str, str]:
            return {"content": (await upload.read()).decode()}

        site = await start(
            app,
            config_source=MappingConfigSource(
                {
                    "app": {"modules": (), "deployment_environment": "local"},
                    "wybra.events": {"enabled": True},
                }
            ),
        )
        observed: list[Event] = []

        async def handler(event: Event) -> None:
            observed.append(event)

        await site.require_capability(EventsCapability).subscribe(EVT_REQUEST, handler)
        try:
            with WybraTestClient(app) as client:
                response = client.post(
                    "/upload",
                    files={"upload": ("message.txt", b"upload contents", "text/plain")},
                )
        finally:
            await site.close()

        assert response.json() == {"content": "upload contents"}
        assert [str(event.scope) for event in observed] == [
            "request.started",
            "request.completed",
        ]
        completed = observed[-1]
        assert isinstance(completed, RequestCompletedEvent)
        assert completed.route_name == "upload"

    @pytest.mark.anyio
    async def test_request_events_complete_after_a_streaming_response(self) -> None:
        app = FastAPI()

        @app.get("/stream", name="stream")
        async def stream_endpoint() -> StreamingResponse:
            async def content():
                yield b"first"
                yield b" second"

            return StreamingResponse(content(), media_type="text/plain")

        site = await start(
            app,
            config_source=MappingConfigSource(
                {
                    "app": {"modules": (), "deployment_environment": "local"},
                    "wybra.events": {"enabled": True},
                }
            ),
        )
        observed: list[Event] = []

        async def handler(event: Event) -> None:
            observed.append(event)

        await site.require_capability(EventsCapability).subscribe(EVT_REQUEST, handler)
        try:
            with WybraTestClient(app) as client:
                response = client.get("/stream")
        finally:
            await site.close()

        assert response.text == "first second"
        assert [str(event.scope) for event in observed] == [
            "request.started",
            "request.completed",
        ]
        completed = observed[-1]
        assert isinstance(completed, RequestCompletedEvent)
        assert completed.status_code == 200
        assert completed.route_name == "stream"

    @pytest.mark.anyio
    async def test_totp_rejection_publishes_a_credential_event(self) -> None:
        site = await start(
            FastAPI(),
            config_source=MappingConfigSource(
                {
                    "app": {"modules": (), "deployment_environment": "local"},
                    "wybra.events": {"enabled": True},
                }
            ),
        )
        observed: list[Event] = []

        async def handler(event: Event) -> None:
            observed.append(event)

        await site.require_capability(EventsCapability).subscribe(
            EVT_CREDENTIAL, handler
        )
        user_id = str(uuid7())
        try:
            result = await verify_totp_code_for_credential(
                store=RejectedTotpStore(),
                credential_id=str(uuid7()),
                user_id=user_id,
                code="123456",
                options=IdentityOptions(totp_mode="opt_in"),
            )
        finally:
            await site.close()

        assert result == (False, None, "invalid")
        assert len(observed) == 1
        event = observed[0]
        assert isinstance(event, CredentialAccessEvent)
        assert event.operation == "verify"
        assert event.provider == "totp"
        assert event.outcome == "rejected"
        assert event.user_id == user_id

    @pytest.mark.anyio
    async def test_request_events_wrap_existing_middleware(self) -> None:
        app = FastAPI()
        order: list[str] = []

        @app.middleware("http")
        async def existing_middleware(
            request: Request,
            call_next: Callable[[Request], Awaitable[Response]],
        ) -> Response:
            order.append(f"middleware.before:{_request_context_id()}")
            response = await call_next(request)
            order.append(f"middleware.after:{_request_context_id()}")
            return response

        @app.get("/ordered")
        async def ordered_endpoint() -> dict[str, bool]:
            order.append(f"endpoint:{_request_context_id()}")
            return {"ok": True}

        site = await start(
            app,
            config_source=MappingConfigSource(
                {
                    "app": {"modules": (), "deployment_environment": "local"},
                    "wybra.events": {"enabled": True},
                }
            ),
        )

        async def handler(event: Event) -> None:
            context = event.context
            assert context is not None
            order.append(f"event:{event.scope}:{context.request_id}")

        await site.require_capability(EventsCapability).subscribe(EVT_REQUEST, handler)
        try:
            with WybraTestClient(app) as client:
                response = client.get("/ordered")
        finally:
            await site.close()

        assert response.json() == {"ok": True}
        assert [entry.split(":", maxsplit=1)[0] for entry in order] == [
            "middleware.before",
            "endpoint",
            "middleware.after",
            "event",
            "event",
        ]
        request_ids = [entry.rsplit(":", maxsplit=1)[1] for entry in order]
        assert len(set(request_ids)) == 1

    @pytest.mark.anyio
    async def test_class_based_view_dispatch_publishes_route_and_outcome(self) -> None:
        app = FastAPI()
        router = ViewRouter()

        @router.view("/observed", name="observed")
        class ObservedView(View):
            async def get(self, _request: Request) -> dict[str, bool]:
                return {"ok": True}

        app.include_router(router)
        site = await start(
            app,
            config_source=MappingConfigSource(
                {
                    "app": {"modules": (), "deployment_environment": "local"},
                    "wybra.events": {"enabled": True},
                }
            ),
        )
        observed: list[Event] = []

        async def handler(event: Event) -> None:
            observed.append(event)

        events = site.require_capability(EventsCapability)
        await events.subscribe(EVT_ROUTE, handler)
        await events.subscribe(EVT_VIEW, handler)
        try:
            with WybraTestClient(app) as client:
                response = client.get("/observed")
        finally:
            await site.close()

        assert response.json() == {"ok": True}
        assert [str(event.scope) for event in observed] == [
            "route.dispatch",
            "view.completed",
        ]
        route_event, view_event = observed
        assert isinstance(route_event, RouteDispatchedEvent)
        assert route_event.method == "GET"
        assert route_event.route_name == "observed"
        assert route_event.view_type == "ObservedView"
        assert isinstance(view_event, ViewCompletedEvent)
        assert view_event.status_code == 200
        assert view_event.error_type is None

    @pytest.mark.anyio
    async def test_class_based_view_error_is_not_changed_by_observation(self) -> None:
        app = FastAPI()
        router = ViewRouter()

        @router.view("/failing", name="failing")
        class FailingView(View):
            async def get(self, _request: Request) -> None:
                raise RuntimeError("expected failure")

        app.include_router(router)
        site = await start(
            app,
            config_source=MappingConfigSource(
                {
                    "app": {"modules": (), "deployment_environment": "local"},
                    "wybra.events": {"enabled": True},
                }
            ),
        )
        observed: list[Event] = []

        async def handler(event: Event) -> None:
            observed.append(event)

        await site.require_capability(EventsCapability).subscribe(EVT_VIEW, handler)
        try:
            with WybraTestClient(app, raise_server_exceptions=False) as client:
                response = client.get("/failing")
        finally:
            await site.close()

        assert response.status_code == 500
        assert len(observed) == 1
        event = observed[0]
        assert isinstance(event, ViewCompletedEvent)
        assert event.status_code is None
        assert event.error_type == "RuntimeError"

    @pytest.mark.anyio
    async def test_template_rendering_publishes_success_and_failure(
        self,
        tmp_path: Path,
    ) -> None:
        (tmp_path / "page.html").write_text("Hello {{ name }}", encoding="utf-8")
        observed: list[Event] = []

        async def handler(event: Event) -> None:
            observed.append(event)

        site = await start(
            FastAPI(),
            config_source=MappingConfigSource(
                {
                    "app": {"modules": (), "deployment_environment": "local"},
                    "wybra.events": {"enabled": True},
                }
            ),
        )
        try:
            await site.require_capability(EventsCapability).subscribe(
                EVT_TEMPLATE, handler
            )
            templates = DefaultTemplateCapability(template_root=tmp_path)

            assert await templates.render_template("page.html", {"name": "Wybra"}) == (
                "Hello Wybra"
            )
            with pytest.raises(TemplateNotFound):
                await templates.render_template(
                    "missing.html", {"secret": "not exposed"}
                )
        finally:
            await site.close()

        assert [str(event.scope) for event in observed] == [
            "template.render.completed",
            "template.render.completed",
        ]
        succeeded, failed = observed
        assert isinstance(succeeded, TemplateRenderCompletedEvent)
        assert succeeded.template_name == "page.html"
        assert succeeded.error_type is None
        assert isinstance(failed, TemplateRenderCompletedEvent)
        assert failed.template_name == "missing.html"
        assert failed.error_type == "TemplateNotFound"

    @pytest.mark.anyio
    async def test_form_validation_publishes_no_submitted_values(self) -> None:
        observed: list[Event] = []

        async def handler(event: Event) -> None:
            observed.append(event)

        site = await start(
            FastAPI(),
            config_source=MappingConfigSource(
                {
                    "app": {"modules": (), "deployment_environment": "local"},
                    "wybra.events": {"enabled": True},
                }
            ),
        )
        try:
            await site.require_capability(EventsCapability).subscribe(EVT_FORM, handler)
            form = ObservedForm()
            result = await form.parse({"title": ""})
        finally:
            await site.close()

        assert result.is_valid is False
        assert len(observed) == 1
        event = observed[0]
        assert isinstance(event, FormValidationCompletedEvent)
        assert str(event.scope) == "form.validation.completed"
        assert event.field_count == 1
        assert event.invalid_field_count == 1
        assert event.valid is False
        assert not hasattr(event, "values")
        assert not hasattr(event, "errors")

    @pytest.mark.anyio
    async def test_account_lifecycle_masks_email_and_retains_user_uuid(self) -> None:
        app = FastAPI()
        site = await start(
            app,
            config_source=MappingConfigSource(
                {
                    "app": {"modules": (), "deployment_environment": "local"},
                    "wybra.events": {"enabled": True},
                }
            ),
        )
        observed: list[Event] = []

        async def handler(event: Event) -> None:
            observed.append(event)

        await site.require_capability(EventsCapability).subscribe(EVT_ACCOUNT, handler)
        request = Request(
            {
                "type": "http",
                "method": "POST",
                "path": "/signup",
                "headers": [],
                "app": app,
            }
        )
        try:
            await publish_account_lifecycle(
                request,
                operation="signup",
                outcome="succeeded",
                user_id="018f261d-0000-7000-8000-000000000001",
                email="username@example.com",
            )
        finally:
            await site.close()

        assert len(observed) == 1
        event = observed[0]
        assert isinstance(event, AccountLifecycleEvent)
        assert event.user_id == "018f261d-0000-7000-8000-000000000001"
        assert event.masked_email == "u**@example**"
        assert "username@example.com" not in repr(event)

    @pytest.mark.anyio
    async def test_capability_proxy_resolution_publishes_available_and_unavailable(
        self,
    ) -> None:
        site = await start(
            FastAPI(),
            config_source=MappingConfigSource(
                {
                    "app": {"modules": (), "deployment_environment": "local"},
                    "wybra.events": {"enabled": True},
                }
            ),
        )
        observed: list[Event] = []

        async def handler(event: Event) -> None:
            observed.append(event)

        await site.require_capability(EventsCapability).subscribe(
            EVT_SITE(CAPABILITY), handler
        )
        available_proxy = site.capability_proxy(ProxyTestCapability)
        unavailable_proxy = site.capability_proxy(UnavailableProxyCapability)
        capability = ProxyTestCapability()
        site.provide_capability(ProxyTestCapability, capability)
        try:
            assert await available_proxy.require() is capability
            assert await unavailable_proxy.optional() is None
        finally:
            await site.close()

        assert [str(event.scope) for event in observed] == [
            "site.capability.resolved",
            "site.capability.unavailable",
        ]
        assert isinstance(observed[0], CapabilityResolvedEvent)
        assert isinstance(observed[1], CapabilityUnavailableEvent)

    @pytest.mark.anyio
    async def test_module_setup_can_register_an_event_handler(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        observed: list[str] = []
        handler_module = ModuleType("test_event_handler_module")

        async def setup_handler_module(site: Site) -> None:
            capability = site.require_capability(EventsCapability)

            async def handler(event: Event) -> None:
                observed.append(str(event.scope))

            await capability.subscribe(EVT_SQL, handler)

        handler_module.__dict__["setup_site"] = setup_handler_module
        monkeypatch.setitem(sys.modules, handler_module.__name__, handler_module)

        def module_loader(name: str) -> ModuleType:
            if name == handler_module.__name__:
                return handler_module
            return import_module(name)

        site = await start(
            FastAPI(),
            config_source=MappingConfigSource(
                {
                    "app": {
                        "modules": (handler_module.__name__,),
                        "deployment_environment": "local",
                    },
                    "wybra.events": {"enabled": True},
                }
            ),
            module_loader=module_loader,
        )
        try:
            dispatcher = site.require_capability(EventsCapability)

            await dispatcher.publish(
                TransactionStarted(topic=EVT_SQL(BEGIN), transaction_id="transaction-1")
            )
        finally:
            await site.close()

        assert observed == ["sql.begin"]

    @pytest.mark.anyio
    async def test_module_hooks_publish_setup_and_post_setup_outcomes(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        observed: list[tuple[str, str, str, str]] = []
        observer_module = ModuleType("event_observer_module")
        subject_module = ModuleType("event_subject_module")

        async def setup_observer(site: Site) -> None:
            dispatcher = site.require_capability(EventsCapability)

            async def handler(event: Event) -> None:
                if not isinstance(
                    event,
                    ModuleSetupEvent
                    | ModuleSetupFinalisationEvent
                    | ModulePostSetupEvent,
                ):
                    return
                observed.append(
                    (
                        str(event.scope),
                        type(event).__name__,
                        event.module,
                        event.outcome,
                    )
                )

            await dispatcher.subscribe(EVT_SITE, handler)

        async def setup_subject(site: Site) -> None:
            async def finalise() -> None:
                return None

            site.defer_setup_finalisation(finalise)

        async def post_setup_subject(_site: Site) -> None:
            return None

        observer_module.__dict__["setup_site"] = setup_observer
        subject_module.__dict__["setup_site"] = setup_subject
        subject_module.__dict__["post_setup_site"] = post_setup_subject
        modules = {
            observer_module.__name__: observer_module,
            subject_module.__name__: subject_module,
        }
        for module_name, module in modules.items():
            monkeypatch.setitem(sys.modules, module_name, module)

        site = await start(
            FastAPI(),
            config_source=MappingConfigSource(
                {
                    "app": {
                        "modules": tuple(modules),
                        "deployment_environment": "local",
                    },
                    "wybra.events": {"enabled": True},
                    "wybra.diagnostics": {"events_enabled": True},
                }
            ),
            module_loader=modules.__getitem__,
        )
        await site.close()

        assert observed == [
            (
                "site.module.setup",
                "ModuleSetupEvent",
                observer_module.__name__,
                "succeeded",
            ),
            (
                "site.module.setup",
                "ModuleSetupEvent",
                subject_module.__name__,
                "started",
            ),
            (
                "site.module.setup",
                "ModuleSetupEvent",
                subject_module.__name__,
                "succeeded",
            ),
            (
                "site.module.setup_finalisation",
                "ModuleSetupFinalisationEvent",
                subject_module.__name__,
                "started",
            ),
            (
                "site.module.setup_finalisation",
                "ModuleSetupFinalisationEvent",
                subject_module.__name__,
                "succeeded",
            ),
            (
                "site.module.post_setup",
                "ModulePostSetupEvent",
                subject_module.__name__,
                "started",
            ),
            (
                "site.module.post_setup",
                "ModulePostSetupEvent",
                subject_module.__name__,
                "succeeded",
            ),
        ]

    @pytest.mark.anyio
    async def test_module_hook_failure_publishes_before_startup_fails(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        observed: list[tuple[str, str, str | None]] = []
        observer_module = ModuleType("failing_event_observer_module")
        failing_module = ModuleType("failing_event_subject_module")

        async def setup_observer(site: Site) -> None:
            dispatcher = site.require_capability(EventsCapability)

            async def handler(event: Event) -> None:
                if not isinstance(event, ModuleSetupEvent | ModulePostSetupEvent):
                    return
                observed.append(
                    (
                        str(event.scope),
                        event.outcome,
                        event.error_type,
                    )
                )

            await dispatcher.subscribe(EVT_SITE, handler)

        async def setup_failing(_site: Site) -> None:
            raise RuntimeError("expected failure")

        observer_module.__dict__["setup_site"] = setup_observer
        failing_module.__dict__["setup_site"] = setup_failing
        modules = {
            observer_module.__name__: observer_module,
            failing_module.__name__: failing_module,
        }
        for module_name, module in modules.items():
            monkeypatch.setitem(sys.modules, module_name, module)

        with pytest.raises(SiteCapabilityError, match="error_type=RuntimeError"):
            await start(
                FastAPI(),
                config_source=MappingConfigSource(
                    {
                        "app": {
                            "modules": tuple(modules),
                            "deployment_environment": "local",
                        },
                        "wybra.events": {"enabled": True},
                    }
                ),
                module_loader=modules.__getitem__,
            )

        assert observed[-2:] == [
            ("site.module.setup", "started", None),
            ("site.module.setup", "failed", "RuntimeError"),
        ]

    @pytest.mark.anyio
    async def test_site_lifecycle_and_capability_setup_events(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        observed: list[Event] = []
        observer_module = ModuleType("site_lifecycle_event_observer")
        capability_module = ModuleType("site_lifecycle_event_capability")

        async def setup_observer(site: Site) -> None:
            async def handler(event: Event) -> None:
                observed.append(event)

            await site.require_capability(EventsCapability).subscribe(EVT_SITE, handler)

        async def setup_capability(site: Site) -> None:
            site.provide_capability(ProxyTestCapability, ProxyTestCapability())

        observer_module.__dict__["setup_site"] = setup_observer
        capability_module.__dict__["setup_site"] = setup_capability
        modules = {
            observer_module.__name__: observer_module,
            capability_module.__name__: capability_module,
        }
        for module_name, module in modules.items():
            monkeypatch.setitem(sys.modules, module_name, module)

        site = await start(
            FastAPI(),
            config_source=MappingConfigSource(
                {
                    "app": {
                        "modules": tuple(modules),
                        "deployment_environment": "local",
                    },
                    "wybra.events": {"enabled": True},
                    "wybra.diagnostics": {"events_enabled": True},
                }
            ),
            module_loader=modules.__getitem__,
        )
        await site.close()

        assert any(
            isinstance(event, CapabilityProvidedEvent)
            and event.capability_type.endswith("ProxyTestCapability")
            for event in observed
        )
        assert any(
            isinstance(event, CapabilityProvidedEvent)
            and event.capability_type.endswith("DiagnosticsCapability")
            for event in observed
        )
        assert [
            (event.phase, event.error_count)
            for event in observed
            if isinstance(event, SiteLifecycleEvent)
        ] == [("startup", 0), ("shutdown", 0)]

    @pytest.mark.anyio
    async def test_cancelling_shutdown_observer_cannot_abort_site_close(self) -> None:
        site = await start(
            FastAPI(),
            config_source=MappingConfigSource(
                {
                    "app": {"modules": (), "deployment_environment": "local"},
                    "wybra.events": {"enabled": True},
                }
            ),
        )

        async def cancelling_handler(event: Event) -> None:
            if isinstance(event, SiteLifecycleEvent) and event.phase == "shutdown":
                raise asyncio.CancelledError()

        await site.require_capability(EventsCapability).subscribe(
            EVT_SITE, cancelling_handler
        )

        await site.close()

        assert not site.has_capability(EventsCapability)

    @pytest.mark.anyio
    async def test_database_request_chain_reaches_events_without_diagnostics(
        self,
    ) -> None:
        app = FastAPI()
        site = await start(
            app,
            config_source=MappingConfigSource(
                {
                    "app": {"modules": (), "deployment_environment": "local"},
                    "wybra.events": {"enabled": True},
                }
            ),
        )
        events = site.require_capability(EventsCapability)
        observed: list[Event] = []

        async def handler(event: Event) -> None:
            observed.append(event)

        await events.subscribe(EVT_REQUEST, handler)
        await events.subscribe(EVT_SQL, handler)
        database = await create_database(
            "sqlite://:memory:",
            modules=("wybra.sessions",),
        )

        @app.get("/database")
        async def database_route() -> dict[str, int]:
            with database.context:
                await database.connection().execute_query("select 1")
            return {"status": 1}

        try:
            async with WybraTestClient(app) as client:
                response = await client.get("/database")
        finally:
            await close_database(database)
            await site.close()

        assert response.json() == {"status": 1}
        scopes = [str(event.scope) for event in observed]
        assert scopes.index("request.started") < scopes.index("sql.statement")
        assert scopes.index("sql.statement") < scopes.index("request.completed")
        assert any(isinstance(event, DatabaseStatementEvent) for event in observed)
        request_ids = {
            event.context.request_id
            for event in observed
            if event.context and event.context.request_id
        }
        assert len(request_ids) == 1
        assert next(iter(request_ids)).version == 7
