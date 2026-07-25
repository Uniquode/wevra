from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import ClassVar, cast

import pytest

import wybra.events
from wybra.events._core import (
    BEGIN,
    EVT_SQL,
    MODEL,
    SQL_STATEMENT,
    TRANSACTION,
    Event,
    EventContext,
    EventScopeError,
    context,
    current_context,
    parse_event_scopes,
)


@dataclass(frozen=True, slots=True)
class TransactionStarted(Event):
    kind: ClassVar = BEGIN
    transaction_id: str


@dataclass(frozen=True, slots=True)
class MissingKind(Event):
    identifier: str


def test_root_event_package_exposes_core_primitives_and_subscription_selectors() -> (
    None
):
    assert wybra.events.__all__ == (
        "Event",
        "EventScope",
        "EventsCapability",
        "available_event_scopes",
        "context",
        "event_delivery_enabled",
        "event_scope",
        "event_segment",
        "observe",
    )


def test_event_scope_creates_a_chainable_topic() -> None:
    topic = EVT_SQL(SQL_STATEMENT, MODEL, "article")

    assert str(topic) == "sql.statement.model.article"
    assert topic.matches(EVT_SQL)
    assert not EVT_SQL.matches(topic)


@pytest.mark.parametrize("value", ("sql.*", "sql..statement", "unknown"))
def test_event_scope_parser_rejects_invalid_or_unknown_selectors(value: str) -> None:
    with pytest.raises(EventScopeError):
        parse_event_scopes(value)


def test_event_scope_parser_accepts_a_comma_separated_selector_list() -> None:
    assert tuple(map(str, parse_event_scopes("sql, template.render"))) == (
        "sql",
        "template.render",
    )


def test_observe_rejects_synchronous_callables() -> None:
    def descriptor(*_arguments: object) -> None:
        return None

    def synchronous_operation() -> None:
        return None

    with pytest.raises(TypeError, match="Observed functions must be async callables"):
        wybra.events.observe(descriptor)(
            cast(Callable[[], Awaitable[None]], synchronous_operation)
        )


@pytest.mark.anyio
def test_event_snapshots_an_explicit_topic_and_occurrence_time() -> None:
    event = TransactionStarted(
        topic=EVT_SQL(TRANSACTION, BEGIN),
        transaction_id="transaction-1",
    )

    assert event.scope == EVT_SQL(TRANSACTION, BEGIN)
    assert event.occurred_at > 0


def test_event_requires_an_explicit_topic_or_declared_scope() -> None:
    with pytest.raises(EventScopeError, match="explicit event topic"):
        TransactionStarted(transaction_id="transaction-1")


def test_event_requires_a_declared_topic() -> None:
    with pytest.raises(EventScopeError, match="explicit event topic"):
        MissingKind(identifier="missing-kind")


@pytest.mark.anyio
async def test_context_decorator_derives_and_restores_immutable_context() -> None:
    @context("outer")
    async def outer() -> tuple[EventContext, EventContext, EventContext]:
        before = current_context()
        assert before is not None

        @context(["inner", "wait"])
        async def inner() -> EventContext:
            context_value = current_context()
            assert context_value is not None
            return context_value

        during = await inner()
        restored = current_context()
        assert restored is not None
        return before, during, restored

    before, during, restored = await outer()

    assert before.segments == ("outer",)
    assert during.segments == ("outer", "inner", "wait")
    assert restored == before
    assert current_context() is None


@pytest.mark.anyio
async def test_context_is_inherited_by_child_tasks_without_leaking_to_the_caller() -> (
    None
):
    @context(["request", "operation"])
    async def create_child() -> EventContext:
        child = asyncio.create_task(_current_context())
        return await child

    observed = await create_child()

    assert observed.segments == ("request", "operation")
    assert current_context() is None


async def _current_context() -> EventContext:
    context_value = current_context()
    assert context_value is not None
    return context_value
