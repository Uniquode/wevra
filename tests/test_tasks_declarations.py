from __future__ import annotations

from dataclasses import dataclass

import pytest

import wybra.tasks
from wybra.tasks import (
    RetryPolicy,
    TaskDeclarationError,
    TaskPayload,
    TaskPayloadError,
    TaskRegistry,
    current_task_context,
    task,
)


def test_task_package_exposes_public_declaration_contracts() -> None:
    assert {
        "RetryPolicy",
        "TaskDeclarationError",
        "TaskDefinition",
        "TaskExecutionContext",
        "TaskIdentity",
        "TaskPayload",
        "TaskPayloadError",
        "TaskRegistrationError",
        "TaskRegistry",
        "current_task_context",
        "task",
    } <= set(wybra.tasks.__all__)


def test_task_declaration_uses_explicit_identity() -> None:
    registry = TaskRegistry()

    @task(name="messages.send", version=2, registry=registry)
    async def send_message(recipient: str) -> str:
        return recipient

    assert send_message.identity.name == "messages.send"
    assert send_message.identity.version == 2
    assert registry.require(send_message.identity) is send_message


def test_task_declaration_derives_identity_from_function() -> None:
    registry = TaskRegistry()

    @task(registry=registry)
    async def rebuild_index() -> None:
        return None

    assert rebuild_index.identity.name.endswith(".rebuild_index")
    assert rebuild_index.identity.version == 1


def test_task_declaration_rejects_synchronous_function() -> None:
    registry = TaskRegistry()

    with pytest.raises(TaskDeclarationError, match="async function"):

        @task(registry=registry)
        def synchronous_task() -> None:
            return None


def test_task_declaration_rejects_variadic_positional_parameters() -> None:
    registry = TaskRegistry()

    with pytest.raises(TaskDeclarationError, match="variadic"):

        @task(registry=registry)
        async def variadic_positional(*values: int) -> None:
            del values


def test_task_declaration_rejects_variadic_keyword_parameters() -> None:
    registry = TaskRegistry()

    with pytest.raises(TaskDeclarationError, match="variadic"):

        @task(registry=registry)
        async def variadic_keyword(**values: int) -> None:
            del values


@pytest.mark.parametrize("version", (0, -1, True))
def test_task_declaration_rejects_invalid_schema_version(version: int) -> None:
    registry = TaskRegistry()

    with pytest.raises(TaskDeclarationError, match="positive integer"):

        @task(version=version, registry=registry)
        async def invalid_version() -> None:
            return None


def test_task_identity_rejects_non_string_name() -> None:
    with pytest.raises(TaskDeclarationError, match="dotted Python identifier"):
        wybra.tasks.TaskIdentity(name=123)  # type: ignore[arg-type]


def test_task_registry_rejects_duplicate_identity() -> None:
    registry = TaskRegistry()

    @task(name="messages.send", registry=registry)
    async def first() -> None:
        return None

    with pytest.raises(
        wybra.tasks.TaskRegistrationError,
        match="already registered",
    ):

        @task(name="messages.send", registry=registry)
        async def second() -> None:
            return None

    assert registry.require(first.identity) is first


def test_task_payload_validates_and_serialises_arguments() -> None:
    registry = TaskRegistry()

    @task(registry=registry)
    async def resize(width: int, *, format_name: str = "webp") -> None:
        return None

    payload = resize.payload("120", format_name="avif")

    assert payload.arguments == {"width": 120, "format_name": "avif"}
    assert payload.to_json() == b'{"format_name":"avif","width":120}'


@dataclass
class UnsupportedPayload:
    callback: object


def test_task_payload_rejects_non_serialisable_arguments() -> None:
    registry = TaskRegistry()

    @task(registry=registry)
    async def unsupported(value: object) -> None:
        return None

    with pytest.raises(TaskPayloadError, match="JSON-compatible"):
        unsupported.payload(UnsupportedPayload(callback=lambda: None))


def test_task_payload_does_not_retain_mutable_argument_mappings() -> None:
    arguments: dict[str, object] = {"value": "safe"}
    payload = TaskPayload(arguments)

    arguments["value"] = "changed"
    payload.arguments["value"] = "also changed"

    assert payload.arguments == {"value": "safe"}
    assert payload.to_json() == b'{"value":"safe"}'


@pytest.mark.parametrize("max_attempts", (True, 1.5))
def test_retry_policy_rejects_non_integer_attempts(max_attempts: object) -> None:
    with pytest.raises(TaskDeclarationError, match="positive integer"):
        RetryPolicy(max_attempts=max_attempts)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("field_name", "value"),
    (
        ("initial_delay_seconds", float("nan")),
        ("backoff_multiplier", float("inf")),
        ("maximum_delay_seconds", float("-inf")),
        ("jitter_seconds", float("nan")),
    ),
)
def test_retry_policy_rejects_non_finite_values(
    field_name: str,
    value: float,
) -> None:
    with pytest.raises(TaskDeclarationError, match="finite"):
        RetryPolicy(**{field_name: value})  # type: ignore[arg-type]


@pytest.mark.anyio
async def test_task_runs_directly_without_site_or_capability() -> None:
    registry = TaskRegistry()

    @task(registry=registry)
    async def total(left: int, right: int) -> int:
        return left + right

    assert await total.run("2", 3) == 5


@pytest.mark.anyio
async def test_direct_task_preserves_original_exception() -> None:
    registry = TaskRegistry()
    failure = RuntimeError("failed")

    @task(registry=registry)
    async def fail() -> None:
        raise failure

    with pytest.raises(RuntimeError) as caught:
        await fail.run()

    assert caught.value is failure


@pytest.mark.anyio
async def test_direct_task_exposes_execution_context() -> None:
    registry = TaskRegistry()

    @task(name="context.inspect", version=3, registry=registry)
    async def inspect_context() -> tuple[str, int, int]:
        context = current_task_context()
        assert context is not None
        return context.task_name, context.schema_version, context.attempt

    assert await inspect_context.run() == ("context.inspect", 3, 1)
    assert current_task_context() is None


@pytest.mark.anyio
async def test_nested_direct_task_inherits_correlation_and_records_causation() -> None:
    registry = TaskRegistry()

    @task(name="context.child", registry=registry)
    async def child() -> tuple[object, object]:
        context = current_task_context()
        assert context is not None
        return context.correlation_id, context.causation_id

    @task(name="context.parent", registry=registry)
    async def parent() -> tuple[object, object, object]:
        parent_context = current_task_context()
        assert parent_context is not None
        child_correlation, child_causation = await child.run()
        return parent_context.task_id, child_correlation, child_causation

    parent_id, child_correlation, child_causation = await parent.run()

    assert child_correlation == parent_id
    assert child_causation == parent_id
