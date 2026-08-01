# Tasks

`wybra.tasks` provides provider-neutral declarations for asynchronous
application work. A task is an ordinary async function with a stable identity,
validated JSON-compatible arguments, optional retry policy, execution context,
and lifecycle status.

Task declarations do not require task infrastructure. Applications can run a
declared task directly, or enable the optional task module and submit it through
`TasksCapability`.

## Execution modes

Wybra's task platform covers three application use cases:

| Use case | Current availability | Behaviour |
| --- | --- | --- |
| Direct task call | Available | Runs in the caller, returns the function result, and propagates the original exception. |
| On-demand submission | Available with the immediate backend | Uses task validation, retry, lifecycle, status, correlation, and idempotency metadata, but executes inline in the submitting process. |
| Durable background submission | Planned | Will publish through a cache-backed provider and return before worker execution. |
| One-time deferred task | Planned | Will submit one ordinary task command for a validated future time. |
| Recurring interval or cron task | Planned | Will be discovered declaratively and published by a separately operated scheduler. |

The immediate backend is useful for development, tests, simple deployments,
and adopting the task API before worker infrastructure is available. It is not
a background thread or process: submission waits for execution and retries to
finish.

Durable background workers, one-time deferred submission, recurring schedules,
and the scheduler command are not yet available. Do not use `asyncio.create_task`
or an in-process timer as a durability substitute; those operations are lost
when the web process exits.

## Enable the task capability

Add `wybra.tasks` to the host application's configured modules:

```toml
[app]
modules = [
  "wybra.tasks",
  "example.application",
]
```

That is the complete minimum configuration. The `[tasks]` section and every
directive within it are optional. When omitted, the module uses the immediate
backend with its built-in defaults.

`"wybra.tasks"` in `[app].modules` makes the module available, and
`[tasks].enabled` controls whether it registers a concrete capability:

- include `"wybra.tasks"` with the default `enabled = true` to provide a
  concrete `TasksCapability`, allowing capability proxies to resolve it;
- set `enabled = false`, or omit `"wybra.tasks"`, to provide no concrete
  capability. A proxy can still be created, but it resolves as unavailable.

Omitting only the `[tasks]` section does not disable submission when the module
is still configured; it selects the immediate defaults. Enabling the module
does not execute tasks automatically. A task runs only when application code
calls `run()`, `submit()`, or `dispatch()`.

Configure `[tasks]` only when overriding those defaults:

```toml
[tasks]
enabled = true
backend = "immediate"
default_queue = "default"
max_attempts = 3
initial_delay_seconds = 0.25
backoff_multiplier = 2.0
maximum_delay_seconds = 10.0
jitter_seconds = 0.5
status_retention_seconds = 3600
```

`backend = "immediate"` is the default and the only execution backend currently
available. The module registers `TasksCapability` during site composition.
Without `wybra.tasks` in `[app].modules`, the capability is absent but declared
tasks remain directly executable. The same is true when the module is present
with `enabled = false`.

### Taskiq preflight

Install the optional dependency before selecting the future Taskiq backend:

```sh
uv add 'wybra[tasks]'
```

Taskiq configuration selects a named provider-neutral cache. It intentionally
performs startup preflight only in the current release; it does not register a
`TasksCapability`, broker, result backend, worker, or scheduler yet.

```toml
[app]
modules = [
  "wybra.cache",
  "wybra.tasks",
  "example.application",
]

[tasks]
backend = "taskiq"
cache_name = "default"
```

`cache_name` defaults to `"default"`; its cache-name format is validated only
for the Taskiq backend, while the immediate backend ignores it. The selected
name must correspond to `[cache]` for `default`, or to a configured named cache
such as `[cache.task_work]` when `cache_name = "task_work"`. Taskiq preflight
runs after cache setup finalisation and checks that `wybra.cache` provides the
selected cache. It then stops startup with an explicit message that no
cache-backed Taskiq broker is available yet. This protects applications from
silently falling back to inline execution while durable task integration is
incomplete.

### Cache-backed Taskiq results

`CacheTaskiqResultBackend` is the reusable Taskiq adapter for retaining result
envelopes through a selected cache's baseline byte-value capability. It is not
activated by `[tasks]` configuration yet; a future broker runtime will create
and register it.

The runtime supplies an immutable `TaskiqResultPolicy` with a positive result
retention period, maximum serialised byte size, and encoder/decoder callables.
When either codec is `None`, the policy uses Taskiq's standard JSON model codec.
The encoder is responsible for producing a safe byte representation and runs
before any cache write. Encoder, decoder, and cache-operation failures
propagate to Taskiq; the adapter does not redact, wrap, or silently retain
unsafe return values. This keeps the safe-result and redaction policy owned by
the task runtime rather than by a particular cache provider.

Readiness uses the baseline cache read operation, so each wait poll reads the
stored result envelope. Configure the result byte limit and future worker poll
interval together to control result-wait cache traffic.

The retry settings above become site defaults for tasks that do not declare
their own `RetryPolicy`. Terminal immediate-task status and lifecycle history
remain available for `status_retention_seconds`; each task's visible lifecycle
history is also bounded.

Task declaration modules must be imported during normal application
composition so their stable identities are registered. Keep declarations in
application-owned modules rather than importing them dynamically from task
messages.

## Declare an application task

Use explicit, stable names and positive schema versions:

```python
from wybra.tasks import RetryPolicy, current_task_context, task


@task(
    name="example.search.rebuild",
    version=1,
    retry=RetryPolicy(
        max_attempts=4,
        initial_delay_seconds=0.5,
        backoff_multiplier=2.0,
        maximum_delay_seconds=15.0,
        jitter_seconds=0.25,
    ),
)
async def rebuild_search(*, section: str, batch_size: int = 500) -> None:
    context = current_task_context()
    if context is None:
        raise RuntimeError("Task execution context is unavailable.")

    await rebuild_section(
        section=section,
        batch_size=batch_size,
        operation_id=context.task_id,
    )
```

Task arguments are validated from the function annotations and canonicalised
as JSON. Values may be `None`, booleans, finite numbers, strings, mappings with
string keys, or lists containing those values. A task payload never serialises
the Python function.

Use explicit named parameters. Ordinary positional-or-keyword and keyword-only
parameters are supported. Variadic `*args` and `**kwargs` declarations are not
supported because their payload shape is not a stable named schema.

Use the schema version when changing the meaning or compatibility of a task's
payload. A worker resolves the stable name and version from its local registry;
task messages never dynamically import or execute arbitrary callables.

## Run directly

Call `run()` when the operation belongs in the current request or command and
does not need task lifecycle handling:

```python
await rebuild_search.run(section="articles", batch_size=250)
```

Direct execution:

- validates arguments;
- creates task, correlation, and causation context;
- returns the task function's result;
- does not require `Site` or `TasksCapability`;
- does not retry; and
- propagates the original exception.

Nested direct tasks inherit their parent's correlation identifier and record
the parent task as their cause.

## Submit an on-demand task

Resolve the capability from the current application and submit a validated
payload:

```python
from fastapi import APIRouter, Request, status

from wybra import get_site
from wybra.tasks import TaskSubmissionOptions, TasksCapability

from example.tasks import rebuild_search

router = APIRouter()


@router.post("/search/rebuild", status_code=status.HTTP_202_ACCEPTED)
async def request_search_rebuild(request: Request) -> dict[str, str]:
    tasks = get_site(request.app).require_capability(TasksCapability)
    handle = await tasks.submit(
        rebuild_search,
        rebuild_search.payload(section="articles", batch_size=250),
        options=TaskSubmissionOptions(
            queue="maintenance",
            idempotency_key="search-rebuild:articles",
        ),
    )
    return {
        "task_id": str(handle.task_id),
        "task_name": handle.identity.name,
    }
```

With the immediate backend, the example returns only after the task has
succeeded or exhausted its retries. The HTTP `202` shape remains useful when
moving the same declaration to a future durable backend, but it does not make
immediate execution concurrent.

`TaskSubmissionOptions` supplies a queue override and optional idempotency key.
Both values are trimmed and must be non-empty strings when supplied.
The immediate backend exposes the idempotency key to the handler but does not
deduplicate submissions. Durable providers may suppress a known duplicate
command, but no provider can guarantee exactly-once external side effects.

## Select a dispatch policy

`dispatch()` makes an application's fallback decision explicit:

```python
from wybra import get_site
from wybra.tasks import TaskDispatchPolicy, TaskSubmissionOptions, dispatch


site = get_site(request.app)
result_or_handle = await dispatch(
    site,
    rebuild_search,
    rebuild_search.payload(section="articles"),
    policy=TaskDispatchPolicy.PREFER_BACKGROUND,
    options=TaskSubmissionOptions(
        queue="maintenance",
        idempotency_key="search-rebuild:articles",
    ),
)
```

- `DIRECT` always calls the task directly.
- `BACKGROUND` requires `TasksCapability` and submits through it.
- `PREFER_BACKGROUND` submits when the capability exists, otherwise it runs
  directly.

Submission options are forwarded whenever `BACKGROUND` or `PREFER_BACKGROUND`
submits through an available capability. They cannot be honoured by direct
execution, so supplying them with `DIRECT`, or with a `PREFER_BACKGROUND` call
that needs direct fallback, raises `ValueError` instead of silently discarding
the queue or idempotency metadata.

`PREFER_BACKGROUND` falls back only when the capability is absent. If a
configured provider accepts the submission path and then fails, the error
propagates. Wybra never silently runs the task directly after a submission
failure because doing so could duplicate side effects.

## Report progress

Submitted task handlers report progress through their current execution
context:

```python
from wybra.tasks import current_task_context, task


@task(name="example.search.rebuild_batches")
async def rebuild_batches(batch_ids: list[str]) -> None:
    context = current_task_context()
    if context is None:
        raise RuntimeError("Task execution context is unavailable.")

    total = len(batch_ids)
    for completed, batch_id in enumerate(batch_ids, start=1):
        await rebuild_batch(batch_id)
        await context.report_progress(
            {
                "completed": completed,
                "total": total,
                "phase": "rebuilding",
            }
        )
```

Progress is an immutable, bounded JSON mapping. Sensitive key names such as
`password`, `credential`, `secret`, `session`, and `token` are recursively
redacted before status, lifecycle history, logs, or local events can observe
them. Non-JSON or oversized metadata raises `TaskProgressError`, a
`TaskLifecycleError` subtype. This metadata failure is terminal and is not
retried, even when the task or site retry policy allows further attempts.
Strings longer than the safe metadata limit are rejected rather than truncated.

The same task can call `report_progress()` during direct `run()` execution.
Wybra still validates and redacts the supplied metadata so direct development
exposes payload errors consistently, but discards the result because direct
execution has no lifecycle or queryable status.

## Receive status and lifecycle feedback

Submission returns a provider-neutral `TaskHandle`. Query its current status,
or use the capability when only a task ID is available:

```python
from uuid import UUID

from fastapi import HTTPException, Request

from wybra import get_site
from wybra.tasks import TasksCapability


async def task_status(request: Request, task_id: UUID) -> dict[str, object]:
    tasks = get_site(request.app).require_capability(TasksCapability)
    current = await tasks.status(task_id)
    if current is None:
        raise HTTPException(status_code=404, detail="Task status is unavailable.")

    return {
        "task_id": str(current.task_id),
        "task_name": current.task_name,
        "state": current.state.value,
        "attempt": current.attempt,
        "queue": current.queue,
        "worker_id": current.worker_id,
        "updated_at": current.updated_at,
        "progress": dict(current.progress) if current.progress is not None else None,
        "error_type": current.error_type,
    }
```

Use `await handle.status()` for the same query through the returned handle.
Use `await tasks.lifecycle(task_id)` when an operator or test needs the retained
ordered lifecycle:

```python
events = await tasks.lifecycle(handle.task_id)
states = [event.kind.value for event in events]
```

Progress metadata is canonicalised as JSON-compatible data and exposed through
an immutable mapping. Mutating the dictionary originally supplied by a
provider, or a nested value read from the mapping, cannot alter retained status
or history without a new lifecycle event. A retained progress reporter also
cannot update a completed task; the lifecycle state machine rejects the late
transition. Progress belongs to one execution attempt and is cleared when a
retry attempt starts, so status never presents stale progress from the failed
attempt as current.

Lifecycle kinds cover:

- `submitted`;
- `scheduled`;
- `started`;
- `progress`;
- `retry_scheduled`;
- `succeeded`;
- `failed`; and
- `dead_lettered`.

Successful immediate execution normally records `submitted`, `started`, and
`succeeded`. A retried failure records `retry_scheduled` between attempts. An
exhausted task records `failed` and then `dead_lettered`.

Status and lifecycle observations exclude task arguments and exception
messages. Failure status records the safe exception type, such as
`ConnectionError`, in `error_type`.

Each accepted transition also emits a fixed-message structured log containing
safe task identity, correlation, causation, attempt, queue, worker, transition,
and error-type fields. Arguments, exception messages, and progress values are
not interpolated into routine log messages.

Immediate status is process-local and retained in memory. It is not shared
between web instances and disappears when the process exits.

## Observe lifecycle events locally

Enable Wybra's process-local event delivery when application code needs
asynchronous lifecycle observations:

```toml
[wybra.events]
enabled = true
```

Subscribe during application setup with the public `task` selector:

```python
from wybra.events import EventsCapability
from wybra.site import Site
from wybra.tasks import TASK_EVENT_SCOPE, TaskLifecycleObservationEvent


async def observe_task(event) -> None:
    if not isinstance(event, TaskLifecycleObservationEvent):
        return
    print(event.task_name, event.kind.value, event.task_id)


async def setup_site(site: Site) -> None:
    events = site.require_capability(EventsCapability)
    await events.subscribe(TASK_EVENT_SCOPE, observe_task)
```

Transition-specific scopes include `task.submitted`, `task.started`,
`task.progress`, `task.retry_scheduled`, `task.succeeded`, `task.failed`, and
`task.dead_lettered`. Mirrored observations contain the same canonical safe
metadata as lifecycle status, but no arguments, exception messages, or return
values.

Local event delivery is observational only. Publication queues work without
waiting for handlers, and a disabled or failing local event sink cannot alter
task execution, retries, or status. Use task status and lifecycle queries as
the immediate backend's source of truth; do not use local event delivery as a
durable task queue.

## Handle failures and retries

Site retry settings apply when a declaration omits `retry=`. A concrete
declaration policy always wins, including `RetryPolicy(max_attempts=1)` to
disable retries for a task:

```python
from wybra.tasks import RetryPolicy, task


@task(
    name="example.billing.capture",
    retry=RetryPolicy(max_attempts=1),
)
async def capture_payment(*, payment_id: str) -> None:
    await payment_gateway.capture(payment_id)
```

Use a single attempt for operations that are unsafe to repeat. For retryable
operations, choose a bounded maximum, delay, exponential multiplier, maximum
delay, and jitter:

```python
RETRY_TRANSIENT = RetryPolicy(
    max_attempts=5,
    initial_delay_seconds=1.0,
    backoff_multiplier=2.0,
    maximum_delay_seconds=30.0,
    jitter_seconds=0.5,
)
```

The current immediate backend retries exceptions raised by the handler until
the maximum attempt count is reached. It does not re-raise the final handler
exception from `submit()`; inspect the returned status for `dead_lettered` and
`error_type`. Direct `run()` never retries and does re-raise the original
exception.

Never place task arguments, credentials, tokens, message contents, or raw
exception messages into routine task logs or progress metadata.

## Idempotency and side effects

Task delivery is designed to be at least once. A durable worker may receive the
same task command again after a crash before acknowledgement.

Use a stable idempotency key for a logical operation:

```python
options = TaskSubmissionOptions(
    idempotency_key=f"welcome-email:{account_id}",
)
```

Inside the handler, read it from execution context:

```python
from wybra.tasks import current_task_context


context = current_task_context()
if context is None:
    raise RuntimeError("Task execution context is unavailable.")

idempotency_key = context.idempotency_key
```

The key is metadata, not an exactly-once guarantee. Handlers that send email,
charge accounts, publish external messages, or mutate external services must
also use provider-supported idempotency or an application-owned operation
record.

## Scheduled tasks

The accepted platform design includes:

- one-time deferred execution at a validated future time;
- declarative recurring intervals;
- timezone-aware cron schedules;
- schedule discovery during application composition;
- one active scheduler owner for each schedule set; and
- a separately operated scheduler that publishes ordinary task commands.

Those APIs and commands are not implemented yet, so there is no supported
scheduled-task code example or configuration to copy. When scheduling lands,
this document will include concrete one-time, interval, and cron examples plus
the `wybra-task-scheduler` deployment requirements.

The immediate backend will continue to reject deferred and recurring work. An
in-process timer would not survive shutdown and would misrepresent the
durability guarantee.

Every `TasksCapability` provider exposes its supported operations through the
required `features` member:

```python
from wybra.tasks import TaskFeature


if tasks.features.supports(TaskFeature.DEFERRED):
    ...

tasks.features.require(TaskFeature.DEFERRED)
```

The immediate provider reports both `TaskFeature.DEFERRED` and
`TaskFeature.RECURRING` as unavailable. `require()` raises
`TaskFeatureUnavailableError` with an actionable provider-neutral message and
does not create a timer or lifecycle record.

## Current limitations

- Only the immediate backend executes tasks; Taskiq configuration currently
  preflights then stops before registering a capability.
- Submission is inline rather than worker-backed.
- Immediate status and lifecycle are process-local.
- Deferred and recurring schedules are not available.
- Worker and scheduler commands are not available.
- Persisted task return values and cancellation are not yet available through
  the configured task capability.
- A complete transactional outbox is deferred; future after-commit publication
  will still document the remaining database-commit-to-broker gap.
