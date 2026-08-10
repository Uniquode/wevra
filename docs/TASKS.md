# Tasks

`wybra.tasks` provides provider-neutral declarations for asynchronous
application work. A task is an ordinary async function with a stable identity,
validated JSON-compatible arguments, optional retry policy, execution context,
and lifecycle status.

Task declarations do not require task infrastructure. Applications can run a
declared task directly, or enable the optional task module and submit it through
`TasksCapability`.

## Execution modes

Wybra's task platform covers five application use cases:

| Use case | Current availability | Behaviour |
| --- | --- | --- |
| Direct task call | Available | Runs in the caller, returns the function result, and propagates the original exception. |
| On-demand submission | Available with the immediate backend | Uses task validation, retry, lifecycle, status, correlation, and idempotency metadata, but executes inline in the submitting process. |
| Durable background submission | Available with the Taskiq backend | Publishes through the selected cache and returns a durable handle before worker execution. |
| One-time deferred task | Planned | Will submit one ordinary task command for a validated future time. |
| Recurring interval or cron task | Planned | Will be discovered declaratively and published by a separately operated scheduler. |

The immediate backend is useful for development, tests, simple deployments,
and adopting the task API before worker infrastructure is available. It is not
a background thread or process: submission waits for execution and retries to
finish.

The Taskiq backend supplies durable submission, lifecycle tracking, result
storage, and a cache-aware worker receiver. Run that receiver in a separate
process with `wybra-task-worker`; declarative deferred and recurring task APIs
remain separate work. Do not use `asyncio.create_task` or an in-process timer
as a durability substitute; those operations are lost when the web process
exits.

## Enable the task capability

Add `wybra.tasks` to the host application's configured modules:

```toml
[app]
modules = [
  "wybra.tasks",
  "example",
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
backend = "taskiq"
default_queue = "default"
max_attempts = 3
initial_delay_seconds = 0.25
backoff_multiplier = 2.0
maximum_delay_seconds = 10.0
jitter_seconds = 0.5
status_retention_seconds = 3600
active_status_timeout_seconds = 86400
worker_shutdown_grace_seconds = 30
visibility_timeout_seconds = 30
wait_timeout_seconds = 1
max_delivery_attempts = 3
result_retention_seconds = 3600
max_result_bytes = 65536
```

Durable task messages, including their validated arguments and Taskiq envelope,
must fit the selected cache feature's 64 KiB payload limit. A submission that
exceeds it is definitively rejected before publication.

`backend = "immediate"` is the default. The optional `taskiq` backend registers
the durable `TasksCapability` during site composition when its selected cache
provides every required feature.
Without `wybra.tasks` in `[app].modules`, the capability is absent but declared
tasks remain directly executable. The same is true when the module is present
with `enabled = false`.

### Taskiq preflight

Install the optional dependency before selecting the Taskiq backend:

```sh
uv add 'wybra[tasks]'
```

Taskiq configuration selects a named provider-neutral cache. Startup constructs
the private broker, result backend, lifecycle middleware, and cache-aware
receiver factory, then registers a durable `TasksCapability`.

```toml
[app]
modules = [
  "wybra.cache",
  "wybra.tasks",
  "example",
]

[tasks]
backend = "taskiq"
cache_name = "default"
```

`cache_name` defaults to `"default"`; its cache-name format is validated only
for the Taskiq backend, while the immediate backend ignores it. The selected
name must correspond to `[cache]` for `default`, or to a configured named cache
such as `[cache.task_work]` when `cache_name = "task_work"`. Taskiq setup runs
after cache setup finalisation and requires the selected cache's baseline
values plus `work-queue`, `stream`, `atomic`, `lease`, and `time` features.
Missing prerequisites stop startup with a safe, provider-neutral diagnostic;
Wybra never falls back to inline execution.

Start one or more workers against the same selected durable cache:

```sh
wybra-task-worker
```

The command loads the configured ASGI application and enters its lifespan
before resolving the Taskiq receiver. A worker therefore uses the same module
composition, cache credentials, lifecycle recovery, logging, and shutdown
behaviour as the application. It refuses to start unless enabled tasks resolve
to the `taskiq` backend. Pass `--config path/to/app.toml` to select a specific
application configuration.

### Cache-backed Taskiq results

`CacheTaskiqResultBackend` is the reusable Taskiq adapter for retaining result
envelopes through a selected cache's baseline byte-value capability. The
configured Taskiq runtime creates and registers it privately.

The runtime supplies an immutable `TaskiqResultPolicy` with a positive result
retention period, maximum serialised byte size, and encoder/decoder callables.
When either codec is `None`, the policy uses Taskiq's standard JSON model codec.
The configured runtime uses a safe encoder that retains only result readiness,
success/failure state, and execution duration. It removes task return values,
worker logs, exception details, and labels before the cache write. The reusable
adapter still accepts an explicit caller-owned codec when another integration
has its own safe-result policy.

Readiness uses the baseline cache read operation, so each wait poll reads the
stored result envelope. Configure the result byte limit and future worker poll
interval together to control result-wait cache traffic.

Configured Taskiq results are retained only as safe completion metadata; they
do not expose arbitrary return values, worker logs, exception details, or task
labels through the task capability.

### Cache-backed Taskiq broker

`CacheTaskiqBroker` is the reusable optional adapter from Taskiq's broker
contract to a named cache's work-queue feature. It publishes the opaque bytes
produced by Taskiq's formatter and returns them to workers with a Taskiq
acknowledgement callback bound to the exact queue delivery. On delivery, the
adapter carries the queue's monotonically increasing delivery attempt in its
private Taskiq envelope. Taskiq task identity remains in that envelope; cache
delivery receipts and provider details remain internal to the adapter.
Durability follows the selected cache provider:
the memory work queue remains process-local and volatile, while a conforming
production provider advertises its shared durability and restart guarantees.

Taskiq retry middleware re-publishes a failed task through the broker and its
float-coercible `delay` label becomes durable queue delay in seconds. A worker
that stops before acknowledging leaves its delivery for normal visibility-expiry
recovery. Production workers that require at-least-once execution must not use
Taskiq's `WHEN_RECEIVED` acknowledgement mode, which settles the delivery before
execution. The configured visibility timeout remains the initial reservation
lease. A task with a longer expected execution time can declare a larger
visibility budget; the cache-backed receiver renews that delivery while the
task executes. A worker that cannot renew before expiry may still lose the
delivery, so task handlers must remain idempotent.

The configured Taskiq runtime activates durable submission and exposes its
cache-aware receiver factory to `wybra-task-worker`. The receiver renews
long-running delivery leases and acknowledges obsolete redeliveries before
task execution. It acknowledges a completed delivery only after Taskiq has
persisted the result and the lifecycle middleware has recorded success; a
result-storage failure leaves the delivery available for visibility recovery.
On shutdown, the worker stops receiving new deliveries, waits for at most
`worker_shutdown_grace_seconds` for active callbacks, then cancels any remaining
callbacks, permits at most one second for cancellation cleanup, and leaves any
remaining deliveries for visibility recovery. Cancellation remains cooperative:
an application handler that suppresses cancellation requires its process
supervisor to terminate the worker after this bounded drain.

The receiver owns Taskiq's execution sequence instead of calling its standard
callback, because Taskiq's callback can log argument-bearing messages at debug
level. Wybra task definitions validate payloads before submission and require
async handlers, so Taskiq parameter validation and executor options are
rejected for this receiver. Taskiq timeout labels, dependency injection, and
sync-callable execution are not supported by the configured runtime.

The runtime suppresses Taskiq kicker debug records that can include task
arguments. Operators should use Wybra lifecycle status and safe task identifiers
for task diagnostics instead of enabling argument-bearing Taskiq debug output.

### Cache-backed Taskiq lifecycle

`CacheTaskiqLifecycleMiddleware` is the private optional adapter that maps
Taskiq submission and worker hooks to Wybra's lifecycle stream and projected
status. It records only task identity, queue, correlation, task and delivery
attempts, a private work identity, worker, safe progress, and failure
classification. A stable work identity prevents one duplicate retry command
from terminalising a distinct live command for the same logical task attempt.
The delivery attempt fences stale workers: a later delivery can replace an
expired worker, but lifecycle facts from the earlier delivery cannot overwrite
its status. It never copies task arguments,
keyword arguments, Taskiq return values, worker logs, or exception messages
into lifecycle records.

The lifecycle adapter requires the selected cache's `stream`, `atomic`, `lease`,
and `time` features. The lifecycle stream is the ordered recovery source. A
revisioned atomic status projection provides `submitted`, running, retry, and
terminal state by task ID. Each lifecycle event refreshes the active-status
timeout, so a task abandoned by a stopped worker is eventually removed rather
than occupying projection storage indefinitely, even if a provider delays
physical key expiry. The first terminal transition instead applies the
configured task status retention, measured from that transition rather than a
later repair. A failed projection write
is repaired by replaying the already-appended lifecycle record, so a worker
restart does not need to interpret Taskiq result storage as public Wybra status.
Lifecycle timestamps and replay retention use the selected cache's calibrated
time, preventing host-clock skew from changing shared status lifetime.

The active-status timeout is an abandonment boundary, not a worker heartbeat.
Silent tasks that can run longer than the configured timeout must emit lifecycle
progress or use a longer timeout. Once an active projection expires, a later
event can be recovered only when the retained lifecycle stream still contains a
legal `submitted` origin; an orphaned terminal event is rejected rather than
inventing task state.

The middleware appends `submitted` before broker delivery, then records
`started`, a non-terminal `attempt_failed`, `retry_scheduled` after the retry
hand-off succeeds, terminal `failed` then `dead_lettered`, or `succeeded` only
after Taskiq has stored the worker result. Install it after one cache-aware wrapper
of Taskiq's
standard retry middleware so its reverse-order error hook records the failed
attempt before Taskiq decides whether to re-publish it; startup rejects an
incompatible order, multiple retry middlewares, or an unobserved standard
retry middleware. The configured Taskiq runtime installs this middleware and
its cache-aware retry wrapper.

A retry schedule is attributed to the worker and delivery that failed. If a
later delivery has already replaced that worker, the stale retry schedule and
any rejected retry hand-off are ignored rather than changing the replacement's
status.

The cache-aware retry wrapper retains Taskiq's retry policy and observes its
send failure. When the selected queue provides a definitive rejection signal
and rejects a retry publication, the adapter records terminal `failed` and
`dead_lettered` facts immediately.
Transport failures remain indeterminate: the queue may have accepted the retry
before the response was lost, so the adapter retains `attempt_failed` rather
than incorrectly declaring it dead-lettered. A later retry execution resolves
that state naturally; startup replay can terminalise an abandoned record after
the active-status timeout.

The lifecycle adapter currently rejects `SmartRetryMiddleware` configured with
a Taskiq schedule source. Taskiq submits scheduled retries directly to the
schedule source and bypasses its send hooks, so this adapter cannot confirm the
hand-off without a dedicated lifecycle-aware schedule integration.

### Cache-backed Taskiq schedule adapter

`CacheTaskiqScheduleSource` is a cache adapter available for direct Taskiq
integration. It is not activated by `[tasks]`, and `wybra-task-worker` does not run
a scheduler. Wybra's declared deferred and recurring task APIs will own that
integration in a later task-platform slice.

`CacheTaskiqScheduleSource` is the reusable optional adapter from Taskiq's
schedule-source interface to a selected cache's `schedule` and `time` features.
It has no Redis or provider-specific dependency. Construct it with a stable
scheduler claimant and a claim TTL that covers the complete broker-enqueue and
post-enqueue settlement path:

```python
from wybra.cache import CacheTimeCapability, ScheduleCacheCapability
from wybra.tasks.taskiq_schedule import (
    CacheTaskiqScheduleSource,
    TaskiqSchedulePolicy,
)

task_cache = caches.require("tasks")
schedules = task_cache.require(
    ScheduleCacheCapability,
    consumer="task scheduler",
)
cache_time = task_cache.require(CacheTimeCapability, consumer="task scheduler")
source = CacheTaskiqScheduleSource(
    schedules,
    policy=TaskiqSchedulePolicy(
        claimant="scheduler-1",
        claim_ttl_seconds=30,
        owner="primary-schedules",
        timezone="Australia/Melbourne",
        source_refresh_interval_seconds=1,
        scan_page_limit=100,
        scan_limit=1_000,
    ),
    cache_time=cache_time,
)
```

The source persists one-time, fixed-interval, and five-field minute-resolution
cron schedules as UTC Unix timestamps. Cron expressions use `croniter`'s
supported five-field calendar syntax in the configured IANA `pytz` timezone.
Taskiq `cron_offset` is deliberately rejected: use the named `timezone`
instead so daylight-saving handling is explicit.
Pass the selected cache's `time` feature so creation and due discovery share the
cache's authoritative clock. For isolated process-local work, pass an
`InMemoryCacheTime` built from the same deterministic clock as the schedule
storage.

`source_refresh_interval_seconds` declares the Taskiq source refresh cadence.
It defaults to 60 seconds, matching Taskiq's default scheduler refresh. Fixed
interval schedules shorter than that value are rejected rather than silently
coalesced. When wiring Taskiq manually, configure its `--update-interval` to a
whole-second value no greater than the policy value; the later Wybra scheduler
runtime will own that wiring. If an existing schedule becomes incompatible after
the source cadence changes, the source records that identity locally, releases
its claim without changing the durable due time, warns without exposing schedule
content, and skips the unchanged revision on later refreshes. Every schedule
feature has a capacity no greater than 10,000 records. `due_limit` bounds the
Taskiq snapshots returned for hand-off; `scan_page_limit` bounds each cache due
page; and `scan_limit` bounds the total due records inspected in one refresh.
If that scan budget is exhausted, the next refresh resumes after its final
record. An all-deferred scan retains its continuation while it has more due
records to inspect, so unchanged deferred records are not re-read on every
poll. On reaching the end, the source wraps once within its remaining current
refresh budget; a later refresh starts again from the earliest due record. This
lets a cooperating source discover earlier peer-created, replaced, or
claim-recovered work. While it tracks deferred revisions, successful ordinary
work also retains its continuation so recurring work is not repeatedly delayed
behind the same deferred prefix. A failed hand-off or lost pending claim resets
the cursor for normal claim-TTL recovery. Adding or deleting a schedule through
that source also starts a new scan. This preserves the durable schedule for a
source with a compatible cadence without allowing incompatible records to hide
later work or requiring one provider response to contain the entire schedule
set.

`owner` identifies one isolated durable schedule set within the selected cache
and defaults to `"taskiq-scheduler"`. Schedulers that cooperate on the same set
use the same owner; independent schedule sets use different owners even when
they share one cache instance. Durable adapter envelopes carry a private schema
version. A source releases an unsupported version without changing or deleting
it, allowing a compatible source to process it, while malformed envelopes for
the current version are claim-fenced and discarded.

For a one-time schedule, a naive `datetime` is interpreted in that timezone.
Naive values that fall in a daylight-saving gap or overlap are rejected; pass
an aware `datetime` when the intended instant must select one occurrence.

Cron schedules coalesce missed work by default: after an outage, only the
latest matching occurrence is dispatched. Set `catch_up_limit` to a larger
bounded value to dispatch the most recent matching occurrences one at a time;
older occurrences are atomically discarded before hand-off. The cache remains
the timing authority, while the adapter gives Taskiq a one-shot dispatch
snapshot for each claimed occurrence.

Deleting a schedule prevents a claim that has not yet passed the source's
pre-send check from being handed to the broker. It cannot revoke a command the
broker has already accepted; scheduled task handlers therefore retain the same
at-least-once and idempotent-effect requirements as other durable tasks.
If shutdown begins after the pre-send check while broker acceptance is still
unknown, the source leaves that claim for normal TTL recovery rather than
releasing it for a competing scheduler. Once shutdown begins, the source stops
accepting new refreshes and pre-send checks, waits for an in-progress source
operation to finish, then releases only claims that have not begun broker
handoff.
The source discards an invalid or internally inconsistent adapter-owned durable
envelope through its live claim and emits a bounded warning with the internal
schedule identity but without schedule payload content, so corrupt records
cannot block later due work.
The adapter validates the record identity, one-time due time, and every
persisted cron catch-up instant before dispatching it.

See the [scheduler/cache interaction diagram](TaskScheduler.mmd) for the
durable schedule and broker hand-off flow.

The configured Taskiq runtime does not activate this source. Public declarative
schedule registration and scheduler orchestration remain separate work.

The retry settings above become site defaults for tasks that do not declare
their own `RetryPolicy`. Terminal immediate-task status and lifecycle history
remain available for `status_retention_seconds`; each task's visible lifecycle
history is also bounded.

Task declaration modules must be imported during normal application
composition so their stable identities are registered. Keep declarations in
application-owned modules rather than importing them dynamically from task
messages. Automatic discovery retains module-bound declarations; a factory-
local declaration must be retained by application code if it is intended for
durable submission. `@task` records the module that declares the task. When the site
composes its configured modules, the durable runtime automatically discovers
declarations owned by those module roots and builds an isolated registry for
that site. No application registry object or decorator argument is required.
Tasks declared by modules outside the configured application module roots
remain directly executable but are unavailable to that site's durable worker.

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

from example.application.tasks import rebuild_search

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

If durable submission raises `TaskSubmissionError`, inspect
`error.acceptance_unknown`. When it is `True`, a provider may have accepted the
command before its response was lost; use `error.task_id` to query lifecycle
status before submitting again. When it is `False`, publication did not begin
or the selected queue definitively rejected it, so the application can safely
choose a new submission. Redis queue failures remain indeterminate unless Redis
itself confirms that a command was not accepted.

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
- `attempt_failed`;
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

Immediate-task transitions emit a fixed-message structured log containing safe
task identity, correlation, causation, attempt, queue, worker, transition, and
error-type fields. The durable Taskiq backend keeps its lifecycle history and
status in the selected cache; both backends exclude arguments, exception
messages, and progress values from routine diagnostics.

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

- The immediate backend executes submissions inline; the Taskiq backend
  publishes durable work for `wybra-task-worker` processes.
- Taskiq result storage is an adapter concern; `TaskHandle` exposes lifecycle
  status rather than arbitrary task return values.
- Deferred and recurring schedules are not available.
- The worker command is available; a scheduler command remains separate work.
- Persisted task return values and cancellation are not available through the
  configured task capability.
- A complete transactional outbox is deferred; future after-commit publication
  will still document the remaining database-commit-to-broker gap.
