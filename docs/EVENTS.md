# Events

`wybra.events` is a core, in-process observation service. It is available to
configured modules during setup without appearing in `[app].modules`, but
delivery is disabled by default.

```toml
[wybra.events]
enabled = true
```

Set `WYBRA_EVENT_DELIVERY_ENABLED=true` for an environment override. When
disabled, subscriptions and publications are harmless no-ops. Producers never
check diagnostics settings; hot producer boundaries may skip constructing an
observation when delivery is disabled. Event publication or handler failures
never change the operation being observed.

## Producing observations

The root package is intentionally small:

```python
from wybra.events import (
    Event,
    EventsCapability,
    available_event_scopes,
    context,
    event_scope,
    observe,
)
```

Operational code normally imports its topic-owned descriptor and adds one
decorator to its existing async boundary. It does not construct event payloads,
resolve an event capability, or inspect subscriptions.

```python
from wybra.events import observe
from wybra.events.cache import cache_event


@observe(cache_event)
async def record_cache_operation(
    operation: str,
    owner: str,
    key: str,
    *,
    outcome: str,
    started: float,
) -> None:
    del operation, owner, key, outcome, started
```

`context=` adds immutable correlation levels for the wrapped call and resets
them when it returns, raises, or is cancelled. Nested calls and child asyncio
tasks inherit the existing context. The request middleware establishes an
opaque UUIDv7 request identifier; it is available only as safe event
correlation, never as request content.

Topic schemas and selector construction remain topic-local rather than being
root-package imports. This keeps raw bound arguments and payload construction
out of ordinary producer code.

## Subscribing

Applications subscribe with the public `event_scope()` selector factory. The
selector matches the named scope and its descendants; `event_scope("sql")`
matches `sql.statement` and `sql.transaction`, but not `sqltransaction`.

```python
from wybra.events import EventsCapability, event_scope


async def audit_sql(event) -> None: ...


async def setup(events: EventsCapability) -> None:
    await events.subscribe(event_scope("sql"), audit_sql)
```

`available_event_scopes()` returns the supported selector/description pairs for
developer tooling and configuration UIs.

Handlers execute sequentially on one dispatcher-owned task in the application
event loop. Publishing queues the observation and does not wait for handlers,
so a slow or failing handler cannot delay, veto, or alter the operation that
emitted its event. At most 1,024 undelivered observations are retained; under
sustained overload, the oldest pending observation is evicted and the drop is
logged. This bounds memory without blocking producers. Failures are logged and
recorded under `events.errors`. During site shutdown, accepted observations are
drained before the dispatcher worker is released so the shutdown lifecycle
observation is delivered. An already-running handler remains part of that
graceful drain.

## Public producer scopes

- `sql`: database connections, statements, transactions, and savepoints.
- `request`, `route`, `view`: HTTP lifecycle, dispatch, and generic-view
  outcomes.
- `template`, `cache`, `form`: rendering, cache, validation, and persistence.
- `account`, `credential`: account lifecycle separately from authentication
  and credential changes.
- `session`, `security`: session lifecycle plus policy and denial outcomes.
- `task`: background task submission, execution, progress, retry, and terminal
  lifecycle observations.
- `site`: module hooks, capability registration/resolution, startup, and
  shutdown.
- `events` and `events.errors`: event delivery diagnostics and handler
  failures.

Within an HTTP operation, observations begin with `request.started`, then any
lower-level database, cache, template, form, route, or view observations at
their existing boundaries, followed by `request.completed`. Site/module events
occur during composition and shutdown, outside an HTTP request. This ordering
is observational only; it does not impose a new execution model.

Event payloads are typed, immutable, and deliberately allowlisted. They never
include request bodies, uploaded content, field values, rendered output,
headers, cookies, session identifiers or payloads, credentials, tokens,
challenge identifiers, provider identities, SQL statement or parameter values, or raw framework
objects. User UUIDs may appear as opaque internal traceability identifiers;
email values are masked.

Diagnostics projects only the opaque request identifier from event context as
`event_context_request_id`; it does not project context segments, request
bodies, headers, cookies, paths, or raw call arguments.

Capability-registration events are emitted at site composition boundaries,
including after the optional diagnostics capability has been registered. A
runtime capability registration is not itself an asynchronous dispatch point;
applications that need to observe it should register through their own
explicit asynchronous composition boundary.

## Diagnostics is optional

Events and diagnostics are independent. Enabling `[wybra.events]` delivers to
application subscribers even when diagnostics is absent or disabled.
`[wybra.diagnostics].events_enabled` installs a passive subscriber that selects
and retains configured event scopes for `/__debug/ws`; it cannot enable event
delivery, change producer behaviour, or alter another subscriber.

Enabled event delivery retains the most recent 32 safe immutable event values
in a process-local ring. Subscriptions are asynchronous. They are live-only by
default; an application that needs recent process-local observations can pass
`history=True` to atomically retain its live subscription and receive matching
history. Diagnostics does this during startup to project early composition
events. This bounded, non-durable history is not a general event store.
Disabled delivery does not create a dispatcher or history buffer.

Database events are operation observations from Wybra's isolated Tortoise
adapter. They do not replace or translate `wybra.db.signals`, which remains a
direct namespace for native Tortoise lifecycle hooks and their controlling
pre-save semantics.
