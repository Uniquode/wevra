# Cache

`wybra.cache` provides an optional registry of capability-backed caches for
application code and Jinja template fragments. Cache values are opaque bytes;
callers own serialisation and cache-key variation.

## Configuration

Install Redis support when using the Redis backend:

```sh
uv add 'wybra[cache]'
```

Configure the module and selected backend in the host application configuration:

```toml
[app]
modules = [
  "wybra.template",
  "wybra.cache",
]

[cache]
backend = "redis"
url = "redis://localhost:6379/0"

[cache.session]
backend = "redis"
url = "redis://localhost:6379/1"

[cache.transient]
backend = "memory"
```

`[cache]` defines the cache named `default`. Each `[cache.<name>]` section
defines another independent cache. Names use lower-case letters, numbers, and
underscores, must start with a letter, and cannot be `default`; that name is
reserved for `[cache]`. Named caches do not inherit the root cache's backend,
URL, or future provider settings.

`backend = "memory"` is the independent default for every section. It is
process-local, and its baseline key/value store removes expired values only
when their keys are accessed. Use it for local development, deterministic
tests, embedded single-process workers, or small bounded workloads; use Redis
when baseline cache state must be shared across workers or instances. A memory
cache cannot configure `url`; a Redis cache requires it.

The root section supports `WYBRA_CACHE_BACKEND` and `WYBRA_CACHE_URL`.
Named overrides use
`WYBRA_CACHE__<UPPER_CASE_NAME>__BACKEND` and
`WYBRA_CACHE__<UPPER_CASE_NAME>__URL`. For example,
`WYBRA_CACHE__SESSION__URL` changes only the `session` cache.

## Resolving caches

Existing code can continue to resolve `CacheCapability`; it is the baseline
byte-cache capability of the named `default` instance:

```python
from wybra.cache import CacheCapability

cache = site.require_capability(CacheCapability)
```

Use `CachesCapability` when a consumer selects a named cache:

```python
from wybra.cache import CachesCapability

caches = site.require_capability(CachesCapability)
session_cache = caches.require("session", consumer="request sessions")
optional_cache = caches.optional("transient")

await session_cache.values.set("sessions", session_id, payload, ttl=3600)
payload = await session_cache.values.get("sessions", session_id)
```

Repeated resolution returns the same site-scoped `CacheInstance`. A missing
required cache raises `CacheNotFoundError` and identifies the requesting
consumer when supplied. `caches.diagnostics()` reports each name, backend,
safe partition identifier, and advertised feature names without exposing
provider URLs or credentials.

Cache-backed request sessions are a baseline consumer. Select an isolated
instance with `wybra.sessions.cache_name`; omit the setting to use `default`:

```toml
[app]
modules = [
  "wybra.cache",
]

[cache.session]
backend = "redis"
url = "redis://localhost:6379/1"

[wybra.sessions]
storage_backend = "cache"
cache_name = "session"
```

Session startup fails before serving requests when `wybra.cache` is absent or
the selected name is not configured. Session diagnostics report only the cache
name or legacy compatibility mode, never provider URLs or credentials. See
[`SESSION.md`](SESSION.md) for legacy `cache_url` migration guidance.

Cache-backed queued messages require `AtomicCacheCapability`. Select an
isolated cache with `wybra.messages.cache_name`, or omit the setting to use
`default`:

```toml
[app]
modules = [
  "wybra.cache",
  "wybra.messages",
]

[cache.messages]
backend = "memory"

[wybra.messages]
storage_backend = "cache"
cache_name = "messages"
```

Messages startup fails when the cache module, selected name, or required atomic
feature is unavailable. The current in-memory provider supplies the feature;
the named Redis provider gains it in the Redis advanced-feature slice. See
[`MESSAGES.md`](MESSAGES.md) for legacy `cache_url` migration guidance and the
queued-alert key change.

Every cache operation requires an owner and a logical key. Owners must be
non-blank and cannot contain `:`; the owner prefixes the backend key and keeps
independent cache domains separate. Cache entries always have an explicit,
positive TTL.

## Optional features

Every backend provides the baseline byte cache through `instance.values`.
Feature-rich backends also register typed optional capabilities. Probe when a
feature is genuinely optional, or require it during startup when the consumer
cannot operate correctly without it:

```python
from wybra.cache import (
    AtomicCacheCapability,
    CacheFeatureUnavailableError,
    WorkQueueCacheCapability,
)

tasks = caches.require("tasks", consumer="background tasks")

atomic = tasks.optional(AtomicCacheCapability)
try:
    work_queue = tasks.require(
        WorkQueueCacheCapability,
        consumer="background tasks",
    )
except CacheFeatureUnavailableError:
    # Treat this as invalid application configuration.
    raise
```

Feature discovery uses explicit backend registration. A baseline backend is not
treated as atomic, queue-capable, or stream-capable merely because it happens
to define a similarly named method. Required-feature errors identify the cache,
backend, consumer, and missing feature without exposing provider credentials.

Each `CacheInstance` exposes `features` and `feature_metadata`. Presence and
operational guarantees are deliberately separate: callers must not infer
durability or horizontal safety from a feature name.

### Memory guarantees

The memory backend implements every current optional feature so application
logic and adapters can be exercised without external infrastructure.

| Feature | Memory behaviour |
| --- | --- |
| Atomic values and counters | Process-local, volatile, ordered per key |
| Leases and fencing | Process-local, volatile, ordered per resource |
| Work queues | Process-local at-least-once delivery with acknowledgement, visibility recovery, delayed retry, and dead-lettering |
| Streams | Process-local ordered replay, bounded retention, and consumer positions |
| Pub/sub | Process-local live fan-out with no replay |
| Schedules | Process-local revisioned records, due ordering, and fenced claims |

Memory features are not durable, do not recover after restart, and are not safe
for horizontal producers, workers, or schedulers. Their metadata reports these
limits explicitly. Durable providers must separately pass the same behavioural
conformance suites while advertising only the guarantees they actually supply.
Memory streams, subscriber buffers, queue depth, dead letters, and schedule
records have explicit capacity or retention bounds; operations fail or evict
the oldest retained dead-letter or stream history according to the relevant
contract rather than growing state indefinitely.

Feature payloads are opaque bytes and are limited to 1 MiB. Callers own
serialisation, schema versioning, redaction, and domain validation.

### Atomic values and leases

Atomic values use revisions for compare-and-swap and compare-and-delete.
Counters return both the resulting value and a new revision:

```python
from wybra.cache import AtomicCacheCapability, LeaseCacheCapability

atomic = tasks.require(AtomicCacheCapability, consumer="task idempotency")
created = await atomic.create("tasks", task_id, payload, ttl=3600)
if created is not None:
    updated = await atomic.compare_and_swap(
        "tasks",
        task_id,
        created.revision,
        replacement,
        ttl=3600,
    )

leases = tasks.require(LeaseCacheCapability, consumer="task scheduler")
lease = await leases.acquire(
    "tasks",
    "scheduler",
    worker_id,
    ttl=30,
)
if lease is not None:
    lease = await leases.renew(lease, ttl=30)
    await leases.release(lease)
```

An expired lease can be acquired by another holder with a newer fencing token.
Renewing or releasing a stale token raises `CacheConflictError`.

### Work queues

Work queues provide stable identities, explicit acknowledgement, visibility
timeouts, delayed availability, bounded attempts, and dead-lettering:

```python
from wybra.cache import WorkQueueCacheCapability

queue = tasks.require(WorkQueueCacheCapability, consumer="task workers")
identity = await queue.publish(
    "tasks",
    "default",
    payload,
    max_attempts=3,
)
delivery = await queue.reserve(
    "tasks",
    "default",
    worker_id,
    visibility_timeout=30,
    wait_timeout=10,
)
if delivery is not None:
    try:
        await handle(delivery.payload)
    except NonRetryableTaskError:
        await queue.dead_letter(delivery)
    except Exception:
        await queue.reject(delivery, delay=5)
    else:
        await queue.acknowledge(delivery)
```

Delivery is at least once: work whose visibility expires can be delivered
again. Consumers must make externally visible effects idempotent. The returned
identity does not imply exactly-once execution. Use `dead_letter()` when
consumer policy determines that a delivery must not be retried; `reject()`
retains the item for another attempt until its configured attempt boundary.

### Streams and pub/sub

Use streams when a consumer needs ordered replay, retained history, or a
resumable position. Use pub/sub only for live notifications where offline
consumers are expected to miss messages:

```python
from wybra.cache import PubSubCacheCapability, StreamCacheCapability

stream = tasks.require(StreamCacheCapability, consumer="task lifecycle")
position = await stream.append("tasks", "lifecycle", event_payload)
records = await stream.read_consumer(
    "tasks",
    "lifecycle",
    "status-projection",
)
if records:
    await stream.acknowledge(
        "tasks",
        "lifecycle",
        "status-projection",
        records[-1].position,
    )

pubsub = tasks.require(PubSubCacheCapability, consumer="live task updates")
subscription = await pubsub.subscribe("tasks", "updates")
try:
    message = await subscription.receive(timeout=10)
finally:
    await subscription.close()
```

A replay request older than retained stream history raises
`CachePositionExpiredError`. Pub/sub never promises replay; publishing before a
subscriber connects returns no delivery for that subscriber.

### Schedules

Schedule records use revisions for concurrent updates and fencing tokens for
competing scheduler claims:

```python
from wybra.cache import ScheduleCacheCapability

schedules = tasks.require(
    ScheduleCacheCapability,
    consumer="task scheduler",
)
record = await schedules.create(
    "tasks",
    "daily-report",
    payload,
    next_due_at=next_run_timestamp,
    interval_seconds=86400,
)
for due in await schedules.due("tasks", before=current_timestamp):
    claim = await schedules.claim(
        "tasks",
        due.identity,
        scheduler_id,
        ttl=30,
    )
    if claim is not None:
        await emit_task(claim.record.payload)
        await schedules.complete(claim)
```

Completing a one-time schedule removes it. Completing a recurring schedule
advances it to the first future interval. A stale claim cannot complete or
release a newer scheduler's work. Updating a schedule with a live claim returns
no update; release or complete the claim before retrying the revisioned update.

## Template fragments

`wybra.template` always recognises the cache tag, even when `wybra.cache` is
not configured. Without a cache capability, the tag simply renders its body.

```jinja
{% cache "profile-card" ttl=300 vary_by=(request.user.id, locale) %}
  <h2>{{ request.user.display_name }}</h2>
{% endcache %}
```

The explicit name, template generation, and `vary_by` values identify a
fragment. Include every value that can change the rendered body in `vary_by`.
For personalised output this normally includes a stable user or request
identity, and may also include locale, permissions, tenant, or feature state.

`vary_by` accepts JSON-compatible values: `None`, booleans, finite numbers,
strings, mappings with string keys, and lists or tuples containing those values.
Mappings are ordered by key; lists and tuples retain their order; sets are
ordered deterministically. Do not pass a model, request, datetime, enum, or
another arbitrary object directly: its display representation is not a safe
cache identity.

Use the template `cache_key()` helper when a fragment varies by several named
conditions or includes an application type. It produces a canonical key value:

```jinja
{% cache "profile-card" ttl=300
   vary_by=cache_key(user=request.user, locale=locale, permissions=permissions) %}
  <h2>{{ request.user.display_name }}</h2>
{% endcache %}
```

Register normalisers for application-specific values on the template
capability. A normaliser must return only JSON-compatible values; for models,
prefer an explicit stable type identifier and primary key:

```python
templates.register_cache_key_normaliser(
    User,
    lambda user: {"type": "accounts.user", "pk": user.pk},
)
```

After registering a normaliser, `cache_key(user=request.user)` and
`vary_by=request.user` both use it. Prefer `cache_key()` for named, readable
variation conditions in templates.

Never cache CSRF tokens, password-reset links, one-time codes, or other
per-request secrets inside a fragment. Keep those values outside the cached
body, or use a design that deliberately separates the per-request value from
the reusable markup.

The fragment cache stores rendered markup as UTF-8 bytes. It does not cache
querysets, serialise structured Python values, or invalidate reverse proxies
or CDNs.
