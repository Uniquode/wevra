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
namespace = "website_default"

[cache.session]
backend = "redis"
url = "redis://localhost:6379/1"
features = []

[cache.messages]
backend = "redis"
url = "redis://localhost:6379/1"
namespace = "website_messages"
features = ["atomic"]

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
cache cannot configure `url`; a Redis cache requires either `url`, `url_source`,
or the documented native environment URL override.

Redis keys are isolated by `namespace`. It defaults to the cache name, so
`[cache]` uses `default` and `[cache.messages]` uses `messages`. Set an
explicit namespace when multiple applications share one Redis database.
Namespaces are non-secret identifiers containing lower-case letters, numbers,
and underscores.

Omitting `features` enables every optional feature implemented by the selected
backend. Set an explicit list to narrow the advertised capabilities; an empty
list provides only the baseline byte cache. Configuration fails when a feature
is duplicated, unknown, or not implemented by that backend.

The root section supports `WYBRA_CACHE_BACKEND`, `WYBRA_CACHE_URL`,
`WYBRA_CACHE_NAMESPACE`, and `WYBRA_CACHE_FEATURES`. Named overrides use
`WYBRA_CACHE__<UPPER_CASE_NAME>__<FIELD>`. For example,
`WYBRA_CACHE__MESSAGES__FEATURES=atomic` changes only the `messages` cache.
Environment feature lists are comma-separated.
A blank environment feature value is treated as unset; use `features = []` in
configuration when a Redis cache must provide only the baseline byte-cache API.

### Redis connection secrets

For production Redis connections, configure both `wybra.secrets` and
`wybra.cache`. Redis URLs and credentials are resolved during cache startup,
after configured module setup has completed and before the cache instance is
registered; their order in the modules list does not matter:

```toml
[app]
modules = [
  "wybra.secrets",
  "wybra.cache",
]
```

The normal environment configuration fields are the preferred way to supply a
complete connection URL outside the configuration file:

```sh
export WYBRA_CACHE_URL='rediss://cache.internal:6380/0'
export WYBRA_CACHE__SESSION__URL='rediss://cache.internal:6380/1'
```

These override the default `[cache]` and named `[cache.session]` URLs
respectively. They do not require `url_source = "environment"` in the file.

For an OS-keychain URL, configure `url_source = "keychain"`. The default cache
then uses `cache/redis/url`; a named cache uses its own deterministic
key, such as `cache/session/redis/url`:

```toml
[cache]
backend = "redis"
url_source = "keychain"

[cache.session]
backend = "redis"
url_source = "keychain"
```

Set `url_key` only to intentionally override those derived keychain names. An
explicit environment URL reference is also supported, but it must select a
separate environment variable to avoid conflicting with the native cache URL
override:

```toml
[cache.reporting]
backend = "redis"
url_source = "environment"
url_key = "REPORTING_REDIS_URL"
```

`wybra-secret list` includes configured keychain references with cache-specific
names such as `cache-url`, `cache-session-credentials`, and
`cache-tasks-url`.

An endpoint without userinfo is valid in configuration. Add a credential
source when the endpoint and credentials have different deployment ownership:

```toml
[cache]
backend = "redis"
url = "rediss://cache.internal:6380/0"
credentials_source = "keychain"

[cache.session]
backend = "redis"
url = "rediss://cache.internal:6380/1"
credentials_source = "environment"
```

The keychain credential default is `cache/redis/credentials`; the environment
credential default is `WYBRA_REDIS_CREDENTIALS`. Both caches may deliberately
use the same credential value. Set `credentials_key` to select a different
credential entry for one cache. Credential values use `username:password`
form; `:password` is also valid when Redis uses password-only authentication.
Wybra URI-encodes both parts when it constructs the final connection URL. The
endpoint must not itself contain credentials when `credentials_source` is
configured.

`url_source` cannot be combined with `url` or `credentials_source`: a resolved
full URL is already a complete connection. If a configured secret source or
key cannot be resolved, startup fails before serving requests and never falls
back to a raw URL.

A raw credential-bearing URL remains accepted only for backwards compatibility:

```toml
[cache]
backend = "redis"
url = "rediss://user:password@cache.internal:6380/0"
```

Treat this as a last resort: it places credentials in the configuration file.
Wybra redacts it from cache settings, diagnostics, events, and startup errors,
but it cannot make the configuration file itself safe.

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
safe partition identifier, advertised feature names, and health state without
exposing provider URLs or credentials.

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
backend = "redis"
url = "redis://localhost:6379/2"
features = ["atomic"]

[wybra.messages]
storage_backend = "cache"
cache_name = "messages"
```

Messages startup fails when the cache module, selected name, or required atomic
feature is unavailable. Both memory and Redis supply the feature. See
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

### Redis guarantees

The Redis backend performs a health check during cache-registry startup and
shares one async client across the named instance's baseline and optional
features. Startup fails with installation guidance when `wybra[cache]` is not
installed, or with a bounded diagnostic when Redis is unavailable.

The baseline byte cache needs only a reachable Redis server when configured
with `features = []`. Enabling `atomic`, `lease`, `schedule`, `stream`, or
`work-queue` additionally requires Redis scripting, append-only persistence,
and an eviction policy other than `allkeys-*`. Wybra checks those prerequisites
at startup before advertising the features, because their revisions and fencing
tokens must survive ordinary key expiry and process restart. Advanced-feature
Redis ACLs must also permit `PEXPIRE` for bounded readiness probes. If the
Redis service blocks `CONFIG GET`, Wybra logs a warning and continues only
when its operational readiness checks pass;
the operator then owns verification of persistence and eviction policy. Redis
Cluster is not yet supported for these advanced features; use a standalone or
compatible non-clustered deployment.

Upgrading an existing Redis baseline cache with omitted `features` enables the
implemented advanced features and therefore requires these prerequisites.
Configure `features = []` to retain baseline-only behaviour while planning a
Redis durability upgrade.

Runtime diagnostics report `ready` after this startup check. They do not claim
that a Redis connection or configuration remains healthy after startup; an
operation reports a safe cache error if the provider later becomes unavailable.

| Feature | Redis behaviour |
| --- | --- |
| Baseline byte cache | Shared expiring values, isolated by named namespace |
| Atomic values and counters | Shared atomic create, compare-and-swap, compare-and-delete, and increment with persistent revisions |
| Leases and fencing | Shared renewable leases with opaque holder tokens and persistent monotonic fencing tokens |
| Work queues | Shared Redis Streams consumer-group delivery with acknowledgement, visibility recovery, delayed retry, and bounded dead letters |
| Schedules | Shared revisioned one-time and interval records with due ordering and fenced claims |
| Streams | Shared ordered replay with a fixed 1,000-record retained history and durable consumer positions |
| Pub/sub | Not yet implemented |

Redis atomic mutations and lease changes are provider-atomic. Revisions and
fencing sequences survive individual entry expiry, deletion, and Wybra runtime
restart according to the Redis provider's configured persistence window.
`appendfsync always` is required where an unclean Redis crash must not lose a
recent sequence update. Deleting sequence keys, `FLUSHDB`, and `FLUSHALL` are
destructive operations that invalidate previously issued revisions and fencing
tokens. Lease expiries use Redis server time, making coordination safe across
horizontally scaled application instances. Redis feature metadata reports
shared scope, durability, restart recovery, and horizontal-consumer support.

Provider failures are translated to bounded cache feature errors without
including URLs, credentials, physical keys, values, scripts, or lease tokens.
Wybra logs the failed operation and exception type for operator diagnostics.
The underlying Redis service still owns persistence and availability; deploy
and back it up according to the application's durability requirements.

### Redis work queues

The Redis `work-queue` feature provides durable, at-least-once delivery across
application instances. Each logical owner and queue receives a namespaced Redis
Stream and consumer group. Consumers acknowledge a delivery with its opaque
receipt; an unacknowledged delivery becomes eligible for recovery after its
visibility timeout. Rejection can defer retry durably, and terminal failures
are retained in a bounded dead-letter stream.

Delayed publication uses bounded, provider-private Redis Streams. One promoter
per configured Redis cache runtime multiplexes active queue signals and writes
due items to their work streams. Blocking reservers also maintain that promoter,
so they promote delayed work promptly even when the publishing process exits.

Task handlers and other consumers must be idempotent: a worker can complete an
external side effect before it loses its delivery receipt, so exactly-once
execution is not promised. Startup probes Redis Streams consumer groups,
claims, scripts, sorted sets, and settlement operations; a server that lacks
that command surface cannot advertise the feature. Live pub/sub, Taskiq
adapters, and JetStream remain separate future work.

### Redis schedules

The Redis `schedule` feature provides durable revisioned one-time and interval
schedule records for each logical owner. A due query returns records in due-time
order and omits schedules holding a live claim. A scheduler claims a due record
with an opaque token and monotonically increasing fencing token; only that
claim can release or complete the record. Expired claims automatically become
eligible for a later scheduler with a newer fencing token.

Completing a one-time schedule removes it. Completing an interval schedule
advances its due time past missed intervals without creating a burst of historic
emissions. Updating a claimed schedule or using a stale revision returns no
update; releasing or completing a stale claim raises `CacheConflictError`.

Schedule records, due indexes, and TTL-backed claim keys are private to the
configured Redis namespace. They survive cache registry reconstruction and
Redis restart according to the configured Redis persistence window. Redis
server time controls claim eligibility, claim expiry, and recurring advancement.
The `due()` boundary remains caller-supplied, so scheduler hosts should stay
synchronised with Redis. Claim-expiry recovery is bounded per query. This
feature stores opaque schedule payload bytes only; cron evaluation, task
dispatch, and Taskiq schedule sources remain separate work.

### Redis streams

The Redis `stream` feature provides durable ordered records for one logical
owner and stream. Each append receives a stable, monotonically increasing
`StreamPosition`; the Redis implementation keeps its internal stream identifier
private. Internally, its identifiers encode the allocated position rather than
a timestamp; do not add auto-generated Redis stream entries to Wybra-managed
stream keys. Redis retains the latest 1,000 records for each logical stream and
trims older records exactly when later records are appended. Stream positions
use positive signed 64-bit integers, matching Redis's durable sequence limit.

Consumers use `read_consumer()` and `acknowledge()` to retain a durable,
monotonically advancing position. A consumer can therefore recreate its Wybra
cache registry or resume on another application instance and read only records
after its last acknowledgement. Reading is at least once until acknowledgement:
an interrupted projection can read a record again and must make its external
effects idempotent. Attempting to move an acknowledgement backwards or beyond
the latest appended position raises `CacheConflictError`.

Each logical Redis stream retains positions for at most 10,000 durable consumer
names. A new consumer beyond that bound receives a cache feature error; use a
stable consumer identity for a projection or explicitly partition independent
projections into separate streams. Call `forget_consumer()` when a projection is
permanently retired: it removes the durable position, frees the per-stream
consumer slot, and causes a later consumer with that name to replay retained
history.

Replay is bounded by retention. Passing an `after` position older than the
retained window, including through `read_consumer()` after a long-lived
consumer has fallen behind, raises `CachePositionExpiredError` rather than
silently skipping history. Recover by rebuilding the projection from its
authoritative source, then acknowledge the most recent retained record before
resuming consumer reads. Redis startup verifies the append, exact trim, replay,
and consumer-position command surface before the feature is advertised.
Deleting stream or sequence keys, `FLUSHDB`, and `FLUSHALL` invalidates their
positions.

### Atomic values and leases

Atomic values use revisions for compare-and-swap and compare-and-delete.
Counters return both the resulting value and a new revision:

Redis counters use Redis signed 64-bit integer arithmetic. Incrementing beyond
that range fails with a bounded cache feature error; in-memory counters use
Python integers and do not share that provider limit.

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

### Redis namespace migration

Named Redis caches now prefix every baseline and advanced key with their
resolved namespace. Existing values written by the earlier unnamespaced named
Redis provider are not read or migrated automatically. Treat those entries as
disposable cache state, expire old session keys according to policy, and plan
for pending legacy queued messages to be cleared or drained before switching.

Changing a configured namespace has the same effect as selecting a fresh cache
partition. Keep it stable after deployment unless intentional invalidation is
required.

### Work queue API

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
`visible_until` is a Unix timestamp float. `dead_letters()` returns retained
entries in their original dead-letter order, oldest first.

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

await stream.forget_consumer(
    "tasks",
    "lifecycle",
    "retired-status-projection",
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

For Redis, a logical stream retains exactly 1,000 records. A projection that
cannot tolerate missed history must acknowledge frequently enough to remain
within that retained window, or rebuild from its authoritative source. Both
memory and Redis backends bound durable consumer names per logical stream;
retire obsolete names with `forget_consumer()` so they do not consume that
stream's capacity.

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
