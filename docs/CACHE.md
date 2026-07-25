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
process-local, has no size bound, and removes expired values only when their
keys are accessed. Use it for local development or small, bounded workloads;
use Redis when cache state must be shared across workers or instances. A
memory cache cannot configure `url`; a Redis cache requires it.

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

Every cache operation requires an owner and a logical key. Owners must be
non-blank and cannot contain `:`; the owner prefixes the backend key and keeps
independent cache domains separate. Cache entries always have an explicit,
positive TTL.

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
