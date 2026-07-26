# Session Configuration

Wybra installs its own session middleware as core application infrastructure.
Applications and Wybra modules use the standard `request.session` value; do not
also install Starlette `SessionMiddleware`.

Sessions are used for short-lived request state such as queued alerts. They are
separate from authentication tokens and do not replace login, MFA, or account
verification policy.

## Storage Backend

Local deployments default to cookie storage when no backend is configured:

```toml
[app]
deployment_environment = "local"
```

For staging and production, configure a backend explicitly:

```toml
[app]
deployment_environment = "production"

[wybra.sessions]
storage_backend = "database"
```

Supported values are:

- `cookie`: encrypted and signed cookie storage for small session payloads.
- `memory`: in-process storage for local development only. Sessions are lost on
  restart and are not shared between workers.
- `file`: server-side session files under a configured directory.
- `cache`: storage through a named `wybra.cache` instance.
- `database`: durable Tortoise-backed storage.

## Recommended Production Backends

Use `database` when the application already has a database and session durability
matters:

```toml
[wybra.sessions]
storage_backend = "database"
database_connection_name = "default"
```

Wybra includes the lightweight session table migration in the application
migration graph as core infrastructure. This keeps switching to the `database`
backend operationally straightforward later, even when a deployment currently
uses another backend.

Use `cache` when a named Wybra cache is the operational session store. Include
`wybra.cache`, configure an isolated cache instance, and select it by name:

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
cache_key_prefix = "wybra:sessions:"
```

Sessions require only the baseline expiring byte key/value cache contract. The
session record lifetime is used as the cache entry TTL. Use an independent
Redis database or namespace when session capacity and eviction policy must be
isolated from application caching.

To use the cache named `default`, configure `[cache]` and omit
`wybra.sessions.cache_name`:

```toml
[app]
modules = [
  "wybra.cache",
]

[cache]
backend = "memory"

[wybra.sessions]
storage_backend = "cache"
```

The memory backend is process-local: sessions are not shared between workers or
application instances and are lost on restart. Use it for local development,
tests, or a deliberately single-process deployment.

### Legacy Cache URL Migration

The previous module-owned URL remains available for one compatibility window:

```toml
[wybra.sessions]
storage_backend = "cache"
cache_url = "redis://localhost:6379/0"
```

This path emits a `DeprecationWarning` and logs an operator-visible warning at
startup. Do not configure `cache_name` and `cache_url` together; Wybra rejects
that combination as ambiguous. Migrate the URL into `[cache]` or
`[cache.<name>]`, include `wybra.cache`, and select the named instance from
`[wybra.sessions]`.

`cache_name` and `cache_url` are ignored, with a startup warning, when
`storage_backend` selects a non-cache backend.

Named caches use provider-neutral owner and key namespacing. Moving from the
legacy URL path may therefore invalidate existing sessions even when the new
cache points to the same provider partition. Plan the migration as a session
rotation unless the physical key layout has been verified.

Use `file` only when a single host or shared filesystem is appropriate:

```toml
[wybra.sessions]
storage_backend = "file"
file_directory = ".wybra/sessions"
```

Use `cookie` only for small, non-sensitive payloads that can safely travel with
each request:

```toml
[wybra.sessions]
storage_backend = "cookie"
cookie_payload_max_bytes = 3800
```

Cookie storage uses Wybra's secret-envelope key material. For environment-based
fallback, configure `WYBRA_SECRET_KEY`. For rotation, use keychain-backed
`[secrets.crypto]` storage. See [`ENV_VARS.md`](ENV_VARS.md),
[`SECRET_KEY.md`](SECRET_KEY.md), and [`SECRET_ROTATION.md`](SECRET_ROTATION.md).

For local unconfigured deployments, Wybra generates process-local cookie
encryption material. Local session cookies therefore do not survive application
restarts unless you configure `WYBRA_SECRET_KEY`.

## Lifetime And Cookie Settings

The default session lifetime is 14 days. Configure it in seconds:

```toml
[wybra.sessions]
lifetime_seconds = 1209600
```

Cookie settings can also be adjusted:

```toml
[wybra.sessions]
cookie_name = "wybra_session"
cookie_path = "/"
cookie_domain = "example.com"
cookie_secure = true
cookie_same_site = "lax"
```

If `cookie_secure` is not configured, Wybra uses insecure cookies only for the
`local` deployment environment and secure cookies elsewhere.

See [`ENV_VARS.md`](ENV_VARS.md) for session environment overrides such as
`SESSIONS_STORAGE_BACKEND`, `SESSIONS_COOKIE_SECURE`, and
`SESSIONS_CACHE_NAME`.

## Payload Limits

Wybra validates stored session payloads as JSON-serialisable mappings and
rejects oversized payloads.

```toml
[wybra.sessions]
payload_max_bytes = 65536
cookie_payload_max_bytes = 3800
```

`payload_max_bytes` applies to server-side storage. `cookie_payload_max_bytes`
applies to the final encrypted cookie value for the `cookie` backend.

## Validation

Run:

```sh
uv run wybra-validate sessions
```

Full validation also includes sessions because they are core infrastructure:

```sh
uv run wybra-validate
```

## Custom Session Storage

Applications with specialised storage requirements may provide a compatible
session storage implementation during site setup. The replacement storage must
support asynchronous `load`, `save`, `delete`, `validate`, `cleanup`, and
`close` operations and must preserve Wybra's serialisable session data and
lifecycle metadata contract.

Request handlers and modules still use `request.session`; storage details should
not be exposed to page, API, or alert code.
