# Wybra Messages

`wybra.messages` provides queued user-facing alerts for outcomes that need to
survive a redirect and display on the next rendered page.

## Enable The Module

Add `wybra.messages` to the configured modules list. For the default widget
layout, keep `wybra.template`, `wybra.assets`, and `wybra.widgets` configured
as usual.

```toml
[app]
modules = [
  "wybra.assets",
  "wybra.template",
  "wybra.widgets",
  "wybra.messages",
]
```

## Storage Backends

The default storage backend is `session`.

```toml
[wybra.messages]
storage_backend = "session"
```

Session storage uses the current request's `request.session` mapping, which is
provided by Wybra's core session middleware. If alert code runs without a
request session, Wybra raises a clear messages error instead of silently losing
alerts.

Cache storage resolves a named cache from `wybra.cache` and requires its
`AtomicCacheCapability`. Omit `cache_name` to use the `default` cache:

```toml
[app]
modules = [
  "wybra.cache",
  "wybra.messages",
]

[cache]
backend = "memory"

[wybra.messages]
storage_backend = "cache"
cache_key_prefix = "wybra:messages:"
```

Use an isolated cache when queued alerts need a separate backend or partition:

```toml
[cache.messages]
backend = "memory"

[wybra.messages]
storage_backend = "cache"
cache_name = "messages"
```

Startup fails before serving requests when the cache module is absent, the
selected name is missing, or the selected backend does not provide
`AtomicCacheCapability`. The in-memory provider supports this feature. The
current named Redis provider does not yet provide advanced atomic features.

The legacy `cache_url` setting remains temporarily available for direct memory
or Redis storage, but it is deprecated and cannot be combined with
`cache_name`:

```toml
[wybra.messages]
storage_backend = "cache"
cache_url = "redis://localhost:6379/0"
```

When the selected named-cache backend provides `AtomicCacheCapability`, move the
URL into `[cache]` or a named `[cache.<name>]` section and select that cache with
`cache_name`. Redis deployments must retain the deprecated `cache_url` until
the named Redis provider supplies atomic features. The legacy Redis path retains
whole-queue acknowledgement, so alerts appended concurrently with
acknowledgement can be removed before they are displayed. The named-cache owner
namespace also changes the physical keys used for queued alerts, so pending
alerts stored through the legacy URL are not migrated and may be cleared during
the changeover.

Database storage stores alerts in Wybra-managed persistence. Configure
`wybra.db` and run migrations before using it.

```toml
[wybra.messages]
storage_backend = "database"
database_connection_name = "default"
```

## Queue Settings

```toml
[wybra.messages]
queue_depth = 20
message_max_length = 1000
message_ttl_seconds = 86400
```

`queue_depth` limits stored alerts per request queue. When the queue exceeds
that depth, the oldest alerts are discarded. `message_ttl_seconds` controls how
long cache and database alerts remain eligible for display.

Named cache storage keeps one queue in an atomic cache value. Configuration
fails when the worst-case serialised queue could exceed the cache feature's
1 MiB payload limit; reduce `queue_depth` or `message_max_length` in that case.

When `wybra.messages` uses session storage and `wybra.sessions` uses cookie
storage, queued alerts are stored in the session cookie. The default
`queue_depth` and `message_max_length` are deliberately generous and can exceed
the default cookie payload limit if many long alerts are queued. For cookie
sessions, keep alert messages short, reduce `queue_depth` or
`message_max_length`, or use a server-side session backend such as `file`,
`cache`, or `database`.

## Severities

Supported alert severities are:

- `success`
- `warning`
- `error`

Messages are stored and rendered as plain text. Raw HTML alert content is not
supported.

## Adding Alerts

Route handlers should use the messages capability instead of touching storage
keys directly.

```python
from wybra.messages import MessagesCapability
from wybra.site import get_site


messages = get_site(request.app).require_capability(MessagesCapability)
await messages.success(request, "Settings saved.")
```

Convenience helpers are available for `success`, `warning`, and `error`, or use
`add_alert(request, severity, message)` for a dynamic severity.

## Form Post Messages

Form post handlers can declare success and failure messages instead of looking
up the optional messages capability in every route.

```python
from wybra.forms import FormPostHandler


class SettingsPostHandler(FormPostHandler[SettingsForm]):
    success_message = "Settings saved."
    failure_message = "Settings could not be saved."

    async def commit(self, request, form):
        await save_settings(form.values)
```

After a valid form is committed without validation errors, the handler queues a
`success` alert when `wybra.messages` is configured. When form validation fails,
the handler queues an `error` alert. If `wybra.messages` is not configured, the
same handler continues without adding alerts.

Override `get_success_message()` or `get_failure_message()` when the message
needs to be computed from the submitted form or request.

## Template Context

When `wybra.messages` is configured, template context includes:

- `messages_enabled`: `True` when the module is configured.
- `alerts`: an iterable collection of alert records.
- `has_alerts`: `True` when alerts are available.

Each alert record exposes:

- `severity`
- `message`
- `created_at`

Template context peeks at queued alerts without immediately removing them. The
alerts are acknowledged and removed only when the alert collection is rendered
or otherwise inspected by the template. Routes that redirect, return JSON, serve
files, or render fragments without touching alert context leave the queued
alerts available for the next page that renders them.

## Templates And Styles

The default component is:

```jinja
{% include "components/alerts.html" ignore missing %}
```

The default stylesheet is:

```jinja
<link href="{{ asset_url('styles/messages.css') }}" rel="stylesheet">
```

Applications can override `components/alerts.html` and `styles/messages.css`
through normal module template/static precedence. CSS hooks are provided for
header, under-header, footer, sticky, closable, and timed display variants.
