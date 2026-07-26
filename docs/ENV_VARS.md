# Environment Variables

Wybra reads environment variables through the central configuration service.
Environment values override application configuration for fields that declare an
environment override.

Secret-store keys are not environment variable names. Keychain, Vault, KMS, and
other secret-source keys are ordinary configured strings and may use application
specific paths such as `secrets/key/dev/current` or
`auth/forms/csrf-token-secret/dev/current`.

Default keychain keys are:

| Name | Key | Description |
| --- | --- | --- |
| `secret` | `secrets/key/current` | Current system secret-envelope key material. |
| `secret-prev` | `secrets/key/previous` | Previous system secret-envelope key material used during rotation. |
| `csrf` | `auth/forms/csrf-token-secret/current` | Current forms CSRF token signing secret. |
| `csrf-prev` | `auth/forms/csrf-token-secret/previous` | Previous forms CSRF token signing secrets used during rotation. |
| `google` | `auth/providers/google/client-secret` | Google OAuth client secret. |
| `github` | `auth/providers/github/client-secret` | GitHub OAuth client secret. |
| `apple` | `auth/providers/apple/private-key` | Apple Sign in private key. |

Development deployments should override those defaults to development-scoped
keys such as `secrets/key/dev/current`,
`auth/forms/csrf-token-secret/dev/current`, and
`auth/providers/google/dev/client-secret` when a shared operator keychain also
contains production values.

## Secrets And Key Material

| Name | Description |
| --- | --- |
| `WYBRA_SECRET_KEY` | Environment fallback for the system secret-envelope key material used by Wybra encryption helpers and cookie-backed sessions. This is a single fallback value and is not automatically rotated. Use keychain-backed `[secrets.crypto]` storage for current/previous rotation. |
| `CSRF_SECRET_KEY` | Environment fallback for the forms CSRF token signing secret. A configured keychain CSRF secret takes precedence when it resolves successfully. |
| `CSRF_SECURE` | Boolean override for the CSRF cookie `Secure` flag. This is not secret key material. |

Configured secret-source keys may also point at environment variables:

| Name | Description |
| --- | --- |
| Configured `[secrets.crypto].current_key` | When `[secrets.crypto].source = "environment"`, this names the environment variable that contains current system secret-key material. If omitted with environment source, Wybra uses `WYBRA_SECRET_KEY`. |
| Configured `[secrets.crypto].previous_keys` | When explicitly configured with environment source, this names the environment variable containing comma-separated previous system secret-key material. Rotation is not automatic for environment values. |
| Configured provider `client_secret_key` | When an OAuth provider has `secrets = "environment"`, this names the environment variable containing the provider client secret. |
| Configured Apple `private_key_secret_key` | When Apple auth has `secrets = "environment"`, this names the environment variable containing the Apple private key. |
| Configured database credential keys | When `[app.database].credential_source = "environment"`, `user_key`, `password_key`, `sa_user_key`, and `sa_password_key` name the environment variables that contain database credentials. |
| Arbitrary environment secret keys | The `environment` secret source can resolve any configured key from the process environment. |

## Application Startup

| Name | Description |
| --- | --- |
| `APP_ROOT` | Overrides the application project root used when resolving relative config paths and project resources. |
| `APP_CONFIG` | Path to the application TOML config file. Relative values are resolved against `APP_ROOT` or the current working directory. |
| `APP_ENV` | Overrides `[app].deployment_environment`. Supported values are the normal Wybra deployment environments such as `local`, `staging`, and `production`. |
| `APP_DEBUG` | Boolean override for app debug mode and Wybra runtime diagnostics availability. |

## Database And Migrations

| Name | Description |
| --- | --- |
| `DATABASE_URL` | Runtime database URL override. When set, it overrides configured database connection settings. |
| `MIGRATIONS_ROOT` | Overrides the configured migrations root used by database tooling. |

## Cache

| Name | Description |
| --- | --- |
| `WYBRA_CACHE_BACKEND` | Overrides the backend for the root `[cache]` instance named `default`. |
| `WYBRA_CACHE_URL` | Overrides the Redis URL for the root `[cache]` instance. |
| `WYBRA_CACHE__<NAME>__BACKEND` | Overrides the backend for the existing `[cache.<name>]` instance only. The cache name is upper-cased. |
| `WYBRA_CACHE__<NAME>__URL` | Overrides the Redis URL for the existing `[cache.<name>]` instance only. The cache name is upper-cased. |

## Authentication

| Name | Description |
| --- | --- |
| `ACCOUNT_CREATION_POLICY` | Overrides the account creation policy. |
| `PROVIDER_ENABLED` | Boolean override for external provider authentication. |
| `PASSKEY_ENABLED` | Boolean override for passkey authentication. |
| `RESET_SECRET` | Overrides the reset-password token secret. |
| `VERIFICATION_SECRET` | Overrides the email verification token secret. |
| `SESSION_COOKIE` | Overrides the authentication session cookie name. This is separate from request-session cookies. |
| `SESSION_FORCE_SECURE` | Boolean override for authentication session cookie secure handling. |
| `SESSION_LIFETIME` | Overrides authentication session lifetime in seconds. |
| `TOTP_MODE` | Overrides TOTP policy mode. |
| `TOTP_ALLOWED_DRIFT` | Overrides the accepted TOTP clock drift. |
| `TOTP_PERIOD_SECONDS` | Overrides the TOTP period length in seconds. |
| `TOTP_CHALLENGE_EXPIRY_SECONDS` | Overrides TOTP challenge expiry in seconds. |
| `TOTP_RECOVERY_WINDOW_SECONDS` | Overrides the TOTP recovery-code window in seconds. |

## Sessions

| Name | Description |
| --- | --- |
| `SESSIONS_STORAGE_BACKEND` | Overrides request-session storage backend. |
| `SESSIONS_LIFETIME_SECONDS` | Overrides request-session lifetime in seconds. |
| `SESSIONS_COOKIE_NAME` | Overrides request-session cookie name. |
| `SESSIONS_COOKIE_DOMAIN` | Overrides request-session cookie domain. |
| `SESSIONS_COOKIE_PATH` | Overrides request-session cookie path. |
| `SESSIONS_COOKIE_SECURE` | Boolean override for request-session cookie secure handling. |
| `SESSIONS_COOKIE_SAME_SITE` | Overrides request-session cookie SameSite policy. |
| `SESSIONS_FILE_DIRECTORY` | Overrides the file-session storage directory. |
| `SESSIONS_CACHE_NAME` | Overrides the named Wybra cache selected for cache-backed sessions. |
| `SESSIONS_CACHE_URL` | Deprecated compatibility override for the module-owned cache/Redis URL. |
| `SESSIONS_CACHE_KEY_PREFIX` | Overrides the cache key prefix for cache-backed sessions. |
| `SESSIONS_DATABASE_CONNECTION` | Overrides the database connection name for database-backed sessions. |
| `SESSIONS_PAYLOAD_MAX_BYTES` | Overrides the maximum server-side session payload size. |
| `SESSIONS_COOKIE_PAYLOAD_MAX_BYTES` | Overrides the maximum cookie-backed session payload size. |

## Messages

| Name | Description |
| --- | --- |
| `MESSAGES_STORAGE_BACKEND` | Overrides queued message storage backend. |
| `MESSAGES_QUEUE_DEPTH` | Overrides the maximum queued message count per queue. |
| `MESSAGES_MESSAGE_MAX_LENGTH` | Overrides the maximum length of an individual message. |
| `MESSAGES_TTL_SECONDS` | Overrides queued message time-to-live in seconds. |
| `MESSAGES_CACHE_URL` | Overrides the cache/Redis URL for cache-backed messages. |
| `MESSAGES_CACHE_KEY_PREFIX` | Overrides the cache key prefix for cache-backed messages. |
| `MESSAGES_DATABASE_CONNECTION` | Overrides the database connection name for database-backed messages. |

## Static Assets, Media, Templates, And API

| Name | Description |
| --- | --- |
| `STATIC_ROOT` | Overrides static asset root directory. |
| `STATIC_SERVE` | Boolean override for serving static assets from the application. |
| `STATIC_URL` | Overrides the static asset URL path. |
| `STATIC_EXPORT_MODE` | Overrides static asset export mode. |
| `MEDIA_ROOT` | Overrides media storage root directory. |
| `MEDIA_MOUNT_PATH` | Overrides media mount path. |
| `MEDIA_SERVE` | Boolean override for serving media from the application. |
| `MEDIA_URL_MODE` | Overrides media URL mode. |
| `TEMPLATE_ROOT` | Overrides template root directory. |
| `REQUEST_CONTEXT_ENABLED` | Boolean override for template request-context support. |
| `API_PATH_PREFIX` | Overrides API path prefix. |
| `API_PAGING_LINK_MODE` | Overrides API paging link mode. |

## Diagnostics

| Name | Description |
| --- | --- |
| `WYBRA_EVENT_DELIVERY_ENABLED` | Boolean override for the core in-process event service, disabled by default. |
| `WYBRA_EVENTS_ENABLED` | Boolean override for runtime event collection. |
| `WYBRA_EVENTS_FILTER` | Comma-separated diagnostic event selectors, defaulting to `sql,template,events.errors`. |
| `WYBRA_DIAG_ENABLED` | Boolean override for the disabled-by-default debug WebSocket endpoint. |
| `WYBRA_DIAG_ALLOWED_HOSTS` | Comma-separated allowed effective client peers for the debug WebSocket endpoint. Trusted proxy normalisation must be configured in Uvicorn; raw forwarded headers are never trusted by Wybra. |
| `WYBRA_DIAG_RETENTION_LIMIT` | Maximum retained completed diagnostic snapshots per process. |
| `WYBRA_DIAG_SUBSCRIPTION_QUEUE_LIMIT` | Maximum pending diagnostic notifications per subscriber. |
| `WYBRA_DIAGNOSTICS_LEVEL` | Overrides diagnostics verbosity. |
| `WYBRA_DIAGNOSTICS_LOGGING_BRIDGE` | Boolean override for forwarding diagnostics into logging. |
| `WYBRA_DIAGNOSTICS_SLOW_SQL_SECONDS` | Overrides the slow-SQL diagnostics threshold. |

## Runserver Reload

| Name | Description |
| --- | --- |
| Configured `[app.runserver].reload_env` | Names the environment variable that `wybra-runserver` checks when deciding whether reload is enabled, unless the CLI `--reload` or `--no-reload` option is supplied. This name is app-configured rather than hard-coded. |
