# Wybra

[![Build Status](https://img.shields.io/github/actions/workflow/status/Uniquode/wybra/tests.yml?branch=main&label=tests&logo=github)](https://github.com/Uniquode/wybra/actions/workflows/tests.yml)
[![Security](https://img.shields.io/github/actions/workflow/status/Uniquode/wybra/codeql.yml?branch=main&label=security&logo=github)](https://github.com/Uniquode/wybra/security/code-scanning)
[![Maintenance](https://img.shields.io/badge/maintenance-active-brightgreen.svg)](https://github.com/Uniquode/wybra)
[![PyPI version](https://img.shields.io/pypi/v/wybra.svg?logo=pypi&logoColor=white)](https://pypi.org/project/wybra/)
[![PyPI downloads](https://img.shields.io/pypi/dm/wybra.svg?logo=pypi&logoColor=white)](https://pypi.org/project/wybra/)
[![Python versions](https://img.shields.io/pypi/pyversions/wybra.svg?logo=python&logoColor=white)](https://pypi.org/project/wybra/)

<p align="center">
  <img src="logo.svg" alt="wybra" width="160"/>
</p>

`wybra` is a reusable async FastAPI framework layer. It provides web
composition, database and migration helpers, project command adapters, and
reusable local authentication building blocks.

The name follows attested Bundjalung and neighbouring dialect forms including
`wybra`, `wibra`, `wybera`, and `waybara`, associated with fire, firewood, or
wood.

Repository: <https://github.com/Uniquode/wybra>

## Package Areas

- `wybra.core`: module composition, route management, package resource helpers,
  settings loading, diagnostics, and shared conventions.
- `wybra.views`: developer-facing view base classes, plain HTML view helpers,
  template view helpers, API view helpers, and paging/result helper types.
- `wybra.assets`: static asset settings, source discovery, runtime serving,
  URL resolution, collection, and validation.
- `wybra.template`: template settings, source discovery, rendering capability,
  context construction, and template validation.
- `wybra.cache`: optional named cache registry with a common byte-store
  baseline, typed advanced memory features, and Redis-backed baseline, atomic,
  counter, lease, fencing, durable work-queue, durable stream, live pub/sub,
  revisioned schedule, and provider-calibrated time support, including Jinja
  template-fragment caching.
- `wybra.forms`: form settings, CSRF protection, request form parsing, form
  safety helpers, form response finalisation, and forms validation.
- `wybra.security`: web-facing security policy, COOP/security headers, CORS
  policy data, middleware setup, and security validation.
- `wybra.errors`: exception handler registration, error classification,
  safe fallback responses, renderer coordination, and error validation.
- `wybra.api`: API request classification, response formatting, error payloads,
  HATEOAS-style paging metadata, streaming responses, and API validation.
- `wybra.db`: Tortoise ORM configuration, async database helpers, database URL
  handling, and native migration command support.
- `wybra.scopes`: provider-neutral declarative scope metadata, access decisions,
  protected-view enforcement, visibility, discovery, and catalogue validation.
- `wybra.tasks`: provider-neutral async task declarations, validated payloads,
  direct execution, optional submission capability, retries, lifecycle status,
  and the infrastructure-free immediate backend.
- `wybra.secrets`: runtime secret lookup from consumer-selected sources,
  including environment variables, AWS Secrets Manager, OS keychains, and Vault.
- `wybra.tools`: generic project command adapters and validation target
  discovery. Host applications provide concrete runtime settings through their
  app config.
- `wybra.auth`: local identity models, browser auth routes, auth templates,
  password policy, group/scope administration, and the `wybra-authmgr`
  operator CLI.

## Local Development

Use `uv` for dependency management and command execution:

```sh
uv sync
uv run pytest
uv run ruff format --check src tests
uv run ruff check src tests
uv run ty check src/
uv build
```

Windows compatibility is validated in CI alongside Linux. See
[`docs/WINDOWS.md`](docs/WINDOWS.md) for the Windows runner coverage, local
check commands, SQLite URL guidance, and optional OS-service boundaries.

See [`docs/CACHE.md`](docs/CACHE.md) for cache configuration and safe Jinja
fragment-caching guidance.

See [`docs/SCOPES.md`](docs/SCOPES.md) for declarative model, view, and endpoint
scope policy, optional auth integration, discovery, and catalogue validation.

See [`docs/TASKS.md`](docs/TASKS.md) for task declarations, direct and
on-demand execution, retries, idempotency, status feedback, and the current
availability of durable background and scheduled execution.

The framework project does not contain host application settings, `app.toml`,
or change-management artifacts. Host-facing commands resolve the configured
application through the host app config file selected by `--config`,
`APP_CONFIG`, or the default `app.toml`.

## Application Startup

Host applications own their FastAPI instance. Wybra owns the common engine setup,
including configured route registration, behind the FastAPI lifespan hook:

```python
from fastapi import FastAPI
import wybra

app = FastAPI(
    title="example",
    lifespan=wybra.start_site(config_source="app.toml"),
)
```

Configured modules expose one async setup hook at their package root:

```python
from wybra import Site


async def setup_site(site: Site) -> None: ...
```

Startup calls configured module hooks in `app.toml` order. Modules use
type-keyed capabilities for shared services rather than importing another
module's implementation details. When a module depends on a capability that may
be provided later in the configured module list, keep the setup order
independent by storing a capability proxy in `setup_site(...)` and finalising it
in `post_setup_site(...)`:

```python
from wybra.db import DatabaseCapability


async def setup_site(site: Site) -> None:
    database = site.capability_proxy(DatabaseCapability)
    ...


async def post_setup_site(site: Site) -> None:
    await database.finalise_required()
```

`post_setup_site(...)` is an optional async hook that runs only after every
configured module has completed `setup_site(...)`. Use it for final composition
checks: hard dependencies bind to real capabilities or fail startup, while soft
dependencies can be awaited with `finalise_optional()` and handled by the
consuming module's fallback behaviour.

Runtime modules should expose behaviour through Wybra-owned capabilities and
domain-focused persistence interfaces where that keeps call sites clear.
Tortoise models, queries, and transactions belong inside the modules that own
the affected data rather than in host-facing capability contracts.

Current hard dependencies include auth, media, and profile data access, auth on
forms for protected browser form routes, widgets on templates, widgets on forms
for theme form routes, and routes that explicitly require template rendering.
Soft dependencies include profile images on media, templates on assets for
`asset_url(...)`, and widgets on auth/profile enrichment.

`wybra.widgets` includes small reusable navigation and dropdown primitives for
server-rendered controls:

```python
from wybra.widgets import DropdownPanel, NavigationItem, NavigationMenu


settings_menu = DropdownPanel(
    label="Settings",
    id="account-settings-menu",
    menu=NavigationMenu(
        label="Settings",
        items=(
            NavigationItem(label="Profile", path="/profile"),
            NavigationItem(label="Login & Security", path="/account"),
        ),
        shortcut_scope="settings-menu",
    ),
)
```

Shortcut metadata is carried with the rendered menu for local panel behaviour,
but Wybra does not install a global keyboard shortcut dispatcher.

Auth is exposed through `AuthCapability`, so applications can depend on public
helpers rather than auth internals:

```python
from fastapi import Depends
from wybra.auth import login_required


@router.get("/admin", dependencies=[Depends(login_required)])
async def admin_page(): ...
```

App-side Wybra database, auth, route, template, static, or runtime-state setup
is not supported. Configure modules and settings once, then use
`wybra.start_site(...)` to initialise the Wybra-owned concerns.

### Login And Security Page

`wybra.auth` registers an authenticated `auth:security` account route at
`/account/security` when the auth browser routes are configured. The page is a
capability-backed shell: it renders the authenticator section only when TOTP is
enabled by auth settings, links inactive users to the existing authenticator
setup flow, and shows active users controls for disabling authenticator
verification or generating replacement recovery codes.

Disabling authenticator verification and replacing recovery codes are protected
POST actions. Each action first renders a confirmation page and accepts one of
the user's existing confirmation methods: password, active authenticator code,
or recovery code. Recovery-code replacement invalidates previous unused codes
and displays the generated replacement codes only once. Provider and passkey
management controls are intentionally left to later feature iterations.

## Declarative Forms

`wybra.forms` provides CSRF protection and a small declarative forms API.
Declare fields as class variables; field names are inferred from the variable
name and labels default to that name:

```python
from wybra.forms import ChoiceField, FileUploadField, Form, TextAreaField, TextField

PRONOUNS = {
    "she|her": "she/her",
    "he|him": "he/him",
}


class ProfileForm(Form):
    preferred_name = TextField(max_length=64)
    bio = TextAreaField(max_length=1024, required=False)
    pronouns = ChoiceField(choices=PRONOUNS, required=False)
    avatar = FileUploadField(required=False)

    async def validate(self, field_name: str | None = None) -> bool:
        inherited = await super().validate(field_name)
        local = True
        if field_name == "preferred_name" and self.values.get(field_name) == "admin":
            self.add_error(field_name, "This preferred name is reserved.")
            local = False
        if field_name is None and not self.values.get("preferred_name"):
            self.add_error(None, "Profile details are incomplete.")
            local = False
        return inherited and local
```

Forms can be constructed with defaults and explicit field values, then parsed
back into structured results containing raw submitted values, parsed values,
field errors, and form-level validity. Form subclasses can add asynchronous
validation with `async validate(field_name: str | None = None) -> bool`; `None`
is used for form-level validation and form-level errors are stored in
`errors[None]`.
Wybra intentionally does not support Django-style `clean` or
`clean_<field_name>` hooks.

Every declared field participates in parsing, even when the browser omits its
control. An omitted optional checkbox or switch becomes `False`, an omitted
optional multi-select becomes `()`, and another omitted optional field becomes
`None`. Required checkboxes and switches require an affirmative submission.
This makes an unchecked control reliably clear a previously stored value.

Text-like fields are plain text by default. `TextField`, `TextAreaField`, and
`HiddenField` reject HTML or markup-like input and unsafe control characters
unless declared with `allow_html=True`. That option only disables input-side
markup rejection for fields that intentionally accept HTML; it does not make
rendered output trusted or replace template escaping. Rich text sanitisation,
URL scheme validation, and other domain-specific safety checks remain owned by
the consuming feature.

```python
form = ProfileForm(values={"preferred_name": "David"})
result = await form.parse(await forms.request_form_data(request))
if result.is_valid:
    values = (await form.save()).primary
```

Pass `target=` to bind a plain form to a dataclass or ordinary object. Without
a target, `save()` returns a `SaveResult` whose `primary` value is a
dictionary-shaped value containing the writable declared fields. Invalid input
never mutates a supplied target.

Template helpers can render a complete form, an individual field inside a
custom application-owned form, or an explicit CSRF hidden field. Complete form
rendering includes configured fields, labels, errors, CSRF, and action widgets;
field rendering uses the same widget/component templates and override
semantics. Forms containing `FileUploadField` render with
`multipart/form-data` by default.

### Model Forms

`ModelForm` layers declarative field binding and writer-bound Tortoise
persistence onto `Form`. Construct it with the application's selected
`DbConnection`; it retains one writer route for relation loading, validation,
create, update, and delete operations.

```python
from wybra.forms import JsonField, ModelForm, TextAreaField, TextField


class ProfileForm(ModelForm):
    preferred_name = TextField(max_length=64)
    bio = TextAreaField(max_length=1024, required=False)
    website_links = JsonField(required=False)
    created_by = TextField(disabled=True, required=False)

    class Meta:
        model = Profile
```

Fields always bind to same-named record attributes. Field classes adapt a
stored value for presentation and a parsed value for persistence; `JsonField`
accepts a complete JSON object or array. Use `disabled=True` or
`FormFieldOptions(editable=False)` for non-editable controls.

For presentation, a field can supply a renderer directly, or a form can map
existing fields to renderer objects through `Meta.renderers`. Explicit field
renderers take precedence; otherwise Wybra uses the field's standard widget
renderer.
`Meta.fields` is an explicit allowlist; recognised Tortoise fields are generated
when named there, while declared fields override their presentation.

```python
form = ProfileForm(instance=profile, connection=connection)
await form.parse(await forms.request_form_data(request))
if form.is_valid():
    saved = await form.save()
    profile = saved.primary
```

`save()` and `delete()` return backend-neutral `SaveResult` values. They expose
the primary and original objects, changed fields, operation flags, and an
affected-record count. An unchanged existing record returns a successful no-op
result without an update. `VersionField` opts a model into atomic stale-update
detection; a conflict adds a form-level error without overwriting stored data.
Include the generated or declared hidden version field in a versioned form's
`Meta.fields` and submit its rendered value; it is a concurrency token, not an
editable model value. Wybra's `PositiveIntField` model base accepts the initial
version `0`, because the installed Tortoise release does not provide that field
itself.
Wybra's normal `makemigrations` command automatically generates a named native
database check constraint for every `VersionField`, so direct SQL and other
non-ORM writers cannot store a negative version either. No additional model
metadata or migration editing is required.
Unversioned models remain supported with last-write-wins semantics. Override
the asynchronous `deletion_action(instance)` hook to return `"soft"` after
marking an instance for application-defined soft deletion; physical deletion
is the default.

For a generated relation field, use `Meta.form_options` rather than a
Tortoise-field subclass. Its optional asynchronous `relation_query`,
`relation_value`, and `option_format` callables scope a paged option query,
authorise and resolve a submitted value, and produce each plain-text option
label. When omitted, Wybra uses the writer route to load records, resolves
primary keys, and formats options with `str(record)`.

`CompositeForm` is one transactional fixed-member form. Its ordered
`Meta.models` tuple can infer unique forward relations from its final primary
model; generated related fields use names such as `address__street`. Declared
fields override generated controls. Repeated or M2M collection members require
a future formset API and are rejected by this fixed-member form.
Fixed members can only use the declared relation chain; independent relation
selectors on a member are not supported by `CompositeForm` yet.

### Phone Contact Widgets

Phone contact entry is a compound form concern: country, subdivision, and phone
number are submitted as separate fields, but they must be rendered and validated
together. Wybra's reusable phone contact widget is intended to make the common
case painless while still allowing applications to apply their own business
filters.

The recommended application-facing shape is a phone contact control that
sources countries and subdivisions from Wybra's standard country data, applies
optional filters, validates submitted values against the filtered options, and
normalises the phone number using the selected country:

```python
from wybra.forms import Form, PhoneContactControl, SelectField, TextField, field_handler


def delivery_country_filter(country):
    return country.code in {"AU", "NZ"}


def delivery_subdivision_filter(subdivision, country):
    return subdivision.code in allowed_delivery_subdivisions(country.code)


class DeliveryAddressForm(Form):
    delivery_country = SelectField(label="Country", required=False)
    delivery_region = SelectField(label="State or region", required=False)
    delivery_phone = TextField(label="Phone number", required=False)

    phone_contact = PhoneContactControl(
        country_field="delivery_country",
        subdivision_field="delivery_region",
        phone_field="delivery_phone",
        handlers=(
            field_handler(
                "/phone-contact/fields",
                name="phone-contact-fields",
                methods={"GET"},
            ),
        ),
        country_filter=delivery_country_filter,
        subdivision_filter=delivery_subdivision_filter,
    )
```

With no filters, the control should expose all supported countries and all
subdivisions for the selected country. With filters, rendering and validation
must agree: a country or subdivision omitted by the filter is not a valid
submitted value. Phone number validation and normalisation are then performed
against the validated country, without requiring the application to duplicate
that logic.

The field handler declares the HTMX fragment endpoint the control needs. The
handler is metadata until the form is attached to a router/application; route
registration applies the application's routing and security policy.

Templates can render through the declared control:

```jinja
{{ render_phone_contact(
  form,
  control=form.phone_contact,
  target_id="delivery-phone-fields"
) }}
```

The widget owns the coordinated rendering, country-change refresh hook, prefix
display, disabled state, and field error placement. Applications own business
filters and persistence. Verification ceremonies, such as SMS, voice, or email
fallback verification, are capability-owned workflows layered on top of the
validated and normalised phone contact.

## Project Commands

Wybra publishes prefixed console scripts to avoid collisions with host
application or environment-specific tooling:

- `wybra-runserver`: start the configured ASGI application with Uvicorn.
- `wybra-task-worker`: run the configured cache-backed Taskiq worker.
- `wybra-migrate`: run native Tortoise migrations for the configured
  application.
- `wybra-collect`: collect configured module static assets for deployment.
- `wybra-routes`: inspect the configured application's installed route tree.
- `wybra-validate`: run configured project validation targets.
- `wybra-authmgr`: manage local identity users, scopes, and groups.
- `wybra-secret`: manage Wybra-known secrets in the OS keychain.

Host applications may add their own short aliases when appropriate, but the
portable package-owned command names are the `wybra-*` commands.

`wybra-runserver` reads the configured Uvicorn app target from
`[app.runserver].asgi_app` in the selected app config file. The reload
environment variable is configured with `[app.runserver].reload_env`.

By default, Wybra uses the current project root and `app.toml` in that project
root for application startup. Runtime overrides are passed through the same
startup configuration channel used by ASGI startup:

- `--project` sets `APP_ROOT` and is the only CLI option that changes the
  effective project root.
- `--config` sets `APP_CONFIG` and selects the application config file without
  changing the project root.
- `--database-url` sets `DATABASE_URL` for database, auth, validation, and
  migration consumers.
- `--deploy` sets `APP_ENV`, which is used as the deployment-environment
  override for this invocation.

Runtime override precedence is CLI override, then environment variable, then app
config default, then built-in default. For deployment environment, this is
`--deploy`, then `APP_ENV`, then `[app].deployment_environment`, then `local`.
Relative config paths and relative SQLite database paths are resolved from the
effective project root.

```toml
[app.runserver]
asgi_app = "example_app.asgi:app"
reload_env = "APP_RELOAD"
```

## Static Asset Collection

`wybra.assets` owns static asset settings, source discovery, runtime serving,
URL resolution, collection, and validation. Wybra can collect the static assets
for the configured application into the filesystem tree configured by
`[app.assets].root`. Collection output is deployment/export output; it does not
become the runtime source of app-served static files:

```sh
uv run wybra-collect --config config/app.toml
```

Collection uses the same configured module order and static asset precedence as
runtime serving. Runtime app-served static handling still serves the configured
module static sources directly, so local development sees the source assets
rather than a previously collected tree. Unchanged files are skipped, copied
files preserve metadata, and Wybra-managed stale files under the asset root are
deleted by default so the collected tree matches the configured asset set.

Use `--dest` for a one-off collection destination:

```sh
uv run wybra-collect --config config/app.toml --dest build/static
```

Use `--no-delete` when stale files should be retained for a diagnostic or
staged deployment run:

```sh
uv run wybra-collect --config config/app.toml --no-delete
```

During local development, keep app-served static handling enabled so Wybra
serves the configured module static sources:

```toml
[app.assets]
url_path = "/static/"
root = "static"
export_mode = "normal"
serve = true
```

For deployments where nginx or another front end serves collected assets
directly, keep the URL path aligned and disable the ASGI static mount:

```toml
[app.assets]
url_path = "/static/"
root = "static"
export_mode = "normal"
serve = false
```

`export_mode = "normal"` is the default and performs a direct collection to the
configured asset root. Manifest collection is a separate mode and backend.

When nginx serves collected assets directly, Wybra runtime middleware cannot
apply asset CORS headers. Configure `wybra.security` and ask collection to write
an nginx CORS include for the same asset-serving policy:

```sh
uv run wybra-collect --config config/app.toml --nginx-cors deploy/asset-cors.conf
```

## Template Context

`wybra.template` composes template context as read-only mapping layers. Adding
context creates a newer layer in front of existing parents; lookup uses the
first matching key from newest to oldest, and parent mappings are not mutated.
At the render boundary Wybra flattens the layered context to a plain mapping for
the configured template engine.

Ordinary caller context can override request or provider context by occupying a
newer ordinary layer. Framework-owned render values are applied as a protected
final layer so page data cannot shadow runtime helpers. Reserved render names
are `asset_url`, `request`, `route_name`, `csrf_field_name`,
`csrf_header_name`, and `csrf_token`.

## Migration Workflow

`wybra-migrate` resolves app configuration once, builds a Tortoise
configuration for the configured Wybra modules, and dispatches lifecycle
operations through Tortoise's native migration tooling.

Initialise migration state explicitly:

```sh
uv run wybra-migrate init
uv run wybra-migrate --config config/app.toml init
```

`init` does not provision database infrastructure. Create the database and
application role before running Wybra migrations. After migration state exists,
apply schema migrations with:

```sh
uv run wybra-migrate migrate
```

Inspect migration state without mutating the database:

```sh
uv run wybra-migrate heads
uv run wybra-migrate history
uv run wybra-migrate --config config/app.toml heads
uv run wybra-migrate --config config/app.toml history
```

Create module-owned Tortoise migrations through the project command:

```sh
uv run wybra-migrate makemigrations wybra_auth -n add_identity_field
```

Migration files are placed in the selected module's `migrations/` package. The
normal roll-forward order is to migrate the working database to the current
head, update the owning module's Tortoise models, generate the migration,
review the generated operations, run `wybra-migrate migrate`, then validate.
Use `wybra-migrate sqlmigrate <app_label> <migration>` to inspect the SQL for a
specific migration before applying it.

Generated migration files must never be edited. When a development or test
database can be explicitly reset, `wybra-migrate destroy --confirm <target>
--reset-migrations` removes replaceable migration history only after the
confirmed destroy succeeds; follow it with `wybra-migrate makemigrations` and
`wybra-migrate migrate`. A non-model-generated migration must declare
`not_generated = True` and a non-empty `not_generated_description` on its
`Migration` class. Such a migration stops reset until it has a reviewed,
baseline-compatible provenance plan.

Tests can generate migrations into an isolated root without modifying committed
output: `wybra-migrate makemigrations --module package.models
--migrations-root .wybra-test-migrations`. The test helpers use this path and
remove their temporary output automatically.

## Route Inspection

Inspect the installed route tree:

```sh
uv run wybra-routes
uv run wybra-routes --config config/app.toml
uv run wybra-routes --graph
uv run wybra-routes --mermaid
uv run wybra-routes --json
uv run wybra-routes --check
uv run wybra-routes --check --quiet
```

The route-tree command imports the configured ASGI app target and reports the
final installed FastAPI/Starlette route graph. Use it for route review and for
explicit route smoke checks. Use `--check --quiet` when only the exit status is
needed. It is separate from `wybra-validate`, which remains the broad
project-structure validation command.

## Secrets Configuration

`wybra.secrets` provides a source-selected runtime lookup capability for values
that must not be stored in app configuration. Add it to the configured module
list before modules that validate or use secret references:

```toml
[app]
modules = [
    "wybra.secrets",
    "wybra.forms",
    "wybra.auth",
    "wybra.providers",
]
```

Consumers choose their own source and key reference. There is no global
`[secrets].backend`; mixed deployments can use different sources for different
features:

```toml
[auth.providers.google]
enabled = true
client_id = "google-client-id"
secrets = "keychain"
client_secret_key = "auth/providers/google/client-secret"
```

Source-specific sections hold only lookup metadata, never resolved secret
values:

```toml
[secrets.crypto]
source = "keychain"
current_key = "WYBRA_SECRET_KEY_CURRENT"
previous_keys = "WYBRA_SECRET_KEYS_PREVIOUS"

[secrets.kms]
region_name = "ap-southeast-2"
base_path = "/production/wybra"

[secrets.vault]
url = "https://vault.example.com"
mount_point = "secret"
secrets_path = "apps/wybra"

[secrets.keychain]
appname = "wybra"
```

`[secrets.crypto]` is optional. When it is configured and `wybra.secrets` is
available, Wybra resolves the system secret keyring through the selected
source before parsing the existing `version:base64-key:checksum` key-entry
format. If `[secrets.crypto]` is absent, the crypto service uses
`WYBRA_SECRET_KEY_CURRENT` and `WYBRA_SECRET_KEYS_PREVIOUS` from the resolved
environment. The previous-keys reference is optional; if the selected source
does not contain it, only the current key is loaded.

`wybra.forms` uses a separate CSRF signing secret. Do not reuse
`WYBRA_SECRET_KEY_CURRENT` or another `[secrets.crypto]` key for CSRF tokens.
For non-local deployments, configure a stable CSRF secret through keychain
lookup, with `CSRF_SECRET` or inline `csrf_token_secret` as an optional
fallback:

```toml
[wybra.forms]
csrf_token_secret_source = "keychain"
```

When `csrf_token_secret_source = "keychain"` is configured, `wybra.forms`
attempts to load the canonical forms CSRF storage key
`auth/forms/csrf-token-secret/current` during startup. Set
`csrf_token_secret_key` only when intentionally overriding or migrating that
storage key. If the key cannot be resolved and `CSRF_SECRET` or inline
`csrf_token_secret` is configured, the fallback value is used. In non-local
deployments, startup fails when neither path provides a stable CSRF secret.
Local deployments may still generate a process-local CSRF secret when no stable
value is configured. See `docs/SECRET_KEY.md` for generation and storage
commands.

The `environment` source reads from the resolved process environment and needs
no optional dependency:

```toml
[auth.providers.github]
enabled = true
client_id = "github-client-id"
secrets = "environment"
client_secret_key = "GITHUB_CLIENT_SECRET"
```

External authentication remains deployment-owned. AWS uses the runtime AWS
credential chain, Vault uses deployment-provided Vault connection and token
state, macOS keychain access uses the host Keychain policy, Windows keychain
access uses Windows Credential Manager, and Linux keychain access uses a
Freedesktop Secret Service provider such as GNOME Keyring, KWallet, KeePassXC,
or another Secret Service-compatible provider. `oo7` is a Rust Secret
Service/keyring implementation in this ecosystem, but Wybra does not depend on
it directly. The `keychain` optional dependency uses the Python `keyring`
package as the platform adapter; on Linux, that still requires a D-Bus session
and Secret Service provider at runtime.
Install only the optional driver clients a deployment needs:

```sh
uv add 'wybra[kms]'
uv add 'wybra[keychain]'
uv add 'wybra[vault]'
```

`wybra-secret` manages OS keychain-backed entries through the same Python
`keyring` adapter used by the runtime keychain source. It does not write
environment variables:

```sh
printf '%s' "$GOOGLE_CLIENT_SECRET" \
  | uv run wybra-secret --config config/app.toml set auth/providers/google/client-secret

uv run wybra-secret --config config/app.toml get --json auth/providers/google/client-secret
uv run wybra-secret --config config/app.toml list --json
```

Use `APP_CONFIG=config/app.toml` instead of `--config` when the selected app
config should come from the environment. `list` is a list of known keys, not
platform keychain enumeration: it includes Wybra's built-in crypto key
references and configured keychain-backed references such as the forms CSRF
token secret and enabled external provider client secret keys. External
identity providers are implemented by the opt-in `wybra.providers` module, with
provider authentication configuration under `[auth.providers.<name>]`. WebAuthn
and passkeys are separate future `wybra.passkeys` work, not provider behaviour.

For Linux keychain verification in the repository development container, start
the root `wybra-dev` Compose shell. The container starts commands inside a
D-Bus session, installs GNOME Keyring and `secret-tool`, and persists keyring
files in a Docker volume. Unlock the keyring in the shell before using
`secret-tool` or running Secret Service integration checks:

```sh
docker compose run --rm dev
printf '%s\n' "$WYBRA_KEYRING_PASSWORD" \
  | gnome-keyring-daemon --unlock --components=secrets > /tmp/wybra-keyring-env
. /tmp/wybra-keyring-env

cd /Users/davidn/Code/wybra-dev/wybra
uv sync --extra keychain
uv run pytest tests/test_secrets.py -k linux_secret_service
```

## Auth Configuration

Wybra-hosted applications configure auth through the host application's
`app.toml`. `wybra-authmgr` resolves the same host application config as the
other package-owned project commands, then reads `[auth]` from that file. Use
`--config <path>` to select a specific app config for one invocation:

```toml
[app]
database_url = "sqlite:///app.sqlite3"
modules = [
    "wybra.secrets",
    "wybra.assets",
    "wybra.security",
    "wybra.forms",
    "wybra.errors",
    "wybra.api",
    "wybra.template",
    "wybra.auth",
]

[app.templates]
auto_reload = true
cache_size = 0

[app.assets]
url_path = "/static/"
root = "static"

[auth]
session_cookie_force_secure = false

[auth.password.policy]
minimum_length = 12
minimum_character_categories = 2
minimum_strength = 0.45
common_fragments = [
  "admin",
  "changeme",
  "changeit",
  "letmein",
  "p4ssw0rd",
  "pass",
  "password",
  "qwerty",
  "test",
  "tester",
  "welcome",
]
```

Database selection precedence for auth configuration is `DATABASE_URL`, then
`[app].database_url`.

```sh
uv run wybra-authmgr --config config/app.toml user list
```

## Database Backends

Wybra database URLs use Tortoise-native async backend schemes. SQLite is
available by default because Tortoise includes its `aiosqlite` backend
dependency. Other backends require the matching Wybra optional dependency before
their URL scheme is treated as available by validation or startup.

| Database | URL scheme | Install extra |
| --- | --- | --- |
| SQLite | `sqlite:///app.sqlite3`, `sqlite://:memory:` | built-in |
| PostgreSQL via asyncpg | `postgresql://user:pass@host/db` | `wybra[postgresql]` |
| PostgreSQL via Tortoise asyncpg alias | `postgres://...`, `asyncpg://...` | `wybra[postgresql]` |
| PostgreSQL via psycopg | `psycopg://user:pass@host/db` | `wybra[psycopg]` |
| MySQL | `mysql://user:pass@host/db` | `wybra[mysql]` |
| Microsoft SQL Server | `mssql://user:pass@host/db` | `wybra[mssql]` |
| Oracle | `oracle://user:pass@host/db` | `wybra[oracle]` |

`postgresql://` is the preferred PostgreSQL configuration form. Wybra
normalises it to Tortoise's asyncpg backend internally because Wybra does not
support synchronous database interfaces.
Tortoise can use either `asyncmy` or `aiomysql` for MySQL when installed; Wybra's
packaged MySQL extra currently installs `aiomysql`.
