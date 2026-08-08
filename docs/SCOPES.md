# Declarative Scopes

`wybra.scopes` provides core, provider-neutral capability policy for models,
class-based views, and FastAPI endpoints. Scope declarations add prerequisites
to operations; they do not create routes, enable unavailable actions, or grant
capabilities to users.

The optional `wybra.auth` module supplies group-backed effective grants and the
persisted scope catalogue. Applications that do not use `wybra.auth` may
provide their own `ScopeGrantsCapability` and `ScopeCatalogueCapability`.
Without a grants provider, protected operations evaluate against an empty grant
set and fail closed.

## Model actions

Model positional values select canonical actions and expand through the
model's finalised content type:

```python
from wybra.db.models import Model
from wybra.scopes import scopes


@scopes("update", "delete")
class Article(Model): ...
```

For a content type named `articles.article`, this adds
`articles.article.update` and `articles.article.delete` requirements to the
corresponding operations. It is equivalent to:

```python
@scopes(actions=("update", "delete"))
class Article(Model): ...
```

The standard actions are `list`, `view`, `create`, `update`, `delete`, and
`manage`. A declaration is a partial policy overlay: unlisted actions receive
no derived requirement and retain their existing availability.

Models may also use `Meta.scopes` as an action-only shorthand:

```python
class Article(Model):
    class Meta:
        scopes = True
```

`True` selects every standard action. An inclusive tuple selects named actions,
while an exclusion-only tuple such as `("-delete",)` selects every standard
action except delete. Exclusion means omission, not denial. An empty
`@scopes()` is a no-op. A non-empty decorator and explicit `Meta.scopes` on the
same model are rejected as ambiguous.

## Literal requirements

On views and endpoints, positional values are complete literal identifiers:

```python
@scopes("reports.export")
async def export_report(...):
    ...
```

This is equivalent to the more explicit form:

```python
@scopes(requires=("reports.export",))
async def export_report(...):
    ...
```

No content type, route, module, or Python-name prefix is applied. Multiple
literal requirements are conjunctive. `requires=` is also valid on models and
then applies unchanged to every model operation:

```python
@scopes("update", requires=("catalog.access",))
class Article(Model): ...
```

Every operation requires `catalog.access`; update additionally requires
`articles.article.update`.

## Model-backed views

A `ModelGenericView` remains a view, so its positional values are literal.
It may additionally declare `actions=` to use its related model's content-type
namespace without modifying the model:

```python
@scopes("backoffice.access", actions=("update",))
class ArticleAdminView(ModelGenericView):
    model = Article
```

Model and view requirements are additive. A caller must satisfy every distinct
requirement contributed by either layer; omission at one layer never cancels a
requirement from the other.

Generic routes map collection and item operations to canonical actions.
Collection actions use `/bulk/{action}` so policy is known before request-body
access. Single and bulk forms of the same operation use the same action:
built-in bulk delete uses `delete`, while custom bulk actions default to
`manage` unless their action object exposes a narrower `scope_action`.

## Grants and aggregate content types

Grants are positive and additive. Neither `is_admin` nor `is_superuser`
bypasses scope checks.

An exact registered content-type identifier is an aggregate grant for its
descendants. Granting `articles.article` therefore satisfies both
`articles.article.update` and an application-defined
`articles.article.export`. Matching occurs only at a dot boundary and does not
match `articles.articleish.export`.

The bare content-type identifier is the aggregate form. `.*` and general
wildcard syntax are not supported.

## Enforcement and visibility

Protected class-based views enforce scope policy before the selected handler
runs. Endpoint callables can use `scope_dependency()` or call
`enforce_scope_access()` explicitly. `access_decision()` exposes the resolved
model, view, required, granted, missing, and object-check results for inspection.

`scope_visibility()` resolves action visibility using the same request-local
subject cache. Generic HTML views place one visibility map into their template
context and omit create, update, delete, and bulk controls unless the action is
both supported by the model's content type and allowed by scope policy. Generic
dispatch applies the same availability check before invoking mutation handlers.
Forms and templates inherit the enclosing view decision rather than declaring
scopes independently.

Generic HTML contexts expose visibility as an immutable
`wybra.views.ScopeVisibility` object with boolean `list`, `view`, `create`,
`update`, `delete`, and `manage` attributes. Templates use natural dot notation
such as `scope_visibility.update`; unlike a mutable dictionary, the typed object
cannot resolve `update` to a mapping method. String-key lookup such as
`scope_visibility["update"]` returns the same value when bracket syntax is
useful. Runtime dispatch enforcement remains the authorisation boundary.

An optional synchronous or asynchronous object callback may be supplied after
declarative requirements pass. This is an extension point, not a persisted
object-permission engine.

## Discovery and catalogue validation

After content types and routes are finalised, `discover_declared_scopes()`
returns literal and derived requirements with their target, origin, content
type, and action. It also returns registered content-type identifiers marked as
optional aggregate grants.

`validate_site_scope_catalogue()` compares required identifiers with the
configured `ScopeCatalogueCapability`. It reports missing identifiers and never
creates scope records or assignments.

`wybra-validate scopes` starts the configured ASGI application through its
lifespan, performs this comparison after site finalisation, and reports every
missing identifier as a validation error. It requires
`[app.runserver].asgi_app` to identify the configured application.

## Reserved field format

Future field-level policy may use
`<content-type>:<field-name>.<action>`, for example
`articles.article:subject.update`. Field-level declarations and enforcement are
not currently implemented.
