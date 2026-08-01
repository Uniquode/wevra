# Database Configuration

Wybra supports two database configuration styles:

- Structured `[app.database]` configuration for production and managed
  deployments.
- `database_url` / `DATABASE_URL` for local development, simple SQLite setups,
  and explicit command-line overrides.

For production, prefer `[app.database]` so credentials can come from a secret
source instead of being stored inside a URL.

## Precedence

Wybra resolves the effective database connection in this order:

1. A command-line/startup database URL override.
2. `DATABASE_URL`.
3. `[app.database]`.
4. `[app].database_url`.

`[app.database]` and `[app].database_url` are mutually exclusive.

`AUTH_DATABASE_URL` is not supported. Authentication uses the same application
database configuration as the rest of the app.

## Read And Write Routing

`[app.database]` defines the base connection and is the default logical
database. With no role-specific instances, ordinary, explicit-read, and
explicit-write work all use that one connection.

For a writer and one or more read replicas, add named instances under
`[app.database.<name>]`. They inherit every connection setting from the base
instance, and may override instance-specific settings such as `host`, `port`,
`database`, credentials, TLS, or AWS metadata. Secondary instances must not
set `backend`: every configured route uses the base database family.

```toml
[app.database]
backend = "postgresql"
host = "writer.db.internal"
database = "uniquode"
credential_source = "keychain"
writer_rotation = "queue"
reader_rotation = "weighted"

[app.database.replica_sydney]
role = "reader"
host = "reader-sydney.db.internal"
weight = 2

[app.database.replica_melbourne]
role = "reader"
host = "reader-melbourne.db.internal"
weight = 1
```

`role` accepts a comma-separated combination of `default`, `reader`, and
`writer`. The base instance implicitly has the `default` role when `role` is
omitted; it participates in reader or writer selection only when those roles
are explicitly declared. A reader or writer selection with no eligible role
instance falls back to a selection from the default pool.

Each role is selected independently with `default_rotation`,
`reader_rotation`, and `writer_rotation`. The supported policies are
`default`, `queue` (the default), `random`, `weighted`, `load`, and
`adaptive`. Weights are positive relative capacities. `load` uses active
client-side work relative to weight, while `adaptive` uses that load with
rolling statement latency as a secondary signal.

Application code selects its intent through the opaque database capability:
`database().default()` for ordinary work, `for_read()` only for data that may
be replica-stale, and `for_write()` for mutations or writer-consistent reads.
A selected route remains fixed for the caller's managed work. An explicit
transaction additionally pins one physical connection until it ends. Wybra
does not automatically retry or replay a failed write through another writer.
Model forms receive a selected writer route for rendering, relation validation,
and persistence so a replica cannot make a submitted mutation inconsistent
with its rendered state.

## Local SQLite

For local development, a URL is still acceptable:

```toml
[app]
database_url = "sqlite:///local.sqlite3"
```

Relative SQLite file paths are resolved from the effective project root, not
from the directory containing the configuration file and not from the process
working directory. This remains true when the active config file is supplied
from another location with `--config`, `APP_CONFIG`, or equivalent startup
configuration.

The structured equivalent is:

```toml
[app.database]
backend = "sqlite"
database = "local.sqlite3"
```

When constructing settings directly from a database URL, use an absolute
SQLite URL. Relative SQLite URLs require the application project root and are
only accepted through the application configuration path.

## PostgreSQL With Secret-Backed Credentials

Use structured config for PostgreSQL so the username and password can be read
from a configured secret source:

```toml
[app.database]
backend = "postgresql"
host = "db.internal.example"
port = 5432
database = "uniquode"
credential_source = "keychain"
```

`credential_source` can use the same sources as the Wybra secrets subsystem,
including `environment`, `keychain`, `kms`, and `vault` when those sources are
available and configured.

For non-environment secret sources, Wybra derives default database credential
keys from the configured database name:

| Credential | Default key |
| --- | --- |
| Runtime username | `database/<database>/app/user` |
| Runtime password | `database/<database>/app/password` |
| Service-account username | `database/<database>/service-account/user` |
| Service-account password | `database/<database>/service-account/password` |

For the example above, the runtime keys are
`database/uniquode/app/user` and `database/uniquode/app/password`.
Configure `user_key`, `password_key`, `sa_user_key`, or `sa_password_key` only
when a deployment needs to override those defaults.

These configured keys are exposed through Wybra's credential-reference
contract. See [`CREDENTIAL_REFERENCES.md`](CREDENTIAL_REFERENCES.md) for the
module-author metadata shape used by `wybra-secret` and related tooling.

Default key derivation accepts Unicode database names, but the database name
must be safe as one key-path segment. If the configured database name contains
path separators, whitespace, or control characters, configure explicit
credential keys instead.

`wybra-migrate init` and `wybra-migrate destroy` resolve both identities: the
runtime identity is retained for application-role creation and grants, while a
separate service-account connection performs database lifecycle work. Schema
changing migration commands connect to the application database as the service
account. Runtime credentials are for application access and should not need
broad DDL permissions.

## Maintenance Tasks

List maintenance tasks for the configured database with:

```sh
wybra-migrate tasks
```

Run a maintenance task with:

```sh
wybra-migrate run repair-privs
```

Maintenance task execution uses service-account credentials because these
tasks inspect or repair database-level state. Current task names are:

| Task | Backend | Description |
| --- | --- | --- |
| `repair-privs` | PostgreSQL, MySQL, MariaDB, SQL Server | Reapply runtime application privileges. |
| `migrations` | PostgreSQL, MySQL, MariaDB, SQL Server | Report the Tortoise migration recorder state. |
| `analyse` | PostgreSQL | Refresh planner statistics after large data changes. |
| `extensions` | PostgreSQL | Validate PostgreSQL extension prerequisites. |
| `prerequisites` | SQL Server | Report SQL Server external setup prerequisites. |

PostgreSQL lifecycle work first connects to a service-account database because
the target application database may not exist yet. Wybra defaults that
maintenance database to `postgres`. Override it only when the service account
cannot connect to that database. The service-account database must be different
from the target application database:

```toml
[app.database]
backend = "postgresql"
database = "uniquode"
sa_database = "postgres"
credential_source = "keychain"
```

PostgreSQL migrations are executed through the service-account connection so
created schema objects remain owned by the service account. Provisioning and
maintenance then apply grants and default privileges for the runtime
application role. Use an application-dedicated runtime role; Wybra will not
drop a runtime role when it is also the service-account role or when it has
dependencies outside the configured target database.

## Environment Credential Source

When `credential_source = "environment"`, the credential keys are environment
variable names:

```toml
[app.database]
backend = "postgresql"
host = "db.internal.example"
database = "uniquode"
credential_source = "environment"
user_key = "UNIQUODE_DB_USER"
password_key = "UNIQUODE_DB_PASSWORD"
```

This is explicit credential lookup, not general TOML interpolation.

## PostgreSQL Unix Sockets

For PostgreSQL deployments that use a Unix socket, set `host` to the socket
directory:

```toml
[app.database]
backend = "postgresql"
host = "/var/run/postgresql"
database = "uniquode"
credential_source = "keychain"
```

Wybra passes the socket path to the database backend without converting it into
a TCP host name.

## AWS RDS And Aurora

AWS RDS and Aurora are managed-database overlays, not separate database
families. Configure the underlying database backend as usual, then add AWS
metadata so Wybra can validate the managed target before delegating database,
schema, user, grant, migration, destroy, and maintenance work to the matching
family provisioner.

Install the AWS optional dependency where managed-target validation is needed:

```sh
uv add "wybra[aws]"
```

Shared AWS defaults belong under `[app.aws]`. Database-specific AWS settings
belong under `[app.database.aws]` and override the shared values only for the
database target:

```toml
[app.aws]
region = "ap-southeast-2"
profile = "production"
role_arn = "arn:aws:iam::123456789012:role/wybra-database-provisioning"
sso_region = "us-east-1"

[app.database]
backend = "postgresql"
host = "uniquode.abc123.ap-southeast-2.rds.amazonaws.com"
port = 5432
database = "uniquode"
credential_source = "keychain"

[app.database.aws]
managed = "rds"
db_instance_identifier = "uniquode-postgresql"
engine = "postgres"
endpoint = "uniquode.abc123.ap-southeast-2.rds.amazonaws.com"
port = 5432
```

For Aurora, use `managed = "aurora"` and `cluster_identifier`:

```toml
[app.database.aws]
managed = "aurora"
cluster_identifier = "uniquode-cluster"
engine = "aurora-postgresql"
```

Wybra validates the observed managed target through the AWS SDK. It checks the
engine family, identifier, account or partition when configured, and the
endpoint and port when those values are configured. Supported AWS engines map
to the existing PostgreSQL, MySQL, MariaDB, or SQL Server provisioners. Oracle
engines remain unsupported until Wybra has first-class Oracle database support.

AWS access keys and session tokens should not be stored in Wybra config. Use
the normal AWS credential chain, such as environment, instance profile, shared
AWS config, SSO, or an assumed role. ARNs, account IDs, profile names, regions,
RDS identifiers, cluster identifiers, endpoints, and ports are treated as
non-secret identifiers.

If an assumed role requires an external id, configure either `external_id` or
`external_id_key`. Use `external_id_key` when the value should be resolved from
a secret source:

```toml
[app.aws]
role_arn = "arn:aws:iam::123456789012:role/wybra-database-provisioning"
external_id_source = "keychain"
external_id_key = "aws/rds/external-id"
```

Wybra does not create, modify, or delete RDS instances, Aurora clusters, VPCs,
subnet groups, security groups, KMS keys, backups, deletion protection, or any
other AWS infrastructure in this change. Provision those resources with your
cloud infrastructure tooling before running Wybra database lifecycle commands.

## Backend Dependencies

SQLite support is available by default. Other database backends require the
matching Wybra optional dependency, such as `wybra[postgresql]`,
`wybra[psycopg]`, `wybra[mysql]`, `wybra[mariadb]`, `wybra[mssql]`, or
`wybra[oracle]`. AWS RDS and Aurora metadata validation additionally requires
`wybra[aws]`.
