# Database Testcontainers

Wybra keeps the ordinary test suite fast and Docker-free. Database lifecycle
integration tests live separately under `tests_integration` and must be selected
explicitly.

## Local Use

Run the default suite:

```sh
uv run pytest -q
```

Run Docker-backed database integration tests:

```sh
uv run pytest tests_integration -q
```

Docker must be available to the test process. Missing Docker causes the
integration tests to skip with a Docker-specific reason; it does not validate
backend integration.

SQL Server integration also requires a working ODBC Driver 18 for SQL Server on
the host running the tests. If the ODBC prerequisite is missing, SQL Server
integration tests skip with an explicit SQL Server driver reason.

On macOS with Homebrew:

```sh
brew install unixodbc
brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release
brew trust --formula microsoft/mssql-release/msodbcsql18
HOMEBREW_ACCEPT_EULA=Y ACCEPT_EULA=Y brew install msodbcsql18
```

## Image Overrides

The integration suite uses pinned default images. Override them with environment
variables or a `.env` file in the repository root:

| Name | Description |
| --- | --- |
| `WYBRA_TESTCONTAINERS_POSTGRES_IMAGE` | PostgreSQL image used for integration tests. |
| `WYBRA_TESTCONTAINERS_MYSQL_IMAGE` | MySQL image used for integration tests. |
| `WYBRA_TESTCONTAINERS_MARIADB_IMAGE` | MariaDB image used for integration tests. |
| `WYBRA_TESTCONTAINERS_MSSQL_IMAGE` | SQL Server image used for integration tests. |
| `WYBRA_TESTCONTAINERS_NATS_IMAGE` | NATS JetStream image used for cache integration tests. |
| `WYBRA_TESTCONTAINERS_REDIS_IMAGE` | Redis image used for cache integration tests. |

The integration suite must not be added to pre-commit. It is intended for
explicit local runs and CI jobs that provide Docker.
