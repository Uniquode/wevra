# Windows Validation

Wybra treats Windows compatibility as part of the package validation surface.
The GitHub Actions test workflow runs the normal package gates on both Linux
and Windows.

The Windows job runs:

- `uv run ruff format --check src tests`
- `uv run ruff check src tests`
- `uv run ty check src/`
- `uv run pytest -q`
- `uv run python scripts/smoke_runserver.py`
- `uv build`

## Local Windows Checks

From a Windows shell in the Wybra checkout:

```powershell
uv sync
uv run ruff format --check src tests
uv run ruff check src tests
uv run ty check src/
uv run pytest -q
uv run python scripts/smoke_runserver.py
uv build
```

The smoke helper starts `wybra-runserver` through `uv`, creates a temporary host
ASGI application, verifies an HTTP response on loopback, and terminates the
server process.

Parameterised tests that use very large values must declare concise `ids`.
Pytest records the active test name in `PYTEST_CURRENT_TEST`; Windows limits a
single environment variable value to 32,767 UTF-16 characters.

## Support Boundaries

- Windows CI validates the package workflow on GitHub-hosted
  `windows-latest` runners.
- SQLite file database URLs should use forward-slash URL paths. For Windows
  drive paths, use the normal SQLite URL form such as
  `sqlite:///C:/path/to/app.sqlite3`.
- Optional operating-system integrations, such as the keychain backend, require
  their Python extra and an accessible platform service. Tests that cannot
  reach that service must skip with a prerequisite-specific reason.
- The Windows validation path does not add a Windows development container or
  guarantee availability of optional local services on every machine.
