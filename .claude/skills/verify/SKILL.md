---
name: verify
description: Run the full quality check suite — lint, type check, and tests. Invoke when done with a feature or before marking work complete.
---

Run the following checks in order and report any failures. Stop on first failure category (don't proceed to tests if lint fails).

1. **Lint**: `ruff check .`
2. **Format check**: `ruff format --check .`
3. **Type check**: `mypy src/` (skip if no src/ or mypy not installed yet)
4. **Tests**: `pytest` (or `pytest -x` to stop on first failure)

If any step fails, show the relevant output and suggest fixes. If all pass, confirm with a one-line summary.
