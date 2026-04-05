---
name: dbt-check
description: Validate dbt project — parse, compile, and run tests. Invoke when working on dbt models, macros, or tests.
---

Run from the `dbt/` directory (or wherever `dbt_project.yml` lives). If no dbt project exists yet, say so and stop.

1. **Parse**: `dbt parse` — validates YAML and SQL syntax
2. **Compile**: `dbt compile` — renders Jinja and resolves refs
3. **Test**: `dbt test` — runs schema and data tests

If `$ARGUMENTS` is provided, treat it as a node selector and scope all commands: `dbt parse && dbt compile -s $ARGUMENTS && dbt test -s $ARGUMENTS`.

Report failures with the relevant dbt error output. If all pass, summarize which models/tests ran.
