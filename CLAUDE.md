# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Stack

- **Language**: Python
- **Orchestration**: Apache Airflow
- **Transformation**: dbt
- **Warehouse**: Google BigQuery
- **Local analytics**: DuckDB
- **Local dev**: Docker / docker-compose

## Project Structure (conventions)

```
dags/        # Airflow DAG definitions
dbt/         # dbt project (models, tests, macros, seeds)
src/         # Python pipeline code (ingestion, utils, connectors)
tests/       # Unit and integration tests
```

## Development Workflow

Always propose a plan before implementing. For non-trivial changes, outline the approach and get confirmation before writing code.

## Commands

Fill these in as the project grows:

```bash
# Python
pip install -e ".[dev]"   # install with dev deps (once pyproject.toml exists)
pytest                    # run tests
ruff check .              # lint
ruff format .             # format
mypy src/                 # type check

# dbt (from dbt/ directory)
dbt parse
dbt compile
dbt run
dbt test

# Airflow (local)
# docker-compose up       # start local Airflow
```

## Environment

Required environment variables (add to `.env` or export before running):

```
GOOGLE_APPLICATION_CREDENTIALS=  # path to GCP service account JSON
GCP_PROJECT=                      # GCP project ID
BQ_DATASET=                       # BigQuery dataset name
```

## Conventions

- Python: use `ruff` for linting and formatting, `mypy` for type checking
- dbt models: staging → intermediate → mart layer pattern
- Airflow DAGs: one DAG per logical pipeline, use TaskFlow API where possible
- Secrets: never hardcode credentials; use env vars or Secret Manager
