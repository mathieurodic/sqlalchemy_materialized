# `etl_decorators`

`etl_decorators` is a collection of small, focused decorators used in ETL / data
applications.

Project-level README (installation and quick links): [`README.md`](../../README.md)

This README is **package-internal** (it lives under `src/`) and is meant to be:

- an overview of what’s available in this package
- a table of contents (TOC) for the subpackages
- a map of where to find **user-facing docs** vs **developer/internal docs**

## Installation

Base package (no optional dependencies):

```bash
pip install etl-decorators
```

Optional subpackages are installed via extras:

```bash
pip install etl-decorators[sqlalchemy]
pip install etl-decorators[llms]
pip install etl-decorators[html]
pip install etl-decorators[csv]
```

## Table of contents

### `etl_decorators.sqlalchemy`

- Main entrypoint: `etl_decorators.sqlalchemy.materialized_property`
- User documentation: [`doc/materialized_property.md`](../../doc/materialized_property.md)
- Developer notes / code layout: [`src/etl_decorators/sqlalchemy/README.md`](./sqlalchemy/README.md)
- Tests: [`tests/sqlalchemy/`](../../tests/sqlalchemy/)

### `etl_decorators.llms`

- Main entrypoint: `etl_decorators.llms.LLM`
- User documentation: [`doc/llm.md`](../../doc/llm.md)
- Developer notes / code layout: [`src/etl_decorators/llms/README.md`](./llms/README.md)
- Tests: [`tests/llms/`](../../tests/llms/)

### `etl_decorators.html`

- Main entrypoint: `etl_decorators.html.parse_html`
- User documentation: [`doc/parse_html.md`](../../doc/parse_html.md)
- Developer notes / code layout: [`src/etl_decorators/html/README.md`](./html/README.md)
- Tests: [`tests/html/`](../../tests/html/)

### `etl_decorators.csv`

- Main entrypoint: `etl_decorators.csv.parse_csv`
- User documentation: [`doc/parse_csv.md`](../../doc/parse_csv.md)
- Developer notes / code layout: [`src/etl_decorators/csv/README.md`](./csv/README.md)
- Tests: [`tests/csv/`](../../tests/csv/)

## Optional dependencies & lazy imports

Most functionality is organized in subpackages with **optional dependencies**.
Where feasible, optional dependencies are imported lazily (only when a decorator
is executed) so that importing `etl_decorators` doesn’t pull heavy dependencies.

Each subpackage’s internal README documents its own dependency behavior.

## Tests

Unit tests live under [`tests/`](../../tests/).

LLM integration tests are environment-gated (they require model + API key
environment variables), and are expected to be skipped in typical local/CI
setups without credentials.
