# etl-decorators

A collection of small, focused decorators used in ETL / data applications.

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
pip install etl-decorators[templating]
```

## Table of contents

### Internal base utilities

- Shared decorator plumbing: [`src/etl_decorators/_base/README.md`](src/etl_decorators/_base/README.md)

### `etl_decorators.sqlalchemy`

- Main entrypoint: [`etl_decorators.sqlalchemy.materialized_property`](src/etl_decorators/sqlalchemy/__init__.py)
- User documentation: [`doc/materialized_property.md`](doc/materialized_property.md)
- Developer notes / code layout: [`src/etl_decorators/sqlalchemy/README.md`](src/etl_decorators/sqlalchemy/README.md)
- Tests: [`tests/sqlalchemy/`](tests/sqlalchemy/)

### `etl_decorators.llms`

- Main entrypoint: [`etl_decorators.llms.LLM`](src/etl_decorators/llms/__init__.py)
- User documentation: [`doc/llm.md`](doc/llm.md)
- Developer notes / code layout: [`src/etl_decorators/llms/README.md`](src/etl_decorators/llms/README.md)
- Tests: [`tests/llms/`](tests/llms/)

### `etl_decorators.html`

- Main entrypoint: [`etl_decorators.html.parse_html`](src/etl_decorators/html/__init__.py)
- User documentation: [`doc/parse_html.md`](doc/parse_html.md)
- Developer notes / code layout: [`src/etl_decorators/html/README.md`](src/etl_decorators/html/README.md)
- Tests: [`tests/html/`](tests/html/)

### `etl_decorators.csv`

- Main entrypoint: [`etl_decorators.csv.parse_csv`](src/etl_decorators/csv/__init__.py)
- User documentation: [`doc/parse_csv.md`](doc/parse_csv.md)
- Developer notes / code layout: [`src/etl_decorators/csv/README.md`](src/etl_decorators/csv/README.md)
- Tests: [`tests/csv/`](tests/csv/)

### `etl_decorators.templating`

- Main entrypoint: [`etl_decorators.templating.template`](src/etl_decorators/templating/__init__.py)
- User documentation: [`doc/template.md`](doc/template.md)
- Developer notes / code layout: [`src/etl_decorators/templating/README.md`](src/etl_decorators/templating/README.md)
- Tests: [`tests/templating/`](tests/templating/)

### `etl_decorators.logging`

- Main entrypoint: [`etl_decorators.logging.log`](src/etl_decorators/logging/__init__.py)
- User documentation: [`doc/log.md`](doc/log.md)
- Tests: [`tests/logging/`](tests/logging/)

## Optional dependencies & lazy imports

Most functionality is organized in subpackages with **optional dependencies**.
Where feasible, optional dependencies are imported lazily (only when a decorator
is executed) so that importing `etl_decorators` doesn’t pull heavy dependencies.

Each subpackage’s README documents its own dependency behavior.

## Tests

Unit tests live under [`tests/`](tests/).

LLM integration tests are environment-gated (they require model + API key
[environment variables](./.env.example)), and are expected to be skipped in typical local/CI
setups without credentials.
