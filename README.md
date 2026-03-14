# ETL-decorators

A collection of small, focused decorators used in ETL / data applications.

## Installation

Base package (no optional dependencies):

```bash
pip install etl-decorators
```

### Optional dependencies

Some subpackages have optional (extra) dependencies. Each such subpackage
documents its extra in its section below.

Most functionality is organized in subpackages with **optional dependencies**.
Where feasible, optional dependencies are imported lazily (only when a decorator
is executed) so that importing `etl_decorators` doesn’t pull heavy dependencies.

Each subpackage’s README documents its own dependency behavior.

## Decorators

### Parsing

#### `etl_decorators.csv`

- Install: `pip install etl-decorators[csv]`
- User documentation: [`etl_decorators.csv.parse_csv`](doc/parse_csv.md)
- Developer notes / code layout: [`src/etl_decorators/csv/README.md`](src/etl_decorators/csv/README.md)
- Tests: [`tests/csv/`](tests/csv/)

#### `etl_decorators.html`

- Install: `pip install etl-decorators[html]`
- User documentation: [`etl_decorators.html.parse_html`](doc/parse_html.md)
- Developer notes / code layout: [`src/etl_decorators/html/README.md`](src/etl_decorators/html/README.md)
- Tests: [`tests/html/`](tests/html/)

#### `etl_decorators.json`

- User documentation:
  - [`etl_decorators.json.parse_json`](doc/parse_json.md)
  - [`etl_decorators.json.parse_jsonl`](doc/parse_jsonl.md)
- Developer notes / code layout: [`src/etl_decorators/json/README.md`](src/etl_decorators/json/README.md)
- Tests: [`tests/json/`](tests/json/)

#### `etl_decorators.xml`

- Install: `pip install etl-decorators[xml]`
- User documentation: [`etl_decorators.xml.parse_xml`](doc/parse_xml.md)
- Developer notes / code layout: [`src/etl_decorators/xml/README.md`](src/etl_decorators/xml/README.md)
- Tests: [`tests/xml/`](tests/xml/)

### Caching

#### `etl_decorators.redis`

- Install: `pip install etl-decorators[redis]`
- User documentation: [`etl_decorators.redis.RedisCache`](doc/cache_redis.md)
- Developer notes / code layout: [`src/etl_decorators/redis/README.md`](src/etl_decorators/redis/README.md)
- Tests: [`tests/redis/`](tests/redis/)

#### `etl_decorators.sqlalchemy`

- Install: `pip install etl-decorators[sqlalchemy]`
- User documentation:
  - [`etl_decorators.sqlalchemy.materialized_property`](doc/materialized_property.md)
  - [`etl_decorators.sqlalchemy.as_model`](doc/as_model.md)
- Developer notes / code layout: [`src/etl_decorators/sqlalchemy/README.md`](src/etl_decorators/sqlalchemy/README.md)
- Tests: [`tests/sqlalchemy/`](tests/sqlalchemy/)

### LLM

#### `etl_decorators.llms`

- Install: `pip install etl-decorators[llms]`
- User documentation: [`etl_decorators.llms.LLM`](doc/llm.md)
- Developer notes / code layout: [`src/etl_decorators/llms/README.md`](src/etl_decorators/llms/README.md)
- Tests: [`tests/llms/`](tests/llms/); LLM integration tests are environment-gated: they require model + API key [environment variables](./.env.example), and are expected to be skipped in typical local/CI setups without credentials

### Resilience

#### `etl_decorators.resilience`

- User documentation:
  - [`etl_decorators.resilience.retry`](doc/retry.md)
  - [`etl_decorators.resilience.timeout`](doc/timeout.md)
- Developer notes / code layout: [`src/etl_decorators/resilience/README.md`](src/etl_decorators/resilience/README.md)
- Tests: [`tests/resilience/`](tests/resilience/)

### Streams

#### `etl_decorators.streams`

- User documentation:
  - [`etl_decorators.streams.batch`](doc/batch.md)
  - [`etl_decorators.streams.transform`](doc/transform.md)
  - [`etl_decorators.streams.keep`](doc/keep.md)
  - [`etl_decorators.streams.dedupe`](doc/dedupe.md)
- Developer notes / code layout: [`src/etl_decorators/streams/README.md`](src/etl_decorators/streams/README.md)
- Tests: [`tests/streams/`](tests/streams/)

### Logging

#### `etl_decorators.logging`

- User documentation: [`etl_decorators.logging.log`](doc/log.md)
- Developer notes / code layout: [`src/etl_decorators/logging/README.md`](src/etl_decorators/logging/README.md)
- Tests: [`tests/logging/`](tests/logging/)

### Templating

#### `etl_decorators.templating`

- Install: `pip install etl-decorators[templating]`
- User documentation: [`etl_decorators.templating.template`](doc/template.md)
- Developer notes / code layout: [`src/etl_decorators/templating/README.md`](src/etl_decorators/templating/README.md)
- Tests: [`tests/templating/`](tests/templating/)

## Internal utilities

### `etl_decorators._base`

- Shared decorator plumbing: [`src/etl_decorators/_base/README.md`](src/etl_decorators/_base/README.md)
