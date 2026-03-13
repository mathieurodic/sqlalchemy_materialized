# etl-decorators

A collection of small, focused decorators used in ETL / data applications.

## Installation

Base package (no optional dependencies):

```bash
pip install etl-decorators
```

SQLAlchemy subpackage:

```bash
pip install etl-decorators[sqlalchemy]
```

HTML subpackage:

```bash
pip install etl-decorators[html]
```

CSV subpackage:

```bash
pip install etl-decorators[csv]
```

## Decorators

- [`etl_decorators.sqlalchemy.materialized_property`](doc/materialized_property.md)
- [`etl_decorators.llms.LLM`](doc/llm.md)
- [`etl_decorators.html.parse_html`](doc/parse_html.md)
- [`etl_decorators.csv.parse_csv`](doc/parse_csv.md)
