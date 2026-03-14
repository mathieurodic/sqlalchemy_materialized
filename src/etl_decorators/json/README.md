# `etl_decorators.json`

Small, dependency-free JSON parsing decorators.

## Provided decorators

- `etl_decorators.json.parse_json`: parse a JSON payload into `dict | list`.
- `etl_decorators.json.parse_jsonl`: iterate a JSONL payload (one JSON value per
  line) as an iterator of `dict | list`.

## Dependency behavior

This subpackage is part of the base package and uses only the Python standard
library.
