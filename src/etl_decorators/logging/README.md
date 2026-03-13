# `etl_decorators.logging`

Helpers for ETL pipelines that want consistent, configurable logging.

This package is part of the **base** install (no extras required):

```bash
pip install etl-decorators
```

## User documentation

User-facing usage and examples live under `doc/`:

- [`doc/log.md`](../../../doc/log.md)

## Dependencies

This subpackage relies only on the Python standard library (`logging`, `time`,
`inspect`). There are no optional dependencies and no lazy-import guards.

## Code layout

The implementation is intentionally small:

- [`decorator.py`](decorator.py): `log` (sync + async wrapper)
- [`__init__.py`](__init__.py): public re-export

## Tests

Tests live under [`tests/logging/`](../../../tests/logging/).

[`__init__.py`](__init__.py) re-exports `log` to preserve a stable import path:

```python
from etl_decorators.logging import log
```
