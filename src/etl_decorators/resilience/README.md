# `etl_decorators.resilience`

Helpers for making ETL steps more robust.

This subpackage is part of the **base package** (no optional extra):

```bash
pip install etl-decorators
```

## User documentation

User-facing usage and examples live under `doc/`:

- [`doc/retry.md`](../../../doc/retry.md)
- [`doc/timeout.md`](../../../doc/timeout.md)

## Optional dependencies & lazy imports

This subpackage has **no optional dependencies**.

## Code layout

The implementation is intentionally split into small modules:

- [`retry.py`](retry.py): `retry` decorator (sync + async, exponential backoff)
- [`timeout.py`](timeout.py): `timeout` decorator (sync + async)

## Tests

Tests live under [`tests/resilience/`](../../../tests/resilience/).

[`__init__.py`](__init__.py) re-exports `retry`:

```python
from etl_decorators.resilience import retry

from etl_decorators.resilience import timeout
```
