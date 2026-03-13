# `etl_decorators.csv`

Helpers for ETL pipelines that deal with CSV inputs/outputs.

This package is an **optional extra**:

```bash
pip install etl-decorators[csv]
```

## User documentation

User-facing usage and examples live under `doc/`:

- [`doc/parse_csv.md`](../../../doc/parse_csv.md)

## Optional dependencies & lazy imports

This subpackage does **not** import `pandas` at import time. Instead, `pandas`
is required only when `parse_csv` is executed.

If the dependency is missing, the code raises a `RuntimeError` with an install
hint (e.g. `pip install etl-decorators[csv]`).

## Code layout

The implementation is intentionally split into small modules:

- [`payload.py`](payload.py): `_read_csv_payload` (payload → CSV string)
- [`deps.py`](deps.py): `_require_pandas` (dependency guard)
- [`scalar.py`](scalar.py): `_parse_scalar` (string → best-effort Python scalar)
- [`dialect.py`](dialect.py): `_sniff_csv_dialect` (delimiter/quoting inference)
- [`rows.py`](rows.py): `_iter_dict_rows` (DataFrame-like → iterator of dicts)
- [`decorator.py`](decorator.py): `parse_csv` and `_process_payload` (core orchestration)

## Tests

Tests live under [`tests/csv/`](../../../tests/csv/).

[`__init__.py`](__init__.py) re-exports `parse_csv` (and a few internals used by tests) to
preserve a stable import path:

```python
from etl_decorators.csv import parse_csv, _read_csv_payload
```
