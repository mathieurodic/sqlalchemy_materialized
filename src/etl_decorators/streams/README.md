# `etl_decorators.streams`

Small stream helpers for ETL pipelines.

This subpackage is part of the **base** install (no extras required):

```bash
pip install etl-decorators
```

## User documentation

User-facing usage and examples live under `doc/`:

- [`doc/batch.md`](../../../doc/batch.md)
- [`doc/transform.md`](../../../doc/transform.md)
- [`doc/keep.md`](../../../doc/keep.md)
- [`doc/dedupe.md`](../../../doc/dedupe.md)

## Design notes

All decorators in this package support functions returning either:

- `Iterable[T]` / `Iterator[T]` (sync streams), or
- `AsyncIterable[T]` / `AsyncIterator[T]` (async streams)

Runtime detection is based on the presence of `__aiter__`.

## Code layout

The implementation is split into small modules:

- [`batch.py`](batch.py): `batch`
- [`transform.py`](transform.py): `transform`
- [`keep.py`](keep.py): `keep`
- [`dedupe.py`](dedupe.py): `dedupe`

## Tests

Tests live under [`tests/streams/`](../../../tests/streams/).

[`__init__.py`](__init__.py) re-exports `batch`, `transform`, `keep`, and `dedupe`.

```python
from etl_decorators.streams import batch, transform, keep, dedupe
```
