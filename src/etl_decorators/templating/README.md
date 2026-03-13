# `etl_decorators.templating`

Helpers for ETL pipelines that need lightweight string templating.

This package is an **optional extra**:

```bash
pip install etl-decorators[templating]
```

## User documentation

User-facing usage and examples live under `doc/`:

- [`doc/template.md`](../../../doc/template.md)

## Optional dependencies & lazy imports

This subpackage does **not** import `jinja2` at import time. Instead, `jinja2`
is required only when `template` is executed.

If the dependency is missing, the code raises a `RuntimeError` with an install
hint (e.g. `pip install etl-decorators[templating]`).

## Notes / behavior

- Template variables are built from the decorated callable’s **bound
  parameters**.
- For methods, you can write `{{ self.attr }}` in templates. Internally, the
  decorator exposes the instance as `self_` and rewrites `self.` → `self_.`
  inside Jinja blocks (because Jinja2 reserves `self`).
- Missing variables raise (`StrictUndefined`).

## Code layout

The implementation is intentionally split into small modules:

- [`payload.py`](payload.py): `_read_template_payload` (payload → template string)
- [`deps.py`](deps.py): `_require_jinja2` (dependency guard)
- [`decorator.py`](decorator.py): `template` and helpers (core orchestration)

## Tests

Tests live under [`tests/templating/`](../../../tests/templating/).

[`__init__.py`](__init__.py) re-exports `template` (and an internal used by
tests) to preserve a stable import path:

```python
from etl_decorators.templating import template, _read_template_payload
```
