# `etl_decorators.xml`

Helpers for ETL pipelines that deal with XML.

This package is an **optional extra**:

```bash
pip install etl-decorators[xml]
```

## User documentation

User-facing usage and examples live under `doc/`:

- [`doc/parse_xml.md`](../../../doc/parse_xml.md)

## Optional dependencies & lazy imports

This subpackage does **not** import `lxml` at import time. Instead dependencies
are imported lazily when [`parse_xml`](decorator.py) is executed.

If a dependency is missing, the code raises a `RuntimeError` with an install
hint (e.g. `pip install etl-decorators[xml]`).

Dependencies:

- [`lxml`](https://pypi.org/project/lxml/) is required for XML parsing.
- [`cssselect`](https://pypi.org/project/cssselect/) is required only when using
  `extract_mode="css"` (or when `extract_mode="auto"` resolves to CSS).

## Code layout

The implementation is intentionally split into small modules:

- [`payload.py`](payload.py): [`_read_xml_payload`](payload.py) (payload → XML string)
- [`deps.py`](deps.py): [`_require_lxml`](deps.py), [`_require_cssselect`](deps.py) (dependency guards)
- [`decorator.py`](decorator.py): [`parse_xml`](decorator.py) and helpers (core logic)

[`__init__.py`](__init__.py) re-exports [`parse_xml`](decorator.py) and
[`_read_xml_payload`](payload.py) to preserve a stable import path:

```python
from etl_decorators.xml import parse_xml, _read_xml_payload
```

## Tests note

Unit tests under [`tests/xml/`](../../../tests/xml/) monkeypatch tiny fake `lxml`
and `cssselect` modules into `sys.modules` so the optional dependencies are not
required just to run the test suite.
