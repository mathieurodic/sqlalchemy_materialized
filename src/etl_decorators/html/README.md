# `etl_decorators.html`

Helpers for ETL pipelines that deal with HTML.

This package is an **optional extra**:

```bash
pip install etl-decorators[html]
```

## User documentation

User-facing usage and examples live under `doc/`:

- [`doc/parse_html.md`](../../../doc/parse_html.md)

## Optional dependencies & lazy imports

This subpackage does **not** import `beautifulsoup4` / `markdownify` at import
time. Instead:

- [`beautifulsoup4`](https://pypi.org/project/beautifulsoup4/) is required only when [`parse_html`](decorator.py) is executed.
- [`markdownify`](https://pypi.org/project/markdownify/) is required only when `convert_to_markdown=True`.

If a dependency is missing, the code raises a `RuntimeError` with an install
hint (e.g. `pip install etl-decorators[html]`).

## Code layout

The implementation is intentionally split into small modules:

- [`payload.py`](payload.py): [`_read_html_payload`](payload.py) (payload → HTML string)
- [`deps.py`](deps.py): [`_require_bs4`](deps.py), [`_require_markdownify`](deps.py) (dependency guards)
- [`markdown.py`](markdown.py): [`_to_markdown`](markdown.py) (HTML → Markdown)
- [`decorator.py`](decorator.py): [`parse_html`](decorator.py) and [`_process_payload`](decorator.py) (core logic)

[`__init__.py`](__init__.py) re-exports [`parse_html`](decorator.py) and [`_read_html_payload`](payload.py) to preserve a
stable import path:

```python
from etl_decorators.html import parse_html, _read_html_payload
```

## Tests note

Unit tests under [`tests/html/`](../../../tests/html/) monkeypatch tiny fake `bs4` and `markdownify`
modules into `sys.modules` so the optional dependencies are not required just
to run the test suite.
