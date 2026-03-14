# TODO

This repository (`etl-decorators`) provides small, focused decorators commonly useful in ETL / data applications.

This TODO captures the next wave of decorators to build, based on the current documentation and the agreed scope:

- **Extraction / IO**: `@fetch`, `@paginate`, `@read_file`, `@write_file`
- **Parsing / normalization**: `@normalize_records`

---

## Success criteria (definition of done)

For each new decorator:

- Clean, typed public API (good docstrings + type hints).
- Sync + async behavior is explicitly defined (and tested).
- Unit tests under `tests/`.
- User docs under `doc/`.
- Optional dependencies follow the project’s conventions (lazy imports, clear error messages) — although the current plan aims for **base package only**.

  Note: this wave includes a couple of **optional extras** proposals (`http`, `xml`); everything else should remain in the base package.

---

## Proposed package layout

Create new subpackages (some with optional dependencies, matching the project’s “extras + lazy import” approach):

- `src/etl_decorators/http/` (**optional** extra: `etl-decorators[http]`)
  - `__init__.py` exports `fetch`, `paginate`
  - `README.md`
  - `fetch.py`
  - `paginate.py`

- `src/etl_decorators/io/` (base)
  - `__init__.py` exports `read_file`, `write_file`
  - `README.md`
  - `read_file.py`
  - `write_file.py`

- `src/etl_decorators/xml/` (**optional** extra: `etl-decorators[xml]`)
  - (implemented)

- `src/etl_decorators/normalize/` (base)
  - `__init__.py` exports `normalize_records`
  - `README.md`
  - `normalize_records.py`

And keep the originally planned ones:

Notes:

- Decide whether to re-export from `etl_decorators/__init__.py` (optional; follow existing conventions).
- Add new sections to root `README.md` (later task) so the table of contents matches the new modules.

---

## Decorators to implement

### `etl_decorators.http.fetch`

**Goal**: Standardize HTTP fetching in ETL (timeouts, headers, retries, response parsing).

**Target API**

```python
from etl_decorators.http import fetch


@fetch(
    method="GET",
    as_="text",  # "text" | "bytes" | "json"
    timeout=30,
)
def users_url(org_id: str) -> str:
    return f"https://api.example.com/orgs/{org_id}/users"
```

**Requirements**

- Decorated callable returns either:
  - URL (`str`), or
  - a small request spec (e.g. `{url, method, headers, params, json, data}`)
- Return type depends on `as_`:
  - `"text"` → `str`
  - `"bytes"` → `bytes`
  - `"json"` → `dict | list`
- Sync + async supported.
- Optional dependency:
  - preferred: `httpx` (sync + async), lazily imported
  - if missing: raise `RuntimeError("Install etl-decorators[http]...")`

**Notes**

- Keep `fetch` simple; prefer composing separate decorators around it.

**Tests**

- Use `respx` (if using httpx) or a small local test server; validate:
  - text response
  - json response
  - timeout behavior (or delegated to an external timeout wrapper)

**Docs**

- Add `doc/fetch.md`.

---

### `etl_decorators.http.paginate`

**Goal**: Turn “fetch a single page” functions into iterators over items/pages.

**Target API**

```python
from etl_decorators.http import paginate


@paginate(mode="cursor", cursor_param="cursor", next_cursor_path=("next", "cursor"))
def fetch_page(cursor: str | None = None) -> dict:
    # returns JSON page
    ...


for item in fetch_page():
    ...
```

**Requirements**

- Support at least:
  - cursor pagination
  - offset pagination
- User can choose to yield:
  - items (default), via `items_path=(...)`, or
  - full pages
- Stop conditions:
  - empty items
  - missing next cursor
  - explicit predicate `stop_if(page) -> bool`

**Tests**

- Cursor-based pagination yields expected items.
- Offset-based pagination yields expected items.

**Docs**

- Add `doc/paginate.md`.

---

### `etl_decorators.io.read_file`

**Goal**: Normalize a function returning “path | file-like | raw content” into `str` or `bytes`.

**Target API**

```python
from etl_decorators.io import read_file


@read_file(as_="text", encoding="utf-8")
def source() -> "str | pathlib.Path":
    return "data/input.txt"
```

**Requirements**

- Accept return values:
  - `pathlib.Path | str` (path)
  - file-like with `.read()`
  - raw `str`/`bytes` content
- `as_`: `"text" | "bytes"`
- When reading bytes and `encoding` is set, decode.

**Tests**

- Path input.
- File-like input.
- Raw content passthrough.

**Docs**

- Add `doc/read_file.md`.

---

### `etl_decorators.io.write_file`

**Goal**: Persist content returned by a function using safe/atomic writes.

**Target API**

```python
from etl_decorators.io import write_file


@write_file(path="out/users.json", mode="text", encoding="utf-8", atomic=True)
def build_users_dump() -> str:
    return "..."
```

**Requirements**

- Writes to `path` (passed via decorator param or computed from function args).
- Atomic write strategy:
  - write to `path.with_suffix(path.suffix + ".tmp")` then `rename`.
- Support text and bytes.
- (Optional later) gzip option.

**Tests**

- File is written and content matches.
- Atomic write uses temp file then rename (can assert via mocks).

**Docs**

- Add `doc/write_file.md`.

---

### `etl_decorators.normalize.normalize_records`

**Goal**: Normalize records so downstream loads are predictable.

**Target API**

```python
from etl_decorators.normalize import normalize_records


@normalize_records(
    key_style="snake",
    empty_string_as_none=True,
    strip_strings=True,
    datetime_to_utc=True,
)
def rows() -> "Iterator[dict]":
    ...
```

**Requirements**

- Works on:
  - a single `dict`, or
  - an iterator of dicts
- Operations (configurable):
  - key normalization (snake_case)
  - trim/strip strings
  - empty string → `None`
  - datetime normalization (tz-aware, convert to UTC)

**Tests**

- Key normalization.
- Empty string handling.
- Datetime UTC conversion.

**Docs**

- Add `doc/normalize_records.md`.

---

## Documentation tasks

### Root README updates

- Add new table-of-contents entries:
  - `etl_decorators.http` → `doc/fetch.md`, `doc/paginate.md`
  - `etl_decorators.io` → `doc/read_file.md`, `doc/write_file.md`
  - `etl_decorators.xml` → `doc/parse_xml.md`
  - `etl_decorators.normalize` → `doc/normalize_records.md`

---

## Test plan

When implemented:

1. Add unit tests for each decorator as described above.
2. Run:

```bash
pytest -q --cov etl_decorators --cov-report=term-missing
```

Success = all tests green.

---

## Follow-ups / future ideas (non-goals for now)

None yet.
