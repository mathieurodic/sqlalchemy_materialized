# TODO

This repository (`etl-decorators`) provides small, focused decorators commonly useful in ETL / data applications.

This TODO captures the next wave of decorators to build, based on the current documentation and the agreed scope:

- **Extraction / IO**: `@fetch_url`, `@paginate`, `@read_file`, `@write_file`
- **Parsing / normalization**: `@parse_json`, `@parse_jsonl`, `@parse_xml`, `@normalize_records`
- **Streams**: `@batch`, `@map`, `@dedupe` (no windowing)

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
  - `__init__.py` exports `fetch_url`, `paginate`
  - `fetch_url.py`
  - `paginate.py`

- `src/etl_decorators/io/` (base)
  - `__init__.py` exports `read_file`, `write_file`
  - `read_file.py`
  - `write_file.py`

- `src/etl_decorators/json/` (base)
  - `__init__.py` exports `parse_json`, `parse_jsonl`
  - `parse_json.py`
  - `parse_jsonl.py`

- `src/etl_decorators/xml/` (**optional** extra: `etl-decorators[xml]`)
  - `__init__.py` exports `parse_xml`
  - `parse_xml.py`

- `src/etl_decorators/normalize/` (base)
  - `__init__.py` exports `normalize_records`
  - `normalize_records.py`

And keep the originally planned ones:

- `src/etl_decorators/streams/`
  - `__init__.py` exports `batch`, `map`, `dedupe`
  - `batch.py`
  - `map.py`
  - `dedupe.py`

Notes:

- Decide whether to re-export from `etl_decorators/__init__.py` (optional; follow existing conventions).
- Add new sections to root `README.md` (later task) so the table of contents matches the new modules.

---

## Decorators to implement

### `etl_decorators.http.fetch_url`

**Goal**: Standardize HTTP fetching in ETL (timeouts, headers, retries, response parsing).

**Target API**

```python
from etl_decorators.http import fetch_url


@fetch_url(
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

- Keep `fetch_url` simple; prefer composing separate decorators around it.

**Tests**

- Use `respx` (if using httpx) or a small local test server; validate:
  - text response
  - json response
  - timeout behavior (or delegated to an external timeout wrapper)

**Docs**

- Add `doc/fetch_url.md`.

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

### `etl_decorators.json.parse_json`

**Goal**: JSON equivalent of `parse_csv`/`parse_html`: accept `path | string | file-like` and return parsed JSON.

**Target API**

```python
from etl_decorators.json import parse_json


@parse_json
def payload() -> str:
    return '{"a": 1}'
```

**Requirements**

- Accept return values:
  - `pathlib.Path | str` (path)
  - file-like with `.read()`
  - JSON string
- Return: `dict | list`.
- Errors: raise `ValueError` (or a small library exception) with helpful context.

**Tests**

- Parses from string.
- Parses from file.

**Docs**

- Add `doc/parse_json.md`.

---

### `etl_decorators.json.parse_jsonl`

**Goal**: Stream JSONL (newline-delimited JSON) as an iterator of dicts.

**Target API**

```python
from etl_decorators.json import parse_jsonl


@parse_jsonl
def events() -> str:
    return '{"id": 1}\n{"id": 2}\n'
```

**Requirements**

- Same accepted inputs as `parse_json`.
- Output: `Iterator[dict]` (or `Iterator[dict | list]` if you want to allow arrays-per-line; decide explicitly).
- Lazy: do not read the whole file/string if a file-like is returned.
- (Optional) `model: type[pydantic.BaseModel] | None` validation; can be deferred to later.

**Tests**

- Iterates all lines.
- Skips/handles blank lines (define behavior).

**Docs**

- Add `doc/parse_jsonl.md`.

---

### `etl_decorators.xml.parse_xml`

**Goal**: XML parser + extraction helper (feeds, sitemaps, invoices).

**Target API**

```python
from etl_decorators.xml import parse_xml


@parse_xml(extract="//url/loc", extract_as_collection=True)
def sitemap() -> str:
    return "...xml..."
```

**Requirements**

- Optional dependency: `lxml` (preferred) or stdlib `xml.etree` (decide; document).
- Accept return values:
  - `pathlib.Path | str` (path)
  - file-like with `.read()`
  - XML string
- Extraction:
  - XPath-like selector (if using lxml)
  - `extract_as_collection` semantics similar to `parse_html`

**Tests**

- Parse from string.
- Extraction returns expected element(s)/text.

**Docs**

- Add `doc/parse_xml.md`.

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

### `etl_decorators.streams.batch`

**Goal**: Transform an iterator of items into an iterator of fixed-size batches.

**Target API**

```python
from etl_decorators.streams import batch


@batch(size=1000)
def rows() -> "Iterator[dict]":
    ...


for chunk in rows():
    assert isinstance(chunk, list)
```

**Requirements**

- Decorated callable returns an iterable/iterator.
- Output is a lazy iterator of `list[T]` batches.
- Parameters:
  - `size: int` (required, > 0)
  - `drop_last: bool = False` (if True, drop incomplete final batch)
- Must not load the entire stream in memory.

**Tests**

- Exact multiples.
- With remainder.
- `drop_last=True`.

**Docs**

- Add `doc/batch.md`.

---

### `etl_decorators.streams.map`

**Goal**: Lazily map items yielded from an iterator-returning function.

**Target API**

```python
from etl_decorators.streams import map


@map(lambda row: {**row, "x": int(row["x"])})
def rows() -> "Iterator[dict]":
    ...
```

**Requirements**

- Decorated callable returns an iterable/iterator of `T`.
- Mapper signature: `(item: T) -> U`.
- Output is an iterator of `U`.
- Prefer a minimal synchronous implementation first.
  - If async iteration/concurrency is desired later, track as follow-up.

**Tests**

- Mapping is lazy (doesn’t pre-consume the iterator).
- Exceptions from mapper bubble up unchanged.

**Docs**

- Add `doc/map.md`.

---

### `etl_decorators.streams.dedupe` (no windowing)

**Goal**: Remove duplicates from a single run’s stream.

**Target API**

```python
from etl_decorators.streams import dedupe


@dedupe(key=lambda row: row["id"])
def rows():
    ...
```

**Requirements**

- Holds an in-memory `set` of seen keys for the lifetime of the iterator.
- Parameters:
  - `key: Callable[[T], Hashable]` (default identity)
- No persistent store, no `window`.

**Notes**

- Document memory tradeoff: the `seen` set grows with unique keys.

**Tests**

- Dedupes items properly.
- Works when items are unhashable but `key(item)` is hashable.

**Docs**

- Add `doc/dedupe.md`.

---

---

## Documentation tasks (tracked, but NOT done in this task)

### Root README updates

- Add new table-of-contents entries:
  - `etl_decorators.http` → `doc/fetch_url.md`, `doc/paginate.md`
  - `etl_decorators.io` → `doc/read_file.md`, `doc/write_file.md`
  - `etl_decorators.json` → `doc/parse_json.md`, `doc/parse_jsonl.md`
  - `etl_decorators.xml` → `doc/parse_xml.md`
  - `etl_decorators.normalize` → `doc/normalize_records.md`
  - `etl_decorators.streams` → `doc/batch.md`, `doc/map.md`, `doc/dedupe.md`

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

- Optional bounded-memory dedupe (`max_seen` / eviction) if users need it.
- Concurrency support for `streams.map` if it becomes a bottleneck.
