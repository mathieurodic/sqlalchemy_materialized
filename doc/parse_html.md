# `etl_decorators.html.parse_html`

`etl_decorators.html.parse_html` is a small decorator that turns a function
returning HTML (or a reference to HTML) into a parsed BeautifulSoup document,
with optional CSS extraction and optional markdown conversion.

## Return types

The return type depends on `extract`, `extract_as_collection`, and
`convert_to_markdown`:

- `extract is None` and `convert_to_markdown=False` → `bs4.BeautifulSoup`
- `extract is None` and `convert_to_markdown=True` → `str`
- `extract is not None` and `extract_as_collection=False` → `bs4.Tag | None`
- `extract is not None` and `extract_as_collection=True` → `list[bs4.Tag]`
- with `convert_to_markdown=True` and extraction → `str` / `list[str]`

When `extract` is a **tuple of selectors**, the decorator returns one value per
selector:

- `extract is tuple[str, ...]` and `extract_as_collection=False` →
  `list[bs4.Tag | None]`
- `extract is tuple[str, ...]` and `extract_as_collection=True` →
  `list[list[bs4.Tag]]`
- with `convert_to_markdown=True` and tuple extraction →
  `list[str | None]` / `list[list[str]]`

## Installation

```bash
pip install etl-decorators[html]
```

## Basic usage (return a BeautifulSoup document)

Decorate a function that returns an HTML string:

```python
from etl_decorators.html import parse_html


@parse_html
def fetch_page() -> str:
    return "<html><body><h1>Hello</h1></body></html>"


soup = fetch_page()
assert soup.select_one("h1").get_text(strip=True) == "Hello"
```

The decorated callable accepts three kinds of return values:

- **file path**: `str` or `pathlib.Path`
- **HTML string**: `str`
- **file-like resource**: any object with `.read()` returning `str` or `bytes`

## Extracting elements (CSS selector)

Use `extract=...` to select HTML fragments with a CSS selector.

```python
from etl_decorators.html import parse_html


@parse_html(extract=".item")
def page() -> str:
    return "<div><span class='item'>a</span><span class='item'>b</span></div>"


tag = page()  # bs4.Tag | None
assert tag is not None
assert tag.get_text(strip=True) == "a"
```

### Collections

If you want all matches, set `extract_as_collection=True`:

```python
@parse_html(extract=".item", extract_as_collection=True)
def page() -> str:
    return "<div><span class='item'>a</span><span class='item'>b</span></div>"


tags = page()  # list[bs4.Tag]
assert [t.get_text(strip=True) for t in tags] == ["a", "b"]
```

Semantics when nothing matches:

- `extract_as_collection=False` => returns `None`
- `extract_as_collection=True` => returns `[]`

### Multiple selectors (tuple)

You can pass multiple selectors by using a tuple.

- When `extract_as_collection=False`, you get a **list of first matches** (or
  `None`) per selector.
- When `extract_as_collection=True`, you get a **list of match lists**, one per
  selector.

```python
from etl_decorators.html import parse_html


@parse_html(extract=("h1", ".item"))
def page() -> str:
    return "<div><h1>T</h1><span class='item'>a</span><span class='item'>b</span></div>"


# first match per selector
tags = page()  # list[bs4.Tag | None]
assert tags[0].name == "h1"
assert tags[1].name == "span"
```

## Converting to Markdown

Set `convert_to_markdown=True` to convert HTML to markdown (via `markdownify`).

- If `extract is None`, the **full document** is converted.
- If `extract is not None`, the **extracted fragment(s)** are converted.

```python
@parse_html(convert_to_markdown=True)
def page() -> str:
    return "<h1>Title</h1><p>Hello <b>world</b></p>"


md = page()  # str
assert "Title" in md
```

## Notes

- `extract` currently supports **CSS selectors only** (BeautifulSoup `select`).
- Optional dependencies are imported lazily; if missing, the decorator raises a
  clear `RuntimeError` telling you to install `etl-decorators[html]`.
