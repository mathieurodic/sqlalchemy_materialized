# `etl_decorators.xml.parse_xml`

`etl_decorators.xml.parse_xml` is a decorator that turns a function returning
XML (or a reference to XML) into a parsed `lxml` document root, with optional
extraction.

Extraction supports:

- **XPath** (default when the selector looks like XPath)
- **CSS selectors** (via `cssselect`)

## Installation

```bash
pip install etl-decorators[xml]
```

## Supported inputs

The decorated callable can return:

- **file path**: `str` or `pathlib.Path`
- **XML string**: `str`
- **file-like resource**: any object with `.read()` returning `str` or `bytes`

## Basic usage (return the parsed root element)

```python
from etl_decorators.xml import parse_xml


@parse_xml
def payload() -> str:
    return """<root><item>hello</item></root>"""


root = payload()
assert root.tag == "root"
```

## Extracting values

Use `extract=...` to query the parsed document.

### XPath extraction

```python
@parse_xml(extract="//item")
def payload() -> str:
    return """<root><item>hello</item><item>world</item></root>"""


# default extract_output="text" => returns text content of the first match
assert payload() == "hello"
```

### Collections

If you want all matches, set `extract_as_collection=True`:

```python
@parse_xml(extract="//item", extract_as_collection=True)
def payload() -> str:
    return """<root><item>a</item><item>b</item></root>"""


assert payload() == ["a", "b"]
```

Semantics when nothing matches:

- `extract_as_collection=False` => returns `None`
- `extract_as_collection=True` => returns `[]`

### Returning elements instead of text

Set `extract_output="elements"` to return `lxml` elements:

```python
@parse_xml(extract="//item", extract_output="elements")
def payload() -> str:
    return """<root><item>hello</item></root>"""


elem = payload()
assert elem.tag == "item"
```

## CSS selectors

CSS selectors are supported via `cssselect`.

```python
@parse_xml(extract="item", extract_mode="css", extract_as_collection=True)
def payload() -> str:
    return """<root><item>a</item><item>b</item></root>"""


assert payload() == ["a", "b"]
```

## Notes

- `extract_mode="auto"` uses a heuristic. If a selector is misclassified,
  force the mode with `extract_mode="xpath"` or `extract_mode="css"`.
- Optional dependencies are imported lazily; if missing, the decorator raises a
  clear `RuntimeError` telling you to install `etl-decorators[xml]`.
