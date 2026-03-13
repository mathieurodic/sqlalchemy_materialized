# `etl_decorators.templating.template`

`etl_decorators.templating.template` is a small decorator that turns a function
returning a **Jinja2 template** into a function returning the **rendered
string**.

## Installation

```bash
pip install etl-decorators[templating]
```

## Basic usage

```python
from etl_decorators.templating import template


@template
def greeting(name: str) -> str:
    return "Hello {{ name }}"


assert greeting("Ada") == "Hello Ada"
```

## Supported return values

The decorated callable can return:

- **template string** (`str`)
- **file path** (`str` or `pathlib.Path`)
- **file-like object** with `.read()` returning `str` or `bytes`

Examples:

### Return a path

```python
from pathlib import Path
from etl_decorators.templating import template

tpl = Path("hello.j2")
tpl.write_text("Hello {{ name }}", encoding="utf-8")


@template
def greeting(name: str):
    return tpl
```

### Return a file-like object

```python
import io
from etl_decorators.templating import template


@template
def greeting(name: str):
    return io.StringIO("Hello {{ name }}")
```

## Template variables

Template variables come from the decorated callable’s **bound parameters**.

### Methods (`self`)

Jinja2 reserves `self` inside templates for internal use.

For convenience, `etl_decorators.templating.template`:

- exposes the Python instance as `self_` in the template context, and
- rewrites `self.` to `self_.` **inside Jinja blocks** (``{{ ... }}`` and
  ``{% ... %}``).

So you can write templates naturally:

```python
from etl_decorators.templating import template


class Greeter:
    def __init__(self, who: str):
        self.who = who

    @template
    def msg(self, punct: str = "!"):
        return "Hello {{ self.who }}{{ punct }}"


g = Greeter("Ada")
assert g.msg() == "Hello Ada!"
```

## Missing variables

The decorator uses Jinja2’s `StrictUndefined`: referencing a missing variable
raises an exception.
