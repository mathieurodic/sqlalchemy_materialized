"""HTML parsing helpers.

This subpackage provides small helpers used in ETL pipelines dealing with HTML.

Optional dependency:

    pip install etl-decorators[html]

The main entrypoint is :func:`etl_decorators.html.parse_html`.
"""

from __future__ import annotations

import inspect
from pathlib import Path
from typing import Any, Callable, ParamSpec, overload


P = ParamSpec("P")


def _read_html_payload(payload: Any) -> str:
    """Normalize function output to a HTML string.

    Supported inputs:
    - file path (str/pathlib.Path)
    - raw HTML string
    - file-like object with .read() returning str or bytes
    """

    # 1) pathlib.Path
    if isinstance(payload, Path):
        return payload.read_text(encoding="utf-8", errors="replace")

    # 2) str (either a file path or HTML)
    if isinstance(payload, str):
        p = Path(payload)
        if p.exists() and p.is_file():
            return p.read_text(encoding="utf-8", errors="replace")
        return payload

    # 3) file-like
    read = getattr(payload, "read", None)
    if callable(read):
        data = read()
        if isinstance(data, bytes):
            return data.decode("utf-8", errors="replace")
        if isinstance(data, str):
            return data
        raise TypeError(
            "parse_html-decorated function returned a file-like object whose read() "
            f"returned {type(data)!r}; expected str or bytes"
        )

    raise TypeError(
        "parse_html-decorated function must return a file path, an HTML string, "
        "or a file-like object with .read(). "
        f"Received: {type(payload)!r}"
    )


def _require_bs4():
    try:
        from bs4 import BeautifulSoup  # noqa: F401
    except Exception as e:  # pragma: no cover
        raise RuntimeError(
            "beautifulsoup4 is required for etl_decorators.html. Install with: "
            "pip install etl-decorators[html]"
        ) from e


def _require_markdownify():
    try:
        from markdownify import markdownify as md  # noqa: F401
    except Exception as e:  # pragma: no cover
        raise RuntimeError(
            "markdownify is required for convert_to_markdown=True. Install with: "
            "pip install etl-decorators[html]"
        ) from e


def _to_markdown(html: str) -> str:
    _require_markdownify()
    from markdownify import markdownify as md

    return md(html)


@overload
def parse_html(fn: Callable[P, Any]) -> Callable[P, Any]: ...


@overload
def parse_html(
    fn: None = None,
    *,
    extract: str | None = None,
    extract_as_collection: bool = False,
    convert_to_markdown: bool = False,
) -> Callable[[Callable[P, Any]], Callable[P, Any]]: ...


def parse_html(
    fn: Callable[P, Any] | None = None,
    *,
    extract: str | None = None,
    extract_as_collection: bool = False,
    convert_to_markdown: bool = False,
):
    """Decorator that parses HTML outputs using BeautifulSoup.

    The decorated function can return:
    - a file path
    - an HTML string
    - a file-like resource (implements `.read()`)

    Parameters:
        extract: optional CSS selector (BeautifulSoup `select`).
        extract_as_collection: when True, returns all matches.
        convert_to_markdown: when True, convert the selected HTML (or full
            document if `extract is None`) to Markdown (requires markdownify).
    """

    def _decorate(f: Callable[P, Any]):
        is_async = inspect.iscoroutinefunction(f)

        if is_async:

            async def wrapped(*args: P.args, **kwargs: P.kwargs):  # type: ignore[misc]
                payload = await f(*args, **kwargs)
                return _process_payload(
                    payload,
                    extract=extract,
                    extract_as_collection=extract_as_collection,
                    convert_to_markdown=convert_to_markdown,
                )

        else:

            def wrapped(*args: P.args, **kwargs: P.kwargs):  # type: ignore[misc]
                payload = f(*args, **kwargs)
                return _process_payload(
                    payload,
                    extract=extract,
                    extract_as_collection=extract_as_collection,
                    convert_to_markdown=convert_to_markdown,
                )

        # Preserve basic metadata.
        wrapped.__name__ = getattr(f, "__name__", wrapped.__name__)
        wrapped.__qualname__ = getattr(f, "__qualname__", wrapped.__qualname__)
        wrapped.__doc__ = getattr(f, "__doc__", None)
        wrapped.__module__ = getattr(f, "__module__", None)
        wrapped.__wrapped__ = f

        return wrapped

    if fn is None:
        return _decorate
    return _decorate(fn)


def _process_payload(
    payload: Any,
    *,
    extract: str | None,
    extract_as_collection: bool,
    convert_to_markdown: bool,
):
    _require_bs4()
    from bs4 import BeautifulSoup

    html = _read_html_payload(payload)
    soup = BeautifulSoup(html, "html.parser")

    # No extraction: return soup or markdown of full doc.
    if extract is None:
        if convert_to_markdown:
            return _to_markdown(str(soup))
        return soup

    # Extraction (CSS selector)
    matches = soup.select(extract)

    if extract_as_collection:
        if convert_to_markdown:
            return [_to_markdown(str(tag)) for tag in matches]
        return matches

    if not matches:
        return None

    first = matches[0]
    if convert_to_markdown:
        return _to_markdown(str(first))
    return first


__all__ = ["parse_html"]
