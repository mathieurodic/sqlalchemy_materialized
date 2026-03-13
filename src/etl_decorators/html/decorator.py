"""Decorator implementation for :pymod:`etl_decorators.html`."""

from __future__ import annotations

import inspect
from typing import Any, Callable, ParamSpec, overload

from .deps import _require_bs4
from .markdown import _to_markdown
from .payload import _read_html_payload


P = ParamSpec("P")


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
