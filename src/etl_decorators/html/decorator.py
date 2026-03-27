"""Decorator implementation for :pymod:`etl_decorators.html`."""

from __future__ import annotations

from typing import Any, Callable, ParamSpec, overload

from etl_decorators._base.decorators import DecoratorBase, OptionalFnDecoratorBase

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
    extract: str | tuple[str, ...] | None = None,
    extract_as_collection: bool = False,
    convert_to_markdown: bool = False,
) -> Callable[[Callable[P, Any]], Callable[P, Any]]: ...


def parse_html(
    fn: Callable[P, Any] | None = None,
    *,
    extract: str | tuple[str, ...] | None = None,
    extract_as_collection: bool = False,
    convert_to_markdown: bool = False,
):
    """Decorator that parses HTML outputs using BeautifulSoup.

    The decorated function can return:
    - a file path
    - an HTML string
    - a file-like resource (implements `.read()`)

    Parameters:
        extract: optional CSS selector (BeautifulSoup `select`), or a tuple of
            selectors. When a tuple is provided, the decorator returns one
            extracted value per selector.
        extract_as_collection: when True, returns all matches.
        convert_to_markdown: when True, convert the selected HTML (or full
            document if `extract is None`) to Markdown (requires markdownify).
    """

    class _ParseHTMLDecorator(DecoratorBase[P, Any, None]):
        def process_result(
            self,
            _fn: Callable[P, Any],
            result: Any,
            _args: tuple[Any, ...],
            _kwargs: dict[str, Any],
            _state: None,
        ) -> Any:
            return _process_payload(
                result,
                extract=extract,
                extract_as_collection=extract_as_collection,
                convert_to_markdown=convert_to_markdown,
            )

    binder = OptionalFnDecoratorBase()

    def _decorate(f: Callable[P, Any]):
        return _ParseHTMLDecorator().decorate(f)

    return binder.bind_optional(fn, _decorate)


def _process_payload(
    payload: Any,
    *,
    extract: str | tuple[str, ...] | None,
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
    if isinstance(extract, tuple):
        if extract_as_collection:
            # One list of matches per selector.
            per_selector = [soup.select(selector) for selector in extract]
            if convert_to_markdown:
                return [[_to_markdown(str(tag)) for tag in tags] for tags in per_selector]
            return per_selector

        # One first-match (or None) per selector.
        per_first: list[Any] = []
        for selector in extract:
            matches = soup.select(selector)
            if not matches:
                per_first.append(None)
                continue

            first = matches[0]
            if convert_to_markdown:
                per_first.append(_to_markdown(str(first)))
            else:
                per_first.append(first)
        return per_first

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
