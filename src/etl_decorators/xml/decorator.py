"""Decorator implementation for :pymod:`etl_decorators.xml`."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Callable, Literal, ParamSpec, overload

from etl_decorators._base.decorators import DecoratorBase, OptionalFnDecoratorBase

from .deps import _require_cssselect, _require_lxml
from .payload import _read_xml_payload


P = ParamSpec("P")


ExtractMode = Literal["auto", "xpath", "css"]
ExtractOutput = Literal["text", "elements"]


@overload
def parse_xml(fn: Callable[P, Any]) -> Callable[P, Any]: ...


@overload
def parse_xml(
    fn: None = None,
    *,
    extract: str | None = None,
    extract_mode: ExtractMode = "auto",
    extract_as_collection: bool = False,
    extract_output: ExtractOutput = "text",
    namespaces: Mapping[str, str] | None = None,
) -> Callable[[Callable[P, Any]], Callable[P, Any]]: ...


def parse_xml(
    fn: Callable[P, Any] | None = None,
    *,
    extract: str | None = None,
    extract_mode: ExtractMode = "auto",
    extract_as_collection: bool = False,
    extract_output: ExtractOutput = "text",
    namespaces: Mapping[str, str] | None = None,
):
    """Decorator that parses XML outputs using `lxml`.

    The decorated function can return:
    - a file path
    - an XML string
    - a file-like resource (implements `.read()`)

    Parameters:
        extract:
            Optional selector string. When `extract_mode` is "xpath", the
            selector is evaluated as XPath. When it is "css", it is evaluated
            as a CSS selector (via `cssselect`). When it is "auto", a small
            heuristic chooses between XPath and CSS.
        extract_mode:
            "auto" (default), "xpath", or "css".
        extract_as_collection:
            When True, returns all matches (as a list, possibly empty).
            When False, returns the first match, or None if there are no matches.
        extract_output:
            "text" (default) returns extracted values as strings (elements are
            converted using `.itertext()`). "elements" returns element objects.
        namespaces:
            Optional mapping of XML namespace prefixes to URIs.
    """

    class _ParseXMLDecorator(DecoratorBase[P, Any, None]):
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
                extract_mode=extract_mode,
                extract_as_collection=extract_as_collection,
                extract_output=extract_output,
                namespaces=namespaces,
            )

    binder = OptionalFnDecoratorBase()

    def _decorate(f: Callable[P, Any]):
        return _ParseXMLDecorator().decorate(f)

    return binder.bind_optional(fn, _decorate)


def _process_payload(
    payload: Any,
    *,
    extract: str | None,
    extract_mode: ExtractMode,
    extract_as_collection: bool,
    extract_output: ExtractOutput,
    namespaces: Mapping[str, str] | None,
):
    _require_lxml()
    from lxml import etree

    xml_text, source_name = _read_xml_payload(payload)

    try:
        root = etree.fromstring(xml_text.encode("utf-8"))
    except Exception as e:
        loc = f" ({source_name})" if source_name else ""
        raise ValueError(f"Unable to parse XML{loc}: {e}") from e

    if extract is None:
        return root

    mode = _resolve_extract_mode(extract, extract_mode)
    raw = _extract(root, extract, mode=mode, namespaces=namespaces)
    values = _normalize_extracted(raw)
    out = _apply_collection_semantics(values, extract_as_collection=extract_as_collection)
    return _coerce_extract_output(out, extract_output=extract_output)


def _resolve_extract_mode(extract: str, extract_mode: ExtractMode) -> Literal["xpath", "css"]:
    if extract_mode != "auto":
        return extract_mode

    # Heuristic: if it looks like XPath, treat it as XPath.
    # This is intentionally conservative: many XPath expressions contain '/',
    # '@', '::', functions, predicates, etc.
    xpath_tokens = ["//", "/", "@", "::", "text()", "[", "("]
    if any(tok in extract for tok in xpath_tokens):
        return "xpath"
    return "css"


def _extract(
    root: Any,
    selector: str,
    *,
    mode: Literal["xpath", "css"],
    namespaces: Mapping[str, str] | None,
) -> Any:
    if mode == "xpath":
        # lxml's xpath returns list|scalar depending on expression.
        return root.xpath(selector, namespaces=dict(namespaces or {}))

    # css
    _require_cssselect()
    from lxml.cssselect import CSSSelector

    sel = CSSSelector(selector, namespaces=dict(namespaces or {}))
    return sel(root)


def _normalize_extracted(raw: Any) -> list[Any]:
    """Return a list of extracted values (elements or scalars)."""
    if raw is None:
        return []
    if isinstance(raw, list):
        return raw
    # xpath scalar result
    return [raw]


def _apply_collection_semantics(values: list[Any], *, extract_as_collection: bool) -> Any:
    if extract_as_collection:
        return values
    return values[0] if values else None


def _coerce_extract_output(out: Any, *, extract_output: ExtractOutput) -> Any:
    if extract_output == "elements":
        return out

    def _to_text(x: Any) -> str:
        # lxml elements have .itertext(); but keep this duck-typed so unit tests
        # can use fake elements.
        it = getattr(x, "itertext", None)
        if callable(it):
            return "".join(it())
        # xpath can return strings, numbers, booleans
        return str(x)

    if out is None:
        return None
    if isinstance(out, list):
        return [_to_text(x) for x in out]
    return _to_text(out)
