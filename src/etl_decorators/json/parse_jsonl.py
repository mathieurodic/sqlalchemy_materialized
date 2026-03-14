"""Core :func:`etl_decorators.json.parse_jsonl` decorator implementation."""

from __future__ import annotations

import json
from collections.abc import Iterator
from typing import Any, Callable, ParamSpec, overload

from etl_decorators._base.decorators import DecoratorBase, OptionalFnDecoratorBase

from .payload import _iter_jsonl_payload


P = ParamSpec("P")


JSONLineValue = dict[str, Any] | list[Any]


@overload
def parse_jsonl(fn: Callable[P, Any]) -> Callable[P, Iterator[JSONLineValue]]: ...


@overload
def parse_jsonl(
    fn: None = None,
) -> Callable[[Callable[P, Any]], Callable[P, Iterator[JSONLineValue]]]: ...


def parse_jsonl(fn: Callable[P, Any] | None = None):
    """Decorator that parses JSONL outputs into an iterator of values.

    The decorated function can return:
    - a file path (str/pathlib.Path)
    - a JSONL string
    - a file-like resource

    The decorated callable returns an iterator of `dict | list`.

    Behavior:
        - blank/whitespace-only lines are skipped

    Errors:
        JSON decoding errors are raised as `ValueError` including line number
        and source info when available.
    """

    class _ParseJSONLDecorator(DecoratorBase[P, Iterator[JSONLineValue], None]):
        def process_result(
            self,
            _fn: Callable[P, Any],
            result: Any,
            _args: tuple[Any, ...],
            _kwargs: dict[str, Any],
            _state: None,
        ) -> Iterator[JSONLineValue]:
            lines, source_name = _iter_jsonl_payload(result)
            return _iter_parsed(lines, source_name=source_name)

    binder = OptionalFnDecoratorBase()

    def _decorate(f: Callable[P, Any]):
        return _ParseJSONLDecorator().decorate(f)

    return binder.bind_optional(fn, _decorate)


def _iter_parsed(lines: Iterator[str], *, source_name: str | None) -> Iterator[JSONLineValue]:
    for idx, raw in enumerate(lines, start=1):
        ln = raw.strip()
        if not ln:
            continue
        try:
            parsed = json.loads(ln)
        except json.JSONDecodeError as e:
            loc = f" ({source_name})" if source_name else ""
            raise ValueError(
                f"Unable to parse JSONL{loc} at line {idx}: {e.msg} (line {e.lineno} col {e.colno})"
            ) from e

        if isinstance(parsed, (dict, list)):
            yield parsed  # type: ignore[misc]
            continue

        loc = f" ({source_name})" if source_name else ""
        raise ValueError(
            f"parse_jsonl expects each line to be a JSON object or array{loc}; got {type(parsed).__name__}"
        )
