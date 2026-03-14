"""Core :func:`etl_decorators.json.parse_json` decorator implementation."""

from __future__ import annotations

import json
from typing import Any, Callable, ParamSpec, overload

from etl_decorators._base.decorators import DecoratorBase, OptionalFnDecoratorBase

from .payload import _read_json_payload


P = ParamSpec("P")


JSONValue = dict[str, Any] | list[Any]


@overload
def parse_json(fn: Callable[P, Any]) -> Callable[P, JSONValue]: ...


@overload
def parse_json(
    fn: None = None,
) -> Callable[[Callable[P, Any]], Callable[P, JSONValue]]: ...


def parse_json(fn: Callable[P, Any] | None = None):
    """Decorator that parses JSON outputs into Python values.

    The decorated function can return:
    - a file path (str/pathlib.Path)
    - a JSON string
    - a file-like resource (implements `.read()`)

    The decorated callable returns a `dict` or `list`.

    Errors:
        JSON parsing errors are raised as `ValueError` with helpful context.
    """

    class _ParseJSONDecorator(DecoratorBase[P, JSONValue, None]):
        def process_result(
            self,
            _fn: Callable[P, Any],
            result: Any,
            _args: tuple[Any, ...],
            _kwargs: dict[str, Any],
            _state: None,
        ) -> JSONValue:
            text, source_name = _read_json_payload(result)
            try:
                parsed = json.loads(text)
            except json.JSONDecodeError as e:
                loc = f" ({source_name})" if source_name else ""
                # include line/col for easier debugging
                raise ValueError(
                    f"Unable to parse JSON{loc}: {e.msg} (line {e.lineno} col {e.colno})"
                ) from e

            if isinstance(parsed, (dict, list)):
                return parsed  # type: ignore[return-value]

            # JSON is valid but not shaped as expected.
            loc = f" ({source_name})" if source_name else ""
            raise ValueError(
                "parse_json expects a JSON object or array" + loc + f"; got {type(parsed).__name__}"
            )

    binder = OptionalFnDecoratorBase()

    def _decorate(f: Callable[P, Any]):
        return _ParseJSONDecorator().decorate(f)

    return binder.bind_optional(fn, _decorate)
