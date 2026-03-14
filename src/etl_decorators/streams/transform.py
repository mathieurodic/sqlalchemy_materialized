"""Core :func:`etl_decorators.streams.transform` decorator implementation."""

from __future__ import annotations

import inspect
from collections.abc import AsyncIterable, AsyncIterator, Awaitable, Iterable, Iterator
from typing import Any, Callable, ParamSpec, TypeVar, overload

from etl_decorators._base.decorators import DecoratorBase
from etl_decorators._base.streams import _is_async_iterable, _require_stream


P = ParamSpec("P")
T = TypeVar("T")
U = TypeVar("U")


@overload
def transform(mapper: Callable[[T], U]):
    ...


def transform(mapper: Callable[[T], U] | Callable[[T], Awaitable[U]]):
    """Map items yielded from a stream.

    This is a decorator factory:

    .. code-block:: python

        @transform(lambda x: x + 1)
        def xs() -> Iterator[int]:
            yield from range(3)

    The decorated function must return an ``Iterable[T]`` or ``AsyncIterable[T]``.

    Async streams support both sync and async mappers:
    - if mapper returns an awaitable, it is awaited per item.
    """

    class _TransformDecorator(DecoratorBase[P, Any, None]):
        def process_result(
            self,
            fn: Callable[P, Any],
            result: Any,
            _args: tuple[Any, ...],
            _kwargs: dict[str, Any],
            _state: None,
        ):
            stream = _require_stream(result, fn_name=getattr(fn, "__name__", None))
            if _is_async_iterable(stream):
                return _map_async(stream, mapper=mapper)
            return _map_sync(stream, mapper=mapper)  # type: ignore[arg-type]

    def _decorate(f: Callable[P, Iterable[T] | AsyncIterable[T]]):
        return _TransformDecorator().decorate(f)

    return _decorate


def _map_sync(stream: Iterable[T], *, mapper: Callable[[T], U]) -> Iterator[U]:
    for item in stream:
        yield mapper(item)


async def _map_async(
    stream: AsyncIterable[T], *, mapper: Callable[[T], U] | Callable[[T], Awaitable[U]]
) -> AsyncIterator[U]:
    async for item in stream:
        out = mapper(item)
        if inspect.isawaitable(out):
            yield await out  # type: ignore[misc]
        else:
            yield out  # type: ignore[misc]
