"""Core :func:`etl_decorators.streams.keep` decorator implementation."""

from __future__ import annotations

import inspect
from collections.abc import AsyncIterable, AsyncIterator, Awaitable, Iterable, Iterator
from typing import Any, Callable, ParamSpec, TypeVar, overload

from etl_decorators._base.decorators import DecoratorBase
from etl_decorators._base.streams import _is_async_iterable, _require_stream


P = ParamSpec("P")
T = TypeVar("T")


@overload
def keep(predicate: Callable[[T], bool]):
    ...


def keep(predicate: Callable[[T], bool] | Callable[[T], Awaitable[bool]]):
    """Keep items yielded from a stream when *predicate(item)* is truthy.

    This is a decorator factory:

    .. code-block:: python

        @keep(lambda x: x % 2 == 0)
        def xs() -> Iterator[int]:
            yield from range(5)

    The decorated function must return an ``Iterable[T]`` or ``AsyncIterable[T]``.

    Async streams support both sync and async predicates:
    - if the predicate returns an awaitable, it is awaited per item.
    """

    class _KeepDecorator(DecoratorBase[P, Any, None]):
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
                return _keep_async(stream, predicate=predicate)
            return _keep_sync(stream, predicate=predicate)  # type: ignore[arg-type]

    def _decorate(f: Callable[P, Iterable[T] | AsyncIterable[T]]):
        return _KeepDecorator().decorate(f)

    return _decorate


def _keep_sync(stream: Iterable[T], *, predicate: Callable[[T], bool]) -> Iterator[T]:
    for item in stream:
        if predicate(item):
            yield item


async def _keep_async(
    stream: AsyncIterable[T], *, predicate: Callable[[T], bool] | Callable[[T], Awaitable[bool]]
) -> AsyncIterator[T]:
    async for item in stream:
        out = predicate(item)
        if inspect.isawaitable(out):
            ok = await out  # type: ignore[misc]
        else:
            ok = out  # type: ignore[misc]

        if ok:
            yield item
