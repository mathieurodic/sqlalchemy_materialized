"""Core :func:`etl_decorators.streams.dedupe` decorator implementation."""

from __future__ import annotations

from collections.abc import AsyncIterable, AsyncIterator, Hashable, Iterable, Iterator
from typing import Any, Callable, ParamSpec, TypeVar, overload

from etl_decorators._base.decorators import DecoratorBase, OptionalFnDecoratorBase
from etl_decorators._base.streams import _is_async_iterable, _require_stream


P = ParamSpec("P")
T = TypeVar("T")


def _identity(x: T) -> T:
    return x


@overload
def dedupe(fn: Callable[P, Iterable[T] | AsyncIterable[T]]) -> Callable[P, Iterator[T] | AsyncIterator[T]]: ...


@overload
def dedupe(
    fn: None = None,
    *,
    key: Callable[[T], Hashable] = _identity,
) -> Callable[[Callable[P, Iterable[T] | AsyncIterable[T]]], Callable[P, Iterator[T] | AsyncIterator[T]]]: ...


def dedupe(
    fn: Callable[P, Iterable[T] | AsyncIterable[T]] | None = None,
    *,
    key: Callable[[T], Hashable] = _identity,
):
    """Remove duplicates from a stream for the lifetime of the iterator.

    Parameters:
        key: function returning a hashable key to compare items. By default the
            item itself is used (requires items to be hashable).
    """

    class _DedupeDecorator(DecoratorBase[P, Any, None]):
        def process_result(
            self,
            f: Callable[P, Any],
            result: Any,
            _args: tuple[Any, ...],
            _kwargs: dict[str, Any],
            _state: None,
        ):
            stream = _require_stream(result, fn_name=getattr(f, "__name__", None))
            if _is_async_iterable(stream):
                return _dedupe_async(stream, key=key)
            return _dedupe_sync(stream, key=key)

    binder = OptionalFnDecoratorBase()

    def _decorate(f: Callable[P, Iterable[T] | AsyncIterable[T]]):
        return _DedupeDecorator().decorate(f)

    return binder.bind_optional(fn, _decorate)


def _dedupe_sync(stream: Iterable[T], *, key: Callable[[T], Hashable]) -> Iterator[T]:
    seen: set[Hashable] = set()
    for item in stream:
        k = key(item)
        if k in seen:
            continue
        seen.add(k)
        yield item


async def _dedupe_async(
    stream: AsyncIterable[T], *, key: Callable[[T], Hashable]
) -> AsyncIterator[T]:
    seen: set[Hashable] = set()
    async for item in stream:
        k = key(item)
        if k in seen:
            continue
        seen.add(k)
        yield item
