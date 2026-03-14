"""Core :func:`etl_decorators.streams.batch` decorator implementation."""

from __future__ import annotations

from collections.abc import AsyncIterable, AsyncIterator, Iterable, Iterator
from typing import Any, Callable, ParamSpec, TypeVar, overload

from etl_decorators._base.decorators import DecoratorBase, OptionalFnDecoratorBase
from etl_decorators._base.streams import _is_async_iterable, _require_stream


P = ParamSpec("P")
T = TypeVar("T")


@overload
def batch(
    fn: None = None,
    *,
    size: int,
    drop_last: bool = False,
) -> Callable[[Callable[P, Iterable[T] | AsyncIterable[T]]], Callable[P, Iterator[list[T]] | AsyncIterator[list[T]]]]: ...


def batch(
    fn: Callable[P, Iterable[T] | AsyncIterable[T]] | None = None,
    *,
    size: int,
    drop_last: bool = False,
):
    """Batch a stream into fixed-size lists.

    The decorated function must return an :class:`collections.abc.Iterable` or
    an :class:`collections.abc.AsyncIterable`.

    Parameters:
        size: batch size (must be > 0).
        drop_last: when True, drop the final incomplete batch.
    """

    if size <= 0:
        raise ValueError("batch(size=...): size must be > 0")

    class _BatchDecorator(DecoratorBase[P, Any, None]):
        def process_result(
            self,
            _fn: Callable[P, Any],
            result: Any,
            _args: tuple[Any, ...],
            _kwargs: dict[str, Any],
            _state: None,
        ):
            stream = _require_stream(result, fn_name=getattr(_fn, "__name__", None))
            if _is_async_iterable(stream):
                return _batch_async(stream, size=size, drop_last=drop_last)
            return _batch_sync(stream, size=size, drop_last=drop_last)

    binder = OptionalFnDecoratorBase()

    def _decorate(f: Callable[P, Iterable[T] | AsyncIterable[T]]):
        return _BatchDecorator().decorate(f)

    return binder.bind_optional(fn, _decorate)


def _batch_sync(stream: Iterable[T], *, size: int, drop_last: bool) -> Iterator[list[T]]:
    buf: list[T] = []
    for item in stream:
        buf.append(item)
        if len(buf) >= size:
            yield buf
            buf = []
    if buf and not drop_last:
        yield buf


async def _batch_async(
    stream: AsyncIterable[T], *, size: int, drop_last: bool
) -> AsyncIterator[list[T]]:
    buf: list[T] = []
    async for item in stream:
        buf.append(item)
        if len(buf) >= size:
            yield buf
            buf = []
    if buf and not drop_last:
        yield buf
