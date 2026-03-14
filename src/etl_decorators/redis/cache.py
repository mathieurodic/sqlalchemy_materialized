"""Redis-backed memoization decorator."""

from __future__ import annotations

import inspect
import math
import sys
from dataclasses import dataclass, field
from datetime import timedelta
from functools import update_wrapper
from typing import Any, Callable, ParamSpec, TypeVar, overload

from .deps import _require_redis
from .hashing import arguments_tuple_hash, callsite_code_hash
from .serde import Serialization, dumps, loads


def _bool_option(value: Any, *, name: str) -> bool:
    if isinstance(value, bool):
        return value
    raise TypeError(f"{name} must be a bool when provided")


P = ParamSpec("P")
R = TypeVar("R")


def _ttl_seconds(value: timedelta | float | None) -> int | None:
    if value is None:
        return None
    if isinstance(value, timedelta):
        seconds = value.total_seconds()
    else:
        seconds = float(value)
    if seconds <= 0:
        raise ValueError("TTL must be > 0")
    # Redis TTL is seconds, integer. Round up to avoid silently producing 0.
    return int(math.ceil(seconds))


@dataclass(frozen=True)
class RedisCache:
    """Callable decorator providing Redis-backed caching.

    The instance itself can be used as a decorator factory, similar to
    :class:`etl_decorators.llms.LLM`.

    Cache key format:

        ``{prefix}:{function_code_hash}:{arguments_tuple_hash}``

    Where the function code hash is a best-effort hash based on source code
    (comments ignored), and args hash is computed from bound args/kwargs.
    """

    url: str
    prefix: str = "etl_constructor.redis.cache"
    client_kwargs: dict[str, Any] = field(default_factory=dict)

    def _key(
        self,
        *,
        code_hash: str,
        fn: Callable[..., Any],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> str:
        return f"{self.prefix}:{code_hash}:{arguments_tuple_hash(fn, args, kwargs)}"

    def _sync_client(self):
        _require_redis()
        import redis

        return redis.Redis.from_url(self.url, **(self.client_kwargs or {}))

    def _async_client(self):
        _require_redis()
        import redis.asyncio as redis_async

        return redis_async.Redis.from_url(self.url, **(self.client_kwargs or {}))

    # `fn` overloads
    @overload
    def __call__(self, fn: Callable[P, R]) -> Callable[P, R]: ...

    @overload
    def __call__(
        self,
        fn: None = None,
        *,
        read_ttl: timedelta | float = timedelta(hours=1),
        write_ttl: timedelta | float = timedelta(hours=1),
        serialization: Serialization = "pickle",
        fail_open: bool = True,
        bypass_kw: str | None = "cache",
        refresh_kw: str | None = "refresh",
    ) -> Callable[[Callable[P, R]], Callable[P, R]]: ...

    def __call__(
        self,
        fn: Callable[P, R] | None = None,
        *,
        read_ttl: timedelta | float = timedelta(hours=1),
        write_ttl: timedelta | float = timedelta(hours=1),
        serialization: Serialization = "pickle",
        fail_open: bool = True,
        bypass_kw: str | None = "cache",
        refresh_kw: str | None = "refresh",
    ):
        # Capture call site of `@cache(...)`.
        frame = sys._getframe(1)
        callsite_filename = frame.f_code.co_filename
        callsite_end_lineno = frame.f_lineno

        if fn is None:

            def wrapper(f: Callable[P, R]) -> Callable[P, R]:
                return self._decorate(
                    f,
                    callsite_filename=callsite_filename,
                    callsite_end_lineno=callsite_end_lineno,
                    read_ttl=read_ttl,
                    write_ttl=write_ttl,
                    serialization=serialization,
                    fail_open=fail_open,
                    bypass_kw=bypass_kw,
                    refresh_kw=refresh_kw,
                )

            return wrapper

        return self._decorate(
            fn,
            callsite_filename=callsite_filename,
            callsite_end_lineno=callsite_end_lineno,
            read_ttl=read_ttl,
            write_ttl=write_ttl,
            serialization=serialization,
            fail_open=fail_open,
            bypass_kw=bypass_kw,
            refresh_kw=refresh_kw,
        )

    def _decorate(
        self,
        fn: Callable[P, R],
        *,
        callsite_filename: str,
        callsite_end_lineno: int,
        read_ttl: timedelta | float,
        write_ttl: timedelta | float,
        serialization: Serialization,
        fail_open: bool,
        bypass_kw: str | None,
        refresh_kw: str | None,
    ) -> Callable[P, R]:
        code_hash = callsite_code_hash(
            filename=callsite_filename,
            start_lineno=callsite_end_lineno + 1,
        )
        read_ttl_s = _ttl_seconds(read_ttl)
        write_ttl_s = _ttl_seconds(write_ttl)

        if serialization not in ("pickle", "json"):
            raise ValueError("serialization must be 'pickle' or 'json'")

        is_async = inspect.iscoroutinefunction(fn)
        self_outer = self

        # Note: we don't use the project's `DecoratorBase` helper here because
        # caching needs to wrap the full call (including reading from Redis
        # *before* calling the function).

        if is_async:

            async def wrapped(*args: P.args, **kwargs: P.kwargs) -> R:  # type: ignore[misc]
                bypass = None
                if bypass_kw is not None and bypass_kw in kwargs:
                    bypass = kwargs.pop(bypass_kw)

                if bypass is not None:
                    bypass = _bool_option(bypass, name=bypass_kw or "cache")

                if bypass is False:
                    return await fn(*args, **kwargs)  # type: ignore[misc]

                refresh = False
                if refresh_kw is not None and refresh_kw in kwargs:
                    refresh = _bool_option(kwargs.pop(refresh_kw), name=refresh_kw)

                key = self_outer._key(code_hash=code_hash, fn=fn, args=args, kwargs=kwargs)

                try:
                    client = self_outer._async_client()
                    if not refresh:
                        cached = await client.get(key)
                        if cached is not None:
                            if read_ttl_s is not None:
                                # refresh TTL on read
                                await client.expire(key, read_ttl_s)
                            return loads(cached, serialization=serialization)
                except Exception:
                    if not fail_open:
                        raise
                    cached = None

                # compute
                result = await fn(*args, **kwargs)  # type: ignore[misc]

                try:
                    client = self_outer._async_client()
                    payload = dumps(result, serialization=serialization)
                    if write_ttl_s is not None:
                        await client.setex(key, write_ttl_s, payload)
                    else:
                        await client.set(key, payload)
                except Exception:
                    if not fail_open:
                        raise

                return result

        else:

            def wrapped(*args: P.args, **kwargs: P.kwargs) -> R:  # type: ignore[misc]
                bypass = None
                if bypass_kw is not None and bypass_kw in kwargs:
                    bypass = kwargs.pop(bypass_kw)

                if bypass is not None:
                    bypass = _bool_option(bypass, name=bypass_kw or "cache")

                if bypass is False:
                    return fn(*args, **kwargs)

                refresh = False
                if refresh_kw is not None and refresh_kw in kwargs:
                    refresh = _bool_option(kwargs.pop(refresh_kw), name=refresh_kw)

                key = self_outer._key(code_hash=code_hash, fn=fn, args=args, kwargs=kwargs)

                try:
                    client = self_outer._sync_client()
                    if not refresh:
                        cached = client.get(key)
                        if cached is not None:
                            if read_ttl_s is not None:
                                client.expire(key, read_ttl_s)
                            return loads(cached, serialization=serialization)
                except Exception:
                    if not fail_open:
                        raise

                result = fn(*args, **kwargs)

                try:
                    client = self_outer._sync_client()
                    payload = dumps(result, serialization=serialization)
                    if write_ttl_s is not None:
                        client.setex(key, write_ttl_s, payload)
                    else:
                        client.set(key, payload)
                except Exception:
                    if not fail_open:
                        raise

                return result

        # preserve metadata
        update_wrapper(wrapped, fn)
        return wrapped


__all__ = ["RedisCache"]
