# `etl_decorators.redis.RedisCache`

Redis-backed memoization.

`etl_decorators.redis.RedisCache` lets you cache *function return values* in
Redis, based on a stable hash of bound arguments.

It supports both sync and async functions.

## Installation

```bash
pip install etl-decorators[redis]
```

## Basic usage

Create a cache instance with connection parameters, then use it as a decorator
factory:

```python
from etl_decorators.redis import RedisCache


cache = RedisCache(
    url="redis://localhost:6379/0",
    prefix="etl_constructor.redis.cache",  # optional
)


@cache(read_ttl=3600, write_ttl=3600, serialization="pickle")
def compute(x: int) -> dict:
    return {"x": x, "y": x * 2}
```

On the first call for a given argument set, the function runs and its return
value is persisted in Redis. Subsequent calls return the cached value.

## Cache keys

The Redis key is:

```text
{prefix}:{function_code_hash}:{arguments_tuple_hash}
```

Notes:

- `prefix` is configurable (constructor parameter).
- `function_code_hash` is a best-effort hash based on the function's source
  code extracted from the source file, starting **immediately below the
  `@cache(...)` line** (comments ignored). This means:
  - decorators *above* `@cache(...)` do not affect cache invalidation
  - decorators *below* `@cache(...)` do affect cache invalidation
  If the source file cannot be read or the function block cannot be extracted,
  decoration fails with a clear `RuntimeError`.
- You can opt out of code-based invalidation by passing
  `key_depends_on_function_code=False` to `@cache(...)`. In that case the
  `function_code_hash` segment becomes empty and the key depends only on
  `prefix` and the argument hash.
- `arguments_tuple_hash` is computed from the function signature + bound
  args/kwargs (defaults applied).

## TTL semantics

- `write_ttl`: expiry set when writing a newly computed value.
- `read_ttl`: expiry refreshed when reading a cached value.

Both accept either seconds (`float`) or a `datetime.timedelta`.

## Serialization

Choose:

- `serialization="pickle"` (default): supports arbitrary Python objects
- `serialization="json"`: stores JSON bytes (requires JSON-serializable values)

## Bypass / refresh

By default the wrapper supports two special keyword arguments:

- `cache=False`: bypass Redis for this call
- `refresh=True`: recompute and overwrite the cached value

These keywords are removed from `kwargs` before calling the wrapped function.

## Error handling

By default, Redis errors are **fail-open**: if Redis is unavailable, the
wrapped function is executed and its return value is returned (without caching).

If you prefer to fail the call when Redis errors occur, set `fail_open=False`.
