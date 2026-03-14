# `etl_decorators.redis`

Redis-backed caching helpers.

This package is an **optional extra**:

```bash
pip install etl-decorators[redis]
```

## User documentation

User-facing usage and examples live under `doc/`:

- [`doc/cache_redis.md`](../../../doc/cache_redis.md)

## Optional dependencies & lazy imports

This subpackage does **not** import `redis` at import time.
Instead, `redis` is required only when a Redis-decorated function is executed.

If the dependency is missing, the code raises a `RuntimeError` with an install
hint (e.g. `pip install etl-decorators[redis]`).

## Code layout

The implementation is split into small modules:

- [`cache.py`](cache.py): `RedisCache` decorator
- [`deps.py`](deps.py): `_require_redis` (dependency guard)
- [`hashing.py`](hashing.py): callsite + argument hashing
- [`serde.py`](serde.py): pickle/json serialization

## Tests

Tests live under [`tests/redis/`](../../../tests/redis/).

Unit tests use [`fakeredis`](https://pypi.org/project/fakeredis/).
Integration tests are environment-gated via `ETL_DECORATORS_TESTS_REDIS_URL`.

[`__init__.py`](__init__.py) re-exports `RedisCache` to preserve a stable
import path:

```python
from etl_decorators.redis import RedisCache
```
