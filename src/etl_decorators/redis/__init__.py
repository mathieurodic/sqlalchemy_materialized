"""Redis-backed caching decorators.

This subpackage is an optional extra:

    pip install etl-decorators[redis]

It provides :class:`etl_decorators.redis.RedisCache`, a small Redis-backed
memoization helper that can decorate sync and async functions.
"""

from __future__ import annotations

from .cache import RedisCache

__all__ = ["RedisCache"]
