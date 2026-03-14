"""Optional dependency guards for :mod:`etl_decorators.redis`."""

from __future__ import annotations


def _require_redis() -> None:
    try:
        import redis  # noqa: F401
    except Exception as e:  # pragma: no cover
        raise RuntimeError(
            "redis is required for etl_decorators.redis. Install with: "
            "pip install etl-decorators[redis]"
        ) from e
