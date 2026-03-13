"""Optional dependency guards for :mod:`etl_decorators.csv`."""

from __future__ import annotations


def _require_pandas() -> None:
    try:
        import pandas  # noqa: F401
    except Exception as e:  # pragma: no cover
        raise RuntimeError(
            "pandas is required for etl_decorators.csv. Install with: "
            "pip install etl-decorators[csv]"
        ) from e
