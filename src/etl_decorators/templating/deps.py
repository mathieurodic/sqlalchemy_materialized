"""Optional dependency guards for :mod:`etl_decorators.templating`."""

from __future__ import annotations


def _require_jinja2() -> None:
    try:
        import jinja2  # noqa: F401
    except Exception as e:  # pragma: no cover
        raise RuntimeError(
            "jinja2 is required for etl_decorators.templating. Install with: "
            "pip install etl-decorators[templating]"
        ) from e
