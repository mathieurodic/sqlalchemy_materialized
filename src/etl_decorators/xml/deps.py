"""Optional-dependency helpers for :pymod:`etl_decorators.xml`.

The xml subpackage is an *optional extra* (``etl-decorators[xml]``). We keep
imports lazy and raise clear errors when dependencies are missing.
"""

from __future__ import annotations


def _require_lxml() -> None:
    try:
        import lxml.etree  # noqa: F401
    except Exception as e:  # pragma: no cover
        raise RuntimeError(
            "lxml is required for etl_decorators.xml. Install with: "
            "pip install etl-decorators[xml]"
        ) from e


def _require_cssselect() -> None:
    try:
        import cssselect  # noqa: F401
    except Exception as e:  # pragma: no cover
        raise RuntimeError(
            "cssselect is required for extract_mode='css'. Install with: "
            "pip install etl-decorators[xml]"
        ) from e
