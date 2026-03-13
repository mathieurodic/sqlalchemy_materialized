"""Optional-dependency helpers for :pymod:`etl_decorators.html`.

The html subpackage is an *optional extra* (``etl-decorators[html]``). We keep
imports lazy and raise clear errors when dependencies are missing.
"""

from __future__ import annotations


def _require_bs4() -> None:
    try:
        from bs4 import BeautifulSoup  # noqa: F401
    except Exception as e:  # pragma: no cover
        raise RuntimeError(
            "beautifulsoup4 is required for etl_decorators.html. Install with: "
            "pip install etl-decorators[html]"
        ) from e


def _require_markdownify() -> None:
    try:
        from markdownify import markdownify as md  # noqa: F401
    except Exception as e:  # pragma: no cover
        raise RuntimeError(
            "markdownify is required for convert_to_markdown=True. Install with: "
            "pip install etl-decorators[html]"
        ) from e
