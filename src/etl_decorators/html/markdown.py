"""Markdown conversion helpers for :pymod:`etl_decorators.html`."""

from __future__ import annotations

from .deps import _require_markdownify


def _to_markdown(html: str) -> str:
    _require_markdownify()
    from markdownify import markdownify as md

    return md(html)
