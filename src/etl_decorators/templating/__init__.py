"""Jinja2 templating helpers.

This subpackage provides a small decorator to render text templates using
Jinja2.

Optional dependency:

    pip install etl-decorators[templating]

Main entrypoint: :func:`etl_decorators.templating.template`.
"""

from __future__ import annotations

from .decorator import template

# Expose internals for unit tests (mirrors csv/html subpackage style)
from .payload import _read_template_payload


__all__ = [
    "template",
    "_read_template_payload",
]
