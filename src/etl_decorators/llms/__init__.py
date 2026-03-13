
"""LLM decorator helpers.

This subpackage provides a small wrapper around `litellm` so users can build
prompt functions and call them like regular Python functions.
"""

from __future__ import annotations

from .llm import LLM


__all__ = ["LLM"]
