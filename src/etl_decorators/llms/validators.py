"""Runtime validators for the LLM decorator."""

from __future__ import annotations

from typing import Any

from .pydantic_utils import BaseModel


def require_str_prompt(prompt: Any, *, fn_name: str) -> str:
    if not isinstance(prompt, str):
        raise TypeError(
            f"LLM-decorated function {fn_name} must return a str prompt; "
            f"received: {type(prompt)!r}"
        )
    return prompt


def require_pydantic_model(return_type: Any) -> type[BaseModel]:
    if return_type is None:
        raise TypeError("return_type cannot be None")
    if not isinstance(return_type, type):
        raise TypeError(
            "return_type must be a pydantic.BaseModel subclass; "
            f"received: {return_type!r}"
        )
    if not (BaseModel is not object and issubclass(return_type, BaseModel)):
        raise TypeError(
            "return_type must be a pydantic.BaseModel subclass; "
            f"received: {return_type!r}"
        )
    return return_type


__all__ = [
    "require_pydantic_model",
    "require_str_prompt",
]
