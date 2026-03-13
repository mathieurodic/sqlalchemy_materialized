"""Helpers to parse text or structured data out of litellm responses."""

from __future__ import annotations

from typing import Any

from .pydantic_utils import BaseModel


def extract_text_content(resp: Any) -> str:
    """Best-effort extraction of a text completion from a litellm response."""

    try:
        # OpenAI-compatible: resp["choices"][0]["message"]["content"]
        return resp["choices"][0]["message"]["content"]
    except Exception:
        pass
    try:
        # Some providers: resp.choices[0].message.content
        return resp.choices[0].message.content
    except Exception:
        pass

    raise TypeError(
        "Unable to extract text completion from litellm response. "
        f"Response type: {type(resp)!r}"
    )


def extract_structured(resp: Any, model: type[BaseModel]) -> BaseModel:
    """Best-effort extraction of structured output from a litellm response."""

    # Common patterns:
    # - resp["choices"][0]["message"]["parsed"]
    # - resp.choices[0].message.parsed
    # - resp is already a dict conforming to the model
    try:
        parsed = resp["choices"][0]["message"].get("parsed")
        if parsed is not None:
            return model.model_validate(parsed)
    except Exception:
        pass
    try:
        parsed = resp.choices[0].message.parsed
        if parsed is not None:
            return model.model_validate(parsed)
    except Exception:
        pass

    # Fallback: parse from content as JSON/text.
    content = extract_text_content(resp)
    return model.model_validate_json(content)


def response_format_arg(model: type[BaseModel]) -> Any:
    """Return the most compatible `response_format` value.

    litellm often accepts the model class, but some APIs require a JSON schema.
    We pass the class (best DX) and allow litellm/provider to handle it.
    """

    return model


__all__ = [
    "extract_structured",
    "extract_text_content",
    "response_format_arg",
]
