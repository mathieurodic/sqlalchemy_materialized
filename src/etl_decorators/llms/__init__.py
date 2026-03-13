
"""LLM decorator helpers.

This subpackage provides a small wrapper around `litellm` so users can build
prompt functions and call them like regular Python functions.

Design goals:
- Support both `@llm` and `@llm(return_type=...)`.
- Decorated functions return a *prompt string*; calling them returns the model
  completion (string) or a structured Pydantic model.
- Support both sync (`def`) and async (`async def`) prompt builders.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import inspect
from typing import Any, Awaitable, Callable, Generic, ParamSpec, TypeVar, overload


P = ParamSpec("P")
T = TypeVar("T")


try:  # pragma: no cover (import is exercised in integration, tests monkeypatch)
    from pydantic import BaseModel
except Exception:  # pragma: no cover
    BaseModel = object  # type: ignore[assignment]


_PromptFn = Callable[P, str]
_AsyncPromptFn = Callable[P, Awaitable[str]]


_MISSING = object()


def _require_str_prompt(prompt: Any, *, fn_name: str) -> str:
    if not isinstance(prompt, str):
        raise TypeError(
            f"LLM-decorated function {fn_name} must return a str prompt; "
            f"received: {type(prompt)!r}"
        )
    return prompt


def _require_pydantic_model(return_type: Any) -> type[BaseModel]:
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


def _extract_text_content(resp: Any) -> str:
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


def _extract_structured(resp: Any, model: type[BaseModel]) -> BaseModel:
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
    content = _extract_text_content(resp)
    return model.model_validate_json(content)


def _response_format_arg(model: type[BaseModel]) -> Any:
    """Return the most compatible `response_format` value for the installed pydantic."""
    # litellm often accepts the model class, but some APIs require a JSON schema.
    # We'll pass the class (best DX) and allow litellm/provider to handle it.
    # If that fails for some users, they can pass custom completion_kwargs.
    return model


@dataclass(frozen=True)
class LLM:
    """Callable decorator that turns prompt builders into LLM calls."""

    model: str
    api_key: str | None = field(default=None, repr=False)
    completion_kwargs: dict[str, Any] = field(default_factory=dict)

    def _completion_kwargs(self) -> dict[str, Any]:
        return dict(self.completion_kwargs or {})

    @overload
    def __call__(self, fn: _PromptFn[P]) -> Callable[P, str]: ...

    @overload
    def __call__(self, fn: _AsyncPromptFn[P]) -> Callable[P, Awaitable[str]]: ...

    @overload
    def __call__(
        self, *, return_type: type[BaseModel]
    ) -> Callable[[_PromptFn[P]], Callable[P, BaseModel]]: ...

    @overload
    def __call__(
        self, *, return_type: type[BaseModel]
    ) -> Callable[[_AsyncPromptFn[P]], Callable[P, Awaitable[BaseModel]]]: ...

    def __call__(self, fn: Any = None, *, return_type: Any = _MISSING):
        """Decorate a prompt function.

        Supports:
        - `@llm`
        - `@llm(return_type=SomePydanticModel)`
        """

        if fn is None:

            def wrapper(f):
                return self._decorate(f, return_type=return_type)

            return wrapper

        return self._decorate(fn, return_type=return_type)

    def _decorate(self, fn: Any, *, return_type: Any):
        model_t: type[BaseModel] | None = None
        if return_type is not _MISSING:
            # If user explicitly passes return_type=None, we want a clear error.
            if return_type is None:
                _require_pydantic_model(return_type)
            else:
                model_t = _require_pydantic_model(return_type)

        is_async = inspect.iscoroutinefunction(fn)

        if is_async:

            async def wrapped(*args: P.args, **kwargs: P.kwargs):  # type: ignore[misc]
                prompt = await fn(*args, **kwargs)
                prompt = _require_str_prompt(prompt, fn_name=getattr(fn, "__name__", "<fn>"))
                return await self.request_async(prompt, return_type=model_t)

        else:

            def wrapped(*args: P.args, **kwargs: P.kwargs):  # type: ignore[misc]
                prompt = fn(*args, **kwargs)
                prompt = _require_str_prompt(prompt, fn_name=getattr(fn, "__name__", "<fn>"))
                return self.request(prompt, return_type=model_t)

        # Preserve basic metadata.
        wrapped.__name__ = getattr(fn, "__name__", wrapped.__name__)
        wrapped.__qualname__ = getattr(fn, "__qualname__", wrapped.__qualname__)
        wrapped.__doc__ = getattr(fn, "__doc__", None)
        wrapped.__module__ = getattr(fn, "__module__", None)
        wrapped.__wrapped__ = fn

        return wrapped

    def request(self, prompt: str, *, return_type: type[BaseModel] | None):
        try:
            import litellm
        except Exception as e:  # pragma: no cover
            raise RuntimeError(
                "litellm is required for etl_decorators.llms. Install with: "
                "pip install etl-decorators[llms]"
            ) from e

        kwargs = self._completion_kwargs()
        kwargs.setdefault("model", self.model)
        kwargs.setdefault("messages", [{"role": "user", "content": prompt}])
        if self.api_key is not None:
            kwargs.setdefault("api_key", self.api_key)

        if return_type is not None:
            kwargs.setdefault("response_format", _response_format_arg(return_type))

        resp = litellm.completion(**kwargs)

        if return_type is None:
            return _extract_text_content(resp)
        return _extract_structured(resp, return_type)

    async def request_async(
        self, prompt: str, *, return_type: type[BaseModel] | None
    ):
        try:
            import litellm
        except Exception as e:  # pragma: no cover
            raise RuntimeError(
                "litellm is required for etl_decorators.llms. Install with: "
                "pip install etl-decorators[llms]"
            ) from e

        kwargs = self._completion_kwargs()
        kwargs.setdefault("model", self.model)
        kwargs.setdefault("messages", [{"role": "user", "content": prompt}])
        if self.api_key is not None:
            kwargs.setdefault("api_key", self.api_key)

        if return_type is not None:
            kwargs.setdefault("response_format", _response_format_arg(return_type))

        # acompletion is optional in some litellm versions; provide a good error.
        if not hasattr(litellm, "acompletion"):
            raise RuntimeError(
                "Your litellm version does not expose `acompletion`. "
                "Upgrade litellm or use sync prompt functions."
            )

        resp = await litellm.acompletion(**kwargs)

        if return_type is None:
            return _extract_text_content(resp)
        return _extract_structured(resp, return_type)


__all__ = ["LLM"]
