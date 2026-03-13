"""LLM decorator implementation."""

from __future__ import annotations

from dataclasses import dataclass, field
import inspect
from typing import Any, Awaitable, Callable, ParamSpec, overload

from .pydantic_utils import BaseModel
from .response_parsing import (
    extract_structured,
    extract_text_content,
    response_format_arg,
)
from .validators import require_pydantic_model, require_str_prompt


P = ParamSpec("P")


_PromptFn = Callable[P, str]
_AsyncPromptFn = Callable[P, Awaitable[str]]


_MISSING = object()


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
                require_pydantic_model(return_type)
            else:
                model_t = require_pydantic_model(return_type)

        is_async = inspect.iscoroutinefunction(fn)

        if is_async:

            async def wrapped(*args: P.args, **kwargs: P.kwargs):  # type: ignore[misc]
                prompt = await fn(*args, **kwargs)
                prompt = require_str_prompt(
                    prompt, fn_name=getattr(fn, "__name__", "<fn>")
                )
                return await self.request_async(prompt, return_type=model_t)

        else:

            def wrapped(*args: P.args, **kwargs: P.kwargs):  # type: ignore[misc]
                prompt = fn(*args, **kwargs)
                prompt = require_str_prompt(
                    prompt, fn_name=getattr(fn, "__name__", "<fn>")
                )
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
            kwargs.setdefault("response_format", response_format_arg(return_type))

        resp = litellm.completion(**kwargs)

        if return_type is None:
            return extract_text_content(resp)
        return extract_structured(resp, return_type)

    async def request_async(self, prompt: str, *, return_type: type[BaseModel] | None):
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
            kwargs.setdefault("response_format", response_format_arg(return_type))

        # acompletion is optional in some litellm versions; provide a good error.
        if not hasattr(litellm, "acompletion"):
            raise RuntimeError(
                "Your litellm version does not expose `acompletion`. "
                "Upgrade litellm or use sync prompt functions."
            )

        resp = await litellm.acompletion(**kwargs)

        if return_type is None:
            return extract_text_content(resp)
        return extract_structured(resp, return_type)


__all__ = ["LLM"]
