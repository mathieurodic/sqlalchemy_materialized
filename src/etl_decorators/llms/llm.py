"""LLM decorator implementation.

Notes on typing / interoperability
-------------------------------

Some other decorators in this repository (notably
``etl_decorators.sqlalchemy.materialized_property``) inspect the *runtime*
return annotation of the decorated callable (via ``__annotations__`` and/or
``typing.get_type_hints``) to decide how to store results.

When decorating prompt builders, :class:`~etl_decorators.llms.LLM` therefore:

- optionally infers a structured output ``return_type`` from the function's
  annotated return type when it is a ``pydantic.BaseModel`` subclass, and
- rewrites the wrapped callable's runtime ``__annotations__["return"]`` to
  reflect the actual output type of the decorated callable.

This makes decorator stacking possible (e.g. ``@materialized_property`` on top
of an ``@llm(return_type=...)`` function).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, ParamSpec, get_type_hints, overload

from etl_decorators._base.decorators import DecoratorBase

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

        # 1) Explicit return_type=... always wins.
        if return_type is not _MISSING:
            # If user explicitly passes return_type=None, we want a clear error.
            if return_type is None:
                require_pydantic_model(return_type)
            else:
                model_t = require_pydantic_model(return_type)
        else:
            # 2) Best-effort inference from the prompt function's return
            #    annotation.
            #
            # This enables ergonomic stacking:
            #
            #   @materialized_property
            #   @llm
            #   def summary(self) -> Summary:
            #       return "..."  # template / prompt
            #
            # Note: static type checkers may complain inside the prompt builder
            # because it returns str, but runtime semantics are correct.
            try:
                hints = get_type_hints(
                    fn,
                    globalns=getattr(fn, "__globals__", None),
                    localns=None,
                )
                ann = hints.get("return")
            except Exception:
                ann = getattr(fn, "__annotations__", {}).get("return")

            if ann is not None:
                try:
                    model_t = require_pydantic_model(ann)
                except TypeError:
                    # Not a pydantic model => treat as text mode.
                    model_t = None

        class _LLMDecorator(DecoratorBase[P, Any, None]):
            def process_result(
                self,
                f: Callable[P, Any],
                result: Any,
                _args: tuple[Any, ...],
                _kwargs: dict[str, Any],
                _state: None,
            ) -> Any:
                prompt = require_str_prompt(
                    result, fn_name=getattr(f, "__name__", "<fn>")
                )
                return self_outer.request(prompt, return_type=model_t)

            async def process_result_async(
                self,
                f: Callable[P, Any],
                result: Any,
                _args: tuple[Any, ...],
                _kwargs: dict[str, Any],
                _state: None,
            ) -> Any:
                prompt = require_str_prompt(
                    result, fn_name=getattr(f, "__name__", "<fn>")
                )
                return await self_outer.request_async(prompt, return_type=model_t)

        self_outer = self
        decorated = _LLMDecorator().decorate(fn)

        # Ensure the decorated callable advertises the *actual* return type at
        # runtime. This helps other decorators which inspect annotations.
        if model_t is not None:
            ann = dict(getattr(decorated, "__annotations__", {}) or {})
            ann["return"] = model_t
            decorated.__annotations__ = ann

        return decorated

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
