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
from typing import Any, Awaitable, Callable, ParamSpec, get_origin, get_type_hints, overload

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


def _is_pydantic_model_subclass(t: Any) -> bool:
    # `BaseModel` is `object` when pydantic isn't installed.
    return (
        BaseModel is not object
        and isinstance(t, type)
        and issubclass(t, BaseModel)
    )


def _make_answer_wrapper(expected_t: Any) -> type[BaseModel]:
    """Create a Pydantic wrapper model: Answer(result: expected_t)."""

    try:
        from pydantic import create_model
    except Exception as e:  # pragma: no cover
        raise RuntimeError(
            "pydantic is required for etl_decorators.llms structured output. "
            "Install with: pip install etl-decorators[llms]"
        ) from e

    return create_model("Answer", result=(expected_t, ...))


def _require_type_annotation(t: Any) -> Any:
    """Validate that `t` looks like a type annotation.

    We accept both real classes (e.g. `int`) and typing constructs
    (e.g. `list[str]`, `dict[str, int]`, `Optional[int]`, ...).
    """

    if t is None:
        raise TypeError("return_type cannot be None")

    # Disallow passing forward-ref strings explicitly; inference handles those.
    if isinstance(t, str):
        raise TypeError(
            "return_type must be a type annotation; received a string forward reference"
        )

    if isinstance(t, type):
        return t

    # typing constructs (Union, list[str], Annotated, etc.)
    if t is Any or get_origin(t) is not None:
        return t

    raise TypeError(f"return_type must be a type annotation; received: {t!r}")


@dataclass(frozen=True)
class LLM:
    """Callable decorator that turns prompt builders into LLM calls."""

    model: str
    api_key: str | None = field(default=None, repr=False)
    temperature: float = 0.1
    completion_kwargs: dict[str, Any] = field(default_factory=dict)

    def _completion_kwargs(self) -> dict[str, Any]:
        kwargs = dict(self.completion_kwargs or {})
        kwargs.setdefault("temperature", self.temperature)
        return kwargs

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
        # `expected_t` is what the decorated callable should return / advertise
        # at runtime.
        expected_t: Any = str

        # `model_t` is the Pydantic model used for structured extraction.
        model_t: type[BaseModel] | None = None

        # Optional unwrapping step when we use an Answer(result=...) wrapper.
        unwrap_result: bool = False

        # 1) Explicit return_type=... always wins.
        if return_type is not _MISSING:
            # If user explicitly passes return_type=None, we want a clear error.
            if return_type is None:
                require_pydantic_model(return_type)
            elif return_type is str:
                # Explicit text mode.
                expected_t = str
                model_t = None
            else:
                return_type = _require_type_annotation(return_type)
                # Structured output:
                # - BaseModel subclass => return it directly
                # - any other expected type => wrap it into Answer(result=...)
                if _is_pydantic_model_subclass(return_type):
                    expected_t = return_type
                    model_t = require_pydantic_model(return_type)
                else:
                    expected_t = return_type
                    model_t = _make_answer_wrapper(return_type)
                    unwrap_result = True
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
                # When __future__.annotations is enabled and get_type_hints fails,
                # we may see a string forward ref here; preserve historical
                # behavior and fall back to text mode.
                if ann is str or isinstance(ann, str):
                    expected_t = str
                    model_t = None
                elif _is_pydantic_model_subclass(ann):
                    expected_t = ann
                    try:
                        model_t = require_pydantic_model(ann)
                    except TypeError:
                        # Defensive fallback (should not happen if _is_pydantic_model_subclass is correct)
                        model_t = None
                        expected_t = str
                else:
                    # Any other annotation => wrapper mode.
                    expected_t = ann
                    model_t = _make_answer_wrapper(ann)
                    unwrap_result = True

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
                out = self_outer.request(prompt, return_type=model_t)
                if unwrap_result and model_t is not None:
                    # mypy: model_t is a BaseModel subclass with a `result` field.
                    return getattr(out, "result")
                return out

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
                out = await self_outer.request_async(prompt, return_type=model_t)
                if unwrap_result and model_t is not None:
                    return getattr(out, "result")
                return out

        self_outer = self
        decorated = _LLMDecorator().decorate(fn)

        # Ensure the decorated callable advertises the *actual* return type at
        # runtime. This helps other decorators which inspect annotations.
        ann = dict(getattr(decorated, "__annotations__", {}) or {})
        ann["return"] = expected_t
        decorated.__annotations__ = ann

        return decorated

    def _ensure_model_registration(self):
        cls = self.__class__
        if not hasattr(cls, "_registered_models"):
            cls._registered_models = set()
        if self.model not in cls._registered_models:
            import litellm
            litellm.register_model({
                self.model: {
                    "supports_response_schema": True,
                    "supports_function_calling": True,
                }
            })
            cls._registered_models.add(self.model)

    def request(self, prompt: str, *, return_type: type[BaseModel] | None):
        self._ensure_model_registration()
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
            kwargs["allowed_openai_params"] = ["response_format"]

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
