"""Core :func:`etl_decorators.templating.template` decorator implementation."""

from __future__ import annotations

import inspect
import re
from typing import Any, Callable, ParamSpec, overload

from .deps import _require_jinja2
from .payload import _read_template_payload


P = ParamSpec("P")


@overload
def template(fn: Callable[P, Any]) -> Callable[P, str]: ...


@overload
def template(
    fn: None = None,
    *,
    jinja_env: Any | None = None,
) -> Callable[[Callable[P, Any]], Callable[P, str]]: ...


def template(
    fn: Callable[P, Any] | None = None,
    *,
    jinja_env: Any | None = None,
):
    """Render returned template content using Jinja2.

    The decorated function can return:
    - a file path (str/pathlib.Path)
    - a template string
    - a file-like resource (implements `.read()`)

    Template variables are the decorated callable's *bound parameters*.

    For methods, Jinja2 reserves `self` inside templates. This decorator:
    - exposes the Python instance as `self_` in the render context, and
    - rewrites `self.` -> `self_.` inside Jinja blocks (``{{ ... }}`` / ``{% ... %}``).

    Missing variables raise (StrictUndefined).

    Parameters:
        jinja_env: optional pre-configured Jinja2 Environment.
            When not provided, an Environment with StrictUndefined is created.
    """

    def _decorate(f: Callable[P, Any]):
        is_async = inspect.iscoroutinefunction(f)

        if is_async:

            async def wrapped(*args: P.args, **kwargs: P.kwargs):  # type: ignore[misc]
                payload = await f(*args, **kwargs)
                return _render_payload(
                    payload,
                    fn=f,
                    args=args,
                    kwargs=kwargs,
                    jinja_env=jinja_env,
                )

        else:

            def wrapped(*args: P.args, **kwargs: P.kwargs):  # type: ignore[misc]
                payload = f(*args, **kwargs)
                return _render_payload(
                    payload,
                    fn=f,
                    args=args,
                    kwargs=kwargs,
                    jinja_env=jinja_env,
                )

        # Preserve basic metadata.
        wrapped.__name__ = getattr(f, "__name__", wrapped.__name__)
        wrapped.__qualname__ = getattr(f, "__qualname__", wrapped.__qualname__)
        wrapped.__doc__ = getattr(f, "__doc__", None)
        wrapped.__module__ = getattr(f, "__module__", None)
        wrapped.__wrapped__ = f

        return wrapped

    if fn is None:
        return _decorate
    return _decorate(fn)


def _render_payload(
    payload: Any,
    *,
    fn: Callable[..., Any],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    jinja_env: Any | None,
) -> str:
    _require_jinja2()
    from jinja2 import Environment, StrictUndefined

    template_text = _read_template_payload(payload)

    sig = inspect.signature(fn)
    bound = sig.bind_partial(*args, **kwargs)
    bound.apply_defaults()
    context = dict(bound.arguments)
    # Jinja2 reserves the name `self` inside templates (TemplateReference).
    # For methods, expose the Python instance as `self_` and rewrite template
    # occurrences of `self.` to `self_.` inside Jinja blocks.
    if "self" in context:
        context.setdefault("self_", context.pop("self"))
        template_text = _rewrite_self_to_self_(template_text)

    env = jinja_env
    if env is None:
        env = Environment(undefined=StrictUndefined)

    tpl = env.from_string(template_text)
    # Pass the context as a single positional dict.
    return tpl.render(context)


_JINJA_BLOCK_RE = re.compile(r"(\{\{.*?\}\}|\{%.*?%\})", re.DOTALL)


def _rewrite_self_to_self_(template_text: str) -> str:
    """Rewrite `self.` into `self_.` inside Jinja blocks.

    This keeps template authors able to write method templates naturally
    (`{{ self.attr }}`), while avoiding Jinja2's internal `self` variable.
    """

    def _sub(match: re.Match[str]) -> str:
        block = match.group(0)
        return re.sub(r"\bself\.", "self_.", block)

    return _JINJA_BLOCK_RE.sub(_sub, template_text)
