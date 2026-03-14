"""Hashing helpers for Redis caching."""

from __future__ import annotations

import hashlib
import inspect
import pickle
import textwrap
import tokenize
from io import BytesIO
from typing import Any, Callable


def _sha256_hex(data: bytes, *, size: int = 16) -> str:
    return hashlib.sha256(data).hexdigest()[:size]


def _strip_comments_and_trailing_ws(src: str) -> str:
    """Remove comments and trailing whitespace from Python source."""

    out_tokens: list[tokenize.TokenInfo] = []
    bs = src.encode("utf-8")
    for tok in tokenize.tokenize(BytesIO(bs).readline):
        if tok.type in (tokenize.ENCODING, tokenize.ENDMARKER):
            continue
        if tok.type == tokenize.COMMENT:
            continue
        out_tokens.append(tok)
    rebuilt = tokenize.untokenize(out_tokens)
    # normalize line endings + trim right
    return "\n".join(line.rstrip() for line in rebuilt.splitlines()).strip() + "\n"


def callsite_code_hash(*, filename: str, start_lineno: int) -> str:
    """Hash the source block starting at a given file/line.

    This is used to make cache invalidation depend on the exact code starting
    just below the `@cache(...)` line.

    Semantics:
    - includes any decorators *below* @cache
    - includes the `def` line + body
    - stops at the end of the function block (dedent)

    Raises RuntimeError if the file cannot be read or the block cannot be
    extracted.
    """

    try:
        raw = open(filename, "r", encoding="utf-8").read().splitlines(True)
    except Exception as e:
        raise RuntimeError(f"Unable to read source file for caching: {filename!r}: {e}") from e

    if start_lineno <= 0:
        raise RuntimeError(f"Invalid start line for caching: {start_lineno}")
    i0 = start_lineno - 1
    if i0 >= len(raw):
        raise RuntimeError(
            f"Unable to extract cached function block: {filename!r}:{start_lineno} out of range"
        )

    # Skip blank lines.
    i = i0
    while i < len(raw) and not raw[i].strip():
        i += 1
    if i >= len(raw):
        raise RuntimeError(
            f"Unable to extract cached function block: {filename!r}:{start_lineno} reached EOF"
        )

    base_indent = len(raw[i]) - len(raw[i].lstrip(" "))

    # Find the def line; allow intervening decorators.
    j = i
    while j < len(raw):
        line = raw[j]
        if not line.strip():
            j += 1
            continue
        indent = len(line) - len(line.lstrip(" "))
        if indent < base_indent:
            break
        if indent == base_indent and line.lstrip().startswith("def "):
            break
        if indent == base_indent and line.lstrip().startswith("async def "):
            break
        if indent == base_indent and line.lstrip().startswith("@"):
            j += 1
            continue
        # unexpected statement before def
        raise RuntimeError(
            "Unable to extract cached function block: expected decorators and then def; "
            f"found {line.strip()!r} at {filename!r}:{j+1}"
        )

    if j >= len(raw):
        raise RuntimeError(
            f"Unable to extract cached function block: missing def after {filename!r}:{start_lineno}"
        )

    def_line = raw[j]
    def_indent = len(def_line) - len(def_line.lstrip(" "))
    if def_indent != base_indent:
        raise RuntimeError(
            f"Unable to extract cached function block: def indentation mismatch at {filename!r}:{j+1}"
        )

    # Grab until dedent.
    k = j + 1
    while k < len(raw):
        line = raw[k]
        if not line.strip():
            k += 1
            continue
        indent = len(line) - len(line.lstrip(" "))
        if indent <= def_indent and not line.lstrip().startswith(("#", "@")):
            break
        k += 1

    block = "".join(raw[i:k])
    block = textwrap.dedent(block)
    normalized = _strip_comments_and_trailing_ws(block)
    return _sha256_hex(normalized.encode("utf-8"))


def function_code_hash(fn: Callable[..., Any]) -> str:
    """Best-effort hash of a callable's behavior for cache invalidation.

    This hash is used as part of Redis cache keys, so changing the callable's
    behavior should ideally change this hash.

    We combine two signals:

    1) Normalized source code (best-effort) via ``inspect.getsource`` + AST
       parsing/unparsing. This effectively ignores comments and formatting.
    2) A fingerprint of the wrapper chain via successive ``__wrapped__``
       traversal (when decorators use ``functools.wraps``). This ensures that
       adding/removing wrappers changes the hash.
    """

    # This function is still used as a fallback fingerprint for callables
    # (e.g. when callsite hashing isn't used). It is not used by RedisCache
    # anymore for key versioning.

    layers: list[tuple[Any, ...]] = []

    # Best-effort fingerprint of the wrapper chain.
    seen: set[int] = set()
    cur: Any = fn
    while cur is not None and id(cur) not in seen:
        seen.add(id(cur))
        mod = getattr(cur, "__module__", "")
        qn = getattr(cur, "__qualname__", getattr(cur, "__name__", ""))
        code = getattr(cur, "__code__", None)
        if code is not None:
            layers.append(("layer", mod, qn, code.co_code, code.co_consts))
        else:
            layers.append(("layer", mod, qn))
        cur = getattr(cur, "__wrapped__", None)

    # Include source when available (comments ignored) as an additional signal.
    try:
        src = inspect.getsource(fn)
        src = _strip_comments_and_trailing_ws(src)
        layers.append(("source", src))
    except Exception:
        pass

    return _sha256_hex(pickle.dumps(layers, protocol=pickle.HIGHEST_PROTOCOL))


def arguments_tuple_hash(fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]) -> str:
    """Hash bound arguments for stable cache keys.

    Uses ``inspect.signature`` to bind args/kwargs and applies defaults.
    The bound argument mapping is pickled for stability.
    """

    sig = inspect.signature(fn)
    bound = sig.bind_partial(*args, **kwargs)
    bound.apply_defaults()
    return _sha256_hex(pickle.dumps(bound.arguments, protocol=pickle.HIGHEST_PROTOCOL))
