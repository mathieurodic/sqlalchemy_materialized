"""Serialization helpers for Redis cache payloads.

Notes
-----

This module is intentionally lightweight and has no hard dependency on
`pydantic`.

When using ``serialization="json"`` and the cached value is a Pydantic v2
``BaseModel`` instance, we store a small envelope including the model's JSON
schema and the JSON-mode dump.

On load, if the model class cannot be resolved, we attempt a best-effort
dynamic model generation from the JSON schema.
"""

from __future__ import annotations

import importlib
import json
import pickle
from typing import Any, Literal


Serialization = Literal["pickle", "json"]


_PYDANTIC_ENVELOPE_MARKER = "pydantic_v2"


def _maybe_pydantic_base_model():
    try:
        from pydantic import BaseModel

        return BaseModel
    except Exception:  # pragma: no cover
        return None


def _is_pydantic_model(value: Any) -> bool:
    BaseModel = _maybe_pydantic_base_model()
    return BaseModel is not None and isinstance(value, BaseModel)


def _model_info(value: Any) -> dict[str, Any]:
    t = type(value)
    schema = value.model_json_schema()
    return {
        "module": getattr(t, "__module__", None),
        "qualname": getattr(t, "__qualname__", getattr(t, "__name__", None)),
        "name": getattr(t, "__name__", None),
        "schema": schema,
    }


def _resolve_model_class(info: dict[str, Any]):
    """Try to resolve a model class from recorded module/qualname."""

    BaseModel = _maybe_pydantic_base_model()
    if BaseModel is None:
        return None

    mod = info.get("module")
    qn = info.get("qualname")
    if not mod or not qn:
        return None

    try:
        m = importlib.import_module(mod)
        obj: Any = m
        for part in str(qn).split("."):
            obj = getattr(obj, part)
        if isinstance(obj, type) and issubclass(obj, BaseModel):
            return obj
    except Exception:
        return None
    return None


def _iter_pydantic_subclasses(cls: type) -> list[type]:
    """Return all subclasses (recursive)."""

    out: list[type] = []
    stack = list(getattr(cls, "__subclasses__", lambda: [])())
    while stack:
        sub = stack.pop()
        out.append(sub)
        try:
            stack.extend(sub.__subclasses__())
        except Exception:
            continue
    return out


def _find_model_by_name_and_schema(name: str, schema: Any):
    BaseModel = _maybe_pydantic_base_model()
    if BaseModel is None:
        return None

    for sub in _iter_pydantic_subclasses(BaseModel):
        try:
            if getattr(sub, "__name__", None) != name:
                continue
            if _schemas_compatible(sub.model_json_schema(), schema):
                return sub
        except Exception:
            continue
    return None


def _schemas_compatible(a: Any, b: Any) -> bool:
    # Be conservative: exact match.
    try:
        return a == b
    except Exception:
        return False


def _best_effort_model_from_schema(name: str, schema: dict[str, Any]):
    """Generate a minimal Pydantic model from a JSON schema.

    This is intentionally incomplete (JSON Schema is big). We support the
    common case of an "object" schema with "properties" and basic scalar types.
    Unknown/complex constructs are mapped to ``Any``.
    """

    BaseModel = _maybe_pydantic_base_model()
    if BaseModel is None:
        return None

    try:
        from pydantic import create_model
    except Exception:  # pragma: no cover
        return None

    required = set(schema.get("required") or [])
    props: dict[str, Any] = schema.get("properties") or {}

    def map_type(s: Any) -> Any:
        if not isinstance(s, dict):
            return Any

        t = s.get("type")
        # union types
        if isinstance(t, list):
            # ignore nullability; if includes null, we handle optional below.
            non_null = [x for x in t if x != "null"]
            if len(non_null) == 1:
                t = non_null[0]
            else:
                return Any

        if t == "string":
            return str
        if t == "integer":
            return int
        if t == "number":
            return float
        if t == "boolean":
            return bool
        if t == "object":
            # nested object: generate nested model
            nested_name = s.get("title") or "Nested"
            nested = _best_effort_model_from_schema(f"{name}_{nested_name}", s)
            return nested or dict[str, Any]
        if t == "array":
            items = s.get("items")
            item_t = map_type(items)
            return list[item_t]  # type: ignore[misc]
        return Any

    fields: dict[str, tuple[Any, Any]] = {}
    for prop_name, prop_schema in props.items():
        py_t = map_type(prop_schema)
        is_nullable = False
        if isinstance(prop_schema, dict) and isinstance(prop_schema.get("type"), list):
            is_nullable = "null" in prop_schema.get("type")

        # If the field is not required, or declared nullable, allow None.
        is_optional = prop_name not in required or is_nullable
        if is_optional:
            try:
                py_t = py_t | None  # type: ignore[operator]
            except Exception:
                py_t = Any

        default = ... if prop_name in required else None

        fields[prop_name] = (py_t, default)

    # Title helps debugging.
    title = schema.get("title") or name
    try:
        return create_model(str(title), __base__=BaseModel, **fields)
    except Exception:
        return None


def dumps(value: Any, *, serialization: Serialization) -> bytes:
    if serialization == "pickle":
        return pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)

    if serialization == "json":
        if _is_pydantic_model(value):
            envelope = {
                "__etl_decorators__": _PYDANTIC_ENVELOPE_MARKER,
                "model": _model_info(value),
                "data": value.model_dump(mode="json"),
            }
            value = envelope

        # A compact, deterministic representation helps debugging, and is stable.
        return json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode("utf-8")

    raise ValueError(f"Unsupported serialization: {serialization!r}")


def loads(data: bytes, *, serialization: Serialization) -> Any:
    if serialization == "pickle":
        return pickle.loads(data)

    if serialization == "json":
        obj = json.loads(data.decode("utf-8"))

        if (
            isinstance(obj, dict)
            and obj.get("__etl_decorators__") == _PYDANTIC_ENVELOPE_MARKER
            and isinstance(obj.get("model"), dict)
            and "data" in obj
        ):
            info = obj.get("model")
            payload = obj.get("data")

            BaseModel = _maybe_pydantic_base_model()
            if BaseModel is None:
                return obj

            model_cls = _resolve_model_class(info)
            schema = info.get("schema")

            # If import-based resolution failed, try best-effort lookup among
            # already-imported subclasses (by name + schema).
            if model_cls is None:
                model_name = info.get("name")
                if isinstance(model_name, str):
                    model_cls = _find_model_by_name_and_schema(model_name, schema)

            if model_cls is not None:
                try:
                    if _schemas_compatible(model_cls.model_json_schema(), schema):
                        return model_cls.model_validate(payload)
                except Exception:
                    pass

            # Attempt best-effort generation
            name = info.get("name") or "CachedModel"
            if isinstance(schema, dict):
                generated = _best_effort_model_from_schema(str(name), schema)
                if generated is not None:
                    try:
                        return generated.model_validate(payload)
                    except Exception:
                        return obj

            return obj

        return obj

    raise ValueError(f"Unsupported serialization: {serialization!r}")
