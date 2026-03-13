from __future__ import annotations

import json
from typing import Any, Type

import sqlalchemy as sa
from pydantic import BaseModel


class PydanticJSON(sa.TypeDecorator):
    """SQLAlchemy type that stores a `pydantic.BaseModel` as JSON.

    DB storage: SQLAlchemy JSON type (`sa.JSON`).
    Python value: an instance of `model_cls`.

    Parameters
    ----------
    model_cls:
        Subclass of `pydantic.BaseModel` to serialize/deserialize.
    """

    impl = sa.JSON
    cache_ok = True

    def __init__(self, model_cls: Type[BaseModel]):
        super().__init__()
        if not isinstance(model_cls, type) or not issubclass(model_cls, BaseModel):
            raise TypeError(
                "PydanticJSON expects a subclass of pydantic.BaseModel, "
                f"received: {model_cls!r}"
            )
        self.model_cls = model_cls

    def process_bind_param(self, value: Any, dialect) -> Any:
        if value is None:
            return None

        # Value already of the right type
        if isinstance(value, self.model_cls):
            return value.model_dump(mode="json")

        # Allow a dict (convenient for caller code)
        if isinstance(value, dict):
            # Validate to avoid pushing invalid JSON.
            model = self.model_cls.model_validate(value)
            return model.model_dump(mode="json")

        # Allow a JSON string
        if isinstance(value, str):
            try:
                parsed = json.loads(value)
            except Exception as e:  # pragma: no cover
                raise ValueError("Invalid JSON string") from e
            model = self.model_cls.model_validate(parsed)
            return model.model_dump(mode="json")

        raise TypeError(
            "PydanticJSON only accepts BaseModel | dict | str(json) | None, "
            f"received: {type(value)!r}"
        )

    def process_result_value(self, value: Any, dialect) -> Any:
        if value is None:
            return None

        # sa.JSON usually returns a dict/list on the Python side
        if isinstance(value, (dict, list)):
            return self.model_cls.model_validate(value)

        # Some configurations may return a JSON string
        if isinstance(value, str):
            return self.model_cls.model_validate_json(value)

        # Fallback: try model_validate (e.g. RowMapping)
        return self.model_cls.model_validate(value)


class PydanticJSONList(sa.TypeDecorator):
    """SQLAlchemy type that stores a list of `pydantic.BaseModel` as JSON.

    DB storage: SQLAlchemy JSON type (`sa.JSON`).
    Python value: `list[model_cls]`.

    Parameters
    ----------
    model_cls:
        Subclass of `pydantic.BaseModel` to serialize/deserialize.
    """

    impl = sa.JSON
    cache_ok = True

    def __init__(self, model_cls: Type[BaseModel]):
        super().__init__()
        if not isinstance(model_cls, type) or not issubclass(model_cls, BaseModel):
            raise TypeError(
                "PydanticJSONList expects a subclass of pydantic.BaseModel, "
                f"received: {model_cls!r}"
            )
        self.model_cls = model_cls

    def process_bind_param(self, value: Any, dialect) -> Any:
        if value is None:
            return None

        if not isinstance(value, list):
            raise TypeError(
                "PydanticJSONList only accepts list[BaseModel|dict|str(json)] | None, "
                f"received: {type(value)!r}"
            )

        out: list[Any] = []
        for item in value:
            if item is None:
                raise TypeError("PydanticJSONList does not accept None items")

            if isinstance(item, self.model_cls):
                out.append(item.model_dump(mode="json"))
                continue

            if isinstance(item, dict):
                model = self.model_cls.model_validate(item)
                out.append(model.model_dump(mode="json"))
                continue

            if isinstance(item, str):
                try:
                    parsed = json.loads(item)
                except Exception as e:  # pragma: no cover
                    raise ValueError("Invalid JSON string") from e
                model = self.model_cls.model_validate(parsed)
                out.append(model.model_dump(mode="json"))
                continue

            raise TypeError(
                "PydanticJSONList items must be BaseModel | dict | str(json), "
                f"received: {type(item)!r}"
            )

        return out

    def process_result_value(self, value: Any, dialect) -> Any:
        if value is None:
            return None

        # sa.JSON usually returns list/dict on the Python side
        if isinstance(value, list):
            return [self.model_cls.model_validate(v) for v in value]

        # Some configurations may return a JSON string
        if isinstance(value, str):
            parsed = json.loads(value)
            if not isinstance(parsed, list):
                raise ValueError("Expected JSON array for PydanticJSONList")
            return [self.model_cls.model_validate(v) for v in parsed]

        # Fallback
        return [self.model_cls.model_validate(value)]
