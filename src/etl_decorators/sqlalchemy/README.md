# `etl_decorators.sqlalchemy`

SQLAlchemy-related helpers for ETL pipelines.

This package is an **optional extra**:

```bash
pip install etl-decorators[sqlalchemy]
```

## User documentation

User-facing usage and examples live under `doc/`:

- [`doc/materialized_property.md`](../../../doc/materialized_property.md)
- [`doc/as_model.md`](../../../doc/as_model.md)

## Optional dependencies

This subpackage requires SQLAlchemy.

`pydantic` is only needed if you use:

- `pydantic.BaseModel` return types (or `list[BaseModel]`) with `materialized_property`, or
- `PydanticJSON` / `PydanticJSONList` types, or
- annotations that are themselves `BaseModel` / `list[BaseModel]` with `as_model`.

The `etl-decorators[sqlalchemy]` extra installs both.

## Code layout

The implementation is split into small modules:

- [`materialized/decorator.py`](materialized/decorator.py): `materialized_property` public decorator
- [`materialized/descriptor.py`](materialized/descriptor.py): `_MaterializedPropertyDescriptor` (injection + hybrid property)
- [`materialized/config.py`](materialized/config.py): `_MaterializedConfig`
- [`materialized/depends_on.py`](materialized/depends_on.py): dependency-based invalidation (SQLAlchemy attribute listeners)
- [`materialized/list_fk.py`](materialized/list_fk.py): association-table storage for `list[MappedClass]`
- [`materialized/helpers.py`](materialized/helpers.py): mapper/session helpers
- [`orm/columns.py`](orm/columns.py): `make_sa_column` (Python annotation → SQLAlchemy column)
- [`orm/as_model.py`](orm/as_model.py): `as_model` (plain annotated class → SQLAlchemy declarative model)
- [`orm/field.py`](orm/field.py): `field` (defaults + column kwargs helper for `as_model`)
- [`orm/soft_delete.py`](orm/soft_delete.py): `enable_soft_delete` (soft delete hooks + `with_deleted(...)`)
- [`type_decorators/pydantic_json.py`](type_decorators/pydantic_json.py): `PydanticJSON`, `PydanticJSONList`

## Tests

Tests live under [`tests/sqlalchemy/`](../../../tests/sqlalchemy/).

[`__init__.py`](__init__.py) re-exports `materialized_property` to preserve a stable import path:

```python
from etl_decorators.sqlalchemy import materialized_property
```

It also exports `as_model` and `field`:

```python
from etl_decorators.sqlalchemy import as_model, field
```
