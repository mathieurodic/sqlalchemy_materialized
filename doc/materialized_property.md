# `etl_decorators.sqlalchemy.materialized_property`

A SQLAlchemy helper that **materializes** computed properties:
compute on first access, persist to the database, then reuse the stored value.

## Installation

```bash
pip install etl-decorators[sqlalchemy]
```

## Usage

```python
import sqlalchemy as sa
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

from etl_decorators.sqlalchemy import materialized_property


class Base(DeclarativeBase):
    pass


class Model(Base):
    __tablename__ = "model"

    id: Mapped[int] = mapped_column(primary_key=True)
    base: Mapped[int] = mapped_column(sa.Integer)

    @materialized_property
    def value(self) -> int:
        return self.base * 2


engine = sa.create_engine("sqlite+pysqlite:///:memory:")
Base.metadata.create_all(engine)

with Session(engine) as session:
    m = Model(base=10)
    session.add(m)
    session.flush()

    # First access => compute + flush => value is persisted
    assert m.value == 20
```

## Notes

- A backing column and a `{name}__computed_at` column are injected automatically.
- If the return type is a mapped class (or `Optional[Model]`), the backing column is created as a FK to the PK.
- `list[T]` return types are supported:
  - `list[int]`, `list[str]`, ... => JSON column
  - `list[MappedClass]` => injected one-to-many relationship (nullable FK on the item class); no association table; order not preserved
- The computation requires the instance to be attached to a SQLAlchemy `Session`.

### Using with other decorators (stacking)

`materialized_property` can be composed with other decorators, but there are
two important rules:

1. The compute function's (decorated) runtime return annotation must reflect
   the **actual value** returned by the compute callable. This library inspects
   that return type to decide how to store the materialized value.
2. Decorator order matters: Python applies decorators **bottom-up**.

#### Example: `template` + `LLM` + `materialized_property`

```python
from pydantic import BaseModel
from etl_decorators.llms import LLM
from etl_decorators.templating import template
from etl_decorators.sqlalchemy import materialized_property


class Summary(BaseModel):
    summary: str


llm = LLM(model="gpt-4o-mini")


class Model(Base):
    name: Mapped[str]

    @materialized_property
    @llm(return_type=Summary)
    @template
    def summary(self) -> str:
        return "Return JSON with a summary for {{ self.name }}"
```

This yields the pipeline:

`template` (render prompt str) → `LLM` (call model, return Summary) →
`materialized_property` (persist Summary in DB as JSON)

## Cache semantics

`materialized_property` is a *cache stored in the database*.

- The value is considered **materialized** when the `{name}__computed_at` column is not `NULL`.
- First Python access (`obj.value`) when not materialized triggers:
  1. a computation (optionally wrapped in a SAVEPOINT)
  2. a `session.flush()` to persist both the value and the `{name}__computed_at` timestamp.

### Flush-on-first-access (important)

This library intentionally flushes inside the getter on first access.
Accessing the property may therefore trigger SQL statements / constraint checks.

## Invalidation & recompute

### Explicit invalidation

Invalidation is explicit: delete the property.

```python
del obj.value
session.flush()  # optional; deleter flushes when attached to a Session
```

After invalidation, the next access recomputes and re-materializes.

### Dependency-based invalidation (`depends_on`)

You can ask `materialized_property` to invalidate itself when specified mapped
attributes change:

```python
class Model(Base):
    __tablename__ = "model"

    id: Mapped[int] = mapped_column(primary_key=True)
    base: Mapped[int] = mapped_column(sa.Integer)

    @materialized_property(depends_on=("base",))
    def value(self) -> int:
        return self.base * 2
```

Semantics:

- Supported dependency types: mapped columns, scalar relationships, and
  collection relationships (append/remove/bulk replace).
- Invalidation is **in-memory only** (no implicit flush).
- Invalidation clears the cached storage and resets `{name}__computed_at` to `NULL`.
- For `list[MappedClass]`, invalidation clears the relationship collection (but does **not** delete target rows).

## Querying (filter / order_by)

`materialized_property` is implemented as a SQLAlchemy `hybrid_property`.

- On instances (`obj.value`), you get the computed (or resolved) Python value.
- On classes (`Model.value` in a SQL expression), it refers to the **stored/materialized value**.

This means you can filter/order using the property directly:

```python
stmt = sa.select(Model).where(Model.value == 20).order_by(Model.value.desc())
rows = session.execute(stmt).scalars().all()
```

The SQL expression does **not** trigger computation.
Rows with `NULL` in the backing column behave like any other `NULL`.

Note: SQL expressions are **not supported** for `list[...]` return types.

## Naming of injected attributes/columns

Two mapped attributes are injected on your model:

- Backing storage:
  - Python attribute name: `_{compute_fn_name}`
  - Database column name: `{property_name}`
- Computed-at timestamp:
  - Python attribute name: `_{compute_fn_name}__computed_at`
  - Database column name: `{property_name}__computed_at`

The computed-at column uses `DateTime(timezone=True)` and values are stored as UTC.

The leading underscore is only for the Python attribute name (to reduce collision risk).
The database column name remains the public property name.

## Return types

### Scalar values

- Standard Python types like `int`, `str`, `datetime`, ... are mapped to sensible SQLAlchemy column types.
- `pydantic.BaseModel` return types are stored as JSON via a `TypeDecorator`.

### Mapped classes (FK resolution)

If the return annotation is a mapped class (or `Optional[MappedClass]`), the
backing column is created as a **foreign key to the target primary key**.

On instances, access requires a Session:

- in-session: returns the resolved mapped instance
- detached: raises `RuntimeError` (detached instances are not supported)

### `list[...]`

- `list[int]`, `list[str]`, ...: stored in a JSON column.
- `list[pydantic.BaseModel]`: stored in a JSON column with validation/round-trip.
- `list[MappedClass]`: stored using an injected one-to-many relationship:
  - `Owner.<prop>` is a `relationship(Child, back_populates=<owner_name>)`
  - `Child.<owner_name>_id` is a **nullable** FK column to the owner's PK
  - `Child.<owner_name>` is a scalar relationship back to the owner
  - no association table is created
  - ordering is **not** preserved

For `list[MappedClass]`, access also requires a Session (detached instances are not supported).

## Transactions & side effects

By default, the compute function runs inside a SAVEPOINT (`session.begin_nested()`).

- `in_transaction=True` (default): if compute raises, DB side effects performed *inside compute* are rolled back.
- `in_transaction=False`: compute runs in the ambient transaction; if it raises, DB side effects are not automatically rolled back.

In both cases, in-memory cached attributes are restored on failure so the property remains “not computed”.

## Retry

This decorator no longer implements retries directly.
Compose retries at the call-site using :func:`etl_decorators.resilience.retry`.
