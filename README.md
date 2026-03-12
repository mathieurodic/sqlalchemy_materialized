# sqlalchemy-materialized

A SQLAlchemy extension to **materialize** computed properties: compute on first access, persist to the database, then reuse the stored value.

## Installation

```bash
pip install sqlalchemy-materialized
# or, from source:
# pip install .
```

## Usage

```python
import sqlalchemy as sa
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

from sqlalchemy_materialized import materialized_property


class Base(DeclarativeBase):
    pass

class Model(Base):
    __tablename__ = "model"
    __allow_unmapped__ = True

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

    # If the compute function raises, the computation is executed inside a SAVEPOINT
    # (via session.begin_nested()) so any DB side effects performed by the compute
    # function are rolled back.
```

### Notes

- A backing column and a `__computed_at` column are injected automatically.
- If the return type is a SQLAlchemy mapped class (or `Optional[Model]`), the backing column is created as a FK to the PK.
- `PydanticJSON` is also exported to store `pydantic.BaseModel` values in a JSON column.
- `PydanticJSONList` is also exported to store `list[pydantic.BaseModel]` values in a JSON column.
- The computation requires the instance to be attached to a SQLAlchemy `Session`.
  If the property is accessed for the first time on a detached instance, a `RuntimeError` is raised.

## Cache semantics, invalidation, recompute

`materialized_property` is a *cache stored in the database*.

- The value is considered **materialized** when the `...__computed_at` column is not `NULL`.
- The first Python access (`obj.value`) triggers:
  1. a computation inside `session.begin_nested()` (SAVEPOINT)
  2. a `session.flush()` to persist both the value and the timestamp.

### Important: invalidation is explicit

This library **does not automatically detect dependencies**.
If your computed value depends on other columns/relationships and those change, you must invalidate the cache yourself.

You can invalidate using the property deleter:

```python
del obj.value
session.flush()  # optional; the deleter flushes when attached to a session
```

After invalidation, the next access recomputes and re-materializes:

```python
del obj.value
assert obj.value == ...  # recompute + persist
```

You can also override the materialized value manually:

```python
obj.value = 123
```

## Transactions & side effects

The compute function runs inside a SAVEPOINT (`session.begin_nested()`). This provides two properties:

- if the compute function raises, DB-side effects performed *inside the compute* are rolled back;
- in-memory values are restored so the property remains "not computed".

Note that the first access performs a `flush()`; this is intentional so the value is immediately persisted.

## Querying (filter / order_by)

`materialized_property` uses a SQLAlchemy `hybrid_property`.

- On instances (`obj.value`), you get the computed (or resolved) Python value.
- On classes (`Model.value` in a SQL expression), it refers to the **stored/materialized value**.

This means you can filter/order using the property directly:

```python
stmt = sa.select(Model).where(Model.value == 20).order_by(Model.value.desc())
rows = session.execute(stmt).scalars().all()
```

The SQL expression does **not** trigger computation. Rows with `NULL` in the backing column behave like any other `NULL`.

For FK return types, the stored/materialized value is the **foreign key identifier**.

## Naming of injected attributes/columns

Two mapped attributes are injected on your model:

- a *backing* mapped column attribute (Python name: `_{property_name}`; DB column name: `{property_name}`)
- a computed-at mapped column attribute (Python name: `_{property_name}__computed_at`; DB column name: `{property_name}__computed_at`)

The leading underscore is only for the **Python attribute name** to reduce the risk of collisions with your public API.
The actual **database column name** remains the public property name.

### Return types: `list[...]`

`materialized_property` also supports return annotations of the form `list[T]`.

- `list[int]`, `list[str]`, ...: the backing column is a `JSON` column.
- `list[BaseModelSubclass]`: the backing column uses `PydanticJSONList(BaseModelSubclass)` and values are validated
  and round-tripped as `list[BaseModelSubclass]`.
- `list[MappedClass]`: the backing column is a JSON array of primary keys. When accessed on an instance that is
  **not attached to a Session**, the property raises (since it cannot compute or resolve instances).

## License

MIT
