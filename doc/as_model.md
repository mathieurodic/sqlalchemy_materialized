# `etl_decorators.sqlalchemy.as_model`

Turn simple, annotated Python classes into SQLAlchemy declarative models.

This is meant for ETL-style codebases where you want a **small amount of ORM
boilerplate** while still ending up with **real SQLAlchemy models**.

## Installation

```bash
pip install etl-decorators[sqlalchemy]
```

## Quick start

```python
from __future__ import annotations

from datetime import date
from sqlalchemy.orm import DeclarativeBase

from etl_decorators.sqlalchemy import JSON, as_model


class Base(DeclarativeBase):
    pass


@as_model(Base)
class User:
    fullname: str
    date_of_birth: date


@as_model(Base)
class Application:
    user: User
    url: str
    details: JSON
```

This has (approximately) the same effect as writing:

```python
from sqlalchemy import Column, Date, ForeignKey, Integer, String
from sqlalchemy.orm import relationship


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True)
    fullname = Column(String, nullable=False)
    date_of_birth = Column(Date, nullable=False)

    applications = relationship("Application", back_populates="user")


class Application(Base):
    __tablename__ = "applications"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    user = relationship("User", back_populates="applications")
    url = Column(String, nullable=False)
    details = Column(JSON, nullable=False)
```

## Table naming

`as_model` accepts an optional `tablename` parameter.

- If `tablename is None`, it defaults to `cls.__name__.lower() + "s"`.
- Example: `User` → `users`, `Application` → `applications`.

## Column inference

`as_model` inspects `__annotations__` and creates columns based on the
annotation type.

### Nullability rules

- `T` (non-Optional) → `nullable=False`
- `Optional[T]` / `T | None` → `nullable=True`

### Supported annotation forms

Supported out of the box:

- `int`, `float`, `str`, `bool`
- `datetime.datetime`, `datetime.time`, `datetime.date`
- `list[T]` → stored as JSON
- `pydantic.BaseModel` and `list[BaseModel]` → stored as JSON via
  `PydanticJSON` / `PydanticJSONList`
- SQLAlchemy types:
  - `JSON`, `sa.String`, ...
  - type instances like `PydanticJSON(Payload)`

If a type is not supported, `as_model` raises a `TypeError`.

## Automatic columns and behaviors

`as_model` can add common ETL-friendly columns automatically. All flags are
optional, and any column already declared on the class takes precedence.

```python
@as_model(
    Base,
    with_primary_key="id",
    with_creation_timestamp="created_at",
    with_modification_timestamp="updated_at",
    with_soft_deletion="deleted_at",
)
class Document:
    title: str
```

### Primary key

`with_primary_key: str | None = "id"`

- When set to a string (default: `"id"`), `as_model` injects a primary key
  column with that name unless already present on the class.
- When set to `None`, no automatic primary key column is added.

### Creation timestamp

`with_creation_timestamp: str | None = None`

If provided, `as_model` adds a `DateTime(timezone=True)` column with a
database default of `NOW()` (via `server_default=sa.func.now()`).

### Modification timestamp

`with_modification_timestamp: str | None = None`

If provided, `as_model` adds a nullable `DateTime(timezone=True)` column. The
initial value is `NULL` and it is set automatically on real UPDATEs (soft
delete does **not** set it).

### Soft deletion

`with_soft_deletion: str | None = None`

If provided, `as_model` adds a nullable `DateTime(timezone=True)` column used
as a soft-deletion marker and enables soft-delete behavior.

- `Session.delete(obj)` and `obj.delete()` update the column to `NOW()` instead
  of issuing a DELETE.
- Queries automatically filter out soft-deleted rows (where the column is not
  `NULL`).
- Call `session.with_deleted()` to disable this filter for a query.

The soft-delete logic is implemented in a dedicated helper module and must be
enabled for a session (see below).

### with_timestamps

`with_timestamps: bool = False`

Convenience flag. When `True`, it behaves as if you set:

- `with_creation_timestamp="created_at"`
- `with_modification_timestamp="updated_at"`
- `with_soft_deletion="deleted_at"`

### Enabling soft-delete on a Session

Soft deletion relies on Session hooks. Use the helper in
`etl_decorators.sqlalchemy.orm.soft_delete`:

```python
from sqlalchemy.orm import Session
from etl_decorators.sqlalchemy.orm.soft_delete import enable_soft_delete

session = Session(engine)
enable_soft_delete(session)

session.query(Document).all()      # excludes deleted
session.with_deleted().query(Document).all()  # includes deleted
```

## Relationships

If an annotation refers to another mapped model class, `as_model` generates:

- a `{name}_id` foreign key column
- a scalar relationship for `{name}`
- a reverse relationship on the target class

Example:

```python
@as_model(Base)
class Application:
    user: User
```

Generates:

- `Application.user_id` (FK to `users.id`, `nullable=False`)
- `Application.user` relationship
- `User.applications` relationship

### Optional relationships

For `user: User | None`, the FK column is nullable.

For **non-Optional** relationships, `as_model` also enforces at construction
time that the relationship cannot be omitted / set to `None`.

## Defaults and the `field(...)` helper

You can specify defaults by assigning a value on the class body:

```python
@as_model(Base)
class User:
    fullname: str = "anonymous"
```

For more control, use `etl_decorators.sqlalchemy.field`:

```python
from etl_decorators.sqlalchemy import field


@as_model(Base)
class User:
    # Column kwargs are forwarded to SQLAlchemy (mapped_column / Column)
    email: str = field(unique=True, index=True)

    # default value
    is_active: bool = field(default=True)

    # default factory: can accept 0 args or 1 arg (self)
    slug: str = field(default_factory=lambda self: self.email.lower())
```

Notes:

- `field(default=...)` and `field(default_factory=...)` are mutually exclusive.
- `default_factory` is evaluated in Python when the instance is constructed.

## Methods & custom attributes

`@as_model` preserves methods and custom class attributes defined on the
decorated class. The mapped class is built from the original class namespace,
so helper methods, properties, and descriptors remain available on the
generated SQLAlchemy model.

This also means you can define SQLAlchemy helpers like
`@materialized_property` on an `@as_model` class; they will be initialized on
the generated model during class creation.

## Pydantic JSON dirty tracking (important)

SQLAlchemy does not always detect in-place mutations inside JSON values.

This package provides `PydanticJSON` and `PydanticJSONList`, and ensures that
mutating a `pydantic.BaseModel` stored in such a column (including items inside
the list variant) marks the ORM attribute as modified so that `flush()` persists
the changes.

If you mutate deeply nested structures inside your Pydantic model, you may
still need to reassign the top-level value; this is a general constraint of
ORM mutation tracking.
