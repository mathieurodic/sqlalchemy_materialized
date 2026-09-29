from __future__ import annotations

import enum
from datetime import date

import sqlalchemy as sa
from sqlalchemy.orm import DeclarativeBase, Session

from etl_decorators.sqlalchemy import JSON, as_model, field, materialized_property


def test_as_model_creates_tablename_id_columns_and_relationships():
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

    assert User.__tablename__ == "users"
    assert Application.__tablename__ == "applications"

    # PK
    assert "id" in User.__table__.c
    assert "id" in Application.__table__.c

    # Scalar columns
    assert isinstance(User.__table__.c.fullname.type, sa.String)
    assert User.__table__.c.fullname.nullable is False
    assert isinstance(User.__table__.c.date_of_birth.type, sa.Date)
    assert User.__table__.c.date_of_birth.nullable is False
    assert isinstance(Application.__table__.c.details.type, sa.JSON)

    # FK + relationship
    assert "user_id" in Application.__table__.c
    fk = list(Application.__table__.c.user_id.foreign_keys)
    assert len(fk) == 1
    assert fk[0].target_fullname == "users.id"
    assert Application.__table__.c.user_id.nullable is False

    # Backref
    assert hasattr(User, "applications")


def test_as_model_optional_relationship_makes_fk_nullable_and_init_allows_none():
    class Base(DeclarativeBase):
        pass

    @as_model(Base)
    class User:
        fullname: str

    @as_model(Base)
    class Application:
        reviewer: User | None
        url: str

    assert Application.__table__.c.reviewer_id.nullable is True

    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        a = Application(url="x", reviewer=None)
        session.add(a)
        session.flush()


def test_as_model_required_relationship_is_enforced_by_init():
    class Base(DeclarativeBase):
        pass

    @as_model(Base)
    class User:
        fullname: str

    @as_model(Base)
    class Application:
        user: User
        url: str

    import pytest

    with pytest.raises(TypeError, match="user.*required"):
        Application(url="x")


def test_as_model_defaults_and_default_factory():
    class Base(DeclarativeBase):
        pass

    @as_model(Base)
    class User:
        # plain default
        fullname: str = "anonymous"

        # via field(default=...)
        is_active: bool = field(default=True)

        # default_factory accepts either 0 or 1 arg
        slug: str = field(default_factory=lambda self: self.fullname.lower())

        # Optional scalar default None
        nickname: str | None = None

    u = User()
    assert u.fullname == "anonymous"
    assert u.is_active is True
    assert u.slug == "anonymous"
    assert u.nickname is None


def test_as_model_preserves_methods():
    class Base(DeclarativeBase):
        pass

    @as_model(Base)
    class User:
        fullname: str

        def greeting(self) -> str:
            return f"Hello {self.fullname}"

    u = User(fullname="Ada")
    assert u.greeting() == "Hello Ada"


def test_as_model_supports_materialized_property():
    class Base(DeclarativeBase):
        pass

    counter = {"calls": 0}

    @as_model(Base)
    class Model:
        base: int

        @materialized_property
        def value(self) -> int:
            counter["calls"] += 1
            return self.base * 2

    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        m = Model(base=10)
        session.add(m)
        session.flush()

        assert counter["calls"] == 0
        assert m.value == 20
        assert counter["calls"] == 1
        assert m._value__computed_at is not None
        assert counter["calls"] == 1

        session.expire(m, ["_value"])
        assert m.value == 20

    assert "value" in Model.__table__.c
    assert "value__computed_at" in Model.__table__.c


def test_as_model_enum_field_roundtrip_sqlite():
    """Enum-annotated fields are stored as VARCHAR and round-trip correctly."""

    class Status(str, enum.Enum):
        active = "active"
        inactive = "inactive"

    class Base(DeclarativeBase):
        pass

    @as_model(Base)
    class Item:
        name: str
        status: Status
        optional_status: Status | None = None

    engine = sa.create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)

    # Verify column type
    assert isinstance(Item.__table__.c.status.type, sa.Enum)
    assert Item.__table__.c.status.type.native_enum is False
    assert Item.__table__.c.status.nullable is False
    assert Item.__table__.c.optional_status.nullable is True

    # Verify round-trip through the DB
    with Session(engine) as session:
        obj = Item(name="foo", status=Status.active)
        session.add(obj)
        session.commit()
        session.expire(obj)

        loaded = session.get(Item, obj.id)
        assert loaded.status is Status.active
        assert loaded.optional_status is None
