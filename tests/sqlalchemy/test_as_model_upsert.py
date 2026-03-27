from __future__ import annotations


import sqlalchemy as sa
from sqlalchemy.orm import DeclarativeBase, Session

from etl_decorators.sqlalchemy import as_model


def test_as_model_upsert_inserts_and_updates_default_search_keys_all_kwargs():
    class Base(DeclarativeBase):
        pass

    @as_model(Base)
    class User:
        email: str
        name: str

    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        u1 = User.upsert(session, email="a@example.com", name="Ada")
        assert u1.id is not None

        # With default search keys, it uses *all* kwargs; so changing name
        # means it won't match and will insert a new row.
        u2 = User.upsert(session, email="a@example.com", name="Ada Lovelace")
        assert u2.id is not None
        assert u2.id != u1.id

        assert session.query(User).count() == 2


def test_as_model_upsert_with_searched_keys_updates_existing_row():
    class Base(DeclarativeBase):
        pass

    @as_model(Base)
    class User:
        email: str
        name: str

    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        u1 = User.upsert(session, email="a@example.com", name="Ada")
        u2 = User.upsert(
            session,
            email="a@example.com",
            name="Ada Lovelace",
            __searched_keys__=("email",),
        )

        assert u2.id == u1.id
        assert u2.name == "Ada Lovelace"
        assert session.query(User).count() == 1


def test_as_model_upsert_autoflush_flag():
    class Base(DeclarativeBase):
        pass

    @as_model(Base)
    class User:
        email: str

    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        u = User.upsert(session, email="a@example.com", __autoflush__=False)

        # Without flush, PK isn't populated yet in typical SQLAlchemy usage.
        assert u.id is None

        session.flush()
        assert u.id is not None
