from __future__ import annotations

from datetime import datetime

import sqlalchemy as sa
from sqlalchemy.orm import DeclarativeBase, Session

from etl_decorators.sqlalchemy import as_model, field
from etl_decorators.sqlalchemy.orm.soft_delete import enable_soft_delete


def test_as_model_with_primary_key_flag_none_disables_auto_pk():
    class Base(DeclarativeBase):
        pass

    @as_model(Base, with_primary_key=None)
    class Doc:
        pk: int = field(primary_key=True)
        title: str

    assert "id" not in Doc.__table__.c
    assert "title" in Doc.__table__.c


def test_as_model_with_primary_key_custom_name():
    class Base(DeclarativeBase):
        pass

    @as_model(Base, with_primary_key="pk")
    class Doc:
        title: str

    assert "pk" in Doc.__table__.c
    assert Doc.__table__.c.pk.primary_key is True


def test_as_model_creation_and_modification_timestamps():
    class Base(DeclarativeBase):
        pass

    @as_model(
        Base,
        with_creation_timestamp="created_at",
        with_modification_timestamp="updated_at",
        with_primary_key=None,
    )
    class Doc:
        pk: int = field(primary_key=True)
        title: str

    created_col = Doc.__table__.c.created_at
    updated_col = Doc.__table__.c.updated_at

    assert isinstance(created_col.type, sa.DateTime)
    assert created_col.type.timezone is True
    assert created_col.server_default is not None

    assert isinstance(updated_col.type, sa.DateTime)
    assert updated_col.type.timezone is True
    assert updated_col.server_default is None
    assert updated_col.onupdate is None

    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        doc = Doc(title="x")
        session.add(doc)
        session.flush()
        assert doc.created_at is not None
        assert doc.updated_at is None

        doc.title = "y"
        session.flush()
        assert doc.updated_at is not None


def test_as_model_soft_delete_flags_and_query_filtering():
    class Base(DeclarativeBase):
        pass

    @as_model(Base, with_soft_deletion="deleted_at", with_primary_key="id")
    class Doc:
        title: str

    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        enable_soft_delete(session)
        doc1 = Doc(title="a")
        doc2 = Doc(title="b")
        session.add_all([doc1, doc2])
        session.flush()

        assert session.query(Doc).count() == 2

        session.delete(doc1)
        session.flush()
        assert doc1.deleted_at is not None
        assert session.query(Doc).count() == 1

        all_docs = session.with_deleted().query(Doc).order_by(Doc.id).all()
        assert [d.id for d in all_docs] == [doc1.id, doc2.id]

        doc2.delete()
        session.flush()
        assert doc2.deleted_at is not None


def test_as_model_instance_delete_hard():
    class Base(DeclarativeBase):
        pass

    @as_model(Base, with_soft_deletion="deleted_at", with_primary_key="id")
    class Doc:
        title: str

    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        enable_soft_delete(session)
        doc = Doc(title="a")
        session.add(doc)
        session.flush()

        doc.delete(hard=True)
        session.flush()

        assert session.with_deleted().query(Doc).count() == 0


def test_as_model_with_timestamps_shortcut():
    class Base(DeclarativeBase):
        pass

    @as_model(Base, with_timestamps=True, with_primary_key=None)
    class Doc:
        pk: int = field(primary_key=True)
        title: str

    assert "created_at" in Doc.__table__.c
    assert "updated_at" in Doc.__table__.c
    assert "deleted_at" in Doc.__table__.c

    deleted_col = Doc.__table__.c.deleted_at
    assert isinstance(deleted_col.type, sa.DateTime)
    assert deleted_col.type.timezone is True