from __future__ import annotations

from datetime import datetime
from datetime import timezone

import pytest
import sqlalchemy as sa
from sqlalchemy.orm import DeclarativeBase, Session

from etl_decorators.sqlalchemy import as_model, field
from etl_decorators.sqlalchemy.orm import soft_delete


def test_as_model_skips_private_annotations():
    class Base(DeclarativeBase):
        pass

    @as_model(Base, with_primary_key=None)
    class Doc:
        pk: int = field(primary_key=True)
        _internal: int
        title: str

    assert "_internal" not in Doc.__table__.c
    assert "title" in Doc.__table__.c


def test_soft_delete_instance_requires_session():
    class Base(DeclarativeBase):
        pass

    @as_model(Base, with_soft_deletion="deleted_at")
    class Doc:
        title: str

    doc = Doc(title="orphan")

    with pytest.raises(RuntimeError, match="attached to a Session"):
        soft_delete.soft_delete_instance(doc)


def test_soft_delete_instance_hard_deletes():
    class Base(DeclarativeBase):
        pass

    @as_model(Base, with_soft_deletion="deleted_at")
    class Doc:
        title: str

    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        soft_delete.enable_soft_delete(session)
        doc = Doc(title="x")
        session.add(doc)
        session.flush()

        soft_delete.soft_delete_instance(doc, hard=True)
        session.flush()

        # Instance is actually deleted.
        assert session.with_deleted().query(Doc).count() == 0


def test_with_deleted_session_execute_and_getattr():
    class Base(DeclarativeBase):
        pass

    @as_model(Base, with_soft_deletion="deleted_at")
    class Doc:
        title: str

    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        soft_delete.enable_soft_delete(session)
        doc1 = Doc(title="a")
        doc2 = Doc(title="b")
        session.add_all([doc1, doc2])
        session.flush()

        session.delete(doc1)
        session.flush()

        stmt = sa.select(Doc).order_by(Doc.id)
        rows = session.with_deleted().execute(stmt).scalars().all()
        assert [row.id for row in rows] == [doc1.id, doc2.id]
        assert session.with_deleted().bind is session.bind

        session.execute(sa.update(Doc).values(title="c"))


def test_with_deleted_only_and_since_until():
    class Base(DeclarativeBase):
        pass

    @as_model(Base, with_soft_deletion="deleted_at")
    class Doc:
        title: str

    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)

    t0 = datetime(2020, 1, 1, tzinfo=timezone.utc)
    t1 = datetime(2020, 1, 2, tzinfo=timezone.utc)
    t2 = datetime(2020, 1, 3, tzinfo=timezone.utc)

    with Session(engine) as session:
        soft_delete.enable_soft_delete(session)
        a = Doc(title="a")
        b = Doc(title="b")
        c = Doc(title="c")
        session.add_all([a, b, c])
        session.flush()

        # Soft delete a + b
        session.delete(a)
        session.delete(b)
        session.flush()

        # Control timestamps to test since/until filtering.
        a.deleted_at = t0
        b.deleted_at = t2
        session.flush()

        # Default: includes non-deleted too
        assert session.with_deleted().query(Doc).count() == 3

        # Only deleted
        only_deleted_ids = [d.id for d in session.with_deleted(only=True).query(Doc).order_by(Doc.id)]
        assert only_deleted_ids == [a.id, b.id]

        # since implies only deleted (per requested semantics)
        q_since = session.with_deleted(since=t1).query(Doc).order_by(Doc.id)
        since_ids = [d.id for d in q_since]
        assert since_ids == [b.id]

        until_ids = [d.id for d in session.with_deleted(until=t1).query(Doc).order_by(Doc.id)]
        assert until_ids == [a.id]

        window_ids = [
            d.id
            for d in session.with_deleted(since=t1, until=datetime(2020, 1, 4, tzinfo=timezone.utc))
            .query(Doc)
            .order_by(Doc.id)
        ]
        assert window_ids == [b.id]


def test_soft_delete_before_flush_skip_unknown_models_and_keep_updated_at():
    class Base(DeclarativeBase):
        pass

    @as_model(Base, with_modification_timestamp="updated_at")
    class Plain:
        title: str

    @as_model(Base, with_soft_deletion="deleted_at", with_modification_timestamp="updated_at")
    class Doc:
        title: str

    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        soft_delete.enable_soft_delete(session)
        plain = Plain(title="keep")
        doc = Doc(title="soft")
        session.add_all([plain, doc])
        session.flush()

        sentinel = datetime(2001, 1, 1)
        doc.updated_at = sentinel
        session.delete(plain)
        session.delete(doc)
        session.flush()

        assert doc.deleted_at is not None
        assert doc.updated_at == sentinel
        assert getattr(doc, "_etl_skip_updated_at", False) is False
        assert session.query(Plain).count() == 0


def test_session_delete_hard_bypasses_soft_delete():
    class Base(DeclarativeBase):
        pass

    @as_model(Base, with_soft_deletion="deleted_at")
    class Doc:
        title: str

    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        soft_delete.enable_soft_delete(session)
        doc1 = Doc(title="a")
        doc2 = Doc(title="b")
        session.add_all([doc1, doc2])
        session.flush()

        # Hard delete: row disappears.
        session.delete(doc1, hard=True)
        # Soft delete: row remains but is filtered.
        session.delete(doc2)
        session.flush()

        assert session.with_deleted().query(Doc).count() == 1
        assert session.query(Doc).count() == 0

        remaining = session.with_deleted().query(Doc).one()
        assert remaining.id == doc2.id
        assert remaining.deleted_at is not None


def test_apply_soft_delete_filter_skips_missing_column():
    original = dict(soft_delete._SOFT_DELETE_COLUMNS)
    soft_delete._SOFT_DELETE_COLUMNS.clear()

    class Ghost:
        pass

    try:
        soft_delete.register_soft_delete_model(Ghost, "deleted_at")
        stmt = soft_delete._apply_soft_delete_filter(sa.select(sa.literal(1)))
        assert len(stmt._with_options) == 0
    finally:
        soft_delete._SOFT_DELETE_COLUMNS.clear()
        soft_delete._SOFT_DELETE_COLUMNS.update(original)