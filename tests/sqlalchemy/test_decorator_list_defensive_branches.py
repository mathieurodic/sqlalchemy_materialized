import sqlalchemy as sa
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


def test_materialized_property_raises_on_non_optional_union_return_annotation():
    from etl_decorators.sqlalchemy import materialized_property

    def compute(self) -> int | str:  # type: ignore[return-value]
        return 1

    try:
        materialized_property(compute)
        raise AssertionError("Expected TypeError")
    except TypeError as e:
        assert "Unsupported return annotation" in str(e)


def test_materialized_property_list_fk_raises_when_compute_returns_non_list(monkeypatch):
    import etl_decorators.sqlalchemy.materialized.descriptor as descriptor
    import etl_decorators.sqlalchemy.materialized.o2m_collection as o2m_collection
    import etl_decorators.sqlalchemy.materialized.decorator as dec

    class FakeSession:
        def begin_nested(self):
            class _BeginNested:
                def __enter__(self):
                    return self

                def __exit__(self, exc_type, exc, tb):
                    return False

            return _BeginNested()

        def flush(self):
            pass

        def get(self, cls, ident):  # pragma: no cover (not reached)
            raise AssertionError("should not be called")

    # Patch the helper used by _require_session (relationship collection path)
    monkeypatch.setattr(o2m_collection, "_require_session", lambda obj: FakeSession())

    class Base(DeclarativeBase):
        pass

    class Author(Base):
        __tablename__ = "author_defensive"

        id: Mapped[int] = mapped_column(primary_key=True)

    def compute(self) -> list[Author]:
        return 123  # type: ignore[return-value]

    class Model(Base):
        __tablename__ = "model_list_fk_bad_compute"
        __allow_unmapped__ = True

        id: Mapped[int] = mapped_column(primary_key=True)
        authors = dec.materialized_property(compute)

    # list[MappedClass] is stored in a relationship collection; invalid compute
    # output should raise on first meaningful access.
    m = Model()
    try:
        _ = list(m.authors)
        raise AssertionError("Expected TypeError")
    except TypeError as e:
        assert "expected a list" in str(e)


def test_materialized_property_list_fk_rejects_none_items(monkeypatch):
    import etl_decorators.sqlalchemy.materialized.descriptor as descriptor
    import etl_decorators.sqlalchemy.materialized.o2m_collection as o2m_collection
    import etl_decorators.sqlalchemy.materialized.decorator as dec

    class _BeginNested:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class FakeSession:
        def begin_nested(self):
            return _BeginNested()

        def flush(self):
            pass

    monkeypatch.setattr(o2m_collection, "_require_session", lambda obj: FakeSession())

    class Base(DeclarativeBase):
        pass

    class Author(Base):
        __tablename__ = "author_defensive2"

        id: Mapped[int] = mapped_column(primary_key=True)

    def compute(self) -> list[Author]:
        return [None]  # type: ignore[list-item]

    class Model(Base):
        __tablename__ = "model_list_fk_none_item"
        __allow_unmapped__ = True

        id: Mapped[int] = mapped_column(primary_key=True)
        authors = dec.materialized_property(compute)

    m = Model()

    # Not in a session, but we should fail earlier due to None item
    # while normalizing the compute result.
    try:
        _ = list(m.authors)
        raise AssertionError("Expected TypeError")
    except TypeError as e:
        assert "does not accept None items" in str(e)
