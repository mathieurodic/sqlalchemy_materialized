import sqlalchemy as sa
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column


def test_make_sa_column_creates_foreign_key_for_mapped_return_type_and_materialized_property_resolves_instance():
    """Integration test: return type is a mapped model => backing column is FK, property returns instance in-session."""
    from sqlalchemy_materialized.decorator import materialized_property

    class Base(DeclarativeBase):
        pass

    class Author(Base):
        __tablename__ = "author"

        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str] = mapped_column(sa.String)

    def compute_author(self) -> Author:
        # Returning an id is also valid; materialized_property should resolve to an instance when in-session.
        return self.author_id  # type: ignore[return-value]

    class Post(Base):
        __tablename__ = "post"
        __allow_unmapped__ = True

        id: Mapped[int] = mapped_column(primary_key=True)
        author_id: Mapped[int] = mapped_column(sa.Integer)
        author = materialized_property(compute_author)

    # Ensure the injected backing column is a FK to author.id
    fk_cols = list(Post.__table__.c.author.foreign_keys)
    assert len(fk_cols) == 1
    assert fk_cols[0].target_fullname == "author.id"
    assert isinstance(Post.__table__.c.author.type, sa.Integer)

    # and that we also have the computed-at column
    assert "author__computed_at" in Post.__table__.c
    assert isinstance(Post.__table__.c.author__computed_at.type, sa.DateTime)
    assert Post.__table__.c.author__computed_at.type.timezone is True

    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        a = Author(name="alice")
        session.add(a)
        session.flush()  # assigns an id

        p = Post(author_id=a.id)
        session.add(p)
        session.flush()

        # In-session => we get the instance
        resolved = p.author
        assert isinstance(resolved, Author)
        assert resolved.id == a.id


def test_materialized_property_fk_returns_id_when_detached_from_session():
    from sqlalchemy_materialized.decorator import materialized_property

    class Base(DeclarativeBase):
        pass

    class Author(Base):
        __tablename__ = "author2"

        id: Mapped[int] = mapped_column(primary_key=True)

    def compute_author(self) -> Author:
        return self.author_id  # type: ignore[return-value]

    class Post(Base):
        __tablename__ = "post2"
        __allow_unmapped__ = True

        id: Mapped[int] = mapped_column(primary_key=True)
        author_id: Mapped[int] = mapped_column(sa.Integer)
        author = materialized_property(compute_author)

    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)

    import pytest

    with Session(engine) as session:
        a = Author()
        session.add(a)
        session.flush()

        p = Post(author_id=a.id)
        session.add(p)
        session.flush()

        # Compute while still in-session so the value gets materialized.
        resolved = p.author
        assert isinstance(resolved, Author)
        assert resolved.id == a.id

        # Detach object from the session
        session.expunge(p)

        # Out of session => detached instances are not supported
        with pytest.raises(RuntimeError, match="detached"):
            _ = p.author


def test_materialized_property_fk_setter_accepts_instance_and_stores_pk():
    """Covers normalize_to_id(value is instance) => _pk_id_from_instance."""
    from sqlalchemy_materialized.decorator import materialized_property

    class Base(DeclarativeBase):
        pass

    class Author(Base):
        __tablename__ = "author3"

        id: Mapped[int] = mapped_column(primary_key=True)

    def compute_author(self) -> Author:
        return self.author_id  # type: ignore[return-value]

    class Post(Base):
        __tablename__ = "post3"
        __allow_unmapped__ = True

        id: Mapped[int] = mapped_column(primary_key=True)
        author_id: Mapped[int] = mapped_column(sa.Integer)
        author = materialized_property(compute_author)

    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        a = Author()
        session.add(a)
        session.flush()

        p = Post(author_id=a.id)
        session.add(p)
        session.flush()

        # Setter with an instance => store the PK
        p.author = a
        assert getattr(p, "_compute_author") == a.id

        # And in-session, the property resolves to the instance
        resolved = p.author
        assert isinstance(resolved, Author)
        assert resolved.id == a.id


def test_materialized_property_fk_setter_accepts_none_but_next_get_recomputes():
    """Covers normalize_to_id(value is None) through the setter.

    With the new `__computed` flag, a backing value of None can still mean
    "computed". Setting the property to None marks it as computed and should
    *not* trigger a recompute on the next access.
    """
    from sqlalchemy_materialized.decorator import materialized_property

    class Base(DeclarativeBase):
        pass

    class Author(Base):
        __tablename__ = "author4"

        id: Mapped[int] = mapped_column(primary_key=True)

    def compute_author(self) -> Author:
        return self.author_id  # type: ignore[return-value]

    class Post(Base):
        __tablename__ = "post4"
        __allow_unmapped__ = True

        id: Mapped[int] = mapped_column(primary_key=True)
        author_id: Mapped[int] = mapped_column(sa.Integer)
        author = materialized_property(compute_author)

    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        a = Author()
        session.add(a)
        session.flush()

        p = Post(author_id=a.id)
        session.add(p)
        session.flush()

        p.author = None
        assert getattr(p, "_compute_author") is None

        # Next access should return the cached None and not recompute.
        assert p.author is None


def test_materialized_property_fk_when_compute_returns_none_property_returns_none():
    """Covers resolve_from_id(ident is None) in the getter."""
    from sqlalchemy_materialized.decorator import materialized_property

    class Base(DeclarativeBase):
        pass

    class Author(Base):
        __tablename__ = "author5"

        id: Mapped[int] = mapped_column(primary_key=True)

    def compute_author(self) -> Author:
        # Force a None (even if the annotation is Author) to cover the branch.
        return None  # type: ignore[return-value]

    class Post(Base):
        __tablename__ = "post5"
        __allow_unmapped__ = True

        id: Mapped[int] = mapped_column(primary_key=True)
        author_id: Mapped[int] = mapped_column(sa.Integer)
        author = materialized_property(compute_author)

    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        a = Author()
        session.add(a)
        session.flush()

        p = Post(author_id=a.id)
        session.add(p)
        session.flush()

        assert p.author is None
        assert getattr(p, "_compute_author") is None

