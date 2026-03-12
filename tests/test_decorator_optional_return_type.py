import sqlalchemy as sa
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column


def test_materialized_property_accepts_optional_mapped_return_type_and_resolves_instance():
    """Optional[MappedClass] should be supported for FK detection and resolution."""

    from sqlalchemy_materialized.decorator import materialized_property

    class Base(DeclarativeBase):
        pass

    class Author(Base):
        __tablename__ = "author_optional"

        id: Mapped[int] = mapped_column(primary_key=True)

    def compute_author(self) -> Author | None:
        return self.author_id  # type: ignore[return-value]

    class Post(Base):
        __tablename__ = "post_optional"
        __allow_unmapped__ = True

        id: Mapped[int] = mapped_column(primary_key=True)
        author_id: Mapped[int] = mapped_column(sa.Integer)
        author = materialized_property(compute_author)

    # Column should be a FK to author_optional.id
    fk_cols = list(Post.__table__.c.author.foreign_keys)
    assert len(fk_cols) == 1
    assert fk_cols[0].target_fullname == "author_optional.id"

    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        a = Author()
        session.add(a)
        session.flush()

        p = Post(author_id=a.id)
        session.add(p)
        session.flush()

        resolved = p.author
        assert isinstance(resolved, Author)
        assert resolved.id == a.id


def test_make_sa_column_raises_on_non_optional_union():
    from sqlalchemy_materialized.columns import make_sa_column

    try:
        make_sa_column("x", int | str)
        raise AssertionError("Expected TypeError")
    except TypeError as e:
        assert "non-Optional union" in str(e) or "Unsupported" in str(e)


def test_unwrap_optional_raises_on_optional_union_with_multiple_non_none():
    """Covers the strictness: Optional[Union[A, B]] is rejected."""
    from sqlalchemy_materialized.type_utils import unwrap_optional

    try:
        unwrap_optional(int | str | None)
        raise AssertionError("Expected TypeError")
    except TypeError as e:
        assert "Unsupported Optional/Union" in str(e) or "Unsupported" in str(e)
