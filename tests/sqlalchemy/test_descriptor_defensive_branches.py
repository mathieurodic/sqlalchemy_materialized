import sqlalchemy as sa
import pytest
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column


def test_descriptor_validate_false_hits_normalize_to_id_list_error_paths(monkeypatch):
    """Covers descriptor.normalize_to_id() list branches:

    - value is None (returns None)
    - value is not a list (raises TypeError)
    - list contains None item (raises TypeError)

    NOTE: list[MappedClass] is now relationship-backed, so these old
    normalize_to_id list+fk paths are no longer applicable.
    """

    import pytest

    pytest.skip("list[MappedClass] is relationship-backed; normalize_to_id list+fk paths no longer apply")

    class Base(DeclarativeBase):
        pass

    class Author(Base):
        __tablename__ = "descriptor_norm_to_id_author"

        id: Mapped[int] = mapped_column(primary_key=True)

    class Post(Base):
        __tablename__ = "descriptor_norm_to_id_post"
        __allow_unmapped__ = True

        id: Mapped[int] = mapped_column(primary_key=True)

        def compute_authors(self) -> list[Author]:
            return []

        # validate=False is required to reach normalize_to_id() list errors.
        authors = dec.materialized_property(compute_authors, validate=False)

    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        a1 = Author()
        session.add(a1)
        session.flush()

        p = Post()
        session.add(p)
        session.flush()

        # None => normalize_to_id list branch returns None
        p.authors = None
        assert getattr(p, "_compute_authors") is None

        with pytest.raises(TypeError, match="expected a list"):
            p.authors = 123  # type: ignore[assignment]

        with pytest.raises(TypeError, match="does not accept None items"):
            p.authors = [None]  # type: ignore[list-item]


def test_descriptor_validate_false_hits_normalize_list_fk_to_instances_error_paths():
    """Covers list[MappedClass] normalization defensive branches.

    This now exercises `_MaterializedPropertyDescriptor._normalize_list_fk_to_instances`.
    """

    from etl_decorators.sqlalchemy import materialized_property

    class Base(DeclarativeBase):
        pass

    class Author(Base):
        __tablename__ = "descriptor_norm_list_fk_author"

        id: Mapped[int] = mapped_column(primary_key=True)

    class Post(Base):
        __tablename__ = "descriptor_norm_list_fk_post"
        __allow_unmapped__ = True

        id: Mapped[int] = mapped_column(primary_key=True)

        def compute_authors(self) -> list[Author]:
            return []

        # validate=False so the setter calls normalize_list_fk_to_instances()
        # without being pre-blocked by validate_value().
        authors = materialized_property(compute_authors, validate=False)

    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        p = Post()
        session.add(p)
        session.flush()

        # Normalizer is installed on the relationship collection class.
        normalize = Post.authors.property.collection_class._normalize_list_to_instances

        # value is None => returns []
        assert normalize(p, None, Author) == []

        with pytest.raises(TypeError, match="expected a list"):
            normalize(p, 123, Author)  # type: ignore[arg-type]

        with pytest.raises(TypeError, match="does not accept None items"):
            normalize(p, [None], Author)  # type: ignore[list-item]


def test_descriptor_is_valid_identifier_bool_is_rejected_for_fk_scalar():
    """Covers `_is_valid_identifier` bool rejection + scalar FK error path."""

    from etl_decorators.sqlalchemy import materialized_property

    class Base(DeclarativeBase):
        pass

    class Author(Base):
        __tablename__ = "descriptor_bool_pk_author"

        id: Mapped[int] = mapped_column(primary_key=True)

    class Post(Base):
        __tablename__ = "descriptor_bool_pk_post"
        __allow_unmapped__ = True

        id: Mapped[int] = mapped_column(primary_key=True)

        def compute_author(self) -> Author:
            # never reached
            raise AssertionError("compute should not be called")

        author = materialized_property(compute_author)

    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        p = Post()
        session.add(p)
        session.flush()

        # bool is subclass of int, but must be rejected.
        with pytest.raises(TypeError, match="expected a mapped instance or identifier"):
            p.author = True  # type: ignore[assignment]


def test_descriptor_is_valid_identifier_accepts_string_pk_identifier():
    """Covers `_is_valid_identifier` branch where pk python type is not int."""

    from etl_decorators.sqlalchemy import materialized_property

    class Base(DeclarativeBase):
        pass

    class Token(Base):
        __tablename__ = "descriptor_string_pk_token"

        id: Mapped[str] = mapped_column(sa.String, primary_key=True)

    class Holder(Base):
        __tablename__ = "descriptor_string_pk_holder"
        __allow_unmapped__ = True

        id: Mapped[int] = mapped_column(primary_key=True)

        def compute_token(self) -> Token:
            # never reached
            raise AssertionError("compute should not be called")

        token = materialized_property(compute_token)

    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        t = Token(id="abc")
        h = Holder()
        session.add_all([t, h])
        session.flush()

        # Set via identifier; getter should resolve session.get(Token, "abc")
        h.token = "abc"  # type: ignore[assignment]
        assert h.token.id == "abc"


def test_descriptor_validate_value_list_fk_invalid_item_type_raises():
    """Covers validate_value() list-FK invalid item branch (TypeError)."""

    from etl_decorators.sqlalchemy import materialized_property

    class Base(DeclarativeBase):
        pass

    class Author(Base):
        __tablename__ = "descriptor_list_fk_invalid_item_author"

        id: Mapped[int] = mapped_column(primary_key=True)

    class Post(Base):
        __tablename__ = "descriptor_list_fk_invalid_item_post"
        __allow_unmapped__ = True

        id: Mapped[int] = mapped_column(primary_key=True)

        def compute_authors(self) -> list[Author]:
            return []

        authors = materialized_property(compute_authors)

    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)

    # For relationship-backed list[MappedClass], there is no setter-based
    # validation. Instead we validate compute outputs.
    #
    # Use a separate DeclarativeBase so the injected FK column name doesn't
    # mutate the same Author table across multiple owners in a single metadata.

    class BaseBad(DeclarativeBase):
        pass

    class AuthorBad(BaseBad):
        __tablename__ = "descriptor_list_fk_invalid_item_author_bad"

        id: Mapped[int] = mapped_column(primary_key=True)

    class PostBad(BaseBad):
        __tablename__ = "descriptor_list_fk_invalid_item_post_bad"
        __allow_unmapped__ = True

        id: Mapped[int] = mapped_column(primary_key=True)

        def compute_authors(self) -> list[AuthorBad]:
            return ["bad"]  # type: ignore[list-item]

        authors = materialized_property(compute_authors)

    engine2 = sa.create_engine("sqlite+pysqlite:///:memory:")
    BaseBad.metadata.create_all(engine2)

    with Session(engine2) as session:
        p2 = PostBad()
        session.add(p2)
        session.flush()

        with pytest.raises(RuntimeError, match="not found"):
            _ = list(p2.authors)


def test_descriptor_validate_value_non_fk_list_skips_validation_for_any():
    """Covers validate_value() non-FK list[Any] early-return branch."""

    from etl_decorators.sqlalchemy import materialized_property

    class Base(DeclarativeBase):
        pass

    class Model(Base):
        __tablename__ = "descriptor_list_any_model"
        __allow_unmapped__ = True

        id: Mapped[int] = mapped_column(primary_key=True)

        def compute_payload(self) -> list[object]:
            return []

        payload = materialized_property(compute_payload)

    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        m = Model()
        session.add(m)
        session.flush()

        # list[Any]/list[object] should accept None and mixed values.
        m.payload = [None, 1, "x"]
        assert m.payload == [None, 1, "x"]


def test_descriptor_validate_value_non_fk_list_rejects_none_and_wrong_item_type():
    """Covers validate_value() non-FK list[int] item validation branches."""

    from etl_decorators.sqlalchemy import materialized_property

    class Base(DeclarativeBase):
        pass

    class Model(Base):
        __tablename__ = "descriptor_list_int_model"
        __allow_unmapped__ = True

        id: Mapped[int] = mapped_column(primary_key=True)

        def compute_values(self) -> list[int]:
            return []

        values = materialized_property(compute_values)

    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        m = Model()
        session.add(m)
        session.flush()

        with pytest.raises(TypeError, match="does not accept None items"):
            m.values = [None]  # type: ignore[list-item]

        with pytest.raises(TypeError, match="list items must be"):
            m.values = ["x"]  # type: ignore[list-item]


def test_descriptor_validate_value_scalar_non_fk_wrong_type_raises():
    """Covers validate_value() non-FK scalar wrong type branch."""

    from etl_decorators.sqlalchemy import materialized_property

    class Base(DeclarativeBase):
        pass

    class Model(Base):
        __tablename__ = "descriptor_scalar_int_model"
        __allow_unmapped__ = True

        id: Mapped[int] = mapped_column(primary_key=True)

        def compute_value(self) -> int:
            return 1

        value = materialized_property(compute_value)

    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        m = Model()
        session.add(m)
        session.flush()

        with pytest.raises(TypeError, match="expected <class 'int'>"):
            m.value = "x"  # type: ignore[assignment]


def test_descriptor_validate_false_early_return_allows_none_for_non_optional_scalar():
    """Covers validate_value() early return when validate=False."""

    from etl_decorators.sqlalchemy import materialized_property

    class Base(DeclarativeBase):
        pass

    class Model(Base):
        __tablename__ = "descriptor_validate_false_scalar"
        __allow_unmapped__ = True

        id: Mapped[int] = mapped_column(primary_key=True)

        def compute_value(self) -> int:
            return 1

        value = materialized_property(compute_value, validate=False)

    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        m = Model()
        session.add(m)
        session.flush()

        # Would normally raise (int is not Optional), but validate=False allows it.
        m.value = None
        assert m.value is None


def test_descriptor_pk_python_type_fallback_branches(monkeypatch):
    """Covers `_pk_python_type` fallbacks:

    - composite PK => returns None
    - missing pk_col.type => returns None
    - sa.inspect raising => returns None

    We assert behavior indirectly by ensuring string identifiers are NOT
    accepted when fk_pk_py_type is forced to None.
    """

    import etl_decorators.sqlalchemy.materialized.descriptor as descriptor
    import etl_decorators.sqlalchemy.materialized.decorator as dec

    class Base(DeclarativeBase):
        pass

    class CompositeTarget(Base):
        __tablename__ = "descriptor_pk_py_type_composite"

        id1: Mapped[int] = mapped_column(primary_key=True)
        id2: Mapped[int] = mapped_column(primary_key=True)

    # composite PK => _pk_python_type returns None (line 119)
    #
    # We want the descriptor to reach `_pk_python_type`, but we *don't* want
    # `make_sa_column` to raise first (it rejects composite PK, which is a
    # separate concern). So we patch make_sa_column for this test.
    def _fake_make_sa_column(_name: str, _t, **_kwargs):  # noqa: ANN001
        return sa.orm.mapped_column(_name, sa.Integer, nullable=True)

    monkeypatch.setattr(descriptor, "make_sa_column", _fake_make_sa_column)

    class ModelComposite(Base):
        __tablename__ = "descriptor_pk_py_type_model_composite"
        __allow_unmapped__ = True

        id: Mapped[int] = mapped_column(primary_key=True)

        def compute(self) -> CompositeTarget:
            raise AssertionError("compute should not be called")

        target = dec.materialized_property(compute)

    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        m = ModelComposite()
        session.add(m)
        session.flush()

        # When pk python type cannot be determined, the descriptor falls back
        # to accepting ints only. A string should fail.
        with pytest.raises(TypeError, match="expected a mapped instance or identifier"):
            m.target = "abc"  # type: ignore[assignment]

    # missing pk_col.type (line 123)
    real_inspect = descriptor.sa.inspect

    class _FakePkColNoType:  # noqa: D401
        """Fake PK column without a 'type' attribute."""

        def __init__(self):
            self.name = "id"

            class _Tbl:
                name = "t"

            self.table = _Tbl()

    class _FakeMapperNoType:
        primary_key = [_FakePkColNoType()]

    monkeypatch.setattr(descriptor.sa, "inspect", lambda cls: _FakeMapperNoType())

    class TargetNoType(Base):
        __tablename__ = "descriptor_pk_py_type_target_no_type"
        id: Mapped[int] = mapped_column(primary_key=True)

    class HolderNoType(Base):
        __tablename__ = "descriptor_pk_py_type_holder_no_type"
        __allow_unmapped__ = True
        id: Mapped[int] = mapped_column(primary_key=True)

        def compute_target(self) -> TargetNoType:
            raise AssertionError("compute should not be called")

        target = dec.materialized_property(compute_target)

    # Trigger mapper config / descriptor closure.
    sa.orm.configure_mappers()

    engine2 = sa.create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine2)

    with Session(engine2) as session:
        h = HolderNoType()
        session.add(h)
        session.flush()
        # fk_pk_py_type is None, so ints are accepted.
        h.target = 1

    # Restore now; we only wanted to hit the branch.
    monkeypatch.setattr(descriptor.sa, "inspect", real_inspect)

    # sa.inspect raising (lines 126-127)
    monkeypatch.setattr(
        descriptor.sa,
        "inspect",
        lambda cls: (_ for _ in ()).throw(RuntimeError("boom")),
    )

    class TargetBoom(Base):
        __tablename__ = "descriptor_pk_py_type_target_boom"
        id: Mapped[int] = mapped_column(primary_key=True)

    class HolderBoom(Base):
        __tablename__ = "descriptor_pk_py_type_holder_boom"
        __allow_unmapped__ = True
        id: Mapped[int] = mapped_column(primary_key=True)

        def compute_target(self) -> TargetBoom:
            raise AssertionError("compute should not be called")

        target = dec.materialized_property(compute_target)

    sa.orm.configure_mappers()

    engine3 = sa.create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine3)
    with Session(engine3) as session:
        h = HolderBoom()
        session.add(h)
        session.flush()
        # With fk_pk_py_type=None due to inspection failure, ints are accepted.
        h.target = 1


def test_descriptor_validate_value_scalar_any_is_not_type_checked(monkeypatch):
    """Covers validate_value() early return when return type is Any."""

    from typing import Any

    import etl_decorators.sqlalchemy.materialized.descriptor as descriptor

    # make_sa_column doesn't support typing.Any, so we patch the descriptor to
    # use a JSON backing column for this test.
    def _fake_make_sa_column(name: str, _t, **_kwargs):  # noqa: ANN001
        return sa.orm.mapped_column(name, sa.JSON, nullable=True)

    monkeypatch.setattr(descriptor, "make_sa_column", _fake_make_sa_column)

    from etl_decorators.sqlalchemy import materialized_property

    class Base(DeclarativeBase):
        pass

    class Model(Base):
        __tablename__ = "descriptor_scalar_any_model"
        __allow_unmapped__ = True

        id: Mapped[int] = mapped_column(primary_key=True)

        def compute_value(self) -> Any:
            return 1

        value = materialized_property(compute_value)

    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        m = Model()
        session.add(m)
        session.flush()

        # Any should accept arbitrary values.
        m.value = "x"
        assert m.value == "x"

def test_descriptor_is_valid_identifier_handles_instancecheck_exceptions():
    """Covers `_is_valid_identifier` try/except around isinstance(...).

    This hits descriptor.py lines where isinstance(v, fk_pk_py_type) raises and
    the code falls back to accepting ints only.
    """

    from etl_decorators.sqlalchemy import materialized_property

    class RaisingMeta(type):
        def __instancecheck__(cls, instance):  # noqa: ANN001
            raise RuntimeError("boom")

    class RaisingPyType(metaclass=RaisingMeta):
        pass

    class RaisingType(sa.types.TypeDecorator):
        impl = sa.String

        @property
        def python_type(self):  # noqa: D401
            """Return a python type whose instancecheck raises."""

            return RaisingPyType

    class Base(DeclarativeBase):
        pass

    class WeirdPK(Base):
        __tablename__ = "descriptor_weird_pk"

        id: Mapped[str] = mapped_column(RaisingType(), primary_key=True)

    class Holder(Base):
        __tablename__ = "descriptor_weird_pk_holder"
        __allow_unmapped__ = True

        id: Mapped[int] = mapped_column(primary_key=True)

        def compute(self) -> WeirdPK:
            raise AssertionError("compute should not be called")

        weird = materialized_property(compute)

    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        h = Holder()
        session.add(h)
        session.flush()

        # Passing a non-int should fail; the internal isinstance(v, RaisingPyType)
        # raises, is swallowed, and we fall back to isinstance(v, int) which is False.
        with pytest.raises(TypeError, match="expected a mapped instance or identifier"):
            h.weird = "abc"  # type: ignore[assignment]
