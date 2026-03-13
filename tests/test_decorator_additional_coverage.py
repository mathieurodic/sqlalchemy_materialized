import sqlalchemy as sa
import pytest
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, relationship


def test_materialized_property_raises_when_missing_return_annotation():
    from sqlalchemy_materialized.decorator import materialized_property

    def compute(self):
        return 1

    with pytest.raises(TypeError, match="must have a return annotation"):
        materialized_property(compute)


def test_materialized_property_depends_on_rejects_non_string_or_empty_dependency():
    from sqlalchemy_materialized.decorator import materialized_property

    def compute(self) -> int:
        return 1

    class Base(DeclarativeBase):
        pass

    with pytest.raises(TypeError, match="dependency names must be non-empty strings"):
        class Model1(Base):  # noqa: F811
            __tablename__ = "depends_on_invalid_1"
            __allow_unmapped__ = True

            id: Mapped[int] = mapped_column(primary_key=True)
            value = materialized_property(compute, depends_on=("",))

    with pytest.raises(TypeError, match="dependency names must be non-empty strings"):
        class Model2(Base):  # noqa: F811
            __tablename__ = "depends_on_invalid_2"
            __allow_unmapped__ = True

            id: Mapped[int] = mapped_column(primary_key=True)
            value = materialized_property(compute, depends_on=(123,))  # type: ignore[arg-type]


def test_materialized_property_depends_on_rejects_missing_attribute():
    from sqlalchemy_materialized.decorator import materialized_property

    def compute(self) -> int:
        return 1

    class Base(DeclarativeBase):
        pass

    with pytest.raises(AttributeError, match="has no attribute"):
        class Model(Base):
            __tablename__ = "depends_on_missing_attr"
            __allow_unmapped__ = True

            id: Mapped[int] = mapped_column(primary_key=True)
            value = materialized_property(compute, depends_on=("does_not_exist",))


def test_depends_on_listener_installation_swallows_event_listen_errors(monkeypatch):
    """Covers the defensive try/except around event.listen for attribute events."""

    import sqlalchemy_materialized.decorator as dec

    real_listen = dec.event.listen

    def listen(obj, name, fn, *args, **kwargs):  # noqa: ANN001
        # We still want mapper-level events to be installed so the code path
        # completes, but attribute-level events should raise and be swallowed.
        if name in {"set", "append", "remove", "bulk_replace"}:
            raise RuntimeError("boom")
        return real_listen(obj, name, fn, *args, **kwargs)

    monkeypatch.setattr(dec.event, "listen", listen)

    calls = {"compute": 0}

    def compute(self) -> int:
        calls["compute"] += 1
        return self.base * 2

    class Base(DeclarativeBase):
        pass

    class Model(Base):
        __tablename__ = "depends_on_swallow_listen"
        __allow_unmapped__ = True

        id: Mapped[int] = mapped_column(primary_key=True)
        base: Mapped[int] = mapped_column(sa.Integer)
        value = dec.materialized_property(compute, depends_on=("base",))

    # Triggers mapper configuration and therefore the listener install.
    sa.orm.configure_mappers()

    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        m = Model(base=10)
        session.add(m)
        session.flush()

        assert m.value == 20
        assert calls["compute"] == 1


def test_depends_on_remove_self_swallows_event_remove_errors(monkeypatch):
    """Covers the defensive try/except around event.remove in remove_self()."""

    import sqlalchemy_materialized.decorator as dec

    real_remove = dec.event.remove

    def remove(obj, name, fn):  # noqa: ANN001
        if name == "mapper_configured":
            raise RuntimeError("boom")
        return real_remove(obj, name, fn)

    monkeypatch.setattr(dec.event, "remove", remove)

    def compute(self) -> int:
        return self.base * 2

    class Base(DeclarativeBase):
        pass

    class Model(Base):
        __tablename__ = "depends_on_swallow_remove"
        __allow_unmapped__ = True

        id: Mapped[int] = mapped_column(primary_key=True)
        base: Mapped[int] = mapped_column(sa.Integer)
        value = dec.materialized_property(compute, depends_on=("base",))

    # Triggers mapper configuration and after_configured.
    sa.orm.configure_mappers()


def test_list_fk_storage_raises_when_owner_has_no_primary_key():
    import sqlalchemy_materialized.decorator as dec

    class Base(DeclarativeBase):
        pass

    class Author(Base):
        __tablename__ = "owner_no_pk_author"

        id: Mapped[int] = mapped_column(primary_key=True)

    def compute_authors(self) -> list[Author]:
        return []

    with pytest.raises(RuntimeError, match="no primary key detected"):
        class OwnerNoPK(Base):
            __tablename__ = "owner_no_pk"
            __allow_unmapped__ = True

            # NOTE: no primary key
            x: Mapped[int] = mapped_column(sa.Integer)
            authors = dec.materialized_property(compute_authors)


def test_list_fk_storage_raises_when_target_has_composite_primary_key():
    import sqlalchemy_materialized.decorator as dec

    class Base(DeclarativeBase):
        pass

    class CompositeAuthor(Base):
        __tablename__ = "target_composite_pk"

        id1: Mapped[int] = mapped_column(primary_key=True)
        id2: Mapped[int] = mapped_column(primary_key=True)

    def compute_authors(self) -> list[CompositeAuthor]:
        return []

    with pytest.raises(ValueError, match="composite PK"):
        class Owner(Base):
            __tablename__ = "owner_list_fk_composite_target"
            __allow_unmapped__ = True

            id: Mapped[int] = mapped_column(primary_key=True)
            authors = dec.materialized_property(compute_authors)


def test_depends_on_invalidation_clears_association_cache_for_list_fk():
    """Covers invalidation branch for list[MappedClass] properties."""

    from sqlalchemy_materialized.decorator import materialized_property

    calls = {"compute": 0}

    class Base(DeclarativeBase):
        pass

    class Author(Base):
        __tablename__ = "depends_on_list_fk_author"

        id: Mapped[int] = mapped_column(primary_key=True)

    class Post(Base):
        __tablename__ = "depends_on_list_fk_post"
        __allow_unmapped__ = True

        id: Mapped[int] = mapped_column(primary_key=True)
        base: Mapped[int] = mapped_column(sa.Integer)

        def compute_authors(self) -> list[Author]:
            calls["compute"] += 1
            # base controls whether we return 0 or 1 authors
            return []

        authors = materialized_property(compute_authors, depends_on=("base",))

    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        p = Post(base=1)
        session.add(p)
        session.flush()

        assert p.authors == []
        assert calls["compute"] == 1
        assert getattr(p, "_compute_authors__computed_at") is not None

        # Changing dependency should invalidate and clear association relationship
        p.base = 2
        assert getattr(p, "_compute_authors__computed_at") is None
        assert getattr(p, "_compute_authors") == []


def test_depends_on_relationship_remove_and_bulk_replace_invalidates():
    from sqlalchemy_materialized.decorator import materialized_property

    calls = {"compute": 0}

    class Base(DeclarativeBase):
        pass

    class Child(Base):
        __tablename__ = "depends_on_child_rm"

        id: Mapped[int] = mapped_column(primary_key=True)
        parent_id: Mapped[int] = mapped_column(sa.ForeignKey("depends_on_parent_rm.id"))

    class Parent(Base):
        __tablename__ = "depends_on_parent_rm"
        __allow_unmapped__ = True

        id: Mapped[int] = mapped_column(primary_key=True)
        children: Mapped[list[Child]] = relationship(Child, cascade="all, delete-orphan")

        def compute_children_count(self) -> int:
            calls["compute"] += 1
            return len(self.children)

        children_count = materialized_property(compute_children_count, depends_on=("children",))

    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        p = Parent(children=[Child(), Child()])
        session.add(p)
        session.flush()

        assert p.children_count == 2
        assert calls["compute"] == 1

        # remove
        p.children.pop()
        assert getattr(p, "_compute_children_count__computed_at") is None

        assert p.children_count == 1
        assert calls["compute"] == 2

        # bulk replace (assignment)
        p.children = [Child(), Child(), Child()]
        assert getattr(p, "_compute_children_count__computed_at") is None
        assert p.children_count == 3
        assert calls["compute"] == 3


def test_in_transaction_false_uses_nullcontext_path():
    from sqlalchemy_materialized.decorator import materialized_property

    calls = {"compute": 0}

    def compute(self) -> int:
        calls["compute"] += 1
        return self.base + 1

    class Base(DeclarativeBase):
        pass

    class Model(Base):
        __tablename__ = "nullcontext_path"
        __allow_unmapped__ = True

        id: Mapped[int] = mapped_column(primary_key=True)
        base: Mapped[int] = mapped_column(sa.Integer)
        value = materialized_property(compute, in_transaction=False)

    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        m = Model(base=1)
        session.add(m)
        session.flush()

        assert m.value == 2
        assert calls["compute"] == 1


def test_hybrid_expression_raises_for_list_return_types():
    from sqlalchemy_materialized.decorator import materialized_property

    def compute(self) -> list[int]:
        return [1]

    class Base(DeclarativeBase):
        pass

    class Model(Base):
        __tablename__ = "expr_list_raises"
        __allow_unmapped__ = True

        id: Mapped[int] = mapped_column(primary_key=True)
        value = materialized_property(compute)

    with pytest.raises(NotImplementedError, match="SQL expression is not supported"):
        _ = Model.value.expression


def test_list_fk_setter_accepts_instances_and_ids_and_deleter_clears():
    from sqlalchemy_materialized.decorator import materialized_property

    class Base(DeclarativeBase):
        pass

    class Author(Base):
        __tablename__ = "setter_list_fk_author"

        id: Mapped[int] = mapped_column(primary_key=True)

    class Post(Base):
        __tablename__ = "setter_list_fk_post"
        __allow_unmapped__ = True

        id: Mapped[int] = mapped_column(primary_key=True)

        def compute_authors(self) -> list[Author]:
            return []

        authors = materialized_property(compute_authors)

    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        a1 = Author()
        session.add(a1)
        session.flush()

        p = Post()
        session.add(p)
        session.flush()

        # setter with instance
        p.authors = [a1]
        assert [a.id for a in p.authors] == [a1.id]

        import pytest

        # With strict validation, None is rejected when the return annotation is not Optional.
        with pytest.raises(TypeError, match="None is not allowed"):
            p.authors = None

        # setter with unknown id triggers the "id not found" RuntimeError
        with pytest.raises(RuntimeError, match="not found"):
            p.authors = [999999]

        # deleter clears association
        p.authors = [a1]
        del p.authors
        assert p.authors == []


def test_materialized_property_decorator_factory_fn_none_path():
    """Covers materialized_property(fn is None) wrapper path."""

    from sqlalchemy_materialized.decorator import materialized_property

    class Base(DeclarativeBase):
        pass

    class Model(Base):
        __tablename__ = "factory_fn_none"
        __allow_unmapped__ = True

        id: Mapped[int] = mapped_column(primary_key=True)
        base: Mapped[int] = mapped_column(sa.Integer)

        @materialized_property(in_transaction=False)
        def value(self) -> int:
            return self.base * 2

    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        m = Model(base=2)
        session.add(m)
        session.flush()
        assert m.value == 4


def test_normalize_to_id_list_branch_via_inject_list_fk_storage_noop(monkeypatch):
    """Forces the `normalize_to_id()` list branch.

    Normally list[MappedClass] uses association-table storage, which bypasses
    `normalize_to_id` for list values. Here we force the descriptor to use a
    JSON backing column by skipping `_inject_list_fk_storage`.
    """

    import sqlalchemy_materialized.decorator as dec

    monkeypatch.setattr(
        dec._MaterializedPropertyDescriptor,
        "_inject_list_fk_storage",
        lambda self, owner: None,
    )

    class Base(DeclarativeBase):
        pass

    class Author(Base):
        __tablename__ = "normalize_to_id_list_author"

        id: Mapped[int] = mapped_column(primary_key=True)

    class Post(Base):
        __tablename__ = "normalize_to_id_list_post"
        __allow_unmapped__ = True

        id: Mapped[int] = mapped_column(primary_key=True)

        def compute_authors(self) -> list[Author]:
            return []

        authors = dec.materialized_property(compute_authors)

    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        a1 = Author()
        session.add(a1)
        session.flush()

        p = Post()
        session.add(p)
        session.flush()

        import pytest

        # With strict validation, None is rejected when the return annotation is not Optional.
        with pytest.raises(TypeError, match="None is not allowed"):
            p.authors = None

        # wrong type branch
        with pytest.raises(TypeError, match="expected a list"):
            p.authors = 123  # type: ignore[assignment]

        # list item None branch
        with pytest.raises(TypeError, match="does not accept None items"):
            p.authors = [None]  # type: ignore[list-item]

        # mixed list of instance + id
        p.authors = [a1, a1.id]
        assert getattr(p, "_compute_authors") == [a1.id, a1.id]

        # getter is_list branch returns backing list
        assert p.authors == [a1.id, a1.id]
