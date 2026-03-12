import sqlalchemy as sa
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


def test_getter_computes_once_and_flushes_when_in_session(monkeypatch):
    """Unit test: verify caching & flush behavior without needing a real DB."""
    import sqlalchemy_materialized.decorator as dec

    calls = {"compute": 0, "flush": 0}

    class _BeginNested:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            # emulate SA behavior: rollback handled by session.begin_nested(); propagate errors
            return False

    class FakeSession:
        def begin_nested(self):
            return _BeginNested()

        def flush(self):
            calls["flush"] += 1

    def fake_object_session(obj):
        return FakeSession()

    monkeypatch.setattr(dec, "object_session", fake_object_session)

    def compute(self) -> int:
        calls["compute"] += 1
        return 7

    class Base(DeclarativeBase):
        pass

    class Model(Base):
        __tablename__ = "model_unit_session"
        __allow_unmapped__ = True

        id: Mapped[int] = mapped_column(primary_key=True)
        value = dec.materialized_property(compute)

    m = Model()

    # First access computes and flushes
    assert m.value == 7
    assert calls == {"compute": 1, "flush": 1}
    assert getattr(m, "_compute__computed_at") is not None

    # Second access returns cached value: no compute, no flush
    assert m.value == 7
    assert calls == {"compute": 1, "flush": 1}


def test_getter_computes_once_and_does_not_flush_when_not_in_session(monkeypatch):
    import sqlalchemy_materialized.decorator as dec

    calls = {"compute": 0}

    def fake_object_session(obj):
        return None

    monkeypatch.setattr(dec, "object_session", fake_object_session)

    def compute(self) -> int:
        calls["compute"] += 1
        return 5

    class Base(DeclarativeBase):
        pass

    class Model(Base):
        __tablename__ = "model_unit_no_session"
        __allow_unmapped__ = True

        id: Mapped[int] = mapped_column(primary_key=True)
        value = dec.materialized_property(compute)

    m = Model()
    import pytest

    with pytest.raises(RuntimeError, match="attached"):
        _ = m.value
    assert calls["compute"] == 0
    assert getattr(m, "_compute__computed_at") is None


def test_setter_sets_backing_value_directly(monkeypatch):
    import sqlalchemy_materialized.decorator as dec

    def fake_object_session(obj):
        # Should not be needed for setter
        raise AssertionError("object_session should not be called by setter")

    monkeypatch.setattr(dec, "object_session", fake_object_session)

    def compute(self) -> int:
        # Should not be called when using setter
        raise AssertionError("compute should not be called when setting")

    class Base(DeclarativeBase):
        pass

    class Model(Base):
        __tablename__ = "model_setter"
        __allow_unmapped__ = True

        id: Mapped[int] = mapped_column(primary_key=True)
        value = dec.materialized_property(compute)

    m = Model()
    m.value = 42
    assert m.value == 42
    assert getattr(m, "_compute") == 42
    assert getattr(m, "_compute__computed_at") is not None


def test_deleter_sets_backing_to_none_and_flushes_when_in_session(monkeypatch):
    import sqlalchemy_materialized.decorator as dec

    calls = {"flush": 0}

    class _BeginNested:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class FakeSession:
        def begin_nested(self):
            return _BeginNested()

        def flush(self):
            calls["flush"] += 1

    def fake_object_session(obj):
        return FakeSession()

    monkeypatch.setattr(dec, "object_session", fake_object_session)

    def compute(self) -> int:
        return 9

    class Base(DeclarativeBase):
        pass

    class Model(Base):
        __tablename__ = "model_deleter"
        __allow_unmapped__ = True

        id: Mapped[int] = mapped_column(primary_key=True)
        value = dec.materialized_property(compute)

    m = Model()
    m.value = 1
    assert getattr(m, "_compute") == 1
    assert getattr(m, "_compute__computed_at") is not None

    del m.value
    assert getattr(m, "_compute") is None
    assert getattr(m, "_compute__computed_at") is None
    assert calls["flush"] == 1
