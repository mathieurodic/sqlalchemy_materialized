from __future__ import annotations

import struct

import numpy as np
import sqlalchemy as sa
from sqlalchemy.orm import DeclarativeBase, Session

from etl_decorators.sqlalchemy import as_model, field


def _embedder(text: str) -> np.ndarray:
    """A deterministic tiny embedding for tests.

    Dims=3.
    """

    t = text.lower()
    if "apple" in t:
        return np.array([1.0, 0.0, 0.0], dtype=np.float32)
    if "banana" in t:
        return np.array([0.0, 1.0, 0.0], dtype=np.float32)
    return np.array([0.0, 0.0, 1.0], dtype=np.float32)


def test_vector_indexing_creates_vec_table_and_supports_similarity_ordering_sqlite():
    import pytest

    pytest.importorskip("sqlite_vec")
    class Base(DeclarativeBase):
        pass

    @as_model(Base)
    class Product:
        name: str = field(index_embedding_using=_embedder)
        category: str

    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)

    # sqlite-vec table should be created as well
    insp = sa.inspect(engine)
    assert "_products__name__vec" in insp.get_table_names()

    with Session(engine) as session:
        p1 = Product(name="apple pie", category="dessert")
        p2 = Product(name="banana bread", category="dessert")
        p3 = Product(name="carrot cake", category="dessert")
        session.add_all([p1, p2, p3])
        session.flush()

        # Most similar to apple should be p1 first.
        ids = (
            session.query(Product.id)
            .order_by(Product.name.similarity_with("apple").desc(), Product.id.asc())
            .all()
        )
        assert [x[0] for x in ids] == [p1.id, p2.id, p3.id]

        # and we can also filter by a distance threshold
        near = (
            session.query(Product)
            .filter(Product.name.similarity_with("apple") > 0.9)
            .order_by(Product.id.asc())
            .all()
        )
        assert [p.id for p in near] == [p1.id]


def test_similarity_with_k_limits_candidates():
    import pytest

    pytest.importorskip("sqlite_vec")
    """`k` should limit candidate rowids considered similar.

    We use 3 rows; when k=1 only the nearest neighbor should get a non-NULL
    similarity score.
    """

    class Base(DeclarativeBase):
        pass

    @as_model(Base)
    class Product:
        name: str = field(index_embedding_using=_embedder)

    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        p1 = Product(name="apple")
        p2 = Product(name="banana")
        p3 = Product(name="carrot")
        session.add_all([p1, p2, p3])
        session.flush()

        sim1 = session.execute(
            sa.select(Product.id, Product.name.similarity_with("apple", k=1).label("s"))
            .order_by(Product.id.asc())
        ).all()

        # Only p1 should be in the top-1 nearest neighbors for "apple".
        by_id = {row[0]: row[1] for row in sim1}
        assert by_id[p1.id] is not None
        assert by_id[p2.id] is None
        assert by_id[p3.id] is None


def test_similarity_expr_default_labels_do_not_conflict_for_multiple_columns():
    import pytest

    pytest.importorskip("sqlite_vec")
    """Selecting multiple similarity expressions should not collide on result keys.

    Historically the underlying scalar subquery was labeled simply
    "similarity", which can cause ambiguous/duplicated keys in SQLAlchemy
    result mappings when selecting more than one similarity expression.
    """

    class Base(DeclarativeBase):
        pass

    @as_model(Base)
    class Product:
        name: str = field(index_embedding_using=_embedder)
        description: str = field(index_embedding_using=_embedder)

    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        session.add_all(
            [
                Product(name="apple", description="banana"),
                Product(name="banana", description="apple"),
            ]
        )
        session.flush()

        row = session.execute(
            sa.select(
                Product.name.similarity_with("apple"),
                Product.description.similarity_with("apple"),
            ).limit(1)
        ).mappings().first()

        assert row is not None
        assert "products__name__similarity" in row
        assert "products__description__similarity" in row
        assert row["products__name__similarity"] is not None
        assert row["products__description__similarity"] is not None


def test_vector_indexing_updates_are_reindexed():
    import pytest

    pytest.importorskip("sqlite_vec")
    class Base(DeclarativeBase):
        pass

    @as_model(Base)
    class Product:
        name: str = field(index_embedding_using=_embedder)

    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        p1 = Product(name="banana")
        p2 = Product(name="apple")
        session.add_all([p1, p2])
        session.flush()

        # apple should win initially
        best_id = (
            session.query(Product.id)
            .order_by(Product.name.similarity_with("apple").desc())
            .first()
        )[0]
        assert best_id == p2.id

        # Update p1 to become apple-like and flush.
        p1.name = "apple smoothie"
        session.flush()

        best_id2 = (
            session.query(Product.id)
            .order_by(Product.name.similarity_with("apple").desc(), Product.id.asc())
            .first()
        )[0]
        assert best_id2 == p1.id


def test_similarity_with_is_not_available_on_non_indexed_fields():
    class Base(DeclarativeBase):
        pass

    @as_model(Base)
    class Product:
        name: str

    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)

    # Attribute should be missing on the InstrumentedAttribute / comparator
    import pytest

    with pytest.raises(AttributeError):
        _ = Product.name.similarity_with("apple")


def test_sqlite_vec_binary_packing_matches_float32_contract():
    """Unit-level guardrail: our packer should match sqlite-vec expectations."""

    from etl_decorators.sqlalchemy.vector_indexing.dialects.sqlite import pack_embedding

    vec = np.array([1.5, 2.5, 3.5], dtype=np.float64)
    out = pack_embedding(vec)
    assert isinstance(out, (bytes, bytearray))

    back = struct.unpack("3f", out)
    assert list(back) == [1.5, 2.5, 3.5]
