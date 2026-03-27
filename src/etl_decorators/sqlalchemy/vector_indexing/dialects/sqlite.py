from __future__ import annotations

import struct
from typing import Any, Callable

import sqlalchemy as sa


def pack_embedding(vec: Any) -> bytes:
    """Pack a vector as float32 bytes for sqlite-vec.

    Accepts list/tuple/numpy.ndarray. We avoid importing numpy at module import
    time to keep optional deps light.
    """

    # Numpy support (optional)
    try:
        import numpy as np  # type: ignore
    except Exception:  # pragma: no cover
        np = None  # type: ignore

    if np is not None and isinstance(vec, np.ndarray):
        arr = vec.astype(np.float32, copy=False)
        return struct.pack(f"{len(arr)}f", *arr.tolist())

    if isinstance(vec, (list, tuple)):
        return struct.pack(f"{len(vec)}f", *[float(x) for x in vec])

    raise TypeError(
        "index_embedding_using: embedder must return list[float] | tuple[float, ...] | numpy.ndarray"
    )


def _infer_dims(embedder: Callable[[str], object]) -> int:
    vec = embedder("")
    # Numpy arrays or list/tuple
    try:
        return len(vec)  # type: ignore[arg-type]
    except Exception as e:  # pragma: no cover
        raise TypeError(
            "index_embedding_using: unable to determine embedding dimension from embedder output"
        ) from e


def _vec_table_name(model_cls: type, column_name: str) -> str:
    table_name = getattr(model_cls, "__tablename__", model_cls.__name__.lower())
    return f"_{table_name}__{column_name}__vec"


def _load_sqlite_vec(dbapi_conn) -> None:  # noqa: ANN001
    """Load sqlite-vec extension on a raw sqlite3 connection."""

    try:
        import sqlite_vec  # type: ignore
    except Exception as e:  # pragma: no cover
        # Optional dependency. When absent, vector indexing features are not
        # available.
        raise RuntimeError(
            "sqlite-vec is required for SQLite vector indexing. "
            "Install etl-decorators[sqlalchemy] (or add sqlite-vec to your env)."
        ) from e

    dbapi_conn.enable_load_extension(True)
    sqlite_vec.load(dbapi_conn)
    dbapi_conn.enable_load_extension(False)


def install_sqlite_vector_indexing(spec) -> None:  # noqa: ANN001
    """Install vector indexing hooks for SQLite.

    We don't attempt to detect dialect at registration time; instead the hooks
    are guarded at runtime by `connection.dialect.name == 'sqlite'`.
    """

    model_cls = spec.model_cls
    column_name = spec.column_name
    embedder = spec.embedder

    vec_table = _vec_table_name(model_cls, column_name)
    dims = _infer_dims(embedder)

    # Optional dependency: if sqlite-vec isn't available (or the extension
    # cannot be loaded), skip installation gracefully so the rest of the ORM
    # remains usable.
    try:
        import sqlite_vec  # type: ignore  # noqa: F401
    except Exception:  # pragma: no cover
        return

    # 1) Ensure the extension is loaded on connect.
    #
    # NOTE: This installs a global Engine listener; it is idempotent in
    # practice for this library, but if you use vector indexing heavily, you
    # may want to move this to a more explicit setup API in the future.
    @sa.event.listens_for(sa.engine.Engine, "connect")
    def _on_connect(dbapi_conn, _conn_record):  # noqa: ANN001
        # Only SQLite DB-API has `enable_load_extension`.
        if getattr(dbapi_conn, "enable_load_extension", None) is None:
            return
        try:
            _load_sqlite_vec(dbapi_conn)
        except RuntimeError:
            # Optional dependency missing; keep engine usable for normal ORM
            # operations. Vector similarity queries will fail later with a
            # clearer message.
            return

    # 2) Create the vec table at metadata create_all time.
    # Use after_create so base tables exist.
    @sa.event.listens_for(model_cls.__table__, "after_create")
    def _after_create(target, connection, **_kw):  # noqa: ANN001
        if connection.dialect.name != "sqlite":
            return
        try:
            connection.execute(
                sa.text(
                    f"CREATE VIRTUAL TABLE IF NOT EXISTS {vec_table} "
                    f"USING vec0(embedding float[{dims}] distance_metric=cosine)"
                )
            )
        except Exception:
            # If the extension couldn't be loaded, vec0 won't exist.
            return

    # 3) Upsert embeddings on insert/update.
    def _upsert(connection, pk: Any, text_value: str | None) -> None:
        if connection.dialect.name != "sqlite":
            return
        if text_value is None:
            # remove if value is NULL
            connection.execute(sa.text(f"DELETE FROM {vec_table} WHERE rowid=:id"), {"id": pk})
            return

        embedding = pack_embedding(embedder(text_value))

        # sqlite-vec doesn't support INSERT OR REPLACE / REPLACE (virtual table
        # primary key constraint). Use delete + insert.
        connection.execute(
            sa.text(f"DELETE FROM {vec_table} WHERE rowid=:id"),
            {"id": pk},
        )
        connection.execute(
            sa.text(f"INSERT INTO {vec_table}(rowid, embedding) VALUES (:id, :e)"),
            {"id": pk, "e": embedding},
        )

    @sa.event.listens_for(model_cls, "after_insert")
    def _after_insert(mapper, connection, target):  # noqa: ANN001
        pk = mapper.primary_key_from_instance(target)[0]
        _upsert(connection, pk, getattr(target, column_name))

    @sa.event.listens_for(model_cls, "after_update")
    def _after_update(mapper, connection, target):  # noqa: ANN001
        state = sa.inspect(target)
        hist = state.attrs[column_name].history
        if not hist.has_changes():
            return
        pk = mapper.primary_key_from_instance(target)[0]
        _upsert(connection, pk, getattr(target, column_name))


def sqlite_similarity_expr(
    *,
    source_col: sa.ColumnElement,
    pk_col: sa.Column,
    vec_table: str,
    embedder: Callable[[str], object],
    query: str,
    k: int,
) -> sa.ColumnElement:
    """Return a SQL expression representing similarity (1 - distance).

    Uses a correlated scalar subquery against the vec table.
    """

    # Validate dims vs query embedding at runtime by packing.
    q = pack_embedding(embedder(query))

    if k <= 0:
        raise ValueError("similarity_with(k=...): k must be >= 1")

    # Candidate restriction using the sqlite-vec index:
    #
    # We must compute the top-k neighbors *globally* first, then correlate by
    # rowid. If we correlate first, LIMIT applies to a single-row slice and `k`
    # becomes meaningless.
    #
    # This form is compatible with outer LIMIT/OFFSET (unlike using the `k = ?`
    # clause, which sqlite-vec forbids mixing with LIMIT).
    topk = (
        sa.select(
            sa.column("rowid").label("rowid"),
            sa.column("distance").label("distance"),
        )
        .select_from(sa.text(f"{vec_table}"))
        .where(sa.text("embedding MATCH :q"))
        .order_by(sa.column("distance").asc())
        .limit(sa.bindparam("k", k))
        .subquery("topk")
    )

    # Use a stable, collision-resistant label for the *returned expression*.
    #
    # This matters when a query selects multiple similarity expressions
    # without explicitly labeling them: SQLAlchemy will use the expression's
    # label as the result mapping key.
    #
    # We try to derive a {tablename}__{columnname}__similarity label from the
    # SQLAlchemy column expression.
    similarity_label = "similarity"
    try:
        table_name = getattr(getattr(source_col, "table", None), "name", None)
        column_name = getattr(source_col, "key", None)
        if table_name and column_name:
            similarity_label = f"{table_name}__{column_name}__similarity"
    except Exception:  # pragma: no cover
        # Fall back to the generic label if we can't introspect.
        pass

    similarity_expr = (
        sa.select(sa.literal(1.0) - topk.c.distance)
        .select_from(topk)
        .where(topk.c.rowid == pk_col)
        .scalar_subquery()
        .params(q=q, k=k)
    )

    # IMPORTANT: label the scalar subquery expression itself (not the inner
    # SELECT column), otherwise SQLAlchemy will still assign anon_1/anon_2
    # keys when selecting multiple similarity expressions.
    return similarity_expr.label(similarity_label)


__all__ = [
    "install_sqlite_vector_indexing",
    "pack_embedding",
    "sqlite_similarity_expr",
]
