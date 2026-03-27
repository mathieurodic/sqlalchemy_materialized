from __future__ import annotations

from datetime import datetime
import weakref

import sqlalchemy as sa
from sqlalchemy.orm import Session, with_loader_criteria


_SOFT_DELETE_COLUMNS: dict[type, str] = {}
_OPTION_WITH_DELETED = "etl_decorators.with_deleted"
_SESSION_INFO_HARD_DELETE_KEY = "etl_decorators.hard_delete"


class _WithDeletedSession:
    def __init__(
        self,
        session: Session,
        *,
        only: bool = False,
        since: datetime | None = None,
        until: datetime | None = None,
    ):
        self._session = session
        self._only = only
        self._since = since
        self._until = until

        if self._since is not None or self._until is not None:
            # Semantics: time window implies `only=True`.
            self._only = True

    def query(self, *entities, **kwargs):  # noqa: ANN001
        q = self._session.query(*entities, **kwargs).execution_options(
            **{_OPTION_WITH_DELETED: True}
        )
        if not self._only:
            return q

        # Restrict to deleted rows (optionally within a time window).
        for model_cls, column_name in _SOFT_DELETE_COLUMNS.items():
            col = getattr(model_cls, column_name, None)
            if col is None:
                continue
            q = q.options(
                with_loader_criteria(
                    model_cls,
                    _only_deleted_criteria(column_name, since=self._since, until=self._until),
                    include_aliases=True,
                )
            )
        return q

    def execute(self, statement, *args, **kwargs):  # noqa: ANN001
        stmt = statement.execution_options(**{_OPTION_WITH_DELETED: True})
        if self._only:
            stmt = _apply_only_deleted_filter(stmt, since=self._since, until=self._until)
        return self._session.execute(stmt, *args, **kwargs)

    def __getattr__(self, name: str):
        return getattr(self._session, name)


def register_soft_delete_model(model_cls: type, column_name: str) -> None:
    """Register a mapped class that uses soft deletion."""

    _SOFT_DELETE_COLUMNS[model_cls] = column_name


def _criteria_for_column(column_name: str):
    def _criteria(cls, column_name=column_name):  # noqa: ANN001
        return getattr(cls, column_name).is_(None)

    return _criteria


def _only_deleted_criteria(column_name: str, *, since: datetime | None, until: datetime | None):
    # NOTE on SQLAlchemy lambda caching:
    # - We must not branch on `since is None` *inside* the lambda, because
    #   SQLAlchemy wraps closure variables for analysis (PyWrapper), and
    #   `PyWrapper is not None` would always be True.
    # - We also can't pass `since/until` as default args, because those values
    #   are not reliably part of SQLAlchemy's lambda cache key.
    #
    # Therefore, we branch in Python to build a different lambda shape, and
    # capture actual datetime values as closure variables only when used.

    if since is None and until is None:
        def _criteria(cls, column_name=column_name):  # noqa: ANN001
            col = getattr(cls, column_name)
            return col.is_not(None)

        return _criteria

    if since is not None and until is None:
        since_ = since

        def _criteria(cls, column_name=column_name):  # noqa: ANN001
            col = getattr(cls, column_name)
            return sa.and_(col.is_not(None), col >= since_)

        return _criteria

    if since is None and until is not None:
        until_ = until

        def _criteria(cls, column_name=column_name):  # noqa: ANN001
            col = getattr(cls, column_name)
            return sa.and_(col.is_not(None), col < until_)

        return _criteria

    # since is not None and until is not None
    since_ = since
    until_ = until

    def _criteria(cls, column_name=column_name):  # noqa: ANN001
        col = getattr(cls, column_name)
        return sa.and_(col.is_not(None), col >= since_, col < until_)

    return _criteria


def _apply_soft_delete_filter(statement):
    for model_cls, column_name in _SOFT_DELETE_COLUMNS.items():
        col = getattr(model_cls, column_name, None)
        if col is None:
            continue
        statement = statement.options(
            with_loader_criteria(
                model_cls,
                _criteria_for_column(column_name),
                include_aliases=True,
            )
        )
    return statement


def _apply_only_deleted_filter(statement, *, since: datetime | None, until: datetime | None):
    for model_cls, column_name in _SOFT_DELETE_COLUMNS.items():
        col = getattr(model_cls, column_name, None)
        if col is None:
            continue
        statement = statement.options(
            with_loader_criteria(
                model_cls,
                _only_deleted_criteria(column_name, since=since, until=until),
                include_aliases=True,
            )
        )
    return statement


def enable_soft_delete(session: Session) -> Session:
    """Enable soft delete behavior and query filtering for a Session.

    - Session.delete() is converted into a timestamp update.
    - Queries exclude soft-deleted rows unless session.with_deleted() is used.
    """

    def with_deleted(
        self: Session,
        *,
        only: bool = False,
        since: datetime | None = None,
        until: datetime | None = None,
    ):  # type: ignore[override]
        return _WithDeletedSession(self, only=only, since=since, until=until)

    if not hasattr(session, "with_deleted"):
        session.with_deleted = with_deleted.__get__(session, Session)  # type: ignore[attr-defined]

    # Add an opt-out for soft deletion: session.delete(obj, hard=True)
    # This is implemented by keeping a per-session WeakSet of instances that
    # should remain truly deleted during flush.
    if not getattr(session.delete, "_etl_supports_hard_delete", False):
        original_delete = session.delete

        def delete(self: Session, instance, *args, hard: bool = False, **kwargs):  # noqa: ANN001
            if hard:
                hard_set = self.info.get(_SESSION_INFO_HARD_DELETE_KEY)
                if hard_set is None:
                    hard_set = weakref.WeakSet()
                    self.info[_SESSION_INFO_HARD_DELETE_KEY] = hard_set
                hard_set.add(instance)
            return original_delete(instance, *args, **kwargs)

        # Mark wrapper so enable_soft_delete is idempotent.
        delete._etl_supports_hard_delete = True  # type: ignore[attr-defined]
        session.delete = delete.__get__(session, Session)  # type: ignore[method-assign]

    @sa.event.listens_for(session, "do_orm_execute")
    def _soft_delete_orm_execute(execute_state):  # noqa: ANN001
        if not execute_state.is_select:
            return

        # with_deleted() wrapper sets this execution option to bypass the
        # default filter; any extra filtering (only/since/until) is applied
        # directly by the wrapper.
        if execute_state.execution_options.get(_OPTION_WITH_DELETED):
            return

        execute_state.statement = _apply_soft_delete_filter(execute_state.statement)

    @sa.event.listens_for(session, "before_flush")
    def _soft_delete_before_flush(_session, _flush_context, _instances):  # noqa: ANN001
        hard_set = _session.info.get(_SESSION_INFO_HARD_DELETE_KEY)
        for obj in list(_session.deleted):
            if hard_set is not None and obj in hard_set:
                # Hard delete requested: keep the instance in `Session.deleted`.
                hard_set.discard(obj)
                continue

            column_name = _SOFT_DELETE_COLUMNS.get(type(obj))
            if not column_name:
                continue
            updated_attr = getattr(type(obj), "__etl_updated_at_attr__", None)
            if updated_attr:
                current_value = getattr(obj, updated_attr, None)
                setattr(obj, updated_attr, current_value)
                setattr(obj, "_etl_skip_updated_at", True)
            setattr(obj, column_name, sa.func.now())
            _session.add(obj)

    return session


def soft_delete_instance(obj, *, hard: bool = False) -> None:
    session = sa.orm.object_session(obj)
    if session is None:
        raise RuntimeError("soft delete requires the instance to be attached to a Session")
    # Prefer the wrapped `Session.delete(..., hard=...)` when soft delete is
    # enabled; fall back to native Session.delete() for non-enabled sessions.
    try:
        session.delete(obj, hard=hard)
    except TypeError:
        session.delete(obj)