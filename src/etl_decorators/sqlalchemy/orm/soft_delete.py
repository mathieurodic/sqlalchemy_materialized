from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.orm import Session, with_loader_criteria


_SOFT_DELETE_COLUMNS: dict[type, str] = {}
_OPTION_WITH_DELETED = "etl_decorators.with_deleted"


class _WithDeletedSession:
    def __init__(self, session: Session):
        self._session = session

    def query(self, *entities, **kwargs):  # noqa: ANN001
        return self._session.query(*entities, **kwargs).execution_options(
            **{_OPTION_WITH_DELETED: True}
        )

    def execute(self, statement, *args, **kwargs):  # noqa: ANN001
        stmt = statement.execution_options(**{_OPTION_WITH_DELETED: True})
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


def enable_soft_delete(session: Session) -> Session:
    """Enable soft delete behavior and query filtering for a Session.

    - Session.delete() is converted into a timestamp update.
    - Queries exclude soft-deleted rows unless session.with_deleted() is used.
    """

    def with_deleted(self: Session):  # type: ignore[override]
        return _WithDeletedSession(self)

    if not hasattr(session, "with_deleted"):
        session.with_deleted = with_deleted.__get__(session, Session)  # type: ignore[attr-defined]

    @sa.event.listens_for(session, "do_orm_execute")
    def _soft_delete_orm_execute(execute_state):  # noqa: ANN001
        if not execute_state.is_select:
            return
        if execute_state.execution_options.get(_OPTION_WITH_DELETED):
            return
        execute_state.statement = _apply_soft_delete_filter(execute_state.statement)

    @sa.event.listens_for(session, "before_flush")
    def _soft_delete_before_flush(_session, _flush_context, _instances):  # noqa: ANN001
        for obj in list(_session.deleted):
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


def soft_delete_instance(obj) -> None:
    session = sa.orm.object_session(obj)
    if session is None:
        raise RuntimeError("soft delete requires the instance to be attached to a Session")
    session.delete(obj)