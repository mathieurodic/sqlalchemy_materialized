from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy import inspect as sa_inspect
from sqlalchemy.orm import object_session


def _is_mapped_class(t: type) -> bool:
    try:
        mapper = sa_inspect(t)
    except Exception:
        return False
    return hasattr(mapper, "primary_key")


def _pk_id_from_instance(obj) -> object:
    insp = sa_inspect(obj)
    ident = getattr(insp, "identity", None)
    if not ident:
        raise ValueError(
            "Unable to extract identity (PK) from the given instance. "
            "Is it persisted / flushed?"
        )
    if len(ident) != 1:
        raise ValueError(
            "materialized_property only supports single-column primary keys "
            f"(composite PK detected for {obj!r})."
        )
    return ident[0]


def _require_session(obj) -> sa.orm.Session:
    session = object_session(obj)
    if session is None:
        raise RuntimeError(
            "materialized_property: object must be attached to a Session "
            "(detached instances are not supported)."
        )
    return session
