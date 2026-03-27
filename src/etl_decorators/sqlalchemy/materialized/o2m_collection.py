from __future__ import annotations

from contextlib import nullcontext
from datetime import datetime, timezone
import logging
from typing import Any, Callable

from .helpers import _require_session

import sqlalchemy as sa

try:  # pragma: no cover
    from sqlalchemy.orm.collections import collection_adapter
except Exception:  # pragma: no cover
    collection_adapter = None  # type: ignore[assignment]


logger = logging.getLogger(__name__)


class _MaterializedO2MList(list):
    """A relationship collection that materializes on first meaningful access.

    This is used for `materialized_property` when the return type is
    `list[MappedClass]`.

    The owning relationship is mapped with `lazy="noload"` so that just
    accessing `obj.prop` doesn't trigger an implicit DB load that would
    bypass the compute-on-first-access semantics.

    SQLAlchemy calls our collection class constructor without arguments.
    We attach runtime context (owner instance, compute fn, etc.) via the
    `init_collection` attribute event.
    """

    # The fields below are set by init_collection handlers.
    _owner: Any | None = None
    _compute_fn: Callable[[Any], Any] | None = None
    _computed_at_attr: str | None = None
    _prop_name: str | None = None
    _child_cls: type | None = None
    _child_owner_attr: str | None = None
    _in_transaction: bool = True
    _validate_value: Callable[[Any], None] | None = None
    _normalize_list_to_instances: Callable[[Any, Any, type], list[Any]] | None = None
    _materializing_guard_attr: str | None = None

    # When the collection is mapped with lazy="noload", SQLAlchemy will not
    # auto-populate it from the DB. Once `computed_at` is set (i.e. the
    # materialized_property was computed previously), we must load the existing
    # children ourselves on first access.
    _loaded_from_db: bool = False

    def _ensure_materialized(self) -> None:
        # Allow both instance-level binding (via events) and class-level binding
        # (set on the collection class).
        owner = self._owner
        compute_fn = self._compute_fn or getattr(self.__class__, "_compute_fn", None)
        computed_at_attr = self._computed_at_attr or getattr(
            self.__class__, "_computed_at_attr", None
        )
        # prop_name is kept mostly for debugging / better error messages.
        prop_name = self._prop_name or getattr(self.__class__, "_prop_name", None)
        child_cls = self._child_cls or getattr(self.__class__, "_child_cls", None)
        child_owner_attr = self._child_owner_attr or getattr(
            self.__class__, "_child_owner_attr", None
        )
        validate_value = self._validate_value or getattr(
            self.__class__, "_validate_value", None
        )
        normalize_list_to_instances = self._normalize_list_to_instances or getattr(
            self.__class__, "_normalize_list_to_instances", None
        )
        in_transaction = self._in_transaction
        if in_transaction is True:
            in_transaction = bool(getattr(self.__class__, "_in_transaction", True))
        guard_attr = self._materializing_guard_attr or getattr(
            self.__class__, "_materializing_guard_attr", None
        )

        if owner is None:
            # Defensive fallback: if init_collection wasn't attached yet, try
            # to recover the owner from SQLAlchemy's collection adapter.
            if collection_adapter is not None:
                try:
                    adapter = collection_adapter(self)
                    if adapter is not None:
                        owner = adapter.owner_state.obj()
                        self._owner = owner
                except Exception:
                    owner = None

        if owner is None or compute_fn is None or computed_at_attr is None:
            return

        if guard_attr and getattr(owner, guard_attr, False):
            return

        # Always require a Session for list[MappedClass] materialized properties,
        # even when already materialized. This keeps behavior consistent with
        # scalar FK materialized properties.
        session = _require_session(owner)

        # If already computed, do *not* recompute, but ensure the relationship
        # collection is populated from the DB at least once.
        if getattr(owner, computed_at_attr, None) is not None:
            if self._loaded_from_db:
                return

            if child_cls is None or child_owner_attr is None:
                # Without mapping info we can't load; keep best-effort semantics.
                self._loaded_from_db = True
                return

            try:
                owner_id = sa.inspect(owner).identity
                owner_id = owner_id[0] if owner_id else None
            except Exception:
                owner_id = None

            # If owner isn't persisted, there can't be persisted children.
            if owner_id is None:
                self._loaded_from_db = True
                return

            fk_attr = f"{type(owner).__name__.lower()}_id"
            if not hasattr(child_cls, fk_attr):
                self._loaded_from_db = True
                return

            # Only hit the DB when the in-memory collection is empty.
            # If it already contains items (e.g. user appended manually), keep them.
            if list.__len__(self) == 0:
                q = session.query(child_cls).filter(getattr(child_cls, fk_attr) == owner_id)
                children = q.all()

                # The query above may already populate the relationship via
                # SQLAlchemy relationship synchronization. Only append manually
                # when the collection is still empty.
                if list.__len__(self) == 0 and children:
                    if collection_adapter is not None:
                        try:
                            adapter = collection_adapter(self)
                            if adapter is not None:
                                adapter.append_multiple_without_event(children)
                            else:
                                list.extend(self, children)
                        except Exception:
                            list.extend(self, children)
                    else:
                        list.extend(self, children)

            self._loaded_from_db = True
            return

        # If user populated the collection manually before first access, consider
        # it materialized and persist computed_at.
        if list.__len__(self) > 0:
            setattr(owner, computed_at_attr, datetime.now(timezone.utc))
            session.flush()
            return

        # Compute + persist.
        old_computed_at = getattr(owner, computed_at_attr)
        try:
            if guard_attr:
                setattr(owner, guard_attr, True)

            cm = session.begin_nested() if in_transaction else nullcontext()
            with cm:
                computed = compute_fn(owner)
                if validate_value is not None:
                    validate_value(computed)

                if normalize_list_to_instances is None:
                    raise RuntimeError(
                        "materialized_property: internal error (normalize_list_to_instances not set)"
                    )
                if child_cls is None or child_owner_attr is None:
                    raise RuntimeError(
                        "materialized_property: internal error (child mapping info not set)"
                    )

                targets = normalize_list_to_instances(owner, computed, child_cls)

                # Link each child to owner.
                # We intentionally set the scalar relationship on the child so
                # SQLAlchemy syncs both sides + FK.
                for child in targets:
                    setattr(child, child_owner_attr, owner)

                # Mark computed.
                setattr(owner, computed_at_attr, datetime.now(timezone.utc))
                session.flush()

                # This instance now reflects the persisted state.
                self._loaded_from_db = True
        except Exception as e:
            logger.error(
                "materialized_property compute failed (%s): %s",
                e.__class__.__name__,
                str(e),
            )
            logger.debug("materialized_property compute traceback", exc_info=True)

            # SAVEPOINT rollback is handled by begin_nested(). We also restore
            # in-memory computed_at so it remains "not computed".
            setattr(owner, computed_at_attr, old_computed_at)
            raise
        finally:
            if guard_attr:
                try:
                    setattr(owner, guard_attr, False)
                except Exception:
                    pass

    # --- read operations that should trigger materialization ---
    def __iter__(self):  # noqa: D105
        self._ensure_materialized()
        return super().__iter__()

    def __len__(self):  # noqa: D105
        self._ensure_materialized()
        return super().__len__()

    def __getitem__(self, i):  # noqa: D105, ANN001
        self._ensure_materialized()
        return super().__getitem__(i)

    def __bool__(self):  # noqa: D105
        self._ensure_materialized()
        return super().__bool__()
