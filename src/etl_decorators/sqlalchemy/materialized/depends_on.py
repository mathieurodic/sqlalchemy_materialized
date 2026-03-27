from __future__ import annotations

from sqlalchemy import event
import sqlalchemy as sa


def setup_dependency_invalidation(descriptor, owner: type) -> None:
    """Install SQLAlchemy attribute listeners to invalidate cache.

    Semantics:
    - invalidation is purely in-memory (no flush)
    - for scalar-backed properties, backing storage is cleared (set to None)
    - for list[MappedClass] properties, association rows are cleared
    - referenced target rows are NOT deleted
    """

    depends_on = tuple(descriptor.config.depends_on or ())
    if not depends_on:
        return

    cache_attr = descriptor.cache_attr
    computed_at_attr = descriptor.computed_at_attr
    prop_name = descriptor._prop_name
    is_list_fk = bool(descriptor._is_list and descriptor._is_fk)

    def invalidate(target):
        # If not computed yet, nothing to do.
        if getattr(target, computed_at_attr, None) is None:
            return

        # Clear cached storage.
        if is_list_fk:
            # list[MappedClass] is stored as an actual relationship collection.
            # NOTE: no deleter exists anymore for relationship-based properties.
            if prop_name is not None:
                setattr(target, prop_name, [])
        else:
            setattr(target, cache_attr, None)

        # Mark as not computed.
        setattr(target, computed_at_attr, None)

    # Event callbacks for the various attribute event signatures.
    def on_set(target, value, oldvalue, initiator):  # noqa: ARG001
        invalidate(target)

    def on_append(target, value, initiator):  # noqa: ARG001
        invalidate(target)

    def on_remove(target, value, initiator):  # noqa: ARG001
        invalidate(target)

    def on_bulk_replace(target, values, initiator):  # noqa: ARG001
        invalidate(target)

    def attach_listeners(attr, dep_name: str):  # noqa: ARG001
        # Scalar attribute / scalar relationship
        try:
            event.listen(attr, "set", on_set, retval=False)
        except Exception:
            # Not all attribute implementations support "set".
            pass

        # Collection relationship
        for evt_name, fn in (
            ("append", on_append),
            ("remove", on_remove),
            ("bulk_replace", on_bulk_replace),
        ):
            try:
                event.listen(attr, evt_name, fn, retval=False)
            except Exception:
                # Not a collection or event not supported.
                pass

    for dep_name in depends_on:
        if not isinstance(dep_name, str) or not dep_name:
            raise TypeError(
                "materialized_property(depends_on=...): dependency names must be non-empty strings"
            )

        if not hasattr(owner, dep_name):
            raise AttributeError(
                f"materialized_property(depends_on=...): {owner.__name__!r} has no attribute {dep_name!r}"
            )

    def install_for_mapped_class(mapper, cls):  # noqa: ARG001
        # Ensure we only install listeners for the specific class this
        # descriptor is attached to.
        if cls is not owner:
            return

        for dep_name in depends_on:
            dep_attr = getattr(cls, dep_name)
            attach_listeners(dep_attr, dep_name)

        # Important: don't remove ourselves immediately, because we're
        # currently iterating the listener deque inside SQLAlchemy.
        # Removing here would raise "deque mutated during iteration".
        #
        # Instead we schedule removal for the end of the current
        # configuration cycle.
        def remove_self():
            try:
                event.remove(sa.orm.Mapper, "mapper_configured", install_for_mapped_class)
            except Exception:
                pass

        event.listen(sa.orm.Mapper, "after_configured", remove_self, once=True)

    # Install after mapping/instrumentation is ready.
    event.listen(sa.orm.Mapper, "mapper_configured", install_for_mapped_class)
