# `etl_decorators._base`

Internal building blocks used by the project’s decorators.

This package is **not part of the public API** (it may change without notice),
but it exists to keep individual decorator implementations small and
consistent.

## Modules

### [`decorators.py`](./decorators.py)

Contains a couple of small helper base classes:

- `DecoratorBase`: standardizes
  - sync/async wrapper generation
  - optional `before_call()` hook (state computed before calling the wrapped fn)
  - `process_result()` hook (post-process the return value)
  - `process_exception()` hook (log/transform errors; default is re-raise)
  - metadata preservation via `functools.update_wrapper`

- `OptionalFnDecoratorBase`: standardizes the common ``fn is None`` binding
  used to support both:
  - `@decorator`
  - `@decorator(...)`

## Why this exists

Several decorators in this repository used to duplicate the same low-level
plumbing (async detection, wrapper creation, `__wrapped__` bookkeeping, etc.).
Centralizing the mechanics here makes the public decorators easier to read and
reduces the chance of subtle inconsistencies.
