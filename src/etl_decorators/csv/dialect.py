"""CSV dialect detection helpers."""

from __future__ import annotations

import csv as _csv
from dataclasses import dataclass


@dataclass(frozen=True)
class _DialectParams:
    delimiter: str | None
    quotechar: str | None
    doublequote: bool | None
    quoting: int | None


def _sniff_csv_dialect(
    text: str,
    *,
    delimiter: str | None,
    quotechar: str | None,
    doublequote: bool | None,
    quoting: int | None,
) -> _DialectParams:
    """Return effective csv dialect parameters.

    If `delimiter` is None, use csv.Sniffer on a small sample.
    Explicit parameters always win over sniffed values.
    """

    if delimiter is not None:
        # user forced delimiter: don't sniff
        return _DialectParams(
            delimiter=delimiter,
            quotechar=quotechar,
            doublequote=doublequote,
            quoting=quoting,
        )

    sample = text[:64_000]
    try:
        dialect = _csv.Sniffer().sniff(sample, delimiters=[",", ";", "\t", "|", ":"])
    except Exception:
        # fallback to comma
        dialect = _csv.get_dialect("excel")

    return _DialectParams(
        delimiter=getattr(dialect, "delimiter", ","),
        quotechar=quotechar if quotechar is not None else getattr(dialect, "quotechar", '"'),
        doublequote=doublequote
        if doublequote is not None
        else getattr(dialect, "doublequote", True),
        quoting=quoting if quoting is not None else getattr(dialect, "quoting", _csv.QUOTE_MINIMAL),
    )
