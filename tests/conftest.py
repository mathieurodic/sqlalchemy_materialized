"""Test configuration.

The project uses a `src/` layout and in this kata/repo context the package
isn't necessarily installed (e.g. via `pip install -e .`) before running tests.

To make `import sqlalchemy_materialized` work reliably under pytest, we add `src/`
to `sys.path`.
"""

from __future__ import annotations

import sys
from pathlib import Path


SRC_DIR = (Path(__file__).resolve().parents[1] / "src").as_posix()
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)
