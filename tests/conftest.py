"""Test configuration.

The project uses a `src/` layout and in this kata/repo context the package
isn't necessarily installed (e.g. via `pip install -e .`) before running tests.

To make `import etl_decorators` work reliably under pytest, we add `src/`
to `sys.path`.
"""

from __future__ import annotations

import sys
from pathlib import Path

import os
import warnings

import pytest

import importlib


SRC_DIR = (Path(__file__).resolve().parents[1] / "src").as_posix()
# Ensure ./src has priority over any installed distribution.
if SRC_DIR in sys.path:
    sys.path.remove(SRC_DIR)
sys.path.insert(0, SRC_DIR)

# If the package is already imported (e.g. because an external plugin imported
# the installed distribution before our sys.path tweak), ensure we use the local
# sources under ./src.
existing = sys.modules.get("etl_decorators")
if existing is not None:
    mod_file = getattr(existing, "__file__", "") or ""
    if SRC_DIR not in mod_file:
        for name in list(sys.modules.keys()):
            if name == "etl_decorators" or name.startswith("etl_decorators."):
                del sys.modules[name]

# Force-import the package now so all subsequent imports in tests resolve to
# our local sources (and not an installed distribution that may exist in the
# virtualenv).
_ed = importlib.import_module("etl_decorators")
_ed_file = getattr(_ed, "__file__", "") or ""
if SRC_DIR not in _ed_file:
    for name in list(sys.modules.keys()):
        if name == "etl_decorators" or name.startswith("etl_decorators."):
            del sys.modules[name]
    _ed = importlib.import_module("etl_decorators")


def _load_dotenv_from_project_root() -> None:
    """Load env vars from the project's `.env`.

    We prefer python-dotenv when available, but we also support a tiny fallback
    parser so that integration tests can be enabled without extra deps.
    """

    root = Path(__file__).resolve().parents[1]
    env_path = root / ".env"
    if not env_path.exists():
        return

    # 1) Preferred: python-dotenv
    try:  # pragma: no cover
        from dotenv import load_dotenv

        load_dotenv(dotenv_path=env_path, override=False)
        return
    except Exception:
        pass

    # 2) Fallback: minimal .env parsing
    try:
        for raw in env_path.read_text().splitlines():
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            k, v = line.split("=", 1)
            k = k.strip()
            v = v.strip().strip('"').strip("'")
            if k and k not in os.environ:
                os.environ[k] = v
    except Exception:  # pragma: no cover
        # Never fail unit tests because of dotenv parsing.
        return


_load_dotenv_from_project_root()


@pytest.fixture
def llm():
    """Return a real `LLM` instance for integration tests.

    This fixture is opt-in: it will skip if required environment variables are
    missing.
    """

    model = os.getenv("ETL_DECORATORS_TESTS_LLM_MODEL")
    api_key = os.getenv("ETL_DECORATORS_TESTS_LLM_API_KEY")

    if not model or not api_key:
        warnings.warn(
            "LLM integration tests skipped: set ETL_DECORATORS_TESTS_LLM_MODEL and "
            "ETL_DECORATORS_TESTS_LLM_API_KEY (you can use a local .env file).",
            RuntimeWarning,
            stacklevel=2,
        )
        pytest.skip("LLM integration tests not configured")

    # Also require optional dependency.
    try:
        import litellm  # noqa: F401
    except Exception:
        warnings.warn(
            "LLM integration tests skipped: `litellm` is not installed. "
            "Install with: pip install etl-decorators[llms]",
            RuntimeWarning,
            stacklevel=2,
        )
        pytest.skip("LLM integration tests require litellm")

    from etl_decorators.llms import LLM

    return LLM(model=model, api_key=api_key)
