# src/fastflowtransform/_version.py
from __future__ import annotations

import os
from importlib.metadata import PackageNotFoundError, version as pkg_version
from pathlib import Path

"""
Lightweight version helper.

The public contract is a single constant:

    __version__: str

Resolution order (first hit wins):
1) Environment variable FFT_VERSION
2) Installed package metadata (importlib.metadata)
3) Optional sidecar file next to this module named "_VERSION"
4) Fallback: "0.0.0+dev"
"""


def _resolve_version() -> str:
    # 1) Explicit override via environment (useful in CI/CD)
    env = os.getenv("FFT_VERSION")
    if env:
        return env

    # 2) Installed package metadata (works for normal + editable installs)
    try:
        v = pkg_version("fastflowtransform")
        if v and isinstance(v, str):
            return v
    except PackageNotFoundError:
        pass
    except Exception:
        # Ignore unexpected metadata issues
        pass

    # 3) Optional sidecar file (can be written by release tooling)
    sidecar = Path(__file__).with_name("_VERSION")
    try:
        if sidecar.exists():
            txt = sidecar.read_text(encoding="utf-8").strip()
            if txt:
                return txt
    except Exception:
        pass

    # 4) Last resort
    return "0.0.0+dev"


__version__: str = _resolve_version()
