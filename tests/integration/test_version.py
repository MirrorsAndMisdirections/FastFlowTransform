import subprocess

import pytest


@pytest.mark.cli
def test_version_flag():
    cp = subprocess.run(["flowforge", "--version"], check=True, stdout=subprocess.PIPE, text=True)
    out = cp.stdout.strip()
    assert out, "version output empty"
    assert any(ch.isdigit() for ch in out), f"unexpected version format: {out}"
