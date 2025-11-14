import subprocess

import pytest


@pytest.mark.integration
def test_version_flag():
    cp = subprocess.run(["fft", "--version"], check=True, stdout=subprocess.PIPE, text=True)
    out = cp.stdout.strip()
    assert out, "version output empty"
    assert any(ch.isdigit() for ch in out), f"unexpected version format: {out}"
