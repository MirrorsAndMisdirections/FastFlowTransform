# tests/conftest.py
import pytest

pytest_plugins = ["tests.common.fixtures"]


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    # Marker expression from CLI: -m "..."
    markexpr = getattr(config.option, "markexpr", "") or ""

    # If the user explicitly mentions remote_engine in -m,
    # we assume they *want* those tests. Don't deselect them.
    if "remote_engine" in markexpr:
        return

    deselected: list[pytest.Item] = []
    kept: list[pytest.Item] = []

    for item in items:
        if "remote_engine" in item.keywords:
            deselected.append(item)
        else:
            kept.append(item)

    if deselected:
        config.hook.pytest_deselected(items=deselected)
        items[:] = kept
