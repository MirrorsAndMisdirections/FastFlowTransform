# src/fastflowtransform/validation.py
from __future__ import annotations

from typing import Any

import pandas as pd


def validate_required_columns(node_name: str, inputs: Any, requires: dict[str, set[str]]) -> None:
    if not requires:
        return
    errors = []

    if isinstance(inputs, pd.DataFrame):
        # 1 Dep - requires muss genau 1 key haben
        need = next(iter(requires.values())) if requires else set()
        have = set(inputs.columns)
        miss = need - have
        if miss:
            errors.append(f"- missing columns: {sorted(miss)} | have={sorted(have)}")
    else:
        # >1 Deps
        for rel, need in requires.items():
            if rel not in inputs:
                errors.append(f"- missing dependency key '{rel}' in inputs dict")
                continue
            have = set(inputs[rel].columns)
            miss = need - have
            if miss:
                errors.append(f"- [{rel}] missing columns: {sorted(miss)} | have={sorted(have)}")

    if errors:
        raise ValueError(
            "Required columns check failed for Python model "
            f"'{node_name}'.\n"
            + "\n".join(errors)
            + "\nHint: define/adjust `require=` in @model or fix upstream models/seeds."
        )
