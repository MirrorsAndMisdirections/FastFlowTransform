from fastflowtransform import engine_model
import pandas as pd


@engine_model(
    only="postgres",
    name="fct_events_py_incremental",
    deps=["events_base.ff"],
    tags=[
        "example:incremental_demo",
        "scope:engine",
        "engine:postgres",
        "kind:python",
        "kind:incremental",
    ],
)
def build(events_df: pd.DataFrame) -> pd.DataFrame:
    """
    Python-Incremental-Beispiel (Postgres).

    Gleiche Semantik wie bei DuckDB; Snapshot out, Incremental-Logik in der Engine.
    """
    df = events_df.copy()
    df["value_x10"] = df["value"] * 10
    return df[["event_id", "updated_at", "value", "value_x10"]]
