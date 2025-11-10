from fastflowtransform import engine_model
import pandas as pd


@engine_model(
    only="duckdb",
    name="fct_events_py_incremental",
    deps=["events_base.ff"],
    tags=[
        "example:incremental_demo",
        "scope:engine",
        "engine:duckdb",
        "kind:python",
        "kind:incremental",
    ],
)
def build(events_df: pd.DataFrame) -> pd.DataFrame:
    """
    Python-Incremental-Beispiel (DuckDB).

    Dieses Modell baut immer einen vollständigen Snapshot:
      - ggf. leichte Transformation
    Die eigentliche Incremental-Logik (Merge per unique_key, Delta-Spalten etc.)
    kommt aus project.yml → models.incremental.fct_events_py_incremental.ff.
    """
    # kleine Beispiel-Transformation: value * 10
    df = events_df.copy()
    df["value_x10"] = df["value"] * 10
    return df[["event_id", "updated_at", "value", "value_x10"]]
