from fastflowtransform import engine_model
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F


@engine_model(
    only="databricks_spark",
    name="fct_events_py_incremental",
    deps=["events_base.ff"],
    tags=[
        "example:incremental_demo",
        "scope:engine",
        "engine:databricks_spark",
        "kind:python",
        "kind:incremental",
    ],
)
def build(events_df: SparkDataFrame) -> SparkDataFrame:
    """
    Python-Incremental-Beispiel (Databricks Spark).

    Auch hier:
      - Build-Snapshot im Python-Model
      - Merge/Delta wird über Konfiguration gesteuert.
    """
    return events_df.withColumn("value_x10", F.col("value") * F.lit(10)).select(
        "event_id", "updated_at", "value", "value_x10"
    )
