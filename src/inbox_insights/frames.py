"""Small DataFrame helpers shared across layers."""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F


def keep_first(df: DataFrame, keys: list[str], *order_by: str | Column) -> DataFrame:
    """
    Keep one row per key: the first one by `order_by`.

    Unlike dropDuplicates, the row kept is deterministic, so reruns produce the
    same table.
    """
    window = Window.partitionBy(*keys).orderBy(*order_by)
    return (
        df.withColumn("_rank", F.row_number().over(window))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )


def save_table(df: DataFrame, full_name: str) -> None:
    """Overwrite a Unity Catalog table, allowing the schema to change between runs."""
    df.write.mode("overwrite").option("overwriteSchema", True).saveAsTable(full_name)
