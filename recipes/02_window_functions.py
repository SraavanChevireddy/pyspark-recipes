"""Recipe: Window functions — running totals, ranking, and deduplication.

Window functions compute across a set of rows *related to the current row*
without collapsing them like groupBy does. Three patterns you'll reach for
constantly.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window


def running_total(df: DataFrame) -> DataFrame:
    """Cumulative sum of `amount` per `account`, ordered by `ts`."""
    w = (
        Window.partitionBy("account")
        .orderBy("ts")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    return df.withColumn("running_total", F.sum("amount").over(w))


def top_n_per_group(df: DataFrame, n: int = 3) -> DataFrame:
    """Keep the top `n` rows by `score` within each `category`."""
    w = Window.partitionBy("category").orderBy(F.col("score").desc())
    return (
        df.withColumn("rank", F.row_number().over(w))
        .filter(F.col("rank") <= n)
        .drop("rank")
    )


def deduplicate_keep_latest(df: DataFrame) -> DataFrame:
    """Keep only the most recent row per `id` (by `updated_at`).

    This is the idiomatic Spark dedup — far cheaper than a self-join.
    """
    w = Window.partitionBy("id").orderBy(F.col("updated_at").desc())
    return (
        df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
