"""Recipe: Fixing data skew in joins with salting.

Symptom: one or two tasks run 100x longer than the rest, often on a join where
a few keys (nulls, a default tenant, a hot product) dominate. Spark hashes by
key, so all rows for a hot key land on one task.

Fix: "salt" the hot side by adding a random suffix to the key, and explode the
small side to match. The hot key's work spreads across `n` tasks.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F


def salted_join(
    spark: SparkSession,
    big: DataFrame,      # large, skewed table  (has column `key`)
    small: DataFrame,    # smaller table         (has column `key`)
    n_salts: int = 16,
) -> DataFrame:
    # 1. Add a random salt 0..n-1 to every row of the skewed side.
    big_salted = big.withColumn("_salt", (F.rand() * n_salts).cast("int"))

    # 2. Replicate each row of the small side once per salt value so the join
    #    keys still match.
    salts = spark.range(n_salts).withColumnRenamed("id", "_salt")
    small_exploded = small.crossJoin(salts)

    # 3. Join on (key, _salt) instead of key — work for a hot key now spreads
    #    across n_salts tasks instead of one.
    return (
        big_salted.join(small_exploded, on=["key", "_salt"], how="inner")
        .drop("_salt")
    )


# Note: on Spark 3.x, also try the built-in adaptive skew handling first:
#   spark.conf.set("spark.sql.adaptive.enabled", "true")
#   spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
# Salting is the manual fallback when AQE can't fully resolve it.
