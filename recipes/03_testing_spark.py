"""Recipe: Unit-testing Spark transformations.

The trick to fast, reliable Spark tests:
1. Keep transformations as pure functions: DataFrame in, DataFrame out. No I/O.
2. Share ONE local SparkSession across the test module (creating one per test
   is painfully slow).
3. Compare results as collected, sorted Python data — not by relying on row order.

This file is runnable with pytest:  pytest recipes/03_testing_spark.py
"""
from __future__ import annotations

import pytest
from pyspark.sql import DataFrame, SparkSession, functions as F


# ---- the transformation under test (pure: DataFrame -> DataFrame) ----

def add_full_name(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "full_name", F.concat_ws(" ", F.col("first"), F.col("last"))
    )


# ---- test fixtures & cases ----

@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[1]")          # single thread = fast & deterministic
        .appName("recipe-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_add_full_name(spark):
    df = spark.createDataFrame(
        [("Ada", "Lovelace"), ("Alan", "Turing")],
        ["first", "last"],
    )
    result = add_full_name(df)

    got = sorted(r["full_name"] for r in result.collect())
    assert got == ["Ada Lovelace", "Alan Turing"]


def test_add_full_name_keeps_row_count(spark):
    df = spark.createDataFrame([("A", "B"), ("C", "D")], ["first", "last"])
    assert add_full_name(df).count() == 2
