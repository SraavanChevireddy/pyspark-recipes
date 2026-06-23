"""Recipe: Explicit schemas and sane partitioning on write.

Two cheap habits that prevent the most common production Spark pain:

1. Never rely on schema inference for production reads. It triggers an extra
   pass over the data and silently changes types when the data changes. Declare
   the schema.

2. Control partitioning on write. Too many tiny files cripples the next reader;
   one giant file kills parallelism. Partition by a low-cardinality column you
   filter on, and coalesce to a reasonable file count.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Declare the schema explicitly — fast, safe, self-documenting.
EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), nullable=False),
    StructField("user_id", IntegerType(), nullable=False),
    StructField("event_type", StringType(), nullable=True),
    StructField("ts", TimestampType(), nullable=True),
])


def read_events(spark: SparkSession, path: str) -> DataFrame:
    return (
        spark.read
        .schema(EVENT_SCHEMA)      # no inference pass, no surprise types
        .json(path)
    )


def write_partitioned(df: DataFrame, path: str, target_files: int = 8) -> None:
    """Write Parquet partitioned by event_type, with a controlled file count.

    `partitionBy` enables partition pruning when readers filter on event_type.
    `coalesce` avoids the small-files problem without a full shuffle.
    """
    (
        df.coalesce(target_files)
        .write
        .mode("overwrite")
        .partitionBy("event_type")
        .parquet(path)
    )
