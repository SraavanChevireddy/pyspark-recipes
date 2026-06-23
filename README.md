# pyspark-recipes

> A cookbook of **Apache Spark / PySpark patterns** for the problems that actually slow you down — data skew, window functions, testing Spark jobs, and schema/partitioning. Real, runnable code with the *why* explained.

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Spark](https://img.shields.io/badge/Apache%20Spark-3.x-E25A1C.svg)](https://spark.apache.org/)
[![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)](https://www.python.org/)

Spark's API is huge and most examples are toy `wordcount` snippets. These recipes target the things that bite real data pipelines — the skewed join that makes one task run forever, the dedup you keep rewriting, the Spark job no one tests. Each file is small, commented, and explains the trap it avoids.

## Recipes

| Recipe | What it covers |
|--------|----------------|
| [`01_handling_skew.py`](recipes/01_handling_skew.py) | Fix a skewed join with key salting (+ when to use AQE instead). |
| [`02_window_functions.py`](recipes/02_window_functions.py) | Running totals, top-N per group, and the idiomatic dedup. |
| [`03_testing_spark.py`](recipes/03_testing_spark.py) | Fast, deterministic unit tests for Spark transformations. |
| [`04_schema_and_partitioning.py`](recipes/04_schema_and_partitioning.py) | Explicit schemas and partitioned writes that don't create a million tiny files. |

## Setup

```bash
pip install "pyspark>=3.4" pytest
git clone https://github.com/SraavanChevireddy/pyspark-recipes.git
cd pyspark-recipes
```

## Run the tests

The testing recipe is fully runnable and demonstrates the recommended setup (one shared local SparkSession, deterministic single-partition shuffles):

```bash
pytest recipes/03_testing_spark.py -v
```

## Use the recipes

Each recipe exposes plain functions you can copy into your project and adapt — for example `deduplicate_keep_latest(df)` keeps the latest row per id. The functions take and return DataFrames, so they're easy to test and compose.

## The philosophy

- **Transformations are pure functions** — DataFrame in, DataFrame out, no I/O. This is what makes Spark code testable.
- **Explain the trap, not just the API.** Read the docstring at the top of each file.
- **Prefer built-ins (AQE, window functions) before manual tricks** — and the recipes say when each applies.

## Contributing

Have a pattern you keep re-deriving (broadcast join tuning, structured streaming, Delta merge/upsert)? PRs welcome — keep each recipe self-contained and commented.

## License

[MIT](LICENSE) © Sraavan Chevireddy
