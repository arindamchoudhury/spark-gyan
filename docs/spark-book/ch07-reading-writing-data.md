# Chapter 05 — Reading and Writing Data

> *Learning-path topic: B4 (Beginner)*
> *Written: 2026-05-31 · Spark 4.1.x / Python 3.10+*

> 🔄 **Needs revisiting — Spark 4.2.0 source trace (flagged 2026-07-18).** Incomplete rather than wrong: nothing here is false, but the trace opened eight gaps, two of which matter enough to fix before relying on this chapter.
>
> **`insertInto` matches columns by position, not name.** Neither `insertInto` nor `saveAsTable` appears in this chapter, and the difference is the highest-consequence one in the writer API — a DataFrame with correct column *names* in the wrong order writes silently corrupted data through `insertInto`.
>
> **The read-parallelism formula is missing.** `maxSplitBytes = min(maxPartitionBytes, max(openCostInBytes, totalBytes / cores))` is what decides how many tasks a read produces; neither config is mentioned, which leaves "why did my read only get two tasks" unanswerable and the small-files problem unexplained.
>
> Also missing: `maxRecordsPerFile` as the output-file-size lever, *why* inference is costly (it reads the data twice), ORC defaulting to `zstd` while Parquet uses `snappy`, `ignoreCorruptFiles`/`ignoreMissingFiles`, and that writing JSON drops null fields by default. Full list in the [B4 source trace](../reference/spark-source-map/topics/b4.md).

Every Spark pipeline starts with a read and ends with a write. The format you choose and how you configure the reader/writer determines whether your pipeline reads 100% of the data or 5% of it — before any transformation runs.

---

## What you'll learn

- How to read CSV, JSON, Parquet, and Delta with `spark.read`
- The role of schema declaration vs. inference
- How to write data with `df.write` and what write modes mean
- Why Parquet produces many output files and how to control it
- How column pruning and predicate pushdown reduce I/O automatically

---

## The problem this solves

You inherit a pipeline that reads a CSV file with `inferSchema=True` and takes 10 minutes just to start. Or you write a result to Parquet and find 200 tiny files in the output directory. Or a downstream job fails because a column type changed silently when the schema was inferred. All of these are read/write configuration problems with clear solutions.

---

## Core concept

Spark's I/O layer is built around two objects:

- **`spark.read`** returns a `DataFrameReader` — a builder you configure before loading data
- **`df.write`** returns a `DataFrameWriter` — a builder you configure before saving data

Both use a fluent pattern: chain options, then call the terminal method (`csv()`, `parquet()`, `save()`, etc.).

The format choice matters enormously. CSV and JSON are row-oriented — reading any column requires reading every byte of every row. Parquet and Delta are columnar with statistics in file footers, enabling two optimisations that run automatically:

**Column pruning** — if your query selects 3 of 50 columns, Spark reads only those 3 column chunks from disk. The other 47 are never loaded into memory.

**Predicate pushdown** — if you filter on a column (e.g. `year == 2024`), Spark reads the Parquet footer statistics (min/max per row group) and skips any row group where year cannot be 2024. For date-partitioned data this can skip 90%+ of files before any data is read.

Both optimisations are automatic — you get them for free by using Parquet and keeping your schema explicit.

---

## Examples

### Minimal example: read CSV, write Parquet

```python
# Apache Spark 4.1.x / PySpark 4.1.x · Python 3.10+
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ch04").master("local[*]").getOrCreate()

# Read — declare schema explicitly; skip inferSchema in production
schema = T.StructType([
    T.StructField("station", T.StringType(),  nullable=False),
    T.StructField("date",    T.StringType(),  nullable=False),
    T.StructField("temp",    T.DoubleType(),  nullable=True),
])

df = spark.read.csv("data/weather.csv", schema=schema, header=True)
df.printSchema()
# root
#  |-- station: string (nullable = false)
#  |-- date: string (nullable = false)
#  |-- temp: double (nullable = true)

# Write to Parquet — overwrite if it exists
df.write.mode("overwrite").parquet("data/weather_parquet/")
```

### Building up: all the read options

```python
# Apache Spark 4.1.x / PySpark 4.1.x · Python 3.10+
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ch04-formats").master("local[*]").getOrCreate()

# CSV — common options
csv_df = spark.read.csv(
    "data/logs.csv",
    sep="|",
    header=True,
    schema="stn STRING, ts TIMESTAMP, event STRING",  # DDL string schema — more concise
    timestampFormat="yyyy-MM-dd HH:mm:ss",
    nullValue="N/A",
)

# JSON (JSON Lines — one JSON object per line)
json_df = spark.read.json("data/events.jsonl")

# Parquet — schema embedded in file, no declaration needed
parquet_df = spark.read.parquet("data/events_parquet/")

# Delta — same API as Parquet
delta_df = spark.read.format("delta").load("data/events_delta/")

# Verify predicate pushdown is firing
filtered = parquet_df.filter(F.col("year") == 2024).select("station", "temp")
filtered.explain()
# Look for PushedFilters and ReadSchema in the physical plan
```

### Write modes and partition control

```python
# Apache Spark 4.1.x / PySpark 4.1.x · Python 3.10+
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ch04-write").master("local[*]").getOrCreate()

df = spark.range(1000).withColumn("year", F.lit(2024))

# Four write modes
# overwrite — replace entire destination
df.write.mode("overwrite").parquet("out/overwrite/")

# append — add to existing data
df.write.mode("append").parquet("out/append/")

# ignore — no-op if destination already exists
df.write.mode("ignore").parquet("out/ignore/")

# error (default) — raises error if destination exists
df.write.parquet("out/new/")

# Partition the output by year — creates subdirectory year=2024/
df.write.mode("overwrite").partitionBy("year").parquet("out/partitioned/")

# Coalesce to reduce output file count — no shuffle
df.coalesce(1).write.mode("overwrite").parquet("out/single/")
```

---

## Common pitfalls

- **`inferSchema=True` in production** — it reads the entire dataset twice: once to infer types, once to load. On a 10 GB CSV this doubles I/O. Worse, it can infer wrong types for sparse columns (a mostly-null integer column may infer as `string`). Always declare schemas in pipelines.
- **CSV is not the right format for repeated reads** — CSV has no column statistics, no column pruning, and no compression by default. Converting to Parquet once and reading Parquet repeatedly is almost always faster for analytical workloads.
- **Spark writes one file per partition** — `df.write.csv("out/")` creates a directory with 200 part files (the default shuffle partition count). Use `coalesce(N)` before writing to reduce file count. Use `repartition(N)` for evenly-sized files (at the cost of a shuffle).
- **`mode("overwrite")` on partitioned writes replaces the whole table** — if you partition by year and overwrite with only 2024 data, 2022 and 2023 directories are deleted. Use `spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")` to replace only the partitions present in the new data.
- **Delta vs plain Parquet for mutable data** — plain Parquet has no update or delete support. If rows change over time, use Delta (which wraps Parquet with a transaction log). For append-only immutable data, plain Parquet is fine.

---

## Exercises

1. **Recall** — What is the difference between column pruning and predicate pushdown? Which one reduces the number of rows read, and which reduces the number of columns?

2. **Apply** — Read a CSV file with `inferSchema=True`, then read the same file with an explicit `StructType` schema. Compare the time of each read. Verify the types with `printSchema()` — are they identical?

3. **Extend** — Write a DataFrame to Parquet with `partitionBy("year", "month")`. Inspect the directory structure. Then read back only one partition using a filter on `year` and verify with `explain()` that Spark is using partition pruning (look for `PartitionFilters` in the physical plan).

---

❓ **To cover — schema inference, driver-side analysis, and AnalysisException:**

- `spark.read.csv()` without an explicit `.schema(...)` runs a data scan in the driver to determine column types — this is eager, not lazy. Always pass a schema explicitly in production.
- Spark resolves column names and validates types in the driver as soon as something forces plan inspection (accessing `.schema`, `.dtypes`, `.explain()`, or calling an action). This is why `AnalysisException` can surface before an action fires.
- What triggers analysis eagerly vs what keeps it deferred; treat `AnalysisException` as a compile error — fix the schema or column reference, don't catch and retry.
- In Spark Connect (opt-in in 4.x), analysis always runs server-side; `AnalysisException` arrives as an RPC error from either an `AnalyzePlan` or `ExecutePlan` call.

---

## Summary

- `spark.read` and `df.write` are fluent builders — chain options, then call the terminal format method.
- Always declare schema explicitly in production; `inferSchema=True` is for exploration only.
- Parquet enables column pruning and predicate pushdown automatically — use it for repeated reads.
- `df.write` creates one file per partition; use `coalesce(N)` to reduce file count without a shuffle.
- Write modes: `overwrite`, `append`, `ignore`, `error` (default).
- Chapter 6 builds on reading by introducing schema definition with `StructType`.

---

## References

- [PySpark data sources](https://spark.apache.org/docs/latest/sql-data-sources.html)
- [Parquet format](https://parquet.apache.org/)
- [Delta Lake quickstart](https://docs.delta.io/latest/quick-start.html)
