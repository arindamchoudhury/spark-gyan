# Chapter 14 — Partitioning: Concepts and Control

> *Learning-path topic: I5 (Intermediate)*
> *Written: 2026-05-31 · Spark 4.1.x / Python 3.10+*

!!! warning "🔄 Needs revisiting — I5 source trace (flagged 2026-07-18)"
    Ten gaps. One is a correction rather than an omission, and it concerns the chapter's central comparison.

    **"`coalesce` avoids a shuffle" is true and, left there, misleading.** Because `CoalescedRDD` is a narrow dependency there is no stage boundary, so the upstream computation runs with the coalesced task count. `coalesce(1)` before a write does not merely produce one file — it makes every transformation in that stage single-threaded. `repartition(1)` inserts a shuffle and is frequently much faster, because the expensive upstream work keeps its parallelism. A chapter whose stated purpose is fixing "my job is slow" and "my job wrote 10,000 tiny files" needs this stated as prominently as the shuffle-avoidance itself.

    **A bare `repartition(n)` performs a hidden local sort, for correctness.** SPARK-23207: round-robin assignment must be deterministic or a retried task loses rows. `spark.sql.execution.sortBeforeRepartition` (default on) buys that guarantee, and it is why `repartition` costs more than expected.

    **AQE has the last word on partition count.** `CoalesceShufflePartitions` merges post-shuffle partitions toward an advisory size with a 1MB floor, so `spark.sql.shuffle.partitions` is a starting point rather than the answer. `RebalancePartitions` / the `REBALANCE` hint removes the need to guess a number at all — the direct fix for the tiny-files problem.

    Also missing: `repartition` and `coalesce` being one logical node with a boolean (mirroring Ch05's RDD-level relationship); `CoalesceExec` advertising `UnknownPartitioning`, so coalescing before a keyed operation saves nothing; partitionings being negotiated by `EnsureRequirements` rather than commanded, which makes a manual `repartition` before a `groupBy` usually redundant; `RangePartitioner` sampling the data in a preliminary job; and `spark.default.parallelism` not controlling DataFrame shuffles. Full list in the [I5 source trace](../reference/spark-source-map/topics/i5.md).

Partitioning is how Spark divides data across executor memory. Get it wrong and your jobs are either too slow (too many tiny tasks), too memory-hungry (too few large ones), or produce thousands of useless tiny output files. Get it right and the same job runs 10× faster.

!!! note "📌 Topics deferred here from Chapter 1"
    Chapter 1 introduces partitions as the unit of parallelism (one partition = one task). The following are covered in full here:

    - **Partition count vs scheduling overhead tradeoff** — more partitions means more parallelism but also more task serialization, launch, and GC overhead; the right count depends on data volume and executor cores
    - **`spark.sql.shuffle.partitions`** — the default 200 shuffle output partitions and when to change it
    - **`repartition()` vs `coalesce()`** — when each triggers a shuffle and when to avoid it
    - **Partition-aware writes** — how `partitionBy()` on `DataFrameWriter` creates directory-based partitions read by Spark and other engines

---

## What you'll learn

- What a physical partition is and how it relates to tasks and files
- The difference between `repartition()` and `coalesce()` and when to use each
- How `partitionBy()` on writes creates directory-based partitions
- How `spark.sql.shuffle.partitions` affects post-shuffle performance
- How AQE (Adaptive Query Execution) handles partitioning automatically in Spark 3.0+

---

## The problem this solves

You write a 1 GB result to CSV and find 200 empty files in the output directory. Or your job finishes all stages in 30 seconds except one stage that takes 10 minutes — and the Spark UI shows 200 tasks where 198 finish instantly but 2 take forever (data skew). Both are partitioning problems with straightforward fixes.

---

## Core concept

A **partition** is a contiguous chunk of a DataFrame stored in one executor's memory. Each partition becomes one **task** in a stage. The number of partitions therefore controls the maximum parallelism for that stage.

Two kinds of partitioning, with different causes and controls:

**1. Input partitions** — determined by the data source at read time. For files, Spark creates one partition per file chunk based on `spark.sql.files.maxPartitionBytes` (default 128 MB). Many tiny files → many tiny partitions → many tiny tasks → scheduling overhead dominates.

**2. Shuffle partitions** — created after a shuffle operation (groupBy, join, repartition). Controlled by `spark.sql.shuffle.partitions` (default 200). On a laptop with 8 cores processing a small dataset, 200 shuffle tasks is wasteful. On a 100-node cluster with 10 TB of data, 200 may be far too few.

**The two repartition tools:**

| | `repartition(n)` | `coalesce(n)` |
|---|---|---|
| Shuffle? | Yes — full shuffle | No — merges existing partitions |
| Result | Evenly distributed partitions | Uneven (inherits skew from input) |
| Use when | Increasing partitions, or rebalancing skewed data | Reducing partitions before write |
| Speed | Slower (shuffle cost) | Faster (no shuffle) |

**Write-time directory partitioning (`partitionBy`)** is a different concept: it organises output files into subdirectories by column value (`year=2024/month=01/`) to enable Spark to skip directories when filtering. This is not the same as controlling the number of partitions in memory.

---

## Examples

### Minimal example: inspect and change partition count

```python
# Apache Spark 4.1.x / PySpark 4.1.x · Python 3.10+
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ch14").master("local[*]").getOrCreate()

df = spark.range(1_000_000)

print(df.rdd.getNumPartitions())   # default — depends on local cores, e.g. 8

# repartition — full shuffle; evenly distributes data
df_repart = df.repartition(16)
print(df_repart.rdd.getNumPartitions())   # 16

# coalesce — no shuffle; merges partitions without redistributing
df_coalesced = df_repart.coalesce(4)
print(df_coalesced.rdd.getNumPartitions())   # 4

# repartition by a column — routes rows with same key to same partition
df.repartition(8, F.col("id") % 4)   # 8 partitions, grouped by id%4
```

### Building up: shuffle partitions and the 200-files problem

```python
# Apache Spark 4.1.x / PySpark 4.1.x · Python 3.10+
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ch14-shuffle").master("local[*]").getOrCreate()

# Set shuffle partitions to match local cores (not the 200 default)
spark.conf.set("spark.sql.shuffle.partitions", "8")

df = spark.range(10_000).withColumn("dept", (F.col("id") % 5).cast("string"))

# groupBy triggers a shuffle — creates spark.sql.shuffle.partitions partitions
result = df.groupBy("dept").count()
print(result.rdd.getNumPartitions())   # 8, not 200

# Write to Parquet — one file per partition = 8 files (not 200)
result.write.mode("overwrite").parquet("out/dept_counts/")

# If you need exactly 1 output file:
result.coalesce(1).write.mode("overwrite").parquet("out/dept_counts_single/")
```

### Directory partitioning for efficient reads

```python
# Apache Spark 4.1.x / PySpark 4.1.x · Python 3.10+
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ch14-partby").master("local[*]").getOrCreate()

data = [(2023, 1, "stA", 5.2), (2023, 2, "stA", 8.1), (2024, 1, "stB", 3.4)]
df = spark.createDataFrame(data, ["year", "month", "stn", "temp"])

# Write with directory partitioning by year and month
df.write.mode("overwrite").partitionBy("year", "month").parquet("out/weather/")
# Creates: out/weather/year=2023/month=1/*.parquet
#          out/weather/year=2023/month=2/*.parquet
#          out/weather/year=2024/month=1/*.parquet

# Read back with partition filter — Spark skips non-matching directories entirely
filtered = spark.read.parquet("out/weather/").filter(F.col("year") == 2024)
filtered.explain()
# Physical plan shows PartitionFilters: [isnotnull(year), (year = 2024)]
# Only year=2024/ directory is read
```

---

## Common pitfalls

- **200 shuffle partitions on a small local job** — `spark.sql.shuffle.partitions=200` is designed for a large cluster. On a laptop or small dataset, it creates 200 nearly-empty tasks with high scheduling overhead. Set it to `2 × local cores` (e.g. 8–16) for local development.
- **`coalesce(1)` in production pipelines** — reduces everything to one partition on one executor, eliminating all parallelism. Use it only immediately before a final write; never in the middle of a pipeline.
- **Over-partitioning with `partitionBy()`** — writing with `partitionBy("user_id")` on a column with 10 million distinct values creates 10 million tiny directories. Delta's rule of thumb: only partition by a column where each partition will be at least 1 GB.
- **Repartition after filter** — filtering reduces the number of rows but not the number of partitions. After a large `filter()`, you may have 200 partitions where 195 are nearly empty. `coalesce(N)` after a filter removes the overhead.
- **Confusing in-memory partitions with directory partitions** — `repartition()` controls how data is split in executor memory (affects task parallelism). `partitionBy()` on `df.write` controls how output files are organised on disk (affects read-time skipping). These are independent settings.

---

## Exercises

1. **Recall** — What is the difference between `repartition(4)` and `coalesce(4)` in terms of what happens to the data? When would you prefer one over the other?

2. **Apply** — Create a DataFrame of 1 million rows. Perform a `groupBy().count()`. Check `spark.sql.shuffle.partitions` and count the output partitions. Then set the value to 8, re-run, and count again. Verify the output by checking the number of files in a Parquet write.

3. **Extend** — Write a DataFrame partitioned by year and month (`partitionBy("year", "month")`). Then read it back with three different filters: (1) no filter, (2) `year == 2024`, (3) `year == 2024 AND month == 3`. Use `explain()` on all three reads and compare the `PartitionFilters` section. What does this reveal about partition pruning?

---

## Summary

- Partitions are chunks of data in executor memory; each partition is one task. Partition count controls parallelism.
- `repartition(n)` triggers a full shuffle and produces evenly distributed partitions. Use when increasing partitions or fixing skew.
- `coalesce(n)` merges partitions without shuffling — faster but may produce uneven sizes. Use to reduce partitions before writing.
- `spark.sql.shuffle.partitions` (default 200) controls post-shuffle partition count. Set to `2 × cores` for local work.
- `partitionBy("col")` on writes creates directory-based partitioning for read-time skipping — different from in-memory partitioning.
- AQE (Spark 3.0+, on by default in 4.x) automatically coalesces shuffle partitions based on runtime data size — reduces the need for manual tuning.
- Chapter 15 covers caching and persistence — how to store intermediate DataFrames to avoid recomputation.

---

## References

- [Spark performance tuning — partitioning](https://spark.apache.org/docs/latest/sql-performance-tuning.html)
- [PySpark repartition API](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.repartition.html)
- [Spark AQE documentation](https://spark.apache.org/docs/latest/sql-performance-tuning.html#adaptive-query-execution)
