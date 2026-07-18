# `coalesce()` and `repartition()`

> *Cross-chapter synthesis — Rioux (2022), Chapters 2, 3.*
>
> Spark's parallelism is controlled by partition count. `coalesce()` and `repartition()` are the two tools for changing that count. Getting them right is the difference between producing one clean output file and producing 200 tiny ones — or between a balanced shuffle and an OOM error.

---

## Ch 3 — One file per partition: `coalesce(1)`

Spark writes one output file per partition. A default job might produce 200 partitions → 200 small files. When you need a single output file (e.g., a CSV for a reporting tool), reduce to one partition before writing:

```python
df.coalesce(1).write.csv("output/report.csv", header=True)
```

`coalesce(n)` merges existing partitions without a shuffle — it pulls partitions together on the same executor. This is cheap for reducing partition count.

**Danger:** `coalesce(1)` funnels all data through a single task on a single executor. For large datasets this kills parallelism and can OOM the executor. Use it only for small output files.

---

## Ch 2 — `coalesce` vs `repartition`: the full comparison

| | `coalesce(n)` | `repartition(n)` |
|---|---|---|
| **Shuffle?** | No | Yes (full shuffle) |
| **Direction** | Reduce only (n < current) | Increase or decrease |
| **Data distribution** | Uneven (merges existing partitions as-is) | Even (re-hashes rows uniformly) |
| **Cost** | Cheap | Expensive |
| **Use case** | Final reduce before write; avoid shuffle | Rebalance skewed data; increase parallelism for downstream stages |

```python
# Cheap reduce for output
df.coalesce(4).write.parquet("output/")

# Rebalance skewed partitions
df.repartition(200)

# Repartition by column (places same-key rows on same partition — good before joins)
df.repartition(200, F.col("country"))
```

`repartition(n, col)` is useful before joins on high-cardinality keys — it co-locates matching rows, reducing shuffle during the join.

---

## Ch 2 — Tuning automatic partition sizing

Spark derives initial partition count for large reads from two config parameters:

```python
# Target size of each partition (default: 128 MB)
spark.conf.set("spark.sql.files.maxPartitionBytes", str(128 * 1024 * 1024))

# Overhead cost per file opened (default: 4 MB)
# Prevents creating tiny partitions for many small files
spark.conf.set("spark.sql.files.openCostInBytes", str(4 * 1024 * 1024))
```

For AQE (Adaptive Query Execution, on by default in Spark 3.2+), Spark can also coalesce shuffle partitions automatically during execution.

---

## Small-files decision guide

| Scenario | Action |
|---|---|
| Output must be a single file (small data) | `coalesce(1)` before write |
| Output should be a fixed number of files | `coalesce(n)` before write |
| Partitions are heavily skewed | `repartition(n)` to redistribute evenly |
| About to join on a specific key | `repartition(n, key_col)` to co-locate |
| Reading many small files (input) | Increase `openCostInBytes`; or use Delta `OPTIMIZE` |
| Writing Delta Lake | Use `optimizeWrite`; run `OPTIMIZE` periodically |

**Delta Lake options:**

```python
# Enable optimiseWrite — coalesces small files at write time (within each partition)
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")

# After writing, compact existing small files into larger ones
# spark.sql("OPTIMIZE my_table")

# Auto-compaction — runs OPTIMIZE automatically after writes
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
```

`optimizeWrite` and auto-compaction are Delta OSS features; `OPTIMIZE` is also available in Delta OSS 2.0+.

---

## Summary

- `coalesce(n)` merges partitions without a shuffle — cheap, can only reduce count, produces uneven distribution.
- `repartition(n)` performs a full shuffle — expensive, can increase or decrease, produces even distribution.
- `coalesce(1)` for single-file output, but only for small datasets.
- `repartition(n, col)` co-locates same-key rows — reduces shuffle cost for downstream joins.
- Tune `maxPartitionBytes` and `openCostInBytes` for large-file and small-file reads respectively.
- Delta's `optimizeWrite` / `OPTIMIZE` / auto-compaction are the production solution for small-files on Delta tables.

---

## Chapter links

- [Ch 2 — First data program](../books/rioux/chapters/02-first-data-program.md)
- [Ch 3 — Submitting and Scaling Your First PySpark Program](../books/rioux/chapters/03-submitting-scaling.md)
