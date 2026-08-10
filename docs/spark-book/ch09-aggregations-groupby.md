# Chapter 07 — Aggregations and GroupBy

> *Learning-path topic: B7 (Beginner)*
> *Written: 2026-05-31 · Spark 4.1.x / Python 3.10+*

!!! warning "🔄 Needs revisiting — B7 source trace (flagged 2026-07-18)"
    Incomplete rather than wrong: the API coverage holds, but the chapter teaches `groupBy().agg()` without the plan it produces, which is where all the cost lives.

    **One `Aggregate` becomes two physical operators.** Spark plans a `Partial` aggregate before the shuffle and a `Final` one after — map-side combine, and the reason `groupBy().count()` on a billion rows is cheap. It also explains why `HashAggregateExec` legitimately appears twice in an `EXPLAIN`, which otherwise reads as a bug.

    **`countDistinct` is a different plan shape.** One distinct aggregate expands to four stages; several are rewritten into an `Expand` that emits one row per distinct group per input row *before* aggregating. That is the actual cost model behind avoiding stacked `countDistinct`s, and the chapter presents it as just another function.

    **Three aggregate operators exist, selected by your functions rather than by config.** Adding a single `collect_list` makes the buffer non-mutable, switching from `HashAggregateExec` to `ObjectHashAggregateExec` — which falls back to sorting after 128 *groups* by default, a count rather than a memory size.

    Also missing: `spark.sql.shuffle.partitions` as the governing knob for any `groupBy`; the `numTasksFallBacked` UI metric for confirming a spill; that `pivot()` without an explicit value list runs a hidden distinct-collect job; that `rollup`/`cube`/`groupingSets` return the same builder type; aggregate pushdown into Parquet/ORC footers; and that `avg` and `mean` are the same registry entry. Full list in the [B7 source trace](../reference/spark-source-map/topics/b6.md).

Aggregation is where distributed computing earns its keep. Counting, summing, and averaging across millions of records is the core of analytical work, and `groupBy().agg()` is the pattern you will write hundreds of times.

---

## What you'll learn

- How `groupBy().agg()` works and what `GroupedData` is
- The most useful aggregate functions from `pyspark.sql.functions`
- How to compute multiple aggregations in a single pass
- Conditional aggregation with `F.when()`
- How nulls behave in aggregations

---

## The problem this solves

You have a table of 50 million sales records. You need the total revenue, number of transactions, and average order value per region per month — in one query. A Python loop would take hours. A pandas `groupby` would require the data to fit in RAM. Spark's distributed `groupBy().agg()` does this across all cores on a cluster in minutes.

---

## Core concept

`groupBy(*cols)` splits the DataFrame into groups — one `GroupedData` object per unique combination of the grouping columns. `GroupedData` is not a DataFrame; it is an intermediate that does nothing until you call an aggregation method on it.

The preferred aggregation method is `.agg()`, which accepts one or more column expressions:

```python
df.groupBy("region").agg(
    F.sum("revenue").alias("total_revenue"),
    F.count("*").alias("n_transactions"),
    F.avg("amount").alias("avg_amount"),
)
```

The alternative shortcut methods (`.count()`, `.sum("col")`, `.avg("col")`) each produce one aggregation. Use `.agg()` when you need more than one — it is more readable and executes in a single pass.

**Null behaviour:** aggregate functions ignore nulls by default. `F.sum("col")` sums only non-null values; `F.count("col")` counts only non-null values; `F.count("*")` counts all rows including those with nulls in any column. This distinction matters: `F.count("user_id")` ≠ `F.count("*")` when `user_id` has nulls.

**Spark 4.x ANSI mode:** `F.sum()` and `F.avg()` raise `SparkArithmeticException` on overflow. Use `F.try_sum()` and `F.try_avg()` (Spark 3.5+) for null-on-overflow behaviour.

---

## Examples

### Minimal example: groupBy and count

```python
# Apache Spark 4.1.x / PySpark 4.1.x · Python 3.10+
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ch06").master("local[*]").getOrCreate()

data = [("eng", 95000), ("eng", 87000), ("mkt", 72000), ("mkt", 68000), ("eng", 91000)]
df = spark.createDataFrame(data, ["dept", "salary"])

df.groupBy("dept").count().show()
# +----+-----+
# |dept|count|
# +----+-----+
# | eng|    3|
# | mkt|    2|
# +----+-----+
```

### Building up: multiple aggregations in one pass

```python
# Apache Spark 4.1.x / PySpark 4.1.x · Python 3.10+
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ch06-multi-agg").master("local[*]").getOrCreate()

data = [
    ("eng", "US", 95000, 1), ("eng", "UK", 87000, 1), ("eng", "US", 91000, 0),
    ("mkt", "US", 72000, 1), ("mkt", "UK", 68000, 0), ("mkt", "US", 71000, 1),
]
df = spark.createDataFrame(data, ["dept", "region", "salary", "is_senior"])

# Multiple aggregations in a single groupBy pass
result = df.groupBy("dept", "region").agg(
    F.count("*").alias("n"),
    F.avg("salary").alias("avg_salary"),
    F.max("salary").alias("max_salary"),
    F.sum("salary").alias("total_payroll"),
    # Conditional aggregation — count only senior employees
    F.sum(F.when(F.col("is_senior") == 1, 1).otherwise(0)).alias("n_senior"),
)
result.orderBy("dept", "region").show()
# +----+------+---+----------+----------+-------------+--------+
# |dept|region|  n|avg_salary|max_salary|total_payroll|n_senior|
# +----+------+---+----------+----------+-------------+--------+
# | eng|    UK|  1|   87000.0|     87000|        87000|       1|
# | eng|    US|  2|   93000.0|     95000|       186000|       1|
# | mkt|    UK|  1|   68000.0|     68000|        68000|       0|
# | mkt|    US|  2|   71500.0|     72000|       143000|       2|
# +----+------+---+----------+----------+-------------+--------+
```

### HAVING equivalent: filter after aggregation

```python
# Apache Spark 4.1.x / PySpark 4.1.x · Python 3.10+
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ch06-having").master("local[*]").getOrCreate()

df = spark.createDataFrame(
    [("eng", 95000), ("eng", 87000), ("eng", 91000), ("mkt", 72000)],
    ["dept", "salary"]
)

# SQL HAVING equivalent: filter after aggregation using .where()
(
    df.groupBy("dept")
    .agg(F.count("*").alias("n"), F.avg("salary").alias("avg_sal"))
    .where(F.col("n") >= 3)  # keep only departments with 3+ employees
    .show()
)
# +----+---+------------------+
# |dept|  n|           avg_sal|
# +----+---+------------------+
# | eng|  3|91000.0           |
# +----+---+------------------+
```

---

## Common pitfalls

- **Not aliasing aggregation results** — `F.sum("salary")` produces a column named `sum(salary)`. Referencing it by name later is fragile and ugly. Always `.alias()`.
- **`count("col")` vs `count("*")`** — `count("col")` skips nulls; `count("*")` counts all rows. Using the wrong one produces silently incorrect metrics. When measuring row count, use `count("*")`.
- **Chaining `.count()` on GroupedData when you need multiple aggs** — `.count()` returns a DataFrame and you lose the GroupedData. Call `.agg(F.count("*").alias("n"), F.sum(...))` instead to get everything in one shuffle.
- **`F.sum()` overflow in Spark 4.x** — summing a large integer column can overflow with ANSI mode on. Use `F.try_sum()` for null-on-overflow, or cast to `LongType` before summing.
- **Aggregating before joining** — aggregating a large table before joining it to a smaller one dramatically reduces shuffle cost. Doing it the other way (join large tables first, then aggregate) is a common performance mistake.

---

## Exercises

1. **Recall** — What does `F.count("salary")` return for a group where 3 of 5 rows have `salary = null`? What does `F.count("*")` return?

2. **Apply** — Create a sales DataFrame with `(product, region, amount, is_returned)`. Compute per-product: total sales, number of transactions, return rate (proportion of `is_returned == 1`), and average amount for non-returned sales only.

3. **Extend** — Compare the execution plan (`.explain()`) of two equivalent approaches: (1) `groupBy().agg(F.count("*"), F.sum("amount"))` in one call, and (2) two separate `.groupBy().agg()` calls joined on the key. Which produces fewer shuffles? Why?

---

## Summary

- `groupBy(*cols).agg(*exprs)` groups the DataFrame and applies aggregate functions in one distributed pass.
- Use `.agg()` with multiple expressions rather than chaining multiple `.groupBy()` calls — one shuffle, one pass.
- Always `.alias()` aggregate results — auto-generated names are fragile.
- `count("*")` counts all rows; `count("col")` counts non-null values — different answers for sparse columns.
- Conditional aggregation with `F.when()` inside `F.sum()` or `F.count()` replaces SQL `COUNT(CASE WHEN ...)`.
- Chapter 8 covers joins — the other half of the data transformation toolkit.

---

## References

- [PySpark groupBy API](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.groupBy.html)
- [PySpark aggregate functions](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions/aggregate_functions.html)
