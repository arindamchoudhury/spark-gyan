# Chapter 12 — Window Functions

> *Learning-path topic: I8 (Intermediate)*
> *Written: 2026-05-31 · Spark 4.1.x / Python 3.10+*

!!! warning "🔄 Needs revisiting — I8 source trace (flagged 2026-07-18)"
    Incomplete rather than wrong. Nine gaps; two are correctness issues rather than missing detail.

    **The default frame changes when you add `orderBy`, including its type.** No ordering gives `ROWS UNBOUNDED PRECEDING TO UNBOUNDED FOLLOWING` — the whole partition. An ordering gives `RANGE UNBOUNDED PRECEDING TO CURRENT ROW` — a running value. So the same aggregate over the same window computes different things depending on a clause that says nothing about frames, and the type flip to `RANGE` means rows tied on the ordering column all enter together. This is the most consequential fact in the topic and it is six lines of `WindowResolution.resolveFrame`.

    **Omitting `partitionBy` moves the entire dataset to one partition.** `requiredChildDistribution` returns `AllTuples` and Spark only logs a warning, so it works on sample data and fails at scale. Every window is also a shuffle *plus* a sort, which is the cost model the chapter does not state.

    Also missing: which of the five frame implementations runs (unbounded computes once, sliding adds and removes per row, `lag`/`lead` buffer nothing) — which is what makes "narrow your frame" actionable; that top-N per group is pushed below the shuffle by `InferWindowGroupLimit` but only for recognised forms with *n* ≤ 1000; that window buffers spill with their own `windowExec.buffer.*` thresholds; that functions with a required frame reject a conflicting explicit one; and the opt-in segment-tree evaluation for sliding frames. Full list in the [I8 source trace](../reference/spark-source-map/topics/i2.md).

Window functions compute an aggregate or rank across a group of rows — but unlike `groupBy`, they keep every original row and add the result as a new column. They are the most powerful single-pass transformation in PySpark for time series, rankings, and running totals.

---

## What you'll learn

- How window functions differ from `groupBy` — same groups, full row count preserved
- How to build a `WindowSpec` with `partitionBy`, `orderBy`, and frame boundaries
- The three function families: aggregate, ranking, and analytic
- The default frame surprise: ordered vs unordered windows behave differently
- How to use row and range frame boundaries for sliding windows

---

## The problem this solves

You need the minimum temperature per station per year alongside the original daily records — but `groupBy().agg()` collapses everything to one row per group. Alternatively, you need to rank stations by temperature within each year, or compute yesterday's temperature as a new column. All three require window functions: group-aware computation that preserves the original DataFrame shape.

---

## Core concept

A window function applies over a *window partition* — a logical group of rows defined by `partitionBy()` — and broadcasts the result back to every row in that partition. The row count never changes.

```
DataFrame (millions of rows)
    ├─ Window partition (stn=A, year=2024): F.min("temp") = -5.2 → appended to every row in this partition
    ├─ Window partition (stn=B, year=2024): F.min("temp") = 12.1 → appended to every row in this partition
    └─ ... union all → original DataFrame + new min_temp column
```

Build a `WindowSpec` with the `Window` builder:

```python
from pyspark.sql.window import Window

w = Window.partitionBy("stn", "year").orderBy("date")
```

Apply it with `.over(w)` on any column expression:

```python
df.withColumn("min_temp", F.min("temp").over(w))
```

**The three function families:**

| Family | Examples | Requires orderBy? |
|---|---|---|
| Aggregate | `F.min`, `F.max`, `F.avg`, `F.sum`, `F.count` | No (but affects frame — see below) |
| Ranking | `F.rank`, `F.dense_rank`, `F.percent_rank`, `F.ntile`, `F.row_number` | Yes |
| Analytic | `F.lag`, `F.lead`, `F.cume_dist` | Yes |

**The ordering surprise.** Adding `.orderBy()` to a window spec changes the default frame:
- Unordered window → frame = whole partition (every record gets the same aggregate value)
- Ordered window → frame = rows from start of partition up to current row (running aggregate — each record sees only past values)

This is why `F.avg("temp").over(Window.partitionBy("year"))` and `F.avg("temp").over(Window.partitionBy("year").orderBy("date"))` return different values even though the partition is the same.

---

## Examples

### Minimal example: aggregate over window vs self-join

```python
# Apache Spark 4.1.x / PySpark 4.1.x · Python 3.10+
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("ch11").master("local[*]").getOrCreate()

data = [
    ("stA", 2024, "01", -2.1), ("stA", 2024, "02",  3.5), ("stA", 2024, "03",  8.1),
    ("stB", 2024, "01",  5.2), ("stB", 2024, "02",  9.8), ("stB", 2024, "03", 12.0),
]
df = spark.createDataFrame(data, ["stn", "year", "mo", "temp"])

# Window: minimum temperature per station per year, appended to every row
each_station_year = Window.partitionBy("stn", "year")

result = df.withColumn("min_temp", F.min("temp").over(each_station_year))
result.show()
# +---+----+---+-----+--------+
# |stn|year| mo| temp|min_temp|
# +---+----+---+-----+--------+
# |stA|2024| 01| -2.1|    -2.1|
# |stA|2024| 02|  3.5|    -2.1|
# |stA|2024| 03|  8.1|    -2.1|
# |stB|2024| 01|  5.2|     5.2|
# |stB|2024| 02|  9.8|     5.2|
# |stB|2024| 03| 12.0|     5.2|
# +---+----+---+-----+--------+
```

### Building up: ranking and analytic functions

```python
# Apache Spark 4.1.x / PySpark 4.1.x · Python 3.10+
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("ch11-ranking").master("local[*]").getOrCreate()

data = [
    ("stA", 2024, -2.1), ("stA", 2024,  3.5), ("stA", 2024,  8.1),
    ("stB", 2024,  5.2), ("stB", 2024,  9.8), ("stB", 2024, 12.0),
]
df = spark.createDataFrame(data, ["stn", "year", "temp"])

# Order within each station-year by temperature ascending
w_ordered = Window.partitionBy("stn", "year").orderBy("temp")

df.withColumns({
    "rank":         F.rank().over(w_ordered),         # 1,2,3 with gaps for ties
    "dense_rank":   F.dense_rank().over(w_ordered),   # 1,2,3 no gaps
    "row_number":   F.row_number().over(w_ordered),   # strict 1,2,3 regardless of ties
    "prev_temp":    F.lag("temp").over(w_ordered),    # temperature of previous row
    "next_temp":    F.lead("temp").over(w_ordered),   # temperature of next row
}).orderBy("stn", "temp").show()
# +---+----+-----+----+----------+----------+---------+---------+
# |stn|year| temp|rank|dense_rank|row_number|prev_temp|next_temp|
# +---+----+-----+----+----------+----------+---------+---------+
# |stA|2024| -2.1|   1|         1|         1|     null|      3.5|
# |stA|2024|  3.5|   2|         2|         2|     -2.1|      8.1|
# |stA|2024|  8.1|   3|         3|         3|      3.5|     null|
```

### Frame boundaries: sliding window

```python
# Apache Spark 4.1.x / PySpark 4.1.x · Python 3.10+
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("ch11-frames").master("local[*]").getOrCreate()

data = [(f"2024-{m:02d}-01", float(m * 5)) for m in range(1, 8)]
df = spark.createDataFrame(data, ["date", "value"]).withColumn(
    "date_num", F.unix_timestamp("date")
)

# Ordered window — default frame is RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
w_ordered = Window.partitionBy(F.lit(1)).orderBy("date_num")

# Explicit 3-row trailing window (current + 2 preceding rows)
w_trailing3 = Window.partitionBy(F.lit(1)).orderBy("date_num").rowsBetween(-2, 0)

# 30-day range window (value-based, not position-based)
ONE_DAY = 60 * 60 * 24
w_30day = (
    Window.partitionBy(F.lit(1))
    .orderBy("date_num")
    .rangeBetween(-30 * ONE_DAY, 0)
)

df.withColumns({
    "running_avg":  F.avg("value").over(w_ordered),   # grows with each row
    "trailing3":    F.avg("value").over(w_trailing3), # always 3 rows (or fewer at start)
    "rolling_30d":  F.avg("value").over(w_30day),     # value-range based
}).select("date", "value", "running_avg", "trailing3").show()
```

---

## Common pitfalls

- **Ordered aggregate window produces a running aggregate, not a partition-wide aggregate** — adding `.orderBy()` to a window used for `F.avg()` or `F.sum()` changes the default frame to "up to current row". This surprises most beginners. If you want the whole-partition value, use an unordered window.
- **`Window.orderBy()` has no `ascending` parameter** — unlike DataFrame `.orderBy()`, the Window method does not accept `ascending=False`. Use `F.col("col").desc()` instead.
- **Using `groupby()` or `where()` directly with `.over()` raises `AnalysisException`** — materialise the window column with `withColumn()` or `select()` first, then filter or group on the result.
- **`maxRecordsPerBatch` does not apply to grouped data windows** — the entire partition arrives at once. If one partition has 50 million rows, that partition must fit in executor memory.
- **`Window.unboundedPreceding` / `Window.unboundedFollowing`** — always use these named constants instead of raw numbers for the first/last record in a window. Raw large integers (e.g. `sys.maxsize`) are fragile; the named constants are translated correctly by Spark.

---

## Exercises

1. **Recall** — What is the default frame for `F.avg("temp").over(Window.partitionBy("stn").orderBy("date"))`? What value does the first row in each partition return?

2. **Apply** — Using the weather dataset, compute: (1) minimum temperature per station-year appended to each row, (2) rank of each daily temperature within its station-year from coldest to warmest, (3) the previous day's temperature using `F.lag`.

3. **Extend** — Implement a 7-day rolling average using `rowsBetween(-6, 0)`. Then implement the same logic using `rangeBetween` over a unix timestamp column. Compare the results on a dataset where some days have no readings (gaps). Which approach handles gaps correctly and which doesn't?

---

## Summary

- Window functions compute across groups but preserve every original row — no collapse, unlike `groupBy`.
- Build a `WindowSpec` with `Window.partitionBy().orderBy()`. Apply with `F.function().over(window_spec)`.
- Unordered window: whole partition is the frame. Ordered window: growing frame (start to current row) by default.
- Three families: aggregate (`min/max/avg/sum`), ranking (`rank/dense_rank/row_number/ntile`), analytic (`lag/lead/cume_dist`).
- Use `rowsBetween` for position-based sliding windows; `rangeBetween` for value-based (time/date) windows.
- Chapter 13 covers UDFs — how to apply custom Python logic as a column transformation.

---

## References

- [PySpark Window API](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Window.html)
- [PySpark window functions guide](https://spark.apache.org/docs/latest/api/python/user_guide/sql/window.html)
