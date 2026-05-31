# Chapter 10 — Your Data Under a Different Lens: Window Functions

> *Source: Rioux (2022), Chapter 10, pages 215–243.*
>
> Window functions apply a computation over a *window* of records without collapsing the data frame — every record gains a new computed column while the row count stays the same. They fill the niche between `groupBy().agg()` (many rows → one row) and `groupBy().applyInPandas()` (many rows → any shape): a window function always preserves the original shape.
>
> 📌 **Notes adapted to Spark 4.1.1 / PySpark 4.1.1.** The book targets Spark 3.2. The Window API has been stable since Spark 1.4 — no API changes in Spark 4.x. One Spark Connect improvement: `WindowSpec` gained Spark Connect support in 3.4.0. Notes on the `PandasUDFType.GROUPED_AGG` decorator used in §10.4: that is the deprecated Spark 2.x syntax — use type hints instead (Spark 3.0+).

---

## 1. What window functions are

A window function applies an operation to a *window partition* — a logical subset of the data frame defined by column values — and broadcasts the result back to every record in that partition. The data frame retains all its original rows.

| Operation | Input → Output | Row count |
|---|---|---|
| `groupBy().agg()` | N rows → 1 row per group | Shrinks |
| `groupBy().applyInPandas()` | N rows → M rows (any shape) | Changes |
| Window function | N rows → N rows + new column(s) | **Unchanged** |

The three stages of a window function map onto split-apply-combine terminology from SQL:

1. **Partition** — the data frame is split according to one or more column values (like `groupBy`).
2. **Apply** — the function runs over the window; the result is broadcast to each record in the partition.
3. **Combine** — implicitly: Spark unions the partitions back.

> 💡 **Vocabulary collision** — Spark has always used *partition* for physical data splits across executors. Window functions borrow the same word for *logical* row groups within one operation. The book calls these *window partitions* to distinguish them; in practice, context makes it clear.

---

## 2. The WindowSpec and `.over()`

### Import

```python
from pyspark.sql.window import Window
import pyspark.sql.functions as F
```

### Building a WindowSpec

`Window` is a builder class — chain methods to parameterise it:

```python
each_year = Window.partitionBy("year")
# <pyspark.sql.window.WindowSpec object>
```

`WindowSpec` is just a blueprint — no computation happens until it is applied.

### Applying: `.over(window_spec)`

```python
(gsod
 .withColumn("min_temp", F.min("temp").over(each_year))
 .where("temp = min_temp")
 .select("year", "mo", "da", "stn", "temp")
 .orderBy("year", "mo", "da")
 .show())
```

`F.min("temp").over(each_year)` computes the minimum temperature per `year` partition and appends it as a column for every record in that partition. The filter keeps only records where the row's temperature equals the partition minimum — the window function version of a self-join.

### Equivalent using `select()`

Window functions can be placed inside `select()` — useful when applying several windows in one pass:

```python
gsod.select(
    "year", "mo", "da", "stn", "temp",
    F.min("temp").over(each_year).alias("min_temp"),
).where("temp = min_temp").drop("min_temp").show()
```

> ⚠️ **Constraint** — Spark raises `AnalysisException` if you try to use `.over()` directly inside `groupby()` or `where()`. Materialise the column with `withColumn()` or `select()` first, then filter or group.

### Multiple partitioning columns

```python
each_station_year = Window.partitionBy("stn", "year")
```

Pass as many column names as needed.

### Self-join vs window function

The self-join approach (book §10.1.1) creates `coldest_temp` via `groupBy().agg()` then joins `gsod` back to itself. Problems:

- Self-joins are considered an anti-pattern: the data is already in the table; joining it to itself is redundant and can be slow.
- Intent is obscured: *why am I joining this table to itself?*

The window version — `F.min("temp").over(each_year)` — reads almost like English and avoids the join.

---

## 3. Ranking functions

Ranking functions require an **ordered** window — append `.orderBy()` to the `WindowSpec`:

```python
temp_per_month_asc = Window.partitionBy("mo").orderBy("count_temp")
```

### The five ranking functions

| Function | Ties | Gaps after ties? | Notes |
|---|---|---|---|
| `F.rank()` | Shared rank | Yes (olympic / nonconsecutive) | 1, 1, 3 — next rank skips |
| `F.dense_rank()` | Shared rank | No (consecutive) | 1, 1, 2 — no gaps |
| `F.percent_rank()` | Shared rank | — | `(rank − 1) / (N − 1)`, range 0.0–1.0 |
| `F.ntile(n)` | By position | — | Splits into `n` equal buckets |
| `F.row_number()` | None (arbitrary for ties) | — | Strictly 1, 2, 3, … — nondeterministic for tied values |

### rank() — nonconsecutive (olympic)

```python
gsod_light.withColumn(
    "rank_tpm", F.rank().over(temp_per_month_asc)
).show()
# mo=03: count_temp 12, 12 → both rank 1; count_temp 24 → rank 3 (skip 2)
```

### dense_rank() — consecutive

```python
gsod_light.withColumn(
    "rank_tpm", F.dense_rank().over(temp_per_month_asc)
).show()
# mo=03: count_temp 12, 12 → both rank 1; count_temp 24 → rank 2 (no skip)
```

### percent_rank()

```python
temp_each_year = Window.partitionBy("year").orderBy("temp")
gsod_light.withColumn(
    "rank_tpm", F.percent_rank().over(temp_each_year)
).show()
# Lowest value → 0.0, highest → 1.0
# For a window of 4 records: (rank_index) / (4 − 1)
```

You can create a new `WindowSpec` by chaining additional methods onto an existing one:

```python
temp_each_year = each_year.orderBy("temp")  # reuses each_year's partitionBy
```

### ntile(n)

```python
gsod_light.withColumn("tile", F.ntile(2).over(temp_each_year)).show()
# Divides each partition into 2 equal buckets (1 or 2)
```

### row_number()

```python
gsod_light.withColumn("rn", F.row_number().over(temp_each_year)).show()
# Always 1, 2, 3 … — no shared values even for ties
```

> ⚠️ **row_number() and ties** — when the `orderBy` column has ties, `row_number()` assigns an arbitrary order among the tied records. Use a tiebreaker column in `orderBy()` if reproducibility matters.

### Reversing sort order

`Window.orderBy()` has no `ascending=False` parameter — use `.desc()` on the `Column` instead:

```python
temp_per_month_desc = Window.partitionBy("mo").orderBy(
    F.col("count_temp").desc()
)
```

---

## 4. Analytical functions

Analytical functions look at the records *around* the current record — useful for time-series feature engineering ("what was yesterday's temperature?").

### lag() and lead()

```python
F.lag(col, n=1, default=None)   # value of col, n rows before
F.lead(col, n=1, default=None)  # value of col, n rows after
```

Both require an ordered window (otherwise "before" and "after" are undefined). When the offset reaches beyond the window boundary, the result is `default` (null by default).

```python
gsod_light.withColumn(
    "previous_temp",   F.lag("temp").over(temp_each_year)       # 1 back
).withColumn(
    "previous_temp_2", F.lag("temp", 2).over(temp_each_year)   # 2 back
).show()
# First record in each window → null (no prior value)
# Second record → first record's value for lag(1), null for lag(2)
```

### cume_dist()

Cumulative distribution: for each record, the fraction of records in the window whose ordered value is ≤ the current record's value.

```python
gsod_light.withColumn(
    "cume_dist", F.cume_dist().over(temp_each_year)
).show()
# First record (lowest temp) → 1/4 = 0.25 (one of four records ≤ this value)
# Last record (highest temp) → 1.0
```

Contrast with `percent_rank()`: `percent_rank()` is `(rank−1)/(N−1)` (starts at 0, excludes the current row from the denominator); `cume_dist()` is `count(≤ current) / N` (starts above 0, includes the current row).

---

## 5. Window frame boundaries

### The surprise: ordering changes the default frame

```python
not_ordered = Window.partitionBy("year")
ordered     = not_ordered.orderBy("temp")

gsod_light.withColumn("avg_NO", F.avg("temp").over(not_ordered)) \
          .withColumn("avg_O",  F.avg("temp").over(ordered)).show()
```

`avg_NO` is the same for every record in a year. `avg_O` is different for every record — it grows as you move through the partition.

**Why?** Spark uses different *default frames* depending on whether the window is ordered:

| Window | Default frame | Behaviour |
|---|---|---|
| Unordered | `rowsBetween(unboundedPreceding, unboundedFollowing)` | Whole partition — every record sees the same value |
| Ordered | `rangeBetween(unboundedPreceding, currentRow)` | Growing — each record sees all records up to and including itself |

### Frame boundary constants

```python
Window.unboundedPreceding  # first record in the partition
Window.currentRow          # the record being evaluated
Window.unboundedFollowing  # last record in the partition
```

Positive integers look forward; negative integers look backward:

```
-2  -1  Window.currentRow  +1  +2
←————————————————→
Window.unboundedPreceding   Window.unboundedFollowing
```

> ⚠️ Do not use raw large integers (e.g., `sys.maxsize`) to represent the first or last record. Use the named constants — Spark translates them to the correct internal values and the intent is clear.

### Explicit frame specification

```python
# Unbounded (whole partition) — same as unordered default
not_ordered = Window.partitionBy("year").rowsBetween(
    Window.unboundedPreceding, Window.unboundedFollowing
)

# Growing (up to and including current row) — same as ordered default
ordered = Window.partitionBy("year").orderBy("temp").rangeBetween(
    Window.unboundedPreceding, Window.currentRow
)

# Static (current row ± 1 neighbour)
three_rows = Window.partitionBy("year").orderBy("temp").rowsBetween(-1, 1)
```

> ⚠️ **Unordered + boundaries = nondeterministic** — if the window spec is unordered, Spark does not guarantee which records land inside bounded frames. Always add `orderBy()` before using `rowsBetween()` or `rangeBetween()` with non-unbounded endpoints.

### Rows vs ranges

`rowsBetween` counts by **row position** relative to the current row.  
`rangeBetween` counts by **column value** relative to the current row's ordered column value.

Use ranges when you want a time window of fixed duration regardless of how many records fall in it:

```python
ONE_MONTH_ISH = 30 * 60 * 60 * 24  # 2_592_000 seconds

one_month_window = (
    Window.partitionBy("year")
    .orderBy("dt_num")  # dt_num = unix_timestamp of the date
    .rangeBetween(-ONE_MONTH_ISH, ONE_MONTH_ISH)
)

gsod_light_p.withColumn(
    "avg_count", F.avg("count_temp").over(one_month_window)
).show()
```

For `rangeBetween` to work, the `orderBy` column must be **numeric** (or a type that can be cast to numeric). `unix_timestamp()` converts a date to seconds since 1970-01-01, making it a usable range axis.

| | `rowsBetween` | `rangeBetween` |
|---|---|---|
| Boundary defined by | Row offset (position) | Column value offset |
| Use when | You want exactly N rows before/after | You want a fixed time/value interval |
| Ordered column type | Any | Numeric (or date cast to numeric) |
| Multiple records with same ordered value | Each counts as one row | All included/excluded as a group |

---

## 6. UDFs within windows

Only **Series → scalar** pandas UDFs (group aggregate UDFs) can be applied over a window. Spark applies the UDF once per window partition, broadcasting the scalar result to every record in that partition — exactly like `F.avg()` over a window.

```python
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.window import Window

@F.pandas_udf("double")   # type-hint style (Spark 3.0+)
def median(vals: pd.Series) -> float:
    return vals.median()

gsod_light.withColumn(
    "median_temp",    median("temp").over(Window.partitionBy("year"))
).withColumn(
    "median_temp_g",  median("temp").over(
        Window.partitionBy("year").orderBy("mo", "da")
    ),
).show()
# median_temp   — same for every record in the year (unbounded window)
# median_temp_g — grows as records are added (bounded/ordered default)
```

Version requirements:

- UDF over **unbounded** window: Spark 2.4+
- UDF over **bounded** window: Spark 3.0+

> ⚠️ Do not mutate the input `pd.Series` inside the UDF — this introduces hard-to-diagnose bugs because pandas operations may share underlying memory.

> ❓ Revisit: `F.median("temp").over(window_spec)` — `F.median()` was added as a built-in in Spark 3.4. Verify whether it supports `.over()` in Spark 4.1 (if so, the custom `median` pandas UDF above becomes unnecessary for this case).

---

## 7. Decision checklist

The book's five-step checklist for building a window function:

1. **What kind of operation?** Summarize (aggregate), rank, or look ahead/behind (analytic)?
2. **How should the window be constructed?** Unbounded (every record gets the same value) or bounded (the answer depends on where the record sits within the partition)?
3. **Row-based or range-based boundaries?** Row: count by position. Range: count by value of the ordered column.
4. **Apply** — `F.function().over(window_spec)` in `withColumn()` or `select()`.
5. **Post-process normally** — a window function does not make the data frame special. Filter, group, or apply another window afterwards as usual.

---

## 8. Summary

- Window functions preserve the data frame's row count — every record gets a new computed column, nothing is collapsed.
- A `WindowSpec` is built with `Window.partitionBy()`, optionally `.orderBy()`, and optionally `rowsBetween()` / `rangeBetween()`. Apply it via `.over(window_spec)`.
- Three families: **aggregate** (any `F.sum/avg/min/max/count/…` with `.over()`), **ranking** (`rank`, `dense_rank`, `percent_rank`, `ntile`, `row_number`), **analytic** (`lag`, `lead`, `cume_dist`). Ranking and analytic functions require an ordered window.
- Default frame is **unbounded** for unordered windows and **growing** (`rangeBetween(unboundedPreceding, currentRow)`) for ordered windows — this is why `F.avg().over(ordered_window)` surprises most people.
- `rowsBetween` uses row position; `rangeBetween` uses the ordered column's value. Use ranges for time windows of fixed duration.
- Series → scalar pandas UDFs work as custom window functions (bounded windows require Spark 3.0+).

---

## 9. References

- [PySpark 4.1.1 — `Window` API reference](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Window.html)
- [PySpark 4.1.1 — window module overview](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/window.html)
- [PySpark window functions — Spark By Examples](https://sparkbyexamples.com/pyspark/pyspark-window-functions/)
- [ONS Spark guide — pandas UDFs](https://best-practice-and-impact.github.io/ons-spark/ancillary-topics/pandas-udfs.html)
