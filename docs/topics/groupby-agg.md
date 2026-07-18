# `groupBy().agg()` Pattern

> *Cross-chapter synthesis — Rioux (2022), Chapters 3, 5, 7.*
>
> `groupBy().agg()` is PySpark's primary aggregation mechanism. Ch 3 introduces the basics; Ch 5 covers the full `agg(*exprs)` signature, conditional aggregation with `F.when()`, and ANSI-safe aggregation functions; Ch 7 maps SQL `GROUP BY` / `HAVING` to their DataFrame equivalents.

---

## Ch 3 — Basics: `groupBy().count()` and simple aggregations

```python
# Count rows per group
df.groupBy("country").count().show()

# Single aggregation
df.groupBy("category").agg(F.sum("amount").alias("total")).show()
```

`groupBy()` returns a `GroupedData` object. No data moves until you call `.count()`, `.agg()`, `.sum()`, `.mean()`, etc.

---

## Ch 5 — Full `agg(*exprs)` signature

`agg()` accepts any number of `Column` expressions, each returning one aggregate value per group. All expressions are computed in a single pass over the data:

```python
result = df.groupBy("region", "product") \
    .agg(
        F.count("*").alias("n"),
        F.sum("amount").alias("total"),
        F.avg("amount").alias("avg_amount"),
        F.min("amount").alias("min_amount"),
        F.max("amount").alias("max_amount"),
        F.countDistinct("customer_id").alias("unique_customers"),
        F.stddev("amount").alias("stddev_amount"),
        F.percentile_approx("amount", 0.5).alias("median_amount"),
    )
```

---

## Ch 5 — Conditional aggregation with `F.when()`

`F.when(condition, value).otherwise(alt)` produces a Column expression that evaluates per-row. Wrapping it inside `F.sum()` or `F.count()` creates a conditional aggregation — the equivalent of SQL `SUM(CASE WHEN … END)`:

```python
result = df.groupBy("region") \
    .agg(
        F.sum(F.when(F.col("status") == "completed", F.col("amount")).otherwise(0))
          .alias("completed_revenue"),
        F.sum(F.when(F.col("status") == "refunded", F.col("amount")).otherwise(0))
          .alias("refunded_revenue"),
        F.count(F.when(F.col("status") == "completed", F.lit(1)))
          .alias("completed_count"),
    )
```

`F.isin()` is a common companion for multi-value conditions:

```python
F.sum(F.when(F.col("channel").isin("web", "mobile"), F.col("amount")).otherwise(0))
```

---

## Ch 5 — ANSI-safe aggregation: `try_sum` and `try_avg`

With ANSI mode on (Spark 4.x default), `F.sum()` raises `SparkArithmeticException` on integer overflow. Use safe alternatives for columns that might overflow:

```python
df.groupBy("region") \
    .agg(
        F.try_sum("small_int_col").alias("safe_total"),   # null on overflow
        F.try_avg("small_int_col").alias("safe_avg"),     # null on overflow
    )
```

Or upcast before aggregating:

```python
df.withColumn("amount_long", F.col("amount").cast("long")) \
  .groupBy("region") \
  .agg(F.sum("amount_long").alias("total"))
```

---

## Ch 5 — New aggregation functions (Spark 3.4+)

| Function | Description |
|---|---|
| `F.mode(col)` | Most-frequent value (added Spark 3.4; `deterministic` param in 4.0) |
| `F.median(col)` | Exact median (uses percentile; added Spark 3.4) |
| `F.try_sum(col)` | Overflow-safe sum (added Spark 3.4) |
| `F.try_avg(col)` | Overflow-safe avg (added Spark 3.4) |
| `F.any_value(col)` | Arbitrary non-null value (added Spark 3.5) |
| `F.bit_and(col)` / `F.bit_or(col)` / `F.bit_xor(col)` | Bitwise aggregations |
| `F.regr_slope(y, x)` / `F.regr_intercept(y, x)` | Linear regression stats |

---

## Ch 7 — SQL `GROUP BY` / `HAVING` → DataFrame equivalents

```sql
-- SQL
SELECT region, SUM(amount) AS total
FROM orders
GROUP BY region
HAVING SUM(amount) > 10000
ORDER BY total DESC;
```

```python
# DataFrame equivalent
result = (
    orders
    .groupBy("region")
    .agg(F.sum("amount").alias("total"))
    .where(F.col("total") > 10000)   # HAVING = chained .where() after .agg()
    .orderBy(F.col("total").desc())
)
```

`HAVING` has no dedicated method — it is a `.where()` (or `.filter()`) applied after `.agg()`. The order matters: `.where()` before `.groupBy()` is a pre-aggregation filter (pushed to the scan); `.where()` after `.agg()` is HAVING (applied to aggregated rows).

---

## Summary

- `groupBy().agg(*exprs)` computes all aggregations in a single pass.
- `F.when(cond, val).otherwise(alt)` inside `F.sum()` / `F.count()` = conditional aggregation (SQL `SUM(CASE WHEN …)`).
- `F.try_sum()` / `F.try_avg()` are the ANSI-safe alternatives (added Spark 3.4).
- `HAVING` = `.where()` applied after `.agg()`; pre-aggregation filter = `.where()` before `.groupBy()`.
- New agg functions in Spark 3.4+: `F.mode()`, `F.median()`, `F.any_value()`.

---

## Chapter links

- [Ch 3 — Submitting and Scaling Your First PySpark Program](../books/rioux/chapters/03-submitting-scaling.md)
- [Ch 5 — Joining and Grouping](../books/rioux/chapters/05-joining-grouping.md)
- [Ch 7 — Python, Spark SQL, and interoperability](../books/rioux/chapters/07-python-sql.md)
