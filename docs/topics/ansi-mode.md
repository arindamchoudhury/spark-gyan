# ANSI Mode

> *Cross-chapter synthesis — Rioux (2022), Chapters 4, 5, 7.*
>
> ANSI SQL compliance mode changes how Spark handles type coercion and arithmetic overflow — from silently returning `null` or wrapping around to raising an exception. It became the default SQL dialect in Spark 4.0 and has progressive consequences in numeric functions and the SQL API.

---

## Ch 4 — `cast()` raises instead of returning null

In Spark 3.x with ANSI mode **off** (the old default), a failed `cast()` silently returns `null`:

```python
# Spark 3.x default — no ANSI
df.withColumn("n", F.col("s").cast("int"))
# "abc" → null (silent)
```

In Spark 4.x with ANSI mode **on** (new default), the same cast raises `SparkNumberFormatException`:

```python
from pyspark.errors import SparkNumberFormatException

# Spark 4.x — ANSI on by default
# df.withColumn("n", F.col("s").cast("int"))
# raises SparkNumberFormatException: [CAST_INVALID_INPUT] ...
```

**Safe cast with `try_cast()`.** `F.try_cast()` is a Column-level method (not a standalone function) that returns `null` on failure without raising — it restores the old lenient behaviour for one column without disabling ANSI globally:

```python
df.withColumn("n", F.try_cast(F.col("s"), T.IntegerType()))
# "abc" → null, "42" → 42
```

`try_cast()` is the preferred pattern when input quality is uncertain. Disable ANSI globally only as a last resort:

```python
spark.conf.set("spark.sql.ansi.enabled", "false")  # discouraged
```

---

## Ch 5 — Arithmetic overflow in aggregations

ANSI mode also affects arithmetic. In Spark 3.x, integer overflow **wraps around** silently (C-style modular arithmetic). In Spark 4.x it raises `SparkArithmeticException`.

```python
# Spark 4.x — summing integers that exceed Int.MaxValue raises:
# pyspark.errors.SparkArithmeticException: [ARITHMETIC_OVERFLOW] ...
df.agg(F.sum("small_int_col")).show()
```

**Safe aggregation alternatives:**

```python
# try_sum / try_avg — return null on overflow instead of raising
df.agg(F.try_sum("small_int_col")).show()     # null if overflow
df.agg(F.try_avg("small_int_col")).show()     # null if overflow

# Or: upcast the column before aggregating
df.withColumn("val_long", F.col("small_int_col").cast("long")) \
  .agg(F.sum("val_long")).show()              # Long cannot overflow in practice
```

`F.try_sum()` and `F.try_avg()` were added in Spark 3.4; they are the canonical ANSI-safe aggregation functions.

---

## Ch 7 — ANSI as the new default SQL dialect

Spark 4.0 made ANSI mode the **default** for the SQL API (`spark.sql()`). This means:

- `SELECT CAST('abc' AS INT)` raises an exception instead of returning `null`.
- Integer overflow in `SUM(int_col)` raises instead of wrapping.
- Division by zero raises `SparkArithmeticException` instead of returning `null`.

**SQL equivalents of `try_cast` and `try_sum`:**

```sql
SELECT TRY_CAST('abc' AS INT)   -- null on failure
SELECT TRY_SUM(small_int_col)   -- null on overflow
```

**Checking the current setting:**

```python
spark.conf.get("spark.sql.ansi.enabled")   # "true" on Spark 4.x
```

**Backward-compat strategy when migrating from Spark 3.x:**

1. Run existing jobs with ANSI on (Spark 4.x default). Any silent nulls that masked bad data will now raise — this is intentional; fix the root cause.
2. For legitimate lenient columns, switch to `try_cast` / `try_sum` / `try_avg` rather than disabling ANSI globally.
3. Only disable `spark.sql.ansi.enabled` temporarily for legacy pipelines that cannot be modified.

---

## Summary

- ANSI mode is **off by default in Spark 3.x**, **on by default in Spark 4.0+**.
- With ANSI on: `cast()` failure raises `SparkNumberFormatException`; integer overflow raises `SparkArithmeticException`; division-by-zero raises.
- `F.try_cast(col, type)` and SQL `TRY_CAST` return `null` on failure — the safe per-column alternative.
- `F.try_sum()` / `F.try_avg()` (added Spark 3.4) return `null` on overflow.
- Prefer lenient functions per-column over globally disabling ANSI.
- Migrating from Spark 3.x: treat new exceptions as surfaced data-quality bugs, not regressions.

---

## Chapter links

- [Ch 4 — Tabular data and CSV ingestion](../books/rioux/chapters/04-tabular-data.md)
- [Ch 5 — Joining and Grouping](../books/rioux/chapters/05-joining-grouping.md)
- [Ch 7 — Python, Spark SQL, and interoperability](../books/rioux/chapters/07-python-sql.md)
