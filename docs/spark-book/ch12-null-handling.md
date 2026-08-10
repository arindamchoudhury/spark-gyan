# Chapter 10 — Null Handling

> *Learning-path topic: B6 (Beginner)*
> *Written: 2026-05-31 · Spark 4.1.x / Python 3.10+*

!!! warning "🔄 Needs revisiting — B6 source trace (flagged 2026-07-18)"
    Incomplete rather than wrong. The chapter covers the null API well; the trace opened nine gaps, and three of them are traps that produce wrong answers rather than errors — which is exactly what a chapter on null handling exists to prevent.

    **`NOT IN` with a nullable subquery returns nothing.** A single null on the right makes the result unknown for every row, so the anti join yields zero rows — an empty result that reads as a legitimate answer. Spark has a dedicated null-aware anti join path for it.

    **Descending order is not the reverse of ascending.** Default null ordering is `NULLS FIRST` for `ASC` and `NULLS LAST` for `DESC`, so nulls sit at the same end either way. A "top N" built by flipping sort direction can silently return N nulls.

    **`NaN` is not null.** `isNull` is false for it, `dropna()` keeps it, `coalesce` returns it — only `nanvl` handles it. A float column can carry both, so cleaning nulls does not clean the data.

    Also missing: that `how="any"`/`"all"` and `thresh` are the *same* parameter (`toMinNonNulls` maps both to `AtLeastNNonNulls`, making `dropna` an ordinary filter); that `count` is the only aggregate that cannot return null; that "null behaves like false" is true inside a `WHERE` and nowhere else (`ReplaceNullWithFalseInPredicate` applies it only there); that nullity is a bit in `UnsafeRow`'s bitmap rather than a value — which links to Ch08's finding that a wrong `nullable=false` yields wrong results; that ANSI mode turns several null-producing paths into errors; and that writing JSON drops null fields by default. Full list in the [B6 source trace](../reference/spark-source-map/topics/b9.md).

Null values are everywhere in real data. They propagate silently through expressions, behave differently from Python's `None` in comparisons, and interact with joins and aggregations in ways that surprise almost every beginner. Getting null semantics right prevents a class of bugs that produces wrong results with no error messages.

---

## What you'll learn

- How nulls propagate through arithmetic and comparisons
- The difference between `== null` and `isNull()` (and why one always returns null)
- How `dropna()` and `fillna()` work and when to use each
- How nulls interact with `count()`, `sum()`, and `groupBy()`
- Null-safe equality and null-safe joins

---

## The problem this solves

You join two tables and count the result — the number is lower than expected. You look at the data and find a `null` in a join key, which matched nothing and disappeared silently. Or you compute `F.avg("salary")` and get a higher result than seems right — because three null salaries were excluded from both the sum and the count. Null semantics are the root cause of both.

---

## Core concept

In SQL and PySpark, `null` means *unknown* — not zero, not empty string, not `None`. This has two critical consequences:

**1. Any comparison involving null returns null (not False).** `null == null` is `null`, not `True`. `null != 5` is `null`, not `True`. This means `filter(F.col("x") == null)` always drops every row because `null` is not truthy. Use `isNull()` and `isNotNull()` instead.

**2. Most aggregate functions ignore nulls.** `F.sum("salary")` sums only the non-null salaries. `F.count("salary")` counts only non-null salaries. `F.count("*")` counts all rows. This means `avg = sum/count` computed manually can differ from `F.avg()` if your count uses `"*"` but your sum excludes nulls.

The tools for handling nulls:

| Tool | What it does |
|---|---|
| `isNull()` / `isNotNull()` | Test for null — the correct null check |
| `dropna(how, thresh, subset)` | Remove rows with nulls |
| `fillna(value, subset)` | Replace nulls with a constant |
| `coalesce(col1, col2, ...)` | Return first non-null value across columns |
| `F.when(condition, val).otherwise(default)` | Conditional replacement including null checks |
| `eqNullSafe` / `<=>` | Null-safe equality: null == null is True |

---

## Examples

### Minimal example: null propagation and isNull

```python
# Apache Spark 4.1.x / PySpark 4.1.x · Python 3.10+
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ch09").master("local[*]").getOrCreate()

data = [("Alice", 95000), ("Bob", None), ("Carol", 72000), (None, 80000)]
df = spark.createDataFrame(data, ["name", "salary"])

# WRONG: == None is always null (not False), no rows are returned
df.filter(F.col("name") == None).show()
# (empty)

# CORRECT: isNull() does the null check
df.filter(F.col("name").isNull()).show()
# +----+------+
# |name|salary|
# +----+------+
# |null| 80000|
# +----+------+

# Aggregation: count("salary") skips the null salary
df.agg(
    F.count("*").alias("total_rows"),
    F.count("salary").alias("non_null_salaries"),
    F.avg("salary").alias("avg_salary"),       # ignores null salary
).show()
# +----------+-----------------+------------------+
# |total_rows|non_null_salaries|        avg_salary|
# +----------+-----------------+------------------+
# |         4|                3| 82333.33333333333|  ← avg of 95k, 72k, 80k
# +----------+-----------------+------------------+
```

### Building up: dropna, fillna, and coalesce

```python
# Apache Spark 4.1.x / PySpark 4.1.x · Python 3.10+
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ch09-handling").master("local[*]").getOrCreate()

data = [("Alice", "eng", 95000), ("Bob", None, None), ("Carol", "mkt", 72000), (None, "eng", 80000)]
df = spark.createDataFrame(data, ["name", "dept", "salary"])

# dropna — remove rows with any null
df.dropna(how="any").show()
# +-----+----+------+
# | name|dept|salary|
# +-----+----+------+
# |Alice| eng| 95000|
# |Carol| mkt| 72000|
# +-----+----+------+

# dropna — remove rows where at least thresh columns are non-null (invert logic)
# or specify which columns to check
df.dropna(how="any", subset=["name"]).show()
# Drops only the row where name is null

# fillna — replace nulls with a constant
df.fillna({"dept": "unknown", "salary": 0}).show()
# +-----+-------+------+
# | name|   dept|salary|
# +-----+-------+------+
# |Alice|    eng| 95000|
# |  Bob|unknown|     0|
# |Carol|    mkt| 72000|
# | null|    eng| 80000|
# +-----+-------+------+

# coalesce — first non-null across multiple columns
data2 = [(1, None, "backup_val"), (2, "primary_val", None), (3, None, None)]
df2 = spark.createDataFrame(data2, ["id", "primary", "backup"])
df2.withColumn("value", F.coalesce("primary", "backup", F.lit("default"))).show()
# +---+-----------+----------+-----------+
# | id|    primary|    backup|      value|
# +---+-----------+----------+-----------+
# |  1|       null|backup_val| backup_val|
# |  2|primary_val|      null|primary_val|
# |  3|       null|      null|    default|
# +---+-----------+----------+-----------+
```

### Null-safe joins

```python
# Apache Spark 4.1.x / PySpark 4.1.x · Python 3.10+
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ch09-nulljoin").master("local[*]").getOrCreate()

left  = spark.createDataFrame([(1, "a"), (2, None), (3, "c")], ["id", "key"])
right = spark.createDataFrame([("a", "X"), (None, "Y"), ("c", "Z")], ["key", "val"])

# Standard join: null keys never match
left.join(right, on="key", how="inner").show()
# +---+---+---+
# |key| id|val|
# +---+---+---+
# |  a|  1|  X|
# |  c|  3|  Z|
# +---+---+---+   (row 2 dropped — null key never matches)

# Null-safe equality: null == null is True (use eqNullSafe or <=>)
left.join(right, left["key"].eqNullSafe(right["key"]), how="inner").show()
# +---+---+---+---+
# | id|key|key|val|
# +---+---+---+---+
# |  1|  a|  a|  X|
# |  2|null|null|  Y|    ← null matched null
# |  3|  c|  c|  Z|
# +---+---+---+---+
```

---

## Common pitfalls

- **Using `== None` or `== null` in a filter** — always returns null (not False), so no rows are kept. Use `isNull()` / `isNotNull()`. This is the most common null mistake.
- **`count("*")` vs `count("col")` discrepancy** — if your column has nulls, these return different numbers. When you want the row count, use `count("*")`. When you want the non-null count of a specific column, use `count("col")`.
- **`fillna(0)` silently skips non-numeric columns** — `fillna(0, subset=["salary"])` only fills numeric columns. String columns are not affected. Use a `dict` to fill different types: `fillna({"name": "unknown", "salary": 0})`.
- **Null in a `groupBy` key forms a separate group** — `df.groupBy("dept")` where `dept` has nulls creates a `null` group in the result. This is often unexpected — add a `filter(F.col("dept").isNotNull())` before grouping if you want to exclude unknowns.
- **`dropna` with `how="all"` keeps rows with partial nulls** — `how="all"` drops only rows where every specified column is null. If you want to drop rows where any column is null, use `how="any"` (the default).

---

## Exercises

1. **Recall** — What does `F.col("x") == None` return in PySpark? Why? What should you use instead?

2. **Apply** — Create a DataFrame with nulls in three columns. Compute `count("*")`, `count("col1")`, `sum("col2")`, and `avg("col3")`. Explain why each number is what it is based on null semantics.

3. **Extend** — Build a salary imputation pipeline: replace null salaries with the average salary of the same department. Hint: compute `dept_avg = df.groupBy("dept").agg(F.avg("salary"))`, then join back and use `coalesce` or `when/otherwise` to fill the nulls.

---

## Summary

- `null` means *unknown* — any comparison with null returns null, not False or True.
- Always use `isNull()` / `isNotNull()` for null checks — never `== None` or `!= None`.
- Aggregate functions (`sum`, `avg`, `count("col")`) ignore nulls; `count("*")` does not.
- `dropna()` removes rows with nulls; `fillna()` replaces them with constants; `coalesce()` picks the first non-null across columns.
- Null join keys never match in standard joins; use `.eqNullSafe()` when null-null matching is needed.
- Chapter 11 moves to intermediate territory: complex column types — arrays, maps, and structs.

---

## References

- [PySpark null handling](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.dropna.html)
- [PySpark Column.isNull](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.isNull.html)
- [PySpark coalesce function](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.coalesce.html)
