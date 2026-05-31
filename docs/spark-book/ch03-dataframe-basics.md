# Chapter 03 — The DataFrame API: Basics

> *Learning-path topic: B3 (Beginner)*
> *Written: 2026-05-31 · Spark 4.1.x / Python 3.10+*

The DataFrame is PySpark's primary data structure — a distributed, column-typed table with a rich transformation API modelled on SQL. Fluency with its core operations is what separates someone who has used PySpark from someone who can actually work with it.

---

## What you'll learn

- How to create DataFrames from scratch and inspect them
- The four ways to reference a column and which to prefer
- How `select`, `filter`, `withColumn`, `drop`, and `distinct` work together
- How to rename and reorder columns
- Why `alias()` every computed column is not optional

---

## The problem this solves

You get a DataFrame with 40 columns. You need to keep 6, rename 2, derive 1 from arithmetic on two others, filter out nulls in one column, and deduplicate. Without knowing the core API you would write a loop, a pandas conversion, or a UDF — all of which are slow. With the API, this is a 10-line method chain.

---

## Core concept

A PySpark DataFrame is typed and columnar. Every column has a name and a declared type (`StringType`, `IntegerType`, `DoubleType`, `TimestampType`, …). Unlike pandas, a PySpark DataFrame is **distributed** — it lives in partitions across executor memory, not on one machine.

The API is transformation-based: every method returns a new DataFrame; nothing is mutated in place. This makes method chaining natural and safe.

`pyspark.sql.functions` (imported as `F`) contains the complete library of column functions — 400+ functions from `F.col()` to `F.sum()` to `F.regexp_extract()`. Import it once at the top; everything flows from there:

```python
import pyspark.sql.functions as F
import pyspark.sql.types as T
```

**Column references.** Four forms exist; one is preferred:

| Form | Example | When to use |
|---|---|---|
| String shorthand | `"name"` | Passing column name to a function: `F.lower("name")` |
| `F.col()` | `F.col("name")` | When you need Column methods or operators: `F.col("salary") * 1.1` |
| Dot notation | `df.name` | Avoid — breaks on column names with spaces, breaks in chains |
| Bracket notation | `df["name"]` | Avoid — same problems as dot notation |

The rule: use `"col_name"` as a string for simple name arguments; use `F.col("col_name")` whenever you need to call a method (`.alias()`, `.cast()`, `.isNull()`) or apply an operator (`>`, `*`, `==`).

---

## Examples

### Minimal example: inspect and project

```python
# Apache Spark 4.1.x / PySpark 4.1.x · Python 3.10+
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ch03").master("local[*]").getOrCreate()

data = [("Alice", "eng", 95000), ("Bob", "eng", 87000), ("Carol", "mkt", 72000)]
df = spark.createDataFrame(data, ["name", "dept", "salary"])

df.printSchema()
# root
#  |-- name: string (nullable = true)
#  |-- dept: string (nullable = true)
#  |-- salary: long (nullable = true)

df.show()
# +-----+----+------+
# | name|dept|salary|
# +-----+----+------+
# |Alice| eng| 95000|
# |  Bob| eng| 87000|
# |Carol| mkt| 72000|
# +-----+----+------+

# Select two columns
df.select("name", "salary").show()
```

### Building up: the full manipulation toolkit

```python
# Apache Spark 4.1.x / PySpark 4.1.x · Python 3.10+
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ch03-toolkit").master("local[*]").getOrCreate()

data = [("Alice", "eng", 95000), ("Bob", "eng", 87000),
        ("Carol", "mkt", 72000), ("Alice", "eng", 95000)]  # duplicate
df = spark.createDataFrame(data, ["name", "dept", "salary"])

result = (
    df
    .filter(F.col("dept") == "eng")                              # keep engineering rows
    .withColumn("bonus", F.col("salary") * 0.1)                 # derive new column
    .withColumn("salary_k", F.round(F.col("salary") / 1000, 1)) # another derived column
    .drop("salary")                                              # remove original
    .withColumnRenamed("salary_k", "salary_k_usd")              # rename
    .distinct()                                                  # remove the duplicate
    .select("name", "dept", "salary_k_usd", "bonus")            # reorder
    .orderBy("name")
)

result.show()
# +-----+----+------------+-------+
# | name|dept|salary_k_usd|  bonus|
# +-----+----+------------+-------+
# |Alice| eng|        95.0| 9500.0|
# |  Bob| eng|        87.0| 8700.0|
# +-----+----+------------+-------+
```

### Batch column operations with `withColumns()` (Spark 3.3+)

```python
# Apache Spark 4.1.x / PySpark 4.1.x · Python 3.10+
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ch03-withcolumns").master("local[*]").getOrCreate()

df = spark.createDataFrame([("Alice", 95000), ("Bob", 87000)], ["name", "salary"])

# withColumns (plural) applies all changes in one plan node — preferred over chaining withColumn
df.withColumns({
    "bonus":    F.col("salary") * 0.1,
    "name_up":  F.upper("name"),
    "level":    F.when(F.col("salary") > 90000, "senior").otherwise("mid"),
}).show()
# +-----+------+-------+--------+------+
# | name|salary|  bonus| name_up| level|
# +-----+------+-------+--------+------+
# |Alice| 95000| 9500.0|   ALICE|senior|
# |  Bob| 87000| 8700.0|     BOB|   mid|
# +-----+------+-------+--------+------+
```

---

## Common pitfalls

- **Auto-generated column names are fragile** — `df.select(F.col("salary") * 1.1)` produces a column named `(salary * 1.1)`. Always `.alias()`: `F.col("salary") * 1.1).alias("adjusted_salary")`. Without aliases, downstream code breaks when Spark changes the auto-name.
- **`withColumn()` silently overwrites** — `df.withColumn("name", F.upper("name"))` overwrites `name` in-place. If you mistype the column name, the old column disappears without an error.
- **Chaining 100+ `withColumn()` calls degrades performance** — each `withColumn()` adds a node to the logical plan. For many new columns, use `withColumns()` (Spark 3.3+) or a single `select()` with a list comprehension.
- **`distinct()` is a shuffle** — it requires Spark to collect all data and compare across partitions. Use it only when duplicates are a real problem, not as a defensive measure.
- **`df.columns` returns a Python list** — you can manipulate it with standard Python: `df.select(*[c for c in df.columns if c != "temp_col"])` is idiomatic for conditional column dropping.

---

## Exercises

1. **Recall** — What is the difference between `filter()` and `where()`? When would you choose one over the other?

2. **Apply** — Create a DataFrame with 5 columns. Use a single `select()` call (no `withColumn`) to keep 3 original columns, add 1 derived column using arithmetic, and alias it. Verify the result with `printSchema()`.

3. **Extend** — Investigate what happens when you call `df.withColumn("existing_col", F.lit(0))` on a column that already exists. Then explore `df.withColumn("NEW_col", F.lit(0))` on a column that does not exist. What rule does this reveal about `withColumn`?

---

## Summary

- DataFrames are typed, columnar, distributed — every operation returns a new DataFrame, nothing is mutated.
- `import pyspark.sql.functions as F` and `import pyspark.sql.types as T` — always these aliases.
- Prefer `"col_name"` string form for name arguments; use `F.col("col_name")` when you need operators or methods.
- Always `.alias()` every computed column expression — auto-generated names are fragile.
- Use `withColumns()` (Spark 3.3+) when adding many columns at once; chained `withColumn()` degrades Catalyst planning.
- Chapter 4 builds on this by covering how to read and write data in multiple formats.

---

## References

- [PySpark DataFrame API](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html)
- [PySpark SQL functions](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions/index.html)
- [Palantir PySpark style guide](https://github.com/palantir/pyspark-style-guide)
