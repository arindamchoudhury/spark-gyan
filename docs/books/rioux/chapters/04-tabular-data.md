# Chapter 4 — Analyzing Tabular Data with pyspark.sql

> *Source: Rioux (2022), Chapter 4, pages 62–86.*
>
> Switches from unstructured text to structured tabular data, using a Canadian broadcast-log dataset. Covers reading delimited files, understanding star schemas, and the full toolkit for column-level manipulation: selecting, dropping, creating, renaming, reordering, and summarizing columns. The chapter's running question is: *which TV channels show the greatest proportion of commercials?*
>
!!! info "📌 Notes adapted to PySpark 4.1.1"
    Core DataFrame API (`select`, `drop`, `withColumn`, `cast`, `substr`) is unchanged in 4.x. **Important behavioral change:** Spark 4.x enables ANSI mode by default (`spark.sql.ansi.enabled = true`). In Spark 3.x, `cast("invalid_string", "int")` silently returned `null`; in Spark 4.x it raises a runtime exception — `SparkNumberFormatException` for invalid string-to-numeric casts, `SparkArithmeticException` for overflow. Validate data before casting, or use `try_cast()` for nullable semantics.

---

## 1. What is tabular data?

- Data organised as a two-dimensional grid of **rows** (records) and **columns** (attributes). Every cell holds a single value.
- Familiar analogies: a spreadsheet, a SQL table, a pandas DataFrame.
- PySpark's DataFrame maps naturally to tabular data — it is **column-major**: operations work on named, typed columns rather than on individual rows.
- The `pyspark.sql` module's name is a direct nod to this SQL heritage.

### Creating a small DataFrame inline

```python
my_grocery_list = [
    ["Banana", 2, 1.74],
    ["Apple",  4, 2.04],
    ["Carrot", 1, 1.09],
    ["Cake",   1, 10.99],
]

df = spark.createDataFrame(my_grocery_list, ["Item", "Quantity", "Price"])
df.printSchema()
# root
#  |-- Item:     string (nullable = true)
#  |-- Quantity: long   (nullable = true)
#  |-- Price:    double (nullable = true)
```

`createDataFrame(data, schema)` accepts a list-of-lists, a pandas DataFrame, or an RDD. When `schema` is a list of column names, PySpark infers types from the Python values.

---

## 2. Reading delimited (CSV) data

### Standard boilerplate

```python
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.getOrCreate()
```

### Reading the broadcast-log CSV

```python
import os
DIRECTORY = "./data/broadcast_logs"

logs = spark.read.csv(
    os.path.join(DIRECTORY, "BroadcastLogs_2018_Q3_M8.CSV"),
    sep="|",                          # field delimiter
    header=True,                      # first row = column names
    inferSchema=True,                 # auto-detect column types (reads data twice)
    timestampFormat="yyyy-MM-dd",     # pattern for timestamp columns
)
```

### Key `spark.read.csv()` parameters

| Parameter | Default | Purpose |
| --- | --- | --- |
| `sep` | `,` | Field delimiter character |
| `header` | `False` | Use first row as column names |
| `inferSchema` | `False` | Auto-detect column types (expensive — reads data twice) |
| `quote` | `"` | Character wrapping text fields that may contain the delimiter |
| `timestampFormat` | ISO 8601 | Date/time pattern string for timestamp columns |
| `schema` | `None` | Explicit schema (`StructType` or DDL string); overrides inference |

!!! warning "⚠️ `inferSchema` in production"
    — Inferring schema requires a full pre-scan of the data. For large, repeatedly-read datasets, infer once, save the schema, and pass it explicitly on subsequent reads (see Ch 6).

!!! info "💡 No schema at all?"
    Columns default to `_c0`, `_c1`, … and all types are `string`. Fine for a quick peek, useless for analysis.

!!! info "📌 ANSI mode & `cast()` in Spark 4.x"
    — With ANSI mode on by default, casting a malformed string to a numeric type raises an error rather than returning `null`. Use `F.col("x").try_cast(T.IntegerType())` (Column method, 4.0+) or the SQL `TRY_CAST` expression when the input is dirty. Note: `try_cast` is a method on `Column`, not a standalone function in `F`.

### Delimiter variants in the wild

- **Comma (`,`)** — most common; breaks when values contain commas (use `quote` to escape).
- **Pipe (`|`)** — rare in text, good delimiter choice.
- **Semicolon (`;`)** — common in French/European locales (decimal comma conflicts with `,`).

---

## 3. Star schemas and denormalisation

The broadcast-log dataset follows a **star schema**: a central fact table (`logs`) full of integer IDs that reference auxiliary **link tables** (e.g., `CD_category`, `CD_ProgramClass`).

```
Logs (fact table)
  BroadcastLogID
  LogServiceID
  CategoryID  ──────► CD_category
  ProgramClassID ────► CD_ProgramClass
  ...
```

**Normalisation** (star schema) keeps data consistent and compact; great for transactional databases. **Denormalisation** (one fat table) removes join overhead; preferred for analytics. PySpark analytics typically builds toward a denormalised table via joins (Ch 5).

> 💭 (mine): Always explore the fact table first before joining link tables. Many ID columns may not be needed for a given question, saving join cost entirely.

---

## 4. Column manipulation

### 4.1 Selecting columns — `select()`

```python
# Select by name strings (simplest)
logs.select("BroadcastLogID", "LogServiceID", "LogDate").show(5, False)

# Four syntactically valid forms for a single column
logs.select(logs.LogDate)           # dot notation        ← avoid
logs.select(logs["LogDate"])        # bracket notation    ← avoid
logs.select(F.col("LogDate"))       # col() — most portable
logs.select("LogDate")              # string shorthand — preferred for simple references

# Unpack a list of column names with *
cols_to_keep = ["LogServiceID", "LogDate", "Duration"]
logs.select(*cols_to_keep)

```

!!! warning "⚠️ Avoid dot and bracket notation"
    (Palantir style guide, ONS guide)

    `logs.LogDate` and `logs["LogDate"]` look convenient but break in common situations:

    - **Chaining** — if you add a new column with `withColumn()` then immediately filter on it in the same chain, `df.new_col` raises `AttributeError` because the column doesn't exist on `df` yet. `F.col("new_col")` resolves at execution time and works correctly.
    - **Stale values** — `df.col_name` captures the column reference at definition time. If you overwrite a column with `withColumn()`, the dot reference still points at the old values — a silent bug with no error.
    - **Special characters** — column names with spaces or symbols (e.g. `"Cost(£)"`) cannot be accessed with dot notation at all.
    - **Renamed variables** — if you rename the DataFrame variable, every `df.col_name` reference must be updated manually.

!!! note "Best practice"
    use `"col_name"` (string) for simple column references passed to functions; use `F.col("col_name")` for filter conditions, `when()` expressions, and any chained operation where the column may have just been created or overwritten.

**Peeking at many columns in groups of 3:**

```python
import numpy as np

column_split = np.array_split(np.array(logs.columns), len(logs.columns) // 3)
for group in column_split:
    logs.select(*group).show(5, False)
```

`logs.columns` is a plain Python list — treat it like one. `np.array_split` divides it into roughly equal chunks.

!!! info "💡 Databricks tip"
    — `display(df)` renders an interactive table in a Databricks notebook; `show()` is for everything else.

### 4.2 Dropping columns — `drop()`

```python
logs = logs.drop("BroadcastLogID", "SequenceNO")

# Verify
"BroadcastLogID" in logs.columns  # False
```

- `drop()` is the inverse of `select()`: keep everything *except* the listed columns.
- Dropping a non-existent column is a **no-op** (no error). Watch your spelling.
- Equivalent via `select()`: `logs.select(*[c for c in logs.columns if c not in ["BroadcastLogID", "SequenceNO"]])`

!!! info "💡 When to use which"
    use `drop()` when removing a small number of columns from a wide DataFrame; use `select()` when explicitly choosing a small subset to keep.

### Best practices for `drop()`

**1. Prefer `select()` over `drop()` for schema control**

`drop()` is implicit — it keeps whatever columns exist now plus any that arrive later if the upstream schema changes. `select()` is explicit — it defines exactly what comes out. Palantir style guide recommends `select()` as the default because it acts as a schema contract:

```python
# Fragile — if upstream adds a new column tomorrow, it silently flows through
df.drop("temp_col")

# Explicit — schema is defined, surprises are caught early
df.select("id", "name", "country", "duration_seconds")
```

Reserve `drop()` for cases where you genuinely can't enumerate the columns to keep — e.g. after a join that introduces one redundant key column on a wide DataFrame.

**2. Use `drop()` after joins to remove duplicate key columns**

After a join, both DataFrames' key columns are retained under the same name, creating an ambiguous duplicate. `drop()` is the idiomatic way to clean this up:

```python
result = logs.join(categories, on="CategoryID", how="left") \
             .drop(categories["CategoryID"])  # remove the right-side duplicate
```

Passing the DataFrame-qualified column (`categories["CategoryID"]`) rather than a string ensures you drop the right one when both sides have the same name.

**3. Silently dropping a non-existent column hides typos**

`drop("BroadcastLogId")` (wrong casing) succeeds with no error and no column removed. Always verify with `assert "col" not in df.columns` after a critical drop, or check spelling first:

```python
cols_to_drop = ["BroadcastLogID", "SequenceNO"]
assert all(c in logs.columns for c in cols_to_drop), f"column not found: {cols_to_drop}"
logs = logs.drop(*cols_to_drop)
```

**4. Don't use `drop()` + `distinct()` to paper over join explosions**

If a join unexpectedly duplicates rows, dropping columns and calling `distinct()` masks the root cause. Investigate why the join multiplied rows instead (non-unique key, unexpected many-to-many) — Palantir style guide calls this out explicitly.

### 4.3 Creating columns — `withColumn()`

`withColumn(name, col_expr)` appends a new column (or overwrites an existing one with the same name) to the DataFrame:

```python
logs = logs.withColumn(
    "duration_seconds",
    (
        F.col("Duration").substr(1, 2).cast("int") * 3600
        + F.col("Duration").substr(4, 2).cast("int") * 60
        + F.col("Duration").substr(7, 2).cast("int")
    ),
)
```

Breaking it down:

| Expression | What it does |
| --- | --- |
| `F.col("Duration").substr(1, 2)` | Extract 2 chars starting at position **1** (1-indexed) — the hours |
| `F.col("Duration").substr(4, 2)` | Minutes |
| `F.col("Duration").substr(7, 2)` | Seconds |
| `.cast("int")` | Convert string `"02"` → integer `2` |
| `* 3600`, `* 60`, `+ …` | Standard arithmetic directly on Column objects |

Column arithmetic uses normal Python operators (`+`, `-`, `*`, `/`, `//`, `%`); PySpark respects operator precedence.

!!! info "💡 When to use `F.col()` vs a plain string"

    - **Plain string** — works as a shorthand inside PySpark functions: `F.lower("word")`, `F.split("value", " ")`. The function internally wraps it in `col()` for you.
    - **`F.col()`** — required whenever you use Python operators (`==`, `!=`, `>`, `+`, `*`) or chain Column methods (`.substr()`, `.cast()`, `.alias()`). A plain string has no operators or methods — `"Duration" * 3600` is a Python string repetition, not a column expression.

    Rule of thumb: if you are doing anything to the column beyond passing it as a name, use `F.col()`.

**`withColumn()` vs `select()` for column creation:**

| | `select()` | `withColumn()` |
| --- | --- | --- |
| **Use when** | Working with a few explicit columns | Adding/replacing one column, keeping the rest |
| **Output columns** | Only what you list | All existing + the new column |
| **Caveat** | Verbose for wide DataFrames | Slow with 100+ new columns — use `select()` instead |

!!! warning "⚠️ Pitfall"
    — `withColumn("name", expr)` silently overwrites any existing column named `"name"`. Use intentionally for in-place updates; unexpected if you typo a column name.

!!! warning "⚠️ Performance"
    — Creating 100+ columns with chained `withColumn()` calls degrades query planning performance significantly. Batch them into a single `select()` call instead.

#### `distinct()` — de-duplicate rows

```python
# Show unique Duration values (avoids repeated identical rows in output)
logs.select(F.col("Duration"), ...).distinct().show(5)
```

`distinct()` is a transformation that removes duplicate rows. Used here just for cleaner REPL output; Chapter 5 covers it more fully.

### 4.4 Renaming columns — `withColumnRenamed()` and `toDF()`

```python
# Rename one column
logs = logs.withColumnRenamed("Duration_seconds", "duration_seconds")

# Rename ALL columns at once (batch lowercase example)
logs.toDF(*[c.lower() for c in logs.columns]).printSchema()
```

- `withColumnRenamed(old, new)` — renames a single column; no-op if `old` doesn't exist.
- `toDF(*new_names)` — replaces **all** column names at once; must pass exactly the right count.

!!! warning "⚠️ Prefer `select()` + `.alias()` over `withColumnRenamed()`"
    (Palantir style guide)

    `withColumnRenamed()` adds an extra step to the plan. When you are already writing a `select()`, rename inline with `.alias()` instead:

    ```python
    # Discouraged — two operations when one will do
    df.select("key", "comments").withColumnRenamed("comments", "num_comments")

    # Preferred — rename in the select itself
    df.select("key", F.col("comments").alias("num_comments"))
    ```

    `withColumnRenamed()` is still appropriate when you need to rename a column without writing a full `select()` — e.g. after a join to resolve a name collision, or when using `toDF()` for bulk renaming.

### 4.5 Reordering columns

Reordering is just `select()` with columns in the desired order:

```python
# Alphabetical
logs.select(sorted(logs.columns))

# Custom order
logs.select("LogDate", "LogServiceID", "duration_seconds", ...)
```

!!! info "💡 Note that Python's `sorted()` puts uppercase before lowercase (ASCII order)"
    Mixed-case column names will sort unexpectedly.

---

## 5. Diagnosing a DataFrame — `describe()` and `summary()`

### `describe(*cols)`

Returns count, mean, stddev, min, max for each numeric or string column. No columns specified → all columns.

```python
logs.describe("LogServiceID", "duration_seconds").show()
# +-------+------------------+------------------+
# |summary|      LogServiceID|  duration_seconds|
# +-------+------------------+------------------+
# |  count|           7169318|           7169318|
# |   mean|3453.8804215407936|            ...|
# | stddev|200.44137201584468|            ...|
# |    min|              3157|               0|
# |    max|              3925|           86400|
# +-------+------------------+------------------+
```

### `summary(*statistics)`

Like `describe()` but customisable statistics. Apply to a pre-selected set of columns:

```python
# Default: count, mean, stddev, min, 25%, 50%, 75%, max
logs.select("LogServiceID").summary().show()

# Custom: specific percentiles
logs.select("LogServiceID").summary("min", "10%", "90%", "max").show()
```

Available statistics: `count`, `mean`, `stddev`, `min`, `max`, and any `XX%` percentile string (e.g., `"25%"`, `"90%"`).

**Both methods:**
- Count only non-`null` values — useful for spotting mostly-empty columns.
- Work on string columns too (count, min, max by lexicographic order).
- Are **not** guaranteed stable across Spark versions — use them for exploration, not in production pipelines. For production, use `F.count()`, `F.mean()`, etc. inside `agg()`.

---

## 6. Charting and `toPandas()`

PySpark has no built-in charting. The pattern is:

```python
# Aggregate down to a small result, then convert
summary_df = logs.groupBy("LogServiceID").agg(F.sum("duration_seconds").alias("total_seconds"))
pandas_df   = summary_df.toPandas()   # collects all data onto the driver

import matplotlib.pyplot as plt
pandas_df.plot(...)
```

!!! info "💡 Always alias `agg()` results"
    Without `.alias()`, the output column is named `sum(duration_seconds)` — a raw function-name string that is awkward to reference downstream and inconsistent with the rest of the schema. Palantir style guide: every selected or aggregated column should have a meaningful name.

`toPandas()` pulls the entire DataFrame to the driver node's RAM. Rules of thumb:

- Only call on aggregated or already-small DataFrames.
- If rows × columns > ~100,000 on a 16 GB driver, reduce further first.
- Never go back from pandas → PySpark repeatedly; the shuffle cost is high.

### Best practices for charting

**1. Always aggregate in Spark before calling `toPandas()`**

`toPandas()` is identical to `collect()` — it moves every row to the driver heap. If the DataFrame is large and unaggregated, it will OOM or be extremely slow. The safe pattern is always: reduce in Spark first, then convert the small result:

```python
import pyspark.sql.functions as F

plot_df = (
    logs.groupBy("LogServiceID")
        .agg(F.sum("duration_seconds").alias("total_seconds"))
        .orderBy(F.desc("total_seconds"))
        .limit(20)
        .toPandas()
)
```

!!! warning "⚠️ Palantir and ONS guidance"
    — Both guides say to avoid `toPandas()` / `collect()` on unaggregated data entirely. Palantir: "eliminates the benefits of a distributed framework, resulting in lower performance or out-of-memory errors." ONS: "use `.limit()` to bring back only a small number of rows."

**2. For distributional plots, sample or use approximate functions — never pull raw data**

```python
# Histogram — use approxQuantile to get bin edges, then count per bucket
quantiles = df.approxQuantile("duration_seconds", [0.0, 0.25, 0.5, 0.75, 1.0], 0.01)

# Scatter plot — sample a fraction first
sample_pdf = df.sample(fraction=0.01, seed=42).select("x", "y").toPandas()

# Cardinality — approximate is fine for charts
df.groupBy("channel").agg(F.approx_count_distinct("user_id").alias("est_users"))
```

`approxQuantile` runs distributed across partitions and returns only five numbers — safe on billion-row DataFrames. `relativeError=0.01` is accurate enough for a chart axis.

**3. Enable Arrow for faster `toPandas()` conversion**

Arrow serializes columnar batches instead of row-at-a-time pickle, typically 10–100× faster:

```python
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
```

Already enabled by default on Databricks. Enable it explicitly in self-managed clusters.

**4. Choose the right library for the chart type**

All mainstream Python plotting libraries require a pandas DataFrame — none read Spark DataFrames directly (except Spark 4.0 native plotting, see below).

| Chart type | Recommended approach |
|---|---|
| Bar / line / area | `groupBy().agg()` → `toPandas()` → matplotlib or plotly |
| Histogram | `approxQuantile()` for bin edges → matplotlib |
| Scatter | `df.sample(0.001)` → `toPandas()` → plotly or seaborn |
| Box plot | `approxQuantile([0, .25, .5, .75, 1])` → matplotlib |
| Correlation heat map | `df.select(numeric_cols).toPandas().corr()` → seaborn `heatmap()` |

**5. Spark 4.0+ native plotting — no `toPandas()` needed**

Spark 4.0 introduced `df.plot` directly on PySpark DataFrames (powered by Plotly). For statistical charts (histogram, box, KDE), Spark computes distributed aggregates and sends only the summary to the driver — no full data collection:

```python
# Spark 4.0+ / Databricks Runtime 17.0+
df.plot(kind="hist", column="duration_seconds", bins=50)
df.plot.scatter(x="LogServiceID", y="duration_seconds")
df.plot(kind="bar", x="channel", y="total_seconds")
```

The API mirrors pandas `.plot` — same argument names, same chart types. Use this whenever running on Spark 4.x; it is strictly safer than `toPandas()` for large DataFrames.

**6. On Databricks, use `display()` for interactive exploration**

`display(df)` renders an interactive paginated table with a built-in chart builder — no `toPandas()`, no plotting code required. Use it for ad-hoc EDA; switch to explicit plotting code when you need a reproducible, customised chart.

---

## 7. Summary

- **Tabular data** = rows + typed columns. PySpark's DataFrame maps directly to this; the `pyspark.sql` API is SQL-inspired throughout.
- **`spark.read.csv(path, sep, header, inferSchema, …)`** reads delimited files. `inferSchema=True` is convenient but expensive; provide explicit schemas for production.
- **Star schemas** are common in relational sources. Explore the fact table first before joining link tables.
- **`select(*cols)`** — keep specific columns (or column expressions); also used to reorder.
- **`drop(*cols)`** — remove columns; no-op for missing names.
- **`withColumn(name, expr)`** — add or overwrite one column; keep everything else. Avoid chaining 100+ calls — use `select()` instead.
- **`cast(type)`** — convert a column's type. In Spark 4.x (ANSI mode on), invalid casts raise errors rather than returning `null`; use `try_cast()` for nullable semantics.
- **`substr(pos, len)`** — extract a substring (1-indexed position).
- **`withColumnRenamed(old, new)`** — rename one column. **`toDF(*names)`** — rename all columns at once.
- **`describe()`** and **`summary()`** — quick statistical snapshot for exploration. Do not use in production pipelines.
- **`toPandas()`** — collect a (small) DataFrame to the driver for charting; only after aggregation.

---

## 8. References

- PySpark CSV reader options — <https://spark.apache.org/docs/latest/sql-data-sources-csv.html>
- Spark date/time patterns — <https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html>
- `try_cast()` (Spark 4.x) — <https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.try_cast.html>
- Book source code (Ch 4) — <http://mng.bz/6ZOR>
