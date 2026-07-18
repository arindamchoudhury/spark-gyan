# Chapter 5 — Data Frame Gymnastics: Joining and Grouping

> *Source: Rioux (2022), Chapter 5, pages 87–112.*
>
> Completes the core DataFrame API by adding joins and grouped aggregation. The running question is: *which Canadian TV channels show the greatest proportion of commercials?* The chapter builds the answer step by step — joining a star-schema log table to three reference tables, grouping by channel, computing conditional sums with `F.when()`, and handling null values at the end.
>
!!! info "📌 Notes adapted to PySpark 4.1.1"
    The join and `groupBy` APIs are unchanged in Spark 4.x. Key Spark 4.x changes that affect this chapter:

    - **`AnalysisException`** moved to `pyspark.errors` in Spark 3.4+ (`pyspark.sql.utils` still re-exports it, but prefer the new import path).
    - **ANSI mode is on by default in Spark 4.0+**: `F.sum()` / `F.avg()` throw `ArithmeticException` on overflow. Use `F.try_sum()` / `F.try_avg()` for null-on-overflow behaviour.
    - **Self-join ambiguity tightened (SPARK-46541, 4.0)**: always `.alias()` both sides of a self-join to avoid `AnalysisException`.
    - **New aggregate functions**: `F.median()` (3.4), `F.count_if()` (3.5), `F.any_value()` (3.5), `F.mode()` (3.4, gained `deterministic` param in 4.0), `F.bit_and/or/xor()` (3.5), `F.try_sum()` / `F.try_avg()` (3.5).
    - **`F.trim()` gained an optional trim-character parameter in 4.0**: `F.trim(col, trimStr)` removes a specific character instead of whitespace.
    - **`groupBy()` accepts integer ordinals in 4.0**: `df.groupBy(1, 2)` groups by column position (like SQL `GROUP BY 1, 2`).

---

## 1. Joining data frames

### 1.1 The three-ingredient blueprint

Every join in PySpark follows the same pattern:

```python
[LEFT].join(
    [RIGHT],
    on=[PREDICATES],
    how=[METHOD],
)
```

- **Left table** — the DataFrame to the left of `.join()`.
- **Right table** — the DataFrame inside the parentheses.
- **Predicates** — Boolean column expressions that determine if a left row matches a right row.
- **Method** — what to do when the predicate is true or false (controls which non-matching rows survive).

### 1.2 Predicates

A predicate is a column expression that evaluates to `True`/`False` per row pair.

```python
# Equality between two DataFrames on the same column name
logs["LogServiceID"] == log_identifier["LogServiceID"]

# Compound predicate — wrap each condition in parentheses to avoid precedence bugs
(logs["LogServiceID"] == log_identifier["LogServiceID"]) & (logs["left_col"] < log_identifier["right_col"])

# Multi-condition AND as a list (cleaner for long chains)
[logs["col1"] == right["colA"], logs["col2"] > right["colB"]]
```

**Equi-join shorthand** — when joining on equality between identically named columns, pass the column name as a string or a list of strings:

```python
logs.join(log_identifier, on="LogServiceID", how="inner")
# or multiple key columns:
logs.join(other, on=["col1", "col2"], how="inner")
```

This is the preferred form for equi-joins: shorter and it automatically deduplicates the join-key column (see naming section below).

### 1.3 Join methods

| `how=` | What it returns |
| --- | --- |
| `"inner"` *(default)* | Only rows where the predicate is `True`. Duplicates left rows if one left row matches multiple right rows. |
| `"left"` / `"left_outer"` | All left rows; unmatched rows get `null` for all right columns. |
| `"right"` / `"right_outer"` | All right rows; unmatched rows get `null` for all left columns. |
| `"outer"` / `"full"` / `"full_outer"` | All rows from both sides; unmatched sides filled with `null`. |
| `"left_semi"` | Rows from the left that have at least one match on the right. Only left columns returned; no duplication. |
| `"left_anti"` | Rows from the left that have **no** match on the right. The inverse of `inner`. |
| `"cross"` | Every left row paired with every right row (m × n records). Use via `crossJoin()` or `how="cross"`. |

!!! info "💡 When to choose a left join"
    Use `left` when you cannot guarantee every left-side key has a corresponding right-side entry — you keep all records and can fill nulls (or filter) later.

!!! warning "⚠️ Cross joins produce explosive row counts"
    Only useful for generating all possible combinations.

!!! warning "⚠️ Always specify `how=` as a keyword argument"
    (Palantir style guide)

    Passing `how` as a positional argument or omitting it entirely silently uses the default (`inner`) and makes intent opaque to the reader:

    ```python
    # Bad — positional, intent unclear
    logs.join(log_identifier, "LogServiceID", "inner")

    # Bad — omitted, defaults to inner silently
    logs.join(log_identifier, "LogServiceID")

    # Good — explicit, self-documenting
    logs.join(log_identifier, "LogServiceID", how="inner")
    ```

    This matters most for `inner` — it is the default so it is the easiest to forget, and a silent `inner` drops non-matching rows without any indication in the code.

### 1.4 Column naming clashes

Joining two DataFrames that share a column name creates ambiguity. PySpark allows the join but raises an `AnalysisException` (from `pyspark.errors`) when you try to reference the duplicate column by name.

**Three solutions, from simplest to most general:**

**Option 1 — Equi-join shorthand (deduplicated automatically):**
```python
# PySpark keeps only one copy of "LogServiceID" in the result
logs_and_channels = logs.join(log_identifier, "LogServiceID", how="inner")
```
*Only works for equality predicates. Best default choice.*

**Option 2 — Origin-name reference (drop the duplicate):**
```python
logs_verbose = logs.join(
    log_identifier,
    logs["LogServiceID"] == log_identifier["LogServiceID"],
)
# Drop the right table's copy; the left table's copy remains unambiguous
logs_verbose.drop(log_identifier["LogServiceID"]).select("LogServiceID")
```
*Works for any predicate. PySpark retains which DataFrame each column came from.*

**Option 3 — DataFrame aliasing (most general):**
```python
logs_verbose = logs.alias("left").join(
    log_identifier.alias("right"),
    logs["LogServiceID"] == log_identifier["LogServiceID"],
)
# F.col() resolves "left." / "right." as a table prefix
logs_verbose.drop(F.col("right.LogServiceID")).select("LogServiceID")
```
*Required when using `F.col()`, which loses origin tracking. Alias the DataFrames first.*

!!! info "📌 Self-join aliasing requirement (Spark 4.0, SPARK-46541)"
    When joining a DataFrame with itself or a derived copy, `.alias()` both sides unconditionally. The plan-identity tracking that previously resolved self-join column references is stricter in Spark 4.0 and may raise `AnalysisException` where it previously succeeded silently.

### 1.5 Multi-table join chain

Use method chaining to join multiple tables in sequence:

```python
cd_category = spark.read.csv(...).select(
    "CategoryID", "CategoryCD",
    F.col("EnglishDescription").alias("Category_Description"),
)
cd_program_class = spark.read.csv(...).select(
    "ProgramClassID", "ProgramClassCD",
    F.col("EnglishDescription").alias("ProgramClass_Description"),
)

full_log = (
    logs_and_channels
    .join(cd_category, "CategoryID", how="left")
    .join(cd_program_class, "ProgramClassID", how="left")
)
```

Use `left` joins when the link tables may not contain every key — preserves all records from the anchor table.

> 💭 (mine): Alias descriptive columns like `EnglishDescription` immediately at read time (inside `select()`) to avoid carrying ambiguous generic names through a multi-join pipeline.

---

## 2. Grouping and aggregation

### 2.1 The groupBy / agg pattern

```python
(
    full_log
    .groupby("ProgramClassCD", "ProgramClass_Description")
    .agg(F.sum("duration_seconds").alias("duration_total"))
    .orderBy("duration_total", ascending=False)
    .show(100, False)
)
```

Step by step:

1. **`groupby(*cols)`** — returns a `GroupedData` object. Not a DataFrame: has no `.show()`, no `.select()`. A transitional state.
2. **`agg(*exprs)`** — applies aggregate functions to each group's "cell" and returns a DataFrame. Takes `Column` objects from `pyspark.sql.functions`.
3. The result is a plain DataFrame again — chain any transformation or action.

**Why `agg()` instead of `.sum()`, `.count()` etc. directly on `GroupedData`:**

- `agg()` accepts multiple aggregate expressions at once; you can't chain aggregation methods on `GroupedData` (the first call converts it back to a DataFrame).
- `agg()` lets you alias the result column immediately, keeping names under control.

**Dict shorthand** (seen in the wild, not recommended for production):
```python
full_log.groupby(...).agg({"duration_seconds": "sum"})
# Produces a column named "sum(duration_seconds)" — hard to alias
```

### 2.2 Custom column definitions inside `agg()`

Because a column expression is a first-class `Column` object, you can pass an arbitrary expression — including conditionals — directly into `agg()`:

```python
answer = (
    full_log.groupby("LogIdentifierID")
    .agg(
        F.sum(
            F.when(
                F.trim(F.col("ProgramClassCD")).isin(
                    ["COM", "PRC", "PGI", "PRO", "LOC", "SPO", "MER", "SOL"]
                ),
                F.col("duration_seconds"),
            ).otherwise(0)
        ).alias("duration_commercial"),
        F.sum("duration_seconds").alias("duration_total"),
    )
    .withColumn(
        "commercial_ratio",
        F.col("duration_commercial") / F.col("duration_total"),
    )
)
```

!!! warning "⚠️ ANSI mode overflow (Spark 4.0+)"
    `F.sum()` and `F.avg()` throw `ArithmeticException` when a partial aggregate overflows (e.g., summing a `LongType` column that exceeds `Long.MAX_VALUE`). Use `F.try_sum()` / `F.try_avg()` to get null-on-overflow behaviour instead of a thrown exception. The `duration_seconds` data in this chapter uses small integer values so overflow is not a concern here, but keep this in mind for production pipelines with large numeric columns.

**`F.when()` blueprint:**

```python
(
    F.when([BOOLEAN TEST], [VALUE IF TRUE])
     .when([ANOTHER TEST], [VALUE IF TRUE])   # optional chain
     .otherwise([DEFAULT])                    # omit → null when no test matches
)
```

- `F.trim(col)` — strips leading and trailing whitespace from a string column. Essential before `isin()` if data may have padding. In Spark 4.0+, accepts an optional second argument: `F.trim(col, trimStr)` removes a specific character instead of whitespace.
- `.isin([list])` — returns `True` if the column value is in the list. Applied to a `Column` expression, not a Python variable.

!!! info "💡 Conditional aggregation with F.when()"
    You can use `F.when()` inline inside `F.sum()`, `F.avg()`, or any aggregate function — no need to create an intermediate `withColumn()` step first.

!!! warning "⚠️ Separate aggregation from column creation and null handling"
    (Palantir style guide)

    The example above chains `groupby → agg → withColumn → fillna` in a single block. Palantir recommends separating chains by operation type — mixing aggregation, column creation, and null handling in one block makes it harder to reason about each step:

    ```python
    # Discouraged — three different operation types in one chain
    answer = (
        full_log.groupby("LogIdentifierID")
        .agg(...)
        .withColumn("commercial_ratio", ...)
        .fillna(0)
    )

    # Better — each logical step is a named assignment
    answer = (
        full_log.groupby("LogIdentifierID")
        .agg(
            F.sum(...).alias("duration_commercial"),
            F.sum("duration_seconds").alias("duration_total"),
        )
    )

    answer = answer.withColumn(
        "commercial_ratio",
        F.col("duration_commercial") / F.col("duration_total"),
    )

    answer = answer.fillna(0)
    ```

    The chain is within Palantir's five-statement limit, so the original form won't cause a linting error. The separation is still preferred for clarity — a reader jumping to `answer =` can immediately see the aggregation logic without scrolling through the `withColumn` and `fillna`.

### 2.3 New aggregate functions (Spark 3.4+)

Several new aggregate functions landed in recent Spark releases that complement the `groupBy` pattern:

| Function | Since | What it does |
|---|---|---|
| `F.median(col)` | 3.4 | Exact median using partial sorting. |
| `F.count_if(condition)` | 3.5 | Counts rows where the boolean condition is `True`. Cleaner alternative to `F.sum(F.when(cond, 1).otherwise(0))`. |
| `F.any_value(col)` | 3.5 | Returns an arbitrary value from the group — useful when all group values are identical and you just need one. |
| `F.mode(col)` | 3.4 | Most frequent value. `F.mode(col, deterministic=True)` gives a stable result when there is a tie (4.0+). |
| `F.bit_and(col)` / `F.bit_or(col)` / `F.bit_xor(col)` | 3.5 | Bitwise aggregations across the group. |
| `F.try_sum(col)` | 3.5 | Like `F.sum()` but returns `null` on overflow (ANSI mode). |
| `F.try_avg(col)` | 3.5 | Like `F.avg()` but returns `null` on overflow. |

!!! info "💡 F.count_if() as a concise alternative"
    `F.count_if(condition)` is a concise replacement for the `F.sum(F.when(..., 1).otherwise(0))` pattern whenever you only need a count, not a weighted sum.

---

## 3. Null handling

### 3.1 `dropna()` — remove rows with nulls

```python
answer.dropna(subset=["commercial_ratio"])
```

Parameters:

| Parameter | Behaviour |
| --- | --- |
| `how="any"` *(default)* | Drop row if at least one specified column is null. |
| `how="all"` | Drop row only if all specified columns are null. |
| `thresh=None` *(default)* | If set, overrides `how`: drop rows with fewer than `thresh` non-null values. |
| `subset` *(default: all columns)* | List of column names to consider. |

### 3.2 `fillna()` — replace nulls with a value

```python
answer.fillna(0)                              # fill all numeric columns with 0
answer.fillna(0, subset=["commercial_ratio"]) # fill only specific columns
answer.fillna({"duration_commercial": 0, "duration_total": 0})  # per-column dict
```

- The `value` type must be compatible with the column type. `fillna("zero")` on a `double` column is a no-op.
- Subset and dict form let you be precise about which columns get filled.

!!! info "💡 dropna() and fillna() have na.* aliases"
    Both `dropna()` and `fillna()` also accept `na.drop()` / `na.fill()` as aliases via the `df.na` accessor.

---

## 4. End-to-end program

Full pipeline assembled for `spark-submit`:

```python
import os
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Canadian TV channels: proportion of commercials")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

DIRECTORY = "./data/broadcast_logs"

logs = spark.read.csv(
    os.path.join(DIRECTORY, "BroadcastLogs_2018_Q3_M8.CSV"),
    sep="|", header=True, inferSchema=True,
)
log_identifier = spark.read.csv(
    os.path.join(DIRECTORY, "ReferenceTables/LogIdentifier.csv"),
    sep="|", header=True, inferSchema=True,
)
cd_category = spark.read.csv(
    os.path.join(DIRECTORY, "ReferenceTables/CD_Category.csv"),
    sep="|", header=True, inferSchema=True,
).select("CategoryID", "CategoryCD", F.col("EnglishDescription").alias("Category_Description"))

cd_program_class = spark.read.csv(
    os.path.join(DIRECTORY, "ReferenceTables/CD_ProgramClass.csv"),
    sep="|", header=True, inferSchema=True,
).select("ProgramClassID", "ProgramClassCD", F.col("EnglishDescription").alias("ProgramClass_Description"))

# --- processing ---
logs = (
    logs
    .drop("BroadcastLogID", "SequenceNO")
    .withColumn(
        "duration_seconds",
        F.col("Duration").substr(1, 2).cast("int") * 3600
        + F.col("Duration").substr(4, 2).cast("int") * 60
        + F.col("Duration").substr(7, 2).cast("int"),
    )
)
log_identifier = log_identifier.where(F.col("PrimaryFG") == 1)

full_log = (
    logs
    .join(log_identifier, "LogServiceID", how="inner")
    .join(cd_category, "CategoryID", how="left")
    .join(cd_program_class, "ProgramClassID", how="left")
)

answer = (
    full_log.groupby("LogIdentifierID")
    .agg(
        F.sum(
            F.when(
                F.trim(F.col("ProgramClassCD")).isin(
                    ["COM", "PRC", "PGI", "PRO", "LOC", "SPO", "MER", "SOL"]
                ),
                F.col("duration_seconds"),
            ).otherwise(0)
        ).alias("duration_commercial"),
        F.sum("duration_seconds").alias("duration_total"),
    )
    .withColumn("commercial_ratio", F.col("duration_commercial") / F.col("duration_total"))
    .fillna(0)
)

answer.orderBy("commercial_ratio", ascending=False).show(1000, False)
```

> 💭 (mine): The key design decision is doing the conditional (`F.when`) inside `agg()` rather than as a pre-step `withColumn()`. This means only one pass through the data for both sums. Channels that appear with `commercial_ratio = null` after the join have a `null` `duration_total` — meaning they contributed zero seconds to the log; `fillna(0)` converts their ratio to `0.0`.

---

## 5. Summary

- **`join(right, on, how)`** — three ingredients: which two DataFrames, on what predicate, with what method.
- **Equi-join shorthand** (`on="col_name"`) is the cleanest form; it deduplicates the key column automatically.
- **Seven join methods**: `inner`, `left`, `right`, `outer`, `left_semi`, `left_anti`, `cross`. Default is `inner`.
- **Column name clashes** after joins: use equi-join shorthand, origin-name reference (`df["col"]`), or DataFrame aliasing (`.alias("tag")`).
- **`groupby(*cols).agg(*exprs)`** — the workhorse pattern; `agg()` accepts arbitrary `Column` expressions, including nested `F.when()`.
- **`F.when(condition, value).otherwise(default)`** — conditional column expression. Chainable. Omitting `.otherwise()` produces `null` for unmatched rows.
- **`F.trim(col)`** — strip leading/trailing whitespace. **`.isin([list])`** — membership test.
- **`dropna(how, thresh, subset)`** — remove rows with nulls. **`fillna(value, subset)`** — replace nulls.
- **ANSI overflow (Spark 4.0+)**: `F.sum()` / `F.avg()` throw on overflow; use `F.try_sum()` / `F.try_avg()` for null-on-overflow behaviour.
- **New aggregate functions (3.4+)**: `F.median()`, `F.count_if()`, `F.any_value()`, `F.mode()`, `F.try_sum()`, `F.try_avg()`.
- **Self-join aliasing (Spark 4.0)**: always `.alias()` both sides to avoid `AnalysisException`.

---

## 6. References

- PySpark DataFrame join API — <https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.join.html>
- PySpark GroupedData API — <https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/grouping.html>
- `pyspark.sql.functions` (F.when, F.trim, F.sum, etc.) — <https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions/index.html>
- `AnalysisException` (Spark 4.x) — import from `pyspark.errors`, not `pyspark.sql.utils`
- Book source code (Ch 5) — <http://mng.bz/6ZOR>
