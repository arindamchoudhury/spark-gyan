# Chapter 7 — Bilingual PySpark: Blending Python and SQL

> *Source: Rioux (2022), Chapter 7, pages 153–174.*
>
> Reveals the deep relationship between PySpark's DataFrame API and SQL. The chapter shows how to register DataFrames as Spark SQL views, query them with `spark.sql()`, reproduce the same transformations in pure SQL, and then blend both languages using `selectExpr()`, `F.expr()`, and SQL strings inside `where()`. The running dataset is Backblaze hard-drive statistics for 2019.
>
> 📌 **Notes adapted to PySpark 4.1.1.** The SQL interop API (`spark.sql`, `createOrReplaceTempView`, `selectExpr`, `expr`) is unchanged from 3.x. Key Spark 4.x context:
>
> - **ANSI SQL is now the default dialect (Spark 4.0+).** The book describes ANSI SQL support as experimental; it is now the standard. HiveQL compatibility is maintained but ANSI SQL behaviour takes precedence where they differ.
> - **ANSI mode on by default** means integer division, overflow, and type-cast semantics changed — see Ch 4/5 notes.
> - `AnalysisException` is now imported from `pyspark.errors` (Spark 3.4+), not `pyspark.sql.utils`.

---

## 1. PySpark and SQL: shared vocabulary, different order

PySpark borrowed its method and function names directly from SQL. The same keywords appear in both — `select`, `where`, `groupBy`, `orderBy`, `count`, `min`, `max` — but the **order of operations** is inverted:

| | PySpark | SQL |
| --- | --- | --- |
| **Starting point** | DataFrame variable | `FROM` clause |
| **Structure** | Chain of methods, top to bottom | Operations block → target → conditions block |
| **Column creation / rename** | `withColumn()`, `withColumnRenamed()` | Everything goes through `SELECT` |

SQL is not case-sensitive; upper-case keywords are convention, not requirement.

---

## 2. Registering DataFrames as SQL views

Spark SQL and Python live in separate namespaces. A Python variable pointing to a DataFrame is invisible to `spark.sql()`. You must register the DataFrame explicitly.

### 2.1 The four view methods

```python
df.createOrReplaceTempView("view_name")    # ← use this almost always
df.createTempView("view_name")             # fails if name already exists
df.createOrReplaceGlobalTempView("name")   # tied to Spark application lifetime
df.createGlobalTempView("name")            # same, but fails if name exists
```

- **Local vs global**: a local (temp) view is tied to the current `SparkSession`. A global view persists across multiple SparkSessions in the same Spark application. In single-session analysis you won't need the global variants.
- **Always use `createOrReplaceTempView()`**: it silently replaces an existing view, mirroring Python's variable assignment semantics. The non-replace variant raises an error if the name is taken.

### 2.2 The Spark catalog

The catalog tracks all registered views and tables:

```python
spark.catalog.listTables()             # → list of Table objects
spark.catalog.dropTempView("name")     # remove a registered view
```

- `spark.catalog` is a property of `SparkSession`.
- `listTables()` returns `Table` objects with `name`, `database`, `tableType`, `isTemporary`.
- Caching is also managed through the catalog (covered in Ch 11).

---

## 3. Spark SQL syntax vs PySpark API

`spark.sql()` takes a SQL string and returns a DataFrame — you still need to call `.show()` to display it.

### 3.1 select and where

```python
# SQL
spark.sql("SELECT serial_number FROM backblaze_stats_2019 WHERE failure = 1").show(5)

# PySpark equivalent
backblaze_2019.where(F.col("failure") == 1).select("serial_number").show(5)
```

### 3.2 GROUP BY, ORDER BY, and HAVING

```sql
SELECT model,
       min(capacity_bytes / pow(1024, 3)) AS min_GB,
       max(capacity_bytes / pow(1024, 3)) AS max_GB
FROM backblaze_stats_2019
GROUP BY 1
HAVING min_GB != max_GB
ORDER BY 3 DESC
```

- SQL's `HAVING` filters on aggregated columns (post-`GROUP BY`). PySpark has no `having()` method — chain a regular `.where()` after `.agg()` instead.
- `GROUP BY 1` / `ORDER BY 3 DESC` are positional shortcuts referring to the 1st and 3rd output columns.

> ⚠️ **Avoid positional column aliases (`GROUP BY 1`, `ORDER BY 3`)**: they are brittle — if you add or reorder a column in the `SELECT` list, the positions shift silently and the query produces wrong results without any error. Use explicit column names: `GROUP BY model`, `ORDER BY max_GB DESC`.

### 3.3 CREATE OR REPLACE TEMP VIEW

```sql
CREATE OR REPLACE TEMP VIEW drive_days AS
    SELECT model, count(*) AS drive_days
    FROM drive_stats
    GROUP BY model
```

PySpark equivalent: assign the transformation result to a Python variable — no extra syntax needed.

- **`CREATE TABLE` vs `CREATE VIEW`**: with a Hive metastore connected, `CREATE TABLE` materialises the data to disk; `CREATE VIEW` stores only the query definition. Without a metastore, both behave as temp views.

### 3.4 UNION ALL and JOIN

```sql
SELECT {cols} FROM Q1
UNION ALL SELECT {cols} FROM Q2   -- stacks rows, keeps duplicates
```

```sql
SELECT drive_days.model, drive_days, failures
FROM drive_days
LEFT JOIN failures ON drive_days.model = failures.model
```

> ⚠️ **`union()` ≠ SQL `UNION`**: PySpark's `union()` keeps duplicates — it is equivalent to SQL's `UNION ALL`. SQL's plain `UNION` deduplicates. To deduplicate after a PySpark union, call `.distinct()` explicitly (it is expensive in a distributed context).

Before unioning, ensure all DataFrames have the same columns, in the same order, with the same types. In PySpark: `df.select(reference_df.columns)`. SQL has no column-list shorthand — you must list every column explicitly.

> ⚠️ **SQL injection when building SQL strings from Python**: constructing `spark.sql()` arguments by interpolating user-controlled values is a SQL injection risk. Only interpolate trusted, internal data (e.g. `df.columns`). Never interpolate raw user input into SQL strings.

### 3.5 Subqueries and CTEs

**Subquery** — replace a table name with a parenthesised `SELECT`:

```sql
SELECT failures.model, failures / drive_days AS failure_rate
FROM (SELECT model, count(*) AS drive_days FROM drive_stats GROUP BY model) drive_days
INNER JOIN (SELECT model, count(*) AS failures FROM drive_stats
            WHERE failure = 1 GROUP BY model) failures
ON drive_days.model = failures.model
```

**CTE (common table expression)** — prefixes named sub-queries with `WITH`; cleaner and easier to debug than nested subqueries:

```sql
WITH drive_days AS (
    SELECT model, count(*) AS drive_days FROM drive_stats GROUP BY model),
failures AS (
    SELECT model, count(*) AS failures FROM drive_stats
    WHERE failure = 1 GROUP BY model)
SELECT failures.model, failures / drive_days AS failure_rate
FROM drive_days INNER JOIN failures ON drive_days.model = failures.model
ORDER BY failure_rate DESC
```

**PySpark equivalent of a CTE**: wrap the query in a Python function. Intermediate DataFrames are scoped to the function body and cleaned up on return — no `DROP VIEW` needed.

---

## 4. Blending SQL syntax into PySpark

Three PySpark methods accept SQL-style expression strings:

| Method | Accepts | Typical use |
| --- | --- | --- |
| `selectExpr(*exprs)` | SQL column expressions | Arithmetic, computed columns with aliases |
| `F.expr(expr_str)` | SQL expression → Column object | Inside `agg()`, `withColumn()`, `select()` |
| `where(str)` / `filter(str)` | SQL predicate string | Simple filter conditions |

### 4.1 `selectExpr()`

```python
full_data = full_data.selectExpr(
    "model",
    "capacity_bytes / pow(1024, 3) capacity_GB",   # SQL arithmetic + inline alias
    "date",
    "failure",
)
```

Equivalent `select()` form (more explicit):

```python
full_data = full_data.select(
    F.col("model"),
    (F.col("capacity_bytes") / F.pow(F.lit(1024), 3)).alias("capacity_GB"),
    F.col("date"),
    F.col("failure"),
)
```

> ⚠️ **Palantir style — use explicit `.alias()`, not inline SQL aliases in `selectExpr`**: `"capacity_bytes / pow(1024,3) capacity_GB"` uses SQL's positional inline-alias syntax (name after expression, no `AS` keyword). This is non-obvious to readers unfamiliar with SQL alias conventions and bypasses IDE rename-refactoring. At minimum, use the `AS` keyword to make intent clear: `"capacity_bytes / pow(1024,3) AS capacity_GB"`. Better: use `select()` with `.alias()` so the output column name is explicit and consistent with the rest of the DataFrame API.

> ⚠️ **Palantir style — `selectExpr` mixes two languages in the same chain**: Palantir's guide recommends keeping transformations in the DataFrame API where the full chain is type-checkable and navigable. `selectExpr` is convenient for one-off arithmetic but the output column contract lives inside a string — invisible to type checkers and refactoring tools. Prefer `select()` with `F.col()` and `.alias()` when the expression is non-trivial or the column name matters downstream.

### 4.2 `F.expr()`

```python
failures = (
    full_data.where("failure = 1")
    .groupby("model", "capacity_GB")
    .agg(F.expr("count(*) failures"))   # SQL expression with embedded alias
)
```

> ⚠️ **Palantir style — avoid embedded aliases in `F.expr()`**: `F.expr("count(*) failures")` hides the output column name inside a SQL string. Prefer `F.count("*").alias("failures")` — explicit, consistent with the DataFrame API, and refactorable.

### 4.3 `where()` / `filter()` with SQL strings

```python
full_data.where("failure = 1")                   # SQL predicate string — shorter
full_data.where(F.col("failure") == 1)           # Column expression — equivalent, explicit
```

Both forms are valid. The SQL string form is shorter for simple conditions involving literal values.

> ⚠️ **SQL injection risk with f-string predicates**: the book uses:
> ```python
> data.where(f"capacity_GB between {capacity_min} and {capacity_max}")
> ```
> The book itself warns about this pattern (§7.4.5): if either variable came from user input, this is a SQL injection vector. The safe equivalent uses Column expressions only:
> ```python
> data.where(F.col("capacity_GB").between(capacity_min, capacity_max))
> ```
> Reserve SQL string predicates for **hard-coded, internal literal values only**. Never interpolate external or user-supplied data into SQL strings.

---

## 5. Data ingestion pattern: `reduce` for multi-file union

When stacking multiple DataFrames with slightly differing schemas, use `functools.reduce`:

```python
from functools import reduce

data = [spark.read.csv(DATA_DIRECTORY + f, header=True, inferSchema=True)
        for f in DATA_FILES]

# intersect column sets to find the safe common subset
common_columns = list(
    reduce(lambda x, y: x.intersection(y), [set(df.columns) for df in data])
)

# assert required columns exist before proceeding — fail early
assert {"model", "capacity_bytes", "date", "failure"}.issubset(set(common_columns))

# union all DataFrames over the common column set
full_data = reduce(
    lambda x, y: x.select(common_columns).union(y.select(common_columns)), data
)
```

- `reduce` applies a binary function across a list left-to-right, chaining `.union()` across all DataFrames.
- The `assert` is a lightweight schema contract: a missing required column raises `AssertionError` immediately, not a cryptic stack trace during a downstream transformation.

---

## 6. Summary

- PySpark shares vocabulary with SQL but the order of operations is reversed: PySpark chains transformations; SQL separates operations / target / conditions.
- Register a DataFrame for SQL with `createOrReplaceTempView("name")`; always use the `OrReplace` variant.
- `spark.sql("...")` returns a DataFrame. Manage views via `spark.catalog`.
- SQL's `HAVING` = PySpark's `.where()` chained after `.agg()`. SQL's `WITH` (CTE) = Python function scope.
- PySpark's `union()` keeps duplicates (= SQL `UNION ALL`). SQL's `UNION` deduplicates.
- `selectExpr()`, `F.expr()`, and `where(str)` accept SQL expressions — convenient shortcuts, but prefer explicit `.alias()` and Column expressions for clarity, consistency, and safety.
- Never interpolate user-supplied values into SQL strings — use Column expressions (`.between()`, `==`, etc.) to stay injection-safe.

---

## 7. References

- Spark SQL API reference: https://spark.apache.org/docs/latest/api/sql/index.html
- Backblaze hard-drive data: https://www.backblaze.com/cloud-storage/resources/hard-drive-test-data
- OWASP SQL Injection: https://owasp.org/www-community/attacks/SQL_Injection
- Python `functools.reduce`: https://docs.python.org/3/library/functools.html#functools.reduce
- Spark SQL programming guide: https://spark.apache.org/docs/latest/sql-programming-guide.html
