# Chapter 09 — Spark SQL

> *Learning-path topic: B8 (Beginner)*
> *Written: 2026-05-31 · Spark 4.1.x / Python 3.10+*

> 🔄 **Needs revisiting — Spark 4.2.0 (flagged 2026-07-18).** Incomplete rather than wrong. Spark 4.2.0 adds three things this chapter should cover: `QUALIFY` ([SPARK-31561]) for filtering on window-function results without a wrapping subquery; path-based name resolution (`SET PATH`, `CURRENT_PATH()`, [SPARK-54806]), which changes how unqualified names resolve and therefore affects the catalog section directly; and metric views (`CREATE VIEW … WITH METRICS`, [SPARK-54119]). The catalog/temp-view material as written still holds.

Spark SQL is not a separate system — it is the same Catalyst engine that runs the DataFrame API, exposed through SQL strings. Knowing both lets you choose the most readable form for each situation and mix them freely in the same pipeline.

---

## What you'll learn

- How to register a DataFrame as a SQL view and query it
- The relationship between the DataFrame API and SQL (same engine, different syntax)
- How to mix SQL expressions into DataFrame pipelines with `selectExpr` and `F.expr()`
- How the Spark catalog works
- Where SQL injection risk lives in PySpark

---

## The problem this solves

A SQL-fluent analyst on your team needs to explore a DataFrame you built in Python. Or you have a complex aggregation that is cleaner in SQL than as a method chain. Or you want to use a SQL function that has no direct `F.` equivalent. In all three cases, `spark.sql()` and `selectExpr()` let you stay in the same pipeline without a language switch.

---

## Core concept

The DataFrame API and SQL are two surfaces over the same optimizer. When you call `spark.sql("SELECT * FROM t WHERE year = 2024")` and `df.filter(F.col("year") == 2024)`, Catalyst produces the same logical plan — same optimisations, same physical execution.

To query a DataFrame with SQL, register it as a **temporary view** with `createOrReplaceTempView()`. The view lives in the Spark catalog — a session-scoped namespace of tables and views. It exists only for the lifetime of the `SparkSession`.

```python
df.createOrReplaceTempView("events")
spark.sql("SELECT * FROM events WHERE year = 2024").show()
```

`selectExpr(*sql_strings)` and `F.expr(sql_string)` let you embed SQL expressions inside a DataFrame chain without a full `spark.sql()` call. These are the most common mixing points:

```python
df.selectExpr("id", "upper(name) as name", "salary * 1.1 as adjusted_salary")
df.withColumn("label", F.expr("CASE WHEN salary > 90000 THEN 'senior' ELSE 'mid' END"))
```

**Spark 4.x:** ANSI SQL is the default dialect. `spark.catalog` exposes database, table, and view metadata programmatically.

---

## Examples

### Minimal example: register and query a view

```python
# Apache Spark 4.1.x / PySpark 4.1.x · Python 3.10+
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ch08").master("local[*]").getOrCreate()

data = [("Alice", "eng", 95000), ("Bob", "eng", 87000), ("Carol", "mkt", 72000)]
employees = spark.createDataFrame(data, ["name", "dept", "salary"])

# Register as a temp view
employees.createOrReplaceTempView("employees")

# Query with SQL
result = spark.sql("""
    SELECT dept, COUNT(*) AS n, AVG(salary) AS avg_salary
    FROM employees
    GROUP BY dept
    ORDER BY avg_salary DESC
""")
result.show()
# +----+---+----------+
# |dept|  n|avg_salary|
# +----+---+----------+
# | eng|  2|   91000.0|
# | mkt|  1|   72000.0|
# +----+---+----------+
```

### Building up: mixing SQL into a DataFrame chain

```python
# Apache Spark 4.1.x / PySpark 4.1.x · Python 3.10+
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ch08-mixing").master("local[*]").getOrCreate()

data = [("Alice", "eng", 95000), ("Bob", "eng", 87000), ("Carol", "mkt", 72000)]
df = spark.createDataFrame(data, ["name", "dept", "salary"])

# selectExpr — SQL expressions inside a DataFrame chain
result = df.selectExpr(
    "name",
    "dept",
    "salary",
    "salary * 1.1 AS adjusted",          # arithmetic
    "upper(name) AS name_upper",          # SQL function
    "CASE WHEN salary > 90000 THEN 'senior' ELSE 'mid' END AS level"  # CASE expression
)
result.show()
# +-----+----+------+---------+----------+------+
# | name|dept|salary| adjusted|name_upper| level|
# +-----+----+------+---------+----------+------+
# |Alice| eng| 95000| 104500.0|     ALICE|senior|
# |  Bob| eng| 87000|  95700.0|       BOB|   mid|
# |Carol| mkt| 72000|  79200.0|     CAROL|   mid|
# +-----+----+------+---------+----------+------+

# F.expr — single SQL expression as a Column
df.withColumn("bonus", F.expr("salary * 0.1")).show(2)
# +-----+----+------+------+
# | name|dept|salary| bonus|
# +-----+----+------+------+
# |Alice| eng| 95000|9500.0|
# |  Bob| eng| 87000|8700.0|
# +-----+----+------+------+

# The catalog: inspect registered views
spark.catalog.listTables()      # returns list of Table objects
spark.catalog.dropTempView("employees")   # clean up
```

### HAVING equivalent and SQL-style filtering

```python
# Apache Spark 4.1.x / PySpark 4.1.x · Python 3.10+
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ch08-having").master("local[*]").getOrCreate()

data = [("eng", 95000), ("eng", 87000), ("eng", 91000), ("mkt", 72000)]
df = spark.createDataFrame(data, ["dept", "salary"])
df.createOrReplaceTempView("salaries")

# SQL HAVING — keep departments with 2+ employees and average salary > 85k
spark.sql("""
    SELECT dept, COUNT(*) AS n, AVG(salary) AS avg_sal
    FROM salaries
    GROUP BY dept
    HAVING COUNT(*) >= 2 AND AVG(salary) > 85000
""").show()
# +----+---+------------------+
# |dept|  n|           avg_sal|
# +----+---+------------------+
# | eng|  3|91000.0           |
# +----+---+------------------+

# DataFrame API equivalent — chain .where() after .agg()
(
    df.groupBy("dept")
    .agg(F.count("*").alias("n"), F.avg("salary").alias("avg_sal"))
    .where((F.col("n") >= 2) & (F.col("avg_sal") > 85000))
    .show()
)
```

---

## Common pitfalls

- **SQL injection in dynamic queries** — never build a `spark.sql()` string by concatenating user input: `spark.sql(f"SELECT * FROM t WHERE name = '{user_input}'")`. If `user_input = "' OR '1'='1"`, you expose all rows. Use `F.col("name") == user_input` in the DataFrame API, or parameterised views.
- **View names are session-scoped and global within the session** — `createOrReplaceTempView("data")` from two different notebooks in the same cluster overwrites the same view. Use descriptive, unique names.
- **SQL written order ≠ execution order** — `SELECT` runs after `WHERE` and `GROUP BY`. You cannot reference a `SELECT` alias in `WHERE` (use `HAVING` for post-aggregation filters). This surprises SQL beginners who learned `SELECT` first.
- **`createTempView()` raises if the name exists** — use `createOrReplaceTempView()` which overwrites silently, or check with `spark.catalog.tableExists("name")` first.
- **`selectExpr` uses SQL function names, not Python names** — inside `selectExpr`, `upper("name")` is the SQL function, not `F.upper`. They behave identically, but the namespace is SQL, not Python.

---

## Exercises

1. **Recall** — What is the difference between a `createTempView` and a `createOrReplaceTempView`? When would you use each?

2. **Apply** — Register a DataFrame as a temp view. Write the same aggregation query three ways: (1) `spark.sql()`, (2) the DataFrame API with `.groupBy().agg()`, and (3) `selectExpr()` + `.groupBy().agg()`. Call `.explain()` on all three and compare the physical plans.

3. **Extend** — Demonstrate the SQL injection risk: create a DataFrame with a `name` column, register it as a view, then show why building a filter string with f-string interpolation from user input is dangerous. Then show the safe DataFrame API equivalent.

---

## Summary

- `df.createOrReplaceTempView("name")` registers a DataFrame as a SQL view in the session catalog.
- `spark.sql("SELECT ...")` queries views — same Catalyst engine as the DataFrame API.
- `selectExpr(*sql_strings)` and `F.expr(sql_string)` embed SQL expressions in DataFrame chains.
- DataFrame API and SQL produce identical physical plans — choose whichever is more readable.
- Never build `spark.sql()` strings from user input — use the DataFrame API for user-controlled filters.
- Chapter 10 covers null handling — the one topic where SQL semantics regularly surprise Python developers.

---

## References

- [Spark SQL programming guide](https://spark.apache.org/docs/latest/sql-programming-guide.html)
- [PySpark catalog API](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.catalog.html)
