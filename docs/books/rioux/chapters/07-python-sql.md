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

### 1.1 The five operations side by side

```python
# PySpark
elements
  .where(F.col("phase") == "liq")   # 2. filter rows
  .groupby("period")                 # 3. group
  .count()                           # 4. aggregate
  .show()                            # 5. display
```

```sql
-- SQL
SELECT period, count(*)   -- 4. what to show  (written first)
FROM elements             -- 1. data source   (written second)
WHERE phase = 'liq'       -- 2. filter rows   (written third)
GROUP BY period;          -- 3. group         (written fourth)
```

Both return the same result (liquid elements per period: Bromine in period 4, Mercury in period 6).

### 1.2 Written order vs execution order

In PySpark the method chain **reads in the order it is logically applied**: start with data → filter → group → aggregate. This makes the pipeline easy to follow.

In SQL, `SELECT` is written first but runs almost last. The actual execution order is:

```
FROM → WHERE → GROUP BY → SELECT → ORDER BY → LIMIT
```

This gap between written order and execution order has a practical consequence: you **cannot reference a `SELECT` alias in a `WHERE` clause** — `WHERE` runs before `SELECT` assigns the alias. Use `HAVING` instead (it runs after `GROUP BY`):

```sql
-- ❌ FAILS: alias not yet defined when WHERE runs
SELECT count(*) AS liq_count FROM elements WHERE liq_count > 1;

-- ✅ HAVING runs after GROUP BY / SELECT
SELECT period, count(*) AS liq_count
FROM elements
WHERE phase = 'liq'
GROUP BY period
HAVING count(*) > 1;
```

In PySpark there is no special keyword — just chain another `.where()` after `.count()`.

### 1.3 Both are declarative

It is tempting to call PySpark's method chain "imperative" because it reads like a sequence of steps. It is not. Both PySpark's DataFrame API and SQL are **declarative**: you describe *what* you want, not *how* to compute it. Spark's **Catalyst optimizer** takes the description from either API and decides the actual execution plan — reordering operations, pushing filters earlier, combining steps. Nothing runs until an action (`.show()`, `.collect()`, etc.) is called.

| | PySpark DataFrame | SQL |
| --- | --- | --- |
| **Style** | Declarative | Declarative |
| **Optimizer** | Catalyst | Catalyst (same engine) |
| **Written order** | Mirrors logical execution order | `SELECT` written first, runs last |
| **Mental model** | Pipeline — "take this, then do this" | Question — "show me X from Y where Z" |

The RDD API (`map()`, `filter()`, `reduce()`) is closer to imperative — you control each transformation explicitly with less optimizer freedom.

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

All four methods behave identically regardless of catalog backend (UC, Delta, Hive). Temp views are **session-scoped in-memory aliases** — they are never written to Unity Catalog, Delta, or any metastore.

| Method | Stored in | Survives session? | Appears in UC UI? |
|---|---|---|---|
| `createOrReplaceTempView` | Session memory | No | No |
| `createTempView` | Session memory | No | No |
| `createOrReplaceGlobalTempView` | App memory (across sessions) | Until app stops | No |
| `createGlobalTempView` | App memory (across sessions) | Until app stops | No |
| `df.write.saveAsTable("unity.default.t")` | UC + Delta on MinIO | Yes | Yes |
| `CREATE VIEW unity.default.v AS ...` | ❌ Not supported | — | — |

To persist a table permanently into Unity Catalog, use `saveAsTable()` with the full three-level name — not a view method.

> ⚠️ **UC OSS 0.4.0 does not support persistent views.** `CREATE OR REPLACE VIEW unity.default.v AS ...` raises `AnalysisException: [MISSING_CATALOG_ABILITY.VIEWS] Catalog unity does not support views. SQLSTATE: 0A000`. Only tables are supported as persistent catalog objects. Use `createOrReplaceTempView()` for session-scoped views or `saveAsTable()` for persistent data.

### 2.2 The Spark catalog

The catalog tracks all registered views and tables:

```python
spark.catalog.listTables("default")        # → list of Table objects (local session)
spark.catalog.listTables("global_temp")    # → list of global temp views
spark.catalog.dropTempView("name")         # remove a local (session-scoped) temp view
spark.catalog.dropGlobalTempView("name")   # remove a global temp view
```

Global temp views live under the reserved `global_temp` database and must be prefixed when queried or listed:

```python
df.createOrReplaceGlobalTempView("my_view")

spark.catalog.listTables("global_temp")                 # list global temp views
spark.sql("SHOW TABLES IN global_temp").show()          # SQL equivalent

spark.sql("SELECT * FROM global_temp.my_view").show()   # ✅ query with prefix
spark.sql("SELECT * FROM my_view").show()               # ❌ not found — missing prefix
```

- `spark.catalog` is a property of `SparkSession`.
- `listTables()` returns `Table` objects with `name`, `database`, `tableType`, `isTemporary`.
- Caching is also managed through the catalog (covered in Ch 11).

> ⚠️ **Spark 4.x bug — always pass the database name.** Calling `spark.catalog.listTables()` with no arguments triggers a `ParseException: [PARSE_EMPTY_STATEMENT]` because the no-arg path calls `currentDatabase()` internally, which can return an empty string that the JVM catalog then tries to parse as a SQL identifier. Pass the database name explicitly. Alternatively use SQL: `spark.sql("SHOW TABLES").show()`.

#### With Unity Catalog + Delta (UCSingleCatalog as default catalog)

`global_temp` behaves differently depending on whether you are **querying** or **managing**:

**Querying works** — Spark resolves `global_temp` as a reserved namespace during query parsing, before the catalog plugin is involved:

```python
spark.sql("SELECT * FROM global_temp.my_view").show()                        # ✅
spark.sql("SELECT period, count(*) FROM global_temp.elements "
          "WHERE phase='liq' GROUP BY period").show()                        # ✅
```

**Listing fails** — catalog management commands (`SHOW TABLES`, `listTables()`) go through UCProxy, which treats `global_temp` as a schema name inside the `unity` UC catalog and returns 404:

```python
spark.catalog.listTables("global_temp")              # ❌ Schema not found: unity.global_temp
spark.sql("SHOW TABLES IN global_temp").show()       # ❌ same error
```

This is a limitation of the UC Spark connector (`unitycatalog-spark 0.4.0`) — `UCSingleCatalog.listTables()` has no special handling for the reserved `global_temp` namespace and passes it directly to the UC REST API.

**Workaround — temporarily switch the default catalog:**

```python
spark.conf.set("spark.sql.defaultCatalog", "spark_catalog")
spark.sql("SHOW TABLES IN global_temp").show()
spark.conf.set("spark.sql.defaultCatalog", "unity")
```

When `spark_catalog` is active, Spark's internal layer handles `global_temp` before routing to any catalog plugin. Switch back to `unity` immediately after.

#### With Unity Catalog (UCSingleCatalog)

Unity Catalog uses a three-level namespace: `catalog.schema.table` (e.g. `unity.default.my_table`). However, the Spark connector (`UCSingleCatalog`) is configured per-catalog in `spark-defaults.conf`:

```
spark.sql.catalog.unity    = io.unitycatalog.spark.UCSingleCatalog
spark.sql.catalog.unity.uri = http://unitycatalog:8080
spark.sql.defaultCatalog   = unity
```

These three lines do different jobs:

| Line | What it does |
|---|---|
| `spark.sql.catalog.unity = io.unitycatalog.spark.UCSingleCatalog` | Registers a Spark catalog named `unity`, backed by the UC connector class. `unity` is now the first segment of the three-level namespace in Spark queries. |
| `spark.sql.catalog.unity.uri = http://unitycatalog:8080` | Tells the connector where the UC server is — the network address of the running UC process. |
| `spark.sql.defaultCatalog = unity` | Makes `unity` the default so unqualified names resolve there. |

Think of it like a JDBC connection: the Spark config is the **driver + connection string** — it tells Spark *how to reach* the database. The UC server at that URI is the **database itself**, with its own objects (catalogs, schemas, tables) persisted in PostgreSQL. Registering the connector in Spark does not create anything in that database, just as adding a JDBC URL to your app config does not create tables.

The `unity` in `spark.sql.catalog.unity` is the name Spark uses locally. The `unity` catalog object inside the UC server is a separate row in PostgreSQL — it must be created via the UC REST API before any catalog operation will find it.

**Check what catalogs exist in UC:**

```bash
curl http://localhost:8080/api/2.1/unity-catalog/catalogs
```

**Create the catalog and schema if missing (e.g. after a metadata reset):**

```bash
curl -sS -X POST http://localhost:8080/api/2.1/unity-catalog/catalogs \
    -H 'Content-Type: application/json' \
    -d '{"name":"unity","comment":"Local default catalog","storage_root":"s3://warehouse"}'

curl -sS -X POST http://localhost:8080/api/2.1/unity-catalog/schemas \
    -H 'Content-Type: application/json' \
    -d '{"name":"default","catalog_name":"unity","comment":"Default schema"}'
```

**`listTables()` with UCSingleCatalog:**

```python
spark.catalog.listTables("default")         # schema only — always works
spark.catalog.listTables("unity.default")   # fully-qualified — works once the catalog exists in UC
```

`UCSingleCatalog` accepts both forms. With `"unity.default"`, `UCProxy` splits on the dot and calls the UC REST API with `catalog=unity, schema=default`. If the `unity` catalog object does not exist in the UC server (e.g. after a metadata reset), this returns a 404. With `"default"`, UCProxy uses the connector's own catalog context and only looks up the schema — it does not depend on the catalog object being present by name.

**SQL is the most portable option** and works regardless of catalog backend:

```python
spark.sql("SHOW TABLES IN unity.default").show()
spark.sql("SHOW SCHEMAS IN unity").show()   # list schemas
spark.sql("SHOW CATALOGS").show()           # → spark_catalog, unity

# Or set context once and omit the qualifier everywhere:
spark.sql("USE unity.default")
spark.sql("SHOW TABLES").show()
```

#### `spark_catalog` — Spark's built-in session catalog

`SHOW CATALOGS` returns two entries even though only `unity` was registered in `spark-defaults.conf`:

```
+-------------+
|      catalog|
+-------------+
|spark_catalog|
|        unity|
+-------------+
```

`spark_catalog` is **Spark's own built-in default catalog** — it predates the pluggable catalog API and exists in every Spark session automatically. In this stack it is configured in `spark-defaults.conf` (space-separated, not `=`):

```
spark.sql.catalog.spark_catalog    org.apache.spark.sql.delta.catalog.DeltaCatalog
spark.sql.extensions               io.delta.sql.DeltaSparkSessionExtension
```

This overrides `spark_catalog` to use Delta Lake instead of the default Hive metastore. Unqualified two-part names (`schema.table` with no catalog prefix) resolve through it.

**Why `spark_catalog` does not appear in the UC UI:**

`SHOW CATALOGS` queries Spark's in-process registry — it lists every catalog registered in the current Spark session regardless of backend. `spark_catalog` is managed entirely by Spark and never touches the UC REST API, so UC has nothing to display.

| Catalog | Backend | In UC server? |
|---|---|---|
| `spark_catalog` | Delta Lake / local Hive metastore | No — Spark-internal |
| `unity` | Unity Catalog (PostgreSQL) | Yes |

They are independent namespaces. `spark_catalog.default.foo` and `unity.default.foo` are different tables backed by different systems.

---

## 3. Spark SQL syntax vs PySpark API

`spark.sql()` takes a SQL string and returns a DataFrame — you still need to call `.show()` to display it.

### 3.0 Data ingestion: Backblaze quarterly data (Listing 7.6)

The chapter uses Backblaze hard-drive statistics for 2019, split across four quarterly CSV directories. Each directory contains multiple daily CSV files — `pathGlobFilter` restricts the reader to `.csv` files only, skipping any non-CSV files in the directory.

```python
import os
DATA_DIRECTORY = "../data/backblaze"

q1 = spark.read.option("pathGlobFilter", "*.csv").csv(
    os.path.join(DATA_DIRECTORY, "data_Q1_2019"), header=True, inferSchema=True)
q2 = spark.read.option("pathGlobFilter", "*.csv").csv(
    os.path.join(DATA_DIRECTORY, "data_Q2_2019"), header=True, inferSchema=True)
q3 = spark.read.option("pathGlobFilter", "*.csv").csv(
    os.path.join(DATA_DIRECTORY, "data_Q3_2019"), header=True, inferSchema=True)
q4 = spark.read.option("pathGlobFilter", "*.csv").csv(
    os.path.join(DATA_DIRECTORY, "data_Q4_2019"), header=True, inferSchema=True)
```

Q4 has two extra columns that Q1–Q3 do not. Before unioning all four, the narrower DataFrames must be padded with null columns.

**Book's approach — `withColumn` in a loop:**

```python
q4_fields_extra = set(q4.columns) - set(q1.columns)

for i in q4_fields_extra:
    q1 = q1.withColumn(i, F.lit(None).cast(T.StringType()))
    q2 = q2.withColumn(i, F.lit(None).cast(T.StringType()))
    q3 = q3.withColumn(i, F.lit(None).cast(T.StringType()))
```

Two problems: (1) each `withColumn` adds a separate `Project` node to the logical plan — N extra columns means N nested projections; (2) `StringType()` is hardcoded and may not match Q4's actual type for those columns.

**Preferred — single `select` deriving types from the reference schema:**

```python
extra_cols = [
    F.lit(None).cast(q4.schema[c].dataType).alias(c)
    for c in q4_fields_extra
]

# pad all three quarters in one line
q1, q2, q3 = [df.select("*", *extra_cols) for df in (q1, q2, q3)]

# explicit chain — readable, mirrors the book's union structure
backblaze_2019 = (
    q1.select(q4.columns)
    .union(q2.select(q4.columns))
    .union(q3.select(q4.columns))
    .union(q4.select(q4.columns))
)

# compact alternative using reduce — useful when the number of quarters is dynamic
from functools import reduce
backblaze_2019 = reduce(
    lambda a, b: a.union(b),
    [df.select("*", *extra_cols).select(q4.columns) for df in (q1, q2, q3)] + [q4]
)

# cast all SMART measurement columns to LongType (documented as integral values)
backblaze_2019 = backblaze_2019.select(
    [
        F.col(x).cast(T.LongType()) if x.startswith("smart") else F.col(x)
        for x in backblaze_2019.columns
    ]
)

# register as a SQL view for use with spark.sql()
backblaze_2019.createOrReplaceTempView("backblaze_stats_2019")
```

- `select("*", *extra_cols)` adds all missing columns in **one** projection node.
- `.cast(q4.schema[c].dataType)` derives the type from Q4's actual schema rather than hardcoding.
- `.select(q4.columns)` reorders columns to match Q4's column order — `union()` matches by position, not name.

### 3.1 select and where

```python
# SQL
spark.sql("SELECT serial_number FROM backblaze_stats_2019 WHERE failure = 1").show(5)

# PySpark equivalent
backblaze_2019.where(F.col("failure") == 1).select("serial_number").show(5)
```

### 3.2 GROUP BY, ORDER BY, and HAVING

**Listing 7.8 — grouping and ordering (without HAVING):**

```python
# SQL
spark.sql("""
    SELECT model,
           min(capacity_bytes / pow(1024, 3)) min_GB,
           max(capacity_bytes / pow(1024, 3)) max_GB
    FROM backblaze_stats_2019
    GROUP BY model
    ORDER BY max_GB DESC
""").show(5)

# PySpark equivalent
backblaze_2019.groupby(F.col("model")).agg(
    F.min(F.col("capacity_bytes") / F.pow(F.lit(1024), 3)).alias("min_GB"),
    F.max(F.col("capacity_bytes") / F.pow(F.lit(1024), 3)).alias("max_GB"),
).orderBy(F.col("max_GB"), ascending=False).show(5)
```

**Listing 7.9 — adding HAVING (SQL) / chained `.where()` (PySpark):**

```python
# SQL — HAVING filters on aggregated columns after GROUP BY
spark.sql("""
    SELECT model,
           min(capacity_bytes / pow(1024, 3)) min_GB,
           max(capacity_bytes / pow(1024, 3)) max_GB
    FROM backblaze_stats_2019
    GROUP BY model
    HAVING min_GB != max_GB
    ORDER BY max_GB DESC
""").show(5)

# PySpark — no having() method; chain .where() after .agg()
backblaze_2019.groupby(F.col("model")).agg(
    F.min(F.col("capacity_bytes") / F.pow(F.lit(1024), 3)).alias("min_GB"),
    F.max(F.col("capacity_bytes") / F.pow(F.lit(1024), 3)).alias("max_GB"),
).where(F.col("min_GB") != F.col("max_GB")).orderBy(
    F.col("max_GB"), ascending=False
).show(5)
```

- `HAVING` is SQL-only — it filters aggregated columns post-`GROUP BY`. In PySpark, since every method returns a new DataFrame, a plain `.where()` after `.agg()` is equivalent.

> ⚠️ **Avoid positional column references (`GROUP BY 1`, `ORDER BY 3`)**: if you add or reorder a column in `SELECT`, the positions shift silently and the query produces wrong results without any error. Use explicit names: `GROUP BY model`, `ORDER BY max_GB DESC`.

### 3.3 CREATE OR REPLACE TEMP VIEW

**Listing 7.10 — creating views in SQL and PySpark:**

```python
# SQL — prefix a SELECT with CREATE OR REPLACE TEMP VIEW to materialise it as a named view
backblaze_2019.createOrReplaceTempView("drive_stats")

spark.sql("""
    CREATE OR REPLACE TEMP VIEW drive_days AS
        SELECT model, count(*) AS drive_days
        FROM drive_stats
        GROUP BY model
""")

spark.sql("""
    CREATE OR REPLACE TEMP VIEW failures AS
        SELECT model, count(*) AS failures
        FROM drive_stats
        WHERE failure = 1
        GROUP BY model
""")

# PySpark equivalent — assign to a variable; no extra syntax needed
drive_days = backblaze_2019.groupby(F.col("model")).agg(
    F.count(F.col("*")).alias("drive_days")
)

failures = (
    backblaze_2019.where(F.col("failure") == 1)
    .groupby(F.col("model"))
    .agg(F.count(F.col("*")).alias("failures"))
)
```

- In SQL, `CREATE OR REPLACE TEMP VIEW` stores the query definition as a named view in the session. Querying it later re-executes the underlying `SELECT`.
- In PySpark, a DataFrame variable is the direct equivalent — no `CREATE` syntax, no `DROP` needed.
- **`CREATE TABLE` vs `CREATE VIEW`**: with a metastore connected, `CREATE TABLE` materialises data to disk; `CREATE VIEW` stores only the query definition. Without a metastore, both behave as temp views.

### 3.4 UNION ALL and JOIN

**Listing 7.11 — unioning quarterly tables in SQL and PySpark:**

```python
# SQL — register each quarter as a view, then union using a column list string
columns_backblaze = ", ".join(q4.columns)   # comma-separated string of all column names

q1.createOrReplaceTempView("Q1")
q2.createOrReplaceTempView("Q2")
q3.createOrReplaceTempView("Q3")
q4.createOrReplaceTempView("Q4")

spark.sql("""
    CREATE OR REPLACE TEMP VIEW backblaze_2019 AS
    SELECT {col} FROM Q1 UNION ALL
    SELECT {col} FROM Q2 UNION ALL
    SELECT {col} FROM Q3 UNION ALL
    SELECT {col} FROM Q4
""".format(col=columns_backblaze))

# PySpark equivalent
backblaze_2019 = (
    q1.select(q4.columns)
    .union(q2.select(q4.columns))
    .union(q3.select(q4.columns))
    .union(q4)
)
```

> ⚠️ **`union()` ≠ SQL `UNION`**: PySpark's `union()` keeps duplicates — it is equivalent to SQL's `UNION ALL`. SQL's plain `UNION` deduplicates. To deduplicate after a PySpark union, call `.distinct()` explicitly (it is expensive in a distributed context).

Before unioning, ensure all DataFrames have the same columns, in the same order, with the same types. In PySpark: `df.select(reference_df.columns)`. SQL has no column-list shorthand — you must list every column explicitly.

**Listing 7.12 — joining `drive_days` and `failures` in SQL and PySpark:**

```python
# SQL
spark.sql("""
    SELECT drive_days.model, drive_days, failures
    FROM drive_days
    LEFT JOIN failures
    ON drive_days.model = failures.model
""").show(5)

# PySpark equivalent
drive_days.join(failures, on="model", how="left").show(5)
```

> ⚠️ **SQL injection when building SQL strings from Python**: constructing `spark.sql()` arguments by interpolating user-controlled values is a SQL injection risk. Only interpolate trusted, internal data (e.g. `df.columns`). Never interpolate raw user input into SQL strings.

### 3.5 Subqueries and CTEs

**Listing 7.13 — subqueries (SQL only):**

A subquery replaces a table name with a standalone `SELECT` in parentheses, aliased at the end:

```python
spark.sql("""
    SELECT
        failures.model,
        failures / drive_days AS failure_rate
    FROM (
        SELECT model, count(*) AS drive_days
        FROM drive_stats
        GROUP BY model
    ) drive_days
    INNER JOIN (
        SELECT model, count(*) AS failures
        FROM drive_stats
        WHERE failure = 1
        GROUP BY model
    ) failures
    ON drive_days.model = failures.model
    ORDER BY failure_rate DESC
""").show(5)
```

Subqueries work but are hard to read and debug — complexity is buried inside the main query.

**Listing 7.14 — CTEs (SQL only):**

A CTE prefixes named sub-queries with `WITH` before the main `SELECT`. Each CTE can be referenced by name in the query body — equivalent to a `CREATE TEMP VIEW` that is automatically dropped at the end of the statement:

```python
spark.sql("""
    WITH drive_days AS (
        SELECT model, count(*) AS drive_days
        FROM drive_stats
        GROUP BY model),
    failures AS (
        SELECT model, count(*) AS failures
        FROM drive_stats
        WHERE failure = 1
        GROUP BY model)
    SELECT
        failures.model,
        failures / drive_days AS failure_rate
    FROM drive_days
    INNER JOIN failures
    ON drive_days.model = failures.model
    ORDER BY failure_rate DESC
""").show(5)
```

**Listing 7.15 — PySpark equivalent using Python function scope:**

PySpark has no CTE syntax. The idiomatic equivalent is a function — intermediate DataFrames are scoped to the function body and garbage-collected on return. No `DROP VIEW` needed:

**Book's version (no type hints):**

```python
def failure_rate(drive_stats):
    drive_days = drive_stats.groupby(F.col("model")).agg(
        F.count(F.col("*")).alias("drive_days")
    )
    failures = (
        drive_stats.where(F.col("failure") == 1)
        .groupby(F.col("model"))
        .agg(F.count(F.col("*")).alias("failures"))
    )
    answer = (
        drive_days.join(failures, on="model", how="inner")
        .withColumn("failure_rate", F.col("failures") / F.col("drive_days"))
        .orderBy(F.col("failure_rate").desc())
    )
    return answer

failure_rate(backblaze_2019).show(5)
print("drive_days" in dir())   # False — intermediate frames don't leak into outer scope
```

**Preferred — with type annotations:**

```python
from pyspark.sql import DataFrame

def failure_rate(drive_stats: DataFrame) -> DataFrame:
    drive_days = drive_stats.groupby(F.col("model")).agg(
        F.count(F.col("*")).alias("drive_days")
    )
    failures = (
        drive_stats.where(F.col("failure") == 1)
        .groupby(F.col("model"))
        .agg(F.count(F.col("*")).alias("failures"))
    )
    return (
        drive_days.join(failures, on="model", how="inner")
        .withColumn("failure_rate", F.col("failures") / F.col("drive_days"))
        .orderBy(F.col("failure_rate").desc())
    )

failure_rate(backblaze_2019).show(5)
```

Type annotations are better for three reasons:

- **Contract is explicit** — a reader knows immediately that `drive_stats` must be a DataFrame and the function returns one, without reading the body.
- **Static analysis** — `mypy` or IDE type checkers can catch callers passing the wrong type before the job runs on a cluster.
- **Composability** — annotated transformer functions can be chained with confidence: `failure_rate(clean(load(...)))` is verifiable at write time, not only at runtime.

This pattern is still the standard in Spark 4.x — no native CTE equivalent exists in the DataFrame API.

> 💡 **Spark 4.1 — recursive CTEs are now GA.** Recursive `WITH` clauses (e.g. graph traversal, hierarchical data) were previously experimental; they are fully supported in Spark 4.1 via `spark.sql()`. The DataFrame API has no equivalent — use SQL for recursive logic.

**Recursive CTE anatomy — org-hierarchy example:**

```python
# Set up sample data as a temp view using inline VALUES
spark.sql("""
    CREATE OR REPLACE TEMP VIEW employees AS
    SELECT * FROM VALUES
        (1, 'Alice', NULL),
        (2, 'Bob',   1),
        (3, 'Carol', 1),
        (4, 'Dave',  2),
        (5, 'Eve',   2),
        (6, 'Frank', 3)
    AS t(id, name, manager_id)
""")

spark.sql("""
    WITH RECURSIVE org AS (
        -- anchor: seed with the root (no manager)
        SELECT id, name, manager_id, 0 AS depth
        FROM employees
        WHERE manager_id IS NULL
        UNION ALL
        -- recursive: extend by one level on each iteration
        SELECT e.id, e.name, e.manager_id, org.depth + 1
        FROM employees e
        JOIN org ON e.manager_id = org.id
    )
    SELECT depth, name, manager_id
    FROM org
    ORDER BY depth, name
""").show()
```

```
+-----+-----+----------+
|depth| name|manager_id|
+-----+-----+----------+
|    0|Alice|      null|
|    1|  Bob|         1|
|    1|Carol|         1|
|    2| Dave|         2|
|    2|  Eve|         2|
|    2|Frank|         3|
+-----+-----+----------+
```

- Spark iterates the recursive term until it produces no new rows.
- Add a `depth` column in the anchor and increment it in the recursive term to track traversal level.
- Spark enforces a maximum recursion depth and a max-rows limit per query to prevent infinite loops on cyclic graphs. If a recursive query terminates unexpectedly, check for cycles in the data.
- Only `UNION ALL` is supported between anchor and recursive term; `UNION` (deduplicating) is not.

---

## 4. Blending SQL syntax into PySpark

### 4.0 Data ingestion for this section (Listing 7.16)

Listing 7.16 is the author's refactored ingestion — simpler than Listing 7.6 and more resilient to schema drift across files. It produces `full_data`, which the `selectExpr` and `expr` examples in 4.1–4.3 operate on.

```python
from functools import reduce

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

DATA_DIRECTORY = "./data/backblaze/"

DATA_FILES = [
    "data_Q1_2019",   # book has "drive_stats_2019_Q1" — corrected to match actual directory name
    "data_Q2_2019",
    "data_Q3_2019",
    "data_Q4_2019",
]

data = [
    spark.read.option("pathGlobFilter", "*.csv").csv(
        DATA_DIRECTORY + file, header=True, inferSchema=True
    )
    for file in DATA_FILES
]

common_columns = list(
    reduce(lambda x, y: x.intersection(y), [set(df.columns) for df in data])
)

assert set(["model", "capacity_bytes", "date", "failure"]).issubset(
    set(common_columns)
)

full_data = reduce(
    lambda x, y: x.select(common_columns).union(y.select(common_columns)), data
)
```

Three changes vs Listing 7.6:

**1. Directories in a list + list comprehension.** Instead of four separate `spark.read.csv()` calls, all quarterly directory names live in `DATA_FILES`. The list comprehension reads them all in one expression. Adding or removing a quarter means editing one list entry — no code duplication.

**2. Column intersection (first `reduce`).** Rather than finding extra columns and padding with nulls, this takes the *intersection* of all DataFrames' column sets — only the columns that every quarter shares. The SMART measurement columns that differ across quarters are dropped automatically. This is the key trade-off vs Listing 7.6:

| Approach | Extra columns (exist in some quarters) | Missing required columns |
|---|---|---|
| Listing 7.6 — pad missing with nulls | Preserved (as `null`) | Silent — nulls appear downstream |
| Listing 7.16 — intersect | Dropped | Caught by `assert` immediately |

Use Listing 7.6's approach when you need to preserve columns that are absent in some files. Use Listing 7.16's approach when you only care about the common core and want schema drift to be handled automatically.

**3. `assert` for early failure.** After intersection, the `assert` verifies that the four columns the analysis depends on (`model`, `capacity_bytes`, `date`, `failure`) survived the intersection. If one is missing, the program raises `AssertionError` immediately rather than a cryptic `AnalysisException` deep in a downstream transformation.

**Second `reduce` — union over common columns.** `reduce` applies the binary `lambda` left-to-right: `data[0].union(data[1])`, then `.union(data[2])`, then `.union(data[3])`. Each side is projected to `common_columns` first so column order is consistent. Adding a fifth quarter means appending one string to `DATA_FILES` — the rest is unchanged.

---

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

## 5. Summary

- PySpark shares vocabulary with SQL but the order of operations is reversed: PySpark chains transformations; SQL separates operations / target / conditions.
- Register a DataFrame for SQL with `createOrReplaceTempView("name")`; always use the `OrReplace` variant.
- `spark.sql("...")` returns a DataFrame. Manage views via `spark.catalog`.
- SQL's `HAVING` = PySpark's `.where()` chained after `.agg()`. SQL's `WITH` (CTE) = Python function scope.
- PySpark's `union()` keeps duplicates (= SQL `UNION ALL`). SQL's `UNION` deduplicates.
- `selectExpr()`, `F.expr()`, and `where(str)` accept SQL expressions — convenient shortcuts, but prefer explicit `.alias()` and Column expressions for clarity, consistency, and safety.
- Never interpolate user-supplied values into SQL strings — use Column expressions (`.between()`, `==`, etc.) to stay injection-safe.

---

## 6. References

- Spark SQL API reference: https://spark.apache.org/docs/latest/api/sql/index.html
- Backblaze hard-drive data: https://www.backblaze.com/cloud-storage/resources/hard-drive-test-data
- OWASP SQL Injection: https://owasp.org/www-community/attacks/SQL_Injection
- Python `functools.reduce`: https://docs.python.org/3/library/functools.html#functools.reduce
- Spark SQL programming guide: https://spark.apache.org/docs/latest/sql-programming-guide.html
