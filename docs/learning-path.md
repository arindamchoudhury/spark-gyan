# Learning Path: Apache Spark / PySpark

> **Last updated:** 2026-05-31 · **Current Spark stable:** 4.1.2
>
> **How to read this page.** Topics are grouped by level — Beginner → Intermediate → Advanced → Expert. Each topic lists what it is, why it matters, and exactly which resources to use and in what order. Books, MOOCs, university courses, official docs, and certifications are all included. Pick the level where you currently are and work through the topics in sequence within that level.

---

## Resources at a glance

These are the sources cited throughout this page. Abbreviations are used inline.

| Abbrev | Full name | Type | URL |
|---|---|---|---|
| **Rioux** | *Data Analysis with Python and PySpark* — Rioux (Manning, 2022) | Book (in this project) | — |
| **LS2e** | *Learning Spark, 2nd Ed.* — Damji et al. (O'Reilly, 2020) | Book | [O'Reilly](https://www.oreilly.com/library/view/learning-spark-2nd/9781492050032/) |
| **SDG** | *Spark: The Definitive Guide* — Chambers & Zaharia (O'Reilly, 2018) | Book | [O'Reilly](https://www.oreilly.com/library/view/spark-the-definitive/9781491912201/) |
| **DLUR** | *Delta Lake: Up and Running* — Haelen & Davis (O'Reilly, 2023) | Book | [O'Reilly](https://www.oreilly.com/library/view/delta-lake-up/9781098139711/) |
| **DLDG** | *Delta Lake: The Definitive Guide* — Lee et al. (O'Reilly, 2024) | Book | [O'Reilly](https://www.oreilly.com/library/view/delta-lake-the/9781098151935/) |
| **FKane** | *Taming Big Data with Apache Spark 4 and Python — Hands On!* — Frank Kane | Udemy | [Udemy](https://www.udemy.com/course/taming-big-data-with-apache-spark-hands-on/) |
| **IBM-Spark** | *Apache Spark for Data Engineering and ML* — IBM | edX / Coursera | [edX](https://www.edx.org/learn/apache-spark/ibm-apache-spark-for-data-engineering-and-machine-learning) |
| **IBM-ML** | *Scalable Machine Learning on Big Data using Apache Spark* — IBM | Coursera | [Coursera](https://www.coursera.org/learn/machine-learning-big-data-apache-spark) |
| **DEB** | *Data Engineering with Databricks* — Databricks Academy | Official course | [Databricks](https://www.databricks.com/training/catalog/data-engineering-with-databricks-911) |
| **ADEB** | *Advanced Data Engineering with Databricks* — Databricks Academy | Official course | [Databricks](https://www.databricks.com/training/catalog/advanced-data-engineering-with-databricks-971) |
| **DagEss** | *Dagster Essentials* — Dagster Academy | Free course | [dagster.io](https://courses.dagster.io/courses/dagster-essentials) |
| **Spark-docs** | Apache Spark 4.1.2 official documentation | Official docs | [spark.apache.org](https://spark.apache.org/docs/latest/) |
| **Delta-docs** | Delta Lake official documentation | Official docs | [docs.delta.io](https://docs.delta.io/latest/) |

---

## Certifications

Three credentials worth knowing about — used as milestones at the end of each level.

| Cert | Level | Topics tested | Fee | When to attempt |
|---|---|---|---|---|
| **Databricks Associate Developer for Apache Spark** | Intermediate→Advanced | DataFrame API 30%, Architecture 20%, Spark SQL 20%, Tuning 10%, Streaming 10%, Spark Connect 5%, pandas on Spark 5% | $200 | After Intermediate |
| **Databricks Data Engineer Associate** | Intermediate→Advanced | Ingestion, ETL/PySpark, Delta Lake, Lakeflow Jobs, Unity Catalog, monitoring | $200 | After Intermediate + Delta Lake |
| **Databricks Data Engineer Professional** | Advanced→Expert | Advanced pipelines, performance optimisation 13%, security/governance 10%, CDC, debugging, data modelling | $200 | After Advanced |

---

## Beginner

**Goal:** Understand what Spark is and why it exists. Write correct PySpark programs that read, transform, and write data. Use the DataFrame API fluently.

**Estimated time to complete this level:** 30–40 hrs

---

### ✅ B1 — Spark Architecture & the Execution Model

**What it is:** The mental model of how Spark distributes work — driver, executors, cluster manager, JVM vs Python process, lazy evaluation, DAG, stages, tasks.

**Why you need it:** Every debugging and optimisation decision later depends on knowing what is happening physically. Without this, you are guessing.

**Learn it with:**

1. **Rioux Ch 1–3** — builds the model from scratch with diagrams; the best prose introduction
2. **LS2e Ch 1–2** — covers the same ground with more technical depth on the execution model
3. **Spark-docs → Overview** ([spark.apache.org/docs/latest/](https://spark.apache.org/docs/latest/)) — skim once; return as reference

**Milestone:** You can explain (without notes) what happens between `spark.read.parquet(...)` and `.show()` — where the plan lives, when it executes, and which process runs the Python code.

---

### ✅ B2 — SparkSession and Entry Points

**What it is:** Creating a `SparkSession`; configuring the application; log levels; local vs cluster mode; the REPL vs script context.

**Why you need it:** Every PySpark program starts here. Understanding modes prevents "why does this work in notebook but not spark-submit" surprises.

**Learn it with:**

1. **Rioux Ch 2** — covers setup, configuration, and the SparkSession builder pattern
2. **FKane** — first two sections; shows the setup in a runnable environment you can follow along

**Milestone:** You can create a SparkSession with custom config, set the log level, and run a script with `spark-submit`.

---

### ✅ B3 — The DataFrame API: Basics

**What it is:** `select`, `filter`/`where`, `withColumn`, `drop`, `rename`, `distinct`, `show`, `printSchema`, `dtypes`, `describe`.

**Why you need it:** The primary tool for 90% of PySpark work. Everything else is built on top of it.

**Learn it with:**

1. **Rioux Ch 2, 4** — the clearest beginner introduction to the DataFrame API
2. **LS2e Ch 3** — adds the Catalyst/Tungsten context; explains *why* the API works the way it does
3. **DataCamp: Introduction to PySpark** ([datacamp.com](https://www.datacamp.com/courses/introduction-to-pyspark)) — ~4 hrs; interactive browser exercises; good for checking comprehension

**Milestone:** You can take a raw CSV, select specific columns, filter rows, add derived columns, and write the result to Parquet — all in a single method-chained program.

---

### ✅ B4 — Reading and Writing Data

**What it is:** SparkReader (`spark.read`) and SparkWriter (`df.write`) for CSV, JSON, Parquet, and ORC; options, modes, schema inference vs declaration.

**Why you need it:** Every pipeline starts with a read and ends with a write. Understanding format trade-offs (row vs columnar) sets up performance intuition.

**Learn it with:**

1. **Rioux Ch 2–3** — reading CSV with options; writing with modes
2. **LS2e Ch 4** — comprehensive treatment of all built-in sources (Parquet, JSON, CSV, Avro, ORC, binary, images)
3. **SDG Ch 9** — the deepest coverage of every data source option

**Milestone:** You can read multi-file datasets with glob patterns, declare a schema programmatically with `StructType`, write in append/overwrite mode, and explain why Parquet is preferred for analytical workloads.

---

### ✅ B5 — Schema: StructType, DDL Strings, and Type Safety

**What it is:** `StructType` / `StructField` schema objects; DDL shorthand strings; `inferSchema` trade-offs; checking schema at runtime.

**Why you need it:** Schema mismatches are the #1 source of silent data corruption in Spark pipelines. Explicit schemas are the fix.

**Learn it with:**

1. **Rioux Ch 4** — introduces schema definition in context of CSV ingestion
2. **Rioux Ch 6** — `StructType` for JSON and nested schemas
3. **Spark-docs → SQL Data Types** ([spark.apache.org/docs/latest/sql-ref-datatypes.html](https://spark.apache.org/docs/latest/sql-ref-datatypes.html)) — canonical type reference

**Milestone:** You can define a schema without `inferSchema`, validate that incoming data matches it, and explain the cost of `inferSchema` on large files.

---

### ✅ B6 — Basic Aggregations and GroupBy

**What it is:** `groupBy().agg()`, built-in aggregate functions (`F.count`, `F.sum`, `F.avg`, `F.min`, `F.max`, `F.countDistinct`), `GroupedData`.

**Why you need it:** Aggregations are the core of analytical workloads. The `groupBy().agg()` pattern appears in every pipeline.

**Learn it with:**

1. **Rioux Ch 3, 5** — covers groupby, agg, and the GroupedData intermediate object
2. **LS2e Ch 4** — adds `F.expr()`, SQL aggregations, and the full function catalogue

**Milestone:** You can compute multiple aggregations in a single `agg()` call, use `F.when()` for conditional counting, and write a query equivalent to a SQL `GROUP BY ... HAVING`.

---

### ✅ B7 — Joins: Types and Mechanics

**What it is:** Inner, left, right, full outer, semi, anti joins; equi-join shorthand; column disambiguation; broadcast join hint.

**Why you need it:** Joins are the most common source of performance problems in Spark. Understanding the types is the foundation for fixing those problems later.

**Learn it with:**

1. **Rioux Ch 5** — all join types with visual diagrams; column-clash solutions
2. **LS2e Ch 4** — join in the context of SQL tables and views
3. **SDG Ch 8** — the most comprehensive treatment of join mechanics, including physical strategies

**Milestone:** You can perform all seven join types, explain what `left_semi` and `left_anti` return without looking it up, and name three situations where a broadcast join is appropriate.

---

### ✅ B8 — Spark SQL

**What it is:** `createOrReplaceTempView`, `spark.sql()`, SQL string expressions in `selectExpr`/`F.expr`, the Spark catalog.

**Why you need it:** SQL is often cleaner for complex transformations. Knowing when to use DataFrame API vs SQL — and how to mix them — is a practical skill.

**Learn it with:**

1. **Rioux Ch 7** — dedicated chapter on PySpark/SQL bilingual programming
2. **LS2e Ch 4** — SQL tables, views, and the catalog API
3. **Spark-docs → SQL Guide** ([spark.apache.org/docs/latest/sql-programming-guide.html](https://spark.apache.org/docs/latest/sql-programming-guide.html))

**Milestone:** You can register a DataFrame as a temp view, query it with `spark.sql()`, and mix SQL expressions into a method-chained DataFrame pipeline.

---

### ✅ B9 — Null Handling

**What it is:** `dropna`, `fillna`, `coalesce`, null-safe equality (`<=>` / `eqNullSafe`), how nulls propagate through aggregations and joins.

**Why you need it:** Real data has nulls everywhere. Getting this wrong silently drops rows or produces wrong aggregates.

**Learn it with:**

1. **Rioux Ch 5** — `dropna`/`fillna` with `how`, `thresh`, and `subset`
2. **SDG Ch 6** — null semantics, null-safe joins, and null coercion rules

**Milestone:** You can explain why `F.count("col")` and `F.count("*")` return different results for a column with nulls.

---

### ✅ Beginner Checkpoint

You are ready to leave this level when you can build a complete end-to-end batch pipeline:

- Read multi-source data (CSV + Parquet)
- Clean (null handling, type casting, deduplication)
- Transform (join, group, aggregate, derive columns)
- Write output to Parquet with a sensible partition scheme

---

---

## Intermediate

**Goal:** Work confidently with complex data structures, window functions, UDFs, and the Delta Lake table format. Begin reading Spark execution plans. Write pipelines that don't fall over on real data.

**Estimated time to complete this level:** 35–50 hrs

---

### ✅ I1 — Complex Column Types: Arrays, Maps, Structs

**What it is:** `ArrayType`, `MapType`, `StructType` as column values; `F.explode`, `F.posexplode`, `F.explode_outer`; array functions (`F.array_contains`, `F.size`, `F.array_distinct`); struct dot notation; `collect_list`/`collect_set`.

**Why you need it:** JSON data, event logs, and nested schemas are ubiquitous. This is the difference between working with 80% of real data and only 20% of it.

**Learn it with:**

1. **Rioux Ch 6** — the most thorough beginner treatment of all three complex types
2. **LS2e Ch 5** — higher-order functions (`TRANSFORM`, `FILTER`, `AGGREGATE` on arrays) — very useful in practice
3. **SDG Ch 6** — working with all data types; the most complete reference

**Milestone:** You can flatten a JSON array-of-structs into rows, extract fields from nested structs, build an array column from grouped rows, and apply a lambda transform to every element of an array column.

---

### ✅ I2 — Window Functions

**What it is:** `Window.partitionBy().orderBy()`, aggregate functions over windows, ranking functions (`rank`, `dense_rank`, `percent_rank`, `ntile`, `row_number`), analytic functions (`lag`, `lead`, `cume_dist`), frame boundaries (`rowsBetween`, `rangeBetween`).

**Why you need it:** Time-series features, running totals, ranking, deduplication keeping only the latest record — window functions handle all of these in one pass without a self-join.

**Learn it with:**

1. **Rioux Ch 10** — the clearest full chapter introduction to all window function types
2. **LS2e Ch 5** — window functions in the context of SQL and DataFrame APIs
3. **SDG Ch 7** — aggregations chapter includes window functions with the deepest semantic explanations

**Milestone:** You can reproduce a self-join using a window function, explain why an ordered aggregate window produces different results than an unordered one, and build a 30-day rolling average using `rangeBetween` on a unix timestamp.

---

### ✅ I3 — User-Defined Functions

**What it is:** `@F.udf` (row-by-row Python UDF); `@F.pandas_udf` (vectorised Series→Series, Iterator→Iterator); the performance hierarchy; `.func` for local testing.

**Why you need it:** When no built-in function covers your logic, UDFs are the escape hatch. Knowing the cost of each type determines which one to reach for.

**Learn it with:**

1. **Rioux Ch 8** — Python UDF full treatment
2. **Rioux Ch 9** — pandas UDF full treatment (Series→Series, Iterator variants, group aggregate, group map)
3. **LS2e Ch 5** — UDF section with Python and SQL interop
4. **IBM-Spark Module 3** — practical ETL + ML pipeline UDFs; hands-on lab

**Milestone:** You can replace a Python UDF with a pandas UDF and measure the speedup; you can load an ML model once per partition using an Iterator UDF; you can test a UDF locally without a SparkSession.

---

### ✅ I4 — RDD Fundamentals

**What it is:** `SparkContext.parallelize`, `map`, `filter`, `reduce`, `flatMap`, `collect`, `take`; when RDDs are still needed vs DataFrames.

**Why you need it:** Needed for tasks that require arbitrary Python objects (not tables), and for understanding what the DataFrame API is built on.

**Learn it with:**

1. **Rioux Ch 8** — RDD introduction alongside UDFs
2. **LS2e Ch 3** — RDD vs DataFrame trade-offs explained
3. **SDG Ch 12–13** — the deepest treatment of RDDs and advanced patterns (accumulators, broadcast variables)
4. **FKane** — Spark Basics and the RDD Interface section (~2 hrs, hands-on)

**Milestone:** You can explain in one sentence why `reduce` requires a commutative and associative function, and name two real tasks where you would use an RDD instead of a DataFrame.

---

### ✅ I5 — Partitioning: Concepts and Control

**What it is:** Physical partitions vs logical partitions; `repartition(n)`, `coalesce(n)`, `partitionBy(col)` on writes; default shuffle partition count; how partition count affects file output.

**Why you need it:** Wrong partition counts are responsible for most "my job is slow" and "my job wrote 10,000 tiny files" problems.

**Learn it with:**

1. **Rioux Ch 3** — `coalesce` and `repartition` basics
2. **LS2e Ch 7** — scaling Spark for large workloads; partition tuning
3. **Spark-docs → Performance Tuning** ([spark.apache.org/docs/latest/sql-performance-tuning.html](https://spark.apache.org/docs/latest/sql-performance-tuning.html)) — `spark.sql.shuffle.partitions` and AQE partition coalescing

**Milestone:** You can explain the difference between `repartition` and `coalesce`, set `spark.sql.shuffle.partitions` appropriately for your data volume, and write a DataFrame to exactly N files.

---

### ⬜ I6 — Caching and Persistence

**What it is:** `df.cache()`, `df.persist(StorageLevel.*)`, `df.unpersist()`; storage levels; when caching helps vs hurts; `MEMORY_AND_DISK_DESER` as the default.

**Why you need it:** Caching an intermediate DataFrame used multiple times avoids recomputing it. Caching the wrong thing wastes memory and slows everything down.

**Learn it with:**

1. **LS2e Ch 7** — caching section with practical rules
2. **SDG Ch 19** — performance tuning; caching strategy

**Milestone:** You can identify in the Spark UI whether a cached DataFrame is being reused, and name three situations where caching makes a job slower.

---

### ⬜ I7 — The Spark UI: Reading Plans and Diagnosing Jobs

**What it is:** Jobs, stages, tasks; the SQL tab (parsed → analysed → optimised → physical plan); DAG visualisation; shuffle read/write metrics; spill indicators.

**Why you need it:** The Spark UI is your debugger for performance problems. Without it, tuning is guessing.

**Learn it with:**

1. **Rioux Ch 11** — dedicated chapter walking through every tab of the Spark UI
2. **LS2e Ch 7** — Spark UI walkthrough with a concrete slow-job example
3. **ADEB Module 3** (Databricks Performance Optimization) — Spark UI analysis section; practise reading plans on Databricks

**Milestone:** You can open the Spark UI on a running job, locate the most expensive stage, identify whether it involves a sort-merge join or a broadcast join, and read a physical plan to find a pushed-down filter.

---

### ⬜ I8 — Delta Lake Basics

**What it is:** Writing and reading Delta tables; ACID transactions; the transaction log; `DESCRIBE HISTORY`; time travel (`VERSION AS OF`, `TIMESTAMP AS OF`); `VACUUM`; `OPTIMIZE`.

**Why you need it:** Delta Lake is the standard table format for Spark-based data engineering. It replaces plain Parquet for anything that gets updated or that needs data reliability.

**Learn it with:**

1. **DLUR Ch 1–4** — architecture + all basic operations; the best hands-on introduction
2. **LS2e Ch 9** — lakehouse overview; positions Delta Lake alongside Hudi and Iceberg
3. **Delta-docs Quickstart** ([docs.delta.io/latest/quick-start.html](https://docs.delta.io/latest/quick-start.html)) — run against your local stack first

**Milestone:** You can create a Delta table, insert/update/delete rows, query a past version, run `OPTIMIZE`, and explain what the `_delta_log/` directory contains.

---

### ⬜ I9 — The Medallion Architecture

**What it is:** Bronze (raw ingest) → Silver (cleaned, typed) → Gold (aggregated, business-ready); schema enforcement at each layer; MERGE INTO for upserts; incremental processing.

**Why you need it:** The medallion pattern is the standard architecture for lakehouses. Every data engineering job description assumes familiarity with it.

**Learn it with:**

1. **DLUR Ch 1** — introduces the medallion concept in the lakehouse context
2. **DEB Module 1** — data ingestion into bronze with Auto Loader; CTAS, COPY INTO, MERGE INTO patterns
3. **DLDG Ch 9** — architecting a lakehouse; design decisions at each layer

**Milestone:** You can build a three-layer pipeline from raw Parquet files to a Gold aggregation table, with schema enforcement on silver, using your local Unity Catalog stack.

---

### ⬜ I10 — Data Formats: Parquet, Delta, Avro, JSON

**What it is:** Columnar vs row storage; predicate pushdown; column pruning; Parquet row groups and page footers; when to use each format.

**Why you need it:** Format choice is a major performance variable. The Catalyst optimizer exploits Parquet metadata — but only if the file is written correctly.

**Learn it with:**

1. **LS2e Ch 4** — data sources and format comparison
2. **SDG Ch 9** — the most complete treatment of every format option
3. **DLDG Ch 1** — how Delta wraps Parquet and what the transaction log adds

**Milestone:** You can explain why `F.col("date") > '2024-01-01'` on a Parquet file can be resolved without reading any data, and why the same filter on a CSV cannot.

---

### ⬜ I11 — SQL Scripting

**What it is:** Multi-statement SQL scripts with procedural constructs: `BEGIN...END` compound bodies, local variable declarations (`DECLARE`, `SET`), `IF...THEN...ELSIF...ELSE`, `CASE` (searched and simple), `WHILE`, `FOR`, `LOOP`, `REPEAT...UNTIL`, and `LEAVE`/`ITERATE` for loop control. New in Spark 4.0.

**Why you need it:** SQL scripting lets you express multi-step procedural logic — conditional branches, loops, intermediate variables — entirely in SQL without switching to Python. Useful for complex ETL stored as SQL scripts and for interoperability with data warehouses that already use procedural SQL.

**Learn it with:**

1. **Spark-docs → SQL Scripting** ([spark.apache.org/docs/latest/sql-scripting.html](https://spark.apache.org/docs/latest/sql-scripting.html)) — the canonical reference; covers all statement types with examples
2. **Spark 4.0 release notes** — understand which constructs were added in 4.0 vs 4.1

**Milestone:** You can write a SQL script that declares a variable, iterates over a cursor with `FOR`, applies a conditional with `IF...ELSIF`, and produces a result — and explain when you would choose SQL scripting over a Python pipeline.

---

### ✅ Intermediate Checkpoint + Certification

You are ready to leave this level when you can:

- Build a medallion pipeline with MERGE INTO upserts
- Use window functions for time-series feature engineering
- Read a Spark UI physical plan and locate the bottleneck
- Write and test a pandas UDF

**Certification target:** Databricks Certified Associate Developer for Apache Spark — validates topics B1–I7.

---

---

## Advanced

**Goal:** Write high-performance, production-grade pipelines. Understand Spark's optimiser deeply enough to fix it when it makes wrong decisions. Handle streaming workloads. Build ML pipelines.

**Estimated time to complete this level:** 40–60 hrs

---

### ⬜ A1 — Query Optimisation: Catalyst and the Physical Plan

**What it is:** Logical plan → analysed plan → optimised plan → physical plan; rule-based optimisations (constant folding, predicate pushdown, projection pruning); cost-based optimisation; `EXPLAIN` output.

**Why you need it:** Knowing what Catalyst does automatically tells you what you do NOT need to do manually — and what you need to force when it gets it wrong.

**Learn it with:**

1. **LS2e Ch 3** — Catalyst and Tungsten overview
2. **SDG Ch 4** — Structured API internals; how plans are built
3. **Rioux Ch 11** — the SQL tab of the Spark UI shows the physical plan; reading it after Ch 11's walkthrough makes both stick
4. **Spark-docs → SQL Performance Tuning** — `EXPLAIN EXTENDED`, join hints, AQE config

**Milestone:** You can generate `EXPLAIN(true, true)` output for a query, identify which stage performs the shuffle, and verify that a filter was pushed below a join in the physical plan.

---

### ⬜ A2 — Adaptive Query Execution (AQE)

**What it is:** Dynamic partition coalescing (reduces post-shuffle partitions automatically); dynamic broadcast join conversion (upgrades sort-merge to broadcast at runtime if a side is small enough); skew join handling (splits skewed partitions).

**Why you need it:** AQE is on by default in Spark 3.0+ and handles cases that static planning gets wrong. Knowing what it does prevents you from adding manual hints that fight it.

**Learn it with:**

1. **LS2e Ch 12** — Spark 3.0 features; AQE is the headline item
2. **Spark-docs → AQE** ([spark.apache.org/docs/latest/sql-performance-tuning.html#adaptive-query-execution](https://spark.apache.org/docs/latest/sql-performance-tuning.html#adaptive-query-execution))
3. **ADEB Module 3** — performance optimisation module; AQE in practice on real workloads

**Milestone:** You can enable/disable AQE on a specific query, verify in the Spark UI whether AQE coalesced partitions, and name a case where you would turn AQE off for a specific query.

---

### ⬜ A3 — Join Strategies and Tuning

**What it is:** Broadcast hash join (small-large); sort-merge join (large-large); shuffle-hash join (medium tables, no sort); `BROADCAST`, `MERGE`, `SHUFFLE_HASH` hints; `spark.sql.autoBroadcastJoinThreshold`; skew joins.

**Why you need it:** Join choice is the single biggest driver of job performance. A misconfigured sort-merge join on a broad table can be 100× slower than a broadcast join.

**Learn it with:**

1. **LS2e Ch 7** — join strategies section; when each is used
2. **SDG Ch 8** — the most thorough treatment of join mechanics and join hints
3. **Spark-docs → Join Hints** ([spark.apache.org/docs/latest/sql-performance-tuning.html#join-strategy-hints-for-sql-queries](https://spark.apache.org/docs/latest/sql-performance-tuning.html#join-strategy-hints-for-sql-queries))
4. **ADEB Module 3** — skew join optimisation; data skew patterns

**Milestone:** You can look at a query's physical plan, identify the join strategy, force a broadcast join on a table below the auto-broadcast threshold, and handle a skewed join key with salting.

---

### ⬜ A4 — Data Skew and Shuffle Optimisation

**What it is:** Why some partitions take 10× longer than others; salting keys; `SKEW HINT`; shuffle partition tuning; `spark.sql.shuffle.partitions`; spill to disk.

**Why you need it:** Data skew is the most common cause of slow Spark jobs and OOM errors in production.

**Learn it with:**

1. **ADEB Module 3** — managing skew and shuffles; the most practical treatment
2. **LS2e Ch 7** — scaling for large workloads; shuffle management
3. **SDG Ch 19** — performance tuning; shuffle configuration

**Milestone:** You can diagnose a skewed stage from the Spark UI task-time histogram, apply a salting strategy, and measure the improvement.

---

### ⬜ A5 — Advanced pandas UDFs and UDFs on Windows

**What it is:** Group aggregate UDF (Series→scalar) used in `.agg()` and over window specs; group map UDF (`applyInPandas`); Iterator of multiple Series; bounded vs unbounded window UDFs (Spark 3.0+).

**Why you need it:** When window functions alone can't express your logic (e.g., custom statistical models per group), pandas UDFs over windows fill the gap.

**Learn it with:**

1. **Rioux Ch 9–10** — pandas UDFs + window functions; the combination in §10.4
2. **LS2e Ch 11** — distributed ML inference using pandas UDFs
3. **Spark-docs → pandas UDF** ([spark.apache.org/docs/latest/api/python/user_guide/sql/arrow_pandas.html](https://spark.apache.org/docs/latest/api/python/user_guide/sql/arrow_pandas.html))

**Milestone:** You can apply a custom rolling-median UDF over an ordered window using a pandas UDF, and load an ML model once per executor partition using an Iterator UDF.

---

### ⬜ A6 — Delta Lake Advanced Operations

**What it is:** Schema evolution (`mergeSchema`, `overwriteSchema`); schema enforcement; `MERGE INTO` for SCD Type 1 and Type 2; Z-ordering; liquid clustering; deletion vectors; Change Data Feed (CDF).

**Why you need it:** These are the features that make Delta Lake production-ready — upserts, slowly changing dimensions, and query-level data skipping.

**Learn it with:**

1. **DLUR Ch 4–5** — MERGE INTO + Z-ordering/OPTIMIZE in full detail
2. **DLDG Ch 8** — advanced features: deletion vectors, row-level concurrency, column mapping
3. **DLDG Ch 10** — performance tuning: liquid clustering internals and when to use it vs Z-order
4. **DEB Module 1** — CDC using `AUTO CDC INTO`; SCD Type 1 and Type 2 via Lakeflow Declarative Pipelines

**Milestone:** You can implement a full SCD Type 2 merge, enable liquid clustering on a table, and explain the difference between deletion vectors and copy-on-write for point deletes.

---

### ⬜ A7 — Structured Streaming: Fundamentals

**What it is:** The micro-batch execution model; input sources (file, Kafka, socket); output sinks (Delta, memory, console, Kafka); output modes (append, update, complete); triggers; checkpointing; fault tolerance.

**Why you need it:** Near-real-time pipelines are now a core data engineering requirement. Structured Streaming integrates with the same DataFrame API you already know.

**Learn it with:**

1. **LS2e Ch 8** — best practical introduction; streaming to Delta sinks
2. **Spark-docs → Structured Streaming** ([spark.apache.org/docs/latest/streaming/index.html](https://spark.apache.org/docs/latest/streaming/index.html)) — official reference; reorganised into modular pages in Spark 4.0
3. **DEB Module 1** — Auto Loader as a streaming file source into Delta (production pattern)

**Milestone:** You can write a streaming job that reads new Parquet files from a directory, applies a transformation, and appends results to a Delta table — and restart it from a checkpoint without data loss.

---

### ⬜ A8 — Structured Streaming: Stateful Processing

**What it is:** Event time vs processing time; watermarking for late data; tumbling, sliding, and session windows; stateful aggregations; streaming joins; `flatMapGroupsWithState` for arbitrary stateful logic.

**Why you need it:** Real streaming workloads have late-arriving events. Without watermarks, your state store grows unbounded and the job eventually OOMs.

**Learn it with:**

1. **SDG Ch 22** — event-time and stateful processing; the most rigorous treatment of watermark semantics
2. **SDG Ch 23** — streaming in production; checkpointing, restart strategies, triggers
3. **Spark-docs → Streaming** — watermark section and state store configuration
4. **LS2e Ch 8** — stateful aggregations and streaming joins

**Milestone:** You can implement a session-windowed aggregate with a watermark, explain what happens to a late event that arrives after the watermark threshold, and describe what is stored in the checkpoint directory.

---

### ⬜ A9 — ML Pipelines with Spark MLlib

**What it is:** `Transformer` / `Estimator` / `Pipeline` API; feature engineering (imputers, scalers, encoders, vectorisers); `CrossValidator` and `TrainValidationSplit`; model persistence; `PipelineModel`.

**Why you need it:** MLlib's Pipeline API makes reproducible ML at scale possible — the same abstraction scikit-learn uses, but distributed.

**Learn it with:**

1. **Rioux Ch 12–14** — full treatment from feature prep through custom transformers
2. **LS2e Ch 10–11** — end-to-end pipeline example + MLflow experiment tracking
3. **IBM-ML** (Coursera, ~8 hrs) — regression, classification, clustering, and pipelines with hands-on labs
4. **Spark-docs → MLlib Guide** ([spark.apache.org/docs/latest/ml-guide.html](https://spark.apache.org/docs/latest/ml-guide.html)) — full transformer/estimator catalogue

**Milestone:** You can build a `Pipeline` that imputes nulls, scales features, assembles a vector, trains a logistic regression, and finds the best hyperparameters with `CrossValidator` — then save and reload the fitted `PipelineModel`.

---

### ⬜ A10 — Testing PySpark Pipelines

**What it is:** Unit testing with `pytest` and a local `SparkSession`; testing transformations in isolation; integration testing; `chispa` for DataFrame equality assertions; testing UDFs via `.func`.

**Why you need it:** Untested pipelines break silently in production. A unit test suite takes minutes to run and catches most schema and logic errors before deployment.

**Learn it with:**

1. **DEB Module 4** — unit tests with pytest for PySpark; integration tests with DLT
2. **SDG Ch 16** — developing Spark applications; testing patterns
3. `chispa` library docs ([github.com/MrPowers/chispa](https://github.com/MrPowers/chispa)) — DataFrame equality assertions

**Milestone:** You can write a pytest test that creates a local SparkSession, runs a transformation function, and asserts the output DataFrame matches an expected schema and row set.

---

### ⬜ A11 — Spark Declarative Pipelines

**What it is:** A Python/SQL framework (new in Spark 4.1, runs over Spark Connect) for defining data pipelines as graphs of `MaterializedView`, `StreamingTable`, and `TemporaryView` outputs connected by `Flow` and `AutoCdcFlow` definitions. The pipeline engine handles incremental processing, dependency ordering, and restart semantics automatically.

**Why you need it:** Declarative Pipelines is Apache Spark's open-source equivalent of Databricks Delta Live Tables (DLT). It removes the boilerplate of managing incremental state, checkpoints, and pipeline dependencies manually — you declare what each dataset should contain; the engine decides how to compute it.

**Learn it with:**

1. **Spark-docs → Declarative Pipelines** ([spark.apache.org/docs/latest/pipelines.html](https://spark.apache.org/docs/latest/pipelines.html)) — the primary reference; covers `@table`, `@materialized_view`, flows, and `AutoCdcFlow`
2. **Spark 4.1 release notes** — feature scope and current limitations
3. **Local stack** — run a pipeline against your Delta Lake + Unity Catalog setup; the `pyspark.pipelines` module is available in Spark 4.1.x

**Milestone:** You can define a three-node pipeline (raw ingest → cleaned materialized view → aggregated streaming table) using Declarative Pipelines, add an `AutoCdcFlow` for CDC ingestion, and explain how the engine determines execution order from the dependency graph.

---

### ✅ Advanced Checkpoint + Certification

You are ready to leave this level when you can:

- Debug a slow job using the Spark UI and fix the bottleneck
- Build a streaming pipeline with watermarks and Delta sinks
- Implement MERGE INTO with SCD Type 2 logic
- Build and evaluate an ML pipeline with cross-validation

**Certification target:** Databricks Certified Data Engineer Associate — validates I8–A6 plus orchestration.

---

---

## Expert

**Goal:** Architect production data platforms. Understand Spark internals deeply enough to reason about memory, serialisation, and execution without the Spark UI. Build governed, observable, CI/CD-deployed pipelines.

**Estimated time to complete this level:** 40–60+ hrs (ongoing)

---

### ⬜ E1 — Spark Internals: Memory, Execution, and Serialisation

**What it is:** Tungsten memory model (off-heap, binary row format, WSCG — whole-stage code generation); task memory (execution vs storage); serialisation (Kryo vs Java vs Arrow); GC pressure and mitigation.

**Why you need it:** When AQE and join hints don't fix your problem, you need to reason at the memory level. OOM errors without spill indicators usually mean this layer.

**Learn it with:**

1. **SDG Ch 15** — how Spark runs on a cluster; the physical execution model
2. **SDG Ch 19** — performance tuning; full section on serialisation and memory
3. **ADEB Module 3** — serialisation best practices; cluster instance selection
4. **LS2e Ch 3** — Tungsten and WSCG overview

**Milestone:** You can explain the difference between execution memory and storage memory in unified memory management, and name two causes of excessive GC in PySpark that the task memory metrics would surface.

---

### ⬜ E2 — Production Deployment: Cluster Management and Scaling

**What it is:** Cluster managers (YARN, Kubernetes, Databricks, standalone); driver and executor sizing; dynamic allocation; auto-scaling; `spark-submit` configuration; deploy modes (client vs cluster).

**Why you need it:** A job that works on a laptop breaks on a cluster in ways that require understanding how the cluster manager allocates resources.

**Learn it with:**

1. **SDG Ch 15–17** — cluster execution, deploying Spark, resource management
2. **ADEB Module 3** — instance type selection for performance
3. **Spark-docs → Cluster Mode Overview** ([spark.apache.org/docs/latest/cluster-overview.html](https://spark.apache.org/docs/latest/cluster-overview.html))
4. **Spark-docs → Kubernetes** ([spark.apache.org/docs/latest/running-on-kubernetes.html](https://spark.apache.org/docs/latest/running-on-kubernetes.html)) — the direction production Spark is moving

**Milestone:** You can size a Spark cluster for a given workload (number of executors, cores per executor, memory), explain the difference between client and cluster deploy mode, and configure dynamic allocation.

---

### ⬜ E3 — Observability: Monitoring, Alerting, and Logging

**What it is:** Spark History Server; Spark metrics system; structured logging from drivers and executors; custom listeners; alerting on job duration regressions; Spark UI on completed jobs.

**Why you need it:** Production pipelines fail at 3am. Observability is the difference between "we have an alert" and "we found out from an angry user".

**Learn it with:**

1. **SDG Ch 18** — monitoring and debugging; the Spark metrics system
2. **ADEB Module 3** — pipeline event logging; monitoring in the Databricks context
3. **Spark-docs → Monitoring** ([spark.apache.org/docs/latest/monitoring.html](https://spark.apache.org/docs/latest/monitoring.html))

**Milestone:** You can configure a custom Spark listener that emits stage completion metrics to a log sink, and set up an alert that fires when a job's duration exceeds 2× its 7-day moving average.

---

### ⬜ E4 — Delta Lake Internals: Transaction Log, MVCC, and Concurrency

**What it is:** The `_delta_log` JSON commit files; checkpoint files; snapshot isolation; optimistic concurrency control; what happens during concurrent writes; `RESTORE`; `CLONE`.

**Why you need it:** When two jobs write to the same Delta table simultaneously, you need to know which one wins, whether data is lost, and how to recover.

**Learn it with:**

1. **DLDG Ch 1** — the transaction log as a single source of truth; MVCC internals
2. **DLDG Ch 8** — row-level concurrency; deletion vectors; advanced write operations
3. **DLUR Ch 6** — time travel and `RESTORE` in full operational detail

**Milestone:** You can describe what a Delta commit JSON file contains, explain what `VACUUM` removes and why running it too aggressively breaks time travel, and demonstrate resolving a `ConcurrentModificationException` during a concurrent MERGE and INSERT.

---

### ⬜ E5 — Data Governance: Unity Catalog, Lineage, and Security

**What it is:** Unity Catalog three-level namespace (`catalog.schema.table`); column-level access control; row filters; audit logs; data lineage (table-level and column-level); Delta Sharing.

**Why you need it:** Governance requirements are now a baseline in regulated industries. Unity Catalog is the Databricks/OSS answer — and it's in your local stack.

**Learn it with:**

1. **DLDG Ch 12–13** — governance, security, and lineage
2. **ADEB Module 2** — data privacy, PII handling, pseudonymisation, CDF for data deletion propagation
3. **DEB Module 4** — Unity Catalog governance patterns
4. **Databricks Unity Catalog docs** ([docs.databricks.com/data-governance/unity-catalog/](https://docs.databricks.com/data-governance/unity-catalog/))

**Milestone:** You can create a Unity Catalog row filter that restricts a table to rows matching the current user's region, set column-level masking on a PII field, and query the lineage graph to trace which source tables contributed to a gold table.

---

### ⬜ E6 — Pipeline Orchestration with Dagster

**What it is:** Software-defined assets, asset dependencies, `Definitions`, `Resources` (passing `SparkSession`), partitioned assets (incremental processing), schedules, sensors (event-driven triggers), backfills.

**Why you need it:** Ad-hoc Spark scripts are not a data platform. Dagster turns your pipelines into observable, testable, re-runnable assets with lineage.

**Learn it with:**

1. **DagEss** — the full Dagster Essentials course (12 lessons, 6–10 hrs, free); the only correct place to start
2. **Dagster docs → dagster-spark / dagster-pyspark** ([docs.dagster.io](https://docs.dagster.io)) — integration docs for wrapping Spark jobs as assets
3. **DEB Module 2** — Lakeflow Jobs for Databricks-native orchestration (conceptual parallel to Dagster)

**Milestone:** You can wire the entire medallion pipeline (bronze → silver → gold → ML training) as Dagster assets with monthly partition keys, set up a sensor that triggers the silver asset when new bronze files land, and backfill a specific month's data.

---

### ⬜ E7 — CI/CD for Data Engineering

**What it is:** Git branching for data pipelines; unit + integration testing in CI; environment promotion (dev → staging → prod); Databricks Asset Bundles (DABs); GitHub Actions for pipeline deployment; parameterised job configurations.

**Why you need it:** Manual deployment of pipeline changes to production is a reliability and auditability problem. CI/CD for data is now a standard job requirement.

**Learn it with:**

1. **DEB Module 4** — DevOps for data engineering; unit testing with pytest; Git integration; DABs
2. **ADEB Module 4** — advanced CI/CD with DABs, multi-environment variable substitution, GitHub Actions
3. **SDG Ch 16** — developing Spark applications; packaging and submission

**Milestone:** You can set up a GitHub Actions workflow that runs pytest on every PR, blocks merge if tests fail, and deploys the validated pipeline to a staging environment using DABs.

---

### ⬜ E8 — Change Data Capture (CDC) and Slowly Changing Dimensions

**What it is:** CDC patterns (full snapshot, append-only log, change data feed); `MERGE INTO` for SCD Type 1 (upsert) and Type 2 (full history with effective dates); `AUTO CDC INTO` in Lakeflow Pipelines; Delta CDF.

**Why you need it:** Source systems change — rows get updated and deleted. CDC is the standard pattern for propagating those changes through a lakehouse without reprocessing everything.

**Learn it with:**

1. **ADEB Module 1** — CDC review; SCD Type 2 with `AUTO CDC INTO`; quarantine pipelines
2. **DLDG Ch 7** — streaming CDC in and out of Delta Lake; CDF for downstream propagation
3. **DEB Module 1** — MERGE INTO patterns; incremental ingestion strategies

**Milestone:** You can implement a full SCD Type 2 merge that adds `effective_start`, `effective_end`, and `is_current` columns, process deletes via Delta CDF, and explain the difference between `UPDATE` and `MERGE INTO` from a transaction-log perspective.

---

### ⬜ E9 — Spark Connect and the Modern Client Architecture

**What it is:** Spark Connect (Spark 3.4+): a gRPC-based client-server protocol that separates the Python client from the Spark cluster; implications for deployment, security, and local development.

**Why you need it:** Spark Connect is the default mode in Spark 4.x (`pyspark` REPL). Understanding it is required for deploying applications in any modern Spark 4.x environment.

**Learn it with:**

1. **Spark-docs → Spark Connect** ([spark.apache.org/docs/latest/spark-connect-overview.html](https://spark.apache.org/docs/latest/spark-connect-overview.html))
2. **Databricks Spark Associate Cert** — Spark Connect is 5% of the exam; a good forcing function to study it

**Milestone:** You can explain the difference between classic mode and Connect mode, start a local Spark Connect server, connect to it from a Python client, and describe what changes in a UDF when running over Connect.

---

### ✅ Expert Checkpoint + Certification

**Certification target:** Databricks Certified Data Engineer Professional — validates A6–E8.

You are operating at Expert level when you can:

- Design a governed lakehouse from scratch (medallion + Unity Catalog + lineage)
- Debug a production incident using Spark metrics + History Server without the live UI
- Implement CI/CD for a multi-environment pipeline with automated tests
- Architect a streaming CDC pipeline with SCD Type 2 history and exactly-once guarantees

---

---

## Suggested Study Sequence

```
Beginner (B1–B9)          → 30–40 hrs
    ↓
Intermediate (I1–I11)     → 35–50 hrs
    ↓  [Certification: Associate Developer for Apache Spark]
Advanced (A1–A11)         → 40–60 hrs
    ↓  [Certification: Data Engineer Associate]
Expert (E1–E9)            → 40–60+ hrs
    ↓  [Certification: Data Engineer Professional]
```

**You are currently here:** B1–B9 ✅ + I1–I5 ✅ (14/40 topics done). Next: ⬜ I6 — Caching and Persistence.

---

## Sources consulted

- O'Reilly TOCs: *Learning Spark 2e*, *Spark: The Definitive Guide*, *Delta Lake: Up and Running*, *Delta Lake: The Definitive Guide*
- Databricks certification guides: [Associate Spark Developer](https://www.databricks.com/learn/certification/apache-spark-developer-associate), [DE Associate](https://www.databricks.com/learn/certification/data-engineer-associate), [DE Professional](https://www.databricks.com/learn/certification/data-engineer-professional)
- Databricks Academy course catalogues: [Data Engineering with Databricks](https://www.databricks.com/training/catalog/data-engineering-with-databricks-911), [Advanced DE with Databricks](https://www.databricks.com/training/catalog/advanced-data-engineering-with-databricks-971)
- [Dagster Essentials syllabus](https://courses.dagster.io/courses/dagster-essentials)
- [Apache Spark 4.1.2 documentation](https://spark.apache.org/docs/latest/)
- [ProjectPro PySpark roadmap](https://www.projectpro.io/learning-paths/pyspark-roadmap), [DataCamp PySpark guide](https://www.datacamp.com/blog/learn-pyspark)
- IBM Spark courses: [edX](https://www.edx.org/learn/apache-spark/ibm-apache-spark-for-data-engineering-and-machine-learning), [Coursera ML](https://www.coursera.org/learn/machine-learning-big-data-apache-spark)
