# Learning Path: Apache Spark / PySpark

> **Last updated:** 2026-07-18 (taxonomy re-derived from the Spark 4.2.0 feature surface, current job requirements, and the exam guides — rather than from what the available books cover. Spine changed from the Databricks certification track to Apache Spark itself, with the certs demoted to optional milestones; added A12 Kafka, `VARIANT` to I1, Iceberg to I8/I15/E5; de-vendored E5 and E7. Earlier the same day: verified releases and all three cert pages against official sources, 4.1.2 → 4.2.0, and folded 4.2.0 features into B7, B8, I3, I7, I10, A3, A11, E1, E3, E8)
>
> **Current Spark stable:** 4.2.0 (Jul 14 2026) · **Maintenance lines:** 4.1.3, 4.0.4 (Jul 15 2026), 3.5.9 (Jul 16 2026)
>
> Spark 4.2.0 is the third 4.x release — 1,700+ Jira tickets. Learn against 4.2.0; the books below are written against 3.x, so the callouts on each topic mark where they diverge.
>
!!! note "Status key"
    ⬜ not started · ✅ done and current · 🔄 done, but written against an older Spark and now needs revisiting (the topic's callout says what drifted).

    **How to read this page.** Topics are grouped by level — Beginner → Intermediate → Advanced → Expert. Each topic lists what it is, why it matters, and exactly which resources to use and in what order. Pick the level where you currently are and work through the topics in sequence within that level.

**What this path is built around.** Apache Spark itself — the open-source engine, its APIs, and the open formats and tooling around it. Vendor platforms (Databricks, and the certifications built on it) appear as *optional milestones* at the end, not as the spine. Rationale: the transferable skill is the engine and the open ecosystem; platform-specific surfaces change with your employer, and a path organised around one vendor's exam quietly under-weights what the wider market asks for. If you decide to sit those exams, the [optional certification milestones](#optional-certification-milestones) section maps them back onto these topics.

**How to actually use each topic.** Read the milestone *first*, and attempt it from memory before opening any resource. You will mostly fail early on — that is the point; the failed attempt is what makes the subsequent reading stick, and it tells you which parts you can skip. Then read, then attempt the milestone again in writing. Self-explanation and retrieval practice both carry roughly twice the effect size of rereading, and the book chapters in `docs/spark-book/` are where the self-explanation happens.

!!! warning "Topic codes are stable identifiers, not an ordering"
    They are referenced by the book index, the chapter files, and the source-map coverage matrix, so they are never renumbered when the taxonomy changes. A code with a gap or an out-of-sequence number (I15, the I12–I14 depth topics) is normal. Read the level headings for order, not the numbers.

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
| **Iceberg-DG** | *Apache Iceberg: The Definitive Guide* — Shiran, Hughes & Merced (O'Reilly, 2024) | Book | [O'Reilly](https://www.oreilly.com/library/view/apache-iceberg-the/9781098148614/) |
| **FKane** | *Taming Big Data with Apache Spark 4 and Python — Hands On!* — Frank Kane | Udemy | [Udemy](https://www.udemy.com/course/taming-big-data-with-apache-spark-hands-on/) |
| **IBM-Spark** | *Apache Spark for Data Engineering and ML* — IBM | edX / Coursera | [edX](https://www.edx.org/learn/apache-spark/ibm-apache-spark-for-data-engineering-and-machine-learning) |
| **IBM-ML** | *Scalable Machine Learning on Big Data using Apache Spark* — IBM | Coursera | [Coursera](https://www.coursera.org/learn/machine-learning-big-data-apache-spark) |
| **DEB** | *Data Engineering with Databricks* — Databricks Academy | Official course | [Databricks](https://www.databricks.com/training/catalog/data-engineering-with-databricks-911) |
| **ADEB** | *Advanced Data Engineering with Databricks* — Databricks Academy | Official course | [Databricks](https://www.databricks.com/training/catalog/advanced-data-engineering-with-databricks-971) |
| **DagEss** | *Dagster Essentials* — Dagster Academy | Free course | [dagster.io](https://courses.dagster.io/courses/dagster-essentials) |
| **Spark-docs** | Apache Spark 4.2.0 official documentation | Official docs | [spark.apache.org](https://spark.apache.org/docs/latest/) |
| **Delta-docs** | Delta Lake official documentation | Official docs | [docs.delta.io](https://docs.delta.io/latest/) |
| **Iceberg-docs** | Apache Iceberg official documentation (1.11.0) | Official docs | [iceberg.apache.org](https://iceberg.apache.org/docs/latest/) |

---

## What a 2026 Spark data engineer is actually asked for

The taxonomy below is derived from three sources, not from what the available books happen to cover: the Spark 4.2.0 feature surface (what the engine now does), current job requirements, and the certification exam guides.

Where they agree — the DataFrame API, SQL, joins, partitioning, streaming, and performance tuning — the path spends most of its time. Where the market asks for something the books do not cover, that gap is marked rather than hidden:

| Market signal | Where it lands here |
|---|---|
| Open table formats (Iceberg increasingly the default; Delta where Databricks is in play) | I8 fundamentals, I15 depth and interop |
| Kafka as the standard event backbone | A12, and as a source throughout A7/A8 |
| Semi-structured data at scale (`VARIANT`, new in Spark 4.0) | I1 |
| Kubernetes as the deployment target | E2 |
| Spark Connect as the default client architecture in 4.x | B2 basics, E9 depth |
| Declarative pipelines replacing hand-rolled orchestration glue | A11 |
| SQL fluency weighted at least as heavily as Python | B8, I11 |

---

---

## Beginner

**Goal:** Understand what Spark is and why it exists. Write correct PySpark programs that read, transform, and write data. Use the DataFrame API fluently.

**Estimated time to complete this level:** 30–40 hrs

---

### 🔄 B1 — Spark Architecture & the Execution Model

**What it is:** The mental model of how Spark distributes work — driver, executors, cluster manager, JVM vs Python process, lazy evaluation, DAG, stages, tasks.

**Why you need it:** Every debugging and optimisation decision later depends on knowing what is happening physically. Without this, you are guessing.

**Learn it with:**

1. **Rioux Ch 1–3** — builds the model from scratch with diagrams; the best prose introduction
2. **LS2e Ch 1–2** — covers the same ground with more technical depth on the execution model
3. **Spark-docs → Cluster Mode Overview** ([cluster-overview.html](https://spark.apache.org/docs/latest/cluster-overview.html)) — start here: the driver/executor/cluster-manager picture stated canonically, plus the Glossary section that pins down application / job / stage / task, which the books use loosely
4. **Spark-docs → Submitting Applications** ([submitting-applications.html](https://spark.apache.org/docs/latest/submitting-applications.html)) — `spark-submit`, and specifically the client-vs-cluster deploy mode distinction (`spark.submit.deployMode`). This is where "works in my notebook, fails on the cluster" is actually explained
5. **Spark-docs → Job Scheduling** ([job-scheduling.html](https://spark.apache.org/docs/latest/job-scheduling.html)) — two separate mechanisms the books blur together: [scheduling *across* applications](https://spark.apache.org/docs/latest/job-scheduling.html#scheduling-across-applications) (what the cluster manager does, including dynamic allocation) and [scheduling *within* one](https://spark.apache.org/docs/latest/job-scheduling.html#scheduling-within-an-application) (FIFO vs FAIR pools, i.e. `spark.scheduler.mode`)
6. **Spark-docs → Configuration** ([configuration.html](https://spark.apache.org/docs/latest/configuration.html)) — the reference for the driver/executor sizing knobs this topic introduces, and the precedence rules deciding which of `SparkConf`, `spark-submit` flags, and `spark-defaults.conf` actually wins
7. **Spark-docs → Spark Connect Overview** ([spark-connect-overview.html](https://spark.apache.org/docs/latest/spark-connect-overview.html)) — skim only: the architecture has **two** shapes in Spark 4.x, and every diagram in the books shows the classic one. Enough here to know which you are running; the depth is E9
8. **Spark-docs → Tuning** ([tuning.html](https://spark.apache.org/docs/latest/tuning.html)) — the shuffle and serialization behaviour behind the stage model; read the data-locality section alongside the locality-wait note below
9. **Source trace — [B1 in the source map](reference/spark-source-map/topics/b1.md)** — the full path: `getOrCreate()` → `DAGScheduler` → `TaskRunner`, then what a task actually produces (shuffle write, the three writers, `MapOutputTracker`), the failure and retry paths, how results return to the driver, and Connect as the alternative front end. Read it *after* the books: it turns "the DAG scheduler splits stages at shuffle boundaries" from a claim you accept into one you can go and look at

**Milestone:** You can explain (without notes) what happens between `spark.read.parquet(...)` and `.show()` — where the plan lives, when it executes, and which process runs the Python code. Stronger version, once you have read the source trace: name the single function that decides where one stage ends and the next begins; explain why a failing task retries four times on a cluster but aborts the stage immediately on your laptop; and explain why a stage you already watched succeed can run again.

!!! warning "Marked 🔄 — the installation chapter has a wrong Java-version claim"
    Chapter 03 (Spark Installation, written under this topic) states that Spark 4.x supports only Java 17 and 21. Spark 4.2.0 builds and runs on **Java 25** ([SPARK-51167]). Two further gaps the 4.2.0 source re-trace opened, both in Ch03: Spark 4.x is **Scala 2.13 only** (Scala 2.12 support was dropped across the whole 4.x line), which decides the `_2.13` suffix on every dependency artifact and silently breaks build files copied from Spark 3.x material; and the chapter's header still pins `Spark 4.1.x`. The architecture material in Ch01–Ch02 re-verified clean against 4.2.0 — only the install chapter needs work.

!!! info "Facts from the source that the books state loosely"
    All surfaced by the 4.2.0 trace and worth carrying into your own notes:

    - **One function enforces the stage split.** `DAGScheduler.getMissingParentStages` is the only place the narrow-vs-wide distinction is mechanically applied: a `ShuffleDependency` starts a new `ShuffleMapStage`, a `NarrowDependency` stays in the current one. Every prose explanation of stage boundaries is a description of this one function.
    - **`spark.task.maxFailures` does not apply in local mode.** `SparkContext` passes a hardcoded `MAX_LOCAL_TASK_FAILURES = 1` when building the local scheduler, so the documented default of 4 is ignored on your laptop and a single task failure aborts the stage. This is a common source of "it retried on the cluster but died locally" confusion.
    - **A shuffle-map task returns file locations, not data.** `ShuffleMapTask.runTask` returns a `MapStatus`; only a `ResultTask` returns values. That return type *is* the stage boundary, which makes the whole stage model concrete rather than diagrammatic.
    - **Three shuffle writers exist and you do not choose directly.** `SortShuffleManager.getWriter` picks between `BypassMergeSortShuffleWriter`, `UnsafeShuffleWriter` and `SortShuffleWriter` based on partition count, map-side combine, and serializer. Crossing `spark.shuffle.sort.bypassMergeThreshold` silently changes which one runs — a real performance cliff that no book names.

!!! warning "Task retry and fetch failure are different mechanisms — do not conflate them"
    The books teach `spark.task.maxFailures` and stop there, which leaves the most confusing production behaviour unexplained.

    A **task failure** retries that task. A **`FetchFailed`** means an upstream stage's shuffle output is *gone* — so the DAGScheduler resubmits the **parent stage** rather than retrying anything. That is why a stage you watched complete can run a second time. The usual cause is executor loss: `handleExecutorLost` unregisters that executor's map output from `MapOutputTracker`, and everything that depended on it must be recomputed.

    Two related behaviours worth knowing before you meet them at 2am: `maxResultSize` is enforced **at the executor**, which discards an oversized result rather than sending it; and the scheduler deliberately waits (`spark.locality.wait`) before demoting locality, so idle cores alongside queued tasks can be correct behaviour rather than a bug.

---

### 🔄 B2 — SparkSession and Entry Points

**What it is:** Creating a `SparkSession`; configuring the application and which settings can still change afterwards; log levels; local vs cluster mode; the REPL vs script context; and **which implementation you get** — classic or Connect — since `SparkSession` is an abstract base with two concrete subclasses in Spark 4.x.

**Why you need it:** Every PySpark program starts here. Understanding modes prevents "why does this work in notebook but not spark-submit" surprises — and in 4.x the same question extends to Connect, where a session that looks identical rejects direct JVM access.

**Learn it with:**

1. **Rioux Ch 2** — covers setup, configuration, and the SparkSession builder pattern
2. **FKane** — first two sections; shows the setup in a runnable environment you can follow along
3. **Spark-docs → Starting Point: SparkSession** ([sql-getting-started.html#starting-point-sparksession](https://spark.apache.org/docs/latest/sql-getting-started.html#starting-point-sparksession)) — the builder pattern from the source of truth
4. **Spark-docs → Configuration** ([configuration.html](https://spark.apache.org/docs/latest/configuration.html)) — three sections carry this topic: [Dynamically Loading Spark Properties](https://spark.apache.org/docs/latest/configuration.html#dynamically-loading-spark-properties) for the precedence rules (`SparkConf` vs `spark-submit` flags vs `spark-defaults.conf`), [Viewing Spark Properties](https://spark.apache.org/docs/latest/configuration.html#viewing-spark-properties) for confirming what actually took effect, and [Configuring Logging](https://spark.apache.org/docs/latest/configuration.html#configuring-logging) for log levels including the structured-logging option
5. **Spark-docs → Spark Connect Overview** ([spark-connect-overview.html](https://spark.apache.org/docs/latest/spark-connect-overview.html)) — what `.remote()` and `spark.api.mode` actually select. Follow with [Application development with Connect](https://spark.apache.org/docs/latest/app-dev-spark-connect.html) and the [Connect gotchas](https://spark.apache.org/docs/latest/spark-connect-gotchas.html), which lists the behaviours that differ — the fastest way to understand why `df._jdf` and `sc._jsc` are unavailable
6. **Spark-docs → `pyspark.sql.SparkSession` API reference** ([API reference](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.html)) — the full builder surface (`appName`, `master`, `config`, `remote`, `enableHiveSupport`, `getOrCreate`, `create`) in one table; keep it open while working
7. **Source trace — [B2 in the source map](reference/spark-source-map/topics/b2.md)** — `getOrCreate`'s real resolution order (thread-local active session → global default → construct new); what `SharedState` owns versus `SessionState`, which is the model that makes `newSession` / `cloneSession` / `create` follow from something rather than needing to be memorised; and how extensions attach

!!! info "`SharedState` vs `SessionState` — learn this and the session API stops needing memorisation"
    A `SparkSession` owns two state objects, and every confusing session behaviour follows from which one holds what.

    **`SharedState` — one per `SparkContext`, shared by every session on the JVM:** the cache manager, the external catalog (metastore), and the global temp database. **`SessionState` — one per session:** SQL conf, temp views, UDF registry, analyzer and optimizer.

    Three consequences worth predicting rather than discovering: `df.cache()` in one session is visible from another, because the cache manager is shared; `createGlobalTempView` outlives the session that made it, because it lives in `SharedState`; and `spark.stop()` tears down the `SparkContext`, invalidating **every** session on the JVM, not just yours.

!!! warning "`spark.sql.extensions` is static — set it at build time or not at all"
    Iceberg, Delta and Sedona all attach themselves through `SparkSessionExtensions`, driven by the `spark.sql.extensions` config. It is a **static** config: read once while the session is being constructed, so setting it afterwards with `spark.conf.set(...)` silently does nothing. This is the usual first failure when adding a table format, and the symptom — "my SQL syntax isn't recognised" — points nowhere near the cause.

**Milestone:** You can create a SparkSession with custom config, set the log level, and run a script with `spark-submit`. Then, the part that catches people: given a config set *after* the session exists, predict whether it takes effect or is silently ignored, and say why — then verify with `spark.conf.isModifiable()`. Finally, using the `SharedState`/`SessionState` split: predict whether a DataFrame cached in one session is visible from a second one created with `newSession()`, and whether a temp view is — then check both.

---

### 🔄 B3 — The DataFrame API: Basics

**What it is:** `select`, `filter`/`where`, `withColumn`, `drop`, `rename`, `distinct`, `show`, `printSchema`, `dtypes`, `describe`.

**Why you need it:** The primary tool for 90% of PySpark work. Everything else is built on top of it.

**Learn it with:**

1. **Rioux Ch 2, 4** — the clearest beginner introduction to the DataFrame API
2. **LS2e Ch 3** — adds the Catalyst/Tungsten context; explains *why* the API works the way it does
3. **DataCamp: Introduction to PySpark** ([datacamp.com](https://www.datacamp.com/courses/introduction-to-pyspark)) — ~4 hrs; interactive browser exercises; good for checking comprehension
4. **Spark-docs → Getting Started** ([sql-getting-started.html](https://spark.apache.org/docs/latest/sql-getting-started.html)) — untyped Dataset operations with the Python tab selected; the shortest correct reference for the core verbs
5. **Spark-docs → `DataFrame` API reference** ([reference/pyspark.sql/dataframe.html](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html)) — all 150+ methods in one indexed page. This is the one to keep open while working; the books cover perhaps twenty of them
6. **Spark-docs → ANSI Compliance** ([sql-ref-ansi-compliance.html](https://spark.apache.org/docs/latest/sql-ref-ansi-compliance.html)) — read the [Cast](https://spark.apache.org/docs/latest/sql-ref-ansi-compliance.html#cast) and [Arithmetic Operations](https://spark.apache.org/docs/latest/sql-ref-ansi-compliance.html#arithmetic-operations) sections before you trust any book example that casts a column. See the warning below
7. **Spark-docs → SELECT syntax** ([sql-ref-syntax-qry-select.html](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select.html)) — the SQL form of everything in this topic; useful for building the DataFrame ↔ SQL mapping early rather than treating B8 as a separate skill
8. **Source trace — [B3 in the source map](reference/spark-source-map/topics/b3.md)** — which plan node each verb produces, *and* what happens when an action finally runs: the `collect` path, where analysis errors are raised, and why `explain()` can disagree with the Spark UI. Read it once you can write the chains fluently: knowing `distinct()` becomes `Deduplicate` and `withColumn` becomes `Project(UnresolvedStarWithColumns)` is what makes `EXPLAIN` output readable later

!!! info "\"Nothing happens until an action\" is true for execution, not for validation"
    The lazy-evaluation rule has an exception the books rarely state. **Resolution errors fire immediately.** `CheckAnalysis` runs during analysis and raises `AnalysisException` before any job is submitted — so `df.select("typo")` fails instantly with nothing in the Spark UI, while a bad *cast* survives analysis and fails inside a task, appearing as a failed job.

    Same-looking mistakes, two entirely different failure modes and two different places to look. Useful corollary: an action is also what creates the SQL-tab entry (every action routes through `withAction` → `SQLExecution.withNewExecutionId`), so a chain of transformations with no action leaves no trace in the UI at all.

!!! warning "`explain()` shows the plan *before* AQE rewrites it"
    Adaptive Query Execution decides its final plan at runtime — `AdaptiveSparkPlanExec.finalPhysicalPlan` is only known after execution. So the plan printed by `df.explain()` before running is legitimately not the plan that ran, and comparing it against the Spark UI will show differences that are not bugs. To see what actually executed, read the SQL tab in the UI, or call `explain()` on a DataFrame that has already been executed.

!!! warning "Spark 4.x changed what a bad cast does — every book here predates it"
    `spark.sql.ansi.enabled` now defaults to **`true`**. Under ANSI mode Spark raises an exception at runtime where it previously returned `null`: invalid casts (`"abc"` to int), arithmetic overflow, and division by zero all now fail loudly. The official docs describe the `false` setting as "the behavior of Spark 3 or older" — which is exactly what Rioux (2022), LS2e (2020) and SDG (2018) document throughout.

    Practical effect: book examples that quietly produced `null` columns will instead stop your job. That is better behaviour, but it means a failing example is not necessarily your mistake. Do not switch ANSI off to make a book example work — read the Cast section, understand which conversion is invalid, and fix the expression with `try_cast` or an explicit filter.

**Milestone:** You can take a raw CSV, select specific columns, filter rows, add derived columns, and write the result to Parquet — all in a single method-chained program. Then predict, before running: which of your casts would throw under ANSI mode, and which columns would silently have been `null` on Spark 3.

---

### 🔄 B4 — Reading and Writing Data

**What it is:** SparkReader (`spark.read`) and SparkWriter (`df.write`) for CSV, JSON, Parquet, and ORC; options, modes, schema inference vs declaration.

**Why you need it:** Every pipeline starts with a read and ends with a write. Understanding format trade-offs (row vs columnar) sets up performance intuition.

**Learn it with:**

1. **Rioux Ch 2–3** — reading CSV with options; writing with modes
2. **LS2e Ch 4** — comprehensive treatment of all built-in sources (Parquet, JSON, CSV, Avro, ORC, binary, images)
3. **SDG Ch 9** — the deepest coverage of every data source option
4. **Spark-docs → Data Sources** ([sql-data-sources.html](https://spark.apache.org/docs/latest/sql-data-sources.html)) — per-format option tables (the [generic options](https://spark.apache.org/docs/latest/sql-data-sources-generic-options.html) page covers path globbing, `recursiveFileLookup`, and `modifiedBefore/After`); the canonical answer for "what options does this reader take"
5. **Spark-docs → Performance Tuning** ([sql-performance-tuning.html](https://spark.apache.org/docs/latest/sql-performance-tuning.html)) — where `spark.sql.files.maxPartitionBytes` and `openCostInBytes` are documented; these decide how many tasks your read gets, and no book covers the formula
6. **Spark-docs → Parquet** ([sql-data-sources-parquet.html](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html)) and [generic file options](https://spark.apache.org/docs/latest/sql-data-sources-generic-options.html) — partition discovery, schema merging, and the `ignoreCorruptFiles` / `ignoreMissingFiles` behaviour that decides whether a concurrent rewrite fails your read
7. **Source trace — [B4 in the source map](reference/spark-source-map/topics/b4.md)** — the full path from `spark.read` through format registration, driver-side file listing, splitting and parsing, to the commit protocol on the write side. Read it for the two things no book states: what actually decides read parallelism, and where write atomicity comes from

**Milestone:** You can read multi-file datasets with glob patterns, declare a schema programmatically with `StructType`, write in append/overwrite mode, and explain why Parquet is preferred for analytical workloads. Then two the source makes checkable: predict how many tasks a read of N files will produce and say which config capped it, and explain what happens to already-written files when a write fails halfway.

!!! warning "`insertInto` matches columns by position, not by name"
    `df.write.insertInto(table)` ignores column names entirely and matches by ordinal, while `saveAsTable` resolves by name. A DataFrame with the *right* column names in the wrong order writes silently corrupted data. None of the three books above covers this distinction; it is the highest-consequence trap in the writer API.

!!! info "Writes are not atomic on object storage"
    Spark writes into a `_temporary` directory and *moves* files on job commit. On HDFS that rename is atomic and cheap; on S3 and other object stores it is a copy — slow, and not atomic, so a failed job can leave partial output. This is the gap that Delta and Iceberg exist to close, and it is worth understanding here rather than treating those formats as magic later (see I8, I15).

---

### 🔄 B5 — Schema: StructType, DDL Strings, and Type Safety

**What it is:** `StructType` / `StructField` schema objects; DDL shorthand strings; `inferSchema` trade-offs; checking schema at runtime.

**Why you need it:** Schema mismatches are the #1 source of silent data corruption in Spark pipelines. Explicit schemas are the fix.

**Learn it with:**

1. **Rioux Ch 4** — introduces schema definition in context of CSV ingestion
2. **Rioux Ch 6** — `StructType` for JSON and nested schemas
3. **Spark-docs → SQL Data Types** ([sql-ref-datatypes.html](https://spark.apache.org/docs/latest/sql-ref-datatypes.html)) — canonical type reference; note `CHAR`/`VARCHAR` and the `VARIANT` type added in 4.0
4. **Spark-docs → ANSI Compliance** ([sql-ref-ansi-compliance.html](https://spark.apache.org/docs/latest/sql-ref-ansi-compliance.html)) — the [type-coercion](https://spark.apache.org/docs/latest/sql-ref-ansi-compliance.html#type-coercion) and [store-assignment](https://spark.apache.org/docs/latest/sql-ref-ansi-compliance.html#store-assignment) sections. Store assignment is the rule set that governs writing into an existing table, and it is stricter than what a `select` allows
5. **Spark-docs → `pyspark.sql.types` reference** ([API reference](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/data_types.html)) — every type class with its Python-value mapping; the table to check before assuming a Python type maps the way you expect
6. **Source trace — [B5 in the source map](reference/spark-source-map/topics/b5.md)** — the three input surfaces converging on one `DataType` tree, the three separate "is this cast allowed" rules, and — the one to read first — exactly where Spark does and does not enforce a schema

**Milestone:** You can define a schema without `inferSchema`, validate that incoming data matches it, and explain the cost of `inferSchema` on large files. Then the part that changes how you write pipelines: declare a column `nullable=False`, read a file containing nulls in it, and predict what happens before you run it.

!!! warning "`nullable=False` is a hint, not a constraint — nothing enforces it on read"
    This is the most consequential thing about schemas in Spark, and it is the opposite of what the word suggests. No part of the file-read path validates nullability. Declaring a column non-nullable tells the **optimizer** it may skip null checks; if the data then contains nulls, you get nulls in a column the plan believes cannot hold them — which produces wrong results rather than an error.

    Spark does enforce in two narrower places, with different rules in each: `createDataFrame` on local Python data with `verifySchema=True` checks type *and* range per row on the driver, and writing into an existing table applies `spark.sql.storeAssignmentPolicy`. Reading a file is checked by neither. If you need a guarantee, assert it yourself after the read.

!!! info "Three different rules answer 'is this cast allowed'"
    `canCast` governs an explicit `.cast()` and is permissive. `canUpCast` governs *implicit* coercion during analysis and allows only safe widening. `canANSIStoreAssign` governs writing into an existing table and sits between the two.

    Practical consequence: an expression that resolves fine in a `select` can fail on `INSERT INTO` — not a bug, a different rule set. Also note `spark.sql.ansi.enabled` selects between two *complete* coercion rule sets (`TypeCoercion` vs `AnsiTypeCoercion`), so the same query can resolve to different result types on either side of that flag.

---

### 🔄 B6 — Basic Aggregations and GroupBy

**What it is:** `groupBy().agg()`, built-in aggregate functions (`F.count`, `F.sum`, `F.avg`, `F.min`, `F.max`, `F.countDistinct`), `GroupedData`.

**Why you need it:** Aggregations are the core of analytical workloads. The `groupBy().agg()` pattern appears in every pipeline.

**Learn it with:**

1. **Rioux Ch 3, 5** — covers groupby, agg, and the GroupedData intermediate object
2. **LS2e Ch 4** — adds `F.expr()`, SQL aggregations, and the full function catalogue
3. **Spark-docs → Built-in Functions** ([sql-ref-functions-builtin.html](https://spark.apache.org/docs/latest/sql-ref-functions-builtin.html)) — the complete aggregate-function list; skim the aggregate section once so you stop reaching for a UDF when a built-in exists
4. **Spark-docs → GROUP BY syntax** ([sql-ref-syntax-qry-select-groupby.html](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-groupby.html)) — `HAVING`, and the `ROLLUP` / `CUBE` / `GROUPING SETS` forms that the DataFrame `rollup()` and `cube()` methods map onto
5. **Spark-docs → Performance Tuning** ([sql-performance-tuning.html](https://spark.apache.org/docs/latest/sql-performance-tuning.html)) — `spark.sql.shuffle.partitions` is *the* knob governing a `groupBy`'s cost, since it sets the partition count between the partial and final aggregate
6. **Source trace — [B6 in the source map](reference/spark-source-map/topics/b6.md)** — why one `groupBy` becomes two operators in the plan, what `countDistinct` actually costs, and which of the three aggregate operators your functions select

**Milestone:** You can compute multiple aggregations in a single `agg()` call, use `F.when()` for conditional counting, and write a query equivalent to a SQL `GROUP BY ... HAVING`. Then, from the plan: run `explain()` on a `groupBy().sum()` and explain why `HashAggregateExec` appears twice, and predict how the plan changes when you add a single `countDistinct`.

!!! info "One `groupBy` becomes two aggregates — that is map-side combine"
    Spark plans a `Partial` aggregate before the shuffle and a `Final` aggregate after it. Seeing `HashAggregateExec` twice in an `EXPLAIN` is that pair, not a duplicated plan, and it is why `groupBy().count()` over a billion rows does not ship a billion rows across the network — each partition sends one partial result per key.

    The shuffle between them is sized by `spark.sql.shuffle.partitions` (default 200), which makes it the main cost lever for any aggregation.

!!! warning "`countDistinct` is a different plan shape, and several of them multiply your data"
    A single distinct aggregate expands to **four** aggregate stages instead of two. Multiple distinct aggregates are rewritten into an `Expand` that emits one row per distinct group *per input row* before aggregating — so three `countDistinct`s over a large table can triple the rows entering the shuffle.

    That is the mechanism behind the usual advice to avoid stacking `countDistinct`s. When an approximation is acceptable, `F.approx_count_distinct` avoids the rewrite entirely.

!!! info "Which aggregate operator you get is decided by your functions, not by config"
    `HashAggregateExec` (the fast path) requires mutable fixed-width buffers. Add one `collect_list` or `percentile` and the buffer is no longer mutable, so Spark switches to `ObjectHashAggregateExec` — which falls back to sorting after just **128 groups** by default (`spark.sql.objectHashAggregate.sortBased.fallbackThreshold`, a group *count*, not a memory size). `SortAggregateExec` is the final fallback.

    Both hash operators can spill. The `numTasksFallBacked` metric in the Spark UI tells you whether yours did, which beats guessing.

**Milestone:** You can compute multiple aggregations in a single `agg()` call, use `F.when()` for conditional counting, and write a query equivalent to a SQL `GROUP BY ... HAVING`.

---

### 🔄 B7 — Joins: Types and Mechanics

**What it is:** Inner, left, right, full outer, semi, anti joins; equi-join shorthand; column disambiguation; broadcast join hint.

**Why you need it:** Joins are the most common source of performance problems in Spark. Understanding the types is the foundation for fixing those problems later.

**Learn it with:**

1. **Rioux Ch 5** — all join types with visual diagrams; column-clash solutions
2. **LS2e Ch 4** — join in the context of SQL tables and views
3. **SDG Ch 8** — the most comprehensive treatment of join mechanics, including physical strategies
4. **Spark-docs → JOIN syntax** ([sql-ref-syntax-qry-select-join.html](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-join.html)) — every join type in SQL form, including the semi/anti variants the books skim; also where `NEAREST BY` is documented from 4.2.0
5. **Spark-docs → Join Strategy Hints** ([sql-performance-tuning.html#join-strategy-hints-for-sql-queries](https://spark.apache.org/docs/latest/sql-performance-tuning.html#join-strategy-hints-for-sql-queries)) — the four hints (`BROADCAST`, `MERGE`, `SHUFFLE_HASH`, `SHUFFLE_REPLICATE_NL`) and the [adaptive skew-join](https://spark.apache.org/docs/latest/sql-performance-tuning.html#optimizing-skew-join) settings; a hint is a request the planner may decline, and this page says when
6. **Source trace — [B7 in the source map](reference/spark-source-map/topics/b7.md)** — the three-line strategy priority chain that explains all join tuning, why a `BROADCAST` hint on the wrong side does nothing, and what happens when your join condition is not an equality

**Milestone:** You can perform all seven join types, explain what `left_semi` and `left_anti` return without looking it up, and name three situations where a broadcast join is appropriate. Then from the plan: run `explain()` on a large-large join, identify the strategy and the `Exchange` nodes feeding it, and predict which strategy you would get if you changed the condition from `a == b` to `a > b`.

!!! info "Join *type* is your decision; join *strategy* is Spark's — and it is a ranked list"
    `JoinSelection` tries exactly three strategies in order: **broadcast hash → shuffled hash → sort-merge**. All join tuning is moving your query up that list.

    Two things about it are counterintuitive. `spark.sql.join.preferSortMergeJoin` defaults to **true**, which gates out the middle rung — most people never see a shuffled hash join. And the join *type* restricts which side may be broadcast: a left outer join can only broadcast its right side, so a `BROADCAST` hint naming the wrong side is silently inapplicable rather than honoured.

!!! warning "A non-equality join condition leaves the hash-join world entirely"
    Broadcast and sort-merge joins both require equi-join keys. Change `df1.a == df2.b` to `df1.a > df2.b` and there are no keys to hash or sort on, so Spark falls to `BroadcastNestedLoopJoinExec` — which compares every left row against every right row, O(n×m). With no condition at all you get `CartesianProductExec`, and `spark.sql.crossJoin.enabled` defaults to **`true`** in 4.x, so Spark will not stop you.

    This is the most common reason a join appears to hang rather than fail. Check the operator name in `explain()` before assuming the data is too big.

!!! info "The broadcast decision uses an estimate, and AQE may overrule it later"
    `canBroadcastBySize` compares `plan.stats.sizeInBytes` — a statistic, not a measurement — against the threshold. Missing or stale statistics are why a broadcast is sometimes chosen for something that then exhausts the driver or trips `spark.sql.broadcastTimeout` (300s).

    After a shuffle completes, AQE knows the real sizes and can promote a sort-merge join to a broadcast using a *separate* threshold, `spark.sql.adaptive.autoBroadcastJoinThreshold`. So the strategy in the Spark UI can legitimately differ from the one `explain()` printed — see the AQE note under B3.

!!! note "New in Spark 4.2.0 — `NEAREST BY` top-K ranking join"
    Spark 4.2.0 adds `NEAREST BY` ([SPARK-56395]), a join primitive for nearest-neighbour queries with both Catalyst and DataFrame API support. It is not one of the seven relational join types and none of the books cover it — learn the seven first, then read the 4.2.0 SQL reference. Relevant if you do vector/embedding work.

---

### 🔄 B8 — Spark SQL

**What it is:** `createOrReplaceTempView`, `spark.sql()`, SQL string expressions in `selectExpr`/`F.expr`, the Spark catalog.

**Why you need it:** SQL is often cleaner for complex transformations. Knowing when to use DataFrame API vs SQL — and how to mix them — is a practical skill.

**Learn it with:**

1. **Rioux Ch 7** — dedicated chapter on PySpark/SQL bilingual programming
2. **LS2e Ch 4** — SQL tables, views, and the catalog API
3. **Spark-docs → SQL Guide** ([sql-programming-guide.html](https://spark.apache.org/docs/latest/sql-programming-guide.html)) — start here; the [Getting Started](https://spark.apache.org/docs/latest/sql-getting-started.html) page covers temp views and the catalog
4. **Spark-docs → SQL Syntax reference** ([sql-ref-syntax.html](https://spark.apache.org/docs/latest/sql-ref-syntax.html)) — the full statement grammar. Worth knowing this exists as a reference rather than reading through: `selectExpr` and `F.expr` use the same parser, so anything documented here works inside them
5. **Spark-docs → Identifiers and name resolution** ([sql-ref-identifier.html](https://spark.apache.org/docs/latest/sql-ref-identifier.html), [sql-ref-name-resolution.html](https://spark.apache.org/docs/latest/sql-ref-name-resolution.html)) — how a bare name becomes a table, view or column, and the qualification rules behind the temp-view shadowing described below
6. **Source trace — [B8 in the source map](reference/spark-source-map/topics/b8.md)** — the three parser entry points, parameter binding as an analyzer rule, and `lookupRelation`'s branch order, which is where temp-view surprises come from

**Milestone:** You can register a DataFrame as a temp view, query it with `spark.sql()`, and mix SQL expressions into a method-chained DataFrame pipeline. Then, with a user-supplied value in hand: write the query so the value can never be parsed as SQL, and say why your approach guarantees that rather than merely making it unlikely.

!!! warning "Use parameterized SQL — string interpolation is the injection bug, not a style issue"
    Since Spark 3.4, `spark.sql` takes arguments: `spark.sql("SELECT * FROM t WHERE dt = :dt", {"dt": value})` for named parameters, or `?` with a list for positional.

    This is a *structural* fix rather than escaping. The query is parsed first, then the `BindParameters` analyzer rule substitutes each argument as a literal expression into the already-built plan — so a value cannot become SQL syntax no matter what it contains. And `spark.sql(text)` is literally defined as `spark.sql(text, Map.empty)`, so there is no cost to always using the parameterized form.

    Retreating to the DataFrame API to avoid injection concedes the SQL surface for no reason; parameters keep both.

!!! info "An unqualified name prefers a temp view; a qualified one cannot see temp views at all"
    `SessionCatalog.lookupRelation` resolves in a fixed order, and the asymmetry catches people. A bare `events` finds a temp view **before** a real table of the same name — so a temp view silently shadows a table. But `mydb.events` skips temp views entirely, so a qualified reference can never reach one.

    Two consequences: name your temp views distinctly to avoid shadowing production tables, and do not expect a database prefix to disambiguate *toward* a temp view — it disambiguates away from it. Global temp views are different again: they live in the `global_temp` database on the shared state, which is why they outlive the session that created them (see B2).

!!! note "New in Spark 4.2.0 — QUALIFY, search paths, metric views"
    Three additions the books predate: `QUALIFY` ([SPARK-31561]) filters on window-function results without a wrapping subquery — worth learning alongside I2; path-based name resolution (`SET PATH`, `CURRENT_PATH()`, [SPARK-54806]) changes how unqualified names resolve; and metric views (`CREATE VIEW … WITH METRICS`, [SPARK-54119]) add a declarative semantic-modelling surface. Learn the classic catalog model first — it's what the exam tests.

---

### 🔄 B9 — Null Handling

**What it is:** `dropna`, `fillna`, `coalesce`, null-safe equality (`<=>` / `eqNullSafe`), how nulls propagate through aggregations and joins.

**Why you need it:** Real data has nulls everywhere. Getting this wrong silently drops rows or produces wrong aggregates.

**Learn it with:**

1. **Rioux Ch 5** — `dropna`/`fillna` with `how`, `thresh`, and `subset`
2. **SDG Ch 6** — null semantics, null-safe joins, and null coercion rules
3. **Spark-docs → NULL Semantics** ([sql-ref-null-semantics.html](https://spark.apache.org/docs/latest/sql-ref-null-semantics.html)) — the authoritative page: how NULL behaves in comparisons, `IN`/`EXISTS`, aggregates, joins, and `GROUP BY`. Settles the cases where the books disagree with intuition
4. **Spark-docs → ORDER BY** ([sql-ref-syntax-qry-select-orderby.html](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-orderby.html)) — `NULLS FIRST` / `NULLS LAST`, and the defaults that make descending order not a mirror of ascending
5. **Spark-docs → Built-in Functions** ([sql-ref-functions-builtin.html](https://spark.apache.org/docs/latest/sql-ref-functions-builtin.html)) — the conditional-function section: `coalesce`, `nvl`, `nullif`, and `nanvl`, which is the only one that handles `NaN`
6. **Source trace — [B9 in the source map](reference/spark-source-map/topics/b9.md)** — how a null is actually stored, the two optimizer rules that rewrite nulls before execution, and where three-valued logic changes a result rather than just a filter

**Milestone:** You can explain why `F.count("col")` and `F.count("*")` return different results for a column with nulls. Then three that catch experienced people: predict what `NOT IN (subquery containing a null)` returns; predict whether `orderBy(c.desc())` puts nulls where `orderBy(c)` did; and say whether a `NaN` in a float column survives `dropna()`.

!!! warning "Three null traps that produce wrong answers, not errors"
    Each follows from three-valued logic, and none announces itself.

    **`NOT IN` with a nullable subquery returns nothing.** If the right-hand side contains a single null, SQL says the result is *unknown* for every row, so the anti join yields zero rows. Not an error, not a warning — an empty result that looks like a legitimate answer. Use `NOT EXISTS` or a left-anti join on a null-safe condition.

    **Descending order is not the reverse of ascending.** The default null ordering is `NULLS FIRST` for `ASC` and `NULLS LAST` for `DESC`, so nulls stay at the same end in both. A "top N" query built by flipping the sort direction can silently return N nulls. Say `NULLS LAST` explicitly when it matters.

    **`NaN` is not null.** A float column can hold both. `isNull` is false for `NaN`, `dropna()` keeps it, and `coalesce` returns it happily — only `nanvl` handles it. If your numeric pipeline can produce `0/0` or a failed cast under non-ANSI settings, cleaning nulls has not cleaned your data.

!!! info "Null behaves like false in a `WHERE` — and nowhere else"
    Inside a filter or join condition, Spark makes this explicit: the `ReplaceNullWithFalseInPredicate` optimizer rule substitutes `false` for a null predicate, because a row is kept only when the predicate is literally `true`.

    Outside a predicate the two are entirely different — `null` in an arithmetic expression propagates, in an aggregate is skipped, in a `GROUP BY` forms its own group, and in an equality yields null rather than false. The "null acts like false" shorthand is safe only in the one place the optimizer applies it.

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

### 🔄 I1 — Complex Column Types: Arrays, Maps, Structs

**What it is:** `ArrayType`, `MapType`, `StructType` as column values; `F.explode`, `F.posexplode`, `F.explode_outer`; array functions (`F.array_contains`, `F.size`, `F.array_distinct`); struct dot notation; `collect_list`/`collect_set`.

**Why you need it:** JSON data, event logs, and nested schemas are ubiquitous. This is the difference between working with 80% of real data and only 20% of it.

**Learn it with:**

1. **Rioux Ch 6** — the most thorough beginner treatment of all three complex types
2. **LS2e Ch 5** — higher-order functions (`TRANSFORM`, `FILTER`, `AGGREGATE` on arrays) — very useful in practice
3. **SDG Ch 6** — working with all data types; the most complete reference
4. **Spark-docs → Data Types** ([sql-ref-datatypes.html](https://spark.apache.org/docs/latest/sql-ref-datatypes.html)) + **Built-in Functions** ([sql-ref-functions-builtin.html](https://spark.apache.org/docs/latest/sql-ref-functions-builtin.html)) — the array/map/struct function catalogue, including the higher-order functions (`transform`, `filter`, `aggregate`) that replace an explode/re-group round trip

5. **Source trace — [I1 in the source map](reference/spark-source-map/topics/i1.md)** — why generators need their own plan node, what `explode_outer` actually adds, and why a higher-order function costs nothing where a UDF doing the same work costs a great deal

**Milestone:** You can flatten a JSON array-of-structs into rows, extract fields from nested structs, build an array column from grouped rows, and apply a lambda transform to every element of an array column. You can also state when `VARIANT` is the better choice than a declared `StructType`, and why. Then the one that catches people: given a column where some arrays are empty or null, predict how many rows survive `explode` versus `explode_outer`.

!!! warning "`explode` silently drops rows — this is data loss, not a variant"
    A null or empty array produces **zero** output rows, so the parent row disappears entirely. Nothing warns you; the result is simply smaller than the input and looks correct.

    `explode_outer` wraps the generator in `GeneratorOuter`, which emits one row with nulls instead. Treat `explode` as the special case you choose deliberately when you *want* rows without array elements removed, and reach for `explode_outer` by default when the array can be empty or null.

!!! info "Prefer higher-order functions over explode-and-regroup — no shuffle, no Python boundary"
    `transform`, `filter` and `aggregate` operate **within a single row**, so they replace the explode → `groupBy` → `collect_list` round trip that shuffles the expanded data. Same result, categorically different cost.

    The lambda you pass is a Catalyst expression (`LambdaFunction`), not a Python callable — it compiles and runs in the JVM. So unlike a UDF doing identical work, it crosses no Python boundary at all (see I3). Reach for `explode` only when you genuinely need one output row per element.

!!! info "Two container types, two behaviours, and `collect_*` changes your aggregate operator"
    Under ANSI, an out-of-range **array index raises**, while a **missing map key returns null** — both correct, easy to conflate. Building a map from data with duplicate keys also raises by default (`spark.sql.mapKeyDedupPolicy=EXCEPTION`); `LAST_WIN` is opt-in.

    And `collect_list`/`collect_set` use a growable JVM collection as their aggregation buffer, which is exactly the non-mutable case that forces `ObjectHashAggregateExec` and its 128-group sort fallback (see B6). Neither preserves order, and `collect_set` drops nulls.

!!! warning "`VARIANT` is missing from every book — and it changes this topic"
    Spark 4.0 introduced `VARIANT`, a first-class type for semi-structured data (JSON and friends) that stores values in a binary encoded form and lets you query into them without declaring a schema up front. It went GA with *shredding* — physically splitting frequently-accessed fields into columnar storage for fast reads — and Parquet has since adopted the type natively.

    This matters because the books teach exactly two options for messy JSON: declare a full `StructType`, or keep it as a string and parse repeatedly. `VARIANT` is a third, and it is usually the right one when the schema is genuinely unstable or wide. Rioux, LS2e and SDG all predate it.

    Learn it from **Spark-docs → [Variant data type](https://spark.apache.org/docs/latest/sql-ref-datatypes.html)** and the [Parquet VARIANT announcement](https://parquet.apache.org/blog/2026/02/27/variant-type-in-apache-parquet-for-semi-structured-data/); verify behaviour on your own 4.2.0 stack. Do the declared-schema work in this topic first — knowing what `VARIANT` saves you from requires having done it the manual way once.

---

### 🔄 I2 — Window Functions

**What it is:** `Window.partitionBy().orderBy()`, aggregate functions over windows, ranking functions (`rank`, `dense_rank`, `percent_rank`, `ntile`, `row_number`), analytic functions (`lag`, `lead`, `cume_dist`), frame boundaries (`rowsBetween`, `rangeBetween`).

**Why you need it:** Time-series features, running totals, ranking, deduplication keeping only the latest record — window functions handle all of these in one pass without a self-join.

**Learn it with:**

1. **Rioux Ch 10** — the clearest full chapter introduction to all window function types
2. **LS2e Ch 5** — window functions in the context of SQL and DataFrame APIs
3. **SDG Ch 7** — aggregations chapter includes window functions with the deepest semantic explanations
4. **Spark-docs → Window Functions** ([sql-ref-syntax-qry-select-window.html](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-window.html)) — frame semantics stated precisely: `ROWS` vs `RANGE`, and what the default frame becomes once you add `ORDER BY` (the single most common window bug)
5. **Source trace — [I2 in the source map](reference/spark-source-map/topics/i2.md)** — the six lines of `resolveFrame` that decide your default frame, why omitting `partitionBy` moves the entire dataset to one partition, and which frame implementation your window actually runs

!!! warning "Adding `orderBy` changes what an aggregate window computes — silently"
    Spark fills in a frame you did not write, and the default depends on whether an ordering is present:

    - **no `orderBy`** → `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING` — the whole partition
    - **with `orderBy`** → `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` — a *running* value

    So `F.sum("x").over(Window.partitionBy("k"))` is a partition total, while the same window `.orderBy("t")` is a running total. Different results, and nothing in your code says so. Note the frame *type* also flips from `ROWS` to `RANGE`: `RANGE` compares ordering **values**, so rows tied on the ordering column all enter the frame together — a running sum over duplicate timestamps jumps rather than stepping.

    Write the frame explicitly whenever the answer matters.

!!! warning "A window without `partitionBy` moves the entire dataset to one partition"
    `WindowExec` requires `AllTuples` distribution when no partitioning is given — every row on one executor. Spark logs `"No Partition Defined for Window operation!"` and carries on, so it works fine on sample data and falls over at scale.

    More generally, every window is a shuffle **plus** a sort, since the operator requires both a distribution and an ordering. Chaining windows with different `partitionBy` clauses means repeating both each time.

!!! info "Top-N per group is optimized — within limits"
    The idiomatic `row_number() <= n` filter is efficient because `InferWindowGroupLimit` pushes a `WindowGroupLimit` below the shuffle, discarding non-qualifying rows before they are moved. But it only fires for recognised comparison forms and when *n* ≤ `spark.sql.optimizer.windowGroupLimitThreshold` (1000) — above that, or with an unusual predicate, you silently rank everything.

**Milestone:** You can reproduce a self-join using a window function, explain why an ordered aggregate window produces different results than an unordered one — naming both default frames — and build a 30-day rolling average using `rangeBetween` on a unix timestamp. Then: given rows with duplicate timestamps, predict how a running sum differs under `rowsBetween` versus `rangeBetween`, and say what `explain()` shows above your window operator.

---

### 🔄 I3 — User-Defined Functions

**What it is:** `@F.udf` (row-by-row Python UDF); `@F.pandas_udf` (vectorised Series→Series, Iterator→Iterator); the performance hierarchy; `.func` for local testing.

**Why you need it:** When no built-in function covers your logic, UDFs are the escape hatch. Knowing the cost of each type determines which one to reach for.

**Learn it with:**

1. **Rioux Ch 8** — Python UDF full treatment
2. **Rioux Ch 9** — pandas UDF full treatment (Series→Series, Iterator variants, group aggregate, group map)
3. **LS2e Ch 5** — UDF section with Python and SQL interop
4. **IBM-Spark Module 3** — practical ETL + ML pipeline UDFs; hands-on lab
5. **Spark-docs → UDFs & UDTFs** ([user_guide/udfandudtf.html](https://spark.apache.org/docs/latest/api/python/user_guide/udfandudtf.html)) — the current taxonomy: scalar Python UDFs, pandas UDFs, and Arrow UDFs (`pyarrow.Array` in and out). This page reflects the 4.2.0 defaults the books predate; read it before trusting any book's performance claim
6. **Spark-docs → Apache Arrow in PySpark** ([tutorial/sql/arrow_pandas.html](https://spark.apache.org/docs/latest/api/python/tutorial/sql/arrow_pandas.html)) — all four pandas UDF shapes plus the function APIs (`applyInPandas`, `mapInPandas`, `cogroup`)
7. **Source trace — [I3 in the source map](reference/spark-source-map/topics/i3.md)** — the eval-type integer that identifies every UDF flavour, why a UDF's output is permanently nullable, and what worker reuse actually buys you

**Milestone:** You can replace a Python UDF with a pandas UDF and measure the speedup **on 4.2.0** — not quote a book's figure; you can load an ML model once per partition using an Iterator UDF and say which config makes that pay off; you can test a UDF locally without a SparkSession. Then, from `explain()`: name which eval operator your UDF ran under, and explain why chaining a plain UDF and a pandas UDF in one `select` costs more than chaining two of the same kind.

!!! warning "The Arrow default flipped in 4.2.0 — re-measure rather than trusting the books"
    `spark.sql.execution.pythonUDF.arrow.enabled` now defaults to **`true`**, so a plain `@F.udf` is Arrow-serialized instead of pickled row by row. The "pandas UDFs are 5–10× faster" figures in Rioux and LS2e were measured against per-row pickle, which is no longer what you get by default.

    The hierarchy still holds directionally — built-ins beat UDFs, vectorised beats scalar — but the *gaps* have narrowed and the reason to prefer a pandas UDF is now more about expressing vectorised logic than about escaping pickle. Measure on your own 4.2.0 stack.

    One trap while benchmarking: if PyArrow or pandas is missing, Spark **silently falls back** to the non-Arrow path with only a `RuntimeWarning`. Identical code can run at very different speeds in two environments.

!!! info "A UDF's output is always nullable, and mixing UDF types costs an extra round trip"
    `PythonUDF.nullable` is `true` unconditionally, whatever return type you declare — so downstream null checks can never be optimized away. That is a permanent optimizer cost on top of serialization, and it is why a UDF in a hot path hurts more than its own runtime suggests.

    Separately, `ExtractPythonUDFs` batches UDFs of the **same eval type** into one plan node. A plain UDF and a pandas UDF in the same `select` therefore produce two nodes and two crossings of the Python boundary. Keeping a chain to one flavour is a free win, and `explain()` shows it as `BatchEvalPython` versus `ArrowEvalPython` nodes.

!!! info "Python worker memory is not part of executor memory"
    Python workers are separate OS processes, so their footprint is outside `spark.executor.memory` and governed by `spark.executor.pyspark.memory`. Heavy pandas UDFs that exhaust container memory usually show up as a killed container rather than a JVM OOM, which sends people tuning the wrong knob.

    Worker reuse (`spark.python.worker.reuse`, on by default) is what makes the Iterator-UDF pattern worthwhile — the process survives across tasks, so a model loaded once stays loaded.

!!! warning "Spark 4.2.0 changes the UDF performance hierarchy the books teach"
    Arrow-optimized Python UDFs and Arrow-based PySpark IPC are now **on by default** ([SPARK-54555]). Rioux and LS2e were written when plain `@F.udf` meant row-by-row pickle serialisation, so the "pandas UDF is dramatically faster" gap they measure is narrower on 4.2.0. Learn the hierarchy anyway — it explains *why* Arrow helps, and the exam still tests it — but re-run the speedup measurement in the milestone on your own 4.2.0 stack rather than trusting the book's numbers. Spark 4.2.0 also adds Arrow and pandas grouped-aggregation UDFs, which belong with A5.

---

### 🔄 I4 — RDD Fundamentals

**What it is:** `SparkContext.parallelize`, `map`, `filter`, `reduce`, `flatMap`, `collect`, `take`; when RDDs are still needed vs DataFrames.

**Why you need it:** Needed for tasks that require arbitrary Python objects (not tables), and for understanding what the DataFrame API is built on.

**Learn it with:**

1. **Rioux Ch 8** — RDD introduction alongside UDFs
2. **LS2e Ch 3** — RDD vs DataFrame trade-offs explained
3. **SDG Ch 12–13** — the deepest treatment of RDDs and advanced patterns (accumulators, broadcast variables)
4. **FKane** — Spark Basics and the RDD Interface section (~2 hrs, hands-on)
5. **Spark-docs → RDD Programming Guide** ([rdd-programming-guide.html](https://spark.apache.org/docs/latest/rdd-programming-guide.html)) — the canonical reference, and the one place that explains closures (why a driver variable mutated inside a transformation stays unchanged) before it bites you
6. **Spark-docs → Tuning** ([tuning.html](https://spark.apache.org/docs/latest/tuning.html)) — serialization and memory tuning matter far more for RDDs than for DataFrames, since there is no Tungsten format underneath: read the [data serialization](https://spark.apache.org/docs/latest/tuning.html#data-serialization) section alongside `spark.serializer`
7. **Source trace — [I4 in the source map](reference/spark-source-map/topics/i4.md)** — the five-method contract every RDD implements, how `iterator()` dispatches between cache, checkpoint and compute, and the exact line where a co-partitioned join becomes shuffle-free

!!! warning "The RDD API is classic-mode only — it does not work over Spark Connect"
    `df.rdd` raises `PySparkNotImplementedError` under Connect, and the Connect client ships no `RDD` class at all. Since Connect is the default mode of the `pyspark` REPL in 4.x, check which mode you are in before assuming this topic's material is available.

    The 4.2.0 release notes are actively misleading here: the heading "RDD API compatibility ([SPARK-55227])" sits above `DataFrame.zipWithIndex`, `Dataset.zipWithIndex` and `DataFrame.toJSON` — DataFrame methods that *remove reasons* to drop to RDDs under Connect. That is the opposite of RDD support. Verified against the 4.2.0 source, not the notes.

!!! info "Porting RDD code to Connect: rewrite to DataFrame, do not look for an RDD escape hatch"
    Since there is no RDD execution in a remote session, code that leans on RDDs has to be refactored into DataFrame transformations *before* it can move to Connect. There is no compatibility shim to wait for — the direction of travel is to remove the reasons people reached for RDDs, and several of those reasons were closed in 4.2.0:

    | Why you reached for an RDD | DataFrame equivalent |
    |---|---|
    | Add a row index | `DataFrame.zipWithIndex` ([SPARK-55229]; Scala `Dataset.zipWithIndex`, [SPARK-55228]) |
    | Serialize rows to JSON strings | `DataFrame.toJSON` ([SPARK-55090]) |
    | Parse a column of JSON/CSV/XML text | `spark.read.json` / `.csv` / `.xml` now accept a DataFrame ([SPARK-56253]–[SPARK-56255]) |
    | Build an empty dataset | `SparkSession.emptyDataFrame` ([SPARK-56256]) |
    | Arbitrary per-row Python logic | a UDF or pandas UDF (I3) |
    | Per-partition setup (connections, models) | `mapInPandas` / `mapPartitions` on a DataFrame (I3) |
    | Custom partitioning | `repartition(col)` / bucketing (I5) |

    Learn the RDD model anyway — it is what the DataFrame API compiles down to, and B1's stage/shuffle material only makes sense in these terms. Just do not build new production code on it if Connect is your target.

!!! info "`repartition` is `coalesce` with one boolean flipped"
    `repartition(n)` is defined as `coalesce(n, shuffle = true)`. One method, one argument — which turns "coalesce avoids a shuffle, repartition forces one" from two APIs to memorise into a single fact about a parameter. Carries directly into I5.

    Two related mechanics worth having here: every closure you pass is run through `SparkContext.clean`, which is what raises `Task not serializable` when driver state leaks into a task; and RDD aggregations spill through `ExternalAppendOnlyMap`/`ExternalSorter`, the RDD-level analogue of the aggregate spill in B6 — which is why `groupByKey` on skewed data degrades instead of failing outright.

**Milestone:** You can explain in one sentence why `reduce` requires a commutative and associative function, and name two real tasks where you would use an RDD instead of a DataFrame.

---

### 🔄 I5 — Partitioning: Concepts and Control

**What it is:** Physical partitions vs logical partitions; `repartition(n)`, `coalesce(n)`, `partitionBy(col)` on writes; default shuffle partition count; how partition count affects file output.

**Why you need it:** Wrong partition counts are responsible for most "my job is slow" and "my job wrote 10,000 tiny files" problems.

**Learn it with:**

1. **Rioux Ch 3** — `coalesce` and `repartition` basics
2. **LS2e Ch 7** — scaling Spark for large workloads; partition tuning
3. **Spark-docs → Performance Tuning** ([sql-performance-tuning.html](https://spark.apache.org/docs/latest/sql-performance-tuning.html)) — `spark.sql.shuffle.partitions`, and the [coalescing post-shuffle partitions](https://spark.apache.org/docs/latest/sql-performance-tuning.html#coalescing-post-shuffle-partitions) section, which is what actually decides your partition count once AQE is on
4. **Spark-docs → SQL Hints** ([sql-ref-syntax-qry-select-hints.html](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-hints.html)) — the partitioning hints (`COALESCE`, `REPARTITION`, `REPARTITION_BY_RANGE`, `REBALANCE`), including `REBALANCE`, which asks AQE to size partitions instead of you picking a number
5. **Source trace — [I5 in the source map](reference/spark-source-map/topics/i5.md)** — why `coalesce` is contagious upstream, why a bare `repartition` does a hidden sort, and how partitionings are negotiated rather than commanded

**Milestone:** You can explain the difference between `repartition` and `coalesce`, set `spark.sql.shuffle.partitions` appropriately for your data volume, and write a DataFrame to exactly N files. Then the one that separates knowing the API from understanding it: explain why `df.transform(...).coalesce(1).write(...)` can be dramatically slower than the same pipeline with `repartition(1)`, and say what `explain()` would show in each case.

!!! warning "`coalesce` is cheaper *and* contagious — it slows everything upstream in the stage"
    `coalesce` avoids a shuffle by grouping existing partitions through a narrow dependency. But a narrow dependency means **no stage boundary**, so the upstream computation runs with the coalesced task count. `coalesce(1)` before a write does not just produce one file — it makes every transformation in that stage single-threaded.

    `repartition(1)` inserts a shuffle, which sounds worse and is often much faster: the expensive work upstream keeps its parallelism, and only the final write is serialized. "Coalesce avoids a shuffle" is true and, taken alone, actively misleading.

    Related: `CoalesceExec` advertises `UnknownPartitioning`, so coalescing before a join or `groupBy` does not save the shuffle those would need anyway.

!!! info "Partitionings are negotiated, not commanded — and AQE has the last word"
    Operators advertise an `outputPartitioning` and demand a `requiredChildDistribution`; `EnsureRequirements` inserts a shuffle only where the two disagree. So `repartition(n, "k")` immediately before `groupBy("k")` is usually redundant — the aggregate would have requested that layout itself. Same machinery as the join exchanges in B7.

    With AQE on, `spark.sql.shuffle.partitions` is a *starting point*: `CoalesceShufflePartitions` merges post-shuffle partitions toward `advisoryPartitionSizeInBytes` with a 1MB floor. Tuning the static number matters far less than it did pre-3.0, and `REBALANCE` (or `RebalancePartitions`) lets you stop guessing a count altogether — the direct fix for "my job wrote 10,000 tiny files".

!!! info "A bare `repartition(n)` does a hidden local sort — for correctness, not speed"
    Round-robin partitioning must be deterministic: if a retried task assigned rows differently, rows would be **lost**, not merely reshuffled (SPARK-23207). Spark guarantees determinism with a local sort before partitioning, controlled by `spark.sql.execution.sortBeforeRepartition` (default `true`).

    So `repartition(n)` costs more than it appears to, and that cost is buying correctness under task retry. Disabling the flag to speed it up trades away data integrity. Also worth knowing: round-robin is implemented as a hash over a synthetic key, which is why partition sizes come out approximately rather than exactly equal.

---

### ⬜ I6 — Caching and Persistence

**What it is:** `df.cache()`, `df.persist(StorageLevel.*)`, `df.unpersist()`; storage levels; when caching helps vs hurts; the default level (`MEMORY_AND_DISK_DESER` in PySpark's naming) and how cache entries are matched and evicted.

**Why you need it:** Caching an intermediate DataFrame used multiple times avoids recomputing it. Caching the wrong thing wastes memory and slows everything down.

**Learn it with:**

1. **LS2e Ch 7** — caching section with practical rules
2. **SDG Ch 19** — performance tuning; caching strategy
3. **Spark-docs → RDD Persistence** ([rdd-programming-guide.html#rdd-persistence](https://spark.apache.org/docs/latest/rdd-programming-guide.html#rdd-persistence)) — the storage-level table and the eviction rules; the DataFrame `cache()` you use daily is this mechanism underneath
4. **Spark-docs → CACHE TABLE** ([sql-ref-syntax-aux-cache-cache-table.html](https://spark.apache.org/docs/latest/sql-ref-syntax-aux-cache-cache-table.html)) — the SQL side, including `LAZY` and why `CACHE TABLE` is eager while `df.cache()` is not
5. **Spark-docs → Memory Management** ([tuning.html#memory-management-overview](https://spark.apache.org/docs/latest/tuning.html#memory-management-overview)) — `spark.memory.fraction` and `storageFraction`; the key point being that storage and execution *share* one region, so cached blocks can be evicted by a shuffle
6. **Spark-docs → `pyspark.StorageLevel`** ([API reference](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.StorageLevel.html)) — the Python constants, which do **not** map one-to-one onto the Scala ones (see the warning below)
7. **Source trace — [I6 in the source map](reference/spark-source-map/topics/i6.md)** — why a cache hit depends on plan equivalence rather than on your variable, and why `storageFraction` is a floor rather than a reservation

**Milestone:** You can identify in the Spark UI whether a cached DataFrame is being reused, and name three situations where caching makes a job slower. Then two the source settles: explain why `cached_df.filter(...)` may recompute from source, and say which storage level `df.cache()` actually gives you — spelled the way PySpark spells it.

!!! warning "`MEMORY_AND_DISK` does not mean the same thing in PySpark as in Scala"
    Storage levels are `(useDisk, useMemory, useOffHeap, deserialized)`, and the two APIs disagree on one flag under the same name:

    | Constant | Flags | Deserialized? |
    |---|---|---|
    | Scala `MEMORY_AND_DISK` | `(true, true, false, true)` | yes |
    | PySpark `MEMORY_AND_DISK` | `(True, True, False, False)` | **no** |
    | PySpark `MEMORY_AND_DISK_DESER` | `(True, True, False, True)` | yes |

    `df.cache()` resolves to the deserialized level — which PySpark spells `MEMORY_AND_DISK_DESER`. So calling `df.persist(StorageLevel.MEMORY_AND_DISK)` in Python does **not** reproduce the default; it selects a serialized level instead. Same name, different behaviour, no warning.

!!! warning "Caching is registered by plan, and it is not a guarantee"
    Three things about `cache()` that the API hides:

    **It computes nothing.** `cache()` registers the plan with the `CacheManager` and returns; data appears on the next action. The first read after caching is no faster — the standard benchmarking mistake. `CACHE TABLE` is the opposite, materializing eagerly, with `CACHE LAZY TABLE` to opt out.

    **Hits are matched by plan equivalence, not by your variable.** `lookupCachedData` compares plans with `sameResult`, so two independently-built identical plans share one entry, while `cached_df.filter(...)` is a *different* plan and recomputes from source. This is the most useful fact in the topic and is invisible from the API.

    **Cached data can be evicted mid-job.** `spark.memory.storageFraction` (0.5) is the floor below which storage cannot be evicted, not a reservation — above it, execution wins. With a memory-only level an evicted block is silently recomputed: correct results, unexplained slowness. The Storage tab (I7) is how you see it.

---

### ⬜ I7 — The Spark UI: Reading Plans and Diagnosing Jobs

**What it is:** Jobs, stages, tasks; the SQL tab (parsed → analysed → optimised → physical plan); DAG visualisation; shuffle read/write metrics; spill indicators.

**Why you need it:** The Spark UI is your debugger for performance problems. Without it, tuning is guessing.

**Learn it with:**

1. **Rioux Ch 11** — dedicated chapter walking through every tab of the Spark UI
2. **LS2e Ch 7** — Spark UI walkthrough with a concrete slow-job example
3. **ADEB Module 3** (Databricks Performance Optimization) — Spark UI analysis section; practise reading plans on Databricks
4. **Spark-docs → Web UI** ([web-ui.html](https://spark.apache.org/docs/latest/web-ui.html)) — every tab and what each column means; the reference to keep open while the books teach you what to look for. Note the UI was rebuilt in 4.2.0, so this page matches your screen and the book screenshots do not
5. **Spark-docs → Monitoring** ([monitoring.html](https://spark.apache.org/docs/latest/monitoring.html)) — the History Server, event logging, and the [REST API](https://spark.apache.org/docs/latest/monitoring.html#rest-api). Everything the UI renders is available as JSON at `/api/v1`, which turns "check the UI" into something you can automate
6. **Source trace — [I7 in the source map](reference/spark-source-map/topics/i7.md)** — the UI is a read model over an event stream, and the stream drops events under load. Read this before trusting a number the UI shows you on a busy job

!!! warning "The UI is derived from an event stream that drops events under load — by design"
    Nothing in the UI is measured directly. The scheduler emits listener events onto a **bounded** asynchronous queue; when it fills, events are discarded so the scheduler is never blocked. The only evidence is one log line — *"Dropping event from queue … one of the listeners is too slow"* — and a counter.

    So on a busy job, missing tasks, totals that do not add up, and stages that never appear to finish are expected rather than mysterious. `spark.scheduler.listenerbus.eventqueue.capacity` (10000) is the knob when you need the UI to be complete.

    Retention compounds it: `spark.ui.retainedJobs`/`retainedStages` (1000) and `retainedTasks` (100000) mean a long or wide application loses its early history entirely.

!!! info "Enable event logging *before* you need it, and read the SQL tab for the real plan"
    `spark.eventLog.enabled` is **off by default**, and the History Server can only replay what was logged — so turning it on after an incident gives you nothing. `spark.eventLog.logStageExecutorMetrics` is separately off and is what you need for memory forensics after the fact.

    On plans: the SQL tab shows the **post-AQE** plan, while `df.explain()` prints the pre-AQE one. They legitimately disagree (see B3 and B7), and the SQL tab is the one that reflects what actually ran. No action means no SQL-tab entry at all, since executions register through `withNewExecutionId`.

**Milestone:** You can open the Spark UI on a running job, locate the most expensive stage, identify whether it involves a sort-merge join or a broadcast join, and read a physical plan to find a pushed-down filter. Then the part that makes the UI trustworthy rather than merely readable: say how you would tell whether the numbers on screen are complete, and fetch the same stage's metrics from `/api/v1` as JSON.

!!! warning "The Spark UI was rebuilt in 4.2.0 — book screenshots are stale"
    Spark 4.2.0 ships a modernized Web UI ([SPARK-55760]) with dark mode and searchable, zoomable, side-by-side SQL plan visualisation. Rioux Ch 11 and LS2e Ch 7 show the old layout. The tabs and the metrics behind them are the same — read the chapters for *what to look for*, then find it in the new UI yourself. The side-by-side plan view makes the A1 milestone (comparing plans before/after a change) substantially easier.

---

### ⬜ I8 — Delta Lake Basics

**What it is:** Writing and reading Delta tables; ACID transactions; the transaction log; `DESCRIBE HISTORY`; time travel (`VERSION AS OF`, `TIMESTAMP AS OF`); `VACUUM`; `OPTIMIZE`.

**Why you need it:** Delta Lake is the standard table format for Spark-based data engineering. It replaces plain Parquet for anything that gets updated or that needs data reliability.

**Learn it with:**

1. **DLUR Ch 1–4** — architecture + all basic operations; the best hands-on introduction
2. **LS2e Ch 9** — lakehouse overview; positions Delta Lake alongside Hudi and Iceberg
3. **Delta-docs Quickstart** ([docs.delta.io/latest/quick-start.html](https://docs.delta.io/latest/quick-start.html)) — run against your local stack first
4. **Delta-docs → Table protocol** ([PROTOCOL.md](https://github.com/delta-io/delta/blob/master/PROTOCOL.md)) — the actual on-disk contract: action types, commit-file naming, checkpoint format, reader/writer versions. Shorter and more concrete than the prose docs, and it settles anything the books leave ambiguous
5. **Source trace — [I8 in the source map](reference/spark-source-map/topics/i8.md)** — how Delta installs itself as a plugin, why a delete is an append, and the single filesystem operation the whole ACID story rests on

!!! warning "Delta 4.3.1 does not support Spark 4.2.0 — check before starting this topic"
    Delta's build targets exactly two Spark versions, **4.0.1 and 4.1.0**, with 4.1 as the default (`project/CrossSparkVersions.scala`, `ALL_SPECS`). There is no 4.2 target.

    So I8 — along with I9, A6 and E4, which all build on it — cannot be practised on the 4.2.0 stack the rest of this path targets. Run a separate Spark 4.1 environment for the Delta topics, or take them after the rest.

    This is now the **second** table format in this position: [I15](#i15-apache-iceberg-and-table-format-interoperability) has the same gap for the same reason. Both lag the Spark release by design — a table format has to be built and tested against a released Spark, so a new Spark minor is always ahead of its connectors. Worth planning around rather than treating as a surprise: pin your learning stack to the newest Spark that *your table format* supports, not the newest Spark.

!!! info "Delta is a plugin, and its whole ACID story is one filesystem operation"
    Delta is not built into Spark. It installs through `spark.sql.extensions` plus a catalog implementation, then intercepts planning for directories containing a `_delta_log`. That config is **static** (see B2), so setting it after the session exists silently gives you no Delta at all — the most common setup failure, and it presents as "my Delta SQL isn't recognised".

    Once installed, the mechanism is simpler than the vocabulary suggests. The log is a numbered sequence of JSON commits; a transaction reads a snapshot and then tries to create file `N+1.json`. **Atomicity is exactly that: exactly one writer can create a given filename.** No lock service, no coordinator.

    Two consequences worth carrying: a losing writer is not automatically failed — the `ConflictChecker` retries against the newer snapshot when the transactions are logically compatible, so concurrent appends normally both succeed. And a delete is an *append* (a `RemoveFile` tombstone), which is why time travel exists at all and why `VACUUM` is the operation that destroys it.

**Milestone:** You can create a Delta table, insert/update/delete rows, query a past version, run `OPTIMIZE`, and explain what the `_delta_log/` directory contains. Then, from the log itself: delete a row, then show which action was appended and which file is still physically present — and say what `VACUUM` would do to your ability to time-travel past that point.

---

### ⬜ I9 — The Medallion Architecture

**What it is:** Bronze (raw ingest) → Silver (cleaned, typed) → Gold (aggregated, business-ready); schema enforcement at each layer; MERGE INTO for upserts; incremental processing.

**Why you need it:** The medallion pattern is the standard architecture for lakehouses. Every data engineering job description assumes familiarity with it.

**Learn it with:**

1. **DLUR Ch 1** — introduces the medallion concept in the lakehouse context
2. **DEB Module 1** — data ingestion into bronze with Auto Loader; CTAS, COPY INTO, MERGE INTO patterns
3. **DLDG Ch 9** — architecting a lakehouse; design decisions at each layer
4. **Delta-docs → Best practices** ([best-practices.html](https://docs.delta.io/latest/best-practices.html)) — partition-column choice, the ≥1 GB per partition guidance, compaction with `dataChange=false`, and why caching a Delta table defeats data skipping. Short, and it prevents the two mistakes that make a bronze layer unusable

**Milestone:** You can build a three-layer pipeline from raw Parquet files to a Gold aggregation table, with schema enforcement on silver, using your local Unity Catalog stack.

---

### ⬜ I10 — Data Formats: Parquet, Delta, Avro, JSON

**What it is:** Columnar vs row storage; predicate pushdown; column pruning; Parquet row groups and page footers; when to use each format.

**Why you need it:** Format choice is a major performance variable. The Catalyst optimizer exploits Parquet metadata — but only if the file is written correctly.

**Learn it with:**

1. **LS2e Ch 4** — data sources and format comparison
2. **SDG Ch 9** — the most complete treatment of every format option
3. **DLDG Ch 1** — how Delta wraps Parquet and what the transaction log adds
4. **Spark-docs → Parquet** ([sql-data-sources-parquet.html](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html)) — partition discovery, schema merging, and the predicate-pushdown knobs; pair with **Performance Tuning** ([sql-performance-tuning.html](https://spark.apache.org/docs/latest/sql-performance-tuning.html)) for the columnar-reader settings

**Milestone:** You can explain why `F.col("date") > '2024-01-01'` on a Parquet file can be resolved without reading any data, and why the same filter on a CSV cannot.

!!! note "New in Spark 4.2.0 — geospatial and TIME types across file formats"
    Native `GEOMETRY` and `GEOGRAPHY` types with `ST_*` functions, WKB/WKT and Parquet I/O, and an SRID registry ([SPARK-51658]) — **enabled by default**, no extension needed. Spark 4.2.0 also lands the `TIME` type across file formats, and vectorized data loading ([SPARK-55722]). None of the books cover any of this; go to the 4.2.0 docs.

---

### ⬜ I11 — SQL Scripting

**What it is:** Multi-statement SQL scripts with procedural constructs: `BEGIN...END` compound bodies, local variable declarations (`DECLARE`, `SET`), `IF...THEN...ELSIF...ELSE`, `CASE` (searched and simple), `WHILE`, `FOR`, `LOOP`, `REPEAT...UNTIL`, and `LEAVE`/`ITERATE` for loop control. New in Spark 4.0.

**Why you need it:** SQL scripting lets you express multi-step procedural logic — conditional branches, loops, intermediate variables — entirely in SQL without switching to Python. Useful for complex ETL stored as SQL scripts and for interoperability with data warehouses that already use procedural SQL.

**Learn it with:**

1. **Spark-docs → SQL Scripting** ([sql-ref-scripting.html](https://spark.apache.org/docs/latest/sql-ref-scripting.html)) — the canonical reference; covers all statement types with examples
2. **Spark 4.0 release notes** — understand which constructs were added in 4.0 vs 4.1
3. **Source** — `sql/catalyst/.../parser/SqlBaseParser.g4` for the grammar; the scripting execution lives under `sql/core/.../scripting/`

!!! info "No book covers this — docs and source only"
    SQL scripting landed in Spark 4.0, after every book in the resources table. Rioux (2022), LS2e (2020) and SDG (2018) have nothing on it. Treat the docs page as primary and verify behaviour against your own 4.2.0 stack rather than waiting for a book to catch up.

**Milestone:** You can write a SQL script that declares a variable, iterates over a cursor with `FOR`, applies a conditional with `IF...ELSIF`, and produces a result — and explain when you would choose SQL scripting over a Python pipeline.

---

### ⬜ I15 — Apache Iceberg and Table-Format Interoperability

**What it is:** The Iceberg table format — metadata tree (catalog → metadata file → manifest list → manifests), snapshots, hidden partitioning and partition evolution, schema evolution, the REST Catalog specification; how it compares to Delta Lake, and the interoperability layers (Delta UniForm, Iceberg's own catalog spec) that let one copy of the data serve several engines.

**Why you need it:** This path teaches Delta everywhere else, which reflects the Databricks certification track. The wider market has moved: Iceberg is the default choice for new open lakehouses, its REST Catalog is the de-facto interoperability standard, and every major platform — AWS, Snowflake, Google, and Databricks itself via UniForm — now reads and writes it. Delta fluency alone increasingly reads as Databricks-specific fluency. The concepts transfer (both are metadata-over-Parquet with snapshot isolation); the file layouts, catalog models, and operational commands do not.

**Learn it with:**

1. **Iceberg-DG Ch 2–3** — *Apache Iceberg: The Definitive Guide*, Shiran, Hughes & Merced (O'Reilly, 2024) — the architecture and metadata tree, then the read/write query lifecycle; the clearest treatment of why the manifest layout enables planning that Hive-style partitioning cannot. **Ch 5** covers catalogs (see E5). The publisher (Dremio) hosts a [free full PDF](https://www.dremio.com/wp-content/uploads/2023/02/apache-iceberg-TDG_ER1.pdf)
2. **DLDG Ch 1** — re-read the Delta transaction log chapter *after* the Iceberg metadata tree; the contrast is what makes both stick
3. **Iceberg-docs → Spark Getting Started** ([iceberg.apache.org/docs/latest/spark-getting-started/](https://iceberg.apache.org/docs/latest/spark-getting-started/)) — catalog configuration and the runtime jar, which is the part that actually blocks beginners
4. **Iceberg-docs → Multi-Engine Support** ([iceberg.apache.org/multi-engine-support/](https://iceberg.apache.org/multi-engine-support/)) — the authoritative Spark-version support matrix; check it before choosing a runtime jar
5. **Local stack** — create the same dataset as both a Delta and an Iceberg table, then diff the on-disk metadata directories

!!! warning "Iceberg does not support Spark 4.2 yet — check before you start"
    As of Iceberg 1.11.0 (May 2026), the newest supported Spark is **4.1** (`iceberg-spark-runtime-4.1_2.13`); 3.5 and 4.0 are also Maintained. There is no 4.2 runtime jar, so this topic cannot be practised on the 4.2.0 stack the rest of this path targets. Either run a separate Spark 4.1 environment for this topic, or defer it until an Iceberg release adds 4.2. Re-check the multi-engine support page rather than assuming — this is the fastest-moving fact on this page.

**Milestone:** You can create an Iceberg table from Spark, evolve its partitioning without rewriting the data, query a previous snapshot, and explain — pointing at the actual files — how Iceberg's manifest tree and Delta's `_delta_log` differ in how a reader discovers which data files belong to the current snapshot. You can state what UniForm does and does not solve.

---

### ✅ Intermediate Checkpoint

You are ready to leave this level when you can:

- Build a medallion pipeline with MERGE INTO upserts
- Use window functions for time-series feature engineering
- Read a Spark UI physical plan and locate the bottleneck
- Write and test a pandas UDF

*Optional:* this is the natural point for the Databricks Associate Developer exam if you want it — see [optional certification milestones](#optional-certification-milestones). Not a prerequisite for continuing.

---

---


### ⬜ I14 — AsyncRDDActions: Non-Blocking Job Submission

> Discovered from source sweep (refinement): `core: async-rdd-actions`

**What it is:** AsyncRDDActions wraps countAsync, collectAsync, takeAsync, foreachAsync, and foreachPartitionAsync, each returning a FutureAction backed by SparkContext.submitJob rather than runJob.

**Why you need it:** Relevant for workloads that interleave Spark jobs with I/O; takeAsync implements a recursive-future scan with configurable scale-up, making its partition-scan behavior non-obvious.

**Learn it with:**

1. **Spark-docs → Job Scheduling** ([job-scheduling.html](https://spark.apache.org/docs/latest/job-scheduling.html)) — scheduling *within* an application, the FAIR scheduler, and pools; async actions are how you get concurrent jobs from one driver thread
2. **SDG Ch 15** — how Spark runs on a cluster; the job/stage/task model that concurrent submission operates on
3. **Source** — `core/src/main/scala/org/apache/spark/rdd/AsyncRDDActions.scala`; trace `takeAsync` for the recursive scale-up

!!! info "No book covers this — docs and source only"
    No book in the resources table covers `AsyncRDDActions` directly. SDG Ch 15 gives the execution model it builds on, but the async API itself is docs-and-source territory.

**Milestone:** You can submit two Spark jobs concurrently from one driver, explain what a `FutureAction` gives you that a blocking action does not, and describe how `takeAsync` decides how many partitions to scan next.

---


### ⬜ I13 — Closure Cleaning and the Task-Not-Serializable Problem

> Discovered from source sweep (refinement): `core: closure-cleaning`

**What it is:** SparkContext.clean() delegates to ClosureCleaner (ASM 9 bytecode analysis) to null out unreferenced outer-object fields in Scala closures before they are serialized to executors.

**Why you need it:** Every transformation lambda passes through closure cleaning; failures here produce the ubiquitous Task not serializable error, and understanding the mechanism is required to reason about what driver-side state leaks into tasks.

**Learn it with:**

1. **Spark-docs → Understanding closures** ([rdd-programming-guide.html#understanding-closures](https://spark.apache.org/docs/latest/rdd-programming-guide.html#understanding-closures)) — the canonical explanation of why mutating a driver variable inside a transformation silently does nothing
2. **SDG Ch 14** — distributed shared variables; broadcast and accumulators as the correct alternatives to capturing driver state
3. **Source** — `core/src/main/scala/org/apache/spark/util/ClosureCleaner.scala`

**Milestone:** You can explain why a counter incremented inside `foreach` stays zero on the driver, predict whether a given lambda will raise `Task not serializable` before running it, and name the two fixes (broadcast the value, or move construction inside the closure).

---


### ⬜ I12 — Pair RDD Aggregations: combineByKey, reduceByKey, groupByKey

> Discovered from source sweep (refinement): `core: pair-rdd-functions`

**What it is:** PairRDDFunctions adds key-value operations to RDD[(K,V)] via implicit conversion; all aggregations bottom out in combineByKeyWithClassTag, which either applies in-place or routes through ShuffledRDD.

**Why you need it:** The cost difference between reduceByKey (map-side combine) and groupByKey (no combine) is the canonical RDD-level skew and OOM lesson; understanding combineByKey explains every higher-level shuffle.

**Learn it with:**

1. **SDG Ch 13** — advanced RDDs; key-value operations and the aggregation family in full
2. **Spark-docs → Shuffle operations** ([rdd-programming-guide.html#shuffle-operations](https://spark.apache.org/docs/latest/rdd-programming-guide.html#shuffle-operations)) — what a shuffle costs and which operations trigger one
3. **Source** — `core/src/main/scala/org/apache/spark/rdd/PairRDDFunctions.scala`; every aggregation bottoms out in `combineByKeyWithClassTag`

**Milestone:** You can explain why `reduceByKey` beats `groupByKey().mapValues(sum)` in terms of what crosses the network, and express both as a `combineByKey` call with its three functions.

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
4. **Spark-docs → SQL Performance Tuning** ([sql-performance-tuning.html](https://spark.apache.org/docs/latest/sql-performance-tuning.html)) — `EXPLAIN EXTENDED`, join hints, AQE config; pair with the [EXPLAIN syntax reference](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-explain.html) for what each mode prints

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

!!! note "New in Spark 4.2.0 — `NEAREST BY` and DSv2 partition-stats filtering"
    `NEAREST BY` ([SPARK-56395]) adds a top-K ranking join with its own physical strategy — see B7. Data Source V2 also gained enhanced partition-stats filtering ([SPARK-55596]) and `TABLESAMPLE SYSTEM` block sampling with DSv2 pushdown ([SPARK-55978]), both of which change what the planner can prune before a join.

---

### ⬜ A4 — Data Skew and Shuffle Optimisation

**What it is:** Why some partitions take 10× longer than others; salting keys; `SKEW HINT`; shuffle partition tuning; `spark.sql.shuffle.partitions`; spill to disk.

**Why you need it:** Data skew is the most common cause of slow Spark jobs and OOM errors in production.

**Learn it with:**

1. **ADEB Module 3** — managing skew and shuffles; the most practical treatment
2. **LS2e Ch 7** — scaling for large workloads; shuffle management
3. **SDG Ch 19** — performance tuning; shuffle configuration
4. **Spark-docs → Optimizing Skew Join** ([sql-performance-tuning.html#optimizing-skew-join](https://spark.apache.org/docs/latest/sql-performance-tuning.html#optimizing-skew-join)) — what AQE now handles for you, with the thresholds that decide when it kicks in; read alongside [Splitting skewed shuffle partitions](https://spark.apache.org/docs/latest/sql-performance-tuning.html#splitting-skewed-shuffle-partitions) before reaching for manual salting

**Milestone:** You can diagnose a skewed stage from the Spark UI task-time histogram, apply a salting strategy, and measure the improvement.

---

### ⬜ A5 — Advanced pandas UDFs and UDFs on Windows

**What it is:** Group aggregate UDF (Series→scalar) used in `.agg()` and over window specs; group map UDF (`applyInPandas`); Iterator of multiple Series; bounded vs unbounded window UDFs (Spark 3.0+).

**Why you need it:** When window functions alone can't express your logic (e.g., custom statistical models per group), pandas UDFs over windows fill the gap.

**Learn it with:**

1. **Rioux Ch 9–10** — pandas UDFs + window functions; the combination in §10.4
2. **LS2e Ch 11** — distributed ML inference using pandas UDFs
3. **Spark-docs → Apache Arrow in PySpark** ([tutorial/sql/arrow_pandas.html](https://spark.apache.org/docs/latest/api/python/tutorial/sql/arrow_pandas.html)) — Series→Scalar and the grouped-map function APIs; note 4.2.0 adds an iterator API for `GROUPED_AGG`

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
5. **Delta-docs → Table deletes, updates, and merges** ([delta-update.html](https://docs.delta.io/latest/delta-update.html)) — the full `MERGE` reference including `WHEN NOT MATCHED BY SOURCE`, automatic schema evolution, and a worked SCD Type 2 example; the authoritative version of what the books paraphrase

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
3. **Spark-docs → Structured Streaming** ([streaming/index.html](https://spark.apache.org/docs/latest/streaming/index.html)) — the watermark and state-store sections; reorganised into modular pages in Spark 4.0, so older bookmarks land on the wrong page
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
3. **Spark-docs → Testing PySpark** ([getting_started/testing_pyspark.html](https://spark.apache.org/docs/latest/api/python/getting_started/testing_pyspark.html)) — the built-in `pyspark.testing` utilities: `assertDataFrameEqual` (with `rtol` for float comparison), `assertSchemaEqual`, and worked `unittest` and `pytest` fixtures. Built in since 3.5, so reach for this before adding a dependency
4. `chispa` library docs ([github.com/MrPowers/chispa](https://github.com/MrPowers/chispa)) — the third-party alternative; still useful for its column-level assertions

**Milestone:** You can write a pytest test that creates a local SparkSession, runs a transformation function, and asserts the output DataFrame matches an expected schema and row set.

---

### ⬜ A11 — Spark Declarative Pipelines

**What it is:** A Python/SQL framework (new in Spark 4.1, runs over Spark Connect) for defining data pipelines as graphs of `MaterializedView`, `StreamingTable`, and `TemporaryView` outputs connected by `Flow` and `AutoCdcFlow` definitions. The pipeline engine handles incremental processing, dependency ordering, and restart semantics automatically.

**Why you need it:** Declarative Pipelines is Apache Spark's open-source equivalent of Databricks Delta Live Tables (DLT). It removes the boilerplate of managing incremental state, checkpoints, and pipeline dependencies manually — you declare what each dataset should contain; the engine decides how to compute it.

**Learn it with:**

1. **Spark-docs → Declarative Pipelines Programming Guide** ([declarative-pipelines-programming-guide.html](https://spark.apache.org/docs/latest/declarative-pipelines-programming-guide.html)) — the primary reference; covers `@table`, `@materialized_view`, flows, and `AutoCdcFlow`
2. **Spark 4.1 release notes** — feature scope and current limitations
3. **Local stack** — run a pipeline against your Delta Lake + Unity Catalog setup; the `pyspark.pipelines` module is available in Spark 4.1.x and later
4. **Source** — `sql/pipelines/src/main/scala/org/apache/spark/sql/pipelines/` (graph construction, `autocdc`); see the `sql/pipelines` sweep groups in the source map

!!! info "No book covers this — docs and source only"
    Declarative Pipelines is new in Spark 4.1 and has no book treatment. The closest published material is Databricks DLT documentation, which describes the proprietary predecessor: concepts transfer, but API names do not. Prefer the Apache docs and the source.

**Milestone:** You can define a three-node pipeline (raw ingest → cleaned materialized view → aggregated streaming table) using Declarative Pipelines, add an `AutoCdcFlow` for CDC ingestion, and explain how the engine determines execution order from the dependency graph.

!!! note "Updated in Spark 4.2.0 — Auto CDC"
    Declarative Pipelines gained Auto CDC for declarative SCD Type 1 upserts ([SPARK-56249]), building on the new engine-wide CDC support (see E8). Run this topic against 4.2.0, not the 4.1 feature set the topic was originally written for.

---

### ⬜ A12 — Kafka and Streaming Ingestion

**What it is:** Apache Kafka as an event backbone — topics, partitions, consumer groups, offsets, and delivery semantics; Spark's Kafka source and sink (`startingOffsets`, `maxOffsetsPerTrigger`, offset commitment via checkpoints); schema handling on the wire (Avro/Protobuf and a schema registry); and where exactly-once actually comes from in a Kafka → Spark → table pipeline.

**Why you need it:** Kafka is the standard event backbone, and streaming job descriptions name it directly — usually alongside Spark. A7 and A8 teach the streaming engine using files as a source, which is the right way to learn the semantics but not what production looks like. This topic is where Structured Streaming meets the queue it is normally attached to, and where the delivery-guarantee reasoning has to become precise: Spark's checkpoint plus an idempotent sink is what gives you effectively-once, not anything Kafka does on its own.

**Learn it with:**

1. **SDG Ch 21** — the streaming source/sink model and how Kafka fits it (predates the current connector's options; use for the model, not the parameters)
2. **Spark-docs → Structured Streaming + Kafka** ([structured-streaming-kafka-integration.html](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html)) — the authoritative option list, offset handling, and the deployment note about the connector jar
3. **Kafka docs → Design and Semantics** ([kafka.apache.org/documentation/#design](https://kafka.apache.org/documentation/#design)) — partitions, consumer groups, and the delivery-guarantee section; you cannot reason about Spark's guarantees without Kafka's
4. **Local stack** — run a single-broker Kafka in Docker, produce a synthetic event stream, and consume it with a Structured Streaming job writing to a table

**Milestone:** You can read a Kafka topic into Structured Streaming with an explicit `startingOffsets` and a rate limit, write to a Delta or Iceberg table, kill the job mid-stream and restart it without losing or duplicating rows — and explain precisely which component provided that guarantee. You can say what happens when the checkpoint is deleted but the sink table is not.

!!! info "Prerequisites: A7 and A8"
    Do not start here. The watermark, trigger and state-store semantics from A7/A8 are what make Kafka's offset model comprehensible; taken first, this topic degrades into copying connector options without understanding what they do.

---

### ✅ Advanced Checkpoint

You are ready to leave this level when you can:

- Debug a slow job using the Spark UI and fix the bottleneck
- Build a streaming pipeline with watermarks and Delta sinks
- Implement MERGE INTO with SCD Type 2 logic
- Build and evaluate an ML pipeline with cross-validation

*Optional:* the Databricks Data Engineer Associate exam maps to roughly I8–A6 plus orchestration, if you are working on that platform.

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
5. **Spark-docs → Memory Tuning** ([tuning.html#memory-tuning](https://spark.apache.org/docs/latest/tuning.html#memory-tuning)) — the unified memory model, GC tuning, and the serialization section; the current numbers, against which the books' JVM-flag advice should be treated as dated

**Milestone:** You can explain the difference between execution memory and storage memory in unified memory management, and name two causes of excessive GC in PySpark that the task memory metrics would surface.

!!! info "Runtime baseline as of Spark 4.2.0"
    Spark 4.2.0 builds and runs on **Java 25** ([SPARK-51167]) and is Scala 2.13-only — Scala 2.12 support was dropped across the whole Spark 4 line. GC behaviour on a modern JVM differs materially from what SDG Ch 19 (written against Java 8/11) describes, so treat its specific GC-flag advice as dated and verify against your own runtime.

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

!!! note "New in Spark 4.2.0 — History Server scalability"
    The History Server got scalability work in 4.2.0 ([SPARK-56287]), which matters directly for this topic's premise (debugging a completed job without the live UI). Kubernetes deployments also gained a Resource Manager API ([SPARK-56603]) and reduced control-plane overhead ([SPARK-55400]) — relevant to E2.

---

### ⬜ E4 — Delta Lake Internals: Transaction Log, MVCC, and Concurrency

**What it is:** The `_delta_log` JSON commit files; checkpoint files; snapshot isolation; optimistic concurrency control; what happens during concurrent writes; `RESTORE`; `CLONE`.

**Why you need it:** When two jobs write to the same Delta table simultaneously, you need to know which one wins, whether data is lost, and how to recover.

**Learn it with:**

1. **DLDG Ch 1** — the transaction log as a single source of truth; MVCC internals
2. **DLDG Ch 8** — row-level concurrency; deletion vectors; advanced write operations
3. **DLUR Ch 6** — time travel and `RESTORE` in full operational detail
4. **Delta-docs → Protocol spec** ([PROTOCOL.md](https://github.com/delta-io/delta/blob/master/PROTOCOL.md)) — the actual commit-file schema, checkpoint format, and reader/writer version rules; the only source that settles concurrency questions definitively. Pair with [Concurrency control](https://docs.delta.io/latest/concurrency-control.html) for the exception taxonomy

**Milestone:** You can describe what a Delta commit JSON file contains, explain what `VACUUM` removes and why running it too aggressively breaks time travel, and demonstrate resolving a `ConcurrentModificationException` during a concurrent MERGE and INSERT.

---

### ⬜ E5 — Catalogs, Governance, and Data Security

**What it is:** The catalog layer: the three-level namespace (`catalog.schema.table`), what a catalog owns versus what the table format owns, and the competing implementations — Unity Catalog, the Iceberg REST Catalog specification, Hive Metastore as the legacy baseline. On top of that: column-level access control, row filters, audit logs, table- and column-level lineage, and cross-organisation sharing.

**Why you need it:** Governance is a baseline requirement in regulated industries, and the catalog is where multi-engine interoperability is actually decided — the REST Catalog spec is why an Iceberg table can be read by Spark, Trino and a warehouse at once. Learn the *shape* of the problem (namespace, grants, lineage, sharing) rather than one vendor's console, because that is what transfers.

**Learn it with:**

1. **DLDG Ch 12–13** — governance, security, and lineage, in the Delta/Unity Catalog framing
2. **Iceberg-DG Ch 5** — catalogs as a first-class concept; the clearest treatment of why the catalog, not the file layout, is the interoperability boundary
3. **Iceberg-docs → REST Catalog spec** ([iceberg.apache.org/concepts/catalog/](https://iceberg.apache.org/concepts/catalog/)) — the de-facto interoperability standard
4. **Databricks Unity Catalog docs** ([docs.databricks.com/data-governance/unity-catalog/](https://docs.databricks.com/data-governance/unity-catalog/)) — the most complete governance implementation; the reference for row filters and column masks
5. **ADEB Module 2** — PII handling, pseudonymisation, CDF for deletion propagation (platform-specific, but the patterns generalise)

**Milestone:** You can explain what a catalog is responsible for versus the table format, name the trade-off between Unity Catalog and a REST-catalog implementation, create a row filter restricting a table to the current user's region, set column-level masking on a PII field, and trace a lineage graph from a gold table back to its sources.

---

### ⬜ E6 — Pipeline Orchestration with Dagster

**What it is:** Software-defined assets, asset dependencies, `Definitions`, `Resources` (passing `SparkSession`), partitioned assets (incremental processing), schedules, sensors (event-driven triggers), backfills.

**Why you need it:** Ad-hoc Spark scripts are not a data platform. Dagster turns your pipelines into observable, testable, re-runnable assets with lineage.

**Learn it with:**

1. **DagEss** — the full Dagster Essentials course (12 lessons, 6–10 hrs, free); the only correct place to start
2. **Dagster docs → dagster-spark / dagster-pyspark** ([docs.dagster.io](https://docs.dagster.io)) — integration docs for wrapping Spark jobs as assets
3. **DEB Module 2** — Lakeflow Jobs for Databricks-native orchestration (conceptual parallel to Dagster)

!!! info "No book covers this — course and docs only"
    Dagster has no established book. The Essentials course plus the official docs are the primary material; the asset model is enough of a departure from task-based orchestrators that Airflow books actively mislead here.

**Milestone:** You can wire the entire medallion pipeline (bronze → silver → gold → ML training) as Dagster assets with monthly partition keys, set up a sensor that triggers the silver asset when new bronze files land, and backfill a specific month's data.

---

### ⬜ E7 — CI/CD for Data Engineering

**What it is:** Git branching for data pipelines; unit + integration testing in CI; environment promotion (dev → staging → prod); Databricks Asset Bundles (DABs); GitHub Actions for pipeline deployment; parameterised job configurations.

**Why you need it:** Manual deployment of pipeline changes to production is a reliability and auditability problem. CI/CD for data is now a standard job requirement.

**Learn it with:**

1. **DEB Module 4** — DevOps for data engineering; unit testing with pytest; Git integration; DABs
2. **ADEB Module 4** — advanced CI/CD with DABs, multi-environment variable substitution, GitHub Actions
3. **SDG Ch 16** — developing Spark applications; packaging and submission
4. **Spark-docs → Submitting Applications** ([submitting-applications.html](https://spark.apache.org/docs/latest/submitting-applications.html)) — what your CI actually invokes: `spark-submit` semantics, dependency packaging, and deploy modes. The DABs layer sits on top of this, and knowing which is which is what lets you debug a failing deploy

**Milestone:** You can set up a GitHub Actions workflow that runs pytest on every PR, blocks merge if tests fail, and promotes the validated pipeline to a staging environment — using whichever deployment mechanism your target platform provides (DABs on Databricks; a packaged wheel plus `spark-submit`, or a container image, elsewhere).

!!! info "Keep the mechanism and the principle separate"
    DABs is one implementation of environment promotion, and the Databricks courses teach it as though it were the concept. The transferable parts are: pipeline code versioned in Git, tests gating the merge, environment-specific config injected rather than hardcoded, and deployment reduced to a single reproducible command. Everything else is a vendor's packaging of that. Learn `spark-submit` and the wheel/container path at least once so you can tell which layer broke when a deploy fails.

---

### ⬜ E8 — Change Data Capture (CDC) and Slowly Changing Dimensions

**What it is:** CDC patterns (full snapshot, append-only log, change data feed); `MERGE INTO` for SCD Type 1 (upsert) and Type 2 (full history with effective dates); `AUTO CDC INTO` in Lakeflow Pipelines; Delta CDF.

**Why you need it:** Source systems change — rows get updated and deleted. CDC is the standard pattern for propagating those changes through a lakehouse without reprocessing everything.

**Learn it with:**

1. **ADEB Module 1** — CDC review; SCD Type 2 with `AUTO CDC INTO`; quarantine pipelines
2. **DLDG Ch 7** — streaming CDC in and out of Delta Lake; CDF for downstream propagation
3. **DEB Module 1** — MERGE INTO patterns; incremental ingestion strategies
4. **Delta-docs → Change Data Feed** ([delta-change-data-feed.html](https://docs.delta.io/latest/delta-change-data-feed.html)) — enabling CDF, what lands in `_change_data`, the `_change_type` / `_commit_version` / `_commit_timestamp` columns, and the retention caveats. Contrast with Spark 4.2.0's engine-level `CHANGES` clause (see the callout above) — two mechanisms, different scopes

**Milestone:** You can implement a full SCD Type 2 merge that adds `effective_start`, `effective_end`, and `is_current` columns, process deletes via Delta CDF, and explain the difference between `UPDATE` and `MERGE INTO` from a transaction-log perspective.

!!! note "New in Spark 4.2.0 — CDC is now a first-class Spark feature, not just a Delta one"
    Spark 4.2.0 adds a SQL `CHANGES` clause plus DataFrame/PySpark/Connect APIs for reading row-level changes in batch *and* streaming ([SPARK-55668]), and Auto CDC in Spark Declarative Pipelines for declarative SCD Type 1 upserts ([SPARK-56249]). This is open-source Spark's answer to Databricks `AUTO CDC INTO` — the ADEB material still teaches the Databricks-specific form, which is what the DE Professional exam tests. Learn the `MERGE INTO` mechanics first; they explain what both engines do underneath.

---

### ⬜ E9 — Spark Connect and the Modern Client Architecture

**What it is:** Spark Connect (Spark 3.4+): a gRPC-based client-server protocol that separates the Python client from the Spark cluster; implications for deployment, security, and local development.

**Why you need it:** Spark Connect is the default mode in Spark 4.x (`pyspark` REPL). Understanding it is required for deploying applications in any modern Spark 4.x environment.

**Learn it with:**

1. **Spark-docs → Spark Connect** ([spark.apache.org/docs/latest/spark-connect-overview.html](https://spark.apache.org/docs/latest/spark-connect-overview.html))
2. **Databricks Spark Associate Cert** — Spark Connect is 5% of the exam; a good forcing function to study it
3. **Spark-docs → Connect gotchas** ([spark-connect-gotchas.html](https://spark.apache.org/docs/latest/spark-connect-gotchas.html)) and [app development with Connect](https://spark.apache.org/docs/latest/app-dev-spark-connect.html) — the behavioural differences that bite in practice, including what JVM access is unavailable

!!! info "No book covers this — docs only"
    Spark Connect arrived in 3.4 and became the default `pyspark` REPL mode in 4.x, after all four books. LS2e and SDG describe classic mode exclusively and never flag the distinction, which makes them quietly wrong about what a UDF can reach. Docs and your own local server are the sources here.

**Milestone:** You can explain the difference between classic mode and Connect mode, start a local Spark Connect server, connect to it from a Python client, and describe what changes in a UDF when running over Connect. Then the migration question: given a codebase, identify which parts cannot move to Connect as written, and say what each would have to become.

!!! info "Assessing a codebase for Connect: what actually blocks a migration"
    Three categories, in descending order of effort:

    1. **RDD usage — must be rewritten.** No RDD execution exists in a remote session and none is planned; the strategy is to close the *gaps that made people use RDDs* rather than to support them remotely. See the porting table under [I4](#i4-rdd-fundamentals).
    2. **Direct JVM access — must be removed.** `df._jdf`, `sc._jsc`, `spark.sparkContext` and anything reaching through Py4J. There is no JVM on the client side to reach.
    3. **Everything else — usually works unchanged.** DataFrame, SQL, Structured Streaming and MLlib are the surfaces receiving active parity work.

    A cheap way to find category 2 early: run the code against `local[*]` in Connect mode before pointing it at a real server. It fails on the same JVM-access violations without needing a cluster.

!!! warning "Correction — 'RDD API compatibility' in the 4.2.0 notes does not mean df.rdd works over Connect"
    Checked against the 4.2.0 source while tracing I4: `pyspark.sql.connect.dataframe.DataFrame.rdd` still raises `PySparkNotImplementedError`. There is no `RDD` class in the Connect client at all.

    In the release notes, "RDD API compatibility ([SPARK-55227])" is a *heading* over items like `DataFrame.zipWithIndex`, `Dataset.zipWithIndex` and `DataFrame.toJSON` — DataFrame methods that fill gaps people previously dropped to RDDs for. That is genuinely useful, but it is the opposite of RDD support: it reduces the *need* for RDDs under Connect rather than enabling them.

    Practical consequence for this topic: **anything in I4 that requires a real `RDD` is classic-mode only.** If your target environment is Connect, treat the RDD API as unavailable and reach for the DataFrame equivalents.

!!! note "What SPARK-55227 actually added"
    DataFrame-side conveniences that reduce the need to drop to RDDs under Connect: `DataFrame.zipWithIndex` and `Dataset.zipWithIndex` ([SPARK-55229], [SPARK-55228]), `DataFrame.toJSON` in the Python client ([SPARK-55090]), and `spark.read.json` accepting a DataFrame ([SPARK-56253]). Useful additions — just not RDD support.

---

### ✅ Expert Checkpoint

*Optional:* the Databricks Data Engineer Professional exam maps to roughly A6–E8.

You are operating at Expert level when you can:

- Design a governed lakehouse from scratch (medallion + a catalog with lineage — Unity Catalog, an Iceberg REST catalog, or equivalent)
- Debug a production incident using Spark metrics + History Server without the live UI
- Implement CI/CD for a multi-environment pipeline with automated tests
- Architect a streaming CDC pipeline with SCD Type 2 history and exactly-once guarantees

---

---


### ⬜ E11 — Serialization: KryoSerializer vs JavaSerializer

> Discovered from source sweep (refinement): `core: serialization`

**What it is:** KryoSerializer uses the Kryo library with a KryoPool, unsafe I/O, and optional class registration; JavaSerializer (default) uses Java object streams with periodic reset to bound stream-table memory.

**Why you need it:** Serializer choice determines shuffle and broadcast throughput; Kryo requires explicit class registration for production determinism, and misconfiguration produces cryptic NotSerializableException or data-corruption failures.

**Learn it with:**

1. **Spark-docs → Data Serialization** ([tuning.html#data-serialization](https://spark.apache.org/docs/latest/tuning.html#data-serialization)) — the official Kryo recommendation, registration, and buffer sizing
2. **SDG Ch 19** — performance tuning; serialization in the context of everything else that makes a job slow. Treat its JVM-flag specifics as dated (see E1 — 4.2.0 runs on Java 25)
3. **Source** — `core/src/main/scala/org/apache/spark/serializer/KryoSerializer.scala`

**Milestone:** You can enable Kryo with class registration, explain what `spark.kryo.registrationRequired=true` protects you from, and describe why this matters far less for pure DataFrame work than for RDDs of custom objects.

---


### ⬜ E10 — AccumulatorV2: Distributed Side-Effect Counters

> Discovered from source sweep (refinement): `core: accumulator-v2`

**What it is:** AccumulatorV2[IN,OUT] is the abstract base for user-defined accumulators registered with SparkContext; each task receives a copy(), calls add() locally, and the driver merges all copies back via merge() at task completion.

**Why you need it:** Accumulators are the only executor-to-driver side-channel in Spark; understanding the copy-merge lifecycle and countFailedValues prevents double-counting bugs on speculative execution and task retries.

**Learn it with:**

1. **Spark-docs → Accumulators** ([rdd-programming-guide.html#accumulators](https://spark.apache.org/docs/latest/rdd-programming-guide.html#accumulators)) — includes the exactly-once caveat: guaranteed only inside actions, not transformations
2. **SDG Ch 14** — distributed shared variables; accumulators and broadcast together, with custom accumulator examples
3. **Source** — `core/src/main/scala/org/apache/spark/util/AccumulatorV2.scala`; the `copy`/`add`/`merge` lifecycle

**Milestone:** You can write a custom `AccumulatorV2`, explain why an accumulator updated inside a `map` may double-count after a task retry or speculative execution while one inside `foreach` does not, and say what `countFailedValues` changes.

---

## Suggested Study Sequence

```
Beginner (B1–B9)              →  9 topics · 30–40 hrs   write correct Spark
    ↓
Intermediate (I1–I11, I15)    → 12 topics · 38–54 hrs   real data, real formats, read a plan
    ↓
Advanced (A1–A12)             → 12 topics · 44–66 hrs   make it fast, make it stream
    ↓
Expert (E1–E9)                →  9 topics · 40–60+ hrs  run it in production

Optional depth (I12–I14, E10–E11) → 5 topics, source-sweep derived; not on the main line
Optional milestones: three Databricks certifications — see the section below
```

**You are currently here:** B1–B9 + I1–I5 done (**14 of 42** main-line topics; 47 including the 5 optional-depth topics). Next: ⬜ I6 — Caching and Persistence.

**Carrying 🔄:** B1–B9 and I1–I5 — every topic with a written chapter — completed against Spark 4.1.x, now partly stale under 4.2.0. B1–B4 each carry gaps from a source-trace completeness pass as well; those are additions, not corrections.

Three contain claims that are actually *wrong* and should be cleared first: **B3** (ANSI mode is on by default, so book examples relying on a bad cast returning `null` now raise), **I3** (Arrow UDFs are default, invalidating the performance hierarchy as written), and the **B1** install chapter (Java 25 is supported; it says 17/21 only). **B2**, **B7** and **B8** are merely missing new surface — safe to read as-is, just incomplete.

**If you only do three things next:** clear I3 (it teaches a now-false performance model), do I6–I7 (caching and the Spark UI — everything in Advanced depends on being able to read a plan), then I8 with both table formats rather than Delta alone.

!!! info "About the optional-depth topics (I12–I14, E10–E11)"
    These five were derived from Spark source sweeps rather than from books, courses, or exam guides. They sit outside the main study line, still carry `Milestone: TBD`, and their only listed resource is the official docs. Treat them as reading prompts when you hit the underlying problem in practice (a `Task not serializable` error, a `groupByKey` OOM), not as sequential coursework.

---

## Optional certification milestones

These are **side-goals, not gates**. Nothing in this path requires them, and no topic is ordered around them. They are worth sitting if you work on Databricks or want a credential an employer recognises; they are worth ignoring otherwise, and skipping them costs you nothing on this path.

All three are proctored, multiple-choice, $200, English-delivered (the DE exams also in 日本語 / Português BR / 한국어), valid 2 years, no test aides. Verified 2026-07-18 against the official certification pages.

| Cert | Maps to | Domain weights | Questions / time |
|---|---|---|---|
| **Databricks Associate Developer for Apache Spark** | B1–I7 | DataFrame/DataSet API 30%, Architecture & Components 20%, Spark SQL 20%, Troubleshooting & Tuning 10%, Structured Streaming 10%, Spark Connect 5%, pandas API on Spark 5% | 45 scored / 90 min |
| **Databricks Data Engineer Associate** | I8–A6 + orchestration | Data Transformation & Modeling 22%, Data Ingestion & Loading 21%, Lakeflow Jobs 16%, Governance & Security 15%, CI/CD 10%, Troubleshooting/Monitoring/Optimization 10%, Databricks Intelligence Platform 6% | 45 scored / 90 min |
| **Databricks Data Engineer Professional** | A6–E8 | Code for Data Processing (Python & SQL) 22%, Cost & Performance Optimisation 13%, Data Transformation/Cleansing/Quality 10%, Monitoring & Alerting 10%, Security & Compliance 10%, Debugging & Deploying 10%, Data Ingestion 7%, Data Governance 7%, Data Modelling 6%, Data Sharing & Federation 5% | 59 scored / 120 min |

!!! info "Spark Associate is Python-only; the DE exams lead with SQL"
    Every code snippet on the Spark Developer Associate exam is Python. On both Data Engineer exams, data-manipulation code is given in SQL where possible and Python otherwise — so B8 and I11 carry more exam weight than their position here suggests.

!!! warning "The DE exams test the platform, not the engine"
    Only the Spark Developer Associate is really an Apache Spark exam. The two Data Engineer exams weight Lakeflow Jobs, Unity Catalog and the Databricks platform heavily — roughly a third of the DE Associate exam is platform surface with no open-source equivalent. That is a fine thing to study deliberately; it is a poor thing to let quietly reshape a Spark learning path, which is what the previous version of this page did.

---

## Sources consulted

- O'Reilly TOCs: *Learning Spark 2e*, *Spark: The Definitive Guide*, *Delta Lake: Up and Running*, *Delta Lake: The Definitive Guide*
- Databricks certification guides: [Associate Spark Developer](https://www.databricks.com/learn/certification/apache-spark-developer-associate), [DE Associate](https://www.databricks.com/learn/certification/data-engineer-associate), [DE Professional](https://www.databricks.com/learn/certification/data-engineer-professional) *(all three re-fetched 2026-07-18)*
- Databricks Academy course catalogues: [Data Engineering with Databricks](https://www.databricks.com/training/catalog/data-engineering-with-databricks-911), [Advanced DE with Databricks](https://www.databricks.com/training/catalog/advanced-data-engineering-with-databricks-971)
- [Dagster Essentials syllabus](https://courses.dagster.io/courses/dagster-essentials)
- [Apache Spark documentation](https://spark.apache.org/docs/latest/), [downloads page](https://spark.apache.org/downloads.html) *(re-fetched 2026-07-18)*
- [Spark 4.2.0 release notes](https://spark.apache.org/releases/spark-release-4-2-0.html) *(fetched 2026-07-18)*
- [ProjectPro PySpark roadmap](https://www.projectpro.io/learning-paths/pyspark-roadmap), [DataCamp PySpark guide](https://www.datacamp.com/blog/learn-pyspark)
- Taxonomy re-derivation (2026-07-18): [Iceberg multi-engine support matrix](https://iceberg.apache.org/multi-engine-support/) *(fetched — Spark 4.1 is newest supported)*, [Iceberg releases](https://iceberg.apache.org/releases/), [Dataquest — data engineering skills 2026](https://www.dataquest.io/blog/data-engineering-skills/), [InterviewStack — data engineer skills 2026](https://interviewstack.io/blog/data-engineer-skills-companies-want-2026), [Parquet VARIANT announcement](https://parquet.apache.org/blog/2026/02/27/variant-type-in-apache-parquet-for-semi-structured-data/)
- Learning-method evidence: [Dunlosky, *Strengthening the Student Toolbox*](https://www.aft.org/ae/fall2013/dunlosky) — self-explanation and retrieval practice both ≈ g 0.55, rereading rated low utility; drives the "attempt the milestone first" instruction in the header
- IBM Spark courses: [edX](https://www.edx.org/learn/apache-spark/ibm-apache-spark-for-data-engineering-and-machine-learning), [Coursera ML](https://www.coursera.org/learn/machine-learning-big-data-apache-spark)
