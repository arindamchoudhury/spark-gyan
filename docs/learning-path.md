# Learning Path: Apache Spark / PySpark

> **Last updated:** 2026-05-31
> **Current stable version:** Spark 4.1.2 (released 2026-05-21)
> **Local stack:** Spark 4.1.1 · Delta Lake OSS · Unity Catalog OSS · Dagster · MinIO
>
> Sources consulted: O'Reilly TOCs for all books below fetched directly; Databricks certification guide; Dagster Essentials syllabus; Class Central course reviews.

---

## Prerequisites

- Python 3.10+: functions, list comprehensions, decorators, basic OOP
- Pandas: read/filter/groupby/merge — comfortable but not expert
- SQL: SELECT / WHERE / GROUP BY / JOIN — fluent
- Command-line: running scripts, virtual envs, basic Docker

---

## The Path

### Stage 1 — Core PySpark: Data Analysis & Transformation ✅ IN PROGRESS

The mental model stage: distributed computation, lazy evaluation, the DataFrame API, and idiomatic PySpark code. Everything later builds on this.

---

#### Rioux — *Data Analysis with Python and PySpark* (Manning, 2022)

| | |
|---|---|
| **File** | `C:\opt\learn\spark\Data Analysis with Python and PySpark.pdf` |
| **Priority** | Essential |
| **Estimated time** | ~35 hrs total (read + code each chapter) |
| **Progress** | Chapters 1–10 done ✅; Chapters 11–14 remaining |
| **Version note** | 📌 Book targets Spark 3.2; notes in this site are adapted to Spark 4.1.1 |

**Chapter map:**

| # | Title | Status | Notes for review |
|---|---|---|---|
| 1 | Introduction | ✅ | Lazy eval + execution model — revisit before internals work |
| 2 | First data program | ✅ | |
| 3 | Submitting and scaling | ✅ | |
| 4 | Analyzing tabular data | ✅ | |
| 5 | Joining and grouping | ✅ | High reuse — worth re-reading before Stage 2 |
| 6 | JSON + complex types | ✅ | |
| 7 | PySpark + SQL | ✅ | |
| 8 | RDD and UDFs | ✅ | |
| 9 | pandas UDFs | ✅ | |
| 10 | Window functions | ✅ | |
| 11 | Query planning (Spark UI) | ⬜ | Critical before Stage 2 — do this before tuning |
| 12 | Feature prep for ML | ⬜ | |
| 13 | ML Pipelines | ⬜ | |
| 14 | Custom ML transformers | ⬜ | |

**Focus for remaining chapters:** Ch 11 is the most important — understanding the Spark UI, query plans, and AQE is the prerequisite for Stage 2. Read it carefully and open the UI against your local stack while working through examples.

---

### Stage 2 — Spark Internals, Performance & Production (~25 hrs)

After Stage 1 you write correct code. This stage makes it fast and production-ready.

---

#### Damji et al. — *Learning Spark, 2nd Edition* (O'Reilly, 2020)

| | |
|---|---|
| **URL** | [O'Reilly](https://www.oreilly.com/library/view/learning-spark-2nd/9781492050032/) |
| **Priority** | Essential |
| **Estimated time** | ~18 hrs (selective — see focus list below) |
| **Version note** | 📌 Covers Spark 3.0; core APIs, optimisation, and streaming patterns are stable through Spark 4.x. Ch 12 (Spark 3.0 epilogue) is outdated — the features it previews are now standard |

**Full chapter list:**

| # | Title | Focus? |
|---|---|---|
| 1 | Introduction to Apache Spark | Skip — Rioux Ch 1 covers this |
| 2 | Downloading and Getting Started | Skip — you're already running Spark 4.1.1 |
| 3 | Spark's Structured APIs (Catalyst, DataFrame, Dataset) | **Read** — deeper internals than Rioux |
| 4 | Spark SQL + built-in data sources (Parquet, JSON, CSV, Avro, ORC) | **Read** — data source options you'll hit constantly |
| 5 | External data sources (Hive, UDFs, JDBC, higher-order functions) | **Read** — higher-order functions section is gold |
| 6 | Spark SQL and Datasets (JVM-focused) | Skim — Python-heavy users can skip Dataset details |
| 7 | Optimizing and Tuning Spark Applications | **Read carefully** — caching, joins (broadcast/sort-merge/shuffle-hash), Spark UI, AQE |
| 8 | Structured Streaming | **Read** — best practical intro to streaming; sets up Stage 4 |
| 9 | Building Reliable Data Lakes with Spark | **Read** — Delta Lake intro, Hudi/Iceberg comparison; sets up Stage 3 |
| 10 | Machine Learning with MLlib | Defer to Stage 5 |
| 11 | Managing/Deploying ML Pipelines + MLflow + Pandas UDFs | Defer to Stage 5 |
| 12 | Epilogue: Spark 3.0 | Skip — AQE/DPP are now standard in 4.x, no longer "new" |

**Chapters to focus on in this stage: 3, 4, 5, 7.** Read 8 and 9 as previews for Stages 3 and 4 but plan to return to them.

---

#### Chambers & Zaharia — *Spark: The Definitive Guide* (O'Reilly, 2018)

| | |
|---|---|
| **URL** | [O'Reilly](https://www.oreilly.com/library/view/spark-the-definitive/9781491912201/) |
| **Priority** | Recommended (use selectively — see below) |
| **Estimated time** | ~8 hrs (selected chapters only) |
| **Version note** | 📌 Covers Spark 2.x (2018). No AQE, no Delta Lake, no pandas UDFs. Use it for depth on topics that haven't changed — structured APIs and production architecture. Don't use it as a primary source for streaming or ML |

**Full chapter list:**

| Part | Chapters | Use? |
|---|---|---|
| I — Overview | Ch 1–3 | Skip — Rioux + Learning Spark 2e cover this |
| II — Structured APIs | Ch 4–11 (DataFrame, SQL, agg, joins, data sources, Datasets) | **Reference** — Ch 6 (types) and Ch 8 (joins) are the deepest treatments of these topics anywhere |
| III — Low-level APIs | Ch 12–14 (RDDs, advanced RDDs, distributed variables) | Read Ch 12–13 if you want deeper RDD mastery; Ch 14 (Accumulators/Broadcast) fills a gap in Learning Spark 2e |
| IV — Production | Ch 15–19 (cluster, deploy, monitoring, **performance tuning**) | **Read Ch 15 and Ch 19** — how Spark runs on a cluster and the full performance tuning chapter are the most comprehensive treatments available |
| V — Streaming | Ch 20–23 | Use as supplement to Stage 4 — deep event-time and stateful processing coverage |
| VI — ML | Ch 24–31 (full ML algorithms) | Optional reference for Stage 5 |
| VII — Ecosystem | Ch 32–33 | Skip |

**Specific reading prescription for this stage: Ch 8 (joins in depth), Ch 14 (Accumulators + Broadcast), Ch 15 (cluster execution), Ch 19 (performance tuning).** ~8 hrs.

---

#### Apache Spark SQL Performance Tuning Guide

| | |
|---|---|
| **URL** | [spark.apache.org/docs/latest/sql-performance-tuning.html](https://spark.apache.org/docs/latest/sql-performance-tuning.html) |
| **Priority** | Essential reference (read alongside Learning Spark Ch 7) |
| **Estimated time** | ~1.5 hrs |
| **Focus** | AQE (Adaptive Query Execution), broadcast join threshold, dynamic partition pruning, coalesce after shuffle, join hints (`BROADCAST`, `MERGE`, `SHUFFLE_HASH`) |

---

#### Frank Kane — *Taming Big Data with Apache Spark 4 and Python — Hands On!* (Udemy)

| | |
|---|---|
| **URL** | [udemy.com/course/taming-big-data-with-apache-spark-hands-on/](https://www.udemy.com/course/taming-big-data-with-apache-spark-hands-on/) |
| **Priority** | Recommended |
| **Estimated time** | ~8 hrs |
| **Version note** | Updated for Spark 4 — current |
| **Focus** | RDD fundamentals, SparkSQL + DataFrames, Spark Connect, SparkML basics. Dense and practical — good for running real examples on your local stack while reading the books |
| **Skip** | Scala variants; SparkR sections |

---

### Stage 3 — Delta Lake & Lakehouse Architecture (~20 hrs)

You already have Delta Lake OSS + Unity Catalog in your local stack. This stage gives you the full mental model and production patterns for the format you're already running.

---

#### Haelen & Davis — *Delta Lake: Up and Running* (O'Reilly, 2023)

| | |
|---|---|
| **URL** | [O'Reilly](https://www.oreilly.com/library/view/delta-lake-up/9781098139711/) |
| **Priority** | Essential — do first |
| **Estimated time** | ~12 hrs |
| **Version note** | Covers Delta Lake 2.x; current OSS is 4.x. Core APIs (read/write/merge/time travel) are stable; performance features (liquid clustering, deletion vectors) are covered but refer to the Definitive Guide for 4.x specifics |

**Chapter list:**

| # | Title | Notes |
|---|---|---|
| 1 | Evolution of Data Architectures | Relational DB → Data Warehouse → Data Lake → Lakehouse; medallion architecture |
| 2 | Getting Started with Delta Lake | Delta format, Parquet + transaction log, write/read basics |
| 3 | Basic Operations on Delta Tables | Create, read, write, insert; partitioning; user metadata |
| 4 | Table Deletes, Updates, and Merges | DML operations; upsert via MERGE — the most practically important chapter |
| 5 | Performance Tuning | Data skipping, OPTIMIZE, ZORDER BY, liquid clustering — run all examples on `unity.default.*` |
| 6 | Time Travel | Versioning, RESTORE, VACUUM, data retention — also the audit trail for your pipelines |
| 7+ | Schema handling, streaming | Read; streaming section complements Stage 4 |

---

#### Lee, Das, Jaiswal — *Delta Lake: The Definitive Guide* (O'Reilly, 2024)

| | |
|---|---|
| **URL** | [O'Reilly](https://www.oreilly.com/library/view/delta-lake-the/9781098151935/) |
| **Priority** | Recommended — read after *Up and Running* |
| **Estimated time** | ~10 hrs (selective) |
| **Version note** | Covers Delta Lake 3.x — most current book available |

**Chapter list:**

| # | Title | Focus? |
|---|---|---|
| 1 | Introduction to Delta Lake Lakehouse Format | **Read** — transaction log internals, MVCC, Delta UniForm; deeper than *Up and Running* |
| 2 | Installing Delta Lake | Skim — you already have it running |
| 3 | Essential Operations | Skim — covered in *Up and Running* |
| 4 | Diving into the Delta Lake Ecosystem (Flink, Kafka, Trino) | **Read** — important for understanding Delta beyond PySpark |
| 5 | Maintaining Your Delta Lake | **Read** — VACUUM strategy, table health monitoring |
| 6 | Building Native Applications (Python/Rust/DataFusion) | Optional — read if you want non-Spark Delta access |
| 7 | Streaming In and Out | **Read** — Delta as streaming source/sink; dovetails with Stage 4 |
| 8 | Advanced Features | **Read** — deletion vectors, row-level concurrency, column mapping |
| 9 | Architecting Your Lakehouse | **Read** — medallion patterns, table design decisions |
| 10 | Performance Tuning | **Read** — liquid clustering GA details, data skipping internals |
| 11 | Successful Design Patterns | **Read** — practical patterns for the architecture you're building |
| 12 | Governance and Security | Read once Unity Catalog is in active use |
| 13 | Metadata, Lineage | Skim for now; revisit with Dagster lineage |
| 14 | Delta Sharing | Optional — relevant if sharing data across teams/orgs |

**Priority chapters for this stage: 1, 4, 5, 7, 8, 9, 10, 11.** ~8 hrs.

---

#### Delta Lake Official Quickstart

| | |
|---|---|
| **URL** | [docs.delta.io/latest/quick-start.html](https://docs.delta.io/latest/quick-start.html) |
| **Priority** | Essential hands-on (do before or during the books) |
| **Estimated time** | ~1 hr |
| **Focus** | Write your first Delta table against `unity.default.<table>` on your local stack; verify time travel and DESCRIBE HISTORY work; run OPTIMIZE |

---

### Stage 4 — Structured Streaming (~14 hrs)

Real-time and near-real-time pipelines. Learning Spark Ch 8 gave you the overview; now go deep.

---

#### Apache Spark Structured Streaming Programming Guide

| | |
|---|---|
| **URL** | [spark.apache.org/docs/latest/streaming/index.html](https://spark.apache.org/docs/latest/streaming/index.html) |
| **Priority** | Essential |
| **Estimated time** | ~4 hrs |
| **Version note** | Reorganised into modular pages in Spark 4.0 — use the 4.1.x index, not the old single-page guide |
| **Focus** | Input sources and output sinks, output modes (append / update / complete), triggers (micro-batch, continuous), watermarking for late data, stateful aggregations, streaming-to-Delta sinks |
| **Skip** | Legacy DStream streaming guide (`streaming-programming-guide.html`) — that API is deprecated |

---

#### Chambers & Zaharia — *Spark: The Definitive Guide*, Chapters 20–23 (Streaming)

| | |
|---|---|
| **Priority** | Recommended |
| **Estimated time** | ~4 hrs |
| **Focus** | Ch 20 (stream processing fundamentals — event vs processing time); Ch 21 (Structured Streaming basics); Ch 22 (event-time + stateful processing — watermarks, session windows); Ch 23 (streaming in production — checkpointing, restarts, triggers) |
| **Version note** | 📌 The API hasn't changed materially from 2.x to 4.x for the patterns here; chapter is still the most rigorous treatment of event-time semantics available |

---

#### Learning Spark 2e — Chapter 8 (Structured Streaming)

| | |
|---|---|
| **Priority** | Recommended (read before or alongside the official guide) |
| **Estimated time** | ~2 hrs |
| **Focus** | Streaming data sources/sinks; stateful aggregations; streaming joins; performance tuning for streaming |

---

### Stage 5 — ML Pipelines with Spark (~14 hrs)

Rioux Chs 12–14 introduce the Transformer/Estimator/Pipeline mental model. This stage goes deeper and connects to MLflow for experiment tracking.

---

#### Rioux — Chapters 12–14

| | |
|---|---|
| **Priority** | Essential prerequisite for this stage |
| **Estimated time** | ~5 hrs (part of Stage 1 reading log) |
| **Focus** | Ch 12: feature engineering (imputation, scaling, binary features, correlation filtering); Ch 13: Pipeline API, LogisticRegression, CrossValidator, ROC curve; Ch 14: custom Transformers and Estimators |

---

#### Apache Spark MLlib Guide

| | |
|---|---|
| **URL** | [spark.apache.org/docs/latest/ml-guide.html](https://spark.apache.org/docs/latest/ml-guide.html) |
| **Priority** | Essential reference |
| **Estimated time** | ~3 hrs (skim entire guide; deep-read ML Pipelines section) |
| **Focus** | Full transformer/estimator catalogue; `CrossValidator` + `TrainValidationSplit`; model persistence (`save`/`load`); `PipelineModel` chaining. Note: the RDD-based `mllib` API (`mllib-guide.html`) is in maintenance mode — use the DataFrame-based `ml` API only |

---

#### Damji et al. — *Learning Spark 2e*, Chapters 10–11 (MLlib + MLflow)

| | |
|---|---|
| **Priority** | Recommended |
| **Estimated time** | ~4 hrs |
| **Focus** | Ch 10: end-to-end ML pipeline example (feature prep → hyperparameter tuning → evaluation); Ch 11: MLflow experiment tracking, model registry, Pandas UDFs for distributed inference |

---

### Stage 6 — Orchestration with Dagster (~10 hrs)

Dagster is already in your local stack. This stage converts ad-hoc PySpark scripts into observable, scheduled, testable data pipelines.

---

#### Dagster Essentials (Official Free Course)

| | |
|---|---|
| **URL** | [courses.dagster.io/courses/dagster-essentials](https://courses.dagster.io/courses/dagster-essentials) |
| **Priority** | Essential |
| **Estimated time** | ~8 hrs (6–10 hrs per Dagster's own estimate) |
| **Modules** | 12 lessons: software-defined assets → asset dependencies → Definitions → Resources → Schedules → Partitions + Backfills → Sensors → Capstone |
| **Focus** | Assets and Dependencies (Lessons 3–4) are the core mental model — everything else builds on this. Partitions (Lesson 8) is the most important lesson for data engineering: it maps directly to Delta table partitions and incremental processing |
| **Skip** | Nothing — the course is well-paced and dense |

---

#### Dagster Official Docs: Integrations

| | |
|---|---|
| **URL** | [docs.dagster.io](https://docs.dagster.io) |
| **Priority** | Recommended reference |
| **Estimated time** | ~2 hrs |
| **Focus** | `dagster-spark` and `dagster-pyspark` integration docs; how to wrap a PySpark job as a Dagster asset; passing `SparkSession` as a Resource |

---

### Stage 7 — Certification (Optional Milestone)

A concrete external validation of Stage 1–4 knowledge. Useful as a forcing function for systematic review.

---

#### Databricks Certified Associate Developer for Apache Spark

| | |
|---|---|
| **URL** | [databricks.com/learn/certification/apache-spark-developer-associate](https://www.databricks.com/learn/certification/apache-spark-developer-associate) |
| **Priority** | Optional |
| **Estimated time** | ~10 hrs prep (if Stages 1–4 are done) |
| **Format** | 45 multiple-choice questions · 90 minutes · $200 · online-proctored |
| **Exam topic breakdown** | DataFrame/DataSet API 30% · Architecture 20% · Spark SQL 20% · Troubleshooting/Tuning 10% · Structured Streaming 10% · Spark Connect 5% · pandas API on Spark 5% |
| **When to attempt** | After completing Stages 1–4. The weak spots for most candidates are architecture (physical plan execution, shuffle mechanics) and Spark Connect — cover these before sitting |
| **Prep resource** | Udemy: [Databricks Certified Associate Developer for Apache Spark](https://www.udemy.com/course/databricks-certified-associate-developer-for-apache-spark/) (~6 hrs, practice exams included) |

---

## Quick Reference Table

| Resource | Type | Stage | Priority | Est. hrs | Version |
|---|---|---|---|---|---|
| Rioux — *Data Analysis with Python and PySpark* | Book | 1 | Essential | 35 hrs | Spark 3.2 📌 |
| Damji et al. — *Learning Spark, 2nd Ed.* (Ch 3–9) | Book (selective) | 2 | Essential | 18 hrs | Spark 3.0 📌 |
| Chambers & Zaharia — *Spark: The Definitive Guide* (Ch 8, 14, 15, 19) | Book (selective) | 2 | Recommended | 8 hrs | Spark 2.x 📌 |
| Spark SQL Performance Tuning Guide | Official docs | 2 | Essential | 1.5 hrs | 4.1.2 |
| Frank Kane — *Taming Big Data with Spark 4* (Udemy) | Video course | 2 | Recommended | 8 hrs | Spark 4 ✅ |
| Haelen & Davis — *Delta Lake: Up and Running* | Book | 3 | Essential | 12 hrs | Delta 2.x 📌 |
| Lee et al. — *Delta Lake: The Definitive Guide* (selected ch) | Book (selective) | 3 | Recommended | 10 hrs | Delta 3.x |
| Delta Lake Official Quickstart | Official docs | 3 | Essential | 1 hr | Latest |
| Spark Structured Streaming Guide | Official docs | 4 | Essential | 4 hrs | 4.1.2 |
| *Spark: The Definitive Guide* Ch 20–23 | Book chapters | 4 | Recommended | 4 hrs | Spark 2.x 📌 |
| *Learning Spark 2e* Ch 8 | Book chapter | 4 | Recommended | 2 hrs | Spark 3.0 📌 |
| Rioux Ch 12–14 | Book chapters | 5 | Essential | 5 hrs | Spark 3.2 📌 |
| Spark MLlib Guide | Official docs | 5 | Essential | 3 hrs | 4.1.2 |
| *Learning Spark 2e* Ch 10–11 | Book chapters | 5 | Recommended | 4 hrs | Spark 3.0 📌 |
| Dagster Essentials (official course) | Course | 6 | Essential | 8 hrs | Current |
| Dagster integrations docs | Official docs | 6 | Recommended | 2 hrs | Current |
| Databricks Spark Associate certification | Certification | 7 | Optional | 10 hrs prep | — |

**Total (essential only): ~92 hrs across all stages**
**Total (including recommended): ~130 hrs**

---

## What to build as you learn

Each project uses your local stack (`C:\opt\learn\spark\spark-delta-unitycatalog`).

**After Stage 1 (finish Rioux Ch 14):**
Build a feature engineering pipeline over GSOD weather data: window functions for rolling averages and lag features, write output to `unity.default.gsod_features` as a Delta table. Use the Spark UI to inspect the plan.

**After Stage 2 (Learning Spark + tuning):**
Take a slow query from Stage 1's pipeline and optimise it — diagnose the shuffle stages in the Spark UI, add a broadcast hint for a small lookup table, enable AQE, compare execution time and plan before/after.

**After Stage 3 (Delta Lake):**
Build a three-layer medallion pipeline: bronze (raw GSOD Parquet → Delta, no schema changes), silver (clean + cast + drop sentinels → Delta with schema enforcement), gold (station-monthly aggregates with ZORDER on `stn`). Run VACUUM and DESCRIBE HISTORY; restore silver to a previous version.

**After Stage 4 (Streaming):**
Add a streaming layer to the medallion pipeline: write new GSOD Parquet files to a watch directory; a Structured Streaming job reads them with a file source, applies a watermark, and appends to the bronze Delta table in append mode. Confirm DESCRIBE HISTORY shows the streaming micro-batches.

**After Stage 5 (ML):**
Train a temperature anomaly detector: build a feature vector from the GSOD silver table, fit a `LinearRegression` inside a `Pipeline`, tune with `CrossValidator`, log runs to MLflow, load the best model and write predictions to `unity.default.gsod_predictions`.

**After Stage 6 (Dagster):**
Wire the entire medallion + ML pipeline into Dagster: each Delta layer is an Asset, the streaming job is a separate long-running asset, the ML training is a partitioned asset (partition key = year). Add a schedule to re-train monthly.

---

## Sources

- [Learning Spark, 2nd Edition — O'Reilly](https://www.oreilly.com/library/view/learning-spark-2nd/9781492050032/)
- [Spark: The Definitive Guide — O'Reilly](https://www.oreilly.com/library/view/spark-the-definitive/9781491912201/)
- [Delta Lake: Up and Running — O'Reilly](https://www.oreilly.com/library/view/delta-lake-up/9781098139711/)
- [Delta Lake: The Definitive Guide — O'Reilly](https://www.oreilly.com/library/view/delta-lake-the/9781098151935/)
- [Databricks Associate Spark Developer exam guide](https://www.databricks.com/learn/certification/apache-spark-developer-associate)
- [Dagster Essentials course](https://courses.dagster.io/courses/dagster-essentials)
- [Spark 4.1.2 official documentation](https://spark.apache.org/docs/latest/)
- [Taming Big Data with Apache Spark 4 — Udemy (Frank Kane)](https://www.udemy.com/course/taming-big-data-with-apache-spark-hands-on/)
