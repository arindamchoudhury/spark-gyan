# Learning Path: Apache Spark / PySpark

> **Last updated:** 2026-05-31
> **Current stable version:** Spark 4.1.2 (released 2026-05-21)
> **Local stack:** Spark 4.1.1 · Delta Lake · Unity Catalog OSS · Dagster · MinIO

---

## Prerequisites

- Python 3.10+: comfortable with functions, list comprehensions, basic OOP
- Pandas: basic DataFrame operations (reading, filtering, groupby)
- SQL: SELECT, WHERE, GROUP BY, JOIN — fluency, not mastery
- Basic command-line comfort (running scripts, virtual envs)

---

## The Path

### Stage 1: Core PySpark — Data Analysis & Transformation ✅ IN PROGRESS (~40 hrs total)

This stage builds the mental model: how Spark distributes work, how the DataFrame API maps to SQL, and how to express data transformations cleanly in Python.

#### Rioux — *Data Analysis with Python and PySpark* (Manning, 2022)

- **Type:** Book
- **File:** `C:\opt\learn\spark\Data Analysis with Python and PySpark.pdf`
- **Priority:** Essential
- **Time:** ~35 hrs (read + code each chapter)
- **Progress:** Chapters 1–10 done ✅; Chapters 11–14 remaining
- **Focus:** Ch 5 (joins/groupby), Ch 6 (complex types), Ch 9 (pandas UDFs), Ch 10 (window functions) — these patterns appear everywhere in real code
- **Skip:** Ch 1 history narrative (already done); exercises are optional but Ch 10 exercises on windows are worth attempting
- **Version note:** 📌 Covers Spark 3.2; notes in this site are adapted to Spark 4.1.1

#### Apache Spark Official Quick Start

- **Type:** Docs walkthrough
- **URL:** [spark.apache.org/docs/latest/quick-start.html](https://spark.apache.org/docs/latest/quick-start.html)
- **Priority:** Recommended
- **Time:** ~1 hr
- **Focus:** Skim while reading Rioux — useful to see the same concepts in the official voice. The PySpark User Guide (`/api/python/user_guide/`) is the canonical API reference.
- **Skip:** Scala/Java sections

---

### Stage 2: Spark Internals & Performance (~25 hrs total)

Rioux gives you the "what"; this stage gives you the "why". After Stage 1 you write correct code; after Stage 2 you write *fast* code.

#### Damji et al. — *Learning Spark, 2nd Edition* (O'Reilly, 2020)

- **Type:** Book
- **URL:** [O'Reilly](https://www.oreilly.com/library/view/learning-spark-2nd/9781492050032/)
- **Priority:** Essential
- **Time:** ~20 hrs
- **Focus:**
    - Ch 3–4: Structured API and Spark SQL (consolidates Rioux Chs 4–7 at a deeper level)
    - Ch 7: Optimizing and Tuning Spark Applications — the most practically important chapter
    - Ch 8: Structured Streaming introduction
    - Ch 9: Building Reliable Data Lakes with Apache Spark (Delta Lake intro)
    - Ch 11–12: MLlib (builds on Rioux Chs 12–14)
- **Skip:** Ch 1–2 if Stage 1 is complete; Ch 10 (MLflow) can be deferred to Stage 5
- **Version note:** 📌 Covers Spark 3.0; core concepts and APIs are stable through Spark 4.x. Chapter on Delta Lake reflects Delta 0.7 — use Stage 3 resources for current Delta Lake.

#### Udemy — *Best Hands-on Big Data Practices with PySpark & Spark Tuning*

- **Type:** Video course
- **URL:** [udemy.com/course/best-hands-on-big-data-practices-and-use-cases-using-pyspark/](https://www.udemy.com/course/best-hands-on-big-data-practices-and-use-cases-using-pyspark/)
- **Priority:** Recommended
- **Time:** ~8 hrs
- **Focus:** Data skew handling, broadcast joins, caching strategy, partition tuning — all things that make the difference between a job that times out and one that runs in minutes
- **Skip:** Sections on Hive/Hadoop if you're on a pure Spark stack

#### Apache Spark Performance Tuning Guide

- **Type:** Official docs
- **URL:** [spark.apache.org/docs/latest/sql-performance-tuning.html](https://spark.apache.org/docs/latest/sql-performance-tuning.html)
- **Priority:** Recommended (reference while doing Rioux Ch 11 and Learning Spark Ch 7)
- **Time:** ~2 hrs
- **Focus:** AQE (Adaptive Query Execution), broadcast threshold, partition coalescing, predicate pushdown confirmation

---

### Stage 3: Delta Lake & Lakehouse Architecture (~15 hrs total)

You already have Delta Lake in your local stack. This stage gives you the mental model for why the lakehouse pattern exists and how to use Delta correctly.

#### Haelen & Davis — *Delta Lake: Up and Running* (O'Reilly, 2024)

- **Type:** Book
- **URL:** [O'Reilly](https://www.oreilly.com/library/view/delta-lake-up/9781098139711/)
- **Priority:** Essential
- **Time:** ~12 hrs
- **Focus:**
    - Ch 1: Lakehouse architecture and the medallion (bronze/silver/gold) pattern
    - Ch 2–4: Delta table operations — create, read, write, update, delete, merge
    - Ch 5: Schema evolution and enforcement
    - Ch 6: Time travel and data versioning
    - Ch 7–8: Performance — Z-ordering, data skipping, liquid clustering
- **Skip:** Databricks-specific sections if you're on OSS Delta Lake; features map directly but the UI is different

#### Delta Lake Official Getting Started

- **Type:** Docs
- **URL:** [docs.delta.io/latest/quick-start.html](https://docs.delta.io/latest/quick-start.html)
- **Priority:** Essential (do this in parallel with or before the book — hands-on first)
- **Time:** ~1 hr
- **Focus:** Write your first Delta table in your local stack (`unity.default.<table>`), verify time travel works

#### Databricks — *Guide to Apache Spark & Delta Lake* (free ebook)

- **Type:** Ebook
- **URL:** [databricks.com/resources/ebook/the-data-engineers-guide-to-apache-spark-and-delta-lake](https://www.databricks.com/resources/ebook/the-data-engineers-guide-to-apache-spark-and-delta-lake)
- **Priority:** Optional (good conceptual companion to the O'Reilly book)
- **Time:** ~2 hrs

---

### Stage 4: Structured Streaming (~12 hrs total)

Real-time and near-real-time pipelines. Rioux and Learning Spark both introduce this; this stage makes it production-ready.

#### Apache Spark Structured Streaming Programming Guide

- **Type:** Official docs
- **URL:** [spark.apache.org/docs/latest/structured-streaming-programming-guide.html](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- **Priority:** Essential
- **Time:** ~4 hrs
- **Focus:** Source/sink types, output modes (append/update/complete), watermarking for late data, stateful aggregations, triggers
- **Skip:** Kafka-specific sections unless you're adding Kafka to your stack

#### Learning Spark, 2nd Ed. — Ch 8 (Structured Streaming)

- **Type:** Book chapter (from Stage 2 resource)
- **Priority:** Recommended (read after the official guide)
- **Time:** ~2 hrs
- **Focus:** Practical patterns; more worked examples than the official guide

#### Udemy — *Apache Spark for Data Engineering Hands-On with PySpark*

- **Type:** Video course (streaming section)
- **URL:** [udemy.com/course/apache-spark-for-data-engineering-hands-on-with-pyspark/](https://www.udemy.com/course/apache-spark-for-data-engineering-hands-on-with-pyspark/)
- **Priority:** Optional
- **Time:** ~6 hrs (streaming modules only)

---

### Stage 5: ML Pipelines with Spark (~15 hrs total)

Rioux Chs 12–14 introduce MLlib. This stage goes deeper and connects to the broader MLOps picture.

#### Rioux Ch 12–14 — Feature prep, ML Pipelines, custom transformers

- **Type:** Book chapters (already in your site — complete before starting this stage)
- **Priority:** Essential prerequisite
- **Time:** ~5 hrs
- **Focus:** `Transformer` / `Estimator` / `Pipeline` mental model — this is the same pattern scikit-learn uses but distributed

#### Apache Spark MLlib Guide

- **Type:** Official docs
- **URL:** [spark.apache.org/docs/latest/ml-guide.html](https://spark.apache.org/docs/latest/ml-guide.html)
- **Priority:** Essential reference
- **Time:** ~3 hrs (skim; deep-read sections as needed)
- **Focus:** Full list of transformers and estimators; `CrossValidator` and `TrainValidationSplit`; model persistence

#### Learning Spark, 2nd Ed. — Ch 11–12 (MLlib and MLflow)

- **Type:** Book chapters (from Stage 2 resource)
- **Priority:** Recommended
- **Time:** ~4 hrs
- **Focus:** Ch 11 for deeper MLlib patterns; Ch 12 for MLflow experiment tracking (relevant if you add MLflow to your stack)

---

## Quick Reference

| Resource | Type | Stage | Priority | Est. hours | Version |
|---|---|---|---|---|---|
| Rioux — *Data Analysis with Python and PySpark* | Book | 1 | Essential | ~35 hrs | Spark 3.2 📌 |
| Spark Official Quick Start + PySpark User Guide | Docs | 1 | Recommended | ~1 hr | 4.1.2 |
| Damji et al. — *Learning Spark, 2nd Ed.* | Book | 2 | Essential | ~20 hrs | Spark 3.0 📌 |
| Udemy — Hands-on PySpark & Spark Tuning | Course | 2 | Recommended | ~8 hrs | Spark 3.x |
| Spark Performance Tuning Guide | Docs | 2 | Recommended | ~2 hrs | 4.1.2 |
| Haelen & Davis — *Delta Lake: Up and Running* | Book | 3 | Essential | ~12 hrs | Delta 3.x |
| Delta Lake Official Getting Started | Docs | 3 | Essential | ~1 hr | Latest |
| Databricks Spark + Delta Lake ebook | Ebook | 3 | Optional | ~2 hrs | — |
| Spark Structured Streaming Guide | Docs | 4 | Essential | ~4 hrs | 4.1.2 |
| Learning Spark Ch 8 | Book chapter | 4 | Recommended | ~2 hrs | Spark 3.0 📌 |
| Udemy — Spark for Data Engineering (streaming) | Course | 4 | Optional | ~6 hrs | Spark 3.x |
| Rioux Ch 12–14 | Book chapters | 5 | Essential | ~5 hrs | Spark 3.2 📌 |
| Spark MLlib Guide | Docs | 5 | Essential | ~3 hrs | 4.1.2 |
| Learning Spark Ch 11–12 | Book chapters | 5 | Recommended | ~4 hrs | Spark 3.0 📌 |

**Total estimated time:** ~105 hrs across all stages

---

## What to build as you learn

Mini-projects against your local stack (`C:\opt\learn\spark\spark-delta-unitycatalog`):

- **After Stage 1 (finish Rioux Ch 14):** Build an end-to-end feature engineering pipeline — read raw GSOD weather data → apply window functions (rolling avg, lag features) → write to a Delta table at `unity.default.gsod_features`
- **After Stage 2:** Diagnose and fix a slow Spark job using the Spark UI — identify shuffle bottlenecks, add a broadcast hint, verify the plan in the SQL tab
- **After Stage 3:** Build a medallion pipeline — bronze (raw parquet), silver (cleaned Delta), gold (aggregated Delta with Z-ordering). Explore time travel to "undo" a bad write
- **After Stage 4:** Add a streaming source — read from a watched directory, apply a watermark, write continuously to a Delta sink
- **After Stage 5:** Train a logistic regression pipeline on the GSOD dataset, log runs to MLflow, load the best model and run batch inference writing predictions to `unity.default.gsod_predictions`
