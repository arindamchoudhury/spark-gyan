# The Spark Book

Personal book written as I work through the [learning path](../learning-path-v2.md). Each chapter is my own synthesis of a topic — my own verification against the official docs, not a summary of any single source.

!!! warning "Chapters 02–16 were written against Spark 4.1.x; current stable is 4.2.0"
    Spark 4.2.0 shipped 2026-07-14, after every chapter below was written. Chapter 01 has been revised against 4.2.0; the remaining fifteen still carry a `Spark 4.1.x` line in their header that needs bumping. Several have substantive drift and are marked 🔄 in the table — start there. The rest are a version-string edit only; correct them as you next touch each chapter rather than in one sweep.

    Four chapters carry claims that are outright **wrong** rather than merely dated, and those come first: **Ch 02** (a word-count walkthrough describing a global-sort shuffle Spark does not plan, and "one action = one job" stated as an invariant), **Ch 03** (Java 17/21 only — 4.2.0 supports Java 25), **Ch 06** (ANSI mode is on by default, so examples relying on a bad cast returning `null` now raise), and **Ch 15** (Arrow UDFs are the default, invalidating the performance hierarchy as written). Each chapter's own banner says which kind of drift it has.

**Status key:** ✅ written and current · 🔄 written, needs revisiting (see the banner at the top of the chapter) · ⬜ not yet written

| Status | Ch | Topic code | Title | Written |
|---|---|---|---|---|
| ✅ | 01 | B1 | [Introduction to Spark](ch01-introduction-to-spark.md) | 2026-06-05 · rev 2026-08-10 |
| 🔄 | 02 | B1 | [Spark Architecture and the Execution Model](ch02-spark-architecture.md) | 2026-06-05 |
| 🔄 | 03 | B1 | [Spark Installation](ch03-spark-installation.md) | 2026-06-05 |
| 🔄 | 04 | B2 | [SparkSession and Entry Points](ch04-sparksession.md) | 2026-06-05 |
| 🔄 | 05 | I16 | [RDD Fundamentals](ch05-rdds.md) | 2026-05-31 |
| 🔄 | 06 | B3 | [The DataFrame API: Basics](ch06-dataframe-basics.md) | 2026-05-31 |
| 🔄 | 07 | B10 | [Reading and Writing Data](ch07-reading-writing-data.md) | 2026-05-31 |
| 🔄 | 08 | B4 | [Schema: StructType, DDL, and Type Safety](ch08-schema-type-safety.md) | 2026-05-31 |
| 🔄 | 09 | B7 | [Aggregations and GroupBy](ch09-aggregations-groupby.md) | 2026-05-31 |
| 🔄 | 10 | B8 | [Joins: Types and Mechanics](ch10-joins.md) | 2026-05-31 |
| 🔄 | 11 | B11 | [Spark SQL](ch11-spark-sql.md) | 2026-05-31 |
| 🔄 | 12 | B6 | [Null Handling](ch12-null-handling.md) | 2026-05-31 |
| 🔄 | 13 | I1 | [Complex Types: Arrays, Maps, and Structs](ch13-complex-types.md) | 2026-05-31 |
| 🔄 | 14 | I8 | [Window Functions](ch14-window-functions.md) | 2026-05-31 |
| 🔄 | 15 | I10 | [User-Defined Functions: Python and pandas UDFs](ch15-udfs.md) | 2026-05-31 |
| 🔄 | 16 | I24 | [Partitioning: Concepts and Control](ch16-partitioning.md) | 2026-05-31 |
| ⬜ | 17 | I25 | Caching and Persistence | — |
| ⬜ | 18 | I26 | The Spark UI | — |
| ⬜ | 19 | I37 | Delta Lake Basics | — |
| ⬜ | 20 | I39 | The Medallion Architecture | — |
| ⬜ | 21 | I36 | Data Formats: Parquet, Delta, Avro, JSON | — |
| ⬜ | 22 | A1 | Query Optimisation: Catalyst and the Physical Plan | — |
| ⬜ | 23 | A11 | Adaptive Query Execution | — |
| ⬜ | 24 | A15 | Join Strategies and Tuning | — |
| ⬜ | 25 | A18 | Data Skew and Shuffle Optimisation | — |
| ⬜ | 26 | A24 | Advanced pandas UDFs and UDFs over Windows | — |
| ⬜ | 27 | A39 | Delta Lake Advanced Operations | — |
| ⬜ | 28 | A32 | Structured Streaming: Fundamentals | — |
| ⬜ | 29 | A34 | Structured Streaming: Stateful Processing | — |
| ⬜ | 30 | A44 | ML Pipelines with Spark MLlib | — |
| ⬜ | 31 | A43 | Testing PySpark Pipelines | — |
| ⬜ | 32 | E1 | Spark Internals: Memory, Execution, Serialisation | — |
| ⬜ | 33 | E15 | Production Deployment: Cluster Management | — |
| ⬜ | 34 | E24 | Observability: Monitoring, Alerting, Logging | — |
| ⬜ | 35 | E33 | Delta Lake Internals: Transaction Log and MVCC | — |
| ⬜ | 36 | E29 | Data Governance: Unity Catalog and Lineage | — |
| ⬜ | 37 | E47 | Pipeline Orchestration with Dagster | — |
| ⬜ | 38 | E48 | CI/CD for Data Engineering | — |
| ⬜ | 39 | E46 | Change Data Capture and Slowly Changing Dimensions | — |
| ⬜ | 40 | E26 | Spark Connect and the Modern Client Architecture | — |
| ⬜ | 41 | I38 | Apache Iceberg and Table-Format Interoperability | — |
| ⬜ | 42 | A35 | Kafka and Streaming Ingestion | — |
