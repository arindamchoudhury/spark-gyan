# The Spark Book

Personal book written as I work through the [learning path](../learning-path.md). Each chapter is my own synthesis of a topic — my own verification against the official docs, not a summary of any single source.

!!! warning "Chapters 01–16 were written against Spark 4.1.x; current stable is 4.2.0"
    Spark 4.2.0 shipped 2026-07-14, after every chapter below was written. All sixteen carry a `Spark 4.1.x` line in their header that needs bumping. Four have substantive drift and are marked 🔄 in the table — start there. The rest are a version-string edit only; correct them as you next touch each chapter rather than in one sweep.

**Status key:** ✅ written and current · 🔄 written, needs revisiting (see the banner at the top of the chapter) · ⬜ not yet written

| Status | Ch | Topic code | Title | Written |
|---|---|---|---|---|
| ✅ | 01 | B1 | [Introduction to Spark](ch01-introduction-to-spark.md) | 2026-06-05 |
| 🔄 | 02 | B1 | [Spark Architecture and the Execution Model](ch02-spark-architecture.md) | 2026-06-05 |
| 🔄 | 03 | B1 | [Spark Installation](ch03-spark-installation.md) | 2026-06-05 |
| 🔄 | 04 | B2 | [SparkSession and Entry Points](ch04-sparksession.md) | 2026-06-05 |
| ✅ | 05 | I4 | [RDD Fundamentals](ch05-rdds.md) | 2026-05-31 |
| 🔄 | 06 | B3 | [The DataFrame API: Basics](ch06-dataframe-basics.md) | 2026-05-31 |
| 🔄 | 07 | B4 | [Reading and Writing Data](ch07-reading-writing-data.md) | 2026-05-31 |
| 🔄 | 08 | B5 | [Schema: StructType, DDL, and Type Safety](ch08-schema-type-safety.md) | 2026-05-31 |
| 🔄 | 09 | B6 | [Aggregations and GroupBy](ch09-aggregations-groupby.md) | 2026-05-31 |
| 🔄 | 10 | B7 | [Joins: Types and Mechanics](ch10-joins.md) | 2026-05-31 |
| 🔄 | 11 | B8 | [Spark SQL](ch11-spark-sql.md) | 2026-05-31 |
| 🔄 | 12 | B9 | [Null Handling](ch12-null-handling.md) | 2026-05-31 |
| ✅ | 13 | I1 | [Complex Types: Arrays, Maps, and Structs](ch13-complex-types.md) | 2026-05-31 |
| ✅ | 14 | I2 | [Window Functions](ch14-window-functions.md) | 2026-05-31 |
| 🔄 | 15 | I3 | [User-Defined Functions: Python and pandas UDFs](ch15-udfs.md) | 2026-05-31 |
| ✅ | 16 | I5 | [Partitioning: Concepts and Control](ch16-partitioning.md) | 2026-05-31 |
| ⬜ | 17 | I6 | Caching and Persistence | — |
| ⬜ | 18 | I7 | The Spark UI | — |
| ⬜ | 19 | I8 | Delta Lake Basics | — |
| ⬜ | 20 | I9 | The Medallion Architecture | — |
| ⬜ | 21 | I10 | Data Formats: Parquet, Delta, Avro, JSON | — |
| ⬜ | 22 | A1 | Query Optimisation: Catalyst and the Physical Plan | — |
| ⬜ | 23 | A2 | Adaptive Query Execution | — |
| ⬜ | 24 | A3 | Join Strategies and Tuning | — |
| ⬜ | 25 | A4 | Data Skew and Shuffle Optimisation | — |
| ⬜ | 26 | A5 | Advanced pandas UDFs and UDFs on Windows | — |
| ⬜ | 27 | A6 | Delta Lake Advanced: MERGE, SCD, Liquid Clustering | — |
| ⬜ | 28 | A7 | Structured Streaming: Fundamentals | — |
| ⬜ | 29 | A8 | Structured Streaming: Stateful Processing | — |
| ⬜ | 30 | A9 | ML Pipelines with Spark MLlib | — |
| ⬜ | 31 | A10 | Testing PySpark Pipelines | — |
| ⬜ | 32 | E1 | Spark Internals: Memory, Execution, Serialisation | — |
| ⬜ | 33 | E2 | Production Deployment: Cluster Management | — |
| ⬜ | 34 | E3 | Observability: Monitoring, Alerting, Logging | — |
| ⬜ | 35 | E4 | Delta Lake Internals: Transaction Log and MVCC | — |
| ⬜ | 36 | E5 | Data Governance: Unity Catalog and Lineage | — |
| ⬜ | 37 | E6 | Pipeline Orchestration with Dagster | — |
| ⬜ | 38 | E7 | CI/CD for Data Engineering | — |
| ⬜ | 39 | E8 | Change Data Capture and Slowly Changing Dimensions | — |
| ⬜ | 40 | E9 | Spark Connect and the Modern Client Architecture | — |
| ⬜ | 41 | I15 | Apache Iceberg and Table-Format Interoperability | — |
| ⬜ | 42 | A12 | Kafka and Streaming Ingestion | — |
