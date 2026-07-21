# Spark Feature & Capability History

This catalog tracks every feature and improvement Apache Spark shipped, from the earliest
tagged release through the current 4.x line. Each entry is organized by capability area and is
traceable to a JIRA `SPARK-*` ID, so any claim in this catalog can be checked against the
original ticket.

> Coverage: 99 releases, 0.3 → 4.2.0, generated from `release-notes/spark_all_changelogs.txt`.
> 4,636 tracked feature/improvement items across 22 capability areas. 9,354 excluded items
> (bugs, tests, sub-tasks, dependency upgrades) are recorded in the ledger below rather than
> silently dropped.

## How to read a timeline entry

Each area page has a `Timeline` table with one row per tracked item:

| Release | JIRA | Type | Title |
|---|---|---|---|
| 3.5.0 | [SPARK-12345](https://issues.apache.org/jira/browse/SPARK-12345) | New Feature | Verbatim title from the release notes |

- **Release** — the Spark version the item first shipped in.
- **JIRA** — a link to the original `SPARK-*` ticket.
- **Type** — `New Feature` or `Improvement` (bugs, tests, sub-tasks, and dependency-upgrade
  items are excluded from these tables; see the ledger below).
- **Title** — the verbatim item title as published in the release notes, unedited.

Rows are sorted oldest-to-newest release, so each table reads as that capability area's history
in order.

## Capability areas

- [Core / RDD / Scheduler](core-rdd.md)
- [SQL & Catalyst](sql-catalyst.md)
- [ANSI & Data Types](ansi-types.md)
- [Built-in Functions](builtin-functions.md)
- [Data Sources & DSv2](datasources-dsv2.md)
- [Connectors (Kafka/JDBC/Parquet/ORC/Avro)](connectors.md)
- [Structured Streaming](structured-streaming.md)
- [DStreams (legacy streaming)](dstreams.md)
- [PySpark & Python UDFs](pyspark.md)
- [pandas API on Spark](pandas-on-spark.md)
- [Arrow](arrow.md)
- [Spark Connect](spark-connect.md)
- [MLlib / ML](mllib.md)
- [GraphX](graphx.md)
- [SparkR](sparkr.md)
- [Deploy (Standalone/YARN/Mesos/K8s)](deploy.md)
- [Shuffle / Storage / Memory](shuffle-storage.md)
- [Web UI / History / Metrics](web-ui.md)
- [Security](security.md)
- [Geospatial](geospatial.md)
- [Build & Language support](build-lang.md)
- [Misc / Other](misc.md)

## Completeness & ledger

Every `<li>` item parsed out of `spark_all_changelogs.txt` ends up in exactly one of two
machine-generated ledger files:

- **`_catalog.jsonl`** — kept items (`New Feature` / `Improvement`) that feed the area page
  timelines. 4,636 records.
- **`_dropped.jsonl`** — excluded items, each tagged with a reason (bug fix, test, sub-task,
  dependency upgrade). 9,354 records.

For every dump release, `kept + dropped == total <li> count for that release` — this invariant
is checked by the build orchestrator (`release-notes/catalog/build.py`) and holds across all
releases with zero unbalanced. Nothing is silently lost between the source text and the ledgers.

> 297 `SPARK-*` IDs referenced in the source text do not correspond to their own `<li>` item:
> 279 appear only in a release's prose "Highlights" preamble (before the first `<h2>` section),
> and 18 appear as a secondary or cross-referenced ID inside another item's title (e.g. a
> multi-ID bracket, or a title that mentions a different ticket by number). These are captured
> and accounted for during the per-era prose passes, not treated as missing items.
