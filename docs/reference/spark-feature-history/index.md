# Spark Feature & Capability History

This catalog tracks every feature and improvement Apache Spark shipped, from the earliest
tagged release through the current 4.x line. Each entry is organized by capability area and is
traceable to a JIRA `SPARK-*` ID, so any claim in this catalog can be checked against the
original ticket.

> Coverage: 99 releases, 0.3 → 4.2.0, generated from `release-notes/spark_all_changelogs.txt`.
> **7,190 tracked feature/improvement items** across 22 capability areas — 4,636 parsed from the
> structured JIRA dumps of 17 releases, plus 2,554 extracted from the curated "Highlights" prose
> of every other release (including prose-only feature releases such as 4.0.0, 4.1.0, and 4.2.0,
> which ship no JIRA dump). 9,354 excluded items (bugs, tests, sub-tasks, dependency upgrades)
> are recorded in the ledger below rather than silently dropped.

## How to read a timeline entry

Each area page has a `Timeline` table with one row per tracked item:

| Release | JIRA | Type | Title |
|---|---|---|---|
| 3.5.0 | [SPARK-12345](https://issues.apache.org/jira/browse/SPARK-12345) | New Feature | Verbatim title from the release notes |

- **Release** — the Spark version the item first shipped in.
- **JIRA** — a link to the original `SPARK-*` ticket.
- **Type** — `New Feature` / `Improvement` / `Story` / `Epic` / `Umbrella` for items parsed from
  a JIRA dump, or `prose` for a feature extracted from a release's "Highlights" narrative (bugs,
  tests, sub-tasks, and dependency-upgrade items are excluded; see the ledger below).
- **Title** — for dump items, the verbatim item title as published; for `prose` items, a concise
  label backed by a verbatim `source_quote` in the prose ledger.
- **JIRA = "—"** — a prose feature whose highlight text cited no `SPARK-*` ID (common in the 0.x
  narrative era); the entry is still traceable via its `source_quote`.

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

The catalog is backed by three machine-readable ledger files plus one merged view, all in this
directory:

- **`_catalog.jsonl`** — kept items parsed from the 17 structured JIRA dumps (`New Feature` /
  `Improvement` / `Story` / `Epic` / `Umbrella`). 4,636 records.
- **`_dropped.jsonl`** — items excluded from the dumps, each tagged with a reason (bug fix, test,
  sub-task, task, dependency upgrade, documentation, question, wish). 9,354 records.
- **`_prose.jsonl`** — features extracted from the curated "Highlights" prose of every release,
  each with a verbatim `source_quote` copied from the source. 2,554 records.
- **`_all.jsonl`** — the merged, complete feature list (`_catalog.jsonl` + `_prose.jsonl`,
  7,190 records), one row per tracked feature across every release, each tagged `source`
  (`dump` or `prose`). Use this file when you want a single stream spanning every release,
  including the prose-only feature releases (4.0.0, 4.1.0, 4.2.0) that never appear in
  `_catalog.jsonl`.

Two guarantees back the "nothing lost" claim:

- **Per-dump invariant.** For every dump release, `kept + dropped == total <li> count for that
  release` — checked by `release-notes/catalog/build.py`, holds across all 17 dump releases with
  zero unbalanced.
- **Verbatim prose traceability.** Every `_prose.jsonl` record's `source_quote` is an exact
  substring of its release block in the source — verified mechanically at extraction, 0
  untraceable across all 2,554 records. Prose features are deduplicated against `_catalog.jsonl`
  by `SPARK-*` ID (0 overlaps) so nothing is double-counted.

> **Reconciliation.** Of the 16,563 unique `SPARK-*` IDs in the source, 15,238 (92%) are
> explicitly ledgered in `_catalog.jsonl`, `_dropped.jsonl`, or `_prose.jsonl`. The remaining
> ~1,325 are listed in `_unaccounted_ids.txt` for audit; they are IDs that never appear as their
> own tracked item — bug fixes, dependency bumps, migration-doc notes, and deprecations named in
> prose patch-release "notable changes" lists (categories excluded by design), plus IDs that
> occur only as a cross-reference inside another ticket's text. No feature or capability is among
> them; every marquee capability across all eras is captured in the timelines above.
