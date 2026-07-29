# Spark 4.2.0 — release facts and flipped defaults

Lookup cache for the 4.2.0 release. Check date before trusting.

Last verified: **2026-07-29** against the local Spark checkout at `C:\opt\learn\spark\repos\spark` (`v4.2.0-rc6`, `32f72996011`) and the official [4.2.0 release notes](https://spark.apache.org/releases/spark-release-4-2-0.html).

Sources: official release notes · [Databricks — Introducing Apache Spark 4.2](https://www.databricks.com/blog/introducing-apache-spark-42) · [Medium — What developers need to know about Apache Spark 4.2 (cralle)](https://medium.com/@cralle/what-developers-need-to-know-about-apache-spark-4-2-bcc70f2c7c7d) · local source.

---

## Defaults that flipped 4.1.0 → 4.2.0

All three confirmed by diffing `SQLConf.scala` between `v4.1.0` and `v4.2.0-rc6`. These change behaviour on upgrade with no config edit.

| Config | 4.1.0 | 4.2.0 | Consequence |
|---|---|---|---|
| `spark.sql.execution.pythonUDF.arrow.enabled` | `false` | **`true`** | A plain `@F.udf` is Arrow-serialized, not pickled row by row |
| `spark.sql.execution.arrow.enabled` | `false` | **`true`** | Fallback source for `spark.sql.execution.arrow.pyspark.enabled` — so `toPandas()` and `createDataFrame(pdf)` take the Arrow path by default |
| `spark.sql.geospatial.enabled` | `Utils.isTesting` (⇒ `false` in a real session) | **`true`**, and now `.internal()` | `GEOMETRY`/`GEOGRAPHY` types and `ST_*` functions resolve without opting in |

**The `arrow.enabled` flip is the easy one to miss.** `spark.sql.execution.arrow.pyspark.enabled` has no default of its own — it is `.fallbackConf(ARROW_EXECUTION_ENABLED)`, i.e. it reads the deprecated `spark.sql.execution.arrow.enabled`. That deprecated key is what changed. Any note claiming `toPandas()` "uses Arrow by default" for 3.3–4.1 is wrong; 4.2.0 is the release where it became true.

Anchors:

- `SQLConf.scala:4774` — `PYTHON_UDF_ARROW_ENABLED`, `createWithDefault(true)`
- `SQLConf.scala:4399` — `ARROW_EXECUTION_ENABLED`, `createWithDefault(true)`
- `SQLConf.scala:4405` — `ARROW_PYSPARK_EXECUTION_ENABLED`, `fallbackConf(ARROW_EXECUTION_ENABLED)`
- `SQLConf.scala:666` — `GEOSPATIAL_ENABLED`, `internal()`, `createWithDefault(true)`

---

## Highlights

**Change Data Capture** ([SPARK-55668](https://issues.apache.org/jira/browse/SPARK-55668)) — a `CHANGES` SQL clause reading row-level change feeds, with DataFrame, PySpark and Connect APIs, plus a DSv2 connector interface so any source can expose a change stream. Auto CDC in Declarative Pipelines ([SPARK-56249](https://issues.apache.org/jira/browse/SPARK-56249)) adds declarative SCD Type 1 upserts.

**Geospatial** ([SPARK-51658](https://issues.apache.org/jira/browse/SPARK-51658)) — native `GEOMETRY` and `GEOGRAPHY` types, `ST_*` functions, WKB/WKT and Parquet I/O, SRID registry from PROJ 9.7.1. Enabled by default ([SPARK-56771](https://issues.apache.org/jira/browse/SPARK-56771)).

> ❓ The release notes say "`ST_*` functions"; the expressions sweep found only five, all format conversion — no `ST_Contains`, no `ST_Distance`, no spatial join. See [sql/catalyst — expressions](../reference/spark-source-map/sweeps/sql-catalyst-expressions.md#geospatial-st-expressions-the-geographygeometry-beachhead).

**Metric Views** ([SPARK-54119](https://issues.apache.org/jira/browse/SPARK-54119)) — `CREATE VIEW … WITH METRICS`, a declarative semantic layer with YAML serde. A *semantic* layer, not a governance layer.

**Python UDF performance** ([SPARK-54555](https://issues.apache.org/jira/browse/SPARK-54555)) — the Arrow defaults above, plus skipping `ColumnarToRow` for Arrow-backed UDF input, an iterator API for Arrow and pandas grouped-aggregation UDFs, and `ExtensionDType` integer support in pandas UDFs.

**Vector and sketch functions** — similarity/distance ([SPARK-54713](https://issues.apache.org/jira/browse/SPARK-54713)), norm/normalize ([SPARK-55030](https://issues.apache.org/jira/browse/SPARK-55030)), vector `avg`/`sum` aggregates ([SPARK-55031](https://issues.apache.org/jira/browse/SPARK-55031)), Apache Tuple Sketches ([SPARK-54179](https://issues.apache.org/jira/browse/SPARK-54179)).

**`NEAREST BY`** ([SPARK-56395](https://issues.apache.org/jira/browse/SPARK-56395)) — a top-K nearest-neighbour join primitive, in Catalyst (`NearestByDistance` / `NearestBySimilarity`) and the DataFrame API.

**Path-based name resolution** ([SPARK-54806](https://issues.apache.org/jira/browse/SPARK-54806)) — `SET PATH`, `CURRENT_PATH()`, qualified names for built-in and session functions and views, and the path persisted into views and SQL functions.

**`QUALIFY`** ([SPARK-31561](https://issues.apache.org/jira/browse/SPARK-31561)) — filter on a window function's result without a subquery.

**Streaming** — Real-Time Mode trigger in PySpark ([SPARK-54660](https://issues.apache.org/jira/browse/SPARK-54660)); stream-stream non-outer join in Update mode ([SPARK-56384](https://issues.apache.org/jira/browse/SPARK-56384)); stable streaming source/sink identifiers; state-store snapshot repair and row checksums.

**DSv2** ([SPARK-55855](https://issues.apache.org/jira/browse/SPARK-55855)) — transactions for atomic multi-operation commits, schema evolution on `INSERT` ([SPARK-55689](https://issues.apache.org/jira/browse/SPARK-55689)), `PartitionPredicate` stats filtering, richer write metrics.

**Web UI** ([SPARK-55760](https://issues.apache.org/jira/browse/SPARK-55760)) — dark mode, pan/zoom/search on SQL plans, side-by-side initial-vs-final AQE plan comparison, job timeline on the SQL execution page.

Also: Java 25 ([SPARK-51167](https://issues.apache.org/jira/browse/SPARK-51167)); vectorized Parquet reader work ([SPARK-55722](https://issues.apache.org/jira/browse/SPARK-55722)); `TABLESAMPLE SYSTEM` with DSv2 pushdown ([SPARK-55978](https://issues.apache.org/jira/browse/SPARK-55978)); collation for char/varchar and CTAS/RTAS ([SPARK-54870](https://issues.apache.org/jira/browse/SPARK-54870)).

---

## Removals and dependency changes

| Change | Detail |
|---|---|
| R 3.x support dropped | [SPARK-57767](https://issues.apache.org/jira/browse/SPARK-57767) |
| `gcs-connector` removed | was `hadoop3-2.2.28`; supply it yourself |
| `jetty-util`, `jetty-util-ajax` removed | were 11.0.26 |
| Netty QUIC / HTTP-3 / io_uring / marshalling codec removed | |
| Hadoop | 3.4.2 → 3.5.0 |
| Arrow | 18.3.0 → 19.0.0 |
| Parquet | 1.16.0 → 1.17.0 |
| ORC | 2.2.1 → 2.3.0 |
| Jakarta servlet API | 5.0.0 → 6.0.0 |
| Kubernetes client | 7.4.0 → 7.6.1 |

---

## On the Medium article

Accurate but partial — it names Auto CDC, the `CHANGES` clause, the DSv2 CDC API, Metric Views, vector functions, geospatial types, Real-Time Mode, faster Python UDFs and Arrow-first Python, with no code and no configs. It omits `NEAREST BY`, `QUALIFY`, `SET PATH`, DSv2 transactions, Java 25, the Web UI work, and every breaking change. Its one substantive error: calling Metric Views a "governance layer". Use the official release notes as the reference; the article adds nothing this cache does not already carry.
