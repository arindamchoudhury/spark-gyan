# Learning Path: Apache Spark / PySpark

> **Last updated:** 2026-08-09 (**re-sweep of `sql/core — streaming-exec` at an unchanged 4.2.0** — the map's largest single group, 149 files, taken from `partial` to `complete`: **33 → 42 concepts**. **The config slice found the work.** Resolving each of the 113 keys to the file that actually reads it showed the previous table was wrong three times, and each error pointed straight at an untraced layer — `…rocksdb.*` was recorded as 30 catalog keys when the catalog holds **two**, `asyncProgressTracking*` was recorded as three configs when it is writer options only, and `transformWithStateOp.stateSchemaVersion` is not a key at all. Now traced: the continuous-processing epoch protocol and its executor tasks (11 files that had one survey concept), the DSv2 streaming write path and how a source implements `MicroBatchStream` (18 files, same), the RocksDB tuning surface, range-scan and timestamp key encoding, Avro state encoding, and the TTL index machinery. **Three new topics**: **A45** writing a streaming sink, **E47** Avro state encoding and state schema evolution, **E48** continuous processing and the epoch coordinator. The findings worth carrying. **Almost the entire RocksDB tuning surface is undeclared** — ~29 keys that exist only as strings in `RocksDBConf`, looked up with a silent fallback to their default, so a misspelling is undetectable and none of them appear in `SET -v`. **Only `transformWithState` can evolve a state schema** — the gate is `usingAvro && schemaEvolutionEnabledForOperator`, and the second flag is `true` in exactly one file; every other stateful operator still faces a checkpoint rebuild. And turning Avro encoding on forces every state field nullable. **Every `*.stateFormatVersion` is read in `SparkStrategies`**, not in the operator — a planning-time decision pinned into the checkpoint by `OffsetSeq`, which is why changing one on a running query does nothing. **A TTL-enabled `ListState` occupies four column families**, because RocksDB's `merge` makes element-level deletion impossible. Also corrected A8's citation of the correctness-check config, which had a Scala accessor name in place of the key.)
>
> **Previously:** 2026-08-09 (**re-sweep of `sql/core — datasources` at an unchanged 4.2.0** — the third and last `status: partial` page closed out, and the map's largest group finished: **44 → 59 concepts**, citation 233 → **303 of 303 files**. **Package breadth found all of the work**: the config slice had been clean since the first pass while the group's **entire Java tree was uncited** — 29 files, ~7,700 lines holding the whole vectorized decode stack for Parquet and ORC. Now swept: schema clipping and the requested-schema synthesis, both Parquet converter trees, the write support and the metadata it stamps into every file, page encodings and the dictionary fallback, definition/repetition-level assembly, the footer read, ORC's zero-copy vectors, Avro and XML record conversion, the JDBC write loop, and the five V2 file-source triples. **Three new topics**: **I36** JDBC as a source and a sink, **A44** type conversion at the file boundary, **E46** Parquet page decoding. The findings worth carrying. **Spark still writes `INT96` timestamps by default** — `spark.sql.parquet.outputTimestampType` defaults to a type the Parquet spec deprecated, with its own separate rebase mode. **A JDBC write is one transaction per partition**, despite `saveTable`'s scaladoc saying "a single transaction" and despite the docs never stating the scope at all — a half-failed write leaves the committed partitions behind, and `numPartitions` can only ever *coalesce*. **The two Parquet readers accept different conversions**: `INT32 → DOUBLE` works in the row-based converter and throws `PARQUET_COLUMN_DATA_TYPE_MISMATCH` in the vectorized one, so an unrelated nested column in the projection can decide whether the query runs. **One dictionary per batch** — a writer that fills its dictionary mid-chunk forces eager decode of the whole batch, and rebased or upcast columns are barred from lazy dictionary decoding entirely. **A column `DEFAULT` is applied by the reader, per file**, and only by the vectorized ones. Geospatial was found again on the I/O side and, consistent with the expressions sweep, still not proposed as a topic.)
>
> **Current Spark stable:** 4.2.0 (Jul 14 2026) · **Maintenance lines:** 4.1.3, 4.0.4 (Jul 15 2026), 3.5.9 (Jul 16 2026)
>
> Spark 4.2.0 is the third 4.x release — 1,700+ Jira tickets. Learn against 4.2.0; the books below are written against 3.x, so the callouts on each topic mark where they diverge.
>
!!! note "Status key"
    **Topics:** ⬜ not started · ✅ done and current · 🔄 done, but written against an older Spark and now needs revisiting (the topic's callout says what drifted).

    **Checkpoints:** 🎯 — a gate, not a topic. It carries no completion status: it is a self-test you attempt to decide whether you are ready to leave a level.

    **How to read this page.** Topics are grouped by level — Beginner → Intermediate → Advanced → Expert. Each topic lists what it is, why it matters, and exactly which resources to use and in what order. Pick the level where you currently are and work through the topics in sequence within that level.

**What this path is built around.** Apache Spark itself — the open-source engine, its APIs, and the open formats and tooling around it. Vendor platforms (Databricks, and the certifications built on it) appear as *optional milestones* at the end, not as the spine. Rationale: the transferable skill is the engine and the open ecosystem; platform-specific surfaces change with your employer, and a path organised around one vendor's exam quietly under-weights what the wider market asks for. If you decide to sit those exams, the [optional certification milestones](#optional-certification-milestones) section maps them back onto these topics.

**How to actually use each topic.** Read the milestone *first*, and attempt it from memory before opening any resource. You will mostly fail early on — that is the point; the failed attempt is what makes the subsequent reading stick, and it tells you which parts you can skip. Then read, then attempt the milestone again in writing. Self-explanation and retrieval practice both carry roughly twice the effect size of rereading, and the book chapters in `docs/spark-book/` are where the self-explanation happens.

!!! note "Topic codes track reading order within a level"

    Codes run in ascending order inside each level, so the numbers and the reading order agree.
    They are referenced by the book index, chapter files and the coverage matrix, so renumbering
    is a cross-file change and is avoided — but it is not forbidden. The I-block was renumbered
    on 2026-07-19 so that Iceberg joined the storage run as I11 instead of trailing as I15.
    Codes are stable between such changes, not permanently frozen; check the refresh logs if an
    external note references an old code.

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
| Open table formats (Iceberg increasingly the default; Delta where Databricks is in play) | I8 fundamentals, I11 depth and interop |
| Kafka as the standard event backbone | A12, and as a source throughout A7/A8 |
| Semi-structured data at scale (`VARIANT`, new in Spark 4.0) | I1 |
| Kubernetes as the deployment target | E2 |
| Spark Connect as the default client architecture in 4.x | B2 basics, E9 depth |
| Declarative pipelines replacing hand-rolled orchestration glue | A11 |
| SQL fluency weighted at least as heavily as Python | B8, I12 |

---

## Beginner

**Goal:** Understand what Spark is and why it exists. Write correct PySpark programs that read, transform, and write data. Use the DataFrame API fluently.

**Estimated time to complete this level:** 30–40 hrs

---

### 🔄 B1 — Spark Architecture & the Execution Model

**What it is:** The mental model of how Spark distributes work — driver, executors, cluster manager, JVM vs Python process, lazy evaluation, DAG, stages, tasks. Both ends matter as much as the middle: what `spark-submit` launches *before* your `main()` runs (in cluster mode, not your class), and how the DataFrame layer above the DAG decides how many jobs an action becomes.

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
9. **Spark-docs → Monitoring and Instrumentation** ([monitoring.html](https://spark.apache.org/docs/latest/monitoring.html)) — where the numbers in the UI come from, the event log the History Server replays, and the listener bus underneath both. Read it knowing what the source trace shows: that bus is **bounded and drops events on overflow**, so the page describes a best-effort record, not a ledger
10. **Source trace — [B1 in the source map](reference/spark-source-map/topics/b1.md)** — the full path, and now both ends of it: what `spark-submit` launches *before* your `main()` (in cluster mode, not your class), how a DataFrame action becomes some number of jobs, then `DAGScheduler` → `TaskRunner`, what a task actually produces (shuffle write, the three writers, `MapOutputTracker`), the failure, liveness and retry paths, how results return to the driver, and Connect as the alternative front end. Read it *after* the books: it turns "the DAG scheduler splits stages at shuffle boundaries" from a claim you accept into one you can go and look at
11. **Source sweep — [core — rdd-layer in the source map](reference/spark-source-map/sweeps/core-rdd-layer.md)** — the RDD contract itself (`compute`, `getPartitions`, `getDependencies`) and the `iterator` → `getOrCompute` path every task runs, which is where the architecture stops being a diagram and becomes code
12. **Source sweep — [core — execution engine in the source map](reference/spark-source-map/sweeps/core-execution-engine.md)** — the whole execution model as code — action to job to stages to tasks, the single-threaded event loop that drives it, and where the driver stops deciding and the executor starts running
13. **Source sweep — [core — shuffle & memory in the source map](reference/spark-source-map/sweeps/core-shuffle-memory.md)** — what a shuffle physically is: two files per map task, an index of offsets, and the executor-wide monitor that commits them
14. **Source sweep — [core — submit & standalone in the source map](reference/spark-source-map/sweeps/core-submit-standalone.md)** — what `spark-submit` actually does before any of your code runs — master-URL resolution, the wrapper main class each cluster mode substitutes, and the classloader `runMain` builds
15. **Source sweep — [sql/catalyst — analysis in the source map](reference/spark-source-map/sweeps/sql-catalyst-analysis.md)** — skim for the execution-model picture only: *before* any DAG, stage or task exists, your DataFrame/SQL is compiled — parsed, then **analyzed** (names bound, types resolved) — and this is why nothing runs until an action fires. The compilation detail belongs to A1; read it here just to place the analyze phase ahead of the runtime this topic covers
16. **Source sweep — [core — storage & serialization in the source map](reference/spark-source-map/sweeps/core-storage-serializer.md)** — where a block physically lives once a task produces it: the driver's `BlockId → BlockManagerId` registry, the `BlockId` taxonomy whose names you will read in the UI and the logs, and the executor-side `BlockManager` that answers both local and remote reads
17. **Source sweep — [core — rpc & resources in the source map](reference/spark-source-map/sweeps/core-rpc-resources.md)** — the messaging substrate under every driver-executor exchange, and the detail that changes how you read a hang: each executor runs **two** Netty servers, one for RPC control messages and a separate one on `spark.blockManager.port` carrying block bytes

**Milestone:** You can explain (without notes) what happens between `spark.read.parquet(...)` and `.show()` — where the plan lives, when it executes, and which process runs the Python code. Stronger version, once you have read the source trace: name the single function that decides where one stage ends and the next begins; explain why a failing task retries four times on a cluster but aborts the stage immediately on your laptop; explain why a stage you already watched succeed can run again; predict how many **jobs** a `df.orderBy(...).show(10)` submits and why the answer is neither "one" nor fixed; and say what actually declares an executor dead.

!!! warning "Marked 🔄 — a wrong Java-version claim in Ch03, and a wrong stage count in Ch02"
    Chapter 03 (Spark Installation, written under this topic) states that Spark 4.x supports only Java 17 and 21. Spark 4.2.0 builds and runs on **Java 25** ([SPARK-51167]). Two further gaps the 4.2.0 source re-trace opened, both in Ch03: Spark 4.x is **Scala 2.13 only** (Scala 2.12 support was dropped across the whole 4.x line), which decides the `_2.13` suffix on every dependency artifact and silently breaks build files copied from Spark 3.x material; and the chapter's header still pins `Spark 4.1.x`.

    The 2026-08-10 completeness pass found a factual error in Ch02 as well. The chapter gives its word-count example **two different stage counts**, and the three-stage version — in which `.orderBy(...)` adds a `rangepartitioning` shuffle and a separate `ResultStage` — describes a plan Spark does not produce. `SpecialLimits` rewrites `Limit(n, Sort(…))` into `TakeOrderedAndProjectExec` below `spark.sql.execution.topKSortFallbackThreshold`, so a sort under a `show(n)` performs **no global-sort shuffle at all**. Ch02's architecture, scheduler and Py4J material re-verified clean; it is this walkthrough, its Mermaid diagram, and the "one action = one job" invariant that need correcting.

!!! info "Facts from the source that the books state loosely"
    All surfaced by the 4.2.0 trace and worth carrying into your own notes:

    - **One function enforces the stage split.** `DAGScheduler.getMissingParentStages` is the only place the narrow-vs-wide distinction is mechanically applied: a `ShuffleDependency` starts a new `ShuffleMapStage`, a `NarrowDependency` stays in the current one. Every prose explanation of stage boundaries is a description of this one function.
    - **`spark.task.maxFailures` does not apply in local mode.** `SparkContext` passes a hardcoded `MAX_LOCAL_TASK_FAILURES = 1` when building the local scheduler, so the documented default of 4 is ignored on your laptop and a single task failure aborts the stage. This is a common source of "it retried on the cluster but died locally" confusion.
    - **A shuffle-map task returns file locations, not data.** `ShuffleMapTask.runTask` returns a `MapStatus`; only a `ResultTask` returns values. That return type *is* the stage boundary, which makes the whole stage model concrete rather than diagrammatic.
    - **Three shuffle writers exist and you do not choose directly.** `SortShuffleManager.getWriter` picks between `BypassMergeSortShuffleWriter`, `UnsafeShuffleWriter` and `SortShuffleWriter` based on partition count, map-side combine, and serializer. Crossing `spark.shuffle.sort.bypassMergeThreshold` silently changes which one runs — a real performance cliff that no book names.
    - **"One action, one job" is a teaching simplification, and `show()` is where it breaks.** `SparkPlan.executeTake` is a loop: it scans `spark.sql.limit.initialNumPartitions` (**1**) partitions, and if it has not yet found enough rows, multiplies by `spark.sql.limit.scaleUpFactor` (**4**) and submits *another* job. A selective query under a `show()` routinely runs four or five jobs. `SQLExecution.withNewExecutionId` groups them under one SQL query, which is why the SQL tab and the Jobs tab never agree on counts.
    - **A sort under a limit is not a shuffle.** `SpecialLimits.planTakeOrdered` rewrites `Limit(n, Sort(…))` into `TakeOrderedAndProjectExec` whenever `n` is below `spark.sql.execution.topKSortFallbackThreshold` (default `Integer.MAX_VALUE - 15`). Each partition emits its local top-*n* and the driver merges. Reading a stage count off the user's transformation chain gets this case wrong every time.
    - **`spark.default.parallelism` resolves differently on your laptop and on a cluster.** `LocalSchedulerBackend` returns `totalCores`; `CoarseGrainedSchedulerBackend` returns `max(cores registered so far, 2)` — a value that is racy during startup, because `isReady` gives up waiting for executors after `spark.scheduler.maxRegisteredResourcesWaitingTime` (30s) and schedules anyway. This is the usual reason a first stage runs at a fraction of the expected parallelism.
    - **Two independent timers decide an executor is dead.** The executor kills *itself* after `spark.executor.heartbeat.maxFailures` (**60**) failed heartbeats; separately, the driver's `HeartbeatReceiver.expireDeadHosts` removes anything silent for `spark.network.timeout` (120s). A long GC pause is indistinguishable from a dead executor at this layer — which is how a GC problem presents as a mystery executor loss.
    - **The Spark UI's numbers arrive over a lossy queue.** Every metric reaches the UI, the event log and the History Server through `LiveListenerBus`, whose four queues are each bounded at `spark.scheduler.listenerbus.eventqueue.capacity` (**10 000**). On overflow, events are dropped and a rate-limited warning is logged — nothing fails, and the UI is quietly wrong. Jobs with very many small tasks are the ones that hit it.

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
8. **Source sweep — [core — submit & standalone in the source map](reference/spark-source-map/sweeps/core-submit-standalone.md)** — how a config gets its value *before* a session exists: the four-stage precedence pipeline and the option table that `--conf` cannot override
9. **Source sweep — [sql/connect — client-server in the source map](reference/spark-source-map/sweeps/sql-connect-client-server.md)** — there are **two** complete `SparkSession` implementations: the classic one and `sql/connect/common`'s, which builds protobuf instead of plans. Both sit behind the `sql/api` interfaces, and `ConnectClientUnsupportedErrors` is the enumerated list of where they diverge — read it rather than guessing whether an API works on Connect
10. **Source sweep — [sql/core — the classic API in the source map](reference/spark-source-map/sweeps/sql-core-classic-api.md)** — the session objects at source level: the private constructor whose two optional parameters *are* the difference between `newSession` (share `SharedState`, not `SessionState`) and `cloneSession` (share both, then force a copy of `SessionState` **and** `ArtifactManager`); `Builder.build`'s three-step resolution; and the coupling nothing documents — `enableHiveSupport` also sets `spark.sql.artifact.isolation.enabled=false`, with the reason given in a source comment. Also two different outcomes for the same mistake: a static config is *rejected* by `spark.conf.set` but only *warned about* by `.config()` on an existing session

!!! info "`SharedState` vs `SessionState` — learn this and the session API stops needing memorisation"
    A `SparkSession` owns two state objects, and every confusing session behaviour follows from which one holds what.

    **`SharedState` — one per `SparkContext`, shared by every session on the JVM:** the cache manager, the external catalog (metastore), and the global temp database. **`SessionState` — one per session:** SQL conf, temp views, UDF registry, analyzer and optimizer.

    Three consequences worth predicting rather than discovering: `df.cache()` in one session is visible from another, because the cache manager is shared; `createGlobalTempView` outlives the session that made it, because it lives in `SharedState`; and `spark.stop()` tears down the `SparkContext`, invalidating **every** session on the JVM, not just yours.

!!! warning "`spark.sql.extensions` is static — set it at build time or not at all"
    Iceberg, Delta and Sedona all attach themselves through `SparkSessionExtensions`, driven by the `spark.sql.extensions` config. It is a **static** config: read once while the session is being constructed, so setting it afterwards with `spark.conf.set(...)` silently does nothing. This is the usual first failure when adding a table format, and the symptom — "my SQL syntax isn't recognised" — points nowhere near the cause.



!!! warning "Config precedence before the session exists"

    This topic covers configuration once a `SparkSession` is running. Submission-time resolution is
    a separate, earlier pipeline with its own order: `--conf` beats `--properties-file`, which beats
    `--extra-properties-file`, which beats `conf/spark-defaults.conf` — and `spark-defaults.conf` is
    **skipped entirely** once `--properties-file` is given, unless `--load-spark-defaults` is passed.

    The counter-intuitive part: `--conf` values are applied **last, via `setIfMissing`**, so any key
    `spark-submit`'s internal option table already wrote — `spark.jars`, `spark.files`, `spark.master`,
    `spark.app.name` among them — is immune to `--conf`. And a key that does not start with `spark.`
    is dropped with only a warning, so a typo'd namespace vanishes rather than failing.
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
9. **Source sweep — [sql/core — Python and Arrow in the source map](reference/spark-source-map/sweeps/sql-core-python-arrow.md)** — what `toPandas()` and `spark.createDataFrame(pandas_df)` actually run: `ArrowConverters` cutting batches on a record limit or an estimated byte limit, and — on the way in — a threshold decision most people never see. Above `spark.sql.execution.arrow.localRelationThreshold` (48 MB) the driver-side Arrow batches are parallelized into a real RDD; below it they are decoded on the driver into a `LocalRelation`, with every row copied through an `UnsafeProjection` because the Arrow vectors are released immediately after
10. **Source sweep — [sql/core — the classic API in the source map](reference/spark-source-map/sweeps/sql-core-classic-api.md)** — what a `Dataset` actually is — a `QueryExecution` plus an encoder plus an id, holding no rows — and the line that surprises everyone: its `logicalPlan` is `queryExecution.commandExecuted`, so a `Dataset` over a command has **already run it** at construction. That is why `spark.sql("INSERT INTO …")` performs the insert without an action. Also `withAction`, which opens an execution id and resets plan metrics per action, so SQL-tab metrics are per-action rather than cumulative

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

8. **Source sweep — [core — rdd-layer in the source map](reference/spark-source-map/sweeps/core-rdd-layer.md)** — the RDD-level twin of the file scan: `HadoopRDD` turning one `InputSplit` into one partition, `spark.hadoopRDD.ignoreEmptySplits` (off by default), and `InputFileBlockHolder`, which is where `input_file_name()` gets its value. Note the config family differs from `spark.sql.files.*`
9. **Source sweep — [core — execution engine in the source map](reference/spark-source-map/sweeps/core-execution-engine.md)** — what happens *after* your rows are written: the `FileCommitProtocol` staging model, why a write lands in a temp directory first, and the fact that `commitJob` promotes files one rename at a time with no rollback — so a driver killed mid-commit leaves a partly-written destination
10. **Source sweep — [sql/catalyst — analysis in the source map](reference/spark-source-map/sweeps/sql-catalyst-analysis.md)** — two analysis-time write behaviours: `CHECK` constraints become a `CheckInvariant` expression inserted above the write (so enforcement costs per row and appears in `EXPLAIN`), and `ResolveSchemaEvolution` reconciles an incoming schema against the table's
11. **Source sweep — [sql/catalyst — planner in the source map](reference/spark-source-map/sweeps/sql-catalyst-planner.md)** — the catalyst half of DataSource V2: `DataSourceV2Relation` before a scan is built, `DataSourceV2ScanRelation` after, and the capability model — `supports` is a set-membership test against what the connector *declares*, so an undeclared capability is a clean analysis error and a falsely declared one fails much later
12. **Source sweep — [sql/catalyst — types & parser in the source map](reference/spark-source-map/sweeps/sql-catalyst-types-parser.md)** — the parser layer under `spark.read`: `CSVOptions` / `JSONOptions` / `XmlOptions` are the real option reference, `enforceSchema=true` (the CSV default) means the header is **skipped and matched by position** rather than validated, and every format funnels malformed records through one 70-line `FailureSafeParser` whose PERMISSIVE default emits a row of nulls with no signal
13. **Source sweep — [sql/hive — hive-metastore in the source map](reference/spark-source-map/sweeps/sql-hive-hive-metastore.md)** — what changes when the table came from a Hive metastore rather than a path: a Spark table's authoritative schema is **JSON in table properties** (split at 4000 chars), so Hive DDL against it changes what Hive sees and not what Spark reads; whether the table is Hive-readable at all is decided at create time by a five-branch method that logs and never fails; and writes go to a `.hive-staging` directory before a Hive `loadTable`/`loadPartition`, so visibility semantics are Hive's
14. **Source sweep — [sql/core — datasources in the source map](reference/spark-source-map/sweeps/sql-core-datasources.md)** — the whole path as source, and the three defaults worth changing your mental model over. `spark.sql.sources.useV1SourceList` defaults to `avro,csv,json,kafka,orc,parquet,text`, so **every built-in format takes the V1 path** and the DSv2 file implementations that exist alongside them are never used. Read parallelism is `min(maxPartitionBytes, max(openCostInBytes, totalBytes / defaultParallelism))` packed Next-Fit-Decreasing, with `openCostInBytes` charged per file — that surcharge, not `maxPartitionBytes`, is the knob for small files. And `spark.sql.files.ignoreCorruptFiles` does not skip a bad file: it marks the partition **finished** at the point of failure, so the job succeeds with silently truncated data
15. **Source sweep — [sql/core — streaming execution in the source map](reference/spark-source-map/sweeps/sql-core-streaming-exec.md)** — the two file-based streaming endpoints and their non-obvious semantics: `FileStreamSource`'s `SeenFilesMap` means a file older than `maxFileAge` relative to the newest seen file is **never** picked up, even if it appears later; and `FileStreamSink` writes a `_spark_metadata` manifest that Spark uses to distinguish committed files from strays — so any other engine reading the same directory sees the strays
16. **Source sweep — [sql/core — the classic API in the source map](reference/spark-source-map/sweeps/sql-core-classic-api.md)** — the reader and writer as plan builders: `DataFrameReader.load` makes **one** `UnresolvedDataSource` node and defers the V1-vs-V2 decision, the format and schema inference entirely to analysis; `DataFrameWriter.saveCommand` is the branchiest method in the package and is where 'which write path did I take' is decided; and `insertInto` is a different operation from `save` — it matches columns by **position** and rejects `partitionBy`
17. **Source sweep — [connector/kafka-0-10-sql — source & sink in the source map](reference/spark-source-map/sweeps/connector-kafka-0-10-sql-source-sink.md)** — a batch read that goes through none of the machinery above. `spark.read.format("kafka")` resolves to `KafkaBatch`/`KafkaRelation`, not a `FileFormat`, so **no `spark.sql.files.*` config applies**: parallelism comes from `minPartitions`/`maxRecordsPerPartition` (topic **A41**) and offsets are bound *on the executor* at read time rather than during planning, because data can age out between the two. Batch mode also forbids what would make it unbounded — `latest` as a start, `earliest` as an end — and warns-and-ignores the three streaming trigger options. On the write side there is no commit protocol at all: `commit` and `abort` are empty, so a failed Kafka write leaves whatever it already sent
18. **Source sweep — [streaming — DStream in the source map](reference/spark-source-map/sweeps/streaming-dstream.md)** — the other file reader in the tree, and a different acceptance model from everything above. `FileInputDStream` does not list-and-plan: it polls each monitored directory **every batch**, warns when the listing takes longer than the batch interval, and selects a file by comparing its **modification time** against a rolling ignore threshold (`max(startTime, now − rememberDuration)`). Four criteria must all hold, including that a file must not be *newer* than the batch it is being tested for — the rule that makes recovery replay deterministic. The practical consequence is the one worth carrying to any file-based ingest: a file whose mtime is older than the window is never read and never reported, so copying rather than atomically moving a file into a watched directory silently loses it

**Milestone:** You can read multi-file datasets with glob patterns, declare a schema programmatically with `StructType`, write in append/overwrite mode, and explain why Parquet is preferred for analytical workloads. Then two the source makes checkable: predict how many tasks a read of N files will produce and say which config capped it, and explain what happens to already-written files when a write fails halfway.

!!! warning "`insertInto` matches columns by position, not by name"
    `df.write.insertInto(table)` ignores column names entirely and matches by ordinal, while `saveAsTable` resolves by name. A DataFrame with the *right* column names in the wrong order writes silently corrupted data. None of the three books above covers this distinction; it is the highest-consequence trap in the writer API.

!!! info "Writes are not atomic on object storage"
    Spark writes into a `_temporary` directory and *moves* files on job commit. On HDFS that rename is atomic and cheap; on S3 and other object stores it is a copy — slow, and not atomic, so a failed job can leave partial output. This is the gap that Delta and Iceberg exist to close, and it is worth understanding here rather than treating those formats as magic later (see I8, I11).

!!! note "New in Spark 4.2.0 — Python Data Sources (write your own reader/writer in pure Python)"
    Spark 4.x lets you implement a custom data source entirely in Python — no Scala, no JVM code — by subclassing `pyspark.sql.datasource.DataSource` and registering it with `spark.dataSource.register(...)`. It then plugs into the same `spark.read.format("mysource")` / `df.write.format("mysource")` surface as the built-ins, and 4.2.0 extends the API to cover **batch and streaming, read and write** ([SPARK-55304] adds admission control / `Trigger.AvailableNow` to the streaming reader) and adds **profiling** for these connectors ([SPARK-55161], see E3). Use it for an API-backed source, a bespoke file layout, or a test fixture — cases where before 4.x you either dropped to an RDD or wrote a JVM `DataSourceV2`. None of the books cover it (all predate 4.x); go to **Spark-docs → [Python Data Source API](https://spark.apache.org/docs/latest/api/python/tutorial/sql/python_data_source.html)** and verify on your own 4.2.0 stack. This is the pure-Python cousin of the DSv2 work in E8 — learn the built-in readers in this topic first.

!!! note "New in Spark 4.2.0 — `INSERT INTO … REPLACE` for conditional overwrite"
    Beyond the append/overwrite modes above, 4.2.0 adds SQL `INSERT INTO … REPLACE ON`/`USING` ([SPARK-56001]) and `BY NAME` support for `INSERT INTO … REPLACE WHERE` ([SPARK-54803]) — a targeted overwrite that atomically replaces only the rows matching a predicate/key, rather than clobbering a whole partition or table. It is the SQL-native cousin of a Delta/Iceberg conditional overwrite on a DSv2 table; reach for it instead of a read-filter-rewrite. One related write change worth knowing: per-write `.option(...)` values now take precedence over session config in file-source writes ([SPARK-56414]), so a behaviour you set per-write can no longer be silently overridden by a session default. Book-absent — 4.2.0 SQL reference.

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
7. **Source sweep — [sql/catalyst — analysis in the source map](reference/spark-source-map/sweeps/sql-catalyst-analysis.md)** — where `CHECK` constraints are turned into plan expressions, and the analysis-side handling of char/varchar padding and collation
8. **Source sweep — [sql/catalyst — types & parser in the source map](reference/spark-source-map/sweeps/sql-catalyst-types-parser.md)** — what a schema is underneath: `DataType` has three non-interchangeable text forms (`json` for the metastore, `catalogString` for `printSchema`, `sql` for DDL) and **four** notions of equality; `CHAR(n)` / `VARCHAR(n)` are erased to `StringType` plus a metadata key, so `printSchema` cannot show what you declared; and the `TIME` type defaults to `Utils.isTesting` — present in the source, off in any real session
9. **Source sweep — [sql/core — datasources in the source map](reference/spark-source-map/sweeps/sql-core-datasources.md)** — where a schema actually comes from when you do not supply one: with `mergeSchema` off (the default for both Parquet and ORC) Spark reads a summary file if present and otherwise *one arbitrary part-file*, on the stated assumption that all the others match. A column present only in newer files therefore may not exist in the DataFrame at all — no error, no warning. Also the four different file↔table column-matching rules (see E25), and `validatePartitionColumn` refusing a table partitioned by every column
10. **Source sweep — [sql/core — Python and Arrow in the source map](reference/spark-source-map/sweeps/sql-core-python-arrow.md)** — the type layer of the Python boundary: `EvaluatePython.needConversionInPython` decides per type whether a value can cross as-is, `toJava` / `makeFromJava` build the converters (one closure per type, resolved once rather than per row), and a `UserDefinedType` is unwrapped to its `sqlType` in both directions — which is exactly why a UDT works on the pickle path and is a special case on the Arrow one
11. **Source sweep — [sql/core — streaming execution in the source map](reference/spark-source-map/sweeps/sql-core-streaming-exec.md)** — schema evolution for streaming state: `StateSchemaCompatibilityChecker` compares an operator's current key/value schema against the one recorded in the checkpoint and either accepts it, accepts it as an evolution, or fails — and `OperatorStateMetadata` (V1/V2) is what lets the error name *which* operator changed rather than just failing to decode

**Milestone:** You can define a schema without `inferSchema`, validate that incoming data matches it, and explain the cost of `inferSchema` on large files. Then the part that changes how you write pipelines: declare a column `nullable=False`, read a file containing nulls in it, and predict what happens before you run it.

!!! warning "`nullable=False` is a hint, not a constraint — nothing enforces it on read"
    This is the most consequential thing about schemas in Spark, and it is the opposite of what the word suggests. No part of the file-read path validates nullability. Declaring a column non-nullable tells the **optimizer** it may skip null checks; if the data then contains nulls, you get nulls in a column the plan believes cannot hold them — which produces wrong results rather than an error.

    Spark does enforce in two narrower places, with different rules in each: `createDataFrame` on local Python data with `verifySchema=True` checks type *and* range per row on the driver, and writing into an existing table applies `spark.sql.storeAssignmentPolicy`. Reading a file is checked by neither. If you need a guarantee, assert it yourself after the read.

!!! info "Three different rules answer 'is this cast allowed'"
    `canCast` governs an explicit `.cast()` and is permissive. `canUpCast` governs *implicit* coercion during analysis and allows only safe widening. `canANSIStoreAssign` governs writing into an existing table and sits between the two.

    Practical consequence: an expression that resolves fine in a `select` can fail on `INSERT INTO` — not a bug, a different rule set. Also note `spark.sql.ansi.enabled` selects between two *complete* coercion rule sets (`TypeCoercion` vs `AnsiTypeCoercion`), so the same query can resolve to different result types on either side of that flag.

!!! note "New in Spark 4.2.0 — string collation is a type property, and it changes comparison/sort/grouping"
    Collation (introduced in Spark 4.0, matured here) attaches a comparison rule — case/accent sensitivity, locale ordering — to a `STRING`/`CHAR`/`VARCHAR` column, so `=`, `ORDER BY`, `GROUP BY` and `DISTINCT` obey that rule rather than raw byte order. 4.2.0 extends collation to `char`/`varchar` and to `CTAS`/`RTAS` ([SPARK-54870]), adds a `SHOW COLLATIONS` command to list what is available ([SPARK-49543]), and lets SQL UDFs declare a default collation ([SPARK-55528]). It belongs here rather than in the function catalogue because it is a *type-level* property: a mis-set collation silently changes join and dedup results, not just display order. Two 4.2.0 correctness fixes underline that — `NOT IN` on collated tables ([SPARK-54852]) and constant propagation under non-binary-stable collations ([SPARK-55647]) both returned wrong answers before. None of the books cover collation (all predate 4.0); go to the 4.2.0 docs.

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
7. **Source sweep — [sql/catalyst — optimizer in the source map](reference/spark-source-map/sweeps/sql-catalyst-optimizer.md)** — the rewrites that happen to your `groupBy` before it is planned: `ReplaceDistinctWithAggregate`, `RewriteDistinctAggregates` (the rule that multiplies every input row N times for N distinct aggregates, and the 4.2.0 `OptimizeExpand` that can undo it), `EliminateDistinct`, `DecimalAggregates`, and the `map_sort` insertion that makes a `MapType` grouping key compare correctly at all
8. **Source sweep — [sql/catalyst — expressions in the source map](reference/spark-source-map/sweeps/sql-catalyst-expressions.md)** — the expression side of aggregation: the four `AggregateMode`s (`Partial`, `PartialMerge`, `Final`, `Complete`) that make one function into both halves of a shuffle, and the three implementation tiers — declarative, imperative, typed-imperative — that decide whether you get `HashAggregateExec` or `ObjectHashAggregateExec`. One `collect_list` or `percentile` in the projection moves the whole aggregation to the slower operator
9. **Source sweep — [sql/core — aggregation, windows and exchange in the source map](reference/spark-source-map/sweeps/sql-core-agg-window-exchange.md)** — the physical layer under `groupBy`: `AggUtils.createAggregate`'s three-operator ladder is a *type check*, not a cost model; the partial aggregate exists only because it is built with `requiredChildDistributionExpressions = None`; `HashAggregateExec` changes algorithm mid-task when it spills (`numTasksFallBacked`, visible in the SQL tab); `ObjectHashAggregateExec` falls back after **128 groups** — a count of groups, not a byte budget; and a `FILTER (WHERE …)` clause is stripped from every non-partial mode
10. **Source sweep — [sql/core — Python and Arrow in the source map](reference/spark-source-map/sweeps/sql-core-python-arrow.md)** — why a Python UDF near a `groupBy` always adds a node: a `PythonUDF` cannot be evaluated inside an `Aggregate` at all, so `ExtractPythonUDFFromAggregate` splits the aggregate and puts a UDF over an aggregate result in a `Project` above it, while `ExtractGroupingPythonUDFFromAggregate` evaluates a UDF in the grouping key below it. Also `ArrowAggregatePythonExec`, the 4.2.0 grouped-aggregate operator, and the `AllTuples` degradation when there are no grouping expressions
11. **Source sweep — [sql/core — streaming execution in the source map](reference/spark-source-map/sweeps/sql-core-streaming-exec.md)** — how a streaming aggregate stores its state: a `StateStoreRestoreExec` / `SaveExec` pair around the ordinary aggregate operators, mediated by a `StreamingAggregationStateManager` with two formats — V1 stores the whole row, V2 only the non-key columns — pinned in the checkpoint. Session windows get their own manager and a merging-sort iterator, because a session's key range changes as sessions merge
12. **Source sweep — [sql/core — the classic API in the source map](reference/spark-source-map/sweeps/sql-core-classic-api.md)** — the grouping objects: `RelationalGroupedDataset` is a builder holding grouping expressions and a `GroupType` (`GroupBy`/`Rollup`/`Cube`/`Pivot`), not a plan node — each terminal method constructs the `Aggregate`. `pivot` without an explicit value list runs a hidden job to collect the distinct values first
13. **Source sweep — [sql/catalyst — analysis in the source map](reference/spark-source-map/sweeps/sql-catalyst-analysis.md)** — what happens to a `Pivot` node after the hidden value-discovery job: `PivotTransformer` rewrites it into an `Aggregate` (or a combination of `Aggregate`s and `Project`s depending on what sits below), checking every pivot value is a literal of the pivot column's type and **deducing the group-by expressions**, since in SQL they are implicit — everything in the select list that is neither aggregated nor pivoted. `UnpivotTransformer` is the inverse, producing an `Expand` plus a `Filter`. Also `ResolveGroupingAnalytics` and `GroupingAnalyticsTransformer` for `ROLLUP`/`CUBE`/`GROUPING SETS`

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
7. **Source sweep — [sql/catalyst — optimizer in the source map](reference/spark-source-map/sweeps/sql-catalyst-optimizer.md)** — everything that rewrites the join *before* a strategy is picked: which predicates can be pushed to which side of an outer join, why `isnotnull(joinKey)` appears in your plan without you writing it, `EliminateOuterJoin` quietly downgrading your `LEFT JOIN` to an inner join, and the float/NaN normalization that makes `-0.0` and `0.0` join as equal

8. **Source sweep — [core — rdd-layer in the source map](reference/spark-source-map/sweeps/core-rdd-layer.md)** — `CoGroupedRDD`, the primitive every RDD join bottoms out in, and the detail that carries over to DataFrames: the shuffle decision is made **per side**, so a parent already partitioned on the join key gets a narrow dependency while the other side shuffles alone
9. **Source sweep — [sql/catalyst — planner in the source map](reference/spark-source-map/sweeps/sql-catalyst-planner.md)** — why a join becomes a `BroadcastNestedLoopJoin`: `ExtractEquiJoinKeys` did not match, because no predicate was an equality between one side's attributes and the other's. The strategy selection itself is in sql/core; the shape recognition is here
10. **Source sweep — [sql/catalyst — framework in the source map](reference/spark-source-map/sweeps/sql-catalyst-framework.md)** — the join vocabulary itself: `joinTypes.scala` including `ExistenceJoin` (the internal type a subquery rewrite produces) and `UsingJoin` / `NaturalJoin` (which exist only until analysis rewrites them), plus the hint machinery — and `HintInfo.merge`, where **two conflicting strategy hints on one join do not error: the first wins with a warning**. Check for a second hint before concluding the optimizer ignored yours
11. **Source sweep — [sql/core — query-execution in the source map](reference/spark-source-map/sweeps/sql-core-query-execution.md)** — `DetectAmbiguousSelfJoin`, the rule behind `AMBIGUOUS_COLUMN_REFERENCE` and `spark.sql.analyzer.failAmbiguousSelfJoin`, plus where `JoinSelection` actually sits in `SparkPlanner.strategies` — **below** `SpecialLimits`, so a top-level `LIMIT` is planned before the join under it
12. **Source sweep — [sql/core — joins in the source map](reference/spark-source-map/sweeps/sql-core-joins-exec.md)** — the five physical operators read as code: `BroadcastHashJoinExec` is the only one that buffers nothing per task, `SortMergeJoinExec` streams one side but buffers **every buffered-side row sharing one key**, and `BroadcastNestedLoopJoinExec` is chosen with **no size check at all** whenever `ExtractEquiJoinKeys` finds no usable key. Also the nullability and `outputPartitioning` table per join type in `ShuffledJoin` — a full outer join returns `UnknownPartitioning`, so anything after it re-shuffles
13. **Source sweep — [sql/core — adaptive in the source map](reference/spark-source-map/sweeps/sql-core-adaptive.md)** — the runtime half of join selection. A join's strategy can still change *after* the plan you printed: a materialized stage reports `isRuntime` statistics and re-planning runs the whole `JoinSelection` ladder again against them. Two consequences worth memorising — a broadcast that has already been built can never be reverted (`LogicalQueryStageStrategy` forces the join to stay a BHJ even if the size tests now disagree), and AQE's skew splitting is by *map-index range*, so it never splits a single hot key
14. **Source sweep — [sql/core — aggregation, windows and exchange in the source map](reference/spark-source-map/sweeps/sql-core-agg-window-exchange.md)** — the build side of a broadcast join in detail — `BroadcastExchangeExec` starts its job on a separate thread pool from `doPrepare` so it overlaps with plan preparation; the row ceiling is ~341 M for a hash relation on a non-single-long key and 512 M otherwise; and the `collectTime` / `buildTime` / `broadcastTime` metrics separate a slow driver collect from a slow relation build
15. **Source sweep — [sql/core — Python and Arrow in the source map](reference/spark-source-map/sweeps/sql-core-python-arrow.md)** — the Python cogroup: `cogroup(...).applyInPandas` clusters and sorts both sides, zips them by key in the JVM, and writes each matched pair of groups as two Arrow streams over one worker connection — a join whose matching happens in Spark and whose combining happens in Python
16. **Source sweep — [sql/core — streaming execution in the source map](reference/spark-source-map/sweeps/sql-core-streaming-exec.md)** — the streaming join, which is a different operator from every join in this topic: `SymmetricHashJoinStateManager` keeps **two** logical stores per side (`keyToNumValues` and `keyWithIndexToValue`), so four in total, and bounds them only through `JoinStateWatermarkPredicates`. A stream-stream join with no watermark and no time-range condition retains state forever — the standard incident
17. **Source sweep — [sql/catalyst — analysis in the source map](reference/spark-source-map/sweeps/sql-catalyst-analysis.md)** — one more way a hint is silently dropped, this time before the optimizer sees it: `ResolveJoinStrategyHints` walks *down* the plan looking for the relations your hint names, and **the traversal stops at a view reference, a `WITH` clause and a subquery alias**. A hint naming a relation behind any of those never matches, routes to `conf.hintErrorHandler` (`hintRelationsNotFound`) and produces a warning, not an error. `RemoveAllHints` then deletes it. Also here: `NaturalAndUsingJoinResolution`, which is where `USING`/`NATURAL` joins stop existing

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

!!! note "New in Spark 4.2.0 — `NEAREST BY` join, and the vector-function family it sits on"
    Spark 4.2.0 adds `NEAREST BY` ([SPARK-56395]), a join primitive for nearest-neighbour queries with both Catalyst and DataFrame API support. It is not one of the seven relational join types and none of the books cover it — learn the seven first, then read the 4.2.0 SQL reference. `NEAREST BY` is the join member of a wider **vector primitive** set 4.2.0 adds for embedding/RAG work: scalar **distance and similarity** functions (cosine, dot, euclidean — [SPARK-54713]), **vector normalization** ([SPARK-55030]), and **vector aggregation** (avg/sum — [SPARK-55031]). These are ordinary built-in functions usable anywhere in a query, not just inside the join — learn them together the day you first need to rank by embedding distance. All 4.2.0, all book-absent; go to the 4.2.0 function reference.

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
7. **Spark-docs → Common Table Expressions** ([sql-ref-syntax-qry-select-cte.html](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-cte.html)) — `WITH` and `WITH RECURSIVE`; worth reading alongside the sweep below, which shows that a CTE is usually *inlined* rather than materialised
8. **Source sweep — [sql/catalyst — optimizer in the source map](reference/spark-source-map/sweeps/sql-catalyst-optimizer.md)** — what happens to your SQL after it parses: `EXCEPT`, `INTERSECT` and `DISTINCT` have no physical operator and are rewritten into joins and aggregates; `WITH` is inlined per definition by `InlineCTE`; and a correlated subquery becomes a semi/anti/outer join before planning
9. **Source sweep — [core — rdd-layer in the source map](reference/spark-source-map/sweeps/core-rdd-layer.md)** — `RDDOperationScope`, the mechanism behind the DAG visualization's named, nested boxes: every public RDD operation wraps its body in `withScope` and each RDD records the scope stack from a job local property, which is why a custom RDD built outside it appears unlabelled
10. **Source sweep — [sql/catalyst — types & parser in the source map](reference/spark-source-map/sweeps/sql-catalyst-types-parser.md)** — what happens to the SQL text: a two-stage parse (fast SLL, then a full LL retry, so a failing query is parsed **twice**), `ParseException` being a subclass of `AnalysisException`, and `AstBuilder`'s 222 visitors turning the parse tree into an *unresolved* plan. Also that SQL scripting, cursors and the pipe operator are gated inside the visitor rather than the grammar, which is why their errors name the feature
11. **Source sweep — [sql/catalyst — framework in the source map](reference/spark-source-map/sweeps/sql-catalyst-framework.md)** — what the session catalog actually holds: temp views are a plain in-memory `HashMap` and die with the session; global temp views live in a virtual database (`spark.sql.globalTempDatabase`); the file-source relation cache **never expires by default** (`spark.sql.metadataCacheTTLSeconds = -1`), which is what `REFRESH TABLE` exists for; and `spark.sql.catalogImplementation` is a *static* conf defaulting to `in-memory`
12. **Source sweep — [sql/core — query-execution in the source map](reference/spark-source-map/sweeps/sql-core-query-execution.md)** — the phase almost nobody knows about: DDL runs during `commandExecuted`, at the moment you call `spark.sql(...)`, not at an action — and `ResolveSessionCatalog` is the 1000-line rule that decides whether each DDL statement becomes a V1 `RunnableCommand` or stays a V2 command, which is why the same statement behaves differently against the session catalog and a custom one
13. **Source sweep — [sql/core — datasources in the source map](reference/spark-source-map/sweeps/sql-core-datasources.md)** — the physical side of DDL: every `CREATE`/`ALTER`/`DROP`/`SHOW` statement against a V2 catalog becomes one thin `V2CommandExec` subclass (about forty of them) that runs once on the driver and caches its rows, with `V2SessionCatalog` the adapter making Spark's own session catalog look like a `TableCatalog`. Also `ResolveSQLOnFile`, the rule behind the `SELECT * FROM parquet.`/path`` syntax
14. **Source sweep — [sql/core — the classic API in the source map](reference/spark-source-map/sweeps/sql-core-classic-api.md)** — two small pieces of the SQL front end: `VariableSubstitution`, which expands `${var}` before parsing and binds four prefixes (`spark`, `sparkconf`, `hivevar`, `hiveconf`) to the same provider with redaction on lookup; and `classic.Catalog`, which implements most of its API by **building a logical command and running it as a Dataset** rather than calling the catalog directly
15. **Source sweep — [sql/catalyst — analysis in the source map](reference/spark-source-map/sweeps/sql-catalyst-analysis.md)** — three pieces this topic needs. **Parameters:** `MoveParameterizedQueriesDown` pushes the wrapper *below* command nodes before `BindParameters` substitutes, which is why `EXPLAIN` over a parameterized query works. **`IDENTIFIER`:** `ResolveIdentifierClause` evaluates the identifier expression and then *re-runs the early batches* on the materialised plan — it holds `earlyBatches` for exactly that — and a parameter is a value while `IDENTIFIER` is a name, so only the first is safe by construction. **CTEs:** `spark.sql.legacy.ctePrecedencePolicy` decides whether an inner `WITH` shadows an outer one, and whether a CTE is inlined at all is decided here for commands and again by the optimizer for everything else — `WITH` is not a materialisation barrier

**Milestone:** You can register a DataFrame as a temp view, query it with `spark.sql()`, and mix SQL expressions into a method-chained DataFrame pipeline. Then, with a user-supplied value in hand: write the query so the value can never be parsed as SQL, and say why your approach guarantees that rather than merely making it unlikely.

!!! warning "Use parameterized SQL — string interpolation is the injection bug, not a style issue"
    Since Spark 3.4, `spark.sql` takes arguments: `spark.sql("SELECT * FROM t WHERE dt = :dt", {"dt": value})` for named parameters, or `?` with a list for positional.

    This is a *structural* fix rather than escaping. The query is parsed first, then the `BindParameters` analyzer rule substitutes each argument as a literal expression into the already-built plan — so a value cannot become SQL syntax no matter what it contains. And `spark.sql(text)` is literally defined as `spark.sql(text, Map.empty)`, so there is no cost to always using the parameterized form.

    Retreating to the DataFrame API to avoid injection concedes the SQL surface for no reason; parameters keep both.

!!! info "An unqualified name prefers a temp view; a qualified one cannot see temp views at all"
    `SessionCatalog.lookupRelation` resolves in a fixed order, and the asymmetry catches people. A bare `events` finds a temp view **before** a real table of the same name — so a temp view silently shadows a table. But `mydb.events` skips temp views entirely, so a qualified reference can never reach one.

    Two consequences: name your temp views distinctly to avoid shadowing production tables, and do not expect a database prefix to disambiguate *toward* a temp view — it disambiguates away from it. Global temp views are different again: they live in the `global_temp` database on the shared state, which is why they outlive the session that created them (see B2).

!!! note "New in Spark 4.2.0 — QUALIFY, search paths, metric views, and SQL surface additions"
    Additions the books predate: `QUALIFY` ([SPARK-31561]) filters on window-function results without a wrapping subquery — worth learning alongside I2; path-based name resolution (`SET PATH`, `CURRENT_PATH()`, [SPARK-54806]) changes how unqualified names resolve; and metric views (`CREATE VIEW … WITH METRICS`, [SPARK-54119]) add a declarative semantic-modelling surface. Learn the classic catalog model first — it's what the exam tests.

    Smaller 4.2.0 SQL additions worth knowing exist (reach for the 4.2.0 release notes for detail): explicit **`SYSTEM.BUILTIN`** qualification to force a built-in function past a same-named UDF, and **`SYSTEM.SESSION`** to name a temp view unambiguously ([SPARK-57109]; wired through the SQL-PATH resolution engine, [SPARK-56605]) — both make the name-resolution order above overridable rather than implicit; **`time_bucket`** for fixed-interval time-series bucketing ([SPARK-56594] — a cleaner alternative to the `window()` idiom, relevant to I2); **tuple sketches** for approximate multi-column cardinality ([SPARK-54179]); **`IGNORE NULLS` / `RESPECT NULLS`** now extended to the **aggregate** functions `collect_list` / `collect_set` / `array_agg` ([SPARK-55256], [SPARK-55533]) — this is about aggregation (relevant to I1's `collect_*`), separate from the `ignoreNulls` option that some window functions (`first`, `last`, `nth_value`) already carried (note `lag`/`lead` did *not* — they gained no such option here); and **top-K** forms of `max_by` / `min_by` that return the N extreme rows rather than one ([SPARK-55322], pairs with B6). All post-date every book here.

!!! note "New in Spark 4.2.0 — SQL pipe syntax gains aggregation and a shorter token"
    The SQL pipe operator (Spark 4.0) chains transformations left-to-right — `FROM t |> WHERE x > 0 |> SELECT a, b` — as an alternative to nesting subqueries. 4.2.0 makes it usable for real queries: aggregate functions and `GROUP BY` now work inside a `|>` step ([SPARK-54292]), and `|` is accepted as a shorter alias for the `|>` token ([SPARK-51518]). Reach for it when a query reads more naturally as a sequence of steps than as nested `SELECT`s; the classic form is still what the exam tests. Book-absent — 4.2.0 SQL reference.

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
7. **Source sweep — [sql/catalyst — optimizer in the source map](reference/spark-source-map/sweeps/sql-catalyst-optimizer.md)** — the wider set of null rules: `NullPropagation` / `NullDownPropagation`, `ReplaceNullWithFalseInPredicate` (a `NULL` in predicate position behaves as `false`), and `constructIsNotNullConstraints` — the constraint machinery that infers `isnotnull` from a null-intolerant expression and materialises it as a real filter
8. **Source sweep — [sql/core — joins in the source map](reference/spark-source-map/sweeps/sql-core-joins-exec.md)** — the execution side of `NOT IN` semantics: the null-aware anti join has three outcomes decided at build time, and the first null key encountered **frees the hash map and returns a sentinel**, so "any null in the subquery means an empty result" costs no probing at all. Also that the optimisation needs exactly one key per side, `LeftAnti`, `BuildRight` and no extra condition — anything else falls back to a normal anti join

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

### 🎯 Beginner Checkpoint

You are ready to leave this level when you can build a complete end-to-end batch pipeline:

- Read multi-source data (CSV + Parquet)
- Clean (null handling, type casting, deduplication)
- Transform (join, group, aggregate, derive columns)
- Write output to Parquet with a sensible partition scheme

---


### ⬜ B10 — Combining DataFrames: union, unionByName, and How Columns Are Matched

> Discovered from source sweep (new topic): `sql/catalyst: Union and set-operation column resolution`

**What it is:** union matches columns by position, unionByName matches by name, and allowMissingColumns fills the gaps with nulls — including inside nested structs.

**Why you need it:** Positional union against two DataFrames whose columns drifted apart produces wrong data with no error at all; knowing which of the three forms you are using, and what each does to nested fields, is the difference between a silent corruption and a caught mistake.

**Learn it with:**

1. **Spark-docs → Set Operators** ([sql-ref-syntax-qry-select-setops.html](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-setops.html)) — the SQL side: `UNION`, `INTERSECT`, `EXCEPT`/`MINUS`, and the `ALL` vs distinct difference. SQL `UNION` is always positional; there is no `BY NAME` in the grammar, so the name-matching behaviour exists only in the DataFrame API
2. **Spark-docs → PySpark API** ([DataFrame.unionByName](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.unionByName.html) and [DataFrame.union](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.union.html)) — the `allowMissingColumns` flag and the note that `union` is positional, matching SQL rather than the column names you can plainly see
3. **Source sweep — [sql/catalyst — analysis in the source map](reference/spark-source-map/sweeps/sql-catalyst-analysis.md)** — the concept "Union and set-operation column resolution": `Union` carries `byName` and `allowMissingCol` booleans, `ResolveUnion.compareAndAddFields` builds the per-side projection and recurses into struct fields via `mergeFields`, and `DeduplicateUnionChildOutput` fixes the case where one child's output repeats an `ExprId` (`SELECT a, a … UNION …`) by aliasing the duplicates — without which a deduplicating union groups on the first column only and returns the wrong rows (SPARK-37865)
4. **Related depth — A42** (UNION ALL: partitioning-aware output and codegen fusion) picks up where this topic stops: what the planner does with a union once its columns are resolved

!!! warning "No book covers this"

    Neither the Rioux book nor the personal book has a section on `union` versus `unionByName`. Both APIs are one line long, which is exactly why the difference goes unexamined — the topic is here because the source made the consequence visible, not because a book flagged it.

**Milestone:** Build two DataFrames with the same three column names in *different orders* and show that `df1.union(df2)` returns rows with values in the wrong columns while `df1.unionByName(df2)` does not — and confirm from `explain()` that `unionByName` inserted a `Project` on one side that `union` did not. Then: add a column to one side only, predict what `unionByName(allowMissingColumns=True)` puts in it, and say what happens without that flag. Finally, run `SELECT a, a FROM VALUES (1,1),(1,2) AS t(a,b) UNION SELECT a, b FROM VALUES (1,1),(1,2) AS t(a,b)`, state how many rows come back, and explain which rule made that the answer.

---

## Intermediate

**Goal:** Work confidently with complex data structures, window functions, UDFs, and the Delta Lake table format. Begin reading Spark execution plans. Write pipelines that don't fall over on real data.

**Estimated time to complete this level:** 38–54 hrs

**Reading order:** I1 → I2 → I3 → I4 → I5 → I6 → I7 → **I8 → I9 → I10 → I11** (the storage-and-table-format run) → I12. The level then ends with its checkpoint. I13–I32 sit around that gate as source-derived depth — they are not required to pass it, and are read on demand rather than in sequence. The one pairing worth keeping in order: **I12 → I31 → I32**, the SQL-scripting run, since cursors depend on condition handlers.

!!! info "Why the numbering jumps"
    I11 (Iceberg) closes the storage-and-table-format run; everything from I13 up is optional-depth material from source sweeps, numbered last because it sits outside the main line.

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
6. **Source sweep — [sql/catalyst — optimizer in the source map](reference/spark-source-map/sweeps/sql-catalyst-optimizer.md)** — why reading one field out of a wide struct can be cheap: `NestedColumnAliasing` rewrites the plan so the scan reads a narrower nested schema, `SimplifyExtractValueOps` means `struct(a, b).a` never builds the struct, chained `withField` calls collapse into one `UpdateFields`, and `from_json` is pruned to the fields you actually extract
7. **Source sweep — [sql/catalyst — expressions in the source map](reference/spark-source-map/sweeps/sql-catalyst-expressions.md)** — the mechanism behind the array/map functions: higher-order functions bind their lambda's parameter types from the *element* type after the arguments resolve, and `NamedLambdaVariable` is a `CodegenFallback` — so one `transform(...)` disables whole-stage codegen for the entire `Project` it sits in, not just for itself
8. **Source sweep — [sql/core — the classic API in the source map](reference/spark-source-map/sweeps/sql-core-classic-api.md)** — the typed grouping counterpart: `KeyValueGroupedDataset` carries key and value encoders and produces `MapGroups` / `CoGroup` plans over objects rather than rows, and `spark.sql.legacy.dataset.nameNonStructGroupingKeyAsValue` decides what the key column is called — the kind of naming difference that breaks a downstream `select`
9. **Source sweep — [sql/catalyst — analysis in the source map](reference/spark-source-map/sweeps/sql-catalyst-analysis.md)** — how a lambda parameter is bound, which is not like a column at all: `ResolveLambdaVariables` binds each `UnresolvedNamedLambdaVariable` to a `NamedLambdaVariable` whose type is taken from the *argument's* collection element type, a scoped binding that shadows outer names for the lambda's duration (`LambdaBinder` is the shared helper). `spark.sql.analyzer.allowSubqueryExpressionsInLambdasOrHigherOrderFunctions` decides whether a subquery may appear inside one at all

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
6. **Source sweep — [sql/catalyst — optimizer in the source map](reference/spark-source-map/sweeps/sql-catalyst-optimizer.md)** — the five rules that make windows cheaper before planning: `CollapseWindow` and `TransposeWindow` (two windows, one sort), `OptimizeWindowFunctions`, `EliminateWindowPartitions`, `LimitPushDownThroughWindow` — and `InferWindowGroupLimit`, which only fires above `spark.sql.optimizer.windowGroupLimitThreshold` (1000 rows per partition), so the top-N optimization you are counting on can quietly not apply
7. **Source sweep — [sql/catalyst — expressions in the source map](reference/spark-source-map/sweeps/sql-catalyst-expressions.md)** — the frame machinery: `RowFrame` counts rows and `RangeFrame` compares against the sort key (which is why `RANGE` needs exactly one `ORDER BY` column), and `SizeBasedWindowFunction` (`percent_rank`, `cume_dist`, `ntile`) needs the partition size, so the whole partition buffers before any row is emitted. Also why `window()` and `OVER (...)` share a name and nothing else
8. **Source sweep — [sql/core — aggregation, windows and exchange in the source map](reference/spark-source-map/sweeps/sql-core-agg-window-exchange.md)** — the execution side of every window: `WindowExec` buffers one *whole* window partition into an `ExternalAppendOnlyUnsafeRowArray` before emitting anything; seven factory keys map to six frame classes, of which only the both-bounded (moving) case re-aggregates per row; `BoundOrdering` is where a `RANGE INTERVAL` offset becomes a typed expression, and why a multi-column `ORDER BY` with a non-zero RANGE offset is rejected outright. See also **A34**, 4.2.0's segment-tree frame, which changes the cost of exactly that moving case
9. **Source sweep — [sql/core — Python and Arrow in the source map](reference/spark-source-map/sweeps/sql-core-python-arrow.md)** — the Python window operator, which solves a problem the JVM one does not have: a pandas UDF cannot ask where its frame starts, so `ArrowWindowPythonExec` **prepends the frame bounds to the data as ordinary integer columns**, two per bounded frame and none for unbounded ones. It extends `WindowExecBase`, so it inherits the same distribution, ordering, single-partition warning and row-array buffering as `WindowExec`
10. **Source sweep — [sql/catalyst — analysis in the source map](reference/spark-source-map/sweeps/sql-catalyst-analysis.md)** — the *other* window: `window(ts, "10 minutes", "5 minutes")` is not a function that runs, it is a plan rewrite `TimeWindowing` performs during analysis, replacing the expression with an `Expand` that emits one row per overlapping window plus a `Filter`. A 10-minute window sliding every 5 minutes therefore **doubles your row count before the aggregation runs** — visible in `explain()` as an `Expand` above the scan. `SessionWindowing` is the sibling for `session_window`, and both refuse two different window expressions in one operator

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
8. **Source sweep — [core — api-bridge in the source map](reference/spark-source-map/sweeps/core-api-bridge.md)** — the machine *underneath* every UDF: `BasePythonRunner`'s exact wire protocol (command → broadcasts → the eval-type integer → rows), the `PythonWorkerFactory` daemon/reuse/idle-pool/UDS lifecycle, and the failure plumbing that turns a Python crash, hang, or OOM into a Spark error (`faulthandler`, traceback dump, kill timeout, the Linux-only `setrlimit` memory cap). Re-swept 2026-08-09 with three additions that change how you read a UDF: a pickled command over 1 MiB is **broadcast** rather than shipped with the task; broadcasts are sent to a worker as a **delta** against what it already holds, so worker reuse saves re-sends as well as process starts; and `pyspark.errors`' structured fields exist only because `PythonErrorUtils` re-exposes `SparkThrowable`'s default methods, which Py4J cannot call
9. **Source sweep — [core — storage & serialization in the source map](reference/spark-source-map/sweeps/core-storage-serializer.md)** — new in 4.2.0: Python worker logs are captured into the `BlockManager` as `PythonWorkerLogBlockId` blocks ([SPARK-53755]/[SPARK-53975]), which is what finally makes a `print()` or `logging` call inside a UDF retrievable instead of stranded on the executor
10. **Source sweep — [sql/catalyst — expressions in the source map](reference/spark-source-map/sweeps/sql-catalyst-expressions.md)** — the catalyst-side view: `PythonUDF` has no `eval` and no `doGenCode` at all — it is a marker carrying an `evalType`, extracted by a planner rule — while `ScalaUDF` runs in-process and pays per-argument encoder conversion instead. Also the V2 function catalog's *magic method*: a `ScalarFunction` whose `invoke` signature matches code-generates into a direct static call rather than a boxed `produceResult`
11. **Source sweep — [sql/connect — client-server in the source map](reference/spark-source-map/sweeps/sql-connect-client-server.md)** — what has to happen before the Python worker protocol even starts on Connect: the UDF crosses the wire as a serialized closure, and the classes it references must already have been uploaded as artifacts into the session's own classloader. A UDF that works classically and fails remotely with `ClassNotFoundException` is this, not the worker
12. **Source sweep — [sql/hive — hive-metastore in the source map](reference/spark-source-map/sweeps/sql-hive-hive-metastore.md)** — the JVM-side Hive UDF wrappers (`HiveSimpleUDF`, `HiveGenericUDF`, `HiveGenericUDTF`, `HiveUDAFFunction`), which let a decade of Hive functions run unchanged. They defer `deterministic` and `foldable` to Hive's own `@UDFType` annotation — so a UDF whose annotation lies breaks constant folding and subexpression elimination — and `HiveGenericUDTF` is a `CodegenFallback`, costing its whole operator's whole-stage codegen
13. **Source sweep — [sql/core — datasources in the source map](reference/spark-source-map/sweeps/sql-core-datasources.md)** — the Python Data Source implementation behind the API: six separate `PythonPlannerRunner` round trips — lookup, schema/partition planning, filter pushdown, read, write, commit — so a Python connector pays a worker launch per phase rather than per query. Also the lookup order that decides which implementation wins: a JVM data source registered under the same short name **always** beats a Python one, because the Python fallback is only reached after every JVM lookup has failed
14. **Source sweep — [sql/core — aggregation, windows and exchange in the source map](reference/spark-source-map/sweeps/sql-core-agg-window-exchange.md)** — where a UDAF actually runs: `ScalaUDAF` (the old `UserDefinedAggregateFunction`) is an `ImperativeAggregate` that converts rows to Scala types on every update, while `ScalaAggregator` — the typed `Aggregator`, what `udaf()` registers — is a `TypedImperativeAggregate` whose buffer is serialized only at a shuffle boundary. The same page's `AggregationIterator` concept covers buffer-offset assignment, the classic imperative-UDAF bug
15. **Source sweep — [sql/core — Python and Arrow in the source map](reference/spark-source-map/sweeps/sql-core-python-arrow.md)** — the JVM half of every PySpark UDF, in one page: `ExtractPythonUDFs` lifting a UDF out of the expression tree into its own operator; the chaining rules that decide whether two UDFs cost one worker crossing or two (**iterator pandas UDFs never chain in parallel**); the `HybridRowQueue` that buffers the *entire input partition* a second time so results can be re-joined positionally, spillable but with no spill metric; the runner conf map — the exact list of SQL configs that reach the worker at all; and `PythonSQLMetrics`, whose split of `pythonBootTime` / `pythonInitTime` from `pythonProcessingTime` is how you tell a slow UDF from a slow worker
16. **Source sweep — [sql/core — the classic API in the source map](reference/spark-source-map/sweeps/sql-core-classic-api.md)** — where a UDF stops being an object and becomes a name: every `UDFRegistration.register` ends at `functionRegistry.createOrReplaceTempFunction(name, builder, source)`, with `source` recorded as `scala_udf` / `python_udf` / `java_udf` so `SHOW FUNCTIONS` and errors can say where it came from. `registerJava` reflectively loads the class and infers its return type, which is the path behind confusing classpath errors
17. **Source sweep — [streaming — DStream in the source map](reference/spark-source-map/sweeps/streaming-dstream.md)** — a different Python bridge from the one this topic traces, worth knowing exists. `PythonDStream` is how PySpark's DStream API reaches the JVM, and its notable behaviour is a failure policy rather than a data path: `stopStreamingContextIfPythonProcessIsDead` is called from **both** the job generator and the job scheduler on error, so a dead Python worker deliberately takes the streaming context down instead of leaving batches to pile up silently

**Milestone:** You can replace a Python UDF with a pandas UDF and measure the speedup **on 4.2.0** — not quote a book's figure; you can load an ML model once per partition using an Iterator UDF and say which config makes that pay off; you can test a UDF locally without a SparkSession. Then, from `explain()`: name which eval operator your UDF ran under, and explain why chaining a plain UDF and a pandas UDF in one `select` costs more than chaining two of the same kind.

!!! warning "The Arrow default flipped in 4.2.0 — re-measure rather than trusting the books"
    `spark.sql.execution.pythonUDF.arrow.enabled` now defaults to **`true`**, so a plain `@F.udf` is Arrow-serialized instead of pickled row by row. The "pandas UDFs are 5–10× faster" figures in Rioux and LS2e were measured against per-row pickle, which is no longer what you get by default.

    The hierarchy still holds directionally — built-ins beat UDFs, vectorised beats scalar — but the *gaps* have narrowed and the reason to prefer a pandas UDF is now more about expressing vectorised logic than about escaping pickle. Measure on your own 4.2.0 stack.

    One trap while benchmarking: if PyArrow or pandas is missing, Spark **silently falls back** to the non-Arrow path with only a `RuntimeWarning`. Identical code can run at very different speeds in two environments.

!!! note "New in Spark 4.2.0 — pandas 3, and zero-copy interchange with Polars/DuckDB"
    Two Arrow-adjacent 4.2.0 changes touch this topic. First, **pandas 3 *compatibility* work landed** — dtype/groupby behaviours were fixed for pandas 3 ([SPARK-56310], [SPARK-56327] among others) — but do not read the blog's flat "pandas 3 support" as "4.2.0 runs on pandas 3". Verified against the `v4.2.0` source: the runtime `install_requires` is `pandas>=2.2.0` (**no** upper bound), but `require_minimum_pandas_version()` emits a `FutureWarning` on pandas ≥ 3.0.0 — *"PySpark does not yet fully support pandas >= 3.0.0"* — and [SPARK-57974] added the `<3.0.0` cap only to the *dev* requirements and install docs, with the explicit note that **4.2.0 does not support pandas 3; official support arrives in 4.3.0 via [SPARK-55139]**. So 4.2.0 is *prepared for* pandas 3, not shipping it as the supported runtime — pin the pandas version your cluster actually runs, and re-test any UDF relying on in-place mutation or implicit dtype coercion when you do move up. (This is exactly why the project verifies version claims against local source, not the blog — the blog overstated it.) Second, PySpark now speaks the **Arrow C Data Interface / PyCapsule protocol** ([SPARK-54337]), which lets a DataFrame's Arrow batches move to and from tools like **Polars and DuckDB with no serialization copy** — the fastest way to hand a Spark result to another in-process engine. Relevant once your pipeline mixes engines; both are docs-and-source territory (no book), verify on your own 4.2.0 stack.

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
8. **Source sweep — [core — rdd-layer in the source map](reference/spark-source-map/sweeps/core-rdd-layer.md)** — the full RDD anchor set — transformations vs actions, the lineage encoded in `Dependency`, closure cleaning via ASM, and `take`'s incremental partition scan
9. **Source sweep — [core — api-bridge in the source map](reference/spark-source-map/sweeps/core-api-bridge.md)** — the *Python* side of the RDD story: how a `PythonRDD` pipes each partition through an external worker process, and how `PythonBroadcast` / `PythonAccumulatorV2` carry shared variables across the JVM↔Python boundary over an auth'd socket (the concrete mechanism behind PySpark broadcast vars and accumulators). Re-swept 2026-08-09: results never come back through Py4J — `collect()` materializes the array in the driver JVM *and then* rebuilds it in the Python process, over an authenticated socket (see **I38**), and the Hadoop InputFormat family is a separate route with its own `Writable` conversion rules (see **I37**)
10. **Source sweep — [sql/core — query-execution in the source map](reference/spark-source-map/sweeps/sql-core-query-execution.md)** — where an RDD enters and leaves a SQL plan: `LogicalRDD` / `RDDScanExec` (also what `df.checkpoint()` leaves behind), and the typed-`Dataset` object operators that convert `UnsafeRow` to JVM objects and back around every typed lambda
11. **Source sweep — [core — execution engine in the source map](reference/spark-source-map/sweeps/core-execution-engine.md)** — the *write* half of the RDD API, which the RDD Programming Guide covers in one sentence. `saveAsHadoopFile` does not go through Spark SQL at all: `SparkHadoopWriter.write` runs its own job, and `HadoopWriteConfigUtil` is the shim serving both the `mapred` and `mapreduce` APIs. Two consequences worth carrying — an RDD write always gets `HadoopMapReduceCommitProtocol` (it ignores `spark.sql.sources.commitProtocolClass`), and bytes-written metrics refresh only every 256 records while record counts are exact
12. **Source sweep — [streaming — DStream in the source map](reference/spark-source-map/sweeps/streaming-dstream.md)** — RDDs with a lifecycle, which is the one thing this topic does not cover. A `DStream` is an **RDD factory keyed by `Time`**: `getOrCompute(t)` memoises into a `generatedRDDs: Map[Time, RDD]`, applies the storage level, marks the RDD for checkpointing on the checkpoint interval, and a later `clearMetadata` drops and unpersists everything older than `rememberDuration` (`spark.streaming.unpersist`, default true). Two carry-overs. Retention is per DStream in the chain, so driver memory grows with window length in a way no batch RDD program does. And `DStream.persist()` defaults to **`MEMORY_ONLY_SER`**, not `MEMORY_ONLY` as on an RDD — a different default for the same-named method

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
6. **Source sweep — [core — rdd-layer in the source map](reference/spark-source-map/sweeps/core-rdd-layer.md)** — `Partitioner`, `HashPartitioner` vs `RangePartitioner` with its reservoir sampling, `ShuffledRDD`, and the narrow `coalesce` path that avoids a shuffle
7. **Source sweep — [core — execution engine in the source map](reference/spark-source-map/sweeps/core-execution-engine.md)** — `computeValidLocalityLevels` and the delay-scheduling timers — including that a `NO_PREF` task is reported as `PROCESS_LOCAL`, so the UI's locality column can flatter you
8. **Source sweep — [core — shuffle & memory in the source map](reference/spark-source-map/sweeps/core-shuffle-memory.md)** — the shuffle index format — a prefix sum of partition lengths — and the reduce-side locality preference with its hardcoded 0.2 fraction
9. **Source sweep — [sql/catalyst — optimizer in the source map](reference/spark-source-map/sweeps/sql-catalyst-optimizer.md)** — what the optimizer does to a repartition before it ever reaches the shuffle: `CollapseRepartition` merges adjacent ones, `OptimizeRepartition` drops a `repartition()` with no explicit number when the child's partitioning already matches, and `ReplaceCTERefWithRepartition` inserts one you did not ask for so a shared CTE is computed once
10. **Source sweep — [sql/core — adaptive in the source map](reference/spark-source-map/sweeps/sql-core-adaptive.md)** — what AQE does to a partition count you chose. `repartition(n)` (`REPARTITION_BY_NUM`) is deliberately **absent** from `CoalesceShufflePartitions.supportedShuffleOrigins`, so its count survives; `repartition(col)` (`REPARTITION_BY_COL`) **is** coalescable and can come out with fewer partitions than you asked for. `REBALANCE` is two rules, not one — `OptimizeSkewInRebalancePartitions` splits the oversized partitions and `CoalesceShufflePartitions` merges the small ones, both aiming at `advisoryPartitionSizeInBytes`. And `parallelismFirst` (default true) means the advisory size is usually a *ceiling* you never reach, not a target
11. **Source sweep — [sql/core — datasources in the source map](reference/spark-source-map/sweeps/sql-core-datasources.md)** — the read-side half of partitioning, which this topic's write-side treatment does not reach: partition *discovery* walks each leaf directory upwards parsing `name=value`, stopping at the first unparseable segment or at `basePath`, and skipping any path containing `_temporary`. Then every value's type is **inferred from the directory name** by a fixed ladder (see I27) — which is how a partition column silently changes type. `recursiveFileLookup` and partition discovery are mutually exclusive by design
12. **Source sweep — [sql/core — aggregation, windows and exchange in the source map](reference/spark-source-map/sweeps/sql-core-agg-window-exchange.md)** — the shuffle operator itself: the six `ShuffleOrigin`s and what each one permits AQE to do; the per-`Partitioning` partitioner choice, including the sampling **job** a `RangePartitioning` runs before the shuffle can even start; and `needToCopyObjectsBeforeShuffle`, the per-row `copy()` that some shuffle paths force because SQL reuses one mutable `UnsafeRow`
13. **Source sweep — [sql/core — Python and Arrow in the source map](reference/spark-source-map/sweeps/sql-core-python-arrow.md)** — two partitioning facts on the Python side: the grouped-map and cogroup APIs require `ClusteredDistribution` **plus** an ordering — a shuffle and a sort, not just a shuffle — and degrade to `AllTuples` with no grouping keys; and `AttachDistributedSequenceExec`, the pandas-API-on-Spark default index, **caches the child RDD** to compute a globally consecutive sequence, so that index is a materialization barrier rather than free
14. **Source sweep — [core — api-bridge in the source map](reference/spark-source-map/sweeps/core-api-bridge.md)** — the Python-only partitioner. `PythonPartitioner`'s equality is `(numPartitions, id-of-the-Python-function)` — the CPython `id()` of your partitioning callable — so Spark's "already partitioned this way, skip the shuffle" reasoning rests on an address-derived integer that is only unique while PySpark holds a reference to the function. It also never trusts your function's answer: whatever it returns goes through `nonNegativeMod(_, numPartitions)`, and a null key lands in partition 0
15. **Source sweep — [sql/core — streaming execution in the source map](reference/spark-source-map/sweeps/sql-core-streaming-exec.md)** — why a stateful streaming query ignores your partitioning: the shuffle partition count is read from the checkpoint and stamped onto every stateful operator, so `spark.sql.shuffle.partitions` does nothing (see **E28** for the supported way to change it). Also `StateStoreRDD.getPreferredLocations`, which asks the driver coordinator where each store was last loaded so a partition is scheduled next to its existing RocksDB directory

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
8. **Source sweep — [core — rdd-layer in the source map](reference/spark-source-map/sweeps/core-rdd-layer.md)** — `persist`/`cache` down to `getOrCompute` and the block manager, plus both checkpoint modes and how `markCheckpointed` truncates lineage
9. **Source sweep — [core — shuffle & memory in the source map](reference/spark-source-map/sweeps/core-shuffle-memory.md)** — unroll memory as the caching admission path, and why a cached DataFrame can be almost entirely evicted with nothing in the logs
10. **Source sweep — [core — storage & serialization in the source map](reference/spark-source-map/sweeps/core-storage-serializer.md)** — what a cached block physically is — the put path's memory-then-disk fallback, LRU eviction with its RDD self-eviction guard, and why `MEMORY_ONLY` on an oversized partition silently caches a prefix
11. **Source sweep — [core — rpc & resources in the source map](reference/spark-source-map/sweeps/core-rpc-resources.md)** — how a remote read of a cached partition actually travels: `BlockTransferService`, its own transport server and thread pools, and the `spark.shuffle.io.maxRetries` layer beneath it
12. **Source sweep — [sql/core — query-execution in the source map](reference/spark-source-map/sweeps/sql-core-query-execution.md)** — the SQL-side cache, which is columnar, not row-based: `CacheManager` matching on the **normalized pre-optimization** plan (so cache hits survive optimizer config changes), the pluggable `CachedBatchSerializer` behind `spark.sql.cache.serializer`, per-batch min/max skipping, and the `PartitionKeyedAccumulator` that exists because concurrent AQE cache-build jobs previously let a non-empty cache report zero rows and **silently drop data**
13. **Source sweep — [sql/core — adaptive in the source map](reference/spark-source-map/sweeps/sql-core-adaptive.md)** — caching's interaction with AQE, which is not in any tuning guide: a cached relation becomes a `TableCacheQueryStageExec` (never reused — a fresh stage each time it is encountered), and `CacheManager` clones the session with `spark.sql.adaptive.applyFinalStageShuffleOptimizations` turned **off** unless `spark.sql.optimizer.canChangeCachedPlanOutputPartitioning` (default false) is set. So by default a cached DataFrame stores its *un-coalesced* partition count
14. **Source sweep — [sql/core — aggregation, windows and exchange in the source map](reference/spark-source-map/sweeps/sql-core-agg-window-exchange.md)** — the aggregate side of memory pressure: `TungstenAggregationIterator` does not merely spill — it converts the whole hash map into a sorted spill, retries the allocation once, and then switches the *task* to sort-based aggregation for the remaining input, so the operator named in the plan is not necessarily the algorithm that ran
15. **Source sweep — [sql/core — Python and Arrow in the source map](reference/spark-source-map/sweeps/sql-core-python-arrow.md)** — a memory consumer that no cache-tuning discussion mentions: every Python UDF operator registers a `HybridRowQueue` with the task's `TaskMemoryManager` to hold its entire input partition, competing with execution memory and spilling whole Tungsten pages to disk queues under pressure — invisibly, since the operator publishes no spill metric
16. **Source sweep — [sql/core — streaming execution in the source map](reference/spark-source-map/sweeps/sql-core-streaming-exec.md)** — a memory consumer outside the cache: the HDFS-backed state store holds a partition's whole state map in JVM heap, competing with everything this topic covers, while RocksDB moves it to native memory and local disk under a bounded budget. The state store's maintenance thread also carries its own timeouts and a deletion budget, so cleanup can lag behind a large version backlog
17. **Source sweep — [sql/core — the classic API in the source map](reference/spark-source-map/sweeps/sql-core-classic-api.md)** — what `df.cache()` actually stores, end to end. The format is pluggable through `spark.sql.cache.serializer`, and the built-in `DefaultCachedBatchSerializer` batches `inMemoryColumnarStorage.batchSize` rows into per-column `ByteBuffer`s with a `ColumnType` codec each, accumulating min/max/null-count/size statistics that `buildFilter` later uses to **skip whole batches**. With compression on (the default) each column is trial-encoded against six schemes — `PassThrough`, `RunLength`, `Dictionary`, `BooleanBitSet`, `IntDelta`, `LongDelta` — and the best wins, so caching is an encode pass whose cost depends on your data's distribution. Also `checkpoint` vs `localCheckpoint`, which share one implementation differing only in `reliableCheckpoint` and `eager`

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
7. **Source sweep — [core — execution engine in the source map](reference/spark-source-map/sweeps/core-execution-engine.md)** — where the UI's numbers come from: the listener events, the accumulator merge, and why skipped stages are grey — `MapOutputTracker` still holds their output. Re-swept 2026-08-09: the *production* side of every number the UI renders is now traced — each metric is a `LongAccumulator` on `TaskMetrics`, arriving by two independent routes (heartbeat and task result), which is why a live page and a finished page can disagree. Study that mechanism as **E49** before trusting a number you are diagnosing from
8. **Source sweep — [sql/catalyst — optimizer in the source map](reference/spark-source-map/sweeps/sql-catalyst-optimizer.md)** — how to see *why* a plan looks the way it does, rather than guessing: `spark.sql.planChangeLog.level` plus `.rules` / `.batches` logs the plan diff after a single named rule, and `spark.sql.planChangeValidation` checks after every rule that the plan is still resolved, schema-stable and free of dangling references. Both are off by default and neither is in the UI
9. **Source sweep — [core — storage & serialization in the source map](reference/spark-source-map/sweeps/core-storage-serializer.md)** — the `BlockId` taxonomy behind every name in the Storage tab: `rdd_5_12` is an `RDDBlockId`, and the seven distinct shuffle block kinds are why "shuffle block" in a log line is ambiguous. Also what the Storage tab's numbers *are* (`RDDInfo`, `StorageStatus`) and where two UI buttons come from: **thread dump and heap histogram are `BlockManagerStorageEndpoint` messages**, so an executor whose storage endpoint is wedged cannot serve the dump that would explain why
10. **Source sweep — [core — monitoring in the source map](reference/spark-source-map/sweeps/core-monitoring.md)** — the two renderers behind the tabs you read: `RDDOperationGraph`, which rebuilds the DAG view from the scope strings each `StageInfo` carries and truncates it past `spark.ui.dagGraph.retainedRootRDDs`, and the `/api/v1` REST resources that serve the same numbers as JSON — scrape those rather than the HTML
11. **Source sweep — [sql/catalyst — planner in the source map](reference/spark-source-map/sweeps/sql-catalyst-planner.md)** — `QueryPlanningTracker`: the four phases (`parsing`, `analysis`, `optimization`, `planning`) behind the SQL tab's timings, and `topRulesByTime(k)` for the per-rule breakdown. A query whose *planning* phase dominates is unusual and points at a strategy returning many candidates
12. **Source sweep — [sql/catalyst — expressions in the source map](reference/spark-source-map/sweeps/sql-catalyst-expressions.md)** — what a missing `*` in `EXPLAIN` actually means: whole-stage codegen has three independent off-switches (a `CodegenFallback` expression anywhere in the operator, too many nested output *or input* fields, columnar execution) plus a fourth — the interpreted fallback — that leaves the plan text unchanged. And the 8000-byte HotSpot JIT limit sits far below `spark.sql.codegen.hugeMethodLimit`, so `too long to be JIT compiled` in the executor log is the diagnosis for a query that codegens and still crawls
13. **Source sweep — [sql/catalyst — framework in the source map](reference/spark-source-map/sweeps/sql-catalyst-framework.md)** — two things that make a plan readable: a `!` prefix on an operator means `missingInput` is non-empty (it references a column its children do not provide), and per-rule timings come from `QueryExecutionMetering` — process-wide via `RuleExecutor.dumpTimeSpent()`, per-query via `QueryPlanningTracker`. Reach for the tracker on one slow query, the meter when profiling a rule across a workload
14. **Source sweep — [sql/core — query-execution in the source map](reference/spark-source-map/sweeps/sql-core-query-execution.md)** — where the SQL tab's numbers are produced and thrown away: `SQLMetric`'s `initValue = -1` convention (a metric that never updated reports 0 and is excluded from min/max), `SQLAppStatusListener`'s throttled writes, and `spark.sql.ui.retainedExecutions`. Also the two **silent** whole-stage-codegen fallbacks — a stage that fell back at runtime still prints its `*(n)` marker, and the only evidence is a driver log line
15. **Source sweep — [sql/core — joins in the source map](reference/spark-source-map/sweeps/sql-core-joins-exec.md)** — which join numbers exist in the SQL tab and which do not: broadcast hash join publishes only `numOutputRows` (its build cost lives on the `BroadcastExchange` node above it), shuffled hash join adds `buildDataSize` and `buildTime`, sort-merge join adds `spillSize` — the one metric that tells you a key group overflowed. Skew handling publishes nothing; the only evidence is `(skew=true)` in the post-AQE operator name
16. **Source sweep — [sql/core — adaptive in the source map](reference/spark-source-map/sweeps/sql-core-adaptive.md)** — why the SQL tab and `df.explain()` disagree, mechanically. `AdaptiveSparkPlanExec.generateTreeString` prints `== Initial Plan ==` alongside `== Current Plan ==` / `== Final Plan ==`, but only once something has changed — before an action the two are identical and a single tree prints. Also the conditional metric set on `AQEShuffleReadExec`: `numSkewedPartitions` and `numCoalescedPartitions` **do not exist** unless the corresponding optimization fired, so a missing metric means "did not happen", not zero
17. **Source sweep — [sql/core — datasources in the source map](reference/spark-source-map/sweeps/sql-core-datasources.md)** — which log lines and metrics actually exist for the scan and the write. `FileSourceStrategy` logs `Pushed Filters:` and `Post-Scan Filters:` at `INFO` on every plan (a filter in *both* is normal — Parquet's row-group filter is best-effort); the V2 equivalent is a `Pushing operators to <relation>` block. Writes publish exactly four driver metrics — `numFiles`, `numOutputBytes`, `numOutputRows`, `numParts` — plus an `Expected N files, but only saw M` warning almost nobody has seen. And the driver-side parallel file listing is a **real Spark job with no SQL query attached**, which is why slow planning looks like nothing at all in the SQL tab
18. **Source sweep — [sql/core — Python and Arrow in the source map](reference/spark-source-map/sweeps/sql-core-python-arrow.md)** — the six metrics every Python operator publishes to the SQL tab — `pythonDataSent`, `pythonDataReceived`, `pythonBootTime`, `pythonInitTime`, `pythonTotalTime`, `pythonProcessingTime`, plus `pythonNumRowsReceived`. Reading boot and init separately from processing turns 'the pandas UDF node is slow' into a decidable question, and it costs nothing
19. **Source sweep — [core — api-bridge in the source map](reference/spark-source-map/sweeps/core-api-bridge.md)** — where those Python numbers are produced, and when they are silently absent. The worker sends one `TIMING_DATA` frame per task carrying boot/init/finish timestamps and processing time; `handleTimingData` adds them to four accumulators **only if the caller supplied a metrics map** — the RDD-path runner passes `Map.empty`, so on a plain `rdd.map(pyfunc)` the timings are logged to the executor and dropped. The spill counters in the same frame are unconditional, which is why Python-side spill appears in a JVM stage's metrics
20. **Source sweep — [sql/core — the classic API in the source map](reference/spark-source-map/sweeps/sql-core-classic-api.md)** — why every action is one SQL-tab entry with its own metrics: `Dataset.withAction` wraps each one in `SQLExecution.withNewExecutionId`, attaches the query-execution id, resets the plan's metrics first, and renames internal failures after the action (`The "collect" action failed`)

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
6. **Source sweep — [sql/catalyst — analysis in the source map](reference/spark-source-map/sweeps/sql-catalyst-analysis.md)** — `TIMESTAMP AS OF` / `VERSION AS OF` resolution, and why the timestamp expression may reference no column and is pinned when the query is analysed rather than per batch
7. **Source sweep — [sql/core — datasources in the source map](reference/spark-source-map/sweeps/sql-core-datasources.md)** — the two hooks Delta and Iceberg use inside Spark's own write, both of them plain SPIs rather than anything Delta-specific: `WriteStatsTracker`, which collects per-file statistics during the write pass that produces the files, and `RequiresDistributionAndOrdering`, by which a V2 connector *demands* a distribution and sort and Spark inserts the exchange to satisfy it — the mechanism behind controlled file layout. Also `spark.sql.sources.partitionOverwriteMode`: the default `STATIC` deletes the destination **before** running the job, so a failure after that point has already destroyed the old data

!!! warning "Delta 4.3.1 does not support Spark 4.2.0 — check before starting this topic"
    Delta's build targets exactly two Spark versions, **4.0.1 and 4.1.0**, with 4.1 as the default (`project/CrossSparkVersions.scala`, `ALL_SPECS`). There is no 4.2 target.

    So I8 — along with I9, A6 and E4, which all build on it — cannot be practised on the 4.2.0 stack the rest of this path targets. Run a separate Spark 4.1 environment for the Delta topics, or take them after the rest.

    This is now the **second** table format in this position: [I11](#i11-apache-iceberg-and-table-format-interoperability) has the same gap for the same reason. Both lag the Spark release by design — a table format has to be built and tested against a released Spark, so a new Spark minor is always ahead of its connectors. Worth planning around rather than treating as a surprise: pin your learning stack to the newest Spark that *your table format* supports, not the newest Spark.

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
5. **Delta-docs → Table streaming reads and writes** ([delta-streaming.html](https://docs.delta.io/latest/delta-streaming.html)) — `maxFilesPerTrigger`, `startingVersion`, and schema-change handling; this is the mechanism the bronze layer's "incremental processing" actually is
6. **Source trace — [I9 in the source map](reference/spark-source-map/topics/i9.md)** — the four mechanisms the pattern is assembled from, and why a `MERGE` updating ten rows can rewrite five hundred files

!!! info "Medallion is a convention, not a feature — the value is in the four mechanisms under it"
    There is no `MedallionTable`, no config, no API. Bronze/Silver/Gold is a naming discipline for *where you spend correctness effort*. What makes it worth a topic is that each transition rests on a mechanism with real semantics:

    | Transition | Mechanism | The thing to understand |
    |---|---|---|
    | source → bronze | streaming read | offsets are `(version, index)`, so a huge commit splits across batches and a restart resumes mid-commit |
    | bronze → silver | schema enforcement | every write passes through `updateMetadata`; `autoMerge` decides evolve-vs-fail, `isReadCompatible` decides what is legal at all |
    | bronze → silver | `MERGE INTO` | two jobs plus a full file rewrite (see below) |
    | any → gold | Change Data Feed | read what changed instead of rescanning — but it must be enabled *before* the changes happen |

    And the layers exist so failures are recoverable: bronze keeps raw data so silver can be rebuilt, silver keeps cleaned history so gold can be recomputed. That is the justification for the storage cost, and it is the part the three-box diagram never conveys.

!!! warning "`MERGE` cost scales with files touched, not rows changed"
    A `MERGE INTO` is **two passes and a rewrite**: `findTouchedFiles` joins source against target to find which data files contain matches, then `writeAllChanges` rewrites each of those files in full.

    So updating 10 rows scattered across 500 files rewrites 500 files. The row count is almost irrelevant; the *spread* is everything. This is why partitioning, clustering and file sizing matter more in the silver layer than anywhere else, and it turns "partition thoughtfully" from advice into arithmetic you can do in advance. An insert-only merge skips the rewrite path entirely, which is why append-only bronze ingestion is cheap by comparison.

!!! warning "This topic needs Spark 4.1 — it is Delta all the way down"
    Every mechanism here is a Delta feature, and Delta 4.3.1 does not build against Spark 4.2.0 (see [I8](#i8-delta-lake-basics)). Plan I8, I9, A6 and E4 as a group on a Spark 4.1 environment.

**Milestone:** You can build a three-layer pipeline from raw Parquet files to a Gold aggregation table, with schema enforcement on silver, using your local Unity Catalog stack. Then two that show you understand the mechanisms rather than the diagram: send a record with an unexpected column into silver and predict whether the write evolves the schema or fails, naming the setting that decides it; and estimate how many files a `MERGE` updating a handful of rows will rewrite, before running it.

---

### ⬜ I10 — Data Formats: Parquet, Delta, Avro, JSON

**What it is:** Columnar vs row storage; predicate pushdown; column pruning; Parquet row groups and page footers; when to use each format.

**Why you need it:** Format choice is a major performance variable. The Catalyst optimizer exploits Parquet metadata — but only if the file is written correctly.

**Learn it with:**

1. **LS2e Ch 4** — data sources and format comparison
2. **SDG Ch 9** — the most complete treatment of every format option
3. **DLDG Ch 1** — how Delta wraps Parquet and what the transaction log adds
4. **Spark-docs → Parquet** ([sql-data-sources-parquet.html](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html)) — partition discovery, schema merging, and the predicate-pushdown knobs; pair with **Performance Tuning** ([sql-performance-tuning.html](https://spark.apache.org/docs/latest/sql-performance-tuning.html)) for the columnar-reader settings
5. **Spark-docs → ORC** ([sql-data-sources-orc.html](https://spark.apache.org/docs/latest/sql-data-sources-orc.html)) and **Avro** ([sql-data-sources-avro.html](https://spark.apache.org/docs/latest/sql-data-sources-avro.html)) — the two comparison points: ORC is Parquet's architecture with different defaults, Avro is the row-oriented case and ships as a separate artifact
6. **Source trace — [I10 in the source map](reference/spark-source-map/topics/i10.md)** — where columnar execution actually stops, why pushdown is per-filter rather than per-format, and what `VARIANT` changed
7. **Source sweep — [sql/catalyst — planner in the source map](reference/spark-source-map/sweeps/sql-catalyst-planner.md)** — how a V2 table reports statistics: `computeStats` asks the connector through `SupportsReportStatistics`, and a format that does not implement it gets the default estimate — which is what starves the cost-based optimizer regardless of how good the file-level metadata is
8. **Source sweep — [sql/catalyst — types & parser in the source map](reference/spark-source-map/sweeps/sql-catalyst-types-parser.md)** — the three text formats' parsers in detail: Univocity for CSV with its 20480-column and column-pruning limits, Jackson for JSON with **filter pushdown into the parser** and partial results, and the 4.1 Stax rewrite for XML with the old parser still behind `spark.sql.legacy.useLegacyXMLParser`. Also `singleVariantColumn`, the JSON option that ingests a whole record as one `VARIANT`
9. **Source sweep — [sql/hive — hive-metastore in the source map](reference/spark-source-map/sweeps/sql-hive-hive-metastore.md)** — the same Parquet and ORC files are read by **two completely different code paths**. Conversion to Spark's vectorised datasource is decided by `serde.toLowerCase.contains("parquet")`, so a renamed SerDe drops you onto Hive's row-at-a-time SerDe reader with no vectorisation or pushdown — `Scan hive` in the plan where you expected `FileScan parquet`. Also `spark.sql.orc.impl=hive`, the legacy ORC reader that still lives in this module
10. **Source sweep — [sql/core — datasources in the source map](reference/spark-source-map/sweeps/sql-core-datasources.md)** — the format layer as source. The vectorized Parquet reader is gated on the **result** schema — `schema.forall(isBatchReadSupported)` over required *plus* partition columns — so one unsupported column drops the whole scan onto row-at-a-time `parquet-mr` with only a `DEBUG` line. Filter pushdown skips **row groups**, not rows, unless you enable `recordLevelFilter`, which requires giving up vectorization. Parquet's `In` pushdown changes shape at `pushdown.inFilterThreshold` (10), and no filter inside an array or map ever pushes because Parquet supports pushdown only for non-repeated primitives. ORC matches Hive-written files **by ordinal** whenever every field is named `_col*`. Avro's file format now lives in `sql/core`, not the separate connector module
11. **Source sweep — [sql/core — the classic API in the source map](reference/spark-source-map/sweeps/sql-core-classic-api.md)** — the 4.x change in how a read is planned: `spark.read.format(...).load(...)` no longer resolves the provider eagerly — it builds a single `UnresolvedDataSource` and lets the analyzer pick V1 or V2, infer schema and apply options. The exception is the `json(Dataset[String])` / `csv(Dataset[String])` family, which must infer inline
12. **Source sweep — [core — storage & serialization in the source map](reference/spark-source-map/sweeps/core-storage-serializer.md)** — Avro's *other* code path. Separate from the `avro` data source, Spark ships `GenericAvroSerializer`, a custom Kryo serializer for `GenericRecord` values held in RDDs, caches, and shuffle. Pre-registering schemas with `SparkConf.registerAvroSchemas` replaces the full schema text with a 64-bit parsing fingerprint per record — through an `avro.schema.*` config namespace with **no `spark.` prefix and no `ConfigEntry`**, so it appears in no configuration listing. Registering on only one side fails the read with `ERROR_READING_AVRO_UNKNOWN_FINGERPRINT`; registering nowhere is safe but writes a compressed schema with every record

!!! info "Spark is columnar at the scan and nowhere else"
    The vectorized reader fills `ColumnarBatch`es of 4096 rows directly from Parquet row groups, constructing no per-row objects. Then `ColumnarToRowExec` converts the batch to `UnsafeRow` as soon as an operator cannot consume columnar input — which, in open-source Spark, is almost immediately.

    So the **reader** is vectorized and the **engine** is row-based (Tungsten). That is the honest account of why Parquet is fast and why the gain is bounded to I/O and decoding rather than the whole query. It is also what makes plugins like Gluten comprehensible: they exist to push that boundary further up the plan.

    Find `ColumnarToRowExec` in your `explain()` output — its position tells you exactly where the columnar advantage ended.

!!! warning "Delta and Iceberg are not formats in this list — they are layers over Parquet"
    Comparing "Parquet, Delta, Avro, JSON" as peers is a category error that this topic's own title invites. Parquet, ORC, Avro and JSON are **storage formats**: how bytes are laid out in one file. Delta and Iceberg are **table formats**: metadata describing which files constitute a table, layered on top of Parquet (I8, I11).

    The practical question is therefore two questions. *Which storage format* — columnar for analytics, row for whole-record access and streaming payloads. *Which table format, if any* — none for immutable data, Delta or Iceberg once you need atomic updates, time travel or concurrent writers.

**Milestone:** You can explain why `F.col("date") > '2024-01-01'` on a Parquet file can be resolved without reading any data, and why the same filter on a CSV cannot. Then, from a real plan: find `ColumnarToRowExec` and say what it tells you about where the columnar advantage stopped; and given a filter that was *not* pushed down, explain why the format is usually not the reason.

!!! note "New in Spark 4.2.0 — geospatial and TIME types across file formats"
    Native `GEOMETRY` and `GEOGRAPHY` types with `ST_*` functions, WKB/WKT and Parquet I/O, and an SRID registry ([SPARK-51658]) — **enabled by default**, no extension needed. Spark 4.2.0 also lands the `TIME` type across file formats, and vectorized data loading ([SPARK-55722]). None of the books cover any of this; go to the 4.2.0 docs.

---

### ⬜ I11 — Apache Iceberg and Table-Format Interoperability

**What it is:** The Iceberg table format — metadata tree (catalog → metadata file → manifest list → manifests), snapshots, hidden partitioning and partition evolution, schema evolution, the REST Catalog specification; how it compares to Delta Lake, and the interoperability layers (Delta UniForm, Iceberg's own catalog spec) that let one copy of the data serve several engines.

**Why you need it:** This path teaches Delta everywhere else, which reflects the Databricks certification track. The wider market has moved: Iceberg is the default choice for new open lakehouses, its REST Catalog is the de-facto interoperability standard, and every major platform — AWS, Snowflake, Google, and Databricks itself via UniForm — now reads and writes it. Delta fluency alone increasingly reads as Databricks-specific fluency. The concepts transfer (both are metadata-over-Parquet with snapshot isolation); the file layouts, catalog models, and operational commands do not.

**Learn it with:**

1. **Iceberg-DG Ch 2–3** — *Apache Iceberg: The Definitive Guide*, Shiran, Hughes & Merced (O'Reilly, 2024) — the architecture and metadata tree, then the read/write query lifecycle; the clearest treatment of why the manifest layout enables planning that Hive-style partitioning cannot. **Ch 5** covers catalogs (see E5). The publisher (Dremio) hosts a [free full PDF](https://www.dremio.com/wp-content/uploads/2023/02/apache-iceberg-TDG_ER1.pdf)
2. **DLDG Ch 1** — re-read the Delta transaction log chapter *after* the Iceberg metadata tree; the contrast is what makes both stick
3. **Iceberg-docs → Spark Getting Started** ([iceberg.apache.org/docs/latest/spark-getting-started/](https://iceberg.apache.org/docs/latest/spark-getting-started/)) — catalog configuration and the runtime jar, which is the part that actually blocks beginners
4. **Iceberg-docs → Multi-Engine Support** ([iceberg.apache.org/multi-engine-support/](https://iceberg.apache.org/multi-engine-support/)) — the authoritative Spark-version support matrix; check it before choosing a runtime jar
5. **Local stack** — create the same dataset as both a Delta and an Iceberg table, then diff the on-disk metadata directories
6. **Source trace — [I11 in the source map](reference/spark-source-map/topics/i11.md)** — the one-sentence design difference from Delta, why the catalog rather than the filesystem provides atomicity, and how pruning happens twice
7. **Source sweep — [sql/catalyst — analysis in the source map](reference/spark-source-map/sweeps/sql-catalyst-analysis.md)** — time-travel resolution is table-format agnostic: the analyzer produces a `TimeTravelSpec` and hands it to the catalog, so Iceberg and Delta differ in what they do with it, not in how it is parsed

!!! info "Delta replays a log; Iceberg follows a pointer to a tree"
    That single sentence explains most of the differences, and it is the way to learn both rather than memorising two systems.

    **Delta** keeps ordered JSON commits; current state is the log replayed, and atomicity is *exactly one writer being able to create `N.json`*. **Iceberg** keeps a catalog pointer → metadata file → snapshot → manifest list → manifests → data files, and atomicity is a **compare-and-swap of that pointer, performed by the catalog**.

    Three consequences worth carrying:

    - **The catalog is Iceberg's first architectural decision** (`rest`, `hive`, `hadoop`, `glue`), because it is the coordination point for commits. For Delta it barely arises. This is also why the REST Catalog spec is what enables cross-engine writes — coordination lives in a service, not in filesystem semantics.
    - **Pruning happens twice.** Whole manifests are skipped on partition bounds *before being opened*, then surviving manifests prune data files on column stats. That extra indirection is why planning stays cheap on very large tables.
    - **Hidden partitioning means queries never name partition columns.** A spec maps a source column through a transform, so filtering the source column is enough — no derived `dt` column, no directory layout leaking into SQL. And because the metadata holds a *list* of specs, partitioning evolves without rewriting data.

    Columns are identified by **field ID**, not name — so rename is metadata-only and drop-then-add cannot resurrect old data.

!!! warning "Iceberg does not support Spark 4.2 yet — confirmed at source"
    Verified in the Iceberg 1.11.0 checkout: the repo contains `spark/v3.4`, `v3.5`, `v4.0` and `v4.1` modules and nothing newer. The newest supported Spark is **4.1** (`iceberg-spark-runtime-4.1_2.13`); 3.5 and 4.0 are also Maintained. There is no 4.2 runtime jar, so this topic cannot be practised on the 4.2.0 stack the rest of this path targets. Either run a separate Spark 4.1 environment for this topic, or defer it until an Iceberg release adds 4.2. Re-check the multi-engine support page rather than assuming — this is the fastest-moving fact on this page.

**Milestone:** You can create an Iceberg table from Spark, evolve its partitioning without rewriting the data, query a previous snapshot, and explain — pointing at the actual files — how Iceberg's manifest tree and Delta's `_delta_log` differ in how a reader discovers which data files belong to the current snapshot. You can state what UniForm does and does not solve.

---

### ⬜ I12 — SQL Scripting

**What it is:** Multi-statement SQL scripts with procedural constructs: `BEGIN...END` compound bodies, local variable declarations (`DECLARE`, `SET`), `IF...THEN...ELSIF...ELSE`, `CASE` (searched and simple), `WHILE`, `FOR`, `LOOP`, `REPEAT...UNTIL`, and `LEAVE`/`ITERATE` for loop control. New in Spark 4.0.

**Why you need it:** SQL scripting lets you express multi-step procedural logic — conditional branches, loops, intermediate variables — entirely in SQL without switching to Python. Useful for complex ETL stored as SQL scripts and for interoperability with data warehouses that already use procedural SQL.

**Learn it with:**

1. **Spark-docs → SQL Scripting** ([sql-ref-scripting.html](https://spark.apache.org/docs/latest/sql-ref-scripting.html)) — the canonical reference; covers all statement types with examples
2. **Spark 4.0 release notes** — understand which constructs were added in 4.0 vs 4.1
3. **Spark-docs → Compound statement** ([control-flow/compound-stmt.html](https://spark.apache.org/docs/latest/control-flow/compound-stmt.html)) and the per-statement pages beside it — [IF](https://spark.apache.org/docs/latest/control-flow/if-stmt.html), [CASE](https://spark.apache.org/docs/latest/control-flow/case-stmt.html), [WHILE](https://spark.apache.org/docs/latest/control-flow/while-stmt.html), [REPEAT](https://spark.apache.org/docs/latest/control-flow/repeat-stmt.html), [LOOP](https://spark.apache.org/docs/latest/control-flow/loop-stmt.html), [FOR](https://spark.apache.org/docs/latest/control-flow/for-stmt.html), [LEAVE](https://spark.apache.org/docs/latest/control-flow/leave-stmt.html), [ITERATE](https://spark.apache.org/docs/latest/control-flow/iterate-stmt.html). The compound-statement page carries the rule the others assume: declarations come first, in a fixed order
4. **Source sweep — [sql/core — SQL scripting in the source map](reference/spark-source-map/sweeps/sql-core-sql-scripting.md)** — the interpreter itself: the `CompoundBody` that bypasses the analyzer entirely, the frame/scope stack, the in-order iterator that *is* the control flow, and the two behaviours nothing else documents — a script runs during analysis rather than on an action, and only its **last** result set is returned
5. **Source sweep — [sql/catalyst — framework in the source map](reference/spark-source-map/sweeps/sql-catalyst-framework.md)** — where SQL scripting's state lives: `VariableManager` / `TempVariableManager` back `DECLARE` and `SET VARIABLE`, `SqlScriptingContextManager` pushes a scoped manager for a script's local variables, and `SQLFunction` stores a SQL UDF as **text** that is re-parsed on use. Cursors arrived in 4.2.0 behind `spark.sql.scripting.cursorEnabled` (default false)
6. **Source sweep — [sql/core — query-execution in the source map](reference/spark-source-map/sweeps/sql-core-query-execution.md)** — the physical commands behind scripting's newest feature: `DECLARE`/`OPEN`/`FETCH`/`CLOSE CURSOR` ([SPARK-54759]), which store the cursor query as **unparsed SQL text** so parameter markers survive until `OPEN`, plus the session-variable commands they share a strategy with
7. **Source sweep — [sql/catalyst — analysis in the source map](reference/spark-source-map/sweeps/sql-catalyst-analysis.md)** — the analysis half of scripting. `VariableResolution` resolves a bare name to a session variable **only when `SYSTEM.SESSION` is on the SQL resolution path**, which is why a variable never shadows a column of the same name; `ResolveSetVariable` rejects duplicate targets and caps the source query so a multi-row result errors rather than silently taking the first row. Note the naming trap: `spark.sql.variable.substitute` is textual `${}` expansion done *before* parsing and has nothing to do with `DECLARE`d variables

!!! info "No book covers this — docs and source only"
    SQL scripting landed in Spark 4.0, after every book in the resources table. Rioux (2022), LS2e (2020) and SDG (2018) have nothing on it. Treat the docs page as primary and verify behaviour against your own 4.2.0 stack rather than waiting for a book to catch up.

!!! info "Error handling and cursors are their own topics"
    This topic is the procedural core: blocks, variables, branches, loops. The two features that hang off it are large enough to study separately — [I31](#i31-sql-scripting-condition-handlers-exit-continue-and-sqlstate-matching) for `DECLARE ... HANDLER`, and [I32](#i32-sql-cursors-row-at-a-time-iteration-and-where-the-snapshot-is-taken) for cursors. Read them in that order after this one; the cursor loop depends on the handler mechanism.

!!! warning "`spark-sql` cannot run a script"
    The CLI splits input on semicolons before parsing and passes `enableSqlScripting = false` to the splitter, so a `BEGIN … END` block arrives as several broken fragments. There is a scripting-aware mode in `StringUtils.splitSemiColonWithIndex`, but nothing calls it with that flag set in 4.2.0. Use `spark.sql(...)`, JDBC/Thrift, or Connect — Connect works unchanged, because its SQL command calls the same classic `SparkSession.sql`.

!!! warning "`SET` inside a script assigns a variable — it cannot change a config"
    The grammar rule behind `SET spark.sql.…` and `RESET` is reachable only from a top-level statement and from `EXPLAIN`, never from a script body. Inside `BEGIN … END`, `SET` always means variable assignment, so `SET spark.sql.shuffle.partitions = 200` parses as an assignment to a four-part *variable* name and fails as an unresolved variable. Set every config — including the scripting feature gates themselves — from the session before you invoke the script.

**Milestone:** You can write a SQL script that declares a variable, iterates over a query result with `FOR`, applies a conditional with `IF...ELSIF`, and produces a result — and explain when you would choose SQL scripting over a Python pipeline. Then two that come from the source rather than the docs: say at what moment a script's statements actually execute (hint: not when you call an action), and what happens to the output of a `SELECT` that is not the last statement in the script.

---

### Optional depth — source-derived topics

**Not required for the checkpoint below.** These three came from a source sweep of `core`, not from a book, course or exam guide. Read one when you hit the underlying problem in practice — a `Task not serializable` error, a `groupByKey` OOM, a job that needs concurrent submission — rather than in sequence. They are numbered last in the level (I13–I15) because they sit outside the main line.

---

### ⬜ I13 — Pair RDD Aggregations: combineByKey, reduceByKey, groupByKey

> Discovered from source sweep (refinement): `core: pair-rdd-functions`

**What it is:** PairRDDFunctions adds key-value operations to RDD[(K,V)] via implicit conversion; all aggregations bottom out in combineByKeyWithClassTag, which either applies in-place or routes through ShuffledRDD.

**Why you need it:** The cost difference between reduceByKey (map-side combine) and groupByKey (no combine) is the canonical RDD-level skew and OOM lesson; understanding combineByKey explains every higher-level shuffle.

**Learn it with:**

1. **SDG Ch 13** — advanced RDDs; key-value operations and the aggregation family in full
2. **Spark-docs → Shuffle operations** ([rdd-programming-guide.html#shuffle-operations](https://spark.apache.org/docs/latest/rdd-programming-guide.html#shuffle-operations)) — what a shuffle costs and which operations trigger one
3. **Source trace — [I13 in the source map](reference/spark-source-map/topics/i13.md)** — the one implementation behind five API names, the boolean that separates the fast case from the slow one, and why the two fail differently rather than merely differing in speed
4. **Source sweep — [core — api-bridge in the source map](reference/spark-source-map/sweeps/core-api-bridge.md)** — PySpark does **not** use the JVM machinery this topic traces. A Python `groupByKey`/`combineByKey` aggregates in `pyspark/shuffle.py`'s own `ExternalMerger`, spilling against `spark.python.worker.memory` (default `512m`) — a config that exists in no catalog because nothing on the JVM reads it. Its spill bytes are then reported back over the worker pipe and folded into the JVM's `taskMetrics`, which is why a stage can show spill that no JVM operator produced

**Milestone:** You can explain why `reduceByKey` beats `groupByKey().mapValues(sum)` in terms of what crosses the network, and express both as a `combineByKey` call with its three functions. Then the sharper version: say what happens to each under a single hot key, and name the one argument that differs between them in the source.

!!! warning "`reduceByKey` degrades under skew; `groupByKey` fails"
    This is a difference in **failure mode**, not just performance, and it is the reason the usual advice is worth following.

    Both route through the same `combineByKeyWithClassTag`. The combine path builds an `ExternalAppendOnlyMap`, which **spills to disk** when memory runs short — slow, but it completes. `groupByKey` passes `mapSideCombine = false` and materialises every value for a key as an in-memory `Iterable`; the source scaladoc states outright that a key with too many values gives an `OutOfMemoryError`.

    So a hot key makes `reduceByKey` slow and `groupByKey` dead.

    Two related facts fall out of the same code. `reduceByKey(f)` passes `f` as *both* `mergeValue` and `mergeCombiners`, which is the mechanical reason `f` must be associative and commutative — Spark applies it within and across partitions. And when the accumulator type differs from the value type (an average needs `(sum, count)`), `aggregateByKey` is the right tool, not `groupByKey`.

!!! info "An already-partitioned RDD skips the shuffle entirely"
    `combineByKeyWithClassTag` checks `self.partitioner == Some(partitioner)` and, on a match, uses `mapPartitions` with **no `ShuffledRDD` at all**. This is the RDD-level counterpart of the partitioning negotiation in [I5](#i5-partitioning-concepts-and-control), and it is the payoff for `partitionBy` when several keyed aggregations share a key.

    Worth reading alongside [B6](#b6-basic-aggregations-and-groupby): `HashAggregateExec`'s partial/final split *is* `mapSideCombine` at the DataFrame level, and its sort-based fallback mirrors the spilling map here. The DataFrame API's real advantage is that it makes this choice for you.

---

### ⬜ I14 — Closure Cleaning and the Task-Not-Serializable Problem

> Discovered from source sweep (refinement): `core: closure-cleaning`

**What it is:** SparkContext.clean() delegates to ClosureCleaner (ASM 9 bytecode analysis) to null out unreferenced outer-object fields in Scala closures before they are serialized to executors.

**Why you need it:** Every transformation lambda passes through closure cleaning; failures here produce the ubiquitous Task not serializable error, and understanding the mechanism is required to reason about what driver-side state leaks into tasks.

**Learn it with:**

1. **Spark-docs → Understanding closures** ([rdd-programming-guide.html#understanding-closures](https://spark.apache.org/docs/latest/rdd-programming-guide.html#understanding-closures)) — the canonical explanation of why mutating a driver variable inside a transformation silently does nothing
2. **SDG Ch 14** — distributed shared variables; broadcast and accumulators as the correct alternatives to capturing driver state
3. **Source** — `core/src/main/scala/org/apache/spark/util/ClosureCleaner.scala`
4. **Source sweep — [core — storage & serialization in the source map](reference/spark-source-map/sweeps/core-storage-serializer.md)** — why closures are always Java-serialized — the closure serializer is hardcoded — and how `SerializationDebugger` builds the serialization stack you actually read
5. **Source sweep — [core — api-bridge in the source map](reference/spark-source-map/sweeps/core-api-bridge.md)** — the Python closure has a threshold the Scala one does not: `_prepare_for_python_RDD` cloudpickles your function and, if the result exceeds `spark.broadcast.UDFCompressionThreshold` (default 1 MiB), **broadcasts it instead of shipping it with the task** — silently, with no warning and no plan change. A UDF that captures a large object therefore changes transport mechanism rather than just getting slower, which is worth knowing before you conclude that capturing it "worked fine"

**Milestone:** You can explain why a counter incremented inside `foreach` stays zero on the driver, predict whether a given lambda will raise `Task not serializable` before running it, and name the two fixes (broadcast the value, or move construction inside the closure).

---


### ⬜ I15 — AsyncRDDActions: Non-Blocking Job Submission

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


### 🎯 Intermediate Checkpoint

You are ready to leave this level when you can:

- Build a layered pipeline (bronze → silver → gold) with `MERGE INTO` upserts, and predict roughly how many files a merge will rewrite before running it
- Use window functions for time-series feature engineering, naming the default frame you get with and without an `ORDER BY`
- Read a Spark UI physical plan, locate the bottleneck, and say whether the numbers you are looking at are complete
- Write and test a pandas UDF, and measure its speedup on 4.2.0 rather than quoting a book
- Choose between a storage format and a table format for a given dataset, and explain what Delta and Iceberg each add over plain Parquet
- Explain when caching helps, when it is evicted, and why a cache hit depends on plan equivalence rather than your variable

*Optional:* this is the natural point for the Databricks Associate Developer exam if you want it — see [optional certification milestones](#optional-certification-milestones). Not a prerequisite for continuing.

---





### ⬜ I16 — Approximate Actions and Partial Results

> Discovered from source sweep (gap): `core: approximate-actions`

**What it is:** `countApprox`, `countByValueApprox`, `sumApprox`, `meanApprox` and `countByKeyApprox` submit an ordinary job, but hand each task's result to an incremental evaluator as it lands and return a `PartialResult[BoundedDouble]` — a point estimate plus a confidence interval — once a wall-clock timeout expires. RDD-only; there is no DataFrame or SQL equivalent, and none of it works over Spark Connect.

**Why you need it:** The API reads as "get a cheap answer fast" and is not. The timeout bounds only how long *the driver* blocks — the job is never cancelled, so the cluster does exactly the work a full `count()` would. The interval extrapolates from the fraction of *partitions* completed, which assumes unseen partitions resemble seen ones; on skewed data the small partitions finish first, so the estimate is biased low and the stated confidence is not the achieved confidence.

**Learn it with:**

1. **Spark-docs → RDD API (Scala)** ([RDD.html](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/rdd/RDD.html)) — the signatures and the `confidence` parameter; the narrative RDD Programming Guide does not list the approximate actions at all
2. **Spark-docs → RDD Programming Guide, Actions** ([rdd-programming-guide.html#actions](https://spark.apache.org/docs/latest/rdd-programming-guide.html#actions)) — the surrounding action model these build on
3. **Source sweep — [core — rdd-layer in the source map](reference/spark-source-map/sweeps/core-rdd-layer.md)** — the `runApproximateJob` path, the three-way timeout decision in `ApproximateActionListener`, the Poisson/Normal/Student-t models behind each evaluator, and the three silent-failure paths

!!! warning "No book in the resources table covers this"

    Rioux (2022), LS2e (2020) and SDG (2018) all teach the RDD action set without the approximate family. This is docs-and-source territory — read the sweep, then verify against your own 4.2.0 stack.

!!! warning "Three failures here are silent"

    A job that fails *after* the timeout is never reported: `PartialResult.setFailure` is unreachable from the main source tree, so `getFinalValue()` on a timed-out result blocks forever. Only successful tasks merge, so retries silently shrink the sample. And an empty RDD returns `(0, +Inf)` at confidence `0.0`, indistinguishable from "learned nothing". From PySpark the timeout is inert entirely — it calls the blocking `getFinalValue()`.

**Milestone:** You can explain why `countApprox(timeout=100)` on a large RDD saves no cluster time, predict whether the returned `BoundedDouble` will be biased high or low on a skewed RDD and say why, and name the method whose call makes the timeout meaningless in PySpark.

---

### ⬜ I17 — Whole-File and Binary RDD Sources

> Discovered from source sweep (gap): `core: whole-file-sources`

**What it is:** `SparkContext.binaryFiles`, `wholeTextFiles` and `binaryRecords` read whole files — or fixed-length records — as RDD records. The first two set `isSplitable = false` and pack whole files into splits with `CombineFileInputFormat`; `binaryRecords` is the only splittable one. They are governed by the `spark.files.*` config family, which is **not** the `spark.sql.files.*` family that DataFrame reads use.

**Why you need it:** Whole-file reads are the standard on-ramp for images, PDFs, logs and scientific binary formats. The two most common failures follow directly from `isSplitable = false` — one task per giant file, and an OOM inside `PortableDataStream.toArray()`, which loads an entire file into a single JVM byte array. Neither consequence is documented user-facing.

**Learn it with:**

1. **Spark-docs → RDD Programming Guide, External Datasets** ([rdd-programming-guide.html#external-datasets](https://spark.apache.org/docs/latest/rdd-programming-guide.html#external-datasets)) — the canonical description of `wholeTextFiles` and `binaryFiles`, including the small-files rationale
2. **Spark-docs → Binary File Data Source** ([sql-data-sources-binaryFile.html](https://spark.apache.org/docs/latest/sql-data-sources-binaryFile.html)) — `spark.read.format("binaryFile")`, the modern Connect-compatible successor you should usually reach for instead
3. **Source sweep — [core — rdd-layer in the source map](reference/spark-source-map/sweeps/core-rdd-layer.md)** — the two different split formulas, which configs each entry point actually reads, and the silent truncation and encoding paths

!!! warning "No book in the resources table covers this"

    SDG Ch 12 and LS2e Ch 3 cover RDD creation from text files, but none of the three books covers `binaryFiles`, `binaryRecords`, or the `spark.files.*` config family. Docs and source only.

!!! warning "`minPartitions` means opposite things in the two whole-file APIs"

    `binaryFiles` computes `max(sc.defaultParallelism, minPartitions)` — a *floor* that `defaultParallelism` can override, so it cannot lower the partition count. `wholeTextFiles` uses `ceil(totalLen / minPartitions)` with no `defaultParallelism` term and no cap, so it is a genuine target. Only `binaryFiles` reads `spark.files.maxPartitionBytes` and `openCostInBytes`; `wholeTextFiles` ignores both.

!!! warning "Corrupt-file handling truncates silently"

    With `spark.files.ignoreCorruptFiles=true`, a mid-file `IOException` marks the partition finished and the **job succeeds with a truncated result** — the only trace is a log warning. `wholeTextFiles` decodes as UTF-8 with replacement, so a latin-1 file yields U+FFFD rather than an error, despite the scaladoc requiring UTF-8.

**Milestone:** You can predict how many partitions `binaryFiles` produces for 10,000 small files given `spark.default.parallelism`, explain why passing `minPartitions=2` does not reduce that number, and say what `spark.read.format("binaryFile")` gives you that `SparkContext.binaryFiles` does not.

---

### ⬜ I18 — Dependency Management at Submit Time: --packages, Ivy, and Jars

> Discovered from source sweep (gap): `core: dependency-resolution`

**What it is:** `spark-submit` resolves `--packages` through Apache Ivy *before* anything touches the classpath, using a fixed resolver chain — local `~/.m2`, the local Ivy cache, Maven Central, then spark-packages — which `--repositories` and `spark.jars.ivySettings` modify. Resolved jars are merged into `spark.jars`, and for Python applications into `spark.submit.pyFiles` as well, since a Spark package can carry Python code.

**Why you need it:** `--packages` is how nearly every connector reaches your job — Kafka, Delta, Iceberg, JDBC drivers, cloud filesystem implementations. It is also the part of submission with the most opaque failures, and none of them look like a dependency problem at the point they surface.

**Learn it with:**

1. **Spark-docs → Submitting Applications, Advanced Dependency Management** ([submitting-applications.html#advanced-dependency-management](https://spark.apache.org/docs/latest/submitting-applications.html#advanced-dependency-management)) — the canonical description of `--packages`, `--repositories`, `--jars` and how each is distributed
2. **Spark-docs → Configuration, Runtime Environment** ([configuration.html#runtime-environment](https://spark.apache.org/docs/latest/configuration.html#runtime-environment)) — `spark.jars`, `spark.jars.packages`, `spark.jars.ivy`, `spark.jars.ivySettings` and their interactions
3. **Source sweep — [core — submit & standalone in the source map](reference/spark-source-map/sweeps/core-submit-standalone.md)** — the resolver chain and its order, Spark's automatic exclusions, where resolution actually happens per cluster manager, and the three ways a dependency silently is not there
4. **Source sweep — [resource-managers/yarn — AM & executor allocation in the source map](reference/spark-source-map/sweeps/resource-managers-yarn-am-executor.md)** — where those resolved jars physically go on YARN, which Ivy resolution says nothing about. Everything is copied to a per-application HDFS staging directory (`.sparkStaging/<appId>`, created `700`) and registered as YARN `LocalResource`s; a `local:` URI is never copied at all. Three facts worth having: **leaving `spark.yarn.jars` unset re-uploads the whole Spark distribution on every submit** — `prepareLocalResources` zips `$SPARK_HOME/jars` into a temp archive with one WARN line — so pre-staging the jars once turns a multi-second submit into a sub-second one; resource **visibility is decided by HDFS permissions, not a config** (`PUBLIC` needs the file other-readable *and* every ancestor other-executable, which the `700` staging directory forecloses, so a shared `PUBLIC` jars location has to live outside it); and files are de-duplicated by URI **and by basename**, because two different paths with the same file name make YARN fail the container launch outright. `spark.jars.ivySettings` is itself localized in cluster mode and its config value rewritten to the localized name

!!! warning "No book covers this"

    SDG, LS2e and Rioux all describe `spark-submit` at the level of `--class` and `--master`. Ivy resolution, `ivySettings`, and the exclusion rules are docs-and-source territory — which is unfortunate, because this is where firewalled and air-gapped environments spend their time.

!!! warning "Three ways a dependency silently is not there"

    A resolution failure throws a bare `RuntimeException` whose message is the `toString` of Ivy's problem list, with no coordinate context. A package whose artifact is a `pom` or `bundle` rather than a `jar` is filtered out at info level and "resolves successfully" while contributing nothing. And a missing local jar — or *any* remote jar — passed to `--jars` is warned about and skipped, so the failure arrives much later as `ClassNotFoundException`.

!!! info "Where resolution happens depends on the cluster manager"

    In client mode and on YARN, the *submitting* machine resolves. In standalone and Kubernetes **cluster** mode it is skipped entirely and the configs are forwarded so the driver resolves after it starts — which means the driver needs the repository access, not your laptop. Note also that Spark's default Ivy home is `~/.ivy2.5.2`, not `~/.ivy2`, so a pre-warmed cache is ignored.

**Milestone:** You can load a connector with `--packages` and explain where the jars were fetched and to which machine, configure `spark.jars.ivySettings` for a private mirror, and diagnose a job that starts cleanly but fails with `ClassNotFoundException` for a class you believe you supplied.

---

### ⬜ I19 — Sampling: sample, takeSample, and Stratified Sampling

> Discovered from source sweep (new topic): `core: sampling`

**What it is:** four APIs that look interchangeable and are not. `df.sample(fraction)` / `rdd.sample(...)` is a **lazy transformation**: each partition gets its own derived seed and runs a sampler over its rows independently. `rdd.takeSample(num)` is an **action** that runs at least two jobs — a `count()`, then a `collect()` — and loops re-sampling until it has enough rows. `df.sampleBy(col, fractions)` / `rdd.sampleByKey` does **stratified** sampling with a per-stratum fraction, and `sampleByKeyExact` makes extra passes to hit the counts exactly. In SQL, `TABLESAMPLE` offers rows, percent, and bucket variants.

**Why you need it:** developing against a subset and building QA sets are everyday tasks, and each of these has a behaviour that surprises people. `fraction` is an *expectation*, not a row count — `sample(0.1)` on 1000 rows does not return 100. `takeSample` pulls into driver memory and its re-sample loop has no iteration cap. Strata you do not name in `sampleBy` get fraction zero and vanish silently. And a sample taken downstream of a shuffle is classified `INDETERMINATE`, which means a stage retry does not quietly return different rows — it triggers rollback or aborts the job.

**Learn it with:**

1. **Spark-docs → Sampling Queries** ([sql-ref-syntax-qry-select-sampling.html](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-sampling.html)) — `TABLESAMPLE` in its three forms (`x ROWS`, `x PERCENT`, `BUCKET x OUT OF y`), and the statement that it returns an *approximate* number of rows
2. **Spark-docs → `DataFrame.sample`** ([pyspark.sql.DataFrame.sample](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.sample.html)) — the API you will actually reach for, with the explicit note that it does not guarantee the requested fraction of the total count
3. **Spark-docs → `DataFrame.sampleBy`** ([pyspark.sql.DataFrame.sampleBy](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.sampleBy.html)) — stratified sampling, including the rule that an unlisted stratum is treated as fraction zero
4. **Source sweep — [core — rdd-layer in the source map](reference/spark-source-map/sweeps/core-rdd-layer.md)** — the per-partition seeding, `takeSample`'s two-plus jobs and uncapped re-sample loop, the exact-vs-approximate stratified split, and `getOutputDeterministicLevel` promoting a sample over an `UNORDERED` parent to `INDETERMINATE`

!!! warning "No book covers this beyond the one-line API"

    SDG, LS2e and Rioux each mention `sample` in passing as a convenience and none discusses the determinism consequence, the driver-memory cost of `takeSample`, or the difference between `sampleByKey` and `sampleByKeyExact`. The behaviour that matters is in the API docs and the source.

!!! warning "Sampling after a shuffle can abort your job on a retry"

    `PartitionwiseSampledRDD` reports `INDETERMINATE` when its parent is `UNORDERED` — i.e. anything downstream of a shuffle. That is the same classification the stage-rollback machinery keys on (see [A14](#a14-determinism-indeterminate-stages-and-correctness-under-retry)): a fetch failure that forces a retry makes Spark roll back succeeding stages or abort, rather than mix rows from two different samples. Caching or checkpointing the sample is the usual fix.

!!! info "Reproducibility comes from the seed *and* the partitioning"

    Each partition's seed is derived once, at RDD construction, from the job seed — so re-running the same RDD returns the same rows. Change the partition count, and every partition's seed changes with it, so the "same" `sample(0.1, seed=42)` over a repartitioned input is a different sample.

**Milestone:** You can explain why `sample(0.1)` on a 1000-row DataFrame does not return exactly 100 rows, say what `takeSample(false, 1000)` costs in jobs and where the result lands, predict what happens to a stratum you omit from a `sampleBy` fractions map, and explain why sampling immediately after a `repartition` is riskier than sampling before it.

---


### ⬜ I20 — ANSI Mode, EvalMode, and Error-Safe Evaluation with try_*

> Discovered from source sweep (new topic): `sql/catalyst: Cast, EvalMode and ANSI — the three evaluation modes and where the errors come from`

**What it is:** The three per-expression evaluation modes (LEGACY, ANSI, TRY) that decide whether an overflow, a bad cast or a division by zero returns null or raises an error, and the `try_*` function family that opts one expression out of the session setting.

**Why you need it:** ANSI mode is on by default in Spark 4.x, so casts and arithmetic that returned null on Spark 3.x now fail the job — and `try_cast` / `try_add` are the per-expression escape hatch that lets you keep strictness everywhere else.

**Learn it with:**

1. **Spark-docs → ANSI Compliance** ([sql-ref-ansi-compliance.html](https://spark.apache.org/docs/latest/sql-ref-ansi-compliance.html)) — the reference page for this topic: the arithmetic and cast tables, the **three** kinds of type conversion (cast, store assignment, type coercion), the reserved-keyword list, and the `spark.sql.storeAssignmentPolicy` setting that governs `INSERT`
2. **Spark-docs → Conversion Functions** ([api/sql/conversion-functions](https://spark.apache.org/docs/latest/api/sql/conversion-functions/)) — where `cast` and `try_cast` are specified side by side; the rest of the `try_*` family (`try_add`, `try_divide`, `try_element_at`, `try_to_number`, …) is spread across the math, string and collection groups of the [built-in function index](https://spark.apache.org/docs/latest/api/sql/)
3. **Spark-docs → Migration Guide, SQL** ([sql-migration-guide.html](https://spark.apache.org/docs/latest/sql-migration-guide.html)) — the Spark 3.x → 4.x entries are largely *this topic*; read it as the list of queries that change meaning
4. **Source sweep — [sql/catalyst — expressions in the source map](reference/spark-source-map/sweeps/sql-catalyst-expressions.md)** — the `Cast` / `EvalMode` concept: three separate cast-validity tables (`canCast`, `canAnsiCast`, `canANSIStoreAssign`), why `try_add` is an `EvalMode.TRY` arithmetic node rather than a try/catch, and where the "line N, position M" context in a Spark 4 error message comes from

!!! warning "No book covers this"

    Rioux, SDG and LS2e were all written against Spark 3.x, where `spark.sql.ansi.enabled` defaulted
    to **false**. Every cast and overflow example in them describes the LEGACY mode behaviour —
    null on failure. On Spark 4 the same code raises. This topic is the largest silent behaviour
    gap between the books and the engine you are running.

!!! info "`try_add` is not a try/catch, and the difference is visible"

    `try_add(a, b)` rewrites to `Add(a, b, EvalMode.TRY)` — the addition itself checks for overflow
    and returns null. It does **not** suppress an error raised by a child expression, so
    `try_add(1, cast('x' as int))` still fails in ANSI mode. Wrap the failing operation, not the
    outer one.

**Milestone:** You can predict, for `SELECT CAST('abc' AS INT)` and for an `INT` addition that
overflows, what Spark 3.5 returns and what Spark 4.2 does; rewrite both to return null without
disabling ANSI mode session-wide; explain why a cast rejected in a `SELECT` can be accepted by an
`INSERT INTO` the same column (store assignment is a different table); and name what
`spark.sql.storeAssignmentPolicy` changes that `spark.sql.ansi.enabled` does not.

---


### ⬜ I21 — String Collation

> Discovered from source sweep (new topic): `sql/catalyst: Collation — Collate, CollationKey, and collation-aware hashing`

**What it is:** Per-column collation on `StringType` (Spark 4.0+): the `COLLATE` clause and `collate()` function, what `UTF8_BINARY` / `UTF8_LCASE` / ICU collations change about comparison and equality, and the collation key that makes grouping and joining agree with comparison.

**Why you need it:** Collation changes the meaning of `=`, `GROUP BY`, `DISTINCT` and join keys on string columns, and it is the supported replacement for the `lower(col) = lower(col)` idiom — but only if you know which operations are collation-aware and which fall back to bytes.

**Learn it with:**

1. **Spark-docs → Data Types** ([sql-ref-datatypes.html](https://spark.apache.org/docs/latest/sql-ref-datatypes.html)) — `StringType` takes a collation parameter, defaulting to `UTF8_BINARY`; this is where the type-level story starts
2. **Spark-docs → SHOW COLLATIONS** ([sql-ref-syntax-aux-show-collations.html](https://spark.apache.org/docs/latest/sql-ref-syntax-aux-show-collations.html)) — the catalogue of available collations and the naming scheme (`SYSTEM.BUILTIN.UTF8_LCASE`, ICU locales, the `_AI` / `_CI` / `_RTRIM` suffixes)
3. **Spark-docs → String Functions** ([api/sql/string-functions](https://spark.apache.org/docs/latest/api/sql/string-functions/)) — `collate` and `collation`, plus which string functions are collation-aware
4. **Source sweep — [sql/catalyst — expressions in the source map](reference/spark-source-map/sweeps/sql-catalyst-expressions.md)** — the collation concept: `Collate` is a pure metadata pass-through with no runtime cost, and the real machinery is `CollationKey`, injected into **join keys** by `HashJoin` so that hashing agrees with comparison
5. **Source sweep — [sql/catalyst — types & parser in the source map](reference/spark-source-map/sweeps/sql-catalyst-types-parser.md)** — the type-system half of collation: `StringType` is a *parameterized* type carrying a `collationId`, so `STRING` and `STRING COLLATE UTF8_LCASE` are different types that need coercion to union; `supportsBinaryEquality` and `supportsBinaryOrdering` are the predicates every collation-aware operation branches on
6. **Source sweep — [sql/core — joins in the source map](reference/spark-source-map/sweeps/sql-core-joins-exec.md)** — what a collated key costs at execution time. Every join key is wrapped by `CollationKey.injectCollationKey` in the hash operators' companion `apply` (the case-class constructors are `private` to force it), and `hashJoinSupported` removes **both** hash strategies from the ladder when a key type is not binary stable — with one `WARN` in the driver log naming the keys, and no error. A collated string key can silently cost you the broadcast join
7. **Source sweep — [sql/core — datasources in the source map](reference/spark-source-map/sweeps/sql-core-datasources.md)** — where collation reaches the storage layer: `DataSourceStrategy.collationAwareFilter` wraps a pushed filter on a non-UTF8-binary collated column in a `Collated*` variant rather than the plain one, so a source that does not understand collation cannot silently apply byte comparison to it. The practical consequence is that a collated column loses ordinary predicate pushdown unless the connector opts in
8. **Source sweep — [sql/catalyst — analysis in the source map](reference/spark-source-map/sweeps/sql-catalyst-analysis.md)** — where a collation is *applied* to a plan, which is a different question from what a collated type is. Three mechanisms: `ResolveCollationName` fully qualifies `COLLATE utf8_lcase` via `CollationFactory`; `ApplyDefaultCollation` gives default-typed strings the collation inherited from table then schema (`DEFAULT COLLATION` on the object); `CollationTypeCasts` decides the result when two collations meet. The rule-ordering hazard is worth reading in full — `ApplyDefaultCollation` and `CollationTypeCasts` can oscillate forever through `ExtractWindowExpressions`, so the former calls the latter directly via `CollationRulesRunner` instead of trusting the batch loop. And `RewriteCollationJoin` wraps each side of a join equality in `CollationKey`, recursing into structs and arrays, because join conditions are evaluated with *binary* equality

!!! warning "No book covers this"

    Collation landed in Spark 4.0, after all three books. Their advice for case-insensitive
    matching is `lower(a) = lower(b)`, which is still correct but defeats every pushdown and
    partition-pruning opportunity on that column. `a = b COLLATE UTF8_LCASE` is the replacement.

!!! warning "Non-binary collation costs an ICU key per string, per shuffle"

    Any collation other than `UTF8_BINARY` fails `supportsBinaryEquality`, so hashing a string
    column computes a collation sort key for every value — at every hash partitioning, join and
    aggregation, not once. Collation is a correctness feature with a real and recurring shuffle
    cost; apply it to the columns that need it, not to the schema.

**Milestone:** You can declare a column with `COLLATE UTF8_LCASE` and show that a join on it
matches rows differing only in case; explain why the join still produces correct results despite
being hash-based (name the expression that makes it work); state what `collation(col)` returns and
what `SHOW COLLATIONS` is for; and give one reason to keep `lower()` instead of collating a column
you join on frequently.

---


### ⬜ I22 — The VARIANT Type and Semi-Structured Data

> Discovered from source sweep (new topic): `sql/catalyst: The VARIANT type and semi-structured extraction`

**What it is:** Spark 4's binary `VARIANT` type for schema-free JSON-like data: `parse_json`, path extraction with `variant_get`, `schema_of_variant` for discovering what is in there, `variant_explode`, and the dot-notation extraction the analyzer rewrites into `variant_get`.

**Why you need it:** It replaces the store-JSON-as-a-string pattern with a binary format that keeps types and supports indexed path access, and — unlike a fixed struct schema — it tolerates fields appearing and disappearing between batches.

**Learn it with:**

1. **Spark-docs → Variant Functions** ([api/sql/variant-functions](https://spark.apache.org/docs/latest/api/sql/variant-functions/)) — the 11-function surface: `parse_json` / `try_parse_json`, `variant_get` / `try_variant_get`, `is_variant_null`, `schema_of_variant`, `schema_of_variant_agg`, `variant_explode`, `to_variant_object`
2. **Spark-docs → Data Types** ([sql-ref-datatypes.html](https://spark.apache.org/docs/latest/sql-ref-datatypes.html)) — `VariantType`, added in 4.0.0, and where it sits relative to `StructType` and `MapType`
3. **Spark-docs → JSON Functions** ([api/sql/json-functions](https://spark.apache.org/docs/latest/api/sql/json-functions/)) — the `get_json_object` / `from_json` surface variant is meant to replace; read it to see what re-parsing per access costs
4. **Source sweep — [sql/catalyst — expressions in the source map](reference/spark-source-map/sweeps/sql-catalyst-expressions.md)** — the VARIANT concept: `failOnError` is the *only* difference between `parse_json` and `try_parse_json`, the path grammar is parsed once per expression rather than per row, and the `col:field.sub` dot syntax is a `SemiStructuredExtract` node the analyzer rewrites into `variant_get`
5. **Source sweep — [sql/catalyst — types & parser in the source map](reference/spark-source-map/sweeps/sql-catalyst-types-parser.md)** — how a VARIANT column gets created on read: the `singleVariantColumn` JSON option routes the whole record through a root converter into one variant value, sharing the same `JacksonParser` that a schema-driven read uses — so the parse modes and `_corrupt_record` behave identically
6. **Source sweep — [sql/core — datasources in the source map](reference/spark-source-map/sweeps/sql-core-datasources.md)** — the two pieces of machinery that make `VARIANT` fast, both **Parquet-only**: `PushVariantIntoScan` rewrites the plan so only the paths the query reads are extracted, inside the scan rather than above it; and *shredding* writes frequently-accessed paths as real typed Parquet columns, with the schema inferred from the data and bounded by `spark.sql.variant.shredding.maxSchemaDepth` (50) and `.maxSchemaWidth` (300). On any other format both are unavailable and every access re-parses the whole value

!!! warning "No book covers this"

    `VARIANT` arrived in Spark 4.0, after all three books. Their answer for semi-structured data is
    a string column plus `from_json` with a declared schema, or `get_json_object` per access — both
    of which re-parse text on every read and neither of which tolerates a changing shape.

!!! info "Shredding is a separate, storage-side topic"

    The `spark.sql.variant.*` shredding configs control how a variant column is physically laid out
    in Parquet so that a path extraction becomes a column read rather than a scan of the blob.
    That is a datasource and optimizer concern, not an expression one — it is worth knowing the
    knobs exist (`writeShredding.enabled`, `pushVariantIntoScan`) before benchmarking variant
    against a flattened struct schema.

**Milestone:** You can ingest a JSON column as `VARIANT`, extract a nested field with both
`variant_get` and the `:` dot syntax, and show they produce the same plan; use
`schema_of_variant_agg` to discover the actual shape of a column you did not write; explain what
`try_parse_json` changes and on what input; and state one case where a declared `StructType` is
still the better choice.

---


### ⬜ I23 — Schema Inference for CSV, JSON and XML

> Discovered from source sweep (new topic): `sql/catalyst: Schema inference — one type lattice, three formats`

**What it is:** The shared algorithm behind `inferSchema`: per-value type guessing, a `compatibleType` lattice that widens conflicts toward `StringType`, a distributed fold over partitions, and the `samplingRatio` / `preferDate` / `prefersDecimal` knobs that steer it.

**Why you need it:** Inference is a full extra job over the data, its result depends on what happened to be in the sample, and understanding the widening lattice is the difference between debugging a surprise `string` column and re-running with an explicit schema.

**Learn it with:**

1. **Spark-docs → CSV Files** ([sql-data-sources-csv.html](https://spark.apache.org/docs/latest/sql-data-sources-csv.html)) — the option table is the reference: `inferSchema`, `samplingRatio`, `preferDate`, `enforceSchema`, `nullValue`, `emptyValue`, and the read/write split on several of them
2. **Spark-docs → JSON Files** ([sql-data-sources-json.html](https://spark.apache.org/docs/latest/sql-data-sources-json.html)) and **XML Files** ([sql-data-sources-xml.html](https://spark.apache.org/docs/latest/sql-data-sources-xml.html)) — the same shape of table for the other two formats; XML's `rowTag` is the one option with no default
3. **Rioux Ch 6** — reading semi-structured JSON and building a schema by hand; the book's argument for explicit schemas is the right one, and this topic is why
4. **Source sweep — [sql/catalyst — types & parser in the source map](reference/spark-source-map/sweeps/sql-catalyst-types-parser.md)** — the inference concept: the try-in-order ladder (integer → long → decimal → double → date → timestamp → boolean → **string**), `compatibleType` as a lattice whose top element is `StringType`, the distributed `mapPartitions` + `fold`, and `canonicalizeType` dropping all-null JSON fields out of the schema entirely
5. **Source sweep — [sql/core — datasources in the source map](reference/spark-source-map/sweeps/sql-core-datasources.md)** — what inference costs before it is even accurate: JSON schema inference builds a `Dataset[String]` and runs `JsonInferSchema` as a **distributed job over the input**, so on a large directory it is a full extra pass before your query starts. Partition columns are inferred separately and by a different mechanism entirely (see I27), and CSV's `multiLine` mode makes each file unsplittable, which changes read parallelism as well as parsing

!!! warning "Inference is a job, not a peek"

    `infer` is a full pass over the input before your query runs — unbounded for CSV, bounded by
    `samplingRatio` for JSON. Two things follow: the inferred schema can change between runs when
    the data changes, and you pay for the pass every time. Supplying an explicit schema removes
    both costs at once.

!!! info "A surprise `string` column means the lattice widened"

    `compatibleType` widens conflicting guesses, and `StringType` is the top element. One
    unparseable value anywhere in the scanned data turns the whole column to `string` — with no
    warning and no indication of which row caused it. For JSON, the bottom element `NullType` is
    dropped at canonicalization, so a field that was null in every sampled record does not appear
    in the schema at all.

**Milestone:** You can read the same CSV with and without `inferSchema` and show the difference in
job count in the Spark UI; explain why a column of integers with one `"N/A"` infers as `string` and
name the function responsible; predict what happens to a JSON field that is `null` in every sampled
record; and state what `samplingRatio` does and does not bound for each of the three formats.

---


### ⬜ I24 — Malformed Records: PERMISSIVE, DROPMALFORMED, FAILFAST and _corrupt_record

> Discovered from source sweep (new topic): `sql/catalyst: Malformed record handling — FailureSafeParser and the corrupt-record column`

**What it is:** The three parse modes shared by CSV, JSON and XML, the `FailureSafeParser` that implements them, and the rules governing the `_corrupt_record` column — it must be declared in the schema, must be a nullable `STRING`, and cannot be selected on its own.

**Why you need it:** PERMISSIVE is the default, so by default a malformed row becomes a row of nulls and nothing tells you; and the corrupt-record column that would tell you is subject to three separate rules that each produce a different confusing error.

**Learn it with:**

1. **Spark-docs → CSV Files** ([sql-data-sources-csv.html](https://spark.apache.org/docs/latest/sql-data-sources-csv.html)) — the `mode` and `columnNameOfCorruptRecord` options, documented per format; the same two appear on the [JSON](https://spark.apache.org/docs/latest/sql-data-sources-json.html) and [XML](https://spark.apache.org/docs/latest/sql-data-sources-xml.html) pages
2. **Spark-docs → Error Conditions** ([sql-error-conditions.html](https://spark.apache.org/docs/latest/sql-error-conditions.html)) — look up `MALFORMED_RECORDS_DETECTED_IN_RECORD_PARSING` and `INVALID_CORRUPT_RECORD_TYPE`; they are what FAILFAST and a mistyped corrupt-record column produce
3. **Rioux Ch 6** — the book reads JSON with permissive defaults throughout and never mentions the mode; read it, then come back and check what its examples would do to a malformed record
4. **Source sweep — [sql/catalyst — types & parser in the source map](reference/spark-source-map/sweeps/sql-catalyst-types-parser.md)** — the malformed-record concept: `FailureSafeParser` is 70 lines and contains the entire behaviour, including the branch where **a schema without a corrupt-record field discards the bad record**, and `ParseMode.fromString` silently falling back to PERMISSIVE on an unrecognised mode name
5. **Source sweep — [sql/core — datasources in the source map](reference/spark-source-map/sweeps/sql-core-datasources.md)** — the read-level counterpart to a malformed *record*: a malformed *file*. `spark.sql.files.ignoreCorruptFiles` sets `finished = true` on the current file's iterator, keeping the rows already read and dropping the rest, with the job reporting success — a per-file version of the same silent-truncation problem `PERMISSIVE` creates per record. `FileNotFoundException`, `AccessControlException` and `BlockMissingException` are always re-thrown regardless

!!! warning "The default loses data silently"

    PERMISSIVE emits a row of nulls for a record it could not parse. No counter, no metric, no log
    line. A pipeline with an explicit schema and no `_corrupt_record` column can null out every row
    of a file and report success. Use FAILFAST while developing, and add the corrupt-record column
    in production if you want to keep the evidence.

!!! info "Three rules govern `_corrupt_record`, with three different errors"

    It must be **declared in the schema** — otherwise the raw record is thrown away and you get
    nulls. It must be a **nullable STRING** — otherwise `INVALID_CORRUPT_RECORD_TYPE`. And it
    **cannot be the only column your query references** — the datasource refuses, because there
    would be nothing to parse against. None of these follows from the option's name.

**Milestone:** You can read a file containing one malformed record under all three modes and
describe the output of each; add `_corrupt_record` to a schema and retrieve the raw text of a bad
row; explain what happens when the column is declared as a non-nullable string, and what happens
when you `select` only that column; and say why `spark.sql.files.ignoreCorruptFiles` is a different
mechanism from `mode`.

---


### ⬜ I25 — Decimal Precision, Scale, and Silent Rounding

> Discovered from source sweep (new topic): `sql/catalyst: DecimalType and Decimal — precision, scale, and the adjustment rule`

**What it is:** How Spark derives the precision and scale of a decimal result: the 38-digit ceiling, the `adjustPrecisionScale` rule that sacrifices fractional digits to protect integral ones, and the six-digit floor it will not go below.

**Why you need it:** A chain of decimal multiplications or divisions silently loses fractional digits — or overflows to null — according to a rule nobody reads, and `spark.sql.decimalOperations.allowPrecisionLoss` picks which of the two failure modes you get.

**Learn it with:**

1. **Spark-docs → Data Types** ([sql-ref-datatypes.html](https://spark.apache.org/docs/latest/sql-ref-datatypes.html)) — `DecimalType`, its 38-digit limit, and the Java/Python type it maps to
2. **Spark-docs → ANSI Compliance** ([sql-ref-ansi-compliance.html](https://spark.apache.org/docs/latest/sql-ref-ansi-compliance.html)) — the arithmetic-overflow section: with ANSI on, a decimal that cannot be represented raises instead of returning null, which interacts directly with the precision-loss setting
3. **Spark-docs → Runtime SQL Configuration** ([configuration.html#runtime-sql-configuration](https://spark.apache.org/docs/latest/configuration.html#runtime-sql-configuration)) — `spark.sql.decimalOperations.allowPrecisionLoss`, whose one-line description does not convey what it trades
4. **Source sweep — [sql/catalyst — types & parser in the source map](reference/spark-source-map/sweeps/sql-catalyst-types-parser.md)** — the decimal concept: `adjustPrecisionScale` and its formula `adjustedScale = max(38 - intDigits, min(scale, 6))`, the `MINIMUM_ADJUSTED_SCALE = 6` floor, and `MAX_LONG_DIGITS = 18` — the boundary at which a `Decimal` stops being a `Long` and an `UnsafeRow` field stops being fixed-width

!!! warning "No book covers this"

    SDG, LS2e and Rioux all mention `DecimalType` as a type you can declare. None describes how the
    precision and scale of a *result* are derived, which is where the data loss happens. The rule
    is inherited from Hive, which took it from SQL Server, and it is documented in a source comment
    rather than in the docs.

!!! warning "You get one of two failure modes and neither is loud"

    With `allowPrecisionLoss=true` (the default), a result needing more than 38 digits keeps its
    integral digits and drops fractional ones — to a floor of 6. Multiply three `DECIMAL(20,10)`
    columns and the declared scale collapses, with no warning. Set it to `false` and the same
    expression returns **null** instead (or raises, under ANSI mode). For financial arithmetic,
    decide which one you want before the pipeline is in production rather than after a reconcile
    fails.

**Milestone:** You can compute the result type of `DECIMAL(20,10) * DECIMAL(20,10)` by hand from
the adjustment formula and confirm it with `printSchema`; show the same expression returning null
under `allowPrecisionLoss=false`; explain why `DECIMAL(18,2)` and `DECIMAL(19,2)` differ in storage
as well as range; and name the config that permits a negative scale and why it is off.

---


### ⬜ I26 — Observing Metrics Mid-Query: df.observe() and the Observation API
> Discovered from source sweep (new topic): `sql/core: Observing metrics mid-query — CollectMetricsExec and AggregatingAccumulator`

**What it is:** `df.observe(name, *aggs)` attaches named aggregate expressions to a point in the plan. The rows stream through unchanged while a real aggregate is computed on the executors and merged at task completion; the result is read afterwards from an `Observation` object or from `QueryExecution.observedMetrics`. It works on batch DataFrames and on Structured Streaming queries, where the values arrive on each `StreamingQueryProgress`.

**Why you need it:** Data-quality checks and pipeline instrumentation normally cost a second pass — `df.count()` or an `.agg()` after a write re-executes the entire plan, including the read and every shuffle. `observe` computes the same numbers during the pass you were already making. It is the difference between a row-count assertion that doubles your job cost and one that is free.

**Learn it with:**

1. **No book covers this.** Rioux, LS2e and SDG all predate widespread use of `observe`; there is no chapter to read. Work from the API docs and the sweep.
2. **Spark-docs → PySpark `Observation`** ([observation.html](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/observation.html)) — the class, its `get` property, and the one-shot lifecycle: an `Observation` can be attached to exactly one action
3. **Spark-docs → `DataFrame.observe`** ([DataFrame.observe.html](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.observe.html)) — the restriction that matters: the aggregate expressions must not contain a distinct aggregate or a window function
4. **Source sweep — [sql/core — query-execution in the source map](reference/spark-source-map/sweeps/sql-core-query-execution.md)** — why the value is trustworthy: updates go to a **task-local** copy of the accumulator and are merged only in a task-completion listener, so a failed or speculative task contributes nothing and a heartbeat never exposes a partial value. Also that `CollectMetricsExec` is not a codegen operator, so it terminates a whole-stage pipeline
5. **Source sweep — [sql/core — the classic API in the source map](reference/spark-source-map/sweeps/sql-core-classic-api.md)** — the JVM side of `df.observe`: it inserts a `CollectMetrics` node tagged with the Dataset's id, and the session's `ObservationManager` completes the observation's future from the executed plan on query completion — including the case worth knowing, where optimization removed the `CollectMetrics` node and the manager unblocks the waiter rather than hanging

**Milestone:** Take a pipeline you already have that ends in a write, and add an `observe` reporting input row count, output row count and the null count of one key column — without adding a single extra action. Confirm from the Spark UI that the number of jobs did not change. Then break it deliberately: attach the same `Observation` to two actions and explain the error you get.

---


### ⬜ I27 — Partition Column Type Inference: How a Directory Name Becomes a Typed Column

> Discovered from source sweep (new topic): `sql/core: Partition value type inference — the seven-step ladder`

**What it is:** A partitioned dataset stores its partition column values in *directory names* — `/sales/year=2024/region=EMEA/`. Those names are strings, so Spark has to decide what type each column is. `PartitioningUtils.inferPartitionColumnValue` tries parsers in a fixed order — `Integer` → `Long` → `Decimal` (whole numbers only) → `Double` → **Timestamp** → **Date** → `Time` → `String` — with `__HIVE_DEFAULT_PARTITION__` becoming `NullType`. A user-specified schema bypasses the ladder for that column, and `spark.sql.sources.partitionColumnTypeInference.enabled=false` turns every partition column into a string.

**Why you need it:** It changes your data without touching a byte of it. `id=007` comes back as the integer `7`, so a join against a `StringType` key matches nothing while the directory on disk still says `007`. `date=2024-01-01` is inferred as a **timestamp**, not a date, because timestamp is tried first — and which timestamp depends on `spark.sql.timestampType`. Neither is visible in the file contents or in an error; only in the schema.

**Learn it with:**

1. **No book covers this** — LS2e, SDG and the Rioux book all describe partition discovery as "Spark finds the columns from the directory names" and stop there. The ordering, and the fact that it is lossy, appear nowhere.
2. **Spark-docs → Partition Discovery** ([sql-data-sources-parquet.html#partition-discovery](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html#partition-discovery)) — the official description, which does document that numeric and date types are inferred and names the config, but not the order or the round-trip loss
3. **Spark-docs → Generic file options** ([sql-data-sources-generic-options.html](https://spark.apache.org/docs/latest/sql-data-sources-generic-options.html)) — `basePath` and `recursiveFileLookup`, which decide where the directory walk stops and whether it happens at all
4. **Source sweep — [sql/core — datasources in the source map](reference/spark-source-map/sweeps/sql-core-datasources.md)** — the "partition value type inference" and "partition discovery" concepts: the literal ladder, the SPARK-23436 cast-and-require guard that stops a partial parse succeeding, the `__HIVE_DEFAULT_PARTITION__` mapping, and `removeLeadingZerosFromNumberTypePartition` on the write side, which is why the round trip loses padding
5. **Related:** I5 (partitioning) for the write side, and E25 for the wider family of "Spark matched a column differently than you expected"

**Milestone:** Write a DataFrame partitioned by a zero-padded string key, read it back, and show from `printSchema()` that the column is now an integer and the padding is gone. Then fix it two ways — a user-specified schema for that column, and `spark.sql.sources.partitionColumnTypeInference.enabled=false` — and say what each costs you. Finally, create `date=2024-01-01` by hand and say what type it comes back as, and why changing `spark.sql.timestampType` changes the answer.

---


### ⬜ I28 — Driver-Side File Listing: The Cost Before Any Task Runs

> Discovered from source sweep (new topic): `sql/core: File listing — parallel discovery, the status cache, and basePath`

**What it is:** Before a single task is scheduled, the driver has to know every file it will read. `InMemoryFileIndex` does this two ways: a direct `FileSystem.listFiles` call — used only for a **single** input path whose scheme is in `spark.sql.sources.useListFilesFileSystemList` (default `s3a`) — or, otherwise, a **Spark job** that lists directories in parallel once there are more than `spark.sql.sources.parallelPartitionDiscovery.threshold` (32) of them, capped at `.parallelism` (10000) tasks. Results land in a process-wide `SharedInMemoryCache`, size-bounded by `spark.sql.hive.filesourcePartitionFileCacheSize` with an optional TTL. A catalog table can skip listing entirely by asking the metastore for its partitions instead (`CatalogFileIndex`).

**Why you need it:** On a large partitioned table this listing, not the scan, is what makes a query take minutes to start — and it is invisible in the SQL tab, because the parallel listing is a plain Spark job with no SQL node attached to it. The cache warns about eviction exactly **once per JVM**, so a driver whose planning is progressively degrading produces no new log output. And `recursiveFileLookup`, `basePath`, `pathGlobFilter` and `modifiedBefore`/`modifiedAfter` each change what gets listed at all — including the rule that recursive lookup and partition discovery are mutually exclusive.

**Learn it with:**

1. **No book covers this** — file listing is treated as an implementation detail everywhere; none of the three books mention the parallel-listing job or the status cache.
2. **Spark-docs → Generic file options** ([sql-data-sources-generic-options.html](https://spark.apache.org/docs/latest/sql-data-sources-generic-options.html)) — `pathGlobFilter`, `recursiveFileLookup`, `modifiedBefore`/`modifiedAfter`: the options that decide what is listed
3. **Spark-docs → Configuration** ([configuration.html#runtime-sql-configuration](https://spark.apache.org/docs/latest/configuration.html#runtime-sql-configuration)) — the generated table is where `spark.sql.sources.parallelPartitionDiscovery.*` and `spark.sql.sources.useListFilesFileSystemList` are documented at all
4. **Source sweep — [sql/core — datasources in the source map](reference/spark-source-map/sweeps/sql-core-datasources.md)** — the "file listing" concept: `bulkListLeafFiles`'s two-way branch and its single-path restriction, the Guava cache's weigher (`SizeEstimator / 32`) and its once-per-JVM eviction warning, `shouldFilterOutPathName` dropping `_`- and `.`-prefixed entries before anything sees them, and the `basePath` rules
5. **Related:** I27 (what happens to the directory names once they *are* listed), and I8/I11 — Delta and Iceberg exist partly because a manifest beats listing an object store

**Milestone:** On a table with a few thousand partitions, time `spark.read.parquet(path).count()` cold, find the listing job in the Spark UI (a job with no associated SQL query), and say which of the two listing strategies ran and why. Then run the same read a second time in the same session and explain the difference from the cache. Finally, add `recursiveFileLookup=true` to a partitioned path and predict the error before you see it.

---


### ⬜ I29 — Bucketed Tables: bucketBy, and the Two Rules That Undo Bucketing

> Discovered from source sweep (new topic): `sql/core: V1 bucketing at execution time — the two rules that coalesce or disable a bucketed scan`

**What it is:** A table written with bucketBy carries its hash partitioning into the scan, so a join on the bucket columns can skip the shuffle — but two physical rules rewrite that decision at planning time: one coalesces the larger side when the bucket counts differ by a divisible ratio, and one disables bucketed scanning entirely whenever nothing downstream is interested in the partitioning.

**Why you need it:** Bucketing is the only way to make a large-to-large join shuffle-free in Spark's own file formats, and it is also the feature most likely to appear to do nothing: two off-by-default configs, a divisibility requirement, an interesting-partition analysis, and a maximum bucket count all sit between `bucketBy` and a plan without an Exchange.

**Learn it with:**

1. **Spark-docs → Bucketing, Sorting and Partitioning** ([sql-data-sources-load-save-functions.html#bucketing-sorting-and-partitioning](https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html#bucketing-sorting-and-partitioning)) — the write side, and the one paragraph of official guidance there is: `bucketBy` for unbounded cardinality, `partitionBy` for low cardinality, and buckets only work on *persistent* tables (`saveAsTable`, not `save`)
2. **SDG Ch 9** — the fullest book treatment: `bucketBy(n, col)` / `sortBy`, why a bucketed write is a shuffle you pay once, and the read-side benefit. Written against Spark 2.x, so nothing below the write API has survived unchanged
3. **LS2e Ch 4** — shorter, in the data-sources chapter; useful only for the API shape
4. **Source sweep — [sql/core — aggregation, windows and exchange in the source map](reference/spark-source-map/sweeps/sql-core-agg-window-exchange.md)** — the "V1 bucketing at execution time" concept, which is where everything post-2018 lives: `CoalesceBucketsInJoin`'s divisibility rule (bucket id is `hash % numBuckets`, so only a divisor merges correctly), the `maxBucketRatio` ceiling of 4, the refusal to coalesce a shuffled-hash-join *build* side, and `DisableUnnecessaryBucketedScan`'s interesting-partition walk
5. **Related:** A25 (storage-partitioned joins) is the DSv2 generalisation of the same idea — the Spark docs describe SPJ as a generalisation of bucket joins. Read I29 first: SPJ's failure modes are easier to recognise once you have seen bucketing's

!!! warning "Two of this feature's five configs are off by default"

    `spark.sql.bucketing.coalesceBucketsInJoin.enabled` is `false` — mismatched bucket counts get a
    shuffle unless you turn it on. `spark.sql.sources.bucketing.autoBucketedScan.enabled` is `true`,
    which means Spark will *disable* your bucketed scan whenever nothing above it needs the
    partitioning. Both defaults are defensible and both surprise people.

**Milestone:** Write two tables bucketed on the same column with the same bucket count, join them, and show from `df.explain()` that the plan has no `Exchange`. Then re-write one side with a different (divisible) bucket count, confirm the exchange reappears, turn on `spark.sql.bucketing.coalesceBucketsInJoin.enabled`, and confirm it disappears again — naming which side got `optionalNumCoalescedBuckets` in the scan node. Finally, run a bare `spark.table(bucketed).count()` and explain why the plan shows a non-bucketed scan.

---


### ⬜ I30 — Python UDTFs: Table Functions That Return Many Rows

> Discovered from source sweep (new topic): `sql/core: Python UDTFs — three eval types, and a UDTF that decides its own schema`

**What it is:** A Python UDTF is a class with an eval() that yields rows and an optional analyze() that runs on the driver at query-analysis time to decide the output schema, partitioning and ordering from the actual arguments — planned as a Generate node rewritten into a dedicated BatchEvalPythonUDTF or ArrowEvalPythonUDTF operator.

**Why you need it:** It is the only PySpark construct that turns one input row into many without an explode, it takes TABLE() arguments so it can consume a whole partition, and its polymorphic analyze() is the one place user Python runs on the driver during analysis — which is also the one place a UDTF bug becomes an analysis error rather than a task failure.

**Learn it with:**

1. **Spark-docs → Python User-defined Table Functions (UDTFs)** ([tutorial/sql/python_udtf.html](https://spark.apache.org/docs/latest/api/python/tutorial/sql/python_udtf.html)) — the primary reference: the class contract (`__init__`, `eval`, `terminate`), `@udtf`, calling it from the `FROM` clause, `TABLE(...)` arguments, and the `analyze` method including the `AnalyzeResult` that gets handed back to `__init__` on every later instantiation
2. **Spark-docs → Vectorized Python UDTFs** ([tutorial/sql/arrow_python_udtf.html](https://spark.apache.org/docs/latest/api/python/tutorial/sql/arrow_python_udtf.html)) — the Arrow form; note that Arrow is **on by default for UDTFs** (`spark.sql.execution.pythonUDTF.arrow.enabled` is `true`, since 3.5.0), so this is the shape you get unless you opt out
3. **Spark-docs → Unleashing UDFs & UDTFs** ([user_guide/udfandudtf.html](https://spark.apache.org/docs/latest/api/python/user_guide/udfandudtf.html)) — places UDTFs next to scalar and pandas UDFs; the same page I3 uses, read the UDTF half here
4. **No book covers this** — UDTFs arrived in Spark 3.5, after SDG, LS2e and Rioux. There is no book treatment at all; the docs above plus the sweep are the whole literature.
5. **Source sweep — [sql/core — Python and Arrow in the source map](reference/spark-source-map/sweeps/sql-core-python-arrow.md)** — the "Python UDTFs" concept: how `ExtractPythonUDTFs` rewrites a `Generate` into one of three operators by eval type, why the execution reuses the UDF `HybridRowQueue` but repeats the buffered input row per emitted row, and how a polymorphic `analyze()` reply becomes a `PythonUDTFAnalyzeResult` carrying schema, `withSinglePartition` / `partitionBy` / `orderBy` and the selected input columns
6. **Prerequisite:** I3 (UDFs) — the planning and worker machinery is shared; learn where a UDF runs before adding arity to it

!!! info "A UDTF can demand its own distribution"

    `analyze()` may return `withSinglePartition`, `partitionBy` or `orderBy`, and Spark honours
    them by planning the required shuffle and sort. No other PySpark construct can ask the planner
    for a distribution — a pandas UDF takes whatever partitioning it is given. That is what makes
    a UDTF the right tool for "process each group as a unit, and let the function decide what a
    group is".

**Milestone:** Write a UDTF that takes a string and a delimiter and yields one row per token, call it from both the DataFrame API and a SQL `FROM` clause, and show `explain()` naming the eval operator. Then write a second UDTF with `analyze()` that derives its output schema from a `TABLE(...)` argument's columns, and demonstrate two things: that passing a table whose schema does not match raises at analysis time rather than at task time, and that requesting `partitionBy` in the `AnalyzeResult` adds an `Exchange` to the plan.

---


### ⬜ I31 — SQL Scripting Condition Handlers: EXIT, CONTINUE and SQLSTATE Matching

> Discovered from source sweep (new topic): `sql/core: Condition handlers — EXIT, CONTINUE, and how a handler is chosen`

**What it is:** The DECLARE ... HANDLER mechanism inside a SQL script: named conditions, SQLSTATE matching, the NOT FOUND and SQLEXCEPTION catch-alls, and the difference between an EXIT handler (which leaves the enclosing block) and a CONTINUE handler (which resumes after the failing statement).

**Why you need it:** It is the only error handling a pure-SQL pipeline has, and its resolution order is not obvious — a handler on a SQLSTATE can silently outrank the one you thought you wrote, an unhandled `02` condition does not fail the script at all, and CONTINUE handlers change which statement runs next.

**Learn it with:**

1. **Spark-docs → SQL Scripting, "Condition handling"** ([sql-ref-scripting.html](https://spark.apache.org/docs/latest/sql-ref-scripting.html)) — the normative statement of the three handler classes (named condition, SQLSTATE, the `SQLEXCEPTION` and `NOT FOUND` catch-alls), the "most appropriate handler" rule, and what `EXIT` versus `CONTINUE` does afterwards. Read this section first and in full; it is short and every sentence is load-bearing
2. **Spark-docs → Compound statement** ([control-flow/compound-stmt.html](https://spark.apache.org/docs/latest/control-flow/compound-stmt.html)) — the syntax and, more importantly, the *ordering* rule: conditions and handlers must be declared at the top of the block, in a fixed order, or the parse fails
3. **Spark-docs → Error Conditions** ([sql-error-conditions.html](https://spark.apache.org/docs/latest/sql-error-conditions.html)) — the catalogue you actually write handlers against. Each entry lists its SQLSTATE, which is what decides whether your `SQLEXCEPTION` or `NOT FOUND` handler fires
4. **Source sweep — [sql/core — SQL scripting in the source map](reference/spark-source-map/sweeps/sql-core-sql-scripting.md)** — the runtime: the five-step handler search in `SqlScriptingExecutionScope.findHandler`, the frame stack that makes a handler body a separate call context, and the two mechanisms that implement the aftermath (`injectLeaveStatement` for EXIT, `interruptConditionalStatements` for CONTINUE)

!!! info "No book covers this — docs and source only"
    Condition handlers arrived with SQL scripting in Spark 4.0, and `CONTINUE` handlers only in 4.1. Rioux (2022), LS2e (2020) and SDG (2018) predate all of it. Treat the docs page as primary, verify against your own 4.2.0 stack, and use the sweep for the behaviour the docs do not spell out.

!!! warning "`CONTINUE HANDLER` is off by default"
    `spark.sql.scripting.continueHandlerEnabled` defaults to **false** (internal, since 4.1.0). A `DECLARE CONTINUE HANDLER` raises `UNSUPPORTED_FEATURE.CONTINUE_EXCEPTION_HANDLER` until you turn it on. Set it before you start on this topic, or half the material is unreachable.

**Milestone:** You can write a script whose inner block declares an `EXIT HANDLER FOR DIVIDE_BY_ZERO` and whose outer block declares an `EXIT HANDLER FOR SQLEXCEPTION`, provoke each, and predict from the source which one fires and where execution resumes. Then the two that catch people: explain why a `SQLEXCEPTION` handler does **not** catch an internal (`XX`-class) error, and why a script containing a failing statement whose SQLSTATE starts `02` completes successfully with no handler at all. Finally, declare your own condition with `DECLARE ... CONDITION FOR SQLSTATE` and say which SQLSTATE values the parser refuses and why, and what a bare `DECLARE c CONDITION` defaults to.

---


### ⬜ I32 — SQL Cursors: Row-at-a-Time Iteration and Where the Snapshot Is Taken

> Discovered from source sweep (new topic): `sql/core: Cursors — DECLARE, OPEN, FETCH, CLOSE and the snapshot taken at OPEN`

**What it is:** The 4.2.0 cursor statements — DECLARE CURSOR, OPEN (with USING parameters), FETCH ... INTO variables, CLOSE — their four-state lifecycle, and the fact that OPEN starts execution and locks in the files that will be read.

**Why you need it:** A cursor is the one place in Spark where you consume a query row by row on the driver, and its semantics are surprising in both directions: the data snapshot is fixed at `OPEN` rather than at `FETCH`, and running off the end raises a condition that is silently ignored unless you declared a `NOT FOUND` handler.

**Learn it with:**

1. **Spark-docs → OPEN** ([control-flow/open-stmt.html](https://spark.apache.org/docs/latest/control-flow/open-stmt.html)), **FETCH** ([control-flow/fetch-stmt.html](https://spark.apache.org/docs/latest/control-flow/fetch-stmt.html)) and **CLOSE** ([control-flow/close-stmt.html](https://spark.apache.org/docs/latest/control-flow/close-stmt.html)) — the three statement pages; `DECLARE CURSOR` and the declaration-ordering rule are on the compound-statement page below
2. **Spark-docs → SQL Scripting, "Variable and cursor scoping"** ([sql-ref-scripting.html](https://spark.apache.org/docs/latest/sql-ref-scripting.html)) — cursors are scoped to their compound statement and are implicitly closed when it exits, including on an `EXIT` handler; also the label-qualification rule for duplicate names in nested scopes
3. **Spark-docs → Compound statement** ([control-flow/compound-stmt.html](https://spark.apache.org/docs/latest/control-flow/compound-stmt.html)) — where a cursor declaration is allowed: after variables and conditions, before handlers
4. **Source sweep — [sql/core — SQL scripting in the source map](reference/spark-source-map/sweeps/sql-core-sql-scripting.md)** — the four-state machine in `CursorState.scala`, the `executeToIterator()` call in `OpenCursorExec` that is *why* the snapshot is taken at `OPEN`, and the ANSI store-assignment casts (plus the multi-column-into-one-struct special case) that `FETCH ... INTO` applies
5. **Source sweep — [sql/core — query-execution in the source map](reference/spark-source-map/sweeps/sql-core-query-execution.md)** — the same four commands seen as DSv2 physical operators, sharing a strategy and a parameter binder with `EXECUTE IMMEDIATE`
6. **Source sweep — [sql/catalyst — analysis in the source map](reference/spark-source-map/sweeps/sql-catalyst-analysis.md)** — cursor *resolution*: `ResolveCursors` looks the name up in the script's scope chain (`findCursorInScope` when label-qualified, `findCursorByName` otherwise), carries the cursor's definition along on the `CursorReference`, rejects a name with more than two parts, and **rejects any cursor reference outside a SQL script at all**. `ResolveFetchCursor` resolves the `FETCH … INTO` targets through the same session-variable machinery and validates their count and types against the cursor's output

!!! info "No book covers this — docs and source only"
    Cursors landed in Spark 4.2.0 ([SPARK-54759]), long after Rioux (2022), LS2e (2020) and SDG (2018). There is no book treatment and there will not be one for some time. Docs plus the sweep, verified against your own stack.

!!! warning "Two internal flags, both off, and you need both"
    `spark.sql.scripting.cursorEnabled` defaults to **false** — every cursor statement raises `UNSUPPORTED_FEATURE.SQL_CURSOR` until it is on. And the idiomatic fetch loop needs `spark.sql.scripting.continueHandlerEnabled` too, because end-of-data is signalled as `CURSOR_NO_MORE_ROWS` (SQLSTATE `02000`) and the only way to observe it is a `CONTINUE HANDLER FOR NOT FOUND` ([I31](#i31-sql-scripting-condition-handlers-exit-continue-and-sqlstate-matching)). Read that topic first.

**Milestone:** You can write a script that declares a cursor over a query, opens it, loops `FETCH ... INTO` local variables until a `NOT FOUND` handler sets a done flag, and closes it. Then the parts that are not obvious: show that modifying the underlying table between `OPEN` and the last `FETCH` does not change the rows you get, and predict what a script does if you drop the `NOT FOUND` handler and fetch past the end. Finally, say which cursor errors surface during *analysis* and which only at execution — a misspelled cursor name and a `FETCH` from a cursor you never opened fail in different phases, and one of them will not fail at all if it sits in a branch that never runs.

---


### ⬜ I33 — SQL UDFs: CREATE FUNCTION … RETURN and Plan Inlining

> Discovered from source sweep (new topic): `sql/core: SQL UDFs — CREATE FUNCTION … RETURN and plan inlining`

**What it is:** `CREATE FUNCTION f(x INT) RETURNS INT RETURN x * 2` — a user-defined function whose body is SQL rather than Python or Scala. The body is stored in the catalog as **text**, parsed and analysed at creation time only to validate it, and then inlined into every calling plan during analysis: as a scalar expression, or as a relation when the function declares `RETURNS TABLE`. Two characteristics travel with it — `[NOT] DETERMINISTIC` and `CONTAINS SQL | READS SQL DATA` — and Spark derives the second from the body and rejects a declaration the body contradicts.

**Why you need it:** It is the only UDF kind the optimizer can see through. A `PythonUDF` is an opaque expression evaluated in a separate process; a `ScalaUDF` is an opaque closure; neither can be pushed down, folded or reordered. A SQL UDF's body becomes ordinary Catalyst after inlining, so predicate pushdown, constant folding and column pruning all apply *inside* it — for logic expressible in SQL this is not a small win over a Python UDF, it is a different order of magnitude. The cost moves elsewhere: plan size grows with every call site, and creation is slow for deep call chains because Spark expands the whole function graph to check for recursion.

**Learn it with:**

1. **No book covers this** — SQL UDFs landed in Spark 4.x, well after every book on the list. Rioux Ch 8–9 cover Python UDFs and pandas UDFs only, which are a different mechanism.
2. **Spark-docs → CREATE FUNCTION (SQL)** ([sql-ref-syntax-ddl-create-sql-function.html](https://spark.apache.org/docs/latest/sql-ref-syntax-ddl-create-sql-function.html)) — the syntax reference: `RETURNS type` vs `RETURNS TABLE(...)`, `DETERMINISTIC`, `CONTAINS SQL` / `READS SQL DATA`, and parameter defaults. Note that the **other** `CREATE FUNCTION` page ([sql-ref-syntax-ddl-create-function.html](https://spark.apache.org/docs/latest/sql-ref-syntax-ddl-create-function.html)) documents only the Hive/Java `AS class_name USING JAR` form — a different statement that happens to share a keyword
3. **Source sweep — [sql/core — query-execution in the source map](reference/spark-source-map/sweeps/sql-core-query-execution.md)** — `CreateSQLFunctionCommand`: the body is `checkAnalysis`-ed at `CREATE` time (so a missing column fails there, not at first call), `checkCyclicFunctionReference` transitively expands every SQL function the body calls and rejects recursion with the full path, `deriveSQLDataAccess` infers the data-access characteristic from the plan shape, and a temporary function records the temp views and temp functions it depends on
4. **Source sweep — [sql/catalyst — analysis in the source map](reference/spark-source-map/sweeps/sql-catalyst-analysis.md)** — why a SQL UDF has no per-row invocation cost: the body is **inlined into the calling plan** during analysis (`ResolveSQLFunctions` → `SQLScalarFunction`, `ResolveSQLTableFunctions` → `SQLFunctionNode`/`SQLTableFunction`), so the optimizer sees through it and pushes predicates into it. `SQLFunctionContext.nestedSQLFunctionDepth` bounds function-calls-function, and `spark.sql.analyzer.sqlFunctionResolution.applyConfOverrides` pins the creation-time configs onto the body — so a function created under one ANSI setting keeps behaving that way when called from a session with the other

**Milestone:** Write a scalar SQL UDF and an equivalent Python UDF that apply the same arithmetic to a column, run both over the same DataFrame, and compare the two `EXPLAIN` outputs — the SQL UDF's body should appear inlined in the plan with no `BatchEvalPython` node. Then put a filter on the UDF's output and confirm from the plan that it is pushed below the SQL UDF but not below the Python one. Finally, write a `RETURNS TABLE` function and a deliberately cyclic pair of functions, and record the exact error the second one raises at `CREATE FUNCTION`.

---


### ⬜ I34 — Row-Multiplying Operators: explode, LATERAL VIEW, and the Expand Behind ROLLUP

> Discovered from source sweep (new topic): `sql/core: Row-multiplying operators — GenerateExec and ExpandExec`

**What it is:** The two physical operators that turn one input row into many. `GenerateExec` runs a `Generator` — `explode`, `posexplode`, `inline`, a `LATERAL VIEW`, a table-valued function — and optionally joins each produced row back to the input row; the `OUTER` variants emit one null row instead of dropping an input whose generator produced nothing. `ExpandExec` applies **N projections** to every input row and emits N rows: it is the mechanism behind `GROUPING SETS`, `ROLLUP` (N+1 sets), `CUBE` (2^N sets), and the optimizer's rewrite of multiple `COUNT(DISTINCT …)` in one query.

**Why you need it:** Both multiply the row count *before* the aggregation or shuffle above them, and neither is obvious from the SQL you wrote. `GROUP BY ROLLUP(a, b, c)` reads the table once and shuffles four expanded copies of every row; `COUNT(DISTINCT x), COUNT(DISTINCT y)` does the same for two. An `Expand` also reports `UnknownPartitioning`, so it destroys whatever partitioning its child had and almost always forces an exchange. On the generator side, whether the multiplication happens inside a fused codegen loop or across an operator boundary depends on one thing — whether the generator implements `terminate()` — and nothing in `EXPLAIN` says which you got.

**Learn it with:**

1. **Book — spark-book [Ch 13: Complex Types](spark-book/ch13-complex-types.md)** for `explode` / `explode_outer` and the row-dropping rule, and **[Ch 09: Aggregations](spark-book/ch09-aggregations-groupby.md)** for `rollup` / `cube` / `groupingSets`. Rioux Ch 6 (JSON data) covers the explode/collect round trip from the API side. None of the three covers the operators underneath.
2. **Spark-docs → LATERAL VIEW** ([sql-ref-syntax-qry-select-lateral-view.html](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-lateral-view.html)) — the SQL surface for generators, including `OUTER`
3. **Spark-docs → GROUP BY** ([sql-ref-syntax-qry-select-groupby.html](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-groupby.html)) — `GROUPING SETS`, `ROLLUP`, `CUBE` and the grouping-set counts each one expands to
4. **Source sweep — [sql/core — query-execution in the source map](reference/spark-source-map/sweeps/sql-core-query-execution.md)** — `GenerateExec`'s `LazyIterator` for `terminate()`, why `supportCodegen` is delegated to the generator, the `CollectionGenerator` fast path that emits a plain `for` loop, and `ExpandExec`'s `UnknownPartitioning(0)` plus its one-`UnsafeProjection`-per-grouping-set execution
5. **Source sweep — [sql/catalyst — analysis in the source map](reference/spark-source-map/sweeps/sql-catalyst-analysis.md)** — how a generator gets into its own operator in the first place, which is two rules rather than one. `ExtractGenerator` pulls a generator expression out of a `Project`/`Aggregate` select list into a `Generate` node *below* it, leaving references behind (and rejects two generators in one select list); `ResolveGenerate` then names the output via `GeneratorResolution.makeGeneratorOutput`, honouring `LATERAL VIEW explode(a) t AS x, y` aliases and re-running when the generator's own children contain a generator

**Milestone:** Run a `GROUP BY ROLLUP(a, b, c)` over a table and read the `Expand` operator's `numOutputRows` in the SQL tab — confirm it is exactly 4× the scan's, and explain from the plan why the exchange sits above rather than below it. Then write the same query as an explicit `UNION ALL` of four `GROUP BY`s and compare shuffle bytes. Separately, explode an array column containing nulls and empty arrays, and show the row-count difference between `explode` and `explode_outer` on the same input.

---


### ⬜ I35 — Column DEFAULT Values: DDL, INSERT, and the Provider Allowlist

> Discovered from source sweep (new topic): `sql/catalyst: Column DEFAULT values`

**What it is:** A table column can carry a DEFAULT expression that Spark substitutes when a write omits the column or names DEFAULT explicitly, resolved during analysis rather than stored by the file format.

**Why you need it:** DEFAULT only works on an allowlisted set of table providers and is off unless enabled, so the same DDL silently succeeds on one format and fails on another — and the value you get on an omitted column depends on a second config.

**Learn it with:**

1. **Spark-docs → Name Resolution** ([sql-ref-name-resolution.html](https://spark.apache.org/docs/latest/sql-ref-name-resolution.html)) — rule 1.3, "Column DEFAULT specification": an unqualified identifier matching `default` that makes up the *entire* expression resolves to the target table's DEFAULT, but only inside `UPDATE SET`, `INSERT VALUES` or `MERGE WHEN [NOT] MATCHED`. Anywhere else it is an ordinary column name
2. **Spark-docs → ALTER TABLE** ([sql-ref-syntax-ddl-alter-table.html](https://spark.apache.org/docs/latest/sql-ref-syntax-ddl-alter-table.html)) — read it for what it *doesn't* say: at 4.2.0 the page gives `ALTER TABLE … ALTER COLUMN col alterColumnAction` and never enumerates `alterColumnAction`, so `SET DEFAULT` / `DROP DEFAULT` appear nowhere in the SQL reference. The `CREATE TABLE` datasource page omits `DEFAULT` from its column syntax too. **The DDL surface of this feature is essentially undocumented** — which is the practical reason to read the source and the DSv2 API instead
3. **Spark-docs → DSv2 API** ([`ColumnDefaultValue`](https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/connector/catalog/ColumnDefaultValue.html)) — the connector-side contract, and the one behaviour you cannot guess: it carries **both** the SQL string and a folded literal. The string is re-evaluated per write command (so a `DEFAULT current_date()` moves), while the literal is what back-fills existing rows when the column is added
4. **Source sweep — [sql/core — datasources in the source map](reference/spark-source-map/sweeps/sql-core-datasources.md)** — where the default is actually *applied* on a read: both vectorized readers fill a missing column from `ResolveDefaultColumns.existenceDefaultValues` and mark the vector constant (`ParquetColumnVector`, `OrcColumnarBatchReader`), each throwing `IllegalArgumentException` for a default whose type the vector cannot append — and **neither row-based path implements it at all**, so the behaviour depends on whether the vectorized reader was used
5. **Source sweep — [sql/catalyst — analysis in the source map](reference/spark-source-map/sweeps/sql-catalyst-analysis.md)** — the concept "Column DEFAULT values": `ResolveColumnDefaultInCommandInputQuery` is a *virtual* rule under `ResolveReferences` with strict conditions on the plan shape between the `Project`/`UnresolvedInlineTable` and the command; `TableOutputResolver` handles a column the write omits entirely; and the three configs — `spark.sql.defaultColumn.enabled` (read in the **parser**, so with it off the DDL itself fails), `...allowedProviders` (the provider allowlist), `...useNullsForMissingDefaultValues` (null vs error for an omitted column with no declared default)

!!! warning "No book covers this"

    Column defaults post-date the Rioux book entirely and the personal book has no chapter on DDL-level table features. Lean on the Name Resolution page and the sweep.

!!! info "It is not stored by the file format"

    A Parquet or ORC file has no notion of a column default. The expression lives in table metadata and Spark substitutes it during *analysis*, which is why the feature depends on the catalog and the provider rather than on the file format — and why a table written by Spark with defaults reads back without them in an engine that does not implement the same metadata.

**Milestone:** With `spark.sql.defaultColumn.enabled=true`, create a table whose column has `DEFAULT current_date()`, insert a row that omits the column and a second row that names `DEFAULT` explicitly, and show both landed the same value. Then flip `spark.sql.defaultColumn.useNullsForMissingDefaultValues` and describe what changes for a column with *no* declared default. Finally, attempt the same DDL against a provider that is not in `spark.sql.defaultColumn.allowedProviders` and quote the error class you get — and say at which phase (parse, analyze, execute) it was raised.

---


### ⬜ I36 — JDBC as a Source and a Sink: Type Mapping, Batching, and the Transaction per Partition

> Discovered from source sweep (new topic): `sql/core: JDBC record conversion, batching, and the transaction per partition`

**What it is:** Spark's JDBC connector maps SQL types to Catalyst through the dialect and the driver's ResultSetMetaData, reads in parallel by generating range predicates over a numeric column, and writes by opening one connection per partition that batches every batchsize rows and commits its own transaction.

**Why you need it:** It is the most common non-file source in real pipelines and the one with the least forgiving failure modes: a write that fails halfway leaves the already-committed partitions in the table, `numPartitions` can only ever reduce write parallelism, a requested isolation level the driver does not support is silently downgraded with a `WARN`, and truncate-vs-drop on overwrite is a dialect decision rather than a Spark one.

**Learn it with:**

1. **Spark-docs → JDBC To Other Databases** ([sql-data-sources-jdbc.html](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html)) — the option table is the whole public surface: `partitionColumn`/`lowerBound`/`upperBound`/`numPartitions` for reads, `batchsize` (1000), `isolationLevel` (**`READ_UNCOMMITTED`**, not `NONE`), `truncate`/`cascadeTruncate`, `createTableOptions`, `createTableColumnTypes`, `pushDownPredicate`/`pushDownAggregate`/`pushDownLimit`/`pushDownOffset`, `queryTimeout`, `sessionInitStatement`. Note what it says about `numPartitions` on the write side — "Spark calls `coalesce(numPartitions)`" — and what it never says at all: the **scope of the transaction**
2. **Spark-docs → DataFrameWriter** ([`jdbc`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.jdbc.html)) — for the `SaveMode` interaction, which is where `truncate` becomes meaningful
3. **Source sweep — [sql/core — datasources in the source map](reference/spark-source-map/sweeps/sql-core-datasources.md)** — three concepts: "JDBC partitioning" (the stride arithmetic, the open-ended first and last predicates, and where `NULL`s go), "JdbcDialects" (registration order, expression compilation, aggregate pushdown), and "JDBC record conversion, batching, and the transaction per partition" (`makeGetter`/`makeSetter`, the isolation-level negotiation and its `WARN`, `executeBatch` every `batchsize` rows, the best-effort rollback, and `numPartitions` as a coalesce-only cap)
4. **Source sweep — the same page, "JDBC connection providers"** — the SPI that decides *how* the connection is authenticated: `spark.sql.sources.disabledJdbcConnProviderList`, two applicable providers being an error rather than a preference order, and the global lock around any provider that mutates the JVM security context

!!! warning "No book covers this"

    Rioux never touches JDBC — the book's data sources are files and its Ch 9 is pandas UDFs. The
    write semantics below are in no book, and the single most important one is not in the official
    docs either: read the sweep and the source.

!!! warning "One transaction per partition, whatever the scaladoc claims"

    `JdbcUtils.saveTable` is documented as saving "in a single transaction" and then calls
    `foreachPartition(savePartition)`. Each task opens its own connection, negotiates its own
    isolation level, batches, and commits **its own** transaction. A write that fails on partition 7
    of 20 leaves six partitions' rows committed in the target table, and no isolation level changes
    that. If you need all-or-nothing, write to a staging table and swap it in one statement you
    issue yourself.

**Milestone:** Write a DataFrame of ~1M rows to a local Postgres in one job, then repeat it with `batchsize=10` and compare wall-clock and the row count the database reports mid-write. Then force a failure partway (a `CHECK` constraint one partition violates) and show that earlier partitions are still present afterwards — and say what `isolationLevel=SERIALIZABLE` did and did not change about that. Finally, read the same table back with `partitionColumn`/`numPartitions=4` and quote the four generated predicates from the query log, including which one catches `NULL`.

---


### ⬜ I37 — Hadoop InputFormats from PySpark: sequenceFile, Writables, and Custom Converters

> Discovered from source sweep (new topic): `core: The Hadoop InputFormat bridge and the Converter plugin point`

**What it is:** `sc.sequenceFile` / `newAPIHadoopRDD` / `saveAsHadoopFile` read and write arbitrary Hadoop InputFormats from Python, converting `Writable` keys and values through a pluggable `Converter` class on the JVM side.

**Why you need it:** It is the only route from PySpark to formats no DataFrame source covers (legacy sequence files, custom InputFormats, HBase-style connectors), and its conversion rules — including the array types it silently refuses — decide whether the data arrives usable.

**Learn it with:**

1. **Spark-docs → RDD Programming Guide, "SequenceFile and Hadoop Input/Output Formats"** ([rdd-programming-guide.html](https://spark.apache.org/docs/latest/rdd-programming-guide.html)) — the Python tab is the only official treatment: how `Writable`s are converted to base Java types and then pickled, the writable-type table, and the note that custom `Converter` classes are how you handle anything outside it
2. **Spark-docs → `pyspark.SparkContext.sequenceFile`** ([reference/api/pyspark.SparkContext.sequenceFile.html](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.SparkContext.sequenceFile.html)) — plus its `newAPIHadoopRDD` / `hadoopRDD` / `RDD.saveAsNewAPIHadoopFile` siblings; read the `keyConverter` / `valueConverter` parameters, which are class *names*, loaded reflectively on the JVM
3. **Source sweep — [core — api-bridge in the source map](reference/spark-source-map/sweeps/core-api-bridge.md)** — the mechanism: `Converter.getInstance` loads your converter class by name and **re-throws** on failure rather than falling back to the default; `WritableToJavaConverter`'s unwrap table (and why `ArrayWritable` always arrives as a Python tuple, never a typed array, because of erasure); `JavaToWritableConverter` throwing on any type it does not recognise, which is what a save of an unsupported type actually looks like

!!! note "No book covers this"

    Neither Rioux nor LS2e treats the Hadoop InputFormat bridge — both teach RDDs through `parallelize` and text files. This topic is docs-and-source only. Treat it as on-demand depth: reach for it when a source has no DataFrame reader, not as part of the main path.

**Milestone:** You can read a SequenceFile written by a non-Spark job from PySpark, say which `Writable` types convert automatically and which need a `Converter`, and write a minimal `Converter` subclass, register it by class name, and explain where in the JVM it gets loaded — including what happens if the class is missing from the executor classpath.

---


### ⬜ I38 — Getting Data Back to the Python Driver: collect, toLocalIterator, and the Serving Socket

> Discovered from source sweep (new topic): `core: Serving results to the Python driver — collect, toLocalIterator, parallelize`

**What it is:** The JVM never hands results to Python in-process — it binds an authenticated socket, serves the rows over it, and PySpark drains it; `toLocalIterator` runs one job per partition over a request/response protocol with optional prefetch.

**Why you need it:** It explains why `collect()` and `toLocalIterator()` fail in different ways at scale, what `prefetchPartitions=True` actually buys, and why a driver-side OOM on a PySpark job has two separate places to happen.

**Learn it with:**

1. **Spark-docs → `pyspark.sql.DataFrame.toLocalIterator`** ([reference/pyspark.sql/api/pyspark.sql.DataFrame.toLocalIterator.html](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.toLocalIterator.html)) — the `prefetchPartitions` parameter and the one-partition-at-a-time contract; `pyspark.RDD.toLocalIterator` has the same signature
2. **Spark-docs → Configuration, `spark.driver.maxResultSize`** ([configuration.html](https://spark.apache.org/docs/latest/configuration.html)) — the guard that turns an oversized `collect()` into an error instead of a driver OOM, and which `toLocalIterator` sidesteps per partition
3. **Rioux Ch 4** — the closest book coverage, and it is a rule rather than a mechanism: `toPandas()` is `collect()`, both move every row to the driver heap, so aggregate first. (Palantir and ONS both say the same.) No book explains *how* the rows get there, which is the whole of this topic
4. **Source sweep — [core — api-bridge in the source map](reference/spark-source-map/sweeps/core-api-bridge.md)** — the actual path: `collectAndServe` materializes the whole result in the driver JVM **before** a byte reaches Python, which then builds its own list — two full copies in two independently sized heaps; `toLocalIteratorAndServe` instead submits **one job per partition** over a request/response protocol (`1` = partition follows, `0` = exhausted, `-1` = failed), where `prefetchPartitions` overlaps exactly one job ahead and no more; and every result arrives over an authenticated local socket, not through Py4J

!!! warning "Two heaps, and only one of them is `spark.driver.memory`"

    The driver JVM holds the collected array *and* the Python driver process holds the deserialized objects. `spark.driver.memory` sizes only the first. A `collect()` that fits the JVM can still kill the Python process, and the traceback will not mention Spark.

**Milestone:** You can say what `collect()` costs on the driver in both processes, predict which of `collect()` and `toLocalIterator()` fails first on a wide RDD and why, and demonstrate — from the job list in the UI — that `toLocalIterator()` submitted one job per partition, with `prefetchPartitions=True` overlapping exactly one.

---

## Advanced

**Goal:** Write high-performance, production-grade pipelines. Understand Spark's optimiser deeply enough to fix it when it makes wrong decisions. Handle streaming workloads. Build ML pipelines.

**Estimated time to complete this level:** 44–66 hrs

**Reading order:** A1 → A2 → A3 → A4 (the optimiser and tuning run) → A5 → A6 → **A7 → A8 → A12** (streaming, in that order — A12 assumes the semantics from A7/A8) → A9 → A10 → A11. A13–A20 are source-derived depth, read on demand: A17–A19 extend the optimiser run (statistics and the CBO, runtime filtering, correlated subqueries), A13–A15 the shuffle-and-retry run, A16 stage-level scheduling, and A20 the accuracy of the statistics A2 and A4 both rely on.

---

### ⬜ A1 — Query Optimisation: Catalyst and the Physical Plan

**What it is:** Logical plan → analysed plan → optimised plan → physical plan; rule-based optimisations (constant folding, predicate pushdown, projection pruning); cost-based optimisation; `EXPLAIN` output.

**Why you need it:** Knowing what Catalyst does automatically tells you what you do NOT need to do manually — and what you need to force when it gets it wrong.

**Learn it with:**

1. **LS2e Ch 3** — Catalyst and Tungsten overview
2. **SDG Ch 4** — Structured API internals; how plans are built
3. **Rioux Ch 11** — the SQL tab of the Spark UI shows the physical plan; reading it after Ch 11's walkthrough makes both stick
4. **Spark-docs → SQL Performance Tuning** ([sql-performance-tuning.html](https://spark.apache.org/docs/latest/sql-performance-tuning.html)) — `EXPLAIN EXTENDED`, join hints, AQE config; pair with the [EXPLAIN syntax reference](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-explain.html) for what each mode prints
5. **Spark-docs → Runtime SQL Configuration** ([configuration.html#runtime-sql-configuration](https://spark.apache.org/docs/latest/configuration.html#runtime-sql-configuration)) — the generated table of every `spark.sql.*` knob with its default and the version it landed in; the optimizer alone reads ~105 of them, so this is the reference to search rather than memorise
6. **Source sweep — [sql/catalyst — analysis in the source map](reference/spark-source-map/sweeps/sql-catalyst-analysis.md)** — the **first** Catalyst phase (parse → **analyze** → optimize → plan): the fixed-point `RuleExecutor` loop and its ~60 resolution rules, how a column name becomes an `AttributeReference` (`ResolveReferences`, the hardest rule), catalog/function lookup, ANSI vs legacy type coercion, `CheckAnalysis` — the pass that produces every `AnalysisException` you see — and the new **single-pass Resolver** (the 4.0/4.1 rewrite of the analyzer, still off by default in 4.2.0)
7. **Source sweep — [sql/catalyst — optimizer in the source map](reference/spark-source-map/sweeps/sql-catalyst-optimizer.md)** — the **third** phase, and the one this topic is named after: the ~28 batches and the ~50-rule operator set run twice around `Infer Filters`, what predicate pushdown and column pruning actually do to the tree, where constant folding and constraint inference come from, why `Finish Analysis` runs correctness rules disguised as optimizations, and the `spark.sql.optimizer.excludedRules` / `planChangeLog` machinery for watching a single rule work on your own query

8. **Source sweep — [core — rdd-layer in the source map](reference/spark-source-map/sweeps/core-rdd-layer.md)** — the `PartitionEvaluator` API ([SPARK-43061]), which is how a physical operator's `doExecute` actually runs on an RDD in 3.5+: a serialized *factory* builds per-partition state on the executor rather than a closure capturing driver state
9. **Source sweep — [sql/catalyst — planner in the source map](reference/spark-source-map/sweeps/sql-catalyst-planner.md)** — the planner *framework*: `QueryPlanner.plan()` returns a lazy iterator of candidates and the caller takes the **first**, so physical planning is rule-order-driven rather than cost-driven. Also `QueryPlanningTracker`, whose `topRulesByTime` answers "which rule is slow on my query" directly
10. **Source sweep — [sql/catalyst — expressions in the source map](reference/spark-source-map/sweeps/sql-catalyst-expressions.md)** — the layer every rule above operates on: `foldable`, `deterministic`, `nullIntolerant`, `canonicalized` / `semanticEquals` are the declarative properties that gate constant folding, pushdown, constraint inference and expression reuse. `semanticEquals` is false whenever either side is non-deterministic, which is why one `rand()` removes a subtree from every reuse optimization at once. Also `With` / `CommonExpressionRef`, the expression-level CTE that rules use to avoid duplicating a subtree
11. **Source sweep — [sql/catalyst — types & parser in the source map](reference/spark-source-map/sweeps/sql-catalyst-types-parser.md)** — the phase *before* parse → analyze → optimize → plan: how text becomes the unresolved plan the analyzer receives. `AstBuilder` emits `UnresolvedRelation` / `UnresolvedAttribute` / `UnresolvedFunction` and nothing else, which is the precise boundary between a `PARSE_*` error and an analysis one
12. **Source sweep — [sql/catalyst — framework in the source map](reference/spark-source-map/sweeps/sql-catalyst-framework.md)** — the substrate all four phases run on: `TreeNode`'s immutability and structural sharing, the **two** independent pruning mechanisms (170 tree patterns; 163 rule ids against a hard cap of 192), `RuleExecutor`'s batch/fixed-point loop with its max-iterations warning and test-only idempotence check, and `spark.sql.planChangeValidation` — which names the exact rule and batch that corrupted a plan, and is the right first move before bisecting with `excludedRules`
13. **Source sweep — [sql/connect — client-server in the source map](reference/spark-source-map/sweeps/sql-connect-client-server.md)** — the *other* front end onto Catalyst: `SparkConnectPlanner` builds the same unresolved `LogicalPlan` from 63 protobuf relation types that `AstBuilder` builds from SQL text. They share no code, which is why a new logical feature lands as a catalyst change, a grammar change **and** a proto change in one release — `NearestByJoin` did exactly that in 4.2.0
14. **Source sweep — [sql/core — query-execution in the source map](reference/spark-source-map/sweeps/sql-core-query-execution.md)** — the **physical** half of the pipeline the rest of this topic's sweeps stop at: the nine lazy phases of `QueryExecution` (there are more than the famous four — `commandExecuted`, `normalized` and `withCachedData` sit between analyze and optimize), `SparkOptimizer`'s ~10 extra batches that catalyst never sees, and the fixed `preparations` chain that inserts every exchange, sort, codegen stage and reuse marker. Also why `prunePlans` doing nothing means "which operator did Spark pick" is answered by reading `SparkPlanner.strategies` top to bottom
15. **Source sweep — [sql/core — joins in the source map](reference/spark-source-map/sweeps/sql-core-joins-exec.md)** — how join choice interacts with the plan you read: `CollapseCodegenStages` forces `InputAdapter`s under both shuffled joins, so a sort-merge join always shows three `*(n)` groups while a broadcast hash join fuses into the streamed side's existing stage — the stage boundaries tell you the strategy before the operator name does. Four join codegen paths are separately config-gated (full-outer and existence SMJ, full-outer and build-side-outer SHJ)
16. **Source sweep — [sql/core — adaptive in the source map](reference/spark-source-map/sweeps/sql-core-adaptive.md)** — optimization that runs *during* execution rather than before it: the AQE loop re-runs the optimizer and the planner from scratch on each completed stage, with `LogicalQueryStage` keeping the logical plan in sync with the physical one. The part that surprises people who know Catalyst well is that a re-plan is only *adopted* if a `CostEvaluator` approves it, and the default evaluator's whole cost function is a shuffle count — see A31
17. **Source sweep — [sql/core — datasources in the source map](reference/spark-source-map/sweeps/sql-core-datasources.md)** — the physical-planning end of the scan: `FileSourceStrategy` splitting filters into four categories by where they can avoid I/O, `DataSourceStrategy.translateLeafNodeFilter` as the complete list of what can cross into a connector (every case matches a bare attribute against a literal, so **any cast or expression kills pushdown**), and the V2 side's twelve-stage `V2ScanRelationPushDown` pipeline whose fixed order — sample, filters, join, aggregate, variant, limit, then column pruning last — determines what is pushable at all
18. **Source sweep — [sql/core — Python and Arrow in the source map](reference/spark-source-map/sweeps/sql-core-python-arrow.md)** — where a Python UDF stops being an expression: `ExtractPythonUDFs` runs before physical planning and rewrites the plan, so `BatchEvalPython` / `ArrowEvalPython` nodes in `explain()` are the *result* of an optimizer rule, not a planner strategy — plus the two aggregate-specific rules that must run before it, and the internal errors it raises when UDFs of different eval types or from two children of a binary operator would land in one node
19. **Source sweep — [sql/core — streaming execution in the source map](reference/spark-source-map/sweeps/sql-core-streaming-exec.md)** — the streaming-only planning rules, which are where several "why doesn't my config apply" answers live: `IncrementalExecution.ShufflePartitionsRule` stamps the partition count read from the *checkpoint* onto every stateful operator, `StateOpIdRule` assigns operator ids by plan-traversal order (so inserting a stateful operator mid-query invalidates the checkpoint), and `ConvertLocalLimitRule` rewrites a `LocalLimit` above a stateful operator because a plain limit is not stable across batches
20. **Source sweep — [sql/core — the classic API in the source map](reference/spark-source-map/sweeps/sql-core-classic-api.md)** — the two places a plan is built outside the parser: `ColumnNodeToExpressionConverter` turning a `Column`'s engine-free tree into Catalyst `Expression`s (see **A37**), and `SessionState.executePlan`, the single entry point every `Dataset` uses to reach the engine. Also that a `Dataset` over a command has already executed it — a plan that ran before you called `explain`

**Milestone:** You can generate `EXPLAIN(true, true)` output for a query, identify which stage performs the shuffle, and verify that a filter was pushed below a join in the physical plan. From the analyze phase: name which rule turns an `UnresolvedAttribute` into a bound column, explain why a self-join needs `DeduplicateRelations` before references can resolve, and say what distinguishes an `AnalysisException` (thrown by `CheckAnalysis` before execution) from a runtime error. From the optimize phase: set `spark.sql.planChangeLog.level=INFO` with `spark.sql.planChangeLog.rules` pinned to one rule and read the before/after plan diff it prints for your own query; then exclude that rule with `spark.sql.optimizer.excludedRules` and show the difference in the optimized plan.

!!! info "\"Rule-based\" is not a metaphor — the optimizer is a list you can read"

    Catalyst's optimizer is ~28 named batches of `LogicalPlan => LogicalPlan` rules, and the list
    is source you can open. Two consequences worth internalising early: a rule can be turned off by
    name (`spark.sql.optimizer.excludedRules`), and every rule's effect on your plan can be printed
    (`spark.sql.planChangeLog.rules`). "The optimizer did something odd" is a debuggable statement,
    not a guess. The [optimizer sweep](reference/spark-source-map/sweeps/sql-catalyst-optimizer.md)
    maps the batch list; ~16 rules are on a non-excludable list because they are correctness
    rewrites (set operations, subqueries, float normalization) rather than optimizations.

!!! info "Four phases, and analysis is where your errors come from"

    Catalyst runs **parse → analyze → optimize → plan**. The [analysis sweep](reference/spark-source-map/sweeps/sql-catalyst-analysis.md) maps the *analyze* phase — the one that binds names to catalog tables and columns, resolves functions, and inserts implicit casts. It matters disproportionately for debugging: nearly every `AnalysisException` ("cannot resolve column", "TABLE_OR_VIEW_NOT_FOUND", "AMBIGUOUS_REFERENCE", type mismatches) is thrown by `CheckAnalysis` at the end of this phase, *before* any optimization or execution. When a query fails to compile, this is the phase to reason about — not the physical plan.

!!! warning "ANSI mode is on by default in Spark 4.x — analysis inserts stricter casts"

    `spark.sql.ansi.enabled` defaults to **true** across Spark 4.x, which makes the analyzer select `AnsiTypeCoercion` instead of the legacy `TypeCoercion`. Implicit lossy casts (e.g. `string`→`int`) that silently worked on the book's Spark 3.2 baseline are now refused at analysis or fail at runtime. This is the single most impactful behaviour change for queries migrating from Spark 3.x; the sweep traces where the rule set is selected (`Analyzer.typeCoercionRules`).

---

### ⬜ A2 — Adaptive Query Execution (AQE)

**What it is:** Not three optimizations but a **loop**. `AdaptiveSparkPlanExec` splits the physical plan at every exchange into query stages, materializes them one wave at a time, and after each wave re-runs the logical optimizer and the whole planner against the sizes the finished stages reported. The three famous outcomes — partition coalescing, runtime broadcast conversion, skew-join splitting — are what that loop produces, alongside five rules no summary mentions: `OptimizeSkewInRebalancePartitions` (the `REBALANCE` path), `OptimizeShuffleWithLocalRead`, `DynamicJoinSelection`, `AQEPropagateEmptyRelation` and `ValidateSparkPlan`.

**Why you need it:** AQE is on by default in Spark 3.0+ and handles cases that static planning gets wrong, so knowing what it does prevents you from adding manual hints that fight it. Knowing that it is a loop prevents the two failures that follow from thinking it is a rule set: reading `df.explain()` (the pre-AQE plan) and concluding AQE did nothing, and tuning thresholds against decisions that are being made and then discarded by a cost gate that never looks at data size — see A31, and A32 for the runtime rule that deletes whole subtrees.

**Learn it with:**

1. **LS2e Ch 12** — Spark 3.0 features; AQE is the headline item
2. **Spark-docs → AQE** ([spark.apache.org/docs/latest/sql-performance-tuning.html#adaptive-query-execution](https://spark.apache.org/docs/latest/sql-performance-tuning.html#adaptive-query-execution))
3. **ADEB Module 3** — performance optimisation module; AQE in practice on real workloads
4. **Source sweep — [sql/core — joins in the source map](reference/spark-source-map/sweeps/sql-core-joins-exec.md)** — what AQE's skew rule does to a join operator: it does not create a new operator, it rewrites the children and sets `isSkewJoin` on the existing `SortMergeJoinExec` / `ShuffledHashJoinExec` / `BroadcastHashJoinExec`. Also `canBroadcastBySize`'s runtime-stats branch, which is where `spark.sql.adaptive.autoBroadcastJoinThreshold` overrides the static threshold
5. **Source sweep — [sql/core — adaptive in the source map](reference/spark-source-map/sweeps/sql-core-adaptive.md)** — the framework itself, file by file: the `withFinalPlanUpdate` loop, the five kinds of query stage, the two rule lists (preparation vs stage-optimizer) and their ordering constraints, the cost gate that decides whether a re-plan is kept, and the seven ways `InsertAdaptiveSparkPlan` opts a query out of AQE entirely — including that a single unsupported subquery disables AQE for the whole query with only a `WARN`. It also covers the rules no summary mentions: `OptimizeSkewInRebalancePartitions`, `OptimizeShuffleWithLocalRead`, `DynamicJoinSelection`, `AQEPropagateEmptyRelation` and `ValidateSparkPlan`
6. **Source sweep — [sql/core — datasources in the source map](reference/spark-source-map/sweeps/sql-core-datasources.md)** — two places AQE and the file source meet. `FileFormatWriter` has to call `finalPhysicalPlan` on an `AdaptiveSparkPlanExec` purely to learn its output ordering when planned writes are off (SPARK-40588), and SPARK-56919 pins `setupJob` *before* that materialization so an AQE failure on an `INSERT OVERWRITE` cannot lose the table path. Also `BatchScanExec.filteredPartitions`, the V2 scan's own execution-time re-planning, which is the DSv2 analogue of AQE re-planning
7. **Source sweep — [sql/core — aggregation, windows and exchange in the source map](reference/spark-source-map/sweeps/sql-core-agg-window-exchange.md)** — two things AQE depends on that live outside the adaptive package: `ValidateRequirements`, the check that silently discards a shuffle-read rule (it logs at DEBUG and nothing else says so), and `ReusedExchangeExec`, the wrapper that makes exchange reuse — and therefore the cheap DPP path — possible at all

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
5. **Spark-docs → SQL Hints** ([sql-ref-syntax-qry-select-hints.html](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-hints.html)) — the full hint grammar, including that a join hint is *attached to a join*, not to a table — which is why a hint on the wrong relation silently does nothing
6. **Source sweep — [sql/catalyst — optimizer in the source map](reference/spark-source-map/sweeps/sql-catalyst-optimizer.md)** — the logical half of join tuning, which happens before any strategy is chosen: `ReorderJoin` and `EliminateOuterJoin`, `CostBasedJoinReorder`'s dynamic program and the four preconditions that must *all* hold before it runs, star-schema detection, and `EliminateResolvedHint` — the rule that moves your hint onto the `Join` node and, in doing so, freezes the join order around it
7. **Source sweep — [sql/catalyst — analysis in the source map](reference/spark-source-map/sweeps/sql-catalyst-analysis.md)** — `MERGE INTO` / `UPDATE` / `DELETE` are rewritten during **analysis**, not planning, and the strategy is chosen from what the connector supports: `SupportsDelta` gets a row-level delta plan, everything else rewrites whole groups (typically whole files). Identical SQL, very different cost per table format
8. **Source sweep — [sql/catalyst — planner in the source map](reference/spark-source-map/sweeps/sql-catalyst-planner.md)** — `ExtractEquiJoinKeys`, the extractor every join strategy pattern-matches against. A predicate with no references on one side is not a join key, and if nothing survives the test the join falls through to nested-loop or cartesian — which is why a `LIKE` or an inequality silently changes your join strategy
9. **Source sweep — [sql/catalyst — framework in the source map](reference/spark-source-map/sweeps/sql-catalyst-framework.md)** — the mechanism a join strategy is chosen *against*: `ShuffleSpec`'s `isCompatibleWith` / `canCreatePartitioning` decide whether one side can dictate the shuffle for both, gated by `spark.sql.requireAllClusterKeysForCoPartition` (default true) — which is why two large tables clustered on overlapping-but-not-identical keys still shuffle both sides. Also `KeyedPartitioning`, the 4.2.0 storage-partitioned-join rewrite
10. **Source sweep — [sql/core — query-execution in the source map](reference/spark-source-map/sweeps/sql-core-query-execution.md)** — the strategy list itself (`SparkPlanner.strategies`), which is the ground truth for join-strategy order, and the fact that `prunePlans` compares nothing — the first candidate wins, so strategy order is the decision
11. **Source sweep — [sql/core — joins in the source map](reference/spark-source-map/sweeps/sql-core-joins-exec.md)** — the ladder as source rather than prose. `spark.sql.join.preferSortMergeJoin` defaults to **true** and appears as `!conf.preferSortMergeJoin && …` inside `getShuffleHashJoinBuildSide`, so rung 2 — shuffled hash join — is unreachable on classic planning without a hint. And three separate code paths drop a join hint **silently** (unusable build side for the join type, strategy hint with no equi-keys, broadcast hint on a side the join type forbids), each routing to `hintErrorHandler` and then falling through to the no-hint ladder. Verify a hint by reading the operator name, never by assuming
12. **Source sweep — [sql/core — adaptive in the source map](reference/spark-source-map/sweeps/sql-core-adaptive.md)** — the two ways AQE changes a join strategy, neither of which is a join rule. `DynamicJoinSelection` injects `NO_BROADCAST_HASH` / `PREFER_SHUFFLE_HASH` / `SHUFFLE_HASH` *hints* into the logical plan based on materialized statistics — and `spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold` (default **0**, i.e. off) is the supported route to a shuffled hash join that `preferSortMergeJoin` otherwise blocks. Runtime broadcast conversion has no rule at all: it falls out of `canBroadcastBySize`'s `isRuntime` branch plus a cost evaluator that likes one fewer shuffle
13. **Source sweep — [sql/core — aggregation, windows and exchange in the source map](reference/spark-source-map/sweeps/sql-core-agg-window-exchange.md)** — `EnsureRequirements`' side of join planning: `reorderJoinKeys` permutes a join's key lists to match a child's existing partitioning, which is why a plan's key order can differ from the SQL text; and the two-child co-partitioning cost model is one line — the candidate shuffle spec with the most partitions wins

**Milestone:** You can look at a query's physical plan, identify the join strategy, force a broadcast join on a table below the auto-broadcast threshold, and handle a skewed join key with salting.

!!! note "New in Spark 4.2.0 — `NEAREST BY` and a batch of DSv2 engine work"
    `NEAREST BY` ([SPARK-56395]) adds a top-K ranking join with its own physical strategy — see B7. Data Source V2 also gained enhanced partition-stats filtering ([SPARK-55596]) and `TABLESAMPLE SYSTEM` block sampling with DSv2 pushdown ([SPARK-55978]), both of which change what the planner can prune before a join.

    Several other DSv2 improvements landed in 4.2.0 that a connector-writer or MERGE-heavy pipeline will feel (detail in the 4.2.0 release notes): **row-level operation improvements** including `MERGE INTO` whole-stage codegen ([SPARK-53652]), **schema evolution on `INSERT`** — both name-based and position-based ([SPARK-56550]), **`UPDATE`/`DELETE` operation metrics/summaries** ([SPARK-56524] UPDATE, [SPARK-56551] DELETE), **transaction-API foundations** ([SPARK-56995]), and **improvements to storage-partitioned joins** ([SPARK-56182], [SPARK-56164]) — the shuffle-free join strategy where a DSv2 source reports its partitioning so Spark can skip the exchange on both sides (directly relevant to this topic: it is a fourth way to avoid a shuffle, alongside broadcast, and it needs a connector that advertises `SupportsReportPartitioning`). Most of these are engine/connector-facing rather than new user syntax — worth knowing exist so you attribute a MERGE speedup, a newly-tolerated INSERT, or a vanished exchange to the release rather than to your own change. The user-facing CDC side of this DSv2 work (the `CHANGES` clause) is covered in E8.

---

### ⬜ A4 — Data Skew and Shuffle Optimisation

**What it is:** Why some partitions take 10× longer than others; salting keys; `SKEW HINT`; shuffle partition tuning; `spark.sql.shuffle.partitions`; spill to disk.

**Why you need it:** Data skew is the most common cause of slow Spark jobs and OOM errors in production.

**Learn it with:**

1. **ADEB Module 3** — managing skew and shuffles; the most practical treatment
2. **LS2e Ch 7** — scaling for large workloads; shuffle management
3. **SDG Ch 19** — performance tuning; shuffle configuration
4. **Spark-docs → Optimizing Skew Join** ([sql-performance-tuning.html#optimizing-skew-join](https://spark.apache.org/docs/latest/sql-performance-tuning.html#optimizing-skew-join)) — what AQE now handles for you, with the thresholds that decide when it kicks in; read alongside [Splitting skewed shuffle partitions](https://spark.apache.org/docs/latest/sql-performance-tuning.html#splitting-skewed-shuffle-partitions) before reaching for manual salting
5. **Source sweep — [core — rdd-layer in the source map](reference/spark-source-map/sweeps/core-rdd-layer.md)** — `combineByKeyWithClassTag`, the single function every key-wise aggregation bottoms out in — and the map-side-combine difference that makes `reduceByKey` cheap and `groupByKey` a skew hazard
6. **Source sweep — [core — execution engine in the source map](reference/spark-source-map/sweeps/core-execution-engine.md)** — speculation's launch criteria and its duplicate-side-effect risk, plus the fetch-failure path that skew and stragglers eventually provoke
7. **Source sweep — [core — shuffle & memory in the source map](reference/spark-source-map/sweeps/core-shuffle-memory.md)** — why skew spills (a task's ceiling is 1/N of the pool regardless of partition size), the size estimation behind 'it OOMed instead of spilling', and the three in-flight limits on the fetch side
8. **Source sweep — [core — storage & serialization in the source map](reference/spark-source-map/sweeps/core-storage-serializer.md)** — the shuffle-block staleness path, and the serializer properties that decide whether the fast shuffle write path is available at all
9. **Source sweep — [core — rpc & resources in the source map](reference/spark-source-map/sweeps/core-rpc-resources.md)** — the block-transfer retry layer that runs *below* the driver's fetch-failure handling: by the time a `FetchFailed` reaches the DAG scheduler, `spark.shuffle.io.maxRetries` attempts have already been spent silently
10. **Source sweep — [sql/catalyst — framework in the source map](reference/spark-source-map/sweeps/sql-catalyst-framework.md)** — where a shuffle comes from in the first place: `Partitioning.satisfies(Distribution)` is the whole rule, and its partition-count precondition is checked **first** and is `final`. Before tuning skew, confirm which requirement forced the exchange you are looking at — and note `HashPartitioning.partitionIdExpression` (`Pmod(CollationAwareMurmur3Hash(keys), numPartitions)`), the literal formula deciding which partition a row lands in
11. **Source sweep — [sql/core — query-execution in the source map](reference/spark-source-map/sweeps/sql-core-query-execution.md)** — the four post-shuffle partition specs in `ShuffledRowRDD` (`CoalescedPartitionSpec`, `PartialReducerPartitionSpec`, `PartialMapperPartitionSpec`, `CoalescedMapperPartitionSpec`) — precisely the shapes AQE coalescing and skew-splitting produce — plus `SortExec`'s `spillSize` metric and how it is computed by subtraction from the task's counter
12. **Source sweep — [sql/core — joins in the source map](reference/spark-source-map/sweeps/sql-core-joins-exec.md)** — the skew case AQE cannot fix, and how to tell that it fired. `SortMergeJoinExec` buffers one *key* group, so splitting a *partition* does not help a single hot key; and the only in-plan evidence skew handling ran is `(skew=true)` appended to the operator name on the **post-AQE** plan — there is no metric for it. Also why the flag has to flip `requiredChildDistribution` to `UnspecifiedDistribution`, or the preparation rules would re-insert an exchange and undo the split
13. **Source sweep — [sql/core — adaptive in the source map](reference/spark-source-map/sweeps/sql-core-adaptive.md)** — the skew machinery itself. `getSkewThreshold` is `max(skewedPartitionThresholdInBytes, median × skewedPartitionFactor)` — both published conditions folded into one `max` — and the split target is `max(advisoryPartitionSize, mean of non-skewed partitions)`. Three things that stop it firing and are invisible without `DEBUG`: a full outer join is never eligible, a `repartition` before the join disqualifies it (`shuffleOrigin` must be `ENSURE_REQUIREMENTS`), and one lost executor's missing map output makes `createSkewPartitionSpecs` return `None` for that partition. Plus the all-or-nothing revert: if any split breaks a distribution requirement the *whole query* loses skew handling unless `spark.sql.adaptive.forceOptimizeSkewedJoin` is set

**Milestone:** You can diagnose a skewed stage from the Spark UI task-time histogram, apply a salting strategy, and measure the improvement.

!!! warning "AQE's skew thresholds are applied to approximate sizes — see [A20](#a20-map-output-sizes-what-aqe-and-skew-detection-actually-see)"

    Above 2000 shuffle partitions Spark reports one *averaged* size for every block it does not classify as huge, and the classifier that would catch moderate skew is off by default (`spark.shuffle.accurateBlockSkewedFactor = -1.0`). Before concluding that `spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes` is set wrong, check whether the skew is visible in the statistics at all.

---

### ⬜ A5 — Advanced pandas UDFs and UDFs on Windows

**What it is:** Group aggregate UDF (Series→scalar) used in `.agg()` and over window specs; group map UDF (`applyInPandas`); Iterator of multiple Series; bounded vs unbounded window UDFs (Spark 3.0+).

**Why you need it:** When window functions alone can't express your logic (e.g., custom statistical models per group), pandas UDFs over windows fill the gap.

**Learn it with:**

1. **Rioux Ch 9–10** — pandas UDFs + window functions; the combination in §10.4
2. **LS2e Ch 11** — distributed ML inference using pandas UDFs
3. **Spark-docs → Apache Arrow in PySpark** ([tutorial/sql/arrow_pandas.html](https://spark.apache.org/docs/latest/api/python/tutorial/sql/arrow_pandas.html)) — Series→Scalar and the grouped-map function APIs; note 4.2.0 adds an iterator API for `GROUPED_AGG`
4. **Source sweep — [core — api-bridge in the source map](reference/spark-source-map/sweeps/core-api-bridge.md)** — the boundary a pandas UDF avoids: `SerDeUtil`'s `AutoBatchedPickler` pickles object by object, adapting its batch size from a cold start of 1 to keep each batch between 1 MB and 10 MB. That is the cost the Arrow path replaces, and the concrete reason `df.rdd.map(...)` is slow on a DataFrame that was fine in SQL
5. **Source sweep — [sql/catalyst — expressions in the source map](reference/spark-source-map/sweeps/sql-catalyst-expressions.md)** — the expression the planner extracts: `PythonUDF` is `Unevaluable`, and its `evalType` (`SQL_ARROW_BATCHED_UDF`, scalar pandas, grouped-agg, UDTF) is the single field that decides which worker protocol and which batching you get
6. **Source sweep — [sql/core — aggregation, windows and exchange in the source map](reference/spark-source-map/sweeps/sql-core-agg-window-exchange.md)** — the operator a pandas UDF over a window actually runs in: `WindowEvaluatorFactoryBase` is shared with the Arrow window evaluator, and its segment-tree metrics default to `None` precisely because the Python path does not wire them. The same page's `ScalaAggregator` / `ScalaUDAF` concept is the JVM-side counterpart to a pandas grouped-agg UDF
7. **Source sweep — [sql/core — Python and Arrow in the source map](reference/spark-source-map/sweeps/sql-core-python-arrow.md)** — the operator behind each function API: `MapInBatchExec` (one struct in, one struct out, no input buffering, optional barrier mode and `ResourceProfile`), the grouped-map family with `PandasGroupUtils`' argument dedup and its whole-group contract, cogroup's two Arrow streams over one connection, `ArrowWindowPythonExec` shipping frame bounds as extra columns, and the 4.2.0 `ArrowAggregatePythonExec`. Also the batch-sizing layer: input batches are cut on `maxRecordsPerBatch` **and** `maxBytesPerBatch` (the ARROW-4890 2 GB workaround), and the *output* batch size is a separate pair of configs that default to unset
8. **Source sweep — [sql/core — the classic API in the source map](reference/spark-source-map/sweeps/sql-core-classic-api.md)** — where a Scala UDF, an `Aggregator` and the old `UserDefinedAggregateFunction` each become an expression: the `ColumnNode` converter's inline-UDF case dispatches them to `ScalaUDF`, `ScalaAggregator`, `ScalaUDAF` or a `TypedAggregateExpression`, so a Column carrying a UDF is still an engine-free tree until conversion

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
6. **Source sweep — [sql/core — aggregation, windows and exchange in the source map](reference/spark-source-map/sweeps/sql-core-agg-window-exchange.md)** — `RowLevelOperationRuntimeGroupFiltering` — the DPP-shaped rule that lets a `MERGE` / `UPDATE` / `DELETE` skip unaffected groups by first running a cheap, few-column subquery against the original table, whenever the primary scan implements `SupportsRuntimeV2Filtering`

**Milestone:** You can implement a full SCD Type 2 merge, enable liquid clustering on a table, and explain the difference between deletion vectors and copy-on-write for point deletes.

---

### ⬜ A7 — Structured Streaming: Fundamentals

**What it is:** The micro-batch execution model; input sources (file, Kafka, socket); output sinks (Delta, memory, console, Kafka); output modes (append, update, complete); triggers; checkpointing; fault tolerance.

**Why you need it:** Near-real-time pipelines are now a core data engineering requirement. Structured Streaming integrates with the same DataFrame API you already know.

**Learn it with:**

1. **LS2e Ch 8** — best practical introduction; streaming to Delta sinks
2. **Spark-docs → Structured Streaming** ([spark.apache.org/docs/latest/streaming/index.html](https://spark.apache.org/docs/latest/streaming/index.html)) — official reference; reorganised into modular pages in Spark 4.0
3. **DEB Module 1** — Auto Loader as a streaming file source into Delta (production pattern)
4. **Source sweep — [core — api-bridge in the source map](reference/spark-source-map/sweeps/core-api-bridge.md)** — `StreamingPythonRunner`, which hands its Python worker a Spark Connect URL pointing back at the local JVM instead of streaming pickled rows. That is why a Python `foreachBatch` body receives a real DataFrame, and why its startup can fail with a timeout or a protocol error before any of your code runs
5. **Source sweep — [sql/catalyst — analysis in the source map](reference/spark-source-map/sweeps/sql-catalyst-analysis.md)** — `UnsupportedOperationChecker`, the source of nearly every "not supported in streaming" message: the batch/streaming split, the arity rules on `mapGroupsWithState`, and the global-watermark correctness check
6. **Source sweep — [sql/catalyst — framework in the source map](reference/spark-source-map/sweeps/sql-catalyst-framework.md)** — the catalyst-side markers: `isStreaming` propagates up from the leaves so one streaming source makes the whole plan streaming, and `StatefulOpClusteredDistribution` pins both the clustering **and** the partition count because state is keyed by partition id across restarts. New in 4.2.0: `SequentialStreamingUnion`, a backfill-then-live union whose children run to completion in order, and `StreamingSourceIdentifyingName`
7. **Source sweep — [sql/connect — client-server in the source map](reference/spark-source-map/sweeps/sql-connect-client-server.md)** — streaming's remote lifecycle: the server keeps a per-session registry of running queries, `foreachBatch` runs **on the server** against a server-side DataFrame with the closure shipped as an artifact, and listener events come back over a long-lived response stream. The consequence to plan for is that a session timeout terminates the queries it started
8. **Source sweep — [sql/core — adaptive in the source map](reference/spark-source-map/sweeps/sql-core-adaptive.md)** — a behaviour change to know about on 4.1+: a **stateless** streaming query now runs under AQE by default (`spark.sql.adaptive.streaming.stateless.enabled`, internal, since 4.1.0), so its post-shuffle partition count is chosen at runtime rather than being `spark.sql.shuffle.partitions`
9. **Source sweep — [sql/core — Python and Arrow in the source map](reference/spark-source-map/sweeps/sql-core-python-arrow.md)** — the two Python operators in the streaming path this topic never names: `PythonForeachWriter`, which drains a spillable row buffer into the worker from a background thread rather than blocking `process()` per row, and `PythonStreamingSourceRunner`, the numbered-function-id protocol behind a Python streaming source (see **A35**)
10. **Source sweep — [sql/core — streaming execution in the source map](reference/spark-source-map/sweeps/sql-core-streaming-exec.md)** — the engine under this topic, at source level: `StreamExecution` is one thread whose failure is *stored* in `streamDeathCause` rather than thrown, so a query that "silently stopped" has its exception waiting in `query.exception`; the trigger **is** the loop (`ProcessingTimeExecutor` never queues — an overrunning batch just starts the next one immediately); `Trigger.AvailableNow` works by wrapping the source to freeze its latest offset, which is why it respects `maxFilesPerTrigger` where `Trigger.Once` did not; and a batch with zero input rows is by design, not an anomaly. The 2026-08-09 re-sweep adds "How a source actually implements `MicroBatchStream`" — the two rate sources read side by side as the smallest complete implementations of the contract, including where a source keeps its *own* metadata log, why one of them refuses to implement `latestOffset()` at all, and what `SupportsTriggerAvailableNow` changes. See **A36** for the two-log durability protocol and **A45** for the write side
11. **Source sweep — [sql/core — the classic API in the source map](reference/spark-source-map/sweeps/sql-core-classic-api.md)** — the classic implementations of the streaming façades — `StreamingQueryManager` (active queries and listener registration), `StreamingQuery`, and the 4.x `StreamingCheckpointManager` for inspecting a query's checkpoint — plus `DataStreamReader`/`DataStreamWriter`, which are the same builders as the batch pair with `isStreaming = true`
12. **Source sweep — [connector/kafka-0-10-sql — source & sink in the source map](reference/spark-source-map/sweeps/connector-kafka-0-10-sql-source-sink.md)** — two things this topic needs from the one source you will actually attach it to. **`Trigger.AvailableNow` is implemented by prefetching the end offsets once and verifying against them every batch** — four checks, five dedicated error classes, and a second `fetchLatestOffsets` per batch as the cost. And **Real-Time Mode is, in practice, a Kafka feature**: grepping `SupportsRealTimeMode` across the checkout returns `KafkaMicroBatchStream`, the interface, three sql/core execution classes, and one test source. Its restrictions throw rather than warn (no rate limits, no `minPartitions`, no `endingTimestamp`, no `maxTriggerDelay`), and its read path has no bounds check and none of the micro-batch path's data-loss recovery
13. **Source sweep — [streaming — DStream in the source map](reference/spark-source-map/sweeps/streaming-dstream.md)** — the engine Structured Streaming replaced, worth one read for contrast. Two things this topic's model makes sense *against*. **Windowing was memory, not state**: a DStream window simply widened `rememberDuration` so parents kept their RDDs around to be re-read, so a 1-hour window on a 1-second batch retained 3,600 RDDs per DStream — no watermark, no state store, no way to bound it. And **the file source was modification-time driven**: `FileInputDStream` polls directories every batch and accepts a file only if its mtime falls inside the remember window, so a file copied in with an old mtime is skipped forever — the same failure shape as `maxFileAge`, and the reason atomic *move* is the only reliable ingest pattern in both engines

**Milestone:** You can write a streaming job that reads new Parquet files from a directory, applies a transformation, and appends results to a Delta table — and restart it from a checkpoint without data loss.

!!! note "New in Spark 4.2.0 — Real-Time Mode (millisecond latency, a different execution model)"
    Everything above is the **micro-batch** model, which floors end-to-end latency at hundreds of milliseconds because each trigger plans and launches a fresh batch. Spark 4.2.0 adds **Real-Time Mode**, a long-running continuous execution that targets **millisecond** latency — a genuinely different engine path, not a trigger option on the micro-batch model. The 4.2.0 release ships it for **stateless PySpark queries only**; stateful support, concurrent-stage scheduling, and Python-UDF support are on the roadmap, not in this release (the stateful-RTM effort is tracked as [SPARK-54699], with a new streaming shuffle [SPARK-56664], concurrent stage scheduling [SPARK-57000], and stateful operators [SPARK-57228]). Learn the micro-batch model in this topic first — it is what every book and the exam teach, and what stateful work (A8) still runs on — then read the 4.2.0 streaming docs to know when the low-latency path is worth the operational cost. No book covers it.

!!! note "New in Spark 4.2.0 — named streaming sources and sinks (stable checkpoint identity)"
    A streaming query identifies its sources and sinks by *position* in the checkpoint, so adding, removing, or reordering them broke recovery. 4.2.0 lets you give them stable names: `DataStreamReader.name()` plus an `IDENTIFIED BY` SQL syntax for sources ([SPARK-54909]), and `.name()` for sinks, backed by a V3 commit log that persists the name ([SPARK-56719]). Name your sources and sinks from day one on any query you expect to evolve — it is the difference between editing the topology and rebuilding the checkpoint from scratch. Book-absent — 4.2.0 streaming docs.

---

### ⬜ A8 — Structured Streaming: Stateful Processing

**What it is:** Event time vs processing time; watermarking for late data; tumbling, sliding, and session windows; stateful aggregations; streaming joins; `flatMapGroupsWithState` for arbitrary stateful logic.

**Why you need it:** Real streaming workloads have late-arriving events. Without watermarks, your state store grows unbounded and the job eventually OOMs.

**Learn it with:**

1. **SDG Ch 22** — event-time and stateful processing; the most rigorous treatment of watermark semantics
2. **SDG Ch 23** — streaming in production; checkpointing, restart strategies, triggers
3. **Spark-docs → Structured Streaming** ([streaming/index.html](https://spark.apache.org/docs/latest/streaming/index.html)) — the watermark and state-store sections; reorganised into modular pages in Spark 4.0, so older bookmarks land on the wrong page
4. **LS2e Ch 8** — stateful aggregations and streaming joins
5. **Source sweep — [core — execution engine in the source map](reference/spark-source-map/sweeps/core-execution-engine.md)** — why scheduler logs carry no query context by default, and the streaming-aware logging that fixes it: the query and batch id ride the `TaskSet`'s properties because the scheduler runs on different threads than the streaming execution loop
6. **Source sweep — [core — api-bridge in the source map](reference/spark-source-map/sweeps/core-api-bridge.md)** — the same `StreamingPythonRunner` that backs stateful Python operators: a Connect-backed worker rather than the pickled-row pipe, with three distinct initialization failure types
7. **Source sweep — [sql/catalyst — analysis in the source map](reference/spark-source-map/sweeps/sql-catalyst-analysis.md)** — the stateful-operator rules in `UnsupportedOperationChecker`, including the chained-stateful correctness check that is **advisory**: its message names `spark.sql.streaming.statefulOperator.checkCorrectness.enabled`, and disabling it lets a query with a known late-row-dropping hazard run rather than fixing anything
8. **Source sweep — [sql/core — adaptive in the source map](reference/spark-source-map/sweeps/sql-core-adaptive.md)** — why none of the AQE tuning in A2/A4 applies to your query: `InsertAdaptiveSparkPlan` returns the plan unchanged if it contains **any** `StatefulOperator` (SPARK-53941). A windowed aggregation, `dropDuplicates` or a stream-stream join reverts the entire query to static planning — including the stateless parts of it
9. **Source sweep — [sql/core — datasources in the source map](reference/spark-source-map/sweeps/sql-core-datasources.md)** — the supported way to look inside streaming state: the `state` DSv2 source reads a checkpoint as a table, keyed by `path` + `operatorId` + `storeName` (+ `joinSide` for stream-stream joins), with `snapshotStartBatchId`/`changeStartBatchId` for time-ranged reads. Its companion `state-metadata` source is how you discover the `operatorId` in the first place. This is a debugging tool that needs no code change to the running query
10. **Source sweep — [sql/core — aggregation, windows and exchange in the source map](reference/spark-source-map/sweeps/sql-core-agg-window-exchange.md)** — the batch-side machinery under session windows: `UpdatingSessionsExec` and `MergingSessionsExec`, the sorted-run walk that assigns each row its session spec, the optional pre-shuffle local merge (`…merge.sessions.in.local.partition`), and the error raised when the *only* grouping key is the session window. Also `BaseAggregateExec`'s `numShufflePartitions` pin — why a stateful aggregate's shuffle cannot be re-partitioned by AQE
11. **Source sweep — [sql/core — Python and Arrow in the source map](reference/spark-source-map/sweeps/sql-core-python-arrow.md)** — the two stateful-PySpark designs side by side: `applyInPandasWithState` encodes state into the Arrow stream as extra columns with a nested metadata schema, which works only when state is touched at group boundaries; `transformWithStateInPySpark` instead runs a per-task protobuf state server on its own socket, making every state operation a synchronous round trip (see **E26**). Both sit on the same state-store engine this topic covers
12. **Source sweep — [sql/core — streaming execution in the source map](reference/spark-source-map/sweeps/sql-core-streaming-exec.md)** — the state layer beneath the semantics: `WatermarkTracker` reduces every operator's watermark to **one** global value, so under the default `min` policy a single idle source holds the whole query's watermark back; `WatermarkPropagator` then computes *two* values per operator (late-events and eviction) and needs a plan simulation once stateful operators are chained; a stream-stream join keeps **four** state stores and retains state forever without a time-bounded condition; and `transformWithState` puts each state variable, its TTL index and its timers in separate column families with range-scan encodings. The 2026-08-09 re-sweep adds the mechanics under those last two: "TTL indexes" (why a TTL-enabled `ListState` needs **four** column families — RocksDB's `merge` makes element-level deletion impossible, so the secondary index becomes a work queue of lists to clean, backed by a min-expiry index and a hand-maintained count index), "Range-scan key encoding" (marker byte, big-endian value, bit-flipped negative floats — and `RangeScanBoundaryUtils`' warning that a badly built scan boundary makes `seek()` **silently skip** matching entries) and "Timestamp key encoders" (the physical layout behind the V4 join format's timestamp-ordered eviction). See **E27** for the store itself and **E47** for state schema evolution
13. **Source sweep — [connector/kafka-0-10-sql — source & sink in the source map](reference/spark-source-map/sweeps/connector-kafka-0-10-sql-source-sink.md)** — the source-side half of what a checkpoint holds. Beyond the offset log this topic already covers, the Kafka source writes its **own** `HDFSMetadataLog` under the source's metadata path, containing exactly one entry — batch 0, the resolved starting offsets — with a `v1` header and a leading zero byte kept for Spark 2.1.0 compatibility. That file is why `startingOffsets` is a one-time decision, and it is a separate thing from the query's offset log: deleting the checkpoint resets both, editing one without the other does not do what you expect
14. **Source sweep — [streaming — DStream in the source map](reference/spark-source-map/sweeps/streaming-dstream.md)** — where checkpointing came from, and why the current design looks the way it does. The DStream driver checkpoint is a **Java-serialised `DStreamGraph`** — closures included — plus the whole `SparkConf`, written by a dedicated writer with three attempts per checkpoint. Two consequences that explain Structured Streaming's choices: recompiling the application changes the closure classes, so the checkpoint usually **cannot be deserialised after a deploy** (there is no versioning and no migration; the documented answer is to delete it and lose position), which is why the offset log stores JSON; and the restored `SparkConf` is rebuilt from the serialised pairs, so config changes between runs are discarded. State has the same shape: `mapWithState` keeps an `OpenHashMapBasedStateMap` that is a **chain of deltas** over the previous batch's map, compacted at a chain length of 20, and because the state lives in RDD lineage a stateful DStream `mustCheckpoint` or fails validation before it starts

**Milestone:** You can implement a session-windowed aggregate with a watermark, explain what happens to a late event that arrives after the watermark threshold, and describe what is stored in the checkpoint directory.

!!! info "Attributing scheduler logs to a query"

    With several streaming queries on one session, driver-side scheduler messages — task launches, locality decisions, exclusions — name no query, because the query id is a thread-local on the streaming thread and the scheduler runs elsewhere. `spark.scheduler.streaming.idAwareLogging.enabled` (new in 4.2.0, and **on by default** — verified against `config/package.scala:2392`) makes `TaskSetManager` read the id from the TaskSet properties and prefix it; `…queryIdLength` truncates the id to 5 characters unless set to `-1`.

!!! note "New in Spark 4.2.0 — stream-stream join upgrades and state-store reliability"
    Two strands, both touching this topic's failure modes. **Stream-stream joins:** non-outer joins now run in `update` output mode ([SPARK-56384]), and the join state moves to format V4 ([SPARK-55628]). **State-store reliability**, aimed squarely at the corruption and slow-recovery problems that bite stateful jobs in production: automatic snapshot repair ([SPARK-54121]), a row checksum for corruption detection ([SPARK-54106]), a snapshot forced on commit when snapshot upload lags so recovery need not replay a long changelog ([SPARK-54063], enabled by default via [SPARK-55999]), and a hard error on inconsistent checkpoint metadata instead of silent misbehaviour ([SPARK-55058]). Learn watermarks and the state-store model first — these change its *reliability*, not its semantics. Book-absent — 4.2.0 streaming docs.

---

### ⬜ A9 — ML Pipelines with Spark MLlib

**What it is:** `Transformer` / `Estimator` / `Pipeline` API; feature engineering (imputers, scalers, encoders, vectorisers); `CrossValidator` and `TrainValidationSplit`; model persistence; `PipelineModel`.

**Why you need it:** MLlib's Pipeline API makes reproducible ML at scale possible — the same abstraction scikit-learn uses, but distributed.

**Learn it with:**

1. **Rioux Ch 12–14** — full treatment from feature prep through custom transformers
2. **LS2e Ch 10–11** — end-to-end pipeline example + MLflow experiment tracking
3. **IBM-ML** (Coursera, ~8 hrs) — regression, classification, clustering, and pipelines with hands-on labs
4. **Spark-docs → MLlib Guide** ([spark.apache.org/docs/latest/ml-guide.html](https://spark.apache.org/docs/latest/ml-guide.html)) — full transformer/estimator catalogue
5. **Source sweep — [sql/connect — client-server in the source map](reference/spark-source-map/sweeps/sql-connect-client-server.md)** — MLlib over the wire: a model cannot be serialized into a response, so the server holds it in a per-session `MLCache` and the client's model object is a reference id. The cache is bounded by driver heap (`maxInMemorySize`, a quarter of it by default), spills to disk rather than evicting, and is **per session** — so fitting many models in one long-lived notebook is a driver-memory question

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
5. **Source sweep — [sql/pipelines — graph in the source map](reference/spark-source-map/sweeps/sql-pipelines-graph.md)** — **the engine**, and the largest body of source behind this topic: 32 files covering how a bag of definitions becomes an ordered, resolved graph and then a run. The five things it teaches that the programming guide does not. (i) Dependencies are never declared — they are *discovered* by calling each flow function and recording which datasets it read, which is why definition order does not matter. (ii) During analysis every table is replaced by an empty DataFrame carrying only its schema, so a schema change propagates through the whole graph before any DDL runs. (iii) A **materialized view is TRUNCATEd and rewritten on every run**, not just on full refresh; only a streaming table appends. (iv) A run whose flows were all skipped or excluded still reports `COMPLETED`, so the run outcome is not a data-movement signal — read the per-flow events. (v) `pipelines.incompatibleViewCheck.enabled` is an undeclared, unprefixed conf that turns off the batch-vs-streaming view-read checks; it appears in no config listing
6. **Source sweep — [sql/pipelines — autocdc in the source map](reference/spark-source-map/sweeps/sql-pipelines-autocdc.md)** — the CDC half of the engine, and the part of Declarative Pipelines with the **least documentation**: the 4.2.0 programming guide mentions CDC nowhere, so the `create_auto_cdc_flow` docstring and this page are the only references. Three facts that change how you use the feature: `apply_as_deletes` is the only delete-detection mechanism (without it, delete events are applied as upserts); the target gains a visible `__spark_autocdc_metadata` struct column you cannot exclude and downstream datasets inherit; and each target gets a hidden `__spark_autocdc_aux_state_*` companion table whose tombstones are never expired by time. The algorithm itself is topic **E32**
7. **Source sweep — [sql/pipelines — pipeline-runtime in the source map](reference/spark-source-map/sweeps/sql-pipelines-pipeline-runtime.md)** — the state vocabulary and event model every pipeline tool reads, plus the shared utilities. Completes the subsystem: `graph` + `autocdc` + `pipeline-runtime` is all 48 files. Three things a user of the feature should know. (i) **Per-flow SQL confs are not isolated at execution time** — `withSqlConf` mutates the one shared session while up to 16 flows run concurrently, and for a batch flow the confs are restored before the write even plans; the analysis phase *is* isolated, which makes the asymmetry easy to miss. (ii) A **crashed streaming flow emits a `COMPLETED` event** as well as a `FAILED` one, because the stream listener ignores the termination exception. (iii) There are **no metrics in the pipeline event stream at all** — `onQueryProgress` is empty, so throughput and batch timings must come from the ordinary Structured Streaming surfaces. Schema behaviour from this page is topic **A39**
8. **Source sweep — [sql/connect — declarative pipelines in the source map](reference/spark-source-map/sweeps/sql-connect-declarative-pipelines.md)** — the **first source-derived material behind this topic**, covering the definition and control surface: a pipeline is a *sequence of protobuf commands* that build server-side graph state, not a plan. Three findings to carry: a dataflow graph lives on the Connect `SessionHolder` and **dies with the 60-minute idle session** (no persistence, no reattach); catalog and database defaults are frozen at `CreateDataflowGraph`, not at flow definition; and a `TABLE` is the *streaming* form while a `MATERIALIZED_VIEW` is the batch form — one boolean apart on a shared code path. Note the page's boundary: the graph engine is `sql/pipelines`, still unswept

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
5. **Source sweep — [connector/kafka-0-10 — consumer in the source map](reference/spark-source-map/sweeps/connector-kafka-0-10-consumer.md)** — the **DStream** connector, read here as contrast rather than as the thing you will use. It is worth the detour because its offset model is explicit where Structured Streaming's is hidden: the driver fixes an `OffsetRange` per partition *before* the batch runs, so exactly-once follows from the range being fixed, not from anything Kafka does — the same reasoning the checkpoint gives you, with the mechanism visible. Three transferable facts. `fixKafkaParams` **rewrites four Kafka params on executors** — `enable.auto.commit → false`, `auto.offset.reset → none`, `group.id → spark-executor-<yours>`, `receive.buffer.bytes → 65536` — which is why a broker shows twice as many consumer groups as you have streams, and the SQL connector does the same thing. Committing offsets **back to Kafka** via `CanCommitOffsets` is at-least-once and lags a batch: `commitAsync` only queues, the flush happens at the start of the next batch, and only the most recently registered callback survives. And on a **compacted** topic you must set `spark.streaming.kafka.allowNonConsecutiveOffsets`, which silently turns `count()` from arithmetic over the offset ranges into a full Spark job per batch
6. **Source sweep — [connector/kafka-0-10-sql — source & sink in the source map](reference/spark-source-map/sweeps/connector-kafka-0-10-sql-source-sink.md)** — **the connector this topic is actually about**, swept end to end: 33 files, 7,137 lines. The structural fact to lead with is that **all eight `spark.kafka.*` configs govern the executor-side pools and nothing else** — offsets, limits, partitioning, retries, group ids and failure behaviour are all *reader options*, which is why searching Spark configs for a Kafka tuning knob finds nothing. Then the operational facts. (i) **`startingOffsets` applies only to a brand-new query**: it is resolved once into batch 0 of an `HDFSMetadataLog` inside the checkpoint, so every restart reads and ignores it — moving a running query means editing the checkpoint, and the connector says so only inside the `kafka.auto.offset.reset` rejection message. (ii) **Lag comes from the source, not from Kafka**: Spark never commits offsets, so broker-side consumer-lag tooling sees nothing; `min`/`max`/`avgOffsetsBehindLatest` in `StreamingQueryProgress` are the only numbers, and they measure the backlog *after* rate limiting. (iii) **Writes are at-least-once with empty `commit`/`abort`** — a retried task re-sends what it already sent — while the *schema* is validated at plan time, so a missing `value` column fails before any record is produced. (iv) Setting `kafka.group.id` yourself puts two queries in one consumer group and each silently sees part of the data; the code appends that warning to its own "partitions are gone" message when it sees a custom group id. (v) **Kafka is the only production source implementing 4.2.0 Real-Time Mode**, and its RTM planner *throws* on `maxOffsetsPerTrigger`, `minOffsetsPerTrigger`, `minPartitions`, `endingTimestamp` and `maxTriggerDelay`. Read partitioning is topic **A41**; `failOnDataLoss` is **E41**
7. **Source sweep — [connector/kafka-0-10-token-provider — auth in the source map](reference/spark-source-map/sweeps/connector-kafka-0-10-token-provider-auth.md)** — the auth layer under both connectors, 681 lines, and the piece that makes a *secured* Kafka read different from the unsecured one this topic otherwise teaches. Every Kafka client Spark builds — DStream and Structured Streaming, driver and executor, consumer and producer — is constructed from params that passed through `KafkaConfigUpdater.setAuthenticationConfigIfNeeded`, the single place a delegation token becomes a `sasl.jaas.config` string. Two things to carry even if you never run multi-cluster: **a JVM-global JAAS configuration silently disables the whole token mechanism** (checked first, skipped at DEBUG, in both the acquisition and the injection path), and **per-cluster `kafka.*` passthrough params are applied last**, so they can replace `sasl.jaas.config` or downgrade `security.protocol` after Spark has set them. Multi-cluster proper is topic **E42**

**Milestone:** You can read a Kafka topic into Structured Streaming with an explicit `startingOffsets` and a rate limit, write to a Delta or Iceberg table, kill the job mid-stream and restart it without losing or duplicating rows — and explain precisely which component provided that guarantee. You can say what happens when the checkpoint is deleted but the sink table is not.

!!! info "Prerequisites: A7 and A8"
    Do not start here. The watermark, trigger and state-store semantics from A7/A8 are what make Kafka's offset model comprehensible; taken first, this topic degrades into copying connector options without understanding what they do.

---

### ⬜ A13 — Stage Retry: Fetch Failures, Executor Loss, and When Spark Gives Up

> Discovered from source sweep (gap): `core: fetch-failure-and-stage-retry`

**What it is:** a `FetchFailed` means a reduce task could not read a map output — the executor that produced it died, its shuffle files are gone, or the node went away. There are **two halves** to it. On the *reduce side*, the fetcher throttles, retries and detects corruption, and only escalates when it gives up. On the *driver side*, that escalation unregisters the lost output, re-runs the producing map stage, and aborts the job once the retry budget is spent. How *much* output is thrown away depends on whether an external shuffle service is running and whether the loss was a graceful decommission.

**Why you need it:** this is the most common production Spark failure you will ever debug, and every default that governs it is non-obvious. `spark.stage.maxConsecutiveAttempts` is 4 and resets on stage success; `spark.stage.maxAttempts` is unbounded and never resets; `spark.stage.ignoreDecommissionFetchFailure` is true but depends on `maxRetainedRemovedDecommissionExecutors`, which is 0; `spark.files.fetchFailure.unRegisterOutputOnHost` is false, so a dead host loses its outputs one fetch failure at a time. Reading `FetchFailed … Resubmitting stage N` in a driver log without this model is guesswork.

**Learn it with:**

1. **SDG Ch 15** — the job/stage/task execution model this failure path operates on; it does not cover retry, but you need the vocabulary first
2. **Spark-docs → Configuration, Scheduling** ([configuration.html#scheduling](https://spark.apache.org/docs/latest/configuration.html#scheduling)) — the `spark.stage.*`, `spark.task.maxFailures` and `spark.excludeOnFailure.*` keys with their defaults
3. **Spark-docs → Job Scheduling** ([job-scheduling.html](https://spark.apache.org/docs/latest/job-scheduling.html)) — dynamic allocation and executor loss, the context in which fetch failures normally arise
4. **Source sweep — [core — execution engine in the source map](reference/spark-source-map/sweeps/core-execution-engine.md)** — the layered `FetchFailed` handler: the staleness check, the decommission exemption, the two retry ceilings, and the executor-vs-host unregistration decision
5. **Source sweep — [core — shuffle & memory in the source map](reference/spark-source-map/sweeps/core-shuffle-memory.md)** — the other half: the three in-flight limits that throttle fetching, the single-retry corruption budget, and the Netty-OOM circuit breaker that halts fetching cluster-wide
6. **Source sweep — [sql/core — adaptive in the source map](reference/spark-source-map/sweeps/sql-core-adaptive.md)** — the failure path AQE adds on top of this one. A stage failure does not propagate directly: it is recorded on the `QueryStageExec`, drained from the event queue, and then every *other* running exchange stage is cancelled before one exception is thrown. Several concurrent failures become a single `MULTI_FAILURES_IN_STAGE_MATERIALIZATION` with the rest attached via `addSuppressed` — which is why the same root cause can produce two very different-looking stack traces
7. **Source sweep — [sql/core — datasources in the source map](reference/spark-source-map/sweeps/sql-core-datasources.md)** — the read-side failure switches that interact with retry: `ignoreMissingFiles` and `ignoreCorruptFiles` are evaluated per file inside `FileScanRDD`, and the corrupt-file path **succeeds with partial data** rather than failing the task — so a transient storage fault becomes a wrong answer instead of a retry. `FileNotFoundException` is still thrown under `ignoreCorruptFiles`, and `AccessControlException`/`BlockMissingException` always are
8. **Source sweep — [resource-managers/kubernetes — driver & executor in the source map](reference/spark-source-map/sweeps/resource-managers-kubernetes-driver-executor.md)** — the Kubernetes answer to "executor died, must I recompute its shuffle?". `KubernetesLocalDiskShuffleDataIO` lets a new executor **adopt a dead executor's PVC**: it disables local-file deletion on stop, then scans the reused volume for orphaned shuffle blocks and re-registers them with its own `BlockManager` — an alternative to the external shuffle service where there is no shuffle service to run. Two cautions: verification **passes when the checksum file is absent** (only the missing/empty/corrupt cases fail), and the executor-side enablement check reads `reusePersistentVolumeClaim` with a literal `false` default against a `ConfigEntry` default of `true`, so set it explicitly. Also relevant to the failure budget: on K8s a pod *deleted* (node drain, eviction) is `exitCausedByApp = false` and does not count toward `spark.executor.maxNumFailures`, while a pod *failed* does
9. **Source sweep — [resource-managers/yarn — AM & executor allocation in the source map](reference/spark-source-map/sweeps/resource-managers-yarn-am-executor.md)** — the YARN answer to "why did this executor die, and does it count?". `YarnAllocator.processCompletedContainers` is a single match on the container exit status that produces the `exitCausedByApp` flag the whole retry budget depends on: `PREEMPTED` is explicitly not your fault (SPARK-8167), `KILLED_EXCEEDED_PMEM`/`VMEM` are, and their messages name the exact configs to raise (`spark.executor.memoryOverhead`, `yarn.nodemanager.vmem-pmem-ratio`, `yarn.nodemanager.vmem-check-enabled`). Since 4.0 (SPARK-46920) Spark's own exit codes are also decoded via `ExecutorExitCode.explainExitCode`, because they overlap YARN's and YARN's diagnostics can therefore be **actively misleading**. The other half is the loss-reason RPC: `YarnDriverEndpoint` overrides `onDisconnected` to *ask the AM why* before removing an executor — on a preempted container that is what stops the running tasks from counting toward job failure — and falls back to `ExecutorProcessLost` if the AM does not answer in time

!!! warning "No book covers the retry state machine"

    SDG (2018), LS2e (2020) and Rioux (2022) all describe the happy path — job to stages to tasks — and stop. The failure machinery is source-and-docs territory, which is unfortunate given it is what you actually debug at 2am.

!!! info "Two independent ceilings, one of which never resets"

    The effective limit is the max of `spark.stage.maxConsecutiveAttempts` (4, cleared whenever the stage succeeds) and `spark.stage.maxAttempts` (unbounded, never cleared). A long-running job that reuses a stage will not accumulate unrelated failures toward the first, which is why the "consecutive" wording matters.

!!! info "The reduce side tries hard before it gives up"

    A `FetchFailed` reaching the driver is the *end* of a sequence, not the start. Before it,
    three independent limits throttle fetching — bytes in flight, requests in flight, and blocks
    in flight per remote address — and `spark.reducer.maxSizeInFlight` is a target rather than a
    cap, since a single oversized request is let through when nothing else is in flight. A block
    that fails to decompress is re-fetched **exactly once**; the second failure is what throws.
    A local corrupt block is not retried at all.

!!! warning "A Netty OOM halts fetching for the whole JVM, and looks like slowness"

    An `OutOfDirectMemoryError` during fetch is not a failure but a cross-task circuit breaker:
    a shared flag stops all new shuffle fetch requests until memory recovers or in-flight requests
    drain. It is logged once per iterator at INFO, with no metric. A cluster spending most of its
    fetch time parked behind this flag shows tasks that are simply slow, with no error and almost
    no log volume — so rule it out before concluding the network or the data is at fault.

**Milestone:** You can read a driver log containing `FetchFailed` followed by `Resubmitting stage`, say which executor's output was unregistered and whether the whole host was affected, predict how many more attempts the stage gets, explain why enabling the external shuffle service changes what an executor loss costs you, and distinguish a genuine fetch failure from a job merely parked behind the Netty-OOM flag.

---

### ⬜ A14 — Determinism, Indeterminate Stages, and Correctness Under Retry

> Discovered from source sweep (gap): `core: indeterminate-stages-and-rollback`

**What it is:** if a shuffle map stage produces *different data* when re-run — `repartition` on unordered input, `zipWithIndex`, a non-deterministic UDF — then any downstream stage that already consumed the old output is now inconsistent. Spark's defence is to roll back and re-run every succeeding stage, or abort the job when it cannot. Spark 4.2.0 adds a second, runtime detection mechanism: a checksum comparison when a `MapStatus` is re-registered for a partition that already had one.

**Why you need it:** the alternative to the abort is **silently wrong data**. The trigger is an unrelated retry, so a pipeline can run correctly for a year and then abort with a message telling you to checkpoint before `repartition`. And the new runtime detection means jobs that previously produced quiet corruption will start failing loudly after a 4.2.0 upgrade — you need to recognise the failure as a pre-existing correctness bug being surfaced, not a regression.

**Learn it with:**

1. **Spark-docs → RDD Programming Guide, Shuffle operations** ([rdd-programming-guide.html#shuffle-operations](https://spark.apache.org/docs/latest/rdd-programming-guide.html#shuffle-operations)) — why shuffle output ordering is not guaranteed, which is the root of indeterminacy
2. **Spark-docs → RDD Programming Guide, RDD Persistence** ([rdd-programming-guide.html#rdd-persistence](https://spark.apache.org/docs/latest/rdd-programming-guide.html#rdd-persistence)) — checkpointing, the prescribed fix
3. **Source sweep — [core — execution engine in the source map](reference/spark-source-map/sweeps/core-execution-engine.md)** — the static and runtime detection paths, `maxAttemptIdToIgnore`, and the query-level rollback that can abort your job because of a different, already-finished job
4. **Source sweep — [core — storage & serialization in the source map](reference/spark-source-map/sweeps/core-storage-serializer.md)** — the storage-side half of correctness under retry: cache visibility tracking holds a block written by a still-running task invisible until the driver learns the task succeeded, and a `TODO` acknowledges that an indeterminate RDD can produce different replicas under one `BlockId`
5. **Source sweep — [sql/catalyst — expressions in the source map](reference/spark-source-map/sweeps/sql-catalyst-expressions.md)** — the expression-level source of indeterminacy: `Nondeterministic` requires an explicit `initialize(partitionIndex)` before `eval`, and `monotonically_increasing_id()` encodes the partition index in bits 33–63 (`partitionMask = partitionIndex << 33`), so every value changes when the partition count does — a repartition, a different cluster size, or an AQE coalesce
6. **Source sweep — [sql/core — aggregation, windows and exchange in the source map](reference/spark-source-map/sweeps/sql-core-agg-window-exchange.md)** — the SQL half of retry correctness: the local **binary** sort that `spark.sql.execution.sortBeforeRepartition` forces ahead of a round-robin shuffle (and the `isOrderSensitive` flag it sets instead when you turn that off), plus where the order-independent row checksums are allocated — including 4.2.0's `enableQueryLevelRollbackOnMismatch`, which is off by default
7. **Source sweep — [sql/core — streaming execution in the source map](reference/spark-source-map/sweeps/sql-core-streaming-exec.md)** — the streaming counterpart to batch determinism: state checkpoint IDs. Each committed state-store version carries a unique id and each changelog file records its lineage back to a snapshot, and the driver compares what every task reports against the expected base id — raising `stateStoreBaseCheckpointIdMismatch` rather than accepting state written by a task attempt whose output was discarded
8. **Source sweep — [sql/core — the classic API in the source map](reference/spark-source-map/sweeps/sql-core-classic-api.md)** — the checkpoint API on the DataFrame side: one private method implements `checkpoint` and `localCheckpoint`, parameterised by `reliableCheckpoint` and `eager`, and it **copies every row** before checkpointing because Spark's rows are mutable and reused. The distinction to hold: a lost reliable checkpoint recomputes, a lost local checkpoint cannot — the lineage it replaced is gone

!!! warning "Docs coverage is thin, book coverage is nil"

    No book in the resources table covers determinism under retry, and the official docs describe shuffle ordering without connecting it to rollback. This topic is largely source-derived — read the sweep and the abort messages themselves.

!!! warning "The blast radius can exceed the failing job"

    `rollbackSucceedingStagesForQuery` widens rollback to every job sharing a SQL execution id, including completed ones. If a completed job in the same query had a `ResultStage`, the situation is unrecoverable and Spark aborts with a "re-run the query to ensure data correctness" message.

**Milestone:** You can name three operations that make a stage indeterminate, explain why the problem only manifests after a fetch failure, say what `checkpoint()` before `repartition` actually fixes, and predict what a 4.2.0 upgrade will do to a pipeline that has been silently producing inconsistent output on retries.

---

### ⬜ A15 — Push-Based Shuffle

> Discovered from source sweep (gap): `core: push-based-shuffle`

**What it is:** a second shuffle write path. Instead of every reducer fetching one small block from every mapper, map tasks *push* their output to remote merger services, which concatenate blocks per reduce partition so a reducer reads a few large merged chunks. It adds a driver-side finalization protocol, thirteen configs, and a reduce-side fallback that silently reverts to ordinary blocks whenever anything goes wrong.

**Why you need it:** it is the standard answer to the small-block problem on large clusters — the case where a 10,000 × 10,000 shuffle produces 100 million tiny fetches — and it is also the highest-config-density, lowest-observability feature in the shuffle subsystem. `spark.shuffle.push.enabled=true` on a non-YARN cluster is accepted and does nothing. Merger negotiation can disable it per stage with no log line at any level. And turning it on forfeits checksum-based corruption diagnosis entirely.

**Learn it with:**

1. **Spark-docs → Configuration, Shuffle Behavior** ([configuration.html#shuffle-behavior](https://spark.apache.org/docs/latest/configuration.html#shuffle-behavior)) — the `spark.shuffle.push.*` family and the external shuffle service settings it depends on
2. **Spark-docs → Job Scheduling** ([job-scheduling.html](https://spark.apache.org/docs/latest/job-scheduling.html)) — the external shuffle service and dynamic allocation, both prerequisites for the merger side
3. **Source sweep — [core — shuffle & memory in the source map](reference/spark-source-map/sweeps/core-shuffle-memory.md)** — the four enablement preconditions, the merger-threshold negotiation that returns an empty list without logging, the pusher's batching and skip rules, and the three reduce-side fallback triggers
4. **Source sweep — [resource-managers/yarn — AM & executor allocation in the source map](reference/spark-source-map/sweeps/resource-managers-yarn-am-executor.md)** — the concrete reason it is YARN-only: `YarnSchedulerBackend.getShufflePushMergerLocations` is the **sole override** of that method in the whole codebase, so on any other cluster manager the base implementation returns nothing and the negotiation can never succeed. It is also where the merger arithmetic lives — `numMergersDesired = min(max(1, ceil(numPartitions / tasksPerExecutor)), maxExecutors)`, with `maxExecutors` taken from `spark.dynamicAllocation.maxExecutors` or `spark.executor.instances` — and the threshold comparison whose failure path returns an empty `Seq` with a DEBUG line only on the *success* branch. Excluded nodes are filtered out before the request, so an excluded host is never proposed as a merger. SPARK-33481 is still open in a `TODO`: the merger count is acknowledged in-source as a naive heuristic

!!! warning "No book covers this"

    Push-based shuffle landed in Spark 3.2, after every book in the resources table. SDG (2018) and LS2e (2020) describe the sort-shuffle write path only. Docs and source only.

!!! warning "It is off unless four separate conditions all hold"

    YARN as the master (checked by string equality), the external shuffle service enabled, IO encryption off, and a relocatable serializer. Spark logs one warning naming all four without saying which failed. Then, per stage, if fewer merger locations come back than `max(mergersMinStaticThreshold, desired × mergersMinThresholdRatio)`, push is disabled for that stage **silently** — no log at any level.

!!! info "Push failures are non-fatal by design"

    An unpushed or unmerged block is simply fetched from the mapper as usual, so every degradation here costs efficiency rather than correctness — which is exactly why none of it is loud. `corruptMergedBlockChunks` is the only metric that moves, and it is not in the UI's standard shuffle metrics.

**Milestone:** You can state the four conditions under which push-based shuffle actually activates, explain why enabling it on a Kubernetes cluster does nothing, predict what happens to a stage when two of its merger nodes are excluded, and say what you lose in corruption diagnosis by turning it on.

---

### 🎯 Advanced Checkpoint

You are ready to leave this level when you can:

- Debug a slow job using the Spark UI and fix the bottleneck — reading the post-AQE plan, not the one `explain()` printed
- Build a streaming pipeline from Kafka with watermarks and a table-format sink, and say which component gives you the delivery guarantee
- Implement `MERGE INTO` with SCD Type 2 logic
- Diagnose a skewed join and know what AQE will and will not fix for you
- Build and evaluate an ML pipeline with cross-validation

*Optional:* the Databricks Data Engineer Associate exam maps to roughly I8–A6 plus orchestration, if you are working on that platform.

---








### ⬜ A16 — Stage-Level Scheduling and Accelerator-Aware Resources (GPU/FPGA)

> *Discovered from the [core — rpc & resources source sweep](reference/spark-source-map/sweeps/core-rpc-resources.md) (2026-07-22): the whole `resource/` package — `ResourceProfile`, `ResourceProfileManager`, `ResourceUtils`, `ResourceAllocator` — backs no existing topic. Placed at Advanced (not Expert) because it is a production scheduling capability built on the DAGScheduler and dynamic allocation the A-track already teaches, not a low-level runtime internal.*

**What it is:** Attaching a custom `ResourceProfile` to an RDD (`rdd.withResources(...)`, built with `ResourceProfileBuilder`) so a *stage* requests different CPUs, memory, or accelerators (GPU/FPGA) than the application default — the canonical case being a CPU-only ETL stage followed by a GPU ML/inference stage in one job, without holding idle GPUs for the whole run. Underneath: how executor and task requests combine into a profile, how Spark **discovers** accelerator addresses (an explicit resources file, or a discovery script/plugin), how it counts how many tasks fit an executor (the *limiting resource* arithmetic), and how fractional task amounts let several tasks share one GPU.

**Why you need it:** GPU inference/ML stages and mixed CPU/GPU pipelines are a real production pattern, and the mechanics have sharp edges no other topic covers — profile-merge conflicts (throw vs max-merge), fractional-GPU sharing (`0.5` ⇒ two tasks per address), discovery-script failures, and the cluster-manager gate (only YARN/K8s/Standalone, and only with dynamic allocation for full profiles).

**Learn it with:**

1. **No book covers this** — stage-level scheduling (Spark 3.1) and accelerator-aware scheduling (Spark 3.0) postdate SDG and LS2e; treat the docs and the source as the primary sources.
2. **Spark-docs → Configuration → "Custom Resource Scheduling and Configuration Overview" + "Stage Level Scheduling Overview"** ([spark.apache.org/docs/latest/configuration.html](https://spark.apache.org/docs/latest/configuration.html)) — the `spark.{driver,executor,task}.resource.{name}.{amount,discoveryScript,vendor}` configs and the `RDD.withResources` / `ResourceProfileBuilder` API contract
3. **Spark-docs → Job Scheduling** ([spark.apache.org/docs/latest/job-scheduling.html](https://spark.apache.org/docs/latest/job-scheduling.html)) — where stage-level scheduling sits relative to dynamic allocation, which it depends on
4. **Source sweep — [core — rpc & resources in the source map](reference/spark-source-map/sweeps/core-rpc-resources.md)** — the four classes that implement it end to end: `ResourceProfileBuilder`/`ResourceProfile` (author + validate), `ResourceProfileManager` (cluster-manager gating, dedup, merge conflicts), `ResourceUtils` (resourcesFile vs discovery-script/plugin), `ResourceAllocator` (fixed-point address assignment) — plus the `calculateTasksAndLimitingResource` fit arithmetic and every edge/failure path
5. **Source sweep — [sql/core — Python and Arrow in the source map](reference/spark-source-map/sweeps/sql-core-python-arrow.md)** — the one place stage-level scheduling reaches Python: `MapInBatchExec` attaches an optional `ResourceProfile` with `rdd.withResources` and can run its child RDD under `.barrier()`, so `mapInPandas(..., barrier=True)` with a GPU profile is the supported shape for a distributed training loop inside PySpark
6. **Source sweep — [resource-managers/yarn — AM & executor allocation in the source map](reference/spark-source-map/sweeps/resource-managers-yarn-am-executor.md)** — how a ResourceProfile becomes an actual container request on YARN, which is the cluster manager most people run this on. YARN forbids different container sizes within one priority, so **Spark uses the ResourceProfile id as the YARN priority** — that is the whole multi-profile mechanism, and it is why the allocator keys every map by profile id. `ResourceRequestHelper` maps Spark's abstract `gpu`/`fpga` names to YARN's `yarn.io/gpu` / `yarn.io/fpga` (remappable via `spark.yarn.resourceGpuDeviceName` / `…FpgaDeviceName`) and rejects 22 spellings of memory and cores under `spark.yarn.*.resource.*` at submit time. Two sharp edges: `spark.yarn.executor.resource.*` applies **only to the default profile** — a custom profile propagates everything it declares instead, deliberately, because there would be no way to remove them — and an unknown resource type is a **warning logged at most twice per JVM**, after which containers are allocated without it and the failure resurfaces much later as a discovery script that finds no devices

**Milestone:** You can build a `ResourceProfile` that requests 1 GPU per executor and a fractional (`0.5`) GPU per task, attach it to a stage with `rdd.withResources`, and predict from `spark.executor.cores` / `spark.task.cpus` / the per-resource amounts how many tasks that executor will run and which resource is *limiting*; explain why the feature needs dynamic allocation and which cluster managers support it; and describe what `spark.scheduler.resource.profileMergeConflicts` changes when two profiles collide on one stage.

---


### ⬜ A17 — Table and Column Statistics and the Cost-Based Optimizer

> *Discovered from the [sql/catalyst — optimizer source sweep](reference/spark-source-map/sweeps/sql-catalyst-optimizer.md) (2026-07-25): the whole `statsEstimation/` package plus `CostBasedJoinReorder` back no existing topic. A1 names "cost-based optimisation" in one clause and never returns to it.*

**What it is:** How Spark estimates the size and row count of every node in a logical plan, and what the cost-based optimizer does with those estimates. Two estimators exist and only one config chooses between them: with `spark.sql.cbo.enabled` **false** (the default) every plan is estimated by `sizeInBytes` alone; with it true, per-operator estimators use column statistics — distinct counts, min/max, null counts, optional equi-height histograms — to produce real row counts. Where those statistics come from (`ANALYZE TABLE … COMPUTE STATISTICS FOR COLUMNS`, data-source metadata, or AQE's runtime numbers), how to inspect them (`DESCRIBE EXTENDED`, `EXPLAIN COST`), and how the CBO consumes them in `CostBasedJoinReorder`'s dynamic program.

**Why you need it:** Every cost-based decision downstream — join reordering, broadcast eligibility, runtime-filter thresholds — is only as good as the statistics behind it, and the failure mode is silence. Without `ANALYZE TABLE` the estimators fall back node by node to multiplying file sizes, so a filter that removes 99% of rows is invisible to the planner and turning the CBO on changes nothing.

**Learn it with:**

1. **No book in this path covers the CBO's statistics model.** LS2e Ch 7 and SDG Ch 19 cover tuning but stop at broadcast thresholds and caching; the estimation model postdates both. Read SDG Ch 4 / LS2e Ch 3 first only for where the optimizer sits in Catalyst.
2. **Spark-docs → SQL Performance Tuning → "Leveraging Statistics"** ([sql-performance-tuning.html#leveraging-statistics](https://spark.apache.org/docs/latest/sql-performance-tuning.html#leveraging-statistics)) — the three sources of statistics (data source, catalog, runtime) and the three ways to inspect them; the shortest correct summary of this topic anywhere
3. **Spark-docs → `ANALYZE TABLE`** ([sql-ref-syntax-aux-analyze-table.html](https://spark.apache.org/docs/latest/sql-ref-syntax-aux-analyze-table.html)) — the syntax that actually populates catalog statistics, including `FOR COLUMNS` and `FOR ALL COLUMNS`
4. **Spark-docs → `EXPLAIN`** ([sql-ref-syntax-qry-explain.html](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-explain.html)) — `EXPLAIN COST` / `df.explain("cost")` prints the estimate attached to each plan node, which is how you tell a real row count from a fallback
5. **Spark-docs → Runtime SQL Configuration** ([configuration.html#runtime-sql-configuration](https://spark.apache.org/docs/latest/configuration.html#runtime-sql-configuration)) — the `spark.sql.cbo.*` and `spark.sql.statistics.*` families in one generated table
6. **Source sweep — [sql/catalyst — optimizer in the source map](reference/spark-source-map/sweeps/sql-catalyst-optimizer.md)** — the "Statistics" and "Cost-based join reorder" concepts: the `cboEnabled` fork in `LogicalPlanStats.stats`, `BasicStatsPlanVisitor`'s per-operator dispatch and its silent `.getOrElse(fallback)`, `FilterEstimation`'s selectivity walk (and its 1/3 fallback for predicates it cannot reason about), and the four independent preconditions a join chain must satisfy before it is reordered at all
7. **Source sweep — [sql/core — query-execution in the source map](reference/spark-source-map/sweeps/sql-core-query-execution.md)** — where `ANALYZE TABLE` is implemented: `CommandUtils.calculateTotalSize` lists files (in parallel above a threshold) and **excludes staging and hidden files**, which is why the stored size can disagree with `du`; histograms are opt-in via `spark.sql.statistics.histogram.enabled`
8. **Source sweep — [sql/core — joins in the source map](reference/spark-source-map/sweeps/sql-core-joins-exec.md)** — what the join planner actually reads: all three build-side tests (`canBroadcastBySize`, `canBuildLocalHashMapBySize`, `muchSmaller`) compare `plan.stats.sizeInBytes`, and the source admits "we does not have the statistic for number of rows". A compressed Parquet relation is judged by its **on-disk** size, which is the mechanism behind the classic broadcast OOM — the plan qualified, the relation did not
9. **Source sweep — [sql/core — adaptive in the source map](reference/spark-source-map/sweeps/sql-core-adaptive.md)** — the other kind of statistic — the runtime one. `QueryStageExec.computeStats()` returns a `Statistics` with `isRuntime = true`, and `canBroadcastBySize` branches on exactly that flag to use `spark.sql.adaptive.autoBroadcastJoinThreshold` instead of the static threshold. Worth knowing what runtime statistics are *not*: there are no runtime column statistics, only `bytesByPartitionId` and a row count, so the CBO's histograms and distinct counts never get a runtime refresh

**Milestone:** You can run `ANALYZE TABLE t COMPUTE STATISTICS FOR ALL COLUMNS`, confirm with `DESCRIBE EXTENDED` that column stats landed, and show with `EXPLAIN COST` that the same query's estimated row count changes when `spark.sql.cbo.enabled` is flipped. You can state which estimator runs for a given config, name the four conditions that must *all* hold before `CostBasedJoinReorder` reorders a join chain, and explain why adding a `BROADCAST` hint disables cost-based reordering for the whole chain.

!!! warning "Enabling the CBO without statistics is a no-op, and nothing says so"

    `spark.sql.cbo.enabled` and `spark.sql.cbo.joinReorder.enabled` both default to `false`. Setting
    them true on tables that were never analysed changes almost nothing: each estimator returns
    `None` and falls back to the size-only visitor node by node, and `CostBasedJoinReorder` refuses
    to run at all because it requires a defined `rowCount` on *every* item in the join chain. There
    is no warning — the plan simply comes out the same.

---

### ⬜ A18 — Runtime Filtering: Dynamic Partition Pruning and Bloom Filters

> *Discovered from the [sql/catalyst — optimizer source sweep](reference/spark-source-map/sweeps/sql-catalyst-optimizer.md) (2026-07-25): `InjectRuntimeFilter` and the `PartitionPruning` batch back no existing topic — A2 is AQE and A4 is skew, and neither of these rules is either.*

**What it is:** Two optimizer rules that plant a filter on the *large* side of a join, computed at runtime from the *small* side. **Dynamic partition pruning** (Spark 3.0) inserts a `DynamicPruningSubquery` on a partition column so the fact-table scan lists only the partitions the dimension side actually produces — the star-schema case. **Runtime bloom filters** (Spark 3.3, on by default) handle the non-partitioned case: a bloom filter built from the small side's join keys is pushed as a `Filter` onto the large side's scan. Both are planned in `SparkOptimizer`, both are governed by size thresholds, and both check whether the other has already fired on the same key.

**Why you need it:** These rules are the difference between scanning a whole fact table and scanning the slice that survives the dimension filter — often an order of magnitude. They are also the clearest case in the optimizer of a feature that *silently does nothing*: miss any one precondition (creation side over 10 MB, application side under 10 GB, no shuffle below the join, a non-trivial join-key expression, more than 10 filters already injected) and the rule returns the plan untouched with no diagnostic. Knowing the preconditions is the only way to tell "it didn't help" from "it never ran".

**Learn it with:**

1. **LS2e Ch 12** — the Spark 3.0 chapter; dynamic partition pruning is one of its headline items. Bloom-filter runtime filtering (3.3) postdates the book entirely — treat the docs and the source as primary for that half.
2. **Spark-docs → SQL Performance Tuning** ([sql-performance-tuning.html](https://spark.apache.org/docs/latest/sql-performance-tuning.html)) — read the join-strategy and AQE sections for the surrounding machinery; note that runtime filtering has **no dedicated docs page**, which is itself worth knowing
3. **Spark-docs → Runtime SQL Configuration** ([configuration.html#runtime-sql-configuration](https://spark.apache.org/docs/latest/configuration.html#runtime-sql-configuration)) — the `spark.sql.optimizer.dynamicPartitionPruning.*` and `spark.sql.optimizer.runtime.bloomFilter.*` families with their defaults; the closest thing to authoritative documentation these rules have
4. **Source sweep — [sql/catalyst — optimizer in the source map](reference/spark-source-map/sweeps/sql-catalyst-optimizer.md)** — the "Runtime filtering" concept: every precondition in `tryInjectRuntimeFilter`, the `hasDynamicPruningSubquery` / `hasBloomFilter` guards that stop the two rules stacking, the `reuseBroadcastOnly` vs `fallbackFilterRatio` decision in DPP, and the cleanup batch that strips pruning filters which could not reach a scan
5. **Source sweep — [sql/catalyst — expressions in the source map](reference/spark-source-map/sweeps/sql-catalyst-expressions.md)** — the two runtime-filter expressions themselves: `BloomFilterMightContain` type-checks that its filter side is a **literal or a scalar subquery** (it must be computable before the probe runs), and `DynamicPruningSubquery.onlyInBroadcast` is the flag deciding whether the pruning filter is worth a separate subquery execution or may only free-ride on an existing broadcast
6. **Source sweep — [sql/hive — hive-metastore in the source map](reference/spark-source-map/sweeps/sql-hive-hive-metastore.md)** — partition pruning against a Hive metastore, which is a different mechanism from the runtime filters this topic covers: with `spark.sql.hive.metastorePartitionPruning` the predicate is sent to the metastore; without it (or with a predicate the metastore cannot express) Spark fetches **every** partition's metadata to the driver and filters locally. Three separate fallback configs exist because metastore-side pruning is fragile
7. **Source sweep — [sql/core — query-execution in the source map](reference/spark-source-map/sweeps/sql-core-query-execution.md)** — the physical side of DPP: `SubqueryBroadcastExec` / `SubqueryAdaptiveBroadcastExec` reusing a join's broadcast as the pruning-filter source, and the `PartitionPruning` and `InjectRuntimeFilter` batches — which live in `SparkOptimizer`, not catalyst's `Optimizer`
8. **Source sweep — [sql/core — adaptive in the source map](reference/spark-source-map/sweeps/sql-core-adaptive.md)** — DPP under AQE, which has three outcomes and only one is free: reuse the join's broadcast (`SubqueryBroadcastExec`), plan a **second** adaptive query running an `Aggregate` over the build side (an extra scan), or — when `onlyInBroadcast` is set and no reuse is found — replace the filter with `Literal.TrueLiteral` and prune nothing at all. Which one you got is visible in the post-AQE plan as `SubqueryBroadcast`, `Subquery`, or neither
9. **Source sweep — [sql/core — datasources in the source map](reference/spark-source-map/sweeps/sql-core-datasources.md)** — the two ends of runtime filtering in the datasource layer: on V1, `PruneFileSourcePartitions` pushes partition filters into the `FileIndex` before planning so the size estimate matches the pruned scan; on V2, `BatchScanExec.filteredPartitions` re-plans partitions at execution time via `pushRuntimeFilters` — but **only if the source implements `SupportsRuntimeV2Filtering`**. Without it, DPP builds a filter the connector never receives, and the only evidence is the `RuntimeFilters: [...]` line in the scan node
10. **Source sweep — [sql/core — aggregation, windows and exchange in the source map](reference/spark-source-map/sweeps/sql-core-agg-window-exchange.md)** — the whole DPP rule chain in one place: `PartitionPruning`'s benefit model (a distinct-count ratio, falling back to 0.5 whenever stats are missing *or* look wrong), the reuse-overrides-benefit shortcut, `PlanDynamicPruningFilters`' three outcomes — reuse the broadcast, duplicate the subquery as a second scan, or silently replace the filter with `true` — and `CleanupDynamicPruningFilters`

**Milestone:** You can read an `EXPLAIN` plan and point at the `DynamicPruningSubquery` or `BloomFilterMightContain` node that proves a runtime filter was planted; explain why DPP requires a *partitioned* table while the bloom filter does not; and, given a join where neither fired, name which threshold or precondition blocked it.

---

### ⬜ A19 — Correlated Subqueries and Decorrelation

> *Discovered from the [sql/catalyst — optimizer source sweep](reference/spark-source-map/sweeps/sql-catalyst-optimizer.md) (2026-07-25): four optimizer rules plus the 1117-line `DecorrelateInnerQuery` and sixteen configs back no existing topic. B8 (Spark SQL) teaches the syntax and stops there.*

**What it is:** Spark has no physical operator for a correlated subquery — every one is rewritten into a join before planning. `EXISTS` / `NOT EXISTS` become `LEFT SEMI` / `LEFT ANTI` joins; `IN` / `NOT IN` the same with an added key equality; a correlated scalar subquery becomes a `LEFT OUTER` join. Non-equality correlation (`WHERE outer.c > inner.a`) cannot be turned into a group-by key, so decorrelation introduces a **`DomainJoin`** — the distinct set of outer values joined into the subquery. And because a left outer join produces `NULL` where a `COUNT` must produce `0`, the rewrite carries explicit **COUNT-bug** compensation, with legacy flags that restore the old wrong answer.

**Why you need it:** Correlated subqueries are the SQL feature most likely to fail at analysis with an unsupported-correlation error, to plan into an accidental cartesian product, or — under a legacy flag — to return a *wrong answer* rather than an error. The rewrite explains all three, and it is the only way to predict what a subquery will cost, since the subquery you wrote is not the plan that runs.

**Learn it with:**

1. **No book in this path covers decorrelation.** SDG Ch 8 and Rioux Ch 7 teach subquery *syntax*; none of them describe the rewrite, `DomainJoin`, or the COUNT bug. Treat the SQL reference and the source as primary.
2. **Spark-docs → Subqueries** ([sql-ref-syntax-qry-select-subqueries.html](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-subqueries.html)) — the supported forms and, importantly, the documented restrictions on where a correlated subquery may appear
3. **Spark-docs → LATERAL Subquery** ([sql-ref-syntax-qry-select-lateral-subquery.html](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-lateral-subquery.html)) — `LATERAL` is the explicit form of what decorrelation does implicitly, and reading it makes the rewrite obvious
4. **Spark-docs → Runtime SQL Configuration** ([configuration.html#runtime-sql-configuration](https://spark.apache.org/docs/latest/configuration.html#runtime-sql-configuration)) — the `spark.sql.optimizer.decorrelate*` family, including the three `legacy…IncorrectCountHandling` flags
5. **Source sweep — [sql/catalyst — optimizer in the source map](reference/spark-source-map/sweeps/sql-catalyst-optimizer.md)** — the "Correlated subqueries" concept: `PullupCorrelatedPredicates` → `DecorrelateInnerQuery` → `RewritePredicateSubquery` / `RewriteCorrelatedScalarSubquery`, where `DomainJoin` is introduced and why, and the `mayHaveCountBug` detection that decides whether compensation is inserted
6. **Source sweep — [sql/catalyst — expressions in the source map](reference/spark-source-map/sweeps/sql-catalyst-expressions.md)** — the expression shape underneath the decorrelation rules: `SubqueryExpression` carries `plan`, `outerAttrs`, `joinCond` and a hint, `isCorrelated` is simply `outerAttrs.nonEmpty`, and all of them are `Unevaluable` — so a subquery that survives to execution is a bug in the rewrite, not a slow path
7. **Source sweep — [sql/core — query-execution in the source map](reference/spark-source-map/sweeps/sql-core-query-execution.md)** — how an uncorrelated subquery actually runs: `PlanSubqueries` gives it its own full preparation chain, `SubqueryExec.doPrepare` launches it on a separate `subquery` thread pool before the main job, and `awaitResult(…, Duration.Inf)` means a hung subquery hangs the driver with no timeout
8. **Source sweep — [sql/core — joins in the source map](reference/spark-source-map/sweeps/sql-core-joins-exec.md)** — the two join types you see in plans but cannot write: `ExistenceJoin`, which carries an extra boolean column and appears when `EXISTS`/`IN` sits inside a disjunction — its `sql` method **throws**, because no SQL text expresses it — and `LeftSingle` (4.0+), the scalar-subquery join that can never be a sort-merge join because SMJ cannot enforce the at-most-one-row rule
9. **Source sweep — [sql/core — adaptive in the source map](reference/spark-source-map/sweeps/sql-core-adaptive.md)** — how subqueries are executed under AQE: each is compiled recursively into its own `AdaptiveSparkPlanExec` **before** the main plan is wrapped, sharing one `stageCache` so an exchange inside a subquery can be reused by the outer query. The failure mode is blunt — one subquery AQE cannot handle throws `SubqueryAdaptiveNotSupportedException`, which is caught at the top and abandons AQE for the entire query

**Milestone:** You can run `EXPLAIN` on an `EXISTS` subquery and a correlated scalar subquery and name the join type each became; explain what `DomainJoin` compensates for and why an equality-correlated subquery does not need one; and demonstrate the COUNT bug by flipping `spark.sql.optimizer.decorrelateSubqueryLegacyIncorrectCountHandling.enabled` and showing the result change from `0` to `NULL`.

!!! warning "Three legacy flags in this family restore a known-wrong result"

    `spark.sql.optimizer.decorrelateSubqueryLegacyIncorrectCountHandling.enabled`,
    `…decorrelateExistsSubqueryLegacyIncorrectCountHandling.enabled` and
    `…decorrelateSubqueryPreventConstantHoldingForCountBug.enabled` exist only for compatibility
    with plans captured before Spark 3.5/4.0. Their defaults give the SQL-standard answer. Setting
    the first two to `true` makes a correlated `COUNT` subquery return `NULL` instead of `0` for
    non-matching outer rows — silent incorrectness, not a performance trade.

---

### ⬜ A20 — Map Output Sizes: What AQE and Skew Detection Actually See

> Discovered from source sweep (new topic): `core: map-status-representation-and-size-accuracy`

**What it is:** the accuracy of the statistics AQE runs on. Every map task reports its per-reducer output sizes as a `MapStatus`, and those sizes are lossy by construction. Each is compressed to **a single byte** as a log base 1.1 of the size — good to about 10% up to ~35 GB, and saturating above that. Then, above `spark.shuffle.minNumPartitionsToHighlyCompress` (2000) partitions, Spark switches representation entirely: `HighlyCompressedMapStatus` keeps a bitmap of empty blocks, byte-compressed sizes only for blocks it judges "huge", and **one shared average** for every other non-empty block.

**Why you need it:** every runtime decision that reasons about partition size reads these numbers, not real ones — AQE's skew-join split, partition coalescing, reduce-side locality preference, and the fetch-to-memory-vs-disk threshold. Above 2000 partitions the reported size of an ordinary block is literally an average across all of them, and the path that would keep skewed blocks accurate is **off by default** (`spark.shuffle.accurateBlockSkewedFactor = -1.0`), leaving only a flat 100 MB cutoff. Tuning `spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes` against averaged inputs is the standard way to conclude that AQE "does not detect" a skew it structurally cannot see.

**Learn it with:**

1. **Spark-docs → Performance Tuning** ([sql-performance-tuning.html](https://spark.apache.org/docs/latest/sql-performance-tuning.html)) — the AQE section: skew-join splitting and partition coalescing, i.e. the consumers of these statistics. Read it knowing the inputs are approximations
2. **Spark-docs → Configuration, Shuffle Behavior** ([configuration.html#shuffle-behavior](https://spark.apache.org/docs/latest/configuration.html#shuffle-behavior)) — the `spark.shuffle.accurateBlockThreshold` / `accurateBlockSkewedFactor` / `maxAccurateSkewedBlockNumber` family, which is the only place these knobs are documented at all
3. **Source sweep — [core — shuffle & memory in the source map](reference/spark-source-map/sweeps/core-shuffle-memory.md)** — the representation switch on partition count alone, the log-1.1 byte encoding and its 35 GB saturation, the skew-threshold formula, and `getSizeForBlock` returning `avgSize` for everything not classified huge
4. **Source sweep — [sql/core — adaptive in the source map](reference/spark-source-map/sweeps/sql-core-adaptive.md)** — the consumer side of those approximate sizes. `bytesByPartitionId` is the *entire* runtime input to coalescing, skew detection and broadcast conversion — and skew splitting additionally reads per-**map** sizes via `MapOutputTrackerMaster.shuffleStatuses`, which returns `-1` for any missing map output and makes the whole split for that partition silently return `None`

!!! warning "No book covers this"

    `HighlyCompressedMapStatus` is an internal class; SDG, LS2e and Rioux all discuss AQE and skew as user-facing features and none mentions that the statistics behind them are lossy. The accuracy configs are one line each in the configuration table with no explanation of what they trade.

!!! warning "The skew-accuracy path is opt-in"

    With `spark.shuffle.accurateBlockSkewedFactor` at its default of `-1.0`, Spark takes the branch that disables skew-relative accuracy and falls back to the flat `spark.shuffle.accurateBlockThreshold` (100 MB). On a 2001-partition shuffle a 90 MB block among 1 MB peers — a 90× skew — is reported as the average. Set the factor positive (5 is the usual starting point) before concluding AQE is not working.

**Milestone:** You can state what changes about reported block sizes when a shuffle crosses 2000 partitions, explain why two blocks of very different size can report identical sizes to the driver, name the config that makes moderate skew visible and say why it is not on by default, and describe how to tell whether a skew AQE missed was invisible in the statistics rather than below the threshold.

---


### ⬜ A21 — Subexpression Elimination and Common Expression Reuse

> Discovered from source sweep (new topic): `sql/catalyst: Subexpression elimination — the same expression, evaluated once`

**What it is:** The mechanism that detects semantically identical subtrees in a projection or filter and evaluates each one once per row instead of once per occurrence — plus the `With` expression, which lets a rule declare reuse explicitly.

**Why you need it:** It is on by default, it silently does nothing for whole classes of expression (lambdas, conditionals, non-deterministic subtrees), and when it does not fire the cost is a full re-evaluation per duplicate — which is how one expensive UDF written three times in a `select` runs three times per row.

**Learn it with:**

1. **Spark-docs → Runtime SQL Configuration** ([configuration.html#runtime-sql-configuration](https://spark.apache.org/docs/latest/configuration.html#runtime-sql-configuration)) — `spark.sql.subexpressionElimination.enabled` and `.cache.maxEntries` are the only two of the family that are public; the other two (`.skipForShortcutExpr`, `.filterExec.enabled`) are internal and documented nowhere but the source
2. **Spark-docs → SQL Performance Tuning** ([sql-performance-tuning.html](https://spark.apache.org/docs/latest/sql-performance-tuning.html)) — context for where this sits relative to the tuning levers that *are* documented; read it to see that this one is not among them
3. **Source sweep — [sql/catalyst — expressions in the source map](reference/spark-source-map/sweeps/sql-catalyst-expressions.md)** — the subexpression-elimination concept: `EquivalentExpressions` counts by `semanticEquals`, `ExpressionStats.useCount > 1` is the trigger, and the three exclusions (`LAMBDA_VARIABLE`, `CodegenFallback` children, anything non-deterministic) that decide whether it fires at all. Also the `With` / `CommonExpressionRef` concept — the *declared* form of the same idea, visible in the plan where this one is not
4. **Source sweep — [sql/core — aggregation, windows and exchange in the source map](reference/spark-source-map/sweeps/sql-core-agg-window-exchange.md)** — the same 64 KB method ceiling one level down, inside an aggregate: `splitAggregateExpressions` moves each function's generated update code into its own method, but declines when the resulting parameter list would exceed a valid JVM method signature — at which point the whole stage loses codegen

!!! warning "No book covers this"

    None of SDG, LS2e or Rioux mentions subexpression elimination. It is invisible in `EXPLAIN` —
    the plan shows the duplicated expression either way — so the only evidence it fired is the
    generated code (`spark.sql.codegen.logLevel=INFO`, or `df.queryExecution.debug.codegen()`).

!!! warning "It does not fire for the case people most expect"

    A subtree containing a lambda variable (anything inside `transform` / `filter` / `aggregate`)
    is excluded outright; a `CodegenFallback` expression contributes no children, so shared work
    *underneath* a Python UDF or an imperative aggregate is invisible; and `semanticEquals` is
    false whenever either side is non-deterministic. Writing `expensive_udf(x)` three times in one
    `select` and expecting one evaluation is the standard disappointment — hoist it into its own
    `withColumn` instead.

**Milestone:** You can dump the generated code for a projection containing the same subexpression
twice and point at the extracted helper method; name the three conditions that disable elimination
for a subtree; explain the difference between `With`/`CommonExpressionRef` and subexpression
elimination (declared vs discovered, plan-visible vs codegen-only); and say why
`spark.sql.subexpressionElimination.filterExec.enabled` exists — i.e. what eager column
materialization costs on a highly selective filter.

---


### ⬜ A22 — Approximate Aggregation with Sketches

> Discovered from source sweep (new topic): `sql/catalyst: Sketch-based approximate aggregates`

**What it is:** The family of aggregate functions backed by probabilistic sketches — HyperLogLog++ for distinct counts, KLL for quantiles, Theta and tuple sketches for set operations, Count-Min for frequencies, and approx_top_k for heavy hitters — including the sketch *state* functions that let you persist a partial sketch and merge it later.

**Why you need it:** They turn aggregations that need a full shuffle-and-sort into bounded-memory single-pass ones, and the accumulate/combine/estimate split lets you precompute daily sketches and union them across arbitrary date ranges without touching the raw data again.

**Learn it with:**

1. **Spark-docs → Sketch Functions** ([api/sql/sketch-functions](https://spark.apache.org/docs/latest/api/sql/sketch-functions/)) — the whole family in one place, 40 functions in Spark 4.2: `approx_count_distinct`, the `hll_sketch_agg` / `hll_union_agg` / `hll_sketch_estimate` triple, KLL quantiles, Theta and tuple sketches, `approx_top_k_accumulate` / `_combine` / `_estimate`
2. **Spark-docs → Agg Functions** ([api/sql/agg-functions](https://spark.apache.org/docs/latest/api/sql/agg-functions/)) — the exact counterparts (`count_distinct`, `percentile`, `collect_set`) each sketch replaces, so you can state what accuracy is being traded for what
3. **Apache DataSketches documentation** ([datasketches.apache.org](https://datasketches.apache.org/)) — Spark's HLL, KLL, Theta and tuple sketches are this library; the accuracy/size tables and the theory behind them live there, not in the Spark docs
4. **Source sweep — [sql/catalyst — expressions in the source map](reference/spark-source-map/sweeps/sql-catalyst-expressions.md)** — the sketch concept: `relativeSD` sizes the HLL++ buffer so accuracy is literally a memory dial, and every one of these is a `TypedImperativeAggregate`, which means `ObjectHashAggregateExec` and no whole-stage codegen for the aggregation
5. **Source sweep — [sql/core — query-execution in the source map](reference/spark-source-map/sweeps/sql-core-query-execution.md)** — the two sketch helpers Spark ships outside the aggregate functions: `StatFunctions.multipleApproxQuantiles` (Greenwald-Khanna, tuned by `spark.sql.statistics.percentile.accuracy`) and `FrequentItems` — whose algorithm can report **false positives**, a caveat the API docstring states and users routinely miss
6. **Source sweep — [sql/core — aggregation, windows and exchange in the source map](reference/spark-source-map/sweeps/sql-core-agg-window-exchange.md)** — why every sketch aggregate lands in `ObjectHashAggregateExec`, and the ceiling that follows from it: the `sortBased.fallbackThreshold` of 128 counts *groups*, not bytes, so one sketch per key over 200 keys falls back to sorting while 100 very large sketches do not

!!! warning "No book covers this"

    SDG and LS2e mention `approx_count_distinct` in a list of functions. None covers the sketch
    *state* functions, which are the ones that change how you model a table — and the tuple-sketch
    family is new in Spark 4.2.0, after all three books.

!!! info "The state functions are the point, not the estimates"

    `approx_count_distinct` saves one shuffle. `hll_sketch_agg` + `hll_union_agg` changes the shape
    of the problem: store one sketch per (day, segment), and a distinct count over *any* date range
    becomes a union of pre-computed sketches with no access to the raw rows. Same for
    `approx_top_k_accumulate` / `_combine`. That is a data-modelling capability rather than an
    optimization, and it is invisible if you only read the estimate functions.

!!! warning "Approximate does not mean cheap per row"

    Every sketch aggregate is a `TypedImperativeAggregate`: the aggregation runs in
    `ObjectHashAggregateExec` rather than `HashAggregateExec`, gets no whole-stage codegen, and
    serializes/deserializes its buffer at every shuffle boundary. What you buy is a *bounded*
    buffer and one pass — not a cheaper inner loop.

**Milestone:** You can replace an exact `count(distinct)` with `approx_count_distinct` and state
the resulting error bound and where it came from; build a table of daily HLL sketches and answer a
30-day distinct count from it without rescanning the source; explain why the aggregation plan
changes operator when you add a sketch function to a projection of ordinary sums; and name a case
where a Theta sketch is needed rather than HLL (set intersection or difference, which HLL cannot
do).

---


### ⬜ A23 — Vector Expressions for Embeddings and Similarity

> Discovered from source sweep (new topic): `sql/catalyst: Vector expressions — similarity and norms over float arrays`

**What it is:** The `vector_funcs` family added in Spark 4.2: cosine similarity, inner product, L2 distance, norm and normalize over `array<float>` columns, plus `vector_avg` and `vector_sum` aggregates for centroids.

**Why you need it:** Embedding columns are now ordinary Spark data, and these push similarity scoring into the engine instead of a Python UDF — which is the difference between a codegen-friendly expression and a per-row round trip to a Python worker.

**Learn it with:**

1. **Spark-docs → Vector Functions** ([api/sql/vector-functions](https://spark.apache.org/docs/latest/api/sql/vector-functions/)) — all seven: `vector_cosine_similarity`, `vector_inner_product`, `vector_l2_distance`, `vector_norm`, `vector_normalize`, `vector_avg`, `vector_sum`
2. **Spark-docs → Array Functions** ([api/sql/array-functions](https://spark.apache.org/docs/latest/api/sql/array-functions/)) — there is no `VECTOR` type; these operate on `array<float>`, so the ordinary array surface is what you build and reshape embeddings with
3. **Source sweep — [sql/catalyst — expressions in the source map](reference/spark-source-map/sweeps/sql-catalyst-expressions.md)** — the vector concept: the scalar functions are `RuntimeReplaceable`, rewriting to a `StaticInvoke` of a hand-written Java kernel; `vector_avg` / `vector_sum` are `ImperativeAggregate`s and therefore `CodegenFallback`
4. **Source sweep — [sql/catalyst — framework in the source map](reference/spark-source-map/sweeps/sql-catalyst-framework.md)** — the **join** half of vector search, which this topic was proposed without: `NearestByJoin`, new in 4.2.0, gives SQL a top-K nearest-neighbour join (`... JOIN base APPROX NEAREST 10 BY SIMILARITY <expr>`). Note from the source that `APPROX` and `EXACT` currently do the same thing — both are a brute-force rewrite, so the join is a cartesian product with a top-K per left row; the flag exists so future indexed strategies can fire on `APPROX` alone

!!! warning "No book covers this"

    These landed in Spark 4.2.0 (2026), long after all three books. The prior art they replace is a
    pandas UDF over numpy, or `mllib`'s `Vector` type — neither of which is what these functions
    use.

!!! warning "Strictly `array<float>`, same dimension"

    The type check rejects `array<double>` and `array<int>` outright. An embedding column stored as
    doubles needs an explicit cast before any of these work, and on a large table that cast is not
    free — decide the storage type when you write the column, not when you query it.

**Milestone:** You can score a query embedding against a table of stored embeddings with
`vector_cosine_similarity` and read the resulting plan; explain why the scalar functions appear in
the plan as their replacement expression rather than by name (`RuntimeReplaceable`); state what
happens if the two arrays have different dimensions or the column is `array<double>`; and say why
adding `vector_avg` to a grouped aggregation changes which aggregate operator you get.

---


### ⬜ A24 — SQL Parsing: the Grammar, Reserved Keywords, and Parser Configuration

> Discovered from source sweep (new topic): `sql/catalyst: The grammar — keyword categories and the parser feature flags`

**What it is:** How Spark turns SQL text into a plan: the ANTLR grammar's two keyword lists, the two-stage SLL-then-LL parse, the identifier-quoting and pipe-syntax flags, and the ANTLR DFA cache that can exhaust driver memory on a query-heavy driver.

**Why you need it:** Every SQL error message you have ever read was produced here, the three ANSI *parser* flags are all still off even though ANSI mode is on by default, and an unbounded parser cache is a real and undiagnosed cause of driver OOM on long-lived SQL services.

**Learn it with:**

1. **Spark-docs → ANSI Compliance, SQL Keywords** ([sql-ref-ansi-compliance.html](https://spark.apache.org/docs/latest/sql-ref-ansi-compliance.html)) — the generated reserved / non-reserved keyword table, which is the rendered form of the two lists in the grammar
2. **Spark-docs → Identifiers** ([sql-ref-identifier.html](https://spark.apache.org/docs/latest/sql-ref-identifier.html)) and **IDENTIFIER clause** ([sql-ref-identifier-clause.html](https://spark.apache.org/docs/latest/sql-ref-identifier-clause.html)) — backquoting, double-quoting, and the clause that lets an identifier be computed
3. **Spark-docs → Literals** ([sql-ref-literals.html](https://spark.apache.org/docs/latest/sql-ref-literals.html)) and **Parameter Markers** ([sql-ref-parameter-markers.html](https://spark.apache.org/docs/latest/sql-ref-parameter-markers.html)) — the `:name` / `?` surface whose implementation is textual substitution
4. **Spark-docs → SQL Syntax** ([sql-ref-syntax.html](https://spark.apache.org/docs/latest/sql-ref-syntax.html)) — the statement catalogue, i.e. the user-facing rendering of the grammar's `statement` rule
5. **Source sweep — [sql/catalyst — types & parser in the source map](reference/spark-source-map/sweeps/sql-catalyst-types-parser.md)** — the parser concepts: the two-stage SLL→LL strategy (a failing query is parsed twice), the seven grammar member flags set in `configureParser`, the 345/410-keyword split, `AstBuilder`'s 222 visitors, textual parameter substitution with `PositionMapper`, and the ANTLR DFA cache with its ~9.7 KB-per-state estimate
6. **Source sweep — [sql/core — Python and Arrow in the source map](reference/spark-source-map/sweeps/sql-core-python-arrow.md)** — a narrow but real use of the parser outside SQL text: a polymorphic Python UDTF's driver-side `analyze()` returns partitioning and ordering as *strings*, and `UserDefinedPythonTableFunction.builder` parses them with the session's `ParserInterface` before building the plan — so a malformed expression from Python surfaces as a parse error at analysis time
7. **Source sweep — [sql/catalyst — analysis in the source map](reference/spark-source-map/sweeps/sql-catalyst-analysis.md)** — the analysis side of `IDENTIFIER(...)`, which the parser only marks: `PlanWithUnresolvedIdentifier` / `ExpressionWithUnresolvedIdentifier` are resolved by `ResolveIdentifierClause`, with explicit cases for `InsertIntoStatement` and `V2WriteCommand` whose table position is not an ordinary child. Also the three `spark.sql.ansi.*` grammar switches (`doubleQuotedIdentifiers`, `enforceReservedKeywords`, `relationPrecedence`) confirmed as parser-owned rather than analyzer-owned

!!! warning "ANSI mode is on by default; the ANSI *parser* flags are not"

    `spark.sql.ansi.enabled` defaults to true in Spark 4.x, but `spark.sql.ansi.enforceReservedKeywords`,
    `spark.sql.ansi.doubleQuotedIdentifiers` and `spark.sql.ansi.relationPrecedence` each default to
    **false**. So `SELECT` still works as a column alias, `"x"` is still a *string literal* rather
    than an identifier, and `t1, t2 JOIN t3` still groups left. Enabling ANSI mode changes what your
    casts do (topic **I20**), not what your identifiers mean.

!!! warning "The ANTLR parser cache is unbounded and its management is off by default"

    ANTLR memoizes prediction decisions in a DFA cache that is never purged and lives on the driver,
    at roughly 9.7 KB per state. Spark 4.1 added `spark.sql.parser.manageParserCaches` plus a static
    and a ratio-based flush threshold — all three disabled by default, so Spark does not even measure
    the cache. On a long-lived driver serving many distinct statements (notebook server, Thrift
    server, templated SQL) this grows without bound and presents as an unexplained driver OOM.

!!! info "No book covers the parser"

    SDG, LS2e and Rioux all teach Spark SQL as a language and none opens the grammar. That is
    usually the right call — but it means the keyword lists, the dialect flags, and the parse-time
    feature gates are only discoverable from the docs pages above and the source.

**Milestone:** You can explain why a syntax error costs roughly twice a successful parse; predict
whether `SELECT "abc"` returns a string or fails, under default settings and with
`doubleQuotedIdentifiers` on; name which of reserved-keyword enforcement, double-quoted identifiers
and relation precedence you would have to enable to make Spark's parser genuinely ANSI; and
describe the symptom, the measurement and the fix for an ANTLR DFA cache growing on a long-lived
driver.

---


### ⬜ A25 — Storage-Partitioned Joins

> Discovered from source sweep (new topic): `sql/catalyst: KeyedPartitioning — the 4.2.0 storage-partitioned-join refactor`

**What it is:** Joining two DSv2 tables on their declared partition transforms without shuffling either side: the connector reports partition values, Spark matches them, and the join runs partition-to-partition — with a grouping step when a table has several splits per key.

**Why you need it:** It is the only way to get a shuffle-free join on tables too large to broadcast and not bucketed the Spark way, it is how Iceberg and Delta avoid re-shuffling partitioned tables, and Spark 4.2 rewrote the mechanism (`KeyGroupedPartitioning` became `KeyedPartitioning` with an explicit grouped flag).

**Learn it with:**

1. **Spark-docs → Performance Tuning, Storage Partition Join** ([sql-performance-tuning.html](https://spark.apache.org/docs/latest/sql-performance-tuning.html)) — the only user-facing documentation this feature has: the config family (`spark.sql.sources.v2.bucketing.enabled`, `.pushPartValues.enabled`, `.partiallyClusteredDistribution.enabled`, `spark.sql.requireAllClusterKeysForCoPartition`) and a worked Iceberg example. The stated success criterion is worth memorising: **the plan contains no `Exchange` before the join**
2. **Spark-docs → Data Source V2** ([sql-data-sources-v2.html](https://spark.apache.org/docs/latest/sql-data-sources-v2.html)) — the connector side: a table declares partition transforms and reports partition values, which is the precondition for any of this
3. **Apache Iceberg documentation** ([iceberg.apache.org/docs/latest/spark-queries](https://iceberg.apache.org/docs/latest/spark-queries/)) — the reference connector implementation, and `spark.sql.iceberg.planning.preserve-data-grouping`, the Iceberg-side switch the Spark docs' example pairs with
4. **Source sweep — [sql/catalyst — framework in the source map](reference/spark-source-map/sweeps/sql-catalyst-framework.md)** — the `KeyedPartitioning` concept: the grouped-vs-ungrouped distinction with a worked before/after example from the source, `KeyedShuffleSpec` for the co-partitioning half, and why `spark.sql.requireAllClusterKeysForCoPartition` (default true) disqualifies a side whose clustering keys are a superset or subset of the join keys
5. **Source sweep — [sql/core — datasources in the source map](reference/spark-source-map/sweeps/sql-core-datasources.md)** — the config surface storage-partitioned joins actually run on: `spark.sql.sources.v2.bucketing.enabled` is true and `pushPartValues.enabled` is true, but **nine** further keys in that family are off by default — `allowCompatibleTransforms`, `allowJoinKeysSubsetOfPartitionKeys`, `partiallyClusteredDistribution`, `partition.filter`, `partitionKeyOrdering`, `preserveKeyOrderingOnCoalesce`, `preserveOrderingOnCoalesce`, `shuffle`, `sorting`. Most 'SPJ didn't fire' cases are one of those nine, not the main switch
6. **Source sweep — [sql/core — aggregation, windows and exchange in the source map](reference/spark-source-map/sweeps/sql-core-agg-window-exchange.md)** — the planner half of SPJ, which lives in `EnsureRequirements` rather than in the connector: `splitKeyedPartitionings`' grouped / non-grouped / other categorisation, `checkKeyGroupCompatible`'s common-partition-value push-down, the reducer path for compatible-but-not-identical transforms (and the incompatible-reduced-types error), and exactly where a `GroupPartitionsExec` gets inserted

!!! warning "No book covers this"

    SPJ landed in Spark 3.3 and was substantially rewritten in 4.2; SDG, LS2e and Rioux all predate
    it. Their answer for a large-to-large join is bucketing (Spark's own, v1) or accepting the
    shuffle. SPJ is the DSv2 replacement and behaves quite differently — in particular it depends
    on what the *connector* reports, not on how Spark wrote the data.

!!! warning "Renamed in 4.2.0: `KeyGroupedPartitioning` → `KeyedPartitioning`"

    A source-level break for connectors, extensions and tests that reference the class by name, from
    SPARK-55535 / SPARK-55092. It does not appear in the query-author migration guide because no
    query author names the class. If you maintain a DSv2 connector, this is the entry that matters.

!!! info "Several splits per key is the normal case, and it needs a grouping step"

    A connector may hand Spark several file splits sharing one partition value. That is not a valid
    clustered distribution — the same key would appear in two partitions — so the join cannot
    proceed until `GroupPartitionsExec` merges them. Spark 4.2 made this explicit with an
    `isGrouped` flag and separate `satisfies` / `groupedSatisfies` predicates; before, the
    distinction was implicit and a recurring planning-bug source.

**Milestone:** You can set up two partitioned V2 tables (Iceberg is the easiest), join them on the
partition columns, and show an `EXPLAIN` with **no `Exchange` above either scan**; then break it by
turning off `spark.sql.sources.v2.bucketing.enabled` and show the two exchanges reappear. You can
explain why a join on a *subset* of the partition columns still shuffles under the default
`requireAllClusterKeysForCoPartition`, and say what has to be true of the connector — not of Spark
— for any of it to be possible.

---


### ⬜ A26 — Distribution, Partitioning, and Why Spark Inserts an Exchange

> Discovered from source sweep (new topic): `sql/catalyst: Distribution and Partitioning — the contract that decides whether you get a shuffle`

**What it is:** The requirement-and-satisfaction contract every physical operator is planned against: an operator declares a `requiredChildDistribution`, each child reports an `outputPartitioning`, and an `Exchange` is inserted exactly when `partitioning.satisfies(distribution)` returns false.

**Why you need it:** It is the single mechanism behind every 'why is there a shuffle here' question, it explains why a repartition on the same columns can still be followed by another shuffle, and `satisfies` has a numPartitions precondition that surprises people who thought clustering was enough.

**Learn it with:**

1. **Spark-docs → SQL Performance Tuning** ([sql-performance-tuning.html](https://spark.apache.org/docs/latest/sql-performance-tuning.html)) — `spark.sql.shuffle.partitions`, the AQE coalescing section, and the repartition hints; all of them are levers on the contract this topic describes, documented from the outside
2. **Spark-docs → EXPLAIN** ([sql-ref-syntax-qry-explain.html](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-explain.html)) — `EXPLAIN FORMATTED` is how you read where the `Exchange` nodes landed and what partitioning each one produces
3. **SDG Ch 19** — the partitioning-and-shuffle discussion; correct as far as it goes, but it describes *when* Spark shuffles without giving the rule, which is the gap this topic fills
4. **Source sweep — [sql/catalyst — framework in the source map](reference/spark-source-map/sweeps/sql-catalyst-framework.md)** — the distribution/partitioning concept: the `Distribution` and `Partitioning` hierarchies, `satisfies` and its `final` partition-count precondition, `PartitioningCollection` (why a join output can satisfy several requirements), `HashPartitioning.partitionIdExpression` — the literal formula placing a row — and the `ShuffleSpec` concept for the two-sided case
5. **Source sweep — [sql/core — query-execution in the source map](reference/spark-source-map/sweeps/sql-core-query-execution.md)** — the other end of the same story: `SparkPlan.outputPartitioning` / `requiredChildDistribution` / `requiredChildOrdering` are the three declarations `EnsureRequirements` reads, and `AliasAwareOutputExpression` is what keeps a rename from silently forcing an extra shuffle
6. **Source sweep — [sql/core — adaptive in the source map](reference/spark-source-map/sweeps/sql-core-adaptive.md)** — what happens to a distribution requirement *after* planning. AQE computes the effective user-specified repartition from `inputPlan` before `EnsureRequirements` can delete it (`AQEUtils.getRequiredDistribution`), then re-validates it on the final stage; and every `AQEShuffleReadRule` is run through `ValidateRequirements` and **silently reverted** at `DEBUG` if it broke a requirement. Coalescing that appears not to run is often coalescing that ran and was rolled back
7. **Source sweep — [sql/core — datasources in the source map](reference/spark-source-map/sweeps/sql-core-datasources.md)** — the two write-side sources of a required distribution the optimizer did not choose. `FileFormatWriter` computes a required ordering of *dynamic partition columns, then bucket id, then bucket sort columns* and inserts a global sort whenever the plan does not already satisfy it — which is why a partitioned write is usually dominated by a sort. And `RequiresDistributionAndOrdering` lets a V2 connector demand its own distribution, which `V2Writes` satisfies by inserting an exchange before the write
8. **Source sweep — [sql/core — aggregation, windows and exchange in the source map](reference/spark-source-map/sweeps/sql-core-agg-window-exchange.md)** — `EnsureRequirements` end to end: the satisfaction walk, the two-child co-partitioning path, `shouldConsiderMinParallelism` (the `spark.sql.shuffle.partitions` floor applies **only** when every child would be reshuffled anyway), the `maxBy(_.numPartitions)` cost model the source itself calls non-optimal, and the `maxSinglePartitionBytes` short-circuit

!!! info "The rule is one line, and everything follows from it"

    `EnsureRequirements` inserts an `Exchange` between an operator and a child exactly when
    `child.outputPartitioning.satisfies(operator.requiredChildDistribution)` is false. Every
    "unexpected shuffle" question resolves to: what did the operator require, what did the child
    report, and which clause of `satisfies` failed.

!!! warning "Matching the clustering columns is not sufficient — the partition count must match"

    `satisfies` checks `requiredNumPartitions` **before** the clustering test, and the method is
    `final`, so no partitioning can opt out. A child already hash-partitioned on exactly the join
    keys still gets an exchange if its partition count differs from the requirement's. This is the
    usual explanation for "I called `repartition(200, 'k')` and it still shuffled".

!!! info "Streaming pins the partition count for a correctness reason"

    `StatefulOpClusteredDistribution` requires both the clustering *and* an exact partition count,
    because streaming state is keyed by partition id and must survive restarts. That is also why
    `HashPartitioning.partitionIdExpression` carries a documented cross-version stability
    guarantee — changing the hash would silently mis-route state.

**Milestone:** You can take a query with an unexpected `Exchange`, name which operator's
`requiredChildDistribution` caused it and which clause of `satisfies` the child failed; demonstrate
a case where `repartition(n, col)` does not prevent a downstream shuffle and explain why; describe
what `PartitioningCollection` buys a chain of joins on the same key; and say why changing
`spark.sql.shuffle.partitions` mid-pipeline can introduce an exchange rather than remove one.

---


### ⬜ A27 — Hive Table Conversion: When Spark Reads Hive Tables Natively

> Discovered from source sweep (new topic): `sql/hive: RelationConversions — reading a Hive table with Spark's own reader`

**What it is:** The `spark.sql.hive.convertMetastore*` family, which decides whether a Parquet or ORC table defined in the Hive metastore is read and written through Spark's own vectorised datasource or through Hive's SerDe path — separately for reads, inserts, CTAS and `INSERT OVERWRITE DIRECTORY`.

**Why you need it:** The native path gets vectorised reads, filter and column pushdown and the file-index cache; the SerDe path gets none of them. All eight switches default to on, so most people are already relying on this — and the cases where conversion silently does *not* happen are exactly the ones where a table is unexpectedly slow.

**Learn it with:**

1. **Spark-docs → Hive Tables** ([sql-data-sources-hive-tables.html](https://spark.apache.org/docs/latest/sql-data-sources-hive-tables.html)) — the user-facing view of Hive-table support and the `hive-site.xml` placement it depends on
2. **Spark-docs → Runtime SQL Configuration** ([configuration.html#runtime-sql-configuration](https://spark.apache.org/docs/latest/configuration.html#runtime-sql-configuration)) — the eight `spark.sql.hive.convert*` keys with their defaults and the release each landed in; the one-line descriptions are the whole of their documentation
3. **Spark-docs → Parquet Files** ([sql-data-sources-parquet.html](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html)) — the "Hive metastore Parquet table conversion" section, including the schema-reconciliation rules that apply when conversion happens
4. **Source sweep — [sql/hive — hive-metastore in the source map](reference/spark-source-map/sweeps/sql-hive-hive-metastore.md)** — the `RelationConversions` concept: the substring test that decides conversion, the four independent switches for read / insert / CTAS / `INSERT DIRECTORY`, and the SerDe path (`HadoopTableReader`) you fall onto when it does not fire

!!! warning "Conversion is decided by a substring match on the SerDe class name"

    The whole test is `serde.toLowerCase.contains("parquet")` or `.contains("orc")`. A table
    registered with a custom or renamed SerDe that stores Parquet but is not *called* Parquet will
    not convert — it reads row-at-a-time through Hive's SerDe with no vectorisation, no filter
    pushdown and no column pruning. Nothing in `EXPLAIN` says why; the tell is `Scan hive` where you
    expected `FileScan parquet`.

!!! info "Reads, inserts, CTAS and INSERT DIRECTORY are four separate switches"

    They arrived across four releases (1.1.1, 3.0.0, 3.3.0, 4.0.0) and can disagree. Reading a table
    natively while writing it through Hive is a legitimate configuration — and usually the right one
    when the write must be visible to Hive with Hive's own file layout.

!!! warning "No book covers this"

    SDG, LS2e and Rioux all treat "Spark reads Hive tables" as a single capability. None mentions
    that there are two entirely different read paths over the same files, or that a config chooses
    between them.

**Milestone:** You can create a Hive-serde Parquet table, query it, and show `FileScan parquet` in
the plan; set `spark.sql.hive.convertMetastoreParquet=false`, re-run, and show `Scan hive` plus the
loss of pushdown in the plan. You can name the four independent conversion switches and say which
release each arrived in; explain why a table with a custom SerDe name never converts; and state
what the converted relation is cached in and why that cache does not expire.

---


### ⬜ A28 — LIMIT, OFFSET and the Incremental Take Loop
> Discovered from source sweep (new topic): `sql/core: LIMIT and OFFSET — the incremental take loop`

**What it is:** `LIMIT n` is not one operator and not one job. Depending on where the limit sits and whether an `ORDER BY` precedes it, the planner emits `CollectLimitExec`, a `LocalLimitExec`/`GlobalLimitExec` pair, or `TakeOrderedAndProjectExec`. When the limit is at the root, the driver runs an **escalating loop**: launch a job over one partition, count the rows, estimate how many more partitions are needed, launch another job — repeating until it has `n` rows or has scanned everything.

**Why you need it:** It explains three things that look like bugs. `df.limit(10).show()` producing several jobs in the UI is the loop escalating. `SELECT * FROM huge WHERE rare_condition LIMIT 10` reading the whole table is the loop failing to find rows and scaling 1 → 4 → 16 → … . And adding an `ORDER BY` changing the cost by an order of magnitude is the plan switching between a bounded priority queue and a full sort, decided by one config.

**Learn it with:**

1. **No book covers the execution.** LS2e and SDG cover `limit` as an API call only. The behaviour is source-level.
2. **Spark-docs → LIMIT clause** ([sql-ref-syntax-qry-select-limit.html](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-limit.html)) and **OFFSET clause** ([sql-ref-syntax-qry-select-offset.html](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-offset.html)) — the syntax, including `LIMIT ALL`
3. **Spark-docs → Runtime SQL Configuration** ([configuration.html#runtime-sql-configuration](https://spark.apache.org/docs/latest/configuration.html#runtime-sql-configuration)) — look up `spark.sql.limit.initialNumPartitions`, `spark.sql.limit.scaleUpFactor`, `spark.sql.execution.topKSortFallbackThreshold` and `spark.sql.orderingAwareLimitOffset`; all four change the plan or the number of jobs
4. **Source sweep — [sql/core — query-execution in the source map](reference/spark-source-map/sweeps/sql-core-query-execution.md)** — the loop itself (`SparkPlan.executeTake`), including the "overestimate by 50%" heuristic and the fact that each iteration is a separate `sc.runJob` whose earlier work is not reused; plus why `TakeOrderedAndProjectExec` stops being used above the top-K threshold

**Milestone:** On a table with many partitions, run a filter that matches nothing until the last partition, then `.limit(5).collect()`, and count the jobs in the Spark UI. Predict each job's partition count from `initialNumPartitions` and `scaleUpFactor` before you look. Then show with `explain()` that adding `.orderBy()` before `.limit()` swaps the operator to `TakeOrderedAndProject`, and that raising the row count past `spark.sql.execution.topKSortFallbackThreshold` swaps it back to a full `Sort`.

---


### ⬜ A29 — Recursive CTEs: WITH RECURSIVE and the UnionLoop Operator
> Discovered from source sweep (new topic): `sql/core: Recursive CTEs — UnionLoopExec`

**What it is:** `WITH RECURSIVE t AS (anchor UNION ALL recursive_term) SELECT …`, landed in Spark 4.1. `UnionLoopExec` runs the anchor once, then repeatedly runs the recursive term with the previous round's output substituted for the self-reference, caching each round and accumulating results until a round returns no rows or a limit fires. The loop is driven **from the driver**: every iteration is its own set of Spark jobs.

**Why you need it:** It is the first supported way to walk a hierarchy in Spark SQL — org charts, bills of materials, graph reachability, date-series generation — without dropping to GraphFrames or writing a Python loop that re-submits queries. And because the loop is driver-driven and each round is a full job, the performance model is unlike any other SQL operator: iteration count, not data volume, dominates.

**Learn it with:**

1. **No book covers this** — recursive CTEs postdate every book on the list by several years.
2. **Spark-docs → Common Table Expression** ([sql-ref-syntax-qry-select-cte.html](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-cte.html)) — the CTE page. Note honestly: as of 4.2.0 this page does **not** mention `RECURSIVE` at all, so the syntax is effectively undocumented and the source is the reference
3. **Spark-docs → Runtime SQL Configuration** ([configuration.html#runtime-sql-configuration](https://spark.apache.org/docs/latest/configuration.html#runtime-sql-configuration)) — the three guards: `spark.sql.cteRecursionLevelLimit` (100), `spark.sql.cteRecursionRowLimit` (1000000), `spark.sql.cteRecursionAnchorRowsLimitToConvertToLocalRelation` (100)
4. **Source sweep — [sql/core — query-execution in the source map](reference/spark-source-map/sweeps/sql-core-query-execution.md)** — the operator's own doc comment with the annotated `UnionLoop`/`UnionLoopRef` plan shape, the `numIterations` metric to read when a recursive query is slow, and the limitation that only `UNION ALL` recursion is implemented — so the "use UNION to break cycles" trick from other engines does not apply
5. **Source sweep — [sql/catalyst — analysis in the source map](reference/spark-source-map/sweeps/sql-catalyst-analysis.md)** — the analysis half, and it confirms the `UNION ALL`-only limitation from the other side. `ResolveWithCTE` matches the CTE body against **exactly four shapes**, and a body that matches none of them fails analysis — which is why the recursion grammar is narrower than `UNION` generally. The deduplicating-`UNION` branch is the interesting read: it opens with an unconditional `failAnalysis("UNION_NOT_SUPPORTED_IN_RECURSIVE_CTE")`, and a complete rewrite (`UnionLoop(Distinct(anchor), Except(recursion, UnionLoopRef(...)))`) sits below that call as dead code. So the feature is written and switched off, not merely absent

**Milestone:** Write a recursive CTE that walks a parent/child table to produce each node's depth, and read `numIterations` from the SQL tab to confirm it matches the tree's height. Then introduce a cycle deliberately and show which of the three configs stops it, and what error you get. Finally, explain why bounding the recursion with an explicit depth predicate is better than relying on `cteRecursionLevelLimit`.

---


### ⬜ A30 — Join-Side Buffering and Spill: Why One Key Kills a Task
> Discovered from source sweep (new topic): `sql/core: Join-side buffering and spill`

**What it is:** Every join operator except broadcast hash join holds rows inside the task. A sort-merge join buffers **all buffered-side rows sharing the current key**; a shuffled hash join builds an entire partition's `HashedRelation` in task memory; a cartesian product buffers a whole right partition so it can be re-iterated per left row; a broadcast nested loop join holds the full broadcast array plus a `BitSet`. Each has its own set of thresholds, in its own config namespace — and the hash relations cannot spill at all.

**Why you need it:** This is the failure mode AQE skew handling does not fix. Skew splitting divides a *partition*; a single key with millions of matches still buffers as one unit inside one task. The eight relevant configs live in three operator-specific namespaces that no tuning guide lists together, and choosing the right one starts with reading which operator you actually got.

**Learn it with:**

1. **No book covers this.** SDG Ch 8 and LS2e Ch 7 describe join *strategies*; neither treats the per-task buffers or the spill thresholds. The knowledge is source-level.
2. **Spark-docs → Memory Management / Tuning** ([tuning.html#memory-management-overview](https://spark.apache.org/docs/latest/tuning.html#memory-management-overview)) — `spark.memory.fraction` and the execution/storage split, which is the pool a hash build competes for and the reason a build failure is an error rather than a spill
3. **Spark-docs → Runtime SQL Configuration** ([configuration.html#runtime-sql-configuration](https://spark.apache.org/docs/latest/configuration.html#runtime-sql-configuration)) — look up all three `spark.sql.sortMergeJoinExec.buffer.*` keys and all three `spark.sql.cartesianProductExec.buffer.*` keys; note the identical names with very different defaults, and that shuffled hash join has none
4. **Source sweep — [sql/core — joins in the source map](reference/spark-source-map/sweeps/sql-core-joins-exec.md)** — the eight thresholds mapped to their operators, the `TODO` in both buffer constructions noting that the byte threshold is passed twice because no separate in-memory byte config exists, and the two hard facts: `LongToUnsafeRowMap.spill` returns `0L`, and a failed `BytesToBytesMap` acquisition raises `cannotAcquireMemoryToBuildUnsafeHashedRelation` rather than spilling
5. **Source sweep — [sql/core — query-execution in the source map](reference/spark-source-map/sweeps/sql-core-query-execution.md)** — `ExternalAppendOnlyUnsafeRowArray` itself, the shared buffer type behind sort-merge join, cartesian product and window functions, with its four thresholds and the trade-off stated in both directions in its own header comment

**Milestone:** Build a skewed join where one key has far more matches than the rest, run it as a sort-merge join, and read `spillSize` from the SQL tab to confirm the buffer overflowed. Show that `spark.sql.adaptive.skewJoin.enabled` does **not** reduce it, and explain why in terms of what gets split versus what gets buffered. Then say, for each of the five join operators, what it holds in task memory and whether that thing can spill.

---


### ⬜ A31 — AQE Cost Evaluation: When a Better Plan Is Thrown Away

> Discovered from source sweep (new topic): `sql/core: Cost evaluation — a re-plan is adopted only if the cost does not rise`

**What it is:** Every AQE re-plan is compared against the current plan by a `CostEvaluator` before it is adopted. The default `SimpleCostEvaluator`'s entire cost function is the number of `ShuffleExchangeLike` nodes in the plan — so a re-plan that removes a shuffle wins, one that trades a shuffle for a far cheaper join loses, and `spark.sql.adaptive.customCostEvaluatorClass` is the only supported way to change the rule. When `spark.sql.adaptive.forceOptimizeSkewedJoin` is on, the number of skew joins is packed into the high 32 bits of the same `Long` so that more skew handling always outranks fewer shuffles.

**Why you need it:** It is the difference between "AQE re-planned" and "AQE re-planned and *kept* the result". The adoption gate ignores data size entirely, so a re-plan you can see in the `Plan changed:` log may still be discarded — and no metric or warning says so. Knowing the gate exists is what stops an afternoon of threshold tuning against a decision that never looked at your thresholds.

**Learn it with:**

1. **No book covers this** — AQE cost evaluation is not in LS2e, SDG or the Rioux book; all three describe AQE's rules and none mention that a re-plan is subject to an adoption test. The source and the docs page below are the only references.
2. **Spark-docs → Adaptive Query Execution** ([sql-performance-tuning.html#adaptive-query-execution](https://spark.apache.org/docs/latest/sql-performance-tuning.html#adaptive-query-execution)) — read for the rule list, then note what it does *not* say: nothing on this page describes how a re-planned plan is accepted or rejected
3. **Source sweep — [sql/core — adaptive in the source map](reference/spark-source-map/sweeps/sql-core-adaptive.md)** — the "Cost evaluation" concept: the `newCost < origCost || (newCost == origCost && plans differ)` condition, `SimpleCostEvaluator`'s shuffle count, the skew-join bit packing, and `CostEvaluator.instantiate`'s contract for a custom implementation

**Milestone:** Set `spark.sql.adaptive.logLevel=INFO` and run a query where AQE converts a sort-merge join to a broadcast join; find the `Plan changed:` side-by-side output in the driver log and state which plan had fewer shuffles. Then write a `CostEvaluator` that returns a constant cost, register it with `spark.sql.adaptive.customCostEvaluatorClass`, and confirm from the log that the conversion no longer happens — because a constant cost never satisfies `newCost < origCost`.

---


### ⬜ A32 — Runtime Empty-Relation Elimination and the All-Null Anti Join Short-Circuit

> Discovered from source sweep (new topic): `sql/core: AQEPropagateEmptyRelation — whole subtrees deleted at runtime`

**What it is:** The static optimizer can only prove a relation empty syntactically. Once a query stage has materialized, `AQEPropagateEmptyRelation` re-runs the propagate-empty-relation batch against a *real* row count, so a join, aggregate or union over an input that turned out to be empty is replaced by an `EmptyRelation` in the middle of the query and its downstream stages are never created. It also carries an AQE-only case: a single-column `NOT IN` (null-aware anti join) whose build side broadcast the `HashedRelationWithAllNullKeys` sentinel collapses to an empty result without probing at all.

**Why you need it:** It explains two things that otherwise look like bugs — a plan that is visibly smaller in the SQL tab than in `df.explain()`, and stages that appear in the plan but never run. It also has sharp edges worth knowing before relying on it: a row count is trusted as empty only when it is exact, a user's root-level `repartition` is deliberately exempted (the `ROOT_REPARTITION` tag), and a broadcast query stage is never eliminated on its own because it cannot execute without its join.

**Learn it with:**

1. **No book covers this** — runtime empty-relation propagation postdates all three books' AQE chapters, and the null-aware anti join short-circuit is not described anywhere outside the source.
2. **Spark-docs → SQL performance tuning** ([sql-performance-tuning.html#adaptive-query-execution](https://spark.apache.org/docs/latest/sql-performance-tuning.html#adaptive-query-execution)) — context for what else runs in the AQE optimizer batch; this rule is not listed, which is itself the point
3. **Source sweep — [sql/core — adaptive in the source map](reference/spark-source-map/sweeps/sql-core-adaptive.md)** — the "AQEPropagateEmptyRelation" concept: `getEstimatedRowCount`'s three-valued contract (0 = must be empty, positive = over-estimate, `None` = unknown), the `ROOT_REPARTITION` exemption, `canExecuteWithoutJoin`, and why the rule sets no `ruleId`
4. **Source sweep — [sql/core — joins in the source map](reference/spark-source-map/sweeps/sql-core-joins-exec.md)** — the other half of the `NOT IN` story: the three sentinel `HashedRelation`s and the fact that the first null key encountered during the build frees the map and returns `HashedRelationWithAllNullKeys`
5. **Related config:** `spark.sql.optimizeNullAwareAntiJoin` (see B9), which is what puts the query into the shape this rule can short-circuit

**Milestone:** Build a query joining a large table to a filter that matches zero rows, run it, and compare `df.explain()` (taken before the action) against the plan in the SQL tab — name the operators that disappeared. Then write a `NOT IN` against a subquery column that is entirely NULL and show from the SQL tab that the probe side scan reported no output rows.

---


### ⬜ A33 — Two-Level Hash Aggregation and the Codegen Fast Hash Map

> Discovered from source sweep (new topic): `sql/core: The codegen fast hash map — two levels, row-based or vectorized`

**What it is:** Whole-stage codegen puts a generated, fixed-capacity hash map in front of the real BytesToBytesMap — a row-based one by default, a columnar one behind a second flag — that probes with at most two linear steps and silently declines every key whose type or aggregation mode it does not support.

**Why you need it:** It is the layer that decides whether a groupBy runs at memory bandwidth or at BytesToBytesMap speed, it is on by default and invisible in EXPLAIN, and its eligibility rules — primitive/decimal/string keys only, partial modes only unless a config is flipped — explain why two structurally identical aggregates can differ several-fold in runtime.

**Learn it with:**

1. **No book covers this** — LS2e and SDG both describe Tungsten's binary rows and whole-stage codegen, and neither mentions that the generated aggregate emits a second hash map. The configs are unlisted in every performance-tuning guide, so the source and the generated code are the only references.
2. **Spark-docs → Runtime SQL Configuration** ([configuration.html#runtime-sql-configuration](https://spark.apache.org/docs/latest/configuration.html#runtime-sql-configuration)) — the four `spark.sql.codegen.aggregate.*` keys and their defaults; note that `map.twolevel.enabled` is `true` and `map.vectorized.enable` is `false`, which is the shipped combination
3. **Source sweep — [sql/core — aggregation, windows and exchange in the source map](reference/spark-source-map/sweeps/sql-core-agg-window-exchange.md)** — the "codegen fast hash map" concept: `checkIfFastHashMapSupported`'s three gates, the `1 << capacityBit` fixed capacity, the two-step linear probe, the row-based vs columnar generators, and the INFO-log-and-continue path when the flag is on but the query is ineligible. Read it alongside the same page's "HashAggregateExec and TungstenAggregationIterator" concept — the fast map is the *first* level, the `BytesToBytesMap` the second, and the sort-based fallback the third
4. **Prerequisite:** B6 (basic aggregations) for the three-operator ladder, and E1 for what `UnsafeRow` and Tungsten memory actually are

!!! warning "By default, only partial aggregates get a fast map"

    `spark.sql.codegen.aggregate.map.twolevel.partialOnly` defaults to `true`, which restricts the
    fast map to `Partial` and `PartialMerge` modes. The post-shuffle final aggregate — often the
    one with the group cardinality that matters — runs without it unless you flip that key.

**Milestone:** Run a `groupBy` over a `LongType` key with `sum`, capture the generated code with
`df.queryExecution.debug.codegen()` (or `spark.sql.codegen.comments=true` and the SQL tab's
generated-code view), and find the `FastHashMap` class and its `findOrInsert`. Then change the
grouping key to an `ArrayType` and show the class is gone. Finally set
`spark.sql.codegen.aggregate.map.twolevel.enabled=false` and compare wall time on a
high-cardinality partial aggregate.

---


### ⬜ A34 — Segment-Tree Window Frames: O(log W) Sliding Windows

> Discovered from source sweep (new topic): `sql/core: Segment-tree window frames — the 4.2.0 sliding-frame algorithm, off by default`

**What it is:** Spark 4.2.0 adds an opt-in window-frame implementation that builds a blocked segment tree over the buffered partition so a moving frame is answered in O(log W) merges instead of re-aggregating W rows, with an LRU of internal nodes registered as a TaskMemoryManager consumer that can spill.

**Why you need it:** It is the first change to sliding-window cost since the operator was written — a `ROWS BETWEEN 1000 PRECEDING AND CURRENT ROW` goes from quadratic to near-linear — but it is disabled by default, restricted to nine allowlisted aggregates, refuses any frame carrying a FILTER, and falls back to the old sliding frame below a row threshold, so knowing when it actually engages is the whole skill.

**Learn it with:**

1. **No book covers this** — the feature is new in Spark 4.2.0 (2026); Rioux, LS2e and SDG all predate it, and none of them discusses window *frame cost* at all beyond noting that windows are expensive. There is also no dedicated docs page: the four configs appear only in the runtime SQL configuration table.
2. **Spark-docs → Window Functions syntax** ([sql-ref-syntax-qry-select-window.html](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-window.html)) — read it for the frame grammar (`{ROWS|RANGE} BETWEEN frame_start AND frame_end`), because the whole topic turns on which of those shapes is a *moving* frame. `UNBOUNDED PRECEDING AND CURRENT ROW` is a running total and was always O(1) amortised; only a bounded-on-both-sides frame ever re-aggregates.
3. **Spark-docs → Runtime SQL Configuration** ([configuration.html#runtime-sql-configuration](https://spark.apache.org/docs/latest/configuration.html#runtime-sql-configuration)) — the only official record of `spark.sql.window.segmentTree.*`; check the defaults there against your cluster's version rather than assuming
4. **Source sweep — [sql/core — aggregation, windows and exchange in the source map](reference/spark-source-map/sweeps/sql-core-agg-window-exchange.md)** — the "Segment-tree window frames" concept, which is the substantive reference: the four eligibility gates, the nine-aggregate allowlist and why the ranking functions are excluded (they extend `DeclarativeAggregate` but reject `mergeExpressions`), the `ceil(W / blockSize) + 2` cache budget, the `SegTreeSpiller` memory-consumer invariants, and the `numSegmentTreeFrames` / `numSegmentTreeFallbackFrames` metrics
5. **Prerequisite:** I2 (window functions) — this topic assumes you can already read a frame spec and know which functions carry one

!!! info "The allowlist is the first thing to check"

    `Min`, `Max`, `Sum`, `Count`, `Average`, `StddevPop`, `StddevSamp`, `VariancePop`,
    `VarianceSamp` — nine functions, and nothing else. A `collect_list` or a percentile over a
    moving frame gets the old `SlidingWindowFunctionFrame` no matter how the configs are set,
    because the segment tree needs a mergeable aggregate buffer.

**Milestone:** Build a table of a few million rows in one window partition, run a
`sum(x) OVER (ORDER BY t ROWS BETWEEN 5000 PRECEDING AND CURRENT ROW)` with
`spark.sql.window.segmentTree.enabled=false`, then with it `true`, and compare stage durations.
Then state, for three variants — the same query with `collect_list`, the same query with a
`FILTER (WHERE …)` clause, and the same query over a 32-row partition — which gate rejects each
and which frame class actually runs.

---


### ⬜ A35 — Python Data Sources: Writing a Connector Without the JVM

> Discovered from source sweep (new topic): `sql/core: Python Data Sources — a reader and writer written entirely in Python`

**What it is:** Spark 4.x lets a data source be implemented in pure Python by subclassing pyspark.sql.datasource.DataSource; the JVM drives it through a long-lived worker process, sending numbered function ids for initialOffset, latestOffset, partitions and commit, and 4.2.0 adds admission control and Trigger.AvailableNow support to the streaming reader.

**Why you need it:** It replaces the two old answers to 'Spark cannot read my system' — drop to an RDD, or write Scala — and it is now a supported batch and streaming, read and write surface with its own profiler; but the protocol is a hand-rolled request/response over a pipe, so knowing what crosses it is what lets you reason about its cost and its failure modes.

**Learn it with:**

1. **Spark-docs → Python Data Source API** ([tutorial/sql/python_data_source.html](https://spark.apache.org/docs/latest/api/python/tutorial/sql/python_data_source.html)) — the primary and effectively only reference: `DataSource`, `DataSourceReader`, `DataSourceWriter` with its commit/abort contract, `DataSourceStreamReader` and the simpler `SimpleDataSourceStreamReader`, `DataSourceStreamWriter`, registration and precedence rules, Arrow batch support, and admission control via `ReadLimit`. It carries complete runnable examples — work through them rather than reading
2. **No book covers this** — the API is new in Spark 4.0; SDG, LS2e and Rioux all predate it and their answer to a custom source is an RDD or a JVM `DataSourceV2`
3. **Source sweep — [sql/core — Python and Arrow in the source map](reference/spark-source-map/sweeps/sql-core-python-arrow.md)** — the "Python Data Sources" concept, i.e. what the JVM actually does: `PythonStreamingSourceRunner` keeps one worker alive for the life of the stream and calls into it with numbered function ids (884 `initialOffset`, 885 `latestOffset`, 886 `partitions`, 887 `commit`, plus 4.2.0's 890 admission-control and 892 report-latest-offset), and `checkSupportedFeatures` asks the reader which optional capabilities it implements rather than requiring them
4. **Related:** B4's Python-Data-Sources callout is the "it exists" entry point; this topic is the build-one version. **E23** (DSv2 catalog transactions) and **A25** are the JVM-connector counterparts — a Python source gets neither transactions nor storage-partitioned joins
5. **Related config:** `spark.sql.python.filterPushdown.enabled` (**`false`**, 4.1.0) — filter push-down into a Python source is opt-in; without it every filter is applied after the source has already produced the rows

!!! warning "Two capabilities are optional, and Spark asks rather than requires"

    `checkSupportedFeatures` probes for admission control (`getDefaultReadLimit` / `ReadLimit`) and
    `Trigger.AvailableNow` (`prepareForTriggerAvailableNow`) separately. A reader missing them still
    works — it just cannot be rate-limited and cannot back a `Trigger.AvailableNow` query. If your
    streaming Python source ignores `maxRecordsPerTrigger`, this is why.

**Milestone:** Implement a batch `DataSource` with a reader that yields synthetic rows, register it with `spark.dataSource.register`, and read it through `spark.read.format(...)`. Then extend it to a `DataSourceStreamReader` with real offsets and run it under both `Trigger.ProcessingTime` and `Trigger.AvailableNow` — stating which of your methods each trigger calls, and confirming from the Spark UI that the worker process is reused across micro-batches rather than restarted.

---


### ⬜ A36 — The Streaming Checkpoint Protocol: Offset Log, Commit Log, and Restart

> Discovered from source sweep (new topic): `sql/core: MicroBatchExecution — the batch loop, and the two-log write-ahead protocol`

**What it is:** A micro-batch is durable before it runs: the offset log records the batch's end offsets before any data is processed, the commit log records completion after the sink commits, and on restart the presence or absence of a commit entry for the latest offset entry is what decides whether Spark replays that batch or moves to the next one.

**Why you need it:** Every exactly-once claim, every "my query reprocessed a batch after restart" question, and every checkpoint-corruption incident resolves to the ordering of those two writes and what the recovery path reads back — and none of it is visible from the DataFrame API, which is why checkpoints are the part of streaming operations people learn by outage.

**Learn it with:**

1. **Spark-docs → Fault Tolerance Semantics** ([streaming/getting-started.html#fault-tolerance-semantics](https://spark.apache.org/docs/latest/streaming/getting-started.html#fault-tolerance-semantics)) — the official statement of the contract: replayable sources plus idempotent sinks plus checkpointing and write-ahead logs give end-to-end exactly-once. It is four paragraphs and it is the *claim*; the mechanism is below
2. **Spark-docs → Recovering from Failures with Checkpointing** ([streaming/apis-on-dataframes-and-datasets.html#recovering-from-failures-with-checkpointing](https://spark.apache.org/docs/latest/streaming/apis-on-dataframes-and-datasets.html#recovering-from-failures-with-checkpointing)) — the `checkpointLocation` option and, importantly, the list of query changes that are *not* allowed across a restart
3. **SDG Ch 23** — streaming in production: checkpointing, restart strategies, and what to do when a checkpoint must be abandoned. Written against Spark 2.x, so the directory layout and log versions have moved, but the operational reasoning is intact
4. **Source sweep — [sql/core — streaming execution in the source map](reference/spark-source-map/sweeps/sql-core-streaming-exec.md)** — the mechanism the docs describe in one sentence: `markMicroBatchStart` writing the offset log *before* the batch runs (timed as `walCommit`) and only then letting sources discard data, `markMicroBatchEnd` writing the commit log after, and `populateStartOffsets` comparing the two logs on restart. Plus `HDFSMetadataLog`'s atomic create-if-absent, `CompactibleFileStreamLog`'s periodic compaction, and 4.1.0's `ChecksumCheckpointFileManager`
5. **Prerequisite:** A7 (streaming fundamentals). **Pairs with:** **E27** (the state store, which is checkpointed on a different schedule) and **A14** (determinism under retry — the batch-level analogue)

!!! info "A re-run batch is the *same* batch, not a different one"

    On restart, an offset-log entry with no matching commit-log entry means the batch was durable
    but unfinished, and Spark re-runs it with **exactly the offsets recorded**. That is what makes
    idempotency the only requirement on a sink: you never have to handle "the same data split
    differently", only "batch N delivered twice". It is also why `foreachBatch` receives the batch
    id — it is the natural idempotency key.

!!! warning "Two offset-log entries are required to restart"

    `validateOffsetLogAndGetPrevOffset` needs entry N-1 as well as N, and says so in its error.
    A checkpoint directory that has been partially cleaned — or restored from a backup that caught
    it mid-purge — fails here rather than silently restarting from the wrong position.
    `spark.sql.streaming.minBatchesToRetain` is what keeps enough of them.

**Milestone:** Run a file-source query with a `checkpointLocation`, stop it after several batches, and read the checkpoint directory by hand: name what is in `offsets/`, `commits/`, `sources/` and `metadata/`, and state which batch id would run next. Then kill the query mid-batch (not a clean stop), restart it, and show from `lastProgress` and the sink that the interrupted batch ran again with the same offset range. Finally, delete the newest `commits/` entry from a stopped query and predict — then verify — what the next start does.

---


### ⬜ A37 — Column Without an Engine: ColumnNode and the api/classic/connect Split

> Discovered from source sweep (new topic): `sql/core: Column without an engine — ColumnNode and its converter`

**What it is:** Since Spark 4.0 a Column is no longer a wrapper around a Catalyst Expression — it holds a ColumnNode, a small serializable tree defined in sql/api with no dependency on the query engine, which classic mode converts to an Expression at plan-construction time and Connect mode serializes to protobuf instead.

**Why you need it:** It is the single design decision that lets one `F.col(...).cast(...)` expression work identically against a local JVM engine and a remote gRPC server, and it explains a class of behaviour differences people attribute to Connect bugs: what a `Column` can carry is bounded by what `ColumnNode` can express, and anything needing a real `Expression` must be converted first.

**Learn it with:**

1. **No book covers this** — the restructuring landed in Spark 4.0, after Rioux, LS2e and SDG. All three describe `Column` as a wrapper around a Catalyst `Expression`, which was true when they were written and is no longer the design.
2. **Spark-docs → Application development with Spark Connect** ([app-dev-spark-connect.html](https://spark.apache.org/docs/latest/app-dev-spark-connect.html)) and the [Connect gotchas](https://spark.apache.org/docs/latest/spark-connect-gotchas.html) — the user-visible consequences of the split, stated without the mechanism. Read them first so the mechanism has something to explain
3. **Spark-docs → `Column` ScalaDoc** ([api/scala/org/apache/spark/sql/Column.html](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/Column.html)) — note where the class now lives (`sql/api`) and what it holds
4. **Source sweep — [sql/core — the classic API in the source map](reference/spark-source-map/sweeps/sql-core-classic-api.md)** — the two concepts that carry this topic: "One API, two implementations" (every class in `classic/` is `extends sql.<Name>`, and `ClassicConversions` is the bridge back) and "Column without an engine" (`ColumnNodeToExpressionConverter`'s full match, the `CurrentOrigin` wrapping that gives Spark 4 error messages their source positions, the `UnresolvedDataFrameStar` case behind `df["*"]` in a self-join, and inline UDF invocation as a node type)
5. **Source sweep — [sql/connect — client-server in the source map](reference/spark-source-map/sweeps/sql-connect-client-server.md)** — the other half: `SparkConnectPlanner` turning the protobuf form of the same tree into an unresolved `LogicalPlan`, and `ConnectClientUnsupportedErrors` as the enumerated divergence list
6. **Prerequisites:** B2 (which implementation you get) and A1 (what an `Expression` and a `LogicalPlan` are). **Pairs with:** E9

!!! info "The constraint that produced the design"

    Anything in `sql/api` must compile without the query engine, because a Connect client has no
    engine. That single rule is why `Column` holds a `ColumnNode`, why encoders were split into an
    `AgnosticEncoder` half, and why `sql/api` exists at all. When you wonder why an API is shaped
    oddly in Spark 4, ask whether it had to cross that line.

**Milestone:** In a Scala or PySpark shell, build a moderately complex `Column` — a `when`/`otherwise` over a cast and a UDF call — without ever putting it in a DataFrame, and describe what object you are holding. Then state which parts of it become which Catalyst expressions on conversion. Finally, name two `Column` operations you would expect to behave identically on classic and Connect and one that cannot, and justify each from where the work happens.

---


### ⬜ A38 — Dataflow Graph Resolution: Parallel Fixed-Point Analysis

> Discovered from source sweep (new topic): `sql/pipelines: DataflowGraphTransformer — parallel fixed-point resolution with retryable failures`

**What it is:** How Spark Declarative Pipelines turns an unordered bag of dataset definitions into a topologically sorted, resolved graph — by resolving flows on a ten-thread pool, treating an unresolved dependency as a retryable exception, and re-queueing the dependents when the dependency lands.

**Why you need it:** Every pipeline error you will actually see — a cycle, a typo'd source, a flow that resolves but whose target does not — is produced by this loop, and the way it classifies direct versus downstream failures is what makes a pipeline error log readable or useless.

**Learn it with:**

1. **Source sweep — [sql/pipelines — graph](reference/spark-source-map/sweeps/sql-pipelines-graph.md)** — the primary material for this topic; read the *DataflowGraphTransformer*, *VirtualTableInput* and *FlowAnalysis* sections together, in that order. The three ideas that make the algorithm click: an unresolved input is a **retryable exception**, not a lookup failure; every table is replaced by an empty DataFrame carrying only its schema, so analysis never touches storage; and the topological order is an *output* of the loop rather than an input to it
2. **Spark-docs → Declarative Pipelines Programming Guide, `spark-pipelines dry-run`** ([declarative-pipelines-programming-guide.html](https://spark.apache.org/docs/latest/declarative-pipelines-programming-guide.html#spark-pipelines-dry-run)) — the user-facing surface of exactly this phase: a dry run *is* resolution plus validation, and nothing else
3. **Source** — `sql/pipelines/.../graph/DataflowGraphTransformer.scala` (395 lines, and the whole algorithm is `transformDownNodes`), then `CoreDataflowNodeProcessor.scala` for the visitor it drives
4. **Local stack** — build a deliberately broken pipeline against your Spark 4.2 + Delta setup: one flow reading a table that does not exist, one pair of flows reading each other, and one flow reading a dataset whose own flow fails. Compare the three error reports

!!! info "No book covers this — source and docs only"
    Declarative Pipelines postdates every published Spark book, and this is its least-documented
    layer: the programming guide describes what a dry run *does for you*, never how resolution
    works. The sweep page is the reference.

!!! warning "Read A11 first"
    This topic assumes you can already write and run a multi-dataset pipeline. Taken cold it is an
    algorithm with no application.

**Milestone:** You can explain why a pipeline definition file needs no dependency declarations and no ordering, and why resolution is nevertheless deterministic in its *result* while non-deterministic in its *sequence*. Given a pipeline whose error log lists six failed flows, you can say which one actually broke and which five are downstream of it — and point at the code that made that distinction. You can state what a `VirtualTableInput` is and why the plan produced by resolution cannot be executed.

---


### ⬜ A39 — Pipeline Schema Inference and Evolution: Merge, Diff, and Alter

> Discovered from source sweep (new topic): `sql/pipelines: SchemaInferenceUtils.inferSchemaFromFlows — merging every writer's schema`

**What it is:** How a declarative pipeline decides what schema a table should have — by merging the analysed schemas of every flow that writes to it, folding in any user-declared schema, then diffing against the catalog to emit the exact ALTER TABLE changes needed.

**Why you need it:** This is what makes "add a column to a query and it appears in the table" work, and its edges are where pipelines surprise people: a case-only rename becomes drop-plus-add, and on a materialized view the diff can emit a column drop.

**Learn it with:**

1. **Source sweep — [sql/pipelines — pipeline-runtime](reference/spark-source-map/sweeps/sql-pipelines-pipeline-runtime.md)** — the primary material: read *SchemaInferenceUtils.inferSchemaFromFlows* for the inference half (fold `StructType.merge` over every writing flow, then merge the user-declared schema on top, user schema first so its metadata wins) and *diffSchemas* for the DDL half. Two edges live in the second: matching is **case-sensitive** here and resolver-aware everywhere else in the subsystem, and the emitted change set includes `deleteColumn`, which the function's own scaladoc does not mention
2. **Source sweep — [sql/pipelines — graph](reference/spark-source-map/sweeps/sql-pipelines-graph.md)** — where the inferred schema is *used*: `VirtualTableInput` loads it during analysis (which is why a new column propagates through the whole graph before any DDL runs), `validateUserSpecifiedSchemas` checks it, and `DatasetManager.materializeTable` decides whether the target schema is a merge or a replacement. That decision is the whole topic in one line: `mergeSchemas(existing, new)` for an incremental streaming table, the new schema **outright** for a materialized view or any full refresh
3. **Spark-docs → Declarative Pipelines Programming Guide** ([declarative-pipelines-programming-guide.html](https://spark.apache.org/docs/latest/declarative-pipelines-programming-guide.html)) — for the schema-declaration syntax the inference interacts with. The guide does not describe evolution behaviour; the sweeps are the reference for that
4. **A6 / E4 — Delta Lake schema evolution and the transaction log** — the contrast worth drawing. `mergeSchema` on a write is opt-in and per-write; pipeline inference is unconditional and derives the schema from the *query*, then reconciles the catalog to match. Same words, opposite direction of control
5. **Local stack** — build a streaming table fed by **two** append flows with different column sets and confirm the table gets the union. Then add a column to one flow and re-run, confirming it appears without DDL. Then remove a column from a materialized view's query and check the catalog: you should find the column dropped

!!! info "No book covers this — source and docs only"
    Schema evolution is covered in book form only for Delta writes (`mergeSchema` / `overwriteSchema`),
    which is a different mechanism with different defaults. The pipeline inference path is
    source-only.

!!! warning "Two behaviours to internalise before running a pipeline against a table you care about"
    A **materialized view's** target schema is the new schema outright, not a merge — so removing a
    column from its query issues an `ALTER TABLE ... DROP COLUMN`, produced by an ordinary query edit
    with no warning. And because `diffSchemas` keys on the raw field name, changing only a column's
    **case** produces an add plus a delete rather than no change at all.

**Milestone:** You can predict a pipeline table's schema from the flows writing to it, without running it, including the multi-flow union case. Given a query edit — add a column, remove a column, change a type, change only the case of a name — you can say which `TableChange`s the next run will emit and whether the answer differs between a streaming table and a materialized view. You can explain why declaring a schema on a table does not prevent a flow adding a column to it, and which validation does catch the mismatch.

---


### ⬜ A40 — Stream Rate Limiting and Backpressure: the PID Loop and Per-Partition Caps

> Discovered from source sweep (new topic): `connector/kafka-0-10: Rate limiting and backpressure — the PID loop and the per-partition split`

**What it is:** How Spark bounds how much a streaming batch reads — a PID controller that turns the previous batch's processing time and scheduling delay into a records-per-second estimate, and the per-partition caps and floors that estimate is then divided across in proportion to each partition's lag.

**Why you need it:** An unbounded first batch after a restart is the classic way a streaming job dies, and every lever that prevents it — backpressure, the initial rate, per-partition maxima and minima — behaves differently from its documentation, including one config that the direct Kafka stream reads past its own declared fallback.

**Learn it with:**

1. **Spark-docs → Configuration, Spark Streaming** ([configuration.html#spark-streaming](https://spark.apache.org/docs/latest/configuration.html#spark-streaming)) — the `spark.streaming.backpressure.*` and `spark.streaming.receiver.maxRate` family, with the declared defaults you will then need to check against the source
2. **Spark-docs → Structured Streaming + Kafka, `maxOffsetsPerTrigger`** ([structured-streaming-kafka-integration.html](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html)) — the modern equivalent: a static per-trigger record cap with no controller behind it. Read both and know which one you are using
3. **Source sweep — [connector/kafka-0-10 — consumer in the source map](reference/spark-source-map/sweeps/connector-kafka-0-10-consumer.md)** — the DStream implementation end to end: `maxMessagesPerPartition`'s lag-proportional split, the per-partition ceiling and floor, the seconds→batch multiply, and the raw-string read of `initialRate`
4. **Local stack** — run a direct Kafka stream against a topic with a deliberate backlog, restart it with and without `spark.streaming.kafka.maxRatePerPartition` set, and watch the first batch's input size in the Streaming UI
5. **Source sweep — [streaming — DStream in the source map](reference/spark-source-map/sweeps/streaming-dstream.md)** — **the implementation home of this topic.** The PID controller, the rate estimator and the `RateController` that drives them all live in `streaming/scheduler/`, and the receiver path is where they were designed to be used: a Guava `RateLimiter` sits in `Receiver.store`'s data path and is acquired once per record, with the driver pushing new rates down as `UpdateReceiverRateLimit` on each batch completion. Read it with the Kafka page, because the comparison exposes a real inconsistency: `RateLimiter.getInitialRateLimit` reads `spark.streaming.backpressure.initialRate` through its **declared `ConfigEntry`**, so the documented `fallbackConf` to `spark.streaming.receiver.maxRate` applies and the effective default is `Long.MaxValue` — while the direct Kafka stream reads the same key as a raw string with a default of **0**. One key, two behaviours, and only the receiver path matches the documentation. Also here: `spark.streaming.backpressure.rateEstimator` accepts only `"pid"` and throws on anything else, and `updateRate` silently ignores any value ≤ 0

!!! warning "No book covers this"

    SDG (2018) and LS2e (2020) describe DStream backpressure only as "turn it on"; Rioux does not
    cover streaming rate control at all. The PID terms, the per-partition split and the interaction
    with the static ceiling are docs-and-source territory.

!!! warning "`spark.streaming.backpressure.initialRate` does not fall back for the direct Kafka stream"

    The config is declared as `fallbackConf(RECEIVER_MAX_RATE)`, so its documented effective default
    is `spark.streaming.receiver.maxRate` — `Long.MaxValue`. But `DirectKafkaInputDStream` reads it
    with `getLong("spark.streaming.backpressure.initialRate", 0)`, a **raw string key with its own
    default of 0**, bypassing both the `ConfigEntry` and the fallback. Setting
    `spark.streaming.receiver.maxRate` therefore has no effect on a direct Kafka stream, and the
    unset default is 0, not `Long.MaxValue`.

!!! warning "The default configuration has no limit at all"

    `spark.streaming.backpressure.enabled` is **false** and `spark.streaming.kafka.maxRatePerPartition`
    is **0**, which means unlimited. With both at their defaults, the first batch after any downtime
    reads every record available in every partition — the standard way a restart turns into an OOM.
    Note also that the `minRatePerPartition` floor (default 1) is applied *after* the rate is
    converted to records-per-batch, so a caught-up stream with a non-zero ceiling still asks for one
    record per partition per batch.

!!! info "Backpressure is a controller, `maxOffsetsPerTrigger` is a constant"

    The DStream mechanism observes the previous batch's processing time and scheduling delay and
    adjusts; Structured Streaming's Kafka source instead takes a fixed per-trigger cap you set
    yourself. The tuning problem is the same, the failure modes are not: a PID loop can oscillate
    and has a `pid.minRate` floor (100) it will never go below, while a static cap cannot adapt to a
    slow sink at all.

**Milestone:** Given a batch interval, a per-partition ceiling and a set of per-partition lags, you can compute how many records each partition contributes to the next batch; explain what changes when backpressure is enabled and what the PID floor guarantees; and say what a direct Kafka stream will read in its first batch after a week of downtime under the default configuration, and which single config you would set to bound it.

---


### ⬜ A41 — Decoupling Spark Tasks from Kafka Partitions: minPartitions and maxRecordsPerPartition

> Discovered from source sweep (new topic): `connector/kafka-0-10-sql: Offset range calculation — minPartitions, maxRecordsPerPartition, and placement`

**What it is:** How the Kafka source decides how many Spark tasks read a batch — a 1:1 mapping to topic-partitions by default, and two options that split a partition's offset range across several tasks, plus the executor-placement hash that decides which JVM each range lands on.

**Why you need it:** Kafka partition count is a broker-side decision you often cannot change, and without these options it hard-caps your read parallelism; but splitting also breaks the consumer-cache affinity that makes reads fast, so the tuning has a cost that is invisible unless you know where it comes from.

**Learn it with:**

1. **Spark-docs → Structured Streaming + Kafka, "Configuration"** ([structured-streaming-kafka-integration.html](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html)) — the `minPartitions` and `maxRecordsPerPartition` option rows, including the documented "approximately" caveat and the note that they apply to batch reads too
2. **Kafka docs → Design, "Partitions"** ([kafka.apache.org/documentation/#design](https://kafka.apache.org/documentation/#design)) — why the partition count is a topic-level decision with ordering consequences, i.e. why you often cannot just raise it
3. **Source sweep — [connector/kafka-0-10-sql — source & sink in the source map](reference/spark-source-map/sweeps/connector-kafka-0-10-sql-source-sink.md)** — `KafkaOffsetRangeCalculator` in full: the ordering of the two options, the proportional split that excludes already-small ranges, the last-part-absorbs-the-remainder division, and the `floorMod` executor assignment that only the unsplit path gets
4. **Local stack** — read one Kafka topic with 3 partitions, then re-run with `minPartitions=12`; compare the task count in the SQL tab and the "Creating Kafka reader" lines in the executor logs to see the ranges actually change

!!! warning "No book covers this"

    SDG (2018) predates the option; LS2e and Rioux describe the Kafka source at the level of
    `subscribe` and `startingOffsets`. This is docs-and-source territory, and the docs give the
    options without the consequence below.

!!! warning "Splitting silently forfeits the consumer cache for that batch"

    `KafkaOffsetRangeCalculator.getRanges` attaches a preferred executor location — a
    `floorMod(topicPartition.hashCode, numExecutors)` chosen precisely "so cached KafkaConsumers in
    the executors can be re-used" — **only on the path where no splitting happens**. As soon as
    `minPartitions` or `maxRecordsPerPartition` causes any range to split, every range in that batch
    comes back with `preferredLoc = None`, the scheduler places tasks freely, and the executor-side
    consumer pool stops hitting. On a steady stream that is the difference between reusing a warm
    consumer with its prefetch buffer and building a fresh `KafkaConsumer` per partition per batch.
    Nothing logs it. Prefer raising the topic's own partition count when you can.

!!! info "`size` is an offset difference, not a record count"

    Both options divide `untilOffset − fromOffset`, and the source's own scaladoc warns that this
    "may be different than the real number of messages due to log compaction or transaction
    metadata". On a compacted topic or one with heavy transactional traffic, `maxRecordsPerPartition`
    is an upper bound on offsets, not on rows — the resulting tasks can be far smaller than asked
    for, and unevenly so.

!!! info "The two options compose, in a fixed order"

    `maxRecordsPerPartition` runs first and splits every range that exceeds it. `minPartitions` then
    splits further *only if* the result still has fewer parts than requested — and it excludes ranges
    that would get one part anyway from the proportional maths, so a single huge partition does not
    drag every small one into being split. Empty and negative-size ranges are dropped both before
    and after.

**Milestone:** Given four topic-partitions with backlogs of 1,000 / 10 / 10 / 10 records and `minPartitions=8`, you can say roughly how many Spark tasks the batch produces and which partition supplies most of them; explain why the same read with `minPartitions` unset gets executor affinity and this one does not; and say what `maxRecordsPerPartition=1000` guarantees, and does not guarantee, on a compacted topic.

---


### ⬜ A42 — UNION ALL: Partitioning-Aware Output and Codegen Fusion

> Discovered from source sweep (new topic): `sql/core: UNION ALL — output partitioning and codegen fusion`

**What it is:** Two decisions `UnionExec` makes that used to be "no" unconditionally. First, **output partitioning** (`spark.sql.unionOutputPartitioning`, true since 4.1): if every child reports the same partitioning after rewriting each one's attributes into the first child's, the union reports it too — so an aggregate above the union needs no re-shuffle. Second, **codegen fusion** (`spark.sql.codegen.wholeStage.union.enabled`): the union's children can be fused into one generated loop over a `UnionRDD` instead of the union ending the pipeline, unless one of eight disqualifiers fires.

**Why you need it:** A union sits in the middle of most real pipelines — backfill plus increment, several sources normalised to one schema — and until 4.x it cost both an extra shuffle and a codegen break every time. Now it costs neither, *sometimes*, and nothing in `EXPLAIN` tells you which case you are in: a fused union and a fallback union print identically. The reason is logged at `DEBUG` on the driver and nowhere else. The partitioning half also has a deliberately strict equality rule — two `RangePartitioning`s with the same ordering and partition count are **not** equal, because their bounds were sampled independently — which is why a union of two globally sorted inputs still shuffles.

**Learn it with:**

1. **Book — Rioux Ch 7 (Python and SQL)** covers the `union` API and the column-padding you need before unioning DataFrames of different widths, which is the correctness prerequisite. It does not touch the physical operator.
2. **Spark-docs → Set Operators** ([sql-ref-syntax-qry-select-setops.html](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-setops.html)) — `UNION` vs `UNION ALL` and the `DISTINCT`/`ALL` variants; only `UNION ALL` is `UnionExec`, since `UNION` adds an aggregate above it
3. **Spark-docs → Runtime SQL Configuration** ([configuration.html#runtime-sql-configuration](https://spark.apache.org/docs/latest/configuration.html#runtime-sql-configuration)) — `spark.sql.unionOutputPartitioning`, `spark.sql.codegen.wholeStage.union.enabled`, `spark.sql.codegen.wholeStage.union.maxChildren`
4. **Source sweep — [sql/core — query-execution in the source map](reference/spark-source-map/sweeps/sql-core-query-execution.md)** — the eight disqualifiers in evaluation order (`partitioning-aware`, `nested-union`, `multi-rdd-child`, `partition-index-dependent-child`, `max-children-exceeded`, `columnar`, `type-mismatch`, plus the config), why fusion builds a raw `UnionRDD` to keep the partition→child mapping, and why the metrics map is registered only when fusion will actually run

**Milestone:** Build a union of two DataFrames that are already hash-partitioned on the same key, aggregate above it, and confirm from the plan that no exchange sits between the union and the aggregate — then set `spark.sql.unionOutputPartitioning=false` and show the exchange reappear. Separately, turn on `DEBUG` logging for `UnionExec`, run a union of six branches, and record which disqualifier (if any) your plan hit; raise `maxChildren` and show the log line change.

---


### ⬜ A43 — Attribute Identity: ExprId, DeduplicateRelations, and Ambiguous Self-Joins

> Discovered from source sweep (new topic): `sql/catalyst: Expression-ID assignment and attribute identity`

**What it is:** Every resolved column is an AttributeReference carrying a globally unique ExprId, and Spark's correctness rules are stated in terms of that ID rather than the column's name.

**Why you need it:** Ambiguous self-joins, a df.join(df) that silently evaluates its condition as always-true, and AMBIGUOUS_REFERENCE errors on a DataFrame that looks unambiguous all come from one invariant about expression IDs — knowing it turns a class of baffling failures into a predictable one.

**Learn it with:**

1. **Spark-docs → Name Resolution** ([sql-ref-name-resolution.html](https://spark.apache.org/docs/latest/sql-ref-name-resolution.html)) — the nine-rule resolution order and the `AMBIGUOUS_COLUMN_OR_FIELD` / `AMBIGUOUS_LATERAL_COLUMN_ALIAS` error conditions. This is the *name*-level view; the point of this topic is that names are the user-facing surface over an ID-level mechanism
2. **Source sweep — [sql/catalyst — analysis in the source map](reference/spark-source-map/sweeps/sql-catalyst-analysis.md)** — the concept "Expression-ID assignment and attribute identity". Read `ExpressionIdAssigner`'s own scaladoc first: it states the invariant (*no multi-child operator may have children with conflicting `AttributeReference` IDs*) and then shows the correct and incorrect plans for `t AS t1 CROSS JOIN t AS t2` side by side. Then `DeduplicateRelations`, the legacy enforcement — `newInstance()` on any `MultiInstanceRelation` whose output IDs collide with a sibling's
3. **Related — B7 (Joins)** for the self-join case in practice, and **A1** for where in the analyze phase this sits. Neither covers identity as a subject
4. **The configs to experiment with:** `spark.sql.analyzer.strictDataFrameColumnResolution` (4.2, on by default — stops a name resolving by `ExprId` across an unrelated plan), `spark.sql.analyzer.failAmbiguousSelfJoin`, `spark.sql.selfJoinAutoResolveAmbiguity`, `spark.sql.useCommonExprIdForAlias`

!!! warning "No book covers this"

    The Rioux book addresses self-join ambiguity as an API problem — alias your DataFrames — and the personal book's joins chapter follows it. Neither explains the mechanism, which is why the same fix sometimes fails and the error message sometimes names a column that looks perfectly unique.

!!! danger "The failure mode is a wrong answer, not an exception"

    If two scans of the same table both output `col1#0`, the join condition `col1#0 = col1#0` is valid, resolvable, and **always true** — so the query returns a cross product and throws nothing. This is why the invariant is asserted rather than checked at the point of use, and why it is worth being able to read expression IDs in `explain()` output.

**Milestone:** Run `df.join(df, df["id"] == df["id"])` on a small DataFrame and describe what you get — then explain from `explain()` which `#N` suffixes made it that. Repeat with `df.alias("l").join(df.alias("r"), F.col("l.id") == F.col("r.id"))` and show the IDs now differ. Then: state the one-sentence invariant `ExpressionIdAssigner` enforces, say why a *DataFrame* program needs an old-ID → new-ID mapping when a *SQL* query does not, and flip `spark.sql.analyzer.strictDataFrameColumnResolution` to false to produce a resolution that the 4.2 default rejects.

---


### ⬜ A44 — Type Conversion at the File Boundary: Widening, Unsigned Types, and Refused Reads

> Discovered from source sweep (new topic): `sql/core: Physical-to-Catalyst type conversion — widening, unsigned types, and the reads Spark refuses`

**What it is:** Each format decides independently which physical type may be read into which Catalyst type — Parquet's vectorized updater factory, its non-vectorized converter tree, Avro's deserializer and JDBC's getters all carry separate tables covering integer widening, unsigned types that do not fit a signed Java type, INT96 timestamps, decimal encodings and calendar rebasing, and each has its own way of refusing.

**Why you need it:** The refusals surface as runtime errors on specific files rather than analysis failures, the two Parquet readers do not accept the same conversions (so an unrelated column in the projection can decide whether the query works), and the conversions that succeed can change values — an unsigned int64 arrives as a decimal, a legacy-calendar date fails on the row that contains it, and an Avro int-into-long read is only allowed if a legacy flag is on.

**Learn it with:**

1. **Spark-docs → Parquet Files** ([sql-data-sources-parquet.html](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html)) — read the "Data Source Option" and configuration tables for the three rebase configs (`datetimeRebaseModeInRead/Write`, `int96RebaseModeInRead/Write`) and `parquet.fieldId.*`. The page documents the switches; it documents none of the conversions they govern
2. **Spark-docs → Avro Files** ([sql-data-sources-avro.html](https://spark.apache.org/docs/latest/sql-data-sources-avro.html)) — the "Supported types for Avro → Spark SQL conversion" table, which is the only official type-mapping table for any format, plus `spark.sql.legacy.avro.allowIncompatibleSchema`
3. **Spark-docs → Error Conditions** ([sql-error-conditions.html](https://spark.apache.org/docs/latest/sql-error-conditions.html)) — look up `AVRO_INCOMPATIBLE_READ_TYPE` and `FAILED_READ_FILE.PARQUET_COLUMN_DATA_TYPE_MISMATCH` (a *sub-condition* of the generic read failure, which is why it is easy to miss); both message texts name the expected and actual types, which is how you diagnose these in production
4. **Source sweep — [sql/core — datasources in the source map](reference/spark-source-map/sweeps/sql-core-datasources.md)** — the concepts "Physical-to-Catalyst type conversion", "ParquetRowConverter" and "Avro record conversion": `ParquetVectorUpdaterFactory.getUpdater` is the vectorized type matrix and `ParquetRowConverter.newConverter` is the row-based one, they disagree, and `failIfRebase` is checked **per value** so a legacy-calendar date fails on the row that contains it
5. **Related topics** — **E25** covers the other axis, which *column* is matched to which; **A44** is about what happens to the value once the column is matched. Read them together

!!! warning "No book covers this"

    Neither Rioux nor the personal book goes below the format API. The docs give you the configs and
    one type table; everything about *refusal* is source-only.

!!! info "It is not one rule, it is four"

    Parquet-vectorized, Parquet-row-based, Avro and JDBC each carry their own conversion table, and
    ORC delegates to Hive's. "Spark can read an int column as a bigint" is true of some of those
    paths and not others, and which path runs is decided by the projection and the config — not by
    anything you wrote in the query.

**Milestone:** Write a Parquet file with an `INT32` column, then read it with an explicit schema declaring that column `DOUBLE` — once with a plain projection (vectorized) and once with a nested struct also in the projection (row-based) — and show one succeeds and the other raises `PARQUET_COLUMN_DATA_TYPE_MISMATCH`. Then write a file with `spark.sql.parquet.datetimeRebaseModeInWrite=LEGACY` containing a pre-1582 date, read it back under `datetimeRebaseModeInRead=EXCEPTION`, and say at what point in the job the failure arrives and why it is not a planning error.

---


### ⬜ A45 — Writing a Streaming Sink: the DSv2 StreamingWrite Path and Epoch-Id Idempotence

> Discovered from source sweep (new topic): `sql/core: The DSv2 streaming write path — MicroBatchWrite, V2Writes, and the V1 marker that gets deleted`

**What it is:** A streaming sink is a DSv2 `SupportsWrite` table whose `StreamingWrite` is wrapped per batch in a `MicroBatchWrite` carrying that batch's id, so the ordinary batch write machinery — writer factory per partition, task-side `commit`/`abort`, driver-side `commit(epochId, messages)` — executes each micro-batch, while the older DSv1 `Sink.addBatch` path survives as a marker node that the streaming optimizer deletes.

**Why you need it:** Every custom sink, every `foreachBatch` alternative and every "my sink wrote the batch twice" incident lives here: the batch id handed to `commit` is the *only* thing that makes a sink idempotent across the replay that the checkpoint protocol guarantees, and the two write paths (V1 and V2) differ in whether Spark can invalidate the relation, refresh the catalog, or report commit progress at all.

**Learn it with:**

1. **Spark-docs → Output Sinks** ([streaming/apis-on-dataframes-and-datasets.html#output-sinks](https://spark.apache.org/docs/latest/streaming/apis-on-dataframes-and-datasets.html#output-sinks)) — the built-in sinks and, in the table, which of them are fault-tolerant and on what condition. Read it for the *contract* a sink is expected to meet, not for how one is written; the docs never describe the write path
2. **Spark-docs → Using Foreach and ForeachBatch** ([streaming/apis-on-dataframes-and-datasets.html#using-foreach-and-foreachbatch](https://spark.apache.org/docs/latest/streaming/apis-on-dataframes-and-datasets.html#using-foreach-and-foreachbatch)) — the two user-facing extension points, and the one paragraph that matters here: `foreachBatch` gives you `(df, batchId)` and tells you to use `batchId` for deduplication. That instruction is the API-level shadow of everything below
3. **No book covers this.** Rioux stops at `writeStream.format(...)`; SDG and LS2e describe sinks as a list of options rather than an interface you implement. Custom-sink material online is mostly pre-DSv2 and describes `Sink.addBatch`, which is now the *legacy* path
4. **Source sweep — [sql/core — streaming execution in the source map](reference/spark-source-map/sweeps/sql-core-streaming-exec.md)** — the substantive reference. Read "The DSv2 streaming write path": how `MicroBatchExecution` picks `WriteToMicroBatchDataSource` (V2) or `WriteToMicroBatchDataSourceV1` (legacy), how `V2Writes` wraps the write as `MicroBatchWrite(batchId, …)` so the batch id is closed over in every `commit`, and why the V1 marker node has no physical plan at all. `ForeachWriterTable` on the same page is the smallest complete `SupportsWrite` in the tree and the best template to copy
5. **Prerequisite:** **A36** — the batch id is only an idempotence key because the checkpoint protocol guarantees a crashed batch re-runs with the *same* id. Without that, `commit(epochId, …)` is just a number
6. **Related:** **E17** (output commit coordination — the batch-write machinery a streaming write inherits), **E23** (DSv2 catalog transactions, which is why a transactional sink is re-resolved per batch), **A35** (the Python data source API, the same contract without the JVM)

!!! warning "A V1 sink is quietly less capable, and 4.2.0 made one of the gaps fatal"

    A legacy `Sink` reports **no** commit progress (`numOutputRows` is simply absent), cannot have
    its relation invalidated — `MicroBatchExecution` refreshes the catalog table by hand instead —
    and since Spark 4.2.0 is **rejected outright** under a `RealTimeTrigger` with
    `STREAMING_REAL_TIME_MODE.SINK_NOT_SUPPORTED`, because it cannot write row by row. If you are
    writing a sink today, write a DSv2 `SupportsWrite` table.

**Milestone:** Write a DSv2 streaming sink: a `Table` with `STREAMING_WRITE` capability whose `StreamingWrite.commit(epochId, messages)` records the epoch id somewhere durable and skips the write when it has already seen that id. Run it against a source, kill the query mid-batch, restart, and show from your own log that the same batch id arrived twice and the second one was skipped. Then state two things: where in the plan the batch id was attached, and what `query.lastProgress.sink` reports for your sink versus for a `foreachBatch` sink.

---

## Expert

**Goal:** Architect production data platforms. Understand Spark internals deeply enough to reason about memory, serialisation, and execution without the Spark UI. Build governed, observable, CI/CD-deployed pipelines.

**Estimated time to complete this level:** 40–60+ hrs (ongoing)

**Reading order:** E1 → E2 → E3 → E4 → E5 → E6 → E7 → E8 → E9. E10–E17 sit around the closing checkpoint as source-derived depth — not required to pass it, read on demand.

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
6. **Source sweep — [core — rdd-layer in the source map](reference/spark-source-map/sweeps/core-rdd-layer.md)** — `TorrentBroadcast`'s block protocol, `ContextCleaner`'s GC-driven cleanup, the `AccumulatorV2` copy/merge lifecycle, and Kryo vs Java serializer construction
7. **Source sweep — [core — execution engine in the source map](reference/spark-source-map/sweeps/core-execution-engine.md)** — the task lifecycle on the executor, `TaskContext` completion listeners, the result-size decision, and the kill path that turns an uninterruptible task into a lost slot. Re-swept 2026-08-09, adding the three layers underneath: the `Task` / `TaskDescription` pair (the RDD travels as a broadcast `taskBinary`, and the description is hand-encoded so the executor can set the classpath *before* deserializing the task), the driver↔executor message protocol, and the single-threaded `DAGScheduler` event loop whose `messageProcessingTime` timer is the only signal that the driver, not the cluster, is the bottleneck
8. **Source sweep — [core — shuffle & memory in the source map](reference/spark-source-map/sweeps/core-shuffle-memory.md)** — the memory system end to end — pool sizing, the execution/storage asymmetry, the acquire/spill loop, Tungsten pages, and the leak detection that is suppressed exactly when leaks are likeliest
9. **Source sweep — [core — storage & serialization in the source map](reference/spark-source-map/sweeps/core-storage-serializer.md)** — the block manager's read and write paths, the lock protocol underneath them, and how a block that is reported but unreadable retracts itself from the driver's registry. Also the RPC plane the whole layer runs on: `BlockManagerMaster` sends nearly every operation as a **blocking `askSync` to one driver endpoint**, with heartbeats split onto a second endpoint purely for latency; the executor side answers on an isolated endpoint that offloads removals to a 100-thread pool. And `BlockManagerManagedBuffer`, which makes Netty's retain/release *be* the block's read lock — so a slow remote reader keeps a served block un-evictable
10. **Source sweep — [core — rpc & resources in the source map](reference/spark-source-map/sweeps/core-rpc-resources.md)** — the messaging substrate every driver↔executor exchange rides: `RpcEnv`/`Dispatcher`/`Inbox` and the shared-vs-dedicated `MessageLoop` threading, local-shortcut vs `Outbox` remote routing, and the `RpcTimeout` fallback chain that explains why a stalled heartbeat surfaces as a `spark.network.timeout` error
11. **Source sweep — [sql/catalyst — optimizer in the source map](reference/spark-source-map/sweeps/sql-catalyst-optimizer.md)** — the serialisation boundary from the *plan* side: `EliminateSerialization` removes the deserialize→serialize round trip between two typed `Dataset` operations, `ObjectSerializerPruning` narrows the encoder, and `ReassignLambdaVariableID` is what makes two structurally identical plans canonicalize equal so exchange/subquery reuse can fire. Note that none of this applies to PySpark — a Python UDF is extracted into its own eval node instead
12. **Source sweep — [sql/catalyst — planner in the source map](reference/spark-source-map/sweeps/sql-catalyst-planner.md)** — the `planLater` placeholder mechanism that lets a strategy plan one operator without knowing how its children will execute, and the cartesian fold over placeholders that makes planning time explode if a custom strategy returns several candidates per operator
13. **Source sweep — [sql/catalyst — expressions in the source map](reference/spark-source-map/sweeps/sql-catalyst-expressions.md)** — the execution engine at expression level: the `UnsafeRow` layout (null bitmap, one 8-byte word per field regardless of type, variable-length tail), the Janino compile path and its 100-entry class cache, the whole-stage `produce`/`consume` protocol and its fallbacks, and `objects.scala` — the deserialize/call/serialize sandwich that is the real cost of every typed `Dataset.map`
14. **Source sweep — [sql/catalyst — types & parser in the source map](reference/spark-source-map/sweeps/sql-catalyst-types-parser.md)** — the type system's internal view: `PhysicalDataType` is the storage-and-ordering projection where several logical types collapse into one (`DateType`, `YearMonthIntervalType` and `IntegerType` are all `PhysicalIntegerType`) and where every sort's `Ordering` comes from. Also the 4.2.0 **Types Framework** (`catalyst/types/ops/`), the seam new types will arrive through, currently behind a test-only flag
15. **Source sweep — [sql/catalyst — framework in the source map](reference/spark-source-map/sweeps/sql-catalyst-framework.md)** — two internals this topic needs and does not have: `ExpressionEncoder`, whose serializer and deserializer are ordinary `objects/` expression trees (so an encoder bug is a generated-Java compile error, not a serialization exception) and whose `AgnosticEncoder` half lives in `sql/api` so a Connect client can hold one; and `Origin` / `CurrentOrigin`, the `ThreadLocal` behind the Spark 4 error messages that point at the line of your DataFrame code
16. **Source sweep — [sql/core — query-execution in the source map](reference/spark-source-map/sweeps/sql-core-query-execution.md)** — the row/object and row/columnar boundaries in one place: `DeserializeToObjectExec` / `SerializeFromObjectExec` excluding typed lambdas from codegen because a JVM object cannot live in an `UnsafeRow`, `ColumnarToRowExec` / `RowToColumnarExec` and the `ColumnarRule` extension point, and `SortExec`'s off-heap `UnsafeExternalRowSorter` with its prefix comparator and radix-sort precondition
17. **Source sweep — [sql/core — joins in the source map](reference/spark-source-map/sweeps/sql-core-joins-exec.md)** — the memory story of a join in one place: `LongToUnsafeRowMap.spill` returns a hard-coded `0L` and `UnsafeHashedRelation` frees its map and throws on a failed acquisition — **hash joins do not spill, at all** — while sort-merge join and cartesian product spill through `ExternalAppendOnlyUnsafeRowArray`. That asymmetry is the real reason `preferSortMergeJoin` defaults to true. Also long-key packing: integral keys summing to ≤ 8 bytes collapse into one `Long` and get an array-backed map that can go dense
18. **Source sweep — [sql/core — aggregation, windows and exchange in the source map](reference/spark-source-map/sweeps/sql-core-agg-window-exchange.md)** — three memory mechanisms in the SQL layer, at source level: the codegen fast hash map's fixed-capacity two-step probe, `TungstenAggregationIterator`'s spill-and-switch, and `WindowSegmentTree`'s private `MemoryConsumer` — a compact worked example of registering heap-only state with the `TaskMemoryManager` and honouring the spill contract
19. **Source sweep — [sql/core — Python and Arrow in the source map](reference/spark-source-map/sweeps/sql-core-python-arrow.md)** — three process-boundary mechanisms worth knowing at source level: the `HybridRowQueue`'s page format (including its `-1` end-of-page marker) and `MemoryConsumer` spill contract; the columnar Arrow input path, which selects vectors into a temporary `VectorSchemaRoot` and unloads them with **no copy of vector data**; and Arrow IPC compression, whose codec must be constructed directly because the factory overload silently drops the configured zstd level
20. **Source sweep — [sql/core — streaming execution in the source map](reference/spark-source-map/sweeps/sql-core-streaming-exec.md)** — the memory story of stateful streaming: `HDFSBackedStateStoreProvider` keeps the **entire state map for a partition in JVM heap**, which is the whole reason the RocksDB provider exists; RocksDB instead uses native memory and local disk with a shared block-cache budget. See **E27** for the engine
21. **Source sweep — [sql/core — the classic API in the source map](reference/spark-source-map/sweeps/sql-core-classic-api.md)** — the memory layout of the in-memory cache: `ColumnType[JvmType]` is a per-type binary codec (a decimal is one of two very different encodings depending on precision), `ColumnBuilder` writes into `ByteBuffer`s, and `GenerateColumnAccessor` generates the code that turns a cached batch back into rows. Reading the type list is the most direct way to reason about what caching a given schema costs

**Milestone:** You can explain the difference between execution memory and storage memory in unified memory management, and name two causes of excessive GC in PySpark that the task memory metrics would surface.

!!! info "Runtime baseline as of Spark 4.2.0"
    Spark 4.2.0 builds and runs on **Java 25** ([SPARK-51167]) and is Scala 2.13-only — Scala 2.12 support was dropped across the whole Spark 4 line. GC behaviour on a modern JVM differs materially from what SDG Ch 19 (written against Java 8/11) describes, so treat its specific GC-flag advice as dated and verify against your own runtime.

---

### ⬜ E2 — Production Deployment: Cluster Management and Scaling

**What it is:** Cluster managers (YARN, Kubernetes, Databricks, standalone); driver and executor sizing; dynamic allocation; auto-scaling; `spark-submit` configuration; deploy modes (client vs cluster). Plus the **cluster-security surface** that comes with any real deployment: the shared authentication secret, wire-level RPC/shuffle encryption, TLS, local-disk (shuffle-spill) encryption, and Kerberos/delegation-token lifecycle for secured Hadoop.

**Why you need it:** A job that works on a laptop breaks on a cluster in ways that require understanding how the cluster manager allocates resources.

**Learn it with:**

1. **SDG Ch 15–17** — cluster execution, deploying Spark, resource management
2. **ADEB Module 3** — instance type selection for performance
3. **Spark-docs → Cluster Mode Overview** ([spark.apache.org/docs/latest/cluster-overview.html](https://spark.apache.org/docs/latest/cluster-overview.html))
4. **Spark-docs → Kubernetes** ([spark.apache.org/docs/latest/running-on-kubernetes.html](https://spark.apache.org/docs/latest/running-on-kubernetes.html)) — the direction production Spark is moving
5. **Source sweep — [core — execution engine in the source map](reference/spark-source-map/sweeps/core-execution-engine.md)** — executor registration, the offer loop, decommissioning as a graceful drain, and dynamic allocation's target arithmetic
6. **Source sweep — [core — shuffle & memory in the source map](reference/spark-source-map/sweeps/core-shuffle-memory.md)** — the external shuffle service — how it makes map output survive executor loss, and its hardcoded five-second registration retry
7. **Source sweep — [core — storage & serialization in the source map](reference/spark-source-map/sweeps/core-storage-serializer.md)** — block replication and its topology requirement, executor loss and proactive re-replication, the disk layout, and decommission migration with fallback storage. Also the registration handshake: an executor registers with a topology-less `BlockManagerId` and must adopt the one the driver returns, because `topologyInfo` is part of equality; a rejected **re-**registration comes back as the `INVALID_EXECUTOR_ID` sentinel and the executor kills itself. Three different things are modelled as a `BlockManagerId` — an executor, the driver, and a push-based-shuffle merger — plus the fallback-storage placeholder
8. **Source sweep — [core — submit & standalone in the source map](reference/spark-source-map/sweeps/core-submit-standalone.md)** — the submission path end to end, standalone placement arithmetic, and the graceful worker drain that a rolling restart depends on
9. **Source sweep — [core — config & security in the source map](reference/spark-source-map/sweeps/core-config-security.md)** — the whole cluster-security surface: how `SecurityManager` mints the auth secret differently per cluster manager (generated on YARN/local, mounted file on k8s, *required in conf* otherwise), the `AuthEngine` X25519 handshake and its SASL fallback, IO (shuffle-spill) encryption, and the Kerberos delegation-token renewal loop — plus the config engine itself (fallback keys, `${…}` substitution, deprecated-key handling) that every knob in this topic is built on
10. **Source sweep — [core — rpc & resources in the source map](reference/spark-source-map/sweeps/core-rpc-resources.md)** — the resource model behind executor/task sizing: how `spark.executor.cores` + `spark.task.cpus` + custom `spark.*.resource.{name}.amount` combine into the *limiting-resource* arithmetic that decides how many tasks an executor runs, and how accelerator addresses get discovered (resources file vs discovery script). Stage-level scheduling proper is its own topic — see [A16](#a16-stage-level-scheduling-and-accelerator-aware-resources-gpufpga)
11. **Source sweep — [sql/connect — client-server in the source map](reference/spark-source-map/sweeps/sql-connect-client-server.md)** — Connect as a deployment surface: the gRPC server is a **`SparkPlugin`** started inside an ordinary driver (`spark.plugins`), its port and bind address are *static* confs fixed at start, authentication is one pre-shared bearer token, and anything stronger has to come from a custom `ServerInterceptor` or a proxy in front. Treat an unprotected Connect port as equivalent to an unprotected driver
12. **Source sweep — [connector/profiler — async-profiler in the source map](reference/spark-source-map/sweeps/connector-profiler-async-profiler.md)** — a deployment-shaped gotcha worth knowing before someone asks for profiling in production: the module lives behind the `jvm-profiler` Maven profile and its `ap-loader-all` native dependency is `provided` scope, so **a standard Spark distribution ships neither**. On Kubernetes it additionally requires `spark.kubernetes.executor.deleteOnTermination=false`, because pods are otherwise reclaimed while the profiler's final flush is still running
13. **Source sweep — [sql/hive — hive-metastore in the source map](reference/spark-source-map/sweeps/sql-hive-hive-metastore.md)** — two deployment-shaped facts: the six metastore-interop configs are all **static**, so attaching Spark to an existing metastore is a submit-time decision that cannot be corrected on a running session; and on a Kerberised cluster `HiveDelegationTokenProvider` logs a **warning** and returns no token when acquisition fails, surfacing much later as an executor authentication error
14. **Source sweep — [sql/core — streaming execution in the source map](reference/spark-source-map/sweeps/sql-core-streaming-exec.md)** — two operational surfaces: `CheckpointFileManager` is the atomic-rename abstraction the whole protocol assumes and is pluggable per scheme — which matters on object storage that does not provide it — and 4.1.0's `ChecksumCheckpointFileManager` decorates any implementation with a `CRC32C` sibling file, on by default, defending against the truncated-log-entry failure mode
15. **Source sweep — [resource-managers/kubernetes — driver & executor in the source map](reference/spark-source-map/sweeps/resource-managers-kubernetes-driver-executor.md)** — **the Kubernetes half of this topic**, and the first source-derived material behind it: 47 files, 89 configs, six of them new in 4.2.0. The frame first — Spark never asks Kubernetes for N executors, it maintains a target and reconciles against snapshots — then five operational facts. (i) **Only the `direct` allocator implements most of the config surface**: `statefulset` and `deployment` never subscribe to the snapshot store, so batch size, all three pending caps and PVC reuse simply do not apply to them. (ii) `spark.kubernetes.allocation.maximum` counts **executor ids ever issued**, not live pods, so a long dynamic-allocation job eventually dies on it. (iii) A single un-acknowledged pod blocks *all* further requests for its resource profile until a ≥600 s timeout, which is why K8s scale-up stalls in bursts. (iv) Exit code **137** is annotated `(SIGKILL, possible container OOM)` by a hand-written table — the fastest `memoryOverhead`-sizing diagnostic there is, and distinct from 52 (JVM heap OOM). (v) A pod *deleted* is `exitCausedByApp = false` and does not burn `spark.executor.maxNumFailures`, while a pod *failed* does. Reconciliation itself is topic **E33**; the 4.2.0 resize plugins are **E34**
16. **Source sweep — [resource-managers/kubernetes — auth & networking in the source map](reference/spark-source-map/sweeps/resource-managers-kubernetes-auth-networking.md)** — the other half, and it completes the subsystem. Deployment-shaped facts: `spark.driver.host` and `spark.driver.bindAddress` are **rejected outright** on Kubernetes because a headless Service manages both; that Service publishes four ports including the Spark Connect gRPC endpoint; and the driver's readiness wait in the allocator exists precisely because the Service is not DNS-resolvable until then. **Upgrade trap for 4.2.0:** the new `NetworkPolicyFeatureStep` has no config gating it, so a submission service account without `create networkpolicies` now fails *after* creating the driver pod — and the failure path deletes that pod. Identity and RBAC proper are topic **E35**
17. **Source sweep — [resource-managers/yarn — AM & executor allocation in the source map](reference/spark-source-map/sweeps/resource-managers-yarn-am-executor.md)** — **the YARN half of this topic**, and the subsystem's only group: 29 files, 61 configs, two new in 4.2.0. The frame first — unlike Kubernetes, YARN is a **request/response protocol, not a reconciliation loop**: one `Reporter` thread calls `allocate()` per round, and that call doubles as the liveness heartbeat. Then the operational facts. (i) `ApplicationMaster` is one class running two unrelated processes — in cluster mode it *is* the driver host and runs user `main` on a side thread, in client mode it is a bare allocator — and almost every branch in it is `isClusterMode`. (ii) The allocation loop polls **faster** when work is pending: `spark.yarn.scheduler.initial-allocation.interval` (200 ms) is the *shortest* sleep and doubles toward `spark.yarn.scheduler.heartbeat.interval-ms` (3 s), which is itself silently capped at half of YARN's AM expiry. (iii) Leaving `spark.yarn.jars` unset makes every submit zip and upload `$SPARK_HOME/jars` — one WARN line, several seconds, and the resources end up `PRIVATE` because the staging dir is `700`. (iv) YARN **decommissioning is disabled whenever the external shuffle service is enabled** (SPARK-39018), so the two features you would want together are mutually exclusive. (v) YARN alone overrides `minRegisteredRatio` to **0.8**, which is the unexplained pause between "application RUNNING" and the first task. Placement is topic **E36**, AM attempts **E37**, the web proxy **E38**, classpath order **E39**
18. **Source sweep — [connector/kafka-0-10 — consumer in the source map](reference/spark-source-map/sweeps/connector-kafka-0-10-consumer.md)** — one deployment-shaped detail this topic's Kerberos section needs: Kafka authentication for **both** connectors is injected at a single point, `KafkaConfigUpdater.setAuthenticationConfigIfNeeded`, which lives in the separate `connector/kafka-0-10-token-provider` module and is called once on the driver (module name `"source"`) and once per executor consumer (`"executor"`). Its entire configuration surface — `spark.kafka.clusters.<id>.{auth.bootstrap.servers, target.bootstrap.servers.regex, security.protocol, sasl.kerberos.service.name, sasl.token.mechanism, ssl.*}` — is read through `getAllWithPrefix` with **no `ConfigBuilder` anywhere**, so none of it appears in the generated configuration tables or in the source map's config catalog. The `target.bootstrap.servers.regex` key is the one that decides which cluster's token a given `bootstrap.servers` gets, and a regex that matches nothing produces no token and an authentication failure at first connect rather than at submit
19. **Source sweep — [connector/kafka-0-10-token-provider — auth in the source map](reference/spark-source-map/sweeps/connector-kafka-0-10-token-provider-auth.md)** — a worked example of this topic's delegation-token half, in the smallest module in the map. It is the shape to recognise: a `HadoopDelegationTokenProvider` found by `ServiceLoader`, gated by `spark.security.credentials.kafka.enabled`, minting one token per configured cluster on the driver at submit time and returning the **lowest** next-renewal date so the manager's renewal thread wakes for whichever expires first. Three transferable facts. **Every failure is a warning and submission continues** — two nested `catch NonFatal` blocks, so an unreachable broker or a bad keytab produces no token and an executor-side authentication error much later; the same silent-degradation shape already recorded for Hive and for Kubernetes Kerberos. **The three login paths have a fixed precedence** — JVM-global JAAS, then keytab, then ticket cache — and the first one, if present, skips the rest at DEBUG only. And **the per-service enable key is built by `String.format`** (`spark.security.credentials.%s.enabled`), which is why it appears in no configuration table
20. **Source sweep — [streaming — DStream in the source map](reference/spark-source-map/sweeps/streaming-dstream.md)** — a second dynamic-allocation policy, and one that is **mutually exclusive with the one this topic teaches**: enabling both `spark.dynamicAllocation.enabled` and `spark.streaming.dynamicAllocation.enabled` throws `IllegalArgumentException` at startup, naming the config to turn off. It exists because core's policy keys on executor idleness and a micro-batch job never leaves an executor idle for long, so the streaming policy scales on `avgBatchProcTime / batchDuration` instead — up at ≥ 0.9, down at ≤ 0.3, evaluated every 60 s. Two operational details: scale-down picks a **random** executor from those not hosting a receiver (and decommissions rather than kills when `spark.decommission.enabled` is set), and the minimum defaults to `max(1, numReceivers)`, so receivers set the floor. The seven `spark.streaming.dynamicAllocation.*` keys are declared in `core` but implemented entirely in the streaming module

!!! warning "The auth secret is not optional on many cluster managers — and the UI is open by default"

    `SecurityManager.initializeAuth` mints the shared secret *differently per master*: on `yarn` and
    `local[*]` it generates one; on Kubernetes it reads a mounted secret file; on **any other
    master** (including standalone) it `require`-fails unless `spark.authenticate.secret` (or a
    secret file) is already set — enabling `spark.authenticate` without providing the secret crashes
    startup, it does not silently disable auth. Separately, the Web UI ships **open**:
    `spark.acls.enable` defaults to false, so every view/modify permission check returns true until
    you turn ACLs on. And enabling both network-crypto and RPC-SSL silently disables network-crypto
    (SSL wins, warning logged) — the two are mutually exclusive. The
    [config & security source sweep](reference/spark-source-map/sweeps/core-config-security.md) traces
    each of these paths.

!!! warning "The standalone Master runs an unauthenticated submission endpoint by default"

    `spark.master.rest.enabled` defaults to **`true`** on the server side, so a standalone Master listens on `spark.master.rest.port` for `POST /v1/submissions/create`. The server has no authentication of its own — `spark.master.rest.filters` is the only hook, and the client's base URL is hardcoded `http://` with no HTTPS submission path. Anyone who can reach that port can submit a driver that runs arbitrary code as the Spark user. (Confusingly, the *client* reads the same key with a default of `"false"`, so your own `--deploy-mode cluster` submits take the legacy RPC path while the endpoint stays open.) The documented fix is a signed-token filter, not just closing the port: `spark.master.rest.filters=org.apache.spark.ui.JWSFilter` with `spark.org.apache.spark.ui.JWSFilter.param.secretKey=<BASE64URL key>`, which requires an `Authorization: Bearer` header on every request — the same filter secures the Web UI and its `/api/v1` endpoints via `spark.ui.filters`. Set the REST config to `false` if you do not need remote submission at all. Traced in the [submit & standalone](reference/spark-source-map/sweeps/core-submit-standalone.md) and [monitoring](reference/spark-source-map/sweeps/core-monitoring.md) source sweeps.

!!! warning "Graceful worker drain is off by default"

    Decommissioning a standalone worker is what lets a rolling restart or a spot reclamation avoid
    recomputation: the Master reports each executor with its host so drivers unregister that host's
    shuffle map output, instead of discovering it later as fetch failures. But
    `spark.decommission.enabled` defaults to **false**, which means the `SIGPWR` handler is never
    installed — so sending the decommission signal to a worker started without it simply kills the
    process, and you pay for the lost shuffle output. Note also that a decommissioning worker
    refuses new executors but still accepts new drivers.

!!! info "Standalone placement has two defaults worth checking"

    `spark.deploy.defaultCores` is unlimited, so the first application to register claims every core
    in the cluster unless it sets `spark.cores.max`. And `spark.worker.timeout` drives two timers in
    two different processes — the worker heartbeats at a quarter of it, the Master sweeps at it — so
    setting it on one side only either leaves dead workers registered or reaps healthy ones, with
    nothing cross-checking the two values.
**Milestone:** You can size a Spark cluster for a given workload (number of executors, cores per executor, memory), explain the difference between client and cluster deploy mode, and configure dynamic allocation.

---

### ⬜ E3 — Observability: Monitoring, Alerting, and Logging

**What it is:** Spark History Server; Spark metrics system; structured logging from drivers and executors; custom listeners; alerting on job duration regressions; Spark UI on completed jobs.

**Why you need it:** Production pipelines fail at 3am. Observability is the difference between "we have an alert" and "we found out from an angry user".

**Learn it with:**

1. **SDG Ch 18** — monitoring and debugging; the Spark metrics system
2. **ADEB Module 3** — pipeline event logging; monitoring in the Databricks context
3. **Spark-docs → Monitoring** ([spark.apache.org/docs/latest/monitoring.html](https://spark.apache.org/docs/latest/monitoring.html)) — the UI, the REST API (`/api/v1`), the metrics system and its sinks, and the History Server, end to end. Pair with **Spark-docs → Web UI** ([spark.apache.org/docs/latest/web-ui.html](https://spark.apache.org/docs/latest/web-ui.html)) for the page-by-page tour
4. **Source sweep — [core — monitoring in the source map](reference/spark-source-map/sweeps/core-monitoring.md)** — the whole observability spine: the `AppStatusListener → ElementTrackingStore → AppStatusStore(KVStore)` indirection that both the live UI *and* the History Server read (they never touch live objects), the async event-queue drop path where monitoring data is silently lost, the metrics registration-by-reflection and its sinks, the **two** Prometheus surfaces, and the History Server's replay/compaction/cleaning lifecycle — with the retention and compaction caps that make history lossy by design
5. **Source sweep — [core — execution engine in the source map](reference/spark-source-map/sweeps/core-execution-engine.md)** — the heartbeat protocol and its expiry, the executor metrics poller, and the listener events every monitoring tool consumes. Re-swept 2026-08-09: `ExecutorMetrics` holds **peaks**, not samples, and `ProcfsMetricsGetter` — the only source of child-process (PySpark worker) memory — is off by default, silently no-ops off Linux, and disables itself permanently after one failed read. An alert built on those figures is measuring less than it looks like
6. **Source sweep — [core — shuffle & memory in the source map](reference/spark-source-map/sweeps/core-shuffle-memory.md)** — which shuffle path a job actually took, and why none of it is visible at default log levels
7. **Source sweep — [core — submit & standalone in the source map](reference/spark-source-map/sweeps/core-submit-standalone.md)** — what standalone actually exposes: four master gauges, five worker gauges, and the states that have no metric at all
8. **Source sweep — [core — config & security in the source map](reference/spark-source-map/sweeps/core-config-security.md)** — the two observability-security surfaces this topic must not get wrong: **secret redaction** (which surfaces `spark.redaction.regex` scrubs — Environment page, event log, YARN logs — vs `spark.redaction.string.regex` for SQL explain only), and **UI/History ACLs** (open by default, wildcard semantics, the separate History-server switch)
9. **Source sweep — [core — rdd-layer in the source map](reference/spark-source-map/sweeps/core-rdd-layer.md)** — `RDDOperationScope`, the presentational layer behind the DAG visualization, and `ContextCleaner`'s weak-reference cleanup loop with its periodic `System.gc()` — both are things you will see in a driver thread dump and neither is documented elsewhere
10. **Source sweep — [core — storage & serialization in the source map](reference/spark-source-map/sweeps/core-storage-serializer.md)** — the 4.2.0 block log writers: `RollingLogWriter` stores log output as `BlockManager` blocks, rolling every 32 MiB, which is the mechanism behind retrievable PySpark worker logs. Also the cost of the `BlockManager.*` metric source: its **eleven gauges each issue a separate synchronous driver RPC**, and every one rebuilds a full `StorageStatus` — block map copy included — for every block manager in the cluster. Nothing caches or batches it, so one metrics poll scales with executors × blocks
11. **Source sweep — [core — api-bridge in the source map](reference/spark-source-map/sweeps/core-api-bridge.md)** — the executor half of PySpark worker logging: capture is a `PYTHON_WORKER_LOGGING:` marker scan over the worker's **stdout only**, active only when `PYSPARK_SPARK_SESSION_UUID` is set — so an unmarked `print()` still goes to the executor log, and a traceback on stderr is never captured
12. **Source sweep — [sql/connect — client-server in the source map](reference/spark-source-map/sweeps/sql-connect-client-server.md)** — Connect's observability story: every execution carries a user id, a session id and an operation id and posts started/analyzed/readyForExecution/finished events to the listener bus, so "which user ran the query that filled the driver" is answerable from the event log — better than a classic multi-tenant driver, where jobs are attributable only by job group. The Connect server tab and its History Server plugin read the same events
13. **Source sweep — [sql/connect — declarative pipelines in the source map](reference/spark-source-map/sweeps/sql-connect-declarative-pipelines.md)** — pipeline progress as an event stream: `PipelineEventSender` runs on a daemon thread with a bounded queue, and above `spark.sql.pipelines.event.queue.capacity` (1000) it **drops intermediate events with no log line and no gap marker**. Run outcomes and terminal flow events always survive, so the final report stays correct — but tooling that counts intermediate events will under-count on a large pipeline
14. **Source sweep — [connector/profiler — async-profiler in the source map](reference/spark-source-map/sweeps/connector-profiler-async-profiler.md)** — the observability tool this topic does not currently mention and the official docs do not either: Spark ships an **async-profiler plugin** (`org.apache.spark.profiler.ProfilerPlugin`, since 4.0) that captures wall-clock, allocation and lock profiles from driver and executor JVMs as JFR files and syncs them to a DFS path. Metrics tell you a stage is slow; this tells you which method. Note it is not built by default and `monitoring.html` never mentions it — see the proposed **E20** for the full treatment
15. **Source sweep — [sql/core — query-execution in the source map](reference/spark-source-map/sweeps/sql-core-query-execution.md)** — what a SQL execution actually emits: `SparkListenerSQLExecutionStart`/`End`, the root-execution id that ties nested executions together, the `spark.sql.event.truncate.length` truncation applied to the query text, and `SQLEventFilterBuilder` — the rule that decides which SQL events survive event-log compaction
16. **Source sweep — [sql/core — Python and Arrow in the source map](reference/spark-source-map/sweeps/sql-core-python-arrow.md)** — the observability surface of the Python side: the `PythonSQLMetrics` set in the SQL tab, `PythonWorkerLogsExec` reading worker log blocks back out of the `BlockManager` as a queryable table, and the worker-diagnostic flags the SQL layer owns — `faulthandler.enabled`, `idleTimeoutSeconds` / `killOnIdleTimeout`, `tracebackDumpIntervalSeconds`, and the `hideTraceback` / `simplifiedTraceback` pair that decides how much Python traceback reaches the JVM exception
17. **Source sweep — [sql/core — streaming execution in the source map](reference/spark-source-map/sweeps/sql-core-streaming-exec.md)** — what to monitor and where it comes from: `ProgressReporter`'s `durationMs` map separates `triggerExecution` from `walCommit` from `addBatch`, so a slow batch is decidable rather than guessable; `numRowsDroppedByWatermark` is the otherwise-silent count of late rows; and the state-store coordinator reports **snapshot upload lag** per instance, which is how you find the one partition that will take an hour to recover
18. **Source sweep — [resource-managers/yarn — AM & executor allocation in the source map](reference/spark-source-map/sweeps/resource-managers-yarn-am-executor.md)** — the YARN observability surface, which is smaller than you would expect and fails quietly. The AM registers exactly **five** gauges (`numExecutorsFailed`, `numExecutorsRunning`, `numReleasedContainers`, `numLocalityAwareTasks`, `numContainersPendingAllocate`) under its own `MetricsSystem` instance started with `start(false)` — deliberately **without** the static sources every other instance registers (SPARK-25277) — and namespaced by `spark.yarn.metrics.namespace`, defaulting to the application id. Container log URLs and the nine executor attributes behind `spark.ui.custom.executor.log.url` are built by `YarnContainerInfoHelper`, whose two entry points wrap everything in a `try` that logs at **INFO** and returns `None`: a missing `NM_HTTP_PORT` produces a UI with no log links and one line to explain it. Rolled-log aggregation patterns are set at submission and an unsupported YARN version downgrades them to a warning. Application-report polling is its own channel — `spark.yarn.report.loggingFrequency` (30) caps how often an unchanged state is logged, and `spark.yarn.includeDriverLogsLink` is opt-in because it costs two extra RM RPCs per poll
18. **Source sweep — [sql/core — the classic API in the source map](reference/spark-source-map/sweeps/sql-core-classic-api.md)** — two observability surfaces this topic can use: `ObservationManager` (metrics collected during the query rather than in a second pass) and the `InMemoryTableScanExec` accumulators `readPartitions` / `readBatches`, which make cache batch-skipping visible instead of theoretical
19. **Source sweep — [sql/pipelines — graph in the source map](reference/spark-source-map/sweeps/sql-pipelines-graph.md)** — pipeline observability from the producing side, and it is thinner than it looks. `QueryOrigin` is the provenance channel — file, line and SQL text attached to exceptions as a **suppressed exception** so the original type survives, which is what lets a pipeline error point at a line of the user's Python or SQL. Against that, three gaps to know about before you build alerting on pipeline runs: the run outcome reports `COMPLETED` when every flow was skipped or excluded; a persisted view that failed to publish does **not** fail the run and appears only as a flow event; and `UnexpectedRunFailure` ("Run FAILED unexpectedly", no cause attached) is an ordinary reachable outcome, not the bug its scaladoc claims
20. **Source sweep — [resource-managers/kubernetes — driver & executor in the source map](reference/spark-source-map/sweeps/resource-managers-kubernetes-driver-executor.md)** — two Kubernetes-specific observability surfaces. `SparkKubernetesDiagnosticsSetter` (opt-in via `spark.kubernetes.driver.annotateExitException`) patches the driver's fatal exception onto the driver **pod as an annotation**, truncated to 64 KiB, so `kubectl describe pod` explains a failure without log access. And `ExecutorRollPlugin` is an observability *consumer* rather than a producer: it reads `ExecutorSummary` from the status store and decommissions the worst executor by one of eleven policies — but its default `OUTLIER` policy falls back to `TOTAL_DURATION`, so once enabled it rolls an executor **every interval regardless of health**; use `OUTLIER_NO_FALLBACK` if you meant "only when something looks wrong"
21. **Source sweep — [sql/pipelines — pipeline-runtime in the source map](reference/spark-source-map/sweeps/sql-pipelines-pipeline-runtime.md)** — the pipeline event model in full, and the page to read before building any alerting on it. **A crashed streaming flow emits `FlowProgress(COMPLETED)`**: the stream listener ignores `QueryTerminatedEvent.exception` and reports every termination as completion, so a failed flow produces both a COMPLETED and a FAILED event from two threads with no ordering guarantee — never treat COMPLETED alone as success. **There are no metrics on this channel at all** (`onQueryProgress` is empty); throughput and batch timings come from the Structured Streaming surfaces instead. And the drop policy the connect sweep documented is defined by a four-line `FlowStatus.isTerminal` whose terminal set is *narrower* than the executor's — `EXCLUDED` and `IDLE` are droppable, which are exactly the events that would tell you a run computed nothing while still reporting `COMPLETED`. Smaller: event ids are random UUIDs with no sequence number, so a gap is undetectable; `origin.datasetName` is never populated; and `RunState.RUNNING` is never emitted, so there is no run-started event
22. **Source sweep — [connector/kafka-0-10-token-provider — auth in the source map](reference/spark-source-map/sweeps/connector-kafka-0-10-token-provider-auth.md)** — what credential material reaches your logs when DEBUG is on for a secured Kafka job. `KafkaRedactionUtil.redactParams` routes everything through `spark.redaction.regex` **except** `sasl.jaas.config`, which gets a dedicated greedy `password="…"` replacement — so the token HMAC is hidden but `username=`, which is the token id, is not. More significant: `getKeytabJaasParams` and `getTicketCacheJaasParams` log their output through plain `logDebug` with **no redaction at all**, so DEBUG on this module prints the full JAAS configuration of your Kerberos login (keytab path and principal, not a secret — but not something to ship to a log aggregator either). The token dump itself replaces the HMAC column with the redaction text
23. **Source sweep — [streaming — DStream in the source map](reference/spark-source-map/sweeps/streaming-dstream.md)** — a third observability surface with its own retention, separate from the Spark UI and the History Server. The streaming tab, a `StreamingSource` metrics source named `<appName>.StreamingMetrics` exposing receiver and batch gauges, and a **REST API** at `/api/v1/…/statistics`, `/receivers` and `/batches` — all fed from one in-memory `StreamingJobProgressListener` bounded by `spark.streaming.ui.retainedBatches` (default 1000), so the REST endpoint you build alerting on forgets at exactly the same point the UI does. New in 4.1.0: `spark.streaming.extraListeners`, the streaming mirror of `spark.extraListeners`. The single most useful log line in a DStream job is the scheduler's "Total delay: X s for time T (execution: Y s)" — the gap between the two numbers is scheduling delay, and a steadily growing gap is the canonical backlog signal

!!! warning "Group-based ACLs deny silently when group lookup fails"

    `spark.ui.view.acls.groups` and `spark.modify.acls.groups` are evaluated by resolving the user's groups through `spark.user.groups.mapping`, which defaults to a provider that forks `id -Gn` on the driver host — with no caching, a fresh reflective instantiation per check. If that lookup throws for any reason (the user does not exist locally, `id` is absent, a custom LDAP provider times out), `Utils.getCurrentUserGroups` logs an ERROR and returns the **empty set**, so every group rule fails to match and the user is denied while the ACL config looks correct. On a containerised driver this is the common case, not the edge case. Suspect it whenever group ACLs work for some users and not others; the user-list ACLs and the `*` wildcard are checked first and never reach the shell. Traced in the [config & security sweep](reference/spark-source-map/sweeps/core-config-security.md).

!!! note "New in Spark 4.2.0 — PySpark worker logs are retrievable"

    Until 4.2.0, a `print()` or `logging` call inside a Python UDF went to the Python worker's stderr on the executor and was effectively unreachable without node access — the single most common "why can't I debug my UDF" complaint. [SPARK-53755] added log-block support to the `BlockManager` and [SPARK-53975] built Python worker log capture on top: each worker's output is written through a `RollingLogWriter` into blocks named `python_worker_log_<time>_<executor>_<session>_<worker>`, rolling every 32 MiB. `spark.executor.python.worker.log.details` controls the detail captured. Book-absent and blog-absent — see the [core — storage & serialization sweep](reference/spark-source-map/sweeps/core-storage-serializer.md) for the writer's lifecycle and the fact that the roll size is a hardcoded default rather than a config.

!!! warning "Spark's most expensive decisions are logged at `debug` or not at all"

    Four branches that each change performance by a large factor are invisible by default, and
    together they are the reason "the job got slower" is so often unexplainable:

    - **Which shuffle writer ran.** All three reasons for rejecting the fast serialized path are
      `log.debug`. Falling back to the deserialized writer — the largest write-path cliff in
      Spark — emits nothing at INFO.
    - **Which merge strategy ran.** Fast vs slow spill merge is `debug`; enabling IO encryption
      silently demotes it.
    - **Whether batch fetch applied.** The eligibility mismatch is reported at `debug` only.
    - **Whether push-based shuffle was actually on.** A per-stage merger shortfall disables it by
      returning an empty list with **no log at any level**.

    Practical consequence for this topic: raising `org.apache.spark.shuffle` and
    `org.apache.spark.storage` to DEBUG on one representative run tells you which paths a job
    takes, and is worth doing once per workload shape rather than never.

**Milestone:** You can configure a custom Spark listener that emits stage completion metrics to a log sink, set up an alert that fires when a job's duration exceeds 2× its 7-day moving average, and determine from logs or metrics which shuffle write path a given job actually used.

!!! warning "Spark ships no alerting engine — the milestone's alert is *yours* to build"

    Nothing in Spark core fires an alert. Core exposes only the raw surfaces — `SparkListener`
    callbacks, the metrics `Source`/`Sink` system, and the event log — and the "alert when a job
    exceeds 2× its moving average" milestone is an application *you* write on top of them (a
    listener that computes the rolling average and pushes to your own paging system, or a metrics
    sink feeding an external rule engine). The [monitoring source sweep](reference/spark-source-map/sweeps/core-monitoring.md) maps exactly which surfaces are available to hang that logic on; do not
    go looking for a built-in `spark.alerting.*` config, there isn't one.

!!! warning "Monitoring data is dropped silently when an event queue overflows"

    Every listener (the UI's `AppStatusListener`, the event log, your custom listener) is fed by a
    bounded `AsyncEventQueue`. When a queue fills — a slow listener, or an event storm from many
    small tasks — `AsyncEventQueue.post` does **not** block: it drops the event, bumps a
    `numDroppedEvents` metric, logs one error then a rate-limited warning at most once per 60s.
    Dropped task events leave the UI and the History Server view of that stage permanently
    incomplete, and the only signal is that metric and a warning most operators never look for.
    The lever is `spark.scheduler.listenerbus.eventqueue.capacity` (default 10000), tunable
    per-queue as `spark.scheduler.listenerbus.eventqueue.<name>.capacity`. The
    [monitoring source sweep](reference/spark-source-map/sweeps/core-monitoring.md) traces the
    drop path (`AsyncEventQueue.post`).

!!! note "New in Spark 4.2.0 — History Server scalability"
    The History Server got scalability work in 4.2.0 ([SPARK-56287]), which matters directly for this topic's premise (debugging a completed job without the live UI). Kubernetes deployments also gained a Resource Manager API ([SPARK-56603]) and reduced control-plane overhead ([SPARK-55400]) — relevant to E2.

!!! note "New in Spark 4.2.0 — richer profiling and diagnostics for Python execution"
    Observability of the Python side of Spark — historically the blind spot, since the Spark UI sees the JVM and not the Python worker — improved in 4.2.0. The existing PySpark profiler (`spark.python.profile`) was extended to **time *and* memory profiling for Python Data Sources** ([SPARK-55161]; memory-profiler fixes for iterator UDFs in [SPARK-55171]; see B4), alongside **improved worker diagnostics** and **logging that can be queried as data** (the structured-logging path this topic already covers, now emitted from Python execution too). This is the debugging counterpart to the Arrow-first Python performance work in I3: when a pandas UDF or a Python Data Source is the bottleneck, these are what let you see *where* rather than guess. Docs-and-source territory; verify on your own 4.2.0 stack.

---

### ⬜ E4 — Delta Lake Internals: Transaction Log, MVCC, and Concurrency

**What it is:** The `_delta_log` JSON commit files; checkpoint files; snapshot isolation; optimistic concurrency control; what happens during concurrent writes; `RESTORE`; `CLONE`.

**Why you need it:** When two jobs write to the same Delta table simultaneously, you need to know which one wins, whether data is lost, and how to recover.

**Learn it with:**

1. **DLDG Ch 1** — the transaction log as a single source of truth; MVCC internals
2. **DLDG Ch 8** — row-level concurrency; deletion vectors; advanced write operations
3. **DLUR Ch 6** — time travel and `RESTORE` in full operational detail
4. **Delta-docs → Protocol spec** ([PROTOCOL.md](https://github.com/delta-io/delta/blob/master/PROTOCOL.md)) — the actual commit-file schema, checkpoint format, and reader/writer version rules; the only source that settles concurrency questions definitively. Pair with [Concurrency control](https://docs.delta.io/latest/concurrency-control.html) for the exception taxonomy
5. **Source sweep — [sql/catalyst — analysis in the source map](reference/spark-source-map/sweeps/sql-catalyst-analysis.md)** — the analyzer half of row-level operations and time travel: `SupportsDelta` versus group rewrite, and `TIMESTAMP AS OF` folded to a fixed microsecond value at analysis time

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
6. **Source sweep — [core — config & security in the source map](reference/spark-source-map/sweeps/core-config-security.md)** — the delegation-token provider SPI behind Kerberised access: `ServiceLoader`-loaded providers, a per-provider enable key built by `String.format` (so it never appears in the config catalog), and `hadoopFSsToAccess` — which is why a second Kerberised filesystem fails at the first task that touches it rather than at submit, unless you name it in `spark.kerberos.access.hadoopFileSystems`
7. **Source sweep — [sql/catalyst — framework in the source map](reference/spark-source-map/sweeps/sql-catalyst-framework.md)** — the catalog object model and its audit seam: `CatalogTable` / `CatalogTablePartition` / `CatalogStatistics` are the case classes behind everything `DESCRIBE TABLE` prints, and `ExternalCatalogWithListener` posts a **pre- and post-event for every DDL operation** to the Spark listener bus — a supported hook for lineage and audit that needs no metastore access
8. **Source sweep — [sql/hive — hive-metastore in the source map](reference/spark-source-map/sweeps/sql-hive-hive-metastore.md)** — the Hive metastore as a governed catalog: `HiveExternalCatalog` implements the `ExternalCatalog` interface the framework sweep describes, stores Spark's schema in reserved `spark.sql.*` table properties, stamps every table with the Spark version that created it, and serialises **all** metastore access behind one `synchronized` lock. Kerberos access uses a delegation token whose acquisition failure is only a warning
9. **Source sweep — [sql/core — query-execution in the source map](reference/spark-source-map/sweeps/sql-core-query-execution.md)** — `ResolveSessionCatalog`, the routing rule that decides V1 versus V2 for every DDL statement, and `spark.sql.catalogImplementation` / `spark.sql.globalTempDatabase` — the two static configs that fix catalog behaviour for the life of the JVM
10. **Source sweep — [sql/core — datasources in the source map](reference/spark-source-map/sweeps/sql-core-datasources.md)** — the catalog plumbing under DSv2: `V2SessionCatalog` adapting Spark's own `SessionCatalog` to the `TableCatalog` API (and rejecting time travel), the `JDBCTableCatalog` that makes a database appear as a Spark catalog, and about forty `V2CommandExec` operators implementing DDL against whichever catalog resolved. Also the JDBC connection-provider SPI: `spark.sql.sources.disabledJdbcConnProviderList` filters providers by name, **two applicable providers is an error rather than a preference order**, and a provider that mutates the JVM security context runs under a global lock with the previous configuration restored in a `finally`
11. **Source sweep — [sql/core — the classic API in the source map](reference/spark-source-map/sweeps/sql-core-classic-api.md)** — how `spark.catalog` relates to the three-level namespace: `classic.Catalog` mostly builds logical commands (`ShowNamespaces`, `ShowTables`, `UnresolvedTableOrView`) and runs them as Datasets, and several methods first inspect whether a one- or two-part name is a temp view or a session-catalog table before qualifying it — which is where `spark.catalog.getTable("x")` and `spark.sql("DESCRIBE x")` can disagree
12. **Source sweep — [resource-managers/kubernetes — auth & networking in the source map](reference/spark-source-map/sweeps/resource-managers-kubernetes-auth-networking.md)** — how credentials and secrets physically reach a Spark pod, which is where this topic meets deployment. **The right way to give a Spark job a password on Kubernetes** is `spark.kubernetes.{driver,executor}.secrets.*` (mount as files) or `…secretKeyRef.*` (project as env), because both produce *references* the kubelet resolves — the value never enters the Spark conf, so it cannot leak through the Environment tab, the event log, or the pod's `spark-defaults.conf`. Prefer the file form: the env form lands in `/proc/<pid>/environ` and never picks up a rotated Secret. Kerberos has three modes in precedence order (keytab → existing delegation-token Secret → tokens minted from the submitter's TGT) with a trade-off worth knowing: only the keytab mode can **renew**, but it also puts a permanent credential in a namespace Secret, while the token mode bounds the application by the tokens' max lifetime. Both a missing `krb5.conf` and a delegation-token acquisition failure are **logged and ignored**, surfacing much later — the same shape as the `HiveDelegationTokenProvider` warning above. Identity and RBAC are topic **E35**
13. **Source sweep — [connector/kafka-0-10-token-provider — auth in the source map](reference/spark-source-map/sweeps/connector-kafka-0-10-token-provider-auth.md)** — governance at the connection level rather than the catalog level: how a Spark job proves who it is to a secured Kafka cluster, and what that identity is scoped to. The model is worth contrasting with a catalog's: there is no central authority mapping principals to permissions here — Spark asks *each* cluster for a delegation token as the submitting user, ships the tokens with the application, and every executor connection then re-derives which token to use by regex-matching its `bootstrap.servers`. Consequences: the token is a **bearer credential** for its lifetime, it inherits exactly the submitting user's Kafka ACLs and nothing narrower, and **proxy users are not supported at all** (`checkProxyUser` `require`-fails, KAFKA-6945) — so an impersonation-based access model does not reach Kafka. Multi-cluster identity is topic **E42**

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
5. **Source sweep — [sql/connect — declarative pipelines in the source map](reference/spark-source-map/sweeps/sql-connect-declarative-pipelines.md)** — the **AutoCDC** declarative API added in 4.2.0: name a source stream, the key columns and a `sequence_by` expression, and the engine applies the changes without you writing a `MERGE`. Read the warning before relying on it — `apply_as_truncates` and the two `ignore_null_updates_*` lists are accepted by the API and **silently ignored by the engine** at 4.2.0 (SPARK-57092, SPARK-57093), so a pipeline reports success while doing something other than what was declared
6. **Source sweep — [sql/pipelines — graph in the source map](reference/spark-source-map/sweeps/sql-pipelines-graph.md)** — the engine side of AutoCDC, and it corrects the scope: **SCD Type 2 is modelled but not implemented at 4.2.0** — only Type 1 runs (and see the next entry for what error you actually get). The mechanism is a hidden **auxiliary state table** per target (`<prefix>aux_state_<table>`, holding the key columns plus a per-key sequence watermark) created at flow execution, dropped on full refresh, and whose key set is written once as a JSON table property and treated as immutable. Two operational consequences: the target's connector must implement `SupportsRowLevelOperations` or the flow fails with `AUTOCDC_TARGET_DOES_NOT_SUPPORT_MERGE`, and **changing the key columns of an existing AutoCDC flow passes a dry run and fails partway through the next real run**, because drift validation happens in the write's constructor rather than during graph validation
7. **Source sweep — [sql/pipelines — autocdc in the source map](reference/spark-source-map/sweeps/sql-pipelines-autocdc.md)** — **the algorithm**, and the most complete worked SCD1 implementation available to read anywhere: five files that dedupe a microbatch by key, classify each row as upsert or delete, filter late events against a tombstone table, and apply the result with two `MERGE`s. Four things it teaches that no CDC book does. (i) The out-of-order delete problem and its fix — SCD1 keeps no history, so a *separate* per-key delete high-water mark is what stops a late update resurrecting a deleted row; this is now its own topic, **E32**. (ii) `apply_as_deletes` is the **only** delete-detection mechanism — AutoCDC recognises no `_change_type` or `op` convention, so a Debezium-style feed without it applies deletes as upserts, silently. (iii) The tie-break is deliberately asymmetric (`>=` for upserts, `>` for deletes), so an update and a delete at the same sequence value leave the row alive — which makes a coarse `sequence_by` a correctness choice, not a performance one. (iv) `__spark_autocdc_metadata` is written into your target table as a visible trailing struct that `except_column_list` cannot remove and that downstream datasets inherit. Also: the client-facing SCD2 rejection is an **untyped `UnsupportedOperationException`** from the Connect handler, not the typed `AUTOCDC_SCD2_NOT_SUPPORTED` the engine defines, and the 4.2.0 programming guide documents none of this — it contains zero mentions of CDC
8. **Source sweep — [sql/catalyst — analysis in the source map](reference/spark-source-map/sweeps/sql-catalyst-analysis.md)** — Spark's own CDC *read* path, distinct from the pipelines AutoCDC write path above: a `CHANGES` clause or `DataFrameReader.changes()` produces a `RelationChanges` node carrying a `ChangelogContext`, and `ResolveChangelogTable` implements the deduplication semantics the connector does not, using reserved `__spark_cdc_*` helper columns that are added and projected away. Two things to carry: **there is no session config for any of it** — version range, bound inclusivity, deduplication mode and `computeUpdates` are all read *options*; and `RelationChanges.relation` is a constructor field rather than a tree child, so `transformUp` never visits it

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
4. **Source sweep — [core — submit & standalone in the source map](reference/spark-source-map/sweeps/core-submit-standalone.md)** — how `--remote` enters submission as a mutually-exclusive alternative to `--master`, and why the Connect server may only run in cluster mode under YARN
5. **Source sweep — [sql/connect — client-server in the source map](reference/spark-source-map/sweeps/sql-connect-client-server.md)** — the **first source-derived material behind this topic**, and the one sweep that covers it end to end: the twelve RPCs and 65 relation messages, `SparkConnectPlanner` (a second front end that builds an unresolved `LogicalPlan`, alongside the SQL parser), `SessionHolder` and its 60-minute idle timeout, the plan cache keyed on the serialized protobuf, reattachable execution, artifacts, retries and error enrichment. Note two things that shape day-to-day use: `df.show()` is a *relation type* so it is a network round trip, and built-in auth is a **single pre-shared token** with a client-supplied, untrusted `user_id`
6. **Source sweep — [sql/core — the classic API in the source map](reference/spark-source-map/sweeps/sql-core-classic-api.md)** — the classic half of the split this topic is about: every class in `classic/` is `extends sql.<Name>`, with Connect supplying a second implementation of the same `sql/api` interfaces. The mechanism that makes it possible is `ColumnNode` (see **A37**) — a `Column` is no longer a Catalyst `Expression` wrapper. Also `ArtifactManager`'s isolation flag, which `enableHiveSupport` silently disables
7. **Source sweep — [sql/core — SQL scripting in the source map](reference/spark-source-map/sweeps/sql-core-sql-scripting.md)** — one concrete case of "what does Connect *not* change": `SparkConnectPlanner`'s SQL command calls the classic `SparkSession.sql`, so a SQL script runs server-side exactly as it would in classic mode, named parameters and all. The one difference is inherited from the classic path, not from Connect — positional parameters are rejected for scripts

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

### Optional depth — source-derived topics

**Not required for the checkpoint below.** As with the Intermediate group: derived from the `core` sweep, read on demand rather than in order.

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

### ⬜ E11 — Serialization: KryoSerializer vs JavaSerializer

> Discovered from source sweep (refinement): `core: serialization`

**What it is:** KryoSerializer uses the Kryo library with a KryoPool, unsafe I/O, and optional class registration; JavaSerializer (default) uses Java object streams with periodic reset to bound stream-table memory.

**Why you need it:** Serializer choice determines shuffle and broadcast throughput; Kryo requires explicit class registration for production determinism, and misconfiguration produces cryptic NotSerializableException or data-corruption failures.

**Learn it with:**

1. **Spark-docs → Data Serialization** ([tuning.html#data-serialization](https://spark.apache.org/docs/latest/tuning.html#data-serialization)) — the official Kryo recommendation, registration, and buffer sizing
2. **SDG Ch 19** — performance tuning; serialization in the context of everything else that makes a job slow. Treat its JVM-flag specifics as dated (see E1 — 4.2.0 runs on Java 25)
3. **Source** — `core/src/main/scala/org/apache/spark/serializer/KryoSerializer.scala`
4. **Source sweep — [core — storage & serialization in the source map](reference/spark-source-map/sweeps/core-storage-serializer.md)** — the serializer abstraction end to end: the Kryo pool and registration order, the 2 GiB buffer validation, relocation support gating the fast shuffle path, and the two places `spark.serializer` is silently ignored
5. **Source sweep — [sql/core — joins in the source map](reference/spark-source-map/sweeps/sql-core-joins-exec.md)** — a concrete case where the serializer choice is load-bearing: `UnsafeHashedRelation` implements **both** `Externalizable` and `KryoSerializable`, because the broadcast relation is moved by whichever serializer the session configures, and a broadcast join ships it to every executor

**Milestone:** You can enable Kryo with class registration, explain what `spark.kryo.registrationRequired=true` protects you from, and describe why this matters far less for pure DataFrame work than for RDDs of custom objects.

---

### ⬜ E12 — Executor Exclusion and Health Tracking

> Discovered from source sweep (gap): `core: executor-exclusion`

**What it is:** two tiers of failure tracking. `TaskSetExcludelist` works within a single stage attempt and escalates — (task, executor), then (task, node), then the whole executor and node *for that stage*. `HealthTracker` accumulates across the application with an expiry, and can kill or decommission a persistently bad executor. Critically, the application-level tracker only learns about failures **when a TaskSet completes successfully**.

**Why you need it:** one flaky disk or one bad NIC manifests as a stage that retries repeatedly and then aborts with "cannot run anywhere due to node and executor excludeOnFailure" — opaque without the two-tier model. The subsystem also has a dry-run mode that silently excludes nothing, and a startup validation that will refuse to launch your application entirely.

**Learn it with:**

1. **Spark-docs → Configuration, Scheduling** ([configuration.html#scheduling](https://spark.apache.org/docs/latest/configuration.html#scheduling)) — the full `spark.excludeOnFailure.*` family, its scopes and timeouts
2. **Spark-docs → Job Scheduling** ([job-scheduling.html](https://spark.apache.org/docs/latest/job-scheduling.html)) — how exclusion interacts with dynamic allocation, which is what supplies replacement executors
3. **Source sweep — [core — execution engine in the source map](reference/spark-source-map/sweeps/core-execution-engine.md)** — the escalation ladder, the "blind until a TaskSet succeeds" constraint, the dry-run mode, and the starvation caveat that can stop the unschedulable-abort timer from ever firing
4. **Source sweep — [resource-managers/yarn — AM & executor allocation in the source map](reference/spark-source-map/sweeps/resource-managers-yarn-am-executor.md)** — a **third tier** the two above do not cover: `YarnAllocatorNodeHealthTracker` runs inside the ApplicationMaster, not the driver, precisely to avoid the delay between excluding a node and the next allocation. It merges three sources — the static `spark.yarn.exclude.nodes`, the scheduler's excluded nodes pushed down with every executor request, and its own per-host allocation-failure count — and pushes the union into YARN with `AMRMClient.updateBlacklist`. Two traps: `spark.yarn.executor.launch.excludeOnFailure.enabled` defaults to **false**, and with it off a failure still burns the application-wide `maxNumExecutorFailures` budget while the bad host is never excluded, so one broken NodeManager can kill the application by itself; and `ContainerExitStatus.DISKS_FAILED` is in the "not the node's fault" set (following Hadoop's `Apps#shouldCountTowardsNodeBlacklisting`), so a NodeManager with failed disks never reaches this tracker at all

!!! warning "No book covers this"

    Executor exclusion post-dates SDG (2018) in its current form and is absent from LS2e and Rioux. The `spark.blacklist.*` keys you will find in older blog posts are the pre-3.1 names for the same thing.

!!! warning "Spark refuses to start on a contradictory configuration"

    If `spark.excludeOnFailure.task.maxTaskAttemptsPerNode` is greater than or equal to `spark.task.maxFailures`, initialisation throws `IllegalArgumentException` — because a task would exhaust its total failure budget on one node before exclusion could ever route it elsewhere. The error names both keys; the reasoning is not obvious.

**Milestone:** You can explain why the application-level tracker sees nothing during a stage that keeps failing, predict what happens to a node after a single fetch failure when the external shuffle service is enabled, and say which combination of settings produces a tracker that records failures but excludes nothing.

---

### ⬜ E13 — Barrier Execution Mode

> Discovered from source sweep (gap): `core: barrier-execution`

**What it is:** a barrier stage is gang-scheduled. `resourceOffers` refuses to launch *any* task of the stage unless it can place *every* task in a single offer round, and at runtime `BarrierTaskContext.barrier()` blocks until all tasks in the stage have called it. This is the execution model that lets distributed training frameworks — which need all workers alive simultaneously and able to talk to each other — embed inside a Spark job.

**Why you need it:** barrier mode is the bridge between Spark's fault-tolerant task model and the all-or-nothing model that MPI-style workloads require, and both of its failure modes are **silent hangs rather than errors**. A cluster that cannot supply every slot at once waits indefinitely instead of failing at submit; an unequal number of `barrier()` calls across code branches hangs the job until the coordinator's own timer fires.

**Learn it with:**

1. **Spark-docs → Job Scheduling** ([job-scheduling.html](https://spark.apache.org/docs/latest/job-scheduling.html)) — the scheduling model barrier mode overrides, and why gang scheduling conflicts with dynamic allocation
2. **Spark-docs → BarrierTaskContext API** ([BarrierTaskContext.html](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/BarrierTaskContext.html)) — the `barrier()` contract and `getTaskInfos()`, with the misuse examples in the scaladoc
3. **Source sweep — [core — execution engine in the source map](reference/spark-source-map/sweeps/core-execution-engine.md)** — the all-or-nothing offer gate, the revert-and-retry round, the 365-day RPC timeout that defers to the coordinator's timer, and the interaction with `spark.locality.wait.legacyResetOnTaskLaunch` that turns a partial launch into an abort
4. **Source sweep — [core — api-bridge in the source map](reference/spark-source-map/sweeps/core-api-bridge.md)** — how a *Python* task reaches any of this. A barrier task gets a second server socket, bound only for barrier stages, whose `accept-connections` thread is the gateway for every `barrier()` and `allGather()` call PySpark makes. Two consequences the Scala API hides: the handshake has a 10 s timeout but the call itself has none (deliberately — a barrier may wait forever), and a `SparkException` is written back as a **bare message string** on the same socket the success values use, so a barrier failure arrives in Python with none of the structure a normal task failure has

!!! warning "No book covers this"

    Barrier execution landed in Spark 2.4, after SDG (2018), and neither LS2e nor Rioux covers it. Docs are thin too — the API scaladoc is the most precise description available.

!!! warning "Both failure modes are hangs, not errors"

    A barrier job needing more slots than the cluster has is **not failed at submit** (there is a standing `TODO` in the source saying so) — it simply waits, logging once a minute. And `barrier()` must be called the same number of times by every task: putting one inside an `if` that only some partitions enter will hang the stage until the sync timeout.

**Milestone:** You can explain why barrier mode and dynamic allocation interact badly, predict what happens when a barrier stage requests more slots than the cluster can offer at once, and say why speculation is disabled for barrier TaskSets.

---

### ⬜ E14 — Unmanaged Memory: Native Allocators Outside the Unified Pool

> Discovered from source sweep (gap): `core: unmanaged-memory-accounting`

**What it is:** Spark's unified memory manager accounts for execution and storage memory it hands out itself. Components that allocate *outside* those pools — RocksDB state stores, native libraries, JNI buffers — are invisible to it unless they register as `UnmanagedMemoryConsumer`s. When registered and polling is enabled, a daemon thread samples their usage and subtracts it from what execution and storage may allocate.

**Why you need it:** the polling interval defaults to `0s`, which means disabled. On a stock install a stateful streaming job's RocksDB memory does not appear in Spark's accounting at all, which is the direct cause of the most common complaint in stateful streaming: **the executor is killed for exceeding its container limit while the Spark UI shows plenty of free storage memory.** Sizing executors from the UI's numbers is wrong by however much the native allocator holds.

**Learn it with:**

1. **Spark-docs → Configuration, Memory Management** ([configuration.html#memory-management](https://spark.apache.org/docs/latest/configuration.html#memory-management)) — `spark.memory.fraction`, `storageFraction` and the off-heap keys, i.e. what *is* accounted for
2. **Spark-docs → Structured Streaming, State Store** ([structured-streaming-programming-guide.html#state-store](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#state-store)) — the RocksDB state store, the usual unmanaged consumer in practice
3. **Source sweep — [core — shuffle & memory in the source map](reference/spark-source-map/sweeps/core-shuffle-memory.md)** — the registration mechanism, the polling daemon and its default-off interval, and how the polled figures are subtracted from the execution and storage ceilings

!!! warning "No book covers this, and it is new"

    `UnmanagedMemoryConsumer` arrived in Spark 4.1 — after every book in the resources table, and after most blog posts on Spark memory tuning. Anything you read about `spark.memory.fraction` predating it describes an incomplete picture on a stateful streaming workload.

!!! warning "Enabled, the numbers are still a stale snapshot"

    Allocation decisions run against poll data up to one interval old, and a consumer whose usage accessor throws is silently counted as zero. This narrows the gap rather than closing it.

**Milestone:** You can explain why a RocksDB-backed streaming executor gets OOM-killed while the Spark UI reports free memory, name the config that makes that memory visible and its default, and describe how you would size executor memory for a stateful streaming job given that the state store sits outside `spark.memory.fraction`.

---

### ⬜ E15 — Block Locking and Cache Visibility

> Discovered from source sweep (gap): `core: block-locking`

**What it is:** every cached or shuffled block sits behind a per-block readers-writer lock, attributed to a task attempt id so all of a task's locks can be reclaimed when it ends. Separately, an RDD block reported by a still-running task is held **invisible** until the driver learns that task succeeded, so a speculative or failed attempt cannot publish partial data to other tasks.

**Why you need it:** two symptoms have no other explanation. A cached iterator you never fully drain keeps its read lock and pins the block against eviction for the rest of the task — `getLocalValues` hands back an iterator that releases the lock only on completion. And the executor log line `N block locks were not released by task X` is unreadable without the protocol, yet it is reported at INFO by default, so a genuine leak is invisible in most production log configurations.

**Learn it with:**

1. **Spark-docs → Configuration, Memory Management** ([configuration.html#memory-management](https://spark.apache.org/docs/latest/configuration.html#memory-management)) — the storage configs this interacts with, including `spark.storage.exceptionOnPinLeak`
2. **Spark-docs → RDD Programming Guide, RDD Persistence** ([rdd-programming-guide.html#rdd-persistence](https://spark.apache.org/docs/latest/rdd-programming-guide.html#rdd-persistence)) — the caching semantics the locking protocol protects
3. **Source sweep — [core — storage & serialization in the source map](reference/spark-source-map/sweeps/core-storage-serializer.md)** — the striped readers-writer lock, task-scoped release, the visibility handshake between executor and driver, and the two concurrent-release bugs the code now guards against

!!! warning "No book covers this"

    Block-level locking is an internal contract; SDG, LS2e and Rioux all describe caching at the API level and stop. This is source territory, and the sweep is the primary reference.

!!! info "A cached block from a failed task is left invisible, not evicted"

    SPARK-42582 is still open. Combined with the acknowledged TODO that indeterminate RDDs can produce *different* replicas under one BlockId, this is the storage-layer half of the correctness story that [A14](#a14--determinism-indeterminate-stages-and-correctness-under-retry) tells from the scheduler side.

**Milestone:** You can explain why an un-drained `BlockResult` iterator keeps memory pinned, say what `N block locks were not released` means and which config turns it into a hard failure, and describe why a cached RDD block is not readable by other tasks until the producing task succeeds.

---

### ⬜ E16 — Standalone High Availability and Recovery

> Discovered from source sweep (gap): `core: leader-election-and-ha`

**What it is:** the standalone Master persists applications, workers and drivers through a `PersistenceEngine`, and on startup a `LeaderElectionAgent` decides whether this Master becomes active. Recovery then reads the persisted state, broadcasts `MasterChanged`, waits `spark.deploy.recoveryTimeout` for everyone to check in, and removes whatever did not. **Only ZooKeeper mode has real leader election** — FILESYSTEM and ROCKSDB use `MonarchyLeaderAgent`, which declares itself leader unconditionally in its constructor.

**Why you need it:** the standalone Master is a single point of failure, and the three configurations that look like they fix it each have a trap that is invisible until the day it matters.

**Learn it with:**

1. **Spark-docs → Spark Standalone Mode, High Availability** ([spark-standalone.html#high-availability](https://spark.apache.org/docs/latest/spark-standalone.html#high-availability)) — the ZooKeeper and single-node recovery modes as documented
2. **Spark-docs → Configuration, Deploy** ([configuration.html#deploy](https://spark.apache.org/docs/latest/configuration.html#deploy)) — the `spark.deploy.recovery*` keys
3. **Source sweep — [core — submit & standalone in the source map](reference/spark-source-map/sweeps/core-submit-standalone.md)** — `MonarchyLeaderAgent` electing in its constructor, the silent `case _` fallthrough, the exit-code-0 behaviour on lost leadership, and what `recoveryTimeout` actually removes

!!! warning "No book covers standalone HA"

    SDG Ch 16 covers standalone deployment without the recovery machinery. This is docs-and-source territory.

!!! warning "FILESYSTEM mode is not high availability"

    It is what people reach for as "HA without ZooKeeper". `MonarchyLeaderAgent` makes **both** Masters believe they are leader; both accept registrations, and the persistence engine throws when the second writes a key the first already wrote. The failure is partial and confusing rather than immediate. A typo in `spark.deploy.recoveryMode` is worse: it falls into a catch-all that gives you no persistence and no error at all.

!!! warning "Losing leadership exits with code 0"

    `RevokedLeadership` calls `System.exit(0)`. A supervisor configured with `Restart=on-failure` reads that as a clean shutdown and does **not** restart the Master — so a ZooKeeper hiccup silently leaves you with one fewer standby than you think.

!!! info "Recovery removes slow workers, not just dead ones"

    `spark.deploy.recoveryTimeout` defaults to `spark.worker.timeout` (60 s). A large cluster whose workers cannot all re-register within that window loses the stragglers on every failover, with their executors reported LOST though the processes are still running. Raising it independently is the fix, and is why the config was split out in 4.0.0.

**Milestone:** You can explain why two Masters against a shared recovery directory is not HA, predict what a process supervisor does when a Master loses ZooKeeper leadership, and size `spark.deploy.recoveryTimeout` for a cluster whose workers take longer than a minute to re-register.

---


### 🎯 Expert Checkpoint

You are operating at Expert level when you can:

- Design a governed lakehouse from scratch (medallion + a catalog with lineage — Unity Catalog, an Iceberg REST catalog, or equivalent)
- Debug a production incident using Spark metrics + History Server without the live UI
- Implement CI/CD for a multi-environment pipeline with automated tests
- Architect a streaming CDC pipeline with SCD Type 2 history and exactly-once guarantees

*Optional:* the Databricks Data Engineer Professional exam maps to roughly A6–E8.

---

### ⬜ E17 — Output Commit Coordination and Speculative Write Safety

> Discovered from source sweep (new topic): `core: output-commit-coordination`

**What it is:** the mechanism that decides *which* attempt of a task is allowed to make its output visible. A driver-side `OutputCommitCoordinator` hands out one commit lock per `(stage, partition)` on a first-committer-wins policy; a denied attempt throws `CommitDeniedException`, which the scheduler converts to `TaskCommitDenied` — a failure that deliberately does not count against `spark.task.maxFailures`. Underneath it sits the Hadoop commit protocol proper: the v1/v2 `FileOutputCommitter` algorithms, and the cloud-native committers that replace rename-based commit entirely.

**Why you need it:** the moment two attempts of one task can run at once — speculation, a stage retry after a fetch failure, or a straggler kill that lands late — something has to stop both from writing the same output. This is that something, and its boundaries are sharp and undocumented. It covers exactly one call site (the Hadoop commit path), so any write your own task code performs is unprotected; it can be switched off by an escape hatch that appears in no configuration table; and on object stores the committer *underneath* it may itself be unsafe, which no amount of coordination fixes.

**Learn it with:**

1. **Spark-docs → Integration with Cloud Infrastructures** ([cloud-integration.html](https://spark.apache.org/docs/latest/cloud-integration.html)) — "Committing work into cloud storage safely and fast": why commit-by-rename is unsafe on eventually-consistent stores, what `spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version` 1 vs 2 actually trades, and the S3A / EMRFS / manifest committers that replace it
2. **Spark-docs → Job Scheduling** ([job-scheduling.html](https://spark.apache.org/docs/latest/job-scheduling.html)) — speculation, the feature that makes concurrent attempts routine
3. **Hadoop-docs → S3A Committers: Architecture and Implementation** ([committer_architecture.html](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/committer_architecture.html)) — the correctness argument for the commit protocol itself, written by the people who had to make it work without atomic rename
4. **Source sweep — [core — execution engine in the source map](reference/spark-source-map/sweeps/core-execution-engine.md)** — **both halves, on one page.** The *authority*: the lock as `TaskIdentifier(stageAttempt, taskAttempt)`, why a request arriving after `stageEnd` is always denied, how a failed attempt is permanently barred while its lock is released, and the single call site the whole mechanism guards. The *protocol*: `FileCommitProtocol`'s staging-directory model, and `commitJob` renaming staged files one at a time with no rollback — which is where job-level atomicity is actually lost
5. **Source sweep — [sql/core — datasources in the source map](reference/spark-source-map/sweeps/sql-core-datasources.md)** — the SQL layer above the commit protocol: `SQLHadoopMapReduceCommitProtocol` is the class `spark.sql.sources.commitProtocolClass` names by default, and it adds one hook — `spark.sql.sources.outputCommitterClass` — instantiated by a `(Path, TaskAttemptContext)` constructor for a `FileOutputCommitter` subclass and a no-arg one otherwise. Parquet has its own second hook, `spark.sql.parquet.output.committer.class`. On the DSv2 side, `useCommitCoordinator` is a **connector's choice**, so a V2 sink can opt out of the coordination this topic describes

!!! warning "No book covers this"

    `OutputCommitCoordinator` (SPARK-4879) appears in none of SDG, LS2e or Rioux, and Spark's own configuration tables omit `spark.hadoop.outputCommitCoordination.enabled` — it is read with a bare `SparkConf.getBoolean` and described in the source itself as undocumented. The sweep page and the Hadoop committer docs are the primary sources.

!!! warning "It protects the commit protocol, not your writes"

    A task that writes through `df.write` is covered. A task that opens a JDBC connection, calls an API, or writes to a path itself never asks the coordinator for permission — speculation duplicates that work and nothing intervenes. This is the concrete meaning of "speculation duplicates side effects, not just computation".

!!! info "Concurrent attempts happen without speculation"

    The source comment at the escape hatch cites SPARK-8029: two attempts of the same task can run simultaneously even with speculation disabled, because a stage retry does not kill the old attempt. Turning speculation off does not make the coordinator redundant.

**Milestone:** You can explain what happens to the second attempt when two attempts of one task both reach the commit point, say why a `TaskCommitDenied` failure does not consume the task's retry budget, name the one call site the coordinator guards and give an example of a write that bypasses it, and describe why `fileoutputcommitter.algorithm.version=2` is faster and when it is unsafe.

---


### ⬜ E18 — Reattachable Execution: How Spark Connect Survives a Dropped Connection

> Discovered from source sweep (new topic): `sql/connect: Reattachable execution — surviving a broken response stream`

**What it is:** The mechanism that makes a Connect query survive a broken gRPC stream: the server buffers responses and numbers them, the client tracks the last response id it consumed and issues `ReattachExecute` to resume from there, and `ReleaseExecute` tells the server what it may forget.

**Why you need it:** It is why a long-running Connect query is not killed by a load balancer's idle timeout, it is the reason the server holds a per-execution response buffer you can size wrong, and every 'INVALID_HANDLE.OPERATION_NOT_FOUND' a Connect user has ever seen comes from this protocol.

**Learn it with:**

1. **Spark-docs → Spark Connect Overview** ([spark-connect-overview.html](https://spark.apache.org/docs/latest/spark-connect-overview.html)) — the architecture this sits inside; the reattach protocol itself is not documented, so read this for context and take the mechanism from the source
2. **Spark-docs → Configuration** ([configuration.html](https://spark.apache.org/docs/latest/configuration.html)) — the `spark.connect.execute.reattachable.*` and `spark.connect.execute.manager.*` families, which are the only user-facing surface this feature has
3. **Source sweep — [sql/connect — client-server in the source map](reference/spark-source-map/sweeps/sql-connect-client-server.md)** — the reattachable-execution concept: the numbered server-side response buffer, the client's `lastReturnedResponseId`, the asynchronous `ReleaseExecute`, and the sender's voluntary 2-minute / 1 GB stream deadline

!!! warning "No book covers this"

    Spark Connect postdates SDG, LS2e and Rioux entirely, and the reattach protocol is not in the
    official docs either — it exists in the proto comments and the client iterator's scaladoc. This
    topic is source-only, which is precisely why it is worth writing down.

!!! info "A healthy session looks like repeated stream failures"

    `spark.connect.execute.reattachable.senderMaxStreamDuration` defaults to **2 minutes**: the
    server ends each response stream cleanly and the client resumes with `ReattachExecute`, so no
    single gRPC stream lives long enough for a proxy or load balancer to time it out. Packet
    captures and gRPC access logs of a working long query show a sequence of short streams — that
    is the design, not a symptom.

!!! warning "The retry buffer is driver memory, per execution"

    `observerRetryBufferSize` (10 MB) of already-sent responses is retained behind the consumer for
    every reattachable execution so a reattach can replay. Many concurrent large-result queries
    multiply it. Disabling reattachable execution removes the buffer and the resilience together.

**Milestone:** You can explain why the server ends a response stream every two minutes and what the
client does next; name the three things that make a retried `ExecutePlan` safe (client-generated
operation id, response ids, `ReattachExecute`) and why an outer retry loop around the client is
*not* safe; describe what `ReleaseExecute` frees and what happens if a client never sends it; and
say what a client must do to lose a query permanently (stop polling for longer than
`detachedTimeout`, five minutes by default).

---


### ⬜ E19 — Spark Connect Artifacts: Shipping Code to a Remote Session

> Discovered from source sweep (new topic): `sql/connect: Artifacts — shipping JARs, classes and UDFs to a remote session`

**What it is:** How code reaches a Connect server: `addArtifact` chunks and hashes files over a streaming RPC, the server stages and verifies them, and each session gets an isolated classloader over its own artifact directory — plus the automatic class-file upload that makes a Scala closure work at all.

**Why you need it:** On a Connect session there is no shared JVM, so a UDF's class, its dependencies and any JAR you used to `--jars` must be transferred explicitly; not knowing this is the single most common reason working classic code fails on Connect.

**Learn it with:**

1. **Spark-docs → Spark Connect Overview** ([spark-connect-overview.html](https://spark.apache.org/docs/latest/spark-connect-overview.html)) — the "Client application development" section is the one place this is documented: **"JAR dependencies must be uploaded to the server using `SparkSession#addArtifact`"**, and a `ClassFinder` must be registered so user code's classfiles are picked up and uploaded
2. **Spark-docs → Configuration** ([configuration.html](https://spark.apache.org/docs/latest/configuration.html)) — `spark.connect.copyFromLocalToFs.allowDestLocal`, the one artifact-related knob
3. **Source sweep — [sql/connect — client-server in the source map](reference/spark-source-map/sweeps/sql-connect-client-server.md)** — the artifacts concept: 32 KB chunking over a client-streaming RPC, content hashing so an unchanged artifact is not re-sent, CRC verification before staged files are installed, and the **per-session classloader** that makes uploaded classes visible to execution
4. **Source sweep — [sql/core — the classic API in the source map](reference/spark-source-map/sweeps/sql-core-classic-api.md)** — the JVM side of what a Connect client uploads: `ArtifactManager` gives each session a directory named by its **session UUID** with a `classes/` subdirectory, builds a `URLClassLoader` over it, and installs that as the thread context classloader through `withResources` while setting the `JobArtifactState` tasks read. When `spark.sql.artifact.isolation.enabled` is false the state is `null` and everything falls back to the application classpath

!!! warning "No book covers this, and `--jars` does not apply"

    On a classic submit, `--jars` and `--packages` place dependencies on the driver and executors.
    A Connect client is a separate process that may not be on the cluster at all, so the equivalent
    is `spark.addArtifact(...)` against the **session**. Working classic code that fails on Connect
    with `ClassNotFoundException` is nearly always this, and none of the three books mentions it
    because all three predate Connect.

!!! warning "Artifacts die with the session"

    They are per session and per classloader, so a session idle timeout
    (`spark.connect.session.manager.defaultSessionTimeout`, 60 minutes) discards every uploaded JAR
    and class along with temp views and cached DataFrames. A notebook that worked before lunch and
    throws `ClassNotFoundException` after it has not lost its JAR — it has lost its session.

!!! info "Re-adding the same artifact is nearly free"

    The client hashes each artifact and asks the server via `ArtifactStatus` whether it already has
    it. Adding the same JAR repeatedly costs one round trip, not a re-upload — so defensive
    `addArtifact` calls at the top of a script are cheap and are the right habit.

**Milestone:** You can start a Connect session, define a Scala or Python UDF that references a class
from an external JAR, watch it fail, and fix it with `addArtifact`; explain why the same code needs
no such step on a classic submit; describe what a `ClassFinder` does in a REPL and why an
interactively defined lambda works without you uploading anything; and predict what happens to your
uploaded artifacts after an hour of inactivity.

---


### ⬜ E20 — JVM Profiling on a Cluster: async-profiler, Flame Graphs and JFR

> Discovered from source sweep (new topic): `connector/profiler: The async-profiler command strings and the default argument set`

**What it is:** Capturing CPU, wall-clock, allocation and lock profiles from driver and executor JVMs with Spark's built-in async-profiler plugin, shipping the resulting JFR files off the cluster, and reading them as flame graphs.

**Why you need it:** The Spark UI tells you which stage is slow and how much it spilled; it cannot tell you which method is burning the CPU or which lock is contended. Profiling is the only way to answer that on a real cluster, and Spark has shipped a plugin for it since 4.0 that almost nobody knows is there.

**Learn it with:**

1. **The module README** ([connector/profiler/README.md](https://github.com/apache/spark/blob/v4.2.0/connector/profiler/README.md)) — the *only* first-party documentation: build commands, the supported-platform list, the required JVM flags, the full config table and a complete `spark-submit` example. Start here, because the official docs site does not cover this plugin at all
2. **Async Profiler Manual** ([krzysztofslusarski.github.io](https://krzysztofslusarski.github.io/2022/12/12/async-manual.html)) — the reference the README itself points to for what the profiler actually does; Spark passes `spark.profiler.asyncProfiler.args` through untouched, so this is where `event=`, `interval=`, `alloc=` and `chunktime=` are specified
3. **async-profiler → Profiler Options** ([github.com/async-profiler/async-profiler](https://github.com/async-profiler/async-profiler/blob/v4.0/docs/ProfilerOptions.md)) — the option list for the v4.x line Spark bundles via `ap-loader`
4. **Spark-docs → Monitoring and Instrumentation** ([monitoring.html](https://spark.apache.org/docs/latest/monitoring.html)) — read the "Advanced Instrumentation" section for what Spark *does* document (jstack, jmap, Ganglia, the `SparkPlugin` API) and note the gap this topic fills
5. **Spark-docs → Tuning** ([tuning.html](https://spark.apache.org/docs/latest/tuning.html)) — the tuning advice a profile lets you target instead of guess at
6. **Source sweep — [connector/profiler — async-profiler in the source map](reference/spark-source-map/sweeps/connector-profiler-async-profiler.md)** — the whole module in seven concepts: the sampling draw and its quantisation, the silent no-op paths, the wall-clock default, the stop/dump/resume gap on every DFS sync, and the packaging that stops most people ever reaching it

!!! warning "No book covers this, and neither do the official docs"

    The plugin arrived in Spark 4.0, after SDG, LS2e and Rioux. It is also absent from
    `monitoring.html` — verified against the 4.2.0 docs, which cover the metrics system, the REST
    API and external tools like jstack and Ganglia but never mention `ProfilerPlugin`,
    `spark.profiler.*` or JFR. The module README is the only first-party source, which is why this
    topic leans on it and on async-profiler's own documentation.

!!! warning "Three things must line up before a single sample is taken"

    The module is behind the `jvm-profiler` Maven profile (and SBT's `optionallyEnabledProjects`),
    and its `ap-loader-all` native dependency is `provided` scope — so a standard distribution
    ships **neither**. You need a build that includes the module, the `ap-loader` bundle on the
    runtime classpath, and `spark.plugins=org.apache.spark.profiler.ProfilerPlugin`. Only then do
    `spark.profiler.driver.enabled` / `spark.profiler.executor.enabled` matter.

!!! warning "Set the JVM flags or the flame graph will lie to you"

    Without `-XX:+UnlockDiagnosticVMOptions -XX:+DebugNonSafepoints -XX:+PreserveFramePointer` on
    `spark.executor.extraJavaOptions`, the profiler still runs and still produces a plausible-looking
    flame graph — with truncated or misattributed stacks. This is the single most important
    prerequisite, and it is easy to skip because nothing warns you.

!!! info "The default is a wall-clock profile, not a CPU profile"

    `event=wall` samples all threads including blocked ones. For Spark that is usually the right
    question — a slow stage is often waiting on I/O, a lock or a shuffle fetch — but if you are
    hunting CPU burn you must set `event=cpu` explicitly. The default arguments also turn on
    allocation profiling every 2 MB and lock profiling at 10 ms, so one run answers several
    questions at once.

**Milestone:** You can build Spark with `-Pjvm-profiler`, run a job with the plugin enabled and
`spark.profiler.dfsDir` set, and retrieve a JFR file per profiled executor; open it as a flame graph
and name the hottest method in a stage you already knew was slow. You can explain why
`spark.profiler.executor.fraction=0.005` does not profile one executor in two hundred; say what is
lost every `dfsWriteInterval` seconds and why that trade cannot be avoided; state the three silent
reasons a JFR file might never appear; and name the Kubernetes setting without which the tail of
every profile is lost.

---


### ⬜ E21 — Connecting to an External Hive Metastore: Versions, Isolated Classloaders and Jars

> Discovered from source sweep (new topic): `sql/hive: Two Hive versions — the bundled client and the metastore it talks to`

**What it is:** How Spark talks to a metastore it was not compiled against: `spark.sql.hive.metastore.version` selects a version shim, `spark.sql.hive.metastore.jars` supplies that version's jars, and an isolated classloader keeps them from colliding with Spark's own Hive — with `sharedPrefixes` and `barrierPrefixes` as the escape hatches.

**Why you need it:** Spark 4.2 bundles Hive 2.3.10 but can talk to metastores from 2.0 to 4.1, and getting that pairing wrong produces classloader errors that look like nothing else in Spark. It is the first thing to configure when attaching Spark to an existing data platform, and the configs are all static — you cannot fix it on a running session.

**Learn it with:**

1. **Spark-docs → Hive Tables** ([sql-data-sources-hive-tables.html](https://spark.apache.org/docs/latest/sql-data-sources-hive-tables.html)) — the reference for this topic: where `hive-site.xml`, `core-site.xml` and `hdfs-site.xml` go, and the "Interacting with Different Versions of Hive Metastore" section documenting all six configs. Note its key sentence — *independent of the metastore version, Spark compiles against the built-in Hive for SerDes, UDFs and UDAFs* — which is exactly the two-version split this topic is about
2. **Spark-docs → Configuration** ([configuration.html](https://spark.apache.org/docs/latest/configuration.html)) — confirm for yourself that all six are listed as static; `spark.sql.warehouse.dir` also belongs here, having replaced `hive.metastore.warehouse.dir` in Spark 2.0
3. **Source sweep — [sql/hive — hive-metastore in the source map](reference/spark-source-map/sweeps/sql-hive-hive-metastore.md)** — the isolated-classloader concept: the three-way split into **barrier** classes (redefined per client), **shared** classes (Spark's copy — all of `org.apache.hadoop.` *except* `org.apache.hadoop.hive.`) and everything else loaded from the isolated jars, plus the shim ladder (`Shim_v2_0` … `Shim_v4_1`, each extending the previous) that absorbs the per-version API differences

!!! warning "All six configs are static — `spark.conf.set` does nothing and says nothing"

    `spark.sql.hive.version`, `.metastore.version`, `.metastore.jars`, `.metastore.jars.path`,
    `.metastore.sharedPrefixes` and `.metastore.barrierPrefixes` are `buildStaticConf`. They are read
    once when the session's Hive client is built, so setting them in a notebook is silently
    ineffective. This is a `spark-submit` / cluster-config decision.

!!! info "`sharedPrefixes` is the fix for a metastore database driver"

    The classic failure: your metastore is backed by a database whose JDBC driver Spark does not
    list as shared, so the driver class is loaded twice — once on each side of the isolation
    boundary — and you get a `ClassNotFoundException` or a `ClassCastException` naming it. Adding
    its package to `spark.sql.hive.metastore.sharedPrefixes` is the intended remedy.
    `barrierPrefixes` is the mirror image, for your own classes that must bind to the isolated Hive.

!!! warning "No book covers this"

    All three books assume the default `builtin` metastore. The version-shim and classloader
    machinery only matters when Spark meets a metastore someone else operates — which is most
    enterprise deployments, and none of the books' subject matter.

**Milestone:** You can point a Spark session at a metastore of a different version than the bundled
Hive, using `spark.sql.hive.metastore.version` plus `jars=path`, and confirm from the driver log
that the isolated loader started. You can explain why `spark.sql.hive.version` cannot be set at all;
say which three buckets a class can fall into and what decides each; diagnose a doubled JDBC-driver
class to the right config; and state why none of this can be fixed without restarting the
application.

---


### ⬜ E22 — Columnar Execution and the ColumnarRule Plugin API
> Discovered from source sweep (new topic): `sql/core: Columnar execution and the ColumnarRule plugin API`

**What it is:** Spark's physical plan is not uniformly row-based. Each operator declares `supportsColumnar` / `supportsRowBased`, and a preparation rule walks the tree inserting `ColumnarToRowExec` and `RowToColumnarExec` wherever the two formats meet. `ColumnarRule` is the `SparkSessionExtensions` hook that runs immediately before and after that insertion pass — the seam a plugin uses to replace row operators with its own columnar ones.

**Why you need it:** Every accelerated Spark backend — Apache Comet, Gluten, the RAPIDS plugin — plugs in exactly here. Understanding the seam tells you what those products can and cannot replace, why a partly-accelerated query shows transitions in the middle of its plan, and how to read `ColumnarToRow` in an `EXPLAIN` as a real cost rather than noise. It is also the API to reach for if you ever need to inject an operator implementation of your own.

**Learn it with:**

1. **No book covers this.** The `ColumnarRule` API is a 3.0-era extension point that no Spark book on the list treats.
2. **Spark-docs → SQL Data Sources: Parquet** ([sql-data-sources-parquet.html](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html)) — the vectorized reader (`spark.sql.parquet.enableVectorizedReader`) is the most common producer of columnar batches, and therefore the most common reason a transition appears
3. **Spark-docs → Runtime SQL Configuration** ([configuration.html#runtime-sql-configuration](https://spark.apache.org/docs/latest/configuration.html#runtime-sql-configuration)) — `spark.sql.columnVector.offheap.enabled` (which defaults to `spark.memory.offHeap.enabled`) and `spark.sql.inMemoryColumnarStorage.batchSize`, reused as the row-to-columnar batch size
4. **Source sweep — [sql/core — query-execution in the source map](reference/spark-source-map/sweeps/sql-core-query-execution.md)** — the transition rule (`ApplyColumnarRulesAndInsertTransitions`) and its two mutually recursive halves, the ordering detail that pre-rules run in declaration order while post-rules run in **reverse**, and the interaction with whole-stage codegen: codegen is row-based, so a columnar child makes the generated stage a pass-through
5. **Source sweep — [sql/core — datasources in the source map](reference/spark-source-map/sweeps/sql-core-datasources.md)** — where the columnar boundary is decided for the Parquet reader specifically: `ParquetUtils.isBatchReadSupportedForSchema` tests the config **and** every field of the result schema, with nested types gated separately by `enableNestedColumnVectorizedReader` and `NullType` by `enableNullTypeVectorizedReader`. Partition columns are supplied as `ConstantColumnVector`s so they cost no per-row work, and the on-heap/off-heap choice comes from `spark.sql.columnVector.offheap.enabled`
6. **Source sweep — [sql/core — Python and Arrow in the source map](reference/spark-source-map/sweeps/sql-core-python-arrow.md)** — the Python end of columnar execution: when the child produces Arrow-backed `ColumnarBatch`es and `spark.sql.execution.arrow.pythonUDF.columnarInput.enabled` is on (default `true`, new in 4.2.0), `ArrowEvalPythonExec` declares `supportsColumnar` and switches to a columnar-in/columnar-out evaluator that skips `ColumnarToRow` and the `ArrowWriter` entirely — a concrete, shipped example of a columnar operator boundary
7. **Source sweep — [sql/core — the classic API in the source map](reference/spark-source-map/sweeps/sql-core-classic-api.md)** — the *other* columnar plugin point, worth contrasting with `ColumnarRule`: `CachedBatchSerializer` (`spark.sql.cache.serializer`) replaces the cache **storage** format rather than execution, and its `buildFilter` turns a predicate into a test over cached-batch statistics so whole batches are skipped without decoding. `convertToColumnarPlanIfPossible` is the hook an accelerator uses to swap the plan under the cache

**Milestone:** Read a Parquet file with the vectorized reader on, and find `ColumnarToRow` in the physical plan. Say which operator forced it. Then turn off `spark.sql.parquet.enableVectorizedReader` and show the transition disappearing along with the columnar scan. Finally, write a trivial `ColumnarRule` registered through `SparkSessionExtensions` that logs the plan it receives, and confirm from the log that `preColumnarTransitions` sees the tree *before* any transition node exists.

---


### ⬜ E23 — Transactional Writes: DSv2 Catalog Transactions
> Discovered from source sweep (new topic): `sql/core: Transaction-scoped query execution`

**What it is:** New in Spark 4.2.0. When a query's plan writes to a catalog implementing `TransactionalCatalogPlugin`, `QueryExecution` opens a transaction **before analysis**, clones the analyzer with a transaction-aware `CatalogManager`, threads it through every phase, and commits inside the V2 write operator or aborts on any failure. There is no `BEGIN` / `COMMIT` in SQL — participation is decided entirely by the connector.

**Why you need it:** It is how a multi-table DSv2 write becomes atomic, and it changes **analysis**, not just execution: catalog lookups during resolution happen inside the transaction's scope, and relations resolved outside one are deliberately un-resolved so they get re-read inside it. If you build or operate a DSv2 connector, this is the contract you either implement or are excluded from. If you consume one, it is the reason a failed write can leave nothing behind where an older Spark left a half-written table.

**Learn it with:**

1. **No book covers this** — the API landed in 4.2.0 (2026).
2. **Spark-docs → Data Sources V2** ([sql-data-sources-v2.html](https://spark.apache.org/docs/latest/sql-data-sources-v2.html)) — the DSv2 catalog interfaces `TransactionalCatalogPlugin` extends; read this first or the transaction API has no context
3. **Spark-docs → Spark SQL Migration Guide** ([sql-migration-guide.html](https://spark.apache.org/docs/latest/sql-migration-guide.html)) — check the 4.2 section for behaviour changes to DSv2 writes before assuming an existing connector is unaffected
4. **Source sweep — [sql/core — query-execution in the source map](reference/spark-source-map/sweeps/sql-core-query-execution.md)** — the lifecycle in `QueryExecution`: the `TransactionalWrite` match on the **unresolved** plan, why `mode != SKIP` means `EXPLAIN` never opens a transaction, the `analyzerOpt` constructor parameter whose own signature warns that a nested `QueryExecution` omitting it "will load tables outside the transaction's catalog scope", and `withAbortTransactionOnFailure` wrapping every phase accessor rather than only execution
5. **Source sweep — [sql/core — datasources in the source map](reference/spark-source-map/sweeps/sql-core-datasources.md)** — where the transaction hook sits in the physical write: `TransactionalExec` and `V2ExistingTableWriteExec` in `WriteToDataSourceV2Exec`, above the two-phase commit (`DataWriter.commit()` per task producing a `WriterCommitMessage`, then one driver-side `batchWrite.commit(messages)`). Also `RowLevelWriteExec` and `MergeRowsExec`, the `MERGE`/`UPDATE`/`DELETE` physical path the transaction API has to cover
6. **Source sweep — [sql/core — aggregation, windows and exchange in the source map](reference/spark-source-map/sweeps/sql-core-agg-window-exchange.md)** — `RowLevelOperationRuntimeGroupFiltering` — the runtime group filter a DSv2 row-level operation gets when its scan implements `SupportsRuntimeV2Filtering`, and the cost argument for it: the filter query projects only the condition's columns, the main scan must project all of them
7. **Source sweep — [sql/core — the classic API in the source map](reference/spark-source-map/sweeps/sql-core-classic-api.md)** — the three DataFrame writer APIs and what distinguishes them: `DataFrameWriter` (`df.write`, `SaveMode`-based, V1 and V2), `DataFrameWriterV2` (`df.writeTo`, explicit `create`/`replace`/`append`/`overwrite` verbs and no `SaveMode` ambiguity), and `MergeIntoWriter` (`df.mergeInto`, building the `whenMatched` / `whenNotMatched` / `whenNotMatchedBySource` clause lists)

**Milestone:** Trace one DSv2 write end to end and name the four points: where the transaction is begun, where the analyzer is cloned, where the transaction is attached to the physical plan, and where it is committed. Then say what a connector must implement to participate, and explain why an `EXPLAIN` of the same statement opens no transaction at all.

---


### ⬜ E24 — Extending AQE: The Four Rule Injection Points

> Discovered from source sweep (new topic): `sql/core: The four AQE rule injection points`

**What it is:** `SparkSessionExtensions` exposes four distinct AQE hooks, collected into an `AdaptiveRulesHolder` and run at four different points of the AQE loop:

| Injection point | Rule type | Runs | Plan it sees |
|---|---|---|---|
| `injectQueryPostPlannerStrategyRule` | `Rule[SparkPlan]` | between planner strategies and preparation | whole plan, **before** exchanges are inserted |
| `injectQueryStagePrepRule` | `Rule[SparkPlan]` | end of the preparation rules | whole plan, exchanges final |
| `injectRuntimeOptimizerRule` | `Rule[LogicalPlan]` | last batch of `AQEOptimizer` | logical plan with `LogicalQueryStage`s and runtime statistics |
| `injectQueryStageOptimizerRule` | `Rule[SparkPlan]` | end of the stage-optimizer rules | one stage's child, per stage |

**Why you need it:** It is how Iceberg, Delta and every accelerator plugin change execution behaviour *at runtime* rather than at planning time, and it is the only hook family where runtime statistics are available to your rule. Picking the wrong one fails quietly: a stage-optimizer rule that extends `AQEShuffleReadRule` is auto-reverted (at `DEBUG`) whenever `ValidateRequirements` says it broke a distribution requirement, while the same logic as a prep rule runs once per re-plan and is bound by the contract that prep rules must not add or remove an `Exchange`.

**Learn it with:**

1. **No book covers this** — `SparkSessionExtensions` appears in the books only as "how Delta/Iceberg register themselves"; none of them enumerate the AQE hooks or their ordering.
2. **Spark-docs → SQL performance tuning: AQE** ([sql-performance-tuning.html#adaptive-query-execution](https://spark.apache.org/docs/latest/sql-performance-tuning.html#adaptive-query-execution)) — the built-in rules your injected rule runs alongside, and the phase names the API's Scaladoc refers to
3. **Source sweep — [sql/core — adaptive in the source map](reference/spark-source-map/sweeps/sql-core-adaptive.md)** — the "four AQE rule injection points" concept, plus the two concepts you need to place a rule correctly: "the two rule lists" (exact ordering, and the no-new-Exchange contract) and "AQEShuffleReadRule and ValidateRequirements" (the silent auto-revert)
4. **Related:** E22 — Columnar Execution and the `ColumnarRule` Plugin API, the other `SparkSessionExtensions` hook that runs inside the AQE loop (`postStageCreationRules`)

**Milestone:** Register a `SparkSessionExtensions` implementation through `spark.sql.extensions` (remembering it is a **static** config — set it at session creation) that injects one no-op logging rule at each of the four points. Run a query with at least two shuffles and, from the log, put the four hooks in the order they actually fired and state how many times each ran. Then make the runtime-optimizer rule read `LogicalQueryStage.computeStats()` and print the materialized size — proving that hook, and only that hook, sees runtime statistics.

---


### ⬜ E25 — Column Matching Between File and Table Schema: by Name, by Position, by Field ID

> Discovered from source sweep (new topic): `sql/core: Column matching between file and table schema`

**What it is:** A table schema says "the third column is `amount`". A file says "I have a column called `amount`". Matching the two is a per-format decision, and every format in Spark does it differently:

| Format | Default | Switches to |
|---|---|---|
| Parquet | by **name** | by **field ID** when `spark.sql.parquet.fieldId.read.enabled` (writes IDs by default) |
| ORC | by **name** | by **ordinal** when `orc.force.positional.evolution` **or every field is named `_col*`** |
| Avro | by **name** | by **position** with the `positionalFieldMatching` option |
| CSV | by **position**, header skipped | by **name** with `enforceSchema=false` |
| `insertInto` (write) | by **position** | — use `saveAsTable` for by-name |

**Why you need it:** Every one of these produces *wrong data* rather than an error when its rule differs from your assumption. This is the mechanism behind an entire class of production incident — a column added in the middle of a table schema, a Hive-written ORC file read by Spark, a renamed column, a CSV whose producer reordered its columns — where the query still runs, still returns rows, and returns them from the wrong columns. Knowing which rule is in force is the difference between a five-minute fix and a data-quality investigation.

**Learn it with:**

1. **No book covers this as a family** — SDG Ch 9 documents `enforceSchema` and LS2e mentions `mergeSchema`, but nothing connects the five rules or names the `_col*` heuristic.
2. **Spark-docs → Parquet** ([sql-data-sources-parquet.html](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html)) — schema merging and the field-ID options; **ORC** ([sql-data-sources-orc.html](https://spark.apache.org/docs/latest/sql-data-sources-orc.html)); **Avro** ([sql-data-sources-avro.html](https://spark.apache.org/docs/latest/sql-data-sources-avro.html)) for `positionalFieldMatching`
3. **Source sweep — [sql/core — datasources in the source map](reference/spark-source-map/sweeps/sql-core-datasources.md)** — the "column matching" concept, which is where each rule actually lives: `OrcUtils.requestedColumnIds` and its `orcFieldNames.forall(_.startsWith("_col"))` test, the case-insensitive-ambiguity error, and the `TimestampType`/`TimestampNTZType` mismatch that is checked *before* any matching happens
4. **Source sweep — [sql/catalyst — types & parser in the source map](reference/spark-source-map/sweeps/sql-catalyst-types-parser.md)** — the CSV half: `enforceSchema=true` (the default) skips the header rather than validating it
5. **Source sweep — [sql/hive — hive-metastore in the source map](reference/spark-source-map/sweeps/sql-hive-hive-metastore.md)** — why a Hive-written ORC table is the common case for positional matching, and where the two reader paths diverge
6. **Related:** B4's `insertInto` warning, B5 (schema), I10 (formats), and I8/I11 — column mapping by ID is exactly what Delta and Iceberg add to make rename safe

**Milestone:** Write an ORC file whose columns are named `_col0, _col1`, read it with a table schema whose column order differs, and show that the values come back transposed rather than erroring. Then do the same experiment with Parquet and explain why it behaves differently. Finally, take one real table you own and state, for each of read and write, which matching rule is in force and what would happen if someone inserted a column in the middle of its schema.

---


### ⬜ E26 — transformWithStateInPySpark: The Per-Task State Server

> Discovered from source sweep (new topic): `sql/core: transformWithStateInPySpark — Python drives the state store over a socket`

**What it is:** Arbitrary stateful processing in PySpark runs a second server thread per task — TransformWithStateInPySparkStateServer — that listens on a dedicated TCP or Unix-domain socket and answers protobuf-framed state requests from the Python worker, so every ValueState/ListState/MapState get or put and every timer registration is a synchronous round trip into the JVM state store.

**Why you need it:** It explains the performance shape of stateful PySpark: the Arrow data path is batched but the state path is one request per operation, so a processor touching state per row behaves nothing like one touching it per group — and it is a second socket, a second thread and a protobuf schema in the failure path of every stateful Python task.

**Learn it with:**

1. **Spark-docs → Arbitrary Stateful Processing with transformWithState** ([streaming/structured-streaming-transform-with-state.html](https://spark.apache.org/docs/latest/streaming/structured-streaming-transform-with-state.html)) — the API reference, and it does cover both Python forms (`transformWithStateInPandas` and the Row-interface `transformWithState`): `ValueState` / `ListState` / `MapState`, `getValueState` with an explicit schema, `registerTimer` / `listTimers` / `deleteTimer` / `handleExpiredTimer`, and `TTLConfig`. Note the Python-specific requirement it calls out — **state encoders must be given explicitly**, unlike Scala's implicits
2. **Spark-docs → Structured Streaming Programming Guide, Arbitrary Stateful Operations** ([streaming/apis-on-dataframes-and-datasets.html#arbitrary-stateful-operations](https://spark.apache.org/docs/latest/streaming/apis-on-dataframes-and-datasets.html#arbitrary-stateful-operations)) — the context: `mapGroupsWithState` / `flatMapGroupsWithState` are now explicitly the legacy operators, and `transformWithState` is the recommended path since 4.0
3. **No book covers this** — `transformWithState` landed in Spark 4.0 and the PySpark form later still; all three books stop at `flatMapGroupsWithState` in Scala
4. **Source sweep — [sql/core — Python and Arrow in the source map](reference/spark-source-map/sweeps/sql-core-python-arrow.md)** — the "transformWithStateInPySpark" concept, which is the architecture the docs do not describe: a per-task server thread on its own TCP (with `TCP_NODELAY`) or Unix-domain socket, protobuf `StateRequest` framing, the full `handleRequest` dispatch (implicit grouping key, stateful-processor calls, per-variable get/put/clear, map iterators, timers, utils), and `TransformWithStateInPySparkExec` declaring column-family schemas to the driver before any task runs
5. **Compare with the older design** — the same sweep's "applyInPandasWithState" concept encodes state *into the Arrow stream* as extra columns with a nested metadata schema. Reading the two side by side is the fastest way to see why arbitrary state access needed a second channel: the in-stream encoding only works when state is read and written at group boundaries
6. **Prerequisites:** A8 (stateful streaming semantics — watermarks, output modes, the state store) and I3 (the Python worker boundary). This topic is the intersection of the two

!!! warning "The data path is batched; the state path is not"

    Rows reach the Python processor in Arrow batches. Every state operation inside that processor
    is a separate synchronous protobuf round trip over the socket. A processor that reads and
    writes state once per group performs like the JVM operator; one that touches state per row
    pays a socket round trip per row. Spark ships its own benchmark for this server
    (`BenchmarkTransformWithStateInPySparkStateServer`), which is itself the strongest evidence
    that the round-trip cost is the thing to design around.

**Milestone:** Write a `StatefulProcessor` in PySpark using a `ValueState` and a processing-time timer, run it, and confirm from the executor thread dump that a state-server thread exists per task. Then write two variants of the same logic — one reading and writing state once per group, one per row — and state the throughput difference and why it is not a data-serialization effect. Finally, set `spark.python.unix.domain.socket.enabled` and confirm from the source-sweep page which socket type the server then opens.

---


### ⬜ E27 — The State Store Engine: RocksDB, Changelog Checkpointing, and Maintenance

> Discovered from source sweep (new topic): `sql/core: RocksDBStateStoreProvider — changelog checkpointing and the snapshot upload queue`

**What it is:** Behind every stateful streaming operator is a versioned key-value store; the RocksDB provider keeps a local instance per partition, writes each batch's mutations to a changelog file, periodically uploads a full snapshot in a background maintenance thread, and reconstructs any version by loading the nearest snapshot and replaying changelogs on top of it.

**Why you need it:** It is the layer that decides whether a stateful query restarts in seconds or in an hour, whether a corrupt checkpoint is recoverable, and what the memory footprint of a large-state job actually is — and its whole configuration surface (changelog checkpointing, snapshot lag reporting, checkpoint IDs, row checksums, auto snapshot repair, maintenance timeouts) is invisible from the query API.

**Learn it with:**

1. **Spark-docs → Performance Tips / RocksDB State Store** ([streaming/apis-on-dataframes-and-datasets.html#rocksdb-state-store-implementation](https://spark.apache.org/docs/latest/streaming/apis-on-dataframes-and-datasets.html#rocksdb-state-store-implementation)) — the official configuration guidance: `spark.sql.streaming.stateStore.providerClass`, bounded memory (`…rocksdb.boundedMemoryUsage`, `…maxMemoryUsageMB`, `…writeBufferSizeMB`, `…maxWriteBufferNumber`) and `…rocksdb.changelogCheckpointing.enabled`. Start here, then note how much of the surface below it does *not* mention
2. **No book covers this** — SDG and LS2e both predate the RocksDB provider being the practical default, and neither discusses changelog checkpointing, snapshot upload, or state-store maintenance at all. Rioux does not cover stateful streaming
3. **Source sweep — [sql/core — streaming execution in the source map](reference/spark-source-map/sweeps/sql-core-streaming-exec.md)** — the substantive reference. Read three of its concepts in order: "The StateStore API" (versions, column families, key encoder specs, the commit that returns a number), "RocksDBStateStoreProvider" (changelog vs snapshot, the upload queue, `RocksDBFileManager`'s immutable-file sharing, why recovery cost is proportional to changelog distance), and "The maintenance thread" (snapshotting, the deletion budget, unloading). Then the two 4.1.0 defences — row checksums (off by default) and auto snapshot repair (on)
4. **Same sweep, added by the 2026-08-09 re-sweep — "The RocksDB tuning surface"** — read this before touching any `…rocksdb.*` setting. Every field of `RocksDBConf` except two is an **undeclared string key**: names and defaults exist only in `RocksDB.scala`, lookup is a lower-cased `getOrElse(default)` over a prefix sweep of the session's confs, and a misspelled key therefore falls back to its default with no warning anywhere. The concept also covers what `boundedMemoryUsage` actually changes (one shared block cache and write-buffer manager per executor instead of one per partition). "Range-scan key encoding" on the same page is the encoder beneath every TTL and timer scan
5. **Related:** **A36** for the offset/commit-log protocol the state store checkpoints alongside, **E28** for changing the partition count, **E47** for evolving a state schema instead of rebuilding the checkpoint, **I6** for the memory model the HDFS-backed provider competes with
6. **Related config family:** `spark.sql.streaming.stateStore.*` — read `StateStoreConf.scala` for the declared configs, and **`RocksDBConf` in `RocksDB.scala` for the `…rocksdb.*` ones**, which are not declared anywhere and so appear in neither `SET -v` nor any generated documentation. Only two — `…rocksdb.formatVersion` and `…rocksdb.mergeOperatorVersion` — are real `ConfigEntry` values

!!! warning "Recovery cost is changelog distance, not state size"

    With changelog checkpointing on, a commit writes only that batch's mutations, and full
    snapshots are produced asynchronously by the maintenance thread. Loading version N means
    loading the nearest snapshot and replaying every changelog since. So a partition whose
    snapshot upload has been lagging replays thousands of files on restart while its neighbours
    take seconds. That is what the coordinator's snapshot-lag reporting exists to surface
    (`…coordinatorReportSnapshotUploadLag`, on by default since 4.1.0) and what 4.2.0's
    `…forceSnapshotUploadOnLag` (default `true`) exists to prevent.

!!! info "Two corruption defences, two different defaults — and that is deliberate"

    `…rowChecksum.enabled` is **`false`**: it verifies stored values on read and costs on every
    read. `…autoSnapshotRepair.enabled` is **on** outside tests: a corrupt snapshot makes Spark
    walk back to an older one and replay more changelog (bounded by `…maxChangeFileReplay`, 500).
    The second means a restart can take far longer than usual with nothing logged as fatal —
    worth recognising before you go looking for a hung query.

**Milestone:** Run a stateful query on the RocksDB provider with changelog checkpointing on, let it run past several maintenance intervals, then inspect the `state/` directory: identify the changelog files, the snapshot files, and which version each snapshot corresponds to. Restart the query and time it. Then delete the newest snapshot, restart again, and state from the logs how many changelog files were replayed and whether auto snapshot repair engaged. Finally, find `numTotalStateRows` and the state-store instance metrics for that query in the SQL tab and say which partition holds the most state.

---


### ⬜ E28 — Offline State Repartition: Changing shuffle.partitions on a Stateful Query

> Discovered from source sweep (new topic): `sql/core: Offline state repartition — changing a stateful query's partition count`

**What it is:** A stateful streaming query's state is keyed by partition id, so its shuffle partition count is frozen at the first batch; Spark 4.2.0 adds an offline runner that reads the existing state through the state data source, repartitions it to a new count, writes it back as an extra batch N+1, and lets the query resume at the new parallelism.

**Why you need it:** Until this existed, the answer to "my stateful query is under-parallelised" was to rebuild the checkpoint and reprocess from the source. It is the single highest-consequence operational procedure in streaming, it leaves a half-finished batch behind if it fails, and 4.2.0 ships a startup check specifically to detect that.

**Learn it with:**

1. **No book covers this, and the docs are thin** — the feature is new in Spark 4.2.0 (2026). Check the 4.2.0 release notes for the SPARK issue and any procedure Databricks or your vendor publishes; the Apache docs do not yet carry a runbook. Verify everything on your own stack before running it against a production checkpoint.
2. **Source sweep — [sql/core — streaming execution in the source map](reference/spark-source-map/sweeps/sql-core-streaming-exec.md)** — the "Offline state repartition" concept is the substantive reference: `OfflineStateRepartitionRunner` reads existing state through the state data source, repartitions on `partition_key`, writes it back with `StatePartitionAllColumnFamiliesWriter`, and records the result as batch **N+1** so the query picks up the new count on its next start. Read it alongside the same page's "IncrementalExecution" concept, which is *why* the procedure is needed at all
3. **Prerequisites:** **E27** (what is actually being rewritten) and **A36** (the batch N+1 model only makes sense once the offset/commit-log protocol does). Also read the `datasources` sweep's state-source entry — the read side of this is the same `state` DSv2 source you use to debug a checkpoint
4. **Related config:** `spark.sql.streaming.checkUnfinishedRepartitionOnRestart` (**`true`**, 4.2.0) — leave it on; it is what stops a query resuming onto half-rewritten state

!!! warning "This mutates a checkpoint. Treat it as such."

    A stateful query's shuffle partition count is frozen at its first batch because
    `IncrementalExecution` pins `numShufflePartitions` to the value in the checkpoint — changing
    `spark.sql.shuffle.partitions` on a running stateful query does nothing at all. Offline
    repartition is the supported way to change it, and it rewrites every column family of every
    state store. Back up the checkpoint directory first, run it with the query stopped, and verify
    the resulting batch id before restarting.

**Milestone:** Take a stateful query with a small state, note its partition count from the state-metadata source, stop it, and run the offline repartition to a different count. Then confirm three things: the checkpoint's newest batch is the repartition batch, `spark.sql.shuffle.partitions` still has no effect on the running query, and the restarted query reports the new partition count. Separately, state what `checkUnfinishedRepartitionOnRestart` would detect if the runner were killed mid-way.

---


### ⬜ E29 — SparkSessionExtensions: The Sixteen Injection Points

> Discovered from source sweep (new topic): `sql/core: BaseSessionStateBuilder — the whole SparkSessionExtensions injection surface`

**What it is:** SparkSessionExtensions is the supported way to change what Spark does without forking it: sixteen inject* methods covering the parser, five analyzer hook positions, the optimizer and a pre-CBO slot, planner strategies, four AQE hooks, columnar rules, plan normalization, and function and table-function registration — all consumed in one place, BaseSessionStateBuilder.

**Why you need it:** Every table format and accelerator you might deploy — Delta, Iceberg, Comet, RAPIDS — attaches here, and the config that loads them is static, so mis-registration fails silently. Knowing the full surface is also what lets you write a targeted rule instead of a fragile workaround, and knowing where each hook runs is what stops the rule firing at the wrong time.

**Learn it with:**

1. **Spark-docs → `SparkSessionExtensions` ScalaDoc** ([api/scala/org/apache/spark/sql/SparkSessionExtensions.html](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/SparkSessionExtensions.html)) — the primary and effectively only official reference: the two registration styles (`Function1[SparkSessionExtensions, Unit]` or `SparkSessionExtensionsProvider`), the `spark.sql.extensions` comma-separated form, and the warning that matters — **an extension builder must not touch the session's internals**, because the session is not fully initialised when it runs
2. **Spark-docs → `SparkSessionExtensionsProvider` ScalaDoc** ([api/scala/org/apache/spark/sql/SparkSessionExtensionsProvider.html](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/SparkSessionExtensionsProvider.html)) — the service-loader form, which is how a jar registers itself without the user setting a config
3. **No book covers this** — SDG, LS2e and Rioux mention `spark.sql.extensions` only as "how Delta and Iceberg register themselves"; none enumerates the hooks or says where each one runs
4. **Source sweep — [sql/core — the classic API in the source map](reference/spark-source-map/sweeps/sql-core-classic-api.md)** — the "BaseSessionStateBuilder" concept, which is the complete list in the order the builder consumes it, plus the detail the ScalaDoc omits: `injectParser` receives the built-in `SparkSqlParser` as a delegate, so a custom parser is a **decorator**, and the standard pattern is try-then-delegate
5. **Related topics for individual hooks:** **E22** (`injectColumnar` — the `ColumnarRule` seam) and **E24** (the four AQE hooks and their ordering) go deeper on five of the sixteen. **A1** for what an analyzer or optimizer rule is; **A24** for what a parser extension has to do
6. **Related:** the classic-api sweep's "CachedBatchSerializer" concept is a *different* plugin mechanism — a config-named class rather than an extension — worth contrasting so you reach for the right one

!!! warning "`spark.sql.extensions` is a static config, and the failure is silent"

    It is read once while the session is being constructed. Setting it afterwards with
    `spark.conf.set(...)` does nothing at all — no error, no warning. The symptom is "my SQL
    syntax isn't recognised" or "my rule never fires", which points nowhere near the cause. Set it
    on the builder, in `spark-defaults.conf`, or on the `spark-submit` command line.

!!! info "Where each hook runs is the other half of the knowledge"

    A resolution rule runs inside the analyzer's fixed-point loop; a post-hoc rule runs once after
    resolution; a check rule cannot rewrite, only reject; an optimizer rule runs on the resolved
    plan and a pre-CBO rule before cost-based decisions; a planner strategy competes with the
    built-ins and the *first* match wins; and the four AQE hooks run at four different points of
    the runtime loop. A correct rule in the wrong position is the common failure.

**Milestone:** Write an extension class that injects one resolution rule which logs the plan it sees, register it two ways — via `spark.sql.extensions` on the builder and via `withExtensions` — and confirm from the log that it fires. Then demonstrate the trap: set the same config with `spark.conf.set` on an already-built session and show nothing happens. Finally, add an `injectPlannerStrategy` that matches a node the built-in strategies also handle, and say from the plan which one won and why.

---


### ⬜ E30 — Pipeline Run Semantics: Flow States, Retry, and Downstream Skipping

> Discovered from source sweep (new topic): `sql/pipelines: TriggeredGraphExecution — the topological state machine and flow retry`

**What it is:** The eight states a flow moves through in a triggered pipeline run, the exponential-backoff retry budget that governs re-execution, the concurrency semaphore that bounds how many flows run at once, and the rule that skips every downstream flow once an upstream one is out of retries.

**Why you need it:** A pipeline run reports one outcome for many flows, and whether that outcome is COMPLETED or FAILED is decided entirely by this state machine — including the counter-intuitive rule that a run whose flows were all SKIPPED still reports success.

**Learn it with:**

1. **Source sweep — [sql/pipelines — graph](reference/spark-source-map/sweeps/sql-pipelines-graph.md)** — the *TriggeredGraphExecution*, *Concurrency limiting* and *RunTerminationReason* sections. Four things to carry away: the trigger is always `AvailableNow`, so a triggered run drains and stops; downstream flows are skipped only once retries are **exhausted**, not on first failure; `determineFlowExecutionActionFromError` ignores the exception entirely and branches only on the retry count, so a permissions error and a network blip get identical treatment; and a run in which everything was `SKIPPED` or `EXCLUDED` reports **COMPLETED**
2. **Spark-docs → Declarative Pipelines Programming Guide, Refresh Selection Behavior** ([declarative-pipelines-programming-guide.html](https://spark.apache.org/docs/latest/declarative-pipelines-programming-guide.html#refresh-selection-behavior)) — how a partial refresh is requested, which is what puts flows into `EXCLUDED` rather than `QUEUED`
3. **Config reference** — the four run-shaping keys, all added in 4.1.0: `spark.sql.pipelines.execution.maxConcurrentFlows` (16), `spark.sql.pipelines.maxFlowRetryAttempts` (2, and **overridable per flow** via that flow's own SQL conf), and the `execution.watchdog.{min,max}RetryTime` pair (5 s / 3600 s) that parameterises the exponential backoff
4. **Local stack** — run a four-dataset pipeline where the second dataset fails deterministically. Watch the retry backoff in the driver log, confirm the downstream two are `SKIPPED` only after attempt three, then set `maxFlowRetryAttempts=0` and observe the run fail immediately. Then request a partial refresh of the *last* dataset alone and check what the run reports
5. **Source sweep — [sql/pipelines — pipeline-runtime in the source map](reference/spark-source-map/sweeps/sql-pipelines-pipeline-runtime.md)** — two mechanics this topic depends on. The backoff curve is `min(maxRetryTime, 2^(n-1) × minRetryTime)`, which with the defaults (5 s / 3600 s / 2 attempts) means the only waits ever used are **5 s and 10 s** — raising `maxRetryTime` alone changes nothing, and the ceiling is unreachable until `maxFlowRetryAttempts` is around ten. And **per-flow SQL confs are not isolated during execution**: `withSqlConf` sets and restores on the one shared session while up to `maxConcurrentFlows` flows start concurrently, so two flows setting the same key race — and for a batch flow the restore happens as soon as the write is *submitted*, before it plans. A per-flow conf meant to shape the write is unlikely to apply

!!! info "No book covers this — source and docs only"
    Pipeline run semantics are documented from the user's side (how to ask for a refresh) but not
    from the engine's (what the run then does). The sweep page is the reference.

!!! warning "Do not trust the run outcome as a data-movement signal"
    Because `SKIPPED` and `EXCLUDED` are both terminal *non-failure* states, a run that computed
    nothing at all can report `COMPLETED`. Read the per-flow `FlowProgress` events. This is the
    single most important operational fact in the topic.

**Milestone:** You can name all eight flow states and say which four make a run report success. Given a pipeline run that reported `COMPLETED` you can prove from the event stream whether any data actually moved. You can predict the wall-clock gap between retry attempts from the two watchdog configs, and explain why raising `maxConcurrentFlows` on a deep, narrow graph changes nothing.

---


### ⬜ E31 — Pipeline Checkpoints and Full Refresh: Numbered Generations, Truncate and Drop

> Discovered from source sweep (new topic): `sql/pipelines: Checkpoint layout, generations, and what full refresh actually resets`

**What it is:** Where a declarative pipeline puts its streaming checkpoints (`<storage>/_checkpoints/<catalog>/<schema>/<table>/<flow>/<N>`), why a full refresh creates generation N+1 rather than deleting N, and the different reset treatment given to streaming tables, materialized views and the AutoCDC auxiliary table.

**Why you need it:** Full refresh is the operation people reach for when a pipeline is wrong, and it does four different things to four different kinds of state — knowing which are reversible and which are not is the difference between a recoverable mistake and a lost table.

**Learn it with:**

1. **Source sweep — [sql/pipelines — graph](reference/spark-source-map/sweeps/sql-pipelines-graph.md)** — the *Checkpoint layout* section carries the four-row table this topic is built on, and the *DatasetManager* section explains the two surprises: a **materialized view is TRUNCATEd on every run**, not only on full refresh, and a full-refresh request against a non-resettable table is silently downgraded to an ordinary refresh when it came from an "all tables" request but **throws** `TABLE_NOT_RESETTABLE` when it came from an explicit selection
2. **Spark-docs → Declarative Pipelines Programming Guide, Refresh Selection Behavior and `spark-pipelines run`** ([declarative-pipelines-programming-guide.html](https://spark.apache.org/docs/latest/declarative-pipelines-programming-guide.html#refresh-selection-behavior)) — the `--full-refresh` / `--full-refresh-all` flags, and the sink caveat: full refresh resets a sink's checkpoint but cannot clean what was already written downstream
3. **A36 — The Streaming Checkpoint Protocol** — the contents of a checkpoint directory (offset log, commit log, state store). This topic is the layer above: which directory a pipeline flow uses and when a new one is minted. Read A36 first if the inside of a checkpoint is still opaque
4. **Local stack** — build a streaming table plus a materialized view over it, run twice, and list `<storage>/_checkpoints/` to see the `.../<table>/<flow>/0` layout. Then full-refresh and confirm generation `1` appears **beside** `0` rather than replacing it. Set `pipelines.reset.allowed=false` as a table property and try both a targeted and an all-tables full refresh; the two behave differently

!!! info "No book covers this — source and docs only"
    The docs describe the CLI flags; the reset semantics behind them are only in the source.

!!! warning "Two of the four resets are irreversible, and the CLI does not say so"
    Data (`TRUNCATE`), schema (replaced, not merged) and the AutoCDC auxiliary table (`DROP`) do not
    come back. Only the checkpoint is preserved, as generation N alongside the new N+1 — and nothing
    in Spark ever prunes those, so they accumulate for as long as the pipeline is refreshed.

**Milestone:** You can point at the exact directory holding a given flow's current checkpoint and say what its numeric suffix means. Given a pipeline containing a streaming table, a materialized view and an AutoCDC target, you can predict precisely what a full refresh destroys and what it keeps, and say which of those you could recover by hand. You can explain why `pipelines.reset.allowed=false` protects a table from one form of full-refresh request and not the other.

---


### ⬜ E32 — Out-of-Order CDC: Tombstones, Sequence Watermarks, and Deletes You Cannot See

> Discovered from source sweep (new topic): `sql/pipelines: The tombstone model — auxiliary state and delete high-water marks`

**What it is:** How a CDC engine applies deletes correctly when events arrive out of order and the target table keeps no history — by holding a per-key delete high-water mark in a separate tombstone table, filtering late events against it, and garbage-collecting the tombstone once the key is re-inserted.

**Why you need it:** SCD Type 1 keeps only the current row, so a deleted key leaves no evidence in the target that it was ever deleted — and without separate state a late-arriving update for that key silently resurrects it. This is the hardest correctness problem in CDC, and the shape of the fix generalises well beyond Spark.

**Learn it with:**

1. **Source sweep — [sql/pipelines — autocdc](reference/spark-source-map/sweeps/sql-pipelines-autocdc.md)** — the primary material, and a complete worked implementation in 859 lines. Read *The tombstone model* first for the three rules (drop late events below the high-water mark; advance the mark on a newer delete; **delete the tombstone when an upsert with sequence `>=` revives the key**), then the two merge sections for how each rule becomes a merge clause. The GC rule is the one to dwell on — it is the only thing bounding the state, and it fires only on re-insertion
2. **E8 — Change Data Capture and Slowly Changing Dimensions** — the prerequisite. E8 teaches you to write the `MERGE INTO` yourself against an ordered feed; this topic is what you additionally need when the feed is *not* ordered. Do not start here
3. **A8 — Structured Streaming: Stateful Processing** — the conceptual sibling. A watermark bounds state by *time* and drops what falls behind it; a tombstone table bounds state by *key liveness* and drops what has been superseded. Comparing the two is the fastest way to see why CDC cannot simply use a watermark: lateness here is measured in the source's sequence, which has no relationship to event time
4. **Source** — `sql/pipelines/.../autocdc/Scd1BatchProcessor.scala` (465 lines, and the whole algorithm). Its `private[autocdc]` per-step methods exist so unit tests can pin each transform; read `Scd1BatchProcessorSuite` alongside
5. **Local stack** — build an AutoCDC flow against your Delta + Unity Catalog setup and feed it events deliberately out of order: delete key `k` at sequence 10, then deliver an update for `k` at sequence 5 in a **later** microbatch. Confirm the update is dropped, then inspect `__spark_autocdc_aux_state_<target>` directly to see the tombstone. Finally deliver an insert for `k` at sequence 11 and confirm the tombstone disappears
6. **Source sweep — [sql/catalyst — analysis in the source map](reference/spark-source-map/sweeps/sql-catalyst-analysis.md)** — the ordering hazard restated at the engine level: batch net-changes collapses a version range with a `Window`, so a row inserted and then deleted inside the range vanishes; streaming net-changes **cannot see the range** and instead keeps per-row-identity state across micro-batches in `CdcNetChangesStatefulProcessor`, emitting corrections as it learns them. The same logical range therefore produces *additional* rows over time in streaming — a streaming CDC read and a batch CDC read over the same versions are not row-for-row identical, by construction

!!! info "No book covers this — source only"
    Published CDC material teaches the `MERGE INTO` pattern against an already-ordered feed and
    stops there. The out-of-order case is engine territory, and Spark's implementation is not
    documented: the 4.2.0 Declarative Pipelines programming guide contains **zero mentions of CDC**.
    The sweep page is the reference.

!!! warning "Two production hazards live in this topic, not in A11"
    Tombstones have **no TTL** and are collected only by re-insertion, so a source that deletes rows
    permanently grows the auxiliary table monotonically until a full refresh. And that table is read
    **in full on every microbatch**, on an explicit "small enough to broadcast" assumption which the
    first fact undermines. A delete-heavy AutoCDC flow degrades gradually and for a reason nothing
    in the UI will name.

**Milestone:** You can state, without looking, what the auxiliary table contains and why the target table alone cannot answer the same question. Given a stream of out-of-order inserts, updates and deletes for one key, you can predict the final target row and the final tombstone state, and say which merge clause produced each. You can explain why an upsert and a delete carrying the *same* sequence value leave the row alive, and why that makes a second-granularity timestamp a poor `sequence_by`.

---


### ⬜ E33 — Executor Pod Reconciliation: Watch, Poll, and the Events You Miss

> Discovered from source sweep (new topic): `resource-managers/kubernetes: ExecutorPodsSnapshotsStore — a producer/consumer bus with per-subscriber batching`

**What it is:** How the Spark driver keeps its idea of the executor set in sync with the Kubernetes API server — two independent snapshot sources (a streaming watch and a periodic full poll) feeding one snapshot store, and the three separate timeout-driven reconcilers that recover when an event is missed.

**Why you need it:** Every "my executors vanished" or "Spark thinks it has executors it does not" incident on Kubernetes is this machinery failing or timing out, and the three timeouts that govern it are the ones you will actually need to tune.

**Learn it with:**

1. **Source sweep — [resource-managers/kubernetes — driver & executor](reference/spark-source-map/sweeps/resource-managers-kubernetes-driver-executor.md)** — the primary material. Read *ExecutorPodsSnapshotsStore*, *ExecutorPodsSnapshot*, *Watch versus polling* and *ExecutorPodsLifecycleManager* in that order. The frame that makes it click: Spark never asks Kubernetes for N executors, it maintains a target and reconciles against snapshots — so every failure mode is a reconciliation that did not converge
2. **Spark-docs → Running Spark on Kubernetes** ([running-on-kubernetes.html](https://spark.apache.org/docs/latest/running-on-kubernetes.html)) — the configuration reference for the keys below. It documents what each does; it does not explain how they interact, which is what the sweep is for
3. **Kubernetes docs → Watches and resourceVersion** ([kubernetes.io/docs/reference/using-api/api-concepts](https://kubernetes.io/docs/reference/using-api/api-concepts/#efficient-detection-of-changes)) — why a watch can drop events, what `410 Gone` means, and why `resourceVersion=0` is a cache read that can go backwards. Spark's design is a direct response to all three
4. **E2 — Production Deployment** — the prerequisite. This topic assumes you can already submit and run a job on Kubernetes
5. **Local stack** — run a job on kind or minikube, then `kubectl delete pod` an executor mid-stage and watch the driver log: you should see the lifecycle manager attribute it as `exitCausedByApp = false`. Then set `spark.kubernetes.executor.enableApiWatcher=false` and repeat, timing how much longer detection takes — that difference is the poll interval, and it is the whole argument for having both sources

!!! info "No book covers this — source and docs only"
    Kubernetes-specific scheduler internals postdate every published Spark book. The programming
    guide lists the configs; the reconciliation design is source-only.

!!! warning "Disabling the poller is a correctness trade, not a load optimisation"
    `fullSnapshotTs` is only ever set by the poller, and the missing-pod reconciler is gated on it
    changing. Turning off `spark.kubernetes.executor.enableApiPolling` therefore leaves the driver
    relying entirely on a watch connection that is known to drop events — with no reconciler behind
    it. Learn this one before you tune anything.

**Milestone:** You can name the two snapshot sources, say which one produces `fullSnapshotTs` and why that matters, and describe all three timeout-driven reconcilers (pod requested but never observed; registered executor absent from a full snapshot; excess pending pods past the idle timeout) including which config governs each. Given a driver that has stopped scaling up, you can say why a single un-acknowledged pod blocks its whole resource profile and how long that lasts by default. You can explain why `spark.kubernetes.allocation.maximum` eventually kills a long-running dynamic-allocation job.

---


### ⬜ E34 — Vertical Scaling on Kubernetes: In-Place Pod Resize and PVC Growth

> Discovered from source sweep (new topic): `resource-managers/kubernetes: Vertical scaling — in-place memory resize and PVC growth`

**What it is:** The two Spark 4.2.0 plugins that grow a running executor rather than adding another one — patching the pod's `resize` subresource to raise its memory limit, and patching the executor's PVC to grow local-disk storage, both driven by observed usage.

**Why you need it:** Horizontal scaling cannot fix an executor that OOMs on one skewed partition or fills its shuffle disk; these are Spark's first answers to that, they are opt-in, undocumented, and each has a prerequisite that will silently disable it.

**Learn it with:**

1. **Source sweep — [resource-managers/kubernetes — driver & executor](reference/spark-source-map/sweeps/resource-managers-kubernetes-driver-executor.md)** — the *Vertical scaling* section is the only description of these plugins that exists. Also read *Recovery mode* beside it: it is the third 4.2.0 answer to the same problem, arriving from the opposite direction (keep the executor's size, give it one task instead of many)
2. **Kubernetes docs → Resize CPU and Memory Resources assigned to Containers** ([kubernetes.io/docs/tasks/configure-pod-container/resize-container-resources](https://kubernetes.io/docs/tasks/configure-pod-container/resize-container-resources/)) — KEP-1287, the `resize` subresource the memory plugin patches. Check the feature-gate status for your cluster version before planning around it
3. **Kubernetes docs → Expanding Persistent Volumes Claims** ([kubernetes.io/docs/concepts/storage/persistent-volumes/#expanding-persistent-volumes-claims](https://kubernetes.io/docs/concepts/storage/persistent-volumes/#expanding-persistent-volumes-claims)) — the `allowVolumeExpansion: true` requirement on the StorageClass, without which the PVC plugin patches and nothing happens
4. **A4 — Data Skew and Shuffle Optimisation** — the alternative. Growing the executor that got the skewed partition treats the symptom; AQE skew splitting treats the cause. Know which one you are reaching for
5. **Local stack** — enable `ExecutorPVCResizePlugin` with on-demand PVCs, run a shuffle-heavy job that fills the volume past 50%, and watch the driver log for the resize patch. Then check `kubectl get pvc` for `spec` versus `status.capacity` — the gap is a resize in flight, and it is the condition the plugin uses to avoid double-patching

!!! info "No book covers this — source only"
    Both plugins are new in Spark 4.2.0 and neither is mentioned in the Kubernetes programming guide.
    The config docs and the sweep page are the whole of the documentation.

!!! warning "Three traps before you enable either"
    (i) Both refuse to start unless `spark.kubernetes.allocation.pods.allocator` is `direct` — they
    log a warning and return. (ii) `ExecutorResizePlugin` bypasses its own typed config: leaving
    `spark.kubernetes.executor.resizeInterval` at its documented default of `0` gives a **60-second
    poll**, not "disabled", and explicitly setting `0` throws `IllegalArgumentException`; the only
    way to disable it is to remove it from `spark.plugins`. (iii) Neither plugin ever shrinks
    anything and memory growth has no ceiling, so pair them with a namespace `LimitRange` or
    `ResourceQuota`.

**Milestone:** You can state each plugin's prerequisite (metrics-server for memory; `allowVolumeExpansion` for disk; the `direct` allocator for both) and predict the growth curve from `threshold` and `factor`. You can explain why the PVC plugin needs an executor-side component while the memory one does not, and why a failed PVC expansion is never retried. Given an executor that OOMs on one partition, you can argue for resize, recovery mode, or fixing the skew — and say what each costs.

---


### ⬜ E35 — Spark on Kubernetes: Identity, RBAC, and Credential Propagation

> Discovered from source sweep (new topic): `resource-managers/kubernetes: SparkKubernetesClientFactory — one prefix, five suffixes, three identities`

**What it is:** Which identity Spark uses to talk to the Kubernetes API server at each of its three stages — submission, cluster-mode driver, client-mode driver — how credentials reach the driver pod when a service account is not enough, and what RBAC each path actually needs.

**Why you need it:** Almost every "works from my laptop, fails in-cluster" failure on Kubernetes is one of these three identities lacking a verb, and the config family that controls it is invisible to every config listing Spark can generate.

**Learn it with:**

1. **Source sweep — [resource-managers/kubernetes — auth & networking](reference/spark-source-map/sweeps/resource-managers-kubernetes-auth-networking.md)** — the primary material. Start with *SparkKubernetesClientFactory* for the three-identity table, then *The authenticate.\* family the config catalog cannot see* for the prefix × suffix matrix — it is **not** a full cross-product, and assuming it is causes most "that config does nothing" reports. Then *DriverKubernetesCredentialsFeatureStep* and *Service accounts and the executor fallback chain* for the two ways credentials actually reach a pod
2. **Spark-docs → Running on Kubernetes, RBAC** ([running-on-kubernetes.html#rbac](https://spark.apache.org/docs/latest/running-on-kubernetes.html#rbac)) — the minimum Role the driver needs, and the worked `kubectl create clusterrolebinding` example. Note it predates the 4.2.0 NetworkPolicy step, so its verb list is now incomplete
3. **Kubernetes docs → Using RBAC Authorization** ([kubernetes.io/docs/reference/access-authn-authz/rbac](https://kubernetes.io/docs/reference/access-authn-authz/rbac/)) and **Configure Service Accounts for Pods** ([kubernetes.io/docs/tasks/configure-pod-container/configure-service-account](https://kubernetes.io/docs/tasks/configure-pod-container/configure-service-account/)) — what a service account actually projects into a pod, which is the mechanism the whole cluster-mode path rests on
4. **E5 — Catalogs, Governance, and Data Security** — the sibling. E5 is about who may read which data; this is about who may create which pods. They meet at the Kerberos and Secret plumbing, which the same sweep covers
5. **Local stack** — on kind or minikube, create a service account with **no** Role and submit; read the 403 and note which verb it names. Add `pods` verbs until submission succeeds, then add an executor and watch it need `pods/patch` for the exec-id label. Finally try it on 4.2.0 without `networkpolicies` in the Role: the submission creates the driver pod, fails on the post-pod resource block, and **deletes the pod it just created** — an instructive failure

!!! info "No book covers this — source and docs only"
    Kubernetes RBAC for Spark is documented as one worked example in the programming guide and
    nowhere else. The failure modes are source-only.

!!! warning "Two upgrade traps for 4.2.0"
    `NetworkPolicyFeatureStep` is new, **has no config gating it**, and runs on every submission — so
    a service account without `create networkpolicies` breaks submissions that worked on 4.1. Escape
    hatch: `spark.kubernetes.driver.pod.excludedFeatureSteps=org.apache.spark.deploy.k8s.features.NetworkPolicyFeatureStep`.
    And the executor service-account fallback checks the **deprecated `spec.serviceAccount`** field,
    so a pod template setting only `serviceAccountName` is silently overridden.

!!! warning "A shipped OAuth token is a namespace-readable pod-management credential"
    `spark.kubernetes.authenticate.driver.oauthToken` is base64'd into a Secret — the Kubernetes wire
    format, not encryption. Anyone with `get secrets` in that namespace obtains a credential that can
    create and delete pods there. Prefer a service account with a scoped Role. And do not run a
    Kubernetes driver at DEBUG where logs are shipped: the client config, resolved token included, is
    serialised to JSON and logged, and `spark.redaction.regex` does not reach arbitrary log lines.

**Milestone:** You can name the three client identities, say which config prefix each uses and where that prefix is chosen in the code, and write the minimum Role for a driver on 4.2.0 from memory — including the verb the NetworkPolicy step added. Given a 403 at submission you can say which identity was in play. You can explain why `spark.kubernetes.authenticate.driver.oauthTokenFile` does not exist while `…driver.mounted.oauthTokenFile` does, and why executors normally need no Kubernetes permissions at all.

---


### ⬜ E36 — YARN Container Placement: Locality Preferences and Rack Resolution

> Discovered from source sweep (new topic): `resource-managers/yarn: Container placement — locality preferences, ratios, and rack resolution`

**What it is:** How Spark turns the driver's per-host pending-task counts into YARN container requests — an expected-containers-per-host ratio that already discounts running and pending containers, a three-pass host → rack → any-host match of what YARN actually grants, and the rack resolver underneath both.

**Why you need it:** Node-local task placement on YARN is decided here, one allocation round before the scheduler ever sees an offer; when executors land on the wrong nodes the cause is in this arithmetic or in a rack resolver that silently fell back to `/default-rack`, and neither is visible in the UI.

**Learn it with:**

1. **Spark-docs → Running on YARN, Configuration** ([running-on-yarn.html#configuration](https://spark.apache.org/docs/latest/running-on-yarn.html#configuration)) — the only user-facing levers that touch placement: `spark.yarn.am.nodeLabelExpression`, `spark.yarn.executor.nodeLabelExpression` and `spark.yarn.exclude.nodes`
2. **Spark-docs → Configuration, Scheduling** ([configuration.html#scheduling](https://spark.apache.org/docs/latest/configuration.html#scheduling)) — the `spark.locality.wait*` family, which governs the *task*-side half once the containers exist; container placement decides which hosts you get, locality waits decide what runs on them
3. **Source sweep — [resource-managers/yarn — AM & executor allocation in the source map](reference/spark-source-map/sweeps/resource-managers-yarn-am-executor.md)** — the whole mechanism: `LocalityPreferredContainerPlacementStrategy`'s expected-containers-per-host ratio (including the fractional discount for pending requests), the host → rack → any-host match, the separate thread the rack pass runs on, and `SparkRackResolver`'s fallback

!!! warning "No book, and no documentation, covers this"

    SDG, LS2e and Rioux all stop at "Spark prefers to run tasks where the data is". Container
    placement — the layer *below* that, where Spark decides which hosts to name on a YARN request —
    is described nowhere in the Spark documentation either. The class comment in
    `LocalityPreferredContainerPlacementStrategy` is the specification, and it is the only prose
    that exists.

!!! warning "A broken topology script degrades to a single rack, silently"

    `SparkRackResolver.coreResolve` catches an empty or failed mapping and assigns every host
    `/default-rack`, logging **one INFO line**. From then on every host is "rack-local" to every
    other, rack-local scheduling is meaningless, and the placement strategy's rack list collapses to
    one entry. Nothing in the UI says locality has been lost — the only symptom is a job that
    suddenly reads far more data over the network than it used to.

!!! info "Node labels are per application, not per ResourceProfile"

    `spark.yarn.executor.nodeLabelExpression` is read once at allocator construction and applied to
    every `ContainerRequest`, whatever its ResourceProfile. There is no way to send a GPU stage to a
    labelled partition and leave the default profile on the general pool — the AM's own label
    (`spark.yarn.am.nodeLabelExpression`) is the only separate one, and it is applied at submission
    through a different code path.

**Milestone:** Given a stage with pending tasks on four hosts in a 30/30/20/10 ratio and a request for 18 containers, you can say how many container requests name which hosts and how many carry no preference; explain why an already-running executor on one of those hosts reduces the requests aimed at it; and describe what changes in the driver log and in job runtime when the cluster's topology script starts failing.

---


### ⬜ E37 — Application Attempts on YARN: Retry, Final Status, and the Staging Directory

> Discovered from source sweep (new topic): `resource-managers/yarn: Application attempts, final status, and the staging directory`

**What it is:** What happens when a YARN ApplicationMaster dies — how many attempts you actually get (the minimum of a Spark and a YARN setting), what final status each deploy mode reports by default, which attempt is allowed to delete the staging directory, and how the failure-validity interval stops old failures from counting.

**Why you need it:** An application that reports `SUCCEEDED` after failing, or `FAILED` after a clean user exit, is this logic; so is a staging directory left behind on HDFS, and the surprise that `spark.yarn.maxAppAttempts` cannot raise the cluster's ceiling.

**Learn it with:**

1. **Spark-docs → Running on YARN, Configuration** ([running-on-yarn.html#configuration](https://spark.apache.org/docs/latest/running-on-yarn.html#configuration)) — `spark.yarn.maxAppAttempts`, `spark.yarn.am.attemptFailuresValidityInterval`, `spark.yarn.preserve.staging.files`, `spark.yarn.stagingDir` and `spark.yarn.submit.waitAppCompletion`, with the documented note that the Spark attempt setting is bounded by YARN's
2. **Spark-docs → Running on YARN, Debugging your Application** ([running-on-yarn.html#debugging-your-application](https://spark.apache.org/docs/latest/running-on-yarn.html#debugging-your-application)) — `yarn logs -applicationId`, per-attempt container logs, and why a failed *first* attempt's logs are the ones you actually want
3. **Source sweep — [resource-managers/yarn — AM & executor allocation in the source map](reference/spark-source-map/sweeps/resource-managers-yarn-am-executor.md)** — `YarnRMClient.getMaxRegAttempts`, the shutdown hook's unregister/cleanup ordering, `getDefaultFinalStatus`, the once-only `finish`, and the eight AM exit codes

!!! warning "No book covers this"

    SDG's YARN chapter predates most of these settings and neither LS2e nor Rioux discusses AM
    restart at all. The docs list the configs but not their interaction — in particular not the fact
    that the same minimum drives both the retry budget *and* the staging-directory cleanup decision.

!!! warning "`spark.yarn.maxAppAttempts` can only lower the limit, never raise it"

    The effective count is `min(spark.yarn.maxAppAttempts, yarn.resourcemanager.am.max-attempts)`,
    taken silently. On a cluster capped at the YARN default of 2, setting the Spark key to 10 gives
    you 2. The AM uses the same minimum to decide whether it is the *last* attempt, which is what
    determines whether it may delete the staging directory and unregister — so raising the Spark
    value alone changes nothing at all.

!!! info "The default final status differs by deploy mode, on purpose"

    A cluster-mode AM starts with `FinalApplicationStatus.FAILED` and a client-mode one with
    `UNDEFINED`. The reason is `System.exit` from user code: in cluster mode, exiting without a
    clean shutdown must not be reported as success, so the default is pessimistic. In client mode
    the AM cannot know why the driver went away, so it declines to judge — unless
    `spark.yarn.am.clientModeTreatDisconnectAsFailed` is set, which turns an unclean disconnect into
    `FAILED`.

!!! info "A long-running job wants the failure-validity interval"

    Without `spark.yarn.am.attemptFailuresValidityInterval`, AM failures accumulate for the entire
    lifetime of the application, so a streaming job that loses its AM twice in six months has
    exhausted a two-attempt budget. Setting the interval to an hour makes old failures expire, so
    only a genuine crash loop runs out of attempts.

**Milestone:** You can state how many AM attempts a given application will actually get from the Spark and YARN settings together, explain why a killed application can leave `.sparkStaging/<appId>` behind on HDFS while a failed one does not, predict what final status YARN reports when user code calls `System.exit(0)` in each deploy mode, and read an AM exit code of 11, 13 or 17 without looking it up.

---


### ⬜ E38 — The YARN Web Proxy: Why the Spark UI Redirects and Who It Thinks You Are

> Discovered from source sweep (new topic): `resource-managers/yarn: The YARN web proxy — AmIpFilter, redirects, and the proxy-user identity`

**What it is:** Every Spark UI on YARN sits behind the ResourceManager's web proxy: a servlet filter installed into the driver's Jetty at startup rejects any request whose source IP is not a known proxy address by bouncing it back through /proxy/<appId>/redirect, and takes the user identity from a proxy-user cookie the proxy sets.

**Why you need it:** This is why hitting the driver host directly bounces you, why the UI's links need `spark.ui.proxyBase` to be right, why a stale proxy address list produces redirect loops for up to five minutes, and why the History Server needs its own filter to escape the same trap.

**Learn it with:**

1. **Spark-docs → Security, Web UI** ([security.html#web-ui](https://spark.apache.org/docs/latest/security.html#web-ui)) — the ACL model (`spark.acls.enable`, view/modify ACLs and their group forms) that is the *actual* authorization layer; the proxy only supplies an identity
2. **Spark-docs → Configuration, Spark UI** ([configuration.html#spark-ui](https://spark.apache.org/docs/latest/configuration.html#spark-ui)) — `spark.ui.proxyBase`, `spark.ui.proxyRedirectUri` and `spark.ui.filters`, the three knobs that decide whether links behind a proxy resolve
3. **Spark-docs → Running on YARN, Using the Spark History Server to replace the Spark Web UI** ([running-on-yarn.html#using-the-spark-history-server-to-replace-the-spark-web-ui](https://spark.apache.org/docs/latest/running-on-yarn.html#using-the-spark-history-server-to-replace-the-spark-web-ui)) — `spark.yarn.historyServer.allowTracking`, and the case `YarnProxyRedirectFilter` exists for
4. **Source sweep — [resource-managers/yarn — AM & executor allocation in the source map](reference/spark-source-map/sweeps/resource-managers-yarn-am-executor.md)** — the two installation paths (system properties in cluster mode, an `AddWebUIFilter` RPC in client mode), the source-IP check and the `/proxy/…/redirect` insertion, the five-minute address cache, the `proxy-user` cookie, the RM-HA probe, and the History Server's client-side meta-refresh

!!! warning "No book covers this, and the docs barely do"

    The YARN web proxy appears in the Spark documentation only as a passing mention of
    `spark.ui.proxyBase` in the configuration table. `AmIpFilter` itself is a fork of Hadoop's class
    maintained inside Spark (migrated to `jakarta.servlet`), and nothing in the Spark docs says a
    filter is installed into your driver's Jetty at all.

!!! warning "The filter propagates an identity; it does not authenticate"

    `AmIpFilter` trusts two things: that the request's source IP is one of the resolved proxy
    addresses, and the value of a `proxy-user` cookie. A request from a proxy host with a forged
    cookie is accepted as that user, and a request with *no* cookie passes down the filter chain
    with no principal at all. The real access control is Spark's ACLs, which default to
    `spark.acls.enable=false`, plus YARN's own `ApplicationAccessType` checks on RM-served pages.
    Treat a reachable driver UI port as a reachable driver.

!!! info "Two different things are called `proxy-user`"

    `spark-submit --proxy-user` is Hadoop *impersonation* — running the application as another
    Kerberos identity — and is described under Security → Proxy user. The `proxy-user` **cookie**
    read by `AmIpFilter` and `YarnProxyRedirectFilter` is unrelated: it is how the RM web proxy tells
    the UI which browser user is looking at the page. Neither implies the other.

**Milestone:** You can explain what happens when you open `http://<driver-host>:4040` on a YARN cluster and why, say where the `proxy-user` identity came from and what it is and is not good for, configure the History Server as an application's tracking URL without landing in a redirect loop, and describe what changes when the ResourceManager's proxy hosts are re-resolved five minutes after a failover.

---


### ⬜ E39 — Container Classpath Construction on YARN: Ordering, User-First, and Path Rewriting

> Discovered from source sweep (new topic): `resource-managers/yarn: Classpath construction and the gateway/cluster path rewrite`

**What it is:** The exact order in which Spark assembles CLASSPATH for the AM and every executor container — working directory, localized conf, optionally the user jar first, the Spark libs directory, the distribution classpath, and the localized Hadoop conf last — plus the gateway-path rewrite that makes a submitter-side path valid on a cluster node.

**Why you need it:** Class-conflict debugging on YARN is entirely a question of what came first in this list, and two of the levers (`spark.yarn.user.classpath.first` and `spark.yarn.populateHadoopClasspath`, whose default depends on how the distribution was built) change the answer without appearing anywhere in the plan or the UI.

**Learn it with:**

1. **Spark-docs → Running on YARN, Preparations + Configuration** ([running-on-yarn.html#preparations](https://spark.apache.org/docs/latest/running-on-yarn.html#preparations)) — the `with-hadoop` vs `no-hadoop` distinction that sets `spark.yarn.populateHadoopClasspath`'s default, and the `spark.yarn.config.gatewayPath` / `replacementPath` pair
2. **Spark-docs → Configuration, Runtime Environment** ([configuration.html#runtime-environment](https://spark.apache.org/docs/latest/configuration.html#runtime-environment)) — `spark.{driver,executor}.extraClassPath` and `spark.{driver,executor}.userClassPathFirst`, the cluster-manager-independent half
3. **Spark-docs → Running on YARN, Adding Other JARs** ([running-on-yarn.html#adding-other-jars](https://spark.apache.org/docs/latest/running-on-yarn.html#adding-other-jars)) — why `--jars` is needed in cluster mode and how a `local:` URI differs from an uploaded one
4. **Source sweep — [resource-managers/yarn — AM & executor allocation in the source map](reference/spark-source-map/sweeps/resource-managers-yarn-am-executor.md)** — `Client.populateClasspath` entry by entry in order, the user-first branch, the build-time `config.properties` behind `isHadoopProvided()`, `getClusterPath`'s literal string replace, and the `$VAR` / `%VAR%` / `{{VAR}}` substitution rules

!!! warning "No book covers this, and one of the configs is undocumented"

    `spark.yarn.user.classpath.first` appears **nowhere** in the Spark documentation — not in
    `running-on-yarn.md`, not in `configuration.md`. It exists only in the source, where it is the
    single switch that puts user jars on the container's *system* classpath instead of behind a
    child-first classloader. SDG and LS2e describe `--jars`; neither describes the resulting order.

!!! warning "The same submission produces different classpaths on two builds of the same version"

    `spark.yarn.populateHadoopClasspath` defaults to `isHadoopProvided()`, read from a
    `config.properties` resource baked into the assembly at build time: `false` on a `with-hadoop`
    distribution (Spark uses its own bundled Hadoop jars), `true` on a `no-hadoop` one (YARN's
    `yarn.application.classpath` is prepended). If that resource cannot be read the code logs a
    warning and assumes `false`. This is the usual root cause of "the same job works on cluster A
    and throws `NoSuchMethodError` on cluster B".

!!! info "The localized Hadoop conf is deliberately last"

    `__spark_conf__/__hadoop_conf__` goes at the *end* of the classpath so the cluster's own
    configuration — and any other service configs living in `HADOOP_CONF_DIR` — cannot shadow
    something the application shipped. The corollary is that a config file you add with `--files`
    is found *before* the cluster's copy of the same name.

**Milestone:** You can write out the container classpath in order for a cluster-mode job with `--jars`, an `extraClassPath` and a `local:` Spark jar; predict what `spark.yarn.user.classpath.first=true` moves and what it does not; explain why the same `spark-submit` line resolves a different Hadoop version on a `no-hadoop` build; and use `spark.yarn.config.gatewayPath` to make a submitter-side install path valid inside a container.

---


### ⬜ E40 — The Kafka Executor Consumer Cache: Reuse, Eviction, and the Random-Access Cliff

> Discovered from source sweep (new topic): `connector/kafka-0-10: KafkaDataConsumer — the per-JVM executor consumer cache`

**What it is:** The per-JVM LRU cache of Kafka consumers each executor keeps, keyed by consumer group and topic-partition — how a task acquires and releases one, when a task retry invalidates it, why the cache can grow past its own maximum capacity, and why sequential offset access is cheap while random access is not.

**Why you need it:** Kafka consumers prefetch, so reusing them across batches is most of the connector's throughput; the cache that provides it has an unbounded-growth path, a silent fall back to non-cached consumers, and a fetch loop whose cost depends entirely on whether your offsets are consecutive.

**Learn it with:**

1. **Spark-docs → Configuration, Spark Streaming** ([configuration.html#spark-streaming](https://spark.apache.org/docs/latest/configuration.html#spark-streaming)) — the four `spark.streaming.kafka.consumer.cache.*` keys and `consumer.poll.ms`; note that the docs describe `maxCapacity` as a maximum without the in-use caveat
2. **Spark-docs → Structured Streaming + Kafka, "Consumer Caching"** ([structured-streaming-kafka-integration.html](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html)) — the *other* Kafka consumer cache, which has a timeout and an evictor thread the DStream one lacks. Reading the two side by side is the fastest way to see what each design bought
3. **Kafka docs → Consumer Configs, `fetch.min.bytes` / `max.partition.fetch.bytes`** ([kafka.apache.org/documentation/#consumerconfigs](https://kafka.apache.org/documentation/#consumerconfigs)) — what a single `poll` actually returns, which is what the buffer this cache preserves is holding
4. **Source sweep — [connector/kafka-0-10 — consumer in the source map](reference/spark-source-map/sweeps/connector-kafka-0-10-consumer.md)** — `KafkaDataConsumer.acquire`'s five branches, the `removeEldestEntry` growth bound, `InternalKafkaConsumer.get`'s seek/poll/`require` path, and the `floorMod` placement that decides whether the cache can hit at all
5. **Source sweep — [connector/kafka-0-10-sql — source & sink in the source map](reference/spark-source-map/sweeps/connector-kafka-0-10-sql-source-sink.md)** — **the other implementation of this topic, and the one you are more likely to be running.** The Structured Streaming connector splits the job into *two* caches: an `InternalKafkaConsumerPool` (commons-pool2 `GenericKeyedObjectPool`, keyed by `(groupId, topicPartition)`) and a separate `FetchedDataPool` holding the pre-fetched records, keyed by the same key **plus the next offset** — so a task can be handed the records the last batch already fetched. Both have idle timeouts and evictor threads the DStream cache lacks. What is *the same* is the failure shape: `PoolConfig` sets and asserts `maxTotal = -1`, so the pool is unbounded by construction and `spark.kafka.consumer.cache.capacity` is checked only before borrowing — exceeding it logs a WARN and makes a best-effort `clearOldest()`, which does nothing when every consumer is active. Its own config doc admits this ("Please note it's a soft limit"). Two further details worth carrying: all consumers sharing a cache key must have **identical Kafka params** or borrowing throws, and a cached consumer holding an expired delegation token is detected on borrow and both pools invalidated for that key
6. **Source sweep — [connector/kafka-0-10-token-provider — auth in the source map](reference/spark-source-map/sweeps/connector-kafka-0-10-token-provider-auth.md)** — the one thing that invalidates a cached consumer for a reason unrelated to offsets. On a secured cluster, `KafkaTokenUtil.needTokenUpdate` rebuilds the current token's JAAS string and compares it to the one the cached consumer was built with; a mismatch invalidates **both** the consumer pool and the fetched-data pool for that key. It has exactly one caller — the Structured Streaming connector's `getOrRetrieveConsumer` — so **the DStream connector's cache never checks**, and on a long-running DStream job against a secured cluster a cached consumer keeps an expired token until something else displaces it

!!! warning "No book covers this"

    The Kafka connector's executor-side cache postdates SDG's Kafka chapter and is absent from LS2e
    and Rioux, both of which stop at "Spark reads from Kafka in parallel". This is source-and-docs
    territory, and the docs omit the two behaviours that actually bite.

!!! warning "`maxCapacity` is not a cap, and the cache never shrinks"

    Eviction fires only when the least-recently-used entry is **not in use**. If every entry is in
    use the map grows past `spark.streaming.kafka.consumer.cache.maxCapacity` (default 64), and the
    source comment states the bound plainly: it grows to the executor's task-slot count "after which
    it will never reduce". Each entry is a live `KafkaConsumer` holding fetch buffers and a TCP
    connection. There is no evictor thread and no TTL — the Structured Streaming connector's cache
    has both, which is the design difference worth knowing.

!!! warning "A cache miss is invisible above DEBUG"

    Four of `acquire`'s five branches return a non-cached consumer that is closed on release: a task
    **retry** (any `attemptNumber ≥ 1`) invalidates the cached entry, the cache being disabled, no
    entry existing yet, and an entry already in use by another task. All four log at DEBUG. A job
    whose partitions keep moving between executors therefore builds a fresh `KafkaConsumer` per
    partition per batch — slower with no error, no warning and no metric. The one INFO-level tell is
    "Initial fetch for &lt;group&gt; &lt;topic-partition&gt; &lt;offset&gt;" appearing every batch
    instead of once.

!!! info "Placement is `hash mod executorCount`, so scaling reshuffles almost everything"

    `KafkaRDD.getPreferredLocations` picks `floorMod(topicPartition.hashCode, executors.length)` over
    a sorted executor list — deliberately consistent so the cache can hit, but not consistent hashing
    in the ring sense. Adding or losing a single executor changes the mapping for most partitions at
    once, invalidating most of the cache. That is a real argument for a fixed executor count on a
    DStream Kafka job.

**Milestone:** You can say which of `acquire`'s branches a given situation takes and whether the resulting consumer is cached; explain why an executor consuming 200 partitions with 64 task slots can hold more than 64 open consumers and what bounds it; predict what `spark.streaming.kafka.consumer.cache.enabled=false` costs on a steady-state stream; and read "Initial fetch"/"Buffer miss" log lines as cache diagnostics rather than errors. For the Structured Streaming connector — the one you are more likely to be running — you can additionally say what the *second* cache (`FetchedDataPool`) holds and why it is keyed by next offset, and name the one thing both designs get wrong in the same way.

---


### ⬜ E41 — failOnDataLoss: What the Kafka Source Does When an Offset Is Gone

> Discovered from source sweep (new topic): `connector/kafka-0-10-sql: Data-loss detection — failOnDataLoss and the recovery walk`

**What it is:** The detection and recovery path behind Structured Streaming's most-toggled Kafka option — what counts as data loss (aged-out offsets, deleted partitions, a recreated topic, a partition that does not start at zero), what the executor does when it hits one, and the two custom metrics that count it.

**Why you need it:** Setting `failOnDataLoss=false` is the standard reaction to a query that will not restart, and it converts a loud failure into a silent skip whose only trace is a WARN and a metric almost nobody reads; knowing exactly which offsets get skipped is the difference between an informed decision and losing data on purpose.

**Learn it with:**

1. **Spark-docs → Structured Streaming + Kafka** ([structured-streaming-kafka-integration.html](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html)) — the `failOnDataLoss` option row and the "Offset Fetching" section; note the docs describe the *option*, not what recovery actually does
2. **Kafka docs → Configuration, Topic-level, `retention.ms` / `cleanup.policy`** ([kafka.apache.org/documentation/#topicconfigs](https://kafka.apache.org/documentation/#topicconfigs)) — the broker-side settings that create the condition in the first place; a data-loss incident is usually a retention decision meeting a slow consumer
3. **Source sweep — [connector/kafka-0-10-sql — source & sink in the source map](reference/spark-source-map/sweeps/connector-kafka-0-10-sql-source-sink.md)** — the five detection sites, the executor's three-case recovery walk with its ASCII range diagrams, the invisible-offset path that is *not* loss, and the two custom metrics
4. **Source sweep — [sql/core — streaming execution in the source map](reference/spark-source-map/sweeps/sql-core-streaming-exec.md)** — the checkpoint and offset-log machinery this sits on: what a restart actually replays, which is what makes an aged-out offset unreachable rather than merely old

!!! warning "No book covers this"

    SDG, LS2e and Rioux all describe the Kafka source's happy path. `failOnDataLoss` is the option
    people set in production without a model of what it does, and the model exists only in the
    source.

!!! warning "`failOnDataLoss=false` skips forward silently, and the counter is not obvious"

    With it set, an offset that no longer exists produces one WARN — "Some data may be lost.
    Recovering from the earliest offset: N" — and the reader jumps to the next available offset, or
    abandons the range entirely when the requested and available ranges do not overlap at all. The
    only quantitative trace is the `offsetOutOfRange` custom metric, which counts **offsets skipped,
    not incidents**, alongside a `dataLoss` metric counting incidents. Treat a non-zero
    `offsetOutOfRange` as the number of records you agreed to lose.

!!! info "Five different things count as data loss, and only one is about retention"

    The driver reports four: it cannot find earliest offsets for a partition that appeared (deleted),
    a **new partition that does not start at 0**, partitions that vanished between batches, and an
    end offset below the start offset (a topic deleted and recreated). The executor reports the
    fifth: the offset it was told to read is outside the partition's available range. Only the last
    is ordinary retention. "Partitions are gone" is very often a **shared `kafka.group.id`**, and
    the connector appends that warning to the message when a custom group id is set.

!!! info "Not every skipped offset is loss"

    Transaction markers, and aborted records under `isolation.level=read_committed`, occupy offsets
    that can never be read. `fetchRecord` distinguishes these from real loss by checking whether the
    requested offset is still at or above the partition's earliest available offset — if so the
    offset is valid but invisible, and it rewinds the buffer one record so the next call returns what
    it just saw. That path increments no counter and logs nothing.

**Milestone:** You can name the five conditions that trigger data-loss reporting and say which are caused by retention, by a shared consumer group, and by a recreated topic; predict what a task does with `failOnDataLoss=false` when its whole `[from, until)` range has aged out versus when only the first half has; and find the `offsetOutOfRange` and `dataLoss` values for a query and say what each one counts.

---


### ⬜ E42 — Multi-Cluster Kafka Authentication: Delegation Tokens Across Several Secured Clusters

> Discovered from source sweep (new topic): `connector/kafka-0-10-token-provider: Cluster matching — targetServersRegex and the one-token rule`

**What it is:** How one Spark application authenticates to more than one secured Kafka cluster at once — a per-cluster config block under an identifier you choose, a delegation token minted per cluster at submit time, and a regex match from a connection's bootstrap.servers back to the credentials it should use.

**Why you need it:** The moment a job reads from one Kerberised Kafka and writes to another, the token model stops being invisible: the matching regex defaults to match-everything, two clusters that both match one connection is a hard failure, and the entire config family is absent from every generated Spark configuration table.

**Learn it with:**

1. **Spark-docs → Structured Streaming + Kafka, "Security"** ([structured-streaming-kafka-integration.html](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html)) — the only place the `spark.kafka.clusters.<id>.*` family is written down at all, in prose; it appears in no generated configuration table
2. **Spark-docs → Security, Kerberos** ([security.html#kerberos](https://spark.apache.org/docs/latest/security.html#kerberos)) — `spark.security.credentials.<service>.enabled`, the keytab-vs-ticket-cache decision, and the delegation-token lifecycle this plugs into
3. **Kafka docs → Security, Delegation Tokens** ([kafka.apache.org/documentation/#security_delegation_token](https://kafka.apache.org/documentation/#security_delegation_token)) — the broker side: who may issue, the SCRAM requirement, `delegation.token.max.lifetime.ms`, and why a token is a bearer credential
4. **Source sweep — [connector/kafka-0-10-token-provider — auth in the source map](reference/spark-source-map/sweeps/connector-kafka-0-10-token-provider-auth.md)** — the whole module in 681 lines: the per-cluster config model, `findMatchingTokenClusterConfig`'s regex match and its `require`, the three login paths in precedence order, and the one injection point both connectors share

!!! warning "No book covers this, and the configs are in no generated table"

    SDG, LS2e and Rioux all stop at unauthenticated Kafka. Worse, the entire
    `spark.kafka.clusters.<id>.*` family is read with `getAllWithPrefix` and declared as no
    `ConfigEntry`, so it appears in no configuration table, gets no deprecation handling, and a typo
    in any key is **silently ignored** rather than rejected. The Structured Streaming Kafka guide's
    security section is the only written source; the rest is the module itself.

!!! warning "The matching regex defaults to `.*`, so adding a second cluster breaks the first"

    `spark.kafka.clusters.<id>.target.bootstrap.servers.regex` decides which cluster's token a given
    connection uses, and it defaults to match-everything. With one cluster that is fine. Add a second
    — also defaulted — and every connection matches two tokens, so
    `findMatchingTokenClusterConfig` fails its `require` with "More than one delegation token matches
    the following bootstrap servers". The failure lands at **connection time on an executor**, not at
    submit, and the message names the servers but not the two clusters. With more than one cluster,
    set the regex on *every* cluster, including the one that already worked.

!!! warning "Every token-acquisition failure is a warning and the application still starts"

    `KafkaDelegationTokenProvider.obtainDelegationTokens` wraps each cluster, and then the whole
    loop, in `catch NonFatal` → `logWarning`. An unreachable `auth.bootstrap.servers`, a bad keytab
    or a broker that refuses to mint produces no token and no failure — the job starts and dies later
    with an authentication error from an executor. The warning does name
    `spark.security.credentials.kafka.enabled` and suggest disabling the provider if you are not
    using Kafka, which is the right fix when the warning is expected.

!!! info "Three login paths, in a fixed precedence, and the first one hides the rest"

    JVM-global JAAS (`java.security.auth.login.config`) → keytab (`spark.kerberos.keytab` /
    `.principal`) → Kerberos ticket cache. If a global JAAS configuration exists, **both** the
    acquisition path and the injection path skip everything else and log only at DEBUG — so setting
    that system property for an unrelated reason silently disables delegation-token auth for Kafka.
    Note also that a proxy user cannot obtain a token at all: `checkProxyUser` `require`-fails,
    pointing at KAFKA-6945.

**Milestone:** You can configure one Spark job against two Kerberised Kafka clusters — distinct identifiers, per-cluster `auth.bootstrap.servers`, and a `target.bootstrap.servers.regex` on each that matches only its own brokers — and explain what breaks if you omit the regex on either. You can say which of the three login paths a given submission will take and how to tell from the logs, name the protocol values that cause a token to be requested at all, and explain why the token ends up as a `sasl.jaas.config` using `ScramLoginModule` rather than as a Kerberos ticket.

---


### ⬜ E43 — The DStream Execution Model: What Structured Streaming Replaced

> Discovered from source sweep (new topic): `streaming: StreamingContext — lifecycle, the one-active-context rule, and getOrCreate`

**What it is:** The original Spark streaming engine — a recurring timer that turns each batch interval into a set of RDD jobs, a DStream graph that remembers a bounded window of past RDDs, a driver checkpoint that serialises that graph so a restarted driver can replay unfinished batches, and a StreamingContext whose lifecycle rules differ from everything else in Spark.

**Why you need it:** DStream jobs are still in production and still need maintaining, and every operational surprise in them — a batch queue that grows without bound, a checkpoint that cannot be restored after a code change, a context that refuses to restart — comes from this model rather than from Spark core; it is also the clearest way to see why Structured Streaming's offset log and watermarks exist.

**Learn it with:**

1. **Spark-docs → Spark Streaming Programming Guide** ([streaming-programming-guide.html](https://spark.apache.org/docs/latest/streaming/index.html)) — still published, still accurate, and explicitly marked legacy; read the "Performance Tuning" and "Fault-tolerance Semantics" sections, which are where the model's constraints are stated
2. **SDG Ch 20** — the DStream chapter, written when this *was* the streaming API; useful for the operator vocabulary (`window`, `updateStateByKey`, `foreachRDD`) that the docs assume
3. **Source sweep — [streaming — DStream in the source map](reference/spark-source-map/sweeps/streaming-dstream.md)** — the engine end to end: `JobGenerator`'s recurring timer and four-event loop, `JobScheduler`'s job sets and thread pool, `DStream.getOrCompute` and the retained-RDD map, `DStreamGraph` validation, and the checkpoint's contents
4. **Topic [A7](#a7-structured-streaming-fundamentals)** — read it first or alongside. This topic is most valuable as a *contrast*: nearly every design decision in Structured Streaming is a response to something here

!!! warning "No current book covers this as a live subject"

    SDG (2018) and LS2e (2020) treat DStreams as the streaming API; Rioux does not cover streaming
    at all. Nothing written since treats it as anything but legacy. Read the old chapters for
    vocabulary and this sweep for the mechanism — and do not take their tuning advice as current.

!!! warning "Three defaults that decide whether a DStream job survives"

    **`spark.streaming.concurrentJobs` is 1**, so one batch runs at a time: a batch that overruns the
    interval pushes the next back and scheduling delay grows without bound. Raising it lets batches
    overlap, which breaks the ordering any stateful or output-ordered operation relies on — it is a
    correctness trade, not a tuning knob, despite being the standard advice since 0.7.0. **Restart
    after downtime is uncapped**: `JobGenerator.restart()` re-submits every batch boundary that
    elapsed while the driver was down, so an hour down on a 1-second interval queues 3,600 job sets
    at once. And **a stopped `StreamingContext` cannot be restarted** — the state machine is one-way,
    which is why `getOrCreate(checkpointDir, factory)` is the only supported recovery pattern.

!!! warning "The checkpoint contains your closures, so a deploy usually invalidates it"

    The driver checkpoint is a Java-serialised `DStreamGraph`, and the graph holds the functions you
    passed to `map`, `filter` and `updateStateByKey`. Recompiling changes those classes and the
    restore fails to deserialise. There is no schema, no version negotiation and no migration path:
    the documented answer is to delete the checkpoint and lose position. Structured Streaming's
    offset log stores offsets as **JSON** precisely to avoid this.

!!! info "Windowing is memory, not state"

    A window operation widens `rememberDuration` so the parent DStream keeps its RDDs long enough to
    be re-read. A 1-hour window on a 1-second batch retains 3,600 RDDs per DStream in the chain,
    bounded by nothing but that duration. That is the constraint the watermark plus state store
    exists to remove — and the reason `updateStateByKey`/`mapWithState` are forced to checkpoint.

**Milestone:** You can trace one batch from timer fire to completion — `GenerateJobs` → block allocation → `graph.generateJobs` → `JobSet` → thread pool → `ClearMetadata` — and say which step each of `batchDuration`, `rememberDuration` and the checkpoint interval controls. You can read "Total delay: X s for time T (execution: Y s)" and say whether the job is falling behind and why, predict what a 30-minute driver outage does on restart, and explain to someone proposing `concurrentJobs=4` exactly what they would be giving up.

---


### ⬜ E44 — Receivers and the Write-Ahead Log: Spark's First Answer to Exactly-Once Ingest

> Discovered from source sweep (new topic): `streaming: Receivers — a never-finishing Spark job per stream`

**What it is:** The receiver-based ingest model — each receiver a one-task Spark job that never returns, buffering records into blocks on a timer, pushing them into the BlockManager and optionally a write-ahead log, with a driver-side tracker that decides which batch each block belongs to and survives driver restart through its own log.

**Why you need it:** It is the design every later Spark ingest path reacts against: it costs a permanently occupied core per receiver, it duplicates data unless the WAL is on, and enabling the WAL silently rewrites your chosen storage level — knowing why makes the direct/offset-based model in Structured Streaming and the direct Kafka connector legible rather than arbitrary.

**Learn it with:**

1. **Spark-docs → Spark Streaming, "Fault-tolerance Semantics"** ([streaming-programming-guide.html](https://spark.apache.org/docs/latest/streaming/index.html)) — the receiver reliability table (reliable vs unreliable receivers, and what each guarantees with and without the WAL); this is the clearest statement of the guarantee anywhere
2. **Spark-docs → Spark Streaming, "Deploying Applications"** — the WAL configuration and the explicit warning that enabling it reduces receiver throughput
3. **Source sweep — [streaming — DStream in the source map](reference/spark-source-map/sweeps/streaming-dstream.md)** — the mechanism: `ReceiverTracker`'s scheduling and restart, `BlockGenerator`'s interval and bounded queue, `ReceivedBlockHandler`'s parallel BlockManager+WAL write, `ReceivedBlockTracker`'s three logged events, and `WriteAheadLogBackedBlockRDD` on the read side
4. **Topic [E44 → A12 contrast]** — read the [kafka-0-10 consumer sweep](reference/spark-source-map/sweeps/connector-kafka-0-10-consumer.md) immediately after. The direct Kafka connector is the same problem solved by making the *source* the durable log, and the difference is the whole argument

!!! warning "No current book covers this"

    SDG's streaming chapter predates the WAL's current form and LS2e treats receivers in a
    paragraph. The reliability model exists in the docs and the source only.

!!! warning "Every receiver permanently occupies one core"

    A receiver is submitted as a **one-task Spark job that never returns**, so its slot is gone for
    the life of the application. Two receivers on a two-core executor leaves **no core to process
    batches**, and the job appears to hang with no error and no failed task — the classic "my
    streaming job produces nothing" report. Size for at least `numReceivers + 1` cores. This cost is
    the single strongest argument for the receiverless direct model, and it is why the DStream Kafka
    connector abandoned receivers entirely.

!!! warning "Enabling the WAL rewrites your storage level and writes every record twice"

    `WriteAheadLogBasedBlockHandler` forces the storage level to serialized with **replication 1**,
    logging two warnings, on the reasoning that the log already provides what replication was for.
    Every received record is then written to the `BlockManager` **and** to the log — in parallel on a
    two-thread pool, but both synchronously, with a 30-second timeout — before the receiver
    acknowledges. That is the throughput price of at-least-once receiver ingest.

!!! info "Two independent write-ahead logs, with different defaults"

    The **driver** log records block-tracking events (`BlockAdded`, `BatchAllocated`, `BatchCleanup`)
    and has batching **on** by default; the **receiver** log records the block data itself and has
    batching **off**. They are configured separately (`spark.streaming.driver.writeAheadLog.*` vs
    `…receiver.writeAheadLog.*`), only the receiver one has an `enable` flag, and both are pluggable
    through a `WriteAheadLog` Java interface. Note also that when the WAL is disabled,
    `ReceivedBlockTracker.writeToLog` is a **no-op that returns `true`** — the same code path, minus
    the durability, with nothing in the logs to say so.

**Milestone:** You can say how many cores a job with three receivers needs before it can process anything, and why the symptom of getting it wrong is silence rather than an error. You can state what a reliable receiver plus the WAL guarantees versus an unreliable receiver without it, name what `spark.streaming.blockInterval` actually controls (partitions per batch ≈ `batchInterval / blockInterval` per receiver), and explain why the direct Kafka connector needs neither a receiver nor a WAL.

---


### ⬜ E45 — TRANSFORM … USING: Piping Rows Through an External Process

> Discovered from source sweep (new topic): `sql/core: Script transformation — TRANSFORM … USING`

**What it is:** `SELECT TRANSFORM(a, b) USING 'my_script.py' AS (x, y) FROM t` — the Hive-inherited operator that forks a process per task (`/bin/bash -c`, from the `SparkFiles` root), writes each row to its stdin as delimited text, and parses its stdout back into rows. A writer thread feeds the process while the task thread reads its output, and a `RedirectThread` drains stderr into a 2 KB circular buffer so a chatty script cannot deadlock on a full pipe. Two modes: without Hive support only `ROW FORMAT DELIMITED` is available; with Hive support a SerDe can be used instead.

**Why you need it:** It is the only way to run an arbitrary non-JVM executable inside a Spark plan without writing a UDF — a compiled binary, an R script, a legacy Perl transform. It is also the least safe boundary in Spark SQL, and the reason is not performance: **three of its failure modes are silent**. A field the script emits in a format the target type cannot parse becomes `NULL` (the source comment says so: "when there is a type case error, return null"); a row with too few fields is null-padded to the declared width; a schema-less transform keeps only the first two columns, Hive-style. None of the three raises, warns, or increments a metric. If you use this operator, validating the script's output shape is your job, not Spark's.

**Learn it with:**

1. **No book covers this** — `TRANSFORM` is a Hive-compatibility surface no PySpark book teaches.
2. **Spark-docs → TRANSFORM clause** ([sql-ref-syntax-qry-select-transform.html](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-transform.html)) — the syntax, the `ROW FORMAT DELIMITED` defaults (`\u0001` fields, `\n` lines, `\N` for null), and the difference between the Hive-support-enabled (SerDe) and disabled (no-SerDe) modes
3. **Source sweep — [sql/core — query-execution in the source map](reference/spark-source-map/sweeps/sql-core-query-execution.md)** — `BaseScriptTransformationExec`: `initProc` prepending the `SparkFiles` root to `PATH` and pinning `OMP_NUM_THREADS` to `spark.task.cpus`, the SPARK-7862 stderr-deadlock fix, `wrapperConvertException`'s silent null-on-parse-error, the writer thread that records rather than rethrows (SPARK-25158, because rethrowing would kill the executor), and `spark.sql.scriptTransformation.exitTimeoutInSeconds` gating when the exit code is finally checked

**Milestone:** Write a `TRANSFORM … USING` over a small table with a Python script that emits tab-separated output, and show that without `FIELDS TERMINATED BY '\t'` you get one column, not several. Then make the script emit a non-numeric value in a column declared `INT` and confirm the result is `NULL` with nothing in the driver log. Finally, make the script `exit 1` after emitting some rows, and note how long the query takes to fail and which config controls that wait.

---


### ⬜ E46 — Parquet Page Decoding: Encodings, Dictionaries, and Definition/Repetition Levels

> Discovered from source sweep (new topic): `sql/core: Definition and repetition levels — rebuilding nested values from flat columns`

**What it is:** Below the row-group and pushdown layer, a Parquet column chunk is a sequence of pages, each declaring its own encoding — RLE/bit-packed, plain, dictionary, or one of the delta encodings — plus two integer level streams that record where nulls and list boundaries were, and Spark ships a hand-written vectorized decoder per encoding that writes straight into a column vector before a second pass reassembles nested values from the levels.

**Why you need it:** It explains the performance cliffs no plan or metric shows: a column that stops being fast because its writer's dictionary filled up mid-chunk and Spark had to decode the whole batch eagerly, a rebased or upcast column that is barred from lazy dictionary decoding entirely, and the two extra integer vectors per nesting level that make a deeply nested column cost far more than its data suggests.

**Learn it with:**

1. **parquet-format → Encodings** ([Encodings.md](https://github.com/apache/parquet-format/blob/master/Encodings.md)) — the normative description of `PLAIN`, `RLE`/bit-packed hybrid, `RLE_DICTIONARY`, `DELTA_BINARY_PACKED`, `DELTA_LENGTH_BYTE_ARRAY` and `DELTA_BYTE_ARRAY`, and which are V1 vs V2 defaults. Spark ships one vectorized decoder per entry in this list
2. **parquet-format → Nested Encoding (the Dremel model)** ([README.md](https://github.com/apache/parquet-format/blob/master/README.md)) — definition and repetition levels, the part every "Parquet is columnar" explanation skips and the part that actually costs you on nested data
3. **Spark-docs → Parquet Files** ([sql-data-sources-parquet.html](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html)) — the only knobs Spark exposes over any of this: `spark.sql.parquet.enableVectorizedReader`, `enableNestedColumnVectorizedReader`, `columnarReaderBatchSize` (4096), `columnIndex.filterPushdown.enabled`. Note that the page documents **none** of the encodings, the dictionary behaviour, or the column index it is switching on
4. **Source sweep — [sql/core — datasources in the source map](reference/spark-source-map/sweeps/sql-core-datasources.md)** — the concepts "Definition and repetition levels", "Parquet encodings" and "The vectorized Parquet reader": `ParquetColumnVector.assemble` runs a second pass over the level vectors to rebuild arrays and structs, `ParquetReadState.constructRanges` is where column-index page skipping enters, `VectorizedRleValuesReader` decodes levels and dictionary IDs alike, and `VectorizedColumnReader` holds **one dictionary per batch** (SPARK-16334)
5. **Related topics** — **E22** for what happens to the batch *after* decoding (columnar execution and the `ColumnarRule` API); **I10** for the format-level view above all of this

!!! warning "No book and no Spark doc covers this"

    This is the one layer where the official documentation gives you switches with no explanation of
    what they switch. The parquet-format spec plus the sweep's anchors into Spark's Java decoders are
    the whole reading list.

!!! info "Two encodings, one column, silently"

    Dictionary encoding is a property of a *page*, not of a column. A writer that exhausts its
    dictionary budget mid-chunk falls back to plain for the remainder; Spark then has to decode the
    batch's accumulated dictionary IDs eagerly and drop the vector's dictionary. Nothing in the plan,
    the metrics or the logs says this happened — the column just gets slower after a data-volume
    change.

**Milestone:** Take one Parquet file and dump its metadata with `parquet-tools`/`pyarrow` — list per column chunk the encodings actually used and whether a dictionary page is present. Then read a nested column (an array of structs) and a flat column of the same row count in Spark, and explain from the level model why the nested one allocates more and reads more. Finally, set `spark.sql.parquet.columnarReaderBatchSize` to 128 and to 40960 on the same query and say which of scan time, GC and peak execution memory moved, and why.

---


### ⬜ E47 — Avro State Encoding and State Schema Evolution

> Discovered from source sweep (new topic): `sql/core: Avro state encoding and state schema evolution — schema IDs, a broadcast, and two ceilings`

**What it is:** Setting `spark.sql.streaming.stateStore.encodingFormat=avro` replaces the UnsafeRow byte layout in the state store with Avro-encoded rows prefixed by a two-byte schema id, which is what allows a `transformWithState` value schema to change between restarts: the checkpoint keeps every historical schema, the driver broadcasts them to executors, and each stored row is decoded with the schema it was written under.

**Why you need it:** It is the only mechanism in Spark that lets a stateful query's state schema evolve rather than forcing a checkpoint rebuild, and every part of it is conditional — it works only with Avro encoding, only on `transformWithState`, only for the value side, only for Avro-compatible changes, and only sixteen times per column family before the query fails.

**Learn it with:**

1. **Spark-docs → State Schema Evolution** ([streaming/structured-streaming-transform-with-state.html#state-schema-evolution](https://spark.apache.org/docs/latest/streaming/structured-streaming-transform-with-state.html#state-schema-evolution)) — the only official page on this, and it is short but precise: evolution across state variables (add/remove, with `deleteIfExists` to purge), evolution *within* a variable (add, remove, widen, reorder — **not** rename, **not** narrow), value side only, and the `encodingFormat=avro` requirement. Read it first; it is the contract
2. **Spark-docs → RocksDB State Store / state store configuration** ([streaming/apis-on-dataframes-and-datasets.html#rocksdb-state-store-implementation](https://spark.apache.org/docs/latest/streaming/apis-on-dataframes-and-datasets.html#rocksdb-state-store-implementation)) — for where `spark.sql.streaming.stateStore.encodingFormat` sits among the other state-store settings
3. **No book covers this.** The feature landed in Spark 4.0 (2025) alongside `transformWithState`; no published Spark book discusses state encoding formats at all
4. **Source sweep — [sql/core — streaming execution in the source map](reference/spark-source-map/sweeps/sql-core-streaming-exec.md)** — the substantive reference, and it answers what the docs page does not. Read "Avro state encoding and state schema evolution" for the wire format (a two-byte schema id in front of every row), the `StateSchemaBroadcast` that ships every historical schema to executors, and `StateSchemaCompatibilityChecker.check` — which validates a candidate against *every* prior schema with Avro's own `canReadStrategy`, not just against the newest. Then read "Range-scan key encoding" beside it to see why the *key* side cannot evolve
5. **Prerequisite:** **E27** (what the encoder is writing into) and **A8** / **E26** (the `transformWithState` model whose column families this is per-variable schema tracking for)
6. **Related config:** `spark.sql.streaming.stateStore.encodingFormat` (`unsaferow` by default), `spark.sql.streaming.transformWithState.stateSchemaVersion` (`3`), and two internal ceilings — `…stateStore.valueStateSchemaEvolutionThreshold` (**16**) and `…stateStore.maxNumStateSchemaFiles` (**128**)

!!! warning "Three conditions, and the second is the one people miss"

    Value-schema evolution requires `usingAvro && schemaEvolutionEnabledForOperator`, and that
    second flag is `false` in the shared base trait and overridden to `true` in exactly one place —
    the `transformWithState` family. **No other stateful operator can evolve its state schema,
    whatever the encoding format.** A streaming aggregation, a stream-stream join or a
    `flatMapGroupsWithState` job still faces a checkpoint rebuild. And switching a query to Avro
    encoding forces every state field nullable, so it is not a transparent change either.

!!! info "Two ceilings, both fatal, both internal"

    A column family may evolve its value schema **16** times before
    `stateStoreValueSchemaEvolutionThresholdExceeded` fails the query, and an operator may
    accumulate **128** schema files before adding or removing a state variable fails. Neither is
    documented outside the source, and both are reached by long-lived queries under active
    development rather than by anything unusual.

**Milestone:** Run a `transformWithState` query with `encodingFormat=avro`, stop it, add a field to the case class behind a `ValueState`, and restart it onto the same checkpoint — then show that the old rows still decode. Repeat with `encodingFormat=unsaferow` and record the exact error. Finally, list the schema files under the checkpoint's operator directory, say how many there are and what each represents, and state which single change to your processor would fail the Avro compatibility check.

---


### ⬜ E48 — Continuous Processing and the Epoch Coordinator

> Discovered from source sweep (new topic): `sql/core: Continuous processing — the epoch protocol, and why it is a different engine`

**What it is:** Continuous processing is Spark's other streaming engine: instead of a batch loop it launches tasks that never finish, and it establishes durability with *epochs* — a driver-side `EpochCoordinator` RPC endpoint increments an epoch counter on a timer, collects an end offset from every reader partition and a commit message from every writer partition, and only writes the offset and commit logs once every partition has reported.

**Why you need it:** It is the clearest worked example in Spark of a distributed two-phase commit over long-running tasks, it explains exactly why continuous processing is at-least-once, unshuffleable and retry-intolerant, and it is the model Spark 4.2.0's Real-Time Mode was written to replace — so understanding it is how you read what Real-Time Mode actually changed.

**Learn it with:**

1. **Spark-docs → Continuous Processing** ([streaming/performance-tips.html#continuous-processing](https://spark.apache.org/docs/latest/streaming/performance-tips.html#continuous-processing)) — the whole official account: `Trigger.Continuous(interval)`, ~1 ms latency, **at-least-once** fault tolerance, the checkpoint-interval meaning of that interval, the short list of supported queries (map-like Dataset operations and selected SQL functions only — no aggregations), and the caveats. Note what it does *not* explain: why any of those limits exist
2. **Spark-docs → Triggers** ([streaming/apis-on-dataframes-and-datasets.html#triggers](https://spark.apache.org/docs/latest/streaming/apis-on-dataframes-and-datasets.html#triggers)) — where the continuous trigger sits among the others, and the note that a checkpoint written by either engine can be restarted under the other
3. **No book covers this.** Rioux does not reach Structured Streaming; SDG and LS2e predate the feature or mention it in a sentence. There is no second written source
4. **Source sweep — [sql/core — streaming execution in the source map](reference/spark-source-map/sweeps/sql-core-streaming-exec.md)** — the substantive reference, and the only place the protocol is written down. Read "Continuous processing — the epoch protocol" for the driver side: the epoch update thread, `EpochCoordinator`'s two `(epoch, partition)` maps, the completeness *and* sequencing conditions in `resolveCommitsAtEpoch`, and the commit ordering (`writeSupport.commit` **before** `query.commit`). Then read "The continuous reader and writer tasks" for the executor side: the retry rejection, the reader that outlives its `compute()`, the epoch-marker queue, and the writer's never-exiting loop
5. **Prerequisite:** **A36** — continuous processing writes the *same two logs* as the micro-batch engine, which is why checkpoints are interchangeable; read that first and this becomes "the same protocol with a different unit of progress"
6. **Contrast with:** **A7** for the micro-batch engine, and with Real-Time Mode (traced on the same sweep page) for 4.2.0's replacement, which gates itself by a literal class-name allowlist rather than by capability
7. **Related config:** `spark.sql.streaming.continuous.epochBacklogQueueSize`, `…continuous.executorQueueSize`, `…continuous.executorPollIntervalMs`

!!! info "Why it is at-least-once, in one sentence of source"

    `getStartOffsets` resumes from the latest **commit**-log epoch and ignores offsets that were
    reported but never committed — the comment says so: "for at least once, we can just ignore
    those reports and risk duplicates". Exactly-once would require replaying precisely to those
    offsets. The guarantee is a deliberate choice at one line of recovery code, not a property of
    the epoch mechanism.

!!! warning "A continuous task is never retried"

    `ContinuousDataSourceRDD.compute` throws `ContinuousTaskRetryException` on any attempt number
    other than 0, because the partition's reader holds position state a fresh attempt would not
    have. One task failure therefore fails the query. Combined with a single-source restriction,
    no shuffles, no watermarks and no `CurrentTimestamp`, that is the real reason continuous
    processing never became the default — the latency was never the problem.

**Milestone:** Run a `rate` → `console` query under `Trigger.Continuous("1 second")` and confirm from the checkpoint that the offset and commit logs are written once per *epoch*, not per row. Then break it deliberately in two ways and explain each from the protocol: add a `groupBy` (planning fails — say which check rejects it) and kill one executor (the query dies rather than retrying — say why a retry is impossible). Finally, restart the same checkpoint under `Trigger.ProcessingTime` and confirm it resumes, and state what the two engines had to agree on for that to work.

---


### ⬜ E49 — Task Metrics and the Accumulator Pipeline

> Discovered from source sweep (new topic): `core: task-metrics-and-the-accumulator-pipeline`

**What it is:** the machinery behind every per-task number Spark reports. There is no special metrics channel — bytes read, shuffle fetch wait, GC time and peak execution memory are each a `LongAccumulator` field on `TaskMetrics`, created on the driver, serialized into the task, mutated on the executor, and merged back through **two independent routes**: partial values on every heartbeat, final values in the task result. A user accumulator is not a different mechanism; it lands in the same object's `_externalAccums` buffer and travels the same two routes. Alongside it runs a second, task-independent channel — `ExecutorMetrics`, a flat array of *peak* values sampled by a poller rather than accumulated.

**Why you need it:** this is the model that decides whether you can trust a number. Accumulator correctness is a property of *where you update it*, not of the accumulator: Spark guarantees an update inside an **action** is applied once even if a task restarts, and explicitly does not guarantee that inside a **transformation**, where a stage retry or a speculative attempt can apply it more than once. The same plumbing explains why shuffle-read metrics read zero until `mergeShuffleReadMetrics` is called, why the live UI and the finished UI can disagree, and why the peak-memory number that would have explained your OOM is missing when `spark.executor.processTreeMetrics.enabled` is off — the default — so PySpark worker memory never entered the total at all.

**Learn it with:**

1. **Spark-docs → RDD Programming Guide, "Accumulators"** ([rdd-programming-guide.html#accumulators](https://spark.apache.org/docs/latest/rdd-programming-guide.html#accumulators)) — the API and, more importantly, the one paragraph stating the actual guarantee: exactly-once for updates inside actions, at-least-once inside transformations. Read that sentence as the contract this topic explains the implementation of
2. **Spark-docs → Monitoring and Instrumentation, "Executor Task Metrics"** ([monitoring.html](https://spark.apache.org/docs/latest/monitoring.html)) — the published list of what each task reports, which is the driver-side rendering of `TaskMetrics`; the neighbouring **"Executor Metrics"** section covers `peakMemoryMetrics.*`, the `spark.executor.metrics.pollingInterval` fast path, and the `ProcessTree_*` metrics gated on `/proc` plus `spark.executor.processTreeMetrics.enabled`
3. **Spark-docs → Monitoring, REST API** ([monitoring.html](https://spark.apache.org/docs/latest/monitoring.html)) — `/applications/[app-id]/executors` and `/metrics/executors/prometheus`, where the peak values surface for alerting; this is the practical reason to care that they are peaks and not samples
4. **Source sweep — [core — execution engine in the source map](reference/spark-source-map/sweeps/core-execution-engine.md)** — two concepts on that page carry this end to end: *task metrics and the accumulator pipeline* (the `LongAccumulator` fields, `nameToAccums` and the `internal.metrics.*` wire names, the temp-shuffle-metrics merge, `DirectTaskResult` vs `IndirectTaskResult`, and the heartbeat filter `spark.executor.heartbeat.dropZeroAccumulatorUpdates`) and *executor memory metrics and the procfs process tree* (`compareAndUpdatePeakValues`, and `ProcfsMetricsGetter` disabling itself permanently after a single failed read)

!!! warning "The books cover the accumulator API, not this pipeline"

    LS2e and SDG both teach `AccumulatorV2` as a user-facing counter and stop there; neither traces how the value reaches the driver, and neither mentions that internal task metrics are the same mechanism. Rioux does not cover accumulators at all. The RDD Programming Guide's guarantee sentence plus the sweep page are the primary sources here.

!!! info "Two channels, and they can legitimately disagree"

    The heartbeat channel is what makes numbers move while a task runs; the task-result channel is what makes them final. A task killed mid-flight leaves only what the last heartbeat carried. So `spark.executor.heartbeat.dropZeroAccumulatorUpdates` changes what a *live* UI shows without changing any completed number — a genuinely confusing property if you assume one source of truth.

**Milestone:** Write a custom `AccumulatorV2`, update it once inside a `map` and once inside a `foreach`, force a stage retry (kill an executor mid-job) and explain the two resulting values from the guarantee, not from observation. Then open the same job in the UI, name three numbers that are internal accumulators, fetch `/applications/[app-id]/executors` and state which of the returned memory figures are peaks rather than current values — and say what `spark.executor.processTreeMetrics.enabled=false` excluded from them.

---


### ⬜ E50 — Executor Class Loading, Classpath Precedence, and Session Isolation

> Discovered from source sweep (new topic): `core: executor-class-loading-and-session-isolation`

**What it is:** an executor does not have *a* classpath. It builds a `MutableURLClassLoader` per job-artifact state, optionally wraps it in an `ExecutorClassLoader` that fetches REPL-defined classes over the RPC env, and — under Spark Connect — keeps one such loader per session in a bounded LRU cache, swapping the thread context class loader **per task**. `spark.executor.userClassPathFirst` (still marked *Experimental* in the configuration table) replaces the parent-first loader with a child-first one, inverting delegation for classes *and* resources.

**Why you need it:** this is where every dependency conflict is actually resolved, and where the standard fix backfires. Flipping `userClassPathFirst` to get past a `NoSuchMethodError` also changes `getResourceAsStream`, so your shaded jar starts winning `META-INF/services` lookups and `log4j2.properties` — the job stops crashing and silently reconfigures its own logging. The same code is the executor half of Connect multi-tenancy: one executor process serves many sessions with different jars, isolation *is* the LRU, and `spark.executor.isolatedSessionCache.size` decides whether a session that went idle re-resolves all its artifacts on its next task.

**Learn it with:**

1. **Spark-docs → Submitting Applications, "Advanced Dependency Management"** ([submitting-applications.html](https://spark.apache.org/docs/latest/submitting-applications.html)) — how `--jars`, `--packages` and the Ivy resolution path put anything on the executor classpath in the first place; everything in this topic happens downstream of it
2. **Spark-docs → Configuration** ([configuration.html](https://spark.apache.org/docs/latest/configuration.html)) — `spark.executor.userClassPathFirst` (default `false`, and the table flags it Experimental — the docs say only "same functionality as `spark.driver.userClassPathFirst`, applied to executor instances", which is the whole published description), plus `spark.executor.extraClassPath` and `spark.executor.defaultExtraClassPath`
3. **Spark-docs → Spark Connect Overview, "Use Spark Connect in standalone applications"** ([spark-connect-overview.html](https://spark.apache.org/docs/latest/spark-connect-overview.html)) — `addArtifact`, `registerClassFinder` and `REPLClassDirMonitor`: the client-side API whose server-side consequence is the per-session loader this topic is about
4. **Source sweep — [core — execution engine in the source map](reference/spark-source-map/sweeps/core-execution-engine.md)** — the *executor class loading and session isolation* concept: the Guava LRU of `IsolatedSessionState`, the per-task context-loader swap, `ExecutorClassLoader` extending `ClassLoader(null)` so delegation is hand-rolled, the `spark://` fetch path for REPL classes, the Connect UDF stub prefixes, and `spark.executor.killOnFatalError.depth`

!!! warning "No book covers this"

    Classpath precedence on the executor appears in none of LS2e, SDG or Rioux beyond a mention of `--jars`. Spark's own documentation for the flag is two lines and a cross-reference. The source is the documentation here, which is precisely why the failure mode is folklore.

!!! info "It is per task, not per executor"

    The context class loader is set from the task's artifact state on every task, so two tasks from different Connect sessions running back to back on the same thread see different classpaths. Anything that caches a `Class` object or a `ServiceLoader` result across tasks is holding a reference into a loader that may already have been evicted.

**Milestone:** Ship two jars containing the same class at different versions — one on the cluster classpath, one via `--jars` — and predict which wins before running, for both settings of `spark.executor.userClassPathFirst`. Then demonstrate the resource half: put a `log4j2.properties` in the user jar and show that flipping the flag changes which one is loaded. Finally, explain what `spark.executor.isolatedSessionCache.size` bounds, and what an evicted Connect session pays on its next task.

---


### ⬜ E51 — Unroll Memory: Materialising a Cached Partition Without an OOM

> Discovered from source sweep (new topic): `core: unroll-memory`

**What it is:** Before a partition can be cached in memory it must be materialised from an iterator whose size is unknown, so the MemoryStore reserves a small initial budget and grows it geometrically while periodically re-estimating the partially-built block — reserving as *unroll* memory, a third accounting category alongside execution and storage, and transferring it to storage memory atomically only once the block is complete.

**Why you need it:** every `Not enough space to cache rdd_N_M in memory!` warning is an unroll failure, not a storage-capacity failure, and the two have different fixes. Unroll memory is charged **per task attempt** and appears nowhere in the Storage tab, so N concurrent tasks each unrolling a large partition must fit in storage memory *simultaneously* while the tab shows almost nothing cached. And a failed unroll hands back a `PartiallyUnrolledIterator` that keeps holding its reservation until the caller drains or closes it — so a `take` over a cache miss pins memory for the rest of the task.

**Learn it with:**

1. **Spark-docs → Tuning, Memory Management Overview** ([tuning.html#memory-management-overview](https://spark.apache.org/docs/latest/tuning.html#memory-management-overview)) — the unified execution/storage region that unroll memory is carved out of, and `spark.memory.fraction` / `spark.memory.storageFraction`
2. **Spark-docs → Configuration, Memory Management** ([configuration.html#memory-management](https://spark.apache.org/docs/latest/configuration.html#memory-management)) — where `spark.storage.unrollMemoryThreshold` is listed; the check period and growth factor are **not documented anywhere on the site**
3. **Source sweep — [core — storage & serialization in the source map](reference/spark-source-map/sweeps/core-storage-serializer.md)** — the grow loop and its 1.5× overshoot, the per-task-attempt accounting maps, the atomic unroll → storage hand-off, and both partial results (`PartiallyUnrolledIterator`, `PartiallySerializedBlock`)
4. **[I6 — Caching and Persistence](#i6--caching-and-persistence)** — read that first; this topic is the mechanism under its "a partition that fails to unroll is dropped instead of caching" line

!!! warning "No book covers this"

    SDG, LS2e and Rioux all describe `persist`/`cache` at the API level and the unified memory model at the region level. None names unroll memory. The two configs that control its behaviour are not on the Spark configuration page either, so this is source-and-sweep territory.

!!! info "The warning names the block, not the cause"

    `Not enough space to cache rdd_5_12 in memory! (computed 412.3 MiB so far)` is emitted by `logUnrollFailureMessage`, immediately followed by a second line reporting block memory, unroll memory, and **how many tasks are unrolling right now**. That second line is the diagnostic — a large `scratch space shared across N task(s)` figure means the fix is fewer concurrent tasks or a bigger storage fraction, not a smaller dataset.

!!! warning "Off-heap caching only ever unrolls serialized"

    `offHeapUnrollMemoryMap` is used exclusively by `putIteratorAsBytes`, because off-heap storage always stores serialized values. So `OFF_HEAP` never pays the deserialized-unroll cost — and never gets the deserialized read speed either.

**Milestone:** You can explain why an executor with 8 cores needs storage memory for 8 partitions being unrolled, not 1, read the two-line unroll failure message and say which of `spark.memory.storageFraction`, task concurrency, or partition count to change, and describe what a `PartiallyUnrolledIterator` holds and when it lets go.

---

## Suggested Study Sequence

```
Beginner (B1–B9)              →  9 topics · 30–40 hrs   write correct Spark
    ↓
Intermediate (I1–I12)         → 12 topics · 38–54 hrs   real data, real formats, read a plan
    ↓
Advanced (A1–A12)             → 12 topics · 44–66 hrs   make it fast, make it stream
    ↓
Expert (E1–E9)                →  9 topics · 40–60+ hrs  run it in production

Main line: 42 topics.
Source-derived depth: 88 more — I13–I34, A13–A42, E10–E45. Off the main line, read on demand.
Optional milestones: three Databricks certifications — see the section below
```

**You are currently here:** B1–B9 + I1–I5 done (**14 of 46** hand-authored topics; **133** topics in total, counting the **87** source-derived ones). Next: ⬜ I6 — Caching and Persistence.

> The hand-authored figure was carried as "42" for several months while topics were added; counted from the headings on 2026-08-08 it is 46 (133 total minus the 87 carrying a "Discovered from source sweep" line).

**Carrying 🔄:** B1–B9 and I1–I5 — every topic with a written chapter — completed against Spark 4.1.x, now partly stale under 4.2.0. B1–B4 each carry gaps from a source-trace completeness pass as well; for B2–B4 those are additions, not corrections.

Three topics contain claims that are actually *wrong* and should be cleared first: **B3** (ANSI mode is on by default, so book examples relying on a bad cast returning `null` now raise), **I3** (Arrow UDFs are default, invalidating the performance hierarchy as written), and **B1**, which now has two — the install chapter says Java 17/21 only when 4.2.0 supports Java 25, and the architecture chapter's word-count walkthrough describes a global-sort shuffle that `TakeOrderedAndProjectExec` means Spark never plans. **B2**, **B7** and **B8** are merely missing new surface — safe to read as-is, just incomplete.

**If you only do three things next:** clear I3 (it teaches a now-false performance model), do I6–I7 (caching and the Spark UI — everything in Advanced depends on being able to read a plan), then I8 with both table formats rather than Delta alone.

!!! info "About the source-derived topics (I13–I32, A13–A41, E10–E44)"
    These eighty-four came from reading the Spark source rather than from books, courses, or exam guides — the [source map](reference/spark-source-map/index.md)'s sweeps scan a subsystem and report what is in it, independently of what this path already covers, so anything they surface that no topic named becomes a new topic here. That is the mechanism working, not the path drifting: nearly two thirds of the topics below exist because the code had something to teach that no book covers.

    They sit off the main study line and are each written to the same standard as the rest — real resources, a concrete milestone, and an explicit note where no book covers the subject at all. Read them on demand, when you hit the underlying problem in practice (a `Task not serializable` error, a `groupByKey` OOM, a join that never got reordered), rather than as sequential coursework.

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
    Every code snippet on the Spark Developer Associate exam is Python. On both Data Engineer exams, data-manipulation code is given in SQL where possible and Python otherwise — so B8 and I12 carry more exam weight than their position here suggests.

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
- [Introducing Apache Spark 4.2 (Databricks blog)](https://www.databricks.com/blog/introducing-apache-spark-42) *(fetched 2026-07-21 — source for the Phase 5 fold-in above)*
- [ProjectPro PySpark roadmap](https://www.projectpro.io/learning-paths/pyspark-roadmap), [DataCamp PySpark guide](https://www.datacamp.com/blog/learn-pyspark)
- Taxonomy re-derivation (2026-07-18): [Iceberg multi-engine support matrix](https://iceberg.apache.org/multi-engine-support/) *(fetched — Spark 4.1 is newest supported)*, [Iceberg releases](https://iceberg.apache.org/releases/), [Dataquest — data engineering skills 2026](https://www.dataquest.io/blog/data-engineering-skills/), [InterviewStack — data engineer skills 2026](https://interviewstack.io/blog/data-engineer-skills-companies-want-2026), [Parquet VARIANT announcement](https://parquet.apache.org/blog/2026/02/27/variant-type-in-apache-parquet-for-semi-structured-data/)
- Learning-method evidence: [Dunlosky, *Strengthening the Student Toolbox*](https://www.aft.org/ae/fall2013/dunlosky) — self-explanation and retrieval practice both ≈ g 0.55, rereading rated low utility; drives the "attempt the milestone first" instruction in the header
- IBM Spark courses: [edX](https://www.edx.org/learn/apache-spark/ibm-apache-spark-for-data-engineering-and-machine-learning), [Coursera ML](https://www.coursera.org/learn/machine-learning-big-data-apache-spark)
