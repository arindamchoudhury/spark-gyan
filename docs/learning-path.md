# Learning Path: Apache Spark / PySpark

> **Last updated:** 2026-07-25 (**first sweep** of `sql/catalyst — planner` — a new page, wired into nav. Four concepts, seven topics reconciled. The group is deliberately tiny (three files in `catalyst/planning/` plus three DSv2 relation files) because catalyst contributes the planner *framework* and every strategy and physical operator lives in sql/core. Findings: **physical planning takes the first candidate, not the best** — `plan()` returns a lazy iterator and the caller takes `.next()`, so it is rule-order-driven, not cost-driven (**A1**, **E1**); **`ExtractEquiJoinKeys` rejects a predicate with no references on one side**, so a `LIKE` or an inequality silently turns your join into a nested-loop or cartesian (**A3**, **B7**); **DSv2 capabilities are declared, not inferred** — `supports` is a set-membership test, and `computeStats` only asks connectors implementing `SupportsReportStatistics`, which is what starves the CBO (**B4**, **I10**); and `QueryPlanningTracker.topRulesByTime` answers "which rule is slow on my query" directly (**I7**). Counts unchanged: 42 main-line topics, 23 source-derived, 65 total. Earlier the same day: re-sweep of `sql/catalyst — optimizer`. **Both breadth checks came back clean, which is itself the result** — this group is the best-covered in the map and needs no further sweeping at 4.2.0. Config breadth is genuinely exhaustive: an independent re-derivation appeared to leave 21 keys uncited, but each is covered by a range row using family shorthand, so the discrepancy was in the check rather than the page. Package breadth was 45 of 46 files. One concept added for the single gap — **`RewriteWithExpression`** → A1/E1, the rule behind `With` / `CommonExpressionRef` that stops `nvl2`, `between` and null-safe equality evaluating a shared child twice, and whose two escape hatches put the duplicate evaluation back silently. No topic reconciliation needed: A1 and E1 already carried the sweep. Counts unchanged: 42 main-line topics, 23 source-derived, 65 total. Earlier the same day: re-sweep of `sql/catalyst — analysis`, the first non-core group re-swept. **Status corrected `complete` → `partial`**: the config slice was exhaustive but `analysis/` is 217 files with only 31 cited, the lowest ratio in the map — the first pass covered the resolution core thoroughly and the rest of the package not at all. Four concepts added from the untouched areas and eight topics reconciled. **`UnsupportedOperationChecker`** → **A7**/**A8**: the source of nearly every streaming restriction message, including a correctness check that is *advisory* — its own error names the config that disables it, and disabling it lets a query with a known late-row-dropping hazard run. **Row-level command rewrite** → **A3**/**E4**/**I8**: `MERGE`/`UPDATE`/`DELETE` pick delta-based vs group-based rewrite during *analysis* from what the connector supports, which is why identical SQL costs wildly different amounts per table format. **Time travel** → I8/**I11**/E4 and **table constraints and schema evolution** → **B4**/**B5**. The page now names the eight clusters still uncovered — `resolver/` alone is ~100 files — so the next run can take one rather than another thin pass. Counts unchanged: 42 main-line topics, 23 source-derived, 65 total. Earlier the same day: re-sweep of `core — api-bridge`, **completing all nine core groups**. Three concepts, no new topic. **Python worker log capture** → **E3**/I3 closes the loop with the storage sweep's block log writers: capture is a `PYTHON_WORKER_LOGGING:` marker scan over the worker's *stdout only*, active only when `PYSPARK_SPARK_SESSION_UUID` is set, so an unmarked `print()` is still lost and a traceback on stderr is never captured. **`SerDeUtil`** → **A5**/I3/I4: the `AutoBatchedPickler` adapts batch size from a cold start of 1 to keep pickled batches between 1 MB and 10 MB — the cost the Arrow path replaces, and why `df.rdd.map(...)` is slow on a DataFrame that was fine in SQL. **`StreamingPythonRunner`** → **A7**/**A8**/I3: it hands its worker a Spark Connect URL pointing back at the local JVM, which is why a Python `foreachBatch` body gets a real DataFrame and why its startup can fail before any user code runs. Counts unchanged: 42 main-line topics, 23 source-derived, 65 total. Earlier the same day: **carving resolved** — the two packages orphaned by the config-security re-sweep now have homes and are swept. `internal/io/`, the **Hadoop commit protocol**, went to core execution-engine, which already held `OutputCommitCoordinator`; topic **E17** now has both halves on one page — the authority that decides who may commit, and the protocol that performs it. **B4** gained it too, for the finding that `commitJob` promotes staged files one rename at a time with no rollback, so a driver killed mid-commit leaves a partly-written destination. `internal/plugin/`, the **`SparkPlugin` framework**, went to core monitoring: the driver component configures the executor component through a conf round-trip, metric registration is a phase separate from `init`, and a plugin throwing in `init` takes the driver or executor down with it. Counts unchanged: 42 main-line topics, 23 source-derived, 65 total. Earlier the same day: re-sweep of `core — rpc-resources` at the same Spark 4.2.0, the eighth core group. No new topic — three concepts. The substantive one: the **`BlockTransferService` data plane** had no concept at all, so **B1**, **I6** and **A4** each gained the sweep for it. Each executor runs *two* Netty servers — RPC control messages on one, block bytes on `spark.blockManager.port` on the other, with separate thread configs — and the block-transfer layer retries `spark.shuffle.io.maxRetries` times *below* the driver, so a `FetchFailed` arriving at the DAG scheduler has already spent that budget silently. Also added the **`RpcEnv` file server** (why `addFile` puts the driver in the data path for every executor, with no peer-to-peer step, unlike broadcast) and the **`RpcCallContext` reply contract** (nothing enforces that an endpoint answers, so a missing reply hangs the caller until its `RpcTimeout`). Counts unchanged: 42 main-line topics, 23 source-derived, 65 total. Earlier the same day: re-sweep of `core — config-security` at the same Spark 4.2.0, the seventh core group. No new topic — three concepts, mapping to E2/E3/E5. The operational finding: **group-based ACLs deny silently when the group lookup fails** — the default provider forks `id -Gn` per check with no caching, and returns the empty group set on any error, so the ACL config looks correct while users are refused; added to **E3** as a warning callout. Also added the **delegation-token provider SPI** → **E5** (a per-provider enable key built by `String.format`, so it is absent from the config catalog, and the `hadoopFSsToAccess` rule that makes a second Kerberised filesystem fail at first task rather than at submit), and **config module organisation**. Structural finding recorded in `groups.yaml`: this group's scope token was a bare `internal/`, which also claimed `internal/io/` (the Hadoop commit protocol) and `internal/plugin/` (the `spark.plugins` framework) — neither config nor security, and swept by nobody. Narrowed to `internal/config/`; the two orphans await a carving decision. Counts unchanged: 42 main-line topics, 23 source-derived, 65 total. Earlier the same day: re-sweep of `core — monitoring` at the same Spark 4.2.0, the sixth core group. No new topic — four concepts, all mapping to E3/E2/I7. The one that closes a loop: **`JWSFilter` is the documented fix for the open REST submission endpoint** flagged in the previous sweep, so E2's warning callout now names the remedy (`spark.master.rest.filters=org.apache.spark.ui.JWSFilter` plus a BASE64URL secret) instead of only the problem. Also added: the **`/api/v1` REST surface** — the thing to build alerting on, sharing the UI's port and its by-default-absent authentication, and serving live thread dumps; **KVStore serialization**, where `spark.history.store.serializer` still defaults to the JSON+GZip path Spark's own source calls slow; and **`RDDOperationGraph`** → **I7**, which truncates the DAG view past `spark.ui.dagGraph.retainedRootRDDs`. Separately, repaired 362 mojibake sequences in two sweep pages (`core-monitoring`, `sql-catalyst-analysis`) from a UTF-8/cp1252 double-encoding that was rendering as garbage. Counts unchanged: 42 main-line topics, 23 source-derived, 65 total. Earlier the same day: re-sweep of `core — submit-standalone` at the same Spark 4.2.0, the fifth core group re-swept this way. No new topic — all five concepts mapped onto E2/E3/B1, which already carried the sweep. The finding worth acting on is a security default: the standalone Master's **REST submission endpoint is enabled by default and has no authentication of its own**, with `spark.master.rest.filters` the only hook and no HTTPS path — added to **E2** as a warning callout. Also added: the REST server's five servlets including `readyz`; the 4.0.0 identifier-pattern configs, validated only for whitespace; the cluster-mode driver process (`DriverWrapper` + `WorkerWatcher`, and where standalone-cluster `--packages` actually resolves); the standalone web UIs and their own retention configs; and the two deploy-side plugins, `DriverTimeoutPlugin` and `RedirectConsolePlugin` — whose console-redirect keys are asymmetric (`spark.driver.log.…` vs `spark.executor.logs.…`). Counts unchanged: 42 main-line topics, 23 source-derived, 65 total. Earlier the same day: re-sweep of `core — storage-serializer` at the same Spark 4.2.0, the fourth core group re-swept this way. **No new topic this run** — all five concepts mapped onto topics that already exist, which is the honest outcome for a page that was already 24 concepts deep. The find worth knowing is a 4.2.0 feature nothing in the path covered: **PySpark worker logs are now captured into the `BlockManager` as blocks** ([SPARK-53755]/[SPARK-53975]), so a `print()` inside a UDF is finally retrievable — folded into **E3** as a callout and into **I3** and E3 as a resource. Also added: the `BlockId` taxonomy → **I7**/I6/E1, `DiskBlockObjectWriter`'s commit/revert model → E1/A4, `DiskStore` and encrypted blocks → I6/E1/E2, and memory-mapped buffer disposal → E1/I6. Two pre-existing reconciliation gaps fixed while here: **B1** and **A14** were mapped by the first storage sweep in July but never gained it as a resource. Counts below are unchanged: 42 main-line topics, 23 source-derived, 65 total. Earlier the same day: re-sweep of `core — rdd-layer` at the same Spark 4.2.0. Unlike the two earlier core re-sweeps this one was not driven by the config slice — that was already 22/24 cited — but by **package breadth**: `rdd/` is 43 files and only 8 had ever been cited. One new topic, **I19** sampling, which no topic covered at all: `fraction` is an expectation rather than a row count, `takeSample` runs two-plus jobs with an uncapped re-sample loop, and a sample downstream of a shuffle is `INDETERMINATE`, so a retry aborts rather than silently returning different rows. Six further concepts folded into existing topics, five of which gained the sweep as a resource: Hadoop input RDDs and `input_file_name()` → **B4**; `CoGroupedRDD`'s per-side shuffle decision → **B7**; `RDDOperationScope` → **I7** and **E3**; the `PartitionEvaluator` API → **A1**; plus the composition/zip family and the `DefaultPartitionCoalescer` algorithm → I4/I5/A4. Counts below: 42 main-line topics, 23 source-derived, 65 total. Earlier the same day: re-sweep of `core — shuffle-memory` at the same Spark 4.2.0, again driven by the config-slice breadth check. One new topic — **A20** map output sizes, i.e. the accuracy of the statistics AQE's skew-join split and partition coalescing both run on: sizes are byte-compressed on a log-1.1 scale, and above 2000 partitions every non-huge block reports one shared average, with the skew-accuracy path off by default. **A4** gained a cross-reference callout to it. Three further concepts folded into existing topics (spill-file merging and read-ahead, including the new-in-4.2.0 `spark.unsafe.sorter.spill.merge.factor` → E1/A4; host-local disk reading → A4/I5/E2; shuffle cleanup and the service state DB → E2/E1), and `groups.yaml` gained `MapStatus` in shuffle-memory's scope with `shared_scope: true` on both core groups that now claim `scheduler/`. Counts below: 42 main-line topics, 22 source-derived, 64 total. Earlier the same day: re-sweep of `core — execution-engine` at the same Spark 4.2.0, driven by the config-slice breadth check rather than a release. One new topic — **E17** output commit coordination and speculative write safety, the mechanism the first sweep mentioned in a single clause and never traced — plus four concepts folded into existing topics (unschedulable TaskSets and the abort timer → E12/E2; cluster-manager selection and local mode → B1/E2; `TaskInfo` accumulable retention → E3/E1; streaming-aware scheduler logging → E3/A8, which gained the sweep as a resource). Counts below: 42 main-line topics, 21 source-derived, 63 total. Earlier the same day: source sweep of `sql/catalyst — optimizer`, the third Catalyst phase. Three new topics from concepts no topic covered — **A17** table/column statistics and the CBO, **A18** runtime filtering (DPP and bloom filters), **A19** correlated subqueries and decorrelation — and eleven existing topics reconciled against what the sweep found: A1, A3, B6–B9, I1, I2, I5, I7, E1 each gained the sweep as a resource, plus the official docs page the sweep proved they needed (SQL hints grammar for A3, CTE syntax for B8, the generated runtime-SQL-config table for A1). A1 also gained a callout on the optimiser being a readable, per-rule-debuggable list, and a milestone built on `planChangeLog` and `excludedRules`. Counts in the study-sequence section below updated: 42 main-line topics, 20 source-derived, 62 total. Earlier, 2026-07-21: Phase 5b, complete audit against the **official 4.2.0 release notes** (verbatim, `cache/web/spark-420-relnotes.txt`) — the earlier sweep audited only the *blog*, a highlight subset, so this pass caught six feature-level gaps the release notes exposed and folded each into its topic: string **collation** (char/varchar, CTAS/RTAS, `SHOW COLLATIONS`, [SPARK-54870]/[SPARK-49543]/[SPARK-55528]) → B5; SQL **pipe-operator** aggregation + `|` token ([SPARK-54292]/[SPARK-51518]) → B8; `INSERT INTO … REPLACE` conditional overwrite ([SPARK-56001]/[SPARK-54803]) → B4; **named streaming sources/sinks** ([SPARK-54909]/[SPARK-56719]) → A7; **stream-stream join** update-mode + state format V4 ([SPARK-56384]/[SPARK-55628]) and **state-store reliability** ([SPARK-54121]/[SPARK-54106]/[SPARK-54063]/[SPARK-55058]) → A8. All six were pre-existing holes (collation and pipe syntax are 4.0 features the path never captured), not new 4.2.0 items. The long tail — library bumps, ~40 minor optimizer items, security hardening, K8s/JDBC plumbing, pandas `axis=1` — was deliberately left out: no distinct learnable surface. Earlier same day, Phase 5, full sweep of the "Introducing Apache Spark 4.2" blog — folded every feature the path had missed into its topic: Python Data Sources → B4; Real-Time Mode streaming → A7; pandas 3 + Arrow C Data / PyCapsule interop → I3; vector distance/similarity/normalization/aggregation functions → B7; DSv2 row-level / MERGE-perf / INSERT-schema-evolution / transaction-API work → A3; and `SYSTEM.BUILTIN` / `SYSTEM.SESSION` qualification + `time_bucket` + tuple sketches + `IGNORE`/`RESPECT NULLS` + top-K `max_by`/`min_by` → B8. A follow-up **verbatim** re-fetch of the blog — the earlier WebFetch paraphrase had silently dropped two items — added storage-partitioned-join improvements → A3 and Python-execution profiling/diagnostics → E3. Then backfilled **verified** SPARK-IDs into every new callout from local source (`repos/spark` @ `v4.2.0`), which surfaced two corrections: **pandas 3 is not the supported runtime** — runtime `install_requires` is unbounded `pandas>=2.2.0` but pandas ≥ 3.0.0 warns, and [SPARK-57974] states 4.2.0 does not support it (full support in 4.3.0, [SPARK-55139]); the blog overstated it; and **`IGNORE`/`RESPECT NULLS`** is new for *aggregate* functions (`collect_list`/`collect_set`/`array_agg`), not the analytic functions that already had it. Every other blog feature was already placed in a prior pass). Earlier, 2026-07-18: taxonomy re-derived from the Spark 4.2.0 feature surface, current job requirements, and the exam guides — rather than from what the available books cover. Spine changed from the Databricks certification track to Apache Spark itself, with the certs demoted to optional milestones; added A12 Kafka, `VARIANT` to I1, Iceberg to I8/I11/E5; de-vendored E5 and E7. Earlier the same day: verified releases and all three cert pages against official sources, 4.1.2 → 4.2.0, and folded 4.2.0 features into B7, B8, I3, I7, I10, A3, A11, E1, E3, E8
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
10. **Source sweep — [core — rdd-layer in the source map](reference/spark-source-map/sweeps/core-rdd-layer.md)** — the RDD contract itself (`compute`, `getPartitions`, `getDependencies`) and the `iterator` → `getOrCompute` path every task runs, which is where the architecture stops being a diagram and becomes code
11. **Source sweep — [core — execution engine in the source map](reference/spark-source-map/sweeps/core-execution-engine.md)** — the whole execution model as code — action to job to stages to tasks, the single-threaded event loop that drives it, and where the driver stops deciding and the executor starts running
12. **Source sweep — [core — shuffle & memory in the source map](reference/spark-source-map/sweeps/core-shuffle-memory.md)** — what a shuffle physically is: two files per map task, an index of offsets, and the executor-wide monitor that commits them
13. **Source sweep — [core — submit & standalone in the source map](reference/spark-source-map/sweeps/core-submit-standalone.md)** — what `spark-submit` actually does before any of your code runs — master-URL resolution, the wrapper main class each cluster mode substitutes, and the classloader `runMain` builds
14. **Source sweep — [sql/catalyst — analysis in the source map](reference/spark-source-map/sweeps/sql-catalyst-analysis.md)** — skim for the execution-model picture only: *before* any DAG, stage or task exists, your DataFrame/SQL is compiled — parsed, then **analyzed** (names bound, types resolved) — and this is why nothing runs until an action fires. The compilation detail belongs to A1; read it here just to place the analyze phase ahead of the runtime this topic covers
15. **Source sweep — [core — storage & serialization in the source map](reference/spark-source-map/sweeps/core-storage-serializer.md)** — where a block physically lives once a task produces it: the driver's `BlockId → BlockManagerId` registry, the `BlockId` taxonomy whose names you will read in the UI and the logs, and the executor-side `BlockManager` that answers both local and remote reads
16. **Source sweep — [core — rpc & resources in the source map](reference/spark-source-map/sweeps/core-rpc-resources.md)** — the messaging substrate under every driver-executor exchange, and the detail that changes how you read a hang: each executor runs **two** Netty servers, one for RPC control messages and a separate one on `spark.blockManager.port` carrying block bytes

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
8. **Source sweep — [core — submit & standalone in the source map](reference/spark-source-map/sweeps/core-submit-standalone.md)** — how a config gets its value *before* a session exists: the four-stage precedence pipeline and the option table that `--conf` cannot override

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

## Intermediate

**Goal:** Work confidently with complex data structures, window functions, UDFs, and the Delta Lake table format. Begin reading Spark execution plans. Write pipelines that don't fall over on real data.

**Estimated time to complete this level:** 38–54 hrs

**Reading order:** I1 → I2 → I3 → I4 → I5 → I6 → I7 → **I8 → I9 → I10 → I11** (the storage-and-table-format run) → I12. The level then ends with its checkpoint. I13–I19 sit around that gate as source-derived depth — they are not required to pass it, and are read on demand rather than in sequence.

!!! info "Why the numbering jumps"
    I11 (Iceberg) closes the storage-and-table-format run; I13–I15 are optional-depth topics from a source sweep, numbered last because they sit outside the main line.

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
8. **Source sweep — [core — api-bridge in the source map](reference/spark-source-map/sweeps/core-api-bridge.md)** — the machine *underneath* every UDF: `BasePythonRunner`'s exact wire protocol (command → broadcasts → the eval-type integer → rows), the `PythonWorkerFactory` daemon/reuse/idle-pool/UDS lifecycle, and the failure plumbing that turns a Python crash, hang, or OOM into a Spark error (`faulthandler`, traceback dump, kill timeout, the Linux-only `setrlimit` memory cap)
9. **Source sweep — [core — storage & serialization in the source map](reference/spark-source-map/sweeps/core-storage-serializer.md)** — new in 4.2.0: Python worker logs are captured into the `BlockManager` as `PythonWorkerLogBlockId` blocks ([SPARK-53755]/[SPARK-53975]), which is what finally makes a `print()` or `logging` call inside a UDF retrievable instead of stranded on the executor
10. **Source sweep — [sql/catalyst — expressions in the source map](reference/spark-source-map/sweeps/sql-catalyst-expressions.md)** — the catalyst-side view: `PythonUDF` has no `eval` and no `doGenCode` at all — it is a marker carrying an `evalType`, extracted by a planner rule — while `ScalaUDF` runs in-process and pays per-argument encoder conversion instead. Also the V2 function catalog's *magic method*: a `ScalarFunction` whose `invoke` signature matches code-generates into a direct static call rather than a boxed `produceResult`

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
9. **Source sweep — [core — api-bridge in the source map](reference/spark-source-map/sweeps/core-api-bridge.md)** — the *Python* side of the RDD story: how a `PythonRDD` pipes each partition through an external worker process, and how `PythonBroadcast` / `PythonAccumulatorV2` carry shared variables across the JVM↔Python boundary over an auth'd socket (the concrete mechanism behind PySpark broadcast vars and accumulators)

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
7. **Source sweep — [core — execution engine in the source map](reference/spark-source-map/sweeps/core-execution-engine.md)** — where the UI's numbers come from: the listener events, the accumulator merge, and why skipped stages are grey — `MapOutputTracker` still holds their output
8. **Source sweep — [sql/catalyst — optimizer in the source map](reference/spark-source-map/sweeps/sql-catalyst-optimizer.md)** — how to see *why* a plan looks the way it does, rather than guessing: `spark.sql.planChangeLog.level` plus `.rules` / `.batches` logs the plan diff after a single named rule, and `spark.sql.planChangeValidation` checks after every rule that the plan is still resolved, schema-stable and free of dangling references. Both are off by default and neither is in the UI
9. **Source sweep — [core — storage & serialization in the source map](reference/spark-source-map/sweeps/core-storage-serializer.md)** — the `BlockId` taxonomy behind every name in the Storage tab: `rdd_5_12` is an `RDDBlockId`, and the seven distinct shuffle block kinds are why "shuffle block" in a log line is ambiguous
10. **Source sweep — [core — monitoring in the source map](reference/spark-source-map/sweeps/core-monitoring.md)** — the two renderers behind the tabs you read: `RDDOperationGraph`, which rebuilds the DAG view from the scope strings each `StageInfo` carries and truncates it past `spark.ui.dagGraph.retainedRootRDDs`, and the `/api/v1` REST resources that serve the same numbers as JSON — scrape those rather than the HTML
11. **Source sweep — [sql/catalyst — planner in the source map](reference/spark-source-map/sweeps/sql-catalyst-planner.md)** — `QueryPlanningTracker`: the four phases (`parsing`, `analysis`, `optimization`, `planning`) behind the SQL tab's timings, and `topRulesByTime(k)` for the per-rule breakdown. A query whose *planning* phase dominates is unusual and points at a strategy returning many candidates
12. **Source sweep — [sql/catalyst — expressions in the source map](reference/spark-source-map/sweeps/sql-catalyst-expressions.md)** — what a missing `*` in `EXPLAIN` actually means: whole-stage codegen has three independent off-switches (a `CodegenFallback` expression anywhere in the operator, too many nested output *or input* fields, columnar execution) plus a fourth — the interpreted fallback — that leaves the plan text unchanged. And the 8000-byte HotSpot JIT limit sits far below `spark.sql.codegen.hugeMethodLimit`, so `too long to be JIT compiled` in the executor log is the diagnosis for a query that codegens and still crawls

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
3. **Source** — `sql/catalyst/.../parser/SqlBaseParser.g4` for the grammar; the scripting execution lives under `sql/core/.../scripting/`

!!! info "No book covers this — docs and source only"
    SQL scripting landed in Spark 4.0, after every book in the resources table. Rioux (2022), LS2e (2020) and SDG (2018) have nothing on it. Treat the docs page as primary and verify behaviour against your own 4.2.0 stack rather than waiting for a book to catch up.

**Milestone:** You can write a SQL script that declares a variable, iterates over a cursor with `FOR`, applies a conditional with `IF...ELSIF`, and produces a result — and explain when you would choose SQL scripting over a Python pipeline.

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
5. **Spark-docs → SQL Hints** ([sql-ref-syntax-qry-select-hints.html](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-hints.html)) — the full hint grammar, including that a join hint is *attached to a join*, not to a table — which is why a hint on the wrong relation silently does nothing
6. **Source sweep — [sql/catalyst — optimizer in the source map](reference/spark-source-map/sweeps/sql-catalyst-optimizer.md)** — the logical half of join tuning, which happens before any strategy is chosen: `ReorderJoin` and `EliminateOuterJoin`, `CostBasedJoinReorder`'s dynamic program and the four preconditions that must *all* hold before it runs, star-schema detection, and `EliminateResolvedHint` — the rule that moves your hint onto the `Join` node and, in doing so, freezes the join order around it
7. **Source sweep — [sql/catalyst — analysis in the source map](reference/spark-source-map/sweeps/sql-catalyst-analysis.md)** — `MERGE INTO` / `UPDATE` / `DELETE` are rewritten during **analysis**, not planning, and the strategy is chosen from what the connector supports: `SupportsDelta` gets a row-level delta plan, everything else rewrites whole groups (typically whole files). Identical SQL, very different cost per table format
8. **Source sweep — [sql/catalyst — planner in the source map](reference/spark-source-map/sweeps/sql-catalyst-planner.md)** — `ExtractEquiJoinKeys`, the extractor every join strategy pattern-matches against. A predicate with no references on one side is not a join key, and if nothing survives the test the join falls through to nested-loop or cartesian — which is why a `LIKE` or an inequality silently changes your join strategy

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
4. **Source sweep — [core — api-bridge in the source map](reference/spark-source-map/sweeps/core-api-bridge.md)** — `StreamingPythonRunner`, which hands its Python worker a Spark Connect URL pointing back at the local JVM instead of streaming pickled rows. That is why a Python `foreachBatch` body receives a real DataFrame, and why its startup can fail with a timeout or a protocol error before any of your code runs
5. **Source sweep — [sql/catalyst — analysis in the source map](reference/spark-source-map/sweeps/sql-catalyst-analysis.md)** — `UnsupportedOperationChecker`, the source of nearly every "not supported in streaming" message: the batch/streaming split, the arity rules on `mapGroupsWithState`, and the global-watermark correctness check

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
7. **Source sweep — [sql/catalyst — analysis in the source map](reference/spark-source-map/sweeps/sql-catalyst-analysis.md)** — the stateful-operator rules in `UnsupportedOperationChecker`, including the chained-stateful correctness check that is **advisory**: its message names `spark.sql.streaming.statefulOperatorCorrectnessCheck.enabled`, and disabling it lets a query with a known late-row-dropping hazard run rather than fixing anything

**Milestone:** You can implement a session-windowed aggregate with a watermark, explain what happens to a late event that arrives after the watermark threshold, and describe what is stored in the checkpoint directory.

!!! info "Attributing scheduler logs to a query"

    With several streaming queries on one session, driver-side scheduler messages — task launches, locality decisions, exclusions — name no query, because the query id is a thread-local on the streaming thread and the scheduler runs elsewhere. `spark.scheduler.streaming.idAwareLogging.enabled` (default `false`) makes `TaskSetManager` read the id from the TaskSet properties and prefix it; `…queryIdLength` controls truncation.

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
7. **Source sweep — [core — execution engine in the source map](reference/spark-source-map/sweeps/core-execution-engine.md)** — the task lifecycle on the executor, `TaskContext` completion listeners, the result-size decision, and the kill path that turns an uninterruptible task into a lost slot
8. **Source sweep — [core — shuffle & memory in the source map](reference/spark-source-map/sweeps/core-shuffle-memory.md)** — the memory system end to end — pool sizing, the execution/storage asymmetry, the acquire/spill loop, Tungsten pages, and the leak detection that is suppressed exactly when leaks are likeliest
9. **Source sweep — [core — storage & serialization in the source map](reference/spark-source-map/sweeps/core-storage-serializer.md)** — the block manager's read and write paths, the lock protocol underneath them, and how a block that is reported but unreadable retracts itself from the driver's registry
10. **Source sweep — [core — rpc & resources in the source map](reference/spark-source-map/sweeps/core-rpc-resources.md)** — the messaging substrate every driver↔executor exchange rides: `RpcEnv`/`Dispatcher`/`Inbox` and the shared-vs-dedicated `MessageLoop` threading, local-shortcut vs `Outbox` remote routing, and the `RpcTimeout` fallback chain that explains why a stalled heartbeat surfaces as a `spark.network.timeout` error
11. **Source sweep — [sql/catalyst — optimizer in the source map](reference/spark-source-map/sweeps/sql-catalyst-optimizer.md)** — the serialisation boundary from the *plan* side: `EliminateSerialization` removes the deserialize→serialize round trip between two typed `Dataset` operations, `ObjectSerializerPruning` narrows the encoder, and `ReassignLambdaVariableID` is what makes two structurally identical plans canonicalize equal so exchange/subquery reuse can fire. Note that none of this applies to PySpark — a Python UDF is extracted into its own eval node instead
12. **Source sweep — [sql/catalyst — planner in the source map](reference/spark-source-map/sweeps/sql-catalyst-planner.md)** — the `planLater` placeholder mechanism that lets a strategy plan one operator without knowing how its children will execute, and the cartesian fold over placeholders that makes planning time explode if a custom strategy returns several candidates per operator
13. **Source sweep — [sql/catalyst — expressions in the source map](reference/spark-source-map/sweeps/sql-catalyst-expressions.md)** — the execution engine at expression level: the `UnsafeRow` layout (null bitmap, one 8-byte word per field regardless of type, variable-length tail), the Janino compile path and its 100-entry class cache, the whole-stage `produce`/`consume` protocol and its fallbacks, and `objects.scala` — the deserialize/call/serialize sandwich that is the real cost of every typed `Dataset.map`

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
7. **Source sweep — [core — storage & serialization in the source map](reference/spark-source-map/sweeps/core-storage-serializer.md)** — block replication and its topology requirement, executor loss and proactive re-replication, the disk layout, and decommission migration with fallback storage
8. **Source sweep — [core — submit & standalone in the source map](reference/spark-source-map/sweeps/core-submit-standalone.md)** — the submission path end to end, standalone placement arithmetic, and the graceful worker drain that a rolling restart depends on
9. **Source sweep — [core — config & security in the source map](reference/spark-source-map/sweeps/core-config-security.md)** — the whole cluster-security surface: how `SecurityManager` mints the auth secret differently per cluster manager (generated on YARN/local, mounted file on k8s, *required in conf* otherwise), the `AuthEngine` X25519 handshake and its SASL fallback, IO (shuffle-spill) encryption, and the Kerberos delegation-token renewal loop — plus the config engine itself (fallback keys, `${…}` substitution, deprecated-key handling) that every knob in this topic is built on
10. **Source sweep — [core — rpc & resources in the source map](reference/spark-source-map/sweeps/core-rpc-resources.md)** — the resource model behind executor/task sizing: how `spark.executor.cores` + `spark.task.cpus` + custom `spark.*.resource.{name}.amount` combine into the *limiting-resource* arithmetic that decides how many tasks an executor runs, and how accelerator addresses get discovered (resources file vs discovery script). Stage-level scheduling proper is its own topic — see [A16](#a16-stage-level-scheduling-and-accelerator-aware-resources-gpufpga)

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
5. **Source sweep — [core — execution engine in the source map](reference/spark-source-map/sweeps/core-execution-engine.md)** — the heartbeat protocol and its expiry, the executor metrics poller, and the listener events every monitoring tool consumes
6. **Source sweep — [core — shuffle & memory in the source map](reference/spark-source-map/sweeps/core-shuffle-memory.md)** — which shuffle path a job actually took, and why none of it is visible at default log levels
7. **Source sweep — [core — submit & standalone in the source map](reference/spark-source-map/sweeps/core-submit-standalone.md)** — what standalone actually exposes: four master gauges, five worker gauges, and the states that have no metric at all
8. **Source sweep — [core — config & security in the source map](reference/spark-source-map/sweeps/core-config-security.md)** — the two observability-security surfaces this topic must not get wrong: **secret redaction** (which surfaces `spark.redaction.regex` scrubs — Environment page, event log, YARN logs — vs `spark.redaction.string.regex` for SQL explain only), and **UI/History ACLs** (open by default, wildcard semantics, the separate History-server switch)
9. **Source sweep — [core — rdd-layer in the source map](reference/spark-source-map/sweeps/core-rdd-layer.md)** — `RDDOperationScope`, the presentational layer behind the DAG visualization, and `ContextCleaner`'s weak-reference cleanup loop with its periodic `System.gc()` — both are things you will see in a driver thread dump and neither is documented elsewhere
10. **Source sweep — [core — storage & serialization in the source map](reference/spark-source-map/sweeps/core-storage-serializer.md)** — the 4.2.0 block log writers: `RollingLogWriter` stores log output as `BlockManager` blocks, rolling every 32 MiB, which is the mechanism behind retrievable PySpark worker logs
11. **Source sweep — [core — api-bridge in the source map](reference/spark-source-map/sweeps/core-api-bridge.md)** — the executor half of PySpark worker logging: capture is a `PYTHON_WORKER_LOGGING:` marker scan over the worker's **stdout only**, active only when `PYSPARK_SPARK_SESSION_UUID` is set — so an unmarked `print()` still goes to the executor log, and a traceback on stderr is never captured

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
4. **Source sweep — [core — submit & standalone in the source map](reference/spark-source-map/sweeps/core-submit-standalone.md)** — how `--remote` enters submission as a mutually-exclusive alternative to `--master`, and why the Connect server may only run in cluster mode under YARN

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

!!! warning "No book covers this"

    `OutputCommitCoordinator` (SPARK-4879) appears in none of SDG, LS2e or Rioux, and Spark's own configuration tables omit `spark.hadoop.outputCommitCoordination.enabled` — it is read with a bare `SparkConf.getBoolean` and described in the source itself as undocumented. The sweep page and the Hadoop committer docs are the primary sources.

!!! warning "It protects the commit protocol, not your writes"

    A task that writes through `df.write` is covered. A task that opens a JDBC connection, calls an API, or writes to a path itself never asks the coordinator for permission — speculation duplicates that work and nothing intervenes. This is the concrete meaning of "speculation duplicates side effects, not just computation".

!!! info "Concurrent attempts happen without speculation"

    The source comment at the escape hatch cites SPARK-8029: two attempts of the same task can run simultaneously even with speculation disabled, because a stage retry does not kill the old attempt. Turning speculation off does not make the coordinator redundant.

**Milestone:** You can explain what happens to the second attempt when two attempts of one task both reach the commit point, say why a `TaskCommitDenied` failure does not consume the task's retry budget, name the one call site the coordinator guards and give an example of a write that bypasses it, and describe why `fileoutputcommitter.algorithm.version=2` is faster and when it is unsafe.

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
Source-derived depth: 23 more — I13–I19, A13–A20, E10–E17. Off the main line, read on demand.
Optional milestones: three Databricks certifications — see the section below
```

**You are currently here:** B1–B9 + I1–I5 done (**14 of 42** main-line topics; 65 topics in total, counting the 23 source-derived ones). Next: ⬜ I6 — Caching and Persistence.

**Carrying 🔄:** B1–B9 and I1–I5 — every topic with a written chapter — completed against Spark 4.1.x, now partly stale under 4.2.0. B1–B4 each carry gaps from a source-trace completeness pass as well; those are additions, not corrections.

Three contain claims that are actually *wrong* and should be cleared first: **B3** (ANSI mode is on by default, so book examples relying on a bad cast returning `null` now raise), **I3** (Arrow UDFs are default, invalidating the performance hierarchy as written), and the **B1** install chapter (Java 25 is supported; it says 17/21 only). **B2**, **B7** and **B8** are merely missing new surface — safe to read as-is, just incomplete.

**If you only do three things next:** clear I3 (it teaches a now-false performance model), do I6–I7 (caching and the Spark UI — everything in Advanced depends on being able to read a plan), then I8 with both table formats rather than Delta alone.

!!! info "About the source-derived topics (I13–I19, A13–A20, E10–E17)"
    These twenty came from reading the Spark source rather than from books, courses, or exam guides — the [source map](reference/spark-source-map/index.md)'s sweeps scan a subsystem and report what is in it, independently of what this path already covers, so anything they surface that no topic named becomes a new topic here. That is the mechanism working, not the path drifting: roughly a third of the topics below exist because the code had something to teach that no book covers.

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
