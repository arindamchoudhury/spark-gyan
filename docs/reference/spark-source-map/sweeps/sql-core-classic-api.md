---
subsystem: sql/core
spark_version: "4.2.0"
swept_at: 2026-08-06
group: classic-api
all_groups: [query-execution, joins-exec, adaptive, datasources, agg-window-exchange, python-arrow, streaming-exec, classic-api, sql-scripting]
status: complete
concepts:
  - name: One API, two implementations — what `classic/` actually is
    topics: [B2, E9]
  - name: Column without an engine — ColumnNode and its converter
    topics: []
    propose:
      code: A37
      level: Advanced
      title: "Column Without an Engine: ColumnNode and the api/classic/connect Split"
      what: "Since Spark 4.0 a Column is no longer a wrapper around a Catalyst Expression — it holds a ColumnNode, a small serializable tree defined in sql/api with no dependency on the query engine, which classic mode converts to an Expression at plan-construction time and Connect mode serializes to protobuf instead."
      why: "It is the single design decision that lets one `F.col(...).cast(...)` expression work identically against a local JVM engine and a remote gRPC server, and it explains a class of behaviour differences people attribute to Connect bugs: what a Column can carry is bounded by what ColumnNode can express, and anything needing a real Expression must be converted first."
  - name: SparkSession — construction, the two thread-locals, and session-scoped job tags
    topics: [B2]
  - name: The Builder — getOrCreate vs create, and the Hive branch that turns off artifact isolation
    topics: [B2, E9]
  - name: SharedState — one per SparkContext, and everything the JVM shares whether you want it to or not
    topics: [B2, I6]
  - name: SessionState — one per session, every field lazy, and what cloneSession copies
    topics: [B2]
  - name: BaseSessionStateBuilder — the whole SparkSessionExtensions injection surface
    topics: []
    propose:
      code: E29
      level: Expert
      title: "SparkSessionExtensions: The Sixteen Injection Points"
      what: "SparkSessionExtensions is the supported way to change what Spark does without forking it: sixteen inject* methods covering the parser, five analyzer hook positions, the optimizer and a pre-CBO slot, planner strategies, four AQE hooks, columnar rules, plan normalization, and function and table-function registration — all consumed in one place, BaseSessionStateBuilder."
      why: "Every table format and accelerator you might deploy — Delta, Iceberg, Comet, RAPIDS — attaches here, and the config that loads them is static, so mis-registration fails silently. Knowing the full surface is also what lets you write a targeted rule instead of a fragile workaround, and knowing where each hook runs is what stops the rule firing at the wrong time."
  - name: Dataset — a QueryExecution plus an encoder, and a command that has already run
    topics: [B3, A1]
  - name: withAction and SQLExecution — why every action gets an execution id
    topics: [B3, I7]
  - name: checkpoint vs localCheckpoint — cutting a lineage, two ways
    topics: [I6, A14]
  - name: observe and the ObservationManager — metrics without a second pass
    topics: [I26, E3]
  - name: DataFrameReader — one unresolved node, and the V1/V2 decision deferred to analysis
    topics: [B4, I10]
  - name: DataFrameWriter — the save decision tree, and the three writer APIs
    topics: [B4, E23]
  - name: Catalog — a façade over a three-level namespace that mostly builds SQL commands
    topics: [E5, B8]
  - name: RuntimeConfig — the static-config guard, and why some settings silently do nothing
    topics: [B2]
  - name: UDFRegistration — where a UDF becomes a temp function
    topics: [I3, A5]
  - name: The grouping datasets — untyped and typed, and where they diverge
    topics: [B6, I1]
  - name: SQLContext — a compatibility shell, not a component
    topics: [B2]
  - name: ArtifactManager — a per-session classloader and the isolation flag
    topics: [E19, E9]
  - name: CachedBatchSerializer — the pluggable in-memory cache format
    topics: [I6, E22]
  - name: The default cache format — DefaultCachedBatchSerializer, column types and six compression schemes
    topics: [I6, E1]
  - name: VariableSubstitution — ${} in SQL text, and the four namespaces it binds
    topics: [B8]
  - name: The small surface — implicits, conversions, helpers, and the streaming façades
    topics: [B2, A7]
---

# sql/core — the classic API

> Source sweep of the `classic-api` group: `classic/` (27 files, ~10.7k lines), `internal/` (7),
> `artifact/` (1), `columnar/` (1) and — because the scope token `columnar/` claims it too —
> `execution/columnar/` with its `compression/` sub-package (13) — **49 files**, swept against
> **Spark 4.2.0**.

This group is where the names users actually type live: `SparkSession`, `Dataset`,
`DataFrameReader`, `DataFrameWriter`, `Catalog`, `udf`. Almost none of it contains logic — these
classes build logical plans and hand them to the engine. What makes the package worth reading is
the *shape*: since Spark 4.0 every one of these classes is a **concrete implementation of an
interface defined in `sql/api`**, and Spark Connect ships a second implementation of the same
interfaces. Reading `classic/` is therefore reading one half of a deliberate split.

!!! info "The naming is load-bearing"

    `org.apache.spark.sql.SparkSession` is now an abstract class in `sql/api`.
    `org.apache.spark.sql.classic.SparkSession` is the JVM implementation, and
    `org.apache.spark.sql.connect.SparkSession` is the gRPC one. The same holds for `Dataset`,
    `DataFrameReader`, `DataFrameWriter`, `Catalog`, `RuntimeConfig`, `SQLImplicits` and the
    streaming entry points. A method that exists in `sql/api` but throws in one implementation is
    a *documented* divergence, not an oversight — `ConnectClientUnsupportedErrors` enumerates
    them (see the `sql/connect` sweep).

---

## The split

### One API, two implementations — what `classic/` actually is

**What it is:** the package is a systematic set of `extends sql.<Name>` declarations.
`classic.SparkSession extends sql.SparkSession`, `classic.Dataset[T] extends sql.Dataset[T]`,
`classic.DataFrameWriter[T] extends sql.DataFrameWriter[T]`, and so on. The parent in `sql/api`
holds the signatures, the scaladoc and the shared logic that needs no engine; the subclass here
supplies everything that needs a `SparkContext`, a `SessionState`, or a Catalyst `LogicalPlan`.

Three consequences worth carrying:

- **The user-facing type is the abstract one.** Code written against
  `org.apache.spark.sql.SparkSession` compiles once and runs in either mode; code that names
  `classic.SparkSession` has chosen.
- **`ClassicConversions` exists to bridge back.** Where the API returns the abstract type but
  classic code needs the concrete one, an implicit in `conversions.scala` casts it — which is why
  you see `import org.apache.spark.sql.classic.ClassicConversions._` throughout `sql/core`.
- **Anything in `sql/api` must be engine-free.** That constraint is what produced `ColumnNode`,
  `AgnosticEncoder` and the `ColumnNodeLike` tree — the next concept.

**Anchor files:**

- [classic/SparkSession.scala:92](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/classic/SparkSession.scala#L92) — `extends sql.SparkSession with Logging with ColumnConversions`
- [classic/Dataset.scala:231](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/classic/Dataset.scala#L231) — `extends sql.Dataset[T]`
- [classic/conversions.scala:38](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/classic/conversions.scala#L38) — `ClassicConversions`, the implicit downcasts
- [classic/package.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/classic/package.scala) — the type aliases (`DataFrame = Dataset[Row]`) that keep the old names working

**Maps to topics:** B2, E9

### Column without an engine — ColumnNode and its converter

**What it is:** the change that made the split possible. Before Spark 4.0, `Column` wrapped a
Catalyst `Expression` — a class that lives in the engine and cannot be sent over a wire. Now
`org.apache.spark.sql.Column` holds a **`ColumnNode`**: a small algebraic tree defined in
`sql/api/.../internal/columnNodes.scala` with cases for literals, unresolved attributes, unresolved
functions, casts, aliases, sort orders, window specifications, lambda functions, subqueries,
`UpdateFields`, star and regex references, and inline user-defined-function invocation. It knows
nothing about analysis, optimization or execution.

`ColumnNodeToExpressionConverter` in this package is the classic half: a single big pattern match
turning each `ColumnNode` case into the corresponding Catalyst `Expression`, run inside
`SQLConf.withExistingConf(conf)` and `CurrentOrigin.withOrigin(node.origin)` so the resulting
expression carries the *user's* source position — which is what Spark 4 error messages point at.
Connect's half serializes the same tree to protobuf instead.

Two details that explain real behaviour:

- **`UnresolvedStar` with a plan id** becomes `UnresolvedDataFrameStar` — the mechanism behind
  `df["*"]` resolving to one specific DataFrame in a self-join.
- **Inline UDF invocation is a node type**, so `SparkUserDefinedFunction`, `UserDefinedAggregator`
  and the old `UserDefinedAggregateFunction` are each converted here into `ScalaUDF`,
  `ScalaAggregator`, `ScalaUDAF` or a `TypedAggregateExpression`. A Column carrying a UDF is
  therefore still an engine-free tree until conversion.

**Code path:** user calls `F.col("a").cast("int")` → `Column(ColumnNode)` → (classic)
`ColumnNodeToExpressionConverter.apply` → Catalyst `Expression` → plan; (connect) protobuf

**Anchor files:**

- [classic/columnNodeSupport.scala:43](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/classic/columnNodeSupport.scala#L43) — `ColumnNodeToExpressionConverter`, the whole match; the singleton at :269
- [classic/columnNodeSupport.scala:59](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/classic/columnNodeSupport.scala#L59) — the `UnresolvedStar` / plan-id case
- `api/.../sql/Column.scala:140` — `class Column(val node: ColumnNode)`, in `sql/api` (the
  `sql/api` module is outside this group's scope; cited for orientation)
- [classic/UserDefinedFunctionUtils.scala:24](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/classic/UserDefinedFunctionUtils.scala#L24) and [classic/TypedAggUtils.scala:28](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/classic/TypedAggUtils.scala#L28) — the UDF and typed-aggregate conversion helpers the converter calls

**Maps to topics:** none — proposed as **A37**

---

## Session lifecycle

### SparkSession — construction, the two thread-locals, and session-scoped job tags

**What it is:** the constructor is private and takes an optional existing `SharedState` and an
optional parent `SessionState` — which is the entire difference between `newSession()` (share the
former, not the latter) and `cloneSession()` (share both, then force a copy).

Three things it sets up that are easy to miss:

- **A session UUID**, used for the artifact directory, the Python worker's session id, and the
  session job tag.
- **`SQLConf.setSQLConfGetter`** — a global hook making `SQLConf.get` return the *active* session's
  conf, falling back to a static conf when there is none. This is why code deep in the engine can
  read session config without being handed it, and why a stopped session's conf is filtered out.
- **Job tags as an `InheritableThreadLocal`.** Every job gets `spark-session-$sessionUUID`, and
  user tags are rewritten to `spark-session-$uuid-thread-$threadUuid-$tag` so the same tag name in
  two threads does not collide. That per-thread UUID is what makes `interruptTag` cancel *your*
  work rather than a colleague's.

`withActive` wraps operations so the session is the thread's active one for their duration —
which matters because so much of the engine reads `SparkSession.getActiveSession`.

**Anchor files:**

- [classic/SparkSession.scala:92](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/classic/SparkSession.scala#L92) — the private constructor and its parameter scaladoc
- [classic/SparkSession.scala:128](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/classic/SparkSession.scala#L128) — `setSQLConfGetter`
- [classic/SparkSession.scala:140](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/classic/SparkSession.scala#L140) — `threadUuid` and `managedJobTags`
- [classic/SparkSession.scala:264](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/classic/SparkSession.scala#L264) — `newSession`; `cloneSession` at :286, including the forced copy of `SessionState` **and** `ArtifactManager`

**Configs:** `spark.sql.session.timeZone`, `spark.sql.legacy.sessionInitWithConfigDefaults`

**Maps to topics:** B2

### The Builder — getOrCreate vs create, and the Hive branch that turns off artifact isolation

**What it is:** `Builder.build(forceCreate)` in strict order:

1. If not forcing and the **thread's active session** exists → apply any modifiable settings to it
   and return it.
2. Otherwise, under a global lock, if the **JVM default session** exists → same.
3. Otherwise create a `SparkContext` (or use a supplied one), load and apply extensions, construct
   the session, and set it as both default and active.

`getOrCreate()` is `build(forceCreate = false)`; `create()` is `build(true)`. So `create()` is the
only way to get a genuinely new session with its own `SharedState`… except it is not — it still
constructs with `existingSharedState = None`, which means a new `SharedState` *and* a new catalog
view of the same `SparkContext`.

The detail worth knowing is in `enableHiveSupport`: when Hive classes are present it also sets
`spark.sql.artifact.isolation.enabled = false`, with a source comment saying isolation would break
the existing use case of one session adding JARs and another using them. So **enabling Hive support
silently disables artifact isolation** — a coupling nothing in the docs mentions.

`applyModifiableSettings` is why `.config(...)` on an existing session partially works: static
configs are ignored with a warning, modifiable ones are applied.

**Anchor files:**

- [classic/SparkSession.scala:1009](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/classic/SparkSession.scala#L1009) — `build`, the three-step resolution
- [classic/SparkSession.scala:992](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/classic/SparkSession.scala#L992) — the Hive branch disabling artifact isolation
- [classic/SparkSession.scala:1174](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/classic/SparkSession.scala#L1174) — `sessionStateClassName`, the `in-memory` vs `hive` switch
- [classic/SparkSession.scala:1243](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/classic/SparkSession.scala#L1243) — `applyAndLoadExtensions`

**Configs:** `spark.sql.catalogImplementation` (`in-memory` / `hive`),
`spark.sql.artifact.isolation.enabled`, `spark.sql.legacy.createHiveTableByDefault`

**Maps to topics:** B2, E9

### SharedState — one per SparkContext, and everything the JVM shares whether you want it to or not

**What it is:** constructed once per `SparkContext` and handed to every session created from it.
It owns:

- the **`CacheManager`** — so `df.cache()` in one session is visible to every other session on the
  same context;
- the **`ExternalCatalog`** (wrapped in `ExternalCatalogWithListener`), reflectively instantiated
  from `spark.sql.catalogImplementation`, and the creation of the default database if absent;
- the **global temp view manager**, whose database name is `spark.sql.globalTempDatabase`;
- the **`SQLAppStatusStore`** and the SQL tab — one listener per context, which is why the SQL tab
  shows every session's queries;
- the streaming query status listener;
- the resolved **warehouse path**, computed once with a documented precedence between
  `spark.sql.warehouse.dir` and Hadoop's `hive.metastore.warehouse.dir`, then written *back* into
  both the `SparkConf` and the Hadoop conf so nothing downstream disagrees.

The warehouse resolution is worth reading in full: it clones both configs, resolves, qualifies the
path against the filesystem, and tolerates a qualification failure by falling back to the raw path.

**Anchor files:**

- [internal/SharedState.scala:51](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/internal/SharedState.scala#L51) — the class; warehouse resolution at :59–:90
- [internal/SharedState.scala:98](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/internal/SharedState.scala#L98) — `cacheManager`
- [internal/SharedState.scala:121](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/internal/SharedState.scala#L121) — the status store and SQL tab registration
- [internal/SharedState.scala:149](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/internal/SharedState.scala#L149) — `externalCatalog`, reflective construction and default-database creation

**Configs:** `spark.sql.warehouse.dir`, `spark.sql.globalTempDatabase`,
`spark.sql.catalogImplementation`

**Maps to topics:** B2, I6

### SessionState — one per session, every field lazy, and what cloneSession copies

**What it is:** the per-session container: SQL conf, the `SessionCatalog`, function and
table-function registries, `UDFRegistration`, the analyzer, the optimizer, the planner, the
streaming query manager, the resource loader, the `ArtifactManager`, plus `columnarRules` and
`adaptiveRulesHolder`.

Two structural choices matter. First, almost every field is a **builder function evaluated
lazily** (`catalogBuilder`, `analyzerBuilder`, `optimizerBuilder`, …), so constructing a session is
cheap and a component is built only when first touched — which is also why a broken extension can
fail at first query rather than at session creation. Second, `clone(newSparkSession)` delegates to
a `createClone` function supplied at construction, which is how `cloneSession` gets a genuinely
independent copy of temp views, conf and UDFs while sharing the catalog underneath.

`executePlan` is the single entry point from a `Dataset` into the engine — every plan in this
package goes through it.

**Anchor files:**

- [internal/SessionState.scala:71](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/internal/SessionState.scala#L71) — the class and its parameter scaladoc
- [internal/SessionState.scala:98](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/internal/SessionState.scala#L98) — the lazy fields
- [internal/SessionState.scala:134](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/internal/SessionState.scala#L134) — `clone`; `executePlan` at :140
- [internal/SessionState.scala:147](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/internal/SessionState.scala#L147) — `newHadoopConf`, which copies **every** SQL conf into the Hadoop conf (a detail that surprises people debugging connector options)
- [internal/SessionStateHelper.scala:29](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/internal/SessionStateHelper.scala#L29) — the accessor trait mixed in across `sql/core`

**Maps to topics:** B2

### BaseSessionStateBuilder — the whole SparkSessionExtensions injection surface

**What it is:** one abstract class assembles every component of a `SessionState`, and at each step
it consults `session.extensions`. That makes it the complete, readable list of what a
`SparkSessionExtensions` can change:

| Component | Extension hook |
|---|---|
| function registry | `injectFunction` (`registerFunctions`) |
| table function registry | `injectTableFunction` |
| SQL parser | `injectParser` (receives the built-in `SparkSqlParser` as a delegate) |
| analyzer resolution rules | `injectResolutionRule` |
| analyzer hint resolution | `injectHintResolutionRule` |
| analyzer post-hoc rules | `injectPostHocResolutionRule` |
| analyzer check rules | `injectCheckRule` |
| single-pass resolver rules/checks | the same builders, consumed separately |
| optimizer rules | `injectOptimizerRule`, `injectPreCBORule` |
| plan normalization | `injectPlanNormalizationRule` |
| planner strategies | `injectPlannerStrategy` |
| columnar | `injectColumnar` |
| AQE | `injectQueryPostPlannerStrategyRule`, `injectQueryStagePrepRule`, `injectRuntimeOptimizerRule`, `injectQueryStageOptimizerRule` |

Sixteen `inject*` methods in total. `newBuilder` is the abstract hook a subclass overrides —
`HiveSessionStateBuilder` in `sql/hive` is the other implementation — and `build()` assembles them
all into a `SessionState` whose fields are the builder's lazy vals.

Note the parser hook's shape: `extensions.buildParser(session, new SparkSqlParser())`, so a custom
parser is a *decorator* over the built-in one, not a replacement — the standard pattern is to try
your grammar and delegate on failure.

**Anchor files:**

- [internal/BaseSessionStateBuilder.scala:65](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/internal/BaseSessionStateBuilder.scala#L65) — the class; `newBuilder` at :75
- [internal/BaseSessionStateBuilder.scala:95](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/internal/BaseSessionStateBuilder.scala#L95) — function and table-function registration through extensions
- [internal/BaseSessionStateBuilder.scala:132](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/internal/BaseSessionStateBuilder.scala#L132) — `buildParser` as a decorator
- [internal/BaseSessionStateBuilder.scala:192](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/internal/BaseSessionStateBuilder.scala#L192) — the analyzer, with the four custom-rule seams at :244, :254, :264, :287–:314
- `sql/core/.../SparkSessionExtensions.scala:168–400` — the sixteen `inject*` methods. The file sits
  directly under `org/apache/spark/sql/`, outside this group's four scope directories; it is the
  API surface for what this builder consumes.

**Configs:** `spark.sql.extensions` (**static** — read once during session construction)

**Maps to topics:** none — proposed as **E29**

---

## Dataset

### Dataset — a QueryExecution plus an encoder, and a command that has already run

**What it is:** a `Dataset[T]` is two things — a `QueryExecution` and an `Encoder[T]` — plus a
globally unique id. It is not a container of data and it holds no rows.

The constructor does more than store them, and one line is worth the whole concept:

```scala
@transient private[sql] val logicalPlan: LogicalPlan = {
  if (queryExecution.isLazyAnalysis) queryExecution.logical
  else { val plan = queryExecution.commandExecuted ; ... }
}
```

`commandExecuted` **runs any command in the plan eagerly**. So `spark.sql("INSERT INTO …")`
performs the insert when the `DataFrame` is created — not when you call an action on it. Every
"why did my DDL run without an action?" question resolves here. (The lazy-analysis path exists for
Spark Connect and for lazily-analysed plans.)

The same block registers this Dataset's id in a `DATASET_ID_TAG` on the plan when
`spark.sql.analyzer.failAmbiguousSelfJoin` is on — the tag `DetectAmbiguousSelfJoin` later reads to
raise on `df.join(df, df("a") === df("a"))`.

Three encoders coexist and are worth distinguishing: the public `Encoder[T]` (lazily generated),
`exprEnc` (its `ExpressionEncoder` form, used to build plans), and `resolvedEnc` (resolved and
bound against the analyzed output, used to turn collected rows into objects on the driver).

**Anchor files:**

- [classic/Dataset.scala:231](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/classic/Dataset.scala#L231) — the class
- [classic/Dataset.scala:272](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/classic/Dataset.scala#L272) — `logicalPlan` and `commandExecuted`
- [classic/Dataset.scala:299](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/classic/Dataset.scala#L299) — the three encoders
- [classic/Dataset.scala:74](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/classic/Dataset.scala#L74) — the `Dataset` object; `ofRows` at :111 with its shuffle-cleanup-mode and query-tracker variants

**Configs:** `spark.sql.analyzer.failAmbiguousSelfJoin`, `spark.sql.debug.maxToStringFields`

**Maps to topics:** B3, A1

### withAction and SQLExecution — why every action gets an execution id

**What it is:** the comment in the source is a contributor instruction and also the best summary:
*"if adding or updating any action in `Dataset`, please make sure you wrap it with
`withNewExecutionId`"*. `withAction` does four things per action: opens a new execution id,
attaches the query-execution id to the session, wraps internal errors with the action's name, and
**resets the plan's metrics** before running.

The consequences are the ones you see in the UI: each action is one entry in the SQL tab with its
own metrics, the metrics are per-action rather than cumulative, and an internal failure names the
action (`The "collect" action failed`) rather than surfacing a bare stack trace.

`collectFromPlan` is the other half — `executeCollect()` on the physical plan, then the resolved
encoder's deserializer per row. That deserialization happens on the driver, single-threaded, which
is the cost `toLocalIterator` trades against memory.

**Anchor files:**

- [classic/Dataset.scala:2320](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/classic/Dataset.scala#L2320) — `withAction`
- [classic/Dataset.scala:2334](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/classic/Dataset.scala#L2334) — `collectFromPlan`
- [classic/Dataset.scala:252](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/classic/Dataset.scala#L252) — the contributor note

**Maps to topics:** B3, I7

### checkpoint vs localCheckpoint — cutting a lineage, two ways

**What it is:** one private method implements both, parameterised by `reliableCheckpoint` and
`eager`. It executes the physical plan, **copies every row** (`.map(_.copy())` — the rows are
mutable and reused), then either `rdd.checkpoint()` (reliable, to the checkpoint directory) or
`rdd.persist(level)` followed by `rdd.localCheckpoint()`. With `eager = true` it forces
`doCheckpoint()` immediately; otherwise the work happens at the next action. The result is wrapped
back into a `LogicalRDD` that carries the original Dataset's schema and, where possible, its
partitioning and ordering.

The distinction to keep: reliable checkpointing survives executor loss; local checkpointing does
not, and a `localCheckpoint` whose blocks are lost makes the query unrecoverable rather than
recomputable — because the lineage it replaced is gone.

**Anchor files:**

- [classic/Dataset.scala:566](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/classic/Dataset.scala#L566) — the shared implementation, including the row copy and the `LogicalRDD.fromDataset` wrap

**Maps to topics:** I6, A14

### observe and the ObservationManager — metrics without a second pass

**What it is:** `df.observe(name, aggs…)` inserts a `CollectMetrics` logical node carrying the
Dataset's id. The `Observation` overload additionally registers with the session's
`ObservationManager`, which listens for query completion, walks the executed plan for
`CollectMetrics` nodes, and completes the observation's future with the collected row.

The failure mode is handled explicitly rather than silently: if the plan contains no
`CollectMetrics` after optimization — for instance the branch was pruned — the manager unblocks the
waiter rather than leaving it hanging, and it distinguishes "metrics could not be collected" from
"query failed".

**Anchor files:**

- [classic/Dataset.scala:1159](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/classic/Dataset.scala#L1159) — the two `observe` overloads
- [classic/ObservationManager.scala:31](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/classic/ObservationManager.scala#L31) — the manager; the no-`CollectMetrics` unblock at :58–:67

**Maps to topics:** I26, E3

---

## Reading, writing, cataloguing

### DataFrameReader — one unresolved node, and the V1/V2 decision deferred to analysis

**What it is:** `load()` builds a single `UnresolvedDataSource(source, userSpecifiedSchema,
extraOptions, isStreaming = false, paths)` and wraps it in a `Dataset`. The reader itself makes
**no** decision about V1 versus V2, about the file format, or about schema inference — all of that
happens in the analyzer when the unresolved node is resolved. That is a 4.x change worth knowing:
`spark.read.format(...).load(...)` used to resolve the provider eagerly.

The exception is the `json(Dataset[String])` / `csv(Dataset[String])` family, which must build a
plan from an existing Dataset and therefore does inference inline — including
`checkJsonSchema` and the header handling.

`spark.sql.legacy.pathOptionBehavior` decides whether a path given to `load(path)` is also added as
the `path` option, a compatibility knob for sources that read it.

**Anchor files:**

- [classic/DataFrameReader.scala:58](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/classic/DataFrameReader.scala#L58) — the class; `load` at :92–:109 building `UnresolvedDataSource`
- [classic/DataFrameReader.scala:158](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/classic/DataFrameReader.scala#L158) — the `Dataset[String]` JSON path and its schema checks
- [classic/DataStreamReader.scala:41](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/classic/DataStreamReader.scala#L41) — the streaming twin, same shape with `isStreaming = true`

**Maps to topics:** B4, I10

### DataFrameWriter — the save decision tree, and the three writer APIs

**What it is:** `saveCommand` is the branchiest method in the package, and reading it answers most
"which code path did my write take" questions. It looks up a V2 provider for the format; if one
exists and the target resolves to a V2 table it builds `AppendData` / `OverwriteByExpression` /
`CreateTableAsSelect` as appropriate; otherwise — and in several explicitly-handled fallback
cases — it returns `saveToV1SourceCommand`, an `InsertIntoHadoopFsRelationCommand`-style plan
carrying the partitioning, bucketing and clustering columns.

`insertInto` is a *different* operation from `save`: it resolves by table name, ignores the
DataFrame's column names and matches by **position**, and rejects `partitionBy` outright.

Three writer APIs coexist and it is worth knowing which is which:

- **`DataFrameWriter`** (`df.write`) — the original, `SaveMode`-based, V1-and-V2.
- **`DataFrameWriterV2`** (`df.writeTo`) — the DSv2 API with explicit `create` / `replace` /
  `append` / `overwrite` verbs and no `SaveMode` ambiguity.
- **`MergeIntoWriter`** (`df.mergeInto`) — the DataFrame form of SQL `MERGE INTO`, building
  `whenMatched` / `whenNotMatched` / `whenNotMatchedBySource` clause lists.

**Anchor files:**

- [classic/DataFrameWriter.scala:55](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/classic/DataFrameWriter.scala#L55) — the class
- [classic/DataFrameWriter.scala:130](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/classic/DataFrameWriter.scala#L130) — `saveCommand`, the V2-or-V1 decision tree; `saveToV1SourceCommand` at :265
- [classic/DataFrameWriter.scala:315](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/classic/DataFrameWriter.scala#L315) — `insertInto` and its by-position semantics
- [classic/DataFrameWriterV2.scala:46](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/classic/DataFrameWriterV2.scala#L46) / [classic/MergeIntoWriter.scala:42](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/classic/MergeIntoWriter.scala#L42)
- [classic/DataStreamWriter.scala:61](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/classic/DataStreamWriter.scala#L61) — the streaming writer, which starts a query rather than running a command

**Configs:** `spark.sql.sources.default`, `spark.sql.legacy.createHiveTableByDefault`,
`spark.sql.storeAssignmentPolicy`

**Maps to topics:** B4, E23

### Catalog — a façade over a three-level namespace that mostly builds SQL commands

**What it is:** `classic.Catalog` implements the `catalog.Catalog` API, and its implementation
strategy is consistent: most methods **construct a logical command and run it as a Dataset**.
`listDatabases` builds a `ShowNamespaces`, `listTables` builds a `ShowTables`, `getTable` builds an
`UnresolvedTableOrView`. Only a few operations reach `sessionState.catalog` (the `SessionCatalog`)
directly.

The interesting complexity is name resolution under the three-level namespace: several methods
inspect whether a one- or two-part name is a temp view, or a table in the session catalog, before
deciding whether to qualify it with the current catalog and database. That is where "why does
`spark.catalog.getTable("x")` disagree with `spark.sql("DESCRIBE x")`" lives.

**Anchor files:**

- [classic/Catalog.scala:90](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/classic/Catalog.scala#L90) — the class and its `sessionCatalog` accessor
- [classic/Catalog.scala:146](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/classic/Catalog.scala#L146) — `listDatabasesInternal`, the command-as-Dataset pattern
- [classic/Catalog.scala:260](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/classic/Catalog.scala#L260) — `makeTable`
- [classic/Catalog.scala:430](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/classic/Catalog.scala#L430) — the temp-view-versus-table qualification logic

**Configs:** `spark.sql.defaultCatalog`, `spark.sql.globalTempDatabase`

**Maps to topics:** E5, B8

### RuntimeConfig — the static-config guard, and why some settings silently do nothing

**What it is:** `spark.conf` is a thin wrapper over the session's `SQLConf` with one piece of real
logic: `requireNonStaticConf(key)` on every `set`, which raises rather than accepting a static SQL
config after the session exists.

That guard is why `spark.conf.set("spark.sql.extensions", …)` fails loudly — but note the failure
mode of the *builder* path is different: `applyModifiableSettings` on an existing session **warns
and ignores** rather than raising. Same class of mistake, two different outcomes depending on
whether you used `.config()` or `.conf.set()`.

**Anchor files:**

- [classic/RuntimeConfig.scala:37](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/classic/RuntimeConfig.scala#L37) — the class; `requireNonStaticConf` at :40–:47

**Maps to topics:** B2

### UDFRegistration — where a UDF becomes a temp function

**What it is:** the bridge between a `UserDefinedFunction` object and the name you can use in SQL.
Every `register` ends at `functionRegistry.createOrReplaceTempFunction(name, builder, source)`,
with the `source` string (`"scala_udf"`, `"python_udf"`, `"java_udf"`) recorded so
`SHOW FUNCTIONS` and error messages can say where a function came from.

Two details: `SparkUserDefinedFunction` registration optionally validates the parameter count at
registration time rather than at call time, and `registerJava` / `registerJavaUDAF` reflectively
load a class and infer its return type — the path that produces confusing errors when the class is
not on the executor classpath.

**Anchor files:**

- [classic/UDFRegistration.scala:47](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/classic/UDFRegistration.scala#L47) — the class
- [classic/UDFRegistration.scala:86](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/classic/UDFRegistration.scala#L86) — `register` and the parameter-count validation
- [classic/UDFRegistration.scala:50](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/classic/UDFRegistration.scala#L50) — `registerPython`; `registerJava` at :141

**Configs:** `spark.sql.legacy.allowUntypedScalaUDF`

**Maps to topics:** I3, A5

### The grouping datasets — untyped and typed, and where they diverge

**What it is:** `RelationalGroupedDataset` (from `df.groupBy`) is a *builder*, not a plan node — it
holds the grouping expressions and a `GroupType` (`GroupBy`, `Rollup`, `Cube`, `Pivot`), and each
terminal method (`agg`, `count`, `pivot`) constructs the `Aggregate` plan. `pivot` is the one that
can run a hidden job: with no explicit value list it collects the distinct values first.

`KeyValueGroupedDataset` (from `ds.groupByKey`) is the typed counterpart and works differently —
it carries key and value encoders and produces `MapGroups` / `FlatMapGroupsWithState` /
`CoGroup` plans, keeping objects rather than rows.
`spark.sql.legacy.dataset.nameNonStructGroupingKeyAsValue` controls what the key column is called,
which is exactly the kind of naming difference that breaks a downstream `select`.

**Anchor files:**

- [classic/RelationalGroupedDataset.scala:54](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/classic/RelationalGroupedDataset.scala#L54)
- [classic/KeyValueGroupedDataset.scala:41](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/classic/KeyValueGroupedDataset.scala#L41)
- [classic/DataFrameNaFunctions.scala:37](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/classic/DataFrameNaFunctions.scala#L37) / [classic/DataFrameStatFunctions.scala:39](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/classic/DataFrameStatFunctions.scala#L39) — the `df.na` and `df.stat` namespaces, same builder pattern

**Configs:** `spark.sql.legacy.dataset.nameNonStructGroupingKeyAsValue`,
`spark.sql.pivotMaxValues`

**Maps to topics:** B6, I1

### SQLContext — a compatibility shell, not a component

**What it is:** the source says it plainly: *"Since Spark 2.0 this class has become a wrapper of
`SparkSession`, where the real functionality resides. This class remains mainly for backward
compatibility."* Its constructors are `@deprecated` and delegate to `SparkSession.builder`. It is
worth one paragraph only so that seeing it in old code does not suggest a second engine path
exists.

**Anchor files:**

- [classic/SQLContext.scala:59](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/classic/SQLContext.scala#L59) — the class and the deprecation note

**Maps to topics:** B2

---

## Artifacts, cache format, and the small surface

### ArtifactManager — a per-session classloader and the isolation flag

**What it is:** the mechanism behind adding code to a running session — the JVM half of what a
Connect client uploads. Each session gets an artifact directory under a root, named by its
**session UUID**, with a `classes/` subdirectory. `addArtifact` copies a file there (JAR, class
file, Python archive, cached blob) and records it; `classloader` builds a `URLClassLoader` over the
session's jars and class directory, chained to the parent.

`withResources` is how execution uses it: it installs the session's classloader as the thread
context classloader *if needed* and sets the `JobArtifactState` so tasks resolve classes from the
same set. When `spark.sql.artifact.isolation.enabled` is false, `state` is `null` and everything
falls back to the application-wide classpath — which is exactly what `enableHiveSupport` triggers.

`cloneSession` explicitly forces a copy of the `ArtifactManager` and its resources, so a cloned
session does not share a mutable artifact set with its parent.

**Anchor files:**

- [artifact/ArtifactManager.scala:56](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/artifact/ArtifactManager.scala#L56) — the class; the per-session paths at :69–:75
- [artifact/ArtifactManager.scala:83](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/artifact/ArtifactManager.scala#L83) — `state`, null when isolation is off
- [artifact/ArtifactManager.scala:110](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/artifact/ArtifactManager.scala#L110) — `withResources`
- [artifact/ArtifactManager.scala:326](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/artifact/ArtifactManager.scala#L326) — `classloader`, with the two-point explanation in the comment at :343

**Configs:** `spark.sql.artifact.isolation.enabled`,
`spark.sql.artifact.isolation.alwaysApplyClassloader`, `spark.sql.artifact.cacheStorageLevel`,
`spark.sql.artifact.copyFromLocalToFs.allowDestLocal`

**Maps to topics:** E19, E9

### CachedBatchSerializer — the pluggable in-memory cache format

**What it is:** `df.cache()` does not store rows; it stores `CachedBatch`es produced by a
`CachedBatchSerializer`, and the serializer is replaceable through `spark.sql.cache.serializer`.
The trait is the full contract for an alternative cache format: whether it accepts columnar input,
how to convert `InternalRow`s or `ColumnarBatch`es into cached batches, how to convert back, and —
crucially — `buildFilter`, which turns a filter expression into a predicate over the *cached batch
statistics* so whole batches can be skipped without decoding.

`SimpleMetricsCachedBatchSerializer` is the built-in base that implements that skipping from
per-column min/max/null-count/size statistics, and `convertToColumnarPlanIfPossible` is the hook an
accelerator uses to swap the plan under the cache.

This is the extension point GPU and native accelerators use to keep cached data in their own
format, and it is separate from `ColumnarRule` (which changes execution, not storage).

**Anchor files:**

- [columnar/CachedBatchSerializer.scala:51](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/columnar/CachedBatchSerializer.scala#L51) — the trait; `CachedBatch` at :41
- [columnar/CachedBatchSerializer.scala:140](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/columnar/CachedBatchSerializer.scala#L140) — `buildFilter`
- [columnar/CachedBatchSerializer.scala:251](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/columnar/CachedBatchSerializer.scala#L251) — `SimpleMetricsCachedBatchSerializer` and its statistics-based skipping

**Configs:** `spark.sql.cache.serializer`

**Maps to topics:** I6, E22

### The default cache format — DefaultCachedBatchSerializer, column types and six compression schemes

**What it is:** the implementation behind `df.cache()` when you have not replaced the serializer,
and it lives in `execution/columnar/` — which this group's `columnar/` scope token also claims
(see the note in "Deliberately not covered").

`DefaultCachedBatchSerializer` declares `supportsColumnarInput = false` and throws on the columnar
conversion, so the built-in path is always rows in. `convertForCacheInternal` batches
`spark.sql.inMemoryColumnarStorage.batchSize` rows at a time into a `DefaultCachedBatch`: for each
column, a `ColumnBuilder` appends values into a `ByteBuffer` in a fixed binary layout given by a
`ColumnType`, while a `ColumnStats` accumulates lower bound, upper bound, null count, row count and
size. Those statistics are the `SimpleMetricsCachedBatch` the `buildFilter` skipping runs on.

**The type layer.** `ColumnType[JvmType]` is a per-type codec — `INT` (4 bytes), `LONG` (8),
`BOOLEAN` (1), the two interval types, `COMPACT_DECIMAL` versus `LARGE_DECIMAL`, `STRING`,
`BINARY`, `CALENDAR_INTERVAL`, `VARIANT`, plus struct/array/map. Reading the list tells you the
cache's real memory cost per column far better than any estimate: a decimal is one of two very
different encodings depending on precision.

**The compression layer.** When `inMemoryColumnarStorage.compressed` is on (the default), a
`CompressibleColumnBuilder` gathers each scheme's compression ratio during the build pass and picks
the best. Six schemes exist, each with a `typeId` written into the buffer and a `supports` test:
`PassThrough` (0), `RunLengthEncoding` (1), `DictionaryEncoding` (2), `BooleanBitSet` (3),
`IntDelta` (4), `LongDelta` (5). So caching is *not* a memcpy — it is an encode pass whose cost and
benefit depend entirely on your column's distribution.

**The read side.** `InMemoryTableScanExec` applies the serializer's filter to skip whole batches
(`readPartitions` / `readBatches` accumulators make that visible), and
`GenerateColumnAccessor` generates the per-batch row-extraction code. Its `supportsColumnar` is
decided by whether the serializer supports columnar *output*, which for the default serializer
depends on the schema.

`CachedRDDBuilder` sits above all of it, holding the cached RDD and the size accumulator, with a
documented synchronisation so a `clearCache` cannot race a concurrent materialisation.

**Anchor files:**

- [execution/columnar/InMemoryRelation.scala:105](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/columnar/InMemoryRelation.scala#L105) — `DefaultCachedBatchSerializer`; `DefaultCachedBatch` at :53, `CachedRDDBuilder` at :255, `buildBuffers` at :343
- [execution/columnar/InMemoryTableScanExec.scala:53](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/columnar/InMemoryTableScanExec.scala#L53) — the scan; the filter application and batch counters at :144–:174
- [execution/columnar/ColumnType.scala:111](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/columnar/ColumnType.scala#L111) — `ColumnType` and every concrete type below it
- [execution/columnar/ColumnStats.scala:25](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/columnar/ColumnStats.scala#L25) — `ColumnStatisticsSchema` (the five statistics per column) and `PartitionStatistics`
- [execution/columnar/ColumnBuilder.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/columnar/ColumnBuilder.scala) / [ColumnAccessor.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/columnar/ColumnAccessor.scala) / [NullableColumnBuilder.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/columnar/NullableColumnBuilder.scala) / [NullableColumnAccessor.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/columnar/NullableColumnAccessor.scala) — the build/read pair and their null-tracking layer
- [execution/columnar/GenerateColumnAccessor.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/columnar/GenerateColumnAccessor.scala) — the codegen that turns a cached batch back into rows
- [execution/columnar/compression/CompressionScheme.scala:50](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/columnar/compression/CompressionScheme.scala#L50) — the `Encoder`/`Decoder`/`CompressionScheme` triple and `AllCompressionSchemes`
- [execution/columnar/compression/compressionSchemes.scala:33](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/columnar/compression/compressionSchemes.scala#L33) — the six schemes with their type ids
- [execution/columnar/compression/CompressibleColumnBuilder.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/columnar/compression/CompressibleColumnBuilder.scala) / [CompressibleColumnAccessor.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/columnar/compression/CompressibleColumnAccessor.scala) — where the best scheme is chosen

**Configs:** `spark.sql.inMemoryColumnarStorage.compressed` (`true`), `…batchSize` (`10000`),
`…partitionPruning`, `…enableVectorizedReader`, `…hugeVectorThreshold`,
`…hugeVectorReserveRatio`, `spark.sql.columnVector.offheap.enabled`

**Maps to topics:** I6, E1

### VariableSubstitution — `${}` in SQL text, and the four namespaces it binds

**What it is:** before parsing, SQL text passes through `VariableSubstitution`, which expands
`${var}` references against the SQL conf. It binds four prefixes to the same provider —
`spark`, `sparkconf`, `hivevar` and `hiveconf` — so all four spellings work, and it routes lookups
through `conf.redactOptions` so a substituted secret is redacted in anything that logs the query.

Controlled by `spark.sql.variable.substitute`; when off, the text passes through untouched.

**Anchor files:**

- [internal/VariableSubstitution.scala:29](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/internal/VariableSubstitution.scala#L29) — the class, the four bindings and the redaction

**Configs:** `spark.sql.variable.substitute`

**Maps to topics:** B8

### The small surface — implicits, conversions, helpers, and the streaming façades

**What it is:** the remainder of the package, each file doing one small job:

- **`SQLImplicits`** — the `import spark.implicits._` surface, extending the `sql/api` version so
  the same implicits exist in Connect.
- **`ClassicConversions`** (`conversions.scala`) — the implicit casts from `sql/api` types to
  classic ones, plus `toRichColumn`.
- **`TableValuedFunction`** — `spark.tvf.range(...)` and friends, the DataFrame entry to SQL's
  table-valued functions.
- **`StreamingQuery`, `StreamingQueryManager`, `StreamingCheckpointManager`** — the classic
  implementations of the streaming façades, with the manager owning active queries and the
  listener registration; `StreamingCheckpointManager` is the 4.x API for inspecting and
  manipulating a query's checkpoint.
- **`HiveSerDe`** — the table of built-in SerDe/format shorthands (`STORED AS PARQUET` → input
  format, output format, serde class) used when creating Hive-format tables.
- **`internal/package.scala`** and **`classic/package.scala`** — the type aliases and package
  objects that keep `DataFrame` meaning `Dataset[Row]`.

**Anchor files:**

- [classic/SQLImplicits.scala:27](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/classic/SQLImplicits.scala#L27) / [classic/conversions.scala:38](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/classic/conversions.scala#L38)
- [classic/TableValuedFunction.scala:23](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/classic/TableValuedFunction.scala#L23)
- [classic/StreamingQueryManager.scala:51](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/classic/StreamingQueryManager.scala#L51) / [classic/StreamingQuery.scala:22](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/classic/StreamingQuery.scala#L22) / [classic/StreamingCheckpointManager.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/classic/StreamingCheckpointManager.scala)
- [internal/HiveSerDe.scala:26](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/internal/HiveSerDe.scala#L26) / [internal/package.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/internal/package.scala)

**Maps to topics:** B2, A7

---

## Breadth checks

### Config breadth

Slice over `sql/catalyst` + `sql/core` with the pattern:

```
catalogImplementation|warehouse|globalTempDatabase|artifact|inMemoryColumnarStorage|columnVector
|\.session|sessionState|sharedState|variable\.substitute|\.udf\.|allowUntypedScalaUDF|\.crossJoin
|\.dataset|\.observ|\.defaultCatalog|\.legacy\.(createHiveTableByDefault|allowUntyped
|sessionInitWithConfigDefaults)|sqlContext|\.debug|\.cache|\.storeAssignmentPolicy|\.charAsVarchar
```

**53 keys**, deliberately wide.

| Family | Keys | Tied to |
|---|---|---|
| `artifact.*` | 4 | ArtifactManager |
| `inMemoryColumnarStorage.*`, `cache.serializer`, `columnVector.offheap.enabled` | 8 | CachedBatchSerializer |
| `catalogImplementation`, `warehouse.dir`, `globalTempDatabase`, `defaultCatalog` | 4 | SharedState, Catalog |
| `session.timeZone`, `legacy.sessionInitWithConfigDefaults` | 2 | SparkSession construction |
| `variable.substitute` | 1 | VariableSubstitution |
| `legacy.allowUntypedScalaUDF`, `legacy.createHiveTableByDefault`, `legacy.dataset.nameNonStructGroupingKeyAsValue` | 3 | UDFRegistration, writer, grouping datasets |
| `debug`, `debug.maxToStringFields` | 2 | Dataset `explain` / `toString` |
| `storeAssignmentPolicy`, `charAsVarchar` | 2 | DataFrameWriter (assignment on insert) |
| `crossJoin.enabled` | 1 | Dataset `crossJoin` |

**Out of scope, and where they belong** (kept in the slice per the err-wide rule):

| Family | Keys | Owning group |
|---|---|---|
| `session.localRelation*` (7) | 7 | `sql/connect` — client-side local-relation chunking |
| `execution.python*` / `pyspark.udf.*` / `pandas.udf.buffer.size` (10) | 10 | `python-arrow` |
| `sessionWindow.*`, `streaming.sessionWindow.*` (5) | 5 | `agg-window-exchange`, `streaming-exec` |
| `codegen.cache.maxEntries`, `subexpressionElimination.cache.maxEntries`, `execution.topKSortFallbackThreshold`, `functionResolution.sessionOrder` | 4 | `query-execution`, `sql/catalyst` |

Every in-scope key ties to a concept above.

### Package breadth

Walked by hand. None of the four scope directories has a sub-package.

| Package | Files | Cited |
|---|---|---|
| `sql/classic/` | 27 | 27 |
| `sql/internal/` | 7 | 7 |
| `sql/artifact/` | 1 | 1 |
| `sql/columnar/` | 1 | 1 |
| `sql/execution/columnar/` (+ `compression/`) | 13 | 13 |

**49 of 49.**

!!! warning "A scope token claimed a second package, and the by-hand walk missed it first"

    The group's scope names `columnar/`, meaning the plugin API at `sql/columnar/`. But a scope
    token is a path *segment*, so it also claims `sql/execution/columnar/` — the 13-file
    implementation of the default cache format. The first pass of this sweep covered only the
    1-file plugin API and reported "36 of 36"; `check_drift.py --sweeps` caught it, reporting
    `columnar/ 15 files 1 cited (6%)`.

    The package was then swept rather than disclaimed, because it belongs here: it is the
    implementation of the very interface this group already owns. But the lesson is the one
    `SKILL.md` states — walk the scope's directories with `ls`, and do not trust a by-hand count
    that agrees with your expectations. (The checker also resolves `columnar/` to 15 and
    `internal/` to 9 where the filesystem shows 14 and 7; the two extra entries are a
    path-matching artefact and are not real files.)

### Deliberately not covered

- **`sql/api`** — the abstract half of every class here. It is a separate module with no group in
  `groups.yaml`; this page cites `Column`/`ColumnNode` for orientation but does not trace it. That
  is the obvious next carving question: `sql/api` is where the Connect-compatible contract lives
  and nothing sweeps it.
- **`SparkSessionExtensions.scala`** — sits directly under `org/apache/spark/sql/`, outside this
  group's four scope directories. Cited as the API surface `BaseSessionStateBuilder` consumes;
  the file itself belongs to whichever group ends up claiming the top-level `sql/` files.
- **`HiveSessionStateBuilder`** (`sql/hive`) — the other `BaseSessionStateBuilder` subclass, in the
  `sql/hive` subsystem's group.
- **The engine behind these calls.** `QueryExecution`, `SQLExecution`, `CacheManager`,
  `SessionCatalog`, `UnresolvedDataSource` resolution — named throughout, traced by the
  `query-execution`, `datasources` and catalyst sweeps.
- **The Connect implementation** of the same interfaces — `sql/connect`'s own group.

---

## Refresh log

| Date | Spark | What changed |
|---|---|---|
| 2026-08-06 | 4.2.0 | Initial sweep of the group: 49/49 files across five packages, 23 concepts, 2 new topics proposed (A37 `ColumnNode` and the api/classic/connect split, E29 the sixteen `SparkSessionExtensions` injection points). Package breadth needed two passes — the first walked only the four directories the scope *names* and reported 36/36; `check_drift.py --sweeps` showed the `columnar/` token also claims `execution/columnar/` (13 files, the default cache format implementation), which was then swept rather than disclaimed. Headline findings: a `Dataset` over a command has **already executed it** at construction (`commandExecuted`), which is why DDL and DML run without an action; `enableHiveSupport` silently sets `spark.sql.artifact.isolation.enabled=false`, with the reason given in a source comment; `DataFrameReader.load` no longer resolves the provider — it builds one `UnresolvedDataSource` and defers everything to analysis; and a static config rejected loudly by `spark.conf.set` is only *warned about* by `.config()` on an existing session. Recorded a carving gap: `sql/api`, the module every class here implements, is claimed by no group |
