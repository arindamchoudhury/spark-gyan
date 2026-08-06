---
subsystem: sql/core
spark_version: "4.2.0"
swept_at: 2026-08-06
group: agg-window-exchange
all_groups: [query-execution, joins-exec, adaptive, datasources, agg-window-exchange, python-arrow, streaming-exec, classic-api, sql-scripting]
status: complete
concepts:
  - name: AggUtils.createAggregate — the three-operator ladder, and the two test-only overrides
    topics: [B6]
  - name: Two-phase aggregation — partial, shuffle, final, and where map-side combine lives
    topics: [B6, I5]
  - name: planAggregateWithOneDistinct — one DISTINCT becomes four aggregate stages
    topics: [B6]
  - name: HashAggregateExec and TungstenAggregationIterator — spill, then the sort-based fallback
    topics: [B6, I6]
  - name: The codegen fast hash map — two levels, row-based or vectorized
    topics: []
    propose:
      code: A33
      level: Advanced
      title: "Two-Level Hash Aggregation and the Codegen Fast Hash Map"
      what: "Whole-stage codegen puts a generated, fixed-capacity hash map in front of the real BytesToBytesMap — a row-based one by default, a columnar one behind a second flag — that probes with at most two linear steps and silently declines every key whose type or aggregation mode it does not support."
      why: "It is the layer that decides whether a groupBy runs at memory bandwidth or at BytesToBytesMap speed, it is on by default and invisible in EXPLAIN, and its eligibility rules — primitive/decimal/string keys only, partial modes only unless a config is flipped — explain why two structurally identical aggregates can differ several-fold in runtime."
  - name: splitAggregateExpressions — the JVM 64 KB method limit inside an aggregate
    topics: [B6, A21]
  - name: ObjectHashAggregateExec and the 128-row sort-based fallback
    topics: [B6, A22]
  - name: SortAggregateExec — the operator of last resort, and its ordering requirement
    topics: [B6]
  - name: AggregationIterator — declarative vs imperative vs typed-imperative, FILTER, and the pre-shuffle serialize
    topics: [B6, I3]
  - name: Session windows in the aggregate layer — UpdatingSessionsExec and MergingSessionsExec
    topics: [A8, I2]
  - name: User-defined aggregates — ScalaUDAF and ScalaAggregator
    topics: [I3, A5]
  - name: BaseAggregateExec.requiredChildDistribution — and why a streaming aggregate pins its partition count
    topics: [B6, A8]
  - name: WindowExec — one partition of one window buffered at a time
    topics: [I2]
  - name: The window frame factory — seven factory keys, six frame implementations
    topics: [I2]
  - name: BoundOrdering — how a RANGE offset is turned into a comparison
    topics: [I2]
  - name: AggregateProcessor and the SizeBasedWindowFunction partition-size injection
    topics: [I2]
  - name: Segment-tree window frames — the 4.2.0 sliding-frame algorithm, off by default
    topics: []
    propose:
      code: A34
      level: Advanced
      title: "Segment-Tree Window Frames: O(log W) Sliding Windows"
      what: "Spark 4.2.0 adds an opt-in window-frame implementation that builds a blocked segment tree over the buffered partition so a moving frame is answered in O(log W) merges instead of re-aggregating W rows, with an LRU of internal nodes registered as a TaskMemoryManager consumer that can spill."
      why: "It is the first change to sliding-window cost since the operator was written — a `ROWS BETWEEN 1000 PRECEDING AND CURRENT ROW` goes from quadratic to near-linear — but it is disabled by default, restricted to nine allowlisted aggregates, refuses any frame carrying a FILTER, and falls back to the old sliding frame below a row threshold, so knowing when it actually engages is the whole skill."
  - name: WindowGroupLimitExec — top-N per group, in a partial and a final mode
    topics: [I2]
  - name: ShuffleExchangeExec and the six ShuffleOrigins
    topics: [I5, A26]
  - name: prepareShuffleDependency — one partitioner per Partitioning, and the RangePartitioner's hidden job
    topics: [I5, A26]
  - name: Round-robin determinism — the local sort before repartition
    topics: [I5, A14]
  - name: needToCopyObjectsBeforeShuffle — when a row must be copied before it is handed to the writer
    topics: [I5, E1]
  - name: Order-independent row checksums on the SQL shuffle
    topics: [A14]
  - name: BroadcastExchangeExec — an async future, two row ceilings, and a job tag
    topics: [B7, A3]
  - name: EnsureRequirements — satisfaction, best shuffle spec, and the minimum-parallelism rule
    topics: [A26, I5]
  - name: reorderJoinKeys and reorderJoinPredicates — matching an existing partitioning instead of reshuffling
    topics: [A26, A3]
  - name: Storage-partitioned join compatibility — checkKeyGroupCompatible, reducers, GroupPartitionsExec
    topics: [A25]
  - name: ValidateRequirements — the check that silently reverts an AQE rule
    topics: [A2, A26]
  - name: ReusedExchangeExec — one exchange, many consumers
    topics: [A26, A2]
  - name: V1 bucketing at execution time — the two rules that coalesce or disable a bucketed scan
    topics: []
    propose:
      code: I29
      level: Intermediate
      title: "Bucketed Tables: bucketBy, and the Two Rules That Undo Bucketing"
      what: "A table written with bucketBy carries its hash partitioning into the scan, so a join on the bucket columns can skip the shuffle — but two physical rules rewrite that decision at planning time: one coalesces the larger side when the bucket counts differ by a divisible ratio, and one disables bucketed scanning entirely whenever nothing downstream is interested in the partitioning."
      why: "Bucketing is the only way to make a large-to-large join shuffle-free in Spark's own file formats, and it is also the feature most likely to appear to do nothing: two off-by-default configs, a divisibility requirement, an interesting-partition analysis, and a maximum bucket count all sit between `bucketBy` and a plan without an Exchange."
  - name: PartitionPruning — where a DPP subquery is inserted, and the cost model that gates it
    topics: [A18]
  - name: PlanDynamicPruningFilters — reuse the broadcast, duplicate the subquery, or give up
    topics: [A18, A2]
  - name: CleanupDynamicPruningFilters — removing the filters that never reached a scan
    topics: [A18]
  - name: RowLevelOperationRuntimeGroupFiltering — a runtime filter for MERGE, UPDATE and DELETE
    topics: [A6, E23]
---

# sql/core — aggregation, windows and exchange

> Source sweep of the `agg-window-exchange` group: `execution/aggregate/`, `execution/window/`,
> `execution/exchange/`, `execution/bucketing/`, `execution/dynamicpruning/` — 43 files, swept
> against **Spark 4.2.0** (`v4.2.0` tag, the version `configs/catalog.yaml` was parsed from).

This is the physical layer under three of the most common things a query does: group rows, rank
rows within a group, and move rows between partitions. It also holds the two rules that decide
whether a bucketed table's partitioning survives planning, and the four rules that turn a join
into a runtime partition filter.

!!! info "What this group is *not*"

    The logical and optimizer-side halves of these features live in `sql/catalyst` and are covered
    by the catalyst sweeps: `RewriteDistinctAggregates`, `InferWindowGroupLimit`, `Distribution` /
    `Partitioning` themselves, and `EliminateDistinct`. AQE's runtime rewrites of the exchanges
    planned here — coalescing, skew splitting, local reads — are the `adaptive` group's page.
    Python and Arrow window/aggregate operators are the `python-arrow` group's.

---

## Aggregation

### AggUtils.createAggregate — the three-operator ladder, and the two test-only overrides

**What it is:** every `Aggregate` logical node is planned by one function, which picks one of three
physical operators in a fixed priority order. `HashAggregateExec` wins whenever every aggregate
buffer field is a mutable, fixed-width type. Otherwise `ObjectHashAggregateExec` takes it if the
functions are `TypedImperativeAggregate`s and the operator is enabled. Otherwise
`SortAggregateExec`. There is no cost model here at all — it is a type check.

Two hard-coded, deliberately unregistered config keys can force the ladder downward, and both are
gated on `Utils.isTesting`, so they are inert in a real cluster:
`spark.sql.test.forceApplySortAggregate` and `spark.sql.test.forceApplyObjectHashAggregate`. They
are worth knowing only because they explain the two `forceApply*` branches at the top of the
function.

`mayRemoveAggFilters` strips the `FILTER (WHERE …)` clause from every aggregate that is not in
`Partial` or `Complete` mode — the filter has already been applied on the map side, so re-applying
it during a merge would double-filter.

**Code path:** `Aggregation` strategy → `AggUtils.planAggregateWithoutDistinct` /
`planAggregateWithOneDistinct` → `createAggregate` → one of three operators

**Anchor files:**

- [AggUtils.scala:69](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/aggregate/AggUtils.scala#L69) — `createAggregate`, the ladder
- [AggUtils.scala:35](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/aggregate/AggUtils.scala#L35) — `mayRemoveAggFilters`
- [AggUtils.scala:591](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/aggregate/AggUtils.scala#L591) — `forceApplySortAggregate`, testing-only

**Configs:** `spark.sql.execution.useObjectHashAggregateExec` (default `true`, 2.2.0)

**Maps to topics:** B6

### Two-phase aggregation — partial, shuffle, final, and where map-side combine lives

**What it is:** `planAggregateWithoutDistinct` builds *two* aggregate operators from one logical
node. The partial one is created with `requiredChildDistributionExpressions = None` — meaning it
demands nothing of its child and therefore never causes an exchange. The final one is created with
`Some(groupingAttributes)`, which is what makes `EnsureRequirements` insert the shuffle between
them. Map-side combine in Spark SQL is not a flag; it is the existence of that first operator.

The partial operator's output expressions are the grouping attributes plus each function's
`inputAggBufferAttributes` — buffers, not results. `initialInputBufferOffset` tells the final
operator how many leading columns to skip before it finds those buffers.

**Anchor files:**

- [AggUtils.scala:126](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/aggregate/AggUtils.scala#L126) — `planAggregateWithoutDistinct`
- [AggUtils.scala:143](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/aggregate/AggUtils.scala#L143) — the partial aggregate, `requiredChildDistributionExpressions = None`
- [AggUtils.scala:163](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/aggregate/AggUtils.scala#L163) — the final aggregate, which is what demands the shuffle

**Maps to topics:** B6, I5

### planAggregateWithOneDistinct — one DISTINCT becomes four aggregate stages

**What it is:** `SELECT count(DISTINCT x), sum(y) FROM t GROUP BY k` cannot be done in two phases,
because the distinct column must be de-duplicated before it is counted. Spark's answer is four
aggregate operators:

1. **Partial** over `(grouping ++ distinct)` columns — this is the de-duplication, done as a
   grouping.
2. **PartialMerge** over the same extended key, after a shuffle.
3. **PartialDistinct** over the grouping key only: merges the non-distinct buffers and starts the
   distinct functions in `Partial` mode, now that their inputs are unique.
4. **Final and Complete** — merges both families.

The distinct functions are rewritten to *non*-distinct aggregate expressions with `isDistinct`
kept true purely so `EXPLAIN` still shows the word — the execution is an ordinary aggregate over
already-unique input.

**Anchor files:**

- [AggUtils.scala:175](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/aggregate/AggUtils.scala#L175) — `planAggregateWithOneDistinct`
- [AggUtils.scala:242](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/aggregate/AggUtils.scala#L242) — stage 3, where the distinct function is rewritten and `isDistinct` is preserved only for display

!!! warning "This path handles exactly one distinct expression"

    Two or more distinct aggregates never reach here: catalyst's `RewriteDistinctAggregates`
    expands them into an `Expand` that emits one row per distinct group *per input row* first.
    That is the multiplication behind the standard advice not to stack `countDistinct`s.

**Maps to topics:** B6

### HashAggregateExec and TungstenAggregationIterator — spill, then the sort-based fallback

**What it is:** the hash aggregate holds an `UnsafeFixedWidthAggregationMap` (a `BytesToBytesMap`
of grouping key → fixed-width buffer). When the map cannot allocate a buffer for a new key, the
operator does **not** fail and does not simply spill and continue with a hash map. It:

1. calls `destructAndCreateExternalSorter()`, converting the whole map into a sorted spill,
2. retries the allocation once against the now-empty map,
3. throws `SparkOutOfMemoryError("AGGREGATE_OUT_OF_MEMORY")` if even the first page cannot be
   allocated,
4. and once the input is exhausted, if a sorter exists, **switches the whole task to sort-based
   aggregation** — merging every spill by key and aggregating a sorted stream.

So a spilling hash aggregate changes algorithm mid-task, per task. The `numTasksFallBacked` metric
counts exactly this, and it is visible in the SQL tab; `spillSize` is computed as the task's
`memoryBytesSpilled` delta rather than tracked directly.

`spark.sql.TungstenAggregate.testFallbackStartsAt` is another unregistered, testing-only key: it
forces the fallback after N rows and also caps the fast-map capacity to `log2(N)`.

**Code path:** `HashAggregateExec.doExecute` → `TungstenAggregationIterator.processInputs` →
`destructAndCreateExternalSorter` → `switchToSortBasedAggregation`

**Anchor files:**

- [HashAggregateExec.scala:51](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/aggregate/HashAggregateExec.scala#L51) — the operator; scaladoc at :49 states the fallback outright
- [HashAggregateExec.scala:80](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/aggregate/HashAggregateExec.scala#L80) — `testFallbackStartsAt`
- [TungstenAggregationIterator.scala:184](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/aggregate/TungstenAggregationIterator.scala#L184) — `processInputs`, the spill loop
- [TungstenAggregationIterator.scala:251](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/aggregate/TungstenAggregationIterator.scala#L251) — `switchToSortBasedAggregation`
- [HashAggregateExec.scala:665](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/aggregate/HashAggregateExec.scala#L665) — the generated `findOrInsertRegularHashMap`, including the `AGGREGATE_OUT_OF_MEMORY` throw

**Maps to topics:** B6, I6

### The codegen fast hash map — two levels, row-based or vectorized

**What it is:** when whole-stage codegen is on, `HashAggregateExec` can generate a *second*,
smaller hash map and probe it before the real one. The generated class is fixed-capacity
(`1 << capacityBit`, default 16 → 65 536 slots), power-of-two bucketed, and probes at most
**two** linear steps before declaring a miss and deferring to the `BytesToBytesMap`. It never
grows and never evicts; a miss is simply a fall-through.

Two implementations exist. `RowBasedHashMapGenerator` backs it with a `RowBasedKeyValueBatch` and
is the default. `VectorizedHashMapGenerator` backs it with `OnHeapColumnVector`s plus a
`MutableColumnarRow` over the aggregate-buffer columns, and is enabled only by a separate flag
described in-source as testing/benchmarking.

Eligibility is checked by `checkIfFastHashMapSupported` and is narrow:

- every grouping-key and buffer field must be primitive, `DecimalType`, `StringType` or
  `CalendarIntervalType`;
- no byte-array-backed (high-precision) decimal in the *buffer* — `ColumnVector.putDecimal` cannot
  update those in place;
- and, unless `…twolevel.partialOnly` is set to false, every aggregate mode must be `Partial` or
  `PartialMerge`. **Final aggregates get no fast map by default.**

When the flag is on but the query is ineligible, the operator logs at INFO and moves on silently.

**Code path:** `doProduceWithKeys` → `enableTwoLevelHashMap` → `checkIfFastHashMapSupported` →
`RowBasedHashMapGenerator.generate()` / `VectorizedHashMapGenerator.generate()` → generated
`findOrInsert`

**Anchor files:**

- [HashAggregateExec.scala:386](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/aggregate/HashAggregateExec.scala#L386) — `checkIfFastHashMapSupported`, all three gates
- [HashAggregateExec.scala:411](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/aggregate/HashAggregateExec.scala#L411) — `enableTwoLevelHashMap`, and the INFO-log-and-continue path
- [HashMapGenerator.scala:34](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/aggregate/HashMapGenerator.scala#L34) — the shared generator skeleton
- [RowBasedHashMapGenerator.scala:149](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/aggregate/RowBasedHashMapGenerator.scala#L149) — the two-step linear probe
- [VectorizedHashMapGenerator.scala:45](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/aggregate/VectorizedHashMapGenerator.scala#L45) — the columnar variant

**Configs:** `spark.sql.codegen.aggregate.map.twolevel.enabled` (`true`, 2.3.0),
`spark.sql.codegen.aggregate.map.twolevel.partialOnly` (`true`, 3.2.1),
`spark.sql.codegen.aggregate.map.vectorized.enable` (`false`, 3.0.0),
`spark.sql.codegen.aggregate.fastHashMap.capacityBit` (`16`, 2.4.0)

**Maps to topics:** none — proposed as **A33**

### splitAggregateExpressions — the JVM 64 KB method limit inside an aggregate

**What it is:** the generated update/merge code for all aggregate functions of one operator is
emitted as a single expression block. With enough functions this exceeds the JVM's 64 KB method
bytecode limit and whole-stage codegen falls back to interpreted execution for the entire stage.
`splitAggregateExpressions` breaks the per-function code into separate generated methods — but
only when the parameter list is short enough for a valid JVM method signature
(`CodeGenerator.isValidParamLength`), otherwise it declines and returns the unsplit code.

`AggregateCodegenSupport.supportCodegen` is the earlier gate: it refuses codegen outright when any
aggregate buffer field is not mutable, or when the operator carries an aggregate with a filter it
cannot express.

**Anchor files:**

- [AggregateCodegenSupport.scala:299](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/aggregate/AggregateCodegenSupport.scala#L299) — `splitAggregateExpressions`
- [AggregateCodegenSupport.scala:83](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/aggregate/AggregateCodegenSupport.scala#L83) — `supportCodegen`

**Configs:** `spark.sql.codegen.aggregate.splitAggregateFunc.enabled` (`true`, 3.0.0),
`spark.sql.codegen.aggregate.sortAggregate.enabled` (`true`, 3.3.0)

**Maps to topics:** B6, A21

### ObjectHashAggregateExec and the 128-row sort-based fallback

**What it is:** the operator for aggregates whose buffer is a JVM object rather than a fixed-width
row — `collect_list`, `collect_set`, `percentile`, the sketch aggregates, and every
`TypedImperativeAggregate`. Its buffer store is an ordinary `java.util.LinkedHashMap` wrapper
(`ObjectAggregationMap`), which has no memory accounting at all. Its only defence is a **row
count**: once the map holds `spark.sql.objectHashAggregate.sortBased.fallbackThreshold` distinct
groups — default **128** — and input remains, it dumps the map through
`dumpToExternalSorter` and switches to `SortBasedAggregator` for the rest of the task.

128 is a very low ceiling, and it is a count of *groups*, not of bytes. A `collect_list` over 200
groups falls back; one over 100 groups holding a million elements each does not.

**Anchor files:**

- [ObjectHashAggregateExec.scala:60](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/aggregate/ObjectHashAggregateExec.scala#L60) — the operator, `numTasksFallBacked` metric at :80
- [ObjectAggregationIterator.scala:176](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/aggregate/ObjectAggregationIterator.scala#L176) — the threshold check and the warning it logs
- [ObjectAggregationIterator.scala:234](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/aggregate/ObjectAggregationIterator.scala#L234) — `SortBasedAggregator`
- [ObjectAggregationMap.scala:71](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/aggregate/ObjectAggregationMap.scala#L71) — `dumpToExternalSorter`

**Configs:** `spark.sql.objectHashAggregate.sortBased.fallbackThreshold` (`128`, 2.2.0)

**Maps to topics:** B6, A22

### SortAggregateExec — the operator of last resort, and its ordering requirement

**What it is:** aggregates a sorted stream with a single reusable buffer, emitting one row per
group boundary. It is the only aggregate operator that declares a `requiredChildOrdering`, so
choosing it adds a `SortExec` on top of the exchange. It supports codegen only when the config
allows sort-aggregate codegen and the buffer is mutable; `BaseAggregateExec.toSortAggregate`
exists so other operators can be rewritten into it.

Its empty-input case is explicit: `outputForEmptyGroupingKeyWithoutInput` returns the initialized
buffer, which is why `SELECT count(*) FROM empty_table` yields `0` rather than no rows.

**Anchor files:**

- [SortAggregateExec.scala:34](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/aggregate/SortAggregateExec.scala#L34) — the operator; `requiredChildOrdering` at :51
- [SortBasedAggregationIterator.scala:123](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/aggregate/SortBasedAggregationIterator.scala#L123) — `processCurrentSortedGroup`
- [SortBasedAggregationIterator.scala:179](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/aggregate/SortBasedAggregationIterator.scala#L179) — the empty-input result

**Maps to topics:** B6

### AggregationIterator — declarative vs imperative vs typed-imperative, FILTER, and the pre-shuffle serialize

**What it is:** the shared base of all three aggregate iterators. It splits aggregate functions
into two families and drives them differently:

- **`DeclarativeAggregate`** — expressed as catalyst expressions (`initialValues`,
  `updateExpressions`, `mergeExpressions`). These are compiled into a single `MutableProjection`
  over the joined (buffer, input) row, so N declarative functions cost one projection.
- **`ImperativeAggregate`** — driven by explicit `update` / `merge` / `eval` calls in a loop, each
  bound to its own buffer offset. `initializeAggregateFunctions` is where those offsets are
  assigned, and getting them wrong is the classic UDAF bug.

A `FILTER (WHERE …)` clause becomes a predicate wrapped around the per-function update, evaluated
before the buffer is touched.

The subtle part is in `generateResultProjection`: in `Partial`/`PartialMerge` mode, every
`TypedImperativeAggregate` must have `serializeAggregateBufferInPlace` called on its buffer before
the row can be shuffled, because the buffer holds a JVM object that the shuffle serializer cannot
write. That call is what makes `TypedImperativeAggregate` work across a shuffle at all.

**Anchor files:**

- [AggregationIterator.scala:74](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/aggregate/AggregationIterator.scala#L74) — `initializeAggregateFunctions`, buffer-offset assignment
- [AggregationIterator.scala:156](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/aggregate/AggregationIterator.scala#L156) — `generateProcessRow`, the two families and the filter
- [AggregationIterator.scala:271](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/aggregate/AggregationIterator.scala#L271) — the pre-shuffle `serializeAggregateBufferInPlace`

**Maps to topics:** B6, I3

### Session windows in the aggregate layer — UpdatingSessionsExec and MergingSessionsExec

**What it is:** `session_window(ts, gap)` is not a window function and does not use `WindowExec`.
It is planned as an aggregation with two extra operators:

- **`UpdatingSessionsExec`** — over rows sorted by `(keys without session, session start)`, walks
  the sorted run and rewrites each row's session column so every row belonging to the same session
  carries the same session spec. It buffers the rows of the current session in an
  `ExternalAppendOnlyUnsafeRowArray` because the session's end is only known once a gap is seen,
  and it keeps a *second* array for rows of a previous session whose iterator was not fully drained.
- **`MergingSessionsExec`** — the same sorted-run walk, but merging aggregate buffers as it goes,
  so the session and the aggregation are computed in one pass.

`AggUtils.mayAppendUpdatingSessionExec` / `mayAppendMergingSessionExec` decide when to insert them,
keyed on the `SessionWindow.marker` metadata attached to the grouping expression. The streaming
variant, `planStreamingAggregationForSession`, threads them through
`SessionWindowStateStoreRestoreExec` / `SaveExec` and can optionally pre-merge sessions inside a
partition before the shuffle to reduce shuffle volume.

Grouping *only* by the session window is rejected with `_LEGACY_ERROR_TEMP_3068` — a session
window needs at least one other grouping key.

**Anchor files:**

- [AggUtils.scala:419](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/aggregate/AggUtils.scala#L419) — `planStreamingAggregationForSession`, and the no-other-key error at :432
- [AggUtils.scala:533](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/aggregate/AggUtils.scala#L533) — `mayAppendUpdatingSessionExec`
- [UpdatingSessionsExec.scala:39](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/aggregate/UpdatingSessionsExec.scala#L39) — the operator and its ordering requirement
- [UpdatingSessionsIterator.scala:40](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/aggregate/UpdatingSessionsIterator.scala#L40) — the two row arrays
- [MergingSessionsExec.scala:41](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/aggregate/MergingSessionsExec.scala#L41) / [MergingSessionsIterator.scala:35](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/aggregate/MergingSessionsIterator.scala#L35)

**Configs:** `spark.sql.sessionWindow.buffer.in.memory.threshold` (`4096`, 3.2.0),
`spark.sql.sessionWindow.buffer.spill.threshold` (3.2.0),
`spark.sql.sessionWindow.buffer.spill.size.threshold` (4.1.0),
`spark.sql.streaming.sessionWindow.merge.sessions.in.local.partition` (`false`, 3.2.0),
`spark.sql.streaming.sessionWindow.stateFormatVersion` (`1`, 3.2.0)

**Maps to topics:** A8, I2

### User-defined aggregates — ScalaUDAF and ScalaAggregator

**What it is:** two distinct UDAF mechanisms live in one file.

- **`ScalaUDAF`** wraps the old `UserDefinedAggregateFunction` (buffer as a `Row` of catalyst
  types, `MutableAggregationBuffer`). It is an `ImperativeAggregate`: every `update` converts the
  input row to Scala types and back.
- **`ScalaAggregator`** wraps the typed `Aggregator[IN, BUF, OUT]` and is a
  `TypedImperativeAggregate`: the buffer is the user's own object, serialized by the buffer encoder
  only when it must cross a shuffle. This is the path `udaf()` registers, and the one to use.

`ScalaAggregator`'s encoders are resolved by a dedicated analyzer extension rule,
`ResolveEncodersInScalaAgg`, not during ordinary expression resolution.

**Anchor files:**

- [udaf.scala:349](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/aggregate/udaf.scala#L349) — `ScalaUDAF`
- [udaf.scala:487](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/aggregate/udaf.scala#L487) — `ScalaAggregator`
- [udaf.scala:575](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/aggregate/udaf.scala#L575) — `ResolveEncodersInScalaAgg`
- [TypedAggregateExpression.scala:32](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/aggregate/TypedAggregateExpression.scala#L32) — the Dataset-API `Aggregator` hook, with a simple (single-primitive-buffer) and a complex variant

**Maps to topics:** I3, A5

### BaseAggregateExec.requiredChildDistribution — and why a streaming aggregate pins its partition count

**What it is:** the shared trait behind all four aggregate operators. Its
`requiredChildDistribution` translates `requiredChildDistributionExpressions` into a
`Distribution`: `None` → `UnspecifiedDistribution` (the partial aggregate), `Some(Nil)` →
`AllTuples` (a global aggregate — one partition), otherwise `ClusteredDistribution`. When
`numShufflePartitions` is set — which happens only for streaming, filled in by
`IncrementalExecution`'s state rules — that number is baked into the distribution so the shuffle
cannot be re-partitioned by AQE, because the state store is keyed by partition id.

It also mixes in `PartitioningPreservingUnaryExecNode`, which is how an aggregate's grouping
columns can keep a child's `HashPartitioning` visible to operators above it.

**Anchor files:**

- [BaseAggregateExec.scala:30](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/aggregate/BaseAggregateExec.scala#L30) — the trait
- [BaseAggregateExec.scala:96](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/aggregate/BaseAggregateExec.scala#L96) — `requiredChildDistribution` and the `numShufflePartitions` pin

**Maps to topics:** B6, A8

---

## Windows

### WindowExec — one partition of one window buffered at a time

**What it is:** `WindowExec` demands `ClusteredDistribution(partitionSpec)` and an ordering of
`partitionSpec ++ orderSpec`, then streams the sorted input, accumulating **all rows of one window
partition** into an `ExternalAppendOnlyUnsafeRowArray` before producing any output for it. The
array holds up to `windowExec.buffer.in.memory.threshold` rows in memory (default 4096), then
switches to an `UnsafeExternalSorter` that spills by row count or by size.

The consequence is stated in the source: with an empty `partitionSpec` the required distribution
becomes `AllTuples` and a warning is logged — every row of the query goes to one task, and that
one task buffers the entire dataset.

Execution runs through `mapPartitionsWithEvaluator` with a `WindowEvaluatorFactory`, the same
factory shape the Python/Arrow window operators reuse.

**Code path:** `WindowExec.doExecute` → `mapPartitionsWithEvaluator(WindowEvaluatorFactory)` →
`fetchNextPartition` → per-frame `prepare` / `write`

**Anchor files:**

- [WindowExecBase.scala:35](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/window/WindowExecBase.scala#L35) — `requiredChildDistribution`, the `AllTuples` warning
- [WindowExec.scala:115](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/window/WindowExec.scala#L115) — `mapPartitionsWithEvaluator`
- [WindowEvaluatorFactory.scala:91](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/window/WindowEvaluatorFactory.scala#L91) — the buffer and its thresholds
- [WindowEvaluatorFactory.scala:105](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/window/WindowEvaluatorFactory.scala#L105) — `fetchNextPartition`
- [ExternalAppendOnlyUnsafeRowArray.scala:48](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/ExternalAppendOnlyUnsafeRowArray.scala#L48) — the in-memory-then-spill array (lives in `execution/`, shared with the join and session paths)

**Configs:** `spark.sql.windowExec.buffer.in.memory.threshold` (`4096`, 2.2.1),
`spark.sql.windowExec.buffer.spill.threshold` (2.2.0),
`spark.sql.windowExec.buffer.spill.size.threshold` (4.1.0)

**Maps to topics:** I2

### The window frame factory — seven factory keys, six frame implementations

**What it is:** window expressions are grouped by a five-part key — `(frame kind, frame type,
lower, upper, offset expr)` — and each group gets a factory producing one `WindowFunctionFrame`.
The seven keys and what they build:

| Key | Frame class | Cost per row |
|---|---|---|
| `FRAME_LESS_OFFSET` | `FrameLessOffsetWindowFunctionFrame` | O(1) — `lag` / `lead` |
| `UNBOUNDED_OFFSET` | `UnboundedOffsetWindowFunctionFrame` | O(1) |
| `UNBOUNDED_PRECEDING_OFFSET` | `UnboundedPrecedingOffsetWindowFunctionFrame` | O(1) |
| `AGGREGATE` + unbounded ↔ unbounded | `UnboundedWindowFunctionFrame` | O(1) — computed once per partition |
| `AGGREGATE` + unbounded preceding | `UnboundedPrecedingWindowFunctionFrame` | O(1) amortised — running total |
| `AGGREGATE` + unbounded following | `UnboundedFollowingWindowFunctionFrame` | O(1) amortised — reverse running total |
| `AGGREGATE` + both bounded | `SlidingWindowFunctionFrame` *or* `SegmentTreeWindowFunctionFrame` | O(W) / O(log W) |

The last row is the one that matters for performance: a genuinely moving frame is the only shape
that re-aggregates, and until 4.2.0 it was the only shape with no better algorithm available.

The `processor` is deliberately a `def`, not a `val` — the three offset branches never read it,
and eagerly constructing an `AggregateProcessor` for `Lag`/`Lead`/`NthValue` throws
`INTERNAL_ERROR: Unsupported aggregate function`.

**Anchor files:**

- [WindowEvaluatorFactoryBase.scala:226](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/window/WindowEvaluatorFactoryBase.scala#L226) — the factory dispatch
- [WindowEvaluatorFactoryBase.scala:211](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/window/WindowEvaluatorFactoryBase.scala#L211) — the lazy-`processor` comment and its reason
- [WindowFunctionFrame.scala:39](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/window/WindowFunctionFrame.scala#L39) — the base class; the six implementations follow at :236, :347, :390, :422, :517, :565, :644

**Maps to topics:** I2

### BoundOrdering — how a RANGE offset is turned into a comparison

**What it is:** `ROWS` and `RANGE` frames differ only in the `BoundOrdering` the factory attaches.
`RowBoundOrdering` compares row *indices* plus an offset — pure arithmetic. `RangeBoundOrdering`
projects the single order-by expression out of each row and compares *values*, with the bound value
computed by a type-specific expression built at planning time: `DateAdd` for dates,
`TimestampAddInterval` / `TimestampAddYMInterval` for timestamps,
`DecimalAddNoOverflowCheck` for decimals, plain `Add` otherwise. A descending order spec flips the
offset's sign with `UnaryMinus`.

This is why `RANGE BETWEEN INTERVAL 7 DAYS PRECEDING AND CURRENT ROW` works but a multi-column
`ORDER BY` with a non-zero RANGE offset does not: the branch requires `orderSpec.size == 1`, and
anything else reaches the internal-error case.

**Anchor files:**

- [WindowEvaluatorFactoryBase.scala:79](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/window/WindowEvaluatorFactoryBase.scala#L79) — `createBoundOrdering`, all type cases
- [BoundOrdering.scala:34](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/window/BoundOrdering.scala#L34) — `RowBoundOrdering`; `RangeBoundOrdering` at :46

**Maps to topics:** I2

### AggregateProcessor and the SizeBasedWindowFunction partition-size injection

**What it is:** the object that actually evaluates aggregate window functions over a frame,
holding one buffer plus an initialize/update/evaluate projection triple — the same
declarative/imperative split as `AggregationIterator`, in miniature.

Its one oddity is `SizeBasedWindowFunction` (`percent_rank`, `cume_dist`, `ntile`): those need the
partition's row count, which is unknown at planning time. The processor appends an extra buffer
slot for it and binds every function's `n` reference to *that* attribute — collected from the
functions themselves rather than reconstructed, because the driver-side and executor-side
expression IDs would otherwise disagree.

**Anchor files:**

- [AggregateProcessor.scala:44](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/window/AggregateProcessor.scala#L44) — `apply`
- [AggregateProcessor.scala:61](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/window/AggregateProcessor.scala#L61) — the SPARK-14244 expression-ID note and the partition-size slot

**Maps to topics:** I2

### Segment-tree window frames — the 4.2.0 sliding-frame algorithm, off by default

**What it is:** new in Spark 4.2.0. For a moving `AGGREGATE` frame, instead of re-aggregating W
rows per output row, the partition is split into blocks of `blockSize` rows (default 65 536); each
block carries its own small segment tree of fanout `fanout` (default 16); block roots stay
resident and internal node arrays are cached in an LRU keyed by block index. A frame query then
costs O(log W) merges.

The engineering around it is the interesting part:

- **Memory.** The LRU is fronted by a private `MemoryConsumer` (`SegTreeSpiller`) registered with
  the task's `TaskMemoryManager`, so cached node arrays are visible to the unified memory manager
  and can be evicted under pressure. Its `spill()` must never call `acquireMemory` (documented
  invariant I1), returns 0 on self-trigger (I2), and returns 0 outright if the underlying row array
  has already spilled to disk (I8) — evicting the cache would then force an O(blockStart) scan of
  the spill file.
- **Cache sizing.** `estimateMaxCachedBlocks` derives the LRU budget from the frame width:
  `ceil(W / blockSize) + 2`, the `+ 2` being one block of slack at each end so the cursor does not
  thrash on block boundaries. Under `RANGE` the width is data-dependent, so it uses a flat budget
  of 8 blocks.
- **Eligibility.** Three independent gates, all in `eligibleForSegTree`: the config is on; the
  frame is `ROWS`, or `RANGE` with exactly one order-by column; **no** aggregate carries a FILTER;
  and every function is on an explicit nine-entry allowlist — `Min`, `Max`, `Sum`, `Count`,
  `Average`, `StddevPop`, `StddevSamp`, `VariancePop`, `VarianceSamp`. The allowlist exists because
  the tree needs `DeclarativeAggregate.mergeExpressions`, and the ranking window functions
  (`Rank`, `DenseRank`, `RowNumber`, `NTile`, `NthValue`) extend `DeclarativeAggregate` but reject
  merging.
- **Runtime fallback.** Below `minPartitionRows` (default 64) rows in the partition, `prepare`
  allocates a `SlidingWindowFunctionFrame` and delegates everything to it. The fallback is
  committed only after the delegate's `prepare` succeeds, so a failure leaves the frame unchanged.
  Two metrics, `numSegmentTreeFrames` and `numSegmentTreeFallbackFrames`, count each path.

**Code path:** `WindowEvaluatorFactoryBase` moving-frame branch → `eligibleForSegTree` →
`SegmentTreeWindowFunctionFrame.prepare` → `WindowSegmentTree` (build blocks, LRU internal nodes)
→ `writeRow` / `writeRange`

**Anchor files:**

- [WindowEvaluatorFactoryBase.scala:359](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/window/WindowEvaluatorFactoryBase.scala#L359) — `eligibleForSegTree`, all four gates
- [WindowEvaluatorFactoryBase.scala:378](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/window/WindowEvaluatorFactoryBase.scala#L378) — `estimateMaxCachedBlocks`
- [SegmentTreeWindowFunctionFrame.scala:120](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/window/SegmentTreeWindowFunctionFrame.scala#L120) — `prepare`, and the fallback-commit ordering
- [SegmentTreeWindowFunctionFrame.scala:235](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/window/SegmentTreeWindowFunctionFrame.scala#L235) — `writeRange`, the two forward-only cursors
- [WindowSegmentTree.scala:207](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/window/WindowSegmentTree.scala#L207) — `SegTreeSpiller` and the spill invariants
- [WindowSegmentTree.scala:594](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/window/WindowSegmentTree.scala#L594) — `EligibleAggregates`, the nine-entry allowlist

**Configs:** `spark.sql.window.segmentTree.enabled` (**`false`**, 4.2.0),
`spark.sql.window.segmentTree.blockSize` (`65536`, 4.2.0),
`spark.sql.window.segmentTree.fanout` (`16`, 4.2.0),
`spark.sql.window.segmentTree.minPartitionRows` (`64`, 4.2.0)

**Maps to topics:** none — proposed as **A34**

### WindowGroupLimitExec — top-N per group, in a partial and a final mode

**What it is:** the physical half of the "top-N per group" rewrite. When a query filters on
`row_number() / rank() / dense_rank() <= k`, catalyst's `InferWindowGroupLimit` inserts a
`WindowGroupLimit` node, and this operator drops rows past rank `k` *before* the window is
evaluated — and, in `Partial` mode, before the shuffle.

The two modes differ in what they require of their child: `Partial` requires nothing (it runs
per-input-partition, on the map side), `Final` requires `ClusteredDistribution(partitionSpec)`.
Three iterators implement the three ranking semantics: `SimpleLimitIterator` for `row_number`
(rank increments every row), `RankLimitIterator` for `rank` (increments by the run length on a key
change), `DenseRankLimitIterator` for `dense_rank` (increments by one on a key change).

`spark.sql.optimizer.windowGroupLimitThreshold` (default 1000) is the ceiling on `k` above which
the rewrite is not applied at all — the saving no longer pays for the extra operator.

**Anchor files:**

- [WindowGroupLimitExec.scala:44](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/window/WindowGroupLimitExec.scala#L44) — the operator; the two modes' distributions at :54
- [WindowGroupLimitExec.scala:98](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/window/WindowGroupLimitExec.scala#L98) — `BaseLimitIterator`; the three subclasses at :125, :147, :175
- [WindowGroupLimitEvaluatorFactory.scala:25](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/window/WindowGroupLimitEvaluatorFactory.scala#L25) — the evaluator factory

**Configs:** `spark.sql.optimizer.windowGroupLimitThreshold` (`1000`, 3.5.0)

**Maps to topics:** I2

---

## Exchange

### ShuffleExchangeExec and the six ShuffleOrigins

**What it is:** the operator that moves rows between partitions. Its `shuffleOrigin` field records
**why** the shuffle exists, and that provenance is what later rules are allowed to act on:

| Origin | Created by | AQE may |
|---|---|---|
| `ENSURE_REQUIREMENTS` | `EnsureRequirements` | coalesce, split for skew, convert to local read |
| `REPARTITION_BY_COL` | `df.repartition(col)` — no partition count given | coalesce |
| `REPARTITION_BY_NUM` | `df.repartition(n)` — user asked for exactly n | not change the count |
| `REBALANCE_PARTITIONS_BY_NONE` | `REBALANCE` with no column | coalesce and split |
| `REBALANCE_PARTITIONS_BY_COL` | `REBALANCE(col)` | coalesce and split, but not local-read |
| `REQUIRED_BY_STATEFUL_OPERATOR` | a `StatefulOpClusteredDistribution` | not touch it |

The `ShuffleExchangeLike` trait is what AQE actually programs against — it exposes
`numPartitions`, `shuffleOrigin`, and the materialization hooks, so an alternative shuffle operator
can participate.

**Anchor files:**

- [ShuffleExchangeExec.scala:49](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/exchange/ShuffleExchangeExec.scala#L49) — `ShuffleExchangeLike`
- [ShuffleExchangeExec.scala:153](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/exchange/ShuffleExchangeExec.scala#L153) — `ShuffleOrigin` and its cases
- [ShuffleExchangeExec.scala:188](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/exchange/ShuffleExchangeExec.scala#L188) — the operator

**Configs:** `spark.sql.shuffle.partitions` (`200`, 1.1.0)

**Maps to topics:** I5, A26

### prepareShuffleDependency — one partitioner per Partitioning, and the RangePartitioner's hidden job

**What it is:** the translation from a SQL `Partitioning` to a Spark-core `Partitioner` plus a
key-extractor function:

- `HashPartitioning` → `HashPartitioner`, but the key is already
  `HashPartitioning.partitionIdExpression`, i.e. the partition id itself; the partitioner is a
  pass-through in effect.
- `RoundRobinPartitioning` → `HashPartitioner` over a per-partition `XORShiftRandom` start
  position, advanced per row. The source notes that `nextInt(bound)` for a power-of-two bound uses
  a different, less-scrambled code path, which is why the start is randomized per input partition.
- `RangePartitioning` → `RangePartitioner`, which **runs a job on the RDD** to sample keys and
  compute bounds. `sortByKey`-like behaviour hidden inside a plan node; the sample size per
  partition is configurable.
- `SinglePartition` → `ConstantPartitioner`.
- `KeyGroupedPartitioning` → `KeyGroupedPartitioner` built from an explicit value → id map.

The resulting `ShuffleDependency` uses `PartitionIdPassthrough`, because the pairs are already
`(partitionId, row)`.

**Anchor files:**

- [ShuffleExchangeExec.scala:339](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/exchange/ShuffleExchangeExec.scala#L339) — `prepareShuffleDependency`
- [ShuffleExchangeExec.scala:356](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/exchange/ShuffleExchangeExec.scala#L356) — the `RangePartitioner` sampling job
- [ShuffleExchangeExec.scala:497](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/exchange/ShuffleExchangeExec.scala#L497) — the `ShuffleDependency` construction

**Configs:** `spark.sql.execution.rangeExchange.sampleSizePerPartition` (`100`, 2.3.0)

**Maps to topics:** I5, A26

### Round-robin determinism — the local sort before repartition

**What it is:** SPARK-23207. `df.repartition(n)` with no columns uses round-robin assignment
seeded per input partition — which means a **retried** task can assign the same rows to different
output partitions, and any downstream stage that already consumed the old output now disagrees with
the new one. That is silent data loss or duplication, not a failure.

Spark's defence is blunt: when the partitioning is round-robin with more than one partition, and
`spark.sql.execution.sortBeforeRepartition` is on (default), each input partition is **locally
sorted by the binary form of the row** — `RecordBinaryComparator` with the row hash as sort prefix
— before assignment, making the assignment a function of the data rather than of arrival order.
Radix sort is explicitly disabled for this sorter (SPARK-28699) because the comparison is binary.

If the config is turned off, the map function is instead flagged `isOrderSensitive = true`, which
propagates into the RDD's determinism level and lets the scheduler roll back consumers instead.

**Anchor files:**

- [ShuffleExchangeExec.scala:432](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/exchange/ShuffleExchangeExec.scala#L432) — the local sort and its comparator
- [ShuffleExchangeExec.scala:471](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/exchange/ShuffleExchangeExec.scala#L471) — `isOrderSensitive` when the sort is off

**Configs:** `spark.sql.execution.sortBeforeRepartition` (`true`, 2.1.4)

**Maps to topics:** I5, A14

### needToCopyObjectsBeforeShuffle — when a row must be copied before it is handed to the writer

**What it is:** Spark SQL reuses one mutable `UnsafeRow` object per operator, so handing rows
straight to the shuffle writer is only safe if the writer consumes each row before the next
`next()` call. Whether that holds depends on the shuffle implementation and the partition count:
the sort-shuffle path buffers records, the bypass-merge path writes them out immediately, and the
serialized path can only be used when the serializer supports relocation and there is no
aggregator or ordering. The function encodes those cases; when the answer is "copy", the map
function calls `row.copy()` per row, which is a real per-row cost.

**Anchor files:**

- [ShuffleExchangeExec.scala:295](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/exchange/ShuffleExchangeExec.scala#L295) — `needToCopyObjectsBeforeShuffle`, with the reasoning per shuffle path
- [ShuffleExchangeExec.scala:472](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/exchange/ShuffleExchangeExec.scala#L472) — the copying vs `MutablePair` branch

**Maps to topics:** I5, E1

### Order-independent row checksums on the SQL shuffle

**What it is:** when order-independent checksums are enabled, the SQL exchange allocates one
`UnsafeRowChecksum` per output partition and attaches them to the `ShuffleDependency`. The checksum
is order-independent by construction, so a re-run of a map task that emits the same *set* of rows
in a different order still matches — which is what makes it usable as a correctness check on
retry rather than only as a corruption check.

Two escalation policies ride on the same dependency: a full retry of the stage on mismatch
(`enableFullRetryOnMismatch`, default true, 4.1.0) and a query-level rollback
(`enableQueryLevelRollbackOnMismatch`, default **false**, new in 4.2.0). The checksum array is
allocated only if one of them is on — otherwise `checksumSize` is 0 and the feature costs nothing.

**Anchor files:**

- [ShuffleExchangeExec.scala:489](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/exchange/ShuffleExchangeExec.scala#L489) — `checksumSize` and the dependency fields it feeds

**Configs:** `spark.sql.shuffle.orderIndependentChecksum.enabled` (`true`, 4.1.0),
`…enableFullRetryOnMismatch` (`true`, 4.1.0),
`…enableQueryLevelRollbackOnMismatch` (`false`, **4.2.0**)

**Maps to topics:** A14

### BroadcastExchangeExec — an async future, two row ceilings, and a job tag

**What it is:** the build side of a broadcast join does not execute inside the join. `doPrepare`
kicks off `relationFuture` on a **separate thread pool**, so the broadcast job overlaps with the
rest of the plan's preparation; `doExecuteBroadcast` then blocks on it with
`spark.sql.broadcastTimeout` (default 300 s).

Inside the future: `executeCollectIterator()` on the driver, a row-count check, `mode.transform`
to build the `HashedRelation`, then `sparkContext.broadcast`. Four timing metrics —
`collectTime`, `buildTime`, `broadcastTime`, plus `dataSize` — are all visible in the SQL tab, and
they are the fastest way to tell a slow *build* from a slow *broadcast*.

The row ceiling is not one number. For a `HashedRelation` on a non-single-long key it is
`BytesToBytesMap.MAX_CAPACITY / 1.5` ≈ **341 million** rows (the map's 1 << 29 capacity, derated
for the 70 % load factor). For everything else it is **512 000 000**. Exceeding it raises a
specific error rather than an OOM.

The future registers a job tag and `setInterruptOnCancel(true)`, so cancelling the query cancels
the broadcast job; `cancelBroadcastJob` cancels by tag.

**Anchor files:**

- [BroadcastExchangeExec.scala:46](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/exchange/BroadcastExchangeExec.scala#L46) — `BroadcastExchangeLike` and its promise/future contract
- [BroadcastExchangeExec.scala:164](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/exchange/BroadcastExchangeExec.scala#L164) — `maxBroadcastRows`, both ceilings
- [BroadcastExchangeExec.scala:177](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/exchange/BroadcastExchangeExec.scala#L177) — `relationFuture`, the whole build
- [BroadcastExchangeExec.scala:248](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/exchange/BroadcastExchangeExec.scala#L248) — `doPrepare`, where the job actually starts

**Configs:** `spark.sql.broadcastTimeout` (`300`, 1.3.0),
`spark.sql.autoBroadcastJoinThreshold` (`10MB`, 1.1.0),
`spark.sql.adaptive.autoBroadcastJoinThreshold` (unset → falls back, 3.2.0)

**Maps to topics:** B7, A3

### EnsureRequirements — satisfaction, best shuffle spec, and the minimum-parallelism rule

**What it is:** the rule that inserts exchanges. For each operator it walks children against
`requiredChildDistribution`; a child whose `outputPartitioning` already `satisfies` the
distribution is left alone, otherwise it gets a `BroadcastExchangeExec` or a `ShuffleExchangeExec`
with the right origin.

The harder half runs when an operator has **two** children that must be co-partitioned. Then:

1. Each child produces a `ShuffleSpec` from its partitioning and required distribution.
2. Candidate specs are filtered to those that can create a partitioning for the others; if *every*
   child would have to be re-shuffled anyway, the candidates are further restricted to those with
   at least `spark.sql.shuffle.partitions` partitions — this is the "minimum parallelism" rule,
   and it applies **only** in that all-shuffled case.
3. The candidate with the **largest partition count** wins. That is the entire cost model — the
   source itself notes it is not optimal for three or more children.
4. Children already compatible with the winner keep their plan; the rest get an exchange built
   from the winner's partitioning.

A documented special case short-circuits all of it: if every co-partitioned child is already
`SinglePartition` and its logical size is under `spark.sql.maxSinglePartitionBytes` (128 MB), no
shuffle is added at all.

**Anchor files:**

- [EnsureRequirements.scala:56](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/exchange/EnsureRequirements.scala#L56) — `ensureDistributionAndOrdering`
- [EnsureRequirements.scala:148](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/exchange/EnsureRequirements.scala#L148) — the single-partition short-circuit
- [EnsureRequirements.scala:191](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/exchange/EnsureRequirements.scala#L191) — `shouldConsiderMinParallelism`, with the worked 5-vs-6 example in the comment
- [EnsureRequirements.scala:217](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/exchange/EnsureRequirements.scala#L217) — `maxBy(_.numPartitions)`, the whole cost model
- [EnsureRequirements.scala:890](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/exchange/EnsureRequirements.scala#L890) — `apply`

**Configs:** `spark.sql.shuffle.partitions` (`200`), `spark.sql.maxSinglePartitionBytes` (`128m`,
3.4.0), `spark.sql.requireAllClusterKeysForDistribution` (`false`, 3.3.0),
`spark.sql.requireAllClusterKeysForCoPartition` (`true`, 3.3.0)

**Maps to topics:** A26, I5

### reorderJoinKeys and reorderJoinPredicates — matching an existing partitioning instead of reshuffling

**What it is:** a join's keys are written in the user's order, but a child may already be
partitioned or ordered on the *same set* of expressions in a different order —
`HashPartitioning(b, a)` under a join on `(a, b)`. Rather than shuffle, `reorderJoinKeys` permutes
the join's key lists to line up with the existing partitioning, recursively descending through
`PartitioningCollection`. `reorderJoinPredicates` applies it to `SortMergeJoinExec` and the two
hash joins.

This is why a plan can show a join whose key order does not match the SQL text.

**Anchor files:**

- [EnsureRequirements.scala:332](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/exchange/EnsureRequirements.scala#L332) — `reorder`
- [EnsureRequirements.scala:376](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/exchange/EnsureRequirements.scala#L376) — `reorderJoinKeys`; the recursive form at :397
- [EnsureRequirements.scala:441](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/exchange/EnsureRequirements.scala#L441) — `reorderJoinPredicates`

**Maps to topics:** A26, A3

### Storage-partitioned join compatibility — checkKeyGroupCompatible, reducers, GroupPartitionsExec

**What it is:** the code that makes a storage-partitioned join actually avoid its shuffle. Spark
4.2.0 expresses V2 source partitioning as `KeyedPartitioning`, which has two states — *grouped*
(one Spark partition per distinct key) and *non-grouped* — and `EnsureRequirements` chooses
between "already satisfies", "satisfies once grouped" (insert a `GroupPartitionsExec`), and "must
shuffle".

For a join, `checkKeyGroupCompatible` builds a keyed shuffle spec per side and then:

- if the two specs' *keys* are compatible but the values differ, and push-down is enabled, it
  computes the **union** of both sides' partition values and pushes it to both sources, so each
  fills the gaps with empty partitions — no shuffle, at the cost of empty tasks;
- if the partition *expressions* are compatible but not identical (`days(a)` vs `hours(b)`), it
  applies **reducers** to group one side's keys up to the coarser transform, and raises
  `storagePartitionJoinIncompatibleReducedTypes` if the two sides reduce to different types;
- `checkShufflePartitionIdPassThroughCompatible` handles the case where one side's partitioning is
  already a plain pass-through of partition ids.

`GroupPartitionsExec` itself lives in `execution/datasources/v2/` (the `datasources` group's
scope), but every decision to insert one is made here.

**Anchor files:**

- [EnsureRequirements.scala:68](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/exchange/EnsureRequirements.scala#L68) — `splitKeyedPartitionings`, the three-way categorisation (definition at :863)
- [EnsureRequirements.scala:481](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/exchange/EnsureRequirements.scala#L481) — `checkKeyGroupCompatible`, the common-value push-down
- [EnsureRequirements.scala:694](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/exchange/EnsureRequirements.scala#L694) — `checkShufflePartitionIdPassThroughCompatible`
- [EnsureRequirements.scala:737](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/exchange/EnsureRequirements.scala#L737) — `applyGroupPartitions`

**Configs:** the `spark.sql.sources.v2.bucketing.*` family — `enabled`,
`pushPartValues.enabled`, `allowJoinKeysSubsetOfPartitionKeys.enabled`,
`partiallyClusteredDistribution.enabled`, `allowCompatibleTransforms.enabled`,
`partitionKeyOrdering.enabled`, `preserveKeyOrderingOnCoalesce.enabled`,
`preserveOrderingOnCoalesce.enabled`, `partition.filter.enabled`, `shuffle.enabled`,
`sorting.enabled`

**Maps to topics:** A25

### ValidateRequirements — the check that silently reverts an AQE rule

**What it is:** a read-only verifier: for every node, does each child's `outputPartitioning`
satisfy the required distribution, does each child's `outputOrdering` satisfy the required
ordering, and — for multi-child nodes with clustered distributions — are the children mutually
co-partitioned? It logs at DEBUG and returns a boolean.

Its importance is that AQE calls it after applying a shuffle-read rule. A rule whose result fails
validation is **discarded without any message above DEBUG level**, which is one of the standard
reasons an expected AQE optimisation does not appear in the final plan.

**Anchor files:**

- [ValidateRequirements.scala:31](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/exchange/ValidateRequirements.scala#L31) — the whole object

**Maps to topics:** A2, A26

### ReusedExchangeExec — one exchange, many consumers

**What it is:** a wrapper that lets two structurally identical exchanges become one. It is needed
at all because the two exchanges produce *different attribute ids* for the same data, so the
wrapper carries the consumer's expected `output` and rewrites the underlying exchange's
`outputPartitioning` and `outputOrdering` through that mapping. `doCanonicalize` returns the
child's canonical form, so reuse is transitive.

In `EXPLAIN` it renders as `ReusedExchange [Reuses operator id: N]` — the only visible sign that a
subtree ran once rather than twice. Dynamic partition pruning depends on this: `PlanDynamicPruningFilters`
only reuses a broadcast when `spark.sql.exchange.reuse` is on.

**Anchor files:**

- [Exchange.scala:36](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/exchange/Exchange.scala#L36) — the `Exchange` base, with its `plan_id` in `stringArgs`
- [Exchange.scala:48](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/exchange/Exchange.scala#L48) — `ReusedExchangeExec` and the attribute remapping at :77

**Configs:** `spark.sql.exchange.reuse` (`true`, 2.0.0)

**Maps to topics:** A26, A2

---

## Bucketing

### V1 bucketing at execution time — the two rules that coalesce or disable a bucketed scan

**What it is:** two physical rules, both operating on already-planned bucketed `FileSourceScanExec`
nodes.

**`CoalesceBucketsInJoin`** handles the case where both sides of a `SortMergeJoin` or
`ShuffledHashJoin` are bucketed but with *different* bucket counts. If the larger count is
divisible by the smaller and their ratio is at most
`spark.sql.bucketing.coalesceBucketsInJoin.maxBucketRatio` (default 4), the larger side's scan is
rewritten with `optionalNumCoalescedBuckets`, merging its buckets down to match — so the join needs
no exchange. The divisibility requirement is not a heuristic: bucket id is `hash % numBuckets`, so
only a divisor produces a correct merge. `ExtractJoinWithBuckets` is the extractor that checks all
of it, including that each side is only scans, filters, projects and broadcast joins — anything
else and the partitioning cannot be trusted. For a shuffled hash join it additionally refuses to
coalesce the *build* side (`isCoalesceSHJStreamSide`), because coalescing there makes the build
larger. The whole rule is **off by default**.

**`DisableUnnecessaryBucketedScan`** goes the other way. Bucketed scanning costs one file-per-bucket
task layout, which is wasteful when nothing above the scan cares about the partitioning. The rule
walks down from each operator that *does* care — one with a non-trivial `requiredChildDistribution`
or `requiredChildOrdering`, an "interesting partition" by analogy with the classic *interesting
order* — and disables bucketed scanning everywhere no such operator is above. It bails out entirely
unless both `spark.sql.sources.bucketing.enabled` and
`spark.sql.sources.bucketing.autoBucketedScan.enabled` are on and the plan actually has a bucketed
scan.

**Anchor files:**

- [CoalesceBucketsInJoin.scala:40](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/bucketing/CoalesceBucketsInJoin.scala#L40) — the rule
- [CoalesceBucketsInJoin.scala:71](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/bucketing/CoalesceBucketsInJoin.scala#L71) — `isCoalesceSHJStreamSide`
- [CoalesceBucketsInJoin.scala:113](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/bucketing/CoalesceBucketsInJoin.scala#L113) — `ExtractJoinWithBuckets`; `isDivisible` at :157
- [DisableUnnecessaryBucketedScan.scala:77](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/bucketing/DisableUnnecessaryBucketedScan.scala#L77) — the rule and its scaladoc worked examples
- [DisableUnnecessaryBucketedScan.scala:121](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/bucketing/DisableUnnecessaryBucketedScan.scala#L121) — `hasInterestingPartition`

**Configs:** `spark.sql.sources.bucketing.enabled` (`true`, 2.0.0),
`spark.sql.sources.bucketing.autoBucketedScan.enabled` (`true`, 3.1.0),
`spark.sql.sources.bucketing.maxBuckets` (`100000`, 2.4.0),
`spark.sql.bucketing.coalesceBucketsInJoin.enabled` (**`false`**, 3.1.0),
`spark.sql.bucketing.coalesceBucketsInJoin.maxBucketRatio` (`4`, 3.1.0),
`spark.sql.legacy.bucketedTableScan.outputOrdering` (`false`, 3.0.0)

**Maps to topics:** none — proposed as **I29**

---

## Dynamic pruning

### PartitionPruning — where a DPP subquery is inserted, and the cost model that gates it

**What it is:** the logical rule that turns `fact JOIN dim ON fact.d = dim.d WHERE dim.x = 'y'`
into a partition filter on `fact`. It requires: an equi-join, a *filterable* scan on the pruning
side (a partitioned `HadoopFsRelation`, a Hive table, or a V2 relation reached through
deterministic projects and filters), a **selective predicate** on the filtering side, and a join
type that permits pruning that side.

The gate is `pruningHasBenefit`. It estimates a filter ratio from CBO distinct counts on both join
columns — `1 - otherDistinct / partDistinct` — and falls back to
`dynamicPartitionPruning.fallbackFilterRatio` (0.5) whenever stats are missing, or whenever the
pruning side's distinct count is *not larger* than the other side's, which it treats as an
estimation error. It then compares `filterRatio × partPlanSize` against the cost of running the
filtering side.

The crucial asymmetry: if `spark.sql.exchange.reuse` is on, the subquery is inserted **regardless**
of the benefit check, because a reused broadcast is nearly free — but the `onlyInBroadcast` flag is
set to true whenever there is no proven benefit, so it will be dropped later if the broadcast turns
out not to be reusable.

**Anchor files:**

- [PartitionPruning.scala:59](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/dynamicpruning/PartitionPruning.scala#L59) — `getFilterableTableScan`
- [PartitionPruning.scala:101](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/dynamicpruning/PartitionPruning.scala#L101) — `insertPredicate`, the reuse-or-benefit gate
- [PartitionPruning.scala:137](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/dynamicpruning/PartitionPruning.scala#L137) — `pruningHasBenefit`, the whole cost model
- [PartitionPruning.scala:204](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/dynamicpruning/PartitionPruning.scala#L204) — `hasSelectivePredicate`

**Configs:** `spark.sql.optimizer.dynamicPartitionPruning.enabled` (`true`, 3.0.0),
`…useStats` (`true`), `…fallbackFilterRatio` (`0.5`), `…reuseBroadcastOnly` (`true`)

**Maps to topics:** A18

### PlanDynamicPruningFilters — reuse the broadcast, duplicate the subquery, or give up

**What it is:** the physical rule that decides what a `DynamicPruningSubquery` actually becomes.
Three outcomes:

1. **Reuse.** If exchange reuse is on and the planned build side is `sameResult` as one side of an
   existing `BroadcastHashJoinExec`, it wraps a `BroadcastExchangeExec` in a
   `SubqueryBroadcastExec` and extracts the pruning keys from the *already-built* hash relation.
   This is DPP at its cheapest — no extra job.
2. **Give up.** If reuse is impossible and `onlyInBroadcast` was set, the filter is replaced with
   `TrueLiteral` — the pruning silently does not happen. This is what
   `dynamicPartitionPruning.reuseBroadcastOnly = true` (the default) makes the common case.
3. **Duplicate.** Otherwise it plans a fresh `Aggregate` over the distinct build keys and runs it
   as an ordinary `SubqueryExec` — a second scan of the dimension table, paid for by the pruning.

The source flags its own fragility: it uses `QueryExecution.createSparkPlan` directly on the
assumption that this rule runs first, right after `InsertAdaptiveSparkPlan`.

**Anchor files:**

- [PlanDynamicPruningFilters.scala:49](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/dynamicpruning/PlanDynamicPruningFilters.scala#L49) — `apply`, all three branches
- [PlanDynamicPruningFilters.scala:44](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/dynamicpruning/PlanDynamicPruningFilters.scala#L44) — `broadcastMode`, which must match the join's own key packing

**Maps to topics:** A18, A2

### CleanupDynamicPruningFilters — removing the filters that never reached a scan

**What it is:** a DPP filter is only useful if it was pushed all the way into a scan. Filters that
got stuck — because a non-deterministic project or aggregate blocked the push — are replaced with
`TrueLiteral`. The rule also removes a DPP subquery whose pruning key already has an ordinary
equality predicate against a foldable value on the same filter: the partition is already pinned,
so the dynamic filter cannot prune anything further.

**Anchor files:**

- [CleanupDynamicPruningFilters.scala:50](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/dynamicpruning/CleanupDynamicPruningFilters.scala#L50) — `removeUnnecessaryDynamicPruningSubquery`
- [CleanupDynamicPruningFilters.scala:66](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/dynamicpruning/CleanupDynamicPruningFilters.scala#L66) — `apply`, the three pushed-down shapes and the catch-all

**Maps to topics:** A18

### RowLevelOperationRuntimeGroupFiltering — a runtime filter for MERGE, UPDATE and DELETE

**What it is:** the same idea as DPP, applied to DSv2 row-level operations. Planning-time data
skipping for a `MERGE`/`UPDATE`/`DELETE` is limited to conditions that convert to data-source
filters; anything else means reading every group. When the operation's primary scan implements
`SupportsRuntimeV2Filtering`, this rule injects a subquery that evaluates the condition against the
*original* table — projecting only the columns the condition needs — and pushes the resulting
matching-group set into the main scan at runtime.

The economics are stated in the scaladoc: the runtime query is cheap because it projects few
columns, whereas the main scan must project everything, so discarding unaffected groups up front
usually dominates.

**Anchor files:**

- [RowLevelOperationRuntimeGroupFiltering.scala:47](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/dynamicpruning/RowLevelOperationRuntimeGroupFiltering.scala#L47) — the rule and its scaladoc
- [RowLevelOperationRuntimeGroupFiltering.scala:52](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/dynamicpruning/RowLevelOperationRuntimeGroupFiltering.scala#L52) — the group-based and delta-based cases
- [RowLevelOperationRuntimeGroupFiltering.scala:70](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/dynamicpruning/RowLevelOperationRuntimeGroupFiltering.scala#L70) — `injectGroupFilters`

**Configs:** `spark.sql.optimizer.runtime.rowLevelOperationGroupFilter.enabled` (`true`, 3.4.0)

**Maps to topics:** A6, E23

---

## Breadth checks

### Config breadth

The slice was taken from `configs/catalog.yaml` over subsystems `sql/catalyst` + `sql/core`
(nearly every `spark.sql.*` key is declared in catalyst's `SQLConf.scala`, so restricting to
`sql/core` would return almost nothing), with this pattern:

```
\.aggregate|\.window|\.exchange|\.bucketing|\.dynamicPartitionPruning|\.shuffle\.|shufflePartitions
|objectHashAggregate|sortAggregate|codegen\.aggregate|adaptive\.coalesce|\.execution\.reuse
|reuseExchange|planChangeLog|\.sort\.|radixSort|windowExec|\.bucket
```

plus a second pass for keys the pattern misses that this group's code demonstrably reads:
`useObjectHashAggregateExec`, `sessionWindow`, `maxSinglePartitionBytes`, `rangeExchange`,
`sortBeforeRepartition`, `broadcastTimeout`, `requireAllClusterKeys`,
`rowLevelOperationGroupFilter`, `autoBroadcastJoinThreshold`. **66 keys.**

| Family | Keys | Tied to |
|---|---|---|
| `codegen.aggregate.*` | 6 | fast hash map, `splitAggregateExpressions` |
| `objectHashAggregate.*`, `execution.useObjectHashAggregateExec` | 2 | ObjectHashAggregateExec |
| `windowExec.buffer.*` | 3 | WindowExec buffering |
| `window.segmentTree.*` | 4 | segment-tree frames |
| `sessionWindow.*`, `streaming.sessionWindow.*` | 5 | session windows |
| `optimizer.windowGroupLimitThreshold` | 1 | WindowGroupLimitExec |
| `shuffle.partitions`, `maxSinglePartitionBytes`, `requireAllClusterKeys*` | 4 | EnsureRequirements |
| `shuffle.orderIndependentChecksum.*` | 3 | prepareShuffleDependency |
| `execution.sortBeforeRepartition`, `execution.rangeExchange.*` | 2 | shuffle determinism, RangePartitioner |
| `exchange.reuse` | 1 | ReusedExchangeExec, DPP |
| `broadcastTimeout`, `autoBroadcastJoinThreshold`, `adaptive.autoBroadcastJoinThreshold` | 3 | BroadcastExchangeExec |
| `sources.v2.bucketing.*` | 11 | SPJ compatibility (some also read in `datasources/v2`) |
| `sources.bucketing.*`, `bucketing.coalesceBucketsInJoin.*`, `legacy.bucketedTableScan.outputOrdering` | 6 | the two bucketing rules |
| `optimizer.dynamicPartitionPruning.*`, `optimizer.runtime.rowLevelOperationGroupFilter.enabled` | 5 | dynamic pruning |

**Out of scope, and where they belong** (kept in the slice deliberately, per the err-wide rule):

| Family | Keys | Owning group |
|---|---|---|
| `adaptive.coalescePartitions.*`, `adaptive.shuffle.targetPostShuffleInputSize` | 6 | `adaptive` |
| `planChangeLog.*` | 3 | `query-execution` |
| `sort.enableRadixSort`, `execution.reuseSubquery` | 2 | `query-execution` |
| `parquet.aggregatePushdown`, `orc.aggregatePushdown` | 2 | `datasources` |
| `join.preferSortMergeJoin`, `adaptive.enabled` | 2 | `joins-exec`, `adaptive` |

Every in-scope key ties to a concept above.

### Package breadth

Walked by hand (`ls` per directory, compared against citations). No sub-packages exist under any of
the five — each is a flat directory of `.scala` files.

| Package | Files | Cited |
|---|---|---|
| `execution/aggregate/` | 20 | 20 |
| `execution/window/` | 11 | 11 |
| `execution/exchange/` | 5 | 5 |
| `execution/bucketing/` | 2 | 2 |
| `execution/dynamicpruning/` | 4 | 4 |

**43 of 43.**

### Deliberately not covered

- **`GroupPartitionsExec`** itself (`execution/datasources/v2/`) — the insertion decision is mapped
  here, the operator's own mechanics belong to the `datasources` group.
- **`ExternalAppendOnlyUnsafeRowArray`** (`execution/`) — cited as the window and session buffer,
  but its spill mechanics are the `query-execution` group's.
- **Catalyst-side halves**: `RewriteDistinctAggregates`, `InferWindowGroupLimit`,
  `Distribution`/`Partitioning`/`ShuffleSpec` themselves, `SQLConf`. Covered by the catalyst sweeps.
- **AQE's rewrites of these exchanges** — `adaptive` group's page.

---

## Refresh log

| Date | Spark | What changed |
|---|---|---|
| 2026-08-06 | 4.2.0 | Initial sweep of the group: 5 packages, 43/43 files cited, 34 concepts, 3 new topics proposed (A33 two-level hash aggregation, A34 segment-tree window frames, I29 V1 bucketing). Both breadth checks run — config breadth found no untied in-scope key; package breadth walked by hand. Headline 4.2.0 findings: `WindowSegmentTree` (a whole new sliding-frame algorithm, off by default) and `spark.sql.shuffle.orderIndependentChecksum.enableQueryLevelRollbackOnMismatch` |
