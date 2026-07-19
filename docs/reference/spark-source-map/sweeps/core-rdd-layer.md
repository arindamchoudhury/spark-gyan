---
subsystem: core
spark_version: "4.2.0"
swept_at: 2026-07-19
group: rdd-layer
all_groups: [rdd-layer, execution-engine, shuffle-memory, storage-serializer,
  submit-standalone, monitoring, config-security, rpc-resources, api-bridge]
status: complete
concepts:
  - name: rdd-model
    topics: [I4, B1]
  - name: transformations-actions
    topics: [I4]
  - name: partitioning
    topics: [I4, I5, A4]
  - name: persistence
    topics: [I6]
  - name: checkpointing
    topics: [I4, I6]
  - name: broadcast
    topics: [I4, E1]
  - name: context-cleaner
    topics: [E1]
  - name: pair-rdd-functions
    topics: [I4, A4]
    propose:
      code: I13
      level: Intermediate
      title: "Pair RDD Aggregations: combineByKey, reduceByKey, groupByKey"
      what: "PairRDDFunctions adds key-value operations to RDD[(K,V)] via implicit conversion; all aggregations bottom out in combineByKeyWithClassTag, which either applies in-place or routes through ShuffledRDD."
      why: "The cost difference between reduceByKey (map-side combine) and groupByKey (no combine) is the canonical RDD-level skew and OOM lesson; understanding combineByKey explains every higher-level shuffle."
  - name: closure-cleaning
    topics: [I4, E1]
    propose:
      code: I14
      level: Intermediate
      title: "Closure Cleaning and the Task-Not-Serializable Problem"
      what: "SparkContext.clean() delegates to ClosureCleaner (ASM 9 bytecode analysis) to null out unreferenced outer-object fields in Scala closures before they are serialized to executors."
      why: "Every transformation lambda passes through closure cleaning; failures here produce the ubiquitous Task not serializable error, and understanding the mechanism is required to reason about what driver-side state leaks into tasks."
  - name: approximate-actions
    topics: []
    propose:
      code: I16
      level: Intermediate
      title: "Approximate Actions and Partial Results"
      what: "countApprox, sumApprox, meanApprox and countByValueApprox submit an ordinary job but hand each task result to an incremental evaluator, returning a BoundedDouble confidence interval after a wall-clock timeout via ApproximateActionListener."
      why: "The API reads as \"get a cheap answer fast\" and is not: the timeout bounds only how long the driver blocks, it never cancels the job, so the cluster cost equals a full count. The interval extrapolates from the fraction of partitions completed, so it is biased on skewed data, and PySpark calls the blocking getFinalValue() which defeats the timeout entirely."
  - name: whole-file-sources
    topics: []
    propose:
      code: I17
      level: Intermediate
      title: "Whole-File and Binary RDD Sources"
      what: "SparkContext.binaryFiles, wholeTextFiles and binaryRecords read whole files or fixed-length records through CombineFileInputFormat, governed by the spark.files.* config family rather than the spark.sql.files.* one used by DataFrame reads."
      why: "Whole-file reads are the standard on-ramp for images, PDFs and scientific binary formats, and the two most common failures follow directly from isSplitable = false: one task per giant file, and an OOM inside PortableDataStream.toArray(). minPartitions also means opposite things in binaryFiles (a floor under defaultParallelism) and wholeTextFiles (a direct divisor)."
  - name: accumulator-v2
    topics: [E1]
    propose:
      code: E10
      level: Expert
      title: "AccumulatorV2: Distributed Side-Effect Counters"
      what: "AccumulatorV2[IN,OUT] is the abstract base for user-defined accumulators registered with SparkContext; each task receives a copy(), calls add() locally, and the driver merges all copies back via merge() at task completion."
      why: "Accumulators are the only executor-to-driver side-channel in Spark; understanding the copy-merge lifecycle and countFailedValues prevents double-counting bugs on speculative execution and task retries."
  - name: async-rdd-actions
    topics: [I4]
    propose:
      code: I15
      level: Intermediate
      title: "AsyncRDDActions: Non-Blocking Job Submission"
      what: "AsyncRDDActions wraps countAsync, collectAsync, takeAsync, foreachAsync, and foreachPartitionAsync, each returning a FutureAction backed by SparkContext.submitJob rather than runJob."
      why: "Relevant for workloads that interleave Spark jobs with I/O; takeAsync implements a recursive-future scan with configurable scale-up, making its partition-scan behavior non-obvious."
  - name: serialization
    topics: [E1]
    propose:
      code: E11
      level: Expert
      title: "Serialization: KryoSerializer vs JavaSerializer"
      what: "KryoSerializer uses the Kryo library with a KryoPool, unsafe I/O, and optional class registration; JavaSerializer (default) uses Java object streams with periodic reset to bound stream-table memory."
      why: "Serializer choice determines shuffle and broadcast throughput; Kryo requires explicit class registration for production determinism, and misconfiguration produces cryptic NotSerializableException or data-corruption failures."
---

## RDD model

**What it is:** `RDD[T]` (abstract class, line 84) is the fundamental Spark abstraction: an immutable, partitioned collection described by five properties — `compute(split, context): Iterator[T]`, `getPartitions: Array[Partition]`, `getDependencies: Seq[Dependency[_]]`, `getPreferredLocations(split): Seq[String]`, and the optional `partitioner: Option[Partitioner]`. The class takes a `SparkContext` and initial `Seq[Dependency[_]]` as `@transient` constructor parameters, is `Serializable`, and assigns a unique `id` from `sc.newRddId()` at construction. The `Dependency` hierarchy in `Dependency.scala` encodes lineage: `NarrowDependency` (pipeline; `OneToOneDependency` at line 266, `RangeDependency` at line 280) and `ShuffleDependency` (wide; line 84; registers with `ShuffleManager` at construction line 137 and with `ContextCleaner` at line 256).

**Code path:** `RDD.iterator(split, context)` (line 334) is the per-partition execution entry called by every task. It branches on `storageLevel`: if non-NONE → `getOrCompute(split, context)` (line 381) → `blockManager.getOrElseUpdateRDDBlock`; on cache miss → `computeOrReadCheckpoint`. If NONE → `computeOrReadCheckpoint(split, context)` (line 369) directly → either reads from `CheckpointRDD` or calls `compute`. Canonical narrow implementation: `MapPartitionsRDD.compute` (line 56): `f(context, split.index, firstParent[T].iterator(split, context))`.

**Anchor files:**

- [RDD.scala:84](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/rdd/RDD.scala#L84) — abstract class; 5-property contract in scaladoc at L69–76
- [RDD.scala:334](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/rdd/RDD.scala#L334) — `iterator()`: cache/checkpoint branch
- [RDD.scala:381](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/rdd/RDD.scala#L381) — `getOrCompute()`: block manager lookup
- [MapPartitionsRDD.scala:56](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/rdd/MapPartitionsRDD.scala#L56) — canonical narrow subclass `compute`
- [Dependency.scala:84](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/Dependency.scala#L84) — `ShuffleDependency`: wide dependency with push-shuffle state
- [Partition.scala:23](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/Partition.scala#L23) — `Partition` trait: `index: Int` only

**Configs:** `spark.rdd.compress`, `spark.rdd.parallelListingThreshold`, `spark.rdd.limit.initialNumPartitions`, `spark.rdd.limit.scaleUpFactor`, `spark.rdd.cache.visibilityTracking.enabled`

**Maps to topics:** I4 (RDD Fundamentals), B1 (Architecture)

---

## Transformations and actions

**What it is:** Transformations return a new RDD and are lazy; actions trigger a job via `SparkContext.runJob`. Every transformation accepting a closure calls `sc.clean(f)` first (e.g. `map` at line 425: `val cleanF = sc.clean(f)`).

**Code path:** `count()` (line 1320): `sc.runJob(this, Utils.getIteratorSize _).sum` — the thinnest action wrapper. `collect()` (line 1072): `sc.runJob` → concat partition arrays on driver. `take(num)` (line 1489): incremental scan, starts at `spark.rdd.limit.initialNumPartitions` partitions and scales by `spark.rdd.limit.scaleUpFactor` until `num` rows collected.

**Anchor files:**

- [RDD.scala:424](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/rdd/RDD.scala#L424) — `map`: `sc.clean` + `MapPartitionsRDD`
- [RDD.scala:1072](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/rdd/RDD.scala#L1072) — `collect()`
- [RDD.scala:1320](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/rdd/RDD.scala#L1320) — `count()`
- [RDD.scala:1489](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/rdd/RDD.scala#L1489) — `take()`: incremental partition scan

**Configs:** `spark.rdd.limit.initialNumPartitions`, `spark.rdd.limit.scaleUpFactor`

**Maps to topics:** I4 (RDD Fundamentals)

---

## Partitioning

**What it is:** `Partitioner` (abstract, `Partitioner.scala:42`) maps a key to a partition index via `numPartitions` and `getPartition(key)`. `HashPartitioner` (line 114): `null` → partition 0; others → `Utils.nonNegativeMod(key.hashCode, numPartitions)`. `RangePartitioner` (line 176): samples input via reservoir sampling (`sketch`, line 335), builds sorted `rangeBounds` (`determineBounds`, line 358), then uses linear scan (≤128 bounds) or binary search. `Partitioner.defaultPartitioner` (line 67) prefers an existing large-enough upstream partitioner over creating a new `HashPartitioner`.

**Anchor files:**

- [Partitioner.scala:42](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/Partitioner.scala#L42) — abstract class
- [Partitioner.scala:67](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/Partitioner.scala#L67) — `defaultPartitioner` selection
- [Partitioner.scala:114](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/Partitioner.scala#L114) — `HashPartitioner`
- [Partitioner.scala:176](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/Partitioner.scala#L176) — `RangePartitioner` with sampling constructor
- [ShuffledRDD.scala:41](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/rdd/ShuffledRDD.scala#L41) — wide-dependency output RDD
- [CoalescedRDD.scala:75](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/rdd/CoalescedRDD.scala#L75) — `coalesce` narrow path

**Configs:** `spark.rdd.parallelListingThreshold`, `spark.default.parallelism`

**Maps to topics:** I4 (RDD Fundamentals), I5 (Partitioning), A4 (Data Skew and Shuffle Optimisation)

---

## Persistence

**What it is:** `RDD.persist(newLevel)` (line 186) marks an RDD for caching. On first call the private overload (line 166) registers with `ContextCleaner` and `sc.persistRDD`. `cache()` (line 205) aliases `MEMORY_ONLY`. At execution time, `getOrCompute` (line 381) calls `blockManager.getOrElseUpdateRDDBlock(taskAttemptId, RDDBlockId(id, partition.index), storageLevel, elementClassTag, () => computeOrReadCheckpoint(...))` — hit → `Left(BlockResult)`; miss → computes and stores.

**Anchor files:**

- [RDD.scala:166](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/rdd/RDD.scala#L166) — private `persist`: cleaner + `persistRDD` registration
- [RDD.scala:205](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/rdd/RDD.scala#L205) — `cache()` alias
- [RDD.scala:381](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/rdd/RDD.scala#L381) — `getOrCompute()`: runtime cache read

**Configs:** `spark.rdd.compress`, `spark.rdd.cache.visibilityTracking.enabled`, `spark.storage.maxReplicationFailures`, `spark.storage.memoryMapThreshold`, `spark.storage.replication.proactive`, `spark.storage.unrollMemoryGrowthFactor`, `spark.storage.unrollMemoryThreshold`

**Maps to topics:** I6 (Caching and Persistence)

---

## Checkpointing

**What it is:** Two modes. **Reliable:** `RDD.checkpoint()` (line 1686) sets `checkpointData = Some(new ReliableRDDCheckpointData(this))`; after the first action, `doCheckpoint()` → `ReliableCheckpointRDD.writeRDDToCheckpointDirectory` writes to DFS. **Local:** `RDD.localCheckpoint()` (line 1722) uses executor block storage via `LocalRDDCheckpointData` and forces a `persist()`. In both cases, `markCheckpointed()` (line 1981) nulls `dependencies_`, `partitions_`, `deps` and replaces lineage with a single `OneToOneDependency` on the `CheckpointRDD`.

**Anchor files:**

- [RDD.scala:1686](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/rdd/RDD.scala#L1686) — `checkpoint()`
- [RDD.scala:1722](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/rdd/RDD.scala#L1722) — `localCheckpoint()`
- [RDD.scala:1981](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/rdd/RDD.scala#L1981) — `markCheckpointed()`: lineage truncation
- [RDDCheckpointData.scala:40](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/rdd/RDDCheckpointData.scala#L40) — state machine: Initialized → CheckpointingInProgress → Checkpointed
- [ReliableRDDCheckpointData.scala:34](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/rdd/ReliableRDDCheckpointData.scala#L34) — DFS write path

**Configs:** `spark.checkpoint.compress`, `spark.checkpoint.dir`, `spark.cleaner.referenceTracking.cleanCheckpoints`, `spark.rdd.checkpoint.cachePreferredLocsExpireTime`

**Maps to topics:** I4 (RDD Fundamentals), I6 (Caching and Persistence)

---

## Broadcast

**What it is:** `TorrentBroadcast[T]` (`TorrentBroadcast.scala:60`) is the sole `Broadcast` implementation. `writeBlocks` (line 139) serializes the value, splits into `spark.broadcast.blockSize` chunks (default 4 MB), optionally checksums, stores each as `BroadcastBlockId(id, "piece"+i)` in the driver's `BlockManager`. On executor read, `readBlocks` (line 189) fetches chunks in random order from local, then remote; each fetched chunk is stored locally and advertised to `BlockManagerMaster` (`tellMaster=true`) so other executors can pull from this peer — the P2P step that prevents driver bottlenecking.

**Anchor files:**

- [Broadcast.scala:57](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/broadcast/Broadcast.scala#L57) — abstract base
- [TorrentBroadcast.scala:60](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/broadcast/TorrentBroadcast.scala#L60) — sole implementation
- [TorrentBroadcast.scala:139](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/broadcast/TorrentBroadcast.scala#L139) — `writeBlocks`: chunk + store on driver
- [TorrentBroadcast.scala:189](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/broadcast/TorrentBroadcast.scala#L189) — `readBlocks`: peer-to-peer fetch with `tellMaster=true`

**Configs:** `spark.broadcast.blockSize`, `spark.broadcast.checksum`, `spark.broadcast.compress`, `spark.broadcast.UDFCompressionThreshold`

**Maps to topics:** I4 (RDD Fundamentals), E1 (Spark Internals)

---

## Context cleaner

**What it is:** `ContextCleaner` (`ContextCleaner.scala:65`) is a daemon thread using Java `WeakReference` + `ReferenceQueue` to detect GC of driver-side RDDs, shuffles, broadcasts, accumulators, and checkpoint data. Six `CleanupTask` subtypes (lines 39–44). A `ScheduledExecutorService` calls `System.gc()` every `spark.cleaner.periodicGC.interval` (default 30 min) to force processing in low-GC driver JVMs.

**Code path:** `registerRDDForCleanup(rdd)` (line 153) → `registerForCleanup` (line 184) → `CleanupTaskWeakReference` added to `referenceBuffer`. Cleaning thread `keepCleaning` (line 189) dispatches: `CleanRDD` → `sc.unpersistRDD`; `CleanShuffle` → `shuffleDriverComponents.removeShuffle` + `mapOutputTrackerMaster.unregisterShuffle`; `CleanBroadcast` → `broadcastManager.unbroadcast`; `CleanCheckpoint` → `ReliableRDDCheckpointData.cleanCheckpoint`.

**Anchor files:**

- [ContextCleaner.scala:38](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/ContextCleaner.scala#L38) — six `CleanupTask` subtypes
- [ContextCleaner.scala:65](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/ContextCleaner.scala#L65) — class declaration
- [ContextCleaner.scala:153](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/ContextCleaner.scala#L153) — registration methods
- [ContextCleaner.scala:189](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/ContextCleaner.scala#L189) — `keepCleaning` daemon loop

**Configs:** `spark.cleaner.periodicGC.interval`, `spark.cleaner.referenceTracking`, `spark.cleaner.referenceTracking.blocking`, `spark.cleaner.referenceTracking.blocking.shuffle`, `spark.cleaner.referenceTracking.cleanCheckpoints`

**Maps to topics:** E1 (Spark Internals)

---

## Pair RDD functions

**What it is:** `PairRDDFunctions[K, V]` (`PairRDDFunctions.scala:52`) is added to `RDD[(K,V)]` via Scala implicit conversion. The universal primitive is `combineByKeyWithClassTag` (line 72): builds an `Aggregator[K,V,C]` and either applies it in-place (if RDD already has the target partitioner) or creates a `ShuffledRDD` with `mapSideCombine=true`. `reduceByKey` (line 305) uses map-side combine — map tasks partially aggregate before shuffle. `groupByKey` (line 497) sets `mapSideCombine=false` — all values cross the network. `join` (line 544) delegates to `cogroup`. This makes `reduceByKey` vs `groupByKey` the canonical map-side-combine example.

**Anchor files:**

- [PairRDDFunctions.scala:52](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/rdd/PairRDDFunctions.scala#L52) — class declaration
- [PairRDDFunctions.scala:72](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/rdd/PairRDDFunctions.scala#L72) — `combineByKeyWithClassTag`: universal primitive
- [PairRDDFunctions.scala:305](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/rdd/PairRDDFunctions.scala#L305) — `reduceByKey`: `mapSideCombine=true`
- [PairRDDFunctions.scala:497](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/rdd/PairRDDFunctions.scala#L497) — `groupByKey`: `mapSideCombine=false`

**Maps to topics:** I4 (RDD Fundamentals), A4 (Data Skew and Shuffle Optimisation)

---

## Closure cleaning

**What it is:** `SparkContext.clean[F](f: F)` (line 2850) delegates to `SparkClosureCleaner.clean` → `ClosureCleaner.clean` in `common/utils`. `ClosureCleaner` uses ASM 9 bytecode analysis to find which `$outer` chain fields the closure actually reads, then nulls the unreferenced ones in-place. After cleaning, if `checkSerializable=true`, the closure is round-trip serialized via `SparkEnv.get.closureSerializer` — producing the `Task not serializable` exception early on the driver rather than later on the executor.

**Anchor files:**

- [SparkContext.scala:2850](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/SparkContext.scala#L2850) — `clean()`: driver entry point
- [SparkClosureCleaner.scala:35](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/util/SparkClosureCleaner.scala#L35) — wrapper with serialization check
- [ClosureCleaner.scala:37](https://github.com/apache/spark/blob/v4.2.0/common/utils/src/main/scala/org/apache/spark/util/ClosureCleaner.scala#L37) — ASM-based outer-field analysis (in `common/utils`, not `core`)
- [RDD.scala:425](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/rdd/RDD.scala#L425) — call site in `map`: `val cleanF = sc.clean(f)`

**Maps to topics:** I4 (RDD Fundamentals), E1 (Spark Internals)

---

## AccumulatorV2

**What it is:** `AccumulatorV2[IN, OUT]` (`AccumulatorV2.scala:44`) is abstract. Required overrides: `isZero`, `copy()`, `reset()`, `add(v: IN)`, `merge(other)`, `value: OUT`. `register(sc, name, countFailedValues)` (line 51) assigns an ID via `AccumulatorContext.newId()` and registers for GC-based cleanup. Each task receives a `copy()` of the driver instance; after task completion the local copy is merged back via `merge()`. `countFailedValues` controls whether failed-task values are included — true for internal metrics (bytes spilled), false for user counters.

**Anchor files:**

- [AccumulatorV2.scala:44](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/util/AccumulatorV2.scala#L44) — abstract class
- [AccumulatorV2.scala:51](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/util/AccumulatorV2.scala#L51) — `register()`: ID + cleanup registration
- [ContextCleaner.scala:157](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/ContextCleaner.scala#L157) — `registerAccumulatorForCleanup`

**Maps to topics:** E1 (Spark Internals)

---

## Async RDD actions

**What it is:** `AsyncRDDActions[T]` (`AsyncRDDActions.scala:35`) adds `countAsync`, `collectAsync`, `takeAsync`, `foreachAsync`, `foreachPartitionAsync` to every `RDD[T]` via implicit conversion. Each returns a `FutureAction` backed by `SparkContext.submitJob` (non-blocking). `takeAsync` implements a recursive-future scan: starts at `spark.rdd.limit.initialNumPartitions` partitions, doubles by `spark.rdd.limit.scaleUpFactor` until `num` records are collected or all partitions exhausted. A bounded thread pool (max 128 threads, `AsyncRDDActions.futureExecutionContext`, line 145) handles future chaining.

**Anchor files:**

- [AsyncRDDActions.scala:35](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/rdd/AsyncRDDActions.scala#L35) — class declaration
- [AsyncRDDActions.scala:40](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/rdd/AsyncRDDActions.scala#L40) — `countAsync`
- [AsyncRDDActions.scala:69](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/rdd/AsyncRDDActions.scala#L69) — `takeAsync`: recursive-future scan

**Configs:** `spark.rdd.limit.initialNumPartitions`, `spark.rdd.limit.scaleUpFactor`

**Maps to topics:** I4 (RDD Fundamentals)

---

## Serialization

**What it is:** `KryoSerializer` (`KryoSerializer.scala:63`) reads all Kryo config at construction (lines 68–101) and builds a `KryoPool` of reusable `Kryo` instances. `newKryo()` (line 140) registers built-in Spark types (block IDs, `CompressedMapStatus`, `RoaringBitmap`, etc.) plus user types from `spark.kryo.classesToRegister` and user registrators from `spark.kryo.registrator`. Unsafe I/O (`KryoUnsafeInput`/`KryoUnsafeOutput`) is the default (`spark.kryo.unsafe=true`). `JavaSerializer` (the `spark.serializer` default) uses Java object streams; stream reset every `spark.serializer.objectStreamReset` objects (default 100) prevents the internal object table from growing unbounded.

**Anchor files:**

- [KryoSerializer.scala:63](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/serializer/KryoSerializer.scala#L63) — class; lines 68–101 read all configs
- [KryoSerializer.scala:140](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/serializer/KryoSerializer.scala#L140) — `newKryo()`: built-in type registration

**Configs:** `spark.serializer`, `spark.serializer.objectStreamReset`, `spark.kryo.classesToRegister`, `spark.kryo.pool`, `spark.kryo.referenceTracking`, `spark.kryo.registrationRequired`, `spark.kryo.registrator`, `spark.kryo.unsafe`, `spark.kryoserializer.buffer`, `spark.kryoserializer.buffer.max`

**Maps to topics:** E1 (Spark Internals)

---

## Approximate actions

**What it is:** `countApprox`, `countByValueApprox`, `sumApprox`, `meanApprox` and `countByKeyApprox` submit an ordinary job, but hand each task result to an incremental `ApproximateEvaluator` as it lands and return a `PartialResult[BoundedDouble]` once a wall-clock timeout expires. The estimate extrapolates from the fraction of **partitions** completed, not the fraction of rows — that is the whole statistical premise. Still live and public in 4.2.0; there is no DataFrame/SQL equivalent.

**Code path:** `RDD.countApprox` → `SparkContext.runApproximateJob` → `DAGScheduler.runApproximateJob` → `new ApproximateActionListener` → per-task `taskSucceeded` → `evaluator.merge` → `listener.awaitResult()` → `PartialResult`

**Anchor files:**

- [RDD.scala:1336](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/rdd/RDD.scala#L1336) — `countApprox`, `confidence` default 0.95
- [SparkContext.scala:2605](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/SparkContext.scala#L2605) — `@DeveloperApi runApproximateJob`
- [DAGScheduler.scala:1089](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala#L1089) — empty-RDD short circuit, returns a *final* result without running anything
- [ApproximateActionListener.scala:70](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/partial/ApproximateActionListener.scala#L70) — `awaitResult`: the three-way timeout decision
- [CountEvaluator.scala:50](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/partial/CountEvaluator.scala#L50) — `bound()`: remainder modelled as `Poisson(sum * (1 - p) / p)`
- [PartialResult.scala:33](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/partial/PartialResult.scala#L33) — `getFinalValue()`, a `wait()` with no timeout

!!! warning "The timeout does not cancel the job, and three failures are silent"

    `runApproximateJob` returns after `awaitResult` without calling `cancelJob` — the full job runs to completion on the cluster, so `countApprox` costs exactly what `count()` costs. Only the driver's blocking time is saved. Three silent paths: a job that fails *after* the timeout is never reported (`PartialResult.setFailure` is unreachable from the main source tree, so `getFinalValue()` blocks forever); only `taskSucceeded` merges, so retries silently shrink the sample; and `CountEvaluator` returns `(0, +Inf)` at confidence 0.0 for a genuinely empty RDD, indistinguishable from "learned nothing".

!!! info "The exchangeability assumption is where the estimate breaks"

    Every evaluator's `p` is the fraction of *partitions*, and assumes unseen partitions resemble seen ones. On skewed data the small partitions finish first, so the extrapolation is biased low and the stated confidence is not the achieved confidence. Nothing detects or warns about this. PySpark compounds matters: `rdd.py:4843` calls the blocking `getFinalValue()`, so the timeout is inert from Python.

**Configs:** none directly; `spark.default.parallelism` only via `partitions.length`, the evaluator's denominator

**Maps to topics:** none — proposed as I16

---

## Whole-file and binary sources

**What it is:** `SparkContext.binaryFiles`, `wholeTextFiles` and `binaryRecords` read whole files (or fixed-length records) as RDD records. The first two set `isSplitable = false` and pack whole files into splits with `CombineFileInputFormat`; `binaryRecords` is the only splittable one, aligning splits to record boundaries. They are governed by the `spark.files.*` config family — a different family from the `spark.sql.files.*` one that DataFrame reads use.

**Code path:** `SparkContext.binaryFiles` → `BinaryFileRDD.getPartitions` → `StreamFileInputFormat.setMinPartitions` → `CombineFileInputFormat.getSplits` → task: `NewHadoopRDD.compute` → `StreamBasedRecordReader.nextKeyValue` → `PortableDataStream`

**Anchor files:**

- [SparkContext.scala:1225](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/SparkContext.scala#L1225) — `binaryFiles`
- [PortableDataStream.scala:41](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/input/PortableDataStream.scala#L41) — `isSplitable = false`: why a 10 GB file is one task
- [PortableDataStream.scala:47](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/input/PortableDataStream.scala#L47) — `setMinPartitions`: the only consumer of `spark.files.maxPartitionBytes` / `openCostInBytes`
- [PortableDataStream.scala:202](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/input/PortableDataStream.scala#L202) — `toArray()`: whole file into one JVM byte array
- [WholeTextFileInputFormat.scala:51](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/input/WholeTextFileInputFormat.scala#L51) — a *different* split formula that ignores both configs
- [FixedLengthBinaryInputFormat.scala:68](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/input/FixedLengthBinaryInputFormat.scala#L68) — record-aligned `computeSplitSize`
- [FixedLengthBinaryRecordReader.scala:87](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/input/FixedLengthBinaryRecordReader.scala#L87) — compressed input rejected outright

!!! warning "`minPartitions` means opposite things in the two whole-file APIs"

    `binaryFiles` computes `max(sc.defaultParallelism, minPartitions)` — so `minPartitions` is a *floor* that `defaultParallelism` can override, and it cannot lower the partition count. `wholeTextFiles` uses `ceil(totalLen / minPartitions)` with no `defaultParallelism` term and no cap, so it is a genuine target. Identical-looking signatures, different behaviour.

!!! warning "Silent truncation and silent mojibake"

    With `ignoreCorruptFiles=true`, a mid-file `IOException` marks the partition finished and the **job succeeds with a truncated result** — the only trace is a `logWarning`. `wholeTextFiles` decodes bytes as UTF-8 with replacement, so a latin-1 or binary file yields U+FFFD and no error, despite the scaladoc requiring UTF-8. For `binaryRecords`, a file length that is not a multiple of `recordLength` throws `EOFException` on the last read — swallowed under `ignoreCorruptFiles`, silently dropping records.

**Configs:** `spark.files.maxPartitionBytes`, `spark.files.openCostInBytes` (**`binaryFiles` only**), `spark.files.ignoreCorruptFiles`, `spark.files.ignoreMissingFiles`, `spark.default.parallelism`

**Maps to topics:** none — proposed as I17. The modern Connect-compatible successors are `spark.read.format("binaryFile")` and `spark.read.option("wholetext", true).text(...)`.

---

## Refresh log

| Date | Spark | What changed |
|---|---|---|
| 2026-06-06 | 4.1.2 | Initial sweep. 12 concepts, 48 anchors. |
| 2026-07-19 | 4.2.0 | Re-verified all 48 anchors against the v4.2.0 checkout: 41 unchanged, 7 moved, 0 gone. Six of the seven are in `RDD.scala` and share a `+16` offset; the seventh is `MapPartitionsRDD.compute`, 51 → 56, because a `preservesPartitionSizes` constructor parameter was added — old line 51 still lands on real RDD code (`override val partitioner`), which is exactly why a stale anchor is dangerous. Also corrected four inline prose references in `Dependency.scala` that the original sweep never verified. Added two concepts for `partial/` and `input/`, newly added to this group's scope. |
