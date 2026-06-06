---
subsystem: core
spark_version: "4.1.2"
group: rdd-layer
all_groups: [rdd-layer, execution-engine, shuffle-memory, storage-serializer, infra]
status: partial
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
---

## RDD model

**What it is:** `RDD[T]` is the fundamental immutable, partitioned, fault-tolerant collection abstraction in Spark. Every RDD is fully characterised by five properties: the partition list, a compute function, a dependency list, an optional partitioner, and optional preferred locations.

**Code path:**

`RDD.scala` (abstract class at line 84) declares the five abstract/overridable members in the block "Methods that should be implemented by subclasses" (lines 108–139):

- `def compute(split: Partition, context: TaskContext): Iterator[T]` — line 116. The single mandatory override; returns a partition iterator. Called only via `iterator()` (line 334).
- `protected def getPartitions: Array[Partition]` — line 125. Invoked once per RDD lifetime; result cached in `partitions_` (line 251) via double-checked locking in the public `partitions` accessor (lines 296–311).
- `protected def getDependencies: Seq[Dependency[_]]` — line 131. Default returns the constructor-injected `deps` field; result cached in `dependencies_` (line 247). After checkpointing, `dependencies` (line 260) short-circuits to a single `OneToOneDependency` on the `CheckpointRDD`.
- `protected def getPreferredLocations(split: Partition): Seq[String]` — line 136. Default returns `Nil`; the public `preferredLocations` wrapper (line 323) delegates to the checkpoint RDD if the RDD has been checkpointed.
- `@transient val partitioner: Option[Partitioner]` — line 139. Default is `None`. RDDs that introduce a partitioning contract (e.g. `ShuffledRDD`, `MapPartitionsRDD` with `preservesPartitioning = true`) override this field.

**Lineage graph.** The lineage DAG is the transitive closure of `RDD.dependencies`. `toDebugString` (line 1983) performs a recursive DFS of the dependency chain, formatting shuffle boundaries with `+-` tree notation and narrow chains with straight indentation; it reads `storageLevel.description` to annotate cached partitions.

**`RDD.iterator()` dispatch** (line 334) — the per-task entry point called by every executor task:

```
RDD.iterator(split, ctx)
  storageLevel != NONE  → getOrCompute(split, ctx)             [line 336]
                              blockId = RDDBlockId(id, partition.index)
                              BlockManager.getOrElseUpdateRDDBlock(taskId, blockId, ...)
                                cache hit  → Left(BlockResult)
                                cache miss → computeOrReadCheckpoint → compute
  storageLevel == NONE  → computeOrReadCheckpoint(split, ctx)  [line 338]
                              isCheckpointedAndMaterialized?
                                yes → firstParent.iterator(split, ctx)
                                no  → compute(split, ctx)
```

**`Partition`** (`Partition.scala` line 23) is a single-method trait: `def index: Int`. `hashCode` is overridden to return `index` for efficient use in maps. Subclasses (e.g. `CoalescedRDDPartition`, `ShuffledRDDPartition`) add implementation-specific fields.

**`Dependency` hierarchy** (`Dependency.scala`):

- `abstract class Dependency[T]` (line 41) — sole method `def rdd: RDD[T]`.
- `abstract class NarrowDependency[T]` (line 52) — adds `getParents(partitionId: Int): Seq[Int]` for partition-to-partition mapping; enables pipelined (no-shuffle) execution.
- `class OneToOneDependency` (line 262) — `getParents` returns `List(partitionId)`; used by `map`, `filter`, `flatMap`, etc.
- `class RangeDependency` (line 276) — maps a contiguous output range back to an input range; used by `UnionRDD`.
- `class ShuffleDependency[K,V,C]` (line 84) — wide dependency; carries `partitioner`, optional `aggregator`, `serializer`, `shuffleId`; registers itself with `ContextCleaner` at construction (line 252) and with `ShuffleManager` (line 131).

**Anchor files:**

- [rdd/RDD.scala](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/rdd/RDD.scala#L84)
- [Dependency.scala](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/Dependency.scala#L41)
- [Partition.scala](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/Partition.scala#L23)

**Configs:** `spark.rdd.compress`, `spark.rdd.cache.visibilityTracking.enabled`, `spark.serializer`

**Maps to topics:** I4 (RDD Fundamentals), B1 (Architecture) · **Coverage:** ⬜ no chapter yet

---

## Transformations and actions

**What it is:** Transformations return new RDD objects and are lazy — no Spark job is submitted. Actions call `SparkContext.runJob`, which submits the DAG to the `DAGScheduler` and blocks until a result is returned to the driver. The lazy/eager split means the entire lineage graph is constructed on the driver before any executor work begins.

**Code path — transformations (all in `rdd/RDD.scala`):**

- `map` (line 424): `new MapPartitionsRDD[U,T](this, (_, _, iter) => iter.map(cleanF))`.
- `flatMap` (line 433): `new MapPartitionsRDD[U,T](this, (_, _, iter) => iter.flatMap(cleanF))`.
- `filter` (line 441): `new MapPartitionsRDD[T,T](..., preservesPartitioning = true)`.

All three call `sc.clean(f)` first to remove closure references to non-serializable driver-side objects before shipping the function to executors.

`MapPartitionsRDD.compute` (`rdd/MapPartitionsRDD.scala` line 51) invokes `f(context, split.index, firstParent[T].iterator(split, context))`. The parent `iterator` call is deferred to task-execution time. `getPartitions` (line 49) delegates directly to the parent, so no new partition array is allocated; the child RDD adds zero per-partition overhead until `compute` is called.

**Code path — actions (all in `rdd/RDD.scala`):**

- `foreach` (line 1037): `sc.runJob(this, (iter) => iter.foreach(cleanF))` — fires tasks, no return value.
- `collect` (line 1056): `sc.runJob(this, (iter) => iter.toArray)` → `Array.concat(results)` on driver.
- `count` (line 1304): `sc.runJob(this, Utils.getIteratorSize _).sum`.
- `reduce` (line 1130): `sc.runJob(this, reducePartition, mergeResult)` — `reducePartition` does `reduceLeft` per partition; `mergeResult` merges partial results on the driver.
- `saveAsTextFile` (line 1616) → overload at line 1623 → `mapPartitions { iter => iter.map(x => (NullWritable.get(), new Text(x.toString))) }.saveAsHadoopFile[TextOutputFormat](path, codec)`.

`sc.runJob` is the universal action trigger. It routes to `DAGScheduler.runJob` → `ResultStage` → one `ResultTask` per partition. Each task calls `RDD.iterator(split, ctx)` on the executor.

**Anchor files:**

- [rdd/RDD.scala — transformations](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/rdd/RDD.scala#L419)
- [rdd/RDD.scala — actions](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/rdd/RDD.scala#L1032)
- [rdd/MapPartitionsRDD.scala](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/rdd/MapPartitionsRDD.scala#L39)
- [rdd/PairRDDFunctions.scala](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/rdd/PairRDDFunctions.scala#L528)

**Configs:** `spark.rdd.limit.initialNumPartitions`, `spark.rdd.limit.scaleUpFactor`, `spark.serializer`

**Maps to topics:** I4 (RDD Fundamentals) · **Coverage:** ⬜ no chapter yet

---

## Partitioning

**What it is:** A `Partitioner` assigns each key to a partition index (0 to `numPartitions - 1`), determining post-shuffle data layout. RDDs that carry a `partitioner` can skip re-shuffling in co-group and join operations when the existing partitioning already satisfies the requirement.

**Code path — abstract contract:**

`abstract class Partitioner` (`Partitioner.scala` line 42) — two abstract methods: `def numPartitions: Int` and `def getPartition(key: Any): Int`.

`Partitioner.defaultPartitioner` (line 67): reads `spark.default.parallelism` to obtain a fallback count (or uses the maximum upstream partition count if the config is absent); reuses an existing partitioner if it is "eligible" (within one order of magnitude of the maximum partition count); otherwise creates `new HashPartitioner(defaultNumPartitions)`.

**`HashPartitioner`** (line 114): `getPartition(key) = Utils.nonNegativeMod(key.hashCode, numPartitions)`. `null` keys map to partition 0. Equality is defined solely by `numPartitions`.

**`RangePartitioner`** (line 176): at construction time it runs `RangePartitioner.sketch` (line 335) — a `mapPartitionsWithIndex` reservoir-sampling job — to collect key samples, then `determineBounds` (line 358) picks weighted quantile cut-points into the `rangeBounds` array. `getPartition` uses linear scan for ≤ 128 bounds and binary search otherwise (lines 244–268). The actual number of output partitions equals `rangeBounds.length + 1`, which may be less than the requested `partitions` parameter if there are fewer distinct keys than partitions.

**`partitionBy`** (`rdd/PairRDDFunctions.scala` line 528): if `self.partitioner == Some(partitioner)` it returns `self` (no shuffle); otherwise `new ShuffledRDD[K,V,V](self, partitioner)`.

**`ShuffledRDD`** (`rdd/ShuffledRDD.scala` line 41): `getDependencies` (line 78) creates a single `ShuffleDependency` with the chosen partitioner, serializer, and optional combiner. `getPartitions` (line 92) allocates one `ShuffledRDDPartition` per output bucket. `compute` (line 102) calls `SparkEnv.get.shuffleManager.getReader(dep.shuffleHandle, ...).read()` to pull sorted/aggregated records from map output files.

**`coalesce` vs `repartition`** (`rdd/RDD.scala` lines 511–537):

- `coalesce(n, shuffle=false)`: `new CoalescedRDD(this, n)`. `CoalescedRDD.getDependencies` (line 104 of `CoalescedRDD.scala`) returns a `NarrowDependency`; `getParents` maps each coalesced partition to a range of parent partitions. Zero shuffle, narrow dependency, no network I/O.
- `repartition(n)` — delegates to `coalesce(n, shuffle=true)`: first `mapPartitionsWithIndexInternal` tags records with a round-robin `(position, element)` key starting from a random offset, then `ShuffledRDD[Int,T,T]` with `HashPartitioner(n)` redistributes the data, then `CoalescedRDD` merges the output partitions. This guarantees all upstream partitions run in parallel.

**Anchor files:**

- [Partitioner.scala](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/Partitioner.scala#L42)
- [rdd/ShuffledRDD.scala](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/rdd/ShuffledRDD.scala#L41)
- [rdd/CoalescedRDD.scala](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/rdd/CoalescedRDD.scala#L75)
- [rdd/PairRDDFunctions.scala](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/rdd/PairRDDFunctions.scala#L528)

**Configs:** `spark.default.parallelism`, `spark.rdd.parallelListingThreshold`, `spark.serializer`

**Maps to topics:** I4 (RDD Fundamentals), I5 (Partitioning), A4 (Data Skew and Shuffle Optimisation) · **Coverage:** ⬜ no chapter yet

---

## Persistence

**What it is:** RDD persistence pins computed partition data in a storage medium (memory, disk, off-heap, or a combination) so repeated actions avoid full recomputation. `cache()` is syntactic sugar for `persist(MEMORY_ONLY)`.

**Code path:**

`StorageLevel` (`common/utils/src/main/scala/org/apache/spark/storage/StorageLevel.scala` line 39) encodes four boolean flags — `useDisk` (bit 3), `useMemory` (bit 2), `useOffHeap` (bit 1), `deserialized` (bit 0) — plus a `_replication` byte. The companion object (lines 149–161) defines 13 named constants. `OFF_HEAP = new StorageLevel(true, true, true, false, 1)` — `useDisk=true` acts as a spill path when off-heap capacity is exhausted. `StorageLevel` implements `Externalizable`, serializing to exactly 2 bytes; a `ConcurrentHashMap` (`storageLevelCache`, line 225) guarantees instance identity after deserialization.

`RDD.persist(newLevel)` (`rdd/RDD.scala` line 186):

```
RDD.persist(newLevel)
  isLocallyCheckpointed?
    → persist(LocalRDDCheckpointData.transformStorageLevel(newLevel), allowOverride=true)
  else
    → private persist(newLevel, allowOverride=false)    [line 166]
        storageLevel == NONE (first call)?
          → sc.cleaner.foreach(_.registerRDDForCleanup(this))   [line 174]
          → sc.persistRDD(this)                                  [line 175]
        storageLevel = newLevel                                  [line 177]
```

`RDD.cache()` (line 205) → `persist()` → `persist(StorageLevel.MEMORY_ONLY)`.

`RDD.unpersist(blocking)` (line 213) → `sc.unpersistRDD(id, blocking)` → removes all `RDDBlockId(id, *)` blocks from every executor's `BlockManager`.

**Read path on executor** (`RDD.getOrCompute`, line 381):

```
getOrCompute(partition, ctx)
  blockId = RDDBlockId(id, partition.index)
  BlockManager.getOrElseUpdateRDDBlock(taskId, blockId, storageLevel, classTag, makeIterator)
    [BlockManager.scala line 1376]
    → isCacheVisible?
        yes → get(blockId)  [try local then remote]
                hit → Left(BlockResult)
        → doPutIterator(blockId, () => computeOrReadCheckpoint(...), level, ...)
            compute partition, store blocks, return iterator
```

`spark.rdd.cache.visibilityTracking.enabled` gates a task-level visibility check: a block computed by one task is not served to other tasks until the master confirms visibility, preventing incorrect cache hits on indeterminate RDDs.

**Anchor files:**

- [storage/StorageLevel.scala](https://github.com/apache/spark/blob/v4.1.2/common/utils/src/main/scala/org/apache/spark/storage/StorageLevel.scala#L39)
- [rdd/RDD.scala — persist section](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/rdd/RDD.scala#L166)
- [storage/BlockManager.scala — getOrElseUpdateRDDBlock](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/storage/BlockManager.scala#L1376)

**Configs:** `spark.rdd.compress`, `spark.rdd.cache.visibilityTracking.enabled`, `spark.storage.memoryMapThreshold`, `spark.storage.replication.proactive`, `spark.storage.maxReplicationFailures`, `spark.storage.unrollMemoryThreshold`, `spark.storage.unrollMemoryGrowthFactor`, `spark.storage.decommission.rddBlocks.enabled`

**Maps to topics:** I6 (Caching and Persistence) · **Coverage:** ⬜ no chapter yet

---

## Checkpointing

**What it is:** Checkpointing materialises an RDD to durable storage and replaces its entire parent dependency chain with a single `OneToOneDependency` on a `CheckpointRDD`, preventing unbounded lineage growth. Two flavours exist: reliable (writes to HDFS/S3 via a Hadoop `FileSystem`) and local (writes to executor block store, no fault-tolerance guarantee).

**Code path — reliable checkpoint:**

`RDD.checkpoint()` (`rdd/RDD.scala` line 1670) — called on the driver before any action:

```
RDD.checkpoint()
  RDDCheckpointData.synchronized
  context.checkpointDir.isEmpty → throw error
  checkpointData = Some(new ReliableRDDCheckpointData(this))    [line 1677]
```

`ReliableRDDCheckpointData` constructor (`rdd/ReliableRDDCheckpointData.scala` line 34) eagerly resolves `cpDir` via `ReliableRDDCheckpointData.checkpointPath(rdd.context, rdd.id)` (line 80) → `new Path(checkpointDir, s"rdd-$rddId")`.

After the first action completes, `SparkContext.runJob` calls `RDD.doCheckpoint()` recursively (line 1941 of `rdd/RDD.scala`):

```
RDD.doCheckpoint()
  checkpointData.isDefined?
    checkpointAllMarkedAncestors → dependencies.foreach(_.rdd.doCheckpoint())
    checkpointData.get.checkpoint()            [RDDCheckpointData.checkpoint, line 64]
      cpState := CheckpointingInProgress
      newRDD = doCheckpoint()                  [dispatches to subclass]
        [ReliableRDDCheckpointData.doCheckpoint, line 60]
        ReliableCheckpointRDD.writeRDDToCheckpointDirectory(rdd, cpDir)
        if cleanCheckpoints → cleaner.registerRDDCheckpointDataForCleanup(newRDD, rdd.id)
      cpRDD = Some(newRDD)
      cpState := Checkpointed
      rdd.markCheckpointed()                   [line 1965 of RDD.scala]
        clearDependencies() → dependencies_ = null
        partitions_ = null
        deps = null
```

After `markCheckpointed()`, `RDD.dependencies` (line 260) routes through `checkpointRDD.map(r => List(new OneToOneDependency(r)))`, replacing the entire prior lineage with a single link to the checkpoint file.

**Local checkpoint** (`RDD.localCheckpoint()` line 1706): sets `checkpointData = Some(new LocalRDDCheckpointData(this))` and forces the RDD to be persisted with a disk-inclusive storage level. The lineage truncation via `markCheckpointed()` still occurs, but the checkpoint data is ephemeral.

`spark.checkpoint.dir` (since 4.0.0) provides a global fallback checkpoint directory in `SparkConf`; `SparkContext.setCheckpointDir()` takes precedence.

**Anchor files:**

- [rdd/RDD.scala — checkpoint methods](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/rdd/RDD.scala#L1670)
- [rdd/RDDCheckpointData.scala](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/rdd/RDDCheckpointData.scala#L40)
- [rdd/ReliableRDDCheckpointData.scala](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/rdd/ReliableRDDCheckpointData.scala#L34)

**Configs:** `spark.checkpoint.compress`, `spark.checkpoint.dir`, `spark.rdd.checkpoint.cachePreferredLocsExpireTime`, `spark.cleaner.referenceTracking.cleanCheckpoints`

**Maps to topics:** I4 (RDD Fundamentals), I6 (Caching and Persistence) · **Coverage:** ⬜ no chapter yet

---

## Broadcast

**What it is:** A broadcast variable distributes a read-only value to every executor once using a BitTorrent-style peer-to-peer block fetch. The driver splits the serialized value into fixed-size chunks, stores them in its `BlockManager`, and executors fetch chunks from the driver or from peers that have already fetched them — removing the driver as the sole bottleneck.

**Code path:**

`SparkContext.broadcast(value)` (`SparkContext.scala` line 1720) → `broadcastInternal(value, serializedOnly=false)` (line 1733):

```
sc.broadcast(v)
  env.broadcastManager.newBroadcast[T](value, isLocal, serializedOnly)
    → new TorrentBroadcast[T](obj, id, serializedOnly)          [TorrentBroadcast.scala line 60]
        setConf(SparkEnv.get.conf)      reads blockSize (default 4MB), compressionCodec, checksumEnabled
        numBlocks = writeBlocks(obj)                            [line 139]
          blockManager.putSingle(broadcastId, value, MEMORY_AND_DISK, tellMaster=false)
          blocks = TorrentBroadcast.blockifyObject(value, blockSize, serializer, codec)
          for each block i:
            blockManager.putBytes(BroadcastBlockId(id,"piece"+i), bytes, MEMORY_AND_DISK_SER, tellMaster=true)
  cleaner.foreach(_.registerBroadcastForCleanup(bc))            [line 1743]
```

On executor (first access to `broadcastVar.value`):

```
Broadcast.value()
  assertValid()
  getValue()                                   [TorrentBroadcast.getValue, line 105]
    → readBroadcastBlock()                     [line 254]
        blockManager.getLocalValues(broadcastId)? → return local
        → readBlocks()                         [line 189]
            for each piece pid in random order:
              bm.getLocalBytes(pieceId)?       → use local
              bm.getRemoteBytes(pieceId)?
                → validate checksum (if enabled)
                → bm.putBytes(pieceId, MEMORY_AND_DISK_SER, tellMaster=true)  [peer now advertised]
        TorrentBroadcast.unBlockifyObject(blocks, serializer, codec)
        blockManager.putSingle(broadcastId, obj, MEMORY_AND_DISK, tellMaster=false)
```

The `tellMaster=true` on `putBytes` in `readBlocks` advertises this executor's newly-fetched piece to `BlockManagerMaster`, making it discoverable by all other executors — this is the peer-to-peer propagation step that avoids driver bottlenecking.

**Cleanup:** `cleaner.registerBroadcastForCleanup(bc)` attaches a `WeakReference`; when the `Broadcast` object is GC'd on the driver, `doCleanupBroadcast` calls `broadcastManager.unbroadcast(id, removeFromDriver=true, blocking)` → removes all `BroadcastBlockId` blocks from all executors and the driver.

**Anchor files:**

- [broadcast/Broadcast.scala](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/broadcast/Broadcast.scala#L57)
- [broadcast/TorrentBroadcast.scala](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/broadcast/TorrentBroadcast.scala#L60)
- [SparkContext.scala — broadcast](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/SparkContext.scala#L1720)

**Configs:** `spark.broadcast.blockSize`, `spark.broadcast.compress`, `spark.broadcast.checksum`, `spark.broadcast.UDFCompressionThreshold`, `spark.serializer`, `spark.io.compression.codec`, `spark.io.compression.lz4.blockSize`

**Maps to topics:** I4 (RDD Fundamentals), E1 (Spark Internals) · **Coverage:** ⬜ no chapter yet

---

## Context cleaner

**What it is:** `ContextCleaner` is a daemon background thread on the driver that tracks RDDs, shuffles, broadcasts, accumulators, and checkpoint data via Java `WeakReference`s. When the driver GC collects a referent, the corresponding `CleanupTask` is dequeued and executor/driver resources are freed asynchronously.

**Code path:**

`ContextCleaner` (`ContextCleaner.scala` line 65) is started by `SparkContext.init` when `spark.cleaner.referenceTracking = true`. Its `cleaningThread` (line 80) runs `keepCleaning()` (line 189) in a daemon loop:

```
keepCleaning()
  referenceQueue.remove(100ms timeout)         [blocks until a referent is GC'd]
  ref.task match
    CleanRDD(rddId)         → doCleanupRDD(rddId, blockOnCleanupTasks)
                                 sc.unpersistRDD(rddId, blocking)
    CleanShuffle(shuffleId) → doCleanupShuffle(shuffleId, blockOnShuffleTasks)
                                 shuffleDriverComponents.removeShuffle(shuffleId)
                                 mapOutputTrackerMaster.unregisterShuffle(shuffleId)
    CleanBroadcast(id)      → doCleanupBroadcast(id, blockOnCleanupTasks)
                                 broadcastManager.unbroadcast(id, true, blocking)
    CleanAccum(id)          → doCleanupAccum(id, ...)
                                 AccumulatorContext.remove(id)
    CleanCheckpoint(rddId)  → doCleanCheckpoint(rddId)
                                 ReliableRDDCheckpointData.cleanCheckpoint(sc, rddId)
```

**Registration pattern:**

- `RDD.persist()` (line 174 of `rdd/RDD.scala`) → `cleaner.registerRDDForCleanup(this)`.
- `ShuffleDependency` constructor (line 252 of `Dependency.scala`) → `cleaner.registerShuffleForCleanup(this)`.
- `SparkContext.broadcastInternal` (line 1743 of `SparkContext.scala`) → `cleaner.registerBroadcastForCleanup(bc)`.
- `ReliableRDDCheckpointData.doCheckpoint` (line 65 of `ReliableRDDCheckpointData.scala`) → `cleaner.registerRDDCheckpointDataForCleanup(newRDD, rdd.id)` — only when `spark.cleaner.referenceTracking.cleanCheckpoints = true`.

`registerForCleanup` (line 184) adds a `CleanupTaskWeakReference` — a `WeakReference` subclass carrying the `CleanupTask` — to `referenceBuffer`. The `referenceBuffer` keeps the `WeakReference` objects alive until the referent is collected, at which point the JVM automatically enqueues the reference on `referenceQueue`.

**Periodic GC.** `periodicGCService` (line 82) schedules `System.gc()` every `spark.cleaner.periodicGC.interval` (default 30 min). In long-running applications with large driver heaps, GC pressure may be too low to collect stale RDDs promptly; this periodic nudge ensures cleanup still fires.

**Blocking behaviour controls:**

- `spark.cleaner.referenceTracking.blocking` (default `true`) — RDD and broadcast cleanups block the cleaning thread until `BlockManager` RPC round-trips complete.
- `spark.cleaner.referenceTracking.blocking.shuffle` (default `false`) — shuffle cleanup is non-blocking by default to avoid RPC timeout cascades under high GC frequency (SPARK-3139).

**Anchor files:**

- [ContextCleaner.scala](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/ContextCleaner.scala#L65)
- [Dependency.scala — ShuffleDependency registration](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/Dependency.scala#L252)
- [rdd/ReliableRDDCheckpointData.scala — cleaner registration](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/rdd/ReliableRDDCheckpointData.scala#L64)

**Configs:** `spark.cleaner.periodicGC.interval`, `spark.cleaner.referenceTracking`, `spark.cleaner.referenceTracking.blocking`, `spark.cleaner.referenceTracking.blocking.shuffle`, `spark.cleaner.referenceTracking.cleanCheckpoints`

**Maps to topics:** E1 (Spark Internals) · **Coverage:** ⬜ no chapter yet
