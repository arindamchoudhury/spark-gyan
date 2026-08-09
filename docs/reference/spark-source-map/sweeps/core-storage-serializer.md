---
subsystem: core
spark_version: "4.2.0"
swept_at: 2026-08-09
group: storage-serializer
all_groups: [rdd-layer, execution-engine, shuffle-memory, storage-serializer,
  submit-standalone, monitoring, config-security, rpc-resources, api-bridge]
status: complete
concepts:
  - name: blockmanager-initialization
    topics: [B1, E2]
  - name: driver-location-registry
    topics: [B1, E2, I6]
  - name: read-path-and-the-iterator-lock
    topics: [I6, E1]
  - name: get-or-else-update
    topics: [I6, E1]
  - name: cache-visibility-tracking
    topics: [I6, A14]
  - name: write-path-and-disk-fallback
    topics: [I6, E1]
  - name: block-locking
    topics: []
    propose:
      code: E15
      level: Expert
      title: "Block Locking and Cache Visibility"
      what: "Every cached or shuffled block sits behind a per-block readers-writer lock attributed to a task attempt, released wholesale when the task ends. Separately, an RDD block reported by a still-running task is held invisible until the driver learns the task succeeded, so a speculative or failed attempt cannot publish partial data."
      why: "Two observable symptoms have no other explanation. A cached iterator you never drain keeps its read lock and pins the block against eviction for the rest of the task. And the executor log line 'N block locks were not released by task X' is unreadable without the protocol — it is reported at INFO by default, so a genuine leak is effectively invisible unless spark.storage.exceptionOnPinLeak is on."
  - name: block-status-reporting-and-reregistration
    topics: [E2, B1]
  - name: unreadable-blocks
    topics: [E1, A4]
  - name: remote-block-fetch-and-location-refresh
    topics: [I6, A4, E2]
  - name: replication-and-topology
    topics: [I6, E2]
  - name: executor-loss-and-proactive-replication
    topics: [E2, B1, I6]
  - name: disk-layout
    topics: [E2, A4]
  - name: disk-lifecycle-and-shutdown
    topics: [E2, A4]
  - name: memory-eviction
    topics: [I6, E1]
  - name: decommission-block-migration
    topics: [E2, A4]
  - name: fallback-storage
    topics: [E2]
  - name: serializer-abstraction
    topics: [E11, E1]
  - name: java-serializer
    topics: [E11, E1]
  - name: not-serializable-enrichment
    topics: [E11, E1, I14]
  - name: kryo-construction-and-buffers
    topics: [E11, E1, A4]
  - name: kryo-pool-and-registration
    topics: [E11, E1]
  - name: relocation-support
    topics: [E11, E1, A4]
  - name: serializer-manager-auto-selection
    topics: [E11, E1, I6]
  - name: storage-level-model
    topics: [I6, E1]
  - name: block-id-taxonomy
    topics: [I6, E1, I7]
  - name: disk-block-object-writer
    topics: [E1, A4]
  - name: disk-store-and-encrypted-blocks
    topics: [I6, E1, E2]
  - name: memory-mapped-buffer-disposal
    topics: [E1, I6]
  - name: block-log-writers
    topics: [E3, I3]
  - name: blockmanagermaster-rpc-plane
    topics: [B1, E2, E1]
  - name: storage-endpoint-async-removal
    topics: [E2, E1]
  - name: blockmanagerid-identity
    topics: [B1, E2]
  - name: unroll-memory
    topics: []
    propose:
      code: E51
      level: Expert
      title: "Unroll Memory: Materialising a Cached Partition Without an OOM"
      what: "Before a partition can be cached in memory it must be materialised from an iterator whose size is unknown, so the MemoryStore reserves a small initial budget and grows it geometrically while periodically re-estimating the partially-built block — reserving as *unroll* memory, a third accounting category alongside execution and storage, and transferring it to storage memory atomically only once the block is complete."
      why: "Every 'Not enough space to cache rdd_N_M in memory' warning is an unroll failure, not a storage-capacity failure, and the two have different fixes. Unroll memory is charged per task attempt and is invisible in the Storage tab, so N concurrent tasks each unrolling a large partition can exhaust storage memory while the tab shows almost nothing cached; and a failed unroll hands back a PartiallyUnrolledIterator that keeps holding its reservation until the caller drains or closes it."
  - name: managed-buffer-lock-bridge
    topics: [E15, E1]
  - name: avro-schema-registration
    topics: [E11, I10]
  - name: serializer-helper-chunked-buffers
    topics: [E11, E1]
  - name: storage-status-and-metrics-model
    topics: [E3, I7, I6]
---

Where a cached or shuffled block physically lives, how it is locked and located, and the serializers that turn objects into the bytes it stores. Swept in two halves — the block storage layer, and the serialization layer with the `StorageLevel` model.

!!! warning "Two claims in this sweep were wrong on first pass and are corrected here"

    The eviction order is **LRU**, not FIFO: `entries` is a `LinkedHashMap(32, 0.75f, true)` where the third argument is `accessOrder`, and `getBytes`/`getValues` call `entries.get`, which reorders. A first-pass reading of the iteration loop alone suggested insertion order and was wrong. Verified at `MemoryStore.scala:93`, `:389` and `:400`.

---

## BlockManager initialization

**What it is:** an executor's `BlockManager` is constructed early but unusable until `initialize(appId)` runs, because the app id only exists after the scheduler registers. It registers with the external shuffle service **before** the driver (SPARK-39647 — the service needs merge-dir metadata before the driver may pick this block manager as a push merger). The driver returns a *different* `BlockManagerId`, with topology filled in, and that is the one the executor uses thereafter.

**Anchor files:**

- [BlockManager.scala:567](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/BlockManager.scala#L567) — `initialize(appId)`, deliberately not in the constructor
- [BlockManager.scala:587](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/BlockManager.scala#L587) — external shuffle service registration ordered first
- [BlockManager.scala:608](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/BlockManager.scala#L608) — `blockManagerId = idFromMaster`, the driver's version wins
- [BlockManager.scala:337](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/BlockManager.scala#L337) — `waitForShuffleManagerInit`: the block manager registers with the driver *before* the executor's `ShuffleManager` exists, so a shuffle-migration request arriving in that window would NPE

!!! info "A second, narrower initialization race"

    Registration order creates a window in which the driver believes this block manager can serve shuffle blocks but the executor's `ShuffleManager` is still null. Shuffle operations block on `waitForShuffleManagerInit` for up to `spark.storage.shuffleManager.initWaitingTimeout` and then throw `ShuffleManagerNotInitializedException` rather than dereferencing null. `BlockManagerDecommissioner` names that exception explicitly so a migration hitting the window is retried rather than treated as a lost block.

**Configs:** `spark.storage.replication.policy`, `spark.storage.replication.topologyMapper`, `spark.blockManager.port`, `spark.storage.shuffleManager.initWaitingTimeout`

**Maps to topics:** B1, E2

---

## The driver location registry

**What it is:** the driver keeps one authoritative `BlockId → Set[BlockManagerId]` map. `getLocationsAndStatus` — what a remote reader calls — deliberately prefers a *disk-resident replica on the requester's own host*, returning that peer's local dirs so the reader can open the file directly and skip the network.

**Anchor files:**

- [BlockManagerMasterEndpoint.scala:86](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/BlockManagerMasterEndpoint.scala#L86) — `blockLocations`, the whole registry
- [BlockManagerMasterEndpoint.scala:889](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/BlockManagerMasterEndpoint.scala#L889) — the same-host disk-block preference
- [BlockManagerMasterEndpoint.scala:69](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/BlockManagerMasterEndpoint.scala#L69) — `executorIdToLocalDirs`, a **bounded** Guava cache

!!! warning "The host-local shortcut stops firing on large clusters"

    `executorIdToLocalDirs` has a hard `maximumSize` from `spark.storage.localDiskByExecutors.cacheSize`. Past that, entries evict, local dirs come back empty, and every read goes over Netty instead of opening a local file. Nothing logs at warn — it shows up only as latency.

**Configs:** `spark.storage.localDiskByExecutors.cacheSize`

**Maps to topics:** B1, E2, I6

---

## The read path and the iterator that holds a lock

**What it is:** `get` tries local, then remote. `getLocalValues` acquires a read lock **that is not released when the method returns** — a `CompletionIterator` releases it when the caller drains the data.

**Code path:** `get` → `getLocalValues` → `lockForReading` → memory | disk → `CompletionIterator(iter, releaseLock)` → (miss) `getRemoteValues`

**Anchor files:**

- [BlockManager.scala:999](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/BlockManager.scala#L999) — `lockForReading` at the top of `getLocalValues`
- [BlockManager.scala:1017](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/BlockManager.scala#L1017) — the `CompletionIterator` that releases on drain
- [BlockManager.scala:1770](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/BlockManager.scala#L1770) — `maybeCacheDiskBytesInMemory`, lazily allocating so an oversized block cannot OOM (SPARK-6076)

!!! warning "An un-drained cached iterator pins the block until the task ends"

    This is the single most surprising property of this layer, and it is the mechanism behind block-lock leak reports.

**Maps to topics:** I6, E1

---

## getOrElseUpdate — compute vs read

**What it is:** the decision behind `rdd.iterator` on a persisted RDD. On a cache miss it puts with `keepReadLock = true` and re-reads locally. If the put *fails*, the computed iterator is handed back to the caller — the data is consumed uncached.

**Anchor files:**

- [BlockManager.scala:1409](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/BlockManager.scala#L1409) — `getOrElseUpdateRDDBlock`
- [BlockManager.scala:1439](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/BlockManager.scala#L1439) — computation deliberately deferred so a decommissioning executor can replicate instead
- [BlockManager.scala:1481](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/BlockManager.scala#L1481) — `Right(iter)`: the put failed, caller processes uncached

!!! warning "This is how `MEMORY_ONLY` on an oversized partition degrades"

    The job succeeds, the Storage tab shows a partial cache fraction, and every later action recomputes. The only signal is a `logWarning` at `BlockManager.scala:1628`.

**Maps to topics:** I6, E1

---

## Cache visibility tracking

**What it is:** a correctness feature. A cached RDD block reported by a still-running task must not be read by others, because a speculative or failed attempt could have produced partial or indeterminate data. The driver holds `invisibleRDDBlocks` and publishes the block only when the task succeeds.

**Anchor files:**

- [BlockManagerMasterEndpoint.scala:92](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/BlockManagerMasterEndpoint.scala#L92) — the driver's `invisibleRDDBlocks`
- [BlockManagerMasterEndpoint.scala:268](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/BlockManagerMasterEndpoint.scala#L268) — on task failure blocks are left invisible, **not evicted** (SPARK-42582, open)
- [BlockManager.scala:1458](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/BlockManager.scala#L1458) — a TODO acknowledging that indeterminate RDDs can produce *different* replicas under one BlockId

**Maps to topics:** I6, A14

---

## The write path and memory-then-disk fallback

**What it is:** all puts funnel through `doPut`, which owns the lock lifecycle and cleanup. `doPutIterator` tries memory first **even when `useDisk` is set**, spilling only if the memory store hands the iterator back. Success is judged by re-reading the actual current status, not by absence of exception.

**Anchor files:**

- [BlockManager.scala:1593](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/BlockManager.scala#L1593) — `doPut`, the shared shell
- [BlockManager.scala:1639](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/BlockManager.scala#L1639) — the `finally`: on exception, remove the block *and* overwrite task metrics, retracting a half-announced block
- [BlockManager.scala:1736](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/BlockManager.scala#L1736) — `blockWasSuccessfullyStored` from the real status
- [BlockManager.scala:485](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/BlockManager.scala#L485) — the bytes path awaits replication with `Duration.Inf`

**Maps to topics:** I6, E1

---

## Block locking

**What it is:** a striped readers-writer lock keyed by `BlockId`, every acquisition attributed to a task attempt so locks can be reclaimed wholesale at task end. `BlockInfo` asserts the invariant: reader count ≥ 0, never both readers and a writer.

**Anchor files:**

- [BlockInfoManager.scala:86](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/BlockInfoManager.scala#L86) — the readers-xor-writer assertion
- [BlockInfoManager.scala:175](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/BlockInfoManager.scala#L175) — `Striped.lock(1024)`
- [BlockInfoManager.scala:459](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/BlockInfoManager.scala#L459) — `lockNewBlockForWriting`, first-writer-wins
- [BlockInfoManager.scala:524](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/BlockInfoManager.scala#L524) — SPARK-53807: re-check writer identity during concurrent release
- [Executor.scala:916](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/executor/Executor.scala#L916) — `spark.storage.exceptionOnPinLeak` chooses throw vs `logInfo`

!!! warning "A leaked lock is reported at INFO by default"

    A leaked read lock keeps a block un-evictable for the rest of the task, and the only default-visible trace is one INFO line. This is the sharpest invisible-degradation case in the subsystem.

**Configs:** `spark.storage.exceptionOnPinLeak`

**Maps to topics:** none — proposed as E15

---

## Block status reporting and re-registration

**What it is:** executors report every block state change. The driver's reply is a boolean meaning "I know you"; `false` triggers asynchronous re-registration, which re-reports every block. If re-registration is rejected outright, the executor **kills itself**.

**Anchor files:**

- [BlockManager.scala:686](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/BlockManager.scala#L686) — `reportAllBlocks`, documented as deliberately failing silently
- [BlockManager.scala:711](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/BlockManager.scala#L711) — `System.exit(BLOCK_MANAGER_REREGISTRATION_FAILED)`
- [BlockManagerMasterEndpoint.scala:703](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/BlockManagerMasterEndpoint.scala#L703) — SPARK-41360: re-registration accepted only if the scheduler still thinks the executor is alive

!!! note "`spark.storage.blockManagerTimeoutIntervalMs` has no direct consumer"

    It only supplies the default for `spark.network.timeoutInterval`. Setting it alone changes nothing in 4.2.0.

**Configs:** `spark.storage.blockManagerHeartbeatTimeoutMs`, `.blockManagerMasterDriverHeartbeatTimeoutMs`

**Maps to topics:** E2, B1

---

## Unreadable blocks and remote fetch

**What it is:** the registry can outlive the data. Three paths converge on "retract the location and let the driver find out". On the fetch side, locations are randomized then ordered same-host → same-rack → elsewhere, with failures counted twice — a running count that triggers a location refresh, and a total count capping attempts.

**Anchor files:**

- [BlockManager.scala:983](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/BlockManager.scala#L983) — `handleLocalReadFailure`: release, remove so unavailability propagates, throw
- [BlockManager.scala:1202](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/BlockManager.scala#L1202) — `sortLocations`, the host/rack tiers
- [BlockManager.scala:1271](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/BlockManager.scala#L1271) — location refresh after `spark.block.failures.beforeLocationRefresh` failures

!!! warning "A failed remote fetch of a cached block means recompute, not error"

    `fetchRemoteManagedBuffer` returning `None` surfaces as `get` returning `None`. For a cached RDD that is a silent recomputation; a cluster with widespread stale locations recomputes with only warn-level logs.

**Configs:** `spark.block.failures.beforeLocationRefresh`

**Maps to topics:** I6, A4, E2

---

## Replication and topology

**What it is:** `replicate` asks for peers, prioritizes them through a pluggable policy, and uploads until the replication factor is met or the failure budget is spent. Peers are cached for `spark.storage.cachedPeersTtl`.

**Anchor files:**

- [BlockManager.scala:1994](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/BlockManager.scala#L1994) — under-replication → `logWarning` + `return false`
- [BlockManager.scala:1749](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/BlockManager.scala#L1749) — the caller **discards** that boolean
- [BlockReplicationPolicy.scala:154](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/BlockReplicationPolicy.scala#L154) — `BasicBlockReplicationPolicy`, HDFS-style one-in-rack + one-out-of-rack
- [TopologyMapper.scala:56](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/TopologyMapper.scala#L56) — `DefaultTopologyMapper` returns `None` for every host

!!! warning "Rack-aware replication needs two configs, not one"

    With the default topology mapper every host reports no topology, so `BasicBlockReplicationPolicy` degrades to pure random placement and `sortLocations` skips its rack tier — with no warning. Setting `spark.storage.replication.policy` without `spark.storage.replication.topologyMapper` looks like it works.

!!! warning "Under-replication is not an error"

    `MEMORY_ONLY_2` can silently become `MEMORY_ONLY`: `replicate` returns `false` and the put path throws that value away.

**Configs:** `spark.storage.cachedPeersTtl`, `.replication.policy`, `.replication.topologyMapper`, `.replication.topologyFile`, `.maxReplicationFailures`, `.replication.proactive`

**Maps to topics:** I6, E2

---

## Disk layout and lifecycle

**What it is:** one file per block, hashed across `spark.local.dir` roots then across `spark.diskStore.subDirectories` per root — the second level exists purely to avoid enormous top-level inodes. A shutdown hook deletes local dirs on exit, skipped when an external shuffle service is serving them.

**Anchor files:**

- [DiskBlockManager.scala:95](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/DiskBlockManager.scala#L95) — `getFile`, the two-level hash, with a "keep in sync with `ExecutorDiskUtils`" warning
- [DiskBlockManager.scala:257](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/DiskBlockManager.scala#L257) — a single failed root dir is logged and **ignored**, silently reducing disk parallelism
- [DiskBlockManager.scala:355](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/DiskBlockManager.scala#L355) — `stop()` removes the shutdown hook to avoid leaking it
- [BlockManager.scala:225](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/BlockManager.scala#L225) — `deleteFilesOnStop = !externalShuffleServiceEnabled || isDriver`

!!! info "`spark.local.dir` is ignored under YARN"

    Container dirs win, as do `SPARK_LOCAL_DIRS`/`SPARK_EXECUTOR_DIRS`. A very common misconfiguration.

**Configs:** `spark.local.dir`, `spark.diskStore.subDirectories`, `spark.storage.cleanupFilesAfterExecutorExit`, `spark.storage.memoryMapThreshold`

**Maps to topics:** E2, A4

---

## Memory eviction

**What it is:** eviction walks `entries` in **access order — LRU** — accumulating candidates until enough space is freed, then drops them. It is strictly all-or-nothing: if the scan ends short, nothing is dropped and `0L` is returned.

**Anchor files:**

- [MemoryStore.scala:93](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/memory/MemoryStore.scala#L93) — `LinkedHashMap(32, 0.75f, true)`: the third argument is `accessOrder`, which is what makes this LRU
- [MemoryStore.scala:481](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/memory/MemoryStore.scala#L481) — `blockIsEvictable`: memory mode must match, **and a block of the same RDD is never evicted**
- [MemoryStore.scala:497](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/memory/MemoryStore.scala#L497) — non-blocking write lock, so blocks being read are silently skipped
- [MemoryStore.scala:557](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/memory/MemoryStore.scala#L557) — short scan: unlock everything, `logInfo("Will not store …")`, return `0L`

!!! info "An RDD cannot evict its own partitions"

    The self-eviction guard exists to avoid cyclic replacement for an RDD that does not fit. The consequence: caching an RDD larger than storage memory caches a *prefix* and silently drops the rest, rather than thrashing.

!!! warning "Eviction can return zero purely because candidates were locked"

    The write-lock attempt is non-blocking. Under a read-heavy workload, eviction may free nothing and the caller simply sees "not enough memory" — with one INFO line.

**Maps to topics:** I6, E1

---

## Decommission block migration and fallback storage

**What it is:** on decommission the executor starts two loops — an RDD-cache migrator and a shuffle migrator (one thread per peer). Shuffle blocks are **copied, not deleted**, so in-flight fetches are not broken. With no live peers, blocks can go to an external filesystem path that masquerades as a peer block manager.

**Anchor files:**

- [BlockManager.scala:305](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/BlockManager.scala#L305) — `checkShouldStore` rejects new non-broadcast blocks once decommissioning
- [BlockManagerDecommissioner.scala:405](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/BlockManagerDecommissioner.scala#L405) — local removal only *after* successful replication
- [BlockManagerDecommissioner.scala:479](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/BlockManagerDecommissioner.scala#L479) — `blocksMigrated` counts a **gave-up** migration as done
- [FallbackStorage.scala:119](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/FallbackStorage.scala#L119) — `FALLBACK_BLOCK_MANAGER_ID`, the placeholder peer
- [FallbackStorage.scala:88](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/FallbackStorage.scala#L88) — a non-`IndexShuffleBlockResolver` resolver logs a warning and copies **nothing**

!!! warning "A decommission can report success while losing shuffle data"

    Three paths convert loss into a clean shutdown: giving up counts as migrated, a target over `decommission.shuffleBlocks.maxDiskSize` rejects migrations, and the fallback path silently no-ops on a custom shuffle manager. The cost surfaces later as recomputation.

**Configs:** `spark.storage.decommission.*`, `.fallbackStorage.path`, `.fallbackStorage.cleanUp`

**Maps to topics:** E2, A4

---

## Serializer abstraction

**What it is:** a three-level abstraction existing purely for thread-safety. `Serializer` is the shareable factory; `SerializerInstance` is `@NotThreadSafe`; the streams wrap actual bytes. Neither format is self-delimiting, so end-of-data is signalled by throwing.

**Anchor files:**

- [Serializer.scala:44](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/serializer/Serializer.scala#L44) — the two-constructor and Java-serializable contract
- [Serializer.scala:160](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/serializer/Serializer.scala#L160) — `asIterator` treats `EOFException` as the normal stop
- [KryoSerializer.scala:331](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/serializer/KryoSerializer.scala#L331) — Kryo's "buffer underflow" message is **string-matched** and translated to `EOFException`

!!! warning "EOF is detected by matching an exception message"

    A genuinely truncated block whose failure happens to be a buffer underflow is indistinguishable from clean end-of-data, and terminates the iterator silently — yielding a short partition rather than an error.

**Maps to topics:** E11, E1

---

## JavaSerializer

**What it is:** wraps `ObjectOutputStream`. Its one Spark-specific behaviour is calling `reset()` every N objects: without it the stream's handle table holds a strong reference to every object ever written; with it, class descriptors must be re-written after each reset. 100 is the compromise.

**Anchor files:**

- [JavaSerializer.scala:45](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/serializer/JavaSerializer.scala#L45) — the counter and the `counterReset > 0` guard
- [JavaSerializer.scala:69](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/serializer/JavaSerializer.scala#L69) — `resolveClass` with a primitive-name fallback map
- [JavaSerializer.scala:162](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/serializer/JavaSerializer.scala#L162) — it never overrides `supportsRelocationOfSerializedObjects`, so Java is always `false`

**Configs:** `spark.serializer.objectStreamReset` (100)

**Maps to topics:** E11, E1

---

## NotSerializableException enrichment

**What it is:** the user-facing failure story. A raw `NotSerializableException` names only the offending class, useless when reached through three levels of closure capture. `SerializationDebugger` walks the object graph and appends a "Serialization stack".

**Anchor files:**

- [JavaSerializer.scala:48](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/serializer/JavaSerializer.scala#L48) — the catch, gated on `spark.serializer.extraDebugInfo`
- [SerializationDebugger.scala:37](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/serializer/SerializationDebugger.scala#L37) — `improveException`; if the debugger itself throws, the **original un-enriched** exception is returned
- [SerializationDebugger.scala:70](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/serializer/SerializationDebugger.scala#L70) — debugging auto-disables when a JVM system property is set

!!! info "Enrichment is Java-serializer-only"

    Kryo has no equivalent. A Kryo serialization failure gives a bare stack with no capture path — and closures are *always* Java-serialized, so this is the path that matters for "Task not serializable".

**Maps to topics:** E11, E1, I14

---

## Kryo construction, buffers, and overflow

**What it is:** eight configs read at construction. Both buffer sizes are validated eagerly and fatally — each must be strictly under 2048 MiB, checked at `SparkEnv` creation rather than at first serialization.

**Anchor files:**

- [KryoSerializer.scala:68](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/serializer/KryoSerializer.scala#L68) — the 2 GiB check → `INVALID_KRYO_SERIALIZER_BUFFER_SIZE`
- [KryoSerializer.scala:103](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/serializer/KryoSerializer.scala#L103) — `newKryoOutput` uses `math.max(bufferSize, maxBufferSize)`
- [KryoSerializer.scala:445](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/serializer/KryoSerializer.scala#L445) — overflow caught by `startsWith("Buffer overflow")` → `KRYO_BUFFER_OVERFLOW`
- [KryoSerializer.scala:284](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/serializer/KryoSerializer.scala#L284) — `KryoSerializationStream.writeObject` is deliberately **not** guarded

!!! info "A buffer larger than buffer.max is not rejected — it silently wins"

    And because only the buffer-based `serialize` path catches overflow, the same oversized object gives a friendly "increase spark.kryoserializer.buffer.max" error as a task or broadcast, and a different, rawer error when written to a shuffle stream.

**Configs:** `spark.kryoserializer.buffer`, `.buffer.max`, `spark.kryo.unsafe`

**Maps to topics:** E11, E1, A4

---

## Kryo pool and class registration

**What it is:** building a `Kryo` is expensive, so instances are pooled behind **soft references**. `newKryo()` is an ordered registration script where the order is load-bearing: references are set before the user registrator so it can override them, and Chill's `AllScalaRegistrar` runs *after* the user so Spark's own overrides win.

**Anchor files:**

- [KryoSerializer.scala:117](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/serializer/KryoSerializer.scala#L117) — `PoolWrapper` with `.softReferences.build`
- [KryoSerializer.scala:180](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/serializer/KryoSerializer.scala#L180) — user registration, wrapped in `FAILED_REGISTER_CLASS_WITH_KRYO`
- [KryoSerializer.scala:201](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/serializer/KryoSerializer.scala#L201) — Chill applied after the user registrator
- [KryoSerializer.scala:242](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/serializer/KryoSerializer.scala#L242) — `loadableSparkClasses` registration swallowing `NonFatal`
- [KryoSerializer.scala:143](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/serializer/KryoSerializer.scala#L143) — `setRegistrationRequired`

!!! warning "`registrationRequired=false` writes a full class name with every object"

    That is the silent cost of the default: no warning, no metric, just a fully-qualified class-name string per record in every shuffle block and cached partition. Set it `true` in development to enumerate what needs registering.

!!! info "A user registrator cannot override Chill"

    Chill runs last and wins for generic Scala types. Also, which Spark classes get registered depends on which jars are on the classpath — failures are swallowed silently.

**Configs:** `spark.kryo.pool`, `.classesToRegister`, `.registrator`, `.registrationRequired`, `.referenceTracking`

**Maps to topics:** E11, E1

---

## Relocation support and the serialized shuffle gate

**What it is:** the contract is that byte ranges for consecutive objects must be independently reorderable. It holds only for stateless serializers writing no stream header. Kryo's answer is **not** unconditional — it reflectively reads Kryo's private `autoReset` field, which a user registrator can turn off.

**Anchor files:**

- [Serializer.scala:67](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/serializer/Serializer.scala#L67) — the full contract with write/reorder pseudocode (SPARK-7311)
- [KryoSerializer.scala:265](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/serializer/KryoSerializer.scala#L265) — the `lazy val` delegating to `getAutoReset()`
- [KryoSerializer.scala:505](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/serializer/KryoSerializer.scala#L505) — reading the private field by reflection

!!! warning "All three consumers fail open and silent"

    Serialized shuffle write, batch fetch, and push-based shuffle all gate on this, and each logs at debug or warns about a bundle of conditions without naming the failing one. Switching to a serializer without relocation support loses `UnsafeShuffleWriter` and produces no error.

**Maps to topics:** E11, E1, A4

---

## SerializerManager auto-selection

**What it is:** the component that decides, per block and per shuffle, which serializer is *actually* used — and it silently ignores `spark.serializer` for a whole class of data. It holds its own privately-constructed `KryoSerializer` and uses it whenever the class tags are primitives, primitive arrays, or `String`.

**Anchor files:**

- [SerializerManager.scala:36](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/serializer/SerializerManager.scala#L36) — a `KryoSerializer` built unconditionally
- [SerializerManager.scala:84](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/serializer/SerializerManager.scala#L84) — `canUseKryo`
- [SerializerManager.scala:88](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/serializer/SerializerManager.scala#L88) — auto-pick disabled for stream blocks (SPARK-18617)
- [SparkEnv.scala:392](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/SparkEnv.scala#L392) — `closureSerializer` is hardcoded `new JavaSerializer(conf)`

!!! info "Two things `spark.serializer` does not control"

    **Closures are never Kryo** — the closure serializer is hardcoded, and there is no `spark.closure.serializer` in 4.2.0 (setting it does nothing). And a `RDD[Int]` shuffle already uses Kryo regardless of the config. So "I use Java serialization" and "I switched to Kryo" are both less true than they sound.

**Configs:** `spark.serializer`

**Maps to topics:** E11, E1, I6

---

## The StorageLevel model

**What it is:** four booleans plus a replication int, packed into one byte — disk=8, memory=4, offHeap=2, deserialized=1. Levels are interned so identity stays stable across deserialization.

**Anchor files:**

- [StorageLevel.scala:84](https://github.com/apache/spark/blob/v4.2.0/common/utils/src/main/scala/org/apache/spark/storage/StorageLevel.scala#L84) — `toInt` bit packing
- [StorageLevel.scala:153](https://github.com/apache/spark/blob/v4.2.0/common/utils/src/main/scala/org/apache/spark/storage/StorageLevel.scala#L153) — `MEMORY_ONLY = (false, true, false, true)` — the trailing flag is `deserialized`
- [StorageLevel.scala:161](https://github.com/apache/spark/blob/v4.2.0/common/utils/src/main/scala/org/apache/spark/storage/StorageLevel.scala#L161) — `OFF_HEAP = (true, true, true, false, 1)`
- [StorageLevelMapper.java:23](https://github.com/apache/spark/blob/v4.2.0/common/utils/src/main/java/org/apache/spark/storage/StorageLevelMapper.java#L23) — parsing is exact-match against 13 names

!!! info "Two names that mislead"

    **`MEMORY_ONLY` is deserialized** — it stores live objects with full JVM overhead and no serializer involvement, which is why Kryo tuning does nothing for it. And **`OFF_HEAP` sets `useDisk = true`**: it is memory + disk + offHeap + serialized, not memory-only.

**Maps to topics:** I6, E1

---

## The BlockId taxonomy

**What it is:** the vocabulary of the whole subsystem. `BlockId` is a sealed hierarchy of ~20 case classes, each with a `name` that is the block's identity everywhere it surfaces — the Storage tab, the executor logs, the on-disk filename, and the network protocol. It round-trips: `BlockId.apply(name)` parses the string back through a table of regexes.

**Anchor files:**

- [BlockId.scala:37](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/BlockId.scala#L37) — the sealed base and its `isRDD` / `isShuffle` / `isShuffleChunk` / `isBroadcast` predicates, which is how the rest of the code branches on block kind
- [BlockId.scala:55](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/BlockId.scala#L55) — `RDDBlockId(rddId, splitIndex)` → `rdd_5_12`, the name in the Storage tab
- [BlockId.scala:62](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/BlockId.scala#L62) — `ShuffleBlockId` and, below it, the batch, chunk, data, index, checksum, push and merged variants — seven distinct shuffle block kinds, which is why "shuffle block" is ambiguous in a log line
- [BlockId.scala:265](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/BlockId.scala#L265) — the regex table; every name format is defined here in one place
- [BlockId.scala:290](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/BlockId.scala#L290) — `apply(name)`; an unmatched name raises `UNRECOGNIZED_BLOCK_ID`
- [BlockId.scala:241](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/BlockId.scala#L241) — `TempLocalBlockId` / `TempShuffleBlockId`: UUID-named, the files you see accumulate in `spark.local.dir` during a spill

!!! info "The name is the filename"

    `DiskBlockManager.getFile` hashes `blockId.name`, so a block's identity string *is* its on-disk path component. That is what makes `rdd_5_12` greppable across the UI, the executor log and the local directory at once — and why a custom `BlockId` must produce a filesystem-safe name.

**Maps to topics:** I6, E1, I7

---

## DiskBlockObjectWriter

**What it is:** the single writer every shuffle write and every spill goes through. It layers a serialization stream over a compression stream over a buffered file stream, and exposes a **commit/revert** model: `commitAndGet()` closes out the current run of records and returns a `FileSegment` (offset + length), while `revertPartialWritesAndClose()` truncates the file back to the last committed position. Both are needed because many logical blocks share one physical file.

**Anchor files:**

- [DiskBlockObjectWriter.scala:236](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/DiskBlockObjectWriter.scala#L236) — `commitAndGet`: flush, record the position, return the `FileSegment` the index file will point at
- [DiskBlockObjectWriter.scala:274](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/DiskBlockObjectWriter.scala#L274) — `revertPartialWritesAndClose`, and the metrics **decrement** at L279–280: a reverted write is subtracted from bytes and records written, so shuffle-write metrics reflect committed data only
- [DiskBlockObjectWriter.scala:288](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/DiskBlockObjectWriter.scala#L288) — the truncate to `committedPosition`
- [DiskBlockObjectWriter.scala:293](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/DiskBlockObjectWriter.scala#L293) — `ClosedByInterruptException` on a killed task is logged without a stack trace (SPARK-28340), so a cancelled job does not fill the log with alarming traces
- [DiskBlockObjectWriter.scala:162](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/DiskBlockObjectWriter.scala#L162) — `open()`: the stream is opened lazily on first write, which is why a writer for an empty partition costs no file handle

!!! warning "Revert is best-effort, and a failed truncate is only logged"

    If the truncate throws anything other than `ClosedByInterruptException`, it is logged and swallowed — the file keeps its partial tail. That is safe only because the *index* still points at `committedPosition`, so the trailing bytes are unreferenced. It does mean a spill file on disk can be larger than the data it represents.

**Configs:** `spark.shuffle.file.buffer`, `spark.shuffle.sync`

**Maps to topics:** E1, A4

---

## DiskStore and encrypted blocks

**What it is:** the disk half of the block store. Writes go through a `WritableByteChannel` supplied by the caller, so the serializer and compressor write straight to the channel with no intermediate byte array. Reads return a `BlockData` whose implementation depends on encryption: plain blocks can be memory-mapped above a threshold, encrypted ones cannot and go through `EncryptedBlockData`, which decrypts on each read.

**Anchor files:**

- [DiskStore.scala:64](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/DiskStore.scala#L64) — `put(blockId)(writeFunc)`: the caller writes into the channel, the store records the resulting size
- [DiskStore.scala:52](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/DiskStore.scala#L52) — `blockSizes`, kept in a separate map because the file length is not the logical size once encryption padding is involved
- [DiskStore.scala:121](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/DiskStore.scala#L121) — the branch on `getIOEncryptionKey()`; `spark.io.encryption.enabled` decides which `BlockData` you get
- [DiskStore.scala:237](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/DiskStore.scala#L237) — `EncryptedBlockData`: no memory mapping, no zero-copy transfer

!!! info "IO encryption removes the zero-copy paths, here and in shuffle"

    An encrypted block cannot be memory-mapped or `transferTo`'d, so `spark.storage.memoryMapThreshold` becomes irrelevant and every read decrypts. This is the storage-side counterpart of the shuffle-side demotion the [shuffle & memory sweep](core-shuffle-memory.md) records — enabling `spark.io.encryption.enabled` quietly costs performance in two separate subsystems. The key management itself belongs to the [config & security sweep](core-config-security.md).

**Configs:** `spark.storage.memoryMapThreshold`, `spark.storage.memoryMapLimitForTests`, `spark.io.encryption.enabled`

**Maps to topics:** I6, E1, E2

---

## Memory-mapped buffer disposal

**What it is:** the reason Spark can memory-map block files at all without exhausting file descriptors. A `MappedByteBuffer`'s underlying mapping is released only when the buffer is garbage collected, which may be arbitrarily late. `StorageUtils.dispose` reaches through `sun.misc.Unsafe.invokeCleaner` to unmap it immediately.

**Anchor files:**

- [StorageUtils.scala:199](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/StorageUtils.scala#L199) — `bufferCleaner`: reflective access to `theUnsafe`, resolved once at class initialization
- [StorageUtils.scala:206](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/StorageUtils.scala#L206) — the comment stating the trade plainly: an *unsafe* API, but waiting for GC "may lead to the depletion of off-heap memory or huge numbers of open files"
- [StorageUtils.scala:214](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/StorageUtils.scala#L214) — `dispose` no-ops on anything that is not a `MappedByteBuffer`

!!! warning "Reading a disposed buffer is undefined behaviour, not an exception"

    The source says so directly: using the buffer after disposal "will cause errors". This is a JVM-level segfault risk rather than a Java exception, which is why disposal is confined to the block-store code paths that own the buffer's lifetime and is not exposed to users.

**Maps to topics:** E1, I6

---

## Block log writers

**What it is:** new in 4.2.0 ([SPARK-53755], with [SPARK-53975] building on it) — the `BlockManager` can now store **log output as blocks**. A `LogBlockWriter` serializes log lines to a temp file in the local dir and, on `save()`, registers it as a `LogBlockId`; `RollingLogWriter` rolls to a fresh block every 32 MiB. The first consumer is Python worker log capture, which is how a PySpark UDF's stdout/stderr becomes retrievable rather than lost on the executor.

**Code path:** `PythonWorkerLogCapture` → `blockManager.getRollingLogWriter(new PythonWorkerLogBlockIdGenerator(sessionId, workerId))` → `RollingLogWriter.writeLog(logLine)` → `LogBlockWriter` → temp file → `save()` → block registered under `python_worker_log_<time>_<executor>_<session>_<worker>`

**Anchor files:**

- [BlockManager.scala:1539](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/BlockManager.scala#L1539) — `getRollingLogWriter`, with the **hardcoded** 32 MiB (`33554432L`) default roll size
- [RollingLogWriter.scala:40](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/RollingLogWriter.scala#L40) — `shouldRollOver`: purely a byte count, no time-based roll
- [RollingLogWriter.scala:57](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/RollingLogWriter.scala#L57) — `writeLog(logEntry, removeBlockOnException)`: on a write failure the block is either dropped entirely or **left possibly corrupt**, chosen by the caller
- [LogBlockWriter.scala:34](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/LogBlockWriter.scala#L34) — the contract: `save` registers the block, `close` discards it; not safe for concurrent writes
- [LogBlockWriter.scala:65](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/LogBlockWriter.scala#L65) — `Files.createTempFile` with owner-only permissions ([SPARK-57920]) — logs can contain user data, so the temp file is not world-readable
- [BlockId.scala:227](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/BlockId.scala#L227) — `PythonWorkerLogBlockId(lastLogTime, executorId, sessionId, workerId)`, and the parsing regex at L288
- [PythonWorkerLogCapture.scala:44](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/api/python/PythonWorkerLogCapture.scala#L44) — one writer per worker id, held in a `ConcurrentHashMap`
- [BlockId.scala:179](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/BlockId.scala#L179) — `LogBlockType`, the `Enumeration` (`TEST`, `PYTHON_WORKER`) that types the whole feature; `LogBlockId.empty(logBlockType)` at L199 builds the placeholder id
- [LogBlockIdGenerator.scala:26](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/LogBlockIdGenerator.scala#L26) — the generator trait; `nextBlockId` (L40) is `final` and re-checks that the subclass produced an id of its own declared type, raising an internal error otherwise
- [LogLine.scala:30](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/LogLine.scala#L30) — the `LogLine` record: `eventTime`, `sequenceId`, `message`. `LogLine.getClassTag` (L37) maps a `LogBlockType` to the concrete case class, which is how the reader deserializes a block it did not write

!!! info "The log-block feature is typed end to end, and extending it means three edits"

    A new kind of log block needs an entry in the `LogBlockType` enumeration, a `LogLine` subclass registered in `getClassTag`, and a `LogBlockIdGenerator`. `getClassTag` throws a plain `RuntimeException` on an unknown type rather than a Spark error class — the one un-classed failure in an otherwise 4.2.0-era feature.

!!! info "Where PySpark worker logs go in 4.2.0"

    Before this, a `print()` or `logging` call inside a Python UDF went to the worker's stderr on the executor and was effectively unreachable unless you had node access. Capturing it into a block gives it a `BlockId`, a session and worker scope, and the same lifecycle as any other block. `spark.executor.python.worker.log.details` controls how much detail is captured. This is the concrete answer to "why can't I see my UDF's logs", and it is new enough that no book or blog covers it.

!!! warning "Log blocks live in the local dir and roll only on size"

    The roll size is a default parameter on `getRollingLogWriter`, not a `spark.*` config, so it cannot be tuned from configuration. A chatty worker produces 32 MiB blocks in `spark.local.dir`; a quiet one holds a partial block open, since there is no time-based roll.

**Configs:** `spark.executor.python.worker.log.details`

**Maps to topics:** E3, I3

---

## BlockManagerMaster — the driver proxy every storage operation goes through

**What it is:** the class the group's scope names first, and the one thing on every executor that talks to the driver about blocks. It holds no state: each method wraps one `BlockManagerMessages` case class and sends it to `driverEndpoint`. What matters is *how* it sends them — nearly every call is `askSync`, so registering a block manager, reporting a block status change, looking up a location, or asking for peers all block the calling thread on a round trip to a single driver-side endpoint.

**Code path:** `BlockManager` → `BlockManagerMaster.<op>` → `askSync(msg)` → `BlockManagerMasterEndpoint` (locations, registry) **or** `BlockManagerMasterHeartbeatEndpoint` (liveness only)

**Anchor files:**

- [BlockManagerMaster.scala:34](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/BlockManagerMaster.scala#L34) — two endpoint refs, not one: `driverEndpoint` and `driverHeartbeatEndPoint`
- [BlockManagerMaster.scala:77](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/BlockManagerMaster.scala#L77) — `registerBlockManager`: the executor sends a topology-less `BlockManagerId` and the driver returns a fleshed-out one, which is the exchange behind `blockManagerId = idFromMaster`
- [BlockManagerMaster.scala:95](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/BlockManagerMaster.scala#L95) — a reply carrying `INVALID_EXECUTOR_ID` is how the driver *refuses* a re-registration; the `assert` says this may only happen on the re-register path
- [BlockManagerMaster.scala:195](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/BlockManagerMaster.scala#L195) — `removeRdd`, `removeShuffle` and `removeBroadcast` all return a `Future` that is awaited only when `blocking = true`; the failure handler merely `logWarning`s
- [BlockManagerMaster.scala:43](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/BlockManagerMaster.scala#L43) — that wait is bounded by `spark.cleaner.referenceTracking.blocking.timeout`, defaulting to 120s
- [BlockManagerMaster.scala:256](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/BlockManagerMaster.scala#L256) — `getBlockStatus` fans out through `Future`s with an explicit comment that the master endpoint must not block waiting on a block manager that may itself be waiting on the master
- [BlockManagerMaster.scala:314](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/BlockManagerMaster.scala#L314) — `tell`: a "one-way" message that is actually a synchronous ask expecting `true`, and throws if not
- [BlockManagerMessages.scala:29](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/BlockManagerMessages.scala#L29) — the protocol split into two sealed traits, `ToBlockManagerMasterStorageEndpoint` (driver → executor) and `ToBlockManagerMaster` (executor → driver, L67)
- [BlockManagerMasterHeartbeatEndpoint.scala:27](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/BlockManagerMasterHeartbeatEndpoint.scala#L27) — the class comment: heartbeats were split out of `BlockManagerMasterEndpoint` "due to performance consideration"
- [BlockManagerMasterHeartbeatEndpoint.scala:50](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/BlockManagerMasterHeartbeatEndpoint.scala#L50) — `heartbeatReceived` returns `false` for an unknown block manager, which is the trigger for the re-registration path

!!! warning "The heartbeat endpoint shares the master endpoint's mutable state"

    `BlockManagerMasterHeartbeatEndpoint` is handed the *same* `mutable.Map[BlockManagerId, BlockManagerInfo]` the master endpoint mutates, and calls `updateLastSeenMs()` on it from its own thread. The split buys latency isolation — a heartbeat no longer queues behind a slow `GetLocations` — at the cost of two endpoints writing one map. It is why stopping the master stops both endpoints ([BlockManagerMaster.scala:300](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/BlockManagerMaster.scala#L300)) and why only one of the two stops is checked for success.

!!! info "Block bookkeeping is a synchronous, single-endpoint protocol"

    Every cached block, every shuffle block, and every removal reaches one driver endpoint, and the executor waits for the reply. That is the structural reason storage bookkeeping shows up as driver pressure on wide jobs, and the reason `getBlockStatus` and `getMatchingBlockIds` carry "should only be used for testing" warnings in their scaladoc — they broadcast a query to every block manager.

**Configs:** `spark.storage.blockManagerHeartbeatTimeoutMs`, `spark.storage.blockManagerMasterDriverHeartbeatTimeoutMs`, `spark.cleaner.referenceTracking.blocking.timeout`

**Maps to topics:** B1, E2, E1

---

## The executor's storage endpoint and asynchronous removal

**What it is:** the other half of the protocol — the executor-side `RpcEndpoint` that receives driver commands. It is an `IsolatedThreadSafeRpcEndpoint`, so it owns its inbox rather than sharing the dispatcher's, and every command that *removes* blocks is pushed onto a separate cached thread pool instead of being handled inline.

**Anchor files:**

- [BlockManagerStorageEndpoint.scala:38](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/BlockManagerStorageEndpoint.scala#L38) — `IsolatedThreadSafeRpcEndpoint`: a slow block removal cannot stall unrelated RPC traffic
- [BlockManagerStorageEndpoint.scala:40](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/BlockManagerStorageEndpoint.scala#L40) — `newDaemonCachedThreadPool("block-manager-storage-async-thread-pool", 100)`, the pool that actually runs removals
- [BlockManagerStorageEndpoint.scala:106](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/BlockManagerStorageEndpoint.scala#L106) — `doAsync`: the reply is sent from the future's callback, so the driver's `askSync` is what waits, not the endpoint thread
- [BlockManagerStorageEndpoint.scala:72](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/BlockManagerStorageEndpoint.scala#L72) — `DecommissionBlockManager` is handled **synchronously**, unlike every other mutating command
- [BlockManagerStorageEndpoint.scala:95](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/BlockManagerStorageEndpoint.scala#L95) — `MarkRDDBlockAsVisible`, the executor end of cache-visibility tracking, with the two cases spelled out in comments
- [BlockManagerStorageEndpoint.scala:86](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/BlockManagerStorageEndpoint.scala#L86) — `TriggerThreadDump` and `TriggerHeapHistogram` (L89): the executor thread dump and heap histogram in the Spark UI arrive over the *storage* endpoint
- [BlockManagerStorageEndpoint.scala:124](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/BlockManagerStorageEndpoint.scala#L124) — `onStop` calls `shutdownNow()`, so in-flight removals are interrupted at executor shutdown

!!! info "The UI's thread dump button rides the block-manager protocol"

    `TriggerThreadDump` / `TriggerHeapHistogram` are `ToBlockManagerMasterStorageEndpoint` messages. There is no separate diagnostics channel: if an executor's storage endpoint is wedged, the thread dump that would tell you why is unavailable too.

!!! warning "A removal reply can outlive the executor"

    `doAsync` replies from a pool thread; `onStop` interrupts that pool. An executor stopping mid-removal therefore fails the driver's `askSync` rather than reporting partial progress — which is safe for `removeRdd` (the driver drops the locations anyway) but means the 100-thread bound is a real ceiling: a driver unpersisting many RDDs at once queues past it silently.

**Maps to topics:** E2, E1

---

## BlockManagerId — identity, interning, and two sentinel ids

**What it is:** the four-field identity (`executorId`, `host`, `port`, `topologyInfo`) that every location, peer list and fetch request is expressed in. It is a `@DeveloperApi` with private constructors: instances may only be made through `apply`, which routes them through a bounded interning cache so that the millions of copies arriving over the wire collapse onto one object per real block manager.

**Anchor files:**

- [BlockManagerId.scala:38](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/BlockManagerId.scala#L38) — private constructors, `var` fields, and `Externalizable` — hand-rolled serialization because this type crosses the wire constantly
- [BlockManagerId.scala:89](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/BlockManagerId.scala#L89) — `readResolve` re-interns on every deserialization, which is what keeps identity stable
- [BlockManagerId.scala:139](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/BlockManagerId.scala#L139) — the Guava cache with a hardcoded `maximumSize(10000)` and the comment justifying it at ~48 bytes an entry
- [BlockManagerId.scala:149](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/BlockManagerId.scala#L149) — `SHUFFLE_MERGER_IDENTIFIER = "shuffle-push-merger"`: a push-based-shuffle merger location is represented as a `BlockManagerId` with a fake executor id
- [BlockManagerId.scala:151](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/BlockManagerId.scala#L151) — `INVALID_EXECUTOR_ID = "invalid"`, the rejection sentinel `registerBlockManager` checks for
- [BlockManagerId.scala:67](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/BlockManagerId.scala#L67) — `isDriver` is decided purely by the executor-id string, via `SparkContext.isDriver`

!!! info "Three different things are modelled as a BlockManagerId"

    A real executor, the driver, and a **shuffle push merger** — plus `FallbackStorage.FALLBACK_BLOCK_MANAGER_ID`, which stands in for a filesystem path. Any code reading a location list must therefore treat `BlockManagerId` as "somewhere blocks can be", not "an executor"; that is exactly the assumption `ShuffleBlockFetcherIterator` encodes when it filters the fallback id out of its remote-fetch set.

!!! warning "topologyInfo is part of equality"

    Two ids for the same host and port are *unequal* if one carries topology and the other does not. That is why registration returns a new id and the executor must adopt it — keeping the pre-registration id would make the executor invisible to peer lookups keyed on the topology-bearing one.

**Maps to topics:** B1, E2

---

## Unroll memory — materialising a block whose size is unknown

**What it is:** the mechanism behind the whole `MEMORY_ONLY` story, and the third memory accounting category after execution and storage. Caching an iterator requires knowing how big it will be, which is only knowable by consuming it — so the `MemoryStore` reserves a small budget up front, consumes the iterator while re-estimating size every `spark.storage.unrollMemoryCheckPeriod` elements, and requests more memory geometrically as the estimate grows. Only when the block is complete is the unroll reservation swapped for a storage reservation, under one lock, atomically.

**Code path:** `putIteratorAsValues` / `putIteratorAsBytes` → `putIterator` → `reserveUnrollMemoryForThisTask` → grow loop → `entryBuilder.build()` → release unroll + acquire storage (atomic) → `entries.put` — or, on failure, `Left(unrollMemoryUsedByThisBlock)` → `PartiallyUnrolledIterator`

**Anchor files:**

- [MemoryStore.scala:97](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/memory/MemoryStore.scala#L97) — `onHeapUnrollMemoryMap` / `offHeapUnrollMemoryMap`, keyed by **task attempt id**: unroll memory is charged per task, not per block
- [MemoryStore.scala:103](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/memory/MemoryStore.scala#L103) — `spark.storage.unrollMemoryThreshold`, the 1 MiB reserved before a single element is read
- [MemoryStore.scala:111](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/memory/MemoryStore.scala#L111) — a startup warning when total storage memory is smaller than that initial threshold
- [MemoryStore.scala:233](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/memory/MemoryStore.scala#L233) — `elementsUnrolled % memoryCheckPeriod == 0`: the size estimate is only refreshed every 16 elements by default
- [MemoryStore.scala:237](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/memory/MemoryStore.scala#L237) — `currentSize * memoryGrowthFactor - memoryThreshold`: the request is sized to 1.5× the *current* estimate, not to the shortfall
- [MemoryStore.scala:252](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/memory/MemoryStore.scala#L252) — SPARK-45025: an interrupted thread abandons the unroll and returns the memory, so a killed task is not later killed again by the task reaper
- [MemoryStore.scala:273](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/memory/MemoryStore.scala#L273) — the hand-off, inside `memoryManager.synchronized`, with `assert(success, "transferring unroll memory to storage memory failed")`
- [MemoryStore.scala:586](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/memory/MemoryStore.scala#L586) — `reserveUnrollMemoryForThisTask`; `releaseUnrollMemoryForThisTask` (L608) defaults to releasing the task's *entire* allocation
- [MemoryStore.scala:777](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/memory/MemoryStore.scala#L777) — `PartiallyUnrolledIterator`: the failure result, which holds the reservation until `unrolled` is drained (L795) or `close()` is called (L814)
- [MemoryStore.scala:850](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/memory/MemoryStore.scala#L850) — `PartiallySerializedBlock`, the `putIteratorAsBytes` equivalent, which registers a task-completion listener (L867) precisely because a caller that abandons it would leak direct buffers
- [MemoryStore.scala:668](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/memory/MemoryStore.scala#L668) — `logUnrollFailureMessage`, the source of "Not enough space to cache … in memory!", followed by a memory-usage line naming `numTasksUnrolling`

!!! warning "\"Not enough space to cache\" is an unroll failure, and the memory it needed is invisible in the UI"

    Unroll memory is scratch space shared across tasks: `logMemoryUsage` reports it separately from block memory precisely because the Storage tab does not. With N cores per executor each unrolling a partition, N × the partition size must fit in storage memory *simultaneously* even though only one partition's worth ever appears as cached. This is the most common way a cache that "should fit" does not.

!!! info "The growth factor makes over-reservation deliberate"

    Requesting `currentSize × 1.5` means an unroll typically holds ~50% more memory than the data it has read so far, trading waste for fewer round trips through `MemoryManager`. Lowering `spark.storage.unrollMemoryGrowthFactor` toward 1.0 reduces the overshoot but raises the number of acquire attempts, each of which can trigger eviction.

!!! warning "An abandoned PartiallyUnrolledIterator holds unroll memory for the rest of the task"

    The reservation is released when the partially-unrolled prefix is fully iterated, not when the put fails. A caller that takes the `Left` result and stops early — a `take`, a short-circuiting operator — pins that memory until task end. `close()` exists for this and is the reason the `getOrElseUpdate` failure path is written the way it is.

**Configs:** `spark.storage.unrollMemoryThreshold` (1 MiB), `spark.storage.unrollMemoryCheckPeriod` (16), `spark.storage.unrollMemoryGrowthFactor` (1.5)

**Maps to topics:** none — proposed as E51

---

## BlockManagerManagedBuffer — read locks expressed as Netty refcounts

**What it is:** the adapter that makes block locking work across the network boundary. When a remote executor fetches a block, the served bytes are wrapped in a `ManagedBuffer` whose `retain`/`release` — the network layer's reference counting — are wired directly to `lockForReading` / `unlock` on the `BlockInfoManager`. The block stays locked exactly as long as Netty holds a reference to it.

**Anchor files:**

- [BlockManagerManagedBuffer.scala:37](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/BlockManagerManagedBuffer.scala#L37) — the class comment naming it "a wrapper / bridge to connect the BlockManager's notion of read locks to the network layer's notion of retain / release counts"
- [BlockManagerManagedBuffer.scala:56](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/BlockManagerManagedBuffer.scala#L56) — `retain` takes a **non-blocking** read lock and `assert`s it succeeded
- [BlockManagerManagedBuffer.scala:63](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/BlockManagerManagedBuffer.scala#L63) — `release` unlocks first, then disposes the `BlockData` when the count reaches zero and `dispose` was requested
- [BlockManagerManagedBuffer.scala:42](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/BlockManagerManagedBuffer.scala#L42) — `unlockOnDeallocate`, the escape hatch for callers that manage the lock themselves

!!! warning "A retain that cannot take the lock trips an assertion, not a retry"

    `retain` uses `blocking = false` and asserts the result is defined. It is sound only because a caller retaining an already-retained buffer holds a read lock already, and read locks are shared — but it means any future path that retains without an existing read lock fails as an `AssertionError` on a Netty thread rather than as a fetch failure.

!!! info "This is where a network stall becomes an un-evictable block"

    Because the lock's lifetime is the buffer's refcount, a slow or stuck remote reader keeps the served block read-locked, and [`MemoryStore.evictBlocksToFreeSpace`](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/memory/MemoryStore.scala#L497) skips blocks whose write lock it cannot take. Serving a block to a slow peer therefore reduces effective storage memory for the duration — the network-side twin of the un-drained-iterator case.

**Maps to topics:** E15, E1

---

## Avro schema registration and GenericAvroSerializer

**What it is:** the one place Spark ships a custom Kryo serializer for a third-party type. Avro `GenericContainer` records carry their schema, and naively Kryo would write the full schema text with every record. `GenericAvroSerializer` instead writes a 64-bit *parsing fingerprint* when the schema was pre-registered, and a compressed schema when it was not. Registration happens through a dynamic config namespace that no `ConfigEntry` declares.

**Code path:** `SparkConf.registerAvroSchemas(schema*)` → `avro.schema.<fingerprint64>` conf keys → `conf.getAvroSchema` → `KryoSerializer.avroSchemas` → `kryo.register(clazz, new GenericAvroSerializer(avroSchemas))`

**Anchor files:**

- [SparkConf.scala:450](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/SparkConf.scala#L450) — `avroNamespace = "avro.schema."` — note there is **no `spark.` prefix**
- [SparkConf.scala:458](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/SparkConf.scala#L458) — `registerAvroSchemas` keys each schema by `SchemaNormalization.parsingFingerprint64`
- [KryoSerializer.scala:98](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/serializer/KryoSerializer.scala#L98) — `conf.getAvroSchema`, read once at construction; L173 registers the serializer for `GenericRecord` and friends
- [GenericAvroSerializer.scala:51](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/serializer/GenericAvroSerializer.scala#L51) — **six** unsynchronized `mutable.HashMap` caches (compress, decompress, writer, reader, fingerprint, schema), safe only because a `SerializerInstance` is single-threaded
- [GenericAvroSerializer.scala:66](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/serializer/GenericAvroSerializer.scala#L66) — the codec is `lazy` with a comment explaining that eager initialization would make `KryoSerializer` non-serializable
- [GenericAvroSerializer.scala:112](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/serializer/GenericAvroSerializer.scala#L112) — the branch: registered → one boolean plus one long; unregistered → the schema, compressed with the *`spark.io.compression.codec`* codec
- [GenericAvroSerializer.scala:141](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/serializer/GenericAvroSerializer.scala#L141) — `ERROR_READING_AVRO_UNKNOWN_FINGERPRINT`: a reader that lacks a schema the writer had registered fails hard

!!! warning "Registering schemas on only one side is a decode failure, not a slowdown"

    The fingerprint is written whenever the *writer's* conf knows the schema. If the schema was registered after the executors' `SparkConf` was fixed, or only in the driver's conf, the reader hits `ERROR_READING_AVRO_UNKNOWN_FINGERPRINT` and the task fails. Registering nowhere is safe (schemas travel compressed); registering asymmetrically is not.

!!! info "This is a Kryo-only optimisation, and it is invisible to the config catalog"

    `avro.schema.*` is set programmatically and has no `ConfigEntry`, so it appears in no config listing, no `--conf` documentation, and no drift check. It also does nothing under the Java serializer — `spark.serializer` must be `KryoSerializer` for any of this to run.

**Configs:** `avro.schema.<fingerprint>` (dynamic, undeclared), `spark.serializer`, `spark.io.compression.codec` (via `CompressionCodec.createCodec`)

**Maps to topics:** E11, I10

---

## SerializerHelper and chunk-sized serialization

**What it is:** a two-method object that exists because serializing a large object into a single `Array[Byte]` caps out at 2 GiB and fragments the heap. It serializes into a `ChunkedByteBuffer` whose chunk size is derived from a caller-supplied *estimate*, so a broadcast or task binary that is expected to be large is written in fewer, bigger chunks.

**Anchor files:**

- [SerializerHelper.scala:35](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/serializer/SerializerHelper.scala#L35) — `serializeToChunkedBuffer(serializerInstance, obj, estimatedSize = -1)`
- [SerializerHelper.scala:39](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/serializer/SerializerHelper.scala#L39) — `ChunkedByteBuffer.estimateBufferChunkSize(estimatedSize)`; `-1` means "no hint, use the default chunk size"
- [SerializerHelper.scala:48](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/serializer/SerializerHelper.scala#L48) — `deserializeFromChunkedBuffer`, the symmetric read

!!! info "The estimate is a hint, not a limit"

    Passing a wrong `estimatedSize` cannot fail a serialization — it only changes chunk granularity, and therefore the number of buffer allocations and how much of the last chunk is wasted. Callers that know the size (a broadcast of a known-size value) pass it; callers that do not pass `-1`.

**Maps to topics:** E11, E1

---

## The storage status model and the BlockManager metric source

**What it is:** how everything outside the block layer — the Storage tab, the REST API, the metrics system — sees storage. Three types carry it: `RDDInfo` (per-RDD cache summary), `BlockUpdatedInfo` (the event-log form of one block state change), and `StorageStatus` (a per-block-manager aggregate the driver rebuilds on demand). `BlockManagerSource` exposes eleven gauges over the last of these.

**Anchor files:**

- [RDDInfo.scala:27](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/RDDInfo.scala#L27) — the `@DeveloperApi` record; `isCached` (L43) requires both non-zero size **and** at least one cached partition
- [RDDInfo.scala:59](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/RDDInfo.scala#L59) — `fromRdd`: the RDD's name falls back to its class name, and the call site is long- or short-form depending on `spark.eventLog.callsite.longForm`
- [BlockUpdatedInfo.scala:28](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/BlockUpdatedInfo.scala#L28) — the `@DeveloperApi` projection of an `UpdateBlockInfo` message; this is what a `SparkListenerBlockUpdated` event carries
- [StorageUtils.scala:37](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/StorageUtils.scala#L37) — `StorageStatus`, documented as **not thread-safe** and assuming its inputs are immutable
- [StorageUtils.scala:156](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/StorageUtils.scala#L156) — `updateStorageInfo`, which maintains the per-RDD and non-RDD running totals so the gauges are O(1) reads
- [BlockManagerMasterEndpoint.scala:617](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/BlockManagerMasterEndpoint.scala#L617) — `storageStatus` constructs a *fresh* `StorageStatus` for **every** registered block manager, copying its whole block map, on every request
- [BlockManagerSource.scala:29](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/BlockManagerSource.scala#L29) — `registerGauge`: each gauge calls `blockManager.master.getStorageStatus` and divides by 1024 twice
- [SparkContext.scala:725](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/SparkContext.scala#L725) — the source is registered on the driver only

!!! warning "Eleven gauges, eleven driver RPCs, and a full rebuild each time"

    `BlockManagerSource` registers eleven gauges and **each one independently** issues `getStorageStatus`, an `askSync` to the block-manager master endpoint that rebuilds a `StorageStatus` — including a copy of the block map — for every block manager in the cluster. One metrics poll is therefore eleven synchronous driver round trips whose cost scales with `executors × blocks`. On a large, heavily-cached application this is a measurable driver cost paid purely for the `BlockManager.*` metrics, and nothing caches or batches it.

!!! info "The metrics are in MiB and are integer-divided"

    `func(...) / 1024 / 1024` on a `Long`: anything under 1 MiB reports as `0`, and the gauges are `Gauge[Long]`, so there is no fractional resolution. `maxMem_MB` and friends cannot be compared against byte-valued configs without accounting for that truncation.

**Configs:** `spark.eventLog.callsite.longForm` (read by `RDDInfo.fromRdd`)

**Maps to topics:** E3, I7, I6

---

## Breadth check 1 — the config slice

Slice — every `core` config in `configs/catalog.yaml` matching:

```
^spark\.(storage\.|block\.|diskStore\.|serializer\.|kryo|local\.dir|blockManager\.|shuffle\.file\.buffer|shuffle\.sync|io\.encryption)
```

**45 keys.** All 45 are now written literally in a `**Configs:**` line. The pre-re-sweep page scored 23/45 on a literal count, but most of that gap was shorthand (`spark.storage.decommission.*`, `` `.replication.policy` ``) rather than missing coverage — expanded below, per the "never a family or a suffix" rule.

| Keys | Concept | Note |
|---|---|---|
| `spark.storage.unrollMemoryThreshold`, `spark.storage.unrollMemoryCheckPeriod`, `spark.storage.unrollMemoryGrowthFactor` | unroll memory | **the real gap this check found** — three keys tied to no concept on the previous page |
| `spark.storage.blockManagerMasterDriverHeartbeatTimeoutMs` | BlockManagerMaster RPC plane | the heartbeat endpoint's own ask timeout, distinct from `spark.storage.blockManagerHeartbeatTimeoutMs` |
| `spark.storage.decommission.enabled`, `.rddBlocks.enabled`, `.shuffleBlocks.enabled`, `.shuffleBlocks.maxThreads`, `.shuffleBlocks.maxDiskSize`, `.maxReplicationFailuresPerBlock`, `.replicationReattemptInterval`, `.fallbackStorage.path`, `.fallbackStorage.cleanUp` | decommission block migration and fallback storage | previously abbreviated to `spark.storage.decommission.*`; expanded |
| `spark.storage.replication.policy`, `.replication.topologyMapper`, `.replication.topologyFile`, `.replication.proactive`, `spark.storage.maxReplicationFailures`, `spark.storage.cachedPeersTtl` | replication and topology | previously abbreviated |
| `spark.kryo.classesToRegister`, `.registrator`, `.registrationRequired`, `.referenceTracking`, `spark.kryo.pool` | Kryo pool and class registration | previously abbreviated |
| `spark.kryoserializer.buffer`, `.buffer.max`, `spark.kryo.unsafe` | Kryo construction and buffers | |
| `spark.serializer.objectStreamReset` | JavaSerializer | |
| `spark.serializer.extraDebugInfo` | NotSerializableException enrichment | |
| `spark.storage.memoryMapThreshold`, `spark.storage.memoryMapLimitForTests`, `spark.io.encryption.enabled` | DiskStore and encrypted blocks | |
| `spark.local.dir`, `spark.diskStore.subDirectories`, `spark.storage.cleanupFilesAfterExecutorExit` | disk layout and lifecycle | |
| `spark.shuffle.file.buffer`, `spark.shuffle.sync` | DiskBlockObjectWriter | shuffle-named but read only by this writer |
| `spark.storage.localDiskByExecutors.cacheSize` | driver location registry | |
| `spark.block.failures.beforeLocationRefresh` | unreadable blocks and remote fetch | |
| `spark.storage.exceptionOnPinLeak` | block locking | |
| `spark.blockManager.port`, `spark.storage.shuffleManager.initWaitingTimeout` | BlockManager initialization | |
| `spark.storage.blockManagerHeartbeatTimeoutMs`, `spark.storage.blockManagerTimeoutIntervalMs` | block status reporting and re-registration | the second has no direct consumer (see that concept) |

**Owned by another group:**

- `spark.io.encryption.keySizeBits`, `spark.io.encryption.keygen.algorithm` — key *generation*, owned by [config & security](core-config-security.md). Only `spark.io.encryption.enabled` is read here, as a branch.

**Configs this group reads that are not in the catalog** — invisible to `check_drift.py --sweeps`, so only findable by eye:

- **`avro.schema.<fingerprint64>`** — a dynamic namespace with **no `spark.` prefix**, written by `SparkConf.registerAvroSchemas` and read by `KryoSerializer` ([SparkConf.scala:450](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/SparkConf.scala#L450)). No `ConfigEntry` declares it.
- `spark.cleaner.referenceTracking.blocking.timeout` — declared in `core` but outside this slice's prefixes; `BlockManagerMaster` reads it directly as the bound on blocking `removeRdd`/`removeShuffle`/`removeBroadcast`.
- `spark.io.compression.codec` — read indirectly by `GenericAvroSerializer` through `CompressionCodec.createCodec`.
- `spark.eventLog.callsite.longForm` — read by `RDDInfo.fromRdd`.
- The 32 MiB log-block roll size is **not a config at all** — a default parameter on `BlockManager.getRollingLogWriter`.

## Breadth check 2 — the packages

Scope: `storage/` (`BlockManager`, `BlockManagerMaster`, `DiskBlockManager`; `StorageLevel` now in `common/utils`), `serializer/` (`KryoSerializer` vs `JavaSerializer`). Walked by hand, including the nested `storage/memory/` sub-package.

| Package | Files | Cited | |
|---|---|---|---|
| `core/…/storage/` | 32 | 24 | 75% |
| `core/…/storage/memory/` | 1 | 1 | 100% |
| `core/…/serializer/` | 9 | 6 | 67% |
| `common/utils/…/storage/` (scala + java) | 2 | 2 | 100% |

**Classes the scope names by hand:** `BlockManager` ✅, `BlockManagerMaster` ✅ *(uncited before this re-sweep — the standing `--sweeps` failure)*, `DiskBlockManager` ✅, `StorageLevel` ✅, `KryoSerializer` ✅, `JavaSerializer` ✅.

**Uncited, and why:**

- `ShuffleBlockFetcherIterator.scala` (1682 lines) and `PushBasedFetchHelper.scala` (360) — **swept by [core — shuffle-memory](core-shuffle-memory.md)**, which cites them 19 times. They sit in the `storage/` package but are the shuffle *read* data plane; leaving them there is the right carve, and duplicating them here would put one mechanism on two pages.
- `BlockManagerSource.scala` — also cited by [core — monitoring](core-monitoring.md) as a metric source. Covered here from the storage side (what the gauges cost); monitoring owns the metrics-system registration.
- `BlockException.scala`, `BlockNotFoundException.scala`, `BlockSavedOnDecommissionedBlockManagerException.scala`, `ShuffleManagerNotInitializedException.scala` — four one-line exception types. Two are named in prose on this page (the decommission and shuffle-manager-init concepts); `BlockException` has no thrower left in `core` and `BlockNotFoundException` is thrown from the network layer's block handler. Plumbing, recorded rather than written up.
- `FileSegment.scala` — a 3-field value type; covered inside the `DiskBlockObjectWriter` concept, which is the only thing that produces one.
- `serializer/package.scala`, `serializer/package-info.java` — package documentation, no code.

**Deliberately not covered:** nothing else. The group is now genuinely walked; the remaining uncited files are the two shuffle-read classes above (another group's) and boilerplate.

**A file no page anywhere cites, found by this walk:** none outside the group. The shuffle-read pair and `BlockManagerSource` all appear on the pages named above.

## Overlapping topic traces

Topic codes in this page's front matter: B1, E1, E2, E3, I3, I6, I7, I10, I14, A4, A14, E11, E15, E51.

Five have a trace, all recorded at **Spark 4.2.0** — the same version as this sweep, so no version mismatch to reconcile. `check_drift.py --sweeps` lists them as the overlap set.

| Trace | Overlap | Verdict |
|---|---|---|
| [`topics/i6.md`](../topics/i6.md) | the real one | **Agree, and this page goes deeper.** i6 already reaches `MemoryStore.putIteratorAsValues` ([:309](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/memory/MemoryStore.scala#L309)) and `evictBlocksToFreeSpace` ([:472](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/memory/MemoryStore.scala#L472)), and lists `spark.storage.unrollMemoryThreshold` in its config table — both anchors re-verified against the checkout and still correct. What it does not have is the *mechanism*: the growth factor, the check period, the per-task-attempt accounting, or `PartiallyUnrolledIterator`. That is the E51 proposal, and it is a deepening of i6 rather than a contradiction of it. |
| [`topics/b1.md`](../topics/b1.md) | architecture | Agrees. It stops at the driver/executor and scheduler level and never reaches the block-manager RPC plane, so the `BlockManagerMaster` concept extends it downward. |
| [`topics/i7.md`](../topics/i7.md) | Spark UI | Agrees. It reads the UI from the `AppStatusStore` side; this page supplies what the Storage tab's numbers *are* (`RDDInfo`, `StorageStatus`) and what they cost to produce. |
| [`topics/i3.md`](../topics/i3.md) | UDFs | Touches this page only through the 4.2.0 Python worker log blocks. No conflict. |
| [`topics/i10.md`](../topics/i10.md) | data formats | Overlaps on Avro. i10 covers the Avro *data source*; the Avro concept here is the Kryo serializer for `GenericContainer` records and the `avro.schema.*` registration namespace — a different mechanism that i10 does not mention. |

## Sweep log

| Date | Spark | What changed |
|---|---|---|
| 2026-08-09 | 4.2.0 | **Second re-sweep, unchanged Spark version**, triggered by `check_drift.py --sweeps` failing the page: `status: complete` while never citing `BlockManagerMaster`, a class the group's own scope names. **Package breadth found the work** — the whole driver/executor RPC plane was missing, and so was the page's *shape*: it had a sweep log and none of the other three required sections, so nothing recorded what the two prior passes had skipped. Eight concepts added: **BlockManagerMaster** and the two-endpoint protocol (heartbeats split out for latency, sharing the master's mutable `blockManagerInfo` map), the **executor storage endpoint** and its 100-thread async removal pool (which also carries the UI's thread-dump and heap-histogram commands), **BlockManagerId** identity and interning (three different things are modelled as one — executor, driver, push merger), **unroll memory** (proposed as E51; three configs that tied to nothing), **BlockManagerManagedBuffer** wiring Netty refcounts to block read locks, **Avro schema registration** and its undeclared `avro.schema.*` namespace, **SerializerHelper**, and the **storage status / metric model** — where `BlockManagerSource`'s eleven gauges each issue a separate driver `askSync` that rebuilds every block manager's `StorageStatus`. The `block-log-writers` concept gained `LogBlockType`, `LogBlockIdGenerator` and `LogLine`. Config breadth was the weaker signal here: it scored 23/45 literal, but only the three unroll keys were a genuine gap — the rest was `spark.storage.decommission.*`-style shorthand inherited from the earlier passes, now expanded. Recorded and left to their owning groups: `ShuffleBlockFetcherIterator` and `PushBasedFetchHelper` (shuffle-memory), the metrics-system half of `BlockManagerSource` (monitoring). |
| 2026-07-25 | 4.2.0 | Re-sweep at the same Spark version. Both breadth checks contributed: the config slice surfaced `spark.storage.shuffleManager.initWaitingTimeout` tied to nothing, and package breadth was the larger gap — `storage/` is 40 files with 11 cited. Five concepts added: the **BlockId taxonomy** (~20 case classes whose `name` is simultaneously the UI label, the log string and the on-disk filename), **`DiskBlockObjectWriter`** and its commit/revert model that every shuffle write and spill runs through, **`DiskStore` and encrypted blocks** (IO encryption removing the memory-map and zero-copy paths, the storage-side twin of the shuffle-side demotion), **memory-mapped buffer disposal** via `Unsafe.invokeCleaner`, and **block log writers** — new in 4.2.0 ([SPARK-53755]/[SPARK-53975]), the mechanism that finally makes PySpark worker logs retrievable by storing them as `LogBlockId` blocks. The initialization concept also gained the `ShuffleManagerNotInitializedException` race it never mentioned. |
| 2026-07-19 | 4.2.0 | Initial sweep, in two halves (block storage; serialization and StorageLevel). 24 concepts. One gap proposed: E15 (block locking and cache visibility). Two sweeper claims were checked at source and found wrong before writing — eviction order is LRU, not FIFO (`MemoryStore.scala:93` sets `accessOrder=true`), and E11 does already carry a "Learn it with" block. Recorded against existing topics rather than proposed: rack-aware replication silently needing two configs, under-replication not being an error, and decommission counting a gave-up migration as success. |
