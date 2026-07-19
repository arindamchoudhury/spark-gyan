---
subsystem: core
spark_version: "4.2.0"
swept_at: 2026-07-19
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

**Configs:** `spark.storage.replication.policy`, `spark.storage.replication.topologyMapper`, `spark.blockManager.port`

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

## Sweep log

| Date | Spark | What changed |
|---|---|---|
| 2026-07-19 | 4.2.0 | Initial sweep, in two halves (block storage; serialization and StorageLevel). 24 concepts. One gap proposed: E15 (block locking and cache visibility). Two sweeper claims were checked at source and found wrong before writing — eviction order is LRU, not FIFO (`MemoryStore.scala:93` sets `accessOrder=true`), and E11 does already carry a "Learn it with" block. Recorded against existing topics rather than proposed: rack-aware replication silently needing two configs, under-replication not being an error, and decommission counting a gave-up migration as success. |
