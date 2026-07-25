---
subsystem: core
spark_version: "4.2.0"
swept_at: 2026-07-25
group: shuffle-memory
all_groups: [rdd-layer, execution-engine, shuffle-memory, storage-serializer,
  submit-standalone, monitoring, config-security, rpc-resources, api-bridge]
status: complete
concepts:
  - name: shuffle-manager-registration
    topics: [B1, E1]
  - name: writer-selection
    topics: [E1, A4, B1, E3]
  - name: bypass-merge-sort-writer
    topics: [E1, A4]
  - name: unsafe-shuffle-writer
    topics: [E1, A4]
  - name: sort-shuffle-writer
    topics: [E1, A4, I5]
  - name: index-file-and-block-lookup
    topics: [E1, I5, B1]
  - name: map-output-commit
    topics: [E1, B1]
  - name: fetch-request-planning
    topics: [A4, E1, I5]
  - name: fetch-to-memory-vs-disk
    topics: [A4, E1]
  - name: corruption-detection-and-retry
    topics: [A4, E1, A13]
  - name: netty-oom-backpressure
    topics: [A4, E1, A13, E3]
  - name: batch-fetch
    topics: [A4, E1]
  - name: push-based-shuffle
    topics: []
    propose:
      code: A15
      level: Advanced
      title: "Push-Based Shuffle"
      what: "A second write path in which map tasks push their output to remote merger services that concatenate blocks per reduce partition, so reducers read a few large merged chunks instead of thousands of small ones. Thirteen configs, a driver-side finalization protocol, and a reduce-side fallback that silently reverts to ordinary blocks on any failure."
      why: "It is the highest-config-density, lowest-observability feature in the shuffle subsystem. Setting spark.shuffle.push.enabled=true on a non-YARN cluster is accepted and does nothing; merger-threshold negotiation can disable it per stage with no log line at any level; and enabling it forfeits checksum-based corruption diagnosis entirely. Anyone tuning large-cluster shuffle needs to know whether it is actually on."
  - name: external-shuffle-service
    topics: [E2, B1]
  - name: reduce-side-locality
    topics: [I5, B1, A4]
  - name: memory-pool-sizing
    topics: [E1, B1]
  - name: execution-storage-borrowing
    topics: [E1, I6]
  - name: per-task-memory-share
    topics: [E1, A4]
  - name: memory-consumer-acquire-spill-loop
    topics: [E1]
  - name: spill-size-estimation
    topics: [A4, E1, I6]
  - name: tungsten-pages
    topics: [E1]
  - name: bytes-to-bytes-map
    topics: [E1, A4]
  - name: unsafe-external-sorter
    topics: [E1, A4]
  - name: unroll-memory
    topics: [I6, E1]
  - name: compression-codecs
    topics: [E1, A4]
  - name: memory-release-and-leak-detection
    topics: [E1, B1]
  - name: unmanaged-memory-accounting
    topics: []
    propose:
      code: E14
      level: Expert
      title: "Unmanaged Memory: Native Allocators Outside the Unified Pool"
      what: "Components that allocate outside Spark's pools — RocksDB state stores, native libraries — can register as UnmanagedMemoryConsumers. A daemon polls them and subtracts their usage from what execution and storage may allocate, but the polling interval defaults to 0s, which means disabled."
      why: "On a stock install this memory is invisible to the unified manager, which is the direct cause of the most common stateful-streaming complaint: the executor is killed for exceeding its container limit while the Spark UI shows plenty of free storage memory. Sizing executors from the UI's numbers is wrong by however much the native allocator holds."
  - name: map-status-representation-and-size-accuracy
    topics: []
    propose:
      code: A20
      level: Advanced
      title: "Map Output Sizes: What AQE and Skew Detection Actually See"
      what: "Every map task reports its per-reducer output sizes as a MapStatus, and those sizes are lossy by construction: each is compressed to a single byte on a log-1.1 scale, and above spark.shuffle.minNumPartitionsToHighlyCompress (2000) partitions Spark switches to HighlyCompressedMapStatus, which keeps an empty-block bitmap, the exact-ish sizes of blocks above a threshold, and a single average for everything else."
      why: "Every runtime decision that reasons about partition size — AQE's skew-join split, partition coalescing, reduce-side locality, the fetch-to-disk threshold — reads these numbers, not real ones. Above 2000 partitions the per-block sizes of ordinary blocks are literally the same average, and the skew-aware accuracy path is off by default (spark.shuffle.accurateBlockSkewedFactor = -1.0). Tuning a skewed-partition threshold in bytes against averaged inputs is the standard way to conclude that AQE 'does not detect' a skew it cannot see."
  - name: spill-file-merging-and-read-ahead
    topics: [E1, A4]
  - name: host-local-disk-reading
    topics: [A4, I5, E2]
  - name: shuffle-cleanup-and-the-service-state-db
    topics: [E2, E1]
---

Where shuffle data is written, how it is read back, and the memory system both sit on. Swept in two halves — the shuffle write/read path, and memory management with the Tungsten structures and compression codecs underneath it.

!!! info "One theme dominates this subsystem: silent path selection"

    Almost every significant branch here — which writer runs, which merge strategy, whether batch fetch applies, whether push-based shuffle is actually enabled — is logged at `debug` or not at all. A job that falls onto the slow path emits nothing at default log levels. That is called out per concept below and is the strongest argument for the observability gap noted at the end.

---

## ShuffleManager registration

**What it is:** `spark.shuffle.manager` resolves through a two-entry alias map, then is instantiated reflectively once on the driver and once per executor. Both aliases point at the **same class** — `tungsten-sort` has been a synonym for `sort` for years, and setting it changes nothing. An unrecognised value is passed through as a fully-qualified class name, so a typo surfaces as `ClassNotFoundException` at `SparkEnv` construction rather than as config validation.

**Code path:** `SparkEnv` → `ShuffleManager.create` → alias lookup → `Utils.instantiateSerializerOrShuffleManager` → `SortShuffleManager` → `IndexShuffleBlockResolver`

**Anchor files:**

- [ShuffleManager.scala:112](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/shuffle/ShuffleManager.scala#L112) — the alias map; `sort` and `tungsten-sort` both map to `SortShuffleManager`
- [ShuffleManager.scala:117](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/shuffle/ShuffleManager.scala#L117) — `getOrElse(shuffleMgrName)`: unknown names are treated as class names
- [SortShuffleManager.scala:84](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/shuffle/sort/SortShuffleManager.scala#L84) — one resolver instance, shared per executor

**Configs:** `spark.shuffle.manager`, `spark.shuffle.sort.io.plugin.class`

**Maps to topics:** B1, E1

---

## Writer selection — the three-way choice

**What it is:** the writer is picked **once, on the driver, at `registerShuffle` time** — not per task. The decision is frozen into the `ShuffleHandle` subclass, and `getWriter` on the executor is a pure pattern match. Bypass is tested *first*, so a shuffle with ≤200 partitions and a relocatable serializer gets the bypass writer and never the unsafe one.

**Code path:** `registerShuffle` → `shouldBypassMergeSort`? → `BypassMergeSortShuffleHandle` : `canUseSerializedShuffle`? → `SerializedShuffleHandle` : `BaseShuffleHandle` → `getWriter` matches the handle

**Anchor files:**

- [SortShuffleManager.scala:93](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/shuffle/sort/SortShuffleManager.scala#L93) — the if/else-if/else that fixes the choice; bypass wins ties
- [SortShuffleWriter.scala:118](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/shuffle/sort/SortShuffleWriter.scala#L118) — `shouldBypassMergeSort`: no map-side combine and `numPartitions <= bypassMergeThreshold`
- [SortShuffleManager.scala:227](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/shuffle/sort/SortShuffleManager.scala#L227) — `canUseSerializedShuffle`: relocatable serializer, no map-side combine, `numPartitions <= 16777216`
- [PackedRecordPointer.java:43](https://github.com/apache/spark/blob/v4.2.0/core/src/main/java/org/apache/spark/shuffle/sort/PackedRecordPointer.java#L43) — `MAXIMUM_PARTITION_ID = (1 << 24) - 1`, the source of that ceiling
- [SortShuffleManager.scala:154](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/shuffle/sort/SortShuffleManager.scala#L154) — `getWriter`'s match on handle type

!!! warning "The biggest write-path cliff in Spark is logged at debug"

    All three rejection reasons in `canUseSerializedShuffle` are `log.debug` (lines 231, 235, 239). A job that falls back from the serialized path to the deserialized `SortShuffleWriter` — the largest write-path performance difference in this subsystem — emits **nothing** at INFO. The usual trigger is a non-relocatable serializer, i.e. plain Java serialization, and the only symptom is that the job is slower than it used to be.

!!! info "Config changes do not affect registered shuffles"

    Because the choice is frozen at `registerShuffle`, changing `spark.shuffle.sort.bypassMergeThreshold` mid-application does nothing to shuffles already registered, and an AQE-reduced partition count does not retroactively enable the bypass path.

**Configs:** `spark.shuffle.sort.bypassMergeThreshold`

**Maps to topics:** E1, A4, B1

---

## BypassMergeSortShuffleWriter

**What it is:** opens one `DiskBlockObjectWriter` per reduce partition *simultaneously*, routes each record to its partition file with no sorting and no in-memory buffering, then concatenates. Cost is `numPartitions` open file handles plus `numPartitions × spark.shuffle.file.buffer` of buffer memory **per concurrent task** — the real reason the 200 threshold exists. There is no spill here: memory pressure appears as file-descriptor exhaustion, not spill metrics.

**Anchor files:**

- [BypassMergeSortShuffleWriter.java:162](https://github.com/apache/spark/blob/v4.2.0/core/src/main/java/org/apache/spark/shuffle/sort/BypassMergeSortShuffleWriter.java#L162) — `numPartitions` writers allocated up front
- [BypassMergeSortShuffleWriter.java:132](https://github.com/apache/spark/blob/v4.2.0/core/src/main/java/org/apache/spark/shuffle/sort/BypassMergeSortShuffleWriter.java#L132) — `spark.shuffle.file.buffer` × 1024, **per writer**
- [BypassMergeSortShuffleWriter.java:241](https://github.com/apache/spark/blob/v4.2.0/core/src/main/java/org/apache/spark/shuffle/sort/BypassMergeSortShuffleWriter.java#L241) — `spark.file.transferTo` gates NIO channel vs stream copy
- [BypassMergeSortShuffleWriter.java:253](https://github.com/apache/spark/blob/v4.2.0/core/src/main/java/org/apache/spark/shuffle/sort/BypassMergeSortShuffleWriter.java#L253) — a temp-file delete failure is logged as error and otherwise ignored; the file leaks until the shuffle is unregistered

**Configs:** `spark.shuffle.file.buffer`, `spark.file.transferTo`, `spark.shuffle.checksum.enabled`

**Maps to topics:** E1, A4

---

## UnsafeShuffleWriter

**What it is:** serializes each record immediately into a reusable buffer and hands the raw bytes plus partition id to `ShuffleExternalSorter`, which sorts 8-byte packed pointers rather than objects. At close, spill files are merged by one of three strategies chosen at runtime from codec and encryption properties. The fast paths concatenate compressed partition bytes without decompressing; the slow path decompresses and recompresses every byte.

**Anchor files:**

- [UnsafeShuffleWriter.java:255](https://github.com/apache/spark/blob/v4.2.0/core/src/main/java/org/apache/spark/shuffle/sort/UnsafeShuffleWriter.java#L255) — serialize, then insert bytes with partition id
- [UnsafeShuffleWriter.java:317](https://github.com/apache/spark/blob/v4.2.0/core/src/main/java/org/apache/spark/shuffle/sort/UnsafeShuffleWriter.java#L317) — the merge-strategy decision
- [UnsafeShuffleWriter.java:319](https://github.com/apache/spark/blob/v4.2.0/core/src/main/java/org/apache/spark/shuffle/sort/UnsafeShuffleWriter.java#L319) — fast merge needs compression off *or* a concatenable codec
- [UnsafeShuffleWriter.java:339](https://github.com/apache/spark/blob/v4.2.0/core/src/main/java/org/apache/spark/shuffle/sort/UnsafeShuffleWriter.java#L339) — transferTo merge needs `spark.file.transferTo` **and** encryption off
- [UnsafeShuffleWriter.java:348](https://github.com/apache/spark/blob/v4.2.0/core/src/main/java/org/apache/spark/shuffle/sort/UnsafeShuffleWriter.java#L348) — the slow merge: full decompress/recompress per partition per spill
- [ShuffleExternalSorter.java:209](https://github.com/apache/spark/blob/v4.2.0/core/src/main/java/org/apache/spark/shuffle/sort/ShuffleExternalSorter.java#L209) — spill files here obey `spark.shuffle.compress`, **not** `spark.shuffle.spill.compress` (SPARK-3426)

!!! warning "IO encryption silently demotes the merge strategy"

    Every merge-strategy log (lines 340, 343, 347) is `logger.debug`. "Using slow merge" — potentially an order of magnitude in write time — is invisible by default, and enabling IO encryption drops transferTo merge to file-stream merge with no warning anywhere.

**Configs:** `spark.shuffle.compress`, `spark.file.transferTo`, `spark.shuffle.unsafe.file.output.buffer`, `spark.shuffle.spill.diskWriteBufferSize`

**Maps to topics:** E1, A4

---

## SortShuffleWriter — the deserialized path

**What it is:** the fallback for everything the other two reject — map-side combine, non-relocatable serializers, more than 16M partitions. It delegates to `ExternalSorter`, which holds *deserialized* objects, and is the only writer that can apply an aggregator during the write.

**Anchor files:**

- [SortShuffleWriter.scala:66](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/shuffle/sort/SortShuffleWriter.scala#L66) — two `ExternalSorter` constructions, with an aggregator iff `mapSideCombine`
- [SortShuffleWriter.scala:71](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/shuffle/sort/SortShuffleWriter.scala#L71) — no ordering is passed; per-partition sorting is deferred to the reduce side

**Maps to topics:** E1, A4, I5

---

## Index file format and block lookup

**What it is:** one map task produces exactly two files — `.data` and `.index`. The index holds `numPartitions + 1` big-endian longs: a prefix sum of partition lengths starting at 0. A reducer's block is `[index[r], index[r+1])`. The lookup seeks and reads only the two longs it needs.

**Anchor files:**

- [IndexShuffleBlockResolver.scala:439](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/shuffle/IndexShuffleBlockResolver.scala#L439) — `lengths.scanLeft(0L)(_ + _)`, the prefix sum
- [IndexShuffleBlockResolver.scala:639](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/shuffle/IndexShuffleBlockResolver.scala#L639) — `channel.position(startReduceId * 8L)`
- [IndexShuffleBlockResolver.scala:647](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/shuffle/IndexShuffleBlockResolver.scala#L647) — the SPARK-22982 position assertion, guarding against a shared FD being seeked underneath and serving **a different reducer's data**
- [IndexShuffleBlockResolver.scala:55](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/shuffle/IndexShuffleBlockResolver.scala#L55) — the warning that this format is mirrored in the external shuffle service and the two must stay in sync

**Maps to topics:** E1, I5, B1

---

## Map output commit

**What it is:** index, data and optionally checksum files are committed under one executor-wide monitor. Before writing, the committer checks whether a *previous attempt of the same task* already produced a valid index/data pair and adopts it if so. Validity is three tests: index length is exactly `(blocks+1)*8`, first offset is 0, and data length equals the sum of derived lengths.

**Anchor files:**

- [IndexShuffleBlockResolver.scala:414](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/shuffle/IndexShuffleBlockResolver.scala#L414) — `this.synchronized`; one resolver per executor makes check-and-rename atomic *within the JVM*
- [IndexShuffleBlockResolver.scala:213](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/shuffle/IndexShuffleBlockResolver.scala#L213) — `checkIndexAndDataFile`, the three-part validity test
- [IndexShuffleBlockResolver.scala:453](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/shuffle/IndexShuffleBlockResolver.scala#L453) — a checksum-file write failure is caught and logged, never propagated

!!! warning "The commit is two renames, not one"

    The data file is deleted before the new one is renamed in, so a JVM kill in that window leaves a valid index with no data. The monitor is per-JVM only — it does not protect against the external shuffle service reading mid-commit. And `checkIndexAndDataFile`'s `IOException` handlers return `null` indistinguishably from "files absent", so an unreadable index causes a silent full rewrite rather than an error.

**Configs:** `spark.shuffle.checksum.enabled`, `spark.shuffle.checksum.algorithm`

**Maps to topics:** E1, B1

---

## Fetch request planning and in-flight limits

**What it is:** blocks are bucketed into four fetch modes (local, host-local, push-merged-local, remote), then remote blocks are packed into requests. Three independent limits throttle simultaneously: bytes in flight, requests in flight, and blocks in flight *per remote address*. Request size targets `maxBytesInFlight / 5` so five peers can stream concurrently.

**Anchor files:**

- [ShuffleBlockFetcherIterator.scala:112](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/ShuffleBlockFetcherIterator.scala#L112) — `targetRemoteRequestSize = max(maxBytesInFlight / 5, 1)`
- [ShuffleBlockFetcherIterator.scala:392](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/ShuffleBlockFetcherIterator.scala#L392) — `partitionBlocksByFetchMode`, the four-way split
- [ShuffleBlockFetcherIterator.scala:1235](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/ShuffleBlockFetcherIterator.scala#L1235) — `isRemoteBlockFetchable`, including the `bytesInFlight == 0` escape hatch
- [ShuffleBlockFetcherIterator.scala:724](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/ShuffleBlockFetcherIterator.scala#L724) — requests are randomized to spread load across peers

!!! info "`maxSizeInFlight` is a target, not a cap"

    The `bytesInFlight == 0` disjunct lets a single request larger than `spark.reducer.maxSizeInFlight` through, because otherwise it could never be fetched at all. Under skew this is exactly the case that materialises.

**Configs:** `spark.reducer.maxSizeInFlight`, `spark.reducer.maxReqsInFlight`, `spark.reducer.maxBlocksInFlightPerAddress`

**Maps to topics:** A4, E1, I5

---

## Fetch to memory vs fetch to disk

**What it is:** one comparison decides whether a response is buffered in Netty memory or streamed straight to a local file. The threshold applies to the **request** size, not to an individual block.

**Anchor files:**

- [ShuffleBlockFetcherIterator.scala:379](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/ShuffleBlockFetcherIterator.scala#L379) — `req.size > maxReqSizeShuffleToMem`
- [ShuffleBlockFetcherIterator.scala:376](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/ShuffleBlockFetcherIterator.scala#L376) — data is already compressed and encrypted on the wire, so it is written through unmodified

**Configs:** `spark.maxRemoteBlockSizeFetchToMem`

**Maps to topics:** A4, E1

---

## Corruption detection and retry

**What it is:** a three-stage defence. A zero-size buffer is corruption outright. Wrapping the stream is attempted eagerly, and with `detectCorrupt.useExtraMemory` the first `maxBytesInFlight / 3` bytes are copied into memory so corruption is caught *before* records reach user code. A remote block that fails wrapping is re-fetched **exactly once**; a second failure throws `FetchFailedException`, and with checksums on the client first computes the block's checksum and asks the server to classify disk vs network.

**Anchor files:**

- [ShuffleBlockFetcherIterator.scala:846](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/ShuffleBlockFetcherIterator.scala#L846) — zero-size buffer branch
- [ShuffleBlockFetcherIterator.scala:921](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/ShuffleBlockFetcherIterator.scala#L921) — `copyStreamUpTo(input, maxBytesInFlight / 3)`, with a TODO noting this memory is unmanaged and cannot spill
- [ShuffleBlockFetcherIterator.scala:945](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/ShuffleBlockFetcherIterator.scala#L945) — a **local** corrupt block fails immediately, no retry
- [ShuffleBlockFetcherIterator.scala:1118](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/ShuffleBlockFetcherIterator.scala#L1118) — `diagnoseCorruption`
- [ShuffleBlockFetcherIterator.scala:1250](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/ShuffleBlockFetcherIterator.scala#L1250) — `throwFetchFailedException`, where the read path becomes a stage retry

!!! warning "Batch fetch and push-based shuffle forfeit checksum diagnosis"

    `diagnoseCorruption` explicitly skips `ShuffleBlockChunkId` (push-based; SPARK-36284 still open) and `ShuffleBlockBatchId` (batch fetch). Enabling either gives you "is corrupted but diagnosis is skipped" instead of a disk-vs-network verdict.

**Configs:** `spark.shuffle.detectCorrupt`, `spark.shuffle.detectCorrupt.useExtraMemory`, `spark.shuffle.checksum.enabled`, `spark.shuffle.checksum.algorithm`

**Maps to topics:** A4, E1, A13

---

## Netty OOM backpressure

**What it is:** an `OutOfDirectMemoryError` during fetch is not a failure but a **JVM-wide, cross-task circuit breaker**. Offending blocks move to a deferred queue, a shared flag halts all new shuffle fetch requests, and the flag clears only when Netty memory recovers or in-flight requests drain to zero.

**Anchor files:**

- [ShuffleBlockFetcherIterator.scala:345](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/ShuffleBlockFetcherIterator.scala#L345) — the OOM branch, gated on a per-block retry counter
- [ShuffleBlockFetcherIterator.scala:1183](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/ShuffleBlockFetcherIterator.scala#L1183) — `fetchUpToMaxBytes` returns immediately while OOMed
- [ShuffleBlockFetcherIterator.scala:342](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/ShuffleBlockFetcherIterator.scala#L342) — the code's own admission that this is "only a workaround"

!!! warning "A cluster parked behind this flag looks merely slow"

    The warning is logged once per iterator, at INFO, and only for the first block. There is no metric. Tasks spend their time waiting with no error and essentially no log volume.

**Configs:** `spark.shuffle.maxAttemptsOnNettyOOM`, `spark.maxRemoteBlockSizeFetchToMem`

**Maps to topics:** A4, E1

---

## Batch fetch of contiguous blocks

**What it is:** when a reducer reads a contiguous *range* of partitions — the AQE coalesce case — adjacent blocks from one map output can be fetched as one `ShuffleBlockBatchId`. Five conditions must hold, and the feature is signalled through a task-local property string rather than a config.

**Anchor files:**

- [SortShuffleManager.scala:217](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/shuffle/sort/SortShuffleManager.scala#L217) — `canUseBatchFetch`, driven by a local property
- [BlockStoreShuffleReader.scala:59](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/shuffle/BlockStoreShuffleReader.scala#L59) — the five-way conjunction: relocatable serializer, concatenable codec, not old protocol, not IO-encrypted

**Maps to topics:** A4, E1

---

## Push-based shuffle

**What it is:** map tasks push their output to remote merger services that concatenate blocks per reduce partition, so reducers read a few large merged chunks. It is only *actually* on when YARN + external shuffle service + no IO encryption + relocatable serializer all hold. Merger locations are then negotiated, and if too few come back, push is disabled for that stage.

**Code path:** `Utils.isPushBasedShuffleEnabled` → `getShufflePushMergerLocations` → `dep.setMergerLocs` → `ShuffleWriteProcessor` → `ShuffleBlockPusher` → driver `finalizeShuffleMerge` → reducer chunk fetch → fallback on any failure

**Anchor files:**

- [Utils.scala:2588](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/util/Utils.scala#L2588) — master must literally equal `"yarn"`
- [Utils.scala:2606](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/util/Utils.scala#L2606) — one warning listing all four requirements, without saying which failed
- [YarnSchedulerBackend.scala:198](https://github.com/apache/spark/blob/v4.2.0/resource-managers/yarn/src/main/scala/org/apache/spark/scheduler/cluster/YarnSchedulerBackend.scala#L198) — below threshold → `Seq.empty`, push silently off for the stage
- [ShuffleBlockPusher.scala:446](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/shuffle/ShuffleBlockPusher.scala#L446) — blocks larger than `maxBlockSizeToPush` are **skipped entirely** and always fetched from the mapper
- [ShuffleBlockPusher.scala:418](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/shuffle/ShuffleBlockPusher.scala#L418) — merger assignment maps the same partition ranges to the same mergers from every mapper
- [DAGScheduler.scala:3040](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala#L3040) — the push-completion-ratio finalize trigger
- [DAGScheduler.scala:2933](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala#L2933) — `ShuffleMergeFinalized` posted in `finally`; the stage always advances
- [ShuffleBlockFetcherIterator.scala:1002](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/ShuffleBlockFetcherIterator.scala#L1002) — the three reduce-side fallback triggers

!!! warning "The quietest failure in the subsystem"

    `YarnSchedulerBackend.scala:198` returns an empty merger list with **no log at any level**. A stage that reverts to ordinary shuffle because two nodes were excluded emits nothing at all. Push failures are deliberately non-fatal — they cost merge efficiency, never correctness — so every degradation here is invisible by design.

!!! info "`corruptMergedBlockChunks` is the only signal"

    Reduce-side fallback increments that metric and nothing else. It is not surfaced in the UI's standard shuffle metrics.

**Configs:** the `spark.shuffle.push.*` family (13 keys), plus `spark.shuffle.service.enabled`

**Maps to topics:** none — proposed as A15

---

## External shuffle service

**What it is:** with the service enabled, the executor's `shuffleServerId` points at the service's port rather than its own, and the executor synchronously registers its local dirs — retrying with a **hardcoded** 5-second sleep. This is what makes map output survive executor loss.

**Anchor files:**

- [BlockManager.scala:587](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/BlockManager.scala#L587) — `shuffleServerId` redirected to the service port
- [BlockManager.scala:657](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/BlockManager.scala#L657) — `SLEEP_TIME_MS = 5000`, not configurable
- [BlockManager.scala:937](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/BlockManager.scala#L937) — blocks reported under `shuffleServerId`, the mechanism behind surviving executor loss

**Configs:** `spark.shuffle.service.enabled`, `.port`, `.registration.timeout`, `.registration.maxAttempts`, `.fetch.rdd.enabled`

**Maps to topics:** E2, B1

---

## Reduce-side locality preference

**What it is:** the scheduler prefers to place a reduce task where the largest fraction of its input already lives — but only for small shuffles, gated behind two hardcoded thresholds plus a config.

**Anchor files:**

- [MapOutputTracker.scala:1080](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/MapOutputTracker.scala#L1080) — the gate: locality enabled and both map and reduce counts below thresholds
- [MapOutputTracker.scala:733](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/MapOutputTracker.scala#L733) — `REDUCER_PREF_LOCS_FRACTION = 0.2`, hardcoded

**Configs:** `spark.shuffle.reduceLocality.enabled`, `spark.shuffle.mapOutput.minSizeForBroadcast`

**Maps to topics:** I5, B1, A4

---

## Memory pool sizing and the reserved floor

**What it is:** one `MemoryManager` per JVM owns four pools — on/off-heap × execution/storage. Sizes are fixed at construction and thereafter only move *between* execution and storage. The formula:

```
usableMemory  = systemMemory - 300 MB reserved
maxHeapMemory = usableMemory * spark.memory.fraction        (0.6)
storageRegion = maxHeapMemory * spark.memory.storageFraction (0.5)
```

Off-heap is a separate, **unreserved** budget: no 300 MB floor and no `spark.memory.fraction`.

**Anchor files:**

- [UnifiedMemoryManager.scala:264](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/memory/UnifiedMemoryManager.scala#L264) — `RESERVED_SYSTEM_MEMORY_BYTES = 300 MB`
- [UnifiedMemoryManager.scala:462](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/memory/UnifiedMemoryManager.scala#L462) — `getMaxMemory`, the `reserved * 1.5` minimum-heap check that refuses to start
- [MemoryManager.scala:61](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/memory/MemoryManager.scala#L61) — off-heap pools sized purely from `spark.memory.offHeap.size`

!!! info "The 450 MB floor is not a round number"

    `minSystemMemory = ceil(reserved × 1.5)` = 450 MiB. A 400 MB driver fails to start with `INVALID_DRIVER_MEMORY`.

**Configs:** `spark.memory.fraction`, `spark.memory.storageFraction`, `spark.memory.offHeap.enabled`, `spark.memory.offHeap.size`

**Maps to topics:** E1, B1

---

## Execution/storage borrowing and its asymmetry

**What it is:** the boundary is soft but **directional**. Storage may borrow all free execution memory, and execution reclaims it by evicting cached blocks. Execution may borrow free storage memory, and storage can never take it back — it just fails to cache.

**Anchor files:**

- [UnifiedMemoryManager.scala:160](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/memory/UnifiedMemoryManager.scala#L160) — `maybeGrowExecutionPool`, reclaiming by eviction
- [UnifiedMemoryManager.scala:239](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/memory/UnifiedMemoryManager.scala#L239) — storage borrows only what execution has free; **no eviction of execution**
- [UnifiedMemoryManager.scala:195](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/memory/UnifiedMemoryManager.scala#L195) — `computeMaxExecutionPoolSize` subtracts the eviction-immune storage region (SPARK-12155)

!!! warning "A cached DataFrame can be evicted with nothing in the logs"

    `acquireStorageMemory` returning `false` is not an error — the block is simply not cached and falls back to disk or recomputation. A `.cache()` can be almost entirely evicted by a shuffle-heavy stage with no signal beyond one `logInfo` for oversized blocks. `spark.memory.storageFraction` is the floor below which storage is immune.

**Configs:** `spark.memory.storageFraction`, `spark.memory.fraction`

**Maps to topics:** E1, I6

---

## Per-task memory share and the 1/2N rule

**What it is:** `ExecutionMemoryPool` arbitrates between concurrent tasks. Each is capped at `maxPoolSize / numActiveTasks` and **blocks** rather than spilling if it has not yet reached `poolSize / (2 × numActiveTasks)`.

**Anchor files:**

- [ExecutionMemoryPool.scala:113](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/memory/ExecutionMemoryPool.scala#L113) — the grant loop, `maxMemoryPerTask`, `minMemoryPerTask`, `lock.wait()`
- [ExecutionMemoryPool.scala:154](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/memory/ExecutionMemoryPool.scala#L154) — over-release is logged as "Internal error" and clamped rather than failing

!!! info "This is *why* skew spills"

    A skewed task's memory ceiling is `1/N` of the pool no matter how large its partition is. The `lock.wait()` has no timeout, so a task can park indefinitely — surfacing as a hang, not an OOM, with only a `waiting for at least 1/2N` log line.

**Maps to topics:** E1, A4

---

## The acquire/spill loop

**What it is:** any Tungsten structure extends `MemoryConsumer` and reaches memory only through its `TaskMemoryManager`. On a shortfall, the manager builds a priority order over same-mode consumers and asks the *smallest one that alone covers the shortfall* to spill, falling back to the largest. The requesting consumer sorts last, so it is asked only after everyone else.

**Anchor files:**

- [TaskMemoryManager.java:192](https://github.com/apache/spark/blob/v4.2.0/core/src/main/java/org/apache/spark/memory/TaskMemoryManager.java#L192) — the `TreeMap` victim-selection heuristic
- [TaskMemoryManager.java:249](https://github.com/apache/spark/blob/v4.2.0/core/src/main/java/org/apache/spark/memory/TaskMemoryManager.java#L249) — `trySpillAndAcquire`, with the SPARK-35486 re-acquire race
- [MemoryConsumer.java:76](https://github.com/apache/spark/blob/v4.2.0/core/src/main/java/org/apache/spark/memory/MemoryConsumer.java#L76) — the "do not call acquireMemory() from spill()" deadlock warning
- [Spillable.scala:118](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/util/collection/Spillable.scala#L118) — `spill` returns `0L` for a self-trigger **and** whenever Tungsten mode is off-heap

!!! warning "Enabling off-heap silently disables on-demand spilling for JVM-object collections"

    `Spillable.spill` short-circuits to `0L` unless the mode is `ON_HEAP`. With `spark.memory.offHeap.enabled=true`, `ExternalSorter` and `ExternalAppendOnlyMap` can no longer be *asked* to spill — they only spill via their own size checks. Victim selection also filters by memory mode, so on-heap consumers hold memory an off-heap request can never reclaim.

**Configs:** `spark.memory.offHeap.enabled`, `spark.buffer.pageSize`

**Maps to topics:** E1

---

## Spill size estimation

**What it is:** `ExternalSorter` and `ExternalAppendOnlyMap` hold ordinary JVM objects, so their size is **sampled and extrapolated**, not measured. `Spillable.maybeSpill` checks that estimate every 32 records and doubles its memory claim; if the doubling is refused, it spills.

**Anchor files:**

- [Spillable.scala:86](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/util/collection/Spillable.scala#L86) — `maybeSpill`: force thresholds, the `% 32` gate, the doubling request
- [SizeTracker.scala:37](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/util/collection/SizeTracker.scala#L37) — `SAMPLE_GROWTH_RATE = 1.1`
- [SizeTracker.scala:96](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/util/collection/SizeTracker.scala#L96) — `estimateSize` = last sample + `bytesPerUpdate × updates since`
- [ExternalSorter.scala:269](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/util/collection/ExternalSorter.scala#L269) — `forceSpill` returns `false` outright in shuffle-sort mode

!!! warning "This is the mechanism behind 'it OOMed instead of spilling'"

    Sampling is on a 1.1× geometric schedule, so late in a large collection thousands of records pass between samples and the interim size is a linear extrapolation. Records of wildly varying size make the estimate arbitrarily wrong *low*, and both backstops — `spark.shuffle.spill.numElementsForceSpillThreshold` and `maxSizeInBytesForSpillThreshold` — default to `MAX_VALUE`.

**Configs:** `spark.shuffle.spill.numElementsForceSpillThreshold`, `spark.shuffle.spill.maxSizeInBytesForSpillThreshold`, `spark.shuffle.spill.initialMemoryThreshold`

**Maps to topics:** A4, E1, I6

---

## Tungsten pages and address encoding

**What it is:** Tungsten memory is allocated as *pages* in a per-task 8192-entry page table. Record pointers are 64-bit: 13 bits of page number, 51 bits of offset. That encoding caps page size and, transitively, the maximum record size in every Tungsten structure.

**Anchor files:**

- [TaskMemoryManager.java:61](https://github.com/apache/spark/blob/v4.2.0/core/src/main/java/org/apache/spark/memory/TaskMemoryManager.java#L61) — `PAGE_NUMBER_BITS = 13`, `OFFSET_BITS = 51`, `PAGE_TABLE_SIZE = 8192`
- [TaskMemoryManager.java:77](https://github.com/apache/spark/blob/v4.2.0/core/src/main/java/org/apache/spark/memory/TaskMemoryManager.java#L77) — `MAXIMUM_PAGE_SIZE_BYTES = ((1L << 31) - 1) * 8L`
- [TaskMemoryManager.java:398](https://github.com/apache/spark/blob/v4.2.0/core/src/main/java/org/apache/spark/memory/TaskMemoryManager.java#L398) — the JVM-OOM catch that books memory to `acquiredButNotUsed` and **recurses**

!!! warning "The page-allocation retry is unbounded"

    `retryCount` only selects a log message; the recursion has no ceiling. Each pass leaks the previously-acquired budget into `acquiredButNotUsed`, which the manager still counts as used. Repeated genuine JVM OOM degrades into a shrinking effective pool with only `WARN Failed to allocate a page` to show for it. Double-free and alignment checks are `assert` statements, inert without `-ea`.

**Configs:** `spark.buffer.pageSize`, `spark.memory.offHeap.enabled`, `spark.unsafe.exceptionOnMemoryLeak`

**Maps to topics:** E1

---

## BytesToBytesMap

**What it is:** the Tungsten hash map behind hash aggregation and hash joins. Two `long` entries per key in a `LongArray`, records appended into data pages. Its constraints are structural, not configurable: `MAX_CAPACITY = 1 << 29`, 8-byte alignment, and key and value must live in the same page.

**Anchor files:**

- [BytesToBytesMap.java:101](https://github.com/apache/spark/blob/v4.2.0/core/src/main/java/org/apache/spark/unsafe/map/BytesToBytesMap.java#L101) — `MAX_CAPACITY = 1 << 29`
- [BytesToBytesMap.java:757](https://github.com/apache/spark/blob/v4.2.0/core/src/main/java/org/apache/spark/unsafe/map/BytesToBytesMap.java#L757) — the `% 8 == 0` alignment contract, **assert-only**
- [BytesToBytesMap.java:815](https://github.com/apache/spark/blob/v4.2.0/core/src/main/java/org/apache/spark/unsafe/map/BytesToBytesMap.java#L815) — `catch (SparkOutOfMemoryError oom) { canGrowArray = false; }`
- [BytesToBytesMap.java:838](https://github.com/apache/spark/blob/v4.2.0/core/src/main/java/org/apache/spark/unsafe/map/BytesToBytesMap.java#L838) — `acquireNewPage` catches the same error and returns `false`

!!! warning "The loudest silent degradation in this sweep"

    `append` returns a boolean. Both OOM paths convert a `SparkOutOfMemoryError` into a flag rather than propagating it, and **nothing is logged at either site**. Once `canGrowArray` latches off the map keeps serving lookups at an ever-worsening load factor and refuses new keys; the caller is expected to notice and spill. A saturated map degrades into linear probing with no diagnostic whatsoever.

**Configs:** `spark.buffer.pageSize`

**Maps to topics:** E1, A4

---

## UnsafeExternalSorter

**What it is:** the Tungsten sorter — records in pages, an 8-byte-per-record pointer array. Growing that array is the usual spill trigger, because it needs a single contiguous allocation twice its current size.

**Anchor files:**

- [UnsafeExternalSorter.java:404](https://github.com/apache/spark/blob/v4.2.0/core/src/main/java/org/apache/spark/util/collection/unsafe/sort/UnsafeExternalSorter.java#L404) — `growPointerArrayIfNecessary`, with the `TooLargePageException` → spill branch
- [UnsafeExternalSorter.java:226](https://github.com/apache/spark/blob/v4.2.0/core/src/main/java/org/apache/spark/util/collection/unsafe/sort/UnsafeExternalSorter.java#L226) — `spill` inverts `Spillable`'s convention; a sorter with zero records never spills
- [UnsafeInMemorySorter.java:158](https://github.com/apache/spark/blob/v4.2.0/core/src/main/java/org/apache/spark/util/collection/unsafe/sort/UnsafeInMemorySorter.java#L158) — `getUsableCapacity`: half the array under radix sort, two-thirds under Tim sort

!!! info "The sorter spills at roughly half its nominal capacity"

    33–50% of the pointer array is reserved as sort scratch, and radix sort reserves *more* than Tim sort. Neither is a tunable.

!!! info "Radix sort is selected two different ways"

    On the **shuffle** side it is a config: `spark.shuffle.sort.useRadixSort` (default `true`) is read once at [ShuffleExternalSorter.java:149](https://github.com/apache/spark/blob/v4.2.0/core/src/main/java/org/apache/spark/shuffle/sort/ShuffleExternalSorter.java#L149) and passed into `ShuffleInMemorySorter`, where it costs capacity — `array.size() / (useRadixSort ? 2 : 1.5)` at [ShuffleInMemorySorter.java:81](https://github.com/apache/spark/blob/v4.2.0/core/src/main/java/org/apache/spark/shuffle/sort/ShuffleInMemorySorter.java#L81), i.e. turning it *off* raises the spill threshold by a third. In `UnsafeInMemorySorter` there is no config at all: radix is used iff the prefix comparator implements `PrefixComparators.RadixSortSupport` ([UnsafeInMemorySorter.java:145](https://github.com/apache/spark/blob/v4.2.0/core/src/main/java/org/apache/spark/util/collection/unsafe/sort/UnsafeInMemorySorter.java#L145)) — a property of the sort key's type, not something you can set.

**Configs:** `spark.shuffle.spill.numElementsForceSpillThreshold`, `spark.shuffle.spill.maxSizeInBytesForSpillThreshold`, `spark.buffer.pageSize`, `spark.shuffle.sort.useRadixSort`, `spark.shuffle.sort.initialBufferSize`

**Maps to topics:** E1, A4

---

## Unroll memory

**What it is:** before caching an iterator, `MemoryStore` "unrolls" it incrementally, holding *unroll* memory — storage memory under a different ledger — and periodically re-checking the estimate. A refused reservation mid-unroll means the block is not cached.

**Anchor files:**

- [MemoryStore.scala:203](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/memory/MemoryStore.scala#L203) — the initial reservation; `keepUnrolling = false` short-circuits the put
- [MemoryStore.scala:98](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/memory/MemoryStore.scala#L98) — off-heap unroll exists only for `putIteratorAsBytes`

!!! warning "Off-heap caching of deserialized values is impossible by construction"

    Setting `spark.memory.offHeap.enabled=true` and expecting `MEMORY_ONLY` to use it gets you on-heap caching, with no warning.

**Configs:** `spark.storage.unrollMemoryThreshold`, `.unrollMemoryGrowthFactor`, `.unrollMemoryCheckPeriod`

**Maps to topics:** I6, E1

---

## Compression codecs

**What it is:** a short name or fully-qualified class is resolved reflectively via a one-arg `SparkConf` constructor. `SerializerManager` decides *whether* to compress by inspecting the `BlockId` type.

**Anchor files:**

- [CompressionCodec.scala:86](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/io/CompressionCodec.scala#L86) — `createCodec`, reflective lookup
- [CompressionCodec.scala:196](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/io/CompressionCodec.scala#L196) — `SnappyCompressionCodec`'s native-library probe in the constructor
- [SerializerManager.scala:110](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/serializer/SerializerManager.scala#L110) — `shouldCompress` per `BlockId` type

!!! info "A broken Snappy native library reports as 'codec not available'"

    The constructor converts a native-load `Error` into `IllegalArgumentException`, which `createCodec` catches alongside `ClassNotFoundException` — so the message blames the codec name rather than the library. Note also that `spark.shuffle.compress` and `spark.shuffle.spill.compress` are separate booleans over the **same** codec; they cannot use different ones.

!!! info "The per-codec knobs are chosen by codec, so most of them are inert"

    Each codec reads only its own prefix, and `spark.io.compression.codec` picks one — so `spark.io.compression.zstd.level` (1), `.workers` (0, meaning single-threaded), `.strategy`, `.bufferSize` and `.bufferPool.enabled` do nothing under LZ4, and `spark.io.compression.lz4.blockSize` / `.snappy.blockSize` / `.lzf.parallel.enabled` do nothing under Zstd. Setting the wrong family is silent. Separately, `spark.shuffle.mapStatus.compression.codec` (ZSTD) is its own key and is **not** governed by `spark.io.compression.codec` — it compresses the map-status broadcast on the driver, not shuffle data.

**Configs:** `spark.io.compression.codec`, `spark.shuffle.compress`, `spark.shuffle.spill.compress`; per-codec: `spark.io.compression.zstd.level`, `.zstd.workers`, `.zstd.strategy`, `.zstd.bufferSize`, `.zstd.bufferPool.enabled`, `spark.io.compression.lz4.blockSize`, `spark.io.compression.snappy.blockSize`, `spark.io.compression.lzf.parallel.enabled`

**Maps to topics:** E1, A4

---

## Memory release and leak detection

**What it is:** at task completion `cleanUpAllAllocatedMemory` drops all consumers, frees every page still in the page table, and releases the task's remaining balance. A non-zero return is Spark's definition of a managed memory leak.

**Anchor files:**

- [TaskMemoryManager.java:528](https://github.com/apache/spark/blob/v4.2.0/core/src/main/java/org/apache/spark/memory/TaskMemoryManager.java#L528) — `cleanUpAllAllocatedMemory`; per-consumer detail is debug-gated
- [Executor.scala:899](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/executor/Executor.scala#L899) — `spark.unsafe.exceptionOnMemoryLeak` decides throw vs warn; **default false**
- [SparkOutOfMemoryError.java:31](https://github.com/apache/spark/blob/v4.2.0/core/src/main/java/org/apache/spark/memory/SparkOutOfMemoryError.java#L31) — extends `java.lang.OutOfMemoryError`

!!! warning "Leak detection is suppressed exactly when leaks are likeliest"

    The check is skipped whenever the task threw. And because `SparkOutOfMemoryError` subclasses the JVM's `OutOfMemoryError`, generic OOM handling catches it — the operationally important distinction (Spark's accounting said no, versus the JVM said no) is visible only in the error class, not the type.

**Configs:** `spark.unsafe.exceptionOnMemoryLeak`, `spark.storage.exceptionOnPinLeak`

**Maps to topics:** E1, B1

---

## Unmanaged memory accounting

**What it is:** components allocating outside Spark's pools — RocksDB state stores, native libraries — can register as `UnmanagedMemoryConsumer`s. A daemon polls them and subtracts their usage from what execution and storage may allocate. Added in 4.1.

**Anchor files:**

- [UnifiedMemoryManager.scala:87](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/memory/UnifiedMemoryManager.scala#L87) — polling starts only if the interval is > 0
- [UnifiedMemoryManager.scala:374](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/memory/UnifiedMemoryManager.scala#L374) — `pollUnmanagedMemoryUsers`; a consumer whose accessor throws is silently counted as zero
- [UnifiedMemoryManager.scala:196](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/memory/UnifiedMemoryManager.scala#L196) — subtracted from the execution ceiling

!!! warning "Disabled by default, which is why streaming executors die 'with free memory'"

    `spark.memory.unmanagedMemoryPollingInterval` defaults to `0s`. On a stock 4.2 install RocksDB state-store memory is invisible to the unified manager — the executor is killed for exceeding its container while the UI shows free storage memory.

**Configs:** `spark.memory.unmanagedMemoryPollingInterval`

**Maps to topics:** none — proposed as E14

---

## Map output size representation

**What it is:** the numbers every size-based runtime decision rests on, and they are lossy twice over. A `MapStatus` reports one size per reduce partition, each compressed to **a single byte** as `log(size)` base 1.1 — up to ~35 GB with at most 10% error. Then, above `spark.shuffle.minNumPartitionsToHighlyCompress` (2000), the implementation switches from `CompressedMapStatus` (one byte *per block*) to `HighlyCompressedMapStatus`, which keeps a `RoaringBitmap` of empty blocks, byte-compressed sizes for blocks judged "huge", and **one average** shared by every other non-empty block.

**Code path:** `ShuffleWriter` → `MapStatus.apply(loc, uncompressedSizes, …)` → `numPartitions > 2000`? `HighlyCompressedMapStatus.apply` : `new CompressedMapStatus` → threshold computation → `MapOutputTracker` → `getSizeForBlock` → AQE / skew detection / fetch planning

**Anchor files:**

- [MapStatus.scala:85](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/MapStatus.scala#L85) — the one branch that decides which representation a shuffle gets, on partition count alone
- [MapStatus.scala:92](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/MapStatus.scala#L92) — `LOG_BASE = 1.1`, and the comment stating the ≤10% error and ~35 GB ceiling
- [MapStatus.scala:99](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/MapStatus.scala#L99) — `compressSize`, clamped to 255: **every block above ~35 GB reports the same size**
- [MapStatus.scala:290](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/MapStatus.scala#L290) — `accurateBlockSkewedFactor > 0` gates the whole skew-aware accuracy path
- [MapStatus.scala:300](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/MapStatus.scala#L300) — the threshold: `max(median × factor, the maxAccurateSkewedBlockNumber-th largest)`, then capped by `accurateBlockThreshold`
- [MapStatus.scala:307](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/MapStatus.scala#L307) — the `else`: with the factor at its default, only blocks above the flat 100 MB threshold are tracked individually
- [MapStatus.scala:329](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/MapStatus.scala#L329) — `avgSize = totalSmallBlockSize / numSmallBlocks`, the number returned for every non-huge block
- [MapStatus.scala:222](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/MapStatus.scala#L222) — `getSizeForBlock`: huge block → its own size, everything else → `avgSize`

!!! warning "Above 2000 partitions, per-block skew is invisible unless you opt in"

    `spark.shuffle.accurateBlockSkewedFactor` defaults to **-1.0**, which takes the `else` branch and disables skew-relative accuracy entirely. What remains is the flat `spark.shuffle.accurateBlockThreshold` of 100 MB. So on a 2001-partition shuffle, a 90 MB block among 1 MB peers — a 90× skew — is reported as the *average*, and every consumer of that number, AQE's skew join included, sees a uniform distribution. Setting the factor to a positive value (5 is the documented starting point) is what makes moderate skew visible at all.

!!! info "The representation is chosen by partition count, not by size"

    2000 partitions of 1 KB each gets the lossy representation; 1999 partitions of 10 GB each gets the per-block one. `spark.shuffle.minNumPartitionsToHighlyCompress` exists because the driver holds one `MapStatus` per map task and the memory is real — but the trade is accuracy, and it is made on a proxy for size rather than size.

**Configs:** `spark.shuffle.minNumPartitionsToHighlyCompress`, `spark.shuffle.accurateBlockThreshold`, `spark.shuffle.accurateBlockSkewedFactor`, `spark.shuffle.maxAccurateSkewedBlockNumber`, `spark.shuffle.mapStatus.compression.codec` (ZSTD), `spark.shuffle.mapOutput.parallelAggregationThreshold`, `spark.shuffle.mapOutput.dispatcher.numThreads`, `spark.shuffle.mapOutput.minSizeForBroadcast`

**Maps to topics:** none — proposed as A20

---

## Spill file merging and read-ahead

**What it is:** when `UnsafeExternalSorter` has spilled, the sorted output is a k-way merge across spill files. The classic path opens **every** spill reader at once, each carrying about 3 MB of buffers — so a task with a thousand spills needs gigabytes of buffer just to merge. Spark 4.2.0 adds a bounded multi-round merge that caps concurrent readers, at the cost of rewriting records once per intermediate round.

**Code path:** `getSortedIterator` → spills empty? in-memory iterator : `spillMergeFactor != -1 && spills > factor`? `UnsafeSorterBoundedSpillMerger` (multi-round) : `UnsafeSorterSpillMerger` (open all) → per-file `UnsafeSorterSpillReader` → optional `ReadAheadInputStream`

**Anchor files:**

- [UnsafeExternalSorter.java:592](https://github.com/apache/spark/blob/v4.2.0/core/src/main/java/org/apache/spark/util/collection/unsafe/sort/UnsafeExternalSorter.java#L592) — the branch; `-1` keeps the legacy open-everything merge
- [UnsafeExternalSorter.java:600](https://github.com/apache/spark/blob/v4.2.0/core/src/main/java/org/apache/spark/util/collection/unsafe/sort/UnsafeExternalSorter.java#L600) — "Original single-round merge: open all spill readers at once"
- [UnsafeSorterBoundedSpillMerger.java:37](https://github.com/apache/spark/blob/v4.2.0/core/src/main/java/org/apache/spark/util/collection/unsafe/sort/UnsafeSorterBoundedSpillMerger.java#L37) — the class doc: ~3 MB per reader, the round arithmetic, eager deletion of consumed files, and the disk-I/O trade
- [UnsafeSorterSpillReader.java:71](https://github.com/apache/spark/blob/v4.2.0/core/src/main/java/org/apache/spark/util/collection/unsafe/sort/UnsafeSorterSpillReader.java#L71) — read-ahead is on by default and wraps the *decompressed* stream, so it costs a thread and a second buffer per open reader
- [UnsafeSorterSpillReader.java:68](https://github.com/apache/spark/blob/v4.2.0/core/src/main/java/org/apache/spark/util/collection/unsafe/sort/UnsafeSorterSpillReader.java#L68) — `spark.unsafe.sorter.spill.reader.buffer.size`, 1 MB, the dominant term in that ~3 MB

!!! warning "The OOM this fixes is off by default"

    `spark.unsafe.sorter.spill.merge.factor` is **internal and defaults to -1**, meaning the legacy unbounded merge. A task that spills heavily still opens every reader simultaneously and can OOM *during the merge*, after the sort itself succeeded — the confusing shape where a job dies at the end of a long-running stage. The doc suggests 64, which it says needs at most one intermediate round for ~4000 spills.

!!! info "Merging is not the only thing spilling costs"

    `spark.shuffle.spill.batchSize` (10000) sets how many records are serialized per batch when writing a spill; the serializer is reset between batches to bound its object-reference table. This is a write-side knob and is separate from the merge factor.

**Configs:** `spark.unsafe.sorter.spill.merge.factor` (4.2.0, internal, -1), `spark.unsafe.sorter.spill.read.ahead.enabled`, `spark.unsafe.sorter.spill.reader.buffer.size`, `spark.shuffle.spill.batchSize`, `spark.shuffle.file.merge.buffer`

**Maps to topics:** E1, A4

---

## Host-local disk reading

**What it is:** the fourth fetch mode, and the one most people do not know exists. A block on a *different executor on the same host* is not fetched over the network — the reducer asks that executor's block manager for its local directories, then reads the file directly off shared disk. It needs the external shuffle service, and it is on by default.

**Code path:** `partitionBlocksByFetchMode` → same host and `hostLocalDirManager` defined? → `hostLocalBlocksByExecutor` → `fetchHostLocalBlocks` → `getHostLocalDirs` (cached per executor) → `getHostLocalShuffleData` → direct file read

**Anchor files:**

- [ShuffleBlockFetcherIterator.scala:430](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/ShuffleBlockFetcherIterator.scala#L430) — the branch: `hostLocalDirManager.isDefined && address.host == blockManagerId.host`, evaluated **after** the local-executor branch and before remote
- [ShuffleBlockFetcherIterator.scala:612](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/ShuffleBlockFetcherIterator.scala#L612) — `fetchHostLocalBlock`, reading the file with no network round trip
- [ShuffleBlockFetcherIterator.scala:625](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/ShuffleBlockFetcherIterator.scala#L625) — "If we see an exception, stop immediately": a host-local read failure is not retried remotely, it fails the fetch
- [ShuffleBlockFetcherIterator.scala:464](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/ShuffleBlockFetcherIterator.scala#L464) — the per-iterator INFO line that reports the host-local block count and bytes; the one place you can see whether it is working
- [SparkContext.scala:3354](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/SparkContext.scala#L3354) — `local-cluster` mode deliberately forces `spark.shuffle.readHostLocalDisk` off, because every executor shares a host there and it would mask remote-fetch bugs in tests

!!! info "Enabled by default, but silently inert without the shuffle service"

    `spark.shuffle.readHostLocalDisk` is `true`, but the branch also requires `hostLocalDirManager`, which exists only when an external shuffle client is configured. On a cluster without the shuffle service, every same-host block goes over the loopback network instead — a real cost on dense nodes, with no log line saying so. The block-count breakdown at L464 is how to check.

**Configs:** `spark.shuffle.readHostLocalDisk`, `spark.shuffle.service.enabled`

**Maps to topics:** A4, I5, E2

---

## Shuffle cleanup and the shuffle service state DB

**What it is:** two pieces of housekeeping that decide whether shuffle files ever go away. `spark.shuffle.service.removeShuffle` lets the driver tell the *service* to delete a shuffle's files when the shuffle is unregistered, rather than leaving them until the application ends. And the service itself keeps a local state DB — RocksDB by default — so it can serve blocks for executors that registered before a service restart.

**Anchor files:**

- [BlockManagerMasterEndpoint.scala:126](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/BlockManagerMasterEndpoint.scala#L126) — removal is attempted only when an external block-store client exists *and* the config is on
- [DiskBlockManager.scala:88](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/DiskBlockManager.scala#L88) — the same config also decides whether the executor keeps its shuffle directories alive for the service
- [ExternalShuffleService.scala:89](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/deploy/ExternalShuffleService.scala#L89) — the DB backend is read and logged at startup
- [BlockManager.scala:1520](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/BlockManager.scala#L1520) — `spark.shuffle.sync` (default `false`): with it on, every shuffle file write is fsynced before the writer reports success

!!! info "Long-lived applications leak shuffle files without this"

    Before `removeShuffle` (3.3.0, now default `true`), the service held every shuffle's files for the life of the application. A long-running Connect or notebook session accumulated them until the driver exited. Both halves must line up: the driver-side removal at `BlockManagerMasterEndpoint` and the executor-side directory retention at `DiskBlockManager` read the same key.

**Configs:** `spark.shuffle.service.removeShuffle`, `spark.shuffle.service.db.enabled`, `spark.shuffle.service.db.backend` (ROCKSDB), `spark.shuffle.service.name`, `spark.shuffle.service.port`, `spark.shuffle.registration.timeout`, `spark.shuffle.registration.maxAttempts`, `spark.shuffle.sync`

**Maps to topics:** E2, E1

---

## Sweep log

| Date | Spark | What changed |
|---|---|---|
| 2026-07-25 | 4.2.0 | Re-sweep at the same Spark version, driven by the config-slice breadth check. Four concepts added from keys tied to nothing: **map output size representation** (proposed as **A20** — the biggest find, since every size-based runtime decision including AQE skew detection reads byte-compressed and, above 2000 partitions, *averaged* sizes, with the skew-accuracy path off by default), **spill file merging and read-ahead** (including `spark.unsafe.sorter.spill.merge.factor`, new in 4.2.0 and defaulting to the legacy unbounded merge), **host-local disk reading**, and **shuffle cleanup and the service state DB**. Two existing concepts deepened rather than left thin: the per-codec tuning knobs under compression-codecs, and the two independent ways radix sort gets selected under UnsafeExternalSorter. `groups.yaml` extended to name `MapStatus` / `HighlyCompressedMapStatus` and `UnsafeSorterBoundedSpillMerger` — MapStatus.scala sits in `scheduler/`, so neither this sweep nor the execution-engine sweep had claimed it. |
| 2026-07-19 | 4.2.0 | Initial sweep, in two halves (shuffle write/read; memory, Tungsten, compression). 27 concepts. Two gaps proposed: A15 (push-based shuffle) and E14 (unmanaged memory). Two further gaps folded into existing topics rather than proposed as new ones, and the folding was done, not just noted: the fetch-side failure taxonomy (three in-flight limits, the single-retry corruption budget, the Netty-OOM circuit breaker) now sits in **A13** alongside the driver-side half it completes, and the cross-cutting observability gap — writer selection, merge strategy, batch-fetch eligibility and merger-threshold failure all being debug-level or silent — now sits in **E3**, with the practical remedy of raising `org.apache.spark.shuffle` and `org.apache.spark.storage` to DEBUG on one representative run. |
