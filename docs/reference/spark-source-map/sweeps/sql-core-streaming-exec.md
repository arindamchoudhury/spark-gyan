---
subsystem: sql/core
spark_version: "4.2.0"
swept_at: 2026-08-09
group: streaming-exec
all_groups: [query-execution, joins-exec, adaptive, datasources, agg-window-exchange, python-arrow, streaming-exec, classic-api, sql-scripting]
status: complete
concepts:
  - name: StreamExecution — one thread, a state machine, and a stored death cause
    topics: [A7]
  - name: TriggerExecutor — four trigger shapes behind one interface
    topics: [A7]
  - name: MicroBatchExecution — the batch loop, and the two-log write-ahead protocol
    topics: [A36]
  - name: No-data batches — the second reason a trigger fires
    topics: [A7, A8]
  - name: StreamExecutionContext and StreamProgress — per-batch state, and why offsets are a map
    topics: [A7]
  - name: Trigger.AvailableNow — three wrappers that fake a bounded source
    topics: [A7]
  - name: Async progress tracking — taking the checkpoint write off the critical path
    topics: [A7]
  - name: ProgressReporter, MetricsReporter and the listener bus — where StreamingQueryProgress comes from
    topics: [A7, E3]
  - name: HDFSMetadataLog and CompactibleFileStreamLog — an append-only log on a filesystem that may not rename atomically
    topics: [A7, E3]
  - name: CheckpointFileManager — atomic-rename abstraction, and the 4.1.0 checksum wrapper
    topics: [A7, E2]
  - name: IncrementalExecution — the rules that turn a batch plan into a stateful one
    topics: [A8, A1]
  - name: WatermarkTracker — one global watermark, and the min/max policy that decides it
    topics: [A8]
  - name: WatermarkPropagator — two watermarks per operator, and why a chained stateful query needs a simulation
    topics: [A8]
  - name: The StateStore API — versions, column families, and a commit that returns a number
    topics: [A8]
  - name: StateStoreRDD and the coordinator — placing a partition where its state already is
    topics: [A8, I5]
  - name: HDFSBackedStateStoreProvider — deltas, snapshots, and the whole map in JVM memory
    topics: [A8, E1]
  - name: RocksDBStateStoreProvider — changelog checkpointing and the snapshot upload queue
    topics: [E27]
  - name: The RocksDB tuning surface — a two-map lookup, and thirty keys no ConfigBuilder declares
    topics: [E27]
  - name: Range-scan key encoding — why byte order is not number order
    topics: [E27, A8]
  - name: Timestamp key encoders — the physical layout behind timestamp-ordered eviction
    topics: [A8]
  - name: Avro state encoding and state schema evolution — schema IDs, a broadcast, and two ceilings
    topics: []
    propose:
      code: E47
      level: Expert
      title: "Avro State Encoding and State Schema Evolution"
      what: "Setting `spark.sql.streaming.stateStore.encodingFormat=avro` replaces the UnsafeRow byte layout in the state store with Avro-encoded rows prefixed by a two-byte schema id, which is what allows a `transformWithState` value schema to change between restarts: the checkpoint keeps every historical schema, the driver broadcasts them to executors, and each stored row is decoded with the schema it was written under."
      why: "It is the only mechanism in Spark that lets a stateful query's state schema evolve rather than forcing a checkpoint rebuild, and every part of it is conditional — it works only with Avro encoding, only on `transformWithState`, only for the value side, only for Avro-compatible changes, and only sixteen times per column family before the query fails."
  - name: State checkpoint IDs — the V2 lineage that makes a state store verifiable
    topics: [A8, A14]
  - name: Row checksums and auto snapshot repair — two 4.1.0 corruption defences, one on by default
    topics: [A8, E3]
  - name: State schema evolution — StateSchemaCompatibilityChecker and the operator metadata log
    topics: [A8, B5]
  - name: Offline state repartition — changing a stateful query's partition count
    topics: [E28]
  - name: The maintenance thread — snapshotting, cleanup, and unloading providers
    topics: [A8, E3]
  - name: statefulOperators — the base traits, the watermark predicates, and the metrics
    topics: [A8]
  - name: Streaming aggregation and session windows — two state managers, two formats
    topics: [A8, B6]
  - name: Streaming deduplication and limits — the cheapest stateful operators, and their traps
    topics: [A8]
  - name: flatMapGroupsWithState — the legacy arbitrary-state operator and GroupState
    topics: [A8]
  - name: Stream-stream join — four state stores per side, and the eviction predicates
    topics: [A8, B7]
  - name: transformWithState — a handle, typed state variables, timers and TTL
    topics: [A8]
  - name: TTL indexes — one-to-one, one-to-many, and the work-queue trick
    topics: [A8]
  - name: FileStreamSource — the seen-files map, the trigger limits, and cleanSource
    topics: [A7, B4]
  - name: FileStreamSink and the _spark_metadata log — a sink whose output only Spark can read correctly
    topics: [A7, B4]
  - name: The built-in sources and sinks — rate, socket, memory, console, foreach
    topics: [A7]
  - name: How a source actually implements MicroBatchStream — the rate sources as the readable example
    topics: [A7]
  - name: The DSv2 streaming write path — MicroBatchWrite, V2Writes, and the V1 marker that gets deleted
    topics: []
    propose:
      code: A45
      level: Advanced
      title: "Writing a Streaming Sink: the DSv2 StreamingWrite Path and Epoch-Id Idempotence"
      what: "A streaming sink is a DSv2 `SupportsWrite` table whose `StreamingWrite` is wrapped per batch in a `MicroBatchWrite` carrying that batch's id, so the ordinary batch write machinery — writer factory per partition, task-side `commit`/`abort`, driver-side `commit(epochId, messages)` — executes each micro-batch, while the older DSv1 `Sink.addBatch` path survives as a marker node that the streaming optimizer deletes."
      why: "Every custom sink, every `foreachBatch` alternative and every 'my sink wrote the batch twice' incident lives here: the batch id handed to `commit` is the *only* thing that makes a sink idempotent across the replay that the checkpoint protocol guarantees, and the two write paths (V1 and V2) differ in whether Spark can invalidate the relation, refresh the catalog, or report commit progress at all."
  - name: Continuous processing — the epoch protocol, and why it is a different engine
    topics: []
    propose:
      code: E48
      level: Expert
      title: "Continuous Processing and the Epoch Coordinator"
      what: "Continuous processing is Spark's other streaming engine: instead of a batch loop it launches tasks that never finish, and it establishes durability with *epochs* — a driver-side `EpochCoordinator` RPC endpoint increments an epoch counter on a timer, collects an end offset from every reader partition and a commit message from every writer partition, and only writes the offset and commit logs once every partition has reported."
      why: "It is the clearest worked example in Spark of a distributed two-phase commit over long-running tasks, it explains exactly why continuous processing is at-least-once, unshuffleable and retry-intolerant, and it is the model Spark 4.2.0's Real-Time Mode was written to replace — so understanding it is how you read what Real-Time Mode actually changed."
  - name: The continuous reader and writer tasks — two background threads, an epoch marker, and no retries
    topics: [E48]
  - name: Real-Time Mode — an allowlist as the feature gate
    topics: [A7]
---

# sql/core — streaming execution

> Source sweep of the `streaming-exec` group: `execution/streaming/` and everything beneath it —
> `runtime/` (37), `state/` (29), `sources/` (18), `continuous/` (11), `checkpointing/` (10),
> `operators/stateful/` and its six nested sub-packages (34), `sinks/` (2), `utils/` (1), plus 7
> top-level files. **149 files**, swept against **Spark 4.2.0**.

This is the largest group in the map, and it is really three systems stacked: a *query lifecycle*
(a thread, a trigger, a batch loop), a *durability protocol* (two append-only logs on a
filesystem), and a *state engine* (a versioned key-value store per partition, with its own
checkpointing, maintenance and recovery). Almost every operational question about streaming lands
in one of the three, and they fail in different ways.

!!! info "`status: complete` as of the 2026-08-09 re-sweep — what changed"

    The first pass marked this page `partial` on a **depth** judgement: `continuous/` and
    `sources/` had one survey concept each, and the state-store encoders, the RocksDB tuning
    surface and the TTL machinery were named rather than traced. The re-sweep took exactly that
    list and closed it — nine new concepts, three of them new learning-path topics. What is
    still deliberately shallow is now small enough to name in one paragraph, at the end of
    breadth check 2.

!!! info "What is *not* here"

    Structured Streaming's *analysis-time* rules (`UnsupportedOperationChecker`, the streaming
    markers on logical plans) are `sql/catalyst`'s. The Python streaming operators are
    `execution/python/streaming/`, swept by the `python-arrow` group. The Kafka connector is a
    separate module entirely. The `state` and `state-metadata` DSv2 read sources — the supported
    way to inspect a checkpoint — are in `execution/datasources/v2/state/`, swept by `datasources`.

---

## The query lifecycle

### StreamExecution — one thread, a state machine, and a stored death cause

**What it is:** every streaming query is one JVM thread. `StreamExecution.start()` launches a
`QueryExecutionThread`, which runs `runStream()` — the wrapper that posts the
`QueryStartedEvent`, initializes the checkpoint directory, calls the subclass's
`runActivatedStream`, and catches everything. The query's state is an `AtomicReference[State]`
over `INITIALIZING` / `ACTIVE` / `RECONFIGURING` / `TERMINATED`.

The design consequence people meet first: **a failure does not throw on your thread.** It is stored
in `streamDeathCause` as a `StreamingQueryException` and re-thrown from `awaitTermination`,
`awaitInitialization` and `processAllAvailable`. A query that "silently stopped" has a death cause
waiting in `query.exception`.

`interruptAndAwaitExecutionThreadTermination` is the stop path, and it is a bounded interrupt
loop rather than a single interrupt — a source blocked in a network call may not notice the first
one.

**Code path:** `StreamingQueryManager.startQuery` → `StreamExecution.start()` →
`QueryExecutionThread.run` → `runStream()` → `runActivatedStream()` (subclass)

**Anchor files:**

- [runtime/StreamExecution.scala:74](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/StreamExecution.scala#L74) — the abstract class; the state trait at :58
- [runtime/StreamExecution.scala:230](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/StreamExecution.scala#L230) — `queryExecutionThread`
- [runtime/StreamExecution.scala:282](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/StreamExecution.scala#L282) — `runStream`, the whole lifecycle including where `streamDeathCause` is set
- [runtime/StreamExecution.scala:500](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/StreamExecution.scala#L500) — `interruptAndAwaitExecutionThreadTermination`
- [runtime/StreamingQueryWrapper.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/StreamingQueryWrapper.scala) — the serializable façade handed to user code, so a `StreamingQuery` reference in a closure does not drag the executor in
- [runtime/ErrorNotifier.scala:27](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/ErrorNotifier.scala#L27) — how an async background failure is surfaced back into the batch loop

**Maps to topics:** A7

### TriggerExecutor — four trigger shapes behind one interface

**What it is:** the trigger is not a scheduling policy inside the batch loop; it *is* the loop.
`TriggerExecutor.execute(batchRunner)` owns the iteration, and four implementations differ only in
how they call back:

| Executor | Trigger | Behaviour |
|---|---|---|
| `SingleBatchExecutor` | `Trigger.Once` (deprecated) | calls the runner exactly once |
| `MultiBatchExecutor` | `Trigger.AvailableNow` | loops until the runner reports no more data |
| `ProcessingTimeExecutor` | `Trigger.ProcessingTime(t)` | runs, then sleeps until the next multiple of `t` |
| — | `Trigger.Continuous` / `RealTimeTrigger` | a different execution class entirely, not a trigger executor |

`ProcessingTimeExecutor` is the one worth reading: it computes the next trigger time from the
clock, and if the batch overran the interval it calls `notifyBatchFallingBehind` and starts the
next batch immediately. There is no queueing and no catch-up — a query that consistently overruns
simply runs back-to-back, which is why "my trigger is 10s but batches run continuously" is normal
rather than a bug.

**Anchor files:**

- [runtime/TriggerExecutor.scala:25](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/TriggerExecutor.scala#L25) — the trait, and all four implementations below it
- [runtime/TriggerExecutor.scala:79](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/TriggerExecutor.scala#L79) — `ProcessingTimeExecutor.execute` and the falling-behind path

**Maps to topics:** A7

### MicroBatchExecution — the batch loop, and the two-log write-ahead protocol

**What it is:** the core of Structured Streaming. `runActivatedStream` builds the first
`MicroBatchExecutionContext` and hands `executeOneBatch` to the trigger executor. Each call:

1. `constructNextBatch` — ask every source for its latest offset, decide whether a batch should
   run at all, and if so **write the batch's end offsets to the offset log** (`markMicroBatchStart`,
   timed as `walCommit`). Only *after* that write is it safe to let sources discard the previous
   batch's data and to purge old log entries.
2. `runBatch` — plan and execute, through `IncrementalExecution`.
3. `markMicroBatchEnd` — write the commit log entry, advance the watermark, and clean up.

That ordering is the whole durability story. On restart, `populateStartOffsets` reads the latest
offset-log entry and the latest commit-log entry and compares them:

- **offset entry with a matching commit entry** → that batch finished; start the next one.
- **offset entry with no commit entry** → the batch was durable but unfinished;
  `isCurrentBatchConstructed = true` and Spark **re-runs exactly that batch** with the same offsets.

That is why re-processing after a crash produces the *same* batch rather than a different slice,
and it is why a sink must be idempotent for the same batch id rather than merely "at least once".
Recovery also restores the watermark from the offset-log metadata, and from the commit log's
`nextBatchWatermarkMs` when it is higher.

`validateOffsetLogAndGetPrevOffset` guards a subtle failure: restarting at batch N needs entry
N-1 as well, and its error message tells you so explicitly — a partially deleted log directory
fails here rather than silently restarting from the wrong place.

**Code path:** `triggerExecutor.execute` → `executeOneBatch` → `constructNextBatch` →
`markMicroBatchStart` (offset log) → `runBatch` → `markMicroBatchEnd` (commit log)

**Anchor files:**

- [runtime/MicroBatchExecution.scala:601](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/MicroBatchExecution.scala#L601) — `runActivatedStream`
- [runtime/MicroBatchExecution.scala:613](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/MicroBatchExecution.scala#L613) — `executeOneBatch`, the whole trigger body
- [runtime/MicroBatchExecution.scala:743](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/MicroBatchExecution.scala#L743) — `populateStartOffsets`, the restart decision and the watermark restore
- [runtime/MicroBatchExecution.scala:984](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/MicroBatchExecution.scala#L984) — the `walCommit` block, with the comment on why cleanup must follow the log write
- [runtime/MicroBatchExecution.scala:1276](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/MicroBatchExecution.scala#L1276) — `markMicroBatchStart`, and the `concurrentStreamLogUpdate` error when two writers race
- [runtime/MicroBatchExecution.scala:704](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/MicroBatchExecution.scala#L704) — `validateOffsetLogAndGetPrevOffset`

**Configs:** `spark.sql.streaming.minBatchesToRetain`,
`spark.sql.streaming.offsetLog.formatVersion` (`1`, 4.2.0),
`spark.sql.streaming.verifyCheckpointDirectoryEmptyOnStart` (`true`, 4.1.0),
`spark.sql.streaming.checkpoint.verifyMetadataExists.enabled` (`true`, 4.2.0)

**Maps to topics:** A36 (the topic this sweep's first pass proposed, now in the learning path)

### No-data batches — the second reason a trigger fires

**What it is:** `constructNextBatch` runs a batch when there is new data **or** when
`lastExecutionRequiresAnotherBatch` — that is, when the previous batch's executed plan answers true
to `shouldRunAnotherBatch(offsetSeqMetadata)`. Every stateful operator implements that method
against the new watermark: a windowed aggregate returns true while it still holds windows the
watermark has now passed.

So a batch with zero input rows is a designed behaviour, not an anomaly — it is how state is
evicted and how a final window is emitted after the source goes quiet. The status message even
distinguishes them: "Processing new data" versus "No new data but cleaning up state".

`spark.sql.streaming.noDataMicroBatches.enabled` turns it off, and the consequence is precisely
that windows stop being emitted once input stops.

**Anchor files:**

- [runtime/MicroBatchExecution.scala:971](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/MicroBatchExecution.scala#L971) — `lastExecutionRequiresAnotherBatch`
- [operators/stateful/statefulOperators.scala:526](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/operators/stateful/statefulOperators.scala#L526) — `shouldRunAnotherBatch`, default `false`, overridden per operator

**Configs:** `spark.sql.streaming.noDataMicroBatches.enabled`

**Maps to topics:** A7, A8

### StreamExecutionContext and StreamProgress — per-batch state, and why offsets are a map

**What it is:** everything that varies per batch — batch id, start/end/latest offsets, the executed
plan, the status, the timing map — lives in a `MicroBatchExecutionContext`, not on the execution
object. `getNextContext()` produces the successor. This is what makes the "previous batch"
reachable for `shouldRunAnotherBatch` and what async progress tracking overrides.

`StreamProgress` is a `Map[SparkDataStream, Offset]` with the ordering semantics a multi-source
query needs: offsets are per source, so "the query's offset" is never a single number, and adding
or removing a source changes the map's shape — which is the problem 4.2.0's named sources solve.

**Anchor files:**

- [runtime/StreamExecutionContext.scala:33](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/StreamExecutionContext.scala#L33) — the abstract context and its micro-batch subclass
- [runtime/StreamProgress.scala:29](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/StreamProgress.scala#L29) — the offset map
- [runtime/StreamingRelation.scala:36](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/StreamingRelation.scala#L36) — the logical leaf that carries a source into the plan
- [runtime/ResolveWriteToStream.scala:41](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/ResolveWriteToStream.scala#L41) — the analyzer rule that resolves the sink and validates the checkpoint location
- [runtime/LongOffset.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/LongOffset.scala) / [runtime/SerializedOffset.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/SerializedOffset.scala) / [runtime/RateStreamOffset.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/RateStreamOffset.scala) — the trivial offset types every source example uses; `RateStreamOffset` is a per-partition map rather than a single number, which is the smallest illustration of why an offset is source-defined

**Maps to topics:** A7

### Trigger.AvailableNow — three wrappers that fake a bounded source

**What it is:** `Trigger.AvailableNow` means "process everything currently available, in several
batches, then stop". Sources do not implement that directly. Instead three wrapper classes
snapshot the source's latest offset once at the start and then serve batches up to that frozen
point: `AvailableNowDataStreamWrapper`, `AvailableNowSourceWrapper` (V1) and
`AvailableNowMicroBatchStreamWrapper` (V2). The loop is `MultiBatchExecutor`, which terminates the
query when a trigger produces no batch.

The wrapper is why `Trigger.AvailableNow` respects `maxFilesPerTrigger` and `maxOffsetsPerTrigger`
while `Trigger.Once` did not — and why a source that does not support the wrapper falls back to
one giant batch.

**Anchor files:**

- [runtime/AvailableNowDataStreamWrapper.scala:30](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/AvailableNowDataStreamWrapper.scala#L30) — the base wrapper
- [runtime/AvailableNowSourceWrapper.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/AvailableNowSourceWrapper.scala) / [runtime/AvailableNowMicroBatchStreamWrapper.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/AvailableNowMicroBatchStreamWrapper.scala) — the V1 and V2 forms
- [runtime/AcceptsLatestSeenOffsetHandler.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/AcceptsLatestSeenOffsetHandler.scala) — how a source is told the latest offset the engine has seen, on restart
- [runtime/MicroBatchExecution.scala:688](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/MicroBatchExecution.scala#L688) — where a `MultiBatchExecutor` query sets itself `TERMINATED`

**Maps to topics:** A7

### Async progress tracking — taking the checkpoint write off the critical path

**What it is:** the `walCommit` in every batch is a durable filesystem write, and on object storage
it can dominate a short batch. `AsyncProgressTrackingMicroBatchExecution` overrides the log writes
to happen on a thread pool, backed by `AsyncOffsetSeqLog` and `AsyncCommitLog`, with a configurable
checkpointing interval so several batches can share one durable write.

The trade is explicit and worth stating: with async tracking, more than one batch may be replayed
after a failure, so the sink must tolerate that. The class carries its own
`validateOffsetLogAndGetPrevOffset` override because "the previous batch" is no longer necessarily
in durable storage, and `AsyncLogPurge` moves log purging off the critical path too.

**Anchor files:**

- [runtime/AsyncProgressTrackingMicroBatchExecution.scala:36](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/AsyncProgressTrackingMicroBatchExecution.scala#L36) — the subclass and its executor pool
- [runtime/AsyncProgressTrackingMicroBatchExecution.scala:144](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/AsyncProgressTrackingMicroBatchExecution.scala#L144) — the async `markMicroBatchStart`
- [checkpointing/AsyncOffsetSeqLog.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/checkpointing/AsyncOffsetSeqLog.scala) / [checkpointing/AsyncCommitLog.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/checkpointing/AsyncCommitLog.scala)
- [runtime/AsyncLogPurge.scala:31](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/AsyncLogPurge.scala#L31)
- [runtime/AsyncStreamingQueryCheckpointMetadata.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/AsyncStreamingQueryCheckpointMetadata.scala)

**Maps to topics:** A7

### ProgressReporter, MetricsReporter and the listener bus — where StreamingQueryProgress comes from

**What it is:** `ProgressReporter` accumulates per-batch timings (`triggerExecution`, `walCommit`,
`getBatch`, `queryPlanning`, `addBatch`), input/processed rates per source, watermark, state
operator metrics and sink progress, and emits a `StreamingQueryProgress` at
`finishTrigger`. `MetricsReporter` exposes a subset through Spark's Dropwizard metrics system so
they reach an external monitoring backend, and `StreamingQueryListenerBus` fans the events out to
registered listeners — including, on Spark Connect, back to a remote client.

The practical point: `query.lastProgress` and the listener event are the *same* object, and its
`durationMs` map is the first place to look for a slow batch, because it separates planning from
`addBatch` from the WAL write.

**Anchor files:**

- [runtime/ProgressReporter.scala:55](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/ProgressReporter.scala#L55)
- [runtime/MetricsReporter.scala:34](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/MetricsReporter.scala#L34)
- [runtime/StreamingQueryListenerBus.scala:43](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/StreamingQueryListenerBus.scala#L43)

**Configs:** `spark.sql.streaming.metricsEnabled`,
`spark.sql.streaming.numRecentProgressUpdates`

**Maps to topics:** A7, E3

---

## The checkpoint

### HDFSMetadataLog and CompactibleFileStreamLog — an append-only log on a filesystem that may not rename atomically

**What it is:** the offset log, commit log, file-source log and file-sink log are all
`HDFSMetadataLog`: a directory of files named by batch id, one serialized entry each, written
through a `CheckpointFileManager` so that "add batch N" is an atomic create-if-absent. `add`
returning false is a concurrent writer, which is exactly the `concurrentStreamLogUpdate` error the
batch loop raises.

`CompactibleFileStreamLog` extends it for logs whose entries accumulate — the file-source and
file-sink logs list *files*, not offsets, so they are periodically **compacted**: every N batches
one entry contains the merged content of everything before it, and older files are deleted. That
compaction interval and its retention are the reason a long-running file-sink query's
`_spark_metadata` directory does not grow without bound — and the reason a compaction batch is
visibly slower than its neighbours.

`MetadataVersionUtil` is the tiny function that reads the `v1` / `v2` header at the top of every
entry and refuses a version from the future, which is how a downgrade fails loudly.

**Anchor files:**

- [checkpointing/HDFSMetadataLog.scala:51](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/checkpointing/HDFSMetadataLog.scala#L51) — the log; `MetadataLog` trait in [MetadataLog.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/checkpointing/MetadataLog.scala)
- [runtime/CompactibleFileStreamLog.scala:46](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/CompactibleFileStreamLog.scala#L46) — compaction
- [checkpointing/OffsetSeqLog.scala:54](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/checkpointing/OffsetSeqLog.scala#L54) and [checkpointing/OffsetSeq.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/checkpointing/OffsetSeq.scala) — the offset log and the metadata (watermark, batch timestamp, and the confs pinned into the checkpoint)
- [checkpointing/CommitLog.scala:51](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/checkpointing/CommitLog.scala#L51) — the commit log and its `nextBatchWatermarkMs`
- [checkpointing/MetadataVersionUtil.scala:22](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/checkpointing/MetadataVersionUtil.scala#L22)
- [runtime/StreamMetadata.scala:44](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/StreamMetadata.scala#L44) — the `metadata` file holding the query's stable id
- [runtime/StreamingQueryCheckpointMetadata.scala:33](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/StreamingQueryCheckpointMetadata.scala#L33) — the object that owns all of the above for one query
- [runtime/StreamingCheckpointConstants.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/StreamingCheckpointConstants.scala) — the directory names (`offsets`, `commits`, `sources`, `state`, `metadata`)

**Configs:** `spark.sql.streaming.fileSource.log.compactInterval`,
`spark.sql.streaming.fileSink.log.compactInterval`, `…log.cleanupDelay`, `…log.deletion`

**Maps to topics:** A7, E3

### CheckpointFileManager — atomic-rename abstraction, and the 4.1.0 checksum wrapper

**What it is:** the whole protocol assumes "create this file atomically, or fail". Not every
filesystem gives that, so `CheckpointFileManager` is the abstraction: a `createAtomic` returning a
`CancellableFSDataOutputStream`, implemented once over HDFS-style rename and once over filesystems
without it. It is pluggable per scheme.

`ChecksumCheckpointFileManager` (new in 4.1.0, **on by default**) decorates any implementation:
every file written gets a sibling checksum file carrying a `CRC32C` plus creator information, and
reads verify it. `…skipCreationIfFileMissingChecksum` (also default true) keeps it compatible with
checkpoints written before the feature existed. This is the defence against a truncated or
partially written log entry — the failure mode that used to present as an unparseable checkpoint.

**Anchor files:**

- [checkpointing/CheckpointFileManager.scala:52](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/checkpointing/CheckpointFileManager.scala#L52) — the trait and its implementations
- [checkpointing/ChecksumCheckpointFileManager.scala:105](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/checkpointing/ChecksumCheckpointFileManager.scala#L105) — the decorator; the checksum record at :59

**Configs:** `spark.sql.streaming.checkpointFileManagerClass`,
`spark.sql.streaming.checkpoint.fileChecksum.enabled` (`true`, 4.1.0),
`spark.sql.streaming.checkpoint.fileChecksum.skipCreationIfFileMissingChecksum` (`true`, 4.1.0),
`spark.sql.streaming.stateStore.fileChecksumThreadPoolSize` (`4`, 4.2.0)

**Maps to topics:** A7, E2

---

## Planning a batch

### IncrementalExecution — the rules that turn a batch plan into a stateful one

**What it is:** the `QueryExecution` subclass used for every micro-batch. It runs a set of
`SparkPlanPartialRule`s the batch path does not have, and they are where the streaming-specific
plan properties come from:

- **`ShufflePartitionsRule`** stamps `numShufflePartitions = numStateStores` onto every stateful
  operator — the partition count read from the *checkpoint*, not from
  `spark.sql.shuffle.partitions`. Changing that config on an existing stateful query therefore does
  nothing, which is the whole motivation for offline state repartition.
- **`StateOpIdRule`** assigns each stateful operator a stable `StatefulOperatorStateInfo`
  (operator id, batch id/version, number of partitions, checkpoint ids). Operator ids are assigned
  by an atomic counter **in plan traversal order** — which is why inserting a stateful operator in
  the middle of an existing query invalidates the checkpoint.
- **`StateSchemaAndOperatorMetadataRule`** validates each operator's state schema against what the
  checkpoint recorded, and broadcasts the resulting schema metadata to executors.
- **`WatermarkPropagationRule`** attaches the per-operator watermarks (below).
- **`ConvertLocalLimitRule`** rewrites a `LocalLimit` above a stateful operator into a
  `StreamingLocalLimitExec`, because a plain limit is not deterministic across batches.

**Anchor files:**

- [runtime/IncrementalExecution.scala:112](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/IncrementalExecution.scala#L112) — `numStateStores`, read from the offset-log metadata
- [runtime/IncrementalExecution.scala:159](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/IncrementalExecution.scala#L159) — `nextStatefulOperationStateInfo`, the counter
- [runtime/IncrementalExecution.scala:178](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/IncrementalExecution.scala#L178) — `ShufflePartitionsRule`
- [runtime/IncrementalExecution.scala:198](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/IncrementalExecution.scala#L198) — `ConvertLocalLimitRule`
- [runtime/IncrementalExecution.scala:236](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/IncrementalExecution.scala#L236) — `StateSchemaAndOperatorMetadataRule`
- [runtime/IncrementalExecution.scala:315](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/IncrementalExecution.scala#L315) — `StateOpIdRule`
- [operators/stateful/StatefulOperatorPartitioning.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/operators/stateful/StatefulOperatorPartitioning.scala) — how the required distribution is built from that pinned partition count

**Configs:** `spark.sql.streaming.internal.stateStore.partitions` (internal, 4.1.0),
`spark.sql.adaptive.streaming.stateless.enabled` (`true`, 4.1.0)

**Maps to topics:** A8, A1

### WatermarkTracker — one global watermark, and the min/max policy that decides it

**What it is:** each `EventTimeWatermarkExec` in the plan reports the maximum event time it saw
minus its delay threshold. The tracker collects all of them and reduces them to **one** global
watermark by a policy: `MinWatermark` (the default) takes the slowest, `MaxWatermark` the fastest.
The global watermark is then **monotonic** — it is only updated when the chosen value is strictly
greater, so a source going backwards cannot pull it down.

The consequence for a multi-source query is the one people hit: under the default policy, one
lagging or idle source holds the watermark back for the entire query, so windows never close and
state never evicts. Switching to `max` closes them, at the cost of dropping late data from the
lagging source.

**Anchor files:**

- [runtime/WatermarkTracker.scala:85](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/WatermarkTracker.scala#L85) — the tracker; `updateWatermark` at :106 and the monotonic check at :141
- [runtime/WatermarkTracker.scala:62](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/WatermarkTracker.scala#L62) — `MinWatermark` / `MaxWatermark`
- [operators/stateful/EventTimeWatermarkExec.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/operators/stateful/EventTimeWatermarkExec.scala) — the operator that observes event times and the `UpdateEventTimeColumnExec` that rewrites the column

**Configs:** `spark.sql.streaming.multipleWatermarkPolicy` (`min`),
`spark.sql.streaming.validateEventTimeWatermarkColumn` (`true`, 4.2.0)

**Maps to topics:** A8

### WatermarkPropagator — two watermarks per operator, and why a chained stateful query needs a simulation

**What it is:** a single global watermark is wrong for a plan with *several* stateful operators in
sequence, because an operator downstream of a windowed aggregate sees rows whose event times were
already advanced by that aggregate. `WatermarkPropagator` computes, per batch and per operator id,
**two** values:

- `getInputWatermarkForLateEvents` — the threshold for dropping a row as late;
- `getInputWatermarkForEviction` — the threshold for evicting state.

Three implementations exist: `NoOpWatermarkPropagator` (no watermark in the query),
`UseSingleWatermarkPropagator` (the legacy behaviour — the same global value everywhere), and
`PropagateWatermarkSimulator`, which walks the plan simulating how each operator transforms the
watermark of its children. The simulator is what makes a chained stateful query — say a windowed
aggregate feeding a stream-stream join — evict correctly instead of dropping rows the downstream
operator had not yet seen.

**Anchor files:**

- [runtime/WatermarkPropagator.scala:39](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/WatermarkPropagator.scala#L39) — the trait and the two-watermark contract
- [runtime/WatermarkPropagator.scala:79](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/WatermarkPropagator.scala#L79) — `UseSingleWatermarkPropagator`
- [runtime/WatermarkPropagator.scala:160](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/WatermarkPropagator.scala#L160) — `PropagateWatermarkSimulator` and `doSimulate`
- [operators/stateful/statefulOperators.scala:530](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/operators/stateful/statefulOperators.scala#L530) — `WatermarkSupport`, where the two values become predicates

**Configs:** `spark.sql.streaming.statefulOperator.useStrictDistribution`,
`spark.sql.streaming.statefulOperator.allowMultiple`

**Maps to topics:** A8

---

## The state store

### The StateStore API — versions, column families, and a commit that returns a number

**What it is:** the abstraction every stateful operator uses. A `StateStore` is opened *at a
version* — the batch id — and `commit()` returns the new version. `ReadStateStore` is the
read-only form used by restore-side operators. The API is richer than a map:

- **column families** — named sub-keyspaces within one store, which is how `transformWithState`
  puts each state variable, its TTL index and its timers in separate namespaces;
- **key encoder specs** — `NoPrefixKeyStateEncoderSpec`, `PrefixKeyScanStateEncoderSpec`,
  `RangeKeyScanStateEncoderSpec`, and the two timestamp variants — declaring what kind of *scan*
  the operator needs, which the provider turns into a physical encoding;
- **`putList` / `merge`** — for list state, so appending does not read-modify-write;
- **metrics** — `StateStoreMetrics` plus custom and *instance* metrics, including the snapshot
  upload lag metric the coordinator reports on.

`abort()` exists and matters: a failed task must abort rather than commit, or the next attempt
would see a half-written version.

**Anchor files:**

- [state/StateStore.scala:102](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/state/StateStore.scala#L102) — `ReadStateStore`; `StateStore` at :285
- [state/StateStore.scala:660](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/state/StateStore.scala#L660) — `KeyStateEncoderSpec` and the five variants
- [state/StateStore.scala:793](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/state/StateStore.scala#L793) — `StateStoreProvider`, the SPI
- [state/StateStore.scala:491](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/state/StateStore.scala#L491) — `StateStoreMetrics`; instance metrics at :563
- [state/StateStoreConf.scala:24](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/state/StateStoreConf.scala#L24) — every state-store config in one place, which is the fastest way to read the surface
- [state/StateStoreErrors.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/state/StateStoreErrors.scala) — the error catalogue, worth skimming for what the engine considers a violation
- [state/StateStoreRow.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/state/StateStoreRow.scala) / [state/package.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/state/package.scala)

**Configs:** `spark.sql.streaming.stateStore.providerClass`,
`spark.sql.streaming.stateStore.minDeltasForSnapshot`,
`spark.sql.streaming.stateStore.encodingFormat`

**Maps to topics:** A8

### StateStoreRDD and the coordinator — placing a partition where its state already is

**What it is:** a stateful operator's physical execution is a `StateStoreRDD` (or
`ReadStateStoreRDD`), whose `compute` opens the store for `(operator id, partition id, store name)`
at the current version and hands it to the operator's function. Its `getPreferredLocations` asks
the **`StateStoreCoordinator`** — a driver RPC endpoint tracking which executor last had each store
loaded — so a partition is scheduled where its RocksDB directory already exists. Getting that wrong
means a full snapshot download before the task can start.

The coordinator does more than placement: executors report snapshot uploads to it, and it reports
**lagging** stores — instances whose latest snapshot is far behind the current version — which is
how you find the one partition that will take an hour to recover.

**Anchor files:**

- [state/StateStoreRDD.scala:52](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/state/StateStoreRDD.scala#L52) — `BaseStateStoreRDD` and `getPreferredLocations` at :73
- [state/StateStoreCoordinator.scala:283](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/state/StateStoreCoordinator.scala#L283) — the endpoint; `StateStoreCoordinatorRef` at :139

**Configs:** `spark.sql.streaming.stateStore.coordinatorReportSnapshotUploadLag` (`true`, 4.1.0),
`…snapshotLagReportInterval` (5 min), `…maxLaggingStoresToReport` (`5`),
`…numStateStoreInstanceMetricsToReport` (`5`),
`…multiplierForMinVersionDiffToLog` / `…multiplierForMinTimeDiffToLog`

**Maps to topics:** A8, I5

### HDFSBackedStateStoreProvider — deltas, snapshots, and the whole map in JVM memory

**What it is:** the original provider, and still the default in some configurations. It keeps the
**entire state map for a partition in JVM heap** (`HDFSBackedStateStoreMap`) and persists it as
a `<version>.delta` file per batch, with a `<version>.snapshot` written by the maintenance thread
every `minDeltasForSnapshot` versions. Loading version N means loading the newest snapshot ≤ N and
replaying deltas.

Its defining limitation follows from that first sentence: state size is bounded by executor heap,
and GC pressure grows with it. That is the reason the RocksDB provider exists, and the reason
"switch the state store provider" is standard advice for large-state jobs.

**Anchor files:**

- [state/HDFSBackedStateStoreProvider.scala:72](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/state/HDFSBackedStateStoreProvider.scala#L72) — the provider; `doMaintenance` at :480, the file naming at :1198
- [state/HDFSBackedStateStoreMap.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/state/HDFSBackedStateStoreMap.scala) — the in-heap map, with a prefix-scan variant
- [state/HDFSBackedStateStoreProvider.scala:1374](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/state/HDFSBackedStateStoreProvider.scala#L1374) — the change-data reader that backs the state DSv2 source's change feed

**Maps to topics:** A8, E1

### RocksDBStateStoreProvider — changelog checkpointing and the snapshot upload queue

**What it is:** a local RocksDB instance per partition, in the executor's local directory, with
DFS used only for durability. Two persistence modes:

- **Snapshot-only (legacy):** each commit uploads a full checkpoint.
- **Changelog checkpointing:** each commit writes only that batch's mutations to a
  `<version>.changelog` file; full snapshots are produced asynchronously by the maintenance thread
  and pushed through `snapshotsToUploadQueue`. Loading version N means loading the nearest snapshot
  and replaying changelogs forward.

That asymmetry is the operational headline: commits become cheap and *recovery* becomes
proportional to the changelog distance since the last snapshot. A partition whose snapshot upload
has been lagging is a partition that will replay thousands of changelog files on restart — which is
exactly what the coordinator's lag reporting exists to surface, and what 4.2.0's
`forceSnapshotUploadOnLag` (default `true`) exists to prevent.

`RocksDBFileManager` handles the DFS side: SST and archived log files are **immutable and shared
between versions**, so it keeps a local-name → DFS-name mapping per version and uploads only new
files; everything else is zipped into a per-version metadata file. `RocksDBMemoryManager` gives
the instances a shared block-cache and write-buffer budget, `RocksDBStateEncoder` implements the
key encoder specs (including range-scan encoding with correct byte ordering for signed numbers),
and `RocksDBStateMachine` enforces the legal load/commit/abort transitions.

**Code path:** `StateStoreRDD.compute` → `RocksDBStateStoreProvider.getStore(version)` →
`RocksDB.load(version)` (snapshot + changelog replay) → operator mutations → `commit()` (changelog
write) → maintenance thread → `saveCheckpointToDfs`

**Anchor files:**

- [state/RocksDBStateStoreProvider.scala:41](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/state/RocksDBStateStoreProvider.scala#L41) — the provider; `doMaintenance` at :1039
- [state/RocksDB.scala:70](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/state/RocksDB.scala#L70) — the instance wrapper; the load path with snapshot selection and changelog replay from :507
- [state/RocksDB.scala:374](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/state/RocksDB.scala#L374) — `snapshotsToUploadQueue`
- [state/RocksDBFileManager.scala:129](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/state/RocksDBFileManager.scala#L129) — the file manager, with the immutable-file sharing model in its scaladoc from :64; `saveCheckpointToDfs` at :290, `loadCheckpointFromDfs` at :361, `deleteOldVersions` at :658
- [state/StateStoreChangelog.scala:100](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/state/StateStoreChangelog.scala#L100) — the changelog writer, its versions, and the `RecordType` enum
- [state/RocksDBStateEncoder.scala:46](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/state/RocksDBStateEncoder.scala#L46) — the key encoders
- [state/RocksDBMemoryManager.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/state/RocksDBMemoryManager.scala) / [state/RocksDBLoader.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/state/RocksDBLoader.scala) / [state/RocksDBStateMachine.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/state/RocksDBStateMachine.scala)
- [state/RangeScanBoundaryUtils.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/state/RangeScanBoundaryUtils.scala)

**Configs:** `spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled`,
the whole `spark.sql.streaming.stateStore.rocksdb.*` family (block cache, write buffer, compaction,
`trackTotalNumberOfRows`, `mergeOperatorVersion` = `2` in 4.2.0),
`spark.sql.streaming.stateStore.forceSnapshotUploadOnLag` (`true`, 4.2.0),
`spark.sql.streaming.stateStore.unloadOnCommit` (`false`, 4.1.0)

**Maps to topics:** E27

### The RocksDB tuning surface — a two-map lookup, and thirty keys no ConfigBuilder declares

**What it is:** `RocksDBConf` is a 31-field case class built once per provider from a
`StateStoreConf`, and the way it is built explains why the RocksDB knobs behave unlike every
other Spark config.

Each field is a `ConfEntry` with a **name and a default written in this file**, in one of two
flavours:

- `SQLConfEntry("blockCacheSizeMB", "8")` reads `spark.sql.streaming.stateStore.rocksdb.blockCacheSizeMB` out of `StateStoreConf.sqlConfs`, which is not a typed lookup at all — it is `sqlConf.getAllConfs.filter(_._1.startsWith("spark.sql.streaming.stateStore."))`, a prefix sweep of whatever the session happens to hold.
- `ExtraConfEntry(name, default)` reads `StateStoreConf.extraOptions` instead — the per-query options a `DataStreamWriter` passes down. No entry uses this flavour in 4.2.0; the branch exists so a provider can be tuned per query rather than per session.

Three consequences, all of them observable:

1. **Only two of these keys are registered configs.** `formatVersion` and `mergeOperatorVersion`
   are declared in `SQLConf` (and the source says in both places that the two definitions must be
   kept in sync). The other ~29 — `blockCacheSizeMB`, `writeBufferSizeMB`, `maxWriteBufferNumber`,
   `boundedMemoryUsage`, `maxMemoryUsageMB`, `writeBufferCacheRatio`, `highPriorityPoolRatio`,
   `compression`, `maxOpenFiles`, `compactOnCommit`, `changelogCheckpointing.enabled`,
   `trackTotalNumberOfRows`, `allowFAllocate`, `lockAcquireTimeoutMs`, `resetStatsOnLoad`,
   `verifyNonEmptyFilesInZip`, `checkStaleReusedFilesInSnapshot`, `memoryUpdateIntervalMs`, … —
   exist **only as strings here**. They are absent from the config catalog, from `SET -v`, and
   from any doc generated off `ConfigEntry`.
2. **A typo is silent.** `getConfigMap(conf).getOrElse(conf.fullName, conf.default)` falls back to
   the default for any key that is not present, and `fullName` is lower-cased, so lookup is
   case-insensitive but membership is not validated. Misspell `blockCacheSizeMB` and you get 8 MB
   with no warning anywhere.
3. **A few of the fields do not come from the rocksdb prefix at all.** `minVersionsToRetain`,
   `minDeltasForSnapshot`, `maxVersionsToDeletePerMaintenance`, `fileChecksumEnabled`,
   `rowChecksumEnabled` and `reportSnapshotUploadLag` are lifted off `StateStoreConf` — i.e. from
   real `SQLConf` entries under `spark.sql.streaming.stateStore.*`. So the RocksDB provider's
   behaviour is configured from two namespaces with two different validation stories.

`RocksDBMemoryManager` is where `boundedMemoryUsage` becomes real: with it on, every RocksDB
instance on the executor shares one `WriteBufferManager` and one LRU block cache sized
`maxMemoryUsageMB`, split by `writeBufferCacheRatio` and `highPriorityPoolRatio`. With it off —
the default — each partition's instance sizes its own cache, so executor memory scales with the
number of state stores on that executor rather than being capped.

**Code path:** `StateStoreConf(sqlConf)` (prefix sweep) → `RocksDBConf.apply(storeConf)`
(per-entry `getOrElse(default)`) → `RocksDB` (`Options`, `BlockBasedTableConfig`) /
`RocksDBMemoryManager.getOrCreateRocksDBMemory`

**Anchor files:**

- [state/RocksDB.scala:2699](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/state/RocksDB.scala#L2699) — the `RocksDBConf` case class, all 31 fields
- [state/RocksDB.scala:2733](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/state/RocksDB.scala#L2733) — the `ConfEntry` / `SQLConfEntry` / `ExtraConfEntry` machinery and every default; `apply` at :2855 with the `getOrElse(default)` lookups
- [state/RocksDB.scala:2774](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/state/RocksDB.scala#L2774) — `formatVersion`, and the comment explaining why the table format version is pinned into the checkpoint so a Spark downgrade still works
- [state/StateStoreConf.scala:174](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/state/StateStoreConf.scala#L174) — `sqlConfs`, the prefix sweep that makes the whole thing untyped
- [state/RocksDBMemoryManager.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/state/RocksDBMemoryManager.scala) — the shared block cache and write-buffer manager

**Configs:** `spark.sql.streaming.stateStore.rocksdb.formatVersion` (`5`),
`…rocksdb.mergeOperatorVersion` (`2`, 4.2.0) — the only two in the catalog; the rest are
string keys defined in `RocksDBConf` (listed above)

**Maps to topics:** E27

### Range-scan key encoding — why byte order is not number order

**What it is:** RocksDB orders keys by raw byte comparison, and a state store that needs
`rangeScan` — TTL expiry, timer expiry, timestamp-ordered join eviction — needs the *encoded* key
to sort the way the *number* sorts. Two's-complement integers and IEEE floats do not, so
`encodePrefixKeyForRangeScan` re-encodes each ordering column into a fixed-width big-endian buffer
of `defaultSize + 1` bytes:

- a **marker byte** first — `0x00` negative, `0x01` positive, `0x02` null — which is why negatives
  sort before positives and nulls sort last, deliberately;
- the value **big-endian** after it, so the most significant byte is compared first;
- for `Float`/`Double`, negatives have **every bit flipped** (`rawBits ^ flipBitMask`) before being
  written, because the magnitude of a negative float increases as its bits increase. The sign test
  is done on the raw bits rather than by comparing to zero, so `-0.0` and `NaN` land consistently.

The buffer is allocated at full width **even when the value is null**, because a variable-width
encoding would break the comparison for every subsequent column.

`RangeKeyScanStateEncoder` is the wrapper: it splits the key into ordering columns and the rest,
stores `[encoded ordering prefix][remaining key]`, and keeps a `restoreKeyProjection` that maps the
reordered joined row back to the caller's original column order. Only fixed-size types may be
ordering columns — `variableSizeOrderingColsNotSupported` and `nullTypeOrderingColsNotSupported`
are the two errors you get otherwise.

`RangeScanBoundaryUtils` is the other half, and its docstring is worth reading in full: callers of
`rangeScan` must pass fully-typed boundary rows, and the *non*-ordering columns of those rows must
encode byte-wise no larger than any real entry or `seek()` **silently skips matching entries**. It
builds recursive byte-wise-minimum defaults, with one explicit exception — `CharType(n)`, whose
`Literal.default` is space-padded (`0x20`) and therefore *not* minimal, so it is overridden with
`n` zero bytes — and one rejection, `VariantType`, whose binary layout has no guaranteed minimum.

**Anchor files:**

- [state/RocksDBStateEncoder.scala:512](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/state/RocksDBStateEncoder.scala#L512) — `encodePrefixKeyForRangeScan`, the whole per-type encoding
- [state/RocksDBStateEncoder.scala:398](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/state/RocksDBStateEncoder.scala#L398) — the flip masks and the three marker bytes, with the comment stating the intended sort order
- [state/RocksDBStateEncoder.scala:1540](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/state/RocksDBStateEncoder.scala#L1540) — `RangeKeyScanStateEncoder`; the fixed-size check at :1561 and `restoreKeyProjection` at :1598
- [state/RangeScanBoundaryUtils.scala:25](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/state/RangeScanBoundaryUtils.scala#L25) — the docstring on silent `seek()` skipping; `recursiveDefaultValue` at :93 and the `VariantType` assertion at :111

**Maps to topics:** E27, A8

### Timestamp key encoders — the physical layout behind timestamp-ordered eviction

**What it is:** the V4 stream-stream-join state format needs "evict everything older than T"
without a scan, so it stores the event time *inside the key* and relies on byte ordering.
`TimestampKeyStateEncoder` is the shared base: it appends a non-nullable `__event_time` `LongType`
column to the key schema, and encodes that long as 8 big-endian bytes **with the sign bit flipped**
(`timestamp ^ 0x8000000000000000L`) so negative timestamps still sort before positive ones under
byte comparison — the same problem the range-scan encoder solves, solved once more locally.

Two subclasses differ only in where the 8 bytes go: `TimestampAsPrefixKeyStateEncoder` puts them
first, so the whole column family is ordered by time and eviction is a single bounded scan;
`TimestampAsPostfixKeyStateEncoder` puts them last, so the store is ordered by key and the
timestamp only orders within one key. Neither supports a prefix-key scan — asking for one throws.

Worth noting for anyone reading the encoder: the reused `ByteBuffer` makes these encoders
**not thread-safe**, and the source says so explicitly; built-in operators only ever touch one from
one thread.

**Anchor files:**

- [state/RocksDBStateEncoder.scala:1751](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/state/RocksDBStateEncoder.scala#L1751) — `TimestampKeyStateEncoder`, `keySchemaWithTimestamp`, and the attach/detach projections
- [state/RocksDBStateEncoder.scala:1824](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/state/RocksDBStateEncoder.scala#L1824) — `encodeTimestamp` and the sign-bit flip; the thread-safety note at :1815
- [state/RocksDBStateEncoder.scala:1860](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/state/RocksDBStateEncoder.scala#L1860) — `TimestampAsPrefixKeyStateEncoder`; `TimestampAsPostfixKeyStateEncoder` at :1907

**Maps to topics:** A8

### Avro state encoding and state schema evolution — schema IDs, a broadcast, and two ceilings

**What it is:** `spark.sql.streaming.stateStore.encodingFormat` selects between two
`RocksDBDataEncoder` implementations, and the choice decides whether a stateful query's schema can
ever change.

- **`unsaferow`** (the default) is `UnsafeRowDataEncoder`, whose `supportsSchemaEvolution` is
  literally `false`: the bytes in RocksDB *are* the UnsafeRow layout, so any schema change makes
  every stored row undecodable.
- **`avro`** is `AvroStateEncoder`. Every stored row is prefixed with a **two-byte schema id**
  (`|schemaId|avro bytes|` for values and no-prefix keys; `|prefix|schemaId|avro bytes|` for prefix
  and range-scan keys), and the id names which historical schema that row was written under.

The lookup side is `StateSchemaProvider`. At planning time
`StateSchemaMetadata.createStateSchemaMetadata` reads *every* schema file the checkpoint holds,
converts each column family's key and value schemas to Avro, and keys them by
`(colFamilyName, schemaId, isKey)`; `StateSchemaBroadcast` ships that map to executors as a Spark
broadcast. An executor decoding a row therefore has every schema the query has ever used, and
encodes new rows with `getCurrentStateSchemaId`, the maximum.

The write side is `StateSchemaCompatibilityChecker.check`, and it is where all the conditions live:

- The **key** schema may never evolve — only `schemasCompatible` (nullability widening) is accepted.
- The **value** schema evolves only when `schemaEvolutionEnabled`, which is
  `usingAvro && schemaEvolutionEnabledForOperator` — and `schemaEvolutionEnabledForOperator` is
  `false` in the shared `SchemaValidationUtils` trait and overridden to `true` in exactly one
  place, the `transformWithState` family. **No other stateful operator can evolve its state
  schema, whatever the encoding format.**
- The candidate is checked with Avro's own `SchemaValidatorBuilder().canReadStrategy.validateAll()`
  against *every* prior schema, not just the newest — so an evolution that is readable from the
  last version but not from an older one still in the store is rejected.
- On success the value schema id is incremented and a new schema file written.

Two ceilings then bound the whole mechanism, both internal and both fatal when hit:
`valueStateSchemaEvolutionThreshold` (**16**) caps evolutions per column family, and
`maxNumStateSchemaFiles` (**128**) caps schema files per operator — the second only trips when a
column family is *added or removed*, which is what a `transformWithState` processor gaining a state
variable does.

One quiet side effect worth knowing before switching format: `getColFamilySchemas(shouldBeNullable)`
is called with `usingAvro`, so **turning on Avro encoding forces every state field nullable**.

**Code path:** `StateStoreWriter.validateAndWriteStateSchema` →
`StateSchemaCompatibilityChecker.validateAndMaybeEvolveStateSchema` → `check` (Avro
`canReadStrategy` + the two thresholds) → new schema file → `StateSchemaBroadcast` →
`AvroStateEncoder.encodeWithStateSchemaId` / `decode`

**Anchor files:**

- [state/RocksDBStateEncoder.scala:732](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/state/RocksDBStateEncoder.scala#L732) — `AvroStateEncoder`, with the byte layouts in its scaladoc from :707
- [state/RocksDBStateEncoder.scala:412](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/state/RocksDBStateEncoder.scala#L412) — `encodeWithStateSchemaId`, the two-byte prefix
- [state/RocksDBStateEncoder.scala:497](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/state/RocksDBStateEncoder.scala#L497) — `UnsafeRowDataEncoder`, and `supportsSchemaEvolution = false` at :502
- [state/RocksDBStateEncoder.scala:63](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/state/RocksDBStateEncoder.scala#L63) — `StateSchemaProvider`; `StateSchemaBroadcast` at :138, `StateSchemaMetadata.createStateSchemaMetadata` at :176
- [state/StateSchemaCompatibilityChecker.scala:188](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/state/StateSchemaCompatibilityChecker.scala#L188) — `check`: the Avro validator at :223, the evolution threshold at :235, the schema-file threshold at :297
- [operators/stateful/statefulOperators.scala:1553](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/operators/stateful/statefulOperators.scala#L1553) — `SchemaValidationUtils`, `schemaEvolutionEnabledForOperator = false` at :1556 and the `usingAvro &&` gate at :1605
- [operators/stateful/transformwithstate/TransformWithStateVariableUtils.scala:185](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/operators/stateful/transformwithstate/TransformWithStateVariableUtils.scala#L185) — the single `= true` override

**Configs:** `spark.sql.streaming.stateStore.encodingFormat` (`unsaferow`; `avro` the alternative,
4.0.0), `spark.sql.streaming.transformWithState.stateSchemaVersion` (`3`, 4.0.0),
`spark.sql.streaming.stateStore.valueStateSchemaEvolutionThreshold` (`16`, internal),
`spark.sql.streaming.stateStore.maxNumStateSchemaFiles` (`128`, internal)

**Maps to topics:** none — proposed as **E47**

### State checkpoint IDs — the V2 lineage that makes a state store verifiable

**What it is:** with checkpoint ids enabled, each committed state store version carries a unique
id, and each changelog file records its **lineage** — the chain of (version, uniqueId) pairs back
to a snapshot. The driver keeps the expected base id per operator and partition, and
`updateStateStoreCkptIdForOperator` compares what each task reports against it, raising
`stateStoreBaseCheckpointIdMismatch` when they disagree.

The problem it solves is real and previously silent: with two attempts of the same task (speculation,
or a retry after a partial commit) the state directory could end up holding a version written by an
attempt whose output was discarded. The lineage makes that detectable instead of producing quietly
wrong results.

**Anchor files:**

- [runtime/MicroBatchExecution.scala:1306](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/MicroBatchExecution.scala#L1306) — `updateStateStoreCkptIdForOperator` and the mismatch check
- [state/RocksDB.scala:385](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/state/RocksDB.scala#L385) — `getLineageFromChangelogFile`; `getFullLineage` at :409
- [operators/stateful/statefulOperators.scala:159](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/operators/stateful/statefulOperators.scala#L159) — `StatefulOpStateStoreCheckpointInfo`
- [state/StateStore.scala:506](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/state/StateStore.scala#L506) — `StateStoreCheckpointInfo`

**Configs:** `spark.sql.streaming.stateStore.checkpointFormatVersion`,
`spark.sql.streaming.stateStore.commitValidation.enabled` (`true`, 4.1.0)

**Maps to topics:** A8, A14

### Row checksums and auto snapshot repair — two 4.1.0 corruption defences, one on by default

**What it is:** two independent mechanisms added in 4.1.0, with deliberately different defaults.

- **Row checksums** (`stateStore.rowChecksum.enabled`, **`false`**) wrap each stored value with a
  checksum and verify it on read, at a sampling ratio set by `…readVerificationRatio` (0 in
  production, 1 under test). Off by default because it costs on every read.
- **Auto snapshot repair** (`stateStore.autoSnapshotRepair.enabled`, **on outside tests**) handles a
  corrupt or missing snapshot by walking backwards to an older eligible snapshot and replaying more
  changelog files on top of it — bounded by `…maxChangeFileReplay` (500) and activated only after
  `…numFailuresBeforeActivating` (1) failures. It is why a single bad snapshot file no longer ends
  the query, and why a restart can take much longer than usual without any error being logged as
  fatal.

**Anchor files:**

- [state/StateStoreRowChecksum.scala:47](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/state/StateStoreRowChecksum.scala#L47) — `KeyValueIntegrityVerifier` and its sampling `create`
- [state/AutoSnapshotLoader.scala:45](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/state/AutoSnapshotLoader.scala#L45) — the repair contract, including `getAdditionalEligibleSnapshots`

**Configs:** `…rowChecksum.enabled` (`false`), `…rowChecksum.readVerificationRatio`,
`…autoSnapshotRepair.enabled`, `…autoSnapshotRepair.maxChangeFileReplay` (`500`),
`…autoSnapshotRepair.numFailuresBeforeActivating` (`1`) — all 4.1.0

**Maps to topics:** A8, E3

### State schema evolution — StateSchemaCompatibilityChecker and the operator metadata log

**What it is:** state is stored as encoded rows, so changing the key or value schema of a stateful
operator between restarts is a compatibility question. `StateSchemaCompatibilityChecker` compares
the operator's current schema against the one recorded in the checkpoint's schema file and either
accepts it, accepts it as an *evolution* (nullability widening, added fields — governed by
`allowAdding`/`allowRemoving` rules), or fails.

`OperatorStateMetadata` (V1 and V2) is the companion record: per operator, its name, state store
names, partition count and — in V2 — the per-column-family schemas that `transformWithState` needs.
It is what the `state-metadata` DSv2 source reads, and what lets Spark tell you *which* operator's
schema changed rather than just failing to decode.

**Anchor files:**

- [state/StateSchemaCompatibilityChecker.scala:80](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/state/StateSchemaCompatibilityChecker.scala#L80) — the checker; `check` at :188
- [state/OperatorStateMetadata.scala:88](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/state/OperatorStateMetadata.scala#L88) — V1 and V2
- [state/SchemaHelper.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/state/SchemaHelper.scala) — the schema file reader/writer and its versions

**Configs:** `spark.sql.streaming.stateStore.stateSchemaCheckEnabled`,
`spark.sql.streaming.queryEvolution.enableSourceEvolution` / `…enableSinkEvolution`
(both `false`, 4.1.0)

**Maps to topics:** A8, B5

### Offline state repartition — changing a stateful query's partition count

**What it is:** new in 4.2.0, and the answer to a problem that previously had none. Because
`IncrementalExecution` pins `numShufflePartitions` to the value recorded in the checkpoint, a
stateful query's parallelism is fixed at its first batch. `OfflineStateRepartitionRunner` changes
it *offline*: with the query stopped, it reads the existing state through the state data source,
repartitions it on `partition_key` to the new count, writes it back with
`StatePartitionAllColumnFamiliesWriter`, and records the result as batch **N+1** so the query picks
up the new partitioning on its next start.

It is a mutation of a checkpoint, so it has a failure story: an interrupted run leaves an unfinished
repartition batch, and `spark.sql.streaming.checkUnfinishedRepartitionOnRestart` (default `true`,
4.2.0) makes the query detect that on startup rather than resuming onto half-rewritten state.
`OfflineStateRepartitionUtils.isRepartitionBatch` is how any component recognises such a batch.

**Anchor files:**

- [state/OfflineStateRepartitionRunner.scala:50](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/state/OfflineStateRepartitionRunner.scala#L50) — the runner and its scaladoc describing the batch N+1 model; `run` at :72
- [state/OfflineStateRepartitionUtils.scala:27](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/state/OfflineStateRepartitionUtils.scala#L27) — `isRepartitionBatch`, `getShufflePartitions`
- [state/StatePartitionWriter.scala:47](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/state/StatePartitionWriter.scala#L47) — the writer that rebuilds every column family
- [state/StatePartitionKeyExtractor.scala:31](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/state/StatePartitionKeyExtractor.scala#L31) and [operators/stateful/StatePartitionKeyExtractorFactory.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/operators/stateful/StatePartitionKeyExtractorFactory.scala) — how the partition key is recovered per operator type
- [state/StateRewriter.scala:64](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/state/StateRewriter.scala#L64) / [state/OfflineStateRepartitionErrors.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/state/OfflineStateRepartitionErrors.scala)

**Configs:** `spark.sql.streaming.checkUnfinishedRepartitionOnRestart` (`true`, 4.2.0)

**Maps to topics:** E28

### The maintenance thread — snapshotting, cleanup, and unloading providers

**What it is:** every executor runs one maintenance thread pool shared by all loaded providers. It
does the work that must not happen on the task path: writing snapshots, deleting old versions, and
unloading providers whose partition has moved elsewhere. `MaintenanceTaskType` distinguishes three
entry points — from the unloaded-providers queue, from a task thread, and from the loaded-providers
sweep.

Two things make it operationally visible. Maintenance has its own timeouts
(`maintenanceProcessingTimeout` 30 s, `maintenanceShutdownTimeout` 300 s), and a deletion budget
(`maxVersionsToDeletePerMaintenance`, `-1` = unbounded) — so a checkpoint with a very large
version backlog can take many maintenance cycles to clean up. And `unloadOnCommit` (4.1.0, off)
changes the lifecycle entirely by unloading after each commit, trading reload cost for memory.

**Anchor files:**

- [state/StateStore.scala:81](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/state/StateStore.scala#L81) — `MaintenanceTaskType`; the `loadedProviders` registry and the maintenance loop follow in the same object
- [state/HDFSBackedStateStoreProvider.scala:480](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/state/HDFSBackedStateStoreProvider.scala#L480) / [state/RocksDBStateStoreProvider.scala:1039](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/state/RocksDBStateStoreProvider.scala#L1039) — the two `doMaintenance` implementations

**Configs:** `spark.sql.streaming.stateStore.maintenanceInterval`,
`…maintenanceProcessingTimeout` (`30`), `…maintenanceShutdownTimeout` (`300`),
`…maxVersionsToDeletePerMaintenance` (`-1`) — all 4.1.0

**Maps to topics:** A8, E3

---

## Stateful operators

### statefulOperators — the base traits, the watermark predicates, and the metrics

**What it is:** `StatefulOperator` (has a `StatefulOperatorStateInfo`), `StateStoreReader` and
`StateStoreWriter` are the shared base. `StateStoreWriter` is where the metrics every stateful
query reports come from — `numTotalStateRows`, `numUpdatedStateRows`, `numRowsDroppedByWatermark`,
`stateMemory`, commit/eviction timings, plus the provider's custom metrics — and where
`setStateStoreCheckpointInfo` reports back to the driver.

`WatermarkSupport` turns the two propagated watermarks into predicates: one on the *key* (evict a
whole window) and one on the *value* (evict a row inside a window). Which one an operator uses is
what determines whether its state eviction is cheap.

`numRowsDroppedByWatermark` is the metric to check first when a streaming aggregate produces less
than expected — it counts rows discarded as late, which is otherwise silent.

**Anchor files:**

- [operators/stateful/statefulOperators.scala:107](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/operators/stateful/statefulOperators.scala#L107) — `StatefulOperator`; `StateStoreReader` at :150, `StateStoreWriter` at :169
- [operators/stateful/statefulOperators.scala:212](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/operators/stateful/statefulOperators.scala#L212) — the metrics, including `numRowsDroppedByWatermark`
- [operators/stateful/statefulOperators.scala:72](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/operators/stateful/statefulOperators.scala#L72) — `StatefulOperatorStateInfo` and the five-step driver/executor contract in its scaladoc

**Maps to topics:** A8

### Streaming aggregation and session windows — two state managers, two formats

**What it is:** a streaming aggregate is a `StateStoreRestoreExec` / `StateStoreSaveExec` pair
around the ordinary aggregate operators, mediated by a `StreamingAggregationStateManager` with two
format versions: V1 stores the whole row as the value, V2 stores only the non-key columns —
smaller state, and the reason the format version is pinned in the checkpoint.

Session windows get their own pair (`SessionWindowStateStoreRestoreExec` / `SaveExec`) and their
own manager, because a session's key range changes as sessions merge;
`MergingSortWithSessionWindowStateIterator` is the merge of new rows with stored sessions in sorted
order.

**Anchor files:**

- [operators/stateful/StreamingAggregationStateManager.scala:66](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/operators/stateful/StreamingAggregationStateManager.scala#L66) — the V1/V2 factory
- [operators/stateful/StreamingSessionWindowStateManager.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/operators/stateful/StreamingSessionWindowStateManager.scala) / [operators/stateful/MergingSortWithSessionWindowStateIterator.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/operators/stateful/MergingSortWithSessionWindowStateIterator.scala)

**Configs:** `spark.sql.streaming.aggregation.stateFormatVersion`,
`spark.sql.streaming.sessionWindow.stateFormatVersion`

**Maps to topics:** A8, B6

### Streaming deduplication and limits — the cheapest stateful operators, and their traps

**What it is:** `StreamingDeduplicateExec` stores each seen key with an empty value and drops
repeats; `StreamingDeduplicateWithinWatermarkExec` is the bounded variant that can actually evict.
The trap is in the first one: without a watermark on the dedup column, state is retained forever.

`StreamingGlobalLimitExec` keeps a running count in a one-row state store and requires `AllTuples`
— every row through one task — while `StreamingLocalLimitExec` is what `ConvertLocalLimitRule`
substitutes above a stateful operator so the limit is stable across batches.

**Anchor files:**

- [operators/stateful/streamingLimits.scala:40](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/operators/stateful/streamingLimits.scala#L40) — `StreamingGlobalLimitExec` and its `AllTuples` requirement at :105; `StreamingLocalLimitExec` at :124
- [operators/stateful/statefulOperators.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/operators/stateful/statefulOperators.scala) — the two deduplicate operators live here alongside the base traits

**Maps to topics:** A8

### flatMapGroupsWithState — the legacy arbitrary-state operator and GroupState

**What it is:** the pre-4.0 arbitrary-state API, now explicitly legacy but still what most existing
jobs use. `FlatMapGroupsWithStateExecBase` requires clustering *and* ordering on the grouping
attributes, keeps one state row per group through a `StateManager` (again with format versions),
and drives timeouts through `GroupStateTimeout` — processing-time or event-time — with
`processTimedOutState` emitting a second iterator after the data iterator is exhausted.

`GroupStateImpl` is the object handed to user code, and its value is that it *enforces* the
contract: calling `setTimeoutDuration` without having configured a timeout, or reading
`getCurrentWatermarkMs` under a processing-time timeout, raises rather than misbehaving.

**Anchor files:**

- [operators/stateful/flatmapgroupswithstate/FlatMapGroupsWithStateExec.scala:44](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/operators/stateful/flatmapgroupswithstate/FlatMapGroupsWithStateExec.scala#L44) — the base trait; the timed-out state iterator at :171
- [operators/stateful/flatmapgroupswithstate/FlatMapGroupsWithStateExecHelper.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/operators/stateful/flatmapgroupswithstate/FlatMapGroupsWithStateExecHelper.scala) — the state managers and their formats
- [operators/stateful/flatmapgroupswithstate/GroupStateImpl.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/operators/stateful/flatmapgroupswithstate/GroupStateImpl.scala) — the user-facing handle and its validation

**Configs:** `spark.sql.streaming.flatMapGroupsWithState.stateFormatVersion`

**Maps to topics:** A8

### Stream-stream join — four state stores per side, and the eviction predicates

**What it is:** the most state-hungry operator. Each side keeps its rows so a later row on the
other side can match them, and `SymmetricHashJoinStateManager` implements that with **two logical
stores per side** — `keyToNumValues` and `keyWithIndexToValue` — so an arbitrary number of rows per
key can be appended and indexed without a list encoding. That is four stores for a two-sided join.

State is bounded by `JoinStateWatermarkPredicates`: a predicate on the key (for a join condition on
the event-time column) and one on the value (for a range condition), evaluated against the propagated
watermark. A join with neither — no watermark, no time-range condition — retains state forever, which
is the standard stream-stream-join incident.

The V4 state format (`spark.sql.streaming.join.stateFormatV4.enabled`, testing-only default in
4.2.0) adds timestamp-ordered eviction — `evictByTimestamp` / `evictAndReturnByTimestamp` — so
eviction no longer scans.

**Anchor files:**

- [operators/stateful/join/StreamingSymmetricHashJoinExec.scala:136](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/operators/stateful/join/StreamingSymmetricHashJoinExec.scala#L136) — the operator; `shouldRunAnotherBatch` at :288
- [operators/stateful/join/SymmetricHashJoinStateManager.scala:53](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/operators/stateful/join/SymmetricHashJoinStateManager.scala#L53) — the manager interface; the eviction family at :157–:207; `SymmetricHashJoinStateManagerV4` at :227
- [operators/stateful/join/StreamingSymmetricHashJoinHelper.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/operators/stateful/join/StreamingSymmetricHashJoinHelper.scala) — `JoinStateWatermarkPredicates` and the state-store-aware zip partitioner
- [operators/stateful/join/StreamingSymmetricHashJoinValueRowConverter.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/operators/stateful/join/StreamingSymmetricHashJoinValueRowConverter.scala) — the matched-flag encoding used by outer joins

**Configs:** `spark.sql.streaming.join.stateFormatVersion`,
`spark.sql.streaming.join.stateFormatV4.enabled` (testing-only default, 4.2.0)

**Maps to topics:** A8, B7

### transformWithState — a handle, typed state variables, timers and TTL

**What it is:** the 4.0+ replacement for `flatMapGroupsWithState`, and structurally different: the
user gets a `StatefulProcessorHandle` and declares named state variables, each of which becomes a
**column family** in one state store rather than a single serialized state row.

- **State variables:** `ValueStateImpl`, `ListStateImpl` (using the store's `putList`/`merge` so an
  append is not a read-modify-write), `MapStateImpl` (a composite key encoding, so a point lookup
  in a large map does not deserialize the map).
- **TTL:** each `*ImplWithTTL` variant maintains a secondary **TTL index** column family keyed by
  expiration timestamp, so expiring state is a range scan rather than a full scan.
  `clearExpiredStateForAllKeys` runs per batch.
- **Timers:** `TimerStateImpl` keeps two column families — key→timestamp and a timestamp-prefixed
  secondary index — so "which timers expired" is also a range scan.
- **Lifecycle enforcement:** `StatefulProcessorHandleImpl` is a state machine
  (`PRE_INIT` / `CREATED` / `INITIALIZED` / `DATA_PROCESSED` / `TIMER_PROCESSED` / `CLOSED`) and
  every operation verifies the current state — which is why declaring a state variable outside
  `init()` fails with a clear error instead of corrupting state.
- **Driver-side schema declaration:** `TransformWithStateExec` instantiates a
  `DriverStatefulProcessorHandleImpl` at planning time and calls the user's `init` purely to learn
  the column-family schemas, then closes it. That is the second place (after the PySpark analyze
  runner) where user processor code runs on the driver.

**Anchor files:**

- [operators/stateful/transformwithstate/TransformWithStateExec.scala:58](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/operators/stateful/transformwithstate/TransformWithStateExec.scala#L58) — the operator; `getDriverProcessorHandle` at :109
- [operators/stateful/transformwithstate/TransformWithStateExecBase.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/operators/stateful/transformwithstate/TransformWithStateExecBase.scala) — the shared base with the PySpark operator
- [operators/stateful/transformwithstate/statefulprocessor/StatefulProcessorHandleImpl.scala:110](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/operators/stateful/transformwithstate/statefulprocessor/StatefulProcessorHandleImpl.scala#L110) — the handle; the state enum at :79, `ImplicitGroupingKeyTracker` at :48
- [operators/stateful/transformwithstate/statevariables/ValueStateImpl.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/operators/stateful/transformwithstate/statevariables/ValueStateImpl.scala), [ListStateImpl.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/operators/stateful/transformwithstate/statevariables/ListStateImpl.scala), [MapStateImpl.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/operators/stateful/transformwithstate/statevariables/MapStateImpl.scala), [ListStateMetricsImpl.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/operators/stateful/transformwithstate/statevariables/ListStateMetricsImpl.scala)
- [operators/stateful/transformwithstate/ttl/TTLState.scala:72](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/operators/stateful/transformwithstate/ttl/TTLState.scala#L72) — the TTL index contract, and its three variants: [ValueStateImplWithTTL.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/operators/stateful/transformwithstate/ttl/ValueStateImplWithTTL.scala), [ListStateImplWithTTL.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/operators/stateful/transformwithstate/ttl/ListStateImplWithTTL.scala), [MapStateImplWithTTL.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/operators/stateful/transformwithstate/ttl/MapStateImplWithTTL.scala)
- [operators/stateful/transformwithstate/statefulprocessor/StatefulProcessorHandleImplBase.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/operators/stateful/transformwithstate/statefulprocessor/StatefulProcessorHandleImplBase.scala) — the shared base carrying the state-machine verification, so the driver-side and executor-side handles enforce the same rules
- [operators/stateful/transformwithstate/timers/TimerStateImpl.scala:82](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/operators/stateful/transformwithstate/timers/TimerStateImpl.scala#L82) — the two timer column families; [ExpiredTimerInfoImpl.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/operators/stateful/transformwithstate/timers/ExpiredTimerInfoImpl.scala) / [TimerValuesImpl.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/operators/stateful/transformwithstate/timers/TimerValuesImpl.scala)
- [operators/stateful/transformwithstate/StateStoreColumnFamilySchemaUtils.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/operators/stateful/transformwithstate/StateStoreColumnFamilySchemaUtils.scala), [StateTypesEncoderUtils.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/operators/stateful/transformwithstate/StateTypesEncoderUtils.scala), [TransformWithStateVariableUtils.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/operators/stateful/transformwithstate/TransformWithStateVariableUtils.scala)
- [operators/stateful/transformwithstate/testing/InMemoryStatefulProcessorHandle.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/operators/stateful/transformwithstate/testing/InMemoryStatefulProcessorHandle.scala) — the supported way to unit-test a processor without a checkpoint

**Configs:** `spark.sql.streaming.stateStore.encodingFormat` (avro vs unsaferow),
`spark.sql.streaming.transformWithState.stateSchemaVersion`

**Maps to topics:** A8

### TTL indexes — one-to-one, one-to-many, and the work-queue trick

**What it is:** the `TTLState` trait is the shared machinery behind all three `*ImplWithTTL` state
variables, and it is a secondary-index design rather than a scan. Every TTL-enabled state variable
creates an internal column family `$ttl_<stateName>` keyed `(expirationMs, elementKey)` with an
empty value and a `RangeKeyScanStateEncoderSpec(schema, Seq(0))` — i.e. ordered by expiry — so
"what has expired" is a bounded range scan rather than a walk of the primary index.

The eviction scan is bounded on **both** ends:
`rangeScan(prevBatchTimestampMs + 1, batchTimestampMs + 1)`. Starting at the previous batch's
timestamp rather than at zero is what keeps expiry cost proportional to one batch's worth of
expirations instead of to the whole index — and it is why a query restarted with a
`prevBatchTimestampMs` gap does more work on its first batch.

Two shapes, and the second is the interesting one:

- **`OneToOneTTLState`** (value and map state) — the primary index is
  `elementKey -> (value, expiration)`, so one secondary entry per element is enough.
- **`OneToManyTTLState`** (list state) — the values for a key are appended with RocksDB's
  `merge`, so Spark **cannot delete an individual element** from the merged value, and therefore
  cannot key a secondary index by element. The source says so plainly: a custom merge operator
  supporting tombstones would be the fix, but RocksDB does not accept merge operators written in
  Java or Scala. So the index instead maps `(minExpirationMs, groupingKey) -> EMPTY`, behaving as
  a **work queue of lists that need cleaning**, backed by two more internal column families — a
  `$min_` index (key → minimum expiry, so the work-queue entry can be found and rewritten) and a
  `$count_` index (key → element count, maintained by hand because reading the merged list to
  count it would defeat the point).

So a single TTL-enabled `ListState` occupies **four** column families in one state store. That is
the concrete reason list state with TTL is the most expensive `transformWithState` variable.

**Code path:** `StatefulProcessorHandleImpl.getListState(ttlConfig)` → `ListStateImplWithTTL`
(`OneToManyTTLState`) → `store.merge` on the primary index + `insertIntoTTLIndex` /
`updateEntryCount` → per batch `clearExpiredStateForAllKeys()` → `ttlEvictionIterator()`
(`store.rangeScan` on `$ttl_`)

**Anchor files:**

- [operators/stateful/transformwithstate/ttl/TTLState.scala:72](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/operators/stateful/transformwithstate/ttl/TTLState.scala#L72) — the trait; the `$ttl_` column family and its range-scan spec at :106–:137
- [operators/stateful/transformwithstate/ttl/TTLState.scala:181](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/operators/stateful/transformwithstate/ttl/TTLState.scala#L181) — `ttlEvictionIterator`, the bounded `[prev+1, now+1)` scan
- [operators/stateful/transformwithstate/ttl/TTLState.scala:251](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/operators/stateful/transformwithstate/ttl/TTLState.scala#L251) — `OneToOneTTLState`
- [operators/stateful/transformwithstate/ttl/TTLState.scala:342](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/operators/stateful/transformwithstate/ttl/TTLState.scala#L342) — `OneToManyTTLState`, with the merge-operator explanation in its scaladoc; the `$min_` and `$count_` families at :387–:419
- [operators/stateful/transformwithstate/ttl/ValueStateImplWithTTL.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/operators/stateful/transformwithstate/ttl/ValueStateImplWithTTL.scala) / [MapStateImplWithTTL.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/operators/stateful/transformwithstate/ttl/MapStateImplWithTTL.scala) — the one-to-one pair
- [operators/stateful/transformwithstate/ttl/ListStateImplWithTTL.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/operators/stateful/transformwithstate/ttl/ListStateImplWithTTL.scala) — the one-to-many case
- [operators/stateful/transformwithstate/statefulprocessor/StatefulProcessorHandleImplBase.scala:27](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/operators/stateful/transformwithstate/statefulprocessor/StatefulProcessorHandleImplBase.scala#L27) — where the handle state machine is actually enforced: `verifyTimerOperations` rejects any timer call under `TimeMode.NoTime`, `verifyStateVarOperations` requires an exact handle state. Both the driver-side and executor-side handles inherit it, which is why a processor that misbehaves fails identically at planning time and at run time

**Configs:** `spark.sql.streaming.stateStore.rocksdb.mergeOperatorVersion` (`2` in 4.2.0 — version
1 joined merged list elements with a `,` delimiter, version 2 with none)

**Maps to topics:** A8

---

## Sources and sinks

### FileStreamSource — the seen-files map, the trigger limits, and cleanSource

**What it is:** the file source is a directory listing plus a memory of what it has already
returned. `SeenFilesMap` holds recently seen paths with their timestamps and purges anything older
than `maxFileAge` — which is the source's real semantics: **a file older than `maxFileAge` relative
to the newest seen file will never be picked up**, even if it appears later. `latestFirst` disables
the age bound entirely for that reason, and the source logs a warning that it may affect the
watermark.

Per trigger it takes at most `maxFilesPerTrigger` files or `maxBytesPerTrigger` bytes.
`cleanSource` (`archive` / `delete` / `off`) runs a `FileStreamSourceCleaner` after a batch
commits. `FileStreamSourceLog` is the compactible log recording what each batch contained, and
`MetadataLogFileIndex` is what lets a *batch* query read a streaming sink's output correctly.

**Anchor files:**

- [runtime/FileStreamSource.scala:48](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/FileStreamSource.scala#L48) — the source; `maxFileAgeMs` and the `latestFirst` interaction at :96–:126
- [runtime/FileStreamSource.scala:171](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/FileStreamSource.scala#L171) — `fetchMaxOffset`, the limit application and the purge
- [runtime/FileStreamSource.scala:546](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/FileStreamSource.scala#L546) — the `cleanSource` dispatch
- [runtime/FileStreamOptions.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/FileStreamOptions.scala) — every file-source option in one class
- [runtime/FileStreamSourceLog.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/FileStreamSourceLog.scala) / [runtime/FileStreamSourceOffset.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/FileStreamSourceOffset.scala) / [runtime/MetadataLogFileIndex.scala:38](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/MetadataLogFileIndex.scala#L38)

**Maps to topics:** A7, B4

### FileStreamSink and the _spark_metadata log — a sink whose output only Spark can read correctly

**What it is:** writing files exactly once means a reader must be able to tell committed files from
leftovers. `FileStreamSink` solves it with a `_spark_metadata` directory beside the data: a
compactible log listing the files each batch committed. `ManifestFileCommitProtocol` is the commit
protocol that collects task outputs and writes that manifest, deleting the pending files on abort.

The consequence is a real interoperability constraint, and `FileStreamSink.hasMetadata` exists to
enforce it: when Spark reads such a directory it uses the manifest and ignores stray files; **any
other engine reading the same path sees the strays**. It is the single most common surprise about
the file sink.

**Anchor files:**

- [sinks/FileStreamSink.scala:128](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/sinks/FileStreamSink.scala#L128) — the sink; `hasMetadata` at :47 and the metadata-dir constant at :41
- [sinks/FileStreamSinkLog.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/sinks/FileStreamSinkLog.scala) — the compactible manifest log
- [ManifestFileCommitProtocol.scala:41](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/ManifestFileCommitProtocol.scala#L41) — `commitJob` at :66, `abortJob` at :85

**Maps to topics:** A7, B4

### The built-in sources and sinks — rate, socket, memory, console, foreach

**What it is:** the `sources/` package holds everything you use to develop and test a streaming
query, plus the two generic sinks:

- **`RateStreamProvider`** (`rate`) and **`RatePerMicroBatchProvider`** (`rate-micro-batch`) — the
  difference matters: the first generates a rate per *second* and back-fills after a slow batch,
  the second a fixed count per *batch*, which is what you want for deterministic tests.
- **`TextSocketSourceProvider`** (`socket`) — documented as test-only, and unable to provide
  end-to-end guarantees because a socket cannot replay.
- **`MemoryStream`** / **`ContinuousMemoryStream`** / **`LowLatencyMemoryStream`** — the in-JVM
  sources every streaming test uses; the third is the Real-Time Mode variant.
- **`ForeachBatchSink`** — wraps each batch's `DataFrame` and calls your function; note it
  reconstructs a `Dataset` from the batch plan, which is why the DataFrame you receive is a real
  one you can write anywhere.
- **`ForeachWriterTable`** — the per-partition `open`/`process`/`close` writer, expressed as a DSv2
  table.
- **`MicroBatchWrite`** / **`WriteToMicroBatchDataSource`** — the adapters that let a DSv2 batch
  write serve as a streaming sink, epoch id and all.

**Anchor files:**

- [sources/RateStreamProvider.scala:47](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/sources/RateStreamProvider.scala#L47) / [sources/RatePerMicroBatchProvider.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/sources/RatePerMicroBatchProvider.scala) / [sources/RateStreamMicroBatchStream.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/sources/RateStreamMicroBatchStream.scala) / [sources/RatePerMicroBatchStream.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/sources/RatePerMicroBatchStream.scala)
- [sources/TextSocketSourceProvider.scala:39](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/sources/TextSocketSourceProvider.scala#L39) / [sources/TextSocketMicroBatchStream.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/sources/TextSocketMicroBatchStream.scala)
- [sources/memory.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/sources/memory.scala) / [runtime/memory.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/memory.scala) / [sources/ContinuousMemoryStream.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/sources/ContinuousMemoryStream.scala) / [sources/ContinuousMemory.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/sources/ContinuousMemory.scala) / [sources/LowLatencyMemoryStream.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/sources/LowLatencyMemoryStream.scala)
- [sources/ForeachBatchSink.scala:31](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/sources/ForeachBatchSink.scala#L31) — and `PythonForeachBatchHelper` at :85
- [sources/ForeachWriterTable.scala:46](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/sources/ForeachWriterTable.scala#L46) / [sources/PackedRowWriterFactory.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/sources/PackedRowWriterFactory.scala) / [sources/RealTimeRowWriterFactory.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/sources/RealTimeRowWriterFactory.scala)
- [sources/MicroBatchWrite.scala:29](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/sources/MicroBatchWrite.scala#L29) / [sources/WriteToMicroBatchDataSource.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/sources/WriteToMicroBatchDataSource.scala) / [sources/WriteToMicroBatchDataSourceV1.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/sources/WriteToMicroBatchDataSourceV1.scala) / [sources/ConsoleStreamingWrite.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/sources/ConsoleStreamingWrite.scala) / [console.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/console.scala)
- [Source.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/Source.scala) / [Sink.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/Sink.scala) — the V1 interfaces everything above implements or adapts; [legacy.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/legacy.scala), [StreamingErrors.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/StreamingErrors.scala), [StreamingQueryPlanTraverseHelper.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/StreamingQueryPlanTraverseHelper.scala), [utils/StreamingUtils.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/utils/StreamingUtils.scala)

**Maps to topics:** A7

### How a source actually implements MicroBatchStream — the rate sources as the readable example

**What it is:** the `MicroBatchStream` contract is four decisions, and the two rate sources are the
smallest complete implementations of it in the tree — which makes them the reference for reading
Kafka's or anyone else's.

1. **What is an offset, and where does the origin live?** `RateStreamMicroBatchStream`'s offset is
   *seconds since the query was created*, so the origin must survive a restart: it writes
   `creationTimeMs` into its own single-entry `HDFSMetadataLog` under the source's metadata path on
   first start and reads it back thereafter. That log — one per source, under
   `<checkpoint>/sources/<n>` — is the general mechanism for source-private state, and it is why a
   rate source restarted from a checkpoint keeps producing the same values for the same offsets.
2. **How is the batch bounded?** Two different answers, and the difference is the whole point of
   having both sources. `RateStreamMicroBatchStream.latestOffset()` reads the *clock*, so a slow
   batch is followed by a larger one — it back-fills. `RatePerMicroBatchStream` refuses to
   implement the no-argument `latestOffset()` at all (it throws) and implements
   `latestOffset(start, limit)` with `getDefaultReadLimit = ReadLimit.maxRows(rowsPerBatch)`, so
   every batch is exactly `rowsPerBatch` rows regardless of wall-clock time. That is what makes it
   the source to use for a deterministic test.
3. **How does it behave under `Trigger.AvailableNow`?** `RatePerMicroBatchStream` implements
   `SupportsTriggerAvailableNow` directly: `prepareForTriggerAvailableNow()` sets a flag, and
   `latestOffset` then freezes and returns one snapshot offset forever. A source that does *not*
   implement it gets the wrapper classes instead.
4. **How is work split?** `planInputPartitions(start, end)` returns
   `Array.empty` when the range is empty — the signal for "no data" — and otherwise one partition
   per `numPartitions`, each computing its own slice arithmetically. No shuffle, no listing.

The error paths are as instructive as the happy one: `RatePerMicroBatchStream` raises
`MALFORMED_STATE_IN_RATE_PER_MICRO_BATCH_SOURCE.INVALID_TIMESTAMP` for a case that can only arise
from a checkpoint written with a different `startingTimestamp` — an uncommitted batch 0 resumed
against a changed option. That is the shape of every source's restart-validation problem.

`TextSocketMicroBatchStream` is the counter-example the docs warn about: a socket cannot replay, so
it buffers what it has read in memory and its guarantees end at the process boundary.
`LowLatencyMemoryStream` is the Real-Time Mode variant of `MemoryStream` — it implements
`SupportsRealTimeMode`/`SupportsRealTimeRead` and hands records to executors through an RPC
endpoint (`LowLatencyMemoryStreamEndpoint`) polled per record, rather than shipping a batch's data
in the partition metadata, which is what "real time" requires at the source level.

**Anchor files:**

- [sources/RateStreamMicroBatchStream.scala:59](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/sources/RateStreamMicroBatchStream.scala#L59) — `creationTimeMs` and its private `HDFSMetadataLog`; `latestOffset` at :104, `planInputPartitions` at :117
- [sources/RatePerMicroBatchStream.scala:41](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/sources/RatePerMicroBatchStream.scala#L41) — the refused `latestOffset()`; `getDefaultReadLimit` at :45, `prepareForTriggerAvailableNow` at :60, the two malformed-state errors at :97–:117
- [sources/TextSocketMicroBatchStream.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/sources/TextSocketMicroBatchStream.scala) — the in-memory buffer that is the reason for the test-only warning
- [sources/LowLatencyMemoryStream.scala:67](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/sources/LowLatencyMemoryStream.scala#L67) — the `SupportsRealTimeMode` source and its per-record RPC endpoint, contrasted in its own scaladoc with `ContinuousMemoryStream`

**Maps to topics:** A7

### The DSv2 streaming write path — MicroBatchWrite, V2Writes, and the V1 marker that gets deleted

**What it is:** a streaming sink is not a special execution path; it is the ordinary DSv2 batch
write, executed once per micro-batch with the batch id threaded through it. The mechanism is one
small class.

`MicroBatchExecution` decides which of two shapes to build when it constructs the logical plan:

- a `SupportsWrite` table becomes **`WriteToMicroBatchDataSource`**;
- a legacy `Sink` becomes **`WriteToMicroBatchDataSourceV1`** — and, since 4.2.0, is rejected
  outright under a `RealTimeTrigger` with `STREAMING_REAL_TIME_MODE.SINK_NOT_SUPPORTED`, because a
  V1 sink cannot write row by row.

Per batch, `withNewBatchId(batchId)` stamps the id onto that node, and then the two paths diverge
completely:

- **V2.** The `V2Writes` optimizer rule matches `WriteToMicroBatchDataSource(..., Some(batchId))`,
  builds the write, wraps it as `new MicroBatchWrite(batchId, write.toStreaming)`, and replaces the
  node with a plain `WriteToDataSourceV2`. `MicroBatchWrite` is a `BatchWrite` that forwards
  everything to the `StreamingWrite` with the epoch id closed over — `commit(messages)` becomes
  `writeSupport.commit(epochId, messages)`, and the writer factory becomes a
  `MicroBatchWriterFactory` that calls `createWriter(partitionId, taskId, epochId)`. **That closed-over
  batch id is the entire idempotence contract**: the checkpoint protocol guarantees a crashed batch
  re-runs with the *same* id, so a sink that dedupes on it is exactly-once and one that ignores it
  is at-least-once. Execution is then `nextBatch.collect()` — collecting nothing, purely to force
  the write — and `WriteToDataSourceV2Exec.commitProgress` is what surfaces as
  `sinkCommitProgress`.
- **V1.** `IncrementalExecution.optimizedPlan` **deletes** the marker node (`case w:
  WriteToMicroBatchDataSourceV1 => w.child`) before optimization, so it has no physical node at
  all. `MicroBatchExecution` calls `sink.addBatch(batchId, nextBatch)` itself, outside the plan,
  and then has to refresh the catalog table by hand — the comment says why: the DSv2 write node has
  a relation-invalidation mechanism and DSv1 has none. A V1 sink also reports **no** commit
  progress, which is why `numOutputRows` is missing for it.

The write-side interfaces beneath both are worth reading together, because they are what a custom
sink implements: `ForeachWriterTable` is the smallest complete `SupportsWrite` in the tree — a
table with no schema, `STREAMING_WRITE` as its only capability, a `truncate()` that deliberately
does nothing, and a `ForeachDataWriter` whose `open`/`process`/`close` is exactly the
`ForeachWriter` contract, with `close(errorOrNull)` guaranteed by `DataWriter.close` and a
synthesised `foreachWriterAbortedDueToTaskFailureError` when the task died without a user error.
`PackedRowWriterFactory` (rows travel to the driver inside the commit message) and
`RealTimeRowWriterFactory` (rows are RPC'd to a driver endpoint per row) are the two test writers,
and both carry the same warning: sending rows to the driver is not production shape.

**Code path:** `MicroBatchExecution.logicalPlan` (`WriteToMicroBatchDataSource` or `…V1`) →
`withNewBatchId` → *V2:* `V2Writes` → `new MicroBatchWrite(batchId, streamingWrite)` →
`WriteToDataSourceV2Exec` → `MicroBatchWriterFactory.createWriter(partition, task, epochId)` →
`StreamingWrite.commit(epochId, messages)` — *V1:* `IncrementalExecution` deletes the node →
`Sink.addBatch(batchId, df)` → `catalog.refreshTable`

**Anchor files:**

- [sources/MicroBatchWrite.scala:29](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/sources/MicroBatchWrite.scala#L29) — the whole adapter, and `MicroBatchWriterFactory` at :51
- [sources/WriteToMicroBatchDataSource.scala:42](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/sources/WriteToMicroBatchDataSource.scala#L42) — the V2 node, with the transactional-catalog re-resolution explained in its scaladoc
- [sources/WriteToMicroBatchDataSourceV1.scala:35](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/sources/WriteToMicroBatchDataSourceV1.scala#L35) — the marker node, documented as pass-through with no physical plan
- [../../datasources/v2/V2Writes.scala:100](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/v2/V2Writes.scala#L100) — the rule that builds the `MicroBatchWrite`
- [runtime/IncrementalExecution.scala:131](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/IncrementalExecution.scala#L131) — the pre-optimization transform that deletes the V1 marker
- [runtime/MicroBatchExecution.scala:395](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/MicroBatchExecution.scala#L395) — the sink dispatch, including the Real-Time Mode rejection of V1 sinks at :422; the per-batch `withNewBatchId` at :1177 and the `addBatch` block at :1236
- [sources/ForeachWriterTable.scala:46](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/sources/ForeachWriterTable.scala#L46) — the minimal `SupportsWrite`; `ForeachDataWriter` at :135 and the abort/close contract at :162–:175
- [sources/PackedRowWriterFactory.scala:35](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/sources/PackedRowWriterFactory.scala#L35) / [sources/RealTimeRowWriterFactory.scala:35](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/sources/RealTimeRowWriterFactory.scala#L35)

**Configs:** `spark.sql.streaming.disabledV2Writers` — forces a named sink onto the V1 path; note
it is read in `classic/DataStreamWriter`, not here — and `spark.sql.streaming.commitProtocolClass`
(the file sink's commit protocol, read in `sinks/FileStreamSink`)

**Maps to topics:** none — proposed as **A45**

---

## Continuous processing and Real-Time Mode

### Continuous processing — the epoch protocol, and why it is a different engine

**What it is:** the pre-4.x low-latency execution path, and a genuinely different model.
`ContinuousExecution` launches **long-running tasks** that never finish, and durability is provided
by *epochs* rather than batches. The whole engine is a two-phase commit driven by one RPC endpoint.

**The driver side.** `runContinuous` plans the query once, sets two task-local properties
(`__continuous_start_epoch` and a per-reconfiguration `__epoch_coordinator_id` — a fresh UUID *on
top of* the run id, precisely so a coordinator from a previous reconfiguration cannot be reached),
creates the `EpochCoordinator` endpoint, and starts an **epoch update thread**. That thread is the
only place a `ProcessingTimeExecutor` is used here: on each tick it either notices
`stream.needsReconfiguration` and interrupts the query thread, or sends `IncrementAndGetEpoch`.
Meanwhile the main thread runs `lastExecution.executedPlan.execute()` — a job that is expected
never to return.

**The coordinator.** `EpochCoordinator` holds two maps keyed `(epoch, partition)`: offsets reported
by readers and commit messages reported by writers. `resolveCommitsAtEpoch` fires only when *both*
are complete for that epoch — `thisEpochCommits.size == numWriterPartitions && nextEpochOffsets.size == numReaderPartitions` — and even then it enforces **sequencing**: if the previous epoch has not
committed, this one is parked in `epochsWaitingToBeCommitted` and drained in order later. The
commit order inside `commitEpoch` is stated in a comment and is the durability rule:
`writeSupport.commit(epoch, messages)` **before** `query.commit(epoch)`, "or we will end up dropping
the commit if we restart in the middle".

**Recovery, and why it is at-least-once.** `getStartOffsets` reads the *commit* log's latest epoch,
takes that epoch's offsets from the offset log, and resumes at `latestEpochId + 1`. The comment
says exactly what is being given up: offsets that were reported but never committed are ignored,
"for at least once, we can just ignore those reports and risk duplicates". Exactly-once would
require replaying to those offsets.

**Two backstops.** `checkProcessingQueueBoundaries` kills the query when any of the three
coordinator structures exceeds `epochBacklogQueueSize` — the failure mode of one lagging partition
is a growing driver-side map, not silent drift. And `StopContinuousExecutionWrites` is a
*synchronous* message sent before the endpoint is stopped, because `RpcEndpoint.stop()` drains its
queue: without it, an old coordinator could commit epoch n+1 after a restarted query had already
begun epoch n.

**Structural limits.** One source only (`assert(sources.length == 1)`), no distribution or ordering
requirements on the write (`writeDistributionAndOrderingNotSupportedInContinuousExecution`), no
`CurrentTimestamp`/`CurrentDate`/`LocalTimestamp`, `WatermarkPropagator.noop()` — so no watermarks
— and a source must declare `TableCapability.CONTINUOUS_READ`. That list, not the latency number,
is why 4.2.0's Real-Time Mode is a new effort rather than an extension.

**Code path:** `runActivatedStream` → `runContinuous` → `EpochCoordinatorRef.create` +
epoch update thread (`IncrementAndGetEpoch`) → `ContinuousScanExec` (`SetReaderPartitions`) /
`WriteToContinuousDataSourceExec` (`SetWriterPartitions`) → per epoch
`ReportPartitionOffset` + `CommitPartitionEpoch` → `resolveCommitsAtEpoch` →
`writeSupport.commit` → `ContinuousExecution.commit` (offset log already written by `addOffset`,
then commit log)

**Anchor files:**

- [continuous/ContinuousExecution.scala:77](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/continuous/ContinuousExecution.scala#L77) — `logicalPlan`, the `CONTINUOUS_READ` capability check and the distribution/ordering rejection
- [continuous/ContinuousExecution.scala:188](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/continuous/ContinuousExecution.scala#L188) — `getStartOffsets`, with the at-least-once comment
- [continuous/ContinuousExecution.scala:219](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/continuous/ContinuousExecution.scala#L219) — `runContinuous`: the epoch update thread at :283, the `StopContinuousExecutionWrites` teardown at :342
- [continuous/ContinuousExecution.scala:361](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/continuous/ContinuousExecution.scala#L361) — `addOffset` (offset log) and `commit` at :390 (commit log, source commit, purge)
- [continuous/EpochCoordinator.scala:106](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/continuous/EpochCoordinator.scala#L106) — the coordinator's three-job contract in its scaladoc; `resolveCommitsAtEpoch` at :147, the commit-ordering comment at :202, `checkProcessingQueueBoundaries` at :233
- [continuous/EpochCoordinator.scala:39](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/continuous/EpochCoordinator.scala#L39) — the `StopContinuousExecutionWrites` message and the restart race it exists to prevent
- [../../datasources/v2/ContinuousScanExec.scala:59](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/v2/ContinuousScanExec.scala#L59) — the reader-side `SetReaderPartitions`, the one piece of this protocol that lives outside the group
- [continuous/WriteToContinuousDataSourceExec.scala:45](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/continuous/WriteToContinuousDataSourceExec.scala#L45) — `SetWriterPartitions`, and the `rdd.collect()` that starts the never-ending job
- [continuous/WriteToContinuousDataSource.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/continuous/WriteToContinuousDataSource.scala) / [continuous/ContinuousRateStreamSource.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/continuous/ContinuousRateStreamSource.scala) / [continuous/ContinuousTextSocketSource.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/continuous/ContinuousTextSocketSource.scala) — the write node and the only two continuous sources

**Configs:** `spark.sql.streaming.continuous.epochBacklogQueueSize` (the kill switch above)

**Maps to topics:** none — proposed as **E48**

### The continuous reader and writer tasks — two background threads, an epoch marker, and no retries

**What it is:** the executor half of the epoch protocol, and the part that explains continuous
processing's operational character.

**No retries, by construction.** `ContinuousDataSourceRDD.compute` opens with
`if (context.attemptNumber() != 0) throw new ContinuousTaskRetryException()`. A continuous task
cannot be retried, because its reader holds position state that a fresh attempt would not have. A
failure therefore fails the whole query rather than one task.

**A partition's reader outlives its `compute()`.** `compute` is called once per *epoch*, but
`ContinuousDataSourceRDDPartition.queueReader` is created on the first call and reused forever —
the scaladoc calls it "semantically a lazy val" — which is what gives offsets continuity across
epoch boundaries. Each `compute` returns an iterator that ends when the reader returns `null`.

**Three threads per reader partition.** `ContinuousQueuedDataReader` owns an
`ArrayBlockingQueue[ContinuousRecord]` of size `continuous.executorQueueSize` and two producers:

- a **`DataReaderThread`** looping on the source's blocking `next()` and pushing
  `ContinuousRow(row, offset)`. It converts to `UnsafeRow` before copying, because "`InternalRow#copy`
  may not be properly implemented". It never rethrows on the reader thread — a throw there could
  kill the executor — but stores `failureReason` for the consuming thread to raise.
- an **`EpochMarkerGenerator`**, a scheduled task polling `GetCurrentEpoch` every
  `continuous.executorPollIntervalMs` and pushing an `EpochMarker` into the same queue. If the
  driver has moved several epochs ahead while the poll was slow, it pushes **one marker per missed
  epoch** to catch up, deliberately producing empty epochs for that partition.

The consumer, `next()`, returns rows until it hits an `EpochMarker`, at which point it sends
`ReportPartitionOffset(partition, currentEpoch, currentOffset)` and returns `null` to end the
epoch. Shutdown is expressed the same way: `shouldStop()` (interrupted or completed) synthesises an
`EpochMarker` so the epoch closes cleanly instead of being torn down mid-flight.

**The writer side is a `while` loop that never exits.** `ContinuousWriteRDD.compute` initialises
`EpochTracker`, then repeatedly calls `prev.compute(split, context)` — each call yielding one
epoch's rows — writes them, commits the `DataWriter`, sends `CommitPartitionEpoch`, and increments
the epoch. An error aborts the writer through `tryWithSafeFinallyAndFailureCallbacks`; an
`InterruptedException` is swallowed, because interruption is how a continuous query stops.

`EpochTracker` itself is an `InheritableThreadLocal[AtomicLong]` whose `childValue` deliberately
**copies** rather than shares, so a child thread's epoch does not track the parent's — the comment
names `ContinuousCoalesceRDD` as the reason.

**Anchor files:**

- [continuous/ContinuousDataSourceRDD.scala:76](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/continuous/ContinuousDataSourceRDD.scala#L76) — the retry rejection and the reused queue reader; the "semantically a lazy val" partition field at :30
- [continuous/ContinuousQueuedDataReader.scala:43](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/continuous/ContinuousQueuedDataReader.scala#L43) — the queue, both background threads; `next()` at :92, `DataReaderThread` at :137, `EpochMarkerGenerator` at :185 with the catch-up loop at :201
- [continuous/ContinuousWriteRDD.scala:46](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/continuous/ContinuousWriteRDD.scala#L46) — the never-exiting write loop and the per-epoch commit
- [continuous/EpochTracker.scala:26](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/continuous/EpochTracker.scala#L26) — the thread-local and its copying `childValue`
- [continuous/ContinuousTaskRetryException.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/continuous/ContinuousTaskRetryException.scala) / [runtime/ContinuousRecordEndpoint.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/ContinuousRecordEndpoint.scala) — the retry marker, and the RPC endpoint the in-memory continuous sources poll

**Configs:** `spark.sql.streaming.continuous.executorQueueSize`,
`spark.sql.streaming.continuous.executorPollIntervalMs`

**Maps to topics:** E48

### Real-Time Mode — an allowlist as the feature gate

**What it is:** 4.2.0's millisecond-latency path does not restrict itself by capability negotiation;
it restricts itself by an **explicit allowlist of class names**. `RealTimeModeAllowlist` checks the
sink table class and every physical operator in a real-time subtree against that list and throws a
named error for anything absent. `spark.sql.streaming.realTimeMode.allowlistCheck` (default `true`)
is the switch, and `…minBatchDuration` (5000 ms) bounds the batch.

Reading the allowlist is the fastest and most reliable way to know what Real-Time Mode actually
supports in this release — more reliable than the release notes, which describe intent.

The micro-batch loop is aware of it in two places: offset logging is **deferred** under a
`RealTimeTrigger` (`markMicroBatchStart` skips the write), and the trigger's `endOffsets` are not
known when progress is recorded, so an empty `StreamProgress` is reported instead.

**Anchor files:**

- [runtime/RealTimeModeAllowlist.scala:27](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/RealTimeModeAllowlist.scala#L27) — the object; `checkAllowedSink` at :88 and `checkAllowedPhysicalOperator` at :123
- [runtime/MicroBatchExecution.scala:1290](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/MicroBatchExecution.scala#L1290) — the deferred offset logging
- [runtime/MicroBatchExecution.scala:645](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/MicroBatchExecution.scala#L645) — the empty end-offset progress under a real-time trigger

**Configs:** `spark.sql.streaming.realTimeMode.allowlistCheck` (`true`, 4.1.0),
`spark.sql.streaming.realTimeMode.minBatchDuration` (`5000`, 4.1.0)

**Maps to topics:** A7

---

## Breadth check 1 — the config slice

Slice, reproducibly — every catalog entry whose `subsystem` is `sql/catalyst`, `sql/core` or
`core` and whose key matches:

```
\.streaming|stateStore|watermark|checkpoint|\.rocksdb|Streaming
```

**113 keys** against the 4.2.0 catalog (2026-07-25). The 2026-08-06 pass recorded 114 and, more
importantly, mis-attributed several families; the table below is rebuilt by resolving each key's
`SQLConf` val to the files that actually read it.

**82 of the 113 are read somewhere under `execution/streaming/`.** By concept:

| Family | Keys | Tied to |
|---|---|---|
| `stateStore.*` — maintenance, coordinator, lag reporting, checksums, auto-repair, schema, formats | 30 | the StateStore API, the maintenance thread, the coordinator, checkpoint IDs, row checksums, schema evolution |
| batch loop and query lifecycle — `minBatchesToRetain`, `noDataMicroBatches.enabled`, `pollingDelay`, `stopTimeout`, `offsetLog.formatVersion`, `triggerAvailableNowWrapper.enabled`, `disabledV2MicroBatchReaders`, `noDataProgressEventInterval`, `asyncLogPurge.enabled`, `forceDeleteTempCheckpointLocation`, `maxBatchesToRetainInMemory`, `ratioExtraSpaceAllowedInCheckpoint` | 12 | StreamExecution, MicroBatchExecution, TriggerExecutor, AsyncLogPurge |
| checkpoint plumbing — `checkpointFileManagerClass`, `checkpoint.fileChecksum.*`, `checkpoint.renamedFileCheck.enabled`, `checkpoint.escapedPathCheck.enabled`, `checkpoint.verifyMetadataExists.enabled`, `verifyCheckpointDirectoryEmptyOnStart`, `metadataCache.enabled`, `commitProtocolClass`, `checkpointLocation` | 10 | CheckpointFileManager, HDFSMetadataLog, ManifestFileCommitProtocol |
| file source / file sink logs — `fileSource.log.*`, `fileSink.log.*`, `fileSource.cleaner.numThreads`, `fileStreamSink.ignoreMetadata` | 8 | CompactibleFileStreamLog, FileStreamSource, FileStreamSink |
| watermarks and stateful-operator planning — `multipleWatermarkPolicy`, `statefulOperator.useStrictDistribution`, `statefulOperator.allowMultiple`, `internal.stateStore.partitions` | 4 | WatermarkTracker, WatermarkPropagator, IncrementalExecution |
| per-operator state formats — `aggregation.stateFormatVersion`, `join.stateFormatVersion`, `join.stateFormatV4.enabled`, `flatMapGroupsWithState.stateFormatVersion`, `transformWithState.stateSchemaVersion` | 5 | the state managers, and `OffsetSeq` which pins them into the checkpoint |
| query evolution and repartition — `queryEvolution.enableSourceEvolution` / `…enableSinkEvolution`, `checkUnfinishedRepartitionOnRestart` | 3 | schema evolution, offline state repartition |
| progress reporting — `metricsEnabled`, `numRecentProgressUpdates` | 2 | ProgressReporter, MetricsReporter |
| continuous and Real-Time Mode — `continuous.epochBacklogQueueSize`, `realTimeMode.allowlistCheck`, `realTimeMode.minBatchDuration` | 3 | EpochCoordinator, RealTimeModeAllowlist |
| analysis rule hosted here — `unsupportedOperationCheck` | 1 | ResolveWriteToStream (also read by `QueryExecution`) |
| miscellaneous single readers — `stateStore.providerClass`, `stateStore.encodingFormat`, `stateStore.compression.codec`, `stateStore.skipNullsForStreamStreamJoins.enabled` (counted in the 30 above) | — | — |

**The remaining 31 are in the slice but owned elsewhere** — a finding, not an omission:

| Keys | Owner |
|---|---|
| `spark.checkpoint.compress`, `spark.checkpoint.dir`, `spark.rdd.checkpoint.cachePreferredLocsExpireTime` (3) | RDD checkpointing — `core — rdd-layer` |
| `spark.scheduler.streaming.idAwareLogging.*` (2) | `core — execution-engine` |
| `spark.streaming.dynamicAllocation.*` (7) | DStream — `streaming` |
| `spark.sql.streaming.ui.retainedQueries`, `…retainedProgressUpdates`, `…enabledCustomMetricList` (3) | `sql/streaming/ui/StreamingQueryStatusListener` — no group claims it |
| `spark.sql.streaming.ui.enabled`, `…streamingQueryListeners`, `…stopActiveRunOnRestart`, `…disabledV2Writers` (4) | `SharedState`, `StreamingQueryManager`, `DataStreamWriter` — `sql/core — classic-api` |
| `spark.sql.streaming.schemaInference`, `…fileSource.schema.forceNullable`, `…continuous.executorQueueSize`, `…continuous.executorPollIntervalMs` (4) | `execution/datasources/` — `DataSource`, `ContinuousScanExec`. The two continuous ones are *read* there and *consumed* by this group's `ContinuousQueuedDataReader` |
| `spark.sql.streaming.optimizeOneRowPlan.enabled`, `…statefulOperator.checkCorrectness.enabled`, `…validateEventTimeWatermarkColumn` (3) | `sql/catalyst` — `OptimizeOneRowPlan`, `UnsupportedOperationChecker`, `Analyzer` |
| `spark.sql.streaming.sessionWindow.stateFormatVersion`, `…sessionWindow.merge.sessions.in.local.partition`, `…flatMapGroupsWithState.skipEmittingInitialStateKeys` (3) | `execution/SparkStrategies.scala` — `sql/core — query-execution` |
| `spark.sql.adaptive.streaming.stateless.enabled` (1) | `sql/core — adaptive` |
| `spark.sql.optimizer.pruneFiltersCanPruneStreamingSubplan` (1) | `sql/catalyst — optimizer` |
| `spark.sql.streaming.kafka.useDeprecatedOffsetFetching` (1) | `connector/kafka-0-10-sql` |

!!! info "Where the state-format versions are actually read"

    Every `*.stateFormatVersion` and the two session-window / `flatMapGroupsWithState` behaviour
    flags are read in **`execution/SparkStrategies.scala`**, not in the operator or its state
    manager — the version is baked into the physical operator at planning time and then pinned
    into the checkpoint by `OffsetSeq`. That is why changing one on a running query does nothing,
    and why the checkpoint is what has to be inspected to find out which format a query uses.

### Configs this group reads that are **not** in the catalog

These are the ones no checker can see, because no `ConfigBuilder` declares them. The
2026-08-06 pass counted some of them as catalog keys, which is what made its table add up wrong.

- **~29 RocksDB tuning keys** under `spark.sql.streaming.stateStore.rocksdb.` — every field of
  `RocksDBConf` except `formatVersion` and `mergeOperatorVersion`. Names and defaults live only in
  `RocksDB.scala`; see the "RocksDB tuning surface" concept above. Lookup is by lower-cased string
  with a silent fallback to the default, so a misspelling is undetectable.
- **Async progress tracking is entirely writer options, not configs** —
  `asyncProgressTrackingEnabled`, `asyncProgressTrackingCheckpointIntervalMs` and the internal
  `_asyncProgressTrackingOverrideSinkSupportCheck` are read off the `DataStreamWriter`'s option
  map in `StreamingQueryManager` / `AsyncProgressTrackingMicroBatchExecution`. There is no
  `spark.sql.streaming.asyncProgressTracking*` config and there never was; the previous pass's
  table listed three.
- **Source and sink options** — the whole `FileStreamOptions` surface (`maxFilesPerTrigger`,
  `maxBytesPerTrigger`, `maxFileAge`, `latestFirst`, `cleanSource`, `sourceArchiveDir`,
  `fileNameOnly`), the rate options (`rowsPerSecond`, `rampUpTime`, `numPartitions`,
  `rowsPerBatch`, `startTimestamp`, `advanceMillisPerBatch`, `useManualClock`) and
  `StateStoreConf.FORMAT_VALIDATION_CHECK_VALUE_CONFIG` (`formatValidationCheckValue`).
- **`spark.sql.streaming.stateStore.forceSnapshotUploadOnLag` is validated inside `SQLConf`
  itself** — its accessor rejects `true` unless lag reporting is on — so the constraint is not
  visible from the key's declaration.

## Breadth check 2 — the packages

Walked by hand: the group has six levels of nesting under `operators/stateful/`, which
`check_drift.py --coverage` structurally cannot see.

| Package | Files | Cited | Traced |
|---|---|---|---|
| `execution/streaming/` (top level) | 7 | 7 | 7 |
| `execution/streaming/runtime/` | 37 | 37 | 37 |
| `execution/streaming/state/` | 29 | 29 | 29 |
| `execution/streaming/sources/` | 18 | 18 | 18 |
| `execution/streaming/continuous/` | 11 | 11 | 11 |
| `execution/streaming/checkpointing/` | 10 | 10 | 10 |
| `execution/streaming/operators/stateful/` | 8 | 8 | 8 |
| `…/stateful/flatmapgroupswithstate/` | 3 | 3 | 3 |
| `…/stateful/join/` | 4 | 4 | 4 |
| `…/stateful/transformwithstate/` | 5 | 5 | 5 |
| `…/stateful/transformwithstate/statefulprocessor/` | 2 | 2 | 2 |
| `…/stateful/transformwithstate/statevariables/` | 4 | 4 | 4 |
| `…/stateful/transformwithstate/ttl/` | 4 | 4 | 4 |
| `…/stateful/transformwithstate/timers/` | 3 | 3 | 3 |
| `…/stateful/transformwithstate/testing/` | 1 | 1 | 1 |
| `execution/streaming/sinks/` | 2 | 2 | 2 |
| `execution/streaming/utils/` | 1 | 1 | 1 |
| **Total** | **149** | **149** | **149** |

The "traced" column is the one the 2026-08-06 pass could not fill: it was 149/149 cited but
`continuous/` and `sources/` had one survey concept between 29 files. Each package now has at
least one concept that follows a path through it rather than naming it.

`check_drift.py --sweeps` reports 148/149 for the top-level scope; its path-aware matcher and the
by-filename count above disagree on one entry. Not material — the check passes either way.

### What is still shallow

Small enough to state in one place, and none of it hides a layer:

- **Individual RocksDB tuning knobs.** The lookup mechanism, the defaults and the memory model are
  now traced; what each knob does to RocksDB's own behaviour (compaction shape, bloom filters,
  block layout) is RocksDB documentation, not Spark source.
- **`ContinuousTextSocketSource` and `ContinuousRateStreamSource` internals.** Both are named and
  placed; the continuous protocol they participate in is fully traced, and neither is used outside
  tests and demos.
- **The `statevariables/` and `timers/` classes** are described through their shared contracts
  (column families, the TTL index, the handle state machine) rather than one section each. That is
  a deliberate grouping, not an unvisited area.

Out of scope by group boundary, and covered elsewhere: the Python streaming operators
(`python-arrow`), the `state` / `state-metadata` read sources (`datasources`),
`UnsupportedOperationChecker` and the streaming plan markers (`sql/catalyst`),
`StreamingQueryManager` and `DataStreamWriter` (`classic-api`), and Kafka (a separate module with
no group).

---

## Overlapping topic traces

`check_drift.py --sweeps` lists five topic traces covering codes in this page's front matter, all
recorded against the same Spark 4.2.0 — so there is no version skew to reconcile.

| Trace | Overlap with this sweep | Verdict |
|---|---|---|
| [`topics/b4.md`](../topics/b4.md) — Reading and Writing Data | `FileStreamSource`, `FileStreamSink` and the `_spark_metadata` log | Agree. B4 approaches the file sink from the *reader's* side (why another engine sees stray files); this page approaches it from the commit protocol. Complementary, no contradiction |
| [`topics/b5.md`](../topics/b5.md) — Schema | state schema compatibility | Agree, and this sweep extends it: B5 treats schema as a read-time concern, while the state schema checker adds a *stored*-schema dimension with its own evolution rules, IDs and ceilings. The new **E47** proposal is where that belongs |
| [`topics/b6.md`](../topics/b6.md) — Basic Aggregations | streaming aggregation state managers, session windows | Agree. B6's trace stops at the batch aggregate operators; the V1/V2 state format split is only visible here, and the re-sweep adds that the format is chosen in `SparkStrategies`, not in the operator |
| [`topics/b7.md`](../topics/b7.md) — Joins | stream-stream join | Agree. B7 traces `JoinSelection` and the batch strategies; `StreamingSymmetricHashJoinExec` is selected elsewhere and its four state stores are only described here. The V4 format's timestamp key encoders are new in this re-sweep |
| [`topics/i5.md`](../topics/i5.md) — Partitioning | `StateStoreRDD` preferred locations, the pinned partition count | Agree, with one thing I5 does not say: a stateful query's partition count is **not** `spark.sql.shuffle.partitions` at all — it is read from the checkpoint by `IncrementalExecution`. That is stated on this page and in **E28** |

No trace exists for A7, A8, A14, A36, E1, E2, E3, E27 or E28, so for those codes this page is the
only source-level coverage.

## Sweep log

| Date | Spark | What changed |
|---|---|---|
| 2026-08-06 | 4.2.0 | Initial sweep of the group — the largest in the map at 149 files, every one cited, across nine packages. 33 concepts, 3 new topics proposed (A36 the checkpoint protocol, E27 the state store engine, E28 offline state repartition). `status: partial` deliberately: every sub-package has a traced concept but `continuous/` and `sources/` got survey-level treatment only, and the remainder is named. Headline findings: the offset-log-before / commit-log-after ordering is the whole exactly-once story and makes a crashed batch re-run *identically* rather than differently; `IncrementalExecution` pins the shuffle partition count from the checkpoint, which is why `spark.sql.shuffle.partitions` does nothing on a stateful query and why 4.2.0 needed an offline repartition runner; and Real-Time Mode gates itself with a literal class-name allowlist, which is the most reliable statement of what it supports. Also corrected an existing error in A8's callout: `spark.scheduler.streaming.idAwareLogging.enabled` defaults to **`true`** (4.2.0), not `false` |
| 2026-08-09 | 4.2.0 | **Re-sweep at an unchanged 4.2.0**, taking the previous run's "deliberately not covered" list as the work item. **Breadth check 1 (the config slice) found the work** — resolving each of the 113 keys to its actual reader showed the old table was wrong in three ways, and each error pointed at an untraced layer: `rocksdb.*` was recorded as 30 catalog keys when the catalog holds **2** (the other ~29 are undeclared strings in `RocksDBConf`), `asyncProgressTracking*` was recorded as 3 configs when it is **writer options only**, and `transformWithStateOp.stateSchemaVersion` does not exist (the key is `transformWithState.stateSchemaVersion`). Breadth check 2 was green on citations both times and found nothing. **9 new concepts**: the epoch protocol and the continuous reader/writer tasks (`continuous/`, 11 files, previously one survey concept); the DSv2 streaming write path and how a source implements `MicroBatchStream` (`sources/`, 18 files, same); the RocksDB tuning surface; range-scan key encoding; timestamp key encoders; Avro state encoding and schema evolution; TTL indexes. **3 new topics proposed** (A45 writing a streaming sink, E47 Avro state encoding and schema evolution, E48 continuous processing and the epoch coordinator), and the previous run's A36 / E27 / E28 proposals converted to mappings now that they are in the path. `status` raised **partial → complete**: every package now has a traced path through it, not just citations, and what remains shallow is named in one paragraph. Also restructured the trailing sections into the four-section contract — the old page carried the deprecated merged `## Breadth checks` shape, no overlap section, and a `## Refresh log` where the sweep log belongs. Headline findings: every `*.stateFormatVersion` is read in `SparkStrategies` and pinned by `OffsetSeq`, so it is a *planning-time* decision recorded in the checkpoint; value-schema evolution is gated on `usingAvro && schemaEvolutionEnabledForOperator`, and that second flag is `true` in exactly one place, so **no operator except `transformWithState` can evolve its state schema**; turning on Avro encoding forces every state field nullable; a TTL-enabled `ListState` occupies four column families because RocksDB `merge` makes element-level deletion impossible; and continuous processing's at-least-once guarantee is a two-line comment in `getStartOffsets` rather than a design limit anyone documented |
