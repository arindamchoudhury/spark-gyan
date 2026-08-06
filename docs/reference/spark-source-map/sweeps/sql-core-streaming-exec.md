---
subsystem: sql/core
spark_version: "4.2.0"
swept_at: 2026-08-06
group: streaming-exec
all_groups: [query-execution, joins-exec, adaptive, datasources, agg-window-exchange, python-arrow, streaming-exec, classic-api, sql-scripting]
status: partial
concepts:
  - name: StreamExecution — one thread, a state machine, and a stored death cause
    topics: [A7]
  - name: TriggerExecutor — four trigger shapes behind one interface
    topics: [A7]
  - name: MicroBatchExecution — the batch loop, and the two-log write-ahead protocol
    topics: []
    propose:
      code: A36
      level: Advanced
      title: "The Streaming Checkpoint Protocol: Offset Log, Commit Log, and Restart"
      what: "A micro-batch is durable before it runs: the offset log records the batch's end offsets before any data is processed, the commit log records completion after the sink commits, and on restart the presence or absence of a commit entry for the latest offset entry is what decides whether Spark replays that batch or moves to the next one."
      why: "Every exactly-once claim, every 'my query reprocessed a batch after restart' question, and every checkpoint-corruption incident resolves to the ordering of those two writes and what the recovery path reads back — and none of it is visible from the DataFrame API, which is why checkpoints are the part of streaming operations people learn by outage."
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
    topics: []
    propose:
      code: E27
      level: Expert
      title: "The State Store Engine: RocksDB, Changelog Checkpointing, and Maintenance"
      what: "Behind every stateful streaming operator is a versioned key-value store; the RocksDB provider keeps a local instance per partition, writes each batch's mutations to a changelog file, periodically uploads a full snapshot in a background maintenance thread, and reconstructs any version by loading the nearest snapshot and replaying changelogs on top of it."
      why: "It is the layer that decides whether a stateful query restarts in seconds or in an hour, whether a corrupt checkpoint is recoverable, and what the memory footprint of a large-state job actually is — and its whole configuration surface (changelog checkpointing, snapshot lag reporting, checkpoint IDs, row checksums, auto snapshot repair, maintenance timeouts) is invisible from the query API."
  - name: State checkpoint IDs — the V2 lineage that makes a state store verifiable
    topics: [A8, A14]
  - name: Row checksums and auto snapshot repair — two 4.1.0 corruption defences, one on by default
    topics: [A8, E3]
  - name: State schema evolution — StateSchemaCompatibilityChecker and the operator metadata log
    topics: [A8, B5]
  - name: Offline state repartition — changing a stateful query's partition count
    topics: []
    propose:
      code: E28
      level: Expert
      title: "Offline State Repartition: Changing shuffle.partitions on a Stateful Query"
      what: "A stateful streaming query's state is keyed by partition id, so its shuffle partition count is frozen at the first batch; Spark 4.2.0 adds an offline runner that reads the existing state through the state data source, repartitions it to a new count, writes it back as an extra batch N+1, and lets the query resume at the new parallelism."
      why: "Until this existed, the answer to 'my stateful query is under-parallelised' was to rebuild the checkpoint and reprocess from the source. It is the single highest-consequence operational procedure in streaming, it leaves a half-finished batch behind if it fails, and 4.2.0 ships a startup check specifically to detect that."
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
  - name: FileStreamSource — the seen-files map, the trigger limits, and cleanSource
    topics: [A7, B4]
  - name: FileStreamSink and the _spark_metadata log — a sink whose output only Spark can read correctly
    topics: [A7, B4]
  - name: The built-in sources and sinks — rate, socket, memory, console, foreach
    topics: [A7]
  - name: Continuous processing and the epoch coordinator
    topics: [A7]
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

!!! warning "`status: partial` — and what that means here"

    Every sub-package below gets at least one traced concept, and the breadth checks are green.
    But depth is genuinely uneven: `runtime/` and `state/` are traced closely, while `continuous/`
    (11 files) and `sources/` (18) each get a single survey-level concept. The
    "Deliberately not covered" section at the end names exactly what was enumerated rather than
    traced, so the next run can take one of those rather than re-skimming the whole group.

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

**Maps to topics:** none — proposed as **A36**

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

**Maps to topics:** none — proposed as **E27**

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

**Maps to topics:** none — proposed as **E28**

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
`spark.sql.streaming.transformWithStateOp.stateSchemaVersion`

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

---

## Continuous processing and Real-Time Mode

### Continuous processing and the epoch coordinator

**What it is:** the pre-4.x low-latency execution path, and a genuinely different model.
`ContinuousExecution` launches **long-running tasks** that never finish, and durability is provided
by *epochs* rather than batches: a driver-side `EpochCoordinator` RPC endpoint increments an epoch
counter on a timer, collects a `ReportPartitionOffset` from every reader partition and a
`CommitPartitionEpoch` from every writer partition, and only when all of them have reported does it
tell `ContinuousExecution` to commit that epoch to the offset and commit logs.

`EpochTracker` is the thread-local carrying the current epoch inside a task;
`ContinuousQueuedDataReader` is the reader-side queue decoupling the source from the epoch
boundary; `ContinuousDataSourceRDD` and `ContinuousWriteRDD` are the never-terminating RDDs; and
`ContinuousTaskRetryException` is how a task restart is distinguished from a failure.

Its limits are structural: at-least-once only, a small set of supported operations, and no
shuffles — which is why 4.2.0's Real-Time Mode is a new effort rather than an extension of it.

**Anchor files:**

- [continuous/ContinuousExecution.scala:49](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/continuous/ContinuousExecution.scala#L49) — `runContinuous` at :219, `commit` at :390
- [continuous/EpochCoordinator.scala:109](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/continuous/EpochCoordinator.scala#L109) — the coordinator's contract in its scaladoc; `receive` at :208
- [continuous/EpochTracker.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/continuous/EpochTracker.scala) / [continuous/ContinuousQueuedDataReader.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/continuous/ContinuousQueuedDataReader.scala) / [continuous/ContinuousDataSourceRDD.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/continuous/ContinuousDataSourceRDD.scala) / [continuous/ContinuousWriteRDD.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/continuous/ContinuousWriteRDD.scala)
- [continuous/WriteToContinuousDataSource.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/continuous/WriteToContinuousDataSource.scala) / [continuous/WriteToContinuousDataSourceExec.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/continuous/WriteToContinuousDataSourceExec.scala) / [continuous/ContinuousRateStreamSource.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/continuous/ContinuousRateStreamSource.scala) / [continuous/ContinuousTextSocketSource.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/continuous/ContinuousTextSocketSource.scala) / [continuous/ContinuousTaskRetryException.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/continuous/ContinuousTaskRetryException.scala)
- [runtime/ContinuousRecordEndpoint.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/ContinuousRecordEndpoint.scala)

**Configs:** `spark.sql.streaming.continuous.executorQueueSize`,
`…continuous.executorPollIntervalMs`, `…continuous.epochBacklogQueueSize`

**Maps to topics:** A7

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

## Breadth checks

### Config breadth

Slice pattern over `sql/catalyst` + `sql/core` + `core`:

```
\.streaming|stateStore|watermark|checkpoint|\.rocksdb|Streaming
```

**114 keys.** Every family ties to a concept above:

| Family | Roughly | Tied to |
|---|---|---|
| `streaming.stateStore.rocksdb.*` | 30 | RocksDB provider |
| `streaming.stateStore.*` (maintenance, coordinator, lag, checksums, repair, formats) | 30 | state-store API, maintenance, coordinator, checksums, repair |
| `streaming.*` batch loop (minBatchesToRetain, noDataMicroBatches, pollingDelay, stopTimeout, offsetLog.formatVersion) | 12 | MicroBatchExecution, TriggerExecutor |
| `streaming.checkpoint*` / `checkpointFileManagerClass` / fileChecksum | 8 | CheckpointFileManager, metadata logs |
| `streaming.*.stateFormatVersion` (aggregation, join, sessionWindow, flatMapGroupsWithState, transformWithState) | 6 | the per-operator state managers |
| `streaming.multipleWatermarkPolicy`, `statefulOperator.*`, `validateEventTimeWatermarkColumn` | 5 | watermarks, statefulOperators |
| `streaming.fileSource.*` / `fileSink.*` log settings | 6 | CompactibleFileStreamLog |
| `streaming.continuous.*` | 4 | continuous processing |
| `streaming.realTimeMode.*` | 2 | Real-Time Mode |
| `streaming.asyncProgressTracking*` | 3 | async progress tracking |
| `streaming.queryEvolution.*`, `checkUnfinishedRepartitionOnRestart`, `internal.stateStore.partitions` | 4 | schema evolution, offline repartition, IncrementalExecution |
| `streaming.metricsEnabled`, `numRecentProgressUpdates`, `ui.*` | 4 | ProgressReporter / MetricsReporter |

**Out of scope but kept in the slice:** `spark.scheduler.streaming.idAwareLogging.*` (2, core
scheduler — `core — execution-engine`), `spark.checkpoint.*` and `spark.cleaner.*` (RDD
checkpointing, `core — rdd-layer`), `spark.sql.adaptive.streaming.stateless.enabled`
(`adaptive`), `spark.sql.streaming.stateStore.providerClass`'s Python-side counterparts
(`python-arrow`).

### Package breadth

Walked by hand — the group has six levels of nesting that `--coverage` cannot see.

| Package | Files | Cited |
|---|---|---|
| `execution/streaming/` (top level) | 7 | 7 |
| `execution/streaming/runtime/` | 37 | 37 |
| `execution/streaming/state/` | 29 | 29 |
| `execution/streaming/sources/` | 18 | 18 |
| `execution/streaming/continuous/` | 11 | 11 |
| `execution/streaming/checkpointing/` | 10 | 10 |
| `execution/streaming/operators/stateful/` (+ 6 nested) | 34 | 34 |
| `execution/streaming/sinks/` | 2 | 2 |
| `execution/streaming/utils/` | 1 | 1 |

**Every file in the group is cited.** (`check_drift.py --sweeps` reports 148/149 for the
top-level scope; its path-aware matcher and the by-filename count above disagree on one entry.
The difference is not material — the check passes either way.)

!!! warning "Citation breadth is green; that is not why this page is `partial`"

    Every file is cited, and `check_drift.py --sweeps` is happy. The `status: partial` is a
    judgement about **depth**, not about the ratio above — several packages are covered by a
    single survey-level concept, and a citation is not a trace. The next section names exactly
    which, so nobody reads 149/149 as "this group is finished".

### Deliberately not covered — where the next run should start

This is why the page is `status: partial`. Each item was read enough to place it, not enough to
trace:

- **`continuous/` in depth.** One survey concept covers 11 files. The epoch-commit protocol
  deserves the same treatment the micro-batch protocol got here.
- **`sources/` in depth.** One concept covers 18 files. The DSv2 streaming write path
  (`MicroBatchWrite` → `WriteToMicroBatchDataSource` → the V1 fallback) is a real trace on its own.
- **RocksDB configuration surface.** ~30 keys are attributed to the provider concept as a family;
  the individual tuning knobs (block cache, write buffer, compaction, bloom filters) are not
  traced.
- **`RocksDBStateEncoder` internals.** Range-scan key encoding with correct ordering for signed and
  floating-point types is subtle and only named here.
- **Avro state encoding.** `spark.sql.streaming.stateStore.encodingFormat=avro` and its schema
  evolution story is mentioned, not traced.
- **Files cited only as a family** rather than individually explained: the three `*ImplWithTTL`
  state variables, `StatefulProcessorHandleImplBase`, and several `runtime/` helpers. They are
  linked and placed, but their behaviour is described at the level of the family they belong to.

Also out of scope by group boundary, and covered elsewhere: the Python streaming operators
(`python-arrow`), the `state` / `state-metadata` read sources (`datasources`),
`UnsupportedOperationChecker` and the streaming plan markers (`sql/catalyst`), and Kafka
(a separate module with no group).

---

## Refresh log

| Date | Spark | What changed |
|---|---|---|
| 2026-08-06 | 4.2.0 | Initial sweep of the group — the largest in the map at 149 files, every one cited, across nine packages. 33 concepts, 3 new topics proposed (A36 the checkpoint protocol, E27 the state store engine, E28 offline state repartition). `status: partial` deliberately: every sub-package has a traced concept but `continuous/` and `sources/` got survey-level treatment only, and the remainder is named. Headline findings: the offset-log-before / commit-log-after ordering is the whole exactly-once story and makes a crashed batch re-run *identically* rather than differently; `IncrementalExecution` pins the shuffle partition count from the checkpoint, which is why `spark.sql.shuffle.partitions` does nothing on a stateful query and why 4.2.0 needed an offline repartition runner; and Real-Time Mode gates itself with a literal class-name allowlist, which is the most reliable statement of what it supports. Also corrected an existing error in A8's callout: `spark.scheduler.streaming.idAwareLogging.enabled` defaults to **`true`** (4.2.0), not `false` |
