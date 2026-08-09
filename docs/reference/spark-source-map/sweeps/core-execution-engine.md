---
subsystem: core
spark_version: "4.2.0"
swept_at: 2026-08-09
group: execution-engine
all_groups: [rdd-layer, execution-engine, shuffle-memory, storage-serializer,
  submit-standalone, monitoring, config-security, rpc-resources, api-bridge]
status: complete
concepts:
  - name: job-submission
    topics: [B1, E1]
  - name: stage-creation-and-reuse
    topics: [B1, E1, I7]
  - name: driver-side-planning
    topics: [E1]
  - name: partition-selection-and-preferred-locations
    topics: [B1, E1, A4]
  - name: task-completion-and-accumulators
    topics: [B1, E1]
  - name: fetch-failure-and-stage-retry
    topics: []
    propose:
      code: A13
      level: Advanced
      title: "Stage Retry: Fetch Failures, Executor Loss, and When Spark Gives Up"
      what: "A FetchFailed means a reduce task could not read a map output; the driver unregisters the lost output, re-runs the map stage, and aborts the job once spark.stage.maxConsecutiveAttempts is exhausted. Executor loss and host loss unregister different amounts of output depending on whether an external shuffle service is running."
      why: "This is the most common production Spark failure a practitioner will ever debug, and every default that governs it is non-obvious: maxConsecutiveAttempts=4, ignoreDecommissionFetchFailure=true, unRegisterOutputOnHost=false, maxRetainedRemovedDecommissionExecutors=0. Reading FetchFailed then Resubmitting stage N in a driver log without this model is guesswork."
  - name: indeterminate-stages-and-rollback
    topics: []
    propose:
      code: A14
      level: Advanced
      title: "Determinism, Indeterminate Stages, and Correctness Under Retry"
      what: "If a shuffle map stage produces different data when re-run — repartition on unordered input, zipWithIndex, a non-deterministic UDF — then downstream stages that already consumed the old output are inconsistent. Spark rolls back every succeeding stage, or aborts the job when it cannot, and 4.2.0 adds runtime detection via a MapStatus checksum comparison."
      why: "The alternative to the abort is silently wrong data. The failure only appears after some unrelated retry, so a job can run correctly a hundred times and then abort with a message about checkpointing before repartition. The new checksum detection also means jobs that previously produced quiet corruption will start failing loudly after upgrading to 4.2.0."
  - name: job-completion-and-cancellation
    topics: [B1, E1]
  - name: push-based-shuffle-finalization
    topics: [A4]
  - name: scheduling-mode-and-pools
    topics: [B1, E1]
  - name: executor-registration-and-offers
    topics: [B1, E2]
  - name: taskset-submission-and-zombies
    topics: [B1, E1]
  - name: delay-scheduling-and-locality
    topics: [B1, I5, A4]
  - name: slot-arithmetic-and-resource-profiles
    topics: [B1, E2]
  - name: task-execution-on-the-executor
    topics: [B1, E1]
  - name: task-result-delivery
    topics: [B1, E1]
  - name: task-failure-and-retry
    topics: [B1, E1]
  - name: speculation
    topics: [A4, B1]
  - name: executor-exclusion
    topics: []
    propose:
      code: E12
      level: Expert
      title: "Executor Exclusion and Health Tracking"
      what: "Two tiers of failure tracking: TaskSetExcludelist escalates within one stage attempt from (task, executor) to (task, node) to whole executors and nodes, while HealthTracker accumulates across the application and can kill or decommission the offender — but only learns about failures when a TaskSet completes successfully."
      why: "A single flaky node manifests as a stage that retries and then aborts with 'cannot run anywhere due to node and executor excludeOnFailure', which is opaque without the two-tier model. Spark also refuses to start when spark.excludeOnFailure.task.maxTaskAttemptsPerNode is greater than or equal to spark.task.maxFailures, because exclusion could then never route around a bad node; the error names both keys but the reasoning is not obvious."
  - name: kill-path-and-task-reaper
    topics: [E1]
  - name: heartbeat-and-expiry
    topics: [E3, E2, B1]
  - name: executor-metrics-polling
    topics: [E3]
  - name: decommissioning
    topics: [E2]
  - name: dynamic-allocation
    topics: [E2, A4]
  - name: barrier-execution
    topics: []
    propose:
      code: E13
      level: Expert
      title: "Barrier Execution Mode"
      what: "A barrier stage is gang-scheduled: resourceOffers refuses to launch any task unless it can place every task of the stage in a single offer round, and at runtime BarrierTaskContext.barrier() blocks until all tasks in the stage have called it or a timeout fails them all."
      why: "Barrier mode is how distributed training frameworks embed in Spark, and both of its failure modes are silent hangs rather than errors: a cluster that cannot supply every slot at once waits forever rather than failing at submit, and an unequal number of barrier() calls across branches hangs the job until the 365-day RPC timeout is overridden by the coordinator's own timer."
  - name: task-context-lifecycle
    topics: [E1, B1]
  - name: executor-loss-handling
    topics: [B1, E1, A4]
  - name: output-commit-coordination
    topics: []
    propose:
      code: E17
      level: Expert
      title: "Output Commit Coordination and Speculative Write Safety"
      what: "A driver-side authority grants exactly one task attempt per partition the right to commit its output, on a first-committer-wins policy; every other attempt is denied and throws CommitDeniedException, which the scheduler treats as a non-counting failure rather than a task error."
      why: "This is the only thing standing between speculation or a stage retry and duplicated output files, and it protects exactly one thing — the Hadoop commit protocol. Writes a user performs directly from a task are outside it, and an undocumented escape hatch, spark.hadoop.outputCommitCoordination.enabled, disables the whole mechanism."
  - name: unschedulable-tasksets-and-the-abort-timer
    topics: [E12, E2]
  - name: cluster-manager-selection-and-local-mode
    topics: [B1, E2]
  - name: taskinfo-accumulable-retention
    topics: [E3, E1]
  - name: streaming-id-aware-scheduler-logging
    topics: [E3, A8]
  - name: the-hadoop-commit-protocol
    topics: [E17, B4]
  - name: the-task-object-and-its-serialization
    topics: [B1, E1]
  - name: task-metrics-and-the-accumulator-pipeline
    topics: []
    propose:
      code: E49
      level: Expert
      title: "Task Metrics and the Accumulator Pipeline"
      what: "Every per-task number Spark reports is a LongAccumulator on TaskMetrics, merged back to the driver through two independent channels — partial values on each executor heartbeat and final values in the task result — with user accumulators riding the same pipeline as _externalAccums."
      why: "It is the only model that explains why a custom accumulator over-counts under speculation and stage retry, why shuffle-read metrics are zero until mergeShuffleReadMetrics runs, and why spark.executor.heartbeat.dropZeroAccumulatorUpdates changes what a live UI shows without changing any final number. Anyone who has written an accumulator and not trusted its value needs this."
  - name: executor-memory-metrics-and-procfs
    topics: [E3]
  - name: driver-executor-message-protocol
    topics: [E1, E2]
  - name: executor-class-loading-and-session-isolation
    topics: []
    propose:
      code: E50
      level: Expert
      title: "Executor Class Loading, Classpath Precedence, and Session Isolation"
      what: "An executor builds one class loader per job-artifact state, optionally wrapping it in an ExecutorClassLoader that fetches REPL classes over RPC, and caches one such state per Spark Connect session in a bounded LRU; spark.executor.userClassPathFirst inverts delegation for both classes and resources."
      why: "Dependency conflicts are resolved — or made worse — here. userClassPathFirst also changes getResourceAsStream, so shading a jar to fix a NoSuchMethodError can silently take over META-INF/services and logging config. It is also the executor half of Connect multi-tenancy, where the LRU size decides whether an idle session re-resolves all its artifacts on its next task."
  - name: rdd-write-path-and-hadoop-output-formats
    topics: [E17, B4, I4]
  - name: the-schedulable-tree
    topics: [B1, E1]
  - name: executor-loss-reasons-and-exit-codes
    topics: [E2, A13]
  - name: fractional-resource-allocation
    topics: [E2, B1]
  - name: dagscheduler-event-loop
    topics: [E1, B1]
  - name: preferred-locations-from-hadoop-input-formats
    topics: [I5, A4]
---

The scheduling core: how an action becomes a job, a job becomes stages, a stage becomes tasks, and what happens when any of it fails. Swept in two halves — the driver-side job/stage layer (`DAGScheduler`) and the task-scheduling/executor layer (`TaskSchedulerImpl`, `TaskSetManager`, `Executor`).

!!! info "Where the boundary sits"

    `DAGScheduler` decides *what* to run and handles stage-level failure. `TaskSchedulerImpl` and `TaskSetManager` decide *where* each task runs and handle task-level failure. Speculation and executor exclusion are decided entirely in the task layer — `DAGScheduler` only re-posts their events to the listener bus, and reads no `spark.speculation.*` or `spark.excludeOnFailure.*` config at all.

---

## Job submission

**What it is:** every RDD action funnels into `DAGScheduler.submitJob`, which validates partition ids, forces partition computation *off* the event loop, allocates a job id and posts `JobSubmitted`. `runJob` blocks on the returned `JobWaiter`. A third door, `submitMapStage`, runs a `ShuffleMapStage` alone and returns `MapOutputStatistics` — that is AQE's entry point.

**Code path:** `RDD action` → `SparkContext.runJob` → `DAGScheduler.submitJob` → `eagerlyComputePartitionsForRddAndAncestors` → `post(JobSubmitted)` → `DAGSchedulerEventProcessLoop.doOnReceive` → `handleJobSubmitted`

**Anchor files:**

- [DAGScheduler.scala:984](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala#L984) — `submitJob`
- [DAGScheduler.scala:1005](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala#L1005) — zero-partition fast path: synthesises `JobStart`/`JobEnd`, never touches the event loop
- [DAGScheduler.scala:1063](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala#L1063) — SPARK-8644: the caller's stack trace is spliced onto the exception so you see your own call site
- [DAGScheduler.scala:1122](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala#L1122) — `submitMapStage`, the AQE door; unlike `submitJob` it *throws* on a zero-partition RDD
- [DAGScheduler.scala:3526](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala#L3526) — `doOnReceive`, the whole event vocabulary in one dispatch table

**Configs:** none directly; `spark.scheduler.listenerbus.*` governs where `JobStart`/`JobEnd` land

**Maps to topics:** B1, E1

---

## Stage creation and cross-job reuse

**What it is:** `handleJobSubmitted` builds a `ResultStage` for the final RDD and walks back over `ShuffleDependency` edges creating a `ShuffleMapStage` per shuffle. The detail that explains the UI: **stages are keyed by shuffle id, not by RDD or job**. A second job over the same shuffle dependency gets the *same* stage object, and if its outputs are still registered with `MapOutputTracker` the stage is skipped entirely.

**Code path:** `handleJobSubmitted` → `createResultStage` → `getShuffleDependenciesAndResourceProfiles` → `getOrCreateParentStages` → `getOrCreateShuffleMapStage` → `createShuffleMapStage` → `mapOutputTracker.registerShuffle`

**Anchor files:**

- [DAGScheduler.scala:160](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala#L160) — `shuffleIdToMapStage`, the reuse table; dropped when the last dependent job finishes
- [DAGScheduler.scala:574](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala#L574) — `createShuffleMapStage`
- [DAGScheduler.scala:593](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala#L593) — `registerShuffle` only when not already present — the reuse guard
- [Stage.scala:149](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/Stage.scala#L149) — `equals`/`hashCode` keyed on `id` alone, which is what makes the `HashSet[Stage]` bookkeeping work
- [DAGScheduler.scala:912](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala#L912) — `cleanupStateForJobAndIndependentStages`

!!! info "This is where grey 'skipped' stages in the UI come from"

    A skipped stage is one whose outputs `MapOutputTracker` still holds. After the owning jobs finish, the `shuffleIdToMapStage` entry is dropped but the tracker entry survives — so a later job creates a *fresh* stage object with a new id and still skips its tasks.

- [StageInfo.scala:96](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/StageInfo.scala#L96) — `fromStage`, the listener-facing snapshot; it carries a `shuffleDepId` only for a `ShuffleMapStage`, which is how the UI tells the two stage kinds apart

**Configs:** `spark.scheduler.mode`, `spark.scheduler.allocation.file`; [`spark.scheduler.resource.profileMergeConflicts`](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala#L238) gates stage-level profile merging — with it off, two conflicting `ResourceProfile`s on one stage throw, and the message [names the key](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala#L662)

**Maps to topics:** B1, E1, I7

---

## Driver-side planning, and why wide lineages stall

**What it is:** before a single task launches, the driver walks the RDD graph several times. In 4.2.0 all of these route through a shared *iterative* helper with an explicit stack — a deliberate defence against `StackOverflowError` on deep lineages. The expensive part is `eagerlyComputePartitionsForRddAndAncestors`, which calls `.partitions` on every RDD in the DAG and runs on the *caller's* thread precisely so a slow `getPartitions()` cannot stall every other job.

**Code path:** `createResultStage` → `getShuffleDependenciesAndResourceProfiles` → `traverseRDDGraph` → `traverseRDDGraphUntil`; and `submitStage` → `getMissingParentStages` → `getCacheLocs` → `blockManagerMaster.getLocations`

**Anchor files:**

- [DAGScheduler.scala:754](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala#L754) — `traverseRDDGraphUntil`, iterative to avoid stack overflow on deep lineage
- [DAGScheduler.scala:875](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala#L875) — `eagerlyComputePartitionsForRddAndAncestors`
- [DAGScheduler.scala:884](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala#L884) — the debug-level timing log; the *only* visibility a user gets into this cost
- [DAGScheduler.scala:840](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala#L840) — cache-location RPC timeout → assume uncached
- [DAGScheduler.scala:499](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala#L499) — `getCacheLocs`, memoised per RDD id, invalidated on job submit, stage completion, executor loss and resubmit

!!! warning "A transient RPC timeout silently recomputes a cached stage"

    `getMissingParentStages` catches `RpcTimeoutException` from the block-manager master, logs a warning, and treats the RDD as **not cached**. A parent stage that was fully cached is then recomputed. There is no metric for this — only the WARN.

!!! info "Why 'job submitted, zero tasks running' happens"

    All of the above is driver-side and mostly on the single-threaded event loop. On iterative ML loops or long chains of unioned DataFrames this can run for minutes with an idle cluster. `checkpoint()` truncates the lineage and is the usual fix. This is routinely misdiagnosed as cluster slowness.

**Configs:** no config gates the traversal; `spark.rdd.cache.visibilityTracking.enabled` adds a per-task block-visibility update

**Maps to topics:** E1

---

## Partition selection and preferred locations

**What it is:** `submitStage` recursively submits missing parents, then `submitMissingTasks` computes what to run. The two stage subclasses answer `findMissingPartitions` from **completely different sources of truth**: `ShuffleMapStage` asks `MapOutputTracker` (which is why an executor loss "un-completes" partitions of a finished stage), while `ResultStage` asks its `ActiveJob`'s `finished` array.

**Code path:** `submitStage` → `getMissingParentStages` → `submitMissingTasks` → `findMissingPartitions` → `getPreferredLocs` → `makeNewStageAttempt` → serialize + broadcast `taskBinary` → `taskScheduler.submitTasks(TaskSet)`

**Anchor files:**

- [DAGScheduler.scala:1635](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala#L1635) — `submitMissingTasks`
- [ShuffleMapStage.scala:92](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/ShuffleMapStage.scala#L92) — tracker-backed `findMissingPartitions`, falls back to all partitions
- [ResultStage.scala:62](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/ResultStage.scala#L62) — job-backed `findMissingPartitions`; valid only while an active job exists
- [DAGScheduler.scala:3443](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala#L3443) — SPARK-695 visited-set guard against exponential path exploration
- [DAGScheduler.scala:3448](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala#L3448) — precedence: cache locations → `rdd.preferredLocations` → first narrow parent with a preference
- [DAGScheduler.scala:1744](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala#L1744) — `RDDCheckpointData.synchronized` around task-binary serialization, for a consistent checkpoint view
- [DAGScheduler.scala:1756](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala#L1756) — the large-task-binary warning

**Configs:** `spark.stage.maxAttempts`, `spark.scheduler.barrier.maxConcurrentTasksCheck.interval`, `spark.scheduler.barrier.maxConcurrentTasksCheck.maxFailures`

**Maps to topics:** B1, E1, A4

---

## Task completion and accumulators

**What it is:** `handleTaskCompletion` is the largest method in `DAGScheduler` and the heart of the layer. Order is deliberate and commented: `OutputCommitCoordinator` first, then accumulator merge, then `postTaskEnd`, then the reason-specific match.

**Code path:** `TaskSetManager` → `dagScheduler.taskEnded` → `post(CompletionEvent)` → `handleTaskCompletion` → `updateAccumulators` → `postTaskEnd` → match on reason

**Anchor files:**

- [DAGScheduler.scala:2203](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala#L2203) — `handleTaskCompletion`
- [DAGScheduler.scala:2214](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala#L2214) — completions for unknown stages are dropped by the scheduler but still posted to listeners
- [DAGScheduler.scala:1882](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala#L1882) — accumulator merge failure logged and swallowed
- [DAGScheduler.scala:2337](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala#L2337) — epoch guard: "Ignoring possibly bogus … completion" from an executor already declared dead
- [DAGScheduler.scala:2969](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala#L2969) — `incrementEpoch` on every successful map stage
- [DAGScheduler.scala:2973](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala#L2973) — a stage can complete and immediately resubmit itself if outputs vanished in between

!!! warning "A custom accumulator that throws in `merge` corrupts silently"

    A `NonFatal` throw from a user `AccumulatorV2.merge` is logged at ERROR with the class name and then **ignored**. The job proceeds with a wrong accumulator value. Metric reconstruction failures in `postTaskEnd` are swallowed the same way.

**Configs:** `spark.rdd.cache.visibilityTracking.enabled`

**Maps to topics:** B1, E1

---

## Fetch failure and stage retry

**What it is:** a `FetchFailed` means a reduce task could not read a map output. The driver's response is layered, and every layer has a way to surprise: stale-attempt failures are dropped entirely; decommission-sourced failures are exempt from the retry budget; two independent retry ceilings apply; and how much output is thrown away depends on whether an external shuffle service is running.

**Code path:** `handleTaskCompletion (FetchFailed)` → staleness check → decommission exemption → `stageAbortReason` → `markStageAsFinished(willRetry)` → `mapOutputTracker.unregister*` → `abortStage` | `failedStages += ; scheduleResubmit` → `resubmitFailedStages` → `submitStage`

**Anchor files:**

- [DAGScheduler.scala:2395](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala#L2395) — `case FetchFailed`
- [DAGScheduler.scala:2399](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala#L2399) — fetch failures from a stale stage attempt are dropped without retry accounting
- [DAGScheduler.scala:2407](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala#L2407) — the decommission exemption: `failedAttemptIds.add` happens only in the `else`
- [DAGScheduler.scala:281](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala#L281) — `stageAbortReason`: both ceilings plus the test flag
- [DAGScheduler.scala:213](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala#L213) — `executorFailureEpoch` vs `shuffleFileLostEpoch`, and exactly why they diverge under an external shuffle service
- [DAGScheduler.scala:2532](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala#L2532) — host-scope unregistration needs ESS-or-decommission **and** `unRegisterOutputOnHost`
- [DAGScheduler.scala:2472](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala#L2472) — a fetch failure on a **barrier `ResultStage` aborts the job outright**, no retry
- [DAGScheduler.scala:3269](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala#L3269) — `clearFailures()` on success: the consecutive counter resets
- [DAGScheduler.scala:3620](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala#L3620) — `RESUBMIT_TIMEOUT = 200 ms`, coalescing the burst one executor loss produces

!!! warning "Four defaults decide whether your job survives, and none is obvious"

    `spark.stage.maxConsecutiveAttempts` = **4** (cleared on stage success) and `spark.stage.maxAttempts` = `Int.MaxValue` (never cleared) — the effective ceiling is the max of the two. `spark.stage.ignoreDecommissionFetchFailure` = **true** since 3.4.0, making graceful-decommission fetch failures free — but the exemption needs the executor to still be remembered, and `spark.scheduler.maxRetainedRemovedDecommissionExecutors` defaults to **0**, so it usually does not apply. `spark.files.fetchFailure.unRegisterOutputOnHost` = **false**, so a dead host with ESS loses its outputs one executor and one fetch failure at a time.

**Configs:** `spark.stage.maxConsecutiveAttempts`, `spark.stage.maxAttempts`, `spark.stage.ignoreDecommissionFetchFailure`, `spark.scheduler.maxRetainedRemovedDecommissionExecutors`, `spark.files.fetchFailure.unRegisterOutputOnHost`, `spark.shuffle.useOldFetchProtocol`, `spark.test.noStageRetry`

**Maps to topics:** none — proposed as A13

---

## Indeterminate stages and rollback

**What it is:** if a shuffle map stage produces *different data* on retry, downstream stages that already consumed the old output are inconsistent — a silent correctness bug. Spark's defence is to roll back and re-run every succeeding stage, or abort if it cannot. 4.2.0 has **two** detection mechanisms: static (`outputDeterministicLevel == INDETERMINATE`, known at RDD creation) and, new, runtime — `registerMapOutput` returns true when a re-registered `MapStatus` has a different checksum than a previous attempt's.

**Code path:** `registerMapOutput` → `isChecksumMismatched` → `rollbackSucceedingStages[ForQuery]` → `collectSucceedingStages` → `filterAndAbortUnrollbackableStages` → `rollbackShuffleMapStage` → `cancelStageAndTryResubmit`

**Anchor files:**

- [Stage.scala:75](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/Stage.scala#L75) — `isChecksumMismatched`, `maxChecksumMismatchedId`, `maxAttemptIdToIgnore`
- [ShuffleMapStage.scala:98](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/ShuffleMapStage.scala#L98) — `isStaticallyIndeterminate` vs `isRuntimeIndeterminate`
- [DAGScheduler.scala:2358](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala#L2358) — runtime detection at task completion
- [DAGScheduler.scala:2282](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala#L2282) — a *successful* `ResultTask` from an ignored attempt aborts the stage
- [DAGScheduler.scala:2705](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala#L2705) — the "eliminate the indeterminacy by checkpointing the RDD before repartition" message
- [DAGScheduler.scala:2717](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala#L2717) — `spark.shuffle.useOldFetchProtocol` makes rollback impossible, so final stages are aborted rather than silently completing with stale output
- [DAGScheduler.scala:2100](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala#L2100) — query-level rollback: a completed `ResultStage` in the same SQL execution makes the whole thing unrecoverable

!!! warning "A 4.2.0 upgrade will surface corruption that used to be silent"

    The checksum-based runtime detection is new. Jobs that previously produced quietly wrong data on retry will now abort loudly. That is an improvement, but it is an upgrade-notes-grade behaviour change: the same pipeline that ran for a year can start failing after the upgrade, and the trigger is an unrelated fetch failure.

!!! info "The blast radius can exceed the job"

    `rollbackSucceedingStagesForQuery` widens rollback to all jobs sharing a SQL execution id, including already-completed ones. A job can be aborted because of a *different, already-finished* job in the same query.

**Configs:** `spark.shuffle.useOldFetchProtocol`; the checksum flags come off the `ShuffleDependency` rather than a `spark.*` key read here

**Maps to topics:** none — proposed as A14

---

## Job completion and cancellation

**What it is:** a result job finishes when `numFinished == numPartitions`. `JobWaiter` is a thin listener over a Scala `Promise`. Cancellation has five entry points — job, group, tag, all, stage — all funnelling to `failJobAndIndependentStages`.

**Anchor files:**

- [JobWaiter.scala:63](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/JobWaiter.scala#L63) — `taskSucceeded`; the user `resultHandler` runs under `synchronized` because it may not be thread-safe
- [DAGScheduler.scala:3380](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala#L3380) — `failJobAndIndependentStages`: the listener is notified **only if cancellation succeeded**
- [DAGScheduler.scala:1923](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala#L1923) — `spark.job.interruptOnCancel` is read from job **local properties**, not SparkConf; an unparseable value falls back to `false` with a warning
- [DAGScheduler.scala:3630](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala#L3630) — `LimitedSizeFIFOSet`: cancelled job groups evict silently past 1000
- [DAGScheduler.scala:3604](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala#L3604) — any uncaught event-loop exception cancels all jobs and stops the SparkContext

!!! warning "An unkillable backend hangs the caller forever"

    If `killAllTaskAttempts` throws `UnsupportedOperationException`, `ableToCancelStages` is false and the job listener is **never notified** — a caller blocked in `runJob` waits indefinitely. Similarly, once a job group is evicted from the bounded tracking set, future jobs in that group are silently not cancelled.

- [JobResult.scala:27](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/JobResult.scala#L27) — the entire result vocabulary is `JobSucceeded` and `JobFailed(exception)`; a cancelled job is a `JobFailed`, which is why cancellation and failure are indistinguishable to a listener that reads only the result

**Configs:** `spark.job.interruptOnCancel` (local property), `spark.scheduler.numCancelledJobGroupsToTrack`, `spark.scheduler.stage.legacyAbortAfterKillTasks`

**Maps to topics:** B1, E1

---

## Push-based shuffle finalization

**What it is:** with push-based shuffle on, a completed `ShuffleMapStage` does not proceed straight to its children — merge finalization must complete first, because `getMissingParentStages` requires `shuffleMergeFinalized` in addition to `isAvailable`. This inserts a driver-side wait between map stage completion and reduce stage submission.

**Anchor files:**

- [DAGScheduler.scala:856](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala#L856) — the `!isAvailable || !shuffleMergeFinalized` condition that blocks child stages
- [DAGScheduler.scala:2987](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala#L2987) — merge statuses arriving after finalization are silently dropped (acknowledged `TODO: SPARK-35549`)
- [DAGScheduler.scala:352](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala#L352) — the six push-shuffle timing/threading configs read into fields

!!! info "Covered in full by the shuffle-memory sweep — not a carving gap"

    Only the *driver-side boundaries* are traced here, because push-based shuffle belongs to the
    `core — shuffle-memory` group, whose scope names it explicitly and whose config slice holds all
    thirteen `spark.shuffle.push.*` keys. An earlier version of this note read their absence from
    *this* group's slice as a `groups.yaml` carving gap; that was wrong — the carving is correct,
    and this group rightly does not own them. For the pusher, the merger negotiation, the server
    side of finalization (`RemoteBlockPushResolver`) and the reduce-side fallback, see
    [core — shuffle & memory](core-shuffle-memory.md). What remains genuinely untraced anywhere is
    `cancelFinalizeShuffleMergeFutures` and the finalize thread pool's timeout model.

- [MergeStatus.scala:45](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/MergeStatus.scala#L45) — the merged-block counterpart to `MapStatus`, carrying a `RoaringBitmap` of which map outputs made it into the merge; a reducer needs both to know what to fetch from where

**Configs:** `spark.shuffle.push.*`

**Maps to topics:** A4

---

## Scheduling mode and pools

**What it is:** `TaskSchedulerImpl` owns one `rootPool`. `spark.scheduler.mode` decides which `SchedulableBuilder` is installed: FIFO puts every `TaskSetManager` directly under the root; FAIR builds a two-level tree from `fairscheduler.xml` and routes each TaskSet by the `spark.scheduler.pool` local property.

**Anchor files:**

- [TaskSchedulerImpl.scala:210](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/TaskSchedulerImpl.scala#L210) — `initialize()` picks the builder and throws on anything but FIFO/FAIR
- [SchedulableBuilder.scala:209](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/SchedulableBuilder.scala#L209) — an unknown pool name is **not an error**: a pool is created with default minShare/weight and only a warning is logged
- [SchedulingAlgorithm.scala:43](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/SchedulingAlgorithm.scala#L43) — FAIR ordering: "needy" (running < minShare) first, then `minShareRatio`, then `runningTasks/weight`

!!! warning "A typo in `spark.scheduler.pool` degrades silently"

    It does not fail — it creates a new pool with default weight, so your job runs unweighted alongside the pools you carefully configured.

**Configs:** `spark.scheduler.mode`, `spark.scheduler.allocation.file`, `spark.scheduler.pool` (local property)

**Maps to topics:** B1, E1

---

## Executor registration and the offer loop

**What it is:** executors register over RPC; the backend keeps an `executorDataMap` of free cores and resources. Offers are generated reactively (registration, task completion, `reviveOffers`) or on a timer, and handed to `resourceOffers` as `WorkerOffer`s.

**Anchor files:**

- [CoarseGrainedSchedulerBackend.scala:249](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/cluster/CoarseGrainedSchedulerBackend.scala#L249) — `RegisterExecutor`: duplicate-id and excluded-node rejection at registration time
- [CoarseGrainedSchedulerBackend.scala:426](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/cluster/CoarseGrainedSchedulerBackend.scala#L426) — a `TaskDescription` at or above `spark.rpc.message.maxSize` **aborts the whole TaskSet**, not just the task
- [TaskSchedulerImpl.scala:794](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/TaskSchedulerImpl.scala#L794) — offers are `Random.shuffle`d, deliberately non-deterministic
- [CoarseGrainedSchedulerBackend.scala:704](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/cluster/CoarseGrainedSchedulerBackend.scala#L704) — `isReady()`: enough resources registered, **or** the waiting time elapsed

- [WorkerOffer.scala:26](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/WorkerOffer.scala#L26) — one offer is `(executorId, host, cores, address, resources, resourceProfileId)`; an offer round is a `Seq` of these, rebuilt from `ExecutorData` each time
- [ExecutorInfo.scala:28](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/cluster/ExecutorInfo.scala#L28) — the listener-visible superclass of `ExecutorData`, which is what reaches `SparkListenerExecutorAdded`

**Configs:** `spark.scheduler.revive.interval`, `spark.scheduler.minRegisteredResourcesRatio`, `spark.scheduler.maxRegisteredResourcesWaitingTime`, `spark.rpc.message.maxSize`

**Maps to topics:** B1, E2

---

## TaskSet submission and zombies

**What it is:** `submitTasks` wraps a `TaskSet` in a `TaskSetManager` and inserts it into the pool. Submitting a new attempt marks **all existing TSMs for that stage as zombies** — they stop launching tasks but keep accounting for those still running.

**Anchor files:**

- [TaskSchedulerImpl.scala:243](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/TaskSchedulerImpl.scala#L243) — `submitTasks`; the comment block explains the zombie corner case
- [TaskSetManager.scala:169](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/TaskSetManager.scala#L169) — the zombie definition, with a standing `TODO` that running attempts are not killed
- [TaskSchedulerImpl.scala:269](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/TaskSchedulerImpl.scala#L269) — the starvation timer warns every `spark.starvation.timeout` (15 s) while nothing launches

- [TaskSet.scala:29](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/TaskSet.scala#L29) — a `TaskSet` is an array of tasks plus stage id, attempt id and priority; its [`id`](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/TaskSet.scala#L37) is the `stageId.attemptId` string that appears in every scheduler log line
- [TaskScheduler.scala:36](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/TaskScheduler.scala#L36) — the trait `TaskSchedulerImpl` implements, and the seam an `ExternalClusterManager` replaces

**Configs:** `spark.starvation.timeout`

**Maps to topics:** B1, E1

---

## Delay scheduling and locality

**What it is:** each TSM computes which locality levels are *achievable* given live executors, then walks them most-local first, staying at a level until it runs out of tasks there or `spark.locality.wait.<level>` elapses.

**Anchor files:**

- [TaskLocality.scala:23](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/TaskLocality.scala#L23) — the five levels: `PROCESS_LOCAL, NODE_LOCAL, NO_PREF, RACK_LOCAL, ANY`
- [TaskSetManager.scala:1361](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/TaskSetManager.scala#L1361) — `computeValidLocalityLevels`: a level counts only if a pending task wants it *and* a live executor/host/rack exists
- [TaskSetManager.scala:621](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/TaskSetManager.scala#L621) — `getAllowedLocalityLevel`, the timer advance
- [TaskSetManager.scala:413](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/TaskSetManager.scala#L413) — **`NO_PREF` tasks are dequeued after `NODE_LOCAL` but reported as `PROCESS_LOCAL`**
- [TaskSchedulerImpl.scala:1172](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/TaskSchedulerImpl.scala#L1172) — base `getRacksForHosts` returns `None`, so **`RACK_LOCAL` is inert** unless a cluster-manager subclass resolves racks

!!! info "The UI's locality column can lie"

    A `NO_PREF` task — one with no preferred location at all, common for the first stage of a `parallelize` job — is reported as `PROCESS_LOCAL`. Seeing 100% `PROCESS_LOCAL` does not mean locality is working.

- [TaskSetManager.scala:1339](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/TaskSetManager.scala#L1339) — `getLocalityWait` maps each level to its own key, and returns **0 immediately** for a barrier TaskSet when `legacyResetOnTaskLaunch` is set

**Configs:** `spark.locality.wait`, `spark.locality.wait.process`, `spark.locality.wait.node`, `spark.locality.wait.rack`, `spark.locality.wait.legacyResetOnTaskLaunch`

**Maps to topics:** B1, I5, A4

---

## Slot arithmetic and resource profiles

**What it is:** how many tasks fit on an executor is **not** simply `cores / spark.task.cpus`. The limiting resource may be a custom resource (a GPU) declared in the `ResourceProfile`, and a TaskSet only sees offers from executors with a compatible profile.

**Anchor files:**

- [TaskSchedulerImpl.scala:420](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/TaskSchedulerImpl.scala#L420) — the per-offer profile gate; `taskCpus` comes from the **profile**, not the global conf
- [TaskSchedulerImpl.scala:1239](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/TaskSchedulerImpl.scala#L1239) — `calculateAvailableSlots`: limiting-resource logic and fractional task-resource amounts
- [TaskSetManager.scala:101](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/TaskSetManager.scala#L101) — slots fall back to **1** when the profile's core limit is unknown (standalone, SPARK-30417)

**Configs:** `spark.task.cpus`, `spark.executor.cores`, `spark.task.resource.*`, `spark.executor.resource.*`

**Maps to topics:** B1, E2

---

## Task execution on the executor

**What it is:** the per-task lifecycle on a pooled thread — classloader session, report RUNNING, fetch dependencies, deserialize, run under `TaskContext`, gather metrics, serialize the result.

**Anchor files:**

- [Executor.scala:307](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/executor/Executor.scala#L307) — an **unbounded cached thread pool**; nothing here caps concurrency, the driver's slot accounting does
- [Executor.scala:887](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/executor/Executor.scala#L887) — `task.run` inside `tryWithSafeFinally`; the finally releases block locks and frees task memory
- [Executor.scala:901](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/executor/Executor.scala#L901) — managed memory-leak detection: warns by default, throws only under `spark.unsafe.exceptionOnMemoryLeak`
- [Executor.scala:923](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/executor/Executor.scala#L923) — a task that **swallowed a `FetchFailedException` and then succeeded** is still reported as success, with only an error logged
- [Executor.scala:1605](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/executor/Executor.scala#L1605) — `SparkOutOfMemoryError` is explicitly **not** fatal: a task OOM fails the task; a JVM `OutOfMemoryError` kills the executor

**Configs:** `spark.task.maxDirectResultSize`, `spark.driver.maxResultSize`, `spark.executor.userClassPathFirst`, `spark.unsafe.exceptionOnMemoryLeak`

**Maps to topics:** B1, E1

---

## Task result delivery

**What it is:** a three-way decision on the executor. Above `spark.driver.maxResultSize` the result is **thrown away**; above `spark.task.maxDirectResultSize` it goes via the BlockManager; otherwise it rides the status update.

**Anchor files:**

- [Executor.scala:1001](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/executor/Executor.scala#L1001) — the three-branch decision; the oversized branch logs "dropping it" and still sends an `IndirectTaskResult` for a block **that was never written**
- [TaskResultGetter.scala:89](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/TaskResultGetter.scala#L89) — `TaskResultLost` when the block is gone between finish and fetch
- [TaskSetManager.scala:793](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/TaskSetManager.scala#L793) — `canFetchMoreResults` accumulates across tasks and **skips the check entirely for shuffle-map tasks**
- [TaskResultGetter.scala:123](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/TaskResultGetter.scala#L123) — any `NonFatal` while fetching a result aborts the whole TaskSet

!!! warning "The most confusing path in the layer"

    An oversized result is discarded on the executor, but a stub pointing at a non-existent block is still sent. The driver then fails the task as `TaskKilled("Tasks result size has exceeded maxResultSize")`. Practitioners meet this as a mysterious kill on `collect()` — the data was already thrown away before the driver ever saw it.

**Configs:** `spark.driver.maxResultSize`, `spark.task.maxDirectResultSize`, `spark.resultGetter.threads`

**Maps to topics:** B1, E1

---

## Task failure and retry

**What it is:** failures route through `TaskSetManager.handleFailedTask`, which classifies the reason and decides whether it counts toward `spark.task.maxFailures`. Several classes deliberately do not count, or do not retry at all.

**Anchor files:**

- [TaskSetManager.scala:976](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/TaskSetManager.scala#L976) — `FetchFailed` marks the task **successful** and the TSM zombie; the DAG layer owns the retry
- [TaskSetManager.scala:996](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/TaskSetManager.scala#L996) — `NotSerializableException` and `TaskOutputFileAlreadyExistException` abort immediately, no retry
- [TaskSetManager.scala:1054](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/TaskSetManager.scala#L1054) — `ExecutorLostFailure(exitCausedByApp = false)` does **not** burn the retry budget
- [TaskSetManager.scala:866](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/TaskSetManager.scala#L866) — a successful attempt resets that task's failure count to zero
- [TaskSchedulerImpl.scala:66](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/TaskSchedulerImpl.scala#L66) — class doc: exceptions thrown in `resourceOffers`/`statusUpdate` are **swallowed by the RPC framework** (SPARK-31485)

!!! info "Repeated identical failures stop printing stack traces"

    `spark.logging.exceptionPrintInterval` (10 s) suppresses duplicates, logging at INFO with a `[duplicate N]` suffix. A storm of the same error looks quieter in the logs than it is.

**Configs:** `spark.task.maxFailures`, `spark.logging.exceptionPrintInterval`

**Maps to topics:** B1, E1

---

## Speculation

**What it is:** a periodic driver thread asks each TSM whether any running task is a straggler — classically `runtime > multiplier × median` once `quantile` of tasks finished. 4.x adds an efficiency check (records/sec vs the stage average), a fixed threshold for small stages, and eager speculation on executors about to be decommissioned.

**Anchor files:**

- [TaskSetManager.scala:1300](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/TaskSetManager.scala#L1300) — no speculation for zombie or **barrier** TaskSets
- [TaskSetManager.scala:1251](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/TaskSetManager.scala#L1251) — the efficiency gate
- [TaskSetManager.scala:337](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/TaskSetManager.scala#L337) — a speculative copy never lands on a host that already ran an attempt
- [TaskSetManager.scala:851](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/TaskSetManager.scala#L851) — when one attempt wins, the others are killed and recorded in `killedByOtherAttempt`

!!! warning "Speculation duplicates side effects, not just computation"

    The losing attempt has already run. Non-idempotent user code — external writes, API calls, counters — has already taken effect by the time it is killed. Output-file commits are protected by the commit coordinator; anything you do yourself is not.

**Configs:** `spark.speculation`, `spark.speculation.interval`, `spark.speculation.multiplier`, `spark.speculation.quantile`, `spark.speculation.minTaskRuntime`, `spark.speculation.task.duration.threshold`, `spark.speculation.efficiency.enabled`, `spark.speculation.efficiency.longRunTaskFactor`, `spark.speculation.efficiency.processRateMultiplier`, `spark.executor.decommission.killInterval`

**Maps to topics:** A4, B1

---

## Executor exclusion

**What it is:** two tiers. `TaskSetExcludelist` escalates within one stage attempt: (task, executor) → (task, node) → whole executors and nodes for that stage. `HealthTracker` accumulates across the application, with expiry, and can kill or decommission — but only learns about failures **when a TaskSet completes successfully**.

**Anchor files:**

- [HealthTracker.scala:29](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/HealthTracker.scala#L29) — the design commentary, including the "does not know anything about task failures until a taskset completes successfully" constraint
- [HealthTracker.scala:220](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/HealthTracker.scala#L220) — with an external shuffle service on, **the whole node is excluded on a single fetch failure**
- [HealthTracker.scala:509](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/HealthTracker.scala#L509) — startup validation: Spark **refuses to start** when `maxTaskAttemptsPerNode >= spark.task.maxFailures` — the valid config is strictly less, or exclusion cannot survive one bad node
- [TaskSetExcludeList.scala:41](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/TaskSetExcludeList.scala#L41) — **dry-run mode**: with app-level exclusion on but task/stage-level off, failures are recorded and nothing is excluded
- [TaskSchedulerImpl.scala:659](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/TaskSchedulerImpl.scala#L659) — the starvation caveat: a steady stream of new TaskSets keeps clearing the expiry map, so the abort timer may never fire

**Configs:** `spark.excludeOnFailure.enabled`, `spark.excludeOnFailure.timeout`, `spark.excludeOnFailure.taskAndStage.enabled`, `spark.excludeOnFailure.task.maxTaskAttemptsPerExecutor`, `spark.excludeOnFailure.task.maxTaskAttemptsPerNode`, `spark.excludeOnFailure.stage.maxFailedTasksPerExecutor`, `spark.excludeOnFailure.stage.maxFailedExecutorsPerNode`, `spark.excludeOnFailure.application.enabled`, `spark.excludeOnFailure.application.maxFailedTasksPerExecutor`, `spark.excludeOnFailure.application.maxFailedExecutorsPerNode`, `spark.excludeOnFailure.application.fetchFailure.enabled`, `spark.excludeOnFailure.killExcludedExecutors`, `spark.excludeOnFailure.killExcludedExecutors.decommission`, `spark.scheduler.executorTaskExcludeOnFailureTime` (legacy, internal)

**Maps to topics:** none — proposed as E12

---

## Kill path and the TaskReaper

**What it is:** cancellation is explicitly best-effort. `TaskRunner.kill` sets a flag and optionally interrupts. With `spark.task.reaper.enabled`, a supervisor watches the killed task and, past `killTimeout`, throws an exception that takes the **whole executor JVM** down.

**Anchor files:**

- [Executor.scala:1210](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/executor/Executor.scala#L1210) — the reaper doc: best effort, zombie tasks starving slots, disabled by default
- [Executor.scala:1249](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/executor/Executor.scala#L1249) — poll loop with thread dumps, then `KilledByTaskReaperException` — except in local mode, where it only logs
- [Executor.scala:428](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/executor/Executor.scala#L428) — `killMarks` with a 10 s TTL, covering a kill that arrives before the launch
- [Executor.scala:1088](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/executor/Executor.scala#L1088) — during JVM shutdown, failures are deliberately **not** reported to the driver (SPARK-20904)

!!! warning "Uninterruptible user code turns a cancelled job into lost capacity"

    JDBC calls and native code ignore `Thread.interrupt`. Combined with speculation or job cancellation, the slots never come back — unless the reaper is enabled, whose remedy is killing the executor.

**Configs:** `spark.task.reaper.enabled`, `spark.task.reaper.killTimeout`, `spark.task.reaper.pollingInterval`, `spark.task.reaper.threadDump`

**Maps to topics:** E1

---

## Heartbeat and expiry

**What it is:** every `spark.executor.heartbeatInterval` the executor pushes accumulator updates and metric peaks. Consecutive failures past `spark.executor.heartbeat.maxFailures` make it kill itself. Independently, the driver evicts executors it has not heard from within the network timeout.

**Anchor files:**

- [Executor.scala:1519](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/executor/Executor.scala#L1519) — `reportHeartBeat`; the RPC timeout is the heartbeat interval itself, and 60 failures × 10 s = 10 minutes before self-exit
- [HeartbeatReceiver.scala:210](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/HeartbeatReceiver.scala#L210) — `expireDeadHosts` explicitly sends `RemoveExecutor` so assigned tasks fail rather than hang (SPARK-27348)
- [HeartbeatReceiver.scala:84](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/HeartbeatReceiver.scala#L84) — `require(heartbeatInterval <= executorTimeout)`

**Configs:** `spark.executor.heartbeatInterval`, `spark.executor.heartbeat.maxFailures`, `spark.executor.heartbeat.dropZeroAccumulatorUpdates`, `spark.network.timeout`

**Maps to topics:** E3, E2, B1

---

## Executor metrics polling

**What it is:** a separate poller samples memory and GC metrics on a timer or piggybacked on the heartbeat, keeping per-task and per-stage running maxima shipped as `metricPeaks`.

**Anchor files:**

- [ExecutorMetricsPoller.scala:77](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/executor/ExecutorMetricsPoller.scala#L77) — lock-free peak tracking via `getAndAccumulate(…, math.max)`
- [ExecutorMetricsPoller.scala:147](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/executor/ExecutorMetricsPoller.scala#L147) — an unknown task returns **all zeros**, so a task killed before `onTaskStart` shows zero peaks in the UI

**Configs:** `spark.executor.metrics.pollingInterval`, `spark.executor.metrics.fileSystemSchemes`, `spark.metrics.executorMetricsSource.enabled`

**Maps to topics:** E3

---

## Decommissioning

**What it is:** a graceful drain. The driver marks the executor decommissioning so it stops receiving offers, tells the BlockManager master, and notifies the executor, which stops accepting tasks, migrates blocks if configured, and exits.

**Anchor files:**

- [CoarseGrainedSchedulerBackend.scala:551](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/cluster/CoarseGrainedSchedulerBackend.scala#L551) — the driver-side sequence and the force-kill timer
- [CoarseGrainedExecutorBackend.scala:355](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/executor/CoarseGrainedExecutorBackend.scala#L355) — the shutdown thread polls every second and **loops forever** if tasks never finish or blocks never migrate
- [CoarseGrainedExecutorBackend.scala:329](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/executor/CoarseGrainedExecutorBackend.scala#L329) — decommission enabled with neither RDD nor shuffle migration logs an error and migrates nothing
- [Executor.scala:544](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/executor/Executor.scala#L544) — `decommission()` only flips a flag; a task launched afterwards is logged at ERROR but **still runs**
- [TaskSetManager.scala:1211](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/TaskSetManager.scala#L1211) — `ExecutorDecommission` sets `exitCausedByApp = false`, sparing the retry budget

- [ExecutorDecommissionInfo.scala:28](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/ExecutorDecommissionInfo.scala#L28) — `workerHost` being defined is the signal that the *whole node* is going away, not just this executor, which is what escalates shuffle-output unregistration

**Configs:** `spark.decommission.enabled`, `spark.executor.decommission.killInterval`, `spark.executor.decommission.forceKillTimeout`, `spark.executor.decommission.signal`, `spark.storage.decommission.*`

**Maps to topics:** E2

---

## Dynamic allocation

**What it is:** a timer-driven controller. Backlogged pending tasks arm an add-timer; each round it computes the target per `ResourceProfile` and grows exponentially, while removing executors idle past a timeout that also accounts for cached blocks and tracked shuffle data.

**Anchor files:**

- [ExecutorAllocationManager.scala:300](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/ExecutorAllocationManager.scala#L300) — `maxNumExecutorsNeededPerResourceProfile`, including the extra request for unschedulable TaskSets
- [ExecutorAllocationManager.scala:506](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/ExecutorAllocationManager.scala#L506) — exponential doubling capped by need and `maxExecutors`
- [ExecutorAllocationManager.scala:475](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/ExecutorAllocationManager.scala#L475) — an unreachable cluster manager **silently rolls the target back** and logs a warning
- [ExecutorMonitor.scala:559](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/dynalloc/ExecutorMonitor.scala#L559) — the effective deadline is `max(cacheTimeout, shuffleTimeout, idleTimeout)`

!!! warning "`.cache()` pins executors forever under dynamic allocation"

    `spark.dynamicAllocation.cachedExecutorIdleTimeout` defaults to `Integer.MAX_VALUE`, so an executor holding cached blocks is never reclaimed. A notebook that cached a DataFrame hours ago is still paying for those executors.

**Configs:** `spark.dynamicAllocation.enabled`, `spark.dynamicAllocation.minExecutors`, `spark.dynamicAllocation.maxExecutors`, `spark.dynamicAllocation.initialExecutors`, `spark.dynamicAllocation.executorAllocationRatio`, `spark.dynamicAllocation.executorIdleTimeout`, `spark.dynamicAllocation.cachedExecutorIdleTimeout`, `spark.dynamicAllocation.schedulerBacklogTimeout`, `spark.dynamicAllocation.sustainedSchedulerBacklogTimeout`, `spark.dynamicAllocation.shuffleTracking.enabled`, `spark.dynamicAllocation.shuffleTracking.timeout`, `spark.dynamicAllocation.testing` (internal)

**Maps to topics:** E2, A4

---

## Barrier execution

**What it is:** a barrier stage is gang-scheduled — `resourceOffers` will not launch any task unless it can place *every* task in one round. At runtime `BarrierTaskContext.barrier()` blocks on a driver-side coordinator that replies only when all tasks have arrived.

**Anchor files:**

- [TaskSchedulerImpl.scala:570](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/TaskSchedulerImpl.scala#L570) — the slot gate; `TODO SPARK-24819` notes a job needing more slots than exist is **not failed at submit — it just waits**
- [TaskSchedulerImpl.scala:677](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/TaskSchedulerImpl.scala#L677) — partial launch reverts all assigned cores and waits, logging at most once a minute
- [BarrierTaskContext.scala:88](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/BarrierTaskContext.scala#L88) — a deliberately huge 365-day RPC timeout, so the *coordinator's* own timer produces the error
- [BarrierCoordinator.scala:128](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/BarrierCoordinator.scala#L128) — the timer that fails all current requesters
- [BarrierTaskContext.scala:129](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/BarrierTaskContext.scala#L129) — the contract doc: unequal numbers of `barrier()` calls **hang the job or time out**

- [BarrierJobAllocationFailed.scala:45](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/BarrierJobAllocationFailed.scala#L45) — the refusal messages are `val`s on a companion object, so the exact strings are greppable: an unsupported RDD chain, barrier with dynamic allocation, and [not enough slots](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/BarrierJobAllocationFailed.scala#L36)

**Configs:** `spark.barrier.sync.timeout`, `spark.locality.wait.legacyResetOnTaskLaunch` (interacts destructively)

**Maps to topics:** none — proposed as E13

---

## TaskContext lifecycle

**What it is:** the user-visible cleanup hook. Listeners are stacked LIFO per thread, invoked exactly once, serialized through a single invocation thread — and a listener that throws **fails the task**, including one whose body succeeded.

**Anchor files:**

- [TaskContext.scala:116](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/TaskContext.scala#L116) — the completion-listener contract: "Exceptions thrown by the listener will result in failure of the task"
- [TaskContext.scala:150](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/TaskContext.scala#L150) — failure listeners now include completion-listener failures, a behaviour change since 3.4.0
- [TaskContextImpl.scala:222](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/TaskContextImpl.scala#L222) — the single-invocation-thread trick: a second thread calling `markTaskCompleted` returns immediately without running anything

**Configs:** none

**Maps to topics:** E1, B1

---

## Executor loss handling

**What it is:** when an executor disappears, each TSM decides whether previously-*successful* map tasks must be re-run because their shuffle output died with it.

**Anchor files:**

- [TaskSchedulerImpl.scala:1006](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/TaskSchedulerImpl.scala#L1006) — `executorLost`, the `LossReasonPending` two-phase handling and the deadlock-avoiding call outside the lock
- [TaskSetManager.scala:1150](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/TaskSetManager.scala#L1150) — re-running map outputs is skipped when shuffle storage is reliable or an external shuffle service is on
- [TaskSetManager.scala:1193](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/TaskSetManager.scala#L1193) — the un-succeed path: `successful(index) = false`, `tasksSuccessful -= 1`

**Configs:** `spark.shuffle.service.enabled`, `spark.shuffle.useOldFetchProtocol`

**Maps to topics:** B1, E1, A4

---

## Output commit coordination

**What it is:** the answer to "what stops speculation from writing the same file twice". A driver-side `OutputCommitCoordinator` hands out an exclusive commit lock per `(stage, partition)` on a **first-committer-wins** policy. Every task that is about to commit through the Hadoop commit protocol asks first; a denial raises `CommitDeniedException`, which the task layer converts to `TaskCommitDenied` — a failure that deliberately does **not** count toward `spark.task.maxFailures`.

**Code path:** `SparkHadoopMapRedUtil.commitTask` → `committer.needsTaskCommit` → `outputCommitCoordinator.canCommit` (executor → driver RPC) → `handleAskPermissionToCommit` → `performCommit()` | `abortTask` + `CommitDeniedException`. Driver side: `DAGScheduler.submitMissingTasks` → `stageStart`; `handleTaskCompletion` → `taskCompleted`; `markStageAsFinished` → `stageEnd`.

**Anchor files:**

- [OutputCommitCoordinator.scala:47](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/OutputCommitCoordinator.scala#L47) — the class doc: instantiated on driver *and* executors, executors holding only a reference to the driver's endpoint (SPARK-4879)
- [OutputCommitCoordinator.scala:95](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/OutputCommitCoordinator.scala#L95) — `canCommit`, a **blocking** ask on every committing task; with the coordinator already stopped it logs an error and returns `false`
- [OutputCommitCoordinator.scala:176](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/OutputCommitCoordinator.scala#L176) — `handleAskPermissionToCommit`: the lock is a `TaskIdentifier(stageAttempt, taskAttempt)` per partition, so the same task running in two *stage* attempts is distinguishable
- [OutputCommitCoordinator.scala:200](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/OutputCommitCoordinator.scala#L200) — no state for the stage → commit **denied**; a request that arrives after `stageEnd` can never win
- [OutputCommitCoordinator.scala:137](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/OutputCommitCoordinator.scala#L137) — `taskCompleted`: a failed attempt is recorded in `failures` and permanently barred from committing; if it *held* the lock, the lock is cleared so a later attempt can take it
- [SparkHadoopMapRedUtil.scala:78](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/mapred/SparkHadoopMapRedUtil.scala#L78) — the single caller in core, guarded by `needsTaskCommit`
- [SparkHadoopMapRedUtil.scala:72](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/mapred/SparkHadoopMapRedUtil.scala#L72) — `spark.hadoop.outputCommitCoordination.enabled`, described in the source as an undocumented escape hatch; `false` commits without asking anyone
- [CommitDeniedException.scala:25](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/executor/CommitDeniedException.scala#L25) — carries the ids needed to build `TaskCommitDenied`

!!! warning "It protects the commit protocol, not your writes"

    The coordinator sits in exactly one place: `SparkHadoopMapRedUtil.commitTask`. A task that opens a JDBC connection, posts to an API, or writes to a path itself never asks for permission, so speculation duplicates that work with nothing to stop it. This is the mechanism the [speculation](#speculation) note's "output-file commits are protected; anything you do yourself is not" refers to.

!!! info "Concurrency is not the same as speculation"

    The source comment at the escape hatch notes that two attempts of the same task can run concurrently **even with speculation off** (SPARK-8029) — a stage retry after a fetch failure is enough. Turning speculation off does not make the coordinator redundant.

**Configs:** `spark.hadoop.outputCommitCoordination.enabled` (read via `SparkConf.getBoolean`, so it is not a declared `ConfigEntry` and does not appear in the config catalog)

**Maps to topics:** none — proposed as E17

---

## Unschedulable TaskSets and the abort timer

**What it is:** the endgame of executor exclusion. When a TaskSet has a task that can run *nowhere*, `TaskSchedulerImpl` does not abort immediately — it tries to manufacture a place to run: kill an idle excluded executor, or ask `ExecutorAllocationManager` for more, and start a timer. Only when the timer expires with nothing scheduled does the stage abort.

**Code path:** `resourceOffers` → `!launchedAnyTask` → `getCompletelyExcludedTaskIfAny` → (idle excluded executor? `killExcludedIdleExecutor` : dynamic allocation? `unschedulableTaskSetAdded` : abort now) → `updateUnschedulableTaskSetTimeoutAndStartAbortTimer` → `createUnschedulableTaskSetAbortTimer` → `abortSinceCompletelyExcludedOnFailure`

**Anchor files:**

- [TaskSchedulerImpl.scala:173](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/TaskSchedulerImpl.scala#L173) — `unschedulableTaskSetToExpiryTime`, and the dedicated `task-abort-timer` thread above it
- [TaskSchedulerImpl.scala:635](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/TaskSchedulerImpl.scala#L635) — an idle excluded executor is **killed** to force a replacement
- [TaskSchedulerImpl.scala:641](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/TaskSchedulerImpl.scala#L641) — with dynamic allocation on, the driver asks for more executors instead
- [TaskSchedulerImpl.scala:655](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/TaskSchedulerImpl.scala#L655) — **without** dynamic allocation and with no idle executor to kill, the stage is aborted immediately, no timer
- [TaskSchedulerImpl.scala:666](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/TaskSchedulerImpl.scala#L666) — any launched task **clears the expiry map for every unschedulable TaskSet**, not just the one that progressed
- [TaskSchedulerImpl.scala:762](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/TaskSchedulerImpl.scala#L762) — the timeout, in seconds × 1000
- [TaskSchedulerImpl.scala:773](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/TaskSchedulerImpl.scala#L773) — the timer re-checks expiry on firing and cancels itself if the entry has gone

!!! warning "The abort timer can be starved indefinitely"

    The clear at L666 is global and unconditional. A steady stream of *other* TaskSets that do schedule keeps resetting the map, so the timer for the stuck TaskSet re-arms forever and the job neither progresses nor fails. The source flags this in a comment as theoretically possible; on a busy shared cluster it is the normal case.

!!! info "Dynamic allocation makes the abort *more* likely, not less"

    The source note is explicit: `ExecutorAllocationManager` sizes on pending tasks and does not kill on idle timeouts, so a killed idle excluded executor may not be replaced before the timer expires. Two or more idle excluded executors plus dynamic allocation is called out as the case that aborts.

**Configs:** `spark.scheduler.excludeOnFailure.unschedulableTaskSetTimeout` (120 s), `spark.dynamicAllocation.enabled`, `spark.excludeOnFailure.killExcludedExecutors`

**Maps to topics:** E12, E2

---

## Cluster-manager selection and local mode

**What it is:** which `TaskScheduler`/`SchedulerBackend` pair you get is decided by pattern-matching the master URL, and everything that is not `local*` or `spark://` is resolved through a `ServiceLoader` SPI — `ExternalClusterManager`, which is how YARN and Kubernetes plug in without core knowing about them.

**Code path:** `SparkContext` → `createTaskScheduler(master)` → regex match → `TaskSchedulerImpl` + (`LocalSchedulerBackend` | `StandaloneSchedulerBackend`) — or `getClusterManager` → `ServiceLoader.load(classOf[ExternalClusterManager])` → `canCreate` → `createTaskScheduler` / `createSchedulerBackend` / `initialize`

**Anchor files:**

- [ExternalClusterManager.scala:25](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/ExternalClusterManager.scala#L25) — the four-method SPI every non-built-in cluster manager implements
- [SparkContext.scala:3401](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/SparkContext.scala#L3401) — `getClusterManager`; **two** managers claiming one URL is a hard `SparkException`, and zero is "Could not parse Master URL"
- [SparkContext.scala:3301](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/SparkContext.scala#L3301) — `MAX_LOCAL_TASK_FAILURES = 1`
- [SparkContext.scala:3336](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/SparkContext.scala#L3336) — `local[N, M]`, the only way to get retries in local mode; `M` is passed as `maxFailures` directly
- [SparkContext.scala:3354](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/SparkContext.scala#L3354) — `local-cluster[…]` starts a real standalone cluster in-process, and forces `spark.shuffle.readHostLocalDisk` off so remote fetching is what gets exercised
- [LocalSchedulerBackend.scala:52](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/local/LocalSchedulerBackend.scala#L52) — `LocalEndpoint` holds an `Executor` directly: no serialization boundary, no RPC, no network
- [LocalSchedulerBackend.scala:174](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/local/LocalSchedulerBackend.scala#L174) — `defaultParallelism` falls back to `totalCores`, which is why `local[1]` silently makes every shuffle single-partition

!!! warning "`spark.task.maxFailures` is ignored in local mode"

    `local` and `local[N]` hard-code `MAX_LOCAL_TASK_FAILURES = 1`: the first task failure fails the job, whatever you set. Only `local[N, M]` gives retries. A flaky test that passes in CI on a cluster and fails locally — or the reverse — usually traces to this.

!!! info "Agrees with the B1 trace"

    [topics/b1.md](../topics/b1.md) traces the same dispatch from the topic side and cites the same anchors; this sweep adds the `ExternalClusterManager` duplicate-registration failure and the `local-cluster` shuffle-config override.

**Configs:** `spark.master` (the URL itself), `spark.default.parallelism`, `spark.task.cpus`

**Maps to topics:** B1, E2

---

## TaskInfo accumulable retention

**What it is:** every `TaskInfo` carries the task's accumulables, and the driver holds `TaskInfo`s for every attempt of every task in a TaskSet. On wide stages this is a real driver heap cost long after the task is done, so 4.x can strip the accumulables at completion — after handing an un-stripped copy to the DAGScheduler so listeners still see the values.

**Code path:** `handleSuccessfulTask` / `handleFailedTask` → `dropTaskInfoAccumulablesOnTaskCompletion`? → `cloneWithEmptyAccumulables` → `taskInfos(taskId) = clonedTaskInfo` → `dagScheduler.taskEnded(…, taskInfoWithAccumulables)`

**Anchor files:**

- [TaskSetManager.scala:276](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/TaskSetManager.scala#L276) — the flag, read once per TaskSet
- [TaskSetManager.scala:921](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/TaskSetManager.scala#L921) — the clone-and-swap: the **stripped** copy is retained, the **full** one is passed to `taskEnded`, so listeners are unaffected (SPARK-46383)
- [TaskSetManager.scala:817](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/TaskSetManager.scala#L817) — the already-finished path clears accumulables in place instead
- [TaskSetManager.scala:956](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/TaskSetManager.scala#L956) — same treatment on the failure side

!!! info "Anything reading `TaskInfo` *after* the fact sees empty accumulables"

    The event that reaches the listener bus is unaffected, so the UI and event logs are complete. But a `SparkListener` that stashes a `TaskInfo` reference and inspects it later, or code reaching into `TaskSetManager.taskInfos`, gets an emptied list once this is on.

- [TaskInfo.scala:78](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/TaskInfo.scala#L78) — `accumulables` is a plain `Seq[AccumulableInfo]` field on every `TaskInfo`, and [`setAccumulables`](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/TaskInfo.scala#L83) is what the retention flag clears

**Configs:** `spark.scheduler.dropTaskInfoAccumulablesOnTaskCompletion.enabled` (internal)

**Maps to topics:** E3, E1

---

## Streaming-aware scheduler logging

**What it is:** scheduler log lines carry no query context, because the streaming query's identifiers are set as *thread-local* properties on the streaming execution thread while the scheduler runs on its own threads. 4.x adds a logging trait that reads the query and batch id from the `TaskSet`'s `Properties` instead — the one carrier that crosses that thread boundary — and prefixes them onto scheduler log messages.

**Code path:** `submitTasks` → `isStreamingTaskSet` (does the TaskSet carry a query-id property?) → `streamingTaskSetManager` → an **anonymous `TaskSetManager` subclass** mixing in the trait → overridden `logInfo`/`logWarning`/`logError` → `constructStreamingLogEntry`

**Anchor files:**

- [StructuredStreamingIdAwareSchedulerLogging.scala:24](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/StructuredStreamingIdAwareSchedulerLogging.scala#L24) — the doc stating exactly why `getLocalProperty` cannot work here
- [StructuredStreamingIdAwareSchedulerLogging.scala:39](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/StructuredStreamingIdAwareSchedulerLogging.scala#L39) — the trait overrides the existing `logInfo`/`logWarning`/`logError` rather than adding new methods, so mixing it in retrofits every call site in the class
- [TaskSchedulerImpl.scala:300](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/TaskSchedulerImpl.scala#L300) — `streamingTaskSetManager`: a streaming TaskSet gets a *different*, anonymous `TaskSetManager` subclass, with `logName` forced back to `TaskSetManager` so log filters still match
- [TaskSchedulerImpl.scala:313](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/TaskSchedulerImpl.scala#L313) — `isStreamingTaskSet` is a property probe, not a config check: no query-id property, no streaming-aware manager
- [SchedulableBuilder.scala:223](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/SchedulableBuilder.scala#L223) — the pool builder calls the helper directly, so pool-assignment logs get the same prefix

!!! info "Off by default, and truncating"

    `spark.scheduler.streaming.idAwareLogging.enabled` is `false` by default; `…queryIdLength` truncates the query id in the prefix. With many concurrent queries on one session, this is the difference between attributable scheduler logs and unusable ones.

**Configs:** `spark.scheduler.streaming.idAwareLogging.enabled`, `spark.scheduler.streaming.idAwareLogging.queryIdLength`

**Maps to topics:** E3, A8

---

## The Hadoop commit protocol

**What it is:** the other half of [output commit coordination](#output-commit-coordination). The coordinator decides *who* may commit; `FileCommitProtocol` is *how* the commit happens. Tasks write to a staging directory, return a `TaskCommitMessage`, and the driver's `commitJob` promotes the staged output into the destination. The default implementation, `HadoopMapReduceCommitProtocol`, delegates to a Hadoop `OutputCommitter` and adds Spark's own handling for absolute-path outputs and dynamic partition overwrite.

**Code path:** `FileCommitProtocol.instantiate(className, jobId, outputPath, dynamicPartitionOverwrite)` → `setupJob` → per task `setupTask` → `newTaskTempFile` (into `stagingDir`) → `commitTask` → `TaskCommitMessage` → driver `commitJob` → `committer.commitJob` + rename staged files → destination

**Anchor files:**

- [FileCommitProtocol.scala:51](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/internal/io/FileCommitProtocol.scala#L51) — the abstract contract: `setupJob`/`commitJob`/`abortJob` on the driver, `setupTask`/`commitTask`/`abortTask` on the executor
- [FileCommitProtocol.scala:210](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/internal/io/FileCommitProtocol.scala#L210) — `instantiate`: reflective, trying the 3-arg constructor and falling back to 2-arg
- [FileCommitProtocol.scala:229](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/internal/io/FileCommitProtocol.scala#L229) — the fallback `require`: a custom committer without the 3-arg constructor **fails the job** when dynamic partition overwrite is on, rather than silently ignoring it
- [HadoopMapReduceCommitProtocol.scala:108](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/internal/io/HadoopMapReduceCommitProtocol.scala#L108) — `stagingDir`, derived from the output path and job id
- [HadoopMapReduceCommitProtocol.scala:127](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/internal/io/HadoopMapReduceCommitProtocol.scala#L127) — dynamic overwrite requires a partitioned write; an unpartitioned one raises
- [HadoopMapReduceCommitProtocol.scala:183](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/internal/io/HadoopMapReduceCommitProtocol.scala#L183) — `commitJob`: the Hadoop committer first, then Spark's own absolute-path renames
- [HadoopMapReduceCommitProtocol.scala:201](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/internal/io/HadoopMapReduceCommitProtocol.scala#L201) — a failed `fs.rename` throws mid-loop, **after** earlier renames have already landed
- [HadoopMapReduceCommitProtocol.scala:207](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/internal/io/HadoopMapReduceCommitProtocol.scala#L207) — dynamic overwrite deletes each destination partition directory *then* renames the staged one in

!!! warning "`commitJob` is not atomic, and the window is proportional to output size"

    The driver renames staged files one at a time. A failure partway through — or a driver killed during commit — leaves the destination with some new files and some old, and there is no rollback. Under dynamic partition overwrite it is worse: each partition is **deleted before** its replacement is renamed in, so a crash in that loop can leave a partition that exists in neither form. This is the structural reason the cloud committers exist; on a real filesystem the rename is cheap and the window is small, on an object store it is neither.

!!! info "Where the two halves meet"

    `commitTask` here is what `OutputCommitCoordinator.canCommit` gates. The coordinator guarantees exactly one attempt reaches this code for a given partition; this protocol guarantees that what that attempt wrote becomes visible atomically *per file*. Neither guarantees job-level atomicity — see the warning above. Both are traced on this page because topic **E17** needs both.

**Configs:** `spark.sql.sources.commitProtocolClass`, `spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version`, `spark.sql.sources.partitionOverwriteMode`

**Maps to topics:** E17, B4

---

## The Task object and how it reaches an executor

**What it is:** the unit the whole engine is built around, and the one class every other concept on this page hands to the next. `Task` is an abstract class with exactly two subclasses — `ShuffleMapTask`, which writes shuffle output and returns a `MapStatus`, and `ResultTask`, which runs the user function and returns its value. Neither carries the RDD: the RDD and the closure travel separately as a **broadcast `taskBinary`**, deserialized on the executor at the top of `runTask`. What the driver actually ships per task is a `TaskDescription` — a hand-rolled binary encoding, not Java serialization.

**Code path:** `DAGScheduler.submitMissingTasks` (broadcasts `taskBinary`) → `new ShuffleMapTask`/`new ResultTask` → `TaskSetManager.resourceOffer` → `TaskDescription.encode` → `LaunchTask` → executor → `TaskDescription.decode` → `TaskRunner.run` → `Task.run` → `runTask`

**Anchor files:**

- [Task.scala:61](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/Task.scala#L61) — the abstract class and its parameter list: `localProperties`, `serializedTaskMetrics`, `jobId`, `appId`, `isBarrier`
- [Task.scala:87](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/Task.scala#L87) — `run` is `final`: it builds the `TaskContextImpl`, registers with the block manager, sets the `CallerContext`, and calls plugins, then delegates the *only* overridable part to `runTask`
- [Task.scala:119](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/Task.scala#L119) — `isBarrier` decides `BarrierTaskContext` vs `TaskContextImpl`, per stage rather than per partition (there is a `TODO` for SPARK-24874 admitting this)
- [Task.scala:178](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/Task.scala#L178) — `def runTask(context: TaskContext): T`, the whole subclass contract
- [Task.scala:213](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/Task.scala#L213) — `collectAccumulatorUpdates`: internal metric accumulators **always** count failed values, external ones only if `countFailedValues`
- [ShuffleMapTask.scala:82](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/ShuffleMapTask.scala#L82) — `runTask` deserializes `(RDD, ShuffleDependency)` from the broadcast and returns a `MapStatus`
- [ResultTask.scala:93](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/ResultTask.scala#L93) — the entire result path is one line: `func(context, rdd.iterator(partition, context))`
- [ResultTask.scala:88](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/ResultTask.scala#L88) — the deserialization of that broadcast is what `executorDeserializeTime` measures, which is why a fat closure shows up as deserialize time, not run time
- [TaskDescription.scala:49](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/TaskDescription.scala#L49) — what is actually sent per launch: ids, artifacts, `properties`, cpus, resources, and the serialized task
- [TaskDescription.scala:92](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/TaskDescription.scala#L92) — `encode`, a manual `DataOutputStream` layout, mirrored by [`decode`](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/TaskDescription.scala#L200)
- [TaskDescription.scala:39](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/TaskDescription.scala#L39) — the comment explaining *why* it is hand-encoded: the executor must add jars to the classpath and set the properties **before** it can deserialize the task itself

!!! info "Thread-local properties are per-task cargo"

    `spark.scheduler.pool`, the job group, the job description and the SQL execution id all reach the executor as the `Properties` map on the `TaskDescription`, snapshotted on the driver at submit time. That is the only channel — a thread local set after submission does not reach the task, and this is exactly the boundary [streaming-aware scheduler logging](#streaming-aware-scheduler-logging) has to work around.

**Configs:** none read here; `spark.rpc.message.maxSize` bounds the encoded `TaskDescription`, and the `taskBinary` broadcast is what keeps the closure out of it

**Maps to topics:** B1, E1

---

## Task metrics and the accumulator pipeline

**What it is:** every number the UI shows for a task — bytes read, shuffle fetch wait, GC time, peak execution memory — is a **`LongAccumulator` on `TaskMetrics`**, not a special-cased field. `TaskMetrics` is created on the driver, serialized into the `Task`, mutated on the executor, and merged back through two independent channels: partial values on each heartbeat, final values on task completion. User accumulators ride the same pipeline as `_externalAccums`.

**Code path:** driver `TaskMetrics.registerAccumulators` → serialized into `Task` → executor mutates via `TaskContext.taskMetrics()` → *(a)* `Executor.reportHeartBeat` every `spark.executor.heartbeatInterval` → `HeartbeatReceiver` → `DAGScheduler.executorHeartbeatReceived`, or *(b)* `Task.collectAccumulatorUpdates` → `DirectTaskResult` → `DAGScheduler.updateAccumulators` → `SparkListenerTaskEnd`

**Anchor files:**

- [TaskMetrics.scala:47](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/executor/TaskMetrics.scala#L47) — the field list is a wall of `new LongAccumulator`: run time, CPU time, GC time, result size, spill, peak on/off-heap execution memory
- [TaskMetrics.scala:236](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/executor/TaskMetrics.scala#L236) — `nameToAccums`, the `LinkedHashMap` that gives each internal accumulator its `internal.metrics.*` name; this naming *is* the wire format
- [TaskMetrics.scala:215](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/executor/TaskMetrics.scala#L215) — `createTempShuffleReadMetrics`: each shuffle dependency gets its own temp object, because a task can read several shuffles at once
- [TaskMetrics.scala:225](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/executor/TaskMetrics.scala#L225) — `mergeShuffleReadMetrics` folds the temps into the reported one; **until it is called the shuffle-read numbers are zero**, which is why the heartbeat path calls it explicitly
- [TaskMetrics.scala:292](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/executor/TaskMetrics.scala#L292) — `_externalAccums`: user accumulators and `SQLMetrics` live in a separate buffer guarded by a read/write lock
- [AccumulableInfo.scala:41](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/AccumulableInfo.scala#L41) — the driver-side, listener-visible view, carrying `internal` and `countFailedValues`
- [InputMetrics.scala:42](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/executor/InputMetrics.scala#L42) / [OutputMetrics.scala](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/executor/OutputMetrics.scala) — two accumulators each: bytes and records
- [ShuffleReadMetrics.scala:31](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/executor/ShuffleReadMetrics.scala#L31) — remote vs local blocks, `remoteBytesReadToDisk`, and `fetchWaitTime`, the number that actually tells you a shuffle is the bottleneck
- [ShuffleWriteMetrics.scala](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/executor/ShuffleWriteMetrics.scala) — bytes, records, write time
- [Executor.scala:1532](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/executor/Executor.scala#L1532) — the heartbeat loop merges shuffle-read metrics and sets GC time for *every running task*, then ships partial accumulator values
- [Executor.scala:1535](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/executor/Executor.scala#L1535) — `spark.executor.heartbeat.dropZeroAccumulatorUpdates` filters zero-valued accumulators out of the heartbeat, and `excludeFromHeartbeat` drops more
- [TaskResult.scala:40](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/TaskResult.scala#L40) — `DirectTaskResult` carries the value, `accumUpdates` and `metricPeaks` together; the accumulator half survives even when the value is spilled to the block manager as an [`IndirectTaskResult`](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/TaskResult.scala#L36)

!!! warning "A user accumulator counts speculative and retried attempts unless you opt out"

    Internal metric accumulators are defined to count failed values. A user accumulator is merged on task *success* only — but speculation means two attempts can both succeed for the same partition on different stage attempts, and a stage retry re-runs partitions whose accumulator contributions were already merged. Accumulators are exact only inside an action that runs once; anything else makes them a lower bound. The `countFailedValues` flag decides whether failures count too, and it is not settable from the public `AccumulatorV2` API.

!!! info "Two channels, two truths"

    The heartbeat channel is what makes the UI's numbers move *while* a task runs; the task-end channel is what makes them final. They can disagree, and a task killed mid-flight leaves only whatever the last heartbeat carried. `spark.executor.heartbeat.dropZeroAccumulatorUpdates` therefore changes what a live UI shows without changing any final number.

**Configs:** `spark.executor.heartbeatInterval`, `spark.executor.heartbeat.dropZeroAccumulatorUpdates`, `spark.executor.heartbeat.maxFailures`, `spark.task.maxDirectResultSize`

**Maps to topics:** none yet — see the `propose:` block for **E49**

---

## Executor memory metrics and the procfs process tree

**What it is:** the second, task-independent metric channel. `ExecutorMetrics` is a flat `Array[Long]` of *peak* values — JVM heap and off-heap, on/off-heap execution and storage memory, direct and mapped pool memory, and optionally the whole process tree's RSS. Peaks are kept by `compareAndUpdatePeakValues`, not by sampling into a time series, so what reaches the driver is "the worst this executor ever got", per stage.

**Code path:** `ExecutorMetricsPoller.poll` (every `spark.executor.metrics.pollingInterval`) → `ExecutorMetrics.getCurrentMetrics` → `ExecutorMetricType.metricGetters` (JVM beans, `MemoryManager`, optionally `ProcfsMetricsGetter`) → `compareAndUpdatePeakValues` → heartbeat → `SparkListenerExecutorMetricsUpdate`; separately `ExecutorMetricsSource` exposes the same snapshot to the metrics sinks

**Anchor files:**

- [ExecutorMetrics.scala:32](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/executor/ExecutorMetrics.scala#L32) — the whole class is an `Array[Long]` indexed by metric type; there are no named fields
- [ExecutorMetrics.scala:78](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/executor/ExecutorMetrics.scala#L78) — `compareAndUpdatePeakValues` returns whether anything moved, so an unchanged executor sends no update
- [ProcfsMetricsGetter.scala:44](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/executor/ProcfsMetricsGetter.scala#L44) — reads `/proc/<pid>/stat` for the executor **and every child process**, which is how PySpark worker memory becomes visible at all
- [ProcfsMetricsGetter.scala:51](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/executor/ProcfsMetricsGetter.scala#L51) — `isProcfsAvailable`: a non-Linux host, or a missing `/proc`, disables it silently
- [ProcfsMetricsGetter.scala:79](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/executor/ProcfsMetricsGetter.scala#L79) — one failed read flips `isAvailable` to `false` **permanently** for that executor; the metric does not come back
- [ProcfsMetricsGetter.scala:108](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/executor/ProcfsMetricsGetter.scala#L108) — RSS is field 23 of `/proc/<pid>/stat` multiplied by the page size, obtained by shelling out to `getconf PAGESIZE`
- [ExecutorMetricsSource.scala:37](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/executor/ExecutorMetricsSource.scala#L37) — a `@volatile` snapshot array republished to Dropwizard gauges, so sink scrapes and heartbeats read the same numbers
- [ExecutorSource.scala:30](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/executor/ExecutorSource.scala#L30) — the *other* executor source: thread-pool gauges, `METRIC_CPU_TIME`, and per-filesystem-scheme counters
- [ExecutorSource.scala:77](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/executor/ExecutorSource.scala#L77) — `spark.executor.metrics.fileSystemSchemes` decides which schemes get `read_bytes`/`write_ops` counters; a scheme not listed is simply absent, not zero

!!! warning "`spark.executor.processTreeMetrics.enabled` is off by default, and silently no-ops off Linux"

    Without it, an executor's reported memory excludes every child process — which on PySpark is where most of the memory actually is. With it on, a container killed for exceeding its memory limit still shows JVM-only peaks in the UI unless the poll happened to land before the kill.

**Configs:** `spark.executor.metrics.pollingInterval`, `spark.executor.processTreeMetrics.enabled`, `spark.executor.metrics.fileSystemSchemes`, `spark.metrics.executorMetricsSource.enabled`

**Maps to topics:** E3

---

## The driver↔executor message protocol

**What it is:** the wire between `CoarseGrainedSchedulerBackend` (driver) and `CoarseGrainedExecutorBackend` (executor) is a single sealed trait of case classes. Every scheduling decision on this page ultimately becomes one of them. The driver keeps an `ExecutorData` per live executor — free cores, resources, log URLs, registration timestamp — and that map *is* the cluster state the offer loop reads.

**Code path:** executor start → `RetrieveSparkAppConfig` → `SparkAppConfig` → `RegisterExecutor` → driver adds `ExecutorData` → `LaunchedExecutor` → `makeOffers` → `LaunchTask` → executor → `StatusUpdate` → driver frees cores → `makeOffers(executorId)`

**Anchor files:**

- [CoarseGrainedClusterMessage.scala:30](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/cluster/CoarseGrainedClusterMessage.scala#L30) — the complete protocol in one object: `LaunchTask`, `KillTask`, `StatusUpdate`, `RegisterExecutor`, `RemoveExecutor`, `ExecutorDecommissioning`, `Shutdown`, `TaskThreadDump`
- [CoarseGrainedClusterMessage.scala:45](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/cluster/CoarseGrainedClusterMessage.scala#L45) — `LaunchTask` carries a `SerializableBuffer`: one already-encoded `TaskDescription`, one message per task
- [CoarseGrainedClusterMessage.scala:99](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/cluster/CoarseGrainedClusterMessage.scala#L99) — `ShufflePushCompletion` rides the same channel, which is how push-based shuffle reports back
- [CoarseGrainedSchedulerBackend.scala:168](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/cluster/CoarseGrainedSchedulerBackend.scala#L168) — `StatusUpdate` is the only thing that returns cores to the pool, and it immediately re-offers **that one executor**
- [CoarseGrainedSchedulerBackend.scala:160](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/cluster/CoarseGrainedSchedulerBackend.scala#L160) — the revive thread: `spark.scheduler.revive.interval`, defaulting to **1000 ms** in code, is the safety net for offers that a status update did not trigger
- [CoarseGrainedSchedulerBackend.scala:115](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/cluster/CoarseGrainedSchedulerBackend.scala#L115) — a bounded cache of decommission requests for executors that have **not registered yet**; `spark.scheduler.maxRetainedUnknownDecommissionExecutors` defaults to `0`, so by default such a request is dropped
- [CoarseGrainedSchedulerBackend.scala:702](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/cluster/CoarseGrainedSchedulerBackend.scala#L702) — `sufficientResourcesRegistered` is `true` in the base class; only a cluster-manager subclass makes `minRegisteredResourcesRatio` mean anything
- [ExecutorData.scala:36](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/cluster/ExecutorData.scala#L36) — `freeCores` is mutable and driver-owned; the executor never reports it
- [ExecutorBackend.scala:27](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/executor/ExecutorBackend.scala#L27) — the executor's whole outbound interface is one method, `statusUpdate`
- [SchedulerBackend.scala:29](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/SchedulerBackend.scala#L29) — the pluggable side: `defaultParallelism`, `killTask`, [`maxNumConcurrentTasks`](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/SchedulerBackend.scala#L99) — the last is what barrier stages check before they will schedule
- [SchedulerBackendUtils.scala:31](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/cluster/SchedulerBackendUtils.scala#L31) — where `spark.executor.instances` is finally read, and the `require` that rejects an initial executor count outside `[min, max]` when dynamic allocation is on
- [MiscellaneousProcessDetails.scala:28](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/MiscellaneousProcessDetails.scala#L28) — the shape behind `MiscellaneousProcessAdded`: a non-executor process (the YARN AM, a Connect server) registering itself so the UI can show its logs
- [SupportsDelegationToken.scala:28](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/SupportsDelegationToken.scala#L28) — the opt-in trait a backend implements to receive `UpdateDelegationTokens`; a backend without it simply never sees renewed Hadoop tokens
- [ExecutorLogUrlHandler.scala:27](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/executor/ExecutorLogUrlHandler.scala#L27) — `spark.ui.custom.executor.log.url` templating, using `{{ATTRIBUTE}}` placeholders
- [ExecutorLogUrlHandler.scala:85](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/executor/ExecutorLogUrlHandler.scala#L85) — a pattern naming an attribute the cluster manager did not supply logs **once** and then falls back to the original URLs forever

!!! info "`spark.executor.instances` is not read by the scheduler"

    It reaches the cluster manager through `SchedulerBackendUtils.getInitialTargetExecutorNumber`, and only when dynamic allocation is off. With dynamic allocation on it is ignored entirely in favour of the min/initial/max triple — setting both is the classic way to be surprised by the executor count.

**Configs:** `spark.executor.instances`, `spark.scheduler.revive.interval`, `spark.scheduler.minRegisteredResourcesRatio`, `spark.scheduler.maxRegisteredResourcesWaitingTime`, `spark.scheduler.maxRetainedUnknownDecommissionExecutors`, `spark.ui.custom.executor.log.url`

**Maps to topics:** E1, E2

---

## Executor class loading and session isolation

**What it is:** the executor does not have one classpath. It builds a `MutableURLClassLoader` per *job artifact state*, optionally wraps it in an `ExecutorClassLoader` that fetches REPL-defined classes over RPC, and caches one such `IsolatedSessionState` per Spark Connect session in a Guava LRU. `spark.executor.userClassPathFirst` inverts the delegation order in both the class and the resource path.

**Code path:** `Executor.createClassLoader` → `MutableURLClassLoader` (or `ChildFirstURLClassLoader`) → `addReplClassLoaderIfNeeded` → `ExecutorClassLoader(uri, parent, userClassPathFirst)` → per task, `isolatedSessionCache.get(uuid)` → `Thread.setContextClassLoader(state.replClassLoader)`

**Anchor files:**

- [Executor.scala:350](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/executor/Executor.scala#L350) — `userClassPathFirst`, read once per executor
- [Executor.scala:392](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/executor/Executor.scala#L392) — `isolatedSessionCache`, a Guava LRU sized by `spark.executor.isolatedSessionCache.size`
- [Executor.scala:809](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/executor/Executor.scala#L809) — every task resolves its session state from `taskDescription.artifacts.state` before it runs
- [Executor.scala:827](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/executor/Executor.scala#L827) — the context class loader is swapped **per task**, not per executor
- [Executor.scala:1381](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/executor/Executor.scala#L1381) — `createClassLoaderWithStub`: Connect installs stub classes for `spark.connect.scalaUdf.stubPrefixes` so a missing client-side class fails at *use* rather than at deserialization
- [Executor.scala:1407](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/executor/Executor.scala#L1407) — `addReplClassLoaderIfNeeded`, only when a REPL class URI was set
- [ExecutorClassLoader.scala:50](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/executor/ExecutorClassLoader.scala#L50) — extends `ClassLoader(null)`: **no parent**, delegation is done by hand through `parentLoader`
- [ExecutorClassLoader.scala:61](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/executor/ExecutorClassLoader.scala#L61) — `fetchFn` switches on the URI scheme: `spark://` fetches class bytes over the RPC env, anything else reads from a filesystem
- [ExecutorClassLoader.scala:100](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/executor/ExecutorClassLoader.scala#L100) — `findClass`, where `userClassPathFirst` flips local-first and parent-first
- [Executor.scala:355](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/executor/Executor.scala#L355) — `spark.executor.killOnFatalError.depth`: how far down the `getCause` chain a fatal error is looked for before the executor kills itself

!!! warning "`userClassPathFirst` changes resource lookup too, not just classes"

    [`getResourceAsStream`](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/executor/ExecutorClassLoader.scala#L77) honours the same flag. A jar shipped to shade a dependency conflict will also start winning `META-INF/services` and `log4j2.properties` lookups, which is why turning it on to fix one `NoSuchMethodError` sometimes silently reconfigures logging.

!!! info "This is the executor half of Connect's multi-tenancy"

    One executor process serves many Connect sessions, each with its own jars. The isolation is the LRU cache — evicting a session's state releases its class loader, so a session that goes idle past the cache size pays a full artifact re-resolution on its next task.

**Configs:** `spark.executor.userClassPathFirst`, `spark.executor.isolatedSessionCache.size`, `spark.executor.killOnFatalError.depth`, `spark.executor.extraClassPath`, `spark.executor.defaultExtraClassPath`, `spark.connect.scalaUdf.stubPrefixes`

**Maps to topics:** none yet — see the `propose:` block for **E50**

---

## The RDD write path and the Hadoop output formats

**What it is:** the other caller of [the Hadoop commit protocol](#the-hadoop-commit-protocol). `saveAsHadoopFile`/`saveAsNewAPIHadoopFile` do not go through Spark SQL at all — `SparkHadoopWriter.write` runs its own job, and `HadoopWriteConfigUtil` is the shim that lets one code path serve both the `mapred` and `mapreduce` APIs.

**Code path:** `PairRDDFunctions.saveAsHadoopFile` → `SparkHadoopWriter.write` → `config.createCommitter` → `committer.setupJob` → `sparkContext.runJob(executeTask)` → per task `initWriter` → `write(pair)` per record → `committer.commitTask` → driver `committer.commitJob`

**Anchor files:**

- [SparkHadoopWriter.scala:60](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/internal/io/SparkHadoopWriter.scala#L60) — `write` is a *job runner*: it sets up the committer, calls `runJob`, and commits — the RDD action is inside this method
- [SparkHadoopWriter.scala:68](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/internal/io/SparkHadoopWriter.scala#L68) — `jobTrackerId` is derived from the current `Date`, which is what makes staging paths unique between runs
- [SparkHadoopWriter.scala:103](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/internal/io/SparkHadoopWriter.scala#L103) — `commitJob` is timed and logged; [line 109](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/internal/io/SparkHadoopWriter.scala#L109) is the `abortJob` on any failure
- [SparkHadoopWriter.scala:150](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/internal/io/SparkHadoopWriter.scala#L150) — the per-task `commitTask`, and the [`abortTask`](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/internal/io/SparkHadoopWriter.scala#L156) in the failure branch
- [HadoopWriteConfigUtil.scala:38](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/internal/io/HadoopWriteConfigUtil.scala#L38) — the abstraction: `createJobContext`, `initWriter`, `write`, `closeWriter`, `initOutputFormat`, `assertConf`
- [HadoopWriteConfigUtil.scala:188](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/internal/io/HadoopWriteConfigUtil.scala#L188) — the old `mapred` implementation, which tags tasks as `TaskType.MAP`
- [HadoopWriteConfigUtil.scala:325](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/internal/io/HadoopWriteConfigUtil.scala#L325) — the new `mapreduce` implementation, which uses `TaskType.REDUCE` for task attempts
- [HadoopMapRedCommitProtocol.scala:31](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/internal/io/HadoopMapRedCommitProtocol.scala#L31) — the `mapred` committer subclass, which exists only to pull the committer off the old `JobConf`
- [SparkHadoopWriterUtils.scala:40](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/internal/io/SparkHadoopWriterUtils.scala#L40) — `RECORDS_BETWEEN_BYTES_WRITTEN_METRIC_UPDATES = 256`, hard-coded
- [SparkHadoopWriterUtils.scala:121](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/internal/io/SparkHadoopWriterUtils.scala#L121) — bytes-written is therefore only refreshed every 256 records; record counts are exact, byte counts are sampled
- [SparkHadoopWriterUtils.scala:103](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/internal/io/SparkHadoopWriterUtils.scala#L103) — `isOutputSpecValidationEnabled`, the `spark.hadoop.validateOutputSpecs` escape hatch that lets a write proceed into an existing directory

!!! warning "The RDD write path reaches commit coordination by a different route"

    A DataFrame write is planned by `sql/core`; an RDD write is not planned at all — `SparkHadoopWriter` submits its own job directly. Both end at `FileCommitProtocol`, so both are protected by `OutputCommitCoordinator`, but only the SQL path honours `spark.sql.sources.commitProtocolClass`. An RDD write always gets `HadoopMapReduceCommitProtocol` or the `mapred` subclass.

**Configs:** `spark.hadoop.validateOutputSpecs`, `spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version`, `spark.speculation` (via the coordinator)

**Maps to topics:** E17, B4, I4

---

## The Schedulable tree

**What it is:** the data structure under [scheduling mode and pools](#scheduling-mode-and-pools). `Schedulable` is a two-implementation trait — `Pool` and `TaskSetManager` — so the FIFO case is genuinely the same code as FAIR with a one-level tree. Ordering is recomputed on every offer round by re-sorting the queue, not maintained incrementally.

**Anchor files:**

- [Schedulable.scala:30](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/Schedulable.scala#L30) — the trait: `weight`, `minShare`, `priority`, `stageId`, `runningTasks`, and a recursive `getSortedTaskSetQueue`
- [Pool.scala:31](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/Pool.scala#L31) — a pool holds a `ConcurrentLinkedQueue[Schedulable]` and its own `schedulingMode`, so pools can nest with different policies
- [Pool.scala:105](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/Pool.scala#L105) — `getSortedTaskSetQueue` sorts the queue **on every call**, then filters out non-schedulable TaskSets; this runs once per offer round
- [Pool.scala:90](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/Pool.scala#L90) — executor loss and decommission are broadcast down the tree to every child
- [SchedulingMode.scala:25](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/SchedulingMode.scala#L25) — three values, and `NONE` is what a `TaskSetManager` reports since it has no sub-queues

!!! info "`minShare` is a claim on *slots*, not a guarantee"

    The FAIR comparator prefers a pool running below its `minShare`, but nothing preempts a running task to get there. A pool whose minShare exceeds what the cluster can supply simply stays "needy" forever and wins every comparison — which looks like the other pools being starved by a misconfiguration rather than by contention.

**Configs:** `spark.scheduler.mode`, `spark.scheduler.allocation.file`, `spark.scheduler.pool` (local property)

**Maps to topics:** B1, E1

---

## Executor loss reasons and exit codes

**What it is:** the taxonomy that decides whether losing an executor counts against the application. Every loss carries an `ExecutorLossReason` whose `exitCausedByApp` flag routes it: app-caused losses count toward failure limits and can trigger exclusion, infrastructure losses do not. The executor's own exit codes are a parallel, smaller vocabulary read from the process exit status when no reason was reported.

**Anchor files:**

- [ExecutorLossReason.scala:26](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/ExecutorLossReason.scala#L26) — the base class is just a message; the meaning is in the subclasses
- [ExecutorLossReason.scala:31](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/ExecutorLossReason.scala#L31) — `ExecutorExited(exitCode, exitCausedByApp, reason)`: the flag is a **constructor argument**, so the cluster-manager integration decides it, not core
- [ExecutorLossReason.scala:66](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/ExecutorLossReason.scala#L66) — `ExecutorProcessLost`, the "we do not know why" case, which defaults to *not* app-caused
- [ExecutorLossReason.scala:82](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/ExecutorLossReason.scala#L82) — `ExecutorDecommission`, carrying the worker's own reason, so a planned removal is distinguishable from a crash
- [ExecutorExitCode.scala:46](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/executor/ExecutorExitCode.scala#L46) — `HEARTBEAT_FAILURE = 56`: an executor that cannot reach the driver kills *itself*
- [ExecutorExitCode.scala:50](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/executor/ExecutorExitCode.scala#L50) — `KILLED_BY_TASK_REAPER = 57`, the exit the [task reaper](#kill-path-and-the-taskreaper) forces
- [ExecutorExitCode.scala:34](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/executor/ExecutorExitCode.scala#L34) — `DISK_STORE_FAILED_TO_CREATE_DIR = 53`, the one people actually hit: a bad `spark.local.dir` kills executors at startup with no task ever having run
- [ExecutorExitCode.scala:55](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/executor/ExecutorExitCode.scala#L55) — `explainExitCode`, which is what turns those numbers into the driver-log sentence
- [ExecutorFailuresInTaskSet.scala:25](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/ExecutorFailuresInTaskSet.scala#L25) — per-executor, per-TaskSet failure counts with timestamps, the accounting [executor exclusion](#executor-exclusion) escalates on

!!! warning "`exitCausedByApp` is set by the cluster manager, so the same failure counts differently on YARN and Kubernetes"

    Core never inspects the exit code to decide blame; it trusts the flag. A container OOM-killed by the node reports differently depending on which resource manager surfaced it, which is why `spark.executor.maxNumFailures` behaves differently across deployments for what looks like the same crash.

**Configs:** `spark.executor.maxNumFailures` and `spark.executor.failuresValidityInterval` (read in `deploy/ExecutorFailureTracker`, outside this group's scope), `spark.excludeOnFailure.*`

**Maps to topics:** E2, A13

---

## Fractional resource allocation

**What it is:** `spark.task.resource.gpu.amount` can be less than one, so a GPU address is not simply held or free — it holds an integer count out of `ONE_ENTIRE_RESOURCE`. `ExecutorResourcesAmounts` is the driver-side ledger doing that arithmetic per executor, and the reason fractional GPU scheduling can silently place fewer tasks than the core count suggests.

**Anchor files:**

- [ExecutorResourcesAmounts.scala:41](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/ExecutorResourcesAmounts.scala#L41) — the internal representation is `Map[resourceName, Map[address, Long]]`, amounts scaled to `ONE_ENTIRE_RESOURCE`
- [ExecutorResourcesAmounts.scala:81](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/ExecutorResourcesAmounts.scala#L81) — `acquire`, and its [`release`](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/ExecutorResourcesAmounts.scala#L105) counterpart, which [throws](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/ExecutorResourcesAmounts.scala#L114) if a release would push an address above one whole unit — a double-release is a hard error, not a leak
- [ExecutorResourcesAmounts.scala:143](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/ExecutorResourcesAmounts.scala#L143) — `assignAddressesCustomResources`: assignment is all-or-nothing per task, and returns `None` rather than a partial allocation
- [ExecutorResourcesAmounts.scala:173](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/ExecutorResourcesAmounts.scala#L173) — a whole-number request takes `ONE_ENTIRE_RESOURCE` per address, so integer and fractional requests share one code path
- [ExecutorResourceInfo.scala:29](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/ExecutorResourceInfo.scala#L29) — the per-executor view the backend keeps, mixing in `ResourceAllocator` from the `resource/` package

!!! info "Two ledgers, one truth"

    `ExecutorResourceInfo` (per executor, held by the backend) and `ExecutorResourcesAmounts` (per offer round, held by the scheduler) both track availability. The offer-round copy is the one tasks are matched against; the backend copy is the one that survives between rounds. Divergence between them is what the `release` guard is defending.

**Configs:** `spark.task.resource.*`, `spark.executor.resource.*`, `spark.task.cpus`, `spark.executor.cores`

**Maps to topics:** E2, B1

---

## The DAGScheduler event loop

**What it is:** `DAGScheduler` is not called directly by anything that matters — every interaction is a message posted to a single-threaded event loop. That serialization is what makes the scheduler's mutable state (`runningStages`, `waitingStages`, `failedStages`, `jobIdToActiveJob`) safe without locks, and it is also why one slow handler stalls all scheduling.

**Anchor files:**

- [DAGSchedulerEvent.scala:37](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/DAGSchedulerEvent.scala#L37) — `JobSubmitted`, the entry point every action funnels into
- [DAGSchedulerEvent.scala:89](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/DAGSchedulerEvent.scala#L89) — `CompletionEvent`, the single message carrying task success, `FetchFailed`, accumulator updates and metric peaks
- [DAGSchedulerEvent.scala:114](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/DAGSchedulerEvent.scala#L114) — `ResubmitFailedStages`, posted on a delay of [`RESUBMIT_TIMEOUT = 200` ms](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala#L3624) so a burst of fetch failures coalesces into one stage retry
- [DAGSchedulerEvent.scala:74](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/DAGSchedulerEvent.scala#L74) — `JobTagCancelled`, the 4.x tag-based cancellation channel alongside `JobGroupCancelled`
- [ActiveJob.scala:45](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/ActiveJob.scala#L45) — an active job is a final stage plus a `finished` boolean array; job completion is `numFinished == numPartitions`, per *partition of the final stage*
- [ActiveJob.scala:57](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/ActiveJob.scala#L57) — for a `ResultStage` the partition count comes from the job's requested partitions, not the RDD's, which is how `take` submits a job over a subset
- [JobWaiter.scala](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/JobWaiter.scala) implements [`JobListener`](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/JobListener.scala) — `taskSucceeded`/`jobFailed`, the two-method interface that bridges the event loop back to the blocking caller
- [DAGSchedulerSource.scala:50](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/DAGSchedulerSource.scala#L50) — `messageProcessingTime`, a timer over the event loop itself: the metric that tells you the scheduler, not the cluster, is the bottleneck
- [DAGSchedulerSource.scala:29](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/DAGSchedulerSource.scala#L29) — gauges for failed/running/waiting stages and active jobs, read straight off the scheduler's mutable collections

!!! warning "One thread, and `messageProcessingTime` is the only warning you get"

    Deep lineages make `submitMissingTasks` expensive, and it runs on the event-loop thread. While it runs, no task completion is processed and no stage is submitted, so a cluster that looks idle can be waiting on the driver. The gauge exists precisely because this is invisible in the UI's job timeline.

**Configs:** `spark.scheduler.numCancelledJobGroupsToTrack`, `spark.metrics.*` (for the source to be exported)

**Maps to topics:** E1, B1

---

## Preferred locations from Hadoop input formats

**What it is:** the *other* source of preferred locations, distinct from the RDD-level `getPreferredLocations` the [partition selection](#partition-selection-and-preferred-locations) concept traces. `InputFormatInfo` asks a Hadoop `InputFormat` for its splits **before any job runs**, so a `SparkContext` can be given locality hints at construction time and the cluster manager can request executors on the right hosts.

**Anchor files:**

- [InputFormatInfo.scala:38](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/InputFormatInfo.scala#L38) — takes a `Configuration` and an `InputFormat` class, and instantiates it reflectively
- [InputFormatInfo.scala:96](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/InputFormatInfo.scala#L96) — `prefLocsFromMapreduceInputFormat`, with a [`mapred` twin](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/InputFormatInfo.scala#L137) selected by which API the class implements
- [InputFormatInfo.scala:168](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/InputFormatInfo.scala#L168) — `computePreferredLocations` inverts split→hosts into host→splits, the shape the cluster manager wants
- [SplitInfo.scala:27](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/SplitInfo.scala#L27) — one host, one path, one length; equality is by value so duplicate splits collapse
- [TaskLocation.scala](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/TaskLocation.scala) — the runtime counterpart: `ExecutorCacheTaskLocation`, `HostTaskLocation`, `HDFSCacheTaskLocation`, encoded as strings with `executor_` and `hdfs_cache_` prefixes

!!! info "Effectively legacy, and it shows"

    This path predates dynamic allocation and is reached only through the `SparkContext` constructor overload taking `preferredNodeLocationData`. It does full split computation on the driver before the application starts. Nothing in the DataFrame API uses it — `sql/core` does its own driver-side file listing — but it is still live code and still the reason `SplitInfo` exists.

**Configs:** none; the `Configuration` is supplied by the caller

**Maps to topics:** I5, A4

---

## Breadth check 1 — the config slice

The slice is every `core` config matching this pattern, run against `configs/catalog.yaml`:

```bash
PYTHONIOENCODING=utf-8 python -c "
import yaml, re
d = yaml.safe_load(open('docs/reference/spark-source-map/configs/catalog.yaml', encoding='utf-8'))
cs = [c for c in d['configs'] if c['subsystem'] == 'core']
pat = re.compile(r'^spark\.(scheduler\.|task\.|speculation|excludeOnFailure\.|blacklist\.|dynamicAllocation\.|stage\.|barrier\.|locality\.|executor\.|resources\.|files\.fetchFailure|job\.)')
sel = sorted({c['key'] for c in cs if pat.search(c['key'])})
print(len(sel)); [print(k) for k in sel]
"
```

**111 keys. 86 are attributed to a concept on this page; 25 belong to other groups.**

| Keys | Concept |
|---|---|
| `spark.scheduler.mode`, `.allocation.file`, `spark.scheduler.pool` | [scheduling mode and pools](#scheduling-mode-and-pools), [the Schedulable tree](#the-schedulable-tree) |
| `spark.scheduler.resource.profileMergeConflicts` | [stage creation and cross-job reuse](#stage-creation-and-cross-job-reuse) |
| `spark.stage.maxAttempts`, `.maxConsecutiveAttempts`, `.ignoreDecommissionFetchFailure`, `spark.files.fetchFailure.unRegisterOutputOnHost`, `spark.scheduler.maxRetainedRemovedDecommissionExecutors` | [fetch failure and stage retry](#fetch-failure-and-stage-retry) |
| `spark.scheduler.barrier.maxConcurrentTasksCheck.interval`, `.maxFailures`, `spark.barrier.sync.timeout` | [barrier execution](#barrier-execution) |
| `spark.job.interruptOnCancel`, `spark.scheduler.numCancelledJobGroupsToTrack`, `spark.scheduler.stage.legacyAbortAfterKillTasks` | [job completion and cancellation](#job-completion-and-cancellation) |
| `spark.scheduler.revive.interval`, `.minRegisteredResourcesRatio`, `.maxRegisteredResourcesWaitingTime`, `.maxRetainedUnknownDecommissionExecutors`, `spark.executor.instances` | [executor registration and the offer loop](#executor-registration-and-the-offer-loop), [the driver↔executor message protocol](#the-driverexecutor-message-protocol) |
| the 5 `spark.locality.wait*` keys | [delay scheduling and locality](#delay-scheduling-and-locality) |
| `spark.task.cpus`, `spark.executor.cores`, `spark.task.resource.*`, `spark.executor.resource.*` | [slot arithmetic](#slot-arithmetic-and-resource-profiles), [fractional resource allocation](#fractional-resource-allocation) |
| `spark.task.maxDirectResultSize`, `spark.task.maxFailures` | [task result delivery](#task-result-delivery), [task failure and retry](#task-failure-and-retry) |
| the 9 `spark.speculation*` keys | [speculation](#speculation) |
| the 13 `spark.excludeOnFailure.*` keys + `spark.scheduler.executorTaskExcludeOnFailureTime` | [executor exclusion](#executor-exclusion) |
| `spark.scheduler.excludeOnFailure.unschedulableTaskSetTimeout` | [unschedulable TaskSets](#unschedulable-tasksets-and-the-abort-timer) |
| the 4 `spark.task.reaper.*` keys | [kill path and the TaskReaper](#kill-path-and-the-taskreaper) |
| `spark.executor.heartbeatInterval`, `.heartbeat.maxFailures`, `.heartbeat.dropZeroAccumulatorUpdates` | [heartbeat and expiry](#heartbeat-and-expiry), [task metrics and the accumulator pipeline](#task-metrics-and-the-accumulator-pipeline) |
| `spark.executor.metrics.pollingInterval`, `.metrics.fileSystemSchemes`, `.processTreeMetrics.enabled` | [executor metrics polling](#executor-metrics-polling), [executor memory metrics and procfs](#executor-memory-metrics-and-the-procfs-process-tree) |
| the 12 `spark.dynamicAllocation.*` keys | [dynamic allocation](#dynamic-allocation) |
| `spark.executor.decommission.killInterval`, `.forceKillTimeout`, `.signal` | [decommissioning](#decommissioning) |
| `spark.executor.userClassPathFirst`, `.isolatedSessionCache.size`, `.killOnFatalError.depth`, `.extraClassPath`, `.defaultExtraClassPath` | [executor class loading and session isolation](#executor-class-loading-and-session-isolation) |
| `spark.scheduler.dropTaskInfoAccumulablesOnTaskCompletion.enabled` | [TaskInfo accumulable retention](#taskinfo-accumulable-retention) |
| `spark.scheduler.streaming.idAwareLogging.enabled`, `.queryIdLength` | [streaming-aware scheduler logging](#streaming-aware-scheduler-logging) |

**The 25 the slice caught that this group does not read** — each is a finding about the carving, not an omission here:

| Keys | Read where | Owning group |
|---|---|---|
| the 5 `spark.scheduler.listenerbus.*` keys | `AsyncEventQueue`, `LiveListenerBus` (in `scheduler/`, but the listener bus, not the scheduler) | `core — monitoring` |
| the 6 `spark.executor.logs.*` keys | `util/logging/FileAppender`, driven by `ExecutorRunner` | `core — submit-standalone` |
| `spark.executor.memory`, `.memoryOverhead`, `.memoryOverheadFactor`, `.minMemoryOverhead`, `.pyspark.memory` | `SparkContext` and each resource manager | `core — shuffle-memory`, `resource-managers/*` |
| `spark.executor.extraJavaOptions`, `.extraLibraryPath` | launch-command construction | `core — submit-standalone` |
| `spark.resources.discoveryPlugin`, `.warnings.testing` | `resource/ResourceUtils` | `core — rpc-resources` |
| `spark.executor.allowSparkContext`, `.syncLogLevel.enabled` | `SparkContext` (root package, not `executor/`) | unclaimed root files — see below |
| `spark.executor.python.worker.log.details` | `api/python/Python.scala` | `core — api-bridge` |
| `spark.executor.limitActiveProcessorCount.enabled` | k8s `Client` | `resource-managers/kubernetes` |
| `spark.executor.id` | set by the backend at launch; read as an identifier, not behaviour | — |

**Configs this group reads that are not in the catalog** (invisible to `--sweeps`, so eye-only): `spark.hadoop.outputCommitCoordination.enabled` (a bare `SparkConf.getBoolean`, already noted under [output commit coordination](#output-commit-coordination)) and `spark.hadoop.validateOutputSpecs`, read through `SparkHadoopWriterUtils.isOutputSpecValidationEnabled`.

## Breadth check 2 — the packages

`ls`-walked against the scope, including the three nested sub-packages of `scheduler/` that `check_drift.py --coverage` structurally cannot see. Ratios are cited files over `.scala` files.

| Package | Files | Cited | Note |
|---|---|---|---|
| `scheduler/` | 57 | 48 | the 9 uncited are listed below |
| `scheduler/cluster/` | 6 | 5 | `StandaloneSchedulerBackend` is swept by `submit-standalone` |
| `scheduler/dynalloc/` | 1 | 1 | `ExecutorMonitor` |
| `scheduler/local/` | 1 | 1 | `LocalSchedulerBackend` |
| `executor/` | 18 | 17 | only `package.scala` uncited |
| `internal/io/` | 6 | 6 | complete as of this run |

**Deliberately left to another group** (8 files, all in `scheduler/`):

- `LiveListenerBus`, `AsyncEventQueue`, `SparkListener`, `SparkListenerBus`, `ReplayListenerBus`, `EventLoggingListener`, `StatsReportListener` — the listener-bus layer. It sits in `scheduler/`, which this group's scope token claims, but it is the observability pipeline and `core — monitoring` owns the theme (and cites four of the seven). **Finding:** `SparkListener`, `SparkListenerBus`, `ReplayListenerBus` and `StatsReportListener` are cited by *no* sweep page. That is a real hole in `core — monitoring`, not in this one, and it is recorded here because this is where the mechanical walk found it.
- `MapStatus` — explicitly assigned to `core — shuffle-memory` by that group's scope, and cited there.

**Not covered, and not owned by anyone else:** nothing. `package.scala` in both packages is a package object holding implicits and constants, no concept.

The two checks disagreed sharply on this group, which is the reason the July pass looked finished. Config breadth was already ~77% attributed once family shorthand is expanded; package breadth was **24 of 88 files**. Everything this run added came from the package walk. Coverage after this run: **78 of 89 files**.

## Overlapping topic traces

Four topic traces cover codes in this page's front matter, all recorded at Spark 4.2.0, so no version mismatch:

- **[B1 — Spark Architecture & the Execution Model](../topics/b1.md).** Agrees. It already cites `ShuffleMapTask.scala:54`/`:82` and `ResultTask.scala:78` for the stage-boundary-is-a-return-type point; [the Task object](#the-task-object-and-how-it-reaches-an-executor) here goes below that to `TaskDescription` and the broadcast `taskBinary`, which the trace does not reach. No contradiction.
- **[B4 — Reading and Writing Data](../topics/b4.md).** Agrees. It traces `FileCommitProtocol` from the SQL side (`setupJob`, `commitJob`, `newTaskTempFile`); [the RDD write path](#the-rdd-write-path-and-the-hadoop-output-formats) adds the *other* caller, `SparkHadoopWriter`, which the trace never mentions because no DataFrame write reaches it. Complementary, and the anchors match.
- **[I5 — Partitioning](../topics/i5.md).** No overlap in substance — the trace is about partition counts and repartitioning, and says nothing about locality hints. [Preferred locations from Hadoop input formats](#preferred-locations-from-hadoop-input-formats) is new to the map.
- **[I7 — The Spark UI](../topics/i7.md).** Partial disagreement in *coverage*, not in fact. The trace lists `spark.executor.metrics.pollingInterval` in its config table and otherwise reads the UI as a finished artefact; it has nothing on where the numbers come from. That gap is exactly what the **E49** proposal is for, and the trace should gain a pointer to it once E49 is written.

## Sweep log

| Date | Spark | What changed |
|---|---|---|
| 2026-07-19 | 4.2.0 | Initial sweep, in two halves. 27 concepts. Four discovery gaps proposed as topics: A13 (stage retry), A14 (determinism and rollback), E12 (executor exclusion), E13 (barrier execution). Push-based shuffle traced only at its driver-side boundaries, since it belongs to the `shuffle-memory` group. |
| 2026-07-19 | 4.2.0 | Correction: this page originally read the absence of `spark.shuffle.push.*` from this group's slice as a `groups.yaml` carving gap. It is not — `shuffle-memory`'s scope names push-based shuffle and its slice holds all thirteen keys, and the [shuffle & memory sweep](core-shuffle-memory.md) covers the subsystem in full. |
| 2026-07-25 | 4.2.0 | Carving fix: `internal/io/` moved into this group's scope and swept. The **Hadoop commit protocol** — `FileCommitProtocol` and `HadoopMapReduceCommitProtocol` — was claimed by `config-security`'s over-broad `internal/` token and covered by no sweep at all. It belongs here: `OutputCommitCoordinator` (already on this page) decides *who* may commit, and this is *how* the commit happens, so topic **E17** now has both halves in one place. The finding worth carrying: `commitJob` renames staged files one at a time with no rollback, and under dynamic partition overwrite each destination partition is deleted *before* its replacement is renamed in. |
| 2026-07-25 | 4.2.0 | Re-sweep at the same Spark version, driven by the config-slice breadth check rather than a release. Five concepts added from keys and files the first pass never tied to anything: output commit coordination (proposed as **E17** — the first pass mentioned the coordinator in one clause of the speculation note and never traced it), unschedulable TaskSets and the abort timer, cluster-manager selection and local mode, `TaskInfo` accumulable retention, and streaming-aware scheduler logging. Correction: the stage-creation note cited `spark.resources.resourceProfileMergeConflicts`, which is not a Spark config key — the real one is `spark.scheduler.resource.profileMergeConflicts` (`DAGScheduler.scala:238`). |
| 2026-08-09 | 4.2.0 | **Re-sweep at an unchanged version, found by breadth check 2 (packages).** The page was carrying `status: complete` on **24 of 88 in-scope files cited** — `executor/` 4/18, `internal/io/` 2/6, `scheduler/` 19/57 — and had no breadth-check or overlap sections at all, only a sweep log, so nothing recorded what had been skipped. Eleven concepts added and nine existing ones extended; coverage now 78/89. Two proposals: **E49** the task-metrics accumulator pipeline, **E50** executor class loading and session isolation. The whole per-task metrics layer (`TaskMetrics` and the four sibling metric classes), the `Task`/`TaskDescription` pair the entire engine passes around, the driver↔executor message protocol, `SparkHadoopWriter` (a class this group's own scope names by name and the previous pass never opened), and the executor class-loading stack were all absent from a page that both checkers passed. Findings worth carrying: bytes-written metrics refresh only every 256 records while record counts are exact; `ProcfsMetricsGetter` disables itself permanently after one failed read; `exitCausedByApp` is set by the cluster manager, so identical crashes count differently across deployments; and `userClassPathFirst` flips resource lookup as well as class lookup, which is how shading a jar silently takes over logging config. Also recorded a hole in another group: `SparkListener`, `SparkListenerBus`, `ReplayListenerBus` and `StatsReportListener` are cited by no sweep page anywhere — they are `core — monitoring`'s theme and its page misses them. |
