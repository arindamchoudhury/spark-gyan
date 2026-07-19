---
subsystem: core
spark_version: "4.2.0"
swept_at: 2026-07-19
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

**Configs:** `spark.scheduler.mode`, `spark.scheduler.allocation.file`; `spark.resources.resourceProfileMergeConflicts` gates stage-level profile merging and throws when profiles conflict

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

**Configs:** `spark.stage.maxAttempts`, `spark.scheduler.barrier.maxConcurrentTasksCheck.interval`, `.maxFailures`

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

**Configs:** `spark.job.interruptOnCancel` (local property), `spark.scheduler.numCancelledJobGroupsToTrack`, `spark.scheduler.stage.legacyAbortAfterKillTasks`

**Maps to topics:** B1, E1

---

## Push-based shuffle finalization

**What it is:** with push-based shuffle on, a completed `ShuffleMapStage` does not proceed straight to its children — merge finalization must complete first, because `getMissingParentStages` requires `shuffleMergeFinalized` in addition to `isAvailable`. This inserts a driver-side wait between map stage completion and reduce stage submission.

**Anchor files:**

- [DAGScheduler.scala:856](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala#L856) — the `!isAvailable || !shuffleMergeFinalized` condition that blocks child stages
- [DAGScheduler.scala:2987](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala#L2987) — merge statuses arriving after finalization are silently dropped (acknowledged `TODO: SPARK-35549`)
- [DAGScheduler.scala:352](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala#L352) — the six push-shuffle timing/threading configs read into fields

!!! note "Partial coverage — flagged deliberately"

    Only the boundaries of this subsystem were traced. The RPC fan-out to merger shuffle-service instances (`finalizeShuffleMerge`), its timeout and threading model, and `cancelFinalizeShuffleMergeFutures` were **not** read line by line and carry no anchors here. The `spark.shuffle.push.*` config family is also absent from this group's config slice, which suggests a `groups.yaml` carving gap worth revisiting.

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

**Configs:** `spark.scheduler.revive.interval`, `spark.scheduler.minRegisteredResourcesRatio`, `spark.scheduler.maxRegisteredResourcesWaitingTime`, `spark.rpc.message.maxSize`

**Maps to topics:** B1, E2

---

## TaskSet submission and zombies

**What it is:** `submitTasks` wraps a `TaskSet` in a `TaskSetManager` and inserts it into the pool. Submitting a new attempt marks **all existing TSMs for that stage as zombies** — they stop launching tasks but keep accounting for those still running.

**Anchor files:**

- [TaskSchedulerImpl.scala:243](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/TaskSchedulerImpl.scala#L243) — `submitTasks`; the comment block explains the zombie corner case
- [TaskSetManager.scala:169](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/TaskSetManager.scala#L169) — the zombie definition, with a standing `TODO` that running attempts are not killed
- [TaskSchedulerImpl.scala:269](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/TaskSchedulerImpl.scala#L269) — the starvation timer warns every `spark.starvation.timeout` (15 s) while nothing launches

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

**Configs:** `spark.locality.wait` with `.process`/`.node`/`.rack` fallbacks, `spark.locality.wait.legacyResetOnTaskLaunch`

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

**Configs:** `spark.speculation`, `.interval`, `.multiplier`, `.quantile`, `.minTaskRuntime`, `.task.duration.threshold`, `.efficiency.*`, `spark.executor.decommission.killInterval`

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

**Configs:** the `spark.excludeOnFailure.*` family (task/stage/application scoped, timeout, kill and decommission variants)

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

**Configs:** `spark.task.reaper.enabled`, `.killTimeout`, `.pollingInterval`, `.threadDump`

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

**Configs:** `spark.decommission.enabled`, `spark.executor.decommission.killInterval`, `.forceKillTimeout`, `.signal`, `spark.storage.decommission.*`

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

**Configs:** the `spark.dynamicAllocation.*` family

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

## Sweep log

| Date | Spark | What changed |
|---|---|---|
| 2026-07-19 | 4.2.0 | Initial sweep, in two halves. 27 concepts. Four discovery gaps proposed as topics: A13 (stage retry), A14 (determinism and rollback), E12 (executor exclusion), E13 (barrier execution). Push-based shuffle finalization covered at its boundaries only — flagged as partial, and its `spark.shuffle.push.*` config family is absent from this group's slice, suggesting a `groups.yaml` carving gap. |
