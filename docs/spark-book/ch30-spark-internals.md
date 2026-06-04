# Chapter 30 — Spark Internals: Memory, Execution, Serialisation

> *Learning-path topic: E1 (Expert)*
> *Status: ⬜ Not yet written*

!!! note "📌 Topics deferred here from Chapter 1"
    The following DAGScheduler internals are introduced conceptually in [Chapter 1](ch01-spark-architecture.md) and covered in full implementation detail here:

    - **`handleJobSubmitted → createResultStage → submitStage → submitMissingTasks`** — the full call chain from job submission to task launch, including how `createResultStage` recursively calls `getOrCreateShuffleMapStage` to build the stage graph bottom-up
    - **State machine** — `activeJobs`, `waitingStages`, `runningStages`, `failedStages` and how `CompletionEvent` and `TaskSetFailed` drive transitions between them
    - **Stage deduplication** — `getOrCreateParentStages` ensures a shared RDD ancestor (e.g. a cached or checkpointed RDD depended on by two branches) becomes a single stage, not one per downstream branch
    - **Barrier execution mode** — all tasks in a barrier stage must launch simultaneously; used for distributed ML frameworks (e.g. Horovod) that need a global synchronization point before proceeding
    - **Stage and job cancellation** — how `cancelJob`, `cancelStage`, and `killTaskAttempt` propagate through the event loop and interrupt running tasks on executors

!!! note "📌 Additional topics deferred here from Chapter 1"
    - **DataFrame → RDD lineage translation** — how `QueryExecution` compiles a DataFrame into an `RDD[InternalRow]`: the `Dataset.withAction()` entry point; the four compilation phases (Analyzer → Optimizer → SparkPlanner → PrepareForExecution); how `executedPlan.execute()` walks the `SparkPlan` tree recursively via `executeRDD = LazyTry { doExecute() }`; how each operator's `doExecute()` calls `child.execute()` bottom-up; how `ShuffleExchangeExec` embeds a `ShuffleDependency` into the RDD lineage (via `ShuffledRowRDD.getDependencies`), which is what the DAGScheduler detects as a stage boundary; `QueryExecution.toRdd` as the bridge: `new SQLExecutionRDD(executedPlan.execute(), conf)` (source-verified v4.1.2). The recursive execution tree:
      ```
      root.execute()
        └── root.doExecute()
              └── child.execute()
                    └── child.doExecute()       ← ShuffleExchangeExec: returns ShuffledRowRDD(shuffleDependency)
                          └── leaf.doExecute()  ← FileScanRDD: one partition per input file split
      ```
    - **Internal row formats** — `InternalRow` (logical row abstraction), `UnsafeRow` (compact binary row used by Tungsten; on-heap by default, off-heap opt-in; reduces GC), and Apache Arrow (columnar format used for cross-process transfer in pandas UDFs); why three formats exist and when each is used
    - **Speculative execution detection mechanism** — executor heartbeat protocol, `spark.speculation.quantile = 0.9` (fraction of stage tasks that must complete before speculation begins), `spark.speculation.multiplier = 3` (how many times slower than median before a task is flagged), `spark.speculation.efficiency.enabled = true` (Spark 3.4+)
    - **Data locality decision logic** — `spark.locality.wait` (default 3s) and per-level overrides (`spark.locality.wait.process`, `.node`, `.rack`); how the TaskScheduler iterates through locality levels and falls back when no slot at the preferred level is available within the wait window
    - **TaskSet internal representation** — `TaskSet` as an immutable collection; how it interacts with `TaskSetManager`; how the event loop processes task completion events and updates TaskSet state
    - **Shuffle file naming scheme** — how `(shuffleId, mapTaskId, attemptId)` are encoded in shuffle filenames; why attempt-scoped naming ensures retried tasks cannot overwrite successful output; how DAGScheduler uses this to safely resubmit stages
    - **DAGScheduler event loop internals** — `DAGSchedulerEventProcessLoop` class; single daemon thread `"dag-scheduler-event-loop"` draining a `LinkedBlockingDeque`; why most state mutations are single-threaded but some shared structures (notably `cacheLocs`) require explicit `synchronized` guards; source "avoid deadlocks" comment covering ordering of `rdd.stateLock` vs `cacheLocs`; AQE re-planning is not a DAGScheduler event — it runs in `AdaptiveSparkPlanExec`'s own execution context and submits new query stages as normal `JobSubmitted` events; backpressure behaviour when events arrive faster than they can be processed
    - **Shuffle write/read mechanics** — how map tasks sort and partition output before writing; how reducer-side merge works; why reducers may need to re-sort fetched data if order matters
    - **MapOutputTracker internals** — `MapOutputTrackerMaster` on the driver vs `MapOutputTrackerWorker` on each executor; worker maintains a local `mapStatuses` cache and uses epoch-based invalidation to avoid querying the master on every shuffle read; after every ShuffleMapStage task completes the master registers `shuffleStatuses[shuffleId][mapIndex] → MapStatus (location: BlockManagerId + getSizeForBlock(reduceId): Long)` — key is `mapIndex` (partition index, 0-based), not a task ID; for large shuffle outputs the master delivers statuses via Broadcast rather than direct RPC
    - **MapOutputTracker operational semantics** — whether it is consulted once per stage or once per task; how stale entries are invalidated when an executor dies and its shuffle blocks are re-registered after ShuffleMapStage resubmission
    - **BlockManager block types** — `BlockId` addressing scheme (`RDDBlockId`, `ShuffleBlockId`, `BroadcastBlockId`); block ownership and eviction lifetime differences between cached partitions, shuffle files, and broadcast copies; how TCP fetch streams blocks
    - **SchedulerBackend RPC internals** — `CoarseGrainedSchedulerBackend` as the shared base for all cluster managers; `DriverEndpoint` as the RPC endpoint executors connect to; `RegisterExecutor` message on startup; `StatusUpdate` message on task completion/failure; `makeOffers()` → `TaskScheduler.resourceOffers(offers)` when a slot becomes free; `launchTasks()` serialising `TaskDescription` and sending to executor; `killTask()` for task-kill signals; `reviveThread` firing `ReviveOffers` periodically so delay scheduling can re-evaluate locality preferences without waiting for a new status update; per-cluster-manager subclasses: `StandaloneSchedulerBackend`, `YarnClientSchedulerBackend` / `YarnClusterSchedulerBackend` (YARN client and cluster deploy modes; `YarnSchedulerBackend` is the abstract base), `KubernetesClusterSchedulerBackend`, `LocalSchedulerBackend` (extends `SchedulerBackend` directly, bypasses `CoarseGrainedSchedulerBackend` entirely)
    - **Executor exclusions (`HealthTracker`)** — when an executor accumulates too many task failures (`MAX_FAILURES_PER_EXEC`), `HealthTracker` marks it excluded and `TaskSchedulerImpl` skips it during slot assignment (`isExecutorExcluded`); exclusion expires after `EXCLUDE_ON_FAILURE_TIMEOUT_MILLIS`; whole nodes can also be excluded via `MAX_FAILED_EXEC_PER_NODE`; `EXCLUDE_ON_FAILURE_KILL_ENABLED` can decommission the executor immediately
    - **Serialization in the shuffle data path** — how `UnsafeRow` binary format avoids extra serialization during shuffle for SQL/DataFrame operations; when Java vs Kryo serialization applies to shuffle data for raw RDD operations

!!! note "✍️ Writing reminder — DataFrame → RDD translation"
    Chapter 1 introduces `QueryExecution` conceptually. This chapter must cover the full implementation: `Dataset.withAction()`, the recursive `execute()` / `doExecute()` tree, how `ShuffleExchangeExec` embeds `ShuffleDependency` into the RDD lineage via `ShuffledRowRDD`, `FileScanRDD` as the leaf, and `QueryExecution.toRdd` as the bridge to `SparkContext.runJob()`. See the deferred-topics note above for the source-verified detail.

*This chapter is not yet written. The above topics will form its core.*
