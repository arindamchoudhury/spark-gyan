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
    - **DataFrame → RDD lineage translation** — how `QueryExecution` compiles a DataFrame into an `RDD[InternalRow]`: the `Dataset.withAction()` entry point; the four compilation phases (Analyzer → Optimizer → SparkPlanner → PrepareForExecution); how `executedPlan.execute()` walks the `SparkPlan` tree recursively via `executeRDD = LazyTry { doExecute() }`; how each operator's `doExecute()` calls `child.execute()` bottom-up; how `ShuffleExchangeExec` embeds a `ShuffleDependency` into the RDD lineage (via `ShuffledRowRDD.getDependencies`), which is what the DAGScheduler detects as a stage boundary; `QueryExecution.toRdd` as the bridge: `new SQLExecutionRDD(executedPlan.execute(), conf)` (source-verified v4.1.2)
    - **Internal row formats** — `InternalRow` (logical row abstraction), `UnsafeRow` (compact binary row used by Tungsten; on-heap by default, off-heap opt-in; reduces GC), and Apache Arrow (columnar format used for cross-process transfer in pandas UDFs); why three formats exist and when each is used
    - **Speculative execution detection mechanism** — executor heartbeat protocol, `spark.speculation.quantile = 0.9` (fraction of stage tasks that must complete before speculation begins), `spark.speculation.multiplier = 3` (how many times slower than median before a task is flagged), `spark.speculation.efficiency.enabled = true` (Spark 3.4+)
    - **Data locality decision logic** — `spark.locality.wait` (default 3s) and per-level overrides (`spark.locality.wait.process`, `.node`, `.rack`); how the TaskScheduler iterates through locality levels and falls back when no slot at the preferred level is available within the wait window
    - **TaskSet internal representation** — `TaskSet` as an immutable collection; how it interacts with `TaskSetManager`; how the event loop processes task completion events and updates TaskSet state
    - **Shuffle file naming scheme** — how `(shuffleId, mapTaskId, attemptId)` are encoded in shuffle filenames; why attempt-scoped naming ensures retried tasks cannot overwrite successful output; how DAGScheduler uses this to safely resubmit stages
    - **DAGScheduler event loop backpressure** — how `DAGSchedulerEventProcessLoop` handles high event arrival rates; what happens when task completion events queue up faster than they can be processed; whether this can become a bottleneck at large cluster scale
    - **Shuffle write/read mechanics** — how map tasks sort and partition output before writing; how reducer-side merge works; why reducers may need to re-sort fetched data if order matters
    - **MapOutputTracker operational semantics** — whether it is consulted once per stage or once per task; how stale entries are invalidated when an executor dies and its shuffle blocks are re-registered after ShuffleMapStage resubmission
    - **BlockManager block types** — `BlockId` addressing scheme (`RDDBlockId`, `ShuffleBlockId`, `BroadcastBlockId`); block ownership and eviction lifetime differences between cached partitions, shuffle files, and broadcast copies; how TCP fetch streams blocks
    - **Serialization in the shuffle data path** — how `UnsafeRow` binary format avoids extra serialization during shuffle for SQL/DataFrame operations; when Java vs Kryo serialization applies to shuffle data for raw RDD operations

*This chapter is not yet written. The above topics will form its core.*
