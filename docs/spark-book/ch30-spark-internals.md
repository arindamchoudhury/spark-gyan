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
    - **Internal row formats** — `InternalRow` (logical row abstraction), `UnsafeRow` (off-heap binary row used by Tungsten; avoids GC), and Apache Arrow (columnar format used for cross-process transfer in pandas UDFs); why three formats exist and when each is used
    - **Speculative execution detection mechanism** — executor heartbeat protocol, `spark.speculation.quantile = 0.9` (fraction of stage tasks that must complete before speculation begins), `spark.speculation.multiplier = 3` (how many times slower than median before a task is flagged), `spark.speculation.efficiency.enabled = true` (Spark 3.4+)
    - **Data locality decision logic** — `spark.locality.wait` (default 3s) and per-level overrides (`spark.locality.wait.process`, `.node`, `.rack`); how the TaskScheduler iterates through locality levels and falls back when no slot at the preferred level is available within the wait window

*This chapter is not yet written. The above topics will form its core.*
