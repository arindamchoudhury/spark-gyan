# Chapter 30 — Spark Internals: Memory, Execution, Serialisation

> *Learning-path topic: E1 (Expert)*
> *Status: ⬜ Not yet written*

!!! note "📌 Topics deferred here from Chapter 1"
    The following DAGScheduler internals are introduced conceptually in [Chapter 1](ch01-spark-architecture.md) and covered in full implementation detail here:

    - **`handleJobSubmitted → createResultStage → submitStage → submitMissingTasks`** — the full call chain from job submission to task launch, including how `createResultStage` recursively calls `getOrCreateShuffleMapStage` to build the stage graph bottom-up
    - **State machine** — `activeJobs`, `waitingStages`, `runningStages`, `failedStages` and how `CompletionEvent` and `TaskSetFailed` drive transitions between them
    - **Stage deduplication** — `getOrCreateParentStages` ensures a shared RDD ancestor (e.g. a cached or checkpointed RDD depended on by two branches) becomes a single stage, not one per downstream branch
    - **Barrier execution mode** — all tasks in a barrier stage must launch simultaneously; used for distributed ML frameworks (e.g. Horovod) that need a global synchronisation point before proceeding
    - **Stage and job cancellation** — how `cancelJob`, `cancelStage`, and `killTaskAttempt` propagate through the event loop and interrupt running tasks on executors

*This chapter is not yet written. The above topics will form its core.*
