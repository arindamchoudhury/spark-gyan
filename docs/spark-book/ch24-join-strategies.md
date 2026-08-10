# Chapter 24 — Join Strategies and Tuning

> *Learning-path topic: A15 (Advanced)*
> *Status: ⬜ Not yet written*

!!! note "📌 Topics deferred here from Chapters 1 and 2"
    Chapter 1 notes that avoiding the shuffle is a join-strategy decision and points here. Chapter 2 explains that Catalyst selects a broadcast join when one side is below `spark.sql.autoBroadcastJoinThreshold` (default 10 MB) and that this eliminates the shuffle. The following execution-level differences are covered in full here:

    - **`BroadcastHashJoin` vs `SortMergeJoin` at execution** — broadcast join: driver serializes the small table, executors cache it as a hash table, probe side streams through and looks up rows locally (no shuffle stage); sort-merge join: both sides are shuffled by join key, sorted, then merged in a single pass; the execution cost difference is one full shuffle stage
    - **`ShuffledHashJoin`** — a third strategy used when one side fits in memory but is too large to broadcast; shuffles both sides, builds a hash table from the smaller side in each partition, probes with the larger side
    - **Join hints** — `BROADCAST`, `MERGE`, `SHUFFLE_HASH`, `SHUFFLE_REPLICATE_NL`; how to force a strategy when Catalyst picks the wrong one
    - **AQE runtime strategy switching** — how AQE can switch from `SortMergeJoin` to `BroadcastHashJoin` mid-execution when actual sizes differ from estimates
    - **Skew join handling** — AQE's split-and-replicate approach for skewed partitions

*This chapter is not yet written. The above topics will form its core.*
