# Chapter 17 — Caching and Persistence

> *Learning-path topic: I25 (Intermediate)*
> *Status: ⬜ Not yet written*

!!! note "📌 Topics deferred here from Chapters 1 and 2"
    Chapter 1 introduces `.cache()` as the working-set mechanism that MapReduce lacked, and points here for the trade-offs. Chapter 2 introduces the executor memory layout (storage memory region, LRU eviction). The following are covered in full here:

    - **Caching as lineage management** — calling `.cache()` persists partitions in the storage memory region after first computation; subsequent actions reuse cached partitions instead of replaying the full lineage from source; the architectural reason is to trade memory for CPU time (avoiding re-reads of stable data)
    - **Storage levels** — `MEMORY_ONLY`, `MEMORY_AND_DISK`, `MEMORY_ONLY_SER`, `DISK_ONLY`, `OFF_HEAP`, and the `_2` replication variants; when to choose each
    - **Eviction policy** — LRU at partition granularity; which partitions are evicted when the storage memory floor is exceeded by execution
    - **`unpersist()`** — when and why to explicitly release cached data
    - **Checkpoint vs cache** — cache keeps data in executor memory with lineage intact; checkpoint writes to HDFS and cuts the lineage entirely; when each is appropriate

*This chapter is not yet written. The above topics will form its core.*
