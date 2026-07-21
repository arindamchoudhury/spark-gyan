# Core / RDD / Scheduler

> Source: `release-notes/spark_all_changelogs.txt`. Timeline rows below are generated from `_catalog.jsonl`; prose outside the AUTO markers is hand-written.

## How it evolved

### 0.x era — origins

The RDD abstraction and its scheduler were already the core of Spark by 0.3, which added Hadoop-backed I/O (HDFS, S3, Hypertable), `hadoopRDD` for non-filesystem sources, outer joins, and finer control over parallelism and split counts. Through 0.5.x the scheduler grew more communication-efficient for large lineage graphs, closure serialization became configurable, and worker exceptions started surfacing in the master log; new operators like `sortByKey` and `takeSample` arrived alongside partition-aware joins that could skip a shuffle entirely.

0.6.0 added a Java API layer, an `Accumulable` class generalizing accumulators, dynamic file/JAR shipping via `SparkContext.addFile/Jar`, and scheduler tuning for sub-500ms jobs. 0.7.0 brought more transformations (`keys`, `values`, `keyBy`, `subtract`, `coalesce`, `zip`), `RDD.toDebugString()`, and a refactored scheduler codebase for testing. By 0.8.0–0.9.0 Spark had a fair scheduler for multi-user sharing, rack-aware topology scheduling, `SparkConf`, and operators like `takeOrdered`, `zipPartitions`, `top`, `repartition`, `histogram`, and `countDistinctApprox`.

### 1.x era — Java 8 lambdas and lineage-driven cleanup

1.0.0 added support for Java 8's lambda syntax in the Java API, letting Java callers write concise anonymous functions instead of verbose `Function` implementations. The same release taught Spark to garbage-collect intermediate job state automatically once the RDDs referencing it fell out of scope, rather than requiring a manual `unpersist()` or accumulating stale state for the life of the `SparkContext`. 1.0.0 also added `SparkContext.wholeTextFiles`, letting small text files be read as individual (filename, content) records instead of being split line by line — useful for corpora made of many small documents.

### 2.x era — off-heap memory and barrier execution bookend the line

2.0.0 opened the era with a simpler, more performant accumulator API and off-heap memory management for both caching and runtime execution — moving data structures outside the JVM heap to reduce GC pressure. 2.2.0 fixed uncancellable tasks that could starve a job of resources (SPARK-18761) and ported the RDD API onto the same commit protocol used by DataFrame writes (SPARK-18191). The line closes with 2.4.0's Barrier Execution Mode (SPARK-24374), a scheduler addition that lets all tasks in a stage start together and communicate directly — built specifically so deep-learning frameworks like Horovod could run gang-scheduled distributed training jobs on top of RDDs.

## Timeline

<!-- AUTO:timeline START -->
| Release | JIRA | Type | Title |
|---|---|---|---|
| 0.3 | — | prose | Save distributed datasets to HDFS/S3/Hypertable and other Hadoop-supported storage |
| 0.3 | — | prose | Native types for common Writable types in SequenceFiles |
| 0.3 | — | prose | SparkContext.hadoopRDD for non-filesystem Hadoop input formats |
| 0.3 | — | prose | Outer join operators added |
| 0.3 | — | prose | Better default parallelism levels for various operations |
| 0.3 | — | prose | Ability to control number of splits in a file |
| 0.5.0 | — | prose | Scheduling made more communication-efficient for large lineage graphs |
| 0.5.0 | — | prose | Configurable closure serializer |
| 0.5.0 | — | prose | Worker exceptions reported back to master log |
| 0.5.0 | — | prose | Automatic marking/filtering of duplicate errors |
| 0.5.0 | — | prose | New operators: sortByKey, takeSample, faster fold/aggregate |
| 0.5.0 | — | prose | Join of identically hash-partitioned RDDs avoids network shuffle |
| 0.5.0 | — | prose | New Hadoop API (org.apache.mapreduce) read/write support |
| 0.5.1 | — | prose | New Accumulable class generalizes Accumulators for non-matching element/accumulated types |
| 0.5.1 | — | prose | Improved random sampling algorithm to avoid bias |
| 0.5.1 | — | prose | Improved load balancing across nodes in sort operations |
| 0.5.1 | — | prose | Reduced memory consumption of saveAsObjectFile for large objects |
| 0.6.0 | — | prose | Java API layer added |
| 0.6.0 | — | prose | Scheduler and control plane optimized for ultra-low-latency jobs |
| 0.6.0 | — | prose | New Accumulable class for accumulating collections/mismatched types |
| 0.6.0 | — | prose | Dynamic file/JAR shipping via SparkContext.addFile/Jar |
| 0.6.0 | — | prose | More operators (e.g. joins) support custom partitioners |
| 0.7.0 | — | prose | New RDD transformations: keys, values, keyBy, subtract, coalesce, zip |
| 0.7.0 | — | prose | SparkContext.hadoopConfiguration for global Hadoop I/O settings |
| 0.7.0 | — | prose | RDD.toDebugString() prints RDD lineage graph for troubleshooting |
| 0.7.0 | — | prose | Scheduler codebase refactored to enable richer unit testing |
| 0.7.0 | — | prose | SparkFiles.getRootDirectory/SparkFiles.get for locating shipped files |
| 0.7.2 | — | prose | New API methods: subtractByKey, foldByKey, mapWith, filterWith, foreachPartition |
| 0.8.0 | — | prose | Job scheduler refactored with fair scheduler for multi-user sharing |
| 0.8.0 | — | prose | Topology-aware scheduling extended: rack locality and multiple executors per machine |
| 0.8.0 | — | prose | New RDD operations: takeOrdered, zipPartitions, top |
| 0.8.0 | — | prose | RDD.coalesce now takes locality into account |
| 0.8.0 | — | prose | RDD.pipe supports passing environment variables to child processes |
| 0.8.0 | — | prose | Hadoop save functions support optional compression codec |
| 0.8.1 | — | prose | Torrent broadcast: faster broadcast implementation for large objects |
| 0.8.1 | — | prose | Support for fetching large result sets without tuning Akka buffer sizes |
| 0.8.1 | — | prose | New repartition operator |
| 0.9.0 | — | prose | New SparkConf class for configuring SparkContext settings |
| 0.9.0 | — | prose | spark-shell -i option to run a startup script |
| 0.9.0 | — | prose | New histogram and countDistinctApprox operators |
| 0.9.1 | — | prose | Support for HBase's TableOutputFormat and other Configurable OutputFormats |
| 1.0.0 | — | prose | Java 8 lambda syntax support in Java bindings |
| 1.0.0 | — | prose | Intermediate job state garbage collected when RDDs become unreferenced |
| 1.0.0 | — | prose | SparkContext.wholeTextFiles for operating on small files as records |
| 1.5.0 | [SPARK-1855](https://issues.apache.org/jira/browse/SPARK-1855) | New Feature | Provide memory-and-local-disk RDD checkpointing |
| 1.5.0 | [SPARK-3071](https://issues.apache.org/jira/browse/SPARK-3071) | Improvement | Increase default driver memory |
| 1.5.0 | [SPARK-5561](https://issues.apache.org/jira/browse/SPARK-5561) | Improvement | Generalize PeriodicGraphCheckpointer for RDDs |
| 1.5.0 | [SPARK-6684](https://issues.apache.org/jira/browse/SPARK-6684) | Improvement | Add checkpointing to GradientBoostedTrees |
| 1.5.0 | [SPARK-7810](https://issues.apache.org/jira/browse/SPARK-7810) | Improvement | rdd.py "_load_from_socket" cannot load data from jvm socket if ipv6 is used |
| 1.5.0 | [SPARK-8001](https://issues.apache.org/jira/browse/SPARK-8001) | Improvement | Make AsynchronousListenerBus.waitUntilEmpty throw TimeoutException if timeout |
| 1.5.0 | [SPARK-8059](https://issues.apache.org/jira/browse/SPARK-8059) | Improvement | Reduce latency between executor requests and RM heartbeat |
| 1.5.0 | [SPARK-8387](https://issues.apache.org/jira/browse/SPARK-8387) | Improvement | [SPARK][Web-UI] Only show 4096 bytes content for executor log instead all |
| 1.5.0 | [SPARK-8392](https://issues.apache.org/jira/browse/SPARK-8392) | Improvement | RDDOperationGraph: getting cached nodes is slow |
| 1.5.0 | [SPARK-8598](https://issues.apache.org/jira/browse/SPARK-8598) | New Feature | Implementation of 1-sample, two-sided, Kolmogorov Smirnov Test for RDDs |
| 1.5.0 | [SPARK-8625](https://issues.apache.org/jira/browse/SPARK-8625) | Improvement | Propagate user exceptions in tasks back to driver |
| 1.5.0 | [SPARK-8820](https://issues.apache.org/jira/browse/SPARK-8820) | Improvement | Add a configuration to set the checkpoint directory for convenience. |
| 1.5.0 | [SPARK-8880](https://issues.apache.org/jira/browse/SPARK-8880) | Improvement | Fix confusing Stage.attemptId member variable |
| 1.5.0 | [SPARK-8914](https://issues.apache.org/jira/browse/SPARK-8914) | Improvement | Remove RDDApi |
| 1.5.0 | [SPARK-9010](https://issues.apache.org/jira/browse/SPARK-9010) | Improvement | Improve the Spark Configuration document about `spark.kryoserializer.buffer` |
| 1.5.0 | [SPARK-9036](https://issues.apache.org/jira/browse/SPARK-9036) | Improvement | SparkListenerExecutorMetricsUpdate messages not included in JsonProtocol |
| 1.5.0 | [SPARK-9128](https://issues.apache.org/jira/browse/SPARK-9128) | Improvement | Get outerclasses and objects at the same time in ClosureCleaner |
| 1.5.0 | [SPARK-9144](https://issues.apache.org/jira/browse/SPARK-9144) | Improvement | Remove DAGScheduler.runLocallyWithinThread and spark.localExecution.enabled |
| 1.5.0 | [SPARK-9388](https://issues.apache.org/jira/browse/SPARK-9388) | Improvement | Make log messages in ExecutorRunnable more readable |
| 1.5.0 | [SPARK-9952](https://issues.apache.org/jira/browse/SPARK-9952) | Improvement | Fix N^2 loop when DAGScheduler.getPreferredLocsInternal accesses cacheLocs |
| 1.6.0 | [SPARK-4424](https://issues.apache.org/jira/browse/SPARK-4424) | Improvement | Clean up all SparkContexts in unit tests so that spark.driver.allowMultipleContexts can be false |
| 1.6.0 | [SPARK-6919](https://issues.apache.org/jira/browse/SPARK-6919) | New Feature | Add .asDict method to StatCounter |
| 1.6.0 | [SPARK-7463](https://issues.apache.org/jira/browse/SPARK-7463) | Umbrella | DAG visualization improvements |
| 1.6.0 | [SPARK-10184](https://issues.apache.org/jira/browse/SPARK-10184) | Improvement | Optimization for bounds determination in RangePartitioner |
| 1.6.0 | [SPARK-10575](https://issues.apache.org/jira/browse/SPARK-10575) | Improvement | Wrap RDD.takeSample with scope |
| 1.6.0 | [SPARK-10699](https://issues.apache.org/jira/browse/SPARK-10699) | Improvement | Support checkpointInterval can be disabled |
| 1.6.0 | [SPARK-10706](https://issues.apache.org/jira/browse/SPARK-10706) | Improvement | Add java wrapper for random vector rdd |
| 1.6.0 | [SPARK-10871](https://issues.apache.org/jira/browse/SPARK-10871) | Improvement | Specify number of failed executors in ApplicationMaster error message |
| 1.6.0 | [SPARK-10921](https://issues.apache.org/jira/browse/SPARK-10921) | Improvement | Completely remove the use of SparkContext.preferredNodeLocationData |
| 1.6.0 | [SPARK-11114](https://issues.apache.org/jira/browse/SPARK-11114) | Improvement | Add getOrCreate for SparkContext/SQLContext for Python |
| 1.6.0 | [SPARK-11163](https://issues.apache.org/jira/browse/SPARK-11163) | Improvement | Remove unnecessary addPendingTask calls in TaskSetManager.executorLost |
| 1.6.0 | [SPARK-11178](https://issues.apache.org/jira/browse/SPARK-11178) | Improvement | Improve naming around task failures in scheduler code |
| 1.6.0 | [SPARK-11362](https://issues.apache.org/jira/browse/SPARK-11362) | Improvement | Use Spark BitSet in BroadcastNestedLoopJoin |
| 1.6.0 | [SPARK-11709](https://issues.apache.org/jira/browse/SPARK-11709) | Improvement | Include call site info in SparkContext.assertNotStopped |
| 1.6.0 | [SPARK-11746](https://issues.apache.org/jira/browse/SPARK-11746) | Improvement | Use checkpoint-aware method 'dependencies' to instead of 'getDependencies' |
| 1.6.0 | [SPARK-11766](https://issues.apache.org/jira/browse/SPARK-11766) | New Feature | JSON serialization of Vectors |
| 1.6.0 | [SPARK-11799](https://issues.apache.org/jira/browse/SPARK-11799) | Improvement | Make it explicit in executor logs that uncaught exceptions are thrown during executor shutdown |
| 1.6.3 | [SPARK-17485](https://issues.apache.org/jira/browse/SPARK-17485) | Improvement | Failed remote cached block reads can lead to whole job failure |
| 1.6.3 | [SPARK-17649](https://issues.apache.org/jira/browse/SPARK-17649) | Improvement | Log how many Spark events got dropped in LiveListenerBus |
| 2.0.0 | — | prose | Simpler, more performant accumulator API |
| 2.0.0 | — | prose | Off-heap memory management for caching and execution |
| 2.0.0 | [SPARK-7727](https://issues.apache.org/jira/browse/SPARK-7727) | Improvement | Avoid inner classes in RuleExecutor |
| 2.0.0 | [SPARK-9819](https://issues.apache.org/jira/browse/SPARK-9819) | Improvement | reduceBy(KeyAnd)Window should specify which is the accumulator argument in invReduceFunc |
| 2.0.0 | [SPARK-10001](https://issues.apache.org/jira/browse/SPARK-10001) | Improvement | Allow Ctrl-C in spark-shell to kill running job |
| 2.0.0 | [SPARK-10911](https://issues.apache.org/jira/browse/SPARK-10911) | Improvement | Executors should System.exit on clean shutdown |
| 2.0.0 | [SPARK-11155](https://issues.apache.org/jira/browse/SPARK-11155) | Improvement | Stage summary json should include stage duration |
| 2.0.0 | [SPARK-12060](https://issues.apache.org/jira/browse/SPARK-12060) | Improvement | Avoid memory copy in JavaSerializerInstance.serialize |
| 2.0.0 | [SPARK-12080](https://issues.apache.org/jira/browse/SPARK-12080) | Improvement | Kryo - Support multiple user registrators |
| 2.0.0 | [SPARK-12392](https://issues.apache.org/jira/browse/SPARK-12392) | Improvement | Optimize a location order of broadcast blocks by considering preferred local hosts |
| 2.0.0 | [SPARK-12411](https://issues.apache.org/jira/browse/SPARK-12411) | Improvement | Reconsider executor heartbeats rpc timeout |
| 2.0.0 | [SPARK-12608](https://issues.apache.org/jira/browse/SPARK-12608) | Improvement | Remove submitJobThreadPool since submitJob doesn't create a separate thread to wait for the job result |
| 2.0.0 | [SPARK-12637](https://issues.apache.org/jira/browse/SPARK-12637) | Improvement | Print stage info of finished stages properly |
| 2.0.0 | [SPARK-12759](https://issues.apache.org/jira/browse/SPARK-12759) | Improvement | Spark should fail fast if --executor-memory is too small for spark to start |
| 2.0.0 | [SPARK-12994](https://issues.apache.org/jira/browse/SPARK-12994) | Improvement | It is not necessary to create ExecutorAllocationManager in local mode |
| 2.0.0 | [SPARK-13074](https://issues.apache.org/jira/browse/SPARK-13074) | New Feature | Add getPersistentRDDs() API to JavaSparkContext |
| 2.0.0 | [SPARK-13213](https://issues.apache.org/jira/browse/SPARK-13213) | Improvement | BroadcastNestedLoopJoin is very slow |
| 2.0.0 | [SPARK-13269](https://issues.apache.org/jira/browse/SPARK-13269) | Improvement | Expose more executor stats in stable status API |
| 2.0.0 | [SPARK-13279](https://issues.apache.org/jira/browse/SPARK-13279) | Improvement | Scheduler does O(N^2) operation when adding a new task set (making it prohibitively slow for scheduling 200K tasks) |
| 2.0.0 | [SPARK-13281](https://issues.apache.org/jira/browse/SPARK-13281) | Improvement | Switch broadcast of RDD to exception from warning |
| 2.0.0 | [SPARK-13348](https://issues.apache.org/jira/browse/SPARK-13348) | Improvement | Avoid duplicated broadcasts |
| 2.0.0 | [SPARK-13465](https://issues.apache.org/jira/browse/SPARK-13465) | New Feature | Add a task failure listener to TaskContext |
| 2.0.0 | [SPARK-13601](https://issues.apache.org/jira/browse/SPARK-13601) | Improvement | Invoke task failure callbacks before calling outputstream.close() |
| 2.0.0 | [SPARK-13621](https://issues.apache.org/jira/browse/SPARK-13621) | Improvement | TestExecutor.scala needs to be moved to test package |
| 2.0.0 | [SPARK-13758](https://issues.apache.org/jira/browse/SPARK-13758) | Improvement | Error message is misleading when RDD refer to null spark context |
| 2.0.0 | [SPARK-13921](https://issues.apache.org/jira/browse/SPARK-13921) | Improvement | Store serialized blocks as multiple chunks in MemoryStore |
| 2.0.0 | [SPARK-13980](https://issues.apache.org/jira/browse/SPARK-13980) | Improvement | Incrementally serialize blocks while unrolling them in MemoryStore |
| 2.0.0 | [SPARK-14069](https://issues.apache.org/jira/browse/SPARK-14069) | Improvement | Improve SparkStatusTracker to also track executor information |
| 2.0.0 | [SPARK-14110](https://issues.apache.org/jira/browse/SPARK-14110) | Improvement | PipedRDD to print the command ran on non zero exit |
| 2.0.0 | [SPARK-14169](https://issues.apache.org/jira/browse/SPARK-14169) | Improvement | Add UninterruptibleThread |
| 2.0.0 | [SPARK-14416](https://issues.apache.org/jira/browse/SPARK-14416) | Improvement | Add thread-safe comments for CoarseGrainedSchedulerBackend's fields |
| 2.0.0 | [SPARK-14475](https://issues.apache.org/jira/browse/SPARK-14475) | Improvement | Propagate user-defined context from driver to executors |
| 2.0.0 | [SPARK-14491](https://issues.apache.org/jira/browse/SPARK-14491) | Improvement | refactor object operator framework to make it easy to eliminate serializations |
| 2.0.0 | [SPARK-14542](https://issues.apache.org/jira/browse/SPARK-14542) | Improvement | PipeRDD should allow configurable buffer size for the stdin writer |
| 2.0.0 | [SPARK-14594](https://issues.apache.org/jira/browse/SPARK-14594) | Improvement | Improve error messages for RDD API |
| 2.0.0 | [SPARK-14636](https://issues.apache.org/jira/browse/SPARK-14636) | Improvement | Spark should fail fast if executor/driver memory is too small for the StaticMemoryManager |
| 2.0.0 | [SPARK-14889](https://issues.apache.org/jira/browse/SPARK-14889) | Improvement | scala.MatchError: NONE (of class scala.Enumeration$Val) when spark.scheduler.mode=NONE |
| 2.0.0 | [SPARK-15003](https://issues.apache.org/jira/browse/SPARK-15003) | Improvement | Use ConcurrentHashMap in place of HashMap for NewAccumulator.originals |
| 2.0.0 | [SPARK-15296](https://issues.apache.org/jira/browse/SPARK-15296) | Improvement | Refactor All Java Tests that use SparkSession |
| 2.0.0 | [SPARK-15670](https://issues.apache.org/jira/browse/SPARK-15670) | Improvement | Add deprecate annotation for acumulator V1 interface in JavaSparkContext class |
| 2.0.0 | [SPARK-15803](https://issues.apache.org/jira/browse/SPARK-15803) | Improvement | Support with statement syntax for SparkSession |
| 2.0.0 | [SPARK-16469](https://issues.apache.org/jira/browse/SPARK-16469) | Improvement | Long running Driver task while multiplying big matrices |
| 2.0.0 | [SPARK-16503](https://issues.apache.org/jira/browse/SPARK-16503) | New Feature | SparkSession should provide Spark version |
| 2.0.1 | [SPARK-15703](https://issues.apache.org/jira/browse/SPARK-15703) | Improvement | Make ListenerBus event queue size configurable |
| 2.0.1 | [SPARK-16870](https://issues.apache.org/jira/browse/SPARK-16870) | Improvement | add "spark.sql.broadcastTimeout" into docs/sql-programming-guide.md to help people to how to fix this timeout error when it happenned |
| 2.0.1 | [SPARK-16932](https://issues.apache.org/jira/browse/SPARK-16932) | Improvement | Programming-guide Accumulator section should be more clear w.r.t new API |
| 2.0.1 | [SPARK-17485](https://issues.apache.org/jira/browse/SPARK-17485) | Improvement | Failed remote cached block reads can lead to whole job failure |
| 2.0.1 | [SPARK-17649](https://issues.apache.org/jira/browse/SPARK-17649) | Improvement | Log how many Spark events got dropped in LiveListenerBus |
| 2.0.2 | [SPARK-17711](https://issues.apache.org/jira/browse/SPARK-17711) | New Feature | Compress rolled executor logs |
| 2.1.0 | [SPARK-928](https://issues.apache.org/jira/browse/SPARK-928) | Improvement | Add support for Unsafe-based serializer in Kryo 2.22 |
| 2.1.0 | [SPARK-4563](https://issues.apache.org/jira/browse/SPARK-4563) | Improvement | Allow spark driver to bind to different ip then advertise ip |
| 2.1.0 | [SPARK-10530](https://issues.apache.org/jira/browse/SPARK-10530) | Improvement | Kill other task attempts when one taskattempt belonging the same task is succeeded in speculation |
| 2.1.0 | [SPARK-13081](https://issues.apache.org/jira/browse/SPARK-13081) | Improvement | Allow set pythonExec of driver and executor through configuration |
| 2.1.0 | [SPARK-15660](https://issues.apache.org/jira/browse/SPARK-15660) | Improvement | Update RDD `variance/stdev` description and add popVariance/popStdev |
| 2.1.0 | [SPARK-15703](https://issues.apache.org/jira/browse/SPARK-15703) | Improvement | Make ListenerBus event queue size configurable |
| 2.1.0 | [SPARK-16159](https://issues.apache.org/jira/browse/SPARK-16159) | Improvement | Move RDD creation logic from FileSourceStrategy.apply |
| 2.1.0 | [SPARK-16395](https://issues.apache.org/jira/browse/SPARK-16395) | Improvement | Fail if too many CheckpointWriteHandlers are queued up in the fixed thread pool |
| 2.1.0 | [SPARK-16606](https://issues.apache.org/jira/browse/SPARK-16606) | Improvement | Misleading warning for SparkContext.getOrCreate "WARN SparkContext: Use an existing SparkContext, some configuration may not take effect." |
| 2.1.0 | [SPARK-16870](https://issues.apache.org/jira/browse/SPARK-16870) | Improvement | add "spark.sql.broadcastTimeout" into docs/sql-programming-guide.md to help people to how to fix this timeout error when it happenned |
| 2.1.0 | [SPARK-16932](https://issues.apache.org/jira/browse/SPARK-16932) | Improvement | Programming-guide Accumulator section should be more clear w.r.t new API |
| 2.1.0 | [SPARK-17365](https://issues.apache.org/jira/browse/SPARK-17365) | Improvement | Kill multiple executors together to reduce lock contention |
| 2.1.0 | [SPARK-17406](https://issues.apache.org/jira/browse/SPARK-17406) | Improvement | Event Timeline will be very slow when there are too many executor events |
| 2.1.0 | [SPARK-17451](https://issues.apache.org/jira/browse/SPARK-17451) | Improvement | CoarseGrainedExecutorBackend should inform driver before self-kill |
| 2.1.0 | [SPARK-17472](https://issues.apache.org/jira/browse/SPARK-17472) | Improvement | Better error message for serialization failures of large objects in Python |
| 2.1.0 | [SPARK-17485](https://issues.apache.org/jira/browse/SPARK-17485) | Improvement | Failed remote cached block reads can lead to whole job failure |
| 2.1.0 | [SPARK-17490](https://issues.apache.org/jira/browse/SPARK-17490) | Improvement | Optimize SerializeFromObject for primitive array |
| 2.1.0 | [SPARK-17648](https://issues.apache.org/jira/browse/SPARK-17648) | Improvement | TaskSchedulerImpl.resourceOffers should take an IndexedSeq, not a Seq |
| 2.1.0 | [SPARK-17649](https://issues.apache.org/jira/browse/SPARK-17649) | Improvement | Log how many Spark events got dropped in LiveListenerBus |
| 2.1.0 | [SPARK-17715](https://issues.apache.org/jira/browse/SPARK-17715) | Improvement | Log INFO per task launch creates a large driver log |
| 2.1.0 | [SPARK-17930](https://issues.apache.org/jira/browse/SPARK-17930) | Improvement | The SerializerInstance instance used when deserializing a TaskResult is not reused |
| 2.1.0 | [SPARK-18182](https://issues.apache.org/jira/browse/SPARK-18182) | New Feature | Expose ReplayListenerBus.replay() overload which accepts Iterator<String> |
| 2.1.0 | [SPARK-18188](https://issues.apache.org/jira/browse/SPARK-18188) | Improvement | Add checksum for block of broadcast |
| 2.1.0 | [SPARK-18337](https://issues.apache.org/jira/browse/SPARK-18337) | Improvement | Memory Sink should be able to recover from checkpoints in Complete OutputMode |
| 2.1.0 | [SPARK-18448](https://issues.apache.org/jira/browse/SPARK-18448) | Improvement | SparkSession should implement java.lang.AutoCloseable like JavaSparkContext |
| 2.1.0 | [SPARK-18575](https://issues.apache.org/jira/browse/SPARK-18575) | Improvement | Keep same style: adjust the position of driver log links |
| 2.1.0 | [SPARK-18751](https://issues.apache.org/jira/browse/SPARK-18751) | Improvement | Deadlock when SparkContext.stop is called in Utils.tryOrStopSparkContext |
| 2.2.0 | [SPARK-8425](https://issues.apache.org/jira/browse/SPARK-8425) | Improvement | Add blacklist mechanism for task scheduling |
| 2.2.0 | [SPARK-13369](https://issues.apache.org/jira/browse/SPARK-13369) | Improvement | Number of consecutive fetch failures for a stage before the job is aborted should be configurable |
| 2.2.0 | [SPARK-16554](https://issues.apache.org/jira/browse/SPARK-16554) | New Feature | Spark should kill executors when they are blacklisted |
| 2.2.0 | [SPARK-16929](https://issues.apache.org/jira/browse/SPARK-16929) | Improvement | Speculation-related synchronization bottleneck in checkSpeculatableTasks |
| 2.2.0 | [SPARK-17711](https://issues.apache.org/jira/browse/SPARK-17711) | New Feature | Compress rolled executor logs |
| 2.2.0 | [SPARK-17724](https://issues.apache.org/jira/browse/SPARK-17724) | Improvement | Unevaluated new lines in tooltip in DAG Visualization of a job |
| 2.2.0 | [SPARK-17769](https://issues.apache.org/jira/browse/SPARK-17769) | Improvement | Some FetchFailure refactoring in the DAGScheduler |
| 2.2.0 | [SPARK-17931](https://issues.apache.org/jira/browse/SPARK-17931) | Improvement | taskScheduler has some unneeded serialization |
| 2.2.0 | [SPARK-18191](https://issues.apache.org/jira/browse/SPARK-18191) | prose | RDD API ported to use commit protocol |
| 2.2.0 | [SPARK-18268](https://issues.apache.org/jira/browse/SPARK-18268) | Improvement | ALS.run fail with UnsupportedOperationException if run on an empty ratings RDD |
| 2.2.0 | [SPARK-18708](https://issues.apache.org/jira/browse/SPARK-18708) | Improvement | Improve documentation in SparkContext.scala file |
| 2.2.0 | [SPARK-18740](https://issues.apache.org/jira/browse/SPARK-18740) | Improvement | Log spark.app.name in driver log |
| 2.2.0 | [SPARK-18742](https://issues.apache.org/jira/browse/SPARK-18742) | Improvement | Clarify that user-defined BroadcastFactory is not supported |
| 2.2.0 | [SPARK-18761](https://issues.apache.org/jira/browse/SPARK-18761) | prose | Fix for uncancellable/unkillable tasks starving job resources |
| 2.2.0 | [SPARK-18975](https://issues.apache.org/jira/browse/SPARK-18975) | Improvement | Add an API to remove SparkListener from SparkContext |
| 2.2.0 | [SPARK-18991](https://issues.apache.org/jira/browse/SPARK-18991) | Improvement | Change ContextCleaner.referenceBuffer to ConcurrentHashMap to make it faster |
| 2.2.0 | [SPARK-19010](https://issues.apache.org/jira/browse/SPARK-19010) | Improvement | Include Kryo exception in case of overflow |
| 2.2.0 | [SPARK-19026](https://issues.apache.org/jira/browse/SPARK-19026) | Improvement | local directories cannot be cleanuped when create directory of "executor-***" throws IOException such as there is no more free disk space to create it etc. |
| 2.2.0 | [SPARK-19207](https://issues.apache.org/jira/browse/SPARK-19207) | Improvement | LocalSparkSession should use Slf4JLoggerFactory.INSTANCE instead of creating new object via constructor |
| 2.2.0 | [SPARK-19365](https://issues.apache.org/jira/browse/SPARK-19365) | Improvement | Optimize RequestMessage serialization |
| 2.2.0 | [SPARK-19398](https://issues.apache.org/jira/browse/SPARK-19398) | Improvement | Log in TaskSetManager is not correct |
| 2.2.0 | [SPARK-19466](https://issues.apache.org/jira/browse/SPARK-19466) | Improvement | Improve Fair Scheduler Logging |
| 2.2.0 | [SPARK-19516](https://issues.apache.org/jira/browse/SPARK-19516) | Improvement | update public doc to use SparkSession instead of SparkContext |
| 2.2.0 | [SPARK-19525](https://issues.apache.org/jira/browse/SPARK-19525) | Improvement | Enable Compression of RDD Checkpoints |
| 2.2.0 | [SPARK-19540](https://issues.apache.org/jira/browse/SPARK-19540) | Improvement | Add ability to clone SparkSession with an identical copy of the SessionState |
| 2.2.0 | [SPARK-19542](https://issues.apache.org/jira/browse/SPARK-19542) | Improvement | Delete the temp checkpoint if a query is stopped without errors |
| 2.2.0 | [SPARK-19679](https://issues.apache.org/jira/browse/SPARK-19679) | Improvement | Destroy broadcasted object without blocking |
| 2.2.0 | [SPARK-19757](https://issues.apache.org/jira/browse/SPARK-19757) | Improvement | Executor with task scheduled could be killed due to idleness |
| 2.2.0 | [SPARK-19777](https://issues.apache.org/jira/browse/SPARK-19777) | Improvement | Scan runningTasksSet when check speculatable tasks in TaskSetManager. |
| 2.2.0 | [SPARK-19876](https://issues.apache.org/jira/browse/SPARK-19876) | Improvement | Add OneTime trigger executor |
| 2.2.0 | [SPARK-19889](https://issues.apache.org/jira/browse/SPARK-19889) | Improvement | Make TaskContext callbacks synchronized |
| 2.2.0 | [SPARK-19998](https://issues.apache.org/jira/browse/SPARK-19998) | Improvement | BlockRDD block not found Exception add RDD id info |
| 2.2.0 | [SPARK-19999](https://issues.apache.org/jira/browse/SPARK-19999) | Improvement | Test failures in Spark Core due to java.nio.Bits.unaligned() |
| 2.2.0 | [SPARK-20084](https://issues.apache.org/jira/browse/SPARK-20084) | Improvement | Remove internal.metrics.updatedBlockStatuses accumulator from history files |
| 2.2.0 | [SPARK-20148](https://issues.apache.org/jira/browse/SPARK-20148) | Improvement | Extend the file commit interface to allow subscribing to task commit messages |
| 2.2.0 | [SPARK-20284](https://issues.apache.org/jira/browse/SPARK-20284) | Improvement | Make SerializationStream and DeserializationStream extend Closeable |
| 2.2.0 | [SPARK-20344](https://issues.apache.org/jira/browse/SPARK-20344) | Improvement | Duplicate call in FairSchedulableBuilder.addTaskSetManager |
| 2.2.0 | [SPARK-20410](https://issues.apache.org/jira/browse/SPARK-20410) | Improvement | Make SparkConf a def instead of a val in SharedSQLContext |
| 2.2.0 | [SPARK-20955](https://issues.apache.org/jira/browse/SPARK-20955) | Improvement | A lot of duplicated "executorId" strings in "TaskUIData"s |
| 2.2.0 | [SPARK-21060](https://issues.apache.org/jira/browse/SPARK-21060) | Improvement | Css style about paging function is error in the executor page. |
| 2.4.0 | [SPARK-24374](https://issues.apache.org/jira/browse/SPARK-24374) | prose | Barrier Execution Mode in the scheduler |
| 3.0.0 | [SPARK-13704](https://issues.apache.org/jira/browse/SPARK-13704) | Improvement | TaskSchedulerImpl.createTaskSetManager can be expensive, and result in lost executors due to blocked heartbeats |
| 3.0.0 | [SPARK-16775](https://issues.apache.org/jira/browse/SPARK-16775) | Improvement | Remove deprecated accumulator v1 APIs |
| 3.0.0 | [SPARK-18161](https://issues.apache.org/jira/browse/SPARK-18161) | Improvement | Default PickleSerializer pickle protocol doesn't handle > 4GB objects |
| 3.0.0 | [SPARK-19147](https://issues.apache.org/jira/browse/SPARK-19147) | Improvement | Gracefully handle error in task after executor is stopped |
| 3.0.0 | [SPARK-24203](https://issues.apache.org/jira/browse/SPARK-24203) | Improvement | Make executor's bindAddress configurable |
| 3.0.0 | [SPARK-24615](https://issues.apache.org/jira/browse/SPARK-24615) | Epic | SPIP: Accelerator-aware task scheduling for Spark |
| 3.0.0 | [SPARK-25429](https://issues.apache.org/jira/browse/SPARK-25429) | Improvement | SparkListenerBus inefficient due to 'LiveStageMetrics#accumulatorIds:Array[Long]' data structure |
| 3.0.0 | [SPARK-25449](https://issues.apache.org/jira/browse/SPARK-25449) | Improvement | Don't send zero accumulators in heartbeats |
| 3.0.0 | [SPARK-25560](https://issues.apache.org/jira/browse/SPARK-25560) | New Feature | Allow Function Injection in SparkSessionExtensions |
| 3.0.0 | [SPARK-25773](https://issues.apache.org/jira/browse/SPARK-25773) | Improvement | Cancel zombie tasks in a result stage when the job finishes |
| 3.0.0 | [SPARK-25839](https://issues.apache.org/jira/browse/SPARK-25839) | Improvement | Implement use of KryoPool in KryoSerializer |
| 3.0.0 | [SPARK-25875](https://issues.apache.org/jira/browse/SPARK-25875) | Improvement | Merge code to set up driver features for different languages |
| 3.0.0 | [SPARK-25885](https://issues.apache.org/jira/browse/SPARK-25885) | Improvement | HighlyCompressedMapStatus deserialization optimization |
| 3.0.0 | [SPARK-25998](https://issues.apache.org/jira/browse/SPARK-25998) | Improvement | TorrentBroadcast holds strong reference to broadcast object |
| 3.0.0 | [SPARK-26060](https://issues.apache.org/jira/browse/SPARK-26060) | Improvement | Track SparkConf entries and make SET command reject such entries. |
| 3.0.0 | [SPARK-26285](https://issues.apache.org/jira/browse/SPARK-26285) | Improvement | Add a metric source for accumulators (aka AccumulatorSource) |
| 3.0.0 | [SPARK-26312](https://issues.apache.org/jira/browse/SPARK-26312) | Improvement | Converting converters in RDDConversions into arrays to improve their access performance |
| 3.0.0 | [SPARK-26329](https://issues.apache.org/jira/browse/SPARK-26329) | Improvement | ExecutorMetrics should poll faster than heartbeats |
| 3.0.0 | [SPARK-26340](https://issues.apache.org/jira/browse/SPARK-26340) | Improvement | Ensure cores per executor is greater than cpu per task |
| 3.0.0 | [SPARK-26389](https://issues.apache.org/jira/browse/SPARK-26389) | Improvement | Add force delete temp checkpoint configuration |
| 3.0.0 | [SPARK-26446](https://issues.apache.org/jira/browse/SPARK-26446) | Improvement | Add cachedExecutorIdleTimeout docs at ExecutorAllocationManager |
| 3.0.0 | [SPARK-26601](https://issues.apache.org/jira/browse/SPARK-26601) | Improvement | Make broadcast-exchange thread pool keepalivetime and maxThreadNumber configurable |
| 3.0.0 | [SPARK-26632](https://issues.apache.org/jira/browse/SPARK-26632) | Improvement | Separate Thread Configurations of Driver and Executor |
| 3.0.0 | [SPARK-26633](https://issues.apache.org/jira/browse/SPARK-26633) | Improvement | Add ExecutorClassLoader.getResourceAsStream |
| 3.0.0 | [SPARK-26755](https://issues.apache.org/jira/browse/SPARK-26755) | Improvement | Optimize Spark Scheduler to dequeue speculative tasks more efficiently |
| 3.0.0 | [SPARK-26774](https://issues.apache.org/jira/browse/SPARK-26774) | Improvement | Document threading concerns in TaskSchedulerImpl |
| 3.0.0 | [SPARK-27210](https://issues.apache.org/jira/browse/SPARK-27210) | Improvement | Cleanup incomplete output files in ManifestFileCommitProtocol if task is aborted |
| 3.0.0 | [SPARK-27254](https://issues.apache.org/jira/browse/SPARK-27254) | Improvement | Cleanup complete but becoming invalid output files in ManifestFileCommitProtocol if job is aborted |
| 3.0.0 | [SPARK-27366](https://issues.apache.org/jira/browse/SPARK-27366) | Story | Spark scheduler internal changes to support GPU scheduling |
| 3.0.0 | [SPARK-27474](https://issues.apache.org/jira/browse/SPARK-27474) | Improvement | avoid retrying a task failed with CommitDeniedException many times |
| 3.0.0 | [SPARK-27488](https://issues.apache.org/jira/browse/SPARK-27488) | Story | Driver interface to support GPU resources |
| 3.0.0 | [SPARK-27636](https://issues.apache.org/jira/browse/SPARK-27636) | Improvement | Remove cached RDD blocks after PIC execution |
| 3.0.0 | [SPARK-27666](https://issues.apache.org/jira/browse/SPARK-27666) | Improvement | Do not release lock while TaskContext already completed |
| 3.0.0 | [SPARK-27777](https://issues.apache.org/jira/browse/SPARK-27777) | Improvement | Eliminate uncessary sliding job in AreaUnderCurve |
| 3.0.0 | [SPARK-27787](https://issues.apache.org/jira/browse/SPARK-27787) | Improvement | Eliminate uncessary job to compute SSreg |
| 3.0.0 | [SPARK-27811](https://issues.apache.org/jira/browse/SPARK-27811) | Improvement | Docs of spark.driver.memoryOverhead and spark.executor.memoryOverhead exists a little ambiguity |
| 3.0.0 | [SPARK-27835](https://issues.apache.org/jira/browse/SPARK-27835) | Story | Resource Scheduling: change driver config from addresses to resourcesFile |
| 3.0.0 | [SPARK-27989](https://issues.apache.org/jira/browse/SPARK-27989) | Improvement | Add retries on the connection to the driver |
| 3.0.0 | [SPARK-28355](https://issues.apache.org/jira/browse/SPARK-28355) | Improvement | Use Spark conf for threshold at which UDF is compressed by broadcast |
| 3.0.0 | [SPARK-28366](https://issues.apache.org/jira/browse/SPARK-28366) | Improvement | Logging in driver when loading single large unsplittable file |
| 3.0.0 | [SPARK-28561](https://issues.apache.org/jira/browse/SPARK-28561) | Improvement | DAG viz for barrier-execution mode |
| 3.0.0 | [SPARK-28577](https://issues.apache.org/jira/browse/SPARK-28577) | Improvement | Ensure executorMemoryHead requested value not less than MEMORY_OFFHEAP_SIZE when MEMORY_OFFHEAP_ENABLED is true |
| 3.0.0 | [SPARK-28639](https://issues.apache.org/jira/browse/SPARK-28639) | Improvement | Configuration doc for Barrier Execution Mode |
| 3.0.0 | [SPARK-28676](https://issues.apache.org/jira/browse/SPARK-28676) | Improvement | Avoid Excessive logging from ContextCleaner |
| 3.0.0 | [SPARK-28769](https://issues.apache.org/jira/browse/SPARK-28769) | Improvement | Improve warning message in Barrier Execution Mode in case required slots > maximum slots |
| 3.0.0 | [SPARK-28843](https://issues.apache.org/jira/browse/SPARK-28843) | Improvement | Set OMP_NUM_THREADS to executor cores reduce Python memory consumption |
| 3.0.0 | [SPARK-28929](https://issues.apache.org/jira/browse/SPARK-28929) | Improvement | Spark Logging level should be INFO instead of Debug in Executor Plugin API[SPARK-24918] |
| 3.0.0 | [SPARK-29081](https://issues.apache.org/jira/browse/SPARK-29081) | Improvement | Replace calls to SerializationUtils.clone on properties with a faster implementation. |
| 3.0.0 | [SPARK-29151](https://issues.apache.org/jira/browse/SPARK-29151) | Story | Support fraction resources for task resource scheduling |
| 3.0.0 | [SPARK-29161](https://issues.apache.org/jira/browse/SPARK-29161) | Improvement | Unify default wait time for LiveListenerBus.waitUntilEmpty |
| 3.0.0 | [SPARK-29287](https://issues.apache.org/jira/browse/SPARK-29287) | Improvement | Executors should not receive any offers before they are actually constructed |
| 3.0.0 | [SPARK-29306](https://issues.apache.org/jira/browse/SPARK-29306) | Story | Executors need to track what ResourceProfile they are created with |
| 3.0.0 | [SPARK-29347](https://issues.apache.org/jira/browse/SPARK-29347) | New Feature | External Row should be JSON serializable |
| 3.0.0 | [SPARK-29396](https://issues.apache.org/jira/browse/SPARK-29396) | New Feature | Extend Spark plugin interface to driver |
| 3.0.0 | [SPARK-29415](https://issues.apache.org/jira/browse/SPARK-29415) | Story | Stage Level Sched: Add base ResourceProfile and Request classes |
| 3.0.0 | [SPARK-29417](https://issues.apache.org/jira/browse/SPARK-29417) | Story | Resource Scheduling - add TaskContext.resource java api |
| 3.0.0 | [SPARK-29434](https://issues.apache.org/jira/browse/SPARK-29434) | New Feature | Improve the MapStatuses serialization performance |
| 3.0.0 | [SPARK-29499](https://issues.apache.org/jira/browse/SPARK-29499) | Improvement | Add mapPartitionsWithIndex for RDDBarrier |
| 3.0.0 | [SPARK-29649](https://issues.apache.org/jira/browse/SPARK-29649) | Improvement | Stop task set if FileAlreadyExistsException was thrown when writing to output file |
| 3.0.0 | [SPARK-29976](https://issues.apache.org/jira/browse/SPARK-29976) | Improvement | Allow speculation even if there is only one task |
| 3.0.0 | [SPARK-30355](https://issues.apache.org/jira/browse/SPARK-30355) | Improvement | Unify isExecutorActive between CoarseGrainedSchedulerBackend and DriverEndpoint |
| 3.0.0 | [SPARK-30359](https://issues.apache.org/jira/browse/SPARK-30359) | Improvement | Do not clear executorsPendingToRemove in CoarseGrainedSchedulerBackend.reset |
| 3.0.0 | [SPARK-30379](https://issues.apache.org/jira/browse/SPARK-30379) | Improvement | Avoid OOM when using collection accumulator |
| 3.0.0 | [SPARK-30529](https://issues.apache.org/jira/browse/SPARK-30529) | Improvement | Improve error messages when Executor dies before registering with driver |
| 3.0.0 | [SPARK-30729](https://issues.apache.org/jira/browse/SPARK-30729) | Improvement | Eagerly filter out zombie TaskSetManager before offering resources |
| 3.0.0 | [SPARK-31565](https://issues.apache.org/jira/browse/SPARK-31565) | Improvement | Unify the font color of label among all DAG-viz. |
| 3.1.1 | [SPARK-27495](https://issues.apache.org/jira/browse/SPARK-27495) | Epic | SPIP: Support Stage level resource configuration and scheduling |
| 3.2.0 | [SPARK-24818](https://issues.apache.org/jira/browse/SPARK-24818) | New Feature | Ensure all the barrier tasks in the same stage are launched together |
| 3.2.0 | [SPARK-32484](https://issues.apache.org/jira/browse/SPARK-32484) | Improvement | Not accurate Log Info in BroadcastExchangeExec.scala |
| 3.2.0 | [SPARK-33741](https://issues.apache.org/jira/browse/SPARK-33741) | Improvement | Add minimum threshold speculation config |
| 3.2.0 | [SPARK-34245](https://issues.apache.org/jira/browse/SPARK-34245) | Improvement | Master may not remove the finished executor when Worker fails to send ExecutorStateChanged |
| 3.2.0 | [SPARK-34355](https://issues.apache.org/jira/browse/SPARK-34355) | Improvement | Add log and time cost for commit job |
| 3.2.0 | [SPARK-34426](https://issues.apache.org/jira/browse/SPARK-34426) | Improvement | Add driver and executors POD logs to integration tests log when the test fails |
| 3.2.0 | [SPARK-34779](https://issues.apache.org/jira/browse/SPARK-34779) | Improvement | ExecutorMetricsPoller should keep stage entry in stageTCMP until a heartbeat occurs |
| 3.2.0 | [SPARK-35083](https://issues.apache.org/jira/browse/SPARK-35083) | Improvement | Support remote scheduler pool file |
| 3.2.0 | [SPARK-35092](https://issues.apache.org/jira/browse/SPARK-35092) | Improvement | the auto-generated rdd's name in the storage tab should be truncated if it is too long. |
| 3.2.0 | [SPARK-35182](https://issues.apache.org/jira/browse/SPARK-35182) | Improvement | Support driver-owned on-demand PVC |
| 3.2.0 | [SPARK-35200](https://issues.apache.org/jira/browse/SPARK-35200) | Improvement | Avoid to recompute the pending speculative tasks in the ExecutorAllocationManager and remove unnecessary code |
| 3.2.0 | [SPARK-35234](https://issues.apache.org/jira/browse/SPARK-35234) | Improvement | Reserve format of the stage failureMessage |
| 3.2.0 | [SPARK-35240](https://issues.apache.org/jira/browse/SPARK-35240) | Improvement | Use CheckpointFileManager for checkpoint manipulation |
| 3.2.0 | [SPARK-35380](https://issues.apache.org/jira/browse/SPARK-35380) | New Feature | Support loading SparkSessionExtensions from ServiceLoader |
| 3.2.0 | [SPARK-35404](https://issues.apache.org/jira/browse/SPARK-35404) | Improvement | Name the timers in TaskSchedulerImpl |
| 3.2.0 | [SPARK-35552](https://issues.apache.org/jira/browse/SPARK-35552) | Improvement | Make query stage materialized more readable |
| 3.2.0 | [SPARK-35683](https://issues.apache.org/jira/browse/SPARK-35683) | Improvement | Fix Index.difference to avoid collect 'other' to driver side |
| 3.2.0 | [SPARK-35714](https://issues.apache.org/jira/browse/SPARK-35714) | Improvement | Bug fix for deadlock during the executor shutdown |
| 3.2.0 | [SPARK-36919](https://issues.apache.org/jira/browse/SPARK-36919) | Improvement | Make BadRecordException serializable |
| 4.1.2 | [SPARK-56235](https://issues.apache.org/jira/browse/SPARK-56235) | Improvement | TaskSetManager.executorLost() O(N) scan over taskInfos causes DriverEndpoint thread stall with large task counts |
<!-- AUTO:timeline END -->
