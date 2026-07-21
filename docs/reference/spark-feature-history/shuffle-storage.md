# Shuffle / Storage / Memory

> Source: `release-notes/spark_all_changelogs.txt`. Timeline rows below are generated from `_catalog.jsonl`; prose outside the AUTO markers is hand-written.

## How it evolved

_TODO: connective prose added during the era passes._

## Timeline

<!-- AUTO:timeline START -->
| Release | JIRA | Type | Title |
|---|---|---|---|
| 1.5.0 | [SPARK-5423](https://issues.apache.org/jira/browse/SPARK-5423) | Improvement | ExternalAppendOnlyMap won't delete temp spilled file if some exception happens during using it |
| 1.5.0 | [SPARK-7075](https://issues.apache.org/jira/browse/SPARK-7075) | Epic | Project Tungsten (Spark 1.5 Phase 1) |
| 1.5.0 | [SPARK-7855](https://issues.apache.org/jira/browse/SPARK-7855) | Improvement | Move hash-style shuffle code out of ExternalSorter and into own file |
| 1.5.0 | [SPARK-7884](https://issues.apache.org/jira/browse/SPARK-7884) | Improvement | Move block deserialization from BlockStoreShuffleFetcher to ShuffleReader |
| 1.5.0 | [SPARK-8101](https://issues.apache.org/jira/browse/SPARK-8101) | Improvement | Upgrade netty to avoid memory leak accord to netty #3837 issues |
| 1.5.0 | [SPARK-8160](https://issues.apache.org/jira/browse/SPARK-8160) | Improvement | Tungsten style external aggregation |
| 1.5.0 | [SPARK-8317](https://issues.apache.org/jira/browse/SPARK-8317) | Improvement | Do not push sort into shuffle in Exchange operator |
| 1.5.0 | [SPARK-8319](https://issues.apache.org/jira/browse/SPARK-8319) | Improvement | Update logic related to key ordering in shuffle dependencies |
| 1.5.0 | [SPARK-8873](https://issues.apache.org/jira/browse/SPARK-8873) | Improvement | Support cleaning up shuffle files when using shuffle service in Mesos |
| 1.5.0 | [SPARK-8875](https://issues.apache.org/jira/browse/SPARK-8875) | Improvement | Shuffle code cleanup: remove BlockStoreShuffleFetcher class |
| 1.5.0 | [SPARK-9247](https://issues.apache.org/jira/browse/SPARK-9247) | Improvement | Use BytesToBytesMap in unsafe broadcast join |
| 1.5.0 | [SPARK-9411](https://issues.apache.org/jira/browse/SPARK-9411) | Improvement | Make page size configurable |
| 1.5.0 | [SPARK-9412](https://issues.apache.org/jira/browse/SPARK-9412) | Improvement | Support records larger than a page size |
| 1.5.0 | [SPARK-9418](https://issues.apache.org/jira/browse/SPARK-9418) | Improvement | Use sort-merge join as the default shuffle join |
| 1.5.0 | [SPARK-9548](https://issues.apache.org/jira/browse/SPARK-9548) | Improvement | BytesToBytesMap could have a destructive iterator |
| 1.5.0 | [SPARK-9641](https://issues.apache.org/jira/browse/SPARK-9641) | Improvement | spark.shuffle.service.port is not documented |
| 1.5.0 | [SPARK-9700](https://issues.apache.org/jira/browse/SPARK-9700) | Improvement | Pick default page size more intelligently |
| 1.5.0 | [SPARK-9703](https://issues.apache.org/jira/browse/SPARK-9703) | Improvement | EnsureRequirements should not add unnecessary shuffles when only ordering requirements are unsatisfied |
| 1.6.0 | [SPARK-4849](https://issues.apache.org/jira/browse/SPARK-4849) | Improvement | Pass partitioning information (distribute by) to In-memory caching |
| 1.6.0 | [SPARK-5354](https://issues.apache.org/jira/browse/SPARK-5354) | Improvement | Set InMemoryColumnarTableScan's outputPartitioning and outputOrdering |
| 1.6.0 | [SPARK-7542](https://issues.apache.org/jira/browse/SPARK-7542) | New Feature | Support off-heap sort buffer in UnsafeExternalSorter |
| 1.6.0 | [SPARK-9043](https://issues.apache.org/jira/browse/SPARK-9043) | Improvement | Serialize key, value and combiner classes in ShuffleDependency |
| 1.6.0 | [SPARK-9702](https://issues.apache.org/jira/browse/SPARK-9702) | Improvement | Repartition operator should use Exchange to perform its shuffle |
| 1.6.0 | [SPARK-9923](https://issues.apache.org/jira/browse/SPARK-9923) | Improvement | ShuffleMapStage.numAvailableOutputs should be an Int instead of Long |
| 1.6.0 | [SPARK-10000](https://issues.apache.org/jira/browse/SPARK-10000) | Story | Consolidate storage and execution memory management |
| 1.6.0 | [SPARK-10065](https://issues.apache.org/jira/browse/SPARK-10065) | Improvement | Avoid triple copy of var-length objects in Array in tungsten projection |
| 1.6.0 | [SPARK-10451](https://issues.apache.org/jira/browse/SPARK-10451) | Improvement | Prevent unnecessary serializations in InMemoryColumnarTableScan |
| 1.6.0 | [SPARK-10745](https://issues.apache.org/jira/browse/SPARK-10745) | Improvement | Separate configs between shuffle and RPC |
| 1.6.0 | [SPARK-10917](https://issues.apache.org/jira/browse/SPARK-10917) | Improvement | Improve performance of complex types in columnar cache |
| 1.6.0 | [SPARK-11149](https://issues.apache.org/jira/browse/SPARK-11149) | Improvement | Improve performance of primitive types in columnar cache |
| 1.6.0 | [SPARK-11256](https://issues.apache.org/jira/browse/SPARK-11256) | Improvement | Mark all Stage/ResultStage/ShuffleMapStage internal state as private. |
| 1.6.0 | [SPARK-11389](https://issues.apache.org/jira/browse/SPARK-11389) | New Feature | Add support for off-heap memory to MemoryManager |
| 1.6.0 | [SPARK-11767](https://issues.apache.org/jira/browse/SPARK-11767) | Improvement | Easy to OOM when cache large column |
| 1.6.0 | [SPARK-12251](https://issues.apache.org/jira/browse/SPARK-12251) | Improvement | Document Spark 1.6's off-heap memory configurations and add config validation |
| 2.0.0 | [SPARK-1239](https://issues.apache.org/jira/browse/SPARK-1239) | Improvement | Improve fetching of map output statuses |
| 2.0.0 | [SPARK-6166](https://issues.apache.org/jira/browse/SPARK-6166) | Improvement | Limit number of in flight outbound requests for shuffle fetch |
| 2.0.0 | [SPARK-10985](https://issues.apache.org/jira/browse/SPARK-10985) | Improvement | Avoid passing evicted blocks throughout BlockManager / CacheManager |
| 2.0.0 | [SPARK-11262](https://issues.apache.org/jira/browse/SPARK-11262) | Improvement | Unit test for gradient, loss layers, memory management for multilayer perceptron |
| 2.0.0 | [SPARK-12130](https://issues.apache.org/jira/browse/SPARK-12130) | Improvement | Replace shuffleManagerClass with shortShuffleMgrNames in ExternalShuffleBlockResolver |
| 2.0.0 | [SPARK-12317](https://issues.apache.org/jira/browse/SPARK-12317) | Improvement | Support configurate value for AUTO_BROADCASTJOIN_THRESHOLD and SHUFFLE_TARGET_POSTSHUFFLE_INPUT_SIZE with unit(e.g. kb/mb/gb) in SQLConf |
| 2.0.0 | [SPARK-12400](https://issues.apache.org/jira/browse/SPARK-12400) | Improvement | Avoid writing a shuffle file if a partition has no output (empty) |
| 2.0.0 | [SPARK-12730](https://issues.apache.org/jira/browse/SPARK-12730) | Improvement | De-duplicate some test code in BlockManagerSuite |
| 2.0.0 | [SPARK-12817](https://issues.apache.org/jira/browse/SPARK-12817) | Improvement | Remove CacheManager and replace it with new BlockManager.getOrElseUpdate method |
| 2.0.0 | [SPARK-12914](https://issues.apache.org/jira/browse/SPARK-12914) | Improvement | Generate TungstenAggregate with grouping keys |
| 2.0.0 | [SPARK-12950](https://issues.apache.org/jira/browse/SPARK-12950) | Improvement | Improve performance of BytesToBytesMap |
| 2.0.0 | [SPARK-12951](https://issues.apache.org/jira/browse/SPARK-12951) | Improvement | Support spilling in generate aggregate |
| 2.0.0 | [SPARK-13057](https://issues.apache.org/jira/browse/SPARK-13057) | Improvement | Add benchmark codes and the performance results for implemented compression schemes for InMemoryRelation |
| 2.0.0 | [SPARK-13136](https://issues.apache.org/jira/browse/SPARK-13136) | Improvement | Data exchange (shuffle, broadcast) should only be handled by the exchange operator |
| 2.0.0 | [SPARK-13347](https://issues.apache.org/jira/browse/SPARK-13347) | Improvement | Reuse the shuffle for duplicated exchange |
| 2.0.0 | [SPARK-13503](https://issues.apache.org/jira/browse/SPARK-13503) | Improvement | Support to specify the (writing) option for compression codec for TEXT |
| 2.0.0 | [SPARK-13528](https://issues.apache.org/jira/browse/SPARK-13528) | Improvement | Make the short names of compression codecs consistent in spark |
| 2.0.0 | [SPARK-13695](https://issues.apache.org/jira/browse/SPARK-13695) | Improvement | Don't cache MEMORY_AND_DISK blocks as bytes in memory store when reading spills |
| 2.0.0 | [SPARK-13696](https://issues.apache.org/jira/browse/SPARK-13696) | Improvement | Remove BlockStore interface to more cleanly reflect different memory and disk store responsibilities |
| 2.0.0 | [SPARK-13833](https://issues.apache.org/jira/browse/SPARK-13833) | Improvement | Guard against race condition when re-caching spilled bytes in memory |
| 2.0.0 | [SPARK-13990](https://issues.apache.org/jira/browse/SPARK-13990) | Improvement | Automatically pick serializer when caching RDDs |
| 2.0.0 | [SPARK-13992](https://issues.apache.org/jira/browse/SPARK-13992) | New Feature | Add support for off-heap caching |
| 2.0.0 | [SPARK-14007](https://issues.apache.org/jira/browse/SPARK-14007) | Improvement | Manage the memory for hash map for shuffle hash join |
| 2.0.0 | [SPARK-14052](https://issues.apache.org/jira/browse/SPARK-14052) | Improvement | Build BytesToBytesMap in HashedRelation |
| 2.0.0 | [SPARK-14075](https://issues.apache.org/jira/browse/SPARK-14075) | Improvement | Refactor MemoryStore to be testable independent of BlockManager |
| 2.0.0 | [SPARK-14135](https://issues.apache.org/jira/browse/SPARK-14135) | New Feature | Add off-heap storage memory bookkeeping support to MemoryManager |
| 2.0.0 | [SPARK-14717](https://issues.apache.org/jira/browse/SPARK-14717) | Improvement | Scala, Python APIs for Dataset.unpersist differ in default blocking value |
| 2.0.0 | [SPARK-14836](https://issues.apache.org/jira/browse/SPARK-14836) | Improvement | Zip local jars before uploading to distributed cache |
| 2.0.0 | [SPARK-14863](https://issues.apache.org/jira/browse/SPARK-14863) | Improvement | Cache TreeNode's hashCode |
| 2.0.0 | [SPARK-14951](https://issues.apache.org/jira/browse/SPARK-14951) | Improvement | Subexpression elimination in wholestage codegen version of TungstenAggregate |
| 2.0.0 | [SPARK-14966](https://issues.apache.org/jira/browse/SPARK-14966) | Improvement | SizeEstimator should ignore classes in the scala.reflect package |
| 2.0.0 | [SPARK-15045](https://issues.apache.org/jira/browse/SPARK-15045) | Improvement | Remove dead code in TaskMemoryManager.cleanUpAllAllocatedMemory for pageTable |
| 2.0.0 | [SPARK-15121](https://issues.apache.org/jira/browse/SPARK-15121) | Improvement | Improve logging of external shuffle handler |
| 2.0.0 | [SPARK-16023](https://issues.apache.org/jira/browse/SPARK-16023) | Improvement | Move InMemoryRelation to its own file |
| 2.0.1 | [SPARK-17480](https://issues.apache.org/jira/browse/SPARK-17480) | Improvement | CompressibleColumnBuilder inefficiently call gatherCompressibilityStats |
| 2.0.1 | [SPARK-17483](https://issues.apache.org/jira/browse/SPARK-17483) | Improvement | Minor refactoring and cleanup in BlockManager block status reporting and block removal |
| 2.0.1 | [SPARK-17484](https://issues.apache.org/jira/browse/SPARK-17484) | Improvement | Race condition when cancelling a job during a cache write can lead to block fetch failures |
| 2.1.0 | [SPARK-5581](https://issues.apache.org/jira/browse/SPARK-5581) | Improvement | When writing sorted map output file, avoid open / close between each partition |
| 2.1.0 | [SPARK-5682](https://issues.apache.org/jira/browse/SPARK-5682) | New Feature | Add encrypted shuffle in spark |
| 2.1.0 | [SPARK-14963](https://issues.apache.org/jira/browse/SPARK-14963) | Improvement | YarnShuffleService should use YARN getRecoveryPath() for leveldb location |
| 2.1.0 | [SPARK-15074](https://issues.apache.org/jira/browse/SPARK-15074) | Improvement | Spark shuffle service bottlenecked while fetching large amount of intermediate data |
| 2.1.0 | [SPARK-15263](https://issues.apache.org/jira/browse/SPARK-15263) | Improvement | Make shuffle service dir cleanup faster by using `rm -rf` |
| 2.1.0 | [SPARK-15994](https://issues.apache.org/jira/browse/SPARK-15994) | Improvement | Allow enabling Mesos fetch cache in coarse executor backend |
| 2.1.0 | [SPARK-16696](https://issues.apache.org/jira/browse/SPARK-16696) | Improvement | unused broadcast variables should call destroy instead of unpersist |
| 2.1.0 | [SPARK-17480](https://issues.apache.org/jira/browse/SPARK-17480) | Improvement | CompressibleColumnBuilder inefficiently call gatherCompressibilityStats |
| 2.1.0 | [SPARK-17483](https://issues.apache.org/jira/browse/SPARK-17483) | Improvement | Minor refactoring and cleanup in BlockManager block status reporting and block removal |
| 2.1.0 | [SPARK-17484](https://issues.apache.org/jira/browse/SPARK-17484) | Improvement | Race condition when cancelling a job during a cache write can lead to block fetch failures |
| 2.1.0 | [SPARK-17524](https://issues.apache.org/jira/browse/SPARK-17524) | Improvement | RowBasedKeyValueBatchSuite always uses 64 mb page size |
| 2.1.0 | [SPARK-17839](https://issues.apache.org/jira/browse/SPARK-17839) | Improvement | Use Nio's directbuffer instead of BufferedInputStream in order to avoid additional copy from os buffer cache to user buffer |
| 2.1.0 | [SPARK-18490](https://issues.apache.org/jira/browse/SPARK-18490) | Improvement | duplicate nodename extrainfo of ShuffleExchange |
| 2.1.0 | [SPARK-18557](https://issues.apache.org/jira/browse/SPARK-18557) | Improvement | Downgrade the memory leak warning message |
| 2.2.0 | [SPARK-17019](https://issues.apache.org/jira/browse/SPARK-17019) | Improvement | Expose off-heap memory usage in various places |
| 2.2.0 | [SPARK-18744](https://issues.apache.org/jira/browse/SPARK-18744) | Improvement | Remove workaround for Netty memory leak |
| 2.2.0 | [SPARK-19244](https://issues.apache.org/jira/browse/SPARK-19244) | Improvement | Sort MemoryConsumers according to their memory usage when spilling |
| 2.2.0 | [SPARK-19537](https://issues.apache.org/jira/browse/SPARK-19537) | Improvement | Move the pendingPartitions variable from Stage to ShuffleMapStage |
| 2.2.0 | [SPARK-19659](https://issues.apache.org/jira/browse/SPARK-19659) | Improvement | Fetch big blocks to disk when shuffle-read |
| 2.2.0 | [SPARK-19693](https://issues.apache.org/jira/browse/SPARK-19693) | Improvement | SET mapreduce.job.reduces automatically converted to spark.sql.shuffle.partitions |
| 2.2.0 | [SPARK-20741](https://issues.apache.org/jira/browse/SPARK-20741) | Improvement | SparkSubmit does not clean up after uploading spark_libs to the distributed cache |
| 2.2.0 | [SPARK-20868](https://issues.apache.org/jira/browse/SPARK-20868) | Improvement | UnsafeShuffleWriter should verify the position after FileChannel.transferTo |
| 2.2.0 | [SPARK-21090](https://issues.apache.org/jira/browse/SPARK-21090) | Improvement | Optimize the unified memory manager code |
| 3.0.0 | [SPARK-24355](https://issues.apache.org/jira/browse/SPARK-24355) | Improvement | Improve Spark shuffle server responsiveness to non-ChunkFetch requests |
| 3.0.0 | [SPARK-25118](https://issues.apache.org/jira/browse/SPARK-25118) | Improvement | Need a solution to persist Spark application console outputs when running in shell/yarn client mode |
| 3.0.0 | [SPARK-25341](https://issues.apache.org/jira/browse/SPARK-25341) | Improvement | Support rolling back a shuffle map stage and re-generate the shuffle files |
| 3.0.0 | [SPARK-25641](https://issues.apache.org/jira/browse/SPARK-25641) | Improvement | Change the spark.shuffle.server.chunkFetchHandlerThreadsPercent default to 100 |
| 3.0.0 | [SPARK-25900](https://issues.apache.org/jira/browse/SPARK-25900) | Improvement | When the the page number is more than the total page size, then fall back to the first page |
| 3.0.0 | [SPARK-25905](https://issues.apache.org/jira/browse/SPARK-25905) | Improvement | BlockManager should expose getRemoteManagedBuffer to avoid creating bytebuffers |
| 3.0.0 | [SPARK-25947](https://issues.apache.org/jira/browse/SPARK-25947) | Improvement | Reduce memory usage in ShuffleExchangeExec by selecting only the sort columns |
| 3.0.0 | [SPARK-26089](https://issues.apache.org/jira/browse/SPARK-26089) | Improvement | Handle large corrupt shuffle blocks |
| 3.0.0 | [SPARK-26287](https://issues.apache.org/jira/browse/SPARK-26287) | Improvement | Don't need to create an empty spill file when memory has no records |
| 3.0.0 | [SPARK-26288](https://issues.apache.org/jira/browse/SPARK-26288) | New Feature | add initRegisteredExecutorsDB in ExternalShuffleService |
| 3.0.0 | [SPARK-26289](https://issues.apache.org/jira/browse/SPARK-26289) | Improvement | cleanup enablePerfMetrics parameter from BytesToBytesMap |
| 3.0.0 | [SPARK-26525](https://issues.apache.org/jira/browse/SPARK-26525) | Improvement | Fast release memory of ShuffleBlockFetcherIterator |
| 3.0.0 | [SPARK-26697](https://issues.apache.org/jira/browse/SPARK-26697) | Improvement | ShuffleBlockFetcherIterator can log block sizes in addition to num blocks |
| 3.0.0 | [SPARK-26768](https://issues.apache.org/jira/browse/SPARK-26768) | Improvement | Remove useless code in BlockManager |
| 3.0.0 | [SPARK-26771](https://issues.apache.org/jira/browse/SPARK-26771) | Improvement | Make .unpersist(), .destroy() consistently non-blocking by default |
| 3.0.0 | [SPARK-27056](https://issues.apache.org/jira/browse/SPARK-27056) | Improvement | Remove `start-shuffle-service.sh` |
| 3.0.0 | [SPARK-27147](https://issues.apache.org/jira/browse/SPARK-27147) | Improvement | Create new unit test cases for SortShuffleWriter |
| 3.0.0 | [SPARK-27610](https://issues.apache.org/jira/browse/SPARK-27610) | Improvement | Yarn external shuffle service fails to start when spark.shuffle.io.mode=EPOLL |
| 3.0.0 | [SPARK-27622](https://issues.apache.org/jira/browse/SPARK-27622) | Improvement | Avoid the network when block manager fetches disk persisted RDD blocks from the same host |
| 3.0.0 | [SPARK-27651](https://issues.apache.org/jira/browse/SPARK-27651) | Improvement | Avoid the network when block manager fetches shuffle blocks from the same host |
| 3.0.0 | [SPARK-27665](https://issues.apache.org/jira/browse/SPARK-27665) | Improvement | Split fetch shuffle blocks protocol from OpenBlocks |
| 3.0.0 | [SPARK-27677](https://issues.apache.org/jira/browse/SPARK-27677) | New Feature | Disk-persisted RDD blocks served by shuffle service, and ignored for Dynamic Allocation |
| 3.0.0 | [SPARK-27773](https://issues.apache.org/jira/browse/SPARK-27773) | Improvement | Add shuffle service metric for number of exceptions caught in ExternalShuffleBlockHandler |
| 3.0.0 | [SPARK-27963](https://issues.apache.org/jira/browse/SPARK-27963) | New Feature | Allow dynamic allocation without an external shuffle service |
| 3.0.0 | [SPARK-28118](https://issues.apache.org/jira/browse/SPARK-28118) | Improvement | Add `spark.eventLog.compression.codec` configuration |
| 3.0.0 | [SPARK-28154](https://issues.apache.org/jira/browse/SPARK-28154) | Improvement | GMM fix double caching |
| 3.0.0 | [SPARK-28593](https://issues.apache.org/jira/browse/SPARK-28593) | Improvement | Rename ShuffleClient to BlockStoreClient which more close to its usage |
| 3.0.0 | [SPARK-29182](https://issues.apache.org/jira/browse/SPARK-29182) | Improvement | Cache preferred locations of checkpointed RDD |
| 3.0.0 | [SPARK-29298](https://issues.apache.org/jira/browse/SPARK-29298) | Improvement | Separate block manager heartbeat endpoint from driver endpoint |
| 3.0.0 | [SPARK-29351](https://issues.apache.org/jira/browse/SPARK-29351) | New Feature | Avoid full synchronization in ShuffleMapStage |
| 3.0.0 | [SPARK-29576](https://issues.apache.org/jira/browse/SPARK-29576) | New Feature | Use Spark's CompressionCodec for Ser/Deser of MapOutputStatus |
| 3.0.0 | [SPARK-29655](https://issues.apache.org/jira/browse/SPARK-29655) | Improvement | Enable adaptive execution should not add more ShuffleExchange |
| 3.0.0 | [SPARK-29686](https://issues.apache.org/jira/browse/SPARK-29686) | Improvement | LinearSVC should persist instances if needed |
| 3.0.0 | [SPARK-29820](https://issues.apache.org/jira/browse/SPARK-29820) | Improvement | Use GitHub Action Cache for `./.m2/repository` |
| 3.0.0 | [SPARK-29939](https://issues.apache.org/jira/browse/SPARK-29939) | Improvement | Add spark.shuffle.mapStatus.compression.codec conf |
| 3.0.0 | [SPARK-31055](https://issues.apache.org/jira/browse/SPARK-31055) | Improvement | Update config docs for shuffle local host reads to have dep on external shuffle service |
| 3.0.0 | [SPARK-31259](https://issues.apache.org/jira/browse/SPARK-31259) | Improvement | Fix log error of curRequestSize in ShuffleBlockFetcherIterator |
| 3.0.0 | [SPARK-31442](https://issues.apache.org/jira/browse/SPARK-31442) | Improvement | Print shuffle id at coalesce partitions target size |
| 3.0.0 | [SPARK-31619](https://issues.apache.org/jira/browse/SPARK-31619) | Improvement | Rename config name "spark.dynamicAllocation.shuffleTimeout" to "spark.dynamicAllocation.shuffleTracking.timeout" |
| 3.0.0 | [SPARK-31646](https://issues.apache.org/jira/browse/SPARK-31646) | Improvement | Remove unused registeredConnections counter from ShuffleMetrics |
| 3.2.0 | [SPARK-29330](https://issues.apache.org/jira/browse/SPARK-29330) | Improvement | Allow users to chose the name of Spark Shuffle service |
| 3.2.0 | [SPARK-30602](https://issues.apache.org/jira/browse/SPARK-30602) | Improvement | SPIP: Support push-based shuffle to improve shuffle efficiency |
| 3.2.0 | [SPARK-32384](https://issues.apache.org/jira/browse/SPARK-32384) | Improvement | repartitionAndSortWithinPartitions avoid shuffle with same partitioner |
| 3.2.0 | [SPARK-33817](https://issues.apache.org/jira/browse/SPARK-33817) | Improvement | Use a logical plan to cache instead of dataframe |
| 3.2.0 | [SPARK-33857](https://issues.apache.org/jira/browse/SPARK-33857) | Improvement | Unify random functions and make Uuid Shuffle support seed in SQL |
| 3.2.0 | [SPARK-34142](https://issues.apache.org/jira/browse/SPARK-34142) | New Feature | Support Fallback Storage Cleanup during stopping SparkContext |
| 3.2.0 | [SPARK-34206](https://issues.apache.org/jira/browse/SPARK-34206) | Improvement | Make Guava Cache to ExecutorPodsLifecycleManager private field |
| 3.2.0 | [SPARK-34278](https://issues.apache.org/jira/browse/SPARK-34278) | Improvement | Make BlockManagerMaster driver heartbeat timeout configurable |
| 3.2.0 | [SPARK-34307](https://issues.apache.org/jira/browse/SPARK-34307) | Improvement | TakeOrderedAndProjectExec avoid shuffle if input rdd has single partition |
| 3.2.0 | [SPARK-34325](https://issues.apache.org/jira/browse/SPARK-34325) | Improvement | remove_shuffleBlockResolver_in_SortShuffleWriter |
| 3.2.0 | [SPARK-34353](https://issues.apache.org/jira/browse/SPARK-34353) | Improvement | CollectLimitExec avoid shuffle if input rdd has single partition |
| 3.2.0 | [SPARK-34828](https://issues.apache.org/jira/browse/SPARK-34828) | Improvement | YARN Shuffle Service: Support configurability of aux service name and service-specific config overrides |
| 3.2.0 | [SPARK-34915](https://issues.apache.org/jira/browse/SPARK-34915) | Improvement | Cache Maven, SBT and Scala in all jobs that use them |
| 3.2.0 | [SPARK-35049](https://issues.apache.org/jira/browse/SPARK-35049) | Improvement | Remove unused MapOutputTracker in BlockStoreShuffleReader |
| 3.2.0 | [SPARK-35263](https://issues.apache.org/jira/browse/SPARK-35263) | Improvement | Refactor ShuffleBlockFetcherIteratorSuite to reduce duplicated code |
| 3.2.0 | [SPARK-35354](https://issues.apache.org/jira/browse/SPARK-35354) | Improvement | Minor cleanup to replace BaseJoinExec with ShuffledJoin in CoalesceBucketsInJoin |
| 3.2.0 | [SPARK-35396](https://issues.apache.org/jira/browse/SPARK-35396) | Improvement | Support to manual close/release entries in MemoryStore and InMemoryRelation instead of replying on GC |
| 3.2.0 | [SPARK-35416](https://issues.apache.org/jira/browse/SPARK-35416) | Improvement | Support PersistentVolumeClaim Reuse |
| 3.2.0 | [SPARK-35447](https://issues.apache.org/jira/browse/SPARK-35447) | Improvement | optimize skew join before coalescing shuffle partitions |
| 3.2.0 | [SPARK-35593](https://issues.apache.org/jira/browse/SPARK-35593) | New Feature | Support shuffle data recovery on the reused PVCs |
| 3.2.0 | [SPARK-35654](https://issues.apache.org/jira/browse/SPARK-35654) | Improvement | Allow ShuffleDataIO control DiskBlockManager.deleteFilesOnStop |
| 3.2.0 | [SPARK-35661](https://issues.apache.org/jira/browse/SPARK-35661) | Improvement | Allow deserialized off-heap memory entry |
| 3.2.0 | [SPARK-35675](https://issues.apache.org/jira/browse/SPARK-35675) | Improvement | EnsureRequirements remove shuffle should respect PartitioningCollection |
| 3.2.0 | [SPARK-36105](https://issues.apache.org/jira/browse/SPARK-36105) | Improvement | OptimizeLocalShuffleReader support reading data of multiple mappers in one task |
| 3.2.0 | [SPARK-36217](https://issues.apache.org/jira/browse/SPARK-36217) | Improvement | Rename CustomShuffleReader and OptimizeLocalShuffleReader |
| 3.2.0 | [SPARK-36221](https://issues.apache.org/jira/browse/SPARK-36221) | Improvement | Make sure CustomShuffleReaderExec has at least one partition |
| 4.1.1 | [SPARK-54850](https://issues.apache.org/jira/browse/SPARK-54850) | Improvement | Improve extractShuffleIds to find AdaptiveSparkPlanExec anywhere in plan tree |
<!-- AUTO:timeline END -->
