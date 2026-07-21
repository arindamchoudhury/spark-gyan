# DStreams (legacy streaming)

> Source: `release-notes/spark_all_changelogs.txt`. Timeline rows below are generated from `_catalog.jsonl`; prose outside the AUTO markers is hand-written.

## How it evolved

### 0.x era — origins

Spark Streaming debuted as an alpha extension in 0.7.0, offering `map`/`filter`/`reduce` plus sliding-window functions over streams, with fault recovery that gave exactly-once semantics without relying on external transactions. 0.7.3 updated the Kafka and Twitter input APIs, moving Twitter to OAuth after the old username/password method was disabled and letting Kafka receive non-string messages.

0.8.0 finished the OAuth migration for Twitter and added operators like `transformWith`, `leftInnerJoin`, and `rightOuterJoin` in 0.8.1. Spark Streaming graduated out of alpha in 0.9.0, gaining simplified high availability (driver auto-recovery through the standalone cluster's HA mode), a `StreamingListener` interface for monitoring, `awaitTermination()`/`stop()` lifecycle controls, windowed operators sped up 30-50%, and input source plugins (Twitter, Kafka, Flume) split into separate Maven modules.

## Timeline

<!-- AUTO:timeline START -->
| Release | JIRA | Type | Title |
|---|---|---|---|
| 0.7.0 | — | prose | Spark Streaming alpha introduced |
| 0.7.0 | — | prose | Streaming API: map/filter/reduce plus sliding window functions |
| 0.7.0 | — | prose | Streaming fault recovery with exactly-once semantics, no external transactions |
| 0.7.3 | — | prose | Streaming Kafka/Twitter APIs updated (OAuth, non-string Kafka messages) |
| 0.8.0 | — | prose | Streaming Twitter API updated to use OAuth authentication |
| 0.8.1 | — | prose | New Streaming operators: transformWith, leftInnerJoin, rightOuterJoin |
| 0.9.0 | — | prose | Spark Streaming graduates out of alpha with simplified HA and optimizations |
| 0.9.0 | — | prose | Streaming driver auto-recovery via standalone cluster HA mode |
| 0.9.0 | — | prose | Windowed operators sped up by 30-50% |
| 0.9.0 | — | prose | Streaming input source plugins split into separate Maven modules |
| 0.9.0 | — | prose | New StreamingListener interface for monitoring streaming statistics |
| 0.9.0 | — | prose | StreamingContext.awaitTermination() waits for shutdown and surfaces exceptions |
| 0.9.0 | — | prose | StreamingContext.stop() can stop streaming without stopping the SparkContext |
| 1.5.0 | [SPARK-5048](https://issues.apache.org/jira/browse/SPARK-5048) | Improvement | Add Flume to the Python Streaming API |
| 1.5.0 | [SPARK-7988](https://issues.apache.org/jira/browse/SPARK-7988) | New Feature | Mechanism to control receiver scheduling |
| 1.5.0 | [SPARK-8378](https://issues.apache.org/jira/browse/SPARK-8378) | Improvement | Add Spark Flume Python API |
| 1.5.0 | [SPARK-8564](https://issues.apache.org/jira/browse/SPARK-8564) | New Feature | Add the Python API for Kinesis |
| 1.5.0 | [SPARK-8630](https://issues.apache.org/jira/browse/SPARK-8630) | Improvement | Prevent from checkpointing QueueInputDStream |
| 1.5.0 | [SPARK-8882](https://issues.apache.org/jira/browse/SPARK-8882) | New Feature | A New Receiver Scheduling Mechanism to solve unbalanced receivers |
| 1.5.0 | [SPARK-9215](https://issues.apache.org/jira/browse/SPARK-9215) | Improvement | Implement WAL-free Kinesis receiver that give at-least once guarantee |
| 1.5.0 | [SPARK-9572](https://issues.apache.org/jira/browse/SPARK-9572) | Improvement | Add StreamingContext.getActiveOrCreate() to python API |
| 1.5.0 | [SPARK-9727](https://issues.apache.org/jira/browse/SPARK-9727) | Improvement | Make the Kinesis project SBT name and consistent with other streaming projects |
| 1.5.0 | [SPARK-10137](https://issues.apache.org/jira/browse/SPARK-10137) | Improvement | Avoid to restart receivers if scheduleReceivers returns balanced results |
| 1.5.0 | [SPARK-10148](https://issues.apache.org/jira/browse/SPARK-10148) | Improvement | Display active and inactive receiver numbers in Streaming page |
| 1.6.0 | [SPARK-2629](https://issues.apache.org/jira/browse/SPARK-2629) | Epic | Improved state management for Spark Streaming (mapWithState) |
| 1.6.0 | [SPARK-4557](https://issues.apache.org/jira/browse/SPARK-4557) | Improvement | Spark Streaming' foreachRDD method should accept a VoidFunction<...>, not a Function<..., Void> |
| 1.6.0 | [SPARK-10071](https://issues.apache.org/jira/browse/SPARK-10071) | Improvement | QueueInputDStream Should Allow Checkpointing |
| 1.6.0 | [SPARK-10889](https://issues.apache.org/jira/browse/SPARK-10889) | Improvement | Upgrade Kinesis Client Library |
| 1.6.0 | [SPARK-10891](https://issues.apache.org/jira/browse/SPARK-10891) | Improvement | Add MessageHandler to KinesisUtils.createStream similar to Direct Kafka |
| 1.6.0 | [SPARK-11127](https://issues.apache.org/jira/browse/SPARK-11127) | Improvement | Upgrade Kinesis Client Library to the latest stable version |
| 1.6.0 | [SPARK-11141](https://issues.apache.org/jira/browse/SPARK-11141) | Improvement | Batching of ReceivedBlockTrackerLogEvents for efficient WAL writes |
| 1.6.0 | [SPARK-11198](https://issues.apache.org/jira/browse/SPARK-11198) | New Feature | Support record de-aggregation in KinesisReceiver |
| 1.6.0 | [SPARK-11212](https://issues.apache.org/jira/browse/SPARK-11212) | Improvement | Make RDD's preferred locations support the executor location and fix ReceiverTracker for multiple executors in a host |
| 1.6.0 | [SPARK-11290](https://issues.apache.org/jira/browse/SPARK-11290) | Improvement | Implement trackStateByKey for improved state management |
| 1.6.0 | [SPARK-11324](https://issues.apache.org/jira/browse/SPARK-11324) | Improvement | Flag to close Write Ahead Log after writing |
| 1.6.0 | [SPARK-11333](https://issues.apache.org/jira/browse/SPARK-11333) | Improvement | Add the receiver's executor information to UI |
| 1.6.0 | [SPARK-11361](https://issues.apache.org/jira/browse/SPARK-11361) | Improvement | Show scopes of RDD operations inside DStream.foreachRDD and DStream.transform in DAG viz |
| 1.6.0 | [SPARK-11419](https://issues.apache.org/jira/browse/SPARK-11419) | Improvement | WriteAheadLog recovery improvements for when closeFileAfterWrite is enabled |
| 1.6.0 | [SPARK-11663](https://issues.apache.org/jira/browse/SPARK-11663) | Improvement | Add Java API for trackStateByKey |
| 1.6.0 | [SPARK-11731](https://issues.apache.org/jira/browse/SPARK-11731) | Improvement | Enable batching on Driver WriteAheadLog by default |
| 1.6.0 | [SPARK-11814](https://issues.apache.org/jira/browse/SPARK-11814) | Improvement | Set better default DStream checkpoint interval |
| 2.0.0 | [SPARK-7661](https://issues.apache.org/jira/browse/SPARK-7661) | New Feature | Support for dynamic allocation of resources in Kinesis Spark Streaming |
| 2.0.0 | [SPARK-8393](https://issues.apache.org/jira/browse/SPARK-8393) | Improvement | JavaStreamingContext#awaitTermination() throws non-declared InterruptedException |
| 2.0.0 | [SPARK-10320](https://issues.apache.org/jira/browse/SPARK-10320) | New Feature | Kafka Support new topic subscriptions without requiring restart of the streaming context |
| 2.0.0 | [SPARK-11713](https://issues.apache.org/jira/browse/SPARK-11713) | New Feature | Initial RDD for updateStateByKey for pyspark |
| 2.0.0 | [SPARK-12177](https://issues.apache.org/jira/browse/SPARK-12177) | Improvement | Update KafkaDStreams to new Kafka 0.10 Consumer API |
| 2.0.0 | [SPARK-12273](https://issues.apache.org/jira/browse/SPARK-12273) | Improvement | Spark Streaming Web UI does not list Receivers in order |
| 2.0.0 | [SPARK-12304](https://issues.apache.org/jira/browse/SPARK-12304) | Improvement | Make Spark Streaming web UI display more friendly Receiver graphs |
| 2.0.0 | [SPARK-12425](https://issues.apache.org/jira/browse/SPARK-12425) | Improvement | DStream union optimisation |
| 2.0.0 | [SPARK-13211](https://issues.apache.org/jira/browse/SPARK-13211) | Improvement | StreamingContext throws NoSuchElementException when created from non-existent checkpoint directory |
| 2.0.0 | [SPARK-13280](https://issues.apache.org/jira/browse/SPARK-13280) | Improvement | FileBasedWriteAheadLog logger name should be under o.a.s namespace |
| 2.0.0 | [SPARK-13569](https://issues.apache.org/jira/browse/SPARK-13569) | Improvement | Kafka DStreams from wildcard topic filters |
| 2.0.0 | [SPARK-14976](https://issues.apache.org/jira/browse/SPARK-14976) | Improvement | make StreamingContext.textFileStream support wildcard |
| 2.0.1 | [SPARK-17569](https://issues.apache.org/jira/browse/SPARK-17569) | Improvement | Don't recheck existence of files when generating File Relation resolution in StructuredStreaming |
| 2.0.1 | [SPARK-17638](https://issues.apache.org/jira/browse/SPARK-17638) | Improvement | Stop JVM StreamingContext when the Python process is dead |
| 2.1.0 | [SPARK-13027](https://issues.apache.org/jira/browse/SPARK-13027) | Improvement | Add API for updateStateByKey to provide batch time as input |
| 2.1.0 | [SPARK-17569](https://issues.apache.org/jira/browse/SPARK-17569) | Improvement | Don't recheck existence of files when generating File Relation resolution in StructuredStreaming |
| 2.1.0 | [SPARK-17638](https://issues.apache.org/jira/browse/SPARK-17638) | Improvement | Stop JVM StreamingContext when the Python process is dead |
| 2.2.0 | [SPARK-18809](https://issues.apache.org/jira/browse/SPARK-18809) | Improvement | Kinesis deaggregation issue on master |
| 2.2.0 | [SPARK-19304](https://issues.apache.org/jira/browse/SPARK-19304) | Improvement | Kinesis checkpoint recovery is 10x slow |
| 2.2.0 | [SPARK-19405](https://issues.apache.org/jira/browse/SPARK-19405) | Improvement | Add support to KinesisUtils for cross-account Kinesis reads via STS |
| 2.2.0 | [SPARK-19911](https://issues.apache.org/jira/browse/SPARK-19911) | New Feature | Add builder interface for Kinesis DStreams |
| 3.0.0 | [SPARK-17159](https://issues.apache.org/jira/browse/SPARK-17159) | Improvement | Improve FileInputDStream.findNewFiles list performance |
| 3.0.0 | [SPARK-25778](https://issues.apache.org/jira/browse/SPARK-25778) | Improvement | WriteAheadLogBackedBlockRDD in YARN Cluster Mode Fails due lack of access to tmpDir from $PWD to HDFS |
| 3.0.0 | [SPARK-25871](https://issues.apache.org/jira/browse/SPARK-25871) | Improvement | Streaming WAL should not use hdfs erasure coding, regardless of FS defaults |
| 3.0.0 | [SPARK-26094](https://issues.apache.org/jira/browse/SPARK-26094) | Improvement | Streaming WAL should create parent dirs |
| 3.0.0 | [SPARK-27420](https://issues.apache.org/jira/browse/SPARK-27420) | Improvement | KinesisInputDStream should expose a way to configure CloudWatch metrics |
| 3.0.0 | [SPARK-29677](https://issues.apache.org/jira/browse/SPARK-29677) | Improvement | Upgrade Kinesis Client |
| 3.0.0 | [SPARK-30901](https://issues.apache.org/jira/browse/SPARK-30901) | Improvement | [DOC] In streaming-kinesis-integration.md, the initialPosition method changed |
| 3.2.0 | [SPARK-34383](https://issues.apache.org/jira/browse/SPARK-34383) | Improvement | Optimize WAL commit phase on SS |
<!-- AUTO:timeline END -->
