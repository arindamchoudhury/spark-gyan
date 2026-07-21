# Structured Streaming

> Source: `release-notes/spark_all_changelogs.txt`. Timeline rows below are generated from `_catalog.jsonl`; prose outside the AUTO markers is hand-written.

## How it evolved

_TODO: connective prose added during the era passes._

## Timeline

<!-- AUTO:timeline START -->
| Release | JIRA | Type | Title |
|---|---|---|---|
| 1.5.0 | [SPARK-8701](https://issues.apache.org/jira/browse/SPARK-8701) | Improvement | Add input metadata to InputInfo and display it in the batch page |
| 2.0.0 | [SPARK-14214](https://issues.apache.org/jira/browse/SPARK-14214) | Improvement | Update State Store to give a more get/put hashmap-style interface |
| 2.0.0 | [SPARK-16031](https://issues.apache.org/jira/browse/SPARK-16031) | New Feature | Add debug-only socket source in Structured Streaming |
| 2.0.0 | [SPARK-16061](https://issues.apache.org/jira/browse/SPARK-16061) | Improvement | The property "spark.streaming.stateStore.maintenanceInterval" should be renamed to "spark.sql.streaming.stateStore.maintenanceInterval" |
| 2.0.1 | [SPARK-17640](https://issues.apache.org/jira/browse/SPARK-17640) | Improvement | Avoid using -1 as the default batchId for FileStreamSource.FileEntry |
| 2.0.2 | [SPARK-17780](https://issues.apache.org/jira/browse/SPARK-17780) | Improvement | Report NoClassDefFoundError in StreamExecution |
| 2.0.2 | [SPARK-18044](https://issues.apache.org/jira/browse/SPARK-18044) | Improvement | FileStreamSource should not infer partitions in every batch |
| 2.1.0 | [SPARK-8360](https://issues.apache.org/jira/browse/SPARK-8360) | Umbrella | Structured Streaming (aka Streaming DataFrames) |
| 2.1.0 | [SPARK-15406](https://issues.apache.org/jira/browse/SPARK-15406) | New Feature | Structured streaming support for consuming from Kafka |
| 2.1.0 | [SPARK-15472](https://issues.apache.org/jira/browse/SPARK-15472) | New Feature | Add support for writing partitioned `csv`, `json`, `text` formats in Structured Streaming |
| 2.1.0 | [SPARK-16411](https://issues.apache.org/jira/browse/SPARK-16411) | Improvement | Add textFile API to structured streaming. |
| 2.1.0 | [SPARK-17510](https://issues.apache.org/jira/browse/SPARK-17510) | Improvement | Set Streaming MaxRate Independently For Multiple Streams |
| 2.1.0 | [SPARK-17640](https://issues.apache.org/jira/browse/SPARK-17640) | Improvement | Avoid using -1 as the default batchId for FileStreamSource.FileEntry |
| 2.1.0 | [SPARK-17780](https://issues.apache.org/jira/browse/SPARK-17780) | Improvement | Report NoClassDefFoundError in StreamExecution |
| 2.1.0 | [SPARK-18044](https://issues.apache.org/jira/browse/SPARK-18044) | Improvement | FileStreamSource should not infer partitions in every batch |
| 2.1.0 | [SPARK-18124](https://issues.apache.org/jira/browse/SPARK-18124) | New Feature | Observed delay based event time watermarks |
| 2.1.0 | [SPARK-18493](https://issues.apache.org/jira/browse/SPARK-18493) | Improvement | Add withWatermark and checkpoint to python dataframe |
| 2.1.0 | [SPARK-18498](https://issues.apache.org/jira/browse/SPARK-18498) | Improvement | Clean up HDFSMetadataLog API for better testing |
| 2.1.0 | [SPARK-18513](https://issues.apache.org/jira/browse/SPARK-18513) | Improvement | Record and recover watermark |
| 2.1.0 | [SPARK-18694](https://issues.apache.org/jira/browse/SPARK-18694) | Improvement | Add StreamingQuery.explain and exception to Python and fix StreamingQueryException |
| 2.1.0 | [SPARK-18734](https://issues.apache.org/jira/browse/SPARK-18734) | Improvement | Represent timestamp in StreamingQueryProgress as formatted string instead of millis |
| 2.1.0 | [SPARK-18754](https://issues.apache.org/jira/browse/SPARK-18754) | Improvement | Rename recentProgresses to recentProgress |
| 2.1.0 | [SPARK-18811](https://issues.apache.org/jira/browse/SPARK-18811) | Improvement | Stream Source resolution should happen in StreamExecution thread, not main thread |
| 2.1.0 | [SPARK-18834](https://issues.apache.org/jira/browse/SPARK-18834) | Improvement | Expose event time time stats through StreamingQueryProgress |
| 2.1.0 | [SPARK-18852](https://issues.apache.org/jira/browse/SPARK-18852) | Improvement | StreamingQuery.lastProgress should be null when recentProgress is empty |
| 2.2.0 | [SPARK-18234](https://issues.apache.org/jira/browse/SPARK-18234) | New Feature | Update mode in structured streaming |
| 2.2.0 | [SPARK-18669](https://issues.apache.org/jira/browse/SPARK-18669) | Improvement | Update Apache docs regard watermarking in Structured Streaming |
| 2.2.0 | [SPARK-18985](https://issues.apache.org/jira/browse/SPARK-18985) | Improvement | Add missing @InterfaceStability.Evolving for Structured Streaming APIs |
| 2.2.0 | [SPARK-19041](https://issues.apache.org/jira/browse/SPARK-19041) | Improvement | Fix code snippet compilation issues in Structured Streaming Programming Guide |
| 2.2.0 | [SPARK-19067](https://issues.apache.org/jira/browse/SPARK-19067) | New Feature | mapGroupsWithState - arbitrary stateful operations with Structured Streaming (similar to DStream.mapWithState) |
| 2.2.0 | [SPARK-19074](https://issues.apache.org/jira/browse/SPARK-19074) | Improvement | Update Structured Streaming Programming guide for Update Mode |
| 2.2.0 | [SPARK-19140](https://issues.apache.org/jira/browse/SPARK-19140) | Improvement | Allow update mode for non-aggregation streaming queries |
| 2.2.0 | [SPARK-19168](https://issues.apache.org/jira/browse/SPARK-19168) | Improvement | StateStore should be aborted upon error |
| 2.2.0 | [SPARK-19497](https://issues.apache.org/jira/browse/SPARK-19497) | New Feature | dropDuplicates with watermark |
| 2.2.0 | [SPARK-19599](https://issues.apache.org/jira/browse/SPARK-19599) | Improvement | Clean up HDFSMetadataLog |
| 2.2.0 | [SPARK-19719](https://issues.apache.org/jira/browse/SPARK-19719) | New Feature | Structured Streaming write to Kafka |
| 3.0.0 | [SPARK-20568](https://issues.apache.org/jira/browse/SPARK-20568) | New Feature | Delete files after processing in structured streaming |
| 3.0.0 | [SPARK-23539](https://issues.apache.org/jira/browse/SPARK-23539) | New Feature | Add support for Kafka headers in Structured Streaming |
| 3.0.0 | [SPARK-26121](https://issues.apache.org/jira/browse/SPARK-26121) | Improvement | [Structured Streaming] Allow users to define prefix of Kafka's consumer group (group.id) |
| 3.0.0 | [SPARK-26170](https://issues.apache.org/jira/browse/SPARK-26170) | Improvement | Add missing metrics in FlatMapGroupsWithState |
| 3.0.0 | [SPARK-26649](https://issues.apache.org/jira/browse/SPARK-26649) | New Feature | Noop Streaming Sink using DSV2 |
| 3.0.0 | [SPARK-26848](https://issues.apache.org/jira/browse/SPARK-26848) | Improvement | Introduce new option to Kafka source - specify timestamp to start and end offset |
| 3.0.0 | [SPARK-26949](https://issues.apache.org/jira/browse/SPARK-26949) | Improvement | Prevent "purge" to remove needed batch files in CompactibleFileStreamLog |
| 3.0.0 | [SPARK-27579](https://issues.apache.org/jira/browse/SPARK-27579) | Improvement | remove BaseStreamingSource and BaseStreamingSink |
| 3.0.0 | [SPARK-27933](https://issues.apache.org/jira/browse/SPARK-27933) | Improvement | Extracting common purge "behaviour" to the parent StreamExecution |
| 3.0.0 | [SPARK-28695](https://issues.apache.org/jira/browse/SPARK-28695) | Improvement | Make Kafka source more robust with CaseInsensitiveMap |
| 3.0.0 | [SPARK-29345](https://issues.apache.org/jira/browse/SPARK-29345) | Epic | Add an API that allows a user to define and observe arbitrary metrics on batch and streaming queries |
| 3.0.0 | [SPARK-29352](https://issues.apache.org/jira/browse/SPARK-29352) | Improvement | Move active streaming query state to the SharedState |
| 3.0.0 | [SPARK-29423](https://issues.apache.org/jira/browse/SPARK-29423) | Improvement | leak on org.apache.spark.sql.execution.streaming.StreamingQueryListenerBus |
| 3.0.0 | [SPARK-29543](https://issues.apache.org/jira/browse/SPARK-29543) | New Feature | Support Structured Streaming UI |
| 3.0.0 | [SPARK-29635](https://issues.apache.org/jira/browse/SPARK-29635) | Improvement | Deduplicate test suites between Kafka micro-batch sink and Kafka continuous sink |
| 3.0.0 | [SPARK-30143](https://issues.apache.org/jira/browse/SPARK-30143) | Improvement | StreamingQuery.stop() should not block indefinitely |
| 3.0.0 | [SPARK-30656](https://issues.apache.org/jira/browse/SPARK-30656) | Improvement | Support the "minPartitions" option in Kafka batch source and streaming source v1 |
| 3.0.0 | [SPARK-30669](https://issues.apache.org/jira/browse/SPARK-30669) | Improvement | Introduce AdmissionControl API to Structured Streaming |
| 3.0.0 | [SPARK-30804](https://issues.apache.org/jira/browse/SPARK-30804) | Improvement | Measure and log elapsed time for "compact" operation in CompactibleFileStreamLog |
| 3.0.0 | [SPARK-30927](https://issues.apache.org/jira/browse/SPARK-30927) | Improvement | StreamingQueryManager should avoid keeping reference to terminated StreamingQuery |
| 3.0.0 | [SPARK-30943](https://issues.apache.org/jira/browse/SPARK-30943) | Improvement | Show "batch ID" in tool tip string for Structured Streaming UI graphs |
| 3.0.0 | [SPARK-31004](https://issues.apache.org/jira/browse/SPARK-31004) | Improvement | Show message for empty Streaming Queries instead of empty timelines and histograms. |
| 3.0.0 | [SPARK-31324](https://issues.apache.org/jira/browse/SPARK-31324) | Improvement | StreamingQuery stop() timeout exception should include the stream ID |
| 3.0.0 | [SPARK-31792](https://issues.apache.org/jira/browse/SPARK-31792) | Improvement | Introduce the structured streaming UI in the Web UI page |
| 3.2.0 | [SPARK-10816](https://issues.apache.org/jira/browse/SPARK-10816) | New Feature | EventTime based sessionization (session window) |
| 3.2.0 | [SPARK-29223](https://issues.apache.org/jira/browse/SPARK-29223) | Improvement | Kafka source: offset by timestamp - allow specifying timestamp for "all partitions" |
| 3.2.0 | [SPARK-33660](https://issues.apache.org/jira/browse/SPARK-33660) | Improvement | Update Kafka Headers Documentation in Structured Streaming |
| 3.2.0 | [SPARK-33827](https://issues.apache.org/jira/browse/SPARK-33827) | Improvement | Unload State Store asap once it becomes inactive |
| 3.2.0 | [SPARK-34297](https://issues.apache.org/jira/browse/SPARK-34297) | Improvement | Add metrics for data loss and offset out range for KafkaMicroBatchStream |
| 3.2.0 | [SPARK-34482](https://issues.apache.org/jira/browse/SPARK-34482) | Improvement | Correct the active SparkSession for streaming query |
| 3.2.0 | [SPARK-34854](https://issues.apache.org/jira/browse/SPARK-34854) | Improvement | Report metrics for streaming source through progress reporter with Kafka source use-case |
| 3.2.0 | [SPARK-35312](https://issues.apache.org/jira/browse/SPARK-35312) | Improvement | Introduce new Option in Kafka source to specify minimum number of records to read per trigger |
| 3.2.0 | [SPARK-35421](https://issues.apache.org/jira/browse/SPARK-35421) | Improvement | Remove redundant ProjectExec from streaming queries with V2Relation |
| 3.2.0 | [SPARK-35763](https://issues.apache.org/jira/browse/SPARK-35763) | Improvement | Minor refactor of StateStoreCustomMetric |
| 3.2.0 | [SPARK-35799](https://issues.apache.org/jira/browse/SPARK-35799) | Improvement | Fix the allUpdatesTimeMs metric measuring in FlatMapGroupsWithStateExec |
| 3.2.0 | [SPARK-35800](https://issues.apache.org/jira/browse/SPARK-35800) | New Feature | Improving testability of GroupState in streaming flatMapGroupsWithState |
| 3.2.0 | [SPARK-35880](https://issues.apache.org/jira/browse/SPARK-35880) | Improvement | [SS] Track the number of duplicates dropped in streaming dedupe operator |
| 3.2.0 | [SPARK-35896](https://issues.apache.org/jira/browse/SPARK-35896) | Improvement | [SS] Include more granular metrics for stateful operators in StreamingQueryProgress |
| 3.2.0 | [SPARK-35897](https://issues.apache.org/jira/browse/SPARK-35897) | Improvement | Support user defined initial state with flatMapGroupsWithState in Structured Streaming |
| 3.2.0 | [SPARK-36314](https://issues.apache.org/jira/browse/SPARK-36314) | Improvement | Update Sessionization example to use native support of session window |
| 3.2.0 | [SPARK-36455](https://issues.apache.org/jira/browse/SPARK-36455) | Improvement | Provide an example of complex session window via flatMapGroupsWithState |
<!-- AUTO:timeline END -->
