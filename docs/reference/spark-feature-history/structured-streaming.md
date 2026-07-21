# Structured Streaming

> Source: `release-notes/spark_all_changelogs.txt`. Timeline rows below are generated from `_catalog.jsonl`; prose outside the AUTO markers is hand-written.

## How it evolved

### 2.x era — Structured Streaming is born

Structured Streaming did not exist before 2.0.0: it shipped as an experimental high-level streaming API built on Spark SQL and Catalyst (SPARK-8360), letting streaming sources and sinks be programmed against with the same DataFrame/Dataset code as static data, with query plans incrementalized automatically. 2.1.0 added Kafka 0.10 support (SPARK-17346), event-time watermarking (SPARK-18124), and runtime metrics (SPARK-17731). 2.2.0 added `mapGroupsWithState` for arbitrary stateful operations (SPARK-19067), watermark-aware `dropDuplicates` (SPARK-19497), a Kafka sink, and General Availability — the experimental label came off (SPARK-20844). 2.3.0 added the Continuous Processing execution engine for sub-millisecond latency, stream-stream joins, and an experimental Streaming API V2 for pluggable sources/sinks spanning batch, micro-batch, and continuous execution. 2.4.0 added `foreachBatch` for exposing each micro-batch as a DataFrame (SPARK-24565) and a Python API for `foreach`/`ForeachWriter` (SPARK-24396).

### 3.x era — RocksDB state store and stateful processing mature

3.0.0 added a dedicated Structured Streaming UI (SPARK-29543) and an API for observing arbitrary metrics on streaming queries (SPARK-29345). 3.2.0 introduced session windows (SPARK-10816) and the RocksDB state store (SPARK-34198, shared with sql-catalyst), giving stateful operators a state backend that didn't need to fit in JVM heap. 3.3.0 added `Trigger.AvailableNow` (SPARK-36533) for running a streaming query like a series of bounded batches, and optimized the RocksDB write path (SPARK-37224). 3.4.0 added async progress tracking (SPARK-39591) and arbitrary stateful processing for Python (SPARK-40434), extending a Scala/Java-only capability to PySpark. 3.5.0 rounded out the state-store story with changelog checkpointing for RocksDB (SPARK-43421), watermark propagation among operators (SPARK-42376), and `dropDuplicatesWithinWatermark` (SPARK-42931).

### 4.x era — TransformWithState v2 and Real-time Mode

4.0.0's headline was Arbitrary State API v2 (SPARK-46815) — a new `transformWithState` operator built around multiple state variables and column families, with a State Data Source reader SPIP (SPARK-45511), batch support (SPARK-46865), Avro encoding with schema evolution (SPARK-50017), and a PySpark implementation with ValueState/ListState/timer support (SPARK-48755, SPARK-49463, SPARK-49513). 4.1.0 introduced a SPIP for Real-time Mode, stateless streaming with sub-batch latency (SPARK-53736), a rewritten row-based `transformWithState` Python API (SPARK-51814), stream-stream join support with virtual column families (SPARK-51779), and RocksDB reliability work — lock-management overhaul, snapshot-lag detection, checksum verification (SPARK-51745, SPARK-51358, SPARK-51972).

4.2.0 extended Real-time Mode into PySpark (SPARK-54660), added named, reorderable streaming sources and sinks via `DataStreamReader.name()`/`IDENTIFIED BY` (SPARK-54909, SPARK-56719), stream-stream join state format V4 (SPARK-55628), and automatic snapshot repair with row-level checksums for the state store (SPARK-54121, SPARK-54106) — turning `transformWithState` from a new operator into stateful streaming's primary API.

## Timeline

<!-- AUTO:timeline START -->
| Release | JIRA | Type | Title |
|---|---|---|---|
| 2.0.0 | — | prose | Structured Streaming initial experimental release |
| 2.0.0 | [SPARK-14214](https://issues.apache.org/jira/browse/SPARK-14214) | Improvement | Update State Store to give a more get/put hashmap-style interface |
| 2.0.0 | [SPARK-16031](https://issues.apache.org/jira/browse/SPARK-16031) | New Feature | Add debug-only socket source in Structured Streaming |
| 2.0.0 | [SPARK-16061](https://issues.apache.org/jira/browse/SPARK-16061) | Improvement | The property "spark.streaming.stateStore.maintenanceInterval" should be renamed to "spark.sql.streaming.stateStore.maintenanceInterval" |
| 2.0.1 | [SPARK-17640](https://issues.apache.org/jira/browse/SPARK-17640) | Improvement | Avoid using -1 as the default batchId for FileStreamSource.FileEntry |
| 2.0.2 | — | prose | Structured Streaming gains Kafka 0.10 support |
| 2.0.2 | — | prose | Structured Streaming gains runtime metrics |
| 2.0.2 | [SPARK-17780](https://issues.apache.org/jira/browse/SPARK-17780) | Improvement | Report NoClassDefFoundError in StreamExecution |
| 2.0.2 | [SPARK-18044](https://issues.apache.org/jira/browse/SPARK-18044) | Improvement | FileStreamSource should not infer partitions in every batch |
| 2.1.0 | [SPARK-8360](https://issues.apache.org/jira/browse/SPARK-8360) | Umbrella | Structured Streaming (aka Streaming DataFrames) |
| 2.1.0 | [SPARK-15406](https://issues.apache.org/jira/browse/SPARK-15406) | New Feature | Structured streaming support for consuming from Kafka |
| 2.1.0 | [SPARK-15472](https://issues.apache.org/jira/browse/SPARK-15472) | New Feature | Add support for writing partitioned `csv`, `json`, `text` formats in Structured Streaming |
| 2.1.0 | [SPARK-16411](https://issues.apache.org/jira/browse/SPARK-16411) | Improvement | Add textFile API to structured streaming. |
| 2.1.0 | [SPARK-17267](https://issues.apache.org/jira/browse/SPARK-17267) | prose | Long-running Structured Streaming query requirements addressed |
| 2.1.0 | [SPARK-17346](https://issues.apache.org/jira/browse/SPARK-17346) | prose | Kafka 0.10 support in Structured Streaming |
| 2.1.0 | [SPARK-17510](https://issues.apache.org/jira/browse/SPARK-17510) | Improvement | Set Streaming MaxRate Independently For Multiple Streams |
| 2.1.0 | [SPARK-17640](https://issues.apache.org/jira/browse/SPARK-17640) | Improvement | Avoid using -1 as the default batchId for FileStreamSource.FileEntry |
| 2.1.0 | [SPARK-17731](https://issues.apache.org/jira/browse/SPARK-17731) | prose | Metrics for Structured Streaming |
| 2.1.0 | [SPARK-17780](https://issues.apache.org/jira/browse/SPARK-17780) | Improvement | Report NoClassDefFoundError in StreamExecution |
| 2.1.0 | [SPARK-18044](https://issues.apache.org/jira/browse/SPARK-18044) | Improvement | FileStreamSource should not infer partitions in every batch |
| 2.1.0 | [SPARK-18124](https://issues.apache.org/jira/browse/SPARK-18124) | New Feature | Observed delay based event time watermarks |
| 2.1.0 | [SPARK-18192](https://issues.apache.org/jira/browse/SPARK-18192) | prose | All file formats supported in Structured Streaming |
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
| 2.2.0 | [SPARK-19968](https://issues.apache.org/jira/browse/SPARK-19968) | prose | Cached Kafka producer lowers Kafka-to-Kafka streaming latency |
| 2.2.0 | [SPARK-20844](https://issues.apache.org/jira/browse/SPARK-20844) | prose | Structured Streaming APIs reach General Availability |
| 2.3.0 | — | prose | Continuous Processing execution engine (sub-millisecond latency) |
| 2.3.0 | — | prose | Stream-stream joins |
| 2.3.0 | — | prose | Streaming API V2 (experimental) |
| 2.3.0 | — | prose | ML Prediction now works with Structured Streaming |
| 2.4.0 | [SPARK-18057](https://issues.apache.org/jira/browse/SPARK-18057) | prose | Kafka client upgraded from 0.10.0.1 to 2.0.0 |
| 2.4.0 | [SPARK-24156](https://issues.apache.org/jira/browse/SPARK-24156) | prose | Faster output generation/state cleanup for stateful ops with no input data |
| 2.4.0 | [SPARK-24396](https://issues.apache.org/jira/browse/SPARK-24396) | prose | Python API for foreach and ForeachWriter |
| 2.4.0 | [SPARK-24565](https://issues.apache.org/jira/browse/SPARK-24565) | prose | foreachBatch exposes microbatch output rows as a DataFrame |
| 2.4.0 | [SPARK-24662](https://issues.apache.org/jira/browse/SPARK-24662) | prose | LIMIT operator support for streams in Append or Complete mode |
| 2.4.0 | [SPARK-24730](https://issues.apache.org/jira/browse/SPARK-24730) | prose | Choose min or max watermark across multiple input streams |
| 2.4.0 | [SPARK-24763](https://issues.apache.org/jira/browse/SPARK-24763) | prose | Remove redundant key data from value in streaming aggregation |
| 2.4.0 | [SPARK-25005](https://issues.apache.org/jira/browse/SPARK-25005) | prose | kafka.isolation.level to read only committed records |
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
| 3.1.1 | [SPARK-24634](https://issues.apache.org/jira/browse/SPARK-24634) | prose | Add a new metric regarding number of rows later than watermark |
| 3.1.1 | [SPARK-27188](https://issues.apache.org/jira/browse/SPARK-27188) | prose | Provide a new option to have retention on output files |
| 3.1.1 | [SPARK-27237](https://issues.apache.org/jira/browse/SPARK-27237) | prose | Introduce State schema validation among query restart |
| 3.1.1 | [SPARK-28367](https://issues.apache.org/jira/browse/SPARK-28367) | prose | Kafka connector infinite wait because metadata never updated |
| 3.1.1 | [SPARK-30462](https://issues.apache.org/jira/browse/SPARK-30462) | prose | Streamline the logic on file stream source and sink metadata log |
| 3.1.1 | [SPARK-30866](https://issues.apache.org/jira/browse/SPARK-30866) | prose | Cache fetched list of files beyond maxFilesPerTrigger as unread file |
| 3.1.1 | [SPARK-30900](https://issues.apache.org/jira/browse/SPARK-30900) | prose | Avoid reading compact metadata log twice if the query restarts from compact batch |
| 3.1.1 | [SPARK-31642](https://issues.apache.org/jira/browse/SPARK-31642) | prose | Pagination support for Structured Streaming UI pages |
| 3.1.1 | [SPARK-31894](https://issues.apache.org/jira/browse/SPARK-31894) | prose | Introduce schema validation for streaming state store |
| 3.1.1 | [SPARK-31953](https://issues.apache.org/jira/browse/SPARK-31953) | prose | Add Spark Structured Streaming History Server Support |
| 3.1.1 | [SPARK-32568](https://issues.apache.org/jira/browse/SPARK-32568) | prose | Upgrade Kafka to 2.6.0 |
| 3.1.1 | [SPARK-32862](https://issues.apache.org/jira/browse/SPARK-32862) | prose | Left semi stream-stream join |
| 3.1.1 | [SPARK-32863](https://issues.apache.org/jira/browse/SPARK-32863) | prose | Full outer stream-stream join |
| 3.1.1 | [SPARK-32885](https://issues.apache.org/jira/browse/SPARK-32885) | prose | Add DataStreamReader.table API |
| 3.1.1 | [SPARK-32896](https://issues.apache.org/jira/browse/SPARK-32896) | prose | Add DataStreamWriter.toTable API |
| 3.1.1 | [SPARK-33223](https://issues.apache.org/jira/browse/SPARK-33223) | prose | State information in Structured Streaming UI |
| 3.1.1 | [SPARK-33224](https://issues.apache.org/jira/browse/SPARK-33224) | prose | Watermark gap information in Structured Streaming UI |
| 3.1.1 | [SPARK-33263](https://issues.apache.org/jira/browse/SPARK-33263) | prose | Support to use a different compression codec in state store |
| 3.1.1 | [SPARK-33287](https://issues.apache.org/jira/browse/SPARK-33287) | prose | Expose state custom metrics information on SS UI |
| 3.2.0 | [SPARK-10816](https://issues.apache.org/jira/browse/SPARK-10816) | New Feature | EventTime based sessionization (session window) |
| 3.2.0 | [SPARK-29223](https://issues.apache.org/jira/browse/SPARK-29223) | Improvement | Kafka source: offset by timestamp - allow specifying timestamp for "all partitions" |
| 3.2.0 | [SPARK-33660](https://issues.apache.org/jira/browse/SPARK-33660) | Improvement | Update Kafka Headers Documentation in Structured Streaming |
| 3.2.0 | [SPARK-33827](https://issues.apache.org/jira/browse/SPARK-33827) | Improvement | Unload State Store asap once it becomes inactive |
| 3.2.0 | [SPARK-33913](https://issues.apache.org/jira/browse/SPARK-33913) | prose | Upgrade Kafka client to 2.8.0 |
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
| 3.2.0 | [SPARK-36132](https://issues.apache.org/jira/browse/SPARK-36132) | prose | Support initial state for flatMapGroupsWithState in batch mode |
| 3.2.0 | [SPARK-36314](https://issues.apache.org/jira/browse/SPARK-36314) | Improvement | Update Sessionization example to use native support of session window |
| 3.2.0 | [SPARK-36455](https://issues.apache.org/jira/browse/SPARK-36455) | Improvement | Provide an example of complex session window via flatMapGroupsWithState |
| 3.3.0 | [SPARK-36533](https://issues.apache.org/jira/browse/SPARK-36533) | prose | Introduce Trigger.AvailableNow for running streaming queries like Trigger.Once in multiple batches |
| 3.3.0 | [SPARK-36649](https://issues.apache.org/jira/browse/SPARK-36649) | prose | Support Trigger.AvailableNow on Kafka data source |
| 3.3.0 | [SPARK-36837](https://issues.apache.org/jira/browse/SPARK-36837) | prose | Upgrade Kafka to 3.1.0 |
| 3.3.0 | [SPARK-37062](https://issues.apache.org/jira/browse/SPARK-37062) | prose | Introduce a new data source for providing consistent set of rows per microbatch |
| 3.3.0 | [SPARK-37224](https://issues.apache.org/jira/browse/SPARK-37224) | prose | Optimize write path on RocksDB state store provider |
| 3.3.0 | [SPARK-38204](https://issues.apache.org/jira/browse/SPARK-38204) | prose | Use StatefulOpClusteredDistribution for stateful operators with respecting backward compatibility |
| 3.3.0 | [SPARK-39218](https://issues.apache.org/jira/browse/SPARK-39218) | prose | Make foreachBatch streaming query stop gracefully |
| 3.4.0 | [SPARK-38564](https://issues.apache.org/jira/browse/SPARK-38564) | prose | Support collecting metrics from streaming sinks |
| 3.4.0 | [SPARK-39564](https://issues.apache.org/jira/browse/SPARK-39564) | prose | Expose the information of catalog table to the logical plan in streaming query |
| 3.4.0 | [SPARK-39591](https://issues.apache.org/jira/browse/SPARK-39591) | prose | Async Progress Tracking in Structured Streaming |
| 3.4.0 | [SPARK-40039](https://issues.apache.org/jira/browse/SPARK-40039) | prose | Introducing a streaming checkpoint file manager based on Hadoop’s Abortable interface |
| 3.4.0 | [SPARK-40434](https://issues.apache.org/jira/browse/SPARK-40434) | prose | Python Arbitrary Stateful Processing in Structured Streaming |
| 3.4.0 | [SPARK-40653](https://issues.apache.org/jira/browse/SPARK-40653) | prose | Protobuf Support in Structured Streaming |
| 3.4.0 | [SPARK-40844](https://issues.apache.org/jira/browse/SPARK-40844) | prose | Flip the default value of Kafka offset fetching config |
| 3.4.0 | [SPARK-41379](https://issues.apache.org/jira/browse/SPARK-41379) | prose | Provide cloned spark session in DataFrame in user function for foreachBatch sink in PySpark |
| 3.5.0 | [SPARK-42353](https://issues.apache.org/jira/browse/SPARK-42353) | prose | Cleanup orphan sst and log files in RocksDB checkpoint directory |
| 3.5.0 | [SPARK-42792](https://issues.apache.org/jira/browse/SPARK-42792) | prose | Add support for WRITE_FLUSH_BYTES for RocksDB used in streaming stateful operators |
| 3.5.0 | [SPARK-42819](https://issues.apache.org/jira/browse/SPARK-42819) | prose | Add support for setting max_write_buffer_number and write_buffer_size for RocksDB used in streaming |
| 3.5.0 | [SPARK-42968](https://issues.apache.org/jira/browse/SPARK-42968) | prose | Add option to skip commit coordinator as part of StreamingWrite API for DSv2 sources/sinks |
| 3.5.0 | [SPARK-43120](https://issues.apache.org/jira/browse/SPARK-43120) | prose | Add support for tracking pinned blocks memory usage for RocksDB state store |
| 3.5.0 | [SPARK-43183](https://issues.apache.org/jira/browse/SPARK-43183) | prose | Introduce a new callback onQueryIdle() to StreamingQueryListener |
| 3.5.0 | [SPARK-43482](https://issues.apache.org/jira/browse/SPARK-43482) | prose | Expand QueryTerminatedEvent to contain error class if it exists in exception |
| 4.0.0 | [SPARK-44865](https://issues.apache.org/jira/browse/SPARK-44865) | prose | Make StreamingRelationV2 support metadata column |
| 4.0.0 | [SPARK-45080](https://issues.apache.org/jira/browse/SPARK-45080) | prose | Explicitly call out support for columnar in DSv2 streaming data sources |
| 4.0.0 | [SPARK-45178](https://issues.apache.org/jira/browse/SPARK-45178) | prose | Fallback to execute a single batch for Trigger.AvailableNow with unsupported sources |
| 4.0.0 | [SPARK-45415](https://issues.apache.org/jira/browse/SPARK-45415) | prose | Allow selective disabling of “fallocate” in RocksDB statestore |
| 4.0.0 | [SPARK-45503](https://issues.apache.org/jira/browse/SPARK-45503) | prose | Add Conf to Set RocksDB Compression |
| 4.0.0 | [SPARK-45511](https://issues.apache.org/jira/browse/SPARK-45511) | prose | SPIP: State Data Source - Reader |
| 4.0.0 | [SPARK-45558](https://issues.apache.org/jira/browse/SPARK-45558) | prose | Introduce a metadata file for streaming stateful operator |
| 4.0.0 | [SPARK-45794](https://issues.apache.org/jira/browse/SPARK-45794) | prose | Introduce state metadata source to query the streaming state metadata information |
| 4.0.0 | [SPARK-45815](https://issues.apache.org/jira/browse/SPARK-45815) | prose | Provide an interface for other Streaming sources to add _metadata columns |
| 4.0.0 | [SPARK-45845](https://issues.apache.org/jira/browse/SPARK-45845) | prose | Add number of evicted state rows to streaming UI |
| 4.0.0 | [SPARK-46641](https://issues.apache.org/jira/browse/SPARK-46641) | prose | Add maxBytesPerTrigger threshold |
| 4.0.0 | [SPARK-46815](https://issues.apache.org/jira/browse/SPARK-46815) | prose | Structured Streaming - Arbitrary State API v2 |
| 4.0.0 | [SPARK-46816](https://issues.apache.org/jira/browse/SPARK-46816) | prose | Add base support for new arbitrary state management operator (multiple state variables/column families) |
| 4.0.0 | [SPARK-46865](https://issues.apache.org/jira/browse/SPARK-46865) | prose | Add Batch Support for TransformWithState Operator |
| 4.0.0 | [SPARK-46906](https://issues.apache.org/jira/browse/SPARK-46906) | prose | Add a check for stateful operator change for streaming |
| 4.0.0 | [SPARK-46961](https://issues.apache.org/jira/browse/SPARK-46961) | prose | Use ProcessorContext to store and retrieve handle |
| 4.0.0 | [SPARK-46962](https://issues.apache.org/jira/browse/SPARK-46962) | prose | Implement python worker to run python streaming data source |
| 4.0.0 | [SPARK-47107](https://issues.apache.org/jira/browse/SPARK-47107) | prose | Partition reader for Python streaming data sources |
| 4.0.0 | [SPARK-47273](https://issues.apache.org/jira/browse/SPARK-47273) | prose | Python data stream writer interface |
| 4.0.0 | [SPARK-47553](https://issues.apache.org/jira/browse/SPARK-47553) | prose | Add Java support for transformWithState operator APIs |
| 4.0.0 | [SPARK-47653](https://issues.apache.org/jira/browse/SPARK-47653) | prose | Add support for negative numeric types and range scan key encoder |
| 4.0.0 | [SPARK-47733](https://issues.apache.org/jira/browse/SPARK-47733) | prose | Add custom metrics for transformWithState operator part of query progress |
| 4.0.0 | [SPARK-47960](https://issues.apache.org/jira/browse/SPARK-47960) | prose | Allow chaining other stateful operators after transformWithState |
| 4.0.0 | [SPARK-48447](https://issues.apache.org/jira/browse/SPARK-48447) | prose | Check StateStoreProvider class before constructor |
| 4.0.0 | [SPARK-48569](https://issues.apache.org/jira/browse/SPARK-48569) | prose | Handle edge cases in query.name for streaming queries |
| 4.0.0 | [SPARK-48589](https://issues.apache.org/jira/browse/SPARK-48589) | prose | Add snapshotStartBatchId / snapshotPartitionId for state data source (see SQL) |
| 4.0.0 | [SPARK-48597](https://issues.apache.org/jira/browse/SPARK-48597) | prose | Introduce marker for isStreaming in text representation of logical plan |
| 4.0.0 | [SPARK-48726](https://issues.apache.org/jira/browse/SPARK-48726) | prose | Create StateSchemaV3 file for TransformWithStateExec |
| 4.0.0 | [SPARK-48742](https://issues.apache.org/jira/browse/SPARK-48742) | prose | Virtual Column Family for RocksDB (arbitrary stateful API v2) |
| 4.0.0 | [SPARK-48755](https://issues.apache.org/jira/browse/SPARK-48755) | prose | transformWithState pyspark base implementation and ValueState support |
| 4.0.0 | [SPARK-48772](https://issues.apache.org/jira/browse/SPARK-48772) | prose | State Data Source Change Feed Reader Mode |
| 4.0.0 | [SPARK-48836](https://issues.apache.org/jira/browse/SPARK-48836) | prose | Integrate SQL schema with state schema/metadata for TWS operator |
| 4.0.0 | [SPARK-48849](https://issues.apache.org/jira/browse/SPARK-48849) | prose | Create OperatorStateMetadataV2 for TransformWithStateExec operator |
| 4.0.0 | [SPARK-48931](https://issues.apache.org/jira/browse/SPARK-48931) | prose | Reduce Cloud Store List API cost for state-store maintenance |
| 4.0.0 | [SPARK-49021](https://issues.apache.org/jira/browse/SPARK-49021) | prose | Add support for reading transformWithState value state variables with state data source reader |
| 4.0.0 | [SPARK-49048](https://issues.apache.org/jira/browse/SPARK-49048) | prose | Add support for reading operator metadata at given batch id |
| 4.0.0 | [SPARK-49191](https://issues.apache.org/jira/browse/SPARK-49191) | prose | Read transformWithState map state with state data source |
| 4.0.0 | [SPARK-49259](https://issues.apache.org/jira/browse/SPARK-49259) | prose | Size-based partition creation during Kafka read |
| 4.0.0 | [SPARK-49411](https://issues.apache.org/jira/browse/SPARK-49411) | prose | Communicate State Store Checkpoint ID |
| 4.0.0 | [SPARK-49463](https://issues.apache.org/jira/browse/SPARK-49463) | prose | ListState support in TransformWithStateInPandas |
| 4.0.0 | [SPARK-49467](https://issues.apache.org/jira/browse/SPARK-49467) | prose | Add state data source reader for list state |
| 4.0.0 | [SPARK-49513](https://issues.apache.org/jira/browse/SPARK-49513) | prose | Add timer support in transformWithStateInPandas |
| 4.0.0 | [SPARK-49630](https://issues.apache.org/jira/browse/SPARK-49630) | prose | Add flatten option for collection types in state data source reader |
| 4.0.0 | [SPARK-49656](https://issues.apache.org/jira/browse/SPARK-49656) | prose | Support state variables with value state collection types |
| 4.0.0 | [SPARK-49676](https://issues.apache.org/jira/browse/SPARK-49676) | prose | Chaining of operators in transformWithStateInPandas |
| 4.0.0 | [SPARK-49699](https://issues.apache.org/jira/browse/SPARK-49699) | prose | Disable PruneFilters for streaming workloads |
| 4.0.0 | [SPARK-49744](https://issues.apache.org/jira/browse/SPARK-49744) | prose | TTL support for ListState in TransformWithStateInPandas |
| 4.0.0 | [SPARK-49745](https://issues.apache.org/jira/browse/SPARK-49745) | prose | Read registered timers in transformWithState |
| 4.0.0 | [SPARK-49802](https://issues.apache.org/jira/browse/SPARK-49802) | prose | Add support for read change feed for map/list types |
| 4.0.0 | [SPARK-49846](https://issues.apache.org/jira/browse/SPARK-49846) | prose | Add numUpdatedStateRows / numRemovedStateRows metrics |
| 4.0.0 | [SPARK-49883](https://issues.apache.org/jira/browse/SPARK-49883) | prose | State Store Checkpoint Structure V2 Integration with RocksDB and RocksDBFileManager |
| 4.0.0 | [SPARK-50017](https://issues.apache.org/jira/browse/SPARK-50017) | prose | Support Avro encoding for TransformWithState operator |
| 4.0.0 | [SPARK-50035](https://issues.apache.org/jira/browse/SPARK-50035) | prose | Explicit handleExpiredTimer function in the stateful processor |
| 4.0.0 | [SPARK-50128](https://issues.apache.org/jira/browse/SPARK-50128) | prose | Add handle APIs using implicit encoders |
| 4.0.0 | [SPARK-50152](https://issues.apache.org/jira/browse/SPARK-50152) | prose | Support handleInitialState with state data source reader |
| 4.0.0 | [SPARK-50194](https://issues.apache.org/jira/browse/SPARK-50194) | prose | Integration of New Timer API and Initial State API |
| 4.0.0 | [SPARK-50378](https://issues.apache.org/jira/browse/SPARK-50378) | prose | Add custom metric for time spent populating initial state |
| 4.0.0 | [SPARK-50428](https://issues.apache.org/jira/browse/SPARK-50428) | prose | Support TransformWithStateInPandas in batch queries |
| 4.0.0 | [SPARK-50573](https://issues.apache.org/jira/browse/SPARK-50573) | prose | Adding State Schema ID to State Rows for schema evolution |
| 4.0.0 | [SPARK-50714](https://issues.apache.org/jira/browse/SPARK-50714) | prose | Enable schema evolution for TransformWithState with Avro encoding |
| 4.0.1 | [SPARK-52989](https://issues.apache.org/jira/browse/SPARK-52989) | prose | Add explicit close API to RocksDB State store iterator and fix current usage |
| 4.1.0 | [SPARK-51097](https://issues.apache.org/jira/browse/SPARK-51097) | prose | Re-introduce RocksDB state store’s last uploaded snapshot version instance metrics |
| 4.1.0 | [SPARK-51358](https://issues.apache.org/jira/browse/SPARK-51358) | prose | Snapshot lag detection with RocksDB state store provider |
| 4.1.0 | [SPARK-51745](https://issues.apache.org/jira/browse/SPARK-51745) | prose | Revamped lock management with RocksDB state store provider |
| 4.1.0 | [SPARK-51779](https://issues.apache.org/jira/browse/SPARK-51779) | prose | Stream-stream join support with virtual column families including support with state data source reader |
| 4.1.0 | [SPARK-51814](https://issues.apache.org/jira/browse/SPARK-51814) | prose | Introduce new row based transformWithState Python API |
| 4.1.0 | [SPARK-51823](https://issues.apache.org/jira/browse/SPARK-51823) | prose | Add config to not persist state store on executors |
| 4.1.0 | [SPARK-51940](https://issues.apache.org/jira/browse/SPARK-51940) | prose | Add interface for managing streaming checkpoint metadata |
| 4.1.0 | [SPARK-51972](https://issues.apache.org/jira/browse/SPARK-51972) | prose | File level checksum verification with RocksDB state store provider |
| 4.1.0 | [SPARK-51981](https://issues.apache.org/jira/browse/SPARK-51981) | prose | Add JobTags to queryStartedEvent |
| 4.1.0 | [SPARK-52008](https://issues.apache.org/jira/browse/SPARK-52008) | prose | Throwing an error if State Stores do not commit at the end of a batch when ForeachBatch is used |
| 4.1.0 | [SPARK-52968](https://issues.apache.org/jira/browse/SPARK-52968) | prose | Emit additional state store metrics |
| 4.1.0 | [SPARK-52989](https://issues.apache.org/jira/browse/SPARK-52989) | prose | Add explicit close() API to State Store iterators |
| 4.1.0 | [SPARK-53001](https://issues.apache.org/jira/browse/SPARK-53001) | prose | Integrate RocksDB Memory Usage with the Unified Memory Manager |
| 4.1.0 | [SPARK-53103](https://issues.apache.org/jira/browse/SPARK-53103) | prose | Throw an error if state directory is not empty when query starts |
| 4.1.0 | [SPARK-53333](https://issues.apache.org/jira/browse/SPARK-53333) | prose | State data source support with state checkpoint format v2 |
| 4.1.0 | [SPARK-53736](https://issues.apache.org/jira/browse/SPARK-53736) | prose | SPIP: Real-time Mode in Structured Streaming (Scala stateless support) |
| 4.1.0 | [SPARK-53794](https://issues.apache.org/jira/browse/SPARK-53794) | prose | Add option to limit deletions per maintenance operation associated with rocksdb state provider |
| 4.1.0 | [SPARK-53941](https://issues.apache.org/jira/browse/SPARK-53941) | prose | Support AQE in stateless streaming workloads |
| 4.1.0 | [SPARK-53942](https://issues.apache.org/jira/browse/SPARK-53942) | prose | Support changing shuffle partitions in stateless streaming workloads |
| 4.1.0 | [SPARK-54063](https://issues.apache.org/jira/browse/SPARK-54063) | prose | Trigger snapshot for next batch when upload lag |
| 4.1.0 | [SPARK-54106](https://issues.apache.org/jira/browse/SPARK-54106) | prose | Recheckin State store row checksum implementation |
| 4.1.0 | [SPARK-54121](https://issues.apache.org/jira/browse/SPARK-54121) | prose | Automatic Snapshot Repair for State store |
| 4.2.0 | [SPARK-54063](https://issues.apache.org/jira/browse/SPARK-54063) | prose | Trigger a state-store snapshot on commit for the next batch when snapshot upload lags, for both RocksDB and HDFS providers |
| 4.2.0 | [SPARK-54106](https://issues.apache.org/jira/browse/SPARK-54106) | prose | Add state store row checksum for corruption detection |
| 4.2.0 | [SPARK-54121](https://issues.apache.org/jira/browse/SPARK-54121) | prose | Automatic snapshot repair for the state store |
| 4.2.0 | [SPARK-54660](https://issues.apache.org/jira/browse/SPARK-54660) | prose | Add Real-time Mode (RTM) trigger support to PySpark |
| 4.2.0 | [SPARK-54909](https://issues.apache.org/jira/browse/SPARK-54909) | prose | Enabling addition, removal and reordering of streaming sources: DataStreamReader.name() and IDENTIFIED BY syntax to give streaming source... |
| 4.2.0 | [SPARK-55058](https://issues.apache.org/jira/browse/SPARK-55058) | prose | Throw an error on inconsistent streaming checkpoint metadata |
| 4.2.0 | [SPARK-55304](https://issues.apache.org/jira/browse/SPARK-55304) | prose | Support Admission Control and Trigger.AvailableNow in Python data source streaming reader |
| 4.2.0 | [SPARK-55628](https://issues.apache.org/jira/browse/SPARK-55628) | prose | Integrate stream-stream join state format V4 |
| 4.2.0 | [SPARK-55999](https://issues.apache.org/jira/browse/SPARK-55999) | prose | Enable forceSnapshotUploadOnLag by default |
| 4.2.0 | [SPARK-56384](https://issues.apache.org/jira/browse/SPARK-56384) | prose | Support stream-stream non-outer join in Update mode |
| 4.2.0 | [SPARK-56719](https://issues.apache.org/jira/browse/SPARK-56719) | prose | Add .name() to enable streaming sink naming, with a V3 commit log that persists the sink name |
<!-- AUTO:timeline END -->
