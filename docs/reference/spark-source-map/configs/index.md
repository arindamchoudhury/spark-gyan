# Spark configuration catalog

> Auto-generated from Apache Spark **4.1.2** source by `tools/spark_source_map/gen_configs.py`. Do not edit by hand — re-run the generator instead.

**1405 configs** across the repo · 3 unparsed · generated 2026-06-06.

## Contents

- [connector/kafka-0-10](#connectorkafka-0-10) — 8 configs
- [connector/kafka-0-10-sql](#connectorkafka-0-10-sql) — 8 configs
- [connector/profiler](#connectorprofiler) — 7 configs
- [core](#core) — 533 configs
- [resource-managers/kubernetes](#resource-managerskubernetes) — 81 configs
- [resource-managers/yarn](#resource-managersyarn) — 59 configs
- [sql/catalyst](#sqlcatalyst) — 656 configs
- [sql/connect](#sqlconnect) — 14 configs
- [sql/hive](#sqlhive) — 11 configs
- [streaming](#streaming) — 28 configs

## connector/kafka-0-10

### `spark.streaming.kafka.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.streaming.kafka.allowNonConsecutiveOffsets` | boolean | `false` | 2.3.1 |  | [src](https://github.com/apache/spark/blob/v4.1.2/connector/kafka-0-10/src/main/scala/org/apache/spark/streaming/kafka010/package.scala#L70) |
| `spark.streaming.kafka.consumer.cache.enabled` | boolean | `true` | 2.2.1 |  | [src](https://github.com/apache/spark/blob/v4.1.2/connector/kafka-0-10/src/main/scala/org/apache/spark/streaming/kafka010/package.scala#L28) |
| `spark.streaming.kafka.consumer.cache.initialCapacity` | int | `16` | 2.0.1 |  | [src](https://github.com/apache/spark/blob/v4.1.2/connector/kafka-0-10/src/main/scala/org/apache/spark/streaming/kafka010/package.scala#L40) |
| `spark.streaming.kafka.consumer.cache.loadFactor` | double | `0.75` | 2.0.1 |  | [src](https://github.com/apache/spark/blob/v4.1.2/connector/kafka-0-10/src/main/scala/org/apache/spark/streaming/kafka010/package.scala#L52) |
| `spark.streaming.kafka.consumer.cache.maxCapacity` | int | `64` | 2.0.1 |  | [src](https://github.com/apache/spark/blob/v4.1.2/connector/kafka-0-10/src/main/scala/org/apache/spark/streaming/kafka010/package.scala#L46) |
| `spark.streaming.kafka.consumer.poll.ms` | long | _(optional)_ | 2.0.1 |  | [src](https://github.com/apache/spark/blob/v4.1.2/connector/kafka-0-10/src/main/scala/org/apache/spark/streaming/kafka010/package.scala#L34) |
| `spark.streaming.kafka.maxRatePerPartition` | long | `0` | 1.3.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/connector/kafka-0-10/src/main/scala/org/apache/spark/streaming/kafka010/package.scala#L58) |
| `spark.streaming.kafka.minRatePerPartition` | long | `1` | 2.4.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/connector/kafka-0-10/src/main/scala/org/apache/spark/streaming/kafka010/package.scala#L64) |

## connector/kafka-0-10-sql

### `spark.kafka.consumer.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.kafka.consumer.cache.capacity` | int | `64` | 3.0.0 | The maximum number of consumers cached. Please note it's a soft limit (check Structured Streaming Kafka integration guide for further details). | [src](https://github.com/apache/spark/blob/v4.1.2/connector/kafka-0-10-sql/src/main/scala/org/apache/spark/sql/kafka010/package.scala#L45) |
| `spark.kafka.consumer.cache.evictorThreadRunInterval` | time | `1m` | 3.0.0 | The interval of time between runs of the idle evictor thread for consumer pool. When non-positive, no idle evictor thread will be run. | [src](https://github.com/apache/spark/blob/v4.1.2/connector/kafka-0-10-sql/src/main/scala/org/apache/spark/sql/kafka010/package.scala#L69) |
| `spark.kafka.consumer.cache.jmx.enable` | boolean | `false` | 3.0.0 | Enable or disable JMX for pools created with this configuration instance. | [src](https://github.com/apache/spark/blob/v4.1.2/connector/kafka-0-10-sql/src/main/scala/org/apache/spark/sql/kafka010/package.scala#L53) |
| `spark.kafka.consumer.cache.timeout` | time | `5m` | 3.0.0 | The minimum amount of time a consumer may sit idle in the pool before it is eligible for eviction by the evictor. When non-positive, no consumers will be evicted from the pool due to idle time alone. | [src](https://github.com/apache/spark/blob/v4.1.2/connector/kafka-0-10-sql/src/main/scala/org/apache/spark/sql/kafka010/package.scala#L60) |
| `spark.kafka.consumer.fetchedData.cache.evictorThreadRunInterval` | time | `1m` | 3.0.0 | The interval of time between runs of the idle evictor thread for fetched data pool. When non-positive, no idle evictor thread will be run. | [src](https://github.com/apache/spark/blob/v4.1.2/connector/kafka-0-10-sql/src/main/scala/org/apache/spark/sql/kafka010/package.scala#L86) |
| `spark.kafka.consumer.fetchedData.cache.timeout` | time | `5m` | 3.0.0 | The minimum amount of time a fetched data may sit idle in the pool before it is eligible for eviction by the evictor. When non-positive, no fetched data will be evicted from the pool due to idle time alone. | [src](https://github.com/apache/spark/blob/v4.1.2/connector/kafka-0-10-sql/src/main/scala/org/apache/spark/sql/kafka010/package.scala#L77) |

### `spark.kafka.producer.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.kafka.producer.cache.evictorThreadRunInterval` | time | `1m` | 3.0.0 | The interval of time between runs of the idle evictor thread for producer pool. When non-positive, no idle evictor thread will be run. | [src](https://github.com/apache/spark/blob/v4.1.2/connector/kafka-0-10-sql/src/main/scala/org/apache/spark/sql/kafka010/package.scala#L37) |
| `spark.kafka.producer.cache.timeout` | time | `10m` | 2.2.1 | The expire time to remove the unused producers. | [src](https://github.com/apache/spark/blob/v4.1.2/connector/kafka-0-10-sql/src/main/scala/org/apache/spark/sql/kafka010/package.scala#L30) |

## connector/profiler

### `spark.profiler.asyncProfiler.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.profiler.asyncProfiler.args` | string | `event=wall,interval=10ms,alloc=2m,lock=10ms,chunktime=300s` | 4.0.0 | Arguments to pass on to the Async Profiler. | [src](https://github.com/apache/spark/blob/v4.1.2/connector/profiler/src/main/scala/org/apache/spark/profiler/package.scala#L64) |

### `spark.profiler.dfsDir.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.profiler.dfsDir` | string | _(optional)_ | 4.0.0 | HDFS compatible file-system path to where the profiler will write output jfr files. | [src](https://github.com/apache/spark/blob/v4.1.2/connector/profiler/src/main/scala/org/apache/spark/profiler/package.scala#L49) |

### `spark.profiler.dfsWriteInterval.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.profiler.dfsWriteInterval` | time | `30` | 4.0.0 | Time interval in seconds after which the profiler output will be synced to DFS. | [src](https://github.com/apache/spark/blob/v4.1.2/connector/profiler/src/main/scala/org/apache/spark/profiler/package.scala#L71) |

### `spark.profiler.driver.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.profiler.driver.enabled` | boolean | `false` | 4.0.0 | Turn on profiling in driver. | [src](https://github.com/apache/spark/blob/v4.1.2/connector/profiler/src/main/scala/org/apache/spark/profiler/package.scala#L26) |

### `spark.profiler.executor.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.profiler.executor.enabled` | boolean | `false` | 4.0.0 | Turn on profiling in executors. | [src](https://github.com/apache/spark/blob/v4.1.2/connector/profiler/src/main/scala/org/apache/spark/profiler/package.scala#L33) |
| `spark.profiler.executor.fraction` | double | `0.1` | 4.0.0 | Fraction of executors to profile | [src](https://github.com/apache/spark/blob/v4.1.2/connector/profiler/src/main/scala/org/apache/spark/profiler/package.scala#L40) |

### `spark.profiler.localDir.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.profiler.localDir` | string | `.` | 4.0.0 | Local file system path on executor where profiler output is saved. Defaults to the working directory of the executor process. | [src](https://github.com/apache/spark/blob/v4.1.2/connector/profiler/src/main/scala/org/apache/spark/profiler/package.scala#L56) |

## core

### `spark.acls.enable.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.acls.enable` | boolean | `false` | 1.1.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/UI.scala#L171) |

### `spark.admin.acls.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.admin.acls` | string | `Nil` | 1.1.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/UI.scala#L188) |
| `spark.admin.acls.groups` | string | `Nil` | 2.0.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/UI.scala#L194) |

### `spark.api.mode.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.api.mode` | string | `if (sys.env.get("SPARK_CONNECT_MODE").contains("1")) "connect" else "classic"` | 4.0.0 | For Spark Classic applications, specify whether to automatically use Spark Connect by running a local Spark Connect server dedicated to the application. The server is terminated when the application is terminated. The value can be `classic` or `connect`. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2880) |

### `spark.app.attempt.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.app.attempt.id` | string | _(optional)_ | 3.2.0 | The application attempt Id assigned from Hadoop YARN. When the application runs in cluster mode on YARN, there can be multiple attempts before failing the application | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2771) |

### `spark.appStateStore.asyncTracking.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.appStateStore.asyncTracking.enable` | boolean | `true` | 2.3.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Status.scala#L24) |

### `spark.archives.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.archives` | string | `Nil` | 3.1.0 | Comma-separated list of archives to be extracted into the working directory of each executor. .jar, .tar.gz, .tgz and .zip are supported. You can specify the directory name to unpack via adding '#' after the file name to unpack, for example, 'file.zip#directory'. This configuration is experimental. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2302) |

### `spark.authenticate.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.authenticate` | boolean | `false` | 1.0.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1318) |

### `spark.authenticate.enableSaslEncryption.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.authenticate.enableSaslEncryption` | boolean | `false` | 1.4.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1324) |

### `spark.authenticate.secret.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.authenticate.secret` | string | _(optional)_ | 1.0.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1306) |
| `spark.authenticate.secret.driver.file` | fallback | → `AUTH_SECRET_FILE` | 3.0.0 | Path to a file that contains the authentication secret to use. Loaded by the driver. In Kubernetes client mode it is often useful to set a different secret path for the driver vs. the executors, since the driver may not be running in a pod unlike the executors. If this is set, an accompanying secret file must be specified for the executors. The fallback configuration allows the same path to be used for both the driver and the executors when running in cluster mode. File-based secret keys are only allowed when using Kubernetes. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1340) |
| `spark.authenticate.secret.executor.file` | fallback | → `AUTH_SECRET_FILE` | 3.0.0 | Path to a file that contains the authentication secret to use. Loaded by the executors only. In Kubernetes client mode it is often useful to set a different secret path for the driver vs. the executors, since the driver may not be running in a pod unlike the executors. If this is set, an accompanying secret file must be specified for the executors. The fallback configuration allows the same path to be used for both the driver and the executors when running in cluster mode. File-based secret keys are only allowed when using Kubernetes. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1352) |
| `spark.authenticate.secret.file` | string | _(optional)_ | 3.0.0 | Path to a file that contains the authentication secret to use. The secret key is loaded from this path on both the driver and the executors if overrides are not set for either entity (see below). File-based secret keys are only allowed when using Kubernetes. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1330) |

### `spark.authenticate.secretBitLength.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.authenticate.secretBitLength` | int | `256` | 1.6.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1312) |

### `spark.barrier.sync.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.barrier.sync.timeout` | time | `365d` | 2.4.0 | The timeout in seconds for each barrier() call from a barrier task. If the coordinator didn't receive all the sync messages from barrier tasks within the configured time, throw a SparkException to fail all the tasks. The default value is set to 31536000(3600 * 24 * 365) so the barrier() call shall wait for one year. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1855) |

### `spark.block.failures.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.block.failures.beforeLocationRefresh` | int | `5` | 2.0.0 | Max number of failures before this block manager refreshes the block locations from the driver. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L698) |

### `spark.blockManager.port.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.blockManager.port` | int | `0` | 1.1.0 | Port to use for the block manager when a more specific setting is not provided. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1219) |

### `spark.broadcast.UDFCompressionThreshold.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.broadcast.UDFCompressionThreshold` | bytes | `1L * 1024 * 1024` | 3.0.0 | The threshold at which user-defined functions (UDFs) and Python RDD commands are compressed by broadcast in bytes unless otherwise specified | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2237) |

### `spark.broadcast.blockSize.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.broadcast.blockSize` | bytes | `4m` | 0.5.0 | Size of each piece of a block for TorrentBroadcastFactory, in KiB unless otherwise specified. Too large a value decreases parallelism during broadcast (makes it slower); however, if it is too small, BlockManager might take a performance hit | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2217) |

### `spark.broadcast.checksum.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.broadcast.checksum` | boolean | `true` | 2.1.1 | Whether to enable checksum for broadcast. If enabled, broadcasts will include a checksum, which can help detect corrupted blocks, at the cost of computing and sending a little more data. It's possible to disable it if the network has other mechanisms to guarantee data won't be corrupted during broadcast | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2226) |

### `spark.broadcast.compress.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.broadcast.compress` | boolean | `true` | 0.6.0 | Whether to compress broadcast variables before sending them. Generally a good idea. Compression will use spark.io.compression.codec | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2211) |

### `spark.buffer.pageSize.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.buffer.pageSize` | bytes | _(optional)_ | 1.5.0 | The amount of memory used per page in bytes | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2490) |

### `spark.buffer.size.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.buffer.size` | int | `65536` | 0.5.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2174) |

### `spark.buffer.write.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.buffer.write.chunkSize` | bytes | `64 * 1024 * 1024` | 2.3.0 | The chunk size in bytes during writing out the bytes of ChunkedByteBuffer. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1364) |

### `spark.checkpoint.compress.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.checkpoint.compress` | boolean | `true` | 2.2.0 | Whether to compress RDD checkpoints. Generally a good idea. Compression will use spark.io.compression.codec. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1384) |

### `spark.checkpoint.dir.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.checkpoint.dir` | string | _(optional)_ | 4.0.0 | Set the default directory for checkpointing. It can be overwritten by SparkContext.setCheckpointDir. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1375) |

### `spark.cleaner.periodicGC.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.cleaner.periodicGC.interval` | time | `30min` | 1.6.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1951) |

### `spark.cleaner.referenceTracking.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.cleaner.referenceTracking` | boolean | `true` | 1.0.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1957) |
| `spark.cleaner.referenceTracking.blocking` | boolean | `true` | 1.0.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1963) |
| `spark.cleaner.referenceTracking.blocking.shuffle` | boolean | `false` | 1.1.1 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1969) |
| `spark.cleaner.referenceTracking.cleanCheckpoints` | boolean | `false` | 1.4.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1975) |

### `spark.connect.scalaUdf.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.connect.scalaUdf.stubPrefixes` | string | `"org.apache.spark.sql.connect.client" :: Nil` | 3.5.0 | \|Comma-separated list of binary names of classes/packages that should be stubbed during \|the Scala UDF serde and execution if not found on the server classpath. \|An empty list effectively disables stubbing for all missing classes. \|By default, the server stubs classes from the Scala client package. \| | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2836) |

### `spark.cores.max.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.cores.max` | int | _(optional)_ | 0.6.0 | When running on a standalone deploy cluster, the maximum amount of CPU cores to request for the application from across the cluster (not from each machine). If not set, the default will be `spark.deploy.defaultCores` on Spark's standalone cluster manager | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L456) |

### `spark.dead.worker.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.dead.worker.persistence` | int | `15` | 0.8.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Deploy.scala#L76) |

### `spark.decommission.enabled.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.decommission.enabled` | boolean | `false` | 3.1.0 | When decommission enabled, Spark will try its best to shutdown the executor gracefully. Spark will try to migrate all the RDD blocks (controlled by ${STORAGE_DECOMMISSION_RDD_BLOCKS_ENABLED.key}) and shuffle blocks (controlled by ${STORAGE_DECOMMISSION_SHUFFLE_BLOCKS_ENABLED.key}) from the decommissioning executor to a remote executor when ${STORAGE_DECOMMISSION_ENABLED.key} is enabled. With decommission enabled, Spark will also decommission an executor instead of killing when ${DYN_ALLOCATION_ENABLED.key} enabled. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2444) |

### `spark.default.parallelism.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.default.parallelism` | int | _(optional)_ | 0.5.0 | Default number of partitions in RDDs returned by transformations like join, reduceByKey, and parallelize when not set by user. For distributed shuffle operations like reduceByKey and join, the largest number of partitions in a parent RDD. For operations like parallelize with no parent RDDs, it depends on the cluster manager. For example in Local mode, it defaults to the number of cores on the local machine | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L46) |

### `spark.deploy.appIdPattern.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.deploy.appIdPattern` | string | `app-%s-%04d` | 4.0.0 | The pattern for app ID generation based on Java `String.format` method.. The default value is `app-%s-%04d` which represents the existing app id string, e.g., `app-20231031224509-0008`. Plesae be careful to generate unique IDs. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Deploy.scala#L147) |

### `spark.deploy.appNumberModulo.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.deploy.appNumberModulo` | int | _(optional)_ | 4.0.0 | The modulo for app number. By default, the next of `app-yyyyMMddHHmmss-9999` is `app-yyyyMMddHHmmss-10000`. If we have 10000 as modulo, it will be `app-yyyyMMddHHmmss-0000`. In most cases, the prefix `app-yyyyMMddHHmmss` is increased already during creating 10000 applications. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Deploy.scala#L128) |

### `spark.deploy.defaultCores.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.deploy.defaultCores` | int | `Int.MaxValue` | 0.9.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Deploy.scala#L115) |

### `spark.deploy.driverIdPattern.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.deploy.driverIdPattern` | string | `driver-%s-%04d` | 4.0.0 | The pattern for driver ID generation based on Java `String.format` method. The default value is `driver-%s-%04d` which represents the existing driver id string , e.g., `driver-20231031224459-0019`. Please be careful to generate unique IDs | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Deploy.scala#L138) |

### `spark.deploy.maxDrivers.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.deploy.maxDrivers` | int | `Int.MaxValue` | 4.0.0 | The maximum number of running drivers. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Deploy.scala#L121) |

### `spark.deploy.maxExecutorRetries.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.deploy.maxExecutorRetries` | int | `10` | 1.6.3 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Deploy.scala#L81) |

### `spark.deploy.recoveryCompressionCodec.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.deploy.recoveryCompressionCodec` | string | _(optional)_ | 4.0.0 | A compression codec for persistence engines. none (default), lz4, lzf, snappy, and zstd. Currently, only FILESYSTEM mode supports this configuration. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Deploy.scala#L29) |

### `spark.deploy.recoveryDirectory.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.deploy.recoveryDirectory` | string | — | 0.8.1 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Deploy.scala#L41) |

### `spark.deploy.recoveryMode.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.deploy.recoveryMode` | string | `NONE` | 0.8.1 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Deploy.scala#L24) |
| `spark.deploy.recoveryMode.factory` | string | — | 1.2.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Deploy.scala#L36) |

### `spark.deploy.recoveryTimeout.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.deploy.recoveryTimeout` | time | _(optional)_ | 4.0.0 | Configures the timeout for recovery process. The default value is the same with ${Worker.WORKER_TIMEOUT.key}. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Deploy.scala#L46) |

### `spark.deploy.retainedApplications.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.deploy.retainedApplications` | int | `200` | 0.8.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Deploy.scala#L66) |

### `spark.deploy.retainedDrivers.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.deploy.retainedDrivers` | int | `200` | 1.1.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Deploy.scala#L71) |

### `spark.deploy.spreadOutApps.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.deploy.spreadOutApps` | boolean | `true` | 0.6.1 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Deploy.scala#L91) |

### `spark.deploy.spreadOutDrivers.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.deploy.spreadOutDrivers` | boolean | `true` | 4.0.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Deploy.scala#L86) |

### `spark.deploy.workerSelectionPolicy.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.deploy.workerSelectionPolicy` | string | `WorkerSelectionPolicy.CORES_FREE_DESC.toString` | 4.0.0 | A policy to assign executors on one of the assignable workers; CORES_FREE_ASC to choose a worker with the least free cores, CORES_FREE_DESC to choose a worker with the most free cores, MEMORY_FREE_ASC to choose a worker with the least free memory, MEMORY_FREE_DESC to choose a worker with the most free memory, WORKER_ID to choose a worker with the smallest worker id. CORES_FREE_DESC is the default behavior. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Deploy.scala#L101) |

### `spark.deploy.zookeeper.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.deploy.zookeeper.dir` | string | _(optional)_ | 0.8.1 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Deploy.scala#L61) |
| `spark.deploy.zookeeper.url` | string | _(optional)_ | 0.8.1 | When `${RECOVERY_MODE.key}` is set to ZOOKEEPER, this configuration is used to set the zookeeper URL to connect to. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Deploy.scala#L54) |

### `spark.diskStore.subDirectories.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.diskStore.subDirectories` | int | `64` | 0.6.0 | Number of subdirectories inside each path listed in spark.local.dir for hashing Block files into. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L689) |

### `spark.driver.bindAddress.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.driver.bindAddress` | fallback | → `DRIVER_HOST_ADDRESS` | 2.1.0 | Address where to bind network listen sockets on the driver. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1214) |

### `spark.driver.blockManager.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.driver.blockManager.port` | fallback | → `BLOCK_MANAGER_PORT` | 2.1.0 | Port to use for the block manager on the driver. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1225) |

### `spark.driver.cores.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.driver.cores` | int | `1` | 1.3.0 | Number of cores to use for the driver process, only in cluster mode. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L113) |

### `spark.driver.defaultExtraClassPath.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.driver.defaultExtraClassPath` | string | `SparkLauncher.DRIVER_DEFAULT_EXTRA_CLASS_PATH_VALUE` | 4.0.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L81) |

### `spark.driver.extraClassPath.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.driver.extraClassPath` | string | _(optional)_ | 1.0.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L88) |

### `spark.driver.extraJavaOptions.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.driver.extraJavaOptions` | string | _(optional)_ | 1.0.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L95) |

### `spark.driver.extraLibraryPath.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.driver.extraLibraryPath` | string | _(optional)_ | 1.0.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L102) |

### `spark.driver.host.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.driver.host` | string | `Utils.localCanonicalHostName()` | 0.7.0 | Address of driver endpoints. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1178) |

### `spark.driver.log.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.driver.log.allowErasureCoding` | boolean | `false` | 3.0.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L200) |
| `spark.driver.log.dfsDir` | string | _(optional)_ | 3.0.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L185) |
| `spark.driver.log.layout` | string | _(optional)_ | 3.0.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L188) |
| `spark.driver.log.localDir` | string | _(optional)_ | 4.0.0 | Specifies a local directory to write driver logs and enable Driver Log UI Tab. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L178) |
| `spark.driver.log.persistToDfs.enabled` | boolean | `false` | 3.0.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L194) |
| `spark.driver.log.redirectConsoleOutputs` | string | `Seq("stdout", "stderr")` | 4.1.0 | Comma-separated list of the console output kind for driver that needs to redirect to logging system. Supported values are `stdout`, `stderr`. It only takes affect when `${PLUGINS.key}` is configured with `org.apache.spark.deploy.RedirectConsolePlugin`. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2892) |

### `spark.driver.maxResultSize.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.driver.maxResultSize` | bytes | `1g` | 1.2.0 | Size limit for results. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1634) |

### `spark.driver.memory.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.driver.memory` | bytes | `1g` | 1.1.1 | Amount of memory to use for the driver process, in MiB unless otherwise specified. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L119) |

### `spark.driver.memoryOverhead.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.driver.memoryOverhead` | bytes | _(optional)_ | 2.3.0 | The amount of non-heap memory to be allocated per driver in cluster mode, in MiB unless otherwise specified. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L125) |

### `spark.driver.memoryOverheadFactor.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.driver.memoryOverheadFactor` | double | `0.1` | 3.3.0 | Fraction of driver memory to be allocated as additional non-heap memory per driver process in cluster mode. This is memory that accounts for things like VM overheads, interned strings, other native overheads, etc. This tends to grow with the container size. This value defaults to 0.10 except for Kubernetes non-JVM jobs, which defaults to 0.40. This is done as non-JVM tasks need more non-JVM heap space and such tasks commonly fail with "Memory Overhead Exceeded" errors. This preempts this error with a higher default. This value is ignored if spark.driver.memoryOverhead is set directly. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L141) |

### `spark.driver.metrics.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.driver.metrics.pollingInterval` | fallback | → `EXECUTOR_HEARTBEAT_INTERVAL` | 4.1.0 | How often to collect driver metrics (in milliseconds). If unset, the polling is done at the executor heartbeat interval. If set, the polling is done at this interval. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1207) |

### `spark.driver.minMemoryOverhead.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.driver.minMemoryOverhead` | bytes | `384m` | 4.0.0 | The minimum amount of non-heap memory to be allocated per driver in cluster mode, in MiB unless otherwise specified. This value is ignored if spark.driver.memoryOverhead is set directly. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L132) |

### `spark.driver.port.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.driver.port` | int | `0` | 0.7.0 | Port of driver endpoints. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1184) |

### `spark.driver.resourcesFile.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.driver.resourcesFile` | string | _(optional)_ | 3.0.0 | Path to a file containing the resources allocated to the driver. The file should be formatted as a JSON array of ResourceAllocation objects. Only used internally in standalone mode. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L71) |

### `spark.driver.supervise.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.driver.supervise` | boolean | `false` | 1.3.0 | If true, restarts the driver automatically if it fails with a non-zero exit status. Only has effect in Spark standalone mode. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1190) |

### `spark.driver.timeout.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.driver.timeout` | time | `0min` | 4.0.0 | A timeout for Spark driver in minutes. 0 means infinite. For the positive time value, terminate the driver with the exit code 124 if it runs after timeout duration. To use, it's required to set `spark.plugins=org.apache.spark.deploy.DriverTimeoutPlugin`. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1197) |

### `spark.driver.userClassPathFirst.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.driver.userClassPathFirst` | boolean | `false` | 1.3.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L108) |

### `spark.dynamicAllocation.cachedExecutorIdleTimeout.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.dynamicAllocation.cachedExecutorIdleTimeout` | time | `Integer.MAX_VALUE` | 1.4.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L751) |

### `spark.dynamicAllocation.enabled.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.dynamicAllocation.enabled` | boolean | `false` | 1.2.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L716) |

### `spark.dynamicAllocation.executorAllocationRatio.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.dynamicAllocation.executorAllocationRatio` | double | `1.0` | 2.4.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L745) |

### `spark.dynamicAllocation.executorIdleTimeout.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.dynamicAllocation.executorIdleTimeout` | time | `60` | 1.2.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L758) |

### `spark.dynamicAllocation.initialExecutors.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.dynamicAllocation.initialExecutors` | fallback | → `DYN_ALLOCATION_MIN_EXECUTORS` | 1.3.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L734) |

### `spark.dynamicAllocation.maxExecutors.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.dynamicAllocation.maxExecutors` | int | `Int.MaxValue` | 1.2.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L739) |

### `spark.dynamicAllocation.minExecutors.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.dynamicAllocation.minExecutors` | int | `0` | 1.2.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L728) |

### `spark.dynamicAllocation.schedulerBacklogTimeout.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.dynamicAllocation.schedulerBacklogTimeout` | time | `1` | 1.2.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L778) |

### `spark.dynamicAllocation.shuffleTracking.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.dynamicAllocation.shuffleTracking.enabled` | boolean | `true` | 3.0.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L765) |
| `spark.dynamicAllocation.shuffleTracking.timeout` | time | `Long.MaxValue` | 3.0.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L771) |

### `spark.dynamicAllocation.sustainedSchedulerBacklogTimeout.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.dynamicAllocation.sustainedSchedulerBacklogTimeout` | fallback | → `DYN_ALLOCATION_SCHEDULER_BACKLOG_TIMEOUT` | 1.2.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L783) |

### `spark.dynamicAllocation.testing.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.dynamicAllocation.testing` | boolean | `false` | 1.2.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L722) |

### `spark.eventLog.buffer.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.eventLog.buffer.kb` | bytes | `100k` | 1.0.0 | Buffer size to use when writing to output streams, in KiB unless otherwise specified. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L248) |

### `spark.eventLog.compress.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.eventLog.compress` | boolean | `true` | 1.0.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L216) |

### `spark.eventLog.compression.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.eventLog.compression.codec` | string | `CompressionCodec.ZSTD` | 3.0.0 | The codec used to compress event log. By default, Spark provides four codecs: lz4, lzf, snappy, and zstd. You can also use fully qualified class names to specify the codec. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2165) |

### `spark.eventLog.dir.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.eventLog.dir` | string | `EventLoggingListener.DEFAULT_LOG_DIR` | 1.0.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L210) |

### `spark.eventLog.enabled.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.eventLog.enabled` | boolean | `false` | 1.0.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L205) |

### `spark.eventLog.erasureCoding.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.eventLog.erasureCoding.enabled` | boolean | `false` | 3.0.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L236) |

### `spark.eventLog.excludedPatterns.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.eventLog.excludedPatterns` | string | `Nil` | 4.1.0 | Specifies comma-separated event names to be excluded from the event logs. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L228) |

### `spark.eventLog.gcMetrics.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.eventLog.gcMetrics.oldGenerationGarbageCollectors` | string | `GarbageCollectionMetrics.OLD_GENERATION_BUILTIN_GARBAGE_COLLECTORS` | 3.0.0 | Names of supported old generation garbage collector. A name usually is the return of GarbageCollectorMXBean.getName. The built-in old generation garbage collectors are ${GarbageCollectionMetrics.OLD_GENERATION_BUILTIN_GARBAGE_COLLECTORS} | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L273) |
| `spark.eventLog.gcMetrics.youngGenerationGarbageCollectors` | string | `GarbageCollectionMetrics.YOUNG_GENERATION_BUILTIN_GARBAGE_COLLECTORS` | 3.0.0 | Names of supported young generation garbage collector. A name usually is the return of GarbageCollectorMXBean.getName. The built-in young generation garbage collectors are ${GarbageCollectionMetrics.YOUNG_GENERATION_BUILTIN_GARBAGE_COLLECTORS} | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L263) |

### `spark.eventLog.includeTaskMetricsAccumulators.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.eventLog.includeTaskMetricsAccumulators` | boolean | `true` | 4.0.0 | Whether to include TaskMetrics' underlying accumulator values in the event log (as part of the Task/Stage/Job metrics' 'Accumulables' fields. The TaskMetrics values are already logged in the 'Task Metrics' fields (so the accumulator updates are redundant). This flag defaults to true for behavioral backwards compatibility for applications that might rely on the redundant logging. See SPARK-42204 for details. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L283) |

### `spark.eventLog.logBlockUpdates.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.eventLog.logBlockUpdates.enabled` | boolean | `false` | 2.3.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L222) |

### `spark.eventLog.logStageExecutorMetrics.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.eventLog.logStageExecutorMetrics` | boolean | `false` | 3.0.0 | Whether to write per-stage peaks of executor metrics (for each executor) to the event log. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L255) |

### `spark.eventLog.longForm.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.eventLog.longForm.enabled` | boolean | `false` | 2.4.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L301) |

### `spark.eventLog.overwrite.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.eventLog.overwrite` | boolean | `false` | 1.0.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L295) |

### `spark.eventLog.rolling.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.eventLog.rolling.enabled` | boolean | `true` | 3.0.0 | Whether rolling over event log files is enabled. If set to true, it cuts down each event log file to the configured size. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L307) |
| `spark.eventLog.rolling.maxFileSize` | bytes | `128m` | 3.0.0 | When ${EVENT_LOG_ENABLE_ROLLING.key}=true, specifies the max size of event log file to be rolled over. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L315) |

### `spark.eventLog.testing.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.eventLog.testing` | boolean | `false` | 1.0.1 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L242) |

### `spark.excludeOnFailure.application.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.excludeOnFailure.application.enabled` | boolean | _(optional)_ | 4.0.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L962) |
| `spark.excludeOnFailure.application.fetchFailure.enabled` | boolean | `false` | 3.1.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1036) |
| `spark.excludeOnFailure.application.maxFailedExecutorsPerNode` | int | `2` | 3.1.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L998) |
| `spark.excludeOnFailure.application.maxFailedTasksPerExecutor` | int | `2` | 3.1.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L986) |

### `spark.excludeOnFailure.enabled.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.excludeOnFailure.enabled` | boolean | _(optional)_ | 3.1.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L956) |

### `spark.excludeOnFailure.killExcludedExecutors.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.excludeOnFailure.killExcludedExecutors` | boolean | `false` | 3.1.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1016) |
| `spark.excludeOnFailure.killExcludedExecutors.decommission` | boolean | `false` | 3.2.0 | Attempt decommission of excluded nodes instead of going directly to kill | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1022) |

### `spark.excludeOnFailure.stage.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.excludeOnFailure.stage.maxFailedExecutorsPerNode` | int | `2` | 3.1.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1004) |
| `spark.excludeOnFailure.stage.maxFailedTasksPerExecutor` | int | `2` | 3.1.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L992) |

### `spark.excludeOnFailure.task.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.excludeOnFailure.task.maxTaskAttemptsPerExecutor` | int | `1` | 3.1.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L974) |
| `spark.excludeOnFailure.task.maxTaskAttemptsPerNode` | int | `2` | 3.1.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L980) |

### `spark.excludeOnFailure.taskAndStage.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.excludeOnFailure.taskAndStage.enabled` | boolean | _(optional)_ | 4.0.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L968) |

### `spark.excludeOnFailure.timeout.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.excludeOnFailure.timeout` | time | _(optional)_ | 3.1.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1010) |

### `spark.executor.allowSparkContext.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.executor.allowSparkContext` | boolean | `false` | 3.0.1 | If set to true, SparkContext can be created in executors. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2517) |

### `spark.executor.cores.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.executor.cores` | int | `1` | 1.0.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L413) |

### `spark.executor.decommission.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.executor.decommission.forceKillTimeout` | time | _(optional)_ | 3.2.0 | Duration after which a Spark will force a decommissioning executor to exit. this should be set to a high value in most situations as low values will prevent block migrations from having enough time to complete. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2469) |
| `spark.executor.decommission.killInterval` | time | _(optional)_ | 3.1.0 | Duration after which a decommissioned executor will be killed forcefully *by an outside* (e.g. non-spark) service. This config is useful for cloud environments where we know in advance when an executor is going to go down after decommissioning signal i.e. around 2 mins in aws spot nodes, 1/2 hrs in spot block nodes etc. This config is currently used to decide what tasks running on decommission executors to speculate. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2457) |
| `spark.executor.decommission.signal` | string | `PWR` | 3.2.0 | The signal that used to trigger the executor to start decommission. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2478) |

### `spark.executor.defaultExtraClassPath.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.executor.defaultExtraClassPath` | string | `SparkLauncher.EXECUTOR_DEFAULT_EXTRA_CLASS_PATH_VALUE` | 4.0.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L328) |

### `spark.executor.extraClassPath.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.executor.extraClassPath` | string | _(optional)_ | 1.0.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L335) |

### `spark.executor.extraJavaOptions.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.executor.extraJavaOptions` | string | _(optional)_ | 1.0.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L395) |

### `spark.executor.extraLibraryPath.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.executor.extraLibraryPath` | string | _(optional)_ | 1.0.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L402) |

### `spark.executor.failuresValidityInterval.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.executor.failuresValidityInterval` | time | _(optional)_ | 3.5.0 | Interval after which executor failures will be considered independent and not accumulate towards the attempt count. This configuration only takes effect on YARN and Kubernetes. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1050) |

### `spark.executor.heartbeat.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.executor.heartbeat.dropZeroAccumulatorUpdates` | boolean | `true` | 3.0.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L342) |
| `spark.executor.heartbeat.maxFailures` | int | `60` | 1.6.2 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L355) |

### `spark.executor.heartbeatInterval.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.executor.heartbeatInterval` | time | `10s` | 1.1.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L349) |

### `spark.executor.id.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.executor.id` | string | _(optional)_ | 1.2.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L325) |

### `spark.executor.instances.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.executor.instances` | int | _(optional)_ | 1.0.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L905) |

### `spark.executor.isolatedSessionCache.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.executor.isolatedSessionCache.size` | int | `100` | 4.1.0 | Maximum number of isolated sessions to cache in the executor. Each cached session maintains its own classloader for artifact isolation. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L362) |

### `spark.executor.killOnFatalError.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.executor.killOnFatalError.depth` | int | `5` | 3.1.0 | The max depth of the exception chain in a failed task Spark will search for a fatal error to check whether it should kill the JVM process. 0 means not checking any fatal error, 1 means checking only the exception but not the cause, and so on. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2532) |

### `spark.executor.logs.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.executor.logs.redirectConsoleOutputs` | string | `Seq("stdout", "stderr")` | 4.1.0 | Comma-separated list of the console output kind for executor that needs to redirect to logging system. Supported values are `stdout`, `stderr`. It only takes affect when `${PLUGINS.key}` is configured with `org.apache.spark.deploy.RedirectConsolePlugin`. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2905) |
| `spark.executor.logs.rolling.enableCompression` | boolean | `false` | 2.0.2 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2005) |
| `spark.executor.logs.rolling.maxRetainedFiles` | int | `-1` | 1.1.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1999) |
| `spark.executor.logs.rolling.maxSize` | string | `(1024 * 1024).toString` | 1.4.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1993) |
| `spark.executor.logs.rolling.strategy` | string | — | 1.1.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1981) |
| `spark.executor.logs.rolling.time.interval` | string | `daily` | 1.1.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1987) |

### `spark.executor.maxNumFailures.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.executor.maxNumFailures` | int | _(optional)_ | 3.5.0 | The maximum number of executor failures before failing the application. This configuration only takes effect on YARN and Kubernetes. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1042) |

### `spark.executor.memory.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.executor.memory` | bytes | `1g` | 0.7.0 | Amount of memory to use per executor process, in MiB unless otherwise specified. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L418) |

### `spark.executor.memoryOverhead.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.executor.memoryOverhead` | bytes | _(optional)_ | 2.3.0 | The amount of non-heap memory to be allocated per executor, in MiB unless otherwise specified. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L424) |

### `spark.executor.memoryOverheadFactor.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.executor.memoryOverheadFactor` | double | `0.1` | 3.3.0 | Fraction of executor memory to be allocated as additional non-heap memory per executor process. This is memory that accounts for things like VM overheads, interned strings, other native overheads, etc. This tends to grow with the container size. This value defaults to 0.10 except for Kubernetes non-JVM jobs, which defaults to 0.40. This is done as non-JVM tasks need more non-JVM heap space and such tasks commonly fail with "Memory Overhead Exceeded" errors. This preempts this error with a higher default. This value is ignored if spark.executor.memoryOverhead is set directly. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L441) |

### `spark.executor.metrics.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.executor.metrics.fileSystemSchemes` | string | `file,hdfs` | 3.1.0 | The file system schemes to report in executor metrics. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L388) |
| `spark.executor.metrics.pollingInterval` | time | `0` | 3.0.0 | How often to collect executor metrics (in milliseconds). If 0, the polling is done on executor heartbeats. If positive, the polling is done at this interval. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L379) |

### `spark.executor.minMemoryOverhead.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.executor.minMemoryOverhead` | bytes | `384m` | 4.0.0 | The minimum amount of non-heap memory to be allocated per executor in MiB unless otherwise specified. This value is ignored if spark.executor.memoryOverhead is set directly. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L432) |

### `spark.executor.processTreeMetrics.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.executor.processTreeMetrics.enabled` | boolean | `false` | 3.0.0 | Whether to collect process tree metrics (from the /proc filesystem) when collecting executor metrics. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L371) |

### `spark.executor.pyspark.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.executor.pyspark.memory` | bytes | _(optional)_ | 2.4.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Python.scala#L54) |

### `spark.executor.python.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.executor.python.worker.log.details` | boolean | `false` | 3.5.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Python.scala#L39) |

### `spark.executor.syncLogLevel.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.executor.syncLogLevel.enabled` | boolean | `false` | 4.0.0 | If set to true, log level applied through SparkContext.setLogLevel() method will be propagated to all executors. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2524) |

### `spark.executor.userClassPathFirst.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.executor.userClassPathFirst` | boolean | `false` | 1.3.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L408) |

### `spark.extraListeners.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.extraListeners` | string | _(optional)_ | 1.3.0 | Class names of listeners to add to SparkContext during initialization. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1593) |

### `spark.file.transferTo.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.file.transferTo` | boolean | `true` | 1.4.0 | If true, NIO's `transferTo` API will be preferentially used when merging Spark shuffle spill files | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1728) |

### `spark.files.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.files` | string | `Nil` | 1.0.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2296) |

### `spark.files.fetchFailure.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.files.fetchFailure.unRegisterOutputOnHost` | boolean | `false` | 2.3.0 | Whether to un-register all the outputs on the host in condition that we receive a FetchFailure. This is set default to false, which means, we only un-register the outputs related to the exact executor(instead of the host) on a FetchFailure. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1059) |

### `spark.files.ignoreCorruptFiles.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.files.ignoreCorruptFiles` | boolean | `false` | 2.1.0 | Whether to ignore corrupt files. If true, the Spark jobs will continue to run when encountering corrupted or non-existing files and contents that have been read will still be returned. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1230) |

### `spark.files.ignoreMissingFiles.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.files.ignoreMissingFiles` | boolean | `false` | 2.4.0 | Whether to ignore missing files. If true, the Spark jobs will continue to run when encountering missing files and the contents that have been read will still be returned. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1238) |

### `spark.files.maxPartitionBytes.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.files.maxPartitionBytes` | bytes | `128 * 1024 * 1024` | 2.1.0 | The maximum number of bytes to pack into a single partition when reading files. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1263) |

### `spark.files.openCostInBytes.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.files.openCostInBytes` | bytes | `4 * 1024 * 1024` | 2.1.0 | The estimated cost to open a file, measured by the number of bytes could be scanned in the same time. This is used when putting multiple files into a partition. It's better to over estimate, then the partitions with small files will be faster than partitions with bigger files. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1269) |

### `spark.hadoopRDD.ignoreEmptySplits.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.hadoopRDD.ignoreEmptySplits` | boolean | `true` | 2.3.0 | When true, HadoopRDD/NewHadoopRDD will not create partitions for empty input splits. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1279) |

### `spark.history.custom.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.history.custom.executor.log.url` | string | _(optional)_ | 3.0.0 | Specifies custom spark executor log url for supporting external log service instead of using cluster managers' application log urls in the history server. Spark will support some path variables via patterns which can vary on cluster manager. Please check the documentation for your cluster manager to see which patterns are supported, if any. This configuration has no effect on a live application, it only affects the history server. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/History.scala#L273) |
| `spark.history.custom.executor.log.url.applyIncompleteApplication` | boolean | `true` | 3.0.0 | Whether to apply custom executor log url, as specified by ${CUSTOM_EXECUTOR_LOG_URL.key}, to incomplete application as well. Even if this is true, this still only affects the behavior of the history server, not running spark applications. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/History.scala#L284) |

### `spark.history.fs.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.history.fs.cleaner.enabled` | boolean | `false` | 1.4.0 | Whether the History Server should periodically clean up event logs from storage | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/History.scala#L57) |
| `spark.history.fs.cleaner.interval` | time | `1d` | 1.4.0 | When spark.history.fs.cleaner.enabled=true, specifies how often the filesystem job history cleaner checks for files to delete. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/History.scala#L63) |
| `spark.history.fs.cleaner.maxAge` | time | `7d` | 1.4.0 | When spark.history.fs.cleaner.enabled=true, history files older than this will be deleted when the filesystem history cleaner runs. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/History.scala#L70) |
| `spark.history.fs.cleaner.maxNum` | int | `Int.MaxValue` | 3.0.0 | The maximum number of log files in the event log directory. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/History.scala#L77) |
| `spark.history.fs.driverlog.cleaner.enabled` | fallback | → `CLEANER_ENABLED` | 3.0.0 | Specifies whether the History Server should periodically clean up driver logs from storage. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/History.scala#L169) |
| `spark.history.fs.driverlog.cleaner.interval` | fallback | → `CLEANER_INTERVAL_S` | 3.0.0 | When ${DRIVER_LOG_CLEANER_ENABLED.key}=true, specifies how often the filesystem driver log cleaner checks for files to delete. Files are only deleted if they are older than ${MAX_DRIVER_LOG_AGE_S.key}. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/History.scala#L181) |
| `spark.history.fs.driverlog.cleaner.maxAge` | fallback | → `MAX_LOG_AGE_S` | 3.0.0 | When ${DRIVER_LOG_CLEANER_ENABLED.key}=true, driver log files older than this will be deleted when the driver log cleaner runs. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/History.scala#L175) |
| `spark.history.fs.endEventReparseChunkSize` | bytes | `1m` | 2.4.0 | How many bytes to parse at the end of log files looking for the end event. This is used to speed up generation of application listings by skipping unnecessary parts of event log files. It can be disabled by setting this config to 0. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/History.scala#L133) |
| `spark.history.fs.eventLog.rolling.compaction.score.threshold` | double | `0.7` | 3.0.0 | The threshold score to determine whether it's good to do the compaction or not. The compaction score is calculated in analyzing, and being compared to this value. Compaction will proceed only when the score is higher than the threshold value. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/History.scala#L153) |
| `spark.history.fs.eventLog.rolling.maxFilesToRetain` | int | `Integer.MAX_VALUE` | 3.0.0 | The maximum number of event log files which will be retained as non-compacted. By default, all event log files will be retained. Please set the configuration and ${EVENT_LOG_ROLLING_MAX_FILE_SIZE.key} accordingly if you want to control the overall size of event log files. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/History.scala#L142) |
| `spark.history.fs.eventLog.rolling.onDemandLoadEnabled` | boolean | `true` | 4.1.0 | Whether to look up rolling event log locations on demand manner before listing files. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/History.scala#L163) |
| `spark.history.fs.inProgressOptimization.enabled` | boolean | `true` | 2.4.0 | Enable optimized handling of in-progress logs. This option may leave finished applications that fail to rename their event logs listed as in-progress. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/History.scala#L125) |
| `spark.history.fs.logDirectory` | string | `DEFAULT_LOG_DIR` | 1.1.0 | Directory where app logs are stored | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/History.scala#L29) |
| `spark.history.fs.numCompactThreads` | int | `() => Math.ceil(Runtime.getRuntime.availableProcessors() / 4f).toInt` | 4.1.0 | Number of threads that will be used by history server to compact event logs. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/History.scala#L231) |
| `spark.history.fs.numReplayThreads` | int | `() => Math.ceil(Runtime.getRuntime.availableProcessors() / 4f).toInt` | 2.0.0 | Number of threads that will be used by history server to process event logs. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/History.scala#L225) |
| `spark.history.fs.safemodeCheck.interval` | time | `5s` | 1.6.0 | Interval between HDFS safemode checks for the event log directory | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/History.scala#L35) |
| `spark.history.fs.update.batchSize` | int | `Int.MaxValue` | 3.4.0 | Specifies the batch size for updating new eventlog files. This controls each scan process to be completed within a reasonable time, and such prevent the initial scan from running too long and blocking new eventlog files to be scanned in time in large environments. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/History.scala#L47) |
| `spark.history.fs.update.interval` | time | `10s` | 1.4.0 | How often(in seconds) to reload log data from storage | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/History.scala#L41) |

### `spark.history.kerberos.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.history.kerberos.enabled` | boolean | `false` | 1.0.1 | Indicates whether the history server should use kerberos to login. This is required if the history server is accessing HDFS files on a secure Hadoop cluster. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/History.scala#L252) |
| `spark.history.kerberos.keytab` | string | _(optional)_ | 1.0.1 | When ${KERBEROS_ENABLED.key}=true, specifies location of the kerberos keytab file for the History Server. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/History.scala#L266) |
| `spark.history.kerberos.principal` | string | _(optional)_ | 1.0.1 | When ${KERBEROS_ENABLED.key}=true, specifies kerberos principal name for the History Server. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/History.scala#L259) |

### `spark.history.provider.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.history.provider` | string | `org.apache.spark.deploy.history.FsHistoryProvider` | 1.1.0 | Name of the class implementing the application history backend. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/History.scala#L246) |

### `spark.history.retainedApplications.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.history.retainedApplications` | int | `50` | 1.0.0 | The number of applications to retain UI data for in the cache. If this cap is exceeded, then the oldest applications will be removed from the cache. If an application is not in the cache, it will have to be loaded from disk if it is accessed from the UI. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/History.scala#L237) |

### `spark.history.store.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.history.store.hybridStore.diskBackend` | string | `HybridStoreDiskBackend.ROCKSDB.toString` | 3.3.0 | Specifies a disk-based store used in hybrid store; ROCKSDB or LEVELDB (deprecated). | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/History.scala#L313) |
| `spark.history.store.hybridStore.enabled` | boolean | `false` | 3.1.0 | Whether to use HybridStore as the store when parsing event logs. HybridStore will first write data to an in-memory store and having a background thread that dumps data to a disk store after the writing to in-memory store is completed. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/History.scala#L293) |
| `spark.history.store.hybridStore.maxMemoryUsage` | bytes | `2g` | 3.1.0 | Maximum memory space that can be used to create HybridStore. The HybridStore co-uses the heap memory, so the heap memory should be increased through the memory option for SHS if the HybridStore is enabled. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/History.scala#L301) |
| `spark.history.store.maxDiskUsage` | bytes | `10g` | 2.3.0 | Maximum disk usage for the local directory where the cache application history information are stored. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/History.scala#L105) |
| `spark.history.store.path` | string | _(optional)_ | 2.3.0 | Local directory where to cache application history information. By default this is not set, meaning all history information will be kept in memory. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/History.scala#L83) |
| `spark.history.store.serializer` | string | `LocalStoreSerializer.JSON.toString` | 3.4.0 | Serializer for writing/reading in-memory UI objects to/from disk-based KV Store; JSON or PROTOBUF. JSON serializer is the only choice before Spark 3.4.0, thus it is the default value. PROTOBUF serializer is fast and compact, and it is the default serializer for disk-based KV store of live UI. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/History.scala#L94) |

### `spark.history.ui.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.history.ui.acls.enable` | boolean | `false` | 1.0.1 | Specifies whether ACLs should be checked to authorize users viewing the applications in the history server. If enabled, access control checks are performed regardless of what the individual applications had set for spark.ui.acls.enable. The application owner will always have authorization to view their own application and any users specified via spark.ui.view.acls and groups specified via spark.ui.view.acls.groups when the application was run will also have authorization to view that application. If disabled, no access control checks are made for any application UIs available through the history server. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/History.scala#L188) |
| `spark.history.ui.admin.acls` | string | `Nil` | 2.1.1 | Comma separated list of users that have view access to all the Spark applications in history server. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/History.scala#L201) |
| `spark.history.ui.admin.acls.groups` | string | `Nil` | 2.1.1 | Comma separated list of groups that have view access to all the Spark applications in history server. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/History.scala#L209) |
| `spark.history.ui.maxApplications` | int | `Integer.MAX_VALUE` | 2.0.1 | The number of applications to display on the history summary page. Application UIs are still available by accessing their URLs directly even if they are not displayed on the history summary page. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/History.scala#L217) |
| `spark.history.ui.port` | int | `18080` | 1.0.0 | Web UI port to bind Spark History Server | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/History.scala#L118) |
| `spark.history.ui.title` | string | `History Server` | 4.0.0 | Specifies the title of the History Server UI page. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/History.scala#L112) |

### `spark.io.compression.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.io.compression.codec` | string | `CompressionCodec.LZ4` | 0.8.0 | The codec used to compress internal data such as RDD partitions, event log, broadcast variables and shuffle outputs. By default, Spark provides four codecs: lz4, lzf, snappy, and zstd. You can also use fully qualified class names to specify the codec | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2094) |
| `spark.io.compression.lz4.blockSize` | bytes | `32k` | 1.4.0 | Block size in bytes used in LZ4 compression, in the case when LZ4 compressioncodec is used. Lowering this block size will also lower shuffle memory usage when LZ4 is used. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2085) |
| `spark.io.compression.lzf.parallel.enabled` | boolean | `true` | 4.0.0 | When true, LZF compression will use multiple threads to compress data in parallel. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2149) |
| `spark.io.compression.snappy.blockSize` | bytes | `32k` | 1.4.0 | Block size in bytes used in Snappy compression, in the case when Snappy compression codec is used. Lowering this block size will also lower shuffle memory usage when Snappy is used | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2076) |
| `spark.io.compression.zstd.bufferPool.enabled` | boolean | `true` | 3.2.0 | If true, enable buffer pool of ZSTD JNI library. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2114) |
| `spark.io.compression.zstd.bufferSize` | bytes | `32k` | 2.3.0 | Buffer size in bytes used in Zstd compression, in the case when Zstd compression codec is used. Lowering this size will lower the shuffle memory usage when Zstd is used, but it might increase the compression cost because of excessive JNI call overhead | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2104) |
| `spark.io.compression.zstd.level` | int | `1` | 2.3.0 | Compression level for Zstd compression codec. Increasing the compression level will result in better compression at the expense of more CPU and memory | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2132) |
| `spark.io.compression.zstd.strategy` | int | _(optional)_ | 4.1.0 | Compression strategy for Zstd compression codec. The higher the value is, the more complex it becomes, usually resulting stronger but slower compression or higher CPU cost. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2140) |
| `spark.io.compression.zstd.workers` | int | `0` | 4.0.0 | Thread size spawned to compress in parallel when using Zstd. When the value is 0, no worker is spawned, it works in single-threaded mode. When value > 0, it triggers asynchronous mode, corresponding number of threads are spawned. More workers improve performance, but also increase memory cost. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2121) |

### `spark.io.crypto.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.io.crypto.cipher.transformation` | string | `AES/CTR/NoPadding` | 2.1.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1172) |

### `spark.io.encryption.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.io.encryption.enabled` | boolean | `false` | 2.1.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1154) |
| `spark.io.encryption.keySizeBits` | int | `128` | 2.1.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1165) |
| `spark.io.encryption.keygen.algorithm` | string | `HmacSHA1` | 2.1.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1160) |

### `spark.io.warning.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.io.warning.largeFileThreshold` | bytes | `1024 * 1024 * 1024` | 3.0.0 | If the size in bytes of a file loaded by Spark exceeds this threshold, a warning is logged with the possible reasons. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2156) |

### `spark.jars.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.jars` | string | `Nil` | 0.9.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2290) |

### `spark.jars.excludes.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.jars.excludes` | string | `Nil` | 1.5.0 | Comma-separated list of groupId:artifactId, to exclude while resolving the dependencies provided in spark.jars.packages to avoid dependency conflicts. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2752) |

### `spark.jars.ivy.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.jars.ivy` | string | `~/.ivy2.5.2` | 1.3.0 | Path to specify the Ivy user directory, used for the local Ivy cache and package files from spark.jars.packages. This will override the Ivy property ivy.default.ivy.user.dir which defaults to ~/.ivy2.5.2 | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2715) |

### `spark.jars.ivySettings.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.jars.ivySettings` | string | _(optional)_ | 2.2.0 | Path to an Ivy settings file to customize resolution of jars specified using spark.jars.packages instead of the built-in defaults, such as maven central. Additional repositories given by the command-line option --repositories or spark.jars.repositories will also be included. Useful for allowing Spark to resolve artifacts from behind a firewall e.g. via an in-house artifact server like Artifactory. Details on the settings file format can be found at Settings Files | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2725) |

### `spark.jars.packages.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.jars.packages` | string | `Nil` | 1.5.0 | Comma-separated list of Maven coordinates of jars to include on the driver and executor classpaths. The coordinates should be groupId:artifactId:version. If spark.jars.ivySettings is given artifacts will be resolved according to the configuration in the file, otherwise artifacts will be searched for in the local maven repo, then maven central and finally any additional remote repositories given by the command-line option --repositories. For more details, see Advanced Dependency Management. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2738) |

### `spark.jars.repositories.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.jars.repositories` | string | `Nil` | 2.3.0 | Comma-separated list of additional remote repositories to search for the maven coordinates given with --packages or spark.jars.packages. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2762) |

### `spark.kerberos.access.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.kerberos.access.hadoopFileSystems` | string | `Nil` | 3.0.0 | Extra Hadoop filesystem URLs for which to request delegation tokens. The filesystem that hosts fs.defaultFS does not need to be listed here. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L885) |

### `spark.kerberos.keytab.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.kerberos.keytab` | string | _(optional)_ | 3.0.0 | Location of user's keytab. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L857) |

### `spark.kerberos.principal.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.kerberos.principal` | string | _(optional)_ | 3.0.0 | Name of the Kerberos principal. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L862) |

### `spark.kerberos.relogin.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.kerberos.relogin.period` | time | `1m` | 3.0.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L868) |

### `spark.kerberos.renewal.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.kerberos.renewal.credentials` | string | `keytab` | 3.0.0 | Which credentials to use when renewing delegation tokens for executors. Can be either 'keytab', the default, which requires a keytab to be provided, or 'ccache', which uses the local credentials cache. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L874) |

### `spark.kryo.classesToRegister.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.kryo.classesToRegister` | string | `Nil` | 1.2.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Kryo.scala#L35) |

### `spark.kryo.pool.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.kryo.pool` | boolean | `true` | 3.0.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Kryo.scala#L46) |

### `spark.kryo.referenceTracking.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.kryo.referenceTracking` | boolean | `true` | 0.8.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Kryo.scala#L51) |

### `spark.kryo.registrationRequired.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.kryo.registrationRequired` | boolean | `false` | 1.1.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Kryo.scala#L24) |

### `spark.kryo.registrator.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.kryo.registrator` | string | `Nil` | 0.5.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Kryo.scala#L29) |

### `spark.kryo.unsafe.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.kryo.unsafe` | boolean | `true` | 2.1.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Kryo.scala#L41) |

### `spark.kryoserializer.buffer.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.kryoserializer.buffer` | bytes | `64k` | 1.4.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Kryo.scala#L56) |
| `spark.kryoserializer.buffer.max` | bytes | `64m` | 1.4.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Kryo.scala#L61) |

### `spark.kubernetes.jars.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.kubernetes.jars.avoidDownloadSchemes` | string | `Nil` | 4.0.0 | Comma-separated list of schemes for which jars will NOT be downloaded to the driver local disk prior to be distributed to executors, only for kubernetes deployment. For use in cases when the jars are big and executor counts are high, concurrent download causes network saturation and timeouts. Wildcard '*' is denoted to not downloading jars for any the schemes. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1570) |

### `spark.locality.wait.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.locality.wait` | time | `3s` | 0.5.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L797) |
| `spark.locality.wait.legacyResetOnTaskLaunch` | boolean | `false` | 3.1.0 | Whether to use the legacy behavior of locality wait, which resets the delay timer anytime a task is scheduled. See Delay Scheduling section of TaskSchedulerImpl's class documentation for more details. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L788) |
| `spark.locality.wait.node` | fallback | → `LOCALITY_WAIT` | 0.8.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2184) |
| `spark.locality.wait.process` | fallback | → `LOCALITY_WAIT` | 0.8.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2180) |
| `spark.locality.wait.rack` | fallback | → `LOCALITY_WAIT` | 0.8.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2188) |

### `spark.log.callerContext.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.log.callerContext` | string | _(optional)_ | 2.2.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1245) |

### `spark.log.legacyTaskNameMdc.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.log.legacyTaskNameMdc.enabled` | boolean | `false` | 4.0.0 | When true, the MDC (Mapped Diagnostic Context) key `mdc.taskName` will be set in the log output, which is the behavior of Spark version 3.1 through Spark 3.5 releases. When false, the logging framework will use `task_name` as the MDC key, aligning it with the naming convention of newer MDC keys introduced in Spark 4.0 release. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L168) |

### `spark.log.level.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.log.level` | string | _(optional)_ | 3.5.0 | When set, overrides any user-defined log settings as if calling SparkContext.setLogLevel() at Spark startup. Valid log levels include: , | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1250) |

### `spark.log.structuredLogging.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.log.structuredLogging.enabled` | boolean | `false` | 4.0.0 | When true, Spark logs are output as structured JSON lines with added Spark Mapped Diagnostic Context (MDC), facilitating easier integration with log aggregation and analysis tools. When false, logs are plain text without MDC. This configuration does not apply to interactive environments such as spark-shell, spark-sql, and PySpark shell. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L157) |

### `spark.master.rest.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.master.rest.enabled` | boolean | `true` | 1.3.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2010) |
| `spark.master.rest.filters` | string | `Nil` | 4.0.0 | Comma separated list of filter class names to apply to the Spark Master REST API. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2032) |
| `spark.master.rest.host` | string | _(optional)_ | 4.0.0 | Specifies the host of the Master REST API endpoint | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2015) |
| `spark.master.rest.maxThreads` | int | `200` | 4.0.0 | Maximum number of threads to use in the Spark Master REST API Server. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2026) |
| `spark.master.rest.port` | int | `6066` | 1.3.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2021) |
| `spark.master.rest.virtualThread.enabled` | boolean | `false` | 4.0.0 | If true, Spark master tries to use Java 21 virtual thread for REST API. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2040) |

### `spark.master.ui.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.master.ui.decommission.allow.mode` | string | `LOCAL` | 3.1.0 | Specifies the behavior of the Master Web UI's /workers/kill endpoint. Possible choices are: `LOCAL` means allow this endpoint from IP's that are local to the machine running the Master, `DENY` means to completely disable this endpoint, `ALLOW` means to allow calling this endpoint from any IP. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/UI.scala#L235) |
| `spark.master.ui.historyServerUrl` | string | _(optional)_ | 4.0.0 | The URL where Spark history server is running. Please note that this assumes that all Spark jobs share the same event log location where the history server accesses. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2052) |
| `spark.master.ui.port` | int | `8080` | 1.1.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2046) |
| `spark.master.ui.title` | string | _(optional)_ | 4.0.0 | Specifies the title of the Master UI page. If unset, `Spark Master at <MasterURL>` is used by default. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/UI.scala#L247) |
| `spark.master.ui.visibleEnvVarPrefixes` | string | `Seq.empty[String]` | 4.0.0 | Comma-separated list of key-prefix strings to show environment variables | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/UI.scala#L254) |

### `spark.master.useAppNameAsAppId.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.master.useAppNameAsAppId.enabled` | boolean | `false` | 4.0.0 | (Experimental) If true, Spark master uses the user-provided appName for appId. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2060) |

### `spark.master.useDriverIdAsAppName.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.master.useDriverIdAsAppName.enabled` | boolean | `false` | 4.0.0 | (Experimental) If true, Spark master tries to set driver ID as appName. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2068) |

### `spark.memory.fraction.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.memory.fraction` | double | `0.6` | 1.6.0 | Fraction of (heap space - 300MB) used for execution and storage. The lower this is, the more frequently spills and cached data eviction occur. The purpose of this config is to set aside memory for internal metadata, user data structures, and imprecise size estimation in the case of sparse, unusually large records. Leaving this at the default value is recommended. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L494) |

### `spark.memory.offHeap.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.memory.offHeap.enabled` | boolean | `false` | 1.6.0 | If true, Spark will attempt to use off-heap memory for certain operations. If off-heap memory use is enabled, then spark.memory.offHeap.size must be positive. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L465) |
| `spark.memory.offHeap.size` | bytes | `0` | 1.6.0 | The absolute amount of memory which can be used for off-heap allocation, in bytes unless otherwise specified. This setting has no impact on heap memory usage, so if your executors' total memory consumption must fit within some hard limit then be sure to shrink your JVM heap size accordingly. This must be set to a positive value when spark.memory.offHeap.enabled=true. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L473) |

### `spark.memory.storageFraction.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.memory.storageFraction` | double | `0.5` | 1.6.0 | Amount of storage memory immune to eviction, expressed as a fraction of the size of the region set aside by spark.memory.fraction. The higher this is, the less working memory may be available to execution and tasks may spill to disk more often. Leaving this at the default value is recommended. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L484) |

### `spark.memory.unmanagedMemoryPollingInterval.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.memory.unmanagedMemoryPollingInterval` | time | `0s` | 4.1.0 | Interval for polling unmanaged memory users to track their memory usage. Unmanaged memory users are components that manage their own memory outside of Spark's core memory management, such as RocksDB for Streaming State Store. Setting this to 0 disables unmanaged memory polling. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L505) |

### `spark.metrics.appStatusSource.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.metrics.appStatusSource.enabled` | boolean | `true` | 3.0.0 | Whether Dropwizard/Codahale metrics will be reported for the status of the running spark app. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Status.scala#L67) |

### `spark.metrics.conf.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.metrics.conf` | string | _(optional)_ | 0.8.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1125) |

### `spark.metrics.executorMetricsSource.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.metrics.executorMetricsSource.enabled` | boolean | `true` | 3.0.0 | Whether to register the ExecutorMetrics source with the metrics system. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1131) |

### `spark.metrics.namespace.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.metrics.namespace` | string | _(optional)_ | 2.1.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1120) |

### `spark.metrics.staticSources.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.metrics.staticSources.enabled` | boolean | `true` | 3.0.0 | Whether to register static sources with the metrics system. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1138) |

### `spark.modify.acls.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.modify.acls` | string | `Nil` | 1.1.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/UI.scala#L200) |
| `spark.modify.acls.groups` | string | `Nil` | 2.0.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/UI.scala#L206) |

### `spark.network.crypto.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.network.crypto.enabled` | boolean | `false` | 2.2.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Network.scala#L31) |
| `spark.network.crypto.saslFallback` | boolean | `true` | 2.2.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Network.scala#L25) |

### `spark.network.maxRemoteBlockSizeFetchToMem.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.network.maxRemoteBlockSizeFetchToMem` | bytes | `200m` | 3.0.0 | Remote block will be fetched to disk when size of the block is above this threshold in bytes. This is to avoid a giant request takes too much memory. Note this configuration will affect both shuffle fetch and block manager remote block fetch. For users who enabled external shuffle service, this feature can only work when external shuffle service is at least 2.3.0. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1472) |

### `spark.network.remoteReadNioBufferConversion.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.network.remoteReadNioBufferConversion` | boolean | `false` | 2.4.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Network.scala#L37) |

### `spark.network.timeout.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.network.timeout` | time | `120s` | 1.3.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Network.scala#L43) |

### `spark.network.timeoutInterval.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.network.timeoutInterval` | time | `STORAGE_BLOCKMANAGER_TIMEOUTINTERVAL.defaultValueString` | 1.3.2 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Network.scala#L49) |

### `spark.plugins.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.plugins` | string | `Nil` | 3.0.0 | Comma-separated list of class names implementing org.apache.spark.api.plugin.SparkPlugin to load into the application. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1941) |

### `spark.pyspark.driver.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.pyspark.driver.python` | string | _(optional)_ | 2.1.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1144) |

### `spark.pyspark.python.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.pyspark.python` | string | _(optional)_ | 2.1.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1149) |

### `spark.python.authenticate.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.python.authenticate.socketTimeout` | time | `15s` | 3.1.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Python.scala#L59) |

### `spark.python.daemon.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.python.daemon.killWorkerOnFlushFailure` | boolean | `true` | 4.1.0 | When enabled, exceptions raised during output flush operations in the Python worker managed under Python daemon are not caught, causing the worker to terminate with the exception. This allows Spark to detect the failure and launch a new worker and retry the task. When disabled, flush exceptions are caught and logged but the worker continues, which could cause the worker to get stuck due to protocol mismatch. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Python.scala#L143) |
| `spark.python.daemon.module` | string | _(optional)_ | 2.4.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Python.scala#L44) |

### `spark.python.factory.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.python.factory.idleWorkerMaxPoolSize` | int | _(optional)_ | 4.1.0 | Maximum number of idle Python workers to keep. If unset, the number is unbounded. If set to a positive integer N, at most N idle workers are retained; least-recently used workers are evicted first. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Python.scala#L132) |

### `spark.python.task.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.python.task.killTimeout` | time | `2s` | 2.2.2 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Python.scala#L29) |

### `spark.python.unix.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.python.unix.domain.socket.dir` | string | _(optional)_ | 4.1.0 | When specified, it uses the directory to create Unix domain socket files. Otherwise, it uses the default location of the temporary directory set in 'java.io.tmpdir' property. This is used when ${PYTHON_UNIX_DOMAIN_SOCKET_ENABLED.key} is enabled. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Python.scala#L82) |
| `spark.python.unix.domain.socket.enabled` | boolean | `sys.env.get("PYSPARK_UDS_MODE").contains("true")` | 4.1.0 | When set to true, the Python driver uses a Unix domain socket for operations like creating or collecting a DataFrame from local data, using accumulators, and executing Python functions with PySpark such as Python UDFs. This configuration only applies to Spark Classic and Spark Connect server. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Python.scala#L73) |

### `spark.python.use.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.python.use.daemon` | boolean | `true` | 2.3.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Python.scala#L34) |

### `spark.python.worker.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.python.worker.faulthandler.enabled` | boolean | `false` | 3.2.0 | When true, Python workers set up the faulthandler for the case when the Python worker exits unexpectedly (crashes), and shows the stack trace of the moment the Python worker crashes in the error message if captured successfully. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Python.scala#L65) |
| `spark.python.worker.idleTimeoutSeconds` | time | `0` | 4.0.0 | The time (in seconds) Spark will wait for activity (e.g., data transfer or communication) from a Python worker before considering it potentially idle or unresponsive. When the timeout is triggered, Spark will log the network-related status for debugging purposes. However, the Python worker will remain active and continue waiting for communication unless explicitly terminated via $PYTHON_WORKER_KILL_ON_IDLE_TIMEOUT_KEY.The default is `0` that means no timeout. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Python.scala#L100) |
| `spark.python.worker.killOnIdleTimeout` | boolean | `false` | 4.1.0 | Whether Spark should terminate the Python worker process when the idle timeout (as defined by $PYTHON_WORKER_IDLE_TIMEOUT_SECONDS_KEY) is reached. If enabled, Spark will terminate the Python worker process in addition to logging the status. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Python.scala#L113) |
| `spark.python.worker.module` | string | _(optional)_ | 2.4.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Python.scala#L49) |
| `spark.python.worker.reuse` | boolean | `true` | 1.2.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Python.scala#L24) |
| `spark.python.worker.tracebackDumpIntervalSeconds` | time | `0` | 4.1.0 | The interval (in seconds) for Python workers to dump their tracebacks. If it's positive, the Python worker will periodically dump the traceback into its `stderr`. The default is `0` that means it is disabled. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Python.scala#L122) |

### `spark.r.backendConnectionTimeout.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.r.backendConnectionTimeout` | int | `6000` | 2.1.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/R.scala#L21) |

### `spark.r.command.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.r.command` | string | _(optional)_ | 1.5.3 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/R.scala#L41) |

### `spark.r.heartBeatInterval.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.r.heartBeatInterval` | int | `100` | 2.1.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/R.scala#L31) |

### `spark.r.numRBackendThreads.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.r.numRBackendThreads` | int | `2` | 1.4.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/R.scala#L26) |

### `spark.rdd.cache.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.rdd.cache.visibilityTracking.enabled` | boolean | `false` | 3.5.0 | Set to be true to enabled RDD cache block's visibility status. Once it's enabled, a RDD cache block can be used only when it's marked as visible. And a RDD block will be marked as visible only when one of the tasks generating the cache block finished successfully. This is relevant in context of consistent accumulator status. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2806) |

### `spark.rdd.checkpoint.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.rdd.checkpoint.cachePreferredLocsExpireTime` | time | _(optional)_ | 3.0.0 | Expire time in minutes for caching preferred locations of checkpointed RDD.Caching preferred locations can relieve query loading to DFS and save the query time. The drawback is that the cached locations can be possibly outdated and lose data locality. If this config is not specified, it will not cache. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1392) |

### `spark.rdd.compress.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.rdd.compress` | boolean | `false` | 0.6.0 | Whether to compress serialized RDD partitions (e.g. for StorageLevel.MEMORY_ONLY_SER in Scala or StorageLevel.MEMORY_ONLY in Python). Can save substantial space at the cost of some extra CPU time. Compression will use spark.io.compression.codec | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2245) |

### `spark.rdd.limit.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.rdd.limit.initialNumPartitions` | int | `1` | 3.4.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2262) |
| `spark.rdd.limit.scaleUpFactor` | int | `4` | 2.1.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2269) |

### `spark.rdd.parallelListingThreshold.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.rdd.parallelListingThreshold` | int | `10` | 2.0.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2256) |

### `spark.redaction.regex.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.redaction.regex` | unknown | `"(?i)secret|password|token|access[.]?key".r` | 2.1.2 | Regex to decide which Spark configuration properties and environment variables in driver and executor environments contain sensitive information. When this regex matches a property key or value, the value is redacted from the environment UI and various logs like YARN and event logs. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1287) |

### `spark.redaction.string.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.redaction.string.regex` | unknown | _(optional)_ | 2.2.0 | Regex to decide which parts of strings produced by Spark contain sensitive information. When this regex matches a string part, that string part is replaced by a dummy value. This is currently used to redact the output of SQL explain commands. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1297) |

### `spark.reducer.maxBlocksInFlightPerAddress.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.reducer.maxBlocksInFlightPerAddress` | int | `Int.MaxValue` | 2.2.1 | This configuration limits the number of remote blocks being fetched per reduce task from a given host port. When a large number of blocks are being requested from a given address in a single fetch or simultaneously, this could crash the serving executor or Node Manager. This is especially useful to reduce the load on the Node Manager when external shuffle is enabled. You can mitigate the issue by setting it to a lower value. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1460) |

### `spark.reducer.maxReqsInFlight.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.reducer.maxReqsInFlight` | int | `Int.MaxValue` | 2.0.0 | This configuration limits the number of remote requests to fetch blocks at any given point. When the number of hosts in the cluster increase, it might lead to very large number of inbound connections to one or more nodes, causing the workers to fail under load. By allowing it to limit the number of fetch requests, this scenario can be mitigated | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2201) |

### `spark.reducer.maxSizeInFlight.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.reducer.maxSizeInFlight` | bytes | `48m` | 1.4.0 | Maximum size of map outputs to fetch simultaneously from each reduce task, in MiB unless otherwise specified. Since each output requires us to create a buffer to receive it, this represents a fixed memory overhead per reduce task, so keep it small unless you have a large amount of memory | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2192) |

### `spark.resources.discoveryPlugin.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.resources.discoveryPlugin` | string | `Nil` | 3.0.0 | Comma-separated list of class names implementingorg.apache.spark.api.resource.ResourceDiscoveryPlugin to load into the application.This is for advanced users to replace the resource discovery class with a custom implementation. Spark will try each class specified until one of them returns the resource information for that resource. It tries the discovery script last if none of the plugins return information for that resource. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L58) |

### `spark.resources.warnings.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.resources.warnings.testing` | boolean | `false` | 3.1.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Tests.scala#L65) |

### `spark.rpc.askTimeout.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.rpc.askTimeout` | string | _(optional)_ | 1.4.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Network.scala#L55) |

### `spark.rpc.connect.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.rpc.connect.threads` | int | `64` | 1.6.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Network.scala#L61) |

### `spark.rpc.io.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.rpc.io.numConnectionsPerPeer` | int | `1` | 1.6.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Network.scala#L67) |
| `spark.rpc.io.threads` | int | _(optional)_ | 1.6.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Network.scala#L73) |

### `spark.rpc.lookupTimeout.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.rpc.lookupTimeout` | string | _(optional)_ | 1.4.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Network.scala#L79) |

### `spark.rpc.message.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.rpc.message.maxSize` | int | `128` | 2.0.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Network.scala#L85) |

### `spark.rpc.netty.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.rpc.netty.dispatcher.numThreads` | int | _(optional)_ | 1.6.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Network.scala#L91) |

### `spark.scheduler.allocation.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.scheduler.allocation.file` | string | _(optional)_ | 0.8.1 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2333) |

### `spark.scheduler.barrier.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.scheduler.barrier.maxConcurrentTasksCheck.interval` | time | `15s` | 2.4.0 | Time in seconds to wait between a max concurrent tasks check failure and the next check. A max concurrent tasks check ensures the cluster can launch more concurrent tasks than required by a barrier stage on job submitted. The check can fail in case a cluster has just started and not enough executors have registered, so we wait for a little while and try to perform the check again. If the check fails more than a configured max failure times for a job then fail current job submission. Note this config only applies to jobs that contain one or more barrier stages, we won't perform the check on non-barrier jobs. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1876) |
| `spark.scheduler.barrier.maxConcurrentTasksCheck.maxFailures` | int | `40` | 2.4.0 | Number of max concurrent tasks check failures allowed before fail a job submission. A max concurrent tasks check ensures the cluster can launch more concurrent tasks than required by a barrier stage on job submitted. The check can fail in case a cluster has just started and not enough executors have registered, so we wait for a little while and try to perform the check again. If the check fails more than a configured max failure times for a job then fail current job submission. Note this config only applies to jobs that contain one or more barrier stages, we won't perform the check on non-barrier jobs. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1890) |

### `spark.scheduler.dropTaskInfoAccumulablesOnTaskCompletion.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.scheduler.dropTaskInfoAccumulablesOnTaskCompletion.enabled` | boolean | `false` | 4.0.0 | If true, the task info accumulables will be cleared upon task completion in TaskSetManager. This reduces the heap usage of the driver by only referencing the task info accumulables for the active tasks and not for completed tasks. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2860) |

### `spark.scheduler.excludeOnFailure.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.scheduler.excludeOnFailure.unschedulableTaskSetTimeout` | time | `120` | 3.1.0 | The timeout in seconds to wait to acquire a new executor and schedule a task before aborting a TaskSet which is unschedulable because all executors are excluded due to failures. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1866) |

### `spark.scheduler.executorTaskExcludeOnFailureTime.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.scheduler.executorTaskExcludeOnFailureTime` | time | _(optional)_ | 3.1.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1029) |

### `spark.scheduler.listenerbus.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.scheduler.listenerbus.eventqueue.capacity` | int | `10000` | 2.3.0 | The default capacity for event queues. Spark will try to initialize an event queue using capacity specified by `spark.scheduler.listenerbus.eventqueue.queueName.capacity` first. If it's not configured, Spark will use the default capacity specified by this config. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1068) |
| `spark.scheduler.listenerbus.exitTimeout` | time | `0` | 4.0.0 | The time that event queue waits until the dispatch thread exits when stop is invoked. This is set to 0 by default for graceful shutdown of the event queue, but allow the user to configure the waiting time. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1109) |
| `spark.scheduler.listenerbus.logSlowEvent` | boolean | `true` | 3.0.0 | When enabled, log the event that takes too much time to process. This helps us discover the event types that cause performance bottlenecks. The time threshold is controlled by spark.scheduler.listenerbus.logSlowEvent.threshold. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1090) |
| `spark.scheduler.listenerbus.logSlowEvent.threshold` | time | `1s` | 3.0.0 | The time threshold of whether a event is considered to be taking too much time to process. Log the event if ${LISTENER_BUS_LOG_SLOW_EVENT_ENABLED.key} is true. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1100) |
| `spark.scheduler.listenerbus.metrics.maxListenerClassesTimed` | int | `128` | 2.3.0 | The number of listeners that have timers to track the elapsed time ofprocessing events. If 0 is set, disables this feature. If -1 is set,it sets no limit to the number. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1079) |

### `spark.scheduler.maxRegisteredResourcesWaitingTime.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.scheduler.maxRegisteredResourcesWaitingTime` | time | `30s` | 1.1.1 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2345) |

### `spark.scheduler.maxRetainedRemovedDecommissionExecutors.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.scheduler.maxRetainedRemovedDecommissionExecutors` | int | `0` | 3.4.0 | Max number of removed executors by decommission to retain. This affects whether fetch failure caused by removed decommissioned executors could be ignored when ${STAGE_IGNORE_DECOMMISSION_FETCH_FAILURE.key} is enabled. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2558) |

### `spark.scheduler.maxRetainedUnknownDecommissionExecutors.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.scheduler.maxRetainedUnknownDecommissionExecutors` | int | `0` | 3.5.0 | Max number of unknown executors by decommission to retain. This affects whether executor could receive decommission request sent before its registration. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2569) |

### `spark.scheduler.minRegisteredResourcesRatio.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.scheduler.minRegisteredResourcesRatio` | double | _(optional)_ | 1.1.1 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2339) |

### `spark.scheduler.mode.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.scheduler.mode` | enum | `SchedulingMode.FIFO` | 0.8.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2351) |

### `spark.scheduler.numCancelledJobGroupsToTrack.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.scheduler.numCancelledJobGroupsToTrack` | int | `1000` | 4.0.0 | The maximum number of tracked job groups that are cancelled with `cancelJobGroupAndFutureJobs`. If this maximum number is hit, the oldest job group will no longer be tracked that future jobs belonging to this job group will not be cancelled. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1905) |

### `spark.scheduler.resource.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.scheduler.resource.profileMergeConflicts` | boolean | `false` | 3.1.0 | If set to true, Spark will merge ResourceProfiles when different profiles are specified in RDDs that get combined into a single stage. When they are merged, Spark chooses the maximum of each resource and creates a new ResourceProfile. The default of false results in Spark throwing an exception if multiple different ResourceProfiles are found in RDDs going into the same stage. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2497) |

### `spark.scheduler.revive.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.scheduler.revive.interval` | time | _(optional)_ | 0.8.1 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2357) |

### `spark.scheduler.stage.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.scheduler.stage.legacyAbortAfterKillTasks` | boolean | `true` | 4.0.0 | Whether to abort a stage after TaskScheduler.killAllTaskAttempts(). This is used to restore the original behavior in case there are any regressions after abort stage is removed | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2850) |

### `spark.security.credentials.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.security.credentials.renewalRatio` | double | `0.75` | 2.4.0 | Ratio of the credential's expiration time when Spark should fetch new credentials. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1641) |
| `spark.security.credentials.retryWait` | time | `1h` | 2.4.0 | How long to wait before retrying to fetch new credentials after a failure. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1648) |

### `spark.serializer.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.serializer` | string | `org.apache.spark.serializer.JavaSerializer` | 0.5.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2274) |

### `spark.serializer.extraDebugInfo.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.serializer.extraDebugInfo` | boolean | `true` | 1.3.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2285) |

### `spark.serializer.objectStreamReset.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.serializer.objectStreamReset` | int | `100` | 1.0.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2280) |

### `spark.shuffle.accurateBlockSkewedFactor.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.shuffle.accurateBlockSkewedFactor` | double | `-1.0` | 3.3.0 | A shuffle block is considered as skewed and will be accurately recorded in HighlyCompressedMapStatus if its size is larger than this factor multiplying the median shuffle block size or SHUFFLE_ACCURATE_BLOCK_THRESHOLD. It is recommended to set this parameter to be the same as SKEW_JOIN_SKEWED_PARTITION_FACTOR.Set to -1.0 to disable this feature by default. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1413) |

### `spark.shuffle.accurateBlockThreshold.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.shuffle.accurateBlockThreshold` | bytes | `100 * 1024 * 1024` | 2.2.1 | Threshold in bytes above which the size of shuffle blocks in HighlyCompressedMapStatus is accurately recorded. This helps to prevent OOM by avoiding underestimating shuffle block size when fetch shuffle blocks. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1404) |

### `spark.shuffle.checksum.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.shuffle.checksum.algorithm` | string | `ADLER32` | 3.2.0 | The algorithm is used to calculate the shuffle checksum. Currently, it only supports built-in algorithms of JDK. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1675) |
| `spark.shuffle.checksum.enabled` | boolean | `true` | 3.2.0 | Whether to calculate the checksum of shuffle data. If enabled, Spark will calculate the checksum values for each partition data within the map output file and store the values in a checksum file on the disk. When there's shuffle data corruption detected, Spark will try to diagnose the cause (e.g., network issue, disk issue, etc.) of the corruption by using the checksum file. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1664) |

### `spark.shuffle.compress.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.shuffle.compress` | boolean | `true` | 0.6.0 | Whether to compress shuffle output. Compression will use spark.io.compression.codec. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1685) |

### `spark.shuffle.detectCorrupt.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.shuffle.detectCorrupt` | boolean | `true` | 2.2.0 | Whether to detect any corruption in fetched blocks. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1770) |
| `spark.shuffle.detectCorrupt.useExtraMemory` | boolean | `false` | 3.0.0 | If enabled, part of a compressed/encrypted stream will be de-compressed/de-crypted by using extra memory to detect early corruption. Any IOException thrown will cause the task to be retried once and if it fails again with same exception, then FetchFailedException will be thrown to retry previous stage | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1777) |

### `spark.shuffle.file.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.shuffle.file.buffer` | bytes | `32k` | 1.4.0 | Size of the in-memory buffer for each shuffle file output stream, in KiB unless otherwise specified. These buffers reduce the number of disk seeks and system calls made in creating intermediate shuffle files. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1506) |
| `spark.shuffle.file.merge.buffer` | fallback | → `SHUFFLE_FILE_BUFFER_SIZE` | 4.0.0 | Size of the in-memory buffer for each shuffle file input stream, in KiB unless otherwise specified. These buffers use off-heap buffers and are related to the number of files in the shuffle file. Too large buffers should be avoided. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1518) |

### `spark.shuffle.localDisk.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.shuffle.localDisk.file.output.buffer` | fallback | → `SHUFFLE_UNSAFE_FILE_OUTPUT_BUFFER_SIZE` | 4.0.0 | The file system for this buffer size after each partition is written in all local disk shuffle writers. In KiB unless otherwise specified. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1536) |

### `spark.shuffle.manager.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.shuffle.manager` | string | `sort` | 1.1.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1744) |

### `spark.shuffle.mapOutput.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.shuffle.mapOutput.dispatcher.numThreads` | int | `8` | 2.0.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1764) |
| `spark.shuffle.mapOutput.minSizeForBroadcast` | bytes | `512k` | 2.0.0 | The size at which we use Broadcast to send the map output statuses to the executors. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1757) |
| `spark.shuffle.mapOutput.parallelAggregationThreshold` | int | `10000000` | 2.3.0 | Multi-thread is used when the number of mappers * shuffle partitions is greater than or equal to this threshold. Note that the actual parallelism is calculated by number of mappers * shuffle partitions / this threshold + 1, so this threshold should be positive. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1624) |

### `spark.shuffle.mapStatus.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.shuffle.mapStatus.compression.codec` | string | `CompressionCodec.ZSTD` | 3.0.0 | The codec used to compress MapStatus, which is generated by ShuffleMapTask. By default, Spark provides four codecs: lz4, lzf, snappy, and zstd. You can also use fully qualified class names to specify the codec. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1701) |

### `spark.shuffle.maxAccurateSkewedBlockNumber.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.shuffle.maxAccurateSkewedBlockNumber` | int | `100` | 3.3.0 | Max skewed shuffle blocks allowed to be accurately recorded in HighlyCompressedMapStatus if its size is larger than SHUFFLE_ACCURATE_BLOCK_SKEWED_FACTOR multiplying the median shuffle block size or SHUFFLE_ACCURATE_BLOCK_THRESHOLD. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1424) |

### `spark.shuffle.maxAttemptsOnNettyOOM.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.shuffle.maxAttemptsOnNettyOOM` | int | `10` | 3.2.0 | The max attempts of a shuffle block would retry on Netty OOM issue before throwing the shuffle fetch failure. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1451) |

### `spark.shuffle.minNumPartitionsToHighlyCompress.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.shuffle.minNumPartitionsToHighlyCompress` | int | `2000` | 2.4.0 | Number of partitions to determine if MapStatus should use HighlyCompressedMapStatus | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1809) |

### `spark.shuffle.push.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.shuffle.push.enabled` | boolean | `false` | 3.2.0 | Set to true to enable push-based shuffle on the client side and this works in conjunction with the server side flag spark.shuffle.push.server.mergedShuffleFileManagerImpl which needs to be set with the appropriate org.apache.spark.network.shuffle.MergedShuffleFileManager implementation for push-based shuffle to be enabled | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2579) |
| `spark.shuffle.push.finalize.timeout` | time | `10s` | 3.2.0 | The amount of time driver waits, after all mappers have finished for a given shuffle map stage, before it sends merge finalize requests to remote external shuffle services. This gives the external shuffle services extra time to merge blocks. Setting this too long could potentially lead to performance regression | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2602) |
| `spark.shuffle.push.maxBlockBatchSize` | bytes | `3m` | 3.2.0 | The max size of a batch of shuffle blocks to be grouped into a single push request. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2666) |
| `spark.shuffle.push.maxBlockSizeToPush` | bytes | `1m` | 3.2.0 | The max size of an individual block to push to the remote external shuffle services. Blocks larger than this threshold are not pushed to be merged remotely. These shuffle blocks will be fetched by the executors in the original manner. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2657) |
| `spark.shuffle.push.maxRetainedMergerLocations` | int | `500` | 3.2.0 | Maximum number of merger locations cached for push-based shuffle. Currently, merger locations are hosts of external shuffle services responsible for handling pushed blocks, merging them and serving merged blocks for later shuffle fetch. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2613) |
| `spark.shuffle.push.merge.finalizeThreads` | int | `8` | 3.3.0 | Number of threads used by driver to finalize shuffle merge. Since it could potentially take seconds for a large shuffle to finalize, having multiple threads helps driver to handle concurrent shuffle merge finalize requests when push-based shuffle is enabled. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2677) |
| `spark.shuffle.push.mergersMinStaticThreshold` | int | `5` | 3.2.0 | The static threshold for number of shuffle push merger locations should be available in order to enable push-based shuffle for a stage. Note this config works in conjunction with ${SHUFFLE_MERGER_LOCATIONS_MIN_THRESHOLD_RATIO.key}. Maximum of spark.shuffle.push.mergersMinStaticThreshold and ${SHUFFLE_MERGER_LOCATIONS_MIN_THRESHOLD_RATIO.key} ratio number of mergers needed to enable push-based shuffle for a stage. For eg: with 1000 partitions for the child stage with spark.shuffle.push.mergersMinStaticThreshold as 5 and ${SHUFFLE_MERGER_LOCATIONS_MIN_THRESHOLD_RATIO.key} set to 0.05, we would need at least 50 mergers to enable push-based shuffle for that stage. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2633) |
| `spark.shuffle.push.mergersMinThresholdRatio` | double | `0.05` | 3.2.0 | Ratio used to compute the minimum number of shuffle merger locations required for a stage based on the number of partitions for the reducer stage. For example, a reduce stage which has 100 partitions and uses the default value 0.05 requires at least 5 unique merger locations to enable push-based shuffle. Merger locations are currently defined as external shuffle services. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2622) |
| `spark.shuffle.push.minCompletedPushRatio` | double | `1.0` | 3.3.0 | Fraction of map partitions that should be push complete before driver starts shuffle merge finalization during push based shuffle | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2707) |
| `spark.shuffle.push.minShuffleSizeToWait` | bytes | `500m` | 3.3.0 | Driver will wait for merge finalization to complete only if total shuffle size is more than this threshold. If total shuffle size is less, driver will immediately finalize the shuffle output | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2698) |
| `spark.shuffle.push.numPushThreads` | int | _(optional)_ | 3.2.0 | Specify the number of threads in the block pusher pool. These threads assist in creating connections and pushing blocks to remote external shuffle services. By default, the threadpool size is equal to the number of spark executor cores. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2648) |
| `spark.shuffle.push.results.timeout` | time | `10s` | 3.2.0 | The maximum amount of time driver waits in seconds for the merge results to be received from all remote external shuffle services for a given shuffle. Driver submits following stages if not all results are received within the timeout. Setting this too long could potentially lead to performance regression | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2590) |
| `spark.shuffle.push.sendFinalizeRPCThreads` | int | `8` | 3.4.0 | Number of threads used by the driver to send finalize shuffle RPC to mergers location and then get MergeStatus. The thread will run for up to PUSH_BASED_SHUFFLE_MERGE_RESULTS_TIMEOUT. The merger ESS may open too many files if the finalize rpc is not received. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2687) |

### `spark.shuffle.readHostLocalDisk.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.shuffle.readHostLocalDisk` | boolean | `true` | 3.0.0 | If enabled (and `${SHUFFLE_USE_OLD_FETCH_PROTOCOL.key}` is disabled, shuffle blocks requested from those block managers which are running on the same host are read from the disk directly instead of being fetched as remote blocks over the network. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1827) |

### `spark.shuffle.reduceLocality.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.shuffle.reduceLocality.enabled` | boolean | `true` | 1.5.0 | Whether to compute locality preferences for reduce tasks | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1750) |

### `spark.shuffle.registration.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.shuffle.registration.maxAttempts` | int | `3` | 2.3.0 | When we fail to register to the external shuffle service, we will retry for maxAttempts times. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1443) |
| `spark.shuffle.registration.timeout` | time | `5000` | 2.3.0 | Timeout in milliseconds for registration to the external shuffle service. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1436) |

### `spark.shuffle.service.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.shuffle.service.db.backend` | enum | `DBBackend.ROCKSDB` | 3.4.0 | Specifies a disk-based store used in shuffle service local db. ROCKSDB or LEVELDB (deprecated). | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L837) |
| `spark.shuffle.service.db.enabled` | boolean | `true` | 3.0.0 | Whether to use db in ExternalShuffleService. Note that this only affects standalone mode. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L829) |
| `spark.shuffle.service.enabled` | boolean | `false` | 1.2.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L803) |
| `spark.shuffle.service.fetch.rdd.enabled` | boolean | `false` | 3.0.0 | Whether to use the ExternalShuffleService for fetching disk persisted RDD blocks. In case of dynamic allocation if this feature is enabled executors having only disk persisted blocks are considered idle after 'spark.dynamicAllocation.executorIdleTimeout' and will be released accordingly. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L819) |
| `spark.shuffle.service.name` | string | `spark_shuffle` | 3.2.0 | The configured name of the Spark shuffle service the client should communicate with. This must match the name used to configure the Shuffle within the YARN NodeManager configuration (`yarn.nodemanager.aux-services`). Only takes effect when $SHUFFLE_SERVICE_ENABLED is set to true. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L848) |
| `spark.shuffle.service.port` | int | `7337` | 1.2.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L845) |
| `spark.shuffle.service.removeShuffle` | boolean | `true` | 3.3.0 | Whether to use the ExternalShuffleService for deleting shuffle blocks for deallocated executors when the shuffle is no longer needed. Without this enabled, shuffle data on executors that are deallocated will remain on disk until the application ends. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L809) |

### `spark.shuffle.sort.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.shuffle.sort.bypassMergeThreshold` | int | `200` | 1.1.1 | In the sort-based shuffle manager, avoid merge-sorting data if there is no map-side aggregation and there are at most this many reduce partitions | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1736) |
| `spark.shuffle.sort.initialBufferSize` | bytes | `4096` | 2.1.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1655) |
| `spark.shuffle.sort.io.plugin.class` | string | `classOf[LocalDiskShuffleDataIO].getName` | 3.0.0 | Name of the class to use for shuffle IO. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1499) |
| `spark.shuffle.sort.useRadixSort` | boolean | `true` | 2.0.0 | Whether to use radix sort for sorting in-memory partition ids. Radix sort is much faster, but requires additional memory to be reserved memory as pointers are added. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1801) |

### `spark.shuffle.spill.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.shuffle.spill.batchSize` | long | `10000` | 0.9.0 | Size of object batches when reading/writing from serializers. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1720) |
| `spark.shuffle.spill.compress` | boolean | `true` | 0.9.0 | Whether to compress data spilled during shuffles. Compression will use spark.io.compression.codec. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1693) |
| `spark.shuffle.spill.diskWriteBufferSize` | bytes | `1024 * 1024` | 2.3.0 | The buffer size, in bytes, to use when writing the sorted records to an on-disk file. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1543) |
| `spark.shuffle.spill.initialMemoryThreshold` | bytes | `5 * 1024 * 1024` | 1.1.1 | Initial threshold for the size of a collection before we start tracking its memory usage. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1711) |
| `spark.shuffle.spill.maxSizeInBytesForSpillThreshold` | bytes | `Long.MaxValue` | 4.1.0 | The maximum in memory size in bytes before forcing the shuffle sorter to spill. By default it is Long.MAX_VALUE, which means we never force the sorter to spill, until we reach some limitations, like the max page size limitation for the pointer array in the sorter. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1612) |
| `spark.shuffle.spill.numElementsForceSpillThreshold` | int | `Integer.MAX_VALUE` | 1.6.0 | The maximum number of elements in memory before forcing the shuffle sorter to spill. By default it's Integer.MAX_VALUE, which means we never force the sorter to spill, until we reach some limitations, like the max page size limitation for the pointer array in the sorter. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1601) |

### `spark.shuffle.sync.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.shuffle.sync` | boolean | `false` | 0.8.0 | Whether to force outstanding writes to disk. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1787) |

### `spark.shuffle.unsafe.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.shuffle.unsafe.fastMergeEnabled` | boolean | `true` | 1.4.0 | Whether to perform a fast spill merge. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1794) |
| `spark.shuffle.unsafe.file.output.buffer` | bytes | `32k` | 2.3.0 | (Deprecated since Spark 4.0, please use 'spark.shuffle.localDisk.file.output.buffer'.) | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1526) |

### `spark.shuffle.useOldFetchProtocol.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.shuffle.useOldFetchProtocol` | boolean | `false` | 3.0.0 | Whether to use the old protocol while doing the shuffle block fetching. It is only enabled while we need the compatibility in the scenario of new Spark version job fetching shuffle blocks from old version external shuffle service. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1818) |

### `spark.shutdown.timeout.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.shutdown.timeout` | time | _(optional)_ | 4.0.0 | Defines the timeout period to wait for all shutdown hooks to be executed. This must be passed as a system property argument in the Java options, for example spark.driver.extraJavaOptions="-Dspark.shutdown.timeout=60s". | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2870) |

### `spark.sparkr.r.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sparkr.r.command` | string | `Rscript` | 1.5.3 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/R.scala#L36) |

### `spark.speculation.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.speculation` | boolean | `false` | 0.6.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2363) |

### `spark.speculation.efficiency.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.speculation.efficiency.enabled` | boolean | `true` | 3.4.0 | When set to true, spark will evaluate the efficiency of task processing through the stage task metrics or its duration, and only need to speculate the inefficient tasks. A task is inefficient when 1)its data process rate is less than the average data process rate of all successful tasks in the stage multiplied by a multiplier or 2)its duration has exceeded the value of multiplying ${SPECULATION_EFFICIENCY_TASK_DURATION_FACTOR.key} and the time threshold (either be ${SPECULATION_MULTIPLIER.key} * successfulTaskDurations.median or ${SPECULATION_MIN_THRESHOLD.key}). | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2430) |
| `spark.speculation.efficiency.longRunTaskFactor` | double | `2.0` | 3.4.0 | A task will be speculated anyway as long as its duration has exceeded the value of multiplying the factor and the time threshold (either be ${SPECULATION_MULTIPLIER.key} * successfulTaskDurations.median or ${SPECULATION_MIN_THRESHOLD.key}) regardless of it's data process rate is good or not. This avoids missing the inefficient tasks when task slow isn't related to data process rate. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2418) |
| `spark.speculation.efficiency.processRateMultiplier` | double | `0.75` | 3.4.0 | A multiplier that used when evaluating inefficient tasks. The higher the multiplier is, the more tasks will be possibly considered as inefficient. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2409) |

### `spark.speculation.interval.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.speculation.interval` | time | `100` | 0.6.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2369) |

### `spark.speculation.minTaskRuntime.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.speculation.minTaskRuntime` | time | `100` | 3.2.0 | Minimum amount of time a task runs before being considered for speculation. This can be used to avoid launching speculative copies of tasks that are very short. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2387) |

### `spark.speculation.multiplier.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.speculation.multiplier` | double | `3` | 0.6.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2375) |

### `spark.speculation.quantile.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.speculation.quantile` | double | `0.9` | 0.6.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2381) |

### `spark.speculation.task.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.speculation.task.duration.threshold` | time | _(optional)_ | 3.0.0 | Task duration after which scheduler would try to speculative run the task. If provided, tasks would be speculatively run if current stage contains less tasks than or equal to the number of slots on a single executor and the task is taking longer time than the threshold. This config helps speculate stage with very few tasks. Regular speculation configs may also apply if the executor slots are large enough. E.g. tasks might be re-launched if there are enough successful runs even though the threshold hasn't been reached. The number of slots is computed based on the conf values of spark.executor.cores and spark.task.cpus minimum 1. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2395) |

### `spark.stage.ignoreDecommissionFetchFailure.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.stage.ignoreDecommissionFetchFailure` | boolean | `true` | 3.4.0 | Whether ignore stage fetch failure caused by executor decommission when count ${STAGE_MAX_CONSECUTIVE_ATTEMPTS.key} | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2550) |

### `spark.stage.maxAttempts.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.stage.maxAttempts` | int | `Int.MaxValue` | 3.5.0 | Specify the max attempts for a stage - the spark job will be aborted if any of its stages is resubmitted multiple times beyond the max retries limitation. The maximum number of stage retries is the maximum of `spark.stage.maxAttempts` and `${STAGE_MAX_CONSECUTIVE_ATTEMPTS.key}`. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2817) |

### `spark.stage.maxConsecutiveAttempts.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.stage.maxConsecutiveAttempts` | int | `4` | 2.2.0 | Number of consecutive stage attempts allowed before a stage is aborted. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2543) |

### `spark.standalone.executorRemoveDelayOnDisconnection.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.standalone.executorRemoveDelayOnDisconnection` | time | `5s` | 3.4.0 | The timeout duration for a disconnected executor to wait for the specific disconnectreason before it gets removed. This is only used for Standalone yet. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2790) |

### `spark.standalone.submit.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.standalone.submit.waitAppCompletion` | boolean | `false` | 3.1.0 | In standalone cluster mode, controls whether the client waits to exit until the application completes. If set to true, the client process will stay alive polling the driver's status. Otherwise, the client process will exit after submission. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2508) |

### `spark.storage.blockManagerHeartbeatTimeoutMs.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.storage.blockManagerHeartbeatTimeoutMs` | time | _(optional)_ | 0.7.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L674) |

### `spark.storage.blockManagerMasterDriverHeartbeatTimeoutMs.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.storage.blockManagerMasterDriverHeartbeatTimeoutMs` | time | `10m` | 3.2.0 | A timeout used for block manager master's driver heartbeat endpoint. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L667) |

### `spark.storage.blockManagerTimeoutIntervalMs.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.storage.blockManagerTimeoutIntervalMs` | time | `60s` | 0.7.3 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L661) |

### `spark.storage.cachedPeersTtl.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.storage.cachedPeersTtl` | int | `60 * 1000` | 1.1.1 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L554) |

### `spark.storage.cleanupFilesAfterExecutorExit.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.storage.cleanupFilesAfterExecutorExit` | boolean | `true` | 2.4.0 | Whether or not cleanup the files not served by the external shuffle service on executor exits. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L681) |

### `spark.storage.decommission.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.storage.decommission.enabled` | boolean | `false` | 3.1.0 | Whether to decommission the block manager when decommissioning executor | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L566) |
| `spark.storage.decommission.fallbackStorage.cleanUp` | boolean | `false` | 3.2.0 | If true, Spark cleans up its fallback storage data once individual shuffles are freed (interval configured via spark.cleaner.periodicGC.interval), and during shutting down. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L617) |
| `spark.storage.decommission.fallbackStorage.path` | string | _(optional)_ | 3.1.0 | The location for fallback storage during block manager decommissioning. For example, `s3a://spark-storage/`. In case of empty, fallback storage is disabled. The storage will not be cleaned up by Spark unless ${STORAGE_DECOMMISSION_FALLBACK_STORAGE_CLEANUP.key} is true. Use an external clean up mechanism when false, for instance a TTL. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L626) |
| `spark.storage.decommission.maxReplicationFailuresPerBlock` | int | `3` | 3.1.0 | Maximum number of failures which can be handled for the replication of one RDD block when block manager is decommissioning and trying to move its existing blocks. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L596) |
| `spark.storage.decommission.rddBlocks.enabled` | boolean | `true` | 3.1.0 | Whether to transfer RDD blocks during block manager decommissioning. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L589) |
| `spark.storage.decommission.replicationReattemptInterval` | time | `30s` | 3.1.0 | The interval of time between consecutive cache block replication reattempts happening on each decommissioning executor (due to storage decommissioning). | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L606) |
| `spark.storage.decommission.shuffleBlocks.enabled` | boolean | `true` | 3.1.0 | Whether to transfer shuffle blocks during block manager decommissioning. Requires a migratable shuffle resolver (like sort based shuffle) | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L573) |
| `spark.storage.decommission.shuffleBlocks.maxDiskSize` | bytes | _(optional)_ | 3.2.0 | Maximum disk space to use to store shuffle blocks before rejecting remote shuffle blocks. Rejecting remote shuffle blocks means that an executor will not receive any shuffle migrations, and if there are no other executors available for migration then shuffle blocks will be lost unless ${STORAGE_DECOMMISSION_FALLBACK_STORAGE_PATH.key} is configured. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L638) |
| `spark.storage.decommission.shuffleBlocks.maxThreads` | int | `8` | 3.1.0 | Maximum number of threads to use in migrating shuffle files. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L581) |

### `spark.storage.exceptionOnPinLeak.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.storage.exceptionOnPinLeak` | boolean | `false` | 1.6.2 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L655) |

### `spark.storage.localDiskByExecutors.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.storage.localDiskByExecutors.cacheSize` | int | `1000` | 3.0.0 | The max number of executors for which the local dirs are stored. This size is both applied for the driver and both for the executors side to avoid having an unbounded store. This cache will be used to avoid the network in case of fetching disk persisted RDD blocks or shuffle blocks (when `${SHUFFLE_HOST_LOCAL_DISK_READING_ENABLED.key}` is set) from the same host. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1836) |

### `spark.storage.maxReplicationFailures.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.storage.maxReplicationFailures` | int | `1` | 1.1.1 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L560) |

### `spark.storage.memoryMapLimitForTests.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.storage.memoryMapLimitForTests` | bytes | `ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH` | 2.3.0 | For testing only, controls the size of chunks when memory mapping a file | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1847) |

### `spark.storage.memoryMapThreshold.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.storage.memoryMapThreshold` | bytes | `2m` | 0.9.2 | Size in bytes of a block above which Spark memory maps when reading a block from disk. This prevents Spark from memory mapping very small blocks. In general, memory mapping has high overhead for blocks close to or below the page size of the operating system. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L532) |

### `spark.storage.replication.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.storage.replication.policy` | string | `classOf[RandomBlockReplicationPolicy].getName` | 2.1.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L543) |
| `spark.storage.replication.proactive` | boolean | `true` | 2.2.0 | Enables proactive block replication for RDD blocks. Cached RDD block replicas lost due to executor failures are replenished if there are any existing available replicas. This tries to get the replication level of the block to the initial number | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L522) |
| `spark.storage.replication.topologyFile` | string | _(optional)_ | 2.1.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L649) |
| `spark.storage.replication.topologyMapper` | string | `classOf[DefaultTopologyMapper].getName` | 2.1.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L549) |

### `spark.storage.unrollMemoryCheckPeriod.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.storage.unrollMemoryCheckPeriod` | long | `16` | 2.3.0 | The memory check period is used to determine how often we should check whether there is a need to request more memory when we try to unroll the given block in memory. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1553) |

### `spark.storage.unrollMemoryGrowthFactor.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.storage.unrollMemoryGrowthFactor` | double | `1.5` | 2.3.0 | Memory to request as a multiple of the size that used to unroll the block. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1562) |

### `spark.storage.unrollMemoryThreshold.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.storage.unrollMemoryThreshold` | long | `1024 * 1024` | 1.1.0 | Initial memory to request before unrolling any block | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L515) |

### `spark.streaming.dynamicAllocation.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.streaming.dynamicAllocation.enabled` | boolean | `false` | 3.0.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Streaming.scala#L25) |
| `spark.streaming.dynamicAllocation.maxExecutors` | int | `Int.MaxValue` | 3.0.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Streaming.scala#L45) |
| `spark.streaming.dynamicAllocation.minExecutors` | int | _(optional)_ | 3.0.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Streaming.scala#L37) |
| `spark.streaming.dynamicAllocation.scalingDownRatio` | double | `0.3` | 3.0.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Streaming.scala#L69) |
| `spark.streaming.dynamicAllocation.scalingInterval` | time | `60` | 3.0.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Streaming.scala#L53) |
| `spark.streaming.dynamicAllocation.scalingUpRatio` | double | `0.9` | 3.0.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Streaming.scala#L61) |
| `spark.streaming.dynamicAllocation.testing` | boolean | `false` | 3.0.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Streaming.scala#L31) |

### `spark.submit.callSystemExitOnMainExit.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.submit.callSystemExitOnMainExit` | boolean | `false` | 4.1.0 | If true, SparkSubmit will call System.exit() to initiate JVM shutdown once the user's main method has exited. This can be useful in cases where non-daemon JVM threads might otherwise prevent the JVM from shutting down on its own. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2324) |

### `spark.submit.deployMode.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.submit.deployMode` | string | `client` | 1.5.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2312) |

### `spark.submit.proxyUser.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.submit.proxyUser.allowCustomClasspathInClusterMode` | boolean | `false` | 3.4.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2799) |

### `spark.submit.pyFiles.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.submit.pyFiles` | string | `Nil` | 1.0.1 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2317) |

### `spark.task.cpus.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.task.cpus` | int | `1` | 0.5.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L713) |

### `spark.task.maxDirectResultSize.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.task.maxDirectResultSize` | bytes | `1L << 20` | 2.0.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L918) |

### `spark.task.maxFailures.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.task.maxFailures` | int | `4` | 0.8.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L926) |

### `spark.task.reaper.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.task.reaper.enabled` | boolean | `false` | 2.0.3 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L932) |
| `spark.task.reaper.killTimeout` | time | `-1` | 2.0.3 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L938) |
| `spark.task.reaper.pollingInterval` | time | `10s` | 2.0.3 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L944) |
| `spark.task.reaper.threadDump` | boolean | `true` | 2.0.3 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L950) |

### `spark.taskMetrics.trackUpdatedBlockStatuses.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.taskMetrics.trackUpdatedBlockStatuses` | boolean | `false` | 2.3.0 | Enable tracking of updatedBlockStatuses in the TaskMetrics. Off by default since tracking the block statuses can use a lot of memory and its not used anywhere within spark. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1490) |

### `spark.test.noStageRetry.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.test.noStageRetry` | boolean | `false` | 1.2.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Tests.scala#L40) |

### `spark.testing.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.testing` | boolean | _(optional)_ | 1.0.1 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Tests.scala#L35) |

### `spark.testing.dynamicAllocation.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.testing.dynamicAllocation.schedule.enabled` | boolean | `true` | 3.1.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Tests.scala#L30) |

### `spark.testing.memory.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.testing.memory` | long | `Runtime.getRuntime.maxMemory` | 1.6.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Tests.scala#L24) |

### `spark.testing.nCoresPerExecutor.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.testing.nCoresPerExecutor` | int | `2` | 3.0.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Tests.scala#L60) |

### `spark.testing.nExecutorsPerHost.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.testing.nExecutorsPerHost` | int | `4` | 3.0.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Tests.scala#L55) |

### `spark.testing.nHosts.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.testing.nHosts` | int | `5` | 3.0.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Tests.scala#L50) |

### `spark.testing.reservedMemory.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.testing.reservedMemory` | long | _(optional)_ | 1.6.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Tests.scala#L45) |

### `spark.testing.resourceProfileManager.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.testing.resourceProfileManager` | boolean | `false` | 3.1.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Tests.scala#L71) |

### `spark.testing.skipESSRegister.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.testing.skipESSRegister` | boolean | `false` | 4.0.0 | None of Spark testing modes (local, local-cluster) enables shuffle service. So it is hard to test ${SHUFFLE_SERVICE_ENABLED.key} when you only want to test this flag but without the real server. This config provides a way to allow tests run with ${SHUFFLE_SERVICE_ENABLED.key} enabled without registration failures. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Tests.scala#L86) |

### `spark.testing.skipValidateCores.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.testing.skipValidateCores` | boolean | `false` | 3.1.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Tests.scala#L81) |

### `spark.ui.allowFramingFrom.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.ui.allowFramingFrom` | string | _(optional)_ | 1.6.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/UI.scala#L58) |

### `spark.ui.consoleProgress.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.ui.consoleProgress.update.interval` | time | `200` | 2.1.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/UI.scala#L34) |

### `spark.ui.custom.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.ui.custom.executor.log.url` | string | _(optional)_ | 3.0.0 | Specifies custom spark executor log url for supporting external log service instead of using cluster managers' application log urls in the Spark UI. Spark will support some path variables via patterns which can vary on cluster manager. Please check the documentation for your cluster manager to see which patterns are supported, if any. This configuration replaces original log urls in event log, which will be also effective when accessing the application on history server. The new log urls must be permanent, otherwise you might have dead link for executor log urls. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/UI.scala#L223) |

### `spark.ui.dagGraph.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.ui.dagGraph.retainedRootRDDs` | int | `Int.MaxValue` | 2.1.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Status.scala#L61) |

### `spark.ui.enabled.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.ui.enabled` | boolean | `true` | 1.1.1 | Whether to run the web UI for the Spark application. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/UI.scala#L39) |

### `spark.ui.filters.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.ui.filters` | string | `Nil` | 1.0.0 | Comma separated list of filter class names to apply to the Spark Web UI. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/UI.scala#L51) |

### `spark.ui.groupSQLSubExecutionEnabled.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.ui.groupSQLSubExecutionEnabled` | boolean | `true` | 3.4.0 | Whether to group sub executions together in SQL UI when they belong to the same root execution | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/UI.scala#L261) |

### `spark.ui.heapHistogramEnabled.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.ui.heapHistogramEnabled` | boolean | `true` | 3.5.0 | Whether to show a link for executor heap histogram in Executor page. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/UI.scala#L107) |

### `spark.ui.jettyStopTimeout.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.ui.jettyStopTimeout` | time | `30s` | 4.0.0 | Timeout for Jetty servers started in UIs, such as SparkUI, HistoryUI, etc, to stop. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/UI.scala#L268) |

### `spark.ui.killEnabled.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.ui.killEnabled` | boolean | `true` | 1.0.0 | Allows jobs and stages to be killed from the web UI. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/UI.scala#L89) |

### `spark.ui.liveUpdate.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.ui.liveUpdate.minFlushPeriod` | time | `1s` | 2.4.2 | Minimum time elapsed before stale UI data is flushed. This avoids UI staleness when incoming task events are not fired frequently. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Status.scala#L34) |
| `spark.ui.liveUpdate.period` | time | `100ms` | 2.3.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Status.scala#L29) |

### `spark.ui.port.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.ui.port` | int | `4040` | 0.7.0 | Port for your application's dashboard, which shows memory and workload data. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/UI.scala#L45) |

### `spark.ui.prometheus.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.ui.prometheus.enabled` | boolean | `true` | 3.0.0 | Expose executor metrics at /metrics/executors/prometheus. For master/worker/driver metrics, you need to configure `conf/metrics.properties`. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/UI.scala#L113) |

### `spark.ui.proxyRedirectUri.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.ui.proxyRedirectUri` | string | _(optional)_ | 3.0.0 | Proxy address to use when responding with HTTP redirects. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/UI.scala#L217) |

### `spark.ui.requestHeaderSize.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.ui.requestHeaderSize` | bytes | `8k` | 2.2.3 | Value for HTTP request header size in bytes. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/UI.scala#L139) |

### `spark.ui.retainedDeadExecutors.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.ui.retainedDeadExecutors` | int | `100` | 2.0.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Status.scala#L56) |

### `spark.ui.retainedJobs.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.ui.retainedJobs` | int | `1000` | 1.2.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Status.scala#L41) |

### `spark.ui.retainedStages.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.ui.retainedStages` | int | `1000` | 0.9.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Status.scala#L46) |

### `spark.ui.retainedTasks.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.ui.retainedTasks` | int | `100000` | 2.0.1 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Status.scala#L51) |

### `spark.ui.reverseProxy.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.ui.reverseProxy` | boolean | `false` | 2.1.0 | Enable running Spark Master as reverse proxy for worker and application UIs. In this mode, Spark master will reverse proxy the worker and application UIs to enable access without requiring direct access to their hosts. Use it with caution, as worker and application UI will not be accessible directly, you will only be able to access themthrough spark master/proxy public URL. This setting affects all the workers and application UIs running in the cluster and must be set on all the workers, drivers and masters. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/UI.scala#L63) |

### `spark.ui.reverseProxyUrl.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.ui.reverseProxyUrl` | string | _(optional)_ | 2.1.0 | This is the URL where your proxy is running. This URL is for proxy which is running in front of Spark Master. This is useful when running proxy for authentication e.g. OAuth proxy. Make sure this is a complete URL including scheme (http/https) and port to reach your proxy. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/UI.scala#L75) |

### `spark.ui.showConsoleProgress.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.ui.showConsoleProgress` | boolean | `false` | 1.2.1 | When true, show the progress bar in the console. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/UI.scala#L27) |

### `spark.ui.store.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.ui.store.path` | string | _(optional)_ | 3.4.0 | Local directory where to cache application information for live UI. By default this is not set, meaning all application information will be kept in memory. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Status.scala#L74) |

### `spark.ui.strictTransportSecurity.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.ui.strictTransportSecurity` | string | _(optional)_ | 2.3.0 | Value for HTTP Strict Transport Security Response Header | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/UI.scala#L133) |

### `spark.ui.threadDump.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.ui.threadDump.flamegraphEnabled` | boolean | `true` | 4.0.0 | Whether to render the Flamegraph for executor thread dumps | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/UI.scala#L101) |

### `spark.ui.threadDumpsEnabled.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.ui.threadDumpsEnabled` | boolean | `true` | 1.2.0 | Whether to show a link for executor thread dumps in Stages and Executor pages. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/UI.scala#L95) |

### `spark.ui.timeline.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.ui.timeline.executors.maximum` | int | `250` | 3.2.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/UI.scala#L166) |
| `spark.ui.timeline.jobs.maximum` | int | `500` | 3.2.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/UI.scala#L156) |
| `spark.ui.timeline.stages.maximum` | int | `500` | 3.2.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/UI.scala#L161) |
| `spark.ui.timeline.tasks.maximum` | int | `1000` | 1.4.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/UI.scala#L151) |

### `spark.ui.timelineEnabled.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.ui.timelineEnabled` | boolean | `true` | 3.4.0 | Whether to display event timeline data on UI pages. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/UI.scala#L145) |

### `spark.ui.view.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.ui.view.acls` | string | `Nil` | 1.0.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/UI.scala#L176) |
| `spark.ui.view.acls.groups` | string | `Nil` | 2.0.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/UI.scala#L182) |

### `spark.ui.xContentTypeOptions.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.ui.xContentTypeOptions.enabled` | boolean | `true` | 2.3.0 | Set to 'true' for setting X-Content-Type-Options HTTP response header to 'nosniff' | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/UI.scala#L127) |

### `spark.ui.xXssProtection.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.ui.xXssProtection` | string | `1; mode=block` | 2.3.0 | Value for HTTP X-XSS-Protection response header | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/UI.scala#L121) |

### `spark.unsafe.exceptionOnMemoryLeak.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.unsafe.exceptionOnMemoryLeak` | boolean | `false` | 1.4.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1916) |

### `spark.unsafe.sorter.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.unsafe.sorter.spill.read.ahead.enabled` | boolean | `true` | 2.3.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1923) |
| `spark.unsafe.sorter.spill.reader.buffer.size` | bytes | `1024 * 1024` | 2.1.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1930) |

### `spark.user.groups.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.user.groups.mapping` | string | `org.apache.spark.security.ShellBasedGroupsMappingProvider` | 2.0.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/UI.scala#L212) |

### `spark.worker.cleanup.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.worker.cleanup.appDataTtl` | long | `7 * 24 * 3600` | 1.0.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Worker.scala#L72) |
| `spark.worker.cleanup.enabled` | boolean | `true` | 1.0.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Worker.scala#L62) |
| `spark.worker.cleanup.interval` | long | `60 * 30` | 1.0.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Worker.scala#L67) |

### `spark.worker.decommission.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.worker.decommission.signal` | string | `PWR` | 3.2.0 | The signal that used to trigger the worker to start decommission. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Worker.scala#L104) |

### `spark.worker.driverTerminateTimeout.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.worker.driverTerminateTimeout` | time | `10s` | 2.1.2 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Worker.scala#L57) |

### `spark.worker.executorStateSync.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.worker.executorStateSync.maxAttempts` | int | `5` | 3.3.0 | The max attempts the worker will try to sync the ExecutorState to the Master, if the failed attempts reach the max attempts limit, the worker will give up and exit. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2781) |

### `spark.worker.idPattern.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.worker.idPattern` | string | `worker-%s-%s-%d` | 4.0.0 | The pattern for worker ID generation based on Java `String.format` method. The default value is `worker-%s-%s-%d` which represents the existing worker id string, e.g., `worker-20231109183042-[fe80::1%lo0]-39729`. Please be careful to generate unique IDs | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Worker.scala#L110) |

### `spark.worker.initialRegistrationRetries.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.worker.initialRegistrationRetries` | int | `6` | 4.0.0 | The number of retries to reconnect in short intervals (between 5 and 15 seconds). | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Worker.scala#L40) |

### `spark.worker.maxRegistrationRetries.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.worker.maxRegistrationRetries` | int | `16` | 4.0.0 | The max number of retries to reconnect. After spark.worker.initialRegistrationRetries attempts, the interval is between 30 and 90 seconds. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Worker.scala#L48) |

### `spark.worker.preferConfiguredMasterAddress.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.worker.preferConfiguredMasterAddress` | boolean | `false` | 2.2.1 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Worker.scala#L77) |

### `spark.worker.resourcesFile.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.worker.resourcesFile` | string | _(optional)_ | 3.0.0 | Path to a file containing the resources allocated to the worker. The file should be formatted as a JSON array of ResourceAllocation objects. Only used internally in standalone mode. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Worker.scala#L26) |

### `spark.worker.timeout.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.worker.timeout` | long | `60` | 0.6.2 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Worker.scala#L35) |

### `spark.worker.ui.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.worker.ui.compressedLogFileLengthCacheSize` | int | `100` | 2.0.2 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Worker.scala#L98) |
| `spark.worker.ui.port` | int | _(optional)_ | 1.1.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Worker.scala#L82) |
| `spark.worker.ui.retainedDrivers` | int | `1000` | 1.5.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Worker.scala#L92) |
| `spark.worker.ui.retainedExecutors` | int | `1000` | 1.5.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/Worker.scala#L87) |

### `spark.yarn.dist.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.yarn.dist.forceDownloadSchemes` | string | `Nil` | 2.3.0 | Comma-separated list of schemes for which resources will be downloaded to the local disk prior to being added to YARN's distributed cache. For use in cases where the YARN service does not support schemes that are supported by Spark, like http, https and ftp, or jars required to be in the local YARN client's classpath. Wildcard '*' is denoted to download resources for all the schemes. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L1582) |
| `spark.yarn.dist.pyFiles` | string | `Nil` | 2.2.1 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L910) |

### `spark.yarn.isPython.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.yarn.isPython` | boolean | `false` | 1.5.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L706) |

### `spark.yarn.kerberos.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.yarn.kerberos.renewal.excludeHadoopFileSystems` | string | `Nil` | 3.2.0 | The list of Hadoop filesystem URLs whose hosts will be excluded from delegation token renewal at resource scheduler. Currently this is known to work under YARN, so YARN Resource Manager won't renew tokens for the application. Note that as resource scheduler does not renew token, so any application running longer than the original token expiration that tries to use that token will likely fail. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L894) |

### `spark.yarn.shuffle.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.yarn.shuffle.server.recovery.disabled` | boolean | `false` | 3.5.0 | Set to true for applications that prefer to disable recovery when the External Shuffle Service restarts. This configuration only takes effect on YARN. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2827) |

### `spark.yarn.stagingDir.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.yarn.stagingDir` | string | _(optional)_ | 2.0.0 | Staging directory used while submitting applications. | [src](https://github.com/apache/spark/blob/v4.1.2/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2484) |

## resource-managers/kubernetes

### `spark.kubernetes.allocation.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.kubernetes.allocation.batch.delay` | time | `1s` | 2.3.0 | Time to wait between each round of executor allocation. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala#L482) |
| `spark.kubernetes.allocation.batch.size` | int | `10` | 2.3.0 | Number of pods to launch at once in each round of executor allocation. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala#L474) |
| `spark.kubernetes.allocation.driver.readinessTimeout` | time | `1s` | 3.1.3 | Time to wait for driver pod to get ready before creating executor pods. This wait only happens on application start. If timeout happens, executor pods will still be created. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala#L498) |
| `spark.kubernetes.allocation.executor.timeout` | time | `600s` | 3.1.0 | Time to wait before a newly created executor POD request, which does not reached the POD pending state yet, considered timedout and will be deleted. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala#L509) |
| `spark.kubernetes.allocation.maxPendingPods` | int | `Int.MaxValue` | 3.2.0 | Maximum number of pending PODs allowed during executor allocation for this application. Those newly requested executors which are unknown by Kubernetes yet are also counted into this limit as they will change into pending PODs by time. This limit is independent from the resource profiles as it limits the sum of all allocation for all the used resource profiles. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala#L763) |
| `spark.kubernetes.allocation.maxPendingPodsPerRp` | int | `Int.MaxValue` | 4.1.0 | Maximum number of pending PODs allowed per resource profile ID during executor allocation. This provides finer-grained control over pending pods by limiting them per resource profile rather than globally. When set, this limit is enforced independently for each resource profile ID. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala#L775) |
| `spark.kubernetes.allocation.maximum` | int | `Int.MaxValue` | 4.1.0 | The maximum number of executor pods to try to create during the whole job lifecycle. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala#L490) |
| `spark.kubernetes.allocation.pods.allocator` | string | `direct` | 3.3.0 | Allocator to use for pods. Possible values are direct (the default) and statefulset , or a full class name of a class implementing AbstractPodsAllocator. Future version may add Job or replicaset. This is a developer API and may change or be removed at anytime. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala#L464) |

### `spark.kubernetes.appKillPodDeletionGracePeriod.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.kubernetes.appKillPodDeletionGracePeriod` | time | _(optional)_ | 3.0.0 | Time to wait for graceful deletion of Spark pods when spark-submit is used for killing an application. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala#L728) |

### `spark.kubernetes.configMap.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.kubernetes.configMap.maxSize` | long | `1048576` | 3.1.0 | Max size limit for a config map. This is configurable as per https://etcd.io/docs/v3.4.0/dev-guide/limit/ on k8s server end. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala#L179) |

### `spark.kubernetes.container.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.kubernetes.container.image` | string | _(optional)_ | 2.3.0 | Container image to use for Spark containers. Individual container types (e.g. driver or executor) can also be configured to use different images if desired, by setting the container type-specific image name. Note that `{{SPARK_VERSION}}` is the built-in variable that will be substituted with current Spark's version. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala#L138) |
| `spark.kubernetes.container.image.pullPolicy` | string | `IfNotPresent` | 2.3.0 | Kubernetes image pull policy. Valid values are Always, Never, and IfNotPresent. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala#L162) |
| `spark.kubernetes.container.image.pullSecrets` | string | `Nil` | 2.4.0 | Comma separated list of the Kubernetes secrets used to access private image registries. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala#L170) |

### `spark.kubernetes.context.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.kubernetes.context` | string | _(optional)_ | 3.0.0 | The desired context from your K8S config file used to configure the K8S client for interacting with the cluster. Useful if your config file has multiple clusters or user identities defined. The client library used locates the config file via the KUBECONFIG environment variable or by defaulting to .kube/config under your home directory. If not specified then your current context is used. You can always override specific aspects of the config file provided configuration using other Spark on K8S configuration options. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala#L37) |

### `spark.kubernetes.decommission.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.kubernetes.decommission.script` | string | `/opt/decom.sh` | 3.2.0 | The location of the script to use for graceful decommissioning | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala#L29) |

### `spark.kubernetes.driver.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.kubernetes.driver.annotateExitException` | boolean | `false` | 4.1.0 | If set to true, Spark will store the exit exception failed applications in the Kubernetes API server using the $EXIT_EXCEPTION_ANNOTATION annotation. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala#L800) |
| `spark.kubernetes.driver.connectionTimeout` | int | `10000` | 3.0.0 | connection timeout to be used in milliseconds for driver to request executors | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala#L273) |
| `spark.kubernetes.driver.container.image` | fallback | → `CONTAINER_IMAGE` | 2.3.0 | Container image to use for the driver. Note that `{{SPARK_VERSION}}` is the built-in variable that will be substituted with current Spark's version. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala#L148) |
| `spark.kubernetes.driver.limit.cores` | string | _(optional)_ | 2.3.0 | Specify the hard cpu limit for the driver pod | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala#L298) |
| `spark.kubernetes.driver.master` | string | `KUBERNETES_MASTER_INTERNAL_URL` | 3.0.0 | The internal Kubernetes master (API server) address to be used for driver to request executors or 'local[*]' for driver-only mode. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala#L50) |
| `spark.kubernetes.driver.ownPersistentVolumeClaim` | boolean | `true` | 3.2.0 | If true, driver pod becomes the owner of on-demand persistent volume claims instead of the executor pods | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala#L92) |
| `spark.kubernetes.driver.pod.excludedFeatureSteps` | string | `Nil` | 4.1.0 | Class names to exclude from driver pod feature steps. Comma separated. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala#L418) |
| `spark.kubernetes.driver.pod.featureSteps` | string | `Nil` | 3.2.0 | Class names of an extra driver pod feature step implementing KubernetesFeatureConfigStep. This is a developer API. Comma separated. Runs after all of Spark internal feature steps. Since 3.3.0, your driver feature step can implement `KubernetesDriverCustomFeatureConfigStep` where the driver config is also available. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala#L406) |
| `spark.kubernetes.driver.pod.name` | string | _(optional)_ | 2.3.0 | Name of the driver pod. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala#L356) |
| `spark.kubernetes.driver.podTemplateContainerName` | string | _(optional)_ | 3.0.0 | container name to be used as a basis for the driver in the given pod template | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala#L666) |
| `spark.kubernetes.driver.podTemplateFile` | string | _(optional)_ | 3.0.0 | File containing a template pod spec for the driver | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala#L652) |
| `spark.kubernetes.driver.request.cores` | string | _(optional)_ | 3.0.0 | Specify the cpu request for the driver pod | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala#L305) |
| `spark.kubernetes.driver.requestTimeout` | int | `10000` | 3.0.0 | request timeout to be used in milliseconds for driver to request executors | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala#L266) |
| `spark.kubernetes.driver.resourceNamePrefix` | string | _(optional)_ | 3.0.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala#L364) |
| `spark.kubernetes.driver.reusePersistentVolumeClaim` | boolean | `true` | 3.2.0 | If true, driver pod tries to reuse driver-owned on-demand persistent volume claims of the deleted executor pods if exists. This can be useful to reduce executor pod creation delay by skipping persistent volume creations. Note that a pod in `Terminating` pod status is not a deleted pod by definition and its resources including persistent volume claims are not reusable yet. Spark will create new persistent volume claims when there exists no reusable one. In other words, the total number of persistent volume claims can be larger than the number of running executors sometimes. This config requires ${KUBERNETES_DRIVER_OWN_PVC.key}=true. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala#L100) |
| `spark.kubernetes.driver.scheduler.name` | string | _(optional)_ | 3.3.0 | Specify the scheduler name for driver pod | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala#L333) |
| `spark.kubernetes.driver.service.deleteOnTermination` | boolean | `true` | 3.2.0 | If true, driver service will be deleted on Spark application termination. If false, it will be cleaned up when the driver pod is deleted. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala#L59) |
| `spark.kubernetes.driver.service.ipFamilies` | string | `IPv4` | 3.4.0 | A list of IP families for K8s Driver Service | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala#L84) |
| `spark.kubernetes.driver.service.ipFamilyPolicy` | string | `SingleStack` | 3.4.0 | K8s IP Family Policy for Driver Service | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala#L76) |
| `spark.kubernetes.driver.waitToReusePersistentVolumeClaim` | boolean | `false` | 3.4.0 | If true, driver pod counts the number of created on-demand persistent volume claims and wait if the number is greater than or equal to the total number of volumes which the Spark job is able to have. This config requires both ${KUBERNETES_DRIVER_OWN_PVC.key}=true and ${KUBERNETES_DRIVER_REUSE_PVC.key}=true. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala#L114) |

### `spark.kubernetes.dynamicAllocation.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.kubernetes.dynamicAllocation.deleteGracePeriod` | time | `5s` | 3.0.0 | How long to wait for executors to shut down gracefully before a forceful kill. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala#L721) |

### `spark.kubernetes.executor.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.kubernetes.executor.apiPollingInterval` | time | `30s` | 2.4.0 | Interval between polls against the Kubernetes API server to inspect the state of executors. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala#L553) |
| `spark.kubernetes.executor.checkAllContainers` | boolean | `true` | 3.1.0 | If set to true, all containers in the executor pod will be checked when reportingexecutor status. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala#L744) |
| `spark.kubernetes.executor.container.image` | fallback | → `CONTAINER_IMAGE` | 2.3.0 | Container image to use for the executors. Note that `{{SPARK_VERSION}}` is the built-in variable that will be substituted with current Spark's version. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala#L155) |
| `spark.kubernetes.executor.decommissionLabel` | string | _(optional)_ | 3.3.0 | Label to apply to a pod which is being decommissioned. Designed for use with pod disruption budgets and similar mechanism such as pod-deletion-cost. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala#L446) |
| `spark.kubernetes.executor.decommissionLabelValue` | string | _(optional)_ | 3.3.0 | Label value to apply to a pod which is being decommissioned. Designed for use with pod disruption budgets and similar mechanism such as pod-deletion-cost. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala#L455) |
| `spark.kubernetes.executor.deleteOnTermination` | boolean | `true` | 3.0.0 | If set to false then executor pods will not be deleted in case of failure or normal termination. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala#L696) |
| `spark.kubernetes.executor.deletedExecutorsCacheTimeout` | time | `180` | 4.1.0 | Time-to-live (TTL) value for the cache for deleted executors | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala#L704) |
| `spark.kubernetes.executor.disableConfigMap` | boolean | `false` | 3.2.0 | If true, disable ConfigMap creation for executors. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala#L399) |
| `spark.kubernetes.executor.enableApiPolling` | boolean | `true` | 3.4.0 | If Spark should poll Kubernetes for executor pod status. You should leave this enabled unless you're encountering issues with your etcd. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala#L534) |
| `spark.kubernetes.executor.enableApiWatcher` | boolean | `true` | 3.4.0 | If Spark should create watchers for executor pod status. You should leave this enabled unless you're encountering issues with your etcd. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala#L543) |
| `spark.kubernetes.executor.enablePollingWithResourceVersion` | boolean | `false` | 3.3.0 | If true, `resourceVersion` is set with `0` during invoking pod listing APIs in order to allow API Server-side caching. This should be used carefully. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala#L563) |
| `spark.kubernetes.executor.eventProcessingInterval` | time | `1s` | 2.4.0 | Interval between successive inspection of executor events sent from the Kubernetes API. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala#L571) |
| `spark.kubernetes.executor.limit.cores` | string | _(optional)_ | 2.3.0 | Specify the hard cpu limit for each executor pod | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala#L319) |
| `spark.kubernetes.executor.minTasksPerExecutorBeforeRolling` | int | `0` | 3.3.0 | The minimum number of tasks per executor before rolling. Spark will not roll executors whose total number of tasks is smaller than this configuration. The default value is zero. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala#L232) |
| `spark.kubernetes.executor.missingPodDetectDelta` | time | `30s` | 3.1.1 | When a registered executor's POD is missing from the Kubernetes API server's polled list of PODs then this delta time is taken as the accepted time difference between the registration time and the time of the polling. After this time the POD is considered missing from the cluster and the executor will be removed. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala#L752) |
| `spark.kubernetes.executor.pod.excludedFeatureSteps` | string | `Nil` | 4.1.0 | Class name to exclude from executor pod feature steps. Comma separated. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala#L438) |
| `spark.kubernetes.executor.pod.featureSteps` | string | `Nil` | 3.2.0 | Class name of an extra executor pod feature step implementing KubernetesFeatureConfigStep. This is a developer API. Comma separated. Runs after all of Spark internal feature steps. Since 3.3.0, your executor feature step can implement `KubernetesExecutorCustomFeatureConfigStep` where the executor config is also available. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala#L426) |
| `spark.kubernetes.executor.podNamePrefix` | string | _(optional)_ | 2.3.0 | Prefix to use in front of the executor pod names. It must conform the rules defined by the Kubernetes <a href="https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#dns-subdomain-names">DNS Subdomain Names</a>. The prefix will be used to generate executor pod names in the form of <code>$podNamePrefix-exec-$id</code>, where the `id` is a positive int value, so the length of the `podNamePrefix` needs to be <= 237(= 253 - 10 - 6). | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala#L384) |
| `spark.kubernetes.executor.podTemplateContainerName` | string | _(optional)_ | 3.0.0 | container name to be used as a basis for executors in the given pod template | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala#L673) |
| `spark.kubernetes.executor.podTemplateFile` | string | _(optional)_ | 3.0.0 | File containing a template pod spec for executors | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala#L659) |
| `spark.kubernetes.executor.request.cores` | string | _(optional)_ | 2.4.0 | Specify the cpu request for each executor pod | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala#L349) |
| `spark.kubernetes.executor.rollInterval` | time | `0` | 3.3.0 | Interval between executor roll operations. To disable, set 0 (default) | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala#L188) |
| `spark.kubernetes.executor.rollPolicy` | string | `ExecutorRollPolicy.OUTLIER.toString` | 3.3.0 | Executor roll policy: Valid values are ID, ADD_TIME, TOTAL_GC_TIME, TOTAL_DURATION, AVERAGE_DURATION, FAILED_TASKS, PEAK_JVM_ONHEAP_MEMORY, PEAK_JVM_OFFHEAP_MEMORY, OUTLIER (default), and OUTLIER_NO_FALLBACK. When executor roll happens, Spark uses this policy to choose an executor and decommission it. The built-in policies are based on executor summary.ID policy chooses an executor with the smallest executor ID. ADD_TIME policy chooses an executor with the smallest add-time. TOTAL_GC_TIME policy chooses an executor with the biggest total task GC time. TOTAL_DURATION policy chooses an executor with the biggest total task time. AVERAGE_DURATION policy chooses an executor with the biggest average task time. FAILED_TASKS policy chooses an executor with the most number of failed tasks. PEAK_JVM_ONHEAP_MEMORY policy chooses an executor with the biggest peak JVM on-heap memory. PEAK_JVM_OFFHEAP_MEMORY policy chooses an executor with the biggest peak JVM off-heap memory. TOTAL_SHUFFLE_WRITE policy chooses an executor with the biggest total shuffle write. DISK_USED policy chooses an executor with the biggest used disk size. OUTLIER policy chooses an executor with outstanding statistics which is bigger thanat least two standard deviation from the mean in average task time, total task time, total task GC time, and the number of failed tasks if exists. If there is no outlier it works like TOTAL_DURATION policy. OUTLIER_NO_FALLBACK policy picks an outlier using the OUTLIER policy above. If there is no outlier then no executor will be rolled. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala#L202) |
| `spark.kubernetes.executor.scheduler.name` | string | _(optional)_ | 3.0.0 | Specify the scheduler name for each executor pod | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala#L326) |
| `spark.kubernetes.executor.terminationGracePeriodSeconds` | time | `30s` | 4.1.0 | Time to wait for graceful termination of executor pods. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala#L713) |
| `spark.kubernetes.executor.useDriverPodIP` | boolean | `false` | 4.1.0 | If true, executor pods use Driver pod IP directly instead of Driver Service. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala#L124) |

### `spark.kubernetes.executorSnapshotsSubscribersShutdownGracePeriod.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.kubernetes.executorSnapshotsSubscribersShutdownGracePeriod` | time | `20s` | 3.4.0 | Time to wait for graceful shutdown kubernetes-executor-snapshots-subscribers thread pool. Since it may be called by ShutdownHookManager, where timeout is controlled by hadoop configuration `hadoop.service.shutdown.timeout` (default is 30s). As the whole Spark shutdown procedure shares the above timeout, this value should be short than that to prevent blocking the following shutdown procedures. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala#L787) |

### `spark.kubernetes.file.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.kubernetes.file.upload.path` | string | _(optional)_ | 3.0.0 | Hadoop compatible file system path where files from the local file system will be uploaded to in cluster mode. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala#L736) |

### `spark.kubernetes.hadoop.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.kubernetes.hadoop.configMapName` | string | _(optional)_ | 3.0.0 | Specify the name of the ConfigMap, containing the HADOOP_CONF_DIR files, to be mounted on the driver and executors for custom Hadoop configuration. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala#L609) |

### `spark.kubernetes.kerberos.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.kubernetes.kerberos.krb5.configMapName` | string | _(optional)_ | 3.0.0 | Specify the name of the ConfigMap, containing the krb5.conf file, to be mounted on the driver and executors for Kerberos. Note: The KDC definedneeds to be visible from inside the containers | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala#L600) |
| `spark.kubernetes.kerberos.krb5.path` | string | _(optional)_ | 3.0.0 | Specify the local location of the krb5.conf file to be mounted on the driver and executors for Kerberos. Note: The KDC defined needs to be visible from inside the containers | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala#L591) |
| `spark.kubernetes.kerberos.tokenSecret.itemKey` | string | _(optional)_ | 3.0.0 | Specify the item key of the data where your existing delegation tokens are stored. This removes the need for the job user to provide any keytab for launching a job | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala#L625) |
| `spark.kubernetes.kerberos.tokenSecret.name` | string | _(optional)_ | 3.0.0 | Specify the name of the secret where your existing delegation tokens are stored. This removes the need for the job user to provide any keytab for launching a job | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala#L617) |

### `spark.kubernetes.legacy.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.kubernetes.legacy.useReadWriteOnceAccessMode` | boolean | `false` | 3.4.3 | If true, use ReadWriteOnce instead of ReadWriteOncePod as persistence volume access mode. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala#L67) |

### `spark.kubernetes.local.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.kubernetes.local.dirs.tmpfs` | boolean | `false` | 3.0.0 | If set to true then emptyDir volumes created to back SPARK_LOCAL_DIRS will have their medium set to Memory so that they will be created as tmpfs (i.e. RAM) backed volumes. This may improve performance but scratch space usage will count towards your pods memory limit so you may wish to request more memory. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala#L642) |

### `spark.kubernetes.memoryOverheadFactor.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.kubernetes.memoryOverheadFactor` | double | `0.1` | 2.4.0 | This sets the Memory Overhead Factor that will allocate memory to non-JVM jobs which in the case of JVM tasks will default to 0.10 and 0.40 for non-JVM jobs | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala#L581) |

### `spark.kubernetes.namespace.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.kubernetes.namespace` | string | `default` | 2.3.0 | The namespace that will be used for running the driver and executor pods. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala#L131) |

### `spark.kubernetes.report.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.kubernetes.report.interval` | time | `1s` | 2.3.0 | Interval between reports of the current app status in cluster mode. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala#L526) |

### `spark.kubernetes.resource.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.kubernetes.resource.type` | string | _(optional)_ | 2.4.1 | This sets the resource type internally | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala#L633) |

### `spark.kubernetes.scheduler.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.kubernetes.scheduler.name` | string | _(optional)_ | 3.3.0 | Specify the scheduler name for driver and executor pods. If `${KUBERNETES_DRIVER_SCHEDULER_NAME.key}` or `${KUBERNETES_EXECUTOR_SCHEDULER_NAME.key}` is set, will override this. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala#L340) |

### `spark.kubernetes.submission.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.kubernetes.submission.connectionTimeout` | int | `10000` | 3.0.0 | connection timeout to be used in milliseconds for starting the driver | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala#L259) |
| `spark.kubernetes.submission.requestTimeout` | int | `10000` | 3.0.0 | request timeout to be used in milliseconds for starting the driver | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala#L252) |
| `spark.kubernetes.submission.waitAppCompletion` | boolean | `true` | 2.3.0 | In cluster mode, whether to wait for the application to finish before exiting the launcher process. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala#L518) |

### `spark.kubernetes.submitInDriver.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.kubernetes.submitInDriver` | boolean | `false` | 2.4.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala#L312) |

### `spark.kubernetes.trust.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.kubernetes.trust.certificates` | boolean | `false` | 3.2.0 | If set to true then client can submit to kubernetes cluster only with token | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala#L683) |

## resource-managers/yarn

### `spark.driver.appUIAddress.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.driver.appUIAddress` | string | _(optional)_ | 1.1.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/yarn/src/main/scala/org/apache/spark/deploy/yarn/config/package.scala#L328) |

### `spark.yarn.am.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.yarn.am.attemptFailuresValidityInterval` | time | _(optional)_ | 1.6.0 | Interval after which AM failures will be considered independent and not accumulate towards the attempt count. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/yarn/src/main/scala/org/apache/spark/deploy/yarn/config/package.scala#L48) |
| `spark.yarn.am.clientModeExitOnError` | boolean | `false` | 3.3.0 | In yarn-client mode, when this is true, if driver got application report with final status of KILLED or FAILED, driver will stop corresponding SparkContext and exit program with code 1. Note, if this is true and called from another application, it will terminate the parent application as well. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/yarn/src/main/scala/org/apache/spark/deploy/yarn/config/package.scala#L71) |
| `spark.yarn.am.clientModeTreatDisconnectAsFailed` | boolean | `false` | 3.3.0 | Treat yarn-client unclean disconnects as failures. In yarn-client mode, normally the application will always finish with a final status of SUCCESS because in some cases, it is not possible to know if the Application was terminated intentionally by the user or if there was a real error. This config changes that behavior such that if the Application Master disconnects from the driver uncleanly (ie without the proper shutdown handshake) the application will terminate with a final status of FAILED. This will allow the caller to decide if it was truly a failure. Note that if this config is set and the user just terminate the client application badly it may show a status of FAILED when it wasn't really FAILED. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/yarn/src/main/scala/org/apache/spark/deploy/yarn/config/package.scala#L56) |
| `spark.yarn.am.cores` | int | `1` | 1.3.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/yarn/src/main/scala/org/apache/spark/deploy/yarn/config/package.scala#L299) |
| `spark.yarn.am.extraJavaOptions` | string | _(optional)_ | 1.3.0 | Extra Java options for the client-mode AM. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/yarn/src/main/scala/org/apache/spark/deploy/yarn/config/package.scala#L304) |
| `spark.yarn.am.extraLibraryPath` | string | _(optional)_ | 1.4.0 | Extra native library path for the client-mode AM. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/yarn/src/main/scala/org/apache/spark/deploy/yarn/config/package.scala#L310) |
| `spark.yarn.am.finalMessageLimit` | bytes | `1m` | 2.4.0 | The limit size of final diagnostic message for our ApplicationMaster to unregister from the ResourceManager. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/yarn/src/main/scala/org/apache/spark/deploy/yarn/config/package.scala#L290) |
| `spark.yarn.am.memory` | bytes | `512m` | 1.3.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/yarn/src/main/scala/org/apache/spark/deploy/yarn/config/package.scala#L321) |
| `spark.yarn.am.memoryOverhead` | bytes | _(optional)_ | 1.3.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/yarn/src/main/scala/org/apache/spark/deploy/yarn/config/package.scala#L316) |
| `spark.yarn.am.nodeLabelExpression` | string | _(optional)_ | 1.6.0 | Node label expression for the AM. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/yarn/src/main/scala/org/apache/spark/deploy/yarn/config/package.scala#L260) |
| `spark.yarn.am.tokenConfRegex` | string | _(optional)_ | 3.3.0 | The value of this config is a regex expression used to grep a list of config entries from the job's configuration file (e.g., hdfs-site.xml) and send to RM, which uses them when renewing delegation tokens. A typical use case of this feature is to support delegation tokens in an environment where a YARN cluster needs to talk to multiple downstream HDFS clusters, where the YARN RM may not have configs (e.g., dfs.nameservices, dfs.ha.namenodes.*, dfs.namenode.rpc-address.*) to connect to these clusters. In this scenario, Spark users can specify the config value to be '^dfs.nameservices$\|^dfs.namenode.rpc-address.*$\|^dfs.ha.namenodes.*$' to parse these HDFS configs from the job's local configuration files. This config is very similar to 'mapreduce.job.send-token-conf'. Please check YARN-5910 for more details. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/yarn/src/main/scala/org/apache/spark/deploy/yarn/config/package.scala#L82) |
| `spark.yarn.am.waitTime` | time | `100s` | 1.3.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/yarn/src/main/scala/org/apache/spark/deploy/yarn/config/package.scala#L249) |

### `spark.yarn.applicationType.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.yarn.applicationType` | string | `SPARK` | 3.1.0 | Type of this application,it allows user to specify a more specific type for the application, such as SPARK,SPARK-SQL, SPARK-STREAMING, SPARK-MLLIB and SPARK-GRAPH | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/yarn/src/main/scala/org/apache/spark/deploy/yarn/config/package.scala#L151) |

### `spark.yarn.archive.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.yarn.archive` | string | _(optional)_ | 2.0.0 | Location of archive containing jars files with Spark classes. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/yarn/src/main/scala/org/apache/spark/deploy/yarn/config/package.scala#L161) |

### `spark.yarn.cache.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.yarn.cache.confArchive` | string | _(optional)_ | 2.0.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/yarn/src/main/scala/org/apache/spark/deploy/yarn/config/package.scala#L426) |
| `spark.yarn.cache.filenames` | string | `Nil` | 2.0.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/yarn/src/main/scala/org/apache/spark/deploy/yarn/config/package.scala#L389) |
| `spark.yarn.cache.sizes` | long | `Nil` | 2.0.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/yarn/src/main/scala/org/apache/spark/deploy/yarn/config/package.scala#L396) |
| `spark.yarn.cache.timestamps` | long | `Nil` | 2.0.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/yarn/src/main/scala/org/apache/spark/deploy/yarn/config/package.scala#L403) |
| `spark.yarn.cache.types` | string | `Nil` | 2.0.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/yarn/src/main/scala/org/apache/spark/deploy/yarn/config/package.scala#L418) |
| `spark.yarn.cache.visibilities` | string | `Nil` | 2.0.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/yarn/src/main/scala/org/apache/spark/deploy/yarn/config/package.scala#L410) |

### `spark.yarn.client.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.yarn.client.statCache.preload.enabled` | boolean | `false` | 4.0.0 | Enables statCache to be preloaded at YARN client side. This feature analyzes the pattern of resources paths, and if multiple resources shared the same parent directory, a single <code>listStatus</code> will be invoked on the parent directory instead of multiple <code>getFileStatus</code> on individual resources. If most resources are from a small set of directories, this can improve job submission time. Enabling this feature may potentially increase client memory overhead. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/yarn/src/main/scala/org/apache/spark/deploy/yarn/config/package.scala#L465) |
| `spark.yarn.client.statCache.preload.perDirectoryThreshold` | int | `5` | 4.0.0 | Minimum resource count in a directory to trigger statCache preloading when submitting an application. If the number of resources in a directory, without any wildcards, equals or exceeds this threshold, the statCache for that directory will be preloaded. This configuration will only take effect when <code>spark.yarn.client.statCache.preloaded.enabled</code> option is enabled. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/yarn/src/main/scala/org/apache/spark/deploy/yarn/config/package.scala#L477) |

### `spark.yarn.clientLaunchMonitorInterval.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.yarn.clientLaunchMonitorInterval` | time | `1s` | 2.3.0 | Interval between requests for status the client mode AM when starting the app. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/yarn/src/main/scala/org/apache/spark/deploy/yarn/config/package.scala#L232) |

### `spark.yarn.config.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.yarn.config.gatewayPath` | string | _(optional)_ | 1.5.0 | Root of configuration paths that is present on gateway nodes, and will be replaced with the corresponding path in cluster machines. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/yarn/src/main/scala/org/apache/spark/deploy/yarn/config/package.scala#L119) |
| `spark.yarn.config.replacementPath` | string | _(optional)_ | 1.5.0 | Path to use as a replacement for ${GATEWAY_ROOT_PATH.key} when launching processes in the YARN cluster. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/yarn/src/main/scala/org/apache/spark/deploy/yarn/config/package.scala#L126) |

### `spark.yarn.containerLauncherMaxThreads.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.yarn.containerLauncherMaxThreads` | int | `25` | 1.2.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/yarn/src/main/scala/org/apache/spark/deploy/yarn/config/package.scala#L267) |

### `spark.yarn.dist.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.yarn.dist.archives` | string | `Nil` | 1.0.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/yarn/src/main/scala/org/apache/spark/deploy/yarn/config/package.scala#L174) |
| `spark.yarn.dist.files` | string | `Nil` | 1.0.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/yarn/src/main/scala/org/apache/spark/deploy/yarn/config/package.scala#L180) |
| `spark.yarn.dist.jars` | string | `Nil` | 2.0.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/yarn/src/main/scala/org/apache/spark/deploy/yarn/config/package.scala#L186) |

### `spark.yarn.exclude.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.yarn.exclude.nodes` | string | `Nil` | 3.0.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/yarn/src/main/scala/org/apache/spark/deploy/yarn/config/package.scala#L440) |

### `spark.yarn.executor.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.yarn.executor.launch.excludeOnFailure.enabled` | boolean | `false` | 3.1.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/yarn/src/main/scala/org/apache/spark/deploy/yarn/config/package.scala#L434) |
| `spark.yarn.executor.nodeLabelExpression` | string | _(optional)_ | 1.4.0 | Node label expression for executors. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/yarn/src/main/scala/org/apache/spark/deploy/yarn/config/package.scala#L336) |

### `spark.yarn.historyServer.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.yarn.historyServer.address` | string | _(optional)_ | 1.0.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/yarn/src/main/scala/org/apache/spark/deploy/yarn/config/package.scala#L138) |
| `spark.yarn.historyServer.allowTracking` | boolean | `false` | 2.2.0 | Allow using the History Server URL for the application as the tracking URL for the application when the Web UI is not enabled. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/yarn/src/main/scala/org/apache/spark/deploy/yarn/config/package.scala#L144) |

### `spark.yarn.includeDriverLogsLink.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.yarn.includeDriverLogsLink` | boolean | `false` | 3.1.0 | In cluster mode, whether the client application report includes links to the driver container's logs. This requires polling the ResourceManager's REST API, so it places some additional load on the RM. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/yarn/src/main/scala/org/apache/spark/deploy/yarn/config/package.scala#L239) |

### `spark.yarn.jars.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.yarn.jars` | string | _(optional)_ | 2.0.0 | Location of jars containing Spark classes. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/yarn/src/main/scala/org/apache/spark/deploy/yarn/config/package.scala#L167) |

### `spark.yarn.maxAppAttempts.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.yarn.maxAppAttempts` | int | _(optional)_ | 1.3.0 | Maximum number of AM attempts before failing the app. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/yarn/src/main/scala/org/apache/spark/deploy/yarn/config/package.scala#L97) |

### `spark.yarn.metrics.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.yarn.metrics.namespace` | string | _(optional)_ | 2.4.0 | The root namespace for AM metrics reporting. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/yarn/src/main/scala/org/apache/spark/deploy/yarn/config/package.scala#L254) |

### `spark.yarn.populateHadoopClasspath.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.yarn.populateHadoopClasspath` | boolean | `isHadoopProvided()` | 2.4.6 | Whether to populate Hadoop classpath from `yarn.application.classpath` and `mapreduce.application.classpath` Note that if this is set to `false`, it requires a `with-Hadoop` Spark distribution that bundles Hadoop runtime or user has to provide a Hadoop installation separately. By default, for `with-hadoop` Spark distribution, this is set to `false`; for `no-hadoop` distribution, this is set to `true`. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/yarn/src/main/scala/org/apache/spark/deploy/yarn/config/package.scala#L109) |

### `spark.yarn.preserve.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.yarn.preserve.staging.files` | boolean | `false` | 1.1.0 | Whether to preserve temporary files created by the job in HDFS. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/yarn/src/main/scala/org/apache/spark/deploy/yarn/config/package.scala#L192) |

### `spark.yarn.priority.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.yarn.priority` | int | _(optional)_ | 3.0.0 | Application priority for YARN to define pending applications ordering policy, those with higher value have a better opportunity to be activated. Currently, YARN only supports application priority when using FIFO ordering policy. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/yarn/src/main/scala/org/apache/spark/deploy/yarn/config/package.scala#L39) |

### `spark.yarn.queue.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.yarn.queue` | string | `default` | 1.0.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/yarn/src/main/scala/org/apache/spark/deploy/yarn/config/package.scala#L133) |

### `spark.yarn.report.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.yarn.report.interval` | time | `1s` | 0.9.0 | Interval between reports of the current app status. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/yarn/src/main/scala/org/apache/spark/deploy/yarn/config/package.scala#L213) |
| `spark.yarn.report.loggingFrequency` | int | `30` | 3.5.0 | Maximum number of application reports processed until the next application status is logged. If there is a change of state, the application status will be logged regardless of the number of application reports processed. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/yarn/src/main/scala/org/apache/spark/deploy/yarn/config/package.scala#L220) |

### `spark.yarn.resourceFpgaDeviceName.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.yarn.resourceFpgaDeviceName` | string | `yarn.io/fpga` | 3.2.1 | Specify the mapping of the Spark resource type of fpga to the YARN resource representing a FPGA. By default YARN uses yarn.io/fpga but if YARN has been configured with a custom resource type, this allows remapping it. Applies when using the <code>spark.{driver/executor}.resource.fpga.*</code> configs. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/yarn/src/main/scala/org/apache/spark/deploy/yarn/config/package.scala#L455) |

### `spark.yarn.resourceGpuDeviceName.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.yarn.resourceGpuDeviceName` | string | `yarn.io/gpu` | 3.2.1 | Specify the mapping of the Spark resource type of gpu to the YARN resource representing a GPU. By default YARN uses yarn.io/gpu but if YARN has been configured with a custom resource type, this allows remapping it. Applies when using the <code>spark.{driver/executor}.resource.gpu.*</code> configs. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/yarn/src/main/scala/org/apache/spark/deploy/yarn/config/package.scala#L446) |

### `spark.yarn.rolledLog.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.yarn.rolledLog.excludePattern` | string | _(optional)_ | 2.0.0 | Java Regex to filter the log files which match the defined exclude pattern and those log files will not be aggregated in a rolling fashion. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/yarn/src/main/scala/org/apache/spark/deploy/yarn/config/package.scala#L362) |
| `spark.yarn.rolledLog.includePattern` | string | _(optional)_ | 2.0.0 | Java Regex to filter the log files which match the defined include pattern and those log files will be aggregated in a rolling fashion. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/yarn/src/main/scala/org/apache/spark/deploy/yarn/config/package.scala#L354) |

### `spark.yarn.scheduler.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.yarn.scheduler.heartbeat.interval-ms` | time | `3s` | 0.8.1 |  | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/yarn/src/main/scala/org/apache/spark/deploy/yarn/config/package.scala#L279) |
| `spark.yarn.scheduler.initial-allocation.interval` | time | `200ms` | 1.4.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/yarn/src/main/scala/org/apache/spark/deploy/yarn/config/package.scala#L285) |
| `spark.yarn.scheduler.reporterThread.maxFailures` | int | `5` | 1.2.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/yarn/src/main/scala/org/apache/spark/deploy/yarn/config/package.scala#L273) |

### `spark.yarn.secondary.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.yarn.secondary.jars` | string | _(optional)_ | 0.9.2 |  | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/yarn/src/main/scala/org/apache/spark/deploy/yarn/config/package.scala#L380) |

### `spark.yarn.submit.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.yarn.submit.file.replication` | int | _(optional)_ | 0.8.1 | Replication factor for files uploaded by Spark to HDFS. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/yarn/src/main/scala/org/apache/spark/deploy/yarn/config/package.scala#L198) |
| `spark.yarn.submit.waitAppCompletion` | boolean | `true` | 1.4.0 | In cluster mode, whether to wait for the application to finish before exiting the launcher process. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/yarn/src/main/scala/org/apache/spark/deploy/yarn/config/package.scala#L206) |

### `spark.yarn.tags.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.yarn.tags` | string | _(optional)_ | 1.5.0 | Comma-separated list of strings to pass through as YARN application tags appearing in YARN Application Reports, which can be used for filtering when querying YARN. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/yarn/src/main/scala/org/apache/spark/deploy/yarn/config/package.scala#L31) |

### `spark.yarn.unmanagedAM.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.yarn.unmanagedAM.enabled` | boolean | `false` | 3.0.0 | In client mode, whether to launch the Application Master service as part of the client using unmanaged am. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/yarn/src/main/scala/org/apache/spark/deploy/yarn/config/package.scala#L344) |

### `spark.yarn.user.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.yarn.user.classpath.first` | boolean | `false` | 1.3.0 | Whether to place user jars in front of Spark's classpath. | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/yarn/src/main/scala/org/apache/spark/deploy/yarn/config/package.scala#L103) |
| `spark.yarn.user.jar` | string | _(optional)_ | 1.1.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/resource-managers/yarn/src/main/scala/org/apache/spark/deploy/yarn/config/package.scala#L372) |

## sql/catalyst

### `spark.sql.adaptive.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.adaptive.advisoryPartitionSizeInBytes` | fallback | → `SHUFFLE_TARGET_POSTSHUFFLE_INPUT_SIZE` | 3.0.0 | The advisory size in bytes of the shuffle partition during adaptive optimization (when ${ADAPTIVE_EXECUTION_ENABLED.key} is true). It takes effect when Spark coalesces small shuffle partitions or splits skewed shuffle partition. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L973) |
| `spark.sql.adaptive.applyFinalStageShuffleOptimizations` | boolean | `true` | 3.4.2 | Configures whether adaptive query execution (if enabled) should apply shuffle coalescing and local shuffle read optimization for the final query stage. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L956) |
| `spark.sql.adaptive.autoBroadcastJoinThreshold` | bytes | _(optional)_ | 3.2.0 | Configures the maximum size in bytes for a table that will be broadcast to all worker nodes when performing a join. By setting this value to -1 broadcasting can be disabled. The default value is same with ${AUTO_BROADCASTJOIN_THRESHOLD.key}. Note that, this config is used only in adaptive framework. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1170) |
| `spark.sql.adaptive.coalescePartitions.enabled` | boolean | `true` | 3.0.0 | When true and '${ADAPTIVE_EXECUTION_ENABLED.key}' is true, Spark will coalesce contiguous shuffle partitions according to the target size (specified by '${ADVISORY_PARTITION_SIZE_IN_BYTES.key}'), to avoid too many small tasks. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L981) |
| `spark.sql.adaptive.coalescePartitions.initialPartitionNum` | int | _(optional)_ | 3.0.0 | The initial number of shuffle partitions before coalescing. If not set, it equals to ${SHUFFLE_PARTITIONS.key}. This configuration only has an effect when '${ADAPTIVE_EXECUTION_ENABLED.key}' and '${COALESCE_PARTITIONS_ENABLED.key}' are both true. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1026) |
| `spark.sql.adaptive.coalescePartitions.minPartitionNum` | int | _(optional)_ | 3.0.0 | (deprecated) The suggested (not guaranteed) minimum number of shuffle partitions after coalescing. If not set, the default value is the default parallelism of the Spark cluster. This configuration only has an effect when '${ADAPTIVE_EXECUTION_ENABLED.key}' and '${COALESCE_PARTITIONS_ENABLED.key}' are both true. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1013) |
| `spark.sql.adaptive.coalescePartitions.minPartitionSize` | bytes | `1MB` | 3.2.0 | The minimum size of shuffle partitions after coalescing. This is useful when the adaptively calculated target size is too small during partition coalescing. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1004) |
| `spark.sql.adaptive.coalescePartitions.parallelismFirst` | boolean | `true` | 3.2.0 | When true, Spark does not respect the target size specified by '${ADVISORY_PARTITION_SIZE_IN_BYTES.key}' (default 64MB) when coalescing contiguous shuffle partitions, but adaptively calculate the target size according to the default parallelism of the Spark cluster. The calculated size is usually smaller than the configured target size. This is to maximize the parallelism and avoid performance regressions when enabling adaptive query execution. It's recommended to set this config to false on a busy cluster to make resource utilization more efficient (not many small tasks). | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L990) |
| `spark.sql.adaptive.customCostEvaluatorClass` | string | _(optional)_ | 3.2.0 | The custom cost evaluator class to be used for adaptive execution. If not being set, Spark will use its own SimpleCostEvaluator by default. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1217) |
| `spark.sql.adaptive.enabled` | boolean | `true` | 1.6.0 | When true, enable adaptive query execution, which re-optimizes the query plan in the middle of query execution, based on accurate runtime statistics. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L929) |
| `spark.sql.adaptive.fetchShuffleBlocksInBatch` | boolean | `true` | 3.0.0 | Whether to fetch the contiguous shuffle blocks in batch. Instead of fetching blocks one by one, fetching contiguous shuffle blocks for the same map task in batch can reduce IO and improve performance. Note, multiple contiguous blocks exist in single fetch request only happen when '${ADAPTIVE_EXECUTION_ENABLED.key}' and '${COALESCE_PARTITIONS_ENABLED.key}' are both true. This feature also depends on a relocatable serializer, the concatenation support codec in use, the new version shuffle fetch protocol and io encryption is disabled. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1097) |
| `spark.sql.adaptive.forceApply` | boolean | `false` | 3.0.0 | Adaptive query execution is skipped when the query does not have exchanges or sub-queries. By setting this config to true (together with '${ADAPTIVE_EXECUTION_ENABLED.key}' set to true), Spark will force apply adaptive query execution for all supported queries. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L945) |
| `spark.sql.adaptive.forceOptimizeSkewedJoin` | boolean | `false` | 3.3.0 | When true, force enable OptimizeSkewedJoin even if it introduces extra shuffle. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1210) |
| `spark.sql.adaptive.localShuffleReader.enabled` | boolean | `true` | 3.0.0 | When true and '${ADAPTIVE_EXECUTION_ENABLED.key}' is true, Spark tries to use local shuffle reader to read the shuffle data when the shuffle partitioning is not needed, for example, after converting sort-merge join to broadcast-hash join. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1111) |
| `spark.sql.adaptive.logLevel` | enum | `Level.DEBUG` | 3.0.0 | Configures the log level for adaptive execution logging of plan changes. The value can be ${VALID_LOG_LEVELS.mkString()}. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L964) |
| `spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold` | bytes | `0` | 3.2.0 | Configures the maximum size in bytes per partition that can be allowed to build local hash map. If this value is not smaller than ${ADVISORY_PARTITION_SIZE_IN_BYTES.key} and all the partition size are not larger than this config, join selection prefer to use shuffled hash join instead of sort merge join regardless of the value of ${PREFER_SORTMERGEJOIN.key}. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1180) |
| `spark.sql.adaptive.nonEmptyPartitionRatioForBroadcastJoin` | double | `0.2` | 3.0.0 | The relation with a non-empty partition ratio lower than this config will not be considered as the build side of a broadcast-hash join in adaptive execution regardless of its size.This configuration only has an effect when '${ADAPTIVE_EXECUTION_ENABLED.key}' is true. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1149) |
| `spark.sql.adaptive.optimizeSkewsInRebalancePartitions.enabled` | boolean | `true` | 3.2.0 | When true and '${ADAPTIVE_EXECUTION_ENABLED.key}' is true, Spark will optimize the skewed shuffle partitions in RebalancePartitions and split them to smaller ones according to the target size (specified by '${ADVISORY_PARTITION_SIZE_IN_BYTES.key}'), to avoid data skew. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1191) |
| `spark.sql.adaptive.optimizer.excludedRules` | string | _(optional)_ | 3.1.0 | Configures a list of rules to be disabled in the adaptive optimizer, in which the rules are specified by their rule names and separated by comma. The optimizer will log the rules that have indeed been excluded. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1161) |
| `spark.sql.adaptive.rebalancePartitionsSmallPartitionFactor` | double | `0.2` | 3.3.0 | A partition will be merged during splitting if its size is small than this factor multiply ${ADVISORY_PARTITION_SIZE_IN_BYTES.key}. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1201) |
| `spark.sql.adaptive.shuffle.targetPostShuffleInputSize` | bytes | `64MB` | 1.6.0 | (Deprecated since Spark 3.0) | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L921) |
| `spark.sql.adaptive.skewJoin.enabled` | boolean | `true` | 3.0.0 | When true and '${ADAPTIVE_EXECUTION_ENABLED.key}' is true, Spark dynamically handles skew in shuffled join (sort-merge and shuffled hash) by splitting (and replicating if needed) skewed partitions. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1120) |
| `spark.sql.adaptive.skewJoin.skewedPartitionFactor` | double | `5.0` | 3.0.0 | A partition is considered as skewed if its size is larger than this factor multiplying the median partition size and also larger than 'spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes' | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1129) |
| `spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes` | bytes | `256MB` | 3.0.0 | A partition is considered as skewed if its size in bytes is larger than this threshold and also larger than '${SKEW_JOIN_SKEWED_PARTITION_FACTOR.key}' multiplying the median partition size. Ideally this config should be set larger than '${ADVISORY_PARTITION_SIZE_IN_BYTES.key}'. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1139) |
| `spark.sql.adaptive.streaming.stateless.enabled` | boolean | `true` | 4.1.0 | When true, enable adaptive query execution for stateless streaming query. To enable this config, `spark.sql.adaptive.enabled` needs to be also enabled. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L937) |

### `spark.sql.addPartitionInBatch.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.addPartitionInBatch.size` | int | `100` | 3.0.0 | The number of partitions to be handled in one turn when use `AlterTableAddPartitionCommand` or `RepairTableCommand` to add partitions into table. The smaller batch size is, the less memory is required for the real handler, e.g. Hive Metastore. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5616) |

### `spark.sql.allowNamedFunctionArguments.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.allowNamedFunctionArguments` | boolean | `true` | 3.5.0 | If true, Spark will turn on support for named parameters for all functions that has it implemented. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L569) |

### `spark.sql.alwaysInlineCommonExpr.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.alwaysInlineCommonExpr` | boolean | `false` | 4.0.0 | When true, always inline common expressions instead of using the WITH expression. This may lead to duplicated expressions and the config should only be enabled if you hit bugs caused by the WITH expression. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4619) |

### `spark.sql.analyzer.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.analyzer.allowSubqueryExpressionsInLambdasOrHigherOrderFunctions` | boolean | `false` | 4.0.0 | When set to false, the analyzer will throw an error if a subquery expression appears in a lambda function or higher-order function. When set to true, it restores the legacy behavior of allowing subquery eexpressions in lambda functions or higher-order functions. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L6300) |
| `spark.sql.analyzer.canonicalization.multiCommutativeOpMemoryOptThreshold` | int | `3` | 3.4.0 | The minimum number of operands in a commutative expression tree to invoke the MultiCommutativeOp memory optimization during canonicalization. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L457) |
| `spark.sql.analyzer.dontDeduplicateExpressionIfExprIdInOutput` | boolean | `true` | 4.1.0 | DeduplicateRelations shouldn't remap expressions to new ExprIds if old ExprId still exists in output. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L255) |
| `spark.sql.analyzer.failAmbiguousSelfJoin` | boolean | `true` | 3.0.0 | When true, fail the Dataset query if it contains ambiguous self-join. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2277) |
| `spark.sql.analyzer.maxIterations` | int | `100` | 3.0.0 | The max number of iterations the analyzer runs. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L292) |
| `spark.sql.analyzer.preferColumnOverLcaInArrayIndex` | boolean | `true` | 4.1.0 | When true, prefer the column from the underlying relation over the lateral column alias reference with the same name (see SPARK-53734). | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L245) |
| `spark.sql.analyzer.scalarSubqueryAllowGroupByColumnEqualToConstant` | boolean | `true` | 4.0.0 | When set to true, allow scalar subqueries with group-by on a column that also has an equality filter with a constant (SPARK-48557). | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L6282) |
| `spark.sql.analyzer.singlePassResolver.dualRunSampleRate` | double | `if (Utils.isTesting) 1.0 else 0.001` | 4.1.0 | Represents the rate of queries that will be run in both fixed-point and single-pass mode (dual run). It should be taken into account that the sample rate is not a strict percentage (in tests we don't sample). It is determined whether query should be run in dual run mode by comparing a random value with the value of this flag. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L359) |
| `spark.sql.analyzer.singlePassResolver.dualRunWithLegacy` | boolean | `false` | 4.0.0 | When true, run both analyzers to check if single-pass Analyzer correctly produces the same analyzed plan as the fixed-point Analyzer for the existing set of features defined in the ResolverGuard | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L335) |
| `spark.sql.analyzer.singlePassResolver.enabled` | boolean | `false` | 4.0.0 | When true, use the single-pass Resolver instead of the fixed-point Analyzer. This is an alternative Analyzer framework, which resolves the parsed logical plan in a single post-order traversal. It uses ExpressionResolver to resolve expressions and NameScope to control the visibility of names. In contrast to the current fixed-point framework, subsequent in-tree traversals are disallowed. Most of the fixed-point Analyzer code is reused in the form of specific node transformation functions (AliasResolution.resolve, FunctionResolution.resolveFunction, etc). This feature is currently under development. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L300) |
| `spark.sql.analyzer.singlePassResolver.enabledTentatively` | boolean | `false` | 4.1.0 | When true, use the single-pass Resolver instead of the fixed-point Analyzer only if a SQL query or a DataFrame program is fully supported by the single-pass Analyzer. This is an alternative Analyzer framework, which resolves the parsed logical plan in a single post-order traversal. It uses ExpressionResolver to resolve expressions and NameScope to control the visibility of names. In contrast to the current fixed-point framework, subsequent in-tree traversals are disallowed. Most of the fixed-point Analyzer code is reused in the form of specific node transformation functions (AliasResolution.resolve, FunctionResolution.resolveFunction, etc).This feature is currently under development. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L317) |
| `spark.sql.analyzer.singlePassResolver.exposeResolverGuardFailure` | boolean | `Utils.isTesting` | 4.1.0 | When true, any failure thrown from ResolverGuard will be exposed as a query failure. Otherwise we just assume that the ResolverGuard returned false and the query is not supported by the single-pass Analyzer. This is important to make dual-runs unnoticeable in production. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L372) |
| `spark.sql.analyzer.singlePassResolver.preventUsingAliasesFromNonDirectChildren` | boolean | `false` | 4.1.0 | When true, in Sort/Having/Filter expressions allow replacing of these expressions only with semantically equal aliased expressions from direct children. This is necessary in order to stay compatible with fixed-point, but the functionality and correctness remain the same. Because enabling this case would break some cases that are supported in single-pass but not in fixed-point, this flag should only be used to hide false positive logical plan mismatches during testing. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L384) |
| `spark.sql.analyzer.singlePassResolver.relationBridging.enabled` | boolean | `true` | 4.0.0 | When set to true, the single-pass Resolver will reuse the relation metadata that was previously resolved in fixed-point run. This makes sense only in ANALYZER_DUAL_RUN_LEGACY_AND_SINGLE_PASS_RESOLVER mode. In that case HybridAnalyzer enables the AnalyzerBridgeState and passes it to the single-pass Analyzer after the fixed-point run is complete. Single-pass Resolver uses this AnalyzerBridgeState to construct a special RelationMetadataProvider implementation - BridgedRelationMetadataProvider. This component simply reuses cached relation metadata and avoids any blocking calls (catalog RPCs or table metadata reads). | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L410) |
| `spark.sql.analyzer.singlePassResolver.returnSinglePassResultInDualRun` | boolean | `Utils.isTesting` | 4.0.0 | When true, return the result of the single-pass resolver as the result of the dual run analysis (which is used if the ANALYZER_DUAL_RUN_LEGACY_AND_SINGLE_PASS_RESOLVER flag value is true). Otherwise, return the result of the fixed-point analyzer. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L347) |
| `spark.sql.analyzer.singlePassResolver.runExtendedResolutionChecks` | boolean | `true` | 4.1.0 | When true, run `extendedResolutionChecks` after the main analysis. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L437) |
| `spark.sql.analyzer.singlePassResolver.runHeavyExtendedResolutionChecks` | boolean | `true` | 4.1.0 | When true, run heavy `extendedResolutionChecks` after the main analysis. Otherwise skip them. Heavy check either involves a network call changing external persistent storage, or changes a global state. For example, `ViewSyncSchemaToMetaStore` calls alter table. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L445) |
| `spark.sql.analyzer.singlePassResolver.throwFromResolverGuard` | boolean | `false` | 4.1.0 | When set to true, ResolverGuard will throw a descriptive error on unsupported features. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L427) |
| `spark.sql.analyzer.singlePassResolver.validationEnabled` | boolean | `true` | 4.0.0 | When true, validate the Resolver output with ResolutionValidator. The ResolutionValidator validates the resolved logical plan tree in one pass and asserts the internal contracts. It uses the ExpressionResolutionValidator internally to validate resolved expression trees in the same manner. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L397) |
| `spark.sql.analyzer.sqlFunctionResolution.applyConfOverrides` | boolean | `true` | 4.0.1 | When true, applies the conf overrides for certain feature flags during the resolution of user-defined sql table valued functions, consistent with view resolution. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2285) |
| `spark.sql.analyzer.unionIsResolvedWhenDuplicatesPerChildResolved` | boolean | `true` | 4.1.0 | When true, union should only be resolved once there are no duplicate attributes in each branch. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L265) |
| `spark.sql.analyzer.uniqueNecessaryMetadataColumns` | boolean | `true` | 4.1.0 | When this conf is enabled, AddMetadataColumns rule should only add necessary metadata columns and only if those columns are not already present in the project list. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L275) |

### `spark.sql.ansi.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.ansi.doubleQuotedIdentifiers` | boolean | `false` | 3.4.0 | When true and '${ANSI_ENABLED.key}' is true, Spark SQL reads literals enclosed in double quoted (") as identifiers. When false they are read as string literals. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4548) |
| `spark.sql.ansi.enabled` | boolean | `!sys.env.get("SPARK_ANSI_SQL_MODE").contains("false")` | 3.0.0 | When true, Spark SQL uses an ANSI compliant dialect instead of being Hive compliant. For example, Spark will throw an exception at runtime instead of returning null results when the inputs to a SQL operator/function are invalid. For full details of this dialect, you can find them in the section "ANSI Compliance" of Spark's documentation. Some ANSI dialect features may be not from the ANSI SQL standard directly, but their behaviors align with ANSI SQL's style | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4529) |
| `spark.sql.ansi.enforceReservedKeywords` | boolean | `false` | 3.3.0 | When true and '${ANSI_ENABLED.key}' is true, the Spark SQL parser enforces the ANSI reserved keywords and forbids SQL queries that use reserved keywords as alias names and/or identifiers for table, view, function, etc. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4540) |
| `spark.sql.ansi.relationPrecedence` | boolean | `false` | 3.4.0 | When true and '${ANSI_ENABLED.key}' is true, JOIN takes precedence over comma when combining relation. For example, `t1, t2 JOIN t3` should result to `t1 X (t2 X t3)`. If the config is false, the result is `(t1 X t2) X t3`. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4565) |

### `spark.sql.artifact.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.artifact.copyFromLocalToFs.allowDestLocal` | boolean | _(optional)_ | 4.0.0 | \|Allow `spark.copyFromLocalToFs` destination to be local file system \| path on spark driver node when \|`spark.sql.artifact.copyFromLocalToFs.allowDestLocal` is true. \|This will allow user to overwrite arbitrary file on spark \|driver node we should only enable it for testing purpose. \| | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L6323) |
| `spark.sql.artifact.isolation.alwaysApplyClassloader` | boolean | `false` | 4.0.0 | When enabled, the classloader holding per-session artifacts will always be applied during SQL executions (useful for Spark Connect). When disabled, the classloader will be applied only when any artifact is added to the session. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4938) |
| `spark.sql.artifact.isolation.enabled` | boolean | `true` | 4.0.0 | When enabled for a Spark Session, artifacts (such as JARs, files, archives) added to this session are isolated from other sessions within the same Spark instance. When disabled for a session, artifacts added to this session are visible to other sessions that have this config disabled. This config can only be set during the creation of a Spark Session and will have no effect when changed in the middle of session usage. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4926) |

### `spark.sql.assumeAnsiFalseIfNotPersisted.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.assumeAnsiFalseIfNotPersisted.enabled` | boolean | `true` | 4.0.1 | If enabled, assume ANSI mode is false if not persisted during view or UDF creation. Otherwise use the default value. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L6633) |

### `spark.sql.autoBroadcastJoinThreshold.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.autoBroadcastJoinThreshold` | bytes | `10MB` | 1.1.0 | Configures the maximum size in bytes for a table that will be broadcast to all worker nodes when performing a join. By setting this value to -1 broadcasting can be disabled. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L839) |

### `spark.sql.avro.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.avro.compression.codec` | string | `snappy` | 2.4.0 | Compression codec used in writing of AVRO files. Supported codecs: uncompressed, deflate, snappy, bzip2, xz and zstandard. Default codec is snappy. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4959) |
| `spark.sql.avro.datetimeRebaseModeInRead` | enum | `LegacyBehaviorPolicy.CORRECTED` | 3.0.0 | When LEGACY, Spark will rebase dates/timestamps from the legacy hybrid (Julian + Gregorian) calendar to Proleptic Gregorian calendar when reading Avro files. When CORRECTED, Spark will not do rebase and read the dates/timestamps as it is. When EXCEPTION, Spark will fail the reading if it sees ancient dates/timestamps that are ambiguous between the two calendars. This config is only effective if the writer info (like Spark, Hive) of the Avro files is unknown. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5729) |
| `spark.sql.avro.datetimeRebaseModeInWrite` | enum | `LegacyBehaviorPolicy.CORRECTED` | 3.0.0 | When LEGACY, Spark will rebase dates/timestamps from Proleptic Gregorian calendar to the legacy hybrid (Julian + Gregorian) calendar when writing Avro files. When CORRECTED, Spark will not do rebase and write the dates/timestamps as it is. When EXCEPTION, Spark will fail the writing if it sees ancient dates/timestamps that are ambiguous between the two calendars. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5717) |
| `spark.sql.avro.deflate.level` | int | `CodecFactory.DEFAULT_DEFLATE_LEVEL` | 2.4.0 | Compression level for the deflate codec used in writing of AVRO files. Valid value must be in the range of from 1 to 9 inclusive or -1. The default value is -1 which corresponds to 6 level in the current implementation. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4967) |
| `spark.sql.avro.filterPushdown.enabled` | boolean | `true` | 3.1.0 | When true, enable filter pushdown to Avro datasource. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5498) |
| `spark.sql.avro.xz.level` | int | `CodecFactory.DEFAULT_XZ_LEVEL` | 4.0.0 | Compression level for the xz codec used in writing of AVRO files. Valid value must be in the range of from 1 to 9 inclusive The default value is 6. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4976) |
| `spark.sql.avro.zstandard.bufferPool.enabled` | boolean | `false` | 4.0.0 | If true, enable buffer pool of ZSTD JNI library when writing of AVRO files | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4991) |
| `spark.sql.avro.zstandard.level` | int | `CodecFactory.DEFAULT_ZSTANDARD_LEVEL` | 4.0.0 | Compression level for the zstandard codec used in writing of AVRO files. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4985) |

### `spark.sql.binaryOutputStyle.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.binaryOutputStyle` | enum | _(optional)_ | 4.0.0 | The output style used display binary data. Valid values are 'UTF-8', 'BASIC', 'BASE64', 'HEX', and 'HEX_DISCRETE'. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2005) |

### `spark.sql.broadcastTimeout.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.broadcastTimeout` | time | `${5 * 60}` | 1.3.0 | Timeout in seconds for the broadcast wait time in broadcast joins. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1846) |

### `spark.sql.bucketing.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.bucketing.coalesceBucketsInJoin.enabled` | boolean | `false` | 3.1.0 | When true, if two bucketed tables with the different number of buckets are joined, the side with a bigger number of buckets will be coalesced to have the same number of buckets as the other side. Bigger number of buckets is divisible by the smaller number of buckets. Bucket coalescing is applied to sort-merge joins and shuffled hash join. Note: Coalescing bucketed table can avoid unnecessary shuffling in join, but it also reduces parallelism and could possibly cause OOM for shuffled hash join. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5751) |
| `spark.sql.bucketing.coalesceBucketsInJoin.maxBucketRatio` | int | `4` | 3.1.0 | The ratio of the number of two buckets being coalesced should be less than or equal to this value for bucket coalescing to be applied. This configuration only has an effect when '${COALESCE_BUCKETS_IN_JOIN_ENABLED.key}' is set to true. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5764) |

### `spark.sql.cartesianProductExec.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.cartesianProductExec.buffer.in.memory.threshold` | int | `4096` | 2.2.1 | Threshold for number of rows guaranteed to be held in memory by the cartesian product operator | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3799) |
| `spark.sql.cartesianProductExec.buffer.spill.size.threshold` | fallback | → `SHUFFLE_SPILL_MAX_SIZE_FORCE_SPILL_THRESHOLD` | 4.1.0 | Threshold for size of rows to be spilled by cartesian product operator | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3816) |
| `spark.sql.cartesianProductExec.buffer.spill.threshold` | int | `SHUFFLE_SPILL_NUM_ELEMENTS_FORCE_SPILL_THRESHOLD.defaultValue.get` | 2.2.0 | Threshold for number of rows to be spilled by cartesian product operator | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3808) |

### `spark.sql.caseSensitive.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.caseSensitive` | boolean | `false` | 1.4.0 | Whether the query analyzer should be case sensitive or not. Default to case insensitive. It is highly discouraged to turn on case sensitive mode. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1260) |

### `spark.sql.cbo.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.cbo.enabled` | boolean | `false` | 2.2.0 | Enables CBO for estimation of plan statistics when set true. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3603) |
| `spark.sql.cbo.joinReorder.card.weight` | double | `0.7` | 2.2.0 | The weight of the ratio of cardinalities (number of rows) in the cost comparison function. The ratio of sizes in bytes has weight 1 - this value. The weighted geometric mean of these ratios is used to decide which of the candidate plans will be chosen by the CBO. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3632) |
| `spark.sql.cbo.joinReorder.dp.star.filter` | boolean | `false` | 2.2.0 | Applies star-join filter heuristics to cost based join enumeration. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3644) |
| `spark.sql.cbo.joinReorder.dp.threshold` | int | `12` | 2.2.0 | The maximum number of joined nodes allowed in the dynamic programming algorithm. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3624) |
| `spark.sql.cbo.joinReorder.enabled` | boolean | `false` | 2.2.0 | Enables join reorder in CBO. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3617) |
| `spark.sql.cbo.planStats.enabled` | boolean | `false` | 3.0.0 | When true, the logical plan will fetch row counts and column statistics from catalog. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3610) |
| `spark.sql.cbo.starJoinFTRatio` | double | `0.9` | 2.2.0 | Specifies the upper limit of the ratio between the largest fact tables for a star join to be considered. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3656) |
| `spark.sql.cbo.starSchemaDetection` | boolean | `false` | 2.2.0 | When true, it enables join reordering based on star schema detection. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3650) |

### `spark.sql.charAsVarchar.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.charAsVarchar` | boolean | `false` | 3.3.0 | When true, Spark replaces CHAR type with VARCHAR type in CREATE/REPLACE/ALTER TABLE commands, so that newly created/updated tables will not have CHAR type columns/fields. Existing tables with CHAR type columns/fields are not affected by this config. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5858) |

### `spark.sql.chunkBase64String.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.chunkBase64String.enabled` | boolean | `true` | 3.5.2 | Whether to truncate string generated by the `Base64` function. When true, base64 strings generated by the base64 function are chunked into lines of at most 76 characters. When false, the base64 strings are not chunked. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4573) |

### `spark.sql.classic.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.classic.shuffleDependency.fileCleanup.enabled` | boolean | `Utils.isTesting` | 4.1.0 | When enabled, shuffle files will be cleaned up at the end of classic SQL executions. Note that this cleanup may cause stage retries and regenerate shuffle files if the same dataframe reference is executed again. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3759) |

### `spark.sql.cli.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.cli.print.header` | boolean | `false` | 3.2.0 | When set to true, spark-sql CLI prints the names of the columns in query output. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5901) |

### `spark.sql.codegen.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.codegen.aggregate.fastHashMap.capacityBit` | int | `16` | 2.4.0 | Capacity for the max number of rows to be held in memory by the fast hash aggregate product operator. The bit is not for actual value, but the actual numBuckets is determined by loadFactor (e.g: default bit value 16 , the actual numBuckets is ((1 << 16) / 0.5). | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4948) |
| `spark.sql.codegen.aggregate.map.twolevel.enabled` | boolean | `true` | 2.3.0 | Enable two-level aggregate hash map. When enabled, records will first be inserted/looked-up at a 1st-level, small, fast map, and then fallback to a 2nd-level, larger, slower map when 1st level is full or keys cannot be found. When disabled, records go directly to the 2nd level. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3105) |
| `spark.sql.codegen.aggregate.map.twolevel.partialOnly` | boolean | `true` | 3.2.1 | Enable two-level aggregate hash map for partial aggregate only, because final aggregate might get more distinct keys compared to partial aggregate. Overhead of looking up 1st-level map might dominate when having a lot of distinct keys. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3116) |
| `spark.sql.codegen.aggregate.map.vectorized.enable` | boolean | `false` | 3.0.0 | Enable vectorized aggregate hash map. This is for testing/benchmarking only. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3126) |
| `spark.sql.codegen.aggregate.sortAggregate.enabled` | boolean | `true` | 3.3.0 | When true, enable code-gen for sort aggregate. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3144) |
| `spark.sql.codegen.aggregate.splitAggregateFunc.enabled` | boolean | `true` | 3.0.0 | When true, the code generator would split aggregate code into individual methods instead of a single big method. This can be used to avoid oversized function that can miss the opportunity of JIT optimization. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3134) |
| `spark.sql.codegen.broadcastCleanedSourceThreshold` | int | `-1` | 4.0.0 | A threshold (in string length) to determine if we should make the generated code a broadcast variable in whole stage codegen. To disable this, set the threshold to < 0; otherwise if the size is above the threshold, it'll use broadcast variable. Note that maximum string length allowed in Java is Integer.MAX_VALUE, so anything above it would be meaningless. The default value is set to -1 (disabled by default). | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2419) |
| `spark.sql.codegen.factoryMode` | enum | `CodegenObjectFactoryMode.FALLBACK` | 2.4.0 | This config determines the fallback behavior of several codegen generators during tests. `FALLBACK` means trying codegen first and then falling back to interpreted if any compile error happens. Disabling fallback if `CODEGEN_ONLY`. `NO_CODEGEN` skips codegen and goes interpreted path always. Note that this configuration is only for the internal usage, and NOT supposed to be set by end users. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2346) |
| `spark.sql.codegen.fallback` | boolean | `true` | 2.0.0 | When true, (whole stage) codegen could be temporary disabled for the part of query that fail to compile generated code | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2358) |
| `spark.sql.codegen.hugeMethodLimit` | int | `65535` | 2.3.0 | The maximum bytecode size of a single compiled Java function generated by whole-stage codegen. When the compiled function exceeds this threshold, the whole-stage codegen is deactivated for this subtree of the current query plan. The default value is 65535, which is the largest bytecode size possible for a valid Java method. When running on HotSpot, it may be preferable to set the value to ${CodeGenerator.DEFAULT_JVM_HUGE_METHOD_LIMIT} to match HotSpot's implementation. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2383) |
| `spark.sql.codegen.join.buildSideOuterShuffledHashJoin.enabled` | boolean | `true` | 3.5.0 | When true, enable code-gen for an OUTER shuffled hash join where outer side is the build side. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3160) |
| `spark.sql.codegen.join.existenceSortMergeJoin.enabled` | boolean | `true` | 3.3.0 | When true, enable code-gen for Existence sort merge join. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3177) |
| `spark.sql.codegen.join.fullOuterShuffledHashJoin.enabled` | boolean | `true` | 3.3.0 | When true, enable code-gen for FULL OUTER shuffled hash join. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3152) |
| `spark.sql.codegen.join.fullOuterSortMergeJoin.enabled` | boolean | `true` | 3.3.0 | When true, enable code-gen for FULL OUTER sort merge join. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3169) |
| `spark.sql.codegen.logLevel` | enum | `Level.DEBUG` | 4.1.0 | Configures the log level for logging of codegen. The value can be ${VALID_LOG_LEVELS.mkString()}. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2366) |
| `spark.sql.codegen.logging.maxLines` | int | `1000` | 2.3.0 | The maximum number of codegen lines to log when errors occur. Use -1 for unlimited. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2374) |
| `spark.sql.codegen.maxFields` | int | `100` | 2.0.0 | The maximum number of fields (including nested fields) that will be supported before deactivating whole-stage codegen. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2338) |
| `spark.sql.codegen.methodSplitThreshold` | int | `1024` | 3.0.0 | The threshold of source-code splitting in the codegen. When the number of characters in a single Java function (without comment) exceeds the threshold, the function will be automatically split to multiple smaller ones. We cannot know how many bytecode will be generated, so use the code length as metric. When running on HotSpot, a function's bytecode should not go beyond 8KB, otherwise it will not be JITted; it also should not be too small, otherwise there will be many function calls. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2395) |
| `spark.sql.codegen.splitConsumeFuncByOperator` | boolean | `true` | 2.3.1 | When true, whole stage codegen would put the logic of consuming rows of each physical operator into individual methods, instead of a single big method. This can be used to avoid oversized function that can miss the opportunity of JIT optimization. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2409) |
| `spark.sql.codegen.useIdInClassName` | boolean | `true` | 2.3.1 | When true, embed the (whole-stage) codegen stage ID into the class name of the generated class as a suffix | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2330) |
| `spark.sql.codegen.wholeStage` | boolean | `true` | 2.0.0 | When true, the whole stage (of multiple operators) will be compiled into single java method. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2321) |

### `spark.sql.collation.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.collation.allowInMapKeys` | boolean | `false` | 4.0.0 | Allow for non-UTF8_BINARY collated strings inside of map's keys | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1037) |
| `spark.sql.collation.objectLevel.enabled` | boolean | `true` | 4.0.0 | Object level collations feature is under development and its use should be done under this feature flag. The feature allows setting default collation for all underlying columns within that object, except the ones that were previously created. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1044) |
| `spark.sql.collation.schemaLevel.enabled` | boolean | `false` | 4.1.0 | Schema level collations feature is under development and its use should be done under this feature flag. The feature allows setting default collation for all underlying objects within that schema, except the ones that were previously created.An object with an explicitly set collation will not inherit the collation from the schema. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1056) |
| `spark.sql.collation.trim.enabled` | boolean | `true` | 4.0.0 | When enabled allows the use of trim collations which trim trailing whitespaces from strings. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1070) |

### `spark.sql.columnNameOfCorruptRecord.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.columnNameOfCorruptRecord` | string | `_corrupt_record` | 1.2.0 | The name of internal column for storing raw/un-parsed JSON and CSV records that fail to parse. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1839) |

### `spark.sql.columnVector.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.columnVector.offheap.enabled` | fallback | → `MEMORY_OFFHEAP_ENABLED` | 2.3.0 | When true, use OffHeapColumnVector in ColumnarBatch. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L784) |

### `spark.sql.connect.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.connect.shuffleDependency.fileCleanup.enabled` | fallback | → `SHUFFLE_DEPENDENCY_FILE_CLEANUP_ENABLED` | 4.1.0 | When enabled, shuffle files will be cleaned up at the end of Spark Connect SQL executions. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3768) |

### `spark.sql.constraintPropagation.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.constraintPropagation.enabled` | boolean | `true` | 2.2.0 | When true, the query optimizer will infer and propagate data constraints in the query plan to optimize them. Constraint propagation can sometimes be computationally expensive for certain kinds of query plans (such as those with a large number of predicates and aliases) which might negatively impact overall runtime. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1268) |

### `spark.sql.crossJoin.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.crossJoin.enabled` | boolean | `true` | 2.0.0 | When false, we will throw an error if a query contains a cartesian product without explicit CROSS JOIN syntax. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2161) |

### `spark.sql.csv.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.csv.filterPushdown.enabled` | boolean | `true` | 3.0.0 | When true, enable filter pushdown to CSV datasource. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5486) |
| `spark.sql.csv.parser.columnPruning.enabled` | boolean | `true` | 2.4.0 | If it is set to true, column names of the requested schema are passed to CSV parser. Other column values can be ignored during parsing even if they are malformed. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4869) |
| `spark.sql.csv.parser.inputBufferSize` | int | _(optional)_ | 3.0.3 | If it is set, it configures the buffer size of CSV input during parsing. It is the same as inputBufferSize option in CSV which has a higher priority. Note that this is a workaround for the parsing library's regression, and this configuration is internal and supposed to be removed in the near future. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4877) |

### `spark.sql.cteRecursionAnchorRowsLimitToConvertToLocalRelation.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.cteRecursionAnchorRowsLimitToConvertToLocalRelation` | int | `100` | 4.1.0 | Maximum number of rows that the anchor in a recursive CTE can return for it to beconverted to a localRelation. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5316) |

### `spark.sql.cteRecursionLevelLimit.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.cteRecursionLevelLimit` | int | `100` | 4.1.0 | Maximum level of recursion that is allowed while executing a recursive CTE definition.If a query does not get exhausted before reaching this limit it fails. Use -1 for unlimited. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5297) |

### `spark.sql.cteRecursionRowLimit.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.cteRecursionRowLimit` | int | `1000000` | 4.1.0 | Maximum number of rows that can be returned when executing a recursive CTE definition.If a query does not get exhausted before reaching this limit it fails. Use -1 for unlimited. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5306) |

### `spark.sql.cteRelationDefMaxRows.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.cteRelationDefMaxRows.enabled` | boolean | `true` | 4.1.0 | When set to true, CTERelationDef.maxRows would output the correct value from the child plan. This is necessary for correct scalar subquery validation in the single-pass Analyzer. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L6457) |

### `spark.sql.dataFrameQueryContext.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.dataFrameQueryContext.enabled` | boolean | `true` | 4.0.0 | Enable the DataFrame query context. This feature is enabled by default, but has a non-trivial performance overhead because of the stack trace collection. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L6353) |

### `spark.sql.dataSource.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.dataSource.alwaysCreateV2Predicate` | boolean | `true` | 4.1.0 | When true, the v2 push-down framework always wraps the expression that returns boolean type with a v2 Predicate so that it can be pushed down. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1921) |
| `spark.sql.dataSource.skipAssertOnPredicatePushdown` | boolean | `!Utils.isTesting` | 4.0.0 | Enable skipping assert when expression in not translated to predicate. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1913) |

### `spark.sql.dataframeCache.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.dataframeCache.logLevel` | enum | `Level.TRACE` | 4.0.0 | Configures the log level of Dataframe cache operations, including adding and removing entries from Dataframe cache, hit and miss on cache application. This log should only be used for debugging purposes and not in the production environment, since it generates a large amount of logs. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2151) |

### `spark.sql.datetime.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.datetime.java8API.enabled` | boolean | `false` | 3.0.0 | If the configuration property is set to true, java.time.Instant and java.time.LocalDate classes of Java 8 API are used as external types for Catalyst's TimestampType and DateType. If it is set to false, java.sql.Timestamp and java.sql.Date are used for the same purpose. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5204) |

### `spark.sql.debug.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.debug.maxToStringFields` | int | `25` | 3.0.0 | Maximum number of fields of sequence-like entries can be converted to strings in debug output. Any elements beyond the limit will be dropped and replaced by a placeholder. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5150) |

### `spark.sql.decimalOperations.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.decimalOperations.allowPrecisionLoss` | boolean | `true` | 2.3.1 | When true (default), establishing the result type of an arithmetic operation happens according to Hive behavior and SQL ANSI 2011 specification, i.e. rounding the decimal part of the result if an exact representation is not possible. Otherwise, NULL is returned in those cases, as previously. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4317) |

### `spark.sql.defaultCacheStorageLevel.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.defaultCacheStorageLevel` | enum | `StorageLevelMapper.MEMORY_AND_DISK` | 4.0.0 | The default storage level of `dataset.cache()`, `catalog.cacheTable()` and sql query `CACHE TABLE t`. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2144) |

### `spark.sql.defaultCatalog.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.defaultCatalog` | string | `SESSION_CATALOG_NAME` | 3.0.0 | Name of the default catalog. This will be the current catalog if users have not explicitly set the current catalog yet. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5242) |

### `spark.sql.defaultColumn.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.defaultColumn.allowedProviders` | string | `csv,json,orc,parquet` | 3.4.0 | List of table providers wherein SQL commands are permitted to assign DEFAULT column values. Comma-separated list, whitespace ignored, case-insensitive. If an asterisk appears after any table provider in this list, any command may assign DEFAULT column except `ALTER TABLE ... ADD COLUMN`. Otherwise, if no asterisk appears, all commands are permitted. This is useful because in order for such `ALTER TABLE ... ADD COLUMN` commands to work, the target data source must include support for substituting in the provided values when the corresponding fields are not present in storage. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4593) |
| `spark.sql.defaultColumn.enabled` | boolean | `true` | 3.4.0 | When true, allow CREATE TABLE, REPLACE TABLE, and ALTER COLUMN statements to set or update default values for specific columns. Following INSERT, MERGE, and UPDATE statements may then omit these values and their values will be injected automatically instead. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4582) |
| `spark.sql.defaultColumn.useNullsForMissingDefaultValues` | boolean | `true` | 3.4.0 | When true, and DEFAULT columns are enabled, allow INSERT INTO commands with user-specified lists of fewer columns than the target table to behave as if they had specified DEFAULT for all remaining columns instead, in order. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4640) |

### `spark.sql.defaultSizeInBytes.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.defaultSizeInBytes` | bytes | `Long.MaxValue` | 1.1.0 | The default table size used in query planning. By default, it is set to Long.MaxValue which is larger than `${AUTO_BROADCASTJOIN_THRESHOLD.key}` to be more conservative. That is to say by default the optimizer will not choose to broadcast a table unless it knows for sure its size is small enough. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3523) |

### `spark.sql.enforceTypeCoercionBeforeUnionDeduplication.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.enforceTypeCoercionBeforeUnionDeduplication.enabled` | boolean | `true` | 4.1.0 | When set to true, we enforce type coercion to run before deduplication of UNION children outputs. Otherwise, order is relative to rule ordering. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L6583) |

### `spark.sql.error.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.error.messageFormat` | enum | `ErrorMessageFormat.PRETTY` | 3.4.0 | When PRETTY, the error message consists of textual representation of error class, message and query context. The MINIMAL and STANDARD formats are pretty JSON formats where STANDARD includes an additional JSON field `message`. This configuration property influences on error messages of Thrift Server and SQL CLI while running queries. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L6074) |

### `spark.sql.exchange.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.exchange.reuse` | boolean | `true` | 2.0.0 | When true, the planner will try to find out duplicated exchanges and re-use them. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2502) |

### `spark.sql.execution.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.execution.arrow.compression.codec` | string | `none` | 4.1.0 | Compression codec used to compress Arrow IPC data when transferring data between JVM and Python processes (e.g., toPandas, toArrow). This can significantly reduce memory usage and network bandwidth when transferring large datasets. Supported codecs: 'none' (no compression), 'zstd' (Zstandard), 'lz4' (LZ4). Note that compression may add CPU overhead but can provide substantial memory savings especially for datasets with high compression ratios. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4070) |
| `spark.sql.execution.arrow.compression.zstd.level` | int | `3` | 4.1.0 | Compression level for Zstandard (zstd) codec when compressing Arrow IPC data. This config is only used when spark.sql.execution.arrow.compression.codec is set to 'zstd'. Negative values provide ultra-fast compression with lower compression ratios. Positive values provide normal to maximum compression, with higher values giving better compression but slower speed. The default value 3 provides a good balance between compression speed and compression ratio. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4084) |
| `spark.sql.execution.arrow.enabled` | boolean | `false` | 2.3.0 | (Deprecated since Spark 3.0, please set 'spark.sql.execution.arrow.pyspark.enabled'.) | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3847) |
| `spark.sql.execution.arrow.fallback.enabled` | boolean | `true` | 2.4.0 | (Deprecated since Spark 3.0, please set 'spark.sql.execution.arrow.pyspark.fallback.enabled'.) | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3996) |
| `spark.sql.execution.arrow.localRelationThreshold` | bytes | `48MB` | 3.4.0 | When converting Arrow batches to Spark DataFrame, local collections are used in the driver side if the byte size of Arrow batches is smaller than this threshold. Otherwise, the Arrow batches are sent and deserialized to Spark internal rows in the executors. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3890) |
| `spark.sql.execution.arrow.maxBytesPerBatch` | bytes | `64MB` | 4.0.0 | When using Apache Arrow, limit the maximum bytes in each batch that can be written to a single ArrowRecordBatch in memory. Unlike 'spark.sql.execution.arrow.maxRecordsPerBatch', this configuration does not work for createDataFrame/toPandas with Arrow/pandas instances. See also spark.sql.execution.arrow.maxRecordsPerBatch. If both are set, each batch is created when any condition of both is met. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4053) |
| `spark.sql.execution.arrow.maxBytesPerOutputBatch` | bytes | `-1` | 4.1.0 | When using Apache Arrow, limit the maximum bytes that can be output in a single ArrowRecordBatch to the downstream operator. If set to zero or negative there is no limit. Note that the complete ArrowRecordBatch is actually created but the number of bytes is limited when sending it to the downstream operator. This is used to avoid large batches being sent to the downstream operator including the columnar-based operator implemented by third-party libraries. Spark will try to create batches with the size equal or less than this value. Normally it should not happen, but if in extreme case that even one record is still larger than this value, Spark will create a batch with one record. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4034) |
| `spark.sql.execution.arrow.maxRecordsPerBatch` | int | `10000` | 2.3.0 | When using Apache Arrow, limit the maximum number of records that can be written to a single ArrowRecordBatch in memory. If set to zero or negative there is no limit. See also spark.sql.execution.arrow.maxBytesPerBatch. If both are set, each batch is created when any condition of both is met. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4011) |
| `spark.sql.execution.arrow.maxRecordsPerOutputBatch` | int | `-1` | 4.1.0 | When using Apache Arrow, limit the maximum number of records that can be output in a single ArrowRecordBatch to the downstream operator. If set to zero or negative there is no limit. Note that the complete ArrowRecordBatch is actually created but the number of records is limited when sending it to the downstream operator. This is used to avoid large batches being sent to the downstream operator including the columnar-based operator implemented by third-party libraries. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4021) |
| `spark.sql.execution.arrow.pyspark.enabled` | fallback | → `ARROW_EXECUTION_ENABLED` | 3.0.0 | When true, make use of Apache Arrow for columnar data transfers in PySpark. This optimization applies to: 1. pyspark.sql.DataFrame.toPandas. 2. pyspark.sql.SparkSession.createDataFrame when its input is a Pandas DataFrame or a NumPy ndarray. The following data type is unsupported: ArrayType of TimestampType. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3854) |
| `spark.sql.execution.arrow.pyspark.fallback.enabled` | fallback | → `ARROW_FALLBACK_ENABLED` | 3.0.0 | When true, optimizations enabled by '${ARROW_PYSPARK_EXECUTION_ENABLED.key}' will fallback automatically to non-optimized implementations if an error occurs. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4004) |
| `spark.sql.execution.arrow.pyspark.selfDestruct.enabled` | boolean | `false` | 3.2.0 | (Experimental) When true, make use of Apache Arrow's self-destruct and split-blocks options for columnar data transfers in PySpark, when converting from Arrow to Pandas. This reduces memory usage at the cost of some CPU time. This optimization applies to: pyspark.sql.DataFrame.toPandas when 'spark.sql.execution.arrow.pyspark.enabled' is set. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3866) |
| `spark.sql.execution.arrow.pyspark.validateSchema.enabled` | boolean | `false` | 4.1.0 | When true, validate the schema of Arrow batches returned by mapInArrow, mapInPandas and DataSource against the expected schema to ensure that they are compatible. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4183) |
| `spark.sql.execution.arrow.sparkr.enabled` | boolean | `false` | 3.0.0 | When true, make use of Apache Arrow for columnar data transfers in SparkR. This optimization applies to: 1. createDataFrame when its input is an R DataFrame 2. collect 3. dapply 4. gapply The following data types are unsupported: FloatType, BinaryType, ArrayType, StructType and MapType. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3982) |
| `spark.sql.execution.arrow.transformWithStateInPySpark.maxStateRecordsPerBatch` | int | `10000` | 4.0.0 | When using TransformWithState in PySpark (both Python Row and Pandas), limit the maximum number of state records that can be written to a single ArrowRecordBatch in memory. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4096) |
| `spark.sql.execution.arrow.useLargeVarTypes` | boolean | `false` | 3.5.0 | When using Apache Arrow, use large variable width vectors for string and binary types. Regular string and binary types have a 2GiB limit for a column in a single record batch. Large variable types remove this limitation at the cost of higher memory usage per value. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4108) |
| `spark.sql.execution.broadcastHashJoin.outputPartitioningExpandLimit` | int | `8` | 3.1.0 | The maximum number of partitionings that a HashPartitioning can be expanded to. This configuration is applicable only for BroadcastHashJoin inner joins and can be set to '0' to disable this feature. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5774) |
| `spark.sql.execution.datasources.hadoopLineRecordReader.enabled` | boolean | `true` | 4.1.0 | Enable the imported Hadoop's LineRecordReader. This was imported and renamed to HadoopLineRecordReader to add support for compression option and other future codecs like ZSTD, etc. Setting the conf to false will use the LineRecordReader class from the hadoop jar instead of the imported one. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L6572) |
| `spark.sql.execution.fastFailOnFileFormatOutput` | boolean | `false` | 3.0.2 | Whether to fast fail task execution when writing output to FileFormat datasource. If this is enabled, in `FileFormatWriter` we will catch `FileAlreadyExistsException` and fast fail output task without further task retry. Only enabling this if you know the `FileAlreadyExistsException` of the output task is unrecoverable, i.e., further task attempts won't be able to success. If the `FileAlreadyExistsException` might be recoverable, you should keep this as disabled and let Spark to retry output tasks. This is disabled by default. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4472) |
| `spark.sql.execution.interruptOnCancel` | boolean | `true` | 4.0.0 | When true, all running tasks will be interrupted if one cancels a query. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1858) |
| `spark.sql.execution.pandas.convertToArrowArraySafely` | boolean | `true` | 3.0.0 | When true, Arrow will perform safe type conversion when converting Pandas.Series to Arrow array during serialization. Arrow will raise errors when detecting unsafe type conversion like overflow. When false, disabling Arrow's type check and do type conversions anyway. This config only works for Arrow 0.11.0+. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4282) |
| `spark.sql.execution.pandas.inferPandasDictAsMap` | boolean | `false` | 4.0.0 | When true, spark.createDataFrame will infer dict from Pandas DataFrame as a MapType. When false, spark.createDataFrame infers dict from Pandas DataFrame as a StructType which is default inferring from PyArrow. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5949) |
| `spark.sql.execution.pandas.structHandlingMode` | string | `legacy` | 3.5.0 | The conversion mode of struct type when creating pandas DataFrame. When "legacy", 1. when Arrow optimization is disabled, convert to Row object, 2. when Arrow optimization is enabled, convert to dict or raise an Exception if there are duplicated nested field names. When "row", convert to Row object regardless of Arrow optimization. When "dict", convert to dict and use suffixed key names, e.g., a_0, a_1, if there are duplicated nested field names, regardless of Arrow optimization. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4146) |
| `spark.sql.execution.pandas.udf.buffer.size` | fallback | → `BUFFER_SIZE` | 3.0.0 | Same as `${BUFFER_SIZE.key}` but only applies to Pandas UDF executions. If it is not set, the fallback is `${BUFFER_SIZE.key}`. Note that Pandas execution requires more than 4 bytes. Lowering this value could make small Pandas UDF batch iterated and pipelined; however, it might degrade performance. See SPARK-27870. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4136) |
| `spark.sql.execution.pyspark.binaryAsBytes` | boolean | `true` | 4.1.0 | When true, BinaryType is consistently mapped to bytes in PySpark. When false, restores the PySpark behavior before 4.1.0. Before 4.1.0, BinaryType is mapped to bytearray for regular UDF and UDTF without Arrow optimization, DataFrame APIs (both Spark Classic and Spark Connect), and data sources; BinaryType is mapped to bytes for Arrow-optimized UDF and UDTF with legacy pandas conversion. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3877) |
| `spark.sql.execution.pyspark.python` | string | _(optional)_ | 3.5.0 | Python binary executable to use for PySpark in executors when running Python UDF, pandas UDF and pandas function APIs. If not set, it falls back to 'spark.pyspark.python' by default. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4293) |
| `spark.sql.execution.pyspark.udf.daemonKillWorkerOnFlushFailure` | fallback | → `Python.PYTHON_DAEMON_KILL_WORKER_ON_FLUSH_FAILURE` | 4.1.0 | Same as ${Python.PYTHON_DAEMON_KILL_WORKER_ON_FLUSH_FAILURE.key} for Python execution with DataFrame and SQL. It can change during runtime. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3956) |
| `spark.sql.execution.pyspark.udf.faulthandler.enabled` | fallback | → `Python.PYTHON_WORKER_FAULTHANLDER_ENABLED` | 4.0.0 | Same as ${Python.PYTHON_WORKER_FAULTHANLDER_ENABLED.key} for Python execution with DataFrame and SQL. It can change during runtime. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3924) |
| `spark.sql.execution.pyspark.udf.hideTraceback.enabled` | boolean | `false` | 4.0.0 | When true, only show the message of the exception from Python UDFs, hiding the stack trace. If this is enabled, simplifiedTraceback has no effect. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4163) |
| `spark.sql.execution.pyspark.udf.idleTimeoutSeconds` | fallback | → `Python.PYTHON_WORKER_IDLE_TIMEOUT_SECONDS` | 4.0.0 | Same as ${Python.PYTHON_WORKER_IDLE_TIMEOUT_SECONDS.key} for Python execution with DataFrame and SQL. It can change during runtime. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3932) |
| `spark.sql.execution.pyspark.udf.killOnIdleTimeout` | fallback | → `Python.PYTHON_WORKER_KILL_ON_IDLE_TIMEOUT` | 4.1.0 | Same as ${Python.PYTHON_WORKER_KILL_ON_IDLE_TIMEOUT.key} for Python execution with DataFrame and SQL. It can change during runtime. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3940) |
| `spark.sql.execution.pyspark.udf.simplifiedTraceback.enabled` | boolean | `!Utils.isTesting` | 3.1.0 | When true, the traceback from Python UDFs is simplified. It hides the Python worker, (de)serialization, etc from PySpark in tracebacks, and only shows the exception messages from UDFs. Note that this works only with CPython 3.7+. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4172) |
| `spark.sql.execution.pyspark.udf.tracebackDumpIntervalSeconds` | fallback | → `Python.PYTHON_WORKER_TRACEBACK_DUMP_INTERVAL_SECONDS` | 4.1.0 | Same as ${Python.PYTHON_WORKER_TRACEBACK_DUMP_INTERVAL_SECONDS.key} for Python execution with DataFrame and SQL. It can change during runtime. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3948) |
| `spark.sql.execution.python.udf.buffer.size` | fallback | → `BUFFER_SIZE` | 4.0.0 | Same as `${BUFFER_SIZE.key}` but only applies to Python UDF executions. If it is not set, the fallback is `${BUFFER_SIZE.key}`. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4128) |
| `spark.sql.execution.python.udf.maxRecordsPerBatch` | int | `100` | 4.0.0 | When using Python UDFs, limit the maximum number of records that can be batched for serialization/deserialization. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4118) |
| `spark.sql.execution.pythonUDF.arrow.concurrency.level` | int | _(optional)_ | 4.0.0 | The level of concurrency to execute Arrow-optimized Python UDF. This can be useful if Python UDFs use I/O intensively. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4200) |
| `spark.sql.execution.pythonUDF.arrow.enabled` | boolean | `false` | 3.4.0 | Enable Arrow optimization in regular Python UDFs. This optimization can only be enabled when the given function takes at least one argument. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4192) |
| `spark.sql.execution.pythonUDF.arrow.legacy.fallbackOnUDT` | boolean | `false` | 4.1.0 | When true, Arrow-optimized Python UDF will fallback to the regular UDF when its input or output is UDT. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4211) |
| `spark.sql.execution.pythonUDF.pandas.intToDecimalCoercionEnabled` | boolean | `false` | 4.1.0 | When true, convert int to Decimal python objects before converting Pandas.Series to Arrow array during serialization.Disabled by default, impacts performance. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4220) |
| `spark.sql.execution.pythonUDTF.arrow.enabled` | boolean | `false` | 3.5.0 | Enable Arrow optimization for Python UDTFs. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4229) |
| `spark.sql.execution.rangeExchange.sampleSizePerPartition` | int | `100` | 2.3.0 | Number of points to sample per partition in order to determine the range boundaries for range partitioning, typically used in global sorting (without limit). | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3838) |
| `spark.sql.execution.removeRedundantProjects` | boolean | `true` | 3.1.0 | Whether to remove redundant project exec node based on children's output and ordering requirement. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2516) |
| `spark.sql.execution.removeRedundantSorts` | boolean | `true` | 2.4.8 | Whether to remove redundant physical sort node | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2524) |
| `spark.sql.execution.replaceHashWithSortAgg` | boolean | `false` | 3.3.0 | Whether to replace hash aggregate node with sort aggregate based on children's ordering | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2531) |
| `spark.sql.execution.reuseSubquery` | boolean | `true` | 3.0.0 | When true, the planner will try to find out duplicated subqueries and re-use them. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2509) |
| `spark.sql.execution.sortBeforeRepartition` | boolean | `true` | 2.1.4 | When perform a repartition following a shuffle, the output row ordering would be nondeterministic. If some downstream stages fail and some tasks of the repartition stage retry, these tasks may generate different data, and that can lead to correctness issues. Turn on this config to insert a local sort before actually doing repartition to generate consistent repartition results. The performance of repartition() may go down since we insert extra local sort before it. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4663) |
| `spark.sql.execution.topKSortFallbackThreshold` | int | `ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH` | 2.4.0 | In SQL queries with a SORT followed by a LIMIT like 'SELECT x FROM t ORDER BY y LIMIT m', if m is under this threshold, do a top-K sort in memory, otherwise do a global sort which spills to disk if necessary. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4844) |
| `spark.sql.execution.useObjectHashAggregateExec` | boolean | `true` | 2.2.0 | Decides if we use ObjectHashAggregateExec | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3309) |
| `spark.sql.execution.usePartitionEvaluator` | boolean | `false` | 3.5.0 | When true, use PartitionEvaluator to execute SQL operators. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2538) |

### `spark.sql.expressionTreeChangeLog.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.expressionTreeChangeLog.level` | enum | `Level.TRACE` | 4.0.0 | Configures the log level for logging the change from the unresolved expression tree to the resolved expression tree in the single-pass bottom-up Resolver. The value can be ${VALID_LOG_LEVELS.mkString()}. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L544) |

### `spark.sql.extendedExplainProviders.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.extendedExplainProviders` | string | _(optional)_ | 4.0.0 | A comma-separated list of classes that implement the org.apache.spark.sql.ExtendedExplainGenerator trait. If provided, Spark will print extended plan information from the providers in explain plan and in the UI | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L584) |

### `spark.sql.files.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.files.ignoreCorruptFiles` | boolean | `false` | 2.1.1 | Whether to ignore corrupt files. If true, the Spark jobs will continue to run when encountering corrupted files and the contents that have been read will still be returned. This configuration is effective only when using file-based sources such as Parquet, JSON and ORC. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2469) |
| `spark.sql.files.ignoreInvalidPartitionPaths` | boolean | `false` | 4.0.0 | Whether to ignore invalid partition paths that do not match <column>=<value>. When the option is enabled, table with two partition directories 'table/invalid' and 'table/col=1' will only load the latter directory and ignore the invalid partition | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2487) |
| `spark.sql.files.ignoreMissingFiles` | boolean | `false` | 2.3.0 | Whether to ignore missing files. If true, the Spark jobs will continue to run when encountering missing files and the contents that have been read will still be returned. This configuration is effective only when using file-based sources such as Parquet, JSON and ORC. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2478) |
| `spark.sql.files.maxPartitionBytes` | bytes | `128MB` | 2.0.0 | The maximum number of bytes to pack into a single partition when reading files. This configuration is effective only when using file-based sources such as Parquet, JSON and ORC. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2430) |
| `spark.sql.files.maxPartitionNum` | int | _(optional)_ | 3.5.0 | The suggested (not guaranteed) maximum number of split file partitions. If it is set, Spark will rescale each partition to make the number of partitions is close to this value if the initial number of partitions exceeds this value. This configuration is effective only when using file-based sources such as Parquet, JSON and ORC. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2459) |
| `spark.sql.files.maxRecordsPerFile` | long | `0` | 2.2.0 | Maximum number of records to write out to a single file. If this value is zero or negative, there is no limit. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2495) |
| `spark.sql.files.minPartitionNum` | int | _(optional)_ | 3.1.0 | The suggested (not guaranteed) minimum number of split file partitions. If not set, the default value is `${LEAF_NODE_DEFAULT_PARALLELISM.key}`. This configuration is effective only when using file-based sources such as Parquet, JSON and ORC. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2449) |
| `spark.sql.files.openCostInBytes` | bytes | `4MB` | 2.0.0 | The estimated cost to open a file, measured by the number of bytes could be scanned in the same time. This is used when putting multiple files into a partition. It's better to over estimated, then the partitions with small files will be faster than partitions with bigger files (which is scheduled first). This configuration is effective only when using file-based sources such as Parquet, JSON and ORC. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2438) |
| `spark.sql.files.supportSecondOffsetFormat` | boolean | `true` | 4.0.0 | When set to true, datetime formatter used for csv, json and xml will support zone offsets that have seconds in it. e.g. LA timezone offset prior to 1883 was -07:52:58. When this flag is not set we lose seconds information. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L6310) |

### `spark.sql.function.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.function.concatBinaryAsString` | boolean | `false` | 2.3.0 | When this option is set to false and all inputs are binary, `functions.concat` returns an output as binary. Otherwise, it returns as a string. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4375) |
| `spark.sql.function.eltOutputAsString` | boolean | `false` | 2.3.0 | When this option is set to false and all inputs are binary, `elt` returns an output as binary. Otherwise, it returns as a string. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4382) |

### `spark.sql.geospatial.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.geospatial.enabled` | boolean | `() => Utils.isTesting` | 4.1.0 | When true, enables geospatial types (GEOGRAPHY/GEOMETRY) and ST functions. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L577) |

### `spark.sql.groupByAliases.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.groupByAliases` | boolean | `true` | 2.2.0 | When true, aliases in a select list can be used in group by clauses. When false, an analysis exception is thrown in the case. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2183) |

### `spark.sql.groupByOrdinal.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.groupByOrdinal` | boolean | `true` | 2.0.0 | When true, the ordinal numbers in group by clauses are treated as the position in the select list. When false, the ordinal numbers are ignored. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2176) |

### `spark.sql.hive.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.hive.advancedPartitionPredicatePushdown.enabled` | boolean | `true` | 2.3.0 | When true, advanced partition predicate pushdown into Hive metastore is enabled. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L874) |
| `spark.sql.hive.caseSensitiveInferenceMode` | enum | `HiveCaseSensitiveInferenceMode.NEVER_INFER` | 2.1.1 | Sets the action to take when a case-sensitive schema cannot be read from a Hive Serde table's properties when reading the table with Spark native data sources. Valid options include INFER_AND_SAVE (infer the case-sensitive schema from the underlying data files and write it back to the table properties), INFER_ONLY (infer the schema but don't attempt to write it to the table properties) and NEVER_INFER (the default mode-- fallback to using the case-insensitive metastore schema instead of inferring). | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1804) |
| `spark.sql.hive.convertCTAS` | boolean | `false` | 2.0.0 | When true, a table created by a Hive CTAS statement (no USING clause) without specifying any storage property will be converted to a data source table, using the data source set by ${DEFAULT_DATA_SOURCE_NAME.key}. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1954) |
| `spark.sql.hive.dropPartitionByName.enabled` | boolean | `false` | 3.4.0 | When true, Spark will get partition name rather than partition object to drop partition, which can improve the performance of drop partition. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1730) |
| `spark.sql.hive.filesourcePartitionFileCacheSize` | long | `250 * 1024 * 1024` | 2.1.1 | When nonzero, enable caching of partition file metadata in memory. All tables share a cache that can use up to specified num bytes for file metadata. This conf only has an effect when hive filesource partition management is enabled. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1792) |
| `spark.sql.hive.gatherFastStats` | boolean | `true` | 2.0.1 | When true, fast stats (number of files and total size of all files) will be gathered in parallel while repairing table partitions to avoid the sequential listing in Hive metastore. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1963) |
| `spark.sql.hive.manageFilesourcePartitions` | boolean | `true` | 2.1.1 | When true, enable metastore partition management for file source tables as well. This includes both datasource and converted Hive tables. When partition management is enabled, datasource tables store partition in the Hive metastore, and use the metastore to prune partitions during query planning when ${HIVE_METASTORE_PARTITION_PRUNING.key} is set to true. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1781) |
| `spark.sql.hive.metastorePartitionPruning` | boolean | `true` | 1.5.0 | When true, some predicates will be pushed down into the Hive metastore so that unmatching partitions can be eliminated earlier. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1738) |
| `spark.sql.hive.metastorePartitionPruningFallbackOnException` | boolean | `false` | 3.3.0 | Whether to fallback to get all partitions from Hive metastore and perform partition pruning on Spark client side, when encountering MetaException from the metastore. Note that Spark query performance may degrade if this is enabled and there are many partitions to be listed. If this is disabled, Spark will fail the query instead. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1760) |
| `spark.sql.hive.metastorePartitionPruningFastFallback` | boolean | `false` | 3.3.0 | When this config is enabled, if the predicates are not supported by Hive or Spark does fallback due to encountering MetaException from the metastore, Spark will instead prune partitions by getting the partition names first and then evaluating the filter expressions on the client side. Note that the predicates with TimeZoneAwareExpression is not supported. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1770) |
| `spark.sql.hive.metastorePartitionPruningInSetThreshold` | int | `1000` | 3.1.0 | The threshold of set size for InSet predicate when pruning partitions through Hive Metastore. When the set size exceeds the threshold, we rewrite the InSet predicate to be greater than or equal to the minimum value in set and less than or equal to the maximum value in set. Larger values may cause Hive Metastore stack overflow. But for InSet inside Not with values exceeding the threshold, we won't push it to Hive Metastore. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1746) |
| `spark.sql.hive.tablePropertyLengthThreshold` | int | _(optional)_ | 3.2.0 | The maximum length allowed in a single cell when storing Spark-specific information in Hive's metastore as table properties. Currently it covers 2 things: the schema's JSON string, the histogram of column statistics. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1817) |

### `spark.sql.icu.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.icu.caseMappings.enabled` | boolean | `true` | 4.0.0 | When enabled we use the ICU library (instead of the JVM) to implement case mappings for strings under UTF8_BINARY collation. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1089) |

### `spark.sql.inMemoryColumnarStorage.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.inMemoryColumnarStorage.batchSize` | int | `10000` | 1.1.1 | Controls the size of batches for columnar caching. Larger batch sizes can improve memory utilization and compression, but risk OOMs when caching data. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L733) |
| `spark.sql.inMemoryColumnarStorage.compressed` | boolean | `true` | 1.0.1 | When set to true Spark SQL will automatically select a compression codec for each column based on statistics of the data. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L726) |
| `spark.sql.inMemoryColumnarStorage.enableVectorizedReader` | boolean | `true` | 2.3.1 | Enables vectorized reader for columnar caching. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L777) |
| `spark.sql.inMemoryColumnarStorage.hugeVectorReserveRatio` | double | `1.2` | 4.0.0 | When spark.sql.inMemoryColumnarStorage.hugeVectorThreshold <= 0 or the required memory is smaller than spark.sql.inMemoryColumnarStorage.hugeVectorThreshold, spark reserves required memory * 2 memory; otherwise, spark reserves required memory * this ratio memory, and will release this column vector memory before reading the next batch rows. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L741) |
| `spark.sql.inMemoryColumnarStorage.hugeVectorThreshold` | bytes | `-1` | 4.0.0 | When the required memory is larger than this, spark reserves required memory * ${VECTORIZED_HUGE_VECTOR_RESERVE_RATIO.key} memory next time and release this column vector memory before reading the next batch rows. -1 means disabling the optimization. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L752) |
| `spark.sql.inMemoryColumnarStorage.partitionPruning` | boolean | `true` | 1.2.0 | When true, enable partition pruning for in-memory columnar tables. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L761) |

### `spark.sql.inMemoryTableScanStatistics.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.inMemoryTableScanStatistics.enable` | boolean | `false` | 3.0.0 | When true, enable in-memory table scan accumulators. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L769) |

### `spark.sql.join.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.join.preferSortMergeJoin` | boolean | `true` | 2.0.0 | When true, prefer sort merge join over shuffled hash join. Sort merge join consumes less memory than shuffled hash join and it works efficiently when both join tables are large. On the other hand, shuffled hash join can improve performance (e.g., of full outer joins) when one of join tables is much smaller. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L790) |

### `spark.sql.json.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.json.enableExactStringParsing` | boolean | `true` | 4.0.0 | When set to true, string columns extracted from JSON objects will be extracted exactly as they appear in the input string, with no changes | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5514) |
| `spark.sql.json.enablePartialResults` | boolean | `true` | 3.4.0 | When set to true, enables partial results for structs, maps, and arrays in JSON when one or more fields do not match the schema | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5505) |
| `spark.sql.json.filterPushdown.enabled` | boolean | `true` | 3.1.0 | When true, enable filter pushdown to JSON datasource. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5492) |
| `spark.sql.json.useUnsafeRow` | boolean | `false` | 4.0.0 | When set to true, use UnsafeRow to represent struct result in the JSON parser. It can be overwritten by the JSON option `useUnsafeRow`. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5523) |

### `spark.sql.jsonGenerator.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.jsonGenerator.ignoreNullFields` | boolean | `true` | 3.0.0 | Whether to ignore null fields when generating JSON objects in JSON data source and JSON functions such as to_json. If false, it generates null for null fields in JSON objects. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3317) |
| `spark.sql.jsonGenerator.writeNullIfWithDefaultValue` | boolean | `true` | 3.4.0 | When true, when writing NULL values to columns of JSON tables with explicit DEFAULT values using INSERT, UPDATE, or MERGE commands, never skip writing the NULL values to storage, overriding spark.sql.jsonGenerator.ignoreNullFields or the ignoreNullFields option. This can be useful to enforce that inserted NULL values are present in storage to differentiate from missing data. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4607) |

### `spark.sql.lateralColumnAlias.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.lateralColumnAlias.enableImplicitResolution` | boolean | `true` | 3.4.0 | Enable resolving implicit lateral column alias defined in the same SELECT list. For example, with this conf turned on, for query `SELECT 1 AS a, a + 1` the `a` in `a + 1` can be resolved as the previously defined `1 AS a`. But note that table column has higher resolution priority than the lateral column alias. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L6084) |

### `spark.sql.lazySetOperatorOutput.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.lazySetOperatorOutput.enabled` | boolean | `true` | 4.0.0 | When set to true, Except/Intersect/Union operator's output will be a lazy val. It is a performance optimization for querires with a large number of stacked set operators. This is because of rules like WidenSetOperationTypes that traverse the logical plan tree and call output on each Except/Intersect/Union node. Such traversal has quadratic complexity: O(number_of_nodes * (1 + 2 + 3 + ... + number_of_nodes)). | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L6412) |

### `spark.sql.leafNodeDefaultParallelism.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.leafNodeDefaultParallelism` | int | _(optional)_ | 3.2.0 | The default parallelism of Spark SQL leaf nodes that produce data, such as the file scan node, the local data scan node, the range node, etc. The default value of this config is 'SparkContext#defaultParallelism'. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L881) |

### `spark.sql.legacy.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.legacy.addSingleFileInAddFile` | boolean | `false` | 3.0.0 | When true, only a single file can be added using ADD FILE. If false, then users can add directory by passing directory path to ADD FILE. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5386) |
| `spark.sql.legacy.allowAutoGeneratedAliasForView` | boolean | `false` | 3.2.0 | When true, it's allowed to use a input query without explicit alias when creating a permanent view. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3264) |
| `spark.sql.legacy.allowEmptySchemaWrite` | boolean | `false` | 3.4.0 | When this option is set to true, validation of empty or empty nested schemas that occurs when writing into a FileFormat based data source does not happen. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4435) |
| `spark.sql.legacy.allowHashOnMapType` | boolean | `false` | 3.0.0 | When set to true, hash expressions can be applied on elements of MapType. Otherwise, an analysis exception will be thrown. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5627) |
| `spark.sql.legacy.allowNegativeScaleOfDecimal` | boolean | `false` | 3.0.0 | When set to true, negative scale of Decimal type is allowed. For example, the type of number 1E10BD under legacy mode is DecimalType(2, -9), but is Decimal(11, 0) in non legacy mode. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5066) |
| `spark.sql.legacy.allowNonEmptyLocationInCTAS` | boolean | `false` | 3.2.0 | When false, CTAS with LOCATION throws an analysis exception if the location is not empty. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3207) |
| `spark.sql.legacy.allowNullComparisonResultInArraySort` | boolean | `false` | 3.2.2 | When set to false, `array_sort` function throws an error if the comparator function returns null. If set to true, it restores the legacy behavior that handles null as zero (equal). | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L6020) |
| `spark.sql.legacy.allowParameterlessCount` | boolean | `false` | 3.1.1 | When true, the SQL function 'count' is allowed to take no parameters. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3199) |
| `spark.sql.legacy.allowSessionVariableInPersistedView` | boolean | `false` | 4.1.0 | When set to true, variables can be found under identifiers in a view query. Throw otherwise. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L6469) |
| `spark.sql.legacy.allowStarWithSingleTableIdentifierInCount` | boolean | `false` | 3.2 | When true, the SQL function 'count' is allowed to take single 'tblName.*' as parameter | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3216) |
| `spark.sql.legacy.allowTempViewCreationWithMultipleNameparts` | boolean | `false` | 3.4.0 | When true, temp view creation Dataset APIs will allow the view creation even if the view name is multiple name parts. The extra name parts will be dropped during the view creation | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3254) |
| `spark.sql.legacy.allowUntypedScalaUDF` | boolean | `false` | 3.0.0 | When set to true, user is allowed to use org.apache.spark.sql.functions. udf(f: AnyRef, dataType: DataType). Otherwise, an exception will be thrown at runtime. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5113) |
| `spark.sql.legacy.allowZeroIndexInFormatString` | boolean | `false` | 3.3 | When false, the `strfmt` in `format_string(strfmt, obj, ...)` and `printf(strfmt, obj, ...)` will no longer support to use "0$" to specify the first argument, the first argument should always reference by "1$" when use argument index to indicating the position of the argument in the argument list. This config will be removed in the future releases. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3224) |
| `spark.sql.legacy.avro.allowIncompatibleSchema` | boolean | `false` | 3.5.1 | When set to false, if types in Avro are encoded in the same format, but the type in the Avro schema explicitly says that the data types are different, reject reading the data type in the format to avoid returning incorrect results. When set to true, it restores the legacy behavior of allow reading the data in the format, which may return incorrect results. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L6030) |
| `spark.sql.legacy.bangEqualsNot` | boolean | `false` | 4.0.0 | When set to true, '!' is a lexical equivalent for 'NOT'. That is '!' can be used outside of the documented prefix usage in a logical expression. Examples are: `expr ! IN (1, 2)` and `expr ! BETWEEN 1 AND 2`, but also `IF ! EXISTS`. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L6392) |
| `spark.sql.legacy.blockCreateTempTableUsingProvider` | boolean | `false` | 4.1.0 | If enabled, we fail legacy CREATE TEMPORARY TABLE ... USING provider during parsing. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L285) |
| `spark.sql.legacy.bucketedTableScan.outputOrdering` | boolean | `false` | 3.0.0 | When true, the bucketed table scan will list files during planning to figure out the output ordering, which is expensive and may make the planning quite slow. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5076) |
| `spark.sql.legacy.castComplexTypesToString.enabled` | boolean | `false` | 3.1.0 | When true, maps and structs are wrapped by [] in casting to strings, and NULL elements of structs/maps/arrays will be omitted while converting to strings. Otherwise, if this is false, which is the default, maps and structs are wrapped by {}, and NULL elements will be converted to "null". | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5806) |
| `spark.sql.legacy.charVarcharAsString` | boolean | `false` | 3.1.0 | When true, Spark treats CHAR/VARCHAR type the same as STRING type, which is the behavior of Spark 3.0 and earlier. This means no length check for CHAR/VARCHAR type and no padding for CHAR type when writing data to the table. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5849) |
| `spark.sql.legacy.codingErrorAction` | boolean | `false` | 4.0.0 | When set to true, encode/decode functions replace unmappable characters with mojibake instead of reporting coding errors. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L6372) |
| `spark.sql.legacy.collationAwareHashFunctions` | boolean | `false` | 4.0.1 | Enables collation aware hashing (legacy behavior) for collated strings in Murmur3Hash and XxHash64 user-facing expressions. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1080) |
| `spark.sql.legacy.consecutiveStringLiterals.enabled` | boolean | `false` | 4.1.0 | When true, consecutive string literals separated by double quotes (e.g. 'a''b') will be parsed as concatenated strings. This preserves pre-Spark 4.0 behavior where'a''b' would be parsed as 'ab' instead of 'a'b'. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4556) |
| `spark.sql.legacy.createEmptyCollectionUsingStringType` | boolean | `false` | 3.0.0 | When set to true, Spark returns an empty collection with `StringType` as element type if the `array`/`map` function is called without any parameters. Otherwise, Spark returns an empty collection with `NullType` as element type. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5103) |
| `spark.sql.legacy.createHiveTableByDefault` | boolean | `sys.env.get("SPARK_SQL_LEGACY_CREATE_HIVE_TABLE").contains("true")` | 3.1.0 | When set to true, CREATE TABLE syntax without USING or STORED AS will use Hive instead of the value of ${DEFAULT_DATA_SOURCE_NAME.key} as the table provider. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5840) |
| `spark.sql.legacy.csv.enableDateTimeParsingFallback` | boolean | _(optional)_ | 3.4.0 | When true, enable legacy date/time parsing fallback in CSV | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5600) |
| `spark.sql.legacy.ctePrecedencePolicy` | enum | `LegacyBehaviorPolicy.CORRECTED` | 3.0.0 | When LEGACY, outer CTE definitions takes precedence over inner definitions. If set to EXCEPTION, AnalysisException is thrown while name conflict is detected in nested CTE. The default is CORRECTED, inner CTE definitions take precedence. This config will be removed in future versions and CORRECTED will be the only behavior. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5287) |
| `spark.sql.legacy.dataFrameWriterV2IgnorePathOption` | boolean | `false` | 3.5.6 | When set to true, DataFrameWriterV2 ignores the 'path' option and always write data to the default table location. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L6448) |
| `spark.sql.legacy.dataset.nameNonStructGroupingKeyAsValue` | boolean | `false` | 3.0.0 | When set to true, the key attribute resulted from running `Dataset.groupByKey` for non-struct key type, will be named as `value`, following the behavior of Spark version 2.4 and earlier. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5141) |
| `spark.sql.legacy.db2.booleanMapping.enabled` | boolean | `false` | 4.0.0 | When true, BooleanType maps to CHAR(1) in DB2; otherwise, BOOLEAN | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5460) |
| `spark.sql.legacy.db2.numericMapping.enabled` | boolean | `false` | 4.0.0 | When true, SMALLINT maps to IntegerType in DB2; otherwise, ShortType | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5452) |
| `spark.sql.legacy.decimal.retainFractionDigitsOnTruncate` | boolean | `false` | 4.0.0 | When set to true, we will try to retain the fraction digits first rather than integral digits as prior Spark 4.0, when getting a least common type between decimal types, and the result decimal precision exceeds the max precision. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L6337) |
| `spark.sql.legacy.disableMapKeyNormalization` | boolean | `false` | 4.0.0 | Disables key normalization when creating a map with `ArrayBasedMapBuilder`. When set to `true` it will prevent key normalization when building a map, which will allow for values such as `-0.0` and `0.0` to be present as distinct keys. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4462) |
| `spark.sql.legacy.doLooseUpcast` | boolean | `false` | 3.0.0 | When true, the upcast will be loose and allows string to atomic types. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5280) |
| `spark.sql.legacy.duplicateBetweenInput` | boolean | `false` | 4.0.0 | When true, we use legacy between implementation. This is a flag that fixes a problem introduced by a between optimization, see ticket SPARK-49063. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5797) |
| `spark.sql.legacy.earlyEvalCurrentTime` | boolean | `false` | 4.0.0 | When set to true, evaluation and constant folding will happen for now() and current_timestamp() expressions before finish analysis phase. This flag will allow a bit more liberal syntax but it will sacrifice correctness - Results of now() and current_timestamp() can be different for different operations in a single query. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L6380) |
| `spark.sql.legacy.emptyCurrentDBInCli` | boolean | `false` | 3.4.0 | When false, spark-sql CLI prints the current database in prompt. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5908) |
| `spark.sql.legacy.execution.pandas.groupedMap.assignColumnsByName` | boolean | `true` | 2.4.1 | When true, columns will be looked up by name if labeled with a string and fallback to use position if not. When false, a grouped map Pandas UDF will assign columns from the returned Pandas DataFrame based on position, regardless of column label type. This configuration will be deprecated in future releases. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4271) |
| `spark.sql.legacy.execution.pythonUDF.pandas.conversion.enabled` | boolean | `false` | 4.1.0 | When true and ${PYTHON_UDF_ARROW_ENABLED.key} is enabled, matches the default Arrow Python UDF behavior before 4.1.0. With this behavior, extrapandas conversion happens during (de)serialization between JVM and Python workers. This matters especially when the produced output has a schema different from specified schema, resulting in a different type coercion. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4247) |
| `spark.sql.legacy.execution.pythonUDTF.pandas.conversion.enabled` | boolean | `false` | 4.1.0 | When true and ${PYTHON_TABLE_UDF_ARROW_ENABLED.key} is enabled, extra pandas conversion happens during (de)serialization between JVM and Python workers. This matters especially when the produced output has a schema different from specified schema, resulting in a different type coercion. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4236) |
| `spark.sql.legacy.exponentLiteralAsDecimal.enabled` | boolean | `false` | 3.0.0 | When set to true, a literal with an exponent (e.g. 1E-30) would be parsed as Decimal rather than Double. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5046) |
| `spark.sql.legacy.extraOptionsBehavior.enabled` | boolean | `false` | 3.1.0 | When true, the extra options will be ignored for DataFrameReader.table(). If set it to false, which is the default, Spark will check if the extra options have the same key, but the value is different with the table serde properties. If the check passes, the extra options will be merged with the serde properties as the scan options. Otherwise, an exception will be thrown. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5828) |
| `spark.sql.legacy.followThreeValuedLogicInArrayExists` | boolean | `true` | 3.0.0 | When true, the ArrayExists will follow the three-valued boolean logic. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5345) |
| `spark.sql.legacy.fromDayTimeString.enabled` | boolean | `false` | 3.0.0 | When true, the `from` bound is not taken into account in conversion of a day-time string to an interval, and the `to` bound is used to skip all interval units out of the specified range. If it is set to `false`, `ParseException` is thrown if the input does not match to the pattern defined by `from` and `to`. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5364) |
| `spark.sql.legacy.groupingIdWithAppendedUserGroupBy` | boolean | `false` | 3.2.3 | When true, grouping_id() returns values based on grouping set columns plus user-given group-by expressions order like Spark 3.2.0, 3.2.1, 3.2.2, and 3.3.0. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5644) |
| `spark.sql.legacy.histogramNumericPropagateInputType` | boolean | `true` | 3.3.0 | The histogram_numeric function computes a histogram on numeric 'expr' using nb bins. The return value is an array of (x,y) pairs representing the centers of the histogram's bins. If this config is set to true, the output type of the 'x' field in the return value is propagated from the input value consumed in the aggregate function. Otherwise, 'x' always has double type. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5986) |
| `spark.sql.legacy.identifierClause` | boolean | `false` | 4.1.0 | When set to false, IDENTIFIER('literal') is resolved to an identifier at parse time anywhere identifiers can occur. When set to true, only the legacy IDENTIFIER(constantExpr) clause is allowed, which evaluates the expression at analysis and is limited to a narrow subset of scenarios. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5055) |
| `spark.sql.legacy.inSubqueryNullability` | boolean | `false` | 3.5.0 | When set to false, IN subquery nullability is correctly calculated based on both the left and right sides of the IN. When set to true, restores the legacy behavior that does not check the right side's nullability. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L6052) |
| `spark.sql.legacy.inlineCTEInCommands` | boolean | `false` | 4.0.0 | If true, always inline the CTE relations for the queries in commands. This is the legacy behavior which may produce incorrect results because Spark may evaluate a CTE relation more than once, even if it's nondeterministic. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5324) |
| `spark.sql.legacy.integerGroupingId` | boolean | `false` | 3.1.0 | When true, grouping_id() returns int values instead of long values. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5636) |
| `spark.sql.legacy.interval.enabled` | boolean | `false` | 3.2.0 | When set to true, Spark SQL uses the mixed legacy interval type `CalendarIntervalType` instead of the ANSI compliant interval types `YearMonthIntervalType` and `DayTimeIntervalType`. For instance, the date subtraction expression returns `CalendarIntervalType` when the SQL config is set to `true` otherwise an ANSI interval. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5924) |
| `spark.sql.legacy.javaCharsets` | boolean | `false` | 4.0.0 | When set to true, the functions like `encode()` can use charsets from JDK while encoding or decoding string values. If it is false, such functions support only one of the charsets: 'US-ASCII', 'ISO-8859-1', 'UTF-8', 'UTF-16BE', 'UTF-16LE', 'UTF-16', 'UTF-32'. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L6362) |
| `spark.sql.legacy.json.allowEmptyString.enabled` | boolean | `false` | 3.0.0 | When set to true, the parser of JSON data source treats empty strings as null for some data types such as `IntegerType`. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5094) |
| `spark.sql.legacy.json.enableDateTimeParsingFallback` | boolean | _(optional)_ | 3.4.0 | When true, enable legacy date/time parsing fallback in JSON | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5608) |
| `spark.sql.legacy.keepCommandOutputSchema` | boolean | `false` | 3.0.2 | When true, Spark will keep the output schema of commands such as SHOW DATABASES unchanged. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5916) |
| `spark.sql.legacy.keepPartitionSpecAsStringLiteral` | boolean | `false` | 3.4.0 | If it is set to true, `PARTITION(col=05)` is parsed as a string literal of its text representation, e.g., string '05', when the partition column is string type. Otherwise, it is always parsed as a numeric literal in the partition spec. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5016) |
| `spark.sql.legacy.literal.pickMinimumPrecision` | boolean | `true` | 2.3.3 | When integral literal is used in decimal operations, pick a minimum precision required by the literal if this config is true, to make the resulting precision and/or scale smaller. This can reduce the possibility of precision lose and/or overflow. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4328) |
| `spark.sql.legacy.lpadRpadAlwaysReturnString` | boolean | `false` | 3.3.0 | When set to false, when the first argument and the optional padding pattern is a byte sequence, the result is a BINARY value. The default padding pattern in this case is the zero byte. When set to true, it restores the legacy behavior of always returning string types even for binary inputs. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5998) |
| `spark.sql.legacy.mssqlserver.datetimeoffsetMapping.enabled` | boolean | `false` | 4.0.0 | When true, DATETIMEOFFSET is mapped to StringType; otherwise, it is mapped to TimestampType. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5415) |
| `spark.sql.legacy.mssqlserver.numericMapping.enabled` | boolean | `false` | 2.4.5 | When true, use legacy MsSqlServer TINYINT, SMALLINT and REAL type mapping. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5407) |
| `spark.sql.legacy.mysql.bitArrayMapping.enabled` | boolean | `false` | 4.0.0 | When true, use LongType to represent MySQL BIT(n>1); otherwise, use BinaryType. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5424) |
| `spark.sql.legacy.mysql.timestampNTZMapping.enabled` | boolean | `false` | 4.0.0 | When true, TimestampNTZType and MySQL TIMESTAMP can be converted bidirectionally. For reading, MySQL TIMESTAMP is converted to TimestampNTZType when JDBC read option preferTimestampNTZ is true. For writing, TimestampNTZType is converted to MySQL TIMESTAMP; otherwise, DATETIME | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5432) |
| `spark.sql.legacy.negativeIndexInArrayInsert` | boolean | `false` | 3.4.2 | When set to true, restores the legacy behavior of `array_insert` for negative indexes - 0-based: the function inserts new element before the last one for the index -1. For example, `array_insert(['a', 'b'], -1, 'x')` returns `['a', 'x', 'b']`. When set to false, the -1 index points out to the last element, and the given example produces `['a', 'b', 'x']`. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L6241) |
| `spark.sql.legacy.noCharPaddingInPredicate` | boolean | `false` | 4.0.0 | When true, Spark will not apply char type padding for CHAR type columns in string comparison predicates, when '${READ_SIDE_CHAR_PADDING.key}' is false. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5892) |
| `spark.sql.legacy.notReserveProperties` | boolean | `false` | 3.0.0 | When true, all database and table properties are not reserved and available for create/alter syntaxes. But please be aware that the reserved properties will be silently removed. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5376) |
| `spark.sql.legacy.nullInEmptyListBehavior` | boolean | _(optional)_ | 3.5.0 | When set to true, restores the legacy incorrect behavior of IN expressions for NULL values IN an empty list (including IN subqueries and literal IN lists): `null IN (empty list)` should evaluate to false, but sometimes (not always) incorrectly evaluates to null in the legacy behavior. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L6064) |
| `spark.sql.legacy.nullValueWrittenAsQuotedEmptyStringCsv` | boolean | `false` | 3.3.0 | When set to false, nulls are written as unquoted empty strings in CSV data source. If set to true, it restores the legacy behavior that nulls were written as quoted empty strings, `""`. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L6010) |
| `spark.sql.legacy.oracle.timestampMapping.enabled` | boolean | `false` | 4.0.0 | When true, TimestampType maps to TIMESTAMP in Oracle; otherwise, TIMESTAMP WITH LOCAL TIME ZONE. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5443) |
| `spark.sql.legacy.parameterSubstitution.constantsOnly` | boolean | `false` | 4.1.0 | When true, limits parameter substitution to constants in DML/queries only, restoring the legacy behavior where parameter markers (? or :param) are only allowed in contexts where constant literals are expected. When false (default), parameter substitution is enabled everywhere a literal is supported, allowing parameter markers in any literal context throughout SQL parsing. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5395) |
| `spark.sql.legacy.parquet.nanosAsLong` | boolean | `false` | 3.2.4 | When true, the Parquet's nanos precision timestamps are converted to SQL long values. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5652) |
| `spark.sql.legacy.parquet.returnNullStructIfAllFieldsMissing` | boolean | `false` | 4.1.0 | When true, if all requested fields of a struct are missing in a parquet file, assume the struct is always null, even if other fields are present. The default behavior is to fetch and read an arbitrary non-requested field present in the file to determine struct nullness. If enabled, schema pruning may cause non-null structs to be read as null. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1557) |
| `spark.sql.legacy.parseNullPartitionSpecAsStringLiteral` | boolean | `false` | 3.0.2 | If it is set to true, `PARTITION(col=null)` is parsed as a string literal of its text representation, e.g., string 'null', when the partition column is string type. Otherwise, it is always parsed as a null literal in the partition spec. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5006) |
| `spark.sql.legacy.parseQueryWithoutEof` | boolean | `false` | 4.0.0 | When set to true, ParserInterface#parseQuery(...) is going to use base `query` grammar term without EOF resulting in some queries (like `SELECT 1 UNION SELECT 2`) to be parsed incorrectly - `UNION` will be treated as an alias, and the rest of SQL input will be thrown away. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L6435) |
| `spark.sql.legacy.parser.havingWithoutGroupByAsWhere` | boolean | `false` | 2.4.1 | If it is set to true, the parser will treat HAVING without GROUP BY as a normal WHERE, which does not follow SQL standard. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5085) |
| `spark.sql.legacy.pathOptionBehavior.enabled` | boolean | `false` | 3.1.0 | When true, "path" option is overwritten if one path parameter is passed to DataFrameReader.load(), DataFrameWriter.save(), DataStreamReader.load(), or DataStreamWriter.start(). Also, "path" option is added to the overall paths if multiple path parameters are passed to DataFrameReader.load() | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5817) |
| `spark.sql.legacy.percentileDiscCalculation` | boolean | `false` | 3.3.4 | If true, the old bogus percentile_disc calculation is used. The old calculation incorrectly mapped the requested percentile to the sorted range of values in some cases and so returned incorrect results. Also, the new implementation is faster as it doesn't contain the interpolation logic that the old percentile_cont based one did. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L6230) |
| `spark.sql.legacy.postgres.datetimeMapping.enabled` | boolean | `false` | 4.0.0 | When true, TimestampType maps to TIMESTAMP WITHOUT TIME ZONE in PostgreSQL for writing; otherwise, TIMESTAMP WITH TIME ZONE. When true, TIMESTAMP WITH TIME ZONE can be converted to TimestampNTZType when JDBC read option preferTimestampNTZ is true; otherwise, converted to TimestampType regardless of preferTimestampNTZ. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5468) |
| `spark.sql.legacy.raiseErrorWithoutErrorClass` | boolean | `false` | 4.0.0 | When set to true, restores the legacy behavior of `raise_error` and `assert_true` to not return the `[USER_RAISED_EXCEPTION]` prefix. For example, `raise_error('error!')` returns `error!` instead of `[USER_RAISED_EXCEPTION] Error!`. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L6253) |
| `spark.sql.legacy.readFileSourceTableCacheIgnoreOptions` | boolean | `false` | 4.0.0 | When set to true, reading from file source table caches the first query plan and ignores subsequent changes in query options. Otherwise, query options will be applied to the cached plan and may produce different results. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5875) |
| `spark.sql.legacy.replaceDatabricksSparkAvro.enabled` | boolean | `true` | 2.4.0 | If it is set to true, the data source provider com.databricks.spark.avro is mapped to the built-in but external Avro data source module for backward compatibility. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5026) |
| `spark.sql.legacy.respectNullabilityInTextDatasetConversion` | boolean | `false` | 3.3.0 | When true, the nullability in the user-specified schema for `DataFrameReader.schema(schema).json(jsonDataset)` and `DataFrameReader.schema(schema).csv(csvDataset)` is respected. Otherwise, they are turned to a nullable schema forcibly. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4888) |
| `spark.sql.legacy.scalarSubqueryAllowGroupByNonEqualityCorrelatedPredicate` | boolean | `false` | 4.0.0 | When set to true, use incorrect legacy behavior for checking whether a scalar subquery with a group-by on correlated columns is allowed. See SPARK-48503 | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L6273) |
| `spark.sql.legacy.scalarSubqueryCountBugBehavior` | boolean | `false` | 4.0.0 | When set to true, restores legacy behavior of potential incorrect count bug handling for scalar subqueries. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L6264) |
| `spark.sql.legacy.setCommandRejectsSparkCoreConfs` | boolean | `true` | 3.0.0 | If it is set to true, SET command will fail when the key is registered as a SparkConf entry. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5179) |
| `spark.sql.legacy.setopsPrecedence.enabled` | boolean | `false` | 2.4.0 | When set to true and the order of evaluation is not specified by parentheses, the set operations are performed from left to right as they appear in the query. When set to false and order of evaluation is not specified by parentheses, INTERSECT operations are performed before any UNION, EXCEPT and MINUS operations. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5035) |
| `spark.sql.legacy.sizeOfNull` | boolean | `true` | 2.4.0 | If it is set to false, or ${ANSI_ENABLED.key} is true, then size of null returns null. Otherwise, it returns -1, which was inherited from Hive. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4997) |
| `spark.sql.legacy.skipTypeValidationOnAlterPartition` | boolean | `false` | 3.4.0 | When true, skip validation for partition spec in ALTER PARTITION. E.g., `ALTER TABLE .. ADD PARTITION(p='a')` would work even the partition type is int. Besides, this config will also be used to skip type validation on partition spec when reading partitioned table. E.g., if the table partition spec is added without type validation, it might not be read correctly with the type validation. When false, the behavior follows ${STORE_ASSIGNMENT_POLICY.key} | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4650) |
| `spark.sql.legacy.statisticalAggregate` | boolean | `false` | 3.1.0 | When set to true, statistical aggregate function returns Double.NaN if divide by zero occurred during expression evaluation, otherwise, it returns null. Before version 3.1.0, it returns NaN in divideByZero case by default. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5122) |
| `spark.sql.legacy.storeAnalyzedPlanForView` | boolean | `false` | 3.1.0 | When true, analyzed plan instead of SQL text will be stored when creating temporary view | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3245) |
| `spark.sql.legacy.timeParserPolicy` | enum | `LegacyBehaviorPolicy.CORRECTED` | 3.0.0 | When LEGACY, java.text.SimpleDateFormat is used for formatting and parsing dates/timestamps in a locale-sensitive manner, which is the approach before Spark 3.0. When set to CORRECTED, classes from java.time.* packages are used for the same purpose. When set to EXCEPTION, RuntimeException is thrown when we will get different results. The default is CORRECTED. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5333) |
| `spark.sql.legacy.truncateForEmptyRegexSplit` | boolean | `false` | 4.1.0 | When set to true, splitting a string of length n using an empty regex with a positive limit discards the last n - limit characters.For example: SELECT split('abcd', '', 2) returns ['a', 'b'].When set to false, the last element of the resulting array contains all input beyond the last matched regex.For example: SELECT split('abcd', '', 2) returns ['a', 'bcd'].According to the description of the split function, this should be set to false by default. See SPARK-49968 for details. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L6605) |
| `spark.sql.legacy.typeCoercion.datetimeToString.enabled` | boolean | `false` | 3.0.0 | If it is set to true, date/timestamp will cast to string in binary comparisons with String when ${ANSI_ENABLED.key} is false. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5234) |
| `spark.sql.legacy.useCurrentConfigsForView` | boolean | `false` | 3.1.0 | When true, SQL Configs of the current active SparkSession instead of the captured ones will be applied during the parsing and analysis phases of the view resolution. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3236) |
| `spark.sql.legacy.useLegacyXMLParser` | boolean | `false` | 4.1.0 | When set to true, use the legacy XML parser for parsing XML files. Compared to the default parser, the legacy parser has less stringent validation checks for malformed content, but it's less memory-efficient | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L6620) |
| `spark.sql.legacy.useV1Command` | boolean | `false` | 3.3.0 | When true, Spark will use legacy V1 SQL commands. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5978) |
| `spark.sql.legacy.v1IdentifierNoCatalog` | boolean | `false` | 3.4.0 | When set to false, the v1 identifier will include '$SESSION_CATALOG_NAME' as the catalog name if database is defined. When set to true, it restores the legacy behavior that does not include catalog name. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L6042) |
| `spark.sql.legacy.viewSchemaBindingMode` | boolean | `true` | 4.0.0 | Set to false to disable the WITH SCHEMA clause for view DDL and suppress the line in DESCRIBE EXTENDED and SHOW CREATE TABLE. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2190) |
| `spark.sql.legacy.viewSchemaCompensation` | boolean | `true` | 4.0.0 | Set to false to revert default view schema binding mode from WITH SCHEMA COMPENSATION to WITH SCHEMA BINDING. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2198) |

### `spark.sql.lightweightPlanChangeValidation.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.lightweightPlanChangeValidation` | boolean | `true` | 4.0.0 | Similar to ${PLAN_CHANGE_VALIDATION.key}, this validates plan changes and runs after every rule, however it is enabled by default and so it should be lightweight. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L561) |

### `spark.sql.limit.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.limit.initialNumPartitions` | int | `1` | 3.4.0 | Initial number of partitions to try when executing a take on a query. Higher values lead to more partitions read. Lower values might lead to longer execution times as more jobs will be run | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L854) |
| `spark.sql.limit.scaleUpFactor` | int | `4` | 2.1.1 | Minimal increase rate in number of partitions between attempts when executing a take on a query. Higher values lead to more partitions read. Lower values might lead to longer execution times as more jobs will be run | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L864) |

### `spark.sql.mapKeyDedupPolicy.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.mapKeyDedupPolicy` | enum | `MapKeyDedupPolicy.EXCEPTION` | 3.0.0 | The policy to deduplicate map keys in builtin function: CreateMap, MapFromArrays, MapFromEntries, StringToMap, MapConcat and TransformKeys. When EXCEPTION, the query fails if duplicated map keys are detected. When LAST_WIN, the map key that is inserted at last takes precedence. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5271) |

### `spark.sql.mapZipWithUsesJavaCollections.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.mapZipWithUsesJavaCollections` | boolean | `true` | 4.1.0 | When true, the `map_zip_with` function uses Java collections instead of Scala collections. This is useful for avoiding NaN equality issues. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1225) |

### `spark.sql.maven.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.maven.additionalRemoteRepositories` | string | `sys.env.getOrElse("DEFAULT_ARTIFACT_REPOSITORY",
          "https://maven-central.storage-download.googleapis.com/maven2/")` | 3.0.0 | A comma-delimited string config of the optional additional remote Maven mirror repositories. This is only used for downloading Hive jars in IsolatedClientLoader if the default Maven Central repo is unreachable. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5353) |

### `spark.sql.maxBroadcastTableSize.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.maxBroadcastTableSize` | bytes | `8L << 30` | 4.1.0 | The maximum table size in bytes that can be broadcast in broadcast joins. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1852) |

### `spark.sql.maxConcurrentOutputFileWriters.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.maxConcurrentOutputFileWriters` | int | `0` | 3.2.0 | Maximum number of output file writers to use concurrently. If number of writers needed reaches this limit, task will sort rest of output then writing them. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5934) |

### `spark.sql.maxMetadataStringLength.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.maxMetadataStringLength` | int | `100` | 3.1.0 | Maximum number of characters to output for a metadata string. e.g. file location in `DataSourceScanExec`, every value will be abbreviated if exceed length. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5170) |

### `spark.sql.maxPlanStringLength.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.maxPlanStringLength` | bytes | `${ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH}` | 3.0.0 | Maximum number of characters to output for a plan string. If the plan is longer, further output will be truncated. The default setting always generates a full plan. Set this to a lower value such as 8k if plan strings are taking up too much memory or are causing OutOfMemory errors in the driver or UI processes. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5158) |

### `spark.sql.maxSinglePartitionBytes.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.maxSinglePartitionBytes` | bytes | `128m` | 3.4.0 | The maximum number of bytes allowed for a single partition. Otherwise, The planner will introduce shuffle to improve parallelism. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L823) |

### `spark.sql.mergeNestedTypeCoercion.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.mergeNestedTypeCoercion.enabled` | boolean | `false` | 4.1.0 | If enabled, allow MERGE INTO to coerce source nested types if they have lessnested fields than the target table's nested types. This is experimental andthe semantics may change. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L6642) |

### `spark.sql.nameResolutionLog.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.nameResolutionLog.level` | enum | `Level.TRACE` | 4.1.0 | Configures the log level for logging the name resolution in the single-pass bottom-up Resolver. The value can be ${VALID_LOG_LEVELS.mkString()}. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L553) |

### `spark.sql.objectHashAggregate.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.objectHashAggregate.sortBased.fallbackThreshold` | int | `128` | 2.2.0 | In the case of ObjectHashAggregateExec, when the size of the in-memory hash map grows too large, we will fall back to sort-based aggregation. This option sets a row count threshold for the size of the hash map. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3297) |

### `spark.sql.operatorPipeSyntaxEnabled.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.operatorPipeSyntaxEnabled` | boolean | `true` | 4.0.0 | If true, enable operator pipe syntax for Apache Spark SQL. This uses the operator pipe marker \|> to indicate separation between clauses of SQL in a manner that describes the sequence of steps that the query performs in a composable fashion. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L6222) |

### `spark.sql.optimizeNullAwareAntiJoin.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.optimizeNullAwareAntiJoin` | boolean | `true` | 3.1.0 | When true, NULL-aware anti join execution will be planed into BroadcastHashJoinExec with flag isNullAwareAntiJoin enabled, optimized from O(M*N) calculation into O(M) calculation using Hash lookup instead of Looping lookup. Only support for singleColumn NAAJ for now. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5785) |

### `spark.sql.optimizer.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.optimizer.avoidCollapseUDFWithExpensiveExpr` | boolean | `true` | 4.0.0 | Whether to avoid collapsing projections that would duplicate expensive expressions in UDFs. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3350) |
| `spark.sql.optimizer.canChangeCachedPlanOutputPartitioning` | boolean | `false` | 3.2.0 | Whether to forcibly enable some optimization rules that can change the output partitioning of a cached query when executing it for caching. If it is set to true, queries may need an extra shuffle to read the cached data. This configuration is disabled by default. The optimization rule enabled by this configuration is ${ADAPTIVE_EXECUTION_APPLY_FINAL_STAGE_SHUFFLE_OPTIMIZATIONS.key}. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2133) |
| `spark.sql.optimizer.collapseProjectAlwaysInline` | boolean | `false` | 3.3.0 | Whether to always collapse two adjacent projections and inline expressions even if it causes extra duplication. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3342) |
| `spark.sql.optimizer.datasourceV2ExprFolding` | boolean | `true` | 4.1.0 | When this config is set to true, do safe constant folding for the expressions before translation and pushdown. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1939) |
| `spark.sql.optimizer.datasourceV2JoinPushdown` | boolean | `false` | 4.1.0 | When this config is set to true, join is tried to be pushed downfor DSv2 data sources in V2ScanRelationPushdown optimization rule. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1930) |
| `spark.sql.optimizer.decorrelateExistsSubqueryLegacyIncorrectCountHandling.enabled` | boolean | `false` | 4.0.0 | If enabled, revert to legacy incorrect behavior for certain EXISTS/IN subqueries with COUNT or similar aggregates. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4772) |
| `spark.sql.optimizer.decorrelateInnerQuery.enabled` | boolean | `true` | 3.2.0 | Decorrelate inner query by eliminating correlated references and build domain joins. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4730) |
| `spark.sql.optimizer.decorrelateJoinPredicate.enabled` | boolean | `true` | 4.0.0 | Decorrelate scalar and lateral subqueries with correlated references in join predicates. This configuration is only effective when '${DECORRELATE_INNER_QUERY_ENABLED.key}' is true. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L6170) |
| `spark.sql.optimizer.decorrelateLimit.enabled` | boolean | `true` | 4.0.0 | Decorrelate subqueries with correlation under LIMIT. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4746) |
| `spark.sql.optimizer.decorrelateOffset.enabled` | boolean | `true` | 4.0.0 | Decorrelate subqueries with correlation under LIMIT with OFFSET. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4754) |
| `spark.sql.optimizer.decorrelatePredicateSubqueriesInJoinPredicate.enabled` | boolean | `true` | 4.0.0 | Decorrelate predicate (in and exists) subqueries with correlated references in join predicates. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L6180) |
| `spark.sql.optimizer.decorrelateSetOps.enabled` | boolean | `true` | 3.4.0 | Decorrelate subqueries with correlation under set operators. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4738) |
| `spark.sql.optimizer.decorrelateSubqueryLegacyIncorrectCountHandling.enabled` | boolean | `false` | 3.5.0 | If enabled, revert to legacy incorrect behavior for certain subqueries with COUNT or similar aggregates: see SPARK-43098. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4781) |
| `spark.sql.optimizer.decorrelateSubqueryPreventConstantHoldingForCountBug.enabled` | boolean | `true` | 4.0.0 | If enabled, prevents constant folding in subqueries that contain a COUNT-bug-susceptible Aggregate. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4790) |
| `spark.sql.optimizer.decorrelateUnionOrSetOpUnderLimit.enabled` | boolean | `true` | 4.0.0 | Decorrelate UNION or SET operation under LIMIT operator. If not enabled,revert to legacy incorrect behavior for certain subqueries with correlation underUNION/SET operator with a LIMIT operator above it. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4762) |
| `spark.sql.optimizer.disableHints` | boolean | `false` | 3.1.0 | When true, the optimizer will disable user-specified hints that are additional directives for better planning of a query. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4687) |
| `spark.sql.optimizer.dynamicPartitionPruning.enabled` | boolean | `true` | 3.0.0 | When true, we will generate predicate for partition column when it's used as join key | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L594) |
| `spark.sql.optimizer.dynamicPartitionPruning.fallbackFilterRatio` | double | `0.5` | 3.0.0 | When statistics are not available or configured not to be used, this config will be used as the fallback filter ratio for computing the data size of the partitioned table after dynamic partition pruning, in order to evaluate if it is worth adding an extra subquery as the pruning filter if broadcast reuse is not applicable. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L611) |
| `spark.sql.optimizer.dynamicPartitionPruning.reuseBroadcastOnly` | boolean | `true` | 3.0.0 | When true, dynamic partition pruning will only apply when the broadcast exchange of a broadcast hash join operation can be reused as the dynamic pruning filter. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L622) |
| `spark.sql.optimizer.dynamicPartitionPruning.useStats` | boolean | `true` | 3.0.0 | When true, distinct count statistics will be used for computing the data size of the partitioned table after dynamic partition pruning, in order to evaluate if it is worth adding an extra subquery as the pruning filter if broadcast reuse is not applicable. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L601) |
| `spark.sql.optimizer.enableCsvExpressionOptimization` | boolean | `true` | 3.2.0 | Whether to optimize CSV expressions in SQL optimizer. It includes pruning unnecessary columns from from_csv. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3335) |
| `spark.sql.optimizer.enableJsonExpressionOptimization` | boolean | `true` | 3.1.0 | Whether to optimize JSON expressions in SQL optimizer. It includes pruning unnecessary columns from from_json, simplifying from_json + to_json, to_json + named_struct(from_json.col1, from_json.col2, ....). | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3326) |
| `spark.sql.optimizer.excludeSubqueryRefsFromRemoveRedundantAliases.enabled` | boolean | `true` | 3.5.1 | When true, exclude the references from the subquery expressions (in, exists, etc.) while removing redundant aliases. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L6199) |
| `spark.sql.optimizer.excludedRules` | string | _(optional)_ | 2.4.0 | Configures a list of rules to be disabled in the optimizer, in which the rules are specified by their rule names and separated by comma. It is not guaranteed that all the rules in this configuration will eventually be excluded, as some rules are necessary for correctness. The optimizer will log the rules that have indeed been excluded. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L474) |
| `spark.sql.optimizer.expression.nestedPruning.enabled` | boolean | `true` | 3.0.0 | Prune nested fields from expressions in an operator which are unnecessary in satisfying a query. Note that this optimization doesn't prune nested fields from physical data source scanning. For pruning nested fields from scanning, please use `spark.sql.optimizer.nestedSchemaPruning.enabled` config. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4719) |
| `spark.sql.optimizer.expressionProjectionCandidateLimit` | int | `100` | 3.4.0 | The maximum number of the candidate of output expressions whose alias are replaced. It can preserve the output partitioning and ordering. Negative value means disable this optimization. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L717) |
| `spark.sql.optimizer.inSetConversionThreshold` | int | `10` | 2.0.0 | The threshold of set size for InSet conversion. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L491) |
| `spark.sql.optimizer.inSetSwitchThreshold` | int | `400` | 3.0.0 | Configures the max set size in InSet for which Spark will generate code with switch statements. This is applicable only to bytes, shorts, ints, dates. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L499) |
| `spark.sql.optimizer.maxIterations` | int | `100` | 2.0.0 | The max number of iterations the optimizer runs. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L483) |
| `spark.sql.optimizer.metadataOnly` | boolean | `false` | 2.1.1 | When true, enable the metadata-only query optimization that use the table's metadata to produce the partition columns instead of table scans. It applies when all the columns scanned are partition columns and the query has an aggregate operator that satisfies distinct semantics. By default the optimization is disabled, and deprecated as of Spark 3.0 since it may return incorrect results when the files are empty, see also SPARK-26709. It will be removed in the future releases. If you must use, use 'SparkSessionExtensions' instead to inject it as a custom rule. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1826) |
| `spark.sql.optimizer.nestedPredicatePushdown.supportedFileSources` | string | `parquet,orc` | 3.0.0 | A comma-separated list of data source short names or fully qualified data source implementation class names for which Spark tries to push down predicates for nested columns and/or names containing `dots` to data sources. This configuration is only effective with file-based data sources in DSv1. Currently, Parquet and ORC implement both optimizations. The other data sources don't support this feature yet. So the default value is 'parquet,orc'. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4696) |
| `spark.sql.optimizer.nestedSchemaPruning.enabled` | boolean | `true` | 2.4.1 | Prune nested fields from a logical relation's output which are unnecessary in satisfying a query. This optimization allows columnar file format readers to avoid reading unnecessary nested column data. Currently Parquet and ORC are the data sources that implement this optimization. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4676) |
| `spark.sql.optimizer.optimizeOneRowRelationSubquery` | boolean | `true` | 3.2.0 | When true, the optimizer will inline subqueries with OneRowRelation as leaf nodes. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4808) |
| `spark.sql.optimizer.optimizeOneRowRelationSubquery.alwaysInline` | boolean | `true` | 3.4.0 | When true, the optimizer will always inline single row subqueries even if it causes extra duplication. It only takes effect when ${OPTIMIZE_ONE_ROW_RELATION_SUBQUERY.key} is set to true. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4827) |
| `spark.sql.optimizer.optimizeUncorrelatedInSubqueriesInJoinCondition.enabled` | boolean | `true` | 4.0.0 | When true, optimize uncorrelated IN subqueries in join predicates by rewriting them to joins. This interacts with ${LEGACY_NULL_IN_EMPTY_LIST_BEHAVIOR.key} because it can rewrite IN predicates. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L6189) |
| `spark.sql.optimizer.plannedWrite.enabled` | boolean | `true` | 3.4.0 | When set to true, Spark optimizer will add logical sort operators to V1 write commands if needed so that `FileFormatWriter` does not need to insert physical sorts. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L708) |
| `spark.sql.optimizer.preserveAliasMetadataWhenCollapsingProjects` | boolean | `true` | 4.1.0 | When true, make sure to explicitly copy the metadata of the aliases from lower project list. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L466) |
| `spark.sql.optimizer.propagateDistinctKeys.enabled` | boolean | `true` | 3.3.0 | When true, the query optimizer will propagate a set of distinct attributes from the current node and use it to optimize query. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1279) |
| `spark.sql.optimizer.pruneFiltersCanPruneStreamingSubplan` | boolean | `false` | 4.0.0 | Allow PruneFilters to remove streaming subplans when we encounter a false filter. This flag is to restore prior buggy behavior for broken pipelines. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4853) |
| `spark.sql.optimizer.pullHintsIntoSubqueries` | boolean | `true` | — | Pull hints into subqueries in EliminateResolvedHint if enabled. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4837) |
| `spark.sql.optimizer.pullOutNestedDataOuterRefExpressions.enabled` | boolean | `true` | 4.0.0 | Handle correlation over nested data extract expressions by pulling out the expression into the outer plan. This enables correlation on map attributes for example. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4799) |
| `spark.sql.optimizer.replaceExceptWithFilter` | boolean | `true` | 2.3.0 | When true, the apply function of the rule verifies whether the right node of the except operation is of type Filter or Project followed by Filter. If yes, the rule further verifies 1) Excluding the filter operations from the right (as well as the left node, if any) on the top, whether both the nodes evaluates to a same result. 2) The left and right nodes don't contain any SubqueryExpressions. 3) The output column names of the left node are distinct. If all the conditions are met, the rule will replace the except operation with a Filter by flipping the filter condition(s) of the right node. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4302) |
| `spark.sql.optimizer.runtime.bloomFilter.applicationSideScanSizeThreshold` | bytes | `10GB` | 3.3.0 | Byte size threshold of the Bloom filter application side plan's aggregated scan size. Aggregated scan byte size of the Bloom filter application side needs to be over this value to inject a bloom filter. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L656) |
| `spark.sql.optimizer.runtime.bloomFilter.creationSideThreshold` | bytes | `10MB` | 3.3.0 | Size threshold of the bloom filter creation side plan. Estimated size needs to be under this value to try to inject bloom filter. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L648) |
| `spark.sql.optimizer.runtime.bloomFilter.enabled` | boolean | `true` | 3.3.0 | When true and if one side of a shuffle join has a selective predicate, we attempt to insert a bloom filter in the other side to reduce the amount of shuffle data. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L640) |
| `spark.sql.optimizer.runtime.bloomFilter.expectedNumItems` | long | `1000000` | 3.3.0 | The default number of expected items for the runtime bloomfilter | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L665) |
| `spark.sql.optimizer.runtime.bloomFilter.maxNumBits` | long | `67108864` | 3.3.0 | The max number of bits to use for the runtime bloom filter | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L687) |
| `spark.sql.optimizer.runtime.bloomFilter.maxNumItems` | long | `4000000` | 3.3.0 | The max allowed number of expected items for the runtime bloom filter | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L672) |
| `spark.sql.optimizer.runtime.bloomFilter.numBits` | long | `8388608` | 3.3.0 | The default number of bits to use for the runtime bloom filter | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L680) |
| `spark.sql.optimizer.runtime.rowLevelOperationGroupFilter.enabled` | boolean | `true` | 3.4.0 | Enables runtime group filtering for group-based row-level operations. Data sources that replace groups of data (e.g. files, partitions) may prune entire groups using provided data source filters when planning a row-level operation scan. However, such filtering is limited as not all expressions can be converted into data source filters and some expressions can only be evaluated by Spark (e.g. subqueries). Since rewriting groups is expensive, Spark can execute a query at runtime to find what records match the condition of the row-level operation. The information about matching records will be passed back to the row-level operation scan, allowing data sources to discard groups that don't have to be rewritten. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L694) |
| `spark.sql.optimizer.runtimeFilter.number.threshold` | int | `10` | 3.3.0 | The total number of injected runtime filters (non-DPP) for a single query. This is to prevent driver OOMs with too many Bloom filters. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L631) |
| `spark.sql.optimizer.scalarSubqueryUseSingleJoin` | boolean | `true` | 4.0.0 | When set to true, use LEFT_SINGLE join for correlated scalar subqueries where optimizer can't prove that only 1 row will be returned | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L6291) |
| `spark.sql.optimizer.serializer.nestedSchemaPruning.enabled` | boolean | `true` | 3.0.0 | Prune nested fields from object serialization operator which are unnecessary in satisfying a query. This optimization allows object serializers to avoid executing unnecessary nested expressions. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4709) |
| `spark.sql.optimizer.windowGroupLimitThreshold` | int | `1000` | 3.5.0 | Threshold for triggering `InsertWindowGroupLimit`. 0 means the output results is empty. -1 means disabling the optimization. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3703) |
| `spark.sql.optimizer.wrapExistsInAggregateFunction` | boolean | `true` | 4.0.0 | When true, the optimizer will wrap newly introduced `exists` attributes in an aggregate function to ensure that Aggregate nodes preserve semantic invariant that each variable among agg expressions appears either in grouping expressions or belongs to and aggregate function. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4816) |

### `spark.sql.orc.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.orc.aggregatePushdown` | boolean | `false` | 3.3.0 | If true, aggregates will be pushed down to ORC for optimization. Support MIN, MAX and COUNT as aggregate expression. For MIN/MAX, support boolean, integer, float and date type. For COUNT, support all data types. If statistics is missing from any ORC file footer, exception would be thrown. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1713) |
| `spark.sql.orc.columnarReaderBatchSize` | int | `4096` | 2.4.0 | The number of rows to include in a orc vectorized reader batch. The number should be carefully chosen to minimize overhead and avoid OOMs in reading data. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1686) |
| `spark.sql.orc.columnarWriterBatchSize` | int | `1024` | 3.4.0 | The number of rows to include in a orc vectorized writer batch. The number should be carefully chosen to minimize overhead and avoid OOMs in writing data. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1693) |
| `spark.sql.orc.compression.codec` | string | `zstd` | 2.3.0 | Sets the compression codec used when writing ORC files. If either `compression` or `orc.compress` is specified in the table-specific options/properties, the precedence would be `compression`, `orc.compress`, `spark.sql.orc.compression.codec`. Acceptable values include: none, uncompressed, snappy, zlib, lzo, zstd, lz4, brotli. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1660) |
| `spark.sql.orc.enableNestedColumnVectorizedReader` | boolean | `true` | 3.2.0 | Enables vectorized orc decoding for nested column. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1701) |
| `spark.sql.orc.enableVectorizedReader` | boolean | `true` | 2.3.0 | Enables vectorized orc decoding. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1680) |
| `spark.sql.orc.filterPushdown` | boolean | `true` | 1.4.0 | When true, enable filter pushdown for ORC files. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1707) |
| `spark.sql.orc.impl` | string | `native` | 2.3.0 | When native, use the native version of ORC support instead of the ORC library in Hive. It is 'hive' by default prior to Spark 2.4. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1671) |
| `spark.sql.orc.mergeSchema` | boolean | `false` | 3.0.0 | When true, the Orc data source merges schemas collected from all data files, otherwise the schema is picked from a random data file. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1722) |

### `spark.sql.orderByOrdinal.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.orderByOrdinal` | boolean | `true` | 2.0.0 | When true, the ordinal numbers are treated as the position in the select list. When false, the ordinal numbers in order/sort by clause are ignored. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2169) |

### `spark.sql.orderingAwareLimitOffset.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.orderingAwareLimitOffset` | boolean | `true` | 4.0.0 | When set to true, a local sort will be inserted between GlobalLimitExec and single-partition ShuffleExchangeExec, if the underlying plan produces sorted data. This is because shuffle reader in Spark fetches shuffle blocks in a random order and can not preserve the data ordering, while LIMIT/OFFSET must preserve ordering. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L6402) |

### `spark.sql.parquet.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.parquet.aggregatePushdown` | boolean | `false` | 3.3.0 | If true, aggregates will be pushed down to Parquet for optimization. Support MIN, MAX and COUNT as aggregate expression. For MIN/MAX, support boolean, integer, float and date type. For COUNT, support all data types. If statistics is missing from any Parquet file footer, exception would be thrown. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1509) |
| `spark.sql.parquet.binaryAsString` | boolean | `false` | 1.1.1 | Some other Parquet-producing systems, in particular Impala and older versions of Spark SQL, do not differentiate between binary data and strings when writing out the Parquet schema. This flag tells Spark SQL to interpret binary data as a string to provide compatibility with these systems. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1388) |
| `spark.sql.parquet.columnarReaderBatchSize` | int | `4096` | 2.4.0 | The number of rows to include in a parquet vectorized reader batch. The number should be carefully chosen to minimize overhead and avoid OOMs in reading data. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1586) |
| `spark.sql.parquet.compression.codec` | string | `snappy` | 1.1.1 | Sets the compression codec used when writing Parquet files. If either `compression` or `parquet.compression` is specified in the table-specific options/properties, the precedence would be `compression`, `parquet.compression`, `spark.sql.parquet.compression.codec`. Acceptable values include: none, uncompressed, snappy, gzip, lzo, brotli, lz4, lz4_raw, zstd. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1428) |
| `spark.sql.parquet.datetimeRebaseModeInRead` | enum | `LegacyBehaviorPolicy.CORRECTED` | 3.0.0 | When LEGACY, Spark will rebase dates/timestamps from the legacy hybrid (Julian + Gregorian) calendar to Proleptic Gregorian calendar when reading Parquet files. When CORRECTED, Spark will not do rebase and read the dates/timestamps as it is. When EXCEPTION, Spark will fail the reading if it sees ancient dates/timestamps that are ambiguous between the two calendars. This config is only effective if the writer info (like Spark, Hive) of the Parquet files is unknown. This config influences on reads of the following parquet logical types: DATE, TIMESTAMP_MILLIS, TIMESTAMP_MICROS. The INT96 type has the separate config: ${PARQUET_INT96_REBASE_MODE_IN_READ.key}. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5700) |
| `spark.sql.parquet.datetimeRebaseModeInWrite` | enum | `LegacyBehaviorPolicy.CORRECTED` | 3.0.0 | When LEGACY, Spark will rebase dates/timestamps from Proleptic Gregorian calendar to the legacy hybrid (Julian + Gregorian) calendar when writing Parquet files. When CORRECTED, Spark will not do rebase and write the dates/timestamps as it is. When EXCEPTION, Spark will fail the writing if it sees ancient dates/timestamps that are ambiguous between the two calendars. This config influences on writes of the following parquet logical types: DATE, TIMESTAMP_MILLIS, TIMESTAMP_MICROS. The INT96 type has the separate config: ${PARQUET_INT96_REBASE_MODE_IN_WRITE.key}. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5672) |
| `spark.sql.parquet.enableNestedColumnVectorizedReader` | boolean | `true` | 3.3.0 | Enables vectorized Parquet decoding for nested columns (e.g., struct, list, map). Requires ${PARQUET_VECTORIZED_READER_ENABLED.key} to be enabled. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1549) |
| `spark.sql.parquet.enableNullTypeVectorizedReader` | boolean | `true` | 4.1.0 | Enables vectorized Parquet reader support for NullType columns. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1569) |
| `spark.sql.parquet.enableVectorizedReader` | boolean | `true` | 2.0.0 | Enables vectorized parquet decoding. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1542) |
| `spark.sql.parquet.fieldId.read.enabled` | boolean | `false` | 3.3.0 | Field ID is a native field of the Parquet schema spec. When enabled, Parquet readers will use field IDs (if present) in the requested Spark schema to look up Parquet fields instead of using column names | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1631) |
| `spark.sql.parquet.fieldId.read.ignoreMissing` | boolean | `false` | 3.3.0 | When the Parquet file doesn't have any field IDs but the Spark read schema is using field IDs to read, we will silently return nulls when this flag is enabled, or error otherwise. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1640) |
| `spark.sql.parquet.fieldId.write.enabled` | boolean | `true` | 3.3.0 | Field ID is a native field of the Parquet schema spec. When enabled, Parquet writers will populate the field Id metadata (if present) in the Spark schema to the Parquet schema. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1594) |
| `spark.sql.parquet.filterPushdown` | boolean | `true` | 1.2.0 | Enables Parquet filter push-down optimization when set to true. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1441) |
| `spark.sql.parquet.filterPushdown.date` | boolean | `true` | 2.4.0 | If true, enables Parquet filter push-down optimization for Date. This configuration only has an effect when '${PARQUET_FILTER_PUSHDOWN_ENABLED.key}' is enabled. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1447) |
| `spark.sql.parquet.filterPushdown.decimal` | boolean | `true` | 2.4.0 | If true, enables Parquet filter push-down optimization for Decimal. This configuration only has an effect when '${PARQUET_FILTER_PUSHDOWN_ENABLED.key}' is enabled. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1467) |
| `spark.sql.parquet.filterPushdown.string.startsWith` | boolean | `true` | 2.4.0 | If true, enables Parquet filter push-down optimization for string startsWith function. This configuration only has an effect when '${PARQUET_FILTER_PUSHDOWN_ENABLED.key}' is enabled. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1477) |
| `spark.sql.parquet.filterPushdown.stringPredicate` | fallback | → `PARQUET_FILTER_PUSHDOWN_STRING_STARTSWITH_ENABLED` | 3.4.0 | If true, enables Parquet filter push-down optimization for string predicate such as startsWith/endsWith/contains function. This configuration only has an effect when '${PARQUET_FILTER_PUSHDOWN_ENABLED.key}' is enabled. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1487) |
| `spark.sql.parquet.filterPushdown.timestamp` | boolean | `true` | 2.4.0 | If true, enables Parquet filter push-down optimization for Timestamp. This configuration only has an effect when '${PARQUET_FILTER_PUSHDOWN_ENABLED.key}' is enabled and Timestamp stored as TIMESTAMP_MICROS or TIMESTAMP_MILLIS type. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1457) |
| `spark.sql.parquet.ignoreVariantAnnotation` | boolean | `false` | 4.1.0 | When true, ignore the variant logical type annotation and treat the Parquet column in the same way as the underlying struct type | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1612) |
| `spark.sql.parquet.inferTimestampNTZ.enabled` | boolean | `true` | 3.4.0 | When enabled, Parquet timestamp columns with annotation isAdjustedToUTC = false are inferred as TIMESTAMP_NTZ type during schema inference. Otherwise, all the Parquet timestamp columns are inferred as TIMESTAMP_LTZ types. Note that Spark writes the output schema into Parquet's footer metadata on file writing and leverages it on file reading. Thus this configuration only affects the schema inference on Parquet files which are not written by Spark. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1649) |
| `spark.sql.parquet.int96AsTimestamp` | boolean | `true` | 1.3.0 | Some Parquet-producing systems, in particular Impala, store Timestamp into INT96. Spark would also store Timestamp as INT96 because we need to avoid precision lost of the nanoseconds field. This flag tells Spark SQL to interpret INT96 data as a timestamp to provide compatibility with these systems. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1397) |
| `spark.sql.parquet.int96RebaseModeInRead` | enum | `LegacyBehaviorPolicy.CORRECTED` | 3.1.0 | When LEGACY, Spark will rebase INT96 timestamps from the legacy hybrid (Julian + Gregorian) calendar to Proleptic Gregorian calendar when reading Parquet files. When CORRECTED, Spark will not do rebase and read the timestamps as it is. When EXCEPTION, Spark will fail the reading if it sees ancient timestamps that are ambiguous between the two calendars. This config is only effective if the writer info (like Spark, Hive) of the Parquet files is unknown. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5687) |
| `spark.sql.parquet.int96RebaseModeInWrite` | enum | `LegacyBehaviorPolicy.CORRECTED` | 3.1.0 | When LEGACY, Spark will rebase INT96 timestamps from Proleptic Gregorian calendar to the legacy hybrid (Julian + Gregorian) calendar when writing Parquet files. When CORRECTED, Spark will not do rebase and write the timestamps as it is. When EXCEPTION, Spark will fail the writing if it sees ancient timestamps that are ambiguous between the two calendars. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5660) |
| `spark.sql.parquet.int96TimestampConversion` | boolean | `false` | 2.3.0 | This controls whether timestamp adjustments should be applied to INT96 data when converting to timestamps, for data written by Impala. This is necessary because Impala stores INT96 data with a different timezone offset than Hive & Spark. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1406) |
| `spark.sql.parquet.mergeSchema` | boolean | `false` | 1.5.0 | When true, the Parquet data source merges schemas collected from all data files, otherwise the schema is picked from the summary file or a random data file if no summary file is available. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1371) |
| `spark.sql.parquet.output.committer.class` | string | `org.apache.parquet.hadoop.ParquetOutputCommitter` | 1.5.0 | The output committer class used by Parquet. The specified class needs to be a subclass of org.apache.hadoop.mapreduce.OutputCommitter. Typically, it's also a subclass of org.apache.parquet.hadoop.ParquetOutputCommitter. If it is not, then metadata summaries will never be created, irrespective of the value of parquet.summary.metadata.level | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1528) |
| `spark.sql.parquet.outputTimestampType` | enum | `ParquetOutputTimestampType.INT96` | 2.3.0 | Sets which Parquet timestamp type to use when Spark writes data to Parquet files. INT96 is a non-standard but commonly used timestamp type in Parquet. TIMESTAMP_MICROS is a standard timestamp type in Parquet, which stores number of microseconds from the Unix epoch. TIMESTAMP_MILLIS is also standard, but with millisecond precision, which means Spark has to truncate the microsecond portion of its timestamp value. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1418) |
| `spark.sql.parquet.pushdown.inFilterThreshold` | int | `10` | 2.4.0 | For IN predicate, Parquet filter will push-down a set of OR clauses if its number of values not exceeds this threshold. Otherwise, Parquet filter will push-down a value greater than or equal to its minimum value and less than or equal to its maximum value. By setting this value to 0 this feature can be disabled. This configuration only has an effect when '${PARQUET_FILTER_PUSHDOWN_ENABLED.key}' is enabled. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1496) |
| `spark.sql.parquet.reader.respectUnknownTypeAnnotation.enabled` | boolean | `false` | 4.1.2 | When enabled, respects the UNKNOWN type annotation in Parquet files during schema inference and infers NullType. When disabled, ignores the UNKNOWN annotation and uses the physical type instead. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1621) |
| `spark.sql.parquet.recordLevelFilter.enabled` | boolean | `false` | 2.3.0 | If true, enables Parquet's native record-level filtering using the pushed down filters. This configuration only has an effect when '${PARQUET_FILTER_PUSHDOWN_ENABLED.key}' is enabled and the vectorized reader is not used. You can ensure the vectorized reader is not used by setting '${PARQUET_VECTORIZED_READER_ENABLED.key}' to false. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1576) |
| `spark.sql.parquet.respectSummaryFiles` | boolean | `false` | 1.5.0 | When true, we make assumption that all part-files of Parquet are consistent with summary files and we will ignore them when merging schema. Otherwise, if this is false, which is the default, we will merge all part-files. This should be considered as expert-only option, and shouldn't be enabled before knowing what it means exactly. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1379) |
| `spark.sql.parquet.variant.annotateLogicalType.enabled` | boolean | `true` | 4.1.0 | When enabled, Spark annotates the variant groups written to Parquet as the parquet variant logical type. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1603) |
| `spark.sql.parquet.writeLegacyFormat` | boolean | `false` | 1.6.0 | If true, data will be written in a way of Spark 1.4 and earlier. For example, decimal values will be written in Apache Parquet's fixed-length byte array format, which other systems such as Apache Hive and Apache Impala use. If false, the newer format in Parquet will be used. For example, decimals will be written in int-based format. If Parquet output is intended for use with systems that do not support this newer format, set to true. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1518) |

### `spark.sql.parser.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.parser.eagerEvalOfUnresolvedInlineTable` | boolean | `true` | — | Controls whether we optimize the ASTree that gets generated when parsing VALUES lists (UnresolvedInlineTable) by eagerly evaluating it in the AST Builder. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1288) |
| `spark.sql.parser.escapedStringLiterals` | boolean | `false` | 2.2.1 | When true, string literals (including regex patterns) remain escaped in our SQL parser. The default is false since Spark 2.0. Setting it to true can restore the behavior prior to Spark 2.0. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1295) |
| `spark.sql.parser.manageParserCaches` | boolean | `false` | 4.1.0 | When true, we install our own ANTLR caches to manage memory usage. When false, we use the \|default ANTLR caches. Dependency for \|`spark.sql.parser.parserDfaCacheFlushThreshold`. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1351) |
| `spark.sql.parser.parserDfaCacheFlushRatio` | double | `-1.0` | 4.1.0 | Like `spark.sql.parser.parserDfaCacheFlushThreshold`, but uses a threshold that is a \|linear function of the memory allocated to the driver process. Represents the percentage \|of the driver memory that the DFA cache can consume before it is flushed. \| \|Estimates the memory used by the DFA cache, assuming each state consumes \|`AbstractParser.BYTES_PER_DFA_STATE` bytes. If this value exceeds the product of the \|driver memory with the config value (interpreted as a percentage), the cache is flushed. \| \|Active values should be in the range 0-100, and a negative value disables the feature. \|If both this config and `spark.sql.parser.parserDfaCacheFlushThreshold` are set, the \|cache is flushed if either condition is met. \|Requires `spark.sql.parser.manageParserCaches` to be true to take effect. \| | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1305) |
| `spark.sql.parser.parserDfaCacheFlushThreshold` | int | `-1` | 4.1.0 | When positive, release ANTLR caches after parsing a SQL query when the number of states \|in the DFA cache exceeds the value of the config. DFA states empirically consume about \|`AbstractParser.BYTES_PER_DFA_STATE` bytes of memory each. \| \|ANTLR parsers retain a DFA cache designed to speed up parsing future input. However, \|there is no limit to how large this cache can become. Parsing large SQL statements can \|lead to an accumulation of objects in the cache that are unlikely to be reused, causing \|high GC overhead and eventually OOMs. \| \|If this config is set to a negative value, it is ignored. \|If both this config and `spark.sql.parser.parserDfaCacheFlushRatio` are set, the \|cache is flushed if either condition is met. \|Requires `spark.sql.parser.manageParserCaches` to be true to take effect. \| \|Can significantly slow down parsing in exchange for better memory stability. \| | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1327) |
| `spark.sql.parser.quotedRegexColumnNames` | boolean | `false` | 2.3.0 | When true, quoted Identifiers (using backticks) in SELECT statement are interpreted as regular expressions. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3822) |

### `spark.sql.pipelines.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.pipelines.event.queue.capacity` | int | `1000` | 4.1.0 | Capacity of the event queue used in pipelined execution. When the queue is full, non-terminal FlowProgressEvents will be dropped. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L6561) |
| `spark.sql.pipelines.execution.maxConcurrentFlows` | int | `16` | 4.1.0 | Max number of flows to execute at once. Used to tune performance for triggered pipelines. Has no effect on continuous pipelines. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L6530) |
| `spark.sql.pipelines.execution.streamstate.pollingInterval` | time | `1` | 4.1.0 | Interval in seconds at which the stream state is polled for changes. This is used to check if the stream has failed and needs to be restarted. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L6492) |
| `spark.sql.pipelines.execution.watchdog.maxRetryTime` | time | `3600` | 4.1.0 | Maximum time interval in seconds at which flows will be restarted. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L6519) |
| `spark.sql.pipelines.execution.watchdog.minRetryTime` | time | `5` | 4.1.0 | Initial duration in seconds between the time when we notice a flow has failed and when we try to restart the flow. The interval between flow restarts doubles with every stream failure up to the maximum value set in `pipelines.execution.watchdog.maxRetryTime`. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L6504) |
| `spark.sql.pipelines.maxFlowRetryAttempts` | int | `2` | 4.1.0 | Maximum number of times a flow can be retried | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L6553) |
| `spark.sql.pipelines.timeoutMsForTerminationJoinAndLock` | time | `60 * 60 * 1000` | 4.1.0 | Timeout in milliseconds to grab a lock for stopping update - default is 1hr. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L6543) |

### `spark.sql.pivotMaxValues.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.pivotMaxValues` | int | `10000` | 1.6.0 | When doing a pivot without specifying values for the pivot column this is the maximum number of (distinct) values that will be collected without error. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2300) |

### `spark.sql.planChangeLog.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.planChangeLog.batches` | string | _(optional)_ | 3.1.0 | Configures a list of batches for logging plan changes, in which the batches are specified by their batch names and separated by comma. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L528) |
| `spark.sql.planChangeLog.level` | enum | `Level.TRACE` | 3.1.0 | Configures the log level for logging the change from the original plan to the new plan after a rule or batch is applied. The value can be ${VALID_LOG_LEVELS.mkString()}. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L511) |
| `spark.sql.planChangeLog.rules` | string | _(optional)_ | 3.1.0 | Configures a list of rules for logging plan changes, in which the rules are specified by their rule names and separated by comma. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L520) |

### `spark.sql.planChangeValidation.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.planChangeValidation` | boolean | `Utils.isTesting` | 3.4.0 | If true, Spark will validate all the plan changes made by analyzer/optimizer and other catalyst rules, to make sure every rule returns a valid plan | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L536) |

### `spark.sql.planner.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.planner.pythonExecution.memory` | bytes | _(optional)_ | 4.0.0 | Specifies the memory allocation for executing Python code in Spark driver, in MiB. When set, it caps the memory for Python execution to the specified amount. If not set, Spark will not limit Python's memory usage and it is up to the application to avoid exceeding the overhead memory space shared with other non-JVM processes. Note: Windows does not support resource limiting and actual resource is not limited on MacOS. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4259) |

### `spark.sql.preserveCharVarcharTypeInfo.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.preserveCharVarcharTypeInfo` | boolean | `false` | 4.0.0 | When true, Spark does not replace CHAR/VARCHAR types the STRING type, which is the default behavior of Spark 3.0 and earlier versions. This means the length checks for CHAR/VARCHAR types is enforced and CHAR type is also properly padded. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5866) |

### `spark.sql.prioritizeOrdinalResolutionInSort.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.prioritizeOrdinalResolutionInSort.enabled` | boolean | `true` | 4.1.0 | When set to true, we prioritize ordinal resolution in Sort over other expressions. Otherwise, no order is enforced. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L6594) |

### `spark.sql.pyspark.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.pyspark.inferNestedDictAsStruct.enabled` | boolean | `false` | 3.3.0 | PySpark's SparkSession.createDataFrame infers the nested dict as a map by default. When it set to true, it infers the nested dict as a struct. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5942) |
| `spark.sql.pyspark.jvmStacktrace.enabled` | boolean | `Utils.isTesting` | 3.0.0 | When true, it shows the JVM stacktrace in the user-facing PySpark exception together with Python stacktrace. By default, it is disabled to hide JVM stacktrace and shows a Python-friendly exception only. Note that this is independent from log level settings. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3902) |
| `spark.sql.pyspark.legacy.inferArrayTypeFromFirstElement.enabled` | boolean | `false` | 3.4.0 | PySpark's SparkSession.createDataFrame infers the element type of an array from all values in the array by default. If this config is set to true, it restores the legacy behavior of only inferring the type from the first array element. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5958) |
| `spark.sql.pyspark.legacy.inferMapTypeFromFirstPair.enabled` | boolean | `false` | 4.0.0 | PySpark's SparkSession.createDataFrame infers the key/value types of a map from all pairs in the map by default. If this config is set to true, it restores the legacy behavior of only inferring the type from the first non-null pair. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5968) |
| `spark.sql.pyspark.plotting.max_rows` | int | `1000` | 4.0.0 | The visual limit on plots. If set to 1000 for top-n-based plots (pie, bar, barh), the first 1000 data points will be used for plotting. For sampled-based plots (scatter, area, line), 1000 data points will be randomly sampled. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3973) |
| `spark.sql.pyspark.udf.profiler` | string | _(optional)_ | 4.0.0 | Configure the Python/Pandas UDF profiler by enabling or disabling it with the option to choose between "perf" and "memory" types, or unsetting the config disables the profiler. This is disabled by default. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3913) |
| `spark.sql.pyspark.worker.logging.enabled` | boolean | `false` | 4.1.0 | When set to true, this configuration enables comprehensive logging within Python worker processes that execute User-Defined Functions (UDFs), User-Defined Table Functions (UDTFs), and other Python-based operations in Spark SQL. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3964) |

### `spark.sql.python.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.python.filterPushdown.enabled` | boolean | `false` | 4.1.0 | When true, enable filter pushdown to Python datasource, at the cost of running Python worker one additional time during planning. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5478) |

### `spark.sql.readSideCharPadding.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.readSideCharPadding` | boolean | `true` | 3.4.0 | When true, Spark applies string padding when reading CHAR type columns/fields, in addition to the write-side padding. This config is true by default to better enforce CHAR type semantic in cases such as external tables. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5884) |

### `spark.sql.redaction.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.redaction.options.regex` | unknown | `"(?i)url".r` | 2.2.2 | Regex to decide which keys in a Spark SQL command's options map contain sensitive information. The values of options whose names that match this regex will be redacted in the explain output. This redaction is applied on top of the global redaction configuration defined by ${SECRET_REDACTION_PATTERN.key}. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4337) |
| `spark.sql.redaction.string.regex` | fallback | → `org.apache.spark.internal.config.STRING_REDACTION_PATTERN` | 2.3.0 | Regex to decide which parts of strings produced by Spark contain sensitive information. When this regex matches a string part, that string part is replaced by a dummy value. This is currently used to redact the output of SQL explain commands. When this conf is not set, the value from `spark.redaction.string.regex` is used. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4347) |

### `spark.sql.repl.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.repl.eagerEval.enabled` | boolean | `false` | 2.4.0 | Enables eager evaluation or not. When true, the top K rows of Dataset will be displayed if and only if the REPL supports the eager evaluation. Currently, the eager evaluation is supported in PySpark and SparkR. In PySpark, for the notebooks like Jupyter, the HTML table (generated by _repr_html_) will be returned. For plain Python REPL, the returned outputs are formatted like dataframe.show(). In SparkR, the returned outputs are showed similar to R data.frame would. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4898) |
| `spark.sql.repl.eagerEval.maxNumRows` | int | `20` | 2.4.0 | The max number of rows that are returned by eager evaluation. This only takes effect when ${REPL_EAGER_EVAL_ENABLED.key} is set to true. The valid range of this config is from 0 to (Int.MaxValue - 1), so the invalid config like negative and greater than (Int.MaxValue - 1) will be normalized to 0 and (Int.MaxValue - 1). | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4909) |
| `spark.sql.repl.eagerEval.truncate` | int | `20` | 2.4.0 | The max number of characters for each cell that is returned by eager evaluation. This only takes effect when ${REPL_EAGER_EVAL_ENABLED.key} is set to true. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4918) |

### `spark.sql.requireAllClusterKeysForCoPartition.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.requireAllClusterKeysForCoPartition` | boolean | `true` | 3.3.0 | When true, the planner requires all the clustering keys as the hash partition keys of the children, to eliminate the shuffles for the operator that needs its children to be co-partitioned, such as JOIN node. This is to avoid data skews which can lead to significant performance regression if shuffles are eliminated. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L801) |

### `spark.sql.requireAllClusterKeysForDistribution.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.requireAllClusterKeysForDistribution` | boolean | `false` | 3.3.0 | When true, the planner requires all the clustering keys as the partition keys (with same ordering) of the children, to eliminate the shuffle for the operator that requires its children be clustered distributed, such as AGGREGATE and WINDOW node. This is to avoid data skews which can lead to significant performance regression if shuffle is eliminated. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L812) |

### `spark.sql.retainGroupColumns.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.retainGroupColumns` | boolean | `true` | 1.4.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2294) |

### `spark.sql.runCollationTypeCastsBeforeAliasAssignment.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.runCollationTypeCastsBeforeAliasAssignment.enabled` | boolean | `true` | 4.1.0 | When set to true, rules like ResolveAliases or ResolveAggregateFunctions will run CollationTypeCasts before alias assignment. This is necessary for correct alias generation. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L6480) |

### `spark.sql.runSQLOnFiles.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.runSQLOnFiles` | boolean | `true` | 1.6.0 | When true, we could use `datasource`.`path` as table in SQL query. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2314) |

### `spark.sql.scriptTransformation.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.scriptTransformation.exitTimeoutInSeconds` | time | `10` | 3.0.0 | Timeout for executor to wait for the termination of transformation script when EOF. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5742) |

### `spark.sql.scripting.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.scripting.continueHandlerEnabled` | boolean | `false` | 4.1.0 | EXPERIMENTAL FEATURE/WORK IN PROGRESS: SQL Scripting CONTINUE HANDLER feature is under development and still not working as intended. This feature switch is intended to be used internally for development and testing, not by end users. YOU ARE ADVISED AGAINST USING THIS FEATURE AS ITS NOT FINISHED. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4365) |
| `spark.sql.scripting.enabled` | boolean | `true` | 4.0.0 | SQL Scripting feature is under development and its use should be done under this feature flag. SQL Scripting enables users to write procedural SQL including control flow and error handling. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4356) |

### `spark.sql.selfJoinAutoResolveAmbiguity.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.selfJoinAutoResolveAmbiguity` | boolean | `true` | 1.4.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2270) |

### `spark.sql.session.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.session.localRelationBatchOfChunksSizeBytes` | long | `1 * 1024 * 1024 * 1024L` | 4.1.0 | Limit on how much memory the client can use when uploading a local relation to the server. The client collects multiple local relation chunks into a single batch in memory until the limit is reached, then uploads the batch to the server. This helps reduce memory pressure on the client when dealing with very large local relations because the client does not have to materialize all chunks in memory. Limits the spark.sql.session.localRelationChunkSizeBytes, a minimum of the two confs is used to determine the chunk size. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L6155) |
| `spark.sql.session.localRelationCacheThreshold` | int | `1024 * 1024` | 3.5.0 | The threshold for the size in bytes of local relations to be cached at the driver side after serialization. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L6104) |
| `spark.sql.session.localRelationChunkSizeBytes` | long | `16 * 1024 * 1024L` | 4.1.0 | The chunk size in bytes when splitting ChunkedCachedLocalRelation.data into batches. A new chunk is created when either spark.sql.session.localRelationChunkSizeBytes or spark.sql.session.localRelationChunkSizeRows is reached. Limited by the spark.sql.session.localRelationBatchOfChunksSizeBytes, a minimum of the two confs is used to determine the chunk size. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L6124) |
| `spark.sql.session.localRelationChunkSizeLimit` | bytes | `2000MB` | 4.1.0 | Limit on how large a single chunk of a ChunkedCachedLocalRelation.data can be in bytes. If the limit is exceeded, an exception is thrown. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L6137) |
| `spark.sql.session.localRelationChunkSizeRows` | int | `10000` | 4.1.0 | The chunk size in number of rows when splitting ChunkedCachedLocalRelation.data into batches. A new chunk is created when either spark.sql.session.localRelationChunkSizeBytes or spark.sql.session.localRelationChunkSizeRows is reached. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L6113) |
| `spark.sql.session.localRelationSizeLimit` | bytes | `3GB` | 4.1.0 | Limit on how large ChunkedCachedLocalRelation.data can be in bytes.If the limit is exceeded, an exception is thrown. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L6146) |
| `spark.sql.session.timeZone` | string | `() => TimeZone.getDefault.getID` | 2.2.0 | The ID of session local timezone in the format of either region-based zone IDs or zone offsets. Region IDs must have the form 'area/city', such as 'America/Los_Angeles'. Zone offsets must be in the format '(+\|-)HH', '(+\|-)HH:mm' or '(+\|-)HH:mm:ss', e.g '-08', '+01:00' or '-13:33:33'. Also 'UTC' and 'Z' are supported as aliases of '+00:00'. Other short names are not recommended to use because they can be ambiguous. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3668) |

### `spark.sql.sessionWindow.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.sessionWindow.buffer.in.memory.threshold` | int | `4096` | 3.2.0 | Threshold for number of windows guaranteed to be held in memory by the session window operator. Note that the buffer is used only for the query Spark cannot apply aggregations on determining session window. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3714) |
| `spark.sql.sessionWindow.buffer.spill.size.threshold` | fallback | → `SHUFFLE_SPILL_MAX_SIZE_FORCE_SPILL_THRESHOLD` | 4.1.0 | Threshold for size of rows to be spilled by window operator. Note that the buffer is used only for the query Spark cannot apply aggregations on determining session window. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3724) |
| `spark.sql.sessionWindow.buffer.spill.threshold` | int | `SHUFFLE_SPILL_NUM_ELEMENTS_FORCE_SPILL_THRESHOLD.defaultValue.get` | 3.2.0 | Threshold for number of rows to be spilled by window operator. Note that the buffer is used only for the query Spark cannot apply aggregations on determining session window. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3733) |

### `spark.sql.shuffle.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.shuffle.orderIndependentChecksum.enableFullRetryOnMismatch` | boolean | `false` | 4.1.0 | Whether to retry all tasks of a consumer stage when we detect checksum mismatches with its producer stages. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L913) |
| `spark.sql.shuffle.orderIndependentChecksum.enabled` | boolean | `false` | 4.1.0 | Whether to calculate order independent checksum for the shuffle data or not. If enabled, Spark will calculate a checksum that is independent of the input row order for each mapper and returns the checksums from executors to driver. This is different from the checksum computed when spark.shuffle.checksum.enabled is enabled which is sensitive to shuffle data ordering to detect file corruption. While this checksum will be the same even if the shuffle row order changes and it is used to detect whether different task attempts of the same partition produce different output data or not (same set of keyValue pairs). In case the output data has changed across retries, Spark will need to retry all tasks of the consumer stages to avoid correctness issues. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L898) |
| `spark.sql.shuffle.partitions` | int | `200` | 1.1.0 | The default number of partitions to use when shuffling data for joins or aggregations. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L890) |

### `spark.sql.shuffleDependency.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.shuffleDependency.fileCleanup.enabled` | boolean | `Utils.isTesting` | 4.0.0 | (Deprecated since Spark 4.1, please set 'spark.sql.connect.shuffleDependency.fileCleanup.enabled'.) | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3751) |
| `spark.sql.shuffleDependency.skipMigration.enabled` | boolean | `Utils.isTesting` | 4.0.0 | When enabled, shuffle dependencies for a Spark Connect SQL execution are marked at the end of the execution, and they will not be migrated during decommissions. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3743) |

### `spark.sql.shuffledHashJoinFactor.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.shuffledHashJoinFactor` | int | `3` | 3.3.0 | The shuffle hash join can be selected if the data size of small side multiplied by this factor is still smaller than the large side. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L846) |

### `spark.sql.sort.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.sort.enableRadixSort` | boolean | `true` | 2.0.0 | When true, enable use of radix sort when possible. Radix sort is much faster but requires additional memory to be reserved up-front. The memory overhead may be significant when sorting very small rows (up to 50% more in this case). | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L830) |

### `spark.sql.sortMergeJoinExec.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.sortMergeJoinExec.buffer.in.memory.threshold` | int | `ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH` | 2.2.1 | Threshold for number of rows guaranteed to be held in memory by the sort merge join operator | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3775) |
| `spark.sql.sortMergeJoinExec.buffer.spill.size.threshold` | fallback | → `SHUFFLE_SPILL_MAX_SIZE_FORCE_SPILL_THRESHOLD` | 4.1.0 | Threshold for size of rows to be spilled by sort merge join operator | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3792) |
| `spark.sql.sortMergeJoinExec.buffer.spill.threshold` | int | `SHUFFLE_SPILL_NUM_ELEMENTS_FORCE_SPILL_THRESHOLD.defaultValue.get` | 2.2.0 | Threshold for number of rows to be spilled by sort merge join operator | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3784) |

### `spark.sql.sources.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.sources.binaryFile.maxLength` | int | `Int.MaxValue` | 3.0.0 | The max length of a file that can be read by the binary file data source. Spark will fail fast and not attempt to read the file if its length exceeds this value. The theoretical max is Int.MaxValue, though VMs might implement a smaller max. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5224) |
| `spark.sql.sources.bucketing.autoBucketedScan.enabled` | boolean | `true` | 3.1.0 | When true, decide whether to do bucketed scan on input tables based on query plan automatically. Do not use bucketed scan if 1. query does not have operators to utilize bucketing (e.g. join, group-by, etc), or 2. there's an exchange operator between these operators and table scan. Note when '${BUCKETING_ENABLED.key}' is set to false, this configuration does not take any effect. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2122) |
| `spark.sql.sources.bucketing.enabled` | boolean | `true` | 2.0.0 | When false, we will treat bucketed table as normal table | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2019) |
| `spark.sql.sources.bucketing.maxBuckets` | int | `100000` | 2.4.0 | The maximum number of buckets allowed. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2114) |
| `spark.sql.sources.commitProtocolClass` | string | `org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol` | 2.1.1 |  | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2215) |
| `spark.sql.sources.default` | string | `parquet` | 1.3.0 | The default data source to use in input/output. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1948) |
| `spark.sql.sources.fileCompressionFactor` | double | `1.0` | 2.3.1 | When estimating the output data size of a table scan, multiply the file size with this factor as the estimated data size, in case the data is compressed in the file and lead to a heavily underestimated result. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1361) |
| `spark.sql.sources.ignoreDataLocality` | boolean | `false` | 3.0.0 | If true, Spark will not fetch the block locations for each file on listing files. This speeds up file listing, but the scheduler cannot schedule tasks to take advantage of data locality. It can be particularly useful if data is read from a remote cluster so the scheduler could never take advantage of locality anyway. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2246) |
| `spark.sql.sources.outputCommitterClass` | string | _(optional)_ | 1.4.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2206) |
| `spark.sql.sources.parallelPartitionDiscovery.parallelism` | int | `10000` | 2.1.1 | The number of parallelism to list a collection of path recursively, Set the number to prevent file listing from generating too many tasks. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2237) |
| `spark.sql.sources.parallelPartitionDiscovery.threshold` | int | `32` | 1.5.0 | The maximum number of paths allowed for listing files at driver side. If the number of detected paths exceeds this value during partition discovery, it tries to list the files with another Spark distributed job. This configuration is effective only when using file-based sources such as Parquet, JSON and ORC. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2225) |
| `spark.sql.sources.partitionColumnTypeInference.enabled` | boolean | `true` | 1.5.0 | When true, automatically infer the data types for partitioned columns. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2013) |
| `spark.sql.sources.partitionOverwriteMode` | enum | `PartitionOverwriteMode.STATIC` | 2.3.0 | When INSERT OVERWRITE a partitioned data source table, we currently support 2 modes: static and dynamic. In static mode, Spark deletes all the partitions that match the partition specification(e.g. PARTITION(a=1,b)) in the INSERT statement, before overwriting. In dynamic mode, Spark doesn't delete partitions ahead, and only overwrite those partitions that have data written into it at runtime. By default we use static mode to keep the same behavior of Spark prior to 2.3. Note that this config doesn't affect Hive serde tables, as they are always overwritten with dynamic mode. This can also be set as an output option for a data source using key partitionOverwriteMode (which takes precedence over this setting), e.g. dataframe.write.option("partitionOverwriteMode", "dynamic").save(path). | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4490) |
| `spark.sql.sources.useListFilesFileSystemList` | string | `s3a` | 4.0.0 | A comma-separated list of file system schemes to use FileSystem.listFiles API for a single root path listing | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2258) |
| `spark.sql.sources.useV1SourceList` | string | `avro,csv,json,kafka,orc,parquet,text` | 3.0.0 | A comma-separated list of data source short names or fully qualified data source implementation class names for which Data Source V2 code path is disabled. These data sources will fallback to Data Source V1 code path. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4426) |
| `spark.sql.sources.v2.bucketing.allowCompatibleTransforms.enabled` | boolean | `false` | 4.0.0 | Whether to allow storage-partition join in the case where the partition transforms are compatible but not identical. This config requires both ${V2_BUCKETING_ENABLED.key} and ${V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key} to be enabled and ${V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key} to be disabled. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2082) |
| `spark.sql.sources.v2.bucketing.allowJoinKeysSubsetOfPartitionKeys.enabled` | boolean | `false` | 4.0.0 | Whether to allow storage-partition join in the case where join keys are a subset of the partition keys of the source tables. At planning time, Spark will group the partitions by only those keys that are in the join keys. This is currently enabled only if ${REQUIRE_ALL_CLUSTER_KEYS_FOR_DISTRIBUTION.key} is false. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2070) |
| `spark.sql.sources.v2.bucketing.enabled` | boolean | `true` | 3.3.0 | Similar to ${BUCKETING_ENABLED.key}, this config is used to enable bucketing for V2 data sources. When turned on, Spark will recognize the specific distribution reported by a V2 data source through SupportsReportPartitioning, and will try to avoid shuffle if necessary. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2025) |
| `spark.sql.sources.v2.bucketing.partiallyClusteredDistribution.enabled` | boolean | `false` | 3.4.0 | During a storage-partitioned join, whether to allow input partitions to be partially clustered, when both sides of the join are of KeyGroupedPartitioning. At planning time, Spark will pick the side with less data size based on table statistics, group and replicate them to match the other side. This is an optimization on skew join and can help to reduce data skewness when certain partitions are assigned large amount of data. This config requires both ${V2_BUCKETING_ENABLED.key} and ${V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key} to be enabled | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2047) |
| `spark.sql.sources.v2.bucketing.partition.filter.enabled` | boolean | `false` | 4.0.0 | Whether to filter partitions when running storage-partition join. When enabled, partitions without matches on the other side can be omitted for scanning, if allowed by the join type. This config requires both ${V2_BUCKETING_ENABLED.key} and ${V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key} to be enabled. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2094) |
| `spark.sql.sources.v2.bucketing.pushPartValues.enabled` | boolean | `true` | 3.4.0 | Whether to pushdown common partition values when ${V2_BUCKETING_ENABLED.key} is enabled. When turned on, if both sides of a join are of KeyGroupedPartitioning and if they share compatible partition keys, even if they don't have the exact same partition values, Spark will calculate a superset of partition values and pushdown that info to scan nodes, which will use empty partitions for the missing partition values on either side. This could help to eliminate unnecessary shuffles | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2035) |
| `spark.sql.sources.v2.bucketing.shuffle.enabled` | boolean | `false` | 4.0.0 | During a storage-partitioned join, whether to allow to shuffle only one side. When only one side is KeyGroupedPartitioning, if the conditions are met, spark will only shuffle the other side. This optimization will reduce the amount of data that needs to be shuffle. This config requires ${V2_BUCKETING_ENABLED.key} to be enabled | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2060) |
| `spark.sql.sources.v2.bucketing.sorting.enabled` | boolean | `false` | 4.0.0 | When turned on, Spark will recognize the specific distribution reported by a V2 data source through SupportsReportPartitioning, and will try to avoid a shuffle if possible when sorting by those columns. This config requires ${V2_BUCKETING_ENABLED.key} to be enabled. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2105) |
| `spark.sql.sources.validatePartitionColumns` | boolean | `true` | 3.0.0 | When this option is set to true, partition column values will be validated with user-specified schema. If the validation fails, a runtime exception is thrown. When this option is set to false, the partition column value will be converted to null if it can not be casted to corresponding user-specified schema. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4390) |

### `spark.sql.stableDerivedColumnAlias.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.stableDerivedColumnAlias.enabled` | boolean | `false` | 3.5.0 | Enable deriving of stable column aliases from the lexer tree instead of parse tree and form them via pretty SQL print. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L6095) |

### `spark.sql.stackTracesInDataFrameContext.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.stackTracesInDataFrameContext` | int | `1` | 4.0.0 | The number of non-Spark stack traces in the captured DataFrame query context. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L6346) |

### `spark.sql.statistics.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.statistics.fallBackToHdfs` | boolean | `false` | 2.0.0 | When true, it will fall back to HDFS if the table statistics are not available from table metadata. This is useful in determining if a table is small enough to use broadcast joins. This flag is effective only for non-partitioned Hive tables. For non-partitioned data source tables, it will be automatically recalculated if table statistics are not available. For partitioned data source and partitioned Hive tables, It is '${DEFAULT_SIZE_IN_BYTES.key}' if table statistics are not available. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3533) |
| `spark.sql.statistics.histogram.enabled` | boolean | `false` | 2.3.0 | Generates histograms when computing column statistics if enabled. Histograms can provide better estimation accuracy. Currently, Spark only supports equi-height histogram. Note that collecting histograms takes extra cost. For example, collecting column statistics usually takes only one table scan, but generating equi-height histogram will cause an extra table scan. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3554) |
| `spark.sql.statistics.histogram.numBins` | int | `254` | 2.3.0 | The number of bins when generating histograms. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3565) |
| `spark.sql.statistics.ndv.maxError` | double | `0.05` | 2.1.1 | The maximum relative standard deviation allowed in HyperLogLog++ algorithm when generating column level statistics. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3545) |
| `spark.sql.statistics.parallelFileListingInStatsComputation.enabled` | boolean | `true` | 2.4.1 | When true, SQL commands use parallel file listing, as opposed to single thread listing. This usually speeds up commands that need to list many directories. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3514) |
| `spark.sql.statistics.percentile.accuracy` | int | `10000` | 2.3.0 | Accuracy of percentile approximation when generating equi-height histograms. Larger value means better accuracy. The relative error can be deduced by 1.0 / PERCENTILE_ACCURACY. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3574) |
| `spark.sql.statistics.size.autoUpdate.enabled` | boolean | `false` | 2.3.0 | Enables automatic update for table size once table's data is changed. Note that if the total number of files of the table is very large, this can be expensive and slow down data change commands. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3584) |
| `spark.sql.statistics.updatePartitionStatsInAnalyzeTable.enabled` | boolean | `false` | 4.0.0 | When this config is enabled, Spark will also update partition statistics in analyze table command (i.e., ANALYZE TABLE .. COMPUTE STATISTICS [NOSCAN]). Note the command will also become more expensive. When this config is disabled, Spark will only update table level statistics. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3593) |

### `spark.sql.storeAssignmentPolicy.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.storeAssignmentPolicy` | enum | `StoreAssignmentPolicy.ANSI` | 3.0.0 | When inserting a value into a column with different data type, Spark will perform type coercion. Currently, we support 3 policies for the type coercion rules: ANSI, legacy and strict. With ANSI policy, Spark performs the type coercion as per ANSI SQL. In practice, the behavior is mostly the same as PostgreSQL. It disallows certain unreasonable type conversions such as converting `string` to `int` or `double` to `boolean`. With legacy policy, Spark allows the type coercion as long as it is a valid `Cast`, which is very loose. e.g. converting `string` to `int` or `double` to `boolean` is allowed. It is also the only behavior in Spark 2.x and it is compatible with Hive. With strict policy, Spark doesn't allow any possible precision loss or data truncation in type coercion, e.g. converting `double` to `int` or `decimal` to `double` is not allowed. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4511) |

### `spark.sql.streaming.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.streaming.aggregation.stateFormatVersion` | int | `2` | 2.4.0 | State format version used by streaming aggregation operations in a streaming query. State between versions are tend to be incompatible, so state format version shouldn't be modified after running. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2897) |
| `spark.sql.streaming.asyncLogPurge.enabled` | boolean | `true` | 3.4.0 | When true, purging the offset log and commit log of old entries will be done asynchronously. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3044) |
| `spark.sql.streaming.checkpoint.escapedPathCheck.enabled` | boolean | `true` | 3.0.0 | Whether to detect a streaming query may pick up an incorrect checkpoint path due to SPARK-26824. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3486) |
| `spark.sql.streaming.checkpoint.fileChecksum.enabled` | boolean | `true` | 4.1.0 | When true, checksum would be generated and verified for checkpoint files. This is used to detect file corruption. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3495) |
| `spark.sql.streaming.checkpoint.fileChecksum.skipCreationIfFileMissingChecksum` | boolean | `true` | 4.1.0 | When true, if a microbatch is retried, if a file already exists but its checksum file does not exist, the file checksum will not be created. This is useful for compatibility with files created before file checksums were enabled. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3504) |
| `spark.sql.streaming.checkpoint.renamedFileCheck.enabled` | boolean | `false` | 3.4.0 | When true, Spark will validate if renamed checkpoint file exists. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2873) |
| `spark.sql.streaming.checkpointFileManagerClass` | string | `true` | 2.4.0 | The class used to write checkpoint files atomically. This class must be a subclass of the interface CheckpointFileManager. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3478) |
| `spark.sql.streaming.checkpointLocation` | string | _(optional)_ | 2.0.0 | The default location for storing checkpoint data for streaming queries. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2724) |
| `spark.sql.streaming.commitProtocolClass` | string | `org.apache.spark.sql.execution.streaming.ManifestFileCommitProtocol` | 2.1.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3273) |
| `spark.sql.streaming.continuous.epochBacklogQueueSize` | int | `10000` | 3.0.0 | The max number of entries to be stored in queue to wait for late epochs. If this parameter is exceeded by the size of the queue, stream will stop with an error. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4401) |
| `spark.sql.streaming.continuous.executorPollIntervalMs` | time | `100` | 2.3.0 | The interval at which continuous execution readers will poll to check whether the epoch has advanced on the driver. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4418) |
| `spark.sql.streaming.continuous.executorQueueSize` | int | `1024` | 2.3.0 | The size (measured in number of rows) of the queue used in continuous execution to buffer the results of a ContinuousDataReader. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4409) |
| `spark.sql.streaming.disabledV2MicroBatchReaders` | string | — | 2.4.0 | A comma-separated list of fully qualified data source register class names for which MicroBatchReadSupport is disabled. Reads from these sources will fall back to the V1 Sources. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4451) |
| `spark.sql.streaming.disabledV2Writers` | string | — | 2.3.1 | A comma-separated list of fully qualified data source register class names for which StreamWriteSupport is disabled. Writes to these sources will fall back to the V1 Sinks. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4443) |
| `spark.sql.streaming.fileSink.log.cleanupDelay` | time | `TimeUnit.MINUTES.toMillis(10)` | 2.0.0 | How long that a file is guaranteed to be visible for all readers. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3374) |
| `spark.sql.streaming.fileSink.log.compactInterval` | int | `10` | 2.0.0 | Number of log files after which all the previous files are compacted into the next log file. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3365) |
| `spark.sql.streaming.fileSink.log.deletion` | boolean | `true` | 2.0.0 | Whether to delete the expired log files in file stream sink. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3357) |
| `spark.sql.streaming.fileSource.cleaner.numThreads` | int | `1` | 3.0.0 | Number of threads used in the file source completed file cleaner. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3416) |
| `spark.sql.streaming.fileSource.log.cleanupDelay` | time | `TimeUnit.MINUTES.toMillis(10)` | 2.0.1 | How long in milliseconds a file is guaranteed to be visible for all readers. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3398) |
| `spark.sql.streaming.fileSource.log.compactInterval` | int | `10` | 2.0.1 | Number of log files after which all the previous files are compacted into the next log file. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3389) |
| `spark.sql.streaming.fileSource.log.deletion` | boolean | `true` | 2.0.1 | Whether to delete the expired log files in file stream source. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3381) |
| `spark.sql.streaming.fileSource.schema.forceNullable` | boolean | `true` | 3.0.0 | When true, force the schema of streaming file source to be nullable (including all the fields). Otherwise, the schema might not be compatible with actual data, which leads to corruptions. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3406) |
| `spark.sql.streaming.fileStreamSink.ignoreMetadata` | boolean | `false` | 3.2.0 | If this is enabled, when Spark reads from the results of a streaming query written by `FileStreamSink`, Spark will ignore the metadata log and treat it as normal path to read, e.g. listing files using HDFS APIs. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3018) |
| `spark.sql.streaming.flatMapGroupsWithState.skipEmittingInitialStateKeys` | boolean | `false` | 4.0.0 | When true, the flatMapGroupsWithState operation in a streaming query will not emit results for the initial state keys of each group. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2716) |
| `spark.sql.streaming.flatMapGroupsWithState.stateFormatVersion` | int | `2` | 2.4.0 | State format version used by flatMapGroupsWithState operation in a streaming query | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2707) |
| `spark.sql.streaming.forceDeleteTempCheckpointLocation` | boolean | `false` | 3.0.0 | When true, enable temporary checkpoint locations force delete. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2731) |
| `spark.sql.streaming.internal.stateStore.partitions` | int | _(optional)_ | 4.1.0 | WARN: This config is used internally and is not intended to be user-facing. This config can be removed without support of compatibility in any time. DO NOT USE THIS CONFIG DIRECTLY AND USE THE CONFIG `spark.sql.shuffle.partitions`. The default number of partitions to use when shuffling data for stateful operations. If not specified, this config picks up the value of `spark.sql.shuffle.partitions`. Note: For structured streaming, this configuration cannot be changed between query restarts from the same checkpoint location. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2690) |
| `spark.sql.streaming.join.stateFormatVersion` | int | `2` | 3.0.0 | State format version used by streaming join operations in a streaming query. State between versions are tend to be incompatible, so state format version shouldn't be modified after running. Version 3 uses a single state store with virtual column families instead of four stores and is only supported with RocksDB. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2918) |
| `spark.sql.streaming.kafka.useDeprecatedOffsetFetching` | boolean | `false` | 3.1.0 | When true, the deprecated Consumer based offset fetching used which could cause infinite wait in Spark queries. Such cases query restart is the only workaround. For further details please see Offset Fetching chapter of Structured Streaming Kafka Integration Guide. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2959) |
| `spark.sql.streaming.maxBatchesToRetainInMemory` | int | `2` | 2.4.0 | The maximum number of batches which will be retained in memory to avoid loading from files. The value adjusts a trade-off between memory usage vs cache miss: '2' covers both success and direct failure cases, '1' covers only success case, and '0' covers extreme case - disable cache to maximize memory size of executors. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2765) |
| `spark.sql.streaming.metadataCache.enabled` | boolean | `true` | — | Whether the streaming HDFSMetadataLog caches the metadata of the latest two batches. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3053) |
| `spark.sql.streaming.metricsEnabled` | boolean | `false` | 2.0.2 | Whether Dropwizard/Codahale metrics will be reported for active streaming queries. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3464) |
| `spark.sql.streaming.minBatchesToRetain` | int | `100` | 2.1.1 | The minimum number of batches that must be retained and made recoverable. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2737) |
| `spark.sql.streaming.multipleWatermarkPolicy` | string | `min` | 2.4.0 | Policy to calculate the global watermark value when there are multiple watermark operators in a streaming query. The default value is 'min' which chooses the minimum watermark reported across multiple operators. Other alternative value is 'max' which chooses the maximum across multiple operators. Note: This configuration cannot be changed between query restarts from the same checkpoint location. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3280) |
| `spark.sql.streaming.noDataMicroBatches.enabled` | boolean | `true` | 2.4.1 | Whether streaming micro-batch engine will execute batches without data for eager state management for stateful streaming queries. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3455) |
| `spark.sql.streaming.noDataProgressEventInterval` | time | `10000` | 2.1.1 | How long to wait before providing query idle event when there is no data | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3447) |
| `spark.sql.streaming.numRecentProgressUpdates` | int | `100` | 2.1.1 | The number of progress updates to retain for a streaming query | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3471) |
| `spark.sql.streaming.optimizeOneRowPlan.enabled` | boolean | `false` | 4.0.0 | When true, enable OptimizeOneRowPlan rule for the case where the child is a streaming Dataset. This is a fallback flag to revert the 'incorrect' behavior, hence this configuration must not be used without understanding in depth. Use this only to quickly recover failure in existing query! | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3072) |
| `spark.sql.streaming.pollingDelay` | time | `10` | 2.0.0 | How long to delay polling new data when no data is available | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3431) |
| `spark.sql.streaming.ratioExtraSpaceAllowedInCheckpoint` | double | `0.3` | 4.0.0 | The ratio of extra space allowed for batch deletion of files when maintenance isinvoked. When value > 0, it optimizes the cost of discovering and deleting old checkpoint versions. The minimum number of stale versions we retain in checkpoint location for batch deletion is calculated by minBatchesToRetain * ratioExtraSpaceAllowedInCheckpoint. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2745) |
| `spark.sql.streaming.realTimeMode.allowlistCheck` | boolean | `true` | 4.1.0 | Whether to check all operators, sinks used in real-time mode are in the allowlist. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3089) |
| `spark.sql.streaming.realTimeMode.minBatchDuration` | time | `5000` | 4.1.0 | The minimum long-running batch duration in milliseconds for real-time mode. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3082) |
| `spark.sql.streaming.schemaInference` | boolean | `false` | 2.0.0 | Whether file-based streaming sources will infer its own schema | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3423) |
| `spark.sql.streaming.sessionWindow.merge.sessions.in.local.partition` | boolean | `false` | 3.2.0 | When true, streaming session window sorts and merge sessions in local partition prior to shuffle. This is to reduce the rows to shuffle, but only beneficial when there're lots of rows in a batch being assigned to same sessions. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2930) |
| `spark.sql.streaming.sessionWindow.stateFormatVersion` | int | `1` | 3.2.0 | State format version used by streaming session window in a streaming query. State between versions are tend to be incompatible, so state format version shouldn't be modified after running. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2939) |
| `spark.sql.streaming.stateStore.checkpointFormatVersion` | int | `1` | 4.0.0 | The version of the approach of doing state store checkpoint | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2795) |
| `spark.sql.streaming.stateStore.commitValidation.enabled` | boolean | `true` | 4.1.0 | When true, Spark will validate that all StateStore instances have committed for stateful streaming queries using foreachBatch. This helps detect cases where user-defined functions in foreachBatch (e.g., show(), limit()) don't process all partitions, which can lead to incorrect results. The validation only applies to foreachBatch sinks without global aggregates or limits. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2862) |
| `spark.sql.streaming.stateStore.compression.codec` | string | `CompressionCodec.LZ4` | 3.1.0 | The codec used to compress delta and snapshot files generated by StateStore. By default, Spark provides four codecs: lz4, lzf, snappy, and zstd. You can also use fully qualified class names to specify the codec. Default codec is lz4. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2840) |
| `spark.sql.streaming.stateStore.coordinatorReportSnapshotUploadLag` | boolean | `true` | 4.1.0 | When enabled, the state store coordinator will report state stores whose snapshot have not been uploaded for some time. See the conf snapshotLagReportInterval for the minimum time between reports, and the conf multiplierForMinVersionDiffToLog and multiplierForMinTimeDiffToLog for the logging thresholds. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2653) |
| `spark.sql.streaming.stateStore.encodingFormat` | string | `unsaferow` | 4.0.0 | The encoding format used for stateful operators to store information in the state store | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2811) |
| `spark.sql.streaming.stateStore.formatValidation.enabled` | boolean | `true` | 3.1.0 | When true, check if the data from state store is valid or not when running streaming queries. This can happen if the state store format has been changed. Note, the feature is only effective in the build-in HDFS state store provider now. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2616) |
| `spark.sql.streaming.stateStore.maintenanceInterval` | time | `TimeUnit.MINUTES.toMillis(1)` | 2.0.0 | The interval in milliseconds between triggering maintenance tasks in StateStore. The maintenance task executes background maintenance task in all the loaded store providers if they are still the active instances according to the coordinator. If not, inactive instances of store providers will be closed. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2776) |
| `spark.sql.streaming.stateStore.maintenanceProcessingTimeout` | time | `30` | 4.1.0 | Timeout in seconds to wait for maintenance to process this partition. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2578) |
| `spark.sql.streaming.stateStore.maintenanceShutdownTimeout` | time | `300` | 4.1.0 | Timeout in seconds for maintenance pool operations to complete on shutdown | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2570) |
| `spark.sql.streaming.stateStore.maxLaggingStoresToReport` | int | `5` | 4.1.0 | Maximum number of state stores the coordinator will report as trailing in snapshot uploads. Stores are selected based on the most lagging behind in snapshot version. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2677) |
| `spark.sql.streaming.stateStore.maxNumStateSchemaFiles` | int | `128` | 4.0.0 | The maximum number of StateSchemaV3 files allowed per operator | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2803) |
| `spark.sql.streaming.stateStore.maxVersionsToDeletePerMaintenance` | int | `-1` | 4.1.0 | The maximum number of versions to delete per maintenance operation. By default, this value is set to -1, which means no limit. Note that, currently this is only supported for the RocksDB state store provider. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2756) |
| `spark.sql.streaming.stateStore.minDeltasForSnapshot` | int | `10` | 2.0.0 | Minimum number of state store delta files that needs to be generated before they consolidated into snapshots. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2594) |
| `spark.sql.streaming.stateStore.multiplierForMinTimeDiffToLog` | long | `10` | 4.1.0 | Determines the time threshold for logging warnings when a state store falls behind. The coordinator logs a warning when the store's uploaded snapshot timestamp trails the current time by the configured maintenance interval, times this multiplier. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2640) |
| `spark.sql.streaming.stateStore.multiplierForMinVersionDiffToLog` | long | `5` | 4.1.0 | Determines the version threshold for logging warnings when a state store falls behind. The coordinator logs a warning when the store's uploaded snapshot version trails the query's latest version by the configured number of deltas needed to create a snapshot, times this multiplier. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2626) |
| `spark.sql.streaming.stateStore.numStateStoreInstanceMetricsToReport` | int | `5` | 4.1.0 | Number of state store instance metrics included in streaming query progress messages per stateful operator. Instance metrics are selected based on metric-specific ordering to minimize noise in the progress report. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2603) |
| `spark.sql.streaming.stateStore.numStateStoreMaintenanceThreads` | int | `Math.max(Runtime.getRuntime.availableProcessors() / 4, 1)` | — | Number of threads in the thread pool that perform clean up and snapshotting tasks for stateful streaming queries. The default value is the number of cores * 0.25 so that this thread pool doesn't take too many resources away from the query and affect performance. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2559) |
| `spark.sql.streaming.stateStore.providerClass` | string | `org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider` | 2.3.0 | The class used to manage state data in stateful streaming queries. This class must be a subclass of StateStoreProvider, and must have a zero-arg constructor. Note: For structured streaming, this configuration cannot be changed between query restarts from the same checkpoint location. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2546) |
| `spark.sql.streaming.stateStore.rocksdb.formatVersion` | int | `5` | 3.2.0 | Set the RocksDB format version. This will be stored in the checkpoint when starting a streaming query. The checkpoint will use this RocksDB format version in the entire lifetime of the query. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2885) |
| `spark.sql.streaming.stateStore.skipNullsForStreamStreamJoins.enabled` | boolean | `false` | 3.3.0 | When true, this config will skip null values in hash based stream-stream joins. The number of skipped null values will be shown as custom metric of stream join operator. If the streaming query was started with Spark 3.5 or above, please exercise caution before enabling this config since it may hide potential data loss/corruption issues. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3033) |
| `spark.sql.streaming.stateStore.snapshotLagReportInterval` | time | `TimeUnit.MINUTES.toMillis(5)` | 4.1.0 | The minimum amount of time between the state store coordinator's reports on state store instances trailing behind in snapshot uploads. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2666) |
| `spark.sql.streaming.stateStore.stateSchemaCheck` | boolean | `true` | 3.1.0 | When true, Spark will validate the state schema against schema on existing state and fail query if it's incompatible. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2586) |
| `spark.sql.streaming.stateStore.unloadOnCommit` | boolean | `false` | 4.1.0 | When true, Spark will synchronously run maintenance and then close each StateStore instance on task completion. This removes the overhead of keeping every StateStore loaded indefinitely, at the cost of having to reload each StateStore every batch. Stateful applications that are failing due to resource exhaustion or that use dynamic allocation may benefit from enabling this. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2850) |
| `spark.sql.streaming.stateStore.valueStateSchemaEvolutionThreshold` | int | `16` | 4.0.0 | The maximum number of value state schema evolutions allowed per column family | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2822) |
| `spark.sql.streaming.statefulOperator.allowMultiple` | boolean | `true` | 3.4.0 | When true, multiple stateful operators are allowed to be present in a streaming pipeline. The support for multiple stateful operators introduces a minor (semantically correct) change in respect to late record filtering - late records are detected and filtered in respect to the watermark from the previous microbatch instead of the current one. This is a behavior change for Spark streaming pipelines and we allow users to revert to the previous behavior of late record filtering (late records are detected and filtered by comparing with the current microbatch watermark) by setting the flag value to false. In this mode, only a single stateful operator will be allowed in a streaming pipeline. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2985) |
| `spark.sql.streaming.statefulOperator.checkCorrectness.enabled` | boolean | `true` | 3.1.0 | When true, the stateful operators for streaming query will be checked for possible correctness issue due to global watermark. The correctness issue comes from queries containing stateful operation which can emit rows older than the current watermark plus allowed late record delay, which are "late rows" in downstream stateful operations and these rows can be discarded. Please refer the programming guide doc for more details. Once the issue is detected, Spark will throw analysis exception. When this config is disabled, Spark will just print warning message for users. Prior to Spark 3.1.0, the behavior is disabling this config. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2970) |
| `spark.sql.streaming.statefulOperator.useStrictDistribution` | boolean | `true` | 3.3.0 | The purpose of this config is only compatibility; DO NOT MANUALLY CHANGE THIS!!! When true, the stateful operator for streaming query will use StatefulOpClusteredDistribution which guarantees stable state partitioning as long as the operator provides consistent grouping keys across the lifetime of query. When false, the stateful operator for streaming query will use ClusteredDistribution which is not sufficient to guarantee stable state partitioning despite the operator provides consistent grouping keys across the lifetime of query. This config will be set to true for new streaming queries to guarantee stable state partitioning, and set to false for existing streaming queries to not break queries which are restored from existing checkpoints. Please refer SPARK-38204 for details. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3001) |
| `spark.sql.streaming.stopActiveRunOnRestart` | boolean | `true` | 3.0.0 | Running multiple runs of the same streaming query concurrently is not supported. If we find a concurrent active run for a streaming query (in the same or different SparkSessions on the same cluster) and this flag is true, we will stop the old streaming query run to start the new one. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2908) |
| `spark.sql.streaming.stopTimeout` | time | `0` | 3.0.0 | How long to wait in milliseconds for the streaming execution thread to stop when calling the streaming query's stop() method. 0 or negative values wait indefinitely. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3439) |
| `spark.sql.streaming.transformWithState.stateSchemaVersion` | int | `3` | 4.0.0 | The version of the state schema used by the transformWithState operator | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2787) |
| `spark.sql.streaming.triggerAvailableNowWrapper.enabled` | boolean | `false` | — | Whether to use the wrapper implementation of Trigger.AvailableNow if the source does not support Trigger.AvailableNow. Enabling this allows the benefits of Trigger.AvailableNow with sources which don't support it, but some sources may show unexpected behavior including duplication, data loss, etc. So use with extreme care! The ideal direction is to persuade developers of source(s) to support Trigger.AvailableNow. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3060) |
| `spark.sql.streaming.unsupportedOperationCheck` | boolean | `true` | 2.0.0 | When true, the logical plan for streaming query will be checked for unsupported operations. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2950) |
| `spark.sql.streaming.verifyCheckpointDirectoryEmptyOnStart` | boolean | `true` | 4.1.0 | When true, verifies that the checkpoint directory (offsets, state, commits) is empty when first starting a streaming query. This prevents prevents sharing checkpoint directories between different queries. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2830) |

### `spark.sql.subexpressionElimination.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.subexpressionElimination.cache.maxEntries` | int | `100` | 3.1.0 | The maximum entries of the cache used for interpreted subexpression elimination. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1242) |
| `spark.sql.subexpressionElimination.enabled` | boolean | `true` | 1.6.0 | When true, common subexpressions will be eliminated. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1234) |
| `spark.sql.subexpressionElimination.skipForShortcutExpr` | boolean | `false` | 3.5.0 | When true, shortcut eliminate subexpression with `AND`, `OR`. The subexpression may not need to eval even if it appears more than once. e.g., `if(or(a, and(b, b)))`, the expression `b` would be skipped if `a` is true. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1251) |

### `spark.sql.thriftServer.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.thriftServer.incrementalCollect` | boolean | `false` | 2.0.3 | When true, enable incremental collection for execution in Thrift Server. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1872) |
| `spark.sql.thriftServer.interruptOnCancel` | fallback | → `INTERRUPT_ON_CANCEL` | 3.2.0 | When true, all running tasks will be interrupted if one cancels a query. When false, all running tasks will remain until finished. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1880) |
| `spark.sql.thriftServer.queryTimeout` | time | `0` | 3.1.0 | Set a query duration timeout in seconds in Thrift Server. If the timeout is set to a positive value, a running query will be cancelled automatically when the timeout is exceeded, otherwise the query continues to run till completion. If timeout values are set for each statement via `java.sql.Statement.setQueryTimeout` and they are smaller than this configuration value, they take precedence. If you set this timeout and prefer to cancel the queries right away without waiting task to finish, consider enabling ${THRIFTSERVER_FORCE_CANCEL.key} together. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1887) |

### `spark.sql.thriftserver.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.thriftserver.scheduler.pool` | string | _(optional)_ | 1.1.1 | Set a Fair Scheduler pool for a JDBC client session. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1865) |
| `spark.sql.thriftserver.ui.retainedSessions` | int | `200` | 1.4.0 | The number of SQL client sessions kept in the JDBC/ODBC web UI history. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1906) |
| `spark.sql.thriftserver.ui.retainedStatements` | int | `200` | 1.4.0 | The number of SQL statements kept in the JDBC/ODBC web UI history. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1900) |

### `spark.sql.timeTravelTimestampKey.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.timeTravelTimestampKey` | string | `timestampAsOf` | 4.0.0 | The option name to specify the time travel timestamp when reading a table. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L6208) |

### `spark.sql.timeTravelVersionKey.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.timeTravelVersionKey` | string | `versionAsOf` | 4.0.0 | The option name to specify the time travel table version when reading a table. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L6215) |

### `spark.sql.timeType.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.timeType.enabled` | boolean | `Utils.isTesting` | 4.1.0 | When true, the TIME data type is supported. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L6652) |

### `spark.sql.timestampType.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.timestampType` | enum | `TimestampTypes.TIMESTAMP_LTZ` | 3.4.0 | Configures the default timestamp type of Spark SQL, including SQL DDL, Cast clause, type literal and the schema inference of data sources. Setting the configuration as ${TimestampTypes.TIMESTAMP_NTZ} will use TIMESTAMP WITHOUT TIME ZONE as the default type while putting it as ${TimestampTypes.TIMESTAMP_LTZ} will use TIMESTAMP WITH LOCAL TIME ZONE. Before the 3.4.0 release, Spark only supports the TIMESTAMP WITH LOCAL TIME ZONE type. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5192) |

### `spark.sql.transposeMaxValues.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.transposeMaxValues` | int | `500` | 4.0.0 | When doing a transpose without specifying values for the index column this is the maximum number of values that will be transposed without error. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2307) |

### `spark.sql.truncateTable.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.truncateTable.ignorePermissionAcl.enabled` | boolean | `false` | 2.4.6 | When set to true, TRUNCATE TABLE command will not try to set back original permission and ACLs when re-creating the table/partition paths. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5132) |

### `spark.sql.tvf.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.tvf.allowMultipleTableArguments.enabled` | boolean | `false` | 3.5.0 | When true, allows multiple table arguments for table-valued functions, receiving the cartesian product of all the rows of these tables. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3830) |

### `spark.sql.ui.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.ui.explainMode` | string | `formatted` | 3.1.0 | Configures the query explain mode used in the Spark SQL UI. The value can be 'simple', 'extended', 'codegen', 'cost', or 'formatted'. The default value is 'formatted'. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5213) |

### `spark.sql.unionOutputPartitioning.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.unionOutputPartitioning` | boolean | `true` | 4.1.0 | When set to true, the output partitioning of UnionExec will be the same as the input partitioning if its children have same partitioning. Otherwise, it will be a default partitioning. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L6426) |

### `spark.sql.useCommonExprIdForAlias.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.useCommonExprIdForAlias` | boolean | `true` | 4.0.0 | When true, use the common expression ID for the alias when rewriting With expressions. Otherwise, use the index of the common expression definition. When true this avoids duplicate alias names, but is helpful to set to false for testing to ensure that alias names are consistent. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4629) |

### `spark.sql.variable.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.variable.substitute` | boolean | `true` | 2.0.0 | This enables substitution using syntax like `${var}`, `${system:var}`, and `${env:var}`. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3097) |

### `spark.sql.variant.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.variant.allowDuplicateKeys` | boolean | `false` | 4.0.0 | When set to false, parsing variant from JSON will throw an error if there are duplicate keys in the input JSON object. When set to true, the parser will keep the last occurrence of all fields with the same key. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5531) |
| `spark.sql.variant.allowReadingShredded` | boolean | `true` | 4.0.0 | When true, the Parquet reader is allowed to read shredded or unshredded variant. When false, it only reads unshredded variant. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5541) |
| `spark.sql.variant.forceShreddingSchemaForTest` | string | — | 4.0.0 | FOR INTERNAL TESTING ONLY. Sets shredding schema for Variant. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5567) |
| `spark.sql.variant.inferShreddingSchema` | boolean | `true` | 4.1.0 | Infer shredding schema when writing Variant columns in Parquet tables. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5592) |
| `spark.sql.variant.pushVariantIntoScan` | boolean | `true` | 4.0.0 | When true, replace variant type in the scan schema with a struct containing requested fields. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5550) |
| `spark.sql.variant.shredding.maxSchemaDepth` | int | `50` | 4.1.0 | Maximum depth in Variant value to traverse when inferring a schema. Any array/object below this depth will be shredded as a single binary. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5583) |
| `spark.sql.variant.shredding.maxSchemaWidth` | int | `300` | 4.1.0 | Maximum number of shredded fields to create when inferring a schema for Variant | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5575) |
| `spark.sql.variant.writeShredding.enabled` | boolean | `true` | 4.0.0 | When true, the Parquet writer is allowed to write shredded variant. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5559) |

### `spark.sql.view.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.view.maxNestedViewDepth` | int | `100` | 2.2.0 | The maximum depth of a view reference in a nested view. A nested view may reference other nested views, the dependencies are organized in a directed acyclic graph (DAG). However the DAG depth may become too large and cause unexpected behavior. This configuration puts a limit on this: when the depth of a view exceeds this value during analysis, we terminate the resolution to avoid potential errors. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3185) |

### `spark.sql.windowExec.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.windowExec.buffer.in.memory.threshold` | int | `4096` | 2.2.1 | Threshold for number of rows guaranteed to be held in memory by the window operator | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3680) |
| `spark.sql.windowExec.buffer.spill.size.threshold` | fallback | → `SHUFFLE_SPILL_MAX_SIZE_FORCE_SPILL_THRESHOLD` | 4.1.0 | Threshold for size of rows to be spilled by window operator | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3696) |
| `spark.sql.windowExec.buffer.spill.threshold` | int | `SHUFFLE_SPILL_NUM_ELEMENTS_FORCE_SPILL_THRESHOLD.defaultValue.get` | 2.2.0 | Threshold for number of rows to be spilled by window operator | [src](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3688) |

## sql/connect

### `spark.connect.ml.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.connect.ml.backend.classes` | string | `Nil` | 4.0.0 | \|Comma separated list of classes that implement the trait \|org.apache.spark.sql.connect.plugin.MLBackendPlugin to replace the \|specified Spark ML operators with a backend-specific implementation. \| | [src](https://github.com/apache/spark/blob/v4.1.2/sql/connect/server/src/main/scala/org/apache/spark/sql/connect/config/Connect.scala#L222) |

### `spark.connect.progress.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.connect.progress.reportInterval` | time | `2s` | 4.0.0 | The interval at which the progress of a query is reported to the client. If the value is set to a negative value the progress reports will be disabled. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/connect/server/src/main/scala/org/apache/spark/sql/connect/config/Connect.scala#L294) |

### `spark.connect.session.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.connect.session.connectML.mlCache.memoryControl.enabled` | boolean | `true` | 4.1.0 | Enables ML cache memory control, it includes offloading model to disk, limiting model size, and limiting per-session ML cache size. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/connect/server/src/main/scala/org/apache/spark/sql/connect/config/Connect.scala#L361) |
| `spark.connect.session.connectML.mlCache.memoryControl.maxInMemorySize` | bytes | `Runtime.getRuntime.maxMemory() / 4` | 4.1.0 | Maximum in-memory size of the MLCache per session. The cache will offload the least recently used models to Spark driver local disk if the size exceeds this limit. The size is in bytes. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/connect/server/src/main/scala/org/apache/spark/sql/connect/config/Connect.scala#L370) |
| `spark.connect.session.connectML.mlCache.memoryControl.maxModelSize` | bytes | `1g` | 4.1.0 | Maximum size of a single SparkML model. The size is in bytes. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/connect/server/src/main/scala/org/apache/spark/sql/connect/config/Connect.scala#L392) |
| `spark.connect.session.connectML.mlCache.memoryControl.maxStorageSize` | bytes | `10g` | 4.1.0 | Maximum total size (including in-memory and offloaded data) of the ml cache. The size is in bytes. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/connect/server/src/main/scala/org/apache/spark/sql/connect/config/Connect.scala#L400) |
| `spark.connect.session.connectML.mlCache.memoryControl.offloadingTimeout` | time | `15` | 4.1.0 | Timeout of model offloading in MLCache. Models will be offloaded to Spark driver local disk if they are not used for this amount of time. The timeout is in minutes. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/connect/server/src/main/scala/org/apache/spark/sql/connect/config/Connect.scala#L382) |
| `spark.connect.session.planCache.alwaysCacheDataSourceReadsEnabled` | boolean | `true` | 4.1.0 | When true, always cache the translation of Read.DataSource plans in the plan cache. This massively improves the performance of queries that reuse the same Read.DataSource within the same session, since these translations/analyses are usually quite costly. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/connect/server/src/main/scala/org/apache/spark/sql/connect/config/Connect.scala#L322) |
| `spark.connect.session.planCache.enabled` | boolean | `true` | 4.0.0 | When true, the cache of resolved logical plans is enabled if '${CONNECT_SESSION_PLAN_CACHE_SIZE.key}' is greater than zero. When false, the cache is disabled even if '${CONNECT_SESSION_PLAN_CACHE_SIZE.key}' is greater than zero. The caching is best-effort and not guaranteed. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/connect/server/src/main/scala/org/apache/spark/sql/connect/config/Connect.scala#L311) |
| `spark.connect.session.planCompression.defaultAlgorithm` | string | `ConnectPlanCompressionAlgorithm.ZSTD.toString` | 4.1.0 | The default algorithm of proto plan compression. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/connect/server/src/main/scala/org/apache/spark/sql/connect/config/Connect.scala#L445) |
| `spark.connect.session.planCompression.threshold` | int | `10 * 1024 * 1024` | 4.1.0 | The threshold in bytes for the size of proto plan to be compressed. If the size of proto plan is smaller than this threshold, it will not be compressed. Set to -1 to disable plan compression. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/connect/server/src/main/scala/org/apache/spark/sql/connect/config/Connect.scala#L435) |
| `spark.connect.session.resultChunking.maxChunkSize` | bytes | `(ConnectCommon.CONNECT_GRPC_MAX_MESSAGE_SIZE * 0.9).toInt` | 4.1.0 | The max size of a chunk in responses for a result batch. Result chunking is enabled if this config is set to a value greater than 0 and if the client allows it in ResultChunkingOptions. Otherwise, for example if set to -1, this feature is disabled. While spark.connect.grpc.arrow.maxBatchSize determines the max size of a result batch, maxChunkSize defines the max size of each individual chunk that is part of the batch that will be sent in a response. This allows the server to send large rows to clients. The size is in bytes. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/connect/server/src/main/scala/org/apache/spark/sql/connect/config/Connect.scala#L409) |

### `spark.sql.connect.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.connect.enrichError.enabled` | boolean | `true` | 4.0.0 | \|When true, it enriches errors with full exception messages and optionally server-side \|stacktrace on the client side via an additional RPC. \| | [src](https://github.com/apache/spark/blob/v4.1.2/sql/connect/server/src/main/scala/org/apache/spark/sql/connect/config/Connect.scala#L268) |
| `spark.sql.connect.serverStacktrace.enabled` | boolean | `true` | 4.0.0 | When true, it sets the server-side stacktrace in the user-facing Spark exception. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/connect/server/src/main/scala/org/apache/spark/sql/connect/config/Connect.scala#L278) |

## sql/hive

### `spark.sql.hive.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.hive.convertInsertingPartitionedTable` | boolean | `true` | 3.0.0 | When set to true, and `spark.sql.hive.convertMetastoreParquet` or `spark.sql.hive.convertMetastoreOrc` is true, the built-in ORC/Parquet writer is usedto process inserting into partitioned ORC/Parquet tables created by using the HiveSQL syntax. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/hive/src/main/scala/org/apache/spark/sql/hive/HiveUtils.scala#L149) |
| `spark.sql.hive.convertInsertingUnpartitionedTable` | boolean | `true` | 4.0.0 | When set to true, and `spark.sql.hive.convertMetastoreParquet` or `spark.sql.hive.convertMetastoreOrc` is true, the built-in ORC/Parquet writer is usedto process inserting into unpartitioned ORC/Parquet tables created by using the HiveSQL syntax. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/hive/src/main/scala/org/apache/spark/sql/hive/HiveUtils.scala#L159) |
| `spark.sql.hive.convertMetastoreAsNullable` | boolean | `false` | 4.1.0 | When set to true, apply nullable to the schema when Spark use datasource APIs instead of Hive serde to read/write Hive tables in Parquet or ORC formats. This flag is effective only if `convertMetastoreParquet` or `convertMetastoreOrc` is enabled respectively. It's recommended to set to true, when the nullability of table schema is inconsistent between the metastore and the data files. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/hive/src/main/scala/org/apache/spark/sql/hive/HiveUtils.scala#L186) |
| `spark.sql.hive.convertMetastoreCtas` | boolean | `true` | 3.0.0 | When set to true, Spark will try to use built-in data source writer instead of Hive serde in CTAS. This flag is effective only if `spark.sql.hive.convertMetastoreParquet` or `spark.sql.hive.convertMetastoreOrc` is enabled respectively for Parquet and ORC formats | [src](https://github.com/apache/spark/blob/v4.1.2/sql/hive/src/main/scala/org/apache/spark/sql/hive/HiveUtils.scala#L168) |
| `spark.sql.hive.convertMetastoreInsertDir` | boolean | `true` | 3.3.0 | When set to true, Spark will try to use built-in data source writer instead of Hive serde in INSERT OVERWRITE DIRECTORY. This flag is effective only if `spark.sql.hive.convertMetastoreParquet` or `spark.sql.hive.convertMetastoreOrc` is enabled respectively for Parquet and ORC formats | [src](https://github.com/apache/spark/blob/v4.1.2/sql/hive/src/main/scala/org/apache/spark/sql/hive/HiveUtils.scala#L177) |
| `spark.sql.hive.convertMetastoreOrc` | boolean | `true` | 2.0.0 | When set to true, the built-in ORC reader and writer are used to process ORC tables created by using the HiveQL syntax, instead of Hive serde. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/hive/src/main/scala/org/apache/spark/sql/hive/HiveUtils.scala#L141) |
| `spark.sql.hive.convertMetastoreParquet` | boolean | `true` | 1.1.1 | When set to true, the built-in Parquet reader and writer are used to process parquet tables created by using the HiveQL syntax, instead of Hive serde. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/hive/src/main/scala/org/apache/spark/sql/hive/HiveUtils.scala#L125) |
| `spark.sql.hive.convertMetastoreParquet.mergeSchema` | boolean | `false` | 1.3.1 | When true, also tries to merge possibly different but compatible Parquet schemas in different Parquet data files. This configuration is only effective when "spark.sql.hive.convertMetastoreParquet" is true. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/hive/src/main/scala/org/apache/spark/sql/hive/HiveUtils.scala#L133) |
| `spark.sql.hive.thriftServer.async` | boolean | `true` | 1.5.0 | When set to true, Hive Thrift server executes SQL queries in an asynchronous way. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/hive/src/main/scala/org/apache/spark/sql/hive/HiveUtils.scala#L219) |
| `spark.sql.hive.useDelegateForSymlinkTextInputFormat` | boolean | `true` | 3.4.0 | When true, SymlinkTextInputFormat is replaced with a similar delegate class during table scan in order to fix the issue of empty splits | [src](https://github.com/apache/spark/blob/v4.1.2/sql/hive/src/main/scala/org/apache/spark/sql/hive/HiveUtils.scala#L234) |

### `spark.sql.legacy.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.sql.legacy.hive.thriftServer.useZeroBasedColumnOrdinalPosition` | boolean | `false` | 4.1.0 | When set to true, Hive Thrift server returns 0-based ORDINAL_POSITION in the result of GetColumns operation, instead of the corrected 1-based. | [src](https://github.com/apache/spark/blob/v4.1.2/sql/hive/src/main/scala/org/apache/spark/sql/hive/HiveUtils.scala#L226) |

## streaming

### `spark.streaming.backpressure.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.streaming.backpressure.enabled` | boolean | `false` | 1.5.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/streaming/src/main/scala/org/apache/spark/streaming/StreamingConf.scala#L28) |
| `spark.streaming.backpressure.initialRate` | fallback | → `RECEIVER_MAX_RATE` | 2.0.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/streaming/src/main/scala/org/apache/spark/streaming/StreamingConf.scala#L40) |
| `spark.streaming.backpressure.pid.derived` | double | `0.0` | 1.5.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/streaming/src/main/scala/org/apache/spark/streaming/StreamingConf.scala#L159) |
| `spark.streaming.backpressure.pid.integral` | double | `0.2` | 1.5.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/streaming/src/main/scala/org/apache/spark/streaming/StreamingConf.scala#L153) |
| `spark.streaming.backpressure.pid.minRate` | double | `100` | 1.5.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/streaming/src/main/scala/org/apache/spark/streaming/StreamingConf.scala#L165) |
| `spark.streaming.backpressure.pid.proportional` | double | `1.0` | 1.5.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/streaming/src/main/scala/org/apache/spark/streaming/StreamingConf.scala#L147) |
| `spark.streaming.backpressure.rateEstimator` | string | `pid` | 1.5.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/streaming/src/main/scala/org/apache/spark/streaming/StreamingConf.scala#L141) |

### `spark.streaming.blockInterval.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.streaming.blockInterval` | time | `200ms` | 0.8.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/streaming/src/main/scala/org/apache/spark/streaming/StreamingConf.scala#L45) |

### `spark.streaming.concurrentJobs.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.streaming.concurrentJobs` | int | `1` | 0.7.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/streaming/src/main/scala/org/apache/spark/streaming/StreamingConf.scala#L171) |

### `spark.streaming.driver.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.streaming.driver.writeAheadLog.allowBatching` | boolean | `true` | 1.6.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/streaming/src/main/scala/org/apache/spark/streaming/StreamingConf.scala#L105) |
| `spark.streaming.driver.writeAheadLog.batchingTimeout` | long | `5000` | 1.6.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/streaming/src/main/scala/org/apache/spark/streaming/StreamingConf.scala#L111) |
| `spark.streaming.driver.writeAheadLog.class` | string | _(optional)_ | 1.4.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/streaming/src/main/scala/org/apache/spark/streaming/StreamingConf.scala#L81) |
| `spark.streaming.driver.writeAheadLog.closeFileAfterWrite` | boolean | `false` | 1.6.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/streaming/src/main/scala/org/apache/spark/streaming/StreamingConf.scala#L99) |
| `spark.streaming.driver.writeAheadLog.maxFailures` | int | `3` | 1.4.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/streaming/src/main/scala/org/apache/spark/streaming/StreamingConf.scala#L93) |
| `spark.streaming.driver.writeAheadLog.rollingIntervalSecs` | int | `60` | 1.4.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/streaming/src/main/scala/org/apache/spark/streaming/StreamingConf.scala#L87) |

### `spark.streaming.extraListeners.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.streaming.extraListeners` | string | _(optional)_ | 4.1.0 | Class names of streaming listeners to add to StreamingContext during initialization. | [src](https://github.com/apache/spark/blob/v4.1.2/streaming/src/main/scala/org/apache/spark/streaming/StreamingConf.scala#L189) |

### `spark.streaming.gracefulStopTimeout.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.streaming.gracefulStopTimeout` | time | _(optional)_ | 1.0.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/streaming/src/main/scala/org/apache/spark/streaming/StreamingConf.scala#L177) |

### `spark.streaming.manualClock.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.streaming.manualClock.jump` | long | `0` | 0.7.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/streaming/src/main/scala/org/apache/spark/streaming/StreamingConf.scala#L183) |

### `spark.streaming.receiver.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.streaming.receiver.maxRate` | long | `Long.MaxValue` | 1.0.2 |  | [src](https://github.com/apache/spark/blob/v4.1.2/streaming/src/main/scala/org/apache/spark/streaming/StreamingConf.scala#L34) |
| `spark.streaming.receiver.writeAheadLog.class` | string | _(optional)_ | 1.4.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/streaming/src/main/scala/org/apache/spark/streaming/StreamingConf.scala#L57) |
| `spark.streaming.receiver.writeAheadLog.closeFileAfterWrite` | boolean | `false` | 1.6.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/streaming/src/main/scala/org/apache/spark/streaming/StreamingConf.scala#L75) |
| `spark.streaming.receiver.writeAheadLog.enable` | boolean | `false` | 1.2.1 |  | [src](https://github.com/apache/spark/blob/v4.1.2/streaming/src/main/scala/org/apache/spark/streaming/StreamingConf.scala#L51) |
| `spark.streaming.receiver.writeAheadLog.maxFailures` | int | `3` | 1.2.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/streaming/src/main/scala/org/apache/spark/streaming/StreamingConf.scala#L69) |
| `spark.streaming.receiver.writeAheadLog.rollingIntervalSecs` | int | `60` | 1.4.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/streaming/src/main/scala/org/apache/spark/streaming/StreamingConf.scala#L63) |

### `spark.streaming.sessionByKey.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.streaming.sessionByKey.deltaChainThreshold` | int | `DELTA_CHAIN_LENGTH_THRESHOLD` | 1.6.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/streaming/src/main/scala/org/apache/spark/streaming/StreamingConf.scala#L135) |

### `spark.streaming.stopGracefullyOnShutdown.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.streaming.stopGracefullyOnShutdown` | boolean | `false` | 1.4.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/streaming/src/main/scala/org/apache/spark/streaming/StreamingConf.scala#L123) |

### `spark.streaming.ui.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.streaming.ui.retainedBatches` | int | `1000` | 1.0.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/streaming/src/main/scala/org/apache/spark/streaming/StreamingConf.scala#L129) |

### `spark.streaming.unpersist.*`

| Config | Type | Default | Since | Description | Source |
|---|---|---|---|---|---|
| `spark.streaming.unpersist` | boolean | `true` | 0.9.0 |  | [src](https://github.com/apache/spark/blob/v4.1.2/streaming/src/main/scala/org/apache/spark/streaming/StreamingConf.scala#L117) |

## Unparsed entries

> These builder chains could not be fully resolved (dynamic keys or missing terminals). They are listed here rather than dropped.

| Reason | Source | Snippet |
|---|---|---|
| dynamic-key | [resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala:280](https://github.com/apache/spark/blob/v4.1.2/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala#L280) | `ConfigBuilder(s"$KUBERNETES_AUTH_DRIVER_CONF_PREFIX.serviceAccountName")       .` |
| dynamic-key | [resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala:290](https://github.com/apache/spark/blob/v4.1.2/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Config.scala#L290) | `ConfigBuilder(s"$KUBERNETES_AUTH_EXECUTOR_CONF_PREFIX.serviceAccountName")      ` |
| dynamic-key | [sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala:5250](https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5250) | `buildConf(s"spark.sql.catalog.$SESSION_CATALOG_NAME")       .doc("A catalog impl` |

