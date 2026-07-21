# Connectors (Kafka/JDBC/Parquet/ORC/Avro)

> Source: `release-notes/spark_all_changelogs.txt`. Timeline rows below are generated from `_catalog.jsonl`; prose outside the AUTO markers is hand-written.

## How it evolved

### 1.x era — JSON, Parquet, JDBC, ORC, and Hive maturity

The 1.x line built out Spark's data source ecosystem from scratch. 1.0.1 added JSON dataset support (SPARK-2060) and improved Parquet reading/writing, including nested records and arrays (SPARK-1293); 1.1.0 layered in automatic JSON schema inference and a public types API for building `SchemaRDD`s from custom sources. 1.2.0 introduced the external data sources API, letting Parquet and JSON be rewritten against it and mounted as temporary tables with predicate pushdown. 1.3.0 added a JDBC data source for MySQL/Postgres and Parquet schema merging, and 1.4.0 brought ORCFile support (SPARK-2883). 1.5.0 was Parquet-heavy — upgrade to Parquet 1.7, predicate pushdown on by default, faster metadata discovery, Hive 1.2 metastore support — with 1.6.0 further speeding up Parquet scans on flat schemas (SPARK-11787).

### 2.x era — vectorized readers and built-in Avro/image sources

2.0.0 brought CSV in-house from Databricks' spark-csv package (SPARK-12420) and made the vectorized, columnar Parquet reader the default (SPARK-13518). 2.2.0 added multi-line CSV parsing (SPARK-19610). 2.3.0 extended vectorization to ORC, improving scan throughput 2-5x (SPARK-16060), and introduced a built-in image data source for reading images straight into a DataFrame (SPARK-21866). 2.4.0 was the connectors release: Avro became a built-in data source with logical-type support (SPARK-24768), the native ORC reader was switched on by default including for Hive serde tables (SPARK-23456, SPARK-22279), ORC filter pushdown was enabled by default (SPARK-21783), Parquet was upgraded to 1.10.0 with better predicate pushdown (SPARK-23972, SPARK-25419), and `count()` over JSON/CSV got a dedicated speedup (SPARK-24959).

### 3.x era — DSv2 migration and vectorized readers mature

3.0.0 migrated the built-in Parquet, ORC, CSV, JSON, Kafka, Text, and Avro sources onto DSv2 (SPARK-27589) and added Hive 3.1 metastore support. 3.1.1 pushed more filter kinds down to Parquet/Avro/JSON (contains, starts/ends-with, not-equals) and added JDBC catalog and connection-provider APIs. 3.2.0 upgraded Parquet to 1.12.1, added column-index support to the vectorized reader, and extended the vectorized ORC reader to nested columns. 3.3.0 supported complex types in the Parquet vectorized reader (SPARK-34863) and added min/max/count aggregate pushdown for both Parquet and ORC. 3.4.0 brought a PyTorch Distributor (SPARK-41589) and UDT support in the vectorized Parquet reader, while 3.5.0 added JDBC catalog char/varchar support and Avro custom-decimal handling — steady maturation of the DSv2-based readers rather than a single headline feature.

## Timeline

<!-- AUTO:timeline START -->
| Release | JIRA | Type | Title |
|---|---|---|---|
| 1.0.0 | — | prose | Avro 1.7.6 upgrade adds support for Avro specific types |
| 1.0.1 | [SPARK-1293](https://issues.apache.org/jira/browse/SPARK-1293) | prose | Improved Parquet read/write incl. nested records and arrays |
| 1.0.1 | [SPARK-2060](https://issues.apache.org/jira/browse/SPARK-2060) | prose | Support for querying JSON datasets |
| 1.1.0 | — | prose | JSON data loading module with automatic schema inference |
| 1.1.0 | — | prose | Public types API for building SchemaRDDs from custom data sources |
| 1.1.0 | — | prose | Parquet support optimizations added throughout the engine |
| 1.2.0 | — | prose | New external data sources API for Spark SQL |
| 1.2.0 | — | prose | Parquet and JSON bindings rewritten on external data sources API |
| 1.2.0 | — | prose | Hive integration adds fixed-precision decimal type and Hive 0.13 |
| 1.2.0 | — | prose | Support for reading binary files (images and other binary formats) |
| 1.3.0 | — | prose | New JDBC data source for importing/exporting MySQL, Postgres, etc. |
| 1.3.0 | — | prose | Schema evolution via merging compatible Parquet schemas |
| 1.4.0 | [SPARK-2883](https://issues.apache.org/jira/browse/SPARK-2883) | prose | Support for ORCFile format |
| 1.5.0 | — | prose | Upgraded Hive dependency to Hive 1.2 |
| 1.5.0 | — | prose | Support connecting to Hive 0.13-1.2 metastore |
| 1.5.0 | — | prose | Support partition pruning pushdown into the Hive metastore |
| 1.5.0 | — | prose | Support persisting data in Hive compatible format in metastore |
| 1.5.0 | — | prose | Parquet upgraded to 1.7 |
| 1.5.0 | — | prose | Parquet metadata discovery and schema merging sped up |
| 1.5.0 | — | prose | Parquet predicate pushdown on by default |
| 1.5.0 | [SPARK-746](https://issues.apache.org/jira/browse/SPARK-746) | Improvement | Automatically Use Avro Serialization for Avro Objects |
| 1.5.0 | [SPARK-4176](https://issues.apache.org/jira/browse/SPARK-4176) | New Feature | Support decimals with precision > 18 in Parquet |
| 1.5.0 | [SPARK-5109](https://issues.apache.org/jira/browse/SPARK-5109) | New Feature | Loading multiple parquet files into a single SchemaRDD |
| 1.5.0 | [SPARK-6154](https://issues.apache.org/jira/browse/SPARK-6154) | Improvement | Support Kafka, JDBC in Scala 2.11 |
| 1.5.0 | [SPARK-6566](https://issues.apache.org/jira/browse/SPARK-6566) | Improvement | Update Spark to use the latest version of Parquet libraries |
| 1.5.0 | [SPARK-6632](https://issues.apache.org/jira/browse/SPARK-6632) | Improvement | Optimize the parquetSchema to metastore schema reconciliation, so that the process is delegated to each map task itself |
| 1.5.0 | [SPARK-6774](https://issues.apache.org/jira/browse/SPARK-6774) | prose | Support for reading non-standard legacy Parquet files |
| 1.5.0 | [SPARK-6906](https://issues.apache.org/jira/browse/SPARK-6906) | Story | Improve Hive integration support |
| 1.5.0 | [SPARK-6964](https://issues.apache.org/jira/browse/SPARK-6964) | New Feature | Support Cancellation in the Thrift Server |
| 1.5.0 | [SPARK-7821](https://issues.apache.org/jira/browse/SPARK-7821) | Improvement | Hide private SQL JDBC classes from Javadoc |
| 1.5.0 | [SPARK-7845](https://issues.apache.org/jira/browse/SPARK-7845) | Improvement | Bump "Hadoop 1" tests to version 1.2.1 |
| 1.5.0 | [SPARK-8106](https://issues.apache.org/jira/browse/SPARK-8106) | Improvement | Set derby.system.durability=test in order to speed up Hive compatibility tests |
| 1.5.0 | [SPARK-8127](https://issues.apache.org/jira/browse/SPARK-8127) | Improvement | KafkaRDD optimize count() take() isEmpty() |
| 1.5.0 | [SPARK-8135](https://issues.apache.org/jira/browse/SPARK-8135) | Improvement | Don't load defaults when reconstituting Hadoop Configurations |
| 1.5.0 | [SPARK-8139](https://issues.apache.org/jira/browse/SPARK-8139) | Improvement | Documents data sources and Parquet output committer related options |
| 1.5.0 | [SPARK-8307](https://issues.apache.org/jira/browse/SPARK-8307) | Improvement | Improve timestamp from parquet |
| 1.5.0 | [SPARK-8390](https://issues.apache.org/jira/browse/SPARK-8390) | Improvement | Update DirectKafkaWordCount examples to show how offset ranges can be used |
| 1.5.0 | [SPARK-8397](https://issues.apache.org/jira/browse/SPARK-8397) | Improvement | Allow custom configuration for TestHive |
| 1.5.0 | [SPARK-8690](https://issues.apache.org/jira/browse/SPARK-8690) | Improvement | Add a setting to disable SparkSQL parquet schema merge by using datasource API |
| 1.5.0 | [SPARK-8785](https://issues.apache.org/jira/browse/SPARK-8785) | Improvement | Improve Parquet schema merging |
| 1.5.0 | [SPARK-8838](https://issues.apache.org/jira/browse/SPARK-8838) | Improvement | Add config to enable/disable merging part-files when merging parquet schema |
| 1.5.0 | [SPARK-8890](https://issues.apache.org/jira/browse/SPARK-8890) | prose | Faster and more robust dynamic partition insert |
| 1.5.0 | [SPARK-9067](https://issues.apache.org/jira/browse/SPARK-9067) | Improvement | Memory overflow and open file limit exhaustion for NewParquetRDD+CoalescedRDD |
| 1.5.0 | [SPARK-9100](https://issues.apache.org/jira/browse/SPARK-9100) | Improvement | DataFrame reader/writer shortcut methods for ORC |
| 1.5.0 | [SPARK-9232](https://issues.apache.org/jira/browse/SPARK-9232) | Improvement | Duplicate code in JSONRelation |
| 1.5.0 | [SPARK-9381](https://issues.apache.org/jira/browse/SPARK-9381) | New Feature | Migrate JSON data source to the new partitioning data source |
| 1.5.0 | [SPARK-9496](https://issues.apache.org/jira/browse/SPARK-9496) | Improvement | Do not print password in Hive Config |
| 1.5.0 | [SPARK-9618](https://issues.apache.org/jira/browse/SPARK-9618) | Improvement | SQLContext.read.schema().parquet() ignores the supplied schema |
| 1.5.0 | [SPARK-9692](https://issues.apache.org/jira/browse/SPARK-9692) | Improvement | Remove SqlNewHadoopRDD's generated Tuple2 and InterruptibleIterator |
| 1.5.0 | [SPARK-10088](https://issues.apache.org/jira/browse/SPARK-10088) | Improvement | Support "stored as avro" HiveQL construct |
| 1.6.0 | [SPARK-9522](https://issues.apache.org/jira/browse/SPARK-9522) | Improvement | SparkSubmit process can not exit if kill application when HiveThriftServer was starting |
| 1.6.0 | [SPARK-9547](https://issues.apache.org/jira/browse/SPARK-9547) | Improvement | Allow testing pull requests with different Hadoop versions |
| 1.6.0 | [SPARK-9818](https://issues.apache.org/jira/browse/SPARK-9818) | Improvement | Revert 6136, use docker to test JDBC datasources |
| 1.6.0 | [SPARK-9935](https://issues.apache.org/jira/browse/SPARK-9935) | Improvement | EqualNullSafe not processed in OrcRelation |
| 1.6.0 | [SPARK-10035](https://issues.apache.org/jira/browse/SPARK-10035) | Improvement | Parquet filters does not process EqualNullSafe filter. |
| 1.6.0 | [SPARK-10040](https://issues.apache.org/jira/browse/SPARK-10040) | Improvement | JDBC writer change to use batch insert for performance |
| 1.6.0 | [SPARK-10113](https://issues.apache.org/jira/browse/SPARK-10113) | Improvement | Support for unsigned Parquet logical types |
| 1.6.0 | [SPARK-10151](https://issues.apache.org/jira/browse/SPARK-10151) | New Feature | Support invocation of hive macro |
| 1.6.0 | [SPARK-10400](https://issues.apache.org/jira/browse/SPARK-10400) | Improvement | Rename or deprecate SQL option "spark.sql.parquet.followParquetFormatSpec" |
| 1.6.0 | [SPARK-10419](https://issues.apache.org/jira/browse/SPARK-10419) | Improvement | Add JDBC dialect for Microsoft SQL Server |
| 1.6.0 | [SPARK-10811](https://issues.apache.org/jira/browse/SPARK-10811) | Improvement | Minimize array copying cost in Parquet converters |
| 1.6.0 | [SPARK-10855](https://issues.apache.org/jira/browse/SPARK-10855) | Improvement | Add a JDBC dialect for Apache Derby |
| 1.6.0 | [SPARK-11044](https://issues.apache.org/jira/browse/SPARK-11044) | Improvement | Parquet writer version fixed as version1 |
| 1.6.0 | [SPARK-11089](https://issues.apache.org/jira/browse/SPARK-11089) | New Feature | Add a option for thrift-server to share a single session across all connections |
| 1.6.0 | [SPARK-11109](https://issues.apache.org/jira/browse/SPARK-11109) | Improvement | move FsHistoryProvider off import org.apache.hadoop.fs.permission.AccessControlException |
| 1.6.0 | [SPARK-11125](https://issues.apache.org/jira/browse/SPARK-11125) | Improvement | Unreadable exception when running spark-sql without building with -Phive-thriftserver and SPARK_PREPEND_CLASSES is set |
| 1.6.0 | [SPARK-11270](https://issues.apache.org/jira/browse/SPARK-11270) | Improvement | Add improved equality testing for TopicAndPartition from the Kafka Streaming API |
| 1.6.0 | [SPARK-11305](https://issues.apache.org/jira/browse/SPARK-11305) | Improvement | Remove Third-Party Hadoop Distributions Doc Page |
| 1.6.0 | [SPARK-11318](https://issues.apache.org/jira/browse/SPARK-11318) | Improvement | Include hive profile in make-distribution.sh command |
| 1.6.0 | [SPARK-11328](https://issues.apache.org/jira/browse/SPARK-11328) | Improvement | Provide more informative error message when direct parquet output committer is used and there is a file already exists error. |
| 1.6.0 | [SPARK-11342](https://issues.apache.org/jira/browse/SPARK-11342) | Improvement | Allow to set hadoop profile when running dev/run_tests |
| 1.6.0 | [SPARK-11351](https://issues.apache.org/jira/browse/SPARK-11351) | Improvement | support hive interval literal in sql parser |
| 1.6.0 | [SPARK-11413](https://issues.apache.org/jira/browse/SPARK-11413) | Improvement | Java 8 build has problem with joda-time and s3 request, should bump joda-time version |
| 1.6.0 | [SPARK-11546](https://issues.apache.org/jira/browse/SPARK-11546) | Improvement | Thrift server makes too many logs about result schema |
| 1.6.0 | [SPARK-11695](https://issues.apache.org/jira/browse/SPARK-11695) | Improvement | Set s3a credentials by default similarly to s3 and s3n |
| 1.6.0 | [SPARK-11787](https://issues.apache.org/jira/browse/SPARK-11787) | prose | Parquet scan performance improved for flat schemas |
| 1.6.0 | [SPARK-11881](https://issues.apache.org/jira/browse/SPARK-11881) | Improvement | [SQL] JDBC Postgres fetchsize parameter ignored |
| 1.6.0 | [SPARK-12103](https://issues.apache.org/jira/browse/SPARK-12103) | Improvement | Clarify documentation of KafkaUtils createStream with multiple topics |
| 1.6.0 | [SPARK-12166](https://issues.apache.org/jira/browse/SPARK-12166) | Improvement | Unset hadoop related environment in testing |
| 2.0.0 | — | prose | Native CSV data source (from Databricks spark-csv) |
| 2.0.0 | — | prose | Improved Parquet scan throughput via vectorization |
| 2.0.0 | — | prose | Improved ORC performance |
| 2.0.0 | [SPARK-5292](https://issues.apache.org/jira/browse/SPARK-5292) | New Feature | optimize join for table that are already sharded/support for hive bucket |
| 2.0.0 | [SPARK-5718](https://issues.apache.org/jira/browse/SPARK-5718) | Improvement | Add native offset management for ReliableKafkaReceiver |
| 2.0.0 | [SPARK-6482](https://issues.apache.org/jira/browse/SPARK-6482) | Improvement | Remove synchronization of Hive Native commands |
| 2.0.0 | [SPARK-9926](https://issues.apache.org/jira/browse/SPARK-9926) | Improvement | Parallelize file listing for partitioned Hive table |
| 2.0.0 | [SPARK-10180](https://issues.apache.org/jira/browse/SPARK-10180) | Improvement | JDBCRDD does not process EqualNullSafe filter. |
| 2.0.0 | [SPARK-10521](https://issues.apache.org/jira/browse/SPARK-10521) | Improvement | Utilize Docker to test DB2 JDBC Dialect support |
| 2.0.0 | [SPARK-10963](https://issues.apache.org/jira/browse/SPARK-10963) | Improvement | Make KafkaCluster api public |
| 2.0.0 | [SPARK-11044](https://issues.apache.org/jira/browse/SPARK-11044) | Improvement | Parquet writer version fixed as version1 |
| 2.0.0 | [SPARK-11164](https://issues.apache.org/jira/browse/SPARK-11164) | Improvement | Add InSet pushdown filter back for Parquet |
| 2.0.0 | [SPARK-11622](https://issues.apache.org/jira/browse/SPARK-11622) | Improvement | Make LibSVMRelation extends HadoopFsRelation and Add LibSVMOutputWriter |
| 2.0.0 | [SPARK-11692](https://issues.apache.org/jira/browse/SPARK-11692) | Improvement | Support for Parquet logical types, JSON and BSON (embedded types) |
| 2.0.0 | [SPARK-11955](https://issues.apache.org/jira/browse/SPARK-11955) | Improvement | Mark one side fields in merging schema for safely pushdowning filters in parquet |
| 2.0.0 | [SPARK-12103](https://issues.apache.org/jira/browse/SPARK-12103) | Improvement | Clarify documentation of KafkaUtils createStream with multiple topics |
| 2.0.0 | [SPARK-12120](https://issues.apache.org/jira/browse/SPARK-12120) | Improvement | Improve exception message when failing to initialize HiveContext in PySpark |
| 2.0.0 | [SPARK-12166](https://issues.apache.org/jira/browse/SPARK-12166) | Improvement | Unset hadoop related environment in testing |
| 2.0.0 | [SPARK-12249](https://issues.apache.org/jira/browse/SPARK-12249) | Improvement | JDBC non-equality comparison operator not pushed down. |
| 2.0.0 | [SPARK-12314](https://issues.apache.org/jira/browse/SPARK-12314) | Improvement | isnull operator not pushed down for JDBC datasource. |
| 2.0.0 | [SPARK-12315](https://issues.apache.org/jira/browse/SPARK-12315) | Improvement | isnotnull operator not pushed down for JDBC datasource. |
| 2.0.0 | [SPARK-12387](https://issues.apache.org/jira/browse/SPARK-12387) | Improvement | JDBC IN operator push down |
| 2.0.0 | [SPARK-12391](https://issues.apache.org/jira/browse/SPARK-12391) | Improvement | JDBC OR operator push down |
| 2.0.0 | [SPARK-12409](https://issues.apache.org/jira/browse/SPARK-12409) | Improvement | JDBC AND operator push down |
| 2.0.0 | [SPARK-12417](https://issues.apache.org/jira/browse/SPARK-12417) | Improvement | Orc bloom filter options are not propagated during file write in spark |
| 2.0.0 | [SPARK-12420](https://issues.apache.org/jira/browse/SPARK-12420) | New Feature | Have a built-in CSV data source implementation |
| 2.0.0 | [SPARK-12440](https://issues.apache.org/jira/browse/SPARK-12440) | Improvement | Avoid setCheckpointDir warning when filesystem is not local |
| 2.0.0 | [SPARK-12476](https://issues.apache.org/jira/browse/SPARK-12476) | Improvement | Implement JdbcRelation#unhandledFilters for removing unnecessary Spark Filter |
| 2.0.0 | [SPARK-12523](https://issues.apache.org/jira/browse/SPARK-12523) | Improvement | Support long-running of the Spark On HBase and hive meta store. |
| 2.0.0 | [SPARK-12542](https://issues.apache.org/jira/browse/SPARK-12542) | New Feature | Support intersect/except in Hive SQL |
| 2.0.0 | [SPARK-12854](https://issues.apache.org/jira/browse/SPARK-12854) | Improvement | Vectorize Parquet reader |
| 2.0.0 | [SPARK-12872](https://issues.apache.org/jira/browse/SPARK-12872) | Improvement | Support to specify the option for compression codec for JSON datasource. |
| 2.0.0 | [SPARK-12953](https://issues.apache.org/jira/browse/SPARK-12953) | Improvement | RDDRelation write set mode will be better to avoid error "pair.parquet already exists" |
| 2.0.0 | [SPARK-12992](https://issues.apache.org/jira/browse/SPARK-12992) | Improvement | Vectorize parquet decoding using ColumnarBatch |
| 2.0.0 | [SPARK-13070](https://issues.apache.org/jira/browse/SPARK-13070) | Improvement | Points out which physical file is the trouble maker when Parquet schema merging fails |
| 2.0.0 | [SPARK-13498](https://issues.apache.org/jira/browse/SPARK-13498) | Improvement | JDBCRDD should update some input metrics |
| 2.0.0 | [SPARK-13499](https://issues.apache.org/jira/browse/SPARK-13499) | Improvement | Optimize vectorized parquet reader for dictionary encoded data and RLE decoding |
| 2.0.0 | [SPARK-13518](https://issues.apache.org/jira/browse/SPARK-13518) | Improvement | Enable vectorized parquet reader by default |
| 2.0.0 | [SPARK-13526](https://issues.apache.org/jira/browse/SPARK-13526) | Improvement | Refactor: Move SQLContext/HiveContext per-session state to separate class |
| 2.0.0 | [SPARK-13530](https://issues.apache.org/jira/browse/SPARK-13530) | Improvement | Add ShortType support to UnsafeRowParquetRecordReader |
| 2.0.0 | [SPARK-13543](https://issues.apache.org/jira/browse/SPARK-13543) | New Feature | Support for specifying compression codec for Parquet/ORC via option() |
| 2.0.0 | [SPARK-13574](https://issues.apache.org/jira/browse/SPARK-13574) | Improvement | Improve parquet dictionary decoding for strings |
| 2.0.0 | [SPARK-13582](https://issues.apache.org/jira/browse/SPARK-13582) | Improvement | Improve performance of parquet reader with dictionary encoding |
| 2.0.0 | [SPARK-13599](https://issues.apache.org/jira/browse/SPARK-13599) | Improvement | Groovy-all ends up in spark-assembly if hive profile set |
| 2.0.0 | [SPARK-13613](https://issues.apache.org/jira/browse/SPARK-13613) | Improvement | Provide ignored tests to export test dataset into CSV format |
| 2.0.0 | [SPARK-13664](https://issues.apache.org/jira/browse/SPARK-13664) | Improvement | Simplify and Speedup HadoopFSRelation |
| 2.0.0 | [SPARK-13764](https://issues.apache.org/jira/browse/SPARK-13764) | New Feature | Parse modes in JSON data source |
| 2.0.0 | [SPARK-13766](https://issues.apache.org/jira/browse/SPARK-13766) | Improvement | Inconsistent file extensions and omitted file extensions written by CSV, TEXT and JSON data sources |
| 2.0.0 | [SPARK-13922](https://issues.apache.org/jira/browse/SPARK-13922) | Improvement | Filter rows with null attributes in parquet vectorized reader |
| 2.0.0 | [SPARK-13953](https://issues.apache.org/jira/browse/SPARK-13953) | Improvement | Support for specifying the field name for corrupted record at JSON datasource. |
| 2.0.0 | [SPARK-13972](https://issues.apache.org/jira/browse/SPARK-13972) | Improvement | hive tests should fail if SQL generation failed |
| 2.0.0 | [SPARK-14056](https://issues.apache.org/jira/browse/SPARK-14056) | Improvement | Add s3 configurations and spark.hadoop.* configurations to hive configuration |
| 2.0.0 | [SPARK-14070](https://issues.apache.org/jira/browse/SPARK-14070) | Improvement | Use ORC data source for SQL queries on ORC tables |
| 2.0.0 | [SPARK-14435](https://issues.apache.org/jira/browse/SPARK-14435) | Improvement | Shade Kryo in our custom Hive 1.2.1 fork |
| 2.0.0 | [SPARK-14482](https://issues.apache.org/jira/browse/SPARK-14482) | Improvement | Change default compression codec for Parquet from gzip to snappy |
| 2.0.0 | [SPARK-14551](https://issues.apache.org/jira/browse/SPARK-14551) | Improvement | Reduce number of NameNode calls in OrcRelation with FileSourceStrategy mode |
| 2.0.0 | [SPARK-14596](https://issues.apache.org/jira/browse/SPARK-14596) | Improvement | Remove not used SqlNewHadoopRDD |
| 2.0.0 | [SPARK-14687](https://issues.apache.org/jira/browse/SPARK-14687) | Improvement | Call path.getFileSystem(conf) instead of call FileSystem.get(conf) |
| 2.0.0 | [SPARK-14773](https://issues.apache.org/jira/browse/SPARK-14773) | Improvement | Enable the tests in HiveCompatibilitySuite for subquery |
| 2.0.0 | [SPARK-14776](https://issues.apache.org/jira/browse/SPARK-14776) | New Feature | Merge HiveSqlAstBuilder and SparkSqlAstBuilder |
| 2.0.0 | [SPARK-14825](https://issues.apache.org/jira/browse/SPARK-14825) | Improvement | Merge functionality in Hive module into SQL core module |
| 2.0.0 | [SPARK-14912](https://issues.apache.org/jira/browse/SPARK-14912) | Improvement | Propagate data source options to Hadoop configurations |
| 2.0.0 | [SPARK-15267](https://issues.apache.org/jira/browse/SPARK-15267) | Improvement | Refactor and add some classes for options in datasources like CSVOptions or JSONOptions |
| 2.0.0 | [SPARK-15280](https://issues.apache.org/jira/browse/SPARK-15280) | Improvement | Extract ORC serialization logic from OrcOutputWriter for reusability |
| 2.0.0 | [SPARK-15649](https://issues.apache.org/jira/browse/SPARK-15649) | Improvement | Avoid serializing MetastoreRelation in HiveTableScanExec |
| 2.0.0 | [SPARK-15676](https://issues.apache.org/jira/browse/SPARK-15676) | Improvement | Disallow Column Names as Partition Columns For Hive Tables |
| 2.0.0 | [SPARK-15745](https://issues.apache.org/jira/browse/SPARK-15745) | Improvement | Use classloader's getResource() for reading resource files in HiveTests |
| 2.0.0 | [SPARK-15756](https://issues.apache.org/jira/browse/SPARK-15756) | Improvement | Support command 'create table stored as orcfile/parquetfile/avrofile' |
| 2.0.0 | [SPARK-16162](https://issues.apache.org/jira/browse/SPARK-16162) | Improvement | Remove dead code: class OrcTableScan |
| 2.0.0 | [SPARK-16221](https://issues.apache.org/jira/browse/SPARK-16221) | Improvement | Redirect Parquet JUL logger via SLF4J for WRITE operations |
| 2.0.0 | [SPARK-16248](https://issues.apache.org/jira/browse/SPARK-16248) | Improvement | Whitelist the list of Hive fallback functions |
| 2.0.1 | [SPARK-13286](https://issues.apache.org/jira/browse/SPARK-13286) | Improvement | JDBC driver doesn't report full exception |
| 2.0.1 | [SPARK-15639](https://issues.apache.org/jira/browse/SPARK-15639) | Improvement | Try to push down filter at RowGroups level for parquet reader |
| 2.0.1 | [SPARK-16216](https://issues.apache.org/jira/browse/SPARK-16216) | Improvement | CSV data source does not write date and timestamp correctly |
| 2.0.1 | [SPARK-16663](https://issues.apache.org/jira/browse/SPARK-16663) | Improvement | desc table should be consistent between data source and hive serde tables |
| 2.0.1 | [SPARK-16764](https://issues.apache.org/jira/browse/SPARK-16764) | Improvement | Recommend disabling vectorized parquet reader on OutOfMemoryError |
| 2.0.1 | [SPARK-17023](https://issues.apache.org/jira/browse/SPARK-17023) | Improvement | Update Kafka connetor to use Kafka 0.10.0.1 |
| 2.0.1 | [SPARK-17063](https://issues.apache.org/jira/browse/SPARK-17063) | Improvement | MSCK REPAIR TABLE is super slow with Hive metastore |
| 2.0.1 | [SPARK-17193](https://issues.apache.org/jira/browse/SPARK-17193) | Improvement | HadoopRDD NPE at DEBUG log level when getLocationInfo == null |
| 2.0.1 | [SPARK-17558](https://issues.apache.org/jira/browse/SPARK-17558) | Improvement | Bump Hadoop 2.7 version from 2.7.2 to 2.7.3 |
| 2.0.2 | [SPARK-17999](https://issues.apache.org/jira/browse/SPARK-17999) | Improvement | Add getPreferredLocations for KafkaSourceRDD |
| 2.1.0 | [SPARK-9876](https://issues.apache.org/jira/browse/SPARK-9876) | Improvement | Upgrade parquet-mr to 1.8.1 |
| 2.1.0 | [SPARK-13286](https://issues.apache.org/jira/browse/SPARK-13286) | Improvement | JDBC driver doesn't report full exception |
| 2.1.0 | [SPARK-14525](https://issues.apache.org/jira/browse/SPARK-14525) | Improvement | DataFrameWriter's save method should delegate to jdbc for jdbc datasource |
| 2.1.0 | [SPARK-15198](https://issues.apache.org/jira/browse/SPARK-15198) | Improvement | Support for filter push down for boolean types in ORC |
| 2.1.0 | [SPARK-15639](https://issues.apache.org/jira/browse/SPARK-15639) | Improvement | Try to push down filter at RowGroups level for parquet reader |
| 2.1.0 | [SPARK-15956](https://issues.apache.org/jira/browse/SPARK-15956) | Improvement | When unwrapping ORC avoid pattern matching at runtime |
| 2.1.0 | [SPARK-16119](https://issues.apache.org/jira/browse/SPARK-16119) | Improvement | Support "DROP TABLE ... PURGE" if Hive client supports it |
| 2.1.0 | [SPARK-16216](https://issues.apache.org/jira/browse/SPARK-16216) | Improvement | CSV data source does not write date and timestamp correctly |
| 2.1.0 | [SPARK-16226](https://issues.apache.org/jira/browse/SPARK-16226) | Improvement | Weaken JDBC isolation level to avoid locking when writing partitions |
| 2.1.0 | [SPARK-16374](https://issues.apache.org/jira/browse/SPARK-16374) | Improvement | Remove Alias from MetastoreRelation and SimpleCatalogRelation |
| 2.1.0 | [SPARK-16389](https://issues.apache.org/jira/browse/SPARK-16389) | Improvement | Remove useless `MetastoreRelation` from `SparkHiveWriterContainer` and `SparkHiveDynamicPartitionWriterContainer` |
| 2.1.0 | [SPARK-16463](https://issues.apache.org/jira/browse/SPARK-16463) | Improvement | Support `truncate` option in Overwrite mode for JDBC DataFrameWriter |
| 2.1.0 | [SPARK-16498](https://issues.apache.org/jira/browse/SPARK-16498) | Improvement | move hive hack for data source table into HiveExternalCatalog |
| 2.1.0 | [SPARK-16516](https://issues.apache.org/jira/browse/SPARK-16516) | Improvement | Support for pushing down filters for decimal and timestamp types in ORC |
| 2.1.0 | [SPARK-16662](https://issues.apache.org/jira/browse/SPARK-16662) | Improvement | The HiveContext deprecate warning in python always shown even if do not use HiveContext |
| 2.1.0 | [SPARK-16663](https://issues.apache.org/jira/browse/SPARK-16663) | Improvement | desc table should be consistent between data source and hive serde tables |
| 2.1.0 | [SPARK-16674](https://issues.apache.org/jira/browse/SPARK-16674) | Improvement | Avoid per-record type dispatch in JDBC when reading |
| 2.1.0 | [SPARK-16675](https://issues.apache.org/jira/browse/SPARK-16675) | Improvement | Avoid per-record type dispatch in JDBC when writing |
| 2.1.0 | [SPARK-16736](https://issues.apache.org/jira/browse/SPARK-16736) | Improvement | remove redundant FileSystem status checks calls from Spark codebase |
| 2.1.0 | [SPARK-16764](https://issues.apache.org/jira/browse/SPARK-16764) | Improvement | Recommend disabling vectorized parquet reader on OutOfMemoryError |
| 2.1.0 | [SPARK-16793](https://issues.apache.org/jira/browse/SPARK-16793) | Improvement | Set the temporary warehouse path to sc'conf in TestHive |
| 2.1.0 | [SPARK-16847](https://issues.apache.org/jira/browse/SPARK-16847) | Improvement | Prevent to potentially read corrupt statstics on binary in Parquet via VectorizedReader |
| 2.1.0 | [SPARK-16858](https://issues.apache.org/jira/browse/SPARK-16858) | Improvement | Removal of TestHiveSharedState |
| 2.1.0 | [SPARK-16909](https://issues.apache.org/jira/browse/SPARK-16909) | Improvement | Streaming for postgreSQL JDBC driver |
| 2.1.0 | [SPARK-16968](https://issues.apache.org/jira/browse/SPARK-16968) | Improvement | Allow to add additional options when creating a new table in DF's JDBC writer. |
| 2.1.0 | [SPARK-17023](https://issues.apache.org/jira/browse/SPARK-17023) | Improvement | Update Kafka connetor to use Kafka 0.10.0.1 |
| 2.1.0 | [SPARK-17052](https://issues.apache.org/jira/browse/SPARK-17052) | Improvement | Remove Duplicate Test Cases auto_join from HiveCompatibilitySuite.scala |
| 2.1.0 | [SPARK-17063](https://issues.apache.org/jira/browse/SPARK-17063) | Improvement | MSCK REPAIR TABLE is super slow with Hive metastore |
| 2.1.0 | [SPARK-17190](https://issues.apache.org/jira/browse/SPARK-17190) | Improvement | Removal of HiveSharedState |
| 2.1.0 | [SPARK-17193](https://issues.apache.org/jira/browse/SPARK-17193) | Improvement | HadoopRDD NPE at DEBUG log level when getLocationInfo == null |
| 2.1.0 | [SPARK-17238](https://issues.apache.org/jira/browse/SPARK-17238) | Improvement | simplify the logic for converting data source table into hive compatible format |
| 2.1.0 | [SPARK-17330](https://issues.apache.org/jira/browse/SPARK-17330) | Improvement | Clean up spark-warehouse in UT |
| 2.1.0 | [SPARK-17351](https://issues.apache.org/jira/browse/SPARK-17351) | New Feature | Refactor JDBCRDD to expose JDBC -> SparkSQL conversion functionality |
| 2.1.0 | [SPARK-17470](https://issues.apache.org/jira/browse/SPARK-17470) | Improvement | unify path for data source table and locationUri for hive serde table |
| 2.1.0 | [SPARK-17509](https://issues.apache.org/jira/browse/SPARK-17509) | Improvement | When wrapping catalyst datatype to Hive data type avoid pattern matching |
| 2.1.0 | [SPARK-17534](https://issues.apache.org/jira/browse/SPARK-17534) | Improvement | Increase timeouts for DirectKafkaStreamSuite tests |
| 2.1.0 | [SPARK-17536](https://issues.apache.org/jira/browse/SPARK-17536) | Improvement | Minor performance improvement to JDBC batch inserts |
| 2.1.0 | [SPARK-17558](https://issues.apache.org/jira/browse/SPARK-17558) | Improvement | Bump Hadoop 2.7 version from 2.7.2 to 2.7.3 |
| 2.1.0 | [SPARK-17583](https://issues.apache.org/jira/browse/SPARK-17583) | Improvement | Remove unused rowSeparator variable and set auto-expanding buffer as default for maxCharsPerColumn option in CSV |
| 2.1.0 | [SPARK-17598](https://issues.apache.org/jira/browse/SPARK-17598) | Improvement | User-friendly name for Spark Thrift Server in web UI |
| 2.1.0 | [SPARK-17661](https://issues.apache.org/jira/browse/SPARK-17661) | Improvement | Consolidate various listLeafFiles implementations |
| 2.1.0 | [SPARK-17719](https://issues.apache.org/jira/browse/SPARK-17719) | Improvement | Unify and tie up options in a single place in JDBC datasource API |
| 2.1.0 | [SPARK-17776](https://issues.apache.org/jira/browse/SPARK-17776) | Improvement | Potentially duplicated names which might have conflicts between JDBC options and properties instance |
| 2.1.0 | [SPARK-17796](https://issues.apache.org/jira/browse/SPARK-17796) | Improvement | spark HiveThriftServer2 sql AnalysisException: LOAD DATA input path does not exist. if sql query is existed wild card characters |
| 2.1.0 | [SPARK-17802](https://issues.apache.org/jira/browse/SPARK-17802) | Improvement | Lots of "java.lang.ClassNotFoundException: org.apache.hadoop.ipc.CallerContext" In spark logs |
| 2.1.0 | [SPARK-17903](https://issues.apache.org/jira/browse/SPARK-17903) | Improvement | MetastoreRelation should talk to external catalog instead of hive client |
| 2.1.0 | [SPARK-17999](https://issues.apache.org/jira/browse/SPARK-17999) | Improvement | Add getPreferredLocations for KafkaSourceRDD |
| 2.1.0 | [SPARK-18377](https://issues.apache.org/jira/browse/SPARK-18377) | Improvement | warehouse path should be a static conf |
| 2.1.0 | [SPARK-18410](https://issues.apache.org/jira/browse/SPARK-18410) | Improvement | Add structured kafka example |
| 2.1.0 | [SPARK-18572](https://issues.apache.org/jira/browse/SPARK-18572) | Improvement | Use the hive client method "getPartitionNames" to answer "SHOW PARTITIONS" queries on partitioned Hive tables |
| 2.1.0 | [SPARK-18760](https://issues.apache.org/jira/browse/SPARK-18760) | New Feature | Provide consistent format output for all file formats |
| 2.2.0 | [SPARK-10101](https://issues.apache.org/jira/browse/SPARK-10101) | Improvement | Spark JDBC writer mapping String to TEXT or VARCHAR |
| 2.2.0 | [SPARK-10849](https://issues.apache.org/jira/browse/SPARK-10849) | Improvement | Allow user to specify database column type for data frame fields when writing data to jdbc data sources. |
| 2.2.0 | [SPARK-12334](https://issues.apache.org/jira/browse/SPARK-12334) | Improvement | Support read from multiple input paths for orc file in DataFrameReader.orc |
| 2.2.0 | [SPARK-13446](https://issues.apache.org/jira/browse/SPARK-13446) | Improvement | Spark need to support reading data from Hive 2.0.0 metastore |
| 2.2.0 | [SPARK-15463](https://issues.apache.org/jira/browse/SPARK-15463) | Improvement | Support for creating a dataframe from CSV in Dataset[String] |
| 2.2.0 | [SPARK-16848](https://issues.apache.org/jira/browse/SPARK-16848) | Improvement | Check schema validation for user-specified schema in jdbc and table APIs |
| 2.2.0 | [SPARK-18362](https://issues.apache.org/jira/browse/SPARK-18362) | Improvement | Use TextFileFormat in implementation of CSVFileFormat |
| 2.2.0 | [SPARK-18413](https://issues.apache.org/jira/browse/SPARK-18413) | Improvement | Add a property to control the number of partitions when save a jdbc rdd |
| 2.2.0 | [SPARK-18682](https://issues.apache.org/jira/browse/SPARK-18682) | New Feature | Batch Source for Kafka |
| 2.2.0 | [SPARK-18885](https://issues.apache.org/jira/browse/SPARK-18885) | Improvement | unify CREATE TABLE syntax for data source and hive serde tables |
| 2.2.0 | [SPARK-18943](https://issues.apache.org/jira/browse/SPARK-18943) | Improvement | Avoid per-record type dispatch in CSV when reading |
| 2.2.0 | [SPARK-18992](https://issues.apache.org/jira/browse/SPARK-18992) | Improvement | Move spark.sql.hive.thriftServer.singleSession to SQLConf |
| 2.2.0 | [SPARK-18997](https://issues.apache.org/jira/browse/SPARK-18997) | Improvement | Recommended upgrade libthrift to 0.9.3 |
| 2.2.0 | [SPARK-19085](https://issues.apache.org/jira/browse/SPARK-19085) | Improvement | cleanup OutputWriterFactory and OutputWriter |
| 2.2.0 | [SPARK-19107](https://issues.apache.org/jira/browse/SPARK-19107) | Improvement | support creating hive table with DataFrameWriter and Catalog |
| 2.2.0 | [SPARK-19150](https://issues.apache.org/jira/browse/SPARK-19150) | Improvement | completely support using hive as data source to create tables |
| 2.2.0 | [SPARK-19219](https://issues.apache.org/jira/browse/SPARK-19219) | Improvement | Parquet log output overly verbose by default |
| 2.2.0 | [SPARK-19239](https://issues.apache.org/jira/browse/SPARK-19239) | Improvement | Check the lowerBound and upperBound whether equal None in jdbc API |
| 2.2.0 | [SPARK-19265](https://issues.apache.org/jira/browse/SPARK-19265) | Improvement | make table relation cache general and does not depend on hive |
| 2.2.0 | [SPARK-19296](https://issues.apache.org/jira/browse/SPARK-19296) | Improvement | Awkward changes for JdbcUtils.saveTable in Spark 2.1.0 |
| 2.2.0 | [SPARK-19359](https://issues.apache.org/jira/browse/SPARK-19359) | Improvement | partition path created by Hive should be deleted after rename a partition with upper-case |
| 2.2.0 | [SPARK-19411](https://issues.apache.org/jira/browse/SPARK-19411) | Improvement | Remove the metadata used to mark optional columns in merged Parquet schema for filter predicate pushdown |
| 2.2.0 | [SPARK-19448](https://issues.apache.org/jira/browse/SPARK-19448) | Improvement | unify some duplication function in MetaStoreRelation |
| 2.2.0 | [SPARK-19464](https://issues.apache.org/jira/browse/SPARK-19464) | Improvement | Remove support for Hadoop 2.5 and earlier |
| 2.2.0 | [SPARK-19570](https://issues.apache.org/jira/browse/SPARK-19570) | Improvement | Allow to disable hive in pyspark shell |
| 2.2.0 | [SPARK-19610](https://issues.apache.org/jira/browse/SPARK-19610) | prose | Support for parsing multi-line CSV files |
| 2.2.0 | [SPARK-19660](https://issues.apache.org/jira/browse/SPARK-19660) | Improvement | Replace the configuration property names that are deprecated in the version of Hadoop 2.6 |
| 2.2.0 | [SPARK-19664](https://issues.apache.org/jira/browse/SPARK-19664) | Improvement | put 'hive.metastore.warehouse.dir' in hadoopConf place |
| 2.2.0 | [SPARK-19678](https://issues.apache.org/jira/browse/SPARK-19678) | Improvement | remove MetastoreRelation |
| 2.2.0 | [SPARK-19739](https://issues.apache.org/jira/browse/SPARK-19739) | Improvement | SparkHadoopUtil.appendS3AndSparkHadoopConfigurations to propagate full set of AWS env vars |
| 2.2.0 | [SPARK-19919](https://issues.apache.org/jira/browse/SPARK-19919) | Improvement | Defer input path validation into DataSource in CSV datasource |
| 2.2.0 | [SPARK-19921](https://issues.apache.org/jira/browse/SPARK-19921) | Improvement | Enable end-to-end testing using different Hive metastore versions. |
| 2.2.0 | [SPARK-19923](https://issues.apache.org/jira/browse/SPARK-19923) | Improvement | Remove unnecessary type conversion per call in Hive |
| 2.2.0 | [SPARK-19946](https://issues.apache.org/jira/browse/SPARK-19946) | Improvement | DebugFilesystem.assertNoOpenStreams should report the open streams to help debugging |
| 2.2.0 | [SPARK-19949](https://issues.apache.org/jira/browse/SPARK-19949) | Improvement | unify bad record handling in CSV and JSON |
| 2.2.0 | [SPARK-20036](https://issues.apache.org/jira/browse/SPARK-20036) | Improvement | impossible to read a whole kafka topic using kafka 0.10 and spark 2.0.0 |
| 2.2.0 | [SPARK-20046](https://issues.apache.org/jira/browse/SPARK-20046) | Improvement | Facilitate loop optimizations in a JIT compiler regarding sqlContext.read.parquet() |
| 2.2.0 | [SPARK-20107](https://issues.apache.org/jira/browse/SPARK-20107) | Improvement | Add spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version option to configuration.md |
| 2.2.0 | [SPARK-20146](https://issues.apache.org/jira/browse/SPARK-20146) | Improvement | Column comment information is missing for Thrift Server's TableSchema |
| 2.2.0 | [SPARK-20160](https://issues.apache.org/jira/browse/SPARK-20160) | Improvement | Move ParquetConversions and OrcConversions Out Of HiveSessionCatalog |
| 2.2.0 | [SPARK-20166](https://issues.apache.org/jira/browse/SPARK-20166) | Improvement | Use XXX for ISO timezone instead of ZZ which is FastDateFormat specific in CSV/JSON time related options |
| 2.2.0 | [SPARK-20600](https://issues.apache.org/jira/browse/SPARK-20600) | Improvement | KafkaRelation should be pretty printed in web UI (Details for Query) |
| 2.3.0 | [SPARK-16060](https://issues.apache.org/jira/browse/SPARK-16060) | prose | Vectorized ORC reader improves scan throughput 2-5x |
| 2.3.0 | [SPARK-21866](https://issues.apache.org/jira/browse/SPARK-21866) | prose | Built-in image data source (reading images into DataFrame) |
| 2.4.0 | [SPARK-4502](https://issues.apache.org/jira/browse/SPARK-4502) | prose | Nested schema pruning for Parquet tables |
| 2.4.0 | [SPARK-21783](https://issues.apache.org/jira/browse/SPARK-21783) | prose | ORC filter push-down turned on by default |
| 2.4.0 | [SPARK-22279](https://issues.apache.org/jira/browse/SPARK-22279) | prose | Native ORC reader used for Hive serde tables by default |
| 2.4.0 | [SPARK-22666](https://issues.apache.org/jira/browse/SPARK-22666) | prose | Spark datasource for image format |
| 2.4.0 | [SPARK-22814](https://issues.apache.org/jira/browse/SPARK-22814) | prose | Date/Timestamp supported in JDBC partition column |
| 2.4.0 | [SPARK-23456](https://issues.apache.org/jira/browse/SPARK-23456) | prose | Native ORC reader on by default |
| 2.4.0 | [SPARK-23786](https://issues.apache.org/jira/browse/SPARK-23786) | prose | CSV schema validation checks column names |
| 2.4.0 | [SPARK-23972](https://issues.apache.org/jira/browse/SPARK-23972) | prose | Parquet upgraded from 1.8.2 to 1.10.0 |
| 2.4.0 | [SPARK-24244](https://issues.apache.org/jira/browse/SPARK-24244) | prose | CSV parser parses only required columns |
| 2.4.0 | [SPARK-24423](https://issues.apache.org/jira/browse/SPARK-24423) | prose | Option query for specifying JDBC read query |
| 2.4.0 | [SPARK-24768](https://issues.apache.org/jira/browse/SPARK-24768) | prose | Built-in Avro data source with logical type support |
| 2.4.0 | [SPARK-24771](https://issues.apache.org/jira/browse/SPARK-24771) | prose | Avro updated from 1.7.7 to 1.8 |
| 2.4.0 | [SPARK-24959](https://issues.apache.org/jira/browse/SPARK-24959) | prose | Speed up count() for JSON and CSV |
| 2.4.0 | [SPARK-25419](https://issues.apache.org/jira/browse/SPARK-25419) | prose | Parquet predicate pushdown improvement |
| 3.0.0 | [SPARK-11412](https://issues.apache.org/jira/browse/SPARK-11412) | New Feature | Support merge schema for ORC |
| 3.0.0 | [SPARK-15616](https://issues.apache.org/jira/browse/SPARK-15616) | prose | Rule PruneHiveTablePartitions |
| 3.0.0 | [SPARK-17636](https://issues.apache.org/jira/browse/SPARK-17636) | prose | Parquet predicate pushdown for nested fields |
| 3.0.0 | [SPARK-23534](https://issues.apache.org/jira/browse/SPARK-23534) | Improvement | Spark run on Hadoop 3.0.0 |
| 3.0.0 | [SPARK-23710](https://issues.apache.org/jira/browse/SPARK-23710) | Umbrella | Upgrade the built-in Hive to 2.3.5 for hadoop-3.2 |
| 3.0.0 | [SPARK-23977](https://issues.apache.org/jira/browse/SPARK-23977) | prose | Support High Performance S3A committers |
| 3.0.0 | [SPARK-24360](https://issues.apache.org/jira/browse/SPARK-24360) | Improvement | Support Hive 3.1 metastore |
| 3.0.0 | [SPARK-24540](https://issues.apache.org/jira/browse/SPARK-24540) | Improvement | Support for multiple character delimiter in Spark CSV read |
| 3.0.0 | [SPARK-24766](https://issues.apache.org/jira/browse/SPARK-24766) | Improvement | CreateHiveTableAsSelect and InsertIntoHiveDir won't generate decimal column stats in parquet |
| 3.0.0 | [SPARK-25016](https://issues.apache.org/jira/browse/SPARK-25016) | Improvement | remove Support for hadoop 2.6 |
| 3.0.0 | [SPARK-25062](https://issues.apache.org/jira/browse/SPARK-25062) | Improvement | Clean up BlockLocations in FileStatus objects |
| 3.0.0 | [SPARK-25102](https://issues.apache.org/jira/browse/SPARK-25102) | Improvement | Write Spark version to ORC/Parquet file metadata |
| 3.0.0 | [SPARK-25151](https://issues.apache.org/jira/browse/SPARK-25151) | Improvement | Apply Apache Commons Pool to KafkaDataConsumer |
| 3.0.0 | [SPARK-25366](https://issues.apache.org/jira/browse/SPARK-25366) | Improvement | Document Zstd and brotli CompressionCodec requirements for Parquet files |
| 3.0.0 | [SPARK-25393](https://issues.apache.org/jira/browse/SPARK-25393) | Improvement | Parsing CSV strings in a column |
| 3.0.0 | [SPARK-25465](https://issues.apache.org/jira/browse/SPARK-25465) | Improvement | Refactor Parquet test suites in project Hive |
| 3.0.0 | [SPARK-25501](https://issues.apache.org/jira/browse/SPARK-25501) | Improvement | Kafka delegation token support |
| 3.0.0 | [SPARK-25595](https://issues.apache.org/jira/browse/SPARK-25595) | Improvement | Ignore corrupt Avro file if flag IGNORE_CORRUPT_FILES enabled |
| 3.0.0 | [SPARK-25603](https://issues.apache.org/jira/browse/SPARK-25603) | prose | Generalize Nested Column Pruning |
| 3.0.0 | [SPARK-25635](https://issues.apache.org/jira/browse/SPARK-25635) | New Feature | Support selective direct encoding in native ORC write |
| 3.0.0 | [SPARK-25638](https://issues.apache.org/jira/browse/SPARK-25638) | Improvement | Convert structs to CSV strings |
| 3.0.0 | [SPARK-25672](https://issues.apache.org/jira/browse/SPARK-25672) | Improvement | Inferring schema from CSV string literal |
| 3.0.0 | [SPARK-25684](https://issues.apache.org/jira/browse/SPARK-25684) | Improvement | Organize header related codes in CSV datasource |
| 3.0.0 | [SPARK-25699](https://issues.apache.org/jira/browse/SPARK-25699) | Improvement | Partially push down conjunctive predicated in Orc |
| 3.0.0 | [SPARK-25806](https://issues.apache.org/jira/browse/SPARK-25806) | Improvement | The instanceof FileSplit is redundant for ParquetFileFormat and OrcFileFormat |
| 3.0.0 | [SPARK-25893](https://issues.apache.org/jira/browse/SPARK-25893) | Improvement | Show a directional error message for unsupported Hive Metastore versions |
| 3.0.0 | [SPARK-25945](https://issues.apache.org/jira/browse/SPARK-25945) | Improvement | Support locale while parsing date/timestamp from CSV/JSON |
| 3.0.0 | [SPARK-25964](https://issues.apache.org/jira/browse/SPARK-25964) | Improvement | Revise OrcReadBenchmark/DataSourceReadBenchmark case names and execution instructions |
| 3.0.0 | [SPARK-25977](https://issues.apache.org/jira/browse/SPARK-25977) | Improvement | Parsing decimals from CSV using locale |
| 3.0.0 | [SPARK-26043](https://issues.apache.org/jira/browse/SPARK-26043) | Improvement | Make SparkHadoopUtil private to Spark |
| 3.0.0 | [SPARK-26081](https://issues.apache.org/jira/browse/SPARK-26081) | Improvement | Do not write empty files by text datasources |
| 3.0.0 | [SPARK-26091](https://issues.apache.org/jira/browse/SPARK-26091) | Improvement | Upgrade to 2.3.4 for Hive Metastore Client 2.3 |
| 3.0.0 | [SPARK-26122](https://issues.apache.org/jira/browse/SPARK-26122) | Improvement | Support encoding for multiLine in CSV datasource |
| 3.0.0 | [SPARK-26151](https://issues.apache.org/jira/browse/SPARK-26151) | Improvement | Return partial results for bad CSV records |
| 3.0.0 | [SPARK-26236](https://issues.apache.org/jira/browse/SPARK-26236) | New Feature | Kafka delegation token support documentation |
| 3.0.0 | [SPARK-26304](https://issues.apache.org/jira/browse/SPARK-26304) | Improvement | Add default value to spark.kafka.sasl.kerberos.service.name parameter |
| 3.0.0 | [SPARK-26322](https://issues.apache.org/jira/browse/SPARK-26322) | Improvement | Simplify kafka delegation token sasl.mechanism configuration |
| 3.0.0 | [SPARK-26350](https://issues.apache.org/jira/browse/SPARK-26350) | Improvement | Allow the user to override the group id of the Kafka's consumer |
| 3.0.0 | [SPARK-26371](https://issues.apache.org/jira/browse/SPARK-26371) | Improvement | Increase Kafka ConfigUpdater test coverage |
| 3.0.0 | [SPARK-26378](https://issues.apache.org/jira/browse/SPARK-26378) | Improvement | Queries of wide CSV/JSON data slowed after SPARK-26151 |
| 3.0.0 | [SPARK-26435](https://issues.apache.org/jira/browse/SPARK-26435) | Improvement | Support creating partitioned table using Hive CTAS by specifying partition column names |
| 3.0.0 | [SPARK-26457](https://issues.apache.org/jira/browse/SPARK-26457) | New Feature | Show hadoop configurations in HistoryServer environment tab |
| 3.0.0 | [SPARK-26550](https://issues.apache.org/jira/browse/SPARK-26550) | New Feature | New datasource for benchmarking |
| 3.0.0 | [SPARK-26630](https://issues.apache.org/jira/browse/SPARK-26630) | Improvement | Support reading Hive-serde tables whose INPUTFORMAT is org.apache.hadoop.mapreduce |
| 3.0.0 | [SPARK-26763](https://issues.apache.org/jira/browse/SPARK-26763) | Improvement | Using fileStatus cache when filterPartitions |
| 3.0.0 | [SPARK-26766](https://issues.apache.org/jira/browse/SPARK-26766) | Improvement | Remove the list of filesystems from HadoopDelegationTokenProvider.obtainDelegationTokens |
| 3.0.0 | [SPARK-26772](https://issues.apache.org/jira/browse/SPARK-26772) | Improvement | Delete ServiceCredentialProvider and make HadoopDelegationTokenProvider a developer API |
| 3.0.0 | [SPARK-26935](https://issues.apache.org/jira/browse/SPARK-26935) | Improvement | Skip DataFrameReader's CSV first line scan when not used |
| 3.0.0 | [SPARK-27010](https://issues.apache.org/jira/browse/SPARK-27010) | Improvement | find out the actual port number when hive.server2.thrift.port=0 |
| 3.0.0 | [SPARK-27022](https://issues.apache.org/jira/browse/SPARK-27022) | Improvement | Kafka delegation token support |
| 3.0.0 | [SPARK-27034](https://issues.apache.org/jira/browse/SPARK-27034) | Improvement | Nested schema pruning for ORC |
| 3.0.0 | [SPARK-27074](https://issues.apache.org/jira/browse/SPARK-27074) | Improvement | Hive 3.1 metastore support HiveClientImpl.runHive |
| 3.0.0 | [SPARK-27105](https://issues.apache.org/jira/browse/SPARK-27105) | Improvement | Prevent exponential complexity in ORC `createFilter` |
| 3.0.0 | [SPARK-27118](https://issues.apache.org/jira/browse/SPARK-27118) | Improvement | Upgrade to latest Hive version for Hive Metastore Client 1.1 and 1.0 |
| 3.0.0 | [SPARK-27119](https://issues.apache.org/jira/browse/SPARK-27119) | Improvement | Do not infer schema when reading Hive serde table with native data source |
| 3.0.0 | [SPARK-27260](https://issues.apache.org/jira/browse/SPARK-27260) | Improvement | Upgrade to Kafka 2.2.0 |
| 3.0.0 | [SPARK-27270](https://issues.apache.org/jira/browse/SPARK-27270) | Improvement | Add Kafka dynamic JAAS authentication debug possibility |
| 3.0.0 | [SPARK-27294](https://issues.apache.org/jira/browse/SPARK-27294) | Improvement | Multi-cluster Kafka delegation token support |
| 3.0.0 | [SPARK-27343](https://issues.apache.org/jira/browse/SPARK-27343) | Improvement | Use ConfigEntry for hardcoded configs for spark-sql-kafka |
| 3.0.0 | [SPARK-27399](https://issues.apache.org/jira/browse/SPARK-27399) | Improvement | Spark streaming of kafka 0.10 contains some scattered config |
| 3.0.0 | [SPARK-27500](https://issues.apache.org/jira/browse/SPARK-27500) | Umbrella | Add tests for built-in Hive 2.3 |
| 3.0.0 | [SPARK-27528](https://issues.apache.org/jira/browse/SPARK-27528) | Improvement | Use Parquet logical type TIMESTAMP_MICROS by default |
| 3.0.0 | [SPARK-27589](https://issues.apache.org/jira/browse/SPARK-27589) | prose | Built-in source migration using DSV2: parquet, ORC, CSV, JSON, Kafka, Text, Avro |
| 3.0.0 | [SPARK-27687](https://issues.apache.org/jira/browse/SPARK-27687) | Improvement | Kafka consumer cache parameter rename and documentation |
| 3.0.0 | [SPARK-27699](https://issues.apache.org/jira/browse/SPARK-27699) | Improvement | Partially push down disjunctive predicated in Parquet/ORC |
| 3.0.0 | [SPARK-27748](https://issues.apache.org/jira/browse/SPARK-27748) | Improvement | Kafka consumer/producer password/token redaction |
| 3.0.0 | [SPARK-27801](https://issues.apache.org/jira/browse/SPARK-27801) | Improvement | InMemoryFileIndex.listLeafFiles should use listLocatedStatus for DistributedFileSystem |
| 3.0.0 | [SPARK-27831](https://issues.apache.org/jira/browse/SPARK-27831) | Improvement | Move Hive test jars to maven dependency |
| 3.0.0 | [SPARK-27838](https://issues.apache.org/jira/browse/SPARK-27838) | New Feature | Support user provided non-nullable avro schema for nullable catalyst schema without any null record |
| 3.0.0 | [SPARK-27846](https://issues.apache.org/jira/browse/SPARK-27846) | Improvement | Eagerly compute Configuration.properties in sc.hadoopConfiguration |
| 3.0.0 | [SPARK-27946](https://issues.apache.org/jira/browse/SPARK-27946) | Improvement | Hive DDL to Spark DDL conversion USING "show create table" |
| 3.0.0 | [SPARK-27970](https://issues.apache.org/jira/browse/SPARK-27970) | Improvement | Support Hive 3.0 metastore |
| 3.0.0 | [SPARK-27973](https://issues.apache.org/jira/browse/SPARK-27973) | Improvement | Streaming sample DirectKafkaWordCount should mention GroupId in usage |
| 3.0.0 | [SPARK-28012](https://issues.apache.org/jira/browse/SPARK-28012) | Improvement | Hive UDF supports struct type foldable expression |
| 3.0.0 | [SPARK-28013](https://issues.apache.org/jira/browse/SPARK-28013) | Improvement | Upgrade to Kafka 2.2.1 |
| 3.0.0 | [SPARK-28097](https://issues.apache.org/jira/browse/SPARK-28097) | Improvement | Map ByteType to SMALLINT when using JDBC with PostgreSQL |
| 3.0.0 | [SPARK-28108](https://issues.apache.org/jira/browse/SPARK-28108) | Improvement | Simplify OrcFilters |
| 3.0.0 | [SPARK-28144](https://issues.apache.org/jira/browse/SPARK-28144) | Improvement | Remove ZKUtils from Kafka tests |
| 3.0.0 | [SPARK-28158](https://issues.apache.org/jira/browse/SPARK-28158) | Improvement | Hive UDFs supports UDT type |
| 3.0.0 | [SPARK-28174](https://issues.apache.org/jira/browse/SPARK-28174) | Improvement | Upgrade to Kafka 2.3.0 |
| 3.0.0 | [SPARK-28187](https://issues.apache.org/jira/browse/SPARK-28187) | Improvement | Add hadoop-cloud module to PR builders |
| 3.0.0 | [SPARK-28208](https://issues.apache.org/jira/browse/SPARK-28208) | Improvement | Upgrade to ORC 1.5.6 |
| 3.0.0 | [SPARK-28311](https://issues.apache.org/jira/browse/SPARK-28311) | Improvement | Spark Thrift Server protocol version compatibility setup too late |
| 3.0.0 | [SPARK-28426](https://issues.apache.org/jira/browse/SPARK-28426) | Umbrella | Metadata Handling in Thrift Server |
| 3.0.0 | [SPARK-28431](https://issues.apache.org/jira/browse/SPARK-28431) | Improvement | CSV datasource throw com.univocity.parsers.common.TextParsingException with large size message |
| 3.0.0 | [SPARK-28637](https://issues.apache.org/jira/browse/SPARK-28637) | Improvement | Thriftserver can not support interval type |
| 3.0.0 | [SPARK-28691](https://issues.apache.org/jira/browse/SPARK-28691) | Improvement | Add Java/Scala DirectKerberizedKafkaWordCount examples |
| 3.0.0 | [SPARK-28694](https://issues.apache.org/jira/browse/SPARK-28694) | Improvement | Add Java/Scala StructuredKerberizedKafkaWordCount examples |
| 3.0.0 | [SPARK-28760](https://issues.apache.org/jira/browse/SPARK-28760) | Improvement | Add end-to-end Kafka delegation token test |
| 3.0.0 | [SPARK-28875](https://issues.apache.org/jira/browse/SPARK-28875) | Improvement | Cover Task retry scenario with test in Kafka connector |
| 3.0.0 | [SPARK-28890](https://issues.apache.org/jira/browse/SPARK-28890) | Improvement | Upgrade Hive Metastore Client to the latest versions for Hive 3.1 |
| 3.0.0 | [SPARK-28901](https://issues.apache.org/jira/browse/SPARK-28901) | Improvement | SparkThriftServer SparkExecuteStatementOpration handle cancel status. |
| 3.0.0 | [SPARK-28922](https://issues.apache.org/jira/browse/SPARK-28922) | Improvement | Safe Kafka parameter redaction |
| 3.0.0 | [SPARK-28928](https://issues.apache.org/jira/browse/SPARK-28928) | Improvement | Use Kafka delegation token protocol on sources/sinks |
| 3.0.0 | [SPARK-28957](https://issues.apache.org/jira/browse/SPARK-28957) | Improvement | Copy any "spark.hive.foo=bar" spark properties into hadoop conf as "hive.foo=bar" |
| 3.0.0 | [SPARK-29036](https://issues.apache.org/jira/browse/SPARK-29036) | Improvement | SparkThriftServer may can't cancel job after call a cancel before start. |
| 3.0.0 | [SPARK-29054](https://issues.apache.org/jira/browse/SPARK-29054) | Improvement | Invalidate Kafka consumer when new delegation token available |
| 3.0.0 | [SPARK-29259](https://issues.apache.org/jira/browse/SPARK-29259) | Improvement | Filesystem.exists is called even when not necessary for append save mode |
| 3.0.0 | [SPARK-29349](https://issues.apache.org/jira/browse/SPARK-29349) | Improvement | Support FETCH_PRIOR in Thriftserver query results fetching |
| 3.0.0 | [SPARK-29454](https://issues.apache.org/jira/browse/SPARK-29454) | Improvement | Reduce unsafeProjection call times when read parquet file |
| 3.0.0 | [SPARK-29461](https://issues.apache.org/jira/browse/SPARK-29461) | Improvement | Spark dataframe writer does not expose metrics for JDBC writer |
| 3.0.0 | [SPARK-29492](https://issues.apache.org/jira/browse/SPARK-29492) | Improvement | SparkThriftServer can't support jar class as table serde class when executestatement in sync mode |
| 3.0.0 | [SPARK-29500](https://issues.apache.org/jira/browse/SPARK-29500) | Improvement | Support partition column when writing to Kafka |
| 3.0.0 | [SPARK-29516](https://issues.apache.org/jira/browse/SPARK-29516) | Improvement | Test ThriftServerQueryTestSuite asynchronously |
| 3.0.0 | [SPARK-29529](https://issues.apache.org/jira/browse/SPARK-29529) | Improvement | Remove unnecessary orc version and hive version in doc |
| 3.0.0 | [SPARK-29531](https://issues.apache.org/jira/browse/SPARK-29531) | Improvement | Refine ThriftServerQueryTestSuite.blackList to reuse code of SQLQueryTestSuite.blackList |
| 3.0.0 | [SPARK-29559](https://issues.apache.org/jira/browse/SPARK-29559) | Improvement | Support pagination for JDBC/ODBC UI page |
| 3.0.0 | [SPARK-29567](https://issues.apache.org/jira/browse/SPARK-29567) | Improvement | Update JDBC Integration Test Docker Images |
| 3.0.0 | [SPARK-29608](https://issues.apache.org/jira/browse/SPARK-29608) | Improvement | Add Hadoop 3.2 profile to binary package |
| 3.0.0 | [SPARK-29611](https://issues.apache.org/jira/browse/SPARK-29611) | Improvement | Sort Kafka metadata by the number of messages |
| 3.0.0 | [SPARK-29613](https://issues.apache.org/jira/browse/SPARK-29613) | Improvement | Upgrade to Kafka 2.3.1 |
| 3.0.0 | [SPARK-29617](https://issues.apache.org/jira/browse/SPARK-29617) | Improvement | Upgrade to ORC 1.5.7 |
| 3.0.0 | [SPARK-29687](https://issues.apache.org/jira/browse/SPARK-29687) | Improvement | Fix jdbc metrics counter type to long |
| 3.0.0 | [SPARK-29724](https://issues.apache.org/jira/browse/SPARK-29724) | prose | JDBC tab in SHS |
| 3.0.0 | [SPARK-29768](https://issues.apache.org/jira/browse/SPARK-29768) | prose | Column pruning through nondeterministic expressions |
| 3.0.0 | [SPARK-29805](https://issues.apache.org/jira/browse/SPARK-29805) | prose | ) and turned on by default |
| 3.0.0 | [SPARK-30032](https://issues.apache.org/jira/browse/SPARK-30032) | Improvement | Upgrade to ORC 1.5.8 |
| 3.0.0 | [SPARK-30034](https://issues.apache.org/jira/browse/SPARK-30034) | Umbrella | Use Apache Hive 2.3 dependency by default |
| 3.0.0 | [SPARK-30091](https://issues.apache.org/jira/browse/SPARK-30091) | Improvement | Document mergeSchema option directly in the Python Parquet APIs |
| 3.0.0 | [SPARK-30113](https://issues.apache.org/jira/browse/SPARK-30113) | Improvement | Document mergeSchema option in Python Orc APIs |
| 3.0.0 | [SPARK-30323](https://issues.apache.org/jira/browse/SPARK-30323) | Improvement | Support filters pushdown in CSV datasource |
| 3.0.0 | [SPARK-30336](https://issues.apache.org/jira/browse/SPARK-30336) | Improvement | Move Kafka consumer related classes to its own package |
| 3.0.0 | [SPARK-30338](https://issues.apache.org/jira/browse/SPARK-30338) | Improvement | Avoid unnecessary InternalRow copies in ParquetRowConverter |
| 3.0.0 | [SPARK-30414](https://issues.apache.org/jira/browse/SPARK-30414) | Improvement | Optimizations for arrays and maps in ParquetRowConverter |
| 3.0.0 | [SPARK-30695](https://issues.apache.org/jira/browse/SPARK-30695) | Improvement | Upgrade Apache ORC to 1.5.9 |
| 3.0.0 | [SPARK-30783](https://issues.apache.org/jira/browse/SPARK-30783) | Improvement | Hive 2.3 profile should exclude hive-service-rpc |
| 3.0.0 | [SPARK-31026](https://issues.apache.org/jira/browse/SPARK-31026) | New Feature | Parquet predicate pushdown on columns with dots |
| 3.0.0 | [SPARK-31064](https://issues.apache.org/jira/browse/SPARK-31064) | New Feature | New Parquet Predicate Filter APIs with multi-part Identifier Support |
| 3.0.0 | [SPARK-31126](https://issues.apache.org/jira/browse/SPARK-31126) | prose | Upgrade Kafka to 2.4.1 |
| 3.0.0 | [SPARK-31184](https://issues.apache.org/jira/browse/SPARK-31184) | Improvement | Support getTablesByType API of Hive Client |
| 3.0.0 | [SPARK-31327](https://issues.apache.org/jira/browse/SPARK-31327) | Improvement | write spark version to avro file metadata |
| 3.0.0 | [SPARK-31388](https://issues.apache.org/jira/browse/SPARK-31388) | Improvement | org.apache.spark.sql.hive.thriftserver.CliSuite result matching is flaky |
| 3.0.0 | [SPARK-31398](https://issues.apache.org/jira/browse/SPARK-31398) | Improvement | Speed up reading dates in ORC |
| 3.0.0 | [SPARK-31582](https://issues.apache.org/jira/browse/SPARK-31582) | New Feature | Being able to not populate Hadoop classpath |
| 3.0.0 | [SPARK-31596](https://issues.apache.org/jira/browse/SPARK-31596) | Improvement | Generate SQL Configurations from hive module to configuration doc |
| 3.1.1 | [SPARK-12312](https://issues.apache.org/jira/browse/SPARK-12312) | prose | Support JDBC Kerberos with keytab |
| 3.1.1 | [SPARK-20628](https://issues.apache.org/jira/browse/SPARK-20628) | prose | Basic framework |
| 3.1.1 | [SPARK-25557](https://issues.apache.org/jira/browse/SPARK-25557) | prose | Nested column predicate pushdown for ORC |
| 3.1.1 | [SPARK-30613](https://issues.apache.org/jira/browse/SPARK-30613) | prose | Support Hive style REPLACE COLUMNS syntax |
| 3.1.1 | [SPARK-30648](https://issues.apache.org/jira/browse/SPARK-30648) | prose | Support filters pushdown in JSON datasource |
| 3.1.1 | [SPARK-31486](https://issues.apache.org/jira/browse/SPARK-31486) | prose | Add spark.submit.waitForCompletion configuration to control spark-submit exit in Standalone cluster mode |
| 3.1.1 | [SPARK-31960](https://issues.apache.org/jira/browse/SPARK-31960) | prose | Do not propagate Hadoopâs classpath for Spark distribution with built-in Hadoop |
| 3.1.1 | [SPARK-32001](https://issues.apache.org/jira/browse/SPARK-32001) | prose | Create JDBC authentication provider developer API |
| 3.1.1 | [SPARK-32047](https://issues.apache.org/jira/browse/SPARK-32047) | prose | Add JDBC connection provider disable possibility |
| 3.1.1 | [SPARK-32270](https://issues.apache.org/jira/browse/SPARK-32270) | prose | Leverage SQL text data source during CSV schema inference |
| 3.1.1 | [SPARK-32346](https://issues.apache.org/jira/browse/SPARK-32346) | prose | Support filters pushdown in Avro datasource |
| 3.1.1 | [SPARK-32375](https://issues.apache.org/jira/browse/SPARK-32375) | prose | Implement catalog APIs for JDBC |
| 3.1.1 | [SPARK-32639](https://issues.apache.org/jira/browse/SPARK-32639) | prose | Allow complex type in mapâs key type in Parquet |
| 3.1.1 | [SPARK-33050](https://issues.apache.org/jira/browse/SPARK-33050) | prose | Upgrade Apache ORC to 1.5.12 |
| 3.1.1 | [SPARK-33088](https://issues.apache.org/jira/browse/SPARK-33088) | prose | Enhance ExecutorPlugin API to include methods for task start and end events |
| 3.1.1 | [SPARK-33160](https://issues.apache.org/jira/browse/SPARK-33160) | prose | Allow saving/loading INT96 in Parquet without rebasing |
| 3.1.1 | [SPARK-33458](https://issues.apache.org/jira/browse/SPARK-33458) | prose | Support contains, starts-with and ends-with filters |
| 3.1.1 | [SPARK-33477](https://issues.apache.org/jira/browse/SPARK-33477) | prose | Support filter by date type |
| 3.1.1 | [SPARK-33504](https://issues.apache.org/jira/browse/SPARK-33504) | prose | Redact sensitive attributes of application log in SHS |
| 3.1.1 | [SPARK-33530](https://issues.apache.org/jira/browse/SPARK-33530) | prose | Support –archives option natively |
| 3.1.1 | [SPARK-33537](https://issues.apache.org/jira/browse/SPARK-33537) | prose | Hive Metastore partition filter pushdown improvement |
| 3.1.1 | [SPARK-33582](https://issues.apache.org/jira/browse/SPARK-33582) | prose | Support filter by not-equals |
| 3.1.1 | [SPARK-33790](https://issues.apache.org/jira/browse/SPARK-33790) | Improvement | Reduce the rpc call of getFileStatus in SingleFileEventLogFileReader |
| 3.2.0 | [SPARK-26345](https://issues.apache.org/jira/browse/SPARK-26345) | Umbrella | Parquet support Column indexes |
| 3.2.0 | [SPARK-26836](https://issues.apache.org/jira/browse/SPARK-26836) | prose | Supporting Avro schema evolution for partitioned Hive tables with “avro.schema.literal” |
| 3.2.0 | [SPARK-29250](https://issues.apache.org/jira/browse/SPARK-29250) | Improvement | Upgrade to Hadoop 3.3.1 |
| 3.2.0 | [SPARK-32668](https://issues.apache.org/jira/browse/SPARK-32668) | Improvement | HiveGenericUDTF initialize UDTF should use StructObjectInspector method |
| 3.2.0 | [SPARK-32703](https://issues.apache.org/jira/browse/SPARK-32703) | Improvement | Replace deprecated API calls from SpecificParquetRecordReaderBase |
| 3.2.0 | [SPARK-32792](https://issues.apache.org/jira/browse/SPARK-32792) | prose | Improve Parquet In filter pushdown |
| 3.2.0 | [SPARK-32864](https://issues.apache.org/jira/browse/SPARK-32864) | Improvement | Support ORC forced positional evolution |
| 3.2.0 | [SPARK-33212](https://issues.apache.org/jira/browse/SPARK-33212) | Improvement | Upgrade to Hadoop 3.2.2 and move to shaded clients for Hadoop 3.x profile |
| 3.2.0 | [SPARK-33526](https://issues.apache.org/jira/browse/SPARK-33526) | Improvement | Add config to control if cancel invoke interrupt task on thriftserver |
| 3.2.0 | [SPARK-33532](https://issues.apache.org/jira/browse/SPARK-33532) | Improvement | Remove unreachable branch in SpecificParquetRecordReaderBase.initialize method |
| 3.2.0 | [SPARK-33655](https://issues.apache.org/jira/browse/SPARK-33655) | Improvement | Thrift server : FETCH_PRIOR does not cause to reiterate from start position. |
| 3.2.0 | [SPARK-33700](https://issues.apache.org/jira/browse/SPARK-33700) | Improvement | Try to push down filters for parquet and orc should add extra `filters.nonEmpty` condition |
| 3.2.0 | [SPARK-33750](https://issues.apache.org/jira/browse/SPARK-33750) | Improvement | Use `hadoop-3.2` distribution in HiveExternalCatalogVersionsSuite |
| 3.2.0 | [SPARK-33790](https://issues.apache.org/jira/browse/SPARK-33790) | Improvement | Reduce the rpc call of getFileStatus in SingleFileEventLogFileReader |
| 3.2.0 | [SPARK-33812](https://issues.apache.org/jira/browse/SPARK-33812) | Improvement | splt the histogram column stats when saving to hive metastore as table property |
| 3.2.0 | [SPARK-33932](https://issues.apache.org/jira/browse/SPARK-33932) | Improvement | Clean up KafkaOffsetReader API document |
| 3.2.0 | [SPARK-33937](https://issues.apache.org/jira/browse/SPARK-33937) | Improvement | Move the old partition data to trash instead of deleting it when inserting rewrite hive table |
| 3.2.0 | [SPARK-33940](https://issues.apache.org/jira/browse/SPARK-33940) | prose | Upgrade univocity-parsers to 2.9.1 |
| 3.2.0 | [SPARK-34029](https://issues.apache.org/jira/browse/SPARK-34029) | Improvement | Add OrcEncryptionSuite and FakeKeyProvider |
| 3.2.0 | [SPARK-34186](https://issues.apache.org/jira/browse/SPARK-34186) | Improvement | Fix DockerJDBCIntegrationSuites to reflect the change of SPARK-33888 |
| 3.2.0 | [SPARK-34271](https://issues.apache.org/jira/browse/SPARK-34271) | Improvement | Use majorMinorPatchVersion for Hive version parsing |
| 3.2.0 | [SPARK-34289](https://issues.apache.org/jira/browse/SPARK-34289) | prose | Support column index in Parquet vectorized reader |
| 3.2.0 | [SPARK-34357](https://issues.apache.org/jira/browse/SPARK-34357) | Improvement | Map JDBC SQL TIME type to TimestampType with time portion fixed regardless of timezone |
| 3.2.0 | [SPARK-34365](https://issues.apache.org/jira/browse/SPARK-34365) | Improvement | Support configurable Avro schema field matching for positional or by-name |
| 3.2.0 | [SPARK-34377](https://issues.apache.org/jira/browse/SPARK-34377) | New Feature | Support parquet datasource options to control datetime rebasing in read |
| 3.2.0 | [SPARK-34385](https://issues.apache.org/jira/browse/SPARK-34385) | Improvement | Unwrap SparkUpgradeException in v2 Parquet datasource |
| 3.2.0 | [SPARK-34404](https://issues.apache.org/jira/browse/SPARK-34404) | New Feature | Support Avro datasource options to control datetime rebasing in read |
| 3.2.0 | [SPARK-34416](https://issues.apache.org/jira/browse/SPARK-34416) | Improvement | Support avroSchemaUrl in addition to avroSchema |
| 3.2.0 | [SPARK-34535](https://issues.apache.org/jira/browse/SPARK-34535) | Improvement | Cleanup unused symbol in Orc related code |
| 3.2.0 | [SPARK-34538](https://issues.apache.org/jira/browse/SPARK-34538) | prose | Hive Metastore support filter by NOT IN |
| 3.2.0 | [SPARK-34542](https://issues.apache.org/jira/browse/SPARK-34542) | Improvement | Upgrade Parquet to 1.12.0 |
| 3.2.0 | [SPARK-34562](https://issues.apache.org/jira/browse/SPARK-34562) | Improvement | Leverage parquet bloom filters |
| 3.2.0 | [SPARK-34578](https://issues.apache.org/jira/browse/SPARK-34578) | Improvement | Ignore ORC encryption tests when ORC is loaded by old Hadoop library by other tests |
| 3.2.0 | [SPARK-34597](https://issues.apache.org/jira/browse/SPARK-34597) | Improvement | Replaces `ParquetFileReader.readFooter` with `ParquetFileReader.open and getFooter` |
| 3.2.0 | [SPARK-34712](https://issues.apache.org/jira/browse/SPARK-34712) | Improvement | Refactor UT about hive UT |
| 3.2.0 | [SPARK-34778](https://issues.apache.org/jira/browse/SPARK-34778) | Improvement | Upgrade to Avro 1.10.2 |
| 3.2.0 | [SPARK-34786](https://issues.apache.org/jira/browse/SPARK-34786) | prose | Read Parquet unsigned int64 logical type that stored as signed int64 physical type to decimal(20, 0) |
| 3.2.0 | [SPARK-34809](https://issues.apache.org/jira/browse/SPARK-34809) | Improvement | Enable spark.hadoopRDD.ignoreEmptySplits by default |
| 3.2.0 | [SPARK-34815](https://issues.apache.org/jira/browse/SPARK-34815) | Improvement | Update CSVBenchmark |
| 3.2.0 | [SPARK-34816](https://issues.apache.org/jira/browse/SPARK-34816) | Improvement | Support for Parquet unsigned LogicalTypes |
| 3.2.0 | [SPARK-34817](https://issues.apache.org/jira/browse/SPARK-34817) | prose | Read parquet unsigned types that are stored as int32 physical type in parquet |
| 3.2.0 | [SPARK-34843](https://issues.apache.org/jira/browse/SPARK-34843) | Improvement | JDBCRelation columnPartition function improperly determines stride size. Upper bound is skewed due to stride alignment. |
| 3.2.0 | [SPARK-34852](https://issues.apache.org/jira/browse/SPARK-34852) | Improvement | Close Hive session state should use withHiveState |
| 3.2.0 | [SPARK-34859](https://issues.apache.org/jira/browse/SPARK-34859) | prose | Handle column index when using vectorized Parquet reader |
| 3.2.0 | [SPARK-34862](https://issues.apache.org/jira/browse/SPARK-34862) | prose | Support nested column in ORC vectorized reader |
| 3.2.0 | [SPARK-34973](https://issues.apache.org/jira/browse/SPARK-34973) | Improvement | Cleanup unused fields and methods in vectorized Parquet reader |
| 3.2.0 | [SPARK-35003](https://issues.apache.org/jira/browse/SPARK-35003) | Improvement | Improve performance for reading smallint in vectorized Parquet reader |
| 3.2.0 | [SPARK-35044](https://issues.apache.org/jira/browse/SPARK-35044) | Improvement | Support retrieve hadoop configurations via SET syntax |
| 3.2.0 | [SPARK-35047](https://issues.apache.org/jira/browse/SPARK-35047) | Improvement | Allow Json datasources to write non-ascii characters as codepoints |
| 3.2.0 | [SPARK-35226](https://issues.apache.org/jira/browse/SPARK-35226) | prose | Support refreshKrb5Config option in JDBC data sources |
| 3.2.0 | [SPARK-35325](https://issues.apache.org/jira/browse/SPARK-35325) | Improvement | Add nested column ORC encryption test case |
| 3.2.0 | [SPARK-35383](https://issues.apache.org/jira/browse/SPARK-35383) | Improvement | Improve s3a magic committer support by inferring missing configs |
| 3.2.0 | [SPARK-35611](https://issues.apache.org/jira/browse/SPARK-35611) | Improvement | Introduce the strategy on mismatched offset for start offset timestamp on Kafka data source |
| 3.2.0 | [SPARK-35612](https://issues.apache.org/jira/browse/SPARK-35612) | Improvement | Support LZ4 compression in ORC data source |
| 3.2.0 | [SPARK-35658](https://issues.apache.org/jira/browse/SPARK-35658) | Improvement | Document Parquet encryption feature in Spark |
| 3.2.0 | [SPARK-35747](https://issues.apache.org/jira/browse/SPARK-35747) | Improvement | Avoid printing full Exception stack trace, if HBase/Kafka/Hive services are not running in a secure cluster |
| 3.2.0 | [SPARK-35783](https://issues.apache.org/jira/browse/SPARK-35783) | prose | Set the list of read columns in the task configuration to reduce reading of ORC data |
| 3.2.0 | [SPARK-35844](https://issues.apache.org/jira/browse/SPARK-35844) | Improvement | Add hadoop-cloud profile to PUBLISH_PROFILES |
| 3.2.0 | [SPARK-35990](https://issues.apache.org/jira/browse/SPARK-35990) | Improvement | Remove avro-sbt plugin dependency |
| 3.2.0 | [SPARK-36128](https://issues.apache.org/jira/browse/SPARK-36128) | prose | Apply spark.sql.hive.metastorePartitionPruning for non-Hive tables that uses Hive metastore for partition management |
| 3.2.0 | [SPARK-36269](https://issues.apache.org/jira/browse/SPARK-36269) | Improvement | Fix only set data columns to Hive column names config |
| 3.2.0 | [SPARK-36482](https://issues.apache.org/jira/browse/SPARK-36482) | Improvement | Bump orc to 1.6.10 |
| 3.2.0 | [SPARK-36726](https://issues.apache.org/jira/browse/SPARK-36726) | prose | Upgrade Apache Parquet used to version 1.12.1 |
| 3.3.0 | [SPARK-27442](https://issues.apache.org/jira/browse/SPARK-27442) | prose | Remove check field name when reading/writing data in parquet |
| 3.3.0 | [SPARK-30062](https://issues.apache.org/jira/browse/SPARK-30062) | prose | Add the IMMEDIATE statement to the DB2 dialect truncate implementation |
| 3.3.0 | [SPARK-32709](https://issues.apache.org/jira/browse/SPARK-32709) | prose | Support writing Hive bucketed table (Parquet/ORC format with Hive hash) |
| 3.3.0 | [SPARK-32712](https://issues.apache.org/jira/browse/SPARK-32712) | prose | Support writing Hive bucketed table (Hive file formats with Hive hash) |
| 3.3.0 | [SPARK-34863](https://issues.apache.org/jira/browse/SPARK-34863) | prose | Support complex types for Parquet vectorized reader |
| 3.3.0 | [SPARK-34960](https://issues.apache.org/jira/browse/SPARK-34960) | prose | Aggregate push down for ORC |
| 3.3.0 | [SPARK-35437](https://issues.apache.org/jira/browse/SPARK-35437) | prose | Use expressions to filter Hive partitions at client side |
| 3.3.0 | [SPARK-35561](https://issues.apache.org/jira/browse/SPARK-35561) | prose | Remove leading zeros from empty static number type partition |
| 3.3.0 | [SPARK-35912](https://issues.apache.org/jira/browse/SPARK-35912) | prose | Add a legacy configuration for respecting nullability in DataFrame.schema.csv/json(ds) |
| 3.3.0 | [SPARK-36163](https://issues.apache.org/jira/browse/SPARK-36163) | prose | Propagate correct JDBC properties in JDBC connector provider and add “connectionProvider” option |
| 3.3.0 | [SPARK-36404](https://issues.apache.org/jira/browse/SPARK-36404) | prose | Support nested columns in ORC vectorized reader for data source V2 |
| 3.3.0 | [SPARK-36536](https://issues.apache.org/jira/browse/SPARK-36536) | prose | Use CAST for datetime in CSV/JSON by default |
| 3.3.0 | [SPARK-36645](https://issues.apache.org/jira/browse/SPARK-36645) | prose | Aggregate (Min/Max/Count) push down for Parquet |
| 3.3.0 | [SPARK-36663](https://issues.apache.org/jira/browse/SPARK-36663) | prose | Support number-only column names in ORC data sources |
| 3.3.0 | [SPARK-36876](https://issues.apache.org/jira/browse/SPARK-36876) | prose | Support Dynamic Partition pruning for HiveTableScanExec |
| 3.3.0 | [SPARK-36879](https://issues.apache.org/jira/browse/SPARK-36879) | prose | Support Parquet V2 data page encoding (DELTA_BINARY_PACKED) for the vectorized path |
| 3.3.0 | [SPARK-36913](https://issues.apache.org/jira/browse/SPARK-36913) | prose | Implement createIndex and IndexExists in DS V2 JDBC (MySQL dialect) |
| 3.3.0 | [SPARK-36914](https://issues.apache.org/jira/browse/SPARK-36914) | prose | Implement dropIndex and listIndexes in JDBC (MySQL dialect) |
| 3.3.0 | [SPARK-37286](https://issues.apache.org/jira/browse/SPARK-37286) | prose | Move compileAggregates from JDBCRDD to JdbcDialect |
| 3.3.0 | [SPARK-37483](https://issues.apache.org/jira/browse/SPARK-37483) | prose | Support push down top N to JDBC data source V2 |
| 3.3.0 | [SPARK-37705](https://issues.apache.org/jira/browse/SPARK-37705) | prose | Rebase timestamps in the session time zone saved in Parquet/Avro metadata |
| 3.3.0 | [SPARK-37864](https://issues.apache.org/jira/browse/SPARK-37864) | prose | Support vectorized read boolean values use RLE encoding with Parquet DataPage V2 |
| 3.3.0 | [SPARK-37867](https://issues.apache.org/jira/browse/SPARK-37867) | prose | Support aggregate functions of build-in JDBC dialect |
| 3.3.0 | [SPARK-37965](https://issues.apache.org/jira/browse/SPARK-37965) | prose | Remove check field name when reading/writing existing data in ORC |
| 3.3.0 | [SPARK-37974](https://issues.apache.org/jira/browse/SPARK-37974) | prose | Implement vectorized DELTA_BYTE_ARRAY and DELTA_LENGTH_BYTE_ARRAY encodings for Parquet V2 support |
| 3.3.0 | [SPARK-38054](https://issues.apache.org/jira/browse/SPARK-38054) | prose | Supports list namespaces in JDBC V2 MySQL dialect |
| 3.3.0 | [SPARK-38094](https://issues.apache.org/jira/browse/SPARK-38094) | prose | Enable matching schema column names by field ids |
| 3.3.0 | [SPARK-38196](https://issues.apache.org/jira/browse/SPARK-38196) | prose | Reactor framework so as JDBC dialect could compile expression by itself |
| 3.3.0 | [SPARK-38236](https://issues.apache.org/jira/browse/SPARK-38236) | prose | Treat table location as absolute when the first letter of its path is slash in create/alter table |
| 3.3.0 | [SPARK-38361](https://issues.apache.org/jira/browse/SPARK-38361) | prose | Add factory method getConnection into JDBCDialect |
| 3.3.0 | [SPARK-38432](https://issues.apache.org/jira/browse/SPARK-38432) | prose | Refactor framework so as JDBC dialect could compile filter by self way |
| 3.3.0 | [SPARK-38437](https://issues.apache.org/jira/browse/SPARK-38437) | prose | Lenient serialization of datetime from datasource |
| 3.3.0 | [SPARK-38633](https://issues.apache.org/jira/browse/SPARK-38633) | prose | Support push down Cast to JDBC data source V2 |
| 3.3.0 | [SPARK-39193](https://issues.apache.org/jira/browse/SPARK-39193) | prose | Fasten Timestamp type inference of default format in JSON/CSV data source |
| 3.3.1 | [SPARK-39951](https://issues.apache.org/jira/browse/SPARK-39951) | prose | Update Parquet V2 columnar check for nested fields |
| 3.3.1 | [SPARK-40280](https://issues.apache.org/jira/browse/SPARK-40280) | prose | Add support for parquet push down for annotated int and long |
| 3.3.2 | [SPARK-39951](https://issues.apache.org/jira/browse/SPARK-39951) | prose | Update Parquet V2 columnar check for nested fields |
| 3.3.2 | [SPARK-40280](https://issues.apache.org/jira/browse/SPARK-40280) | prose | Add support for parquet push down for annotated int and long |
| 3.4.0 | [SPARK-37259](https://issues.apache.org/jira/browse/SPARK-37259) | prose | Support CTE and temp table queries with MSSQL JDBC |
| 3.4.0 | [SPARK-37980](https://issues.apache.org/jira/browse/SPARK-37980) | prose | Extend METADATA column to support row indexes for Parquet files |
| 3.4.0 | [SPARK-39002](https://issues.apache.org/jira/browse/SPARK-39002) | prose | StringEndsWith/Contains support push down to Parquet so that we can leverage dictionary filter |
| 3.4.0 | [SPARK-39086](https://issues.apache.org/jira/browse/SPARK-39086) | prose | Support UDT in Spark Parquet vectorized reader |
| 3.4.0 | [SPARK-39469](https://issues.apache.org/jira/browse/SPARK-39469) | prose | Infer DATE type for CSV schema inference |
| 3.4.0 | [SPARK-41096](https://issues.apache.org/jira/browse/SPARK-41096) | prose | Support reading parquet FIXED_LEN_BYTE_ARRAY type |
| 3.4.0 | [SPARK-41589](https://issues.apache.org/jira/browse/SPARK-41589) | prose | Implement PyTorch Distributor |
| 3.4.0 | [SPARK-42051](https://issues.apache.org/jira/browse/SPARK-42051) | prose | Codegen Support for HiveGenericUDF |
| 3.5.0 | [SPARK-25050](https://issues.apache.org/jira/browse/SPARK-25050) | prose | Avro: writing complex unions |
| 3.5.0 | [SPARK-39280](https://issues.apache.org/jira/browse/SPARK-39280) | prose | Speed up Timestamp type inference with user-provided format in JSON/CSV data source |
| 3.5.0 | [SPARK-39281](https://issues.apache.org/jira/browse/SPARK-39281) | prose | Speed up Timestamp type inference with legacy format in JSON/CSV data source |
| 3.5.0 | [SPARK-41516](https://issues.apache.org/jira/browse/SPARK-41516) | prose | Allow jdbc dialects to override the query used to create a table |
| 3.5.0 | [SPARK-42051](https://issues.apache.org/jira/browse/SPARK-42051) | prose | Codegen Support for HiveGenericUDF |
| 3.5.0 | [SPARK-42052](https://issues.apache.org/jira/browse/SPARK-42052) | prose | Codegen Support for HiveSimpleUDF |
| 3.5.0 | [SPARK-42169](https://issues.apache.org/jira/browse/SPARK-42169) | prose | Implement code generation for to_csv function (StructsToCsv) |
| 3.5.0 | [SPARK-42237](https://issues.apache.org/jira/browse/SPARK-42237) | prose | Change binary to unsupported dataType in CSV format |
| 3.5.0 | [SPARK-42904](https://issues.apache.org/jira/browse/SPARK-42904) | prose | Char/Varchar Support for JDBC Catalog |
| 3.5.0 | [SPARK-43119](https://issues.apache.org/jira/browse/SPARK-43119) | prose | Support Get SQL Keywords Dynamically Thru JDBC API and TVF |
| 3.5.0 | [SPARK-43273](https://issues.apache.org/jira/browse/SPARK-43273) | prose | Support lz4raw compression codec for Parquet |
| 3.5.0 | [SPARK-43333](https://issues.apache.org/jira/browse/SPARK-43333) | prose | Allow Avro to convert union type to SQL with field name stable with type |
| 3.5.0 | [SPARK-43901](https://issues.apache.org/jira/browse/SPARK-43901) | prose | Avro to Support custom decimal type backed by Long |
<!-- AUTO:timeline END -->
