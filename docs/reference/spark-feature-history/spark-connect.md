# Spark Connect

> Source: `release-notes/spark_all_changelogs.txt`. Timeline rows below are generated from `_catalog.jsonl`; prose outside the AUTO markers is hand-written.

## How it evolved

### 3.x era — Spark Connect is born

Spark Connect did not exist before 3.4.0, which introduced it as a decoupled client/server architecture: a thin Python client (SPARK-39375) talking to a Spark server over gRPC, with DataFrame, Column, Functions, SparkSession, I/O, and Catalog APIs (SPARK-41279, SPARK-41282, SPARK-41283, SPARK-41281, SPARK-41284, SPARK-41289) plus Python UDF support (SPARK-41661) and the first cut of a Scala client (SPARK-41534). 3.5.0 broadened the client surface substantially: Scala and Go clients matured (SPARK-42554), a Go client shipped its initial version (SPARK-43351), and Structured Streaming (SPARK-42938) and the pandas API (SPARK-42497) both gained Connect support, alongside a `sql`/`sql-api` module split (SPARK-44273) to keep the client's dependency footprint minimal — moving Connect from a Python-only proof of concept toward a multi-language, feature-complete remote execution client in two releases.

### 4.x era — GetStatus API and History Server tab

4.0.0 built Scala Client parity with the classic Dataset/DataFrame API (SPARK-49248) and unified the SQL Scala interface shared by regular SQL and Connect (SPARK-48918), while also bringing `pyspark.ml` (SPARK-50812) and dozens of DataFrame/Column/plotting APIs onto Connect — the client was effectively catching the rest of PySpark and Scala up to where Classic already stood. 4.1.0 added a JDBC driver for Spark Connect as a SPIP (SPARK-53484), `transformWithState` support (SPARK-51827), a `CloneSession` RPC (SPARK-53455), and server-side column-name validation (SPARK-52723).

4.2.0 added a server-side `GetStatus` API for monitoring execution status with matching client support (SPARK-55606, SPARK-55691), made the Spark Connect tab available in the History Server (SPARK-57601) — giving Connect the observability its 3.5.0 UI page started — a client-side limit for local relation size (SPARK-55047), and RDD API compatibility including `zipWithIndex` for both Scala and PySpark (SPARK-55227, SPARK-55228, SPARK-55229).

## Timeline

<!-- AUTO:timeline START -->
| Release | JIRA | Type | Title |
|---|---|---|---|
| 3.4.0 | [SPARK-39375](https://issues.apache.org/jira/browse/SPARK-39375) | prose | Python client for Spark Connect |
| 3.4.0 | [SPARK-40451](https://issues.apache.org/jira/browse/SPARK-40451) | prose | Type annotations for Spark Connect Python Client |
| 3.4.0 | [SPARK-41279](https://issues.apache.org/jira/browse/SPARK-41279) | prose | Implement DataFrame API |
| 3.4.0 | [SPARK-41281](https://issues.apache.org/jira/browse/SPARK-41281) | prose | Implement SparkSession API |
| 3.4.0 | [SPARK-41282](https://issues.apache.org/jira/browse/SPARK-41282) | prose | Implement Column API |
| 3.4.0 | [SPARK-41283](https://issues.apache.org/jira/browse/SPARK-41283) | prose | Implement Functions API |
| 3.4.0 | [SPARK-41284](https://issues.apache.org/jira/browse/SPARK-41284) | prose | Implement I/O API |
| 3.4.0 | [SPARK-41286](https://issues.apache.org/jira/browse/SPARK-41286) | prose | Build, package and infrastructure for Spark Connect |
| 3.4.0 | [SPARK-41289](https://issues.apache.org/jira/browse/SPARK-41289) | prose | Implement Catalog API |
| 3.4.0 | [SPARK-41534](https://issues.apache.org/jira/browse/SPARK-41534) | prose | Implement basic Scala Client |
| 3.4.0 | [SPARK-41661](https://issues.apache.org/jira/browse/SPARK-41661) | prose | Support for User-defined Functions in Python |
| 3.4.0 | [SPARK-42172](https://issues.apache.org/jira/browse/SPARK-42172) | prose | Test infrastructure for Spark Connect |
| 3.4.0 | [SPARK-42283](https://issues.apache.org/jira/browse/SPARK-42283) | prose | Basic User Defined Function support |
| 3.4.0 | [SPARK-42393](https://issues.apache.org/jira/browse/SPARK-42393) | prose | Support for Pandas/Arrow Function API |
| 3.4.0 | [SPARK-42440](https://issues.apache.org/jira/browse/SPARK-42440) | prose | Implement Dataframe API |
| 3.4.0 | [SPARK-42441](https://issues.apache.org/jira/browse/SPARK-42441) | prose | Implement Column API |
| 3.4.0 | [SPARK-42457](https://issues.apache.org/jira/browse/SPARK-42457) | prose | Implement I/O API |
| 3.4.0 | [SPARK-42461](https://issues.apache.org/jira/browse/SPARK-42461) | prose | Implement Functions API |
| 3.4.0 | [SPARK-42499](https://issues.apache.org/jira/browse/SPARK-42499) | prose | Support for Runtime SQL configuration |
| 3.4.0 | [SPARK-42580](https://issues.apache.org/jira/browse/SPARK-42580) | prose | Basic Typed API support |
| 3.4.0 | [SPARK-42586](https://issues.apache.org/jira/browse/SPARK-42586) | prose | Implement Runtime SQL configuration |
| 3.4.0 | [SPARK-42639](https://issues.apache.org/jira/browse/SPARK-42639) | prose | Implement SparkSession API |
| 3.4.0 | [SPARK-42656](https://issues.apache.org/jira/browse/SPARK-42656) | prose | Implement REPL Support |
| 3.5.0 | [SPARK-42471](https://issues.apache.org/jira/browse/SPARK-42471) | prose | PyTorch-based distributed ML Support for Spark Connect |
| 3.5.0 | [SPARK-42497](https://issues.apache.org/jira/browse/SPARK-42497) | prose | Pandas API support for the Python Spark Connect Client |
| 3.5.0 | [SPARK-42554](https://issues.apache.org/jira/browse/SPARK-42554) | prose | Scala and Go client support in Spark Connect |
| 3.5.0 | [SPARK-42938](https://issues.apache.org/jira/browse/SPARK-42938) | prose | Structured Streaming support for Spark Connect in Python and Scala |
| 3.5.0 | [SPARK-43351](https://issues.apache.org/jira/browse/SPARK-43351) | prose | Initial version of the Go client |
| 3.5.0 | [SPARK-44273](https://issues.apache.org/jira/browse/SPARK-44273) | prose | Refactoring of the sql module into sql and sql-api to produce a minimum set of dependencies that can be shared between the Scala Spark Conne |
| 4.0.0 | [SPARK-41065](https://issues.apache.org/jira/browse/SPARK-41065) | prose | Implement DataFrame.freqItems and DataFrame.stat.freqItems |
| 4.0.0 | [SPARK-41066](https://issues.apache.org/jira/browse/SPARK-41066) | prose | Implement DataFrame.sampleBy and DataFrame.stat.sampleBy |
| 4.0.0 | [SPARK-41067](https://issues.apache.org/jira/browse/SPARK-41067) | prose | Implement DataFrame.stat.cov |
| 4.0.0 | [SPARK-41068](https://issues.apache.org/jira/browse/SPARK-41068) | prose | Implement DataFrame.stat.corr |
| 4.0.0 | [SPARK-41069](https://issues.apache.org/jira/browse/SPARK-41069) | prose | Implement DataFrame.approxQuantile and DataFrame.stat.approxQuantile |
| 4.0.0 | [SPARK-41292](https://issues.apache.org/jira/browse/SPARK-41292) | prose | Implement Window functions |
| 4.0.0 | [SPARK-41333](https://issues.apache.org/jira/browse/SPARK-41333) | prose | Implement GroupedData.{min, max, avg, sum} |
| 4.0.0 | [SPARK-41364](https://issues.apache.org/jira/browse/SPARK-41364) | prose | Implement broadcast function |
| 4.0.0 | [SPARK-41383](https://issues.apache.org/jira/browse/SPARK-41383) | prose | Implement rollup , cube , and pivot |
| 4.0.0 | [SPARK-41434](https://issues.apache.org/jira/browse/SPARK-41434) | prose | Initial LambdaFunction implementation |
| 4.0.0 | [SPARK-41440](https://issues.apache.org/jira/browse/SPARK-41440) | prose | Implement DataFrame.randomSplit |
| 4.0.0 | [SPARK-41464](https://issues.apache.org/jira/browse/SPARK-41464) | prose | Implement DataFrame.to |
| 4.0.0 | [SPARK-41473](https://issues.apache.org/jira/browse/SPARK-41473) | prose | Implement format_number function |
| 4.0.0 | [SPARK-41503](https://issues.apache.org/jira/browse/SPARK-41503) | prose | Implement Partition Transformation Functions |
| 4.0.0 | [SPARK-41529](https://issues.apache.org/jira/browse/SPARK-41529) | prose | Implement SparkSession.stop |
| 4.0.0 | [SPARK-41629](https://issues.apache.org/jira/browse/SPARK-41629) | prose | Support for Protocol Extensions in Relation and Expression |
| 4.0.0 | [SPARK-41663](https://issues.apache.org/jira/browse/SPARK-41663) | prose | Implement the rest of Lambda functions |
| 4.0.0 | [SPARK-41673](https://issues.apache.org/jira/browse/SPARK-41673) | prose | Implement Column.astype |
| 4.0.0 | [SPARK-41707](https://issues.apache.org/jira/browse/SPARK-41707) | prose | Implement Catalog API in Spark Connect |
| 4.0.0 | [SPARK-41710](https://issues.apache.org/jira/browse/SPARK-41710) | prose | Implement Column.between |
| 4.0.0 | [SPARK-41722](https://issues.apache.org/jira/browse/SPARK-41722) | prose | Implement 3 missing time window functions |
| 4.0.0 | [SPARK-41723](https://issues.apache.org/jira/browse/SPARK-41723) | prose | Implement sequence function |
| 4.0.0 | [SPARK-41724](https://issues.apache.org/jira/browse/SPARK-41724) | prose | Implement call_udf function |
| 4.0.0 | [SPARK-41728](https://issues.apache.org/jira/browse/SPARK-41728) | prose | Implement unwrap_udt function |
| 4.0.0 | [SPARK-41731](https://issues.apache.org/jira/browse/SPARK-41731) | prose | Implement the column accessor ( getItem , getField , getitem , etc.) |
| 4.0.0 | [SPARK-41740](https://issues.apache.org/jira/browse/SPARK-41740) | prose | Implement Column.name |
| 4.0.0 | [SPARK-41767](https://issues.apache.org/jira/browse/SPARK-41767) | prose | Implement Column.{withField, dropFields} |
| 4.0.0 | [SPARK-41785](https://issues.apache.org/jira/browse/SPARK-41785) | prose | Implement GroupedData.mean |
| 4.0.0 | [SPARK-41803](https://issues.apache.org/jira/browse/SPARK-41803) | prose | Add missing function log(arg1, arg2) |
| 4.0.0 | [SPARK-41811](https://issues.apache.org/jira/browse/SPARK-41811) | prose | Implement SQLStringFormatter with WithRelations |
| 4.0.0 | [SPARK-42664](https://issues.apache.org/jira/browse/SPARK-42664) | prose | Support bloomFilter function for DataFrameStatFunction s |
| 4.0.0 | [SPARK-43662](https://issues.apache.org/jira/browse/SPARK-43662) | prose | Support merge_asof in Spark Connect |
| 4.0.0 | [SPARK-43704](https://issues.apache.org/jira/browse/SPARK-43704) | prose | Support MultiIndex for to_series() in Spark Connect |
| 4.0.0 | [SPARK-44736](https://issues.apache.org/jira/browse/SPARK-44736) | prose | Add Dataset.explode to Spark Connect Scala Client |
| 4.0.0 | [SPARK-44740](https://issues.apache.org/jira/browse/SPARK-44740) | prose | Support specifying session_id in SPARK_REMOTE connection string |
| 4.0.0 | [SPARK-44747](https://issues.apache.org/jira/browse/SPARK-44747) | prose | Add missing SparkSession.Builder methods |
| 4.0.0 | [SPARK-44753](https://issues.apache.org/jira/browse/SPARK-44753) | prose | XML: pyspark SQL XML reader/writer |
| 4.0.0 | [SPARK-44761](https://issues.apache.org/jira/browse/SPARK-44761) | prose | Support DataStreamWriter.foreachBatch(VoidFunction2) |
| 4.0.0 | [SPARK-44788](https://issues.apache.org/jira/browse/SPARK-44788) | prose | Add from_xml and schema_of_xml to pyspark, Spark Connect, and SQL functions |
| 4.0.0 | [SPARK-44807](https://issues.apache.org/jira/browse/SPARK-44807) | prose | Add Dataset.metadataColumn to Scala Client |
| 4.0.0 | [SPARK-44877](https://issues.apache.org/jira/browse/SPARK-44877) | prose | Support python protobuf functions for Spark Connect |
| 4.0.0 | [SPARK-45000](https://issues.apache.org/jira/browse/SPARK-45000) | prose | Implement DataFrame.foreach |
| 4.0.0 | [SPARK-45001](https://issues.apache.org/jira/browse/SPARK-45001) | prose | Implement DataFrame.foreachPartition |
| 4.0.0 | [SPARK-45091](https://issues.apache.org/jira/browse/SPARK-45091) | prose | Function floor / round / bround now accept Column type scale |
| 4.0.0 | [SPARK-45121](https://issues.apache.org/jira/browse/SPARK-45121) | prose | Support Series.empty for Spark Connect |
| 4.0.0 | [SPARK-45137](https://issues.apache.org/jira/browse/SPARK-45137) | prose | Support map/array parameters in parameterized sql() |
| 4.0.0 | [SPARK-45143](https://issues.apache.org/jira/browse/SPARK-45143) | prose | Make PySpark compatible with PyArrow 13.0.0 |
| 4.0.0 | [SPARK-45190](https://issues.apache.org/jira/browse/SPARK-45190) | prose | Make from_xml support StructType schema |
| 4.0.0 | [SPARK-45235](https://issues.apache.org/jira/browse/SPARK-45235) | prose | Support map and array parameters by sql() |
| 4.0.0 | [SPARK-45485](https://issues.apache.org/jira/browse/SPARK-45485) | prose | User agent improvements: Use SPARK_CONNECT_USER_AGENT env variable and include environment specific attributes |
| 4.0.0 | [SPARK-45506](https://issues.apache.org/jira/browse/SPARK-45506) | prose | Add ivy URI support to SparkcConnect addArtifact |
| 4.0.0 | [SPARK-45619](https://issues.apache.org/jira/browse/SPARK-45619) | prose | Apply the observed metrics to Observation object |
| 4.0.0 | [SPARK-45733](https://issues.apache.org/jira/browse/SPARK-45733) | prose | Support multiple retry policies |
| 4.0.0 | [SPARK-45851](https://issues.apache.org/jira/browse/SPARK-45851) | prose | Support multiple policies in scala client |
| 4.0.0 | [SPARK-46039](https://issues.apache.org/jira/browse/SPARK-46039) | prose | Upgrade grpcio\* to 1.59.3 for Python 3.12 |
| 4.0.0 | [SPARK-46085](https://issues.apache.org/jira/browse/SPARK-46085) | prose | Dataset.groupingSets in Scala Spark Connect client |
| 4.0.0 | [SPARK-46202](https://issues.apache.org/jira/browse/SPARK-46202) | prose | Expose new ArtifactManager APIs to support custom target directories |
| 4.0.0 | [SPARK-46229](https://issues.apache.org/jira/browse/SPARK-46229) | prose | Add applyInArrow to groupBy and cogroup in Spark Connect |
| 4.0.0 | [SPARK-46255](https://issues.apache.org/jira/browse/SPARK-46255) | prose | Support complex type -> string conversion |
| 4.0.0 | [SPARK-46465](https://issues.apache.org/jira/browse/SPARK-46465) | prose | Add Column.isNaN in PySpark |
| 4.0.0 | [SPARK-46620](https://issues.apache.org/jira/browse/SPARK-46620) | prose | Introduce a basic fallback mechanism for frame methods |
| 4.0.0 | [SPARK-46812](https://issues.apache.org/jira/browse/SPARK-46812) | prose | Make mapInPandas / mapInArrow support ResourceProfile |
| 4.0.0 | [SPARK-46919](https://issues.apache.org/jira/browse/SPARK-46919) | prose | Upgrade grpcio* and grpc-java to 1.62.x |
| 4.0.0 | [SPARK-47014](https://issues.apache.org/jira/browse/SPARK-47014) | prose | Implement methods dumpPerfProfile and dumpMemoryProfiles of SparkSession |
| 4.0.0 | [SPARK-47069](https://issues.apache.org/jira/browse/SPARK-47069) | prose | Introduce spark.profile.show / .dump for SparkSession-based profiling |
| 4.0.0 | [SPARK-47081](https://issues.apache.org/jira/browse/SPARK-47081) | prose | Support Query Execution Progress |
| 4.0.0 | [SPARK-47137](https://issues.apache.org/jira/browse/SPARK-47137) | prose | Add getAll to spark.conf for feature parity with Scala |
| 4.0.0 | [SPARK-47233](https://issues.apache.org/jira/browse/SPARK-47233) | prose | Client & Server logic for client-side streaming query listener |
| 4.0.0 | [SPARK-47276](https://issues.apache.org/jira/browse/SPARK-47276) | prose | Introduce spark.profile.clear for SparkSession-based profiling |
| 4.0.0 | [SPARK-47367](https://issues.apache.org/jira/browse/SPARK-47367) | prose | Support Python data sources with Spark Connect |
| 4.0.0 | [SPARK-47543](https://issues.apache.org/jira/browse/SPARK-47543) | prose | Infer dict as MapType from Pandas DataFrame (via new config) |
| 4.0.0 | [SPARK-47545](https://issues.apache.org/jira/browse/SPARK-47545) | prose | Dataset.observe for Scala Connect |
| 4.0.0 | [SPARK-47694](https://issues.apache.org/jira/browse/SPARK-47694) | prose | Make max message size configurable on the client side |
| 4.0.0 | [SPARK-47712](https://issues.apache.org/jira/browse/SPARK-47712) | prose | Allow connect plugins to create and process Datasets |
| 4.0.0 | [SPARK-47812](https://issues.apache.org/jira/browse/SPARK-47812) | prose | Support Serialization of SparkSession for ForEachBatch worker |
| 4.0.0 | [SPARK-47818](https://issues.apache.org/jira/browse/SPARK-47818) | prose | Introduce plan cache in SparkConnectPlanner to improve performance of Analyze requests |
| 4.0.0 | [SPARK-47845](https://issues.apache.org/jira/browse/SPARK-47845) | prose | Support Column type in split function for Scala and Python |
| 4.0.0 | [SPARK-47908](https://issues.apache.org/jira/browse/SPARK-47908) | prose | Parent classes for Spark Connect and Spark Classic |
| 4.0.0 | [SPARK-47909](https://issues.apache.org/jira/browse/SPARK-47909) | prose | Parent DataFrame class for Spark Connect and Spark Classic |
| 4.0.0 | [SPARK-48008](https://issues.apache.org/jira/browse/SPARK-48008) | prose | Support UDAFs in Spark Connect |
| 4.0.0 | [SPARK-48048](https://issues.apache.org/jira/browse/SPARK-48048) | prose | Added client side listener support for Scala |
| 4.0.0 | [SPARK-48112](https://issues.apache.org/jira/browse/SPARK-48112) | prose | Expose session in SparkConnectPlanner to plugins |
| 4.0.0 | [SPARK-48113](https://issues.apache.org/jira/browse/SPARK-48113) | prose | Allow Plugins to integrate with Spark Connect |
| 4.0.0 | [SPARK-48258](https://issues.apache.org/jira/browse/SPARK-48258) | prose | Checkpoint and localCheckpoint in Spark Connect |
| 4.0.0 | [SPARK-48510](https://issues.apache.org/jira/browse/SPARK-48510) | prose | Support UDAF toColumn API in Spark Connect |
| 4.0.0 | [SPARK-48555](https://issues.apache.org/jira/browse/SPARK-48555) | prose | Support using Columns as parameters for several functions ( array_remove , array_position , etc.) |
| 4.0.0 | [SPARK-48638](https://issues.apache.org/jira/browse/SPARK-48638) | prose | Add ExecutionInfo support for DataFrame |
| 4.0.0 | [SPARK-48794](https://issues.apache.org/jira/browse/SPARK-48794) | prose | DataFrame.mergeInto support for Spark Connect (Scala & Python) |
| 4.0.0 | [SPARK-48918](https://issues.apache.org/jira/browse/SPARK-48918) | prose | Create a unified SQL Scala interface shared by regular SQL and Connect |
| 4.0.0 | [SPARK-48960](https://issues.apache.org/jira/browse/SPARK-48960) | prose | Makes sparkâshell work with Spark Connect ( âremote support) |
| 4.0.0 | [SPARK-49027](https://issues.apache.org/jira/browse/SPARK-49027) | prose | Share Column API between Classic and Connect |
| 4.0.0 | [SPARK-49028](https://issues.apache.org/jira/browse/SPARK-49028) | prose | Create a shared SparkSession |
| 4.0.0 | [SPARK-49029](https://issues.apache.org/jira/browse/SPARK-49029) | prose | Create shared Dataset interface |
| 4.0.0 | [SPARK-49185](https://issues.apache.org/jira/browse/SPARK-49185) | prose | Reimplement kde plot with Spark SQL |
| 4.0.0 | [SPARK-49201](https://issues.apache.org/jira/browse/SPARK-49201) | prose | Reimplement hist plot with Spark SQL |
| 4.0.0 | [SPARK-49248](https://issues.apache.org/jira/browse/SPARK-49248) | prose | Scala Client Parity with existing Dataset/DataFrame API |
| 4.0.0 | [SPARK-49249](https://issues.apache.org/jira/browse/SPARK-49249) | prose | Add addArtifac t API to the Spark SQL Core |
| 4.0.0 | [SPARK-49273](https://issues.apache.org/jira/browse/SPARK-49273) | prose | Origin support for Spark Connect Scala client |
| 4.0.0 | [SPARK-49282](https://issues.apache.org/jira/browse/SPARK-49282) | prose | Create a shared SparkSessionBuilder interface |
| 4.0.0 | [SPARK-49284](https://issues.apache.org/jira/browse/SPARK-49284) | prose | Create a shared Catalog interface |
| 4.0.0 | [SPARK-49413](https://issues.apache.org/jira/browse/SPARK-49413) | prose | Create a shared RuntimeConfig interface |
| 4.0.0 | [SPARK-49416](https://issues.apache.org/jira/browse/SPARK-49416) | prose | Add shared DataStreamReader interface |
| 4.0.0 | [SPARK-49417](https://issues.apache.org/jira/browse/SPARK-49417) | prose | Add shared StreamingQueryManager interface |
| 4.0.0 | [SPARK-49419](https://issues.apache.org/jira/browse/SPARK-49419) | prose | Create shared DataFrameStatFunctions |
| 4.0.0 | [SPARK-49429](https://issues.apache.org/jira/browse/SPARK-49429) | prose | Add shared DataStreamWriter interface |
| 4.0.0 | [SPARK-49526](https://issues.apache.org/jira/browse/SPARK-49526) | prose | Support Windows-style paths in ArtifactManager |
| 4.0.0 | [SPARK-49531](https://issues.apache.org/jira/browse/SPARK-49531) | prose | Support line plot with plotly backend |
| 4.0.0 | [SPARK-49626](https://issues.apache.org/jira/browse/SPARK-49626) | prose | Support horizontal/vertical bar plots |
| 4.0.0 | [SPARK-49907](https://issues.apache.org/jira/browse/SPARK-49907) | prose | Support spark.ml on Connect |
| 4.0.0 | [SPARK-49948](https://issues.apache.org/jira/browse/SPARK-49948) | prose | Add “precision” parameter to pandas on Spark box plot |
| 4.0.0 | [SPARK-50050](https://issues.apache.org/jira/browse/SPARK-50050) | prose | Make lit accept str/bool numpy ndarray |
| 4.0.0 | [SPARK-50054](https://issues.apache.org/jira/browse/SPARK-50054) | prose | Support histogram plots |
| 4.0.0 | [SPARK-50063](https://issues.apache.org/jira/browse/SPARK-50063) | prose | Add support for Variant in the Spark Connect Scala client |
| 4.0.0 | [SPARK-50298](https://issues.apache.org/jira/browse/SPARK-50298) | prose | Implement verifySchema parameter of createDataFrame |
| 4.0.0 | [SPARK-50306](https://issues.apache.org/jira/browse/SPARK-50306) | prose | Support Python 3.13 in Spark Connect |
| 4.0.0 | [SPARK-50544](https://issues.apache.org/jira/browse/SPARK-50544) | prose | Implement StructType.toDDL |
| 4.0.0 | [SPARK-50605](https://issues.apache.org/jira/browse/SPARK-50605) | prose | Add spark.api.mode for better compatibility with Spark Classic |
| 4.0.0 | [SPARK-50710](https://issues.apache.org/jira/browse/SPARK-50710) | prose | Add support for optional client reconnection to sessions after release |
| 4.0.0 | [SPARK-50812](https://issues.apache.org/jira/browse/SPARK-50812) | prose | Support pyspark.ml on Connect |
| 4.0.0 | [SPARK-50828](https://issues.apache.org/jira/browse/SPARK-50828) | prose | Deprecate pyspark.ml.connect |
| 4.0.1 | [SPARK-52397](https://issues.apache.org/jira/browse/SPARK-52397) | prose | Idempotent ExecutePlan: second ExecutePlan with same operationId and plan should reattach |
| 4.0.4 | [SPARK-58042](https://issues.apache.org/jira/browse/SPARK-58042) | Improvement | Validate the UDT jvm_class is a UserDefinedType before instantiating it in Spark Connect |
| 4.1.0 | [SPARK-51774](https://issues.apache.org/jira/browse/SPARK-51774) | prose | Add GRPC Status code to Python Connect GRPC Exception |
| 4.1.0 | [SPARK-51827](https://issues.apache.org/jira/browse/SPARK-51827) | prose | transformWithState |
| 4.1.0 | [SPARK-52397](https://issues.apache.org/jira/browse/SPARK-52397) | prose | Idempotent ExecutePlan: the second ExecutePlan with same operationId and plan reattaches |
| 4.1.0 | [SPARK-52448](https://issues.apache.org/jira/browse/SPARK-52448) | prose | Add simplified Struct Expression.Literal |
| 4.1.0 | [SPARK-52723](https://issues.apache.org/jira/browse/SPARK-52723) | prose | Server side column name validation |
| 4.1.0 | [SPARK-53455](https://issues.apache.org/jira/browse/SPARK-53455) | prose | Add CloneSession RPC |
| 4.1.0 | [SPARK-53484](https://issues.apache.org/jira/browse/SPARK-53484) | prose | SPIP: JDBC Driver for Spark Connect |
| 4.1.0 | [SPARK-53507](https://issues.apache.org/jira/browse/SPARK-53507) | prose | Add breaking change info to errors |
| 4.1.0 | [SPARK-53808](https://issues.apache.org/jira/browse/SPARK-53808) | prose | Allow to pass optional JVM args to spark-connect-scala-client |
| 4.1.0 | [SPARK-54357](https://issues.apache.org/jira/browse/SPARK-54357) | prose | Improve SparkConnect usability and performance |
| 4.1.3 | [SPARK-58042](https://issues.apache.org/jira/browse/SPARK-58042) | Improvement | Validate the UDT jvm_class is a UserDefinedType before instantiating it in Spark Connect |
| 4.2.0 | [SPARK-54314](https://issues.apache.org/jira/browse/SPARK-54314) | prose | Optionally transmit client-side code locations with actions for server-side logging and telemetry |
| 4.2.0 | [SPARK-55047](https://issues.apache.org/jira/browse/SPARK-55047) | prose | Add client-side limit for local relation size |
| 4.2.0 | [SPARK-55090](https://issues.apache.org/jira/browse/SPARK-55090) | prose | Implement DataFrame.toJSON in the Python client |
| 4.2.0 | [SPARK-55227](https://issues.apache.org/jira/browse/SPARK-55227) | prose | RDD API compatibility |
| 4.2.0 | [SPARK-55228](https://issues.apache.org/jira/browse/SPARK-55228) | prose | Implement Dataset.zipWithIndex in the Scala API |
| 4.2.0 | [SPARK-55229](https://issues.apache.org/jira/browse/SPARK-55229) | prose | Implement DataFrame.zipWithIndex in PySpark |
| 4.2.0 | [SPARK-55326](https://issues.apache.org/jira/browse/SPARK-55326) | prose | Release the remote session on process exit when SPARK_CONNECT_RELEASE_SESSION_ON_EXIT is set |
| 4.2.0 | [SPARK-55606](https://issues.apache.org/jira/browse/SPARK-55606) | prose | Server-side implementation of the GetStatus API for execution status monitoring |
| 4.2.0 | [SPARK-55691](https://issues.apache.org/jira/browse/SPARK-55691) | prose | Add GetStatus client support |
| 4.2.0 | [SPARK-55887](https://issues.apache.org/jira/browse/SPARK-55887) | prose | Optimize head()/take()/tail() to avoid full table scans |
| 4.2.0 | [SPARK-56007](https://issues.apache.org/jira/browse/SPARK-56007) | prose | Fix ArrowDeserializer to use positional binding so the Scala client handles duplicate column names correctly |
| 4.2.0 | [SPARK-56253](https://issues.apache.org/jira/browse/SPARK-56253) | prose | Make spark.read.json accept DataFrame input |
| 4.2.0 | [SPARK-56254](https://issues.apache.org/jira/browse/SPARK-56254) | prose | Make spark.read.xml accept DataFrame input |
| 4.2.0 | [SPARK-56255](https://issues.apache.org/jira/browse/SPARK-56255) | prose | Make spark.read.csv accept DataFrame input |
| 4.2.0 | [SPARK-56256](https://issues.apache.org/jira/browse/SPARK-56256) | prose | Add emptyDataFrame API to SparkSession |
| 4.2.0 | [SPARK-57601](https://issues.apache.org/jira/browse/SPARK-57601) | prose | Make the Spark Connect tab available in the History Server |
<!-- AUTO:timeline END -->
