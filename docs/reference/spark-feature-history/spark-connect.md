# Spark Connect

> Source: `release-notes/spark_all_changelogs.txt`. Timeline rows below are generated from `_catalog.jsonl`; prose outside the AUTO markers is hand-written.

## How it evolved

### 3.x era — Spark Connect is born

Spark Connect did not exist before 3.4.0, which introduced it as a decoupled client/server architecture: a thin Python client (SPARK-39375) talking to a Spark server over gRPC, with DataFrame, Column, Functions, SparkSession, I/O, and Catalog APIs (SPARK-41279, SPARK-41282, SPARK-41283, SPARK-41281, SPARK-41284, SPARK-41289) plus Python UDF support (SPARK-41661) and the first cut of a Scala client (SPARK-41534). 3.5.0 broadened the client surface substantially: Scala and Go clients matured (SPARK-42554), a Go client shipped its initial version (SPARK-43351), and Structured Streaming (SPARK-42938) and the pandas API (SPARK-42497) both gained Connect support, alongside a `sql`/`sql-api` module split (SPARK-44273) to keep the client's dependency footprint minimal — moving Connect from a Python-only proof of concept toward a multi-language, feature-complete remote execution client in two releases.

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
| 4.0.4 | [SPARK-58042](https://issues.apache.org/jira/browse/SPARK-58042) | Improvement | Validate the UDT jvm_class is a UserDefinedType before instantiating it in Spark Connect |
| 4.1.3 | [SPARK-58042](https://issues.apache.org/jira/browse/SPARK-58042) | Improvement | Validate the UDT jvm_class is a UserDefinedType before instantiating it in Spark Connect |
<!-- AUTO:timeline END -->
