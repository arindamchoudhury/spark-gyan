# Arrow

> Source: `release-notes/spark_all_changelogs.txt`. Timeline rows below are generated from `_catalog.jsonl`; prose outside the AUTO markers is hand-written.

## How it evolved

### 2.x era — Arrow lands as the PySpark/pandas bridge

Arrow first appears in the changelog at 2.3.0, bundled at version 0.8.0 alongside a Netty upgrade to 4.1.17 — the same release that introduced scalar Pandas UDFs and faster `toPandas()`/`createDataFrame()` conversion elsewhere in PySpark. 2.4.0 bumped the bundled Arrow to 0.10.0 (SPARK-23874) and switched Pandas DataFrame conversion to the Arrow stream format for both creating Spark DataFrames from pandas and collecting them back (SPARK-23030), replacing a row-at-a-time Python serialization loop with Arrow's columnar wire format. These two releases mark Arrow's shift from an internal dependency bump to a user-facing performance feature that later PySpark and pandas-on-Spark work builds directly on top of.

### 3.x era — from version bumps to Arrow-optimized UDFs

3.0.0's Arrow work was almost entirely version-upgrade maintenance — bumping the minimum PyArrow version (SPARK-27276), moving SparkR to the Arrow 0.15 API (SPARK-29378), and fixing `toPandas`/`createDataFrame` edge cases around `NaT` and exceptions. 3.1.1 upgraded Arrow to 2.0.0 (SPARK-33213) and added `MapType` support for PySpark-with-Arrow (SPARK-24554). 3.2.0 trimmed `toPandas` memory usage via Arrow's `self_destruct` option (SPARK-32953). The real shift came in 3.5.0, which introduced Arrow-optimized Python UDFs (SPARK-40307) — moving scalar UDF serialization off pickle and onto Arrow — plus large-variable-width-vector support, barrier-mode `mapInPandas`/`mapInArrow` (SPARK-42896), and Arrow-optimized Python UDTFs (SPARK-43964), turning Arrow from a pandas-conversion detail into a first-class UDF execution path.

### 4.x era — Arrow becomes the default IPC format

4.1.0 built out Arrow-optimized Python UDFs and UDTFs (SPARK-52214, SPARK-52979), vectorized `@udf` support (SPARK-53592), an iterator-of-`RecordBatch` API for `applyInArrow` (SPARK-49547), UDT input/output support for Arrow UDFs and UDTFs (SPARK-51619, SPARK-52959), Arrow compression for Pandas UDFs (SPARK-54226), batch-size limits for `applyInArrow`/`applyInPandas` (SPARK-53562), and Arrow memory and serializer performance work (SPARK-54134, SPARK-52877).

4.2.0 is the milestone release: Arrow-optimized Python UDFs and Arrow-based PySpark IPC became the default (SPARK-54555), turning a decade of incremental Arrow adoption into the standard Python execution path. The same release introduced an iterator API for Arrow and pandas grouped-aggregation UDFs (SPARK-53615, SPARK-53616), registered both for SQL usage (SPARK-54617, SPARK-54722), and added `ExtensionDtype` support for integers in Pandas UDFs (SPARK-55788) — extending Arrow from a scalar-UDF optimization into grouped aggregation as well.

## Timeline

<!-- AUTO:timeline START -->
| Release | JIRA | Type | Title |
|---|---|---|---|
| 2.3.0 | — | prose | Arrow upgraded to 0.8.0 and Netty to 4.1.17 |
| 2.4.0 | [SPARK-23030](https://issues.apache.org/jira/browse/SPARK-23030) | prose | Arrow stream format used for creating from/collecting Pandas DataFrames |
| 2.4.0 | [SPARK-23874](https://issues.apache.org/jira/browse/SPARK-23874) | prose | Arrow upgraded to 0.10.0 in PySpark |
| 3.0.0 | [SPARK-25811](https://issues.apache.org/jira/browse/SPARK-25811) | Improvement | Support PyArrow's feature to raise an error for unsafe cast |
| 3.0.0 | [SPARK-26759](https://issues.apache.org/jira/browse/SPARK-26759) | Umbrella | Arrow optimization in SparkR's interoperability |
| 3.0.0 | [SPARK-27276](https://issues.apache.org/jira/browse/SPARK-27276) | Improvement | Increase the minimum pyarrow version to 0.12.1 |
| 3.0.0 | [SPARK-27805](https://issues.apache.org/jira/browse/SPARK-27805) | Improvement | toPandas does not propagate SparkExceptions with arrow enabled |
| 3.0.0 | [SPARK-27971](https://issues.apache.org/jira/browse/SPARK-27971) | Improvement | MapPartitionsInRWithArrowExec.evaluate shouldn't eagerly read the first batch |
| 3.0.0 | [SPARK-27995](https://issues.apache.org/jira/browse/SPARK-27995) | Improvement | Note the difference between str of Python 2 and 3 at Arrow optimized |
| 3.0.0 | [SPARK-28003](https://issues.apache.org/jira/browse/SPARK-28003) | Improvement | spark.createDataFrame with Arrow doesn't work with pandas.NaT |
| 3.0.0 | [SPARK-29339](https://issues.apache.org/jira/browse/SPARK-29339) | Improvement | Support Arrow 0.14 in vectoried dapply and gapply (test it in AppVeyor build) |
| 3.0.0 | [SPARK-29376](https://issues.apache.org/jira/browse/SPARK-29376) | Improvement | Upgrade Apache Arrow to 0.15.1 |
| 3.0.0 | [SPARK-29378](https://issues.apache.org/jira/browse/SPARK-29378) | Improvement | Upgrade SparkR to use Arrow 0.15 API |
| 3.0.0 | [SPARK-30640](https://issues.apache.org/jira/browse/SPARK-30640) | Improvement | Prevent unnessary copies of data in Arrow to Pandas conversion with Timestamps |
| 3.0.0 | [SPARK-31701](https://issues.apache.org/jira/browse/SPARK-31701) | Improvement | Bump up the minimum Arrow version as 0.15.1 in SparkR |
| 3.1.1 | [SPARK-24554](https://issues.apache.org/jira/browse/SPARK-24554) | prose | Add MapType support for PySpark with Arrow |
| 3.1.1 | [SPARK-33213](https://issues.apache.org/jira/browse/SPARK-33213) | prose | Upgrade Apache Arrow to 2.0.0 |
| 3.2.0 | [SPARK-32953](https://issues.apache.org/jira/browse/SPARK-32953) | Improvement | Lower memory usage in toPandas with Arrow self_destruct |
| 3.2.0 | [SPARK-33489](https://issues.apache.org/jira/browse/SPARK-33489) | Improvement | Support null for conversion from and to Arrow type |
| 3.5.0 | [SPARK-39979](https://issues.apache.org/jira/browse/SPARK-39979) | prose | Add option to use large variable width vectors for arrow UDF operations |
| 3.5.0 | [SPARK-40307](https://issues.apache.org/jira/browse/SPARK-40307) | prose | Introduce Arrow Python UDFs |
| 3.5.0 | [SPARK-41971](https://issues.apache.org/jira/browse/SPARK-41971) | prose | Use deduplicated field names when creating Arrow RecordBatch |
| 3.5.0 | [SPARK-42896](https://issues.apache.org/jira/browse/SPARK-42896) | prose | Make mapInPandas / mapInArrow support barrier mode execution |
| 3.5.0 | [SPARK-43964](https://issues.apache.org/jira/browse/SPARK-43964) | prose | Support arrow-optimized Python UDTFs |
| 4.1.0 | [SPARK-49547](https://issues.apache.org/jira/browse/SPARK-49547) | prose | Add iterator of RecordBatch API to applyInArrow |
| 4.1.0 | [SPARK-51619](https://issues.apache.org/jira/browse/SPARK-51619) | prose | Support UDT input / output in Arrow-optimized Python UDF |
| 4.1.0 | [SPARK-52214](https://issues.apache.org/jira/browse/SPARK-52214) | prose | Python Arrow UDF |
| 4.1.0 | [SPARK-52821](https://issues.apache.org/jira/browse/SPARK-52821) | prose | Add intâDecimalType pyspark udf return type coercion |
| 4.1.0 | [SPARK-52877](https://issues.apache.org/jira/browse/SPARK-52877) | prose | Improve Python UDF Arrow Serializer Performance |
| 4.1.0 | [SPARK-52904](https://issues.apache.org/jira/browse/SPARK-52904) | prose | Enable convertToArrowArraySafely by default |
| 4.1.0 | [SPARK-52934](https://issues.apache.org/jira/browse/SPARK-52934) | prose | Allow yielding scalar values with Arrow-optimized Python UDTF |
| 4.1.0 | [SPARK-52959](https://issues.apache.org/jira/browse/SPARK-52959) | prose | Support UDT in Arrow-optimized Python UDTF |
| 4.1.0 | [SPARK-52979](https://issues.apache.org/jira/browse/SPARK-52979) | prose | Python Arrow UDTF |
| 4.1.0 | [SPARK-53562](https://issues.apache.org/jira/browse/SPARK-53562) | prose | Limit Arrow batch sizes in applyInArrow and applyInPandas |
| 4.1.0 | [SPARK-53592](https://issues.apache.org/jira/browse/SPARK-53592) | prose | Make @udf support vectorized UDF |
| 4.1.0 | [SPARK-53614](https://issues.apache.org/jira/browse/SPARK-53614) | prose | Add Iterator[pandas.DataFrame] support to applyInPandas |
| 4.1.0 | [SPARK-54134](https://issues.apache.org/jira/browse/SPARK-54134) | prose | Optimize Arrow memory usage |
| 4.1.0 | [SPARK-54226](https://issues.apache.org/jira/browse/SPARK-54226) | prose | Extend Arrow compression to Pandas UDF |
| 4.1.2 | [SPARK-56344](https://issues.apache.org/jira/browse/SPARK-56344) | Improvement | Update outdated PyArrow minimum version in arrow_pandas.rst documentation (branch-4.1) |
| 4.2.0 | [SPARK-53615](https://issues.apache.org/jira/browse/SPARK-53615) | prose | Introduce iterator API for Arrow grouped aggregation UDF |
| 4.2.0 | [SPARK-53616](https://issues.apache.org/jira/browse/SPARK-53616) | prose | Introduce iterator API for pandas grouped aggregation UDF |
| 4.2.0 | [SPARK-54555](https://issues.apache.org/jira/browse/SPARK-54555) | prose | Enable Arrow-optimized Python UDFs and Arrow-based PySpark IPC by default |
| 4.2.0 | [SPARK-54617](https://issues.apache.org/jira/browse/SPARK-54617) | prose | Enable Arrow grouped iterator aggregate UDF registration for SQL |
| 4.2.0 | [SPARK-54722](https://issues.apache.org/jira/browse/SPARK-54722) | prose | Register pandas grouped iterator aggregate UDF for SQL usage |
| 4.2.0 | [SPARK-55788](https://issues.apache.org/jira/browse/SPARK-55788) | prose | Support ExtensionDType for integers in Pandas UDF |
<!-- AUTO:timeline END -->
