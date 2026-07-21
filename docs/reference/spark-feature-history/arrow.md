# Arrow

> Source: `release-notes/spark_all_changelogs.txt`. Timeline rows below are generated from `_catalog.jsonl`; prose outside the AUTO markers is hand-written.

## How it evolved

### 2.x era — Arrow lands as the PySpark/pandas bridge

Arrow first appears in the changelog at 2.3.0, bundled at version 0.8.0 alongside a Netty upgrade to 4.1.17 — the same release that introduced scalar Pandas UDFs and faster `toPandas()`/`createDataFrame()` conversion elsewhere in PySpark. 2.4.0 bumped the bundled Arrow to 0.10.0 (SPARK-23874) and switched Pandas DataFrame conversion to the Arrow stream format for both creating Spark DataFrames from pandas and collecting them back (SPARK-23030), replacing a row-at-a-time Python serialization loop with Arrow's columnar wire format. These two releases mark Arrow's shift from an internal dependency bump to a user-facing performance feature that later PySpark and pandas-on-Spark work builds directly on top of.

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
| 3.2.0 | [SPARK-32953](https://issues.apache.org/jira/browse/SPARK-32953) | Improvement | Lower memory usage in toPandas with Arrow self_destruct |
| 3.2.0 | [SPARK-33489](https://issues.apache.org/jira/browse/SPARK-33489) | Improvement | Support null for conversion from and to Arrow type |
| 4.1.2 | [SPARK-56344](https://issues.apache.org/jira/browse/SPARK-56344) | Improvement | Update outdated PyArrow minimum version in arrow_pandas.rst documentation (branch-4.1) |
<!-- AUTO:timeline END -->
