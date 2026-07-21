# pandas API on Spark

> Source: `release-notes/spark_all_changelogs.txt`. Timeline rows below are generated from `_catalog.jsonl`; prose outside the AUTO markers is hand-written.

## How it evolved

### 2.x era — grouped and windowed Pandas UDFs

This area's 2.x footprint is small but concrete, and lands entirely in 2.4.0: user-defined aggregation functions built on Pandas UDFs (SPARK-22274), user-defined window functions using the same mechanism (SPARK-22239), and support for mixing plain Python UDFs with Scalar Pandas UDFs in the same query (SPARK-24624). Together they extend the vectorized, Arrow-backed UDF model — introduced for scalar functions via the broader PySpark performance work in 2.3.0 — into `GROUP BY` aggregation and windowed computation, the two query shapes where row-at-a-time Python UDFs had been slowest. The one earlier record against this area, 2.0.0's `UnsafeRow` support in `MapPartitions`/`MapGroups`/`CoGroup` (SPARK-12287), is unrelated internal plumbing rather than pandas-specific work.

## Timeline

<!-- AUTO:timeline START -->
| Release | JIRA | Type | Title |
|---|---|---|---|
| 2.0.0 | [SPARK-12287](https://issues.apache.org/jira/browse/SPARK-12287) | Improvement | Support UnsafeRow in MapPartitions/MapGroups/CoGroup |
| 2.4.0 | [SPARK-22239](https://issues.apache.org/jira/browse/SPARK-22239) | prose | User-defined window functions with Pandas UDF |
| 2.4.0 | [SPARK-22274](https://issues.apache.org/jira/browse/SPARK-22274) | prose | User-defined aggregation functions with Pandas UDF |
| 2.4.0 | [SPARK-24624](https://issues.apache.org/jira/browse/SPARK-24624) | prose | Support mixture of Python UDF and Scalar Pandas UDF |
| 3.0.0 | [SPARK-26364](https://issues.apache.org/jira/browse/SPARK-26364) | Improvement | Clean up import statements in pandas udf tests |
| 3.0.0 | [SPARK-26412](https://issues.apache.org/jira/browse/SPARK-26412) | New Feature | Allow Pandas UDF to take an iterator of pd.DataFrames |
| 3.0.0 | [SPARK-27163](https://issues.apache.org/jira/browse/SPARK-27163) | Improvement | Cleanup and consolidate Pandas UDF functionality |
| 3.0.0 | [SPARK-27240](https://issues.apache.org/jira/browse/SPARK-27240) | Improvement | Use pandas DataFrame for struct type argument in Scalar Pandas UDF. |
| 3.0.0 | [SPARK-27463](https://issues.apache.org/jira/browse/SPARK-27463) | Improvement | Support Dataframe Cogroup via Pandas UDFs |
| 3.0.0 | [SPARK-27870](https://issues.apache.org/jira/browse/SPARK-27870) | Improvement | Flush each batch for pandas UDF (for improving pandas UDFs pipeline) |
| 3.0.0 | [SPARK-28128](https://issues.apache.org/jira/browse/SPARK-28128) | Improvement | Pandas Grouped UDFs should skip over empty partitions |
| 3.0.0 | [SPARK-28226](https://issues.apache.org/jira/browse/SPARK-28226) | New Feature | Document Pandas UDF mapParitionsInPandas |
| 3.0.0 | [SPARK-28264](https://issues.apache.org/jira/browse/SPARK-28264) | Improvement | Revisiting Python / pandas UDF |
| 3.0.0 | [SPARK-29317](https://issues.apache.org/jira/browse/SPARK-29317) | Improvement | Avoid inheritance hierarchy in pandas CoGroup arrow runner and its plan |
| 3.0.0 | [SPARK-29402](https://issues.apache.org/jira/browse/SPARK-29402) | Improvement | Add tests for grouped map pandas_udf using window |
| 3.1.1 | [SPARK-34588](https://issues.apache.org/jira/browse/SPARK-34588) | Improvement | Support int64 buffer lengths in Java for pyspark Pandas UDF as buffer expanding |
| 3.2.0 | [SPARK-35464](https://issues.apache.org/jira/browse/SPARK-35464) | Umbrella | pandas API on Spark: Enable mypy check "disallow_untyped_defs" for main codes. |
| 3.2.0 | [SPARK-35638](https://issues.apache.org/jira/browse/SPARK-35638) | Improvement | Introduce InternalField to manage dtypes and StructFields. |
| 3.2.0 | [SPARK-35976](https://issues.apache.org/jira/browse/SPARK-35976) | Story | Adjust `astype` method for ExtensionDtype in pandas API on Spark |
| 3.2.0 | [SPARK-36003](https://issues.apache.org/jira/browse/SPARK-36003) | Improvement | Implement unary operator `invert` of integral ps.Series/Index |
| 3.2.0 | [SPARK-36103](https://issues.apache.org/jira/browse/SPARK-36103) | Improvement | Manage InternalField in DataTypeOps.invert |
| 3.2.0 | [SPARK-36104](https://issues.apache.org/jira/browse/SPARK-36104) | Improvement | Manage InternalField in DataTypeOps.neg/abs |
| 3.2.0 | [SPARK-36167](https://issues.apache.org/jira/browse/SPARK-36167) | Improvement | Revisit more InternalField managements. |
| 3.2.0 | [SPARK-36185](https://issues.apache.org/jira/browse/SPARK-36185) | Umbrella | Implement functions in CategoricalAccessor/CategoricalIndex |
| 3.2.0 | [SPARK-36192](https://issues.apache.org/jira/browse/SPARK-36192) | Improvement | Better error messages for DataTypeOps against list |
| 3.2.0 | [SPARK-36350](https://issues.apache.org/jira/browse/SPARK-36350) | Improvement | Make nanvl work with DataTypeOps |
| 3.2.0 | [SPARK-36559](https://issues.apache.org/jira/browse/SPARK-36559) | Improvement | Allow column pruning on distributed sequence index (pandas API on Spark) |
<!-- AUTO:timeline END -->
