# pandas API on Spark

> Source: `release-notes/spark_all_changelogs.txt`. Timeline rows below are generated from `_catalog.jsonl`; prose outside the AUTO markers is hand-written.

## How it evolved

### 2.x era — grouped and windowed Pandas UDFs

This area's 2.x footprint is small but concrete, and lands entirely in 2.4.0: user-defined aggregation functions built on Pandas UDFs (SPARK-22274), user-defined window functions using the same mechanism (SPARK-22239), and support for mixing plain Python UDFs with Scalar Pandas UDFs in the same query (SPARK-24624). Together they extend the vectorized, Arrow-backed UDF model — introduced for scalar functions via the broader PySpark performance work in 2.3.0 — into `GROUP BY` aggregation and windowed computation, the two query shapes where row-at-a-time Python UDFs had been slowest. The one earlier record against this area, 2.0.0's `UnsafeRow` support in `MapPartitions`/`MapGroups`/`CoGroup` (SPARK-12287), is unrelated internal plumbing rather than pandas-specific work.

### 3.x era — Koalas becomes pandas API on Spark

pandas API on Spark did not exist as such before 3.2.0: the area's earlier 3.0.0/3.1.1 entries are Pandas-UDF plumbing, not the pandas-compatible API itself. 3.2.0 merged the external Koalas project directly into PySpark as `pyspark.pandas` (SPARK-34849) — giving users a pandas-shaped DataFrame/Series API backed by Spark, complete with mypy type-checking (SPARK-34941) and an internal `InternalField` abstraction for managing dtypes. 3.3.0 added a profiler for Python/pandas UDFs (SPARK-37443), `TimedeltaIndex` support, and dozens of coverage gaps closed (`DataFrame.describe`, `Index.map`, catalog introspection methods). 3.4.0 kept expanding coverage aggressively — `resample`, `interpolate`, `ewm`, a dozen new `GroupBy` methods — while also improving PySpark's own error messages (SPARK-41597). 3.5.0 rounded out `createDataFrame` interop with pandas: struct types, duplicate field names, and generic tuple type hints for Pandas UDFs.

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
| 3.2.0 | [SPARK-34849](https://issues.apache.org/jira/browse/SPARK-34849) | prose | Support Pandas API layer on PySpark |
| 3.2.0 | [SPARK-34941](https://issues.apache.org/jira/browse/SPARK-34941) | prose | Enable mypy for pandas-on-Spark |
| 3.2.0 | [SPARK-35464](https://issues.apache.org/jira/browse/SPARK-35464) | Umbrella | pandas API on Spark: Enable mypy check "disallow_untyped_defs" for main codes. |
| 3.2.0 | [SPARK-35638](https://issues.apache.org/jira/browse/SPARK-35638) | Improvement | Introduce InternalField to manage dtypes and StructFields. |
| 3.2.0 | [SPARK-35976](https://issues.apache.org/jira/browse/SPARK-35976) | Story | Adjust `astype` method for ExtensionDtype in pandas API on Spark |
| 3.2.0 | [SPARK-36003](https://issues.apache.org/jira/browse/SPARK-36003) | Improvement | Implement unary operator `invert` of integral ps.Series/Index |
| 3.2.0 | [SPARK-36031](https://issues.apache.org/jira/browse/SPARK-36031) | prose | Match behaviours on Series with NaN to pandas â |
| 3.2.0 | [SPARK-36103](https://issues.apache.org/jira/browse/SPARK-36103) | Improvement | Manage InternalField in DataTypeOps.invert |
| 3.2.0 | [SPARK-36104](https://issues.apache.org/jira/browse/SPARK-36104) | Improvement | Manage InternalField in DataTypeOps.neg/abs |
| 3.2.0 | [SPARK-36167](https://issues.apache.org/jira/browse/SPARK-36167) | Improvement | Revisit more InternalField managements. |
| 3.2.0 | [SPARK-36185](https://issues.apache.org/jira/browse/SPARK-36185) | Umbrella | Implement functions in CategoricalAccessor/CategoricalIndex |
| 3.2.0 | [SPARK-36192](https://issues.apache.org/jira/browse/SPARK-36192) | Improvement | Better error messages for DataTypeOps against list |
| 3.2.0 | [SPARK-36350](https://issues.apache.org/jira/browse/SPARK-36350) | Improvement | Make nanvl work with DataTypeOps |
| 3.2.0 | [SPARK-36559](https://issues.apache.org/jira/browse/SPARK-36559) | Improvement | Allow column pruning on distributed sequence index (pandas API on Spark) |
| 3.3.0 | [SPARK-18621](https://issues.apache.org/jira/browse/SPARK-18621) | prose | Make sql type reprs eval-able |
| 3.3.0 | [SPARK-32079](https://issues.apache.org/jira/browse/SPARK-32079) | prose | Remove namedtuple hack by replacing built-in pickle to cloudpickle |
| 3.3.0 | [SPARK-35173](https://issues.apache.org/jira/browse/SPARK-35173) | prose | Add multiple columns adding support |
| 3.3.0 | [SPARK-35929](https://issues.apache.org/jira/browse/SPARK-35929) | prose | Support to infer nested dict as a struct when creating a DataFrame |
| 3.3.0 | [SPARK-36176](https://issues.apache.org/jira/browse/SPARK-36176) | prose | Expose tableExists in pyspark.sql.catalog |
| 3.3.0 | [SPARK-36207](https://issues.apache.org/jira/browse/SPARK-36207) | prose | Expose databaseExists in pyspark.sql.catalog |
| 3.3.0 | [SPARK-36258](https://issues.apache.org/jira/browse/SPARK-36258) | prose | Exposing functionExists in pyspark sql catalog |
| 3.3.0 | [SPARK-36263](https://issues.apache.org/jira/browse/SPARK-36263) | prose | Add Dataframe.observation to PySpark |
| 3.3.0 | [SPARK-36396](https://issues.apache.org/jira/browse/SPARK-36396) | prose | Implement DataFrame.cov |
| 3.3.0 | [SPARK-36469](https://issues.apache.org/jira/browse/SPARK-36469) | prose | Implement Index.map |
| 3.3.0 | [SPARK-36709](https://issues.apache.org/jira/browse/SPARK-36709) | prose | Support to specify index type and name in pandas API on Spark |
| 3.3.0 | [SPARK-36751](https://issues.apache.org/jira/browse/SPARK-36751) | prose | Add bit/octet_length APIs to Scala, Python and R |
| 3.3.0 | [SPARK-36882](https://issues.apache.org/jira/browse/SPARK-36882) | prose | Support ILIKE API on Python |
| 3.3.0 | [SPARK-36930](https://issues.apache.org/jira/browse/SPARK-36930) | prose | Support ps.MultiIndex.dtypes |
| 3.3.0 | [SPARK-36953](https://issues.apache.org/jira/browse/SPARK-36953) | prose | Expose SQL state and error class in PySpark exceptions |
| 3.3.0 | [SPARK-36972](https://issues.apache.org/jira/browse/SPARK-36972) | prose | Add max_by/min_by API to PySpark |
| 3.3.0 | [SPARK-37207](https://issues.apache.org/jira/browse/SPARK-37207) | prose | Add isEmpty method for the Python DataFrame API |
| 3.3.0 | [SPARK-37228](https://issues.apache.org/jira/browse/SPARK-37228) | prose | Implement DataFrame.mapInArrow in Python |
| 3.3.0 | [SPARK-37275](https://issues.apache.org/jira/browse/SPARK-37275) | prose | Support Pythonâs timedelta |
| 3.3.0 | [SPARK-37396](https://issues.apache.org/jira/browse/SPARK-37396) | prose | Inline type hints for fpm.py in python/pyspark/mllib |
| 3.3.0 | [SPARK-37436](https://issues.apache.org/jira/browse/SPARK-37436) | prose | Uses Python’s standard string formatter for SQL API in pandas API on Spark |
| 3.3.0 | [SPARK-37443](https://issues.apache.org/jira/browse/SPARK-37443) | prose | Provide a profiler for Python/Pandas UDFs |
| 3.3.0 | [SPARK-37465](https://issues.apache.org/jira/browse/SPARK-37465) | prose | Bump minimum pandas version to 1.0.5 |
| 3.3.0 | [SPARK-37510](https://issues.apache.org/jira/browse/SPARK-37510) | prose | Support basic operations of timedelta Series/Index |
| 3.3.0 | [SPARK-37516](https://issues.apache.org/jira/browse/SPARK-37516) | prose | Uses Python’s standard string formatter for SQL API in PySpark |
| 3.3.0 | [SPARK-37525](https://issues.apache.org/jira/browse/SPARK-37525) | prose | Support TimedeltaIndex in pandas API on Spark |
| 3.3.0 | [SPARK-37657](https://issues.apache.org/jira/browse/SPARK-37657) | prose | DataFrame).describe() |
| 3.3.0 | [SPARK-38278](https://issues.apache.org/jira/browse/SPARK-38278) | prose | Add SparkContext.addArchive in PySpark |
| 3.3.0 | [SPARK-38654](https://issues.apache.org/jira/browse/SPARK-38654) | prose | Show default index type in SQL plans for pandas API on Spark |
| 3.4.0 | [SPARK-38774](https://issues.apache.org/jira/browse/SPARK-38774) | prose | Implement Series.autocorr |
| 3.4.0 | [SPARK-38785](https://issues.apache.org/jira/browse/SPARK-38785) | prose | Implement DataFrame.ewm and Series.ewm |
| 3.4.0 | [SPARK-38844](https://issues.apache.org/jira/browse/SPARK-38844) | prose | Implement DataFrame.interpolate and Series.interpolate |
| 3.4.0 | [SPARK-38907](https://issues.apache.org/jira/browse/SPARK-38907) | prose | Implement DataFrame.corrwith |
| 3.4.0 | [SPARK-38947](https://issues.apache.org/jira/browse/SPARK-38947) | prose | Support GroupBy positional indexing |
| 3.4.0 | [SPARK-38993](https://issues.apache.org/jira/browse/SPARK-38993) | prose | Implement DataFrame.boxplot and DataFrame.plot.box |
| 3.4.0 | [SPARK-39081](https://issues.apache.org/jira/browse/SPARK-39081) | prose | Implement DataFrame.resample and Series.resample |
| 3.4.0 | [SPARK-39129](https://issues.apache.org/jira/browse/SPARK-39129) | prose | Implement GroupBy.ewm |
| 3.4.0 | [SPARK-39246](https://issues.apache.org/jira/browse/SPARK-39246) | prose | Implement GroupBy.skew |
| 3.4.0 | [SPARK-39284](https://issues.apache.org/jira/browse/SPARK-39284) | prose | Implement GroupBy.mad |
| 3.4.0 | [SPARK-39760](https://issues.apache.org/jira/browse/SPARK-39760) | prose | Support Varchar in PySpark |
| 3.4.0 | [SPARK-39809](https://issues.apache.org/jira/browse/SPARK-39809) | prose | Support CharType in PySpark |
| 3.4.0 | [SPARK-39877](https://issues.apache.org/jira/browse/SPARK-39877) | prose | Implement âunpivot/meltâ function |
| 3.4.0 | [SPARK-40003](https://issues.apache.org/jira/browse/SPARK-40003) | prose | Implement âmedianâ function |
| 3.4.0 | [SPARK-40007](https://issues.apache.org/jira/browse/SPARK-40007) | prose | Implement âmodeâ function |
| 3.4.0 | [SPARK-40138](https://issues.apache.org/jira/browse/SPARK-40138) | prose | Implement DataFrame.mode |
| 3.4.0 | [SPARK-40305](https://issues.apache.org/jira/browse/SPARK-40305) | prose | Implement GroupBy.sem |
| 3.4.0 | [SPARK-40330](https://issues.apache.org/jira/browse/SPARK-40330) | prose | Implement Series.searchsorted |
| 3.4.0 | [SPARK-40332](https://issues.apache.org/jira/browse/SPARK-40332) | prose | Implement GroupBy.quantile |
| 3.4.0 | [SPARK-40333](https://issues.apache.org/jira/browse/SPARK-40333) | prose | Implement GroupBy.nth |
| 3.4.0 | [SPARK-40334](https://issues.apache.org/jira/browse/SPARK-40334) | prose | Implement GroupBy.prod |
| 3.4.0 | [SPARK-40576](https://issues.apache.org/jira/browse/SPARK-40576) | prose | Pandas 1.5 support |
| 3.4.0 | [SPARK-41597](https://issues.apache.org/jira/browse/SPARK-41597) | prose | PySpark error improvements |
| 3.4.0 | [SPARK-41666](https://issues.apache.org/jira/browse/SPARK-41666) | prose | Support parameterized SQL in PySpark |
| 3.4.0 | [SPARK-42882](https://issues.apache.org/jira/browse/SPARK-42882) | prose | Pandas API coverage improvements |
| 3.4.0 | [SPARK-42883](https://issues.apache.org/jira/browse/SPARK-42883) | prose | Implement pandas API missing parameters |
| 3.5.0 | [SPARK-43473](https://issues.apache.org/jira/browse/SPARK-43473) | prose | Support struct type in createDataFrame from pandas DataFrame |
| 3.5.0 | [SPARK-43528](https://issues.apache.org/jira/browse/SPARK-43528) | prose | Support duplicated field names in createDataFrame with pandas DataFrame |
| 3.5.0 | [SPARK-43574](https://issues.apache.org/jira/browse/SPARK-43574) | prose | Support to set Python executable for UDF and pandas function APIs in workers during runtime |
| 3.5.0 | [SPARK-43817](https://issues.apache.org/jira/browse/SPARK-43817) | prose | Support UserDefinedType in createDataFrame from pandas DataFrame and toPandas |
| 3.5.0 | [SPARK-43886](https://issues.apache.org/jira/browse/SPARK-43886) | prose | Accept generics tuple as typing hints of Pandas UDF |
<!-- AUTO:timeline END -->
