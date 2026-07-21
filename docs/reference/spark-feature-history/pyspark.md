# PySpark & Python UDFs

> Source: `release-notes/spark_all_changelogs.txt`. Timeline rows below are generated from `_catalog.jsonl`; prose outside the AUTO markers is hand-written.

## How it evolved

### 0.x era — origins

PySpark arrived in 0.7.0, bringing RDDs, accumulators, broadcast variables, and HDFS I/O to Python for both standalone programs and interactive shells. 0.7.3 sped up spawning of the Python worker VMs, especially for large JVM heap sizes. 0.8.0 extended the API with storage levels, sampling, and missing RDD operators, and added support for running PySpark under IPython (including the IPython Notebook) as well as on Windows.

0.8.1 rounded out the Python surface with the ability to set Spark config properties directly from Python, sort operations, and an explicitly named `add` method on accumulators. By 0.9.0 PySpark jobs showed their call sites in the Spark web UI, IPython integration was updated for newer versions, and 0.9.1 filled in more missing RDD operations (`top`, `zip`, `foldByKey`, `repartition`, `coalesce`, `getStorageLevel`, `setName`, `toDebugString`).

### 1.x era — YARN, Python 3, and closing the API gap

1.0.0 extended the Python API with several new functions, added support for running on YARN, and widened compatible Python/NumPy versions down to 2.6/1.4. 1.1.0 ported disk spilling during aggregations to PySpark and let it read and write arbitrary Hadoop InputFormats (SequenceFiles, HBase, Cassandra, Avro). 1.2.0 covered nearly all DStream transformations in Python and lifted the 2GB broadcast-variable ceiling; 1.3.0 brought the ML pipeline API, gradient-boosted trees, and GMM to Python. 1.4.0 added Python 3 support (SPARK-4897) — a major compatibility milestone — plus external spilling for `groupByKey` (SPARK-3074), and by 1.6.0 the release notes describe "many improvements to Python API to approach feature parity" with Scala.

## Timeline

<!-- AUTO:timeline START -->
| Release | JIRA | Type | Title |
|---|---|---|---|
| 0.7.0 | — | prose | Python API (PySpark) added |
| 0.7.0 | — | prose | PySpark supports RDDs, accumulators, broadcast variables, and HDFS I/O |
| 0.7.3 | — | prose | Faster Python VM spawning for large JVM heap sizes |
| 0.8.0 | — | prose | Python API extended with storage levels, sampling, and missing RDD operators |
| 0.8.0 | — | prose | PySpark support for IPython (incl. Notebook) and Windows |
| 0.8.0 | — | prose | Various optimizations to PySpark and the job scheduler |
| 0.8.1 | — | prose | Set Spark config properties directly from Python |
| 0.8.1 | — | prose | Python support for sort operations |
| 0.8.1 | — | prose | Python accumulators gain an explicitly named add method |
| 0.9.0 | — | prose | PySpark shows job call sites in the Spark application UI |
| 0.9.0 | — | prose | IPython integration updated for newer versions |
| 0.9.1 | — | prose | Missing RDD operations added to PySpark (top, zip, foldByKey, repartition, coalesce, getStorageLevel, setName, toDebugString) |
| 1.0.0 | — | prose | Python API extended with several new functions |
| 1.0.0 | — | prose | PySpark now supports running on YARN |
| 1.0.0 | — | prose | PySpark supports more Python/NumPy versions |
| 1.1.0 | — | prose | Disk spilling during aggregations ported to PySpark |
| 1.1.0 | — | prose | PySpark reads/writes arbitrary Hadoop InputFormats |
| 1.2.0 | — | prose | Spark Streaming Python API covering DStream transformations/output ops |
| 1.2.0 | — | prose | PySpark sort operator supports external spilling |
| 1.2.0 | — | prose | PySpark supports broadcast variables larger than 2GB |
| 1.2.1 | [SPARK-5223](https://issues.apache.org/jira/browse/SPARK-5223) | prose | Support Vector types within a dictionary |
| 1.3.0 | — | prose | PySpark gains ML pipeline API, gradient boosted trees, and GMM |
| 1.4.0 | [SPARK-3074](https://issues.apache.org/jira/browse/SPARK-3074) | prose | External spilling for Python groupByKey operations |
| 1.4.0 | [SPARK-4897](https://issues.apache.org/jira/browse/SPARK-4897) | prose | Python 3 support |
| 1.4.1 | [SPARK-8766](https://issues.apache.org/jira/browse/SPARK-8766) | prose | Support non ASCII characters in columns |
| 1.5.0 | [SPARK-4561](https://issues.apache.org/jira/browse/SPARK-4561) | Improvement | PySparkSQL's Row.asDict() should convert nested rows to dictionaries |
| 1.5.0 | [SPARK-5155](https://issues.apache.org/jira/browse/SPARK-5155) | New Feature | Python API for MQTT streaming |
| 1.5.0 | [SPARK-5161](https://issues.apache.org/jira/browse/SPARK-5161) | Improvement | Parallelize Python test execution |
| 1.5.0 | [SPARK-6591](https://issues.apache.org/jira/browse/SPARK-6591) | Improvement | Python data source load options should auto convert common types into strings |
| 1.5.0 | [SPARK-7639](https://issues.apache.org/jira/browse/SPARK-7639) | New Feature | Add Python API for Statistics.kernelDensity |
| 1.5.0 | [SPARK-8144](https://issues.apache.org/jira/browse/SPARK-8144) | Improvement | For PySpark SQL, automatically convert values provided in readwriter options to string |
| 1.5.0 | [SPARK-8511](https://issues.apache.org/jira/browse/SPARK-8511) | Improvement | Modify ML Python tests to remove saved models |
| 1.5.0 | [SPARK-8528](https://issues.apache.org/jira/browse/SPARK-8528) | Improvement | Add applicationId to SparkContext object in pyspark |
| 1.5.0 | [SPARK-8706](https://issues.apache.org/jira/browse/SPARK-8706) | New Feature | Implement Pylint / Prospector checks for PySpark |
| 1.5.0 | [SPARK-8727](https://issues.apache.org/jira/browse/SPARK-8727) | Improvement | Add missing python api |
| 1.5.0 | [SPARK-8996](https://issues.apache.org/jira/browse/SPARK-8996) | New Feature | Add Python API for Kolmogorov-Smirnov Test |
| 1.5.0 | [SPARK-9766](https://issues.apache.org/jira/browse/SPARK-9766) | Improvement | check and add missing docs for PySpark ML |
| 1.6.0 | — | prose | Many improvements to Python API to approach feature parity |
| 1.6.0 | [SPARK-3842](https://issues.apache.org/jira/browse/SPARK-3842) | Improvement | Remove the hacks for Python callback server in py4j |
| 1.6.0 | [SPARK-6328](https://issues.apache.org/jira/browse/SPARK-6328) | Improvement | Python API for StreamingListener |
| 1.6.0 | [SPARK-7021](https://issues.apache.org/jira/browse/SPARK-7021) | Improvement | JUnit output for Python tests |
| 1.6.0 | [SPARK-8472](https://issues.apache.org/jira/browse/SPARK-8472) | New Feature | Python API for DCT |
| 1.6.0 | [SPARK-9821](https://issues.apache.org/jira/browse/SPARK-9821) | Improvement | pyspark reduceByKey should allow a custom partitioner |
| 1.6.0 | [SPARK-9964](https://issues.apache.org/jira/browse/SPARK-9964) | New Feature | PySpark DataFrameReader accept RDD of String for JSON |
| 1.6.0 | [SPARK-10056](https://issues.apache.org/jira/browse/SPARK-10056) | Improvement | PySpark Row - Support for row["columnName"] syntax |
| 1.6.0 | [SPARK-10373](https://issues.apache.org/jira/browse/SPARK-10373) | Improvement | Move @since annotator to pyspark to be shared by all components |
| 1.6.0 | [SPARK-10415](https://issues.apache.org/jira/browse/SPARK-10415) | Improvement | Enhance Navigation Sidebar in PySpark API |
| 1.6.0 | [SPARK-10535](https://issues.apache.org/jira/browse/SPARK-10535) | New Feature | Support for recommendUsersForProducts and recommendProductsForUsers in matrix factorization model for PySpark |
| 1.6.0 | [SPARK-10577](https://issues.apache.org/jira/browse/SPARK-10577) | Improvement | [PySpark] DataFrame hint for broadcast join |
| 1.6.0 | [SPARK-10714](https://issues.apache.org/jira/browse/SPARK-10714) | Improvement | Refactor PythonRDD to decouple iterator computation from PythonRDD |
| 1.6.0 | [SPARK-10767](https://issues.apache.org/jira/browse/SPARK-10767) | Improvement | Make pyspark shared params codegen more consistent |
| 1.6.0 | [SPARK-11279](https://issues.apache.org/jira/browse/SPARK-11279) | Improvement | Add DataFrame#toDF in PySpark |
| 1.6.0 | [SPARK-11292](https://issues.apache.org/jira/browse/SPARK-11292) | New Feature | Python API for text data source |
| 1.6.0 | [SPARK-11467](https://issues.apache.org/jira/browse/SPARK-11467) | New Feature | Add Python API stddev/variance |
| 1.6.0 | [SPARK-11658](https://issues.apache.org/jira/browse/SPARK-11658) | Improvement | simplify documentation for PySpark combineByKey |
| 1.6.0 | [SPARK-11690](https://issues.apache.org/jira/browse/SPARK-11690) | Improvement | Add pivot to python api |
| 1.6.0 | [SPARK-11917](https://issues.apache.org/jira/browse/SPARK-11917) | Improvement | Add SQLContext#dropTempTable to PySpark |
| 1.6.0 | [SPARK-12115](https://issues.apache.org/jira/browse/SPARK-12115) | Improvement | Change numPartitions() in RDD to be "getNumPartitions" to be consistent with pyspark/scala |
| 1.6.3 | [SPARK-15761](https://issues.apache.org/jira/browse/SPARK-15761) | Improvement | pyspark shell should load if PYSPARK_DRIVER_PYTHON is ipython an Python3 |
| 2.0.0 | [SPARK-10009](https://issues.apache.org/jira/browse/SPARK-10009) | Improvement | PySpark Param of Vector type can be set with Python array or numpy.array |
| 2.0.0 | [SPARK-10380](https://issues.apache.org/jira/browse/SPARK-10380) | Improvement | Confusing examples in pyspark SQL docs |
| 2.0.0 | [SPARK-11295](https://issues.apache.org/jira/browse/SPARK-11295) | Improvement | Add packages to JUnit output for Python tests |
| 2.0.0 | [SPARK-11904](https://issues.apache.org/jira/browse/SPARK-11904) | Improvement | pyspark reduceByKeyAndWindow with invFunc=None requires checkpointing |
| 2.0.0 | [SPARK-11939](https://issues.apache.org/jira/browse/SPARK-11939) | Umbrella | PySpark support model export/import for Pipeline API |
| 2.0.0 | [SPARK-12115](https://issues.apache.org/jira/browse/SPARK-12115) | Improvement | Change numPartitions() in RDD to be "getNumPartitions" to be consistent with pyspark/scala |
| 2.0.0 | [SPARK-12361](https://issues.apache.org/jira/browse/SPARK-12361) | Improvement | Should set PYSPARK_DRIVER_PYTHON before python test |
| 2.0.0 | [SPARK-12905](https://issues.apache.org/jira/browse/SPARK-12905) | Improvement | PCAModel return eigenvalues for PySpark |
| 2.0.0 | [SPARK-12962](https://issues.apache.org/jira/browse/SPARK-12962) | Improvement | PySpark support covar_samp and covar_pop |
| 2.0.0 | [SPARK-13068](https://issues.apache.org/jira/browse/SPARK-13068) | Improvement | Extend pyspark ml paramtype conversion |
| 2.0.0 | [SPARK-13625](https://issues.apache.org/jira/browse/SPARK-13625) | Improvement | PySpark-ML method to get list of params for an obj should not check property attr |
| 2.0.0 | [SPARK-13687](https://issues.apache.org/jira/browse/SPARK-13687) | Improvement | Cleanup pyspark temporary files |
| 2.0.0 | [SPARK-13807](https://issues.apache.org/jira/browse/SPARK-13807) | Improvement | De-duplicate `Python*Helper` instantiation code in PySpark streaming |
| 2.0.0 | [SPARK-14215](https://issues.apache.org/jira/browse/SPARK-14215) | Improvement | Support chained Python UDF |
| 2.0.0 | [SPARK-14267](https://issues.apache.org/jira/browse/SPARK-14267) | Improvement | Execute multiple Python UDFs in single batch |
| 2.0.0 | [SPARK-14433](https://issues.apache.org/jira/browse/SPARK-14433) | New Feature | PySpark ml GaussianMixture |
| 2.0.0 | [SPARK-14472](https://issues.apache.org/jira/browse/SPARK-14472) | Improvement | Cleanup PySpark-ML Java wrapper classes so that JavaWrapper will inherit from JavaCallable |
| 2.0.0 | [SPARK-14768](https://issues.apache.org/jira/browse/SPARK-14768) | Improvement | Remove expectedType arg for PySpark Param |
| 2.0.0 | [SPARK-15163](https://issues.apache.org/jira/browse/SPARK-15163) | Improvement | Mark experimental algorithms experimental in PySpark |
| 2.0.0 | [SPARK-15238](https://issues.apache.org/jira/browse/SPARK-15238) | Improvement | Clarify Python 3 support in docs |
| 2.0.0 | [SPARK-15464](https://issues.apache.org/jira/browse/SPARK-15464) | Improvement | Replace SQLContext and SparkContext with SparkSession using builder pattern in python testsuites |
| 2.0.0 | [SPARK-15741](https://issues.apache.org/jira/browse/SPARK-15741) | Improvement | PySpark Cleanup of _setDefault with seed=None |
| 2.0.0 | [SPARK-15761](https://issues.apache.org/jira/browse/SPARK-15761) | Improvement | pyspark shell should load if PYSPARK_DRIVER_PYTHON is ipython an Python3 |
| 2.0.0 | [SPARK-15788](https://issues.apache.org/jira/browse/SPARK-15788) | Improvement | PySpark IDFModel missing "idf" property |
| 2.0.1 | [SPARK-16772](https://issues.apache.org/jira/browse/SPARK-16772) | Improvement | Correct API doc references to PySpark classes + formatting fixes |
| 2.1.0 | [SPARK-11775](https://issues.apache.org/jira/browse/SPARK-11775) | New Feature | Allow PySpark to register Java UDF |
| 2.1.0 | [SPARK-16399](https://issues.apache.org/jira/browse/SPARK-16399) | Improvement | Set PYSPARK_PYTHON to point to "python" instead of "python2.7" |
| 2.1.0 | [SPARK-16536](https://issues.apache.org/jira/browse/SPARK-16536) | Improvement | Expose `sql` in PySpark shell |
| 2.1.0 | [SPARK-16546](https://issues.apache.org/jira/browse/SPARK-16546) | Improvement | Dataframe.drop supported multi-columns in spark api and should make python api also support it. |
| 2.1.0 | [SPARK-16772](https://issues.apache.org/jira/browse/SPARK-16772) | Improvement | Correct API doc references to PySpark classes + formatting fixes |
| 2.1.0 | [SPARK-16861](https://issues.apache.org/jira/browse/SPARK-16861) | Improvement | Refactor PySpark accumulator API to be on top of AccumulatorV2 API |
| 2.1.0 | [SPARK-17197](https://issues.apache.org/jira/browse/SPARK-17197) | Improvement | PySpark LiR/LoR supports tree aggregation level configurable |
| 2.1.0 | [SPARK-17437](https://issues.apache.org/jira/browse/SPARK-17437) | Improvement | uiWebUrl is not accessible to JavaSparkContext or pyspark.SparkContext |
| 2.1.0 | [SPARK-17585](https://issues.apache.org/jira/browse/SPARK-17585) | Improvement | PySpark SparkContext.addFile supports adding files recursively |
| 2.1.0 | [SPARK-17745](https://issues.apache.org/jira/browse/SPARK-17745) | Improvement | Update Python API for NB to support weighted instances |
| 2.1.0 | [SPARK-18361](https://issues.apache.org/jira/browse/SPARK-18361) | New Feature | Expose RDD localCheckpoint in PySpark |
| 2.2.0 | [SPARK-18080](https://issues.apache.org/jira/browse/SPARK-18080) | New Feature | Locality Sensitive Hashing (LSH) Python API |
| 2.2.0 | [SPARK-18267](https://issues.apache.org/jira/browse/SPARK-18267) | New Feature | Distribute PySpark via Python Package Index (pypi) |
| 2.2.0 | [SPARK-18541](https://issues.apache.org/jira/browse/SPARK-18541) | Improvement | Add pyspark.sql.Column.aliasWithMetadata to allow dynamic metadata management in pyspark SQL API |
| 2.2.0 | [SPARK-18576](https://issues.apache.org/jira/browse/SPARK-18576) | Improvement | Expose basic TaskContext info in PySpark |
| 2.2.0 | [SPARK-18766](https://issues.apache.org/jira/browse/SPARK-18766) | Improvement | Push Down Filter Through BatchEvalPython |
| 2.2.0 | [SPARK-19336](https://issues.apache.org/jira/browse/SPARK-19336) | New Feature | LinearSVC Python API |
| 2.2.0 | [SPARK-19467](https://issues.apache.org/jira/browse/SPARK-19467) | Improvement | PySpark ML shouldn't use circular imports |
| 2.2.0 | [SPARK-19706](https://issues.apache.org/jira/browse/SPARK-19706) | Improvement | add Column.contains in pyspark |
| 2.2.0 | [SPARK-19806](https://issues.apache.org/jira/browse/SPARK-19806) | Improvement | PySpark GLR supports tweedie distribution |
| 2.2.0 | [SPARK-19986](https://issues.apache.org/jira/browse/SPARK-19986) | Improvement | Make pyspark.streaming.tests.CheckpointTests more stable |
| 2.2.0 | [SPARK-20627](https://issues.apache.org/jira/browse/SPARK-20627) | Improvement | Remove pip local version string (PEP440) |
| 2.2.0 | [SPARK-22337](https://issues.apache.org/jira/browse/SPARK-22337) | Improvement | new pyspark release |
| 3.0.0 | [SPARK-19926](https://issues.apache.org/jira/browse/SPARK-19926) | Improvement | Make pyspark exception more readable |
| 3.0.0 | [SPARK-21094](https://issues.apache.org/jira/browse/SPARK-21094) | Improvement | Allow stdout/stderr pipes in pyspark.java_gateway.launch_gateway |
| 3.0.0 | [SPARK-25255](https://issues.apache.org/jira/browse/SPARK-25255) | Improvement | Add getActiveSession to SparkSession in PySpark |
| 3.0.0 | [SPARK-26349](https://issues.apache.org/jira/browse/SPARK-26349) | Improvement | Pyspark should not accept insecure p4yj gateways |
| 3.0.0 | [SPARK-26449](https://issues.apache.org/jira/browse/SPARK-26449) | Improvement | Missing Dataframe.transform API in Python API |
| 3.0.0 | [SPARK-26754](https://issues.apache.org/jira/browse/SPARK-26754) | Improvement | Add hasTrainingSummary to replace duplicate code in PySpark |
| 3.0.0 | [SPARK-26803](https://issues.apache.org/jira/browse/SPARK-26803) | Improvement | include sbin subdirectory in pyspark |
| 3.0.0 | [SPARK-26831](https://issues.apache.org/jira/browse/SPARK-26831) | Improvement | bin/pyspark: avoid hardcoded `python` command and improve version checks |
| 3.0.0 | [SPARK-27659](https://issues.apache.org/jira/browse/SPARK-27659) | Improvement | Allow PySpark toLocalIterator to prefetch data |
| 3.0.0 | [SPARK-27884](https://issues.apache.org/jira/browse/SPARK-27884) | Story | Deprecate Python 2 and Python 3 prior to 3.6 support in Spark 3.0 |
| 3.0.0 | [SPARK-27968](https://issues.apache.org/jira/browse/SPARK-27968) | Improvement | ArrowEvalPythonExec.evaluate shouldn't eagerly read the first batch |
| 3.0.0 | [SPARK-28130](https://issues.apache.org/jira/browse/SPARK-28130) | Improvement | Pretty messages not being printed for skipped PySpark tests when xmlrunner is available |
| 3.0.0 | [SPARK-28507](https://issues.apache.org/jira/browse/SPARK-28507) | Improvement | remove deprecated API context(self, sqlContext) from pyspark/ml/util.py |
| 3.0.0 | [SPARK-28622](https://issues.apache.org/jira/browse/SPARK-28622) | Improvement | Move PullOutPythonUDFInJoinCondition rule into 'Extract Python UDFs' |
| 3.0.0 | [SPARK-28654](https://issues.apache.org/jira/browse/SPARK-28654) | Improvement | Move "Extract Python UDFs" to the last in optimizer |
| 3.0.0 | [SPARK-28678](https://issues.apache.org/jira/browse/SPARK-28678) | Improvement | Specify that start index is 1-based in docstring of pyspark.sql.functions.slice |
| 3.0.0 | [SPARK-30084](https://issues.apache.org/jira/browse/SPARK-30084) | Improvement | Add docs showing how to automatically rebuild Python API docs |
| 3.0.0 | [SPARK-30128](https://issues.apache.org/jira/browse/SPARK-30128) | Improvement | Promote remaining "hidden" PySpark DataFrameReader options to load APIs |
| 3.0.0 | [SPARK-30231](https://issues.apache.org/jira/browse/SPARK-30231) | Improvement | Support explain mode in PySpark df.explain |
| 3.0.0 | [SPARK-30539](https://issues.apache.org/jira/browse/SPARK-30539) | Improvement | DataFrame.tail in PySpark API |
| 3.0.0 | [SPARK-30861](https://issues.apache.org/jira/browse/SPARK-30861) | Improvement | Deprecate constructor of SQLContext and getOrCreate in SQLContext at PySpark |
| 3.0.0 | [SPARK-31748](https://issues.apache.org/jira/browse/SPARK-31748) | Improvement | Document resource module in PySpark doc and rename/move classes |
| 3.0.0 | [SPARK-31767](https://issues.apache.org/jira/browse/SPARK-31767) | Improvement | Remove ResourceInformation in pyspark module's namespace |
| 3.0.0 | [SPARK-31807](https://issues.apache.org/jira/browse/SPARK-31807) | Improvement | Use python 3 style in release-build.sh |
| 3.1.1 | [SPARK-34398](https://issues.apache.org/jira/browse/SPARK-34398) | Improvement | Missing pyspark 3.1.1 doc migration |
| 3.2.0 | [SPARK-35419](https://issues.apache.org/jira/browse/SPARK-35419) | Improvement | Enable spark.sql.execution.pyspark.udf.simplifiedTraceback.enabled by default |
| 3.2.0 | [SPARK-35498](https://issues.apache.org/jira/browse/SPARK-35498) | Improvement | Add an API "inheritable_thread_target" which return a wrapped thread target for pyspark pin thread mode |
| 3.2.0 | [SPARK-35946](https://issues.apache.org/jira/browse/SPARK-35946) | Improvement | Respect Py4J server if InheritableThread API |
| 3.2.0 | [SPARK-35986](https://issues.apache.org/jira/browse/SPARK-35986) | Improvement | fix pyspark.rdd.RDD.histogram's buckets argument |
| 3.2.0 | [SPARK-36062](https://issues.apache.org/jira/browse/SPARK-36062) | Improvement | Try to capture faulthanlder when a Python worker crashes. |
| 3.2.0 | [SPARK-36154](https://issues.apache.org/jira/browse/SPARK-36154) | Improvement | pyspark documentation doesn't mention week and quarter as valid format arguments to trunc |
| 3.2.0 | [SPARK-36158](https://issues.apache.org/jira/browse/SPARK-36158) | Improvement | pyspark sql/functions documentation for months_between isn't as precise as scala version |
| 3.2.0 | [SPARK-36181](https://issues.apache.org/jira/browse/SPARK-36181) | Improvement | Update pyspark sql readwriter documentation to Scala level |
| 3.2.0 | [SPARK-36198](https://issues.apache.org/jira/browse/SPARK-36198) | Improvement | Skip UNIDOC generation in pyspark GHA job |
| 3.2.0 | [SPARK-36226](https://issues.apache.org/jira/browse/SPARK-36226) | Improvement | improve python docstring links to other pyspark classes |
| 3.2.0 | [SPARK-36285](https://issues.apache.org/jira/browse/SPARK-36285) | Improvement | Skip MiMa in PySpark GHA job |
| 3.2.0 | [SPARK-36288](https://issues.apache.org/jira/browse/SPARK-36288) | Improvement | Update API usage on pyspark pandas documents |
<!-- AUTO:timeline END -->
