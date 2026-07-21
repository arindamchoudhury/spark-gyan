# SparkR

> Source: `release-notes/spark_all_changelogs.txt`. Timeline rows below are generated from `_catalog.jsonl`; prose outside the AUTO markers is hand-written.

## How it evolved

### 1.x era — SparkR's first packaging on the DataFrame API

1.4.0 was the first release to package SparkR at all, an R binding built on the new DataFrame API rather than raw RDDs — R users got a `data.frame`-like interface backed by Spark's distributed execution from day one. 1.4.1 quickly followed with support for initializing SparkR with Spark packages (SPARK-8506) and user-defined schemas when reading from data sources (SPARK-8085). 1.5.0 focused on usability: clearer R-facing error messages (SPARK-8742) and aliases that made DataFrame functions read more idiomatically to R users (SPARK-9315). 1.6.0 extended SparkR into modeling, adding R-like summary statistics for generalized linear models (SPARK-9836) and feature interaction terms in the R formula interface (SPARK-9681).

### 2.x era — UDFs, structured streaming, and array functions for R

2.0.0 was transformative for SparkR: three new user-defined-function forms (`dapply`, `gapply`, `lapply`) for partition-based UDFs and hyperparameter tuning, save/load support for all ML models, broader GLM family/link support, and DataFrame functionality (window functions, JDBC/CSV readers, `SparkSession`) reaching parity with Scala/Python. 2.2.0 added a Structured Streaming API for R (SPARK-19654) and completed Catalog API coverage (SPARK-20159). 2.3.0 extended that streaming API with `withWatermark`, triggers, `partitionBy`, and stream-stream joins (SPARK-22933), plus DDL-formatted UDF schemas (SPARK-21266). 2.4.0 was largely array-function catch-up — `array_intersect`, `array_join`, `array_sort`, `flatten`, `map_entries`, and a dozen more — bringing R's function surface in line with the array/map functions Scala and Python had gained via higher-order functions.

### 3.x era — higher-order functions and steady function parity

3.0.0 added eager execution for the R shell/IDE (SPARK-24572) and the `forall` higher-order function, alongside routine parity work (`overlay`, `from_csv`/`schema_of_csv`). 3.1.1 added a full SparkR interface for higher-order functions (SPARK-30682), `from_avro`/`to_avro` (SPARK-33304), and bumped the minimum Arrow version to 1.0.0 (SPARK-32452). 3.2.0 added `current_user` (SPARK-21957) and subexpression elimination for higher-order functions. 3.3.0 migrated SparkR's documentation to pkgdown (SPARK-37474) and added `max_by`/`min_by`, `ILIKE`, `sec`/`csc`/`cot` — closing gaps with the Scala/Python function surface. 3.4.0 added `unpivot`/`melt`, comparator-based `array_sort`, R 4.2.0 support, and Catalog-API compatibility with the three-layer namespace (SPARK-39579) — SparkR tracking the same catalog changes landing in Spark SQL rather than diverging on its own feature set.

### 4.x era — R 3.x support dropped

SparkR's only 4.x record is its most consequential in years: 4.2.0 dropped support for R 3.x (SPARK-57767), effectively raising the floor to R 4.x. This is consistent with SparkR's broader deprecation trajectory elsewhere in the Spark 4.0.0 changelog, but the sparkr area itself records no new 4.x feature work — the 3.4.0 catalog-parity additions remain the last capability investment, and the 4.x line here is entirely about narrowing supported versions rather than adding capability.

## Timeline

<!-- AUTO:timeline START -->
| Release | JIRA | Type | Title |
|---|---|---|---|
| 1.4.0 | — | prose | SparkR packaged for the first time, an R binding based on the DataFrame API |
| 1.4.1 | [SPARK-8085](https://issues.apache.org/jira/browse/SPARK-8085) | prose | Support for user defined schemas when reading from data sources |
| 1.4.1 | [SPARK-8506](https://issues.apache.org/jira/browse/SPARK-8506) | prose | Support for Spark packages when initializing SparkR |
| 1.5.0 | [SPARK-6805](https://issues.apache.org/jira/browse/SPARK-6805) | Umbrella | MLlib + SparkR integration for 1.5 |
| 1.5.0 | [SPARK-6813](https://issues.apache.org/jira/browse/SPARK-6813) | New Feature | SparkR style guide |
| 1.5.0 | [SPARK-6820](https://issues.apache.org/jira/browse/SPARK-6820) | New Feature | Convert NAs to null type in SparkR DataFrames |
| 1.5.0 | [SPARK-7691](https://issues.apache.org/jira/browse/SPARK-7691) | Improvement | Use type-specific row accessor functions in CatalystTypeConverters' toScala functions |
| 1.5.0 | [SPARK-7714](https://issues.apache.org/jira/browse/SPARK-7714) | Improvement | SparkR tests should use more specific expectations than expect_true |
| 1.5.0 | [SPARK-8019](https://issues.apache.org/jira/browse/SPARK-8019) | New Feature | [SparkR] Create worker R processes with a command other then Rscript |
| 1.5.0 | [SPARK-8084](https://issues.apache.org/jira/browse/SPARK-8084) | Improvement | SparkR install script should fail with error if any packages required are not found |
| 1.5.0 | [SPARK-8124](https://issues.apache.org/jira/browse/SPARK-8124) | New Feature | Created more examples on SparkR DataFrames |
| 1.5.0 | [SPARK-8364](https://issues.apache.org/jira/browse/SPARK-8364) | New Feature | Add crosstab to SparkR DataFrames |
| 1.5.0 | [SPARK-8431](https://issues.apache.org/jira/browse/SPARK-8431) | New Feature | Add in operator to DataFrame Column in SparkR |
| 1.5.0 | [SPARK-8446](https://issues.apache.org/jira/browse/SPARK-8446) | New Feature | Add helper functions for testing physical SparkPlan operators |
| 1.5.0 | [SPARK-8742](https://issues.apache.org/jira/browse/SPARK-8742) | prose | Improved error messages for R |
| 1.5.0 | [SPARK-8807](https://issues.apache.org/jira/browse/SPARK-8807) | New Feature | Add between operator in SparkR |
| 1.5.0 | [SPARK-8847](https://issues.apache.org/jira/browse/SPARK-8847) | New Feature | String concatination with column in SparkR |
| 1.5.0 | [SPARK-9201](https://issues.apache.org/jira/browse/SPARK-9201) | New Feature | Integrate MLlib with SparkR using RFormula |
| 1.5.0 | [SPARK-9230](https://issues.apache.org/jira/browse/SPARK-9230) | New Feature | SparkR RFormula should support StringType features |
| 1.5.0 | [SPARK-9315](https://issues.apache.org/jira/browse/SPARK-9315) | prose | Aliases to make DataFrame functions more R-like |
| 1.5.0 | [SPARK-9391](https://issues.apache.org/jira/browse/SPARK-9391) | New Feature | Support minus, dot, and intercept operators in SparkR RFormula |
| 1.5.0 | [SPARK-10106](https://issues.apache.org/jira/browse/SPARK-10106) | New Feature | Add `ifelse` Column function to SparkR |
| 1.6.0 | [SPARK-6819](https://issues.apache.org/jira/browse/SPARK-6819) | New Feature | Support nested types in SparkR DataFrame |
| 1.6.0 | [SPARK-9647](https://issues.apache.org/jira/browse/SPARK-9647) | Umbrella | MLlib + SparkR integration for 1.6 |
| 1.6.0 | [SPARK-9681](https://issues.apache.org/jira/browse/SPARK-9681) | prose | Feature interactions in R formula |
| 1.6.0 | [SPARK-9836](https://issues.apache.org/jira/browse/SPARK-9836) | prose | R-like statistics for GLMs |
| 1.6.0 | [SPARK-11369](https://issues.apache.org/jira/browse/SPARK-11369) | Improvement | SparkR glm should support setting standardize |
| 1.6.0 | [SPARK-11468](https://issues.apache.org/jira/browse/SPARK-11468) | New Feature | Add R API for stddev/variance |
| 1.6.0 | [SPARK-11773](https://issues.apache.org/jira/browse/SPARK-11773) | New Feature | Implement collection functions in SparkR |
| 1.6.0 | [SPARK-11774](https://issues.apache.org/jira/browse/SPARK-11774) | New Feature | Implement "struct", "encode","decode" in SparkR |
| 1.6.0 | [SPARK-12025](https://issues.apache.org/jira/browse/SPARK-12025) | Improvement | Rename some window rank function names for SparkR |
| 1.6.0 | [SPARK-12034](https://issues.apache.org/jira/browse/SPARK-12034) | Improvement | Eliminate warnings in SparkR test cases |
| 1.6.0 | [SPARK-12144](https://issues.apache.org/jira/browse/SPARK-12144) | New Feature | Support more external data source API in SparkR |
| 1.6.0 | [SPARK-12364](https://issues.apache.org/jira/browse/SPARK-12364) | Improvement | Add ML example for SparkR |
| 2.0.0 | — | prose | SparkR UDFs: dapply, gapply, lapply |
| 2.0.0 | — | prose | SparkR GLMs support more families and link functions |
| 2.0.0 | — | prose | Save/load support for all SparkR ML models |
| 2.0.0 | — | prose | More SparkR DataFrame functionality: window functions, JDBC/CSV, SparkSession |
| 2.0.0 | [SPARK-7264](https://issues.apache.org/jira/browse/SPARK-7264) | New Feature | SparkR API for parallel functions |
| 2.0.0 | [SPARK-11395](https://issues.apache.org/jira/browse/SPARK-11395) | New Feature | Support over and window specification in SparkR |
| 2.0.0 | [SPARK-11774](https://issues.apache.org/jira/browse/SPARK-11774) | New Feature | Implement "struct", "encode","decode" in SparkR |
| 2.0.0 | [SPARK-12204](https://issues.apache.org/jira/browse/SPARK-12204) | New Feature | Implement drop method for DataFrame in SparkR |
| 2.0.0 | [SPARK-12337](https://issues.apache.org/jira/browse/SPARK-12337) | New Feature | Implement dropDuplicates() method of DataFrame in SparkR |
| 2.0.0 | [SPARK-12364](https://issues.apache.org/jira/browse/SPARK-12364) | Improvement | Add ML example for SparkR |
| 2.0.0 | [SPARK-12547](https://issues.apache.org/jira/browse/SPARK-12547) | Improvement | Tighten scala style checker enforcement for UDF registration |
| 2.0.0 | [SPARK-12566](https://issues.apache.org/jira/browse/SPARK-12566) | New Feature | GLM model family, link function support in SparkR:::glm |
| 2.0.0 | [SPARK-12645](https://issues.apache.org/jira/browse/SPARK-12645) | Improvement | SparkR support hash function |
| 2.0.0 | [SPARK-12903](https://issues.apache.org/jira/browse/SPARK-12903) | Improvement | Add covar_samp and covar_pop for SparkR |
| 2.0.0 | [SPARK-12910](https://issues.apache.org/jira/browse/SPARK-12910) | Improvement | Support for specifying version of R to use while creating sparkR libraries |
| 2.0.0 | [SPARK-13010](https://issues.apache.org/jira/browse/SPARK-13010) | New Feature | Survival analysis in SparkR |
| 2.0.0 | [SPARK-13011](https://issues.apache.org/jira/browse/SPARK-13011) | New Feature | K-means wrapper in SparkR |
| 2.0.0 | [SPARK-13389](https://issues.apache.org/jira/browse/SPARK-13389) | Improvement | SparkR support first/last with ignore NAs |
| 2.0.0 | [SPARK-13449](https://issues.apache.org/jira/browse/SPARK-13449) | New Feature | Naive Bayes wrapper in SparkR |
| 2.0.0 | [SPARK-13479](https://issues.apache.org/jira/browse/SPARK-13479) | New Feature | Python API for DataFrame approxQuantile |
| 2.0.0 | [SPARK-13504](https://issues.apache.org/jira/browse/SPARK-13504) | New Feature | Add approxQuantile for SparkR |
| 2.0.0 | [SPARK-13734](https://issues.apache.org/jira/browse/SPARK-13734) | New Feature | SparkR histogram |
| 2.0.0 | [SPARK-13925](https://issues.apache.org/jira/browse/SPARK-13925) | New Feature | Expose R-like summary statistics in SparkR::glm for more family and link functions |
| 2.0.0 | [SPARK-14303](https://issues.apache.org/jira/browse/SPARK-14303) | Improvement | Refactor k-means code in SparkRWrappers |
| 2.0.0 | [SPARK-14311](https://issues.apache.org/jira/browse/SPARK-14311) | Umbrella | Model persistence in SparkR 2.0 |
| 2.0.0 | [SPARK-14324](https://issues.apache.org/jira/browse/SPARK-14324) | Improvement | Refactor GLMs code in SparkRWrappers |
| 2.0.0 | [SPARK-14556](https://issues.apache.org/jira/browse/SPARK-14556) | Improvement | Code clean-ups for package o.a.s.sql.execution.streaming.state |
| 2.0.0 | [SPARK-14780](https://issues.apache.org/jira/browse/SPARK-14780) | Improvement | Add `setLogLevel` to SparkR |
| 2.0.0 | [SPARK-14808](https://issues.apache.org/jira/browse/SPARK-14808) | Umbrella | Spark MLlib, GraphX, SparkR 2.0 QA umbrella |
| 2.0.0 | [SPARK-14831](https://issues.apache.org/jira/browse/SPARK-14831) | Improvement | Make ML APIs in SparkR consistent |
| 2.0.0 | [SPARK-14995](https://issues.apache.org/jira/browse/SPARK-14995) | Improvement | Add "since" tag in Roxygen documentation for SparkR API methods |
| 2.0.0 | [SPARK-15030](https://issues.apache.org/jira/browse/SPARK-15030) | New Feature | Support formula in spark.kmeans in SparkR |
| 2.0.0 | [SPARK-15091](https://issues.apache.org/jira/browse/SPARK-15091) | Improvement | Fix warnings and a failure in SparkR test cases with testthat version 1.0.1 |
| 2.0.0 | [SPARK-15110](https://issues.apache.org/jira/browse/SPARK-15110) | New Feature | SparkR - Implement repartitionByColumn on DataFrame |
| 2.0.0 | [SPARK-15222](https://issues.apache.org/jira/browse/SPARK-15222) | Improvement | SparkR ML examples update in 2.0 |
| 2.0.0 | [SPARK-15294](https://issues.apache.org/jira/browse/SPARK-15294) | Improvement | Add pivot functionality to SparkR |
| 2.0.0 | [SPARK-15684](https://issues.apache.org/jira/browse/SPARK-15684) | Improvement | Not mask startsWith and endsWith in R |
| 2.0.0 | [SPARK-15908](https://issues.apache.org/jira/browse/SPARK-15908) | New Feature | Add varargs-type dropDuplicates() function in SparkR |
| 2.0.0 | [SPARK-16012](https://issues.apache.org/jira/browse/SPARK-16012) | Improvement | add gapplyCollect() for SparkDataFrame |
| 2.0.0 | [SPARK-16051](https://issues.apache.org/jira/browse/SPARK-16051) | Improvement | Add `read.orc/write.orc` to SparkR |
| 2.0.0 | [SPARK-16053](https://issues.apache.org/jira/browse/SPARK-16053) | Improvement | Add `spark_partition_id` in SparkR |
| 2.0.0 | [SPARK-16059](https://issues.apache.org/jira/browse/SPARK-16059) | Improvement | Add `monotonically_increasing_id` function in SparkR |
| 2.0.0 | [SPARK-16090](https://issues.apache.org/jira/browse/SPARK-16090) | Umbrella | Improve method grouping in SparkR generated docs |
| 2.0.0 | [SPARK-16259](https://issues.apache.org/jira/browse/SPARK-16259) | Improvement | Cleanup options for DataFrame reader API in Python |
| 2.0.1 | [SPARK-17577](https://issues.apache.org/jira/browse/SPARK-17577) | Improvement | SparkR support add files to Spark job and get by executors |
| 2.1.0 | [SPARK-11879](https://issues.apache.org/jira/browse/SPARK-11879) | Improvement | Checkpoint support for DataFrame/Dataset |
| 2.1.0 | [SPARK-16442](https://issues.apache.org/jira/browse/SPARK-16442) | Umbrella | MLlib wrappers for SparkR 2.1 |
| 2.1.0 | [SPARK-16710](https://issues.apache.org/jira/browse/SPARK-16710) | Improvement | SparkR spark.glm should support weightCol |
| 2.1.0 | [SPARK-17178](https://issues.apache.org/jira/browse/SPARK-17178) | Improvement | Allow to set sparkr shell command through --conf |
| 2.1.0 | [SPARK-17241](https://issues.apache.org/jira/browse/SPARK-17241) | Improvement | SparkR spark.glm should have configurable regularization parameter |
| 2.1.0 | [SPARK-17315](https://issues.apache.org/jira/browse/SPARK-17315) | New Feature | Add Kolmogorov-Smirnov Test to SparkR |
| 2.1.0 | [SPARK-17317](https://issues.apache.org/jira/browse/SPARK-17317) | Improvement | Add package vignette to SparkR |
| 2.1.0 | [SPARK-17499](https://issues.apache.org/jira/browse/SPARK-17499) | Improvement | make the default params in sparkR spark.mlp consistent with MultilayerPerceptronClassifier |
| 2.1.0 | [SPARK-17551](https://issues.apache.org/jira/browse/SPARK-17551) | Improvement | support null ordering for DataFrame API |
| 2.1.0 | [SPARK-17577](https://issues.apache.org/jira/browse/SPARK-17577) | Improvement | SparkR support add files to Spark job and get by executors |
| 2.1.0 | [SPARK-17665](https://issues.apache.org/jira/browse/SPARK-17665) | Improvement | SparkR does not support options in other types consistently other APIs |
| 2.1.0 | [SPARK-17919](https://issues.apache.org/jira/browse/SPARK-17919) | Story | Make timeout to RBackend configurable in SparkR |
| 2.1.0 | [SPARK-17961](https://issues.apache.org/jira/browse/SPARK-17961) | Improvement | Add storageLevel to Dataset for SparkR |
| 2.1.0 | [SPARK-18007](https://issues.apache.org/jira/browse/SPARK-18007) | Improvement | update SparkR MLP - add initalWeights parameter |
| 2.1.0 | [SPARK-18349](https://issues.apache.org/jira/browse/SPARK-18349) | Improvement | Update R API documentation on ml model summary |
| 2.1.0 | [SPARK-18714](https://issues.apache.org/jira/browse/SPARK-18714) | New Feature | SparkSession.time - a simple timer function |
| 2.1.0 | [SPARK-18797](https://issues.apache.org/jira/browse/SPARK-18797) | Improvement | Update spark.logit in sparkr-vignettes |
| 2.2.0 | [SPARK-18285](https://issues.apache.org/jira/browse/SPARK-18285) | Improvement | approxQuantile in R support multi-column |
| 2.2.0 | [SPARK-18335](https://issues.apache.org/jira/browse/SPARK-18335) | Improvement | Add a numSlices parameter to SparkR's createDataFrame |
| 2.2.0 | [SPARK-18788](https://issues.apache.org/jira/browse/SPARK-18788) | New Feature | Add getNumPartitions() to SparkR |
| 2.2.0 | [SPARK-18821](https://issues.apache.org/jira/browse/SPARK-18821) | New Feature | Bisecting k-means wrapper in SparkR |
| 2.2.0 | [SPARK-18862](https://issues.apache.org/jira/browse/SPARK-18862) | Improvement | Split SparkR mllib.R into multiple files |
| 2.2.0 | [SPARK-18903](https://issues.apache.org/jira/browse/SPARK-18903) | Improvement | uiWebUrl is not accessible to SparkR |
| 2.2.0 | [SPARK-19282](https://issues.apache.org/jira/browse/SPARK-19282) | Improvement | RandomForestRegressionModel should expose getMaxDepth in R |
| 2.2.0 | [SPARK-19391](https://issues.apache.org/jira/browse/SPARK-19391) | Improvement | Tweedie GLM API in SparkR |
| 2.2.0 | [SPARK-19399](https://issues.apache.org/jira/browse/SPARK-19399) | prose | Coalesce on DataFrame and coalesce on column in SparkR |
| 2.2.0 | [SPARK-19456](https://issues.apache.org/jira/browse/SPARK-19456) | New Feature | Add LinearSVC R API |
| 2.2.0 | [SPARK-19572](https://issues.apache.org/jira/browse/SPARK-19572) | Improvement | Allow to disable hive in sparkR shell |
| 2.2.0 | [SPARK-19616](https://issues.apache.org/jira/browse/SPARK-19616) | Improvement | weightCol and aggregationDepth should be improved for some SparkR APIs |
| 2.2.0 | [SPARK-19654](https://issues.apache.org/jira/browse/SPARK-19654) | prose | Structured Streaming API for R |
| 2.2.0 | [SPARK-19669](https://issues.apache.org/jira/browse/SPARK-19669) | New Feature | Open up visibility for sharedState, sessionState, and a few other functions |
| 2.2.0 | [SPARK-19795](https://issues.apache.org/jira/browse/SPARK-19795) | prose | SparkR column functions to_json/from_json |
| 2.2.0 | [SPARK-20020](https://issues.apache.org/jira/browse/SPARK-20020) | prose | SparkR DataFrame checkpointing support |
| 2.2.0 | [SPARK-20092](https://issues.apache.org/jira/browse/SPARK-20092) | Improvement | Trigger AppVeyor R tests for changes in Scala code related with R API |
| 2.2.0 | [SPARK-20159](https://issues.apache.org/jira/browse/SPARK-20159) | prose | Complete Catalog API support in R |
| 2.2.0 | [SPARK-20360](https://issues.apache.org/jira/browse/SPARK-20360) | Improvement | Create repr functions for interpreters to use |
| 2.3.0 | [SPARK-15767](https://issues.apache.org/jira/browse/SPARK-15767) | prose | Several new SparkML API wrappers in SparkR |
| 2.3.0 | [SPARK-20726](https://issues.apache.org/jira/browse/SPARK-20726) | prose | Several new DataFrame API wrappers in SparkR |
| 2.3.0 | [SPARK-21266](https://issues.apache.org/jira/browse/SPARK-21266) | prose | SparkR UDF with DDL-formatted schema support |
| 2.3.0 | [SPARK-22933](https://issues.apache.org/jira/browse/SPARK-22933) | prose | Structured Streaming APIs for R: withWatermark, trigger, partitionBy, stream-stream joins |
| 2.4.0 | [SPARK-23770](https://issues.apache.org/jira/browse/SPARK-23770) | prose | repartitionByRange API added in SparkR |
| 2.4.0 | [SPARK-24054](https://issues.apache.org/jira/browse/SPARK-24054) | prose | array_position / element_at functions added to SparkR |
| 2.4.0 | [SPARK-24069](https://issues.apache.org/jira/browse/SPARK-24069) | prose | array_min / array_max functions added to SparkR |
| 2.4.0 | [SPARK-24185](https://issues.apache.org/jira/browse/SPARK-24185) | prose | flatten function added to SparkR |
| 2.4.0 | [SPARK-24187](https://issues.apache.org/jira/browse/SPARK-24187) | prose | array_join function added to SparkR |
| 2.4.0 | [SPARK-24197](https://issues.apache.org/jira/browse/SPARK-24197) | prose | array_sort function added to SparkR |
| 2.4.0 | [SPARK-24198](https://issues.apache.org/jira/browse/SPARK-24198) | prose | slice function added to SparkR |
| 2.4.0 | [SPARK-24331](https://issues.apache.org/jira/browse/SPARK-24331) | prose | arrays_overlap, array_repeat, map_entries added to SparkR |
| 2.4.0 | [SPARK-24537](https://issues.apache.org/jira/browse/SPARK-24537) | prose | array_remove/array_zip/map_from_arrays/array_distinct added to SparkR |
| 2.4.0 | [SPARK-25007](https://issues.apache.org/jira/browse/SPARK-25007) | prose | array_intersect/array_except/array_union/shuffle added to SparkR |
| 2.4.0 | [SPARK-25117](https://issues.apache.org/jira/browse/SPARK-25117) | prose | EXCEPT ALL and INTERSECT ALL support added in R |
| 2.4.0 | [SPARK-25234](https://issues.apache.org/jira/browse/SPARK-25234) | prose | Avoid integer overflow in SparkR parallelize |
| 3.0.0 | [SPARK-21291](https://issues.apache.org/jira/browse/SPARK-21291) | Improvement | R partitionBy API |
| 3.0.0 | [SPARK-24572](https://issues.apache.org/jira/browse/SPARK-24572) | prose | “eager execution” for R shell, IDE |
| 3.0.0 | [SPARK-26107](https://issues.apache.org/jira/browse/SPARK-26107) | Improvement | Extend ReplaceNullWithFalseInPredicate to support higher-order functions: ArrayExists, ArrayFilter, MapFilter |
| 3.0.0 | [SPARK-26180](https://issues.apache.org/jira/browse/SPARK-26180) | Improvement | Add a withCreateTempDir function to the SparkCore test case |
| 3.0.0 | [SPARK-26227](https://issues.apache.org/jira/browse/SPARK-26227) | Improvement | from_[csv\|json] should accept schema_of_[csv\|json] in R API |
| 3.0.0 | [SPARK-26860](https://issues.apache.org/jira/browse/SPARK-26860) | Improvement | Improve RangeBetween docs in Pyspark, SparkR |
| 3.0.0 | [SPARK-27297](https://issues.apache.org/jira/browse/SPARK-27297) | Improvement | Add higher order functions to Scala API |
| 3.0.0 | [SPARK-27794](https://issues.apache.org/jira/browse/SPARK-27794) | Improvement | Use secure URLs for downloading CRAN artifacts |
| 3.0.0 | [SPARK-27834](https://issues.apache.org/jira/browse/SPARK-27834) | Improvement | Make separate PySpark/SparkR vectorization configurations |
| 3.0.0 | [SPARK-27905](https://issues.apache.org/jira/browse/SPARK-27905) | New Feature | Add higher order function`forall` |
| 3.0.0 | [SPARK-28615](https://issues.apache.org/jira/browse/SPARK-28615) | Improvement | Add a guide line for dataframe functions to say column signature function is by default |
| 3.0.0 | [SPARK-30607](https://issues.apache.org/jira/browse/SPARK-30607) | Improvement | overlay wrappers for SparkR and PySpark |
| 3.0.0 | [SPARK-31510](https://issues.apache.org/jira/browse/SPARK-31510) | Improvement | Set setwd in R documentation build |
| 3.0.0 | [SPARK-31785](https://issues.apache.org/jira/browse/SPARK-31785) | Improvement | Add a helper function to test all parquet readers |
| 3.0.1 | [SPARK-32451](https://issues.apache.org/jira/browse/SPARK-32451) | prose | Support Apache Arrow 1.0.0 in SparkR |
| 3.1.1 | [SPARK-30682](https://issues.apache.org/jira/browse/SPARK-30682) | prose | Add SparkR interface for higher order functions |
| 3.1.1 | [SPARK-32452](https://issues.apache.org/jira/browse/SPARK-32452) | prose | Minimum Arrow version bumped up to 1.0.0 |
| 3.1.1 | [SPARK-32946](https://issues.apache.org/jira/browse/SPARK-32946) | prose | Support withColumn in SparkR functions |
| 3.1.1 | [SPARK-32949](https://issues.apache.org/jira/browse/SPARK-32949) | prose | Support timestamp_seconds in SparkR functions |
| 3.1.1 | [SPARK-33030](https://issues.apache.org/jira/browse/SPARK-33030) | prose | Support nth_value in SparkR functions |
| 3.1.1 | [SPARK-33304](https://issues.apache.org/jira/browse/SPARK-33304) | prose | Support from_avro and to_avro |
| 3.1.1 | [SPARK-33622](https://issues.apache.org/jira/browse/SPARK-33622) | prose | Support array_to_vector in SparkR functions |
| 3.1.1 | [SPARK-34132](https://issues.apache.org/jira/browse/SPARK-34132) | Improvement | Update Roxygen version references to 7.1.1 |
| 3.2.0 | [SPARK-21957](https://issues.apache.org/jira/browse/SPARK-21957) | New Feature | Add current_user function |
| 3.2.0 | [SPARK-34481](https://issues.apache.org/jira/browse/SPARK-34481) | Improvement | Refactor dataframe reader/writer path option logic |
| 3.2.0 | [SPARK-35171](https://issues.apache.org/jira/browse/SPARK-35171) | Improvement | Declare the markdown package as a dependency of the SparkR package |
| 3.2.0 | [SPARK-35180](https://issues.apache.org/jira/browse/SPARK-35180) | Improvement | Allow to build SparkR with SBT |
| 3.2.0 | [SPARK-35230](https://issues.apache.org/jira/browse/SPARK-35230) | Improvement | Move custom metric classes to proper package |
| 3.2.0 | [SPARK-35580](https://issues.apache.org/jira/browse/SPARK-35580) | Improvement | Support subexpression elimination for higher order functions |
| 3.2.0 | [SPARK-35636](https://issues.apache.org/jira/browse/SPARK-35636) | Improvement | Do not push down extract value in higher order function that references both sides of a join |
| 3.2.0 | [SPARK-35885](https://issues.apache.org/jira/browse/SPARK-35885) | prose | Use keyserver.ubuntu.com as a keyserver for CRAN |
| 3.2.0 | [SPARK-36631](https://issues.apache.org/jira/browse/SPARK-36631) | Improvement | Ask users if they want to download and install SparkR in non Spark scripts |
| 3.3.0 | [SPARK-36688](https://issues.apache.org/jira/browse/SPARK-36688) | prose | Add cot as an R function |
| 3.3.0 | [SPARK-36824](https://issues.apache.org/jira/browse/SPARK-36824) | prose | Add sec and csc as R functions |
| 3.3.0 | [SPARK-36899](https://issues.apache.org/jira/browse/SPARK-36899) | prose | Support ILIKE API on R |
| 3.3.0 | [SPARK-36976](https://issues.apache.org/jira/browse/SPARK-36976) | prose | Add max_by/min_by API to SparkR |
| 3.3.0 | [SPARK-37108](https://issues.apache.org/jira/browse/SPARK-37108) | prose | Expose make_date expression in R |
| 3.3.0 | [SPARK-37474](https://issues.apache.org/jira/browse/SPARK-37474) | prose | Migrate SparkR docs to pkgdown |
| 3.4.0 | [SPARK-39372](https://issues.apache.org/jira/browse/SPARK-39372) | prose | Support R 4.2.0 |
| 3.4.0 | [SPARK-39579](https://issues.apache.org/jira/browse/SPARK-39579) | prose | Make Catalog API be compatible with 3-layer-namespace |
| 3.4.0 | [SPARK-40114](https://issues.apache.org/jira/browse/SPARK-40114) | prose | Arrow 9.0.0 support with SparkR |
| 3.4.0 | [SPARK-40167](https://issues.apache.org/jira/browse/SPARK-40167) | prose | Add array_sort(column, comparator) |
| 3.4.0 | [SPARK-41267](https://issues.apache.org/jira/browse/SPARK-41267) | prose | Add unpivot / melt |
| 4.2.0 | [SPARK-57767](https://issues.apache.org/jira/browse/SPARK-57767) | prose | Drop support for R 3.x |
<!-- AUTO:timeline END -->
