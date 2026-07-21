# Misc / Other

> Source: `release-notes/spark_all_changelogs.txt`. Timeline rows below are generated from `_catalog.jsonl`; prose outside the AUTO markers is hand-written.

## How it evolved

### 0.x era — origins

This page collects cross-cutting or uncategorized 0.x items that don't map to a single subsystem — improved log messages tying RDDs/jobs back to source code (0.6.0), better error reporting for resource shortfalls and failures (0.7.0), non-serializable exceptions and oversized task results (0.7.3), and the archivable `JobLogger` class (0.8.0).

### 1.x era — instrumentation and error-message polish

The misc bucket stays thin in the 1.x line: 1.0.0 added internal instrumentation so applications could monitor and instrument their own Spark jobs, and 1.3.0 improved error reporting for a handful of easy-to-hit "gotcha" operations that had previously failed with unhelpful messages. Most of the era's cross-cutting work otherwise shows up as internal cleanup, documentation, and test-infrastructure changes rather than user-facing capability — a pattern that only grows as the codebase and contributor base expand through the later 1.5 and 1.6 releases.

### 2.x era — the grab-bag keeps growing with the codebase

As in earlier lines, most of the 2.x misc bucket is internal cleanup, testing, and documentation rather than user-facing capability: style-checker rules, deprecated-method removal ahead of the 2.0 API break (SPARK-11806), JSON-formatted logical/physical plan output for tooling (SPARK-12321), and an improved thread-dump page (SPARK-9516). A few entries reflect Spark's growing maturity as a project rather than a product — TPCDS query support for benchmarking (SPARK-12540), a Java linting script (SPARK-6990), and infinite-scrolling log viewers (SPARK-8171). The pattern from the 1.x line holds: as the codebase, test suite, and contributor base keep expanding, the cross-cutting bucket grows roughly in proportion, with no single 2.x release standing out as thematically different from the rest.

## Timeline

<!-- AUTO:timeline START -->
| Release | JIRA | Type | Title |
|---|---|---|---|
| 0.6.0 | — | prose | Log now records which program operation produced each RDD/job for easier debugging |
| 0.7.0 | — | prose | Better error reporting when jobs cannot launch due to insufficient resources |
| 0.7.0 | — | prose | Improved failure handling and error message reporting |
| 0.7.3 | — | prose | Better error reporting for non-serializable exceptions and large task results |
| 0.8.0 | — | prose | JobLogger class for archivable workload logs |
| 1.0.0 | — | prose | Internal instrumentation added to monitor and instrument Spark jobs |
| 1.3.0 | — | prose | Improved error reporting for certain gotcha operations |
| 1.5.0 | [SPARK-1564](https://issues.apache.org/jira/browse/SPARK-1564) | Improvement | Add JavaScript into Javadoc to turn ::Experimental:: and such into badges |
| 1.5.0 | [SPARK-2774](https://issues.apache.org/jira/browse/SPARK-2774) | Improvement | Set preferred locations for reduce tasks |
| 1.5.0 | [SPARK-3382](https://issues.apache.org/jira/browse/SPARK-3382) | Improvement | GradientDescent convergence tolerance |
| 1.5.0 | [SPARK-3617](https://issues.apache.org/jira/browse/SPARK-3617) | New Feature | Configurable case sensitivity |
| 1.5.0 | [SPARK-4151](https://issues.apache.org/jira/browse/SPARK-4151) | Improvement | Add string operation function trim, ltrim, rtrim, length to support SparkSql (HiveQL) |
| 1.5.0 | [SPARK-4273](https://issues.apache.org/jira/browse/SPARK-4273) | Improvement | Providing ExternalSet to avoid OOM when count(distinct) |
| 1.5.0 | [SPARK-5016](https://issues.apache.org/jira/browse/SPARK-5016) | Improvement | GaussianMixtureEM should distribute matrix inverse for large numFeatures, k |
| 1.5.0 | [SPARK-5090](https://issues.apache.org/jira/browse/SPARK-5090) | Improvement | The improvement of python converter for hbase |
| 1.5.0 | [SPARK-5180](https://issues.apache.org/jira/browse/SPARK-5180) | Story | Data source API improvement (Spark 1.5) |
| 1.5.0 | [SPARK-5482](https://issues.apache.org/jira/browse/SPARK-5482) | Improvement | Allow individual test suites in python/run-tests |
| 1.5.0 | [SPARK-6126](https://issues.apache.org/jira/browse/SPARK-6126) | Improvement | Support UDTs in JSON |
| 1.5.0 | [SPARK-6196](https://issues.apache.org/jira/browse/SPARK-6196) | Improvement | Add MAPR 4.0.2 support to the build |
| 1.5.0 | [SPARK-6324](https://issues.apache.org/jira/browse/SPARK-6324) | Improvement | Clean up usage code in command-line scripts |
| 1.5.0 | [SPARK-6749](https://issues.apache.org/jira/browse/SPARK-6749) | Improvement | Make metastore client robust to underlying socket connection loss |
| 1.5.0 | [SPARK-6833](https://issues.apache.org/jira/browse/SPARK-6833) | New Feature | Extend `addPackage` so that any given R file can be sourced in the worker before functions are run. |
| 1.5.0 | [SPARK-6980](https://issues.apache.org/jira/browse/SPARK-6980) | Improvement | Akka timeout exceptions indicate which conf controls them |
| 1.5.0 | [SPARK-7020](https://issues.apache.org/jira/browse/SPARK-7020) | Improvement | Restrict module testing based on commit contents |
| 1.5.0 | [SPARK-7042](https://issues.apache.org/jira/browse/SPARK-7042) | Improvement | Spark version of akka-actor_2.11 is not compatible with the official akka-actor_2.11 2.3.x |
| 1.5.0 | [SPARK-7137](https://issues.apache.org/jira/browse/SPARK-7137) | Improvement | Add checkInputColumn back to Params and print more info |
| 1.5.0 | [SPARK-7171](https://issues.apache.org/jira/browse/SPARK-7171) | Improvement | Allow for more flexible use of metric sources |
| 1.5.0 | [SPARK-7235](https://issues.apache.org/jira/browse/SPARK-7235) | Improvement | Refactor the GroupingSet implementation |
| 1.5.0 | [SPARK-7254](https://issues.apache.org/jira/browse/SPARK-7254) | New Feature | Extend PIC to handle Graphs directly |
| 1.5.0 | [SPARK-7261](https://issues.apache.org/jira/browse/SPARK-7261) | Improvement | Change default log level to WARN in the REPL |
| 1.5.0 | [SPARK-7357](https://issues.apache.org/jira/browse/SPARK-7357) | Improvement | Improving HBaseTest example |
| 1.5.0 | [SPARK-7444](https://issues.apache.org/jira/browse/SPARK-7444) | Improvement | Eliminate noisy css warn/error logs for UISeleniumSuite |
| 1.5.0 | [SPARK-7524](https://issues.apache.org/jira/browse/SPARK-7524) | Improvement | add configs for keytab and principal, move originals to internal |
| 1.5.0 | [SPARK-7533](https://issues.apache.org/jira/browse/SPARK-7533) | Improvement | Decrease spacing between AM-RM heartbeats. |
| 1.5.0 | [SPARK-7547](https://issues.apache.org/jira/browse/SPARK-7547) | New Feature | Example code for ElasticNet |
| 1.5.0 | [SPARK-7705](https://issues.apache.org/jira/browse/SPARK-7705) | Improvement | Cleanup of .sparkStaging directory fails if application is killed |
| 1.5.0 | [SPARK-7814](https://issues.apache.org/jira/browse/SPARK-7814) | Improvement | Turn code generation on by default |
| 1.5.0 | [SPARK-7826](https://issues.apache.org/jira/browse/SPARK-7826) | Improvement | Suppress extra calling getCacheLocs. |
| 1.5.0 | [SPARK-7878](https://issues.apache.org/jira/browse/SPARK-7878) | Improvement | Rename Stage.jobId to Stage.earliestJobId |
| 1.5.0 | [SPARK-7945](https://issues.apache.org/jira/browse/SPARK-7945) | Improvement | Do trim to values of properties |
| 1.5.0 | [SPARK-7983](https://issues.apache.org/jira/browse/SPARK-7983) | Improvement | Add require for one-based indices in loadLibSVMFile |
| 1.5.0 | [SPARK-8097](https://issues.apache.org/jira/browse/SPARK-8097) | Improvement | add ifExists parameter for dropTempTable |
| 1.5.0 | [SPARK-8126](https://issues.apache.org/jira/browse/SPARK-8126) | Improvement | Use temp directory under build dir for unit tests |
| 1.5.0 | [SPARK-8131](https://issues.apache.org/jira/browse/SPARK-8131) | Improvement | Improve Database support |
| 1.5.0 | [SPARK-8140](https://issues.apache.org/jira/browse/SPARK-8140) | Improvement | Remove empty model check in StreamingLinearAlgorithm |
| 1.5.0 | [SPARK-8149](https://issues.apache.org/jira/browse/SPARK-8149) | Improvement | Break ExpressionEvaluationSuite down to multiple files |
| 1.5.0 | [SPARK-8158](https://issues.apache.org/jira/browse/SPARK-8158) | Improvement | HiveShim improvement |
| 1.5.0 | [SPARK-8168](https://issues.apache.org/jira/browse/SPARK-8168) | Improvement | Add Python friendly constructor to PipelineModel |
| 1.5.0 | [SPARK-8282](https://issues.apache.org/jira/browse/SPARK-8282) | Improvement | Make number of threads used in RBackend configurable |
| 1.5.0 | [SPARK-8313](https://issues.apache.org/jira/browse/SPARK-8313) | New Feature | Support Spark Packages containing R code with --packages |
| 1.5.0 | [SPARK-8314](https://issues.apache.org/jira/browse/SPARK-8314) | Improvement | improvement in performance of MLUtils.appendBias |
| 1.5.0 | [SPARK-8343](https://issues.apache.org/jira/browse/SPARK-8343) | Improvement | Improve the Spark Streaming Guides |
| 1.5.0 | [SPARK-8429](https://issues.apache.org/jira/browse/SPARK-8429) | Improvement | Add ability to set additional tags |
| 1.5.0 | [SPARK-8478](https://issues.apache.org/jira/browse/SPARK-8478) | Improvement | Harmonize UDF-related code to use uniformly UDF instead of Udf |
| 1.5.0 | [SPARK-8479](https://issues.apache.org/jira/browse/SPARK-8479) | New Feature | Add numNonzeros and numActives to linalg.Matrices |
| 1.5.0 | [SPARK-8482](https://issues.apache.org/jira/browse/SPARK-8482) | Improvement | Add M4 instances support |
| 1.5.0 | [SPARK-8551](https://issues.apache.org/jira/browse/SPARK-8551) | New Feature | Python example code for elastic net |
| 1.5.0 | [SPARK-8575](https://issues.apache.org/jira/browse/SPARK-8575) | Improvement | Deprecate callUDF in favor of udf |
| 1.5.0 | [SPARK-8576](https://issues.apache.org/jira/browse/SPARK-8576) | Improvement | Add spark-ec2 options to assign launched instances into IAM roles and to set instance-initiated shutdown behavior |
| 1.5.0 | [SPARK-8596](https://issues.apache.org/jira/browse/SPARK-8596) | Improvement | Install and configure RStudio server on Spark EC2 |
| 1.5.0 | [SPARK-8647](https://issues.apache.org/jira/browse/SPARK-8647) | Improvement | Potential issues with the constant hashCode |
| 1.5.0 | [SPARK-8711](https://issues.apache.org/jira/browse/SPARK-8711) | New Feature | Add additional methods to JavaModel wrappers in trees |
| 1.5.0 | [SPARK-8723](https://issues.apache.org/jira/browse/SPARK-8723) | Improvement | improve code gen for divide and remainder |
| 1.5.0 | [SPARK-8771](https://issues.apache.org/jira/browse/SPARK-8771) | Improvement | Actor system deprecation tag uses deprecated deprecation tag |
| 1.5.0 | [SPARK-8776](https://issues.apache.org/jira/browse/SPARK-8776) | Improvement | Increase the default MaxPermSize |
| 1.5.0 | [SPARK-8867](https://issues.apache.org/jira/browse/SPARK-8867) | New Feature | Show the UDF usage for user. |
| 1.5.0 | [SPARK-8899](https://issues.apache.org/jira/browse/SPARK-8899) | Improvement | remove duplicated equals method for Row |
| 1.5.0 | [SPARK-8913](https://issues.apache.org/jira/browse/SPARK-8913) | Improvement | Follow-up on SPARK-8700. Cleanup the test |
| 1.5.0 | [SPARK-8949](https://issues.apache.org/jira/browse/SPARK-8949) | Improvement | Remove references to preferredNodeLocalityData in javadoc and print warning when used |
| 1.5.0 | [SPARK-8967](https://issues.apache.org/jira/browse/SPARK-8967) | New Feature | Implement @since as an annotation |
| 1.5.0 | [SPARK-8994](https://issues.apache.org/jira/browse/SPARK-8994) | Improvement | Tiny cleanups to Params, Pipeline |
| 1.5.0 | [SPARK-9022](https://issues.apache.org/jira/browse/SPARK-9022) | New Feature | UnsafeProject |
| 1.5.0 | [SPARK-9023](https://issues.apache.org/jira/browse/SPARK-9023) | New Feature | UnsafeExchange |
| 1.5.0 | [SPARK-9076](https://issues.apache.org/jira/browse/SPARK-9076) | Improvement | Improve NaN value handling |
| 1.5.0 | [SPARK-9115](https://issues.apache.org/jira/browse/SPARK-9115) | New Feature | date/time function: dayInYear |
| 1.5.0 | [SPARK-9130](https://issues.apache.org/jira/browse/SPARK-9130) | Improvement | throw exception when check equality between external and internal row |
| 1.5.0 | [SPARK-9143](https://issues.apache.org/jira/browse/SPARK-9143) | New Feature | Add planner rule for automatically inserting Unsafe <-> Safe row format converters |
| 1.5.0 | [SPARK-9179](https://issues.apache.org/jira/browse/SPARK-9179) | Improvement | Allow committers to specify the primary author of the PR to be merged |
| 1.5.0 | [SPARK-9185](https://issues.apache.org/jira/browse/SPARK-9185) | Improvement | improve code gen for mutable states to support complex initialization |
| 1.5.0 | [SPARK-9262](https://issues.apache.org/jira/browse/SPARK-9262) | Improvement | Treat Scala compiler warnings as errors |
| 1.5.0 | [SPARK-9268](https://issues.apache.org/jira/browse/SPARK-9268) | Improvement | Params.setDefault should not keep varargs annotation |
| 1.5.0 | [SPARK-9304](https://issues.apache.org/jira/browse/SPARK-9304) | Improvement | Improve backwards compatibility of SPARK-8401 |
| 1.5.0 | [SPARK-9305](https://issues.apache.org/jira/browse/SPARK-9305) | Improvement | Rename org.apache.spark.Row to Item |
| 1.5.0 | [SPARK-9457](https://issues.apache.org/jira/browse/SPARK-9457) | Umbrella | Sorting improvements |
| 1.5.0 | [SPARK-9486](https://issues.apache.org/jira/browse/SPARK-9486) | Improvement | Add aliasing to data sources to allow external packages to register themselves with Spark |
| 1.5.0 | [SPARK-9519](https://issues.apache.org/jira/browse/SPARK-9519) | Improvement | Confirm stop sc successfully when application was killed |
| 1.5.0 | [SPARK-9534](https://issues.apache.org/jira/browse/SPARK-9534) | Improvement | Enable javac lint for scalac parity; fix a lot of build warnings, 1.5.0 edition |
| 1.5.0 | [SPARK-9553](https://issues.apache.org/jira/browse/SPARK-9553) | Improvement | remove the createCode and createStructCode, and replace the usage of them by createStructCode |
| 1.5.0 | [SPARK-9564](https://issues.apache.org/jira/browse/SPARK-9564) | Epic | Spark 1.5.0 Testing Plan |
| 1.5.0 | [SPARK-9569](https://issues.apache.org/jira/browse/SPARK-9569) | Umbrella | Spark streaming 1.5.0 testing umbrella |
| 1.5.0 | [SPARK-9598](https://issues.apache.org/jira/browse/SPARK-9598) | Improvement | do not expose generic getter in internal row |
| 1.5.0 | [SPARK-9693](https://issues.apache.org/jira/browse/SPARK-9693) | Improvement | Reserve a page in all unsafe operators to avoid starving an operator |
| 1.5.0 | [SPARK-9845](https://issues.apache.org/jira/browse/SPARK-9845) | Improvement | Add built-in UDF |
| 1.5.0 | [SPARK-9934](https://issues.apache.org/jira/browse/SPARK-9934) | Improvement | Deprecate NIO ConnectionManager |
| 1.5.0 | [SPARK-10095](https://issues.apache.org/jira/browse/SPARK-10095) | Improvement | Should not use the private field of BigInteger |
| 1.5.0 | [SPARK-10099](https://issues.apache.org/jira/browse/SPARK-10099) | Improvement | Use @deprecated instead of @Deprecated in Scala code |
| 1.5.0 | [SPARK-10140](https://issues.apache.org/jira/browse/SPARK-10140) | Improvement | Add target fields to @Since annotation |
| 1.5.0 | [SPARK-10331](https://issues.apache.org/jira/browse/SPARK-10331) | Improvement | Update user guide to address minor comments during code review |
| 1.5.0 | [SPARK-10344](https://issues.apache.org/jira/browse/SPARK-10344) | Improvement | Add tests for extraStrategies |
| 1.5.0 | [SPARK-10348](https://issues.apache.org/jira/browse/SPARK-10348) | Improvement | Improve Spark ML user guide |
| 1.5.0 | [SPARK-10667](https://issues.apache.org/jira/browse/SPARK-10667) | Improvement | Add 86 Runnable TPCDS Queries into spark-sql-perf |
| 1.6.0 | [SPARK-3147](https://issues.apache.org/jira/browse/SPARK-3147) | New Feature | Implement streaming testing |
| 1.6.0 | [SPARK-9613](https://issues.apache.org/jira/browse/SPARK-9613) | Improvement | Ban use of JavaConversions and migrate all existing uses to JavaConverters |
| 1.6.0 | [SPARK-9867](https://issues.apache.org/jira/browse/SPARK-9867) | Improvement | Move utilities for binary data into ByteArray |
| 1.6.0 | [SPARK-9929](https://issues.apache.org/jira/browse/SPARK-9929) | Improvement | support adding metadata in withColumn |
| 1.6.0 | [SPARK-10022](https://issues.apache.org/jira/browse/SPARK-10022) | Improvement | Scala-Python method/parameter inconsistency check for ML during 1.5 QA |
| 1.6.0 | [SPARK-10371](https://issues.apache.org/jira/browse/SPARK-10371) | New Feature | Optimize sequential projections |
| 1.6.0 | [SPARK-10443](https://issues.apache.org/jira/browse/SPARK-10443) | Improvement | Refactor SortMergeOuterJoin to reduce duplication |
| 1.6.0 | [SPARK-10458](https://issues.apache.org/jira/browse/SPARK-10458) | Improvement | Would like to know if a given Spark Context is stopped or currently stopping |
| 1.6.0 | [SPARK-10547](https://issues.apache.org/jira/browse/SPARK-10547) | Improvement | Streamline / improve style of Java API tests |
| 1.6.0 | [SPARK-10565](https://issues.apache.org/jira/browse/SPARK-10565) | Improvement | New /api/v1/[path] APIs don't contain as much information as original /json API |
| 1.6.0 | [SPARK-10576](https://issues.apache.org/jira/browse/SPARK-10576) | Improvement | Move .java files out of src/main/scala |
| 1.6.0 | [SPARK-10585](https://issues.apache.org/jira/browse/SPARK-10585) | Improvement | only copy data once when generate unsafe projection |
| 1.6.0 | [SPARK-10615](https://issues.apache.org/jira/browse/SPARK-10615) | Improvement | changes assertEquals to assertEqual for existing unit tests |
| 1.6.0 | [SPARK-10674](https://issues.apache.org/jira/browse/SPARK-10674) | New Feature | Flaky test: network.sasl.SaslIntegrationSuite.testNoSaslClient |
| 1.6.0 | [SPARK-10721](https://issues.apache.org/jira/browse/SPARK-10721) | Improvement | Log warning when file deletion fails |
| 1.6.0 | [SPARK-10736](https://issues.apache.org/jira/browse/SPARK-10736) | Improvement | Use 1 for all ratings if $(ratingCol) = "" |
| 1.6.0 | [SPARK-10807](https://issues.apache.org/jira/browse/SPARK-10807) | New Feature | Add as.data.frame() as a synonym for collect() |
| 1.6.0 | [SPARK-10833](https://issues.apache.org/jira/browse/SPARK-10833) | Improvement | Inline, organize BSD/MIT licenses in LICENSE |
| 1.6.0 | [SPARK-10883](https://issues.apache.org/jira/browse/SPARK-10883) | Improvement | Document building each module individually |
| 1.6.0 | [SPARK-10930](https://issues.apache.org/jira/browse/SPARK-10930) | Improvement | History "Stages" page "duration" can be confusing |
| 1.6.0 | [SPARK-10932](https://issues.apache.org/jira/browse/SPARK-10932) | Improvement | Port two minor changes to release packaging scripts back into Spark repo |
| 1.6.0 | [SPARK-11056](https://issues.apache.org/jira/browse/SPARK-11056) | Improvement | Improve documentation on how to build Spark efficiently |
| 1.6.0 | [SPARK-11113](https://issues.apache.org/jira/browse/SPARK-11113) | Improvement | Remove DeveloperApi annotation from private classes |
| 1.6.0 | [SPARK-11119](https://issues.apache.org/jira/browse/SPARK-11119) | Improvement | cleanup unsafe array and map |
| 1.6.0 | [SPARK-11169](https://issues.apache.org/jira/browse/SPARK-11169) | Improvement | Remove the extra spaces in merge script |
| 1.6.0 | [SPARK-11226](https://issues.apache.org/jira/browse/SPARK-11226) | Improvement | Empty line in json file should be skipped |
| 1.6.0 | [SPARK-11235](https://issues.apache.org/jira/browse/SPARK-11235) | New Feature | Support streaming data using network library |
| 1.6.0 | [SPARK-11297](https://issues.apache.org/jira/browse/SPARK-11297) | Improvement | code example generated by include_example is not exactly the same with {% highlight %} |
| 1.6.0 | [SPARK-11371](https://issues.apache.org/jira/browse/SPARK-11371) | Improvement | Make "mean" an alias for "avg" operator |
| 1.6.0 | [SPARK-11423](https://issues.apache.org/jira/browse/SPARK-11423) | Improvement | Remove PrepareRDD |
| 1.6.0 | [SPARK-11440](https://issues.apache.org/jira/browse/SPARK-11440) | Improvement | Declare rest of @Experimental items non-experimental if they've existed since 1.2.0 |
| 1.6.0 | [SPARK-11449](https://issues.apache.org/jira/browse/SPARK-11449) | Improvement | PortableDataStream should be a factory |
| 1.6.0 | [SPARK-11456](https://issues.apache.org/jira/browse/SPARK-11456) | Improvement | Remove deprecatd junit.framework in Java tests |
| 1.6.0 | [SPARK-11462](https://issues.apache.org/jira/browse/SPARK-11462) | Improvement | Add JavaStreamingListener |
| 1.6.0 | [SPARK-11495](https://issues.apache.org/jira/browse/SPARK-11495) | Improvement | Fix potential socket / file handle leaks identified via static analysis |
| 1.6.0 | [SPARK-11506](https://issues.apache.org/jira/browse/SPARK-11506) | Improvement | Code Optimization to remove a redundant operation |
| 1.6.0 | [SPARK-11583](https://issues.apache.org/jira/browse/SPARK-11583) | Improvement | Make MapStatus use less memory uage |
| 1.6.0 | [SPARK-11646](https://issues.apache.org/jira/browse/SPARK-11646) | Improvement | WholeTextFileRDD should return Text rather than String |
| 1.6.0 | [SPARK-11685](https://issues.apache.org/jira/browse/SPARK-11685) | Improvement | Find duplicate content under examples/ |
| 1.6.0 | [SPARK-11736](https://issues.apache.org/jira/browse/SPARK-11736) | Improvement | Add MonotonicallyIncreasingID to function registry |
| 1.6.0 | [SPARK-11745](https://issues.apache.org/jira/browse/SPARK-11745) | Improvement | Enable more JSON parsing options for parsing non-standard JSON files |
| 1.6.0 | [SPARK-11750](https://issues.apache.org/jira/browse/SPARK-11750) | Improvement | revert SPARK-11727 and code clean up |
| 1.6.0 | [SPARK-11771](https://issues.apache.org/jira/browse/SPARK-11771) | Improvement | Maximum memory is determined by two params but error message only lists one. |
| 1.6.0 | [SPARK-11786](https://issues.apache.org/jira/browse/SPARK-11786) | Improvement | Tone down error messages from AkkaRpcEnv |
| 1.6.0 | [SPARK-11864](https://issues.apache.org/jira/browse/SPARK-11864) | Improvement | Improve performance of max/min |
| 1.6.0 | [SPARK-12007](https://issues.apache.org/jira/browse/SPARK-12007) | Improvement | Network library's RPC layer requires a lot of copying |
| 1.6.0 | [SPARK-12018](https://issues.apache.org/jira/browse/SPARK-12018) | Improvement | Refactor common subexpression elimination code |
| 1.6.0 | [SPARK-12035](https://issues.apache.org/jira/browse/SPARK-12035) | Improvement | Add more debug information in include_example tag of Jekyll |
| 1.6.0 | [SPARK-12044](https://issues.apache.org/jira/browse/SPARK-12044) | Improvement | Fix usage of isnan, isNaN |
| 1.6.0 | [SPARK-12057](https://issues.apache.org/jira/browse/SPARK-12057) | Improvement | Prevent failure on corrupt JSON records |
| 1.6.0 | [SPARK-12397](https://issues.apache.org/jira/browse/SPARK-12397) | Improvement | Improve error messages for data sources when they are not found |
| 2.0.0 | [SPARK-529](https://issues.apache.org/jira/browse/SPARK-529) | Improvement | Have a single file that controls the environmental variables and spark config options |
| 2.0.0 | [SPARK-3854](https://issues.apache.org/jira/browse/SPARK-3854) | Improvement | Scala style: require spaces before `{` |
| 2.0.0 | [SPARK-4587](https://issues.apache.org/jira/browse/SPARK-4587) | Umbrella | Model export/import |
| 2.0.0 | [SPARK-5293](https://issues.apache.org/jira/browse/SPARK-5293) | Umbrella | Enable Spark user applications to use different versions of Akka |
| 2.0.0 | [SPARK-5865](https://issues.apache.org/jira/browse/SPARK-5865) | Improvement | Add doc warnings for methods that return local data structures |
| 2.0.0 | [SPARK-6429](https://issues.apache.org/jira/browse/SPARK-6429) | Improvement | Add to style checker "hashCode and equals should be defined together" |
| 2.0.0 | [SPARK-6725](https://issues.apache.org/jira/browse/SPARK-6725) | Umbrella | Model export/import for Pipeline API (Scala) |
| 2.0.0 | [SPARK-6990](https://issues.apache.org/jira/browse/SPARK-6990) | New Feature | Add Java linting script |
| 2.0.0 | [SPARK-7992](https://issues.apache.org/jira/browse/SPARK-7992) | Improvement | Hide private classes/objects in in generated Java API doc |
| 2.0.0 | [SPARK-8171](https://issues.apache.org/jira/browse/SPARK-8171) | New Feature | Support Javascript-based infinite scrolling in Spark log viewers |
| 2.0.0 | [SPARK-8725](https://issues.apache.org/jira/browse/SPARK-8725) | Improvement | Test modules in topological order in dev/run-tests and python/run-tests |
| 2.0.0 | [SPARK-9383](https://issues.apache.org/jira/browse/SPARK-9383) | Improvement | Merge script should reset back to previous ref instead of detached commit |
| 2.0.0 | [SPARK-9516](https://issues.apache.org/jira/browse/SPARK-9516) | New Feature | Improve Thread Dump page |
| 2.0.0 | [SPARK-10123](https://issues.apache.org/jira/browse/SPARK-10123) | Improvement | Cannot set "--deploy-mode" in default configuration |
| 2.0.0 | [SPARK-10498](https://issues.apache.org/jira/browse/SPARK-10498) | Improvement | Add requirements file for create dev python tools |
| 2.0.0 | [SPARK-10509](https://issues.apache.org/jira/browse/SPARK-10509) | Improvement | Excessive param boiler plate code |
| 2.0.0 | [SPARK-11157](https://issues.apache.org/jira/browse/SPARK-11157) | Umbrella | Allow Spark to be built without assemblies |
| 2.0.0 | [SPARK-11337](https://issues.apache.org/jira/browse/SPARK-11337) | Umbrella | Make example code in user guide testable |
| 2.0.0 | [SPARK-11439](https://issues.apache.org/jira/browse/SPARK-11439) | Improvement | Optimization of creating sparse feature without dense one |
| 2.0.0 | [SPARK-11565](https://issues.apache.org/jira/browse/SPARK-11565) | Improvement | replace deprecated DigestUtils.shaHex call |
| 2.0.0 | [SPARK-11592](https://issues.apache.org/jira/browse/SPARK-11592) | Improvement | flush spark-sql command line history to history file |
| 2.0.0 | [SPARK-11717](https://issues.apache.org/jira/browse/SPARK-11717) | Improvement | Ignore R session and history files from git |
| 2.0.0 | [SPARK-11806](https://issues.apache.org/jira/browse/SPARK-11806) | Umbrella | Spark 2.0 deprecations and removals |
| 2.0.0 | [SPARK-11903](https://issues.apache.org/jira/browse/SPARK-11903) | Improvement | Deprecate make-distribution.sh --skip-java-test |
| 2.0.0 | [SPARK-11929](https://issues.apache.org/jira/browse/SPARK-11929) | Improvement | spark-shell log level customization is lost if user provides a log4j.properties file |
| 2.0.0 | [SPARK-12044](https://issues.apache.org/jira/browse/SPARK-12044) | Improvement | Fix usage of isnan, isNaN |
| 2.0.0 | [SPARK-12057](https://issues.apache.org/jira/browse/SPARK-12057) | Improvement | Prevent failure on corrupt JSON records |
| 2.0.0 | [SPARK-12074](https://issues.apache.org/jira/browse/SPARK-12074) | Improvement | Avoid memory copy involving ByteBuffer.wrap(ByteArrayOutputStream.toByteArray) |
| 2.0.0 | [SPARK-12164](https://issues.apache.org/jira/browse/SPARK-12164) | Improvement | [SQL] Display the binary/encoded values |
| 2.0.0 | [SPARK-12228](https://issues.apache.org/jira/browse/SPARK-12228) | Improvement | Use in-memory for execution hive's derby metastore |
| 2.0.0 | [SPARK-12321](https://issues.apache.org/jira/browse/SPARK-12321) | New Feature | JSON format for logical/physical execution plans |
| 2.0.0 | [SPARK-12332](https://issues.apache.org/jira/browse/SPARK-12332) | Improvement | Typo in ResetSystemProperties.scala's comments |
| 2.0.0 | [SPARK-12374](https://issues.apache.org/jira/browse/SPARK-12374) | Improvement | Improve performance of Range APIs via adding logical/physical operators |
| 2.0.0 | [SPARK-12384](https://issues.apache.org/jira/browse/SPARK-12384) | Improvement | Allow -Xms to be set differently then -Xmx |
| 2.0.0 | [SPARK-12388](https://issues.apache.org/jira/browse/SPARK-12388) | Improvement | Change default compressor to LZ4 |
| 2.0.0 | [SPARK-12397](https://issues.apache.org/jira/browse/SPARK-12397) | Improvement | Improve error messages for data sources when they are not found |
| 2.0.0 | [SPARK-12401](https://issues.apache.org/jira/browse/SPARK-12401) | New Feature | Add support for enums in postgres |
| 2.0.0 | [SPARK-12510](https://issues.apache.org/jira/browse/SPARK-12510) | Improvement | Refactor ActorReceiver to support Java |
| 2.0.0 | [SPARK-12534](https://issues.apache.org/jira/browse/SPARK-12534) | Improvement | Document missing command line options to Spark properties mapping |
| 2.0.0 | [SPARK-12537](https://issues.apache.org/jira/browse/SPARK-12537) | Improvement | Add option to accept quoting of all character backslash quoting mechanism |
| 2.0.0 | [SPARK-12540](https://issues.apache.org/jira/browse/SPARK-12540) | Epic | Support all TPCDS queries |
| 2.0.0 | [SPARK-12545](https://issues.apache.org/jira/browse/SPARK-12545) | New Feature | Support exists condition |
| 2.0.0 | [SPARK-12597](https://issues.apache.org/jira/browse/SPARK-12597) | Improvement | Use udf to replace callUDF for ML |
| 2.0.0 | [SPARK-12653](https://issues.apache.org/jira/browse/SPARK-12653) | Improvement | Re-enable test "SPARK-8489: MissingRequirementError during reflection" |
| 2.0.0 | [SPARK-12656](https://issues.apache.org/jira/browse/SPARK-12656) | Improvement | Rewrite Intersect phyiscal plan using semi-join |
| 2.0.0 | [SPARK-12660](https://issues.apache.org/jira/browse/SPARK-12660) | Improvement | Rewrite except using anti-join |
| 2.0.0 | [SPARK-12663](https://issues.apache.org/jira/browse/SPARK-12663) | Improvement | More informative error message in MLUtils.loadLibSVMFile |
| 2.0.0 | [SPARK-12735](https://issues.apache.org/jira/browse/SPARK-12735) | Improvement | Move spark-ec2 scripts to AMPLab |
| 2.0.0 | [SPARK-12757](https://issues.apache.org/jira/browse/SPARK-12757) | Improvement | Use reference counting to prevent blocks from being evicted during reads |
| 2.0.0 | [SPARK-12788](https://issues.apache.org/jira/browse/SPARK-12788) | Improvement | Simplify BooleanEquality by using casts |
| 2.0.0 | [SPARK-12856](https://issues.apache.org/jira/browse/SPARK-12856) | Improvement | speed up hashCode of unsafe array |
| 2.0.0 | [SPARK-12879](https://issues.apache.org/jira/browse/SPARK-12879) | Improvement | improve unsafe row writing framework |
| 2.0.0 | [SPARK-12882](https://issues.apache.org/jira/browse/SPARK-12882) | Improvement | simplify bucket tests and add more comments |
| 2.0.0 | [SPARK-12898](https://issues.apache.org/jira/browse/SPARK-12898) | Improvement | Consider having dummyCallSite for HiveTableScan |
| 2.0.0 | [SPARK-12913](https://issues.apache.org/jira/browse/SPARK-12913) | Improvement | Reimplement stat functions as declarative function |
| 2.0.0 | [SPARK-12925](https://issues.apache.org/jira/browse/SPARK-12925) | Improvement | Improve HiveInspectors.unwrap for StringObjectInspector.getPrimitiveWritableObject |
| 2.0.0 | [SPARK-13008](https://issues.apache.org/jira/browse/SPARK-13008) | Improvement | Make ML Python package all list have one algorithm per line |
| 2.0.0 | [SPARK-13086](https://issues.apache.org/jira/browse/SPARK-13086) | Improvement | Spark Shell for 2.11 does not allow loading files via '-i' |
| 2.0.0 | [SPARK-13094](https://issues.apache.org/jira/browse/SPARK-13094) | Improvement | No encoder implicits for Seq[Primitive] |
| 2.0.0 | [SPARK-13113](https://issues.apache.org/jira/browse/SPARK-13113) | Improvement | Remove unnecessary bit operation when decoding page number |
| 2.0.0 | [SPARK-13118](https://issues.apache.org/jira/browse/SPARK-13118) | Improvement | Support for classes defined in package objects |
| 2.0.0 | [SPARK-13131](https://issues.apache.org/jira/browse/SPARK-13131) | Improvement | Use best time and average time in micro benchmark |
| 2.0.0 | [SPARK-13138](https://issues.apache.org/jira/browse/SPARK-13138) | Improvement | Add "logical" package prefix for ddl.scala |
| 2.0.0 | [SPARK-13139](https://issues.apache.org/jira/browse/SPARK-13139) | Improvement | Create native DDL commands |
| 2.0.0 | [SPARK-13147](https://issues.apache.org/jira/browse/SPARK-13147) | Improvement | improve readability of generated code |
| 2.0.0 | [SPARK-13148](https://issues.apache.org/jira/browse/SPARK-13148) | New Feature | document zero-keytab Oozie application launch; add diagnostics |
| 2.0.0 | [SPARK-13154](https://issues.apache.org/jira/browse/SPARK-13154) | Improvement | Add pydoc lint for docs |
| 2.0.0 | [SPARK-13241](https://issues.apache.org/jira/browse/SPARK-13241) | Improvement | add long--formatted timestamps to org.apache.spark.status.api.v1.ApplicationAttemptInfo |
| 2.0.0 | [SPARK-13248](https://issues.apache.org/jira/browse/SPARK-13248) | Improvement | Remove depecrated Streaming APIs |
| 2.0.0 | [SPARK-13264](https://issues.apache.org/jira/browse/SPARK-13264) | Improvement | Remove multi-byte character in spark-env.sh.template |
| 2.0.0 | [SPARK-13271](https://issues.apache.org/jira/browse/SPARK-13271) | Improvement | Better error message if 'path' is not specified |
| 2.0.0 | [SPARK-13293](https://issues.apache.org/jira/browse/SPARK-13293) | Improvement | Generate code for Expand |
| 2.0.0 | [SPARK-13339](https://issues.apache.org/jira/browse/SPARK-13339) | Improvement | Clarify commutative / associative operator requirements for reduce, fold |
| 2.0.0 | [SPARK-13357](https://issues.apache.org/jira/browse/SPARK-13357) | Improvement | Use generated projection and ordering for TakeOrderedAndProjectNode |
| 2.0.0 | [SPARK-13361](https://issues.apache.org/jira/browse/SPARK-13361) | Improvement | Add benchmark codes for Encoder#compress() in CompressionSchemeBenchmark |
| 2.0.0 | [SPARK-13386](https://issues.apache.org/jira/browse/SPARK-13386) | Improvement | ConnectedComponents should support maxIteration option |
| 2.0.0 | [SPARK-13404](https://issues.apache.org/jira/browse/SPARK-13404) | Improvement | Create the variables for input when it's used |
| 2.0.0 | [SPARK-13416](https://issues.apache.org/jira/browse/SPARK-13416) | Improvement | Add positive check for option 'numIter' in StronglyConnectedComponents |
| 2.0.0 | [SPARK-13423](https://issues.apache.org/jira/browse/SPARK-13423) | Improvement | Static analysis fixes for 2.x |
| 2.0.0 | [SPARK-13466](https://issues.apache.org/jira/browse/SPARK-13466) | Improvement | Remove redundant project in colum pruning rule |
| 2.0.0 | [SPARK-13527](https://issues.apache.org/jira/browse/SPARK-13527) | Improvement | Prune Filters based on Constraints |
| 2.0.0 | [SPARK-13538](https://issues.apache.org/jira/browse/SPARK-13538) | Improvement | Add GaussianMixture to ML |
| 2.0.0 | [SPARK-13617](https://issues.apache.org/jira/browse/SPARK-13617) | Improvement | remove unnecessary GroupingAnalytics trait |
| 2.0.0 | [SPARK-13637](https://issues.apache.org/jira/browse/SPARK-13637) | Improvement | use more information to simplify the code in Expand builder |
| 2.0.0 | [SPARK-13659](https://issues.apache.org/jira/browse/SPARK-13659) | Improvement | Remove returnValues from BlockStore APIs |
| 2.0.0 | [SPARK-13673](https://issues.apache.org/jira/browse/SPARK-13673) | Improvement | script bin\beeline.cmd pollutes environment variables in Windows. |
| 2.0.0 | [SPARK-13702](https://issues.apache.org/jira/browse/SPARK-13702) | Improvement | Use diamond operator for generic instance creation in Java code |
| 2.0.0 | [SPARK-13706](https://issues.apache.org/jira/browse/SPARK-13706) | Improvement | Python Example for Train Validation Split Missing |
| 2.0.0 | [SPARK-13715](https://issues.apache.org/jira/browse/SPARK-13715) | Improvement | Remove last usages of jblas in tests |
| 2.0.0 | [SPARK-13732](https://issues.apache.org/jira/browse/SPARK-13732) | Improvement | Remove projectList from Windows |
| 2.0.0 | [SPARK-13742](https://issues.apache.org/jira/browse/SPARK-13742) | Improvement | Add non-iterator interface to RandomSampler |
| 2.0.0 | [SPARK-13751](https://issues.apache.org/jira/browse/SPARK-13751) | Improvement | Generate better code for Filter |
| 2.0.0 | [SPARK-13761](https://issues.apache.org/jira/browse/SPARK-13761) | Improvement | Deprecate validateParams |
| 2.0.0 | [SPARK-13763](https://issues.apache.org/jira/browse/SPARK-13763) | Improvement | Remove Project when its projectList is Empty |
| 2.0.0 | [SPARK-13810](https://issues.apache.org/jira/browse/SPARK-13810) | Improvement | Add Port Configuration Suggestions on Bind Exceptions |
| 2.0.0 | [SPARK-13814](https://issues.apache.org/jira/browse/SPARK-13814) | Improvement | Delete unnecessary imports in python examples files |
| 2.0.0 | [SPARK-13823](https://issues.apache.org/jira/browse/SPARK-13823) | Improvement | Always specify Charset in String <-> byte[] conversions (and remaining Coverity items) |
| 2.0.0 | [SPARK-13838](https://issues.apache.org/jira/browse/SPARK-13838) | Improvement | Clear variable code to prevent it to be re-evaluated in BoundAttribute |
| 2.0.0 | [SPARK-13869](https://issues.apache.org/jira/browse/SPARK-13869) | Improvement | Remove redundant conditions while combining filters |
| 2.0.0 | [SPARK-13882](https://issues.apache.org/jira/browse/SPARK-13882) | Improvement | Remove org.apache.spark.sql.execution.local |
| 2.0.0 | [SPARK-13887](https://issues.apache.org/jira/browse/SPARK-13887) | Improvement | PyLint should fail fast to make errors easier to discover |
| 2.0.0 | [SPARK-13905](https://issues.apache.org/jira/browse/SPARK-13905) | Improvement | Change signature of as.data.frame() to be consistent with the R base package |
| 2.0.0 | [SPARK-13924](https://issues.apache.org/jira/browse/SPARK-13924) | Improvement | officially support multi-insert |
| 2.0.0 | [SPARK-13927](https://issues.apache.org/jira/browse/SPARK-13927) | New Feature | Add row/column iterator to local matrices |
| 2.0.0 | [SPARK-13928](https://issues.apache.org/jira/browse/SPARK-13928) | Improvement | Move org.apache.spark.Logging into org.apache.spark.internal.Logging |
| 2.0.0 | [SPARK-13973](https://issues.apache.org/jira/browse/SPARK-13973) | Improvement | `ipython notebook` is going away... |
| 2.0.0 | [SPARK-13981](https://issues.apache.org/jira/browse/SPARK-13981) | Improvement | Improve Filter generated code to defer variable evaluation within operator |
| 2.0.0 | [SPARK-13986](https://issues.apache.org/jira/browse/SPARK-13986) | Improvement | Remove `DeveloperApi`-annotation for non-publics |
| 2.0.0 | [SPARK-13988](https://issues.apache.org/jira/browse/SPARK-13988) | Improvement | Large history files block new applications from showing up in History UI. |
| 2.0.0 | [SPARK-14028](https://issues.apache.org/jira/browse/SPARK-14028) | Improvement | Remove deprecated methods; fix two other warnings |
| 2.0.0 | [SPARK-14038](https://issues.apache.org/jira/browse/SPARK-14038) | Improvement | Enable native view by default |
| 2.0.0 | [SPARK-14050](https://issues.apache.org/jira/browse/SPARK-14050) | Improvement | Add multiple languages support for Stop Words Remover |
| 2.0.0 | [SPARK-14058](https://issues.apache.org/jira/browse/SPARK-14058) | Improvement | Incorrect docstring in Window.orderBy |
| 2.0.0 | [SPARK-14072](https://issues.apache.org/jira/browse/SPARK-14072) | Improvement | Show JVM information when we run Benchmark |
| 2.0.0 | [SPARK-14089](https://issues.apache.org/jira/browse/SPARK-14089) | Improvement | Remove methods that has been deprecated since 1.1.x, 1.2.x and 1.3.x |
| 2.0.0 | [SPARK-14102](https://issues.apache.org/jira/browse/SPARK-14102) | Improvement | Block `reset` command in SparkShell |
| 2.0.0 | [SPARK-14104](https://issues.apache.org/jira/browse/SPARK-14104) | Improvement | All Python param setters should use the `_set` method. |
| 2.0.0 | [SPARK-14118](https://issues.apache.org/jira/browse/SPARK-14118) | Improvement | Implement DDL/DML commands for Spark 2.0 |
| 2.0.0 | [SPARK-14149](https://issues.apache.org/jira/browse/SPARK-14149) | Improvement | Log exceptions in tryOrIOException |
| 2.0.0 | [SPARK-14205](https://issues.apache.org/jira/browse/SPARK-14205) | Improvement | remove trait Queryable |
| 2.0.0 | [SPARK-14210](https://issues.apache.org/jira/browse/SPARK-14210) | Improvement | Add timing metric for how long the query spent in scan |
| 2.0.0 | [SPARK-14227](https://issues.apache.org/jira/browse/SPARK-14227) | Improvement | Add method for printing out generated code for debugging |
| 2.0.0 | [SPARK-14242](https://issues.apache.org/jira/browse/SPARK-14242) | Improvement | avoid too many copies in network when a network frame is large |
| 2.0.0 | [SPARK-14254](https://issues.apache.org/jira/browse/SPARK-14254) | Improvement | Add logs to help investigate the network performance |
| 2.0.0 | [SPARK-14277](https://issues.apache.org/jira/browse/SPARK-14277) | Improvement | Significant amount of CPU is being consumed in SnappyNative arrayCopy method |
| 2.0.0 | [SPARK-14279](https://issues.apache.org/jira/browse/SPARK-14279) | Improvement | Improve the spark build to pick the version information from the pom file and add git commit information |
| 2.0.0 | [SPARK-14304](https://issues.apache.org/jira/browse/SPARK-14304) | Improvement | Fix tests that don't create temp files in the `java.io.tmpdir` folder |
| 2.0.0 | [SPARK-14502](https://issues.apache.org/jira/browse/SPARK-14502) | Improvement | Add optimization for Binary Comparison Simplification |
| 2.0.0 | [SPARK-14547](https://issues.apache.org/jira/browse/SPARK-14547) | Improvement | Avoid DNS resolution for reusing connections |
| 2.0.0 | [SPARK-14581](https://issues.apache.org/jira/browse/SPARK-14581) | Improvement | Improve filter push down |
| 2.0.0 | [SPARK-14614](https://issues.apache.org/jira/browse/SPARK-14614) | Improvement | Add `bround` function |
| 2.0.0 | [SPARK-14633](https://issues.apache.org/jira/browse/SPARK-14633) | Improvement | Use more readable format to show memory bytes in Error Message |
| 2.0.0 | [SPARK-14639](https://issues.apache.org/jira/browse/SPARK-14639) | Improvement | Add `bround` function in Python/R. |
| 2.0.0 | [SPARK-14685](https://issues.apache.org/jira/browse/SPARK-14685) | Improvement | Properly document heritability of localProperties |
| 2.0.0 | [SPARK-14733](https://issues.apache.org/jira/browse/SPARK-14733) | Improvement | Allow custom timing control in microbenchmarks |
| 2.0.0 | [SPARK-14756](https://issues.apache.org/jira/browse/SPARK-14756) | Improvement | Use parseLong instead of valueOf |
| 2.0.0 | [SPARK-14758](https://issues.apache.org/jira/browse/SPARK-14758) | Improvement | Add checking for StepSize and Tol |
| 2.0.0 | [SPARK-14855](https://issues.apache.org/jira/browse/SPARK-14855) | Improvement | Add "Exec" suffix to all physical operators |
| 2.0.0 | [SPARK-14874](https://issues.apache.org/jira/browse/SPARK-14874) | Improvement | Remove the obsolete Batch representation |
| 2.0.0 | [SPARK-14882](https://issues.apache.org/jira/browse/SPARK-14882) | Improvement | Clarify that Spark can be cross-built for other Scala versions |
| 2.0.0 | [SPARK-14952](https://issues.apache.org/jira/browse/SPARK-14952) | Improvement | Remove methods that were deprecated in 1.6.0 |
| 2.0.0 | [SPARK-14996](https://issues.apache.org/jira/browse/SPARK-14996) | Improvement | Add TPCDS Benchmark Queries for SparkSQL |
| 2.0.0 | [SPARK-15023](https://issues.apache.org/jira/browse/SPARK-15023) | Improvement | Add support for testing against the `ProcessingTime(intervalMS > 0)` trigger and `ManualClock` |
| 2.0.0 | [SPARK-15104](https://issues.apache.org/jira/browse/SPARK-15104) | Improvement | Bad spacing in log line |
| 2.0.0 | [SPARK-15132](https://issues.apache.org/jira/browse/SPARK-15132) | Improvement | Debug log for generated code should be printed with proper indentation |
| 2.0.0 | [SPARK-15136](https://issues.apache.org/jira/browse/SPARK-15136) | Improvement | Linkify ML PyDoc |
| 2.0.0 | [SPARK-15152](https://issues.apache.org/jira/browse/SPARK-15152) | Improvement | Scaladoc and Code style Improvements |
| 2.0.0 | [SPARK-15158](https://issues.apache.org/jira/browse/SPARK-15158) | Improvement | Too aggressive logging in SizeBasedRollingPolicy? |
| 2.0.0 | [SPARK-15178](https://issues.apache.org/jira/browse/SPARK-15178) | Improvement | Remove LazyFileRegion |
| 2.0.0 | [SPARK-15197](https://issues.apache.org/jira/browse/SPARK-15197) | Improvement | Improve documentation for countApprox and related functions |
| 2.0.0 | [SPARK-15220](https://issues.apache.org/jira/browse/SPARK-15220) | Improvement | Add hyperlink to "running application" and "completed application" |
| 2.0.0 | [SPARK-15229](https://issues.apache.org/jira/browse/SPARK-15229) | Improvement | Make case sensitivity setting internal |
| 2.0.0 | [SPARK-15290](https://issues.apache.org/jira/browse/SPARK-15290) | Improvement | Move annotations, like @Since / @DeveloperApi, into spark-tags |
| 2.0.0 | [SPARK-15333](https://issues.apache.org/jira/browse/SPARK-15333) | Improvement | Reorganize building-spark.md; rationalize vs wiki |
| 2.0.0 | [SPARK-15363](https://issues.apache.org/jira/browse/SPARK-15363) | Improvement | Example code shouldn't use VectorImplicits._, asML/fromML |
| 2.0.0 | [SPARK-15398](https://issues.apache.org/jira/browse/SPARK-15398) | Improvement | Update the warning message to recommend ML usage |
| 2.0.0 | [SPARK-15413](https://issues.apache.org/jira/browse/SPARK-15413) | Improvement | Change `toBreeze` to `asBreeze` in Vector and Matrix |
| 2.0.0 | [SPARK-15416](https://issues.apache.org/jira/browse/SPARK-15416) | Improvement | Display a better message for not finding classes removed in Spark 2.0 |
| 2.0.0 | [SPARK-15494](https://issues.apache.org/jira/browse/SPARK-15494) | Improvement | encoder code cleanup |
| 2.0.0 | [SPARK-15498](https://issues.apache.org/jira/browse/SPARK-15498) | Improvement | fix slow tests |
| 2.0.0 | [SPARK-15537](https://issues.apache.org/jira/browse/SPARK-15537) | Improvement | clean up the temp folders after finishing the tests |
| 2.0.0 | [SPARK-15542](https://issues.apache.org/jira/browse/SPARK-15542) | Improvement | Make error message clear for script './R/install-dev.sh' when R is missing on Mac |
| 2.0.0 | [SPARK-15584](https://issues.apache.org/jira/browse/SPARK-15584) | Improvement | Abstract duplicate code: "spark.sql.sources." properties |
| 2.0.0 | [SPARK-15643](https://issues.apache.org/jira/browse/SPARK-15643) | Improvement | ML 2.0 QA: migration guide update |
| 2.0.0 | [SPARK-15645](https://issues.apache.org/jira/browse/SPARK-15645) | Improvement | Fix some typos of Streaming module |
| 2.0.0 | [SPARK-15681](https://issues.apache.org/jira/browse/SPARK-15681) | Improvement | Allow case-insensitiveness in sc.setLogLevel |
| 2.0.0 | [SPARK-15707](https://issues.apache.org/jira/browse/SPARK-15707) | Improvement | Make Code Neat - Use map instead of if check |
| 2.0.0 | [SPARK-15749](https://issues.apache.org/jira/browse/SPARK-15749) | Improvement | Make the error message more meaningful |
| 2.0.0 | [SPARK-15770](https://issues.apache.org/jira/browse/SPARK-15770) | Improvement | annotation audit for Experimental and DeveloperApi |
| 2.0.0 | [SPARK-15778](https://issues.apache.org/jira/browse/SPARK-15778) | Improvement | Add 2.0.0-preview to dropdown / reorg description of previews at spark.apache.org/downloads.html |
| 2.0.0 | [SPARK-15796](https://issues.apache.org/jira/browse/SPARK-15796) | Improvement | Reduce spark.memory.fraction default to avoid overrunning old gen in JVM default config |
| 2.0.0 | [SPARK-15813](https://issues.apache.org/jira/browse/SPARK-15813) | Improvement | Spark Dyn Allocation Cancel log message misleading |
| 2.0.0 | [SPARK-15821](https://issues.apache.org/jira/browse/SPARK-15821) | Improvement | Should we use mvn -T for multithreaded Spark builds? |
| 2.0.0 | [SPARK-15875](https://issues.apache.org/jira/browse/SPARK-15875) | Improvement | Avoid using Seq.length == 0 and Seq.lenth > 0. Use Seq.isEmpty and Seq.nonEmpty instead. |
| 2.0.0 | [SPARK-15887](https://issues.apache.org/jira/browse/SPARK-15887) | Improvement | Bring back the hive-site.xml support for Spark 2.0 |
| 2.0.0 | [SPARK-15942](https://issues.apache.org/jira/browse/SPARK-15942) | Improvement | Unblock `:reset` command in REPL. |
| 2.0.0 | [SPARK-16588](https://issues.apache.org/jira/browse/SPARK-16588) | Improvement | Deprecate monotonicallyIncreasingId in Scala |
| 2.0.1 | [SPARK-12370](https://issues.apache.org/jira/browse/SPARK-12370) | Improvement | Documentation should link to examples from its own release version |
| 2.0.1 | [SPARK-16320](https://issues.apache.org/jira/browse/SPARK-16320) | Improvement | Document G1 heap region's effect on spark 2.0 vs 1.6 |
| 2.0.1 | [SPARK-16650](https://issues.apache.org/jira/browse/SPARK-16650) | Improvement | Improve documentation of spark.task.maxFailures |
| 2.0.1 | [SPARK-16812](https://issues.apache.org/jira/browse/SPARK-16812) | Improvement | Open up SparkILoop.getAddedJars |
| 2.0.1 | [SPARK-16877](https://issues.apache.org/jira/browse/SPARK-16877) | Improvement | Add a rule for preventing use Java's Override annotation |
| 2.0.1 | [SPARK-17231](https://issues.apache.org/jira/browse/SPARK-17231) | Improvement | Avoid building debug or trace log messages unless the respective log level is enabled |
| 2.0.1 | [SPARK-17297](https://issues.apache.org/jira/browse/SPARK-17297) | Improvement | Clarify window/slide duration as absolute time, not relative to a calendar |
| 2.0.1 | [SPARK-17445](https://issues.apache.org/jira/browse/SPARK-17445) | Improvement | Reference an ASF page as the main place to find third-party packages |
| 2.0.1 | [SPARK-17456](https://issues.apache.org/jira/browse/SPARK-17456) | New Feature | Utility for parsing Spark versions |
| 2.0.1 | [SPARK-17651](https://issues.apache.org/jira/browse/SPARK-17651) | Improvement | Automate Spark version update for documentations |
| 2.1.0 | [SPARK-5992](https://issues.apache.org/jira/browse/SPARK-5992) | New Feature | Locality Sensitive Hashing (LSH) |
| 2.1.0 | [SPARK-9288](https://issues.apache.org/jira/browse/SPARK-9288) | Umbrella | Improve test speed |
| 2.1.0 | [SPARK-10541](https://issues.apache.org/jira/browse/SPARK-10541) | Improvement | Allow ApplicationHistoryProviders to provide their own text when there aren't any complete apps |
| 2.1.0 | [SPARK-11597](https://issues.apache.org/jira/browse/SPARK-11597) | Improvement | improve performance of array and map encoder |
| 2.1.0 | [SPARK-12370](https://issues.apache.org/jira/browse/SPARK-12370) | Improvement | Documentation should link to examples from its own release version |
| 2.1.0 | [SPARK-12920](https://issues.apache.org/jira/browse/SPARK-12920) | Improvement | Honor "spark.ui.retainedStages" to reduce mem-pressure |
| 2.1.0 | [SPARK-13238](https://issues.apache.org/jira/browse/SPARK-13238) | Improvement | Add ganglia dmax parameter |
| 2.1.0 | [SPARK-13770](https://issues.apache.org/jira/browse/SPARK-13770) | Improvement | Document the ML feature Interaction |
| 2.1.0 | [SPARK-14269](https://issues.apache.org/jira/browse/SPARK-14269) | Improvement | Eliminate unnecessary submitStage() call. |
| 2.1.0 | [SPARK-14702](https://issues.apache.org/jira/browse/SPARK-14702) | Improvement | Expose SparkLauncher's ProcessBuilder for user flexibility |
| 2.1.0 | [SPARK-15958](https://issues.apache.org/jira/browse/SPARK-15958) | Improvement | Make initial buffer size for the Sorter configurable |
| 2.1.0 | [SPARK-16021](https://issues.apache.org/jira/browse/SPARK-16021) | Improvement | Zero out freed memory in test to help catch correctness bugs |
| 2.1.0 | [SPARK-16272](https://issues.apache.org/jira/browse/SPARK-16272) | New Feature | Allow configs to reference other configs, env and system properties |
| 2.1.0 | [SPARK-16320](https://issues.apache.org/jira/browse/SPARK-16320) | Improvement | Document G1 heap region's effect on spark 2.0 vs 1.6 |
| 2.1.0 | [SPARK-16331](https://issues.apache.org/jira/browse/SPARK-16331) | Improvement | [SQL] Reduce code generation time |
| 2.1.0 | [SPARK-16398](https://issues.apache.org/jira/browse/SPARK-16398) | Improvement | Make cancelJob and cancelStage API public |
| 2.1.0 | [SPARK-16685](https://issues.apache.org/jira/browse/SPARK-16685) | Improvement | Remove defunct audit-release dir |
| 2.1.0 | [SPARK-16706](https://issues.apache.org/jira/browse/SPARK-16706) | New Feature | support java map in encoder |
| 2.1.0 | [SPARK-16812](https://issues.apache.org/jira/browse/SPARK-16812) | Improvement | Open up SparkILoop.getAddedJars |
| 2.1.0 | [SPARK-16822](https://issues.apache.org/jira/browse/SPARK-16822) | Improvement | Support latex in scaladoc with MathJax |
| 2.1.0 | [SPARK-16828](https://issues.apache.org/jira/browse/SPARK-16828) | Improvement | remove MaxOf and MinOf |
| 2.1.0 | [SPARK-16851](https://issues.apache.org/jira/browse/SPARK-16851) | Improvement | Incorrect threshould length in 'setThresholds()' evoke Exception |
| 2.1.0 | [SPARK-16855](https://issues.apache.org/jira/browse/SPARK-16855) | Improvement | move Greatest and Least from conditionalExpressions.scala to arithmetic.scala |
| 2.1.0 | [SPARK-16877](https://issues.apache.org/jira/browse/SPARK-16877) | Improvement | Add a rule for preventing use Java's Override annotation |
| 2.1.0 | [SPARK-16960](https://issues.apache.org/jira/browse/SPARK-16960) | Improvement | Deprecate approxCountDistinct, toDegrees and toRadians according to FunctionRegistry in Scala and Python |
| 2.1.0 | [SPARK-16962](https://issues.apache.org/jira/browse/SPARK-16962) | Improvement | Unsafe accesses (Platform.getLong()) not supported on unaligned boundaries in SPARC/Solaris |
| 2.1.0 | [SPARK-17068](https://issues.apache.org/jira/browse/SPARK-17068) | Improvement | Retain view visibility information through out Analysis |
| 2.1.0 | [SPARK-17095](https://issues.apache.org/jira/browse/SPARK-17095) | Improvement | Latex and Scala doc do not play nicely |
| 2.1.0 | [SPARK-17127](https://issues.apache.org/jira/browse/SPARK-17127) | Improvement | Include AArch64 in the check of cached unaligned-access capability |
| 2.1.0 | [SPARK-17231](https://issues.apache.org/jira/browse/SPARK-17231) | Improvement | Avoid building debug or trace log messages unless the respective log level is enabled |
| 2.1.0 | [SPARK-17297](https://issues.apache.org/jira/browse/SPARK-17297) | Improvement | Clarify window/slide duration as absolute time, not relative to a calendar |
| 2.1.0 | [SPARK-17308](https://issues.apache.org/jira/browse/SPARK-17308) | Improvement | Replace all pattern match on boolean value by if/else block. |
| 2.1.0 | [SPARK-17331](https://issues.apache.org/jira/browse/SPARK-17331) | Improvement | Avoid allocating 0-length arrays |
| 2.1.0 | [SPARK-17332](https://issues.apache.org/jira/browse/SPARK-17332) | Improvement | Make Java Loggers static members |
| 2.1.0 | [SPARK-17359](https://issues.apache.org/jira/browse/SPARK-17359) | Improvement | Use +=(A) instead of append(A) in performance critical paths |
| 2.1.0 | [SPARK-17445](https://issues.apache.org/jira/browse/SPARK-17445) | Improvement | Reference an ASF page as the main place to find third-party packages |
| 2.1.0 | [SPARK-17449](https://issues.apache.org/jira/browse/SPARK-17449) | Improvement | Relation between heartbeatInterval and network timeout |
| 2.1.0 | [SPARK-17456](https://issues.apache.org/jira/browse/SPARK-17456) | New Feature | Utility for parsing Spark versions |
| 2.1.0 | [SPARK-17506](https://issues.apache.org/jira/browse/SPARK-17506) | Improvement | Improve the check double values equality rule |
| 2.1.0 | [SPARK-17651](https://issues.apache.org/jira/browse/SPARK-17651) | Improvement | Automate Spark version update for documentations |
| 2.1.0 | [SPARK-17703](https://issues.apache.org/jira/browse/SPARK-17703) | Improvement | Add unnamed version of addReferenceObj for minor objects. |
| 2.1.0 | [SPARK-17829](https://issues.apache.org/jira/browse/SPARK-17829) | Improvement | Stable format for offset log |
| 2.1.0 | [SPARK-17854](https://issues.apache.org/jira/browse/SPARK-17854) | Improvement | rand(null) should be supported |
| 2.1.0 | [SPARK-18049](https://issues.apache.org/jira/browse/SPARK-18049) | Improvement | Add missing tests for truePositiveRate and weightedTruePositiveRate |
| 2.1.0 | [SPARK-18073](https://issues.apache.org/jira/browse/SPARK-18073) | Improvement | Migrate wiki to spark.apache.org web site |
| 2.1.0 | [SPARK-18126](https://issues.apache.org/jira/browse/SPARK-18126) | Improvement | getIteratorZipWithIndex accepts negative value as index. |
| 2.1.0 | [SPARK-18198](https://issues.apache.org/jira/browse/SPARK-18198) | Improvement | Highlight code snippets for Streaming integretion docs |
| 2.1.0 | [SPARK-18276](https://issues.apache.org/jira/browse/SPARK-18276) | Improvement | Some ML training summaries are not copied when {{copy()}} is called. |
| 2.1.0 | [SPARK-18329](https://issues.apache.org/jira/browse/SPARK-18329) | Umbrella | Spark R 2.1 QA umbrella |
| 2.1.0 | [SPARK-18420](https://issues.apache.org/jira/browse/SPARK-18420) | Improvement | Fix the errors caused by lint check in Java |
| 2.1.0 | [SPARK-18433](https://issues.apache.org/jira/browse/SPARK-18433) | Improvement | Improve DataSource option keys to be more case-insensitive |
| 2.1.0 | [SPARK-18481](https://issues.apache.org/jira/browse/SPARK-18481) | Improvement | ML 2.1 QA: Remove deprecated methods for ML |
| 2.1.0 | [SPARK-18590](https://issues.apache.org/jira/browse/SPARK-18590) | New Feature | R - Include package vignettes and help pages, build source package in Spark distribution |
| 2.1.0 | [SPARK-18628](https://issues.apache.org/jira/browse/SPARK-18628) | Improvement | Update handle invalid documentation string |
| 2.1.0 | [SPARK-18666](https://issues.apache.org/jira/browse/SPARK-18666) | Improvement | Remove the codes checking deprecated config spark.sql.unsafe.enabled |
| 2.1.0 | [SPARK-18690](https://issues.apache.org/jira/browse/SPARK-18690) | Improvement | Backward compatibility of unbounded frames |
| 2.1.0 | [SPARK-18764](https://issues.apache.org/jira/browse/SPARK-18764) | Improvement | Add a warning log when skipping a corrupted file |
| 2.1.0 | [SPARK-18774](https://issues.apache.org/jira/browse/SPARK-18774) | Improvement | Ignore non-existing files when ignoreCorruptFiles is enabled |
| 2.1.0 | [SPARK-18790](https://issues.apache.org/jira/browse/SPARK-18790) | Improvement | Keep a general offset history of stream batches |
| 2.1.0 | [SPARK-18826](https://issues.apache.org/jira/browse/SPARK-18826) | Improvement | Make FileStream be able to start with most recent files |
| 2.2.0 | [SPARK-8617](https://issues.apache.org/jira/browse/SPARK-8617) | Improvement | Handle history files better |
| 2.2.0 | [SPARK-15214](https://issues.apache.org/jira/browse/SPARK-15214) | Improvement | Implement code generation for Generate |
| 2.2.0 | [SPARK-15352](https://issues.apache.org/jira/browse/SPARK-15352) | New Feature | Topology aware block replication |
| 2.2.0 | [SPARK-16043](https://issues.apache.org/jira/browse/SPARK-16043) | Improvement | Prepare GenericArrayData implementation specialized for a primitive array |
| 2.2.0 | [SPARK-16920](https://issues.apache.org/jira/browse/SPARK-16920) | Improvement | Investigate and fix issues introduced in SPARK-15858 |
| 2.2.0 | [SPARK-17471](https://issues.apache.org/jira/browse/SPARK-17471) | New Feature | Add compressed method for Matrix class |
| 2.2.0 | [SPARK-17564](https://issues.apache.org/jira/browse/SPARK-17564) | Improvement | Flaky RequestTimeoutIntegrationSuite, furtherRequestsDelay |
| 2.2.0 | [SPARK-18127](https://issues.apache.org/jira/browse/SPARK-18127) | New Feature | Add hooks and extension points to Spark |
| 2.2.0 | [SPARK-18352](https://issues.apache.org/jira/browse/SPARK-18352) | New Feature | Parse normal, multi-line JSON files (not just JSON Lines) |
| 2.2.0 | [SPARK-18379](https://issues.apache.org/jira/browse/SPARK-18379) | Improvement | Make the parallelism of parallelPartitionDiscovery configurable. |
| 2.2.0 | [SPARK-18719](https://issues.apache.org/jira/browse/SPARK-18719) | Improvement | Document spark.ui.showConsoleProgress |
| 2.2.0 | [SPARK-18720](https://issues.apache.org/jira/browse/SPARK-18720) | Improvement | Code Refactoring of withColumn |
| 2.2.0 | [SPARK-18923](https://issues.apache.org/jira/browse/SPARK-18923) | Improvement | Support SKIP_PYTHONDOC/RDOC in doc generation |
| 2.2.0 | [SPARK-18953](https://issues.apache.org/jira/browse/SPARK-18953) | Improvement | Do not show the link to a dead worker on the master page |
| 2.2.0 | [SPARK-18960](https://issues.apache.org/jira/browse/SPARK-18960) | Improvement | Avoid double reading file which is being copied. |
| 2.2.0 | [SPARK-19002](https://issues.apache.org/jira/browse/SPARK-19002) | Improvement | Check pep8 against all the python scripts |
| 2.2.0 | [SPARK-19054](https://issues.apache.org/jira/browse/SPARK-19054) | Improvement | Eliminate extra pass in NB |
| 2.2.0 | [SPARK-19080](https://issues.apache.org/jira/browse/SPARK-19080) | Improvement | simplify data source analysis |
| 2.2.0 | [SPARK-19146](https://issues.apache.org/jira/browse/SPARK-19146) | Improvement | Drop more elements when stageData.taskData.size > retainedTasks to reduce the number of times on call drop |
| 2.2.0 | [SPARK-19183](https://issues.apache.org/jira/browse/SPARK-19183) | Improvement | Add deleteWithJob hook to internal commit protocol API |
| 2.2.0 | [SPARK-19227](https://issues.apache.org/jira/browse/SPARK-19227) | Improvement | Typo in `org.apache.spark.internal.config.ConfigEntry` |
| 2.2.0 | [SPARK-19236](https://issues.apache.org/jira/browse/SPARK-19236) | Improvement | Add createOrReplaceGlobalTempView |
| 2.2.0 | [SPARK-19249](https://issues.apache.org/jira/browse/SPARK-19249) | Improvement | Update Download page to describe how to download archived releases |
| 2.2.0 | [SPARK-19251](https://issues.apache.org/jira/browse/SPARK-19251) | Improvement | remove unused imports and outdated comments |
| 2.2.0 | [SPARK-19254](https://issues.apache.org/jira/browse/SPARK-19254) | Improvement | Support Seq, Map, and Struct in functions.lit |
| 2.2.0 | [SPARK-19291](https://issues.apache.org/jira/browse/SPARK-19291) | Improvement | spark.gaussianMixture supports output log-likelihood |
| 2.2.0 | [SPARK-19295](https://issues.apache.org/jira/browse/SPARK-19295) | Improvement | IsolatedClientLoader's downloadVersion should log the location of downloaded metastore client jars |
| 2.2.0 | [SPARK-19330](https://issues.apache.org/jira/browse/SPARK-19330) | Improvement | Also show tooltip for successful batches |
| 2.2.0 | [SPARK-19333](https://issues.apache.org/jira/browse/SPARK-19333) | Improvement | Files out of compliance with ASF policy |
| 2.2.0 | [SPARK-19377](https://issues.apache.org/jira/browse/SPARK-19377) | Improvement | Killed tasks should have the status as KILLED |
| 2.2.0 | [SPARK-19385](https://issues.apache.org/jira/browse/SPARK-19385) | Improvement | During canonicalization, `NOT(l, r)` should not expect such cases that l.hashcode > r.hashcode |
| 2.2.0 | [SPARK-19421](https://issues.apache.org/jira/browse/SPARK-19421) | Improvement | Remove numClasses and numFeatures methods in LinearSVC |
| 2.2.0 | [SPARK-19436](https://issues.apache.org/jira/browse/SPARK-19436) | Improvement | Add missing tests for approxQuantiles |
| 2.2.0 | [SPARK-19450](https://issues.apache.org/jira/browse/SPARK-19450) | Improvement | Replace askWithRetry with askSync. |
| 2.2.0 | [SPARK-19499](https://issues.apache.org/jira/browse/SPARK-19499) | Improvement | Add more notes in the comments of Sink.addBatch() |
| 2.2.0 | [SPARK-19508](https://issues.apache.org/jira/browse/SPARK-19508) | Improvement | Improve error message when binding service fails |
| 2.2.0 | [SPARK-19549](https://issues.apache.org/jira/browse/SPARK-19549) | New Feature | Allow providing reasons for stage/job cancelling |
| 2.2.0 | [SPARK-19555](https://issues.apache.org/jira/browse/SPARK-19555) | Improvement | Improve inefficient StringUtils.escapeLikeRegex() method |
| 2.2.0 | [SPARK-19562](https://issues.apache.org/jira/browse/SPARK-19562) | Improvement | Gitignore Misses Folder dev/pr-deps |
| 2.2.0 | [SPARK-19563](https://issues.apache.org/jira/browse/SPARK-19563) | Improvement | advoid unnecessary sort in FileFormatWriter |
| 2.2.0 | [SPARK-19567](https://issues.apache.org/jira/browse/SPARK-19567) | Improvement | Support some Schedulable variables immutability and access |
| 2.2.0 | [SPARK-19598](https://issues.apache.org/jira/browse/SPARK-19598) | Improvement | Remove the alias parameter in UnresolvedRelation |
| 2.2.0 | [SPARK-19633](https://issues.apache.org/jira/browse/SPARK-19633) | New Feature | FileSource read from FileSink |
| 2.2.0 | [SPARK-19682](https://issues.apache.org/jira/browse/SPARK-19682) | Improvement | Issue warning (or error) when subset method "[[" takes vector index |
| 2.2.0 | [SPARK-19684](https://issues.apache.org/jira/browse/SPARK-19684) | Improvement | Move info about running specific tests to developer website |
| 2.2.0 | [SPARK-19715](https://issues.apache.org/jira/browse/SPARK-19715) | New Feature | Option to Strip Paths in FileSource |
| 2.2.0 | [SPARK-19749](https://issues.apache.org/jira/browse/SPARK-19749) | Improvement | Name socket source with a meaningful name |
| 2.2.0 | [SPARK-19786](https://issues.apache.org/jira/browse/SPARK-19786) | Improvement | Facilitate loop optimizations in a JIT compiler regarding range() |
| 2.2.0 | [SPARK-19820](https://issues.apache.org/jira/browse/SPARK-19820) | Improvement | Allow users to kill tasks, and propagate a kill reason |
| 2.2.0 | [SPARK-19831](https://issues.apache.org/jira/browse/SPARK-19831) | Improvement | Sending the heartbeat master from worker maybe blocked by other rpc messages |
| 2.2.0 | [SPARK-19846](https://issues.apache.org/jira/browse/SPARK-19846) | Improvement | Add a flag to disable constraint propagation |
| 2.2.0 | [SPARK-19904](https://issues.apache.org/jira/browse/SPARK-19904) | Improvement | SPIP Add Spark Project Improvement Proposal doc to website |
| 2.2.0 | [SPARK-19916](https://issues.apache.org/jira/browse/SPARK-19916) | Improvement | simplify bad file handling |
| 2.2.0 | [SPARK-19918](https://issues.apache.org/jira/browse/SPARK-19918) | Improvement | Use TextFileFormat in implementation of JsonFileFormat |
| 2.2.0 | [SPARK-19956](https://issues.apache.org/jira/browse/SPARK-19956) | Improvement | Optimize a location order of blocks with topology information |
| 2.2.0 | [SPARK-19961](https://issues.apache.org/jira/browse/SPARK-19961) | Improvement | unify a exception erro msg for dropdatabase |
| 2.2.0 | [SPARK-19987](https://issues.apache.org/jira/browse/SPARK-19987) | Improvement | Pass all filters into FileIndex |
| 2.2.0 | [SPARK-19991](https://issues.apache.org/jira/browse/SPARK-19991) | Improvement | FileSegmentManagedBuffer performance improvement. |
| 2.2.0 | [SPARK-20038](https://issues.apache.org/jira/browse/SPARK-20038) | Improvement | FileFormatWriter.ExecuteWriteTask.releaseResources() implementations to be re-entrant |
| 2.2.0 | [SPARK-20041](https://issues.apache.org/jira/browse/SPARK-20041) | Improvement | Update docs for NaN handling in approxQuantile |
| 2.2.0 | [SPARK-20097](https://issues.apache.org/jira/browse/SPARK-20097) | Improvement | Fix visibility discrepancy with numInstances and degreesOfFreedom in LR and GLR |
| 2.2.0 | [SPARK-20120](https://issues.apache.org/jira/browse/SPARK-20120) | Improvement | spark-sql CLI support silent mode |
| 2.2.0 | [SPARK-20121](https://issues.apache.org/jira/browse/SPARK-20121) | Improvement | simplify NullPropagation with NullIntolerant |
| 2.2.0 | [SPARK-20127](https://issues.apache.org/jira/browse/SPARK-20127) | Improvement | Minor code cleanup |
| 2.2.0 | [SPARK-20177](https://issues.apache.org/jira/browse/SPARK-20177) | Improvement | Document about compression way has some little detail changes. |
| 2.2.0 | [SPARK-20232](https://issues.apache.org/jira/browse/SPARK-20232) | Improvement | Better combineByKey documentation: clarify memory allocation, better example |
| 2.2.0 | [SPARK-20253](https://issues.apache.org/jira/browse/SPARK-20253) | Improvement | Remove unnecessary nullchecks of a return value from Spark runtime routines in generated Java code |
| 2.2.0 | [SPARK-20255](https://issues.apache.org/jira/browse/SPARK-20255) | Improvement | FileIndex hierarchy inconsistency |
| 2.2.0 | [SPARK-20265](https://issues.apache.org/jira/browse/SPARK-20265) | Improvement | Improve Prefix'span pre-processing efficiency |
| 2.2.0 | [SPARK-20283](https://issues.apache.org/jira/browse/SPARK-20283) | New Feature | Add preOptimizationBatches |
| 2.2.0 | [SPARK-20304](https://issues.apache.org/jira/browse/SPARK-20304) | Improvement | AssertNotNull should not include path in string representation |
| 2.2.0 | [SPARK-20316](https://issues.apache.org/jira/browse/SPARK-20316) | Improvement | In SparkSQLCLIDriver, val and var should strictly follow the Scala syntax |
| 2.2.0 | [SPARK-20400](https://issues.apache.org/jira/browse/SPARK-20400) | Improvement | Remove References to Third Party Vendors from Spark ASF Documentation |
| 2.2.0 | [SPARK-20401](https://issues.apache.org/jira/browse/SPARK-20401) | Improvement | In the spark official configuration document, the 'spark.driver.supervise' configuration parameter specification and default values are necessary. |
| 2.2.0 | [SPARK-20423](https://issues.apache.org/jira/browse/SPARK-20423) | Improvement | fix MLOR coeffs centering when reg == 0 |
| 2.2.0 | [SPARK-20426](https://issues.apache.org/jira/browse/SPARK-20426) | Improvement | OneForOneStreamManager occupies too much memory. |
| 2.2.0 | [SPARK-20465](https://issues.apache.org/jira/browse/SPARK-20465) | Improvement | Throws a proper exception rather than ArrayIndexOutOfBoundsException when temp directories could not be got/created |
| 2.2.0 | [SPARK-20508](https://issues.apache.org/jira/browse/SPARK-20508) | Umbrella | Spark R 2.2 QA umbrella |
| 2.2.0 | [SPARK-20523](https://issues.apache.org/jira/browse/SPARK-20523) | Improvement | Clean up build warnings for 2.2.0 release |
| 2.2.0 | [SPARK-20588](https://issues.apache.org/jira/browse/SPARK-20588) | Improvement | from_utc_timestamp causes bottleneck |
| 2.2.0 | [SPARK-20606](https://issues.apache.org/jira/browse/SPARK-20606) | Improvement | ML 2.2 QA: Remove deprecated methods for ML |
| 2.2.0 | [SPARK-20621](https://issues.apache.org/jira/browse/SPARK-20621) | Improvement | Delete deprecated config parameter in 'spark-env.sh' |
| 2.2.0 | [SPARK-20674](https://issues.apache.org/jira/browse/SPARK-20674) | Improvement | Support registering UserDefinedFunction as named UDF |
| 2.2.0 | [SPARK-20707](https://issues.apache.org/jira/browse/SPARK-20707) | Improvement | ML deprecated APIs should be removed in major release. |
| 2.2.0 | [SPARK-20764](https://issues.apache.org/jira/browse/SPARK-20764) | Improvement | Fix visibility discrepancy with numInstances and degreesOfFreedom in LR and GLR - Python version |
| 2.2.0 | [SPARK-20907](https://issues.apache.org/jira/browse/SPARK-20907) | Improvement | Use testQuietly for test suites that generate long log output |
| 2.2.0 | [SPARK-20979](https://issues.apache.org/jira/browse/SPARK-20979) | New Feature | Add a rate source to generate values for tests and benchmark |
| 2.2.0 | [SPARK-21210](https://issues.apache.org/jira/browse/SPARK-21210) | Improvement | Javadoc 8 fixes for ML shared param traits |
| 3.0.0 | [SPARK-13677](https://issues.apache.org/jira/browse/SPARK-13677) | New Feature | Support Tree-Based Feature Transformation for ML |
| 3.0.0 | [SPARK-20351](https://issues.apache.org/jira/browse/SPARK-20351) | Improvement | Add trait hasTrainingSummary to replace the duplicate code |
| 3.0.0 | [SPARK-23182](https://issues.apache.org/jira/browse/SPARK-23182) | Improvement | Allow enabling of TCP keep alive for RPC connections |
| 3.0.0 | [SPARK-23472](https://issues.apache.org/jira/browse/SPARK-23472) | Improvement | Add config properties for administrator JVM options |
| 3.0.0 | [SPARK-24109](https://issues.apache.org/jira/browse/SPARK-24109) | Improvement | Remove class SnappyOutputStreamWrapper |
| 3.0.0 | [SPARK-24243](https://issues.apache.org/jira/browse/SPARK-24243) | Improvement | Expose exceptions from InProcessAppHandle |
| 3.0.0 | [SPARK-24417](https://issues.apache.org/jira/browse/SPARK-24417) | New Feature | Build and Run Spark on JDK11 |
| 3.0.0 | [SPARK-24544](https://issues.apache.org/jira/browse/SPARK-24544) | Improvement | Print actual failure cause when look up function failed |
| 3.0.0 | [SPARK-24625](https://issues.apache.org/jira/browse/SPARK-24625) | Improvement | put all the backward compatible behavior change configs under spark.sql.legacy.* |
| 3.0.0 | [SPARK-24898](https://issues.apache.org/jira/browse/SPARK-24898) | Improvement | Adding spark.checkpoint.compress to the docs |
| 3.0.0 | [SPARK-24902](https://issues.apache.org/jira/browse/SPARK-24902) | Improvement | Add integration tests for PVs |
| 3.0.0 | [SPARK-24933](https://issues.apache.org/jira/browse/SPARK-24933) | Improvement | SinkProgress should report written rows |
| 3.0.0 | [SPARK-25035](https://issues.apache.org/jira/browse/SPARK-25035) | Improvement | Replicating disk-stored blocks should avoid memory mapping |
| 3.0.0 | [SPARK-25338](https://issues.apache.org/jira/browse/SPARK-25338) | Improvement | Several tests miss calling super.afterAll() in their afterAll() method |
| 3.0.0 | [SPARK-25348](https://issues.apache.org/jira/browse/SPARK-25348) | Story | Data source for binary files |
| 3.0.0 | [SPARK-25426](https://issues.apache.org/jira/browse/SPARK-25426) | Improvement | Remove the duplicate fallback logic in UnsafeProjection |
| 3.0.0 | [SPARK-25475](https://issues.apache.org/jira/browse/SPARK-25475) | Improvement | Refactor all benchmark to save the result as a separate file |
| 3.0.0 | [SPARK-25515](https://issues.apache.org/jira/browse/SPARK-25515) | Improvement | Add a config property for disabling auto deletion of PODS for debugging. |
| 3.0.0 | [SPARK-25539](https://issues.apache.org/jira/browse/SPARK-25539) | Improvement | Update lz4-java to get speed improvement |
| 3.0.0 | [SPARK-25565](https://issues.apache.org/jira/browse/SPARK-25565) | Improvement | Add scala style checker to check add Locale.ROOT to .toLowerCase and .toUpperCase for internal calls |
| 3.0.0 | [SPARK-25581](https://issues.apache.org/jira/browse/SPARK-25581) | Improvement | Rename method `benchmark` in BenchmarkBase as benchmarkSuite |
| 3.0.0 | [SPARK-25584](https://issues.apache.org/jira/browse/SPARK-25584) | Story | Document libsvm data source in doc site |
| 3.0.0 | [SPARK-25589](https://issues.apache.org/jira/browse/SPARK-25589) | New Feature | Add BloomFilterBenchmark |
| 3.0.0 | [SPARK-25683](https://issues.apache.org/jira/browse/SPARK-25683) | Improvement | Updated the log for the firstTime event Drop occurs. |
| 3.0.0 | [SPARK-25712](https://issues.apache.org/jira/browse/SPARK-25712) | Improvement | Improve usage message of start-master.sh and start-slave.sh |
| 3.0.0 | [SPARK-25760](https://issues.apache.org/jira/browse/SPARK-25760) | Improvement | Set AddJarCommand return empty |
| 3.0.0 | [SPARK-25856](https://issues.apache.org/jira/browse/SPARK-25856) | Improvement | Remove AverageLike and CountLike classes. |
| 3.0.0 | [SPARK-25860](https://issues.apache.org/jira/browse/SPARK-25860) | Improvement | Replace Literal(null, _) with FalseLiteral whenever possible |
| 3.0.0 | [SPARK-25861](https://issues.apache.org/jira/browse/SPARK-25861) | Improvement | Remove unused refreshInterval parameter from the headerSparkPage method. |
| 3.0.0 | [SPARK-25877](https://issues.apache.org/jira/browse/SPARK-25877) | Improvement | Put all feature-related code in the feature step itself |
| 3.0.0 | [SPARK-25904](https://issues.apache.org/jira/browse/SPARK-25904) | Improvement | Avoid allocating arrays too large for JVMs |
| 3.0.0 | [SPARK-25926](https://issues.apache.org/jira/browse/SPARK-25926) | Improvement | Move config entries in core module to internal.config. |
| 3.0.0 | [SPARK-25972](https://issues.apache.org/jira/browse/SPARK-25972) | Improvement | Missed JSON options in streaming.py |
| 3.0.0 | [SPARK-25973](https://issues.apache.org/jira/browse/SPARK-25973) | Improvement | Spark History Main page performance improvement |
| 3.0.0 | [SPARK-25974](https://issues.apache.org/jira/browse/SPARK-25974) | Improvement | Optimizes Generates bytecode for ordering based on the given order |
| 3.0.0 | [SPARK-25986](https://issues.apache.org/jira/browse/SPARK-25986) | Improvement | Banning throw new Errors |
| 3.0.0 | [SPARK-26014](https://issues.apache.org/jira/browse/SPARK-26014) | Improvement | Deprecate R < 3.4 support |
| 3.0.0 | [SPARK-26016](https://issues.apache.org/jira/browse/SPARK-26016) | Improvement | Document that UTF-8 is required in text data source |
| 3.0.0 | [SPARK-26055](https://issues.apache.org/jira/browse/SPARK-26055) | Improvement | InterfaceStability annotations should be retained at runtime |
| 3.0.0 | [SPARK-26073](https://issues.apache.org/jira/browse/SPARK-26073) | Improvement | remove invalid comment as we don't use it anymore |
| 3.0.0 | [SPARK-26076](https://issues.apache.org/jira/browse/SPARK-26076) | Improvement | Revise ambiguous error message from load-spark-env.sh |
| 3.0.0 | [SPARK-26090](https://issues.apache.org/jira/browse/SPARK-26090) | Improvement | Resolve most miscellaneous deprecation and build warnings for Spark 3 |
| 3.0.0 | [SPARK-26117](https://issues.apache.org/jira/browse/SPARK-26117) | Improvement | use SparkOutOfMemoryError instead of OutOfMemoryError when catch exception |
| 3.0.0 | [SPARK-26124](https://issues.apache.org/jira/browse/SPARK-26124) | Improvement | Update plugins, including MiMa |
| 3.0.0 | [SPARK-26161](https://issues.apache.org/jira/browse/SPARK-26161) | Improvement | Ignore empty files in load |
| 3.0.0 | [SPARK-26177](https://issues.apache.org/jira/browse/SPARK-26177) | Improvement | Automated formatting for Scala code |
| 3.0.0 | [SPARK-26294](https://issues.apache.org/jira/browse/SPARK-26294) | Improvement | Delete Unnecessary If statement |
| 3.0.0 | [SPARK-26297](https://issues.apache.org/jira/browse/SPARK-26297) | Improvement | improve the doc of Distribution/Partitioning |
| 3.0.0 | [SPARK-26300](https://issues.apache.org/jira/browse/SPARK-26300) | Improvement | The `checkForStreaming` mothod may be called twice in `createQuery` |
| 3.0.0 | [SPARK-26303](https://issues.apache.org/jira/browse/SPARK-26303) | Improvement | Return partial results for bad JSON records |
| 3.0.0 | [SPARK-26318](https://issues.apache.org/jira/browse/SPARK-26318) | Improvement | Deprecate function merge in Row |
| 3.0.0 | [SPARK-26319](https://issues.apache.org/jira/browse/SPARK-26319) | Improvement | Add appendReadColumns Unit Test for HiveShimSuite |
| 3.0.0 | [SPARK-26360](https://issues.apache.org/jira/browse/SPARK-26360) | Improvement | Avoid extra validateQuery call in createStreamingWriteSupport |
| 3.0.0 | [SPARK-26362](https://issues.apache.org/jira/browse/SPARK-26362) | Improvement | Remove 'spark.driver.allowMultipleContexts' to disallow multiple Spark contexts |
| 3.0.0 | [SPARK-26392](https://issues.apache.org/jira/browse/SPARK-26392) | Improvement | Cancel pending allocate requests by taking locality preference into account |
| 3.0.0 | [SPARK-26428](https://issues.apache.org/jira/browse/SPARK-26428) | Improvement | Minimize deprecated `ProcessingTime` usage |
| 3.0.0 | [SPARK-26448](https://issues.apache.org/jira/browse/SPARK-26448) | Improvement | retain the difference between 0.0 and -0.0 |
| 3.0.0 | [SPARK-26459](https://issues.apache.org/jira/browse/SPARK-26459) | Improvement | remove UpdateNullabilityInAttributeReferences |
| 3.0.0 | [SPARK-26493](https://issues.apache.org/jira/browse/SPARK-26493) | Improvement | spark.sql.extensions should support multiple extensions |
| 3.0.0 | [SPARK-26495](https://issues.apache.org/jira/browse/SPARK-26495) | Improvement | Simplify SelectedField extractor |
| 3.0.0 | [SPARK-26504](https://issues.apache.org/jira/browse/SPARK-26504) | Improvement | Rope-wise dumping of Spark plans |
| 3.0.0 | [SPARK-26529](https://issues.apache.org/jira/browse/SPARK-26529) | Improvement | Add debug logs for confArchive when preparing local resource |
| 3.0.0 | [SPARK-26530](https://issues.apache.org/jira/browse/SPARK-26530) | Improvement | Validate heartheat arguments in HeartbeatReceiver |
| 3.0.0 | [SPARK-26547](https://issues.apache.org/jira/browse/SPARK-26547) | Improvement | Remove duplicate toHiveString from HiveUtils |
| 3.0.0 | [SPARK-26548](https://issues.apache.org/jira/browse/SPARK-26548) | Improvement | Don't block during query optimization |
| 3.0.0 | [SPARK-26584](https://issues.apache.org/jira/browse/SPARK-26584) | Improvement | Remove `spark.sql.orc.copyBatchToSpark` internal configuration |
| 3.0.0 | [SPARK-26604](https://issues.apache.org/jira/browse/SPARK-26604) | Improvement | Register channel for stream request |
| 3.0.0 | [SPARK-26616](https://issues.apache.org/jira/browse/SPARK-26616) | Improvement | Expose document frequency in IDFModel |
| 3.0.0 | [SPARK-26637](https://issues.apache.org/jira/browse/SPARK-26637) | Improvement | Makes GetArrayItem nullability more precise |
| 3.0.0 | [SPARK-26660](https://issues.apache.org/jira/browse/SPARK-26660) | Improvement | Add warning logs for large taskBinary size |
| 3.0.0 | [SPARK-26674](https://issues.apache.org/jira/browse/SPARK-26674) | Improvement | Consolidate CompositeByteBuf when reading large frame |
| 3.0.0 | [SPARK-26681](https://issues.apache.org/jira/browse/SPARK-26681) | Improvement | Support Ammonite scopes in OuterScopes |
| 3.0.0 | [SPARK-26685](https://issues.apache.org/jira/browse/SPARK-26685) | Improvement | Building Spark Images with latest Docker does not honour spark_uid build argument |
| 3.0.0 | [SPARK-26700](https://issues.apache.org/jira/browse/SPARK-26700) | Improvement | enable fetch-big-block-to-memory by default |
| 3.0.0 | [SPARK-26733](https://issues.apache.org/jira/browse/SPARK-26733) | Improvement | Clean up entrypoint.sh |
| 3.0.0 | [SPARK-26747](https://issues.apache.org/jira/browse/SPARK-26747) | Improvement | Makes GetMapValue nullability more precise |
| 3.0.0 | [SPARK-26798](https://issues.apache.org/jira/browse/SPARK-26798) | Improvement | HandleNullInputsForUDF should trust nullability |
| 3.0.0 | [SPARK-26813](https://issues.apache.org/jira/browse/SPARK-26813) | Improvement | Consolidate java version across language compilers and build tools |
| 3.0.0 | [SPARK-26817](https://issues.apache.org/jira/browse/SPARK-26817) | Improvement | Use System.nanoTime to measure time intervals |
| 3.0.0 | [SPARK-26856](https://issues.apache.org/jira/browse/SPARK-26856) | Improvement | Python support for "from_avro" and "to_avro" APIs |
| 3.0.0 | [SPARK-26882](https://issues.apache.org/jira/browse/SPARK-26882) | Improvement | lint-scala script does not check all components |
| 3.0.0 | [SPARK-26900](https://issues.apache.org/jira/browse/SPARK-26900) | Improvement | Simplify truncation to quarter of year |
| 3.0.0 | [SPARK-26908](https://issues.apache.org/jira/browse/SPARK-26908) | Improvement | Fix toMillis |
| 3.0.0 | [SPARK-26952](https://issues.apache.org/jira/browse/SPARK-26952) | Improvement | Row count statics should respect the data reported by data source |
| 3.0.0 | [SPARK-26960](https://issues.apache.org/jira/browse/SPARK-26960) | Improvement | Reduce flakiness of Spark ML Listener test suite by waiting for listener bus to clear |
| 3.0.0 | [SPARK-26965](https://issues.apache.org/jira/browse/SPARK-26965) | Improvement | Makes ElementAt nullability more precise |
| 3.0.0 | [SPARK-26978](https://issues.apache.org/jira/browse/SPARK-26978) | Improvement | Avoid magic time constants |
| 3.0.0 | [SPARK-26982](https://issues.apache.org/jira/browse/SPARK-26982) | Improvement | Enhance describe framework to describe the output of a query |
| 3.0.0 | [SPARK-27005](https://issues.apache.org/jira/browse/SPARK-27005) | Story | Design sketch for SPIP discussion: Accelerator-aware scheduling |
| 3.0.0 | [SPARK-27009](https://issues.apache.org/jira/browse/SPARK-27009) | Improvement | Add standard deviation to Benchmark tests |
| 3.0.0 | [SPARK-27035](https://issues.apache.org/jira/browse/SPARK-27035) | Improvement | Current time with microsecond resolution |
| 3.0.0 | [SPARK-27046](https://issues.apache.org/jira/browse/SPARK-27046) | Improvement | Remove SPARK-19185 related references from documentation since its resolved |
| 3.0.0 | [SPARK-27079](https://issues.apache.org/jira/browse/SPARK-27079) | Improvement | Fix typo & Remove useless imports |
| 3.0.0 | [SPARK-27102](https://issues.apache.org/jira/browse/SPARK-27102) | Improvement | Remove the references to Python's Scala codes in R's Scala codes |
| 3.0.0 | [SPARK-27151](https://issues.apache.org/jira/browse/SPARK-27151) | Improvement | ClearCacheCommand extends IgnoreCahedData to avoid plan node copys |
| 3.0.0 | [SPARK-27184](https://issues.apache.org/jira/browse/SPARK-27184) | Improvement | Replace "spark.jars" & "spark.files" with the variables of JARS & FILES in config object |
| 3.0.0 | [SPARK-27193](https://issues.apache.org/jira/browse/SPARK-27193) | Improvement | CodeFormatter should format multi comment lines correctly |
| 3.0.0 | [SPARK-27202](https://issues.apache.org/jira/browse/SPARK-27202) | Improvement | update comments to keep according with code |
| 3.0.0 | [SPARK-27209](https://issues.apache.org/jira/browse/SPARK-27209) | Improvement | Split parsing of SELECT and INSERT into two top-level rules in the grammar file. |
| 3.0.0 | [SPARK-27219](https://issues.apache.org/jira/browse/SPARK-27219) | Improvement | Misleading exceptions in transport code's SASL fallback path |
| 3.0.0 | [SPARK-27222](https://issues.apache.org/jira/browse/SPARK-27222) | Improvement | Support Instant and LocalDate in Literal.apply |
| 3.0.0 | [SPARK-27236](https://issues.apache.org/jira/browse/SPARK-27236) | Improvement | Refactor log-appender pattern in tests |
| 3.0.0 | [SPARK-27242](https://issues.apache.org/jira/browse/SPARK-27242) | Improvement | Avoid using default time zone in formatting TIMESTAMP/DATE literals |
| 3.0.0 | [SPARK-27256](https://issues.apache.org/jira/browse/SPARK-27256) | Improvement | If the configuration is used to set the number of bytes, we'd better use `bytesConf`'. |
| 3.0.0 | [SPARK-27262](https://issues.apache.org/jira/browse/SPARK-27262) | Improvement | Add explicit UTF-8 Encoding to DESCRIPTION |
| 3.0.0 | [SPARK-27277](https://issues.apache.org/jira/browse/SPARK-27277) | Improvement | Recover from setting fix version failure in merge script |
| 3.0.0 | [SPARK-27325](https://issues.apache.org/jira/browse/SPARK-27325) | New Feature | Support implicit encoders for LocalDate and Instant |
| 3.0.0 | [SPARK-27344](https://issues.apache.org/jira/browse/SPARK-27344) | Improvement | Support the LocalDate and Instant classes in Java Bean encoders |
| 3.0.0 | [SPARK-27364](https://issues.apache.org/jira/browse/SPARK-27364) | Story | User-facing APIs for GPU-aware scheduling |
| 3.0.0 | [SPARK-27397](https://issues.apache.org/jira/browse/SPARK-27397) | Improvement | Take care of OpenJ9 in JVM dependant parts |
| 3.0.0 | [SPARK-27401](https://issues.apache.org/jira/browse/SPARK-27401) | Improvement | Refactoring conversion of Date/Timestamp to/from java.sql.Date/Timestamp |
| 3.0.0 | [SPARK-27405](https://issues.apache.org/jira/browse/SPARK-27405) | Improvement | Restrict the range of generated random timestamps |
| 3.0.0 | [SPARK-27460](https://issues.apache.org/jira/browse/SPARK-27460) | Improvement | Running slowest test suites in their own forked JVMs for higher parallelism |
| 3.0.0 | [SPARK-27464](https://issues.apache.org/jira/browse/SPARK-27464) | Improvement | Add Constant instead of referring string literal used from many places |
| 3.0.0 | [SPARK-27492](https://issues.apache.org/jira/browse/SPARK-27492) | Story | GPU scheduling - High level user documentation |
| 3.0.0 | [SPARK-27536](https://issues.apache.org/jira/browse/SPARK-27536) | Improvement | Code improvements for 3.0: existentials edition |
| 3.0.0 | [SPARK-27586](https://issues.apache.org/jira/browse/SPARK-27586) | Improvement | Improve binary comparison: replace Scala's for-comprehension if statements with while loop |
| 3.0.0 | [SPARK-27588](https://issues.apache.org/jira/browse/SPARK-27588) | Story | Fail fast if binary file data source will load a file that is bigger than 2GB |
| 3.0.0 | [SPARK-27607](https://issues.apache.org/jira/browse/SPARK-27607) | Improvement | Improve performance of Row.toString() |
| 3.0.0 | [SPARK-27642](https://issues.apache.org/jira/browse/SPARK-27642) | Improvement | make v1 offset extends v2 offset |
| 3.0.0 | [SPARK-27649](https://issues.apache.org/jira/browse/SPARK-27649) | Improvement | Unify the way you use 'spark.network.timeout' |
| 3.0.0 | [SPARK-27707](https://issues.apache.org/jira/browse/SPARK-27707) | Improvement | Prune unnecessary nested fields from Generate |
| 3.0.0 | [SPARK-27725](https://issues.apache.org/jira/browse/SPARK-27725) | Story | GPU Scheduling - add an example discovery Script |
| 3.0.0 | [SPARK-27726](https://issues.apache.org/jira/browse/SPARK-27726) | Umbrella | Performance of InMemoryStore suffers under load |
| 3.0.0 | [SPARK-27739](https://issues.apache.org/jira/browse/SPARK-27739) | Improvement | df.persist should save stats from optimized plan |
| 3.0.0 | [SPARK-27752](https://issues.apache.org/jira/browse/SPARK-27752) | Improvement | Updata lz4-java from 1.5.1 to 1.6.0 |
| 3.0.0 | [SPARK-27760](https://issues.apache.org/jira/browse/SPARK-27760) | Story | Spark resources - user configs change .count to be .amount |
| 3.0.0 | [SPARK-27774](https://issues.apache.org/jira/browse/SPARK-27774) | Improvement | Avoid hardcoded configs |
| 3.0.0 | [SPARK-27776](https://issues.apache.org/jira/browse/SPARK-27776) | Improvement | Avoid duplicate Java reflection in DataSource |
| 3.0.0 | [SPARK-27823](https://issues.apache.org/jira/browse/SPARK-27823) | Story | Add an abstraction layer for accelerator resource handling to avoid manipulating raw confs |
| 3.0.0 | [SPARK-27859](https://issues.apache.org/jira/browse/SPARK-27859) | Improvement | Use efficient sorting instead of `.sorted.reverse` sequence |
| 3.0.0 | [SPARK-27875](https://issues.apache.org/jira/browse/SPARK-27875) | Improvement | Wrap all PrintWriter with Utils.tryWithResource |
| 3.0.0 | [SPARK-27897](https://issues.apache.org/jira/browse/SPARK-27897) | Story | GPU Scheduling - move example discovery Script to scripts directory |
| 3.0.0 | [SPARK-27920](https://issues.apache.org/jira/browse/SPARK-27920) | Improvement | Add `interceptParseException` test utility function |
| 3.0.0 | [SPARK-27921](https://issues.apache.org/jira/browse/SPARK-27921) | Umbrella | Convert applicable *.sql tests into UDF integrated test base |
| 3.0.0 | [SPARK-27938](https://issues.apache.org/jira/browse/SPARK-27938) | Improvement | Turn on LEGACY_PASS_PARTITION_BY_AS_OPTIONS by default |
| 3.0.0 | [SPARK-27965](https://issues.apache.org/jira/browse/SPARK-27965) | Improvement | Add extractors for logical transforms |
| 3.0.0 | [SPARK-28004](https://issues.apache.org/jira/browse/SPARK-28004) | Improvement | Update jquery to 3.4.1 |
| 3.0.0 | [SPARK-28041](https://issues.apache.org/jira/browse/SPARK-28041) | Improvement | Increase the minimum pandas version to 0.23.2 |
| 3.0.0 | [SPARK-28042](https://issues.apache.org/jira/browse/SPARK-28042) | Improvement | Support mapping spark.local.dir to hostPath volume |
| 3.0.0 | [SPARK-28045](https://issues.apache.org/jira/browse/SPARK-28045) | Improvement | add missing RankingEvaluator |
| 3.0.0 | [SPARK-28074](https://issues.apache.org/jira/browse/SPARK-28074) | Improvement | [SS] Log warn message on possible correctness issue for multiple stateful operations in single query |
| 3.0.0 | [SPARK-28102](https://issues.apache.org/jira/browse/SPARK-28102) | Improvement | Failed LZ4 JNI initialization is repeatedly re-attempted, causing lock contention issues |
| 3.0.0 | [SPARK-28159](https://issues.apache.org/jira/browse/SPARK-28159) | Improvement | Make the transform natively in ml framework to avoid extra conversion |
| 3.0.0 | [SPARK-28179](https://issues.apache.org/jira/browse/SPARK-28179) | Improvement | Avoid hard-coded config: spark.sql.globalTempDatabase |
| 3.0.0 | [SPARK-28199](https://issues.apache.org/jira/browse/SPARK-28199) | Improvement | Move Trigger implementations to Triggers.scala and avoid exposing these to the end users |
| 3.0.0 | [SPARK-28234](https://issues.apache.org/jira/browse/SPARK-28234) | Story | Spark Resources - add python support to get resources |
| 3.0.0 | [SPARK-28294](https://issues.apache.org/jira/browse/SPARK-28294) | Improvement | Support `spark.history.fs.cleaner.maxNum` configuration |
| 3.0.0 | [SPARK-28340](https://issues.apache.org/jira/browse/SPARK-28340) | Improvement | Noisy exceptions when tasks are killed: "DiskBlockObjectWriter: Uncaught exception while reverting partial writes to file: java.nio.channels.ClosedByInterruptException" |
| 3.0.0 | [SPARK-28378](https://issues.apache.org/jira/browse/SPARK-28378) | Improvement | Remove usage of cgi.escape |
| 3.0.0 | [SPARK-28433](https://issues.apache.org/jira/browse/SPARK-28433) | Improvement | Incorrect assertion in scala test for aarch64 platform |
| 3.0.0 | [SPARK-28440](https://issues.apache.org/jira/browse/SPARK-28440) | Improvement | Use TestingUtils to compare floating point values |
| 3.0.0 | [SPARK-28456](https://issues.apache.org/jira/browse/SPARK-28456) | New Feature | Add a public API `Encoder.makeCopy` to allow creating Encoder without touching Scala reflections |
| 3.0.0 | [SPARK-28473](https://issues.apache.org/jira/browse/SPARK-28473) | Improvement | Build command in README should start with ./ |
| 3.0.0 | [SPARK-28496](https://issues.apache.org/jira/browse/SPARK-28496) | Improvement | Use branch name instead of tag during dry-run |
| 3.0.0 | [SPARK-28525](https://issues.apache.org/jira/browse/SPARK-28525) | Improvement | Allow Launcher to be applied Java options |
| 3.0.0 | [SPARK-28534](https://issues.apache.org/jira/browse/SPARK-28534) | Improvement | Update node affinity for DockerForDesktop backend in PVTestsSuite |
| 3.0.0 | [SPARK-28549](https://issues.apache.org/jira/browse/SPARK-28549) | Improvement | Use `text.StringEscapeUtils` instead `lang3.StringEscapeUtils` |
| 3.0.0 | [SPARK-28552](https://issues.apache.org/jira/browse/SPARK-28552) | Improvement | The URL prefix lowercase of MySQL is not necessary, but it is necessary in spark |
| 3.0.0 | [SPARK-28564](https://issues.apache.org/jira/browse/SPARK-28564) | Improvement | Access history application defaults to the last attempt id |
| 3.0.0 | [SPARK-28574](https://issues.apache.org/jira/browse/SPARK-28574) | Improvement | Allow to config different sizes for event queues |
| 3.0.0 | [SPARK-28601](https://issues.apache.org/jira/browse/SPARK-28601) | Improvement | Use StandardCharsets.UTF_8 instead of "UTF-8" string representation |
| 3.0.0 | [SPARK-28604](https://issues.apache.org/jira/browse/SPARK-28604) | Improvement | Use log1p(x) instead of log(1+x) and expm1(x) instead of exp(x)-1 |
| 3.0.0 | [SPARK-28616](https://issues.apache.org/jira/browse/SPARK-28616) | Improvement | Improve merge-spark-pr script to warn WIP PRs and strip trailing dots |
| 3.0.0 | [SPARK-28649](https://issues.apache.org/jira/browse/SPARK-28649) | Improvement | Git Ignore does not ignore python/.eggs |
| 3.0.0 | [SPARK-28745](https://issues.apache.org/jira/browse/SPARK-28745) | New Feature | Add benchmarks for `extract()` |
| 3.0.0 | [SPARK-28762](https://issues.apache.org/jira/browse/SPARK-28762) | New Feature | Read JAR main class if JAR is not located in local file system |
| 3.0.0 | [SPARK-28857](https://issues.apache.org/jira/browse/SPARK-28857) | Improvement | Clean up the comments of PR template during merging |
| 3.0.0 | [SPARK-28858](https://issues.apache.org/jira/browse/SPARK-28858) | Improvement | add tree-based transformation in the py side |
| 3.0.0 | [SPARK-28907](https://issues.apache.org/jira/browse/SPARK-28907) | Improvement | Review invalid usage of new Configuration() |
| 3.0.0 | [SPARK-28920](https://issues.apache.org/jira/browse/SPARK-28920) | Improvement | Set up java version for github workflow |
| 3.0.0 | [SPARK-28937](https://issues.apache.org/jira/browse/SPARK-28937) | Improvement | Improve error reporting in Spark Secrets Test Suite |
| 3.0.0 | [SPARK-28972](https://issues.apache.org/jira/browse/SPARK-28972) | Improvement | [Spark] spark.memory.offHeap.size description require to update in document |
| 3.0.0 | [SPARK-28976](https://issues.apache.org/jira/browse/SPARK-28976) | Improvement | Use KeyLock to simplify MapOutputTracker.getStatuses |
| 3.0.0 | [SPARK-28997](https://issues.apache.org/jira/browse/SPARK-28997) | New Feature | Add `spark.sql.dialect` |
| 3.0.0 | [SPARK-28998](https://issues.apache.org/jira/browse/SPARK-28998) | Improvement | reorganize the packages of DS v2 interfaces/classes |
| 3.0.0 | [SPARK-29001](https://issues.apache.org/jira/browse/SPARK-29001) | Improvement | Print better log when process of events becomes slow |
| 3.0.0 | [SPARK-29020](https://issues.apache.org/jira/browse/SPARK-29020) | Improvement | Unifying behaviour between array_sort and sort_array |
| 3.0.0 | [SPARK-29030](https://issues.apache.org/jira/browse/SPARK-29030) | Improvement | Simplify lookupV2Relation |
| 3.0.0 | [SPARK-29074](https://issues.apache.org/jira/browse/SPARK-29074) | Improvement | Optimize `date_format` for foldable `fmt` |
| 3.0.0 | [SPARK-29079](https://issues.apache.org/jira/browse/SPARK-29079) | Improvement | Enable GitHub Action on PR |
| 3.0.0 | [SPARK-29080](https://issues.apache.org/jira/browse/SPARK-29080) | Improvement | Support R file extension case-insensitively |
| 3.0.0 | [SPARK-29084](https://issues.apache.org/jira/browse/SPARK-29084) | Improvement | Check method bytecode size in BenchmarkQueryTest |
| 3.0.0 | [SPARK-29087](https://issues.apache.org/jira/browse/SPARK-29087) | Improvement | Use DelegatingServletContextHandler to avoid CCE |
| 3.0.0 | [SPARK-29095](https://issues.apache.org/jira/browse/SPARK-29095) | Improvement | add extractInstances |
| 3.0.0 | [SPARK-29118](https://issues.apache.org/jira/browse/SPARK-29118) | Improvement | Avoid redundant computation in GMM.transform && GLR.transform |
| 3.0.0 | [SPARK-29121](https://issues.apache.org/jira/browse/SPARK-29121) | Improvement | Support Dot Product for Vectors |
| 3.0.0 | [SPARK-29124](https://issues.apache.org/jira/browse/SPARK-29124) | Improvement | Use MurmurHash3 `bytesHash(data, seed)` instead of `bytesHash(data)` |
| 3.0.0 | [SPARK-29155](https://issues.apache.org/jira/browse/SPARK-29155) | Improvement | Support special date/timestamp values in the PostgreSQL dialect only |
| 3.0.0 | [SPARK-29159](https://issues.apache.org/jira/browse/SPARK-29159) | Improvement | Increase ReservedCodeCacheSize to 1G |
| 3.0.0 | [SPARK-29162](https://issues.apache.org/jira/browse/SPARK-29162) | Improvement | Simplify NOT(isnull(x)) and NOT(isnotnull(x)) |
| 3.0.0 | [SPARK-29165](https://issues.apache.org/jira/browse/SPARK-29165) | Improvement | Set log level of log generated code as ERROR in case of compile error on generated code in UT |
| 3.0.0 | [SPARK-29189](https://issues.apache.org/jira/browse/SPARK-29189) | Improvement | Add an option to ignore block locations when listing file |
| 3.0.0 | [SPARK-29190](https://issues.apache.org/jira/browse/SPARK-29190) | Improvement | Optimize `extract`/`date_part` for the milliseconds `field` |
| 3.0.0 | [SPARK-29200](https://issues.apache.org/jira/browse/SPARK-29200) | Improvement | Optimize `extract`/`date_part` for epoch |
| 3.0.0 | [SPARK-29224](https://issues.apache.org/jira/browse/SPARK-29224) | New Feature | Implement Factorization Machines as a ml-pipeline component |
| 3.0.0 | [SPARK-29227](https://issues.apache.org/jira/browse/SPARK-29227) | Improvement | Track rule info in optimization phase |
| 3.0.0 | [SPARK-29246](https://issues.apache.org/jira/browse/SPARK-29246) | Improvement | Remove unnecessary imports in `core` module |
| 3.0.0 | [SPARK-29256](https://issues.apache.org/jira/browse/SPARK-29256) | Improvement | Fix typo in building document |
| 3.0.0 | [SPARK-29310](https://issues.apache.org/jira/browse/SPARK-29310) | Improvement | TestMemoryManager should implement getExecutionMemoryUsageForTask() |
| 3.0.0 | [SPARK-29463](https://issues.apache.org/jira/browse/SPARK-29463) | Improvement | move v2 commands to a new file |
| 3.0.0 | [SPARK-29469](https://issues.apache.org/jira/browse/SPARK-29469) | Improvement | Avoid retries by RetryingBlockFetcher when ExternalBlockStoreClient is closed |
| 3.0.0 | [SPARK-29470](https://issues.apache.org/jira/browse/SPARK-29470) | Improvement | Update plugins to latest versions |
| 3.0.0 | [SPARK-29515](https://issues.apache.org/jira/browse/SPARK-29515) | New Feature | MapStatuses SerDeser Benchmark |
| 3.0.0 | [SPARK-29532](https://issues.apache.org/jira/browse/SPARK-29532) | Improvement | simplify interval string parsing |
| 3.0.0 | [SPARK-29537](https://issues.apache.org/jira/browse/SPARK-29537) | Improvement | throw exception when user defined a wrong base path |
| 3.0.0 | [SPARK-29568](https://issues.apache.org/jira/browse/SPARK-29568) | Improvement | Add flag to stop existing stream when new copy starts |
| 3.0.0 | [SPARK-29572](https://issues.apache.org/jira/browse/SPARK-29572) | Improvement | add v1 read fallback API in DS v2 |
| 3.0.0 | [SPARK-29605](https://issues.apache.org/jira/browse/SPARK-29605) | Improvement | Optimize string to interval casting |
| 3.0.0 | [SPARK-29623](https://issues.apache.org/jira/browse/SPARK-29623) | Improvement | do not allow multiple unit TO unit statements in interval literal syntax |
| 3.0.0 | [SPARK-29645](https://issues.apache.org/jira/browse/SPARK-29645) | Improvement | ML add param RelativeError |
| 3.0.0 | [SPARK-29671](https://issues.apache.org/jira/browse/SPARK-29671) | Improvement | Change format of interval string |
| 3.0.0 | [SPARK-29675](https://issues.apache.org/jira/browse/SPARK-29675) | Improvement | Add exception when isolationLevel is Illegal |
| 3.0.0 | [SPARK-29723](https://issues.apache.org/jira/browse/SPARK-29723) | Improvement | Get date and time parts of an interval as java classes |
| 3.0.0 | [SPARK-29819](https://issues.apache.org/jira/browse/SPARK-29819) | Improvement | Introduce an enum for interval units |
| 3.0.0 | [SPARK-29863](https://issues.apache.org/jira/browse/SPARK-29863) | Improvement | rename EveryAgg/AnyAgg to BoolAnd/BoolOr |
| 3.0.0 | [SPARK-29864](https://issues.apache.org/jira/browse/SPARK-29864) | Improvement | Strict parsing of day-time strings to intervals |
| 3.0.0 | [SPARK-29883](https://issues.apache.org/jira/browse/SPARK-29883) | Improvement | Implement a helper method for aliasing bool_and() and bool_or() |
| 3.0.0 | [SPARK-29885](https://issues.apache.org/jira/browse/SPARK-29885) | Improvement | Improve the exception message when reading the daemon port |
| 3.0.0 | [SPARK-29889](https://issues.apache.org/jira/browse/SPARK-29889) | Improvement | unify the interval tests |
| 3.0.0 | [SPARK-29902](https://issues.apache.org/jira/browse/SPARK-29902) | Improvement | Add listener event queue capacity configuration to documentation |
| 3.0.0 | [SPARK-29903](https://issues.apache.org/jira/browse/SPARK-29903) | Improvement | Add documentation for recursiveFileLookup |
| 3.0.0 | [SPARK-29913](https://issues.apache.org/jira/browse/SPARK-29913) | Improvement | Improve Exception in postgreCastToBoolean |
| 3.0.0 | [SPARK-29937](https://issues.apache.org/jira/browse/SPARK-29937) | Improvement | Make FileSourceScanExec class fields lazy |
| 3.0.0 | [SPARK-29948](https://issues.apache.org/jira/browse/SPARK-29948) | Improvement | make the default alias consistent between date, timestamp and interval |
| 3.0.0 | [SPARK-29956](https://issues.apache.org/jira/browse/SPARK-29956) | Improvement | A literal number with an exponent should be converted into Double |
| 3.0.0 | [SPARK-29964](https://issues.apache.org/jira/browse/SPARK-29964) | Improvement | lintr github action failed due to buggy GnuPG |
| 3.0.0 | [SPARK-30057](https://issues.apache.org/jira/browse/SPARK-30057) | Improvement | Add a statement of platforms that Spark runs on |
| 3.0.0 | [SPARK-30102](https://issues.apache.org/jira/browse/SPARK-30102) | Improvement | GMM supports instance weighting |
| 3.0.0 | [SPARK-30125](https://issues.apache.org/jira/browse/SPARK-30125) | Improvement | Remove PostgreSQL dialect |
| 3.0.0 | [SPARK-30148](https://issues.apache.org/jira/browse/SPARK-30148) | Improvement | Optimize writing plans if there is an analysis exception |
| 3.0.0 | [SPARK-30150](https://issues.apache.org/jira/browse/SPARK-30150) | Improvement | Manage resources (ADD/LIST) does not support quoted path |
| 3.0.0 | [SPARK-30173](https://issues.apache.org/jira/browse/SPARK-30173) | Improvement | Automatically close stale PRs |
| 3.0.0 | [SPARK-30183](https://issues.apache.org/jira/browse/SPARK-30183) | Improvement | Disallow to specify reserved properties in CREATE NAMESPACE syntax |
| 3.0.0 | [SPARK-30184](https://issues.apache.org/jira/browse/SPARK-30184) | Improvement | Implement a helper method for aliasing functions |
| 3.0.0 | [SPARK-30205](https://issues.apache.org/jira/browse/SPARK-30205) | Improvement | Import ABC from collections.abc to remove deprecation warnings |
| 3.0.0 | [SPARK-30206](https://issues.apache.org/jira/browse/SPARK-30206) | Improvement | Rename normalizeFilters in DataSourceStrategy to be generic |
| 3.0.0 | [SPARK-30211](https://issues.apache.org/jira/browse/SPARK-30211) | Improvement | Use python3 in make-distribution.sh |
| 3.0.0 | [SPARK-30214](https://issues.apache.org/jira/browse/SPARK-30214) | Improvement | A new framework to resolve v2 commands with a case of COMMENT ON syntax implementation |
| 3.0.0 | [SPARK-30216](https://issues.apache.org/jira/browse/SPARK-30216) | Improvement | Use python3 in Docker release image |
| 3.0.0 | [SPARK-30226](https://issues.apache.org/jira/browse/SPARK-30226) | Improvement | Remove withXXX functions in WriteBuilder |
| 3.0.0 | [SPARK-30227](https://issues.apache.org/jira/browse/SPARK-30227) | Improvement | Add close() on DataWriter interface |
| 3.0.0 | [SPARK-30234](https://issues.apache.org/jira/browse/SPARK-30234) | Improvement | ADD FILE can not add folder from Spark-sql |
| 3.0.0 | [SPARK-30253](https://issues.apache.org/jira/browse/SPARK-30253) | Improvement | Do not add commits when releasing preview version |
| 3.0.0 | [SPARK-30290](https://issues.apache.org/jira/browse/SPARK-30290) | Improvement | Count for merged block when fetch continuous blocks in batch |
| 3.0.0 | [SPARK-30309](https://issues.apache.org/jira/browse/SPARK-30309) | Improvement | Mark `Filter` as a `sealed` class |
| 3.0.0 | [SPARK-30321](https://issues.apache.org/jira/browse/SPARK-30321) | Improvement | log weightSum in Algo that has weights support |
| 3.0.0 | [SPARK-30330](https://issues.apache.org/jira/browse/SPARK-30330) | Improvement | Support single quotes json parsing for get_json_object and json_tuple |
| 3.0.0 | [SPARK-30339](https://issues.apache.org/jira/browse/SPARK-30339) | Improvement | Avoid to fail twice in function lookup |
| 3.0.0 | [SPARK-30353](https://issues.apache.org/jira/browse/SPARK-30353) | Improvement | Use constraints in SimplifyBinaryComparison optimization |
| 3.0.0 | [SPARK-30376](https://issues.apache.org/jira/browse/SPARK-30376) | Improvement | Unify the computation of numFeatures |
| 3.0.0 | [SPARK-30401](https://issues.apache.org/jira/browse/SPARK-30401) | Improvement | Call requireNonStaticConf() only once |
| 3.0.0 | [SPARK-30413](https://issues.apache.org/jira/browse/SPARK-30413) | Improvement | Avoid unnecessary WrappedArray roundtrip in GenericArrayData constructor |
| 3.0.0 | [SPARK-30434](https://issues.apache.org/jira/browse/SPARK-30434) | Improvement | Move pandas related functionalities into 'pandas' sub-package |
| 3.0.0 | [SPARK-30498](https://issues.apache.org/jira/browse/SPARK-30498) | Improvement | Fix some ml parity issues between python and scala |
| 3.0.0 | [SPARK-30510](https://issues.apache.org/jira/browse/SPARK-30510) | Improvement | Publicly document options under spark.sql.* |
| 3.0.0 | [SPARK-30570](https://issues.apache.org/jira/browse/SPARK-30570) | Improvement | Update scalafmt to 1.0.3 with onlyChangedFiles feature |
| 3.0.0 | [SPARK-30594](https://issues.apache.org/jira/browse/SPARK-30594) | Improvement | Do not post SparkListenerBlockUpdated when updateBlockInfo returns false |
| 3.0.0 | [SPARK-30625](https://issues.apache.org/jira/browse/SPARK-30625) | Improvement | Add `escapeChar` parameter to the `like` function |
| 3.0.0 | [SPARK-30638](https://issues.apache.org/jira/browse/SPARK-30638) | Improvement | add resources as parameter to the PluginContext |
| 3.0.0 | [SPARK-30653](https://issues.apache.org/jira/browse/SPARK-30653) | Improvement | EOL character enforcement for java/scala/xml/py/R files |
| 3.0.0 | [SPARK-30674](https://issues.apache.org/jira/browse/SPARK-30674) | Improvement | Use python3 in dev/lint-python |
| 3.0.0 | [SPARK-30773](https://issues.apache.org/jira/browse/SPARK-30773) | Improvement | Support NativeBlas for level-1 routines |
| 3.0.0 | [SPARK-30780](https://issues.apache.org/jira/browse/SPARK-30780) | New Feature | LocalRelation should use emptyRDD if it is empty |
| 3.0.0 | [SPARK-30788](https://issues.apache.org/jira/browse/SPARK-30788) | Improvement | Support `SimpleDateFormat` and `FastDateFormat` as legacy date/timestamp formatters |
| 3.0.0 | [SPARK-30806](https://issues.apache.org/jira/browse/SPARK-30806) | Improvement | Evaluate once per group in UnboundedWindowFunctionFrame |
| 3.0.0 | [SPARK-30812](https://issues.apache.org/jira/browse/SPARK-30812) | Improvement | Revise boolean config name according to new config naming policy |
| 3.0.0 | [SPARK-30839](https://issues.apache.org/jira/browse/SPARK-30839) | Improvement | Add version information for Spark configuration |
| 3.0.0 | [SPARK-30851](https://issues.apache.org/jira/browse/SPARK-30851) | Improvement | Add 'path' field to the 'LoadInstanceEnd' ML listener event |
| 3.0.0 | [SPARK-30892](https://issues.apache.org/jira/browse/SPARK-30892) | Improvement | Exclude spark.sql.variable.substitute.depth from removedSQLConfigs |
| 3.0.0 | [SPARK-30919](https://issues.apache.org/jira/browse/SPARK-30919) | Improvement | Make interval multiply and divide's overflow behavior consisitent with other interval operations |
| 3.0.0 | [SPARK-30936](https://issues.apache.org/jira/browse/SPARK-30936) | Improvement | Forwards-compatibility in JsonProtocol in broken |
| 3.0.0 | [SPARK-30954](https://issues.apache.org/jira/browse/SPARK-30954) | Improvement | TreeModelWrappers class name do not correspond to file name |
| 3.0.0 | [SPARK-30956](https://issues.apache.org/jira/browse/SPARK-30956) | Improvement | Use intercept instead of try-catch to assert failures in IntervalUtilsSuite |
| 3.0.0 | [SPARK-30964](https://issues.apache.org/jira/browse/SPARK-30964) | Improvement | Accelerate InMemoryStore with a new index |
| 3.0.0 | [SPARK-30992](https://issues.apache.org/jira/browse/SPARK-30992) | Improvement | Arrange scattered config of streaming module |
| 3.0.0 | [SPARK-31005](https://issues.apache.org/jira/browse/SPARK-31005) | New Feature | Support time zone ids in casting strings to timestamps |
| 3.0.0 | [SPARK-31012](https://issues.apache.org/jira/browse/SPARK-31012) | Improvement | Update ML 3.0 docs |
| 3.0.0 | [SPARK-31019](https://issues.apache.org/jira/browse/SPARK-31019) | Improvement | make it clear that people can deduplicate map keys |
| 3.0.0 | [SPARK-31036](https://issues.apache.org/jira/browse/SPARK-31036) | Improvement | Use stringArgs in Expression.toString to respect hidden parameters |
| 3.0.0 | [SPARK-31053](https://issues.apache.org/jira/browse/SPARK-31053) | Improvement | mark connector API as Evolving |
| 3.0.0 | [SPARK-31058](https://issues.apache.org/jira/browse/SPARK-31058) | Improvement | Consolidate the implementation of quoteIfNeeded |
| 3.0.0 | [SPARK-31085](https://issues.apache.org/jira/browse/SPARK-31085) | Umbrella | Amend Spark's Semantic Versioning Policy |
| 3.0.0 | [SPARK-31135](https://issues.apache.org/jira/browse/SPARK-31135) | Improvement | Upgrdade docker-client version to 8.14.1 |
| 3.0.0 | [SPARK-31207](https://issues.apache.org/jira/browse/SPARK-31207) | Improvement | Ensure the total number of blocks to fetch equals to the sum of local/hostLocal/remote blocks |
| 3.0.0 | [SPARK-31225](https://issues.apache.org/jira/browse/SPARK-31225) | Improvement | Override `sql` method for OuterReference |
| 3.0.0 | [SPARK-31313](https://issues.apache.org/jira/browse/SPARK-31313) | Improvement | Add `m01` node name to support Minikube 1.8.x |
| 3.0.0 | [SPARK-31344](https://issues.apache.org/jira/browse/SPARK-31344) | Improvement | Polish implementation of barrier() and allGather() |
| 3.0.0 | [SPARK-31415](https://issues.apache.org/jira/browse/SPARK-31415) | Improvement | builtin date-time functions/operations improvement |
| 3.0.0 | [SPARK-31507](https://issues.apache.org/jira/browse/SPARK-31507) | Improvement | Remove millennium, century, decade, millisecond, microsecond and epoch from extract fucntion |
| 3.0.0 | [SPARK-31528](https://issues.apache.org/jira/browse/SPARK-31528) | Improvement | Remove millennium, century, decade from trunc/date_trunc fucntions |
| 3.0.0 | [SPARK-31597](https://issues.apache.org/jira/browse/SPARK-31597) | Improvement | extracting day from intervals should be interval.days + days in interval.microsecond |
| 3.0.0 | [SPARK-31626](https://issues.apache.org/jira/browse/SPARK-31626) | Improvement | Port HIVE-10415: hive.start.cleanup.scratchdir configuration is not taking effect |
| 3.0.0 | [SPARK-31721](https://issues.apache.org/jira/browse/SPARK-31721) | Improvement | Assert optimized plan is initialized before tracking the execution of planning |
| 3.0.0 | [SPARK-31839](https://issues.apache.org/jira/browse/SPARK-31839) | Improvement | delete duplicate code |
| 3.0.0 | [SPARK-31849](https://issues.apache.org/jira/browse/SPARK-31849) | Improvement | Improve Python exception messages to be more Pythonic |
| 3.0.0 | [SPARK-31853](https://issues.apache.org/jira/browse/SPARK-31853) | Improvement | Mention removal of params mixins setter in migration guide |
| 3.0.0 | [SPARK-31860](https://issues.apache.org/jira/browse/SPARK-31860) | Improvement | Only push release tags on success |
| 3.0.0 | [SPARK-31874](https://issues.apache.org/jira/browse/SPARK-31874) | Improvement | Use `FastDateFormat` as the legacy fractional formatter |
| 3.0.0 | [SPARK-31878](https://issues.apache.org/jira/browse/SPARK-31878) | Improvement | Create date formatter only once in HiveResult |
| 3.0.0 | [SPARK-35355](https://issues.apache.org/jira/browse/SPARK-35355) | Improvement | improve execution performance in insert...select...limit case |
| 3.1.1 | [SPARK-33434](https://issues.apache.org/jira/browse/SPARK-33434) | Improvement | Document spark.conf.isModifiable() |
| 3.1.1 | [SPARK-33796](https://issues.apache.org/jira/browse/SPARK-33796) | Improvement | Show hidden text from the left menu of Spark Doc |
| 3.1.1 | [SPARK-34059](https://issues.apache.org/jira/browse/SPARK-34059) | Improvement | Use for/foreach rather than map to make sure execute it eagerly |
| 3.1.1 | [SPARK-34118](https://issues.apache.org/jira/browse/SPARK-34118) | Improvement | Replaces filter and check for emptiness with exists or forall |
| 3.1.1 | [SPARK-34151](https://issues.apache.org/jira/browse/SPARK-34151) | Improvement | Replaces `java.io.FIle.toURL` with `java.io.File.toURI.toURL` |
| 3.1.1 | [SPARK-34178](https://issues.apache.org/jira/browse/SPARK-34178) | Improvement | Copy tags for the new node created by MultiInstanceRelation.newInstance |
| 3.1.1 | [SPARK-34181](https://issues.apache.org/jira/browse/SPARK-34181) | Improvement | Update build doc help document |
| 3.1.1 | [SPARK-34185](https://issues.apache.org/jira/browse/SPARK-34185) | Improvement | Review and fix issues in API docs |
| 3.1.1 | [SPARK-34192](https://issues.apache.org/jira/browse/SPARK-34192) | Improvement | Move char padding to write side |
| 3.1.1 | [SPARK-34235](https://issues.apache.org/jira/browse/SPARK-34235) | Improvement | Make spark.sql.hive as a private package |
| 3.1.1 | [SPARK-34275](https://issues.apache.org/jira/browse/SPARK-34275) | Improvement | Replaces filter and size with count |
| 3.1.1 | [SPARK-34310](https://issues.apache.org/jira/browse/SPARK-34310) | Improvement | Replaces map and flatten with flatMap |
| 3.1.1 | [SPARK-34384](https://issues.apache.org/jira/browse/SPARK-34384) | Improvement | Add missing docs for ResourceProfile APIs |
| 3.1.1 | [SPARK-34431](https://issues.apache.org/jira/browse/SPARK-34431) | Improvement | Only load hive-site.xml once |
| 3.2.0 | [SPARK-22256](https://issues.apache.org/jira/browse/SPARK-22256) | Improvement | Introduce spark.mesos.driver.memoryOverhead |
| 3.2.0 | [SPARK-32161](https://issues.apache.org/jira/browse/SPARK-32161) | Improvement | Hide JVM traceback for SparkUpgradeException |
| 3.2.0 | [SPARK-32320](https://issues.apache.org/jira/browse/SPARK-32320) | Improvement | Remove mutable default arguments |
| 3.2.0 | [SPARK-33207](https://issues.apache.org/jira/browse/SPARK-33207) | Improvement | Reduce the number of tasks launched after bucket pruning |
| 3.2.0 | [SPARK-33261](https://issues.apache.org/jira/browse/SPARK-33261) | Improvement | Allow people to extend the pod feature steps |
| 3.2.0 | [SPARK-33346](https://issues.apache.org/jira/browse/SPARK-33346) | Improvement | Change the never changed var to val |
| 3.2.0 | [SPARK-33434](https://issues.apache.org/jira/browse/SPARK-33434) | Improvement | Document spark.conf.isModifiable() |
| 3.2.0 | [SPARK-33717](https://issues.apache.org/jira/browse/SPARK-33717) | Improvement | deprecate spark.launcher.childConectionTimeout |
| 3.2.0 | [SPARK-33835](https://issues.apache.org/jira/browse/SPARK-33835) | Improvement | Refector AbstractCommandBuilder |
| 3.2.0 | [SPARK-33909](https://issues.apache.org/jira/browse/SPARK-33909) | Improvement | Check rand functions seed is legal at analyer side |
| 3.2.0 | [SPARK-33936](https://issues.apache.org/jira/browse/SPARK-33936) | Improvement | Add the version when connector methods and interfaces were added |
| 3.2.0 | [SPARK-33955](https://issues.apache.org/jira/browse/SPARK-33955) | Improvement | Add latest offsets to source progress |
| 3.2.0 | [SPARK-33983](https://issues.apache.org/jira/browse/SPARK-33983) | Improvement | Update cloudpickle to v1.6.0 |
| 3.2.0 | [SPARK-34009](https://issues.apache.org/jira/browse/SPARK-34009) | Improvement | Activate profile 'aarch64' based on OS |
| 3.2.0 | [SPARK-34037](https://issues.apache.org/jira/browse/SPARK-34037) | Improvement | Remove unnecessary upcasting for Avg & Sum which handle by themself internally |
| 3.2.0 | [SPARK-34051](https://issues.apache.org/jira/browse/SPARK-34051) | Improvement | Support 32-bit unicode escape in string literals |
| 3.2.0 | [SPARK-34059](https://issues.apache.org/jira/browse/SPARK-34059) | Improvement | Use for/foreach rather than map to make sure execute it eagerly |
| 3.2.0 | [SPARK-34068](https://issues.apache.org/jira/browse/SPARK-34068) | Improvement | Remove redundant collection conversion in Spark code |
| 3.2.0 | [SPARK-34070](https://issues.apache.org/jira/browse/SPARK-34070) | Improvement | Replaces find and emptiness check with exists. |
| 3.2.0 | [SPARK-34093](https://issues.apache.org/jira/browse/SPARK-34093) | Improvement | param maxDepth should check upper bound |
| 3.2.0 | [SPARK-34101](https://issues.apache.org/jira/browse/SPARK-34101) | Improvement | Make spark-sql CLI configurable for the behavior of printing header by SET command |
| 3.2.0 | [SPARK-34220](https://issues.apache.org/jira/browse/SPARK-34220) | Improvement | BucketedRandomProjectionLSH transform opt |
| 3.2.0 | [SPARK-34261](https://issues.apache.org/jira/browse/SPARK-34261) | Improvement | Avoid side effect if create exists temporary function |
| 3.2.0 | [SPARK-34263](https://issues.apache.org/jira/browse/SPARK-34263) | Improvement | Simplify the code for treating unicode/octal/escaped characters in string literals |
| 3.2.0 | [SPARK-34269](https://issues.apache.org/jira/browse/SPARK-34269) | Improvement | simplify view resolution |
| 3.2.0 | [SPARK-34275](https://issues.apache.org/jira/browse/SPARK-34275) | Improvement | Replaces filter and size with count |
| 3.2.0 | [SPARK-34295](https://issues.apache.org/jira/browse/SPARK-34295) | Improvement | Allow option similar to mapreduce.job.hdfs-servers.token-renewal.exclude |
| 3.2.0 | [SPARK-34310](https://issues.apache.org/jira/browse/SPARK-34310) | Improvement | Replaces map and flatten with flatMap |
| 3.2.0 | [SPARK-34339](https://issues.apache.org/jira/browse/SPARK-34339) | Improvement | Expose the number of truncated paths in Utils.buildLocationMetadata() |
| 3.2.0 | [SPARK-34342](https://issues.apache.org/jira/browse/SPARK-34342) | Improvement | Format DateLiteral and TimestampLiteral toString |
| 3.2.0 | [SPARK-34374](https://issues.apache.org/jira/browse/SPARK-34374) | Improvement | Use standard methods to extract keys or values from a Map. |
| 3.2.0 | [SPARK-34395](https://issues.apache.org/jira/browse/SPARK-34395) | Improvement | Clean up unused code for code simplifications |
| 3.2.0 | [SPARK-34408](https://issues.apache.org/jira/browse/SPARK-34408) | Improvement | Refactor spark.udf.register to share the same path to generate UDF instance |
| 3.2.0 | [SPARK-34433](https://issues.apache.org/jira/browse/SPARK-34433) | Improvement | Lock jekyll version by Gemfile and Bundler |
| 3.2.0 | [SPARK-34434](https://issues.apache.org/jira/browse/SPARK-34434) | Improvement | Mention DS rebase options in SparkUpgradeException |
| 3.2.0 | [SPARK-34455](https://issues.apache.org/jira/browse/SPARK-34455) | Improvement | Deprecate spark.sql.legacy.replaceDatabricksSparkAvro.enabled |
| 3.2.0 | [SPARK-34495](https://issues.apache.org/jira/browse/SPARK-34495) | Improvement | Add DedicatedJVMTest test tag |
| 3.2.0 | [SPARK-34500](https://issues.apache.org/jira/browse/SPARK-34500) | Improvement | Replace symbol literals with $"" in examples and documents |
| 3.2.0 | [SPARK-34553](https://issues.apache.org/jira/browse/SPARK-34553) | Improvement | Rename GITHUB_API_TOKEN to GITHUB_OAUTH_KEY in translate-contributors |
| 3.2.0 | [SPARK-34570](https://issues.apache.org/jira/browse/SPARK-34570) | Improvement | Remove dead code from constructors of [Hive]SessionStateBuilder |
| 3.2.0 | [SPARK-34590](https://issues.apache.org/jira/browse/SPARK-34590) | Improvement | Allow JDWP debug for tests |
| 3.2.0 | [SPARK-34635](https://issues.apache.org/jira/browse/SPARK-34635) | Improvement | Add trailing slash in URLs to reduce unnecessary redirect |
| 3.2.0 | [SPARK-34651](https://issues.apache.org/jira/browse/SPARK-34651) | Umbrella | Improve ZSTD support |
| 3.2.0 | [SPARK-34657](https://issues.apache.org/jira/browse/SPARK-34657) | Improvement | Replace the tag of release to the hash to hide RC tags in Binder |
| 3.2.0 | [SPARK-34692](https://issues.apache.org/jira/browse/SPARK-34692) | Improvement | Support Not(Int) and Not(InSet) propagate null |
| 3.2.0 | [SPARK-34722](https://issues.apache.org/jira/browse/SPARK-34722) | Improvement | Clean up deprecated API usage related to JUnit |
| 3.2.0 | [SPARK-34749](https://issues.apache.org/jira/browse/SPARK-34749) | Improvement | Simplify CreateNamedStruct |
| 3.2.0 | [SPARK-34783](https://issues.apache.org/jira/browse/SPARK-34783) | Improvement | Support remote template files |
| 3.2.0 | [SPARK-34810](https://issues.apache.org/jira/browse/SPARK-34810) | Improvement | Update PostgreSQL test with the latest results |
| 3.2.0 | [SPARK-34812](https://issues.apache.org/jira/browse/SPARK-34812) | Improvement | RowNumberLike and RankLike should not be nullable |
| 3.2.0 | [SPARK-34818](https://issues.apache.org/jira/browse/SPARK-34818) | Improvement | Reorder the items in User Guide |
| 3.2.0 | [SPARK-34821](https://issues.apache.org/jira/browse/SPARK-34821) | Improvement | Set up a workflow for developers to run benchmark in their fork |
| 3.2.0 | [SPARK-34907](https://issues.apache.org/jira/browse/SPARK-34907) | Improvement | Add main class that runs all benchmarks |
| 3.2.0 | [SPARK-34923](https://issues.apache.org/jira/browse/SPARK-34923) | Improvement | Metadata output should not always be propagated |
| 3.2.0 | [SPARK-34940](https://issues.apache.org/jira/browse/SPARK-34940) | Improvement | Fix minor unit test in BasicWriteTaskStatsTrackerSuite |
| 3.2.0 | [SPARK-34962](https://issues.apache.org/jira/browse/SPARK-34962) | Improvement | Explicit representation of star in MergeIntoTable's Update and Insert action |
| 3.2.0 | [SPARK-34989](https://issues.apache.org/jira/browse/SPARK-34989) | Improvement | Improve the performance of mapChildren and withNewChildren methods |
| 3.2.0 | [SPARK-35002](https://issues.apache.org/jira/browse/SPARK-35002) | Improvement | Fix the java.net.BindException when testing with Github Action |
| 3.2.0 | [SPARK-35013](https://issues.apache.org/jira/browse/SPARK-35013) | Improvement | Spark allows to set spark.driver.cores=0 |
| 3.2.0 | [SPARK-35029](https://issues.apache.org/jira/browse/SPARK-35029) | Improvement | Extract a new method to eliminate duplicate code in `BufferReleasingInputStream` |
| 3.2.0 | [SPARK-35045](https://issues.apache.org/jira/browse/SPARK-35045) | Improvement | Add an internal option to control input buffer in univocity |
| 3.2.0 | [SPARK-35074](https://issues.apache.org/jira/browse/SPARK-35074) | Improvement | spark.jars.xxx configs should be moved to config/package.scala |
| 3.2.0 | [SPARK-35086](https://issues.apache.org/jira/browse/SPARK-35086) | Improvement | --verbose is not passed to SparkSQLCliDriver |
| 3.2.0 | [SPARK-35102](https://issues.apache.org/jira/browse/SPARK-35102) | Improvement | Make spark.sql.hive.version meaningful and not deprecated |
| 3.2.0 | [SPARK-35105](https://issues.apache.org/jira/browse/SPARK-35105) | Improvement | Support multiple paths for ADD FILE/JAR/ARCHIVE commands |
| 3.2.0 | [SPARK-35127](https://issues.apache.org/jira/browse/SPARK-35127) | Improvement | When we switch between different stage-detail pages, the entry item in the newly-opened page may be blank. |
| 3.2.0 | [SPARK-35135](https://issues.apache.org/jira/browse/SPARK-35135) | Improvement | Duplicate code implementation of `WritablePartitionedIterator` |
| 3.2.0 | [SPARK-35140](https://issues.apache.org/jira/browse/SPARK-35140) | Improvement | Establish error message guidelines |
| 3.2.0 | [SPARK-35143](https://issues.apache.org/jira/browse/SPARK-35143) | Improvement | Add default log config for spark-sql |
| 3.2.0 | [SPARK-35145](https://issues.apache.org/jira/browse/SPARK-35145) | Improvement | CurrentOrigin should support nested invoking |
| 3.2.0 | [SPARK-35194](https://issues.apache.org/jira/browse/SPARK-35194) | Improvement | Improve readability of NestingColumnAliasing |
| 3.2.0 | [SPARK-35223](https://issues.apache.org/jira/browse/SPARK-35223) | Improvement | Add IssueNavigationLink for IDEA |
| 3.2.0 | [SPARK-35231](https://issues.apache.org/jira/browse/SPARK-35231) | Improvement | logical.Range override maxRowsPerPartition |
| 3.2.0 | [SPARK-35255](https://issues.apache.org/jira/browse/SPARK-35255) | Improvement | Automated formatting for Scala Code for Blank Lines. |
| 3.2.0 | [SPARK-35286](https://issues.apache.org/jira/browse/SPARK-35286) | Improvement | Replace SessionState.start with SessionState.setCurrentSessionState |
| 3.2.0 | [SPARK-35292](https://issues.apache.org/jira/browse/SPARK-35292) | Improvement | Delete redundant parameter in mypy.ini |
| 3.2.0 | [SPARK-35295](https://issues.apache.org/jira/browse/SPARK-35295) | Improvement | Replace fully com.github.fommil.netlib by dev.ludovic.netlib:2.0 |
| 3.2.0 | [SPARK-35323](https://issues.apache.org/jira/browse/SPARK-35323) | Improvement | Remove unused libraries from LICENSE-binary |
| 3.2.0 | [SPARK-35329](https://issues.apache.org/jira/browse/SPARK-35329) | Improvement | Split generated switch code into pieces in ExpandExec |
| 3.2.0 | [SPARK-35333](https://issues.apache.org/jira/browse/SPARK-35333) | Improvement | skip object null check in Invoke if possible |
| 3.2.0 | [SPARK-35358](https://issues.apache.org/jira/browse/SPARK-35358) | Improvement | Set maximum Java heap used for release build |
| 3.2.0 | [SPARK-35384](https://issues.apache.org/jira/browse/SPARK-35384) | Improvement | Improve performance for InvokeLike.invoke |
| 3.2.0 | [SPARK-35424](https://issues.apache.org/jira/browse/SPARK-35424) | Improvement | Remove some useless code in ExternalBlockHandler |
| 3.2.0 | [SPARK-35445](https://issues.apache.org/jira/browse/SPARK-35445) | Improvement | Reduce the execution time of DeduplicateRelations |
| 3.2.0 | [SPARK-35456](https://issues.apache.org/jira/browse/SPARK-35456) | Improvement | Show invalid value in config entry check error message |
| 3.2.0 | [SPARK-35541](https://issues.apache.org/jira/browse/SPARK-35541) | Improvement | Simplify OptimizeSkewedJoin |
| 3.2.0 | [SPARK-35558](https://issues.apache.org/jira/browse/SPARK-35558) | Improvement | Avoid redundant computation in retrieval of approximate quantiles |
| 3.2.0 | [SPARK-35565](https://issues.apache.org/jira/browse/SPARK-35565) | Improvement | Add a config for ignoring metadata directory of file stream sink |
| 3.2.0 | [SPARK-35585](https://issues.apache.org/jira/browse/SPARK-35585) | Improvement | Support propagate empty relation through project/filter |
| 3.2.0 | [SPARK-35687](https://issues.apache.org/jira/browse/SPARK-35687) | Improvement | PythonUDFSuite move assume into its methods |
| 3.2.0 | [SPARK-35691](https://issues.apache.org/jira/browse/SPARK-35691) | Improvement | addFile/addJar/addDirectory should put CanonicalFile |
| 3.2.0 | [SPARK-35757](https://issues.apache.org/jira/browse/SPARK-35757) | Improvement | Add bitwise AND operation to BitArray and add intersect AND operation for bloom filters |
| 3.2.0 | [SPARK-35794](https://issues.apache.org/jira/browse/SPARK-35794) | Improvement | Allow custom plugin for AQE cost evaluator |
| 3.2.0 | [SPARK-35829](https://issues.apache.org/jira/browse/SPARK-35829) | Improvement | Clean up evaluates subexpressions and add more flexibility to evaluate particular subexpressoin |
| 3.2.0 | [SPARK-35831](https://issues.apache.org/jira/browse/SPARK-35831) | Improvement | Handle PathOperationException in copyFileToRemote with force on the same src and dest |
| 3.2.0 | [SPARK-35855](https://issues.apache.org/jira/browse/SPARK-35855) | Improvement | Unify reuse map data structures in non-AQE and AQE rules |
| 3.2.0 | [SPARK-35872](https://issues.apache.org/jira/browse/SPARK-35872) | Improvement | automatic some work for Spark releases |
| 3.2.0 | [SPARK-35894](https://issues.apache.org/jira/browse/SPARK-35894) | Improvement | Introduce new style enforce to not import scala.collection.Seq/IndexedSeq |
| 3.2.0 | [SPARK-35903](https://issues.apache.org/jira/browse/SPARK-35903) | Improvement | Parameterize `master` in TPCDSQueryBenchmark |
| 3.2.0 | [SPARK-35910](https://issues.apache.org/jira/browse/SPARK-35910) | Improvement | Update remoteBlockBytes based on merged block info |
| 3.2.0 | [SPARK-35940](https://issues.apache.org/jira/browse/SPARK-35940) | Improvement | Refactor EquivalentExpressions to make it more efficient |
| 3.2.0 | [SPARK-35947](https://issues.apache.org/jira/browse/SPARK-35947) | Improvement | Increase JVM stack size in release-build.sh |
| 3.2.0 | [SPARK-35958](https://issues.apache.org/jira/browse/SPARK-35958) | Improvement | Refactor SparkError.scala to SparkThrowable.java |
| 3.2.0 | [SPARK-36051](https://issues.apache.org/jira/browse/SPARK-36051) | Improvement | Remove automatic update of documentation build in the guides |
| 3.2.0 | [SPARK-36164](https://issues.apache.org/jira/browse/SPARK-36164) | Improvement | Change run-test.py so that it does not fail when os.environ["APACHE_SPARK_REF"] is not defined. |
| 3.2.0 | [SPARK-36265](https://issues.apache.org/jira/browse/SPARK-36265) | Improvement | Use __getitem__ instead of getItem to suppress warnings. |
| 3.2.0 | [SPARK-36270](https://issues.apache.org/jira/browse/SPARK-36270) | Improvement | Change memory settings for enabling GA |
| 3.2.0 | [SPARK-36333](https://issues.apache.org/jira/browse/SPARK-36333) | Improvement | Reuse isnull where the null check is needed. |
| 3.2.0 | [SPARK-36338](https://issues.apache.org/jira/browse/SPARK-36338) | Improvement | Move distributed-sequence implementation to Scala side |
| 3.2.0 | [SPARK-36365](https://issues.apache.org/jira/browse/SPARK-36365) | Improvement | Remove old workarounds related to null ordering. |
| 3.2.0 | [SPARK-36367](https://issues.apache.org/jira/browse/SPARK-36367) | Umbrella | Fix the behavior to follow pandas >= 1.3 |
| 3.2.0 | [SPARK-36393](https://issues.apache.org/jira/browse/SPARK-36393) | Improvement | Try to raise memory and parallelism again for GA |
| 3.2.0 | [SPARK-36617](https://issues.apache.org/jira/browse/SPARK-36617) | Improvement | Inconsistencies in approxQuantile annotations |
| 3.2.0 | [SPARK-36685](https://issues.apache.org/jira/browse/SPARK-36685) | Improvement | Fix wrong assert messages |
| 3.2.0 | [SPARK-36788](https://issues.apache.org/jira/browse/SPARK-36788) | Improvement | Change log level of AQE for non-supported plans from warning to debug |
| 4.1.1 | [SPARK-54800](https://issues.apache.org/jira/browse/SPARK-54800) | Improvement | Changed default implementation for isObjectNotFoundException |
| 4.1.1 | [SPARK-54801](https://issues.apache.org/jira/browse/SPARK-54801) | Improvement | Mark a few new 4.1 configs as internal |
| 4.1.2 | [SPARK-55096](https://issues.apache.org/jira/browse/SPARK-55096) | New Feature | Update pandas minimum version in `connect/setup.py` |
| 4.1.2 | [SPARK-55223](https://issues.apache.org/jira/browse/SPARK-55223) | Improvement | Document sinks in declarative pipelines programming guide |
| 4.1.2 | [SPARK-55258](https://issues.apache.org/jira/browse/SPARK-55258) | Improvement | Document CLI parameters in declarative pipelines programming guide |
| 4.1.2 | [SPARK-56133](https://issues.apache.org/jira/browse/SPARK-56133) | New Feature | SparkSQL AI Function |
<!-- AUTO:timeline END -->
