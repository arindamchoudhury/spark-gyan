# SQL & Catalyst

> Source: `release-notes/spark_all_changelogs.txt`. Timeline rows below are generated from `_catalog.jsonl`; prose outside the AUTO markers is hand-written.

## How it evolved

### 1.x era — the DataFrame API and Spark SQL's graduation

Spark SQL debuted as an alpha component in 1.0.0, querying structured data from Hive or Parquet (or a schema-tagged RDD) through the Catalyst optimizer, which chose execution plans and pushed predicates into formats like Parquet. 1.1.0 added a JDBC/ODBC server for shared access to cached tables, dynamic bytecode generation for expression evaluation, and UDF registration from Python, Scala, and Java lambdas. The turning point was 1.3.0: the DataFrame API arrived with named columns and schema information across Python, Scala, and Java, and Spark SQL graduated from alpha with HiveQL and API compatibility guarantees. 1.4.0 added sort-merge joins for large joins (SPARK-2213); 1.5.0 turned on code generation by default and rewrote aggregation, join, and sort execution around Tungsten's memory model. 1.6.0 closed the era with initial adaptive query execution (SPARK-9858), auto-selecting reducer counts for joins and aggregations.

## Timeline

<!-- AUTO:timeline START -->
| Release | JIRA | Type | Title |
|---|---|---|---|
| 1.0.0 | — | prose | Spark SQL introduced as new alpha component |
| 1.0.0 | — | prose | Spark SQL API interoperates with the RDD data model |
| 1.0.0 | — | prose | Catalyst optimizer chooses execution plan and pushes predicates into Parquet |
| 1.0.1 | [SPARK-1508](https://issues.apache.org/jira/browse/SPARK-1508) | prose | Support for SQL-specific configuration (initial use: partition count) |
| 1.0.1 | [SPARK-1968](https://issues.apache.org/jira/browse/SPARK-1968) | prose | Improved SQL command support (CACHE TABLE, DESCRIBE, SHOW TABLES) |
| 1.0.1 | [SPARK-2191](https://issues.apache.org/jira/browse/SPARK-2191) | prose | Idempotence for DDL operations |
| 1.1.0 | — | prose | JDBC/ODBC server for Spark SQL with shared cached-table access |
| 1.1.0 | — | prose | Dynamic bytecode generation for complex expression evaluation |
| 1.1.0 | — | prose | Register Python/Scala/Java lambda functions as SQL UDFs |
| 1.1.1 | [SPARK-3708](https://issues.apache.org/jira/browse/SPARK-3708) | prose | Support backticks in aliases |
| 1.2.0 | — | prose | Dynamically partitioned inserts added to Spark SQL |
| 1.2.0 | — | prose | Improved caching of SchemaRDD instances and statistics-based partition pruning |
| 1.3.0 | — | prose | Multi-level aggregation trees speed up expensive reduce operations |
| 1.3.0 | — | prose | New DataFrame API with named fields and schema information |
| 1.3.0 | — | prose | DataFrame API supported in Python, Scala, and Java |
| 1.3.0 | — | prose | Spark SQL graduates from alpha with HiveQL and API compatibility guarantees |
| 1.4.0 | [SPARK-2213](https://issues.apache.org/jira/browse/SPARK-2213) | prose | Sort-merge joins to optimize very large joins |
| 1.4.0 | [SPARK-6117](https://issues.apache.org/jira/browse/SPARK-6117) | prose | Summary and descriptive statistics |
| 1.4.0 | [SPARK-6231](https://issues.apache.org/jira/browse/SPARK-6231) | prose | Improved API support for self joins |
| 1.4.0 | [SPARK-8299](https://issues.apache.org/jira/browse/SPARK-8299) | prose | Improved error message reporting for DataFrame and SQL |
| 1.5.0 | — | prose | Consistent resolution of column names |
| 1.5.0 | — | prose | Code generation on by default for almost all DataFrame/SQL functions |
| 1.5.0 | — | prose | Improved aggregation execution in DataFrame/SQL |
| 1.5.0 | — | prose | Improved join execution in DataFrame/SQL |
| 1.5.0 | — | prose | Improved sort execution in DataFrame/SQL |
| 1.5.0 | [SPARK-2660](https://issues.apache.org/jira/browse/SPARK-2660) | Improvement | Enable pretty-printing SchemaRDD Rows |
| 1.5.0 | [SPARK-2695](https://issues.apache.org/jira/browse/SPARK-2695) | Improvement | Figure out a good way to handle NullType columns. |
| 1.5.0 | [SPARK-3379](https://issues.apache.org/jira/browse/SPARK-3379) | Improvement | Implement 'POWER' for sql |
| 1.5.0 | [SPARK-3700](https://issues.apache.org/jira/browse/SPARK-3700) | Improvement | Improve the performance of scanning JSON datasets |
| 1.5.0 | [SPARK-3947](https://issues.apache.org/jira/browse/SPARK-3947) | prose | New experimental UDAF interface |
| 1.5.0 | [SPARK-4234](https://issues.apache.org/jira/browse/SPARK-4234) | Improvement | Always do paritial aggregation |
| 1.5.0 | [SPARK-4485](https://issues.apache.org/jira/browse/SPARK-4485) | Improvement | Add broadcast outer join to optimize left outer join and right outer join |
| 1.5.0 | [SPARK-6116](https://issues.apache.org/jira/browse/SPARK-6116) | Umbrella | DataFrame API improvement umbrella ticket (Spark 1.5) |
| 1.5.0 | [SPARK-6489](https://issues.apache.org/jira/browse/SPARK-6489) | Improvement | Optimize lateral view with explode to not read unnecessary columns |
| 1.5.0 | [SPARK-6583](https://issues.apache.org/jira/browse/SPARK-6583) | Improvement | Support aggregated function in order by |
| 1.5.0 | [SPARK-7083](https://issues.apache.org/jira/browse/SPARK-7083) | New Feature | Binary processing dimensional join |
| 1.5.0 | [SPARK-7165](https://issues.apache.org/jira/browse/SPARK-7165) | Story | Sort-merge Join for left/right outer joins |
| 1.5.0 | [SPARK-7184](https://issues.apache.org/jira/browse/SPARK-7184) | Improvement | Investigate turning codegen on by default |
| 1.5.0 | [SPARK-7289](https://issues.apache.org/jira/browse/SPARK-7289) | Improvement | Combine Limit and Sort to avoid total ordering |
| 1.5.0 | [SPARK-7293](https://issues.apache.org/jira/browse/SPARK-7293) | New Feature | Report memory used in aggregations and joins |
| 1.5.0 | [SPARK-7325](https://issues.apache.org/jira/browse/SPARK-7325) | Improvement | Dataframe should support partitioned JSON Relation |
| 1.5.0 | [SPARK-7440](https://issues.apache.org/jira/browse/SPARK-7440) | New Feature | Remove physical Distinct operator in favor of Aggregate |
| 1.5.0 | [SPARK-7812](https://issues.apache.org/jira/browse/SPARK-7812) | Improvement | Speed up SQL code generation |
| 1.5.0 | [SPARK-7824](https://issues.apache.org/jira/browse/SPARK-7824) | Improvement | Collapsing operator reordering and constant folding into a single batch to push down the single side. |
| 1.5.0 | [SPARK-7910](https://issues.apache.org/jira/browse/SPARK-7910) | Improvement | Expose partitioner information in JavaRDD |
| 1.5.0 | [SPARK-7913](https://issues.apache.org/jira/browse/SPARK-7913) | Improvement | Increase the maximum capacity of PartitionedPairBuffer, PartitionedSerializedPairBuffer and AppendOnlyMap |
| 1.5.0 | [SPARK-7961](https://issues.apache.org/jira/browse/SPARK-7961) | Improvement | Redesign SQLConf for better error message reporting |
| 1.5.0 | [SPARK-7969](https://issues.apache.org/jira/browse/SPARK-7969) | Improvement | Drop method on Dataframes should handle Column |
| 1.5.0 | [SPARK-8002](https://issues.apache.org/jira/browse/SPARK-8002) | Umbrella | Support virtual columns in SQL and DataFrames |
| 1.5.0 | [SPARK-8070](https://issues.apache.org/jira/browse/SPARK-8070) | Improvement | Improve createDataFrame in Python |
| 1.5.0 | [SPARK-8077](https://issues.apache.org/jira/browse/SPARK-8077) | Improvement | Optimisation of TreeNode for large number of children |
| 1.5.0 | [SPARK-8138](https://issues.apache.org/jira/browse/SPARK-8138) | Improvement | Error message for discovered conflicting partition columns is not intuitive |
| 1.5.0 | [SPARK-8141](https://issues.apache.org/jira/browse/SPARK-8141) | Improvement | Precompute datatypes for partition columns and reuse it |
| 1.5.0 | [SPARK-8154](https://issues.apache.org/jira/browse/SPARK-8154) | Improvement | Remove Term/Code type aliases in code generation |
| 1.5.0 | [SPARK-8278](https://issues.apache.org/jira/browse/SPARK-8278) | Story | Remove deprecated JsonRDD functionality in Spark SQL |
| 1.5.0 | [SPARK-8286](https://issues.apache.org/jira/browse/SPARK-8286) | Improvement | Rewrite UTF8String in Java and move it into unsafe package. |
| 1.5.0 | [SPARK-8300](https://issues.apache.org/jira/browse/SPARK-8300) | prose | DataFrame hint for broadcast joins |
| 1.5.0 | [SPARK-8301](https://issues.apache.org/jira/browse/SPARK-8301) | Improvement | Improve UTF8String substring/startsWith/endsWith/contains performance |
| 1.5.0 | [SPARK-8328](https://issues.apache.org/jira/browse/SPARK-8328) | Improvement | Add a CheckAnalysis rule to ensure that Union branches have the same schema |
| 1.5.0 | [SPARK-8346](https://issues.apache.org/jira/browse/SPARK-8346) | Improvement | Use InternalRow instread of catalyst.InternalRow |
| 1.5.0 | [SPARK-8348](https://issues.apache.org/jira/browse/SPARK-8348) | New Feature | Add in operator to DataFrame Column |
| 1.5.0 | [SPARK-8381](https://issues.apache.org/jira/browse/SPARK-8381) | Improvement | reuse typeConvert when convert Seq[Row] to catalyst type |
| 1.5.0 | [SPARK-8579](https://issues.apache.org/jira/browse/SPARK-8579) | New Feature | Support arbitrary object in UnsafeRow |
| 1.5.0 | [SPARK-8590](https://issues.apache.org/jira/browse/SPARK-8590) | Improvement | add code gen for ExtractValue |
| 1.5.0 | [SPARK-8610](https://issues.apache.org/jira/browse/SPARK-8610) | Improvement | Separate Row and InternalRow (part 2) |
| 1.5.0 | [SPARK-8620](https://issues.apache.org/jira/browse/SPARK-8620) | Improvement | cleanup CodeGenContext |
| 1.5.0 | [SPARK-8635](https://issues.apache.org/jira/browse/SPARK-8635) | Improvement | improve performance of CatalystTypeConverters |
| 1.5.0 | [SPARK-8638](https://issues.apache.org/jira/browse/SPARK-8638) | prose | Improved performance & memory usage in window functions |
| 1.5.0 | [SPARK-8695](https://issues.apache.org/jira/browse/SPARK-8695) | Improvement | TreeAggregation shouldn't be triggered when it doesn't save wall-clock time |
| 1.5.0 | [SPARK-8708](https://issues.apache.org/jira/browse/SPARK-8708) | Improvement | MatrixFactorizationModel.predictAll() populates single partition only |
| 1.5.0 | [SPARK-8718](https://issues.apache.org/jira/browse/SPARK-8718) | Improvement | Improve EdgePartition2D for non perfect square number of partitions |
| 1.5.0 | [SPARK-8748](https://issues.apache.org/jira/browse/SPARK-8748) | Improvement | Move castability test out from Cast case class into Cast object |
| 1.5.0 | [SPARK-8749](https://issues.apache.org/jira/browse/SPARK-8749) | Improvement | Remove HiveTypeCoercion trait |
| 1.5.0 | [SPARK-8777](https://issues.apache.org/jira/browse/SPARK-8777) | New Feature | Add random data generation test utilities to Spark SQL |
| 1.5.0 | [SPARK-8782](https://issues.apache.org/jira/browse/SPARK-8782) | New Feature | GenerateOrdering fails for NullType (i.e. ORDER BY NULL crashes) |
| 1.5.0 | [SPARK-8787](https://issues.apache.org/jira/browse/SPARK-8787) | Improvement | Change the parameter order of @deprecated in package object sql |
| 1.5.0 | [SPARK-8809](https://issues.apache.org/jira/browse/SPARK-8809) | Improvement | Remove ConvertNaNs analyzer rule |
| 1.5.0 | [SPARK-8828](https://issues.apache.org/jira/browse/SPARK-8828) | prose | Sum function returns null when all input values are nulls |
| 1.5.0 | [SPARK-8837](https://issues.apache.org/jira/browse/SPARK-8837) | Improvement | support using keyword in column name |
| 1.5.0 | [SPARK-8856](https://issues.apache.org/jira/browse/SPARK-8856) | Umbrella | Better instrumentation and visualization for physical plan (Spark 1.5) |
| 1.5.0 | [SPARK-8879](https://issues.apache.org/jira/browse/SPARK-8879) | Improvement | Remove EmptyRow class |
| 1.5.0 | [SPARK-8883](https://issues.apache.org/jira/browse/SPARK-8883) | Improvement | Remove the class OverrideFunctionRegistry |
| 1.5.0 | [SPARK-8893](https://issues.apache.org/jira/browse/SPARK-8893) | Improvement | Require positive partition counts in RDD.repartition |
| 1.5.0 | [SPARK-8931](https://issues.apache.org/jira/browse/SPARK-8931) | Improvement | Fallback to interpret mode if failed to compile in codegen |
| 1.5.0 | [SPARK-8948](https://issues.apache.org/jira/browse/SPARK-8948) | Improvement | Remove ExtractValueWithOrdinal abstract class |
| 1.5.0 | [SPARK-8970](https://issues.apache.org/jira/browse/SPARK-8970) | Improvement | remove unnecessary abstraction for ExtractValue |
| 1.5.0 | [SPARK-8991](https://issues.apache.org/jira/browse/SPARK-8991) | Improvement | Update SharedParamsCodeGen's Generated Documentation |
| 1.5.0 | [SPARK-9024](https://issues.apache.org/jira/browse/SPARK-9024) | New Feature | Unsafe HashJoin |
| 1.5.0 | [SPARK-9029](https://issues.apache.org/jira/browse/SPARK-9029) | Improvement | shortcut CaseKeyWhen if key is null |
| 1.5.0 | [SPARK-9085](https://issues.apache.org/jira/browse/SPARK-9085) | Improvement | Remove LeafNode, UnaryNode, BinaryNode from TreeNode |
| 1.5.0 | [SPARK-9113](https://issues.apache.org/jira/browse/SPARK-9113) | Improvement | enable analysis check code for self join |
| 1.5.0 | [SPARK-9142](https://issues.apache.org/jira/browse/SPARK-9142) | Improvement | Removing unnecessary self types in Catalyst |
| 1.5.0 | [SPARK-9174](https://issues.apache.org/jira/browse/SPARK-9174) | Improvement | Add documentation for all public SQLConfs |
| 1.5.0 | [SPARK-9178](https://issues.apache.org/jira/browse/SPARK-9178) | New Feature | UTF8String empty string method |
| 1.5.0 | [SPARK-9224](https://issues.apache.org/jira/browse/SPARK-9224) | Improvement | OnlineLDAOptimizer Performance Improvements |
| 1.5.0 | [SPARK-9243](https://issues.apache.org/jira/browse/SPARK-9243) | Improvement | Update crosstab doc for pairs that have no occurrences |
| 1.5.0 | [SPARK-9244](https://issues.apache.org/jira/browse/SPARK-9244) | Improvement | Increase some default memory limits |
| 1.5.0 | [SPARK-9285](https://issues.apache.org/jira/browse/SPARK-9285) | Improvement | Remove InternalRow's inheritance from Row |
| 1.5.0 | [SPARK-9329](https://issues.apache.org/jira/browse/SPARK-9329) | Umbrella | Bring UnsafeRow up to feature parity with other InternalRow implementations |
| 1.5.0 | [SPARK-9334](https://issues.apache.org/jira/browse/SPARK-9334) | Improvement | Remove UnsafeRowConverter in favor of UnsafeProjection |
| 1.5.0 | [SPARK-9336](https://issues.apache.org/jira/browse/SPARK-9336) | Improvement | Remove all extra JoinedRows |
| 1.5.0 | [SPARK-9397](https://issues.apache.org/jira/browse/SPARK-9397) | Improvement | DataFrame should provide an API to find source data files if applicable |
| 1.5.0 | [SPARK-9464](https://issues.apache.org/jira/browse/SPARK-9464) | New Feature | Add property-based tests for UTF8String |
| 1.5.0 | [SPARK-9489](https://issues.apache.org/jira/browse/SPARK-9489) | Improvement | Remove compatibleWith, meetsRequirements, and needsAnySort checks from Exchange |
| 1.5.0 | [SPARK-9535](https://issues.apache.org/jira/browse/SPARK-9535) | Improvement | Modify document for codegen |
| 1.5.0 | [SPARK-9551](https://issues.apache.org/jira/browse/SPARK-9551) | Improvement | add copyTo for UnsafeRow to reuse a copy buffer |
| 1.5.0 | [SPARK-9554](https://issues.apache.org/jira/browse/SPARK-9554) | Improvement | Turn on in-memory relation partition pruning by default |
| 1.5.0 | [SPARK-9565](https://issues.apache.org/jira/browse/SPARK-9565) | Story | Spark SQL 1.5.0 QA/testing umbrella |
| 1.5.0 | [SPARK-9667](https://issues.apache.org/jira/browse/SPARK-9667) | Improvement | Remove SparkSqlSerializer2 in favor of Unsafe exchange |
| 1.5.0 | [SPARK-9674](https://issues.apache.org/jira/browse/SPARK-9674) | Improvement | Remove GeneratedAggregate |
| 1.5.0 | [SPARK-9677](https://issues.apache.org/jira/browse/SPARK-9677) | Improvement | Enable SQLQuerySuite."aggregation with codegen updates peak execution memory" |
| 1.5.0 | [SPARK-9733](https://issues.apache.org/jira/browse/SPARK-9733) | Improvement | Improve explain message for data source scan node |
| 1.5.0 | [SPARK-9736](https://issues.apache.org/jira/browse/SPARK-9736) | Improvement | JoinedRow.anyNull should delegate to the underlying rows |
| 1.5.0 | [SPARK-9738](https://issues.apache.org/jira/browse/SPARK-9738) | Improvement | remove FromUnsafe and add its codegen version to GenerateSafe |
| 1.5.0 | [SPARK-9751](https://issues.apache.org/jira/browse/SPARK-9751) | Umbrella | Audit operators to make sure they can support UnsafeRows |
| 1.5.0 | [SPARK-9814](https://issues.apache.org/jira/browse/SPARK-9814) | Improvement | EqualNullSafe not passing to data sources |
| 1.5.0 | [SPARK-9815](https://issues.apache.org/jira/browse/SPARK-9815) | Improvement | Rename PlatformDependent.UNSAFE -> Platform |
| 1.5.0 | [SPARK-9912](https://issues.apache.org/jira/browse/SPARK-9912) | Improvement | QRDecomposition should use QType and RType for type names instead of UType and VType |
| 1.6.0 | [SPARK-3580](https://issues.apache.org/jira/browse/SPARK-3580) | Improvement | Add Consistent Method To Get Number of RDD Partitions Across Different Languages |
| 1.6.0 | [SPARK-6006](https://issues.apache.org/jira/browse/SPARK-6006) | Improvement | Optimize count distinct in case of high cardinality columns |
| 1.6.0 | [SPARK-6981](https://issues.apache.org/jira/browse/SPARK-6981) | Improvement | [SQL] SparkPlanner and QueryExecution should be factored out from SQLContext |
| 1.6.0 | [SPARK-7316](https://issues.apache.org/jira/browse/SPARK-7316) | Improvement | Add step capability to RDD sliding window |
| 1.6.0 | [SPARK-7970](https://issues.apache.org/jira/browse/SPARK-7970) | Improvement | Optimize code for SQL queries fired on Union of RDDs (closure cleaner) |
| 1.6.0 | [SPARK-8287](https://issues.apache.org/jira/browse/SPARK-8287) | Improvement | Filters not pushed with substitution through aggregation |
| 1.6.0 | [SPARK-8992](https://issues.apache.org/jira/browse/SPARK-8992) | Improvement | Add Pivot functionality to Spark SQL |
| 1.6.0 | [SPARK-9241](https://issues.apache.org/jira/browse/SPARK-9241) | prose | Improved query planner for queries having distinct aggregations |
| 1.6.0 | [SPARK-9410](https://issues.apache.org/jira/browse/SPARK-9410) | Improvement | Better Multi-User Session Semantics for SQL Context |
| 1.6.0 | [SPARK-9715](https://issues.apache.org/jira/browse/SPARK-9715) | Improvement | Store numFeatures in all ML PredictionModel types |
| 1.6.0 | [SPARK-9730](https://issues.apache.org/jira/browse/SPARK-9730) | New Feature | Sort Merge Join for Full Outer Join |
| 1.6.0 | [SPARK-9858](https://issues.apache.org/jira/browse/SPARK-9858) | prose | Adaptive query execution: initial support for auto-selecting reducer count |
| 1.6.0 | [SPARK-9928](https://issues.apache.org/jira/browse/SPARK-9928) | Improvement | LogicalLocalTable in ExistingRDD.scala is not referenced by any code else |
| 1.6.0 | [SPARK-10117](https://issues.apache.org/jira/browse/SPARK-10117) | New Feature | Implement SQL data source API for reading LIBSVM data |
| 1.6.0 | [SPARK-10186](https://issues.apache.org/jira/browse/SPARK-10186) | New Feature | Add support for more postgres column types |
| 1.6.0 | [SPARK-10378](https://issues.apache.org/jira/browse/SPARK-10378) | Improvement | Remove HashJoinCompatibilitySuite |
| 1.6.0 | [SPARK-10384](https://issues.apache.org/jira/browse/SPARK-10384) | Umbrella | Univariate statistics as UDAFs |
| 1.6.0 | [SPARK-10395](https://issues.apache.org/jira/browse/SPARK-10395) | Improvement | Simplify CatalystReadSupport |
| 1.6.0 | [SPARK-10446](https://issues.apache.org/jira/browse/SPARK-10446) | Improvement | Support to specify join type when calling join with usingColumns |
| 1.6.0 | [SPARK-10450](https://issues.apache.org/jira/browse/SPARK-10450) | Improvement | Minor SQL style, format, typo, readability fixes |
| 1.6.0 | [SPARK-10459](https://issues.apache.org/jira/browse/SPARK-10459) | Improvement | PythonUDF could process UnsafeRow |
| 1.6.0 | [SPARK-10461](https://issues.apache.org/jira/browse/SPARK-10461) | Improvement | make sure `input.primitive` is always variable name not code at GenerateUnsafeProjection |
| 1.6.0 | [SPARK-10463](https://issues.apache.org/jira/browse/SPARK-10463) | Improvement | remove PromotePrecision during optimization |
| 1.6.0 | [SPARK-10468](https://issues.apache.org/jira/browse/SPARK-10468) | Improvement | Verify schema before Dataframe select API call |
| 1.6.0 | [SPARK-10475](https://issues.apache.org/jira/browse/SPARK-10475) | Improvement | improve column prunning for Project on Sort |
| 1.6.0 | [SPARK-10477](https://issues.apache.org/jira/browse/SPARK-10477) | Improvement | using DSL in ColumnPruningSuite to improve readablity |
| 1.6.0 | [SPARK-10546](https://issues.apache.org/jira/browse/SPARK-10546) | Improvement | Check partitionId's range in ExternalSorter#spill() |
| 1.6.0 | [SPARK-10579](https://issues.apache.org/jira/browse/SPARK-10579) | New Feature | Extend statistical functions: Add Cardinality/Quantiles/Quartiles/Median in Statistics , e.g. for columns |
| 1.6.0 | [SPARK-10630](https://issues.apache.org/jira/browse/SPARK-10630) | New Feature | createDataFrame from a Java List<Row> |
| 1.6.0 | [SPARK-10720](https://issues.apache.org/jira/browse/SPARK-10720) | Improvement | Add a java wrapper to create dataframe from a local list of Java Beans. |
| 1.6.0 | [SPARK-10770](https://issues.apache.org/jira/browse/SPARK-10770) | Improvement | SparkPlan.executeCollect/executeTake should return InternalRow rather than external Row |
| 1.6.0 | [SPARK-10810](https://issues.apache.org/jira/browse/SPARK-10810) | Improvement | Improve session management for SQL |
| 1.6.0 | [SPARK-10864](https://issues.apache.org/jira/browse/SPARK-10864) | Improvement | SparkUI: app name is hidden if window is resized |
| 1.6.0 | [SPARK-10947](https://issues.apache.org/jira/browse/SPARK-10947) | Improvement | With schema inference from JSON into a Dataframe, add option to infer all primitive object types as strings |
| 1.6.0 | [SPARK-10978](https://issues.apache.org/jira/browse/SPARK-10978) | New Feature | Allow PrunedFilterScan to eliminate predicates from further evaluation |
| 1.6.0 | [SPARK-10982](https://issues.apache.org/jira/browse/SPARK-10982) | Improvement | Rename ExpressionAggregate -> DeclarativeAggregate |
| 1.6.0 | [SPARK-10990](https://issues.apache.org/jira/browse/SPARK-10990) | Improvement | Avoid the serialization multiple times during unrolling of complex types |
| 1.6.0 | [SPARK-10996](https://issues.apache.org/jira/browse/SPARK-10996) | New Feature | Implement sampleBy() in DataFrameStatFunctions |
| 1.6.0 | [SPARK-11030](https://issues.apache.org/jira/browse/SPARK-11030) | Improvement | SQLTab should be shared by across sessions |
| 1.6.0 | [SPARK-11042](https://issues.apache.org/jira/browse/SPARK-11042) | New Feature | Introduce a mechanism to ban creating new root SQLContexts in a JVM |
| 1.6.0 | [SPARK-11068](https://issues.apache.org/jira/browse/SPARK-11068) | New Feature | Add callback to query execution |
| 1.6.0 | [SPARK-11086](https://issues.apache.org/jira/browse/SPARK-11086) | Improvement | createDataFrame should dropFactor column-wise not cell-wise |
| 1.6.0 | [SPARK-11111](https://issues.apache.org/jira/browse/SPARK-11111) | Improvement | Fast null-safe join |
| 1.6.0 | [SPARK-11117](https://issues.apache.org/jira/browse/SPARK-11117) | Improvement | PhysicalRDD.outputsUnsafeRows should return true when the underlying data source produces UnsafeRows |
| 1.6.0 | [SPARK-11158](https://issues.apache.org/jira/browse/SPARK-11158) | Improvement | Add more information in Error statment for sql/types _verify_type() |
| 1.6.0 | [SPARK-11180](https://issues.apache.org/jira/browse/SPARK-11180) | Improvement | Support BooleanType in DataFrame.na.fill |
| 1.6.0 | [SPARK-11194](https://issues.apache.org/jira/browse/SPARK-11194) | Improvement | Use a single URLClassLoader for jars added through SQL's "ADD JAR" command |
| 1.6.0 | [SPARK-11205](https://issues.apache.org/jira/browse/SPARK-11205) | Improvement | Match the output of DataFrame#explain() in both scala api and python |
| 1.6.0 | [SPARK-11243](https://issues.apache.org/jira/browse/SPARK-11243) | Improvement | Output UnsafeRow in InMemoryTableScan |
| 1.6.0 | [SPARK-11258](https://issues.apache.org/jira/browse/SPARK-11258) | Improvement | Converting a Spark DataFrame into an R data.frame is slow / requires a lot of memory |
| 1.6.0 | [SPARK-11325](https://issues.apache.org/jira/browse/SPARK-11325) | Improvement | Alias alias in Scala's DataFrame to as to match python |
| 1.6.0 | [SPARK-11329](https://issues.apache.org/jira/browse/SPARK-11329) | Improvement | Expand Star when creating a struct |
| 1.6.0 | [SPARK-11410](https://issues.apache.org/jira/browse/SPARK-11410) | Improvement | Add a DataFrame API that provides functionality similar to HiveQL's DISTRIBUTE BY |
| 1.6.0 | [SPARK-11425](https://issues.apache.org/jira/browse/SPARK-11425) | Improvement | Improve hybrid aggregation (sort-based after hash-based) |
| 1.6.0 | [SPARK-11437](https://issues.apache.org/jira/browse/SPARK-11437) | Improvement | createDataFrame shouldn't .take() when provided schema |
| 1.6.0 | [SPARK-11450](https://issues.apache.org/jira/browse/SPARK-11450) | Improvement | Add support for UnsafeRow to Expand |
| 1.6.0 | [SPARK-11477](https://issues.apache.org/jira/browse/SPARK-11477) | Improvement | support create Dataset from RDD |
| 1.6.0 | [SPARK-11485](https://issues.apache.org/jira/browse/SPARK-11485) | Improvement | Make DataFrameHolder and DatasetHolder public |
| 1.6.0 | [SPARK-11503](https://issues.apache.org/jira/browse/SPARK-11503) | Improvement | SQL API audit for Spark 1.6 |
| 1.6.0 | [SPARK-11513](https://issues.apache.org/jira/browse/SPARK-11513) | Improvement | Remove the internal implicit conversion from LogicalPlan to DataFrame |
| 1.6.0 | [SPARK-11573](https://issues.apache.org/jira/browse/SPARK-11573) | Improvement | correct 'reflective access of structural type member method should be enabled' Scala warnings |
| 1.6.0 | [SPARK-11590](https://issues.apache.org/jira/browse/SPARK-11590) | Improvement | use native json_tuple in lateral view |
| 1.6.0 | [SPARK-11628](https://issues.apache.org/jira/browse/SPARK-11628) | Improvement | spark-sql do not support for column datatype of CHAR |
| 1.6.0 | [SPARK-11644](https://issues.apache.org/jira/browse/SPARK-11644) | Improvement | Remove the option to turn off unsafe and codegen |
| 1.6.0 | [SPARK-11645](https://issues.apache.org/jira/browse/SPARK-11645) | Improvement | Remove OpenHashSet for the old aggregate. |
| 1.6.0 | [SPARK-11723](https://issues.apache.org/jira/browse/SPARK-11723) | Improvement | Use LibSVM data source rather than MLUtils.loadLibSVMFile to load DataFrame |
| 1.6.0 | [SPARK-11743](https://issues.apache.org/jira/browse/SPARK-11743) | Improvement | Add UserDefinedType support to RowEncoder |
| 1.6.0 | [SPARK-11754](https://issues.apache.org/jira/browse/SPARK-11754) | Improvement | consolidate `ExpressionEncoder.tuple` and `Encoders.tuple` |
| 1.6.0 | [SPARK-11778](https://issues.apache.org/jira/browse/SPARK-11778) | prose | DataFrameReader.table supports specifying database name |
| 1.6.0 | [SPARK-11848](https://issues.apache.org/jira/browse/SPARK-11848) | Improvement | [SQL] Support EXPLAIN in DataSet APIs |
| 1.6.0 | [SPARK-11876](https://issues.apache.org/jira/browse/SPARK-11876) | Improvement | [SQL] Support PrintSchema in DataSet APIs |
| 1.6.0 | [SPARK-11908](https://issues.apache.org/jira/browse/SPARK-11908) | Improvement | Add NullType support to RowEncoder |
| 1.6.0 | [SPARK-11914](https://issues.apache.org/jira/browse/SPARK-11914) | Improvement | [SQL] Support coalesce and repartition in Dataset APIs |
| 1.6.0 | [SPARK-11926](https://issues.apache.org/jira/browse/SPARK-11926) | Improvement | unify GetStructField and GetInternalRowField |
| 1.6.0 | [SPARK-12011](https://issues.apache.org/jira/browse/SPARK-12011) | Improvement | Stddev/Variance etc should support columnName as arguments |
| 1.6.0 | [SPARK-12077](https://issues.apache.org/jira/browse/SPARK-12077) | Improvement | Use more robust plan for single distinct aggregation |
| 1.6.0 | [SPARK-12094](https://issues.apache.org/jira/browse/SPARK-12094) | Improvement | Better format for query plan tree string |
| 1.6.0 | [SPARK-12188](https://issues.apache.org/jira/browse/SPARK-12188) | Improvement | [SQL] Code refactoring and comment correction in Dataset APIs |
| 1.6.0 | [SPARK-12242](https://issues.apache.org/jira/browse/SPARK-12242) | New Feature | DataFrame.transform function |
| 2.0.0 | [SPARK-4226](https://issues.apache.org/jira/browse/SPARK-4226) | Improvement | SparkSQL - Add support for subqueries in predicates |
| 2.0.0 | [SPARK-6735](https://issues.apache.org/jira/browse/SPARK-6735) | Improvement | Provide options to make maximum executor failure count ( which kills the application ) relative to a window duration or disable it. |
| 2.0.0 | [SPARK-6744](https://issues.apache.org/jira/browse/SPARK-6744) | Improvement | Add support for CROSS JOIN syntax |
| 2.0.0 | [SPARK-8745](https://issues.apache.org/jira/browse/SPARK-8745) | Improvement | Remove GenerateProjection |
| 2.0.0 | [SPARK-8964](https://issues.apache.org/jira/browse/SPARK-8964) | Improvement | Use Exchange in limit operations (per partition limit -> exchange to one partition -> per partition limit) |
| 2.0.0 | [SPARK-8968](https://issues.apache.org/jira/browse/SPARK-8968) | Improvement | dynamic partitioning in spark sql performance issue due to the high GC overhead |
| 2.0.0 | [SPARK-9013](https://issues.apache.org/jira/browse/SPARK-9013) | Improvement | generate MutableProjection directly instead of return a function |
| 2.0.0 | [SPARK-9041](https://issues.apache.org/jira/browse/SPARK-9041) | New Feature | Support reading of delimited text files as DataFrames |
| 2.0.0 | [SPARK-9843](https://issues.apache.org/jira/browse/SPARK-9843) | New Feature | Catalyst: Allow adding custom optimizers |
| 2.0.0 | [SPARK-9965](https://issues.apache.org/jira/browse/SPARK-9965) | New Feature | Scala, Python SQLContext input methods' deprecation statuses do not match |
| 2.0.0 | [SPARK-9999](https://issues.apache.org/jira/browse/SPARK-9999) | Story | Dataset API on top of Catalyst/DataFrame |
| 2.0.0 | [SPARK-10477](https://issues.apache.org/jira/browse/SPARK-10477) | Improvement | using DSL in ColumnPruningSuite to improve readablity |
| 2.0.0 | [SPARK-10600](https://issues.apache.org/jira/browse/SPARK-10600) | Improvement | SparkSQL - Support for Not Exists in a Correlated Subquery |
| 2.0.0 | [SPARK-10605](https://issues.apache.org/jira/browse/SPARK-10605) | New Feature | collect_list() and collect_set() should accept struct types as argument |
| 2.0.0 | [SPARK-11011](https://issues.apache.org/jira/browse/SPARK-11011) | Improvement | UserDefinedType serialization should be strongly typed |
| 2.0.0 | [SPARK-11012](https://issues.apache.org/jira/browse/SPARK-11012) | New Feature | Canonicalize view definitions |
| 2.0.0 | [SPARK-11735](https://issues.apache.org/jira/browse/SPARK-11735) | Improvement | Add a check in the constructor of SqlContext to make sure the SparkContext is not stopped |
| 2.0.0 | [SPARK-11827](https://issues.apache.org/jira/browse/SPARK-11827) | Improvement | Support java.math.BigInteger in Type-Inference utilities for POJOs |
| 2.0.0 | [SPARK-11884](https://issues.apache.org/jira/browse/SPARK-11884) | Improvement | Drop multiple columns in the DataFrame API |
| 2.0.0 | [SPARK-11982](https://issues.apache.org/jira/browse/SPARK-11982) | Improvement | Improve performance of CartesianProduct |
| 2.0.0 | [SPARK-11983](https://issues.apache.org/jira/browse/SPARK-11983) | Improvement | remove all unused codegen fallback traits |
| 2.0.0 | [SPARK-12032](https://issues.apache.org/jira/browse/SPARK-12032) | Improvement | Filter can't be pushed down to correct Join because of bad order of Join |
| 2.0.0 | [SPARK-12054](https://issues.apache.org/jira/browse/SPARK-12054) | Improvement | Consider nullable in codegen |
| 2.0.0 | [SPARK-12150](https://issues.apache.org/jira/browse/SPARK-12150) | Improvement | numPartitions argument to sqlContext.range() should be optional |
| 2.0.0 | [SPARK-12213](https://issues.apache.org/jira/browse/SPARK-12213) | Improvement | Query with only one distinct should not having on expand |
| 2.0.0 | [SPARK-12235](https://issues.apache.org/jira/browse/SPARK-12235) | Improvement | Enhance mutate() to support replace existing columns |
| 2.0.0 | [SPARK-12286](https://issues.apache.org/jira/browse/SPARK-12286) | Epic | Support UnsafeRow in all SparkPlan (if possible) |
| 2.0.0 | [SPARK-12288](https://issues.apache.org/jira/browse/SPARK-12288) | Improvement | Support UnsafeRow in Coalesce/Except/Intersect |
| 2.0.0 | [SPARK-12289](https://issues.apache.org/jira/browse/SPARK-12289) | Improvement | Support UnsafeRow in TakeOrderedAndProject/Limit |
| 2.0.0 | [SPARK-12290](https://issues.apache.org/jira/browse/SPARK-12290) | Improvement | Change the default value in SparkPlan |
| 2.0.0 | [SPARK-12292](https://issues.apache.org/jira/browse/SPARK-12292) | Improvement | Support UnsafeRow in Generate |
| 2.0.0 | [SPARK-12293](https://issues.apache.org/jira/browse/SPARK-12293) | Improvement | Support UnsafeRow in LocalTableScan |
| 2.0.0 | [SPARK-12294](https://issues.apache.org/jira/browse/SPARK-12294) | Improvement | Support UnsafeRow in HiveTableScan |
| 2.0.0 | [SPARK-12362](https://issues.apache.org/jira/browse/SPARK-12362) | Improvement | Create a full-fledged built-in SQL parser |
| 2.0.0 | [SPARK-12398](https://issues.apache.org/jira/browse/SPARK-12398) | Improvement | Smart truncation of DataFrame / Dataset toString |
| 2.0.0 | [SPARK-12438](https://issues.apache.org/jira/browse/SPARK-12438) | Improvement | Add SQLUserDefinedType support for encoder |
| 2.0.0 | [SPARK-12492](https://issues.apache.org/jira/browse/SPARK-12492) | Improvement | SQL page of Spark-sql is always blank |
| 2.0.0 | [SPARK-12503](https://issues.apache.org/jira/browse/SPARK-12503) | Improvement | Pushdown a Limit on top of a Union |
| 2.0.0 | [SPARK-12515](https://issues.apache.org/jira/browse/SPARK-12515) | Improvement | Minor clarification on DataFrameReader.jdbc doc |
| 2.0.0 | [SPARK-12541](https://issues.apache.org/jira/browse/SPARK-12541) | New Feature | Support rollup/cube in SQL query |
| 2.0.0 | [SPARK-12543](https://issues.apache.org/jira/browse/SPARK-12543) | New Feature | Support subquery in select/where/having |
| 2.0.0 | [SPARK-12549](https://issues.apache.org/jira/browse/SPARK-12549) | Improvement | UDFs' input type specification should take Option[Seq[DataType]] rather than Seq[DataType] |
| 2.0.0 | [SPARK-12564](https://issues.apache.org/jira/browse/SPARK-12564) | Improvement | Improve missing column AnalysisException |
| 2.0.0 | [SPARK-12585](https://issues.apache.org/jira/browse/SPARK-12585) | Improvement | The numFields of UnsafeRow should not changed by pointTo() |
| 2.0.0 | [SPARK-12594](https://issues.apache.org/jira/browse/SPARK-12594) | Improvement | Outer Join Elimination by Filter Condition |
| 2.0.0 | [SPARK-12616](https://issues.apache.org/jira/browse/SPARK-12616) | Improvement | Union logical plan should support arbitrary number of children (rather than binary) |
| 2.0.0 | [SPARK-12636](https://issues.apache.org/jira/browse/SPARK-12636) | Improvement | Expose API on UnsafeRowRecordReader to just run on files |
| 2.0.0 | [SPARK-12700](https://issues.apache.org/jira/browse/SPARK-12700) | Improvement | SortMergeJoin and BroadcastHashJoin should support condition |
| 2.0.0 | [SPARK-12701](https://issues.apache.org/jira/browse/SPARK-12701) | Improvement | Logging FileAppender should use join to ensure thread is finished |
| 2.0.0 | [SPARK-12740](https://issues.apache.org/jira/browse/SPARK-12740) | Improvement | grouping()/grouping_id() should work with having and order by |
| 2.0.0 | [SPARK-12785](https://issues.apache.org/jira/browse/SPARK-12785) | New Feature | Implement columnar in memory representation |
| 2.0.0 | [SPARK-12795](https://issues.apache.org/jira/browse/SPARK-12795) | Epic | Whole stage codegen |
| 2.0.0 | [SPARK-12796](https://issues.apache.org/jira/browse/SPARK-12796) | New Feature | initial prototype: projection/filter/range |
| 2.0.0 | [SPARK-12797](https://issues.apache.org/jira/browse/SPARK-12797) | New Feature | Aggregation without grouping keys |
| 2.0.0 | [SPARK-12798](https://issues.apache.org/jira/browse/SPARK-12798) | New Feature | Broadcast hash join |
| 2.0.0 | [SPARK-12818](https://issues.apache.org/jira/browse/SPARK-12818) | New Feature | Implement Bloom filter and count-min sketch in DataFrames |
| 2.0.0 | [SPARK-12828](https://issues.apache.org/jira/browse/SPARK-12828) | New Feature | support natural join |
| 2.0.0 | [SPARK-12834](https://issues.apache.org/jira/browse/SPARK-12834) | Improvement | Use type conversion instead of Ser/De of Pickle to transform JavaArray and JavaList |
| 2.0.0 | [SPARK-12860](https://issues.apache.org/jira/browse/SPARK-12860) | Improvement | speed up safe projection for primitive types |
| 2.0.0 | [SPARK-12873](https://issues.apache.org/jira/browse/SPARK-12873) | Improvement | Add more comment in HiveTypeCoercion for type widening |
| 2.0.0 | [SPARK-12912](https://issues.apache.org/jira/browse/SPARK-12912) | Improvement | Add test suite for EliminateSubQueries |
| 2.0.0 | [SPARK-12926](https://issues.apache.org/jira/browse/SPARK-12926) | Improvement | SQLContext to display warning message when non-sql configs are being set |
| 2.0.0 | [SPARK-12932](https://issues.apache.org/jira/browse/SPARK-12932) | Improvement | Bad error message with trying to create Dataset from RDD of Java objects that are not bean-compliant |
| 2.0.0 | [SPARK-12957](https://issues.apache.org/jira/browse/SPARK-12957) | New Feature | Derive and propagate data constrains in logical plan |
| 2.0.0 | [SPARK-12976](https://issues.apache.org/jira/browse/SPARK-12976) | Improvement | Add LazilyGenerateOrdering and use it for RangePartitioner of Exchange. |
| 2.0.0 | [SPARK-13020](https://issues.apache.org/jira/browse/SPARK-13020) | Improvement | fix random generator for map type |
| 2.0.0 | [SPARK-13031](https://issues.apache.org/jira/browse/SPARK-13031) | Improvement | Improve test coverage for whole stage codegen |
| 2.0.0 | [SPARK-13098](https://issues.apache.org/jira/browse/SPARK-13098) | Improvement | remove GenericInternalRowWithSchema |
| 2.0.0 | [SPARK-13130](https://issues.apache.org/jira/browse/SPARK-13130) | Improvement | Make whole stage codegen variable names slightly easier to read |
| 2.0.0 | [SPARK-13168](https://issues.apache.org/jira/browse/SPARK-13168) | Improvement | Collapse adjacent Repartition operations |
| 2.0.0 | [SPARK-13215](https://issues.apache.org/jira/browse/SPARK-13215) | Improvement | Remove fallback in codegen |
| 2.0.0 | [SPARK-13235](https://issues.apache.org/jira/browse/SPARK-13235) | Improvement | Remove an Extra Distinct in Union |
| 2.0.0 | [SPARK-13237](https://issues.apache.org/jira/browse/SPARK-13237) | Improvement | Generate broadcast outer join |
| 2.0.0 | [SPARK-13249](https://issues.apache.org/jira/browse/SPARK-13249) | Improvement | Filter null keys for inner join |
| 2.0.0 | [SPARK-13329](https://issues.apache.org/jira/browse/SPARK-13329) | Improvement | Considering output for statistics of logical plan |
| 2.0.0 | [SPARK-13353](https://issues.apache.org/jira/browse/SPARK-13353) | Improvement | Fast serialization for collecting DataFrame |
| 2.0.0 | [SPARK-13373](https://issues.apache.org/jira/browse/SPARK-13373) | New Feature | Generate code for sort merge join |
| 2.0.0 | [SPARK-13376](https://issues.apache.org/jira/browse/SPARK-13376) | Improvement | Improve column pruning |
| 2.0.0 | [SPARK-13399](https://issues.apache.org/jira/browse/SPARK-13399) | Improvement | Investigate type erasure warnings in CheckpointSuite |
| 2.0.0 | [SPARK-13401](https://issues.apache.org/jira/browse/SPARK-13401) | Improvement | Fix SQL test warnings |
| 2.0.0 | [SPARK-13422](https://issues.apache.org/jira/browse/SPARK-13422) | Improvement | Use HashedRelation instead of HashSet in Left Semi Joins |
| 2.0.0 | [SPARK-13427](https://issues.apache.org/jira/browse/SPARK-13427) | Improvement | Support USING clause in JOIN |
| 2.0.0 | [SPARK-13457](https://issues.apache.org/jira/browse/SPARK-13457) | Improvement | Remove DataFrame RDD operations |
| 2.0.0 | [SPARK-13485](https://issues.apache.org/jira/browse/SPARK-13485) | Improvement | (Dataset-oriented) API evolution in Spark 2.0 |
| 2.0.0 | [SPARK-13511](https://issues.apache.org/jira/browse/SPARK-13511) | Improvement | Add wholestage codegen for limit |
| 2.0.0 | [SPARK-13523](https://issues.apache.org/jira/browse/SPARK-13523) | New Feature | Reuse the exchanges in a query |
| 2.0.0 | [SPARK-13544](https://issues.apache.org/jira/browse/SPARK-13544) | Improvement | Rewrite/Propagate constraints for Aliases in Aggregate |
| 2.0.0 | [SPARK-13549](https://issues.apache.org/jira/browse/SPARK-13549) | Improvement | Refactor the Optimizer Rule CollapseProject |
| 2.0.0 | [SPARK-13616](https://issues.apache.org/jira/browse/SPARK-13616) | Improvement | Let SQLBuilder convert logical plan without a Project on top of it |
| 2.0.0 | [SPARK-13635](https://issues.apache.org/jira/browse/SPARK-13635) | Improvement | Enable LimitPushdown optimizer rule because we have whole-stage codegen for Limit |
| 2.0.0 | [SPARK-13647](https://issues.apache.org/jira/browse/SPARK-13647) | Improvement | also check if numeric value is within allowed range in _verify_type |
| 2.0.0 | [SPARK-13661](https://issues.apache.org/jira/browse/SPARK-13661) | Improvement | Avoid the copy of UnsafeRow in HashedRelation |
| 2.0.0 | [SPARK-13668](https://issues.apache.org/jira/browse/SPARK-13668) | Improvement | Reorder filter/join predicates to short-circuit isNotNull checks |
| 2.0.0 | [SPARK-13674](https://issues.apache.org/jira/browse/SPARK-13674) | Improvement | Add wholestage codegen support to Sample |
| 2.0.0 | [SPARK-13740](https://issues.apache.org/jira/browse/SPARK-13740) | Improvement | add null check for _verify_type in types.py |
| 2.0.0 | [SPARK-13745](https://issues.apache.org/jira/browse/SPARK-13745) | Improvement | Support columnar in memory representation on Big Endian platforms |
| 2.0.0 | [SPARK-13749](https://issues.apache.org/jira/browse/SPARK-13749) | Improvement | Faster pivot implementation for many distinct values with two phase aggregation |
| 2.0.0 | [SPARK-13790](https://issues.apache.org/jira/browse/SPARK-13790) | Improvement | Speed up ColumnVector's getDecimal |
| 2.0.0 | [SPARK-13797](https://issues.apache.org/jira/browse/SPARK-13797) | Improvement | Eliminate Unnecessary Window |
| 2.0.0 | [SPARK-13805](https://issues.apache.org/jira/browse/SPARK-13805) | Improvement | Direct consume ColumnVector in generated code when ColumnarBatch is used |
| 2.0.0 | [SPARK-13822](https://issues.apache.org/jira/browse/SPARK-13822) | Improvement | Follow-ups of DataFrame/Dataset API unification |
| 2.0.0 | [SPARK-13844](https://issues.apache.org/jira/browse/SPARK-13844) | Improvement | Generate better code for filters with a non-nullable column |
| 2.0.0 | [SPARK-13873](https://issues.apache.org/jira/browse/SPARK-13873) | Improvement | Avoid the copy in whole stage codegen when there is no joins |
| 2.0.0 | [SPARK-13916](https://issues.apache.org/jira/browse/SPARK-13916) | Improvement | For whole stage codegen, measure and add the execution duration as a metric |
| 2.0.0 | [SPARK-13917](https://issues.apache.org/jira/browse/SPARK-13917) | New Feature | Generate code for broadcast left semi join |
| 2.0.0 | [SPARK-13919](https://issues.apache.org/jira/browse/SPARK-13919) | Improvement | Resolving the Conflicts of ColumnPruning and PushPredicateThroughProject |
| 2.0.0 | [SPARK-13926](https://issues.apache.org/jira/browse/SPARK-13926) | Improvement | Automatically use Kryo serializer when shuffling RDDs with simple types |
| 2.0.0 | [SPARK-13930](https://issues.apache.org/jira/browse/SPARK-13930) | Improvement | Apply fast serialization on collect limit |
| 2.0.0 | [SPARK-13957](https://issues.apache.org/jira/browse/SPARK-13957) | Improvement | Support group by ordinal in SQL |
| 2.0.0 | [SPARK-13974](https://issues.apache.org/jira/browse/SPARK-13974) | Improvement | sub-query names do not need to be globally unique while generate SQL |
| 2.0.0 | [SPARK-13996](https://issues.apache.org/jira/browse/SPARK-13996) | Improvement | Add more not null attributes for Filter codegen |
| 2.0.0 | [SPARK-14019](https://issues.apache.org/jira/browse/SPARK-14019) | Improvement | Remove noop SortOrder in Sort |
| 2.0.0 | [SPARK-14039](https://issues.apache.org/jira/browse/SPARK-14039) | Improvement | make SubqueryHolder an inner class |
| 2.0.0 | [SPARK-14042](https://issues.apache.org/jira/browse/SPARK-14042) | New Feature | Add support for custom coalescers |
| 2.0.0 | [SPARK-14060](https://issues.apache.org/jira/browse/SPARK-14060) | Improvement | Move StringToColumn implicit class into SQLImplicits |
| 2.0.0 | [SPARK-14155](https://issues.apache.org/jira/browse/SPARK-14155) | Improvement | Hide UserDefinedType in Spark 2.0 |
| 2.0.0 | [SPARK-14157](https://issues.apache.org/jira/browse/SPARK-14157) | Improvement | Parse Drop Function DDL command |
| 2.0.0 | [SPARK-14175](https://issues.apache.org/jira/browse/SPARK-14175) | Improvement | Simplify whole stage codegen interface |
| 2.0.0 | [SPARK-14225](https://issues.apache.org/jira/browse/SPARK-14225) | Improvement | Cap the length of toCommentSafeString at 128 chars |
| 2.0.0 | [SPARK-14251](https://issues.apache.org/jira/browse/SPARK-14251) | Improvement | Add SQL command for printing out generated code for debugging |
| 2.0.0 | [SPARK-14259](https://issues.apache.org/jira/browse/SPARK-14259) | Improvement | Add config to control maximum number of files when coalescing partitions |
| 2.0.0 | [SPARK-14268](https://issues.apache.org/jira/browse/SPARK-14268) | Improvement | rename toRowExpressions and fromRowExpression to serializer and deserializer in ExpressionEncoder |
| 2.0.0 | [SPARK-14270](https://issues.apache.org/jira/browse/SPARK-14270) | Improvement | whole stage codegen support for typed filter |
| 2.0.0 | [SPARK-14275](https://issues.apache.org/jira/browse/SPARK-14275) | Improvement | Reimplement TypedAggregateExpression to DeclarativeAggregate |
| 2.0.0 | [SPARK-14296](https://issues.apache.org/jira/browse/SPARK-14296) | Improvement | whole stage codegen support for Dataset.map |
| 2.0.0 | [SPARK-14317](https://issues.apache.org/jira/browse/SPARK-14317) | Improvement | Clean up hash join |
| 2.0.0 | [SPARK-14334](https://issues.apache.org/jira/browse/SPARK-14334) | New Feature | Add toLocalIterator for Dataset |
| 2.0.0 | [SPARK-14338](https://issues.apache.org/jira/browse/SPARK-14338) | Improvement | Improve `SimplifyConditionals` rule to handle `null` in IF/CASEWHEN |
| 2.0.0 | [SPARK-14353](https://issues.apache.org/jira/browse/SPARK-14353) | New Feature | Dateset Time Windowing API for Python, R, and SQL |
| 2.0.0 | [SPARK-14362](https://issues.apache.org/jira/browse/SPARK-14362) | Improvement | DDL Native Support: Drop View |
| 2.0.0 | [SPARK-14370](https://issues.apache.org/jira/browse/SPARK-14370) | Improvement | Avoid creating duplicate ids in OnlineLDAOptimizer |
| 2.0.0 | [SPARK-14372](https://issues.apache.org/jira/browse/SPARK-14372) | Improvement | Dataset.randomSplit() needs a Java version |
| 2.0.0 | [SPARK-14419](https://issues.apache.org/jira/browse/SPARK-14419) | Improvement | Improve the HashedRelation for key fit within Long |
| 2.0.0 | [SPARK-14422](https://issues.apache.org/jira/browse/SPARK-14422) | Improvement | Improve handling of optional configs in SQLConf |
| 2.0.0 | [SPARK-14426](https://issues.apache.org/jira/browse/SPARK-14426) | Improvement | Merge ParserUtils and ParseUtils |
| 2.0.0 | [SPARK-14448](https://issues.apache.org/jira/browse/SPARK-14448) | Improvement | Improvements to ColumnVector |
| 2.0.0 | [SPARK-14518](https://issues.apache.org/jira/browse/SPARK-14518) | Improvement | Support Comment in CREATE VIEW |
| 2.0.0 | [SPARK-14545](https://issues.apache.org/jira/browse/SPARK-14545) | New Feature | Improve `LikeSimplification` by adding `a%b` rule |
| 2.0.0 | [SPARK-14548](https://issues.apache.org/jira/browse/SPARK-14548) | Improvement | Support !> and !< operator in Spark SQL |
| 2.0.0 | [SPARK-14562](https://issues.apache.org/jira/browse/SPARK-14562) | Improvement | Improve constraints propagation in Union |
| 2.0.0 | [SPARK-14577](https://issues.apache.org/jira/browse/SPARK-14577) | Improvement | spark.sql.codegen.maxCaseBranches config option |
| 2.0.0 | [SPARK-14600](https://issues.apache.org/jira/browse/SPARK-14600) | Improvement | Push predicates through Expand |
| 2.0.0 | [SPARK-14630](https://issues.apache.org/jira/browse/SPARK-14630) | Improvement | Code style: public abstract methods should have explicit return types |
| 2.0.0 | [SPARK-14696](https://issues.apache.org/jira/browse/SPARK-14696) | Improvement | Needs implicit encoders for boxed primitive types |
| 2.0.0 | [SPARK-14710](https://issues.apache.org/jira/browse/SPARK-14710) | Improvement | Rename gen/genCode to genCode/doGenCode to better reflect the semantics |
| 2.0.0 | [SPARK-14718](https://issues.apache.org/jira/browse/SPARK-14718) | Improvement | Avoid mutating ExprCode in doGenCode |
| 2.0.0 | [SPARK-14722](https://issues.apache.org/jira/browse/SPARK-14722) | Improvement | Rename upstreams() -> inputRDDs() in WholeStageCodegen |
| 2.0.0 | [SPARK-14724](https://issues.apache.org/jira/browse/SPARK-14724) | Improvement | Improve performance of sorting by using radix sort when possible |
| 2.0.0 | [SPARK-14747](https://issues.apache.org/jira/browse/SPARK-14747) | Improvement | Add assertStreaming/assertNoneStreaming checks in DataFrameWriter |
| 2.0.0 | [SPARK-14781](https://issues.apache.org/jira/browse/SPARK-14781) | New Feature | Support subquery in nested predicates |
| 2.0.0 | [SPARK-14785](https://issues.apache.org/jira/browse/SPARK-14785) | New Feature | Support correlated scalar subquery |
| 2.0.0 | [SPARK-14796](https://issues.apache.org/jira/browse/SPARK-14796) | Improvement | Add spark.sql.optimizer.inSetConversionThreshold config option |
| 2.0.0 | [SPARK-14830](https://issues.apache.org/jira/browse/SPARK-14830) | Improvement | Add RemoveRepetitionFromGroupExpressions optimizer |
| 2.0.0 | [SPARK-14853](https://issues.apache.org/jira/browse/SPARK-14853) | Improvement | Support LeftSemi/LeftAnti in SortMergeJoin |
| 2.0.0 | [SPARK-14858](https://issues.apache.org/jira/browse/SPARK-14858) | Improvement | Push predicates with subquery |
| 2.0.0 | [SPARK-14866](https://issues.apache.org/jira/browse/SPARK-14866) | Improvement | Break SQLQuerySuite out into smaller test suites |
| 2.0.0 | [SPARK-14869](https://issues.apache.org/jira/browse/SPARK-14869) | Improvement | Don't mask exceptions in ResolveRelations |
| 2.0.0 | [SPARK-14871](https://issues.apache.org/jira/browse/SPARK-14871) | Improvement | Disable StatsReportListener to declutter output |
| 2.0.0 | [SPARK-14939](https://issues.apache.org/jira/browse/SPARK-14939) | Improvement | Add FoldablePropagation optimizer |
| 2.0.0 | [SPARK-14961](https://issues.apache.org/jira/browse/SPARK-14961) | Improvement | Support LongToUnsafeRowMap larger than 1G |
| 2.0.0 | [SPARK-14972](https://issues.apache.org/jira/browse/SPARK-14972) | Improvement | Improve performance of JSON schema inference's inferField step |
| 2.0.0 | [SPARK-15047](https://issues.apache.org/jira/browse/SPARK-15047) | Improvement | Cleanup SQLParser |
| 2.0.0 | [SPARK-15171](https://issues.apache.org/jira/browse/SPARK-15171) | New Feature | Deprecate registerTempTable and add dataset.createTempView |
| 2.0.0 | [SPARK-15205](https://issues.apache.org/jira/browse/SPARK-15205) | Improvement | Codegen can compile the same source code more than twice |
| 2.0.0 | [SPARK-15210](https://issues.apache.org/jira/browse/SPARK-15210) | Improvement | Add missing @DeveloperApi annotation in sql.types |
| 2.0.0 | [SPARK-15225](https://issues.apache.org/jira/browse/SPARK-15225) | Improvement | Replace SQLContext with SparkSession in Encoder documentation |
| 2.0.0 | [SPARK-15250](https://issues.apache.org/jira/browse/SPARK-15250) | Improvement | Remove deprecated json API in DataFrameReader |
| 2.0.0 | [SPARK-15255](https://issues.apache.org/jira/browse/SPARK-15255) | Improvement | RDD name from DataFrame op should not include full local relation data |
| 2.0.0 | [SPARK-15310](https://issues.apache.org/jira/browse/SPARK-15310) | Improvement | Rename HiveTypeCoercion -> TypeCoercion |
| 2.0.0 | [SPARK-15419](https://issues.apache.org/jira/browse/SPARK-15419) | Improvement | monotonicallyIncreasingId should use less memory with multiple partitions |
| 2.0.0 | [SPARK-15426](https://issues.apache.org/jira/browse/SPARK-15426) | Improvement | Spark 2.0 SQL API audit |
| 2.0.0 | [SPARK-15431](https://issues.apache.org/jira/browse/SPARK-15431) | Improvement | Support LIST FILE(s)\|JAR(s) command natively |
| 2.0.0 | [SPARK-15438](https://issues.apache.org/jira/browse/SPARK-15438) | Improvement | Improve the explain of whole-stage codegen |
| 2.0.0 | [SPARK-15471](https://issues.apache.org/jira/browse/SPARK-15471) | Improvement | ScalaReflection cleanup |
| 2.0.0 | [SPARK-15696](https://issues.apache.org/jira/browse/SPARK-15696) | Improvement | Improve `crosstab` to have a consistent column order |
| 2.0.0 | [SPARK-15733](https://issues.apache.org/jira/browse/SPARK-15733) | Improvement | Makes the explain output less verbose by hiding some verbose output like None, null, empty List, and etc.. |
| 2.0.0 | [SPARK-15734](https://issues.apache.org/jira/browse/SPARK-15734) | Improvement | Avoids printing internal row in explain output |
| 2.0.0 | [SPARK-15753](https://issues.apache.org/jira/browse/SPARK-15753) | Improvement | Move some Analyzer stuff to Analyzer from DataFrameWriter |
| 2.0.0 | [SPARK-15759](https://issues.apache.org/jira/browse/SPARK-15759) | Improvement | Fallback to non-codegen if fail to compile generated code |
| 2.0.0 | [SPARK-15789](https://issues.apache.org/jira/browse/SPARK-15789) | Improvement | Allow reserved keywords in most places |
| 2.0.0 | [SPARK-15792](https://issues.apache.org/jira/browse/SPARK-15792) | Improvement | [SQL] Allows operator to change the verbosity in explain output. |
| 2.0.0 | [SPARK-15807](https://issues.apache.org/jira/browse/SPARK-15807) | Improvement | Support varargs for dropDuplicates in Dataset/DataFrame |
| 2.0.0 | [SPARK-15871](https://issues.apache.org/jira/browse/SPARK-15871) | Improvement | Add assertNotPartitioned check in DataFrameWriter |
| 2.0.0 | [SPARK-16135](https://issues.apache.org/jira/browse/SPARK-16135) | Improvement | Remove hashCode and euqals in ArrayBasedMapData |
| 2.0.0 | [SPARK-16192](https://issues.apache.org/jira/browse/SPARK-16192) | Improvement | Improve the type check of CollectSet in CheckAnalysis |
| 2.0.0 | [SPARK-16476](https://issues.apache.org/jira/browse/SPARK-16476) | Improvement | Restructure MimaExcludes for easier union excludes |
| 2.0.0 | [SPARK-16615](https://issues.apache.org/jira/browse/SPARK-16615) | Improvement | Expose sqlContext in SparkSession |
| 2.0.1 | [SPARK-16568](https://issues.apache.org/jira/browse/SPARK-16568) | Improvement | update sql programing guide refreshTable API |
| 2.0.1 | [SPARK-16651](https://issues.apache.org/jira/browse/SPARK-16651) | Improvement | Document no exception using DataFrame.withColumnRenamed when existing column doesn't exist |
| 2.0.1 | [SPARK-16813](https://issues.apache.org/jira/browse/SPARK-16813) | Improvement | Remove private[sql] and private[spark] from catalyst package |
| 2.0.1 | [SPARK-16865](https://issues.apache.org/jira/browse/SPARK-16865) | Improvement | A file-based end-to-end SQL query suite |
| 2.0.1 | [SPARK-16875](https://issues.apache.org/jira/browse/SPARK-16875) | Improvement | Add args checking for DataSet randomSplit and sample |
| 2.0.1 | [SPARK-16964](https://issues.apache.org/jira/browse/SPARK-16964) | Improvement | Remove private[sql] and private[spark] from sql.execution package |
| 2.0.1 | [SPARK-17084](https://issues.apache.org/jira/browse/SPARK-17084) | Improvement | Rename ParserUtils.assert to validate |
| 2.0.1 | [SPARK-17279](https://issues.apache.org/jira/browse/SPARK-17279) | Improvement | better error message for exceptions during ScalaUDF execution |
| 2.0.1 | [SPARK-17301](https://issues.apache.org/jira/browse/SPARK-17301) | Improvement | Remove unused classTag field from AtomicType base class |
| 2.0.1 | [SPARK-17347](https://issues.apache.org/jira/browse/SPARK-17347) | Improvement | Encoder in Dataset example has incorrect type |
| 2.0.2 | [SPARK-16343](https://issues.apache.org/jira/browse/SPARK-16343) | Improvement | Improve the PushDownPredicate rule to pushdown predicates currectly in non-deterministic condition |
| 2.0.2 | [SPARK-17751](https://issues.apache.org/jira/browse/SPARK-17751) | Improvement | Remove spark.sql.eagerAnalysis |
| 2.1.0 | [SPARK-10601](https://issues.apache.org/jira/browse/SPARK-10601) | Improvement | Spark SQL - Support for MINUS |
| 2.1.0 | [SPARK-10747](https://issues.apache.org/jira/browse/SPARK-10747) | New Feature | add support for NULLS FIRST\|LAST in ORDER BY clause |
| 2.1.0 | [SPARK-12639](https://issues.apache.org/jira/browse/SPARK-12639) | Improvement | Improve Explain for DataSources with Handled Predicate Pushdowns |
| 2.1.0 | [SPARK-13417](https://issues.apache.org/jira/browse/SPARK-13417) | Umbrella | SQL subquery support |
| 2.1.0 | [SPARK-14839](https://issues.apache.org/jira/browse/SPARK-14839) | Improvement | Support for other types as option in OPTIONS clause |
| 2.1.0 | [SPARK-14851](https://issues.apache.org/jira/browse/SPARK-14851) | Improvement | Support radix sort with nullable longs |
| 2.1.0 | [SPARK-15076](https://issues.apache.org/jira/browse/SPARK-15076) | Improvement | Add ReorderAssociativeOperator optimizer |
| 2.1.0 | [SPARK-15204](https://issues.apache.org/jira/browse/SPARK-15204) | Improvement | Improve nullability inference for Aggregator |
| 2.1.0 | [SPARK-15752](https://issues.apache.org/jira/browse/SPARK-15752) | Improvement | Optimize metadata only query that has an aggregate whose children are deterministic project or filter operators |
| 2.1.0 | [SPARK-15962](https://issues.apache.org/jira/browse/SPARK-15962) | Improvement | Introduce additonal implementation with a dense format for UnsafeArrayData |
| 2.1.0 | [SPARK-15985](https://issues.apache.org/jira/browse/SPARK-15985) | Improvement | Reduce runtime overhead of a program that reads an primitive array in Dataset |
| 2.1.0 | [SPARK-16052](https://issues.apache.org/jira/browse/SPARK-16052) | Improvement | Improve `CollapseRepartition` optimizer for Repartition/RepartitionBy |
| 2.1.0 | [SPARK-16063](https://issues.apache.org/jira/browse/SPARK-16063) | Improvement | Add storageLevel to Dataset |
| 2.1.0 | [SPARK-16115](https://issues.apache.org/jira/browse/SPARK-16115) | Improvement | Improve output column name for SHOW PARTITIONS command and improve an error message |
| 2.1.0 | [SPARK-16128](https://issues.apache.org/jira/browse/SPARK-16128) | Improvement | Allow setting length of characters to be truncated to, in Dataset.show function. |
| 2.1.0 | [SPARK-16134](https://issues.apache.org/jira/browse/SPARK-16134) | Improvement | optimizer rules for typed filter |
| 2.1.0 | [SPARK-16174](https://issues.apache.org/jira/browse/SPARK-16174) | Improvement | Improve `OptimizeIn` optimizer to remove literal repetitions |
| 2.1.0 | [SPARK-16186](https://issues.apache.org/jira/browse/SPARK-16186) | Improvement | Support partition batch pruning with `IN` predicate in InMemoryTableScanExec |
| 2.1.0 | [SPARK-16189](https://issues.apache.org/jira/browse/SPARK-16189) | Improvement | Add ExistingRDD logical plan for input with RDD to have a chance to eliminate serialize/deserialize. |
| 2.1.0 | [SPARK-16199](https://issues.apache.org/jira/browse/SPARK-16199) | Improvement | Add a method to list the referenced columns in data source Filter |
| 2.1.0 | [SPARK-16208](https://issues.apache.org/jira/browse/SPARK-16208) | Improvement | Add `PropagateEmptyRelation` optimizer |
| 2.1.0 | [SPARK-16302](https://issues.apache.org/jira/browse/SPARK-16302) | Improvement | Set the right number of partitions for reading data from a local collection. |
| 2.1.0 | [SPARK-16343](https://issues.apache.org/jira/browse/SPARK-16343) | Improvement | Improve the PushDownPredicate rule to pushdown predicates currectly in non-deterministic condition |
| 2.1.0 | [SPARK-16351](https://issues.apache.org/jira/browse/SPARK-16351) | Improvement | Avoid record-per type dispatch in JSON when writing |
| 2.1.0 | [SPARK-16360](https://issues.apache.org/jira/browse/SPARK-16360) | Improvement | Speed up SQL query performance by removing redundant `executePlan` call in `Dataset` |
| 2.1.0 | [SPARK-16429](https://issues.apache.org/jira/browse/SPARK-16429) | Improvement | Include `StringType` columns in `describe()` |
| 2.1.0 | [SPARK-16434](https://issues.apache.org/jira/browse/SPARK-16434) | Improvement | Avoid record-per type dispatch in JSON when reading |
| 2.1.0 | [SPARK-16461](https://issues.apache.org/jira/browse/SPARK-16461) | Improvement | Support partition batch pruning with `<=>` (EqualNullSafe) predicate in InMemoryTableScanExec |
| 2.1.0 | [SPARK-16543](https://issues.apache.org/jira/browse/SPARK-16543) | Improvement | Rename the columns of `SHOW PARTITION/COLUMNS` commands |
| 2.1.0 | [SPARK-16568](https://issues.apache.org/jira/browse/SPARK-16568) | Improvement | update sql programing guide refreshTable API |
| 2.1.0 | [SPARK-16640](https://issues.apache.org/jira/browse/SPARK-16640) | Improvement | Add codegen for Elt function |
| 2.1.0 | [SPARK-16671](https://issues.apache.org/jira/browse/SPARK-16671) | Improvement | Merge variable substitution code in core and SQL |
| 2.1.0 | [SPARK-16697](https://issues.apache.org/jira/browse/SPARK-16697) | Improvement | redundant RDD computation in LDAOptimizer |
| 2.1.0 | [SPARK-16726](https://issues.apache.org/jira/browse/SPARK-16726) | Improvement | Improve `Union/Intersect/Except` error messages on incompatible types |
| 2.1.0 | [SPARK-16749](https://issues.apache.org/jira/browse/SPARK-16749) | Improvement | Clean-up OffsetWindowFrame |
| 2.1.0 | [SPARK-16813](https://issues.apache.org/jira/browse/SPARK-16813) | Improvement | Remove private[sql] and private[spark] from catalyst package |
| 2.1.0 | [SPARK-16862](https://issues.apache.org/jira/browse/SPARK-16862) | Improvement | Configurable buffer size in `UnsafeSorterSpillReader` |
| 2.1.0 | [SPARK-16865](https://issues.apache.org/jira/browse/SPARK-16865) | Improvement | A file-based end-to-end SQL query suite |
| 2.1.0 | [SPARK-16875](https://issues.apache.org/jira/browse/SPARK-16875) | Improvement | Add args checking for DataSet randomSplit and sample |
| 2.1.0 | [SPARK-16916](https://issues.apache.org/jira/browse/SPARK-16916) | Improvement | serde/storage properties should not have limitations |
| 2.1.0 | [SPARK-16928](https://issues.apache.org/jira/browse/SPARK-16928) | Improvement | Recursive call of ColumnVector::getInt() breaks JIT inlining |
| 2.1.0 | [SPARK-16958](https://issues.apache.org/jira/browse/SPARK-16958) | New Feature | Reuse subqueries within single query |
| 2.1.0 | [SPARK-16964](https://issues.apache.org/jira/browse/SPARK-16964) | Improvement | Remove private[sql] and private[spark] from sql.execution package |
| 2.1.0 | [SPARK-17021](https://issues.apache.org/jira/browse/SPARK-17021) | Improvement | simplify the constructor parameters of QuantileSummaries |
| 2.1.0 | [SPARK-17032](https://issues.apache.org/jira/browse/SPARK-17032) | Improvement | Add test cases for methods in ParserUtils |
| 2.1.0 | [SPARK-17033](https://issues.apache.org/jira/browse/SPARK-17033) | Improvement | GaussianMixture should use treeAggregate to improve performance |
| 2.1.0 | [SPARK-17084](https://issues.apache.org/jira/browse/SPARK-17084) | Improvement | Rename ParserUtils.assert to validate |
| 2.1.0 | [SPARK-17106](https://issues.apache.org/jira/browse/SPARK-17106) | Improvement | Simplify subquery interface |
| 2.1.0 | [SPARK-17107](https://issues.apache.org/jira/browse/SPARK-17107) | Improvement | Remove redundant pushdown rule for Union |
| 2.1.0 | [SPARK-17144](https://issues.apache.org/jira/browse/SPARK-17144) | Improvement | Removal of useless CreateHiveTableAsSelectLogicalPlan |
| 2.1.0 | [SPARK-17187](https://issues.apache.org/jira/browse/SPARK-17187) | New Feature | Support using arbitrary Java object as internal aggregation buffer object |
| 2.1.0 | [SPARK-17192](https://issues.apache.org/jira/browse/SPARK-17192) | Improvement | Issuing an exception when users specify the partitioning columns without a given schema |
| 2.1.0 | [SPARK-17199](https://issues.apache.org/jira/browse/SPARK-17199) | Improvement | Use CatalystConf.resolver for case-sensitivity comparison |
| 2.1.0 | [SPARK-17215](https://issues.apache.org/jira/browse/SPARK-17215) | Improvement | Method `SQLContext.parseDataType(dataTypeString: String)` could be removed. |
| 2.1.0 | [SPARK-17263](https://issues.apache.org/jira/browse/SPARK-17263) | Improvement | Support binary literals in SQL |
| 2.1.0 | [SPARK-17268](https://issues.apache.org/jira/browse/SPARK-17268) | Improvement | Break Optimizer.scala apart |
| 2.1.0 | [SPARK-17279](https://issues.apache.org/jira/browse/SPARK-17279) | Improvement | better error message for exceptions during ScalaUDF execution |
| 2.1.0 | [SPARK-17298](https://issues.apache.org/jira/browse/SPARK-17298) | Story | Require explicit CROSS join for cartesian products by default |
| 2.1.0 | [SPARK-17301](https://issues.apache.org/jira/browse/SPARK-17301) | Improvement | Remove unused classTag field from AtomicType base class |
| 2.1.0 | [SPARK-17338](https://issues.apache.org/jira/browse/SPARK-17338) | New Feature | Add global temp view support |
| 2.1.0 | [SPARK-17347](https://issues.apache.org/jira/browse/SPARK-17347) | Improvement | Encoder in Dataset example has incorrect type |
| 2.1.0 | [SPARK-17388](https://issues.apache.org/jira/browse/SPARK-17388) | Improvement | Support for inferring type date/timestamp/decimal for partition column |
| 2.1.0 | [SPARK-17415](https://issues.apache.org/jira/browse/SPARK-17415) | Improvement | Better error message for driver-side broadcast join OOMs |
| 2.1.0 | [SPARK-17447](https://issues.apache.org/jira/browse/SPARK-17447) | Improvement | performance improvement in Partitioner.DefaultPartitioner |
| 2.1.0 | [SPARK-17529](https://issues.apache.org/jira/browse/SPARK-17529) | Improvement | On highly skewed data, outer join merges are slow |
| 2.1.0 | [SPARK-17530](https://issues.apache.org/jira/browse/SPARK-17530) | Improvement | Add Statistics into DESCRIBE FORMATTED |
| 2.1.0 | [SPARK-17590](https://issues.apache.org/jira/browse/SPARK-17590) | Improvement | Analyze CTE definitions at once and allow CTE subquery to define CTE |
| 2.1.0 | [SPARK-17614](https://issues.apache.org/jira/browse/SPARK-17614) | Improvement | sparkSession.read() .jdbc(***) use the sql syntax "where 1=0" that Cassandra does not support |
| 2.1.0 | [SPARK-17623](https://issues.apache.org/jira/browse/SPARK-17623) | Improvement | Failed tasks end reason is always a TaskFailedReason, types should reflect this |
| 2.1.0 | [SPARK-17653](https://issues.apache.org/jira/browse/SPARK-17653) | Improvement | Optimizer should remove unnecessary distincts (in multiple unions) |
| 2.1.0 | [SPARK-17677](https://issues.apache.org/jira/browse/SPARK-17677) | Improvement | Break WindowExec.scala into multiple files |
| 2.1.0 | [SPARK-17720](https://issues.apache.org/jira/browse/SPARK-17720) | New Feature | Static configurations in SQL |
| 2.1.0 | [SPARK-17739](https://issues.apache.org/jira/browse/SPARK-17739) | Improvement | Collapse adjacent similar Window operators |
| 2.1.0 | [SPARK-17751](https://issues.apache.org/jira/browse/SPARK-17751) | Improvement | Remove spark.sql.eagerAnalysis |
| 2.1.0 | [SPARK-17761](https://issues.apache.org/jira/browse/SPARK-17761) | Improvement | Simplify InternalRow hierarchy |
| 2.1.0 | [SPARK-17844](https://issues.apache.org/jira/browse/SPARK-17844) | Improvement | DataFrame API should simplify defining frame boundaries without partitioning/ordering |
| 2.1.0 | [SPARK-17848](https://issues.apache.org/jira/browse/SPARK-17848) | Improvement | Move LabelCol datatype cast into Predictor.fit |
| 2.1.0 | [SPARK-17861](https://issues.apache.org/jira/browse/SPARK-17861) | Improvement | Store data source partitions in metastore and push partition pruning into metastore |
| 2.1.0 | [SPARK-17955](https://issues.apache.org/jira/browse/SPARK-17955) | Improvement | Use the same read path in DataFrameReader.jdbc and DataFrameReader.format("jdbc") |
| 2.1.0 | [SPARK-18038](https://issues.apache.org/jira/browse/SPARK-18038) | Improvement | Move output partitioning definition from UnaryNodeExec to its children |
| 2.1.0 | [SPARK-18179](https://issues.apache.org/jira/browse/SPARK-18179) | Improvement | Throws analysis exception with a proper message for unsupported argument types in reflect/java_method function |
| 2.1.0 | [SPARK-18197](https://issues.apache.org/jira/browse/SPARK-18197) | Improvement | Optimise AppendOnlyMap implementation |
| 2.1.0 | [SPARK-18215](https://issues.apache.org/jira/browse/SPARK-18215) | Improvement | Make Column.expr public |
| 2.1.0 | [SPARK-18216](https://issues.apache.org/jira/browse/SPARK-18216) | Improvement | Make Column.expr public |
| 2.1.0 | [SPARK-18261](https://issues.apache.org/jira/browse/SPARK-18261) | New Feature | Add statistics to MemorySink for joining |
| 2.1.0 | [SPARK-18351](https://issues.apache.org/jira/browse/SPARK-18351) | New Feature | from_json and to_json for parsing JSON for string columns |
| 2.1.0 | [SPARK-18396](https://issues.apache.org/jira/browse/SPARK-18396) | Improvement | "Duration" column makes search result confused, maybe we should make it unsearchable |
| 2.1.0 | [SPARK-18398](https://issues.apache.org/jira/browse/SPARK-18398) | Improvement | Fix nullabilities of MapObjects and optimize not to check null if lambda is not nullable. |
| 2.1.0 | [SPARK-18467](https://issues.apache.org/jira/browse/SPARK-18467) | Improvement | Refactor StaticInvoke, Invoke and NewInstance. |
| 2.1.0 | [SPARK-18516](https://issues.apache.org/jira/browse/SPARK-18516) | Improvement | Separate instantaneous state from progress performance statistics |
| 2.1.0 | [SPARK-18604](https://issues.apache.org/jira/browse/SPARK-18604) | Improvement | Collapse Window optimizer rule changes column order |
| 2.1.0 | [SPARK-18674](https://issues.apache.org/jira/browse/SPARK-18674) | Improvement | improve the error message of using join |
| 2.1.0 | [SPARK-18729](https://issues.apache.org/jira/browse/SPARK-18729) | Improvement | MemorySink should not call DataFrame.collect when holding a lock |
| 2.1.0 | [SPARK-18869](https://issues.apache.org/jira/browse/SPARK-18869) | New Feature | Add TreeNode.p that returns BaseType |
| 2.2.0 | [SPARK-13721](https://issues.apache.org/jira/browse/SPARK-13721) | Improvement | Add support for LATERAL VIEW OUTER explode() |
| 2.2.0 | [SPARK-13748](https://issues.apache.org/jira/browse/SPARK-13748) | Improvement | Document behavior of createDataFrame and rows with omitted fields |
| 2.2.0 | [SPARK-14584](https://issues.apache.org/jira/browse/SPARK-14584) | Improvement | Improve recognition of non-nullability in Dataset transformations |
| 2.2.0 | [SPARK-16213](https://issues.apache.org/jira/browse/SPARK-16213) | Improvement | Reduce runtime overhead of a program that creates an primitive array in DataFrame |
| 2.2.0 | [SPARK-16475](https://issues.apache.org/jira/browse/SPARK-16475) | Improvement | Broadcast Hint for SQL Queries |
| 2.2.0 | [SPARK-17626](https://issues.apache.org/jira/browse/SPARK-17626) | Umbrella | TPC-DS performance improvements using star-schema heuristics |
| 2.2.0 | [SPARK-17838](https://issues.apache.org/jira/browse/SPARK-17838) | Improvement | Strict type checking for arguments with a better messages across APIs. |
| 2.2.0 | [SPARK-17868](https://issues.apache.org/jira/browse/SPARK-17868) | Improvement | Do not use bitmasks during parsing and analysis of CUBE/ROLLUP/GROUPING SETS |
| 2.2.0 | [SPARK-17912](https://issues.apache.org/jira/browse/SPARK-17912) | Improvement | Refactor code generation to get data for ColumnVector/ColumnarBatch |
| 2.2.0 | [SPARK-17949](https://issues.apache.org/jira/browse/SPARK-17949) | Improvement | Introduce a JVM object based aggregate operator |
| 2.2.0 | [SPARK-18471](https://issues.apache.org/jira/browse/SPARK-18471) | Improvement | In treeAggregate, generate (big) zeros instead of sending them. |
| 2.2.0 | [SPARK-18632](https://issues.apache.org/jira/browse/SPARK-18632) | Improvement | AggregateFunction should not ImplicitCastInputTypes |
| 2.2.0 | [SPARK-18775](https://issues.apache.org/jira/browse/SPARK-18775) | New Feature | Limit the max number of records written per file |
| 2.2.0 | [SPARK-18800](https://issues.apache.org/jira/browse/SPARK-18800) | Improvement | Correct the assert in UnsafeKVExternalSorter which ensures array size |
| 2.2.0 | [SPARK-18909](https://issues.apache.org/jira/browse/SPARK-18909) | Improvement | The error message in `ExpressionEncoder.toRow` and `fromRow` is too verbose |
| 2.2.0 | [SPARK-18917](https://issues.apache.org/jira/browse/SPARK-18917) | Improvement | Dataframe - Time Out Issues / Taking long time in append mode on object stores |
| 2.2.0 | [SPARK-18932](https://issues.apache.org/jira/browse/SPARK-18932) | Improvement | Partial aggregation for collect_set / collect_list |
| 2.2.0 | [SPARK-18980](https://issues.apache.org/jira/browse/SPARK-18980) | Improvement | implement Aggregator with TypedImperativeAggregate |
| 2.2.0 | [SPARK-18990](https://issues.apache.org/jira/browse/SPARK-18990) | Improvement | make DatasetBenchmark fairer for Dataset |
| 2.2.0 | [SPARK-18999](https://issues.apache.org/jira/browse/SPARK-18999) | Improvement | simplify Literal codegen |
| 2.2.0 | [SPARK-19008](https://issues.apache.org/jira/browse/SPARK-19008) | Improvement | Avoid boxing/unboxing overhead of calling a lambda with primitive type from Dataset program |
| 2.2.0 | [SPARK-19060](https://issues.apache.org/jira/browse/SPARK-19060) | Improvement | remove the supportsPartial flag in AggregateFunction |
| 2.2.0 | [SPARK-19070](https://issues.apache.org/jira/browse/SPARK-19070) | Improvement | Clean-up dataset actions |
| 2.2.0 | [SPARK-19088](https://issues.apache.org/jira/browse/SPARK-19088) | Improvement | Optimize sequence type deserialization codegen |
| 2.2.0 | [SPARK-19089](https://issues.apache.org/jira/browse/SPARK-19089) | Improvement | Support nested arrays/seqs in Datasets |
| 2.2.0 | [SPARK-19126](https://issues.apache.org/jira/browse/SPARK-19126) | Improvement | Join Documentation Improvements |
| 2.2.0 | [SPARK-19290](https://issues.apache.org/jira/browse/SPARK-19290) | Improvement | add a new extending interface in Analyzer for post-hoc resolution |
| 2.2.0 | [SPARK-19415](https://issues.apache.org/jira/browse/SPARK-19415) | Improvement | Improve the implicit type conversion between numeric type and string to avoid precesion loss |
| 2.2.0 | [SPARK-19446](https://issues.apache.org/jira/browse/SPARK-19446) | Improvement | Remove unused findTightestCommonType in TypeCoercion |
| 2.2.0 | [SPARK-19453](https://issues.apache.org/jira/browse/SPARK-19453) | Improvement | Correct DataFrame.replace docs |
| 2.2.0 | [SPARK-19454](https://issues.apache.org/jira/browse/SPARK-19454) | Improvement | Improve DataFrame.replace API |
| 2.2.0 | [SPARK-19495](https://issues.apache.org/jira/browse/SPARK-19495) | New Feature | Make SQLConf slightly more extensible |
| 2.2.0 | [SPARK-19544](https://issues.apache.org/jira/browse/SPARK-19544) | Improvement | Improve error message when some column types are compatible and others are not in set/union operations |
| 2.2.0 | [SPARK-19557](https://issues.apache.org/jira/browse/SPARK-19557) | Improvement | Output parameters are not present in SQL Query Plan |
| 2.2.0 | [SPARK-19589](https://issues.apache.org/jira/browse/SPARK-19589) | Improvement | Removal of SQLGEN files |
| 2.2.0 | [SPARK-19607](https://issues.apache.org/jira/browse/SPARK-19607) | New Feature | Finding QueryExecution that matches provided executionId |
| 2.2.0 | [SPARK-19637](https://issues.apache.org/jira/browse/SPARK-19637) | New Feature | add to_json APIs to SQL |
| 2.2.0 | [SPARK-19658](https://issues.apache.org/jira/browse/SPARK-19658) | Improvement | Set NumPartitions of RepartitionByExpression In Analyzer |
| 2.2.0 | [SPARK-19695](https://issues.apache.org/jira/browse/SPARK-19695) | Improvement | Throw an exception if a `columnNameOfCorruptRecord` field violates requirements in Json formats |
| 2.2.0 | [SPARK-19716](https://issues.apache.org/jira/browse/SPARK-19716) | New Feature | Dataset should allow by-name resolution for struct type elements in array |
| 2.2.0 | [SPARK-19745](https://issues.apache.org/jira/browse/SPARK-19745) | Improvement | SVCAggregator serializes coefficients |
| 2.2.0 | [SPARK-19805](https://issues.apache.org/jira/browse/SPARK-19805) | Improvement | Log the row type when query result dose not match |
| 2.2.0 | [SPARK-19830](https://issues.apache.org/jira/browse/SPARK-19830) | Improvement | Add parseTableSchema API to ParserInterface |
| 2.2.0 | [SPARK-19843](https://issues.apache.org/jira/browse/SPARK-19843) | Improvement | UTF8String => (int / long) conversion expensive for invalid inputs |
| 2.2.0 | [SPARK-19944](https://issues.apache.org/jira/browse/SPARK-19944) | Improvement | Move SQLConf from sql/core to sql/catalyst |
| 2.2.0 | [SPARK-19967](https://issues.apache.org/jira/browse/SPARK-19967) | New Feature | Add from_json APIs to SQL |
| 2.2.0 | [SPARK-20009](https://issues.apache.org/jira/browse/SPARK-20009) | Improvement | Use user-friendly DDL formats for defining a schema in functions.from_json |
| 2.2.0 | [SPARK-20134](https://issues.apache.org/jira/browse/SPARK-20134) | New Feature | SQLMetrics.postDriverMetricUpdates to simplify driver side metric updates |
| 2.2.0 | [SPARK-20143](https://issues.apache.org/jira/browse/SPARK-20143) | Improvement | DataType.fromJson should throw an exception with better message |
| 2.2.0 | [SPARK-20175](https://issues.apache.org/jira/browse/SPARK-20175) | Improvement | Exists should not be evaluated in Join operator and can be converted to ScalarSubquery if no correlated reference |
| 2.2.0 | [SPARK-20204](https://issues.apache.org/jira/browse/SPARK-20204) | Improvement | remove SimpleCatalystConf and CatalystConf type alias |
| 2.2.0 | [SPARK-20229](https://issues.apache.org/jira/browse/SPARK-20229) | Improvement | add semanticHash to QueryPlan |
| 2.2.0 | [SPARK-20289](https://issues.apache.org/jira/browse/SPARK-20289) | Improvement | Use StaticInvoke rather than NewInstance for boxing primitive types |
| 2.2.0 | [SPARK-20302](https://issues.apache.org/jira/browse/SPARK-20302) | Improvement | Short circuit cast when from and to types are structurally the same |
| 2.2.0 | [SPARK-20487](https://issues.apache.org/jira/browse/SPARK-20487) | Improvement | `HiveTableScan` node is quite verbose in explained plan |
| 2.2.0 | [SPARK-20492](https://issues.apache.org/jira/browse/SPARK-20492) | Improvement | Do not print empty parentheses for invalid primitive types in parser |
| 2.2.0 | [SPARK-20554](https://issues.apache.org/jira/browse/SPARK-20554) | Improvement | Remove usage of scala.language.reflectiveCalls |
| 2.2.0 | [SPARK-20576](https://issues.apache.org/jira/browse/SPARK-20576) | New Feature | Support generic hint function in Dataset/DataFrame |
| 2.2.0 | [SPARK-20710](https://issues.apache.org/jira/browse/SPARK-20710) | Improvement | Support aliases in CUBE/ROLLUP/GROUPING SETS |
| 2.2.0 | [SPARK-20854](https://issues.apache.org/jira/browse/SPARK-20854) | Improvement | extend hint syntax to support any expression, not just identifiers or strings |
| 2.2.0 | [SPARK-20857](https://issues.apache.org/jira/browse/SPARK-20857) | Improvement | Generic resolved hint node |
| 2.2.0 | [SPARK-21072](https://issues.apache.org/jira/browse/SPARK-21072) | Improvement | `TreeNode.mapChildren` should only apply to the children node. |
| 3.0.0 | [SPARK-8288](https://issues.apache.org/jira/browse/SPARK-8288) | Improvement | ScalaReflection should also try apply methods defined in companion objects when inferring schema from a Product type |
| 3.0.0 | [SPARK-11150](https://issues.apache.org/jira/browse/SPARK-11150) | New Feature | Dynamic partition pruning |
| 3.0.0 | [SPARK-14023](https://issues.apache.org/jira/browse/SPARK-14023) | Improvement | Make exceptions consistent regarding fields and columns |
| 3.0.0 | [SPARK-16323](https://issues.apache.org/jira/browse/SPARK-16323) | Improvement | Avoid unnecessary cast when doing integral divide |
| 3.0.0 | [SPARK-19851](https://issues.apache.org/jira/browse/SPARK-19851) | Improvement | Add support for EVERY and ANY (SOME) aggregates |
| 3.0.0 | [SPARK-21351](https://issues.apache.org/jira/browse/SPARK-21351) | Improvement | Update nullability based on children's output in optimized logical plan |
| 3.0.0 | [SPARK-21436](https://issues.apache.org/jira/browse/SPARK-21436) | Improvement | Take advantage of known partioner for distinct on RDDs |
| 3.0.0 | [SPARK-21870](https://issues.apache.org/jira/browse/SPARK-21870) | Improvement | Split codegen'd aggregation code into small functions for the HotSpot |
| 3.0.0 | [SPARK-24762](https://issues.apache.org/jira/browse/SPARK-24762) | Improvement | Aggregator should be able to use Option of Product encoder |
| 3.0.0 | [SPARK-24901](https://issues.apache.org/jira/browse/SPARK-24901) | Improvement | Merge the codegen of RegularHashMap and fastHashMap to reduce compiler maxCodesize when VectorizedHashMap is false |
| 3.0.0 | [SPARK-25038](https://issues.apache.org/jira/browse/SPARK-25038) | Improvement | Accelerate Spark Plan generation when Spark SQL read large amount of data |
| 3.0.0 | [SPARK-25048](https://issues.apache.org/jira/browse/SPARK-25048) | Improvement | Pivoting by multiple columns in Scala/Java |
| 3.0.0 | [SPARK-25083](https://issues.apache.org/jira/browse/SPARK-25083) | Improvement | remove the type erasure hack in data source scan |
| 3.0.0 | [SPARK-25121](https://issues.apache.org/jira/browse/SPARK-25121) | Improvement | Support multi-part column name for hint resolution |
| 3.0.0 | [SPARK-25153](https://issues.apache.org/jira/browse/SPARK-25153) | Improvement | Improve error messages for columns with dots/periods |
| 3.0.0 | [SPARK-25243](https://issues.apache.org/jira/browse/SPARK-25243) | Improvement | Use FailureSafeParser in from_json |
| 3.0.0 | [SPARK-25381](https://issues.apache.org/jira/browse/SPARK-25381) | Improvement | Stratified sampling by Column argument |
| 3.0.0 | [SPARK-25415](https://issues.apache.org/jira/browse/SPARK-25415) | Improvement | Make plan change log in RuleExecutor configurable by SQLConf |
| 3.0.0 | [SPARK-25440](https://issues.apache.org/jira/browse/SPARK-25440) | Improvement | Dump query execution info to a file |
| 3.0.0 | [SPARK-25444](https://issues.apache.org/jira/browse/SPARK-25444) | Improvement | Refactor GenArrayData.genCodeToCreateArrayData() method |
| 3.0.0 | [SPARK-25446](https://issues.apache.org/jira/browse/SPARK-25446) | Improvement | Add schema_of_json() and schema_of_csv() to R |
| 3.0.0 | [SPARK-25447](https://issues.apache.org/jira/browse/SPARK-25447) | Improvement | Support JSON options by schema_of_json |
| 3.0.0 | [SPARK-25497](https://issues.apache.org/jira/browse/SPARK-25497) | Improvement | limit operation within whole stage codegen should not consume all the inputs |
| 3.0.0 | [SPARK-25514](https://issues.apache.org/jira/browse/SPARK-25514) | Improvement | Generating pretty JSON by to_json |
| 3.0.0 | [SPARK-25556](https://issues.apache.org/jira/browse/SPARK-25556) | New Feature | Predicate Pushdown for Nested fields |
| 3.0.0 | [SPARK-25573](https://issues.apache.org/jira/browse/SPARK-25573) | Improvement | Combine resolveExpression and resolve in the rule ResolveReferences |
| 3.0.0 | [SPARK-25713](https://issues.apache.org/jira/browse/SPARK-25713) | Improvement | Implement copy() for ColumnarArray |
| 3.0.0 | [SPARK-25716](https://issues.apache.org/jira/browse/SPARK-25716) | Improvement | Project and Aggregate generate valid constraints with unnecessary operation |
| 3.0.0 | [SPARK-25734](https://issues.apache.org/jira/browse/SPARK-25734) | Improvement | Literal should have a value corresponding to dataType |
| 3.0.0 | [SPARK-25746](https://issues.apache.org/jira/browse/SPARK-25746) | Improvement | Refactoring ExpressionEncoder |
| 3.0.0 | [SPARK-25747](https://issues.apache.org/jira/browse/SPARK-25747) | Improvement | remove ColumnarBatchScan.needsUnsafeRowConversion |
| 3.0.0 | [SPARK-25755](https://issues.apache.org/jira/browse/SPARK-25755) | Improvement | Supplementation of non-CodeGen unit tested for BroadcastHashJoinExec |
| 3.0.0 | [SPARK-25785](https://issues.apache.org/jira/browse/SPARK-25785) | Improvement | Add prettyNames for from_json, to_json, from_csv, and schema_of_json |
| 3.0.0 | [SPARK-25851](https://issues.apache.org/jira/browse/SPARK-25851) | Improvement | Fix deprecated API warning in SQLListener |
| 3.0.0 | [SPARK-25886](https://issues.apache.org/jira/browse/SPARK-25886) | Improvement | Improve error message of `FailureSafeParser` and `from_avro` in FAILFAST mode |
| 3.0.0 | [SPARK-25892](https://issues.apache.org/jira/browse/SPARK-25892) | Improvement | AttributeReference.withMetadata method should have return type AttributeReference |
| 3.0.0 | [SPARK-25913](https://issues.apache.org/jira/browse/SPARK-25913) | Improvement | Unary SparkPlan nodes should extend UnaryExecNode |
| 3.0.0 | [SPARK-25971](https://issues.apache.org/jira/browse/SPARK-25971) | Improvement | Ignore partition byte-size statistics in SQLQueryTestSuite |
| 3.0.0 | [SPARK-26003](https://issues.apache.org/jira/browse/SPARK-26003) | Improvement | Improve performance in SQLAppStatusListener |
| 3.0.0 | [SPARK-26004](https://issues.apache.org/jira/browse/SPARK-26004) | Improvement | InMemoryTable support StartsWith predicate push down |
| 3.0.0 | [SPARK-26065](https://issues.apache.org/jira/browse/SPARK-26065) | Improvement | Change query hint from a `LogicalPlan` to a field |
| 3.0.0 | [SPARK-26098](https://issues.apache.org/jira/browse/SPARK-26098) | Improvement | Show associated SQL query in Job page |
| 3.0.0 | [SPARK-26099](https://issues.apache.org/jira/browse/SPARK-26099) | Improvement | Verification of the corrupt column in from_csv/from_json |
| 3.0.0 | [SPARK-26230](https://issues.apache.org/jira/browse/SPARK-26230) | Improvement | FileIndex: if case sensitive, validate partitions with original column names |
| 3.0.0 | [SPARK-26262](https://issues.apache.org/jira/browse/SPARK-26262) | Improvement | Runs SQLQueryTestSuite on mixed config sets: WHOLESTAGE_CODEGEN_ENABLED and CODEGEN_FACTORY_MODE |
| 3.0.0 | [SPARK-26263](https://issues.apache.org/jira/browse/SPARK-26263) | Improvement | Throw exception when Partition column value can't be converted to user specified type |
| 3.0.0 | [SPARK-26321](https://issues.apache.org/jira/browse/SPARK-26321) | Improvement | Split a SQL in a correct way |
| 3.0.0 | [SPARK-26368](https://issues.apache.org/jira/browse/SPARK-26368) | Improvement | Make it clear that getOrInferFileFormatSchema doesn't create InMemoryFileIndex |
| 3.0.0 | [SPARK-26383](https://issues.apache.org/jira/browse/SPARK-26383) | Improvement | NPE when use DataFrameReader.jdbc with wrong URL |
| 3.0.0 | [SPARK-26390](https://issues.apache.org/jira/browse/SPARK-26390) | Improvement | ColumnPruning rule should only do column pruning |
| 3.0.0 | [SPARK-26409](https://issues.apache.org/jira/browse/SPARK-26409) | Improvement | SQLConf should be serializable in test sessions |
| 3.0.0 | [SPARK-26450](https://issues.apache.org/jira/browse/SPARK-26450) | Improvement | Map of schema is built too frequently in some wide queries |
| 3.0.0 | [SPARK-26502](https://issues.apache.org/jira/browse/SPARK-26502) | Improvement | Get rid of hiveResultString() in QueryExecution |
| 3.0.0 | [SPARK-26527](https://issues.apache.org/jira/browse/SPARK-26527) | Improvement | Let acquireUnrollMemory fail fast if required space exceeds memory limit |
| 3.0.0 | [SPARK-26617](https://issues.apache.org/jira/browse/SPARK-26617) | Improvement | CacheManager blocks during requery |
| 3.0.0 | [SPARK-26622](https://issues.apache.org/jira/browse/SPARK-26622) | Improvement | Improve wording in SQLMetrics labels |
| 3.0.0 | [SPARK-26696](https://issues.apache.org/jira/browse/SPARK-26696) | Improvement | Dataset encoder should be publicly accessible |
| 3.0.0 | [SPARK-26716](https://issues.apache.org/jira/browse/SPARK-26716) | Improvement | Refactor supportDataType API: the supported types of read/write should be consistent |
| 3.0.0 | [SPARK-26736](https://issues.apache.org/jira/browse/SPARK-26736) | Improvement | if filter condition `And` has non-determined sub function it does not do partition prunning |
| 3.0.0 | [SPARK-26835](https://issues.apache.org/jira/browse/SPARK-26835) | Improvement | Document configuration properties of Spark SQL Generic Load/Save Functions |
| 3.0.0 | [SPARK-26861](https://issues.apache.org/jira/browse/SPARK-26861) | Improvement | deprecate typed sum/count/average |
| 3.0.0 | [SPARK-26917](https://issues.apache.org/jira/browse/SPARK-26917) | Improvement | CacheManager blocks while traversing plans |
| 3.0.0 | [SPARK-26955](https://issues.apache.org/jira/browse/SPARK-26955) | Improvement | Align Spark's TimSort to JDK 11 TimSort |
| 3.0.0 | [SPARK-27001](https://issues.apache.org/jira/browse/SPARK-27001) | Improvement | Refactor "serializerFor" method between ScalaReflection and JavaTypeInference |
| 3.0.0 | [SPARK-27057](https://issues.apache.org/jira/browse/SPARK-27057) | Improvement | Common trait for limit exec operators |
| 3.0.0 | [SPARK-27083](https://issues.apache.org/jira/browse/SPARK-27083) | Improvement | Add a config to control subqueryReuse |
| 3.0.0 | [SPARK-27088](https://issues.apache.org/jira/browse/SPARK-27088) | Improvement | Apply conf "spark.sql.optimizer.planChangeLog.level" to batch plan change in RuleExecutor |
| 3.0.0 | [SPARK-27099](https://issues.apache.org/jira/browse/SPARK-27099) | Improvement | Expose xxHash64 as a flexible 64-bit column hash like `hash` |
| 3.0.0 | [SPARK-27103](https://issues.apache.org/jira/browse/SPARK-27103) | Improvement | SparkSql reserved keywords don't list in alphabet order |
| 3.0.0 | [SPARK-27106](https://issues.apache.org/jira/browse/SPARK-27106) | Improvement | merge CaseInsensitiveStringMap and DataSourceOptions |
| 3.0.0 | [SPARK-27110](https://issues.apache.org/jira/browse/SPARK-27110) | Improvement | Moves some functions from AnalyzeColumnCommand to command/CommandUtils for reuse |
| 3.0.0 | [SPARK-27125](https://issues.apache.org/jira/browse/SPARK-27125) | Improvement | Add test suite for sql execution page |
| 3.0.0 | [SPARK-27145](https://issues.apache.org/jira/browse/SPARK-27145) | Improvement | Close store after test, in the SQLAppStatusListenerSuite |
| 3.0.0 | [SPARK-27161](https://issues.apache.org/jira/browse/SPARK-27161) | Improvement | improve the document of SQL keywords |
| 3.0.0 | [SPARK-27166](https://issues.apache.org/jira/browse/SPARK-27166) | Improvement | Improve `printSchema` to print up to the given level |
| 3.0.0 | [SPARK-27221](https://issues.apache.org/jira/browse/SPARK-27221) | Improvement | Improve the assert error message in TreeNode.parseToJson |
| 3.0.0 | [SPARK-27225](https://issues.apache.org/jira/browse/SPARK-27225) | New Feature | Implement join strategy hints |
| 3.0.0 | [SPARK-27241](https://issues.apache.org/jira/browse/SPARK-27241) | Improvement | Add map_keys and map_values support to SelectedField in nested schema pruning |
| 3.0.0 | [SPARK-27285](https://issues.apache.org/jira/browse/SPARK-27285) | Improvement | Support describing output of a CTE |
| 3.0.0 | [SPARK-27314](https://issues.apache.org/jira/browse/SPARK-27314) | Improvement | Deduplicate exprIds for Union. |
| 3.0.0 | [SPARK-27320](https://issues.apache.org/jira/browse/SPARK-27320) | Improvement | Converting seq to array in AggregationIterator to improve its access performance |
| 3.0.0 | [SPARK-27327](https://issues.apache.org/jira/browse/SPARK-27327) | Improvement | New JSON benchmarks: functions, dataset parsing |
| 3.0.0 | [SPARK-27333](https://issues.apache.org/jira/browse/SPARK-27333) | Improvement | Add StatisticsDataReferenceCleaner/process reaper/broadcast-exchange to thread audit whitelist |
| 3.0.0 | [SPARK-27342](https://issues.apache.org/jira/browse/SPARK-27342) | Improvement | Optimize limit 0 queries |
| 3.0.0 | [SPARK-27393](https://issues.apache.org/jira/browse/SPARK-27393) | Improvement | Show ReusedSubquery in the plan when the subquery is reused |
| 3.0.0 | [SPARK-27395](https://issues.apache.org/jira/browse/SPARK-27395) | New Feature | New format of EXPLAIN command |
| 3.0.0 | [SPARK-27404](https://issues.apache.org/jira/browse/SPARK-27404) | Improvement | Fix build warnings for 3.0: postfixOps edition |
| 3.0.0 | [SPARK-27423](https://issues.apache.org/jira/browse/SPARK-27423) | Improvement | Cast DATE to/from TIMESTAMP according to SQL standard |
| 3.0.0 | [SPARK-27449](https://issues.apache.org/jira/browse/SPARK-27449) | Improvement | Clean-up checks in CodegenSupport.limitNotReachedCond |
| 3.0.0 | [SPARK-27476](https://issues.apache.org/jira/browse/SPARK-27476) | Improvement | Refactoring SchemaPruning rule to remove duplicate code |
| 3.0.0 | [SPARK-27480](https://issues.apache.org/jira/browse/SPARK-27480) | Improvement | Improve `EXPLAIN DESC QUERY` to show the input SQL statement |
| 3.0.0 | [SPARK-27506](https://issues.apache.org/jira/browse/SPARK-27506) | Improvement | Function `from_avro` doesn't allow deserialization of data using other compatible schemas |
| 3.0.0 | [SPARK-27534](https://issues.apache.org/jira/browse/SPARK-27534) | Story | Do not load `content` column in binary data source if it is not selected |
| 3.0.0 | [SPARK-27551](https://issues.apache.org/jira/browse/SPARK-27551) | Improvement | Improve error message of mismatched types for CASE WHEN |
| 3.0.0 | [SPARK-27571](https://issues.apache.org/jira/browse/SPARK-27571) | Improvement | Spark 3.0 build warnings: reflectiveCalls edition |
| 3.0.0 | [SPARK-27675](https://issues.apache.org/jira/browse/SPARK-27675) | Improvement | do not use MutableColumnarRow in ColumnarBatch |
| 3.0.0 | [SPARK-27684](https://issues.apache.org/jira/browse/SPARK-27684) | Improvement | Reduce ScalaUDF conversion overheads for primitives |
| 3.0.0 | [SPARK-27701](https://issues.apache.org/jira/browse/SPARK-27701) | Improvement | Extend NestedColumnAliasing to more nested field cases |
| 3.0.0 | [SPARK-27713](https://issues.apache.org/jira/browse/SPARK-27713) | Improvement | Move RecordBinaryComparator and unsafe sorters from catalyst project to core |
| 3.0.0 | [SPARK-27722](https://issues.apache.org/jira/browse/SPARK-27722) | Improvement | Remove UnsafeKeyValueSorter |
| 3.0.0 | [SPARK-27747](https://issues.apache.org/jira/browse/SPARK-27747) | Improvement | add a logical plan link in the physical plan |
| 3.0.0 | [SPARK-27763](https://issues.apache.org/jira/browse/SPARK-27763) | Umbrella | Port test cases from PostgreSQL to Spark SQL |
| 3.0.0 | [SPARK-27771](https://issues.apache.org/jira/browse/SPARK-27771) | Improvement | Add SQL description for grouping functions (cube, rollup, grouping and grouping_id) |
| 3.0.0 | [SPARK-27772](https://issues.apache.org/jira/browse/SPARK-27772) | Improvement | SQLTestUtils Refactoring |
| 3.0.0 | [SPARK-27783](https://issues.apache.org/jira/browse/SPARK-27783) | Improvement | Add customizable hint error handler |
| 3.0.0 | [SPARK-27815](https://issues.apache.org/jira/browse/SPARK-27815) | Improvement | Improve SQL optimizer's predicate pushdown performance for cascading joins |
| 3.0.0 | [SPARK-27816](https://issues.apache.org/jira/browse/SPARK-27816) | Improvement | make TreeNode tag type safe |
| 3.0.0 | [SPARK-27829](https://issues.apache.org/jira/browse/SPARK-27829) | Improvement | In Dataset.joinWith inner joins, don't nest data before shuffling |
| 3.0.0 | [SPARK-27839](https://issues.apache.org/jira/browse/SPARK-27839) | Improvement | Improve UTF8String.replace() / StringReplace performance |
| 3.0.0 | [SPARK-27871](https://issues.apache.org/jira/browse/SPARK-27871) | Improvement | LambdaVariable should use per-query unique IDs instead of globally unique IDs |
| 3.0.0 | [SPARK-27944](https://issues.apache.org/jira/browse/SPARK-27944) | Improvement | Unify the behavior of checking empty output column names |
| 3.0.0 | [SPARK-27945](https://issues.apache.org/jira/browse/SPARK-27945) | Improvement | Make minimal changes to support columnar processing |
| 3.0.0 | [SPARK-27947](https://issues.apache.org/jira/browse/SPARK-27947) | Improvement | Enhance redactOptions to accept any Map type |
| 3.0.0 | [SPARK-28051](https://issues.apache.org/jira/browse/SPARK-28051) | Improvement | Exposing JIRA issue component types at GitHub PRs |
| 3.0.0 | [SPARK-28057](https://issues.apache.org/jira/browse/SPARK-28057) | Improvement | Add method `clone` in catalyst TreeNode |
| 3.0.0 | [SPARK-28066](https://issues.apache.org/jira/browse/SPARK-28066) | Improvement | Optimize UTF8String.trim() for common case of no whitespace |
| 3.0.0 | [SPARK-28096](https://issues.apache.org/jira/browse/SPARK-28096) | Improvement | Lazy val performance pitfall in Spark SQL LogicalPlans |
| 3.0.0 | [SPARK-28106](https://issues.apache.org/jira/browse/SPARK-28106) | Improvement | Spark SQL add jar with wrong hdfs path, SparkContext still add it to jar path ,and cause Task Failed |
| 3.0.0 | [SPARK-28127](https://issues.apache.org/jira/browse/SPARK-28127) | Improvement | Micro optimization on TreeNode's mapChildren method |
| 3.0.0 | [SPARK-28198](https://issues.apache.org/jira/browse/SPARK-28198) | New Feature | Add mapPartitionsInPandas to allow an iterator of DataFrames |
| 3.0.0 | [SPARK-28213](https://issues.apache.org/jira/browse/SPARK-28213) | Improvement | Remove duplication between columnar and ColumnarBatchScan |
| 3.0.0 | [SPARK-28216](https://issues.apache.org/jira/browse/SPARK-28216) | Improvement | Add calculate local directory size to SQLTestUtils |
| 3.0.0 | [SPARK-28250](https://issues.apache.org/jira/browse/SPARK-28250) | Improvement | QueryPlan#references should exclude producedAttributes |
| 3.0.0 | [SPARK-28257](https://issues.apache.org/jira/browse/SPARK-28257) | Improvement | Use ConfigEntry for hardcoded configs in SQL module |
| 3.0.0 | [SPARK-28292](https://issues.apache.org/jira/browse/SPARK-28292) | Improvement | Enable inject user-defined Hint |
| 3.0.0 | [SPARK-28339](https://issues.apache.org/jira/browse/SPARK-28339) | Improvement | Rename Spark SQL adaptive execution configuration name |
| 3.0.0 | [SPARK-28345](https://issues.apache.org/jira/browse/SPARK-28345) | Improvement | PythonUDF predicate should be able to pushdown to join |
| 3.0.0 | [SPARK-28356](https://issues.apache.org/jira/browse/SPARK-28356) | Improvement | Do not reduce the number of partitions for repartition in adaptive execution |
| 3.0.0 | [SPARK-28477](https://issues.apache.org/jira/browse/SPARK-28477) | Improvement | Rewrite `CASE WHEN cond THEN ifTrue OTHERWISE ifFalse` END into `IF(cond, ifTrue, ifFalse)` |
| 3.0.0 | [SPARK-28545](https://issues.apache.org/jira/browse/SPARK-28545) | Improvement | Add the hash map size to the directional log of ObjectAggregationIterator |
| 3.0.0 | [SPARK-28588](https://issues.apache.org/jira/browse/SPARK-28588) | Umbrella | Build a SQL reference doc |
| 3.0.0 | [SPARK-28595](https://issues.apache.org/jira/browse/SPARK-28595) | Improvement | explain should not trigger partition listing |
| 3.0.0 | [SPARK-28598](https://issues.apache.org/jira/browse/SPARK-28598) | Improvement | Few date time manipulation functions does not provide versions supporting Column as input through the Dataframe API |
| 3.0.0 | [SPARK-28644](https://issues.apache.org/jira/browse/SPARK-28644) | Improvement | Port HIVE-10646: ColumnValue does not handle NULL_TYPE |
| 3.0.0 | [SPARK-28702](https://issues.apache.org/jira/browse/SPARK-28702) | Improvement | Display useful error message (instead of NPE) for invalid Dataset operations (e.g. calling actions inside of transformations) |
| 3.0.0 | [SPARK-28715](https://issues.apache.org/jira/browse/SPARK-28715) | Improvement | Introduce collectInPlanAndSubqueries and subqueriesAll in QueryPlan |
| 3.0.0 | [SPARK-28716](https://issues.apache.org/jira/browse/SPARK-28716) | Improvement | Add id to Exchange and Subquery's stringArgs method for easier identifying their reuses in query plans |
| 3.0.0 | [SPARK-28746](https://issues.apache.org/jira/browse/SPARK-28746) | Improvement | Add repartitionby hint to support RepartitionByExpression |
| 3.0.0 | [SPARK-28835](https://issues.apache.org/jira/browse/SPARK-28835) | Improvement | Introduce TPCDSSchema |
| 3.0.0 | [SPARK-28836](https://issues.apache.org/jira/browse/SPARK-28836) | Improvement | Remove the canonicalize(attributes) method from PlanExpression |
| 3.0.0 | [SPARK-28837](https://issues.apache.org/jira/browse/SPARK-28837) | Improvement | CTAS/RTAS should use nullable schema |
| 3.0.0 | [SPARK-28910](https://issues.apache.org/jira/browse/SPARK-28910) | Improvement | Prevent schema verification when connecting to in memory derby |
| 3.0.0 | [SPARK-29008](https://issues.apache.org/jira/browse/SPARK-29008) | Improvement | Define an individual method for each common subexpression in HashAggregateExec |
| 3.0.0 | [SPARK-29026](https://issues.apache.org/jira/browse/SPARK-29026) | Improvement | Improve error message when constructor in `ScalaReflection` isn't found |
| 3.0.0 | [SPARK-29061](https://issues.apache.org/jira/browse/SPARK-29061) | Improvement | Prints bytecode statistics in debugCodegen |
| 3.0.0 | [SPARK-29092](https://issues.apache.org/jira/browse/SPARK-29092) | Improvement | EXPLAIN FORMATTED does not work well with DPP |
| 3.0.0 | [SPARK-29122](https://issues.apache.org/jira/browse/SPARK-29122) | Improvement | Propagate all the SQL conf to executors in SQLQueryTestSuite |
| 3.0.0 | [SPARK-29191](https://issues.apache.org/jira/browse/SPARK-29191) | Improvement | Add tag ExtendedSQLTest for SQLQueryTestSuite |
| 3.0.0 | [SPARK-29343](https://issues.apache.org/jira/browse/SPARK-29343) | Improvement | Eliminate sorts without limit in the subquery of Join/Aggregation |
| 3.0.0 | [SPARK-29346](https://issues.apache.org/jira/browse/SPARK-29346) | New Feature | Create Aggregating Accumulator |
| 3.0.0 | [SPARK-29473](https://issues.apache.org/jira/browse/SPARK-29473) | Improvement | move statement logical plans to a new file |
| 3.0.0 | [SPARK-29545](https://issues.apache.org/jira/browse/SPARK-29545) | Improvement | Implement bitwise integer aggregates bit_xor |
| 3.0.0 | [SPARK-29746](https://issues.apache.org/jira/browse/SPARK-29746) | Improvement | implement validateInputType in Normalizer |
| 3.0.0 | [SPARK-29855](https://issues.apache.org/jira/browse/SPARK-29855) | Improvement | typed literals with negative sign with proper result or exception |
| 3.0.0 | [SPARK-29930](https://issues.apache.org/jira/browse/SPARK-29930) | Improvement | Remove SQL configs declared to be removed in Spark 3.0 |
| 3.0.0 | [SPARK-29945](https://issues.apache.org/jira/browse/SPARK-29945) | Improvement | do not handle negative sign specially in the parser |
| 3.0.0 | [SPARK-29968](https://issues.apache.org/jira/browse/SPARK-29968) | Improvement | Remove the Predicate code from SparkPlan |
| 3.0.0 | [SPARK-29977](https://issues.apache.org/jira/browse/SPARK-29977) | Improvement | Remove newMutableProjection/newOrdering/newNaturalAscendingOrdering from SparkPlan |
| 3.0.0 | [SPARK-29986](https://issues.apache.org/jira/browse/SPARK-29986) | Improvement | Introduce java like string trim to UTF8String |
| 3.0.0 | [SPARK-30047](https://issues.apache.org/jira/browse/SPARK-30047) | Improvement | HashAggregate support for interval value aggs |
| 3.0.0 | [SPARK-30072](https://issues.apache.org/jira/browse/SPARK-30072) | Improvement | Create dedicated planner for subqueries |
| 3.0.0 | [SPARK-30107](https://issues.apache.org/jira/browse/SPARK-30107) | Improvement | Expose nested schema pruning to all V2 sources |
| 3.0.0 | [SPARK-30127](https://issues.apache.org/jira/browse/SPARK-30127) | New Feature | UDF should work for case class like Dataset operations |
| 3.0.0 | [SPARK-30138](https://issues.apache.org/jira/browse/SPARK-30138) | Improvement | Separate configuration key of max iterations for analyzer and optimizer |
| 3.0.0 | [SPARK-30151](https://issues.apache.org/jira/browse/SPARK-30151) | Improvement | Issue better error message when user-specified schema not match relation schema |
| 3.0.0 | [SPARK-30185](https://issues.apache.org/jira/browse/SPARK-30185) | New Feature | Implement Dataset.tail API |
| 3.0.0 | [SPARK-30192](https://issues.apache.org/jira/browse/SPARK-30192) | New Feature | support column position in DS v2 |
| 3.0.0 | [SPARK-30200](https://issues.apache.org/jira/browse/SPARK-30200) | Improvement | Add ExplainMode for Dataset.explain |
| 3.0.0 | [SPARK-30207](https://issues.apache.org/jira/browse/SPARK-30207) | Improvement | Enhance the SQL NULL Semantics document |
| 3.0.0 | [SPARK-30213](https://issues.apache.org/jira/browse/SPARK-30213) | New Feature | Remove the mutable status in QueryStage when enable AQE |
| 3.0.0 | [SPARK-30278](https://issues.apache.org/jira/browse/SPARK-30278) | Improvement | Update Spark SQL document menu for new changes |
| 3.0.0 | [SPARK-30326](https://issues.apache.org/jira/browse/SPARK-30326) | Improvement | Raise exception if analyzer exceed max iterations |
| 3.0.0 | [SPARK-30342](https://issues.apache.org/jira/browse/SPARK-30342) | Improvement | Update LIST JAR/FILE command |
| 3.0.0 | [SPARK-30343](https://issues.apache.org/jira/browse/SPARK-30343) | Improvement | Skip unnecessary checks in RewriteDistinctAggregates |
| 3.0.0 | [SPARK-30350](https://issues.apache.org/jira/browse/SPARK-30350) | Improvement | Fix ScalaReflection to use an empty array for getting its class object |
| 3.0.0 | [SPARK-30356](https://issues.apache.org/jira/browse/SPARK-30356) | Improvement | Codegen support for the function str_to_map |
| 3.0.0 | [SPARK-30415](https://issues.apache.org/jira/browse/SPARK-30415) | Improvement | Improve Readability of SQLConf Doc |
| 3.0.0 | [SPARK-30416](https://issues.apache.org/jira/browse/SPARK-30416) | Improvement | Log a warning for deprecated SQL config in `set()` and `unset()` |
| 3.0.0 | [SPARK-30431](https://issues.apache.org/jira/browse/SPARK-30431) | Improvement | Update SqlBase.g4 to create commentSpec pattern as same as locationSpec |
| 3.0.0 | [SPARK-30433](https://issues.apache.org/jira/browse/SPARK-30433) | Improvement | Make conflict attributes resolution more scalable in ResolveReferences |
| 3.0.0 | [SPARK-30508](https://issues.apache.org/jira/browse/SPARK-30508) | Improvement | Add DataFrameReader.executeCommand API for external datasource |
| 3.0.0 | [SPARK-30554](https://issues.apache.org/jira/browse/SPARK-30554) | Improvement | Return Iterable from FailureSafeParser.rawParser |
| 3.0.0 | [SPARK-30558](https://issues.apache.org/jira/browse/SPARK-30558) | Improvement | Avoid rebuilding `AvroOptions` per each partition |
| 3.0.0 | [SPARK-30614](https://issues.apache.org/jira/browse/SPARK-30614) | New Feature | The native ALTER COLUMN syntax should change one thing at a time |
| 3.0.0 | [SPARK-30615](https://issues.apache.org/jira/browse/SPARK-30615) | New Feature | normalize the column name in AlterTable |
| 3.0.0 | [SPARK-30620](https://issues.apache.org/jira/browse/SPARK-30620) | Improvement | avoid unnecessary serialization in AggregateExpression |
| 3.0.0 | [SPARK-30644](https://issues.apache.org/jira/browse/SPARK-30644) | Improvement | Remove query index from the golden files of SQLQueryTestSuite |
| 3.0.0 | [SPARK-30671](https://issues.apache.org/jira/browse/SPARK-30671) | New Feature | SparkSession emptyDataFrame should not create an RDD |
| 3.0.0 | [SPARK-30725](https://issues.apache.org/jira/browse/SPARK-30725) | Improvement | Make all legacy SQL configs as internal configs |
| 3.0.0 | [SPARK-30762](https://issues.apache.org/jira/browse/SPARK-30762) | Story | Add dtype="float32" support to vector_to_array UDF |
| 3.0.0 | [SPARK-30764](https://issues.apache.org/jira/browse/SPARK-30764) | Improvement | Improve the readability of EXPLAIN FORMATTED style |
| 3.0.0 | [SPARK-30790](https://issues.apache.org/jira/browse/SPARK-30790) | Improvement | The datatype of map() should be map<null,null> |
| 3.0.0 | [SPARK-30798](https://issues.apache.org/jira/browse/SPARK-30798) | Improvement | Scope Session.active in QueryExecution |
| 3.0.0 | [SPARK-30953](https://issues.apache.org/jira/browse/SPARK-30953) | Improvement | InsertAdaptiveSparkPlan should apply AQE on child plan of write commands |
| 3.0.0 | [SPARK-31010](https://issues.apache.org/jira/browse/SPARK-31010) | Improvement | forbid untyped scala UDF API by default |
| 3.0.0 | [SPARK-31060](https://issues.apache.org/jira/browse/SPARK-31060) | Improvement | Handle column names containing `dots` in data source `Filter` |
| 3.0.0 | [SPARK-31187](https://issues.apache.org/jira/browse/SPARK-31187) | Improvement | Sort the whole-stage codegen debug output by codegenStageId |
| 3.0.0 | [SPARK-31190](https://issues.apache.org/jira/browse/SPARK-31190) | Improvement | ScalaReflection should not erasure user defined AnyVal type |
| 3.0.0 | [SPARK-31292](https://issues.apache.org/jira/browse/SPARK-31292) | Improvement | Replace toSet.toSeq with distinct for readability |
| 3.0.0 | [SPARK-31322](https://issues.apache.org/jira/browse/SPARK-31322) | Improvement | rename QueryPlan.collectInPlanAndSubqueries to collectWithSubqueries |
| 3.0.0 | [SPARK-31412](https://issues.apache.org/jira/browse/SPARK-31412) | New Feature | New Adaptive Query Execution in Spark SQL |
| 3.0.0 | [SPARK-31424](https://issues.apache.org/jira/browse/SPARK-31424) | Improvement | Rename AdaptiveSparkPlanHelper.collectInPlanAndSubqueries to collectWithSubqueries |
| 3.0.0 | [SPARK-31425](https://issues.apache.org/jira/browse/SPARK-31425) | Improvement | UnsafeKVExternalSorter/VariableLengthRowBasedKeyValueBatch should also respect UnsafeAlignedOffset |
| 3.0.0 | [SPARK-31495](https://issues.apache.org/jira/browse/SPARK-31495) | Improvement | Support formatted explain for Adaptive Query Execution |
| 3.0.0 | [SPARK-31498](https://issues.apache.org/jira/browse/SPARK-31498) | Improvement | Dump public static sql configurations through doc generation |
| 3.0.0 | [SPARK-31529](https://issues.apache.org/jira/browse/SPARK-31529) | Improvement | Remove extra whitespaces in the formatted explain |
| 3.0.0 | [SPARK-31678](https://issues.apache.org/jira/browse/SPARK-31678) | Improvement | PrintStackTrace for Spark SQL CLI when error occurs |
| 3.1.1 | [SPARK-33138](https://issues.apache.org/jira/browse/SPARK-33138) | Improvement | unify temp view and permanent view behaviors |
| 3.1.1 | [SPARK-33818](https://issues.apache.org/jira/browse/SPARK-33818) | Improvement | Doc `spark.sql.parser.quotedRegexColumnNames` |
| 3.1.1 | [SPARK-33938](https://issues.apache.org/jira/browse/SPARK-33938) | Improvement | Optimize Like Any/All by LikeSimplification |
| 3.1.1 | [SPARK-34191](https://issues.apache.org/jira/browse/SPARK-34191) | Improvement | udf type hint should allow dectorator with named returnType |
| 3.2.0 | [SPARK-26138](https://issues.apache.org/jira/browse/SPARK-26138) | Improvement | Pushdown limit through InnerLike when condition is empty |
| 3.2.0 | [SPARK-28220](https://issues.apache.org/jira/browse/SPARK-28220) | Improvement | join foldable condition not pushed down when parent filter is totally pushed down |
| 3.2.0 | [SPARK-28940](https://issues.apache.org/jira/browse/SPARK-28940) | Improvement | Subquery reuse across all subquery levels |
| 3.2.0 | [SPARK-29375](https://issues.apache.org/jira/browse/SPARK-29375) | Improvement | Exchange reuse across all subquery levels |
| 3.2.0 | [SPARK-30027](https://issues.apache.org/jira/browse/SPARK-30027) | Improvement | Support codegen for filter exprs in HashAggregateExec |
| 3.2.0 | [SPARK-31897](https://issues.apache.org/jira/browse/SPARK-31897) | Improvement | Enable codegen for GenerateExec |
| 3.2.0 | [SPARK-31936](https://issues.apache.org/jira/browse/SPARK-31936) | Improvement | Implement ScriptTransform in sql/core |
| 3.2.0 | [SPARK-32855](https://issues.apache.org/jira/browse/SPARK-32855) | Improvement | Improve DPP for some join type do not support broadcast filtering side |
| 3.2.0 | [SPARK-33122](https://issues.apache.org/jira/browse/SPARK-33122) | Improvement | Remove redundant aggregates in the Optimzier |
| 3.2.0 | [SPARK-33307](https://issues.apache.org/jira/browse/SPARK-33307) | Improvement | Refactor GROUPING ANALYTICS |
| 3.2.0 | [SPARK-33497](https://issues.apache.org/jira/browse/SPARK-33497) | Improvement | Override maxRows in some LogicalPlan |
| 3.2.0 | [SPARK-33678](https://issues.apache.org/jira/browse/SPARK-33678) | Improvement | Numerical product aggregation |
| 3.2.0 | [SPARK-33690](https://issues.apache.org/jira/browse/SPARK-33690) | Improvement | Escape meta-characters in showString |
| 3.2.0 | [SPARK-33735](https://issues.apache.org/jira/browse/SPARK-33735) | Improvement | Handle UPDATE in ReplaceNullWithFalseInPredicate |
| 3.2.0 | [SPARK-33736](https://issues.apache.org/jira/browse/SPARK-33736) | Improvement | Handle MERGE in ReplaceNullWithFalseInPredicate |
| 3.2.0 | [SPARK-33758](https://issues.apache.org/jira/browse/SPARK-33758) | Improvement | Prune unnecessary output partitioning when the attribute is not part of output. |
| 3.2.0 | [SPARK-33769](https://issues.apache.org/jira/browse/SPARK-33769) | Improvement | improve the next-day function of the sql component to deal with Column type |
| 3.2.0 | [SPARK-33800](https://issues.apache.org/jira/browse/SPARK-33800) | Improvement | Remove command name in AnalysisException message when a relation is not resolved |
| 3.2.0 | [SPARK-33828](https://issues.apache.org/jira/browse/SPARK-33828) | Umbrella | SQL Adaptive Query Execution QA |
| 3.2.0 | [SPARK-33939](https://issues.apache.org/jira/browse/SPARK-33939) | Improvement | Make Column.named use UnresolvedAlias to assign name |
| 3.2.0 | [SPARK-33951](https://issues.apache.org/jira/browse/SPARK-33951) | Improvement | Distinguish the error between filter and distinct |
| 3.2.0 | [SPARK-33964](https://issues.apache.org/jira/browse/SPARK-33964) | Improvement | Combine distinct unions in more cases |
| 3.2.0 | [SPARK-33971](https://issues.apache.org/jira/browse/SPARK-33971) | Improvement | Eliminate distinct from more aggregates |
| 3.2.0 | [SPARK-33988](https://issues.apache.org/jira/browse/SPARK-33988) | Improvement | Add an option to enable CBO in TPCDSQueryBenchmark |
| 3.2.0 | [SPARK-33989](https://issues.apache.org/jira/browse/SPARK-33989) | Improvement | Strip auto-generated cast when using Cast.sql |
| 3.2.0 | [SPARK-33998](https://issues.apache.org/jira/browse/SPARK-33998) | Improvement | Refactor v2CommandExec to provide an API to create an InternalRow |
| 3.2.0 | [SPARK-34004](https://issues.apache.org/jira/browse/SPARK-34004) | Improvement | Change FrameLessOffsetWindowFunction as sealed abstract class |
| 3.2.0 | [SPARK-34030](https://issues.apache.org/jira/browse/SPARK-34030) | Improvement | Fold RepartitionExpression num partition should at Optimizer |
| 3.2.0 | [SPARK-34046](https://issues.apache.org/jira/browse/SPARK-34046) | Improvement | Use join hint in test cases for Join |
| 3.2.0 | [SPARK-34081](https://issues.apache.org/jira/browse/SPARK-34081) | Improvement | Only pushdown LeftSemi/LeftAnti over Aggregate if join can be planned as broadcast join |
| 3.2.0 | [SPARK-34120](https://issues.apache.org/jira/browse/SPARK-34120) | Umbrella | Improve the statistics estimation |
| 3.2.0 | [SPARK-34147](https://issues.apache.org/jira/browse/SPARK-34147) | Improvement | Keep data partitioning in TPCDSQueryBenchmark when CBO is enabled |
| 3.2.0 | [SPARK-34150](https://issues.apache.org/jira/browse/SPARK-34150) | Improvement | Strip Null literal.sql in resolve alias |
| 3.2.0 | [SPARK-34165](https://issues.apache.org/jira/browse/SPARK-34165) | New Feature | Add countDistinct option to Dataset#summary |
| 3.2.0 | [SPARK-34182](https://issues.apache.org/jira/browse/SPARK-34182) | Improvement | [AVRO] Improve error messages when matching Catalyst-to-Avro schemas |
| 3.2.0 | [SPARK-34222](https://issues.apache.org/jira/browse/SPARK-34222) | Improvement | Enhance Boolean Simplification Rule |
| 3.2.0 | [SPARK-34234](https://issues.apache.org/jira/browse/SPARK-34234) | Improvement | Remove TreeNodeException that didn't work |
| 3.2.0 | [SPARK-34283](https://issues.apache.org/jira/browse/SPARK-34283) | Improvement | Combines all adjacent 'Union' operators into a single 'Union' when using 'Dataset.union.distinct.union.distinct' |
| 3.2.0 | [SPARK-34308](https://issues.apache.org/jira/browse/SPARK-34308) | Improvement | Escape meta-characters in printSchema |
| 3.2.0 | [SPARK-34317](https://issues.apache.org/jira/browse/SPARK-34317) | Improvement | Introduce relationTypeMismatchHint to UnresolvedTable for a better error message |
| 3.2.0 | [SPARK-34343](https://issues.apache.org/jira/browse/SPARK-34343) | Improvement | Add missing test for some non-array types in PostgreSQL |
| 3.2.0 | [SPARK-34356](https://issues.apache.org/jira/browse/SPARK-34356) | Improvement | OVR transform fix potential column conflict |
| 3.2.0 | [SPARK-34388](https://issues.apache.org/jira/browse/SPARK-34388) | Improvement | Propogate the registered UDF names to ScalaUDAF and ScalaAggregator |
| 3.2.0 | [SPARK-34419](https://issues.apache.org/jira/browse/SPARK-34419) | Improvement | Move PartitionTransforms from java to scala directory |
| 3.2.0 | [SPARK-34420](https://issues.apache.org/jira/browse/SPARK-34420) | Improvement | Throw exception if non-streaming Deduplicate is not replaced by aggregate |
| 3.2.0 | [SPARK-34454](https://issues.apache.org/jira/browse/SPARK-34454) | Improvement | SQL configs from the legacy namespace must be internal |
| 3.2.0 | [SPARK-34474](https://issues.apache.org/jira/browse/SPARK-34474) | Improvement | Remove unnecessary Union under Distinct like operators |
| 3.2.0 | [SPARK-34502](https://issues.apache.org/jira/browse/SPARK-34502) | Improvement | Remove unused parameters in join methods |
| 3.2.0 | [SPARK-34514](https://issues.apache.org/jira/browse/SPARK-34514) | Improvement | Push down limit for LEFT SEMI and LEFT ANTI join |
| 3.2.0 | [SPARK-34524](https://issues.apache.org/jira/browse/SPARK-34524) | Improvement | simplify v2 partition commands resolution |
| 3.2.0 | [SPARK-34548](https://issues.apache.org/jira/browse/SPARK-34548) | Improvement | Remove unnecessary children from Union under Distince and Deduplicate |
| 3.2.0 | [SPARK-34573](https://issues.apache.org/jira/browse/SPARK-34573) | Improvement | SQLConf sqlConfEntries map has a global lock, should not lock on get |
| 3.2.0 | [SPARK-34575](https://issues.apache.org/jira/browse/SPARK-34575) | Improvement | Push down limit through window when partitionSpec is empty |
| 3.2.0 | [SPARK-34598](https://issues.apache.org/jira/browse/SPARK-34598) | Improvement | RewritePredicateSubquery Rule must not update Filters without subqueries |
| 3.2.0 | [SPARK-34609](https://issues.apache.org/jira/browse/SPARK-34609) | Improvement | unify resolveExpressionBottomUp and resolveExpressionTopDown |
| 3.2.0 | [SPARK-34622](https://issues.apache.org/jira/browse/SPARK-34622) | Improvement | Push down limit through Project |
| 3.2.0 | [SPARK-34627](https://issues.apache.org/jira/browse/SPARK-34627) | Improvement | Use FunctionIdentifier in UnresolvedTableValuedFunction |
| 3.2.0 | [SPARK-34628](https://issues.apache.org/jira/browse/SPARK-34628) | Improvement | Remove GlobalLimit operator if it's child max row <= limit |
| 3.2.0 | [SPARK-34638](https://issues.apache.org/jira/browse/SPARK-34638) | Improvement | Spark SQL reads unnecessary nested fields (another type of pruning case) |
| 3.2.0 | [SPARK-34639](https://issues.apache.org/jira/browse/SPARK-34639) | Improvement | always remove unnecessary Alias in Analyzer.resolveExpression |
| 3.2.0 | [SPARK-34661](https://issues.apache.org/jira/browse/SPARK-34661) | Improvement | Replaces `OriginalType` with `LogicalTypeAnnotation` in VectorizedColumnReader |
| 3.2.0 | [SPARK-34728](https://issues.apache.org/jira/browse/SPARK-34728) | Improvement | Remove all SQLConf.get if extends from SQLConfHelper |
| 3.2.0 | [SPARK-34758](https://issues.apache.org/jira/browse/SPARK-34758) | Improvement | Simplify Analyzer.resolveLiteralFunction |
| 3.2.0 | [SPARK-34781](https://issues.apache.org/jira/browse/SPARK-34781) | Improvement | Eliminate LEFT SEMI/ANTI join to its left child side with AQE |
| 3.2.0 | [SPARK-34807](https://issues.apache.org/jira/browse/SPARK-34807) | Improvement | Push down filter through window after TransposeWindow |
| 3.2.0 | [SPARK-34808](https://issues.apache.org/jira/browse/SPARK-34808) | Improvement | Removes outer join if it only has distinct on streamed side |
| 3.2.0 | [SPARK-34853](https://issues.apache.org/jira/browse/SPARK-34853) | Improvement | Move partitioning and ordering to common limit trait |
| 3.2.0 | [SPARK-34884](https://issues.apache.org/jira/browse/SPARK-34884) | Improvement | Improve dynamic partition pruning evaluation |
| 3.2.0 | [SPARK-34894](https://issues.apache.org/jira/browse/SPARK-34894) | Improvement | Use 'io.connectionTimeout' as a hint instead of `spark.network.timeout` |
| 3.2.0 | [SPARK-34906](https://issues.apache.org/jira/browse/SPARK-34906) | Improvement | Refactor TreeNode's children handling methods into specialized traits |
| 3.2.0 | [SPARK-34919](https://issues.apache.org/jira/browse/SPARK-34919) | Improvement | Change partitioning to SinglePartition if partition number is 1 |
| 3.2.0 | [SPARK-34920](https://issues.apache.org/jira/browse/SPARK-34920) | New Feature | Introduce SQLSTATE and ERRORCODE to SQL Exception |
| 3.2.0 | [SPARK-34922](https://issues.apache.org/jira/browse/SPARK-34922) | Improvement | Use better CBO cost function |
| 3.2.0 | [SPARK-34932](https://issues.apache.org/jira/browse/SPARK-34932) | Improvement | deprecate GROUP BY ... GROUPING SETS (...) and promote GROUP BY GROUPING SETS (...) |
| 3.2.0 | [SPARK-34945](https://issues.apache.org/jira/browse/SPARK-34945) | Improvement | Fix Javadoc for catalyst module |
| 3.2.0 | [SPARK-34946](https://issues.apache.org/jira/browse/SPARK-34946) | Improvement | Block unsupported correlated scalar subquery in Aggregate |
| 3.2.0 | [SPARK-34969](https://issues.apache.org/jira/browse/SPARK-34969) | Improvement | Followup for Refactor TreeNode's children handling methods into specialized traits (SPARK-34906) |
| 3.2.0 | [SPARK-35041](https://issues.apache.org/jira/browse/SPARK-35041) | Improvement | Revise the overflow in UTF8String |
| 3.2.0 | [SPARK-35109](https://issues.apache.org/jira/browse/SPARK-35109) | Improvement | Fix minor exception messages of HashedRelation and HashJoin |
| 3.2.0 | [SPARK-35141](https://issues.apache.org/jira/browse/SPARK-35141) | Improvement | Support two level map for final hash aggregation |
| 3.2.0 | [SPARK-35204](https://issues.apache.org/jira/browse/SPARK-35204) | Improvement | CatalystTypeConverters of date/timestamp should accept both the old and new Java time classes |
| 3.2.0 | [SPARK-35209](https://issues.apache.org/jira/browse/SPARK-35209) | Improvement | CLONE - CatalystTypeConverters of date/timestamp should accept both the old and new Java time classes |
| 3.2.0 | [SPARK-35225](https://issues.apache.org/jira/browse/SPARK-35225) | Improvement | EXPLAIN command should handle empty output of an analyzed plan |
| 3.2.0 | [SPARK-35281](https://issues.apache.org/jira/browse/SPARK-35281) | Improvement | StaticInvoke should not apply boxing if return type is primitive |
| 3.2.0 | [SPARK-35316](https://issues.apache.org/jira/browse/SPARK-35316) | Improvement | UnwrapCastInBinaryComparison support In/InSet predicate |
| 3.2.0 | [SPARK-35347](https://issues.apache.org/jira/browse/SPARK-35347) | Improvement | Use MethodUtils for method looking up in Invoke and StaticInvoke |
| 3.2.0 | [SPARK-35362](https://issues.apache.org/jira/browse/SPARK-35362) | Improvement | Update null count in the column stats for UNION stats estimation |
| 3.2.0 | [SPARK-35368](https://issues.apache.org/jira/browse/SPARK-35368) | Improvement | [SQL]Update histogram statistics for RANGE operator stats estimation |
| 3.2.0 | [SPARK-35397](https://issues.apache.org/jira/browse/SPARK-35397) | Improvement | Replace sys.err usage with explicit exception type |
| 3.2.0 | [SPARK-35400](https://issues.apache.org/jira/browse/SPARK-35400) | Improvement | improve error message for correlated subquery |
| 3.2.0 | [SPARK-35408](https://issues.apache.org/jira/browse/SPARK-35408) | Improvement | Improve parameter validation in DataFrame.show |
| 3.2.0 | [SPARK-35411](https://issues.apache.org/jira/browse/SPARK-35411) | Improvement | Essential information missing in TreeNode json string |
| 3.2.0 | [SPARK-35479](https://issues.apache.org/jira/browse/SPARK-35479) | Improvement | Format PartitionFilters IN strings in scan nodes |
| 3.2.0 | [SPARK-35604](https://issues.apache.org/jira/browse/SPARK-35604) | Improvement | Fix condition check for FULL OUTER sort merge join |
| 3.2.0 | [SPARK-35689](https://issues.apache.org/jira/browse/SPARK-35689) | Improvement | Add logging for null value retrieval for SymmetricHashJoinStateManager |
| 3.2.0 | [SPARK-35701](https://issues.apache.org/jira/browse/SPARK-35701) | Improvement | Contention on SQLConf.sqlConfEntries and SQLConf.staticConfKeys |
| 3.2.0 | [SPARK-35710](https://issues.apache.org/jira/browse/SPARK-35710) | Improvement | Support DPP + AQE when no reused broadcast exchange |
| 3.2.0 | [SPARK-35712](https://issues.apache.org/jira/browse/SPARK-35712) | Improvement | Simplify ResolveAggregateFunctions |
| 3.2.0 | [SPARK-35756](https://issues.apache.org/jira/browse/SPARK-35756) | Improvement | unionByName should support nested struct also |
| 3.2.0 | [SPARK-35760](https://issues.apache.org/jira/browse/SPARK-35760) | Improvement | Fix the max rows check for broadcast exchange |
| 3.2.0 | [SPARK-35791](https://issues.apache.org/jira/browse/SPARK-35791) | Improvement | Release on-going map properly for NULL-aware ANTI join |
| 3.2.0 | [SPARK-35813](https://issues.apache.org/jira/browse/SPARK-35813) | Improvement | Add new adaptive config into sql-performance-tuning docs |
| 3.2.0 | [SPARK-35906](https://issues.apache.org/jira/browse/SPARK-35906) | Improvement | Remove order by if the maximum number of rows less than or equal to 1 |
| 3.2.0 | [SPARK-35923](https://issues.apache.org/jira/browse/SPARK-35923) | Improvement | Coalesce empty partition with mixed CoalescedPartitionSpec and PartialReducerPartitionSpec |
| 3.2.0 | [SPARK-36161](https://issues.apache.org/jira/browse/SPARK-36161) | Improvement | dropDuplicates does not type check argument |
| 3.2.0 | [SPARK-36320](https://issues.apache.org/jira/browse/SPARK-36320) | Improvement | Fix Series/Index.copy() to drop extra columns. |
| 3.2.0 | [SPARK-36331](https://issues.apache.org/jira/browse/SPARK-36331) | Improvement | Add SQLSTATE guideline |
| 3.2.0 | [SPARK-36444](https://issues.apache.org/jira/browse/SPARK-36444) | Improvement | Remove OptimizeSubqueries from batch of PartitionPruning |
| 3.2.0 | [SPARK-36637](https://issues.apache.org/jira/browse/SPARK-36637) | Improvement | Bad error message when using non-existing named window |
| 4.1.1 | [SPARK-54728](https://issues.apache.org/jira/browse/SPARK-54728) | Improvement | Remove a wrong note in dataframe.isEmpty |
| 4.1.2 | [SPARK-54785](https://issues.apache.org/jira/browse/SPARK-54785) | Improvement | Add support for binary sketch aggregations in KLL |
| 4.1.2 | [SPARK-55070](https://issues.apache.org/jira/browse/SPARK-55070) | Improvement | Allow hidden column in dataframe column resolution |
<!-- AUTO:timeline END -->
