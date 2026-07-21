# Built-in Functions

> Source: `release-notes/spark_all_changelogs.txt`. Timeline rows below are generated from `_catalog.jsonl`; prose outside the AUTO markers is hand-written.

## How it evolved

### 1.x era — math, window, and expression functions

1.4.0 substantially grew the expression library available to DataFrames and SQL: mathematical functions (SPARK-6829), window functions for ranking and analytics (SPARK-1442), and `rollup`/`cube` for multi-dimensional aggregation (SPARK-7320) all landed in the same release, turning Spark SQL from a basic query engine into one with real analytic-function support. 1.5.0 added `expr()`, letting a raw SQL expression string be used as a DataFrame column (SPARK-8668) — a small addition that bridged the DataFrame and SQL-string worlds and remains a common escape hatch when the typed column API lacks an operator you need.

### 2.x era — higher-order functions cap a decade of expression growth

2.0.0 filled out core SQL functions — `IFNULL`/`NULLIF`/`NVL`/`NVL2` (SPARK-14541), `CreateMap` (SPARK-14061), `assert_true`, and XPath UDFs (SPARK-16270) — while porting window functions fully into `SQLContext` (SPARK-12544). 2.2.0 let SELECT aliases be reused in GROUP BY and later expressions (SPARK-14471) and added `input_file_block_start`/`input_file_block_length` (SPARK-18702). 2.3.0 was a UDF-focused release end to end: general UDF enhancements (SPARK-19285), a more comprehensive built-in function library (SPARK-20746), and generated documentation for all of them (SPARK-21485). The era closes with 2.4.0's headline addition — higher-order functions like `transform`, `filter`, `exists`, and `aggregate`, plus 30-odd new built-ins for arrays and maps that avoid exploding them into rows first (SPARK-23899).

### 3.x era — `try_*` functions and SQL-standard additions

3.0.0 added a large batch of SQL-standard functions in one release: inverse hyperbolic trig (`sinh`/`cosh`/`tanh` and their inverses), `bit_and`/`bit_or`, `max_by`/`min_by`, and an index-aware `filter` higher-order function (SPARK-28962). 3.1.1 added `json_array_length`, `json_object_keys`, `current_catalog`, and timestamp constructors (`timestamp_seconds`/`millis`/`micros`). 3.2.0 introduced `try_cast` (SPARK-34881), the first of the `try_*` family that returns null instead of raising under ANSI mode, alongside `regexp`-as-function and datetime arithmetic helpers. 3.3.0 and 3.4.0 kept extending the SQL-standard surface — `SEC`/`CSC`, `TO_NUMBER`/`TRY_TO_NUMBER` (SPARK-38796), URL encode/decode, and the `MASK` data-masking function (SPARK-40687). 3.5.0 added named-argument support for built-in functions (SPARK-44059), letting SQL calls pass arguments by name rather than position.

## Timeline

<!-- AUTO:timeline START -->
| Release | JIRA | Type | Title |
|---|---|---|---|
| 1.4.0 | [SPARK-1442](https://issues.apache.org/jira/browse/SPARK-1442) | prose | Window functions in Spark SQL and DataFrames |
| 1.4.0 | [SPARK-6829](https://issues.apache.org/jira/browse/SPARK-6829) | prose | Mathematical functions in DataFrames |
| 1.4.0 | [SPARK-7320](https://issues.apache.org/jira/browse/SPARK-7320) | prose | Rollup and cube functions |
| 1.5.0 | [SPARK-7813](https://issues.apache.org/jira/browse/SPARK-7813) | Improvement | Push code generation into expression definition |
| 1.5.0 | [SPARK-7886](https://issues.apache.org/jira/browse/SPARK-7886) | Improvement | Add built-in expressions to FunctionRegistry |
| 1.5.0 | [SPARK-7887](https://issues.apache.org/jira/browse/SPARK-7887) | Improvement | Remove EvaluatedType from SQL Expression |
| 1.5.0 | [SPARK-8117](https://issues.apache.org/jira/browse/SPARK-8117) | Improvement | Push codegen into Expression |
| 1.5.0 | [SPARK-8159](https://issues.apache.org/jira/browse/SPARK-8159) | Improvement | Improve expression function coverage (Spark 1.5) |
| 1.5.0 | [SPARK-8164](https://issues.apache.org/jira/browse/SPARK-8164) | Improvement | transformExpressions should support nested expression sequence |
| 1.5.0 | [SPARK-8349](https://issues.apache.org/jira/browse/SPARK-8349) | Improvement | Use expression constructors (rather than apply) in FunctionRegistry |
| 1.5.0 | [SPARK-8589](https://issues.apache.org/jira/browse/SPARK-8589) | Improvement | cleanup DateTimeUtils |
| 1.5.0 | [SPARK-8599](https://issues.apache.org/jira/browse/SPARK-8599) | Improvement | Improve non-deterministic expression handling |
| 1.5.0 | [SPARK-8668](https://issues.apache.org/jira/browse/SPARK-8668) | prose | expr function for turning a SQL expression into a DataFrame column |
| 1.5.0 | [SPARK-8686](https://issues.apache.org/jira/browse/SPARK-8686) | Improvement | DataFrame should support `where` with expression represented by String |
| 1.5.0 | [SPARK-8713](https://issues.apache.org/jira/browse/SPARK-8713) | Improvement | Support codegen for not thread-safe expressions |
| 1.5.0 | [SPARK-8759](https://issues.apache.org/jira/browse/SPARK-8759) | Improvement | add default eval to binary and unary expression according to default behavior of nullable |
| 1.5.0 | [SPARK-8876](https://issues.apache.org/jira/browse/SPARK-8876) | Improvement | Remove InternalRow type alias in expressions package |
| 1.5.0 | [SPARK-9251](https://issues.apache.org/jira/browse/SPARK-9251) | Improvement | do not order by expressions which still need evaluation |
| 1.5.0 | [SPARK-9287](https://issues.apache.org/jira/browse/SPARK-9287) | Improvement | Speedup unit test of Date expressions |
| 1.5.0 | [SPARK-9351](https://issues.apache.org/jira/browse/SPARK-9351) | Improvement | remove literals from grouping expressions in Aggregate |
| 1.5.0 | [SPARK-9628](https://issues.apache.org/jira/browse/SPARK-9628) | Improvement | Rename Int and Long to SQLDate SQLTimestamp In DateTimeUtils |
| 1.6.0 | [SPARK-7173](https://issues.apache.org/jira/browse/SPARK-7173) | Improvement | Support YARN node label expressions for the application master |
| 1.6.0 | [SPARK-8829](https://issues.apache.org/jira/browse/SPARK-8829) | Umbrella | Improve expression performance |
| 1.6.0 | [SPARK-10389](https://issues.apache.org/jira/browse/SPARK-10389) | Improvement | support order by non-attribute grouping expression on Aggregate |
| 1.6.0 | [SPARK-10743](https://issues.apache.org/jira/browse/SPARK-10743) | Improvement | keep the name of expression if possible when do cast |
| 1.6.0 | [SPARK-11179](https://issues.apache.org/jira/browse/SPARK-11179) | Improvement | Push filters through aggregate if filters are subset of 'group by' expressions |
| 1.6.0 | [SPARK-11532](https://issues.apache.org/jira/browse/SPARK-11532) | Improvement | Remove implicit conversion from Expression to Column |
| 1.6.0 | [SPARK-11536](https://issues.apache.org/jira/browse/SPARK-11536) | Improvement | Remove the internal implicit conversion from Expression to Column in functions.scala |
| 1.6.0 | [SPARK-11768](https://issues.apache.org/jira/browse/SPARK-11768) | New Feature | Support now function in SQL (alias for current_timestamp) |
| 1.6.0 | [SPARK-11897](https://issues.apache.org/jira/browse/SPARK-11897) | Improvement | Add @scala.annotations.varargs to sql functions that don't have it |
| 1.6.3 | [SPARK-16341](https://issues.apache.org/jira/browse/SPARK-16341) | Improvement | [SQL] In regexp_replace function column and/or column expression should also allowed as replacement. |
| 2.0.0 | [SPARK-7712](https://issues.apache.org/jira/browse/SPARK-7712) | Improvement | Window Function Improvements |
| 2.0.0 | [SPARK-9571](https://issues.apache.org/jira/browse/SPARK-9571) | Umbrella | Improve expression function coverage |
| 2.0.0 | [SPARK-10343](https://issues.apache.org/jira/browse/SPARK-10343) | Improvement | Consider nullability of expression in codegen |
| 2.0.0 | [SPARK-11878](https://issues.apache.org/jira/browse/SPARK-11878) | Improvement | Eliminate distribute by in case group by is present with exactly the same grouping expressions |
| 2.0.0 | [SPARK-12284](https://issues.apache.org/jira/browse/SPARK-12284) | Improvement | Output UnsafeRow from window function |
| 2.0.0 | [SPARK-12295](https://issues.apache.org/jira/browse/SPARK-12295) | Improvement | Manage the memory used by window function |
| 2.0.0 | [SPARK-12454](https://issues.apache.org/jira/browse/SPARK-12454) | Improvement | Add ExpressionDescription to expressions that are registered in FunctionRegistry |
| 2.0.0 | [SPARK-12544](https://issues.apache.org/jira/browse/SPARK-12544) | New Feature | Support window functions in SQLContext |
| 2.0.0 | [SPARK-12580](https://issues.apache.org/jira/browse/SPARK-12580) | Improvement | Remove string concatenations from usage and extended in @ExpressionDescription |
| 2.0.0 | [SPARK-12642](https://issues.apache.org/jira/browse/SPARK-12642) | Improvement | improve the hash expression to be decoupled from unsafe row |
| 2.0.0 | [SPARK-12767](https://issues.apache.org/jira/browse/SPARK-12767) | Improvement | Improve conditional expressions |
| 2.0.0 | [SPARK-12840](https://issues.apache.org/jira/browse/SPARK-12840) | Improvement | Support passing arbitrary objects (not just expressions) into code generated classes |
| 2.0.0 | [SPARK-12888](https://issues.apache.org/jira/browse/SPARK-12888) | Improvement | benchmark the new hash expression |
| 2.0.0 | [SPARK-12949](https://issues.apache.org/jira/browse/SPARK-12949) | Improvement | Support common expression elimination |
| 2.0.0 | [SPARK-13072](https://issues.apache.org/jira/browse/SPARK-13072) | Improvement | simplify and improve murmur3 hash expression codegen |
| 2.0.0 | [SPARK-13093](https://issues.apache.org/jira/browse/SPARK-13093) | Improvement | improve null check in nullSafeCodeGen for unary, binary and ternary expression |
| 2.0.0 | [SPARK-13135](https://issues.apache.org/jira/browse/SPARK-13135) | Improvement | Don't print expressions recursively in generated code |
| 2.0.0 | [SPARK-13467](https://issues.apache.org/jira/browse/SPARK-13467) | Improvement | abstract python function to simplify pyspark code |
| 2.0.0 | [SPARK-13694](https://issues.apache.org/jira/browse/SPARK-13694) | Improvement | QueryPlan.expressions should always include all expressions |
| 2.0.0 | [SPARK-14061](https://issues.apache.org/jira/browse/SPARK-14061) | New Feature | implement CreateMap |
| 2.0.0 | [SPARK-14202](https://issues.apache.org/jira/browse/SPARK-14202) | Improvement | python_full_outer_join should use generator expression instead of list comp |
| 2.0.0 | [SPARK-14345](https://issues.apache.org/jira/browse/SPARK-14345) | Improvement | Decouple deserializer expression resolution from ObjectOperator |
| 2.0.0 | [SPARK-14415](https://issues.apache.org/jira/browse/SPARK-14415) | Improvement | All functions should show usages by command `DESC FUNCTION` |
| 2.0.0 | [SPARK-14541](https://issues.apache.org/jira/browse/SPARK-14541) | New Feature | SQL function: IFNULL, NULLIF, NVL and NVL2 |
| 2.0.0 | [SPARK-14637](https://issues.apache.org/jira/browse/SPARK-14637) | Improvement | object expressions cleanup |
| 2.0.0 | [SPARK-14655](https://issues.apache.org/jira/browse/SPARK-14655) | Improvement | Add `assert_true` function |
| 2.0.0 | [SPARK-15021](https://issues.apache.org/jira/browse/SPARK-15021) | New Feature | cannot run aggregate function on explode result |
| 2.0.0 | [SPARK-15932](https://issues.apache.org/jira/browse/SPARK-15932) | Improvement | document the contract of encoder serializer expressions |
| 2.0.0 | [SPARK-16270](https://issues.apache.org/jira/browse/SPARK-16270) | Improvement | Implement xpath user defined functions |
| 2.0.0 | [SPARK-16582](https://issues.apache.org/jira/browse/SPARK-16582) | Improvement | Explicitly define isNull = false for non-nullable expressions |
| 2.0.0 | [SPARK-16584](https://issues.apache.org/jira/browse/SPARK-16584) | Improvement | Move regexp unit tests to RegexpExpressionsSuite |
| 2.0.1 | [SPARK-16324](https://issues.apache.org/jira/browse/SPARK-16324) | Improvement | regexp_extract should doc that it returns empty string when match fails |
| 2.1.0 | [SPARK-16324](https://issues.apache.org/jira/browse/SPARK-16324) | Improvement | regexp_extract should doc that it returns empty string when match fails |
| 2.1.0 | [SPARK-16340](https://issues.apache.org/jira/browse/SPARK-16340) | Improvement | In regexp_replace function column and/or column expression should also allowed as replacement. |
| 2.1.0 | [SPARK-16694](https://issues.apache.org/jira/browse/SPARK-16694) | Improvement | Use for/foreach rather than map for Unit expressions whose side effects are required |
| 2.1.0 | [SPARK-16888](https://issues.apache.org/jira/browse/SPARK-16888) | Improvement | Implements eval method for expression AssertNotNull |
| 2.1.0 | [SPARK-17682](https://issues.apache.org/jira/browse/SPARK-17682) | Improvement | nit: Mark children as final for Unary, Binary, Leaf expression and plan nodes |
| 2.1.0 | [SPARK-17821](https://issues.apache.org/jira/browse/SPARK-17821) | Improvement | Expression Canonicalization should support Add and Or |
| 2.1.0 | [SPARK-17845](https://issues.apache.org/jira/browse/SPARK-17845) | Improvement | Improve window function frame boundary API in DataFrame |
| 2.1.0 | [SPARK-18287](https://issues.apache.org/jira/browse/SPARK-18287) | Improvement | Move hash expressions from misc.scala into hash.scala |
| 2.1.0 | [SPARK-18296](https://issues.apache.org/jira/browse/SPARK-18296) | Improvement | Use consistent naming for expression test suites |
| 2.2.0 | [SPARK-14049](https://issues.apache.org/jira/browse/SPARK-14049) | Improvement | Add functionality in spark history sever API to query applications by end time |
| 2.2.0 | [SPARK-14471](https://issues.apache.org/jira/browse/SPARK-14471) | New Feature | The alias created in SELECT could be used in GROUP BY and followed expressions |
| 2.2.0 | [SPARK-16609](https://issues.apache.org/jira/browse/SPARK-16609) | Improvement | Single function for parsing timestamps/dates |
| 2.2.0 | [SPARK-17161](https://issues.apache.org/jira/browse/SPARK-17161) | Improvement | Add PySpark-ML JavaWrapper convenience function to create py4j JavaArrays |
| 2.2.0 | [SPARK-18186](https://issues.apache.org/jira/browse/SPARK-18186) | Improvement | Migrate HiveUDAFFunction to TypedImperativeAggregate for partial aggregation support |
| 2.2.0 | [SPARK-18601](https://issues.apache.org/jira/browse/SPARK-18601) | Improvement | Simplify Create/Get complex expression pairs in optimizer |
| 2.2.0 | [SPARK-18702](https://issues.apache.org/jira/browse/SPARK-18702) | New Feature | input_file_block_start and input_file_block_length function |
| 2.2.0 | [SPARK-19127](https://issues.apache.org/jira/browse/SPARK-19127) | Improvement | Inconsistencies in dense_rank and rank documentation |
| 2.2.0 | [SPARK-19518](https://issues.apache.org/jira/browse/SPARK-19518) | Improvement | IGNORE NULLS in first_value / last_value should be supported in SQL statements |
| 2.2.0 | [SPARK-19850](https://issues.apache.org/jira/browse/SPARK-19850) | Improvement | Support aliased expressions in function parameters |
| 2.2.0 | [SPARK-20303](https://issues.apache.org/jira/browse/SPARK-20303) | Improvement | Rename createTempFunction to registerFunction |
| 2.2.0 | [SPARK-20350](https://issues.apache.org/jira/browse/SPARK-20350) | Improvement | Apply Complementation Laws during boolean expression simplification |
| 2.2.0 | [SPARK-20409](https://issues.apache.org/jira/browse/SPARK-20409) | Improvement | fail early if aggregate function in GROUP BY |
| 2.3.0 | [SPARK-19285](https://issues.apache.org/jira/browse/SPARK-19285) | prose | UDF enhancements |
| 2.3.0 | [SPARK-20746](https://issues.apache.org/jira/browse/SPARK-20746) | prose | More comprehensive SQL built-in functions |
| 2.3.0 | [SPARK-21485](https://issues.apache.org/jira/browse/SPARK-21485) | prose | Spark SQL documentation generation for built-in functions |
| 2.4.0 | [SPARK-23899](https://issues.apache.org/jira/browse/SPARK-23899) | prose | Higher-order functions and 30+ built-in functions for complex data types |
| 3.0.0 | [SPARK-12045](https://issues.apache.org/jira/browse/SPARK-12045) | Improvement | Use joda's DateTime to replace Calendar |
| 3.0.0 | [SPARK-20636](https://issues.apache.org/jira/browse/SPARK-20636) | Improvement | Eliminate unnecessary shuffle with adjacent Window expressions |
| 3.0.0 | [SPARK-23356](https://issues.apache.org/jira/browse/SPARK-23356) | Improvement | Pushes Project to both sides of Union when expression is non-deterministic |
| 3.0.0 | [SPARK-25202](https://issues.apache.org/jira/browse/SPARK-25202) | New Feature | SQL Function Split Should Respect Limit Argument |
| 3.0.0 | [SPARK-26205](https://issues.apache.org/jira/browse/SPARK-26205) | Improvement | Optimize InSet expression for bytes, shorts, ints, dates |
| 3.0.0 | [SPARK-26353](https://issues.apache.org/jira/browse/SPARK-26353) | Improvement | Add typed aggregate functions(max/min) to the example module |
| 3.0.0 | [SPARK-26720](https://issues.apache.org/jira/browse/SPARK-26720) | Improvement | Remove unused methods from DateTimeUtils |
| 3.0.0 | [SPARK-26730](https://issues.apache.org/jira/browse/SPARK-26730) | Improvement | Strip redundant AssertNotNull expression for ExpressionEncoder's serializer |
| 3.0.0 | [SPARK-26735](https://issues.apache.org/jira/browse/SPARK-26735) | Improvement | Verify plan integrity for special expressions |
| 3.0.0 | [SPARK-26853](https://issues.apache.org/jira/browse/SPARK-26853) | Improvement | Add example and version for commonly used aggregate function descriptions |
| 3.0.0 | [SPARK-26979](https://issues.apache.org/jira/browse/SPARK-26979) | Improvement | [PySpark] Some SQL functions do not take column names |
| 3.0.0 | [SPARK-27252](https://issues.apache.org/jira/browse/SPARK-27252) | Improvement | Make current_date() independent from time zones |
| 3.0.0 | [SPARK-27255](https://issues.apache.org/jira/browse/SPARK-27255) | Improvement | Aggregate functions should not be allowed in WHERE |
| 3.0.0 | [SPARK-27328](https://issues.apache.org/jira/browse/SPARK-27328) | Improvement | Create 'deprecate' property in ExpressionDescription for SQL functions documentation |
| 3.0.0 | [SPARK-27346](https://issues.apache.org/jira/browse/SPARK-27346) | Improvement | Loosen the newline assert condition on 'examples' field in ExpressionInfo |
| 3.0.0 | [SPARK-27425](https://issues.apache.org/jira/browse/SPARK-27425) | Improvement | Add count_if functions |
| 3.0.0 | [SPARK-27514](https://issues.apache.org/jira/browse/SPARK-27514) | Improvement | Empty window expression results in error in optimizer |
| 3.0.0 | [SPARK-27606](https://issues.apache.org/jira/browse/SPARK-27606) | Improvement | Deprecate `extended` field in ExpressionDescription/ExpressionInfo |
| 3.0.0 | [SPARK-27653](https://issues.apache.org/jira/browse/SPARK-27653) | New Feature | Add max_by() / min_by() SQL aggregate functions |
| 3.0.0 | [SPARK-27672](https://issues.apache.org/jira/browse/SPARK-27672) | Improvement | Add since info to string expressions |
| 3.0.0 | [SPARK-27673](https://issues.apache.org/jira/browse/SPARK-27673) | Improvement | Add since info to random. regex, null expressions |
| 3.0.0 | [SPARK-27879](https://issues.apache.org/jira/browse/SPARK-27879) | prose | bit_and, bit_or |
| 3.0.0 | [SPARK-28133](https://issues.apache.org/jira/browse/SPARK-28133) | prose | sinh, cosh, tanh, asinh, acosh, atanh |
| 3.0.0 | [SPARK-28521](https://issues.apache.org/jira/browse/SPARK-28521) | Improvement | Fix error message for built-in functions |
| 3.0.0 | [SPARK-28581](https://issues.apache.org/jira/browse/SPARK-28581) | Improvement | Replace _FUNC_ in UDF ExpressionInfo |
| 3.0.0 | [SPARK-28782](https://issues.apache.org/jira/browse/SPARK-28782) | Improvement | explode() fails on aggregate expressions |
| 3.0.0 | [SPARK-28962](https://issues.apache.org/jira/browse/SPARK-28962) | prose | filter can now take the index as input as well as the element |
| 3.0.0 | [SPARK-29233](https://issues.apache.org/jira/browse/SPARK-29233) | Improvement | Add regex expression checks for executorEnv in K8S mode |
| 3.0.0 | [SPARK-29237](https://issues.apache.org/jira/browse/SPARK-29237) | Improvement | Use _FUNC_ in expression examples |
| 3.0.0 | [SPARK-29491](https://issues.apache.org/jira/browse/SPARK-29491) | Improvement | Add bit_count function support |
| 3.0.0 | [SPARK-29554](https://issues.apache.org/jira/browse/SPARK-29554) | Improvement | Add `version` SQL function |
| 3.0.0 | [SPARK-29961](https://issues.apache.org/jira/browse/SPARK-29961) | Improvement | Implement typeof builtin function |
| 3.0.0 | [SPARK-30832](https://issues.apache.org/jira/browse/SPARK-30832) | Improvement | SQL function doc headers should link to anchors |
| 3.0.0 | [SPARK-31119](https://issues.apache.org/jira/browse/SPARK-31119) | Improvement | Add interval value support for extract expression as source |
| 3.0.0 | [SPARK-31195](https://issues.apache.org/jira/browse/SPARK-31195) | Improvement | Reuse days rebase functions of DateTimeUtils in DaysWritable |
| 3.0.0 | [SPARK-31205](https://issues.apache.org/jira/browse/SPARK-31205) | Improvement | support string literal as the second argument of date_add/date_sub functions |
| 3.0.0 | [SPARK-31372](https://issues.apache.org/jira/browse/SPARK-31372) | Improvement | Display expression schema for double checkout alias |
| 3.0.0 | [SPARK-31393](https://issues.apache.org/jira/browse/SPARK-31393) | Improvement | Show the correct alias in schema for expression |
| 3.0.0 | [SPARK-31408](https://issues.apache.org/jira/browse/SPARK-31408) | Umbrella | Build Spark’s own datetime pattern definition |
| 3.0.0 | [SPARK-31429](https://issues.apache.org/jira/browse/SPARK-31429) | Improvement | Add additional fields in ExpressionDescription for more granular category in documentation |
| 3.0.0 | [SPARK-31474](https://issues.apache.org/jira/browse/SPARK-31474) | Improvement | Consistancy between dayofweek/dow in extract expression and dayofweek function |
| 3.0.0 | [SPARK-31476](https://issues.apache.org/jira/browse/SPARK-31476) | Improvement | Add an ExpressionInfo entry for EXTRACT |
| 3.0.0 | [SPARK-31562](https://issues.apache.org/jira/browse/SPARK-31562) | Improvement | Update ExpressionDescription for substring, current_date, and current_timestamp |
| 3.1.1 | [SPARK-30352](https://issues.apache.org/jira/browse/SPARK-30352) | prose | current_catalog |
| 3.1.1 | [SPARK-31008](https://issues.apache.org/jira/browse/SPARK-31008) | prose | json_array_length |
| 3.1.1 | [SPARK-31009](https://issues.apache.org/jira/browse/SPARK-31009) | prose | json_object_keys |
| 3.1.1 | [SPARK-31710](https://issues.apache.org/jira/browse/SPARK-31710) | prose | timestamp_seconds, timestamp_millis, timestamp_micros |
| 3.1.1 | [SPARK-34244](https://issues.apache.org/jira/browse/SPARK-34244) | Improvement | Remove the Scala function version of regexp_extract_all |
| 3.2.0 | [SPARK-33527](https://issues.apache.org/jira/browse/SPARK-33527) | New Feature | Extend the function of decode so as consistent with mainstream databases |
| 3.2.0 | [SPARK-33597](https://issues.apache.org/jira/browse/SPARK-33597) | New Feature | Support REGEXP_LIKE for consistent with mainstream databases |
| 3.2.0 | [SPARK-33806](https://issues.apache.org/jira/browse/SPARK-33806) | Improvement | limit partition num to 1 when distributing by foldable expressions |
| 3.2.0 | [SPARK-33890](https://issues.apache.org/jira/browse/SPARK-33890) | Improvement | Improve the implement of trim/trimleft/trimright |
| 3.2.0 | [SPARK-33910](https://issues.apache.org/jira/browse/SPARK-33910) | Umbrella | Simplify/Optimize conditional expressions |
| 3.2.0 | [SPARK-33995](https://issues.apache.org/jira/browse/SPARK-33995) | New Feature | Make datetime addition easier for years, weeks, hours, minutes, and seconds |
| 3.2.0 | [SPARK-34067](https://issues.apache.org/jira/browse/SPARK-34067) | Improvement | PartitionPruning push down pruningHasBenefit function into insertPredicate function to decrease calculate time |
| 3.2.0 | [SPARK-34094](https://issues.apache.org/jira/browse/SPARK-34094) | Improvement | Extends StringTranslate to support unicode characters whose code point >= U+10000 |
| 3.2.0 | [SPARK-34350](https://issues.apache.org/jira/browse/SPARK-34350) | Improvement | replace withTimeZone defined in OracleIntegrationSuite with DateTimeTestUtils.withDefaultTimeZone |
| 3.2.0 | [SPARK-34376](https://issues.apache.org/jira/browse/SPARK-34376) | New Feature | Support regexp as a function |
| 3.2.0 | [SPARK-34451](https://issues.apache.org/jira/browse/SPARK-34451) | Improvement | Add alternatives for datetime rebasing SQL configs and deprecate legacy configs |
| 3.2.0 | [SPARK-34881](https://issues.apache.org/jira/browse/SPARK-34881) | prose | try_cast |
| 3.2.0 | [SPARK-35005](https://issues.apache.org/jira/browse/SPARK-35005) | Improvement | Improve error msg if UTF8String concatWs length overflow |
| 3.2.0 | [SPARK-35206](https://issues.apache.org/jira/browse/SPARK-35206) | Improvement | Extract common get project path ability as function to SparkFunctionSuite |
| 3.2.0 | [SPARK-35273](https://issues.apache.org/jira/browse/SPARK-35273) | Improvement | CombineFilters support non-deterministic expressions |
| 3.2.0 | [SPARK-35418](https://issues.apache.org/jira/browse/SPARK-35418) | Improvement | Add sentences function to functions.{scala,py} |
| 3.2.0 | [SPARK-35537](https://issues.apache.org/jira/browse/SPARK-35537) | Improvement | Introduce a util function to check whether the underlying expressions of the columns are the same. |
| 3.2.0 | [SPARK-35618](https://issues.apache.org/jira/browse/SPARK-35618) | Improvement | Resolve star expressions in subquery |
| 3.2.0 | [SPARK-35899](https://issues.apache.org/jira/browse/SPARK-35899) | Improvement | Add a utility to convert connector expressions to Catalyst expressions |
| 3.2.0 | [SPARK-36567](https://issues.apache.org/jira/browse/SPARK-36567) | Improvement | Support foldable special datetime values in CAST |
| 3.2.1 | [SPARK-30789](https://issues.apache.org/jira/browse/SPARK-30789) | prose | Support IGNORE/RESPECT NULLS for LEAD/LAG/NTH_VALUE/FIRST_VALUE/LAST_VALUE |
| 3.3.0 | [SPARK-36683](https://issues.apache.org/jira/browse/SPARK-36683) | prose | Add new built-in SQL functions: SEC and CSC |
| 3.3.0 | [SPARK-38783](https://issues.apache.org/jira/browse/SPARK-38783) | prose | New built-in functions and their extensions |
| 3.4.0 | [SPARK-38796](https://issues.apache.org/jira/browse/SPARK-38796) | prose | Support the TO_NUMBER and TRY_TO_NUMBER SQL functions according to a new specification |
| 3.4.0 | [SPARK-39741](https://issues.apache.org/jira/browse/SPARK-39741) | prose | Support url encode/decode as built-in function and tidy up url-related functions |
| 3.4.0 | [SPARK-40687](https://issues.apache.org/jira/browse/SPARK-40687) | prose | Support data masking built-in function MASK |
| 3.5.0 | [SPARK-44059](https://issues.apache.org/jira/browse/SPARK-44059) | prose | Add analyzer support of named arguments for built-in functions |
| 4.1.1 | [SPARK-54843](https://issues.apache.org/jira/browse/SPARK-54843) | Improvement | Try_to_number expression not working for empty string input |
<!-- AUTO:timeline END -->
