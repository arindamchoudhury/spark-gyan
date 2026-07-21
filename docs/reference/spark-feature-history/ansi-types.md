# ANSI & Data Types

> Source: `release-notes/spark_all_changelogs.txt`. Timeline rows below are generated from `_catalog.jsonl`; prose outside the AUTO markers is hand-written.

## How it evolved

### 1.x era — type precision and Tungsten encoding

The 1.x line spent less time on ANSI semantics and more on nailing down type precision and low-level representation. 1.5.0 settled `TimestampType` on microsecond precision (SPARK-8866), added `CalendarIntervalType` for time intervals (SPARK-8943), and overhauled expression type coercion, casting, and type checking (SPARK-8947). The same release taught the Tungsten unsafe row format to hold `ArrayType`, `MapType`, `StructType`, and `BinaryType` values directly, plus decimals wider than 18 digits (SPARK-9644). 1.6.0 added decimal support for `ceil`/`floor` (SPARK-11076), an orderable `ArrayType` (SPARK-11738), and `BigDecimal`/`Date`/`Timestamp` support in the new Encoder API (SPARK-12195) — groundwork for the strict typing later formalized as ANSI mode.

### 2.x era — array coercion and session-local time semantics

The 2.x line kept extending type coercion into composite types rather than touching ANSI mode directly. 2.0.0 taught `RowEncoder` to accept arrays as the external type for `ArrayType` (SPARK-15351) and JSON schema inference to treat floating-point numbers as `BigDecimal` (SPARK-12749). 2.1.0 added `BigDecimal` literal parsing and parsed scientific-notation decimals as `Decimal` rather than `Double` (SPARK-17246, SPARK-17258). 2.2.0 was the busiest release here: session-local timezone support (SPARK-18350) let each session control how timestamps were interpreted, general type coercion and implicit casts were extended to cover `ArrayType` (SPARK-18624, SPARK-19435), and `ArrayType` gained `to_json` support (SPARK-19849). SPARK-20463 (2.3.0) folded in broader ANSI SQL compliance and Hive-compatibility fixes.

### 3.x era — ANSI mode reaches general availability

3.0.0 built out the ANSI groundwork: a store-assignment policy for table inserts (SPARK-28495, made default in SPARK-28885), ANSI SQL filter clauses, `OVERLAY`, and reserved-keyword parsing (SPARK-26215). 3.1.1 added char/varchar data types (SPARK-33480) and runtime errors instead of silent nulls under ANSI mode (SPARK-33275). 3.2.0 was the milestone release: ANSI SQL mode reached general availability (SPARK-35030), alongside ANSI SQL `INTERVAL` types (SPARK-27790) and `LATERAL` subqueries (SPARK-34382). 3.3.0 added a wave of ANSI aggregate functions — `REGR_R2`, `REGR_SXY`, and others — plus `TRY_AVG` and stricter reserved-keyword handling (SPARK-37724). 3.4.0 extended ANSI casting to intervals and decimals and rounded out the `REGR_*` family, while later 3.5.x point releases stayed largely maintenance on the same framework.

## Timeline

<!-- AUTO:timeline START -->
| Release | JIRA | Type | Title |
|---|---|---|---|
| 1.5.0 | [SPARK-4302](https://issues.apache.org/jira/browse/SPARK-4302) | Improvement | Support fixed precision decimal type in JsonParser |
| 1.5.0 | [SPARK-8189](https://issues.apache.org/jira/browse/SPARK-8189) | Improvement | Use 100ns precision for TimestampType |
| 1.5.0 | [SPARK-8692](https://issues.apache.org/jira/browse/SPARK-8692) | Improvement | re-order the case statements that handling catalyst data types |
| 1.5.0 | [SPARK-8866](https://issues.apache.org/jira/browse/SPARK-8866) | Improvement | Use 1 microsecond (us) precision for TimestampType |
| 1.5.0 | [SPARK-8943](https://issues.apache.org/jira/browse/SPARK-8943) | Improvement | CalendarIntervalType for time intervals |
| 1.5.0 | [SPARK-8947](https://issues.apache.org/jira/browse/SPARK-8947) | Improvement | Improve expression type coercion, casting & checking |
| 1.5.0 | [SPARK-8995](https://issues.apache.org/jira/browse/SPARK-8995) | Improvement | Cast date strings with date, date and time and just time information to DateType and TimestampTzpe |
| 1.5.0 | [SPARK-9360](https://issues.apache.org/jira/browse/SPARK-9360) | Improvement | Support BinaryType in PrefixComparators for UnsafeExternalSort |
| 1.5.0 | [SPARK-9373](https://issues.apache.org/jira/browse/SPARK-9373) | Improvement | Support StructType in Tungsten style Projection |
| 1.5.0 | [SPARK-9389](https://issues.apache.org/jira/browse/SPARK-9389) | Umbrella | Support ArrayType in Tungsten |
| 1.5.0 | [SPARK-9413](https://issues.apache.org/jira/browse/SPARK-9413) | Umbrella | Support MapType in Tungsten |
| 1.5.0 | [SPARK-9632](https://issues.apache.org/jira/browse/SPARK-9632) | Improvement | update InternalRow.toSeq to make it accept data type info |
| 1.5.0 | [SPARK-9644](https://issues.apache.org/jira/browse/SPARK-9644) | Improvement | Support update DecimalType with precision > 18 in UnsafeRow |
| 1.5.0 | [SPARK-9675](https://issues.apache.org/jira/browse/SPARK-9675) | Improvement | GenerateUnsafeProjection seems to corrupt MapType data |
| 1.5.0 | [SPARK-9759](https://issues.apache.org/jira/browse/SPARK-9759) | Improvement | Improve performance of Decimal.times() and casting from integral |
| 1.6.0 | [SPARK-7142](https://issues.apache.org/jira/browse/SPARK-7142) | Improvement | Minor enhancement to BooleanSimplification Optimizer rule |
| 1.6.0 | [SPARK-10684](https://issues.apache.org/jira/browse/SPARK-10684) | Improvement | StructType.interpretedOrdering need not to be serialized |
| 1.6.0 | [SPARK-11076](https://issues.apache.org/jira/browse/SPARK-11076) | Improvement | Decimal Support for Ceil/Floor |
| 1.6.0 | [SPARK-11738](https://issues.apache.org/jira/browse/SPARK-11738) | Improvement | Make ArrayType orderable |
| 1.6.0 | [SPARK-12195](https://issues.apache.org/jira/browse/SPARK-12195) | Improvement | Adding BigDecimal, Date and Timestamp into Encoder |
| 2.0.0 | [SPARK-12498](https://issues.apache.org/jira/browse/SPARK-12498) | Improvement | BooleanSimplification cleanup |
| 2.0.0 | [SPARK-12749](https://issues.apache.org/jira/browse/SPARK-12749) | Improvement | Spark SQL JSON schema infernce should allow floating-point numbers as BigDecimal |
| 2.0.0 | [SPARK-13100](https://issues.apache.org/jira/browse/SPARK-13100) | Improvement | improving the performance of stringToDate method in DateTimeUtils.scala |
| 2.0.0 | [SPARK-13185](https://issues.apache.org/jira/browse/SPARK-13185) | Improvement | Improve the performance of DateTimeUtils.StringToDate by reusing Calendar objects |
| 2.0.0 | [SPARK-13593](https://issues.apache.org/jira/browse/SPARK-13593) | Improvement | improve the `createDataFrame` method to accept data type string and verify the data |
| 2.0.0 | [SPARK-13842](https://issues.apache.org/jira/browse/SPARK-13842) | Improvement | Consider __iter__ and __getitem__ methods for pyspark.sql.types.StructType |
| 2.0.0 | [SPARK-14029](https://issues.apache.org/jira/browse/SPARK-14029) | Improvement | Improve BooleanSimplification optimization by implementing `Not` canonicalization |
| 2.0.0 | [SPARK-14085](https://issues.apache.org/jira/browse/SPARK-14085) | Improvement | Star Expansion for Hash |
| 2.0.0 | [SPARK-15351](https://issues.apache.org/jira/browse/SPARK-15351) | Improvement | RowEncoder should support array as the external type for ArrayType |
| 2.0.0 | [SPARK-16175](https://issues.apache.org/jira/browse/SPARK-16175) | Improvement | Handle None for all Python UDT |
| 2.0.1 | [SPARK-16805](https://issues.apache.org/jira/browse/SPARK-16805) | Improvement | Log timezone when query result does not match |
| 2.0.1 | [SPARK-17246](https://issues.apache.org/jira/browse/SPARK-17246) | Improvement | Support BigDecimal literal parsing |
| 2.1.0 | [SPARK-16805](https://issues.apache.org/jira/browse/SPARK-16805) | Improvement | Log timezone when query result does not match |
| 2.1.0 | [SPARK-17246](https://issues.apache.org/jira/browse/SPARK-17246) | Improvement | Support BigDecimal literal parsing |
| 2.1.0 | [SPARK-17258](https://issues.apache.org/jira/browse/SPARK-17258) | Improvement | Scientific decimals should be parsed as Decimals |
| 2.1.0 | [SPARK-17683](https://issues.apache.org/jira/browse/SPARK-17683) | Improvement | Support ArrayType in Literal.apply |
| 2.1.0 | [SPARK-18214](https://issues.apache.org/jira/browse/SPARK-18214) | Improvement | Simplify RuntimeReplaceable type coercion |
| 2.2.0 | [SPARK-18350](https://issues.apache.org/jira/browse/SPARK-18350) | New Feature | Support session local timezone |
| 2.2.0 | [SPARK-18624](https://issues.apache.org/jira/browse/SPARK-18624) | Improvement | Implict cast between ArrayTypes |
| 2.2.0 | [SPARK-19435](https://issues.apache.org/jira/browse/SPARK-19435) | Improvement | Type coercion between ArrayTypes |
| 2.2.0 | [SPARK-19441](https://issues.apache.org/jira/browse/SPARK-19441) | Improvement | Remove IN type coercion from PromoteStrings |
| 2.2.0 | [SPARK-19849](https://issues.apache.org/jira/browse/SPARK-19849) | Improvement | Support ArrayType in to_json function/expression |
| 2.3.0 | [SPARK-20463](https://issues.apache.org/jira/browse/SPARK-20463) | prose | Improved ANSI SQL compliance and Hive compatibility |
| 3.0.0 | — | prose | Better ANSI SQL compatibility |
| 3.0.0 | [SPARK-20964](https://issues.apache.org/jira/browse/SPARK-20964) | Improvement | Make some keywords reserved along with the ANSI/SQL standard |
| 3.0.0 | [SPARK-23128](https://issues.apache.org/jira/browse/SPARK-23128) | prose | Basic framework |
| 3.0.0 | [SPARK-23836](https://issues.apache.org/jira/browse/SPARK-23836) | Improvement | Support returning StructType to the level support in GroupedMap Arrow's "scalar" UDFS (or similar) |
| 3.0.0 | [SPARK-26163](https://issues.apache.org/jira/browse/SPARK-26163) | Improvement | Parsing decimals from JSON using locale |
| 3.0.0 | [SPARK-26215](https://issues.apache.org/jira/browse/SPARK-26215) | prose | SQL Parser defines ANSI compliant reserved keywords |
| 3.0.0 | [SPARK-26246](https://issues.apache.org/jira/browse/SPARK-26246) | Improvement | Infer timestamp types from JSON |
| 3.0.0 | [SPARK-26805](https://issues.apache.org/jira/browse/SPARK-26805) | Improvement | Eliminate double checking of stringToDate and stringToTimestamp inputs |
| 3.0.0 | [SPARK-26902](https://issues.apache.org/jira/browse/SPARK-26902) | Improvement | Support java.time.Instant as an external type of TimestampType |
| 3.0.0 | [SPARK-26903](https://issues.apache.org/jira/browse/SPARK-26903) | Improvement | Remove the TimeZone cache |
| 3.0.0 | [SPARK-26976](https://issues.apache.org/jira/browse/SPARK-26976) | prose | Forbid reserved keywords as identifiers when ANSI mode is on |
| 3.0.0 | [SPARK-27008](https://issues.apache.org/jira/browse/SPARK-27008) | Improvement | Support java.time.LocalDate as an external type of DateType |
| 3.0.0 | [SPARK-27031](https://issues.apache.org/jira/browse/SPARK-27031) | Improvement | Avoid double formatting in timestampToString |
| 3.0.0 | [SPARK-27109](https://issues.apache.org/jira/browse/SPARK-27109) | Improvement | Refactoring of TimestampFormatter and DateFormatter |
| 3.0.0 | [SPARK-27174](https://issues.apache.org/jira/browse/SPARK-27174) | Improvement | Support casting integer types to BinaryType |
| 3.0.0 | [SPARK-27199](https://issues.apache.org/jira/browse/SPARK-27199) | Improvement | Replace TimeZone by ZoneId in TimestampFormatter API |
| 3.0.0 | [SPARK-27212](https://issues.apache.org/jira/browse/SPARK-27212) | Improvement | Eliminate TimeZone to ZoneId conversion in stringToTimestamp |
| 3.0.0 | [SPARK-27414](https://issues.apache.org/jira/browse/SPARK-27414) | Improvement | make it clear that date type is timezone independent |
| 3.0.0 | [SPARK-27422](https://issues.apache.org/jira/browse/SPARK-27422) | Improvement | CurrentDate should return local date |
| 3.0.0 | [SPARK-27438](https://issues.apache.org/jira/browse/SPARK-27438) | Improvement | Increase precision of to_timestamp |
| 3.0.0 | [SPARK-27924](https://issues.apache.org/jira/browse/SPARK-27924) | prose | Support ANSI SQL Boolean-Predicate syntax |
| 3.0.0 | [SPARK-27986](https://issues.apache.org/jira/browse/SPARK-27986) | prose | Support ANSI SQL filter clause for aggregate expression |
| 3.0.0 | [SPARK-28077](https://issues.apache.org/jira/browse/SPARK-28077) | prose | Support ANSI SQL OVERLAY function |
| 3.0.0 | [SPARK-28083](https://issues.apache.org/jira/browse/SPARK-28083) | prose | Support ANSI SQL: LIKE … ESCAPE syntax |
| 3.0.0 | [SPARK-28177](https://issues.apache.org/jira/browse/SPARK-28177) | prose | Post shuffle partition number adjustment |
| 3.0.0 | [SPARK-28469](https://issues.apache.org/jira/browse/SPARK-28469) | Improvement | Change CalendarIntervalType's readable string representation from calendarinterval to interval |
| 3.0.0 | [SPARK-28495](https://issues.apache.org/jira/browse/SPARK-28495) | prose | Introduce ANSI store assignment policy for table insertion |
| 3.0.0 | [SPARK-28880](https://issues.apache.org/jira/browse/SPARK-28880) | prose | Support ANSI nested bracketed comments |
| 3.0.0 | [SPARK-28885](https://issues.apache.org/jira/browse/SPARK-28885) | prose | Follow ANSI store assignment rule in table insertion by default |
| 3.0.0 | [SPARK-28989](https://issues.apache.org/jira/browse/SPARK-28989) | New Feature | Introduce ANSI SQL Dialect |
| 3.0.0 | [SPARK-29607](https://issues.apache.org/jira/browse/SPARK-29607) | Improvement | Move static methods from CalendarInterval to IntervalUtils |
| 3.0.0 | [SPARK-29757](https://issues.apache.org/jira/browse/SPARK-29757) | Improvement | Move calendar interval constants together |
| 3.0.0 | [SPARK-29761](https://issues.apache.org/jira/browse/SPARK-29761) | Improvement | do not output leading 'interval' in CalendarInterval.toString |
| 3.0.0 | [SPARK-29783](https://issues.apache.org/jira/browse/SPARK-29783) | Improvement | Support SQL Standard output style for interval type |
| 3.0.0 | [SPARK-29787](https://issues.apache.org/jira/browse/SPARK-29787) | Improvement | Move method add/subtract/negate from CalendarInterval to IntervalUtils |
| 3.0.0 | [SPARK-29807](https://issues.apache.org/jira/browse/SPARK-29807) | Improvement | Rename "spark.sql.ansi.enabled" to "spark.sql.dialect.spark.ansi.enabled" |
| 3.0.0 | [SPARK-29860](https://issues.apache.org/jira/browse/SPARK-29860) | Improvement | [SQL] Fix data type mismatch issue for inSubQuery |
| 3.0.0 | [SPARK-29870](https://issues.apache.org/jira/browse/SPARK-29870) | Improvement | Unify the logic of multi-units interval string to CalendarInterval |
| 3.0.0 | [SPARK-29927](https://issues.apache.org/jira/browse/SPARK-29927) | Improvement | Parse timestamps in microsecond precision by `to_timestamp`, `to_unix_timestamp`, `unix_timestamp` |
| 3.0.0 | [SPARK-29941](https://issues.apache.org/jira/browse/SPARK-29941) | prose | Add ANSI type aliases for char and decimal |
| 3.0.0 | [SPARK-29943](https://issues.apache.org/jira/browse/SPARK-29943) | Improvement | Improve error messages for unsupported data type |
| 3.0.0 | [SPARK-30066](https://issues.apache.org/jira/browse/SPARK-30066) | Improvement | Columnar execution support for interval types |
| 3.0.0 | [SPARK-30252](https://issues.apache.org/jira/browse/SPARK-30252) | Improvement | Disallow negative scale of Decimal under ansi mode |
| 3.0.0 | [SPARK-30292](https://issues.apache.org/jira/browse/SPARK-30292) | Improvement | Throw Exception when invalid string is cast to decimal in ANSI mode |
| 3.0.0 | [SPARK-30439](https://issues.apache.org/jira/browse/SPARK-30439) | Improvement | support NOT NULL in column data type |
| 3.0.0 | [SPARK-30518](https://issues.apache.org/jira/browse/SPARK-30518) | Improvement | Precision and scale should be same for values between -1.0 and 1.0 in Decimal |
| 3.0.0 | [SPARK-30546](https://issues.apache.org/jira/browse/SPARK-30546) | New Feature | Make interval type more future-proof |
| 3.0.0 | [SPARK-30863](https://issues.apache.org/jira/browse/SPARK-30863) | Improvement | Distinguish Cast and AnsiCast in toString() |
| 3.0.0 | [SPARK-31227](https://issues.apache.org/jira/browse/SPARK-31227) | Improvement | Non-nullable null type should not coerce to nullable type |
| 3.0.0 | [SPARK-31277](https://issues.apache.org/jira/browse/SPARK-31277) | Improvement | Migrate `DateTimeTestUtils` from `TimeZone` to `ZoneId` |
| 3.0.0 | [SPARK-31392](https://issues.apache.org/jira/browse/SPARK-31392) | New Feature | Support CalendarInterval to be reflect to CalendarIntervalType |
| 3.0.0 | [SPARK-31469](https://issues.apache.org/jira/browse/SPARK-31469) | Improvement | Make extract interval field ANSI compliance |
| 3.0.0 | [SPARK-31527](https://issues.apache.org/jira/browse/SPARK-31527) | Improvement | date add/subtract interval only allow those day precision in ansi mode |
| 3.0.0 | [SPARK-31750](https://issues.apache.org/jira/browse/SPARK-31750) | Improvement | Eliminate UpCast if child's dataType is DecimalType |
| 3.0.0 | [SPARK-31834](https://issues.apache.org/jira/browse/SPARK-31834) | Improvement | Improve error message for incompatible data types |
| 3.1.1 | [SPARK-28880](https://issues.apache.org/jira/browse/SPARK-28880) | prose | Support ANSI nested bracketed comments |
| 3.1.1 | [SPARK-32272](https://issues.apache.org/jira/browse/SPARK-32272) | prose | Add SQL standard command SET TIME ZONE |
| 3.1.1 | [SPARK-33275](https://issues.apache.org/jira/browse/SPARK-33275) | prose | ANSI mode: runtime errors instead of returning null |
| 3.1.1 | [SPARK-33354](https://issues.apache.org/jira/browse/SPARK-33354) | prose | ANSI mode: new explicit cast syntax rules |
| 3.1.1 | [SPARK-33480](https://issues.apache.org/jira/browse/SPARK-33480) | prose | Support char/varchar data type |
| 3.1.1 | [SPARK-34083](https://issues.apache.org/jira/browse/SPARK-34083) | Improvement | Using TPCDS original definitions for char/varchar columns |
| 3.1.1 | [SPARK-34130](https://issues.apache.org/jira/browse/SPARK-34130) | Improvement | Impove preformace for char varchar padding and length check with StaticInvoke |
| 3.2.0 | [SPARK-7768](https://issues.apache.org/jira/browse/SPARK-7768) | New Feature | Make user-defined type (UDT) API public |
| 3.2.0 | [SPARK-27790](https://issues.apache.org/jira/browse/SPARK-27790) | prose | Support for ANSI SQL INTERVAL types |
| 3.2.0 | [SPARK-34164](https://issues.apache.org/jira/browse/SPARK-34164) | Improvement | Improve write side varchar check to visit only last few tailing spaces |
| 3.2.0 | [SPARK-34199](https://issues.apache.org/jira/browse/SPARK-34199) | prose | Block count(table.*) to follow ANSI standard and other SQL engines |
| 3.2.0 | [SPARK-34246](https://issues.apache.org/jira/browse/SPARK-34246) | New Feature | New type coercion syntax rules in ANSI mode |
| 3.2.0 | [SPARK-34382](https://issues.apache.org/jira/browse/SPARK-34382) | prose | Support LATERAL subqueries |
| 3.2.0 | [SPARK-34665](https://issues.apache.org/jira/browse/SPARK-34665) | Improvement | Revise the type coercion section of ANSI Compliance |
| 3.2.0 | [SPARK-34908](https://issues.apache.org/jira/browse/SPARK-34908) | Improvement | Add test cases for char and varchar with functions |
| 3.2.0 | [SPARK-34944](https://issues.apache.org/jira/browse/SPARK-34944) | Improvement | Employ correct data type for web_returns and store_returns in TPCDS tests |
| 3.2.0 | [SPARK-35028](https://issues.apache.org/jira/browse/SPARK-35028) | New Feature | ANSI mode: disallow group by aliases |
| 3.2.0 | [SPARK-35030](https://issues.apache.org/jira/browse/SPARK-35030) | prose | ANSI SQL mode GA |
| 3.2.0 | [SPARK-35103](https://issues.apache.org/jira/browse/SPARK-35103) | Improvement | Improve the performance of type coercion rules |
| 3.2.0 | [SPARK-35152](https://issues.apache.org/jira/browse/SPARK-35152) | prose | ANSI mode: IntegralDivide throws an exception on overflow |
| 3.2.0 | [SPARK-35446](https://issues.apache.org/jira/browse/SPARK-35446) | Improvement | Override getJDBCType in MySQLDialect to map FloatType to FLOAT |
| 3.2.0 | [SPARK-35706](https://issues.apache.org/jira/browse/SPARK-35706) | Improvement | Consider making the ':' in STRUCT data type definition optional |
| 3.2.0 | [SPARK-35955](https://issues.apache.org/jira/browse/SPARK-35955) | prose | ANSI mode: Check for overflow in Average |
| 3.3.0 | [SPARK-27790](https://issues.apache.org/jira/browse/SPARK-27790) | prose | Support ANSI SQL INTERVAL types |
| 3.3.0 | [SPARK-33354](https://issues.apache.org/jira/browse/SPARK-33354) | prose | New explicit cast syntax rules in ANSI mode |
| 3.3.0 | [SPARK-36841](https://issues.apache.org/jira/browse/SPARK-36841) | prose | Add ansi syntax set catalog xxx to change the current catalog |
| 3.3.0 | [SPARK-36931](https://issues.apache.org/jira/browse/SPARK-36931) | prose | Support reading and writing ANSI intervals from/to ORC data sources |
| 3.3.0 | [SPARK-37133](https://issues.apache.org/jira/browse/SPARK-37133) | prose | Add a config to optionally enforce ANSI reserved keywords |
| 3.3.0 | [SPARK-37641](https://issues.apache.org/jira/browse/SPARK-37641) | prose | Support ANSI Aggregate Function: regr_r2 |
| 3.3.0 | [SPARK-37671](https://issues.apache.org/jira/browse/SPARK-37671) | prose | ANSI Aggregation Function |
| 3.3.0 | [SPARK-37714](https://issues.apache.org/jira/browse/SPARK-37714) | prose | Allow casting between numeric type and timestamp type |
| 3.3.0 | [SPARK-37724](https://issues.apache.org/jira/browse/SPARK-37724) | prose | Disable ANSI reserved keywords by default |
| 3.3.0 | [SPARK-37750](https://issues.apache.org/jira/browse/SPARK-37750) | prose | Optionally return null result if element not exists in array/map |
| 3.3.0 | [SPARK-38589](https://issues.apache.org/jira/browse/SPARK-38589) | prose | New SQL function: try_avg |
| 3.3.0 | [SPARK-38860](https://issues.apache.org/jira/browse/SPARK-38860) | prose | ANSI enhancements |
| 3.4.0 | [SPARK-37623](https://issues.apache.org/jira/browse/SPARK-37623) | prose | Support ANSI Aggregate Function: REGR_INTERCEPT |
| 3.4.0 | [SPARK-37641](https://issues.apache.org/jira/browse/SPARK-37641) | prose | Support ANSI Aggregate Function: REGR_R2 |
| 3.4.0 | [SPARK-37672](https://issues.apache.org/jira/browse/SPARK-37672) | prose | Support ANSI Aggregate Function: REGR_SXX |
| 3.4.0 | [SPARK-37681](https://issues.apache.org/jira/browse/SPARK-37681) | prose | Support ANSI Aggregate Function: REGR_SXY |
| 3.4.0 | [SPARK-37691](https://issues.apache.org/jira/browse/SPARK-37691) | prose | Support ANSI Aggregation Function: PERCENTILE_DISC |
| 3.4.0 | [SPARK-37702](https://issues.apache.org/jira/browse/SPARK-37702) | prose | Support ANSI Aggregate Function: REGR_SYY |
| 3.4.0 | [SPARK-38219](https://issues.apache.org/jira/browse/SPARK-38219) | prose | Support ANSI aggregation function PERCENTILE_CONT as window function |
| 3.4.0 | [SPARK-39138](https://issues.apache.org/jira/browse/SPARK-39138) | prose | Support ANSI general value specification and function - USER |
| 3.4.0 | [SPARK-39230](https://issues.apache.org/jira/browse/SPARK-39230) | prose | Support ANSI Aggregate Function: REGR_SLOPE |
| 3.4.0 | [SPARK-39451](https://issues.apache.org/jira/browse/SPARK-39451) | prose | Support casting intervals to integrals in ANSI mode |
| 3.4.0 | [SPARK-39470](https://issues.apache.org/jira/browse/SPARK-39470) | prose | Support cast of ANSI intervals to decimals |
| 3.4.0 | [SPARK-40008](https://issues.apache.org/jira/browse/SPARK-40008) | prose | Support casting of integrals to ANSI intervals |
| 3.4.0 | [SPARK-40014](https://issues.apache.org/jira/browse/SPARK-40014) | prose | Support cast of decimals to ANSI intervals |
| 3.4.0 | [SPARK-40066](https://issues.apache.org/jira/browse/SPARK-40066) | prose | ANSI SQL mode: always return null on invalid access to map column |
| 3.4.0 | [SPARK-40151](https://issues.apache.org/jira/browse/SPARK-40151) | prose | Return wider ANSI interval types from the percentile functions |
| 3.4.0 | [SPARK-40585](https://issues.apache.org/jira/browse/SPARK-40585) | prose | Support double quoted identifiers |
| 3.4.1 | [SPARK-43425](https://issues.apache.org/jira/browse/SPARK-43425) | prose | Add TimestampNTZType to ColumnarBatchRow |
| 3.5.5 | [SPARK-50624](https://issues.apache.org/jira/browse/SPARK-50624) | prose | Add TimestampNTZType to ColumnarRow/MutableColumnarRow |
<!-- AUTO:timeline END -->
