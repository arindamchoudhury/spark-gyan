# Topics

Cross-book synthesis — each page distills how multiple books treat the same topic.

## Active topic pages

*(Written from Rioux Ch 1–8 cross-chapter synthesis. Expand as further books are added.)*

| Topic | Source chapters | Summary |
|---|---|---|
| [Lazy evaluation & execution model](lazy-evaluation.md) | Rioux Ch 1, 2, 3, 7, 8 | Transformations vs actions; Catalyst/Tungsten/WSCG internals; cache() |
| [Catalyst: column pruning & predicate pushdown](catalyst-optimizations.md) | Rioux Ch 2, 4 | How optimisations work; what disables them; JDBC rewriting; Delta Z-ordering |
| [ANSI mode](ansi-mode.md) | Rioux Ch 4, 5, 7 | cast() raises in Spark 4.x; try_cast/try_sum/try_avg; migration strategy |
| [pyspark.errors exception hierarchy](pyspark-errors.md) | Rioux Ch 4, 5, 6, 8 | Import path change; full exception class table; RDD Py4JJavaError caveat |
| [Column references: F.col() vs strings](column-references.md) | Rioux Ch 2, 4, 5, 6 | Four forms; precise use rule; join disambiguation; struct/array/map access |
| [select() best practices](select-best-practices.md) | Rioux Ch 2, 4, 5 | Nine rules; withColumns(); UDF pruning impact; post-join select |
| [Explode and array expansion patterns](explode-patterns.md) | Rioux Ch 2, 6 | explode variants; inline(); collect_list/collect_set; higher-order functions |
| [Schema definition: StructType vs DDL](schema-definition.md) | Rioux Ch 4, 6 | StructType API; DDL strings; partial schemas; toDDL/fromDDL; FAILFAST |
| [coalesce() and repartition()](coalesce-repartition.md) | Rioux Ch 2, 3 | Comparison table; coalesce(1) trap; partition tuning; Delta compaction |
| [Logging: log4j2.xml and setLogLevel()](logging.md) | Rioux Ch 2, 8 | Three-decision log4j2.xml; os.path.abspath() requirement; RDD task noise |
| [groupBy().agg() pattern](groupby-agg.md) | Rioux Ch 3, 5, 7 | Full agg(*exprs); F.when() conditional agg; try_sum; HAVING = chained where() |
| [RDD vs DataFrame](rdd-vs-dataframe.md) | Rioux Ch 1, 2, 8 | Contrast table; UnsafeRow memory model; JVM bridge cost; 3–8× language gap |
| [Python↔JVM boundary and UDF cost](python-jvm-boundary.md) | Rioux Ch 1, 8 | Where crossing happens; UDF eval types; cloudpickle; WSCG barriers |
| [union() and multi-source schema alignment](union-schema-alignment.md) | Rioux Ch 3, 7 | Positional matching; column intersection; functools.reduce; assert contract |

---

## Topics to write (≥2 source chapters available)

*(None remaining from Ch 1–8 — all identified cross-chapter topics have been written.)*

---

## Single-source backlog (waiting for a second book)

- **Spark architecture** — Rioux Ch 1 only. Driver, executor, cluster manager, worker node, master. *(Add a second source to write this page.)*
- **Scale-out vs scale-up** — Rioux Ch 1 only. Horizontal scaling economics; distributed compute vs. single-machine RAM. *(Add a second source to write this page.)*
- **PySpark + pandas interop** — Rioux Ch 1 only. pyspark.pandas (formerly Koalas); when to use which. *(Add a second source to write this page.)*
- **Spark ecosystem and language APIs** — Rioux Ch 1 only. Python, Scala, Java, SQL; SparkR (deprecated 4.x). *(Add a second source to write this page.)*
- **DataFrame vs RDD** — Rioux Ch 2 only. Mental models, when to use each; RDD is the lower-level structure the DataFrame builds on. *(Add a second source to write this page.)*
- **Column transformations pattern** — Rioux Ch 2 only. select() + col() + alias(); the standard rhythm for applying pyspark.sql.functions. *(Add a second source to write this page.)*
- **SparkSession setup** — Rioux Ch 2 only. Builder pattern, getOrCreate(), appName, log level, eager eval config. *(Add a second source to write this page.)*
- **Partitioning and output files** — Rioux Ch 3 only. One file per partition; coalesce vs repartition; writing modes. *(Add a second source to write this page.)*
- **Method chaining and code style** — Rioux Ch 3 only. F import convention; chaining transformations; when to keep intermediate variables. *(Add a second source to write this page.)*
- **Batch mode and spark-submit** — Rioux Ch 3 only. spark-submit CLI; batch vs interactive; scaling with glob patterns. *(Add a second source to write this page.)*
- **Tabular data and CSV ingestion** — Rioux Ch 4 only. SparkReader CSV options; inferSchema; star schemas; denormalisation. *(Add a second source to write this page.)*
- **Column manipulation toolkit** — Rioux Ch 4 only. select, drop, withColumn, withColumnRenamed, toDF, cast, substr, distinct. *(Add a second source to write this page.)*
- **EDA methods** — Rioux Ch 4 only. describe() and summary(); toPandas() for charting; when not to use them. *(Add a second source to write this page.)*
- **Join types and mechanics** — Rioux Ch 5 only. Three-ingredient blueprint (left, predicate, method); seven join methods; equi-join shorthand; column-name clash solutions (shorthand / origin-name / alias). *(Add a second source to write this page.)*
- **Conditional aggregation with F.when()** — Rioux Ch 5 only. F.when().otherwise() blueprint; nesting inside F.sum() inside agg(); F.trim() and isin() as companions. *(Add a second source to write this page.)*
- **Null handling: dropna and fillna** — Rioux Ch 5 only. dropna(how, thresh, subset); fillna(value, subset); df.na accessor aliases. *(Add a second source to write this page.)*
- **JSON ingestion patterns** — Rioux Ch 6 only. JSON Lines vs multiLine; specialized JSON SparkReader; `from_json()` vs `parse_json()` (Spark 4.x); VARIANT type. *(Add a second source to write this page.)*
- **Complex column types (array, map, struct)** — Rioux Ch 6 only. Semantics and constraints of each; Python analogues; `array_` function family; dot-notation navigation; printSchema() reading. *(Add a second source to write this page.)*
- **Hierarchical vs tabular data models** — Rioux Ch 6 only. Normalised (join-based) vs denormalised (duplicated rows) vs hierarchical (array of structs); when each is appropriate for big data. *(Add a second source to write this page.)*
- **PySpark vs SQL API comparison** — Rioux Ch 7 only. Vocabulary mapping; order-of-operations inversion; HAVING vs chained where; UNION ALL vs union(); CTE vs Python function scope. *(Add a second source to write this page.)*
- **Spark SQL interop** — Rioux Ch 7 only. createOrReplaceTempView; spark.sql(); catalog API; selectExpr/expr/where SQL strings; SQL injection risk with f-string predicates. *(Add a second source to write this page.)*
- **RDD fundamentals** — Rioux Ch 8 only. Distributed bag of objects; SparkContext.parallelize(); map/filter/reduce higher-order functions; MapReduce lineage; RDD vs DataFrame tradeoffs. *(Add a second source to write this page.)*
- **Python UDFs** — Rioux Ch 8 only. F.udf() and @F.udf() decorator; Python-to-PySpark type mapping; type annotations for safety; .func for local testing; performance cost vs built-ins. *(Add a second source to write this page.)*
