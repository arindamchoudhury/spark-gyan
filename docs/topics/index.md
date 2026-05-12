# Topics

Cross-book synthesis — each page distills how multiple books treat the same topic.

## Active topic pages

_(None yet — topic pages are written once ≥2 books cover the same ground.)_

## Topics to write (≥2 source chapters available)

_(None yet — promote from backlog when a second book overlaps.)_

## Single-source backlog (waiting for a second book)

- **Spark architecture** — Rioux Ch 1 only. Driver, executor, cluster manager, worker node, master. *(Add a second source to write this page.)*
- **Lazy evaluation** — Rioux Ch 1 only. Transformations vs. actions; eager vs. lazy evaluation; why laziness enables Spark's speed and fault tolerance. *(Add a second source to write this page.)*
- **Scale-out vs. scale-up** — Rioux Ch 1 only. Horizontal scaling economics; distributed compute vs. single-machine RAM. *(Add a second source to write this page.)*
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
- **Schema definition: StructType vs DDL** — Rioux Ch 6 only. Bottom-up programmatic construction; DDL string alternative; partial schemas; `StructType.toDDL()` / `DataType.fromDDL()` (Spark 4.0+); FAILFAST vs PERMISSIVE mode. *(Add a second source to write this page.)*
- **Hierarchical vs tabular data models** — Rioux Ch 6 only. Normalised (join-based) vs denormalised (duplicated rows) vs hierarchical (array of structs); when each is appropriate for big data. *(Add a second source to write this page.)*
- **Explode and collect patterns** — Rioux Ch 6 only. explode/explode_outer; posexplode; collect_list/collect_set; null-row retention; ordering guarantees. *(Add a second source to write this page.)*
- **PySpark vs SQL API comparison** — Rioux Ch 7 only. Vocabulary mapping; order-of-operations inversion; HAVING vs chained where; UNION ALL vs union(); CTE vs Python function scope. *(Add a second source to write this page.)*
- **Spark SQL interop** — Rioux Ch 7 only. createOrReplaceTempView; spark.sql(); catalog API; selectExpr/expr/where SQL strings; SQL injection risk with f-string predicates. *(Add a second source to write this page.)*
- **Multi-source union with reduce** — Rioux Ch 7 only. functools.reduce for chaining union(); column intersection for schema alignment; assert as early schema contract. *(Add a second source to write this page.)*
