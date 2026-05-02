# Glossary

A running glossary of terms across all books. Each entry is attributed to its source.

## From Rioux (2022)

| Term | Meaning | Source |
| --- | --- | --- |
| **Action** | An operation that triggers actual data computation — e.g., `show()`, `write()`, `count()` on a data frame. Contrast with *transformation*. | Rioux Ch 1 |
| **Cluster manager** | The component that allocates computing resources (machines and executors) before a Spark job runs. Can be Spark Standalone, YARN, Mesos, or Kubernetes. | Rioux Ch 1 |
| **Driver program** | The process that hosts the user's code, translates it into Spark instructions, and coordinates executors. Sometimes called the master in older docs. | Rioux Ch 1 |
| **Eager evaluation** | Execution model where each instruction is performed immediately as it is received. Default in plain Python, pandas, R. Contrast with *lazy evaluation*. | Rioux Ch 1 |
| **Executor** | A JVM process that performs the actual data work on a worker node. Multiple executors can run on a single worker node. | Rioux Ch 1 |
| **Lazy evaluation** | Spark's execution model: transformations are recorded but not computed until an action is reached. Enables query optimization, memory efficiency, and fault recovery. | Rioux Ch 1 |
| **Master** | Legacy term for the entity that allocates cluster resources; being phased out in Spark docs. See *driver program* and *cluster manager*. | Rioux Ch 1 |
| **PySpark** | The Python API for Apache Spark. Exposes Spark's data processing capabilities through Python idioms. | Rioux Ch 1 |
| **pyspark.pandas** | A pandas-compatible API layered on top of PySpark (formerly the Koalas project; integrated into Spark as of 3.2.0). Allows pandas-style code to run at Spark scale. | Rioux Ch 1 |
| **RDD** | Resilient Distributed Dataset — Spark's low-level data structure. Predates the DataFrame API; still available but rarely the best choice for new code. | Rioux Ch 1 |
| **Scale out** | Adding more machines to a cluster rather than upgrading a single machine. Spark's primary scaling strategy. | Rioux Ch 1 |
| **Scale up** | Adding more CPU/RAM/disk to a single machine. Less cost-effective than scale-out past a certain threshold. | Rioux Ch 1 |
| **SparkContext** | The object encoding connection details and capacity configuration for a Spark cluster. | Rioux Ch 1 |
| **SparkSession** | The unified entry point to PySpark (introduced in Spark 2.0). Subsumes `SparkContext`, `SQLContext`, and `HiveContext`. | Rioux Ch 1 |
| **Transformation** | Any Spark operation that produces a new data frame without triggering computation — e.g., `select()`, `filter()`, `groupBy()`, `withColumn()`. Evaluated lazily. | Rioux Ch 1 |
| **Worker node** | A physical or virtual machine in the Spark cluster where executors run. | Rioux Ch 1 |
| **alias()** | Column method that renames the result of a column expression inside a `select()` call. Contrast with `withColumnRenamed()`. | Rioux Ch 2 |
| **DataFrameReader** | Object accessed via `spark.read`; provides format-specific methods (`text()`, `csv()`, `json()`, `parquet()`, `orc()`) to ingest data into a DataFrame. | Rioux Ch 2 |
| **ETL** | Extract, Transform, Load — the three-step pattern underpinning most data pipelines. PySpark maps naturally to this: read → transform → write. | Rioux Ch 2 |
| **explode()** | `pyspark.sql.functions` function that unpacks a column of arrays (or maps) into one row per element. Converts a `array<string>` column into a `string` column with more rows. | Rioux Ch 2 |
| **filter() / where()** | Identical DataFrame methods that keep only rows satisfying a Boolean column expression. `filter()` is the primary name; `where()` is an alias. | Rioux Ch 2 |
| **lower()** | `pyspark.sql.functions` function that lowercases all characters in a string column. | Rioux Ch 2 |
| **printSchema()** | DataFrame method that prints the column tree (names, types, nullability) to the console. One of the two primary exploration tools alongside `show()`. | Rioux Ch 2 |
| **regexp_extract()** | `pyspark.sql.functions` function that extracts a regex match group from a string column. Uses Java regex syntax. | Rioux Ch 2 |
| **REPL** | Read, Evaluate, Print, Loop — an interactive programming shell. The `pyspark` command launches a Python REPL pre-configured with `spark` and `sc`. | Rioux Ch 2 |
| **schema** | The description of a DataFrame's column names and their data types. Accessed via `printSchema()` (tree view), `.dtypes` (list of tuples), or `.schema` (programmatic `StructType` object). | Rioux Ch 2 |
| **select()** | The primary DataFrame transformation for choosing or computing columns. Equivalent to SQL `SELECT`. Accepts column objects, `col()` references, string names, or column expressions. | Rioux Ch 2 |
| **show()** | DataFrame **action** that triggers computation and prints up to N rows as an ASCII table. Parameters: `n`, `truncate`, `vertical`. | Rioux Ch 2 |
| **split()** | `pyspark.sql.functions` function that splits a string column by a Java regex delimiter, producing an `array<string>` column. | Rioux Ch 2 |
| **withColumnRenamed()** | DataFrame method that renames an existing column without otherwise changing the DataFrame. No-op if the named column doesn't exist. | Rioux Ch 2 |
| **coalesce(n)** | Transformation that reduces the number of partitions to `n` without a full shuffle. Use before writing to produce fewer output files. Contrast with `repartition(n)`. | Rioux Ch 3 |
| **DataFrameWriter** | Object accessed via `df.write`; mirrors DataFrameReader. Provides format-specific write methods (`csv()`, `parquet()`, `json()`) and a `mode()` setter (`overwrite`, `append`, `ignore`, `error`). | Rioux Ch 3 |
| **glob pattern** | A wildcard path like `*.txt` or a directory path that `spark.read` expands to match multiple files, loading them into a single unified DataFrame. | Rioux Ch 3 |
| **GroupedData** | Intermediate object returned by `groupBy()`. Holds grouped records and awaits an aggregation method (`count()`, `sum()`, `agg()`, …) to produce a new DataFrame. | Rioux Ch 3 |
| **groupBy()** | DataFrame transformation that groups rows by the values of one or more columns, returning a `GroupedData` object. Lowercase alias `groupby()` also accepted. | Rioux Ch 3 |
| **method chaining** | Calling the next transformation directly on the return value of the previous one, eliminating intermediate variables. Enabled by each transformation returning a new DataFrame. | Rioux Ch 3 |
| **orderBy()** | DataFrame transformation that sorts rows by one or more columns. Use `.desc()` on a `col()` or `ascending=False` for descending order. Alias: `sort()`. | Rioux Ch 3 |
| **partition** | A logical chunk of a distributed DataFrame, stored and processed by a single executor. PySpark writes one output file per partition by default. | Rioux Ch 3 |
| **repartition(n)** | Transformation that reshuffles data into exactly `n` partitions via a full shuffle. Use to increase partition count or rebalance skewed data. Contrast with `coalesce(n)`. | Rioux Ch 3 |
| **spark-submit** | CLI tool (`spark-submit script.py`) for submitting PySpark (and Scala/Java) programs to a Spark cluster in batch mode. The script must create its own `SparkSession`. | Rioux Ch 3 |
| **stop words** | Common words (the, a, of, to, …) that carry little semantic meaning and are typically removed before NLP analysis. | Rioux Ch 3 |
| **cast()** | Column method that converts a column to a specified type (e.g., `"int"`, `"double"`, `"timestamp"`). In Spark 4.x (ANSI mode on), invalid casts raise an error; use `try_cast()` for nullable semantics. | Rioux Ch 4 |
| **createDataFrame()** | `SparkSession` method that creates a DataFrame from a list-of-lists, a pandas DataFrame, or an RDD. Accepts a schema as a list of column names or a `StructType`. | Rioux Ch 4 |
| **denormalisation** | Merging multiple normalised tables into one wide ("fat") table to avoid join overhead. Preferred for analytical workloads in Spark. | Rioux Ch 4 |
| **describe()** | DataFrame method returning count, mean, stddev, min, max for numeric/string columns. For exploration only — output format not guaranteed stable across Spark versions. | Rioux Ch 4 |
| **distinct()** | DataFrame transformation that removes duplicate rows. Returns a new DataFrame with one copy of each unique row. | Rioux Ch 4 |
| **drop()** | DataFrame method that removes one or more named columns. Opposite of `select()`; dropping a non-existent column is a no-op. | Rioux Ch 4 |
| **EDA** | Exploratory Data Analysis — initial investigation of a new dataset to understand its structure, types, distributions, and quality before modelling. | Rioux Ch 4 |
| **inferSchema** | `spark.read.csv()` option (`True`/`False`) that auto-detects column types by pre-scanning the data. Convenient but expensive (reads data twice). | Rioux Ch 4 |
| **normalisation** | Data-modelling technique that eliminates redundancy by splitting data into multiple related tables linked by keys. Common in relational/SQL databases. | Rioux Ch 4 |
| **star schema** | Database schema with a central fact table (holding IDs and metrics) surrounded by dimension/link tables (holding the meaning of each ID). Common in data warehouses. | Rioux Ch 4 |
| **substr()** | Column method that extracts a substring: `col.substr(start, length)`. Position is **1-indexed** (not 0-indexed like Python strings). | Rioux Ch 4 |
| **summary()** | DataFrame method like `describe()` but with customisable statistics (e.g., `summary("min", "10%", "90%", "max")`). Counts only non-null values. | Rioux Ch 4 |
| **toDF()** | DataFrame method that renames **all** columns at once by accepting a new list of names (`df.toDF(*new_names)`). | Rioux Ch 4 |
| **toPandas()** | DataFrame action that collects the entire distributed DataFrame onto the driver as a pandas DataFrame. Only safe after aggregating to a small result. | Rioux Ch 4 |
| **try_cast()** | `pyspark.sql.functions` function (Spark 4.x) that attempts a type conversion and returns `null` on failure instead of raising an error. Replaces the Spark 3.x silent-null behaviour of `cast()` under ANSI mode. | Rioux Ch 4 |
| **withColumn()** | DataFrame method that adds a new column (or overwrites an existing one) while keeping all other columns intact. Signature: `withColumn(name, col_expr)`. | Rioux Ch 4 |
| **agg()** | `GroupedData` method that applies one or more aggregate functions (e.g., `F.sum()`, `F.avg()`) to each group and returns a new DataFrame. Preferred over calling aggregation methods directly on `GroupedData` because it accepts multiple functions and allows column aliasing. | Rioux Ch 5 |
| **crossJoin()** | DataFrame method (and `how="cross"` in `join()`) that returns every possible combination of left × right rows regardless of any predicate. Produces m × n output rows. | Rioux Ch 5 |
| **dropna()** | DataFrame method that removes rows containing null values. Parameters: `how` (`"any"` or `"all"`), `thresh` (min non-null count), `subset` (columns to inspect). Alias: `df.na.drop()`. | Rioux Ch 5 |
| **equi-join** | A join whose predicate tests equality between identically named columns. PySpark shorthand: pass the column name as a string to `on=`. Automatically deduplicates the join-key column in the result. | Rioux Ch 5 |
| **F.trim()** | `pyspark.sql.functions` function that removes leading and trailing whitespace characters from a string column. | Rioux Ch 5 |
| **F.when()** | `pyspark.sql.functions` function that creates a conditional column expression: `F.when(condition, value).when(...).otherwise(default)`. Omitting `.otherwise()` returns `null` for unmatched rows. Can be nested inside aggregate functions. | Rioux Ch 5 |
| **fillna()** | DataFrame method that replaces null values with a specified scalar value or a column-to-value dict. The fill value must be type-compatible with the column. Alias: `df.na.fill()`. | Rioux Ch 5 |
| **full outer join** | Join method (`how="outer"`, `"full"`, or `"full_outer"`) that returns all rows from both tables, padding unmatched sides with `null`. | Rioux Ch 5 |
| **GroupedData** | Transitional object returned by `groupby()`. Holds grouped records awaiting an aggregation method (`agg()`, `count()`, `sum()`, etc.) before becoming a DataFrame again. Has no `.show()` or `.select()`. | Rioux Ch 5 |
| **inner join** | Default join method (`how="inner"`). Returns only rows where the predicate is `True`. Duplicates a left row if it matches multiple right rows. | Rioux Ch 5 |
| **isin()** | Column method that returns `True` if the column value is a member of the supplied list. Used as `F.col("x").isin(["a", "b", "c"])`. | Rioux Ch 5 |
| **join()** | DataFrame method for combining two DataFrames. Three parameters: `on` (predicate or column name(s)), `how` (join method). Blueprint: `left.join(right, on=..., how=...)`. | Rioux Ch 5 |
| **left anti-join** | Join method (`how="left_anti"`) that returns only rows from the left table that have **no** matching row in the right table. The inverse of an inner join. | Rioux Ch 5 |
| **left join** | Join method (`how="left"` or `"left_outer"`) that returns all left rows; unmatched rows are padded with `null` for right-table columns. | Rioux Ch 5 |
| **left semi-join** | Join method (`how="left_semi"`) that returns left rows that match at least one right row. Only left-table columns are returned and matched left rows are not duplicated. | Rioux Ch 5 |
| **predicate** | In the context of a join, a Boolean column expression that determines whether a pair of left and right rows is considered a match. | Rioux Ch 5 |
| **right join** | Join method (`how="right"` or `"right_outer"`) that returns all right rows; unmatched rows are padded with `null` for left-table columns. | Rioux Ch 5 |
