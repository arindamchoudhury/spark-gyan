# Chapter 05 — Schema: StructType, DDL, and Type Safety

> *Learning-path topic: B5 (Beginner)*
> *Written: 2026-05-31 · Spark 4.1.x / Python 3.10+*

Schema is the contract between your data and your code. An explicit schema catches corrupt data at ingestion time, prevents silent type coercions, and makes pipelines self-documenting. A missing schema turns bugs into mysteries.

---

## What you'll learn

- How to define a schema with `StructType` and `StructField`
- The DDL string shorthand for concise schema declarations
- How `inferSchema` works and why it fails silently
- How Spark 4.x ANSI mode changes type casting behaviour
- How to define nested schemas for JSON and complex data

---

## The problem this solves

A nightly pipeline reads a CSV file. One morning the data team adds a column without telling you. Because you used `inferSchema=True`, the column count shifts, types re-infer differently for some columns, and your aggregations produce wrong results — silently. With an explicit schema, the pipeline fails loudly on the new column instead of producing bad data quietly.

---

## Core concept

Every PySpark DataFrame has a **schema** — a `StructType` containing a list of `StructField` objects, each describing one column's name, type, and nullability. The schema is the source of truth for what the data looks like.

`StructType` and `StructField` live in `pyspark.sql.types` (import as `T`):

```python
import pyspark.sql.types as T
```

A `StructField` takes three arguments: `name`, `dataType`, `nullable` (default `True`). Setting `nullable=False` is a declaration of intent — Spark won't enforce it at read time but downstream operations can rely on it and avoid null-checking overhead.

The **DDL string** is a concise alternative to the `StructType` API: `"name STRING NOT NULL, temp DOUBLE, date TIMESTAMP"`. It is parsed at session creation time and compiles to the same `StructType` object. Use DDL strings for simple schemas; use the `StructType` API when building schemas programmatically or for nested types.

**ANSI mode (Spark 4.x default)** changes what happens when a cast fails. In Spark 3.x, `cast("abc" AS INT)` silently returned `null`. In Spark 4.x with ANSI mode on, it raises `SparkNumberFormatException`. This is the right default — silent null returns hide data quality problems — but it requires using `try_cast()` when you genuinely want nullable semantics for bad input.

---

## Examples

### Minimal example: StructType schema

```python
# Apache Spark 4.1.x / PySpark 4.1.x · Python 3.10+
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ch05").master("local[*]").getOrCreate()

schema = T.StructType([
    T.StructField("station",  T.StringType(),    nullable=False),
    T.StructField("year",     T.IntegerType(),   nullable=False),
    T.StructField("temp_f",   T.DoubleType(),    nullable=True),
    T.StructField("precip",   T.DoubleType(),    nullable=True),
])

df = spark.read.csv("data/weather.csv", schema=schema, header=True)
df.printSchema()
# root
#  |-- station: string (nullable = false)
#  |-- year: integer (nullable = false)
#  |-- temp_f: double (nullable = true)
#  |-- precip: double (nullable = true)
```

### Building up: DDL strings and casting with ANSI mode

```python
# Apache Spark 4.1.x / PySpark 4.1.x · Python 3.10+
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ch05-ddl").master("local[*]").getOrCreate()

# DDL string — more concise than StructType for simple schemas
schema_ddl = "station STRING NOT NULL, year INT NOT NULL, temp_f DOUBLE, precip DOUBLE"
df = spark.read.csv("data/weather.csv", schema=schema_ddl, header=True)

# Safe casting with try_cast (ANSI mode, Spark 4.x)
# cast() raises SparkNumberFormatException on bad input
# try_cast() returns null instead — use when input may be dirty
data = [("42.5",), ("bad_value",), ("38.1",)]
raw = spark.createDataFrame(data, ["raw_temp"])

safe = raw.withColumn(
    "temp_d",
    F.col("raw_temp").try_cast(T.DoubleType())  # null for "bad_value"
)
safe.show()
# +---------+------+
# | raw_temp|temp_d|
# +---------+------+
# |     42.5|  42.5|
# |bad_value|  null|
# |     38.1|  38.1|
# +---------+------+
```

### Nested schema for JSON

```python
# Apache Spark 4.1.x / PySpark 4.1.x · Python 3.10+
import pyspark.sql.types as T
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ch05-nested").master("local[*]").getOrCreate()

# Nested schema: a record with an embedded struct and an array
schema = T.StructType([
    T.StructField("id",    T.StringType(), nullable=False),
    T.StructField("loc",   T.StructType([
        T.StructField("lat",  T.DoubleType(), nullable=True),
        T.StructField("lon",  T.DoubleType(), nullable=True),
    ]), nullable=True),
    T.StructField("tags",  T.ArrayType(T.StringType()), nullable=True),
])

df = spark.read.json("data/events.jsonl", schema=schema)
df.printSchema()
# root
#  |-- id: string (nullable = false)
#  |-- loc: struct (nullable = true)
#  |    |-- lat: double (nullable = true)
#  |    |-- lon: double (nullable = true)
#  |-- tags: array (nullable = true)
#  |    |-- element: string (containsNull = true)
```

---

## Common pitfalls

- **`inferSchema=True` re-reads the file and may infer wrong types** — a column with all nulls in the first N rows infers as `StringType`. A column that looks like integers but has one decimal infers as `DoubleType`. Declare schemas explicitly in production.
- **ANSI mode surprises in Spark 4.x** — `F.col("x").cast(T.IntegerType())` on a string column raises an error in Spark 4.x where it silently returned null in 3.x. Use `try_cast()` for nullable semantics. Note: `try_cast` is a method on `Column`, not a standalone `F.` function.
- **`nullable=False` is not enforced at read time** — it is a hint to the optimizer. Null values can still appear. Use `filter(F.col("col").isNotNull())` if you genuinely need to remove nulls.
- **DDL string case sensitivity** — type names in DDL strings are case-insensitive (`STRING`, `string`, `String` all work). Column names in DDL strings are case-sensitive by default — use backticks for names with spaces: `` `column name` STRING ``.
- **Using `describe()` for schema validation** — `describe()` is for exploratory statistics, not schema validation. Use `printSchema()` for types and `df.schema` for the programmatic `StructType` object.

---

## Exercises

1. **Recall** — In Spark 4.x with ANSI mode on, what is the difference between `F.col("x").cast(T.IntegerType())` and `F.col("x").try_cast(T.IntegerType())` when `x = "abc"`?

2. **Apply** — Create a schema with one `StringType`, one `IntegerType` (nullable=False), and one `ArrayType(StringType())`. Create a DataFrame from a Python list using this schema. Verify the schema with `printSchema()`. Then try inserting `None` into the `nullable=False` column — what happens?

3. **Extend** — Compare reading a large CSV with `inferSchema=True` vs. an explicit `StructType` schema (use `time.time()` to measure). Then use `df.schema.json()` to export the inferred schema and save it — this is a pattern for capturing a schema once and reusing it.

---

## Summary

- `StructType([StructField("name", T.Type(), nullable)])` defines a schema programmatically.
- DDL string shorthand (`"name STRING NOT NULL, age INT"`) is more concise for simple schemas.
- Spark 4.x has ANSI mode on by default: `cast()` raises on invalid input; use `try_cast()` for null-on-failure.
- `nullable=False` is an optimizer hint, not an enforcement — use explicit filters if nulls must be excluded.
- Nested schemas use `StructType` inside `StructField` for structs and `ArrayType(elementType)` for arrays.
- Chapter 6 builds on schema by covering aggregations where types determine aggregate function behaviour.

---

## References

- [PySpark SQL data types](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/data_types.html)
- [Spark SQL data types reference](https://spark.apache.org/docs/latest/sql-ref-datatypes.html)
- [PySpark Column.try_cast](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.try_cast.html)
