# Chapter 11 — Complex Types: Arrays, Maps, and Structs

> *Learning-path topic: I1 (Intermediate)*
> *Written: 2026-05-31 · Spark 4.1.x / Python 3.10+*

Real-world data is rarely flat. Event logs carry arrays of tags, JSON APIs return nested objects, and analytical schemas embed structs. The three complex column types — Array, Map, Struct — are what separates PySpark from a tool that works on toy data from one that works on production data.

---

## What you'll learn

- The three complex types: `ArrayType`, `MapType`, `StructType`
- How to navigate nested struct fields with dot notation
- How to expand arrays into rows with `explode` and its variants
- Higher-order functions for operating on arrays without row explosion
- How to build and collapse complex types with `collect_list` and `struct()`

---

## The problem this solves

You receive a JSON dataset where each user record has an array of product IDs they browsed and a nested struct of address fields. To compute "how many sessions included a purchase", you need to expand those arrays. To compute "users per city", you need to navigate the struct. Without complex type operations, you are stuck doing `toPandas()` — which collapses the distributed advantage entirely.

---

## Core concept

PySpark has three complex column types, each mapping to a Python analogy:

| PySpark type | Python analogy | What it stores |
|---|---|---|
| `ArrayType(elementType)` | `list` | Ordered sequence of values of one type |
| `MapType(keyType, valueType)` | `dict` | Key-value pairs; keys must be the same type |
| `StructType([StructField(...)])` | `dataclass` / `namedtuple` | Named, typed fields — a schema within a column |

All three appear as column values in a DataFrame row. They can be nested inside each other.

**Navigation:** struct fields are accessed with dot notation — `F.col("address.city")` reads the `city` field from the `address` struct column. Bracket notation (`F.col("address")["city"]`) is equivalent but reserved for maps. For column names that themselves contain dots, use backtick escaping: `` F.col("`address.city`") ``.

**`explode` vs higher-order functions:** `explode()` converts one row with an array into N rows — one per element. This is the right tool when you need each element as a separate row. Higher-order functions (`F.transform`, `F.filter`, `F.aggregate`) operate on the array in place — one row in, one row out. They avoid the shuffle overhead of row explosion when you only need to transform or aggregate within the array.

---

## Examples

### Minimal example: struct navigation and array explode

```python
# Apache Spark 4.1.x / PySpark 4.1.x · Python 3.10+
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ch10").master("local[*]").getOrCreate()

schema = T.StructType([
    T.StructField("user_id", T.StringType()),
    T.StructField("address", T.StructType([
        T.StructField("city",    T.StringType()),
        T.StructField("country", T.StringType()),
    ])),
    T.StructField("tags", T.ArrayType(T.StringType())),
])

data = [
    ("u1", {"city": "London", "country": "UK"},    ["spark", "python", "data"]),
    ("u2", {"city": "Paris",  "country": "FR"},    ["java", "spark"]),
    ("u3", {"city": "London", "country": "UK"},    ["python"]),
]
df = spark.createDataFrame(data, schema)

# Navigate struct fields with dot notation
df.select("user_id", F.col("address.city").alias("city")).show()
# +-------+------+
# |user_id|  city|
# +-------+------+
# |     u1|London|
# |     u2| Paris|
# |     u3|London|
# +-------+------+

# Explode array: one row per tag
df.select("user_id", F.explode("tags").alias("tag")).show()
# +-------+------+
# |user_id|   tag|
# +-------+------+
# |     u1| spark|
# |     u1|python|
# |     u1|  data|
# |     u2|  java|
# |     u2| spark|
# |     u3|python|
# +-------+------+
```

### Building up: higher-order functions and collect

```python
# Apache Spark 4.1.x / PySpark 4.1.x · Python 3.10+
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ch10-hof").master("local[*]").getOrCreate()

data = [("u1", [85, 92, 78, 95]), ("u2", [70, 65, None, 80])]
df = spark.createDataFrame(data, ["user_id", "scores"])

# transform — apply function to each element, keep as array
df.withColumn("scaled", F.transform("scores", lambda x: x * 1.1)).show()
# +-------+------------------+----------------------------+
# |user_id|            scores|                      scaled|
# +-------+------------------+----------------------------+
# |     u1|[85, 92, 78, 95]  |[93.5, 101.2, 85.8, 104.5]  |

# filter — keep only elements satisfying a condition
df.withColumn("passing", F.filter("scores", lambda x: x >= 75)).show()

# aggregate — reduce array to a single value (sum)
df.withColumn("total", F.aggregate("scores", F.lit(0), lambda acc, x: acc + x)).show()

# collect_list — aggregate rows back into an array (inverse of explode)
exploded = df.select("user_id", F.explode("scores").alias("score"))
exploded.groupBy("user_id").agg(F.collect_list("score").alias("scores_back")).show()
```

### Map type operations

```python
# Apache Spark 4.1.x / PySpark 4.1.x · Python 3.10+
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ch10-map").master("local[*]").getOrCreate()

schema = T.StructType([
    T.StructField("user_id",  T.StringType()),
    T.StructField("metadata", T.MapType(T.StringType(), T.StringType())),
])
data = [
    ("u1", {"plan": "premium", "region": "EU", "trial": "false"}),
    ("u2", {"plan": "free",    "region": "US"}),
]
df = spark.createDataFrame(data, schema)

# Access map value by key with bracket notation
df.withColumn("plan", F.col("metadata")["plan"]).show()
# +-------+-----------------------------------+-------+
# |user_id|                           metadata|   plan|
# +-------+-----------------------------------+-------+
# |     u1|{plan -> premium, region -> EU, ...}|premium|
# |     u2|       {plan -> free, region -> US}|   free|
# +-------+-----------------------------------+-------+

# Explode map into key-value rows
df.select("user_id", F.explode("metadata").alias("key", "value")).show()
```

---

## Common pitfalls

- **`explode()` silently drops rows with null or empty arrays** — if a row's array is null, the row disappears. Use `explode_outer()` to keep null-array rows as a single row with a null element value.
- **Only one generator per `select()`** — you cannot use `explode()` twice in the same `select()`. Chain two `select()` calls for multiple explosions.
- **Dot notation vs bracket notation on maps** — `F.col("map.key")` does NOT work for map access; it looks for a nested struct field named `key`. Use `F.col("map")["key"]` for maps.
- **`collect_list()` is non-deterministic** — the order of elements in the resulting array depends on partition and task execution order. If order matters, use `collect_list()` after `orderBy()` within a window function, not on a plain DataFrame.
- **`posexplode()` returns two columns and requires `select()`** — `withColumn()` only accepts one column at a time. Use `df.select("*", F.posexplode("arr").alias("pos", "val"))`.

---

## Exercises

1. **Recall** — What is the difference between `explode()` and `explode_outer()` when the array column contains null values?

2. **Apply** — Create a DataFrame with a `tags` array column. Use `F.transform()` to uppercase every tag in place (no row explosion). Then use `F.filter()` to keep only tags longer than 4 characters.

3. **Extend** — Implement a pipeline that: (1) reads a JSON file with nested structs and arrays, (2) extracts two struct fields into flat columns, (3) explodes an array column, (4) groups by one of the extracted fields and counts the total number of exploded elements.

---

## Summary

- Three complex column types: `ArrayType` (ordered list), `MapType` (key-value), `StructType` (named fields).
- Navigate struct fields with dot notation: `F.col("address.city")`. Access map values with brackets: `F.col("map")["key"]`.
- `explode()` turns one row-with-array into N rows — one per element; `explode_outer()` preserves null-array rows.
- Higher-order functions (`F.transform`, `F.filter`, `F.aggregate`, `F.exists`) operate on arrays in place without row explosion.
- `collect_list()` / `collect_set()` are the inverse of `explode()` — aggregate rows back into an array.
- Chapter 12 covers window functions — a technique that lets you compute across groups while keeping every row.

---

## References

- [PySpark array functions](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions/array_functions.html)
- [PySpark complex types](https://spark.apache.org/docs/latest/sql-ref-datatypes.html)
