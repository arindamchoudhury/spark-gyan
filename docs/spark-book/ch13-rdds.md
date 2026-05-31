# Chapter 13 — RDD Fundamentals

> *Learning-path topic: I4 (Intermediate)*
> *Written: 2026-05-31 · Spark 4.1.x / Python 3.10+*

The RDD (Resilient Distributed Dataset) is Spark's original data model — a schema-free distributed collection of Python objects. Understanding it explains what the DataFrame API is built on and reveals when to reach below the DataFrame abstraction.

---

## What you'll learn

- What an RDD is and how it differs from a DataFrame
- How to create an RDD and apply `map`, `filter`, and `reduce`
- Why RDDs are slower for structured data (and when they aren't)
- The Python-JVM cost model for RDD operations
- How to convert between RDDs and DataFrames

---

## The problem this solves

You receive a dataset of unstructured log lines that don't fit a tabular schema — some lines have 3 fields, others 7. Or you need to implement a graph traversal algorithm where the data structure is inherently object-based. The DataFrame API cannot model these directly; the RDD is the right tool.

---

## Core concept

An RDD is a distributed, immutable collection of Python objects. There is no schema, no column types, and no SQL optimiser. Each element can be any picklable Python object: a string, a tuple, a dict, a numpy array.

The critical cost: every RDD operation in PySpark crosses the JVM-Python boundary. Data is serialised from the JVM (where Spark's storage lives) into Python worker processes, the function runs, then results are serialised back. This happens per partition per operation.

```
JVM (Spark storage) ──cloudpickle──► Python worker (executes your lambda) ──cloudpickle──► JVM
                         ↑ expensive ↑                                      ↑ expensive ↑
```

For structured data, DataFrames keep everything in the JVM via Tungsten's binary row format — the Python process only sends the plan, not the data. This is why DataFrames are typically 3–8× faster than RDDs for tabular operations.

**Use RDDs when:**
- Data is genuinely unstructured (no fixed schema)
- You need arbitrary Python objects (not tables)
- You're implementing custom algorithms with no relational equivalent

**The three core higher-order operations:**

| Operation | Input | Output | Analogous to |
|---|---|---|---|
| `map(f)` | Each element | New element | `[f(x) for x in rdd]` |
| `filter(f)` | Each element | Keep if `f(x)` is truthy | `[x for x in rdd if f(x)]` |
| `reduce(f)` | Pairs of elements | Single value | `functools.reduce(f, rdd)` |

---

## Examples

### Minimal example: create, map, filter, collect

```python
# Apache Spark 4.1.x / PySpark 4.1.x · Python 3.10+
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ch13").master("local[*]").getOrCreate()
sc = spark.sparkContext

# Create an RDD from a Python list
numbers = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])

# map — apply a function to every element
squared = numbers.map(lambda x: x ** 2)

# filter — keep elements where condition is True
even_squared = squared.filter(lambda x: x % 2 == 0)

# collect — action: bring all elements to the driver as a Python list
result = even_squared.collect()
print(result)   # [4, 16, 36, 64, 100]

# take — collect only the first N elements (safer for large RDDs)
print(even_squared.take(3))   # [4, 16, 36]
```

### Building up: key-value RDDs and reduce

```python
# Apache Spark 4.1.x / PySpark 4.1.x · Python 3.10+
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ch13-kv").master("local[*]").getOrCreate()
sc = spark.sparkContext

# Key-value pairs — (word, 1) for word counting
words = sc.parallelize(["the", "quick", "brown", "fox", "the", "fox", "the"])

# map each word to a (word, 1) pair
pairs = words.map(lambda w: (w, 1))

# reduceByKey — aggregate values by key: (word, sum_of_1s)
counts = pairs.reduceByKey(lambda a, b: a + b)

print(counts.collect())
# [('the', 3), ('quick', 1), ('brown', 1), ('fox', 2)]

# reduce — fold the entire RDD to a single value
total = sc.parallelize([1, 2, 3, 4, 5]).reduce(lambda a, b: a + b)
print(total)   # 15

# flatMap — map followed by flatten (list output per element)
sentences = sc.parallelize(["hello world", "foo bar baz"])
words_flat = sentences.flatMap(lambda s: s.split(" "))
print(words_flat.collect())   # ['hello', 'world', 'foo', 'bar', 'baz']
```

### Converting between RDD and DataFrame

```python
# Apache Spark 4.1.x / PySpark 4.1.x · Python 3.10+
import pyspark.sql.types as T
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ch13-convert").master("local[*]").getOrCreate()
sc = spark.sparkContext

# RDD of tuples → DataFrame
rdd = sc.parallelize([(1, "Alice", 95000), (2, "Bob", 87000)])
schema = T.StructType([
    T.StructField("id",     T.IntegerType()),
    T.StructField("name",   T.StringType()),
    T.StructField("salary", T.IntegerType()),
])
df = spark.createDataFrame(rdd, schema)
df.show()
# +---+-----+------+
# | id| name|salary|
# +---+-----+------+
# |  1|Alice| 95000|
# |  2|  Bob| 87000|
# +---+-----+------+

# DataFrame → RDD (each row becomes a Row object)
back_to_rdd = df.rdd
print(back_to_rdd.first())   # Row(id=1, name='Alice', salary=95000)
```

---

## Common pitfalls

- **`reduce()` requires a commutative and associative function** — Spark reduces partitions in parallel, then merges the partial results. If your function is not commutative (`f(a,b) != f(b,a)`) or not associative (`f(f(a,b),c) != f(a,f(b,c))`), results will be non-deterministic. Order subtraction (`a - b`) violates commutativity.
- **`collect()` on a large RDD causes OOM** — `collect()` brings everything to the driver. Use `take(N)` for sampling, `count()` for size, or `saveAsTextFile()` for writing large results.
- **RDD actions trigger re-computation** — like DataFrames, RDDs are lazy. Without `cache()`, calling `count()` twice re-runs the full chain twice. Call `rdd.cache()` if you need to query the same RDD multiple times.
- **Python RDDs are 3–8× slower than Scala RDDs for the same logic** — every operation crosses the Python-JVM boundary. For RDD-heavy code, prefer Scala or switch to DataFrames.
- **`sc.parallelize()` is for testing, not production ingestion** — it puts data on the driver then distributes it. For large data, use `spark.read` to load directly into partitions without the driver bottleneck.

---

## Exercises

1. **Recall** — Why must the function passed to `reduce()` be commutative and associative? Give an example of a function that would produce wrong results if it violated these properties.

2. **Apply** — Use an RDD to count word frequencies in a list of sentences. Use `flatMap()` to tokenise, `map()` to create `(word, 1)` pairs, and `reduceByKey()` to count. Then convert the result to a DataFrame with an explicit schema.

3. **Extend** — Compare the performance of the word-count pipeline using: (1) RDD `map` + `reduceByKey`, (2) `spark.createDataFrame()` + `F.explode(F.split(...))` + `groupBy().count()`. Measure wall-clock time for a 10 MB text file. What does this reveal about when RDDs are justified?

---

## Summary

- An RDD is a distributed, schema-free collection of Python objects — no types, no SQL optimiser.
- Core operations: `map()` (transform each element), `filter()` (keep matching elements), `reduce()` (fold to one value), `flatMap()` (transform and flatten), `reduceByKey()` (aggregate by key in key-value RDDs).
- RDD operations cross the Python-JVM boundary per partition per operation — slower than DataFrames for structured data.
- `collect()` brings all data to the driver — use only when the result is small; prefer `take(N)` for sampling.
- Convert RDD → DataFrame with `spark.createDataFrame(rdd, schema)`; DataFrame → RDD with `df.rdd`.
- Chapter 14 covers partitioning — how Spark distributes data and how to control it.

---

## References

- [PySpark RDD programming guide](https://spark.apache.org/docs/latest/rdd-programming-guide.html)
- [PySpark SparkContext API](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.SparkContext.html)
