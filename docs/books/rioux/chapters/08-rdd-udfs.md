# Chapter 8 — Extending PySpark with Python: RDD and UDFs

> *Source: Rioux (2022), Chapter 8, pages 175–191.*
>
> PySpark's DataFrame API covers most data-manipulation needs, but sometimes you need to run arbitrary Python logic on your data. This chapter introduces two escape hatches: the **RDD** (a schema-free, object-level container) and **Python UDFs** (a way to promote any Python function to a DataFrame column transformer).
>
> 📌 **Notes adapted to PySpark 4.1.1 / Python 3.10+.**
>
> - The RDD API is still fully present in 4.1.1 — it has not been deprecated. `SparkContext.parallelize()` and all RDD methods shown in the chapter work as documented.
> - The `F.udf()` function and `@F.udf()` decorator are unchanged. Return type defaults to `StringType` if omitted (not new to 4.x).
> - **Spark 4.1.0 new**: `udf()` now also accepts vectorized functions via type hints (same syntax as `pandas_udf`). The scalar Python UDF API in this chapter is unaffected.
> - **Python 3.10+ typing**: `from typing import Tuple, Optional` still works, but modern Python prefers `tuple[int, int]` and `int | None`. The book's code is correct; both styles are valid.
> - `Py4JJavaError` (used in the chapter's error-handling example) remains in `py4j.protocol`.

---

## 1. The RDD — a schema-free distributed container

### What it is

- A **resilient distributed dataset** is a distributed collection of Python objects with no required schema, ordering, or type consistency — "a bag of elements."
- Contrast with the DataFrame: DataFrames are column-major (structured, typed); RDDs are element-major (arbitrary Python objects).
- The RDD shines in two specific cases:
  - An unordered collection of arbitrary Python objects (picklable).
  - Unordered key-value pairs (like a Python dict).

> 💡 **Default to DataFrames.** The RDD is still available but DataFrames are faster and cleaner for everything tabular. Reach for the RDD only when the DataFrame is genuinely restrictive.

### Creating an RDD

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext          # SparkContext is accessed as an attribute of SparkSession

collection = [1, "two", 3.0, ("four", 4), {"five": 5}]
collection_rdd = sc.parallelize(collection)
# ParallelCollectionRDD[0] at parallelize at PythonRDD.scala:195
```

- `spark.sparkContext` gives access to the `SparkContext` — the RDD API lives here.
- `sc.parallelize(list)` serialises (pickles) each element and distributes them across worker nodes.
- The RDD is **lazy** just like a DataFrame: errors only surface at action time (`collect()`, etc.).

### Peeking inside a DataFrame's RDD

A DataFrame is, under the hood, an `RDD[Row]`:

```python
df = spark.createDataFrame([[1], [2], [3]], schema=["column"])
df.rdd.collect()
# [Row(column=1), Row(column=2), Row(column=3)]
```

Moving between DataFrame and RDD is possible but costs a column-major → row-major conversion. Prefer UDFs (§2) over this pattern.

---

## 2. Manipulating an RDD: `map()`, `filter()`, `reduce()`

These three **higher-order functions** take another function as their parameter and apply it element-wise. They form the core of the RDD API and are the direct inspiration for the MapReduce framework.

### `map()` — apply a function to every element

```python
def add_one(value):
    return value + 1

collection_rdd = collection_rdd.map(add_one)
collection_rdd.collect()   # ← action; explodes here if function fails on any element
```

- `map(f)` returns a new RDD where each element is replaced by `f(element)`.
- If `f` raises on any element, the error surfaces at the next action.
- Always write defensive functions when mapping over heterogeneous RDDs:

```python
def safer_add_one(value):
    try:
        return value + 1
    except TypeError:
        return value

collection_rdd.map(safer_add_one).collect()
# [2, 'two', 4.0, ('four', 4), {'five': 5}]
```

### `filter()` — keep elements that satisfy a predicate

```python
collection_rdd.filter(lambda elem: isinstance(elem, (float, int))).collect()
# [2, 4.0]
```

- `filter(f)` keeps only elements where `f(element)` is **truthy** (Python's truthy rules — not just `True`/`False`).
- Lambda functions are convenient for simple one-use predicates.

> 💡 **Lambda vs named function**: both are valid. A named function is easier to test; a lambda saves lines when the predicate is trivially short. Either is idiomatic.

### `reduce()` — fold the collection to a single value

```python
from operator import add

sc.parallelize([4, 7, 9, 1, 3]).reduce(add)   # 24
```

- `reduce(f)` applies `f(a, b)` pair-wise across all elements until one value remains.
- **The function must be commutative and associative** — Spark reduces per-partition in parallel, then merges the intermediate results on the driver. Functions like `subtract()` will produce wrong results.
- Commutative + associative examples: `add`, `multiply`, `min`, `max`.

### MapReduce lineage

Google's 2004 MapReduce paper directly inspired Hadoop and, later, Spark. The names `map` and `reduce` are the same concepts. Modern Spark abstractions (DataFrames, query plans) sit on top, but understanding the primitives helps reason about distributed execution.

---

## 3. Python UDFs on DataFrames

UDFs (**user-defined functions**) bring the flexibility of arbitrary Python code to the DataFrame API. They bridge the gap between the RDD's openness and the DataFrame's structure.

- A UDF wraps a Python function so it can be applied to a DataFrame **column** (scalar, row-by-row).
- Lives in `pyspark.sql.functions`: `F.udf()`.

### 3.1 Write the Python function first

Blueprint:

1. Write and document the function.
2. Annotate input and return types (Python type hints).
3. Test with plain Python assertions before promoting to a UDF.

```python
from fractions import Fraction
from typing import Tuple, Optional     # Python 3.10+: tuple[int,int] and int|None also work

Frac = Tuple[int, int]   # type alias for readability

def py_reduce_fraction(frac: Frac) -> Optional[Frac]:
    """Reduce a fraction represented as a 2-tuple of integers."""
    num, denom = frac
    if denom:
        answer = Fraction(num, denom)
        return answer.numerator, answer.denominator
    return None

assert py_reduce_fraction((3, 6)) == (1, 2)
assert py_reduce_fraction((1, 0)) is None

def py_fraction_to_float(frac: Frac) -> Optional[float]:
    """Transform a fraction (2-tuple) to float."""
    num, denom = frac
    if denom:
        return num / denom
    return None

assert py_fraction_to_float((2, 8)) == 0.25
assert py_fraction_to_float((10, 0)) is None
```

- `Optional[Frac]` means "either a `Frac` or `None`". PySpark maps Python `None` to Spark `null`.
- Type annotations aren't enforced at runtime by default, but `mypy` will catch type mismatches. They also document intent for other readers.

### 3.2 Python ↔ PySpark type mapping

| PySpark type | Python equivalent |
| --- | --- |
| `T.NullType()` | `None` |
| `T.StringType()` | `str` |
| `T.BooleanType()` | `bool` |
| `T.LongType()` | `int` |
| `T.DoubleType()` | `float` |
| `T.FloatType()` | `float` (less precision) |
| `T.ArrayType(T)` | `list`, `tuple`, or numpy array |
| `T.MapType(K, V)` | `dict` |
| `T.StructType([…])` | `list` or `tuple` |
| `T.DateType()` | `datetime.date` |
| `T.TimestampType()` | `datetime.datetime` |

> ⚠️ Python `int` is unbounded; PySpark's `LongType` is 64-bit. A Python `int` exceeding `pow(2, 63) - 1` will silently overflow or error. Always use the narrowest type that fits.

### 3.3 Two ways to create a UDF

**Option A — `F.udf()` function:**

```python
import pyspark.sql.functions as F
import pyspark.sql.types as T

SparkFrac = T.ArrayType(T.LongType())

reduce_fraction = F.udf(py_reduce_fraction, SparkFrac)

frac_df = frac_df.withColumn(
    "reduced_fraction", reduce_fraction(F.col("fraction"))
)
```

**Option B — `@F.udf()` decorator (preferred when defining a new function):**

```python
@F.udf(T.DoubleType())
def fraction_to_float(frac: Frac) -> Optional[float]:
    """Transform a fraction (2-tuple) to float."""
    num, denom = frac
    if denom:
        return num / denom
    return None

frac_df = frac_df.withColumn(
    "fraction_float", fraction_to_float(F.col("reduced_fraction"))
)

# Access the underlying Python function via .func:
assert fraction_to_float.func((1, 2)) == 0.5
```

Both forms are equivalent. The decorator keeps the function definition and its UDF promotion in one place.

> ⚠️ **UDFs are slower than built-in functions.** Every row crosses the Python–JVM boundary (serialisation + deserialisation). Use built-in `pyspark.sql.functions` whenever possible; reach for a UDF only when no built-in covers your logic. Chapter 9 introduces pandas UDFs which are significantly faster for column-level operations.

### 3.4 Testing UDFs locally before distributing

Because UDF stack traces surface at action time and are noisy, test the underlying Python function with plain assertions first:

```python
assert py_reduce_fraction((3, 6)) == (1, 2)   # ← runs on the driver, instant feedback
```

After promoting, use `.func` to test the UDF's Python layer without Spark:

```python
assert fraction_to_float.func((2, 4)) == 0.5
```

---

## 4. Summary

- The **RDD** distributes arbitrary Python objects; it is element-major, schema-free, and flexible. Use it when the DataFrame's column structure is genuinely a constraint.
- Core RDD operations are higher-order functions: `map(f)` transforms each element, `filter(f)` keeps elements where `f` is truthy, `reduce(f)` folds the collection to one value. `reduce` requires a commutative and associative `f`.
- A **Python UDF** promotes a Python function to a Spark column operator. Create it with `F.udf(fn, return_type)` or the `@F.udf(return_type)` decorator.
- Always write and test the Python function first; annotate input/output types; use `fn.func(...)` to test without Spark overhead.
- UDFs cross the Python–JVM boundary row by row — they are slower than built-ins. Prefer `pyspark.sql.functions`; use pandas UDFs (Chapter 9) when you need custom logic at scale.

---

## 5. References

- PySpark RDD API: https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.RDD.html
- PySpark UDF docs: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.udf.html
- UDF and UDTF user guide: https://spark.apache.org/docs/latest/api/python/user_guide/udfandudtf.html
- RDD Programming Guide: https://spark.apache.org/docs/latest/rdd-programming-guide.html
- Python `typing` module: https://docs.python.org/3/library/typing.html
- Google MapReduce paper: https://research.google/pubs/pub62/
