# `pyspark.errors` Exception Hierarchy

> *Cross-chapter synthesis — Rioux (2022), Chapters 4, 5, 6, 8.*
>
> Spark 3.3 reorganised all PySpark exceptions under a new `pyspark.errors` module. Ch 4 introduces the import change; subsequent chapters surface new exception classes as each feature is covered. Ch 8 adds an important caveat about classic RDD operations.

---

## The import change (Spark 3.3+)

Before Spark 3.3, PySpark exceptions were scattered across the module tree or required catching broad Java exceptions via Py4J. From Spark 3.3 onward, all PySpark-specific exceptions live under `pyspark.errors`:

```python
# Old (still works but unorganised)
from pyspark.sql.utils import AnalysisException

# New canonical import (Spark 3.3+)
from pyspark.errors import AnalysisException
from pyspark.errors import SparkNumberFormatException
from pyspark.errors import SparkArithmeticException
from pyspark.errors import SparkRuntimeException
from pyspark.errors import PySparkTypeError
```

All old import paths are still aliased for backward compatibility, but `pyspark.errors` is the authoritative location.

---

## Exception class reference

| Exception | Trigger | First covered |
|---|---|---|
| `AnalysisException` | Column not found, ambiguous reference, schema mismatch, invalid plan | Ch 4 |
| `SparkNumberFormatException` | `cast()` fails on non-numeric string (ANSI mode on) | Ch 4 |
| `SparkArithmeticException` | Integer overflow in arithmetic/aggregation (ANSI mode on) | Ch 5 |
| `SparkRuntimeException` | FAILFAST mode hits a malformed/null record during JSON/CSV read | Ch 6 |
| `PySparkTypeError` | Wrong Python type passed to a PySpark API | Ch 4 |
| `PySparkValueError` | Invalid argument value (e.g., negative `n` in `head`) | Ch 4 |

---

## Ch 4 — `AnalysisException` and `SparkNumberFormatException`

```python
from pyspark.errors import AnalysisException, SparkNumberFormatException

try:
    df.select("nonexistent_column").show()
except AnalysisException as e:
    print(f"Plan error: {e}")

try:
    df.withColumn("n", F.col("text").cast("int")).show()
except SparkNumberFormatException as e:
    print(f"Cast failed: {e}")
# Use F.try_cast() to avoid raising on bad input
```

---

## Ch 5 — `SparkArithmeticException`

```python
from pyspark.errors import SparkArithmeticException

try:
    df.agg(F.sum("small_int_col")).show()
except SparkArithmeticException as e:
    print(f"Overflow: {e}")
# Fix: upcast to LongType or use F.try_sum()
```

---

## Ch 6 — `SparkRuntimeException` from FAILFAST mode

When reading with `mode="FAILFAST"`, any malformed record or unexpected null raises `SparkRuntimeException` at action time:

```python
from pyspark.errors import SparkRuntimeException

schema = T.StructType([
    T.StructField("id", T.IntegerType(), nullable=False),
    T.StructField("name", T.StringType()),
])

try:
    df = spark.read.schema(schema).option("mode", "FAILFAST").json("data/")
    df.show()   # exception raised here, not at read()
except SparkRuntimeException as e:
    print(f"Malformed record: {e}")
```

`mode="FAILFAST"` is lazy — the exception fires at the action (`.show()`, `.collect()`, `.write()`), not at the `.read` call.

---

## Ch 8 — RDD caveat: `Py4JJavaError` not `pyspark.errors`

For classic RDD operations, PySpark exceptions do **not** surface as `pyspark.errors` classes. When a Python UDF or map function raises inside an RDD stage, Spark wraps the error in a Java `PythonException` on the JVM side. PySpark surfaces this as `py4j.protocol.Py4JJavaError` with the original Python traceback embedded in the string message:

```python
from py4j.protocol import Py4JJavaError

rdd = spark.sparkContext.parallelize([1, 0, 2])
try:
    rdd.map(lambda x: 1 // x).collect()
except Py4JJavaError as e:
    # e.java_exception.getMessage() contains the Python ZeroDivisionError traceback
    print(str(e))
```

> ❓ Spark 4.x may improve RDD exception propagation; verify with `pyspark.errors.PythonException` if it exists in your version.

**Summary of the two worlds:**

| API | Exception type |
|---|---|
| DataFrame / SQL / structured streaming | `pyspark.errors.*` |
| Classic RDD operations | `py4j.protocol.Py4JJavaError` wrapping `PythonException` |

---

## Summary

- Import from `pyspark.errors` (canonical since Spark 3.3); old paths still work but are deprecated.
- `AnalysisException` — plan/schema errors; `SparkNumberFormatException` — bad cast; `SparkArithmeticException` — overflow.
- `SparkRuntimeException` fires at the action, not the read call, in FAILFAST mode.
- Classic RDD operations surface errors as `Py4JJavaError` — the `pyspark.errors` hierarchy does not apply.
- Use `F.try_cast()`, `F.try_sum()`, or `mode="PERMISSIVE"` to handle errors without raising.

---

## Chapter links

- [Ch 4 — Tabular data and CSV ingestion](../books/rioux/chapters/04-tabular-data.md)
- [Ch 5 — Joining and Grouping](../books/rioux/chapters/05-joining-grouping.md)
- [Ch 6 — JSON and complex types](../books/rioux/chapters/06-json-data.md)
- [Ch 8 — RDDs and UDFs](../books/rioux/chapters/08-rdd-udfs.md)
