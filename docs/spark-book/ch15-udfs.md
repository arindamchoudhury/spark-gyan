# Chapter 13 — User-Defined Functions: Python and pandas UDFs

> *Learning-path topic: I3 (Intermediate)*
> *Written: 2026-05-31 · Spark 4.1.x / Python 3.10+*

!!! warning "🔄 Needs revisiting — Spark 4.2.0 + I3 source trace (flagged 2026-07-18)"
    The I3 trace confirmed the Arrow default at source (`spark.sql.execution.pythonUDF.arrow.enabled = true`) and opened ten gaps. Beyond the stale performance framing noted below, four are worth adding:

    **A UDF's output is always nullable.** `PythonUDF.nullable = true` unconditionally, whatever return type you declare — so downstream null checks can never be optimized away. That is a permanent optimizer cost on top of serialization, and it explains why a UDF hurts more than its own runtime suggests.

    **Mixing UDF types in one `select` costs an extra round trip.** `ExtractPythonUDFs` batches UDFs of the *same* eval type into one node, so a plain UDF beside a pandas UDF produces two nodes and two crossings of the boundary. Visible in `explain()` as `BatchEvalPython` versus `ArrowEvalPython`.

    **The eval-type integer is the real taxonomy.** `PythonEvalType` numbers every variant (100 batched, 101 Arrow-batched, 200 scalar pandas, 201/209 grouped map, 215/216 iterator); it appears in plans and is what the Python worker dispatches on — the fastest way to confirm which flavour ran.

    **Worker reuse is why the Iterator-UDF pattern works**, and Python worker memory sits outside `spark.executor.memory` (`spark.executor.pyspark.memory`) — so heavy pandas UDFs surface as killed containers rather than JVM OOMs.

    Also missing: Arrow silently downgrading with only a `RuntimeWarning` when PyArrow or pandas is absent; non-deterministic UDFs blocking optimizer rewrites; and batch size being capped by bytes (64MB) as well as rows. Full list in the [I3 source trace](../reference/spark-source-map/topics/i3.md).

    The originally-noted gap: Arrow-optimized Python UDFs and Arrow-based PySpark IPC are **enabled by default** as of 4.2.0 ([SPARK-54555]). Two things below are now stale: the performance table describes Arrow-native UDFs as opt-in via `useArrow=True` (4.1+ framing), and the "typically 5–10× faster than Python UDFs" figure for pandas UDFs was measured against non-Arrow Python UDFs — the real gap on 4.2.0 is narrower. The cost *hierarchy* and the reasoning behind it still hold; the defaults and the multipliers need re-measuring on 4.2.0. Spark 4.2.0 also adds Arrow and pandas grouped-aggregation UDFs, not covered here.

Built-in Spark functions cover the vast majority of column transformations. When they don't, UDFs are the escape hatch. Understanding the cost hierarchy — and when to pay it — is what separates accidental slowness from intentional trade-offs.

---

## What you'll learn

- How Python UDFs work and what the Python-JVM boundary costs
- How pandas UDFs (vectorised) eliminate per-row serialisation overhead
- When to use Iterator UDFs for expensive one-time initialisation (model loading)
- How to test UDFs locally without a SparkSession
- The performance hierarchy from built-ins to Python UDFs

---

## The problem this solves

You need to apply a custom scoring model to every row in a 100-million-row DataFrame. A Python UDF serialises each row to Python, scores it, serialises back to JVM — 200 million serialisation operations. A pandas UDF sends one Arrow batch at a time — hundreds of thousands of rows per operation. An Iterator pandas UDF loads the model once per task instead of once per batch. The choice of UDF type can make the difference between a job that takes 2 hours and one that takes 5 minutes.

---

## Core concept

!!! note "Why the JVM-Python boundary exists"
    As covered in **Chapter 1**, PySpark's `DataFrame` in classic mode is a two-layer object. The Python `DataFrame` is a thin proxy; the real object is a `Dataset[Row]` in the driver JVM. DataFrame column expressions like `F.col("x") > 0` are Catalyst expression tree nodes that live entirely in that JVM object — they are compiled to bytecode before any task runs and never touch Python at execution time. A Python UDF (`@F.udf`) is different: it is a function that only exists in the Python process. To execute it, every row must be serialised out of the JVM, sent to a Python worker subprocess, and the result serialised back — which is why the per-row cost is high regardless of what the UDF actually does.

When Spark executes Python code on data, it must cross the JVM-Python boundary. The cost depends on how much data crosses:

| UDF type | Data boundary crossing | Use when |
|---|---|---|
| Built-in `F.` function | None — runs in JVM | Always prefer if available |
| Arrow-native UDF (`useArrow=True`, Spark 4.1+) | Per Arrow batch, no pandas | Need raw speed, `pyarrow.compute` covers the logic |
| pandas UDF (`@F.pandas_udf`) | Per Arrow batch (thousands of rows) | Custom vectorisable logic; ML ecosystem integration |
| Python UDF (`@F.udf`) | Per row (pickle) | Last resort; unavoidable record-by-record logic |

**Python UDF** (`@F.udf`): Spark pickles each row, calls your function, unpickles the result. Slowest; no Catalyst visibility. The return type defaults to `StringType()` if omitted — always declare it explicitly.

**pandas UDF** (`@F.pandas_udf`): Spark serialises batches of rows as Apache Arrow arrays, calls your function with `pd.Series` (or `pd.DataFrame`) arguments, deserialises the result. One function call per batch of 10,000 rows (default `spark.sql.execution.arrow.maxRecordsPerBatch`). Typically 5–10× faster than Python UDFs for vectorisable operations.

**Iterator pandas UDF**: same as pandas UDF but your function receives an iterator of batches. The code outside the loop runs once per task — use this to load a model, compile a regex, or open a database connection once instead of once per batch.

---

## Examples

### Minimal example: Python UDF vs pandas UDF

```python
# Apache Spark 4.1.x / PySpark 4.1.x · Python 3.10+
import pyspark.sql.functions as F
import pyspark.sql.types as T
import pandas as pd
from typing import Iterator
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ch12").master("local[*]").getOrCreate()

df = spark.range(10).withColumn("temp_f", (F.col("id") * 10 + 32).cast(T.DoubleType()))

# Python UDF — row-by-row, slow
@F.udf(T.DoubleType())
def f_to_c_python(temp_f: float) -> float:
    return (temp_f - 32) * 5 / 9

# pandas UDF — vectorised, fast (Spark 3.0+ type-hint style)
@F.pandas_udf(T.DoubleType())
def f_to_c_pandas(temp_f: pd.Series) -> pd.Series:
    return (temp_f - 32) * 5 / 9

df.withColumn("c_python", f_to_c_python("temp_f")) \
  .withColumn("c_pandas", f_to_c_pandas("temp_f")) \
  .show(5)
# +---+------+------------------+------------------+
# | id|temp_f|          c_python|          c_pandas|
# +---+------+------------------+------------------+
# |  0|  32.0|               0.0|               0.0|
# |  1|  42.0| 5.555555555555555| 5.555555555555555|
```

### Building up: Iterator UDF for cold-start model loading

```python
# Apache Spark 4.1.x / PySpark 4.1.x · Python 3.10+
import pyspark.sql.functions as F
import pyspark.sql.types as T
import pandas as pd
from typing import Iterator
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ch12-iter").master("local[*]").getOrCreate()

@F.pandas_udf(T.DoubleType())
def score_with_model(features: Iterator[pd.Series]) -> Iterator[pd.Series]:
    # This block runs ONCE per task — load expensive resource here
    # In production: import joblib; model = joblib.load("/mnt/models/model.pkl")
    multiplier = 0.95  # simulating model.predict(batch)

    for batch in features:           # iterate over Arrow batches
        yield batch * multiplier     # one pd.Series in, one pd.Series out

df = spark.range(100).withColumn("feature", F.col("id").cast(T.DoubleType()))
df.withColumn("score", score_with_model("feature")).show(5)
```

### Testing a UDF locally without SparkSession

```python
# Apache Spark 4.1.x / PySpark 4.1.x · Python 3.10+
import pyspark.sql.functions as F
import pyspark.sql.types as T
import pandas as pd

@F.pandas_udf(T.DoubleType())
def f_to_c(temp_f: pd.Series) -> pd.Series:
    return (temp_f - 32) * 5 / 9

# Test via .func — calls the underlying Python function directly
test_input = pd.Series([32.0, 212.0, 98.6])
result = f_to_c.func(test_input)
print(result.tolist())  # [0.0, 100.0, 37.0]
# No SparkSession needed for this test
```

---

## Common pitfalls

- **Omitting the return type on a Python UDF** — defaults to `StringType()`. A UDF that returns `None` for some inputs will produce `"None"` strings if you forget to declare `T.DoubleType()`. Always declare the return type.
- **Using `PandasUDFType.GROUPED_AGG` or `PandasUDFType.SCALAR`** — this is the deprecated Spark 2.x API. Use Python type hints to declare the UDF type. `@F.pandas_udf("double")` with type hints infers the UDF type automatically.
- **Mutating the input `pd.Series` inside a pandas UDF** — pandas operations may share underlying memory. Mutating in place produces hard-to-diagnose bugs. Always work on a copy or return a new Series.
- **Type checker warnings on `@F.pandas_udf`** — Pylance and pyright report `No overloads for "pandas_udf" match the provided arguments`. This is a known PySpark stub bug (SPARK-43189). Suppress with `# type: ignore[call-overload]` or `# type: ignore[arg-type]`.
- **Using `.func` for testing but getting different behaviour in Spark** — `.func` bypasses Spark's Arrow serialisation. If your UDF depends on the precise dtype Spark sends (e.g., Arrow float32 vs float64), `.func` won't reproduce it. Also test with a small Spark DataFrame.

---

## Exercises

1. **Recall** — What is the performance difference between a Python UDF and a pandas UDF? What mechanism makes pandas UDFs faster?

2. **Apply** — Write a pandas UDF that takes a `pd.Series` of temperature strings in the format `"XX.X°F"` and returns a `pd.Series` of Celsius doubles. Test it with `.func()` first, then apply it to a Spark DataFrame.

3. **Extend** — Implement an Iterator UDF that simulates model loading: outside the loop, load a sklearn `StandardScaler` fitted to `[0, 100]`; inside the loop, apply `.transform()` to each batch. Measure the execution time vs a plain pandas UDF that loads the scaler inside the loop on every batch.

---

## Summary

- Built-in `F.` functions are fastest; pandas UDFs are next; Python UDFs are slowest — due to per-row vs per-batch boundary crossing.
- Declare the return type explicitly: `@F.udf(T.DoubleType())` or `@F.pandas_udf(T.DoubleType())`.
- Use Iterator pandas UDFs when initialisation is expensive (model loading, regex compilation) — code outside the loop runs once per task.
- Test UDFs with `.func()` on a local `pd.Series` — no SparkSession needed for unit tests.
- Never use `PandasUDFType.SCALAR/GROUPED_AGG` — use Python type hints instead (Spark 3.0+ style).
- Chapter 3 covers RDDs — the lower-level alternative when DataFrames are genuinely restrictive.

---

## References

- [PySpark pandas_udf API](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.pandas_udf.html)
- [PySpark arrow/pandas user guide](https://spark.apache.org/docs/latest/api/python/user_guide/sql/arrow_pandas.html)
