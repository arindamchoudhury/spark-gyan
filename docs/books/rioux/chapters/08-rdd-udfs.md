# Chapter 8 — Extending PySpark with Python: RDD and UDFs

> *Source: Rioux (2022), Chapter 8, pages 175–191.*
>
> PySpark's DataFrame API covers most data-manipulation needs, but sometimes you need to run arbitrary Python logic on your data. This chapter introduces two escape hatches: the **RDD** (a schema-free, object-level container) and **Python UDFs** (a way to promote any Python function to a DataFrame column transformer).
>
!!! info "📌 Notes adapted to PySpark 4.1.1 / Python 3.10+"

    - The RDD API is still fully present in 4.1.1 — it has not been deprecated. `SparkContext.parallelize()` and all RDD methods shown in the chapter work as documented.
    - The `F.udf()` function and `@F.udf()` decorator are unchanged. Return type defaults to `StringType` if omitted (not new to 4.x).
    - **Spark 4.1.0 new**: `udf()` now also accepts vectorized functions via type hints (same syntax as `pandas_udf`). The scalar Python UDF API in this chapter is unaffected.
    - **Python 3.10+ typing**: `from typing import Tuple, Optional` still works, but modern Python prefers `tuple[int, int]` and `int | None`. The book's code is correct; both styles are valid.
    - `Py4JJavaError` (used in the chapter's error-handling example) is still the correct catch for classic RDD operations in 4.1.1. `pyspark.errors.PythonException` applies to DataFrame/Spark Connect paths, not RDD — see §3 error-handling note.

---

## 1. The RDD — a schema-free distributed container

### What it is

- A **resilient distributed dataset** is a distributed collection of Python objects with no required schema, ordering, or type consistency — "a bag of elements."
- Contrast with the DataFrame: a DataFrame is a **distributed table** — named, typed columns enforced by a schema. The Spark SQL layer that executes DataFrames uses Tungsten's `UnsafeRow` binary format for storage and Catalyst for query optimization. An RDD is element-major (arbitrary Python objects, no schema, no optimizer).
- The RDD shines in two specific cases:
    - An unordered collection of arbitrary Python objects (picklable).
    - Unordered key-value pairs (like a Python dict).

!!! info "💡 Default to DataFrames — but the 'RDD is slower' claim needs context"

!!! warning "Where DataFrames are faster (structured/tabular data)"
    - Catalyst can't see inside a lambda — predicate pushdown, projection pruning, and constant folding don't apply to RDD operations.
    - Python RDDs add cloudpickle serialization + Python-JVM bridge overhead on every operation.

    **Catalyst optimisations that RDDs miss:**

    | Optimisation | What it does | RDD equivalent |
    |---|---|---|
    | **Predicate pushdown** | Moves `filter` as early as possible — ideally into the file reader so unmatched rows are never loaded | Lambda runs after all data is loaded |
    | **Projection pruning** | Reads only the columns the query actually uses from Parquet/ORC | All columns always loaded |
    | **Constant folding** | Evaluates constant sub-expressions at planning time — `salary * (100 + 10)` → `salary * 110` before any row is touched | Full expression re-evaluated for every element at runtime |

    **Where RDDs are not slower (or are the right tool):**
    - Unstructured data (text streams, arbitrary Python objects, binary blobs) — forcing it into a DataFrame schema adds overhead with no benefit.
    - Custom algorithms with no relational equivalent (`reduce`, graph traversal, etc.).
    - Scala/Java RDDs — the Python-JVM bridge cost doesn't apply; the gap vs DataFrames is much narrower.

    **If you need RDDs, prefer Scala or Java over Python.**

    PySpark RDD operations cross the Python↔JVM bridge on every operation: data is cloudpickled to the JVM, unpickled in a Python worker process, the function runs, then pickled back. This happens per partition, per operation.

    Scala/Java RDD functions run natively inside the JVM executor — no serialization bridge, no Python worker. Typically **3–8× faster** for RDD-heavy code.

    | | PySpark RDD | Scala/Java RDD | DataFrame (any language) |
    |---|---|---|---|
    | Execution | Python worker + JVM bridge | Native JVM | Native JVM + Catalyst/WSCG |
    | Serialization overhead | Per partition, per op | None | None (UnsafeRow stays in JVM) |
    | Catalyst optimizations | No | No | Yes |

    For DataFrames, the language gap essentially disappears — Catalyst produces the same optimized physical plan regardless of whether you wrote it in Python, Scala, or Java.

    Reach for the RDD only when the DataFrame is genuinely restrictive.

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
- `sc.parallelize(list)` splits the collection into partitions (default: `sc.defaultParallelism`), **cloudpickles each partition as a unit** (not element-by-element), and ships the serialized bytes to executors.

**Data flow:**
```
Python list (driver)
  → slice into N partitions
  → cloudpickle each partition → bytes
  → send bytes to executor JVM
  → executor Python worker unpickles → Python objects
```
In local mode the "executors" are threads in the same JVM process — no network transfer, but the cloudpickle round-trip still happens.

**Key details:**

| Detail | Notes |
|---|---|
| Serializer | `cloudpickle` (not stdlib `pickle`) — handles lambdas and locally-defined classes |
| Default partitions | `sc.defaultParallelism` — number of local cores in local mode; `spark.default.parallelism` config in cluster mode |
| Custom partitions | `sc.parallelize(collection, numSlices=4)` |
| Partition shape | Contiguous index slices — no shuffle, no sort |
| Driver memory | **The entire collection must fit in driver memory.** `parallelize` is for small seed/test data, not large datasets. For large data, read from a file or distributed store instead. |

**Inspect partitions:**
```python
rdd.getNumPartitions()          # how many partitions
rdd.glom().collect()            # list-of-lists: one inner list per partition
```

- The RDD is **lazy** just like a DataFrame: errors only surface at action time (`collect()`, etc.).

---

## 2. Spark execution internals

### How RDDs and DataFrames are stored in memory

| | RDD | DataFrame |
|---|---|---|
| **Storage location** | JVM heap — each element is a GC-managed object | `UnsafeRow` binary byte array, **on-heap by default**; `sun.misc.Unsafe` is the write API, not an off-heap allocator. True off-heap requires `spark.memory.offHeap.enabled=true`. |
| **GC pressure** | High — every element adds GC overhead; large RDDs cause long GC pauses | Reduced — one byte array per row instead of one JVM object per field. Fully GC-free only with off-heap enabled. |
| **Memory overhead** | PySpark: ~28 bytes/element (Python `int` object — object header + ref count + size field). Scala/Java: ~16 bytes/element (boxed `Integer` — 8-byte JVM header + 4-byte value + 4-byte padding). 1M integers: **~28 MB** (PySpark) or **~16 MB** (Scala/Java) | ~16 bytes/row for a single-column `IntType`: 8-byte null bitmap + 8-byte field slot (all types padded to 8 bytes). 1M single-column integers: **~16 MB**. No JVM object headers; GC pressure reduced (eliminated with off-heap enabled). |
| **PySpark extra cost** | Python object → cloudpickle → JVM → back for every operation | Stays in JVM throughout execution. Python touches data only at action boundaries (`collect()`, `take()`, `first()`, `show()`, `toPandas()`) or when a Python UDF runs (row-by-row serialization) or a pandas UDF runs (Arrow batch). |
| **Cache format** | Deserialized Java objects or serialized byte arrays | Compact `UnsafeRow` blocks — much smaller and faster to reconstruct |

**Why "exactly 4 MB for 1M integers" is wrong — UnsafeRow actual layout:**

Every field in `UnsafeRow` occupies **8 bytes** in the fixed-length region, regardless of its actual type. The field offset formula in the source is `baseOffset + bitSetWidthInBytes + ordinal * 8`. For `IntType`, `setInt` writes 4 bytes of data into that 8-byte slot — the upper 4 bytes are unused padding.

```
Single-column IntType row layout:
  [null bitmap — 8 bytes] [int value in 8-byte slot]
  total = 16 bytes/row
  1M rows = ~16 MB
```

The null bitmap is `ceil(numFields / 64) * 8` bytes — 8 bytes covers up to 64 columns. For wide tables the null bitmap cost is amortized; for single-column tables it doubles the per-row size.

DataFrame still wins over Scala/Java RDD (also ~16 MB for 1M boxed `Integer`) because: no per-object JVM headers, reduced GC, contiguous byte array layout (cache-friendly), and the null bitmap cost shrinks as column count grows.

**Off-heap for execution and caching — both are opt-in:**
- Tungsten's `UnsafeRow` byte arrays are **on-heap by default**. `sun.misc.Unsafe` is the write API used to write unaligned data into those arrays — the "Unsafe" in the name refers to bypassing Java's type safety, not to off-heap allocation.
- Off-heap for both **execution and caching** is opt-in: `spark.memory.offHeap.enabled=true` + `spark.memory.offHeap.size=Xg`.

**Why this matters for PySpark specifically:**
In PySpark, RDD elements live in the Python worker process as Python objects, then get cloudpickled across the py4j bridge to the JVM — two copies and two serializations per operation. DataFrame data stays in the JVM/off-heap world; Python only touches it when you explicitly call `collect()` or trigger a Python UDF.

### DataFrames under the hood

A DataFrame is a **distributed table**. Understanding its physical internals explains when the RDD API is slower — and when it isn't:

| Layer | What it does |
|---|---|
| **Schema** (`StructType`) | Named, typed columns — enforced at the boundary, not at runtime |
| **Tungsten** (`UnsafeRow`) | Compact, off-heap binary row format. Physically row-oriented, like a database heap file. Not columnar. |
| **Catalyst** | Relational query optimizer — rewrites and fuses operations before execution |
| **WSCG** | Whole-stage code generation — compiles chains of operators to a single JVM bytecode loop |
| **Vectorized reader** | Reads Parquet/ORC in Arrow column batches for I/O efficiency (this is where "columnar" applies — on disk, not in memory) |

> The book describes DataFrames as "column-major" — this refers to the **programming model** (you address data by column name: `F.col("age")`), not the physical memory layout. In memory, rows are stored as `UnsafeRow` objects, which is row-oriented, same as a traditional RDBMS table.

### Why Tungsten chose row-oriented over columnar

Tungsten was designed (Spark 1.4, 2015) to fix **CPU and memory bottlenecks**, not I/O. At the time, the choice between row and columnar came down to workload fit:

| Factor | Row-oriented (`UnsafeRow`) | Columnar |
|---|---|---|
| **CPU register fit** | 8-byte aligned fields map directly to 64-bit registers — comparisons in one machine instruction | Column scans are CPU-friendly only for aggregations over few columns |
| **Row-level ops** | Natural for ETL — filter, join, project all touch whole rows | Requires reassembling rows from column buffers; expensive for row-level work |
| **JVM GC** | Off-heap binary format eliminates GC pressure | Same benefit is possible but harder to implement |
| **Write performance** | Append one row directly | Must update N separate column buffers |
| **Mixed workloads** | Handles point access and scans reasonably | Excels at narrow column scans (OLAP); poor for wide-row ETL |

**Core reason:** Spark's dominant workload is ETL — whole-row transformations (filter, join, map). Columnar shines for OLAP (aggregate 2 columns from a 100-column table). UnsafeRow was the better general-purpose choice.

**What "columnar" actually means in core Spark 4.1.1:**

Core Spark has not gone columnar. Every built-in SQL operator (filter, join, agg, sort) runs on `UnsafeRow`. The only place `ColumnarBatch` appears is during Parquet/ORC I/O — and it is immediately converted back to `UnsafeRow` before the first operator runs (`ColumnarToRowExec`). That I/O optimisation is the full extent of columnar support in unmodified Spark.

| What | Format | Since |
|---|---|---|
| Parquet/ORC scan | `ColumnarBatch` (briefly, then converted) | Spark 2.0 |
| All built-in SQL operators | `UnsafeRow` | Spark 1.4 (Tungsten) |
| Shuffle wire | `UnsafeRow` serialized | Spark 1.4 |

**True end-to-end columnar execution requires leaving core Spark:**

- **Apache Gluten + Velox** — open-source plugin that replaces JVM operators with a C++ Velox engine. Used by Microsoft Fabric. Spark 4.1 support exists but is not GA as of May 2026 (ANSI mode must be off; RSS not supported).

    **RSS = Remote Shuffle Service.** In standard Spark, shuffle data (intermediate results between map and reduce stages) is written to local executor disk. A Remote Shuffle Service moves that data to a *separate dedicated server cluster* (e.g. Apache Celeborn, Apache Uniffle), fully decoupling compute from shuffle storage — better fault tolerance and no executor disk pressure. Gluten+Velox cannot plug into these external RSS frameworks.

- **Databricks Photon** — Databricks' proprietary C++ columnar engine. Stable, but closed-source and Databricks-only.

    Databricks avoids the RSS problem differently: it uses the standard Spark **external shuffle service** (enabled by default), which keeps shuffle files alive on the worker's local disk after an executor exits. Photon adds an optimised columnar shuffle for better throughput. Neither is a true RSS — shuffle data stays on local disk attached to the worker node, not a dedicated remote cluster. Photon's shuffle is tightly integrated and proprietary, so it does not need to plug into Celeborn/Uniffle.

Neither is part of the Apache Spark 4.1.1 release.

### What format does Spark 4.1.1 actually use?

`UnsafeRow` is the default for all SQL operators. `ColumnarBatch` appears only during Parquet/ORC I/O and is converted back to rows before the rest of the query runs:

```
Parquet file
  → VectorizedParquetRecordReader → ColumnarBatch   (columnar, fast I/O)
        ↓
  Does the next operator support columnar?
  ├── yes (GPU plugin, Gluten) → stays ColumnarBatch
  └── no  (all built-in operators) → ColumnarToRowExec → UnsafeRow
                                                              ↓
                                               filter / join / agg / sort
                                               all run on UnsafeRow + WSCG
```

| Stage | Format in Spark 4.1.1 |
|---|---|
| Parquet/ORC scan (I/O) | `ColumnarBatch` |
| Filter, join, agg, sort | `UnsafeRow` |
| Shuffle wire | `UnsafeRow` (serialized) |
| Python UDF / RDD boundary | cloudpickle bytes |

`ColumnarBatch` is a brief I/O optimization, not a sign that Spark runs columnar end-to-end. Every built-in SQL operator operates on `UnsafeRow`.

### Enabling columnar execution — `spark.plugins`

External plugins intercept the `ColumnarBatch` path before `ColumnarToRowExec` runs, keeping data columnar end-to-end. Both RAPIDS and Gluten use the same registration mechanism:

```python
SparkSession.builder.config("spark.plugins", "<plugin-class>")
```

**NVIDIA RAPIDS Accelerator (`spark-rapids`) — GPU plugin:**

Executes Spark SQL operations on NVIDIA GPUs. Since GPUs process columnar data natively, the plugin accepts `ColumnarBatch` directly — no `ColumnarToRowExec` step.

```bash
# Step 1 — attach the jar
spark-submit --jars rapids-4-spark_2.12-<version>-cuda12.jar ...
```

```python
# Step 2 — register the plugin
spark = SparkSession.builder \
    .config("spark.plugins", "com.nvidia.spark.SQLPlugin") \
    .config("spark.rapids.sql.concurrentGpuTasks", 2) \
    .getOrCreate()

# Toggle at runtime (on by default once plugin is loaded)
spark.conf.set("spark.rapids.sql.enabled", "true")   # or "false" to fall back to CPU
```

!!! warning "⚠️ Requires NVIDIA GPU hardware + CUDA on every machine running executors"
    Local mode works if the local machine has a GPU; most dev machines don't, so it fails there in practice — not a local-mode restriction, a hardware one.

**Apache Gluten + Velox:**

```python
SparkSession.builder.config("spark.plugins", "org.apache.gluten.GlutenPlugin")
```

Replaces JVM operators with a C++ Velox engine. Not GA for Spark 4.1 as of May 2026 — see note above.

### Peeking inside a DataFrame's RDD

A DataFrame is, under the hood, an `RDD[Row]`:

```python
df = spark.createDataFrame([[1], [2], [3]], schema=["column"])
df.rdd.collect()
# [Row(column=1), Row(column=2), Row(column=3)]
```

Calling `df.rdd` is possible but expensive — avoid it. The cost is **not** a column→row layout conversion (Tungsten is already row-oriented). The real costs are:

1. **Leaving Catalyst/Tungsten** — all query planning and code-generation optimizations are abandoned.
2. **JVM → Python serialization** — each `UnsafeRow` is deserialized into a Python `Row` object and sent across the py4j bridge. This is the dominant overhead.
3. **No vectorized execution** — Tungsten processes rows in tight JVM bytecode loops; the Python RDD path is object-by-object.

Prefer UDFs (§4) over dropping to the RDD API — UDFs stay inside the DataFrame execution model.

---

## 3. Manipulating an RDD: `map()`, `filter()`, `reduce()`

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

### Catching RDD errors — PySpark 4.1.1 reality

For classic RDD `collect()`, PySpark 4.1.1 still raises `Py4JJavaError` — **the book is correct**. PySpark's `pyspark.errors` conversion layer (`captured.py`) does not cover the RDD Python worker path; the `Py4JJavaError` propagates unchanged to user code.

```python
from py4j.protocol import Py4JJavaError

try:
    collection_rdd.map(add_one).collect()
except Py4JJavaError as e:
    print(e)   # noisy: full Java + Python stacktrace
```

**Printing only the Python traceback** — it is embedded as text inside the Java exception message:

```python
from py4j.protocol import Py4JJavaError

try:
    collection_rdd.map(add_one).collect()
except Py4JJavaError as e:
    msg = e.java_exception.getMessage()
    print(msg[msg.find("Traceback"):] if "Traceback" in msg else msg)
```

Output:
```
Traceback (most recent call last):
  File "...worker.py", line N, in add_one
TypeError: can only concatenate str (not "int") to str
```

**`pyspark.errors` hierarchy (4.1.1):**

| Exception | When raised |
|---|---|
| `PySparkException` | Base class for all PySpark exceptions |
| `PythonException` | Python worker error — does **not** reliably surface for classic RDD `collect()` (tested: still raises `Py4JJavaError` there) |
| `AnalysisException` | Unresolved column / bad plan — raised by classic DataFrame and SQL operations |
| `ParseException` | Malformed SQL — raised by classic `spark.sql()` |
| `SparkRuntimeException` | Runtime error from the Spark engine itself |

`AnalysisException` and `ParseException` work in classic mode — you can and should catch them for DataFrame/SQL error handling. Only `PythonException` is the one that doesn't reliably reach Python for classic RDD worker failures.

> ❓ Revisit: verify whether `pyspark.errors.PythonException` surfaces for DataFrame UDFs (`F.udf`) in classic (non-Connect) mode.

### Suppressing executor log noise

With the default partition count, multiple tasks run in parallel. Each failing task logs its own Java stacktrace at ERROR level — producing the wall of `org.apache.spark.executor.Executor` lines before the exception even reaches Python.

**Two causes, two fixes:**

**1 — Reduce partitions for small test collections:**
```python
sc.parallelize(collection, numSlices=1)   # one task → one error log line
```

**2 — Suppress the chatty loggers.**

Quick fix in the notebook (session-scoped):
```python
spark.sparkContext.setLogLevel("FATAL")   # silences ERROR + WARN from executors
```

Permanent fix in `log4j2.xml` — add two specific loggers that override the parent `org.apache.spark` entry:
```xml
<!-- Task failure noise — only show if the JVM itself is dying -->
<Logger name="org.apache.spark.executor.Executor"       level="FATAL" additivity="false">
    <AppenderRef ref="console"/>
</Logger>
<Logger name="org.apache.spark.scheduler.TaskSetManager" level="FATAL" additivity="false">
    <AppenderRef ref="console"/>
</Logger>
```

Place these **before** the existing `org.apache.spark` logger entry. Log4j2 resolves loggers most-specific-first, so these override the parent without affecting other Spark logs (startup, query plans, etc.).

> Why not just set `org.apache.spark` to `FATAL`? That would also silence useful startup and query-plan messages. Targeting only the two executor/scheduler loggers gives quiet task failures while keeping everything else at `WARN`.

### `filter()` — keep elements that satisfy a predicate

```python
collection_rdd.filter(lambda elem: isinstance(elem, (float, int))).collect()
# [2, 4.0]
```

- `filter(f)` keeps only elements where `f(element)` is **truthy** (Python's truthy rules — not just `True`/`False`).
- Lambda functions are convenient for simple one-use predicates.

!!! info "💡 Lambda vs named function"
    both are valid. A named function is easier to test; a lambda saves lines when the predicate is trivially short. Either is idiomatic.

### `reduce()` — fold the collection to a single value

```python
from operator import add

sc.parallelize([4, 7, 9, 1, 3]).reduce(add)   # 24
```

- `reduce(f)` applies `f(a, b)` pair-wise across all elements until one value remains.
- **The function must be commutative and associative** — Spark reduces per-partition in parallel, then merges the intermediate results on the driver. Functions like `subtract()` will produce wrong results.
- Commutative + associative examples: `add`, `multiply`, `min`, `max`.

**Why `operator.add` and not `lambda x, y: x + y`?**

`operator` is a Python stdlib module that exposes every Python operator as a named C-implemented function. Three reasons it's preferred over a lambda in PySpark:

| Reason | Detail |
|---|---|
| **Speed** | C-implemented — ~2× faster per call than a Python lambda going through bytecode interpretation |
| **Picklability** | Named module-level object — cloudpickle serializes it by reference (`operator.add`). Inline lambdas must have their bytecode serialized, which can fail for complex closures |
| **Readability** | `reduce(add)` reads as intent; `reduce(lambda x, y: x + y)` reads as mechanics |

Both forms work; `operator.add` is the idiomatic choice when passing stdlib operators to higher-order functions.

**Useful `operator` functions for RDD work:**

```python
from operator import add, mul, itemgetter, attrgetter, methodcaller

# arithmetic — always call .reduce() on the RDD, not functools.reduce()
rdd.reduce(add)          # sum
rdd.reduce(mul)          # product

# key access — useful when mapping over RDDs of dicts or named tuples
rdd.map(itemgetter("name"))        # equivalent to: lambda x: x["name"]
rdd.map(attrgetter("value"))       # equivalent to: lambda x: x.value
rdd.map(methodcaller("upper"))     # equivalent to: lambda x: x.upper()
```

### MapReduce lineage

Google's 2004 MapReduce paper directly inspired Hadoop and, later, Spark. The names `map` and `reduce` are the same concepts. Modern Spark abstractions (DataFrames, query plans) sit on top, but understanding the primitives helps reason about distributed execution.

---

## 4. Python UDFs on DataFrames

UDFs (**user-defined functions**) bring the flexibility of arbitrary Python code to the DataFrame API. They bridge the gap between the RDD's openness and the DataFrame's structure.

- A UDF wraps a Python function so it can be applied to a DataFrame **column** (scalar, row-by-row).
- Lives in `pyspark.sql.functions`: `F.udf()`.

### 4.1 Write the Python function first

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

### 4.2 Python ↔ PySpark type mapping

| PySpark type | Python equivalent | String repr (DDL) |
| --- | --- | --- |
| `T.ArrayType(T)` | `list`, `tuple`, or numpy array | `"array<type>"` |
| `T.ByteType()` | `int` (8-bit, −128 to 127) | `"byte"` / `"tinyint"` |
| `T.BinaryType()` | `bytes` (Spark 4.1.1 default); `bytearray` in Spark ≤ 4.0 — changed by `spark.sql.execution.pyspark.binaryAsBytes=true` (default in 4.1.0+) | `"binary"` |
| `T.BooleanType()` | `bool` | `"boolean"` |
| `T.DateType()` | `datetime.date` | `"date"` |
| `T.DayTimeIntervalType()` | `datetime.timedelta` | `"interval day to second"` *(Spark 3.3+)* |
| `T.DecimalType(p, s)` | `decimal.Decimal` | `"decimal"` (defaults: precision=10, scale=0) or `"decimal(10,2)"` to specify explicitly |
| `T.DoubleType()` | `float` (64-bit) | `"double"` |
| `T.FloatType()` | `float` (32-bit) | `"float"` |
| `T.IntegerType()` | `int` (32-bit) | `"int"` |
| `T.LongType()` | `int` (64-bit) | `"long"` / `"bigint"` |
| `T.MapType(K, V)` | `dict` | `"map<K,V>"` |
| `T.NullType()` | `None` | `"void"` |
| `T.ShortType()` | `int` (16-bit, −32,768 to 32,767) | `"short"` / `"smallint"` |
| `T.StringType()` | `str` | `"string"` |
| `T.StructType([…])` | `list` or `tuple` | `"struct<name:type,...>"` |
| `T.TimestampNTZType()` | `datetime.datetime` (no tz) | `"timestamp_ntz"` *(Spark 3.3+)* |
| `T.TimestampType()` | `datetime.datetime` (tz-aware) | `"timestamp"` |
| `T.VariantType()` | N/A for UDFs — use `try_parse_json()` | `"variant"` *(Spark 4.0+)* |
| `T.YearMonthIntervalType()` | `int` (total months) | `"interval year to month"` *(Spark 3.3+)* |

**Why PySpark types look like function calls (`StringType()`, `LongType()`):**

They are class instances, not plain values — for three reasons:

1. **Parameterised types need to carry state.** `DecimalType(10, 2)`, `ArrayType(StringType())`, `MapType(StringType(), LongType())` store configuration (precision, element type, key/value types) as instance attributes. A plain value like `str` can't do this.

2. **Types need methods.** Spark calls methods on each type object internally:

    ```python
    StringType().simpleString()    # → "string"  (DDL)
    StringType().needConversion()  # → False     (skip Python↔JVM conversion step)
    StringType().toInternal(v)     # convert Python value to Spark internal repr
    ```

3. **Type hierarchy enables polymorphism.** Types form a tree (`DataType → AtomicType → NumericType → IntegralType → LongType`). Spark can ask `isinstance(t, IntegralType)` to handle all integer types uniformly.

**Why `StringType()` isn't expensive despite the `()`:** parameterless types use a `DataTypeSingleton` metaclass — `StringType()` always returns the *same* object.

!!! warning "⚠️ Python `int` is unbounded;"
    PySpark's `LongType` is 64-bit. A Python `int` exceeding `pow(2, 63) - 1` will silently overflow or error. Always use the narrowest type that fits.

### 4.3 Two ways to create a UDF

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

!!! warning "⚠️ Spark 4.x — 'Cannot infer the eval type from type hints' warning"

    Spark 4.x added type-hint inference to `F.udf()`: it reads the function's return annotation (`-> Optional[Frac]`) to automatically pick Arrow-optimized execution. `Frac = Tuple[int, int]` is a custom alias it can't map to a Spark eval type, so the inference throws, the warning fires, and it falls back to `SQL_BATCHED_UDF`. The warning is just noise — **the UDF still works** with the explicit return type you provided.

    The fallback is actually the **correct** outcome here. `infer_eval_type_for_udf` only returns pandas or Arrow UDF types (`SQL_SCALAR_PANDAS_UDF`, `SQL_ARROW_SCALAR_UDF`, etc.) — never `SQL_BATCHED_UDF`. If inference had *succeeded*, Spark would have classified the function as a vectorised UDF and called it with a `pandas.Series` or `pyarrow.Array` — which would fail at runtime because the function expects a plain `tuple`.

    | Eval type | How triggered | Function receives |
    |---|---|---|
    | `SQL_BATCHED_UDF` | `useArrow=False` or fallback | one Python value per call |
    | `SQL_ARROW_BATCHED_UDF` | `useArrow=True`, no pandas hints | Arrow array per batch |
    | `SQL_SCALAR_PANDAS_UDF` | inferred from `Series → Series` hints | `pandas.Series` per batch |
    | `SQL_ARROW_SCALAR_UDF` | inferred from `pa.Array → pa.Array` hints | `pyarrow.Array` per batch |

    Fix: pass `useArrow` explicitly. Setting it to anything other than `None` skips inference entirely — no warning:
    ```python
    reduce_fraction = F.udf(py_reduce_fraction, SparkFrac, useArrow=False)
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

!!! warning "⚠️ UDFs are slower than built-in functions"
    Every row crosses the Python–JVM boundary (serialisation + deserialisation). Use built-in `pyspark.sql.functions` whenever possible; reach for a UDF only when no built-in covers your logic. Chapter 9 introduces pandas UDFs which are significantly faster for column-level operations.

### 4.4 Testing UDFs locally before distributing

Because UDF stack traces surface at action time and are noisy, test the underlying Python function with plain assertions first:

```python
assert py_reduce_fraction((3, 6)) == (1, 2)   # ← runs on the driver, instant feedback
```

After promoting, use `.func` to test the UDF's Python layer without Spark:

```python
assert fraction_to_float.func((2, 4)) == 0.5
```

---

## 5. Summary

- The **RDD** distributes arbitrary Python objects; it is element-major, schema-free, and flexible. Use it when the DataFrame's column structure is genuinely a constraint.
- Core RDD operations are higher-order functions: `map(f)` transforms each element, `filter(f)` keeps elements where `f` is truthy, `reduce(f)` folds the collection to one value. `reduce` requires a commutative and associative `f`.
- A **Python UDF** promotes a Python function to a Spark column operator. Create it with `F.udf(fn, return_type)` or the `@F.udf(return_type)` decorator.
- Always write and test the Python function first; annotate input/output types; use `fn.func(...)` to test without Spark overhead.
- UDFs cross the Python–JVM boundary row by row — they are slower than built-ins. Prefer `pyspark.sql.functions`; use pandas UDFs (Chapter 9) when you need custom logic at scale.

---

## 6. References

- PySpark RDD API: https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.RDD.html
- PySpark UDF docs: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.udf.html
- UDF and UDTF user guide: https://spark.apache.org/docs/latest/api/python/user_guide/udfandudtf.html
- RDD Programming Guide: https://spark.apache.org/docs/latest/rdd-programming-guide.html
- Python `typing` module: https://docs.python.org/3/library/typing.html
- Google MapReduce paper: https://research.google/pubs/pub62/
