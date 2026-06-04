# RDD vs DataFrame

> *Cross-chapter synthesis — Rioux (2022), Chapters 1, 2, 8.*
>
> Spark offers two primary data abstractions: the low-level RDD and the high-level DataFrame. Understanding their relationship, performance characteristics, and when to use each is foundational to writing efficient PySpark.

---

## Ch 1 — High-level contrast

| | **RDD** | **DataFrame** |
|---|---|---|
| **Abstraction** | Distributed bag of arbitrary Python/Scala/Java objects | Distributed table with a named, typed schema |
| **Optimisation** | None — Spark executes what you write | Catalyst-optimised; Tungsten-executed |
| **Type safety** | Runtime only (Python is already dynamic) | Schema enforced at plan time via `AnalysisException` |
| **Language gap** | Python RDDs are ~3–8× slower than Scala/Java RDDs | No significant gap — all languages compile to same JVM bytecode via Catalyst |
| **Use case** | Unstructured data, custom serialisation, algorithms that Catalyst cannot express | Structured/semi-structured data, SQL-style analytics, ML pipelines |

The key insight from Ch 1: the DataFrame API **erases the Python performance penalty**. A PySpark DataFrame job runs at the same speed as its Scala equivalent because both compile to the same Catalyst physical plan and Tungsten bytecode.

---

## Ch 2 — The two structures defined

**RDD (Resilient Distributed Dataset)** — the original Spark abstraction (2012). A read-only, partitioned collection of records distributed across a cluster. Records are arbitrary Python objects. Spark tracks lineage (the sequence of transformations) but knows nothing about the record structure.

```python
sc = spark.sparkContext
rdd = sc.parallelize([1, 2, 3, 4, 5], numSlices=3)
rdd.map(lambda x: x * 2).filter(lambda x: x > 4).collect()
# [6, 8, 10]
```

**DataFrame** — introduced in Spark 1.3. Built on top of RDD but with a schema (a `StructType`). Records are stored in Tungsten's `UnsafeRow` binary format, not as Python objects. Catalyst optimises the logical plan before execution.

```python
df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
df.filter(F.col("id") > 1).show()
```

A DataFrame is internally an RDD of `UnsafeRow` objects — but that RDD is never directly visible to user code.

---

## Ch 8 — Performance deep dive

**UnsafeRow memory layout.** Tungsten stores each DataFrame row as a compact binary byte array (`UnsafeRow`), reducing JVM object overhead and GC pressure. The byte array is **on-heap by default**; off-heap requires `spark.memory.offHeap.enabled=true`. A single-column `IntegerType` DataFrame with 1M rows occupies approximately 16 MB — 8-byte null bitmap + 8-byte field slot per row.

Contrast with a Python RDD of the same 1M integers: each Python `int` is a 28-byte heap object, plus the JVM overhead of the wrapper object. Memory usage is 5–10× higher, and GC pauses can stall execution.

**The JVM bridge overhead for RDDs.** For every RDD operation (map, filter, reduce), PySpark must:

1. Serialise each partition from JVM objects to Python bytes (using cloudpickle or pickle).
2. Send the bytes over a local socket to a Python worker process.
3. Deserialise, apply the Python lambda.
4. Serialise results back and return through the same socket.

This round-trip happens for **every partition** on **every operation**. For a 200-partition RDD with 5 chained operations, that is 1,000 serialise/deserialise cycles per executor.

**DataFrame operations avoid this entirely.** The Python API constructs a logical plan (pure Python objects describing intent), passes it to the JVM once, and the JVM executes the entire plan without any Python involvement during execution.

```
RDD (Python UDF on each partition):
  Python → [pickle] → JVM → [socket] → Python worker → [socket] → JVM
  (repeated per partition per operation)

DataFrame (built-in functions):
  Python builds plan → JVM executes plan in Tungsten bytecode
  (Python is only involved at planning time)
```

**Quantified language gap for RDDs.** Scala/Java RDD operations run 3–8× faster than equivalent PySpark RDD operations for CPU-bound workloads. The gap disappears entirely for DataFrame operations.

---

## When to use RDD

Despite the performance cost, RDDs remain the right tool for:

- **Unstructured data** — free-form text, binary blobs, arbitrary Python objects with no natural schema.
- **Algorithms that Catalyst cannot express** — iterative graph algorithms, custom partitioners, fine-grained control over data locality.
- **Migrating legacy Spark 1.x code** — where rewriting to DataFrames is not immediately feasible.
- **`SparkContext.textFile()`** — still the canonical way to read raw text files line-by-line.

For everything else — structured, semi-structured, or tabular data — prefer DataFrames.

---

## Summary

- RDD: distributed bag of arbitrary objects; no Catalyst, no Tungsten; Python is slow here.
- DataFrame: distributed table with schema; Catalyst-optimised, Tungsten-executed; Python/Scala parity.
- DataFrames are internally RDDs of `UnsafeRow` binary objects — the RDD is an implementation detail.
- `UnsafeRow` off-heap storage: ~4 bytes per int value vs ~28 bytes for a Python `int` RDD object.
- Python RDD operations incur a per-partition JVM↔Python serialisation round-trip; DataFrames do not.
- Use RDDs for unstructured data or algorithms Catalyst cannot express; DataFrames for everything else.

---

## Chapter links

- [Ch 1 — Intro to PySpark](../books/rioux/chapters/01-intro-to-pyspark.md)
- [Ch 2 — First data program](../books/rioux/chapters/02-first-data-program.md)
- [Ch 8 — RDDs and UDFs](../books/rioux/chapters/08-rdd-udfs.md)
