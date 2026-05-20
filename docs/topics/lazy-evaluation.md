# Lazy Evaluation & Execution Model

> *Cross-chapter synthesis — Rioux (2022), Chapters 1, 2, 3, 7, 8.*
>
> PySpark never runs a computation when you ask it to — it records what you want and executes only when a result is unavoidable. Understanding *when* execution actually happens, and what Spark does with that gap, is the single most important mental model for writing correct and performant PySpark.

---

## Ch 1 — Transformations vs actions

Spark splits all operations into two categories:

| Kind | Effect | Examples |
|---|---|---|
| **Transformation** | Adds a step to the logical plan; nothing runs | `select`, `filter`, `groupBy`, `join`, `withColumn` |
| **Action** | Triggers execution; returns a result | `show`, `collect`, `count`, `write`, `toPandas` |

A chain of transformations builds a **DAG** (directed acyclic graph) of logical steps. Spark executes the DAG only when an action is called — and only then does it touch the data.

**Why laziness?** Three concrete wins:

1. **Optimisation window.** Spark can see the full pipeline before running it. Catalyst can reorder filters, push predicates into the source, and prune columns that are never used.
2. **Fault tolerance.** Because Spark knows the lineage of every dataset, it can recompute a lost partition by replaying the chain — no need to checkpoint everything.
3. **Resource efficiency.** Columns and partitions that disappear before an action are never read at all.

---

## Ch 2 — Traps created by lazy evaluation

Laziness creates two subtle bugs that catch new PySpark users:

**Trap 1 — `getOrCreate()` silently ignores new config.**

```python
# This does NOT change the log level if a session already exists
spark = (
    SparkSession.builder
    .appName("MyApp")
    .config("spark.logLevel", "ERROR")
    .getOrCreate()
)
# Fix: set config after getting the session
spark.sparkContext.setLogLevel("ERROR")
```

`getOrCreate()` returns the existing session if one is alive. Config passed to the builder is ignored. Always use the `spark` handle to mutate live session config.

**Trap 2 — `inferSchema=True` triggers an eager full scan.**

```python
df = spark.read.csv("data/*.csv", inferSchema=True)
# Spark reads the entire file once just to infer types —
# this is an action hidden inside a "read" call.
```

Prefer explicit schemas (`T.StructType` or DDL) for large files; `inferSchema` is fine for interactive exploration on small data.

---

## Ch 3 — The double-pass problem and `cache()`

When two actions share the same transformation chain, Spark executes the whole chain *twice* — once per action — because it never materialises intermediate results automatically.

```python
clean = raw.filter(F.col("status") == "OK").select("id", "value")

count = clean.count()   # chain runs once
total = clean.agg(F.sum("value")).collect()  # chain runs again
```

Fix: call `cache()` (or `persist()`) before the first action to tell Spark to materialise the result after the first pass and reuse it for subsequent actions.

```python
clean = (
    raw.filter(F.col("status") == "OK")
       .select("id", "value")
       .cache()
)
count = clean.count()   # triggers materialisation, stored in memory
total = clean.agg(F.sum("value")).collect()  # reads from cache
```

`cache()` is itself lazy — it only materialises on the *first* action that touches the DataFrame. Call `unpersist()` when you're done to free memory.

---

## Ch 7 — Laziness in the SQL API

The SQL API (and `spark.sql()`) is identically lazy. A `spark.sql("SELECT …")` call returns a DataFrame object — a logical plan — not data. The same transformation/action split applies; `spark.sql("SELECT …").show()` is the action.

This means a CTE defined with `createOrReplaceTempView` is not materialised until a downstream action runs — the view is just a named logical plan pointer.

---

## Ch 8 — Catalyst, Tungsten, and whole-stage code generation

The gap between logical plan and physical execution is where Spark's two main internal engines operate:

**Catalyst** — the query optimiser. Applies rule-based rewrites (predicate pushdown, column pruning, constant folding) and cost-based join reordering to the logical plan before converting it to a physical plan.

**Tungsten** — the memory and execution engine. Uses **UnsafeRow** — a binary, off-heap row format — to avoid Java object overhead and GC pressure.

**Whole-stage code generation (WSCG)** — Catalyst emits a single fused JVM bytecode class per pipeline stage, collapsing the per-row virtual-dispatch cost of the iterator model. A single stage processes rows top-to-bottom in one generated loop rather than calling into each operator separately.

```
Logical plan
    ↓  Catalyst: rule-based rewrites
Optimised logical plan
    ↓  Catalyst: physical planning
Physical plan
    ↓  Tungsten + WSCG: code generation
Compiled bytecode executed on JVM
```

**UDFs break WSCG.** A Python UDF forces Spark to interrupt the fused bytecode loop, serialise the row, send it to a Python worker via the JVM bridge, wait for a result, and deserialise back. Every UDF call is a WSCG boundary. Prefer built-in `pyspark.sql.functions` — they compile into the generated bytecode.

---

## Summary

- Every PySpark operation is lazy until an action (`show`, `collect`, `count`, `write`) triggers execution.
- Laziness is the precondition for Catalyst optimisation and lineage-based fault recovery.
- `getOrCreate()` silently ignores builder config on existing sessions — set config on the live `spark` handle.
- `inferSchema=True` is a hidden eager scan — use explicit schemas in production.
- Multiple actions on the same DataFrame re-execute the full chain; `cache()` prevents this.
- Catalyst optimises, Tungsten executes efficiently, WSCG fuses the pipeline into compiled bytecode.
- Python UDFs break whole-stage code generation — use built-in functions wherever possible.

---

## Chapter links

- [Ch 1 — Intro to PySpark](../books/rioux/chapters/01-intro-to-pyspark.md)
- [Ch 2 — First data program](../books/rioux/chapters/02-first-data-program.md)
- [Ch 3 — DataFrames and datasets](../books/rioux/chapters/03-dataframes.md)
- [Ch 7 — Python, Spark SQL, and interoperability](../books/rioux/chapters/07-python-sql.md)
- [Ch 8 — RDDs and UDFs](../books/rioux/chapters/08-rdd-udfs.md)
