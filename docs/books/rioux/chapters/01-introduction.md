# Chapter 1 — Introduction

> *Source: Rioux (2022), Chapter 1, pages 1–15.*
>
> A scene-setting chapter: what PySpark is, why you'd choose it, how Spark distributes work across a cluster, and what you need to get started. Establishes the mental models — the data factory analogy, transformations vs. actions, lazy evaluation — that the rest of the book builds on.
>
> 📌 **Notes adapted to PySpark 4.1.1.** The book targets Spark 3.2 (released October 2021); PySpark 4.1.1 is the current production release as of January 2026. Core concepts (lazy evaluation, the driver/executor model, transformations vs. actions) are unchanged. Version-specific details — Python requirements, Java runtime, install commands, language API status, and behavioral defaults — are updated below.

---

## 1. What is PySpark?

### The one-line definition

- **PySpark = Python API for Apache Spark.** The name breaks down literally: Py (Python) + Spark.
- Spark itself is a **unified analytics engine for large-scale data processing**. Think of it as a data factory: raw data comes in; results (insights, models, aggregations) come out.

### What is Spark?

- Spark was created at Berkeley AMPLab and is now an Apache top-level project, maintained and significantly driven by Databricks.
- The factory analogy: a cluster of computers is the factory building. Each machine is a workbench. Workers (executors) sit at those workbenches and perform the actual labor. A floor manager (the driver) coordinates everything.
- **Scale out, not scale up.** Instead of buying one machine with enormous RAM (expensive, non-linear cost), Spark spreads data across many modest machines. Two 64 GB RAM machines are cheaper than one 128 GB machine. In the cloud this math is even more pronounced.
- **Fault tolerance.** With 100 machines, the probability that at least one fails is high. Spark's design assumes failures and handles them — it doesn't ask you to.
- Key value: Spark exposes a powerful API that makes it *look* like you're working with one coherent data source, while hiding the distributed machinery underneath.

### PySpark = Spark + Python

- Spark is written in **Scala** and runs on the JVM. PySpark bridges Python to this Scala runtime.
- The **DataFrame API** (the main way you'll use PySpark) maps Python calls to efficient Spark operations that run at the same speed regardless of whether you wrote Scala, Java, or Python. The performance gap between Python and Scala almost entirely disappears when using DataFrames.
- Performance differences *do* remain with **RDDs** and pure **Python UDFs**, because those cross the Python-JVM boundary repeatedly (covered in Ch 8).

> 📌 **Spark Connect (4.x).** Spark 4.x makes Spark Connect the primary client-server architecture. Python code runs in a separate process and communicates with the Spark driver over gRPC rather than in-process (classic mode). The `pyspark` REPL defaults to Connect mode — it attempts to connect to a local Spark Connect server on port 15002 at startup. Plain scripts using `SparkSession.builder` use classic mode unless the `SPARK_REMOTE` environment variable is set. Spark 4.0 added `spark.api.mode` (`"connect"` / `"classic"`) to switch between them. Spark Connect improves isolation and IDE support but is transparent for DataFrame API usage (RDD and SparkContext APIs are not supported in Connect mode).

### pyspark.pandas (formerly Koalas)

- **pandas** is the standard in-memory DataFrame library in Python, but it requires the whole data set to fit in one machine's RAM.
- **pyspark.pandas** (integrated into Spark as of 3.2.0, previously the standalone Koalas project) gives you a pandas-compatible API that executes on Spark — same syntax, distributed scale.
- Rioux recommends using native PySpark syntax for new code and reserving `pyspark.pandas` for migrating existing pandas programs to Spark.

```python
# Native PySpark — preferred for new code
from pyspark.sql import functions as F
df.groupBy("category").agg(F.count("*"))

# pyspark.pandas — when porting existing pandas code
import pyspark.pandas as ps
psdf = ps.from_pandas(pd_df)
psdf.groupby("category").size()
```

### Why PySpark?

Three headline advantages:

| Advantage | What it means |
| --- | --- |
| **Fast** | Up to 100× faster than plain Hadoop MapReduce thanks to in-memory processing and an aggressive lazy query optimizer |
| **Expressive** | Fluent, SQL-inspired API; each operation returns a new DataFrame so you can chain naturally |
| **Versatile** | Runs on all three major clouds, locally, and on-premise; APIs in Python, Scala, Java, and SQL |

#### PySpark is fast

- Hadoop's MapReduce (Google, 2004) wrote intermediate results to disk after each step. Spark keeps data in memory across steps, slashing I/O.
- Spark's lazy query optimizer can reorder, fuse, and prune operations before any data moves.

#### PySpark is expressive

A complete ETL pipeline in a few lines:

```python
(
    spark.read.csv("./data/sample.csv", header=True)
    .withColumn("new_col", F.when(F.col("old_col") > 10, 10).otherwise(0))
    .where("old_col > 8")
    .groupBy("new_col")
    .count()
    .write.csv("output.csv", mode="overwrite")
)
```

Each method returns a DataFrame; the chain reads almost like a sentence.

#### PySpark is versatile

- Available on AWS (EMR), GCP (Dataproc), Azure (HDInsight / Synapse), and Databricks.
- Open source (Apache license) — no vendor lock-in on the core engine.
- Ecosystem: Python, Scala, Java, SQL. SparkR (R API) existed but is **deprecated as of Spark 4.x** and should not be used for new projects.

#### Where PySpark falls short

- **Small data**: coordination overhead across nodes makes PySpark slower than a single-machine library for data that fits in RAM. A PySpark shell takes a few seconds just to start.
- **Python UDFs and RDDs**: Python code that crosses the JVM boundary is slower than the equivalent Scala. The DataFrame API eliminates most of this; RDDs/UDFs do not.
- **Cluster operations**: running and tuning a production cluster is complex. Cloud managed services (Databricks, EMR, Dataproc) have greatly reduced this burden.

---

## 2. How PySpark works — the data factory

### Cluster anatomy

| Role | Factory analogy | Spark term |
| --- | --- | --- |
| Building | Factory | Cluster |
| Workbench | Machine in the cluster | Worker node |
| Employee | Person doing work | Executor |
| Floor manager | Receives your instructions, assigns work | Driver |
| Factory owner | Allocates floor space and headcount | Cluster manager |

> 📌 **"Master" terminology.** The book uses "master" to mean the resource allocator. Spark is actively retiring this term (SPARK-32333). In 4.x docs, the two separate roles are clearer: the **driver** orchestrates a specific job; the **cluster manager** (Standalone, YARN, Mesos, Kubernetes) allocates resources. "Master" now mainly appears in legacy config keys.

### The cluster manager's job (section 1.2.1)

When you submit a program (the **driver program**), the cluster manager:

1. Looks at available machines.
2. Allocates the requested resources (cores, memory).
3. Launches the required number of **executors** across those machines.

Capacity configuration lives in a **SparkContext** (or, in practice, the `SparkSession` that wraps it). Default capacity is whatever the Spark installation specifies.

**Distributing a computation — worked example:**

Computing the average of a column across 12 rows with 4 executors:

- Each executor gets ~3 rows, independently computes `(sum, count)` for its chunk.
- Results: `(9,3)`, `(19,3)`, `(31,3)`, `(13,3)` — small intermediate data, not the raw rows.
- One executor aggregates: total sum = 72, total count = 12 → average = 6.
- Only the tiny intermediate results travel across the network, not the full data set.

> 💡 **Tip** — In the cloud, many vendors (Databricks, EMR, Dataproc) offer auto-scaling: the cluster grows and shrinks during a job based on load. Fixed-size clusters require upfront capacity planning.

### Lazy evaluation (section 1.2.2)

This is Spark's most distinctive — and most misunderstood — feature.

#### Transformations vs. actions

Every Spark operation is one of two things:

| Type | What it is | Examples |
| --- | --- | --- |
| **Transformation** | Describes a computation; records the intent; does no actual work | `select()`, `filter()` / `where()`, `groupBy()`, `withColumn()`, `join()`, `model.transform()` |
| **Action** | Triggers the actual computation; produces a visible result | `show()`, `write()`, `count()` on a DataFrame, `collect()`, `estimator.fit()` (ML model training) |

> ⚠️ **Pitfall** — `count()` has dual identity: as an aggregation *function* inside `groupBy().agg(F.count("*"))` it is a transformation; as a method called on a DataFrame (`df.count()`) it is an action that triggers full computation.

#### How laziness works in practice

- You can chain many transformations; PySpark returns almost instantly after each one because no data is moving.
- Only when an action is reached does the **driver** take the full instruction list, optimize it, and send work to executors.
- Benefits:
  - **Memory**: no intermediate DataFrames need to be materialized.
  - **Optimization**: the driver can reorder, eliminate, and fuse operations knowing the full plan.
  - **Fault tolerance**: since instructions are stored, a failed node can be told to re-execute its slice from the original source — no manual recovery needed.
  - **Iterative development**: build your transformation chain interactively; fire an action only when you're ready.

```
spark.read.csv   ← transformation (even reading is lazy in Spark)
  .withColumn    ← transformation
  .where         ← transformation
  .groupBy       ← transformation
  .count         ← transformation (aggregation context)
  .write.csv     ← ACTION — triggers everything above
```

> 💡 **Tip** — Spark does not cache results automatically. If you trigger the same action twice, Spark re-executes the full chain twice. Use `.cache()` / `.persist()` to store a hot DataFrame across multiple actions (but see Ch 11 before caching eagerly — it's often not worth it).

#### Actors in a running job

1. **Driver** — hosts your Python code. Receives your instructions, builds the logical plan, optimizes it into a physical plan, and slices work for executors.
2. **Executor** — a JVM process on a worker node. Performs the actual data operations. Sends intermediate results back to the driver (or a designated worker) when a shuffle is needed.
3. **Worker node** — the physical/virtual machine. One worker can host multiple executors.

---

## 3. What will you learn in this book?

The book covers:

- Reading and writing data from various sources and formats (CSV, JSON, Parquet, …).
- Data manipulation: filtering, joining, grouping, aggregating.
- Exploratory data analysis on new datasets.
- Building automated data pipelines.
- Troubleshooting common PySpark errors.

And deeper topics:

- Machine learning models (simple experiments → robust ML pipelines, Ch 12–14).
- Multiple data formats: text, tabular, JSON (Ch 4–7).
- Blending Python, pandas, and PySpark code (Ch 8–9).

---

## 4. What do I need to get started?

### Versions

> 📌 **Version update.** The book targets Spark 3.2. The current stable release is **PySpark 4.1.1** (January 9, 2026). Install with:

```bash
pip install pyspark==4.1.1
```

PySpark 4.1.1 requires **Python ≥ 3.10** (Python 3.8/3.9 are no longer supported). Recommended: Python 3.12 or 3.13.

**Java runtime:** Spark 4.x requires **Java 17** (LTS). Java 8 and 11 support was dropped in Spark 4.0. Verify:

```bash
java -version   # should report 17.x or newer
```

### Python prerequisites

The book assumes basic Python. Appendix C covers:

- List comprehensions
- `*args` / `**kwargs` packing and unpacking
- Python typing / mypy
- Closures and the `transform()` method
- Decorators

### Setup options

- **Local install**: `pip install pyspark` + a JDK 17 install. Appendix B walks through Windows, macOS, and Linux/WSL.
- **Cloud**: all three major clouds have managed Spark; Databricks offers the tightest Spark integration (they maintain the project).
- **Jupyter**: all examples work in Jupyter; see Appendix B.

### Tooling

- Any Python-aware editor works (VS Code, PyCharm, Vim/Emacs).
- Book's code on GitHub: <http://mng.bz/6ZOR>

> 💭 (mine): Drawing a diagram before coding a new PySpark pipeline pays off quickly — the factory/floor model makes it natural to sketch which transformations happen on each partition and where shuffles (data movement) will occur.

---

## 5. Summary

- **PySpark** is the Python API for Apache Spark — a distributed analytics engine that scales out across many machines instead of requiring one giant machine.
- **Speed**: Spark keeps intermediate data in RAM and uses lazy evaluation to optimize the full instruction chain before running anything. This is how it achieves up to 100× speedup vs. Hadoop MapReduce.
- **Versatility**: Python, Scala, Java, and SQL APIs; available on all major clouds and locally. SparkR is deprecated in Spark 4.x.
- **The execution model**: your code defines a *driver program*. A *cluster manager* allocates resources. The driver translates your code into a plan, dispatches work to *executors* on *worker nodes*, and collects results.
- **Transformations vs. actions**: all Spark operations are either a transformation (lazy, no data moves) or an action (eager, triggers the full computation chain). Recognizing which is which is foundational to reasoning about performance and behavior.

---

## 6. References

- Apache Spark project — <https://spark.apache.org/>
- PySpark 4.1.1 release — <https://spark.apache.org/releases/spark-release-4-1-0.html>
- Spark 4.0 migration guide — <https://spark.apache.org/docs/latest/migration-guide.html>
- SPARK-32333 (retire "master" terminology) — <https://issues.apache.org/jira/browse/SPARK-32333>
- Book source code — <http://mng.bz/6ZOR>
