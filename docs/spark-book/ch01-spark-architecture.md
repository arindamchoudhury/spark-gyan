# Chapter 01 — Spark Architecture and the Execution Model

> *Learning-path topic: B1 (Beginner)*
> *Written: 2026-05-31 · Spark 4.1.x / Python 3.10+*

Apache Spark is a distributed analytics engine. Understanding how it distributes work — and why — is the foundation every debugging and tuning decision rests on. Get this mental model right and most Spark surprises become predictable.

---

## What you'll learn

- How a Spark cluster is organised: driver, executors, worker nodes, cluster manager
- Why Python code can be fast despite running in a separate process from the JVM
- What lazy evaluation is and why it exists
- The difference between a transformation and an action
- How Spark 4.x changes the default execution model with Spark Connect

---

## The problem this solves

By 2009, when Spark was created at Berkeley's AMPLab, the dominant large-scale data processing tool was Hadoop MapReduce. MapReduce solved the right problem — distributing computation across many cheap machines — but had two sources of disk I/O that made complex pipelines slow.

First, the shuffle between the Map and Reduce phases: map output is written to local disk on each mapper node, then reducers fetch it from disk over the network. This happens inside every single MapReduce job.

Second, chained jobs: a real analytical pipeline requires multiple MapReduce jobs in sequence (filter → join → aggregate → …). Each job writes its full output to HDFS, and the next job reads it back from HDFS. AWS describes the consequence directly: Hadoop "only does so in batches and with substantial delay."

Iterative algorithms — machine learning trains by making dozens of passes over the same data — pay both costs on every iteration. A logistic regression that needs 50 iterations triggers 50 HDFS writes and 50 HDFS reads of the full dataset.

Spark was built to remove that bottleneck. Rather than writing intermediate results to disk after every step, Spark keeps data in RAM across the full computation and writes to storage only once at the end. The difference in speed — the commonly cited "100× faster for in-memory operations, 10× faster on disk" — comes directly from this single architectural decision.

**How other tools approach the same problem:**

| Tool | Model | Latency | Best for |
|---|---|---|---|
| **Hadoop MapReduce** | Batch, disk-bound between steps | Minutes to hours | Large batch ETL where latency is not a concern |
| **Apache Spark** | Micro-batch and batch, in-memory | Seconds to minutes | General-purpose: batch, ML, SQL, near-real-time |
| **Apache Flink** | True event-at-a-time streaming | Sub-second | Real-time fraud detection, event pattern matching, stateful streams requiring precise ordering |
| **Dask** | Parallel Python (pandas/NumPy) | Depends on hardware | Data science workloads that outgrow a single machine but don't require a full cluster |
| **Ray** | Distributed Python task graph | Low | Distributed ML training, hyperparameter search, model serving |
| **Trino/Presto** | Federated interactive SQL | Sub-second for queries | Querying data in-place across multiple sources (S3, databases, Hive) without ingestion |

Spark is the most general-purpose of these. It runs batch jobs, serves SQL queries, trains ML models, and handles near-real-time streaming — all through one engine and one API. The trade-off is that specialised engines outperform it in their target domain: Flink for sub-second streaming, Trino for interactive federated SQL, Ray for fine-grained ML parallelism.

Sources: [AWS — Hadoop vs Spark](https://aws.amazon.com/compare/the-difference-between-hadoop-vs-spark/), [Flexera — Spark vs Flink](https://www.flexera.com/blog/finops/apache-spark-vs-flink/)

---

## A first Spark program

Before explaining the architecture, here is a complete word count program — the canonical "hello world" of distributed computing. It reads *Pride and Prejudice* from the Gutenberg corpus in the local stack, counts every word, and shows the top 10. All the behaviour described in the rest of this chapter is visible in this program.

```python
# Apache Spark 4.1.x / PySpark 4.1.x · Python 3.14
# Run from workspace/notebooks/ — requires the local stack running (docker compose up)
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
conf_path = os.path.abspath("log4j2.xml")   # silences Spark's INFO noise

spark = (
    SparkSession.builder
    .appName("word-count")
    .config("spark.ui.port", "4041")
    .config("spark.driver.extraJavaOptions",
            f"-Dlog4j2.configurationFile={conf_path}")
    .getOrCreate()
)

# Everything below this line is lazy — no data moves yet
book = spark.read.text("../data/gutenberg_books/1342-0.txt")

top_words = (
    book
    .select(F.explode(F.split("value", " ")).alias("word"))    # split lines into words
    .select(F.lower(F.regexp_extract("word", "[a-z]+", 0))     # lowercase, strip punctuation
             .alias("word"))
    .filter(F.col("word") != "")                               # drop empties
    .groupBy("word")
    .count()
    .orderBy(F.col("count").desc())
)

top_words.show(10)   # <-- THIS is the first action; only now does Spark execute the plan
# +----+-----+
# |word|count|
# +----+-----+
# | the| 4480|
# |  to| 4218|
# |  of| 3711|
# | and| 3504|
# | her| 2199|
# |   a| 1982|
# |  in| 1909|
# | was| 1838|
# |   i| 1749|
# | she| 1668|
# +----+-----+

spark.stop()
```

Every line between `spark.read.text(...)` and `.show(10)` is a **transformation** — an instruction recorded but not executed. `.show(10)` is the first **action** — the moment Spark takes all the recorded instructions, builds an optimised physical plan, distributes the work across executors, and returns a result. The rest of this chapter explains exactly what happens during those few milliseconds.

---

## Core concept

The official Spark documentation describes the cluster architecture with this diagram:

[![Spark cluster overview](assets/ch01/cluster-overview.png)](assets/ch01/cluster-overview.png)

*Source: [Apache Spark — Cluster Mode Overview](https://spark.apache.org/docs/latest/cluster-overview.html)*

The components, using the official definitions:

| Component | Official definition |
|---|---|
| **Driver Program** | The process running the main() function of the application and creating the SparkContext. Coordinates the application; must be network-addressable from worker nodes. |
| **SparkContext** | The object in your main program that connects to the cluster manager and coordinates the distributed computation. In PySpark, `spark = SparkSession.builder.getOrCreate()` creates this for you. |
| **Cluster Manager** | An external service that allocates resources across applications — Spark Standalone, YARN, or Kubernetes. |
| **Worker Node** | Any node in the cluster that can run application code. |
| **Executor** | A process launched on a worker node for one application. Runs tasks and keeps data in memory or on disk. Each application has its own isolated executors; they stay up for the lifetime of the application. |
| **Task** | A unit of work sent to one executor. One task processes one partition. |
| **Cache** | In-memory or disk storage maintained by executors across tasks — populated when you call `.cache()` or `.persist()`. |

When you run a PySpark script, your Python process is the driver. It connects to the cluster manager, which launches executors on worker nodes. The driver sends your application code to those executors, then schedules tasks — one task per data partition — to run on them. Executors report results back to the driver.

Your data is split into **partitions** — chunks distributed across executor memory. Each partition is processed by exactly one task, independently and in parallel with other tasks. This is the source of Spark's scalability: more executors means more tasks can run at the same time.

The JVM-Python boundary matters here. PySpark's DataFrame API generates JVM instructions — so `F.sum()`, `F.join()`, and `F.filter()` all run at full JVM speed regardless of Python. The Python process only sends the plan; the JVM does the heavy lifting. Python UDFs break this model (covered in Chapter 12), but for the DataFrame API the performance gap between Python and Scala is negligible.

**Lazy evaluation** is Spark's defining characteristic. Every transformation you write — `select`, `filter`, `groupBy`, `join` — does nothing immediately. Spark records the instruction and returns instantly. Only an **action** — `show()`, `write()`, `count()` — triggers the actual computation. At that point the driver takes the full instruction list, optimises it into a physical plan, and dispatches work to executors.

This laziness enables three things: Catalyst can reorder and prune operations across the whole chain; intermediate DataFrames never need to be materialised in memory; and failed partitions can be recomputed from source without manual recovery.

A **job** is triggered by one action. Each job is broken into **stages** — groups of operations that can run without shuffling data across the network. Within a stage, each partition becomes a **task**. Understanding this hierarchy (job → stage → task) is what makes the Spark UI readable.

Spark 4.x introduces **Spark Connect** as the default client model: your Python process communicates with the Spark driver over gRPC rather than being embedded in it. The DataFrame API is identical; the change is architectural and improves isolation and IDE tooling. Classic mode (driver inside the Python process) still works via `spark-submit` and `SparkSession.builder.master(...)`.

---

## Examples

### Minimal example: transformations are lazy, actions are not

```python
# Apache Spark 4.1.x / PySpark 4.1.x · Python 3.10+
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ch01-architecture").getOrCreate()

df = spark.range(1_000_000)  # creates a DataFrame of 0..999999 — instantly, no data yet

# These three lines execute in microseconds — no data moves
filtered   = df.filter(F.col("id") % 2 == 0)
with_label = filtered.withColumn("label", F.lit("even"))
limited    = with_label.limit(5)

# THIS triggers the job — only now does Spark plan, optimise, and execute
limited.show()
# +---+-----+
# | id|label|
# +---+-----+
# |  0| even|
# |  2| even|
# |  4| even|
# |  6| even|
# |  8| even|
# +---+-----+
```

### Building up: the plan vs the execution

```python
# Apache Spark 4.1.x / PySpark 4.1.x · Python 3.10+
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ch01-plan").getOrCreate()

data = [("Alice", "eng", 95000), ("Bob", "eng", 87000), ("Carol", "mkt", 72000)]
employees = spark.createDataFrame(data, ["name", "dept", "salary"])

# Chain of transformations — all lazy
result = (
    employees
    .filter(F.col("dept") == "eng")
    .withColumn("bonus", F.col("salary") * 0.1)
    .select("name", "salary", "bonus")
)

# See the plan Catalyst will execute — no data moves
result.explain()
# == Physical Plan ==
# Project [name#0, salary#2, (salary#2 * 0.1) AS bonus#5]
# +- Filter (dept#1 = eng)
#    +- Scan ExistingRDD[name#0,dept#1,salary#2]

# Action — now Spark runs the optimised plan
result.show()
# +-----+------+-------+
# | name|salary|  bonus|
# +-----+------+-------+
# |Alice| 95000| 9500.0|
# |  Bob| 87000| 8700.0|
# +-----+------+-------+
```

---

## Common pitfalls

- **Calling `count()` on a DataFrame you didn't intend to materialise** — `df.count()` as a method call is an action that triggers a full scan. `F.count("col")` inside `groupBy().agg()` is a transformation. They look similar but behave very differently.
- **Expecting `show()` to be free** — every `show()` is a job. In a debugging loop with five `show()` calls, Spark re-executes the entire chain five times (unless the DataFrame is cached).
- **Caching too eagerly** — `df.cache()` is not always faster. If the DataFrame is consumed only once, caching wastes memory and adds overhead. Cache only DataFrames that feed two or more actions.
- **Assuming Python overhead is large for DataFrame operations** — it isn't. Python sends a plan to the JVM; the JVM runs it. The bottleneck is almost never the Python-JVM round trip for standard DataFrame operations.
- **Confusing Spark's logical partitions with Spark Connect's gRPC transport** — in Spark 4.x, the `pyspark` shell defaults to Connect mode and may fail to start if no Connect server is running. For local scripts, use `SparkSession.builder.master("local[*]").getOrCreate()` to force classic mode.

---

## Exercises

1. **Recall** — In a PySpark program that chains five `.filter()` calls followed by one `.show()`, how many jobs does Spark create? How many times does data move?
   *Hint: count the actions.*

2. **Apply** — Create a DataFrame from `spark.range(100)`. Chain three transformations. Call `.explain()` on the result and identify which step Catalyst pushed earliest in the physical plan.

3. **Extend** — Set up a local SparkSession and use `spark.sparkContext.getConf().getAll()` to list all active configuration. Identify which setting controls the number of shuffle partitions and what the default is. Explain why the default might be too high for a laptop.

---

## Summary

- Spark uses a driver-executor model: the driver plans, executors process data in parallel partitions.
- Python DataFrames generate JVM plans — most operations run at JVM speed regardless of the Python API.
- All transformations are lazy: no data moves until an action (`show`, `write`, `count`) triggers a job.
- A job breaks into stages (shuffle boundaries) and tasks (one per partition).
- Spark 4.x defaults to Spark Connect (gRPC-based); use `.master("local[*]")` for classic local mode in scripts.
- Chapter 2 builds on this by showing how to create and configure a `SparkSession`.

---

## References

- [Apache Spark cluster overview](https://spark.apache.org/docs/latest/cluster-overview.html)
- [PySpark 4.1.x documentation](https://spark.apache.org/docs/latest/api/python/)
- [Spark Connect overview](https://spark.apache.org/docs/latest/spark-connect-overview.html)
