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

Before explaining the architecture, here is a complete word count program — the canonical "hello world" of distributed computing. It reads *Pride and Prejudice* from the Gutenberg corpus in the [local stack](https://github.com/arindamchoudhury/spark-delta-unitycatalog), counts every word, and shows the top 10. All the behaviour described in the rest of this chapter is visible in this program.

The full runnable versions are in the repo:

- **[`workspace/notebooks/intro.ipynb`](https://github.com/arindamchoudhury/spark-delta-unitycatalog/blob/main/workspace/notebooks/intro.ipynb)** — notebook with cells labelled Read / Transform / Action / Inspect the plan
- **[`workspace/pyscript/intro.py`](https://github.com/arindamchoudhury/spark-delta-unitycatalog/blob/main/workspace/pyscript/intro.py)** — standalone script for `spark-submit`

To run `intro.py` with `spark-submit`:

```bash
# Local mode — no cluster required, uses all available CPU cores
spark-submit --master "local[*]" workspace/pyscript/intro.py

# Against the Docker stack — submit inside the spark container
docker compose exec spark spark-submit \
    --master "local[*]" \
    /workspace/pyscript/intro.py
```

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

[![Spark cluster overview](assets/ch01/cluster-overview.png)](assets/ch01/cluster-overview.png)

*Source: [Apache Spark — Cluster Mode Overview](https://spark.apache.org/docs/latest/cluster-overview.html)*

Before walking through the components, there is a foundational choice that shapes how the architecture works: **which execution mode is Spark running in?**

---

### Two execution modes: Classic and Spark Connect

The official Spark Connect overview ([spark.apache.org](https://spark.apache.org/docs/latest/spark-connect-overview.html)) defines two modes:

**Classic mode** — two separate OS processes, both on the same machine, communicating over a local socket via **Py4J** ([steadbytes.com](https://steadbytes.com/blog/pyspark-runtime-architecture/)):

```
Python process        ◄──Py4J local socket──►  JVM process
(your code runs here)                           (Spark engine: Catalyst, scheduler)
```

Data processing logic runs in Python. Data persistence, query planning, and cluster coordination run in the JVM.

**Spark Connect** — introduced in Spark 3.4, default `pyspark` shell in Spark 4.x. The Python client and the Spark engine are fully decoupled and communicate over gRPC — they can be on different machines ([spark.apache.org](https://spark.apache.org/docs/latest/spark-connect-overview.html)):

```
Python process (your code)  ──gRPC (sc://host:15002)──►  Spark Connect Server (JVM, remote)
```

The Python client has no embedded JVM at all.

| | Classic | Spark Connect |
|---|---|---|
| Introduced | Spark 1.0 | Spark 3.4 |
| Client-server | Two OS processes, same machine (Python + JVM) | Decoupled (Python client + remote JVM server) |
| Python↔JVM | Py4J (local socket) | gRPC + Apache Arrow |
| RDD support | Yes | No |
| Direct JVM access (`df._jdf`) | Yes | No |
| Default for `pyspark` shell | Pre-4.x | Spark 4.x |

**How to choose:**

```python
# Classic mode — no remote() call
spark = SparkSession.builder.appName("app").getOrCreate()

# Spark Connect — use remote()
spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()
```

Or with the environment variable:

```bash
export SPARK_REMOTE="sc://localhost"
pyspark   # picks up SPARK_REMOTE automatically
```

The word count program in this chapter uses classic mode. The components described below apply to classic mode. The key difference in Connect mode: the Driver Program (Python process) has no embedded JVM — it sends plans to the Connect server instead.

---

### Driver Program

The official definition ([cluster-overview](https://spark.apache.org/docs/latest/cluster-overview.html)):

> *"The process running the main() function of the application and creating the SparkContext."*

In PySpark classic mode the driver is **not a single process** — it is two processes working together ([source: steadbytes.com](https://dev.to/steadbytes/python-spark-and-the-jvm-an-overview-of-the-pyspark-runtime-architecture-21gg)):

```
┌────────────────────────┐        Py4J (local socket)       ┌───────────────────────────┐
│  Python process         │ ◄──────────────────────────────► │  JVM process               │
│  runs your script       │                                   │  runs Spark's core engine  │
│  (the "driver program") │                                   │  planning, optimisation    │
└────────────────────────┘                                   └───────────────────────────┘
```

- The **Python process** runs your code — `top_words = book.select(...)...` — and is what Spark officially calls the driver program. It holds the logical plan.
- The **JVM process** runs Spark's engine (Catalyst optimiser, scheduler, cluster coordination). The Python process talks to it via **Py4J**, a gateway library that lets Python call JVM methods over a local socket.

In **Spark Connect mode** (default in the `pyspark` shell in Spark 4.x), the JVM is fully remote on the Connect server. The Python client has no embedded JVM at all:

```
┌────────────────────────┐       gRPC (sc://host:15002)      ┌───────────────────────────┐
│  Python process         │ ──────────────────────────────►  │  Spark Connect Server       │
│  your script / notebook │                                   │  JVM, remote               │
└────────────────────────┘                                   └───────────────────────────┘
```

**Where the driver runs** — from the official submitting-applications docs:

| How you run | Deploy mode | Driver location | Verified |
|---|---|---|---|
| `spark-submit --deploy-mode client` | client (default) | Python process on the submitting machine | ✅ official docs |
| `spark-submit --deploy-mode cluster` | cluster | process launched on a worker node | ✅ official docs |
| Jupyter notebook / `pyspark` shell | client | the kernel / shell Python process | inferred from definition — no explicit official statement |

`SparkSession.builder.getOrCreate()` does not create the driver process. It runs inside the already-running Python process and creates a SparkSession (which internally starts the JVM via Py4J, or connects to the remote Spark Connect server).

Every call you make — `spark.read.text(...)`, `.select(...)`, `.filter(...)`, `.groupBy(...)` — executes in the driver process. It records the instructions as a logical plan but moves no data. The driver must be network-addressable from worker nodes because executors send results back to it.

---

### SparkContext and SparkSession

The official definition of SparkContext ([cluster-overview](https://spark.apache.org/docs/latest/cluster-overview.html)):

> *"The SparkContext object in your main program (called the driver program). It coordinates independent sets of processes on a cluster and connects to cluster managers to allocate resources."*

`SparkSession.builder.getOrCreate()` creates a **SparkSession** — the entry point for DataFrame and SQL functionality introduced in Spark 2.0. SparkSession is a higher-level abstraction that encapsulates the SparkContext internally. You can access the underlying SparkContext via `spark.sparkContext`, but for DataFrame operations you interact with SparkSession directly.

In the word count program, once `.show(10)` fires, the SparkContext (accessed through SparkSession) takes the logical plan, optimises it, and hands it to the cluster manager to allocate executors.

---

### Cluster Manager

The **Cluster Manager** is an external service that controls the machines in the cluster. In the local stack (`docker compose up`) it is Spark Standalone, running inside the `spark` container. On cloud deployments it is typically YARN or Kubernetes.

When `.show(10)` triggers the job, the SparkContext asks the cluster manager: "I need executors." The cluster manager decides how many executors to launch, on which machines, and with how much memory — based on what you configured when starting the session.

In the local stack the cluster manager is the same service you connected to at `sc://spark:15002`.

---

### Worker Nodes and Executors

A **Worker Node** is any machine in the cluster that can run application code. The cluster manager launches an **Executor** process on each worker node it allocates to your application.

In the word count program, executors are the processes that actually read `1342-0.txt`, run `split`, `regexp_extract`, `lower`, and `filter` on lines of text, and count words. The driver never touches the file contents directly — it delegates all of that to executors.

Each application gets its own isolated executors. They stay alive for the entire application (from `getOrCreate()` to `spark.stop()`), not just one query.

---

### Partitions and Tasks

`1342-0.txt` is not loaded as a single block. Spark splits it into **partitions** — contiguous chunks of the file, each small enough to fit in executor memory. Each partition is assigned to exactly one **Task**, and each task runs on one executor.

In the word count program:
- Tasks for `split`, `lower`, `filter` can all run independently on each partition in parallel — no executor needs to see another's data. These are called **narrow transformations**.
- `groupBy("word").count()` is different: to count "the" across the whole book, every occurrence of "the" from every partition must land on the same executor. Spark triggers a **shuffle** — data moves across the network, regrouped by word. This is the most expensive step in the program.

After the shuffle, each executor holds all occurrences of a distinct set of words, counts them, and sends the top results back to the driver. The driver then calls `.show(10)`.

---

### The full sequence for `.show(10)`

```
1. driver builds logical plan from all the transformation calls
2. SparkContext optimises the plan and splits it into stages (shuffle boundaries)
3. cluster manager launches executors on worker nodes
4. driver sends application code (the transformations) to executors
5. executors read their partition of 1342-0.txt and run split → lower → filter  (Stage 1)
6. shuffle: data moves across executors, regrouped by word                       (stage boundary)
7. executors count their local word groups                                        (Stage 2)
8. driver receives top 10 results; show() prints them
```

Every step before line 5 is the driver doing planning. Every step from line 5 onward is executors doing work.

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
