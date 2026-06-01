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
- How to install and run Spark, and what `--master` and `--deploy-mode` mean

---

## Why Spark exists

The motivation for Spark comes directly from Matei Zaharia's 2010 paper *"Spark: Cluster Computing with Working Sets"* (UC Berkeley). Understanding that motivation explains every major design decision in Spark.

### MapReduce's constraint: acyclic data flows

By 2010, Hadoop MapReduce was the dominant large-scale data processing framework. It solved distribution, fault tolerance, and load balancing on commodity clusters — but it enforced a strict constraint: all computation had to be expressed as **acyclic data flow graphs**. Each MapReduce job reads from disk, applies map and reduce functions, and writes results back to HDFS. There is no way to carry data in memory from one job to the next.

This constraint is fine for single-pass batch jobs. It breaks down for two classes of applications that Hadoop users were struggling with:

**1. Iterative machine learning.** Training algorithms like logistic regression or k-means apply a function to the same dataset repeatedly — often dozens or hundreds of times — updating a parameter vector on each pass. With MapReduce, every iteration is a separate job. Every iteration reloads the full dataset from HDFS. A logistic regression that needs 50 iterations triggers 50 full HDFS reads of the training data.

Zaharia measured this directly: on a 29 GB dataset on 20 EC2 nodes, Hadoop took **127 seconds per iteration**. After the first iteration, Spark took **6 seconds** — because it kept the dataset in memory. The job ran 10× faster overall.

**2. Interactive analytics.** Hadoop is often used to run ad-hoc exploratory queries over large datasets, via SQL interfaces like Hive or Pig. Ideally, a user loads a dataset once and queries it repeatedly. With MapReduce, every query is a separate job reading from disk, incurring tens of seconds of latency per query.

Zaharia demonstrated this too: a 39 GB Wikipedia dump queried interactively — first query took 35 seconds (comparable to a Hadoop job), subsequent queries took **0.5–1 second**, because the dataset was cached in memory across machines.

### The insight: working sets

Both problems have the same root cause. MapReduce cannot express computations that **reuse a working set of data across multiple parallel operations**. The acyclic data flow model forces everything through disk.

Spark's solution was a new abstraction: the **Resilient Distributed Dataset (RDD)** — a read-only, partitioned collection of objects that can be cached in memory across operations. Users can explicitly cache an RDD after the first computation and reuse it in subsequent operations without re-reading from disk.

Fault tolerance comes not from replication but from **lineage**: each RDD knows how it was derived from its parent. If a partition is lost, Spark recomputes only that partition from the original source — without rolling back the entire job to a checkpoint.

### Why this matters for the DataFrame API

RDDs were Spark's original API. The DataFrame API (Spark 1.3+) is built on top of RDDs, adding a schema and a query optimiser (Catalyst). When you write `df.filter(...).groupBy(...).count()`, Spark builds an RDD lineage graph underneath. The in-memory caching and lineage-based fault tolerance from the 2010 paper are still the foundation.

### Spark version milestones

| Version | Date | Key addition |
|---|---|---|
| Research paper | 2010 | RDDs, lineage-based fault tolerance, in-memory caching (Zaharia et al., UC Berkeley) |
| Open sourced | 2010 | Public release; Scala API only |
| Apache incubator | 2013 | Moved to Apache Software Foundation |
| **1.0** | May 2014 | First stable release; Spark SQL; Java + Python APIs |
| **1.3** | Mar 2015 | **DataFrame API** — schema + Catalyst optimiser on top of RDDs |
| **1.6** | Jan 2016 | **Dataset API** — typed DataFrames (Scala/Java) |
| **2.0** | Jul 2016 | **SparkSession** replaces SQLContext/HiveContext; **Structured Streaming** replaces DStreams; Dataset API recommended |
| **2.1** | Dec 2016 | `pip install pyspark` — JARs bundled in wheel ([PR #15659](https://github.com/apache/spark/pull/15659)) |
| **2.2** | Jul 2017 | Structured Streaming **GA**; pip officially announced |
| **2.4** | Nov 2018 | **pandas UDFs** with Apache Arrow (vectorised UDFs); LTS release |
| **3.0** | Jun 2020 | Python 2 dropped; **type-hint pandas UDFs**; Adaptive Query Execution (AQE); ANSI mode opt-in |
| **3.2** | Oct 2021 | **pandas API on Spark** (Koalas merged); AQE on by default |
| **3.4** | Apr 2023 | **Spark Connect** — decoupled gRPC client-server architecture |
| **3.5** | Sep 2023 | Spark Connect GA (Scala + Go clients); LTS release |
| **4.0** | May 2025 | **ANSI mode on by default**; `pyspark-client` (Connect-only, no JVM); `spark.api.mode`; Python 3.10+ / JDK 17 or 21 required |
| **4.1** | Dec 2025 | **Spark Declarative Pipelines**; `spark-submit` improvements; current stable line |

The chapters in this book map to the modern API surface (Spark 4.1.x). RDDs appear only in Chapter 13; everything else uses the DataFrame/SparkSession API that arrived in 1.3–2.0.

**How other tools approach the same problem:**

| Tool | Model | Latency | Best for |
|---|---|---|---|
| **Hadoop MapReduce** | Batch, acyclic, disk-bound | Minutes to hours | Large single-pass batch ETL |
| **Apache Spark** | In-memory, iterative + batch + streaming | Seconds to minutes | General-purpose: batch, ML, SQL, near-real-time |
| **Apache Flink** | True event-at-a-time streaming | Sub-second | Real-time stateful streams requiring precise ordering |
| **Dask** | Parallel Python (pandas/NumPy) | Depends on hardware | Data science workloads outgrowing a single machine |
| **Ray** | Distributed Python task graph | Low | Distributed ML training, hyperparameter search |
| **Trino/Presto** | Federated interactive SQL | Sub-second for queries | Querying data in-place across multiple sources without ingestion |

Spark is the most general-purpose of these. The trade-off is that specialised engines outperform it in their target domain: Flink for sub-second streaming, Trino for interactive federated SQL, Ray for fine-grained ML parallelism.

Sources: [Zaharia et al. — Spark: Cluster Computing with Working Sets (2010)](https://www.usenix.org/legacy/event/hotcloud10/tech/full_papers/Zaharia.pdf), [Apache Spark history](https://spark.apache.org/history.html), [AWS — Hadoop vs Spark](https://aws.amazon.com/compare/the-difference-between-hadoop-vs-spark/)

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

A Spark application has three kinds of processes: a **driver**, one or more **executors**, and a **cluster manager** that brokers between them. The driver plans the work; executors do the work; the cluster manager decides where executors run.

---

### Driver Program

The official definition ([cluster-overview](https://spark.apache.org/docs/latest/cluster-overview.html)):

> *"The process running the main() function of the application and creating the SparkContext."*

In PySpark **classic mode** the driver *side* is two OS processes working together ([source: steadbytes.com](https://dev.to/steadbytes/python-spark-and-the-jvm-an-overview-of-the-pyspark-runtime-architecture-21gg)):

```mermaid
flowchart LR
    P["Python process\n(driver program —\nyour application code)"]
    J["JVM process\n(Spark engine — SparkContext,\nCatalyst, DAGScheduler)"]
    P <-->|"Py4J (local socket)"| J
```

- The **Python process** is the *driver program* — it runs your application code and builds a logical plan from your DataFrame calls. This is what Spark's official docs mean by "the process running the main() function."
- The **JVM process** is the *Spark engine* — it hosts SparkContext, the Catalyst query optimiser, DAGScheduler, and TaskScheduler. It is a separate OS process from Python, running on the same machine. Executors (covered below) are different processes again, running on worker nodes — they receive tasks but do no planning or scheduling.

The two processes together constitute what Spark calls "the driver." Neither alone is the full picture.

In **Spark Connect mode** (opt-in; activate with `export SPARK_REMOTE="sc://localhost"` before launching `pyspark`), the Python process is a **client only** — it serialises your DataFrame operations as protobuf and sends them over gRPC. It has no JVM at all. The Spark engine runs on the Connect server:

```mermaid
flowchart LR
    P["Python process\n(client — your application code,\nNOT the driver)"]
    S["Spark Connect Server\n(the driver — SparkContext,\nCatalyst, DAGScheduler)"]
    P <-->|"gRPC (sc://host:15002)"| S
```

| | Classic | Spark Connect |
|---|---|---|
| Introduced | Spark 1.0 | Spark 3.4 |
| Python process role | Driver program (user code) | Client only — serialises plans, receives results |
| Spark engine (SparkContext, Catalyst, DAGScheduler) | Driver-side JVM process (co-located with Python) | Connect Server JVM (remote) |
| Python↔JVM transport | Py4J local socket | gRPC + Apache Arrow |
| RDD support | Yes | No |
| Direct JVM access (`df._jdf`) | Yes | No |
| Default for `pyspark` shell | Yes — in all Spark 4.x | No — opt-in via `SPARK_REMOTE`, `--remote`, or `spark.api.mode=connect` |

**How to activate each mode:**

```python
# Classic mode (default) — Python process is the driver program
spark = SparkSession.builder.appName("app").getOrCreate()

# Spark Connect via .remote() — Python is a client, connects to an existing server
spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()
```

**`.remote(url)`** takes a Spark Connect URL of the form `sc://host:port`. It tells the Python process to skip the JVM entirely and connect to a running Connect server at that address. `--master` and `--deploy-mode` are not used — they are meaningless to a client that has no cluster manager and no driver to place.

There are three distinct ways to run in Connect mode, each with a different relationship to `--master`:

| How | `--master` | `--deploy-mode` | What happens |
|---|---|---|---|
| `.remote("sc://host:port")` | Not used | Not used | Client connects to an already-running Connect server |
| `spark.remote = "local[4]"` | Not used | Not used | Spark starts a local Connect server inline (testing only) |
| `spark.api.mode=connect` + `--master yarn` | Used | Used | Spark submits via classic cluster manager but runs in Connect mode |

The first two paths decouple the client completely from cluster management. The third path (`spark.api.mode=connect`) is the bridge for production clusters: you keep the familiar `--master`/`--deploy-mode` mechanics but gain Connect's client-server isolation. The official docs note that `spark.remote` is **limited to `local[*]`** values — for real cluster URLs (`yarn`, `spark://`, `k8s://`) you must use `spark.api.mode=connect`.

```bash
# Connect mode on a real YARN cluster
spark-submit --master yarn --deploy-mode cluster \
  --conf spark.api.mode=connect \
  myapp.py
```

The word count program uses classic mode. The components described below apply to classic mode; in Connect mode they all live on the Connect server.

---

### SparkSession and SparkContext

**SparkSession** is the entry point you create in every PySpark program (introduced in Spark 2.0):

```python
spark = SparkSession.builder.appName("my-app").getOrCreate()
```

It is the single object through which you read data, run SQL, and build DataFrames. You rarely need anything else.

**SparkContext** is the internal component that SparkSession wraps. It is what the official architecture docs refer to when they define the driver:

> *"The SparkContext object in your main program. It coordinates independent sets of processes on a cluster and connects to cluster managers to allocate resources."* — [cluster-overview](https://spark.apache.org/docs/latest/cluster-overview.html)

You don't create or interact with SparkContext directly in normal work — SparkSession creates and owns it. It surfaces in architecture discussions because it is the actual coordinator: when `.show(10)` triggers a job, it is SparkContext that hands the optimised plan to the cluster manager to allocate executors. You can reach it via `spark.sparkContext` if you need low-level RDD operations or configuration inspection, but for all DataFrame and SQL work SparkSession is sufficient.

---

### Cluster Manager

The **Cluster Manager** is an external service that controls the machines in the cluster. In the local stack (`docker compose up`) it is Spark Standalone, running inside the `spark` container. On cloud deployments it is typically YARN or Kubernetes.

When `.show(10)` triggers the job, the SparkContext asks the cluster manager: "I need executors." The cluster manager decides how many to launch, on which machines, and with how much memory — based on what you configured when starting the session.

---

### Worker Nodes and Executors

A **Worker Node** is any machine in the cluster that can run application code. The cluster manager launches an **Executor** process on each worker node it allocates to your application.

In the word count program, executors are the processes that actually read `1342-0.txt`, run `split`, `regexp_extract`, `lower`, and `filter` on lines of text, and count words. The driver never touches the file contents directly — it delegates all of that to executors.

Each application gets its own isolated executors. They stay alive for the entire application (from `getOrCreate()` to `spark.stop()`), not just one query.

---

### Partitions and Tasks

`1342-0.txt` is not loaded as a single block. Spark splits it into **partitions** — subdivisions of the dataset, each processed by exactly one task on one executor. During execution a partition lives in executor memory; if it exceeds available memory Spark spills it to disk. Each partition is assigned to exactly one **Task**, and each task runs on one executor.

In the word count program:

**Narrow transformations** — `split`, `lower`, `filter` each run independently on each partition. No executor needs to see another's data; all partitions process in parallel.

**Wide transformation (shuffle)** — `groupBy("word").count()` requires every occurrence of "the" from every partition to land on the same executor. Spark triggers a **shuffle**: data moves across the network, regrouped by key. This is the most expensive step in the program.

After the shuffle, each executor holds all occurrences of a distinct set of words, counts them, and sends the top results back to the driver.

---

### Lazy evaluation

Every transformation you write — `select`, `filter`, `groupBy`, `join` — does nothing immediately. Spark records the instruction and returns instantly. Only an **action** — `show()`, `write()`, `count()` — triggers actual computation. At that point the driver takes the full instruction list, optimises it into a physical plan, and dispatches work to executors.

Laziness enables three things: Catalyst can reorder and prune operations across the whole chain; intermediate DataFrames never need to be materialised in memory; failed partitions can be recomputed from source without manual recovery.

A **job** is triggered by one action. Each job is broken into **stages** — groups of operations that can run without shuffling data across the network. Within a stage, each partition becomes a **task**. Understanding this hierarchy (job → stage → task) is what makes the Spark UI readable.

---

### Fault tolerance: lineage, not replication

Hadoop HDFS achieves durability through **replication** — every data block is copied to 3 nodes by default. If a node fails, another replica serves the data immediately with no recomputation.

Spark takes a fundamentally different approach: **lineage**. Every RDD and DataFrame records the full chain of transformations that produced it — from the original source through every `filter`, `join`, and `groupBy`. If a partition is lost (executor crash, node failure), Spark does not need a backup copy. It replays the lineage for that partition from the source and recomputes only what was lost.

```mermaid
flowchart LR
    SRC["Source\n(HDFS / S3 — durable)"]
    T1["filter"]
    T2["groupBy"]
    T3["Partition 3\n(lost)"]
    T3R["Partition 3\n(recomputed)"]

    SRC --> T1 --> T2 --> T3
    SRC -.->|"lineage replay\n(only partition 3)"| T3R
```

**The key principle:** Spark delegates *storage durability* to the underlying filesystem (HDFS, S3, GCS). It never tries to own that problem. Spark only manages *compute-level* fault tolerance — rerunning tasks, not replicating bytes.

Three mechanisms cover different failure scenarios:

| Failure | Mechanism | What happens |
|---|---|---|
| Partition lost in executor memory | **Lineage recomputation** | Spark replays the transformation chain from source for that partition only |
| Executor that wrote shuffle output dies | **ShuffleMapStage resubmission** | DAGScheduler resubmits the entire stage that produced the lost shuffle files |
| Lineage is very long (e.g. 100 ML iterations) | **Checkpointing** | User explicitly saves the RDD/DataFrame to HDFS, cutting the lineage; recovery reads the checkpoint instead of replaying 100 steps |

The trade-off versus HDFS replication: recovery requires CPU time (recomputation) rather than just reading a replica. For very long lineage chains this can be slow — which is when checkpointing pays for itself.

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

---

## How Spark runs an application: from action to result

The eight-step sequence above describes *what* happens. This section explains *how* — the internal components that manage the process and the decisions each one makes.

### The components involved

Four internal components coordinate every Spark job:

**DAGScheduler** — lives in the driver JVM. Its job is to construct a **DAG of stages** for each job — a directed acyclic graph where each node is a stage and each edge is a dependency (a stage cannot start until all its parent stages have completed and written their shuffle output). To build this DAG, the DAGScheduler walks the RDD lineage, identifies wide dependencies (shuffles), and groups all narrow transformations between two shuffles into a single stage. It does not think about machines or threads — it only thinks about the logical structure of the computation. When you use the DataFrame API, you never write RDDs yourself — but Catalyst (Spark's query optimiser) compiles your DataFrame logical plan into a physical plan represented as RDD operations before handing it to the DAGScheduler. The DAGScheduler always works at the RDD level; DataFrames are the user-facing abstraction above it.

**TaskScheduler** — lives in the driver JVM. Receives stages (as TaskSets) from the DAGScheduler and converts them into individual tasks assigned to specific executor slots. It knows nothing about DAGs; it only knows about available CPU slots and data locality.

**SchedulerBackend** — the bridge between the TaskScheduler and the cluster manager. It handles executor registration, resource offers, and task launch RPCs. There is a different SchedulerBackend implementation for each cluster manager (StandaloneSchedulerBackend, YarnSchedulerBackend, KubernetesClusterSchedulerBackend).

**BlockManager** — lives in every executor (and a smaller one in the driver). It manages all data storage: cached partitions, shuffle write files, and broadcast variables. When an executor writes shuffle output, it goes through the BlockManager.

---

### Stage 1: action triggers a job — and DataFrame becomes RDD

When `.show(10)` is called, the Python process sends the unresolved logical plan across Py4J to the driver JVM. Before `SparkContext.runJob` is called, **`QueryExecution`** — Spark SQL's execution pipeline — compiles the DataFrame plan through the following phases entirely inside the driver JVM:

```mermaid
flowchart TD
    A["Unresolved Logical Plan\n(DataFrame calls as written by the user)"]
    B["Analyzed Logical Plan\n(column names and types resolved against catalog)"]
    C["Analyzed Logical Plan — cache-aware\n(cached subtrees replaced)"]
    D["Optimized Logical Plan\n(predicate pushdown, column pruning,\nconstant folding, join reordering)"]
    E["Physical Plan — sparkPlan\n(candidates generated, best selected via cost model)"]
    F["executedPlan\n(13 preparation rules applied in order:\nCoalesceBucketsInJoin, PlanDynamicPruningFilters,\nPlanSubqueries, RemoveRedundantProjects,\nEnsureRequirements, InsertSortForLimitAndOffset,\nReplaceHashWithSortAgg, RemoveRedundantSorts,\nRemoveRedundantWindowGroupLimits,\nDisableUnnecessaryBucketedScan,\nApplyColumnarRulesAndInsertTransitions,\nCollapseCodegenStages → Tungsten codegen,\nReuseExchangeAndSubquery)"]
    G["SQLExecutionRDD wrapping RDD[InternalRow]\n◀ boundary: DataFrame world ends, RDD world begins"]
    H["SparkContext.runJob(RDD[InternalRow])"]
    I["DAGScheduler.handleJobSubmitted()"]

    A -->|"Analyzer"| B
    B -->|"withCachedData"| C
    C -->|"Catalyst Optimizer"| D
    D -->|"SparkPlanner"| E
    E -->|"prepareForExecution"| F
    F -->|"QueryExecution.toRdd\nreturns SQLExecutionRDD(executedPlan.execute())"| G
    G --> H --> I
```

`QueryExecution.toRdd` is the boundary between Spark SQL and Spark Core. In Spark 4.1.2 it returns `SQLExecutionRDD(executedPlan.execute(), conf)` — a thin wrapper around `RDD[InternalRow]` that carries SQL execution metadata. Only after this step does `SparkContext.runJob` get called.

Spark 4.x added internal phases (`commandExecuted`, `tableVersionsRefreshed`, `normalized`) to `QueryExecution` for the new SQL scripting and Declarative Pipelines features. For standard DataFrame queries these phases pass through unchanged — the six-phase pipeline above is what matters for DataFrame execution.

At this point no data has moved. The DAGScheduler receives the compiled `RDD[InternalRow]` — every transformation the user wrote, from `spark.read.text(...)` to `.orderBy(...)`, now expressed as RDD operations.

---

### Stage 2: DAGScheduler builds the stage DAG

The DAGScheduler walks the RDD lineage backwards from the final operation, identifying two types of dependency:

- **Narrow dependency** — each partition of the child depends on at most one partition of the parent (e.g. `filter`, `select`, `map`). These can be pipelined: one executor processes the full chain on its partition without any data movement. All consecutive narrow transformations are collapsed into a single stage.
- **Wide dependency** — each partition of the child depends on multiple partitions of the parent (e.g. `groupBy`, `join`, `repartition`). This requires a shuffle: data must move across executors before the next operation can proceed. Wide dependencies become **stage boundaries**.

The result is a DAG of stages: each node is a stage, each edge is a shuffle dependency. A stage cannot start until all its parent stages have completed and written their shuffle output to disk.

There are two types of stage:

- **ShuffleMapStage** — a stage whose output is written to shuffle files on disk, to be consumed by the next stage. Tasks in a ShuffleMapStage write partitioned output; they do not return results to the driver.
- **ResultStage** — the final stage in a job. Its tasks produce the output that goes back to the driver (the rows that `.show(10)` prints).

For the word count program:

```mermaid
flowchart LR
    subgraph S0["ShuffleMapStage 0"]
        A["read"] --> B["split"] --> C["lower"] --> D["filter"]
    end
    D -->|"shuffle\ngroupBy(word)"| S1
    subgraph S1["ResultStage 1"]
        E["count"] --> F["orderBy"] --> G["show"]
    end
```

`groupBy("word")` is a wide dependency — every partition must send its words to the executor responsible for that word. That is the shuffle boundary. Everything before it is Stage 0; everything after is Stage 1.

The DAGScheduler does not schedule all stages at once. It schedules Stage 0 first, waits for it to complete, then schedules Stage 1. If a new shuffle boundary is discovered mid-execution (e.g. with AQE), it can insert additional stages dynamically.

---

### Stage 3: TaskSet creation — one task per partition

For each stage, the DAGScheduler creates a **TaskSet**: a collection of tasks, one per input partition of that stage.

If `1342-0.txt` is split into 4 partitions, Stage 0 gets a TaskSet of 4 tasks. Each task is a serialised closure — the transformation code plus enough metadata to read exactly one partition. The TaskSet is handed to the TaskScheduler.

```mermaid
flowchart LR
    D["DAGScheduler"] -->|"submitTasks(TaskSet\n[task0, task1, task2, task3])"| T["TaskScheduler"]
```

---

### Stage 4: TaskScheduler assigns tasks to executors

The TaskScheduler wraps each TaskSet in a **TaskSetManager**, which tracks the state of every task (pending, running, succeeded, failed) and implements retry logic.

When an executor signals it has a free slot, the TaskScheduler picks the best task for that slot using **data locality** — it prefers to run a task on the executor that already holds the data partition in memory or on the same node as the data file. Locality levels, from best to worst:

| Level | Meaning |
|---|---|
| `PROCESS_LOCAL` | Data is in the executor's own memory (cached partition) |
| `NODE_LOCAL` | Data is on the same physical machine as the executor |
| `NO_PREF` | No locality preference — data is equally accessible from anywhere (e.g. off-heap or external storage) |
| `RACK_LOCAL` | Data is on a different machine but same network rack |
| `ANY` | Data must be fetched over the network |

If no executor with better locality is available, the TaskScheduler will wait briefly before falling back to a worse locality level rather than leave a slot idle.

The SchedulerBackend serialises the task and launches it on the chosen executor via RPC.

---

### Stage 5: executor runs the task

The executor deserialises the task closure and runs it against its assigned partition. For Stage 0 (ShuffleMapStage) in the word count:

1. Reads lines from its partition of `1342-0.txt` via the BlockManager
2. Runs `split → lower → filter` on each line
3. Hash-partitions the resulting `(word, 1)` pairs by key — each word is deterministically assigned to one of the output partitions
4. Writes the partitioned output to shuffle files on local disk via the BlockManager
5. Reports completion to the driver (including shuffle file locations)

The driver's DAGScheduler receives the completion event for each task. Once all 4 tasks in Stage 0 are done, it schedules Stage 1.

---

### Stage 6: shuffle — data moves between stages

Before Stage 1 can start, executors running Stage 1 tasks must fetch the shuffle data written by Stage 0. Each Stage 1 task reads its partition of the shuffle output from every Stage 0 executor — this is the **shuffle read**. The data crosses the network here.

```mermaid
flowchart LR
    subgraph S0["Stage 0 executors (shuffle write)"]
        P0["partition 0"]
        P1["partition 1"]
        P2["partition 2"]
        P3["partition 3"]
    end
    subgraph S1["Stage 1 executors (shuffle read)"]
        A["executor A\n(all words → partition 0)"]
        B["executor B\n(all words → partition 1)"]
        C["executor C\n(all words → partition 2)"]
        D["executor D\n(all words → partition 3)"]
    end
    P0 --> A & B & C & D
    P1 --> A & B & C & D
    P2 --> A & B & C & D
    P3 --> A & B & C & D
```

This is why shuffles are expensive: every Stage 1 executor must fetch data from every Stage 0 executor. Network I/O, disk I/O, and serialisation all happen here.

---

### Stage 7: ResultStage — results return to the driver

Stage 1 tasks run `count → orderBy` on their local word groups. The final `orderBy` requires another partial sort on each executor. The top-N results from each executor are sent back to the driver via the SchedulerBackend.

The driver merges the partial results, selects the top 10 overall, and `show()` prints them.

---

### Failure handling

The DAGScheduler and TaskScheduler handle failures at different levels:

- **Task failure** (executor crash, out-of-memory): the TaskScheduler retries the task on a different executor, up to `spark.task.maxFailures` times (default 4). The task is re-serialised and sent to a new slot.
- **Shuffle file lost** (executor that wrote Stage 0 output is gone before Stage 1 reads it): the DAGScheduler resubmits the entire ShuffleMapStage that produced the lost output. This is lineage-based recomputation — only the affected stage re-runs, not the whole job.
- **Stage failure** (all retries exhausted): the job fails and the exception surfaces to the driver.

---

### Shuffle storage: local, external, and remote

By default, Spark executors write shuffle output to **local disk** on the worker node. This creates two problems:

1. **Executor lifecycle coupling** — if an executor dies before Stage 1 reads its shuffle files, the DAGScheduler must resubmit the entire ShuffleMapStage to regenerate the lost data.
2. **Random small-file I/O** — each reducer fetches many small files from many executors across the network, resulting in scattered random reads.

Three progressively decoupled solutions exist:

---

**External Shuffle Service (ESS)** — a long-running JVM process deployed on every worker node, separate from executor processes. Executors write shuffle files and register them with the local ESS. If an executor is killed, the ESS continues serving its shuffle files to reducers. ESS is required for dynamic allocation on YARN and Standalone (so executors can be removed without losing their shuffle data).

```mermaid
flowchart LR
    subgraph WN["Worker node"]
        EA["Executor A\n(may be killed)"]
        EB["Executor B\n(may be killed)"]
        ESS["External Shuffle Service\n(stays alive; serves files to reducers)"]
    end
    EA -->|writes| ESS
    EB -->|writes| ESS
```

Enable with: `spark.shuffle.service.enabled = true` (default: `false`)

Limitation: ESS still ties shuffle data to the physical worker node. If the node fails, the data is gone.

---

**Push-based shuffle** — built into Spark (YARN + ESS only). Instead of waiting for reducers to pull data, map tasks actively **push** shuffle blocks to the ESS as they complete. The ESS merges blocks from multiple mappers into larger merged files per output partition. Reducers then read one large sequential merged file instead of many small random files.

```mermaid
flowchart LR
    subgraph Mappers
        M1["map task 1"] & M2["map task 2"] & M3["map task 3"]
    end
    subgraph ESS["External Shuffle Service"]
        MF["merged partition file\n(per output partition)"]
    end
    M1 & M2 & M3 -->|"push blocks"| MF
    MF -->|"one sequential read"| R["reducer"]
```

Enable with: `spark.shuffle.push.enabled = true` (default: `false`; YARN + ESS only)

---

**Remote Shuffle Service (RSS)** — a dedicated cluster of shuffle servers, completely separate from the Spark cluster. Executors write shuffle data over the network to the RSS cluster instead of local disk. No shuffle data touches the worker node's disk at all. This is the architecture required for **compute-storage separation** — common in cloud-native Kubernetes deployments where mounting hostPath volumes on every node is impractical.

```mermaid
flowchart LR
    subgraph Spark["Spark Cluster"]
        E1["Executor"] & E2["Executor"] & E3["Executor"]
    end
    subgraph RSS["Remote Shuffle Service Cluster"]
        S1["Shuffle server 1"]
        S2["Shuffle server 2"]
        S3["Shuffle server 3"]
    end
    E1 & E2 & E3 -->|"push over network"| S1 & S2 & S3
    S1 & S2 & S3 -->|"serve to reducers"| E1 & E2 & E3
```

Two production-grade Apache-incubated RSS implementations:

| Project | Apache status | Storage tiers | Notes |
|---|---|---|---|
| **Apache Celeborn** | Apache TLP | Memory → local disk → HDFS / object store | Supports Spark 2.4–4.x; LifecycleManager runs inside the driver |
| **Apache Uniffle** | Apache TLP | Memory → local disk → HDFS | Coordinator cluster assigns shuffle servers per job; official docs cover Spark 2/3 — verify Spark 4 JAR availability |

Both implement Spark's shuffle plugin API (`spark.shuffle.manager`). The Spark application sets the plugin class and the shuffle plugin intercepts all shuffle write/read calls, redirecting them to the RSS cluster instead of local disk.

**Configuring Apache Celeborn:**

```bash
# 1. Copy the Celeborn client JAR to the Spark classpath
# Use the spark-4 variant for Spark 4.x (spark-3 for Spark 3.x)
cp celeborn-client-spark-4-shaded_*.jar $SPARK_HOME/jars/
```

```properties
# Required
spark.shuffle.manager               org.apache.spark.shuffle.celeborn.SparkShuffleManager
spark.serializer                    org.apache.spark.serializer.KryoSerializer
spark.celeborn.master.endpoints     clb-1:9097,clb-2:9097,clb-3:9097
spark.shuffle.service.enabled       false

# Recommended
spark.celeborn.client.push.replicate.enabled  true   # server-side replication for fault tolerance
spark.sql.adaptive.localShuffleReader.enabled false  # must disable for Celeborn compatibility
```

**Configuring Apache Uniffle (Spark 3.x):**

```bash
# 1. Copy the Uniffle client JAR to the Spark classpath
# Uniffle ships separate JARs per Spark major version under <RSS_HOME>/jars/client/spark3/
cp rss-client-spark3-shaded-*.jar $SPARK_HOME/jars/
```

```properties
# Required
spark.shuffle.manager              org.apache.spark.shuffle.RssShuffleManager
spark.rss.coordinator.quorum       coord-1:19999,coord-2:19999
spark.shuffle.sort.io.plugin.class org.apache.spark.shuffle.RssShuffleDataIo
```

Coordinator dynamic configuration is enabled by default — the coordinator pushes optimal client settings to each job at startup, so only the quorum address is required beyond the manager class.

❓ As of Spark 4.1.2, Uniffle's official client guide documents Spark 2 and Spark 3 JARs only. Check the [Uniffle releases page](https://github.com/apache/uniffle/releases) for a Spark 4 client JAR before deploying with Spark 4.x.

---

### The full component map

```mermaid
flowchart TD
    A["Action called\n(.show, .write, .count)"]
    B["SparkContext.runJob()"]
    C["DAGScheduler\nBuilds DAG, finds shuffle\nboundaries, creates stages"]
    D["TaskScheduler\nReceives TaskSets,\nassigns tasks to slots"]
    E["SchedulerBackend\nRPC to executors,\nexecutor lifecycle"]
    F["Cluster Manager\n(YARN / K8s / Standalone)"]
    G["Executor\nDeserialises + runs task\nBlockManager handles data"]
    H["Results / shuffle files"]

    A --> B --> C
    C -->|"TaskSet per stage"| D
    D --> E
    E <-->|"resource offers\ntask launches"| F
    F -->|"allocates"| G
    E -->|"serialised task"| G
    G -->|"task completion\n+ shuffle locations"| D
    G --> H
    H -->|"ResultStage output"| B
```

Every component in the driver (SparkContext, DAGScheduler, TaskScheduler, SchedulerBackend) runs in the driver JVM. Executors are separate JVM processes on worker nodes. The cluster manager is an external service that neither the driver nor the executors run inside.

---

## Submitting applications: `--master` and `--deploy-mode`

Now that the architecture is clear — driver, executor, cluster manager — the `spark-submit` flags become concrete.

**`--master`** tells Spark how to run — either in a single local JVM, or where to find the cluster manager:

| `--master` value | Cluster? | Meaning |
|---|---|---|
| `local` | No | One JVM, one thread, one task at a time |
| `local[N]` | No | One JVM, N parallel threads (`local[4]` = 4 tasks) |
| `local[*]` | No | One JVM, one thread per CPU core — standard for local dev |
| `spark://host:7077` | Yes | Spark Standalone cluster manager at that address |
| `yarn` | Yes | YARN — no IP needed; ResourceManager address is read from `yarn-site.xml` inside `HADOOP_CONF_DIR` |
| `k8s://https://host:443` | Yes | Kubernetes API server |

With any `local[...]` value there is no cluster manager, no network, and no `--deploy-mode` concept — driver and executors share one JVM.

**`--deploy-mode`** answers one question: *where does the driver process run?* It only applies when `--master` points at a real cluster.

**`client` (default)** — the driver runs on the machine that called `spark-submit`. Stdout streams to your terminal. Kill the terminal and the job dies.

```mermaid
flowchart LR
    D["Your machine\n(spark-submit process — this IS the driver)"]
    E["Executors\non worker nodes"]
    D -->|tasks| E
    E -->|results| D
```

**`cluster`** — the driver is launched by the cluster manager on a worker node. `spark-submit` exits after handoff; you can close your laptop.

```mermaid
flowchart LR
    S["Your machine\n(spark-submit — exits after handoff)"]
    D["Worker node A\n(driver — launched by cluster manager)"]
    E["Worker nodes B, C, D\n(executors)"]
    S -->|submits| D
    D -->|tasks| E
    E -->|results| D
```

**Availability by cluster manager:**

| Setup | `--master` | `client` | `cluster` |
|---|---|---|---|
| pip / local | `local[*]` | N/A | N/A |
| Docker / Standalone | `spark://host:7077` | ✅ | ✅ Scala/Java only — Standalone cannot launch a Python process on a worker node, so PySpark must use `client` mode |
| YARN | `yarn` | ✅ | ✅ incl. PySpark |
| Kubernetes | `k8s://...` | ✅ | ✅ incl. PySpark (recommended) |
| Managed (Databricks, EMR, GCP Managed Spark, MS Fabric…) | platform-managed | abstracted | abstracted |

**When to use each:**

| Scenario | Choice |
|---|---|
| Local dev, notebook, `pyspark` shell | `--master local[*]` |
| Submitting from a gateway node inside the cluster | `--deploy-mode client` |
| Submitting from your laptop to a remote YARN/K8s cluster | `--deploy-mode cluster` |
| Production scheduled job | `--deploy-mode cluster` — no dependency on the submitting machine |

**Three equivalent ways to set master and deploy mode:**

```bash
# 1. spark-submit flags (most common for production jobs)
spark-submit --master yarn --deploy-mode cluster my_job.py
```

```python
# 2. SparkSession builder methods (scripts and notebooks)
spark = (
    SparkSession.builder
    .master("yarn")
    .config("spark.submit.deployMode", "cluster")
    .appName("my-job")
    .getOrCreate()
)
```

```bash
# 3. spark-defaults.conf (cluster-wide default, applies to all jobs)
spark.master                yarn
spark.submit.deployMode     cluster
```

---

## Installation

### Option 1 — pip (client/driver side only)

`pip install pyspark` bundles the Spark JARs inside the Python package — no tarball download or `SPARK_HOME` setup needed. Java 17+ must still be installed separately.

```bash
pip install pyspark          # Spark JARs + Python bindings (~300 MB); Java 17+ required separately
pip install pyspark-client   # Spark 4.0+ only: Connect-only pure-Python client, no JVM at all (~1.5 MB)
```

This gives you `spark-submit` and the `pyspark` shell. You can run locally (`--master local[*]`) or use it as the driver to connect to an existing cluster in `client` deploy mode.

What pip does **not** include: cluster setup scripts, Scala/R bindings. For a real cluster, every **executor node** still needs Spark installed — either via the tarball (Options 3–5 below) or baked into a Docker image (Option 2).

**How pip packaging evolved:**

| Era | What `pip install pyspark` contained | JAR source |
|---|---|---|
| PySpark ≤ 2.0.x | Python wrapper scripts only | Required manual tarball download + `SPARK_HOME` |
| **PySpark 2.1.0 (Nov 2016)** | **Full Spark JARs bundled into the wheel** | Self-contained — no tarball needed |
| PySpark 4.0.0 (May 2025) | Same + new `pyspark-client` sibling package | `pyspark-client` is pure Python, zero JARs, Connect-only |

The shift happened in [PR #15659](https://github.com/apache/spark/pull/15659), merged into branch-2.1 in November 2016: *"copy the jars over and package them with the Python code."* This is why older books still instruct you to download the tarball and set `SPARK_HOME` — they were written before or without awareness of the bundled-JAR approach, or assumed an enterprise context where executor nodes need the tarball anyway.

Use this for: local development, unit tests, notebooks, and as the driver when connecting to an existing cluster.

### Option 2 — Docker / local stack (Standalone cluster)

This project's setup (`docker compose up` in the [spark-delta-unitycatalog](https://github.com/arindamchoudhury/spark-delta-unitycatalog) repo). A Spark Standalone cluster runs inside Docker with a Spark Connect server on port 15002.

```bash
docker compose up   # starts Spark master + worker + Connect server
```

You connect via Spark Connect (`SPARK_REMOTE="sc://localhost"`) or submit directly to the Standalone cluster. Deploy mode is `client` only for PySpark — Standalone cannot ship a Python environment to a worker node.

Use this for: integration testing, local experimentation with Delta Lake and Unity Catalog.

### Option 3 — Standalone cluster (bare metal / VMs)

You install Spark on a set of machines, start a master process and worker processes yourself. Spark's own lightweight cluster manager handles resource allocation.

```bash
# on master node
$SPARK_HOME/sbin/start-master.sh

# on each worker node
$SPARK_HOME/sbin/start-worker.sh spark://master-host:7077
```

Submit with `--master spark://master-host:7077`. PySpark supports `client` deploy mode only.

Use this for: small on-prem clusters, learning cluster management without Hadoop or Kubernetes overhead.

### Option 4 — YARN (Hadoop clusters)

The dominant enterprise on-prem setup. Spark runs on top of Hadoop's resource manager. Both `client` and `cluster` deploy modes are fully supported for PySpark.

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  my_job.py
```

Use this for: existing Hadoop infrastructure, enterprise data lakes.

### Option 5 — Kubernetes

Spark submits each application as a set of Pods. `cluster` mode is the recommended and most natural fit — the driver runs as a Pod inside the cluster.

```bash
spark-submit \
  --master k8s://https://k8s-api-server:443 \
  --deploy-mode cluster \
  --conf spark.kubernetes.container.image=my-spark-image \
  my_job.py
```

Use this for: cloud-native deployments, containerised data platforms.

### Option 6 — Managed services

Databricks, Amazon EMR, GCP **Managed Service for Apache Spark** (formerly Dataproc), Microsoft **Fabric** (formerly Azure HDInsight, which retired in 2025). Spark is pre-installed and the platform manages the cluster. You don't write `spark-submit` directly — you use the platform's job submission UI or API. `--deploy-mode` is abstracted away.

Use this for: production workloads where you want managed infrastructure.

### Wiring PySpark from the tarball into a venv (Options 3–5)

When Spark is installed via tarball, `$SPARK_HOME/python/` already contains `pyspark` and `$SPARK_HOME/python/lib/` contains the matching `py4j-*.zip`. On a cluster, the daemon and workers load from these files. If you also `pip install pyspark` into your venv, you now have two copies — and version drift between them is a common source of hard-to-diagnose errors.

The clean solution is a `.pth` file: Python processes every `.pth` file found in `site-packages` at startup and adds the listed paths to `sys.path`. No duplication, no separate install.

```bash
# find the py4j zip bundled with the tarball
PY4J=$(ls $SPARK_HOME/python/lib/py4j-*.zip)

# write the .pth file into your active venv
cat > $(python -c "import site; print(site.getsitepackages()[0])")/spark_tarball.pth <<EOF
$SPARK_HOME/python
$PY4J
EOF
```

After this, `import pyspark` and `import py4j` resolve to the tarball's copies — identical to what the daemons and executors use. No `pip install pyspark` needed, and `PYTHONPATH` does not need to be set manually.

---


## Where Spark doesn't shine

Spark is a general-purpose distributed engine, not the best tool for every workload. Reaching for Spark when a simpler tool would do is itself a common mistake.

| Situation | Why Spark struggles | Better choice |
|---|---|---|
| **Data fits on one machine** (up to ~100 GB) | JVM startup, cluster coordination, and shuffle overhead dominate the runtime. A single-node engine avoids all of it. | pandas, Polars, DuckDB |
| **Sub-second interactive SQL** | Spark executes in stages; stage boundaries and task scheduling add latency. Trino pipelines stages concurrently and skips JVM serialisation overhead. | Trino / Presto |
| **True real-time streaming** (millisecond latency) | Structured Streaming is micro-batch, not event-by-event. Even the Real-Time Mode (4.0+) targets seconds, not milliseconds. | Apache Flink |
| **Row-level OLTP** (inserts, point lookups, transactions) | Spark is an analytics engine — it reads large columnar datasets in bulk. It has no row-level index and is not a database. | PostgreSQL, MySQL |
| **Many small files** (millions of files, KB each) | Each file becomes at least one task. Scheduling overhead dwarfs the actual work. | Consolidate files first, or use a purpose-built tool |
| **GPU-based deep learning** | Spark MLlib is CPU-oriented and designed for data parallelism over DataFrames. It doesn't natively handle GPU communication or all-reduce patterns. | PyTorch + Ray / Horovod |

The pattern: Spark is the right choice when data is large enough that distribution is necessary and the workload is batch, iterative, or near-real-time. When data is small, latency is tight, or the workload is transactional, a specialised tool will be faster, cheaper, and simpler to operate.

Sources: [AltexSoft — Spark pros and cons](https://www.altexsoft.com/blog/apache-spark-pros-cons/), [Trino vs Spark](https://snicsolutions.com/compare/trino-vs-spark), [DuckDB vs Spark benchmark](https://blog.dataexpert.io/p/duckdb-can-be-100x-faster-than-spark)

---


## Summary

- Spark uses a driver-executor model: the driver plans, executors process data in parallel partitions.
- In classic mode the driver is two processes (Python + JVM via Py4J); in Connect mode the Python client talks to a remote JVM server over gRPC. Classic is the default in all Spark 4.x.
- All transformations are lazy: no data moves until an action (`show`, `write`, `count`) triggers a job.
- A job breaks into stages (shuffle boundaries) and tasks (one per partition).
- `--master` sets where the cluster manager is (or `local[*]` for no cluster). `--deploy-mode` sets where the driver runs — only meaningful with a real cluster.

---

## References

- [Apache Spark cluster overview](https://spark.apache.org/docs/latest/cluster-overview.html)
- [PySpark 4.1.x documentation](https://spark.apache.org/docs/latest/api/python/)
- [Spark Connect overview](https://spark.apache.org/docs/latest/spark-connect-overview.html)
- [Submitting Applications](https://spark.apache.org/docs/latest/submitting-applications.html)
- [PR #15659 — bundled JARs in pip package](https://github.com/apache/spark/pull/15659)
