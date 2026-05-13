# Chapter 2 — Your First Data Program in PySpark

> *Source: Rioux (2022), Chapter 2, pages 17–44.*
>
> Builds a word-frequency program on *Pride and Prejudice* to introduce the core PySpark development loop: launch a REPL, ingest data into a DataFrame, apply a chain of column transformations, filter rows, and print results. The chapter deliberately stops before counting and scaling — those land in Chapter 3 — so the focus stays on transformation mechanics.
>
> 📌 **Notes adapted to PySpark 4.1.1.** The book shows Python 3.8 and Spark 3.2.0. PySpark 4.1.1 requires **Python ≥ 3.10** and **Java 17**. Exception classes moved from `pyspark.sql.utils` to `pyspark.errors` in Spark 4.x — update any `from pyspark.sql.utils import AnalysisException` to `from pyspark.errors import AnalysisException`. The DataFrame API (select, filter, split, explode, lower, regexp_extract) is unchanged.

---

## 1. The three meta-steps of every PySpark program

Almost every data-driven program — from a quick summary to a full ML pipeline — follows this skeleton:

1. **Read** — ingest data from a source into a structure.
2. **Transform** — apply operations to reshape, filter, enrich the data.
3. **Export / sink** — write results to a file, database, or screen.

This chapter walks steps 1 and 2 for a concrete problem: *which words appear most often in Pride and Prejudice?*

---

## 2. Setting up the PySpark shell

### Launching the shell

```bash
pyspark
```

This drops you into an IPython (or plain Python) REPL with two variables pre-configured:

- `spark` — a `SparkSession`; main entry point for DataFrame operations.
- `sc` — a `SparkContext`; lower-level entry point; rarely needed directly.

> 💡 **Tip** — IPython (`pip install ipython`) is strongly recommended over the plain Python shell: friendlier paste, tab completion, syntax highlighting, and the `?` / `??` doc shortcuts.

### Creating SparkSession from scratch (for scripts and IDEs)

When writing a `.py` file or using a Python IDE, skip the `pyspark` launcher and create the session manually. This makes it explicit that PySpark is just a Python library.

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Analyzing the vocabulary of Pride and Prejudice")
    .getOrCreate()
)
```

Key points:

- **Builder pattern** — `SparkSession.builder` chains config methods before calling `.getOrCreate()`.
- **`getOrCreate()`** — checks the JVM process for an active `SparkSession`. If one exists, returns it as-is — **any `.config()` calls on the builder are silently ignored**. If none exists, creates a new session with the builder's config applied.
- **`appName`** — shows up in the Spark UI (Ch 11); pick something meaningful.
- `SparkSession` wraps and supersedes the older `SparkContext` / `SQLContext` combo. Access the underlying context via `spark.sparkContext` if you need it.

> ⚠️ **Notebook trap** — Re-running the builder cell after changing a `.config()` value silently does nothing: `getOrCreate()` returns the existing session unchanged. Call `spark.stop()` first to force a fresh session with the new config. Note: `spark.close()` does not exist on `SparkSession` — the correct method is `spark.stop()`.

> ⚠️ **Legacy code warning** — Older tutorials use `sc` and `sqlContext` as separate entry points. In current PySpark: `sc = spark.sparkContext` and `sqlContext = spark` are the equivalents. Avoid creating them directly in new code.

### Binding to localhost and routing log output (scripts)

For scripts run on laptops — especially with VPN, Docker, or multiple network adapters — add two lines before building the session:

```python
import os
from pyspark.sql import SparkSession

os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
conf_path = os.path.abspath("log4j2.xml")

spark = (
    SparkSession
    .builder
    .appName("chapter4")
    .config(
        "spark.driver.extraJavaOptions",
        f"-Dlog4j2.configurationFile={conf_path}"
    )
    .getOrCreate()
)
```

What each piece does:

- **`os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"`** — tells Spark's JVM to bind its network listeners to the loopback address. Without this, Spark auto-detects a network interface, which can land on a VPN adapter or a dormant one, producing `Connection refused` errors or slow startup.
- **`os.path.abspath("log4j2.xml")`** — resolves a relative path to an absolute one. The JVM is launched from an unpredictable working directory, so a relative path silently fails to locate the file; `abspath` anchors it to your script's location.
- **`spark.driver.extraJavaOptions`** — passes raw JVM flags to the driver process. `-Dlog4j2.configurationFile=<path>` overrides the bundled Log4j 2 config with your own, giving you finer-grained control over which loggers are active and at what level — a more permanent alternative to `spark.sparkContext.setLogLevel()`.

> 💡 **Tip** — This pattern (env var + custom log4j2.xml) is the standard script setup used throughout the book. A minimal `log4j2.xml` that sets `org.apache.spark` to `WARN` eliminates Spark's own log lines entirely, leaving only your application output.

### Passing multiple config values

Chaining `.config()` calls is the idiomatic approach — there is no limit on the number of calls:

```python
spark = (
    SparkSession
    .builder
    .config("spark.ui.port", "4042")
    .config("spark.driver.extraJavaOptions",
            f"-Dlog4j2.configurationFile={conf_path}")
    .getOrCreate()
)
```

When config needs to be assembled programmatically from separate sources (e.g. a base config dict plus an environment-specific override), use `SparkConf.setAll()` and pass the result to `.config(conf=...)`:

```python
from pyspark import SparkConf
from pyspark.sql import SparkSession

base_conf = SparkConf().setAll([
    ("spark.ui.port", "4042"),
])
log_conf = SparkConf().setAll([
    ("spark.driver.extraJavaOptions",
     f"-Dlog4j2.configurationFile={conf_path}"),
])

merged = SparkConf().setAll(base_conf.getAll() + log_conf.getAll())

spark = SparkSession.builder.config(conf=merged).getOrCreate()
```

- `SparkConf.getAll()` returns a list of `(key, value)` tuples — easy to combine with `+`.
- `.config(conf=...)` accepts a `SparkConf` object instead of individual key/value pairs.

> 💡 **Rule of thumb** — use chained `.config()` when all settings are known at write time; use `SparkConf` when building config dynamically (loading from a file, merging environment-specific overrides, or sharing a conf object across multiple session builders).

### The log4j2.xml file

Place `log4j2.xml` alongside your script (the `os.path.abspath` call resolves it from there). Three decisions govern the whole file:

**Decision 1 — `status` on `<Configuration>`**

Controls log4j2's *own* internal diagnostic messages. Set to `WARN` so log4j2 doesn't announce itself on every run.

**Decision 2 — the Appender (where logs go)**

A `ConsoleAppender` targeting `SYSTEM_ERR` is the right choice for local scripts: Spark already writes its own logs to stderr, so everything lands in the same stream and terminal tools (e.g. `2>/dev/null`) can suppress it in one shot.

The `PatternLayout` format `%d{HH:mm:ss} %-5level %logger{36} - %msg%n` gives you a readable timestamp + level + logger name without burying the message.

**Decision 3 — the Loggers (what level for each namespace)**

The Root logger catches everything not explicitly named. The four namespaces that generate Spark's noise are `org.apache.spark`, `org.apache.hadoop`, `io.netty`, and `org.spark_project`. Setting each to `WARN` with `additivity="false"` silences the noise without hiding genuine problems — and prevents each named logger from also firing the Root appender (which would produce duplicate lines).

| Root level | Named Spark loggers | When to use |
|---|---|---|
| `ERROR` | `ERROR` | Fully silent — production-like runs |
| `WARN` | `WARN` | Normal study / chapter scripts |
| `INFO` | `WARN` | Debugging your own app code while keeping Spark quiet |

**Full file:**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<Configuration status="WARN">

    <Appenders>
        <Console name="console" target="SYSTEM_ERR">
            <PatternLayout pattern="%d{HH:mm:ss} %-5level %logger{36} - %msg%n"/>
        </Console>
    </Appenders>

    <Loggers>
        <!-- Silence the four noisy Spark/Hadoop namespaces -->
        <Logger name="org.apache.spark"  level="WARN" additivity="false">
            <AppenderRef ref="console"/>
        </Logger>
        <Logger name="org.apache.hadoop" level="WARN" additivity="false">
            <AppenderRef ref="console"/>
        </Logger>
        <Logger name="io.netty"          level="WARN" additivity="false">
            <AppenderRef ref="console"/>
        </Logger>
        <Logger name="org.spark_project" level="WARN" additivity="false">
            <AppenderRef ref="console"/>
        </Logger>

        <!-- Root catches everything else — your own code lands here -->
        <Root level="WARN">
            <AppenderRef ref="console"/>
        </Root>
    </Loggers>

</Configuration>
```

> ⚠️ **Spark 3.3+ uses log4j 2.x.** If you see the JVM complain that it can't find a `log4j2.xml`, check that you're not accidentally placing a log4j *1.x* style file (`log4j.properties`) — the formats are incompatible. The `<Configuration>` root element is the log4j2 tell.

### Configuring the log level

The default log level is `WARN` in the shell and `INFO` in batch mode. `INFO` is very noisy. Change it with:

```python
spark.sparkContext.setLogLevel("WARN")   # or ERROR, OFF, DEBUG, TRACE, ALL
```

| Level | What you see |
| --- | --- |
| `OFF` | Nothing |
| `FATAL` | Fatal crashes only |
| `ERROR` | Recoverable errors too |
| `WARN` | Warnings — good default for learning |
| `INFO` | Runtime info (repartitioning, data recovery) |
| `DEBUG` | Debug messages |
| `TRACE` | Very verbose debug |
| `ALL` | Everything |

> 💡 **Tip** — Anything chattier than `WARN` in the shell will interleave log lines with your typing. `WARN` is the sweet spot for interactive development.

### (Optional) Eager evaluation in the REPL

By default, entering a DataFrame variable prints only its schema, not its data — because data evaluation is lazy. If you want a pandas-style "show me the data" experience during exploration:

```python
spark = (
    SparkSession.builder
    .config("spark.sql.repl.eagerEval.enabled", "True")
    .getOrCreate()
)
```

> ⚠️ **Pitfall** — Eager mode triggers full computation on every assignment. Great for demos, expensive for large data. Leave it off in production pipelines.

### Connecting to a remote cluster

By default, `SparkSession.builder` starts a local Spark process (equivalent to `.master("local[*]")`). To connect to an already-running cluster in **classic mode**, call `.master()` with the cluster URL before `.getOrCreate()`.

```python
spark = (
    SparkSession.builder
    .appName("my_app")
    .master("spark://master-host:7077")   # Standalone cluster
    .getOrCreate()
)
```

Common `.master()` URLs:

| URL | Mode |
|---|---|
| `local` | 1 thread (sequential) |
| `local[N]` | N threads |
| `local[*]` | One thread per CPU core — default for scripts |
| `spark://host:7077` | Standalone cluster manager |
| `yarn` | Hadoop YARN (reads `HADOOP_CONF_DIR` / `YARN_CONF_DIR`) |
| `k8s://https://host:443` | Kubernetes |
| `mesos://host:5050` | Apache Mesos — **removed in Spark 4.x** |

> 💡 **Tip** — In practice, the master URL is almost never hard-coded. Prefer passing it via `spark-submit --master <url>` or an environment variable so the same script runs locally and on the cluster without edits.

> ⚠️ **Is `.master()` deprecated?** As of PySpark 4.1.1, the method carries no formal deprecation notice. However, the Spark team's direction is clearly toward Spark Connect (`.remote()`) for Python clients — classic mode is being superseded, not yet removed. The safe read: `.master()` still works and is fine for `spark-submit` cluster submissions, but new local/interactive code should prefer `.remote()`. Expect a formal deprecation in a future major release.

### Spark Connect (Spark 3.4+ / default in 4.x)

**What it is:** Spark Connect is a thin client-server protocol built on gRPC + Apache Arrow. Instead of embedding the Spark driver inside your Python process, your code talks to a remote Spark Connect *server* over the wire. The JVM lives on the server; your Python process stays lightweight.

```
Your Python script
      │  gRPC (port 15002)
      ▼
Spark Connect Server (JVM)
      │
      ▼
Spark Cluster (executors)
```

**Why it matters:**
- No JVM in your Python process — `pip install pyspark` is all you need, no local Java required.
- Stable, versioned API surface — client and server can differ by minor version.
- Better isolation: a buggy client cannot crash the server.
- Native support from non-JVM languages (Go, Rust, etc. can speak the protocol).

**How to use it — client side:**

Replace `.master(...)` with `.remote(...)`:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()
```

Or set the environment variable so all scripts pick it up automatically:

```bash
export SPARK_REMOTE="sc://localhost:15002"
```

```python
spark = SparkSession.builder.getOrCreate()  # picks up SPARK_REMOTE automatically
```

**How to start the server (local dev):**

```bash
# Standalone Connect server — from your Spark installation
./sbin/start-connect-server.sh

# Or via the pyspark shell in Connect mode
pyspark --remote "sc://localhost"
```

**PySpark 4.x — Connect is the default for the REPL.** The `pyspark` shell in Spark 4.0+ attempts to connect to a local Spark Connect server on port 15002 by default. The classic "driver inside the client" mode is still available via `.master(...)` or `spark.api.mode=classic`, but is no longer the primary path for the shell.

**Limitations to know:**
- A small number of low-level APIs (direct RDD operations, `SparkContext.addFile`, some JVM interop) are not yet available through Connect — they require the classic mode.
- Spark UI access is on the *server* side; you browse to the server's web UI, not localhost.
- **Port 4040 conflict with Docker** — the Connect server's Spark UI binds to port 4040 on the host (via `com.docker.backend.exe` when running in Docker). A local PySpark session started alongside will find 4040 occupied and fall back to 4041 with a `WARN SparkUI could not bind on port 4040` message. This is harmless; to suppress it, pin the local session explicitly: `.config("spark.ui.port", "4041")`.

> 📌 **Version note (PySpark 4.1.1)** — Spark Connect is stable and production-ready. The `pyspark` shell defaults to Connect mode; pass `--master local[*]` explicitly to force classic mode.

---

## 3. Mapping the program

Before writing code, sketch the steps. For this problem:

| Step | Description | PySpark operation |
| --- | --- | --- |
| 1. Read | Load the text file | `spark.read.text()` |
| 2. Token | Split each line into a list of words | `split()` + `select()` |
| 3. Clean | Lowercase and remove punctuation; one word per row | `lower()`, `regexp_extract()`, `explode()`, `filter()` |
| 4. Count | Frequency per word | (Chapter 3) |
| 5. Answer | Top N words | (Chapter 3) |

> 💭 (mine): The "map before coding" habit pays double dividends in PySpark — you spot which steps are transformations (cheap to chain) vs. actions (expensive to trigger accidentally) before you write a line.

---

## 4. Ingesting data

### The two core data structures

| Structure | Mental model | When to use |
| --- | --- | --- |
| **RDD** | A distributed bag of arbitrary Python objects | Low-level control; record-by-record Python logic (Ch 8) |
| **DataFrame** | A typed table of columns | Almost everything — fast, optimizable, SQL-compatible |

The DataFrame is the dominant structure in modern PySpark. The module for it is named `pyspark.sql` — it takes heavy inspiration from SQL.

### `spark.read` — the DataFrameReader

`spark.read` gives you a `DataFrameReader` object with format-specific methods:

```python
spark.read.text(path)     # plain text — one line = one row
spark.read.csv(path)      # CSV
spark.read.json(path)     # JSON
spark.read.parquet(path)  # Parquet (Spark's default storage format)
spark.read.orc(path)      # ORC
```

> 📌 **Version note** — Parquet is Spark's default storage format (read and write). ORC is an Apache competitor; both are columnar, compressed, and optimized for big data.

Loading *Pride and Prejudice*:

```python
book = spark.read.text("./data/gutenberg_books/1342-0.txt")
# book → DataFrame[value: string]
```

The result is a DataFrame with one column (`value`) of type `string`. Each row is one line of the file.

### Exploring a DataFrame's structure

```python
# See the schema (column names + types) — printed to REPL on variable inspection
book

# Tree view of schema — most useful in complex DataFrames
book.printSchema()
# root
#  |-- value: string (nullable = true)

# Schema as a list of (name, type) tuples
book.dtypes
# [('value', 'string')]
```

### Peeking at the data — `show()`

`show()` is an **action** — it triggers computation and prints rows to the screen.

```python
book.show()                          # 20 rows, truncated at 20 chars
book.show(10, truncate=50)           # 10 rows, truncated at 50 chars
book.show(5, truncate=False)         # 5 rows, full length
book.show(5, truncate=False, vertical=True)  # each record as a mini-table
```

| Parameter | Default | What it does |
| --- | --- | --- |
| `n` | 20 | Number of rows to display |
| `truncate` | `True` (20 chars) | `False` = full; any int = chars limit |
| `vertical` | `False` | Display each record as a key-value mini-table |

> 💡 **Tip** — `printSchema()` + `show()` together are your primary exploration tools. Use them constantly when building a new pipeline.

### Column pruning

Column pruning is Spark's optimization where it avoids reading columns from disk that your query doesn't need.

For columnar formats (Parquet, ORC), data is stored column-by-column on disk. If your table has 50 columns but you only `select("id", "country")`, Spark physically skips the other 48 — they are never loaded into memory. Catalyst folds the projection down into the file scan automatically.

```python
# Reads ALL 50 columns from disk
df = spark.read.parquet("data/events/")
df.select("id", "country").show()

# Reads ONLY id and country — 48 columns never touched
df = spark.read.parquet("data/events/").select("id", "country")
df.show()
```

On a wide table this can reduce I/O by 95%+. This is one of the core reasons Parquet outperforms CSV — CSV is row-oriented so every byte of every row is always read regardless of how many columns you need.

**When column pruning breaks down:**

- `select("*")` — tells Spark you need everything; nothing is pruned.
- **Python UDFs** — Catalyst cannot inspect a UDF's body, so it conservatively keeps all columns in scope.
- **Nested field access without explicit projection** — accessing a struct field may pull the whole struct.

Verify it is working with `df.explain(True)` and check `ReadSchema` in the physical plan — it should list only the columns you selected.

### Predicate pushdown

Predicate pushdown is Spark's optimization where it moves `filter()` conditions into the data source reader, so rows that don't match are discarded before they ever enter Spark's processing engine.

For Parquet/ORC, this works at two levels:

1. **Row-group skipping** — each Parquet row group stores min/max statistics per column in its footer. If a filter condition (e.g. `year == 2024`) cannot match any value in a row group's range, that entire row group is skipped without reading it.
2. **Page-level filtering** — Parquet's optional bloom filters and column indexes allow skipping at finer granularity within a row group.

```python
import pyspark.sql.functions as F

# Spark reads only row groups where year could be 2024 — rest are skipped at the file level
df = spark.read.parquet("data/events/").filter(F.col("year") == 2024)
```

The pushdown is automatic. Verify it fired with `df.explain()` — look for `PushedFilters` in the physical plan scan node:

```
FileScan parquet [id#0, year#1]
  PushedFilters: [IsNotNull(year), EqualTo(year,2024)]
  ReadSchema: struct<id:string,year:int>
```

**When predicate pushdown does not fire:**

- Filters on computed/derived columns — `filter(F.col("year") + 1 == 2025)` cannot be pushed; only equality/range on raw stored columns.
- **Python UDFs inside the filter** — Catalyst can't push an opaque function into the reader.
- Non-columnar formats (CSV, JSON) — no row-group statistics exist, so there's nothing to skip.
- JDBC sources require explicit `partitionColumn` / `predicates` config for meaningful pushdown.

---

### Column pruning vs. predicate pushdown

Both reduce I/O at the storage layer, but they cut along different axes:

| | What it eliminates | Axis | Physical plan signal |
|---|---|---|---|
| **Column pruning** | unneeded columns | vertical — fewer fields per record | `ReadSchema` lists only selected columns |
| **Predicate pushdown** | rows that fail a filter | horizontal — fewer records read | `PushedFilters` in the scan node |

They are complementary and usually both fire on the same query. A query like:

```python
df = (
    spark.read.parquet("data/events/")
    .filter(F.col("year") == 2024)   # predicate pushdown eliminates row groups
    .select("id", "country")         # column pruning reads only 2 of N columns
)
```

…reads only the row groups that contain 2024 data, and within those, only the `id` and `country` column chunks. Both optimizations happen inside the file reader before any data reaches Spark's executor memory.

**Key difference:** column pruning is always free and safe to rely on for columnar formats. Predicate pushdown depends on the quality of the file's statistics — freshly written Parquet files have statistics; files written by non-Spark tools may not.

> 💡 **JDBC sources** — both optimizations work for databases too, but the mechanism shifts from file I/O to SQL query rewriting:
>
> - **Column pruning** → Spark generates `SELECT id, country FROM table` instead of `SELECT *`.
> - **Predicate pushdown** → Spark appends a `WHERE` clause: `SELECT id, country FROM table WHERE year = 2024`. The database executes the filter so only matching rows cross the network.
>
> Simple comparisons (`=`, `<`, `>`, `IN`, `IS NULL`) push cleanly. Arithmetic on columns, UDFs, and non-standard functions fall back to Spark-side filtering after the full result is transferred — check `PushedFilters` on the `JDBCScan` node in `df.explain()`.
>
> One JDBC-specific gotcha: without explicit partition config (`partitionColumn`, `numPartitions`, `lowerBound`, `upperBound`), Spark uses a single connection regardless of pushdown — the filter reduces rows transferred but parallelism stays at 1.

---

### Best practices for `spark.read`

**1. Declare the schema explicitly — don't infer it in production**

`inferSchema=True` triggers a full extra pass over the data just to guess types. Fine for exploration; expensive and fragile in pipelines. Define it once with `StructType`:

```python
import pyspark.sql.types as T

schema = T.StructType([
    T.StructField("word", T.StringType(), nullable=False),
    T.StructField("count", T.LongType(), nullable=False),
])

df = spark.read.csv("data/words.csv", schema=schema, header=True)
```

Benefits: faster reads, no surprise type coercions, catches malformed data early.

**2. Prefer Parquet (or ORC) over CSV/JSON**

| Format | Column pruning | Predicate pushdown | Compression | Schema embedded |
|---|---|---|---|---|
| CSV | No | No | No | No |
| JSON | No | No | No | No |
| Parquet | Yes | Yes | Yes | Yes |
| ORC | Yes | Yes | Yes | Yes |

CSV and JSON require reading every byte to find a column. Parquet skips entire column chunks that aren't needed, and can skip row groups whose min/max statistics don't match a filter. For repeated reads of the same data, converting once to Parquet pays for itself quickly.

**3. Let predicate pushdown work — filter early**

For Parquet/ORC, Spark pushes `filter()` conditions down into the file reader, skipping row groups before they're loaded into memory. This only works when the filter is on a column that has statistics in the file footer (all columns written by Spark do).

```python
import pyspark.sql.functions as F

# Spark reads only the row groups where year == 2024 — never touches the rest
df = spark.read.parquet("data/events/").filter(F.col("year") == 2024)
```

Nothing to do — it's automatic. But it only helps if you actually filter before collecting/aggregating.

**4. Use partition pruning for directory-partitioned data**

When data is stored in Hive-style partition directories (`year=2024/month=01/`), Spark reads only the matching subdirectories when you filter on a partition column. This can eliminate 99% of I/O on large datasets.

```
data/events/
    year=2023/
    year=2024/   ← only this is read when filtering year == 2024
```

```python
import pyspark.sql.functions as F

df = spark.read.parquet("data/events/").filter(F.col("year") == 2024)
```

Partition pruning and predicate pushdown work together — pruning cuts directories, pushdown cuts row groups within those directories.

> 📌 **Delta tables: two extra layers on top of partition pruning**
>
> Delta still uses Hive-style directories when you specify `partitionBy`, so partition pruning works identically. But Delta adds:
>
> **Data skipping** — Delta maintains column statistics (min, max, null count) per data file in its transaction log (`_delta_log/`). Spark reads the log first and skips any file whose stats prove it can't contain matching rows — for *any* filtered column, not just partition columns:
>
> ```python
> # No user_id partition needed — Delta skips files where max(user_id) < 42
> df = spark.read.format("delta").load("data/events/").filter(F.col("user_id") == 42)
> ```
>
> **Z-Ordering** — data skipping only helps when values are clustered within files. `OPTIMIZE ... ZORDER BY` physically rearranges data so related values land together, making skipping effective for high-cardinality non-partition columns:
>
> ```sql
> OPTIMIZE events ZORDER BY (user_id, event_type)
> ```
>
> **How this changes partition strategy:**
>
> | | Plain Parquet | Delta |
> |---|---|---|
> | Partition columns | Everything you filter on | High-cardinality temporal only (`date`, `year`) |
> | Non-partition filters | Full scan within partition | Skipped via file stats |
> | Point-lookup tuning | Add partitions | ZORDER instead |
>
> Over-partitioning is a common Delta mistake — too many small partitions create too many tiny files. Delta's rule of thumb: only partition when each partition will be ≥ 1 GB.

**5. Read directories, not individual files**

Pass a directory path, not `file1.parquet, file2.parquet`. Spark handles the glob internally, parallelises across files naturally, and applies partition pruning from directory structure.

```python
# Good — directory; Spark discovers and parallelises files
df = spark.read.parquet("data/events/")

# Fine for one-offs; brittle for evolving datasets
df = spark.read.parquet("data/events/part-00001.parquet")
```

Use `pathGlobFilter` to restrict which files are picked up without listing them individually:

```python
df = spark.read.option("pathGlobFilter", "*.parquet").load("data/mixed/")
```

**6. Avoid many tiny files — and what to do when you can't**

Each file becomes at least one Spark task. A directory with 10,000 one-KB CSV files creates 10,000 tasks — scheduling overhead dominates actual work.

*When you can't avoid tiny files at the source*, there are four tools depending on where in the pipeline you can intervene:

**At read time (source files are fixed):**

`maxPartitionBytes` and `openCostInBytes` control how Spark bins files into partitions. Raising both forces more aggressive combining of small files into fewer, larger tasks:

```python
spark.conf.set("spark.sql.files.maxPartitionBytes", str(256 * 1024 * 1024))  # 256 MB
spark.conf.set("spark.sql.files.openCostInBytes",   str(8   * 1024 * 1024))  # 8 MB
```

- **`maxPartitionBytes`** (default 128 MB) — target partition size; files are binned together up to this limit.
- **`openCostInBytes`** (default 4 MB) — estimated cost of opening one file, added to each file's size when packing. Raising it biases Spark toward combining more files per partition.

A cheap downstream fix is `coalesce(N)` right after the read — reduces partition count without a shuffle:

```python
df = spark.read.csv("data/tiny/").coalesce(20)
```

**At write time (you control the output):**

- **`coalesce(N)` before write** — no shuffle; fast but can produce uneven file sizes if input partitions are skewed.
- **`repartition(N)` before write** — full shuffle; evenly sized output files; worth the cost for data read many times.
- **Delta `optimizeWrite`** — Delta auto-sizes files before writing, eliminating small output files without manual tuning:

```python
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
# or per-write:
df.write.option("optimizeWrite", "true").format("delta").save(path)
```

**After the fact — Delta `OPTIMIZE`:**

Compacts existing small files into target-size files (default 1 GB). Can be scoped to recent partitions to keep cost low:

```sql
OPTIMIZE events                                  -- whole table
OPTIMIZE events WHERE date >= '2024-01-01'       -- recent partitions only
```

**Delta auto-compaction (ongoing writes are small and frequent):**

Runs a lightweight `OPTIMIZE` automatically after each write — no scheduled job needed:

```python
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
```

**Decision guide:**

| Situation | Best tool |
|---|---|
| Source files fixed, just reading | `maxPartitionBytes` + `openCostInBytes` |
| Controlling your write output | `repartition(N)` or Delta `optimizeWrite` |
| Delta table accumulating small files over time | `OPTIMIZE` (scheduled) |
| Delta, writes are frequent and small | Auto-compaction |
| Non-Delta, files already written | Read → `coalesce` → rewrite in place |

**7. `spark.read` is lazy — the schema step is not**

The read itself is lazy (no data is loaded until an action runs). But schema inference (`inferSchema=True`) and the initial directory listing are *eager* — they happen the moment you call `spark.read.csv(...)`. Explicit schemas skip the inference scan entirely.

**8. Cache DataFrames that are reused multiple times**

`spark.read` re-executes the full read (and all upstream transformations) every time an action is called on an uncached DataFrame. If the same DataFrame feeds multiple branches of your pipeline, cache it after reading:

```python
df = spark.read.parquet("data/events/").cache()

count   = df.count()              # triggers read + caches
summary = df.groupBy("type").count()   # reads from cache
```

Use `persist()` to control storage level — `cache()` is shorthand for `MEMORY_AND_DISK_DESER` (deserialized, spills to disk if memory is full):

```python
from pyspark import StorageLevel

df.persist(StorageLevel.DISK_ONLY)   # for DataFrames too large for memory
```

Unpersist when done to release memory:

```python
df.unpersist()
```

> ⚠️ **Pitfall** — Caching a DataFrame that is used only once wastes memory and adds overhead. Cache only when the same DataFrame is consumed by two or more actions.

**9. Use `df.explain()` to verify column pruning and predicate pushdown**

Spark's physical plan shows whether your read optimisations are actually firing. Run it after building your query — before collecting any results:

```python
df = spark.read.parquet("data/events/").filter(F.col("year") == 2024).select("id", "type")
df.explain(True)   # True = show logical + physical plans
```

Look for `PushedFilters` in the physical plan (predicate pushdown working) and check that only the columns you selected appear in `ReadSchema` (column pruning working). If `ReadSchema` includes columns you didn't select, something in the pipeline — a UDF, a `select("*")`, a nested field access — is blocking pruning.

---

## 5. Column transformations

### The `select()` method

`select()` returns a new DataFrame containing only the specified columns (or column expressions). It's the PySpark equivalent of SQL `SELECT`.

Four equivalent ways to select the `value` column:

```python
import pyspark.sql.functions as F

book.select(book.value)          # dot notation — fails on column names with spaces
book.select(book["value"])       # bracket notation — handles any column name
book.select(F.col("value"))      # F.col() — most flexible; doesn't bind to a specific df
book.select("value")             # string shorthand — fine for plain selects
```

> 💡 **Prefer `F.col()` in complex expressions, strings elsewhere.** A plain string (`"value"`) works as a column reference in `select()` and as an argument to most functions (`F.split("value", " ")`). Use `F.col()` when you need to call Column methods on the reference (`.alias()`, `.cast()`, `.isNull()`, etc.), when the column name is ambiguous after a join, or when building expressions that are passed around as variables. In simple, unambiguous cases the string form is cleaner.

### Best practices for `select()`

**1. Select only what you need, as early as possible**

Dropping unused columns early reduces the data Spark carries through every subsequent shuffle and join. For Parquet/ORC it also enables column pruning — unselected columns are never read from disk.

```python
import pyspark.sql.functions as F

# Bad — drags all 50 columns through the entire pipeline
df.filter(F.col("year") == 2024).groupBy("country").count()

# Good — prune to needed columns first
df.select("year", "country").filter(F.col("year") == 2024).groupBy("country").count()
```

**2. Always `alias()` computed columns**

Auto-generated names like `split(value, , -1)` or `lower(word)` are unreadable and fragile — a version upgrade can change the auto-name and break downstream code that refers to it by string.

```python
import pyspark.sql.functions as F

# Bad — column is named "split(value,  , -1)"
book.select(F.split("value", " "))

# Good — string shorthand is fine when passing a column name into a function
book.select(F.split("value", " ").alias("words"))
```


**3. Prefer one `select()` with multiple expressions over repeated `withColumn()` calls**

`withColumn()` is convenient for adding or replacing a single column. But calling it N times in a loop builds a deeply nested logical plan — Catalyst has to unroll it, and in extreme cases (N > ~100) this causes serious performance degradation or stack overflows.

```python
# Bad — O(N) plan nodes, slow to parse for large N
for col_name in many_columns:
    df = df.withColumn(col_name, some_expr(col_name))

# Good — single projection, one plan node
df = df.select(
    "*",
    *[some_expr(c).alias(c) for c in many_columns]
)
```

For adding just one or two columns, `withColumn()` is fine and more readable.

**4. Avoid `select("*")` in production pipelines**

`select("*")` is convenient interactively but disables column pruning and makes pipelines brittle — a schema change upstream silently adds columns that flow through the entire DAG.

```python
# Fine for exploration
df.select("*").show()

# In a pipeline, be explicit
df.select("id", "event_type", "ts")
```

When you need all existing columns plus new ones, use `df.columns` to be explicit:

```python
df.select(*df.columns, new_expr.alias("new_col"))
```

**5. Use `selectExpr()` for SQL-style expressions**

`selectExpr()` accepts SQL string expressions, making it a concise bridge between the DataFrame API and SQL syntax — useful for quick casts, arithmetic, and renaming without importing functions:

```python
df.selectExpr(
    "id",
    "upper(name) as name",
    "price * 1.2 as price_with_tax",
    "cast(ts as timestamp) as ts",
)
```

Equivalent to a `select()` with `F.expr()` wrapping each string. Use it when the SQL form is more readable than the function form; avoid it when you need type safety or composable column objects.

**6. Use `withColumns()` (plural) instead of a `withColumn()` loop — Spark 3.3+**

Spark 3.3 added `withColumns()` (plural), which accepts a `{column_name: expression}` dict and applies all changes in a single plan node — the official solution to the repeated-`withColumn` performance problem.

Say you have a raw events DataFrame and want to clean several columns at once:

```python
import pyspark.sql.functions as F

# Bad — three separate plan nodes, Catalyst re-analyses the full plan each time
df = df.withColumn("name",       F.upper("name"))
df = df.withColumn("price_usd",  F.round(F.col("price") * 0.93, 2))
df = df.withColumn("event_date", F.to_date("ts"))

# Good — one plan node (Spark 3.3+)
df = df.withColumns({
    "name":       F.upper("name"),
    "price_usd":  F.round(F.col("price") * 0.93, 2),
    "event_date": F.to_date("ts"),
})
```

Both produce the same result. The dict form keeps the original columns and adds/overwrites only the named ones — exactly like `withColumn()`, just batched.

For Spark < 3.3, or when you need to drop or reorder columns at the same time, use the `select()` approach from point 3.

**7. Column pruning is not guaranteed when UDFs are involved**

Spark's Catalyst optimizer cannot look inside a Python UDF to determine which columns it actually reads. When a UDF is present in the plan, Spark conservatively keeps all columns in scope — breaking column pruning even if you have a `select()` earlier.

```python
import pyspark.sql.functions as F
import pyspark.sql.types as T

@F.udf(returnType=T.StringType())
def my_udf(val):
    return val.upper()

# Column pruning may not fire here — Spark keeps all columns to be safe
df.select("id", my_udf(F.col("name")).alias("name_upper")).explain(True)
```

Mitigation: project to only the columns the UDF needs *before* calling it, and project away the rest immediately *after*:

```python
df.select("id", "name") \
  .select("id", my_udf("name").alias("name_upper"))
```

**8. Re-select immediately after joins to enforce a schema contract**

Joins inflate the column count — both DataFrames' columns appear in the result, often with duplicate names. An explicit `select()` right after a join prevents wide intermediate results from flowing through subsequent shuffles and makes the output schema clear:

```python
import pyspark.sql.functions as F

result = (
    orders.join(customers, on="customer_id", how="left")
    .select(
        "order_id",
        "amount",
        F.col("customers.country").alias("country"),  # F.col() for two reasons: (1) .alias() is a Column method — plain strings don't have it; (2) the dot notation resolves the post-join DataFrame qualifier, not a struct field
    )
)
```

Without this, all columns from both sides travel through every downstream operation.

**9. Use `df.explain(True)` to verify your select() is pruning columns**

After building a pipeline, check the physical plan to confirm Spark is only reading the columns you asked for:

```python
df.explain(True)
```

In the physical plan, `ReadSchema` lists the columns Spark actually reads from disk. If it contains columns absent from your `select()`, something upstream — a UDF, `select("*")`, or a complex expression — is blocking pruning. Fix it and re-run `explain()` to confirm.

### Splitting strings — `split()`

`split()` takes a string column and a Java regex delimiter, and returns an array column:

```python
import pyspark.sql.functions as F

lines = book.select(F.split("value", " ").alias("line"))
lines.printSchema()
# root
#  |-- line: array (nullable = true)
#  |    |-- element: string (containsNull = true)
```

Each row now contains an array of words. The original `"hello world"` → `["hello", "world"]`.

> 💡 **Note** — PySpark uses **Java regular expressions** in built-in functions like `split()` and `regexp_extract()`, not Python's `re` module syntax. They're very similar but not identical.

### Best practices for `split()`

**1. Use the `limit` parameter when you only need the first N parts**

`limit` caps the number of splits — the last element absorbs any remaining text. Avoids building a large array when you only need the head:

```python
import pyspark.sql.functions as F

# "user@domain.com@extra" → ["user", "domain.com@extra"]  (stops after 1 split)
df.select(F.split("email", "@", limit=2).alias("parts"))
```

> 📌 **Version note (Spark 3.0)** — the `limit` parameter was added in Spark 3.0 with a default of `-1`. Before 3.0 there was no `limit` parameter and the behavior matched Java's `String.split(pattern, 0)`, which discards trailing empty strings. With `-1` as the default, trailing empty strings are now preserved. Be explicit if trailing empties matter.

**2. Use `split_part()` when you only need one element — Spark 3.3+**

`split_part(str, delimiter, part_num)` directly returns the Nth segment as a string — more efficient and readable than `split()[n]`, which builds the full array first and then indexes into it:

```python
# split() — 0-indexed array access
df.select(F.split("email", "@")[0].alias("username"))   # [0] = first element

# split_part() — 1-indexed, like SQL SPLIT_PART
df.select(F.split_part("email", "@", 1).alias("username"))  # 1 = first element
```

> ⚠️ **Index mismatch trap** — `split()[0]` and `split_part(..., 1)` both return the first element, but the indexes differ by 1. Mixing them up silently returns the wrong field.

**3. Escape regex metacharacters in literal delimiters**

The `pattern` argument is a Java regex. Characters like `.`, `|`, `(`, `)`, `+`, `*`, `?` are metacharacters — they must be escaped when you want them treated literally. Use a raw string `r"\."` (preferred) or a regular string `"\\."` — both pass the two-character sequence `\.` to Java regex:

```python
# Wrong — "." means "any character" in regex; splits on every character
df.select(F.split("version", ".").alias("parts"))

# Correct — raw string (preferred)
df.select(F.split("version", r"\.").alias("parts"))

# Also correct — double-escaped regular string
df.select(F.split("version", "\\.").alias("parts"))
```

Common literals that need escaping: `.` `|` `(` `)` `[` `]` `{` `}` `+` `*` `?` `^` `$`

**4. Escape backslashes in Python strings for Java regex**

Java regex `\d` (digit) must be written as `"\\d"` in a Python string — Python consumes one backslash, leaving `\d` for Java. Use a raw string `r"\d"` to avoid the confusion:

```python
# Wrong — Python sees "\d", sends "d" to Java regex
F.split("col", "\d")

# Correct — raw string sends "\d" to Java regex
F.split("col", r"\d")
```

**5. Filter empty strings from leading/trailing delimiters**

Splitting `" hello world "` on `" "` produces `["", "hello", "world", ""]` — leading and trailing delimiters yield empty-string array elements. After `explode()`, these become empty-string rows that usually need filtering:

```python
(
    df
    .select(F.explode(F.split("text", " ")).alias("word"))
    .filter(F.col("word") != "")
)
```

**6. Prefer `split()` + `explode()` over Python UDFs for tokenisation**

Splitting and exploding with built-in functions runs on the JVM — no Python serialisation overhead. Reach for a UDF only when the tokenisation logic can't be expressed with built-in functions.

### Renaming columns — `alias()` vs `withColumnRenamed()`

When a transformation creates a column, PySpark auto-generates a name like `split(value, , -1)`. Always rename:

```python
# alias() — chains directly on the column expression inside select()
lines = book.select(F.split("value", " ").alias("line"))

# withColumnRenamed() — renames an existing column on the whole DataFrame
# Useful when you don't want to rewrite the select(); no-op if column doesn't exist
lines = book.select(F.split("value", " "))
lines = lines.withColumnRenamed("split(value,  , -1)", "line")
```

**Rule of thumb:** use `alias()` when you're already inside a `select()` or column expression; use `withColumnRenamed()` when renaming without changing the rest of the DataFrame.

### Exploding arrays into rows — `explode()`

After splitting, each row holds a list of words. `explode()` unrolls that list — one element per row:

```python
words = lines.select(F.explode("line").alias("word"))
words.show(5)
# +--------+
# |    word|
# +--------+
# |     The|
# | Project|
# |Gutenberg|
# ...
```

Visually: `["This", "is", "a", "list"]` → four rows, each holding one string.

### Best practices for `explode()`

**1. Know the four variants**

| Function | Empty/null arrays | Position column |
|---|---|---|
| `F.explode()` | Row dropped | No |
| `F.explode_outer()` | Row kept as `null` | No |
| `F.posexplode()` | Row dropped | Yes (`pos`) |
| `F.posexplode_outer()` | Row kept as `null` | Yes (`pos`) |

**2. `explode()` silently drops rows with null or empty arrays — use `explode_outer()` when that matters**

```python
import pyspark.sql.functions as F

# Rows where tags is null or [] are silently lost
df.select("id", F.explode("tags").alias("tag"))

# Rows with null/empty tags become a single row with tag = null
df.select("id", F.explode_outer("tags").alias("tag"))
```

This is the most common `explode()` bug — a join later produces fewer rows than expected because source rows were quietly dropped.

**3. Only one explode per `select()` — use chained selects for multiple arrays**

Spark raises an error if you use more than one generator (explode, posexplode, inline) in the same `select()`:

```python
# Error — two generators in one select
df.select(F.explode("tags").alias("tag"), F.explode("categories").alias("cat"))

# Correct — chain separate selects
df.select("id", "categories", F.explode("tags").alias("tag")) \
  .select("id", "tag", F.explode("categories").alias("cat"))
```

**4. Filter and prune before exploding**

`explode()` multiplies rows by the array length — a row with 1,000 elements becomes 1,000 rows. Reduce data first:

```python
# Bad — explodes everything, filters after
df.select(F.explode("events").alias("event")).filter(F.col("event.type") == "click")

# Good — filter the array inline before exploding
df.select(F.explode(F.filter("events", lambda e: e["type"] == "click")).alias("event"))
```

**5. Consider higher-order functions instead of explode for aggregations**

If you only need to aggregate or transform an array without creating one row per element, higher-order functions avoid the row explosion entirely:

```python
# Explode approach — creates N rows then aggregates back
df.select("id", F.explode("scores").alias("score")) \
  .groupBy("id").agg(F.avg("score"))

# Higher-order — stays at one row per record
df.select("id", F.aggregate("scores", F.lit(0.0), lambda acc, x: acc + x).alias("total"))
```

Higher-order functions (`F.transform()`, `F.filter()`, `F.aggregate()`, `F.exists()`) are generally faster for array operations that don't genuinely require row-level expansion.

### Exploding with position — `posexplode()`

`posexplode()` is `explode()` with an extra `pos` column prepended — the zero-based index of each element in the original array:

```python
import pyspark.sql.functions as F

data = [("alice", ["python", "spark", "sql"])]
df = spark.createDataFrame(data, ["name", "skills"])

df.select("name", F.posexplode("skills").alias("pos", "skill")).show()
# +-----+---+------+
# | name|pos| skill|
# +-----+---+------+
# |alice|  0|python|
# |alice|  1| spark|
# |alice|  2|   sql|
# +-----+---+------+
```

`pos` is useful when:

- **Order matters** — e.g. the first `skill` is the primary one, the rest are secondary
- **Re-joining back** — you need to re-assemble or compare elements by their original position
- **Debugging** — confirming array contents line up with expectations

Because it yields two columns (`pos` and `skill`) simultaneously, it must go in `select()` — `withColumn()` only accepts one column at a time.

### Best practices for `posexplode()`

**1. Always alias both output columns**

`posexplode()` produces two unnamed columns (`pos`, `col`) by default. Always provide explicit aliases:

```python
# Bad — output columns are named pos and col (generic)
df.select(F.posexplode("skills"))

# Good — names reflect the data
df.select(F.posexplode("skills").alias("pos", "skill"))
```

**2. Use `posexplode_outer()` when null/empty arrays must be preserved**

`posexplode()` drops rows with null or empty arrays, just like `explode()`. Use `posexplode_outer()` to keep them as `pos=null, skill=null`:

```python
df.select("name", F.posexplode_outer("skills").alias("pos", "skill"))
```

**3. `pos` is zero-based — adjust if you need one-based ranks**

```python
df.select(
    "name",
    F.posexplode("skills").alias("pos", "skill")
).withColumn("rank", F.col("pos") + 1)
```

**4. Generator rule still applies — one `posexplode()` per `select()`**

`posexplode()` is a generator like `explode()`. You cannot use it alongside another generator in the same `select()`. Chain selects if you need to expand two arrays.

### Flattening arrays of structs — `inline()`

When each array element is a struct, `inline()` expands the array *and* flattens the struct fields into separate columns in one step:

```python
import pyspark.sql.functions as F
import pyspark.sql.types as T

schema = T.StructType([
    T.StructField("user", T.StringType()),
    T.StructField("events", T.ArrayType(T.StructType([
        T.StructField("ts", T.StringType()),
        T.StructField("type", T.StringType()),
        T.StructField("value", T.IntegerType()),
    ]))),
])

data = [("alice", [("2024-01-01", "click", 1), ("2024-01-02", "buy", 50)])]
df = spark.createDataFrame(data, schema)

# Verbose — explode, then extract each struct field manually
df.select(F.explode("events").alias("e")) \
  .select(F.col("e.ts"), F.col("e.type"), F.col("e.value"))

# Clean — inline flattens struct fields directly into columns
df.select("user", F.inline("events")).show()
# +-----+----------+-----+-----+
# | user|        ts| type|value|
# +-----+----------+-----+-----+
# |alice|2024-01-01|click|    1|
# |alice|2024-01-02|  buy|   50|
# +-----+----------+-----+-----+
```

Use `F.inline_outer()` to preserve rows with null/empty arrays (same null behaviour as `explode_outer()`).

### Best practices for `inline()`

**1. Prefer `inline()` over `explode()` + field access for arrays of structs**

`explode()` produces a single struct column that you must then unpack field-by-field. `inline()` does both in one call — less code, one fewer step in the plan.

**2. `inline()` is a generator — same one-per-select rule as `explode()`**

You cannot use more than one generator in the same `select()`. Chain a second `select()` if you need to inline a second array column.

**3. Schema must be an `ArrayType(StructType)` — not an array of primitives**

`inline()` only works when the array elements are structs. For arrays of strings, ints, or other primitives, use `explode()` instead. Passing a primitive array to `inline()` raises an `AnalysisException`.

**4. Column names come from the struct field names — rename early if they clash**

`inline()` uses the struct's field names as output column names. If those names collide with existing columns in the DataFrame, rename them with `.alias()` inside the `inline()` call or with a `select()` immediately after.

```python
import pyspark.sql.functions as F
import pyspark.sql.types as T

# The outer DataFrame already has a "type" column.
# The events struct also has a "type" field — inline() would produce two "type" columns.
schema = T.StructType([
    T.StructField("user", T.StringType()),
    T.StructField("type", T.StringType()),          # clash: same name as events.type
    T.StructField("events", T.ArrayType(T.StructType([
        T.StructField("ts", T.StringType()),
        T.StructField("type", T.StringType()),
        T.StructField("value", T.IntegerType()),
    ]))),
])

data = [("alice", "premium", [("2024-01-01", "click", 1)])]
df = spark.createDataFrame(data, schema)

# Bad — produces two columns both named "type"; downstream references are ambiguous
df.select("user", "type", F.inline("events")).show()

# Good — rename the clashing field immediately after inline
df.select("user", "type", F.inline("events")) \
  .withColumnRenamed("type", "event_type") \
  .show()
# +-----+-------+----------+----------+-----+
# | user|   type|        ts|event_type|value|
# +-----+-------+----------+----------+-----+
# |alice|premium|2024-01-01|     click|    1|
# +-----+-------+----------+----------+-----+
```

### Higher-order array functions — overview

Higher-order functions operate on array columns *in place* — no row explosion, no shuffle, one row in and one row out. Use them whenever you need to transform, filter, or inspect an array without needing the elements as separate rows.

| Function | Input | Output | Use when |
|---|---|---|---|
| `F.transform()` | array | array (same length) | apply a function to every element |
| `F.filter()` | array | array (shorter or equal) | keep only elements that match a condition |
| `F.aggregate()` | array | scalar | reduce an array to one value |
| `F.exists()` | array | boolean | check if any element satisfies a condition |

All four take a lambda expression. The lambda receives one element at a time (plus an accumulator for `aggregate()`).

### `F.transform()` — map a function over an array

Applies a function to every element and returns a new array of the same length:

```python
import pyspark.sql.functions as F

data = [("alice", [1, 2, 3])]
df = spark.createDataFrame(data, ["name", "scores"])

df.select("name", F.transform("scores", lambda x: x * 10).alias("scaled")).show()
# +-----+----------+
# | name|    scaled|
# +-----+----------+
# |alice|[10,20,30]|
# +-----+----------+
```

Use `transform()` instead of explode + `withColumn` + `groupBy` whenever the output is still a per-element array — it's faster and keeps the row structure intact.

### `F.filter()` — keep elements that match a condition

Returns a new array containing only the elements for which the lambda returns `True`:

```python
import pyspark.sql.functions as F

data = [("alice", ["python", "spark", "java", "sql"])]
df = spark.createDataFrame(data, ["name", "skills"])

df.select(
    "name",
    F.filter("skills", lambda x: x != "java").alias("skills")
).show()
# +-----+-------------------+
# | name|             skills|
# +-----+-------------------+
# |alice|[python, spark, sql]|
# +-----+-------------------+
```

The output array can be empty `[]` if no elements match — it is never `null`. To check whether the result is empty use `F.size(col) == 0`.

> 💡 **Tip** — `F.filter()` inside `F.explode()` (as seen in the explode best practices) is a common combo: filter the array first to reduce row count, then explode only the elements you need.

### `F.aggregate()` — reduce an array to a scalar

Reduces an array to a single value by applying a merge function with a running accumulator:

```python
import pyspark.sql.functions as F

data = [("alice", [10, 20, 30])]
df = spark.createDataFrame(data, ["name", "scores"])

df.select(
    "name",
    F.aggregate("scores", F.lit(0), lambda acc, x: acc + x).alias("total")
).show()
# +-----+-----+
# | name|total|
# +-----+-----+
# |alice|   60|
# +-----+-----+
```

Signature: `F.aggregate(col, initialValue, mergeFunction, finishFunction=None)`. The optional `finishFunction` transforms the final accumulator — useful for computing a mean:

```python
F.aggregate(
    "scores",
    F.struct(F.lit(0).alias("sum"), F.lit(0).alias("count")),
    lambda acc, x: F.struct((acc["sum"] + x).alias("sum"), (acc["count"] + 1).alias("count")),
    lambda acc: acc["sum"] / acc["count"]
).alias("mean")
```

Use `aggregate()` instead of explode + `groupBy().agg()` when the aggregation is per-row (not across rows) — it avoids a shuffle entirely.

### `F.exists()` — check if any element satisfies a condition

Returns `True` if at least one element in the array satisfies the lambda, `False` otherwise:

```python
import pyspark.sql.functions as F

data = [("alice", ["python", "spark", "sql"]), ("bob", ["java", "c++"])]
df = spark.createDataFrame(data, ["name", "skills"])

df.select(
    "name",
    F.exists("skills", lambda x: x == "spark").alias("knows_spark")
).show()
# +-----+-----------+
# | name|knows_spark|
# +-----+-----------+
# |alice|       true|
# |  bob|      false|
# +-----+-----------+
```

For the inverse — *all* elements satisfy a condition — use `F.forall()`:

```python
F.forall("scores", lambda x: x > 0).alias("all_positive")
```

`exists()` short-circuits: once a matching element is found, the remaining elements are not evaluated.

### Lowercasing — `lower()`

```python
words_lower = words.select(F.lower("word").alias("word_lower"))
```

`"Prejudice,"` → `"prejudice,"`

### Removing punctuation — `regexp_extract()`

Keep only the first contiguous run of lowercase letters:

```python
words_clean = words_lower.select(
    F.regexp_extract("word_lower", "[a-z]+", 0).alias("word")
)
```

- Pattern `[a-z]+` matches one or more lowercase ASCII letters.
- The `0` argument extracts group 0 (the whole match).
- `"prejudice,"` → `"prejudice"`. An empty string `""` is left for rows that contained only punctuation.

> 💡 **Regex resource** — [regexr.com](https://regexr.com/) is excellent for testing Java/JavaScript-compatible regexes interactively.

### Best practices for `regexp_extract()`

**1. Use capturing groups to extract a specific part, not the whole match**

Group `0` returns the entire match. Use a numbered capturing group `(...)` to extract only the part you care about:

```python
import pyspark.sql.functions as F

# Group 0 — whole match: "2024-01-15"
F.regexp_extract("ts", r"\d{4}-\d{2}-\d{2}", 0)

# Group 1 — just the year: "2024"
F.regexp_extract("ts", r"(\d{4})-\d{2}-\d{2}", 1)
```

**2. A non-matching row returns `""` — not `null`**

This is the most common `regexp_extract()` surprise. Rows where the pattern doesn't match produce an empty string, not `null`. Filter or replace explicitly if you need to distinguish "no match" from "empty":

```python
import pyspark.sql.functions as F

extracted = df.withColumn("year", F.regexp_extract("ts", r"(\d{4})", 1))

# Replace "" with null so downstream aggregations ignore non-matches
extracted.withColumn(
    "year",
    F.when(F.col("year") == "", None).otherwise(F.col("year"))
)
```

**3. PySpark uses Java regex — not Python `re` syntax**

The pattern is compiled by the JVM. Almost everything works the same, but watch for:

- Use raw strings (`r"..."`) to avoid double-escaping backslashes.
- Named groups (`(?P<name>...)`) are Python-only — Java uses `(?<name>...)` syntax.
- Lookaheads/lookbehinds work in Java regex; possessive quantifiers (`x++`) do not exist in Python `re` but do in Java.

**4. Use `regexp_extract_all()` when a string can have multiple matches**

`regexp_extract()` returns only the *first* match. Use `F.regexp_extract_all()` (Spark 3.1+) to get all matches as an array:

```python
import pyspark.sql.functions as F

# "order #123 and #456" → ["123", "456"]
F.regexp_extract_all("text", r"#(\d+)", 1).alias("order_ids")
```

**5. Prefer `regexp_replace()` for substitution, `regexp_extract()` for extraction**

`regexp_extract()` pulls a value out; `regexp_replace()` rewrites the string. Don't use extract-then-reconstruct when replace does it in one step:

```python
# Awkward — extract then re-wrap
F.concat(F.lit("["), F.regexp_extract("x", r"\w+", 0), F.lit("]"))

# Direct
F.regexp_replace("x", r"(\w+)", r"[\1]")
```

---

## 6. Filtering rows

`filter()` and `where()` are identical — PySpark exposes both to reduce friction for users coming from different backgrounds. Either keeps only rows where the condition is `True`.

```python
import pyspark.sql.functions as F

words_nonull = words_clean.filter(F.col("word") != "")

# Equivalently:
words_nonull = words_clean.where(F.col("word") != "")
```

Useful operators inside filter/where:

```python
F.col("x") != ""              # not equal
F.col("x") > 3                # greater than
F.col("x").isin(["a", "b"])   # membership
~(F.col("x") == "")           # negation with ~ operator
```

> 💡 **Tip** — Don't stress about filtering "too late" in the chain. Because Spark is lazy, it can push filter predicates earlier in the physical plan automatically. Write filters where they're most *readable*, and let the optimizer handle placement.

---

## 7. The full Chapter 2 pipeline (steps 1–3)

```python
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.appName("word_count").getOrCreate()

book = spark.read.text("./data/gutenberg_books/1342-0.txt")

words_nonull = (
    book
    .select(F.split("value", " ").alias("line"))                  # tokenise
    .select(F.explode("line").alias("word"))                      # one word per row
    .select(F.lower("word").alias("word_lower"))                  # lowercase
    .select(F.regexp_extract("word_lower", "[a-z]+", 0)          # strip punctuation
            .alias("word"))
    .filter(F.col("word") != "")                                  # drop empties
)
```

Nothing executes until an action (like `show()`) is called. Chapter 3 adds `groupBy().count()` and `orderBy()` to complete steps 4 and 5.

---

## 8. Summary

- Every PySpark program follows **Read → Transform → Export**.
- The **`pyspark` shell** gives a pre-configured REPL with `spark` (SparkSession) and `sc` (SparkContext) ready to use. For scripts, create `SparkSession` with the builder pattern.
- **DataFrames** are the primary data structure — typed, columnar, SQL-inspired.
- **`spark.read`** (DataFrameReader) ingests files: `.text()`, `.csv()`, `.json()`, `.parquet()`.
- **`printSchema()` + `show()`** are the exploration workhorses.
- **`select()`** returns a new DataFrame with chosen columns or column expressions. Prefer `F.col("name")` (with `import pyspark.sql.functions as F`) for portability.
- **`pyspark.sql.functions`** (`import … as F`) is the library of built-in column functions: `F.split()`, `F.explode()`, `F.lower()`, `F.regexp_extract()`, and hundreds more. These map to JVM implementations and run at full Spark speed.
- **`alias()`** renames a column inside a select; **`withColumnRenamed()`** renames on the whole DataFrame.
- **`filter()` / `where()`** are identical — keep rows where the Boolean column expression is `True`.
- The full transformation chain is lazy: no data moves until an **action** (`show()`, `write()`, `count()`) is called.

---

## 9. References

- PySpark SQL functions API — <https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html>
- Book source code (Ch 2) — <http://mng.bz/6ZOR>
- RegExr (Java-compatible regex tester) — <https://regexr.com/>
