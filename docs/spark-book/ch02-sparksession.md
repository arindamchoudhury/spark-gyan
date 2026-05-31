# Chapter 02 — SparkSession and Entry Points

> *Learning-path topic: B2 (Beginner)*
> *Written: 2026-05-31 · Spark 4.1.x / Python 3.10+*

Every PySpark program begins with a `SparkSession`. Understanding how to create it, configure it, and control what it logs is a prerequisite for everything else. This chapter also covers Spark 4.x's new default architecture — Spark Connect — and when to use classic mode instead.

---

## What you'll learn

- How to create a `SparkSession` with the builder pattern
- What `getOrCreate()` does and when it silently ignores config
- How to configure log verbosity so Spark stops drowning your output
- The difference between classic mode and Spark Connect in Spark 4.x
- How to connect to a remote cluster

---

## The problem this solves

You open a Spark script and immediately see 200 lines of `INFO` logging before your first line of output. Or you try to change a config setting mid-session and nothing happens. Or your app crashes at startup because the `pyspark` shell is trying to connect to a server that doesn't exist. All of these are SparkSession setup problems, and they are almost universally encountered in the first hour of learning PySpark.

---

## Core concept

`SparkSession` is the unified entry point for all PySpark operations introduced in Spark 2.0. It supersedes the older `SparkContext` / `SQLContext` / `HiveContext` split — one object now gives you DataFrames, SQL, streaming, and ML all in one place.

You build a session with a fluent builder pattern: chain configuration methods on `SparkSession.builder`, then call `.getOrCreate()` to materialise it. The critical detail is that `getOrCreate()` checks the running JVM process first. If a session already exists, it returns that one and **ignores every `.config()` call on the builder**. This catches many beginners off guard in notebooks and REPLs — to force a fresh session with new config, call `spark.stop()` first.

Spark 4.x changes the default model. The `pyspark` shell now starts in **Spark Connect** mode: your Python process communicates with a separate Spark Connect server over gRPC rather than embedding the Spark driver in the same process. For interactive scripts run with `spark-submit`, classic mode still applies. Understanding which mode you're in prevents confusing startup failures.

---

## Examples

### Minimal example: create a local SparkSession

```python
# Apache Spark 4.1.x / PySpark 4.1.x · Python 3.10+
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("my-first-app")
    .master("local[*]")       # classic mode; use all available CPU cores
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")   # silence INFO noise

print(spark.version)   # 4.1.x
```

### Building up: configuration and log control

```python
# Apache Spark 4.1.x / PySpark 4.1.x · Python 3.10+
import os
from pyspark.sql import SparkSession

# Fix network binding for laptops with VPN or Docker
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

spark = (
    SparkSession.builder
    .appName("configured-app")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "8")      # reduce from default 200 for local work
    .config("spark.ui.port", "4041")                   # avoid port 4040 conflict with Docker
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Verify config was applied
print(spark.conf.get("spark.sql.shuffle.partitions"))   # 8
```

### Spark Connect (Spark 4.x default shell)

```python
# Spark Connect — connects to a running Spark Connect server
# Start server first: ./sbin/start-connect-server.sh
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .remote("sc://localhost:15002")
    .getOrCreate()
)

# DataFrame API is identical — only the transport layer changes
spark.range(5).show()
# +---+
# | id|
# +---+
# |  0|
# |  1|
# |  2|
# |  3|
# |  4|
# +---+
```

---

## Common pitfalls

- **`getOrCreate()` ignoring your `.config()` calls** — if a session already exists in the process (notebook kernel, REPL), `getOrCreate()` returns it unchanged. Call `spark.stop()` first, then rebuild.
- **`spark.close()` doesn't exist** — the correct method to shut down a session is `spark.stop()`. `close()` raises `AttributeError`.
- **Spark Connect server not running** — in Spark 4.x, the `pyspark` shell defaults to Connect mode and attempts port 15002 at startup. If no server is running, it fails immediately. Use `pyspark --master local[*]` or `SparkSession.builder.master("local[*]")` to force classic mode.
- **Changing `shuffle.partitions` after the session starts** — some configs are only read at session creation time. For runtime-settable configs (like `spark.sql.shuffle.partitions`), use `spark.conf.set(key, value)` to change them mid-session.
- **Too many shuffle partitions locally** — the default `spark.sql.shuffle.partitions` is 200, designed for large clusters. On a laptop with 8 cores processing a small dataset, 200 shuffle tasks are pure overhead. Set it to `2 × CPU cores` (e.g. 8–16) for local development.

---

## Exercises

1. **Recall** — You run a notebook cell that calls `SparkSession.builder.config("spark.sql.shuffle.partitions", "4").getOrCreate()`. A session already exists. What value does `spark.conf.get("spark.sql.shuffle.partitions")` return after that cell runs?

2. **Apply** — Create a `SparkSession`, print `spark.conf.get("spark.sql.shuffle.partitions")`, then use `spark.conf.set()` to change it to `16`. Print it again to verify. What does this confirm about runtime vs. build-time configuration?

3. **Extend** — Investigate the difference between `master("local")`, `master("local[2]")`, and `master("local[*]")` by creating sessions with each and checking `spark.sparkContext.defaultParallelism`. What drives the parallelism value in each case?

---

## Summary

- `SparkSession` is the single entry point for all PySpark operations; built with `SparkSession.builder.getOrCreate()`.
- `getOrCreate()` returns an existing session if one is running — `.config()` calls are silently ignored in that case.
- Set `SPARK_LOCAL_IP=127.0.0.1` before session creation on laptops with VPN or Docker to avoid network binding errors.
- Set `spark.sql.shuffle.partitions` to `2 × CPU cores` for local work; the default 200 is for large clusters.
- Spark 4.x defaults the `pyspark` shell to Spark Connect; use `.master("local[*]")` to force classic mode in scripts.
- Chapter 3 builds on this by introducing the DataFrame API — the primary tool for data manipulation.

---

## References

- [PySpark SparkSession API](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.html)
- [Spark Connect overview](https://spark.apache.org/docs/latest/spark-connect-overview.html)
- [Spark configuration reference](https://spark.apache.org/docs/latest/configuration.html)
