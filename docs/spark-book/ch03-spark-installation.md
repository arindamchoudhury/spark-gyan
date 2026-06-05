# Chapter 01b — Installing Spark

> *Learning-path topic: B1 (Beginner)*
> *Written: 2026-06-05 · Spark 4.1.x / Python 3.11+*

---

## Installation

### Option 1 — pip (client/driver side only)

`pip install pyspark` bundles the Spark JARs inside the Python package — no tarball download or `SPARK_HOME` setup needed. Java 17 or 21 must still be installed separately (Spark 4.x supports only these two LTS releases).

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
| **PySpark 2.1.0 (Dec 2016)** | **Full Spark JARs bundled into the wheel** | Self-contained — no tarball needed |
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
