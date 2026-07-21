"""Capability taxonomy and keyword-based area tagging."""
from pathlib import Path

# (slug, display name) — order = page order on the index.
AREAS = [
    ("core-rdd", "Core / RDD / Scheduler"),
    ("sql-catalyst", "SQL & Catalyst"),
    ("ansi-types", "ANSI & Data Types"),
    ("builtin-functions", "Built-in Functions"),
    ("datasources-dsv2", "Data Sources & DSv2"),
    ("connectors", "Connectors (Kafka/JDBC/Parquet/ORC/Avro)"),
    ("structured-streaming", "Structured Streaming"),
    ("dstreams", "DStreams (legacy streaming)"),
    ("pyspark", "PySpark & Python UDFs"),
    ("pandas-on-spark", "pandas API on Spark"),
    ("arrow", "Arrow"),
    ("spark-connect", "Spark Connect"),
    ("mllib", "MLlib / ML"),
    ("graphx", "GraphX"),
    ("sparkr", "SparkR"),
    ("deploy", "Deploy (Standalone/YARN/Mesos/K8s)"),
    ("shuffle-storage", "Shuffle / Storage / Memory"),
    ("web-ui", "Web UI / History / Metrics"),
    ("security", "Security"),
    ("geospatial", "Geospatial"),
    ("build-lang", "Build & Language support"),
    ("misc", "Misc / Other"),
]

# Ordered rules: FIRST match wins. Put specific/narrow areas before broad ones
# (e.g. pandas-on-spark and spark-connect before generic pyspark/sql).
_RULES = [
    ("geospatial", ["st_", "geospatial", "geometry", "geography", " srid", "wkb", "wkt"]),
    ("spark-connect", ["spark connect", "connect client", "connect server", "connect: "]),
    ("pandas-on-spark", ["pandas api on spark", "pandas-on-spark", "koalas", "ps.dataframe"]),
    ("arrow", ["arrow-optimized", "pyarrow", " arrow ", "arrow-based", "arrow ipc"]),
    ("structured-streaming", ["structured streaming", "streaming query", "watermark",
                               "continuous processing", "statestore", "state store",
                               "foreachbatch", "microbatch", "micro-batch", "kafka source"]),
    ("dstreams", ["dstream", "receiver", "streaming context", "kinesis receiver"]),
    ("pandas-on-spark", ["pandas udf", "pandas_udf", "grouped map", "cogroup"]),
    ("pyspark", ["pyspark", "python udf", "python worker", "python api", "py4j"]),
    ("sparkr", ["sparkr", " r api", "r udf", "r dataframe", "dapply", "gapply"]),
    ("mllib", ["mllib", " ml ", "ml.", "estimator", "transformer", "classifier",
                "regression", "clustering", "feature transformer", "pipeline stage"]),
    ("graphx", ["graphx", "pregel", "graphframe", "connected components"]),
    ("connectors", ["kafka", "jdbc", "parquet", "orc", "avro", "csv datasource",
                     "json datasource", "hive metastore", "thrift"]),
    ("datasources-dsv2", ["data source v2", "datasource v2", "dsv2", "datasourcev2",
                           "table catalog", "catalog api", "file source", "partition stats"]),
    ("ansi-types", ["ansi", "interval type", "timestamp_ntz", "decimal", "char/varchar",
                     "variant type", "data type", "collation"]),
    ("builtin-functions", ["built-in function", "builtin function", "add function",
                            "sql function", "add expression", "aggregate function"]),
    ("security", ["security", "authentication", "encryption", "kerberos", "acl",
                   "ssl", "tls", "token", "credential"]),
    ("web-ui", ["web ui", " ui ", "history server", "metrics", "rest api", "prometheus",
                 "event log"]),
    ("shuffle-storage", ["shuffle", "memory management", "off-heap", "spill",
                          "block manager", "storage level", "external shuffle"]),
    ("deploy", ["yarn", "mesos", "kubernetes", "k8s", "standalone", "cluster manager",
                 "deploy mode", "resource manager", "executor pod"]),
    ("build-lang", ["scala 2.1", "scala 2.13", "java 1", "java 17", "java 21", "build",
                     "maven", "sbt", "upgrade to scala", "python 3."]),
    ("sql-catalyst", ["sql", "catalyst", "dataframe", "dataset", "optimizer",
                       "query plan", "adaptive query", "join", "aggregate", "subquery"]),
    ("core-rdd", ["rdd", "scheduler", "task", "dag", "accumulator", "broadcast",
                   "serializer", "closure", "partition"]),
]

def load_overrides(path) -> dict[str, str]:
    result: dict[str, str] = {}
    p = Path(path)
    if not p.exists():
        return result
    for line in p.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        sid, _, area = line.partition("\t")
        if sid and area:
            result[sid.strip()] = area.strip()
    return result

def assign_area(spark_id, title, overrides) -> str:
    if spark_id and spark_id in overrides:
        return overrides[spark_id]
    hay = f" {title.lower()} "
    for slug, keywords in _RULES:
        if any(kw in hay for kw in keywords):
            return slug
    return "misc"
