# GraphX

> Source: `release-notes/spark_all_changelogs.txt`. Timeline rows below are generated from `_catalog.jsonl`; prose outside the AUTO markers is hand-written.

## How it evolved

### 0.x era — origins

Before GraphX, graph processing lived in Bagel, Spark's Pregel-inspired module; 0.7.2 shipped performance fixes and a configurable storage level for it. GraphX itself arrived in 0.9.0 as a new alpha framework for graph-parallel computation, letting users build graphs from arbitrary Spark RDDs using standard operators, then transform them or extract subgraphs with graph-specific operators.

0.9.0 also delivered an optimized Pregel API that took advantage of graph partitioning and indexing, a set of standard algorithms (PageRank, connected components, strongly connected components, SVD++, triangle counting), and interactive use directly from the Spark shell.

## Timeline

<!-- AUTO:timeline START -->
| Release | JIRA | Type | Title |
|---|---|---|---|
| 0.7.2 | — | prose | Bagel improvements: performance fixes and configurable storage level |
| 0.9.0 | — | prose | GraphX graph-processing framework introduced (alpha) |
| 0.9.0 | — | prose | GraphX: build graphs from arbitrary Spark RDDs |
| 0.9.0 | — | prose | GraphX: basic operations to transform graphs or extract subgraphs |
| 0.9.0 | — | prose | GraphX: optimized Pregel API using graph partitioning and indexing |
| 0.9.0 | — | prose | GraphX standard algorithms: PageRank, connected/strongly-connected components, SVD++, triangle counting |
| 0.9.0 | — | prose | GraphX interactive use from the Spark shell |
| 1.5.0 | [SPARK-9436](https://issues.apache.org/jira/browse/SPARK-9436) | Improvement | Simplify Pregel by merging joins |
| 1.6.0 | [SPARK-10682](https://issues.apache.org/jira/browse/SPARK-10682) | Improvement | Remove Bagel test suites |
| 2.0.0 | [SPARK-13816](https://issues.apache.org/jira/browse/SPARK-13816) | Improvement | Add parameter checks for algorithms in Graphx |
| 2.0.0 | [SPARK-16345](https://issues.apache.org/jira/browse/SPARK-16345) | Improvement | Extract graphx programming guide example snippets from source files instead of hard code them |
| 2.1.0 | [SPARK-11496](https://issues.apache.org/jira/browse/SPARK-11496) | New Feature | Parallel implementation of personalized pagerank |
| 2.1.0 | [SPARK-16478](https://issues.apache.org/jira/browse/SPARK-16478) | Improvement | strongly connected components doesn't cache returned RDD |
| 2.1.0 | [SPARK-17171](https://issues.apache.org/jira/browse/SPARK-17171) | Improvement | DAG will list all partitions in the graph |
| 2.1.0 | [SPARK-18428](https://issues.apache.org/jira/browse/SPARK-18428) | Improvement | Update docs for GraphX |
| 3.0.0 | [SPARK-29877](https://issues.apache.org/jira/browse/SPARK-29877) | Improvement | static PageRank allow checkPoint from previous computations |
| 3.2.0 | [SPARK-35357](https://issues.apache.org/jira/browse/SPARK-35357) | Improvement | Allow to turn off the normalization applied by static PageRank utilities |
<!-- AUTO:timeline END -->
