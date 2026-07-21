# GraphX

> Source: `release-notes/spark_all_changelogs.txt`. Timeline rows below are generated from `_catalog.jsonl`; prose outside the AUTO markers is hand-written.

## How it evolved

### 0.x era — origins

Before GraphX, graph processing lived in Bagel, Spark's Pregel-inspired module; 0.7.2 shipped performance fixes and a configurable storage level for it. GraphX itself arrived in 0.9.0 as a new alpha framework for graph-parallel computation, letting users build graphs from arbitrary Spark RDDs using standard operators, then transform them or extract subgraphs with graph-specific operators.

0.9.0 also delivered an optimized Pregel API that took advantage of graph partitioning and indexing, a set of standard algorithms (PageRank, connected components, strongly connected components, SVD++, triangle counting), and interactive use directly from the Spark shell.

### 1.x era — GraphX graduates to a stable API

1.0.0 brought substantial performance boosts to graph loading, edge reversal, and neighborhood computation, needing less communication and producing simpler RDD lineages. 1.1.0 added custom storage levels for vertices and edges, tightened numerical precision throughout, and introduced a label propagation algorithm. GraphX graduated from alpha in 1.2.0 with a stable API — guaranteeing forward compatibility — alongside a new `aggregateMessages` API replacing the deprecated `mapReduceTriplet`, and graph checkpointing with lineage truncation for long iterative jobs. 1.3.0 added conversion into a canonical edge graph, 1.4.0 added personalized PageRank (SPARK-5854), and 1.5.0 shipped a more efficient Pregel API implementation.

### 2.x era — Pregel checkpointing and steady-state tuning

GraphX saw comparatively little 2.x investment. 2.0.0 and 2.1.0 were maintenance: parameter checks for GraphX algorithms (SPARK-13816), a parallel implementation of personalized PageRank (SPARK-11496), and a fix so strongly-connected-components no longer skipped caching its returned RDD (SPARK-16478). A long-standing fix finally landed twice in the changelog: Pregel checkpointing periodically to avoid `StackOverflowError` on deep iterative jobs (SPARK-5484, recorded against both 2.2.0 and 2.3.0). 2.2.0 also improved PageRank's initial value for faster convergence (SPARK-18845), and 2.3.0 closed the line with small performance improvements scattered across several GraphX operators (SPARK-21491).

### 3.x era — two tuning knobs, no new capability

GraphX's 3.x footprint is two catalog entries and no new capability. 3.0.0 let static PageRank checkpoint from a previous computation instead of always starting cold (SPARK-29877). 3.2.0 added a flag to turn off the normalization static PageRank applies by default (SPARK-35357). Both are narrow tuning knobs on an algorithm that was already mature by the 2.x line; GraphX did not gain a new operator, algorithm, or API surface anywhere from 3.0.0 through 3.5.x, consistent with a module in pure maintenance mode while graph-processing investment shifted to GraphFrames outside the core project.

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
| 1.0.0 | — | prose | GraphX performance boosts in loading, edge reversal, neighborhood computation |
| 1.1.0 | — | prose | Custom storage levels for GraphX vertices and edges |
| 1.1.0 | — | prose | Improved numerical precision across GraphX |
| 1.1.0 | — | prose | New GraphX label propagation algorithm |
| 1.2.0 | — | prose | GraphX graduates from alpha with stable API |
| 1.2.0 | — | prose | New aggregateMessages API replaces mapReduceTriplet |
| 1.2.0 | — | prose | GraphX graph checkpointing and lineage truncation |
| 1.3.0 | — | prose | GraphX adds conversion into a canonical edge graph |
| 1.4.0 | [SPARK-5854](https://issues.apache.org/jira/browse/SPARK-5854) | prose | Personalized PageRank for GraphX |
| 1.5.0 | — | prose | More efficient Pregel API implementation for GraphX |
| 1.5.0 | [SPARK-9436](https://issues.apache.org/jira/browse/SPARK-9436) | Improvement | Simplify Pregel by merging joins |
| 1.6.0 | [SPARK-10682](https://issues.apache.org/jira/browse/SPARK-10682) | Improvement | Remove Bagel test suites |
| 2.0.0 | [SPARK-13816](https://issues.apache.org/jira/browse/SPARK-13816) | Improvement | Add parameter checks for algorithms in Graphx |
| 2.0.0 | [SPARK-16345](https://issues.apache.org/jira/browse/SPARK-16345) | Improvement | Extract graphx programming guide example snippets from source files instead of hard code them |
| 2.1.0 | [SPARK-11496](https://issues.apache.org/jira/browse/SPARK-11496) | New Feature | Parallel implementation of personalized pagerank |
| 2.1.0 | [SPARK-16478](https://issues.apache.org/jira/browse/SPARK-16478) | Improvement | strongly connected components doesn't cache returned RDD |
| 2.1.0 | [SPARK-17171](https://issues.apache.org/jira/browse/SPARK-17171) | Improvement | DAG will list all partitions in the graph |
| 2.1.0 | [SPARK-18428](https://issues.apache.org/jira/browse/SPARK-18428) | Improvement | Update docs for GraphX |
| 2.2.0 | [SPARK-5484](https://issues.apache.org/jira/browse/SPARK-5484) | prose | Pregel checkpoints periodically to avoid StackOverflowError |
| 2.2.0 | [SPARK-18845](https://issues.apache.org/jira/browse/SPARK-18845) | prose | PageRank initial value improvement for faster convergence |
| 2.3.0 | [SPARK-5484](https://issues.apache.org/jira/browse/SPARK-5484) | prose | Pregel checkpoints periodically to avoid StackOverflowErrors |
| 2.3.0 | [SPARK-21491](https://issues.apache.org/jira/browse/SPARK-21491) | prose | Small performance improvement in several GraphX places |
| 3.0.0 | [SPARK-29877](https://issues.apache.org/jira/browse/SPARK-29877) | Improvement | static PageRank allow checkPoint from previous computations |
| 3.2.0 | [SPARK-35357](https://issues.apache.org/jira/browse/SPARK-35357) | Improvement | Allow to turn off the normalization applied by static PageRank utilities |
<!-- AUTO:timeline END -->
