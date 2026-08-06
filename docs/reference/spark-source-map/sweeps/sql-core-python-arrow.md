---
subsystem: sql/core
spark_version: "4.2.0"
swept_at: 2026-08-06
group: python-arrow
all_groups: [query-execution, joins-exec, adaptive, datasources, agg-window-exchange, python-arrow, streaming-exec, classic-api, sql-scripting]
status: complete
concepts:
  - name: ExtractPythonUDFs — a UDF is not an expression, it is an operator
    topics: [I3, A1]
  - name: Chaining rules — why two UDFs in one select can cost two Python round trips
    topics: [I3, A5]
  - name: correctEvalType — the Arrow-to-pickle downgrade on UDT, now off by default
    topics: [I3]
  - name: The two aggregate extraction rules — Python UDFs before and after the aggregate
    topics: [I3, B6]
  - name: EvalPythonExec and the HybridRowQueue — every input row is buffered a second time
    topics: [I3, E1, I6]
  - name: BatchEvalPythonExec — the pickle path, and what it still costs
    topics: [I3, E1]
  - name: ArrowEvalPythonExec — batching, and the columnar-input fast path
    topics: [I3, A5, E22]
  - name: PythonArrowInput — how a batch is sized, and the 2 GB workaround
    topics: [A5, I3]
  - name: PythonArrowOutput — three output processors, and where the output batch size is set
    topics: [A5, I3]
  - name: The runner conf map — the settings shipped to the worker on every task
    topics: [I3, A5]
  - name: PythonSQLMetrics — the six numbers that say whether the UDF or the worker is slow
    topics: [I3, E3, I7]
  - name: Python worker lifecycle — daemon vs fork, reuse, idle timeout, faulthandler
    topics: [I3, E1, E3]
  - name: MapInBatchExec — one struct in, one struct out, optionally under a barrier
    topics: [A5, I3, A16]
  - name: The grouped-map family and PandasGroupUtils — dedup, sort, and the whole-group requirement
    topics: [A5, B6]
  - name: Cogroup — two Arrow streams interleaved on one worker
    topics: [A5, B7]
  - name: ArrowWindowPythonExec — window bounds shipped as extra columns
    topics: [A5, I2]
  - name: ArrowAggregatePythonExec — the 4.2.0 grouped-aggregate operator
    topics: [A5, B6]
  - name: AttachDistributedSequenceExec — the distributed-sequence index of pandas API on Spark
    topics: [I3, I5]
  - name: ArrowConverters — the toPandas / createDataFrame path, and the local-relation threshold
    topics: [I3, B3]
  - name: Arrow IPC compression — a codec whose level is silently dropped if built the obvious way
    topics: [I3, E1]
  - name: EvaluatePython — pickling, and the types that need conversion at all
    topics: [I3, B5]
  - name: UserDefinedPythonFunction and PythonPlannerRunner — registration, and a Python call at analysis time
    topics: [I3, A24]
  - name: Python UDTFs — three eval types, and a UDTF that decides its own schema
    topics: []
    propose:
      code: I30
      level: Intermediate
      title: "Python UDTFs: Table Functions That Return Many Rows"
      what: "A Python UDTF is a class with an eval() that yields rows and an optional analyze() that runs on the driver at query-analysis time to decide the output schema, partitioning and ordering from the actual arguments — planned as a Generate node rewritten into a dedicated BatchEvalPythonUDTF or ArrowEvalPythonUDTF operator."
      why: "It is the only PySpark construct that turns one input row into many without an explode, it takes TABLE() arguments so it can consume a whole partition, and its polymorphic analyze() is the one place user Python runs on the driver during analysis — which is also the one place a UDTF bug becomes an analysis error rather than a task failure."
  - name: Python Data Sources — a reader and writer written entirely in Python
    topics: []
    propose:
      code: A35
      level: Advanced
      title: "Python Data Sources: Writing a Connector Without the JVM"
      what: "Spark 4.x lets a data source be implemented in pure Python by subclassing pyspark.sql.datasource.DataSource; the JVM drives it through a long-lived worker process, sending numbered function ids for initialOffset, latestOffset, partitions and commit, and 4.2.0 adds admission control and Trigger.AvailableNow support to the streaming reader."
      why: "It replaces the two old answers to 'Spark cannot read my system' — drop to an RDD, or write Scala — and it is now a supported batch and streaming, read and write surface with its own profiler; but the protocol is a hand-rolled request/response over a pipe, so knowing what crosses it is what lets you reason about its cost and its failure modes."
  - name: transformWithStateInPySpark — Python drives the state store over a socket
    topics: []
    propose:
      code: E26
      level: Expert
      title: "transformWithStateInPySpark: The Per-Task State Server"
      what: "Arbitrary stateful processing in PySpark runs a second server thread per task — TransformWithStateInPySparkStateServer — that listens on a dedicated TCP or Unix-domain socket and answers protobuf-framed state requests from the Python worker, so every ValueState/ListState/MapState get or put and every timer registration is a synchronous round trip into the JVM state store."
      why: "It explains the performance shape of stateful PySpark: the Arrow data path is batched but the state path is one request per operation, so a processor touching state per row behaves nothing like one touching it per group — and it is a second socket, a second thread and a protobuf schema in the failure path of every stateful Python task."
  - name: applyInPandasWithState — state and data in one Arrow stream
    topics: [A8, A5]
  - name: PythonForeachWriter — a background writer thread and a spillable row buffer
    topics: [A8, A7]
  - name: PythonWorkerLogsExec — reading worker stdout back as a table
    topics: [E3, I3]
---

# sql/core — Python and Arrow

> Source sweep of the `python-arrow` group: `execution/python/` (39 files), its nested
> `execution/python/streaming/` (10 files) and `streaming/benchmark/` (1), plus
> `execution/arrow/` (3) — **53 files**, swept against **Spark 4.2.0** (`v4.2.0` tag, the version
> `configs/catalog.yaml` was parsed from).

This is the JVM half of everything PySpark does that the JVM cannot do itself: scalar and pandas
UDFs, UDTFs, `mapInPandas` / `applyInPandas` / `cogroup`, Python data sources, stateful Python
streaming, `toPandas` and `createDataFrame`, and the pandas-API-on-Spark index. Almost none of it
is one operator — it is a *process boundary*, and every concept here is either about how data
crosses it or about what happens when the process on the other side misbehaves.

!!! warning "`execution/python/streaming/` is inside this group, not `streaming-exec`"

    The `streaming-exec` group's scope names `execution/streaming/`. The Python streaming
    operators live at `execution/python/streaming/`, which the `python-arrow` scope token
    `python/` claims. They are swept here — and they matter here, because their distinguishing
    feature is the Python boundary, not the streaming engine.

---

## Planning: getting a Python UDF out of the expression tree

### ExtractPythonUDFs — a UDF is not an expression, it is an operator

**What it is:** the JVM cannot evaluate a `PythonUDF` inside a projection, so before physical
planning a rule lifts every scalar Python UDF out of whatever expression contains it and puts it
in its own logical node — `BatchEvalPython` or `ArrowEvalPython` — below the operator that used it.
The UDF call site is replaced by an `AttributeReference` named `pythonUDF0`, `pythonUDF1`, ….

The rule is recursive: after rewriting one layer it calls itself on the result until no evaluable
UDFs remain, and if the rewrite added columns that were only needed for a filter it caps the plan
with a `Project` to trim them. Correlated subqueries are skipped outright — they become joins
later and would otherwise be extracted twice.

Two guards are worth knowing because they produce internal errors rather than plans:

- every UDF batched into one node must have the **same eval type**, and
- a UDF referencing attributes from more than one child of a binary operator is rejected — the
  alternative would be a cartesian product.

**Code path:** `ExtractPythonUDFFromAggregate` → `ExtractGroupingPythonUDFFromAggregate` →
`ExtractPythonUDFs` → `BatchEvalPython` / `ArrowEvalPython` logical nodes → the matching exec

**Anchor files:**

- [ExtractPythonUDFs.scala:162](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/python/ExtractPythonUDFs.scala#L162) — the rule
- [ExtractPythonUDFs.scala:297](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/python/ExtractPythonUDFs.scala#L297) — `extract`, the rewrite, the two internal errors, and the recursive call
- [ExtractPythonUDFs.scala:384](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/python/ExtractPythonUDFs.scala#L384) — `ExtractPythonUDTFs`, the same idea for `Generate`

**Maps to topics:** I3, A1

### Chaining rules — why two UDFs in one select can cost two Python round trips

**What it is:** the rule fuses UDFs into one node only under conditions that are easy to violate
by accident. Two shapes of chaining exist:

- **parallel chaining** — siblings, `foo(x)` and `bar(x)`, evaluated together over the same input;
- **nested chaining** — `foo(bar(x))`, where the whole tree is sent to Python and evaluated as a
  `ChainedPythonFunctions` pipeline in one crossing.

`shouldExtractUDFExpressionTree` allows nested chaining only when the child UDF has the *same*
corrected eval type; a mixed tree is rejected here so a later pass extracts the inner UDF first,
producing two nodes.

And the sharp one: **iterator UDFs never chain in parallel.** If the first UDF visited is
`SQL_SCALAR_PANDAS_ITER_UDF` or `SQL_SCALAR_ARROW_ITER_UDF`, `canChainWithParallelUDFs` returns
false for everything, so each gets its own node. Otherwise only UDFs whose eval type equals the
first one visited join it.

The practical rule this yields: a plain `@F.udf` and a `@F.pandas_udf` in the same `select` produce
`BatchEvalPython` *and* `ArrowEvalPython` — two serializations, two worker crossings. `explain()`
shows it directly.

**Anchor files:**

- [ExtractPythonUDFs.scala:196](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/python/ExtractPythonUDFs.scala#L196) — `shouldExtractUDFExpressionTree`, with the `foo(bar(baz()))` worked example in the scaladoc
- [ExtractPythonUDFs.scala:232](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/python/ExtractPythonUDFs.scala#L232) — `collectEvaluableUDFsFromExpressions`, `firstVisitedScalarUDFEvalType` and the iterator-UDF exclusion

**Maps to topics:** I3, A5

### correctEvalType — the Arrow-to-pickle downgrade on UDT, now off by default

**What it is:** a `PythonUDF` carries an eval type, but the type the planner acts on is the one
`PythonUDF.correctEvalType` returns. It rewrites exactly one case: an Arrow-optimized UDF
(`SQL_ARROW_BATCHED_UDF`, which since 4.2.0 is what a plain `@F.udf` produces) whose return type or
any argument type contains a `UserDefinedType` — recursively through arrays, structs and maps —
becomes `SQL_BATCHED_UDF`, the pickle path. The extraction rule then logs a `WARN` ("Arrow
optimization disabled due to UDT input or return type") and plans a `BatchEvalPython`.

!!! warning "The downgrade is gated on a legacy config that defaults to `false`"

    `spark.sql.execution.pythonUDF.arrow.legacy.fallbackOnUDT` is **`false`** at 4.2.0 (added
    4.1.0). So the rewrite above does *not* happen by default — an Arrow UDF over a `VectorUDT`
    stays on the Arrow path. The config name says what it is: the legacy behaviour, available for
    anyone who depended on the pickle path handling UDTs. Read `correctEvalType` as "how a cluster
    that opted in behaves", not as the shipped default.

The second-order effect is worth noting either way: the *corrected* eval type is also what the
chaining check compares, so turning the legacy key on can split a UDF chain that would otherwise
have fused into one node.

**Anchor files:**

- [ExtractPythonUDFs.scala:333](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/python/ExtractPythonUDFs.scala#L333) — the eval-type dispatch and the warning
- `catalyst/expressions/PythonUDF.scala:55` — `correctEvalType` and its recursive `containsUDT`
  (in `sql/catalyst`, the `expressions` group's scope)

**Configs:** `spark.sql.execution.pythonUDF.arrow.enabled` (`true`, 3.4.0),
`spark.sql.execution.pythonUDF.arrow.legacy.fallbackOnUDT` (**`false`**, 4.1.0)

**Maps to topics:** I3

### The two aggregate extraction rules — Python UDFs before and after the aggregate

**What it is:** a Python UDF cannot run inside an `Aggregate` node at all, so two rules run before
the general one, in a required order:

1. **`ExtractPythonUDFFromAggregate`** handles a UDF *over* an aggregate result —
   `udf(sum(x))` or a UDF over a grouping key. It splits the `Aggregate` into an `Aggregate` that
   computes the aggregate expressions and a `Project` above it that applies the UDF. A UDF with no
   references at all (`udf()` of a constant) also lands here.
2. **`ExtractGroupingPythonUDFFromAggregate`** handles a UDF *in the grouping key* —
   `groupBy(udf(x))`. It evaluates the UDF below the aggregate and groups by the resulting
   attribute. It asserts the UDF is deterministic, on the grounds that `PullOutNondeterministic`
   should already have hoisted a non-deterministic one.

The upshot for a reader of `explain()`: a Python UDF near an aggregate always produces an extra
node, above or below, never inside.

**Anchor files:**

- [ExtractPythonUDFs.scala:39](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/python/ExtractPythonUDFs.scala#L39) — `ExtractPythonUDFFromAggregate`
- [ExtractPythonUDFs.scala:92](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/python/ExtractPythonUDFs.scala#L92) — `ExtractGroupingPythonUDFFromAggregate`, and the ordering requirement in its scaladoc

**Maps to topics:** I3, B6

---

## Execution: crossing the boundary

### EvalPythonExec and the HybridRowQueue — every input row is buffered a second time

**What it is:** the Python worker is sent only the UDF *arguments*, but the operator's output is
`child.output ++ resultAttrs` — the original row plus the UDF results. Since the worker returns
results positionally with no key, the JVM must keep every input row to re-join it.

It does that with a `HybridRowQueue`: a FIFO of `UnsafeRow`s registered as a `MemoryConsumer` with
the task's `TaskMemoryManager`, holding rows in Tungsten pages while memory allows and spilling
whole pages to disk queues under pressure. Rows go in as the input is consumed and come out as
Python results arrive, joined by a `JoinedRow` and re-projected.

This is the cost of a Python UDF that no documentation mentions: **the operator buffers the entire
partition's input rows**, in addition to whatever Arrow batches are in flight. The queue is spillable,
so it degrades rather than failing, but the spill is invisible in the SQL tab — `EvalPythonExec`
publishes no spill metric.

Argument handling is deduplicated first: identical argument expressions across UDFs are collected
once into `allInputs` and referenced by offset, so `foo(x)` and `bar(x)` ship `x` once.

**Code path:** `EvalPythonExec.doExecute` → `EvalPythonEvaluatorFactory.eval` → `HybridRowQueue.add`
per input row → `evaluate(...)` (subclass) → `JoinedRow(queue.remove(), outputRow)`

**Anchor files:**

- [EvalPythonExec.scala:62](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/python/EvalPythonExec.scala#L62) — the trait and its `output`
- [EvalPythonEvaluatorFactory.scala:62](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/python/EvalPythonEvaluatorFactory.scala#L62) — `eval`: the queue, the argument dedup, the join
- [EvalPythonEvaluatorFactory.scala:50](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/python/EvalPythonEvaluatorFactory.scala#L50) — `collectFunctions`, which flattens a nested UDF chain into `ChainedPythonFunctions`
- [HybridQueue.scala:47](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/python/HybridQueue.scala#L47) — the generic spillable queue and its `spill`
- [RowQueue.scala:52](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/python/RowQueue.scala#L52) — `InMemoryRowQueue`'s page format, including the `-1` end-of-page marker; `DiskRowQueue` at :96, `HybridRowQueue` at :156
- [EvalPythonUDTFExec.scala:37](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/python/EvalPythonUDTFExec.scala#L37) — the same queue-and-join structure for UDTFs

**Maps to topics:** I3, E1, I6

### BatchEvalPythonExec — the pickle path, and what it still costs

**What it is:** the non-Arrow path, now reached only by `SQL_BATCHED_UDF` — which since 4.2.0 means
a UDF that was *downgraded* (UDT) or explicitly opted out. Rows are converted to Java objects by
`EvaluatePython.toJava`, pickled in batches of `spark.sql.execution.python.udf.maxRecordsPerBatch`,
and the results unpickled and converted back with `EvaluatePython.makeFromJava`.

Two details in the result handling: with one UDF it reuses a single `GenericInternalRow` as a fast
path; with several, Python returns a struct per row. `pythonNumRowsReceived` is incremented **per
row** here, not per batch.

**Anchor files:**

- [BatchEvalPythonExec.scala:37](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/python/BatchEvalPythonExec.scala#L37) — the operator
- [BatchEvalPythonExec.scala:77](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/python/BatchEvalPythonExec.scala#L77) — `evaluate`: pickler registration, the runner, the unpickle loop, the single-UDF fast path
- [PythonUDFRunner.scala:33](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/python/PythonUDFRunner.scala#L33) — `BasePythonUDFRunner`; `writeUDFs` at :176

**Configs:** `spark.sql.execution.python.udf.maxRecordsPerBatch`,
`spark.sql.execution.python.udf.buffer.size`, `spark.sql.execution.pyspark.binaryAsBytes`

**Maps to topics:** I3, E1

### ArrowEvalPythonExec — batching, and the columnar-input fast path

**What it is:** the Arrow path. Its `BatchIterator` slices the input iterator into groups of
`spark.sql.execution.arrow.maxRecordsPerBatch` rows, each becoming one Arrow record batch. Beyond
that it has a second mode: when the child produces columnar batches *and*
`spark.sql.execution.arrow.pythonUDF.columnarInput.enabled` is on, the operator declares
`supportsColumnar` and switches to a `ColumnarArrowEvalPythonEvaluatorFactory`, which takes
`ColumnarBatch` in and returns `ColumnarBatch` out.

The columnar factory's own comments are the clearest statement of what that buys: for
Arrow-backed input vectors it selects the needed columns into a temporary `VectorSchemaRoot` and
serializes with `VectorUnloader` — `VectorSchemaRoot.of()` is a wrapper, so **no vector data is
copied**. Row conversion is skipped entirely on the way in.

**Anchor files:**

- [ArrowEvalPythonExec.scala:41](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/python/ArrowEvalPythonExec.scala#L41) — `BatchIterator`
- [ArrowEvalPythonExec.scala:98](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/python/ArrowEvalPythonExec.scala#L98) — `supportsColumnar` and the two-factory branch
- [ColumnarArrowEvalPythonEvaluatorFactory.scala:66](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/python/ColumnarArrowEvalPythonEvaluatorFactory.scala#L66) — the columnar evaluator, with the three combining strategies in its scaladoc
- [ColumnarArrowPythonInput.scala:44](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/python/ColumnarArrowPythonInput.scala#L44) — the zero-copy write path and its `isArrowBacked` guard
- [ColumnarArrowPythonRunner.scala:34](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/python/ColumnarArrowPythonRunner.scala#L34) — the runner that mixes them together

**Configs:** `spark.sql.execution.arrow.maxRecordsPerBatch` (`10000`),
`spark.sql.execution.arrow.pythonUDF.columnarInput.enabled`

**Maps to topics:** I3, A5, E22

### PythonArrowInput — how a batch is sized, and the 2 GB workaround

**What it is:** the write side of the Arrow stream, mixed into the runner. It owns a child
`BufferAllocator`, a `VectorSchemaRoot` built from the Spark schema via
`ArrowUtils.toArrowSchema(schema, timeZoneId, largeVarTypes)`, an `ArrowWriter` and a
`VectorUnloader` carrying the compression codec.

Two implementations differ in how a batch ends:

- **`BasicPythonArrowInput`** writes one Arrow batch per element of the input iterator — i.e. the
  caller already decided the batching (a group, or a `BatchIterator` slice).
- **`BatchedPythonArrowInput`** sizes batches itself against **both**
  `arrow.maxRecordsPerBatch` and `arrow.maxBytesPerBatch`, splitting a group across several batches
  when needed. The scaladoc names the reason: Arrow's 2 GB limit (ARROW-4890). A split never mixes
  two groups in one batch.

Note the record limit is normalised at construction — a non-positive `maxRecordsPerBatch` becomes
`Int.MaxValue`, i.e. unlimited, not zero. Bytes written are added to `pythonDataSent` per batch.

**Anchor files:**

- [PythonArrowInput.scala:44](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/python/PythonArrowInput.scala#L44) — the trait, allocator, root and codec
- [PythonArrowInput.scala:114](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/python/PythonArrowInput.scala#L114) — `BasicPythonArrowInput`
- [PythonArrowInput.scala:161](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/python/PythonArrowInput.scala#L161) — `BatchedPythonArrowInput`; `writeSizedBatch` and the ARROW-4890 note at :194
- [CoGroupedArrowPythonRunner.scala:39](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/python/CoGroupedArrowPythonRunner.scala#L39) — a hand-written writer for two schemas, with `ArrowUtils.failDuplicatedFieldNames` on each

**Configs:** `spark.sql.execution.arrow.maxRecordsPerBatch`,
`spark.sql.execution.arrow.maxBytesPerBatch`, `spark.sql.execution.arrow.useLargeVarTypes`

**Maps to topics:** A5, I3

### PythonArrowOutput — three output processors, and where the output batch size is set

**What it is:** the read side. It reads a length-prefixed protocol from the worker and dispatches
on `SpecialLengths`: `START_ARROW_STREAM` constructs the `ArrowStreamReader`, `TIMING_DATA` feeds
the timing metrics, `PYTHON_EXCEPTION_THROWN` raises. On `START_ARROW_STREAM` it selects one of
three processors:

- a **record-limited** one when `spark.sql.execution.arrow.maxRecordsPerOutputBatch` is set,
- a **byte-limited** one when `spark.sql.execution.arrow.maxBytesPerOutputBatch` is set,
- and a plain pass-through otherwise.

That is the piece people miss: the *output* batch size is a separate pair of configs from the
input batch size, and by default the JVM re-emits whatever batch shape Python produced.

**Anchor files:**

- [PythonArrowOutput.scala:39](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/python/PythonArrowOutput.scala#L39) — the trait
- [PythonArrowOutput.scala:102](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/python/PythonArrowOutput.scala#L102) — `read`, the `SpecialLengths` dispatch and the processor choice

**Configs:** `spark.sql.execution.arrow.maxRecordsPerOutputBatch`,
`spark.sql.execution.arrow.maxBytesPerOutputBatch`

**Maps to topics:** A5, I3

### The runner conf map — the settings shipped to the worker on every task

**What it is:** the Python worker is a separate process and cannot read `SQLConf`. A fixed list of
keys is therefore serialized into the command stream on every task by
`ArrowPythonRunner.getPythonRunnerConfMap`. Reading that list tells you exactly which SQL configs
can affect Python-side behaviour at all:

`spark.sql.session.timeZone`, `…pandas.groupedMap.assignColumnsByName`,
`…pandas.convertToArrowArraySafely`, `…arrow.useLargeVarTypes`, the two legacy pandas-conversion
keys (UDF and table UDF), `…pandas.intToDecimalCoercionEnabled`,
`…pandas.preferIntExtensionDtype`, `…pyspark.binaryAsBytes`, and three optional ones —
`…pythonUDF.arrow.concurrency.level`, `spark.sql.pyspark.udf.profiler`,
`spark.sql.pyspark.dataSource.profiler`. Optional entries are omitted when unset rather than sent
as a default.

Anything not on that list is a JVM-side setting no matter how Python-sounding its name.

**Anchor files:**

- [ArrowPythonRunner.scala:154](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/python/ArrowPythonRunner.scala#L154) — `getPythonRunnerConfMap`, the whole list
- [ArrowPythonRunner.scala:33](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/python/ArrowPythonRunner.scala#L33) — `BaseArrowPythonRunner` and the worker-behaviour overrides it reads from `SQLConf`

**Maps to topics:** I3, A5

### PythonSQLMetrics — the six numbers that say whether the UDF or the worker is slow

**What it is:** every Python operator mixes in `PythonSQLMetrics`, which registers a fixed set:

| Metric | Kind | Meaning |
|---|---|---|
| `pythonDataSent` | size | bytes written to the worker |
| `pythonDataReceived` | size | bytes read back |
| `pythonBootTime` | timing | starting the worker process |
| `pythonInitTime` | timing | initializing it (imports, broadcast reads) |
| `pythonTotalTime` | timing | the whole worker interaction |
| `pythonProcessingTime` | timing | executing the user's Python |
| `pythonNumRowsReceived` | count | output rows |

The separation of `pythonBootTime` / `pythonInitTime` from `pythonProcessingTime` is the whole
diagnostic: a slow node with a large boot time is a worker-reuse or environment problem, not a slow
UDF. All of it is in the SQL tab per node, and it costs nothing to look at.

**Anchor files:**

- [PythonSQLMetrics.scala:37](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/python/PythonSQLMetrics.scala#L37) — the three metric maps

**Maps to topics:** I3, E3, I7

### Python worker lifecycle — daemon vs fork, reuse, idle timeout, faulthandler

**What it is:** the runner passes a set of worker-behaviour flags read from `SQLConf` rather than
from the core `spark.python.*` keys, and the two families are easy to confuse. The SQL-side ones,
all read in `BaseArrowPythonRunner`:

- `spark.sql.execution.pyspark.udf.faulthandler.enabled` — install Python's `faulthandler` so a
  segfault produces a traceback instead of a bare exit code;
- `…udf.idleTimeoutSeconds` and `…udf.killOnIdleTimeout` — bound how long a worker may sit idle,
  and whether it is killed or merely reported;
- `…udf.tracebackDumpIntervalSeconds` — periodic traceback dumps from a stuck worker;
- `…udf.daemonKillWorkerOnFlushFailure`;
- `…udf.hideTraceback.enabled` and `…udf.simplifiedTraceback.enabled` — how much of the Python
  traceback reaches the JVM exception;
- `spark.sql.execution.pyspark.python` — the interpreter, overriding `spark.pyspark.python`.

The corresponding `spark.python.*` keys (`use.daemon`, `worker.reuse`, `worker.module`,
`daemon.module`, `factory.idleWorkerMaxPoolSize`, `unix.domain.socket.enabled`) are core-side and
belong to the `core — api-bridge` group; they govern how the process is created, while these
govern how this operator treats it.

**Anchor files:**

- [ArrowPythonRunner.scala:56](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/python/ArrowPythonRunner.scala#L56) — the whole block of overrides, one per flag
- [PythonPlannerRunner.scala:145](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/python/PythonPlannerRunner.scala#L145) — the release-vs-destroy decision, and the "Python worker exited unexpectedly (crashed)" message

**Maps to topics:** I3, E1, E3

---

## The function APIs

### MapInBatchExec — one struct in, one struct out, optionally under a barrier

**What it is:** the operator behind `mapInPandas` and `mapInArrow`. Unlike a UDF it does **not**
buffer input rows: the whole partition is one logical batch, each input row is wrapped in a
single-field struct so Python sees a DataFrame, and the output batch's struct column is *unwrapped*
by taking the child vectors — no row-level conversion on the way back.

Two capabilities not present anywhere else in this group:

- **Barrier mode.** When `isBarrier` is set (`mapInPandas(..., barrier=True)`), the child RDD is
  `.barrier()`-ed, so all tasks of the stage start together — the hook for distributed training
  loops inside PySpark.
- **A `ResourceProfile`.** An optional profile is attached with `rdd.withResources`, making this
  the stage-level-scheduling entry point for Python work (a GPU profile for the map stage only).

Output schema is checked only when `spark.sql.execution.arrow.pyspark.validateSchema.enabled` is
on, and the check deliberately allows a nullable declared field to come back non-nullable.

**Anchor files:**

- [MapInBatchExec.scala:36](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/python/MapInBatchExec.scala#L36) — the trait; the barrier branch and `withResources` at :75
- [MapInBatchEvaluatorFactory.scala:50](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/python/MapInBatchEvaluatorFactory.scala#L50) — the struct wrap, the schema validation, the struct unwrap
- [MapInPandasExec.scala:29](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/python/MapInPandasExec.scala#L29) / [MapInArrowExec.scala:29](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/python/MapInArrowExec.scala#L29) — the two eval types over the same trait

**Configs:** `spark.sql.execution.arrow.pyspark.validateSchema.enabled`

**Maps to topics:** A5, I3, A16

### The grouped-map family and PandasGroupUtils — dedup, sort, and the whole-group requirement

**What it is:** `applyInPandas` / `applyInArrow` are planned as `FlatMapGroupsInBatchExec`, which
requires `ClusteredDistribution(groupingAttributes)` *and* an ordering on those attributes — a
shuffle **and** a sort — then walks the sorted stream grouping by key. With no grouping attributes
at all it degrades to `AllTuples`: the whole dataset in one task.

`PandasGroupUtils` holds the two non-obvious pieces:

- **`resolveArgOffsets`** deduplicates attributes that appear both as a grouping key and as data,
  producing a reduced schema plus the offsets Python needs to reconstruct the original order. Without
  it a grouping column would be sent twice.
- **`groupAndProject`** applies that reduced projection per group.

Each group is handed to Python as one unit — the API's contract is a whole pandas DataFrame per
group — which is why `BatchedPythonArrowInput` splits *within* a group rather than across groups,
and why one very large group is the failure mode of this API.

**Anchor files:**

- [FlatMapGroupsInBatchExec.scala:35](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/python/FlatMapGroupsInBatchExec.scala#L35) — the trait; distribution and ordering at :62
- [PandasGroupUtils.scala:90](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/python/PandasGroupUtils.scala#L90) — `resolveArgOffsets`, the dedup
- [PandasGroupUtils.scala:60](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/python/PandasGroupUtils.scala#L60) — `groupAndProject`
- [FlatMapGroupsInPandasExec.scala:41](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/python/FlatMapGroupsInPandasExec.scala#L41) / [FlatMapGroupsInArrowExec.scala:40](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/python/FlatMapGroupsInArrowExec.scala#L40) — the two eval types

**Configs:** `spark.sql.legacy.execution.pandas.groupedMap.assignColumnsByName`,
`spark.sql.execution.pandas.structHandlingMode`

**Maps to topics:** A5, B6

### Cogroup — two Arrow streams interleaved on one worker

**What it is:** `cogroup(...).applyInPandas` is a binary operator: both sides are clustered and
sorted on their grouping keys, zipped by key in the JVM, and each matched pair of groups is written
to the worker as **two Arrow streams over one connection** — a left batch then a right batch, each
with its own schema, written by a hand-rolled writer rather than the shared `PythonArrowInput`.
Duplicate field names in either schema are rejected up front.

**Anchor files:**

- [FlatMapCoGroupsInBatchExec.scala:34](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/python/FlatMapCoGroupsInBatchExec.scala#L34) — the binary operator
- [CoGroupedArrowPythonRunner.scala:118](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/python/CoGroupedArrowPythonRunner.scala#L118) — `writeNextInputToStream`, the two writers
- [FlatMapCoGroupsInPandasExec.scala:44](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/python/FlatMapCoGroupsInPandasExec.scala#L44) / [FlatMapCoGroupsInArrowExec.scala:44](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/python/FlatMapCoGroupsInArrowExec.scala#L44)

**Maps to topics:** A5, B7

### ArrowWindowPythonExec — window bounds shipped as extra columns

**What it is:** a pandas UDF over a window cannot ask the JVM where its frame starts and ends, so
the operator **prepends the frame bounds to the data as ordinary columns**. For each frame that is
bounded, two extra integer columns (lower and upper index) are computed per row and written into
the Arrow batch ahead of the UDF's real inputs; unbounded frames need none and get none. A
`window_bound_types` entry in the runner conf tells Python how to interpret them.

It extends `WindowExecBase`, so it inherits the same distribution, ordering and single-partition
warning as the JVM `WindowExec` — and the same `ExternalAppendOnlyUnsafeRowArray` buffering, with
its own `spillSize` metric. It is the one subclass of `WindowEvaluatorFactoryBase` that does not
wire the segment-tree metrics (they default to `None`).

**Anchor files:**

- [ArrowWindowPythonExec.scala:80](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/python/ArrowWindowPythonExec.scala#L80) — the operator; supported eval types at :127
- [ArrowWindowPythonEvaluatorFactory.scala:90](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/python/ArrowWindowPythonEvaluatorFactory.scala#L90) — `computeWindowBoundHelpers`, the index arithmetic
- [ArrowWindowPythonEvaluatorFactory.scala:196](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/python/ArrowWindowPythonEvaluatorFactory.scala#L196) — where the bound columns are prepended, with the comment explaining why unbounded frames are skipped

**Maps to topics:** A5, I2

### ArrowAggregatePythonExec — the 4.2.0 grouped-aggregate operator

**What it is:** the operator for a grouped-aggregate pandas/Arrow UDF (`SQL_GROUPED_AGG_*`).
`ClusteredDistribution` on the grouping expressions, or `AllTuples` when there are none, plus an
ordering — and it understands **session windows**: if a grouping expression carries the
`SessionWindow` marker it is excluded from the required ordering prefix and handled specially,
mirroring the JVM aggregate path.

**Anchor files:**

- [ArrowAggregatePythonExec.scala:53](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/python/ArrowAggregatePythonExec.scala#L53) — the operator; the session-window handling at :69 and the distribution at :82

**Maps to topics:** A5, B6

### AttachDistributedSequenceExec — the distributed-sequence index of pandas API on Spark

**What it is:** pandas-API-on-Spark's default index needs a globally consecutive `long` per row,
which no partition can compute alone. This operator does it with a `zipWithIndex`-style pass — and
because that pass must know each partition's size, it **caches the child RDD** first, at a storage
level taken from the pandas-on-Spark option `compute.default_index_cache` rather than from any
Spark config.

That caching is the point worth knowing: creating a pandas-on-Spark DataFrame with the default
index materializes and caches the input, so the "free" index is a materialization barrier.

**Anchor files:**

- [AttachDistributedSequenceExec.scala:38](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/python/AttachDistributedSequenceExec.scala#L38) — the operator; `cacheRDD` at :54

**Maps to topics:** I3, I5

---

## Arrow conversion outside the UDF path

### ArrowConverters — the toPandas / createDataFrame path, and the local-relation threshold

**What it is:** `execution/arrow/` is not about UDFs at all — it is the conversion used by
`df.toPandas()`, `spark.createDataFrame(pandas_df)` and Spark Connect. Two directions:

- **Out:** `toBatchIterator` / `toBatchWithSchemaIterator` turn an `InternalRow` iterator into
  serialized Arrow record batches, cutting a batch when either `maxRecordsPerBatch` or an
  estimated byte limit is hit (a non-positive record limit means unlimited). The
  `ArrowBatchStreamWriter` frames them as a stream.
- **In:** `fromBatchIterator` reads batches back, and `toDataFrame` makes a decision worth knowing:
  if the total size of the driver-side batches exceeds
  `spark.sql.execution.arrow.localRelationThreshold`, it parallelizes them into an RDD and builds a
  real distributed DataFrame; below it, it decodes on the driver into a `LocalRelation` — copying
  each row through an `UnsafeProjection` first, because the Arrow vectors are released afterwards.

The reader tracks `batchesLoaded`, `totalRowsProcessed`, `allocatedMemory` and
`peakMemoryAllocation`, which is the usable handle on Arrow allocator pressure during a large
`toPandas`.

**Anchor files:**

- [ArrowConverters.scala:80](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/arrow/ArrowConverters.scala#L80) — the object; the batch-cut conditions at :171
- [ArrowConverters.scala:447](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/arrow/ArrowConverters.scala#L447) — `fromBatchIterator`
- [ArrowConverters.scala:543](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/arrow/ArrowConverters.scala#L543) — `toDataFrame` and the RDD-vs-`LocalRelation` threshold
- [ArrowFileReadWrite.scala:88](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/arrow/ArrowFileReadWrite.scala#L88) — `save` / `load`, a DataFrame as a single Arrow file

**Configs:** `spark.sql.execution.arrow.pyspark.enabled`,
`spark.sql.execution.arrow.pyspark.fallback.enabled`,
`spark.sql.execution.arrow.localRelationThreshold`,
`spark.sql.execution.arrow.pyspark.selfDestruct.enabled`,
`spark.sql.execution.arrow.sparkr.enabled`

**Maps to topics:** I3, B3

### Arrow IPC compression — a codec whose level is silently dropped if built the obvious way

**What it is:** Arrow batches on the wire can be compressed with `zstd` or `lz4`, chosen by
`spark.sql.execution.arrow.compression.codec` — which defaults to **`none`** (added 4.1.0), so no
compression happens unless you ask. Its config doc is the case for turning it on: significant
memory and bandwidth savings on a large `toPandas` / `toArrow`, at CPU cost. The file itself exists
mainly to document a trap, and states it directly: the codec must be constructed with `new ZstdCompressionCodec(level)`
rather than through `CompressionCodec.Factory.INSTANCE.createCodec(type)`, because the codec-type
enum carries no level, so the factory silently builds a default-level codec and drops the
configured `spark.sql.execution.arrow.compression.zstd.level`. The level matters only on the write
side; the reader looks the codec up from the IPC message.

**Anchor files:**

- [ArrowCompressionUtils.scala:25](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/arrow/ArrowCompressionUtils.scala#L25) — the object, the trap, and the three supported names

**Configs:** `spark.sql.execution.arrow.compression.codec`,
`spark.sql.execution.arrow.compression.zstd.level`

**Maps to topics:** I3, E1

### EvaluatePython — pickling, and the types that need conversion at all

**What it is:** the row/object bridge for the non-Arrow path. `needConversionInPython` decides,
per type, whether a value can cross as-is; `toJava` and `makeFromJava` build the converters, with
`makeFromJava` returning a closure per type so the dispatch happens once rather than per row. It
registers custom picklers for `StructType` and `Row`, and it unwraps a `UserDefinedType` to its
`sqlType` in both directions — which is precisely why the UDT case works at all on the pickle path
and not on the Arrow one.

**Anchor files:**

- [EvaluatePython.scala:37](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/python/EvaluatePython.scala#L37) — the object; `toJava` at :63, `makeFromJava` at :119, the picklers at :279

**Configs:** `spark.sql.execution.pyspark.binaryAsBytes`,
`spark.sql.execution.pandas.convertToArrowArraySafely`,
`spark.sql.execution.pandas.inferPandasDictAsMap`

**Maps to topics:** I3, B5

### UserDefinedPythonFunction and PythonPlannerRunner — registration, and a Python call at analysis time

**What it is:** `UserDefinedPythonFunction` is what `@F.udf` produces on the JVM side: a name, a
`PythonFunction` (the pickled closure plus its environment), a return type, an eval type and a
determinism flag. Its `builder` turns argument expressions into a `PythonUDF`, `PythonUDAF` or a
UDTF node.

`PythonPlannerRunner` is the more surprising half: an abstract runner that starts a Python worker
**from the driver, during query analysis**, writes a pickled request and reads a typed reply. It
is how a polymorphic UDTF's `analyze()` is called (`UserDefinedPythonTableFunctionAnalyzeRunner`),
and it has its own memory setting, `spark.sql.planner.pythonExecution.memory`. A crash there
produces "Python worker exited unexpectedly (crashed)" at *analysis* time, not task time.

**Anchor files:**

- [UserDefinedPythonFunction.scala:41](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/python/UserDefinedPythonFunction.scala#L41) — the UDF case class and `builder`
- [UserDefinedPythonFunction.scala:98](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/python/UserDefinedPythonFunction.scala#L98) — `UserDefinedPythonTableFunction`, and the fixed-vs-polymorphic branch at :140
- [UserDefinedPythonFunction.scala:213](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/python/UserDefinedPythonFunction.scala#L213) — the analyze runner and how it parses back a schema, partitioning and ordering
- [PythonPlannerRunner.scala:42](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/python/PythonPlannerRunner.scala#L42) — `runInPython`, worker creation, broadcast write, error handling

**Configs:** `spark.sql.planner.pythonExecution.memory`, `spark.python.use.daemon`

**Maps to topics:** I3, A24

### Python UDTFs — three eval types, and a UDTF that decides its own schema

**What it is:** a Python UDTF is a `Generate` node rewritten by `ExtractPythonUDTFs` into one of
three operators by eval type: `SQL_TABLE_UDF` → `BatchEvalPythonUDTF` (pickle),
`SQL_ARROW_TABLE_UDF` → `ArrowEvalPythonUDTF`, and `SQL_ARROW_UDTF` → a third form. The execution
shape reuses the UDF machinery — `EvalPythonUDTFExec` has the same `HybridRowQueue` join with the
required child output — but the arity differs: one input row produces zero or more output rows, so
the queue's row is repeated per emitted row rather than consumed one-for-one.

The distinguishing feature is **polymorphic analysis**. A UDTF registered without a fixed
`returnType` becomes an `UnresolvedPolymorphicPythonUDTF` whose resolution calls the user's
`analyze()` in a driver-side Python worker; the reply carries the output schema, an optional
`withSinglePartition` / `partitionBy` / `orderBy` request, and the subset of input columns the UDTF
actually wants. A UDTF can therefore demand its own distribution — the only PySpark construct
that can.

`TABLE(...)` arguments arrive as `FunctionTableSubqueryArgumentExpression`, which is how a UDTF
consumes a whole relation rather than a row.

**Anchor files:**

- [ExtractPythonUDFs.scala:390](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/python/ExtractPythonUDFs.scala#L390) — the three-way eval-type dispatch
- [EvalPythonUDTFExec.scala:37](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/python/EvalPythonUDTFExec.scala#L37) — the shared trait and its queue
- [BatchEvalPythonUDTFExec.scala:46](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/python/BatchEvalPythonUDTFExec.scala#L46) — the pickle operator; `PythonUDTFRunner` at :104
- [ArrowEvalPythonUDTFExec.scala:43](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/python/ArrowEvalPythonUDTFExec.scala#L43) — the Arrow operator and its `BatchIterator`
- [ArrowPythonUDTFRunner.scala:36](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/python/ArrowPythonUDTFRunner.scala#L36) — the runner
- [UserDefinedPythonFunction.scala:256](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/python/UserDefinedPythonFunction.scala#L256) — `receiveFromPython`: how the analyze reply becomes a `PythonUDTFAnalyzeResult`

**Configs:** `spark.sql.execution.pythonUDTF.arrow.enabled`,
`spark.sql.legacy.execution.pythonUDTF.pandas.conversion.enabled`

**Maps to topics:** none — proposed as **I30**

---

## Python streaming

### Python Data Sources — a reader and writer written entirely in Python

**What it is:** the JVM side of `pyspark.sql.datasource.DataSource` for the streaming case is
`PythonStreamingSourceRunner`, a proxy that keeps one Python worker alive for the life of the
stream and calls into it with **numbered function ids** written on a pipe:
`INITIAL_OFFSET_FUNC_ID = 884`, `LATEST_OFFSET = 885`, `PARTITIONS = 886`, `COMMIT = 887`, plus
4.2.0's `LATEST_OFFSET_ADMISSION_CONTROL = 890` and `REPORT_LATEST_OFFSET = 892`.

`checkSupportedFeatures` asks the Python reader which of two optional capabilities it implements —
admission control (`ReadLimit` / `getDefaultReadLimit`) and `Trigger.AvailableNow`
(`prepareForTriggerAvailableNow`) — so the same API supports both a minimal reader and a
rate-limited one. Records can be prefetched, with a status code telling the JVM how to receive them.

**Anchor files:**

- [streaming/PythonStreamingSourceRunner.scala:44](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/python/streaming/PythonStreamingSourceRunner.scala#L44) — the function-id table
- [streaming/PythonStreamingSourceRunner.scala:73](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/python/streaming/PythonStreamingSourceRunner.scala#L73) — the runner; `init` at :96, `checkSupportedFeatures` at :143, `latestOffset` at :205

**Configs:** `spark.sql.pyspark.dataSource.profiler`, `spark.sql.python.filterPushdown.enabled`

**Maps to topics:** none — proposed as **A35**

### transformWithStateInPySpark — Python drives the state store over a socket

**What it is:** the most unusual thing in this group. Arbitrary stateful processing in PySpark
cannot pass state through the Arrow stream, because the processor reads and writes state at
arbitrary points inside its own Python code. Spark's answer is a **second channel**: per task,
`TransformWithStateInPySparkPythonRunner` spawns a thread running
`TransformWithStateInPySparkStateServer`, which listens on its own socket — TCP (with `TCP_NODELAY`)
or a Unix domain socket — and answers protobuf `StateRequest` messages from the Python worker.

The request surface is the whole state API: implicit grouping key set/remove, stateful-processor
calls (declare a `ValueState` / `ListState` / `MapState`, with `TTLConfig`), per-variable get/put/
clear, map iterators, timer registration and expiry queries, and utility calls. Each is a
length-prefixed protobuf round trip.

The performance consequence follows directly: the **data** path is Arrow-batched, but the **state**
path is one synchronous round trip per operation. A processor that touches state once per group
behaves like the JVM operator; one that touches it per row pays a socket round trip per row.

`TransformWithStateInPySparkExec` itself is a normal stateful operator — it declares column-family
schemas to the driver through a `DriverStatefulProcessorHandleImpl` before any task runs, supports
an initial state input, and uses `StateStoreAwareZipPartitions` — so all the usual state-store
configuration applies underneath.

**Anchor files:**

- [streaming/TransformWithStateInPySparkStateServer.scala:54](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/python/streaming/TransformWithStateInPySparkStateServer.scala#L54) — the server; the scaladoc describing the per-task thread and dedicated socket at :45
- [streaming/TransformWithStateInPySparkStateServer.scala:215](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/python/streaming/TransformWithStateInPySparkStateServer.scala#L215) — `handleRequest`, the full method dispatch
- [streaming/TransformWithStateInPySparkExec.scala:69](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/python/streaming/TransformWithStateInPySparkExec.scala#L69) — the operator; `driverProcessorHandle` and the schema declaration at :130
- [streaming/TransformWithStateInPySparkPythonRunner.scala:49](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/python/streaming/TransformWithStateInPySparkPythonRunner.scala#L49) — the runner; the initial-state and pre-init variants at :122 and :291
- [streaming/TransformWithStateInPySparkDeserializer.scala:38](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/python/streaming/TransformWithStateInPySparkDeserializer.scala#L38) — the row deserializer
- [streaming/benchmark/BenchmarkTransformWithStateInPySparkStateServer.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/python/streaming/benchmark/BenchmarkTransformWithStateInPySparkStateServer.scala) — Spark's own benchmark for the server, which is itself evidence the round-trip cost is the concern

**Configs:** `spark.sql.execution.arrow.transformWithStateInPySpark.maxStateRecordsPerBatch`,
`spark.python.unix.domain.socket.enabled`

**Maps to topics:** none — proposed as **E26**

### applyInPandasWithState — state and data in one Arrow stream

**What it is:** the older stateful API, and it took the opposite approach: rather than a second
channel, it encodes the state *into the same Arrow stream* as a second set of columns with a nested
`STATE_METADATA_SCHEMA`. `ApplyInPandasWithStateWriter` maintains two `ArrowWriter`s over one
`VectorSchemaRoot` — one for data, one for state metadata — and exposes a group-oriented protocol
(`startNewGroup`, `writeRow`, `finalizeGroup`, `finalizeData`) that respects both the record and
byte batch limits.

Comparing the two designs is the fastest way to understand why `transformWithState` needed a
socket: the in-stream encoding works only when state is read and written *at group boundaries*.

**Anchor files:**

- [streaming/ApplyInPandasWithStateWriter.scala:50](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/python/streaming/ApplyInPandasWithStateWriter.scala#L50) — the writer and the two Arrow writers
- [streaming/BaseStreamingArrowWriter.scala:31](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/python/streaming/BaseStreamingArrowWriter.scala#L31) — the shared group/batch bookkeeping
- [streaming/ApplyInPandasWithStatePythonRunner.scala:56](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/python/streaming/ApplyInPandasWithStatePythonRunner.scala#L56) — the runner
- [streaming/FlatMapGroupsInPandasWithStateExec.scala:58](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/python/streaming/FlatMapGroupsInPandasWithStateExec.scala#L58) — the operator

**Maps to topics:** A8, A5

### PythonForeachWriter — a background writer thread and a spillable row buffer

**What it is:** `foreach` with a Python function is a `ForeachWriter` whose `process()` cannot
block on Python for each row. It writes rows into an `UnsafeRowBuffer` (spillable, like the UDF
queue) and a separate `WriterThread` drains it into the worker, using `hasNext`/`next` on the
output iterator purely as a mechanism to push input across. `close()` marks the buffer complete so
the writer thread can finish.

**Anchor files:**

- [streaming/PythonForeachWriter.scala:78](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/python/streaming/PythonForeachWriter.scala#L78) — the writer and the sequence described in its scaladoc
- [streaming/PythonForeachWriter.scala:42](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/python/streaming/PythonForeachWriter.scala#L42) — `WriterThread`, and the comment about the indirect data-shipping trick

**Maps to topics:** A8, A7

### PythonWorkerLogsExec — reading worker stdout back as a table

**What it is:** a leaf operator that reads the Python-worker log blocks the executors stored in the
`BlockManager` and returns them as a one-column JSON relation, keyed by session id. This is the SQL
end of the 4.2.0 PySpark worker-logging feature — the reason a `print()` or a logger call inside a
UDF can now be retrieved as data instead of being lost in an executor's stdout.

**Anchor files:**

- [PythonWorkerLogsExec.scala:29](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/python/PythonWorkerLogsExec.scala#L29) — the operator; `getBlockIds` at :55

**Configs:** `spark.sql.pyspark.worker.logging.enabled`,
`spark.executor.python.worker.log.details`

**Maps to topics:** E3, I3

---

## Breadth checks

### Config breadth

The slice was taken over subsystems `sql/catalyst` + `sql/core` + `core` (the worker-lifecycle
keys are declared in core's `Python.scala`, and every `spark.sql.*` key in catalyst's `SQLConf`),
with the pattern:

```
\.python|\.arrow|pyspark|pandas|Arrow|Python
```

**72 keys.** Err-wide deliberately: the `spark.python.*` family is core's, and appears here as an
out-of-scope row rather than being dropped.

| Family | Keys | Tied to |
|---|---|---|
| `execution.arrow.max*PerBatch` / `PerOutputBatch` | 4 | PythonArrowInput / PythonArrowOutput |
| `execution.arrow.compression.*` | 2 | Arrow IPC compression |
| `execution.arrow.pyspark.*` (enabled, fallback, selfDestruct, validateSchema) | 4 | ArrowConverters, MapInBatch |
| `execution.arrow.enabled`, `.fallback.enabled`, `.sparkr.enabled`, `.localRelationThreshold`, `.useLargeVarTypes` | 5 | ArrowConverters, PythonArrowInput |
| `execution.arrow.pythonUDF.columnarInput.enabled` | 1 | columnar fast path |
| `execution.arrow.transformWithStateInPySpark.maxStateRecordsPerBatch` | 1 | transformWithState |
| `execution.pythonUDF.arrow.*` (enabled, legacy.fallbackOnUDT, concurrency.level) | 3 | eval-type selection, runner conf |
| `execution.pythonUDF.pandas.*` (intToDecimalCoercion, preferIntExtensionDtype) | 2 | runner conf |
| `execution.pythonUDTF.arrow.enabled`, `legacy.execution.pythonUDTF.pandas.conversion.enabled` | 2 | UDTFs |
| `execution.pandas.*` (convertToArrowArraySafely, inferPandasDictAsMap, structHandlingMode, udf.buffer.size) | 4 | EvaluatePython, grouped map, runner |
| `legacy.execution.pandas.groupedMap.assignColumnsByName`, `legacy.execution.pythonUDF.pandas.conversion.enabled` | 2 | grouped map, runner conf |
| `execution.python.udf.buffer.size`, `.maxRecordsPerBatch` | 2 | BatchEvalPython |
| `execution.pyspark.udf.*` (faulthandler, hideTraceback, simplifiedTraceback, idleTimeout, killOnIdleTimeout, tracebackDumpInterval, daemonKillWorkerOnFlushFailure) | 7 | worker lifecycle |
| `execution.pyspark.python`, `execution.pyspark.binaryAsBytes` | 2 | runner, EvaluatePython |
| `planner.pythonExecution.memory` | 1 | PythonPlannerRunner |
| `pyspark.udf.profiler`, `pyspark.dataSource.profiler` | 2 | runner conf, Python data sources |
| `pyspark.worker.logging.enabled`, `executor.python.worker.log.details` | 2 | PythonWorkerLogsExec |
| `python.filterPushdown.enabled` | 1 | Python data sources |
| `pyspark.jvmStacktrace.enabled` | 1 | worker lifecycle (error surface) |

**Out of scope, and where they belong:**

| Family | Keys | Owning group |
|---|---|---|
| `spark.python.*` (daemon, worker.reuse, worker.module, idleTimeout, unix.domain.socket, authenticate, killTimeout, faulthandler, tracebackDump, factory pool) | 15 | `core — api-bridge` (process creation; the SQL keys above govern how this group *uses* the process) |
| `spark.pyspark.python`, `spark.pyspark.driver.python`, `spark.executor.pyspark.memory`, `spark.yarn.isPython` | 4 | `core — api-bridge`, `resource-managers/yarn` |
| `spark.sql.pyspark.legacy.infer*`, `inferNestedDictAsStruct`, `plotting.max_rows`, `toJSON.returnDataFrame` | 6 | Python-side inference and plotting; no JVM operator in this scope reads them |
| `spark.python.sql.dataFrameDebugging.enabled` | 1 | `core — api-bridge` |

Every in-scope key ties to a concept above.

### Package breadth

Walked by hand — this group's scope token `python/` claims a nested sub-package that
`check_drift.py --coverage` structurally cannot see, so `ls -R` was compared against the citations.

| Package | Files | Cited |
|---|---|---|
| `execution/python/` | 39 | 39 |
| `execution/python/streaming/` | 10 | 10 |
| `execution/python/streaming/benchmark/` | 1 | 1 |
| `execution/arrow/` | 3 | 3 |

**53 of 53.**

### Deliberately not covered

- **`BasePythonRunner` and the worker protocol itself** (`core/api/python/`) — every runner here
  extends it, but process creation, the daemon, authentication and the socket handshake are the
  `core — api-bridge` group's, which has already swept them.
- **`ArrowUtils`, `ArrowWriter`, `ArrowColumnVector`** — the type-mapping and vector layer under
  these converters lives in `sql/catalyst` and `sql/core/vectorized/`; cited by name, mapped by the
  `datasources` and catalyst sweeps.
- **The Python side of every one of these protocols** (`python/pyspark/worker.py`,
  `sql/pandas/serializers.py`, `datasource.py`) — the JVM half is what this map covers.
- **pandas API on Spark as a library** — only its one JVM operator,
  `AttachDistributedSequenceExec`, is in this scope; the rest is Python.
- **The state-store engine under `transformWithState`** — RocksDB providers, checkpointing and
  column families belong to the `streaming-exec` group.

---

## Refresh log

| Date | Spark | What changed |
|---|---|---|
| 2026-08-06 | 4.2.0 | Initial sweep of the group: 53/53 files cited across four packages, 28 concepts, 3 new topics proposed (I30 Python UDTFs, A35 Python Data Sources, E26 transformWithStateInPySpark). Package breadth found the nested `python/streaming/` sub-package that `--coverage` cannot see — 11 of the 53 files — and it is swept here rather than by `streaming-exec`, whose scope names `execution/streaming/` only. Headline findings: every Python UDF buffers its entire input partition a second time in a spillable `HybridRowQueue` with no spill metric; iterator pandas UDFs never chain in parallel, so each gets its own worker crossing; the UDT-to-pickle downgrade in `correctEvalType` is gated on a legacy config that defaults to `false` at 4.2.0 (checked against the catalog, not assumed from the code path); and `transformWithStateInPySpark` runs a per-task protobuf state server on its own socket |
