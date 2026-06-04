# Spark 4.1.2 Source-Verified Internals

Facts verified by reading source code at `C:\opt\learn\spark\spark` tag `v4.1.2`.
Do not re-search these — read directly from source if deeper detail is needed.

Last verified: **2026-06-04**

---

## Execution pipeline — QueryExecution

| Fact | Source location | Detail |
|---|---|---|
| `QueryExecution.toRdd` return | `sql/core/.../QueryExecution.scala` | `new SQLExecutionRDD(executedPlan.execute(), conf)` — wraps `RDD[InternalRow]` with SQL execution metadata |
| All lazy | Same file | `analyzed`, `optimizedPlan`, `sparkPlan`, `executedPlan` are all lazy vals — nothing runs until accessed |
| `SparkPlan.execute()` contract | `sql/core/.../SparkPlan.scala` | `final def execute(): RDD[InternalRow]` — ALL physical operators return this type |

---

## WholeStageCodegenExec — Tungsten execution

| Fact | Source location | Detail |
|---|---|---|
| `doExecute()` return type | `sql/core/.../WholeStageCodegenExec.scala` | Returns `RDD[InternalRow]` — RDD layer is NOT bypassed |
| Codegen pipeline | Same file | `doCodeGen()` generates Java source → `CodeGenerator.compile()` compiles to JVM bytecode |
| Leaf RDD injection | Same file | `child.inputRDDs(): Seq[RDD[InternalRow]]` — gets leaf RDDs from child operators |
| Partition wrapping | Same file | `rdds.head.mapPartitionsWithIndex { evaluator.eval(index, iter) }` — Tungsten evaluator runs inside each RDD partition |
| Two-RDD variant | Same file | `rdds.head.zipPartitions(rdds(1)) { ... }.mapPartitionsWithIndex { evaluator.eval(...) }` |
| Type erasure comment | Same file | `"Even though rdds is an RDD[InternalRow] it may actually be an RDD[ColumnarBatch] with type erasure hiding that"` |
| Codegen fallback | Same file | If `CodeGenerator.compile()` fails and `spark.sql.codegen.fallback=true` (default), falls back silently to interpreted execution |

---

## FileScanRDD — the leaf

| Fact | Source location | Detail |
|---|---|---|
| Class declaration | `sql/core/.../FileScanRDD.scala` | `class FileScanRDD extends RDD[InternalRow]` — a real RDD; the RDD layer is NOT bypassed |
| Type erasure hack | Same file | `iterator.asInstanceOf[Iterator[InternalRow]] // This is an erasure hack.` — Spark's own comment at end of `compute()` |
| Actual element type | Same file | Runtime elements may be `UnsafeRow` or `ColumnarBatch`; cast to `InternalRow` is unchecked |

---

## UnsafeRow — memory layout

| Fact | Source location | Detail |
|---|---|---|
| Default allocation | `sql/core/.../QueryExecution.scala`, `WholeStageCodegenExec.scala` | `UnsafeRow` byte arrays are **on-heap by default**; JVM GC manages them |
| `sun.misc.Unsafe` role | `UnsafeRow.scala` | Used as the **write API** for unaligned memory access into the byte array — NOT the allocator |
| True off-heap | Config | Requires `spark.memory.offHeap.enabled=true` + `spark.memory.offHeap.size=Xg` |
| `GenericInternalRow` appearance | Source + docs | Appears only when `spark.sql.codegen.wholeStage=false`, during Catalyst analysis/planning (before execution), or in test paths — never in normal Tungsten execution |
| Codegen fallback config | `spark.sql.codegen.fallback` | Default `true` — silent fallback to `GenericInternalRow` on compilation failure |

---

## Stage types — ShuffleMapStage and ResultStage

| Fact | Source location | Detail |
|---|---|---|
| `ShuffleMapTask.runTask()` return type | `core/.../ShuffleMapTask.scala` | Returns `MapStatus` — NOT void; contains shuffle file location metadata (which executor BlockManager holds each output partition) |
| `MapOutputTrackerMaster` role | `core/.../ShuffleMapStage.scala` | `numAvailableOutputs`, `findMissingPartitions` — driver tracks which partitions have written shuffle output via MapStatus |
| `ShuffleMapStage.isAvailable` | `core/.../ShuffleMapStage.scala` | `numAvailableOutputs == numPartitions` — stage is ready when ALL partitions have registered MapStatus |
| `ResultStage` partial execution | `core/.../ResultStage.scala` | `"Some stages may not run on all partitions of the RDD, for actions like first() and lookup()"` — Spark's own Scaladoc |
| `ResultTask` purpose | `core/.../ResultTask.scala` | `"A task that sends back the output to the driver application."` — Spark's own Scaladoc |
| `ResultTask.runTask()` | Same file | `func(context, rdd.iterator(partition, context))` — applies user function to partition, result sent to driver |

---

## Catalyst optimizer rule count

| Fact | Value | Source |
|---|---|---|
| `operatorOptimizationRuleSet` rules | **54** | `Optimizer.scala` v4.1.2, lines ~101–165 |
| Total rules in `defaultBatches` | **100+** (54 in operatorOptimizationRuleSet + additional batches) | `Optimizer.scala` v4.1.2 |
| "60+ rules" claim | **Understates** — correct for older Spark versions; Spark 4.1.2 has 100+ | Source-verified |

Use **"100+ optimization rules"** in chapter content for Spark 4.1.2 accuracy.

---

## Unmanaged memory — new in Spark 4.1.0

"Unmanaged memory" = memory consumed by components (RocksDB, native libraries, external caches) that manage their own allocation **outside** `UnifiedMemoryManager`. Spark 4.1.0 added a tracking + back-pressure mechanism for it.

### The trait — `UnmanagedMemoryConsumer`

Source: `core/src/main/scala/org/apache/spark/memory/UnmanagedMemoryConsumer.scala`

```scala
trait UnmanagedMemoryConsumer {
  def unmanagedMemoryConsumerId: UnmanagedMemoryConsumerId   // (componentType, instanceKey)
  def memoryMode: MemoryMode                                  // ON_HEAP or OFF_HEAP
  def getMemBytesUsed: Long  // return -1L to auto-remove from registry
}
```

- `UnmanagedMemoryConsumerId(componentType: String, instanceKey: String)` — instanceKey should be shared across instances that pool memory, to avoid double-counting.

### Registration

```scala
UnifiedMemoryManager.registerUnmanagedMemoryConsumer(consumer)  // companion object — per-JVM singleton
```

Source: `UnifiedMemoryManager.scala` companion object. Registry is a `ConcurrentHashMap` on the object (not the instance).

### Polling config

| Key | Default | Added |
|---|---|---|
| `spark.memory.unmanagedMemoryPollingInterval` | **0s (disabled)** | 4.1.0 |

Source: `core/src/main/scala/org/apache/spark/internal/config/package.scala` line 505, and `docs/configuration.md` line 2185.

**Polling is disabled by default.** When set to 0, `getUnmanagedMemoryUsed()` always returns 0 and has no effect on allocation.

### What polling does when enabled

A single daemon thread ("unmanaged-memory-poller", started once per JVM via `AtomicBoolean`) polls all registered consumers at the interval. Updates two `AtomicLong` values on the companion object: `unmanagedOnHeapUsed` and `unmanagedOffHeapUsed`.

If `getMemBytesUsed` returns `-1L`, the consumer is automatically removed from the registry. Exceptions during polling return 0 (not a crash).

### Effect on allocation (when polling enabled)

Back-pressure is applied in two places in `UnifiedMemoryManager.scala`:

1. **`computeMaxExecutionPoolSize()`** — subtracts unmanaged usage from the max execution budget:
   ```scala
   math.max(0L, availableMemory - unmanagedMemory)
   ```
2. **`acquireStorageMemory()`** — reduces effective max storage memory and fast-fails if a block won't fit after accounting for unmanaged usage.

### Only production implementor (4.1.2)

`RocksDBMemoryManager` (singleton object) in `sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/state/RocksDBMemoryManager.scala`

- `componentType = "RocksDB"`, `instanceKey = "RocksDB-Memory-Manager"`
- `memoryMode = OFF_HEAP` — RocksDB uses native memory
- Registers itself in `getOrCreateRocksDBMemoryManagerAndCache()` (called when a RocksDB state store is opened on an executor)
- Handles bounded-memory mode (shared LRU cache) vs unbounded (per-instance cache) — tracks each via `instanceMemoryMap: ConcurrentHashMap[String, InstanceMemoryInfo]`

### Key verified facts

| Claim | Verdict | Source |
|---|---|---|
| `UnmanagedMemoryConsumer` trait is new in Spark 4.x | **True — 4.1.0** | config `.version("4.1.0")` |
| Polling is on by default | **False — default is 0s (disabled)** | config `createWithDefaultString("0s")` |
| Only RocksDB implements it (4.1.2) | **True** | grep across non-test source |
| RocksDB unmanaged memory is ON_HEAP | **False — OFF_HEAP** | `RocksDBMemoryManager.memoryMode` |
| Polling thread is per `UnifiedMemoryManager` instance | **False — per JVM singleton** | `pollingStarted: AtomicBoolean` on companion |

---

## Spark Connect mode — client/server transport

Source files: `python/pyspark/sql/connect/client/core.py`, `python/pyspark/sql/connect/dataframe.py`, `python/pyspark/sql/connect/session.py`

| Claim | Verdict | Source evidence |
|---|---|---|
| Plan serialized as protobuf, sent over gRPC; no Py4J | **True** | `connect/client/core.py` imports `proto as pb2`, `base_pb2_grpc as grpc_lib`; `_execute_plan_request_with_metadata()` builds `pb2.ExecutePlanRequest`; transport is `self._stub.ExecutePlan(...)` / `self._stub.AnalyzePlan(...)` gRPC stubs. No Py4J imports anywhere in the connect client. |
| `AnalysisException` only arrives with an action result — never before | **False** | `.schema` property calls `client.schema(query)` → `_analyze("schema", ...)` → `self._stub.AnalyzePlan(req, ...)` — a separate `AnalyzePlan` RPC that runs server-side analysis and **can raise `AnalysisException` before any action fires**. Same applies to `.explain()`, `.dtypes`, `.isStreaming`, etc. The `_schema` docstring confirms: *"Connect is lazy by nature. This means that we only resolve the plan when it is submitted for execution or analysis."* |
| No `df._jdf` in Connect mode | **True** | `dataframe.py` `__getattr__`: `if name in ["_jseq", "_jdf", "_jmap", "_jcols", "rdd"]: raise PySparkAttributeError(errorClass="JVM_ATTRIBUTE_NOT_SUPPORTED", ...)` |
| Results returned as Apache Arrow record batches | **True** | `core.py` imports `pyarrow as pa`; processes `pa.RecordBatch` responses; `_execute_plan_request_with_metadata` sets `ResultChunkingOptions(allow_arrow_batch_chunking=..., preferred_arrow_chunk_size=...)` |
| No RDD support | **True** | `rdd` is in the same `JVM_ATTRIBUTE_NOT_SUPPORTED` list as `_jdf` — confirmed in `dataframe.py` `__getattr__` |

### Correct framing for chapter content

The real distinction from classic mode is **where** errors originate, not purely **when**:

- **Classic mode**: `AnalysisException` is raised client-side in the driver JVM. It can fire when `.schema`/`.dtypes` is accessed, when `.explain()` is called, or at action time — all locally.
- **Connect mode**: `AnalysisException` always comes from the server as an RPC error response (never from local Python code). It can fire from an `AnalyzePlan` RPC (`.schema`, `.dtypes`, `.explain()`) **or** from an `ExecutePlan` RPC (actions) — both trigger server-side analysis.

The chapter claim *"AnalysisException always arrives with the action result — never before it"* is **wrong for Connect mode**. Accessing `.schema` on a Connect DataFrame triggers `AnalyzePlan` → server analysis → can raise `AnalysisException` before any action.

---

## Implications for chapter content

- **"Tasks do not return results to the driver" for ShuffleMapStage is wrong** — they return `MapStatus` (metadata, not user data)
- **"ResultStage runs on all partitions" is wrong** — `first()` runs on 1 partition, `lookup(key)` runs on 1 partition
- **"DataFrame becomes RDD[InternalRow]" is accurate** but misleading — it's the scheduling shell; Tungsten does the computation inside via `mapPartitionsWithIndex`
- **"UnsafeRow is off-heap"** is wrong by default — on-heap unless `spark.memory.offHeap.enabled=true`
- **`sun.misc.Unsafe` ≠ off-heap allocation** — it's the byte-level write API used on an ordinary on-heap byte array
