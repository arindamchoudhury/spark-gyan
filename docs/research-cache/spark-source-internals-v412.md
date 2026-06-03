# Spark 4.1.2 Source-Verified Internals

Facts verified by reading source code at `C:\opt\learn\spark\spark` tag `v4.1.2`.
Do not re-search these — read directly from source if deeper detail is needed.

Last verified: **2026-06-03**

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

## Implications for chapter content

- **"Tasks do not return results to the driver" for ShuffleMapStage is wrong** — they return `MapStatus` (metadata, not user data)
- **"ResultStage runs on all partitions" is wrong** — `first()` runs on 1 partition, `lookup(key)` runs on 1 partition
- **"DataFrame becomes RDD[InternalRow]" is accurate** but misleading — it's the scheduling shell; Tungsten does the computation inside via `mapPartitionsWithIndex`
- **"UnsafeRow is off-heap"** is wrong by default — on-heap unless `spark.memory.offHeap.enabled=true`
- **`sun.misc.Unsafe` ≠ off-heap allocation** — it's the byte-level write API used on an ordinary on-heap byte array
