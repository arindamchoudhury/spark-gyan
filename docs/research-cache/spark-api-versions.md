# Spark API versions — verified facts

Lookup cache to avoid re-searching. Check date before trusting — facts may have shifted if a new Spark release landed.

Last verified: **2026-05-12** against Spark 4.1.1 docs.

---

## Aggregate functions — version added

| Function | Added | Notes |
|---|---|---|
| `F.median(col)` | 3.4 | |
| `F.mode(col)` | 3.4 | `deterministic` parameter added in 4.0 |
| `F.count_if(condition)` | 3.5 | |
| `F.any_value(col)` | 3.5 | |
| `F.bit_and/or/xor(col)` | 3.5 | |
| `F.try_sum(col)` | 3.5 | Returns null on overflow instead of throwing |
| `F.try_avg(col)` | 3.5 | Returns null on overflow instead of throwing |

---

## Cast / ANSI mode

- ANSI mode is **on by default in Spark 4.0+**.
- Invalid string → numeric cast raises `SparkNumberFormatException` at runtime (not `AnalysisException`).
- Integer overflow raises `SparkArithmeticException` at runtime.
- `try_cast` is a **Column method**, not a standalone function: `F.col("x").try_cast(T.IntegerType())`. There is no `F.try_cast(...)`.

---

## Array functions

- `F.size(col)` returns **-1** for a null array (not `null`, not `0`). `F.coalesce` is useless for null-guarding it.
- `F.array_size(col)` (added **3.5**) returns `null` for null input — safe with standard null-handling patterns.
- `F.array_position(col, value)` accepts a `Column` as `value` since **4.0** (previously only literals).
- `F.element_at` is **1-based**; `col[0]` / `getItem(0)` / `F.get(col, 0)` are **0-based**.
- `F.lit()` accepts Python lists directly since **3.4**, returning `ArrayType`.

---

## Struct / Map access

- Dot notation (`F.col("outer.field")`) is the default for struct field access.
- `getField("field")` is the escape hatch for field names with dots, spaces, or special characters.
- Bracket notation (`F.col("outer")["field"]`) calls `getItem` internally — reserve for maps and arrays.
- Dot notation on a `MapType` column raises `AnalysisException` — only bracket or `getItem` work for maps.

---

## Spark Connect defaults (4.x)

- `pyspark` REPL defaults to **Connect mode** — attempts to connect to a local Spark Connect server on port 15002 at startup.
- Plain scripts using `SparkSession.builder` use **classic mode** unless the `SPARK_REMOTE` environment variable is set.
- `spark.api.mode` config (`"connect"` / `"classic"`) controls which mode is used; added in **4.0**.

---

## `cache()` storage level

- `DataFrame.cache()` uses **`MEMORY_AND_DISK_DESER`** storage level (not `MEMORY_AND_DISK`).
- `MEMORY_AND_DISK_DESER` stores deserialized Java objects in memory; spills to disk when memory is insufficient.

---

## `split()` limit parameter

- `limit` parameter added in **Spark 3.0** with default `-1`.
- Prior behaviour (pre-3.0) matched Java's `String.split(pattern, 0)` — discards trailing empty strings.
- With `limit=-1` (default since 3.0) trailing empty strings are preserved.

---

## Exception module (3.4+)

- Exceptions moved to `pyspark.errors` in Spark 3.4+.
- `pyspark.sql.utils` still re-exports them for backwards compatibility, but prefer the new import path.

---

## SparkR

- SparkR is **deprecated in Spark 4.x** — do not use for new projects.

---

## pandas UDF (pandas_udf / Arrow)

Last verified: **2026-05-20** against Spark 4.1.1 / PySpark 4.1.1.

| Claim | Verified value | Notes |
|---|---|---|
| pandas minimum version (Spark 4.1) | **2.2.0** | Was 2.0.0 in Spark 4.0; was 1.0.5 in Spark 3.x |
| `convertToArrowArraySafely` default | **on** (Spark 4.1) | Unsafe Arrow casts (int overflow, float truncation) now raise errors |
| Arrow-native UDFs | Added **Spark 4.1** | `@F.udf(..., useArrow=True)` / `@F.udtf(..., useArrow=True)`; bypass pandas entirely |
| Iterator API in GROUPED_MAP | Added **Spark 4.1** | `applyInPandas` now accepts an iterator of DataFrames variant |
| Keyword arguments in SCALAR/GROUPED_AGG | Added **Spark 4.0** | Typed pandas UDFs with function decorators enhanced |
| Legacy `PandasUDFType.SCALAR` syntax | Deprecated (use type hints) | Removed need for explicit PandasUDFType since Spark 3.0 |
| `ARROW_PRE_0_15_IPC_FORMAT=1` workaround | **Obsolete** (was Spark 2.x only) | Not needed on Spark 3.0+ |
| Default Arrow batch size | **10,000 records** | `spark.sql.execution.arrow.maxRecordsPerBatch` |

Sources: [PySpark 4.1.1 pandas_udf docs](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.pandas_udf.html), [Spark 4.1.0 release notes](https://spark.apache.org/releases/spark-release-4.1.0.html), [PySpark upgrade guide](https://spark.apache.org/docs/latest/api/python/migration_guide/pyspark_upgrade.html)

---

## Window functions (pyspark.sql.window)

Last verified: **2026-05-31** against Spark 4.1.1.

| Claim | Verified value | Notes |
|---|---|---|
| `Window` import path | `from pyspark.sql.window import Window` | Stable since Spark 1.4.0 |
| Spark Connect support | Added **3.4.0** | `WindowSpec` works over Spark Connect |
| API changes in Spark 4.x | None — API stable | No breaking changes to Window/WindowSpec |
| UDF over unbounded window | Spark **2.4+** | Series → scalar pandas UDF required |
| UDF over bounded window | Spark **3.0+** | Series → scalar pandas UDF required |
| `PandasUDFType.GROUPED_AGG` decorator | **Deprecated** (use type hints since 3.0) | Still works but avoid for new code |
| `F.median()` as window function | **Unverified** — added as aggregate in 3.4 but `.over()` support not confirmed in official docs | Use custom pandas UDF as fallback |

Sources: [PySpark Window API](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Window.html), [PySpark window module](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/window.html)
