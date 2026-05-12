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
