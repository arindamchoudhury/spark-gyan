# Explode and Array Expansion Patterns

> *Cross-chapter synthesis — Rioux (2022), Chapters 2, 6.*
>
> `explode()` and its variants convert array (or map) columns into rows — one row per element. Ch 2 introduces the basics; Ch 6 shows how explode and its inverse (`collect_list`/`collect_set`) fit into hierarchical data modelling, and how higher-order functions let you transform arrays without exploding at all.

---

## Ch 2 — `explode()` and `posexplode()`

**`F.explode(col)`** — creates one output row per element of an array or map. The original row is duplicated for every element.

```python
df = spark.createDataFrame([
    (1, ["a", "b", "c"]),
    (2, ["x"]),
], ["id", "tags"])

df.select("id", F.explode("tags").alias("tag")).show()
# id | tag
# 1  | a
# 1  | b
# 1  | c
# 2  | x
```

**`F.posexplode(col)`** — same as `explode` but also produces a positional index column named `pos`:

```python
df.select("id", F.posexplode("tags").alias("pos", "tag")).show()
# id | pos | tag
# 1  | 0   | a
# 1  | 1   | b
# 1  | 2   | c
```

**`F.explode_outer(col)`** — preserves rows where the array is empty or null (emits a single null-valued row instead of dropping the row entirely):

```python
# array is null → explode drops the row; explode_outer keeps it with null element
df.select("id", F.explode_outer("tags").alias("tag"))
```

---

## Ch 6 — The explode/collect cycle in hierarchical models

Chapter 6 frames the explode↔collect operations as the bridge between hierarchical (array-of-structs) and tabular (flat row) representations:

```
Hierarchical model          Tabular model
─────────────────          ──────────────────────
user_id | orders            user_id | order_id | amount
--------|--------           --------|----------|-------
u1      | [{o1,10},         u1      | o1       | 10
        |  {o2,20}]         u1      | o2       | 20

      explode ─────────────────────────→
      ←────────────────────── collect_list
```

**`F.collect_list(col)`** — aggregates values into an array, preserving duplicates and order (order is non-deterministic unless `orderBy` is applied first inside a window).

**`F.collect_set(col)`** — aggregates values into an array, deduplicating. Order is not guaranteed.

```python
# Tabular → hierarchical
orders_nested = (
    flat_orders
    .groupBy("user_id")
    .agg(F.collect_list(F.struct("order_id", "amount")).alias("orders"))
)

# Hierarchical → tabular
flat_orders = (
    orders_nested
    .select("user_id", F.explode("orders").alias("order"))
    .select("user_id", F.col("order.order_id"), F.col("order.amount"))
)
```

**Collect-map pattern** — using `collect_list` of `F.struct(key, value)` pairs then converting to a map:

```python
from pyspark.sql import functions as F

props_map = (
    props
    .groupBy("entity_id")
    .agg(F.map_from_entries(F.collect_list(F.struct("key", "value"))).alias("props_map"))
)
```

---

## Ch 6 — `F.inline()` for array-of-structs

`F.inline(col)` explodes an array-of-structs column and promotes each struct field to its own column in a single step — equivalent to `explode()` followed by `select(F.col("elem.*"))`:

```python
df.select(F.inline("orders"))
# Produces one row per order with columns: order_id, amount, status
# (the struct field names become the column names)
```

---

## Ch 6 — Higher-order functions (no explode needed)

For transforming array contents without changing cardinality, use higher-order functions — they avoid the row-multiplication cost of explode/collect:

| Function | SQL equivalent | Effect |
|---|---|---|
| `F.transform(col, f)` | `TRANSFORM(arr, x -> expr)` | Map a lambda over every element |
| `F.filter(col, f)` | `FILTER(arr, x -> cond)` | Keep elements matching predicate |
| `F.aggregate(col, init, merge, finish)` | `AGGREGATE(…)` | Fold / reduce to a single value |
| `F.exists(col, f)` | `EXISTS(arr, x -> cond)` | Boolean: any element matches |

```python
# Double every price in the prices array
df.withColumn("doubled", F.transform(F.col("prices"), lambda x: x * 2))

# Keep only positive prices
df.withColumn("positive", F.filter(F.col("prices"), lambda x: x > 0))

# Sum the prices array
df.withColumn("total", F.aggregate(F.col("prices"), F.lit(0), lambda acc, x: acc + x))

# Any price over 100?
df.withColumn("any_expensive", F.exists(F.col("prices"), lambda x: x > 100))
```

Higher-order functions are Spark's native alternative to explode-transform-collect when only the array contents need changing.

---

## Summary

- `explode()` one row → N rows (drops null/empty); `explode_outer()` preserves null/empty rows with a null element.
- `posexplode()` adds a `pos` index column alongside the element.
- `collect_list()` / `collect_set()` are the inverse: N rows → one row with an array column.
- `F.inline()` explodes array-of-structs and promotes struct fields to columns in one step.
- Higher-order functions (`transform`, `filter`, `aggregate`, `exists`) transform arrays in-place without changing cardinality — prefer them over explode when you don't need to change row count.

---

## Chapter links

- [Ch 2 — First data program](../books/rioux/chapters/02-first-data-program.md)
- [Ch 6 — JSON and complex types](../books/rioux/chapters/06-json-data.md)
