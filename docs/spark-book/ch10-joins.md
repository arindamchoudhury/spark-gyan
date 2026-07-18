# Chapter 08 — Joins: Types and Mechanics

> *Learning-path topic: B7 (Beginner)*
> *Written: 2026-05-31 · Spark 4.1.x / Python 3.10+*

> 🔄 **Needs revisiting — Spark 4.2.0 (flagged 2026-07-18).** Nothing here is wrong, but the chapter is now incomplete: Spark 4.2.0 adds `NEAREST BY` ([SPARK-56395]), a top-K ranking join primitive for nearest-neighbour queries with Catalyst support and a DataFrame API. It is not one of the seven relational join types this chapter covers and needs its own section — probably after the seven, framed as "the eighth thing called a join that isn't one."

Joins are the most common source of performance problems in Spark, and the most common source of subtle data bugs. Getting join types right is foundational; everything in advanced tuning (Chapter 22) builds on this.

---

## What you'll learn

- The seven join types and when to use each
- The three-ingredient join blueprint: left, predicate, method
- How equi-join shorthand deduplicates join keys automatically
- How to resolve column name clashes after a join
- Where broadcast joins apply and why they matter

---

## The problem this solves

You have an events table and a users table. You want to enrich events with user metadata, but you cannot guarantee every event has a matching user. An inner join silently drops unmatched events. A left join preserves them with nulls. Using the wrong join type produces a "correct-looking" result with fewer rows than expected — the quietest category of data bug.

---

## Core concept

Every PySpark join follows one blueprint:

```python
left.join(right, on=predicate, how="join_type")
```

The `on` predicate defines which rows match. The `how` method defines what to do with non-matching rows.

**The seven join types:**

| `how=` | Left rows kept | Right rows kept | Use when |
|---|---|---|---|
| `"inner"` (default) | Match only | Match only | You need only rows that exist on both sides |
| `"left"` | All | Match only | You want all left rows; fill missing right data with null |
| `"right"` | Match only | All | Inverse of left |
| `"outer"` / `"full"` | All | All | You want all rows from both, filling gaps with null |
| `"left_semi"` | Match only | Not returned | Check which left rows *have* a match without pulling right columns |
| `"left_anti"` | No-match only | Not returned | Check which left rows *have no* match |
| `"cross"` | All | All (× N) | Cartesian product — extremely expensive, avoid |

**Equi-join shorthand:** when joining on equality between identically named columns, pass the column name as a string. This is the preferred form — it is shorter and automatically deduplicates the join key column in the result:

```python
events.join(users, on="user_id", how="left")
# result has one "user_id" column, not two
```

**Column name clashes:** joining two DataFrames that share a column name (other than the key used in equi-join shorthand) creates an ambiguous duplicate. Reference the post-join duplicate by DataFrame origin, then drop one:

```python
result = events.join(users, events["user_id"] == users["user_id"])
result = result.drop(users["user_id"])  # drop the right-side copy
```

---

## Examples

### Minimal example: inner and left joins

```python
# Apache Spark 4.1.x / PySpark 4.1.x · Python 3.10+
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ch07").master("local[*]").getOrCreate()

events = spark.createDataFrame(
    [("e1", "u1", "click"), ("e2", "u2", "buy"), ("e3", "u99", "view")],
    ["event_id", "user_id", "action"]
)
users = spark.createDataFrame(
    [("u1", "Alice", "US"), ("u2", "Bob", "UK")],
    ["user_id", "name", "country"]
)

# Inner join — drops e3 (u99 has no matching user)
events.join(users, on="user_id", how="inner").show()
# +-------+--------+------+-----+-------+
# |user_id|event_id|action| name|country|
# +-------+--------+------+-----+-------+
# |     u1|      e1| click|Alice|     US|
# |     u2|      e2|   buy|  Bob|     UK|
# +-------+--------+------+-----+-------+

# Left join — keeps e3 with nulls for missing user columns
events.join(users, on="user_id", how="left").show()
# +-------+--------+------+-----+-------+
# |user_id|event_id|action| name|country|
# +-------+--------+------+-----+-------+
# |     u1|      e1| click|Alice|     US|
# |     u2|      e2|   buy|  Bob|     UK|
# |    u99|      e3|  view| null|   null|
# +-------+--------+------+-----+-------+
```

### Building up: semi, anti, and column clash resolution

```python
# Apache Spark 4.1.x / PySpark 4.1.x · Python 3.10+
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ch07-semi").master("local[*]").getOrCreate()

events = spark.createDataFrame(
    [("e1","u1","click"), ("e2","u2","buy"), ("e3","u99","view")],
    ["event_id","user_id","action"]
)
users = spark.createDataFrame([("u1","Alice"), ("u2","Bob")], ["user_id","name"])

# Semi join — which events have a known user? (left columns only, no duplication)
events.join(users, on="user_id", how="left_semi").show()
# +-------+--------+------+
# |user_id|event_id|action|
# +-------+--------+------+
# |     u1|      e1| click|
# |     u2|      e2|   buy|
# +-------+--------+------+

# Anti join — which events have NO known user?
events.join(users, on="user_id", how="left_anti").show()
# +-------+--------+------+
# |user_id|event_id|action|
# +-------+--------+------+
# |    u99|      e3|  view|
# +-------+--------+------+

# Compound predicate with column clash resolution
orders = spark.createDataFrame(
    [("o1","u1","US",100), ("o2","u2","UK",200)],
    ["order_id","user_id","region","amount"]
)
regions = spark.createDataFrame([("US","North America"), ("UK","Europe")], ["region","zone"])

# Both tables have "region" — use explicit column references
result = (
    orders.join(regions, orders["region"] == regions["region"], how="inner")
    .drop(regions["region"])  # drop duplicate; keep orders.region
    .select("order_id", "user_id", "region", "amount", "zone")
)
result.show()
# +--------+-------+------+------+-------------+
# |order_id|user_id|region|amount|         zone|
# +--------+-------+------+------+-------------+
# |      o1|     u1|    US|   100|North America|
# |      o2|     u2|    UK|   200|       Europe|
# +--------+-------+------+------+-------------+
```

### Broadcast join hint for small tables

```python
# Apache Spark 4.1.x / PySpark 4.1.x · Python 3.10+
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ch07-broadcast").master("local[*]").getOrCreate()

# When the right table is small, broadcast it to all executors — avoids shuffle
events = spark.range(1_000_000).withColumn("user_id", (F.col("id") % 100).cast("string"))
small_lookup = spark.createDataFrame(
    [(str(i), f"user_{i}") for i in range(100)], ["user_id", "name"]
)

result = events.join(F.broadcast(small_lookup), on="user_id", how="left")
result.explain()
# Physical plan shows BroadcastHashJoin instead of SortMergeJoin
```

---

## Common pitfalls

- **Using inner join when you need left** — inner join silently drops rows with no match. If downstream metrics are unexpectedly low, check for an inner join that should be a left join.
- **Not specifying `how=` explicitly** — the default is `inner`. A join that accidentally drops rows is one of the hardest bugs to spot. Always write `how="left"`, `how="inner"`, etc. — make intent explicit.
- **Row explosion from many-to-many joins** — if both sides have duplicate join keys, the result is the cartesian product of matching rows. A key appearing 100 times on each side produces 10,000 result rows. Always check the row count before and after a join during development.
- **Ambiguous column names after a join** — joining two DataFrames that share a non-key column name creates a column reference that raises `AnalysisException` when accessed. Use `DataFrame.alias()` on both sides or `drop()` the duplicate immediately after the join.
- **Cross join with `how="cross"`** — the result is m × n rows. With two tables of 10,000 rows each, that is 100 million output rows. Use only when you genuinely need all combinations, and always `.limit()` in development.

---

## Exercises

1. **Recall** — What is the difference between a `left_semi` join and an `inner` join? In what situation would you choose one over the other?

2. **Apply** — Create two DataFrames: `orders (order_id, customer_id, amount)` and `customers (customer_id, name, country)`. Perform a left join and then verify that the row count of the result equals the row count of `orders`. Then do an inner join and check how many rows are lost.

3. **Extend** — Create a many-to-many join scenario where an order can have multiple items and an item can appear in multiple orders. Observe the row explosion. Then describe two ways to handle this: (1) deduplicate before joining, (2) use `left_semi` instead of `inner`.

---

## Summary

- Every join needs three things: left table, predicate, join type (`how=`). Always specify `how=` explicitly.
- Equi-join shorthand (`on="col_name"`) is preferred for equality on identically-named columns — it deduplicates the key automatically.
- Inner join drops unmatched rows; left join preserves all left rows with nulls for missing right data.
- Semi/anti joins return only left columns and don't duplicate rows — use them for existence checks.
- Broadcast small lookup tables to avoid shuffle: `events.join(F.broadcast(lookup), ...)`.
- Chapter 9 covers Spark SQL — a declarative alternative for the same join and aggregation operations.

---

## References

- [PySpark join API](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.join.html)
- [PySpark broadcast function](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.broadcast.html)
