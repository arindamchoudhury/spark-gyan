# `select()` Best Practices

> *Cross-chapter synthesis — Rioux (2022), Chapters 2, 4, 5.*
>
> `select()` is PySpark's most-used transformation. Used well, it drives column pruning and keeps schemas readable; used carelessly, it widens schemas, breaks Catalyst optimisations, and accumulates technical debt.

---

## Ch 2 — The foundational pattern

The core idiom: `select()` with `F.col()` and `.alias()` for any computed column.

```python
result = df.select(
    F.col("id"),
    F.col("raw_price").cast("double").alias("price"),
    F.upper(F.col("name")).alias("name_upper"),
)
```

Always alias computed columns. Without an alias, Spark auto-names them from the expression string (e.g., `upper(name)`) — these names are fragile and break downstream code when the expression changes.

---

## Ch 4 — Nine best practices

**1. Avoid `select("*")` in production pipelines.**
`select("*")` reads every column, disabling column pruning for all upstream sources. Use an explicit column list instead.

```python
# Bad
df.select("*")

# Good
df.select("id", "name", "amount")
```

**2. Prefer `withColumns({…})` over repeated `withColumn()`.**
Each `withColumn()` call adds a projection node to the logical plan. Many chained calls can create deeply nested plans that are slow to optimise. Batch them:

```python
# Bad — N separate plan nodes
df = df.withColumn("a", expr_a)
df = df.withColumn("b", expr_b)
df = df.withColumn("c", expr_c)

# Good — one projection node
df = df.withColumns({"a": expr_a, "b": expr_b, "c": expr_c})
```

`withColumns()` (plural) was added in Spark 3.3.

**3. Use `drop()` only for removing one or two columns.**
If you want most columns except a few, `drop()` is readable. If you want a minority of all columns, name them explicitly in `select()`.

**4. Never produce duplicate column names.**
`df.select("id", "id")` silently succeeds but creates a DataFrame with two columns both named `id`. Any downstream `select("id")` raises `AnalysisException: Reference 'id' is ambiguous`.

**5. Cast in `select()`, not in a separate `withColumn()`.**
Combining the cast and the rename into one `select()` call is more readable and produces one fewer plan node:

```python
# Combined
df.select(F.col("ts").cast("timestamp").alias("created_at"))

# Separate (less clean)
df.withColumn("ts", F.col("ts").cast("timestamp")) \
  .withColumnRenamed("ts", "created_at")
```

**6. Alias all expressions, never rely on auto-generated names.**
Auto-names like `(price * 1.1)` break silently when the expression is refactored.

**7. UDFs block column pruning for their input columns.**
A UDF's input columns are opaque to Catalyst — it cannot tell which fields the Python function uses. If you pass a struct column to a UDF, Catalyst must read all fields of that struct. Narrow inputs before calling the UDF:

```python
# UDF receives the full struct — all fields read from disk
df.select(my_udf(F.col("nested"))).show()

# Better: extract only what the UDF needs
df.select(my_udf(F.col("nested.field_a"), F.col("nested.field_b"))).show()
```

**8. Apply `select()` early in pipelines to narrow the schema.**
A narrow DataFrame is cheaper to process at every subsequent stage.

**9. Use `toDF(*new_names)` for bulk rename.**
When a source produces positionally-ordered columns and you want to rename all at once:

```python
df = raw.toDF("id", "name", "amount", "ts")
```

---

## Ch 5 — Re-select after joins

After a join, the result DataFrame contains columns from both inputs, potentially with duplicate names. Always `select()` explicitly after a join to define the output schema:

```python
result = (
    orders
    .join(customers, on="customer_id", how="left")
    .select(
        F.col("order_id"),
        F.col("customer_id"),
        F.col("amount"),
        F.col("name").alias("customer_name"),  # from customers
    )
)
```

Skipping the post-join `select()` is the most common way to accidentally propagate ambiguous column names into downstream transformations.

---

## Summary

- Alias all computed columns; never rely on auto-generated expression names.
- Avoid `select("*")` in production — it disables column pruning.
- Batch multiple `withColumn()` calls into `withColumns({…})` (Spark 3.3+).
- UDFs receive their input columns opaquely — narrow inputs to avoid reading unused fields.
- Always `select()` explicitly after joins to define the output schema.
- Apply `select()` early in pipelines to keep schemas narrow throughout.

---

## Chapter links

- [Ch 2 — First data program](../books/rioux/chapters/02-first-data-program.md)
- [Ch 4 — Tabular data and CSV ingestion](../books/rioux/chapters/04-tabular-data.md)
- [Ch 5 — Joins and aggregations](../books/rioux/chapters/05-joins-aggregations.md)
