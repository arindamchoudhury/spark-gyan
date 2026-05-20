# Catalyst Optimizations: Column Pruning & Predicate Pushdown

> *Cross-chapter synthesis — Rioux (2022), Chapters 2, 4.*
>
> Catalyst is Spark's query optimiser. Two of its most impactful rules — column pruning and predicate pushdown — can eliminate the majority of I/O before a single row is processed. Both are automatic, but they are silently disabled by common coding patterns.

---

## Ch 2 — How the optimisations work

**Column pruning** — Catalyst inspects which columns appear anywhere downstream in the plan (in `filter`, `select`, `agg`, `join`). Columns that are never referenced are dropped from the scan. For columnar formats (Parquet, ORC, Delta), this means Spark never reads those column files from disk.

**Predicate pushdown** — `filter` conditions that depend only on source columns are moved as close to the data source as possible. For Parquet/Delta, this activates row-group and file-level skipping (min/max statistics). For JDBC, Catalyst rewrites the filter into a SQL `WHERE` clause so the database executes it — Spark never receives the filtered-out rows.

Both optimisations apply together: Spark reads only the rows that pass the filter, and reads only the columns those rows need.

**Verifying with `explain()`:**

```python
df = spark.read.parquet("data/")
result = df.filter(F.col("country") == "CA").select("id", "name")
result.explain(True)
```

Look for `PushedFilters` in the physical plan output — each entry is a filter the source connector will execute. Look for `ReadSchema` to confirm only the referenced columns are listed.

---

## What disables the optimisations

### Column pruning breaks

| Pattern | Why pruning breaks |
|---|---|
| `select("*")` | Spark cannot determine which columns are actually needed |
| UDF that takes a whole row (`F.udf(lambda row: …)` on a `StructType`) | Opaque to Catalyst — must pass all fields |
| `df.toPandas()` before narrowing | Forces full materialisation before the Python boundary |

### Predicate pushdown breaks

| Pattern | Why pushdown breaks |
|---|---|
| `F.udf()` in the filter predicate | UDFs are opaque; Catalyst cannot push them into the source |
| Complex Python expressions mixed with column expressions | Catalyst cannot translate to source-native SQL/min-max |
| Calling `cache()` before filtering | The cached dataset is already materialised; the filter runs in-memory on the full data |

---

## Ch 4 — JDBC query rewriting and Delta data skipping

**JDBC pushdown.** When reading from a relational database via `spark.read.jdbc(…)`, Catalyst converts simple column-equality and range predicates into a SQL `WHERE` clause injected into the JDBC subquery. The database handles the filter — Spark only receives matching rows.

```python
df = (
    spark.read.jdbc(
        url=jdbc_url,
        table="orders",
        properties={"user": "…", "password": "…"},
    )
    .filter(F.col("region") == "EMEA")   # pushed into JDBC query
    .select("order_id", "amount")         # column pruning applied
)
df.explain(True)
# Physical plan shows: PushedFilters: [IsNotNull(region), EqualTo(region,EMEA)]
```

**Delta Lake data skipping.** Delta tables maintain min/max statistics per file for every column. Predicate pushdown on a Delta table uses these statistics to skip entire files whose min/max range cannot contain matching rows — no row-group scan needed.

**Z-ordering** (Delta `OPTIMIZE … ZORDER BY`) co-locates rows with similar values of the Z-order key into the same files, maximising the effectiveness of predicate pushdown for high-cardinality columns like `user_id` or `event_date`.

```sql
-- After writing a Delta table:
OPTIMIZE orders ZORDER BY (region, event_date);
```

Once Z-ordered, a filter on `region = 'EMEA'` may skip 95%+ of files because matching rows are clustered together.

---

## Summary

- Column pruning reads only referenced columns; predicate pushdown reads only matching rows.
- Both are automatic and apply to Parquet, ORC, Delta, and JDBC sources.
- `select("*")` disables column pruning. UDFs in filters disable predicate pushdown.
- Verify with `df.explain(True)` — look for `PushedFilters` and `ReadSchema`.
- JDBC pushdown rewrites predicates into SQL `WHERE`; Delta data skipping uses file-level statistics.
- Z-ordering clusters related rows to maximise file-level skipping.

---

## Chapter links

- [Ch 2 — First data program](../books/rioux/chapters/02-first-data-program.md)
- [Ch 4 — Tabular data and CSV ingestion](../books/rioux/chapters/04-tabular-data.md)
