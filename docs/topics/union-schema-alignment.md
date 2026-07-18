# `union()` and Multi-Source Schema Alignment

> *Cross-chapter synthesis — Rioux (2022), Chapters 3, 7.*
>
> `union()` stacks DataFrames vertically. It is positional, not name-based, which makes schema alignment a hard requirement. Ch 3 introduces the function; Ch 7 covers the multi-source pattern with `functools.reduce`, column intersection for alignment, and using `assert` as an early schema contract.

---

## Ch 3 — `union()` introduced

```python
combined = df_a.union(df_b)
```

`union()` requires both DataFrames to have the same number of columns. Columns are matched **by position**, not by name. If `df_a` has columns `["id", "name", "amount"]` and `df_b` has columns `["name", "id", "amount"]` in that order, `union()` will silently map `df_b.name` to `df_a.id` — wrong, with no error.

`union()` is `UNION ALL` semantics — it does not deduplicate. Use `distinct()` after if you need deduplication:

```python
combined = df_a.union(df_b).distinct()
```

---

## Ch 7 — `union()` = SQL `UNION ALL`

The SQL vocabulary mapping:

| SQL | PySpark |
|---|---|
| `UNION ALL` | `.union()` |
| `UNION` (dedup) | `.union().distinct()` |
| `UNION ALL` with reorder | `.select(common_cols).union(other.select(common_cols))` |

---

## Ch 7 — Column intersection for schema alignment

When stacking DataFrames from multiple sources (e.g., monthly CSV files with slightly different schemas), find the common columns and select only those before unioning:

```python
def align_schemas(*dfs):
    """Select the intersection of all DataFrame schemas before union."""
    common = set(dfs[0].columns)
    for df in dfs[1:]:
        common &= set(df.columns)
    common = sorted(common)   # deterministic ordering
    return [df.select(common) for df in dfs]

aligned = align_schemas(jan, feb, mar)
```

This is a pragmatic approach when sources have additive schema evolution — new columns in some months that don't exist in others.

---

## Ch 7 — `functools.reduce` for N-source union

For two DataFrames, `a.union(b)` is readable. For N DataFrames, chaining produces deeply nested code. Use `functools.reduce`:

```python
from functools import reduce

monthly_files = ["jan.csv", "feb.csv", "mar.csv", "apr.csv"]
dfs = [spark.read.csv(f, header=True, schema=schema) for f in monthly_files]

combined = reduce(lambda a, b: a.union(b), dfs)
# or equivalently:
from pyspark.sql import DataFrame
combined = reduce(DataFrame.union, dfs)
```

`reduce` applies `.union()` left-associatively: `((jan.union(feb)).union(mar)).union(apr)`.

---

## Ch 7 — `assert` as early schema contract

Before unioning DataFrames from different sources, assert the columns you expect. This fails immediately at data-loading time with a clear message rather than silently misaligning columns:

```python
REQUIRED_COLS = {"id", "name", "amount", "region"}

def load_and_validate(path: str) -> DataFrame:
    df = spark.read.csv(path, header=True, schema=schema)
    assert set(df.columns) >= REQUIRED_COLS, (
        f"{path} missing columns: {REQUIRED_COLS - set(df.columns)}"
    )
    return df.select(sorted(REQUIRED_COLS))   # deterministic column order

combined = reduce(DataFrame.union, [load_and_validate(p) for p in paths])
```

The `assert` is a schema contract at the ingestion boundary. It converts a silent misalignment (wrong data, no error) into a loud failure (immediate `AssertionError` with a useful message).

> Note: `assert` statements are disabled when Python runs with `-O` (optimize flag). For production pipelines, replace `assert` with an explicit `if … raise ValueError(…)`.

---

## Complete multi-source union pattern

```python
from functools import reduce
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
import pyspark.sql.types as T

SCHEMA = T.StructType([
    T.StructField("id",     T.LongType()),
    T.StructField("name",   T.StringType()),
    T.StructField("amount", T.DoubleType()),
    T.StructField("region", T.StringType()),
])

REQUIRED = {"id", "name", "amount", "region"}

def load(path: str) -> DataFrame:
    df = spark.read.schema(SCHEMA).csv(path, header=True)
    missing = REQUIRED - set(df.columns)
    if missing:
        raise ValueError(f"{path} missing columns: {missing}")
    return df.select(sorted(REQUIRED))

paths = ["data/jan.csv", "data/feb.csv", "data/mar.csv"]
combined = reduce(DataFrame.union, [load(p) for p in paths])
combined.show(5)
```

---

## Summary

- `union()` is positional — columns are matched by position, not name. Column order must match.
- `union()` = `UNION ALL`; use `.union().distinct()` for SQL `UNION`.
- Use column intersection (`set(a.columns) & set(b.columns)`) to align schemas when sources diverge.
- Use `functools.reduce(DataFrame.union, dfs)` to union N DataFrames without nesting.
- Assert or validate expected columns at the ingestion boundary to catch misalignment immediately.

---

## Chapter links

- [Ch 3 — Submitting and Scaling Your First PySpark Program](../books/rioux/chapters/03-submitting-scaling.md)
- [Ch 7 — Python, Spark SQL, and interoperability](../books/rioux/chapters/07-python-sql.md)
