# Column References: `F.col()` vs Strings vs Dot Notation

> *Cross-chapter synthesis — Rioux (2022), Chapters 2, 4, 5, 6.*
>
> PySpark offers four syntactically distinct ways to refer to a column. They are not equivalent: each has a specific domain of applicability, and mixing them in the wrong context causes `AnalysisException` or silently produces wrong results.

---

## Ch 2 — The four forms introduced

| Form | Syntax | Type |
|---|---|---|
| String | `"col_name"` | Python `str` |
| `F.col()` | `F.col("col_name")` | `Column` |
| `df["col_name"]` | `df["col_name"]` | `Column` bound to `df` |
| Attribute | `df.col_name` | `Column` bound to `df` |

All four produce a `Column` object except bare strings, which are interpreted as column names only inside certain functions (e.g., `groupBy("name")`, `orderBy("amount")`).

---

## Ch 4 — The precise rule: when to use each form

**Use a plain string when passing a column name as a selector** — i.e., inside `select`, `drop`, `groupBy`, `orderBy`, and `partitionBy` when you just need the column itself without transformation:

```python
df.select("id", "name")
df.groupBy("country")
df.orderBy("created_at", ascending=False)
```

**Use `F.col()` for everything else** — when you apply operators, methods, or functions to a column:

```python
df.filter(F.col("amount") > 0)
df.withColumn("upper_name", F.upper(F.col("name")))
df.select(F.col("price") * F.col("qty"))
```

`F.col("x") > 0` creates a `Column` expression. `"x" > 0` in Python compares the string to the integer and returns `False` — not a Column expression. This is the most common mistake.

**Avoid `df["col"]` and `df.col` in transformations** — they bind the Column to a specific DataFrame object. When used inside a chained transformation that creates a new DataFrame, the reference can become stale and raise `AnalysisException`. Reserve these forms for interactive exploration.

---

## Ch 5 — Join predicate disambiguation

After a join, both DataFrames may contribute a column with the same name. `F.col("id")` is now ambiguous — Spark raises `AnalysisException: Reference 'id' is ambiguous`.

**Three solutions, in order of preference:**

**1. Equi-join shorthand (string predicate) — Spark auto-deduplicates:**

```python
joined = left.join(right, on="user_id", how="inner")
# "user_id" appears only once in the result
```

**2. Origin-name predicate — keep both, qualify later:**

```python
joined = left.join(right, on=left["user_id"] == right["user_id"], how="inner")
# Both "user_id" columns exist; select by origin
joined.select(left["user_id"], right["amount"])
```

**3. Alias before join — rename to avoid collision:**

```python
left_a = left.alias("l")
right_a = right.alias("r")
joined = left_a.join(right_a, on=F.col("l.user_id") == F.col("r.user_id"))
joined.select(F.col("l.user_id"), F.col("r.amount"))
```

The dot notation `F.col("alias.col_name")` is used **only** with DataFrame aliases after a join — it is not general Python attribute access.

---

## Ch 6 — Array/struct access conventions

For **struct fields**, use dot notation inside `F.col()`:

```python
df.select(F.col("address.city"))
df.select(F.col("address.geo.lat"))
```

For **array elements**, use bracket subscript inside `F.col()`:

```python
df.select(F.col("tags")[0])      # first element
df.select(F.col("scores")[2])    # third element
```

For **map keys**, use the same bracket notation:

```python
df.select(F.col("metadata")["source"])
```

These are `Column` expression operators, not Python dict/list access — they work within Spark's execution engine, not in Python.

**Do not** use Python's `df.struct_col.field` attribute chain inside transformations — it works interactively but is fragile in pipelines. Always use `F.col("struct_field.nested_field")`.

---

## Summary

- Plain strings work as column selectors in `select`, `groupBy`, `orderBy`; use `F.col()` for any expression.
- `"x" > 0` is a Python boolean; `F.col("x") > 0` is a Spark `Column` expression.
- `df["col"]` and `df.col` bind to a DataFrame instance — avoid in chained transformations.
- After joins, use equi-join shorthand, origin binding (`left["col"]`), or alias dot notation (`F.col("alias.col")`) to resolve ambiguity.
- Struct: `F.col("parent.child")`; Array/Map: `F.col("arr")[0]` / `F.col("map")["key"]`.

---

## Chapter links

- [Ch 2 — First data program](../books/rioux/chapters/02-first-data-program.md)
- [Ch 4 — Tabular data and CSV ingestion](../books/rioux/chapters/04-tabular-data.md)
- [Ch 5 — Joining and Grouping](../books/rioux/chapters/05-joining-grouping.md)
- [Ch 6 — JSON and complex types](../books/rioux/chapters/06-json-data.md)
