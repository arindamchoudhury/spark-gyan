# Chapter 6 — Multidimensional Data Frames: Using PySpark with JSON Data

> *Source: Rioux (2022), Chapter 6, pages 117–150.*
>
> Pushes the data frame model beyond flat rows/columns into hierarchical data. The chapter ingests a TV-show JSON document from TVMaze, introduces the three PySpark complex column types (array, map, struct), builds schemas programmatically, and shows how to expand and contract complex columns with explode/collect.
>
> 📌 **Notes adapted to PySpark 4.1.1.** Core complex-type API (array, map, struct, StructType, explode, collect_list) is unchanged from 3.x. Key Spark 4.x additions:
>
> - **`parse_json()` (4.0)** — parses a JSON string column into a semi-structured VARIANT column without a pre-declared schema. Modern alternative to `from_json()` when schema is unknown at write time.
> - **VARIANT type GA (4.1)** — designed for schema-less JSON-like data; accessed with the `:` colon-sign operator (e.g., `variant_col:field_name`).
> - **`StructType.toDDL()` and `DataType.fromDDL()` (4.0+)** — convert between StructType objects and DDL strings programmatically.
> - **`MapType` now supported in `GROUP BY` (4.0+)** — previously raised an error.

---

## 1. JSON and Python data structures

### 1.1 JSON as a limited Python dictionary

JSON maps cleanly onto Python types:

| JSON | Python equivalent |
| --- | --- |
| Object (`{...}`) | `dict` |
| Array (`[...]`) | `list` |
| String (`"..."`) | `str` |
| Number | `int` or `float` |
| Boolean (`true` / `false`) | `True` / `False` |
| `null` | `None` |

Key constraints: JSON keys must always be strings; Python dict keys don't have that restriction. JSON booleans are lower-case (`true`/`false`), unlike Python's capitalised forms.

The root of every practical JSON document is an object (a curly-bracket `{}`), giving it the same top-level structure as a Python dict.

### 1.2 Reading JSON in PySpark

```python
shows = spark.read.json("./data/shows/shows-silicon-valley.json")
```

- **Default rule: one JSON document = one line = one record.** This is the JSON Lines format. The entire document must be on a single line with no internal newlines.
- No need for `sep`, `header`, or `inferSchema` — JSON self-describes types through its syntax (unquoted number vs. quoted string).

**JSON Lines (JSONL / NDJSON)** — plain text where each line is one complete, valid JSON document. Three names; same format:

| Name | Extension | Spec |
| --- | --- | --- |
| JSON Lines | `.jsonl` | jsonlines.org |
| Newline-Delimited JSON | `.ndjson` | ndjson.org |
| JSON (Spark default) | `.json` | — |

Rules: no commas between records, no surrounding `[...]` wrapper, each line parseable in isolation.

Why Spark defaults to this: a file of newline-separated records can be **split across workers** — each executor reads a chunk of lines independently without touching the rest of the file. With `multiLine=True`, one file is one record and cannot be split (parallelism comes from having many files, not from within a file).

**Writing JSON Lines:**

```python
df.write.json("./output/shows/")
# writes one .json file per partition; each file is in JSON Lines format
```

- **Multiple documents in separate files** — use `multiLine=True` (note capital L):

```python
three_shows = spark.read.json("./data/shows/shows-*.json", multiLine=True)
# rule becomes: one JSON document = one file = one record
```

**`multiLine` only matters when files are pretty-printed.** If each file contains exactly one minified JSON document on a single line, both `multiLine=True` and `multiLine=False` produce the same record count. The difference appears with pretty-printed files:

| File format | `multiLine=False` | `multiLine=True` |
| --- | --- | --- |
| Minified — 1 line per file | ✅ 1 record per file | ✅ 1 record per file |
| Pretty-printed — N lines per file | ❌ parse errors / nulls per line | ✅ 1 record per file |

With a pretty-printed file, `multiLine=False` tries to parse each line as a standalone JSON document:

```
# shows-silicon-valley.json — pretty-printed (4 lines)
{
  "name": "Silicon Valley",
  "type": "Scripted"
}

# multiLine=False: each line is attempted as a self-contained JSON document
# line 1  {                           → invalid (incomplete object)  → _corrupt_record
# line 2    "name": "Silicon Valley", → invalid (key:value ≠ document, trailing comma) → _corrupt_record
# line 3    "type": "Scripted"        → invalid (key:value ≠ document) → _corrupt_record
# line 4  }                           → invalid (no opening brace) → _corrupt_record
#
# result: df.count() → 4 rows, name/type columns all null, _corrupt_record populated
```

Rule of thumb: API responses and programmatically generated files are usually minified (default works); human-edited or `jq`-formatted files need `multiLine=True`.

> ⚠️ **Spark 4.x WARN — `FileStreamSink` glob false alarm**: passing a glob directly into `.json()` triggers a spurious warning on Spark 4.x:
> ```
> WARN FileStreamSink - Assume no metadata directory.
> Error while looking for metadata directory in the path: .../shows-*.json
> java.io.FileNotFoundException: File .../shows-*.json does not exist
> ```
> Spark's streaming layer tries to stat the literal glob string as a path (it doesn't exist as a file), logs a WARN, then continues normally. The read succeeds. To eliminate the warning, pass a directory and use `pathGlobFilter` instead:
>
> ```python
> # Preferred — no WARN, same result
> three_shows = (
>     spark.read
>     .option("pathGlobFilter", "*.json")
>     .json("./data/shows/", multiLine=True)
> )
> ```
>
> `pathGlobFilter` matches on filename only (not subdirectory names), so `"*.json"` works but `"subdir/*.json"` does not. For files spread across nested subdirectories, add `recursiveFileLookup`:
>
> ```python
> three_shows = (
>     spark.read
>     .option("pathGlobFilter", "*.json")
>     .option("recursiveFileLookup", "true")
>     .json("./data/shows/", multiLine=True)
> )
> ```
>
> Alternatively, silence the logger in `log4j2.xml` (useful when you can't change the read call):
> ```xml
> <Logger name="org.apache.spark.sql.execution.streaming.sinks.FileStreamSink"
>         level="ERROR" additivity="false">
>     <AppenderRef ref="console"/>
> </Logger>
> ```

> ⚠️ **Pitfall — mixed-type arrays**: PySpark won't raise an error if an array contains mixed types. It silently infers the lowest common denominator (usually `string`). You get a wrong type later, not an error at read time.

---

## 2. Complex column types: array and map

PySpark calls these *complex* (also *container* or *compound*) types — types that hold other types. The three container types map to Python's collections:

| PySpark type | Python analogue | Key constraint |
| --- | --- | --- |
| `ArrayType(elementType)` | `list` | All elements same type |
| `MapType(keyType, valueType)` | `dict` | Keys same type; values same type; keys cannot be null |
| `StructType([StructField(...)])` | named tuple | Fixed named fields; each field can differ in type |

### 2.1 Array

Arrays are zero-indexed. Access elements with `[index]` or `.getItem(index)`. Slicing is **not** supported — `genres[0:10]` raises `AnalysisException`.

```python
# All four forms are equivalent; prefer F.col() (Palantir style)
F.col("genres")[0]
F.col("genres").getItem(0)
array_subset.genres[0]        # dot-accessor — avoid (see note below)
array_subset.genres.getItem(0)
```

> ⚠️ **Palantir style — avoid dot-accessor (`df.colName`)**: the book uses `array_subset.genres[0]`. Palantir discourages this form because it breaks for column names that contain spaces or clash with DataFrame method names. Prefer `F.col("genres")[0]`.

Key array functions (all in `pyspark.sql.functions`, most prefixed `array_`):

```python
F.array("col1", "col2", "col3")        # create from columns
F.array_repeat(col, n)                 # repeat n times
F.size(col)                            # element count — returns -1 for null, not 0
F.array_distinct(col)                  # deduplicate
F.array_intersect(col1, col2)          # intersection
F.array_position(col, value)           # position of value (1-based)
```

> ⚠️ **Indexing inconsistency (confirmed PySpark 4.1.1)**: three schemes coexist in the same API:
>
> - `col[0]` / `getItem(0)` / `F.get(col, 0)` — **0-based**
> - `F.element_at(col, 1)` — **1-based**
> - `F.array_position(col, value)` — **1-based** (returns `0` if not found); accepts a `Column` as `value` since 4.0

**Creating array literals from Python lists:**

The book (Spark 3.2) says `F.lit()` won't accept a Python list and requires the long route:

```python
# Spark 3.2 workaround — still valid
F.array(F.lit(1), F.lit(2), F.lit(3))
```

> 📌 **PySpark 3.4+ — `F.lit()` accepts Python lists directly**, returning an `ArrayType` column equivalent to `F.array(F.lit(v1), F.lit(v2), ...)`. Both work in `select()` and produce the same result.

**Three forms compared — different output types:**

```python
# Form 1 — one F.lit() per scalar (all versions)
# Result: ArrayType(StringType)  →  array('Comedy', 'Horror', 'Drama')
F.array(F.lit("Comedy"), F.lit("Horror"), F.lit("Drama"))

# Form 2 — F.lit() with a list (PySpark 3.4+)
# Result: ArrayType(StringType)  →  array('Comedy', 'Horror', 'Drama')
F.lit(["Comedy", "Horror", "Drama"])

# Form 3 — F.array() wrapping F.lit() of a list  ← TRAP
# Result: ArrayType(ArrayType(StringType))  →  array(array('Comedy', 'Horror', 'Drama'))
F.array(F.lit(["Comedy", "Horror", "Drama"]))   # nested array — probably not what you want
```

Form 1 and Form 2 produce identical results. Form 3 looks similar but wraps the array in another array — use only when a nested array is intentional.

Both forms produce identical output — use whichever reads more clearly:

```python
# Form 1 — explicit F.lit() per scalar (all versions)
array_subset_repeated = array_subset.select(
    "name",
    F.array(F.lit("Comedy"), F.lit("Horror"), F.lit("Drama")).alias("some_genres"),
    F.array_repeat("dot_and_index", 5).alias("repeated_genres")
)

# Form 2 — F.lit() with a list (PySpark 3.4+), same result
array_subset_repeated = array_subset.select(
    "name",
    F.lit(["Comedy", "Horror", "Drama"]).alias("some_genres"),
    F.array_repeat("dot_and_index", 5).alias("repeated_genres")
)
```

> ⚠️ **ONS style — `F.size()` returns `-1` for null**: `F.size()` on a null array column returns `-1`, not `0` or `null`. A comparison like `F.size("col") > 0` silently passes null rows (because `-1 > 0` is false, not an error, but the intent is wrong). Since the return is `-1` and not `null`, `F.coalesce()` won't help. Prefer `F.array_size()` (Spark 3.5+) which returns `null` for null input and is safe with standard null-handling patterns, or guard explicitly: `F.when(F.col("col").isNull(), 0).otherwise(F.size("col"))`.

### 2.2 Map

A map is a typed dictionary: keys and values both have a declared type. Keys cannot be null; values can (controlled by `valueContainsNull`).

Reading JSON does **not** produce map columns — JSON objects become structs. Maps are most useful for programmatic key-value construction.

**Building a map from column names → column values** (common pattern):

```python
columns = ["name", "language", "type"]

# Book version (Spark 3.2) — 3 selects
shows_map = shows.select(
    *[F.lit(column) for column in columns],
    F.array(*columns).alias("values")
).select(
    F.array(*columns).alias("keys"),
    "values"
).select(
    F.map_from_arrays("keys", "values").alias("mapped")
)

# Improved — 2 selects, build both arrays directly
shows_map = shows.select(
    F.array(*[F.lit(c) for c in columns]).alias("keys"),
    F.array(*columns).alias("values")
).select(
    F.map_from_arrays("keys", "values").alias("mapped")
)

# create_map — 1 select, alternating key/value args
shows_map = shows.select(
    F.create_map(*[x for c in columns for x in (F.lit(c), F.col(c))]).alias("mapped")
)
```

`create_map` takes interleaved `(key, value, key, value, ...)` columns — more concise but harder to read. The 2-select version is preferred for clarity. No functional difference in PySpark 4.1.1.

```python
# Access a value by key — bracket notation only (dot notation is for structs, not maps)
F.col("mapped")["key_name"]          # bracket notation
F.col("mapped").getItem("key_name")  # equivalent
```

Other useful map functions: `map_values()`, `map_keys()`, `map_entries()`.

> 📌 **Spark 4.0+**: `MapType` is now supported in `GROUP BY`. Previously this raised an error.

---

## 3. The struct: a data frame within a column

- The struct maps to a JSON object: named fields, each can be a different type, schema is fixed and known ahead of time.
- Best mental model: *a mini data frame trapped within the column* — it has named fields just like columns.
### 3.1 Accessing struct fields

Four ways to read a field out of a struct column:

| Form | Syntax | Notes |
|---|---|---|
| **Dot notation** ✅ default | `F.col("outer.field")` | Most idiomatic; mirrors SQL nested syntax; used in Spark docs and style guides |
| **`getField()`** | `F.col("outer").getField("field")` | Escape hatch when the field name contains a dot, space, or other special character |
| **Bracket notation** | `F.col("outer")["field"]` | Calls `getItem` internally; reserve for arrays (int index) and maps — reads as map/array access to a reader |
| **Wildcard `.*`** | `F.col("outer.*")` | Special purpose: expands **all** sub-fields into separate columns in `select()` |

```python
# All three access the same "time" field inside the "schedule" struct
F.col("schedule.time")
F.col("schedule").getField("time")
F.col("schedule")["time"]

# Drill through nested structs (chained dots work for all three non-wildcard forms)
F.col("_embedded.episodes")                         # dot notation
F.col("_embedded").getField("episodes")             # getField
F.col("_embedded")["episodes"]                      # bracket

# Expand all sub-fields of "schedule" into individual columns
shows.select(F.col("schedule.*"))  # → one column per StructField in schedule
```

**When to prefer each:**

- **Dot notation** — always, unless one of the exceptions below applies.
- **`getField()`** — field name contains a dot, space, or special character (e.g. a field literally named `"my.field"`).
- **Bracket notation** — field name is in a variable at runtime (`F.col("s")[var]`), or you're already writing array/map access and want visual consistency. Avoid using it for struct fields when dot notation would work — it misleads readers.
- **`.*` wildcard** — only in `select()` to flatten one level of a struct. Cannot be chained (but `"outer.inner.*"` works for a doubly-nested struct).

> ⚠️ **Dot vs `getField` for ambiguous names**: if a column is named `"a.b"` (the dot is part of the name, not a path separator), `F.col("a.b")` will raise `AnalysisException` — it tries to resolve `b` as a sub-field of `a`. Use backticks to escape: `` F.col("`a.b`") `` or use `getField` after resolving the top-level column.

Drilling into an `Array[Struct]` field returns an array of that field's values across all elements — useful for extracting all episode names without an explicit explode:

```python
F.col("episodes.name")  # Array[Struct] → Array[string]  (all names, no explode)
```

### 3.2 Promoting struct fields to top-level columns

Unwrap a single-field wrapper struct with `withColumn` + dot notation:

```python
shows_clean = shows.withColumn(
    "episodes", F.col("_embedded.episodes")
).drop("_embedded")
```

Unpack all sub-fields of a struct at once: `F.col("struct_col.*")`.

---

## 4. Building schemas programmatically

### 4.1 PySpark type objects

Types live in `pyspark.sql.types` (imported as `T`):

- **Scalar**: `T.StringType()`, `T.LongType()`, `T.IntegerType()`, `T.DoubleType()`, `T.BooleanType()`, `T.DateType()`, `T.TimestampType()`.
- **`T.DecimalType(precision, scale)`** — takes required precision/scale parameters. (`T.VarcharType(length)` and `T.CharType(length)` also take a length parameter but are rarely used directly in schema definitions.)
- **Complex**: `T.ArrayType(T.StringType())`, `T.MapType(T.StringType(), T.LongType())`.
- **`T.StructField(name, dataType, nullable=True)`** — a single named field.
- **`T.StructType([StructField(...), ...])`** — a schema or a struct column type.

Build bottom-up — define leaf types first, compose upward:

```python
import pyspark.sql.types as T

episode_links_schema = T.StructType([
    T.StructField("self", T.StructType([T.StructField("href", T.StringType())]))
])

episode_schema = T.StructType([
    T.StructField("_links", episode_links_schema),
    T.StructField("airdate", T.DateType()),
    T.StructField("airstamp", T.TimestampType()),
    T.StructField("name", T.StringType()),
    T.StructField("number", T.LongType()),
    T.StructField("season", T.LongType()),
])
```

> 💡 **Tip (book)**: split schemas with more than ~3 fields into their own named variables. The code becomes self-documenting without comments and avoids one giant nested block.

> 💡 **ONS style — DDL for simple schemas**: for flat schemas, DDL strings are terser and easier to read:
> ```python
> schema_ddl = "`incident_number` string, `cal_year` int, `fin_year` string"
> df = spark.read.csv(path, schema=schema_ddl)
> ```
> Use `StructType`/`StructField` when you need programmatic construction, `ArrayType`, `MapType`, or deeply nested structs. Spark 4.0+ adds `DataType.fromDDL(ddl_string)` and `my_struct_type.toDDL()` to convert between the two representations freely.

> 💡 **Partial schema**: passing a `StructType` that covers only a subset of fields makes PySpark read **only those fields** — a cheap way to avoid reading a wide document when you need just a few columns.

### 4.2 Reading with a strict schema

```python
shows_with_schema = spark.read.json(
    "./data/shows/shows-silicon-valley.json",
    schema=embedded_schema,
    mode="FAILFAST",
)
```

- JSON has no native date/timestamp types. A `DateType()` or `TimestampType()` field in the schema tells the reader to parse ISO-8601 strings into those types automatically.
- `mode="FAILFAST"` — crash on any malformed record.
- `mode="PERMISSIVE"` (default) — set malformed records to null silently.

> ⚠️ **Prefer `FAILFAST` in production**: `PERMISSIVE` mode can silently turn bad records into null rows, which then propagate wrong results downstream. The earlier you surface a schema mismatch, the cheaper it is to fix. `FAILFAST` errors identify the type mismatch but not which field — narrow down by elimination.

### 4.3 JSON-formatted schemas

Round-trip a schema through JSON for versioning and sharing:

```python
import json

schema_str = df.schema.json()                          # → JSON string
schema_dict = df.schema.jsonValue()                    # → Python dict

restored = T.StructType.fromJson(json.loads(schema_str))
assert restored == df.schema  # True
```

`StructField` JSON shape: `{"name": "...", "type": "...", "nullable": true, "metadata": {}}`. Complex types have additional fields: arrays add `containsNull` + `elementType`; maps add `keyType`, `valueType`, `valueContainsNull`.

> 💡 **Spark 4.0+ — `toDDL()` shortcut**: `df.schema.toDDL()` produces a DDL string which is more human-readable than the JSON representation and directly usable in SQL DDL statements.

---

## 5. Hierarchical model and expanding/contracting complex columns

### 5.1 Why hierarchical beats flat for nested data

Two ways to represent a show-with-episodes in a flat (2D) model:

1. **Normalised** — separate `shows` and `episodes` tables linked by `show_id`. No duplication, but requires joins.
2. **Denormalised** — one wide table. Duplicates `show_id` and `genre` for every episode row; loses the clarity of which record is the "unit."

The hierarchical model (episodes as `Array[Struct]` inside the show row) gives the best of both: one record per show, no duplication, no joins needed.

### 5.2 Explode and collect

**Explode** — expand an array/map column into one row per element:

```python
episodes = shows.select(
    "id", F.explode("_embedded.episodes").alias("episodes")
)
# shows → 1 row; episodes → 53 rows (one per episode)
```

- `F.explode(col)` — **silently drops rows where the array/map is null**. A show with no episodes disappears from the output entirely.
- `F.explode_outer(col)` — same but **retains null rows**, producing `null` for the exploded column.

> ⚠️ **ONS style — prefer `explode_outer()`**: unless you explicitly intend to discard records with null arrays, use `explode_outer()`. Silent row loss from `explode()` is one of the hardest data bugs to detect — it only shows up as unexpectedly low row counts, not an error.

- `F.posexplode(col)` — explodes and prepends a 0-based **position** column. Returns **two columns** (`pos`, `col`).
- `F.posexplode_outer(col)` — same but retains null rows.

> ⚠️ **ONS style — `posexplode` requires `.select()`, not `.withColumn()`**: since the function produces two columns simultaneously, `.withColumn()` cannot wrap it. Use:
> ```python
> df.select("*", F.posexplode("array_col").alias("pos", "value"))
> # adjust 0-based pos to 1-based ordinal if needed:
> .withColumn("pos", F.col("pos") + 1)
> ```

**Collect** — aggregate rows back into an array column:

```python
collected = episodes.groupby("id").agg(
    F.collect_list("episodes").alias("episodes")
)
```

- `F.collect_list(col)` — one array element per row; preserves duplicates; **order is not guaranteed**.
- `F.collect_set(col)` — one array element per distinct value; deduplicated; **order is not guaranteed**.

> ❓ Revisit: `F.sort_array(F.collect_list(...))` or window functions for ordered collection.

### 5.3 Struct as a function

Create a struct column from existing columns using `F.struct()`:

```python
struct_ex = shows.select(
    F.struct(
        F.col("status"),
        F.col("weight"),
        F.lit(True).alias("has_watched"),
    ).alias("info")
)
# struct_ex schema: info: struct<status: string, weight: long, has_watched: boolean>
```

Unpack all struct fields: `F.col("info.*")`.

---

## 6. Summary

- JSON → PySpark: object = dict = struct; array = list = `ArrayType`; PySpark adds `MapType` for typed key-value data.
- Three complex types: **array** (ordered, same-type elements), **map** (typed key-value, no null keys), **struct** (named fixed-schema fields — a data frame within a column).
- Schema: build `StructType` bottom-up or use DDL strings for flat schemas. Pass a partial schema to read only needed columns and skip the inferSchema pre-scan.
- `FAILFAST` mode surfaces schema mismatches at ingestion — prefer it in production over silent `PERMISSIVE` nulling.
- `explode()` silently drops null-array rows — use `explode_outer()` to retain them.
- `posexplode()` returns two columns; must use `.select()` not `.withColumn()`.
- `collect_list()` / `collect_set()` are the inverse of explode; neither guarantees order.
- Hierarchical model (arrays of structs) avoids the duplication of the denormalised tabular model without needing joins.

---

## 7. References

- JSON Lines format: https://jsonlines.org/
- JSON RFC 8259: https://datatracker.ietf.org/doc/html/rfc8259
- PySpark collection functions: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions/collection.html
- PySpark types module: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/types.html
- Spark 4.0 release notes (VARIANT, `parse_json`, MapType GROUP BY): https://spark.apache.org/releases/spark-release-4-0-0.html
- Spark 4.1 release notes (VARIANT GA): https://spark.apache.org/releases/spark-release-4-1-0.html
