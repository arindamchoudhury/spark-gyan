# Schema Definition: `StructType` vs DDL Strings

> *Cross-chapter synthesis — Rioux (2022), Chapters 4, 6.*
>
> Spark supports two ways to define a schema programmatically: the bottom-up `StructType` / `StructField` API and compact DDL strings. Ch 4 introduces explicit schemas for CSV; Ch 6 goes deep on the full API — partial schemas, JSON round-trips, and FAILFAST/PERMISSIVE modes.

---

## Ch 4 — Why explicit schemas matter

`inferSchema=True` works for exploration but has two costs in production:

1. **An extra full scan** — Spark reads the entire file once to infer types. For large files this is an unnecessary job.
2. **Inferred types can be wrong** — a column with values `"01"`, `"02"` is inferred as `StringType`; a column with one missing value and otherwise integers may become `DoubleType`.

Provide an explicit schema for any CSV (or JSON/Parquet) read that runs in production:

```python
import pyspark.sql.types as T

schema = T.StructType([
    T.StructField("id",         T.IntegerType(),   nullable=False),
    T.StructField("name",       T.StringType(),    nullable=True),
    T.StructField("amount",     T.DoubleType(),    nullable=True),
    T.StructField("created_at", T.TimestampType(), nullable=True),
])

df = spark.read.schema(schema).csv("data/orders.csv", header=True)
```

---

## Ch 6 — The full `StructType` API

**Bottom-up programmatic construction:**

```python
address_schema = T.StructType([
    T.StructField("street", T.StringType()),
    T.StructField("city",   T.StringType()),
    T.StructField("geo", T.StructType([
        T.StructField("lat", T.DoubleType()),
        T.StructField("lon", T.DoubleType()),
    ])),
])

user_schema = T.StructType([
    T.StructField("user_id", T.LongType(),    nullable=False),
    T.StructField("name",    T.StringType()),
    T.StructField("address", address_schema),
    T.StructField("tags",    T.ArrayType(T.StringType())),
    T.StructField("scores",  T.MapType(T.StringType(), T.IntegerType())),
])
```

**DDL string alternative** — more compact, identical semantics:

```python
schema_ddl = "user_id BIGINT NOT NULL, name STRING, address STRUCT<street:STRING, city:STRING>"
schema = T.StructType.fromDDL(schema_ddl)
```

DDL strings are useful for quick schemas and for storing schemas as configuration values.

**Converting between representations:**

```python
# StructType → DDL string
ddl = user_schema.toDDL()
print(ddl)
# user_id BIGINT NOT NULL, name STRING, address STRUCT<street:STRING,city:STRING>, ...

# StructType → JSON (for storage/serialisation)
import json
schema_json = user_schema.jsonValue()
schema_back = T.StructType.fromJson(schema_json)

# DDL string → StructType (Spark 4.0+)
schema = T.DataType.fromDDL(ddl)
```

`toDDL()` and `fromDDL()` were added in Spark 4.0. For Spark 3.x use `json()` / `fromJson()` for round-tripping.

---

## Ch 6 — Partial schemas

For JSON/CSV sources with many fields where you only need a few, define a **partial schema** containing only the columns you care about. Spark reads only those fields (column pruning):

```python
partial = T.StructType([
    T.StructField("user_id", T.LongType()),
    T.StructField("amount",  T.DoubleType()),
    # all other fields in the file are ignored
])

df = spark.read.schema(partial).json("data/events.json")
```

Partial schemas are the primary mechanism for schema-at-read column pruning with semi-structured sources.

---

## Ch 6 — FAILFAST vs PERMISSIVE mode

| Mode | Behaviour on malformed record |
|---|---|
| `PERMISSIVE` (default) | Nulls bad fields; optionally writes raw string to `_corrupt_record` |
| `DROPMALFORMED` | Silently drops malformed rows |
| `FAILFAST` | Raises `SparkRuntimeException` at first malformed record (at action time) |

```python
# FAILFAST — good for validation jobs; exceptions bubble to the caller
df = spark.read.schema(schema).option("mode", "FAILFAST").json("data/")

# PERMISSIVE with corrupt record column — useful for auditing
schema_with_corrupt = schema.add(T.StructField("_corrupt_record", T.StringType()))
df = (
    spark.read
    .schema(schema_with_corrupt)
    .option("columnNameOfCorruptRecord", "_corrupt_record")
    .json("data/")
)
bad_rows = df.filter(F.col("_corrupt_record").isNotNull())
```

---

## Ch 6 — Schema validation utilities

```python
def assert_schema(df, expected: T.StructType) -> None:
    """Fail fast if the DataFrame schema does not match expected."""
    actual_fields = {f.name: f.dataType for f in df.schema}
    expected_fields = {f.name: f.dataType for f in expected}
    missing = set(expected_fields) - set(actual_fields)
    wrong_type = {
        name: (actual_fields[name], expected_fields[name])
        for name in expected_fields
        if name in actual_fields and actual_fields[name] != expected_fields[name]
    }
    assert not missing, f"Missing columns: {missing}"
    assert not wrong_type, f"Wrong types: {wrong_type}"
```

Use at pipeline entry points to catch schema drift early rather than letting mismatches surface as downstream `AnalysisException` or silent nulls.

---

## Summary

- `inferSchema=True` costs a full extra scan and can infer wrong types — use explicit schemas in production.
- `T.StructType([T.StructField(…), …])` for programmatic construction; DDL strings for compact one-liners.
- `toDDL()` / `fromDDL()` available in Spark 4.0+; use `.json()` / `.fromJson()` for Spark 3.x round-trips.
- Partial schemas (only the needed fields) activate column pruning for JSON/CSV.
- `FAILFAST` raises `SparkRuntimeException` at the action; `PERMISSIVE` + `_corrupt_record` captures bad rows for auditing.
- Validate schemas at pipeline boundaries to catch drift before it propagates.

---

## Chapter links

- [Ch 4 — Tabular data and CSV ingestion](../books/rioux/chapters/04-tabular-data.md)
- [Ch 6 — JSON and complex types](../books/rioux/chapters/06-json-data.md)
