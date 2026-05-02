# Chapter 2 — Your First Data Program in PySpark

> *Source: Rioux (2022), Chapter 2, pages 17–44.*
>
> Builds a word-frequency program on *Pride and Prejudice* to introduce the core PySpark development loop: launch a REPL, ingest data into a DataFrame, apply a chain of column transformations, filter rows, and print results. The chapter deliberately stops before counting and scaling — those land in Chapter 3 — so the focus stays on transformation mechanics.
>
> 📌 **Notes adapted to PySpark 4.1.1.** The book shows Python 3.8 and Spark 3.2.0. PySpark 4.1.1 requires **Python ≥ 3.10** and **Java 17**. Exception classes moved from `pyspark.sql.utils` to `pyspark.errors` in Spark 4.x — update any `from pyspark.sql.utils import AnalysisException` to `from pyspark.errors import AnalysisException`. The DataFrame API (select, filter, split, explode, lower, regexp_extract) is unchanged.

---

## 1. The three meta-steps of every PySpark program

Almost every data-driven program — from a quick summary to a full ML pipeline — follows this skeleton:

1. **Read** — ingest data from a source into a structure.
2. **Transform** — apply operations to reshape, filter, enrich the data.
3. **Export / sink** — write results to a file, database, or screen.

This chapter walks steps 1 and 2 for a concrete problem: *which words appear most often in Pride and Prejudice?*

---

## 2. Setting up the PySpark shell

### Launching the shell

```bash
pyspark
```

This drops you into an IPython (or plain Python) REPL with two variables pre-configured:

- `spark` — a `SparkSession`; main entry point for DataFrame operations.
- `sc` — a `SparkContext`; lower-level entry point; rarely needed directly.

> 💡 **Tip** — IPython (`pip install ipython`) is strongly recommended over the plain Python shell: friendlier paste, tab completion, syntax highlighting, and the `?` / `??` doc shortcuts.

### Creating SparkSession from scratch (for scripts and IDEs)

When writing a `.py` file or using a Python IDE, skip the `pyspark` launcher and create the session manually. This makes it explicit that PySpark is just a Python library.

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Analyzing the vocabulary of Pride and Prejudice")
    .getOrCreate()
)
```

Key points:
- **Builder pattern** — `SparkSession.builder` chains config methods before calling `.getOrCreate()`.
- **`getOrCreate()`** — returns an existing session if one already exists; safe for both interactive and batch use.
- **`appName`** — shows up in the Spark UI (Ch 11); pick something meaningful.
- `SparkSession` wraps and supersedes the older `SparkContext` / `SQLContext` combo. Access the underlying context via `spark.sparkContext` if you need it.

> ⚠️ **Legacy code warning** — Older tutorials use `sc` and `sqlContext` as separate entry points. In current PySpark: `sc = spark.sparkContext` and `sqlContext = spark` are the equivalents. Avoid creating them directly in new code.

### Configuring the log level

The default log level is `WARN` in the shell and `INFO` in batch mode. `INFO` is very noisy. Change it with:

```python
spark.sparkContext.setLogLevel("WARN")   # or ERROR, OFF, DEBUG, TRACE, ALL
```

| Level | What you see |
| --- | --- |
| `OFF` | Nothing |
| `FATAL` | Fatal crashes only |
| `ERROR` | Recoverable errors too |
| `WARN` | Warnings — good default for learning |
| `INFO` | Runtime info (repartitioning, data recovery) |
| `DEBUG` | Debug messages |
| `TRACE` | Very verbose debug |
| `ALL` | Everything |

> 💡 **Tip** — Anything chattier than `WARN` in the shell will interleave log lines with your typing. `WARN` is the sweet spot for interactive development.

### (Optional) Eager evaluation in the REPL

By default, entering a DataFrame variable prints only its schema, not its data — because data evaluation is lazy. If you want a pandas-style "show me the data" experience during exploration:

```python
spark = (
    SparkSession.builder
    .config("spark.sql.repl.eagerEval.enabled", "True")
    .getOrCreate()
)
```

> ⚠️ **Pitfall** — Eager mode triggers full computation on every assignment. Great for demos, expensive for large data. Leave it off in production pipelines.

---

## 3. Mapping the program

Before writing code, sketch the steps. For this problem:

| Step | Description | PySpark operation |
| --- | --- | --- |
| 1. Read | Load the text file | `spark.read.text()` |
| 2. Token | Split each line into a list of words | `split()` + `select()` |
| 3. Clean | Lowercase and remove punctuation; one word per row | `lower()`, `regexp_extract()`, `explode()`, `filter()` |
| 4. Count | Frequency per word | (Chapter 3) |
| 5. Answer | Top N words | (Chapter 3) |

> 💭 (mine): The "map before coding" habit pays double dividends in PySpark — you spot which steps are transformations (cheap to chain) vs. actions (expensive to trigger accidentally) before you write a line.

---

## 4. Ingesting data

### The two core data structures

| Structure | Mental model | When to use |
| --- | --- | --- |
| **RDD** | A distributed bag of arbitrary Python objects | Low-level control; record-by-record Python logic (Ch 8) |
| **DataFrame** | A typed table of columns | Almost everything — fast, optimizable, SQL-compatible |

The DataFrame is the dominant structure in modern PySpark. The module for it is named `pyspark.sql` — it takes heavy inspiration from SQL.

### `spark.read` — the DataFrameReader

`spark.read` gives you a `DataFrameReader` object with format-specific methods:

```python
spark.read.text(path)     # plain text — one line = one row
spark.read.csv(path)      # CSV
spark.read.json(path)     # JSON
spark.read.parquet(path)  # Parquet (Spark's default storage format)
spark.read.orc(path)      # ORC
```

> 📌 **Version note** — Parquet is Spark's default storage format (read and write). ORC is an Apache competitor; both are columnar, compressed, and optimized for big data. Full comparison in Ch 6.

Loading *Pride and Prejudice*:

```python
book = spark.read.text("./data/gutenberg_books/1342-0.txt")
# book → DataFrame[value: string]
```

The result is a DataFrame with one column (`value`) of type `string`. Each row is one line of the file.

### Exploring a DataFrame's structure

```python
# See the schema (column names + types) — printed to REPL on variable inspection
book

# Tree view of schema — most useful in complex DataFrames
book.printSchema()
# root
#  |-- value: string (nullable = true)

# Schema as a list of (name, type) tuples
book.dtypes
# [('value', 'string')]
```

### Peeking at the data — `show()`

`show()` is an **action** — it triggers computation and prints rows to the screen.

```python
book.show()                          # 20 rows, truncated at 20 chars
book.show(10, truncate=50)           # 10 rows, truncated at 50 chars
book.show(5, truncate=False)         # 5 rows, full length
book.show(5, truncate=False, vertical=True)  # each record as a mini-table
```

| Parameter | Default | What it does |
| --- | --- | --- |
| `n` | 20 | Number of rows to display |
| `truncate` | `True` (20 chars) | `False` = full; any int = chars limit |
| `vertical` | `False` | Display each record as a key-value mini-table |

> 💡 **Tip** — `printSchema()` + `show()` together are your primary exploration tools. Use them constantly when building a new pipeline.

---

## 5. Column transformations

### The `select()` method

`select()` returns a new DataFrame containing only the specified columns (or column expressions). It's the PySpark equivalent of SQL `SELECT`.

Four equivalent ways to select the `value` column:

```python
from pyspark.sql.functions import col

book.select(book.value)        # dot notation — fails on column names with spaces
book.select(book["value"])     # bracket notation — handles any column name
book.select(col("value"))      # col() — most flexible; doesn't bind to a specific df
book.select("value")           # string shorthand — fine for plain selects
```

> 💡 **Prefer `col()`** — it's the most portable form and works cleanly in complex pipelines where you don't always have a direct reference to the source DataFrame.

### Splitting strings — `split()`

`split()` takes a string column and a Java regex delimiter, and returns an array column:

```python
from pyspark.sql.functions import col, split

lines = book.select(split(col("value"), " ").alias("line"))
lines.printSchema()
# root
#  |-- line: array (nullable = true)
#  |    |-- element: string (containsNull = true)
```

Each row now contains an array of words. The original `"hello world"` → `["hello", "world"]`.

> 💡 **Note** — PySpark uses **Java regular expressions** in built-in functions like `split()` and `regexp_extract()`, not Python's `re` module syntax. They're very similar but not identical.

### Renaming columns — `alias()` vs `withColumnRenamed()`

When a transformation creates a column, PySpark auto-generates a name like `split(value, , -1)`. Always rename:

```python
# alias() — chains directly on the column expression inside select()
lines = book.select(split(col("value"), " ").alias("line"))

# withColumnRenamed() — renames an existing column on the whole DataFrame
# Useful when you don't want to rewrite the select(); no-op if column doesn't exist
lines = book.select(split(col("value"), " "))
lines = lines.withColumnRenamed("split(value,  , -1)", "line")
```

**Rule of thumb:** use `alias()` when you're already inside a `select()` or column expression; use `withColumnRenamed()` when renaming without changing the rest of the DataFrame.

### Exploding arrays into rows — `explode()`

After splitting, each row holds a list of words. `explode()` unrolls that list — one element per row:

```python
from pyspark.sql.functions import explode, col

words = lines.select(explode(col("line")).alias("word"))
words.show(5)
# +--------+
# |    word|
# +--------+
# |     The|
# | Project|
# |Gutenberg|
# ...
```

Visually: `["This", "is", "a", "list"]` → four rows, each holding one string.

### Lowercasing — `lower()`

```python
from pyspark.sql.functions import lower

words_lower = words.select(lower(col("word")).alias("word_lower"))
```

`"Prejudice,"` → `"prejudice,"`

### Removing punctuation — `regexp_extract()`

Keep only the first contiguous run of lowercase letters:

```python
from pyspark.sql.functions import regexp_extract

words_clean = words_lower.select(
    regexp_extract(col("word_lower"), "[a-z]+", 0).alias("word")
)
```

- Pattern `[a-z]+` matches one or more lowercase ASCII letters.
- The `0` argument extracts group 0 (the whole match).
- `"prejudice,"` → `"prejudice"`. An empty string `""` is left for rows that contained only punctuation.

> 💡 **Regex resource** — [regexr.com](https://regexr.com/) is excellent for testing Java/JavaScript-compatible regexes interactively.

---

## 6. Filtering rows

`filter()` and `where()` are identical — PySpark exposes both to reduce friction for users coming from different backgrounds. Either keeps only rows where the condition is `True`.

```python
words_nonull = words_clean.filter(col("word") != "")

# Equivalently:
words_nonull = words_clean.where(col("word") != "")
```

Useful operators inside filter/where:

```python
col("x") != ""          # not equal
col("x") > 3            # greater than
col("x").isin(["a", "b"])   # membership
~(col("x") == "")       # negation with ~ operator
```

> 💡 **Tip** — Don't stress about filtering "too late" in the chain. Because Spark is lazy, it can push filter predicates earlier in the physical plan automatically. Write filters where they're most *readable*, and let the optimizer handle placement.

---

## 7. The full Chapter 2 pipeline (steps 1–3)

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, explode, lower, regexp_extract

spark = SparkSession.builder.appName("word_count").getOrCreate()

book = spark.read.text("./data/gutenberg_books/1342-0.txt")

words_nonull = (
    book
    .select(split(col("value"), " ").alias("line"))          # tokenise
    .select(explode(col("line")).alias("word"))               # one word per row
    .select(lower(col("word")).alias("word_lower"))           # lowercase
    .select(regexp_extract(col("word_lower"), "[a-z]+", 0)   # strip punctuation
            .alias("word"))
    .filter(col("word") != "")                               # drop empties
)
```

Nothing executes until an action (like `show()`) is called. Chapter 3 adds `groupBy().count()` and `orderBy()` to complete steps 4 and 5.

---

## 8. Summary

- Every PySpark program follows **Read → Transform → Export**.
- The **`pyspark` shell** gives a pre-configured REPL with `spark` (SparkSession) and `sc` (SparkContext) ready to use. For scripts, create `SparkSession` with the builder pattern.
- **DataFrames** are the primary data structure — typed, columnar, SQL-inspired.
- **`spark.read`** (DataFrameReader) ingests files: `.text()`, `.csv()`, `.json()`, `.parquet()`.
- **`printSchema()` + `show()`** are the exploration workhorses.
- **`select()`** returns a new DataFrame with chosen columns or column expressions. Prefer `col("name")` from `pyspark.sql.functions` for portability.
- **`pyspark.sql.functions`** is the library of built-in column functions: `split()`, `explode()`, `lower()`, `regexp_extract()`, and hundreds more. These map to JVM implementations and run at full Spark speed.
- **`alias()`** renames a column inside a select; **`withColumnRenamed()`** renames on the whole DataFrame.
- **`filter()` / `where()`** are identical — keep rows where the Boolean column expression is `True`.
- The full transformation chain is lazy: no data moves until an **action** (`show()`, `write()`, `count()`) is called.

---

## 9. References

- PySpark SQL functions API — <https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html>
- Book source code (Ch 2) — <http://mng.bz/6ZOR>
- RegExr (Java-compatible regex tester) — <https://regexr.com/>
