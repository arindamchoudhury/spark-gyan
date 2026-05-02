# Chapter 3 — Submitting and Scaling Your First PySpark Program

> *Source: Rioux (2022), Chapter 3, pages 45–61.*
>
> Completes the word-frequency program started in Chapter 2: adds grouping/counting (step 4) and ordering (step 5), then shows how to write results to disk, package the program for batch submission via `spark-submit`, refactor it for readability using method chaining and the `F` import convention, and finally scale it to multiple files with a single glob pattern change.
>
> 📌 **Notes adapted to PySpark 4.1.1.** The book mentions `spark-submit` can submit SparkR programs — SparkR is **deprecated in Spark 4.x** and should not be used for new projects. All other content in this chapter (groupBy, orderBy, write, coalesce, spark-submit for Python, glob patterns) is unchanged in Spark 4.1.1.

---

## 1. Grouping records — `groupBy()` and `count()`

With one word per row, counting frequencies is a two-step operation:

1. **Group** all rows that share the same word into a `GroupedData` object.
2. **Aggregate** — call `.count()` on `GroupedData` to get one row per word with its frequency.

```python
# Step-by-step
groups  = words_nonull.groupBy(col("word"))   # GroupedData — not a DataFrame yet
results = groups.count()                       # returns DataFrame[word: string, count: bigint]

# Chained (equivalent)
results = words_nonull.groupBy(col("word")).count()

results.show(5)
# +-------------+-----+
# |         word|count|
# +-------------+-----+
# |       online|    4|
# |         some|  203|
# |        still|   72|
# ...
```

### Key mechanics

- **`groupBy()` returns `GroupedData`**, an intermediate object that only becomes a DataFrame again once an aggregation method is applied (`.count()`, `.sum()`, `.agg()`, …).
- The order of rows in the result is **not guaranteed**. Spark distributes grouping work across executors; no executor knows the global sort order. Explicitly order with `orderBy()` when order matters.
- To group by multiple columns: `groupBy("col_a", "col_b")` or `groupBy(col("col_a"), col("col_b"))`.

> 💡 **Naming** — `groupby` (all lowercase) is an alias for `groupBy` (camelCase). Both work. `orderBy` has no lowercase alias — you must use the camelCase spelling. PySpark's method-naming is inconsistent (Scala heritage for camelCase, Python heritage for snake_case); just accept it and keep a cheatsheet handy.

---

## 2. Ordering results — `orderBy()`

`orderBy()` (or its alias `sort()`) reorders the DataFrame by one or more columns.

Two equivalent syntaxes for descending order:

```python
# Syntax A: column name as string + ascending parameter
results.orderBy("count", ascending=False).show(10)

# Syntax B: col() + .desc() method
results.orderBy(col("count").desc()).show(10)

# Top 10 most frequent words in Pride and Prejudice
# +----+-----+
# |word|count|
# +----+-----+
# | the| 4480|
# |  to| 4218|
# |  of| 3711|
# | and| 3504|
# | her| 2199|
# |   a| 1982|
# |  in| 1909|
# | was| 1838|
# |   i| 1749|
# | she| 1668|
# +----+-----+
```

- Multi-column ordering: `orderBy("col_a", col("col_b").desc())` — applies left-to-right, using later columns to break ties.
- An `orderBy()` applied before a `groupBy()` is silently discarded — grouping destroys the pre-existing sort. Order *after* aggregation.

> 💭 (mine): "the", "to", "of" dominating makes sense — these are stop words. Any real NLP pipeline would filter them. The book notes this and points to NLP libraries for stop-word removal, but deliberately keeps the example simple.

---

## 3. Writing data — `DataFrameWriter`

Mirror of `spark.read`, accessed via `df.write`:

```python
results.write.csv("./data/simple_count.csv")
```

### The partition-per-file reality

PySpark writes **one file per partition**. On a local machine with default settings this can produce 200 part files in a directory — not one file:

```
./data/simple_count.csv/
  _SUCCESS
  part-00000-…-c000.csv
  part-00001-…-c000.csv
  … (up to part-00199)
```

- `_SUCCESS` is a zero-byte sentinel file that confirms the write completed successfully.
- The directory-of-files design is intentional for distributed environments: each worker writes its own partition concurrently, with no bottleneck.

### Collapsing to a single file — `coalesce()`

```python
results.coalesce(1).write.csv("./data/simple_count_single.csv")
# Produces one CSV inside the directory
```

- `coalesce(n)` reduces the number of partitions to `n` **without a full shuffle** (moves data only to fill empty partitions). Efficient for reducing partition count.
- `repartition(n)` does a full shuffle — use when you need to *increase* partition count or balance uneven partitions.

> ⚠️ **Pitfall** — Ordering is not preserved through writes and subsequent reads unless you `orderBy()` immediately before the action *and* write to a single partition. On a large dataset, `coalesce(1)` forces all data to one executor — fine for dev/small data, never for production-scale output.

### Write modes

```python
results.write.mode("overwrite").csv("./data/output.csv")
results.write.mode("append").csv("./data/output.csv")
results.write.mode("ignore").csv("./data/output.csv")   # skip if exists
results.write.mode("error").csv("./data/output.csv")    # default — fails if exists
```

---

## 4. Putting it all together

### The `F` import convention

When using many functions from `pyspark.sql.functions`, the idiomatic import is:

```python
import pyspark.sql.functions as F
```

Then prefix every function call with `F.`:

```python
F.split(F.col("value"), " ")
F.explode(F.col("line"))
F.lower(F.col("word"))
F.regexp_extract(F.col("word"), "[a-z']*", 0)
```

**Why `F`?**
- The community has informally standardised on `F` — unfamiliar readers know immediately where a function comes from.
- Many function names (`sum`, `min`, `max`, `round`, `abs`) clash with Python built-ins. The `F.` prefix avoids silent shadowing.
- Avoids the `from pyspark.sql.functions import *` anti-pattern, which makes code unreadable and can mask bugs.

> ⚠️ **Anti-pattern** — `from pyspark.sql.functions import *` — never do this in a real script. It pollutes the namespace and makes it impossible to tell what's PySpark vs. Python.

### Method chaining

Every transformation returns a new DataFrame (or `GroupedData`). Chain them directly instead of storing intermediate variables:

```python
# Before (six intermediate variables — good for REPL exploration)
book        = spark.read.text("./data/gutenberg_books/1342-0.txt")
lines       = book.select(split(book.value, " ").alias("line"))
words       = lines.select(explode(col("line")).alias("word"))
words_lower = words.select(lower(col("word")).alias("word"))
words_clean = words_lower.select(regexp_extract(col("word"), "[a-z']*", 0).alias("word"))
words_nonull = words_clean.where(col("word") != "")
results     = words_nonull.groupby("word").count()

# After (one variable — clean production form)
import pyspark.sql.functions as F

results = (
    spark.read.text("./data/gutenberg_books/1342-0.txt")
    .select(F.split(F.col("value"), " ").alias("line"))
    .select(F.explode(F.col("line")).alias("word"))
    .select(F.lower(F.col("word")).alias("word"))
    .select(F.regexp_extract(F.col("word"), "[a-z']*", 0).alias("word"))
    .where(F.col("word") != "")
    .groupby("word")
    .count()
)
```

**Wrapping in parentheses** lets Python allow line breaks between chained method calls without needing backslash continuations (`\`).

**When to keep intermediate variables:**
- During REPL exploration — you need to call `show()` mid-chain.
- When you need to reuse a DataFrame in multiple branches (two separate `write()` calls, or feeding two different `.groupBy()` operations).
- When a step is logically distinct enough to deserve a name for readability.

> 💡 **Tip** — The [Black](https://black.readthedocs.io/) Python formatter automatically handles indentation in long chains. Recommended for all PySpark projects.

### The complete program

```python
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Analyzing the vocabulary of Pride and Prejudice")
    .getOrCreate()
)

results = (
    spark.read.text("./data/gutenberg_books/1342-0.txt")
    .select(F.split(F.col("value"), " ").alias("line"))
    .select(F.explode(F.col("line")).alias("word"))
    .select(F.lower(F.col("word")).alias("word"))
    .select(F.regexp_extract(F.col("word"), "[a-z']*", 0).alias("word"))
    .where(F.col("word") != "")
    .groupby("word")
    .count()
)

results.orderBy("count", ascending=False).show(10)
results.coalesce(1).write.mode("overwrite").csv("./simple_count.csv")
```

Two actions trigger two separate computation passes over the full chain. Cache `results` with `.cache()` to avoid the double pass (only worthwhile if the chain is expensive — see Ch 11).

---

## 5. Batch mode with `spark-submit`

The `pyspark` shell is for interactive development. For production scripts, use `spark-submit`:

```bash
spark-submit ./code/Ch03/word_count_submit.py
```

- Works for Python (PySpark), Scala, Java, and SQL programs.
- SparkR is supported by `spark-submit` but is **deprecated in Spark 4.x**; avoid for new projects.
- The script must create its own `SparkSession` (as shown in section 4 above) — the shell's auto-created `spark` variable isn't available.
- Log verbosity defaults to `INFO` in batch mode. Add `spark.sparkContext.setLogLevel("WARN")` early in the script to quiet it.

```bash
# Useful spark-submit options
spark-submit \
  --master local[4] \          # 4 local threads; use yarn / k8s for a real cluster
  --executor-memory 4g \
  --driver-memory 2g \
  ./code/Ch03/word_count_submit.py
```

> 📌 **Spark Connect (4.x)** — When using Spark Connect (the default client-server mode in Spark 4.x), `spark-submit` still works for local and cluster modes. The Python process now communicates with the driver over gRPC, but this is transparent from the script's perspective.

---

## 6. What this chapter deliberately skipped

The author flags three things intentionally left out:

| Topic | Where it's covered |
| --- | --- |
| Partition management and data distribution | Ch 11 (Spark UI, query planning) |
| Detailed SparkSession configuration (memory, cores, connectors) | Ch 11 (resources), Ch 9 (external connectors) |
| Manually ordering transformations for performance | Ch 11 (Catalyst optimizer) |

The key insight: Spark's Catalyst optimizer lets you **write for readability** and handles most physical planning automatically. Only reach for manual tuning when profiling reveals a bottleneck.

---

## 7. Scaling to multiple files — glob patterns

The only change needed to process an entire directory of text files:

```python
# Single file
spark.read.text("./data/gutenberg_books/1342-0.txt")

# All .txt files in the directory
spark.read.text("./data/gutenberg_books/*.txt")

# Entire directory (all files, any extension)
spark.read.text("./data/gutenberg_books/")
```

- The `*` glob matches any filename — Spark collects all matching files into a **single unified DataFrame**.
- The same code, zero other changes, runs on 6 files or 6,000.
- For a real big-data run: provision a cloud cluster (Appendix B), upload your files to S3 / GCS / ADLS, change the path to the cloud URI, and re-submit.

Top 10 across 6 Gutenberg books:

```
| the| 38895|
| and| 23919|
|  of| 21199|
|  to| 20526|
|   a| 14464|
|   i| 13973|
```

---

## 8. Summary

- **`groupBy("col").count()`** — the standard pattern for frequency counting. `groupBy()` returns `GroupedData`; `.count()` (or any aggregation) returns a DataFrame.
- **`orderBy("col", ascending=False)`** or **`orderBy(col("col").desc())`** — sort a DataFrame; use *after* aggregation.
- **`df.write.csv(path)`** writes one file per partition into a directory. Use **`coalesce(1)`** to get a single output file for small results.
- **`import pyspark.sql.functions as F`** is the idiomatic import — use `F.col()`, `F.split()`, etc. throughout.
- **Method chaining** inside parentheses eliminates intermediate variables and makes the transformation pipeline read as a sequence of steps.
- **`spark-submit script.py`** runs a Python PySpark script in batch mode. The script must create its own `SparkSession`.
- **Glob patterns** (`*.txt`, directory path) in `spark.read.text()` scale a program to multiple files with a single character change.

---

## 9. References

- PySpark DataFrameWriter — <https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.html>
- `spark-submit` documentation — <https://spark.apache.org/docs/latest/submitting-applications.html>
- Black Python formatter — <https://black.readthedocs.io/>
- Book source code (Ch 3) — <http://mng.bz/6ZOR>
