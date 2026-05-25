# Chapter 9 — Big Data Is Just a Lot of Small Data: Using pandas UDFs

> *Source: Rioux (2022), Chapter 9, pages 192–214.*
>
> Where Chapter 8 introduced Python UDFs (record-by-record), this chapter introduces **pandas UDFs** — UDFs that operate on batches of rows serialised as pandas Series or DataFrames. The result is faster execution (vectorised operations), tight integration with the Python ML ecosystem (scikit-learn, scipy), and a clean split-apply-combine story via `GroupedData`.
>
> 📌 **Notes adapted to Spark 4.1.1 / PySpark 4.1.1.** The book targets Spark 3.2. Key changes in Spark 4.x:
>
> - **PyArrow** is required and used for all pandas/Spark data transfer. No `ARROW_PRE_0_15` workarounds needed — those were for Spark 2.x and are long obsolete.
> - **pandas ≥ 2.2.0** required (Spark 4.1). Book examples use pandas 1.x style; `.iteritems()` and `.append()` were deprecated in pandas 1.5 and **removed** in pandas 2.0 — nothing that affects UDF internals, but any book code that calls them will error.
> - **`spark.sql.execution.pandas.convertToArrowArraySafely` is on by default** (Spark 4.1): Arrow raises errors for unsafe casts (int overflow, float truncation) rather than silently coercing.
> - **Spark 4.1 adds Arrow-native UDFs** that bypass pandas conversion entirely — not covered by the book; see the references section.
> - **Iterator API now supported in GROUPED_MAP** (Spark 4.1) — book shows `applyInPandas` with a plain function; the new iterator variant allows memory-efficient streaming over large groups.
> - **Keyword arguments** in SCALAR and GROUPED_AGG pandas UDFs added in Spark 4.0.

---

## 1. The core idea

- PySpark distributes a DataFrame across partitions. pandas UDFs let you operate on **each partition (or batch within a partition) as a local pandas object** — a Series or a DataFrame — rather than record by record.
- Serialisation between Spark and pandas is handled by **PyArrow** (zero-copy columnar format). This is why pandas UDFs are significantly faster than regular Python UDFs: one Arrow batch per UDF call vs. one Python call per row.
- Two main families, five total UDF types:

| Family | UDF type | Signature |
|---|---|---|
| **Scalar** — Spark decides batches | Series to Series | `pd.Series` → `pd.Series` |
| | Iterator of Series to Iterator of Series | `Iterator[pd.Series]` → `Iterator[pd.Series]` |
| | Iterator of multiple Series to Iterator of Series | `Iterator[Tuple[pd.Series, ...]]` → `Iterator[pd.Series]` |
| **Grouped data** — you control batches via `groupby()` | Group aggregate (Series to Scalar) | `pd.Series` → scalar |
| | Group map | `pd.DataFrame` → `pd.DataFrame` |

### Why doesn't Spark provide its own UDF language?

It does — **built-in functions** (`pyspark.sql.functions`) *are* Spark's native way. They are written in Scala, run entirely inside the JVM, and are fully visible to the Catalyst optimiser. The design intent is: reach for built-ins first; use a UDF only when no built-in covers your case.

UDFs exist as an escape hatch, and their cost comes down to a fundamental constraint: **Python is a separate process from the JVM**. There is no way to run CPython code inside the JVM without crossing a process boundary.

| | Runs in | Catalyst-aware? | Boundary crossing |
|---|---|---|---|
| Built-in functions (`F.sum`, `F.col`, …) | JVM (Scala) | ✅ fully | None |
| Scala / Java UDF | JVM | Partial | None |
| Python UDF (`@F.udf`) | Python process | ❌ black box | Per row — serialise every record via Py4J |
| pandas UDF (`@F.pandas_udf`) | Python process | ❌ black box | Per Arrow batch — far cheaper |
| Arrow-native UDF (Spark 4.1+) | Python process | ❌ black box | Per Arrow batch, no pandas conversion |

Spark's answer has been to make the crossing as cheap as possible: pandas UDFs batch via Arrow instead of pickling per row; Arrow-native UDFs (Spark 4.1) remove the pandas conversion layer on top of that.

Two hierarchies apply depending on what you're optimising for:

**By performance** (fastest → slowest):

built-in function → Arrow-native UDF → pandas UDF → Python UDF

**By practical reach** (what to actually reach for):

built-in function → pandas UDF → Arrow-native UDF → Python UDF

The practical order differs because pandas UDFs have been around since Spark 2.3 and the entire scientific Python ecosystem (scikit-learn, scipy, statsmodels) speaks `pd.Series` — whenever you need those libraries, pandas UDF is the right tool regardless of the overhead. Arrow-native UDFs (Spark 4.1+) are the better choice only when you need raw speed *and* `pyarrow.compute` covers your logic without needing pandas or its ecosystem.

---

## 2. Setup and dependencies

```python
# Install
pip install pandas scikit-learn pyarrow

# Recommended imports
import pandas as pd
from typing import Iterator, Tuple
import pyspark.sql.functions as F
import pyspark.sql.types as T
```

> 💡 **Tip** — The book's Spark 2.x workarounds (`PandasUDFType.SCALAR`, the IPC format env var) are dead. On Spark 3.0+ you use Python type hints in the function signature; Spark infers the UDF kind from the types.

### Batch size

When Spark executes a Series UDF, it doesn't hand the entire partition to pandas at once. It splits the partition's columns into **Arrow record batches** and calls the UDF function once per batch, then concatenates the results. Smaller batches lower peak JVM and Python memory per call; larger batches reduce per-call overhead and give pandas more data to vectorise over in a single pass.

- Default: **10,000 records per Arrow batch** (`spark.sql.execution.arrow.maxRecordsPerBatch`).
- Reduce on memory-constrained executors; if wide (many columns), reduce further — column count multiplies the memory cost per batch.
- Raising it marginally helps throughput but risks OOM on large or wide partitions.
- `maxRecordsPerBatch` does **not** apply to grouped data UDFs — each group arrives whole; it is the user's responsibility to ensure a group fits in memory.

### Data used in this chapter

The book connects to Google BigQuery's NOAA GSOD weather dataset (40 M+ rows across 2010–2020). If you don't have a GCP account, the same data is available as Parquet from the book's repo:

```python
from functools import reduce
import pyspark.sql.functions as F

gsod = (
    reduce(
        lambda x, y: x.unionByName(y, allowMissingColumns=True),
        [
            spark.read.parquet(f"./data/gsod_noaa/gsod{year}.parquet")
            for year in range(2010, 2021)
        ],
    )
    .dropna(subset=["year", "mo", "da", "temp"])
    .where(F.col("temp") != 9999.9)
    .drop("date")
)
```

> 💡 **Tip** — On a local Spark instance, limit to a single year (e.g., 2018) to keep execution fast.

The cleaned `gsod` DataFrame has 31 columns. Key ones used in this chapter:

| Column | Type | Notes |
|---|---|---|
| `stn` | string | Station identifier |
| `year`, `mo`, `da` | string | Year, month, day |
| `temp` | double | Mean temperature in °F; `9999.9` = missing (filtered out above) |
| `dewp` | double | Dew point in °F; `9999.9` = missing |
| `slp`, `stp` | double | Sea-level / station pressure; `9999.9` = missing |
| `visib` | double | Visibility in miles; `999.9` = missing |
| `wdsp`, `mxpsd`, `gust` | double | Wind speed / max / gust; `999.9` = missing |
| `max`, `min` | double | Max/min temperature; `flag_max`/`flag_min` = NULL when derived |
| `prcp` | double | Precipitation; `99.99` = missing |
| `sndp` | double | Snow depth; `999.9` = missing |
| `fog`, `rain_drizzle`, `snow_ice_pellets`, `hail`, `thunder`, `tornado_funnel_cloud` | int | Binary weather indicators (0/1) |

> 💡 **Sentinel pattern** — missing values are encoded as out-of-range numbers (`9999.9`, `999.9`, `99.99`), not `null`. The `.where(F.col("temp") != 9999.9)` filter in the ingestion code is handling exactly this — always check for sentinels before aggregating.

> ⚠️ **Expected warning with this dataset** — loading the 31-column GSOD schema triggers:
> ```
> WARN SparkStringUtils - Truncated the string representation of a plan since it was too large.
> This behavior can be adjusted by setting 'spark.sql.debug.maxToStringFields'.
> ```
> Spark truncates plan strings at **25 fields** by default; GSOD has 31. No data is lost — it's cosmetic. Fix by raising the limit on the session:
> ```python
> spark.conf.set("spark.sql.debug.maxToStringFields", 50)
> ```
> Or set it at `SparkSession` creation time via `.config("spark.sql.debug.maxToStringFields", 50)`.

### Libraries that "play well with pandas"

The book names **scikit-learn** explicitly. The broader category is any library whose functions accept and return `pd.Series` or `np.ndarray` — most of the scientific Python stack qualifies:

| Library | Fits because… | Typical pandas UDF use |
|---|---|---|
| **NumPy** | Series wraps a NumPy array; arithmetic ops are automatic | Vectorised math not in Spark built-ins (trig, log, clip) |
| **scikit-learn** | Models fit/predict on arrays; no per-row conversion needed | Distributed inference; cold-start load with Iterator UDF |
| **SciPy** | `scipy.stats` functions accept Series directly | CDFs, distribution fitting, signal processing per group |
| **statsmodels** | Fits statistical models on DataFrames | OLS, ARIMA per entity with group map UDF |
| **Prophet** | Takes a DataFrame with `ds`/`y` columns | Per-entity time-series forecasting with group map UDF |

The key criterion: if `library_function(my_series)` works in a plain Python session, it works inside a pandas UDF body with zero extra conversion. Libraries that only speak raw Python scalars belong in regular Python UDFs instead.

---

## 3. Series UDFs (Scalar UDFs)

Three variants, all decorated with `@F.pandas_udf(return_type)`.

### 3.1 Series → Series (most common)

Takes one or more `pd.Series` objects, returns one `pd.Series`. This is the pandas equivalent of a standard column function.

```python
@F.pandas_udf(T.DoubleType())
def f_to_c(degrees: pd.Series) -> pd.Series:
    return (degrees - 32) * 5 / 9

gsod = gsod.withColumn("temp_c", f_to_c(F.col("temp")))
gsod.select("temp", "temp_c").distinct().show(5)
# +-----+-------------------+
# | temp|             temp_c|
# +-----+-------------------+
# | 37.2| 2.8888888888888906|
# | 85.9| 29.944444444444443|
# +-----+-------------------+
```

- **Why faster than Python UDF?** One vectorised pandas operation per batch vs. one Python call per row.
- Accepts multiple Series: `def my_udf(a: pd.Series, b: pd.Series) -> pd.Series`.

> ⚠️ **Pitfall** — Spark **does not guarantee batch composition or ordering**. Don't write logic that assumes records from the same group land in the same batch — use a grouped data UDF for that.

> ⚠️ **Type checker warning** — `@F.pandas_udf(T.DoubleType())` will produce a Pylance/pyright error:
> ```
> No overloads for "pandas_udf" match the provided arguments
> Untyped function decorator obscures type of function
> ```
> This is a **known PySpark stub bug** ([SPARK-43189](https://issues.apache.org/jira/browse/SPARK-43189), filed April 2023, still open). The stubs were written for the old Spark 2.x API that required an explicit `functionType` argument (`PandasUDFType.SCALAR`). The modern Spark 3.0+ API infers the UDF type from Python type hints and needs only the return type — but no overload for that pattern exists in the stubs. The PySpark team's own codebase works around this with `# type: ignore[call-overload]`. Workarounds:
> ```python
> @F.pandas_udf(T.DoubleType())  # type: ignore[arg-type]
> def f_to_c(degrees: pd.Series) -> pd.Series: ...
> ```
> Or suppress session-wide in `pyrightconfig.json`:
> ```json
> { "reportUntypedFunctionDecorator": "none", "reportCallIssue": "none" }
> ```

#### Working with complex types

PySpark has a richer type system than pandas, which collapses strings and complex types into a catchall `object` dtype. When you drop from PySpark into pandas inside a UDF, **you are solely responsible for aligning types** — this is why specifying the return type in `@F.pandas_udf(return_type)` is recommended, not optional: it surfaces type mismatches early.

| PySpark type | What to use in the UDF |
|---|---|
| Scalar (`IntegerType`, `DoubleType`, …) | Plain `pd.Series` |
| `ArrayType` | `pd.Series` whose values are Python lists — Spark promotes them back to `ArrayType` |
| `StructType` | `pd.DataFrame` — struct columns are mini DataFrames, the equivalence holds inside UDFs too |

### 3.2 Iterator of Series → Iterator of Series (cold start)

Same as Series → Series, but you receive an iterator of batches. The key win: an **expensive initialisation** (loading an ML model from disk, opening a DB connection, loading a large lookup table) happens **once per partition**, not once per batch.

```python
from typing import Iterator

@F.pandas_udf(T.DoubleType())
def f_to_c_iter(degrees: Iterator[pd.Series]) -> Iterator[pd.Series]:
    # cold-start work happens here, outside the loop — runs once per task
    for batch in degrees:
        yield (batch - 32) * 5 / 9
```

Usage is identical to Series → Series — call it inside `withColumn()` or `select()`.

### 3.3 Iterator of multiple Series → Iterator of Series

Same iterator pattern, but accepts multiple columns packed as a `Tuple` of Series.

```python
from typing import Tuple

@F.pandas_udf(T.DateType())
def create_date(
    year_mo_da: Iterator[Tuple[pd.Series, pd.Series, pd.Series]]
) -> Iterator[pd.Series]:
    for year, mo, da in year_mo_da:
        yield pd.to_datetime(
            pd.DataFrame(dict(year=year, month=mo, day=da))
        )

gsod.select(
    "year", "mo", "da",
    create_date(F.col("year"), F.col("mo"), F.col("da")).alias("date"),
).distinct().show(5)
```

---

## 4. UDFs on grouped data (split-apply-combine)

The **split-apply-combine** pattern is a standard data analysis term coined by Hadley Wickham in a [2011 Journal of Statistical Software paper](https://www.jstatsoft.org/v40/i01/), introduced alongside the R `plyr` package. Pandas `groupby()`, dplyr, and Spark's `applyInPandas()` all implement the same concept.

1. **Split** — `groupby()` divides the DataFrame into batches keyed by column values.
2. **Apply** — a function runs on each batch independently (as a local pandas object).
3. **Combine** — results are unioned back into a Spark DataFrame.

> ⚠️ **Memory warning** — each batch must fit in executor memory. If one group is enormous, you get OOM. Spark 4.1 added an iterator API to `applyInPandas` as a safety valve for skewed data. **This is opt-in** — Spark detects which form you're using from the function's type hints:
>
> ```python
> # Normal: whole group loaded into memory at once
> def my_func(df: pd.DataFrame) -> pd.DataFrame:
>     return df.transform(...)
>
> # Iterator: Spark feeds the group as batches — avoids OOM on large groups
> def my_func(batches: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
>     for batch in batches:
>         yield batch.transform(...)
> ```
>
> Same `applyInPandas(my_func, schema="...")` call either way — the signature determines the behaviour.

> ⚠️ **PySpark 4.1.0+ noise (SPARK-54344)** — `BrokenPipeError: [Errno 32] Broken pipe` appears in notebook output when running pandas UDFs. **Queries actually succeed** — the error is cosmetic. The cause: SPARK-54344 changed worker recycling so idle workers flush output after the JVM has already closed the socket. Worker stderr goes unconditionally to JVM `System.err` with no config gate, so no Spark config flag silences it.
>
> **Workaround** — redirect fd 2 before `SparkSession` initialisation:
> ```python
> import os
> _errfd = os.open("/tmp/spark-stderr.log", os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
> os.dup2(_errfd, 2)
> os.close(_errfd)
> # now create SparkSession
> ```
> See [deep-dive article](https://medium.com/@arindam_62474/a-deep-dive-into-what-causes-it-why-every-config-knob-fails-and-how-to-actually-silence-it-17755d23e1ad) for full analysis.

### 4.1 Group aggregate UDF (Series → scalar)

A custom `agg()` function. Takes Series inputs, returns a single scalar per group. The "apply" stage collapses each group to one row. This is a **reduction**, not a transform — N values in, one value out:

```
# Series → Series (transform): same length
[37.2, 85.9, 42.1, 91.0]  →  [2.9, 30.0, 5.6, 32.8]

# Group aggregate (reduction): many → one
[37.2, 85.9, 42.1, 91.0]  →  64.05
```

Contrast with `groupby().transform()` (keeps all rows) vs `groupby().agg()` (one row per group) in plain pandas — same distinction.

```python
from sklearn.linear_model import LinearRegression

@F.pandas_udf(T.DoubleType())
def rate_of_change_temperature(day: pd.Series, temp: pd.Series) -> float:
    """Linear slope of temperature over the period."""
    return (
        LinearRegression()
        .fit(X=day.astype(int).values.reshape(-1, 1), y=temp)
        .coef_[0]
    )

result = gsod.groupby("stn", "year", "mo").agg(
    rate_of_change_temperature(gsod["da"], gsod["temp"]).alias("rt_chg_temp")
)
result.show(5, False)
# +------+----+---+---------------------+
# |stn   |year|mo |rt_chg_temp          |
# +------+----+---+---------------------+
# |010250|2018|12 |-0.01014397905759162 |
# +------+----+---+---------------------+
```

- Used inside `.agg()`, exactly like `F.sum()` or `F.count()`.
- Return type must be a Python scalar or a pandas scalar.

### 4.2 Group map UDF (DataFrame → DataFrame)

Receives an entire group as a `pd.DataFrame`, returns a `pd.DataFrame`. The returned schema must match the one declared in `applyInPandas()`. The output row count doesn't have to match the input — your function can return fewer rows (filtering), the same rows (transforming), or more rows (interpolating).

The key contrast with group aggregate:

```
# Group aggregate: N rows → 1 scalar (group collapses)
[37.2, 42.1, 39.8]  →  0.24

# Group map: N rows → M rows (group survives, transformed)
[37.2, 42.1, 39.8]  →  [-2.9, 1.9, -0.4]   (e.g. demeaned)
```

Fewer rows (filtering outliers) and more rows (interpolating missing dates) are both valid — Spark unions whatever DataFrames come back from each group.

Inside the function, `temp_by_day` is a plain **pandas DataFrame** — not a Spark `GroupedData` object. Spark shuffles the data so all rows for a given `(stn, year, mo)` land on the same partition, converts them to a pandas DataFrame, then calls your function. By the time Python sees it, the grouping is done and you're writing ordinary local pandas logic. The DataFrame includes all columns from the original Spark DataFrame, including the groupby key columns ([official docs](https://spark.apache.org/docs/latest/api/python/tutorial/sql/arrow_pandas.html): *"The input data contains all the rows and columns for each group."*).

```python
def scale_temperature(temp_by_day: pd.DataFrame) -> pd.DataFrame:
    """Min-max normalise temperature within each station-month batch."""
    temp = temp_by_day.temp
    answer = temp_by_day[["stn", "year", "mo", "da", "temp"]]
    if temp.min() == temp.max():
        return answer.assign(temp_norm=0.5)
    return answer.assign(
        temp_norm=(temp - temp.min()) / (temp.max() - temp.min())
    )

gsod_map = gsod.groupby("stn", "year", "mo").applyInPandas(
    scale_temperature,
    schema="stn string, year string, mo string, da string, temp double, temp_norm double",
)
gsod_map.show(5, False)
```

- No `@F.pandas_udf` decorator needed for group map (Spark 3.0+).
- Schema can use DDL string (as above) or `StructType`.
- **All columns** you want in the result must be explicitly returned from your function.
### 4.3 Map (`mapInPandas()`) *(not in book)*

Applies a function to the whole DataFrame as an iterator of batches — no grouping key. Each batch arrives as a `pd.DataFrame`; yield a `pd.DataFrame` back. Row count can change (filter, expand), just like group map.

```python
from typing import Iterable

def filter_func(iterator: Iterable[pd.DataFrame]) -> Iterable[pd.DataFrame]:
    for pdf in iterator:
        yield pdf[pdf.id == 1]

df.mapInPandas(filter_func, schema=df.schema).show()
```

Key differences from `applyInPandas`:

| | `mapInPandas` | `applyInPandas` |
|---|---|---|
| Grouping | None — operates on Spark partitions as-is | Shuffles by key first |
| Input | `Iterator[pd.DataFrame]` | `pd.DataFrame` (one group) |
| Output | `Iterator[pd.DataFrame]` | `pd.DataFrame` |
| Use case | Batch transforms, filtering, enrichment without needing group boundaries | Logic that depends on all rows in a group being together |

### 4.4 Co-grouped map UDF (`cogroup().applyInPandas()`) *(not in book)*

Joins two DataFrames by a common key and applies a pandas function to each co-group. Each call receives **two** `pd.DataFrame` arguments — one per source DataFrame — and returns one `pd.DataFrame`.

```python
df1 = spark.createDataFrame(
    [(20000101, 1, 1.0), (20000101, 2, 2.0), (20000102, 1, 3.0), (20000102, 2, 4.0)],
    ("time", "id", "v1"))

df2 = spark.createDataFrame(
    [(20000101, 1, "x"), (20000101, 2, "y")],
    ("time", "id", "v2"))

def merge_ordered(left: pd.DataFrame, right: pd.DataFrame) -> pd.DataFrame:
    return pd.merge_ordered(left, right)

df1.groupby("id").cogroup(df2.groupby("id")).applyInPandas(
    merge_ordered,
    schema="time int, id int, v1 double, v2 string",
).show()
# +--------+---+---+----+
# |    time| id| v1|  v2|
# +--------+---+---+----+
# |20000101|  1|1.0|   x|
# |20000102|  1|3.0|null|   ← row only in df1; v2 = null
# |20000101|  2|2.0|   y|
# |20000102|  2|4.0|null|
# +--------+---+---+----+
```

Use case: time-series alignment, ordered merge, feature joining where group-level pandas logic is needed rather than a SQL join.

> ⚠️ **Memory warning** — `maxRecordsPerBatch` is **not applied** to co-groups. All data for both sides of a co-group loads into memory at once. No iterator API escape hatch here — size your groups carefully.

---

## 5. Decision tree: which UDF to use?

```
Do I need to control batch composition (split-apply-combine)?
├── Yes → grouped data UDF
│   ├── Returns a scalar → Group aggregate UDF  [groupby().agg()]
│   └── Returns a DataFrame → Group map UDF  [groupby().applyInPandas()]
└── No
    ├── Need pandas on the full DataFrame → mapInPandas()
    └── Only transforming some columns
        ├── Cold start (load model, compile regex)?
        │   ├── One input column → Iterator of Series UDF
        │   └── Multiple columns → Iterator of multiple Series UDF
        └── No cold start → Series to Series UDF
```

---

## 6. Testing pandas UDFs locally

The book (listing 9.12) uses the `.func` attribute of a decorated UDF to call the underlying pandas function directly, without a SparkSession:

```python
# Pull one full group into local pandas
gsod_local = gsod.where(
    "year = '2018' and mo = '08' and stn = '710920'"
).toPandas()

# Call the raw pandas function via .func (implementation detail — not in official docs)
print(rate_of_change_temperature.func(gsod_local["da"], gsod_local["temp"]))
```

> ❓ Revisit: `.func` is not listed as a public API in the PySpark 4.1 docs — it is an implementation detail exposed by the `UserDefinedFunction` wrapper. The officially recommended approach is to define the function separately from the decorator, then the function is directly callable for local testing without needing `.func`.

- For grouped UDFs, filter to **one full group** so the local data matches what Spark would pass.
- Build and validate locally first, then promote to a UDF.

> 💡 **Tip** — pandas 2.1 renamed `DataFrame.applymap()` to `DataFrame.map()` for element-wise DataFrame operations. `Series.apply()` was not deprecated — it still works. If you see a `FutureWarning` mentioning `applymap`, switch to `df.map(...)`.

---

## 7. Spark 4.x: Arrow-native UDFs (not in book)

Spark 4.1 adds Arrow-native UDF support that bypasses pandas entirely and works directly with `pyarrow.Array` objects. Two ways to get it:

- **`@arrow_udf` decorator** — a dedicated decorator; takes `pa.Array` in, returns `pa.Array` out.
- **`useArrow=True` on `@F.udf`** — opts an existing UDF into Arrow mode. Can also be enabled session-wide: `spark.conf.set("spark.sql.execution.pythonUDF.arrow.enabled", True)`. This config defaults to `true` in Spark 4.2+; in Spark 4.1 it must be set explicitly.

```python
import pyarrow as pa
import pyarrow.compute as pc

# Dedicated Arrow UDF (Spark 4.1+)
# Note: exact import path of arrow_udf not yet verified — check the 4.1 API docs
@arrow_udf("double")
def f_to_c_arrow(degrees: pa.Array) -> pa.Array:
    return pc.divide(pc.subtract(degrees, 32.0), 1.8)
```

> ❓ Revisit: verify the import path for `arrow_udf` in PySpark 4.1 (likely `from pyspark.sql.functions import arrow_udf` or `F.arrow_udf`). Benchmark against Series-to-Series UDFs — the theoretical win (no pandas roundtrip) is clear but the practical gap may be small for simple transformations.

---

## 8. Summary

- pandas UDFs bridge Spark's distributed world and the Python/pandas ecosystem using **PyArrow** for efficient serialisation.
- **Series → Series** is the go-to for column transformations. Prefer it over Python UDFs whenever pandas vectorisation is available.
- **Iterator variants** amortise expensive cold starts (model loading, regex compilation) over many batches.
- **Group aggregate** and **group map** UDFs implement split-apply-combine: `groupby()` controls the batch; the UDF runs pandas logic on each group.
- `mapInPandas()` offers DataFrame-in/DataFrame-out flexibility without a group key.
- Spark 4.1 raises the floor: pandas ≥ 2.2.0, safe Arrow casts by default, iterator API in grouped map, Arrow-native UDFs.
- Test with `.func` locally; filter to a real group's worth of data for grouped UDFs.

---

## 9. References

- [PySpark 4.1.1 — `pandas_udf` API reference](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.pandas_udf.html)
- [PySpark 4.1.1 — UDFs & UDTFs user guide](https://spark.apache.org/docs/latest/api/python/user_guide/udfandudtf.html)
- [PySpark 4.1.1 — `applyInPandas` API reference](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.GroupedData.applyInPandas.html)
- [ONS Spark guide — pandas UDFs](https://best-practice-and-impact.github.io/ons-spark/ancillary-topics/pandas-udfs.html)
- [Spark 4.1.0 release notes](https://spark.apache.org/releases/spark-release-4.1.0.html)
