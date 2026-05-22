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
- Two main families:

| Family | Controls batch composition? | Input / output |
|---|---|---|
| **Scalar (Series) UDFs** | No — Spark decides batches | Series → Series, or Iterator[Series] → Iterator[Series] |
| **Grouped data UDFs** | Yes — you choose `groupby()` keys | Series → scalar (aggregate) or DataFrame → DataFrame (map) |

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

- Default: **10,000 records per Arrow batch** (`spark.sql.execution.arrow.maxRecordsPerBatch`).
- Reduce on memory-constrained executors; raising it marginally helps throughput but risks OOM.

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
- Complex return types: return a `pd.DataFrame` when the output schema is a `StructType`.

> ⚠️ **Pitfall** — Spark **does not guarantee batch composition or ordering**. Don't write logic that assumes records from the same group land in the same batch — use a grouped data UDF for that.

### 3.2 Iterator of Series → Iterator of Series (cold start)

Same as Series → Series, but you receive an iterator of batches. The key win: an **expensive initialisation** (loading an ML model, compiling a regex) happens **once per partition**, not once per batch.

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

The **split-apply-combine** pattern:

1. **Split** — `groupby()` divides the DataFrame into batches keyed by column values.
2. **Apply** — a function runs on each batch independently (as a local pandas object).
3. **Combine** — results are unioned back into a Spark DataFrame.

> ⚠️ **Memory warning** — each batch must fit in executor memory. If one group is enormous, you get OOM. Spark 4.1 added an iterator API to `applyInPandas` as a safety valve for skewed data.

### 4.1 Group aggregate UDF (Series → scalar)

A custom `agg()` function. Takes Series inputs, returns a single scalar per group. The "apply" stage collapses each group to one row.

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

Receives an entire group as a `pd.DataFrame`, returns a `pd.DataFrame`. The returned schema must match the one declared in `applyInPandas()`. Unlike aggregate UDFs, **row count can change**.

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
- For whole-DataFrame iteration without a group key, use `DataFrame.mapInPandas(fn, schema)` — the same iterator-of-DataFrames pattern but applied to the full DataFrame.

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
- **`useArrow=True` on `@F.udf`** — opts an existing UDF into Arrow mode via a config flag. Can also be enabled session-wide with `spark.sql.execution.pythonUDF.arrow.enabled`.

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
