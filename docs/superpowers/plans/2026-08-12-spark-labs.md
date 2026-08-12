# Spark Labs Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the labs skeleton and two worked labs, wired into the learning path, so the remaining ten labs are a repeatable recipe.

**Architecture:** Notebooks live in the stack repo at `workspace/notebooks/labs/` and execute against the running Spark 4.2.0 + Delta + Unity Catalog stack over Spark Connect. A single `_labkit.py` module holds all plumbing (session, dataset fetch, plan and partition helpers) so notebooks stay about Spark. Datasets are declared in `datasets.yaml`, fetched on demand, never committed. The notes site gains a labs index and one `**Lab**` line per covered topic; a checker script keeps the two in sync.

**Tech Stack:** Python 3.10+, PySpark 4.2.0 (Spark Connect client), PyYAML, pytest, Jupyter notebooks, Delta Lake 4.3.1, Unity Catalog OSS 0.5.1, Zensical for the site.

## Global Constraints

- **Two repos.** Notebooks, `_labkit.py`, `datasets.yaml` and their tests live in `C:/opt/learn/spark/spark-delta-unitycatalog` (the "stack repo"). The labs index, `**Lab**` lines and `check_labs.py` live in `C:/opt/learn/spark/notes` (the "notes repo"). Every task states which repo it commits to. Commit directly on `main` in both.
- **Python 3.10+ syntax only.** The stack root conda env is Python 3.14, the Dagster venv is 3.10. `_labkit.py` must import on both.
- **No dataset bytes are ever committed.** `workspace/data/**` is already gitignored in the stack repo. Do not add exceptions.
- **Notebooks carry no assertions.** Expectations are markdown prose: "roughly N on this data", with the reason it may differ. This is a spec decision, not a preference.
- **Spark Connect endpoint differs by caller.** From the host, `sc://localhost:15002`; from inside a stack container, `sc://spark:15002`. `session()` resolves this; never hard-code either in a notebook.
- **Spark UI** is published on host ports 4040–4042. Notebook instructions name the tab and the port.
- Every code block in this plan is the actual content to write. Copy it.

---

### Task 1: Dataset manifest and `fetch()`

**Files:**
- Create: `C:/opt/learn/spark/spark-delta-unitycatalog/workspace/notebooks/labs/_labkit.py`
- Create: `C:/opt/learn/spark/spark-delta-unitycatalog/workspace/notebooks/labs/datasets.yaml`
- Create: `C:/opt/learn/spark/spark-delta-unitycatalog/workspace/notebooks/labs/tests/test_labkit_fetch.py`

**Interfaces:**
- Consumes: nothing.
- Produces: `load_manifest(path=None) -> dict`, `dataset_dir(name) -> Path`, `fetch(name, scale="small", manifest=None) -> Path`, and the exception class `DatasetUnavailable(Exception)` with attribute `.substitute: str | None`. Tasks 2, 3 and 6 call `fetch`.

- [ ] **Step 1: Confirm the labs directory is not gitignored**

Run from the stack repo:

```bash
mkdir -p workspace/notebooks/labs/tests
touch workspace/notebooks/labs/.keep
git check-ignore -v workspace/notebooks/labs/.keep; echo "exit=$?"
```

Expected: no output and `exit=1`, meaning the file is **not** ignored. The rule in `.gitignore` is `workspace/notebooks/*.ipynb`, which matches only direct children. If instead a rule is printed, stop and add `!workspace/notebooks/labs/` to `.gitignore` before continuing.

- [ ] **Step 2: Write the failing tests**

Create `workspace/notebooks/labs/tests/test_labkit_fetch.py`:

```python
"""Tests for the dataset side of _labkit. No Spark, no network."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from _labkit import DatasetUnavailable, dataset_dir, fetch, load_manifest  # noqa: E402

MANIFEST = {
    "demo_set": {
        "source": "https://example.invalid/demo/{slice}.parquet",
        "format": "parquet",
        "small": ["2025-01"],
        "full": ["2025-01", "2025-02"],
        "bytes_small": 1024,
        "licence": "test fixture",
        "substitute": "gsod_noaa",
    }
}


def test_load_manifest_reads_the_shipped_file():
    m = load_manifest()
    assert isinstance(m, dict) and m, "datasets.yaml must parse to a non-empty dict"
    for name, entry in m.items():
        for key in ("source", "format", "small", "licence", "substitute"):
            assert key in entry, f"{name} is missing '{key}'"


def test_fetch_returns_existing_directory_without_network(tmp_path, monkeypatch):
    monkeypatch.setattr("_labkit.DATA_ROOT", tmp_path)
    target = tmp_path / "demo_set"
    target.mkdir()
    (target / "2025-01.parquet").write_bytes(b"x" * 1024)

    def explode(*args, **kwargs):
        raise AssertionError("fetch must not download when files are present")

    monkeypatch.setattr("_labkit._download", explode)

    assert fetch("demo_set", manifest=MANIFEST) == target


def test_fetch_raises_naming_the_substitute_when_unreachable(tmp_path, monkeypatch):
    monkeypatch.setattr("_labkit.DATA_ROOT", tmp_path)

    def boom(url, dest):
        raise OSError("no route to host")

    monkeypatch.setattr("_labkit._download", boom)

    with pytest.raises(DatasetUnavailable) as excinfo:
        fetch("demo_set", manifest=MANIFEST)
    assert excinfo.value.substitute == "gsod_noaa"
    assert "gsod_noaa" in str(excinfo.value)


def test_dataset_dir_is_under_the_data_root(tmp_path, monkeypatch):
    monkeypatch.setattr("_labkit.DATA_ROOT", tmp_path)
    assert dataset_dir("demo_set") == tmp_path / "demo_set"
```

- [ ] **Step 3: Run the tests to verify they fail**

Run from `workspace/notebooks/labs`:

```bash
python -m pytest tests/test_labkit_fetch.py -v
```

Expected: collection error — `ModuleNotFoundError: No module named '_labkit'`.

- [ ] **Step 4: Write `_labkit.py` — dataset half only**

Create `workspace/notebooks/labs/_labkit.py`:

```python
"""Plumbing for the Spark labs.

Notebooks import from here so they stay about Spark rather than about paths,
downloads and session wiring. See labs/README.md.
"""
from __future__ import annotations

import hashlib
import os
import shutil
import urllib.request
from pathlib import Path

import yaml

# workspace/notebooks/labs/_labkit.py -> workspace/data
DATA_ROOT = Path(__file__).resolve().parents[2] / "data"
MANIFEST_PATH = Path(__file__).resolve().parent / "datasets.yaml"


class DatasetUnavailable(Exception):
    """Raised when a dataset cannot be fetched and a local stand-in exists."""

    def __init__(self, message: str, substitute: str | None = None):
        super().__init__(message)
        self.substitute = substitute


def load_manifest(path: Path | None = None) -> dict:
    """Parse datasets.yaml into a dict keyed by dataset name."""
    with open(path or MANIFEST_PATH, encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


def dataset_dir(name: str) -> Path:
    """Where a dataset's files live on disk."""
    return DATA_ROOT / name


def _download(url: str, dest: Path) -> None:
    """Fetch one URL to one path. Separated so tests can replace it."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    with urllib.request.urlopen(url, timeout=60) as response, open(dest, "wb") as out:
        shutil.copyfileobj(response, out)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def fetch(name: str, scale: str = "small", manifest: dict | None = None) -> Path:
    """Ensure a dataset is on disk and return its directory.

    Idempotent: if the directory has files, returns immediately without touching
    the network. On failure raises DatasetUnavailable naming a local substitute.
    """
    entries = manifest if manifest is not None else load_manifest()
    if name not in entries:
        raise KeyError(f"{name} is not in datasets.yaml — add it there, not in the notebook")
    entry = entries[name]
    target = dataset_dir(name)

    existing = sorted(p for p in target.glob("*") if p.is_file()) if target.exists() else []
    if existing:
        total = sum(p.stat().st_size for p in existing)
        print(f"{name}: reusing {len(existing)} file(s), {total / 1e6:.1f} MB, at {target}")
        return target

    slices = entry.get(scale) or entry.get("small") or []
    try:
        for slice_id in slices:
            url = entry["source"].format(slice=slice_id)
            dest = target / f"{slice_id}.{entry['format']}"
            print(f"{name}: downloading {url}")
            _download(url, dest)
    except Exception as exc:  # noqa: BLE001 — any failure means "use the substitute"
        substitute = entry.get("substitute")
        if target.exists():
            shutil.rmtree(target, ignore_errors=True)
        raise DatasetUnavailable(
            f"could not fetch {name} ({exc}). Use the local substitute instead: "
            f"{substitute}. Every lab states what changes at reduced fidelity.",
            substitute=substitute,
        ) from exc

    recorded = entry.get("sha256", {})
    for path in sorted(target.glob("*")):
        actual = _sha256(path)
        expected = recorded.get(path.name)
        if expected and expected != actual:
            print(
                f"WARNING {name}/{path.name}: checksum changed — the publisher "
                f"republished this slice. Expected {expected[:12]}…, got {actual[:12]}…"
            )
    total = sum(p.stat().st_size for p in target.glob("*"))
    print(f"{name}: fetched {len(slices)} slice(s), {total / 1e6:.1f} MB, to {target}")
    return target
```

- [ ] **Step 5: Write the manifest with one real entry**

Before writing, open the publisher's page and confirm the current file-naming pattern for a recent month; do not trust the pattern below without checking, and record the month you verified in the comment. Create `workspace/notebooks/labs/datasets.yaml`:

```yaml
# One entry per dataset. This is the only place a URL appears.
# `substitute` names a dataset already on disk, used when the fetch fails.
nyc_taxi_yellow:
  source: https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{slice}.parquet
  format: parquet
  small: ["2025-01"]
  full: ["2025-01", "2025-02", "2025-03", "2025-04", "2025-05", "2025-06"]
  bytes_small: 50000000
  licence: >-
    NYC TLC trip record data, published by the NYC Taxi and Limousine Commission.
    Fetched at runtime; not redistributed in this repo.
  substitute: gsod_noaa
  verified: "2026-08-12"
```

- [ ] **Step 6: Run the tests to verify they pass**

```bash
python -m pytest tests/test_labkit_fetch.py -v
```

Expected: 4 passed. If `test_load_manifest_reads_the_shipped_file` fails, the YAML is missing one of the required keys — fix the manifest, not the test.

- [ ] **Step 7: Commit (stack repo)**

```bash
cd C:/opt/learn/spark/spark-delta-unitycatalog
rm -f workspace/notebooks/labs/.keep
git add workspace/notebooks/labs/_labkit.py workspace/notebooks/labs/datasets.yaml workspace/notebooks/labs/tests/test_labkit_fetch.py
git commit -m "labs: dataset manifest and idempotent fetch

fetch() returns immediately when files are present, so a second lab on the
same dataset costs nothing and re-running a notebook never re-downloads. A
failed fetch raises DatasetUnavailable naming a substitute already on disk,
which is what makes the labs work offline. Checksums warn rather than fail:
for a live dataset a mismatch means the publisher republished."
```

---

### Task 2: `session()` and the Spark helpers

**Files:**
- Modify: `C:/opt/learn/spark/spark-delta-unitycatalog/workspace/notebooks/labs/_labkit.py`
- Create: `C:/opt/learn/spark/spark-delta-unitycatalog/workspace/notebooks/labs/tests/test_labkit_spark.py`

**Interfaces:**
- Consumes: `_labkit` from Task 1.
- Produces: `connect_url() -> str`, `session(app_name: str) -> SparkSession`, `explain_contains(df, node: str) -> bool`, `partition_sizes(df) -> list[int]`. Tasks 3 and 6 call all four.

- [ ] **Step 1: Write the failing tests**

Create `workspace/notebooks/labs/tests/test_labkit_spark.py`:

```python
"""Tests for the Spark side of _labkit. Uses a local session, not the stack."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from _labkit import connect_url, explain_contains, partition_sizes, session  # noqa: E402


def test_connect_url_prefers_the_environment(monkeypatch):
    monkeypatch.setenv("SPARK_REMOTE", "sc://somewhere:15002")
    assert connect_url() == "sc://somewhere:15002"


def test_connect_url_uses_the_container_name_inside_a_container(monkeypatch, tmp_path):
    monkeypatch.delenv("SPARK_REMOTE", raising=False)
    marker = tmp_path / "dockerenv"
    marker.write_text("")
    monkeypatch.setattr("_labkit.IN_CONTAINER_MARKER", marker)
    assert connect_url() == "sc://spark:15002"


def test_connect_url_uses_localhost_on_the_host(monkeypatch, tmp_path):
    monkeypatch.delenv("SPARK_REMOTE", raising=False)
    monkeypatch.setattr("_labkit.IN_CONTAINER_MARKER", tmp_path / "absent")
    assert connect_url() == "sc://localhost:15002"


@pytest.fixture(scope="module")
def spark():
    s = session("labkit-tests", local=True)
    yield s
    s.stop()


def test_partition_sizes_counts_rows_per_partition(spark):
    df = spark.range(0, 100).repartition(4)
    sizes = partition_sizes(df)
    assert len(sizes) == 4
    assert sum(sizes) == 100


def test_explain_contains_finds_an_operator_in_the_plan(spark):
    df = spark.range(0, 10).filter("id > 5")
    assert explain_contains(df, "Filter")
    assert not explain_contains(df, "BroadcastHashJoin")
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
python -m pytest tests/test_labkit_spark.py -v
```

Expected: `ImportError: cannot import name 'connect_url' from '_labkit'`.

- [ ] **Step 3: Add the Spark half to `_labkit.py`**

Append to `workspace/notebooks/labs/_labkit.py`:

```python
# --- Spark ------------------------------------------------------------------

IN_CONTAINER_MARKER = Path("/.dockerenv")


def connect_url() -> str:
    """Where Spark Connect is, for whoever is asking.

    Inside a stack container the service is reachable by its compose name; from
    the host it is the published port. SPARK_REMOTE overrides both.
    """
    override = os.environ.get("SPARK_REMOTE")
    if override:
        return override
    if IN_CONTAINER_MARKER.exists():
        return "sc://spark:15002"
    return "sc://localhost:15002"


def session(app_name: str, local: bool = False):
    """A SparkSession for a lab.

    Connects to the stack over Spark Connect. If the stack is not running, falls
    back to a local session so the notebook still opens on a laptop — the lab
    says which of its observations require the stack.
    """
    from pyspark.sql import SparkSession

    if local:
        return (
            SparkSession.builder.appName(app_name)
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", "8")
            .getOrCreate()
        )

    url = connect_url()
    try:
        spark = SparkSession.builder.appName(app_name).remote(url).getOrCreate()
        spark.sql("SELECT 1").collect()
        print(f"connected to {url}")
        return spark
    except Exception as exc:  # noqa: BLE001 — any failure means "fall back"
        print(
            f"could not reach Spark Connect at {url} ({exc}).\n"
            "Falling back to local[*]. Observations that need the stack — the "
            "Spark UI, Unity Catalog, MinIO — will not work in this mode."
        )
        return session(app_name, local=True)


def explain_contains(df, node: str) -> bool:
    """True when the physical plan contains an operator whose name includes `node`."""
    return node in df._jdf.queryExecution().executedPlan().toString() if hasattr(df, "_jdf") else (
        node in df.__getattr__("explain") and False
    ) or node in _plan_text(df)


def _plan_text(df) -> str:
    """Physical plan as text, on both classic and Connect sessions."""
    import io
    from contextlib import redirect_stdout

    buffer = io.StringIO()
    with redirect_stdout(buffer):
        df.explain(mode="formatted")
    return buffer.getvalue()


def partition_sizes(df) -> list[int]:
    """Row count per partition, in partition order."""
    from pyspark.sql import functions as F

    counts = (
        df.withColumn("_pid", F.spark_partition_id())
        .groupBy("_pid")
        .count()
        .orderBy("_pid")
        .collect()
    )
    return [row["count"] for row in counts]
```

- [ ] **Step 4: Simplify `explain_contains` before running the tests**

The version above is deliberately wrong — it mixes a classic-only `_jdf` path with the Connect path and will not behave the same on both. Replace the whole `explain_contains` function with:

```python
def explain_contains(df, node: str) -> bool:
    """True when the physical plan contains an operator whose name includes `node`.

    Uses the printed plan rather than the JVM object, because a Connect
    DataFrame has no `_jdf` — the same call must work in both session modes.
    """
    return node in _plan_text(df)
```

- [ ] **Step 5: Run the tests to verify they pass**

```bash
python -m pytest tests/test_labkit_spark.py -v
```

Expected: 5 passed. The two Spark tests take ~20 s while the local session starts.

- [ ] **Step 6: Commit (stack repo)**

```bash
cd C:/opt/learn/spark/spark-delta-unitycatalog
git add workspace/notebooks/labs/_labkit.py workspace/notebooks/labs/tests/test_labkit_spark.py
git commit -m "labs: session resolution and plan/partition helpers

connect_url resolves the endpoint by caller: sc://spark:15002 inside a stack
container, sc://localhost:15002 from the host, SPARK_REMOTE overriding both.
session() falls back to local[*] when the stack is down and says which
observations that costs. explain_contains reads the printed plan rather than
_jdf, because a Connect DataFrame has no JVM handle."
```

---

### Task 3: Lab 01 — reading and writing (topic B10)

**Files:**
- Create: `C:/opt/learn/spark/spark-delta-unitycatalog/workspace/notebooks/labs/01-reading-writing.ipynb`
- Create: `C:/opt/learn/spark/spark-delta-unitycatalog/workspace/notebooks/labs/README.md`

**Interfaces:**
- Consumes: `session`, `fetch`, `partition_sizes` from Tasks 1–2.
- Produces: the notebook shape every later lab copies — header, setup, data, body, teardown.

- [ ] **Step 1: Write the labs README**

Create `workspace/notebooks/labs/README.md`:

```markdown
# Labs

Hands-on labs for the [Spark learning path](../../../../notes/docs/learning-path-v2.md).
Each lab is one topic's Milestone made runnable against this stack.

## Running one

1. Start the stack: `docker compose up -d` from the repo root.
2. Open a lab notebook from the host (VS Code or Jupyter) and run it top to bottom.
3. Watch the Spark UI at <http://localhost:4040> when a step says to.

`_labkit.session()` connects over Spark Connect — `sc://localhost:15002` from
the host, `sc://spark:15002` inside a container, `SPARK_REMOTE` overrides both.
With the stack down it falls back to `local[*]`; the lab says what that costs.

## Data

Datasets are declared in `datasets.yaml` and fetched on first use into
`../../data/<name>/`. Nothing is committed — the repo holds the manifest, not
the bytes. Offline, `fetch()` raises and names a substitute already on disk.

## Expectations, not assertions

Labs state what you should see in prose — "roughly N partitions here" — and never
assert it. Real datasets get republished; a lab that fails because a publisher
added a month teaches nothing.

## Tests

`python -m pytest tests/ -v` covers `_labkit` only. The notebooks are not tested;
they are read and run.
```

- [ ] **Step 2: Create the notebook with the five-part shape**

Create `workspace/notebooks/labs/01-reading-writing.ipynb`. Build it with this script so the JSON is valid, then open and run it:

```python
import json
from pathlib import Path

cells = []

def md(text):
    cells.append({"cell_type": "markdown", "metadata": {}, "source": text.splitlines(keepends=True)})

def code(text):
    cells.append({"cell_type": "code", "execution_count": None, "metadata": {},
                  "outputs": [], "source": text.splitlines(keepends=True)})

md("""# Lab 01 — Reading and writing

**Topic:** [B10 — Reading and Writing Data](../../../../notes/docs/learning-path-v2.md)

**What you will observe.** That the number of partitions a read produces is decided
by file layout and one config, not by the size of the data; and that the number of
files a write produces is decided by the partition count you hand it.

**You need:** the stack running (`docker compose up -d`). Runtime about 10 minutes.
Data: one month of NYC taxi parquet, roughly 50 MB. The scale-up step at the end
fetches six months.
""")

code("""import sys
sys.path.insert(0, ".")
from _labkit import session, fetch, partition_sizes

spark = session("lab01-reading-writing")
""")

code("""data = fetch("nyc_taxi_yellow")   # idempotent: prints what it did
sorted(p.name for p in data.glob("*"))
""")

md("""## 1. One file in, how many partitions out?

Before running the next cell, predict the partition count. The file is ~50 MB and
`spark.sql.files.maxPartitionBytes` defaults to 128 MB.
""")

code("""df = spark.read.parquet(str(data))
print("partitions:", df.rdd.getNumPartitions() if hasattr(df, "rdd") else "n/a (Connect)")
print("rows:", df.count())
print("maxPartitionBytes:", spark.conf.get("spark.sql.files.maxPartitionBytes"))
""")

md("""Roughly one partition per 128 MB of *file* — so a single 50 MB file gives one,
regardless of how many rows it holds. Now lower the threshold and read again.
""")

code("""spark.conf.set("spark.sql.files.maxPartitionBytes", "8m")
df_split = spark.read.parquet(str(data))
print("partitions now:", len(partition_sizes(df_split.select("VendorID"))))
spark.conf.set("spark.sql.files.maxPartitionBytes", "128m")
""")

md("""## 2. How many files does a write produce?

One per partition of the DataFrame being written — not one per input file, and not
one per executor. Watch the file count follow the partition count.
""")

code("""out = "/workspace/data/_lab01_out"

df.repartition(4).write.mode("overwrite").parquet(out + "/four")
df.coalesce(1).write.mode("overwrite").parquet(out + "/one")

from pathlib import Path
for name in ("four", "one"):
    files = [p for p in Path(out + "/" + name).glob("*.parquet")]
    print(f"{name}: {len(files)} data file(s)")
""")

md("""## 3. Look at the UI

Open <http://localhost:4040>, find the two write jobs, and read the task count for
each. Four tasks wrote four files; one task wrote one — and that one task read the
whole month by itself, which is the cost `coalesce(1)` hides.

## Scale it up (optional)

`fetch("nyc_taxi_yellow", scale="full")` pulls six months. Re-run section 1 and
watch the partition count follow total *file* size rather than row count.

## Teardown
""")

code("""import shutil
shutil.rmtree("/workspace/data/_lab01_out", ignore_errors=True)
print("removed lab output; the fetched dataset stays for the other labs")
""")

nb = {"cells": cells, "metadata": {"kernelspec": {"display_name": "Python 3",
      "language": "python", "name": "python3"}, "language_info": {"name": "python"}},
      "nbformat": 4, "nbformat_minor": 5}
Path("01-reading-writing.ipynb").write_text(json.dumps(nb, indent=1), encoding="utf-8")
print("written")
```

- [ ] **Step 3: Run the notebook top to bottom against the stack**

Start the stack if needed (`docker compose up -d` from the stack repo root), then run every cell. Fix anything that errors — a wrong URL in the manifest, a path that does not exist inside the container — and re-run. The lab is done when it runs clean from a fresh kernel.

- [ ] **Step 4: Replace predicted numbers with observed ones**

Go back through the markdown and make every "roughly N" match what you actually saw, keeping the hedge. A number in the prose that contradicts the output is worse than no number.

- [ ] **Step 5: Clear outputs and commit (stack repo)**

```bash
cd C:/opt/learn/spark/spark-delta-unitycatalog
jupyter nbconvert --clear-output --inplace workspace/notebooks/labs/01-reading-writing.ipynb
git add workspace/notebooks/labs/01-reading-writing.ipynb workspace/notebooks/labs/README.md
git commit -m "labs: lab 01, reading and writing (B10)

The template every later lab copies: header stating what you will observe,
setup, idempotent fetch, alternating predict-then-run body, teardown. Shows
that read partitioning follows file size and maxPartitionBytes rather than row
count, and that write file count follows the DataFrame's partition count."
```

---

### Task 4: Site integration — index, nav, and the first `**Lab**` line

**Files:**
- Create: `C:/opt/learn/spark/notes/docs/labs.md`
- Modify: `C:/opt/learn/spark/notes/zensical.toml`
- Modify: `C:/opt/learn/spark/notes/docs/learning-path-v2.md` (topic B10)

**Interfaces:**
- Consumes: the notebook filename from Task 3.
- Produces: the index table shape and the `**Lab**` line format Task 6 extends.

- [ ] **Step 1: Write the labs index**

Create `docs/labs.md`:

```markdown
# Labs

Hands-on labs for the topics where running the thing teaches more than reading it.
Each lab is one topic's [Milestone](learning-path-v2.md) made runnable against the
local Spark 4.2.0 + Delta + Unity Catalog stack.

The notebooks live in the stack repo at
`C:/opt/learn/spark/spark-delta-unitycatalog/workspace/notebooks/labs/`, because that
is where they execute — `workspace/` is bind-mounted into the Spark container. Start
the stack with `docker compose up -d`, open a notebook from the host, run it top to
bottom, and watch the Spark UI at <http://localhost:4040> when a step says to.

**Data** is declared in `labs/datasets.yaml` and fetched on first use. Nothing is
committed; offline, a lab names a substitute already on disk and says what changes.

**Expectations are prose, not assertions.** A lab states what you should see and why
it might differ. Real datasets get republished; a lab that fails because a publisher
added a month teaches nothing.

| Lab | Topics | Dataset | Size | Runtime | Notebook |
|---|---|---|---|---|---|
| 01 | **B10** | NYC taxi yellow | ~50 MB | ~10 min | `labs/01-reading-writing.ipynb` |

Ten more labs are planned — see the
[design](superpowers/specs/2026-08-12-spark-labs-design.md) for the full set and the
order they get built.
```

- [ ] **Step 2: Wire it into nav**

In `zensical.toml`, immediately after the `{ "Learning Path — changelog" = … }` line, add:

```toml
    { "Labs" = "labs.md" },
```

- [ ] **Step 3: Add the `**Lab**` line to topic B10**

In `docs/learning-path-v2.md`, find the B10 topic and add this as the last line of that topic, after its Milestone paragraph and after any `>` callouts it already has:

```markdown
**Lab** — [01 · reading and writing](labs.md) runs this against a month of real NYC taxi parquet: partition count from file size, file count from partition count, both read off the UI.
```

- [ ] **Step 4: Build and check**

```bash
cd C:/opt/learn/spark/notes
python -m zensical build 2>&1 | grep -E "issues found|labs.md"
```

Expected: no warning names `labs.md` or `learning-path-v2.md`.

- [ ] **Step 5: Commit (notes repo)**

```bash
cd C:/opt/learn/spark/notes
git add docs/labs.md zensical.toml docs/learning-path-v2.md
git commit -m "docs: labs index, nav entry, and the B10 lab link

The index is the one place notebook paths appear, so a published stack repo
later means changing one file. Topics gain a single Lab line rather than lab
content, keeping the notebook the lab and the page a pointer."
```

---

### Task 5: `check_labs.py`

**Files:**
- Create: `C:/opt/learn/spark/notes/tools/check_labs.py`
- Create: `C:/opt/learn/spark/notes/tools/test_check_labs.py`

**Interfaces:**
- Consumes: `docs/labs.md` and `docs/learning-path-v2.md` from Task 4.
- Produces: `check(notes_root: Path, labs_root: Path) -> list[str]` returning human-readable problems, empty when consistent.

- [ ] **Step 1: Write the failing tests**

Create `tools/test_check_labs.py`:

```python
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from check_labs import check  # noqa: E402

INDEX = """# Labs

| Lab | Topics | Dataset | Size | Runtime | Notebook |
|---|---|---|---|---|---|
| 01 | **B10** | set | 1 MB | 1 min | `labs/01-reading-writing.ipynb` |
"""

PATH_OK = """#### 🔄 B10 — Reading and Writing Data

**Milestone** — do the thing.

**Lab** — [01 · reading and writing](labs.md) runs this.
"""

PATH_MISSING_LINE = """#### 🔄 B10 — Reading and Writing Data

**Milestone** — do the thing.
"""


def _build(tmp_path, index: str, path_doc: str, notebooks: list[str]):
    notes = tmp_path / "notes" / "docs"
    notes.mkdir(parents=True)
    (notes / "labs.md").write_text(index, encoding="utf-8")
    (notes / "learning-path-v2.md").write_text(path_doc, encoding="utf-8")
    labs = tmp_path / "labs"
    labs.mkdir()
    for name in notebooks:
        (labs / name).write_text("{}", encoding="utf-8")
    return notes.parent, labs


def test_clean_when_index_notebooks_and_lab_lines_agree(tmp_path):
    notes, labs = _build(tmp_path, INDEX, PATH_OK, ["01-reading-writing.ipynb"])
    assert check(notes, labs) == []


def test_reports_a_missing_notebook(tmp_path):
    notes, labs = _build(tmp_path, INDEX, PATH_OK, [])
    problems = check(notes, labs)
    assert any("01-reading-writing.ipynb" in p for p in problems)


def test_reports_a_topic_without_a_lab_line(tmp_path):
    notes, labs = _build(tmp_path, INDEX, PATH_MISSING_LINE, ["01-reading-writing.ipynb"])
    problems = check(notes, labs)
    assert any("B10" in p for p in problems)
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
cd C:/opt/learn/spark/notes
python -m pytest tools/test_check_labs.py -v
```

Expected: `ModuleNotFoundError: No module named 'check_labs'`.

- [ ] **Step 3: Write the checker**

Create `tools/check_labs.py`:

```python
#!/usr/bin/env python3
"""Keep the labs index, the learning path and the notebooks on disk in agreement.

Checks three things:
  - every notebook named in docs/labs.md exists,
  - every topic listed in docs/labs.md carries a **Lab** line on the path,
  - every **Lab** line on the path names a lab that the index lists.

Usage: python tools/check_labs.py [--labs <dir>]
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

DEFAULT_LABS = Path("C:/opt/learn/spark/spark-delta-unitycatalog/workspace/notebooks/labs")

ROW_RE = re.compile(r"^\|\s*(\d+)\s*\|\s*(.+?)\s*\|.*`labs/([^`]+)`", re.MULTILINE)
TOPIC_RE = re.compile(r"^#### \S+ ([BIAE]\d+) — ", re.MULTILINE)


def check(notes_root: Path, labs_root: Path) -> list[str]:
    """Return a list of problems; empty means consistent."""
    problems: list[str] = []
    index_path = notes_root / "docs" / "labs.md"
    path_doc = (notes_root / "docs" / "learning-path-v2.md").read_text(encoding="utf-8")
    index = index_path.read_text(encoding="utf-8")

    blocks = re.split(r"(?=^#### \S+ [BIAE]\d+ — )", path_doc, flags=re.MULTILINE)
    lab_line_topics = {
        TOPIC_RE.match(b).group(1)
        for b in blocks
        if TOPIC_RE.match(b) and "**Lab**" in b
    }

    listed_labs = set()
    for _, topics_cell, notebook in ROW_RE.findall(index):
        listed_labs.add(notebook)
        if not (labs_root / notebook).exists():
            problems.append(f"index lists {notebook}, which is not in {labs_root}")
        for topic in re.findall(r"\*\*([BIAE]\d+)\*\*", topics_cell):
            if topic not in lab_line_topics:
                problems.append(f"{topic} is in the labs index but has no **Lab** line on the path")

    for block in blocks:
        m = TOPIC_RE.match(block)
        if not m or "**Lab**" not in block:
            continue
        if not re.search(r"\[\d+ · ", block):
            problems.append(f"{m.group(1)} has a **Lab** line that names no lab")
    return problems


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--labs", default=str(DEFAULT_LABS), help="labs notebook directory")
    parser.add_argument("--root", default=str(Path(__file__).resolve().parents[1]))
    args = parser.parse_args()

    problems = check(Path(args.root), Path(args.labs))
    for problem in problems:
        print(f"FAIL {problem}")
    if problems:
        return 1
    print("labs index, learning path and notebooks agree")
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
python -m pytest tools/test_check_labs.py -v
```

Expected: 3 passed.

- [ ] **Step 5: Run it for real**

```bash
python tools/check_labs.py
```

Expected: `labs index, learning path and notebooks agree`. If it fails, the real content disagrees — fix the content, not the checker.

- [ ] **Step 6: Commit (notes repo)**

```bash
cd C:/opt/learn/spark/notes
git add tools/check_labs.py tools/test_check_labs.py
git commit -m "tools: check the labs index against the path and the notebooks

Three assertions: every notebook the index names exists, every topic the index
claims carries a Lab line, and every Lab line names a lab. This page has
drifted before; twenty lines of checker is cheaper than finding out later."
```

---

### Task 6: Lab 04 — skew (topics A18, A19)

**Files:**
- Create: `C:/opt/learn/spark/spark-delta-unitycatalog/workspace/notebooks/labs/04-skew.ipynb`
- Modify: `C:/opt/learn/spark/notes/docs/labs.md`
- Modify: `C:/opt/learn/spark/notes/docs/learning-path-v2.md` (topics A18 and A19)

**Interfaces:**
- Consumes: everything from Tasks 1–5.
- Produces: proof the template generalises to a lab whose point is a runtime behaviour rather than a file count.

- [ ] **Step 1: Find the real skew in the dataset before writing the lab**

Run this against the stack and record the numbers — the lab is written around what you find, not around an invented example:

```python
import sys; sys.path.insert(0, ".")
from _labkit import session, fetch
from pyspark.sql import functions as F

spark = session("lab04-explore")
data = fetch("nyc_taxi_yellow")
df = spark.read.parquet(str(data))

top = (df.groupBy("PULocationID").count().orderBy(F.desc("count")).limit(10)).collect()
total = df.count()
for row in top:
    print(row["PULocationID"], row["count"], f"{100 * row['count'] / total:.1f}%")
```

If the largest key is under ~5% of rows, the skew is too mild to demonstrate AQE splitting. In that case use `fetch("nyc_taxi_yellow", scale="full")` and repeat, and record in the notebook header that this lab needs the scale-up.

- [ ] **Step 2: Write the notebook**

Same five-part shape as lab 01. The body has four sections, each predict-then-run:

1. **Show the skew** — the group-by counts you recorded in Step 1, as a table.
2. **Join on the skewed key with AQE off** — `spark.conf.set("spark.sql.adaptive.enabled", "false")`, run a join against a small dimension built from the distinct location ids, and read task durations off the UI's stage page. Prose: one task runs many times longer than its peers.
3. **Turn AQE on and repeat** — `spark.sql.adaptive.enabled=true`, `spark.sql.adaptive.skewJoin.enabled=true` (both already default true; set them explicitly so the notebook is self-describing), then use `explain_contains(joined, "AQEShuffleRead")` and look for the skew-split note in the UI. Prose: the long task is now several shorter ones.
4. **The case AQE cannot see** — set `spark.sql.shuffle.partitions` above 2000 so `MapStatus` reports averaged sizes (**A19**), re-run, and observe that the split no longer triggers. Prose: this is the difference between a skew below the threshold and a skew that is invisible in the statistics.

Teardown drops any tables written; the dataset stays.

- [ ] **Step 3: Run it top to bottom from a fresh kernel, then correct the prose numbers**

Every "roughly N" must match what you saw. If section 4 does not reproduce, say so in the notebook rather than deleting the section — a documented "this did not reproduce on this data" is worth more than a section that quietly went missing.

- [ ] **Step 4: Add the index row**

In `docs/labs.md`, add below the lab 01 row:

```markdown
| 04 | **A18**, **A19** | NYC taxi yellow | ~50 MB (scale-up may be needed) | ~20 min | `labs/04-skew.ipynb` |
```

- [ ] **Step 5: Add the `**Lab**` lines**

To topic **A18**, as its last line:

```markdown
**Lab** — [04 · skew](labs.md) uses a genuinely skewed pickup-location key: the same join with AQE off, on, and with the partition count raised past the point where the statistics stop showing the skew.
```

To topic **A19**, as its last line:

```markdown
**Lab** — [04 · skew](labs.md) section 4 is this topic: past 2,000 partitions the reported sizes are an average, and the skew AQE was splitting becomes invisible to it.
```

- [ ] **Step 6: Run the checker and the build**

```bash
cd C:/opt/learn/spark/notes
python tools/check_labs.py
python -m zensical build 2>&1 | grep -c "issues found"
```

Expected: checker prints the agreement line; build reports no new warnings naming `labs.md` or `learning-path-v2.md`.

- [ ] **Step 7: Commit both repos**

```bash
cd C:/opt/learn/spark/spark-delta-unitycatalog
jupyter nbconvert --clear-output --inplace workspace/notebooks/labs/04-skew.ipynb
git add workspace/notebooks/labs/04-skew.ipynb
git commit -m "labs: lab 04, skew (A18, A19)

A real hot key in the taxi data, joined with AQE off and on, then with the
partition count raised past 2000 so MapStatus reports averages and the skew
becomes invisible to the splitter — which is A19's point made observable
rather than asserted."

cd C:/opt/learn/spark/notes
git add docs/labs.md docs/learning-path-v2.md
git commit -m "docs: link lab 04 from the index and from A18/A19"
```

---

## After this plan

Ten labs remain: 02 inference and malformed records, 03 partitioning, 05 joins, 06
caching, 07 AQE, 08 Delta, 09 UDF performance, 10 streaming, 11 transformWithState,
12 catalog boundary. Each follows Task 6's recipe exactly — explore the data first,
write the five-part notebook, run it from a fresh kernel, correct the prose numbers,
add the index row and the `**Lab**` lines, run the checker, commit both repos.

Build them in the spec's order (03, 05 next, then 08, 09, 07, then the rest) rather
than numerically, because that order front-loads the labs where running beats reading
by the widest margin.
