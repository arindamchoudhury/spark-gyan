# Spark labs — design

> **Status:** approved 2026-08-12. Implementation plan follows separately.

Hands-on labs with real datasets, anchored to topics in
[learning path v2](../../learning-path-v2.md), executed as Jupyter notebooks against the
local Spark 4.2.0 + Delta + Unity Catalog + MinIO stack at
`C:/opt/learn/spark/spark-delta-unitycatalog`.

## Why

The path's 185 topics each end in a Milestone, and the Milestones already ask for
observation rather than opinion — *measure this*, *read it from the UI*, *predict then
check*. What they lack is a dataset and a starting point. Twelve labs supply both for the
topics where running the thing teaches something reading cannot.

This is deliberately not a second curriculum. The path is the curriculum; a lab is one
topic's Milestone made runnable.

## Decisions

| Decision | Choice | Rejected alternative and why |
|---|---|---|
| Format | Runnable Jupyter notebooks in the stack repo | Site-only lab pages — nothing executes; readers copy snippets and diverge |
| Anchoring | **Topic-anchored**: one lab per topic, 1:1 links | Dataset-anchored arcs — richer, but many-to-many links into the middle of an arc |
| Coverage | Curated set of 12 labs, ~20 topics | One per topic (185, absurd); one per checkpoint (4, too coarse) |
| Data | Real datasets fetched from their publishers | Generated-only — loses the messiness that makes real data instructive |
| Size | Small slice by default, documented opt-in scale-up | A single global budget — either too small for skew or too slow for everything else |
| Location | Stack repo `workspace/notebooks/labs/`, site links to it | Notes repo — needs a compose mount and puts notebooks in a Zensical-built tree |
| Verification | Exploratory, prose expectations | Assertions — brittle against live datasets that republish monthly |

## Lab set

Twelve labs. The topic column is the anchor; a lab may touch neighbours in passing.

| Lab | Topics | What only running it shows |
|---|---|---|
| 01 `reading-writing` | B10 | File count and partition count from one read; how options change both |
| 02 `inference-malformed` | I28, I29 | The three parse modes on genuinely dirty rows, and `_corrupt_record` |
| 03 `partitioning` | I24 | `repartition` vs `coalesce` on one job; coalesce propagating upward |
| 04 `skew` | A18, A19 | A real hot key; AQE splitting it, and the case it structurally cannot see |
| 05 `joins` | A15, A16 | The four strategies on one dataset; the buffering that kills a task |
| 06 `caching` | I25 | Storage levels, spill, and cache-then-checkpoint |
| 07 `aqe` | A11, A12 | A plan re-planned at runtime, and one where the better plan is discarded |
| 08 `delta` | I37, A39 | Time travel, then `VACUUM` breaking it; deletion vectors on and off |
| 09 `udf-performance` | I10, I50 | Python UDF vs Arrow vs built-in, measured; worker reuse made visible |
| 10 `streaming` | A32, A34 | A stream with a watermark; state growing and being bounded |
| 11 `transform-with-state` | A59, A60 | State variables and timers, then reading state back as a DataFrame |
| 12 `catalog-boundary` | E29, A51 | Three-level names, then reading the path directly to bypass governance |

## Layout

```
spark-delta-unitycatalog/workspace/notebooks/labs/
  _labkit.py                  shared helpers
  datasets.yaml               dataset manifest
  README.md                   what the labs are, how to run them
  01-reading-writing.ipynb
  …
  12-catalog-boundary.ipynb
```

## Notebook anatomy

Every notebook has the same five parts, in this order.

1. **Header (markdown).** Lab number; the topic codes it exercises with a link back to those
   topics on the site; *what you will observe*; which containers must be running; rough
   runtime; data size at default scale, with the opt-in scale noted.
2. **Setup (one cell).** `session()` from `_labkit`, which connects over Spark Connect
   (`sc://spark:15002`), matching how the stack's Dagster assets already reach Spark, and
   falls back to `local[*]` when the stack is not running so the notebook still opens on a
   laptop.
3. **Data (one cell).** `fetch("<dataset>")` — idempotent; prints whether it downloaded or
   reused, and how much.
4. **Body.** Alternating markdown and code. Each step states what to expect *before* the
   cell runs, then the cell, then an instruction to go and look — a named Spark UI tab, or a
   printed metric. Numbers are phrased as "roughly N on this data", with the reason they may
   differ. No assertions.
5. **Teardown.** Drops the tables and paths the lab wrote, and states what it left behind on
   purpose — lab 08 deliberately leaves a vacuumed table so time travel can be seen to fail.

### `_labkit.py`

Plumbing lives here so notebooks stay about Spark:

- `session(app_name)` — Connect with `local[*]` fallback
- `fetch(name, scale="small")` — manifest-driven download, idempotent
- `explain_contains(df, node)` — does the physical plan contain this operator
- `stage_metrics()` — last query's stage-level numbers as a DataFrame
- `partition_sizes(df)` — rows per partition, for skew and coalesce labs

One module, no framework.

## Data

`datasets.yaml` is the single place a URL appears:

```yaml
<name>:
  source:      <publisher URL>
  format:      parquet | csv | json
  small:       [<slice ids>]        # the default
  full:        [<slice ids>]        # opt-in scale-up
  bytes_small: <int>
  licence:     <terms, and that we fetch rather than redistribute>
  substitute:  <dataset already on disk>
```

Rules:

- **Nothing is committed.** `workspace/data/` stays gitignored apart from the small files
  already present. The repo holds the manifest and the fetcher; the bytes live on disk only.
  We link to publishers rather than redistributing, which keeps the licence question simple.
- **`fetch` is idempotent.** Files present means return immediately, so a second lab on the
  same dataset costs nothing and re-running a notebook never re-downloads.
- **Checksums warn, never fail.** Recorded on first fetch. For live datasets a mismatch is
  information — the publisher republished — not an error.
- **Offline is a first-class path.** No network, or a dead URL, raises with a message naming
  the `substitute` from the data already on disk: `gsod_noaa` (684 MB, real weather),
  `broadcast_logs` (33 MB, genuine join keys), `recipes` (53 MB, messy CSV). Every lab is
  written so the substitute still demonstrates the effect at reduced fidelity.
- **URLs are verified when each lab is written**, not up front. A URL confirmed today would
  need re-confirming at build time anyway; the manifest is where rot gets fixed once.

## Site integration

Three touchpoints, no more.

1. **`docs/labs.md`** — an index of twelve rows: lab number, topics exercised, dataset, size
   at default scale, rough runtime, notebook path. Added to `zensical.toml` nav after
   Learning Path.
2. **A `**Lab**` line** on each covered topic, after its Milestone, naming the notebook and
   what it exercises. Twenty topics gain one line; the other 165 are untouched.
3. **No lab content is duplicated into the path.** The notebook is the lab; the page points
   at it.

Links are local paths, since the stack is not published. If that repo gains a remote, the
index is the one file to change.

### `tools/check_labs.py`

Asserts three things and exits non-zero otherwise:

- every lab listed in `docs/labs.md` exists on disk,
- every topic named in the index carries a `**Lab**` line,
- every `**Lab**` line names a notebook that exists.

The path has drifted before. A twenty-line checker is cheap insurance.

## Build order

1. Skeleton — `_labkit.py`, `datasets.yaml`, `fetch()`, labs README.
2. **Lab 01 end to end**, including the site index, nav entry and its `**Lab**` line —
   proves the whole loop before it is repeated.
3. Labs 04, 05, 03 — skew, joins, partitioning: where running beats reading by the widest
   margin.
4. Labs 08, 09, 07 — Delta, UDF performance, AQE.
5. Labs 02, 06, 10, 11, 12.
6. `check_labs.py`, once three labs exist and the pattern has stopped moving.

## Out of scope

- **Assertions and a headless runner.** Considered and rejected: brittle against datasets
  that republish monthly. If the labs later need regression-proofing against a Spark
  upgrade, that is a separate decision.
- **Labs for the other 165 topics.** The set is curated by design.
- **Publishing the stack repo.** Local paths are sufficient while both repos are local.
- **Any change to the 165 topics that do not get a lab.**
