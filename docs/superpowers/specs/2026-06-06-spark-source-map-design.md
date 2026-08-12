# Spark Source Map — Design

> **Superseded in part (2026-08-10).** Where this spec says `learning-path.md`, the pipeline now reads and writes **`learning-path-v2.md`**; v1 is frozen. Topic headings are `####` rather than `###`, proposals land before a level's 🎯 checkpoint, and a proposal whose code is already taken is reallocated to the next free code in its level rather than dropped. The rest of the design holds.

**Date:** 2026-06-06 · **Status:** approved, implementing · **Author:** brainstormed with Claude

## Goal

A repeatable, Spark-specific pipeline that mines the Apache Spark source to **drive the
spark-book**: it produces (1) a deterministic, whole-repo catalog of every Spark config,
(2) on-demand **topic-first traces** (for each learning-path topic, find the backing source),
and (3) on-demand **source sweeps** (scan a subsystem and discover concepts the learning path
doesn't yet cover). All three are reconciled into a hybrid coverage view.

The learning path is never assumed to be complete — sweeps expand it by surfacing unknown unknowns.

## Decisions (from brainstorming)

- **Taxonomy:** *map + discover* — build a source-derived concept map AND reconcile it against
  the 40 learning-path topics (B/I/A/E). Surface both coverage (which topics have a source trace)
  and discovery gaps (which source concepts have no topic).
- **Hybrid direction:** Two complementary tracing directions:
  - **Topic-first** (`trace <code>`) — start from a learning-path topic, find backing source.
    Right for "ground the book in source" work.
  - **Source-first** (`sweep <subsystem>`) — scan a subsystem, find unmapped concepts.
    Right for "what did we miss" discovery that expands the learning path.
- **Depth:** *full code-path tracing* per concept (entry point → analysis → physical execution →
  anchor classes with file:line anchors).
- **Execution:** *incremental, on-demand* — deterministic config catalog for the whole repo once
  (cheap, complete); topic traces and sweeps done one unit at a time, each reviewable before commit.
- **Output form:** *structured data + rendered pages* — `catalog.yaml` is source of truth for
  configs; `topics/*.md` are the primary artifacts; `sweeps/*.md` are the discovery artifacts;
  `index.md` is auto-generated from all of them.
- **Approach:** deterministic Python config parser + LLM topic tracer + LLM sweeper. All-LLM
  rejected (configs must not be guessed). Config-only rejected (under-delivers on tracing).

## Architecture

```
Spark source ──(gen_configs.py, deterministic)──────────────> configs/catalog.yaml ──> configs/index.md
                                                                        │ (lookup)
Learning-path topic ──(topic tracer subagent, on-demand)──────────────> topics/<code>.md
Spark subsystem ──────(sweeper subagent, on-demand)───────────────────> sweeps/<slug>.md

learning-path.md + configs/catalog.yaml + topics/*.md + sweeps/*.md ──> index.md
```

Scripts live in `tools/spark_source_map/`. Artifacts live in
`docs/reference/spark-source-map/`. The skill `spark-source-map` orchestrates.

### Engine 1 — Config catalog (`gen_configs.py`, deterministic)

Scans every `*.scala` for the two builder families `buildConf("…")` and
`ConfigBuilder("…")` across the whole repo. Per chained builder, extracts: key, type
(`.intConf/.booleanConf/.stringConf/.bytesConf/.timeConf/.doubleConf/.longConf/.enumConf`),
default (`createWithDefault*` / `createOptional` / `createWithDefaultFunction` / `.fallbackConf`),
`.version(…)`, multi-line concatenated `.doc(…)`, and source file:line. Emits `configs/catalog.yaml`;
renders `configs/index.md`. Used as a lookup by both tracing engines.

### Engine 2 — Topic tracer (skill-driven subagent, on-demand)

Takes one learning-path topic code (B7, I4, A11, …). Finds the relevant source classes and their
code paths, pulls matching configs from `catalog.yaml` by keyword, and produces a topic page
(`topics/<code>.md`) with source anchors, code-path narrative, and relevant config table.
Source-faithful: sets `status: partial` and notes gaps rather than inventing paths.

**Repos:** Most topics → `C:\opt\learn\spark\spark`. Delta Lake topics (I8, A6, E4) → local
Delta repo. Unity Catalog (E5), Dagster (E6), CI/CD (E7) → appropriate external sources.

### Engine 3 — Source sweeper (skill-driven subagent, on-demand)

Takes one subsystem (or a group within it). Enumerates major concepts from the package layout
and `groups.yaml` scope definitions. For each concept, traces the code path and maps it to
existing topic codes from `learning-path.md`. Unmapped concepts become `propose:` blocks in
`sweeps/<slug>.md`, which `gen_coverage.py` auto-appends to `learning-path.md`.

### Coverage index (`index.md`, `gen_coverage.py`)

Generated from:
- `topics/*.md` → topic-trace coverage matrix (which of the 40 topics are traced)
- `sweeps/*.md` → concept map (mermaid) + discovery gaps table + sweep-status table
- `configs/catalog.yaml` → config counts per subsystem for sweep-status table
- `learning-path.md` → full topic list

## Data formats

`configs/catalog.yaml`:

```yaml
meta: {spark_version, source_root, generated_at, entry_count, unparsed_count}
configs:
  - {key, type, default, default_kind, version, doc, source_file, source_line, prefix, subsystem}
unparsed:
  - {raw, source_file, source_line}
```

`default_kind` ∈ `literal | string | optional | expr | fallback`. Computed defaults stored verbatim
as `expr` — never coerced to a fake literal.

`topics/<code>.md` front matter (topic-first traces):

```yaml
---
topic: B7
title: "Joins: Types and Mechanics"
status: complete          # or: partial
chapter: 10
repos: [apache/spark]
configs: [spark.sql.autoBroadcastJoinThreshold, ...]
sources:
  - subsystem: sql/catalyst
    concepts: [JoinSelection]
  - subsystem: sql/core
    concepts: [SortMergeJoinExec, BroadcastHashJoinExec]
---
```

`sweeps/<slug>.md` front matter (source-first discovery):

```yaml
---
subsystem: sql/core
spark_version: "4.1.2"
group: joins-and-agg        # optional; for large subsystems swept in parts
all_groups: [joins-and-agg, streaming-integration, connector-v2]
status: complete            # or: partial
concepts:
  - name: joins
    topics: [B7, A3]        # [] = discovery gap
  - name: vectorized-reader
    topics: []
    propose:
      code: A12
      level: Advanced
      title: "..."
      what: "..."
      why: "..."
---
```

`groups.yaml` (`docs/reference/spark-source-map/groups.yaml`) — authoritative scope definitions
for all 10 subsystems. Used by the sweeper to scope its search and by `gen_configs.py` to inject
group descriptions into the config catalog Contents section.

## Skill workflow (`spark-source-map`)

- `gen configs` — run `gen_configs.py`, render, report entry/unparsed counts + version; wire nav; commit.
- `trace <code>` — look up topic in `learning-path.md`; grep config slice by keyword; dispatch topic
  tracer subagent; write `topics/<code>.md`; run `gen_coverage.py`; wire nav; commit.
- `sweep <subsystem>` — confirm catalog exists; enumerate concepts (via `groups.yaml`); if subsystem
  too large, ask user to pick a group (`status: partial`); dispatch sweeper subagent; write
  `sweeps/<slug>.md`; run `gen_coverage.py` (auto-appends proposals to `learning-path.md`); wire nav; commit.
- `coverage` — regenerate `index.md` from all existing artifacts; cheap; pass `--no-write-proposals`
  to skip learning-path updates.

Recommended starting order: `gen configs` once, then alternate between `trace` (book-priority topics)
and `sweep` (highest-config subsystems: `sql/catalyst`, `sql/core`).

## Error handling

- **Never silently drop a config.** Unresolvable builder chains → `unparsed[]` with raw snippet +
  file:line; `unparsed_count` in meta. Missing = bug; flagged = honest.
- Computed defaults → `default_kind: expr`, verbatim.
- Multi-line chains joined from `buildConf(` to terminal before field extraction.
- Version drift: `meta.spark_version` parsed from source; `trace`/`coverage` warn on mismatch.
- Topic tracer: un-traceable path → noted in prose, `status: partial`.
- Sweeper: too-large subsystem → group-scoped, `status: partial`; un-traceable concept → says so.

## Testing

- Parser unit tests (pytest) on embedded Scala snippets: exact extraction (e.g.
  `spark.sql.shuffle.partitions` → int, default 200, version 1.1.0). Floor assertion
  (`entry_count > 1000` against real source) so a regex regression that drops configs fails loudly.
- Renderer: snapshot of `configs/index.md` for a small fixture catalog.
- Topic and sweep pages: human review against local source; not auto-tested.

## Conventions (project memory)

Blank line before bullet lists; Mermaid never ASCII; blockquotes not bare admonitions after lists;
nav wired in `zensical.toml`; Spark facts verified against **local source at v4.1.x**, not the web.
