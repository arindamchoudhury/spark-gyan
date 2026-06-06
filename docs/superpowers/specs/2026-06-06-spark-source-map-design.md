# Spark Source Map — Design

**Date:** 2026-06-06 · **Status:** approved, implementing · **Author:** brainstormed with Claude

## Goal

A repeatable, Spark-specific pipeline that mines the Apache Spark source to **drive the
spark-book**: it produces (1) a deterministic, whole-repo catalog of every Spark config and
(2) on-demand, full code-path-traced concept maps per subsystem, both reconciled against the
book's existing 40-topic learning path (coverage + discovery gaps).

## Decisions (from brainstorming)

- **Taxonomy:** *map + discover* — build a source-derived concept map AND reconcile it against
  the 40 learning-path topics (B/I/A/E). Surface both coverage and uncovered "discovery gaps".
- **Depth:** *full code-path tracing* per concept (entry point → analysis → physical execution →
  anchor classes).
- **Execution:** *incremental, per-subsystem repeatable skill* — deterministic config catalog for
  the whole repo up front (cheap, complete); full tracing run subsystem-by-subsystem on demand.
- **Output form:** *structured data + rendered pages* — `catalog.yaml` is source of truth, rendered
  to Zensical reference pages; traced maps are Markdown pages; all wired into nav.
- **Approach A (two-engine):** deterministic Python config parser + LLM subsystem tracer. Chosen
  over all-LLM (configs must not be guessed) and config-only (under-delivers on tracing).

## Architecture

```
Spark source ──(gen_configs.py, deterministic)──> configs/catalog.yaml ──render──> configs/index.md
Spark source ──(tracer subagent, on-demand)──────> subsystems/<slug>.md
learning-path.md + catalog + subsystems ─────────> index.md (concept map + coverage matrix)
```

Scripts live in `tools/spark_source_map/`. Artifacts live in
`docs/reference/spark-source-map/`. The skill `spark-source-map` orchestrates.

### Engine 1 — Config catalog generator (`gen_configs.py`)

Deterministic. Scans every `*.scala` for the two builder families `buildConf("…")` and
`ConfigBuilder("…")` across the whole repo. Per chained builder, extracts: key, type
(`.intConf/.booleanConf/.stringConf/.bytesConf/.timeConf/.doubleConf/.longConf/.enumConf`),
default (`createWithDefault*` / `createOptional` / `createWithDefaultFunction` / `.fallbackConf`),
`.version(…)` (often absent in core), multi-line concatenated `.doc(…)`, and source file:line.
Emits `configs/catalog.yaml`; renders `configs/index.md`.

### Engine 2 — Subsystem tracer (skill-driven subagent, on-demand)

Takes one subsystem (or a concept within it if too large). Enumerates concepts bottom-up, full
code-path traces each, pulls its configs from `catalog.yaml`, reconciles to topic codes. Emits
`subsystems/<slug>.md`. Source-faithful: says so rather than inventing a path it can't trace.

### Coverage index (`index.md`)

Mermaid concept map (subsystems → concepts) + two-way matrix: each of the 40 topics → backing
source concepts (or "gap, untraced"); inverse → source concepts mapping to no topic.

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

`subsystems/<slug>.md`: front matter (`subsystem, spark_version, status, traced_concepts`) + one
section per concept (what-it-is, code path, anchor files, configs, topic mapping + coverage).

## Skill workflow (`spark-source-map`)

- `gen configs` — run generator, render, report entry/unparsed counts + version; wire nav; commit.
- `trace <subsystem>` — verify catalog/version; dispatch tracer subagent; if subsystem too large,
  list concepts and ask for a subset (`status: partial`); write page; update matrix; nav; commit.
- `coverage` — regenerate the matrix from existing pages + learning path; cheap.

Recommended order: `gen configs` once, then `trace` in book-priority order — `sql/core`,
`sql/catalyst`, then `streaming`, `mllib`, `core`, `connect`.

## Error handling

- **Never silently drop a config.** Unresolvable builder chains → `unparsed[]` with raw snippet +
  file:line; `unparsed_count` in meta. Missing = bug; flagged = honest.
- Computed defaults → `default_kind: expr`, verbatim.
- Multi-line chains joined from `buildConf(` to terminal before field extraction.
- Version drift: `meta.spark_version` parsed from source; `trace`/`coverage` warn on mismatch.
- Tracer: too-large subsystem → concept-scoped, `status: partial`; un-traceable concept → says so.

## Testing

- Parser unit tests (pytest) on embedded Scala snippets: exact extraction (e.g.
  `spark.sql.shuffle.partitions` → int, default 200, version 1.1.0). Floor assertion
  (`entry_count > 1000` against real source) so a regex regression that drops configs fails loudly.
- Renderer: snapshot of `configs/index.md` for a small fixture catalog.
- Traced pages: human / `book-gap-filler` review against local source; not auto-tested.

## Conventions (project memory)

Blank line before bullet lists; Mermaid never ASCII; blockquotes not bare admonitions after lists;
nav wired in `zensical.toml`; Spark facts verified against **local source at v4.1.x**, not the web.
