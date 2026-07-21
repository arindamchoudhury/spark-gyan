# Spark Feature & Capability Evolution Catalog — Design

**Date:** 2026-07-21
**Status:** Approved (design), pending implementation plan
**Source corpus:** `release-notes/spark_all_changelogs.txt` (99 releases, 0.3 → 4.2.0; 16,563 unique SPARK-IDs, 32,849 mentions)

## Goal

A complete, non-hallucinated catalog of every Spark **feature and improvement**, organized
**by capability**, showing **when each was added and how it evolved**, every entry traceable to a
JIRA SPARK-ID (or, where the source gives none, to a release + source line). Built progressively
from the first release forward.

## Scope decisions (locked)

- **Granularity:** Features + all Improvements. Track JIRA types `New Feature`, `Improvement`,
  `Story`, `Epic`, `Umbrella`. Exclude pure `Bug`, `Sub-task`, `Task`, `Test`,
  `Dependency upgrade`, `Documentation`, `Question`, `Wish`, `Technical task`.
- **Organizing axis:** By capability, evolving. Each capability area is a chronological timeline.
- **Audit ledger:** Yes — machine-readable `_catalog.jsonl` (kept) + `_dropped.jsonl` (excluded,
  with reason) as source of truth. Enables mechanical proof that nothing was silently lost.
- **Excluded types in pages:** Fully omitted from rendered pages (they live only in `_dropped.jsonl`).

## Corpus shape (verified)

- **17 releases carry structured JIRA-type dumps** (the major `x.y.0` line releases + a few
  others). These embed `<h2>` section headers: `New Feature`, `Improvement`, `Bug`, `Sub-task`,
  `Task`, `Test`, `Umbrella`, `Story`, `Epic`, `Dependency upgrade`, `Documentation`, `Question`,
  `Wish`, `Technical task`. Each `<li>` under a section is one JIRA item with SPARK-ID + verbatim
  title. This is the bulk of the feature/improvement signal.
- **The other 82 releases** are either narrative prose (0.x–1.x eras) or patch releases (`x.y.z`,
  z>0) that are mostly bug fixes with occasional backported improvements. SPARK-IDs sparse.
- **Every release** opens with prose highlights describing named capabilities; SPARK-IDs may or
  may not be attached in that prose.

## Deliverable

New docs section: `docs/reference/spark-feature-history/`

- `index.md` — landing page: what this catalog is; coverage table (99 releases, date range);
  how to read a timeline entry; the capability map with links; ledger explanation + completeness
  invariant.
- One page per **capability area** (~18–22 pages). Each page:
  - Chronological timeline of that capability's Features + Improvements.
  - Entry format: `release · [SPARK-ID](jira-url) · verbatim title` (SPARK-ID omitted only when
    the source truly has none; then `release · (prose) · description`).
  - Grouped into eras; short connective "how it evolved" prose at inflection points
    (e.g. DSv1→DSv2, DStreams→Structured Streaming, RDD→DataFrame, Arrow adoption,
    classic→Spark Connect).
- `_catalog.jsonl` — one record per tracked item:
  `{spark_id, release, area, type, title}`.
- `_dropped.jsonl` — one record per excluded item:
  `{spark_id, release, type, title, reason}`.

These `_*.jsonl` files are audit artifacts, not a second rendered view.

## Capability taxonomy (~18–22 areas)

Core/RDD/Scheduler · SQL & Catalyst · ANSI & Data Types · Built-in Functions ·
Data Sources & DSv2 · Connectors (Kafka/JDBC/Parquet/ORC/Avro) · Structured Streaming ·
DStreams (legacy) · PySpark & Python UDFs · pandas API on Spark · Arrow ·
Spark Connect · MLlib/ML · GraphX · SparkR · Deploy (Standalone/YARN/Mesos/K8s) ·
Shuffle/Storage/Memory · Web UI/History/Metrics · Security · Geospatial ·
Build & Language support · (catch-all: Misc/Other, only if an item fits nowhere).

Each catalog record is tagged to **exactly one** area. Ambiguous items get a documented
tie-break rule (primary subsystem wins).

## Extraction pipeline (per era — the no-hallucination guarantee)

Every rendered entry must trace to a specific source line. No inference of features that the
source does not state.

1. **Structured-dump releases (17):** parse `<h2>` sections.
   - KEEP items under `New Feature`, `Improvement`, `Story`, `Epic`, `Umbrella`.
   - DROP items under `Bug`, `Sub-task`, `Task`, `Test`, `Dependency upgrade`, `Documentation`,
     `Question`, `Wish`, `Technical task` → `_dropped.jsonl` with `reason=type:<type>`.
   - Each kept `<li>` → one `_catalog.jsonl` record with verbatim title + SPARK-ID.
2. **Prose highlights (all releases):** named features/capabilities in narrative → records.
   Attach SPARK-ID when the prose gives one; else `spark_id=null`, `type=prose`. Do not
   fabricate an ID.
3. **Patch releases (`x.y.z`, z>0):** scan for improvement/new-feature language and any
   SPARK-IDs. Expect few (mostly backports). Include what is present; invent nothing.
4. **Area tagging:** each record → exactly one capability area via keyword + subsystem rules;
   tie-break = primary subsystem.

## Build order (progressive)

Process oldest → newest: 0.3, 0.5.x, 0.6.x, … 4.2.0. Append to `_catalog.jsonl` /
`_dropped.jsonl` release by release. Capability pages are (re)generated/grown from the catalog.

**Checkpoints per major line** — pause for user review after each:
`0.x` → `1.x` → `2.x` → `3.x` → `4.x`. This matches "start from the first release and build it
piece by piece progressively."

## No-loss verification (completeness invariant)

After each release is processed, assert:

```
count(kept) + count(dropped_by_type) + count(prose_only) == total distinct items in that release
```

where "total distinct items" = the SPARK-IDs and `<li>` entries the parser saw in that release's
section. Any unaccounted item is a parser gap and blocks progress until resolved. Result: even the
excluded bug/test/dep tickets are recorded in `_dropped.jsonl` — nothing vanishes silently.

Final cross-check: `sort -u` of all SPARK-IDs across `_catalog.jsonl` + `_dropped.jsonl` should
reconcile against `grep -o 'SPARK-[0-9]*' | sort -u` on the source (allowing for IDs that appear
only in prose / cross-references, which are logged separately).

## Nav & site integration

- Add the new section + all pages to the `nav` array in `zensical.toml` (no auto-discovery).
- Follow project conventions: blank line before bullet lists; Mermaid (not ASCII) for any diagram;
  `>` blockquotes instead of `!!!` admonitions after lists.

## Out of scope

- Per-release "both views" rendering (user chose capability-only view).
- Cataloging pure bug fixes as page content (they stay in the ledger only).
- Rewriting or re-verifying the source changelog file itself (treated as given input).

## Open questions

- Exact final count of capability pages (may merge low-volume areas like GraphX + GraphFrames,
  or split high-volume SQL). Decided during build from real record counts.
- Whether `_catalog.jsonl` generation is scripted (Python parser) or done by careful manual
  extraction per release — resolved in the implementation plan. Scripting the `<h2>` dump parse is
  strongly preferred for the 17 structured releases (deterministic, auditable); prose extraction
  is manual.
