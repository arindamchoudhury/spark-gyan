# Spark Feature & Capability Evolution Catalog — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a capability-organized, JIRA-tracked catalog of every Spark feature + improvement across all 99 releases (0.3 → 4.2.0), with a machine-readable audit ledger that proves no item is silently dropped.

**Architecture:** A deterministic Python parser extracts JIRA items from the 17 structured `<h2>`-dump releases in `release-notes/spark_all_changelogs.txt`, classifies each by type (keep Feature/Improvement/Story/Epic/Umbrella; drop Bug/Test/Sub-task/Task/Dep-upgrade/Doc/Question/Wish), tags each to one capability area, and writes `_catalog.jsonl` (kept) + `_dropped.jsonl` (excluded). A renderer emits per-area chronological timeline tables into Markdown pages between AUTO markers; humans add "how it evolved" connective prose outside those markers and extract prose-only features from the 82 non-dump releases. A completeness checker enforces `kept + dropped + prose == total items` per release.

**Tech Stack:** Python 3.13 (stdlib only — `re`, `json`, `html`, `pathlib`), pytest 9.0.3, Zensical (docs site). No new runtime deps.

## Global Constraints

- Source corpus is read-only input: `release-notes/spark_all_changelogs.txt`. Never edit it.
- No hallucination: every rendered entry traces to a SPARK-ID or a verbatim source line in that release's block. Never invent a SPARK-ID or a feature the source does not state.
- Kept JIRA types: `New Feature`, `Improvement`, `Story`, `Epic`, `Umbrella`. Dropped types: `Bug`, `Sub-task`, `Task`, `Test`, `Dependency upgrade`, `Documentation`, `Question`, `Wish`, `Technical task`.
- Dump releases are self-detected by presence of `<h2` in the release block — do NOT hardcode by version number (patch releases 3-5-9, 4-0-3, 4-0-4, 4-1-1, 4-1-2, 4-1-3, 3-1-1, 2-0-1, 2-0-2 also carry dumps).
- Each catalog record is tagged to exactly one capability area.
- Titles stored verbatim from source, HTML-unescaped (`&amp;` → `&`, `&#39;` → `'`).
- Build order is oldest → newest; checkpoint for user review after each major line (0.x → 1.x → 2.x → 3.x → 4.x).
- Project docs conventions: blank line before every bullet list; Mermaid (never ASCII) for diagrams; `>` blockquote instead of `!!!` admonition when a paragraph follows a list; every new page added to `nav` in `zensical.toml` (no auto-discovery).
- SPARK-ID links: `https://issues.apache.org/jira/browse/SPARK-NNNNN`.

## File Structure

- `release-notes/catalog/__init__.py` — package marker.
- `release-notes/catalog/parser.py` — release-block iteration, `<h2>` section split, `<li>` item extraction, type classification.
- `release-notes/catalog/areas.py` — capability taxonomy, keyword rules, override loader, area assignment.
- `release-notes/catalog/completeness.py` — per-release invariant + global SPARK-ID reconciliation.
- `release-notes/catalog/build.py` — orchestrator; writes the two ledgers.
- `release-notes/catalog/render.py` — emits per-area timeline tables into AUTO-marker blocks.
- `release-notes/catalog/area_overrides.tsv` — manual `spark_id<TAB>area` corrections (tagger consults first).
- `release-notes/tests/test_parser.py`, `test_areas.py`, `test_completeness.py`, `test_render.py` — pytest.
- `docs/reference/spark-feature-history/index.md` — landing page.
- `docs/reference/spark-feature-history/<area-slug>.md` — one per capability area.
- `docs/reference/spark-feature-history/_catalog.jsonl`, `_dropped.jsonl` — generated ledgers.
- `zensical.toml` — nav wiring (modify).

---

## Task 1: Parser package scaffold + JIRA-type classifier

**Files:**
- Create: `release-notes/catalog/__init__.py`
- Create: `release-notes/catalog/parser.py`
- Test: `release-notes/tests/test_parser.py`

**Interfaces:**
- Produces: `KEEP_TYPES: set[str]`, `DROP_TYPES: set[str]`, `classify_type(type_label: str) -> str` returning `"keep"` or `"drop"` (raises `ValueError` on an unknown type so new JIRA types can never be silently mis-bucketed).

- [ ] **Step 1: Write the failing test**

```python
# release-notes/tests/test_parser.py
from catalog.parser import classify_type, KEEP_TYPES, DROP_TYPES

def test_keep_types_classified_keep():
    for t in ["New Feature", "Improvement", "Story", "Epic", "Umbrella"]:
        assert classify_type(t) == "keep"

def test_drop_types_classified_drop():
    for t in ["Bug", "Sub-task", "Task", "Test", "Dependency upgrade",
              "Documentation", "Question", "Wish", "Technical task"]:
        assert classify_type(t) == "drop"

def test_unknown_type_raises():
    import pytest
    with pytest.raises(ValueError):
        classify_type("Brand New Jira Type")

def test_keep_and_drop_disjoint():
    assert KEEP_TYPES.isdisjoint(DROP_TYPES)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd release-notes && python -m pytest tests/test_parser.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'catalog'`.

- [ ] **Step 3: Write minimal implementation**

```python
# release-notes/catalog/__init__.py
# (empty package marker)
```

```python
# release-notes/catalog/parser.py
"""Deterministic parser for release-notes/spark_all_changelogs.txt."""

KEEP_TYPES = {"New Feature", "Improvement", "Story", "Epic", "Umbrella"}
DROP_TYPES = {
    "Bug", "Sub-task", "Task", "Test", "Dependency upgrade",
    "Documentation", "Question", "Wish", "Technical task",
}

def classify_type(type_label: str) -> str:
    label = type_label.strip()
    if label in KEEP_TYPES:
        return "keep"
    if label in DROP_TYPES:
        return "drop"
    raise ValueError(f"Unknown JIRA type: {label!r}")
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd release-notes && python -m pytest tests/test_parser.py -v`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add release-notes/catalog/__init__.py release-notes/catalog/parser.py release-notes/tests/test_parser.py
git commit -m "feat(catalog): JIRA-type classifier with fail-closed unknown handling"
```

---

## Task 2: Release-block iterator

**Files:**
- Modify: `release-notes/catalog/parser.py`
- Test: `release-notes/tests/test_parser.py`

**Interfaces:**
- Produces: `iter_releases(text: str) -> Iterator[tuple[str, str]]` yielding `(release_slug, block_text)` where `release_slug` is the token after `RELEASE:` (e.g. `spark-release-3-0-0`) and `block_text` is everything up to the next `RELEASE:` header. `has_dump(block_text: str) -> bool` returns True iff the block contains `<h2`.

- [ ] **Step 1: Write the failing test**

```python
# add to release-notes/tests/test_parser.py
from catalog.parser import iter_releases, has_dump

SAMPLE = """RELEASE: spark-release-9-9-9
SOURCE: http://x
====
Spark Release 9.9.9
Some prose.
RELEASE: spark-release-8-8-8
SOURCE: http://y
====
<h2>New Feature</h2>
<li>[SPARK-1] - thing</li>
"""

def test_iter_releases_splits_on_header():
    rels = list(iter_releases(SAMPLE))
    assert [r[0] for r in rels] == ["spark-release-9-9-9", "spark-release-8-8-8"]

def test_has_dump_detects_h2():
    rels = dict(iter_releases(SAMPLE))
    assert has_dump(rels["spark-release-8-8-8"]) is True
    assert has_dump(rels["spark-release-9-9-9"]) is False
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd release-notes && python -m pytest tests/test_parser.py -k "iter_releases or has_dump" -v`
Expected: FAIL — `ImportError: cannot import name 'iter_releases'`.

- [ ] **Step 3: Write minimal implementation**

```python
# add to release-notes/catalog/parser.py
import re
from typing import Iterator

_RELEASE_RE = re.compile(r"^RELEASE:\s*(\S+)\s*$", re.MULTILINE)

def iter_releases(text: str) -> Iterator[tuple[str, str]]:
    matches = list(_RELEASE_RE.finditer(text))
    for i, m in enumerate(matches):
        slug = m.group(1)
        start = m.end()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        yield slug, text[start:end]

def has_dump(block_text: str) -> bool:
    return "<h2" in block_text
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd release-notes && python -m pytest tests/test_parser.py -v`
Expected: PASS (6 tests).

- [ ] **Step 5: Commit**

```bash
git add release-notes/catalog/parser.py release-notes/tests/test_parser.py
git commit -m "feat(catalog): release-block iterator + dump detection"
```

---

## Task 3: `<h2>` dump section + `<li>` item extraction

**Files:**
- Modify: `release-notes/catalog/parser.py`
- Test: `release-notes/tests/test_parser.py`

**Interfaces:**
- Produces: dataclass `Item(spark_id: str | None, title: str, jira_type: str, disposition: str)` and `parse_dump(block_text: str) -> list[Item]`. Each `<li>` under an `<h2>` section becomes one `Item` whose `jira_type` is the enclosing section label, `disposition` is `classify_type(jira_type)`, `spark_id` like `"SPARK-24882"` (or None if the `<li>` has no SPARK link), `title` is the verbatim item text after the ID, HTML-unescaped and whitespace-collapsed.

- [ ] **Step 1: Write the failing test**

```python
# add to release-notes/tests/test_parser.py
from catalog.parser import parse_dump, Item

DUMP = """<h2>        New Feature</h2>
<ul>
<li>[<a href='https://issues.apache.org/jira/browse/SPARK-24882'>SPARK-24882</a>] -         data source v2 API improvement &amp; cleanup</li>
</ul>
<h2>        Bug</h2>
<ul>
<li>[<a href='https://issues.apache.org/jira/browse/SPARK-25567'>SPARK-25567</a>] -         Table listing doesn&#39;t sort</li>
</ul>
"""

def test_parse_dump_extracts_items():
    items = parse_dump(DUMP)
    assert len(items) == 2
    feat = items[0]
    assert feat.spark_id == "SPARK-24882"
    assert feat.jira_type == "New Feature"
    assert feat.disposition == "keep"
    assert feat.title == "data source v2 API improvement & cleanup"

def test_parse_dump_unescapes_and_drops_bug():
    items = parse_dump(DUMP)
    bug = items[1]
    assert bug.jira_type == "Bug"
    assert bug.disposition == "drop"
    assert bug.title == "Table listing doesn't sort"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd release-notes && python -m pytest tests/test_parser.py -k parse_dump -v`
Expected: FAIL — `ImportError: cannot import name 'parse_dump'`.

- [ ] **Step 3: Write minimal implementation**

```python
# add to release-notes/catalog/parser.py
import html
from dataclasses import dataclass

@dataclass
class Item:
    spark_id: str | None
    title: str
    jira_type: str
    disposition: str

_H2_RE = re.compile(r"<h2[^>]*>(.*?)</h2>", re.IGNORECASE | re.DOTALL)
_LI_RE = re.compile(r"<li>(.*?)</li>", re.IGNORECASE | re.DOTALL)
_ID_RE = re.compile(r"SPARK-\d+")
_TAG_RE = re.compile(r"<[^>]+>")

def _clean(fragment: str) -> str:
    text = _TAG_RE.sub("", fragment)
    text = html.unescape(text)
    return re.sub(r"\s+", " ", text).strip()

def parse_dump(block_text: str) -> list[Item]:
    items: list[Item] = []
    # Walk h2 headers; each governs the text until the next h2.
    headers = list(_H2_RE.finditer(block_text))
    for i, h in enumerate(headers):
        jira_type = _clean(h.group(1))
        seg_start = h.end()
        seg_end = headers[i + 1].start() if i + 1 < len(headers) else len(block_text)
        segment = block_text[seg_start:seg_end]
        try:
            disposition = classify_type(jira_type)
        except ValueError:
            raise ValueError(f"Unknown JIRA <h2> type: {jira_type!r}")
        for li in _LI_RE.finditer(segment):
            raw = li.group(1)
            id_match = _ID_RE.search(raw)
            spark_id = id_match.group(0) if id_match else None
            title = _clean(raw)
            # Strip leading "[SPARK-NNNN] - " prefix from the title.
            title = re.sub(r"^\[?SPARK-\d+\]?\s*-?\s*", "", title).strip()
            items.append(Item(spark_id, title, jira_type, disposition))
    return items
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd release-notes && python -m pytest tests/test_parser.py -v`
Expected: PASS (8 tests).

- [ ] **Step 5: Commit**

```bash
git add release-notes/catalog/parser.py release-notes/tests/test_parser.py
git commit -m "feat(catalog): extract <li> JIRA items from <h2> dump sections"
```

---

## Task 4: Capability-area taxonomy + tagger

**Files:**
- Create: `release-notes/catalog/areas.py`
- Create: `release-notes/catalog/area_overrides.tsv`
- Test: `release-notes/tests/test_areas.py`

**Interfaces:**
- Produces: `AREAS: list[tuple[str, str]]` (list of `(slug, display_name)`); `assign_area(spark_id: str | None, title: str, overrides: dict[str, str]) -> str` returning an area slug (falls back to `"misc"`); `load_overrides(path) -> dict[str, str]`. Override map (spark_id → slug) always wins over keyword rules.

- [ ] **Step 1: Write the failing test**

```python
# release-notes/tests/test_areas.py
from catalog.areas import assign_area, AREAS, load_overrides

SLUGS = {s for s, _ in AREAS}

def test_all_areas_have_unique_slugs():
    slugs = [s for s, _ in AREAS]
    assert len(slugs) == len(set(slugs))
    assert "misc" in slugs

def test_keyword_routing():
    assert assign_area("SPARK-1", "Structured Streaming watermark support", {}) == "structured-streaming"
    assert assign_area("SPARK-2", "pandas API on Spark: add DataFrame.corr", {}) == "pandas-on-spark"
    assert assign_area("SPARK-3", "Spark Connect: support foreachBatch", {}) == "spark-connect"
    assert assign_area("SPARK-4", "Kubernetes: executor pod template", {}) == "deploy"

def test_override_wins_over_keywords():
    ov = {"SPARK-5": "geospatial"}
    # title looks like SQL, but override forces geospatial
    assert assign_area("SPARK-5", "add ST_Distance to SQL functions", ov) == "geospatial"

def test_unmatched_falls_back_to_misc():
    assert assign_area("SPARK-6", "zzz unclassifiable text", {}) == "misc"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd release-notes && python -m pytest tests/test_areas.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'catalog.areas'`.

- [ ] **Step 3: Write minimal implementation**

```python
# release-notes/catalog/area_overrides.tsv
# spark_id<TAB>area_slug   — manual corrections; one per line; '#' comments allowed.
```

```python
# release-notes/catalog/areas.py
"""Capability taxonomy and keyword-based area tagging."""
from pathlib import Path

# (slug, display name) — order = page order on the index.
AREAS = [
    ("core-rdd", "Core / RDD / Scheduler"),
    ("sql-catalyst", "SQL & Catalyst"),
    ("ansi-types", "ANSI & Data Types"),
    ("builtin-functions", "Built-in Functions"),
    ("datasources-dsv2", "Data Sources & DSv2"),
    ("connectors", "Connectors (Kafka/JDBC/Parquet/ORC/Avro)"),
    ("structured-streaming", "Structured Streaming"),
    ("dstreams", "DStreams (legacy streaming)"),
    ("pyspark", "PySpark & Python UDFs"),
    ("pandas-on-spark", "pandas API on Spark"),
    ("arrow", "Arrow"),
    ("spark-connect", "Spark Connect"),
    ("mllib", "MLlib / ML"),
    ("graphx", "GraphX"),
    ("sparkr", "SparkR"),
    ("deploy", "Deploy (Standalone/YARN/Mesos/K8s)"),
    ("shuffle-storage", "Shuffle / Storage / Memory"),
    ("web-ui", "Web UI / History / Metrics"),
    ("security", "Security"),
    ("geospatial", "Geospatial"),
    ("build-lang", "Build & Language support"),
    ("misc", "Misc / Other"),
]

# Ordered rules: FIRST match wins. Put specific/narrow areas before broad ones
# (e.g. pandas-on-spark and spark-connect before generic pyspark/sql).
_RULES = [
    ("geospatial", ["st_", "geospatial", "geometry", "geography", " srid", "wkb", "wkt"]),
    ("spark-connect", ["spark connect", "connect client", "connect server", "connect: "]),
    ("pandas-on-spark", ["pandas api on spark", "pandas-on-spark", "koalas", "ps.dataframe"]),
    ("arrow", ["arrow-optimized", "pyarrow", " arrow ", "arrow-based", "arrow ipc"]),
    ("structured-streaming", ["structured streaming", "streaming query", "watermark",
                               "continuous processing", "statestore", "state store",
                               "foreachbatch", "microbatch", "micro-batch", "kafka source"]),
    ("dstreams", ["dstream", "receiver", "streaming context", "kinesis receiver"]),
    ("pandas-on-spark", ["pandas udf", "pandas_udf", "grouped map", "cogroup"]),
    ("pyspark", ["pyspark", "python udf", "python worker", "python api", "py4j"]),
    ("sparkr", ["sparkr", " r api", "r udf", "r dataframe", "dapply", "gapply"]),
    ("mllib", ["mllib", " ml ", "ml.", "estimator", "transformer", "classifier",
                "regression", "clustering", "feature transformer", "pipeline stage"]),
    ("graphx", ["graphx", "pregel", "graphframe", "connected components"]),
    ("connectors", ["kafka", "jdbc", "parquet", "orc", "avro", "csv datasource",
                     "json datasource", "hive metastore", "thrift"]),
    ("datasources-dsv2", ["data source v2", "datasource v2", "dsv2", "datasourcev2",
                           "table catalog", "catalog api", "file source", "partition stats"]),
    ("ansi-types", ["ansi", "interval type", "timestamp_ntz", "decimal", "char/varchar",
                     "variant type", "data type", "collation"]),
    ("builtin-functions", ["built-in function", "builtin function", "add function",
                            "sql function", "add expression", "aggregate function"]),
    ("security", ["security", "authentication", "encryption", "kerberos", "acl",
                   "ssl", "tls", "token", "credential"]),
    ("web-ui", ["web ui", " ui ", "history server", "metrics", "rest api", "prometheus",
                 "event log"]),
    ("shuffle-storage", ["shuffle", "memory management", "off-heap", "spill",
                          "block manager", "storage level", "external shuffle"]),
    ("deploy", ["yarn", "mesos", "kubernetes", "k8s", "standalone", "cluster manager",
                 "deploy mode", "resource manager", "executor pod"]),
    ("build-lang", ["scala 2.1", "scala 2.13", "java 1", "java 17", "java 21", "build",
                     "maven", "sbt", "upgrade to scala", "python 3."]),
    ("sql-catalyst", ["sql", "catalyst", "dataframe", "dataset", "optimizer",
                       "query plan", "adaptive query", "join", "aggregate", "subquery"]),
    ("core-rdd", ["rdd", "scheduler", "task", "dag", "accumulator", "broadcast",
                   "serializer", "closure", "partition"]),
]

def load_overrides(path) -> dict[str, str]:
    result: dict[str, str] = {}
    p = Path(path)
    if not p.exists():
        return result
    for line in p.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        sid, _, area = line.partition("\t")
        if sid and area:
            result[sid.strip()] = area.strip()
    return result

def assign_area(spark_id, title, overrides) -> str:
    if spark_id and spark_id in overrides:
        return overrides[spark_id]
    hay = f" {title.lower()} "
    for slug, keywords in _RULES:
        if any(kw in hay for kw in keywords):
            return slug
    return "misc"
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd release-notes && python -m pytest tests/test_areas.py -v`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
git add release-notes/catalog/areas.py release-notes/catalog/area_overrides.tsv release-notes/tests/test_areas.py
git commit -m "feat(catalog): capability taxonomy + keyword area tagger with overrides"
```

---

## Task 5: Completeness checker

**Files:**
- Create: `release-notes/catalog/completeness.py`
- Test: `release-notes/tests/test_completeness.py`

**Interfaces:**
- Produces: `check_release(block_text: str, items: list[Item]) -> dict` returning `{"total_li": int, "kept": int, "dropped": int, "unaccounted": int, "ok": bool}` where `total_li` counts `<li>` tags in the block, `kept`+`dropped` come from item dispositions, and `ok` is True iff `kept + dropped == total_li` (every `<li>` accounted). Raises nothing; the caller asserts `ok`.

- [ ] **Step 1: Write the failing test**

```python
# release-notes/tests/test_completeness.py
from catalog.parser import parse_dump
from catalog.completeness import check_release

DUMP = """<h2>New Feature</h2>
<li>[<a href='x'>SPARK-1</a>] - feat one</li>
<li>[<a href='x'>SPARK-2</a>] - feat two</li>
<h2>Bug</h2>
<li>[<a href='x'>SPARK-3</a>] - bug one</li>
"""

def test_check_release_balances():
    items = parse_dump(DUMP)
    report = check_release(DUMP, items)
    assert report["total_li"] == 3
    assert report["kept"] == 2
    assert report["dropped"] == 1
    assert report["unaccounted"] == 0
    assert report["ok"] is True
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd release-notes && python -m pytest tests/test_completeness.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'catalog.completeness'`.

- [ ] **Step 3: Write minimal implementation**

```python
# release-notes/catalog/completeness.py
"""Per-release no-loss invariant."""
import re

_LI_COUNT_RE = re.compile(r"<li>", re.IGNORECASE)

def check_release(block_text: str, items) -> dict:
    total_li = len(_LI_COUNT_RE.findall(block_text))
    kept = sum(1 for it in items if it.disposition == "keep")
    dropped = sum(1 for it in items if it.disposition == "drop")
    unaccounted = total_li - (kept + dropped)
    return {
        "total_li": total_li,
        "kept": kept,
        "dropped": dropped,
        "unaccounted": unaccounted,
        "ok": unaccounted == 0,
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd release-notes && python -m pytest tests/test_completeness.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add release-notes/catalog/completeness.py release-notes/tests/test_completeness.py
git commit -m "feat(catalog): per-release no-loss completeness checker"
```

---

## Task 6: Build orchestrator — generate the ledgers over all dump releases

**Files:**
- Create: `release-notes/catalog/build.py`
- Create: `docs/reference/spark-feature-history/_catalog.jsonl` (generated)
- Create: `docs/reference/spark-feature-history/_dropped.jsonl` (generated)

**Interfaces:**
- Consumes: `iter_releases`, `has_dump`, `parse_dump` (parser); `assign_area`, `load_overrides` (areas); `check_release` (completeness).
- Produces: `build(source_path, out_dir) -> dict` writing `_catalog.jsonl` (records `{spark_id, release, area, type, title}` for kept items) and `_dropped.jsonl` (`{spark_id, release, type, title, reason}` for dropped), and returning a per-release report list. This is a script task (no unit test); its gate is the completeness assertion + reconciliation in Step 3–4.

- [ ] **Step 1: Write the orchestrator**

```python
# release-notes/catalog/build.py
"""Build _catalog.jsonl + _dropped.jsonl from the changelog dumps."""
import json
import sys
from pathlib import Path

from catalog.parser import iter_releases, has_dump, parse_dump
from catalog.areas import assign_area, load_overrides
from catalog.completeness import check_release

HERE = Path(__file__).resolve().parent
DEFAULT_SOURCE = HERE.parent / "spark_all_changelogs.txt"
DEFAULT_OUT = HERE.parent.parent / "docs" / "reference" / "spark-feature-history"
OVERRIDES = HERE / "area_overrides.tsv"

def normalize_release(slug: str) -> str:
    # spark-release-3-0-0 / spark-release-4.1.1 -> 3.0.0 / 4.1.1
    tail = slug.replace("spark-release-", "")
    return tail.replace("-", ".")

def build(source_path=DEFAULT_SOURCE, out_dir=DEFAULT_OUT) -> list[dict]:
    text = Path(source_path).read_text(encoding="utf-8", errors="replace")
    overrides = load_overrides(OVERRIDES)
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    catalog_f = (out / "_catalog.jsonl").open("w", encoding="utf-8")
    dropped_f = (out / "_dropped.jsonl").open("w", encoding="utf-8")
    reports = []
    try:
        for slug, block in iter_releases(text):
            if not has_dump(block):
                continue
            release = normalize_release(slug)
            items = parse_dump(block)
            report = check_release(block, items)
            report["release"] = release
            reports.append(report)
            if not report["ok"]:
                print(f"UNBALANCED {release}: {report}", file=sys.stderr)
            for it in items:
                if it.disposition == "keep":
                    area = assign_area(it.spark_id, it.title, overrides)
                    catalog_f.write(json.dumps({
                        "spark_id": it.spark_id, "release": release,
                        "area": area, "type": it.jira_type, "title": it.title,
                    }) + "\n")
                else:
                    dropped_f.write(json.dumps({
                        "spark_id": it.spark_id, "release": release,
                        "type": it.jira_type, "title": it.title,
                        "reason": f"type:{it.jira_type}",
                    }) + "\n")
    finally:
        catalog_f.close()
        dropped_f.close()
    return reports

if __name__ == "__main__":
    reps = build()
    bad = [r for r in reps if not r["ok"]]
    tot_kept = sum(r["kept"] for r in reps)
    tot_drop = sum(r["dropped"] for r in reps)
    print(f"dump releases: {len(reps)}  kept: {tot_kept}  dropped: {tot_drop}  unbalanced: {len(bad)}")
    sys.exit(1 if bad else 0)
```

- [ ] **Step 2: Run the build**

Run: `cd release-notes && python -m catalog.build`
Expected: prints a summary line; exit code 0 (no unbalanced releases). If any release is unbalanced, STOP and inspect that release's block — the parser missed `<li>`s. Fix `parse_dump` before continuing.

- [ ] **Step 3: Reconcile SPARK-IDs against raw source (no-loss proof for dump releases)**

Run:
```bash
cd release-notes
# every SPARK-ID appearing under a dump release must land in catalog OR dropped
python - <<'PY'
import json, re
from pathlib import Path
from catalog.parser import iter_releases, has_dump
src = Path("spark_all_changelogs.txt").read_text(encoding="utf-8", errors="replace")
dump_ids = set()
for slug, block in iter_releases(src):
    if has_dump(block):
        dump_ids |= set(re.findall(r"SPARK-\d+", block))
seen = set()
for fn in ("_catalog.jsonl", "_dropped.jsonl"):
    for line in Path("../docs/reference/spark-feature-history", fn).read_text(encoding="utf-8").splitlines():
        sid = json.loads(line)["spark_id"]
        if sid: seen.add(sid)
missing = dump_ids - seen
print("dump SPARK-IDs:", len(dump_ids), "captured:", len(seen & dump_ids), "MISSING:", len(missing))
print(sorted(missing)[:20])
PY
```
Expected: `MISSING: 0` (IDs may appear multiple times / in prose; the check is that no dump `<li>` ID is absent from both ledgers). A nonzero MISSING count means an `<li>` was skipped — fix `parse_dump` and rebuild.

- [ ] **Step 4: Commit**

```bash
git add release-notes/catalog/build.py docs/reference/spark-feature-history/_catalog.jsonl docs/reference/spark-feature-history/_dropped.jsonl
git commit -m "feat(catalog): build ledgers from 17 dump releases (reconciled, 0 missing)"
```

---

## Task 7: Page renderer — AUTO-marker timeline tables

**Files:**
- Create: `release-notes/catalog/render.py`
- Test: `release-notes/tests/test_render.py`

**Interfaces:**
- Consumes: `_catalog.jsonl`, `AREAS`.
- Produces: `render_timeline(records: list[dict]) -> str` returning a Markdown table (columns: Release · SPARK-ID (linked) · Type · Title), sorted by semantic version ascending then SPARK-ID; and `write_pages(catalog_path, out_dir)` which, for each area, replaces the content between `<!-- AUTO:timeline START -->` and `<!-- AUTO:timeline END -->` in `<slug>.md` (creating the file from a template if absent), leaving all human prose outside the markers untouched.

- [ ] **Step 1: Write the failing test**

```python
# release-notes/tests/test_render.py
from catalog.render import render_timeline, replace_auto_block

RECS = [
    {"spark_id": "SPARK-2", "release": "3.0.0", "area": "sql-catalyst", "type": "Improvement", "title": "b feature"},
    {"spark_id": "SPARK-1", "release": "1.6.0", "area": "sql-catalyst", "type": "New Feature", "title": "a feature"},
]

def test_render_timeline_sorted_by_version_asc():
    md = render_timeline(RECS)
    assert md.index("1.6.0") < md.index("3.0.0")
    assert "https://issues.apache.org/jira/browse/SPARK-1" in md
    assert "| a feature |" in md

def test_replace_auto_block_preserves_prose():
    page = "Intro prose.\n<!-- AUTO:timeline START -->\nOLD\n<!-- AUTO:timeline END -->\nOutro."
    out = replace_auto_block(page, "NEW TABLE")
    assert "Intro prose." in out and "Outro." in out
    assert "OLD" not in out and "NEW TABLE" in out
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd release-notes && python -m pytest tests/test_render.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'catalog.render'`.

- [ ] **Step 3: Write minimal implementation**

```python
# release-notes/catalog/render.py
"""Render per-area timeline tables into AUTO-marker blocks."""
import json
import re
from pathlib import Path

from catalog.areas import AREAS

START = "<!-- AUTO:timeline START -->"
END = "<!-- AUTO:timeline END -->"
JIRA = "https://issues.apache.org/jira/browse/"

def _ver_key(release: str):
    parts = []
    for p in release.split("."):
        parts.append(int(p) if p.isdigit() else 0)
    while len(parts) < 3:
        parts.append(0)
    return tuple(parts)

def render_timeline(records: list[dict]) -> str:
    rows = sorted(records, key=lambda r: (_ver_key(r["release"]),
                                          int(re.sub(r"\D", "", r["spark_id"] or "0") or 0)))
    out = ["| Release | JIRA | Type | Title |", "|---|---|---|---|"]
    for r in rows:
        sid = r["spark_id"]
        link = f"[{sid}]({JIRA}{sid})" if sid else "—"
        title = r["title"].replace("|", "\\|")
        out.append(f"| {r['release']} | {link} | {r['type']} | {title} |")
    return "\n".join(out)

def replace_auto_block(page_text: str, new_block: str) -> str:
    pattern = re.compile(re.escape(START) + r".*?" + re.escape(END), re.DOTALL)
    replacement = f"{START}\n{new_block}\n{END}"
    if pattern.search(page_text):
        return pattern.sub(lambda _: replacement, page_text)
    return page_text.rstrip() + "\n\n" + replacement + "\n"

def _template(display: str) -> str:
    return (f"# {display}\n\n"
            f"> Source: `release-notes/spark_all_changelogs.txt`. "
            f"Timeline rows below are generated from `_catalog.jsonl`; "
            f"prose outside the AUTO markers is hand-written.\n\n"
            f"## How it evolved\n\n_TODO: connective prose added during the era passes._\n\n"
            f"## Timeline\n\n{START}\n{END}\n")

def write_pages(catalog_path, out_dir):
    records = [json.loads(l) for l in Path(catalog_path).read_text(encoding="utf-8").splitlines() if l.strip()]
    by_area: dict[str, list] = {}
    for r in records:
        by_area.setdefault(r["area"], []).append(r)
    out = Path(out_dir)
    for slug, display in AREAS:
        recs = by_area.get(slug, [])
        page_path = out / f"{slug}.md"
        page = page_path.read_text(encoding="utf-8") if page_path.exists() else _template(display)
        table = render_timeline(recs) if recs else "_No tracked items in this area yet._"
        page_path.write_text(replace_auto_block(page, table), encoding="utf-8")

if __name__ == "__main__":
    here = Path(__file__).resolve().parent
    write_pages(here.parent.parent / "docs/reference/spark-feature-history/_catalog.jsonl",
                here.parent.parent / "docs/reference/spark-feature-history")
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd release-notes && python -m pytest tests/test_render.py -v`
Expected: PASS.

- [ ] **Step 5: Generate the pages + eyeball area distribution**

Run:
```bash
cd release-notes && python -m catalog.render
python - <<'PY'
import json, collections
from pathlib import Path
c = collections.Counter(json.loads(l)["area"] for l in Path("../docs/reference/spark-feature-history/_catalog.jsonl").read_text(encoding="utf-8").splitlines() if l.strip())
for k, v in c.most_common(): print(f"{v:5d} {k}")
PY
```
Expected: per-area counts printed. Review `misc` — if it is large (say >10% of items), add keyword rules or `area_overrides.tsv` entries and re-run Tasks 6+7. This is the area-quality gate.

- [ ] **Step 6: Commit**

```bash
git add release-notes/catalog/render.py release-notes/tests/test_render.py docs/reference/spark-feature-history/*.md
git commit -m "feat(catalog): render per-area timeline tables into AUTO blocks"
```

---

## Task 8: Section index + nav wiring + build smoke test

**Files:**
- Create: `docs/reference/spark-feature-history/index.md`
- Modify: `zensical.toml` (nav array)

**Interfaces:** none (docs + config).

- [ ] **Step 1: Write the index page**

Create `docs/reference/spark-feature-history/index.md` with: title; one-paragraph purpose; a coverage line (99 releases, 0.3 → 4.2.0, generated from `spark_all_changelogs.txt`); a "How to read a timeline entry" note; a Markdown list (blank line before it) linking to every area page; and a "Completeness & ledger" section explaining `_catalog.jsonl` / `_dropped.jsonl` and the `kept + dropped == total <li>` invariant. Use a `>` blockquote (not `!!!`) for the ledger note that follows the list.

- [ ] **Step 2: Wire nav in `zensical.toml`**

Read the existing `nav` array, find the `reference` grouping, and add a `Spark Feature History` subtree listing `index.md` + every `<slug>.md` in `AREAS` order. Match the existing nav indentation/style exactly.

- [ ] **Step 3: Build smoke test**

Run: `cd /c/opt/learn/spark/notes && zensical build`
Expected: build succeeds, no errors about missing nav targets. If a page is missing from nav, Zensical warns — add it.

- [ ] **Step 4: Commit**

```bash
git add docs/reference/spark-feature-history/index.md zensical.toml
git commit -m "feat(catalog): section index + nav wiring; site builds clean"
```

---

## Task 9: Era pass — 0.x (progressive checkpoint 1)

**Files:**
- Modify: `docs/reference/spark-feature-history/<area>.md` (prose outside AUTO markers)
- Append prose-only records to a sidecar: `docs/reference/spark-feature-history/_prose.jsonl`

**Interfaces:**
- Produces: `_prose.jsonl` records `{spark_id: null|str, release, area, title, source_quote}` for named features that appear only in narrative prose (no dump `<li>`), each with a verbatim `source_quote` copied from the release block. These feed the timeline via a small extension: render reads catalog + prose.

- [ ] **Step 1: Extract 0.x narrative features**

For each release `0.3 … 0.9.2` (no dumps), read its block in `spark_all_changelogs.txt`. For every named feature/capability in the prose (e.g. "standalone deploy mode", "Java API", "per-RDD storage levels"), append one `_prose.jsonl` record: pick the area slug, set `spark_id` to the ID if the prose gives one else `null`, and copy the exact sentence into `source_quote`. Invent nothing — if the prose does not name it, it does not go in.

- [ ] **Step 2: Verify every prose record traces to source**

Run:
```bash
cd release-notes && python - <<'PY'
import json
from pathlib import Path
src = Path("spark_all_changelogs.txt").read_text(encoding="utf-8", errors="replace")
bad = []
for l in Path("../docs/reference/spark-feature-history/_prose.jsonl").read_text(encoding="utf-8").splitlines():
    if not l.strip(): continue
    rec = json.loads(l)
    q = rec["source_quote"].strip()
    if q and q not in src: bad.append((rec["release"], q[:60]))
print("untraceable prose quotes:", len(bad))
for b in bad[:20]: print(b)
PY
```
Expected: `untraceable prose quotes: 0`. Any nonzero means a quote was paraphrased — fix it to verbatim.

- [ ] **Step 3: Extend render to include prose records, regenerate, write 0.x connective prose**

Update `render.write_pages` to also load `_prose.jsonl` (same shape, `type="prose"`) and merge into `by_area` before rendering. Regenerate pages (`python -m catalog.render`). Then, in each area page touched by 0.x, write the "How it evolved" opening prose covering the 0.x era, outside the AUTO markers.

- [ ] **Step 4: Build + commit (CHECKPOINT — pause for user review)**

Run: `cd /c/opt/learn/spark/notes && zensical build`
Expected: clean build.
```bash
git add release-notes/catalog/render.py docs/reference/spark-feature-history/_prose.jsonl docs/reference/spark-feature-history/*.md
git commit -m "content(catalog): 0.x era features + connective prose"
```
Then STOP and ask the user to review the 0.x pages before starting 1.x.

---

## Task 10: Era pass — 1.x (checkpoint 2)

**Files:** same pattern as Task 9.

- [ ] **Step 1:** Extract prose-only features for releases `1.0.0 … 1.6.3`. The dump releases in this line (`1.5.0`, `1.6.0`, `1.6.3`) already contributed to `_catalog.jsonl` in Task 6 — do NOT re-enter their `<li>` items; only add narrative-only capabilities from the prose highlights (e.g. DataFrame API introduction, Tungsten, ML Pipelines, SparkR arrival) as `_prose.jsonl` records with verbatim `source_quote`.
- [ ] **Step 2:** Run the traceability check from Task 9 Step 2. Expected `0`.
- [ ] **Step 3:** Regenerate pages; write 1.x connective prose (DataFrame/Tungsten/ML-Pipelines/SparkR inflection points) outside AUTO markers.
- [ ] **Step 4:** `zensical build`; commit `content(catalog): 1.x era features + connective prose`; STOP for user review.

---

## Task 11: Era pass — 2.x (checkpoint 3)

**Files:** same pattern.

- [ ] **Step 1:** Prose-only features for `2.0.0 … 2.4.8`. Dump releases `2.0.0/2.0.1/2.0.2/2.1.0/2.2.0` already in catalog — only add narrative-only items (Structured Streaming GA, Datasets unification, Spark Session, cost-based optimizer, continuous processing preview) as `_prose.jsonl`.
- [ ] **Step 2:** Traceability check → `0`.
- [ ] **Step 3:** Regenerate; write 2.x connective prose (RDD→Dataset/DataFrame convergence, DStreams→Structured Streaming) outside markers.
- [ ] **Step 4:** `zensical build`; commit `content(catalog): 2.x era`; STOP for review.

---

## Task 12: Era pass — 3.x (checkpoint 4)

**Files:** same pattern.

- [ ] **Step 1:** Prose-only features for `3.0.0 … 3.5.9`. Dump releases `3.0.0/3.1.1/3.2.0/3.5.9` already in catalog — add narrative-only items (AQE, dynamic partition pruning, ANSI mode, pandas API on Spark / Koalas merge, Spark Connect preview in 3.4, K8s GA) as `_prose.jsonl`.
- [ ] **Step 2:** Traceability check → `0`.
- [ ] **Step 3:** Regenerate; write 3.x connective prose (AQE, pandas-on-Spark arrival, Spark Connect introduction) outside markers.
- [ ] **Step 4:** `zensical build`; commit `content(catalog): 3.x era`; STOP for review.

---

## Task 13: Era pass — 4.x + final reconciliation (checkpoint 5)

**Files:** same pattern, plus a final audit.

- [ ] **Step 1:** Prose-only features for `4.0.0 … 4.2.0`. Dump releases `4.0.3/4.0.4/4.1.1/4.1.2/4.1.3` already in catalog — add narrative-only items (Geospatial types, CDC `CHANGES` clause, Auto CDC in Declarative Pipelines, Arrow UDFs default-on, Spark Connect maturation, `SET PATH`/metric views) as `_prose.jsonl`.
- [ ] **Step 2:** Traceability check → `0`.
- [ ] **Step 3:** Regenerate; write 4.x connective prose (geospatial, CDC, Arrow-by-default, Connect as the default client path) outside markers.
- [ ] **Step 4: Final global reconciliation audit**

Run:
```bash
cd release-notes && python - <<'PY'
import json, re
from pathlib import Path
base = Path("../docs/reference/spark-feature-history")
src_ids = set(re.findall(r"SPARK-\d+", Path("spark_all_changelogs.txt").read_text(encoding="utf-8", errors="replace")))
seen = set()
for fn in ("_catalog.jsonl", "_dropped.jsonl", "_prose.jsonl"):
    p = base / fn
    if not p.exists(): continue
    for l in p.read_text(encoding="utf-8").splitlines():
        if not l.strip(): continue
        sid = json.loads(l).get("spark_id")
        if sid: seen.add(sid)
unaccounted = src_ids - seen
print("source SPARK-IDs:", len(src_ids))
print("accounted:", len(seen & src_ids))
print("unaccounted (prose/cross-ref only):", len(unaccounted))
Path("_unaccounted_ids.txt").write_text("\n".join(sorted(unaccounted)), encoding="utf-8")
PY
```
Expected: prints counts. Unaccounted IDs are those appearing ONLY in prose sentences or as cross-references inside other tickets' text (not as their own `<li>` and not named in a prose feature). Review `_unaccounted_ids.txt`: for any that name a real capability, add a `_prose.jsonl` record. Document the residual count in `index.md` under "Completeness & ledger" so the gap is explicit, not hidden.

- [ ] **Step 5: Full test + build + commit (FINAL CHECKPOINT)**

Run:
```bash
cd release-notes && python -m pytest -v
cd /c/opt/learn/spark/notes && zensical build
```
Expected: all tests pass; clean build.
```bash
git add release-notes docs/reference/spark-feature-history zensical.toml
git commit -m "content(catalog): 4.x era + final reconciliation; catalog complete"
```
Then STOP and present the finished catalog for user review.

---

## Self-review notes

- **Spec coverage:** deliverable section (Tasks 6–8), capability taxonomy (Task 4), extraction rules for dumps (Tasks 1–3, 6) and prose (Tasks 9–13), audit ledger (Task 6), no-loss invariant (Tasks 5, 6-Step3, 13-Step4), nav integration (Task 8), progressive build + per-major checkpoints (Tasks 9–13). All spec sections mapped.
- **Deviation from spec:** ledger `.jsonl` files live in `docs/reference/spark-feature-history/` per spec; a third artifact `_prose.jsonl` (not in original spec) is added to hold narrative-only features — needed because 82 releases have no `<li>` dumps. Render merges catalog + prose.
- **Type consistency:** `Item` fields (`spark_id`, `title`, `jira_type`, `disposition`) used consistently across parser/completeness/build. `assign_area(spark_id, title, overrides)` signature stable across Tasks 4/6. `render_timeline`/`replace_auto_block`/`write_pages` stable across Task 7 and Task 9-Step3 extension.
- **Open item resolved:** scripted parse for the 17 dump releases (deterministic); manual prose extraction for the rest, gated by verbatim-quote traceability checks.
