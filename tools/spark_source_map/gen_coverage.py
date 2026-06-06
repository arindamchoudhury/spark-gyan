#!/usr/bin/env python3
"""Render the spark-source-map landing page (topic coverage + sweep gaps).

Hybrid pipeline — two complementary engines:
  - topic pages  (docs/reference/spark-source-map/topics/*.md)  — topic-first traces
  - sweep pages  (docs/reference/spark-source-map/sweeps/*.md)  — source-first discovery

Coverage matrix comes from topic pages (which topics have been traced).
Gap discovery comes from sweep pages (which source concepts have no topic).

Usage:
    python gen_coverage.py [--root <notes-repo-root>] [--no-write-proposals]
"""
from __future__ import annotations

import argparse
import re
from pathlib import Path

import yaml

TOPIC_RE = re.compile(r"^###\s+(?:✅|⬜)\s+([BIAE]\d+)\s+—\s+(.+?)\s*$", re.MULTILINE)
BOOK_ROW_RE = re.compile(
    r"^\|\s*(✅|⬜)\s*\|\s*(\d+)\s*\|\s*([BIAE]\d+)\s*\|\s*\[([^\]]+)\]\(([^)]+)\)", re.MULTILINE)
FRONT_MATTER_RE = re.compile(r"^---\n(.*?)\n---", re.DOTALL)


def parse_topics(learning_path: Path) -> list[tuple[str, str]]:
    text = learning_path.read_text(encoding="utf-8")
    return [(m.group(1), m.group(2)) for m in TOPIC_RE.finditer(text)]


def parse_book_chapters(book_index: Path) -> dict[str, dict]:
    """topic code -> {chapter, title, link, done}."""
    out: dict[str, dict] = {}
    if not book_index.exists():
        return out
    text = book_index.read_text(encoding="utf-8")
    for m in BOOK_ROW_RE.finditer(text):
        done, chapter, code, title, link = m.groups()
        out[code] = {"chapter": chapter, "title": title, "link": link,
                     "done": done == "✅"}
    return out


def load_catalog_subsystems(catalog: Path) -> dict[str, int]:
    if not catalog.exists():
        return {}
    data = yaml.safe_load(catalog.read_text(encoding="utf-8"))
    counts: dict[str, int] = {}
    for c in data.get("configs", []):
        counts[c["subsystem"]] = counts.get(c["subsystem"], 0) + 1
    return counts


def load_topic_pages(topics_dir: Path) -> dict[str, dict]:
    """Load topic-first trace pages; returns {topic-code -> front matter}."""
    pages: dict[str, dict] = {}
    if not topics_dir.exists():
        return pages
    for path in sorted(topics_dir.glob("*.md")):
        m = FRONT_MATTER_RE.match(path.read_text(encoding="utf-8"))
        if not m:
            continue
        fm = yaml.safe_load(m.group(1)) or {}
        code = fm.get("topic")
        if code:
            fm["_file"] = path.name
            pages[code] = fm
    return pages


def load_sweep_pages(sweeps_dir: Path) -> list[dict]:
    """Load source-sweep pages (subsystem-first discovery)."""
    pages: list[dict] = []
    if not sweeps_dir.exists():
        return pages
    for path in sorted(sweeps_dir.glob("*.md")):
        m = FRONT_MATTER_RE.match(path.read_text(encoding="utf-8"))
        if not m:
            continue
        fm = yaml.safe_load(m.group(1)) or {}
        fm["_file"] = path.name
        pages.append(fm)
    return pages


def collect_proposals(swept: list[dict]) -> list[dict]:
    """Return propose blocks from gap concepts (topics: []) across all sweep pages."""
    proposals: list[dict] = []
    for page in swept:
        sub = page.get("subsystem", "?")
        for concept in page.get("concepts", []) or []:
            ctopics = concept.get("topics", []) or []
            propose = concept.get("propose")
            if not ctopics and propose:
                proposals.append({"subsystem": sub, "concept": concept.get("name", "?"),
                                   **propose})
    return proposals


LEVEL_HEADING_RE = re.compile(r"^## (Beginner|Intermediate|Advanced|Expert)\s*$", re.MULTILINE)
SUGGESTED_RE = re.compile(r"^## Suggested Study Sequence", re.MULTILINE)


def append_proposals_to_learning_path(root: Path, proposals: list[dict]) -> list[str]:
    """Append proposed topic sections to learning-path.md. Returns list of appended codes."""
    lp = root / "docs" / "learning-path.md"
    text = lp.read_text(encoding="utf-8")
    existing_codes = {m.group(1) for m in re.finditer(r"###\s+[✅⬜]\s+([BIAE]\d+)\s+—", text)}
    appended: list[str] = []

    insert_point = SUGGESTED_RE.search(text)
    if not insert_point:
        return appended

    additions: list[tuple[int, str, str]] = []
    for p in proposals:
        code = p.get("code", "")
        if not code or code in existing_codes:
            continue
        level = p.get("level", "Advanced")
        title = p.get("title", code)
        what = p.get("what", "")
        why = p.get("why", "")
        section = (
            f"\n### ⬜ {code} — {title}\n\n"
            f"> Discovered from source sweep: `{p['subsystem']}: {p['concept']}`\n\n"
            f"**What it is:** {what}\n\n"
            f"**Why you need it:** {why}\n\n"
            "**Learn it with:**\n\n"
            "1. **Spark-docs** — see official documentation.\n\n"
            "**Milestone:** TBD\n\n"
            "---\n"
        )
        level_matches = list(LEVEL_HEADING_RE.finditer(text))
        target_pos = insert_point.start()
        for i, lm in enumerate(level_matches):
            if lm.group(1) == level:
                next_pos = (level_matches[i + 1].start()
                            if i + 1 < len(level_matches) else insert_point.start())
                target_pos = min(next_pos, insert_point.start())
                break
        additions.append((target_pos, section, code))
        existing_codes.add(code)

    if additions:
        for pos, section, code in sorted(additions, key=lambda x: -x[0]):
            text = text[:pos] + section + "\n" + text[pos:]
            appended.append(code)
        lp.write_text(text, encoding="utf-8", newline="\n")

    return appended


def build_index(root: Path) -> str:
    base = root / "docs" / "reference" / "spark-source-map"
    topics = parse_topics(root / "docs" / "learning-path.md")
    chapters = parse_book_chapters(root / "docs" / "spark-book" / "index.md")
    sub_counts = load_catalog_subsystems(base / "configs" / "catalog.yaml")
    topic_pages = load_topic_pages(base / "topics")
    swept = load_sweep_pages(base / "sweeps")

    # gaps from sweeps: concepts that map to no topic
    gaps: list[tuple[str, str, dict | None]] = []
    for page in swept:
        sub = page.get("subsystem", "?")
        for concept in page.get("concepts", []) or []:
            if not (concept.get("topics") or []):
                gaps.append((sub, concept.get("name", "?"), concept.get("propose")))

    L: list[str] = []
    L.append("# Spark source map")
    L.append("")
    L.append(
        "> Auto-generated by `tools/spark_source_map/gen_coverage.py`. Do not edit by hand.")
    L.append("")
    L.append(
        "Two complementary engines: **topic traces** (start from a learning-path topic, find "
        "the backing source) and **source sweeps** (scan a subsystem, discover concepts that "
        "aren't in the learning path yet). The [config catalog](configs/index.md) is a "
        "shared lookup for both engines.")
    L.append("")

    # --- topic trace coverage -----------------------------------------------
    L.append("## Topic traces")
    L.append("")
    L.append(
        "One row per learning-path topic. A topic is traced when its page exists under "
        "`topics/`. The Chapter column links to the spark-book chapter.")
    L.append("")
    L.append("| Topic | Title | Chapter | Repos | Status |")
    L.append("|---|---|---|---|---|")
    for code, title in topics:
        ch = chapters.get(code)
        if ch:
            link = ch["link"]
            chap_link = f"../../spark-book/{link}" if not link.startswith("http") else link
            chap = f"[{ch['chapter']}]({chap_link})"
            chap += " ✅" if ch["done"] else " ⬜"
        else:
            chap = "—"
        tp = topic_pages.get(code)
        if tp:
            repos = ", ".join(tp.get("repos") or []) or "—"
            status = tp.get("status", "complete")
            mark = "✅ complete" if status == "complete" else "⬡ partial"
        else:
            repos = "—"
            mark = "⬜"
        L.append(f"| {code} | {title} | {chap} | {repos} | {mark} |")
    L.append("")

    # --- source concept map (from sweeps) -----------------------------------
    L.append("## Source concept map")
    L.append("")
    if swept:
        L.append("```mermaid")
        L.append("flowchart LR")
        for i, page in enumerate(swept):
            sub = page.get("subsystem", "?")
            sid = f"S{i}"
            L.append(f'    {sid}["{sub}"]')
            for j, concept in enumerate(page.get("concepts", []) or []):
                cid = f"{sid}c{j}"
                L.append(f'    {sid} --> {cid}["{concept.get("name", "?")}"]')
        L.append("```")
    else:
        L.append(
            "> No sweeps yet. Run `sweep <subsystem>` (e.g. `core`) to populate this map.")
    L.append("")

    # --- discovery gaps -----------------------------------------------------
    L.append("## Discovery gaps")
    L.append("")
    L.append(
        "Source concepts found during sweeps that don't map to any learning-path topic. "
        "Run `gen_coverage.py` to auto-append proposed stubs to `learning-path.md`.")
    L.append("")
    if gaps:
        L.append("| Concept | Subsystem | Proposed code | Proposed title |")
        L.append("|---|---|---|---|")
        for sub, cname, propose in sorted(gaps, key=lambda x: (x[0], x[1])):
            pcode = propose.get("code", "—") if propose else "—"
            ptitle = propose.get("title", "—") if propose else "—"
            L.append(f"| {cname} | {sub} | {pcode} | {ptitle} |")
        L.append("")
    else:
        L.append("> None yet — appears as sweeps are run.")
    L.append("")

    # --- sweep status -------------------------------------------------------
    # group_meta: (sub, group) -> {status, spark_version, swept_at}
    group_meta: dict[tuple[str, str], dict] = {}
    all_groups_by_sub: dict[str, list[str]] = {}
    # sub_meta: sub -> {status, spark_version, swept_at}  (for non-grouped subs)
    sub_meta: dict[str, dict] = {}
    for page in swept:
        sub = page.get("subsystem", "?")
        group = page.get("group")
        all_groups = page.get("all_groups") or []
        meta = {
            "status": page.get("status", "complete"),
            "spark_version": page.get("spark_version", "—"),
            "swept_at": str(page.get("swept_at", "—")),
        }
        if group:
            group_meta[(sub, group)] = meta
        else:
            sub_meta[sub] = meta
        if all_groups and sub not in all_groups_by_sub:
            all_groups_by_sub[sub] = all_groups
    grouped_subs = set(all_groups_by_sub)
    swept_names = {p.get("subsystem") for p in swept}

    L.append("## Sweep status")
    L.append("")
    L.append(
        "Which subsystems have been swept for source-concept discovery. "
        "Sweep in book-priority order: `sql/catalyst`, `sql/core` first.")
    L.append("")
    L.append("| Subsystem | Configs | Status | Spark version | When |")
    L.append("|---|---|---|---|---|")
    for sub in sorted(sub_counts, key=lambda s: (-sub_counts[s], s)):
        if sub in grouped_subs:
            for i, g in enumerate(all_groups_by_sub[sub]):
                configs_col = str(sub_counts[sub]) if i == 0 else "—"
                if (sub, g) in group_meta:
                    m = group_meta[(sub, g)]
                    st = "✅ " + m["status"]
                    ver = m["spark_version"]
                    when = m["swept_at"]
                else:
                    st, ver, when = "⬜ pending", "—", "—"
                L.append(f"| {sub} — {g} | {configs_col} | {st} | {ver} | {when} |")
        elif sub in swept_names:
            m = sub_meta.get(sub, {})
            st = "✅ " + m.get("status", "complete")
            ver = m.get("spark_version", "—")
            when = m.get("swept_at", "—")
            L.append(f"| {sub} | {sub_counts[sub]} | {st} | {ver} | {when} |")
        else:
            L.append(f"| {sub} | {sub_counts[sub]} | ⬜ pending | — | — |")
    L.append("")

    return "\n".join(L) + "\n"


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Render the spark-source-map landing page.")
    ap.add_argument("--root", default=str(Path(__file__).resolve().parents[2]),
                    help="Notes repo root.")
    ap.add_argument("--no-write-proposals", action="store_true",
                    help="Skip appending proposed topics from sweep gaps to learning-path.md.")
    args = ap.parse_args(argv)
    root = Path(args.root).resolve()

    if not args.no_write_proposals:
        swept = load_sweep_pages(root / "docs" / "reference" / "spark-source-map" / "sweeps")
        proposals = collect_proposals(swept)
        if proposals:
            appended = append_proposals_to_learning_path(root, proposals)
            if appended:
                print(f"Appended {len(appended)} topic(s) to learning-path.md: {', '.join(appended)}")

    out = root / "docs" / "reference" / "spark-source-map" / "index.md"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(build_index(root), encoding="utf-8", newline="\n")
    print(f"wrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
