#!/usr/bin/env python3
"""Render the spark-source-map landing page (concept map + coverage matrix).

Engine-3 of the pipeline: deterministic assembly of ``index.md`` from
  - learning-path.md       (the 40 topics, ordered)
  - spark-book/index.md    (topic -> chapter mapping)
  - configs/catalog.yaml    (subsystems + per-subsystem config counts)
  - subsystems/*.md          (front matter: which concepts back which topics)

The traced subsystem pages are LLM-authored, but this matrix is recomputed
from their structured front matter, so coverage is never hand-maintained.

Subsystem page front matter contract:

    ---
    subsystem: sql/core
    spark_version: "5.0.0-SNAPSHOT"
    status: complete            # or: partial
    concepts:
      - name: joins
        topics: [B7, A3]         # learning-path codes this concept backs
      - name: vectorized-reader
        topics: []                # empty => discovery gap (no topic)
    ---

Usage:
    python gen_coverage.py --root <notes-repo-root>
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


def load_traced(subsystems_dir: Path) -> list[dict]:
    pages: list[dict] = []
    if not subsystems_dir.exists():
        return pages
    for path in sorted(subsystems_dir.glob("*.md")):
        m = FRONT_MATTER_RE.match(path.read_text(encoding="utf-8"))
        if not m:
            continue
        fm = yaml.safe_load(m.group(1)) or {}
        fm["_file"] = path.name
        pages.append(fm)
    return pages


def collect_proposals(traced: list[dict]) -> list[dict]:
    """Return propose blocks from gap concepts (topics: []) across all traced pages."""
    proposals: list[dict] = []
    for page in traced:
        sub = page.get("subsystem", "?")
        for concept in page.get("concepts", []) or []:
            ctopics = concept.get("topics", []) or []
            propose = concept.get("propose")
            if not ctopics and propose:
                proposals.append({"subsystem": sub, "concept": concept.get("name", "?"),
                                   **propose})
    return proposals


LEVEL_ORDER = {"Beginner": 0, "Intermediate": 1, "Advanced": 2, "Expert": 3}
LEVEL_HEADING_RE = re.compile(r"^## (Beginner|Intermediate|Advanced|Expert)\s*$", re.MULTILINE)
SUGGESTED_RE = re.compile(r"^## Suggested Study Sequence", re.MULTILINE)


def append_proposals_to_learning_path(root: Path, proposals: list[dict]) -> list[str]:
    """Append proposed topic sections to learning-path.md. Returns list of appended codes."""
    lp = root / "docs" / "learning-path.md"
    text = lp.read_text(encoding="utf-8")
    existing_codes = {m.group(1) for m in re.finditer(r"###\s+[✅⬜]\s+([BIAE]\d+)\s+—", text)}
    appended: list[str] = []

    # Group proposals by level; insert each before "## Suggested Study Sequence"
    insert_point = SUGGESTED_RE.search(text)
    if not insert_point:
        return appended

    additions: list[str] = []
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
            f"> Discovered from source trace: `{p['subsystem']}: {p['concept']}`\n\n"
            f"**What it is:** {what}\n\n"
            f"**Why you need it:** {why}\n\n"
            "**Learn it with:**\n\n"
            "1. **Spark-docs** — see official documentation.\n\n"
            "**Milestone:** TBD\n\n"
            "---\n"
        )
        # Find the right level section to append to
        level_matches = list(LEVEL_HEADING_RE.finditer(text))
        target_pos = insert_point.start()
        for i, m in enumerate(level_matches):
            if m.group(1) == level:
                next_pos = level_matches[i + 1].start() if i + 1 < len(level_matches) else insert_point.start()
                target_pos = min(next_pos, insert_point.start())
                break
        additions.append((target_pos, section, code))
        existing_codes.add(code)

    if additions:
        # Insert in reverse order so positions stay valid
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
    traced = load_traced(base / "subsystems")

    # topic code -> list of "subsystem: concept" that back it
    backing: dict[str, list[str]] = {code: [] for code, _ in topics}
    # gaps: (subsystem, concept_name, propose_dict_or_None)
    gaps: list[tuple[str, str, dict | None]] = []
    traced_names = set()
    for page in traced:
        sub = page.get("subsystem", "?")
        traced_names.add(sub)
        for concept in page.get("concepts", []) or []:
            cname = concept.get("name", "?")
            ctopics = concept.get("topics", []) or []
            if not ctopics:
                gaps.append((sub, cname, concept.get("propose")))
            for tc in ctopics:
                backing.setdefault(tc, []).append(f"{sub}: {cname}")

    L: list[str] = []
    L.append("# Spark source map")
    L.append("")
    L.append(
        "> Auto-generated by `tools/spark_source_map/gen_coverage.py`. It reconciles the "
        "[configuration catalog](configs/index.md) and the traced subsystem maps against the "
        "book's [40-topic learning path](../../learning-path.md). Do not edit by hand.")
    L.append("")
    L.append(
        "This is the map+discover view: which parts of the Apache Spark source back each "
        "learning-path topic, and which source concepts are **not yet** covered by any topic "
        "(discovery gaps). Full code-path traces live under `subsystems/`.")
    L.append("")

    # --- concept map -------------------------------------------------------
    L.append("## Concept map")
    L.append("")
    if traced:
        L.append("```mermaid")
        L.append("flowchart LR")
        for i, page in enumerate(traced):
            sub = page.get("subsystem", "?")
            sid = f"S{i}"
            L.append(f'    {sid}["{sub}"]')
            for j, concept in enumerate(page.get("concepts", []) or []):
                cid = f"{sid}c{j}"
                L.append(f'    {sid} --> {cid}["{concept.get("name", "?")}"]')
        L.append("```")
    else:
        L.append(
            "> No subsystems traced yet. Run `trace <subsystem>` (e.g. `sql/core`) to populate "
            "this map. The diagram renders once the first subsystem page exists.")
    L.append("")

    # --- coverage matrix ---------------------------------------------------
    L.append("## Topic coverage")
    L.append("")
    L.append("Each learning-path topic and the traced source concepts that back it.")
    L.append("")
    L.append("| Topic | Title | Chapter | Backed by (subsystem: concept) | Traced |")
    L.append("|---|---|---|---|---|")
    for code, title in topics:
        ch = chapters.get(code)
        if ch:
            chap = f"[{ch['chapter']}]({('../../spark-book/' + ch['link']) if not ch['link'].startswith('http') else ch['link']})"
            chap += " ✅" if ch["done"] else " ⬜"
        else:
            chap = "—"
        back = backing.get(code) or []
        back_txt = "; ".join(back) if back else "—"
        mark = "✅" if back else "⬜"
        L.append(f"| {code} | {title} | {chap} | {back_txt} | {mark} |")
    L.append("")

    # --- discovery gaps ----------------------------------------------------
    L.append("## Discovery gaps — source concepts mapping to no topic")
    L.append("")
    if gaps:
        L.append("| Concept | Subsystem | Proposed code | Proposed title |")
        L.append("|---|---|---|---|")
        for sub, cname, propose in sorted(gaps, key=lambda x: (x[0], x[1])):
            code = propose.get("code", "—") if propose else "—"
            title = propose.get("title", "—") if propose else "—"
            L.append(f"| {cname} | {sub} | {code} | {title} |")
        L.append("")
        proposals_with_detail = [(sub, cname, p) for sub, cname, p in gaps if p]
        if proposals_with_detail:
            L.append("> Run `python tools/spark_source_map/gen_coverage.py --write-proposals`"
                     " to append the proposed topics to `learning-path.md`.")
    else:
        L.append("> None recorded yet (appears as subsystems are traced).")
    L.append("")

    # --- subsystem tracing status -----------------------------------------
    # Collect group info: subsystems traced in named groups declare group + all_groups.
    group_pages: dict[tuple[str, str], str] = {}   # (sub, group) -> status
    all_groups_by_sub: dict[str, list[str]] = {}   # sub -> ordered group list
    for page in traced:
        sub = page.get("subsystem", "?")
        group = page.get("group")
        all_groups = page.get("all_groups") or []
        if group:
            group_pages[(sub, group)] = page.get("status", "complete")
        if all_groups and sub not in all_groups_by_sub:
            all_groups_by_sub[sub] = all_groups
    grouped_subs = set(all_groups_by_sub)

    status_by_sub = {p.get("subsystem"): p.get("status", "complete") for p in traced}

    L.append("## Subsystem tracing status")
    L.append("")
    L.append(
        "Subsystems are listed with their config count (from the catalog) and trace status. "
        "Trace in book-priority order: `sql/catalyst`, `sql/core` first.")
    L.append("")
    L.append("| Subsystem | Configs | Traced |")
    L.append("|---|---|---|")
    for sub in sorted(sub_counts, key=lambda s: (-sub_counts[s], s)):
        if sub in grouped_subs:
            for i, g in enumerate(all_groups_by_sub[sub]):
                configs_col = str(sub_counts[sub]) if i == 0 else "—"
                if (sub, g) in group_pages:
                    st = "✅ " + group_pages[(sub, g)]
                else:
                    st = "⬜ pending"
                L.append(f"| {sub} — {g} | {configs_col} | {st} |")
        elif sub in traced_names:
            L.append(f"| {sub} | {sub_counts[sub]} | ✅ {status_by_sub.get(sub, 'complete')} |")
        else:
            L.append(f"| {sub} | {sub_counts[sub]} | ⬜ pending |")
    L.append("")

    return "\n".join(L) + "\n"


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Render the spark-source-map landing page.")
    ap.add_argument("--root", default=str(Path(__file__).resolve().parents[2]),
                    help="Notes repo root.")
    ap.add_argument("--no-write-proposals", action="store_true",
                    help="Skip appending proposed topics from gap concepts to learning-path.md.")
    args = ap.parse_args(argv)
    root = Path(args.root).resolve()

    if not args.no_write_proposals:
        traced = load_traced(
            root / "docs" / "reference" / "spark-source-map" / "subsystems")
        proposals = collect_proposals(traced)
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
