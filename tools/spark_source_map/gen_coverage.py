#!/usr/bin/env python3
"""Render the spark-source-map landing page (topic coverage + sweep gaps).

Hybrid pipeline — two complementary engines:
  - topic pages  (docs/reference/spark-source-map/topics/*.md)  — topic-first traces
  - sweep pages  (docs/reference/spark-source-map/sweeps/*.md)  — source-first discovery

Coverage matrix comes from topic pages (which topics have been traced).
Gap discovery comes from sweep pages (which source concepts have no topic).

Usage:
    python gen_coverage.py [--root <notes-repo-root>] [--no-write-proposals]

The learning path is docs/learning-path-v2.md; docs/learning-path.md is the frozen v1
and is never read or written here.
"""
from __future__ import annotations

import argparse
import re
from pathlib import Path

import yaml

# Status markers used in learning-path-v2.md headings and spark-book/index.md rows.
# 🔄 means written but needing revisiting against a newer Spark; it must parse as
# a real topic/chapter, otherwise those rows silently vanish from the matrix.
STATUS_MARK = "✅|⬜|🔄"
TOPIC_RE = re.compile(rf"^####\s+(?:{STATUS_MARK})\s+([BIAE]\d+)\s+—\s+(.+?)\s*$", re.MULTILINE)
BOOK_ROW_RE = re.compile(
    rf"^\|\s*({STATUS_MARK})\s*\|\s*(\d+)\s*\|\s*([BIAE]\d+)\s*\|\s*\[([^\]]+)\]\(([^)]+)\)",
    re.MULTILINE)
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
                     "done": done in ("✅", "🔄"), "mark": done}
    return out


def load_catalog_subsystems(catalog: Path) -> dict[str, int]:
    if not catalog.exists():
        return {}
    data = yaml.safe_load(catalog.read_text(encoding="utf-8"))
    counts: dict[str, int] = {}
    for c in data.get("configs", []):
        counts[c["subsystem"]] = counts.get(c["subsystem"], 0) + 1
    return counts


def load_group_subsystems(groups_file: Path) -> dict[str, list[str]]:
    """Sweepable subsystems from groups.yaml -> their group names.

    Some subsystems declare no configs of their own (Spark puts nearly every SQL
    config in sql/catalyst's SQLConf.scala), so the catalog alone cannot list
    them — sql/core and sql/pipelines among them. They are still sweepable.
    """
    if not groups_file.exists():
        return {}
    data = yaml.safe_load(groups_file.read_text(encoding="utf-8")) or {}
    # Keys starting with "_" are file metadata (_meta: spark_version, verified_at),
    # not subsystems; without this they render as phantom rows in the sweep table.
    return {sub: [g["name"] for g in groups]
            for sub, groups in data.items() if not sub.startswith("_")}


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
    """Return all propose blocks from sweep pages.

    Includes both pure gaps (topics: []) and refinement proposals (topics: [...] + propose:).
    Each proposal dict carries a 'gap' bool: True = no topic covers it, False = refinement.
    """
    proposals: list[dict] = []
    for page in swept:
        sub = page.get("subsystem", "?")
        for concept in page.get("concepts", []) or []:
            ctopics = concept.get("topics", []) or []
            propose = concept.get("propose")
            if propose:
                proposals.append({
                    "subsystem": sub,
                    "concept": concept.get("name", "?"),
                    "gap": not ctopics,
                    **propose,
                })
    return proposals


LEVEL_HEADING_RE = re.compile(r"^## (Beginner|Intermediate|Advanced|Expert)\s*$", re.MULTILINE)
SUGGESTED_RE = re.compile(r"^## Suggested study sequence", re.MULTILINE | re.IGNORECASE)
# v2 closes each level with a checkpoint; a new topic belongs before that gate.
CHECKPOINT_RE = re.compile(r"^### 🎯 (Beginner|Intermediate|Advanced|Expert) Checkpoint\s*$",
                           re.MULTILINE)
CODE_RE = re.compile(r"([BIAE])(\d+)$")


def code_sort_key(code: str) -> tuple[int, int]:
    """Sort topic codes the way the learning path reads them: B1 < B10 < I1 < A1 < E1."""
    m = CODE_RE.match(code or "")
    if not m:
        return (9, 0)
    return ("BIAE".index(m.group(1)), int(m.group(2)))


def next_free_code(level: str, existing: set[str]) -> str:
    """Lowest unused code in a level, e.g. 'A61' when A1..A60 exist."""
    letter = {"Beginner": "B", "Intermediate": "I", "Advanced": "A", "Expert": "E"}.get(level, "A")
    n = 1
    while f"{letter}{n}" in existing:
        n += 1
    return f"{letter}{n}"


def append_proposals_to_learning_path(root: Path, proposals: list[dict]) -> list[str]:
    """Append proposed topic sections to learning-path-v2.md. Returns appended codes.

    v1 (docs/learning-path.md) is frozen and is never read or written here.
    """
    lp = root / "docs" / "learning-path-v2.md"
    text = lp.read_text(encoding="utf-8")
    # Must accept every status marker, or a 🔄 topic reads as non-existent and a
    # sweep proposal reuses its code, producing two topics with the same code.
    existing_codes = {m.group(1)
                      for m in re.finditer(rf"####\s+(?:{STATUS_MARK})\s+([BIAE]\d+)\s+—", text)}
    # Identity is the sweep concept, not the code: a proposal whose code was taken gets
    # reallocated below, so keying dedup on the code would append it again -- with a new
    # code -- on every run. The generated section carries `subsystem: concept` verbatim,
    # and it is matched as a literal: a regex over backticked spans pairs the closing
    # backtick of one section with the opening backtick of the next and loses concepts.
    def concept_key(prop: dict) -> str:
        return f"`{prop['subsystem']}: {prop['concept']}`"
    appended: list[str] = []

    insert_point = SUGGESTED_RE.search(text)
    if not insert_point:
        return appended

    additions: list[tuple[int, str, str]] = []
    pending_concepts: set[str] = set()
    for p in proposals:
        level = p.get("level", "Advanced")
        if concept_key(p) in text or concept_key(p) in pending_concepts:
            continue
        code = p.get("code", "")
        # v2 renumbered every topic, so a proposal authored against v1 numbering can
        # name a code that now belongs to something unrelated. Never overwrite and
        # never silently drop: take the next free code in the proposed level.
        if not code or code in existing_codes:
            code = next_free_code(level, existing_codes)
        title = p.get("title", code)
        what = p.get("what", "")
        why = p.get("why", "")
        kind = "new topic" if p.get("gap") else "refinement"
        section = (
            f"\n#### ⬜ {code} — {title}\n\n"
            f"**New topic** · proposed by a source sweep ({kind}): "
            f"`{p['subsystem']}: {p['concept']}` — **unreviewed**, generated by "
            f"`gen_coverage.py`. Rewrite it to this page's standard before relying on it: "
            f"verify every claim against the source checkout, then replace this line with "
            f"what the sweep actually found.\n\n"
            f"**What** — {what}\n\n"
            f"**Why** — {why}\n\n"
            "**Learn** — TBD · docs: TBD · feature history: TBD · source: the sweep page "
            "this came from\n\n"
            "**Milestone** — TBD\n"
        )
        # Prefer the level's checkpoint: a new topic belongs inside its level, ahead of
        # the gate that closes it. Fall back to the next level heading, then to the
        # study-sequence section at the end.
        target_pos = insert_point.start()
        checkpoints = {m.group(1): m.start() for m in CHECKPOINT_RE.finditer(text)}
        if level in checkpoints:
            target_pos = checkpoints[level]
        else:
            level_matches = list(LEVEL_HEADING_RE.finditer(text))
            for i, lm in enumerate(level_matches):
                if lm.group(1) == level:
                    next_pos = (level_matches[i + 1].start()
                                if i + 1 < len(level_matches) else insert_point.start())
                    target_pos = min(next_pos, insert_point.start())
                    break
        additions.append((target_pos, section, code))
        existing_codes.add(code)
        # Later proposals in this same run must see it too, before anything is written.
        pending_concepts.add(concept_key(p))

    if additions:
        # Proposals from one sweep usually share a level, and so share an offset.
        # Inserting them one at a time at that offset stacks them in reverse --
        # each new section lands ahead of the one before it -- which produced
        # A19, A18, A17 in a file whose every other block ascends. Group by
        # offset and write each group as a single block in code order.
        by_pos: dict[int, list[tuple[str, str]]] = {}
        for pos, section, code in additions:
            by_pos.setdefault(pos, []).append((code, section))
        for pos in sorted(by_pos, reverse=True):
            group = sorted(by_pos[pos], key=lambda cs: code_sort_key(cs[0]))
            block = "".join(section + "\n" for _, section in group)
            text = text[:pos] + block + text[pos:]
            appended.extend(code for code, _ in group)
        lp.write_text(text, encoding="utf-8", newline="\n")

    appended.sort(key=code_sort_key)
    return appended


def build_index(root: Path) -> str:
    base = root / "docs" / "reference" / "spark-source-map"
    topics = parse_topics(root / "docs" / "learning-path-v2.md")
    chapters = parse_book_chapters(root / "docs" / "spark-book" / "index.md")
    sub_counts = load_catalog_subsystems(base / "configs" / "catalog.yaml")
    group_subs = load_group_subsystems(base / "groups.yaml")
    topic_pages = load_topic_pages(base / "topics")
    swept = load_sweep_pages(base / "sweeps")

    # proposals from sweeps: gaps (topics: []) and refinements (topics: [...] + propose:)
    gaps: list[tuple[str, str, dict | None, bool]] = []
    for page in swept:
        sub = page.get("subsystem", "?")
        for concept in page.get("concepts", []) or []:
            ctopics = concept.get("topics") or []
            propose = concept.get("propose")
            if not ctopics or propose:
                gaps.append((sub, concept.get("name", "?"), propose, not ctopics))

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
            chap += f" {ch.get('mark', '✅' if ch['done'] else '⬜')}"
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
    L.append("## Topics discovered from the source")
    L.append("")
    L.append(
        "Source-first sweeps discover concepts independently of the learning path; these are "
        "the ones it did not already cover. **New** = no topic covered the concept at all. "
        "**Refinement** = a broader topic touched it, but it warrants its own. Both are "
        "auto-appended to `learning-path-v2.md` when `gen_coverage.py` runs — growing the path "
        "is the point of sweeping, not a side effect.")
    L.append("")
    if gaps:
        L.append("| Concept | Subsystem | Kind | Proposed code | Proposed title |")
        L.append("|---|---|---|---|---|")
        for sub, cname, propose, is_gap in sorted(gaps, key=lambda x: (x[0], x[1])):
            pcode = propose.get("code", "—") if propose else "—"
            ptitle = propose.get("title", "—") if propose else "—"
            kind = "new" if is_gap else "refinement"
            L.append(f"| {cname} | {sub} | {kind} | {pcode} | {ptitle} |")
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
        "Order by discovery yield — densest unexplored subsystems first "
        "(`sql/catalyst`, `sql/core`), not by what the book needs next.")
    L.append("")
    L.append("| Subsystem | Configs | Status | Spark version | When |")
    L.append("|---|---|---|---|---|")
    # Union with groups.yaml so config-free but sweepable subsystems still get a
    # row; they sort last, since the sort key is config count.
    all_subs = set(sub_counts) | set(group_subs)
    for sub in sorted(all_subs, key=lambda s: (-sub_counts.get(s, 0), s)):
        count_col = str(sub_counts[sub]) if sub in sub_counts else "—"
        # groups.yaml is authoritative for which groups exist. A sweep page's
        # all_groups is only a fallback for a subsystem groups.yaml does not
        # list -- relying on it meant that adding a group to an already-swept
        # subsystem silently dropped it from this table, with no row at all.
        rows = group_subs.get(sub) or all_groups_by_sub.get(sub)
        if rows:
            for i, g in enumerate(rows):
                configs_col = count_col if i == 0 else "—"
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
            L.append(f"| {sub} | {count_col} | {st} | {ver} | {when} |")
        else:
            L.append(f"| {sub} | {count_col} | ⬜ pending | — | — |")
    L.append("")

    return "\n".join(L) + "\n"


def list_groups(root: Path, subsystem: str | None) -> int:
    """Print sweepable groups and whether each has been swept.

    A sweep covers one group per run, so this answers the question you have
    before starting one: which groups exist here, and which are still open.
    """
    base = root / "docs" / "reference" / "spark-source-map"
    groups_file = base / "groups.yaml"
    if not groups_file.exists():
        print(f"error: {groups_file} not found")
        return 1
    data = yaml.safe_load(groups_file.read_text(encoding="utf-8")) or {}
    subs = {k: v for k, v in data.items() if not k.startswith("_")}

    if subsystem and subsystem not in subs:
        print(f"error: no such subsystem '{subsystem}'. Known: {', '.join(sorted(subs))}")
        return 1

    # (subsystem, group) -> the page that swept it
    done: dict[tuple[str, str], dict] = {}
    for page in load_sweep_pages(base / "sweeps"):
        if page.get("group"):
            done[(page.get("subsystem", "?"), page["group"])] = page

    for sub in ([subsystem] if subsystem else sorted(subs)):
        print(f"\n{sub}")
        for g in subs[sub] or []:
            name = g.get("name", "?")
            page = done.get((sub, name))
            if page:
                mark = f"[swept: {page.get('status', 'complete')}, " \
                       f"Spark {page.get('spark_version', '?')}, {page['_file']}]"
            else:
                mark = "[not swept]"
            topics = ", ".join(g.get("topics") or []) or "none"
            print(f"  {name:<24} {mark}")
            print(f"      topics: {topics}")
            print(f"      scope:  {g.get('scope', '')}")
    print("\nSweep one group per run:  sweep <subsystem> <group>")
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Render the spark-source-map landing page.")
    ap.add_argument("--root", default=str(Path(__file__).resolve().parents[2]),
                    help="Notes repo root.")
    ap.add_argument("--no-write-proposals", action="store_true",
                    help="Skip appending proposed topics from sweep gaps to learning-path-v2.md.")
    ap.add_argument("--list-groups", nargs="?", const="", metavar="SUBSYSTEM",
                    help="List sweepable groups (all subsystems, or just one) and exit. "
                         "Writes nothing.")
    args = ap.parse_args(argv)
    root = Path(args.root).resolve()

    if args.list_groups is not None:
        return list_groups(root, args.list_groups or None)

    if not args.no_write_proposals:
        swept = load_sweep_pages(root / "docs" / "reference" / "spark-source-map" / "sweeps")
        proposals = collect_proposals(swept)
        if proposals:
            appended = append_proposals_to_learning_path(root, proposals)
            if appended:
                print(f"Appended {len(appended)} topic(s) to learning-path-v2.md: {', '.join(appended)}")

    out = root / "docs" / "reference" / "spark-source-map" / "index.md"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(build_index(root), encoding="utf-8", newline="\n")
    print(f"wrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
