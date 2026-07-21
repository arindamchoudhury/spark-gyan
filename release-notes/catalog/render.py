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
    catalog_path_obj = Path(catalog_path)
    records = [json.loads(l) for l in catalog_path_obj.read_text(encoding="utf-8").splitlines() if l.strip()]

    # Try to load prose records from _prose.jsonl in the same directory
    prose_path = catalog_path_obj.parent / "_prose.jsonl"
    if prose_path.exists():
        prose_records = [json.loads(l) for l in prose_path.read_text(encoding="utf-8").splitlines() if l.strip()]
        records.extend(prose_records)

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
