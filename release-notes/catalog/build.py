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
