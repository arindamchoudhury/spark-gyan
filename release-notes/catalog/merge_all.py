"""Merge the two source ledgers into one complete feature list: _all.jsonl.

_catalog.jsonl (dump-derived) + _prose.jsonl (highlights/prose-derived) ->
_all.jsonl, one record per tracked feature/improvement across every release,
sorted oldest->newest. Each record is tagged with its `source` ("dump" or
"prose"). The two source ledgers remain the audit artifacts; this is the
single unified view (spans every release, including prose-only ones like
4.2.0 that never appear in _catalog.jsonl).
"""
import json
import re
from pathlib import Path

HERE = Path(__file__).resolve().parent
OUT_DIR = HERE.parent.parent / "docs" / "reference" / "spark-feature-history"


def _ver_key(release: str):
    parts = [int(p) if p.isdigit() else 0 for p in release.split(".")]
    while len(parts) < 3:
        parts.append(0)
    return tuple(parts)


def _load(path: Path, source: str) -> list[dict]:
    out = []
    if not path.exists():
        return out
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        r = json.loads(line)
        out.append({
            "spark_id": r.get("spark_id"),
            "release": r["release"],
            "area": r["area"],
            "type": r["type"],
            "title": r["title"],
            "source": source,
        })
    return out


def merge(out_dir: Path = OUT_DIR) -> int:
    records = _load(out_dir / "_catalog.jsonl", "dump") + _load(out_dir / "_prose.jsonl", "prose")
    records.sort(key=lambda r: (_ver_key(r["release"]),
                                int(re.sub(r"\D", "", r["spark_id"] or "0") or 0)))
    with (out_dir / "_all.jsonl").open("w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")
    return len(records)


if __name__ == "__main__":
    n = merge()
    print(f"_all.jsonl written: {n} records")
