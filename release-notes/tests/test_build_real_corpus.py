"""Real-corpus regression test: protects the no-loss invariant end-to-end.

Runs the actual build() over the real spark_all_changelogs.txt source (not a
fixture) into a tmp_path output dir, and asserts:
  1. every per-release completeness report is balanced (report["ok"] is True)
  2. the generated _catalog.jsonl / _dropped.jsonl are non-empty
  3. every SPARK-<id> that parse_dump extracted from a dump release's <li>
     items shows up in _catalog.jsonl or _dropped.jsonl (nothing silently lost)
"""
import json
from pathlib import Path

from catalog.build import build
from catalog.parser import iter_releases, has_dump, parse_dump

SOURCE = Path(__file__).resolve().parent.parent / "spark_all_changelogs.txt"


def test_build_real_corpus_no_loss(tmp_path):
    assert SOURCE.exists(), f"real corpus source missing: {SOURCE}"

    reports = build(source_path=SOURCE, out_dir=tmp_path)

    # 1. Every dump release must be balanced (no unaccounted <li> items).
    unbalanced = [r for r in reports if not r["ok"]]
    assert not unbalanced, f"unbalanced dump releases: {unbalanced}"
    assert all(r["ok"] is True for r in reports)

    catalog_path = tmp_path / "_catalog.jsonl"
    dropped_path = tmp_path / "_dropped.jsonl"

    catalog_lines = [l for l in catalog_path.read_text(encoding="utf-8").splitlines() if l.strip()]
    dropped_lines = [l for l in dropped_path.read_text(encoding="utf-8").splitlines() if l.strip()]

    # 2. Non-trivial output.
    assert len(catalog_lines) > 0
    assert len(dropped_lines) > 0

    catalog_ids = {json.loads(l)["spark_id"] for l in catalog_lines if json.loads(l)["spark_id"]}
    dropped_ids = {json.loads(l)["spark_id"] for l in dropped_lines if json.loads(l)["spark_id"]}
    known_ids = catalog_ids | dropped_ids

    # 3. Reconcile: every SPARK-<id> parsed from a dump release's <li> items
    # must appear in either the kept catalog or the dropped file.
    text = SOURCE.read_text(encoding="utf-8", errors="replace")
    missing = []
    for slug, block in iter_releases(text):
        if not has_dump(block):
            continue
        for item in parse_dump(block):
            if item.spark_id and item.spark_id not in known_ids:
                missing.append((slug, item.spark_id))

    assert not missing, f"SPARK ids parsed from dumps but missing from catalog+dropped: {missing}"
