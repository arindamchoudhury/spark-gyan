"""Tests for gen_coverage.py's learning-path proposal writer.

The ordering test is a regression guard: proposals from one sweep usually share
a level, and so share an insertion offset. Inserting them one at a time at that
offset stacked them in reverse, and the sql/catalyst optimizer sweep appended
A19, A18, A17 to a file whose every other block ascends.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from gen_coverage import (  # noqa: E402
    append_proposals_to_learning_path,
    code_sort_key,
    collect_proposals,
)

LEARNING_PATH = """# Learning path

## Beginner

### ⬜ B1 — First topic

**What it is:** something.

---

## Advanced

### ⬜ A1 — An advanced topic

**What it is:** something.

---

### 🔄 A2 — A revisited topic

**What it is:** something.

---

## Expert

### ⬜ E1 — An expert topic

**What it is:** something.

---

## Suggested Study Sequence

B1 → A1 → E1
"""


def make_root(tmp_path: Path, text: str = LEARNING_PATH) -> Path:
    docs = tmp_path / "docs"
    docs.mkdir()
    (docs / "learning-path.md").write_text(text, encoding="utf-8")
    return tmp_path


def proposal(code: str, level: str = "Advanced") -> dict:
    return {
        "subsystem": "sql/catalyst",
        "concept": f"concept for {code}",
        "gap": True,
        "code": code,
        "level": level,
        "title": f"Title {code}",
        "what": "What it is.",
        "why": "Why you need it.",
    }


def codes_in_order(text: str) -> list[str]:
    return re.findall(r"^###\s+\S+\s+([BIAE]\d+)\s+—", text, re.MULTILINE)


def test_code_sort_key_orders_by_level_then_number():
    codes = ["E1", "A17", "B10", "I2", "A9", "B2"]
    assert sorted(codes, key=code_sort_key) == ["B2", "B10", "I2", "A9", "A17", "E1"]


def test_proposals_at_one_level_are_appended_in_ascending_order(tmp_path):
    root = make_root(tmp_path)
    appended = append_proposals_to_learning_path(
        root, [proposal("A17"), proposal("A18"), proposal("A19")])

    assert appended == ["A17", "A18", "A19"]
    text = (root / "docs" / "learning-path.md").read_text(encoding="utf-8")
    assert codes_in_order(text) == ["B1", "A1", "A2", "A17", "A18", "A19", "E1"]


def test_declaration_order_does_not_change_written_order(tmp_path):
    """A sweep lists concepts in reading order, not code order."""
    root = make_root(tmp_path)
    append_proposals_to_learning_path(
        root, [proposal("A19"), proposal("A17"), proposal("A18")])

    text = (root / "docs" / "learning-path.md").read_text(encoding="utf-8")
    assert codes_in_order(text) == ["B1", "A1", "A2", "A17", "A18", "A19", "E1"]


def test_proposals_land_under_their_own_level(tmp_path):
    root = make_root(tmp_path)
    append_proposals_to_learning_path(
        root, [proposal("A17"), proposal("B2", level="Beginner"),
               proposal("E2", level="Expert")])

    text = (root / "docs" / "learning-path.md").read_text(encoding="utf-8")
    assert codes_in_order(text) == ["B1", "B2", "A1", "A2", "A17", "E1", "E2"]


def test_existing_codes_are_never_duplicated(tmp_path):
    """A 🔄 or ✅ topic still exists -- reusing its code would fork the topic."""
    root = make_root(tmp_path)
    appended = append_proposals_to_learning_path(
        root, [proposal("A1"), proposal("A2"), proposal("A17")])

    assert appended == ["A17"]
    text = (root / "docs" / "learning-path.md").read_text(encoding="utf-8")
    assert codes_in_order(text).count("A2") == 1
    assert "🔄 A2" in text


def test_repeated_run_is_idempotent(tmp_path):
    root = make_root(tmp_path)
    props = [proposal("A17"), proposal("A18")]
    append_proposals_to_learning_path(root, props)
    first = (root / "docs" / "learning-path.md").read_text(encoding="utf-8")

    assert append_proposals_to_learning_path(root, props) == []
    assert (root / "docs" / "learning-path.md").read_text(encoding="utf-8") == first


def test_collect_proposals_reads_gap_and_refinement_blocks():
    pages = [{
        "subsystem": "sql/catalyst",
        "concepts": [
            {"name": "covered", "topics": ["A1"]},
            {"name": "gap", "topics": [],
             "propose": {"code": "A17", "level": "Advanced", "title": "T"}},
            {"name": "refinement", "topics": ["A3"],
             "propose": {"code": "A18", "level": "Advanced", "title": "U"}},
        ],
    }]
    got = collect_proposals(pages)

    assert [p["code"] for p in got] == ["A17", "A18"]
    assert [p["gap"] for p in got] == [True, False]
    assert got[0]["concept"] == "gap"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
