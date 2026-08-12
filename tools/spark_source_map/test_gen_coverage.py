"""Tests for gen_coverage.py's learning-path proposal writer.

The ordering test is a regression guard: proposals from one sweep usually share
a level, and so share an insertion offset. Inserting them one at a time at that
offset stacked them in reverse, and the sql/catalyst optimizer sweep appended
A19, A18, A17 to a file whose every other block ascends.

The fixture mirrors learning-path-v2.md: `####` topic headings, a `🎯` checkpoint
closing each level, and a lowercase study-sequence section. v1 (learning-path.md)
is frozen and the writer never touches it.
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
    next_free_code,
)

LEARNING_PATH = """# Learning path v2

## Beginner

#### ⬜ B1 — First topic

**What** — something.

### 🎯 Beginner Checkpoint

Prove you can do the thing.

---

## Advanced

#### ⬜ A1 — An advanced topic

**What** — something.

#### 🔄 A2 — A revisited topic

**What** — something.

### 🎯 Advanced Checkpoint

Prove you can do the harder thing.

---

## Expert

#### ⬜ E1 — An expert topic

**What** — something.

### 🎯 Expert Checkpoint

Prove you can run it.

---

## Suggested study sequence

B1 → A1 → E1
"""

PATH_NAME = "learning-path-v2.md"


def make_root(tmp_path: Path, text: str = LEARNING_PATH) -> Path:
    docs = tmp_path / "docs"
    docs.mkdir()
    (docs / PATH_NAME).write_text(text, encoding="utf-8")
    # v1 sits beside it and must never be written to.
    (docs / "learning-path.md").write_text("# frozen v1\n", encoding="utf-8")
    return tmp_path


def read_path(root: Path) -> str:
    return (root / "docs" / PATH_NAME).read_text(encoding="utf-8")


def proposal(code: str, level: str = "Advanced", concept: str | None = None) -> dict:
    return {
        "subsystem": "sql/catalyst",
        "concept": concept or f"concept for {code}",
        "gap": True,
        "code": code,
        "level": level,
        "title": f"Title {code}",
        "what": "What it is.",
        "why": "Why you need it.",
    }


def codes_in_order(text: str) -> list[str]:
    return re.findall(r"^####\s+\S+\s+([BIAE]\d+)\s+—", text, re.MULTILINE)


def test_code_sort_key_orders_by_level_then_number():
    codes = ["E1", "A17", "B10", "I2", "A9", "B2"]
    assert sorted(codes, key=code_sort_key) == ["B2", "B10", "I2", "A9", "A17", "E1"]


def test_next_free_code_skips_taken_numbers():
    assert next_free_code("Advanced", {"A1", "A2", "A3"}) == "A4"
    assert next_free_code("Beginner", set()) == "B1"
    assert next_free_code("Intermediate", {"I1", "I3"}) == "I2"


def test_proposals_at_one_level_are_appended_in_ascending_order(tmp_path):
    root = make_root(tmp_path)
    appended = append_proposals_to_learning_path(
        root, [proposal("A17"), proposal("A18"), proposal("A19")])

    assert appended == ["A17", "A18", "A19"]
    assert codes_in_order(read_path(root)) == ["B1", "A1", "A2", "A17", "A18", "A19", "E1"]


def test_declaration_order_does_not_change_written_order(tmp_path):
    """A sweep lists concepts in reading order, not code order."""
    root = make_root(tmp_path)
    append_proposals_to_learning_path(
        root, [proposal("A19"), proposal("A17"), proposal("A18")])

    assert codes_in_order(read_path(root)) == ["B1", "A1", "A2", "A17", "A18", "A19", "E1"]


def test_proposals_land_under_their_own_level(tmp_path):
    root = make_root(tmp_path)
    append_proposals_to_learning_path(
        root, [proposal("A17"), proposal("B2", level="Beginner"),
               proposal("E2", level="Expert")])

    assert codes_in_order(read_path(root)) == ["B1", "B2", "A1", "A2", "A17", "E1", "E2"]


def test_proposals_land_before_their_level_checkpoint(tmp_path):
    """A checkpoint is the gate that closes a level; a new topic belongs ahead of it."""
    root = make_root(tmp_path)
    append_proposals_to_learning_path(root, [proposal("A17")])

    text = read_path(root)
    assert text.index("#### ⬜ A17 —") < text.index("### 🎯 Advanced Checkpoint")


def test_taken_code_is_reallocated_not_dropped(tmp_path):
    """v2 renumbered every topic, so a proposal can name a code that now belongs
    to something unrelated. Reallocate; never overwrite and never silently drop."""
    root = make_root(tmp_path)
    appended = append_proposals_to_learning_path(root, [proposal("A1"), proposal("A2")])

    assert appended == ["A3", "A4"]
    text = read_path(root)
    assert codes_in_order(text).count("A1") == 1
    assert codes_in_order(text).count("A2") == 1
    assert "🔄 A2 — A revisited topic" in text  # the real A2 is untouched


def test_repeated_run_is_idempotent(tmp_path):
    root = make_root(tmp_path)
    props = [proposal("A17"), proposal("A18")]
    append_proposals_to_learning_path(root, props)
    first = read_path(root)

    assert append_proposals_to_learning_path(root, props) == []
    assert read_path(root) == first


def test_repeated_run_is_idempotent_when_codes_were_reallocated(tmp_path):
    """Dedup keys on the sweep concept, not the code: keying on the code would
    re-append a reallocated proposal under a fresh code on every run."""
    root = make_root(tmp_path)
    props = [proposal("A1"), proposal("A2")]
    assert append_proposals_to_learning_path(root, props) == ["A3", "A4"]
    first = read_path(root)

    assert append_proposals_to_learning_path(root, props) == []
    assert read_path(root) == first


def test_v1_is_never_written(tmp_path):
    root = make_root(tmp_path)
    before = (root / "docs" / "learning-path.md").read_text(encoding="utf-8")
    append_proposals_to_learning_path(root, [proposal("A17")])
    assert (root / "docs" / "learning-path.md").read_text(encoding="utf-8") == before


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
