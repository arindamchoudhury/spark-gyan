"""Tests for refresh_anchors.py's line re-resolution and link rewriting.

The resolution logic is pure -- (old lines, new lines, line number) -> new line
number -- so it is tested directly. Only the git reads are I/O, and those are a
thin wrapper.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from refresh_anchors import (  # noqa: E402
    LINK_RE,
    bump_front_matter,
    enclosing_decl,
    resolve_line,
    rewrite_label,
)

OLD = """package org.apache.spark.sql

class Optimizer(catalogManager: CatalogManager) {

  def defaultBatches: Seq[Batch] = {
    val rules = Seq(
      PushDownPredicates,
      ColumnPruning)
    rules
  }
}
""".split("\n")

# Two lines inserted at the top, one inside the body.
NEW = """package org.apache.spark.sql

import scala.collection.mutable

class Optimizer(catalogManager: CatalogManager) {

  def defaultBatches: Seq[Batch] = {
    val rules = Seq(
      PushDownPredicates,
      CollapseProject,
      ColumnPruning)
    rules
  }
}
""".split("\n")


def test_exact_match_finds_the_shifted_line():
    # `class Optimizer` is line 3 in OLD, line 5 in NEW.
    assert resolve_line(OLD, NEW, 3) == (5, "exact")


def test_exact_match_survives_an_insertion_further_down():
    # `def defaultBatches` is 5 -> 7.
    line, how = resolve_line(OLD, NEW, 5)
    assert (line, how) == (7, "exact")


def test_line_after_an_inserted_body_line_still_resolves():
    # `ColumnPruning)` is 8 in OLD, 11 in NEW.
    assert resolve_line(OLD, NEW, 8)[0] == 11


def test_missing_content_falls_back_to_the_enclosing_declaration():
    removed = [l for l in NEW if "PushDownPredicates" not in l]
    line, how = resolve_line(OLD, removed, 7)  # PushDownPredicates, now gone
    assert line is not None
    assert "via declaration" in how


def test_unresolvable_line_reports_and_returns_none():
    line, how = resolve_line(OLD, ["totally", "different", "file"], 7)
    assert line is None
    assert how


def test_line_past_end_of_old_file_is_rejected():
    assert resolve_line(OLD, NEW, 999) == (None, "line is past the end of the file at the old version")


def test_ambiguous_line_takes_the_nearest_and_says_so():
    old = ["a", "  }", "b", "  }", "c"]
    new = ["a", "  }", "b", "  }", "c"]
    line, how = resolve_line(old, new, 4)
    assert line == 4
    assert "ambiguous" in how


def test_enclosing_decl_finds_the_nearest_one_above():
    assert enclosing_decl(OLD, 6) == (5, "rules")   # `val rules`, not the enclosing def
    assert enclosing_decl(OLD, 4) == (4, "defaultBatches")
    assert enclosing_decl(OLD, 3) == (2, "Optimizer")


def test_enclosing_decl_returns_none_without_one():
    assert enclosing_decl(["// just", "// comments"], 1) is None


def test_rewrite_label_updates_first_and_bare_line_refs():
    label = "Optimizer.scala:3 (class), :5 (defaultBatches), :8 (rules)"
    got = rewrite_label(label, {3: 5, 5: 7, 8: 11})
    assert got == "Optimizer.scala:5 (class), :7 (defaultBatches), :11 (rules)"


def test_rewrite_label_leaves_unmapped_numbers_alone():
    assert rewrite_label("F.scala:10, :20", {10: 12}) == "F.scala:12, :20"


def test_rewrite_label_does_not_touch_the_filename():
    assert rewrite_label("SparkContext.scala:86", {86: 90}) == "SparkContext.scala:90"


@pytest.mark.parametrize("link,expected", [
    ("[A.scala:9](https://github.com/apache/spark/blob/v4.2.0/core/A.scala#L9)",
     ("A.scala:9", "v4.2.0", "core/A.scala", "9")),
    ("[A.scala](https://github.com/apache/spark/blob/v4.1.2/core/A.scala)",
     ("A.scala", "v4.1.2", "core/A.scala", None)),
])
def test_link_regex_parses_both_forms(link, expected):
    m = LINK_RE.search(link)
    assert m is not None
    assert (m.group("label"), m.group("ref"), m.group("path"), m.group("line")) == expected


def test_link_regex_ignores_other_hosts():
    assert LINK_RE.search("[x](https://example.com/blob/v1/a.scala#L1)") is None


def test_bump_front_matter_rewrites_only_the_version():
    text = 'topic: B7\nspark_version: "4.1.2"\nstatus: complete\nspark_version: "keep"\n'
    got = bump_front_matter(text, "v4.2.0")
    assert 'spark_version: "4.2.0"' in got
    assert got.count('spark_version:') == 2
    assert 'spark_version: "keep"' in got  # only the first is touched


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
