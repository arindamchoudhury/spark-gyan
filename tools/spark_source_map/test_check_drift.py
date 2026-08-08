"""Tests for check_drift.py's config-coverage check.

The package half of --sweeps needs a real checkout to test; this half does not,
so the config logic is tested directly. It exists because `core` was 9/9 swept,
every page `status: complete`, with 40% of its configs cited nowhere.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from check_drift import (  # noqa: E402
    MIN_FAMILY,
    SRC_FILE_RE,
    config_family,
    report_config_coverage,
)


def cfg(key: str, subsystem: str = "core") -> dict:
    return {"key": key, "subsystem": subsystem}


def groups(n: int) -> list[dict]:
    return [{"name": f"g{i}"} for i in range(n)]


def test_config_family_takes_two_segments():
    assert config_family("spark.shuffle.sort.bypassMergeThreshold") == "spark.shuffle"
    assert config_family("spark.executor.cores") == "spark.executor"


def test_config_family_keeps_short_keys_whole():
    assert config_family("spark.master") == "spark.master"
    assert config_family("spark") == "spark"


def test_skipped_until_every_group_is_swept():
    configs = [cfg(f"spark.orphan.k{i}") for i in range(5)]
    out = report_config_coverage(configs, "core", groups(3), {"a.md": "nothing"}, ())
    assert out == []


def test_untouched_family_fails_once_all_groups_are_swept():
    configs = [cfg(f"spark.orphan.k{i}") for i in range(MIN_FAMILY)]
    out = report_config_coverage(configs, "core", groups(1), {"a.md": "nothing"}, ())
    assert len(out) == 1
    assert "spark.orphan.*" in out[0]
    assert "core" in out[0]


def test_small_untouched_family_reports_but_does_not_fail():
    configs = [cfg(f"spark.tiny.k{i}") for i in range(MIN_FAMILY - 1)]
    out = report_config_coverage(configs, "core", groups(1), {"a.md": "nothing"}, ())
    assert out == []


def test_partly_cited_family_does_not_fail():
    """A page names the configs carrying a concept, not every config in the family."""
    configs = [cfg(f"spark.shuffle.k{i}") for i in range(10)]
    page = "we discuss spark.shuffle.k0 at length"
    out = report_config_coverage(configs, "core", groups(1), {"a.md": page}, ())
    assert out == []


def test_fully_cited_family_does_not_fail():
    configs = [cfg("spark.a.one"), cfg("spark.a.two"), cfg("spark.a.three")]
    page = "spark.a.one spark.a.two spark.a.three"
    assert report_config_coverage(configs, "core", groups(1), {"a.md": page}, ()) == []


def test_config_plumbing_suppresses_a_family():
    configs = [cfg(f"spark.streaming.k{i}") for i in range(MIN_FAMILY + 2)]
    out = report_config_coverage(configs, "core", groups(1), {"a.md": ""},
                                 ("core:spark.streaming.",))
    assert out == []


def test_config_plumbing_is_scoped_to_its_subsystem():
    """A config excluded for core must still be checkable where it belongs."""
    configs = [cfg(f"spark.streaming.k{i}", "streaming") for i in range(MIN_FAMILY)]
    out = report_config_coverage(configs, "streaming", groups(1), {"a.md": ""},
                                 ("core:spark.streaming.",))
    assert len(out) == 1


def test_other_subsystems_configs_are_ignored():
    configs = [cfg("spark.sql.x.y", "sql/catalyst"), cfg("spark.sql.x.z", "sql/catalyst")]
    assert report_config_coverage(configs, "core", groups(1), {"a.md": ""}, ()) == []


def test_report_gives_a_per_page_count_so_the_thin_page_is_visible(capsys):
    """Coverage is judged across all pages of a subsystem; per-page counts locate the gap."""
    configs = [cfg("spark.a.one"), cfg("spark.a.two")]
    report_config_coverage(configs, "core", groups(1),
                           {"thin.md": "", "thick.md": "spark.a.one spark.a.two"}, ())
    out = capsys.readouterr().out
    assert "2/2 keys cited" in out          # the subsystem is covered...
    assert "thick.md" in out and "cites    2" in out
    assert "thin.md" in out and "cites    0" in out   # ...but not evenly


def test_src_file_re_matches_hyphenated_source_names():
    """`package-info.java` and `coalesce-public.scala` are real files in the checkout.

    The stem character class had no hyphen, so a page citing one could never be
    credited and its package reported one fewer cited file forever. Found by the
    connector/kafka-0-10 sweep, the first swept module with a hyphenated source file.
    """
    cited = set(SRC_FILE_RE.findall(
        "opened package-info.java, coalesce-public.scala and KafkaRDD.scala"))
    assert cited == {"package-info.java", "coalesce-public.scala", "KafkaRDD.scala"}


def test_src_file_re_widening_changes_nothing_else():
    """The hyphen must only add hyphenated stems, not loosen the pattern otherwise."""
    # A bare extension is still not a filename.
    assert SRC_FILE_RE.findall("a bare .scala suffix") == []
    # A leading hyphen is a word boundary, not part of the stem.
    assert SRC_FILE_RE.findall("-leading.scala") == ["leading.scala"]
    # Pre-existing and unchanged: a dotted config key ending in .java still matches its
    # last two segments. Harmless here — pages cite configs and files in the same blob,
    # and a spurious `sql.java` matches no real file, so it is never counted as cited.
    assert SRC_FILE_RE.findall("spark.sql.java") == ["sql.java"]


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
