import json
from pathlib import Path

import pytest

from catalog.render import render_timeline, replace_auto_block, write_pages

RECS = [
    {"spark_id": "SPARK-2", "release": "3.0.0", "area": "sql-catalyst", "type": "Improvement", "title": "b feature"},
    {"spark_id": "SPARK-1", "release": "1.6.0", "area": "sql-catalyst", "type": "New Feature", "title": "a feature"},
]

def test_render_timeline_sorted_by_version_asc():
    md = render_timeline(RECS)
    assert md.index("1.6.0") < md.index("3.0.0")
    assert "https://issues.apache.org/jira/browse/SPARK-1" in md
    assert "| a feature |" in md

def test_replace_auto_block_preserves_prose():
    page = "Intro prose.\n<!-- AUTO:timeline START -->\nOLD\n<!-- AUTO:timeline END -->\nOutro."
    out = replace_auto_block(page, "NEW TABLE")
    assert "Intro prose." in out and "Outro." in out
    assert "OLD" not in out and "NEW TABLE" in out

def test_replace_auto_block_raises_on_malformed_markers():
    """START present without a matching END means the page is half-deleted/malformed;
    replace_auto_block must raise rather than silently appending a duplicate block."""
    page = "Intro prose.\n<!-- AUTO:timeline START -->\nOLD (no end marker here)\nOutro."
    with pytest.raises(ValueError, match="malformed AUTO markers: START without END"):
        replace_auto_block(page, "NEW TABLE")

def test_write_pages_merges_prose_records(tmp_path):
    """Test that write_pages merges prose records from _prose.jsonl into timelines."""
    # Create catalog with one record for arrow area
    catalog_data = [
        {"spark_id": "SPARK-1", "release": "3.0.0", "area": "arrow", "type": "New Feature", "title": "arrow optimized"}
    ]
    catalog_file = tmp_path / "_catalog.jsonl"
    catalog_file.write_text("\n".join(json.dumps(r) for r in catalog_data), encoding="utf-8")

    # Create prose file with one record (no spark_id) for arrow area
    prose_data = [
        {"spark_id": None, "release": "2.4.0", "area": "arrow", "type": "Improvement", "title": "early arrow note", "source_quote": "from changelog"}
    ]
    prose_file = tmp_path / "_prose.jsonl"
    prose_file.write_text("\n".join(json.dumps(r) for r in prose_data), encoding="utf-8")

    # Call write_pages
    write_pages(str(catalog_file), str(tmp_path))

    # Verify arrow.md was created with both records
    arrow_md = (tmp_path / "arrow.md").read_text(encoding="utf-8")

    # Should contain the catalog record
    assert "arrow optimized" in arrow_md
    assert "SPARK-1" in arrow_md
    assert "3.0.0" in arrow_md

    # Should contain the prose record with "—" for JIRA (no spark_id)
    assert "early arrow note" in arrow_md
    assert "2.4.0" in arrow_md
    # Prose record should have "—" in JIRA column since spark_id is None
    assert "| 2.4.0 | — |" in arrow_md

def test_write_pages_handles_missing_prose_file(tmp_path):
    """Test that write_pages works correctly when _prose.jsonl is absent."""
    # Create catalog with one record
    catalog_data = [
        {"spark_id": "SPARK-1", "release": "3.0.0", "area": "arrow", "type": "New Feature", "title": "arrow optimized"}
    ]
    catalog_file = tmp_path / "_catalog.jsonl"
    catalog_file.write_text("\n".join(json.dumps(r) for r in catalog_data), encoding="utf-8")

    # Do NOT create _prose.jsonl

    # Call write_pages - should not crash
    write_pages(str(catalog_file), str(tmp_path))

    # Verify arrow.md was created with catalog record
    arrow_md = (tmp_path / "arrow.md").read_text(encoding="utf-8")
    assert "arrow optimized" in arrow_md
    assert "SPARK-1" in arrow_md
    assert "3.0.0" in arrow_md
