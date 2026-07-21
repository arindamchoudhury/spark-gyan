from catalog.render import render_timeline, replace_auto_block

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
