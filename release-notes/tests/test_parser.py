from catalog.parser import classify_type, KEEP_TYPES, DROP_TYPES, iter_releases, has_dump, parse_dump, Item


SAMPLE = """RELEASE: spark-release-9-9-9
SOURCE: http://x
====
Spark Release 9.9.9
Some prose.
RELEASE: spark-release-8-8-8
SOURCE: http://y
====
<h2>New Feature</h2>
<li>[SPARK-1] - thing</li>
"""


def test_keep_types_classified_keep():
    for t in ["New Feature", "Improvement", "Story", "Epic", "Umbrella"]:
        assert classify_type(t) == "keep"


def test_drop_types_classified_drop():
    for t in ["Bug", "Sub-task", "Task", "Test", "Dependency upgrade",
              "Documentation", "Question", "Wish", "Technical task"]:
        assert classify_type(t) == "drop"


def test_unknown_type_raises():
    import pytest
    with pytest.raises(ValueError):
        classify_type("Brand New Jira Type")


def test_keep_and_drop_disjoint():
    assert KEEP_TYPES.isdisjoint(DROP_TYPES)


def test_iter_releases_splits_on_header():
    rels = list(iter_releases(SAMPLE))
    assert [r[0] for r in rels] == ["spark-release-9-9-9", "spark-release-8-8-8"]


def test_has_dump_detects_h2():
    rels = dict(iter_releases(SAMPLE))
    assert has_dump(rels["spark-release-8-8-8"]) is True
    assert has_dump(rels["spark-release-9-9-9"]) is False


DUMP = """<h2>        New Feature</h2>
<ul>
<li>[<a href='https://issues.apache.org/jira/browse/SPARK-24882'>SPARK-24882</a>] -         data source v2 API improvement &amp; cleanup</li>
</ul>
<h2>        Bug</h2>
<ul>
<li>[<a href='https://issues.apache.org/jira/browse/SPARK-25567'>SPARK-25567</a>] -         Table listing doesn&#39;t sort</li>
</ul>
"""


def test_parse_dump_extracts_items():
    items = parse_dump(DUMP)
    assert len(items) == 2
    feat = items[0]
    assert feat.spark_id == "SPARK-24882"
    assert feat.jira_type == "New Feature"
    assert feat.disposition == "keep"
    assert feat.title == "data source v2 API improvement & cleanup"


def test_parse_dump_unescapes_and_drops_bug():
    items = parse_dump(DUMP)
    bug = items[1]
    assert bug.jira_type == "Bug"
    assert bug.disposition == "drop"
    assert bug.title == "Table listing doesn't sort"
