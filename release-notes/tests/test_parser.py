from catalog.parser import classify_type, KEEP_TYPES, DROP_TYPES


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
