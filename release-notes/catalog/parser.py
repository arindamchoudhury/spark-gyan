"""Deterministic parser for release-notes/spark_all_changelogs.txt."""

KEEP_TYPES = {"New Feature", "Improvement", "Story", "Epic", "Umbrella"}
DROP_TYPES = {
    "Bug", "Sub-task", "Task", "Test", "Dependency upgrade",
    "Documentation", "Question", "Wish", "Technical task",
}


def classify_type(type_label: str) -> str:
    label = type_label.strip()
    if label in KEEP_TYPES:
        return "keep"
    if label in DROP_TYPES:
        return "drop"
    raise ValueError(f"Unknown JIRA type: {label!r}")
