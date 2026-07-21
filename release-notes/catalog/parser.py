"""Deterministic parser for release-notes/spark_all_changelogs.txt."""

import re
from typing import Iterator

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


_RELEASE_RE = re.compile(r"^RELEASE:\s*(\S+)\s*$", re.MULTILINE)


def iter_releases(text: str) -> Iterator[tuple[str, str]]:
    matches = list(_RELEASE_RE.finditer(text))
    for i, m in enumerate(matches):
        slug = m.group(1)
        start = m.end()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        yield slug, text[start:end]


def has_dump(block_text: str) -> bool:
    return "<h2" in block_text
