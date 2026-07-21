"""Deterministic parser for release-notes/spark_all_changelogs.txt."""

import html
import re
from dataclasses import dataclass
from typing import Iterator

KEEP_TYPES = {"New Feature", "Improvement", "Story", "Epic", "Umbrella", "Planned Work", "Github Integration"}
DROP_TYPES = {
    "Bug", "Sub-task", "Task", "Test", "Dependency upgrade",
    "Documentation", "Question", "Wish", "Technical task", "Brainstorming",
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


@dataclass
class Item:
    spark_id: str | None
    title: str
    jira_type: str
    disposition: str


_H2_RE = re.compile(r"<h2[^>]*>(.*?)</h2>", re.IGNORECASE | re.DOTALL)
_LI_RE = re.compile(r"<li>(.*?)</li>", re.IGNORECASE | re.DOTALL)
_ID_RE = re.compile(r"SPARK-\d+")
_TAG_RE = re.compile(r"<[^>]+>")


def _clean(fragment: str) -> str:
    text = _TAG_RE.sub("", fragment)
    text = html.unescape(text)
    return re.sub(r"\s+", " ", text).strip()


def parse_dump(block_text: str) -> list[Item]:
    items: list[Item] = []
    # Walk h2 headers; each governs the text until the next h2.
    headers = list(_H2_RE.finditer(block_text))
    for i, h in enumerate(headers):
        jira_type = _clean(h.group(1))
        seg_start = h.end()
        seg_end = headers[i + 1].start() if i + 1 < len(headers) else len(block_text)
        segment = block_text[seg_start:seg_end]
        try:
            disposition = classify_type(jira_type)
        except ValueError:
            raise ValueError(f"Unknown JIRA <h2> type: {jira_type!r}")
        for li in _LI_RE.finditer(segment):
            raw = li.group(1)
            id_match = _ID_RE.search(raw)
            spark_id = id_match.group(0) if id_match else None
            title = _clean(raw)
            # Strip leading "[SPARK-NNNN] - " prefix from the title.
            title = re.sub(r"^\[?SPARK-\d+\]?\s*-?\s*", "", title).strip()
            items.append(Item(spark_id, title, jira_type, disposition))
    return items
