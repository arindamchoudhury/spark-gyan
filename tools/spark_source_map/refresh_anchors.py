#!/usr/bin/env python
"""Re-resolve the file:line anchors on topic and sweep pages against a newer Spark.

Every anchor on a source-map page is a GitHub blob URL pinned to a tag and a line
number. Line numbers drift heavily between releases -- 26 of 33 moved for B3
between 4.1.2 and 4.2.0 -- and a stale anchor still renders perfectly on GitHub
while pointing at the wrong code. Bumping the tag in the URL without re-checking
the line is therefore worse than leaving it alone.

Doing that by hand across 30+ pages and 1300+ anchors is what makes a release
runbook stall, so this does the mechanical half: for each anchor it reads the
line as it was at the *recorded* version, finds that same line in the *new*
version, and rewrites both the URL and the visible `File.scala:N` label.

It resolves by content, not by guessing:

  1. exact match on the stripped line text -- unique hit wins; several hits, the
     one nearest the old position wins
  2. failing that, walk up to the nearest enclosing declaration (`def`/`class`/
     `object`/`val`/...), locate *that* in the new file, and apply the same
     offset -- this is what carries a line inside a body that was itself edited
  3. failing that, report it unresolved and change nothing

What it cannot do is tell you the code still *means* what the page says. A
resolved anchor points at the same line of source; whether the prose around it
is still true is a judgement call, and the skill's re-verification rule still
applies. Treat the output as a reviewable diff, not as a finished re-trace.

Read-only unless --apply is passed.

Usage:
    python tools/spark_source_map/refresh_anchors.py                 # dry run, all pages
    python tools/spark_source_map/refresh_anchors.py --to v4.3.0
    python tools/spark_source_map/refresh_anchors.py --page topics/b7.md --apply
"""
from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path

import yaml

REPO = "apache/spark"
# [SparkContext.scala:86 (class), :91 (init)](https://github.com/apache/spark/blob/v4.2.0/core/.../SparkContext.scala#L86)
LINK_RE = re.compile(
    r"\[(?P<label>[^\]]*)\]\("
    r"(?P<base>https://github\.com/" + REPO + r"/blob/)"
    r"(?P<ref>[^/]+)/(?P<path>[^)#]+)"
    r"(?:\#L(?P<line>\d+))?\)")
# A label's own line references: the first carries a filename ("Optimizer.scala:100"),
# later ones are bare (", :301"). Both are just `:<digits>` -- a label never contains a
# URL, so there is nothing else here for this to collide with.
LABEL_LINE_RE = re.compile(r"(:)(\d+)")
DECL_RE = re.compile(
    r"^\s*(?:@\w+\s+)*(?:private|protected|final|abstract|sealed|implicit|override|case|lazy|"
    r"public|static|@\w+)?[\w\s\[\]]*?\b"
    r"(?:def|class|object|trait|val|var|type|enum|interface)\s+(\w+)")

MAX_DECL_WALK = 400


class GitFiles:
    """Reads file contents at a ref, memoised. A missing path yields None."""

    def __init__(self, source: Path):
        self.source = source
        self._cache: dict[tuple[str, str], list[str] | None] = {}

    def lines(self, ref: str, path: str) -> list[str] | None:
        key = (ref, path)
        if key not in self._cache:
            try:
                out = subprocess.run(
                    ["git", "-C", str(self.source), "show", f"{ref}:{path}"],
                    capture_output=True, check=True)
                text = out.stdout.decode("utf-8", errors="replace")
                self._cache[key] = text.split("\n")
            except subprocess.CalledProcessError:
                self._cache[key] = None
        return self._cache[key]

    def ref_exists(self, ref: str) -> bool:
        return subprocess.run(
            ["git", "-C", str(self.source), "rev-parse", "--verify", f"{ref}^{{commit}}"],
            capture_output=True).returncode == 0


def enclosing_decl(lines: list[str], idx: int) -> tuple[int, str] | None:
    """Nearest declaration at or above a 0-based index, as (index, name)."""
    for i in range(idx, max(-1, idx - MAX_DECL_WALK), -1):
        m = DECL_RE.match(lines[i])
        if m:
            return i, m.group(1)
    return None


def find_all(lines: list[str], needle: str) -> list[int]:
    return [i for i, l in enumerate(lines) if l.strip() == needle]


def resolve_line(old: list[str], new: list[str], line: int) -> tuple[int | None, str]:
    """Map a 1-based line number from `old` to `new`. Returns (line, how)."""
    if line < 1 or line > len(old):
        return None, "line is past the end of the file at the old version"
    idx = line - 1
    text = old[idx].strip()

    if text:
        hits = find_all(new, text)
        if len(hits) == 1:
            return hits[0] + 1, "exact"
        if len(hits) > 1:
            # Several identical lines (a closing brace, a repeated call). The one
            # nearest the old position is the least surprising choice, but say so.
            best = min(hits, key=lambda i: abs(i - idx))
            return best + 1, f"ambiguous ({len(hits)} identical lines), took nearest"

    decl = enclosing_decl(old, idx)
    if decl:
        d_idx, name = decl
        pattern = re.compile(r"\b(?:def|class|object|trait|val|var|type|enum|interface)\s+"
                             + re.escape(name) + r"\b")
        cands = [i for i, l in enumerate(new) if pattern.search(l)]
        if len(cands) == 1:
            shifted = cands[0] + (idx - d_idx)
            if 0 <= shifted < len(new):
                return shifted + 1, f"via declaration `{name}` (+{idx - d_idx} lines)"
        elif len(cands) > 1:
            return None, f"declaration `{name}` is no longer unique ({len(cands)} matches)"

    if not text:
        return None, "blank line at the old version, nothing to match on"
    return None, "line content not found at the new version"


def rewrite_label(label: str, mapping: dict[int, int]) -> str:
    """Rewrite `File.scala:86`, and any later bare `:91`, using resolved numbers."""
    def sub(m: re.Match) -> str:
        old = int(m.group(2))
        return f":{mapping.get(old, old)}"
    return LABEL_LINE_RE.sub(sub, label)


def front_matter(path: Path) -> dict:
    m = re.match(r"^---\n(.*?)\n---\n", path.read_text(encoding="utf-8"), re.DOTALL)
    return (yaml.safe_load(m.group(1)) or {}) if m else {}


def refresh_page(path: Path, git: GitFiles, to_ref: str) -> tuple[str, list[str], dict]:
    """Return (new_text, messages, stats) for one page. Does not write."""
    text = path.read_text(encoding="utf-8")
    fm = front_matter(path)
    stats = {"total": 0, "moved": 0, "same": 0, "unresolved": 0, "skipped": 0}
    msgs: list[str] = []

    if fm.get("version_pinned"):
        msgs.append(f"pinned ({fm['version_pinned']}) — skipped")
        return text, msgs, stats

    out: list[str] = []
    pos = 0
    for m in LINK_RE.finditer(text):
        out.append(text[pos:m.start()])
        pos = m.end()
        label, from_ref, src = m.group("label"), m.group("ref"), m.group("path")
        line = int(m.group("line")) if m.group("line") else None
        stats["total"] += 1

        if from_ref == to_ref:
            stats["same"] += 1
            out.append(m.group(0))
            continue

        old_lines = git.lines(from_ref, src)
        new_lines = git.lines(to_ref, src)
        if new_lines is None:
            stats["unresolved"] += 1
            msgs.append(f"{src}: gone at {to_ref} — left at {from_ref}")
            out.append(m.group(0))
            continue
        if old_lines is None:
            stats["skipped"] += 1
            msgs.append(f"{src}: not present at {from_ref}, cannot re-resolve — left alone")
            out.append(m.group(0))
            continue

        # Resolve the URL's line plus every line named in the label.
        wanted = {line} if line else set()
        wanted |= {int(n) for _, n in LABEL_LINE_RE.findall(label)}
        mapping: dict[int, int] = {}
        failed = False
        for old_n in sorted(x for x in wanted if x):
            new_n, how = resolve_line(old_lines, new_lines, old_n)
            if new_n is None:
                msgs.append(f"{src}:{old_n} — {how}")
                failed = True
                continue
            mapping[old_n] = new_n
            if new_n != old_n:
                msgs.append(f"{src}:{old_n} -> :{new_n} ({how})")

        if failed:
            stats["unresolved"] += 1
            out.append(m.group(0))
            continue

        stats["moved" if any(v != k for k, v in mapping.items()) else "same"] += 1
        new_label = rewrite_label(label, mapping)
        new_line = mapping.get(line, line) if line else None
        suffix = f"#L{new_line}" if new_line else ""
        out.append(f"[{new_label}]({m.group('base')}{to_ref}/{src}{suffix})")

    out.append(text[pos:])
    return "".join(out), msgs, stats


def bump_front_matter(text: str, to_ref: str) -> str:
    """Record the new version on a page whose anchors all moved to it."""
    ver = to_ref.lstrip("v")
    return re.sub(r'(?m)^(spark_version:\s*)["\']?[^"\'\n]+["\']?$',
                  rf'\1"{ver}"', text, count=1)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--source", help="Spark checkout (default: groups.yaml _meta.source_root).")
    ap.add_argument("--to", help="Target git ref (default: catalog.yaml meta.spark_version as a v-tag).")
    ap.add_argument("--page", action="append",
                    help="Page relative to the source-map dir, e.g. topics/b7.md. Repeatable.")
    ap.add_argument("--apply", action="store_true", help="Write the changes (default: dry run).")
    args = ap.parse_args(argv)

    root = Path(__file__).resolve().parents[2]
    base = root / "docs" / "reference" / "spark-source-map"
    meta = (yaml.safe_load((base / "groups.yaml").read_text(encoding="utf-8")) or {}).get("_meta", {})
    source = Path(args.source or meta.get("source_root", ""))
    if not source.is_dir():
        print(f"error: Spark source not found at {source!s}")
        return 1

    to_ref = args.to
    if not to_ref:
        head = (base / "configs" / "catalog.yaml").read_text(encoding="utf-8")[:2000]
        m = re.search(r"^\s+spark_version:\s*['\"]?([^'\"\n]+)", head, re.MULTILINE)
        if not m:
            print("error: could not read catalog.yaml meta.spark_version; pass --to")
            return 1
        to_ref = "v" + m.group(1).strip()

    git = GitFiles(source)
    if not git.ref_exists(to_ref):
        print(f"error: ref {to_ref} not found in {source}. Fetch tags first.")
        return 1

    if args.page:
        pages = [base / p for p in args.page]
    else:
        pages = sorted(base.glob("topics/*.md")) + sorted(base.glob("sweeps/*.md"))

    total = {"total": 0, "moved": 0, "same": 0, "unresolved": 0, "skipped": 0}
    changed_pages = 0
    for page in pages:
        if not page.exists():
            print(f"{page}: no such page")
            return 1
        new_text, msgs, stats = refresh_page(page, git, to_ref)
        for k in total:
            total[k] += stats[k]
        if not stats["total"] and not msgs:
            continue
        head = (f"{page.relative_to(base).as_posix()}  "
                f"{stats['total']} anchors: {stats['moved']} moved, {stats['same']} unchanged, "
                f"{stats['unresolved']} unresolved, {stats['skipped']} skipped")
        print(head)
        for msg in msgs:
            print(f"    {msg}")
        if new_text != page.read_text(encoding="utf-8"):
            changed_pages += 1
            if args.apply:
                if not stats["unresolved"]:
                    new_text = bump_front_matter(new_text, to_ref)
                else:
                    print("    front matter left at the old version — unresolved anchors remain")
                page.write_text(new_text, encoding="utf-8", newline="")
        print()

    verb = "rewritten" if args.apply else "would change"
    print(f"{total['total']} anchors across {len(pages)} pages -> {to_ref}: "
          f"{total['moved']} moved, {total['same']} unchanged, "
          f"{total['unresolved']} unresolved, {total['skipped']} skipped. "
          f"{changed_pages} page(s) {verb}.")
    if not args.apply and changed_pages:
        print("Dry run. Re-run with --apply, then read the diff: a resolved anchor points at the "
              "same line of source, not at prose that is still true.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
