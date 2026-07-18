#!/usr/bin/env python3
"""Generate a deterministic catalog of Apache Spark configuration entries from source.

Engine 1 of the spark-source-map pipeline. Parses ``buildConf("...")`` and
``ConfigBuilder("...")`` builder chains across the whole Spark repo, extracting
key / type / default / version / doc / source location, writes ``catalog.yaml``
(source of truth), and renders ``index.md`` (a grouped Markdown reference page).

Design: docs/superpowers/specs/2026-06-06-spark-source-map-design.md

Usage:
    python gen_configs.py --source <spark-root> --out-dir <docs/reference/spark-source-map/configs>
    python gen_configs.py --render-only --out-dir <...>   # re-render md from existing yaml

Honesty guarantee: a builder chain that cannot be fully resolved is never dropped.
It is recorded under ``unparsed`` with its raw snippet and file:line.
"""
from __future__ import annotations

import argparse
import datetime as _dt
import os
import re
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Iterator

import yaml

# --- builder grammar ---------------------------------------------------------

# Entry points of a config builder chain. `buildConf` / `buildStaticConf` are the
# SQLConf.scala wrappers (grep `def build.*Conf` in the Spark tree — those two are
# the only ones); everything else calls `ConfigBuilder` directly.
BUILDER_NAMES = ("buildConf", "buildStaticConf", "ConfigBuilder")
BUILDER_RE = re.compile(r"\b(?:%s)\s*\(" % "|".join(BUILDER_NAMES))
# Terminal builder calls that close a config chain.
TERMINAL_RE = re.compile(
    r"\.(createWithDefaultString|createWithDefaultFunction|createWithDefault"
    r"|createOptional|fallbackConf|create)\b"
)
VERSION_RE = re.compile(r"\.version\(\s*\"([^\"]*)\"\s*\)")
# First type method in a chain decides the logical type.
TYPE_RE = re.compile(
    r"\.(intConf|longConf|doubleConf|booleanConf|stringConf|bytesConf"
    r"|timeConf|enumConf|fallbackConf)\b"
)
TYPE_MAP = {
    "intConf": "int", "longConf": "long", "doubleConf": "double",
    "booleanConf": "boolean", "stringConf": "string", "bytesConf": "bytes",
    "timeConf": "time", "enumConf": "enum", "fallbackConf": "fallback",
}
STRING_LITERAL_RE = re.compile(r'"((?:[^"\\]|\\.)*)"')
NUMERIC_RE = re.compile(r"^-?\d[\d_]*(?:\.\d+)?(?:[eE][+-]?\d+)?[LlDdFf]?$")
KEY_ARG_RE = re.compile(r'^\s*"((?:[^"\\]|\\.)*)"\s*$')
IDENT_ARG_RE = re.compile(r"^[A-Za-z_][\w.]*$")  # a (possibly dotted) constant ref
# `NAME = "spark...."` constant definitions in Scala (val) and Java (static final String).
SYMBOL_DEF_RE = re.compile(
    r'\b([A-Za-z_][A-Za-z0-9_]*)\s*(?::\s*String\s*)?=\s*"(spark\.[^"]*)"')
# `s"...$IDENT..."` / `s"...${IDENT}..."` interpolation placeholders.
INTERP_RE = re.compile(r"\$\{?([A-Za-z_][\w.]*)\}?")

MAX_BLOCK_CHARS = 6000  # a builder chain never spans more than this


@dataclass
class ConfigEntry:
    key: str
    type: str
    default: object
    default_kind: str  # literal | string | optional | expr | fallback | none
    version: str | None
    doc: str
    source_file: str
    source_line: int
    prefix: str
    subsystem: str


@dataclass
class Unparsed:
    raw: str
    source_file: str
    source_line: int
    reason: str


@dataclass
class Catalog:
    meta: dict = field(default_factory=dict)
    configs: list = field(default_factory=list)
    unparsed: list = field(default_factory=list)


# --- low-level scanning helpers ----------------------------------------------

def read_paren(text: str, open_idx: int) -> tuple[str | None, int]:
    """Return (inner, index_after_close) for the parenthesis group at ``open_idx``.

    String-literal aware so parens inside strings don't unbalance the scan.
    """
    assert text[open_idx] == "("
    depth = 0
    i = open_idx
    in_str = False
    esc = False
    while i < len(text):
        c = text[i]
        if in_str:
            if esc:
                esc = False
            elif c == "\\":
                esc = True
            elif c == '"':
                in_str = False
        else:
            if c == '"':
                in_str = True
            elif c == "(":
                depth += 1
            elif c == ")":
                depth -= 1
                if depth == 0:
                    return text[open_idx + 1:i], i + 1
        i += 1
    return None, len(text)


def line_of(text: str, idx: int) -> int:
    return text.count("\n", 0, idx) + 1


def unescape(s: str) -> str:
    return (s.replace('\\"', '"').replace("\\\\", "\\")
             .replace("\\n", "\n").replace("\\t", "\t"))


def join_doc(doc_arg: str) -> str:
    """Concatenate the string literals inside a ``.doc(...)`` argument."""
    parts = [unescape(m.group(1)) for m in STRING_LITERAL_RE.finditer(doc_arg)]
    return re.sub(r"\s+", " ", "".join(parts)).strip()


# --- field extraction --------------------------------------------------------

def parse_default(terminal: str, arg: str | None) -> tuple[object, str]:
    """Map a terminal builder call to (default_value, default_kind)."""
    if terminal == "createOptional":
        return None, "optional"
    if terminal == "create":
        return None, "none"
    if terminal == "createWithDefaultFunction":
        return (arg or "").strip(), "expr"
    if terminal == "fallbackConf":
        return (arg or "").strip(), "fallback"
    if terminal == "createWithDefaultString":
        m = STRING_LITERAL_RE.search(arg or "")
        return (unescape(m.group(1)) if m else (arg or "").strip()), "string"
    # createWithDefault(...)
    raw = (arg or "").strip()
    if raw in ("true", "false"):
        return raw == "true", "literal"
    m = KEY_ARG_RE.match(raw)
    if m:
        return unescape(m.group(1)), "literal"
    if NUMERIC_RE.match(raw):
        token = raw.rstrip("LlDdFf").replace("_", "")
        try:
            return (int(token) if re.fullmatch(r"-?\d+", token) else float(token)), "literal"
        except ValueError:
            return raw, "expr"
    return raw, "expr"  # enum value, computed expression, Seq(...), etc.


def prefix_of(key: str) -> str:
    return ".".join(key.split(".")[:3])


def subsystem_of(rel_path: str) -> str:
    parts = rel_path.replace("\\", "/").split("/")
    top = parts[0]
    if top in ("sql", "connector", "resource-managers") and len(parts) > 1:
        return f"{top}/{parts[1]}"
    return top


# --- per-file parse ----------------------------------------------------------

def resolve_key(arg: str, symbols: dict[str, str]) -> str | None:
    """Resolve a non-literal ``buildConf`` argument to a config key, or None.

    Handles a bare/dotted constant reference (``SqlApiConfHelper.ANSI_ENABLED_KEY``)
    and simple ``s"...$CONST..."`` interpolation, using the repo symbol table.
    """
    arg = arg.strip()
    if IDENT_ARG_RE.match(arg):
        return symbols.get(arg.split(".")[-1])
    m = re.match(r'^s"((?:[^"\\]|\\.)*)"$', arg)
    if m:
        body = m.group(1)
        ok = True

        def _sub(im: re.Match) -> str:
            nonlocal ok
            val = symbols.get(im.group(1).split(".")[-1])
            if val is None:
                ok = False
                return im.group(0)
            return val
        out = INTERP_RE.sub(_sub, body)
        if ok and out.startswith("spark."):
            return out
    return None


def parse_file(text: str, rel_path: str,
               symbols: dict[str, str] | None = None) -> tuple[list[ConfigEntry], list[Unparsed]]:
    symbols = symbols or {}
    configs: list[ConfigEntry] = []
    unparsed: list[Unparsed] = []
    for bm in BUILDER_RE.finditer(text):
        open_idx = bm.end() - 1  # position of '('
        arg, after = read_paren(text, open_idx)
        if arg is None:
            continue
        block_window = text[bm.start():bm.start() + MAX_BLOCK_CHARS]
        tm = TERMINAL_RE.search(block_window, after - bm.start())
        if not tm:
            # No terminal nearby -> a def / unrelated buildConf reference. Skip
            # unless the key is a literal, in which case flag it as unparsed.
            km = KEY_ARG_RE.match(arg)
            if km and km.group(1).startswith("spark."):
                unparsed.append(Unparsed(
                    raw=block_window[:120], source_file=rel_path,
                    source_line=line_of(text, bm.start()), reason="no-terminal"))
            continue

        # Resolve the terminal's own argument (if any).
        term_name = tm.group(1)
        term_abs = bm.start() + tm.end()
        term_arg = None
        j = term_abs
        while j < len(text) and text[j] in " \t\r\n":
            j += 1
        if j < len(text) and text[j] == "(":
            term_arg, _ = read_paren(text, j)

        km = KEY_ARG_RE.match(arg)
        if km:
            key = unescape(km.group(1))
        else:
            arg_s = arg.strip()
            if arg_s == "key" or ":" in arg_s:
                continue  # the buildConf/buildStaticConf helper definition, not a config
            key = resolve_key(arg_s, symbols)
            if key is None:
                # Real config chain but the key constant couldn't be resolved.
                unparsed.append(Unparsed(
                    raw=block_window[:160].replace("\n", " "), source_file=rel_path,
                    source_line=line_of(text, bm.start()), reason="dynamic-key"))
                continue
        if not key.startswith("spark."):
            continue  # buildConf used for something that isn't a public config key

        block = block_window[:tm.end()]
        doc = ""
        dm = re.search(r"\.doc\s*\(", block)
        if dm:
            doc_arg, _ = read_paren(block_window, dm.end() - 1)
            doc = join_doc(doc_arg or "")
        vmatch = VERSION_RE.search(block)
        version = vmatch.group(1) if vmatch else None
        tmatch = TYPE_RE.search(block)
        ctype = TYPE_MAP.get(tmatch.group(1), "unknown") if tmatch else "unknown"
        default, kind = parse_default(term_name, term_arg)
        if ctype == "unknown" and kind == "string":
            ctype = "string"
        if term_name == "fallbackConf":
            ctype = "fallback"
        configs.append(ConfigEntry(
            key=key, type=ctype, default=default, default_kind=kind,
            version=version, doc=doc, source_file=rel_path,
            source_line=line_of(text, bm.start()),
            prefix=prefix_of(key), subsystem=subsystem_of(rel_path)))
    return configs, unparsed


# --- repo walk ---------------------------------------------------------------

def iter_scala_main(source_root: Path) -> Iterator[Path]:
    for path in source_root.rglob("*.scala"):
        p = path.as_posix()
        if "/src/main/" not in p:
            continue
        if "/target/" in p:
            continue
        yield path


def build_symbol_table(source_root: Path) -> dict[str, str]:
    """Map ``CONSTANT_NAME -> "spark...."`` for constants used as config keys.

    Scans Scala and Java main sources. Ambiguous names (same identifier bound to
    different keys in different files) are dropped so a key is never mis-resolved.
    """
    table: dict[str, str] = {}
    ambiguous: set[str] = set()
    for ext in ("*.scala", "*.java"):
        for path in source_root.rglob(ext):
            p = path.as_posix()
            if "/src/main/" not in p or "/target/" in p:
                continue
            text = path.read_text(encoding="utf-8", errors="ignore")
            if "spark." not in text:
                continue
            for m in SYMBOL_DEF_RE.finditer(text):
                name, val = m.group(1), m.group(2)
                if name in table and table[name] != val:
                    ambiguous.add(name)
                else:
                    table[name] = val
    for name in ambiguous:
        table.pop(name, None)
    return table


def detect_version(source_root: Path) -> str:
    pom = source_root / "pom.xml"
    if pom.exists():
        text = pom.read_text(encoding="utf-8", errors="ignore")
        m = re.search(
            r"<artifactId>spark-parent[^<]*</artifactId>\s*<version>([^<]+)</version>",
            text)
        if m:
            return m.group(1).strip()
        m = re.search(r"<version>([^<]+)</version>", text)
        if m:
            return m.group(1).strip()
    return "unknown"


def build_catalog(source_root: Path) -> Catalog:
    symbols = build_symbol_table(source_root)
    configs: list[ConfigEntry] = []
    unparsed: list[Unparsed] = []
    for path in iter_scala_main(source_root):
        rel = path.relative_to(source_root).as_posix()
        text = path.read_text(encoding="utf-8", errors="ignore")
        if not any(f"{name}(" in text for name in BUILDER_NAMES):
            continue
        c, u = parse_file(text, rel, symbols)
        configs.extend(c)
        unparsed.extend(u)
    configs.sort(key=lambda e: e.key)
    unparsed.sort(key=lambda u: (u.source_file, u.source_line))
    cat = Catalog()
    cat.meta = {
        "spark_version": detect_version(source_root),
        "source_root": source_root.as_posix(),
        "generated_at": _dt.date.today().isoformat(),
        "entry_count": len(configs),
        "unparsed_count": len(unparsed),
    }
    cat.configs = [asdict(e) for e in configs]
    cat.unparsed = [asdict(u) for u in unparsed]
    return cat


# --- YAML I/O ----------------------------------------------------------------

def write_yaml(cat: Catalog, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as fh:
        yaml.safe_dump(
            {"meta": cat.meta, "configs": cat.configs, "unparsed": cat.unparsed},
            fh, sort_keys=False, allow_unicode=True, width=100, default_flow_style=False)


def load_yaml(path: Path) -> Catalog:
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    cat = Catalog()
    cat.meta = data.get("meta", {})
    cat.configs = data.get("configs", [])
    cat.unparsed = data.get("unparsed", [])
    return cat


# --- Markdown rendering ------------------------------------------------------

def _fmt_default(c: dict) -> str:
    kind = c.get("default_kind")
    val = c.get("default")
    if kind == "optional":
        return "_(optional)_"
    if kind == "none":
        return "—"
    if kind == "fallback":
        return f"→ `{val}`"
    if kind == "expr":
        return f"`{val}`"
    if isinstance(val, bool):
        return "`true`" if val else "`false`"
    if val is None or val == "":
        return "—"
    return f"`{val}`"


def _esc(text: str) -> str:
    return (text or "").replace("|", "\\|").replace("\n", " ")


def github_url(version: str, source_file: str, line: int) -> str:
    if version and version != "unknown" and "SNAPSHOT" not in version:
        ref = f"v{version}"
    else:
        ref = "master"  # SNAPSHOT/dev builds have no release tag
    return f"https://github.com/apache/spark/blob/{ref}/{source_file}#L{line}"


def render_markdown(cat: Catalog, groups: dict | None = None) -> str:
    meta = cat.meta
    ver = meta.get("spark_version", "unknown")
    lines: list[str] = []
    lines.append("# Spark configuration catalog")
    lines.append("")
    lines.append(
        f"> Auto-generated from Apache Spark **{ver}** source by "
        "`tools/spark_source_map/gen_configs.py`. Do not edit by hand — "
        "re-run the generator instead.")
    lines.append("")
    lines.append(
        f"**{meta.get('entry_count', 0)} configs** across the repo · "
        f"{meta.get('unparsed_count', 0)} unparsed · "
        f"generated {meta.get('generated_at', '?')}.")
    lines.append("")

    by_sub: dict[str, list[dict]] = {}
    for c in cat.configs:
        by_sub.setdefault(c["subsystem"], []).append(c)

    lines.append("## Contents")
    lines.append("")
    for sub in sorted(by_sub):
        anchor = sub.replace("/", "").replace(".", "")
        lines.append(f"- [{sub}](#{anchor}) — {len(by_sub[sub])} configs")
        if groups and sub in groups:
            for g in groups[sub]:
                topics = g.get("topics", [])
                topics_str = f" (topics {', '.join(topics)})" if topics else ""
                scope = g.get("scope", "")
                lines.append(
                    f"    - **Group {g['number']} — {g['title']}**"
                    f"{topics_str}: {scope}")
    lines.append("")

    for sub in sorted(by_sub):
        lines.append(f"## {sub}")
        lines.append("")
        by_prefix: dict[str, list[dict]] = {}
        for c in by_sub[sub]:
            by_prefix.setdefault(c["prefix"], []).append(c)
        for prefix in sorted(by_prefix):
            lines.append(f"### `{prefix}.*`")
            lines.append("")
            lines.append("| Config | Type | Default | Since | Description | Source |")
            lines.append("|---|---|---|---|---|---|")
            for c in sorted(by_prefix[prefix], key=lambda x: x["key"]):
                url = github_url(ver, c["source_file"], c["source_line"])
                src = f"[src]({url})"
                row = (f"| `{c['key']}` | {c['type']} | {_fmt_default(c)} "
                       f"| {c.get('version') or '—'} | {_esc(c.get('doc', ''))} | {src} |")
                lines.append(row)
            lines.append("")

    if cat.unparsed:
        lines.append("## Unparsed entries")
        lines.append("")
        lines.append(
            "> These builder chains could not be fully resolved (dynamic keys or "
            "missing terminals). They are listed here rather than dropped.")
        lines.append("")
        lines.append("| Reason | Source | Snippet |")
        lines.append("|---|---|---|")
        for u in cat.unparsed:
            url = github_url(ver, u["source_file"], u["source_line"])
            lines.append(
                f"| {u['reason']} | [{u['source_file']}:{u['source_line']}]({url}) "
                f"| `{_esc(u['raw'])[:80]}` |")
        lines.append("")

    return "\n".join(lines) + "\n"


def load_groups(out_dir: Path) -> dict | None:
    groups_file = out_dir.parent / "groups.yaml"
    if not groups_file.exists():
        return None
    return yaml.safe_load(groups_file.read_text(encoding="utf-8"))


def write_markdown(cat: Catalog, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    groups = load_groups(path.parent)
    path.write_text(render_markdown(cat, groups), encoding="utf-8", newline="\n")


# --- CLI ---------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Generate the Spark config catalog.")
    ap.add_argument("--source", default=os.environ.get("SPARK_SRC", r"C:/opt/learn/spark/repos/spark"),
                    help="Spark source root.")
    _repo_root = Path(__file__).resolve().parents[2]
    ap.add_argument("--out-dir", default=str(_repo_root / "docs/reference/spark-source-map/configs"),
                    help="Directory for catalog.yaml and index.md.")
    ap.add_argument("--render-only", action="store_true",
                    help="Skip parsing; re-render index.md from existing catalog.yaml.")
    args = ap.parse_args(argv)

    out_dir = Path(args.out_dir)
    yaml_path = out_dir / "catalog.yaml"
    md_path = out_dir / "index.md"

    if args.render_only:
        cat = load_yaml(yaml_path)
    else:
        source_root = Path(args.source)
        if not source_root.exists():
            ap.error(f"source root not found: {source_root}")
        cat = build_catalog(source_root)
        write_yaml(cat, yaml_path)

    write_markdown(cat, md_path)
    m = cat.meta
    print(f"spark {m.get('spark_version')}: {m.get('entry_count')} configs, "
          f"{m.get('unparsed_count')} unparsed -> {yaml_path}, {md_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
