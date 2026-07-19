#!/usr/bin/env python
"""Check the hand-authored source-map metadata against the Spark checkout.

`groups.yaml` is written by hand and nothing regenerates it, so its group
`scope` fields quietly stop matching the source when Spark reorganises a
package. This checker verifies what can be verified mechanically:

  1. groups.yaml `_meta.spark_version` still matches the parsed catalog
  2. every subsystem key is a real module directory in the checkout
  3. every class named in a `scope` exists *inside that subsystem's module*
  4. every `pkg/` named in a `scope` resolves somewhere under that module
  5. sweep/topic pages record the catalog's Spark version (warning only —
     those pages are historical records, not claims about the present)

Check 3 is the one that earns its keep: the sql/catalyst "planner" scope
claimed SparkPlanner and SparkStrategies, both of which live in sql/core.

Exit 1 on errors, 0 if only warnings. Read-only; writes nothing.

Usage:
    python tools/spark_source_map/check_drift.py [--source PATH] [--quiet]
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import yaml

# A scope names classes in prose: "planning/ (QueryPlanner, GenericStrategy,
# plan-matching patterns e.g. ExtractEquiJoinKeys)". Pick out the identifiers
# without dragging in ordinary capitalised words -- "History Server configs"
# must not read as two class names. Requiring two capitals and one lowercase
# keeps SparkPlanner and SQLConf while dropping History, Server, Kryo, Antlr4.
# Leading capital required: Scala/Java type names have one, prose like "gRPC"
# does not. Prose that still slips through ("Whole-Stage CodeGen") is a sign the
# scope should name the real class (WholeStageCodegenExec) instead.
IDENT_RE = re.compile(r"\b[A-Z][A-Za-z0-9]{3,}\b")
# Directory hints are the tokens ending in a slash: "execution/datasources/v2/".
DIR_RE = re.compile(r"\b([a-z][a-z0-9]*(?:/[a-z0-9]+)*)/")

DECL_RE_CACHE: dict[str, re.Pattern] = {}

SRC_EXTS = (".scala", ".java")


def looks_like_class(tok: str) -> bool:
    uppers = sum(1 for c in tok if c.isupper())
    lowers = sum(1 for c in tok if c.islower())
    return uppers >= 2 and lowers >= 1


def module_dir(source: Path, subsystem: str) -> Path:
    """Subsystem names are module paths -- see subsystem_of() in gen_configs.py."""
    return source / subsystem


def index_module(mod: Path) -> tuple[set[str], set[str], list[Path]]:
    """Return (file stems, relative dir paths, source files) under a module.

    Only src/main is indexed: a scope points at production code, and pulling in
    src/test would let a deleted class keep resolving via its orphaned test.
    """
    stems: set[str] = set()
    dirs: set[str] = set()
    files: list[Path] = []
    for p in mod.rglob("*"):
        parts = p.parts
        if "target" in parts or "src" not in parts:
            continue
        try:
            after_src = parts[parts.index("src") + 1]
        except IndexError:
            continue
        if after_src != "main":
            continue
        if p.is_dir():
            dirs.add(p.relative_to(mod).as_posix())
        elif p.suffix in SRC_EXTS:
            stems.add(p.stem)
            files.append(p)
    return stems, dirs, files


def declared_in(files: list[Path], tokens: set[str]) -> set[str]:
    """Which tokens are declared as a class/object/trait in these files.

    A scope may name a pattern object that shares a file with others --
    ExtractEquiJoinKeys lives in patterns.scala -- so a filename match alone
    would report it missing.
    """
    if not tokens:
        return set()
    pattern = re.compile(
        r"\b(?:class|object|trait|enum|interface)\s+(" + "|".join(sorted(map(re.escape, tokens))) + r")\b")
    found: set[str] = set()
    for f in files:
        try:
            text = f.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue
        found.update(m.group(1) for m in pattern.finditer(text))
        if found == tokens:
            break
    return found


def package_roots(mod: Path, subsystem: str) -> list[Path]:
    """Every natural package root under a module.

    A module may hold several source trees (resource-managers/kubernetes keeps
    its under a core/ submodule), so this returns a list. Within each,
    src/main/scala/org/apache/spark/... is a single chain until it fans out;
    the fan-out point is where the module's own packages begin. For sql/* the
    fan-out lands on org/apache/spark, one level above the interesting
    packages, so keep descending while a child echoes the module path
    (sql/core -> .../spark/sql, streaming -> .../spark/streaming).
    """
    seg = {s for part in subsystem.split("/") for s in (part, part.replace("-", ""))}
    # Every Spark package sits under org/apache/spark; descend it unconditionally
    # so a sibling like org/apache/datasketches cannot stall the walk.
    prefix = ("org", "apache", "spark")
    roots: list[Path] = []
    for src in sorted(mod.rglob("src/main/scala")) + sorted(mod.rglob("src/main/java")):
        if "target" in src.parts:
            continue
        cur = src
        while True:
            kids = {d.name: d for d in cur.iterdir() if d.is_dir()}
            has_src = any(f.suffix in SRC_EXTS for f in cur.iterdir() if f.is_file())
            step = next((kids[p] for p in prefix if p in kids), None)
            if step is None and len(kids) == 1 and not has_src:
                step = next(iter(kids.values()))
            if step is None and not has_src:
                echo = [k for n, k in kids.items() if n in seg]
                step = echo[0] if len(echo) == 1 else None
            if step is None:
                break
            cur = step
        if cur not in roots:
            roots.append(cur)
    return roots


def scope_tokens(scope: str) -> set[str]:
    """Package segments a scope actually claims, lowercased.

    Only segments of a real `foo/bar/` path token count. A scope's prose
    mentions must not claim a package -- "analysis/ (Analyzer, resolution
    rules, catalog)" describes the analyzer, and reading it as coverage of
    catalyst's rules/ and catalog/ packages would hide two genuine gaps.
    Split per segment so 'sources' is not satisfied by 'datasources'.
    """
    out: set[str] = set()
    for m in DIR_RE.finditer(scope):
        out.update(m.group(1).lower().split("/"))
    return out


def report_coverage(source: Path, subsystems: dict) -> int:
    """Report packages that no group scope claims.

    check_drift's other checks verify that what a scope *names* still exists.
    This is the inverse: what exists that no scope names. It matters because a
    sweep only walks a group's scope, so a package no group claims can never be
    swept -- and its concepts can never surface as learning-path proposals.

    Advisory only: some packages are plumbing that deserves no group, and a
    scope may cover a package by naming its classes rather than its directory.
    """
    print("Packages no group scope claims (largest first).")
    print("Advisory: plumbing rightly has no group -- judge, don't auto-add.\n")
    total = 0
    for sub in sorted(subsystems):
        mod = module_dir(source, sub)
        if not mod.is_dir():
            continue
        scope_text = " ".join((g.get("scope") or "") for g in (subsystems[sub] or []))
        toks = scope_tokens(scope_text)
        # Class names in the scope resolve a package too: a group naming
        # DStream covers dstream/ without spelling out the directory.
        scope_classes = {t for t in IDENT_RE.findall(scope_text) if looks_like_class(t)}

        for root in package_roots(mod, sub):
            rows = []
            for pkg in sorted(d for d in root.iterdir() if d.is_dir()):
                if pkg.name.lower() in toks:      # whole segment, case-insensitive
                    continue
                # A scope may claim a nested package (k8s/ lives under deploy/);
                # the parent is then covered, not a gap.
                if any(d.is_dir() and d.name.lower() in toks for d in pkg.rglob("*")):
                    continue
                files = [f for f in pkg.rglob("*") if f.suffix in SRC_EXTS]
                if any(f.stem in scope_classes for f in files):
                    continue
                if files:
                    rows.append((len(files), pkg.name))
            if rows:
                total += len(rows)
                print(f"{sub}  ({root.relative_to(mod).as_posix()})")
                for n, name in sorted(rows, reverse=True):
                    print(f"    {n:>4}  {name}/")
                print()
    print(f"{total} unclaimed package(s). Add a group, extend a scope, or leave as plumbing.")
    return 0


def load_front_matter(path: Path) -> dict:
    text = path.read_text(encoding="utf-8")
    m = re.match(r"^---\n(.*?)\n---\n", text, re.DOTALL)
    return (yaml.safe_load(m.group(1)) or {}) if m else {}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--source", help="Spark checkout (default: groups.yaml _meta.source_root).")
    ap.add_argument("--quiet", action="store_true", help="Only print problems.")
    ap.add_argument("--coverage", action="store_true",
                    help="Report packages no group scope claims, and exit. Advisory, "
                         "never fails: judging what deserves a group is editorial.")
    args = ap.parse_args(argv)

    root = Path(__file__).resolve().parents[2]
    base = root / "docs" / "reference" / "spark-source-map"
    groups_file = base / "groups.yaml"
    catalog_file = base / "configs" / "catalog.yaml"

    if not groups_file.exists():
        print(f"error: {groups_file} not found")
        return 1

    data = yaml.safe_load(groups_file.read_text(encoding="utf-8")) or {}
    meta = data.get("_meta") or {}
    subsystems = {k: v for k, v in data.items() if not k.startswith("_")}

    errors: list[str] = []
    warnings: list[str] = []

    source = Path(args.source or meta.get("source_root", ""))
    if not source or not source.is_dir():
        print(f"error: Spark source not found at {source!s} "
              f"(set _meta.source_root in groups.yaml or pass --source)")
        return 1

    if args.coverage:
        return report_coverage(source, subsystems)

    # --- 1. version stamp ----------------------------------------------------
    cat_version = None
    if catalog_file.exists():
        # The configs list is long; the meta block is the first document lines.
        head = catalog_file.read_text(encoding="utf-8")[:2000]
        m = re.search(r"^\s+spark_version:\s*['\"]?([^'\"\n]+)", head, re.MULTILINE)
        cat_version = m.group(1).strip() if m else None
    stamped = str(meta.get("spark_version", "")).strip()
    if cat_version and stamped and cat_version != stamped:
        errors.append(
            f"version stamp stale: groups.yaml _meta.spark_version={stamped} "
            f"but catalog.yaml was parsed from {cat_version}. Re-walk the group "
            f"scopes against the new checkout, then bump _meta.spark_version "
            f"and _meta.verified_at.")
    elif not stamped:
        warnings.append("groups.yaml has no _meta.spark_version — drift cannot be detected.")

    # --- 2-4. per-subsystem scope checks ------------------------------------
    index_cache: dict[str, tuple[set[str], set[str], list[Path]]] = {}

    def index(sub_name: str):
        if sub_name not in index_cache:
            d = module_dir(source, sub_name)
            index_cache[sub_name] = index_module(d) if d.is_dir() else (set(), set(), [])
        return index_cache[sub_name]

    for sub, groups in subsystems.items():
        mod = module_dir(source, sub)
        if not mod.is_dir():
            errors.append(f"{sub}: no such module directory in the checkout ({mod})")
            continue

        for g in groups or []:
            gname = g.get("name", "?")
            scope = g.get("scope", "") or ""
            # A group may legitimately reach into other modules -- Spark 4.x
            # scattered one subsystem's classes across several. Declaring them
            # keeps the check strict while letting the scope tell the truth.
            mods = [sub] + [m for m in (g.get("modules") or []) if m != sub]
            for extra in mods[1:]:
                if not module_dir(source, extra).is_dir():
                    errors.append(f"{sub} / {gname}: modules: names '{extra}' — no such module")

            stems: set[str] = set()
            dirs: set[str] = set()
            files: list[Path] = []
            for m_name in mods:
                s, d, f = index(m_name)
                stems |= s
                dirs |= d
                files += f

            for d in {m.group(1) for m in DIR_RE.finditer(scope)}:
                # A scope may mention a module by path ("StorageLevel now in
                # common/utils"); that is a module reference, not a package hint.
                if any(m_name == d or m_name.startswith(d + "/") for m_name in mods) \
                        or (source / d).is_dir():
                    continue
                if not any(rel == d or rel.endswith("/" + d) for rel in dirs):
                    errors.append(
                        f"{sub} / {gname}: scope names package '{d}/' — not found under "
                        f"{', '.join(mods)}")

            toks = {t for t in IDENT_RE.findall(scope) if looks_like_class(t)}
            missing = toks - stems
            # A filename miss may still be an object sharing a file --
            # ExtractEquiJoinKeys lives in patterns.scala -- so scan contents.
            gone = sorted(missing - declared_in(files, missing)) if missing else []
            if gone:
                errors.append(
                    f"{sub} / {gname}: scope names {', '.join(gone)} — no such class/object "
                    f"under {', '.join(mods)}. It may have moved: add the owning module to "
                    f"this group's `modules:` list, or fix the name.")

    # --- 5. page version stamps (warn only) ---------------------------------
    for kind in ("sweeps", "topics"):
        d = base / kind
        if not d.is_dir():
            continue
        for p in sorted(d.glob("*.md")):
            text = p.read_text(encoding="utf-8")
            fm = load_front_matter(p)
            ver = str(fm.get("spark_version", "")).strip()
            if not (cat_version and ver and ver != cat_version):
                continue
            # A page may deliberately target an older Spark: Iceberg and Delta
            # ship no Spark 4.2 module, so their traces pin to 4.1 on purpose.
            # version_pinned states that, and is not drift.
            if fm.get("version_pinned"):
                continue
            # The warning is about apache/spark file:line anchors moving. A page
            # whose anchors all point at another repo has none to drift.
            if "apache/spark/blob/" not in text:
                continue
            warnings.append(
                f"{kind}/{p.name}: recorded against Spark {ver}, catalog is now "
                f"{cat_version} — file:line anchors likely drifted. If the older version "
                f"is deliberate, set version_pinned: <reason> in the front matter.")

    # --- report --------------------------------------------------------------
    for w in warnings:
        print(f"warn:  {w}")
    for e in errors:
        print(f"ERROR: {e}")

    if not args.quiet and not errors:
        n_groups = sum(len(g or []) for g in subsystems.values())
        print(f"ok: {len(subsystems)} subsystems, {n_groups} groups checked against "
              f"Spark {stamped or '?'} at {source}")
    if errors:
        print(f"\n{len(errors)} error(s). groups.yaml is hand-authored — fix it by hand.")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
