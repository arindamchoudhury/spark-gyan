# The Spark Book

A [Zensical](https://zensical.org/) static site built from personal study notes on Apache Spark and PySpark.

Three layers, in dependency order:

- `docs/learning-path.md` — the topic taxonomy (B/I/A/E codes), what to read for each, and where I am
- `docs/books/<slug>/` — source-faithful reading notes, one directory per external book
- `docs/spark-book/` — the synthesis: one chapter per learning-path topic, blending every source read on it

Targets **Spark 4.2.0**. Chapters 01–16 were written against 4.1.x; those with real drift are marked 🔄 in `docs/spark-book/index.md`.

## Run with Docker (recommended)

```bash
docker compose up
# open http://localhost:8000
```

`zensical.toml` and `docs/` are bind-mounted, so edits live-reload.

## Run locally with Python

```bash
python -m venv .venv
. .venv/Scripts/Activate.ps1   # macOS/Linux: source .venv/bin/activate
pip install zensical
zensical serve
```

## Spark source map

Tools in `tools/spark_source_map/` mine the Apache Spark source using a hybrid two-direction pipeline under `docs/reference/spark-source-map/`.

**Two tracing directions:**
- **Topic-first** (`trace <code>`) — start from a learning-path topic, find the backing source classes and configs. Output: `topics/<code>.md`.
- **Source-first sweep** (`sweep <subsystem>`) — scan a subsystem, surface concepts the learning path doesn't yet cover. Output: `sweeps/<slug>.md` + proposals auto-appended to `learning-path.md`.

```bash
# Refresh the whole-repo config catalog (deterministic, ~8 s)
python tools/spark_source_map/gen_configs.py
# Output: docs/reference/spark-source-map/configs/catalog.yaml + configs/index.md

# Check hand-authored metadata against the checkout (read-only, ~2 s, exit 1 on drift)
python tools/spark_source_map/check_drift.py

# Regenerate the landing page / coverage matrix
# Also appends proposed topics from sweep gaps to learning-path.md automatically.
# Pass --no-write-proposals to skip the learning-path update.
python tools/spark_source_map/gen_coverage.py
# Output: docs/reference/spark-source-map/index.md (+ learning-path.md if proposals exist)

# Run tests
python -m pytest tools/spark_source_map/test_gen_configs.py
```

All scripts work from any directory. Never hand-edit generated files — re-run the generator instead.

### Sweeping a subsystem, group by group

A sweep covers **one group of one subsystem per run**. Big subsystems (`sql/catalyst` at ~750 configs, `sql/core`) are far too large for a single pass, so `groups.yaml` carves each into study-sized groups with a `scope` (which packages and classes), the topic codes it backs, and a description.

**See what's there before picking one:**

```bash
python tools/spark_source_map/gen_coverage.py --list-groups core   # one subsystem
python tools/spark_source_map/gen_coverage.py --list-groups        # all of them
```

Prints each group with its topics, its scope, and whether it has already been swept (and at which Spark version). Read-only — it writes nothing.

```
core
  rdd-layer                [swept: complete, Spark 4.1.2, core-rdd-layer.md]
      topics: I4, I5, I6
      scope:  rdd/, Dependency, Partition, Partitioner, broadcast/
  execution-engine         [not swept]
      topics: B1, E1
      scope:  scheduler/ (DAGScheduler, TaskScheduler, Stage), executor/, TaskContext, BarrierTaskContext
```

**Then ask the `spark-source-map` skill for that group by name:**

```
sweep core execution-engine
```

Naming the group is what keeps a run finishable. `sweep core` on its own is a request to sweep every group in `core` at once; the skill will stop and ask you to pick one rather than attempt it. Either form is fine — naming the group up front just skips the question.

Each group gets **its own page** — `sweeps/<subsystem-with-slashes-as-dashes>-<group>.md`, e.g. `core-rdd-layer.md`, `sql-catalyst-optimizer.md`. Never merge two groups into one file: `gen_coverage.py` keys the sweep-status table on `(subsystem, group)` and the `group:` field is a scalar, so the second group would be dropped and left showing `⬜ pending`. Filenames are otherwise inert — the generator globs `sweeps/*.md` and reads front matter — but they must be unique.

The sweep-status rows come from `groups.yaml`, which is authoritative for which groups exist. A page's optional `all_groups:` is only a fallback for a subsystem `groups.yaml` does not list. It used to be the primary source, which meant adding a group to an already-swept subsystem silently dropped it from the table — no row at all, not even pending. Keep `all_groups` accurate if you write it, but nothing breaks when it trails.

```yaml
---
subsystem: core
group: rdd-layer
all_groups: [rdd-layer, execution-engine, shuffle-memory, storage-serializer, infra]
status: complete            # or: partial, when the group's scope isn't fully covered
spark_version: "4.2.0"
concepts:
  - name: broadcast
    topics: [I4, E1]        # learning-path codes this concept backs
  - name: pair-rdd-functions
    topics: []              # [] = discovery gap; add a propose: block, and
    propose:                # gen_coverage.py appends it to learning-path.md
      code: I13
      level: Intermediate
      title: "Pair RDD Aggregations"
      what: "One sentence."
      why: "One sentence."
---
```

### groups.yaml is hand-authored

Nothing generates it — `gen_configs.py` and `gen_coverage.py` only read it. Editing it directly is the only way to change it, and no generator run notices when its scopes drift from the source. Its `_meta` block records the Spark version the scopes were last walked against.

That's what `check_drift.py` is for. It verifies the `_meta` stamp against the catalog, that every subsystem is a real module directory, and that every class and package named in a `scope` still exists **inside that group's modules**. Subsystem names are module paths, so the search is exact rather than guessed.

Those checks are all one direction: does what a scope *names* still exist. The inverse — what exists that no scope names — is `--coverage`:

```bash
python tools/spark_source_map/check_drift.py --coverage
```

This matters because a sweep only walks a group's scope, so **a package no group claims can never be swept**, and its concepts can never surface as `propose:` blocks. The gap hides itself. Run it after any Spark upgrade, and when adding a group. It is advisory and never fails the build: plenty of packages (`util/`, `errors/`, `test/`, `dsl/`) are plumbing that rightly has no group, and deciding what deserves one is editorial.

**What is checked at which level.** The two directions do not have the same granularity:

| Check | Subsystem | Per group |
|---|---|---|
| `_meta.spark_version` matches the catalog | ✓ | — |
| module directory exists | ✓ | — |
| classes named in a `scope` resolve | ✓ | ✓ (against that group's `modules:`) |
| packages named in a `scope` resolve | ✓ | ✓ |
| unclaimed packages (`--coverage`) | ✓ | ✗ |
| two groups claiming one package (`--coverage`) | — | ✓ |

`--coverage` joins every group's scope into one string, so a package claimed by *any* group counts as claimed. It will tell you `core` has a hole; it will not tell you which group should own it, or that one group's scope is thin.

`--coverage` also reports the one per-group question that *is* well defined: **two groups claiming the identical package path**. Both sweeps would then walk the same code. A parent/child pair is fine and is not flagged — `sql/core`'s `query-execution` claims `execution/` while `joins-exec` claims `execution/joins/`, and sweeping one does not duplicate the other. Only an exact-path collision counts. Where the sharing is deliberate, set `shared_scope: true` on each group involved and the report labels it declared rather than flagging it; `resource-managers/kubernetes` is the standing example, splitting `k8s/` into `driver-executor` and `auth-networking` by theme.

The wider gap is deliberate, because general per-group coverage is not well defined. Some groups partition by directory (`sql/core`'s seven groups each take a different `execution/*` child), others by concern *within* one directory — `resource-managers/kubernetes` splits `k8s/` into `driver-executor` and `auth-networking` by theme, not by path. There is no mechanical way to say a themed group "missed" a package, so such a check would be noise, or would force an unnatural path-based carving.

Per-group completeness is enforced at sweep time instead, by the sweeper rather than a script: every config in the group's slice must tie to a concept, and one that doesn't means an area that was never visited. That check is mechanical, and the skill requires it before a sweep page is written.

Spark 4.x moved classes between modules repeatedly — `StorageLevel` to `common/utils`, the `DataType` hierarchy to `sql/api`. A group can declare `modules: [sql/api]` to name the other modules its classes legitimately live in; that keeps the check strict instead of forcing scopes to stay vague enough to always pass.

Topic and sweep pages carry a `spark_version`. When it trails the catalog, `check_drift.py` warns that `file:line` anchors have likely drifted. Set `version_pinned: "<reason>"` when the older version is deliberate — the Iceberg and Delta traces pin to 4.1 because neither ships a Spark 4.2 module.

### Editing groups.yaml

Adding a group, extending a scope, or recarving a subsystem. All by hand — no generator writes this file.

**1. Decide what changes.** `--coverage` tells you where the holes are; `--list-groups` shows the current carving and what has already been swept. A new group is warranted when a real body of code has no owner (`sql/core/scripting/` backing topic I12, say); extending an existing scope is right when the code belongs to a group that simply never named it.

**2. Edit the YAML, watching two traps.**

- **Numbers must ascend** within a subsystem. A new group goes last unless you renumber.
- **Inserting after a `scope:` line orphans the following `description:` onto your new group**, producing a duplicate key. `yaml.safe_load` accepts this silently and keeps the last one. Insert after the group's `description:` line, not after its `scope:`.

Verify with a duplicate-key-aware parse before moving on:

```bash
python -c "
import yaml, collections, sys
class D(yaml.SafeLoader): pass
def nodup(l, n, deep=False):
    ks = [l.construct_object(k, deep=True) for k, _ in n.value]
    d = [k for k, c in collections.Counter(ks).items() if c > 1]
    if d: sys.exit(f'DUPLICATE KEYS: {d}')
    return yaml.SafeLoader.construct_mapping(l, n, deep)
D.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, nodup)
g = yaml.load(open('docs/reference/spark-source-map/groups.yaml', encoding='utf-8'), Loader=D)
print(sum(len(v) for k, v in g.items() if not k.startswith('_')), 'groups, no duplicate keys')"
```

**3. Name real classes in the scope, then let the checker prove it.**

```bash
python tools/spark_source_map/check_drift.py
```

Write the classes you actually mean — `WholeStageCodegenExec`, not "Whole-Stage CodeGen" — because a name is what makes the scope checkable. Expect to be wrong: the last five scopes written here named four classes that were in a different module or misspelled. When a class legitimately lives elsewhere, add that module to the group's `modules:` list rather than vaguening the scope.

**4. Re-run coverage.**

```bash
python tools/spark_source_map/check_drift.py --coverage
```

Confirms the change closed the gap you meant, and flags an identical-path collision with another group if you created one.

**5. Bump `_meta`** — `spark_version` and `verified_at` — if you re-walked scopes against a checkout. Note in `_meta.note` what changed and why, since nothing else records the reasoning.

**6. Regenerate and sync.**

```bash
python tools/spark_source_map/gen_coverage.py
```

New groups appear in the sweep-status table as `⬜ pending` automatically. Then update the group table in this README and in the skill — both are hand-copied from `groups.yaml`, so they drift the moment you add a group. Check them against the file:

```bash
python -c "
import yaml, re
g = yaml.safe_load(open('docs/reference/spark-source-map/groups.yaml', encoding='utf-8'))
rd = open('README.md', encoding='utf-8').read()
for s, v in g.items():
    if s.startswith('_'): continue
    m = re.search(r'^\|\s*\`' + re.escape(s) + r'\`\s*\|([^|]*)\|', rd, re.M)
    want = [x['name'] for x in v]
    got = [t.strip() for t in m.group(1).split(',')] if m else None
    if got != want: print('DRIFT', s, got, '!=', want)
print('table checked')"
```

**7. Commit `groups.yaml` and the regenerated `index.md` together**, so the carving and the rendered matrix never disagree in history.

### When a new Spark version ships

Work in this order — each step feeds the next, and steps 3–4 are the ones that catch silent breakage.

**1. Move the checkout to the new tag.**

```bash
git -C C:/opt/learn/spark/repos/spark fetch --tags
git -C C:/opt/learn/spark/repos/spark checkout v<new>
```

Do this first. Everything downstream reads whatever is checked out, and none of it warns when that isn't what you meant.

**2. Regenerate the config catalog.**

```bash
python tools/spark_source_map/gen_configs.py
python -m pytest tools/spark_source_map/test_gen_configs.py
```

Read the printed summary. Config count should be **> 1000** and in the same ballpark as last release — a sharp drop means a parser regression against new source syntax, not a shrinking Spark. If `unparsed` climbs above ~5, open `catalog.yaml`'s `unparsed:` block and confirm they are genuinely hard cases. Never hand-edit the catalog.

**3. Check for drift, and fix what it names.**

```bash
python tools/spark_source_map/check_drift.py
```

This is the step that earns the release. Spark moves classes between modules every major version — 4.x moved `StorageLevel` to `common/utils` and the `DataType` hierarchy to `sql/api` — and a `groups.yaml` scope naming a relocated class is prose, so nothing else notices. For each error: add the owning module to that group's `modules:` list, or fix the name. Then bump `_meta.spark_version` and `_meta.verified_at` in `groups.yaml` so the stamp matches the catalog and the checker goes green.

Warnings about topic and sweep pages recorded against the old version are advisory. They mean the anchors on those pages likely moved.

**4. Regenerate the coverage matrix.**

```bash
python tools/spark_source_map/gen_coverage.py
```

**5. Decide what to re-trace, and don't just bump tags.** The warnings from step 3 list every page whose anchors predate the new release. Re-tracing is a real pass over the source, not a find-and-replace on the version in the GitHub URLs: line numbers drift heavily between releases (26 of 33 anchors moved for B3 between 4.1.2 and 4.2.0), and a stale anchor still renders perfectly on GitHub while pointing at the wrong code. Re-verify each anchor against the local checkout, then update the page's `spark_version`, `traced_at`, and refresh-log row. Pages that pin deliberately (`version_pinned`) need no action.

**6. Reconcile the prose layers.**

- `docs/learning-path.md` header — bump **Last updated**, **Current Spark stable**, and the maintenance lines; note what changed in the new release and which topics it touches.
- `docs/spark-book/index.md` — chapters written against the old version need their status reviewed. Distinguish **wrong** (a changed default, a dropped requirement) from merely **incomplete** (new surface not yet covered): mark the first 🔄 and fix it before trusting the chapter, and treat the second as safe to read as-is. Every chapter also carries a `Spark <version>` line in its header.
- `README.md` — the "Targets Spark X" line at the top.

**7. Commit the catalog, `groups.yaml`, `index.md`, and any re-traced pages together**, so the source map and the path never disagree in history.

The Spark source defaults to `C:/opt/learn/spark/repos/spark`; override with `--source` or the `SPARK_SRC` environment variable.

> **Check the checkout before regenerating.** The catalog records whatever it parsed in `meta.spark_version`, with no warning if that isn't what you meant. A checkout left on `master` yields a `5.0.0-SNAPSHOT` catalog that looks perfectly valid. To target a release: `git -C C:/opt/learn/spark/repos/spark checkout v4.2.0`.

### Sweepable subsystems

| Subsystem | Groups |
|---|---|
| `sql/catalyst` | analysis, optimizer, planner, expressions, types-parser, framework |
| `sql/core` | query-execution, joins-exec, adaptive, datasources, agg-window-exchange, python-arrow, streaming-exec, classic-api, sql-scripting |
| `core` | rdd-layer, execution-engine, shuffle-memory, storage-serializer, infra, rpc-resources, api-bridge |
| `sql/pipelines` | graph, autocdc, pipeline-runtime |
| `sql/connect` | client-server, declarative-pipelines |
| `resource-managers/kubernetes` | driver-executor, auth-networking |
| `resource-managers/yarn` | am-executor |
| `sql/hive` | hive-metastore |
| `streaming` | dstream |
| `connector/kafka-0-10` | consumer |
| `connector/kafka-0-10-sql` | source-sink |
| `connector/profiler` | async-profiler |

Sweep in book-priority order: `sql/catalyst` and `sql/core` first — highest config density, and closest to what the book covers.

**Config counts are deliberately not repeated here.** The sweep-status table in `docs/reference/spark-source-map/index.md` is generated from `catalog.yaml` on every `gen_coverage.py` run, so it is always current; a copy in this file is a second number to maintain, and it rotted last time (this section carried v4.1.2 counts well past the 4.2.0 refresh, and disagreed with its own prose). Counts also say where a config is *declared*, not where the feature runs — nearly every SQL config is declared in `sql/catalyst`'s `SQLConf.scala`, so `sql/core` and `sql/pipelines` show none at all while holding the physical execution for most of the book's topics.

Two subsystems are not where their name suggests:

- **Structured Streaming is not in `streaming`.** That module is DStream only. Structured Streaming executes in `sql/core` (`execution/streaming/runtime/`) and is covered by the `sql/core — streaming-exec` group.
- **DataSource V2 spans two modules** by design — logical relations in `sql/catalyst`, ~87 physical exec classes in `sql/core`.

Group definitions live in `docs/reference/spark-source-map/groups.yaml`.

Topic traces and source sweeps (LLM-driven, one unit at a time) are done via the `spark-source-map` Claude Code skill.

## Fetching vendor pages

Certification pages, training catalogs, and release notes are JavaScript-rendered; a plain fetch returns a login shell or silently drops the exact numbers (question counts, domain weights, versions). `scripts/fetch_page.py` drives system Chrome via Playwright and saves verbatim text:

```bash
python scripts/fetch_page.py "<url>" --slug <slug> --timeout 45000
# Output: cache/web/<slug>.txt  (gitignored scratch)
```

Needs `pip install playwright` once — no browser download, it uses the installed Chrome. Used when re-verifying the learning path against current cert and release facts.

## Adding a new chapter's notes

Reading notes for an external book:

1. Edit `docs/books/<slug>/chapters/<NN>-<slug>.md`.
2. Nav is already wired in `zensical.toml`.
3. Flip the row to ✅ in `docs/books/<slug>/index.md`.
4. Update `docs/topics/index.md` backlog with any topics the chapter touches.
5. Append new terms to `docs/reference/glossary.md` with source attribution.

A synthesized chapter in the book itself is a different job — it blends every source read on one learning-path topic. Use the `spark-book` skill; it handles the chapter arc, the index and nav wiring, and the glossary sync. Zensical has no page auto-discovery, so **any** new page must be added to `nav` in `zensical.toml` or it won't appear.
