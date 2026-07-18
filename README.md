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

# Regenerate the landing page / coverage matrix
# Also appends proposed topics from sweep gaps to learning-path.md automatically.
# Pass --no-write-proposals to skip the learning-path update.
python tools/spark_source_map/gen_coverage.py
# Output: docs/reference/spark-source-map/index.md (+ learning-path.md if proposals exist)

# Run tests
python -m pytest tools/spark_source_map/test_gen_configs.py
```

Both scripts work from any directory. Never hand-edit generated files — re-run the generator instead.

The Spark source defaults to `C:/opt/learn/spark/repos/spark`; override with `--source` or the `SPARK_SRC` environment variable.

> **Check the checkout before regenerating.** The catalog records whatever it parsed in `meta.spark_version`, with no warning if that isn't what you meant. A checkout left on `master` yields a `5.0.0-SNAPSHOT` catalog that looks perfectly valid. To target a release: `git -C C:/opt/learn/spark/repos/spark checkout v4.2.0`.

**Subsystems by config density** (Spark 4.2.0). These counts say where each config is *declared*, which is not where the feature runs — Spark declares nearly every SQL config in `sql/catalyst`'s `SQLConf.scala`, so that 721 covers all of Spark SQL:

| Subsystem | Configs | Groups |
|---|---|---|
| `sql/catalyst` | 750 | analysis, optimizer, planner, expressions, types-parser |
| `core` | 546 | rdd-layer, execution-engine, shuffle-memory, storage-serializer, infra |
| `resource-managers/kubernetes` | 89 | driver-executor, auth-networking |
| `resource-managers/yarn` | 61 | am-executor |
| `streaming` | 28 | structured-streaming, dstream |
| `sql/connect` | 44 | client-server, declarative-pipelines |
| `sql/hive` | 17 | hive-metastore |
| `connector/kafka-0-10` | 8 | consumer |
| `connector/kafka-0-10-sql` | 8 | source-sink |
| `connector/profiler` | 7 | async-profiler |

Totals **1558 configs** across the repo at 4.2.0 (4 unparsed — known dynamic-key cases in the Kubernetes `Config.scala` and the two `s"spark.sql.catalog.$SESSION_CATALOG_NAME..."` entries in `SQLConf.scala` / `StaticSQLConf.scala`).

**Sweepable but config-free.** A subsystem with no configs of its own is invisible to the table above while still being worth sweeping — `sql/core` holds the physical execution for most of the book's topics, and `sql/pipelines` backs topic A11:

| Subsystem | Groups |
|---|---|
| `sql/core` | query-execution, joins-exec, adaptive, datasources, agg-window-exchange, python-arrow, streaming-exec |
| `sql/pipelines` | graph, autocdc, pipeline-runtime |

Group definitions for every subsystem live in `docs/reference/spark-source-map/groups.yaml`.

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
