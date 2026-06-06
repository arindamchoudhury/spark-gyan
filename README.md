# PySpark — Reading Notes

A [Zensical](https://zensical.org/) static site built from personal study notes on Apache Spark and PySpark.

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

Tools in `tools/spark_source_map/` mine the Apache Spark source to generate a config catalog and topic-coverage matrix under `docs/reference/spark-source-map/`.

```bash
# Refresh the whole-repo config catalog (deterministic, ~8 s)
python tools/spark_source_map/gen_configs.py
# Output: docs/reference/spark-source-map/configs/catalog.yaml + configs/index.md

# Regenerate the landing page / coverage matrix
python tools/spark_source_map/gen_coverage.py
# Output: docs/reference/spark-source-map/index.md

# Run tests
python -m pytest tools/spark_source_map/test_gen_configs.py
```

Both scripts work from any directory. `SPARK_SRC` env var or `--source` flag overrides the default Spark source path (`C:/opt/learn/spark/spark`). Never hand-edit the generated files — re-run the generator instead.

Subsystem traces (LLM-driven, one subsystem at a time) are done via the `spark-source-map` Claude Code skill.

## Adding a new chapter's notes

1. Edit `docs/books/<slug>/chapters/<NN>-<slug>.md`.
2. Nav is already wired in `zensical.toml`.
3. Flip the row to ✅ in `docs/books/<slug>/index.md`.
4. Update `docs/topics/index.md` backlog with any topics the chapter touches.
5. Append new terms to `docs/reference/glossary.md` with source attribution.
