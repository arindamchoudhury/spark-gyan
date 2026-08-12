# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Recommended: Docker with live-reload
docker compose up          # http://localhost:8000; auto-rebuilds on docs/ or zensical.toml changes

# Local Python alternative
python -m venv .venv
.venv\Scripts\Activate.ps1   # macOS/Linux: source .venv/bin/activate
pip install zensical
zensical serve               # built-in live-reload server

# One-off static build (output → ./site/)
zensical build
```

## Architecture

This is a [Zensical](https://zensical.org/) static site — a Python-based docs generator. Content lives in `docs/` as Markdown; `zensical.toml` holds all project metadata and the complete navigation tree. There is no auto-discovery of pages: **every new page must be added to the `nav` array in `zensical.toml`** or it won't appear in the site.

The site is scaffolded for multiple books under `docs/books/<book-slug>/`. Currently only one book is active (`rioux`). The `docs/topics/` directory is intentionally sparse — cross-book topic synthesis pages are only written once a second book covers the same topic.

**`[project.theme]` is intentionally absent from `zensical.toml`.** Zensical 0.0.x raises an install error if a theme name is set. Do not add it until Zensical 0.1+ ships.

## Site customisation

Custom CSS and JS are loaded via `zensical.toml`:

```toml
extra_css = ["stylesheets/extra.css"]
extra_javascript = ["javascripts/sidebar-toggle.js"]
```

**Sidebar collapse toggle** (`docs/javascripts/sidebar-toggle.js` + `docs/stylesheets/extra.css`):
- Both sidebars are `position: sticky` so they float in place as the main content scrolls
- Adds a ◀/▶ button to each sidebar; click to collapse/expand
- Collapsed state persists in `localStorage` across page navigations
- Left nav uses key `sidebar-nav-collapsed`, right TOC uses `sidebar-toc-collapsed`
- To remove: delete both files and remove the `extra_css`/`extra_javascript` lines from `zensical.toml`

## Adding a chapter

This section covers **reading notes** for the Rioux book. Synthesized chapters of the personal book (`docs/spark-book/`) are a different job — use the `spark-book` skill, which owns the chapter arc, index, nav, and glossary sync.

1. Fill in `docs/books/rioux/chapters/<NN>-<slug>.md` (all 14 files already exist as placeholders, and nav is already wired in `zensical.toml`).
2. Flip the chapter row from ⬜ to ✅ in `docs/books/rioux/index.md`.
3. Append new terms to `docs/reference/glossary.md` with source attribution (column: "Rioux Ch N").
4. Add any topics the chapter touches to the backlog table in `docs/topics/index.md`.

## The learning path

`docs/learning-path-v2.md` is **the** learning path: 185 topics in strands across four levels.
`docs/learning-path.md` is v1 — deprecated and frozen, kept only for its per-topic source-finding
callouts. Never update v1; its codes mean different topics than v2's, and the crosswalk is at the
end of v2.

**Coverage is audited, not assumed.** All 22 capability areas in
`docs/reference/spark-feature-history/` have been walked row by row against the path. The
conventions that came out of those passes, and that any future audit should follow:

- Enumerate the area's complete non-`Improvement` row list *before* grepping the path — grep
  confirms what you name and cannot reveal what you forgot to name.
- Verify every claim against the local checkout at `C:/opt/learn/spark/repos/spark`, not
  against the release note. Several release-note claims turned out to describe a config that
  defaults to off, or a feature whose name resolves to nothing in the OSS tree.
- Fold small clusters into existing topics as `>` callouts; add a topic only when nothing owns
  the mechanism. New topics go at the end of their level in a new strand, so nothing renumbers.
- State deliberate omissions in the coverage section with a reason. "Out of scope" and "thin"
  are different claims: the first is argued and closed, the second is an open decision.
- After adding topics, resync the counts that appear in five places: the level header, the
  mermaid subgraph, the map table, the strand list, and "14 of N".

## Chapter content conventions

- Open with a source citation line and a 📌 callout noting version differences from Spark 3.2 (book) to current Spark 4.2.0 / Python 3.10+.
- **Check the current stable version before writing** rather than trusting this line — it drifts. `docs/learning-path-v2.md`'s header records what was last verified (v1, `docs/learning-path.md`, is deprecated and frozen — never update it), and `docs/reference/spark-source-map/configs/catalog.yaml` records the version the config catalog was parsed from.
- Chapters 01–16 were written against Spark 4.1.x; four carry a 🔄 revisit banner (see `docs/spark-book/index.md`). When touching any pre-4.2.0 chapter, bump its header version line as you go.
- Exceptions moved in Spark 4.x live under `pyspark.errors`, not the old paths.
- Use `❓` to mark open questions inline.
- PySpark import aliases — always use these exactly:
  ```python
  import pyspark.sql.functions as F
  import pyspark.sql.types as T
  # Reference as F.col(), F.sum(), T.StructType(), T.StringType(), etc.
  ```

## Best practices and conventions sections

Before writing any best-practices content, **web search for current guidance** — do not rely solely on the Rioux book. Primary references to consult:
- Palantir PySpark Style Guide
- ONS (Office for National Statistics) Spark Style Guide
