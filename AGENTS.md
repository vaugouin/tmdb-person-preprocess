# AGENTS.md - Agent Guide for TMDb Person Preprocess

This file gives you the agentic context you need to work on this codebase safely. For project overview, features, install / deploy steps and human-facing security / performance / troubleshooting material, read @README.md — that file is canonical and not duplicated here.

This is the single canonical guide for autonomous coding agents in this repository. Assistant-specific files such as @CLAUDE.md, and any future tool-specific guide such as `GEMINI.md`, should only point here and should not duplicate repository instructions.

Deeper specs live in their own files:
- @doc/sql/*.sql — reference DDL for the database schema; treat these files as read-only unless the user explicitly asks you to edit schema documentation

- For any project update, keep documentation aligned:
  - Update `README.md` for user-facing behavior, configuration, setup, deployment, troubleshooting, or verification changes.
  - Update this file only when agent workflow or safety context changes.

---

## Related repositories (project ecosystem)

`tmdb-person-preprocess` is one stage of **Agent BBB**, a multi-repository movie/TV database system owned by GitHub user `vaugouin`. All sibling repos live under `%USERPROFILE%/Code/<repo>` and at `github.com/vaugouin/<repo>`; they are interdependent stages of one pipeline that converges on a shared MySQL/MariaDB database (`T_WC_*` tables) and a ChromaDB vector store. The canonical roster of sibling repositories is kept in `doc/related-repositories/related-repositories.txt` in the `tmdb-front` repo.

Pipeline stages:
- **Infrastructure** — `python` (shared crawler base image), `chromadb` (vector service), `reverseproxy` (NGINX TLS ingress), `chromadb-security-test` (firewall validation).
- **Acquisition** — `tmdb-crawler`, `imdb-crawler`, `sparql-crawler`, `sparql-movies-persons`, `wikidata-crawler`, `wikipedia-crawler`, `selenium-tmdb`, `download-images`, `sqlite-plex-to-tmdb`, `movieparadise`.
- **Preprocessing → `T_WC_T2S_*`** — `tmdb-movie-preprocess`, `tmdb-person-preprocess`, `keywords-processing`.
- **Semantic index & name resolution** — `embedding-update`, `embedding-query`, `rapidfuzz_query`.
- **Serving** — `fastapi-text2sql` (NL→SQL API + MCP server), `voice-agent`, `tmdb-front` (PHP web front-end).
- **Evaluation** — `eval-text2sql`, `extract-movie-questions`.
- **Maintenance & tooling** — `plex-duplicates`, `subtitle-translate`, `powershell`, `playwright-test`.
- **Monitoring & observability** — `data-monitoring`.

**This repository's role:** Preprocessing stage. Enriches person records — country-of-birth parsing and alias normalization — writing back to the `T_WC_TMDB_PERSON*` tables. Runs alongside `tmdb-movie-preprocess` and `keywords-processing` to prepare the `T_WC_T2S_*` read-model consumed by `fastapi-text2sql`.

---

## Where things live (file → role)

Edit at the right layer; the architecture is intentionally split.

- `tmdb-person-preprocess.py` : the pipeline itself, one block per process in
  `arrprocessscope` (1 = COUNTRY_OF_BIRTH, 2 = ALSO_KNOWN_AS).
- `citizenphil.py` : shared DB layer, also vendored into the sibling repos. A change
  here is a stack-wide change: `f_sqlupdatearray` costs one `SELECT *` + one UPDATE +
  one COMMIT per call, `f_sqlbulkupsert` collapses N rows into one statement. Prefer
  the latter in any loop over more than a few hundred rows.
- `migrations/*.py` : one-shot schema/data repairs, dry run by default, `--apply` to
  act. Never invoked by the pipeline; run by hand, in a low-traffic window.

## Do not write TIM_UPDATED on T_WC_TMDB_PERSON

`tmdb-crawler` fills its person refresh queue with
`WHERE T_WC_TMDB_PERSON.TIM_UPDATED < <J-30>`. This preprocess derives
`COUNTRY_OF_BIRTH*` from data it did not author, so stamping `TIM_UPDATED` on the
rows it touches marks the whole table as freshly crawled and starves that queue.

Concretely: the country-of-birth write goes through `f_sqlbulkupsert(...,
intaddstdfields=0)`. Do not "fix" that 0 into a 1, and do not route the write back
through `f_sqlupdatearray(..., 1)`.

The same reasoning is why both processes are differential: they compare against the
stored value and send only real changes. Reintroducing an unconditional rewrite turns
a few-minute run back into a multi-hour one and, through `TIM_UPDATED`, breaks the
crawler downstream.

## Code conventions

- **Hungarian notation** for variables (legacy style):
  - `str` — strings (`strtablename`, `strapiversion`)
  - `lng` — integers (`lngpage`, `lngrowsperpage`)
  - `dbl` — floats (`dblavailableram`)
  - `arr` — lists / arrays
  - `int` — boolean-like flags (`intcleanupenabled`, `intentity`)
- **Function naming**: public pipeline entry points use `f_` (`f_text2sql`, `f_entity_extraction`, `f_resolve_complex_question`, `f_answer_single_value`, `f_hello_world`); private helpers use `_` (`_call_chat_llm`, `_normalize_llm_model`).
- **Docstrings**: Google-style on public functions.
- **Error handling**: broad try/except with console logging; surface failures via the `error` response field and the `messages` trace. Database execution errors are not returned directly to clients — they go through the complex-question retry path when enabled.
- **JSON serialization**: use `logs.decimal_serializer()` for `Decimal` and `datetime`.

---

## Database Schema Sources

Full DDL lives under [doc/sql/](doc/sql/); do not duplicate table definitions here. Treat these files as reference-only unless the user explicitly asks for schema-doc edits.

- [doc/sql/TMDb-tables.sql](doc/sql/TMDb-tables.sql) — upstream/source TMDb tables and reference tables.

---

## SQL Object Naming Conventions

- SQL table and column names are uppercase snake case, except legacy imported TMDb genre columns such as `id` and `name`.
- Persistent tables use `T_WC_*`.
- Text2SQL read-model tables use `T_WC_T2S_*`.
- TMDb source/reference tables use `T_WC_TMDB_*`.
- Wikidata tables use `T_WC_WIKIDATA_*`; staging tables use `STG_T_WC_WIKIDATA_*`.
- Wikipedia tables use `T_WC_WIKIPEDIA_*`.
- Join tables usually follow `T_WC_T2S_{PARENT}_{CHILD}`, for example `T_WC_T2S_MOVIE_GENRE`, `T_WC_T2S_PERSON_MOVIE`.
- Primary keys are usually `ID_{ENTITY}` for entity tables, `ID_ROW` for generic/join rows, or a table-specific surrogate such as `ID_T2S_PERSON_MOVIE`.
- Foreign keys reuse the referenced primary-key name, for example `ID_MOVIE`, `ID_PERSON`, `ID_GENRE`.
- Date columns use `DAT_*`; datetime/timestamp columns use `TIM_*`.
- Boolean-like flags use `IS_*` or legacy integer flags such as `DELETED`.
- Ordering uses `DISPLAY_ORDER`.
- Aggregate counters use `*_COUNT`.
- Media paths use `*_PATH`.
- Language-specific labels/titles often use suffixes such as `_FR`; generic language rows use `LANG`.
- RapidFuzz/generated search columns use `*_NORM` and `*_KEY`; popularity tie-breakers commonly use `POPULARITY`.
- Index names are mixed legacy style. Preserve existing style: simple `KEY COLUMN_NAME`, `IDX_*` for indexes, `UK_*` for unique keys, `FK_*` for foreign keys, and `ft_*` for FULLTEXT indexes.

---

## Encoding

Keep Markdown, prompt files, JSON config, and logs UTF-8. These files contain non-ASCII names and multilingual examples. Avoid editor or terminal operations that rewrite them with mojibake.

---

## Build & deployment (Docker)

This preprocessor is built and run as a Docker container via the repo's root `Dockerfile`. The image is based on `python:3.10.5-slim-buster`, installs `requirements.txt`, copies the repo into `/app`, and runs `python ./tmdb-person-preprocess.py` as its `CMD`. Pass database credentials at runtime (e.g. `docker run --env-file ...`); do not bake secrets into the image.

---

**Last Updated**: 2026-08-28
**Current Version**: 1.0.0 

## Backlog (Nestor second-brain)

The prioritized, agent-ready implementation backlog for this repo lives in the **Nestor**
knowledge repo (a separate repo, not cloned alongside this one):

- This repo: `C:\Users\vaugo\Nestor\projets\t2s-backlog\repos\tmdb-person-preprocess.md`
- Cross-repo dashboard: `C:\Users\vaugo\Nestor\projets\t2s-backlog\index.md`

Consult it before implementing: tasks are `TMDB-PERSON-PREPROCESS-NNN` with status (done / in-progress /
todo), priority, and quick-wins. NOTE: these are local paths on Philippe's PC and do not
resolve on the VPS or on cloud agents (claude.ai/code).

## SQL files live in `doc/sql/`

Stack-wide convention, set 2026-08-20. Every **read-only** `.sql` of this repo, audit
queries, monitoring queries, exports, reference DDL dumps, lives in `doc/sql/`, never
at the root and never in a `doc/queries/` of its own.

Two deliberate exceptions, and they are the reason the rule is worded around reading
rather than around file type. A `.sql` **executed by code** stays where the code expects
it, because moving it breaks a run silently. And a `.sql` that **writes** (migration,
seed, `DELETE` cleanup) stays put too: it belongs to a procedure, not to documentation.
When in doubt, ask whether running the file twice by accident would change the database.
If yes, it is not a `doc/sql/` file.
