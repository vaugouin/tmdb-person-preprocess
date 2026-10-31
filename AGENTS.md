# AGENTS.md - Agent Guide for TMDb Person Preprocess

Rules an agent needs to work on this codebase safely. Docker usage, secrets, server variables, migrations and the reasoning behind the write behaviour live in @README.md, which is canonical: update it for any user-facing change, and update this file only when agent workflow or safety context changes. `CLAUDE.md` and any other tool-specific file only point here.

---

## Related repositories (project ecosystem)

Preprocessing stage of **Agent BBB** (owner `vaugouin`; sibling roster in `%USERPROFILE%/Nestor/projets/t2s-backlog/topics/related-repositories.txt`). Derives country of birth and normalized aliases from the `T_WC_TMDB_PERSON*` tables filled by `tmdb-crawler`, and writes back there. `tmdb-movie-preprocess` (Process 51) reads `T_WC_TMDB_PERSON_ALSO_KNOWN_AS`.

## Where things live (file → role)

- `tmdb-person-preprocess.py`: the pipeline, one block per process in `arrprocessscope` (1 = COUNTRY_OF_BIRTH, 2 = ALSO_KNOWN_AS).
- `citizenphil.py`: shared DB layer, vendored into sibling repos; a change here is stack-wide. `f_sqlupdatearray` costs one `SELECT *` + UPDATE + COMMIT per call, `f_sqlbulkupsert` collapses N rows into one statement: prefer the latter in any loop over a few hundred rows.
- `migrations/*.py`: one-shot repairs, dry run by default, `--apply` to act; never invoked by the pipeline (README § Migrations).

## Do not write TIM_UPDATED on T_WC_TMDB_PERSON

`tmdb-crawler` builds its person refresh queue from `TIM_UPDATED < <J-30>`; stamping it here starves that queue. The country-of-birth write goes through `f_sqlbulkupsert(..., intaddstdfields=0)`: do not "fix" that 0 into a 1, nor route it back through `f_sqlupdatearray(..., 1)`. Both passes stay differential (only real changes are sent); an unconditional rewrite turns minutes into hours and breaks the crawler through `TIM_UPDATED`.

## A differential pass must be a no-op on its second run

After any change to either pass, run the pipeline twice in a row with `-e PREPROCESS_FULL_PASS=1` and nothing else touching the database: the second run must write zero rows (command: README § The invariant to watch).

Alias convergence rests on two rules (reasoning: README § Alias matching happens under the server's collation). Never undo them:

- look an alias up by exact spelling first, by `person_names.f_aliaskey` only as a fallback;
- an insert never overwrites the row it lands on (`citizenphil.f_sqlbulkinsertnoclobber`), never an upsert. Do not try to reproduce the collation in Python instead.

## Code conventions

- **Hungarian notation** for variables (legacy): `str`, `lng` (integers), `dbl` (floats), `arr` (lists), `int` (boolean-like flags).
- Function names: `f_` prefix (`f_countrylookup`, `f_aliaskey`); private helpers `_`.
- Keep Markdown, JSON and logs UTF-8: they carry non-ASCII names; avoid operations that rewrite them with mojibake.

---

## The passes read incrementally: bump lngderivationversion when logic changes

Each pass reads only persons whose `TIM_UPDATED` moved past its watermark (README § Incremental reading), so it is blind to **your own change to the derivation logic**. If you touch `clean_place_of_birth`, `f_countrylookup`, `guess_language_family`, `f_aliaskey` or `build_person_names`, bump `lngderivationversion` to force one full pass. Forgetting is only caught a week later by `lngfullpassmaxagedays`.

## Database Schema Sources

Reference DDL: [doc/sql/TMDb-tables.sql](doc/sql/TMDb-tables.sql). Do not duplicate table definitions elsewhere; edit it only when asked.

## SQL Object Naming Conventions

- Tables and columns uppercase snake case (except legacy TMDb genre columns `id`, `name`). Prefixes: `T_WC_*` persistent, `T_WC_T2S_*` Text2SQL read model, `T_WC_TMDB_*`, `T_WC_WIKIDATA_*` (staging `STG_T_WC_WIKIDATA_*`), `T_WC_WIKIPEDIA_*`. Join tables `T_WC_T2S_{PARENT}_{CHILD}`.
- Keys: `ID_{ENTITY}` for entities, `ID_ROW` for generic/join rows (or a surrogate like `ID_T2S_PERSON_MOVIE`); foreign keys reuse the referenced key name.
- Columns: `DAT_*` dates, `TIM_*` datetimes, `IS_*` or legacy `DELETED` flags, `DISPLAY_ORDER`, `*_COUNT`, `*_PATH`, `_FR`-style language suffixes (generic rows use `LANG`), `*_NORM` / `*_KEY` for RapidFuzz columns, `POPULARITY` as tie-breaker.
- Indexes: preserve the mixed legacy style (`KEY COLUMN_NAME`, `IDX_*`, `UK_*`, `FK_*`, `ft_*` for FULLTEXT).

## Build & deployment (Docker)

See README § Running with Docker and the `Dockerfile`. Pass credentials at runtime (`--env-file`), never bake secrets into the image.

---

**Last Updated**: 2026-10-31
**Current Version**: 1.0.6 

## Backlog (Nestor second-brain)

Tasks `TMDB-PERSON-PREPROCESS-NNN` (status, priority, quick wins) live in the Nestor repo; consult before implementing: `C:\Users\vaugo\Nestor\projets\t2s-backlog\repos\tmdb-person-preprocess.md` (dashboard: `...\t2s-backlog\index.md`). These paths exist only on Philippe's PC, not on the VPS or cloud agents.

## SQL files live in `doc/sql/`

Stack-wide convention (2026-08-20). Every **read-only** `.sql` (audit, monitoring, exports, reference DDL) lives in `doc/sql/`, never at the root nor in a `doc/queries/`. Exceptions: a `.sql` **executed by code** stays where the code expects it (moving it breaks a run silently), and a `.sql` that **writes** (migration, seed, cleanup) stays with its procedure. Test: if running it twice by accident would change the database, it is not a `doc/sql/` file.
