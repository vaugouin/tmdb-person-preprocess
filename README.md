# TMDb Person Preprocess

Preprocessing pipeline for TMDb person data (language-family detection, country-of-birth extraction, name normalization). Ships as a Docker container.

For agent / contributor conventions see [AGENTS.md](AGENTS.md).

---

## Running with Docker

### Secrets handling

**Secrets are never baked into the image.** The Dockerfile contains only non-sensitive defaults; secrets are injected at runtime from a host-managed env file that lives **outside** the application source tree.

- [`.dockerignore`](.dockerignore) excludes `.env` from the build context, so local environment files cannot end up in image layers, the build cache, or any registry.
- The Dockerfile does **not** `COPY .env` and does **not** declare secret `ENV` values.
- At runtime, secrets are passed with Docker's [`--env-file`](https://docs.docker.com/engine/reference/commandline/run/#env-file) option, pointing at a file managed on the host:

  ```
  --env-file /home/debian/docker/tmdb-person-preprocess/.env
  ```

Keep the host env file outside the repository working tree (e.g. under `/home/debian/docker/tmdb-person-preprocess/.env`), readable only by the user that runs the container.

### Required environment variables

The container expects the following variables to be present in the host env file:

| Variable | Purpose |
| --- | --- |
| `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`, `DB_NAMESPACE` | Database connection (see `citizenphil.py`) |
| `TMDB_API_DOMAIN_URL`, `TMDB_API_KEY`, `TMDB_API_TOKEN` | TMDb API access |
| `OPENAI_API_KEY` | OpenAI API key used by the Text2SQL conversion path |
| `USER_TIMEZONE` | IANA timezone string, e.g. `Europe/Paris` |

### Build and run

Build the image (no secrets required at build time):

```bash
docker build -t tmdb-person-preprocess-python-app .
```

Run the container, injecting secrets from the host env file:

```bash
docker run -d --rm \
  --network="host" \
  --env-file /home/debian/docker/tmdb-person-preprocess/.env \
  --name tmdb-person-preprocess \
  tmdb-person-preprocess-python-app
```

Interactive variant for debugging:

```bash
docker run -it --rm \
  --network="host" \
  --env-file /home/debian/docker/tmdb-person-preprocess/.env \
  --name tmdb-person-preprocess \
  tmdb-person-preprocess-python-app
```

The wrapper script [`tmdb-person-preprocess.sh`](tmdb-person-preprocess.sh) performs the build-and-run with the `--env-file` option already wired in.

### What NOT to do

- Do **not** add `COPY .env ...` to the Dockerfile.
- Do **not** put secrets in Dockerfile `ENV` lines — only non-sensitive defaults belong in the image.
- Do **not** commit `.env` to the repository or include it in the build context (`.dockerignore` enforces this).
- Do **not** pass secrets via `-e SECRET=value` on the command line in shared scripts; they end up in shell history and process listings. Use `--env-file` instead.

---

## Write behaviour and monitoring

Both processes are **differential**: they read what the table already holds, compute
the target value, and send only the rows that actually differ. On a settled database
a run writes a handful of rows rather than the whole table, so the runtime is
dominated by the read, not by the write.

### TIM_UPDATED on `T_WC_TMDB_PERSON` is deliberately not written

`COUNTRY_OF_BIRTH` and `COUNTRY_OF_BIRTH_LONG` are derived columns, so the
country-of-birth process updates them without touching `TIM_UPDATED`.

This matters beyond this repository: `tmdb-crawler` builds its person refresh queue
from `WHERE T_WC_TMDB_PERSON.TIM_UPDATED < <J-30>`. Stamping `TIM_UPDATED` here marks
every person as freshly crawled and starves that queue, so no person ever comes up
for a refresh from the TMDb API. If you ever reintroduce a write to that column,
check `tmdb-crawler` first.

`T_WC_TMDB_PERSON_ALSO_KNOWN_AS` rows keep their own `TIM_UPDATED`, written only on
the aliases that are actually inserted or corrected.

### Server variables

Progress is published to `T_WC_SERVER_VARIABLE` under the `strtmdbpersonpreprocess`
prefix (the front-end lists them by prefix, see `tmdb-front/lib/srvvar.inc.php`).

| Variable | Meaning |
| --- | --- |
| `...countryofbirthparsedcount` | persons **examined** by the country-of-birth pass |
| `...countryofbirthupdatedcount` | persons whose country of birth actually **changed** |
| `...countryofbirthfailedcount` | persons the pass could not process |
| `...alsoknownaspersons` | persons examined by the alias pass |
| `...alsoknownasupserted` | aliases **written**: inserted, or corrected because the language family or display order had drifted |
| `...alsoknownasdeleted` | alias rows deleted (stale aliases and duplicates) |
| `...alsoknownasduplicates` | of those, rows that were duplicate `(ID_PERSON, PERSON_NAME)` |

A healthy steady-state run shows a large *examined* count next to a near-zero
*updated* / *written* count. A *written* count that stays as high as *examined* run
after run means the diff is not converging: something else is overwriting the same
columns between runs.

### Pending migration: unique key on the alias table

`T_WC_TMDB_PERSON_ALSO_KNOWN_AS` has no `UNIQUE` key on `(ID_PERSON, PERSON_NAME)`,
although that pair is the table's logical key. The alias pass deletes the duplicates
it meets, but nothing prevents new ones. [`migrations/add_unique_person_alias_key.py`](migrations/add_unique_person_alias_key.py)
deduplicates and adds the key. It is **not applied automatically**:

```bash
python migrations/add_unique_person_alias_key.py           # dry run, reports only
python migrations/add_unique_person_alias_key.py --apply   # dedupe, then ALTER
```

The `ALTER` is blocking and the table has millions of rows: run it in a low-traffic
window, or pass `--skip-alter` and add the index with `pt-online-schema-change`.

### Tracing

Per-row tracing is off (`intverbose = False` in `tmdb-person-preprocess.py`). Turning
it on prints one line per person, which under Docker's `json-file` log driver is a
blocking write on every row: usable to debug a few hundred rows, not to run a full
pass.
