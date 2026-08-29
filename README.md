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
| `...alsoknownasinserted` | aliases the table did not hold yet |
| `...alsoknownasupdated` | existing aliases corrected in place (language family or display order had drifted) |
| `...alsoknownasupserted` | the sum of the two, kept for continuity |
| `...alsoknownasdeleted` | alias rows deleted (stale aliases and duplicates) |
| `...alsoknownasduplicates` | of those, rows that were duplicate `(ID_PERSON, PERSON_NAME)` |

### The invariant to watch: two runs in a row

A healthy run shows a large *examined* count next to a near-zero *updated* / *inserted*
count. The real test is stronger, and worth running after any change to either pass:

```bash
# nothing else touching the database in between
docker run --rm --network="host" --env-file <envfile>   --name tmdb-person-preprocess-run2 tmdb-person-preprocess-python-app 2>&1 | tail -20
```

**The second run must write nothing at all.** A pass that writes on a database it has
just converged is thrashing: it is not reacting to a change, it is fighting itself. That
is how the alias collation defect below was found, after it had shipped.

### Migrations

One-shot schema and data repairs live in [`migrations/`](migrations/). None of them is
ever invoked by the pipeline: they are run by hand, in a low-traffic window, with the
preprocess stopped (`bash off.sh`, then `bash on.sh` afterwards, see below). All of
them are **dry run by default** and idempotent, so a second run after success reports
that there is nothing to do.

Run them from inside the container, overriding the image's default command:

```bash
docker run -it --rm --network="host" \
  --env-file /home/debian/docker/tmdb-person-preprocess/.env \
  --name tmdb-person-preprocess-migration \
  tmdb-person-preprocess-python-app \
  python ./migrations/<script>.py            # add --apply to write
```

Use a container name distinct from `tmdb-person-preprocess`, and prefer `-d` plus
`docker logs -f` (without `--rm`) for the long ones, so a dropped SSH session does not
kill the job halfway.

| Script | What it repairs | Status |
| --- | --- | --- |
| [`add_unique_person_alias_key.py`](migrations/add_unique_person_alias_key.py) | Deduplicates `T_WC_TMDB_PERSON_ALSO_KNOWN_AS` on `(ID_PERSON, PERSON_NAME)` and adds the missing `UNIQUE` key, so duplicate aliases cannot come back | Applied 2026-08-28 (729 groups, 737 rows removed) |
| [`clear_none_place_of_birth.py`](migrations/clear_none_place_of_birth.py) | Turns the literal text `"None"` back into SQL `NULL` in `PLACE_OF_BIRTH` and `COUNTRY_OF_BIRTH_LONG` | Run **after** the `tmdb-crawler` fix is deployed |

#### About the `"None"` cleanup

`tmdb-crawler` used to read the person payload with `str(data['place_of_birth'])`, and
TMDb sends `"place_of_birth": null` for most persons. `str(None)` is the 4-character
string `"None"`, which is neither `NULL` nor empty, so it defeated every
`IS NOT NULL AND <> ''` filter: the country-of-birth pass was reading and parsing about
4.5M junk rows out of 5M on every run, and storing `COUNTRY_OF_BIRTH_LONG = 'none'`
derived from them.

The crawler now stores `NULL` (`tmdb_functions.py`, person payload block). Deploy that
first: running the cleanup while the old crawler is live just lets it write `"None"`
again on the persons it refreshes. The pass also carries a `PLACE_OF_BIRTH <> 'None'`
guard, kept as cheap insurance against the same regression upstream.

The cleanup deliberately leaves `COUNTRY_OF_BIRTH` alone. It already holds `''` on those
rows, which is what the pass stores for any place it cannot resolve; turning only this
subset into `NULL` would make "unknown country" mean two different things.

### Alias matching happens under the server's collation

`T_WC_TMDB_PERSON_ALSO_KNOWN_AS.PERSON_NAME` is `utf8mb4_unicode_ci` and, since the
unique key was added, `(ID_PERSON, PERSON_NAME)` identifies one row. That collation
folds case **and** diacritics on Latin letters, so `Jean Reno`, `JEAN RENO`, `Beyonce`
and `Beyoncé` are not four rows the server is willing to hold: they are two.

Matching aliases with Python's `==` therefore made the pass insert a spelling it
believed missing, the upsert landed on the existing row and imposed its `DISPLAY_ORDER`,
and the next run put the other spelling back. Around 26k writes per run, indefinitely,
with no deletion to show for it.

`person_names.f_aliaskey` is the fold that keeps Python and the server in agreement, and
the pass looks an alias up by its **exact** spelling first, by the folded key only as a
fallback. That order is deliberate: the fold does not claim to reproduce the collation
exactly, and looking exact-first means a fold that is too aggressive can at worst skip an
insert. It can never make the pass delete an alias the server was willing to keep.

If you ever simplify that lookup back to a plain dictionary hit on `PERSON_NAME`, the
two-runs-in-a-row check above will go red.

### Tracing

Per-row tracing is off (`intverbose = False` in `tmdb-person-preprocess.py`). Turning
it on prints one line per person, which under Docker's `json-file` log driver is a
blocking write on every row: usable to debug a few hundred rows, not to run a full
pass.
