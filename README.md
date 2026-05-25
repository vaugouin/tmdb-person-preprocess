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
