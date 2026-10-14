# gecko

gecko is the Calypr service that owns project-facing configuration and the
Git-backed project file surface. It is a single Go binary. It exposes two route
families:

- `/config/*` — typed UI configuration: explorer, nav, file_summary, project,
  projects. Stored in PostgreSQL. Writes are validated before they are saved.
- `/git/*` — Calypr projects backed by GitHub repositories: organization and
  project connection, refs, tree, manifest, file and download reads, upload
  sessions, presentation config, thumbnails, storage audit and repair.

Outbound dependencies are PostgreSQL, Fence, GitHub, Syfon, and Grip.

## local setup

Initialize the database. The script creates `testdb`, the `config_schema`
schema, and the config tables that `config.KnownTypes()` serves.

```
./init_postgres.sh
```

Build and run. The server blocks and writes logs to stdout.

```
go build -o bin/gecko
./bin/gecko -db "postgresql://postgres:your_strong_password@localhost:5432/testdb?sslmode=disable" -port 8080
```

Run tests from a second shell.

```
go test -v ./...
```

Check the process is healthy.

```
curl -s localhost:8080/health
```

## flags

Each flag falls back to the named environment variable when it is empty.

| Flag | Env var | Purpose |
| --- | --- | --- |
| `-port` | — | Listen port. Default `8080`. |
| `-db` | `DATABASE_URL` | PostgreSQL DSN. Without it, all data-dependent route families (`/config/*`, `/git/*`, presentation, thumbnail) are skipped at startup; `/health` and swagger are still served. |
| `-jwks` | `JWKS_ENDPOINT` | Fence JWKS endpoint. Without it, protected routes return `401`. |
| `-github-api-base-url` | `GITHUB_API_BASE_URL` | GitHub API base URL. |
| `-fence-base-url` | `FENCE_BASE_URL` | Fence base URL, used for the GitHub installation token exchange. |
| `-git-data-dir` | `GIT_DATA_DIR` | Directory for local git mirrors. |
| `-grip-host` | `GRIP_HOST` | Reserved. Parsed and passed down, no code path uses it today. |
| `-grip-port` | `GRIP_PORT` | Reserved. Same as `-grip-host`. |
| `-grip-graph-zname` | `GRIP_GRAPH` | Reserved. Same as `-grip-host`. |

Environment variables read inside `internal/git`:

| Env var | Purpose |
| --- | --- |
| `SYFON_DATA_API_BASE_URL` | Syfon data API base URL. |
| `STORAGE_CHAIN_AUDIT_CACHE_ENABLED` | Switch for the storage chain audit cache. Default on. |
| `STORAGE_CHAIN_AUDIT_CACHE_TTL_SECONDS` | Cache TTL. Default 5 minutes, minimum 60 seconds. |
| `STORAGE_CHAIN_AUDIT_CACHE_REDIS_URL` | Redis URL for the audit cache. Falls back to `REDIS_URL`. |
| `STORAGE_CHAIN_AUDIT_CACHE_REDIS_PASSWORD` | Redis password. |

## tests

Unit tests are colocated with the code they cover and use `sqlmock`, so they
need no running services. Tests under `tests/` dial `localhost:8080` and skip
themselves when nothing is listening. The CI recipe in
`.github/workflows/tests.yaml` starts PostgreSQL, seeds `config_schema`, starts
the binary, waits on `/health`, and then runs the suite.

## api

Handler annotations are the source of truth for request and response shapes.
Regenerate the spec after any handler change and serve it from `docs/`.

```
make swagger
```

That target needs the `swag` CLI.

```
go install github.com/swaggo/swag/cmd/swag@latest
```

## helm cluster setup

See helm charts for cluster setup.
