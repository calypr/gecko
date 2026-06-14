# PR Notes: `feature/project-config`

## What This Branch Actually Does

This branch is not a small project-config patch. It changes Gecko from a relatively flat config and vector service into a multi-surface backend that now owns:

- typed project configuration records
- project-oriented config CRUD routes
- Git-backed project repository state and read APIs
- GitHub/Fence integration points for installation and token brokering
- organization-level Git connect/reconcile flows
- upload session persistence and PR-oriented file submission flows
- thumbnail storage for projects
- a full internal package reorganization
- CI and runtime changes required by the new Git subsystem

The branch is large because it combines feature delivery with a substantial internal re-layout. The biggest review question is not any one handler. It is whether the new package boundaries, auth model, data model, and route surface still line up cleanly with the CALYPR frontend and deployment stack.

## Scope by Area

`git diff --dirstat` against `main` shows the branch is concentrated in these areas:

- `internal/git/` ~10%
- `internal/server/http/git/` ~9%
- `internal/server/middleware/` ~8%
- `gecko/` legacy removal/replacement ~8%
- `internal/server/http/config/` ~7%
- `internal/db/` ~6%
- `config/` ~5%
- `internal/thumbnail/` ~5%
- `tests/integration/` ~4%

That matches the real shape of the work: this is mostly a Git/project backend branch, plus the refactor needed to support it.

## Architecture Reorganization

### Before

The old service logic lived mostly in the top-level `gecko/` package with mixed concerns:

- HTTP handlers
- middleware
- DB logic
- vector logic
- config logic
- response helpers

### After

The branch moves Gecko toward a clearer package layout:

- `config/`
  - typed config models and validation
- `internal/db/`
  - config and Git persistence
- `internal/git/`
  - repository domain logic, reconciliation, setup, sync, upload workflows
- `internal/integrations/fence/`
  - Fence broker client for GitHub App operations
- `internal/integrations/github/`
  - GitHub API client wrapper built on `go-github`
- `internal/server/http/`
  - route families split by surface:
    - `config`
    - `directory`
    - `git`
    - `health`
    - `vector`
    - `shared`
- `internal/server/middleware/`
  - request auth, access checks, logging, resource helpers
- `internal/thumbnail/`
  - thumbnail storage and validation
- `internal/httputil/`
  - shared JSON/error response utilities
- `internal/logging/`
  - service logging wrapper/helpers
- `internal/vectoradapter/`
  - Qdrant request/response translation

This is the right direction structurally. The tradeoff is that the branch mixes architectural reorganization with product behavior changes, so code review needs to separate “moved” from “changed.”

## Runtime / Bootstrap Changes

`main.go` now builds Gecko as a composable server with optional integrations:

- PostgreSQL
- Git service
- thumbnail store
- Qdrant
- Grip
- JWKS-backed JWT validation

The important runtime additions are:

- `--git-data-dir` / `GIT_DATA_DIR`
- `--fence-base-url` / `FENCE_BASE_URL`
- `--github-api-base-url` / `GITHUB_API_BASE_URL`

Once DB connectivity is available, Gecko now also constructs:

- `git.NewGitService(...)`
- `thumbnail.NewFilesystemStore(gitDataDir)`

That makes `gitDataDir` a hard runtime dependency for the Git-enabled server path.

### CI impact

Because Gecko now requires a Git data directory, CI had to be updated. The branch modifies:

- `.github/workflows/tests.yaml`

to launch Gecko with:

```bash
-git-data-dir /tmp/gecko-git
```

Without that, the service exits before health checks come up.

## Config Model Changes

### Project config becomes first-class

This branch adds a typed `ProjectConfig` model in:

- `config/projectConfig.go`

Supported fields:

- `title`
- `contact_email`
- `src_repo`
- `org_title`
- `description`
- `project_title`

Validation includes:

- required-field checks
- email validation
- repository URL normalization into a GitHub-style `host/owner/repo` form

This is a real shift from using generic config blobs everywhere. Project metadata now has a stronger contract and normalization behavior.

### Explorer config compatibility

This branch also carries forward explorer config compatibility work, including the richer `fileActions` shape:

```json
{
  "extensions": {"ext": ["action"]},
  "actions": {"action": "/path"}
}
```

That matters because the frontend expects this richer form, and Gecko has to unmarshal it correctly for CALYPR configs to load.

## Database / Persistence Changes

The database layer is no longer only about simple `config_schema.<type>` JSON tables.

### Existing typed config tables

The init path now ensures config tables for:

- `explorer`
- `nav`
- `file_summary`
- `project`
- `projects`

### New Git-related state tables

`internal/db/EnsureGitProjectStateTable` expands the DB footprint significantly. This branch adds persistence for:

1. `config_schema.git_project_state`
   - repository identity
   - installation metadata
   - mirror path
   - sync/default branch/error state

2. `config_schema.git_organization_state`
   - organization installation/configuration status
   - target metadata
   - timestamps and last error

3. `config_schema.git_upload_session`
   - upload/PR submission session metadata

4. `config_schema.git_upload_session_file`
   - per-file status inside an upload session

5. `config_schema.git_pending_repository`
   - repositories discovered but not yet reconciled into project config
   - supports both webhook-originated and user-scoped pending records

6. `config_schema.git_setup_session`
   - setup snapshot used to compare repository sets before/after install/connect flows

This is one of the biggest branch changes. Gecko is no longer stateless around Git operations; it is now persisting lifecycle and reconciliation state explicitly.

## HTTP Surface Changes

The route surface is much broader than before.

### Top-level registration

The new entrypoint is:

- `internal/server/http/register.go`

Registered route families:

- `/health`
- `/Dir...`
- `/config...`
- `/git...`
- vector routes
- swagger JSON route

### Config routes

`internal/server/http/config/register.go` now exposes:

#### Generic config routes

- `GET /config/types`
- `GET /config/list`

Typed config groups:

- `/config/explorer`
- `/config/nav`
- `/config/file_summary`
- `/config/project`

Per-type operations include combinations of:

- `GET /list`
- `GET /:configId`
- `PUT /:configId`
- `DELETE /:configId`

#### Project config routes

This branch adds a dedicated project config surface:

- `GET /config/projects`
- `GET /config/projects/list`
- `GET /config/projects/summary`
- `GET /config/projects/:orgTitle/:projectTitle`
- `PUT /config/projects/:orgTitle/:projectTitle`
- `DELETE /config/projects/:orgTitle/:projectTitle`
- `DELETE /config/projects/:orgTitle`

This is materially different from the old “config row by arbitrary key” model. The route shape now matches organization/project semantics directly.

### Git routes

`internal/server/http/git/register.go` is one of the largest new route families in the service.

#### Organization-level Git routes

- `GET /git/projects`
- `GET /git/organizations/status`
- `POST /git/organizations/reconcile`
- `POST /git/organizations/:orgTitle/init-connect`
- `POST /git/organizations/:orgTitle/connect`
- `GET /git/organizations/:orgTitle/status`
- `POST /git/organizations/:orgTitle/reconcile`

These routes cover installation status, connect flows, and organization-wide repository reconciliation.

#### Project-level Git read routes

- `GET /git/projects/:orgTitle/:projectTitle`
- `GET /git/projects/:orgTitle/:projectTitle/refs`
- `GET /git/projects/:orgTitle/:projectTitle/tree`
- `GET /git/projects/:orgTitle/:projectTitle/tree/*`
- `GET /git/projects/:orgTitle/:projectTitle/file/*`
- `GET /git/projects/:orgTitle/:projectTitle/download/*`
- `GET /git/projects/:orgTitle/:projectTitle/thumbnail`

These expose Gecko as a repository-backed project read service, not just a config API.

#### Project-level Git write/workflow routes

- `PUT /git/projects/:orgTitle/:projectTitle/setup`
- `PUT /git/projects/:orgTitle/:projectTitle/storage`
- `PUT /git/projects/:orgTitle/:projectTitle/thumbnail`
- `DELETE /git/projects/:orgTitle/:projectTitle/thumbnail`
- `POST /git/projects/:orgTitle/:projectTitle/edit-connect`
- `POST /git/projects/:orgTitle/:projectTitle/update`
- `POST /git/projects/:orgTitle/:projectTitle/uploads/session`
- `GET /git/projects/:orgTitle/:projectTitle/uploads/session/:sessionID`
- `POST /git/projects/:orgTitle/:projectTitle/uploads/session/:sessionID/files`
- `POST /git/projects/:orgTitle/:projectTitle/uploads/session/:sessionID/finalize`

This is a major expansion of product surface. Gecko is now responsible for project repository setup, update, artifact flow staging, and PR-style finalization.

## Auth and Access Model

The middleware layer has been significantly expanded.

### Resource path model

The branch standardizes project authorization around Arborist-style paths:

```text
/programs/{organization}/projects/{project}
```

Helper functions in `internal/server/middleware/access.go` normalize and check these resource paths.

### Config auth model

`ConfigAuth` now treats explorer configs differently from base/global config routes:

- explorer config access is project-scoped
- non-explorer GET routes are broadly readable
- non-explorer write/delete routes require route-specific authorization

### Project config auth model

`ProjectConfigAuth` checks direct access on:

```text
/programs/{org}/projects/{project}
```

and also allows certain broader admin-like resource paths such as:

- `*`
- `/programs`
- `/programs/{org}`
- `/programs/{org}/projects`

### Git auth model

Git reads and organization reads are protected separately:

- `GitProjectAuth`
- `GitOrganizationAuth`

The important boundary in this branch is:

- caller authorization still comes from the request `Authorization` token and Arborist/Fence checks
- Git remote access is not the same thing as caller auth and is handled separately through the Git integration flow

Project write routes are intentionally split:

- `PUT /git/projects/:orgTitle/:projectTitle/setup` should only require an `Authorization` header at the route layer
- setup then calls Arborist directly to determine whether the caller can read an existing project, create descendants under `/programs/{org}/projects`, manage owners on `/programs/{org}`, or bootstrap missing ownership resources
- Gecko should not pre-judge whether the org or project already exists before making that Arborist call
- follow-on mutation routes such as storage, thumbnail updates, connect edits, uploads, and update actions can use stricter project-scoped middleware because they operate after bootstrap

That split is correct because Gecko does not have enough local state to distinguish "missing org/project" from "existing but unauthorized"; Arborist is the authority for that decision.

## Fence / GitHub Integration Model

This branch adds explicit integration clients instead of spreading ad hoc HTTP logic through handlers.

### Fence integration

`internal/integrations/fence/client.go` turns Fence into a GitHub App broker that Gecko calls for:

- install URL requests
- organization installation status
- repository installation status
- installation repository listing
- installation token minting

The request target is:

- `POST {FENCE_BASE_URL}/credentials/github`

with action-based payloads.

This is architecturally important. Gecko is moving away from owning GitHub App secrets directly and toward asking Fence for short-lived GitHub access on demand.

### GitHub integration

`internal/integrations/github/client.go` uses:

- `github.com/google/go-github/v87/github`

for GitHub API metadata reads.

At minimum, it currently centralizes repository metadata lookup:

- default branch
- HTML URL

This is the right direction and avoids more hand-written GitHub REST client code.

## Git Service and Repository Semantics

The branch adds a large `internal/git/` package that now owns:

- repository identity/domain types
- setup and reconcile flows
- repository state persistence coordination
- update/sync operations
- upload workflows
- response shaping
- error mapping

The key product shift is that Gecko is no longer just proxying config or metadata. It now maintains local repository state under a configured data directory and serves project Git views from there.

Reviewers should pay special attention to:

- when local repository state is created
- how `update` behaves when the local repo is missing vs already present
- how default branch information is sourced and persisted
- where Fence tokens are requested and how long they are retained in memory

## Upload and Thumbnail Workflows

Two entirely new concerns land in this branch.

### Upload sessions

Upload state is now explicit and persistent:

- session creation
- file list replacement/storage
- session lookup
- finalize flow
- PR metadata persistence

This means Gecko now participates in a staged contribution workflow rather than just reading repository state.

### Thumbnails

`internal/thumbnail/` adds filesystem-backed thumbnail storage plus validation.

Route support includes:

- `GET /git/projects/:orgTitle/:projectTitle/thumbnail`
- `PUT /git/projects/:orgTitle/:projectTitle/thumbnail`
- `DELETE /git/projects/:orgTitle/:projectTitle/thumbnail`

That is a durable product-surface change and should be reviewed as such, not as a minor helper addition.

## Legacy Code Removal

A large part of the diff is deletion of the old flat handlers from `gecko/`, including legacy files such as:

- `handleConfig.go`
- `handleDir.go`
- `handleVector.go`
- `middleware.go`
- `response.go`
- `server.go`

This is not dead-code cleanup alone. These deletions are paired with replacements under `internal/server/http/...`, `internal/server/middleware/...`, and supporting packages.

## Build / Tooling Changes

This branch also touches:

- `Dockerfile`
- `Makefile`
- `.dockerignore`
- `go.mod`
- `go.sum`
- swagger/docs artifacts

Those are not side noise. They are part of the fallout from:

- root build compatibility
- package reorganization
- new GitHub client dependency
- swagger generation drift

Review should include a sanity pass on container build assumptions and root `go build .` behavior.

## Testing Changes

The branch adds or updates tests across multiple layers:

- config tests
- project config tests
- middleware tests
- Git service tests
- upload tests
- Fence integration tests
- thumbnail tests
- integration tests

Notable files include:

- `config/explorerConfig_test.go`
- `config/projectConfig_test.go`
- `internal/git/service_test.go`
- `internal/git/upload_test.go`
- `internal/integrations/fence/client_test.go`
- `internal/server/middleware/git_test.go`
- `internal/thumbnail/store_test.go`
- `tests/integration/*`

Given the branch size, the main review question is coverage shape rather than raw test count:

- do route tests reflect current auth semantics?
- do config tests lock frontend/backend schema compatibility?
- do Git tests cover first-time setup, update, and persistence transitions?

## Highest-Risk Areas

If reviewing this branch for merge readiness, focus here first:

1. **Auth correctness**
   - project vs organization vs generic config auth
   - read vs write route consistency

2. **Route compatibility**
   - does the route surface still match the frontend and revproxy expectations?

3. **DB/state lifecycle**
   - are new tables initialized everywhere Gecko runs?
   - are deletes/updates cleaning up related Git state correctly?

4. **Git/Fence boundary**
   - does Gecko request the right thing from Fence?
   - is Gecko still assuming too much GitHub App behavior locally?

5. **Config schema compatibility**
   - especially explorer config and project config alignment with the frontend

6. **Startup behavior**
   - new hard dependency on `git-data-dir`
   - optional integrations degrading cleanly when unset

## Bottom Line

This branch should be read as a backend expansion and service re-platforming branch, not as a narrow project-config feature.

The durable outcomes are:

- Gecko now has a first-class project config model.
- Gecko now has a real Git-backed project API surface.
- Gecko now persists Git lifecycle state instead of treating repo operations as transient.
- Gecko now relies on Fence as the GitHub App broker boundary.
- Gecko’s internal organization is materially better, but the branch is broad enough that compatibility review has to be disciplined.
