# Releasing LangGraph Go

LangGraph Go is a workspace of independently versioned Go modules. A release
uses one semantic version across the workspace, but nested modules require
path-prefixed Git tags.

For version `v0.1.0`, the coordinated tags are:

```text
v0.1.0
providers/v0.1.0
remote/v0.1.0
mcpclient/v0.1.0
observability/otel/v0.1.0
backend/temporal/v0.1.0
redis/v0.1.0
```

## Release checklist

1. Move relevant entries from `Unreleased` to a dated version in
   `CHANGELOG.md`.
2. Verify that every module's `go` directive and cross-module requirement is
   intentional.
3. Confirm consumer-safety rules in [docs/PUBLISHING.md](docs/PUBLISHING.md)
   (additive APIs, opt-in defaults, no secrets, PR into `main`).
4. Run `pwsh ./scripts/release.ps1 -Version vX.Y.Z` for a read-only release
   audit (requires a clean worktree).
5. Run the full workspace CI, including race, PostgreSQL, Redis, and Temporal
   integration jobs.
6. Merge the release PR into `main` before tagging when working on a branch.
7. Create the coordinated tags with
   `pwsh ./scripts/release.ps1 -Version vX.Y.Z -CreateTags`.
8. Push the tags only after reviewing them locally, then create the GitHub
   release from the root tag and changelog section.

The script never pushes tags. If tag creation must be undone before pushing,
delete only the exact local tags printed by its dry run.

## Compatibility policy

- Patch releases preserve exported APIs and persisted formats.
- Before v1.0, minor releases may change APIs, but require changelog and
  migration notes.
- Checkpoint, remote protocol, and database schema changes require explicit
  compatibility tests.
- Deprecated APIs remain for at least one minor release unless retaining them
  would preserve unsafe behavior.
