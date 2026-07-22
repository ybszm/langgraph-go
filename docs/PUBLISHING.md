# Publishing without breaking consumers

This guide is for maintainers preparing a GitHub-visible revision that others
may `go get`. It complements [RELEASING.md](../RELEASING.md).

## Consumer safety rules

1. **Module path stays** `github.com/ybszm/langgraph-go` (and nested modules under
   that path). Do not rename published import paths without a major migration.
2. **Additive APIs only** on patch releases. New symbols are fine; renaming or
   removing exported APIs requires a minor version and changelog notes before
   v1.0.
3. **Opt-in behavior**. Defaults must preserve previous semantics
   (example: unreachable-node compile checks stay strict unless
   `WithAllowUnreachableNodes` is set).
4. **No credentials** in tree: `.env`, keys, and local DBs are gitignored.
5. **`go.work` is for this monorepo only**. Library consumers never need it;
   they import individual modules via `go get`.
6. **Optional modules stay optional**. Core `go.mod` must not pull Temporal,
   Redis, OTel, or providers transitively.

## Pre-push checklist

```powershell
# From repository root
go test ./... -count=1
go test -race ./graph ./prebuilt ./checkpoint/... ./compat -count=1
go vet ./...
pwsh ./scripts/release.ps1 -Version v0.1.0   # requires clean tree
```

Also verify:

- [ ] `CHANGELOG.md` has a dated section for the version you will tag
- [ ] `COMPATIBILITY.md` and `docs/DURABILITY.md` match reality
- [ ] `python学习文档/` remains documented as teaching-only (not a dependency)
- [ ] Branch is pushed; prefer PR into `main` over force-pushing `main`
- [ ] CI is green on the PR
- [ ] Tags are created only after merge (see RELEASING.md); never rewrite
  already-pushed tags

## Suggested GitHub flow (does not disturb other users)

1. Push a feature or release branch (for example `agent/prebuilt-agent-harness`
   or `release/v0.1.0`).
2. Open a PR into `main`.
3. After merge, tag from `main` and create a GitHub Release from the root tag
   and changelog section.
4. Consumers upgrade only when they choose a new version; old module versions
   remain immutable on the proxy.

## What “v0.1.0” means

`v0.1.0` is the first **supported development release**. It is usable, tested,
and documented, but still pre-1.0: minor releases may refine APIs with changelog
entries. Downstream projects should pin a version in `go.mod` rather than using
`@latest` without review.
