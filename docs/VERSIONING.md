# Pre-1.0 versioning policy

langgraph-go follows [Semantic Versioning](https://semver.org/) with explicit
pre-1.0 rules so consumers can upgrade safely.

## Guarantees

| Release type | Promise |
|---|---|
| **Patch** (`v0.1.x`) | No breaking changes to exported APIs or documented on-disk formats without a security exception. |
| **Minor** (`v0.x.0`, x>1) | May add APIs; may change APIs **only** with CHANGELOG + migration notes. Prefer deprecation for one minor when possible. |
| **v1.0.0** | Stability bar: breaking changes require major versions thereafter. |

## Compatibility of persisted data

- Checkpoint codecs carry `Type` + `Version` fields; bump codec versions when
  encodings change incompatibly.
- Remote protocol version is advertised in `LangGraph-Protocol-Version`.
- Database migrations for Postgres/SQLite adapters are additive when possible;
  incompatible migrations are called out in the changelog.

## What “supported” means at v0.1

- CI (unit, race on core packages, optional integration jobs) must pass on `main`.
- Documented examples build without credentials.
- Security reports follow [SECURITY.md](../SECURITY.md).

## What is not promised yet

- Full Python LangGraph API parity ([COMPATIBILITY.md](../COMPATIBILITY.md))
- Long-term support of old minors (only latest `main` receives security fixes
  until an LTS policy is published)

## Consumer guidance

```bash
# Pin a version
go get github.com/ybszm/langgraph-go@v0.1.0

# Upgrade deliberately
go get github.com/ybszm/langgraph-go@v0.2.0
```

Avoid `@latest` in production modules without review.
