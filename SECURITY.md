# Security Policy

## Supported Versions

Security fixes are released only for the latest `1.x` version. Releases
before `1.0.0` are no longer maintained; upgrade to the latest `1.x`
release to receive a fix. The supported Python and Redis versions are
listed in [docs/versioning.md](docs/versioning.md).

| Version | Supported          |
| ------- | ------------------ |
| 1.x     | :white_check_mark: |
| 0.x     | :x:                |

## Reporting a Vulnerability

**Please do not open a public GitHub issue for security problems.**

Report vulnerabilities privately via GitHub's
[private vulnerability reporting](https://github.com/ilovepixelart/toro/security/advisories/new)
form. This routes the report directly to the maintainer through a private
advisory and keeps the details out of the public issue tracker until a fix
is available.

When reporting, please include:

- The affected version(s) of `toro-queue`, plus the Python, redis-py and
  Redis server versions
- A minimal reproduction (producer and worker code, job options, observed
  vs expected behavior)
- The impact you believe the issue has (code execution, data integrity,
  cross-queue access, denial of service, etc.)

## Response Expectations

- **Acknowledgement:** within 7 days of the report.
- **Triage and fix window:** targeted within 30 days for confirmed issues,
  depending on severity and complexity.
- **Disclosure:** coordinated via the GitHub advisory. A CVE will be
  requested where applicable, and a patched release will be published to
  PyPI before the advisory is made public.

## Scope

In scope:

- The `toro` package source in this repository.
- The published distribution on PyPI (`toro-queue`).

Out of scope:

- Vulnerabilities in redis-py or the Redis server itself: report those to
  [redis-py](https://github.com/redis/redis-py/security) or
  [Redis](https://github.com/redis/redis/security).
- Access that follows from reaching the Redis server. Whoever can reach
  the Redis a queue uses can do anything that queue can do; securing that
  Redis is the operator's part of the model.

## Security model

[docs/security.md](docs/security.md) describes what toro guarantees
(JSON-only serialization, no dynamic dispatch from Redis, no string-built
commands, key-safe identifiers), what the operator owns, and the limits
worth knowing.
