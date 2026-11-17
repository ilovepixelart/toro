# Contributing

Thanks for helping improve toro, an async-first, Redis-backed job queue for
Python. The [docs](docs/index.md) explain the architecture and the reliability
model; [docs/architecture.md](docs/architecture.md) is the place to start before
changing a state transition.

## Getting started

Requirements: Python 3.10 or newer, [uv](https://docs.astral.sh/uv/), and, for
the Redis-backed layers, a Redis server (6.2 or newer) that you own, named by the
`REDIS_URL` environment variable.

```bash
git clone https://github.com/ilovepixelart/toro
cd toro
uv sync                                   # venv + deps + dev group
docker run -d --name toro-redis -p 127.0.0.1:6391:6379 redis:7-alpine   # a Redis for the tests
export REDIS_URL=redis://localhost:6391
```

Tests write under the `torotest` key prefix, but use a Redis of its own for
them rather than one that holds real data. There is deliberately no default:
port 6379 on a development machine often belongs to another project, so the
suite never assumes it.

## Checks

The toolchain is Astral's throughout: uv for the environment, ruff for lint and
format, ty for types. CI runs these on every pull request, on every supported
Python:

```bash
uv run ruff check .                        # lint (strict: select = ALL)
uv run ruff format --check .               # format (drop --check to apply)
uv run ty check                            # type check
uv run pytest -m "unit or integration"     # tests
```

The suite is a pyramid, marked by folder (`tests/conftest.py` applies the
marker):

| Marker | Folder | Needs Redis | Runs |
| --- | --- | --- | --- |
| `unit` | `tests/unit` | no | in CI |
| `integration` | `tests/integration` | yes | in CI |
| `load` | `tests/load` | yes | not in CI: `uv run pytest -m load` |
| `perf` | `tests/perf` | yes | excluded by default; CI runs it as its own step: `uv run pytest -m perf` |

`-m unit` is fast and needs nothing running. The integration, load and perf
layers skip when `REDIS_URL` is unset ("set REDIS_URL to a Redis you own"), and
integration and load also skip when no Redis answers on it, so a green run
without `REDIS_URL` has not run them. The scripts in `tests/load/harness.py`,
`tests/perf/harness.py` and `tests/compat/rolling_upgrade.py` read the same
variable and refuse to start without it.
CI also runs the unit and integration layers against the oldest Redis and
redis-py the project supports.

## Making a change

- Open an issue first for anything beyond a small fix, so the approach can be
  agreed before the work.
- Keep each pull request to one focused change, with tests that fail without it.
  A state transition is a Lua script in `toro/scripts.py`, so its tests belong in
  the integration layer, against a real Redis.
- The public API is everything `toro.__all__` exports, and the Redis key layout
  is versioned separately ([docs/versioning.md](docs/versioning.md)). A change
  that breaks either needs a major release; say so in the pull request, and add
  the entry to [docs/upgrading.md](docs/upgrading.md).
- Update the docs page that describes any behavior you change.

## Reporting bugs and security issues

Open an issue for a bug or a feature idea. For anything security-sensitive, do
not open a public issue: report it privately through
[GitHub Security Advisories](https://github.com/ilovepixelart/toro/security/advisories/new),
as described in [SECURITY.md](SECURITY.md).

By contributing you agree to the [Code of Conduct](CODE_OF_CONDUCT.md) and that
your work is licensed under the project's [MIT license](LICENSE).
