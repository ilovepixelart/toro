# Versioning

What you can rely on, what can change under you, and how you will hear about it.

## What is public

**Everything `import toro` exports, and nothing else.**

```python
import toro
toro.__all__   # the whole contract
```

A name that is not in that list is internal, whatever it looks like: a module path
(`toro.scripts`, `toro.keys`), a method with no underscore on an internal class, an
attribute you can reach. Those change in patch releases without notice.

The Lua in `toro/scripts.py` is internal by the same rule. The **Redis key layout** is
not internal (an operator inspects it, a dashboard reads it), but it is versioned
separately: see [Data model](#the-data-model-has-its-own-version).

## Semver, as it says on the tin

| Change | Version |
|---|---|
| A new public name, a new option with a default | minor |
| A public name or option removed, a default changed, behavior a correct caller would notice | major |
| A fix that makes the code do what it already documented | patch |
| A change to internals, however large | patch |

A default that changes is a major change even when the new default is better. The
0.7 retention flip was exactly that, and it is why the upgrading page exists.

## Deprecation

A public name on its way out warns for **one minor release** before it goes:

```
DeprecationWarning: Queue.foo() is deprecated and will be removed in 2.0; use bar()
```

The warning names the replacement and the release that removes it, and
[Upgrading](upgrading.md) lists it. A deprecation never appears in a patch release,
and nothing is removed without one.

## The data model has its own version

Two toro versions share one Redis during any rolling upgrade, so the keys have a
version of their own, stamped on the queue the first time a 1.0+ library writes to
it. A library that finds a **newer** model than it understands refuses to run and
says both numbers, rather than reading a shape it was not built for:

```
IncompatibleDataModelError: queue "emails" uses data model 2; this toro understands 1
```

An absent marker means the queue predates 1.0, and the first write adopts it. The
number this library understands is `toro.DATA_MODEL_VERSION`, and the queue's own is
the `model` field of its `meta` hash ([Data model](data-model.md)):

```python
import toro
print(toro.DATA_MODEL_VERSION)                      # what this library writes
await queue.redis.hget(queue.keys.meta, "model")    # what the queue says
```

The model version changes only when the stored shape changes incompatibly, which is a
major release of the library too. Upgrading the workers of one queue in a rolling
fashion is supported for **one minor version at a time**: that is the window the
tests cover.

## Supported Pythons

Every version supported upstream, which today is 3.10 through 3.13. A Python that
reaches end of life is dropped in the next minor release, and that is not a major
change: the language moved, not the API.

## Redis

**Redis 6.2 and later.** The floor is one command: `ZDIFFSTORE`, which the root-first
listings use and which arrived in 6.2. Everything else toro runs is older than that.
A Redis-compatible server that implements the same commands and scripting works by
construction, though only Redis itself is tested here.

The floor moves only in a minor release, and only for a feature that cannot be
written without it.
