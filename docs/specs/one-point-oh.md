# 1.0: the contract

## Problem and outcome

Everything a 1.0 promises is built. What is missing is the promise: which names are
public, what may change under them, what a stored queue looks like across versions,
and what toro will never do. Without that, a caller cannot tell a supported API from
something that happened to work, and every future change is a judgement call made
twice.

The docs are not as complete as the contract needs either, and that is measurable
rather than a feeling: `url=`, the most basic connection option on both `Queue` and
`Worker`, is named nowhere in `docs/`; nor are `Queue.clear_departed`,
`Worker.check_stalled`, `ToroError`, or the seven exported type aliases.

Outcome: nothing new ships. What exists is frozen, written down, and checked by
something that fails when it drifts.

## Design

- **Public is what `toro/__init__.py` exports.** Anything else, including a module
  path, is internal and may change in a patch release. A test pins the list, so a
  name added or removed is a diff someone has to defend.
- **Semver, with the queue's own twist.** The API is versioned by semver in the
  ordinary way. The **data model** (the Redis keys and their shapes) gets a version of
  its own, because two toro versions share one Redis during any rolling upgrade, and
  that is the normal case rather than the exception.
- **The marker is read once per process, not once per call.** A queue stamps its model
  version on first use and refuses to run against a model from the future, naming both
  versions. Checking on every write would cost a round trip on the hot path to catch a
  condition that changes once, during an upgrade; checking once per process catches it
  in the same minute and costs nothing after.
- **An absent marker means 0.x.** A queue created before this release has no marker,
  and the first 1.0 write stamps it: the absence is information, not an error.
- **Deprecation is a release, not a surprise.** A public name that is going away warns
  for one minor release before it goes, and the upgrading page names it.
- **The docs are the contract's text**, so their completeness is checked mechanically:
  every exported name, every public method and every constructor option appears in the
  docs, or the test fails.

## Acceptance clauses

| ID | Behavior | Check |
|---|---|---|
| ON-001 | The public API is exactly `toro.__all__`; a test fails when an export appears or disappears without the list being updated. | `tests/unit/test_public_api.py::test_the_public_api_is_the_one_that_was_frozen` |
| ON-002 | Every exported name, public method and constructor option is named in the docs. | `::test_every_public_name_is_documented` |
| ON-003 | A queue stamps its data-model version on first use, and a library that finds a newer one refuses, naming both. | `tests/integration/test_data_model_version.py` |
| ON-004 | The check costs one round trip per process, not one per call. | `::test_the_model_is_checked_once_per_process` |
| ON-005 | A queue created before the marker existed is stamped on first write rather than rejected. | `::test_an_unmarked_queue_is_adopted` |
| ON-006 | The semver policy, the deprecation rule and what counts as public are written down. | `docs/versioning.md` |
| ON-007 | A migration page maps the sync queues' vocabulary onto toro's, and says what has no equivalent. | `docs/migrating.md` |
| ON-008 | The "just use Postgres" question has an honest answer, including when the answer is yes. | `docs/faq.md` |
| ON-009 | A security review of both repos, with findings fixed or written down. | done, and it found real holes in both. toro: an unbounded result decoded inside the finish script (a server-wide DoS whose script could not be killed), three ways an option of the wrong type left a job no API could reach, `remove_job` deleting the queue's own hashes, an unvalidated queue name that could share a namespace, an unbounded per-name metric, a flush that lost its batch on a dead connection, a model check that covered only the hot path, a dependency floor that did not run, and a cron that wrote before it refused. matador: CSRF off by default in the commonest mount shape, auto-docs outside `dependencies=`, unbounded rendering of attacker-written payloads, a page number that returned 500, and read-only drawing selection in search results. Each fixed with a test; the claims the reviews falsified are corrected on both security pages. |
| ON-010 | A rolling upgrade across the previous minor and this one, against one Redis, in both directions. | `tests/compat/rolling_upgrade.py`, run against the published 0.11.0: 50 jobs per direction, every one processed exactly once, none twice, neither side refusing the other. The old side reads no marker (it has none) and the new side reads `1` |

## Out of scope

- Any new feature. A 1.0 that adds something is a 0.x with a marketing budget.
- Long-term support branches. One line, forward.
- Renaming anything for tidiness. The freeze is of what is, not of what would have
  been nicer.

## Risks

- **A frozen API freezes mistakes too.** Anything that still feels wrong should change
  before the freeze, not after; the docs pass is where those surface.
- **A model marker cannot describe what came before it.** Absence means 0.x, so the
  first 1.0 to touch a queue stamps it, and an older library never checks at all: the
  guard protects future readers, not past ones.

## Tasks

| # | Clause | Work | Files | Test strategy |
|---|---|---|---|---|
| 1 | ON-001 | Freeze the export list | `tests/unit/test_public_api.py` | red first |
| 2 | ON-002 | The docs-coverage check, then the docs it fails on | `tests/unit/`, `docs/` | red first |
| 3 | ON-003, ON-004, ON-005 | The data-model marker | `toro/keys.py`, `toro/queue.py`, `toro/worker.py`, `toro/errors.py` | integration, red first |
| 4 | ON-006 | `docs/versioning.md` | `docs/` | review |
| 5 | ON-007, ON-008 | `docs/migrating.md`, `docs/faq.md` | `docs/` | review |
| 6 | ON-009 | Security review of toro and matador | both repos | evidence |
| 7 | ON-010 | Rolling upgrade proof | | evidence |
