# Enqueue when the transaction commits, not before

## Problem and outcome

The usual shape of a producer is a database write and an enqueue:

```python
user = User(email=...)
session.add(user)
await session.commit()
await queue.add("welcome", {"user_id": user.id})
```

Written the other way round, or with the enqueue anywhere inside the transaction, a
rollback leaves a job referring to a row that does not exist. The worker picks it up,
cannot find the row, and fails or, worse, treats the absence as an empty result. This
is the single most common producer bug in every queue's issue tracker, and the people
arriving from the sync queues expect their framework to have an answer for it
(`transaction.on_commit`, or the equivalent).

Outcome: jobs can be collected during a transaction and sent only if it commits, in
one round trip, with one line in the commit path.

## Design

- **A buffer, not a database integration.** toro has no database dependency and is
  not going to grow one. `queue.pending()` returns a collector with the same `add`
  and `add_flow` signatures, which records intents; `await pending.flush()` sends
  them. Where the flush is called from is the host app's business, and the docs show
  the hook for SQLAlchemy and for Django.
- **One round trip.** A flush pipelines every collected add, so collecting ten jobs
  costs what one costs, not ten. That also makes "flush after commit" cheap enough
  that nobody is tempted to skip it.
- **Ids are minted at flush, not at collection.** An id is a Redis counter value; a
  job that is never sent must not consume one, and a `job_id=` a caller supplies is
  honored as it is today.
- **Nothing is sent twice.** A flush empties the buffer, so a second flush sends
  nothing. A buffer that is discarded, or garbage collected, sends nothing: silence
  is what a rollback should produce.
- **What this does not do**, stated plainly because the gap is the interesting part:
  it does not survive the process dying between the commit and the flush. Closing
  that needs an outbox table and a relay, which is a database integration and a
  different product. The half this closes is the half people hit: a job for a row
  that was rolled back.

## Acceptance clauses

| ID | Behavior | Check |
|---|---|---|
| EC-001 | `pending()` collects `add()` calls and enqueues nothing until `flush()`. | `tests/integration/test_pending.py::test_nothing_is_enqueued_until_the_flush` |
| EC-002 | `flush()` enqueues every collected job, in order, and returns the jobs with their ids. | `::test_a_flush_sends_everything_in_order` |
| EC-003 | A flush is one round trip whatever the count. | `::test_a_flush_is_one_round_trip` |
| EC-004 | A buffer that is never flushed, or is discarded, enqueues nothing and consumes no job id. | `::test_a_rollback_sends_nothing_and_burns_no_id` |
| EC-005 | Flushing twice sends nothing the second time. | `::test_a_second_flush_is_a_no_op` |
| EC-006 | Options behave exactly as on `add()`: delay, priority, custom ids, dedup, and `add_flow` trees. | `::test_the_options_are_the_same_ones` |
| EC-007 | A flush that fails leaves the buffer intact, so the caller can retry it. | `::test_a_failed_flush_keeps_what_it_could_not_send` |

## Out of scope

- An outbox table, a relay, or any database dependency.
- Hooking a specific ORM's session events for the user. The docs show the two lines;
  guessing at session lifecycles from inside a queue is how integrations rot.
- Two-phase commit. Nobody is asking for it, and Redis is not a participant.

## Risks

- **It looks like a transaction and is not.** The name, the docs and the spec all say
  the same thing: it defers, it does not guarantee. A caller who needs delivery after
  a crash needs an outbox.
- **A buffer held across requests would grow.** It is a per-transaction object by
  design; the docs show it created and flushed in the same scope.

## Tasks

| # | Clause | Work | Files | Test strategy |
|---|---|---|---|---|
| 1 | EC-001, EC-002, EC-004, EC-005 | The buffer and its flush | `toro/queue.py` | integration, red first |
| 2 | EC-003 | Pipelined flush | `toro/queue.py` | counted, red first |
| 3 | EC-006, EC-007 | Options and failure | `toro/queue.py` | integration |
| 4 | | Docs: producing, with the SQLAlchemy and Django hooks | `docs/producing.md` | review |
