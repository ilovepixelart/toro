# Upgrading

Breaking changes by release, newest first, each with what to do about it.

## 0.8.0

### Upgrade every worker before you use a concurrency key

A key is taken when a job is enqueued and passed on by the script that commits the
job's finish, and that script is the one the WORKER registers. A worker from an
earlier release commits a finish without passing the key on, so the key stays held
by a job that is already done and every job behind it waits forever. Roll the whole
fleet to 0.8.0 first, then start passing `concurrency_key`. Nothing changes for a
queue that never passes one.

`held` is a new `JobState`, so anything that enumerates states (a dashboard, a
`counts()` reader, a `get_jobs` loop) sees a seventh. A held job waits on its key
rather than on a worker: it is in no other collection, `latency()` does not see it,
and `remove_job` is what cancels it.

## 0.7.0

### Finished jobs are bounded by default

To keep every finished job, as earlier releases did, set `False` for the queue on
every producer:

```python
queue = Queue(
    "emails",
    default_job_options={"remove_on_complete": False, "remove_on_fail": False},
)
```

`remove_on_complete` and `remove_on_fail` left unset used to keep every finished
job, so a queue on its defaults grew until Redis ran out of memory. Unset now
keeps the newest 1000 completed jobs and the newest 5000 failed jobs. `False`,
`True`, a count and `{"count", "age"}` mean what they meant before, with one
edge made whole: a count or an age is now floored to an integer wherever it is
given (`2.9` keeps 2), where a fractional bare count kept everything and a
fractional `count` in the dict form failed the finish inside Redis.

| | Before | Now |
|---|---|---|
| Option unset | keep every finished job | keep the newest 1000 completed, 5000 failed |
| `False` | keep every finished job | keep every finished job |
| A count bound meeting a deep backlog | deleted it all in one script | deletes at most 1000 jobs per finish |

- **This deletes history.** Upgrading the *workers* is what changes the behavior:
  retention is applied as a job finishes, under the options stored with it, jobs
  enqueued before the upgrade included. From the first finish on, history past the
  bound is deleted, oldest first, at most 1000 jobs per finish.
- **`False` on some jobs is not enough.** A trim covers the whole set, so a job kept
  with `False` is still trimmed by a later job that finishes under a bound. See
  [Retention](producing.md#retention).
- **Register your schedulers again.** A scheduler stores its job options when it is
  registered, and workers mint every occurrence from that stored template. Templates
  written by an earlier release store retention unset, so their jobs trim the queue
  whatever `default_job_options` says. `add_scheduler()` with the same id updates the
  template in place; from this release it merges the queue's defaults into it.
- **A custom `job_id` stays idempotent only while its job is kept.** Adding an id that
  exists is a no-op; once the job is trimmed the id is free, and the same `add()`
  enqueues the work again. On defaults that is after 1000 newer completions. Keep
  more history, or keep everything, on queues that rely on an id for longer.
- A trimmed job can no longer be read: `get_job()` returns `None` and `result()`
  called after the trim times out. Metrics are separate counters and keep
  counting.
- **A flow is retained as one unit.** While it runs, its finished children are out
  of every bound's reach; when its root is trimmed, the subtree goes with it. In
  `completed` and `failed` a flow child's score is its retention position, not
  its finish time (`finishedOn` is): a running flow's children score above every
  timestamp, a settled flow's children score just above their root.

### Schedulers inherit the queue's `default_job_options`

`add_scheduler()` ignored them: a queue with `default_job_options={"attempts": 3}`
ran its scheduled jobs with one attempt. They now merge under the scheduler's own
options, as they do for `add()`. Pass the option to `add_scheduler()` to keep a
scheduler on a different value.

### Some custom job ids are refused

`add()` raises `ValueError` for a `job_id` that would land on another key of the
queue: a queue key's name (`completed`, `marker`, ...), a queue namespace or its
bare name (`repeat:`, `worker:`, `metrics:`, `de:`), or another job's aux key
(`...:lock`, `:logs`, `:deps`, `:results`, `:cfail`). Such an id made the job's
hash and that key one Redis key: `completed` broke the queue with `WRONGTYPE`,
`prioritized` made `add()` return as if the job existed and enqueue nothing. A
job already stored under one of these ids keeps working; only `add()` refuses
it, including a repeat `add()` of that job. Colons are otherwise allowed. See
[Custom ids](producing.md#custom-ids-and-deduplication).

### `JobOptions.keep_args` is removed

It mapped a remove option to two arguments of the finish scripts. The scripts now
read the option from the job themselves, so nothing computes it on the Python side.
