# Upgrading

Breaking changes by release, newest first, each with what to do about it.

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
`True`, a count and `{"count", "age"}` mean what they meant before.

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
  called after the trim times out. A trimmed flow child leaves its flow's tree; the
  parent's `children_results()` and the flow's progress counts are unaffected.
  Metrics are separate counters and keep counting.

### Schedulers inherit the queue's `default_job_options`

`add_scheduler()` ignored them: a queue with `default_job_options={"attempts": 3}`
ran its scheduled jobs with one attempt. They now merge under the scheduler's own
options, as they do for `add()`. Pass the option to `add_scheduler()` to keep a
scheduler on a different value.

### `JobOptions.keep_args` takes the default

`JobOptions.keep_args(opt)` is now `keep_args(opt, default)`, where `default` is the
count an unset option keeps.
