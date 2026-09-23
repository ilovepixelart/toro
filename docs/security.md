# Security

toro's security model in one sentence: **whoever can reach your Redis can do
anything your queue can do** - so secure the Redis, and toro takes care of not
amplifying that access.

## What toro guarantees

- **JSON-only serialization.** Job data, options, results and events are
  `json` in and out - never pickle. A compromised or shared Redis cannot
  achieve code execution through toro's deserialization.
- **No dynamic dispatch from Redis.** A job's `name` is a label, not an import
  path. The processor function is registered in your worker's code; nothing
  read from Redis decides what code runs.
- **No string-built commands.** Every state transition is a Lua script taking ids and
  payloads as arguments (`KEYS[]`/`ARGV[]`), and no command name is ever built from
  data. Event payloads are `cjson.encode` documents, with one exception: the cancel
  channel carries `"<jobId>:<processedOn>"`, a two-field message whose second half is
  always digits. Key *names* are assembled inside the scripts from values read out of
  Redis (`base .. "ck:" .. key`), which is how one script addresses many keys; all of
  them sit under the queue's own base.
- **Key-safe identifiers.** The prefix, the queue name, scheduler ids, deduplication
  ids and `concurrency_key` are all validated as key segments: non-empty, bounded, no
  control characters, and no `:` except in the prefix, which is the one segment
  allowed to carry its own namespace. Without that rule two different (prefix, name)
  pairs could be one namespace, and one queue's worker could expire another's locks.
  A custom job id may contain `:`, may not contain `/` (it is a path segment in any
  dashboard), and is refused when it would land on another key of the queue: a queue
  key's name, a queue namespace, or another job's aux key.
- **Bounded by construction.** A job name is a label (128 characters), and the
  per-minute by-name breakdown stops taking new names past 1024 fields, so a name with
  an id in it cannot grow the metrics without limit. A return value over 16 KiB is
  read from the hash rather than decoded and re-encoded inside the finish script,
  which is what kept one job from blocking the whole server.

## What you own

- **Redis access control.** Use a password (`requirepass`/ACLs), network
  isolation, and `rediss://` URLs with a trusted CA for anything that crosses
  a network boundary. For custom TLS options, build the connection yourself
  and pass it as `connection=`.
- **Secrets in job payloads and exceptions.** Payloads, return values, failure
  reasons and stack traces are stored in Redis and visible to anything that
  can read it (including dashboards). Don't put credentials in `data`, and
  don't interpolate secrets into exception messages.
- **Payload discipline.** toro does not enforce a payload size limit. Large
  payloads cost memory in every worker that touches them - store big data
  elsewhere (object storage) and enqueue a reference.
- **Validate what you process.** Arriving as JSON doesn't make `job.data`
  trustworthy if multiple producers share the queue; validate shape and
  ranges in the handler like you would any external input.
- **What a reader of your Redis learns.** Presence records carry each worker's
  hostname and pid, and a worker's lock token is its own id, stored in plain sight:
  the token fences a lock against a stale worker, not against anyone who can write to
  Redis. Per-job logs have no cap and no TTL of their own; they go when the job does.

## Limits worth knowing

None of these is a hole, and each is a shape to design around:

- **A `concurrency_key` is a queue of its own.** The key is taken at enqueue, so a job
  delayed by an hour holds its key for that hour and everything sharing it waits. If
  the key comes from user input, that is a one-call stall of everything on it.
- **The root-first listings scan the state set.** `roots_counts()` and
  `get_jobs_roots()` diff the whole set; at tens of millions of retained finished jobs
  that is a slow script. Bounded retention (the default) keeps it far from that.
- **A `pending()` buffer has no ceiling.** It is a per-transaction object by design;
  holding one across requests grows it without limit.
- **`search()` is a bounded scan, not an index.** The bound is the caller's
  `scan_limit`, and a big one is a big pipelined read.
