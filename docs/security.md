# Security

toro's security model in one sentence: **whoever can reach your Redis can do
anything your queue can do** — so secure the Redis, and toro takes care of not
amplifying that access.

## What toro guarantees

- **JSON-only serialization.** Job data, options, results and events are
  `json` in and out — never pickle. A compromised or shared Redis cannot
  achieve code execution through toro's deserialization.
- **No dynamic dispatch from Redis.** A job's `name` is a label, not an import
  path. The processor function is registered in your worker's code; nothing
  read from Redis decides what code runs.
- **No string-built commands.** Every state transition is a Lua script taking
  ids and payloads as arguments (`KEYS[]`/`ARGV[]`), and every published event
  is one `cjson.encode` document — there is no string interpolation into
  commands or event JSON anywhere.
- **Key-safe identifiers.** Custom job ids, scheduler ids and deduplication
  ids are validated (no `:`, no control characters) so two logically distinct
  ids can never collide into one Redis key.

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
  payloads cost memory in every worker that touches them — store big data
  elsewhere (object storage) and enqueue a reference.
- **Validate what you process.** Arriving as JSON doesn't make `job.data`
  trustworthy if multiple producers share the queue; validate shape and
  ranges in the handler like you would any external input.
