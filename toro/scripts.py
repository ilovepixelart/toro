"""Lua scripts for atomic state transitions.

Every job state change goes through one of these scripts so transitions are
atomic on the Redis server: no client-side race windows between "check state"
and "act on state".

Job ordering is a single GLOBAL priority order: every waiting job lives in the
`prioritized` ZSET, scored by (priority, sequence). Higher `priority` number =
more urgent; ties break FIFO by an enqueue sequence counter (`pc`). There is no
separate "wait" fast lane - `priority 0` (the default) is simply the least urgent
band, ordered FIFO among itself.

Wakeup uses a base marker: producers do an idempotent `ZADD marker 0 "0"`, and an
idle worker blocks on `BZPOPMIN marker`. The marker only signals "there may be
work"; the actual job move is the atomic `MOVE_TO_ACTIVE` (ZPOPMIN prioritized ->
active -> lock), so a job is never lost between wakeup and claim.

Lua conventions (Redis best practices) followed here:
  * No globals - every variable is `local` (Redis rejects globals).
  * Deterministic - the clock (`now`) is always passed in via ARGV; scripts never
    call `TIME`/`random`, so they replicate and unit-test reproducibly.
  * `tonumber()` before any arithmetic on ARGV (which arrive as strings).
  * ZSET scores stay < 2^53 (the priority packing) so doubles stay exact.
  * Big fan-out is chunked (see MOVE_STALLED's `unpack` in 1000s) to respect Lua's
    argument limit.
SINGLE-NODE assumption: per-job keys are derived from the key `base` inside the
scripts (`base .. jobId`, `base .. "de:" .. id`) rather than all being passed via
KEYS[]. This keeps the scripts simple for a single Redis; running on Redis Cluster
would require hash-tagging the keys (e.g. `{queue}`) so a queue's keys share a slot.
"""

# Priority score packing constants (kept well under 2^53 so ZSET double scores
# stay exact). priority in [0, PRIORITY_OFFSET]; sequence in [0, SEQ_MOD).
PRIORITY_OFFSET = 1048576  # 2^20  - max priority (most urgent)
SEQ_MOD = 4294967296  # 2^32  - sequence wrap window

# Max due jobs one PROMOTE_DELAYED call moves (~6 Redis commands each). Bounds
# how long a sweep can block Redis (~3ms a batch); callers loop until a short
# batch signals the backlog is drained.
PROMOTE_BATCH = 1000

# How long per-minute metrics buckets live: enough history for dashboard
# charts, bounded key count (at most 480 small hashes per queue).
METRICS_RETENTION_MS = 8 * 60 * 60 * 1000

# Duration histogram shape: log-scaled buckets so one set covers 20ms jobs and
# 5-minute jobs alike. Bucket 0 is [0, 20ms); each next bucket grows 1.5x;
# the last bucket absorbs everything past ~5.6 minutes. Successful jobs only -
# failures have unpredictable timing and would read as fake regressions.
HIST_BASE_MS = 20
HIST_GROWTH = 1.5
HIST_BUCKETS = 26

# Lua → Python return protocol: sentinels the scripts emit, decoded in worker.py.
RL_SENTINEL = "__rl__"  # ACQUIRE hit the rate limiter; res[1] = ms until a token frees
LOCK_LOST = -2  # a finish script: the worker's lock was lost (job already reclaimed)
NOT_ACTIVE = -3  # a finish script: the job was no longer in `active`
OUTCOME_FAILED = 1  # MOVE_TO_FAILED outcome: terminally failed (vs 0 = will retry)

# Shared routines, prepended to every script that enqueues or acquires a job.
# This is the single definition of "how a job is ordered, woken, claimed":
#   priorityScore  - (priority, seq) -> ZSET score (lower score = sooner)
#   enqueue        - put a job into `prioritized` + arm the base marker
#   lockAndLoad    - lock a job already on `active`, stamp it, return its hash
#   acquireNext    - ZPOPMIN the next job into `active`, then lockAndLoad it
# To add markers-with-delay or grouping later, we change only these functions.
_LIB = """
local function priorityScore(priority, pcKey)
  local seq = redis.call("INCR", pcKey) % 4294967296
  return (1048576 - priority) * 4294967296 + seq
end
local function enqueue(prioritizedKey, markerKey, jobId, priority, pcKey)
  redis.call("ZADD", prioritizedKey, priorityScore(priority, pcKey), jobId)
  redis.call("ZADD", markerKey, 0, "0")
end
local function lockAndLoad(jobId, stalledKey, base, token, lockMs, now)
  local jobKey = base .. jobId
  redis.call("SET", jobKey .. ":lock", token, "PX", lockMs)
  redis.call("SREM", stalledKey, jobId)
  redis.call("HINCRBY", jobKey, "attemptsMade", 1)
  redis.call("HSET", jobKey, "processedOn", now, "state", "active")
  return {redis.call("HGETALL", jobKey), jobId}
end
-- Queue-wide token bucket (shared by all workers). capacity = maxJobs, refilled
-- at maxJobs/durationMs tokens per ms. Returns 0 if a token was consumed (allowed),
-- else the ms until one frees up. `now` is injected (never redis TIME) so the
-- script stays deterministic and unit-testable. maxJobs <= 0 disables it.
local function tryRateLimit(rlKey, maxJobs, durationMs, now)
  if not maxJobs or maxJobs <= 0 then return 0 end
  now = tonumber(now)
  local cur = redis.call("HMGET", rlKey, "tokens", "ts")
  local tokens = tonumber(cur[1])
  local ts = tonumber(cur[2])
  if tokens == nil then tokens = maxJobs; ts = now end
  local refill = maxJobs / durationMs
  tokens = math.min(maxJobs, tokens + (now - ts) * refill)
  if tokens >= 1 then
    redis.call("HSET", rlKey, "tokens", tokens - 1, "ts", now)
    redis.call("PEXPIRE", rlKey, durationMs + 1000)
    return 0
  end
  return math.ceil((1 - tokens) / refill)   -- ms until one token is available
end
local function acquireNext(prioritizedKey, activeKey, markerKey, stalledKey,
                           base, pcKey, metaKey, token, lockMs, now,
                           rlKey, rlMax, rlDuration)
  if redis.call("EXISTS", metaKey) == 1 then return false end  -- queue paused
  local res = redis.call("ZPOPMIN", prioritizedKey)
  if #res == 0 then
    redis.call("DEL", pcKey)
    return false
  end
  local jobId = res[1]
  -- Spend a token only once we actually have a job; if rate limited, put the job
  -- back untouched and tell the caller when to retry (it re-delays, never fails).
  local retry = tryRateLimit(rlKey, rlMax, rlDuration, now)
  if retry > 0 then
    redis.call("ZADD", prioritizedKey, res[2], jobId)
    return {"__rl__", retry}
  end
  redis.call("LPUSH", activeKey, jobId)
  if redis.call("ZCARD", prioritizedKey) > 0 then
    redis.call("ZADD", markerKey, 0, "0")   -- re-arm so another idle worker wakes
  end
  return lockAndLoad(jobId, stalledKey, base, token, lockMs, now)
end
local function delJobs(ids, base)
  for _, id in ipairs(ids) do
    redis.call("DEL", base .. id, base .. id .. ":lock", base .. id .. ":logs")
  end
end
-- Per-minute metrics bucket: one small hash per (queue, minute) holding
-- `added` / `completed` / `failed` counts and `ms` (summed processing
-- duration), each bucket self-expiring after retentionMs. Written here,
-- inside the same scripts as the transitions, so a counter can never
-- disagree with the transition it counts. When a job name is given, the
-- same counts also land in per-name fields ("completed:<name>", ...) so
-- a dashboard can answer "which job is responsible".
local function recordMetrics(base, field, now, durMs, retentionMs, name)
  local bucket = base .. "metrics:" .. tostring(math.floor(now / 60000) * 60000)
  redis.call("HINCRBY", bucket, field, 1)
  if durMs > 0 then redis.call("HINCRBY", bucket, "ms", durMs) end
  if name then
    redis.call("HINCRBY", bucket, field .. ":" .. name, 1)
    if durMs > 0 then redis.call("HINCRBY", bucket, "ms:" .. name, durMs) end
    -- duration histogram ("h:<name>:<bucketIdx>"), successful jobs only:
    -- log-scaled buckets, [0,20ms) then 1.5x each, overflow clamps to the last
    if field == "completed" then
      local idx = 0
      if durMs >= 20 then
        idx = math.min(25, math.floor(math.log(durMs / 20) / math.log(1.5)) + 1)
      end
      redis.call("HINCRBY", bucket, "h:" .. name .. ":" .. idx, 1)
    end
  end
  redis.call("PEXPIRE", bucket, retentionMs)
end
-- Record a terminal job in a finished set, applying auto-removal:
--   keepCount: -1 keep all, 0 remove immediately (don't record), N keep newest N
--   keepAge:   -1 no age limit, S keep only those finished within S seconds
local function recordFinished(setKey, jobKey, base, jobId, now, prop, val,
                              state, keepCount, keepAge)
  if keepCount == 0 and keepAge < 0 then
    redis.call("DEL", jobKey, jobKey .. ":logs")
    return
  end
  redis.call("ZADD", setKey, now, jobId)
  redis.call("HSET", jobKey, prop, val, "finishedOn", now, "state", state)
  if keepAge >= 0 then
    local cutoff = now - keepAge * 1000
    -- Bounded per call: enabling an age limit on a deep finished backlog must
    -- not sweep it all in one Redis-blocking pass - the remainder amortizes
    -- over subsequent finishes (same idea as PROMOTE_DELAYED's batch).
    local expired = redis.call("ZRANGEBYSCORE", setKey, "-inf", "(" .. cutoff,
                               "LIMIT", 0, 1000)
    if #expired > 0 then
      delJobs(expired, base)
      redis.call("ZREM", setKey, unpack(expired))
    end
  end
  if keepCount > 0 then
    delJobs(redis.call("ZREVRANGE", setKey, keepCount, -1), base)
    redis.call("ZREMRANGEBYRANK", setKey, 0, -(keepCount + 1))
  end
end
"""

# Add a job. With no custom id, generates one server-side (INCR) so concurrent
# producers never collide. With a custom id, the add is IDEMPOTENT: if a job with
# that id already exists it's returned unchanged (dedup).
# Publishes the "added" event from HERE (not a second client round trip) so a
# single round trip covers enqueue + dashboard wakeup; every return path
# announces, matching the previous always-publish behavior.
# KEYS[1] id counter  KEYS[2] prioritized  KEYS[3] marker  KEYS[4] delayed
# KEYS[5] key base  KEYS[6] pc (priority counter)  KEYS[7] events channel
# ARGV[1] name  ARGV[2] data(json)  ARGV[3] opts(json)
# ARGV[4] now(ms)  ARGV[5] delay(ms)  ARGV[6] priority  ARGV[7] custom id ("" = auto)
# ARGV[8] dedup id ("" = none)  ARGV[9] dedup ttl(ms)  -- throttle window
# ARGV[10] metricsRetention(ms)
ADD_JOB = (
    _LIB
    + """
local function announce(jobId)
  -- whole-message cjson.encode: no hand-built JSON anywhere on the event bus
  redis.call("PUBLISH", KEYS[7],
    cjson.encode({jobId = tostring(jobId), event = "added"}))
end
local base = KEYS[5]
-- Throttle dedup: within the TTL window, a repeat dedup id is ignored and the
-- already-queued job's id is returned (self-expiring, no finish-side cleanup).
local dedupKey
if ARGV[8] ~= "" then
  dedupKey = base .. "de:" .. ARGV[8]
  local existing = redis.call("GET", dedupKey)
  if existing then
    announce(existing)
    return existing
  end
end
local jobId = ARGV[7]
if jobId == "" then
  jobId = redis.call("INCR", KEYS[1])
elseif redis.call("EXISTS", base .. jobId) == 1 then
  announce(jobId)
  return jobId
end
local jobKey = base .. jobId
redis.call("HSET", jobKey,
  "id", jobId, "name", ARGV[1], "data", ARGV[2], "opts", ARGV[3],
  "timestamp", ARGV[4], "attemptsMade", 0, "priority", ARGV[6])
if dedupKey then
  redis.call("SET", dedupKey, jobId, "PX", tonumber(ARGV[9]))
  redis.call("HSET", jobKey, "deid", ARGV[8])
end
local delay = tonumber(ARGV[5])
if delay > 0 then
  redis.call("HSET", jobKey, "delay", delay, "state", "delayed")
  redis.call("ZADD", KEYS[4], tonumber(ARGV[4]) + delay, jobId)
else
  redis.call("HSET", jobKey, "state", "wait")
  enqueue(KEYS[2], KEYS[3], jobId, tonumber(ARGV[6]), KEYS[6])
end
-- only real inserts count (dedup hits and id replays returned above)
recordMetrics(base, "added", tonumber(ARGV[4]), 0, tonumber(ARGV[10]))
announce(jobId)
return jobId
"""
)

# Claim the next job: pop highest-priority from `prioritized` into `active`, lock
# it, and return its hash. The blocking BZPOPMIN on the marker only wakes the
# worker; THIS is the atomic move. Returns {jobHash, jobId} or nil if none.
# KEYS[1] prioritized  KEYS[2] active  KEYS[3] marker  KEYS[4] stalled
# KEYS[5] key base  KEYS[6] pc  KEYS[7] meta-paused  KEYS[8] limiter
# ARGV[1] token  ARGV[2] lockDuration(ms)  ARGV[3] now(ms)
# ARGV[4] rlMax (0 = no limit)  ARGV[5] rlDuration(ms)
# Returns false (none/paused), {jobHash, jobId}, or {"__rl__", retryMs} when rate limited.
MOVE_TO_ACTIVE = (
    _LIB
    + """
return acquireNext(KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5], KEYS[6], KEYS[7],
                   ARGV[1], tonumber(ARGV[2]), ARGV[3],
                   KEYS[8], tonumber(ARGV[4]), tonumber(ARGV[5]))
"""
)

# Renew a lock we still own. Token-guarded: we can NEVER renew a lock another
# worker has taken over. A successful renew also resets the stalled window.
# KEYS[1] lock key  KEYS[2] stalled set
# ARGV[1] token  ARGV[2] lockDuration(ms)  ARGV[3] jobId
EXTEND_LOCK = """
if redis.call("GET", KEYS[1]) == ARGV[1] then
  redis.call("SET", KEYS[1], ARGV[1], "PX", tonumber(ARGV[2]))
  redis.call("SREM", KEYS[2], ARGV[3])
  return 1
end
return 0
"""

# Commit a completed job, then (when fetch=1) acquire the next job in the SAME
# round trip. Token-guarded: a worker that lost its lock commits NOTHING.
# KEYS[1] active  KEYS[2] completed  KEYS[3] job hash  KEYS[4] lock
# KEYS[5] prioritized  KEYS[6] marker  KEYS[7] stalled  KEYS[8] base  KEYS[9] pc
# KEYS[10] events channel  KEYS[11] meta-paused  KEYS[12] limiter
# ARGV[1] jobId  ARGV[2] returnvalue(json)  ARGV[3] now(ms)  ARGV[4] token
# ARGV[5] fetch(1/0)  ARGV[6] lockDuration(ms)  ARGV[7] keepCount  ARGV[8] keepAge(s)
# ARGV[9] rlMax  ARGV[10] rlDuration(ms)  ARGV[11] metricsRetention(ms)
# Returns -2 lock lost, -3 not active, {1} committed, {1, nextHash, nextId}.
MOVE_TO_COMPLETED = (
    _LIB
    + """
if redis.call("GET", KEYS[4]) ~= ARGV[4] then return -2 end
redis.call("DEL", KEYS[4])
if redis.call("LREM", KEYS[1], 0, ARGV[1]) == 0 then return -3 end
local now = tonumber(ARGV[3])
-- read BEFORE recordFinished (remove-on-complete may DEL the hash)
local meta = redis.call("HMGET", KEYS[3], "processedOn", "name")
local startedOn = tonumber(meta[1]) or now
recordFinished(KEYS[2], KEYS[3], KEYS[8], ARGV[1], now,
  "returnvalue", ARGV[2], "completed", tonumber(ARGV[7]), tonumber(ARGV[8]))
recordMetrics(KEYS[8], "completed", now, now - startedOn, tonumber(ARGV[11]), meta[2])
-- the result is decoded and re-encoded as part of ONE cjson document: a
-- return value full of JSON metacharacters can never corrupt the message
local okr, resultDoc = pcall(cjson.decode, ARGV[2])
local completedMsg = {jobId = ARGV[1], event = "completed"}
if okr then completedMsg.result = resultDoc end
redis.call("PUBLISH", KEYS[10], cjson.encode(completedMsg))
if ARGV[5] == "1" then
  local nxt = acquireNext(KEYS[5], KEYS[1], KEYS[6], KEYS[7], KEYS[8], KEYS[9], KEYS[11],
                          ARGV[4], tonumber(ARGV[6]), ARGV[3],
                          KEYS[12], tonumber(ARGV[9]), tonumber(ARGV[10]))
  if nxt then
    if nxt[1] == "__rl__" then
      redis.call("ZADD", KEYS[6], 0, "0")   -- rate limited: wake a worker to re-check
    else
      return {1, nxt[1], nxt[2]}
    end
  end
end
return {1}
"""
)

# Decide a failed job's fate (retry vs `failed`), then fetch-next. Retries
# re-enqueue at the job's stored priority.
# KEYS[1] active  KEYS[2] prioritized  KEYS[3] delayed  KEYS[4] failed
# KEYS[5] job hash  KEYS[6] lock  KEYS[7] marker  KEYS[8] stalled  KEYS[9] base  KEYS[10] pc
# KEYS[11] events channel  KEYS[12] meta-paused  KEYS[13] limiter
# ARGV[1] jobId  ARGV[2] failedReason  ARGV[3] now(ms)  ARGV[4] attemptsMade
# ARGV[5] maxAttempts  ARGV[6] backoff(ms)  ARGV[7] token  ARGV[8] fetch(1/0)
# ARGV[9] lockDuration(ms)  ARGV[10] keepCount  ARGV[11] keepAge(s)
# ARGV[12] rlMax  ARGV[13] rlDuration(ms)  ARGV[14] metricsRetention(ms)
# Returns -2/-3, else {outcome} or {outcome, nextHash, nextId}; outcome 1=failed 0=retry.
MOVE_TO_FAILED = (
    _LIB
    + """
if redis.call("GET", KEYS[6]) ~= ARGV[7] then return -2 end
redis.call("DEL", KEYS[6])
if redis.call("LREM", KEYS[1], 0, ARGV[1]) == 0 then return -3 end
local attemptsMade = tonumber(ARGV[4])
local maxAttempts = tonumber(ARGV[5])
redis.call("HSET", KEYS[5], "failedReason", ARGV[2], "attemptsMade", attemptsMade)
local outcome
if attemptsMade < maxAttempts then
  local backoff = tonumber(ARGV[6])
  if backoff > 0 then
    redis.call("HSET", KEYS[5], "state", "delayed")
    redis.call("ZADD", KEYS[3], tonumber(ARGV[3]) + backoff, ARGV[1])
  else
    local priority = tonumber(redis.call("HGET", KEYS[5], "priority")) or 0
    redis.call("HSET", KEYS[5], "state", "wait")
    enqueue(KEYS[2], KEYS[7], ARGV[1], priority, KEYS[10])
  end
  outcome = 0
else
  local now = tonumber(ARGV[3])
  -- read BEFORE recordFinished (remove-on-fail may DEL the hash)
  local meta = redis.call("HMGET", KEYS[5], "processedOn", "name")
  local startedOn = tonumber(meta[1]) or now
  recordFinished(KEYS[4], KEYS[5], KEYS[9], ARGV[1], now,
    "failedReason", ARGV[2], "failed", tonumber(ARGV[10]), tonumber(ARGV[11]))
  recordMetrics(KEYS[9], "failed", now, now - startedOn, tonumber(ARGV[14]), meta[2])
  redis.call("PUBLISH", KEYS[11],
    cjson.encode({jobId = ARGV[1], event = "failed", reason = ARGV[2]}))
  outcome = 1
end
if ARGV[8] == "1" then
  local nxt = acquireNext(KEYS[2], KEYS[1], KEYS[7], KEYS[8], KEYS[9], KEYS[10], KEYS[12],
                          ARGV[7], tonumber(ARGV[9]), ARGV[3],
                          KEYS[13], tonumber(ARGV[12]), tonumber(ARGV[13]))
  if nxt then
    if nxt[1] == "__rl__" then
      redis.call("ZADD", KEYS[7], 0, "0")   -- rate limited: wake a worker to re-check
    else
      return {outcome, nxt[1], nxt[2]}
    end
  end
end
return {outcome}
"""
)

# Add a delayed job with a caller-provided id, idempotently. Used by schedulers:
# the deterministic id `repeat:<schedulerId>:<nextMillis>` means the same
# occurrence can never be enqueued twice. Returns 1 if added, 0 if it existed.
# KEYS[1] delayed  KEYS[2] key base
# ARGV[1] jobId  ARGV[2] name  ARGV[3] data(json)  ARGV[4] opts(json)
# ARGV[5] now(ms)  ARGV[6] processAt(ms)  ARGV[7] priority  ARGV[8] schedulerId
ADD_SCHEDULED = """
local jobKey = KEYS[2] .. ARGV[1]
if redis.call("EXISTS", jobKey) == 1 then return 0 end
redis.call("HSET", jobKey,
  "id", ARGV[1], "name", ARGV[2], "data", ARGV[3], "opts", ARGV[4],
  "timestamp", ARGV[5], "attemptsMade", 0, "priority", ARGV[7],
  "delay", tonumber(ARGV[6]) - tonumber(ARGV[5]), "state", "delayed",
  "schedulerId", ARGV[8])
redis.call("ZADD", KEYS[1], tonumber(ARGV[6]), ARGV[1])
return 1
"""

# Promote a delayed job to run now (admin/dashboard action).
# KEYS[1] delayed  KEYS[2] prioritized  KEYS[3] marker  KEYS[4] job hash  KEYS[5] pc
# ARGV[1] jobId
PROMOTE_JOB = (
    _LIB
    + """
if redis.call("ZREM", KEYS[1], ARGV[1]) == 0 then return 0 end
local priority = tonumber(redis.call("HGET", KEYS[4], "priority")) or 0
redis.call("HSET", KEYS[4], "state", "wait", "delay", 0)
enqueue(KEYS[2], KEYS[3], ARGV[1], priority, KEYS[5])
return 1
"""
)

# Re-queue a failed job for another attempt (admin/dashboard action).
# KEYS[1] failed  KEYS[2] prioritized  KEYS[3] marker  KEYS[4] job hash  KEYS[5] pc
# ARGV[1] jobId
RETRY_JOB = (
    _LIB
    + """
if redis.call("ZREM", KEYS[1], ARGV[1]) == 0 then return 0 end
redis.call("HDEL", KEYS[4], "failedReason", "finishedOn")
local priority = tonumber(redis.call("HGET", KEYS[4], "priority")) or 0
redis.call("HSET", KEYS[4], "state", "wait")
enqueue(KEYS[2], KEYS[3], ARGV[1], priority, KEYS[5])
return 1
"""
)

# Remove a job from wherever it lives and delete its hash (admin/dashboard action).
# KEYS[1] prioritized  KEYS[2] active  KEYS[3] delayed  KEYS[4] completed
# KEYS[5] failed  KEYS[6] job hash   ARGV[1] jobId
REMOVE_JOB = """
redis.call("ZREM", KEYS[1], ARGV[1])
redis.call("LREM", KEYS[2], 0, ARGV[1])
redis.call("ZREM", KEYS[3], ARGV[1])
redis.call("ZREM", KEYS[4], ARGV[1])
redis.call("ZREM", KEYS[5], ARGV[1])
redis.call("DEL", KEYS[6] .. ":lock", KEYS[6] .. ":logs")
return redis.call("DEL", KEYS[6])
"""

# Mark-and-sweep recovery of jobs whose worker died. Recovered jobs are
# re-enqueued at their stored priority; jobs past maxStalledCount go to `failed`.
# KEYS[1] stalled  KEYS[2] active  KEYS[3] prioritized  KEYS[4] failed
# KEYS[5] stalled-check  KEYS[6] key base  KEYS[7] marker  KEYS[8] pc
# ARGV[1] maxStalledCount  ARGV[2] now(ms)  ARGV[3] throttle(ms), 0 disables
# ARGV[4] metricsRetention(ms)
# Returns {failedIds, recoveredIds}.
MOVE_STALLED = (
    _LIB
    + """
local throttle = tonumber(ARGV[3])
if throttle > 0 then
  if redis.call("EXISTS", KEYS[5]) == 1 then return {{}, {}} end
  redis.call("SET", KEYS[5], ARGV[2], "PX", throttle)
end

local failed = {}
local recovered = {}
local stalling = redis.call("SMEMBERS", KEYS[1])
if #stalling > 0 then
  redis.call("DEL", KEYS[1])
  local maxStalled = tonumber(ARGV[1])
  for _, jobId in ipairs(stalling) do
    local jobKey = KEYS[6] .. jobId
    if redis.call("EXISTS", jobKey .. ":lock") == 0 then
      if redis.call("LREM", KEYS[2], 1, jobId) > 0 then
        local count = redis.call("HINCRBY", jobKey, "stalledCounter", 1)
        if count > maxStalled then
          redis.call("ZADD", KEYS[4], tonumber(ARGV[2]), jobId)
          redis.call("HSET", jobKey, "state", "failed",
            "failedReason", "job stalled more than allowable limit",
            "finishedOn", ARGV[2])
          recordMetrics(KEYS[6], "failed", tonumber(ARGV[2]), 0, tonumber(ARGV[4]),
                        redis.call("HGET", jobKey, "name"))
          table.insert(failed, jobId)
        else
          local priority = tonumber(redis.call("HGET", jobKey, "priority")) or 0
          redis.call("HSET", jobKey, "state", "wait")
          enqueue(KEYS[3], KEYS[7], jobId, priority, KEYS[8])
          table.insert(recovered, jobId)
        end
      end
    end
  end
end

local active = redis.call("LRANGE", KEYS[2], 0, -1)
local i = 1
while i <= #active do
  local j = math.min(i + 999, #active)
  redis.call("SADD", KEYS[1], unpack(active, i, j))
  i = j + 1
end
return {failed, recovered}
"""
)

# Move delayed jobs whose time has come into `prioritized` (at their priority),
# at most ARGV[2] per call so a big due-backlog can't block Redis for one long
# sweep - callers loop while a full batch comes back (see Worker._promote_loop).
# KEYS[1] delayed  KEYS[2] prioritized  KEYS[3] marker  KEYS[4] key base  KEYS[5] pc
# ARGV[1] now(ms)  ARGV[2] max jobs per call
PROMOTE_DELAYED = (
    _LIB
    + """
local jobs = redis.call("ZRANGEBYSCORE", KEYS[1], 0, ARGV[1], "LIMIT", 0, tonumber(ARGV[2]))
for _, jobId in ipairs(jobs) do
  redis.call("ZREM", KEYS[1], jobId)
  local jobKey = KEYS[4] .. jobId
  local priority = tonumber(redis.call("HGET", jobKey, "priority")) or 0
  redis.call("HSET", jobKey, "state", "wait", "delay", 0)
  enqueue(KEYS[2], KEYS[3], jobId, priority, KEYS[5])
end
return #jobs
"""
)
