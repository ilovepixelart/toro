"""Lua scripts for atomic state transitions.

Every job state change goes through one of these scripts so transitions are
atomic on the Redis server: no client-side race windows between "check state"
and "act on state".

Job ordering is a single GLOBAL priority order: every waiting job lives in the
`prioritized` ZSET, scored by (priority, sequence). Higher `priority` number =
more urgent; ties break FIFO by an enqueue sequence counter (`pc`). There is no
separate "wait" fast lane — `priority 0` (the default) is simply the least urgent
band, ordered FIFO among itself.

Wakeup uses a base marker: producers do an idempotent `ZADD marker 0 "0"`, and an
idle worker blocks on `BZPOPMIN marker`. The marker only signals "there may be
work"; the actual job move is the atomic `MOVE_TO_ACTIVE` (ZPOPMIN prioritized ->
active -> lock), so a job is never lost between wakeup and claim.
"""

# Priority score packing constants (kept well under 2^53 so ZSET double scores
# stay exact). priority in [0, PRIORITY_OFFSET]; sequence in [0, SEQ_MOD).
PRIORITY_OFFSET = 1048576      # 2^20  — max priority (most urgent)
SEQ_MOD = 4294967296           # 2^32  — sequence wrap window

# Shared routines, prepended to every script that enqueues or acquires a job.
# This is the single definition of "how a job is ordered, woken, claimed":
#   priorityScore  — (priority, seq) -> ZSET score (lower score = sooner)
#   enqueue        — put a job into `prioritized` + arm the base marker
#   lockAndLoad    — lock a job already on `active`, stamp it, return its hash
#   acquireNext    — ZPOPMIN the next job into `active`, then lockAndLoad it
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
local function acquireNext(prioritizedKey, activeKey, markerKey, stalledKey,
                           base, pcKey, token, lockMs, now)
  local res = redis.call("ZPOPMIN", prioritizedKey)
  if #res == 0 then
    redis.call("DEL", pcKey)
    return false
  end
  local jobId = res[1]
  redis.call("LPUSH", activeKey, jobId)
  if redis.call("ZCARD", prioritizedKey) > 0 then
    redis.call("ZADD", markerKey, 0, "0")   -- re-arm so another idle worker wakes
  end
  return lockAndLoad(jobId, stalledKey, base, token, lockMs, now)
end
"""

# Add a job. Generates the id server-side so concurrent producers never collide.
# KEYS[1] id counter  KEYS[2] prioritized  KEYS[3] marker  KEYS[4] delayed
# KEYS[5] key base  KEYS[6] pc (priority counter)
# ARGV[1] name  ARGV[2] data(json)  ARGV[3] opts(json)
# ARGV[4] now(ms)  ARGV[5] delay(ms)  ARGV[6] priority
ADD_JOB = _LIB + """
local jobId = redis.call("INCR", KEYS[1])
local jobKey = KEYS[5] .. jobId
redis.call("HSET", jobKey,
  "id", jobId, "name", ARGV[1], "data", ARGV[2], "opts", ARGV[3],
  "timestamp", ARGV[4], "attemptsMade", 0, "priority", ARGV[6])
local delay = tonumber(ARGV[5])
if delay > 0 then
  redis.call("HSET", jobKey, "delay", delay, "state", "delayed")
  redis.call("ZADD", KEYS[4], tonumber(ARGV[4]) + delay, jobId)
else
  redis.call("HSET", jobKey, "state", "wait")
  enqueue(KEYS[2], KEYS[3], jobId, tonumber(ARGV[6]), KEYS[6])
end
return jobId
"""

# Claim the next job: pop highest-priority from `prioritized` into `active`, lock
# it, and return its hash. The blocking BZPOPMIN on the marker only wakes the
# worker; THIS is the atomic move. Returns {jobHash, jobId} or nil if none.
# KEYS[1] prioritized  KEYS[2] active  KEYS[3] marker  KEYS[4] stalled
# KEYS[5] key base  KEYS[6] pc
# ARGV[1] token  ARGV[2] lockDuration(ms)  ARGV[3] now(ms)
MOVE_TO_ACTIVE = _LIB + """
return acquireNext(KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5], KEYS[6],
                   ARGV[1], tonumber(ARGV[2]), ARGV[3])
"""

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
# ARGV[1] jobId  ARGV[2] returnvalue(json)  ARGV[3] now(ms)  ARGV[4] token
# ARGV[5] fetch(1/0)  ARGV[6] lockDuration(ms)
# Returns -2 lock lost, -3 not active, {1} committed, {1, nextHash, nextId}.
MOVE_TO_COMPLETED = _LIB + """
if redis.call("GET", KEYS[4]) ~= ARGV[4] then return -2 end
redis.call("DEL", KEYS[4])
if redis.call("LREM", KEYS[1], 0, ARGV[1]) == 0 then return -3 end
redis.call("ZADD", KEYS[2], tonumber(ARGV[3]), ARGV[1])
redis.call("HSET", KEYS[3],
  "returnvalue", ARGV[2], "finishedOn", ARGV[3], "state", "completed")
if ARGV[5] == "1" then
  local nxt = acquireNext(KEYS[5], KEYS[1], KEYS[6], KEYS[7], KEYS[8], KEYS[9],
                          ARGV[4], tonumber(ARGV[6]), ARGV[3])
  if nxt then return {1, nxt[1], nxt[2]} end
end
return {1}
"""

# Decide a failed job's fate (retry vs `failed`), then fetch-next. Retries
# re-enqueue at the job's stored priority.
# KEYS[1] active  KEYS[2] prioritized  KEYS[3] delayed  KEYS[4] failed
# KEYS[5] job hash  KEYS[6] lock  KEYS[7] marker  KEYS[8] stalled  KEYS[9] base  KEYS[10] pc
# ARGV[1] jobId  ARGV[2] failedReason  ARGV[3] now(ms)  ARGV[4] attemptsMade
# ARGV[5] maxAttempts  ARGV[6] backoff(ms)  ARGV[7] token  ARGV[8] fetch(1/0)
# ARGV[9] lockDuration(ms)
# Returns -2/-3, else {outcome} or {outcome, nextHash, nextId}; outcome 1=failed 0=retry.
MOVE_TO_FAILED = _LIB + """
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
  redis.call("ZADD", KEYS[4], tonumber(ARGV[3]), ARGV[1])
  redis.call("HSET", KEYS[5], "finishedOn", ARGV[3], "state", "failed")
  outcome = 1
end
if ARGV[8] == "1" then
  local nxt = acquireNext(KEYS[2], KEYS[1], KEYS[7], KEYS[8], KEYS[9], KEYS[10],
                          ARGV[7], tonumber(ARGV[9]), ARGV[3])
  if nxt then return {outcome, nxt[1], nxt[2]} end
end
return {outcome}
"""

# Re-queue a failed job for another attempt (admin/dashboard action).
# KEYS[1] failed  KEYS[2] prioritized  KEYS[3] marker  KEYS[4] job hash  KEYS[5] pc
# ARGV[1] jobId
RETRY_JOB = _LIB + """
if redis.call("ZREM", KEYS[1], ARGV[1]) == 0 then return 0 end
redis.call("HDEL", KEYS[4], "failedReason", "finishedOn")
local priority = tonumber(redis.call("HGET", KEYS[4], "priority")) or 0
redis.call("HSET", KEYS[4], "state", "wait")
enqueue(KEYS[2], KEYS[3], ARGV[1], priority, KEYS[5])
return 1
"""

# Remove a job from wherever it lives and delete its hash (admin/dashboard action).
# KEYS[1] prioritized  KEYS[2] active  KEYS[3] delayed  KEYS[4] completed
# KEYS[5] failed  KEYS[6] job hash   ARGV[1] jobId
REMOVE_JOB = """
redis.call("ZREM", KEYS[1], ARGV[1])
redis.call("LREM", KEYS[2], 0, ARGV[1])
redis.call("ZREM", KEYS[3], ARGV[1])
redis.call("ZREM", KEYS[4], ARGV[1])
redis.call("ZREM", KEYS[5], ARGV[1])
return redis.call("DEL", KEYS[6])
"""

# Mark-and-sweep recovery of jobs whose worker died. Recovered jobs are
# re-enqueued at their stored priority; jobs past maxStalledCount go to `failed`.
# KEYS[1] stalled  KEYS[2] active  KEYS[3] prioritized  KEYS[4] failed
# KEYS[5] stalled-check  KEYS[6] key base  KEYS[7] marker  KEYS[8] pc
# ARGV[1] maxStalledCount  ARGV[2] now(ms)  ARGV[3] throttle(ms), 0 disables
# Returns {failedIds, recoveredIds}.
MOVE_STALLED = _LIB + """
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

# Move every delayed job whose time has come into `prioritized` (at its priority).
# KEYS[1] delayed  KEYS[2] prioritized  KEYS[3] marker  KEYS[4] key base  KEYS[5] pc
# ARGV[1] now(ms)
PROMOTE_DELAYED = _LIB + """
local jobs = redis.call("ZRANGEBYSCORE", KEYS[1], 0, ARGV[1])
for _, jobId in ipairs(jobs) do
  redis.call("ZREM", KEYS[1], jobId)
  local jobKey = KEYS[4] .. jobId
  local priority = tonumber(redis.call("HGET", jobKey, "priority")) or 0
  redis.call("HSET", jobKey, "state", "wait", "delay", 0)
  enqueue(KEYS[2], KEYS[3], jobId, priority, KEYS[5])
end
return #jobs
"""
