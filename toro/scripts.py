"""Lua scripts for atomic state transitions.

Every job state change (wait -> active -> completed/failed/delayed) goes through
one of these scripts so transitions are atomic on the Redis server: no
client-side race windows between "check state" and "act on state".
"""

# Add a job. Generates the id server-side so concurrent producers never collide.
# KEYS[1] = id counter   KEYS[2] = wait list   KEYS[3] = delayed zset
# KEYS[4] = key base (job hash key = base .. id)
# ARGV[1] = name  ARGV[2] = data(json)  ARGV[3] = opts(json)
# ARGV[4] = now(ms)  ARGV[5] = delay(ms)
ADD_JOB = """
local jobId = redis.call("INCR", KEYS[1])
local jobKey = KEYS[4] .. jobId
redis.call("HSET", jobKey,
  "id", jobId,
  "name", ARGV[1],
  "data", ARGV[2],
  "opts", ARGV[3],
  "timestamp", ARGV[4],
  "attemptsMade", 0)
local delay = tonumber(ARGV[5])
if delay > 0 then
  redis.call("HSET", jobKey, "delay", delay, "state", "delayed")
  redis.call("ZADD", KEYS[3], tonumber(ARGV[4]) + delay, jobId)
else
  redis.call("HSET", jobKey, "state", "wait")
  redis.call("LPUSH", KEYS[2], jobId)
end
return jobId
"""

# Acquire a job that the worker has just blocking-popped onto `active`.
# Sets the lock to our token, stamps processedOn, bumps attemptsMade, and clears
# any stale `stalled` membership. Done in one script so a job is never "active
# but unlocked" from the worker's point of view for longer than necessary.
# KEYS[1] = job hash   KEYS[2] = lock key   KEYS[3] = stalled set
# ARGV[1] = token  ARGV[2] = lockDuration(ms)  ARGV[3] = now(ms)  ARGV[4] = jobId
# Returns the new attemptsMade.
LOCK_JOB = """
redis.call("SET", KEYS[2], ARGV[1], "PX", tonumber(ARGV[2]))
redis.call("SREM", KEYS[3], ARGV[4])
local attemptsMade = redis.call("HINCRBY", KEYS[1], "attemptsMade", 1)
redis.call("HSET", KEYS[1], "processedOn", ARGV[3], "state", "active")
return attemptsMade
"""

# Renew a lock we still own. Token-guarded: we can NEVER renew a lock another
# worker has taken over. A successful renew also resets the stalled window by
# removing us from the `stalled` set.
# KEYS[1] = lock key   KEYS[2] = stalled set
# ARGV[1] = token  ARGV[2] = lockDuration(ms)  ARGV[3] = jobId
# Returns 1 if renewed, 0 if the lock is gone or owned by someone else.
EXTEND_LOCK = """
if redis.call("GET", KEYS[1]) == ARGV[1] then
  redis.call("SET", KEYS[1], ARGV[1], "PX", tonumber(ARGV[2]))
  redis.call("SREM", KEYS[2], ARGV[3])
  return 1
end
return 0
"""

# Move a finished job out of `active` into `completed`.
# Token-guarded: if we no longer own the lock (a stalled sweep handed the job to
# another worker) we commit NOTHING and report it, so a result can't be written
# twice. This is what makes "at-least-once handler runs" still mean
# "exactly-once result commit".
# KEYS[1] = active list  KEYS[2] = completed zset  KEYS[3] = job hash  KEYS[4] = lock key
# ARGV[1] = jobId  ARGV[2] = returnvalue(json)  ARGV[3] = now(ms)  ARGV[4] = token
# Returns 1 on success, -2 if the lock was lost, -3 if the job was not active.
MOVE_TO_COMPLETED = """
if redis.call("GET", KEYS[4]) ~= ARGV[4] then return -2 end
redis.call("DEL", KEYS[4])
if redis.call("LREM", KEYS[1], 0, ARGV[1]) == 0 then return -3 end
redis.call("ZADD", KEYS[2], tonumber(ARGV[3]), ARGV[1])
redis.call("HSET", KEYS[3],
  "returnvalue", ARGV[2],
  "finishedOn", ARGV[3],
  "state", "completed")
return 1
"""

# Decide a failed job's fate: retry (wait/delayed) or move to `failed`.
# Token-guarded like MOVE_TO_COMPLETED: a worker that lost its lock can't push
# the job to retry/failed on top of whoever took it over.
# KEYS[1] = active  KEYS[2] = wait  KEYS[3] = delayed  KEYS[4] = failed
# KEYS[5] = job hash  KEYS[6] = lock key
# ARGV[1] = jobId  ARGV[2] = failedReason  ARGV[3] = now(ms)
# ARGV[4] = attemptsMade  ARGV[5] = maxAttempts  ARGV[6] = backoff(ms)  ARGV[7] = token
# Returns 1 if permanently failed, 0 if scheduled for retry,
#         -2 if the lock was lost, -3 if the job was not active.
MOVE_TO_FAILED = """
if redis.call("GET", KEYS[6]) ~= ARGV[7] then return -2 end
redis.call("DEL", KEYS[6])
if redis.call("LREM", KEYS[1], 0, ARGV[1]) == 0 then return -3 end
local attemptsMade = tonumber(ARGV[4])
local maxAttempts = tonumber(ARGV[5])
redis.call("HSET", KEYS[5], "failedReason", ARGV[2], "attemptsMade", attemptsMade)
if attemptsMade < maxAttempts then
  local backoff = tonumber(ARGV[6])
  if backoff > 0 then
    redis.call("HSET", KEYS[5], "state", "delayed")
    redis.call("ZADD", KEYS[3], tonumber(ARGV[3]) + backoff, ARGV[1])
  else
    redis.call("HSET", KEYS[5], "state", "wait")
    redis.call("LPUSH", KEYS[2], ARGV[1])
  end
  return 0
end
redis.call("ZADD", KEYS[4], tonumber(ARGV[3]), ARGV[1])
redis.call("HSET", KEYS[5], "finishedOn", ARGV[3], "state", "failed")
return 1
"""

# Re-queue a failed job for another attempt (admin/dashboard action).
# KEYS[1] = failed zset  KEYS[2] = wait list  KEYS[3] = job hash   ARGV[1] = jobId
RETRY_JOB = """
if redis.call("ZREM", KEYS[1], ARGV[1]) == 0 then return 0 end
redis.call("HDEL", KEYS[3], "failedReason", "finishedOn")
redis.call("HSET", KEYS[3], "state", "wait")
redis.call("LPUSH", KEYS[2], ARGV[1])
return 1
"""

# Remove a job from wherever it lives and delete its hash (admin/dashboard action).
# KEYS[1..5] = wait, active, delayed, completed, failed   KEYS[6] = job hash
# ARGV[1] = jobId
REMOVE_JOB = """
redis.call("LREM", KEYS[1], 0, ARGV[1])
redis.call("LREM", KEYS[2], 0, ARGV[1])
redis.call("ZREM", KEYS[3], ARGV[1])
redis.call("ZREM", KEYS[4], ARGV[1])
redis.call("ZREM", KEYS[5], ARGV[1])
return redis.call("DEL", KEYS[6])
"""

# Mark-and-sweep recovery of jobs whose worker died.
#
# A job is only treated as truly stalled if it was marked on the PREVIOUS pass
# (it was in `active`) and is STILL in `active` with NO lock on this pass. Live
# workers renew their lock every lockDuration/2 (which SREMs them from `stalled`),
# so a healthy job is removed from the set before the next sweep and never
# falsely recovered. Two passes => one stalledInterval of grace.
#
# Per stalled job: LREM it from `active`; bump stalledCounter; if it has now
# stalled more than maxStalledCount times -> `failed`, else -> back to `wait`.
# Then re-mark: SADD the current `active` list into `stalled` for next time.
#
# KEYS[1] = stalled set  KEYS[2] = active  KEYS[3] = wait  KEYS[4] = failed
# KEYS[5] = stalled-check (throttle key)  KEYS[6] = key base (job hash = base..id)
# ARGV[1] = maxStalledCount  ARGV[2] = now(ms)  ARGV[3] = throttle(ms), 0 disables
# Returns {failedIds, recoveredIds}.
MOVE_STALLED = """
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
          redis.call("HSET", jobKey, "state", "wait")
          redis.call("RPUSH", KEYS[3], jobId)
          table.insert(recovered, jobId)
        end
      end
    end
  end
end

-- Re-mark everything currently active (batched to stay well under unpack limits).
local active = redis.call("LRANGE", KEYS[2], 0, -1)
local i = 1
while i <= #active do
  local j = math.min(i + 999, #active)
  redis.call("SADD", KEYS[1], unpack(active, i, j))
  i = j + 1
end
return {failed, recovered}
"""

# Move every delayed job whose time has come into `wait`.
# KEYS[1] = delayed zset  KEYS[2] = wait list  KEYS[3] = key base (job hash = base .. id)
# ARGV[1] = now(ms)
PROMOTE_DELAYED = """
local jobs = redis.call("ZRANGEBYSCORE", KEYS[1], 0, ARGV[1])
for _, jobId in ipairs(jobs) do
  redis.call("ZREM", KEYS[1], jobId)
  redis.call("HSET", KEYS[3] .. jobId, "state", "wait")
  redis.call("LPUSH", KEYS[2], jobId)
end
return #jobs
"""
