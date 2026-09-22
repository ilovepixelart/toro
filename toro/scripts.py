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

from .job import DEFAULT_KEEP_COMPLETED, DEFAULT_KEEP_FAILED

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

# A finished child of a RUNNING flow is scored LIVE + now in its finished set: above
# every timestamp and out of the trims' reach, which look below LIVE only. Under 2^53
# with a timestamp added, so the score stays exact.
LIVE_SCORE = 2**52

# Python-side numbers the Lua needs, interpolated so the two cannot drift.
_CONSTANTS = (
    f"local DEFAULT_KEEP_COMPLETED = {DEFAULT_KEEP_COMPLETED}\n"
    f"local DEFAULT_KEEP_FAILED = {DEFAULT_KEEP_FAILED}\n"
    f"local LIVE = {LIVE_SCORE}\n"
    # as a string: Lua formats a number it concatenates with %.14g, four short of LIVE
    f'local LIVE_BOUND = "({LIVE_SCORE}"\n'
)

# Shared routines, prepended to every script that enqueues or acquires a job.
# This is the single definition of "how a job is ordered, woken, claimed":
#   priorityScore  - (priority, seq) -> ZSET score (lower score = sooner)
#   enqueue        - put a job into `prioritized` + arm the base marker
#   lockAndLoad    - lock a job already on `active`, stamp it, return its hash
#   acquireNext    - ZPOPMIN the next job into `active`, then lockAndLoad it
# To add markers-with-delay or grouping later, we change only these functions.
_LIB = (
    _CONSTANTS
    + """
local function priorityScore(priority, pcKey)
  local seq = redis.call("INCR", pcKey) % 4294967296
  return (1048576 - priority) * 4294967296 + seq
end
local function enqueue(prioritizedKey, markerKey, jobId, priority, pcKey)
  redis.call("ZADD", prioritizedKey, priorityScore(priority, pcKey), jobId)
  redis.call("ZADD", markerKey, 0, "0")
end
-- A cancellation names the CLAIM it is meant for, not just the job: an id is free
-- again the moment its job is gone, so a bare id can land on the next job to wear it.
-- `processedOn` is rewritten by every claim (see lockAndLoad), which is exactly the
-- incarnation a worker is running.
local function cancelMessage(base, jobId)
  return jobId .. ":" .. (redis.call("HGET", base .. jobId, "processedOn") or "")
end
-- Jobs that share a concurrency key run one at a time, in the order they were added.
-- The key is held from enqueue until the holder reaches a terminal state: `ck:<key>`
-- names the holder and `held:<key>` is the queue behind it, scored as `prioritized`
-- would have scored those jobs, so a job keeps its place among the ones added with it.
-- A held job is in no other collection: it waits on the key, not on a worker.
local function takeKey(base, jobKey, jobId, ckey, priority, pcKey, now)
  if ckey == "" then return true end
  redis.call("HSET", jobKey, "ckey", ckey)
  if redis.call("SET", base .. "ck:" .. ckey, jobId, "NX") then return true end
  redis.call("HSET", jobKey, "state", "held")
  redis.call("ZADD", base .. "held:" .. ckey, priorityScore(priority, pcKey), jobId)
  redis.call("ZADD", base .. "held", now, jobId)
  return false
end
-- Hand a key to the job that has waited longest at the highest priority, or give it
-- up. A job released with time still to run on its delay goes back to `delayed`: it
-- waited on the key, which is not the same as having waited out its delay.
local function releaseKey(base, jobKey, jobId, now)
  local ckey = redis.call("HGET", jobKey, "ckey")
  if not ckey then return end
  local holder = base .. "ck:" .. ckey
  if redis.call("GET", holder) ~= jobId then return end
  local queued = base .. "held:" .. ckey
  while true do
    local nxt = redis.call("ZPOPMIN", queued)
    if not nxt[1] then
      redis.call("DEL", holder)
      return
    end
    local nid = nxt[1]
    redis.call("ZREM", base .. "held", nid)
    local meta = redis.call("HMGET", base .. nid, "timestamp", "delay", "state")
    -- An id whose hash is gone is not a job. Made the holder it would be resurrected
    -- as an empty one, run, and pin the key to itself for good, so it is skipped and
    -- the next real job takes the key instead.
    if meta[3] then
      redis.call("SET", holder, nid)
      local due = (tonumber(meta[1]) or now) + (tonumber(meta[2]) or 0)
      if due > now then
        redis.call("HSET", base .. nid, "state", "delayed")
        redis.call("ZADD", base .. "delayed", due, nid)
      else
        redis.call("HSET", base .. nid, "state", "wait")
        redis.call("ZADD", base .. "prioritized", tonumber(nxt[2]), nid)
        redis.call("ZADD", base .. "marker", 0, "0")
      end
      return
    end
  end
end
-- Take a job out of the key machinery before its hash goes: a holder hands the key
-- on, a job queued behind one leaves that queue (its place in the `held` listing
-- goes with its state, like any other). Nothing reads the key once the hash is gone,
-- so a job deleted while still named there would hold it forever.
local function unkey(base, jobId, state, ckey, now)
  if not ckey then return end
  if state == "held" then
    redis.call("ZREM", base .. "held:" .. ckey, jobId)
  else
    releaseKey(base, base .. jobId, jobId, now)
  end
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
-- `cap` is the global concurrency limit (0 = none): jobs active at once across every
-- worker. It reads `active` itself, so there is no counter to leak. Checked before
-- the pop, so a capped claim touches nothing: no put-back, no rate-limit token
-- spent. Only MOVE_TO_ACTIVE can actually be refused here: a finish script LREMs
-- its own job before it fetches, so its claim is a swap that cannot raise occupancy.
local function acquireNext(prioritizedKey, activeKey, markerKey, stalledKey,
                           base, pcKey, metaKey, token, lockMs, now,
                           rlKey, rlMax, rlDuration, cap)
  if redis.call("EXISTS", metaKey) == 1 then return false end  -- queue paused
  if cap > 0 and redis.call("LLEN", activeKey) >= cap then return false end
  local res = redis.call("ZPOPMIN", prioritizedKey)
  if #res == 0 then
    -- Held jobs carry a sequence minted from this counter and keep it until their key
    -- frees, so resetting it while any of them waits would sort a later job ahead of
    -- an earlier one. Nothing waiting anywhere: the counter is free to start over.
    if redis.call("ZCARD", base .. "held") == 0 then redis.call("DEL", pcKey) end
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
  -- re-arm so another idle worker wakes - unless this claim took the last slot,
  -- when a worker woken now could only be turned away
  if redis.call("ZCARD", prioritizedKey) > 0
     and (cap <= 0 or redis.call("LLEN", activeKey) < cap) then
    redis.call("ZADD", markerKey, 0, "0")
  end
  return lockAndLoad(jobId, stalledKey, base, token, lockMs, now)
end
-- A missing cap is an error, not "no cap": a limit must never fail open. Callers
-- read it BEFORE their first write: Redis does not roll a script back, so an error
-- raised after a finish has committed would leave that commit standing.
local function requireCap(raw)
  local cap = tonumber(raw)
  if cap == nil then error("missing global concurrency cap argument") end
  return cap
end
-- A slot in `active` was freed WITHOUT a claim (a draining worker's finish, a
-- stalled job failed for good). Under a global concurrency cap a worker may be
-- parked with jobs still waiting, and nothing else would wake it.
local function wakeIfWaiting(prioritizedKey, markerKey)
  if redis.call("ZCARD", prioritizedKey) > 0 then
    redis.call("ZADD", markerKey, 0, "0")
  end
end
-- A job is finished when it will not run again: no attempt left, retention applies.
-- Every place that asks reads this, so a new terminal state joins them all at once.
local function isFinished(state)
  return state == "completed" or state == "failed" or state == "cancelled"
end
local function delKeys(base, id)
  redis.call("DEL", base .. id, base .. id .. ":lock", base .. id .. ":logs",
             base .. id .. ":deps", base .. id .. ":results", base .. id .. ":cfail",
             base .. id .. ":ccancel", base .. id .. ":live")
  redis.call("ZREM", base .. "children", id)  -- prune the flow-child index (hygiene)
end
local function delJobs(ids, base)
  for _, id in ipairs(ids) do
    -- A running flow's finished jobs are scored above every bound (see recordFinished),
    -- so if the flow goes without them nothing else would ever reclaim them. Its live
    -- index holds the whole subtree, so one pass covers a node that is already gone.
    -- Settled nodes only: a node the index still lists but that is running again (a
    -- retry) is reachable from its own state, and deleting it here would delete a job
    -- mid-flight and strand the concurrency key it holds.
    for _, cid in ipairs(redis.call("ZRANGE", base .. id .. ":live", 0, -1)) do
      local cstate = redis.call("HGET", base .. cid, "state")
      if isFinished(cstate) then
        redis.call("ZREM", base .. cstate, cid)
        delKeys(base, cid)
      end
    end
    delKeys(base, id)
  end
end
-- Per-minute metrics bucket: one small hash per (queue, minute) holding
-- `added` / `completed` / `failed` counts and `ms` (summed processing
-- duration), each bucket self-expiring after retentionMs. Written here,
-- inside the same scripts as the transitions, so a counter can never
-- disagree with the transition it counts. When a job name is given, the
-- same counts also land in per-name fields ("completed:<name>", ...) so
-- a dashboard can answer "which job is responsible".
-- Log-scaled duration bucket index for the percentile histograms: [0,20ms) is
-- bucket 0, then 1.5x growth per bucket, overflow clamped to the last (25).
local function histIdx(durMs)
  if durMs < 20 then return 0 end
  return math.min(25, math.floor(math.log(durMs / 20) / math.log(1.5)) + 1)
end
local function recordMetrics(base, field, now, durMs, retentionMs, name, count)
  local bucket = base .. "metrics:" .. tostring(math.floor(now / 60000) * 60000)
  redis.call("HINCRBY", bucket, field, count or 1)
  if durMs > 0 then redis.call("HINCRBY", bucket, "ms", durMs) end
  if name then
    redis.call("HINCRBY", bucket, field .. ":" .. name, 1)
    if durMs > 0 then redis.call("HINCRBY", bucket, "ms:" .. name, durMs) end
    -- duration histogram ("h:<name>:<bucketIdx>"), successful jobs only
    if field == "completed" then
      redis.call("HINCRBY", bucket, "h:" .. name .. ":" .. histIdx(durMs), 1)
    end
  end
  redis.call("PEXPIRE", bucket, retentionMs)
end
-- Flow-level metrics: a whole flow is ONE unit, recorded only for the ROOT (a
-- parent with no parentId) when it settles, so nested sub-flows don't double
-- count. `flows:completed` / `flows:failed` are per-minute counters in the same
-- bucket; a completed flow also feeds a duration histogram ("fh:<idx>") over the
-- END-TO-END wall clock (enqueue -> root finished), which no per-job metric sees.
local function recordFlow(base, completed, now, durMs, retentionMs)
  local bucket = base .. "metrics:" .. tostring(math.floor(now / 60000) * 60000)
  if completed then
    redis.call("HINCRBY", bucket, "flows:completed", 1)
    redis.call("HINCRBY", bucket, "fh:" .. histIdx(durMs), 1)
  else
    redis.call("HINCRBY", bucket, "flows:failed", 1)
  end
  redis.call("PEXPIRE", bucket, retentionMs)
end
-- Finished jobs this SCRIPT may still trim. A bound that meets a deep backlog (a
-- limit just enabled, a default just tightened) must not sweep it all in one
-- Redis-blocking pass: the remainder amortizes over the following finishes (same
-- idea as PROMOTE_DELAYED's batch). One budget for the whole script, because one
-- script can record many jobs (a failing flow child fails its ancestors with it)
-- and one job can carry both bounds. A flow goes whole: the last root removed may
-- overrun the budget by the rest of its subtree, at most MAX_FLOW_NODES.
local trimBudget = 1000
-- How much of a finished set a job's remove option keeps:
--   keepCount: -1 keep all, 0 remove immediately (don't record), N keep newest N
--   keepAge:   -1 no age limit, S keep only those finished within S seconds
-- The ONE place the option is read. It lives here and not in the worker because two
-- of the ways a job finishes have no worker behind them: a parent failed with its
-- child, and a job the stalled sweep gives up on. Unset, null and unreadable opts
-- keep the default for the set.
-- A count is a rank and an age is multiplied into a score: both must be whole, and
-- neither may be NaN or infinite, which compare as nothing and would slip past
-- every bound below. Anything else means "no bound".
local function whole(v)
  local n = tonumber(v)
  if n == nil or n ~= n or n < 0 or n == math.huge then return -1 end
  return math.floor(n)
end
local function keepFor(optsJson, state)
  local completed = state == "completed"
  local default = completed and DEFAULT_KEEP_COMPLETED or DEFAULT_KEEP_FAILED
  local ok, opts = pcall(cjson.decode, optsJson or "{}")
  if not ok or type(opts) ~= "table" then return default, -1 end
  local v = opts.removeOnFail
  if completed then v = opts.removeOnComplete end
  if v == nil or v == cjson.null then return default, -1 end
  if v == false then return -1, -1 end
  if v == true then return 0, -1 end
  if type(v) == "number" then return whole(v), -1 end
  if type(v) == "table" then return whole(v.count), whole(v.age) end
  return -1, -1
end
-- Record a terminal job in its finished set and apply the job's own retention,
-- oldest first. Every way a job finishes comes through here, so none of them can
-- skip the trim or apply another job's bound.
-- The root of a job's flow while that flow is STILL RUNNING, or nil. What keeps a
-- finished job is its root, not its parent: a parent can settle mid-flow (a tolerated
-- failure) while the root carries on. `rootId` is stored on every flow node at enqueue;
-- a flow from before that walks up, and a walk that meets a missing ancestor gives up,
-- as does a flow whose root is gone: such a flow is already partial, and the job is
-- settled history like any other.
local function runningRoot(base, parentId, rootId)
  if not rootId then
    rootId = parentId
    local up = redis.call("HGET", base .. rootId, "parentId")
    while up do
      rootId = up
      up = redis.call("HGET", base .. rootId, "parentId")
    end
  end
  local rstate = redis.call("HGET", base .. rootId, "state")
  if not rstate or isFinished(rstate) then return nil end
  return rootId
end
-- Whether a job's own option keeps every job in its set: the one thing a cascade
-- may not remove, whatever happens to the root.
local function keptForever(base, jobId, state)
  local keepCount, keepAge = keepFor(redis.call("HGET", base .. jobId, "opts"), state)
  return keepCount < 0 and keepAge < 0
end
-- Remove a finished job from `setKey` and, with it, the finished subtree of a flow
-- it roots: the trim reaches a root first (it is the oldest of its flow) and a
-- partial flow is worth nothing. Returns how many jobs went, which the trim budget
-- pays for. Left alone: a descendant still running (it settles as an orphan and is
-- trimmed alone) and one whose own option keeps everything. An id whose hash is
-- already gone (an earlier cascade in this script, an operator's DEL) is not a job:
-- it costs nothing, and its listing goes with it rather than holding the bound.
local function removeFinished(base, setKey, jobId)
  local meta = redis.call("HMGET", base .. jobId, "state", "children")
  redis.call("ZREM", setKey, jobId)
  if not meta[1] then return 0 end
  delJobs({jobId}, base)
  local gone = 1
  if meta[2] then
    for _, cid in ipairs(cjson.decode(meta[2])) do
      local cstate = redis.call("HGET", base .. cid, "state")
      if isFinished(cstate) and not keptForever(base, cid, cstate) then
        gone = gone + removeFinished(base, base .. cstate, cid)
      end
    end
  end
  return gone
end
-- A root has settled (or is being removed at once): its live descendants become
-- settled jobs, scored just above the root so the root is the oldest of its flow
-- and a trim reaches it first, taking the subtree with it.
local function settleLive(base, rootId, now)
  local key = base .. rootId .. ":live"
  for _, cid in ipairs(redis.call("ZRANGE", key, 0, -1)) do
    local cstate = redis.call("HGET", base .. cid, "state")
    if isFinished(cstate) then
      redis.call("ZADD", base .. cstate, now + 1, cid)
    end
  end
  redis.call("DEL", key)
end
-- A settled root runs again (a retry): its finished descendants are live again,
-- scored LIVE and indexed, until the flow settles once more.
local function reviveSubtree(base, rootId, jobId, now)
  local children = redis.call("HGET", base .. jobId, "children")
  if not children then return end
  for _, cid in ipairs(cjson.decode(children)) do
    local cstate = redis.call("HGET", base .. cid, "state")
    if isFinished(cstate) then
      redis.call("ZADD", base .. cstate, LIVE + now, cid)
      redis.call("ZADD", base .. rootId .. ":live", now, cid)
    end
    reviveSubtree(base, rootId, cid, now)
  end
end
local function recordFinished(setKey, jobKey, base, jobId, now, prop, val, state)
  releaseKey(base, jobKey, jobId, now)  -- terminal: whatever happens below, the key goes on
  local meta = redis.call("HMGET", jobKey, "opts", "parentId", "rootId", "children")
  local keepCount, keepAge = keepFor(meta[1], state)
  local root = meta[2] and runningRoot(base, meta[2], meta[3])
  local score = now
  if root then
    score = LIVE + now
  elseif meta[2] then
    score = now + 1  -- an orphan is never older than its root, whatever the tie
  end
  if keepCount == 0 and keepAge < 0 then
    if meta[4] then settleLive(base, jobId, now) end  -- nothing else would place them
    delJobs({jobId}, base)
    return
  end
  redis.call("ZADD", setKey, score, jobId)
  redis.call("HSET", jobKey, prop, val, "finishedOn", now, "state", state)
  if root then
    redis.call("ZADD", base .. root .. ":live", now, jobId)
  elseif meta[4] then  -- a settling root: its flow becomes history with it
    settleLive(base, jobId, now)
  end
  if keepAge >= 0 and trimBudget > 0 then
    local cutoff = now - keepAge * 1000
    local expired = redis.call("ZRANGEBYSCORE", setKey, "-inf", "(" .. cutoff,
                               "LIMIT", 0, trimBudget)
    for _, id in ipairs(expired) do
      if trimBudget <= 0 then break end
      trimBudget = trimBudget - removeFinished(base, setKey, id)
    end
  end
  -- trimBudget > 0 is load-bearing: at 0 the range below would end at -1, the whole set
  if keepCount > 0 and trimBudget > 0 then
    -- the bound counts what has settled: a running flow's children sit above LIVE
    local excess = redis.call("ZCOUNT", setKey, "-inf", LIVE_BOUND) - keepCount
    if excess > 0 then
      local victims = redis.call("ZRANGE", setKey, 0, math.min(excess, trimBudget) - 1)
      for _, id in ipairs(victims) do
        if trimBudget <= 0 then break end
        trimBudget = trimBudget - removeFinished(base, setKey, id)
      end
    end
  end
end
-- Flow plumbing. A parent is parked in the `waiting-children` ZSET with a
-- `<id>:deps` SET of pending child ids. Children settle into their parent
-- inside the SAME script that commits their own transition (finish, stalled
-- escalation, removal), so the fan-in barrier resolves on the crash path too.
-- Queue-level keys derive from `base` (single-node assumption, see header).
local function releaseParent(base, parentId, now)
  -- no-op unless the parent is still parked (it may have failed eagerly)
  if redis.call("ZREM", base .. "waiting-children", parentId) == 0 then return end
  local parentKey = base .. parentId
  local priority = tonumber(redis.call("HGET", parentKey, "priority")) or 0
  -- a parent takes its key only now: it was not runnable while its children ran. The
  -- key is a field (written at enqueue) rather than a decode of `opts`: this runs after
  -- a child's commit, and Redis rolls nothing back, so it may not raise.
  local ckey = redis.call("HGET", parentKey, "ckey") or ""
  if takeKey(base, parentKey, parentId, ckey, priority, base .. "pc", now) then
    redis.call("HSET", parentKey, "state", "wait")
    enqueue(base .. "prioritized", base .. "marker", parentId, priority, base .. "pc")
  end
end
local function settleChildCompleted(base, jobId, parentId, returnvalue, now)
  -- a fully-removed parent (retention trim) must not get orphan keys recreated
  if redis.call("EXISTS", base .. parentId) == 0 then return end
  redis.call("HSET", base .. parentId .. ":results", jobId, returnvalue)
  redis.call("SREM", base .. parentId .. ":deps", jobId)
  if redis.call("SCARD", base .. parentId .. ":deps") == 0 then
    releaseParent(base, parentId, now)
  end
end
-- A child that will never deliver settles its parent per its `onFail` policy:
-- "continue" records the failure and releases the parent once nothing is
-- pending; anything else fails the parent NOW - eagerly, no worker needed -
-- walking up through ancestors that are themselves fail_parent children.
-- There is no "park forever" outcome by design. Eager failure goes through
-- recordFinished so remove_on_fail retention applies like any other failure.
-- `state` is how the child ended: an ancestor stopped by a CANCELLED child is
-- cancelled, not failed. Counting a deliberate stop as a failure is the one thing
-- the separate state exists to prevent, so it must not come back through the flow.
-- Under `continue` the parent still runs either way, with the reason in its `:cfail`
-- record: a cancelled child reads there as "cancelled".
local function settleChildGone(base, jobId, parentId, onFail, reason, now, retentionMs, state)
  local cid = jobId
  local pid = parentId
  while pid do
    if onFail == "continue" then
      if redis.call("EXISTS", base .. pid) == 1 then
        -- its own record: the parent runs either way, but a stopped child is not a
        -- failed one and its fan-in must not say otherwise
        local record = state == "cancelled" and ":ccancel" or ":cfail"
        redis.call("HSET", base .. pid .. record, cid, reason)
        redis.call("SREM", base .. pid .. ":deps", cid)
        if redis.call("SCARD", base .. pid .. ":deps") == 0 then
          releaseParent(base, pid, now)
        end
      end
      return
    end
    -- already settled (a sibling settled it first, or it was removed): stop
    if redis.call("ZREM", base .. "waiting-children", pid) == 0 then return end
    -- read BEFORE recordFinished (retention may DEL the hash)
    local pmeta = redis.call("HMGET", base .. pid, "parentId", "onFail", "name")
    if state == "cancelled" then
      recordFinished(base .. "cancelled", base .. pid, base, pid, now,
        "cancel", "1", "cancelled")
      redis.call("PUBLISH", base .. "events",
        cjson.encode({jobId = tostring(pid), event = "cancelled"}))
    else
      reason = "child " .. cid .. " failed: " .. reason
      recordFinished(base .. "failed", base .. pid, base, pid, now,
        "failedReason", reason, "failed")
      recordMetrics(base, "failed", now, 0, retentionMs, pmeta[3])
      -- reached the ROOT of the flow (no grandparent): count one flow failure
      if not pmeta[1] then recordFlow(base, false, now, 0, retentionMs) end
      redis.call("PUBLISH", base .. "events",
        cjson.encode({jobId = tostring(pid), event = "failed", reason = reason}))
    end
    cid = pid
    pid = pmeta[1]
    onFail = pmeta[2]
  end
end
"""
)

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
# ARGV[10] metricsRetention(ms)  ARGV[11] concurrency key ("" = none)
# Returns {jobId, state}: on a dedup hit or an id replay, the id of the job that is
# already there and the state it is really in.
ADD_JOB = (
    _LIB
    + """
local base = KEYS[5]
local function announce(jobId)
  -- whole-message cjson.encode: no hand-built JSON anywhere on the event bus
  redis.call("PUBLISH", KEYS[7],
    cjson.encode({jobId = tostring(jobId), event = "added"}))
end
-- An add that enqueued nothing answers with the job that is already there, in the
-- state it is really in: the caller's Job must not read `wait` for one that is
-- running, or for one held on its key.
local function alreadyThere(jobId)
  announce(jobId)
  return {tostring(jobId), redis.call("HGET", base .. jobId, "state") or ""}
end
-- Throttle dedup: within the TTL window, a repeat dedup id is ignored and the
-- already-queued job's id is returned (self-expiring, no finish-side cleanup).
local dedupKey
if ARGV[8] ~= "" then
  dedupKey = base .. "de:" .. ARGV[8]
  local existing = redis.call("GET", dedupKey)
  if existing then return alreadyThere(existing) end
end
local jobId = ARGV[7]
if jobId == "" then
  jobId = redis.call("INCR", KEYS[1])
elseif redis.call("EXISTS", base .. jobId) == 1 then
  return alreadyThere(jobId)
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
local now = tonumber(ARGV[4])
if delay > 0 then redis.call("HSET", jobKey, "delay", delay) end
local state = "held"  -- takeKey parks it there when the key is taken
if takeKey(base, jobKey, jobId, ARGV[11], tonumber(ARGV[6]), KEYS[6], now) then
  if delay > 0 then
    state = "delayed"
    redis.call("HSET", jobKey, "state", state)
    redis.call("ZADD", KEYS[4], now + delay, jobId)
  else
    state = "wait"
    redis.call("HSET", jobKey, "state", state)
    enqueue(KEYS[2], KEYS[3], jobId, tonumber(ARGV[6]), KEYS[6])
  end
end
-- only real inserts count (dedup hits and id replays returned above)
recordMetrics(base, "added", tonumber(ARGV[4]), 0, tonumber(ARGV[10]))
announce(jobId)
return {tostring(jobId), state}
"""
)

# Add a whole flow tree atomically: every node's hash is created, leaves are
# enqueued (or delayed), interior nodes are parked in `waiting-children` with
# their `:deps` barrier populated. Either the entire flow exists or none of it.
# Node `data`/`opts` arrive pre-encoded as JSON strings (stored verbatim).
# KEYS[1] id counter  KEYS[2] key base
# ARGV[1] now(ms)  ARGV[2] flow tree (json)  ARGV[3] metricsRetention(ms)
# Returns the parent (root) job id.
ADD_FLOW = (
    _LIB
    + """
local base = KEYS[2]
local now = tonumber(ARGV[1])
local retention = tonumber(ARGV[3])
local total = 0
local function createNode(node, parentId, rootId)
  local jobId = tostring(redis.call("INCR", KEYS[1]))
  local jobKey = base .. jobId
  total = total + 1
  redis.call("HSET", jobKey,
    "id", jobId, "name", node.name, "data", node.data, "opts", node.opts,
    "timestamp", now, "attemptsMade", 0, "priority", node.priority)
  if parentId then
    -- rootId: where a finished node is indexed while its flow runs (see recordFinished)
    redis.call("HSET", jobKey, "parentId", parentId, "onFail", node.onFail, "rootId", rootId)
    redis.call("ZADD", base .. "children", now, jobId)  -- index as a flow child (ROOTS listing)
  end
  if node.children and #node.children > 0 then
    local cids = {}
    for i, child in ipairs(node.children) do
      cids[i] = createNode(child, jobId, rootId or jobId)
    end
    redis.call("SADD", jobKey .. ":deps", unpack(cids))
    redis.call("HSET", jobKey, "children", cjson.encode(cids),
               "state", "waiting-children")
    -- a parked parent queues for its key when its children settle (see releaseParent)
    if node.concurrencyKey ~= "" then
      redis.call("HSET", jobKey, "ckey", node.concurrencyKey)
    end
    redis.call("ZADD", base .. "waiting-children", now, jobId)
  else
    local delay = tonumber(node.delay)
    if delay > 0 then redis.call("HSET", jobKey, "delay", delay) end
    if takeKey(base, jobKey, jobId, node.concurrencyKey, tonumber(node.priority),
               base .. "pc", now) then
      if delay > 0 then
        redis.call("HSET", jobKey, "state", "delayed")
        redis.call("ZADD", base .. "delayed", now + delay, jobId)
      else
        redis.call("HSET", jobKey, "state", "wait")
        enqueue(base .. "prioritized", base .. "marker", jobId,
                tonumber(node.priority), base .. "pc")
      end
    end
  end
  return jobId
end
local rootId = createNode(cjson.decode(ARGV[2]), false, false)
-- one metrics increment and one announce for the whole tree: per-node events
-- have no result() waiter, and every pub/sub subscriber would pay for them
recordMetrics(base, "added", now, 0, retention, nil, total)
redis.call("PUBLISH", base .. "events",
  cjson.encode({jobId = rootId, event = "added"}))
return rootId
"""
)

# Claim the next job: pop highest-priority from `prioritized` into `active`, lock
# it, and return its hash. The blocking BZPOPMIN on the marker only wakes the
# worker; THIS is the atomic move. Returns {jobHash, jobId} or nil if none.
# KEYS[1] prioritized  KEYS[2] active  KEYS[3] marker  KEYS[4] stalled
# KEYS[5] key base  KEYS[6] pc  KEYS[7] meta-paused  KEYS[8] limiter
# ARGV[1] token  ARGV[2] lockDuration(ms)  ARGV[3] now(ms)
# ARGV[4] rlMax (0 = no limit)  ARGV[5] rlDuration(ms)
# ARGV[6] globalConcurrency (0 = no cap)
# Returns false (none/paused/capped), {jobHash, jobId}, or {"__rl__", retryMs} when
# rate limited.
MOVE_TO_ACTIVE = (
    _LIB
    + """
return acquireNext(KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5], KEYS[6], KEYS[7],
                   ARGV[1], tonumber(ARGV[2]), ARGV[3],
                   KEYS[8], tonumber(ARGV[4]), tonumber(ARGV[5]), requireCap(ARGV[6]))
"""
)

# What EXTEND_LOCK answers when the job has been asked to stop. Named so the worker
# and the script cannot drift over a bare 2.
LOCK_CANCEL_REQUESTED = 2

# Renew a lock we still own. Token-guarded: we can NEVER renew a lock another
# worker has taken over. A successful renew also resets the stalled window.
# Returns 0 (the lock is gone), 1 (renewed) or LOCK_CANCEL_REQUESTED (renewed, and a
# cancellation has been asked for). The renewal is the backstop for a cancel message
# that never arrived: a worker that stops renewing has lost the job to the stalled
# sweep anyway, so this cannot be the thing that is missed.
# KEYS[1] lock  KEYS[2] stalled  KEYS[3] job hash
# ARGV[1] token  ARGV[2] lockDuration(ms)  ARGV[3] jobId
EXTEND_LOCK = """
if redis.call("GET", KEYS[1]) == ARGV[1] then
  redis.call("SET", KEYS[1], ARGV[1], "PX", tonumber(ARGV[2]))
  redis.call("SREM", KEYS[2], ARGV[3])
  if redis.call("HGET", KEYS[3], "cancel") then return 2 end
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
# ARGV[5] fetch(1/0)  ARGV[6] lockDuration(ms)
# ARGV[7] rlMax  ARGV[8] rlDuration(ms)  ARGV[9] metricsRetention(ms)
# ARGV[10] globalConcurrency (0 = no cap)
# Returns -2 lock lost, -3 not active, {1} committed, {1, nextHash, nextId}.
MOVE_TO_COMPLETED = (
    _LIB
    + """
local cap = 0
if ARGV[5] == "1" then cap = requireCap(ARGV[10]) end
if redis.call("GET", KEYS[4]) ~= ARGV[4] then return -2 end
redis.call("DEL", KEYS[4])
if redis.call("LREM", KEYS[1], 0, ARGV[1]) == 0 then return -3 end
local now = tonumber(ARGV[3])
-- read BEFORE recordFinished (remove-on-complete may DEL the hash)
local meta = redis.call("HMGET", KEYS[3],
  "processedOn", "name", "parentId", "children", "timestamp")
local startedOn = tonumber(meta[1]) or now
recordFinished(KEYS[2], KEYS[3], KEYS[8], ARGV[1], now, "returnvalue", ARGV[2], "completed")
recordMetrics(KEYS[8], "completed", now, now - startedOn, tonumber(ARGV[9]), meta[2])
-- a root flow completing: count the whole flow and its end-to-end wall clock
if meta[4] and not meta[3] then
  recordFlow(KEYS[8], true, now, now - (tonumber(meta[5]) or now), tonumber(ARGV[9]))
end
-- a flow child settles into its parent here, atomically with its own commit
if meta[3] then settleChildCompleted(KEYS[8], ARGV[1], meta[3], ARGV[2], now) end
-- the result is decoded and re-encoded as part of ONE cjson document: a
-- return value full of JSON metacharacters can never corrupt the message
local okr, resultDoc = pcall(cjson.decode, ARGV[2])
local completedMsg = {jobId = ARGV[1], event = "completed"}
if okr then completedMsg.result = resultDoc end
redis.call("PUBLISH", KEYS[10], cjson.encode(completedMsg))
if ARGV[5] == "1" then
  local nxt = acquireNext(KEYS[5], KEYS[1], KEYS[6], KEYS[7], KEYS[8], KEYS[9], KEYS[11],
                          ARGV[4], tonumber(ARGV[6]), ARGV[3],
                          KEYS[12], tonumber(ARGV[7]), tonumber(ARGV[8]), cap)
  if nxt then
    if nxt[1] == "__rl__" then
      redis.call("ZADD", KEYS[6], 0, "0")   -- rate limited: wake a worker to re-check
    else
      return {1, nxt[1], nxt[2]}
    end
  end
else
  wakeIfWaiting(KEYS[5], KEYS[6])
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
# ARGV[9] lockDuration(ms)
# ARGV[10] rlMax  ARGV[11] rlDuration(ms)  ARGV[12] metricsRetention(ms)
# ARGV[13] globalConcurrency (0 = no cap)
# Returns -2/-3, else {outcome} or {outcome, nextHash, nextId}; outcome 1=failed 0=retry.
MOVE_TO_FAILED = (
    _LIB
    + """
local cap = 0
if ARGV[8] == "1" then cap = requireCap(ARGV[13]) end
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
  local meta = redis.call("HMGET", KEYS[5], "processedOn", "name", "parentId", "onFail", "children")
  local startedOn = tonumber(meta[1]) or now
  recordFinished(KEYS[4], KEYS[5], KEYS[9], ARGV[1], now, "failedReason", ARGV[2], "failed")
  recordMetrics(KEYS[9], "failed", now, now - startedOn, tonumber(ARGV[12]), meta[2])
  redis.call("PUBLISH", KEYS[11],
    cjson.encode({jobId = ARGV[1], event = "failed", reason = ARGV[2]}))
  -- a root flow whose own processor failed (children all settled): count it.
  -- A child failing (meta[3] set) instead propagates through settleChildGone,
  -- which counts the root it reaches - the two paths are disjoint, no double count
  if meta[5] and not meta[3] then recordFlow(KEYS[9], false, now, 0, tonumber(ARGV[12])) end
  -- a flow child settles into its parent per its on_fail policy, atomically
  if meta[3] then
    settleChildGone(KEYS[9], ARGV[1], meta[3], meta[4], ARGV[2], now, tonumber(ARGV[12]),
      "failed")
  end
  outcome = 1
end
if ARGV[8] == "1" then
  local nxt = acquireNext(KEYS[2], KEYS[1], KEYS[7], KEYS[8], KEYS[9], KEYS[10], KEYS[12],
                          ARGV[7], tonumber(ARGV[9]), ARGV[3],
                          KEYS[13], tonumber(ARGV[10]), tonumber(ARGV[11]), cap)
  if nxt then
    if nxt[1] == "__rl__" then
      redis.call("ZADD", KEYS[7], 0, "0")   -- rate limited: wake a worker to re-check
    else
      return {outcome, nxt[1], nxt[2]}
    end
  end
else
  wakeIfWaiting(KEYS[2], KEYS[7])
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
# ARGV[9] concurrency key ("" = none)
ADD_SCHEDULED = (
    _LIB
    + """
local base = KEYS[2]
local jobKey = base .. ARGV[1]
if redis.call("EXISTS", jobKey) == 1 then return 0 end
local now = tonumber(ARGV[5])
redis.call("HSET", jobKey,
  "id", ARGV[1], "name", ARGV[2], "data", ARGV[3], "opts", ARGV[4],
  "timestamp", now, "attemptsMade", 0, "priority", ARGV[7],
  "delay", tonumber(ARGV[6]) - now, "schedulerId", ARGV[8])
-- an occurrence waits for its key like any job: held now, and back to `delayed` at
-- its own due time when the key frees (see releaseKey)
if takeKey(base, jobKey, ARGV[1], ARGV[9], tonumber(ARGV[7]), base .. "pc", now) then
  redis.call("HSET", jobKey, "state", "delayed")
  redis.call("ZADD", KEYS[1], tonumber(ARGV[6]), ARGV[1])
end
return 1
"""
)

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
# Flow-aware: a failed flow PARENT whose children haven't all settled re-parks
# in `waiting-children` (the barrier re-arms) instead of running at once with
# partial results; a failed flow CHILD re-joins its parked parent's barrier and
# clears its stale failure record, so the parent's post-retry view is honest.
# Retrying parent and children in any order (e.g. retry-all) recovers the flow.
# KEYS[1] failed  KEYS[2] prioritized  KEYS[3] marker  KEYS[4] job hash
# KEYS[5] pc  KEYS[6] key base
# ARGV[1] jobId  ARGV[2] now(ms)
RETRY_JOB = (
    _LIB
    + """
if redis.call("ZREM", KEYS[1], ARGV[1]) == 0 then return 0 end
-- A retry is a decision to run this job again, so a cancellation that was asked for
-- but never acted on goes with the failure it outlived. Left behind, it would stop
-- the job at its next claim and leave it unrunnable: retry refuses a cancelled job.
redis.call("HDEL", KEYS[4], "failedReason", "finishedOn", "cancel", "cancelReason")
local base = KEYS[6]
-- A flow that runs again: its finished jobs are live until it settles once more. Only
-- a ROOT revives the subtree. A descendant retried under a running root finds its flow
-- already live, and one retried under a root that will never settle again would index
-- jobs where nothing would ever drain them.
if redis.call("HEXISTS", KEYS[4], "parentId") == 0 then
  reviveSubtree(base, ARGV[1], ARGV[1], tonumber(ARGV[2]))
end
local parentId = redis.call("HGET", KEYS[4], "parentId")
if parentId then
  redis.call("HDEL", base .. parentId .. ":cfail", ARGV[1])
  if redis.call("ZSCORE", base .. "waiting-children", parentId) then
    redis.call("SADD", base .. parentId .. ":deps", ARGV[1])
  end
end
if redis.call("SCARD", base .. ARGV[1] .. ":deps") > 0 then
  redis.call("HSET", KEYS[4], "state", "waiting-children")
  redis.call("ZADD", base .. "waiting-children", tonumber(ARGV[2]), ARGV[1])
  return 1
end
local priority = tonumber(redis.call("HGET", KEYS[4], "priority")) or 0
-- it gave its key up when it failed, so it queues for it again rather than
-- running beside whoever holds it now
local ckey = redis.call("HGET", KEYS[4], "ckey") or ""
if takeKey(base, KEYS[4], ARGV[1], ckey, priority, KEYS[5], tonumber(ARGV[2])) then
  redis.call("HSET", KEYS[4], "state", "wait")
  enqueue(KEYS[2], KEYS[3], ARGV[1], priority, KEYS[5])
end
return 1
"""
)

# Remove a job from wherever it lives and delete its hash (admin/dashboard
# action). A flow parent takes its whole subtree with it; a flow child unlinks
# from its parent's barrier, releasing the parent when it was the last
# dependency (nothing left to wait for).
# KEYS[1] prioritized  KEYS[2] active  KEYS[3] delayed  KEYS[4] completed
# KEYS[5] failed  KEYS[6] waiting-children  KEYS[7] key base  KEYS[8] held
# KEYS[9] cancelled  KEYS[10] cancel channel
# ARGV[1] jobId  ARGV[2] now(ms)
REMOVE_JOB = (
    _LIB
    + """
local base = KEYS[7]
-- Read BEFORE the first write, like requireCap: Redis rolls nothing back, so a
-- missing clock has to fail the call rather than halfway through it.
local now = tonumber(ARGV[2])
if now == nil then error("REMOVE_JOB needs a now argument") end
-- The job's `state` field says which single collection holds it (every
-- transition writes it atomically); only an unreadable state pays the blanket
-- sweep - notably the O(active) LREM.
local function removeFromState(jobId, state)
  if state == "wait" then redis.call("ZREM", KEYS[1], jobId)
  elseif state == "active" then redis.call("LREM", KEYS[2], 0, jobId)
  elseif state == "delayed" then redis.call("ZREM", KEYS[3], jobId)
  elseif state == "completed" then redis.call("ZREM", KEYS[4], jobId)
  elseif state == "failed" then redis.call("ZREM", KEYS[5], jobId)
  elseif state == "cancelled" then redis.call("ZREM", KEYS[9], jobId)
  elseif state == "waiting-children" then redis.call("ZREM", KEYS[6], jobId)
  elseif state == "held" then redis.call("ZREM", KEYS[8], jobId)
  else
    redis.call("ZREM", KEYS[1], jobId)
    redis.call("LREM", KEYS[2], 0, jobId)
    redis.call("ZREM", KEYS[3], jobId)
    redis.call("ZREM", KEYS[4], jobId)
    redis.call("ZREM", KEYS[5], jobId)
    redis.call("ZREM", KEYS[6], jobId)
    redis.call("ZREM", KEYS[8], jobId)
    redis.call("ZREM", KEYS[9], jobId)
  end
end
local function removeTree(jobId)
  local meta = redis.call("HMGET", base .. jobId, "children", "state", "ckey")
  removeFromState(jobId, meta[2])
  if meta[2] == "active" then
    -- Its processor is still running, and removal gives it nowhere to report. Tell
    -- the worker, or the slot it holds (and any global-concurrency slot) stays taken
    -- until the work happens to end. Its commit finds no lock and stands down.
    redis.call("PUBLISH", KEYS[10], cancelMessage(base, jobId))
  end
  unkey(base, jobId, meta[2], meta[3], now)
  delJobs({jobId}, base)
  if meta[1] then
    for _, cid in ipairs(cjson.decode(meta[1])) do removeTree(cid) end
  end
end
local existed = redis.call("EXISTS", base .. ARGV[1])
local parentId = redis.call("HGET", base .. ARGV[1], "parentId")
removeTree(ARGV[1])
if parentId then
  -- unlink from the barrier only: a completed child's result and a failed
  -- child's post-mortem record belong to the parent, and routine history
  -- cleanup (clean('completed')) must never mutate a pending parent's data
  redis.call("SREM", base .. parentId .. ":deps", ARGV[1])
  if redis.call("SCARD", base .. parentId .. ":deps") == 0 then
    releaseParent(base, parentId, now)
  end
end
return existed
"""
)

# Commit a job its worker stopped. Token-guarded like every other finish: a worker
# that lost its lock commits NOTHING, so a job taken over mid-cancellation is not
# ended twice. No fetch-next: a cancellation is rare, and the loop takes the next job
# the way an idle worker does.
# KEYS[1] active  KEYS[2] cancelled  KEYS[3] job hash  KEYS[4] lock
# KEYS[5] prioritized  KEYS[6] marker  KEYS[7] base  KEYS[8] events channel
# ARGV[1] jobId  ARGV[2] now(ms)  ARGV[3] token  ARGV[4] metricsRetention(ms)
# Returns -2 lock lost, -3 not active, 1 committed.
MOVE_TO_CANCELLED = (
    _LIB
    + """
local base = KEYS[7]
if redis.call("GET", KEYS[4]) ~= ARGV[3] then return -2 end
redis.call("DEL", KEYS[4])
if redis.call("LREM", KEYS[1], 0, ARGV[1]) == 0 then return -3 end
local now = tonumber(ARGV[2])
-- read BEFORE recordFinished (retention may DEL the hash)
local meta = redis.call("HMGET", KEYS[3], "parentId", "onFail", "cancelReason")
recordFinished(KEYS[2], KEYS[3], base, ARGV[1], now, "cancel", "1", "cancelled")
local msg = {jobId = ARGV[1], event = "cancelled"}
if meta[3] then msg.reason = meta[3] end
redis.call("PUBLISH", KEYS[8], cjson.encode(msg))
if meta[1] then
  settleChildGone(base, ARGV[1], meta[1], meta[2], meta[3] or "cancelled", now,
    tonumber(ARGV[4]), "cancelled")
end
wakeIfWaiting(KEYS[5], KEYS[6])
return 1
"""
)

# Cancel a job: stop it wherever it is. A job that has not started is ended right
# here, with no worker involved; a running one is asked to stop, and its worker acts
# on the request (through the events channel, or EXTEND_LOCK as the backstop).
# Cancellation commits through recordFinished, so it hands a concurrency key on,
# settles a flow parent and applies retention exactly as any other finish does.
# KEYS[1] prioritized  KEYS[2] delayed  KEYS[3] held  KEYS[4] waiting-children
# KEYS[5] cancelled  KEYS[6] key base  KEYS[7] events channel  KEYS[8] cancel channel
# ARGV[1] jobId  ARGV[2] now(ms)  ARGV[3] metricsRetention(ms)
# ARGV[4] reason ("" = none)
# Returns 0 (nothing to cancel), 1 (cancelled here) or 2 (a running job was asked).
CANCEL_JOB = (
    _LIB
    + """
local base = KEYS[6]
local now = tonumber(ARGV[2])
local why = ARGV[4]
-- The caller's reason belongs to every job this call stops, the subtree included:
-- one cancellation, one explanation.
local function saveReason(jobKey)
  if why ~= "" then redis.call("HSET", jobKey, "cancelReason", why) end
end
local cancelledMsg = {jobId = "", event = "cancelled"}
local function announceCancelled(jobId)
  cancelledMsg.jobId = jobId
  cancelledMsg.reason = nil
  if why ~= "" then cancelledMsg.reason = why end
  redis.call("PUBLISH", KEYS[7], cjson.encode(cancelledMsg))
end
-- Cancel one job and answer with its child list (read before recordFinished, which
-- retention may follow with a DEL of the hash). A job already finished is left alone;
-- a running one can only be stopped by the worker that owns its processor, so it is
-- asked here and commits its own cancellation.
local stopped = 0  -- how many jobs this call actually stopped
local function cancelOne(jobId)
  local jobKey = base .. jobId
  local meta = redis.call("HMGET", jobKey, "state", "children", "ckey")
  local state = meta[1]
  if not state or isFinished(state) then return meta[2] end
  stopped = stopped + 1
  if state == "active" then
    redis.call("HSET", jobKey, "cancel", "1")
    saveReason(jobKey)
    -- to the workers' own channel: `events` carries a message per job, and a worker
    -- listening there would parse every one of them to catch this
    redis.call("PUBLISH", KEYS[8], cancelMessage(base, jobId))
    return meta[2]
  end
  if state == "wait" then redis.call("ZREM", KEYS[1], jobId)
  elseif state == "delayed" then redis.call("ZREM", KEYS[2], jobId)
  elseif state == "held" then redis.call("ZREM", KEYS[3], jobId)
  elseif state == "waiting-children" then redis.call("ZREM", KEYS[4], jobId)
  end
  -- a job queued behind a key leaves that queue; a holder hands its key on inside
  -- recordFinished, like any other job reaching a terminal state
  unkey(base, jobId, state, meta[3], now)
  saveReason(jobKey)
  recordFinished(KEYS[5], jobKey, base, jobId, now, "cancel", "1", "cancelled")
  announceCancelled(jobId)
  return meta[2]
end
-- A flow is cancelled as a unit: a child left running would report into a parent
-- that has already gone. Descendants do NOT settle into their parents on the way,
-- because those parents are being cancelled too.
local function cancelTree(jobId)
  local children = cancelOne(jobId)
  if children then
    for _, cid in ipairs(cjson.decode(children)) do cancelTree(cid) end
  end
end
local topState = redis.call("HGET", base .. ARGV[1], "state")
if not topState then return 0 end
-- read BEFORE the cancellation (retention may DEL the hash)
local top = redis.call("HMGET", base .. ARGV[1], "parentId", "onFail")
-- A settled root is still the handle on its flow: one child failing the parent
-- eagerly leaves its siblings running, and walking the tree by hand is the only
-- other way to reach them. So the walk goes ahead whatever the root's own state,
-- and the answer is whether anything was actually stopped.
cancelTree(ARGV[1])
if stopped == 0 then return 0 end
if isFinished(topState) then return 1 end  -- the root was already done; its subtree was not
if topState == "active" then return 2 end   -- its worker settles it when it stops
-- A cancelled child did not produce what its parent waits for, so the parent's own
-- onFail policy decides, exactly as it does for a child that failed: there is one
-- rule for "this child will never deliver", not two.
if top[1] then
  settleChildGone(base, ARGV[1], top[1], top[2], why ~= "" and why or "cancelled", now,
    tonumber(ARGV[3]), "cancelled")
end
return 1
"""
)

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
          local now = tonumber(ARGV[2])
          local reason = "job stalled more than allowable limit"
          -- read BEFORE recordFinished (remove-on-fail may DEL the hash)
          local meta = redis.call("HMGET", jobKey, "name", "parentId", "onFail", "children")
          recordFinished(KEYS[4], jobKey, KEYS[6], jobId, now, "failedReason", reason, "failed")
          recordMetrics(KEYS[6], "failed", now, 0, tonumber(ARGV[4]), meta[1])
          -- announce the terminal failure so result() waiters resolve instead
          -- of timing out (the sweeping worker's local events don't reach them)
          redis.call("PUBLISH", KEYS[6] .. "events",
            cjson.encode({jobId = jobId, event = "failed", reason = reason}))
          -- the crash path settles flow parents too - a dead worker must not
          -- leave a parent parked forever
          if meta[2] then
            settleChildGone(KEYS[6], jobId, meta[2], meta[3], reason, now,
              tonumber(ARGV[4]), "failed")
          elseif meta[4] then
            -- a released root flow parent that stalled out: count the flow failed
            recordFlow(KEYS[6], false, now, 0, tonumber(ARGV[4]))
          end
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
-- recovered jobs re-arm the marker through enqueue; a terminal failure does not
if #failed > 0 then wakeIfWaiting(KEYS[3], KEYS[7]) end

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

# Roots-only page of one ZSET state: the jobs a root-first dashboard shows with
# flow children hidden. Roots = state \ children (a child is any node with a
# parentId; the `children` index holds them all). ZDIFFSTORE keeps the state
# set's own scores, so the page order matches get_jobs() (priority / due-time /
# insertion); the caller picks ZRANGE vs ZREVRANGE for newest-first states.
# Exact and unbounded - no scan cap. The scratch zset lives only for this atomic
# call, so a single fixed key is safe (scripts never interleave).
# KEYS[1] state zset  KEYS[2] children zset  KEYS[3] scratch zset
# ARGV[1] start  ARGV[2] stop (inclusive)  ARGV[3] rev (1 = ZREVRANGE)
LIST_ROOTS = """
local total = redis.call("ZDIFFSTORE", KEYS[3], 2, KEYS[1], KEYS[2])
local ids = {}
if total > 0 then
  if ARGV[3] == "1" then
    ids = redis.call("ZREVRANGE", KEYS[3], ARGV[1], ARGV[2])
  else
    ids = redis.call("ZRANGE", KEYS[3], ARGV[1], ARGV[2])
  end
  redis.call("DEL", KEYS[3])
end
return {total, ids}
"""

# Exact roots-only count per state - the root-first counterpart of counts().
# Each ZSET state is diffed against the children index in turn through one shared
# scratch key (DELeted between uses); `active` is a LIST, so its roots are
# counted by membership in the children index. One atomic round trip for all seven.
# KEYS[1] prioritized  KEYS[2] delayed  KEYS[3] completed  KEYS[4] failed
# KEYS[5] waiting-children  KEYS[6] active (LIST)  KEYS[7] held
# KEYS[8] children  KEYS[9] scratch  KEYS[10] cancelled
ROOTS_COUNTS = """
local ch = KEYS[8]
local sc = KEYS[9]
local function rc(k)
  local n = redis.call("ZDIFFSTORE", sc, 2, k, ch)
  redis.call("DEL", sc)
  return n
end
local active = 0
for _, jid in ipairs(redis.call("LRANGE", KEYS[6], 0, -1)) do
  if redis.call("ZSCORE", ch, jid) == false then active = active + 1 end
end
return {rc(KEYS[1]), rc(KEYS[2]), rc(KEYS[3]), rc(KEYS[4]), rc(KEYS[5]), rc(KEYS[7]),
        rc(KEYS[10]), active}
"""


# The ARGV of the scripts with many parameters, in the order their headers document.
# Built here and nowhere else: a hand-rolled list keeps "working" when the layout
# changes, with its values in the wrong slots.
def add_job_args(
    *,
    name: str,
    data: str,
    opts: str,
    now: int,
    delay: int,
    priority: int,
    job_id: str,
    dedup_id: str,
    dedup_ttl: int,
    concurrency_key: str,
) -> list[str | int]:
    """ARGV for ADD_JOB."""
    return [
        name,
        data,
        opts,
        now,
        delay,
        priority,
        job_id,
        dedup_id,
        dedup_ttl,
        METRICS_RETENTION_MS,
        concurrency_key,
    ]


def completed_args(
    *,
    job_id: str,
    returnvalue: str,
    now: int,
    token: str,
    fetch: str,
    lock_duration: int,
    rl_max: int,
    rl_duration: int,
    global_concurrency: int,
) -> list[str | int]:
    """ARGV for MOVE_TO_COMPLETED."""
    return [
        job_id,
        returnvalue,
        now,
        token,
        fetch,
        lock_duration,
        rl_max,
        rl_duration,
        METRICS_RETENTION_MS,
        global_concurrency,
    ]


def failed_args(
    *,
    job_id: str,
    reason: str,
    now: int,
    attempts_made: int,
    max_attempts: int,
    backoff: int,
    token: str,
    fetch: str,
    lock_duration: int,
    rl_max: int,
    rl_duration: int,
    global_concurrency: int,
) -> list[str | int]:
    """ARGV for MOVE_TO_FAILED."""
    return [
        job_id,
        reason,
        now,
        attempts_made,
        max_attempts,
        backoff,
        token,
        fetch,
        lock_duration,
        rl_max,
        rl_duration,
        METRICS_RETENTION_MS,
        global_concurrency,
    ]
