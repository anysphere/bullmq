const content = `--[[
  Move next job to be processed to active, lock it and fetch its data. The job
  may be delayed, in that case we need to move it to the delayed set instead.
  This operation guarantees that the worker owns the job during the lock
  expiration time. The worker is responsible of keeping the lock fresh
  so that no other worker picks this job again.
  Input:
    KEYS[1] wait key
    KEYS[2] active key
    KEYS[3] priority key
    KEYS[4] stream events key
    KEYS[5] stalled key
    -- Rate limiting
    KEYS[6] rate limiter key
    KEYS[7] delayed key
    -- Promote delayed jobs
    KEYS[8] paused key
    KEYS[9] meta key
    -- Arguments
    ARGV[1] key prefix
    ARGV[2] timestamp
    ARGV[3] optional job ID
    ARGV[4] opts
    opts - token - lock token
    opts - lockDuration
    opts - limiter
]]
local jobId
local rcall = redis.call
-- Includes
--[[
  Function to move job from wait state to active.
  Input:
    keys[1] wait key
    keys[2] active key
    keys[3] priority key
    keys[4] stream events key
    keys[5] stalled key
    -- Rate limiting
    keys[6] rate limiter key
    keys[7] delayed key
    opts - token - lock token
    opts - lockDuration
    opts - limiter
]]
-- Includes
--[[
  Function to add push back job considering priority in front of same prioritized jobs.
]]
local function pushBackJobWithPriority(priorityKey, priority, targetKey, jobId)
  rcall("ZADD", priorityKey, priority, jobId)
  local count = rcall("ZCOUNT", priorityKey, 0, priority-1)
  local len = rcall("LLEN", targetKey)
  local id = rcall("LINDEX", targetKey, len - count)
  rcall("ZADD", priorityKey, priority, jobId)
  if id then
    rcall("LINSERT", targetKey, "BEFORE", id, jobId)
  else
    rcall("RPUSH", targetKey, jobId)
  end
end
local function moveJobFromWaitToActive(keys, keyPrefix, targetKey, jobId, processedOn,
    maxJobs, expireTime, opts)
  rcall("ZREM", keys[3], jobId) -- remove from priority
  local jobKey = keyPrefix .. jobId
  -- Check if we need to perform rate limiting.
  if maxJobs then
    local rateLimiterKey = keys[6];
    -- check if we passed rate limit, we need to remove the job and return expireTime
    if expireTime > 0 then
      -- remove from active queue and add back to the wait list
      rcall("LREM", keys[2], 1, jobId)
      local priority = tonumber(rcall("HGET", jobKey, "priority")) or 0
      if priority > 0 then
        pushBackJobWithPriority(keys[3], priority, targetKey, jobId)
      else
        rcall("RPUSH", targetKey, jobId)
      end
      -- Return when we can process more jobs
      return {0, 0, expireTime, 0}
    end
    local jobCounter = tonumber(rcall("INCR", rateLimiterKey))
    if jobCounter == 1 then
      local limiterDuration = opts['limiter'] and opts['limiter']['duration']
      local integerDuration = math.floor(math.abs(limiterDuration))
      rcall("PEXPIRE", rateLimiterKey, integerDuration)
    end
  end
  local lockKey = jobKey .. ':lock'
  -- get a lock
  if opts['token'] ~= "0" then
    rcall("SET", lockKey, opts['token'], "PX", opts['lockDuration'])
  end
  rcall("XADD", keys[4], "*", "event", "active", "jobId", jobId, "prev", "waiting")
  rcall("HSET", jobKey, "processedOn", processedOn)
  rcall("HINCRBY", jobKey, "attemptsMade", 1)
  return {rcall("HGETALL", jobKey), jobId, 0, 0} -- get job data
end
--[[
  Function to return the next delayed job timestamp.
]] 
local function getNextDelayedTimestamp(delayedKey)
  local result = rcall("ZRANGE", delayedKey, 0, 0, "WITHSCORES")
  if #result then
    local nextTimestamp = tonumber(result[2])
    if (nextTimestamp ~= nil) then 
      nextTimestamp = nextTimestamp / 0x1000
    end
    return nextTimestamp
  end
end
local function getRateLimitTTL(maxJobs, rateLimiterKey)
  if maxJobs then
    local pttl = rcall("PTTL", rateLimiterKey)
    if pttl <= 0 then
      rcall("DEL", rateLimiterKey)
    end
    local jobCounter = tonumber(rcall("GET", rateLimiterKey) or 0)
    if jobCounter >= maxJobs then
      if pttl > 0 then
        return pttl
      end
    end
  end
  return 0
end
--[[
  Function to check for the meta.paused key to decide if we are paused or not
  (since an empty list and !EXISTS are not really the same).
]]
local function getTargetQueueList(queueMetaKey, waitKey, pausedKey)
  if rcall("HEXISTS", queueMetaKey, "paused") ~= 1 then
    return waitKey, false
  else
    return pausedKey, true
  end
end
--[[
  Updates the delay set, by moving delayed jobs that should
  be processed now to "wait".
     Events:
      'waiting'
]]
-- Includes
--[[
  Function to add job considering priority.
]]
local function addJobWithPriority(priorityKey, priority, targetKey, jobId)
  rcall("ZADD", priorityKey, priority, jobId)
  local count = rcall("ZCOUNT", priorityKey, 0, priority)
  local len = rcall("LLEN", targetKey)
  local id = rcall("LINDEX", targetKey, len - (count - 1))
  if id then
    rcall("LINSERT", targetKey, "BEFORE", id, jobId)
  else
    rcall("RPUSH", targetKey, jobId)
  end
end
-- Try to get as much as 1000 jobs at once
local function promoteDelayedJobs(delayedKey, targetKey, priorityKey,
                                  eventStreamKey, prefix, timestamp)
    local jobs = rcall("ZRANGEBYSCORE", delayedKey, 0, (timestamp + 1) * 0x1000, "LIMIT", 0, 1000)
    if (#jobs > 0) then
        rcall("ZREM", delayedKey, unpack(jobs))
        for _, jobId in ipairs(jobs) do
            local priority =
                tonumber(rcall("HGET", prefix .. jobId, "priority")) or 0
            if priority == 0 then
                -- LIFO or FIFO
                rcall("LPUSH", targetKey, jobId)
            else
                addJobWithPriority(priorityKey, priority, targetKey, jobId)
            end
            -- Emit waiting event
            rcall("XADD", eventStreamKey, "*", "event", "waiting", "jobId",
                  jobId, "prev", "delayed")
            rcall("HSET", prefix .. jobId, "delay", 0)
        end
    end
end
local target = getTargetQueueList(KEYS[9], KEYS[1], KEYS[8])
-- Check if there are delayed jobs that we can move to wait.
promoteDelayedJobs(KEYS[7], target, KEYS[3], KEYS[4], ARGV[1], ARGV[2])
local opts = cmsgpack.unpack(ARGV[4])
local maxJobs = tonumber(opts['limiter'] and opts['limiter']['max'])
local expireTime = getRateLimitTTL(maxJobs, KEYS[6])
if (ARGV[3] ~= "") then
  jobId = ARGV[3]
  -- clean stalled key
  rcall("SREM", KEYS[5], jobId)
else
  -- Check if we are rate limited first.
  if expireTime > 0 then
    return { 0, 0, expireTime, 0 }
  end
  -- no job ID, try non-blocking move from wait to active
  jobId = rcall("RPOPLPUSH", KEYS[1], KEYS[2])
end
-- If jobId is special ID 0:delay, then there is no job to process
if jobId then
  if string.sub(jobId, 1, 2) == "0:" then
    rcall("LREM", KEYS[2], 1, jobId)
    if expireTime > 0 then
      return { 0, 0, expireTime, 0 }
    end
    -- Move again since we just got the marker job.
    jobId = rcall("RPOPLPUSH", KEYS[1], KEYS[2])
    -- Since it is possible that between a call to BRPOPLPUSH and moveToActive
    -- another script puts a new maker in wait, we need to check again.
    if jobId and string.sub(jobId, 1, 2) == "0:" then
      rcall("LREM", KEYS[2], 1, jobId)
      jobId = rcall("RPOPLPUSH", KEYS[1], KEYS[2])
    end
  end
  if jobId then
    -- this script is not really moving, it is preparing the job for processing
    return moveJobFromWaitToActive(KEYS, ARGV[1], target, jobId, ARGV[2], maxJobs, expireTime, opts)
  end
end
-- Return the timestamp for the next delayed job if any.
local nextTimestamp = getNextDelayedTimestamp(KEYS[7])
if (nextTimestamp ~= nil) then
  return { 0, 0, 0, nextTimestamp}
end
return { 0, 0, 0, 0}
`;
export const moveToActive = {
    name: 'moveToActive',
    content,
    keys: 9,
};
//# sourceMappingURL=moveToActive-9.js.map