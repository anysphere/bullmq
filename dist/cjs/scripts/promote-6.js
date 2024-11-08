"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.promote = void 0;
const content = `--[[
  Promotes a job that is currently "delayed" to the "waiting" state
    Input:
      KEYS[1] 'delayed'
      KEYS[2] 'wait'
      KEYS[3] 'paused'
      KEYS[4] 'meta'
      KEYS[5] 'priority'
      KEYS[6] 'event stream'
      ARGV[1]  queue.toKey('')
      ARGV[2]  jobId
    Output:
       0 - OK
      -3 - Job not in delayed zset.
    Events:
      'waiting'
]]
local rcall = redis.call
local jobId = ARGV[2]
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
if rcall("ZREM", KEYS[1], jobId) == 1 then
  local priority = tonumber(rcall("HGET", ARGV[1] .. jobId, "priority")) or 0
  local target = getTargetQueueList(KEYS[4], KEYS[2], KEYS[3])
  -- Remove delayed "marker" from the wait list if there is any.
  -- Since we are adding a job we do not need the marker anymore.
  local marker = rcall("LINDEX", target, 0)
  if marker and string.sub(marker, 1, 2) == "0:" then
    rcall("LPOP", target)
  end
  if priority == 0 then
    -- LIFO or FIFO
    rcall("LPUSH", target, jobId)
  else
    -- Priority add
    addJobWithPriority(KEYS[5], priority, target, jobId)
  end
  -- Emit waiting event (wait..ing@token)
  rcall("XADD", KEYS[6], "*", "event", "waiting", "jobId", jobId, "prev", "delayed");
  rcall("HSET", ARGV[1] .. jobId, "delay", 0)
  return 0
else
  return -3
end`;
exports.promote = {
    name: 'promote',
    content,
    keys: 6,
};
//# sourceMappingURL=promote-6.js.map