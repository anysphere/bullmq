"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.moveJobFromActiveToWait = void 0;
const content = `--[[
  Function to move job from active state to wait.
  Input:
    KEYS[1] active key
    KEYS[2] wait key
    KEYS[3] stalled key
    KEYS[4] job lock key
    KEYS[5] paused key
    KEYS[6] meta key
    KEYS[7] limiter key
    KEYS[8] priority key
    KEYS[9] event key
    ARGV[1] job id
    ARGV[2] lock token
    ARGV[3] job id key
]]
local rcall = redis.call
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
local jobId = ARGV[1]
local token = ARGV[2]
local lockKey = KEYS[4]
local lockToken = rcall("GET", lockKey)
local pttl = rcall("PTTL", KEYS[7])
if lockToken == token and pttl > 0 then
  local removed = rcall("LREM", KEYS[1], 1, jobId)
  if (removed > 0) then
    local target = getTargetQueueList(KEYS[6], KEYS[2], KEYS[5])
    rcall("SREM", KEYS[3], jobId)
    local priority = tonumber(rcall("HGET", ARGV[3], "priority")) or 0
    if priority > 0 then
      pushBackJobWithPriority(KEYS[8], priority, target, jobId)
    else
      rcall("RPUSH", target, jobId)
    end
    rcall("DEL", lockKey)
    -- Emit waiting event
    rcall("XADD", KEYS[9], "*", "event", "waiting", "jobId", jobId)
  end
end
return pttl
`;
exports.moveJobFromActiveToWait = {
    name: 'moveJobFromActiveToWait',
    content,
    keys: 9,
};
//# sourceMappingURL=moveJobFromActiveToWait-9.js.map