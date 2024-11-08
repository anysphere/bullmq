const content = `--[[
  Change job priority
  Input:
    KEYS[1] 'wait',
    KEYS[2] 'paused'
    KEYS[3] 'meta'
    KEYS[4] 'priority'
    ARGV[1] priority value
    ARGV[2] job key
    ARGV[3] job id
    ARGV[4] lifo
    Output:
       0  - OK
      -1  - Missing job
]]
local jobKey = ARGV[2]
local jobId = ARGV[3]
local priority = tonumber(ARGV[1])
local rcall = redis.call
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
if rcall("EXISTS", jobKey) == 1 then
  local target = getTargetQueueList(KEYS[3], KEYS[1], KEYS[2])
  local numRemovedElements = rcall("LREM", target, -1, jobId)
  if numRemovedElements > 0 then
    rcall("ZREM", KEYS[4], jobId)
    -- Standard or priority add
    if priority == 0 then
      -- LIFO or FIFO
      local pushCmd = ARGV[4] == '1' and 'RPUSH' or 'LPUSH';
      rcall(pushCmd, target, jobId)
    else
      -- Priority add
      addJobWithPriority(KEYS[4], priority, target, jobId)
    end
  end
  rcall("HSET", jobKey, "priority", priority)
  return 0
else
  return -1
end
`;
export const changePriority = {
    name: 'changePriority',
    content,
    keys: 4,
};
//# sourceMappingURL=changePriority-4.js.map