local heads, queue, legacy, payload_key = KEYS[1], KEYS[2], KEYS[3], KEYS[4]
local member, expected, next_score = ARGV[1], tonumber(ARGV[2]), tonumber(ARGV[3])
local old_queue = KEYS[5]

if expected > 0 then
  local score = redis.call('ZSCORE', old_queue, member)
  if not score or tonumber(score) ~= expected + 0.5 then return 0 end
end

if ARGV[4] ~= '' then
  local payload = ARGV[4]
  local current = redis.call('GET', payload_key)
  if current and ARGV[5] ~= '' and current ~= ARGV[5] then
    -- Apply only this worker's ownership delta to the current payload. Keep
    -- credentials and workspaces published while the check was running.
    local live, before, after = cjson.decode(current), cjson.decode(ARGV[5]), cjson.decode(payload)
    local function owners(p)
      local ids = p.WorkspaceIDs or p.UserIDs
      if ids then return ids end
      ids = {}
      for _, u in ipairs(p.Users or {}) do table.insert(ids, u.ID) end
      return ids
    end
    local old, desired = {}, {}
    for _, id in ipairs(owners(before)) do old[id] = true end
    for _, id in ipairs(owners(after)) do desired[id] = true end
    local merged, seen = {}, {}
    for _, id in ipairs(owners(live)) do
      if (not old[id] or desired[id]) and not seen[id] then
        table.insert(merged, id)
        seen[id] = true
      end
    end
    for id, _ in pairs(desired) do
      if not old[id] and not seen[id] then table.insert(merged, id) end
    end
    live.WorkspaceIDs = nil
    if #merged > 0 then live.WorkspaceIDs = merged end
    live.UserIDs, live.Users = nil, nil
    payload = cjson.encode(live)
  end
  redis.call('SET', payload_key, payload)
end

if old_queue ~= queue then redis.call('ZREM', old_queue, member) end
if legacy ~= queue and legacy ~= old_queue then redis.call('ZREM', legacy, member) end
redis.call('ZADD', queue, next_score, member)
redis.call('ZADD', heads, 'LT', next_score, queue)
return 1
