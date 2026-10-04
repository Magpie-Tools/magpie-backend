local heads, old_payload_key, payload_key, old_queue, target = KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5]
local old_member, member = ARGV[1], ARGV[2]
local expected, now = tonumber(ARGV[3]), tonumber(ARGV[4])
local lease_score = expected + 0.5

local function refresh_head(queue)
  local head = redis.call('ZRANGE', queue, 0, 0, 'WITHSCORES')
  if #head == 0 then
    redis.call('ZREM', heads, queue)
  else
    redis.call('ZADD', heads, head[2], queue)
  end
end

-- Decoding and fingerprinting occur before this exceptional script. Fence a
-- candidate that was reclaimed, deleted, or updated in that interval.
local old_score = redis.call('ZSCORE', old_queue, old_member)
if not old_score or tonumber(old_score) ~= lease_score or expected <= now then
  return {0, '', ''}
end
local source = redis.call('GET', old_payload_key)
if source ~= ARGV[6] then
  if source then
    -- We still own this score but its payload changed. Release only our lease
    -- so dequeue can decode the new payload instead of overwriting it.
    redis.call('ZADD', old_queue, 'XX', now, old_member)
  else
    redis.call('ZREM', old_queue, old_member)
  end
  refresh_head(old_queue)
  return {0, '', ''}
end

if old_member == member then
  redis.call('SET', payload_key, ARGV[5])
  return {1, ARGV[5], old_queue}
end

local queues, seen_queues = {}, {}
for i = 4, #KEYS do
  if not seen_queues[KEYS[i]] then
    table.insert(queues, KEYS[i])
    seen_queues[KEYS[i]] = true
  end
end

local owned_queue
for _, queue in ipairs(queues) do
  local score = redis.call('ZSCORE', queue, member)
  if score and tonumber(score) % 1 == 0.5 and tonumber(score) > now then
    owned_queue = queue
    break
  end
end

local normalized = cjson.decode(ARGV[5])
local current = redis.call('GET', payload_key)
local live = normalized
local format_changed = not current
if current then
  live = cjson.decode(current)
  local function present(value) return value and value ~= cjson.null and value ~= '' end
  if (tonumber(live.Version) or 0) >= 3 and present(live.Hash) and live.Hash ~= normalized.Hash then
    return redis.error_reply('canonical proxy payload hash does not match its member')
  end
  format_changed = (tonumber(live.Version) or 0) < 3 or not present(live.Hash)
  if ARGV[7] == 'true' then
    format_changed = format_changed or present(live.Username) or present(live.Password)
  else
    format_changed = format_changed or present(live.UsernameEncrypted) or present(live.PasswordEncrypted)
  end
end

local function owners(p)
  if type(p.WorkspaceIDs) == 'table' and #p.WorkspaceIDs > 0 then return p.WorkspaceIDs end
  if type(p.UserIDs) == 'table' and #p.UserIDs > 0 then return p.UserIDs end
  local ids = {}
  if type(p.Users) == 'table' then
    for _, user in ipairs(p.Users) do table.insert(ids, user.ID) end
  end
  return ids
end

local merged, seen = {}, {}
for _, id in ipairs(owners(live)) do
  if id ~= 0 and not seen[id] then
    table.insert(merged, id)
    seen[id] = true
  end
end
local ownership_changed = false
for _, id in ipairs(owners(normalized)) do
  if id ~= 0 and not seen[id] then
    table.insert(merged, id)
    seen[id] = true
    ownership_changed = true
  end
end

local payload = current or ARGV[5]
if current and (format_changed or ownership_changed) then
  if format_changed then
    live.Version, live.Hash = normalized.Version, normalized.Hash
    live.Username, live.Password = normalized.Username, normalized.Password
    live.UsernameEncrypted, live.PasswordEncrypted = normalized.UsernameEncrypted, normalized.PasswordEncrypted
  end
  live.WorkspaceIDs, live.UserIDs, live.Users = nil, nil, nil
  if #merged > 0 then live.WorkspaceIDs = merged end
  payload = cjson.encode(live)
end
if payload ~= current then redis.call('SET', payload_key, payload) end

-- Consume the old alias even if another worker owns the current hash. Its
-- lease, shard, credentials, and ID stay authoritative; never grant a second
-- check merely because this candidate has a later expiry.
redis.call('DEL', old_payload_key)
local queue = owned_queue or target
local dirty = {}
for _, key in ipairs(queues) do
  if redis.call('ZREM', key, old_member) > 0 then dirty[key] = true end
  if key ~= queue and redis.call('ZREM', key, member) > 0 then dirty[key] = true end
end
if not owned_queue then
  redis.call('ZADD', queue, lease_score, member)
  dirty[queue] = true
end
for key, _ in pairs(dirty) do refresh_head(key) end

if owned_queue then return {0, '', ''} end
return {1, payload, queue}
