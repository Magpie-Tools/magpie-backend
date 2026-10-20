local queue_key = KEYS[1]
local score = redis.call('ZSCORE', queue_key, ARGV[1])
if not score and KEYS[6] ~= queue_key then
  queue_key = KEYS[6]
  score = redis.call('ZSCORE', queue_key, ARGV[1])
end
if redis.call('EXISTS', KEYS[3]) == 0 then
  return 0
end

local incoming = cjson.decode(ARGV[3])
local stored = cjson.decode(redis.call('GET', KEYS[4]) or '{}')
local retry_after = math.max(tonumber(stored.retry_after_ms) or 0, tonumber(incoming.retry_after_ms) or 0)
local claim = redis.call('GET', KEYS[5])

-- A provider deadline is an observation about the URL, independent of claim
-- ownership. Preserve it without letting a stale worker replace mode progress
-- or disturb a newer worker's active lease.
if not score or claim ~= ARGV[4] then
  if retry_after > (tonumber(stored.retry_after_ms) or 0) then
    stored.retry_after_ms = retry_after
    redis.call('SET', KEYS[4], cjson.encode(stored))
  end
  if score and not claim and retry_after > tonumber(score) then
    redis.call('ZADD', queue_key, retry_after, ARGV[1])
    local head = redis.call('ZRANGE', queue_key, 0, 0, 'WITHSCORES')
    redis.call('ZADD', KEYS[2], head[2], queue_key)
  end
  return 0
end

-- The claim token stays stable across renewals; its sorted-set score changes.
if retry_after > 0 then
  incoming.retry_after_ms = retry_after
end
redis.call('SET', KEYS[4], cjson.encode(incoming))
redis.call('DEL', KEYS[5])
redis.call('ZADD', queue_key, math.max(tonumber(ARGV[2]), retry_after), ARGV[1])
local head = redis.call('ZRANGE', queue_key, 0, 0, 'WITHSCORES')
redis.call('ZADD', KEYS[2], head[2], queue_key)
return 1
