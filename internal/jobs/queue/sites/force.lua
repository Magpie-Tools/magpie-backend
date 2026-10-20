-- An explicit requeue resets mode progress, but cannot shorten Retry-After.
-- An active claim already represents queued work. Join it instead of revoking
-- the worker before it can commit a response's provider deadline.
if redis.call('GET', KEYS[6]) then
  return 1
end
local raw = redis.call('GET', KEYS[4])
local retry_after = 0
if raw then
  retry_after = tonumber(cjson.decode(raw).retry_after_ms) or 0
end
if ARGV[3] ~= '' then
  redis.call('SET', KEYS[3], ARGV[3])
elseif not redis.call('ZSCORE', KEYS[1], ARGV[1]) then
  return 0
end
if retry_after > 0 then
  redis.call('SET', KEYS[4], cjson.encode({retry_after_ms = retry_after}))
else
  redis.call('DEL', KEYS[4])
end
redis.call('DEL', KEYS[6])
if KEYS[1] ~= KEYS[5] then
  redis.call('ZREM', KEYS[5], ARGV[1])
end
redis.call('ZADD', KEYS[1], math.max(tonumber(ARGV[2]), retry_after), ARGV[1])
local head = redis.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES')
redis.call('ZADD', KEYS[2], head[2], KEYS[1])
return 1
