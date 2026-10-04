local updated = 0
for i = 1, #ARGV, 2 do
  local score = redis.call('ZSCORE', KEYS[1], ARGV[i])
  -- Integer scores are scheduled work. A half-millisecond marks a worker's
  -- lease, which bulk scheduling must never overwrite, even after expiry.
  if score and tonumber(score) % 1 ~= 0.5 then
    redis.call('ZADD', KEYS[1], 'XX', ARGV[i + 1], ARGV[i])
    updated = updated + 1
  end
end
local head = redis.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES')
if #head == 0 then
  redis.call('ZREM', KEYS[2], KEYS[1])
else
  redis.call('ZADD', KEYS[2], head[2], KEYS[1])
end
return updated
