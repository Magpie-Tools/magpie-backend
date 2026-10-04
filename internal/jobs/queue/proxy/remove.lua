if tonumber(ARGV[2]) > 0 then
  local score = redis.call('ZSCORE', KEYS[2], ARGV[1])
  if not score or tonumber(score) ~= tonumber(ARGV[2]) + 0.5 then return 0 end
  -- An import may publish new owners while this worker is removing its old
  -- orphan. Leave the route scheduled when ownership has changed.
  if redis.call('GET', KEYS[1]) ~= ARGV[3] then return 0 end
end
redis.call('DEL', KEYS[1])
redis.call('ZREM', KEYS[2], ARGV[1])
if KEYS[3] ~= KEYS[2] then redis.call('ZREM', KEYS[3], ARGV[1]) end
return 1
