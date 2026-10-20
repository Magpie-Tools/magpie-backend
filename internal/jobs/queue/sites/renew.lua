if redis.call('GET', KEYS[4]) ~= ARGV[2] or
   not redis.call('ZSCORE', KEYS[1], ARGV[1]) or
   redis.call('EXISTS', KEYS[3]) == 0 then
  return 0
end
redis.call('PEXPIRE', KEYS[4], ARGV[4])
redis.call('ZADD', KEYS[1], ARGV[3], ARGV[1])
local head = redis.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES')
redis.call('ZADD', KEYS[2], head[2], KEYS[1])
return 1
