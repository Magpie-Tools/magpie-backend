local score = redis.call('ZSCORE', KEYS[1], ARGV[1])
local expected, now, renewed = tonumber(ARGV[2]), tonumber(ARGV[3]), tonumber(ARGV[4])
if not score or tonumber(score) ~= expected + 0.5 or expected <= now then return 0 end
redis.call('ZADD', KEYS[1], 'XX', renewed + 0.5, ARGV[1])
-- An earlier indexed head is safe: pop repairs it from the current shard.
return renewed
