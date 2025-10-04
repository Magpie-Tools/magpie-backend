local result = redis.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES')
if #result == 0 then return nil end

local member = result[1]
local score = tonumber(result[2])
local current_time = tonumber(ARGV[1])

if score > current_time then return nil end

local proxy_key = KEYS[2] .. member
local proxy_data = redis.call('GET', proxy_key)

if redis.call('ZREM', KEYS[1], member) == 0 then return nil end
redis.call('DEL', proxy_key)

return {member, proxy_data, score}
