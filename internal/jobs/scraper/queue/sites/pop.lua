local popped = redis.call('ZPOPMIN', KEYS[1], 1)
if #popped == 0 then
  return nil                                  -- queue empty
end

local member = popped[1]                      -- site url
local score  = tonumber(popped[2])            -- next-due timestamp
local now    = tonumber(ARGV[1])

-- If the next-due time is still in the future, push it back and exit
if score > now then
  redis.call('ZADD', KEYS[1], score, member)  -- restore exactly as it was
  return nil
end

-- Fetch the cached site definition, then delete the key
local site_key  = KEYS[2] .. member
local site_data = redis.call('GET', site_key)
redis.call('DEL', site_key)

return { member, site_data, score }
