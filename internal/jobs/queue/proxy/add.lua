local heads, payload_key, target = KEYS[1], KEYS[2], KEYS[3]
local member, due = ARGV[1], tonumber(ARGV[2])
local owned_queue, owned_score

-- A half-millisecond score marks a lease. Inspect every supported shard so
-- an import never moves the member away from its current worker. Keep expired
-- leases too; dequeue will reclaim and fence them in the ordinary way.
for i = 3, #KEYS do
  local score = redis.call('ZSCORE', KEYS[i], member)
  if score and tonumber(score) % 1 == 0.5 then
    owned_queue, owned_score = KEYS[i], tonumber(score)
    break
  end
end

redis.call('SET', payload_key, ARGV[3])
local queue = owned_queue or target
redis.call('ZADD', queue, 'NX', due, member)
for i = 3, #KEYS do
  if KEYS[i] ~= queue then redis.call('ZREM', KEYS[i], member) end
end
redis.call('ZADD', heads, 'LT', owned_score or due, queue)
return 1
