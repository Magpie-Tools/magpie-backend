local heads, payload_key, target = KEYS[1], KEYS[2], KEYS[3]
local member, due = ARGV[1], tonumber(ARGV[2])
-- An explicit requeue takes the member from a worker that holds it: the
-- member is scheduled like a waiting one, and the worker's completion finds
-- its lease gone in whichever shard it was.
local take = ARGV[4] == '1'
local owned_queue, owned_score, queued

-- A half-millisecond score marks a lease. Inspect every supported shard so
-- an import never moves the member away from its current worker. Keep expired
-- leases too; dequeue will reclaim and fence them in the ordinary way.
for i = 3, #KEYS do
  local score = redis.call('ZSCORE', KEYS[i], member)
  if score then
    score = tonumber(score)
    if score % 1 == 0.5 and not take then
      owned_queue, owned_score = KEYS[i], score
      break
    end
    if not queued or score < queued then queued = score end
  end
end

redis.call('SET', payload_key, ARGV[3])
local queue = owned_queue or target
if not owned_queue then
  -- The due time is the earliest cadence point of all the route's owners.
  -- A waiting member keeps an earlier due time, in whichever shard it waits,
  -- and takes a sooner one: this import may have added an owner with a faster
  -- cadence. A leased member is left to its worker, which schedules it when
  -- the check completes.
  if queued and queued < due then due = queued end
  redis.call('ZADD', queue, due, member)
end
for i = 3, #KEYS do
  if KEYS[i] ~= queue then redis.call('ZREM', KEYS[i], member) end
end
redis.call('ZADD', heads, 'LT', owned_score or due, queue)
return 1
