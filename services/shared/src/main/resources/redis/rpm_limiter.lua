-- KEYS[1] = rpm sorted set key
-- ARGV[1] = now_ms (unused, kept only for call-signature compatibility —
-- see below), ARGV[2] = window_ms, ARGV[3] = limit, ARGV[4] = member
--
-- The window boundary uses Redis's own clock (TIME), not the client-supplied
-- ARGV[1] timestamp. A client timestamp is captured before the network round
-- trip to Redis, so under load there's a small gap between "when now was
-- captured" and "when this script actually runs" — during that gap the
-- eviction boundary is based on a slightly stale now, which can let a
-- handful of extra admissions slip past the limit right at the boundary.
-- Redis's TIME is safe to use here: EVAL scripts get a fixed, consistent
-- time value for replication/AOF, so this stays deterministic.
local time = redis.call('TIME')
local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
local window = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])
local member = ARGV[4]
redis.call('ZREMRANGEBYSCORE', KEYS[1], 0, now - window)
local count = redis.call('ZCARD', KEYS[1])
if count < limit then
  redis.call('ZADD', KEYS[1], now, member)
  redis.call('PEXPIRE', KEYS[1], window + 1000)
  return 1
end
return 0
