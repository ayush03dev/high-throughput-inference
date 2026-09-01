-- KEYS[1] = tpm sorted set (member=requestId, score=timestamp) + parallel hash for token weights
-- Uses ZADD with score=timestamp and stores token count in a hash tpm_tokens:{model}
-- ARGV[1]=now_ms ARGV[2]=window_ms ARGV[3]=tpm_limit ARGV[4]=tokens ARGV[5]=member
local now = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])
local tokens = tonumber(ARGV[4])
local member = ARGV[5]
local tokensKey = KEYS[1] .. ":tokens"

redis.call('ZREMRANGEBYSCORE', KEYS[1], 0, now - window)
local members = redis.call('ZRANGEBYSCORE', KEYS[1], now - window, now)
local total = 0
for _, m in ipairs(members) do
  local t = redis.call('HGET', tokensKey, m)
  if t then total = total + tonumber(t) end
end

if total + tokens <= limit then
  redis.call('ZADD', KEYS[1], now, member)
  redis.call('HSET', tokensKey, member, tokens)
  redis.call('PEXPIRE', KEYS[1], window + 1000)
  redis.call('PEXPIRE', tokensKey, window + 1000)
  -- cleanup orphaned hash entries for removed members
  return 1
end
return 0
