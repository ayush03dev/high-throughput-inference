-- Sliding-window TPM limiter using a prefix-sum ledger, not a per-request scan.
--
-- KEYS[1] = ledger sorted set key, e.g. "tpm:{model}"
-- ARGV[1] = now_ms (unused — see below), ARGV[2] = window_ms,
-- ARGV[3] = tpm_limit, ARGV[4] = tokens
-- (ARGV[5] = requestId is accepted for call-signature compatibility with the
-- RPM script but is not needed here — see below.)
--
-- Like rpm_limiter.lua, the window boundary uses Redis's own clock (TIME)
-- rather than the client-supplied ARGV[1], to avoid a stale boundary from
-- client/network latency letting a few extra admissions through right at
-- the edge of the window under load.
--
-- Each accepted request appends one entry: score = now_ms, member = the
-- model's running cumulative token total *after* this request (a string).
-- Because that running total only ever increases, the member is always
-- unique on its own, so no separate per-request id or token hash is needed.
--
-- The token sum for the trailing `window_ms` is then just:
--   (cumulative total as of "now") - (cumulative total just before the window started)
-- Both lookups are a single ZREVRANGEBYSCORE ... LIMIT 0 1, i.e. O(log N)
-- regardless of how many requests fall inside the window — the previous
-- version re-summed every member in the window on every single call
-- (O(window size)), which pegged Redis's single thread once the window held
-- more than a few thousand entries.
--
-- Tradeoff: entries are not pruned on the hot path (pruning safely without
-- risking the "before window" anchor being deleted needs more care than is
-- worth adding here), so ledger size grows for as long as a model sees
-- continuous traffic; it is bounded by PEXPIRE only once a model goes idle
-- for a full window. Every operation stays O(log N) regardless, so this
-- only costs memory, never latency, and is a reasonable tradeoff for this
-- system's scale.
local time = redis.call('TIME')
local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
local window = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])
local tokens = tonumber(ARGV[4])

local latest = redis.call('ZREVRANGE', KEYS[1], 0, 0)
local totalNow = 0
if latest[1] then
  totalNow = tonumber(latest[1])
end

local beforeWindow = redis.call('ZREVRANGEBYSCORE', KEYS[1], '(' .. tostring(now - window), '-inf', 'LIMIT', 0, 1)
local totalBefore = 0
if beforeWindow[1] then
  totalBefore = tonumber(beforeWindow[1])
end

local windowSum = totalNow - totalBefore

if windowSum + tokens <= limit then
  local newTotal = totalNow + tokens
  redis.call('ZADD', KEYS[1], now, tostring(newTotal))
  redis.call('PEXPIRE', KEYS[1], window + 1000)
  return 1
end
return 0
