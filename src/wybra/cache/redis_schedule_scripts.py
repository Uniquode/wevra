from __future__ import annotations

SCHEDULE_CREATE_SCRIPT = """
if redis.call('exists', KEYS[1]) == 1 then
    return {0}
end
if tonumber(redis.call('get', KEYS[5]) or '0') >= tonumber(ARGV[5]) then
    return {-1}
end
redis.call('incr', KEYS[3])
local revision = redis.call('get', KEYS[3])
redis.call(
    'hset',
    KEYS[1],
    'i', ARGV[1],
    'p', ARGV[3],
    'r', revision,
    'd', ARGV[2],
    'n', ARGV[4]
)
redis.call('hset', KEYS[4], ARGV[1], KEYS[1])
redis.call('zadd', KEYS[2], ARGV[2], ARGV[1])
redis.call('incr', KEYS[5])
return {1, revision}
"""

SCHEDULE_UPDATE_SCRIPT = """
if redis.call('exists', KEYS[2]) == 1 then
    return {0}
end
if redis.call('hget', KEYS[1], 'r') ~= ARGV[1] then
    return {0}
end
redis.call('incr', KEYS[4])
local revision = redis.call('get', KEYS[4])
redis.call(
    'hset',
    KEYS[1],
    'p', ARGV[3],
    'r', revision,
    'd', ARGV[2],
    'n', ARGV[4]
)
redis.call('zadd', KEYS[3], ARGV[2], ARGV[5])
return {1, revision}
"""

SCHEDULE_DUE_SCRIPT = """
local limit = tonumber(ARGV[2])
local recovery_limit = 100
local now = redis.call('time')
local now_seconds = tonumber(now[1]) + tonumber(now[2]) / 1000000
local expired = redis.call(
    'zrangebyscore', KEYS[2], '-inf', now_seconds, 'LIMIT', 0, recovery_limit
)
for _, identity in ipairs(expired) do
    local record_key = redis.call('hget', KEYS[3], identity)
    if record_key then
        local due_at = redis.call('hget', record_key, 'd')
        if due_at then
            redis.call('zadd', KEYS[1], due_at, identity)
        end
    end
    redis.call('zrem', KEYS[2], identity)
end
local identities = redis.call(
    'zrangebyscore', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, limit
)
local result = {}
for _, identity in ipairs(identities) do
    local record_key = redis.call('hget', KEYS[3], identity)
    if record_key then
        local record = redis.call('hmget', record_key, 'i', 'p', 'r', 'd', 'n')
        if record[1] then
            table.insert(result, record)
        end
    end
end
return result
"""

SCHEDULE_CLAIM_SCRIPT = """
if redis.call('exists', KEYS[2]) == 1 then
    return {0}
end
local record = redis.call('hmget', KEYS[1], 'i', 'p', 'r', 'd', 'n')
if not record[1] then
    return {0}
end
local now = redis.call('time')
local now_seconds = tonumber(now[1]) + tonumber(now[2]) / 1000000
if tonumber(record[4]) > now_seconds then
    return {0}
end
redis.call('incr', KEYS[3])
local fencing_token = redis.call('get', KEYS[3])
local expires_at = now_seconds + tonumber(ARGV[3]) / 1000
local expires_at_value = string.format('%.17g', expires_at)
redis.call(
    'hset',
    KEYS[2],
    'h', ARGV[1],
    't', ARGV[2],
    'f', fencing_token,
    'r', record[3]
)
redis.call('pexpire', KEYS[2], ARGV[3])
redis.call('zrem', KEYS[4], record[1])
redis.call('zadd', KEYS[5], expires_at_value, record[1])
return {
    1,
    record[1],
    record[2],
    record[3],
    record[4],
    record[5],
    fencing_token,
    expires_at_value
}
"""

SCHEDULE_RELEASE_SCRIPT = """
if redis.call('hget', KEYS[1], 'r') ~= ARGV[4]
    or redis.call('hget', KEYS[2], 'h') ~= ARGV[1]
    or redis.call('hget', KEYS[2], 't') ~= ARGV[2]
    or redis.call('hget', KEYS[2], 'f') ~= ARGV[3]
    or redis.call('hget', KEYS[2], 'r') ~= ARGV[4]
then
    return {0}
end
local due_at = redis.call('hget', KEYS[1], 'd')
redis.call('del', KEYS[2])
redis.call('zrem', KEYS[4], ARGV[5])
redis.call('zadd', KEYS[3], due_at, ARGV[5])
return {1}
"""

SCHEDULE_COMPLETE_SCRIPT = """
if redis.call('hget', KEYS[1], 'r') ~= ARGV[4]
    or redis.call('hget', KEYS[3], 'h') ~= ARGV[1]
    or redis.call('hget', KEYS[3], 't') ~= ARGV[2]
    or redis.call('hget', KEYS[3], 'f') ~= ARGV[3]
    or redis.call('hget', KEYS[3], 'r') ~= ARGV[4]
then
    return {0}
end
local record = redis.call('hmget', KEYS[1], 'i', 'p', 'r', 'd', 'n')
if not record[1] then
    return {0}
end
if record[5] == '' then
    redis.call('del', KEYS[3], KEYS[1])
    redis.call('zrem', KEYS[2], record[1])
    redis.call('zrem', KEYS[6], record[1])
    redis.call('hdel', KEYS[7], record[1])
    redis.call('decr', KEYS[5])
    return {1, 0}
end
local now = redis.call('time')
local now_seconds = tonumber(now[1]) + tonumber(now[2]) / 1000000
local interval = tonumber(record[5])
local next_due_at = tonumber(record[4]) + interval
if next_due_at <= now_seconds then
    local elapsed = now_seconds - tonumber(record[4])
    next_due_at = tonumber(record[4])
        + (math.floor(elapsed / interval) + 1) * interval
    if next_due_at <= now_seconds then
        next_due_at = now_seconds + 0.000001
    end
end
if next_due_at ~= next_due_at
    or next_due_at == math.huge
    or next_due_at == -math.huge
then
    return {-2}
end
local next_due_at_value = string.format('%.17g', next_due_at)
redis.call('incr', KEYS[4])
local revision = redis.call('get', KEYS[4])
redis.call('hset', KEYS[1], 'r', revision, 'd', next_due_at_value)
redis.call('zadd', KEYS[2], next_due_at_value, record[1])
redis.call('zrem', KEYS[6], record[1])
redis.call('del', KEYS[3])
return {
    1,
    1,
    record[1],
    record[2],
    revision,
    next_due_at_value,
    record[5]
}
"""

__all__ = (
    "SCHEDULE_CLAIM_SCRIPT",
    "SCHEDULE_COMPLETE_SCRIPT",
    "SCHEDULE_CREATE_SCRIPT",
    "SCHEDULE_DUE_SCRIPT",
    "SCHEDULE_RELEASE_SCRIPT",
    "SCHEDULE_UPDATE_SCRIPT",
)
