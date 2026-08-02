from __future__ import annotations

SCHEDULE_DUE_ORDER_FUNCTION = """
-- Keep this ordering identical to redis_schedule_ordering.encode_due_order.
-- It emits a fixed-width key whose lexicographic order matches numeric due time.
local function due_order(value)
    if value == 0 then
        return '1000' .. string.rep('0', 18)
    end
    local absolute = math.abs(value)
    local scientific = string.format('%.17e', absolute)
    local mantissa, exponent = string.match(
        scientific, '([0-9])%.[0-9]+e([+-][0-9]+)'
    )
    local exponent_code = tonumber(exponent) + 500
    local digits = mantissa .. string.match(scientific, '[0-9]%.([0-9]+)e')
    if value > 0 then
        return '1' .. string.format('%03d', exponent_code) .. digits
    end
    local inverted = string.gsub(digits, '%d', function(digit)
        return string.char(string.byte('9') - (string.byte(digit) - string.byte('0')))
    end)
    return '0' .. string.format('%03d', 999 - exponent_code) .. inverted
end
"""

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
    'n', ARGV[4],
    'm', ARGV[6]
)
redis.call('hset', KEYS[4], ARGV[1], KEYS[1])
redis.call('zadd', KEYS[2], ARGV[2], ARGV[1])
redis.call('zadd', KEYS[6], 0, ARGV[6])
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
local previous_member = redis.call('hget', KEYS[1], 'm')
redis.call('incr', KEYS[4])
local revision = redis.call('get', KEYS[4])
redis.call(
    'hset',
    KEYS[1],
    'p', ARGV[3],
    'r', revision,
    'd', ARGV[2],
    'n', ARGV[4],
    'm', ARGV[6]
)
redis.call('zadd', KEYS[3], ARGV[2], ARGV[5])
if previous_member then
    redis.call('zrem', KEYS[5], previous_member)
end
redis.call('zadd', KEYS[5], 0, ARGV[6])
return {1, revision}
"""

SCHEDULE_DELETE_SCRIPT = """
if redis.call('exists', KEYS[1]) == 0 then
    return {0}
end
local member = redis.call('hget', KEYS[1], 'm')
redis.call('del', KEYS[1], KEYS[3])
redis.call('zrem', KEYS[2], ARGV[1])
if member then
    redis.call('zrem', KEYS[7], member)
end
redis.call('zrem', KEYS[4], ARGV[1])
redis.call('hdel', KEYS[5], ARGV[1])
redis.call('decr', KEYS[6])
return {1}
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
        local member = redis.call('hget', record_key, 'm')
        if due_at and member then
            redis.call('zadd', KEYS[1], due_at, identity)
            redis.call('zadd', KEYS[4], 0, member)
        end
    end
    redis.call('zrem', KEYS[2], identity)
end
local before = tonumber(ARGV[1])
local before_member = ARGV[3]
local after_member = ARGV[4]
local entries = {}
if after_member == '' then
    local identities = redis.call(
        'zrangebyscore', KEYS[1], '-inf', before, 'LIMIT', 0, limit
    )
    for _, identity in ipairs(identities) do
        table.insert(entries, identity)
    end
else
    entries = redis.call(
        'zrangebylex', KEYS[4], '(' .. after_member, '[' .. before_member,
        'LIMIT', 0, limit
    )
end
local result = {}
for _, entry in ipairs(entries) do
    local identity = entry
    local member = nil
    if after_member ~= '' then
        member = entry
        local separator = string.find(member, ':', 1, true)
        identity = separator and string.sub(member, separator + 1) or nil
    end
    if identity then
        local record_key = redis.call('hget', KEYS[3], identity)
        if record_key then
            local record = redis.call(
                'hmget', record_key, 'i', 'p', 'r', 'd', 'n', 'm'
            )
            if record[1] and (member == nil or record[6] == member) then
                table.insert(
                    result, {record[1], record[2], record[3], record[4], record[5]}
                )
            else
                redis.call('zrem', KEYS[1], identity)
                if member then
                    redis.call('zrem', KEYS[4], member)
                end
            end
        else
            redis.call('zrem', KEYS[1], identity)
            if member then
                redis.call('zrem', KEYS[4], member)
            end
        end
    elseif member then
        redis.call('zrem', KEYS[4], member)
    end
end
return result
"""

SCHEDULE_CLAIM_SCRIPT = """
if redis.call('exists', KEYS[2]) == 1 then
    return {0}
end
local record = redis.call('hmget', KEYS[1], 'i', 'p', 'r', 'd', 'n', 'm')
if not record[1] then
    return {0}
end
local now = redis.call('time')
local now_seconds = tonumber(now[1]) + tonumber(now[2]) / 1000000
if tonumber(record[4]) > now_seconds then
    return {0}
end
local member = record[6]
if not member then
    member = due_order(tonumber(record[4])) .. ':' .. record[1]
    redis.call('hset', KEYS[1], 'm', member)
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
if member then
    redis.call('zrem', KEYS[6], member)
end
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
local member = redis.call('hget', KEYS[1], 'm')
if due_at and not member then
    member = due_order(tonumber(due_at)) .. ':' .. ARGV[5]
    redis.call('hset', KEYS[1], 'm', member)
end
redis.call('del', KEYS[2])
redis.call('zrem', KEYS[4], ARGV[5])
redis.call('zadd', KEYS[3], due_at, ARGV[5])
if member then
    redis.call('zadd', KEYS[5], 0, member)
end
return {1}
"""

SCHEDULE_COMPLETE_SCRIPT = (
    SCHEDULE_DUE_ORDER_FUNCTION
    + """
if redis.call('hget', KEYS[1], 'r') ~= ARGV[4]
    or redis.call('hget', KEYS[3], 'h') ~= ARGV[1]
    or redis.call('hget', KEYS[3], 't') ~= ARGV[2]
    or redis.call('hget', KEYS[3], 'f') ~= ARGV[3]
    or redis.call('hget', KEYS[3], 'r') ~= ARGV[4]
then
    return {0}
end
local record = redis.call('hmget', KEYS[1], 'i', 'p', 'r', 'd', 'n', 'm')
if not record[1] then
    return {0}
end
local member = record[6]
if not member then
    member = due_order(tonumber(record[4])) .. ':' .. record[1]
end
if record[5] == '' then
    redis.call('del', KEYS[3], KEYS[1])
    redis.call('zrem', KEYS[2], record[1])
    redis.call('zrem', KEYS[6], record[1])
    redis.call('hdel', KEYS[7], record[1])
    if member then
        redis.call('zrem', KEYS[8], member)
    end
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
local next_member = due_order(next_due_at) .. ':' .. record[1]
redis.call('incr', KEYS[4])
local revision = redis.call('get', KEYS[4])
redis.call('hset', KEYS[1], 'r', revision, 'd', next_due_at_value, 'm', next_member)
redis.call('zadd', KEYS[2], next_due_at_value, record[1])
redis.call('zadd', KEYS[8], 0, next_member)
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
)

SCHEDULE_DISCARD_SCRIPT = """
if redis.call('hget', KEYS[1], 'r') ~= ARGV[4]
    or redis.call('hget', KEYS[3], 'h') ~= ARGV[1]
    or redis.call('hget', KEYS[3], 't') ~= ARGV[2]
    or redis.call('hget', KEYS[3], 'f') ~= ARGV[3]
    or redis.call('hget', KEYS[3], 'r') ~= ARGV[4]
then
    return {0}
end
local member = redis.call('hget', KEYS[1], 'm')
redis.call('del', KEYS[3], KEYS[1])
redis.call('zrem', KEYS[2], ARGV[5])
redis.call('zrem', KEYS[4], ARGV[5])
redis.call('hdel', KEYS[5], ARGV[5])
if member then
    redis.call('zrem', KEYS[7], member)
end
redis.call('decr', KEYS[6])
return {1}
"""

SCHEDULE_ADVANCE_SCRIPT = """
if redis.call('hget', KEYS[1], 'r') ~= ARGV[4]
    or redis.call('hget', KEYS[3], 'h') ~= ARGV[1]
    or redis.call('hget', KEYS[3], 't') ~= ARGV[2]
    or redis.call('hget', KEYS[3], 'f') ~= ARGV[3]
    or redis.call('hget', KEYS[3], 'r') ~= ARGV[4]
then
    return {0}
end
if redis.call('exists', KEYS[1]) == 0 then
    return {0}
end
redis.call('incr', KEYS[4])
local revision = redis.call('get', KEYS[4])
redis.call(
    'hset',
    KEYS[1],
    'p', ARGV[6],
    'r', revision,
    'd', ARGV[5],
    'm', ARGV[8]
)
redis.call('zadd', KEYS[2], ARGV[5], ARGV[7])
redis.call('zrem', KEYS[5], ARGV[7])
redis.call('zadd', KEYS[6], 0, ARGV[8])
redis.call('del', KEYS[3])
return {1, revision}
"""

SCHEDULE_HELD_SCRIPT = """
if redis.call('hget', KEYS[1], 'r') ~= ARGV[4]
    or redis.call('hget', KEYS[2], 'h') ~= ARGV[1]
    or redis.call('hget', KEYS[2], 't') ~= ARGV[2]
    or redis.call('hget', KEYS[2], 'f') ~= ARGV[3]
    or redis.call('hget', KEYS[2], 'r') ~= ARGV[4]
then
    return {0}
end
return {1}
"""

__all__ = (
    "SCHEDULE_ADVANCE_SCRIPT",
    "SCHEDULE_CLAIM_SCRIPT",
    "SCHEDULE_COMPLETE_SCRIPT",
    "SCHEDULE_CREATE_SCRIPT",
    "SCHEDULE_DISCARD_SCRIPT",
    "SCHEDULE_DELETE_SCRIPT",
    "SCHEDULE_DUE_ORDER_FUNCTION",
    "SCHEDULE_DUE_SCRIPT",
    "SCHEDULE_HELD_SCRIPT",
    "SCHEDULE_RELEASE_SCRIPT",
    "SCHEDULE_UPDATE_SCRIPT",
)
