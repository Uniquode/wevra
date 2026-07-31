from __future__ import annotations

_POSITION_COMPARISON_FUNCTION = """
local function compare_positions(left, right)
    if #left < #right then
        return -1
    end
    if #left > #right then
        return 1
    end
    if left < right then
        return -1
    end
    if left > right then
        return 1
    end
    return 0
end
"""

STREAM_APPEND_SCRIPT = """
redis.call('incr', KEYS[2])
local position = redis.call('get', KEYS[2])
-- Redis entry IDs deliberately encode the private allocated position, not time.
redis.call('xadd', KEYS[1], position .. '-0', 'p', position, 'd', ARGV[1])
redis.call('xtrim', KEYS[1], 'MAXLEN', '=', ARGV[2])
if ARGV[3] then
    redis.call('pexpire', KEYS[1], ARGV[3])
    redis.call('pexpire', KEYS[2], ARGV[3])
end
return position
"""

STREAM_READ_SCRIPT = (
    _POSITION_COMPARISON_FUNCTION
    + """
local function increment_position(position)
    local digits = {}
    local carry = 1
    for index = #position, 1, -1 do
        local digit = string.byte(position, index) - string.byte('0') + carry
        if digit == 10 then
            digit = 0
            carry = 1
        else
            carry = 0
        end
        digits[index] = string.char(string.byte('0') + digit)
    end
    if carry == 1 then
        return '1' .. table.concat(digits)
    end
    return table.concat(digits)
end

local function field_value(fields, name)
    for index = 1, #fields, 2 do
        if fields[index] == name then
            return fields[index + 1]
        end
    end
    return nil
end

local after = ARGV[1]
local limit = tonumber(ARGV[2])
local head = redis.call('xrange', KEYS[1], '-', '+', 'COUNT', 1)
if #head == 0 then
    return {0}
end
if after ~= '' then
    local earliest = field_value(head[1][2], 'p')
    if not earliest or compare_positions(increment_position(after), earliest) < 0 then
        return {-1}
    end
end

local start = '-'
local count = limit
if after ~= '' then
    start = after .. '-0'
    count = limit + 1
end
local records = redis.call('xrange', KEYS[1], start, '+', 'COUNT', count)
if after ~= '' and #records > 0 and field_value(records[1][2], 'p') == after then
    table.remove(records, 1)
end
table.insert(records, 1, 1)
return records
"""
)

STREAM_ACKNOWLEDGE_SCRIPT = (
    _POSITION_COMPARISON_FUNCTION
    + """
local latest = redis.call('get', KEYS[1]) or '0'
local position = ARGV[2]
if compare_positions(position, latest) > 0 then
    return 0
end
local current = redis.call('hget', KEYS[2], ARGV[1])
if current and compare_positions(position, current) < 0 then
    return -1
end
if not current and redis.call('hlen', KEYS[2]) >= tonumber(ARGV[3]) then
    return -2
end
redis.call('hset', KEYS[2], ARGV[1], position)
if ARGV[4] then
    redis.call('pexpire', KEYS[2], ARGV[4])
end
return 1
"""
)

STREAM_FORGET_CONSUMER_SCRIPT = """
return redis.call('hdel', KEYS[1], ARGV[1])
"""


__all__ = (
    "STREAM_ACKNOWLEDGE_SCRIPT",
    "STREAM_APPEND_SCRIPT",
    "STREAM_FORGET_CONSUMER_SCRIPT",
    "STREAM_READ_SCRIPT",
)
