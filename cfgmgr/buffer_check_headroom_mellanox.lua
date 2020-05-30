-- KEYS - port name

local port = KEYS[1]
local profile
local lossless_profile
local lossless_headroom_size
local lossless_profile_name
local accumulative_size = 0

local appl_db = "0"
local state_db = "6"

local ret = {}

-- Fetch all keys in BUFFER_PG according to the port
redis.call('SELECT', appl_db)

local function get_number_of_pgs(keyname)
    local range = string.match(keyname, "Ethernet%d+:([^%s]+)$")
    local size
    if string.len(range) == 1 then
        size = 1
    else
        size = 1 + tonumber(string.sub(range, -1)) - tonumber(string.sub(range, 1, 1))
    end
    return size
end

-- Fetch all the PGs, accumulate the sizes
-- Assume there is only one lossless profile configured among all PGs on each port
local pg_keys = redis.call('KEYS', 'BUFFER_PG:' .. port .. '*')
for i = 1, #pg_keys do
    profile = string.sub(redis.call('HGET', pg_keys[i], 'profile'), 2, -2)
    if lossless_profile_name ~= nil then
        if profile == lossless_profile_name then
            accumulative_size = accumulative_size + lossless_headroom_size * get_number_of_pgs(pg_keys[i])
        end
    else
        lossless_profile = redis.call('HGETALL', profile)
        for j = 1, #lossless_profile, 2 do
            if lossless_profile[j] == 'xoff' then
                lossless_profile_name = profile
            end
            if lossless_profile[j] == 'size' then
                lossless_headroom_size = tonumber(lossless_profile[j+1])
                accumulative_size = lossless_headroom_size * get_number_of_pgs(pg_keys[i])
            end
        end
    end
end

-- Fetch the threshold from STATE_DB
redis.call('SELECT', state_db)

local asic_keys = redis.call('KEYS', 'BUFFER_MAX_PARAM*')
local max_headroom_size = tonumber(redis.call('HGET', asic_keys[1], 'max_headroom_size'))

asic_keys = redis.call('KEYS', 'ASIC_TABLE*')
local pipeline_delay = tonumber(redis.call('HGET', asic_keys[1], 'pipeline_latency'))

accumulative_size = accumulative_size + 2 * pipeline_delay * 1024

if max_headroom_size > accumulative_size then
    table.insert(ret, "result:true")
else
    table.insert(ret, "result:false")
end

table.insert(ret, "max headroom:" .. max_headroom_size)
table.insert(ret, "accumulative headroom:" .. accumulative_size)

return ret
