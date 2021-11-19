-- KEYS - port name
-- ARGV[1] - profile name
-- ARGV[2] - new size
-- ARGV[3] - pg to add

local port = KEYS[1]
local input_profile_name = ARGV[1]
local input_profile_size = ARGV[2]
local new_pg = ARGV[3]

local function is_port_with_8lanes(lanes)
    -- On Spectrum 3, ports with 8 lanes have doubled pipeline latency
    local number_of_lanes = 0
    if lanes then
        local _
        _, number_of_lanes = string.gsub(lanes, ",", ",")
        number_of_lanes = number_of_lanes + 1
    end
    return number_of_lanes == 8
end

-- Initialize the accumulative size with 4096
-- This is to absorb the possible deviation
local accumulative_size = 4096
-- Egress mirror size: 2 * maximum MTU (10k)
local egress_mirror_size = 20*1024

local appl_db = "0"
local state_db = "6"
local config_db = "4"

local ret_true = {}
local ret = {}
local default_ret = {}

table.insert(ret_true, "result:true")

default_ret = ret_true

-- Connect to CONFIG_DB
redis.call('SELECT', config_db)

local lanes

-- We need to know whether it's a 8-lane port because it has extra pipeline latency
lanes = redis.call('HGET', 'PORT|' .. port, 'lanes')

-- Fetch the threshold from STATE_DB
redis.call('SELECT', state_db)

local max_headroom_size = tonumber(redis.call('HGET', 'BUFFER_MAX_PARAM_TABLE|' .. port, 'max_headroom_size'))
if max_headroom_size == nil then
    return default_ret
end

local asic_keys = redis.call('KEYS', 'ASIC_TABLE*')
local pipeline_latency = tonumber(redis.call('HGET', asic_keys[1], 'pipeline_latency'))
if is_port_with_8lanes(lanes) then
    -- The pipeline latency should be adjusted accordingly for ports with 2 buffer units
    pipeline_latency = pipeline_latency * 2 - 1
    egress_mirror_size = egress_mirror_size * 2
end
accumulative_size = accumulative_size + 2 * pipeline_latency * 1024 + egress_mirror_size

-- Fetch all keys in BUFFER_PG according to the port
redis.call('SELECT', appl_db)

local debuginfo = {}

local function get_number_of_pgs(keyname)
    local range = string.match(keyname, "Ethernet%d+:([^%s]+)$")
    local size
    if range == nil then
        return 0
    end
    if string.len(range) == 1 then
        size = 1
    else
        size = 1 + tonumber(string.sub(range, -1)) - tonumber(string.sub(range, 1, 1))
    end
    return size
end

local no_input_pg = true
if new_pg ~= nil then
    if get_number_of_pgs(new_pg) ~= 0 then
        no_input_pg = false
        new_pg = 'BUFFER_PG_TABLE:' .. new_pg
    end
end

-- Fetch all the PGs, accumulate the sizes
-- Assume there is only one lossless profile configured among all PGs on each port
table.insert(debuginfo, 'debug:other overhead:' .. accumulative_size)
local pg_keys = redis.call('KEYS', 'BUFFER_PG_TABLE:' .. port .. ':*')
for i = 1, #pg_keys do
    local profile = string.sub(redis.call('HGET', pg_keys[i], 'profile'), 2, -2)
    local current_profile_size
    if profile ~= 'BUFFER_PROFILE_TABLE:ingress_lossy_profile' and (no_input_pg or new_pg ~= pg_keys[i]) then
        if profile ~= input_profile_name and not no_input_pg then
            local referenced_profile = redis.call('HGETALL', profile)
            for j = 1, #referenced_profile, 2 do
                if referenced_profile[j] == 'size' then
                    current_profile_size = tonumber(referenced_profile[j+1])
                end
            end
        else
            current_profile_size = input_profile_size
            profile = input_profile_name
        end
        accumulative_size = accumulative_size + current_profile_size * get_number_of_pgs(pg_keys[i])
        table.insert(debuginfo, 'debug:' .. pg_keys[i] .. ':' .. profile .. ':' .. current_profile_size .. ':' .. get_number_of_pgs(pg_keys[i]) .. ':accu:' .. accumulative_size)
    end
end

if not no_input_pg then
    accumulative_size = accumulative_size + input_profile_size * get_number_of_pgs(new_pg)
    table.insert(debuginfo, 'debug:' .. new_pg .. '*:' .. input_profile_name .. ':' .. input_profile_size .. ':' .. get_number_of_pgs(new_pg) .. ':accu:' .. accumulative_size)
end

if max_headroom_size > accumulative_size then
    table.insert(ret, "result:true")
    table.insert(ret, "debug:Accumulative headroom on port " .. accumulative_size .. ", the maximum available headroom " .. max_headroom_size)
else
    table.insert(ret, "result:false")
    table.insert(ret, "debug:Accumulative headroom on port " .. accumulative_size .. " exceeds the maximum available headroom which is " .. max_headroom_size)
end

for i = 1, #debuginfo do
    table.insert(ret, debuginfo[i])
end

return ret
