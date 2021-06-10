-- KEYS - None
-- ARGV - None

local appl_db = "0"
local config_db = "4"
local state_db = "6"

-- Number of ports with double buffer units
local port_count_double_buffer_units = 0
-- Number of lossy PG on ports with double buffer units
local lossypg_double_buffer_units = 0

local is_spc3 = false

local result = {}
local profiles = {}

local total_port = 0

local mgmt_pool_size = 256 * 1024
local egress_mirror_headroom = 10 * 1024

local port_set_double_buffer_units = {}

local function iterate_all_items(all_items)
    table.sort(all_items)
    local port
    local fvpairs
    for i = 1, #all_items, 1 do
        -- Count the number of priorities or queues in each BUFFER_PG or BUFFER_QUEUE item
        -- For example, there are:
        --     3 queues in 'BUFFER_QUEUE_TABLE:Ethernet0:0-2'
        --     2 priorities in 'BUFFER_PG_TABLE:Ethernet0:3-4'
        port = string.match(all_items[i], "Ethernet%d+")
        if port ~= nil then
            local range = string.match(all_items[i], "Ethernet%d+:([^%s]+)$")
            local profile_name = string.sub(redis.call('HGET', all_items[i], 'profile'), 2, -2)
            local profile_ref_count = profiles[profile_name]
            if profile_ref_count == nil then
                -- Indicate an error in case the referenced profile hasn't been inserted or has been removed
                -- It's possible when the orchagent is busy
                -- The buffermgrd will take care of it and retry later
                return 1
            end
            local size
            if string.len(range) == 1 then
                size = 1
            else
                size = 1 + tonumber(string.sub(range, -1)) - tonumber(string.sub(range, 1, 1))
            end
            profiles[profile_name] = profile_ref_count + size
            if is_spc3 and port_set_double_buffer_units[port] and profile_name == 'BUFFER_PROFILE_TABLE:ingress_lossy_profile' then
                lossypg_double_buffer_units = lossypg_double_buffer_units + size
            end
        end
    end
    return 0
end

-- Connect to CONFIG_DB
redis.call('SELECT', config_db)

local ports_table = redis.call('KEYS', 'PORT|*')

total_port = #ports_table

-- Initialize the port_set_double_buffer_units set
local platform = redis.call('HGET', 'DEVICE_METADATA|localhost', 'platform')
if platform ~= nil and string.sub(platform, 1, 16) == "x86_64-mlnx_msn4" then
    is_spc3 = true
    local lanes
    local number_of_lanes
    local port
    for i = 1, total_port, 1 do
        -- Load lanes from PORT table
        lanes = redis.call('HGET', ports_table[i], 'lanes')
        if lanes ~= nil then
            local _
            _, number_of_lanes = string.gsub(lanes, ",", ",")
            number_of_lanes = number_of_lanes + 1
            port = string.sub(ports_table[i], 6, -1)
            if (number_of_lanes == 8) then
                port_set_double_buffer_units[port] = true
                port_count_double_buffer_units = port_count_double_buffer_units + 1
            else
                port_set_double_buffer_units[port] = false
            end
        end
    end
end

local egress_lossless_pool_size = redis.call('HGET', 'BUFFER_POOL|egress_lossless_pool', 'size')

-- Whether shared headroom pool is enabled?
local default_lossless_param_keys = redis.call('KEYS', 'DEFAULT_LOSSLESS_BUFFER_PARAMETER*')
local over_subscribe_ratio = tonumber(redis.call('HGET', default_lossless_param_keys[1], 'over_subscribe_ratio'))

-- Fetch the shared headroom pool size
local shp_size = tonumber(redis.call('HGET', 'BUFFER_POOL|ingress_lossless_pool', 'xoff'))

local shp_enabled = false
if over_subscribe_ratio ~= nil and over_subscribe_ratio ~= 0 then
    shp_enabled = true
end

if shp_size ~= nil and shp_size ~= 0 then
    shp_enabled = true
else
    shp_size = 0
end

-- Fetch mmu_size
redis.call('SELECT', state_db)
local mmu_size = tonumber(redis.call('HGET', 'BUFFER_MAX_PARAM_TABLE|global', 'mmu_size'))
if mmu_size == nil then
    mmu_size = tonumber(egress_lossless_pool_size)
end
local asic_keys = redis.call('KEYS', 'ASIC_TABLE*')
local cell_size = tonumber(redis.call('HGET', asic_keys[1], 'cell_size'))
local pipeline_latency = tonumber(redis.call('HGET', asic_keys[1], 'pipeline_latency'))

local lossypg_reserved = pipeline_latency * 1024
local lossypg_reserved_double_buffer_units = (2 * pipeline_latency - 1) * 1024

-- Align mmu_size at cell size boundary, otherwise the sdk will complain and the syncd will fail
local number_of_cells = math.floor(mmu_size / cell_size)
local ceiling_mmu_size = number_of_cells * cell_size

-- Switch to APPL_DB
redis.call('SELECT', appl_db)

-- Fetch names of all profiles and insert them into the look up table
local all_profiles = redis.call('KEYS', 'BUFFER_PROFILE*')
for i = 1, #all_profiles, 1 do
    profiles[all_profiles[i]] = 0
end

-- Fetch all the PGs
local all_pgs = redis.call('KEYS', 'BUFFER_PG*')
local all_tcs = redis.call('KEYS', 'BUFFER_QUEUE*')

local fail_count = 0
fail_count = fail_count + iterate_all_items(all_pgs)
fail_count = fail_count + iterate_all_items(all_tcs)
if fail_count > 0 then
    return {}
end

local statistics = {}

-- Fetch sizes of all of the profiles, accumulate them
local accumulative_occupied_buffer = 0
local accumulative_xoff = 0

for name in pairs(profiles) do
    if name ~= "BUFFER_PROFILE_TABLE_KEY_SET" and name ~= "BUFFER_PROFILE_TABLE_DEL_SET" then
        local size = tonumber(redis.call('HGET', name, 'size'))
        if size ~= nil then 
            if name == "BUFFER_PROFILE_TABLE:ingress_lossy_profile" then
                size = size + lossypg_reserved
            end
            if name == "BUFFER_PROFILE_TABLE:egress_lossy_profile" then
                profiles[name] = total_port
            end
            if size ~= 0 then
                if shp_enabled and shp_size == 0 then
                    local xon = tonumber(redis.call('HGET', name, 'xon'))
                    local xoff = tonumber(redis.call('HGET', name, 'xoff'))
                    if xon ~= nil and xoff ~= nil and xon + xoff > size then
                        accumulative_xoff = accumulative_xoff + (xon + xoff - size) * profiles[name]
                    end
                end
                accumulative_occupied_buffer = accumulative_occupied_buffer + size * profiles[name]
            end
            table.insert(statistics, {name, size, profiles[name]})
        end
    end
end

-- Extra lossy xon buffer for ports with double buffer unit
local lossypg_extra_for_double_buffer_units = (lossypg_reserved_double_buffer_units - lossypg_reserved) * lossypg_double_buffer_units
accumulative_occupied_buffer = accumulative_occupied_buffer + lossypg_extra_for_double_buffer_units

-- Accumulate sizes for management PGs
local accumulative_management_pg = (total_port - port_count_double_buffer_units) * lossypg_reserved + port_count_double_buffer_units * lossypg_reserved_double_buffer_units
accumulative_occupied_buffer = accumulative_occupied_buffer + accumulative_management_pg

-- Accumulate sizes for egress mirror and management pool
local accumulative_egress_mirror_overhead = total_port * egress_mirror_headroom
accumulative_occupied_buffer = accumulative_occupied_buffer + accumulative_egress_mirror_overhead + mgmt_pool_size

-- Switch to CONFIG_DB
redis.call('SELECT', config_db)

-- Fetch all the pools that need update
local pools_need_update = {}
local ipools = redis.call('KEYS', 'BUFFER_POOL|ingress*')
local ingress_pool_count = 0
local ingress_lossless_pool_size = nil
for i = 1, #ipools, 1 do
    local size = tonumber(redis.call('HGET', ipools[i], 'size'))
    if not size then
        table.insert(pools_need_update, ipools[i])
        ingress_pool_count = ingress_pool_count + 1
    else
        if ipools[i] == 'BUFFER_POOL|ingress_lossless_pool' and shp_enabled and shp_size == 0 then
            ingress_lossless_pool_size = size
        end
    end
end

local epools = redis.call('KEYS', 'BUFFER_POOL|egress*')
for i = 1, #epools, 1 do
    local size = redis.call('HGET', epools[i], 'size')
    if not size then
        table.insert(pools_need_update, epools[i])
    end
end

if shp_enabled and shp_size == 0 then
    shp_size = math.ceil(accumulative_xoff / over_subscribe_ratio)
end

local pool_size
if shp_size then
    accumulative_occupied_buffer = accumulative_occupied_buffer + shp_size
end
if ingress_pool_count == 1 then
    pool_size = mmu_size - accumulative_occupied_buffer
else
    pool_size = (mmu_size - accumulative_occupied_buffer) / 2
end

if pool_size > ceiling_mmu_size then
    pool_size = ceiling_mmu_size
end

local shp_deployed = false
for i = 1, #pools_need_update, 1 do
    local pool_name = string.match(pools_need_update[i], "BUFFER_POOL|([^%s]+)$")
    if shp_size ~= 0 and pool_name == "ingress_lossless_pool" then
        table.insert(result, pool_name .. ":" .. math.ceil(pool_size) .. ":" .. math.ceil(shp_size))
        shp_deployed = true
    else
        table.insert(result, pool_name .. ":" .. math.ceil(pool_size))
    end
end

if not shp_deployed and shp_size ~= 0 and ingress_lossless_pool_size ~= nil then
    table.insert(result, "ingress_lossless_pool:" .. math.ceil(ingress_lossless_pool_size) .. ":" .. math.ceil(shp_size))
end

table.insert(result, "debug:mmu_size:" .. mmu_size)
table.insert(result, "debug:accumulative size:" .. accumulative_occupied_buffer)
for i = 1, #statistics do
    table.insert(result, "debug:" .. statistics[i][1] .. ":" .. statistics[i][2] .. ":" .. statistics[i][3])
end
table.insert(result, "debug:extra_double_buffer_units:" .. (lossypg_reserved_double_buffer_units - lossypg_reserved) .. ":" .. lossypg_double_buffer_units .. ":" .. port_count_double_buffer_units)
table.insert(result, "debug:mgmt_pool:" .. mgmt_pool_size)
table.insert(result, "debug:accumulative_mgmt_pg:" .. accumulative_management_pg)
table.insert(result, "debug:egress_mirror:" .. accumulative_egress_mirror_overhead)
table.insert(result, "debug:shp_enabled:" .. tostring(shp_enabled))
table.insert(result, "debug:shp_size:" .. shp_size)
table.insert(result, "debug:accumulative xoff:" .. accumulative_xoff)
table.insert(result, "debug:total port:" .. total_port .. " ports with double buffer units:" .. port_count_double_buffer_units)

return result
